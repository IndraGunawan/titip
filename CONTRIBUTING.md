# Contributing to Titip

Thank you for your interest in contributing to **Titip**!

Titip is designed as a high-performance, low-allocation, RFC-7234 & RFC-9211 compliant HTTP caching middleware in Go. To maintain rock-solid reliability and sub-millisecond latency, all components adhere to strict architectural standards.

---

## Table of Contents

1. [Monorepo Architecture & `go.work`](#1-monorepo-architecture--gowork)
2. [Core Principles & Prohibitions](#2-core-principles--prohibitions)
3. [How to Implement a New Storage Driver (`storage/*`)](#3-how-to-implement-a-new-storage-driver-storage)
   - [Storage Interface](#the-storagestorage-interface)
   - [Contract Requirements](#storage-contract-requirements)
   - [Caddy Storage Module Integration](#caddy-guest-storage-module-integration)
4. [How to Implement a New Framework Adapter (`adapter/*`)](#4-how-to-implement-a-new-framework-adapter-adapter)
   - [Adapter Architecture](#adapter-architecture)
   - [ESI Subrequest Bridging](#esi-subrequest-bridging)
5. [Testing & Quality Standards](#5-testing--quality-standards)
6. [Submitting a Pull Request](#6-submitting-a-pull-request)

---

## 1. Monorepo Architecture & `go.work`

Titip is organized as a multi-module monorepo using standard Go workspaces (`go.work`):

```text
titip/
├── go.mod                      # Core library (Zero external deps except Protobuf & LZ4)
├── adapter/
│   ├── caddy/                  # Caddy v2 web server plugin
│   └── chi/                    # Go-Chi HTTP middleware adapter
├── storage/
│   └── redis/                  # Redis 7+ storage driver (using rueidis)
│       └── caddy/              # Caddy guest storage plugin for Redis
└── examples/
    ├── caddy-demo/             # Standalone Caddy reverse proxy demo
    ├── chi-demo/               # Standalone Chi web server demo
    └── frankenphp-demo/        # FrankenPHP + Caddy + Titip Docker demo
```

### Monorepo Rules

1. **Isolated `go.mod`**: Every subpackage in `adapter/*`, `storage/*`, and `examples/*` must maintain its own `go.mod`.
2. **Workspace Registration**: When adding a new module, register it in the root `go.work` file:

   ```bash
   go work use ./storage/memcached
   go work sync
   ```

3. **No Dependency Bleed**:
   - `core` (`titip`) must **never** import third-party web frameworks (e.g. `chi`, `gin`, `caddy`) or database drivers (e.g. `rueidis`, `memcached`).
   - Adapters and storage modules import `github.com/indragunawan/titip`.

---

## 2. Core Principles & Prohibitions

Before writing code, ensure your implementation complies with our core architectural rules:

| Rule | Requirement |
| :--- | :--- |
| **Fail-Open Design** | Backend timeouts, serialization errors, or storage crashes must **never** return a 500 error to end users. Always fall back gracefully to the origin handler (`fwd=bypass`). |
| **Zero Per-Request Hashing** | Never hash URLs with SHA-256, MD5, or xxHash. Assemble normalized string keys directly using pooled string builders. |
| **Zero Allocation Hot Paths** | Use `sync.Pool` for byte buffers and recorders. Never hold slice references after returning a buffer to the pool (`PutBuffer`). |
| **No Read-Modify-Write Races** | For storage drivers, never read a metadata record in Go, modify a variant, and re-write the whole object. Use atomic storage operations (e.g. Redis Hashes). |
| **Zero Goroutine Leaks** | Track background tasks (e.g. singleflight revalidation) with `sync.WaitGroup` and await clean termination in `Close(ctx)`. |

---

## 3. How to Implement a New Storage Driver (`storage/*`)

To add support for a new storage engine (e.g. Memcached, Dragonfly, Cloudflare KV, Aerospike, S3/DynamoDB):

### The `storage.Storage` Interface

Create your driver under `storage/<engine>/` and implement the standard interface defined in [`storage/storage.go`](file:///Users/indra/code/project/titip/storage/storage.go):

```go
package storage

import (
    "context"
    "time"

    pb "github.com/indragunawan/titip/proto"
)

type Storage interface {
    // GetMetadata retrieves the compact Protobuf metadata for key.
    // Returns nil, nil if the key does not exist.
    GetMetadata(ctx context.Context, key string) (*pb.CacheMetadata, error)

    // GetVariant retrieves the compressed variant payload bytes.
    // Returns nil, nil if the variant does not exist.
    GetVariant(ctx context.Context, key, variantKey string) ([]byte, error)

    // SetVariant atomically persists the metadata and variant payload with a TTL.
    SetVariant(ctx context.Context, key, variantKey string, metadata *pb.CacheMetadata, body []byte, ttl time.Duration) error

    // Delete hard-deletes the metadata AND all variant body payloads for key.
    Delete(ctx context.Context, key string) error

    // SoftPurge marks all variants for key as stale as of staleTime.
    SoftPurge(ctx context.Context, key string, staleTime time.Time) error

    // PurgeByTag invalidates all cache entries associated with the tag.
    PurgeByTag(ctx context.Context, tag string, soft bool, staleTime time.Time) error

    // PurgeAll purges or soft-invalidates all cache entries across the storage engine.
    PurgeAll(ctx context.Context, soft bool, staleTime time.Time) error

    // Close gracefully flushes buffers and closes client connections.
    Close(ctx context.Context) error
}
```

### Storage Contract Requirements

1. **Atomic Variant Storage**:
   - Store metadata and variant payloads atomically.
   - For example, Redis uses `HSET <key> "meta" <meta_bytes> "v:<variantKey>" <body_bytes>`.
2. **Dynamic TTL Extension**:
   - When a new variant is stored on an existing key, the key's TTL must be extended to `max(existing_ttl, new_ttl)`. (e.g. Redis `EXPIRE ... GT`).
3. **Zero Orphaned Payloads**:
   - `Delete(ctx, key)` must delete the metadata record and all variant body payloads atomically.
4. **Soft-Purge Timestamping**:
   - `SoftPurge` sets `stale_after_unix = staleTime.Unix()`. Requests arriving with cached data older than this timestamp will serve stale while asynchronously revalidating in the background.

---

### Caddy Guest Storage Module Integration

If your storage driver should be configurable in Caddyfiles (e.g. `storage memcached { address 127.0.0.1:11211 }`), add a Caddy guest module under `storage/<engine>/caddy/`:

1. **Implement `caddy.Module` & `titipcaddy.StorageModule`**:

   ```go
   package caddy

   import (
       "github.com/caddyserver/caddy/v2"
       "github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
       "github.com/indragunawan/titip/adapter/caddy"
       "github.com/indragunawan/titip/storage"
       mymod "github.com/indragunawan/titip/storage/mymod"
   )

   func init() {
       caddy.RegisterModule(Storage{})
   }

   type Storage struct {
       Address string `json:"address,omitempty"`
       store   storage.Storage
   }

   func (Storage) CaddyModule() caddy.ModuleInfo {
       return caddy.ModuleInfo{
           ID:  "titip.storage.mymod",
           New: func() caddy.Module { return new(Storage) },
       }
   }

   func (s *Storage) Storage() storage.Storage {
       return s.store
   }

   func (s *Storage) Provision(ctx caddy.Context) error {
       store, err := mymod.New(mymod.Config{Address: s.Address})
       if err != nil {
           return err
       }
       s.store = store
       return nil
   }

   func (s *Storage) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
       for d.Next() {
           for d.NextBlock(0) {
               switch d.Val() {
               case "address":
                   if !d.NextArg() {
                       return d.ArgErr()
                   }
                   s.Address = d.Val()
               }
           }
       }
       return nil
   }
   ```

---

## 4. How to Implement a New Framework Adapter (`adapter/*`)

To add an adapter for another web framework (e.g. **Gin**, **Echo**, **Fiber**, **Fiber/v3**, **FastHTTP**):

### Adapter Architecture

1. Create a submodule under `adapter/<framework>/` with its own `go.mod`.
2. Wrap Titip's core `*titip.Titip` instance and bridge the framework's context and response writer:

   ```go
   package ginadapter

   import (
       "net/http"

       "github.com/gin-gonic/gin"
       "github.com/indragunawan/titip"
   )

   func Middleware(t *titip.Titip) gin.HandlerFunc {
       return func(c *gin.Context) {
           downstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
               c.Request = r
               c.Next()
           })

           t.ServeHTTP(c.Writer, c.Request, downstream)
       }
   }
   ```

### ESI Subrequest Bridging

To support in-process Edge Side Includes (ESI) subrequests without loopback HTTP overhead:

- Allow developers to pass their root router into `titip.WithESI(esi.WithInternalFetcher(esi.HandlerFetcher(router)))`.
- Ensure all virtual subrequests created by Titip execute through the framework router in memory.

---

## 5. Testing & Quality Standards

Every feature, adapter, or storage driver must pass our automated quality suite before merging.

### 1. Run Automated Unit & Concurrency Tests

```bash
go test -v ./...
```

### 2. Run Continuous Race Detection (Zero-Race Guarantee)

```bash
go test -race -count=100 -parallel=8 ./...
```

### 3. Verify Low-Allocation Standards

```bash
go test -benchmem -bench=. ./...
```

### 4. Linting & Formatting

```bash
go vet ./...
golangci-lint run
```

---

## 6. Submitting a Pull Request

1. **Branch Naming**: Use descriptive branch names (e.g. `feat/memcached-storage`, `fix/cache-key-normalization`, `feat/gin-adapter`).
2. **Conventional Commits**:
   - `feat(storage/memcached)`: Add Memcached driver
   - `feat(adapter/gin)`: Add Gin framework adapter
   - `fix(esi)`: Fix ESI quote parsing edge case
   - `test(redis)`: Add race condition test for dynamic TTL extension
   - `docs(readme)`: Update architectural diagrams
3. **Include an Example & README**:
   - When adding a new storage driver or adapter, include an example under `examples/<name>-demo/` with a self-contained `Makefile` and `README.md`.
