# Contributing to Titip

Thank you for your interest in contributing to **Titip**!

Titip is designed as a high-performance, low-allocation, RFC-7234, RFC-9111 & RFC-9211 compliant HTTP caching middleware in Go. To maintain rock-solid reliability and sub-millisecond latency, all components adhere to strict architectural standards.

> [!NOTE]
> **Prerequisites**: A Go toolchain (matching the version declared in [`go.mod`](go.mod)) and **Docker** (for running the local Redis test container).

## Monorepo Architecture & `go.work`

Titip is organized as a multi-module monorepo using standard Go workspaces (`go.work`). The codebase is divided into distinct zones of responsibility:

- **Core Engine (`/`)**: The root module (`github.com/indragunawan/titip`) containing the state machine, RFC-compliant HTTP caching pipeline, internal test store, and ESI processing.
- **Framework Adapters (`adapter/<framework>/`)**: Web server and framework integrations (e.g. `adapter/caddy`). Each adapter is a standalone Go module with its own `go.mod`.
- **Storage Drivers (`storage/<engine>/`)**: Distributed storage backend implementations (e.g. `storage/redis`). Each driver is a standalone Go module with its own `go.mod`.
- **Internal Test Sandboxes (`examples/`)**: Integration environments maintained by core maintainers for live verification, profiling, and local testing. These are not public library modules.

### Monorepo Rules

1. **Isolated `go.mod`**: Every distributed submodule under `adapter/*` and `storage/*` must maintain its own `go.mod`. Do not add new directories to `examples/` without prior maintainer discussion.
2. **Workspace Registration**: When adding a new module, register it in the root `go.work` file:

   ```bash
   go work use ./storage/memcached
   go work sync
   ```

3. **Local `replace` Directive**: When a submodule requires internal `titip` modules, declare a relative `replace` directive in its `go.mod` (enforced by CI so local `go mod tidy` and dev builds succeed):

   ```go
   replace github.com/indragunawan/titip => ../..
   ```

4. **No Dependency Bleed**:
   - `core` (`titip`) must **never** import third-party web frameworks (e.g. `chi`, `gin`, `caddy`) or database drivers (e.g. `rueidis`, `memcached`).
   - Adapters and storage modules import `github.com/indragunawan/titip`.

## Core Principles & Prohibitions

Before writing code, ensure your implementation complies with our core architectural rules:

| Rule | Requirement |
| :--- | :--- |
| **Fail-Open Design** | Backend timeouts, serialization errors, or storage crashes must **never** return a 500 error to end users. Always fall back gracefully to the origin handler (`fwd=bypass`). |
| **Zero Per-Request Hashing** | Never hash URLs with SHA-256, MD5, or xxHash. Assemble normalized string keys directly using pooled string builders. |
| **Low Allocation & Buffer Pooling** | Use `sync.Pool` for byte buffers and recorders to prevent payload heap churn. Never retain slice references after returning a buffer to the pool (`PutBuffer`). |
| **Concurrency & Race Safety** | Storage drivers must handle concurrent variant writes and purges safely, preventing lost updates using native backend mechanisms (e.g. atomic operations, CAS, or transactions). |
| **Zero Goroutine Leaks** | Track background tasks (e.g. singleflight revalidation) with `sync.WaitGroup` and await clean termination in `Close(ctx)`. |

## Implementing a Storage Driver

To add support for a new storage engine (e.g. Memcached, Dragonfly, Cloudflare KV, Aerospike, S3/DynamoDB):

### The Storage Interfaces

Create your driver under `storage/<engine>/` and implement the interfaces defined in [`storage/storage.go`](storage/storage.go):

- **Required**: Implement [`storage.Storage`](storage/storage.go) interface:
  - `GetMeta`, `GetVariant`, `SetVariant` (reading & writing variants)
  - `Purge`, `PurgeByPattern`, `PurgeByTag`, `PurgeAll` (complete invalidation lifecycle)
- **Optional Teardown**: Storage backends managing internal resources (goroutines, file handles, flush timers) may optionally implement [`storage.Closer`](storage/storage.go) (`Close(ctx context.Context) error`) or standard library `io.Closer` (`Close() error`). Drivers that wrap externally injected clients (like `storage/redis`) should not implement `Close` to respect caller ownership.

> [!TIP]
> See [`storage/redis/`](storage/redis/) as our existing reference implementation, which demonstrates atomic hash variant storage, dynamic TTL extension, and soft-purge timestamping with `rueidis`.

### Storage Contract Requirements

1. **Concurrency & Safe Writes**:
   - Persist metadata and variant payloads safely against concurrent writes without lost updates, using your backend's native capabilities (e.g. atomic commands, CAS, or transactions).
2. **Dynamic TTL Extension**:
   - When a new variant is stored on an existing key, extend the key's TTL to `max(existing_ttl, new_ttl)` where supported by the backend.
3. **Zero Orphaned Payloads**:
   - Hard purges (`Purge(ctx, key, soft=false)`) must physically delete the metadata record and all associated variant body payloads. Zero orphaned keys may remain in the backend.
4. **Soft-Purge Freshness Marker**:
   - Soft purges (`Purge(ctx, key, soft=true)`) must mark the primary entry as soft-purged (`isSoftPurged = true` in `GetMeta`) while preserving payload data, allowing safe stale fallback while revalidating.

### Caddy Guest Storage Module Integration

If your storage driver should be configurable in Caddyfiles (e.g. `storage memcached { address 127.0.0.1:11211 }`), add a Caddy guest module under `storage/<engine>/caddy/`:

1. **Implement `caddy.Module`, `caddy.Provisioner`, and `caddy.CleanerUpper`**:

   ```go
   package caddy

   import (
       "github.com/caddyserver/caddy/v2"
       "github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
       "github.com/indragunawan/titip/storage"
       mymod "github.com/indragunawan/titip/storage/mymod"
   )

   func init() {
       caddy.RegisterModule(Storage{})
   }

   type Storage struct {
       Address string `json:"address,omitempty"`
       client  *mymod.Client
       store   storage.Storage
   }

   func (Storage) CaddyModule() caddy.ModuleInfo {
       return caddy.ModuleInfo{
           ID:  "titip.storage.mymod",
           New: func() caddy.Module { return new(Storage) },
       }
   }

   // Storage satisfies titipcaddy.StorageModule via implicit interface.
   func (s *Storage) Storage() storage.Storage {
       return s.store
   }

   func (s *Storage) Provision(ctx caddy.Context) error {
       client, err := mymod.NewClient(s.Address)
       if err != nil {
           return err
       }
       s.client = client

       store, err := mymod.New(client)
       if err != nil {
           client.Close()
           return err
       }
       s.store = store
       return nil
   }

   // Cleanup closes the client connection provisioned by this module.
   func (s *Storage) Cleanup() error {
       if s.client != nil {
           return s.client.Close()
       }
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

> [!TIP]
> In `Cleanup()`, only call the single teardown method specific to what your module manages:
> - If your module provisions a connection pool or client (like `s.client`), close that client directly.
> - If your storage backend manages its own lifecycle and implements `storage.Closer` or `io.Closer`, call its close method directly (e.g. `s.store.Close(ctx)`).
>
> You do not need to implement multiple fallback checks—only invoke the one close method required for your storage backend. See [`storage/redis/caddy/`](storage/redis/caddy/) as our reference implementation.

## Implementing a Framework Adapter

Framework adapters bridge web servers or HTTP routers with Titip's core caching pipeline.

### The Middleware Contract

Titip instances are initialized via `titip.New(titip.WithStorage(store), opts...)`. The engine exposes a standard execution method designed around Go HTTP primitives:

```go
t.ServeHTTP(w http.ResponseWriter, r *http.Request, next http.Handler)
```

- **Cache Hit**: Titip streams the cached headers, status code, and body directly to `w` and terminates immediately. Downstream application handlers (`next`) are **never called**.
- **Cache Miss / Revalidation**: Titip intercepts the request, wraps `w` with an internal pooled response recorder, and executes `next.ServeHTTP(rec, r)` to invoke origin handlers. If the origin response is cacheable per `Cache-Control` rules, Titip asynchronously stores the entry and writes the response back to `w`.
- **Background Revalidation (SWR)**: On stale-while-revalidate hits, Titip serves stale data immediately to `w` while dispatching an asynchronous background fetch to `next` tracked by `sync.WaitGroup`.

### Standard Middleware Pattern (e.g. Chi, `net/http`)

Adapters typically accept `*titip.Titip` directly, keeping the adapter cleanly decoupled from specific storage implementations and allowing users to configure Titip options freely. For routers using the standard Go middleware signature (`func(http.Handler) http.Handler`), wrapping Titip requires only a clean bridge:

```go
package chiadapter

import (
    "net/http"

    "github.com/indragunawan/titip"
)

// Middleware wraps Titip into a standard func(http.Handler) http.Handler middleware.
func Middleware(t *titip.Titip) func(http.Handler) http.Handler {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            t.ServeHTTP(w, r, next)
        })
    }
}
```

#### Application Usage Example

```go
package main

import (
    "context"
    "log"
    "net/http"

    "github.com/go-chi/chi/v5"
    "github.com/redis/rueidis"

    "github.com/indragunawan/titip"
    chiadapter "github.com/indragunawan/titip/adapter/chi"
    redisstorage "github.com/indragunawan/titip/storage/redis"
)

func main() {
    // 1. Initialize a storage driver (e.g. Redis)
    client, err := rueidis.NewClient(rueidis.ClientOption{
        InitAddress: []string{"localhost:6379"},
    })
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    store, err := redisstorage.New(client)
    if err != nil {
        log.Fatal(err)
    }

    // 2. Initialize the Titip instance
    t, err := titip.New(
        titip.WithStorage(store),
        // ...and any optional titip.With... settings (e.g. WithESI, WithCacheStatus)
    )
    if err != nil {
        log.Fatal(err)
    }
    defer t.Close(context.Background())

    // 3. Mount Titip middleware on the router
    r := chi.NewRouter()
    r.Use(chiadapter.Middleware(t))

    r.Get("/api/products", func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Cache-Control", "public, max-age=60")
        w.Write([]byte(`{"status": "ok"}`))
    })

    http.ListenAndServe(":8080", r)
}
```

### Adapter Responsibilities

When building an adapter for any framework (e.g. Chi, Echo, Gin, Fiber):

1. **Graceful Shutdown**:
   - Always expose or document a shutdown hook: during server termination, callers must invoke `t.Close(ctx)` to drain pending asynchronous SWR revalidation goroutines.
2. **In-Memory ESI Dispatching**:
   - When Edge Side Includes (ESI) are enabled, allow users to pass the root router into `titip.WithESI(esi.WithInternalFetcher(esi.HandlerFetcher(router)))` so fragment includes execute in-process without network overhead.
3. **URL Preservation**:
   - If the framework or upstream directives perform URL rewrites (e.g. stripping path prefixes or rewriting to an index file), ensure the original request path is available for cache key assembly when route-specific caching is needed.

> [!TIP]
> For complex web server plugins requiring configuration parsing, dynamic reloads, and admin endpoints, inspect [`adapter/caddy/`](adapter/caddy/) as our primary production reference implementation.

## Testing & Quality Standards

Every feature, adapter, or storage driver must pass our automated quality suite before merging.

> [!NOTE]
> Because Titip is a multi-module workspace (`go.work`), running `go test ./...` from the root directory only tests the root module. Use the root `Makefile` to run tasks across all workspace modules automatically, or run `go test` inside the specific submodule directory.

### Running Test Redis

Storage drivers (such as `storage/redis`) require a local Redis instance for integration and concurrency tests:

```bash
docker compose up -d    # Starts Redis 8 container
docker compose down     # Stops Redis container
```

### Workspace Quality Commands

```bash
make test        # Run unit tests across all workspace modules
make race        # Run race detection (-race -count=100 -parallel=8)
make bench       # Run memory allocation benchmarks (-benchmem -bench=.)
make vet         # Run go vet across all workspace modules
make fmt         # Format and simplify Go code across workspace
make lint        # Run golangci-lint across all workspace modules
make fix         # Run go fix across all workspace modules
```

To run tests in a single submodule:

```bash
cd storage/redis
go test -v -race -count=100 ./...
```

### Linting & Formatting

Run `golangci-lint` inside the module directory you are actively modifying:

```bash
golangci-lint run ./...
```

## Submitting a Pull Request

1. **Workspace Integrity & Tidy**:
   - Run `go mod tidy` in every submodule you created or modified.
   - Run `go work sync` at the workspace root to ensure clean dependency graphs.
   - Confirm there are no uncommitted diffs (`git status`). CI automatically checks `git diff --exit-code` after `go mod tidy`.
1. **Module Documentation & Tests**:
   - When adding a new storage driver or adapter, provide comprehensive documentation (`README.md`) and unit/concurrency tests inside the module's own directory (`adapter/<name>/` or `storage/<name>/`). Do not create new directories under `examples/` without prior discussion.
