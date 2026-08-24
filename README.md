<div align="center">

# Titip

**High-Performance, Low-Allocation, RFC-Compliant HTTP Caching Middleware for Go**

[![Go Reference](https://pkg.go.dev/badge/github.com/indragunawan/titip.svg)](https://pkg.go.dev/github.com/indragunawan/titip)
[![Go Report Card](https://goreportcard.com/badge/github.com/indragunawan/titip)](https://goreportcard.com/report/github.com/indragunawan/titip)
[![CI Matrix](https://github.com/indragunawan/titip/actions/workflows/ci.yml/badge.svg)](https://github.com/indragunawan/titip/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Go Version](https://img.shields.io/github/go-mod/go-version/indragunawan/titip)](https://golang.org/)

<p align="center">
  <a href="#-key-features">Key Features</a> •
  <a href="#-architecture">Architecture</a> •
  <a href="#-modules--ecosystem">Modules</a> •
  <a href="#-quickstart">Quickstart</a> •
  <a href="#-cache-invalidation--purge-api">Purge API</a>
</p>

</div>

---

## 📖 Overview

**Titip** (*Indonesian for "to entrust / leave in care"*) is a high-throughput, low-allocation HTTP caching middleware for Go applications and API gateways.

Designed for high-load services, Titip eliminates backend load by serving cached responses with **low memory allocation**, atomic Redis Hash multi-variant negotiation, standard RFC-7234/9111 status code handling, and fail-open resilience.

---

## 🚀 Key Features

* **Low-Allocation & Memory Pooled**: Utilizes `sync.Pool` for byte buffers, response recorders, and LZ4 streaming decompression to drastically reduce heap churn and garbage collector pauses under heavy concurrent load.
* **🛡️ Fail-Open Resilience**: Storage outages (Redis down/timeout), decompression errors, or upstream panics **never crash your server or return 500 errors to users**. Requests transparently fall back to origin (`fwd=bypass`) or serve stale cache.
* **🔒 Data-Leak & Session Protection**: Cold URL misses execute independently without singleflight coalescing, preventing accidental broadcast of `Set-Cookie` or private session headers across concurrent unauthenticated callers.
* **🎯 RFC-7234 & RFC-9111 Section 4.2.3**: Implements the official Age & Freshness calculation standard (apparent age, corrected initial age, resident time, clock-skew correction, and multi-variant `Vary` header negotiation).
* **📊 RFC-9211 `Cache-Status` Observability**: Structured diagnostics (`Cache-Status: titip; hit; ttl=295`, `fwd=stale-while-revalidate`, `fwd=bypass`).
* **🔄 Flexible Cache Purge API**: Cloudflare-style single-target invalidation via Go API (`urls`, `tags`, or $O(1)$ epoch-based `purge_everything`).
* **🔌 Pluggable Architecture**: Standard `net/http` middleware with modular framework adapters and decoupled storage engines.

---

## 🏛️ Architecture: Two-Stage Split Lookup

Titip separates metadata from variant payloads to enable atomic multi-variant negotiation (`Vary`) and rapid short-circuiting with **zero redundant body I/O**:

```
[ Incoming Request ] ──► (Primary Key: Scheme + Host + Path + Filtered Query)
                               │
                               ▼
        ┌──────────────────────────────────────────────┐
        │ Stage 1: Fast Metadata Lookup (Redis Hash)   │
        │ Redis Key: titip:meta:<primaryKey>           │
        │ Fields: _index (pb.CacheMetadata), <variant> │
        └──────────────────────┬───────────────────────┘
                               │
        ┌──────────────────────┴───────────────────────┐
        │ Match Vary Variant & Evaluate Freshness      │
        └──────┬───────────────────────────────┬───────┘
               │                               │
      (Downstream 304 / HEAD)             (Cache Hit)
               │                               │
               ▼                               ▼
    ┌─────────────────────┐       ┌──────────────────────────────┐
    │ Serve 304 / Headers │       │ Stage 2: Fetch Variant Body  │
    │  (0 Redis Body I/O) │       │ Redis Key: titip:body:...    │
    └─────────────────────┘       └──────────────┬───────────────┘
                                                 │
                                                 ▼
                                  ┌──────────────────────────────┐
                                  │ LZ4 Decompress & Stream Body │
                                  │      (0 allocs / op)         │
                                  └──────────────────────────────┘
```

---

## 📦 Modules & Ecosystem

Titip is organized as a multi-module workspace. Each module is versioned independently:

| Module | Description | Documentation |
| --- | --- | --- |
| **`github.com/indragunawan/titip`** | Core caching middleware, state machine, and programmatic Purge API | [**Core Quickstart**](#-quickstart) |
| **`github.com/indragunawan/titip/adapter/caddy`** | Native Caddy HTTP middleware directive (`titip`) & Admin Purge API | [**Caddy Adapter Guide**](adapter/caddy/README.md) |
| **`github.com/indragunawan/titip/adapter/chi`** | Go-Chi HTTP router middleware adapter | [**Chi Adapter Guide**](adapter/chi/README.md) |
| **`github.com/indragunawan/titip/storage/redis`** | High-performance Redis 7+/8 distributed storage driver (`rueidis`) | [**Redis Storage Guide**](storage/redis/README.md) |
| **`github.com/indragunawan/titip/storage/redis/caddy`** | Guest storage module for Caddy (`titip.storage.redis`) | [**Caddy Redis Guide**](storage/redis/README.md) |

---

## 🛠️ Quickstart

Install the core package and Redis storage driver:

```bash
go get github.com/indragunawan/titip
go get github.com/indragunawan/titip/storage/redis
```

Wrap any standard `net/http` handler:

```go
package main

import (
 "context"
 "net/http"
 "time"

 "github.com/redis/rueidis"

 "github.com/indragunawan/titip"
 storageRedis "github.com/indragunawan/titip/storage/redis"
)

func main() {
 // 1. Initialize Redis Client
 client, err := rueidis.NewClient(rueidis.ClientOption{
  InitAddress: []string{"127.0.0.1:6379"},
 })
 if err != nil {
  panic(err)
 }
 defer client.Close()

 // 2. Create Titip Redis Storage
 store, err := storageRedis.New(client, storageRedis.WithKeyPrefix("titip:"))
 if err != nil {
  panic(err)
 }
 defer store.Close()

 // 3. Configure Titip Engine
 cache, err := titip.New(
  titip.WithStorage(store),
  titip.WithCacheStatusMode(titip.CacheStatusRFC9211),
  titip.WithOriginTimeout(10*time.Second),
 )
 if err != nil {
  panic(err)
 }
 defer cache.Close(context.Background())

 // 4. Wrap Standard HTTP Handler
 mux := http.NewServeMux()
 mux.HandleFunc("GET /api/data", func(w http.ResponseWriter, r *http.Request) {
  w.Header().Set("Content-Type", "application/json")
  // Cache publicly for 60 seconds, allow serving stale for 5 minutes during revalidation
  w.Header().Set("Cache-Control", "public, max-age=60, stale-while-revalidate=300")
  w.Header().Set("Cache-Tag", "catalog items")
  w.Write([]byte(`{"message": "hello from origin", "timestamp": "` + time.Now().String() + `"}`))
 })

 http.ListenAndServe(":8080", cache.Handler(mux))
}
```

---

## 🧹 Cache Invalidation & Purge API

Titip supports **URL Purging**, **Tag Purging**, and **Purge All** with safe soft-purging by default.

### Programmatic Go API

```go
// 1. Soft-Purge URL (serves stale while revalidating fresh in background)
err := cache.PurgeURL(ctx, "http://example.com/api/products?id=10", titip.WithSoftPurge())

// 2. Hard-Purge Tag (instantly evicts metadata + all variant bodies in Redis)
err := cache.PurgeTag(ctx, "catalog")

// 3. Purge All (O(1) instant global invalidation via epoch timestamp)
err := cache.PurgeAll(ctx, titip.WithSoftPurge())
```

---

## 🧩 Edge Side Includes (ESI)

Titip includes a streaming **Edge Side Includes (ESI 1.0)** engine with parallel fragment fetching, circular loop protection, and SSRF prevention.

### Supported ESI Tags & Syntax

| Syntax | Description |
| :--- | :--- |
| `<esi:include src="/fragment" />` | Self-closing fragment include. Fetched concurrently. |
| `<esi:include src="/fragment" alt="/fallback" onerror="continue" />` | Include with fallback URL on failure or silent omission (`onerror="continue"`). |
| `<esi:include src="...">Fallback HTML</esi:include>` | Paired include with inline fallback block. |
| `<!--esi <div>Visible only when ESI active</div> -->` | Unescapes enclosed HTML comments when ESI is enabled. |
| `<esi:remove><p>Placeholder</p></esi:remove>` | Strips placeholder content intended for non-ESI clients. |
| `<!--esi-comment text="..." -->` | Strips internal comments without emitting bytes. |

### ESI Configuration Options

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `Enabled` | `bool` | `false` | Master toggle for ESI parsing and fragment splicing. |
| `HeaderRequired` | `bool` | `false` | Process ESI only when origin sets `Surrogate-Control: content="ESI/1.0"`. |
| `InternalFetcher` | `InternalFetcherFunc` | `nil` | Custom hook for in-memory virtual subrequests (e.g. `titip.ESIHandlerFetcher(r)`). |
| `MaxDepth` | `uint32` | `3` | Maximum nesting depth for recursive ESI includes. |
| `MaxTimeout` | `time.Duration` | `30s` | Maximum time budget per fragment include fetch. |
| `MaxConcurrentRequests` | `int` | `8` | Maximum concurrent fetch goroutines per document. |
| `BlockPrivateIPs` | `bool` | `true` | SSRF guard blocking RFC 1918 / loopback / cloud metadata CIDRs. |
| `AllowedHosts` | `[]string` | `[]` | Whitelist for external domain includes (empty allows all public hosts). |
| `ForwardFragmentCookies`| `bool` | `true` | Forwards `Set-Cookie` headers from fragments to the client. |

---

## 📊 Observability & Metrics

Titip exports a unified, low-cardinality Prometheus counter metric:

```prometheus
# HELP titip_requests_total Total number of HTTP requests processed by Titip cache middleware.
# TYPE titip_requests_total counter
titip_requests_total{status="HIT"} 4125
titip_requests_total{status="MISS"} 102
titip_requests_total{status="STALE"} 18
titip_requests_total{status="BYPASS"} 5
titip_requests_total{status="REVALIDATED"} 12
titip_requests_total{status="ERROR"} 0
```

---

## 🛡️ Testing & Concurrency Standards

Titip enforces continuous race detection and zero-leak concurrency standards:

```bash
# Run all unit and concurrency stress tests with race detector
go test -race -count=50 -v ./...
```

---

## 🤝 Contributing

We welcome contributions for new framework adapters and storage drivers.

Please read our [Contributing Guide](CONTRIBUTING.md) for architectural guidelines, interface contracts, and testing standards.

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).
