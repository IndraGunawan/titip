# Titip

[![Go Reference](https://pkg.go.dev/badge/github.com/indragunawan/titip.svg)](https://pkg.go.dev/github.com/indragunawan/titip)
[![Go Version](https://img.shields.io/github/go-mod/go-version/indragunawan/titip)](https://golang.org/)

**Titip** is a high-throughput, low-allocation HTTP caching middleware for Go applications and API gateways.

Designed for high-concurrency services, Titip reduces backend load by serving cached responses with minimal memory allocation, atomic Redis Hash multi-variant negotiation, RFC-compliant freshness calculations, and fail-open resilience.

## Key Features

* **Low-Allocation Design**: Reuses internal memory buffers and decompression streams to minimize heap allocations under high concurrency.
* **Fail-Open Resilience**: Storage outages, decompression errors, or upstream panics safely bypass to the origin handler (`fwd=bypass`) or serve stale cache without terminating the process.
* **Session & Privacy Protection**: Cold URL misses execute independently without singleflight coalescing, preventing accidental sharing of `Set-Cookie` or private session headers across concurrent callers.
* **RFC-7234, RFC-9111 & RFC-9213 Compliant**: Implements the official Age & Freshness calculation standard (apparent age, corrected initial age, resident time, clock-skew correction, and multi-variant `Vary` header negotiation).
* **Tiered Cache-Control (RFC 9213)**: Supports targeted header resolution (`Titip-Cache-Control` → `CDN-Cache-Control` → `Cache-Control`), allowing backends to configure edge caching independently from browser caching.
* **RFC-9211 `Cache-Status` Observability**: Structured diagnostics (`Cache-Status: titip; hit; ttl=295`, `fwd=stale`, `fwd=bypass`) with multi-tier cache chaining.
* **Granular Cache Purge API**: Invalidation via programmatic Go API (exact URL, wildcard prefixes, surrogate `Cache-Tag`, soft-purge, or namespace purge).
* **Pluggable Architecture**: Standard `net/http` middleware with modular framework adapters and decoupled storage engines.

## Architecture

Titip separates metadata from variant payloads to enable atomic multi-variant negotiation (`Vary`) and short-circuiting with **zero redundant body I/O**:

```
[ Incoming Request ] ──► (Primary Key: Scheme + Host + Path + Filtered Query)
                               │
                               ▼
        ┌──────────────────────────────────────────────┐
        │ Stage 1: Metadata Lookup (Redis Hash)        │
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
    │ (0 Body Payload I/O)│       │ Redis Key: titip:body:...    │
    └─────────────────────┘       └──────────────┬───────────────┘
                                                 │
                                                 ▼
                                  ┌──────────────────────────────┐
                                  │ LZ4 Decompress & Stream Body │
                                  └──────────────────────────────┘
```

## Modules & Ecosystem

Titip is organized as a multi-module workspace. Each module is versioned independently:

| Module | Description | Documentation |
| --- | --- | --- |
| **`github.com/indragunawan/titip`** | Core caching middleware, state machine, and programmatic Purge API | [**Core Quickstart**](#quickstart) |
| **`github.com/indragunawan/titip/adapter/caddy`** | Native Caddy HTTP middleware directive (`titip`) & Admin Purge API | [**Caddy Adapter Guide**](adapter/caddy/README.md) |
| **`github.com/indragunawan/titip/storage/redis`** | High-performance Redis 7.4+/8 distributed storage driver (`rueidis`) | [**Redis Storage Guide**](storage/redis/README.md) |
| **`github.com/indragunawan/titip/storage/redis/caddy`** | Guest storage module for Caddy (`titip.storage.redis`) | [**Caddy Redis Guide**](storage/redis/README.md#caddy-integration) |

## Quickstart

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

 http.ListenAndServe(":8080", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
  cache.ServeHTTP(w, r, mux)
 }))
}
```

## Configuration Reference

Pass any of the following functional options to `titip.New(...)`:

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `WithStorage(s)` | `storage.Storage` | *(Required)* | Storage backend implementation (e.g. `storage/redis`). |
| `WithCacheStatusMode(mode)` | `CacheStatusMode` | `CacheStatusSimpleToken` | Emitted status format (`CacheStatusRFC9211`, `CacheStatusSimpleToken`, or `CacheStatusNone`). |
| `WithKeyConfig(cfg)` | `KeyConfig` | `{}` (standard) | Primary cache key generation rules and query parameter filtering. |
| `WithTagHeaderName(name)` | `string` | `"Cache-Tag"` | Response header inspected for surrogate cache tags. |
| `WithOriginTimeout(d)` | `time.Duration` | `30s` | Maximum time budget for fetching responses from the origin. |
| `WithStorageTimeout(d)` | `time.Duration` | `1s` | Maximum time budget for storage reads/writes before fail-open bypass. |
| `WithRespectClientCacheControl()` | `bool` | `false` | When enabled, honors client request `Cache-Control: no-cache` / `no-store`. |
| `WithConvertHeadToGet(bool)` | `bool` | `true` | Converts origin `HEAD` cache misses to `GET` to prime the cache with body bytes. |
| `WithAutoInvalidateMutatingMethods()` | `bool` | `false` | RFC 9111 §4.4: Auto-purges URI cache when mutating requests (`POST`/`PUT`/`DELETE`) succeed. |
| `WithLogger(l)` | `*slog.Logger` | `slog.Default()` | Structured logger instance for diagnostic events. |
| `WithMetrics(reg)` | `prometheus.Registerer` | `nil` | Prometheus registry for cache and ESI telemetry. |
| `WithESI(opts...)` | `...esi.Option` | `disabled` | Edge Side Includes processing configuration and options. |

## Cache Key & Query Parameter Normalization

Titip constructs normalized cache keys directly without expensive hashing. Use `KeyConfig` to filter query parameters and strip tracking tags to prevent cache fragmentation:

```go
cache, err := titip.New(
    titip.WithStorage(store),
    titip.WithKeyConfig(titip.KeyConfig{
        // Strips marketing query parameters (utm_*, fbclid, gclid, mc_eid, etc.)
        ExcludeMarketingParams: true,
        // Whitelist specific query parameters to include (or use ExcludedQueryParams for a blacklist)
        IncludedQueryParams:    []string{"page", "sort", "filter"},
    }),
)
```

## Cache-Status Diagnostics

Titip supports three `Cache-Status` modes configured via `WithCacheStatusMode`:

### 1. `CacheStatusRFC9211` (Structured Header)

Emits structured diagnostics compliant with RFC 9211, supporting multi-tier cache chaining:

```http
Cache-Status: titip; hit; ttl=240
Cache-Status: "Fastly"; hit, titip; hit; ttl=240
```

### 2. `CacheStatusSimpleToken` (Single Token Header)

Emits a concise single-token status header:

| Token | Description |
| :--- | :--- |
| `HIT` | Served fresh directly from cache or matched downstream conditional `304 Not Modified`. |
| `MISS` | Cache miss: fetched from origin and stored in cache. |
| `EXPIRED` | Expired cache entry was synchronously revalidated with origin and refreshed (`200 OK`). |
| `REVALIDATED` | Expired cache entry was revalidated with origin via conditional headers (`304 Not Modified`). |
| `UPDATING` | Stale cache entry served immediately while revalidating asynchronously in the background (`stale-while-revalidate`). |
| `STALE` | Stale cache entry served as failover fallback due to origin error (`stale-if-error`). |
| `BYPASS` | Caching explicitly bypassed (mutating method, client `no-store`, Range request, WebSocket). |
| `DYNAMIC` | Evaluated for caching, but origin response is uncacheable (`Set-Cookie`, `private`, `no-store`). |

### 3. `CacheStatusNone`

Disables the `Cache-Status` response header completely.

## Cache Invalidation & Purge API

Titip provides a programmatic Go API for **Hierarchical Path Purging**, **Surrogate Tag Purging**, and **Namespace Invalidation**.

### Programmatic Go API

```go
// 1. Path Sweep (purges /api/products and all its query string variants)
err := cache.Purge(ctx, "/api/products")

// 2. Exact Query Invalidation (purges only ?id=10, leaves other queries intact)
err := cache.Purge(ctx, "http://example.com/api/products?id=10", titip.WithSoftPurge())

// 3. Directory Wildcard (purges all cached paths under /assets/)
err := cache.Purge(ctx, "/assets/*")

// 4. Surrogate Tag Invalidation (invalidates all cached entries matching the tag)
err := cache.PurgeTag(ctx, "catalog")

// 5. Namespace Invalidation (invalidates all cached entries under the configured prefix)
err := cache.PurgeAll(ctx)
```

## Tiered & Targeted Cache-Control (RFC 9213)

Titip supports **RFC 9213 Targeted Cache-Control**, allowing backend origins to define separate caching rules for the edge/proxy layer versus end-user browsers.

### Precedence Hierarchy (First Match Wins)

$$\text{\textbf{Titip-Cache-Control}} \;\longrightarrow\; \text{\textbf{CDN-Cache-Control (RFC 9213)}} \;\longrightarrow\; \text{\textbf{Cache-Control (RFC 9111)}}$$

```http
HTTP/1.1 200 OK
Titip-Cache-Control: public, max-age=86400, stale-while-revalidate=3600
Cache-Control: private, no-store
```

* **Titip (Intermediary)**: Caches the response in Redis for 24 hours (`max-age=86400`), shielding the origin from load.
* **Client (Browser)**: Receives `Cache-Control: private, no-store`, preventing sensitive data from persisting in local browser history.

## Edge Side Includes (ESI)

Titip includes a streaming **Edge Side Includes (ESI 1.0)** engine with parallel fragment fetching, circular loop protection, and SSRF prevention.

```go
cache, err := titip.New(
    titip.WithStorage(store),
    titip.WithESI(
        esi.WithInternalFetcher(esi.HandlerFetcher(router)),
        esi.WithMaxDepth(3),
        esi.WithMaxTimeout(5 * time.Second),
    ),
)
```

### Supported ESI Tags & Syntax

| Syntax | Description |
| :--- | :--- |
| `<esi:include src="/fragment" />` | Self-closing fragment include. Fetched concurrently. |
| `<esi:include src="/fragment" alt="/fallback" onerror="continue" />` | Include with fallback URL on failure or silent omission (`onerror="continue"`). |
| `<esi:include src="...">Fallback HTML</esi:include>` | Paired include with inline fallback block. |
| `<!--esi <div>Visible only when ESI active</div> -->` | Unescapes enclosed HTML comments when ESI is enabled. |
| `<esi:remove><p>Placeholder</p></esi:remove>` | Strips placeholder content intended for non-ESI clients. |
| `<!--esi-comment text="..." -->` | Strips internal comments without emitting bytes. |

### ESI Functional Options (`esi.Option`)

| Option Builder | Default | Description |
| :--- | :--- | :--- |
| `esi.WithHeaderRequired(bool)` | `false` | Process ESI only when origin sets `Surrogate-Control: content="ESI/1.0"`. |
| `esi.WithInternalFetcher(fn)` | `nil` | Custom hook for in-memory virtual subrequests (e.g. `esi.HandlerFetcher(r)`). |
| `esi.WithMaxDepth(uint32)` | `3` | Maximum nesting depth for recursive ESI includes. |
| `esi.WithMaxTimeout(duration)` | `30s` | Maximum time budget per fragment include fetch. |
| `esi.WithMaxConcurrentRequests(int)` | `8` | Maximum concurrent fetch goroutines per document. |
| `esi.WithAllowPrivateIPs(bool)` | `false` | SSRF guard: when false (default), blocks RFC 1918 / loopback / cloud metadata CIDRs. |
| `esi.WithAllowedHosts(...string)` | `[]` | Whitelist for external domain includes (empty allows all public hosts). |
| `esi.WithAllowPrivateIPsForAllowedHosts(bool)` | `false` | Permits private IPs specifically for explicitly allowed hosts. |
| `esi.WithMaxResponseSize(int64)` | `10MB` | Maximum allowed fragment body size in bytes. |
| `esi.WithDisableForwardCookies(bool)` | `false` | When false (default), forwards `Set-Cookie` headers from fragments to the client. |
| `esi.WithIncludeErrorMarker(string)` | `""` | HTML placeholder rendered on unhandled fetch errors. |

## Observability & Metrics

Titip exports comprehensive Prometheus metrics for request traffic, cache latencies, purge invalidations, and ESI fragment processing:

```go
// Register with a Prometheus registry:
cache, err := titip.New(
    titip.WithStorage(store),
    titip.WithRegisterer(prometheus.DefaultRegisterer),
)
```

### Exported Metrics

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `titip_requests_total` | Counter | `status` (`hit`, `miss`, `stale_hit`, `revalidated`, `bypass`, `error`) | Total HTTP requests processed by Titip caching middleware. |
| `titip_request_duration_seconds` | Histogram | `status` | Request latency distribution in seconds across cache statuses. |
| `titip_purges_total` | Counter | `type` (`url`, `tag`, `all`), `mode` (`hard`, `soft`), `status` (`success`, `error`) | Total purge operations executed by type and mode. |
| `titip_purged_entries_total` | Counter | `type` (`url`, `tag`, `all`), `mode` (`hard`, `soft`) | Total logical cache entries invalidated by purge operations. |
| `titip_esi_fragments_total` | Counter | `status` (`success`, `fallback`, `error`) | Total ESI fragment includes processed (enabled when ESI is active). |
| `titip_esi_duration_seconds` | Histogram | `mode` (`in_process`, `outbound`) | Latency distribution of ESI fragment fetching and document splicing. |

```prometheus
# Example Prometheus Scrape Output:
titip_requests_total{status="hit"} 4125
titip_requests_total{status="miss"} 102
titip_requests_total{status="stale_hit"} 18
titip_requests_total{status="revalidated"} 12
titip_requests_total{status="bypass"} 5
titip_requests_total{status="error"} 0

titip_purges_total{mode="soft",status="success",type="url"} 45
titip_purged_entries_total{mode="soft",type="url"} 45

titip_esi_fragments_total{status="success"} 230
titip_esi_duration_seconds_bucket{mode="in_process",le="0.005"} 225
```

## Testing & Concurrency Standards

Titip enforces continuous race detection and zero-leak concurrency standards:

```bash
# Run all unit and concurrency stress tests with race detector
go test -race -count=50 -v ./...
```

## Contributing

We welcome contributions for new framework adapters and storage drivers.

Please read our [Contributing Guide](CONTRIBUTING.md) for architectural guidelines, interface contracts, and testing standards.

## License

This project is licensed under the [MIT License](LICENSE).
