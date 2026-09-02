# Titip Redis Storage Engine

Redis storage backend for the `titip` HTTP caching middleware, powered by [`rueidis`](https://github.com/redis/rueidis).

## Requirements

- **Redis 7.4+** (utilizes native Hash field expiration `HEXPIRE` and dynamic TTL extension `EXPIRE ... GT`)
- **Go 1.22+**

## Key Layout

| Key Pattern | Redis Type | Description |
| :--- | :--- | :--- |
| `<prefix>meta:<primaryKey>` | **Hash** | Stores metadata index and variant descriptors with per-variant field expiration. |
| `<prefix>body:<primaryKey>:<variantKey>` | **String** | Stores the compressed response payload for an individual variant. |
| `<prefix>tag:<tag>` | **Hash** | Indexes primary keys under surrogate tags with per-item field expiration. |

## Storage Architecture & Lifecycle

Titip decouples cache storage into a **Two-Stage Resolution** model to optimize I/O and memory usage:

1. **Two-Stage Resolution**:
   - **Stage 1 (Metadata Hash)**: Fast lookup of metadata and variant descriptors to negotiate `Vary` headers, check freshness, and serve conditional `304 Not Modified` responses with zero body payload I/O.
   - **Stage 2 (Variant Body)**: Compressed response payloads are fetched only when serving full cache hits (`200 OK`).

2. **Field-Level Expiration (`HEXPIRE`)**:
   - Individual variant fields in metadata hashes and primary key members in tag hashes have their own TTLs set via native `HEXPIRE`. When an item expires, Redis automatically evicts the field in the background.

3. **Dynamic TTL Extension (`EXPIRE ... GT`)**:
   - Metadata hashes and tag hashes dynamically extend their key-level TTL to match the expiration of the longest-surviving item using native `EXPIRE ... GT` without requiring read-modify-write operations.

## Usage

```go
package main

import (
 "context"
 "log"
 "time"

 "github.com/redis/rueidis"

 "github.com/indragunawan/titip"
 storageRedis "github.com/indragunawan/titip/storage/redis"
)

func main() {
 // 1. Initialize Rueidis client
 client, err := rueidis.NewClient(rueidis.ClientOption{
  InitAddress: []string{"127.0.0.1:6379"},
 })
 if err != nil {
  log.Fatalf("failed to connect to Redis: %v", err)
 }
 defer client.Close()

 // 2. Initialize Titip Redis Storage Engine
 store, err := storageRedis.New(
  client,
  storageRedis.WithKeyPrefix("myapp:cache:"),
 )
 if err != nil {
  log.Fatalf("failed to initialize storage: %v", err)
 }

 // 3. Attach storage to Titip
 cache, err := titip.New(
  titip.WithStorage(store),
  titip.WithOriginTimeout(5*time.Second),
  titip.WithStorageTimeout(1*time.Second),
 )
 if err != nil {
  log.Fatalf("failed to initialize titip: %v", err)
 }
 defer cache.Close(context.Background())
}
```

## Caddy Integration

To use Redis storage with Caddy, include `github.com/indragunawan/titip/storage/redis/caddy` when compiling Caddy with `xcaddy`:

```bash
xcaddy build \
  --with github.com/indragunawan/titip/adapter/caddy \
  --with github.com/indragunawan/titip/storage/redis/caddy
```

### Caddyfile Configuration

```caddyfile
:8080 {
    route {
        titip {
            storage redis {
                address localhost:6379
                key_prefix titip:
                password {env.REDIS_PASSWORD}
                db 0
            }
        }
    }
}
```
