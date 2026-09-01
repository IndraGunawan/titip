# Titip Caddy Adapter (`http.handlers.titip`)

High-performance, RFC-compliant HTTP caching middleware module for the [Caddy Web Server](https://caddyserver.com/).

---

## 1. Installation & Building with `xcaddy`

Because Titip decouples storage from the core handler, build Caddy with both the Caddy adapter and your desired storage module (e.g. Redis):

```bash
xcaddy build \
  --with github.com/indragunawan/titip/adapter/caddy \
  --with github.com/indragunawan/titip/storage/redis/caddy
```

---

## 2. Caddyfile Syntax & Configuration

### Option A: Global Configuration (Recommended)

Configure shared storage and default cache policies in the global `{ ... }` block, then simply use `titip` in your site blocks:

```caddyfile
{
    # Global Titip Cache Configuration (Shared Redis Storage & Default Policies)
    titip {
        storage redis {
            address localhost:6379
            key_prefix caddy:
            password {env.REDIS_PASSWORD}
        }
        cache_status rfc9211
        origin_timeout 10s
        esi {
            enabled true
            max_depth 3
            max_timeout 5s
            max_concurrent_requests 8
            block_private_ips true
            forward_fragment_cookies true
        }
    }
}

:8080 {
    # Inherits global Titip settings and shared storage
    titip
    reverse_proxy localhost:9000
}
```

### Option B: Per-Site / Per-Route Configuration & Overrides

You can also configure `titip` locally or override specific settings per route:

```caddyfile
:8080 {
    # Route with custom storage prefix override:
    handle /api/* {
        titip {
            storage redis {
                address localhost:6379
                key_prefix api_cache:
            }
            cache_status rfc9211
        }
        reverse_proxy localhost:9000
    }

    # Catch-all using global defaults:
    handle {
        titip
        reverse_proxy localhost:9000
    }
}
```

### ESI Directive Reference

| Subdirective | Default | Description |
| :--- | :--- | :--- |
| `enabled <bool>` | `false` | Enable ESI fragment parsing and parallel splicing. |
| `header_required <bool>` | `false` | Process ESI only if origin sends `Surrogate-Control: content="ESI/1.0"`. |
| `max_depth <int>` | `3` | Maximum recursion depth for nested `<esi:include>`. |
| `max_timeout <duration>` | `30s` | Maximum time budget for fetching individual fragment includes. |
| `max_concurrent_requests <int>` | `8` | Maximum concurrent fetch goroutines per document. |
| `block_private_ips <bool>` | `true` | SSRF protection: block private/loopback/cloud metadata IPs on external includes. |
| `allowed_hosts <hosts...>` | `(all)` | Whitelist of allowed external hosts for ESI includes. |
| `forward_fragment_cookies <bool>` | `true` | Forward `Set-Cookie` headers from fragments to downstream client. |

---

## 3. Caddy Admin Purge API (`POST /titip/purge`)

Titip exposes an Admin API endpoint on Caddy's private admin port (default `:2019`) to invalidate cache entries across all active instances.

### Request Payloads (Single-Target Mutual Exclusivity)

#### A. Purge by URLs

```bash
curl -X POST http://localhost:2019/titip/purge \
  -H "Content-Type: application/json" \
  -d '{
    "urls": ["http://localhost:8080/api/item?id=1"],
    "soft": true
  }'
```

#### B. Purge by Tags

```bash
curl -X POST http://localhost:2019/titip/purge \
  -H "Content-Type: application/json" \
  -d '{
    "tags": ["catalog", "products"],
    "soft": true
  }'
```

#### C. Purge Everything

```bash
curl -X POST http://localhost:2019/titip/purge \
  -H "Content-Type: application/json" \
  -d '{
    "purge_everything": true,
    "soft": true
  }'
```

* `"soft": true` (default): Marks cache entries as stale, allowing backend revalidation in the background without causing origin stampedes.
* `"soft": false`: Hard-purges and evicts entries immediately from storage.

---

## 4. Multi-Site & Graceful Reloads

* **Multi-Site Routing**: Multiple `titip` directive blocks can run simultaneously across different virtual hosts or path prefixes.
* **Graceful Dynamic Reloads**: Implements `caddy.CleanerUpper` to cleanly drain background revalidation tasks and close connections during `caddy reload`.
