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

Add the `titip` directive inside your site block:

```caddyfile
:8080 {
    route {
        titip {
            # Storage driver configuration
            storage redis {
                address localhost:6379
                key_prefix caddy:
                password {env.REDIS_PASSWORD}
            }

            # Response header mode: rfc9211 (default), simple, or none
            cache_status rfc9211

            # Origin timeout (e.g. 5s, 10s)
            origin_timeout 10s

            # Invalidate cached GET requests upon successful mutating POST/PUT/DELETE
            auto_invalidate_mutating_methods false

            # Custom Tag header name (defaults to "Cache-Tag")
            tag_header Cache-Tag

            # Cache key customization
            key {
                include_protocol false
                include_host true
                include_path true
                query whitelist id category page
                ignore_marketing_params true
                include_headers X-App-Version
                include_cookies currency
            }
        }
        reverse_proxy localhost:9000
    }
}
```

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

## 4. Multi-Site & Zero-Downtime Reloads

* **Multi-Site Routing**: Multiple `titip` directive blocks can run simultaneously across different virtual hosts or path prefixes.
* **Zero-Downtime Dynamic Reloads**: Implements `caddy.CleanerUpper` to cleanly drain background revalidation tasks and terminate connections during `caddy reload`.
