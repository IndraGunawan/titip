# Titip Caddy Adapter — Turnkey Interactive Demo

Demonstrates native Caddy HTTP reverse proxy caching with dynamic Redis storage and the Caddy Admin Purge API (`POST /titip/purge`).

---

## 1. Quick Start (Single Command)

```bash
cd examples/caddy-demo
make run
```

This single command will automatically:

1. Start Redis 8 container (`titip-redis8`) if not running.
2. Build the custom Caddy binary with Titip and Redis plugins (`./cmd/caddy`).
3. Start the mock upstream origin server on `http://localhost:9000`.
4. Start Caddy reverse proxy on `http://localhost:8080` (Admin API on `:2019`).

---

### Manual Setup (Step by Step)

#### Step 1: Start Redis 8

```bash
make redis-up
```

#### Step 2: Build Custom Caddy Binary

```bash
make build-caddy
# Or manually: go build -o ./caddy ./cmd/caddy
```

#### Step 3: Run Mock Origin & Caddy

```bash
# In Terminal 1:
make origin

# In Terminal 2:
./caddy run --config Caddyfile
```

---

## 2. Testing HTTP Caching & ESI

### A. Edge Side Includes (ESI) Live Splicing

Open `http://localhost:8080/esi-demo` in your browser or inspect via `curl`:

```bash
curl -i http://localhost:8080/esi-demo
```

**What happens behind the scenes**:

1. Caddy intercepts the origin HTML containing `<esi:include>`, `<esi:remove>`, `<esi:comment>`, and `<!--esi ... -->`.
2. Concurrently dispatches in-process virtual subrequests to fetch `/api/esi/header`, `/api/esi/user`, and `/api/esi/footer`.
3. Slices and splices the components together into a single memory buffer with **$0\text{ heap allocations}$** on cache hits.
4. Forwards fragment `Set-Cookie` headers (`caddy_user_session`) directly to downstream clients.

---

### B. Inspect Cache Hits

```bash
# 1st Request (Cold Miss -> Origin Execution #1)
curl -i http://localhost:8080/api/time
# Header: Cache-Status: titip; fwd=uri-miss; fwd-status=200; stored; ttl=15

# 2nd Request (Cache Hit -> 0 origin calls)
curl -i http://localhost:8080/api/time
# Header: Cache-Status: titip; hit; ttl=13
```

---

### C. Trigger Invalidation via Caddy Admin Purge API (`:2019`)

Upstream backend applications or workers can purge cached items on demand:

```bash
# 1. Soft-Purge a specific URL
curl -i -X POST http://localhost:2019/titip/purge \
  -H "Content-Type: application/json" \
  -d '{"urls": ["http://localhost:8080/api/time"], "soft": true}'

# 2. Next request fetches fresh data from origin
curl -i http://localhost:8080/api/time
# Header: Cache-Status: titip; fwd=uri-miss; fwd-status=200; stored; detail=soft-refreshed

# 3. Purge by Tag
curl -i -X POST http://localhost:2019/titip/purge \
  -H "Content-Type: application/json" \
  -d '{"tags": ["catalog", "products"], "soft": true}'

# 4. Purge Everything
curl -i -X POST http://localhost:2019/titip/purge \
  -H "Content-Type: application/json" \
  -d '{"purge_everything": true, "soft": true}'
```

---

### D. Single-Target Mutual Exclusivity Enforcement

Mixing purge targets in a single request returns `400 Bad Request`:

```bash
curl -i -X POST http://localhost:2019/titip/purge \
  -H "Content-Type: application/json" \
  -d '{"urls": ["http://localhost:8080/api/time"], "tags": ["catalog"]}'
# Response: 400 Bad Request {"error":"specify exactly one of: urls, tags, or purge_everything"}
```

---

### E. Prometheus Metrics

Inspect live cache and ESI telemetry in your browser or with `curl`:

- **Browser direct link**: [http://localhost:8080/metrics](http://localhost:8080/metrics)
- **Admin API endpoint**:

  ```bash
  curl http://localhost:2019/metrics | grep titip
  ```

**Available Metric Series**:

- `titip_requests_total{status="hit|miss|stale_hit|revalidated|bypass|error"}`
- `titip_storage_duration_seconds{operation="...", backend="redis"}`
- `titip_esi_fragments_total{status="success|fallback|error|ssrf_blocked"}`
- `titip_esi_duration_seconds{mode="in_process|http"}`
