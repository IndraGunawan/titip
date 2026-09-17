# Titip Caddy Adapter — Interactive Demo

Demonstrates native Caddy HTTP reverse proxy caching with dynamic Redis storage and the Caddy Admin Purge API (`POST /titip/purge`).

## 1. Quick Start (Single Command)

```bash
cd examples/caddy-demo
make run
```

This single command will automatically:

1. Start **Redis 8**, **Prometheus**, and **Grafana** via Docker Compose.
2. Build the custom Caddy binary with Titip and Redis plugins (`./cmd/caddy`).
3. Start the mock upstream origin server on `http://localhost:9000`.
4. Start Caddy reverse proxy on `http://localhost:8080` (Admin API on `:2019`).

Services provisioned:
- **Caddy Proxy**: [http://localhost:8080](http://localhost:8080) (Admin API on `:2019`)
- **Grafana Dashboard**: [http://localhost:3000](http://localhost:3000) (preloaded Titip Dashboard, anonymous admin access)
- **Redis 8**: `localhost:6379`

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

## 2. Testing HTTP Caching & ESI

### A. Edge Side Includes (ESI) Live Splicing

Open `http://localhost:8080/esi-demo` in your browser or inspect via `curl`:

```bash
curl -i http://localhost:8080/esi-demo
```

**What happens behind the scenes**:

1. Caddy intercepts the origin HTML containing `<esi:include>`, `<esi:remove>`, `<esi:comment>`, and `<!--esi ... -->`.
2. Concurrently dispatches in-process virtual subrequests to fetch `/api/esi/header`, `/api/esi/user`, and `/api/esi/footer`.
3. Slices and splices the components together using pooled memory buffers.
4. Forwards fragment `Set-Cookie` headers (`caddy_user_session`) directly to downstream clients.

---

### B. Inspect Cache Hits & Multi-Variant `Vary` Header

#### 1. Basic Single-Variant Freshness

```bash
# 1st Request (Cold Miss -> Origin Execution #1)
curl -i http://localhost:8080/api/time
# Header: Cache-Status: titip; fwd=uri-miss; fwd-status=200; stored; ttl=15

# 2nd Request (Cache Hit -> 0 origin calls)
curl -i http://localhost:8080/api/time
# Header: Cache-Status: titip; hit; ttl=13
```

#### 2. Multi-Variant Secondary Keys (`Vary: Accept-Language`)

Titip supports RFC 9111 Section 4.1 content negotiation without cache pollution:

```bash
# 1. Fetch English variant (MISS -> stored as accept-language=en-us)
curl -i -H "Accept-Language: en-US" http://localhost:8080/api/vary
# Header: Cache-Status: titip; fwd=uri-miss; stored

# 2. Fetch French variant (MISS -> stored as accept-language=fr-fr under SAME primary key)
curl -i -H "Accept-Language: fr-FR" http://localhost:8080/api/vary
# Header: Cache-Status: titip; fwd=uri-miss; stored

# 3. Fetch English variant again (HIT -> serves English without origin call)
curl -i -H "Accept-Language: en-US" http://localhost:8080/api/vary
# Header: Cache-Status: titip; hit
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

### E. Observability: Grafana Dashboard & Prometheus Metrics

Titip provides full Prometheus telemetry out of the box with zero configuration:

- **Grafana Dashboard**: Open [http://localhost:3000](http://localhost:3000) (pre-provisioned, no login required)
  - **Panels included**:
    - **Cache Hit Ratio (%)**: Real-time gauge and trend showing cache effectiveness
    - **Request Rates by Status**: Real-time breakdown of `hit`, `miss`, `stale_hit`, `revalidated`, `bypass`, and `error`
    - **Latency Distributions**: p50 (median), p90, and p99 response times in milliseconds
    - **Hit vs Miss Latency**: Direct comparison showing sub-millisecond cache hits vs upstream miss penalties
    - **Cache Invalidation / Purge Rate**: Real-time tracking of URL and Tag purges
    - **Edge Side Includes (ESI)**: Fragment splicing rates and latency
- **Raw Metrics Endpoint**:
  - `http://localhost:8080/metrics`
  - Or via Caddy Admin: `curl http://localhost:2019/metrics | grep titip_`

---

### F. Running Load Tests (k6)

Simulate high-concurrency traffic across cached endpoints, multi-language `Vary` variants, ESI splicing, and background purges, and watch the graphs update live in Grafana:

```bash
make loadtest
# Or directly:
k6 run loadtest.js
```

Open [http://localhost:3000](http://localhost:3000) side-by-side with your terminal while running k6 to watch cache hit rates climb to 85%+, latencies drop to < 1ms on cache hits, and ESI fragments splice concurrently!
