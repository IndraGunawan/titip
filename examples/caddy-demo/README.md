# Titip Caddy Adapter — Turnkey Interactive Demo

Demonstrates native Caddy HTTP reverse proxy caching with dynamic Redis storage and the Caddy Admin Purge API (`POST /titip/purge`).

---

## 1. Quick Start

### Step 1: Start Redis 8
```bash
# In the root repository directory
docker compose up -d
```
*(Starts `redis:8-alpine` bound to `localhost:6379`)*

### Step 2: Start the Mock Upstream Origin
```bash
cd examples/caddy-demo
go run main.go
```
*(Starts mock backend on `http://localhost:9000`)*

### Step 3: Build & Run Caddy with Titip
Build a custom Caddy binary incorporating the Titip adapter and Redis storage module using `xcaddy`:
```bash
xcaddy build \
  --with github.com/indragunawan/titip/adapter/caddy=../../adapter/caddy \
  --with github.com/indragunawan/titip/storage/redis/caddy=../../storage/redis/caddy
```

Then run Caddy with the provided `Caddyfile`:
```bash
./caddy run --config Caddyfile
```
Caddy will listen on `http://localhost:8080` (proxying to `:9000`) and the private Admin API on `http://localhost:2019`.

---

## 2. Testing HTTP Caching & Invalidation

### A. Inspect Cache Hits
```bash
# 1st Request (Cold Miss -> Origin Execution #1)
curl -i http://localhost:8080/api/time
# Header: Cache-Status: titip; fwd=uri-miss; fwd-status=200; stored; ttl=15

# 2nd Request (Cache Hit -> 0 origin calls)
curl -i http://localhost:8080/api/time
# Header: Cache-Status: titip; hit; ttl=13
```

---

### B. Trigger Invalidation via Caddy Admin Purge API (`:2019`)

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

### C. Single-Target Mutual Exclusivity Enforcement
Mixing purge targets in a single request returns `400 Bad Request`:
```bash
curl -i -X POST http://localhost:2019/titip/purge \
  -H "Content-Type: application/json" \
  -d '{"urls": ["http://localhost:8080/api/time"], "tags": ["catalog"]}'
# Response: 400 Bad Request {"error":"specify exactly one of: urls, tags, or purge_everything"}
```
