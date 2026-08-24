package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

var reqCounter atomic.Int64

func main() {
	mux := http.NewServeMux()

	// 1. Time Endpoint
	mux.HandleFunc("GET /api/time", func(w http.ResponseWriter, r *http.Request) {
		count := reqCounter.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=15")
		w.Header().Set("Cache-Tag", "time-tag")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"timestamp":       time.Now().Format(time.RFC3339Nano),
			"origin_exec_seq": count,
			"message":         "Response generated at mock upstream origin",
		})
	})

	// 2. Products Endpoint
	mux.HandleFunc("GET /api/products", func(w http.ResponseWriter, r *http.Request) {
		reqCounter.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=30")
		w.Header().Set("Cache-Tag", "products,catalog")
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"id": 1, "name": "Cloud Edge CDN", "price": 49.99},
			{"id": 2, "name": "High-Speed Cache Accelerator", "price": 99.00},
		})
	})

	// 3. Multi-Variant Vary Endpoint
	mux.HandleFunc("GET /api/vary", func(w http.ResponseWriter, r *http.Request) {
		lang := r.Header.Get("Accept-Language")
		if lang == "" {
			lang = "en-US"
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Vary", "Accept-Language, Accept-Encoding")
		w.Header().Set("Cache-Control", "public, max-age=30")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"language": lang,
			"greeting": fmt.Sprintf("Hello! Resolved language variant: %s", lang),
		})
	})

	// 4. Cached User Endpoint
	mux.HandleFunc("GET /api/user/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("Cache-Tag", fmt.Sprintf("users,user-%s", id))
		_ = json.NewEncoder(w).Encode(map[string]any{
			"user_id":   id,
			"username":  fmt.Sprintf("user_%s", id),
			"timestamp": time.Now().Format(time.RFC3339),
		})
	})

	// 5. Mutating User Endpoint
	mux.HandleFunc("POST /api/user/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":  "updated",
			"user_id": id,
			"message": "User record updated at origin",
		})
	})

	// 6. ESI Interactive Demo Page (Proxied & Spliced through Caddy Titip Middleware)
	mux.HandleFunc("GET /esi-demo", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("Surrogate-Control", "content=\"ESI/1.0\"")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, `<!DOCTYPE html>
<html>
<head>
    <title>Titip Caddy Adapter — Edge Side Includes (ESI) Demo</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 850px; margin: 40px auto; padding: 0 20px; line-height: 1.6; background: #0f172a; color: #e2e8f0; }
        .card { background: #1e293b; border-radius: 8px; padding: 20px; margin-bottom: 20px; border: 1px solid #334155; }
        .highlight { color: #38bdf8; font-weight: bold; }
        .tag { background: #047857; color: #a7f3d0; padding: 2px 8px; border-radius: 4px; font-size: 0.85em; }
        code { background: #0f172a; padding: 2px 6px; border-radius: 4px; color: #f43f5e; }
        a { color: #38bdf8; text-decoration: none; }
    </style>
</head>
<body>
    <div style="margin-bottom: 20px;"><a href="/">← Back to Dashboard</a></div>
    
    <!-- 1. Spliced Caddy Native Respond Fragment -->
    <esi:include src="/api/esi/caddy-static" />

    <!-- 2. Spliced Header Fragment (5-minute cache) -->
    <esi:include src="/api/esi/header" />

    <div class="card">
        <h3>Parent Template (Cached 60s in Caddy Titip)</h3>
        <p>This outer HTML skeleton is cached for <span class="highlight">60 seconds</span>. Caddy dynamically executes virtual subrequests in parallel and splices them into memory before returning to the browser.</p>
    </div>

    <!-- 2. Spliced 5-Second Differential TTL Clock Fragment -->
    <esi:include src="/api/esi/clock" />

    <!-- 3. Spliced Dynamic User Session Fragment (Sets Cookie & Never Cached) -->
    <esi:include src="/api/esi/user">
        <div class="card"><p><em>Fallback: User session loading...</em></p></div>
    </esi:include>

    <!-- 4. ESI Remove Tag (stripped by ESI engine) -->
    <esi:remove>
        <div class="card" style="border-color: #ef4444;"><p>⚠️ This notice only renders if ESI is disabled!</p></div>
    </esi:remove>

    <!-- 5. ESI Comment (stripped silently) -->
    <esi:comment text="Caddy internal rendering diagnostic marker" />

    <!-- 6. Spliced Footer via Inline Comment Unescaping (10-minute cache) -->
    <!--esi
    <esi:include src="/api/esi/footer" />
    -->
</body>
</html>`)
	})

	// ESI Subrequest Endpoints
	mux.HandleFunc("GET /api/esi/header", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "public, max-age=300")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, `<div class="card" style="border-left: 4px solid #38bdf8;">
    <h2>🧩 Header Navigation Component <span class="tag">TTL: 300s</span></h2>
    <p>Rendered independently. Origin timestamp: <code>`+time.Now().Format("15:04:05.000")+`</code></p>
    <div style="font-size:0.85em;margin-top:8px;"><a href="/api/esi/header" target="_blank" style="color:#38bdf8;">🔗 Open Fragment Directly: <code>/api/esi/header</code></a></div>
</div>`)
	})

	mux.HandleFunc("GET /api/esi/clock", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "public, max-age=5")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, `<div class="card" style="border-left: 4px solid #10b981;">
    <h3>⏱️ 5-Second Differential TTL Component <span class="tag">TTL: 5s</span></h3>
    <p>Clock: <code>`+time.Now().Format("15:04:05.000")+`</code></p>
    <p style="color:#94a3b8;font-size:0.9em;"><em>Freezes for 5 seconds in cache, then updates independently while parent page stays cached!</em></p>
    <div style="font-size:0.85em;margin-top:8px;"><a href="/api/esi/clock" target="_blank" style="color:#10b981;">🔗 Open Fragment Directly: <code>/api/esi/clock</code></a></div>
</div>`)
	})

	mux.HandleFunc("GET /api/esi/user", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		// Dynamic uncacheable session setting cookie
		w.Header().Set("Cache-Control", "private, no-store")
		http.SetCookie(w, &http.Cookie{
			Name:     "caddy_user_session",
			Value:    fmt.Sprintf("sess_%d", time.Now().Unix()),
			Path:     "/",
			HttpOnly: true,
		})
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, `<div class="card" style="border-left: 4px solid #ec4899;">
    <h3>👤 Live Private User Session <span class="tag" style="background:#831843;color:#fbcfe8;">Always Fresh (Set-Cookie)</span></h3>
    <p>User: <strong>Bob (Site Reliability Engineer)</strong> | Status: <strong>Online</strong></p>
    <p>Generated At: <code>`+time.Now().Format("15:04:05.000")+`</code></p>
    <p style="color:#94a3b8;font-size:0.9em;"><em>Bypasses cache on every request to forward live session cookies without leaking private data to other users.</em></p>
    <div style="font-size:0.85em;margin-top:8px;"><a href="/api/esi/user" target="_blank" style="color:#ec4899;">🔗 Open Fragment Directly: <code>/api/esi/user</code></a></div>
</div>`)
	})

	mux.HandleFunc("GET /api/esi/footer", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "public, max-age=600")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, `<div class="card" style="border-left: 4px solid #a855f7; text-align: center;">
    <p>© 2026 Titip Caching Engine • Powered by Caddy v2 & Zero-Allocation ESI Engine <span class="tag">TTL: 600s</span></p>
    <div style="font-size:0.85em;margin-top:8px;"><a href="/api/esi/footer" target="_blank" style="color:#a855f7;">🔗 Open Fragment Directly: <code>/api/esi/footer</code></a></div>
</div>`)
	})

	// Root overview
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = fmt.Fprint(w, `<!DOCTYPE html>
<html>
<head>
    <title>Titip Caddy Adapter — Interactive Demo</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 800px; margin: 40px auto; padding: 0 20px; line-height: 1.6; background: #0f172a; color: #e2e8f0; }
        h1 { color: #38bdf8; }
        .card { background: #1e293b; border-radius: 8px; padding: 20px; margin-bottom: 20px; border: 1px solid #334155; }
        code { background: #0f172a; padding: 2px 6px; border-radius: 4px; color: #f43f5e; }
        a { color: #38bdf8; text-decoration: none; font-weight: 500; }
        a:hover { text-decoration: underline; }
        .badge { background: #10b981; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.8em; margin-left: 6px; }
    </style>
</head>
<body>
    <h1>🚀 Titip Caddy Adapter Demo</h1>
    <div class="card">
        <h3>Interactive Pages & Demos</h3>
        <ul>
            <li><a href="/esi-demo" target="_blank"><code>GET /esi-demo</code></a> <span class="badge">ESI Full Page</span> — Complete assembled HTML page with all fragments spliced in parallel</li>
        </ul>
    </div>

    <div class="card" style="border-left: 4px solid #38bdf8;">
        <h3>🧩 Standalone ESI Fragment Endpoints</h3>
        <p>You can access each ESI fragment independently to see its raw output, headers, and TTL:</p>
        <ul>
            <li><a href="/api/esi/caddy-static" target="_blank"><code>GET /api/esi/caddy-static</code></a> <span class="badge" style="background:#b45309;">Caddy Native Respond</span> — Served directly by Caddy without origin</li>
            <li><a href="/api/esi/header" target="_blank"><code>GET /api/esi/header</code></a> <span class="badge">TTL: 300s</span> — Navigation header fragment with independent cache</li>
            <li><a href="/api/esi/clock" target="_blank"><code>GET /api/esi/clock</code></a> <span class="badge">TTL: 5s</span> — Short differential TTL clock (cached for 5s)</li>
            <li><a href="/api/esi/user" target="_blank"><code>GET /api/esi/user</code></a> <span class="badge" style="background:#ec4899;">Dynamic Session</span> — Live user profile setting <code>Set-Cookie</code> (uncached)</li>
            <li><a href="/api/esi/footer" target="_blank"><code>GET /api/esi/footer</code></a> <span class="badge">TTL: 600s</span> — Footer fragment (unescaped from HTML comment)</li>
        </ul>
    </div>

    <div class="card">
        <h3>Sample REST API Endpoints (Proxied on :8080)</h3>
        <ul>
            <li><a href="/api/time" target="_blank"><code>GET /api/time</code></a> — Freshness caching (max-age=15)</li>
            <li><a href="/api/products" target="_blank"><code>GET /api/products</code></a> — Product catalog (max-age=30)</li>
            <li><a href="/api/vary" target="_blank"><code>GET /api/vary</code></a> — Multi-Variant (Vary: Accept-Language)</li>
            <li><a href="/api/user/101" target="_blank"><code>GET /api/user/101</code></a> — User record (max-age=60)</li>
        </ul>
    </div>
    <div class="card">
        <h3>Quick Test ESI via Terminal</h3>
        <pre><code># 1. Test full ESI spliced document
curl -i http://localhost:8080/esi-demo

# 2. Test individual fragments directly
curl -i http://localhost:8080/api/esi/caddy-static
curl -i http://localhost:8080/api/esi/header
curl -i http://localhost:8080/api/esi/user
curl -i http://localhost:8080/api/esi/footer</code></pre>
    </div>
</body>
</html>`)
	})

	log.Println("Mock upstream origin server listening on :9000...")
	if err := http.ListenAndServe(":9000", mux); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
