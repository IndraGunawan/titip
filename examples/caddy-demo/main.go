package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
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
		w.Header().Set("Cache-Control", "public, max-age=30, stale-while-revalidate=60")
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

	// 6. Purge Proxy Endpoint -> Forwards requests directly to Caddy Admin API on :2019/titip/purge
	mux.HandleFunc("POST /api/purge", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": fmt.Sprintf("failed to read request body: %v", err),
			})
			return
		}

		adminURL := "http://localhost:2019/titip/purge"
		adminReq, err := http.NewRequestWithContext(r.Context(), http.MethodPost, adminURL, bytes.NewReader(bodyBytes))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": fmt.Sprintf("failed to create admin request: %v", err),
			})
			return
		}
		adminReq.Header.Set("Content-Type", "application/json")

		client := &http.Client{Timeout: 5 * time.Second}
		resp, err := client.Do(adminReq)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error":    fmt.Sprintf("failed to connect to Caddy Admin API at %s: %v", adminURL, err),
				"guidance": "Make sure Caddy is running (e.g. ./caddy run --config Caddyfile)",
			})
			return
		}
		defer resp.Body.Close()

		respBytes, _ := io.ReadAll(resp.Body)
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(respBytes)
	})

	// 7. ESI Interactive Demo Page (Proxied & Spliced through Caddy Titip Middleware)
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
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}

		w.Header().Set("Cache-Control", "public, max-age=30")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = fmt.Fprint(w, `<!DOCTYPE html>
<html>
<head>
    <title>Titip Caddy Adapter — Interactive Demo</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 900px; margin: 30px auto; padding: 0 20px; line-height: 1.6; background: #0f172a; color: #e2e8f0; }
        h1 { color: #38bdf8; }
        .card { background: #1e293b; border-radius: 8px; padding: 20px; margin-bottom: 20px; border: 1px solid #334155; }
        code { background: #0f172a; padding: 2px 6px; border-radius: 4px; color: #f43f5e; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; }
        pre { background: #0f172a; padding: 12px; border-radius: 6px; overflow-x: auto; color: #e2e8f0; font-size: 0.9em; }
        a { color: #38bdf8; text-decoration: none; font-weight: 500; }
        a:hover { text-decoration: underline; }
        .badge { background: #10b981; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.8em; margin-left: 6px; }
        input, select, button { font-family: inherit; font-size: 0.95em; border-radius: 6px; }
        input[type="text"] { width: 100%; box-sizing: border-box; padding: 8px 12px; background: #0f172a; border: 1px solid #475569; color: #f8fafc; margin-top: 6px; margin-bottom: 12px; }
        button { background: #0284c7; color: white; border: none; padding: 8px 16px; cursor: pointer; font-weight: 600; transition: background 0.2s; }
        button:hover { background: #0369a1; }
        .btn-test { background: #334155; padding: 6px 12px; font-size: 0.85em; margin-right: 6px; margin-bottom: 6px; }
        .btn-test:hover { background: #475569; }
        .flex-row { display: flex; gap: 12px; align-items: center; }
        .box-req { background: #1e1e2e; border: 1px solid #3b4252; padding: 10px; border-radius: 6px; font-family: monospace; font-size: 0.85em; white-space: pre-wrap; margin-top: 10px; }
        .box-resp { background: #0b192c; border: 1px solid #1e3e62; padding: 10px; border-radius: 6px; font-family: monospace; font-size: 0.85em; white-space: pre-wrap; margin-top: 10px; }
    </style>
</head>
<body>
    <h1>🚀 Titip Caddy Adapter Demo</h1>

    <!-- 1. Interactive Purge Console (Posts to Caddy Admin :2019) -->
    <div class="card" style="border-left: 4px solid #f59e0b;">
        <h2 style="margin-top:0; color:#f59e0b;">🧹 Interactive Cache Purge Console (via Caddy Admin :2019)</h2>
        <p>Send an invalidation request directly to Caddy's Admin API endpoint (<code>POST http://localhost:2019/titip/purge</code>):</p>

        <div style="margin-bottom: 12px;">
            <label style="margin-right: 15px; font-weight: 600;">
                <input type="radio" name="purgeType" value="urls" checked onchange="updateForm()"> URL / Path Pattern
            </label>
            <label style="margin-right: 15px; font-weight: 600;">
                <input type="radio" name="purgeType" value="tags" onchange="updateForm()"> Surrogate Tag
            </label>
            <label style="font-weight: 600;">
                <input type="radio" name="purgeType" value="all" onchange="updateForm()"> Purge Everything
            </label>
        </div>

        <div id="targetInputDiv">
            <label id="targetLabel" style="font-size:0.9em; color:#94a3b8;">Target URLs/Paths (comma-separated):</label>
            <input type="text" id="targetInput" value="http://localhost:8080/api/time">
            <div style="font-size:0.8em; color:#64748b; margin-top:-6px; margin-bottom:10px;">
                Presets:
                <a href="javascript:void(0)" onclick="setPreset('http://localhost:8080/api/time')">/api/time</a> •
                <a href="javascript:void(0)" onclick="setPreset('http://localhost:8080/api/products')">/api/products</a> •
                <a href="javascript:void(0)" onclick="setPreset('http://localhost:8080/esi-demo')">/esi-demo</a> •
                <a href="javascript:void(0)" onclick="setPreset('/assets/*')">/assets/* (Wildcard)</a>
            </div>
        </div>

        <div class="flex-row" style="margin-bottom: 15px;">
            <label style="display:flex; align-items:center; gap:6px; cursor:pointer;">
                <input type="checkbox" id="softPurge" checked>
                <span><strong>Soft Purge</strong> (mark stale for background SWR revalidation)</span>
            </label>
        </div>

        <button onclick="executePurge()">🚀 Dispatch Purge to Caddy Admin</button>

        <div id="purgeResultDiv" style="display:none; margin-top: 15px;">
            <div style="font-size:0.85em; font-weight:600; color:#38bdf8;">📤 HTTP Request Sent to Caddy Admin:</div>
            <div id="reqPayloadBox" class="box-req"></div>

            <div style="font-size:0.85em; font-weight:600; color:#10b981; margin-top:10px;">📥 Caddy Admin Response:</div>
            <div id="respPayloadBox" class="box-resp"></div>
        </div>
    </div>

    <!-- 2. Live Cache Hit Inspector -->
    <div class="card" style="border-left: 4px solid #10b981;">
        <h3 style="margin-top:0; color:#10b981;">⚡ Live Cache Tester (Verify HIT / MISS & RFC-9211 Headers)</h3>
        <p>Click below to test live requests through Caddy (:8080) and inspect the returned <code>Cache-Status</code> headers:</p>

        <div>
            <button class="btn-test" onclick="testFetch('/api/time')">📡 Fetch /api/time</button>
            <button class="btn-test" onclick="testFetch('/api/products')">📡 Fetch /api/products</button>
            <button class="btn-test" onclick="testFetch('/api/user/101')">📡 Fetch /api/user/101</button>
            <button class="btn-test" onclick="testFetch('/esi-demo')">📡 Fetch /esi-demo</button>
        </div>

        <div id="fetchResultDiv" style="display:none; margin-top: 15px;">
            <div style="font-size:0.85em; font-weight:600; color:#38bdf8;">HTTP Headers & Status:</div>
            <div id="fetchHeaderBox" class="box-req"></div>
            <div style="font-size:0.85em; font-weight:600; color:#10b981; margin-top:8px;">Response Payload:</div>
            <div id="fetchBodyBox" class="box-resp"></div>
        </div>
    </div>

    <!-- 3. Multi-Variant Vary Tester (Vary: Accept-Language) -->
    <div class="card" style="border-left: 4px solid #8b5cf6;">
        <h3 style="margin-top:0; color:#a78bfa;">🌐 Multi-Variant Cache Tester (<code>Vary: Accept-Language</code>)</h3>
        <p>Titip stores multiple representation variants under the <strong>same primary URL</strong> without cache collisions. Test requests with different languages:</p>

        <div>
            <button class="btn-test" style="background:#5b21b6;" onclick="testVary('en-US')">🇺🇸 Accept-Language: en-US</button>
            <button class="btn-test" style="background:#1e40af;" onclick="testVary('fr-FR')">🇫🇷 Accept-Language: fr-FR</button>
            <button class="btn-test" style="background:#9f1239;" onclick="testVary('ja-JP')">🇯🇵 Accept-Language: ja-JP</button>
            <button class="btn-test" style="background:#065f46;" onclick="testVary('id-ID')">🇮🇩 Accept-Language: id-ID</button>
        </div>

        <div id="varyResultDiv" style="display:none; margin-top: 15px;">
            <div style="font-size:0.85em; font-weight:600; color:#38bdf8;">HTTP Headers & Status:</div>
            <div id="varyHeaderBox" class="box-req"></div>
            <div style="font-size:0.85em; font-weight:600; color:#10b981; margin-top:8px;">Response Payload:</div>
            <div id="varyBodyBox" class="box-resp"></div>
        </div>
    </div>

    <div class="card">
        <h3>Interactive Pages & ESI Demos</h3>
        <ul>
            <li><a href="/esi-demo" target="_blank"><code>GET /esi-demo</code></a> <span class="badge">ESI Full Page</span> — Complete assembled HTML page with all fragments spliced in parallel</li>
            <li><a href="/api/esi/caddy-static" target="_blank"><code>GET /api/esi/caddy-static</code></a> <span class="badge" style="background:#b45309;">Caddy Native Respond</span> — Served directly by Caddy without origin</li>
            <li><a href="/api/esi/header" target="_blank"><code>GET /api/esi/header</code></a> <span class="badge">TTL: 300s</span> — Navigation header fragment with independent cache</li>
            <li><a href="/api/esi/clock" target="_blank"><code>GET /api/esi/clock</code></a> <span class="badge">TTL: 5s</span> — Short differential TTL clock (cached for 5s)</li>
            <li><a href="/api/esi/user" target="_blank"><code>GET /api/esi/user</code></a> <span class="badge" style="background:#ec4899;">Dynamic Session</span> — Live user profile setting <code>Set-Cookie</code> (uncached)</li>
            <li><a href="/api/esi/footer" target="_blank"><code>GET /api/esi/footer</code></a> <span class="badge">TTL: 600s</span> — Footer fragment (unescaped from HTML comment)</li>
        </ul>
    </div>

    <div class="card">
        <h3>Quick Test via Terminal (cURL)</h3>
        <pre><code># 1. Test live endpoint (1st request = MISS, 2nd request = HIT)
curl -i http://localhost:8080/api/time

# 2. Invalidate via Caddy Admin API (:2019)
curl -i -X POST http://localhost:2019/titip/purge \
  -H "Content-Type: application/json" \
  -d '{"urls": ["http://localhost:8080/api/time"], "soft": true}'

# 3. Next request revalidates cleanly!
curl -i http://localhost:8080/api/time</code></pre>
    </div>

    <script>
    function updateForm() {
        const type = document.querySelector('input[name="purgeType"]:checked').value;
        const targetDiv = document.getElementById('targetInputDiv');
        const targetLabel = document.getElementById('targetLabel');
        const targetInput = document.getElementById('targetInput');

        if (type === 'urls') {
            targetDiv.style.display = 'block';
            targetLabel.innerText = 'Target URLs/Paths (comma-separated):';
            targetInput.value = 'http://localhost:8080/api/time';
        } else if (type === 'tags') {
            targetDiv.style.display = 'block';
            targetLabel.innerText = 'Target Surrogate Tags (comma-separated):';
            targetInput.value = 'catalog,products';
        } else {
            targetDiv.style.display = 'none';
        }
    }

    function setPreset(val) {
        document.getElementById('targetInput').value = val;
    }

    async function executePurge() {
        const type = document.querySelector('input[name="purgeType"]:checked').value;
        const inputVal = document.getElementById('targetInput').value.trim();
        const soft = document.getElementById('softPurge').checked;

        let payload = { soft: soft };
        if (type === 'urls') {
            payload.urls = inputVal.split(',').map(s => s.trim()).filter(Boolean);
        } else if (type === 'tags') {
            payload.tags = inputVal.split(',').map(s => s.trim()).filter(Boolean);
        } else if (type === 'all') {
            payload.purge_everything = true;
        }

        const reqJson = JSON.stringify(payload, null, 2);
        document.getElementById('reqPayloadBox').innerText = 'POST http://localhost:2019/titip/purge\nContent-Type: application/json\n\n' + reqJson;
        document.getElementById('respPayloadBox').innerText = 'Executing purge request...';
        document.getElementById('purgeResultDiv').style.display = 'block';

        try {
            const res = await fetch('/api/purge', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: reqJson
            });
            const text = await res.text();
            let formatted = text;
            try {
                formatted = JSON.stringify(JSON.parse(text), null, 2);
            } catch(e) {}
            document.getElementById('respPayloadBox').innerText = 'HTTP/1.1 ' + res.status + ' ' + res.statusText + '\n\n' + formatted;
        } catch (err) {
            document.getElementById('respPayloadBox').innerText = 'Error: ' + err.message;
        }
    }

    async function testFetch(path) {
        document.getElementById('fetchResultDiv').style.display = 'block';
        document.getElementById('fetchHeaderBox').innerText = 'Fetching ' + path + '...';
        document.getElementById('fetchBodyBox').innerText = '';

        try {
            const start = performance.now();
            const res = await fetch(path, { cache: 'default' });
            const duration = Math.round(performance.now() - start);
            const text = await res.text();

            let headersText = 'GET ' + path + ' -> HTTP ' + res.status + ' (' + duration + 'ms)\n';
            headersText += 'Cache-Status: ' + (res.headers.get('cache-status') || '(none)') + '\n';
            headersText += 'Cache-Control: ' + (res.headers.get('cache-control') || '(none)') + '\n';
            headersText += 'Cache-Tag: ' + (res.headers.get('cache-tag') || '(none)') + '\n';
            headersText += 'Age: ' + (res.headers.get('age') || '0') + '\n';
            headersText += 'Date: ' + (res.headers.get('date') || '(none)');

            document.getElementById('fetchHeaderBox').innerText = headersText;
            document.getElementById('fetchBodyBox').innerText = text;
        } catch(err) {
            document.getElementById('fetchHeaderBox').innerText = 'Error: ' + err.message;
        }
    }

    async function testVary(lang) {
        document.getElementById('varyResultDiv').style.display = 'block';
        document.getElementById('varyHeaderBox').innerText = 'Fetching /api/vary with Accept-Language: ' + lang + '...';
        document.getElementById('varyBodyBox').innerText = '';

        try {
            const start = performance.now();
            const res = await fetch('/api/vary', {
                headers: { 'Accept-Language': lang },
                cache: 'default'
            });
            const duration = Math.round(performance.now() - start);
            const text = await res.text();

            let headersText = 'GET /api/vary (Accept-Language: ' + lang + ') -> HTTP ' + res.status + ' (' + duration + 'ms)\n';
            headersText += 'Cache-Status: ' + (res.headers.get('cache-status') || '(none)') + '\n';
            headersText += 'Vary: ' + (res.headers.get('vary') || '(none)') + '\n';
            headersText += 'Cache-Control: ' + (res.headers.get('cache-control') || '(none)') + '\n';
            headersText += 'Age: ' + (res.headers.get('age') || '0') + '\n';
            headersText += 'Date: ' + (res.headers.get('date') || '(none)');

            document.getElementById('varyHeaderBox').innerText = headersText;
            document.getElementById('varyBodyBox').innerText = text;
        } catch(err) {
            document.getElementById('varyHeaderBox').innerText = 'Error: ' + err.message;
        }
    }
    </script>
</body>
</html>`)
	})

	log.Println("Mock upstream origin server listening on :9000...")
	if err := http.ListenAndServe(":9000", mux); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
