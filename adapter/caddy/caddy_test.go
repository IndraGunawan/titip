package caddy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	caddymain "github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	_ "github.com/caddyserver/caddy/v2/modules/standard"
	"github.com/pierrec/lz4/v4"
	"go.uber.org/zap/zapcore"
)

func init() {
	caddymain.RegisterSlogHandlerFactory(func(h slog.Handler, core zapcore.Core, moduleID string) slog.Handler {
		return slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn})
	})
}

// parseAndProvisionHandler is a helper that parses a Caddyfile snippet and provisions the Handler.
func parseAndProvisionHandler(t testing.TB, caddyfileBlock string) (*Handler, func()) {
	t.Helper()
	d := caddyfile.NewTestDispenser(caddyfileBlock)
	var h Handler
	if err := h.UnmarshalCaddyfile(d); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	ctx, cancel := caddymain.NewContext(caddymain.Context{Context: context.Background()})
	if err := h.Provision(ctx); err != nil {
		cancel()
		t.Fatalf("provision error: %v", err)
	}

	cleanup := func() {
		_ = h.Cleanup()
		cancel()
	}
	return &h, cleanup
}

func TestCaddyHandler_MiddlewareExecution(t *testing.T) {
	t.Parallel()
	caddyfileInput := `titip {
		cache_status rfc9211
		background_fetch_timeout 5s
		storage test
	}`

	h, cleanup := parseAndProvisionHandler(t, caddyfileInput)
	defer cleanup()

	var upstreamCalls atomic.Int32
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		upstreamCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, `{"message":"caddy proxied content"}`)
		return nil
	})

	// 1. Initial request (miss)
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/caddy/test", nil)
	rec1 := httptest.NewRecorder()
	if err := h.ServeHTTP(rec1, req1, next); err != nil {
		t.Fatalf("serveHTTP error: %v", err)
	}

	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec1.Code)
	}
	if upstreamCalls.Load() != 1 {
		t.Fatalf("expected 1 upstream call, got %d", upstreamCalls.Load())
	}

	// 2. Second request (hit)
	rec2 := httptest.NewRecorder()
	if err := h.ServeHTTP(rec2, req1, next); err != nil {
		t.Fatalf("serveHTTP error: %v", err)
	}

	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec2.Code)
	}
	if upstreamCalls.Load() != 1 {
		t.Fatalf("cache hit should not call upstream: %d", upstreamCalls.Load())
	}
}

func TestCaddyHandler_ProvisionMissingStorage(t *testing.T) {
	t.Parallel()
	config := `titip {
		cache_status rfc9211
	}`

	d := caddyfile.NewTestDispenser(config)
	var h Handler
	if err := h.UnmarshalCaddyfile(d); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	ctx, cancel := caddymain.NewContext(caddymain.Context{Context: context.Background()})
	defer cancel()

	err := h.Provision(ctx)
	if err == nil {
		t.Fatalf("expected failure when provisioning Handler without storage")
	}
}

func TestCaddyHandler_ProvisionUnknownStorage(t *testing.T) {
	t.Parallel()
	config := `titip {
		storage memcached {
			address localhost:11211
		}
	}`

	d := caddyfile.NewTestDispenser(config)
	var h Handler
	err := h.UnmarshalCaddyfile(d)
	if err == nil {
		ctx, cancel := caddymain.NewContext(caddymain.Context{Context: context.Background()})
		defer cancel()
		err = h.Provision(ctx)
	}

	if err == nil {
		t.Fatalf("expected failure when unknown storage module 'memcached' is configured")
	}
}

// End-to-end live admin purge invalidation.
func TestAdminPurge_EndToEndLiveInvalidation(t *testing.T) {
	t.Parallel()
	caddyfileInput := `titip {
		storage test
	}`

	h, cleanup := parseAndProvisionHandler(t, caddyfileInput)
	defer cleanup()

	h.id = "test-admin-e2e"
	registerEngine(h.id, h.engine)
	defer unregisterEngine(h.id)

	var originCalls atomic.Int32
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		callNum := originCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("Cache-Tag", "catalog,items")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `{"call":%d,"data":"item-123"}`, callNum)
		return nil
	})

	testURL := "http://example.com/api/item-123"
	req := httptest.NewRequest(http.MethodGet, testURL, nil)

	// 1. Prime cache (Miss -> call #1)
	rec1 := httptest.NewRecorder()
	_ = h.ServeHTTP(rec1, req, next)
	if rec1.Body.String() != `{"call":1,"data":"item-123"}` {
		t.Fatalf("expected call 1, got %s", rec1.Body.String())
	}
	if originCalls.Load() != 1 {
		t.Fatalf("expected 1 origin call, got %d", originCalls.Load())
	}

	// 2. Cache Hit (Hit -> 0 origin calls)
	rec2 := httptest.NewRecorder()
	_ = h.ServeHTTP(rec2, req, next)
	if rec2.Body.String() != `{"call":1,"data":"item-123"}` {
		t.Fatalf("expected cached call 1, got %s", rec2.Body.String())
	}
	if originCalls.Load() != 1 {
		t.Fatalf("cache hit should not increment origin calls: %d", originCalls.Load())
	}

	// 3. Trigger Soft Purge via Admin API
	purgeBody := fmt.Sprintf(`{"urls": [%q], "soft": true}`, testURL)
	purgeReq := httptest.NewRequest(http.MethodPost, "/titip/purge", bytes.NewBufferString(purgeBody))
	purgeReq.Header.Set("Content-Type", "application/json")
	purgeRec := httptest.NewRecorder()

	_ = handleAdminPurge(purgeRec, purgeReq)
	if purgeRec.Code != http.StatusOK {
		t.Fatalf("admin purge failed with status %d: %s", purgeRec.Code, purgeRec.Body.String())
	}

	// 4. Subsequent request must synchronously fetch fresh data (call #2)
	rec3 := httptest.NewRecorder()
	_ = h.ServeHTTP(rec3, req, next)
	if rec3.Body.String() != `{"call":2,"data":"item-123"}` {
		t.Fatalf("expected fresh call 2 after purge, got %s", rec3.Body.String())
	}
	if originCalls.Load() != 2 {
		t.Fatalf("expected 2 origin calls after purge, got %d", originCalls.Load())
	}

	// 5. Test Tag Purge
	tagPurgeBody := `{"tags": ["catalog"], "soft": true}`
	tagPurgeReq := httptest.NewRequest(http.MethodPost, "/titip/purge", bytes.NewBufferString(tagPurgeBody))
	tagPurgeReq.Header.Set("Content-Type", "application/json")
	tagPurgeRec := httptest.NewRecorder()

	_ = handleAdminPurge(tagPurgeRec, tagPurgeReq)
	if tagPurgeRec.Code != http.StatusOK {
		t.Fatalf("tag purge failed with status %d", tagPurgeRec.Code)
	}

	// 6. Request after tag purge fetches fresh data (call #3)
	rec4 := httptest.NewRecorder()
	_ = h.ServeHTTP(rec4, req, next)
	if rec4.Body.String() != `{"call":3,"data":"item-123"}` {
		t.Fatalf("expected fresh call 3 after tag purge, got %s", rec4.Body.String())
	}
	if originCalls.Load() != 3 {
		t.Fatalf("expected 3 origin calls after tag purge, got %d", originCalls.Load())
	}
}

// TestCaddy_StandaloneStorageDirective_Fails verifies that storage modules cannot be configured standalone
func TestCaddy_StandaloneStorageDirective_Fails(t *testing.T) {
	t.Parallel()
	config := `:8080 {
		storage test
	}`

	adapter := caddyconfig.GetAdapter("caddyfile")
	if adapter != nil {
		_, _, err := adapter.Adapt([]byte(config), nil)
		if err == nil {
			t.Fatalf("expected failure when configuring standalone storage directive without titip block")
		}
	}
}

// TestCaddyfile_EndToEnd_GlobalOption_MultiHandleRoutes tests compiling a real Caddyfile
// with global titip options and route-wrapped multi-handle routes, verifying live cache HITs.
func TestCaddyfile_EndToEnd_GlobalOption_MultiHandleRoutes(t *testing.T) {
	var originHitCount int64
	originServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&originHitCount, 1)
		switch r.URL.Path {
		case "/api/time":
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintf(w, `{"hits":%d,"time":%d}`, atomic.LoadInt64(&originHitCount), time.Now().UnixNano())
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer originServer.Close()

	originURL, err := url.Parse(originServer.URL)
	if err != nil {
		t.Fatalf("parse origin url: %v", err)
	}

	caddyfileInput := fmt.Sprintf(`{
		admin off
		skip_install_trust
		log {
			output discard
		}
		titip {
			storage test
			cache_status rfc9211
			background_fetch_timeout 5s
		}
	}
	:18090 {
		route {
			titip

			handle /api/esi/caddy-static {
				header Content-Type "text/html; charset=utf-8"
				respond "<div>Caddy Native</div>" 200
			}

			handle {
				reverse_proxy %s
			}
		}
	}`, originURL.Host)

	cadAdapter := caddyconfig.GetAdapter("caddyfile")
	if cadAdapter == nil {
		t.Fatalf("caddyfile adapter not registered")
	}
	jsonBytes, warnings, err := cadAdapter.Adapt([]byte(caddyfileInput), map[string]any{"filename": "Caddyfile"})
	if err != nil {
		t.Fatalf("adapt failed: %v", err)
	}
	for _, w := range warnings {
		t.Logf("warning: %v", w)
	}

	// 1. Verify JSON AST correctness
	var root map[string]any
	if err := json.Unmarshal(jsonBytes, &root); err != nil {
		t.Fatalf("unmarshal adapted json: %v", err)
	}
	apps := root["apps"].(map[string]any)
	titipApp := apps["titip"].(map[string]any)
	storageMap := titipApp["storage"].(map[string]any)
	if storageMap["name"] != "test" {
		t.Errorf("expected storage.name='test', got %v", storageMap["name"])
	}

	// 2. Load and start Caddy instance
	cfg := new(caddymain.Config)
	if err := json.Unmarshal(jsonBytes, cfg); err != nil {
		t.Fatalf("unmarshal config: %v", err)
	}

	if err := caddymain.Run(cfg); err != nil {
		t.Fatalf("caddy run failed: %v", err)
	}
	defer func() {
		_ = caddymain.Stop()
	}()

	client := &http.Client{Timeout: 5 * time.Second}

	// Test 1: First request to /api/time -> MISS
	req1, _ := http.NewRequest(http.MethodGet, "http://localhost:18090/api/time", nil)
	resp1, err := client.Do(req1)
	if err != nil {
		t.Fatalf("request 1 failed: %v", err)
	}
	body1, _ := io.ReadAll(resp1.Body)
	_ = resp1.Body.Close()

	if resp1.StatusCode != http.StatusOK {
		t.Errorf("request 1 status = %d, want 200", resp1.StatusCode)
	}
	cacheStatus1 := resp1.Header.Get("Cache-Status")
	if !strings.Contains(cacheStatus1, "fwd=origin") && !strings.Contains(cacheStatus1, "miss") {
		t.Errorf("request 1 expected MISS/fwd=origin in Cache-Status, got %q", cacheStatus1)
	}

	// Test 2: Second request to /api/time -> HIT
	req2, _ := http.NewRequest(http.MethodGet, "http://localhost:18090/api/time", nil)
	resp2, err := client.Do(req2)
	if err != nil {
		t.Fatalf("request 2 failed: %v", err)
	}
	body2, _ := io.ReadAll(resp2.Body)
	_ = resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		t.Errorf("request 2 status = %d, want 200", resp2.StatusCode)
	}
	cacheStatus2 := resp2.Header.Get("Cache-Status")
	if !strings.Contains(cacheStatus2, "hit") {
		t.Errorf("request 2 expected HIT in Cache-Status, got %q", cacheStatus2)
	}
	if string(body1) != string(body2) {
		t.Errorf("request 2 body mismatch: want cached %q, got %q", string(body1), string(body2))
	}
	if atomic.LoadInt64(&originHitCount) != 1 {
		t.Errorf("expected originHitCount=1 (cached), got %d", atomic.LoadInt64(&originHitCount))
	}

	// Test 3: Request to Caddy Native respond
	req3, _ := http.NewRequest(http.MethodGet, "http://localhost:18090/api/esi/caddy-static", nil)
	resp3, err := client.Do(req3)
	if err != nil {
		t.Fatalf("request 3 failed: %v", err)
	}
	body3, _ := io.ReadAll(resp3.Body)
	_ = resp3.Body.Close()
	if !strings.Contains(string(body3), "Caddy Native") {
		t.Errorf("unexpected body for native respond: %q", string(body3))
	}
}

func TestCaddyHandler_SWR_PreservesReplacerContext(t *testing.T) {
	t.Parallel()
	h, cleanup := parseAndProvisionHandler(t, "titip { storage test }")
	defer cleanup()

	var replacerFound atomic.Bool
	var currentPayload atomic.Value
	currentPayload.Store("v1")

	nextHandler := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		if repl, ok := r.Context().Value(caddymain.ReplacerCtxKey).(*caddymain.Replacer); ok && repl != nil {
			replacerFound.Store(true)
		}
		w.Header().Set("Cache-Control", "public, max-age=1, stale-while-revalidate=10")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(currentPayload.Load().(string)))
		return nil
	})

	// 1. Prime cache
	req1 := httptest.NewRequest(http.MethodGet, "http://localhost:8080/swr-caddy-replacer", nil)
	repl1 := caddymain.NewReplacer()
	req1 = req1.WithContext(context.WithValue(req1.Context(), caddymain.ReplacerCtxKey, repl1))
	rec1 := httptest.NewRecorder()
	_ = h.ServeHTTP(rec1, req1, nextHandler)

	// Wait to enter SWR window
	time.Sleep(1100 * time.Millisecond)
	currentPayload.Store("v2")
	replacerFound.Store(false)

	// 2. Trigger SWR stale hit + async revalidation
	req2 := httptest.NewRequest(http.MethodGet, "http://localhost:8080/swr-caddy-replacer", nil)
	repl2 := caddymain.NewReplacer()
	req2 = req2.WithContext(context.WithValue(req2.Context(), caddymain.ReplacerCtxKey, repl2))
	rec2 := httptest.NewRecorder()
	_ = h.ServeHTTP(rec2, req2, nextHandler)

	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 on stale hit, got %d", rec2.Code)
	}
	if rec2.Body.String() != "v1" {
		t.Fatalf("expected stale v1 body, got %q", rec2.Body.String())
	}

	// Drain SWR
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.engine.Close(closeCtx); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	if !replacerFound.Load() {
		t.Fatal("expected background SWR goroutine to have caddymain.Replacer in request context")
	}
}

func TestCaddyHandler_OriginalRequestRewrite(t *testing.T) {
	t.Parallel()
	caddyfileInput := `titip {
		storage test
	}`

	h, cleanup := parseAndProvisionHandler(t, caddyfileInput)
	defer cleanup()

	var nextCalls atomic.Int32
	nextHandler := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		// Next handler (e.g. FrankenPHP) must see the rewritten URL "index.php"
		if r.URL.Path != "/index.php" {
			t.Errorf("expected nextHandler to receive rewritten path /index.php, got %s", r.URL.Path)
		}
		nextCalls.Add(1)

		// Simulating PHP front-controller reading OriginalRequestCtxKey or REQUEST_URI
		origReq, ok := r.Context().Value(caddyhttp.OriginalRequestCtxKey).(http.Request)
		page := "unknown"
		if ok && origReq.URL != nil {
			page = strings.TrimPrefix(origReq.URL.Path, "/")
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `{"page":%q}`, page)
		return nil
	})

	makeRewrittenReq := func(origPath string) *http.Request {
		req := httptest.NewRequest(http.MethodGet, "http://localhost:8080/index.php", nil)
		origURL, _ := url.Parse("http://localhost:8080" + origPath)
		origReq := *req
		origReq.URL = origURL
		origReq.RequestURI = origPath
		ctx := context.WithValue(req.Context(), caddyhttp.OriginalRequestCtxKey, origReq)
		return req.WithContext(ctx)
	}

	// 1. Request /articles (Miss -> next call 1)
	req1 := makeRewrittenReq("/articles")
	rec1 := httptest.NewRecorder()
	_ = h.ServeHTTP(rec1, req1, nextHandler)
	if rec1.Body.String() != `{"page":"articles"}` {
		t.Fatalf("expected page articles, got %s", rec1.Body.String())
	}
	if nextCalls.Load() != 1 {
		t.Fatalf("expected 1 next call, got %d", nextCalls.Load())
	}

	// 2. Request /products (Different original URL -> Must be a MISS, NOT collided with /articles)
	req2 := makeRewrittenReq("/products")
	rec2 := httptest.NewRecorder()
	_ = h.ServeHTTP(rec2, req2, nextHandler)
	if rec2.Body.String() != `{"page":"products"}` {
		t.Fatalf("expected page products (not collided with articles), got %s", rec2.Body.String())
	}
	if nextCalls.Load() != 2 {
		t.Fatalf("expected 2 next calls (separate cache entries), got %d", nextCalls.Load())
	}

	// 3. Request /articles again (Same original URL -> HIT, no next call)
	req3 := makeRewrittenReq("/articles")
	rec3 := httptest.NewRecorder()
	_ = h.ServeHTTP(rec3, req3, nextHandler)
	if rec3.Body.String() != `{"page":"articles"}` {
		t.Fatalf("expected cached page articles, got %s", rec3.Body.String())
	}
	if nextCalls.Load() != 2 {
		t.Fatalf("expected cache hit to not invoke next handler, got %d calls", nextCalls.Load())
	}
}

// TestCaddyHandler_DirectiveOrder_EncodePlainResponse verifies that because Titip is ordered After encode,
// Titip intercepts origin responses BEFORE Caddy's encode middleware compresses them.
// Therefore:
// 1. The cached variant body in Titip storage is stored as raw uncompressed bytes (never double-compressed or pre-encoded).
// 2. The client still receives the response encoded (e.g. gzip) via Caddy's encode middleware.
// 3. On subsequent cache HITs, Titip serves raw bytes to encode, which compresses them for the client.
func TestCaddyHandler_DirectiveOrder_EncodePlainResponse(t *testing.T) {
	plainContent := strings.Repeat("Hello Plain Uncompressed Text Payload For Titip Caching. ", 20)

	caddyfileInput := `{
		admin off
		skip_install_trust
		log {
			output discard
		}
		titip {
			storage test
			cache_status rfc9211
		}
	}
	:18095 {
		encode gzip
		titip

		handle /api/plain {
			header Cache-Control "public, max-age=60"
			header Content-Type "text/plain"
			respond "` + plainContent + `" 200
		}
	}`

	cadAdapter := caddyconfig.GetAdapter("caddyfile")
	if cadAdapter == nil {
		t.Fatalf("caddyfile adapter not registered")
	}
	jsonBytes, _, err := cadAdapter.Adapt([]byte(caddyfileInput), map[string]any{"filename": "Caddyfile"})
	if err != nil {
		t.Fatalf("adapt failed: %v", err)
	}

	cfg := new(caddymain.Config)
	if err := json.Unmarshal(jsonBytes, cfg); err != nil {
		t.Fatalf("unmarshal config: %v", err)
	}

	if err := caddymain.Run(cfg); err != nil {
		t.Fatalf("caddy run failed: %v", err)
	}
	defer func() { _ = caddymain.Stop() }()

	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			DisableCompression: true, // Disable automatic decompression so we inspect raw response bytes
		},
	}

	// 1. First request with Accept-Encoding: gzip (Cache MISS)
	req1, _ := http.NewRequest(http.MethodGet, "http://localhost:18095/api/plain", nil)
	req1.Header.Set("Accept-Encoding", "gzip")
	resp1, err := client.Do(req1)
	if err != nil {
		t.Fatalf("request 1 failed: %v", err)
	}
	body1, _ := io.ReadAll(resp1.Body)
	_ = resp1.Body.Close()

	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("request 1 status = %d, want 200", resp1.StatusCode)
	}
	if resp1.Header.Get("Content-Encoding") != "gzip" {
		t.Errorf("expected Content-Encoding 'gzip' to client, got %q", resp1.Header.Get("Content-Encoding"))
	}
	cacheStatus1 := resp1.Header.Get("Cache-Status")
	if !strings.Contains(cacheStatus1, "miss") && !strings.Contains(cacheStatus1, "fwd=origin") {
		t.Errorf("request 1 expected MISS, got %q", cacheStatus1)
	}

	// 2. Inspect underlying teststore to verify what Titip cached
	store := getLastTestStore()
	if store == nil {
		t.Fatalf("expected lastTestStore to be populated")
	}

	// The primary key for :18095/api/plain
	meta, _, err := store.GetMeta(context.Background(), "p=/api/plain:h=localhost:18095:m=GET")
	if err != nil {
		t.Fatalf("GetMeta error: %v", err)
	}
	if meta == nil {
		t.Fatalf("expected cached metadata in teststore, got nil")
	}

	// Get cached variant body
	varInfo, compBody, err := store.GetVariant(context.Background(), "p=/api/plain:h=localhost:18095:m=GET", "default")
	if err != nil {
		t.Fatalf("GetVariant error: %v", err)
	}
	if varInfo == nil || len(compBody) == 0 {
		t.Fatalf("expected cached variant and body in teststore")
	}

	// In the cached variant headers, Content-Encoding MUST NOT be gzip (it must be plain)
	if ceValues, exists := varInfo.ResponseHeaders["Content-Encoding"]; exists {
		t.Errorf("cached variant should NOT have Content-Encoding header stored, got %v", ceValues.Values)
	}

	// Titip stores bodies compressed with internal LZ4; decompressing it must yield the raw plainContent string!
	// (NOT a gzip-compressed stream)
	lz4Reader := lz4.NewReader(bytes.NewReader(compBody))
	decompressedBytes, err := io.ReadAll(lz4Reader)
	if err != nil {
		t.Fatalf("failed to decompress cached variant LZ4 body: %v", err)
	}
	if string(decompressedBytes) != plainContent {
		t.Errorf("cached body = %q, want plain raw content %q", string(decompressedBytes), plainContent)
	}

	// 3. Second request with Accept-Encoding: gzip (Cache HIT)
	req2, _ := http.NewRequest(http.MethodGet, "http://localhost:18095/api/plain", nil)
	req2.Header.Set("Accept-Encoding", "gzip")
	resp2, err := client.Do(req2)
	if err != nil {
		t.Fatalf("request 2 failed: %v", err)
	}
	body2, _ := io.ReadAll(resp2.Body)
	_ = resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("request 2 status = %d, want 200", resp2.StatusCode)
	}
	if resp2.Header.Get("Content-Encoding") != "gzip" {
		t.Errorf("expected Content-Encoding 'gzip' on cache HIT, got %q", resp2.Header.Get("Content-Encoding"))
	}
	cacheStatus2 := resp2.Header.Get("Cache-Status")
	if !strings.Contains(cacheStatus2, "hit") {
		t.Errorf("request 2 expected HIT, got %q", cacheStatus2)
	}
	// The compressed bytes delivered to client should match body1
	if !bytes.Equal(body1, body2) {
		t.Errorf("body1 (%d bytes) != body2 (%d bytes)", len(body1), len(body2))
	}

	// 4. Third request WITHOUT Accept-Encoding (Cache HIT without compression)
	// Because Titip stored the raw uncompressed body, it can serve a client that doesn't accept gzip!
	req3, _ := http.NewRequest(http.MethodGet, "http://localhost:18095/api/plain", nil)
	resp3, err := client.Do(req3)
	if err != nil {
		t.Fatalf("request 3 failed: %v", err)
	}
	body3, _ := io.ReadAll(resp3.Body)
	_ = resp3.Body.Close()

	if resp3.Header.Get("Content-Encoding") != "" {
		t.Errorf("expected no Content-Encoding for client without Accept-Encoding, got %q", resp3.Header.Get("Content-Encoding"))
	}
	if string(body3) != plainContent {
		t.Errorf("expected plain content for unencoded client, got %q", string(body3))
	}
}

func TestCaddyHandler_UseRewrittenURL_LiveExecution(t *testing.T) {
	t.Parallel()
	caddyfileInput := `titip {
		storage test
		use_rewritten_url true
	}`

	h, cleanup := parseAndProvisionHandler(t, caddyfileInput)
	defer cleanup()

	var downstreamCalls atomic.Int32
	downstream := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		downstreamCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, `{"status":"ok"}`)
		return nil
	})

	// Two different original paths rewritten to the same target: /canonical
	makeRewrittenReq := func(origPath string) *http.Request {
		req := httptest.NewRequest(http.MethodGet, "http://localhost:8080/canonical", nil)
		origURL, _ := url.Parse("http://localhost:8080" + origPath)
		origReq := *req
		origReq.URL = origURL
		origReq.RequestURI = origPath
		ctx := context.WithValue(req.Context(), caddyhttp.OriginalRequestCtxKey, origReq)
		return req.WithContext(ctx)
	}

	// 1. Request via /alias1 -> Miss (downstream call 1)
	req1 := makeRewrittenReq("/alias1")
	rec1 := httptest.NewRecorder()
	_ = h.ServeHTTP(rec1, req1, downstream)
	if downstreamCalls.Load() != 1 {
		t.Fatalf("expected 1 downstream call, got %d", downstreamCalls.Load())
	}

	// 2. Request via /alias2 -> Since use_rewritten_url=true, it uses /canonical -> HIT (0 downstream calls)
	req2 := makeRewrittenReq("/alias2")
	rec2 := httptest.NewRecorder()
	_ = h.ServeHTTP(rec2, req2, downstream)
	if downstreamCalls.Load() != 1 {
		t.Fatalf("expected cache HIT on rewritten path /canonical (downstreamCalls stays 1), got %d", downstreamCalls.Load())
	}
}

type testCaptureLogHandler struct {
	mu      sync.Mutex
	records []slog.Record
	level   slog.Level
}

func (c *testCaptureLogHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= c.level
}

func (c *testCaptureLogHandler) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, r.Clone())
	return nil
}

func (c *testCaptureLogHandler) WithAttrs(_ []slog.Attr) slog.Handler { return c }
func (c *testCaptureLogHandler) WithGroup(_ string) slog.Handler      { return c }

func TestCaddyHandler_ModuleLifecycleLogging(t *testing.T) {
	t.Parallel()

	capture := &testCaptureLogHandler{level: slog.LevelDebug}
	testLogger := slog.New(capture)

	d := caddyfile.NewTestDispenser(`titip {
		storage test
	}`)
	var h Handler
	if err := h.UnmarshalCaddyfile(d); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	h.logger = testLogger

	ctx, cancel := caddymain.NewContext(caddymain.Context{Context: context.Background()})
	defer cancel()

	if err := h.Provision(ctx); err != nil {
		t.Fatalf("provision error: %v", err)
	}

	capture.mu.Lock()
	records := slices.Clone(capture.records)
	capture.mu.Unlock()

	var foundInit, foundRegister bool
	for _, r := range records {
		if r.Level == slog.LevelInfo && r.Message == "module initialized" {
			foundInit = true
			var storageAttr string
			r.Attrs(func(a slog.Attr) bool {
				if a.Key == "storage" {
					storageAttr = a.Value.String()
				}
				return true
			})
			if storageAttr != "test" {
				t.Errorf("expected storage 'test', got %q", storageAttr)
			}
		}
		if r.Level == slog.LevelDebug && r.Message == "module registered" {
			foundRegister = true
			var idAttr string
			r.Attrs(func(a slog.Attr) bool {
				if a.Key == "id" {
					idAttr = a.Value.String()
				}
				return true
			})
			if idAttr != h.id {
				t.Errorf("expected id %q, got %q", h.id, idAttr)
			}
		}
	}

	if !foundInit {
		t.Errorf("expected INFO 'module initialized' log record")
	}
	if !foundRegister {
		t.Errorf("expected DEBUG 'module registered' log record")
	}

	// Test Cleanup
	if err := h.Cleanup(); err != nil {
		t.Fatalf("cleanup error: %v", err)
	}

	capture.mu.Lock()
	cleanupRecords := slices.Clone(capture.records)
	capture.mu.Unlock()

	var foundCleanup bool
	for _, r := range cleanupRecords {
		if r.Level == slog.LevelDebug && r.Message == "module cleaned up" {
			foundCleanup = true
			var idAttr string
			r.Attrs(func(a slog.Attr) bool {
				if a.Key == "id" {
					idAttr = a.Value.String()
				}
				return true
			})
			if idAttr != h.id {
				t.Errorf("expected cleanup id %q, got %q", h.id, idAttr)
			}
		}
	}
	if !foundCleanup {
		t.Errorf("expected DEBUG 'module cleaned up' log record")
	}
}
