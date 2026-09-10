package caddy

import (
	"bytes"
	"context"
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
	"github.com/caddyserver/caddy/v2/caddytest"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	_ "github.com/caddyserver/caddy/v2/modules/standard"
	"go.uber.org/zap/zapcore"
)

func init() {
	caddymain.RegisterSlogHandlerFactory(func(h slog.Handler, core zapcore.Core, moduleID string) slog.Handler {
		return slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn})
	})
}

func TestCaddy_BasicCaching(t *testing.T) {
	var originCalls atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("Content-Type", "text/plain")
		_, _ = fmt.Fprint(w, "hello from origin")
	}))
	defer origin.Close()

	tester := caddytest.NewTester(t)
	tester.InitServer(fmt.Sprintf(`
	{
		skip_install_trust
		admin localhost:2999
		http_port 9080
		https_port 9443
		grace_period 1ns
	}
	http://localhost:9080 {
		titip {
			storage test
			cache_status rfc9211
		}
		reverse_proxy %s
	}
	`, origin.Listener.Addr().String()), "caddyfile")

	// 1. Initial request -> Cache Miss (origin called)
	req1, err := http.NewRequest(http.MethodGet, "http://localhost:9080/data", nil)
	if err != nil {
		t.Fatalf("new request error: %v", err)
	}
	resp1, body1 := tester.AssertResponse(req1, 200, "hello from origin")
	status1 := resp1.Header.Get("Cache-Status")
	if !strings.Contains(status1, "fwd=uri-miss") {
		t.Errorf("expected Cache-Status on miss to contain fwd=uri-miss, got %q", status1)
	}
	if originCalls.Load() != 1 {
		t.Errorf("expected 1 origin call on miss, got %d", originCalls.Load())
	}

	// 2. Second request -> Cache Hit (origin NOT called)
	req2, err := http.NewRequest(http.MethodGet, "http://localhost:9080/data", nil)
	if err != nil {
		t.Fatalf("new request error: %v", err)
	}
	resp2, body2 := tester.AssertResponse(req2, 200, "hello from origin")
	if body1 != body2 {
		t.Errorf("body mismatch: %q vs %q", body1, body2)
	}
	status2 := resp2.Header.Get("Cache-Status")
	if !strings.Contains(status2, "hit") {
		t.Errorf("expected Cache-Status on hit to contain hit, got %q", status2)
	}
	if originCalls.Load() != 1 {
		t.Errorf("cache hit should not increment origin calls, got %d", originCalls.Load())
	}
}

func TestCaddy_AdminPurge(t *testing.T) {
	var originCalls atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := originCalls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("Cache-Tag", "catalog")
		_, _ = fmt.Fprintf(w, "call-%d", count)
	}))
	defer origin.Close()

	tester := caddytest.NewTester(t)
	tester.InitServer(fmt.Sprintf(`
	{
		skip_install_trust
		admin localhost:2999
		http_port 9080
		https_port 9443
		grace_period 1ns
	}
	http://localhost:9080 {
		titip {
			storage test
			cache_status rfc9211
		}
		reverse_proxy %s
	}
	`, origin.Listener.Addr().String()), "caddyfile")

	targetURL := "http://localhost:9080/purge-test"
	adminClient := &http.Client{}

	// 1. Miss -> primes cache with call-1
	req1, _ := http.NewRequest(http.MethodGet, targetURL, nil)
	resp1, body1 := tester.AssertResponse(req1, 200, "call-1")
	if !strings.Contains(resp1.Header.Get("Cache-Status"), "fwd=uri-miss") {
		t.Fatalf("expected miss on first request, got: %s", resp1.Header.Get("Cache-Status"))
	}
	if body1 != "call-1" {
		t.Fatalf("expected body call-1, got %s", body1)
	}

	// 2. Hit -> served from cache (still call-1)
	req2, _ := http.NewRequest(http.MethodGet, targetURL, nil)
	resp2, body2 := tester.AssertResponse(req2, 200, "call-1")
	if !strings.Contains(resp2.Header.Get("Cache-Status"), "hit") {
		t.Fatalf("expected hit on second request, got: %s", resp2.Header.Get("Cache-Status"))
	}
	if body2 != "call-1" {
		t.Fatalf("expected cached body call-1, got %s", body2)
	}
	if originCalls.Load() != 1 {
		t.Fatalf("expected origin calls to stay 1, got %d", originCalls.Load())
	}

	// 3. Purge by URL via real Caddy admin API POST /titip/purge
	purgePayload := `{"urls":["http://localhost:9080/purge-test"],"soft":false}`
	purgeReq, _ := http.NewRequest(http.MethodPost, "http://localhost:2999/titip/purge", bytes.NewBufferString(purgePayload))
	purgeReq.Header.Set("Content-Type", "application/json")

	purgeResp, err := adminClient.Do(purgeReq)
	if err != nil {
		t.Fatalf("admin purge request failed: %v", err)
	}
	defer purgeResp.Body.Close()
	if purgeResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(purgeResp.Body)
		t.Fatalf("admin purge status %d: %s", purgeResp.StatusCode, string(body))
	}

	// 4. Request after URL purge must be a MISS -> calls origin again (call-2)
	req3, _ := http.NewRequest(http.MethodGet, targetURL, nil)
	resp3, body3 := tester.AssertResponse(req3, 200, "call-2")
	if !strings.Contains(resp3.Header.Get("Cache-Status"), "fwd=uri-miss") {
		t.Errorf("expected miss after admin purge, got Cache-Status: %q", resp3.Header.Get("Cache-Status"))
	}
	if body3 != "call-2" {
		t.Errorf("expected fresh body call-2, got %s", body3)
	}
	if originCalls.Load() != 2 {
		t.Errorf("expected origin calls to be 2 after purge, got %d", originCalls.Load())
	}

	// 5. Request primes cache again with call-2 (confirm hit)
	req4, _ := http.NewRequest(http.MethodGet, targetURL, nil)
	resp4, body4 := tester.AssertResponse(req4, 200, "call-2")
	if !strings.Contains(resp4.Header.Get("Cache-Status"), "hit") {
		t.Fatalf("expected hit on second request, got: %s", resp4.Header.Get("Cache-Status"))
	}
	if body4 != "call-2" {
		t.Fatalf("expected cached body call-2, got %s", body4)
	}

	// 6. Purge by Tag via real Caddy admin API POST /titip/purge
	tagPurgePayload := `{"tags":["catalog"],"soft":false}`
	tagPurgeReq, _ := http.NewRequest(http.MethodPost, "http://localhost:2999/titip/purge", bytes.NewBufferString(tagPurgePayload))
	tagPurgeReq.Header.Set("Content-Type", "application/json")

	tagPurgeResp, err := adminClient.Do(tagPurgeReq)
	if err != nil {
		t.Fatalf("admin tag purge request failed: %v", err)
	}
	defer tagPurgeResp.Body.Close()
	if tagPurgeResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(tagPurgeResp.Body)
		t.Fatalf("admin tag purge status %d: %s", tagPurgeResp.StatusCode, string(body))
	}

	// 7. Request after Tag purge must be a MISS -> calls origin again (call-3)
	req5, _ := http.NewRequest(http.MethodGet, targetURL, nil)
	resp5, body5 := tester.AssertResponse(req5, 200, "call-3")
	if !strings.Contains(resp5.Header.Get("Cache-Status"), "fwd=uri-miss") {
		t.Errorf("expected miss after tag purge, got Cache-Status: %q", resp5.Header.Get("Cache-Status"))
	}
	if body5 != "call-3" {
		t.Errorf("expected fresh body call-3, got %s", body5)
	}
	if originCalls.Load() != 3 {
		t.Errorf("expected origin calls to be 3 after tag purge, got %d", originCalls.Load())
	}
}

func TestCaddy_ESI(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=60")
		switch r.URL.Path {
		case "/page":
			_, _ = fmt.Fprint(w, `<div>Page: <esi:include src="/fragment" /></div>`)
		case "/fragment":
			_, _ = fmt.Fprint(w, `<span>Dynamic Fragment</span>`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer origin.Close()

	tester := caddytest.NewTester(t)
	tester.InitServer(fmt.Sprintf(`
	{
		skip_install_trust
		admin localhost:2999
		http_port 9080
		https_port 9443
		grace_period 1ns
	}
	http://localhost:9080 {
		titip {
			storage test
			esi {
				enabled true
			}
		}
		reverse_proxy %s
	}
	`, origin.Listener.Addr().String()), "caddyfile")

	req, _ := http.NewRequest(http.MethodGet, "http://localhost:9080/page", nil)
	_, body := tester.AssertResponse(req, 200, "<div>Page: <span>Dynamic Fragment</span></div>")
	expected := "<div>Page: <span>Dynamic Fragment</span></div>"
	if body != expected {
		t.Errorf("ESI splicing mismatch: expected %q, got %q", expected, body)
	}
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

	if err := h.Provision(ctx); err == nil {
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
	if err := h.instance.Close(closeCtx); err != nil {
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

func TestCaddyHandler_UseRewrittenURL_True(t *testing.T) {
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
