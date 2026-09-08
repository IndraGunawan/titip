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
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	_ "github.com/caddyserver/caddy/v2/modules/standard"
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
	registerInstance(h.id, h.instance)
	defer unregisterInstance(h.id)

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
