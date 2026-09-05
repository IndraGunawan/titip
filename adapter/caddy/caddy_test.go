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
	"github.com/indragunawan/titip"
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

func TestCaddyHandler_UnmarshalCaddyfile(t *testing.T) {
	t.Parallel()
	config := `titip {
		cache_status RFC9211
		background_fetch_timeout 20s
		storage_timeout 5s
		storage test
	}`

	d := caddyfile.NewTestDispenser(config)
	var h Handler
	if err := h.UnmarshalCaddyfile(d); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if h.CacheStatus != "RFC9211" {
		t.Errorf("expected cache_status RFC9211, got %s", h.CacheStatus)
	}
	if h.BackgroundFetchTimeout != "20s" {
		t.Errorf("expected background_fetch_timeout 20s, got %s", h.BackgroundFetchTimeout)
	}
	if h.StorageTimeout != "5s" {
		t.Errorf("expected storage_timeout 5s, got %s", h.StorageTimeout)
	}
}

func TestCaddyHandler_ConvertHeadToGet_Unmarshal(t *testing.T) {
	t.Parallel()
	config := `titip {
		convert_head_to_get false
		storage test
	}`

	d := caddyfile.NewTestDispenser(config)
	var h Handler
	if err := h.UnmarshalCaddyfile(d); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if h.ConvertHeadToGet == nil || *h.ConvertHeadToGet != false {
		t.Fatalf("expected ConvertHeadToGet false, got %v", h.ConvertHeadToGet)
	}
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

// AC-3 / AC-4: End-to-End Live Admin Purge Invalidation
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

func TestCaddyHandler_KeyConfig_UnmarshalCaddyfile(t *testing.T) {
	t.Parallel()
	config := `titip {
		storage test
		use_rewritten_url true
		key {
			include_protocol false
			exclude_host true
			exclude_query_string false
			disable_query_string_sort false
			included_query_params id category page
			excluded_query_params tracking
			exclude_marketing_params true
			included_header_names X-App-Version Accept-Language
			included_cookie_names session_currency
			case_insensitive_path true
			included_query_param_values format json xml
		}
	}`

	d := caddyfile.NewTestDispenser(config)
	var h Handler
	if err := h.UnmarshalCaddyfile(d); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if h.UseRewrittenURL == nil || *h.UseRewrittenURL != true {
		t.Errorf("expected UseRewrittenURL true, got %v", h.UseRewrittenURL)
	}
	if h.Key == nil {
		t.Fatalf("expected Key config to be populated")
	}
	if h.Key.IncludeProtocol == nil || *h.Key.IncludeProtocol != false {
		t.Errorf("expected IncludeProtocol false, got %v", h.Key.IncludeProtocol)
	}
	if h.Key.ExcludeHost == nil || *h.Key.ExcludeHost != true {
		t.Errorf("expected ExcludeHost true, got %v", h.Key.ExcludeHost)
	}
	if h.Key.ExcludeQueryString == nil || *h.Key.ExcludeQueryString != false {
		t.Errorf("expected ExcludeQueryString false, got %v", h.Key.ExcludeQueryString)
	}
	if len(h.Key.IncludedQueryParams) != 3 || h.Key.IncludedQueryParams[0] != "id" {
		t.Errorf("unexpected IncludedQueryParams: %v", h.Key.IncludedQueryParams)
	}
	if len(h.Key.ExcludedQueryParams) != 1 || h.Key.ExcludedQueryParams[0] != "tracking" {
		t.Errorf("unexpected ExcludedQueryParams: %v", h.Key.ExcludedQueryParams)
	}
	if h.Key.ExcludeMarketingParams == nil || *h.Key.ExcludeMarketingParams != true {
		t.Errorf("expected ExcludeMarketingParams true, got %v", h.Key.ExcludeMarketingParams)
	}
	if len(h.Key.IncludedHeaderNames) != 2 || h.Key.IncludedHeaderNames[0] != "X-App-Version" {
		t.Errorf("unexpected IncludedHeaderNames: %v", h.Key.IncludedHeaderNames)
	}
	if len(h.Key.IncludedCookieNames) != 1 || h.Key.IncludedCookieNames[0] != "session_currency" {
		t.Errorf("unexpected IncludedCookieNames: %v", h.Key.IncludedCookieNames)
	}
	if h.Key.CaseInsensitivePath == nil || *h.Key.CaseInsensitivePath != true {
		t.Errorf("expected CaseInsensitivePath true, got %v", h.Key.CaseInsensitivePath)
	}
	if len(h.Key.IncludedQueryParamValues) != 1 || len(h.Key.IncludedQueryParamValues["format"]) != 2 {
		t.Errorf("unexpected IncludedQueryParamValues: %v", h.Key.IncludedQueryParamValues)
	}
}

func TestCaddyHandler_KeyConfig_LiveExecution(t *testing.T) {
	t.Parallel()
	caddyfileInput := `titip {
		storage test
		key {
			include_protocol false
			exclude_host true
			included_query_params id
			exclude_marketing_params true
		}
	}`

	h, cleanup := parseAndProvisionHandler(t, caddyfileInput)
	defer cleanup()

	var originCalls atomic.Int64
	downstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls := originCalls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `{"call":%d,"query":%q}`, calls, r.URL.RawQuery)
	})
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		downstream.ServeHTTP(w, r)
		return nil
	})

	// 1. Initial request with id=100 and utm_source=twitter (Origin Call #1)
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/api/items?id=100&utm_source=twitter", nil)
	rec1 := httptest.NewRecorder()
	if err := h.ServeHTTP(rec1, req1, next); err != nil {
		t.Fatalf("serve failed: %v", err)
	}
	if originCalls.Load() != 1 {
		t.Fatalf("expected 1 origin call, got %d", originCalls.Load())
	}

	// 2. Second request with id=100 and utm_source=google & fbclid=123 (Must HIT cache because utm/fbclid ignored, only id kept)
	req2 := httptest.NewRequest(http.MethodGet, "http://different-host.com/api/items?id=100&utm_source=google&fbclid=abc", nil)
	rec2 := httptest.NewRecorder()
	if err := h.ServeHTTP(rec2, req2, next); err != nil {
		t.Fatalf("serve failed: %v", err)
	}
	if originCalls.Load() != 1 {
		t.Fatalf("expected cache HIT (1 origin call), got %d", originCalls.Load())
	}
	if rec2.Body.String() != `{"call":1,"query":"id=100&utm_source=twitter"}` {
		t.Fatalf("expected cached response from call 1, got %s", rec2.Body.String())
	}
}

func TestCaddyHandler_UnmarshalCaddyfile_ESI(t *testing.T) {
	t.Parallel()
	config := `titip {
		cache_status simple
		storage test
		esi {
			enabled true
			header_required false
			max_depth 3
			max_timeout 15s
			max_concurrent_requests 4
			block_private_ips true
			allowed_hosts cdn.example.com api.partner.com
			max_response_size 5MB
			forward_fragment_cookies true
			error_marker "<!-- error -->"
		}
	}`

	d := caddyfile.NewTestDispenser(config)
	var h Handler
	if err := h.UnmarshalCaddyfile(d); err != nil {
		t.Fatalf("failed to unmarshal caddyfile with esi: %v", err)
	}

	if h.ESI == nil {
		t.Fatalf("expected ESI configuration to be populated")
	}
	if h.ESI.Enabled == nil || !*h.ESI.Enabled {
		t.Errorf("expected ESI.Enabled to be true")
	}
	if h.ESI.MaxDepth == nil || *h.ESI.MaxDepth != 3 {
		t.Errorf("expected MaxDepth 3, got %v", h.ESI.MaxDepth)
	}
	if h.ESI.MaxTimeout != "15s" {
		t.Errorf("expected MaxTimeout 15s, got %s", h.ESI.MaxTimeout)
	}
	if h.ESI.MaxConcurrentRequests == nil || *h.ESI.MaxConcurrentRequests != 4 {
		t.Errorf("expected MaxConcurrentRequests 4, got %v", h.ESI.MaxConcurrentRequests)
	}
	if len(h.ESI.AllowedHosts) != 2 || h.ESI.AllowedHosts[0] != "cdn.example.com" {
		t.Errorf("unexpected allowed hosts: %v", h.ESI.AllowedHosts)
	}
	if h.ESI.MaxResponseSize != "5MB" {
		t.Errorf("expected MaxResponseSize 5MB, got %s", h.ESI.MaxResponseSize)
	}
	if h.ESI.ErrorMarker != "<!-- error -->" {
		t.Errorf("expected ErrorMarker <!-- error -->, got %s", h.ESI.ErrorMarker)
	}
}

func TestCaddyHandler_ESI_MultiRouteResolution(t *testing.T) {
	t.Parallel()
	caddyfileInput := `titip {
		storage test
		esi {
			enabled true
		}
	}`

	h, cleanup := parseAndProvisionHandler(t, caddyfileInput)
	defer cleanup()

	// 1. Define a root Caddy server handler that routes both /page and /api/fragment
	rootServer := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		switch r.URL.Path {
		case "/page":
			w.Header().Set("Content-Type", "text/html")
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `<div>Page Content: <esi:include src="/api/fragment" /></div>`)
		case "/api/fragment":
			w.Header().Set("Content-Type", "text/html")
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `<span>Dynamic Spliced Fragment</span>`)
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = fmt.Fprint(w, `404 Not Found`)
		}
		return nil
	})

	// 2. Next handler represents only Route A's downstream (which ONLY knows /page)
	routeANext := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		if r.URL.Path == "/page" {
			w.Header().Set("Content-Type", "text/html")
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `<div>Page Content: <esi:include src="/api/fragment" /></div>`)
			return nil
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprint(w, `Route A does not know this path`)
		return nil
	})

	req := httptest.NewRequest(http.MethodGet, "http://example.com/page", nil)
	ctx := context.WithValue(req.Context(), caddyhttp.ServerCtxKey, rootServer)
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	if err := h.ServeHTTP(rec, req, routeANext); err != nil {
		t.Fatalf("serve failed: %v", err)
	}

	expected := `<div>Page Content: <span>Dynamic Spliced Fragment</span></div>`
	if rec.Body.String() != expected {
		t.Fatalf("ESI multi-route splicing failed.\nExpected: %s\nGot:      %s", expected, rec.Body.String())
	}
}

func TestCaddyHandler_ESI_ConcurrentReplacerSafety(t *testing.T) {
	t.Parallel()
	caddyfileInput := `titip {
		storage test
		esi {
			enabled true
		}
	}`

	h, cleanup := parseAndProvisionHandler(t, caddyfileInput)
	defer cleanup()

	rootServer := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		switch r.URL.Path {
		case "/multi-frag":
			w.Header().Set("Content-Type", "text/html")
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `<doc><esi:include src="/f1" /><esi:include src="/f2" /><esi:include src="/f3" /><esi:include src="/f4" /></doc>`)
		case "/f1":
			w.Header().Set("Content-Type", "text/html")
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `[F1]`)
		case "/f2":
			w.Header().Set("Content-Type", "text/html")
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `[F2]`)
		case "/f3":
			w.Header().Set("Content-Type", "text/html")
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `[F3]`)
		case "/f4":
			w.Header().Set("Content-Type", "text/html")
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `[F4]`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
		return nil
	})

	var wg sync.WaitGroup
	concurrentRequests := 20
	for range concurrentRequests {
		wg.Go(func() {
			req := httptest.NewRequest(http.MethodGet, "http://example.com/multi-frag", nil)
			repl := caddymain.NewReplacer()
			ctx := context.WithValue(req.Context(), caddymain.ReplacerCtxKey, repl)
			ctx = context.WithValue(ctx, caddyhttp.ServerCtxKey, rootServer)
			req = req.WithContext(ctx)

			rec := httptest.NewRecorder()
			if err := h.ServeHTTP(rec, req, rootServer); err != nil {
				t.Errorf("serve failed: %v", err)
				return
			}

			expected := `<doc>[F1][F2][F3][F4]</doc>`
			if rec.Body.String() != expected {
				t.Errorf("unexpected body: got %q, want %q", rec.Body.String(), expected)
			}
		})
	}
	wg.Wait()
}

func TestCaddyHandler_ESI_Subrequest404_FallbackToOutbound(t *testing.T) {
	t.Parallel()

	var outboundCalls atomic.Int32
	extServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/remote-alb-part" {
			outboundCalls.Add(1)
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `<span>From ALB Upstream</span>`)
			return
		}
		http.NotFound(w, r)
	}))
	defer extServer.Close()

	caddyfileInput := `titip {
		storage test
		esi {
			enabled true
			block_private_ips false
		}
	}`

	h, cleanup := parseAndProvisionHandler(t, caddyfileInput)
	defer cleanup()

	// Root server only knows /local-part, but returns 404 for /remote-alb-part
	rootServer := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		switch r.URL.Path {
		case "/local-part":
			w.Header().Set("Content-Type", "text/html")
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `<span>Local In-Process</span>`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
		return nil
	})

	routeNext := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		if r.URL.Path == "/hybrid-page" {
			w.Header().Set("Content-Type", "text/html")
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `<div><esi:include src="/local-part" /> + <esi:include src="/remote-alb-part" /></div>`)
			return nil
		}
		w.WriteHeader(http.StatusNotFound)
		return nil
	})

	extURL := strings.TrimPrefix(extServer.URL, "http://")
	req := httptest.NewRequest(http.MethodGet, "http://"+extURL+"/hybrid-page", nil)
	ctx := context.WithValue(req.Context(), caddyhttp.ServerCtxKey, rootServer)
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	if err := h.ServeHTTP(rec, req, routeNext); err != nil {
		t.Fatalf("serve failed: %v", err)
	}

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	expected := `<div><span>Local In-Process</span> + <span>From ALB Upstream</span></div>`
	if rec.Body.String() != expected {
		t.Fatalf("unexpected body.\nExpected: %s\nGot:      %s", expected, rec.Body.String())
	}

	if outboundCalls.Load() != 1 {
		t.Errorf("expected 1 outbound call to ALB for 404 subrequest, got %d", outboundCalls.Load())
	}
}

// TestCaddyGlobalOption_Adapt verifies that { titip { ... } } in global options
// compiles properly to apps.titip in Caddy JSON.
func TestCaddyGlobalOption_Adapt(t *testing.T) {
	t.Parallel()

	caddyfileInput := `{
		skip_install_trust
		titip {
			storage test
			cache_status rfc9211
			background_fetch_timeout 5s
			respect_client_cache_control false
			auto_invalidate_mutating_methods true
			esi {
				enabled true
				max_depth 3
				max_timeout 5s
			}
			key {
				include_protocol true
				query whitelist a b
			}
		}
	}
	:8080 {
		route {
			titip
			respond "Hello Global"
		}
	}`

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

	var root map[string]any
	if err := json.Unmarshal(jsonBytes, &root); err != nil {
		t.Fatalf("unmarshal adapted json: %v", err)
	}

	apps, ok := root["apps"].(map[string]any)
	if !ok {
		t.Fatalf("expected apps map in adapted json, got %v", root)
	}

	titipApp, ok := apps["titip"].(map[string]any)
	if !ok {
		t.Fatalf("expected titip app in apps map, got %v", apps)
	}

	storageMap := titipApp["storage"].(map[string]any)
	if storageMap["name"] != "test" {
		t.Errorf("expected storage.name='test', got %v", storageMap["name"])
	}
	if titipApp["cache_status"] != "rfc9211" {
		t.Errorf("expected cache_status rfc9211, got %v", titipApp["cache_status"])
	}
	if titipApp["background_fetch_timeout"] != "5s" {
		t.Errorf("expected background_fetch_timeout 5s, got %v", titipApp["background_fetch_timeout"])
	}
}

// TestCaddyGlobalOption_InheritanceAndOverride tests App provisioning and Handler inheritance via full Caddyfile.
func TestCaddyGlobalOption_InheritanceAndOverride(t *testing.T) {
	caddyfileInput := `{
		admin off
		skip_install_trust
		log {
			output discard
		}
		titip {
			storage test
			cache_status rfc9211
			background_fetch_timeout 10s
			respect_client_cache_control false
			auto_invalidate_mutating_methods true
			esi {
				enabled true
				max_depth 3
			}
		}
	}
	:18091 {
		route {
			titip
			respond "Hello Global Inherit" 200
		}
	}`

	cadAdapter := caddyconfig.GetAdapter("caddyfile")
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

	resp, err := http.Get("http://localhost:18091")
	if err != nil {
		t.Fatalf("http get failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status code = %d, want 200", resp.StatusCode)
	}
}

// TestDeepMerge_ESIAndKeyConfig_Caddyfile verifies that route-level overrides
// in Caddyfile merge cleanly on top of global defaults during full Caddyfile compilation.
func TestDeepMerge_ESIAndKeyConfig_Caddyfile(t *testing.T) {
	caddyfileInput := `{
		admin off
		skip_install_trust
		log {
			output discard
		}
		titip {
			storage test
			cache_status rfc9211
			background_fetch_timeout 5s
			esi {
				enabled true
				max_depth 3
				max_timeout 5s
				max_concurrent_requests 8
			}
			key {
				include_protocol true
				query whitelist a b
			}
		}
	}
	:18092 {
		route {
			titip {
				esi {
					max_depth 10
				}
				key {
					query whitelist a b c
				}
			}
			respond "Deep Merge Route" 200
		}
	}`

	cadAdapter := caddyconfig.GetAdapter("caddyfile")
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

	resp, err := http.Get("http://localhost:18092")
	if err != nil {
		t.Fatalf("http get failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status code = %d, want 200", resp.StatusCode)
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

	downstream := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
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
	_ = h.ServeHTTP(rec1, req1, downstream)

	// Wait to enter SWR window
	time.Sleep(1100 * time.Millisecond)
	currentPayload.Store("v2")
	replacerFound.Store(false)

	// 2. Trigger SWR stale hit + async revalidation
	req2 := httptest.NewRequest(http.MethodGet, "http://localhost:8080/swr-caddy-replacer", nil)
	repl2 := caddymain.NewReplacer()
	req2 = req2.WithContext(context.WithValue(req2.Context(), caddymain.ReplacerCtxKey, repl2))
	rec2 := httptest.NewRecorder()
	_ = h.ServeHTTP(rec2, req2, downstream)

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

	var downstreamCalls atomic.Int32
	downstream := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		// Downstream (e.g. FrankenPHP) must see the rewritten URL "index.php"
		if r.URL.Path != "/index.php" {
			t.Errorf("expected downstream to receive rewritten path /index.php, got %s", r.URL.Path)
		}
		downstreamCalls.Add(1)

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

	// 1. Request /articles (Miss -> downstream call 1)
	req1 := makeRewrittenReq("/articles")
	rec1 := httptest.NewRecorder()
	_ = h.ServeHTTP(rec1, req1, downstream)
	if rec1.Body.String() != `{"page":"articles"}` {
		t.Fatalf("expected page articles, got %s", rec1.Body.String())
	}
	if downstreamCalls.Load() != 1 {
		t.Fatalf("expected 1 downstream call, got %d", downstreamCalls.Load())
	}

	// 2. Request /products (Different original URL -> Must be a MISS, NOT collision with /articles)
	req2 := makeRewrittenReq("/products")
	rec2 := httptest.NewRecorder()
	_ = h.ServeHTTP(rec2, req2, downstream)
	if rec2.Body.String() != `{"page":"products"}` {
		t.Fatalf("expected page products (not collided with articles), got %s", rec2.Body.String())
	}
	if downstreamCalls.Load() != 2 {
		t.Fatalf("expected 2 downstream calls (separate cache entries), got %d", downstreamCalls.Load())
	}

	// 3. Request /articles again (Same original URL -> HIT, no downstream call)
	req3 := makeRewrittenReq("/articles")
	rec3 := httptest.NewRecorder()
	_ = h.ServeHTTP(rec3, req3, downstream)
	if rec3.Body.String() != `{"page":"articles"}` {
		t.Fatalf("expected cached page articles, got %s", rec3.Body.String())
	}
	if downstreamCalls.Load() != 2 {
		t.Fatalf("expected cache hit to not invoke downstream, got %d calls", downstreamCalls.Load())
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

func TestCaddyHandler_CaseInsensitivePath_LiveExecution(t *testing.T) {
	t.Parallel()
	caddyfileInput := `titip {
		storage test
		key {
			case_insensitive_path true
		}
	}`

	h, cleanup := parseAndProvisionHandler(t, caddyfileInput)
	defer cleanup()

	var originCalls atomic.Int64
	downstream := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		originCalls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("shoes-catalog"))
		return nil
	})

	// 1. Request uppercase
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/Products/Shoes/Running", nil)
	rec1 := httptest.NewRecorder()
	_ = h.ServeHTTP(rec1, req1, downstream)
	if originCalls.Load() != 1 {
		t.Fatalf("expected origin call 1, got %d", originCalls.Load())
	}

	// 2. Request lowercase -> Should HIT cache!
	req2 := httptest.NewRequest(http.MethodGet, "http://example.com/products/shoes/running", nil)
	rec2 := httptest.NewRecorder()
	_ = h.ServeHTTP(rec2, req2, downstream)
	if originCalls.Load() != 1 {
		t.Fatalf("expected cache HIT on lowercase URL (originCalls stays 1), got %d", originCalls.Load())
	}
}

func TestCaddyHandler_IncludedQueryParamValues_LiveExecution(t *testing.T) {
	t.Parallel()
	caddyfileInput := `titip {
		storage test
		key {
			included_query_param_values format json
		}
	}`

	h, cleanup := parseAndProvisionHandler(t, caddyfileInput)
	defer cleanup()

	var originCalls atomic.Int64
	downstream := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		originCalls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("product-content"))
		return nil
	})

	// 1. Request no query -> Miss (originCalls = 1)
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/items", nil)
	rec1 := httptest.NewRecorder()
	_ = h.ServeHTTP(rec1, req1, downstream)
	if originCalls.Load() != 1 {
		t.Fatalf("expected origin call 1, got %d", originCalls.Load())
	}

	// 2. Request with disallowed format=xml -> Pruned -> Hits default cache! (originCalls stays 1)
	req2 := httptest.NewRequest(http.MethodGet, "http://example.com/items?format=xml", nil)
	rec2 := httptest.NewRecorder()
	_ = h.ServeHTTP(rec2, req2, downstream)
	if originCalls.Load() != 1 {
		t.Fatalf("expected cache HIT on default layout for format=xml (originCalls stays 1), got %d", originCalls.Load())
	}

	// 3. Request with allowed format=json -> Separate cache entry -> Miss (originCalls = 2)
	req3 := httptest.NewRequest(http.MethodGet, "http://example.com/items?format=json", nil)
	rec3 := httptest.NewRecorder()
	_ = h.ServeHTTP(rec3, req3, downstream)
	if originCalls.Load() != 2 {
		t.Fatalf("expected origin call 2 for format=json, got %d", originCalls.Load())
	}

	// 4. Second request with format=json -> Cache HIT! (originCalls stays 2)
	req4 := httptest.NewRequest(http.MethodGet, "http://example.com/items?format=json", nil)
	rec4 := httptest.NewRecorder()
	_ = h.ServeHTTP(rec4, req4, downstream)
	if originCalls.Load() != 2 {
		t.Fatalf("expected cache HIT for format=json, got %d", originCalls.Load())
	}
}

func TestCaddyHandler_KeyConfig_SmartExcludeQueryStringInheritance(t *testing.T) {
	t.Parallel()

	boolPtr := func(b bool) *bool { return &b }

	// Baseline global config with ExcludeQueryString: true
	globalKey := &KeyConfig{
		ExcludeQueryString: boolPtr(true),
	}

	// 1. Route defines IncludedQueryParamValues without ExcludeQueryString -> ExcludeQueryString should become false
	t.Run("RouteWithIncludedQueryParamValues", func(t *testing.T) {
		target := titip.KeyConfig{}
		_ = applyKeyConfig(&target, globalKey)
		if !target.ExcludeQueryString {
			t.Fatalf("expected global ExcludeQueryString to be true")
		}

		routeKey := &KeyConfig{
			IncludedQueryParamValues: map[string][]string{
				"layout": {"marketplace"},
			},
		}
		_ = applyKeyConfig(&target, routeKey)
		if target.ExcludeQueryString {
			t.Errorf("expected route whitelist to automatically deactivate ExcludeQueryString, got true")
		}
		if len(target.IncludedQueryParamValues["layout"]) != 1 || target.IncludedQueryParamValues["layout"][0] != "marketplace" {
			t.Errorf("expected IncludedQueryParamValues to be populated, got: %v", target.IncludedQueryParamValues)
		}
	})

	// 2. Route defines IncludedQueryParams without ExcludeQueryString -> ExcludeQueryString should become false
	t.Run("RouteWithIncludedQueryParams", func(t *testing.T) {
		target := titip.KeyConfig{}
		_ = applyKeyConfig(&target, globalKey)
		if !target.ExcludeQueryString {
			t.Fatalf("expected global ExcludeQueryString to be true")
		}

		routeKey := &KeyConfig{
			IncludedQueryParams: []string{"page", "sort"},
		}
		_ = applyKeyConfig(&target, routeKey)
		if target.ExcludeQueryString {
			t.Errorf("expected route whitelist to automatically deactivate ExcludeQueryString, got true")
		}
		if len(target.IncludedQueryParams) != 2 {
			t.Errorf("expected 2 IncludedQueryParams, got: %v", target.IncludedQueryParams)
		}
	})

	// 3. Route defines no whitelist and no ExcludeQueryString -> ExcludeQueryString remains true
	t.Run("RouteWithoutWhitelist", func(t *testing.T) {
		target := titip.KeyConfig{}
		_ = applyKeyConfig(&target, globalKey)

		routeKey := &KeyConfig{
			CaseInsensitivePath: boolPtr(true),
		}
		_ = applyKeyConfig(&target, routeKey)
		if !target.ExcludeQueryString {
			t.Errorf("expected ExcludeQueryString to remain true when route has no whitelist, got false")
		}
		if !target.CaseInsensitivePath {
			t.Errorf("expected CaseInsensitivePath to be true, got false")
		}
	})

	// 4. Route defines whitelist but explicitly sets ExcludeQueryString: true -> Explicit choice honored
	t.Run("RouteExplicitExcludeQueryStringTrue", func(t *testing.T) {
		target := titip.KeyConfig{}
		_ = applyKeyConfig(&target, globalKey)

		routeKey := &KeyConfig{
			ExcludeQueryString: boolPtr(true),
			IncludedQueryParamValues: map[string][]string{
				"layout": {"marketplace"},
			},
		}
		_ = applyKeyConfig(&target, routeKey)
		if !target.ExcludeQueryString {
			t.Errorf("expected explicit ExcludeQueryString=true on route to take precedence, got false")
		}
	})

	// 5. Route defines whitelist and explicitly sets ExcludeQueryString: false -> Explicit choice honored
	t.Run("RouteExplicitExcludeQueryStringFalse", func(t *testing.T) {
		target := titip.KeyConfig{}
		_ = applyKeyConfig(&target, globalKey)

		routeKey := &KeyConfig{
			ExcludeQueryString:  boolPtr(false),
			IncludedQueryParams: []string{"page"},
		}
		_ = applyKeyConfig(&target, routeKey)
		if target.ExcludeQueryString {
			t.Errorf("expected explicit ExcludeQueryString=false on route to take precedence, got true")
		}
	})
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

func TestCaddyHandler_GlobalExcludeQueryString_RouteWhitelistOverride_LiveExecution(t *testing.T) {
	var originCalls atomic.Int64
	originServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, "content for %s (call %d)", r.URL.Path, originCalls.Load())
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
			key {
				exclude_query_string true
			}
		}
	}
	:18094 {
		route {
			handle /items/* {
				titip {
					key {
						included_query_param_values format json
					}
				}
				reverse_proxy %s
			}

			handle /blog/* {
				titip
				reverse_proxy %s
			}
		}
	}`, originURL.Host, originURL.Host)

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
	defer func() {
		_ = caddymain.Stop()
	}()

	client := &http.Client{Timeout: 5 * time.Second}

	// 1. /items with format=json -> Call 1 (MISS)
	resp1, err := client.Get("http://localhost:18094/items/detail?format=json")
	if err != nil {
		t.Fatalf("req 1 failed: %v", err)
	}
	_ = resp1.Body.Close()
	if originCalls.Load() != 1 {
		t.Fatalf("expected originCalls=1, got %d", originCalls.Load())
	}

	// /items with format=json again -> HIT (originCalls stays 1)
	resp2, err := client.Get("http://localhost:18094/items/detail?format=json")
	if err != nil {
		t.Fatalf("req 2 failed: %v", err)
	}
	_ = resp2.Body.Close()
	if originCalls.Load() != 1 {
		t.Fatalf("expected cache HIT for format=json (originCalls stays 1), got %d", originCalls.Load())
	}

	// 2. /items with unlisted format=xml -> pruned, so it collapses to /items/detail (Call 2)
	resp3, err := client.Get("http://localhost:18094/items/detail?format=xml")
	if err != nil {
		t.Fatalf("req 3 failed: %v", err)
	}
	_ = resp3.Body.Close()
	if originCalls.Load() != 2 {
		t.Fatalf("expected originCalls=2 for unlisted format=xml, got %d", originCalls.Load())
	}

	// Second request for unlisted format=xml -> HIT on default /items/detail (originCalls stays 2)
	resp4, err := client.Get("http://localhost:18094/items/detail?format=xml")
	if err != nil {
		t.Fatalf("req 4 failed: %v", err)
	}
	_ = resp4.Body.Close()
	if originCalls.Load() != 2 {
		t.Fatalf("expected cache HIT for second format=xml (originCalls stays 2), got %d", originCalls.Load())
	}

	// 3. /blog with query -> query stripped per global exclude_query_string: true (Call 3)
	respBlog1, err := client.Get("http://localhost:18094/blog/post?token=123")
	if err != nil {
		t.Fatalf("blog req 1 failed: %v", err)
	}
	_ = respBlog1.Body.Close()
	if originCalls.Load() != 3 {
		t.Fatalf("expected originCalls=3 on first blog request, got %d", originCalls.Load())
	}

	// /blog with different query token -> HIT on stripped /blog/post (originCalls stays 3)
	respBlog2, err := client.Get("http://localhost:18094/blog/post?token=456")
	if err != nil {
		t.Fatalf("blog req 2 failed: %v", err)
	}
	_ = respBlog2.Body.Close()
	if originCalls.Load() != 3 {
		t.Fatalf("expected cache hit on blog with different query string (originCalls stays 3), got %d", originCalls.Load())
	}
}



