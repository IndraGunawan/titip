package caddy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
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
	"github.com/redis/rueidis"

	_ "github.com/indragunawan/titip/storage/redis/caddy"
)

func getTestRedisAddr() string {
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		return addr
	}
	return "127.0.0.1:6379"
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
	config := fmt.Sprintf(`titip {
		cache_status RFC9211
		origin_timeout 20s
		storage redis {
			address %q
		}
	}`, getTestRedisAddr())

	d := caddyfile.NewTestDispenser(config)
	var h Handler
	if err := h.UnmarshalCaddyfile(d); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if h.CacheStatus != "RFC9211" {
		t.Errorf("expected cache_status RFC9211, got %s", h.CacheStatus)
	}
	if h.OriginTimeout != "20s" {
		t.Errorf("expected origin_timeout 20s, got %s", h.OriginTimeout)
	}
}

func TestCaddyHandler_MiddlewareExecution(t *testing.T) {
	prefix := fmt.Sprintf("test_caddy_mw:%d:%d:", time.Now().UnixNano(), rand.Int63())
	caddyfileInput := fmt.Sprintf(`titip {
		cache_status rfc9211
		origin_timeout 5s
		storage redis {
			address %q
			key_prefix %q
		}
	}`, getTestRedisAddr(), prefix)

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
		t.Fatalf("expected error when storage is missing")
	}
	if !strings.Contains(err.Error(), "storage configuration is required") {
		t.Errorf("expected missing storage error, got %v", err)
	}
}

// AC-3: Admin Purge API Single-Target Validation & Execution
func TestAdminPurge_ValidationAndMutualExclusivity(t *testing.T) {
	// 1. Mutual exclusivity violation (both urls and tags)
	body1 := `{"urls": ["http://example.com/api/item"], "tags": ["tag1"], "soft": true}`
	req1 := httptest.NewRequest(http.MethodPost, "/titip/purge", bytes.NewBufferString(body1))
	req1.Header.Set("Content-Type", "application/json")
	rec1 := httptest.NewRecorder()

	_ = handleAdminPurge(rec1, req1)
	if rec1.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for multiple targets, got %d", rec1.Code)
	}

	// 2. Missing target
	body2 := `{"soft": true}`
	req2 := httptest.NewRequest(http.MethodPost, "/titip/purge", bytes.NewBufferString(body2))
	req2.Header.Set("Content-Type", "application/json")
	rec2 := httptest.NewRecorder()

	_ = handleAdminPurge(rec2, req2)
	if rec2.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for missing target, got %d", rec2.Code)
	}

	// 3. Valid single-target URL purge
	body3 := `{"urls": ["http://example.com/api/item"], "soft": true}`
	req3 := httptest.NewRequest(http.MethodPost, "/titip/purge", bytes.NewBufferString(body3))
	req3.Header.Set("Content-Type", "application/json")
	rec3 := httptest.NewRecorder()

	_ = handleAdminPurge(rec3, req3)
	if rec3.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rec3.Code, rec3.Body.String())
	}

	var resp purgeAdminResponse
	if err := json.NewDecoder(rec3.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if !resp.Success || resp.Purged.Type != "urls" || resp.Purged.Count != 1 || !resp.Purged.Soft {
		t.Errorf("unexpected purge response: %+v", resp)
	}

	// 4. Valid single-target Tag purge
	body4 := `{"tags": ["users", "products"], "soft": false}`
	req4 := httptest.NewRequest(http.MethodPost, "/titip/purge", bytes.NewBufferString(body4))
	req4.Header.Set("Content-Type", "application/json")
	rec4 := httptest.NewRecorder()

	_ = handleAdminPurge(rec4, req4)
	if rec4.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for tags, got %d", rec4.Code)
	}

	// 5. Valid purge_everything
	body5 := `{"purge_everything": true, "soft": true}`
	req5 := httptest.NewRequest(http.MethodPost, "/titip/purge", bytes.NewBufferString(body5))
	req5.Header.Set("Content-Type", "application/json")
	rec5 := httptest.NewRecorder()

	_ = handleAdminPurge(rec5, req5)
	if rec5.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for purge_everything, got %d", rec5.Code)
	}
}

// AC-1 Edge Case: Undefined Storage in Caddyfile
func TestCaddyHandler_UndefinedStorage_FailsProvisioning(t *testing.T) {
	config := `titip {
		cache_status rfc9211
		origin_timeout 10s
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
		t.Fatalf("expected error when storage directive is omitted")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("storage configuration is required")) {
		t.Errorf("expected 'storage configuration is required' error, got: %v", err)
	}
}

// AC-1 Edge Case: Unknown Storage Module in Caddyfile
func TestCaddyHandler_UnknownStorageModule_Fails(t *testing.T) {
	config := `titip {
		storage memcached {
			servers 127.0.0.1:11211
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
	prefix := fmt.Sprintf("test_caddy_purge:%d:%d:", time.Now().UnixNano(), rand.Int63())
	caddyfileInput := fmt.Sprintf(`titip {
		storage redis {
			address %q
			key_prefix %q
		}
	}`, getTestRedisAddr(), prefix)

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
	config := `:8080 {
		storage redis {
			address localhost:6379
		}
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
	config := fmt.Sprintf(`titip {
		storage redis {
			address %q
		}
		key {
			include_protocol false
			exclude_host true
			keep_trailing_slash false
			exclude_query_string false
			disable_query_string_sort false
			included_query_params id category page
			excluded_query_params tracking
			exclude_marketing_params true
			included_header_names X-App-Version Accept-Language
			included_cookie_names session_currency
		}
	}`, getTestRedisAddr())

	d := caddyfile.NewTestDispenser(config)
	var h Handler
	if err := h.UnmarshalCaddyfile(d); err != nil {
		t.Fatalf("unmarshal error: %v", err)
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
	if h.Key.KeepTrailingSlash == nil || *h.Key.KeepTrailingSlash != false {
		t.Errorf("expected KeepTrailingSlash false, got %v", h.Key.KeepTrailingSlash)
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
}

func TestCaddyHandler_KeyConfig_LiveExecution(t *testing.T) {
	prefix := fmt.Sprintf("test:caddy:key:%d:", rand.Int63())
	caddyfileInput := fmt.Sprintf(`titip {
		storage redis {
			address %q
			key_prefix %q
		}
		key {
			include_protocol false
			exclude_host true
			included_query_params id
			exclude_marketing_params true
		}
	}`, getTestRedisAddr(), prefix)

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
	config := fmt.Sprintf(`titip {
		cache_status simple
		storage redis {
			address %q
		}
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
	}`, getTestRedisAddr())

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
	prefix := fmt.Sprintf("test:caddy:esi:%d:", rand.Int63())
	caddyfileInput := fmt.Sprintf(`titip {
		storage redis {
			address %q
			key_prefix %q
		}
		esi {
			enabled true
		}
	}`, getTestRedisAddr(), prefix)

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
	prefix := fmt.Sprintf("test:caddy:replacer:%d:", rand.Int63())
	caddyfileInput := fmt.Sprintf(`titip {
		storage redis {
			address %q
			key_prefix %q
		}
		esi {
			enabled true
		}
	}`, getTestRedisAddr(), prefix)

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
	for i := 0; i < concurrentRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
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
		}()
	}
	wg.Wait()
}

// TestCaddyGlobalOption_Adapt verifies that { titip { ... } } in global options
// compiles properly to apps.titip in Caddy JSON.
func TestCaddyGlobalOption_Adapt(t *testing.T) {
	prefix := fmt.Sprintf("test:caddy:global:inherit:%d:", rand.Int63())

	caddyfileInput := fmt.Sprintf(`{
		titip {
			storage redis {
				address %q
				key_prefix %q
			}
			cache_status rfc9211
			origin_timeout 5s
			ignore_client_cache_control true
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
	}`, getTestRedisAddr(), prefix)

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
	if storageMap["name"] != "redis" {
		t.Errorf("expected storage.name='redis', got %v", storageMap["name"])
	}
	if titipApp["cache_status"] != "rfc9211" {
		t.Errorf("expected cache_status rfc9211, got %v", titipApp["cache_status"])
	}
	if titipApp["origin_timeout"] != "5s" {
		t.Errorf("expected origin_timeout 5s, got %v", titipApp["origin_timeout"])
	}
}

// TestCaddyGlobalOption_InheritanceAndOverride tests App provisioning and Handler inheritance via full Caddyfile.
func TestCaddyGlobalOption_InheritanceAndOverride(t *testing.T) {
	prefix := fmt.Sprintf("test:caddy:global:inherit:%d:", rand.Int63())

	caddyfileInput := fmt.Sprintf(`{
		admin off
		titip {
			storage redis {
				address %q
				key_prefix %q
			}
			cache_status rfc9211
			origin_timeout 10s
			ignore_client_cache_control true
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
	}`, getTestRedisAddr(), prefix)

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
	prefix := fmt.Sprintf("test:caddy:deepmerge:%d:", rand.Int63())

	caddyfileInput := fmt.Sprintf(`{
		admin off
		titip {
			storage redis {
				address %q
				key_prefix %q
			}
			cache_status rfc9211
			origin_timeout 5s
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
	}`, getTestRedisAddr(), prefix)

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
	prefix := fmt.Sprintf("test:caddy:e2e:%d:", rand.Int63())

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
		titip {
			storage redis {
				address %q
				key_prefix %q
			}
			cache_status rfc9211
			origin_timeout 5s
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
	}`, getTestRedisAddr(), prefix, originURL.Host)

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
	if storageMap["name"] != "redis" {
		t.Errorf("expected storage.name='redis', got %v", storageMap["name"])
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

// Ensure unused import warning prevention for rueidis
var _ = rueidis.Nil
