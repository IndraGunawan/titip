package caddy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	caddymain "github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

// setupMultiInstanceCaddy creates an adapted Caddy context with a global App and
// returns two provisioned Handlers representing two distinct routes sharing global storage.
func setupMultiInstanceCaddy(t testing.TB, route1Caddyfile, route2Caddyfile string) (*Handler, *Handler, func()) {
	t.Helper()

	globalCaddyfile := `{
		titip {
			storage test
			cache_status rfc9211
		}
	}`

	cadAdapter := caddyconfig.GetAdapter("caddyfile")
	if cadAdapter == nil {
		t.Fatalf("caddyfile adapter not registered")
	}

	jsonBytes, _, err := cadAdapter.Adapt([]byte(globalCaddyfile), map[string]any{"filename": "Caddyfile"})
	if err != nil {
		t.Fatalf("adapt global caddyfile failed: %v", err)
	}

	cfg := new(caddymain.Config)
	if err := json.Unmarshal(jsonBytes, cfg); err != nil {
		t.Fatalf("unmarshal config failed: %v", err)
	}

	ctx, err := caddymain.ProvisionContext(cfg)
	if err != nil {
		t.Fatalf("provision context failed: %v", err)
	}

	// Provision Handler 1
	var h1 Handler
	d1 := caddyfile.NewTestDispenser(route1Caddyfile)
	if err := h1.UnmarshalCaddyfile(d1); err != nil {
		t.Fatalf("unmarshal h1 failed: %v", err)
	}
	if err := h1.Provision(ctx); err != nil {
		t.Fatalf("provision h1 failed: %v", err)
	}

	// Provision Handler 2
	var h2 Handler
	d2 := caddyfile.NewTestDispenser(route2Caddyfile)
	if err := h2.UnmarshalCaddyfile(d2); err != nil {
		t.Fatalf("unmarshal h2 failed: %v", err)
	}
	if err := h2.Provision(ctx); err != nil {
		t.Fatalf("provision h2 failed: %v", err)
	}

	cleanup := func() {
		_ = h1.Cleanup()
		_ = h2.Cleanup()
	}

	return &h1, &h2, cleanup
}

func TestCaddy_MultiInstance_SharedGlobalStorage(t *testing.T) {
	h1, h2, cleanup := setupMultiInstanceCaddy(t,
		`titip {
			cache_status rfc9211
		}`,
		`titip {
			cache_status simple
		}`,
	)
	defer cleanup()

	if h1.instance == nil || h2.instance == nil {
		t.Fatalf("expected both instances to be non-nil")
	}
	if h1.id == h2.id {
		t.Fatalf("expected distinct instance IDs, got both %s", h1.id)
	}

	// Verify both handlers inherit from global storage (route storageMod is nil)
	if h1.storageMod != nil || h2.storageMod != nil {
		t.Fatalf("expected route storageMod to be nil when inheriting global storage")
	}

	var upstreamCalls1, upstreamCalls2 atomic.Int32
	next1 := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		upstreamCalls1.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=300")
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("api-shared-response"))
		return nil
	})
	next2 := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		upstreamCalls2.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=300")
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("assets-shared-response"))
		return nil
	})

	// 1. Prime through Handler 1
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/shared/item", nil)
	rec1 := httptest.NewRecorder()
	if err := h1.ServeHTTP(rec1, req1, next1); err != nil {
		t.Fatalf("h1 serve error: %v", err)
	}
	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec1.Code)
	}
	if upstreamCalls1.Load() != 1 {
		t.Fatalf("expected 1 upstream call on h1 miss, got %d", upstreamCalls1.Load())
	}

	// 2. Second request through Handler 1 -> cache hit (RFC 9211 header format)
	rec1Hit := httptest.NewRecorder()
	if err := h1.ServeHTTP(rec1Hit, req1, next1); err != nil {
		t.Fatalf("h1 serve error: %v", err)
	}
	if !strings.Contains(rec1Hit.Header().Get("Cache-Status"), "titip; hit") {
		t.Errorf("expected RFC9211 cache hit on h1, got %q", rec1Hit.Header().Get("Cache-Status"))
	}
	if upstreamCalls1.Load() != 1 {
		t.Fatalf("expected h1 cache hit to not call upstream, got %d", upstreamCalls1.Load())
	}

	// 3. Prime through Handler 2
	req2 := httptest.NewRequest(http.MethodGet, "http://example.com/shared/other", nil)
	rec2 := httptest.NewRecorder()
	if err := h2.ServeHTTP(rec2, req2, next2); err != nil {
		t.Fatalf("h2 serve error: %v", err)
	}
	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec2.Code)
	}
	if upstreamCalls2.Load() != 1 {
		t.Fatalf("expected 1 upstream call on h2 miss, got %d", upstreamCalls2.Load())
	}

	// 4. Second request through Handler 2 -> cache hit (Simple format)
	rec2Hit := httptest.NewRecorder()
	if err := h2.ServeHTTP(rec2Hit, req2, next2); err != nil {
		t.Fatalf("h2 serve error: %v", err)
	}
	if rec2Hit.Header().Get("Cache-Status") != "HIT" {
		t.Errorf("expected simple token HIT on h2, got %q", rec2Hit.Header().Get("Cache-Status"))
	}
	if upstreamCalls2.Load() != 1 {
		t.Fatalf("expected h2 cache hit to not call upstream, got %d", upstreamCalls2.Load())
	}
}

func TestCaddy_MultiInstance_ActiveInstancesRegistry(t *testing.T) {
	h1, h2, cleanup := setupMultiInstanceCaddy(t,
		`titip { cache_status simple }`,
		`titip { cache_status rfc9211 }`,
	)
	defer cleanup()

	instances := getInstances()
	var found1, found2 bool
	for _, inst := range instances {
		if inst == h1.instance {
			found1 = true
		}
		if inst == h2.instance {
			found2 = true
		}
	}
	if !found1 || !found2 {
		t.Fatalf("expected both h1 and h2 to be registered in activeInstances, found1=%v, found2=%v", found1, found2)
	}

	// Unregister h1 via Cleanup
	_ = h1.Cleanup()

	instancesAfterH1 := getInstances()
	found1After := false
	found2After := false
	for _, inst := range instancesAfterH1 {
		if inst == h1.instance {
			found1After = true
		}
		if inst == h2.instance {
			found2After = true
		}
	}
	if found1After {
		t.Errorf("expected h1 to be unregistered from activeInstances after Cleanup")
	}
	if !found2After {
		t.Errorf("expected h2 to remain registered in activeInstances")
	}

	// Unregister h2
	_ = h2.Cleanup()
	for _, inst := range getInstances() {
		if inst == h2.instance {
			t.Errorf("expected h2 to be unregistered from activeInstances after Cleanup")
		}
	}
}

func TestCaddy_MultiInstance_AdminPurgeFanOut(t *testing.T) {
	h1, h2, cleanup := setupMultiInstanceCaddy(t,
		`titip {
			cache_status simple
			tag_header Cache-Tag
		}`,
		`titip {
			cache_status simple
			tag_header Cache-Tag
		}`,
	)
	defer cleanup()

	var upstreamCalls atomic.Int32
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		call := upstreamCalls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=300")
		w.Header().Set("Cache-Tag", "tenant-alpha")
		w.Header().Set("Content-Type", "text/plain")
		_, _ = fmt.Fprintf(w, "response-v%d", call)
		return nil
	})

	url1 := "http://example.com/site1/item"
	url2 := "http://example.com/site2/item"

	// 1. Prime cache entries on both handlers
	rec1 := httptest.NewRecorder()
	_ = h1.ServeHTTP(rec1, httptest.NewRequest(http.MethodGet, url1, nil), next)
	if rec1.Body.String() != "response-v1" {
		t.Fatalf("expected response-v1, got %q", rec1.Body.String())
	}

	rec2 := httptest.NewRecorder()
	_ = h2.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, url2, nil), next)
	if rec2.Body.String() != "response-v2" {
		t.Fatalf("expected response-v2, got %q", rec2.Body.String())
	}

	// Verify both hit cache
	rec1Hit := httptest.NewRecorder()
	_ = h1.ServeHTTP(rec1Hit, httptest.NewRequest(http.MethodGet, url1, nil), next)
	if rec1Hit.Header().Get("Cache-Status") != "HIT" {
		t.Fatalf("expected HIT on url1, got %q", rec1Hit.Header().Get("Cache-Status"))
	}

	rec2Hit := httptest.NewRecorder()
	_ = h2.ServeHTTP(rec2Hit, httptest.NewRequest(http.MethodGet, url2, nil), next)
	if rec2Hit.Header().Get("Cache-Status") != "HIT" {
		t.Fatalf("expected HIT on url2, got %q", rec2Hit.Header().Get("Cache-Status"))
	}

	if upstreamCalls.Load() != 2 {
		t.Fatalf("expected exactly 2 upstream calls before purge, got %d", upstreamCalls.Load())
	}

	// 2. Send Admin Purge by tag "tenant-alpha" with soft=false
	softFalse := false
	purgeReqBody, _ := json.Marshal(purgeAdminRequest{
		Tags: []string{"tenant-alpha"},
		Soft: &softFalse,
	})
	adminReq := httptest.NewRequest(http.MethodPost, "/titip/purge", bytes.NewReader(purgeReqBody))
	adminReq.Header.Set("Content-Type", "application/json")
	adminRec := httptest.NewRecorder()

	if err := handleAdminPurge(adminRec, adminReq); err != nil {
		t.Fatalf("admin purge failed: %v", err)
	}
	if adminRec.Code != http.StatusOK {
		t.Fatalf("admin purge expected 200, got %d: %s", adminRec.Code, adminRec.Body.String())
	}

	var purgeResp purgeAdminResponse
	if err := json.Unmarshal(adminRec.Body.Bytes(), &purgeResp); err != nil {
		t.Fatalf("unmarshal purge response: %v", err)
	}
	if !purgeResp.Success {
		t.Fatalf("expected success=true in purge response")
	}
	if purgeResp.Purged.Count < 2 {
		t.Fatalf("expected at least 2 purged items across both instances, got %d", purgeResp.Purged.Count)
	}

	// 3. Both handlers must miss cache and re-fetch from upstream
	rec1After := httptest.NewRecorder()
	_ = h1.ServeHTTP(rec1After, httptest.NewRequest(http.MethodGet, url1, nil), next)
	if rec1After.Header().Get("Cache-Status") != "MISS" {
		t.Errorf("url1 expected MISS after tag purge, got %q", rec1After.Header().Get("Cache-Status"))
	}

	rec2After := httptest.NewRecorder()
	_ = h2.ServeHTTP(rec2After, httptest.NewRequest(http.MethodGet, url2, nil), next)
	if rec2After.Header().Get("Cache-Status") != "MISS" {
		t.Errorf("url2 expected MISS after tag purge, got %q", rec2After.Header().Get("Cache-Status"))
	}

	if upstreamCalls.Load() != 4 {
		t.Fatalf("expected 4 total upstream calls (2 original + 2 refreshed), got %d", upstreamCalls.Load())
	}
}

func TestCaddy_MultiInstance_RouteOptionIsolation(t *testing.T) {
	// Route 1: ignores host in cache key
	// Route 2: preserves host in cache key
	h1, h2, cleanup := setupMultiInstanceCaddy(t,
		`titip {
			cache_status rfc9211
			cache_key {
				exclude_host true
			}
		}`,
		`titip {
			cache_status simple
			cache_key {
				exclude_host false
			}
		}`,
	)
	defer cleanup()

	var upstreamCalls atomic.Int32
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		call := upstreamCalls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=300")
		_, _ = fmt.Fprintf(w, "host=%s,call=%d", r.Host, call)
		return nil
	})

	// Handler 1: exclude_host=true -> different hosts hit the SAME cache entry
	rec1A := httptest.NewRecorder()
	_ = h1.ServeHTTP(rec1A, httptest.NewRequest(http.MethodGet, "http://host-a.com/page", nil), next)
	if rec1A.Body.String() != "host=host-a.com,call=1" {
		t.Fatalf("expected call=1, got %q", rec1A.Body.String())
	}

	rec1B := httptest.NewRecorder()
	_ = h1.ServeHTTP(rec1B, httptest.NewRequest(http.MethodGet, "http://host-b.com/page", nil), next)
	// Must HIT cached response from host-a because exclude_host=true
	if !strings.Contains(rec1B.Header().Get("Cache-Status"), "hit") {
		t.Errorf("expected hit on h1 with different host, got %q", rec1B.Header().Get("Cache-Status"))
	}
	if rec1B.Body.String() != "host=host-a.com,call=1" {
		t.Errorf("expected cached host-a response, got %q", rec1B.Body.String())
	}

	// Handler 2: exclude_host=false -> different hosts produce DIFFERENT cache entries
	rec2A := httptest.NewRecorder()
	_ = h2.ServeHTTP(rec2A, httptest.NewRequest(http.MethodGet, "http://host-a.com/page2", nil), next)
	if rec2A.Body.String() != "host=host-a.com,call=2" {
		t.Fatalf("expected call=2, got %q", rec2A.Body.String())
	}

	rec2B := httptest.NewRecorder()
	_ = h2.ServeHTTP(rec2B, httptest.NewRequest(http.MethodGet, "http://host-b.com/page2", nil), next)
	// Must MISS because host-b is distinct when exclude_host=false
	if rec2B.Header().Get("Cache-Status") != "MISS" {
		t.Errorf("expected MISS on h2 with different host, got %q", rec2B.Header().Get("Cache-Status"))
	}
	if rec2B.Body.String() != "host=host-b.com,call=3" {
		t.Errorf("expected fresh call=3, got %q", rec2B.Body.String())
	}
}
