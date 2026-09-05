package titip

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// ─────────────────────────────────────────────────────────────────────────────
// parsePurgeTarget
// ─────────────────────────────────────────────────────────────────────────────

func TestParsePurgeTarget_Empty(t *testing.T) {
	pt, err := parsePurgeTarget("", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pt != nil {
		t.Fatalf("expected nil target for empty string, got: %+v", pt)
	}
}

func TestParsePurgeTarget_PathOnly(t *testing.T) {
	pt, err := parsePurgeTarget("/api/products", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pt.mode != purgeModePathSweep {
		t.Errorf("expected PathSweep mode, got: %v", pt.mode)
	}
	if pt.path != "/api/products" {
		t.Errorf("expected path /api/products, got: %s", pt.path)
	}
	if pt.host != "" {
		t.Errorf("expected empty host, got: %s", pt.host)
	}
}

func TestParsePurgeTarget_PathWithQuery_ExactMode(t *testing.T) {
	pt, err := parsePurgeTarget("/api/products?id=42&page=1", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pt.mode != purgeModeExact {
		t.Errorf("expected Exact mode, got: %v", pt.mode)
	}
	if pt.path != "/api/products" {
		t.Errorf("expected path /api/products, got: %s", pt.path)
	}
	if pt.query == "" {
		t.Error("expected non-empty query, got empty")
	}
}

func TestParsePurgeTarget_WildcardSuffix(t *testing.T) {
	pt, err := parsePurgeTarget("/assets/*", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pt.mode != purgeModeWildcard {
		t.Errorf("expected Wildcard mode, got: %v", pt.mode)
	}
	if pt.path != "/assets" {
		t.Errorf("expected path /assets, got: %s", pt.path)
	}
}

func TestParsePurgeTarget_WildcardDeepPath(t *testing.T) {
	pt, err := parsePurgeTarget("/images/product/thumbnail/*", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pt.mode != purgeModeWildcard {
		t.Errorf("expected Wildcard mode, got: %v", pt.mode)
	}
	if pt.path != "/images/product/thumbnail" {
		t.Errorf("expected stripped path, got: %s", pt.path)
	}
}

func TestParsePurgeTarget_FullHTTPSURL_HostScoped(t *testing.T) {
	pt, err := parsePurgeTarget("https://example.com/api/products", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pt.mode != purgeModePathSweep {
		t.Errorf("expected PathSweep, got: %v", pt.mode)
	}
	if pt.host != "example.com" {
		t.Errorf("expected host example.com, got: %s", pt.host)
	}
	if pt.scheme != "https" {
		t.Errorf("expected scheme https, got: %s", pt.scheme)
	}
	if pt.path != "/api/products" {
		t.Errorf("expected path /api/products, got: %s", pt.path)
	}
}

func TestParsePurgeTarget_FullHTTPURL_HostScoped(t *testing.T) {
	pt, err := parsePurgeTarget("http://cdn.example.com/assets/style.css", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pt.scheme != "http" {
		t.Errorf("expected scheme http, got: %s", pt.scheme)
	}
	if pt.host != "cdn.example.com" {
		t.Errorf("expected host cdn.example.com, got: %s", pt.host)
	}
}

func TestParsePurgeTarget_HostSchemeAmbiguous(t *testing.T) {
	// No scheme prefix → scheme is empty (ambiguous).
	pt, err := parsePurgeTarget("example.com/api/items", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pt.scheme != "" {
		t.Errorf("expected empty scheme for ambiguous target, got: %s", pt.scheme)
	}
	if pt.host != "example.com" {
		t.Errorf("expected host example.com, got: %s", pt.host)
	}
}

func TestParsePurgeTarget_HostWithDefaultPortStripped(t *testing.T) {
	pt, err := parsePurgeTarget("https://example.com:443/api", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pt.host != "example.com" {
		t.Errorf("expected :443 stripped, got host: %s", pt.host)
	}
}

func TestParsePurgeTarget_HostWithNonDefaultPortPreserved(t *testing.T) {
	pt, err := parsePurgeTarget("http://example.com:8080/api", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pt.host != "example.com:8080" {
		t.Errorf("expected :8080 preserved, got host: %s", pt.host)
	}
}

func TestParsePurgeTarget_TrailingSlashIsWildcard(t *testing.T) {
	// Root "/" is treated as wildcard.
	pt, err := parsePurgeTarget("/", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pt.mode != purgeModeWildcard {
		t.Errorf("expected Wildcard mode for '/', got: %v", pt.mode)
	}
}

func TestParsePurgeTarget_DotSegmentsInPath(t *testing.T) {
	pt, err := parsePurgeTarget("/a/b/../c", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pt.path != "/a/c" {
		t.Errorf("expected dot segments cleaned, got: %s", pt.path)
	}
}

func TestParsePurgeTarget_QuerySortedForExactMatch(t *testing.T) {
	// Query params must be sorted to match the canonical key format.
	pt1, _ := parsePurgeTarget("/items?b=2&a=1", nil)
	pt2, _ := parsePurgeTarget("/items?a=1&b=2", nil)
	if pt1.query != pt2.query {
		t.Errorf("query must be sorted regardless of input order:\n pt1=%s\n pt2=%s", pt1.query, pt2.query)
	}
}

func TestParsePurgeTarget_RespectsKeyConfig(t *testing.T) {
	// 1. ExcludeQueryString turns URL with query into PathSweep.
	cfgNoQuery := &KeyConfig{ExcludeQueryString: true}
	pt1, err := parsePurgeTarget("/items?id=100&page=2", cfgNoQuery)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pt1.mode != purgeModePathSweep {
		t.Errorf("expected PathSweep when query is excluded, got: %v", pt1.mode)
	}
	if pt1.query != "" {
		t.Errorf("expected empty query, got: %s", pt1.query)
	}

	// 2. IncludedQueryParams whitelist filtering.
	cfgWhitelist := &KeyConfig{IncludedQueryParams: []string{"id"}}
	pt2, err := parsePurgeTarget("/items?id=100&page=2&utm_source=fb", cfgWhitelist)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pt2.mode != purgeModeExact {
		t.Errorf("expected Exact mode, got: %v", pt2.mode)
	}
	if pt2.query != "id=100" {
		t.Errorf("expected whitelisted id=100 query only, got: %s", pt2.query)
	}

	// 3. Trailing slash preserved on path.
	pt3, err := parsePurgeTarget("/docs/", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pt3.path != "/docs/" {
		t.Errorf("expected trailing slash preserved to /docs/, got: %s", pt3.path)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// buildPurgePatterns
// ─────────────────────────────────────────────────────────────────────────────

func TestBuildPurgePatterns_ExactMode_NoHost(t *testing.T) {
	pt := &purgeTarget{
		mode:  purgeModeExact,
		path:  "/api/products",
		query: "id=42",
	}
	cfg := &KeyConfig{}
	patterns := buildPurgePatterns(pt, cfg)
	if len(patterns) != 1 {
		t.Fatalf("expected 1 pattern, got %d: %v", len(patterns), patterns)
	}
	expected := "p=/api/products:h=*:m=*:qs=id=42*"
	if patterns[0] != expected {
		t.Errorf("expected pattern %q\n got %q", expected, patterns[0])
	}
}

func TestBuildPurgePatterns_ExactMode_NoHost_ExcludeHostConfig(t *testing.T) {
	pt := &purgeTarget{
		mode:  purgeModeExact,
		path:  "/api/products",
		query: "id=42",
	}
	cfg := &KeyConfig{ExcludeHost: true}
	patterns := buildPurgePatterns(pt, cfg)
	if len(patterns) != 1 {
		t.Fatalf("expected 1 pattern, got %d: %v", len(patterns), patterns)
	}
	expected := "p=/api/products:m=GET:qs=id=42"
	if patterns[0] != expected {
		t.Errorf("expected pattern %q\n got %q", expected, patterns[0])
	}
}

func TestBuildPurgePatterns_ExactMode_WithHost(t *testing.T) {
	pt := &purgeTarget{
		mode:  purgeModeExact,
		path:  "/api/products",
		host:  "example.com",
		query: "id=42",
	}
	cfg := &KeyConfig{}
	patterns := buildPurgePatterns(pt, cfg)
	expected := "p=/api/products:h=example.com:m=GET:qs=id=42"
	if patterns[0] != expected {
		t.Errorf("expected %q\n got %q", expected, patterns[0])
	}
}

func TestBuildPurgePatterns_ExactMode_WithScheme(t *testing.T) {
	pt := &purgeTarget{
		mode:   purgeModeExact,
		path:   "/api/products",
		host:   "example.com",
		scheme: "https",
		query:  "id=42",
	}
	cfg := &KeyConfig{IncludeProtocol: true}
	patterns := buildPurgePatterns(pt, cfg)
	expected := "p=/api/products:h=example.com:m=GET:s=https:qs=id=42"
	if patterns[0] != expected {
		t.Errorf("expected %q\n got %q", expected, patterns[0])
	}
}

func TestBuildPurgePatterns_PathSweep_NoHost(t *testing.T) {
	pt := &purgeTarget{
		mode: purgeModePathSweep,
		path: "/api/products",
	}
	cfg := &KeyConfig{}
	patterns := buildPurgePatterns(pt, cfg)
	if len(patterns) != 1 {
		t.Fatalf("expected 1 pattern, got %d: %v", len(patterns), patterns)
	}
	expected := "p=/api/products:h=*:m=*"
	if patterns[0] != expected {
		t.Errorf("expected %q\n got %q", expected, patterns[0])
	}
}

func TestBuildPurgePatterns_PathSweep_NoHost_ExcludeHostConfig(t *testing.T) {
	pt := &purgeTarget{
		mode: purgeModePathSweep,
		path: "/api/products",
	}
	cfg := &KeyConfig{ExcludeHost: true}
	patterns := buildPurgePatterns(pt, cfg)
	if len(patterns) != 1 {
		t.Fatalf("expected 1 pattern, got %d: %v", len(patterns), patterns)
	}
	expected := "p=/api/products:m=*"
	if patterns[0] != expected {
		t.Errorf("expected %q\n got %q", expected, patterns[0])
	}
}

func TestBuildPurgePatterns_PathSweep_WithHost(t *testing.T) {
	pt := &purgeTarget{
		mode: purgeModePathSweep,
		path: "/api/products",
		host: "example.com",
	}
	cfg := &KeyConfig{}
	patterns := buildPurgePatterns(pt, cfg)
	expected := "p=/api/products:h=example.com:m=*"
	if patterns[0] != expected {
		t.Errorf("expected %q\n got %q", expected, patterns[0])
	}
}

func TestBuildPurgePatterns_PathSweep_DualProtocol(t *testing.T) {
	// When IncludeProtocol=true and no scheme specified → two patterns (http + https).
	pt := &purgeTarget{
		mode: purgeModePathSweep,
		path: "/api/products",
		host: "example.com",
	}
	cfg := &KeyConfig{IncludeProtocol: true}
	patterns := buildPurgePatterns(pt, cfg)
	if len(patterns) != 2 {
		t.Fatalf("expected 2 patterns for dual-protocol, got %d: %v", len(patterns), patterns)
	}
	httpFound, httpsFound := false, false
	for _, p := range patterns {
		if containsStr(p, ":s=http*") {
			httpFound = true
		}
		if containsStr(p, ":s=https*") {
			httpsFound = true
		}
	}
	if !httpFound || !httpsFound {
		t.Errorf("expected both http and https patterns, got: %v", patterns)
	}
}

func TestBuildPurgePatterns_PathSweep_SingleScheme(t *testing.T) {
	// When IncludeProtocol=true and explicit scheme → single pattern.
	pt := &purgeTarget{
		mode:   purgeModePathSweep,
		path:   "/api/products",
		host:   "example.com",
		scheme: "https",
	}
	cfg := &KeyConfig{IncludeProtocol: true}
	patterns := buildPurgePatterns(pt, cfg)
	if len(patterns) != 1 {
		t.Fatalf("expected 1 pattern for known scheme, got %d: %v", len(patterns), patterns)
	}
	if !containsStr(patterns[0], ":s=https*") {
		t.Errorf("expected s=https* in pattern, got: %s", patterns[0])
	}
}

func TestBuildPurgePatterns_Wildcard_NoHost(t *testing.T) {
	pt := &purgeTarget{
		mode: purgeModeWildcard,
		path: "/assets",
	}
	cfg := &KeyConfig{}
	patterns := buildPurgePatterns(pt, cfg)
	if len(patterns) != 1 {
		t.Fatalf("expected 1 pattern, got %d: %v", len(patterns), patterns)
	}
	expected := "p=/assets/*"
	if patterns[0] != expected {
		t.Errorf("expected %q\n got %q", expected, patterns[0])
	}
}

func TestBuildPurgePatterns_Wildcard_WithHost(t *testing.T) {
	pt := &purgeTarget{
		mode: purgeModeWildcard,
		path: "/assets",
		host: "cdn.example.com",
	}
	cfg := &KeyConfig{}
	patterns := buildPurgePatterns(pt, cfg)
	if len(patterns) != 1 {
		t.Fatalf("expected 1 pattern, got %d: %v", len(patterns), patterns)
	}
	if !containsStr(patterns[0], "/assets/") || !containsStr(patterns[0], ":h=cdn.example.com") {
		t.Errorf("expected path prefix and host scope, got: %s", patterns[0])
	}
}

func TestBuildPurgePatterns_Wildcard_DualProtocol(t *testing.T) {
	pt := &purgeTarget{
		mode: purgeModeWildcard,
		path: "/assets",
		host: "cdn.example.com",
	}
	cfg := &KeyConfig{IncludeProtocol: true}
	patterns := buildPurgePatterns(pt, cfg)
	if len(patterns) != 2 {
		t.Fatalf("expected 2 patterns for dual-protocol wildcard, got %d: %v", len(patterns), patterns)
	}
}

func TestBuildPurgePatterns_Wildcard_RootPath(t *testing.T) {
	pt := &purgeTarget{
		mode: purgeModeWildcard,
		path: "/",
	}
	cfg := &KeyConfig{}
	patterns := buildPurgePatterns(pt, cfg)
	// Root wildcard must match everything.
	if !containsStr(patterns[0], "p=/*") {
		t.Errorf("expected root wildcard pattern, got: %s", patterns[0])
	}
}

func TestBuildPurgePatterns_Nil(t *testing.T) {
	patterns := buildPurgePatterns(nil, &KeyConfig{})
	if len(patterns) != 0 {
		t.Errorf("expected empty patterns for nil target, got: %v", patterns)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// normalizeHost
// ─────────────────────────────────────────────────────────────────────────────

func TestNormalizeHost(t *testing.T) {
	tests := []struct {
		host     string
		scheme   string
		expected string
	}{
		{"EXAMPLE.COM", "", "example.com"},
		{"example.com:80", "http", "example.com"},
		{"example.com:443", "https", "example.com"},
		{"example.com:8080", "http", "example.com:8080"},
		{"example.com:443", "http", "example.com:443"}, // :443 on http is NOT stripped
		{"Example.COM:80", "https", "example.com:80"},   // :80 on https is NOT stripped
	}
	for _, tt := range tests {
		got := normalizeHost(tt.host, tt.scheme)
		if got != tt.expected {
			t.Errorf("normalizeHost(%q, %q) = %q, want %q", tt.host, tt.scheme, got, tt.expected)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// helpers
// ─────────────────────────────────────────────────────────────────────────────

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// ─────────────────────────────────────────────────────────────────────────────
// End-to-End Live Purge Integration Tests
// ─────────────────────────────────────────────────────────────────────────────

func TestPurge_EndToEnd_MatrixOfTargets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		cachedURL     string
		purgeTarget   string
		expectDeleted bool
		keyCfg        KeyConfig
	}{
		{
			name:          "PathOnly_Sweep_PurgesFullHostKey",
			cachedURL:     "http://localhost:8080/api/time",
			purgeTarget:   "/api/time",
			expectDeleted: true,
		},
		{
			name:          "FullURL_PurgesExactHostKey",
			cachedURL:     "http://localhost:8080/api/time",
			purgeTarget:   "http://localhost:8080/api/time",
			expectDeleted: true,
		},
		{
			name:          "DifferentHost_DoesNotPurge",
			cachedURL:     "http://localhost:8080/api/time",
			purgeTarget:   "http://otherdomain.com/api/time",
			expectDeleted: false,
		},
		{
			name:          "PathSweep_PurgesAllQueryVariations",
			cachedURL:     "http://localhost:8080/api/products?page=2&limit=50",
			purgeTarget:   "/api/products",
			expectDeleted: true,
		},
		{
			name:          "QuerySpecificPurge_UnorderedAndMarketingStripped",
			cachedURL:     "http://localhost:8080/api/products?a=1&b=2",
			purgeTarget:   "/api/products?b=2&a=1&utm_source=twitter",
			expectDeleted: true,
			keyCfg:        KeyConfig{ExcludeMarketingParams: true},
		},
		{
			name:          "WildcardDirectory_PurgesDeepChildren",
			cachedURL:     "http://localhost:8080/assets/css/theme/dark.css",
			purgeTarget:   "/assets/*",
			expectDeleted: true,
		},
		{
			name:          "RootWildcard_PurgesEntireNamespace",
			cachedURL:     "http://localhost:8080/deep/nested/page",
			purgeTarget:   "/",
			expectDeleted: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, _, engine := setupTestTitip(t, WithKeyConfig(tt.keyCfg))

			var originCalls atomic.Int64
			origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				originCalls.Add(1)
				w.Header().Set("Cache-Control", "public, max-age=300")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("payload-" + tt.name))
			})
			handler := engine.testHandler(origin)

			// Step 1: Prime cache
			req1 := httptest.NewRequest(http.MethodGet, tt.cachedURL, nil)
			rec1 := httptest.NewRecorder()
			handler.ServeHTTP(rec1, req1)
			if originCalls.Load() != 1 {
				t.Fatalf("expected 1 origin call to prime cache, got %d", originCalls.Load())
			}

			// Step 2: Confirm cache HIT
			req2 := httptest.NewRequest(http.MethodGet, tt.cachedURL, nil)
			rec2 := httptest.NewRecorder()
			handler.ServeHTTP(rec2, req2)
			if originCalls.Load() != 1 {
				t.Fatalf("expected cache HIT (0 additional origin calls), got %d", originCalls.Load())
			}

			// Step 3: Execute Purge
			count, err := engine.Purge(context.Background(), tt.purgeTarget)
			if err != nil {
				t.Fatalf("purge error: %v", err)
			}

			if tt.expectDeleted && count == 0 {
				t.Errorf("expected at least 1 deleted entry, got 0")
			} else if !tt.expectDeleted && count > 0 {
				t.Errorf("expected 0 deleted entries for mismatched host, got %d", count)
			}

			// Step 4: Verify whether request afterwards is a MISS or still HIT
			req3 := httptest.NewRequest(http.MethodGet, tt.cachedURL, nil)
			rec3 := httptest.NewRecorder()
			handler.ServeHTTP(rec3, req3)

			if tt.expectDeleted {
				if originCalls.Load() != 2 {
					t.Errorf("expected origin call #2 (cache was purged), got %d", originCalls.Load())
				}
			} else {
				if originCalls.Load() != 1 {
					t.Errorf("expected cache to remain intact (still HIT), got %d origin calls", originCalls.Load())
				}
			}
		})
	}
}

func TestPurgeTarget_CaseInsensitivePath(t *testing.T) {
	cfg := &KeyConfig{CaseInsensitivePath: true}

	// 1. Exact mode
	ptExact, err := parsePurgeTarget("http://example.com/Products/Shoes/Running?token=123", cfg)
	if err != nil {
		t.Fatalf("parsePurgeTarget failed: %v", err)
	}
	if ptExact.path != "/products/shoes/running" {
		t.Errorf("expected lowercase path /products/shoes/running, got %s", ptExact.path)
	}
	exactPatterns := buildPurgePatterns(ptExact, cfg)
	expectedKey := "p=/products/shoes/running:h=example.com:m=GET:qs=token=123"
	if len(exactPatterns) != 1 || exactPatterns[0] != expectedKey {
		t.Errorf("expected exact pattern %q, got %v", expectedKey, exactPatterns)
	}

	// 2. Path sweep mode
	ptSweep, err := parsePurgeTarget("http://example.com/Products/Shoes/Running", cfg)
	if err != nil {
		t.Fatalf("parsePurgeTarget failed: %v", err)
	}
	if ptSweep.path != "/products/shoes/running" {
		t.Errorf("expected lowercase path /products/shoes/running, got %s", ptSweep.path)
	}
	sweepPatterns := buildPurgePatterns(ptSweep, cfg)
	if len(sweepPatterns) != 1 || sweepPatterns[0] != "p=/products/shoes/running:h=example.com:m=*" {
		t.Errorf("expected sweep pattern %q, got %v", "p=/products/shoes/running:h=example.com:m=*", sweepPatterns)
	}

	// 3. Wildcard mode
	ptWildcard, err := parsePurgeTarget("http://example.com/Products/*", cfg)
	if err != nil {
		t.Fatalf("parsePurgeTarget failed: %v", err)
	}
	if ptWildcard.path != "/products" {
		t.Errorf("expected lowercase wildcard dir /products, got %s", ptWildcard.path)
	}
	wildcardPatterns := buildPurgePatterns(ptWildcard, cfg)
	if len(wildcardPatterns) != 1 || wildcardPatterns[0] != "p=/products/*:h=example.com:m=*" {
		t.Errorf("expected wildcard pattern %q, got %v", "p=/products/*:h=example.com:m=*", wildcardPatterns)
	}
}
