package titip

import (
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

	// 3. Trailing slash normalized to stripped path.
	pt3, err := parsePurgeTarget("/docs/", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pt3.path != "/docs" {
		t.Errorf("expected trailing slash stripped to /docs, got: %s", pt3.path)
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
