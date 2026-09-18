package titip

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/indragunawan/titip/internal/teststore"
)

// ─────────────────────────────────────────────────────────────────────────────
// buildPurgeOperation Exhaustive Table Test
// ─────────────────────────────────────────────────────────────────────────────

func TestBuildPurgeOperation_Table(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		target          string
		cfg             CacheKey
		expectedExact   bool
		expectedKeys    []string
		sampleCachedURL string // must match via exact equality or matchGlob == true
		unrelatedURL    string // must NOT match via matchGlob
	}{
		{
			name:            "PathOnly_DefaultConfig",
			target:          "/api/products",
			cfg:             CacheKey{},
			expectedExact:   false,
			expectedKeys:    []string{"p=/api/products:*"},
			sampleCachedURL: "http://example.com/api/products?id=42",
			unrelatedURL:    "http://example.com/api/products_other?id=42",
		},
		{
			name:            "PathOnly_ExcludeHost",
			target:          "/api/products",
			cfg:             CacheKey{ExcludeHost: true},
			expectedExact:   false,
			expectedKeys:    []string{"p=/api/products:*"},
			sampleCachedURL: "http://example.com/api/products",
			unrelatedURL:    "http://example.com/api/v2",
		},
		{
			name:            "PathOnly_IncludeProtocol_NoSchemeInTarget",
			target:          "/api/products",
			cfg:             CacheKey{IncludeProtocol: true},
			expectedExact:   false,
			expectedKeys:    []string{"p=/api/products:*"},
			sampleCachedURL: "https://example.com/api/products",
			unrelatedURL:    "https://example.com/api/orders",
		},
		{
			name:            "PathOnly_HostScoped_DefaultProtocol",
			target:          "https://example.com/api/products",
			cfg:             CacheKey{},
			expectedExact:   false,
			expectedKeys:    []string{"p=/api/products:h=example.com:*"},
			sampleCachedURL: "http://example.com/api/products?id=1",
			unrelatedURL:    "http://other.com/api/products?id=1",
		},
		{
			name:            "PathOnly_HostScoped_IncludeProtocol_HTTPS",
			target:          "https://example.com/api/products",
			cfg:             CacheKey{IncludeProtocol: true},
			expectedExact:   false,
			expectedKeys:    []string{"p=/api/products:h=example.com:s=https:*"},
			sampleCachedURL: "https://example.com/api/products",
			unrelatedURL:    "http://example.com/api/products", // http must NOT match https
		},
		{
			name:            "PathOnly_HostScoped_IncludeProtocol_HTTP",
			target:          "http://example.com/api/products",
			cfg:             CacheKey{IncludeProtocol: true},
			expectedExact:   false,
			expectedKeys:    []string{"p=/api/products:h=example.com:s=http:*"},
			sampleCachedURL: "http://example.com/api/products",
			unrelatedURL:    "https://example.com/api/products", // https must NOT match http
		},
		{
			name:            "PathOnly_CaseInsensitivePath",
			target:          "/API/Products",
			cfg:             CacheKey{CaseInsensitivePath: true},
			expectedExact:   false,
			expectedKeys:    []string{"p=/api/products:*"},
			sampleCachedURL: "http://example.com/api/products",
			unrelatedURL:    "http://example.com/api/other",
		},
		{
			name:            "ColonInPath_Supported",
			target:          "/api/users/id:123",
			cfg:             CacheKey{},
			expectedExact:   false,
			expectedKeys:    []string{"p=/api/users/id:123:*"},
			sampleCachedURL: "http://example.com/api/users/id:123?page=1",
			unrelatedURL:    "http://example.com/api/users/id:124?page=1",
		},
		{
			name:            "FullURL_ExactQuery_O1Purge",
			target:          "https://example.com/api/products?id=42",
			cfg:             CacheKey{},
			expectedExact:   true,
			expectedKeys:    []string{"p=/api/products:h=example.com:qs=id=42:m=GET:"},
			sampleCachedURL: "https://example.com/api/products?id=42",
			unrelatedURL:    "https://example.com/api/products?id=420",
		},
		{
			name:            "PathWithQuery_ExcludeHost_ExactPurge",
			target:          "/api/products?id=42",
			cfg:             CacheKey{ExcludeHost: true},
			expectedExact:   true,
			expectedKeys:    []string{"p=/api/products:qs=id=42:m=GET:"},
			sampleCachedURL: "http://example.com/api/products?id=42",
			unrelatedURL:    "http://example.com/api/products?id=43",
		},
		{
			name:            "PathWithQuery_NoHost_PatternPurge",
			target:          "/api/products?id=42",
			cfg:             CacheKey{},
			expectedExact:   false,
			expectedKeys:    []string{"p=/api/products:h=*:qs=id=42:*"},
			sampleCachedURL: "http://anydomain.com/api/products?id=42",
			unrelatedURL:    "http://anydomain.com/api/products?id=420", // must NOT match id=420
		},
		{
			name:            "QuerySorting_Default_Sorted",
			target:          "https://example.com/api?b=2&a=1",
			cfg:             CacheKey{},
			expectedExact:   true,
			expectedKeys:    []string{"p=/api:h=example.com:qs=a=1&b=2:m=GET:"},
			sampleCachedURL: "https://example.com/api?a=1&b=2",
			unrelatedURL:    "https://example.com/api?a=1&b=3",
		},
		{
			name:            "QuerySorting_PreserveQueryOrder_PreservesOrder",
			target:          "https://example.com/api?b=2&a=1",
			cfg:             CacheKey{PreserveQueryOrder: true},
			expectedExact:   true,
			expectedKeys:    []string{"p=/api:h=example.com:qs=b=2&a=1:m=GET:"},
			sampleCachedURL: "https://example.com/api?b=2&a=1",
			unrelatedURL:    "https://example.com/api?a=1&b=2",
		},
		{
			name:            "ColonInQuery_PercentEncodedSafely",
			target:          "https://example.com/api?time=12:30:00",
			cfg:             CacheKey{},
			expectedExact:   true,
			expectedKeys:    []string{"p=/api:h=example.com:qs=time=12%3A30%3A00:m=GET:"},
			sampleCachedURL: "https://example.com/api?time=12:30:00",
			unrelatedURL:    "https://example.com/api?time=12:30:01",
		},
		{
			name:            "ExcludeQuery_IgnoresQuery",
			target:          "/api/products?id=42",
			cfg:             CacheKey{ExcludeQuery: true},
			expectedExact:   false,
			expectedKeys:    []string{"p=/api/products:*"},
			sampleCachedURL: "http://example.com/api/products",
			unrelatedURL:    "http://example.com/api/categories",
		},
		{
			name:            "ExcludeMarketingQueryParams_Stripped",
			target:          "/api/products?id=42&utm_source=twitter",
			cfg:             CacheKey{ExcludeMarketingQueryParams: true},
			expectedExact:   false,
			expectedKeys:    []string{"p=/api/products:h=*:qs=id=42:*"},
			sampleCachedURL: "http://example.com/api/products?id=42&utm_source=twitter",
			unrelatedURL:    "http://example.com/api/products?id=43",
		},
		{
			name:            "URLWithPort_CustomPortPreserved",
			target:          "http://example.com:8080/api",
			cfg:             CacheKey{},
			expectedExact:   false,
			expectedKeys:    []string{"p=/api:h=example.com:8080:*"},
			sampleCachedURL: "http://example.com:8080/api?page=1",
			unrelatedURL:    "http://example.com/api?page=1", // without port must not match
		},
		{
			name:            "URLWithPort_DefaultPort80Stripped",
			target:          "http://example.com:80/api",
			cfg:             CacheKey{},
			expectedExact:   false,
			expectedKeys:    []string{"p=/api:h=example.com:*"},
			sampleCachedURL: "http://example.com/api",
			unrelatedURL:    "http://other.com/api",
		},
		{
			name:            "URLWithPort_DefaultPort443Stripped",
			target:          "https://example.com:443/api",
			cfg:             CacheKey{},
			expectedExact:   false,
			expectedKeys:    []string{"p=/api:h=example.com:*"},
			sampleCachedURL: "https://example.com/api",
			unrelatedURL:    "https://other.com/api",
		},
		{
			name:            "Homepage_Only",
			target:          "/",
			cfg:             CacheKey{},
			expectedExact:   false,
			expectedKeys:    []string{"p=/:*"},
			sampleCachedURL: "http://example.com/",
			unrelatedURL:    "http://example.com/api", // must NOT match /api
		},
		{
			name:            "LiteralAsteriskInPath_PercentEncoded",
			target:          "/math/2*2",
			cfg:             CacheKey{},
			expectedExact:   false,
			expectedKeys:    []string{"p=/math/2%2A2:*"},
			sampleCachedURL: "http://example.com/math/2*2?id=1",
			unrelatedURL:    "http://example.com/math/2lesson2?id=1", // must NOT match /math/2lesson2
		},
		{
			name:            "LiteralBracketInPath_PercentEncoded",
			target:          "/search/item[1]",
			cfg:             CacheKey{},
			expectedExact:   false,
			expectedKeys:    []string{"p=/search/item%5B1%5D:*"},
			sampleCachedURL: "http://example.com/search/item[1]?id=1",
			unrelatedURL:    "http://example.com/search/item1?id=1", // must NOT match /search/item1
		},
		{
			name:            "NonASCII_ChinesePath_PatternPurge",
			target:          "/你好/世界?id=42",
			cfg:             CacheKey{},
			expectedExact:   false,
			expectedKeys:    []string{"p=/%E4%BD%A0%E5%A5%BD/%E4%B8%96%E7%95%8C:h=*:qs=id=42:*"},
			sampleCachedURL: "http://example.com/你好/世界?id=42",
			unrelatedURL:    "http://example.com/你好/世界?id=43",
		},
		{
			name:            "NonASCII_ChinesePath_ExactPurge",
			target:          "http://example.com/你好/世界?id=42",
			cfg:             CacheKey{},
			expectedExact:   true,
			expectedKeys:    []string{"p=/%E4%BD%A0%E5%A5%BD/%E4%B8%96%E7%95%8C:h=example.com:qs=id=42:m=GET:"},
			sampleCachedURL: "http://example.com/你好/世界?id=42",
			unrelatedURL:    "http://example.com/你好/世界?id=43",
		},
		{
			name:            "PathWithSpaces_ExactPurge",
			target:          "http://example.com/hello world/profile?id=42",
			cfg:             CacheKey{},
			expectedExact:   true,
			expectedKeys:    []string{"p=/hello%20world/profile:h=example.com:qs=id=42:m=GET:"},
			sampleCachedURL: "http://example.com/hello world/profile?id=42",
			unrelatedURL:    "http://example.com/hello world/profile?id=43",
		},
		{
			name:            "PathWithPreEscapedPercent_ExactPurge",
			target:          "http://example.com/deal/50%25off?id=42",
			cfg:             CacheKey{},
			expectedExact:   true,
			expectedKeys:    []string{"p=/deal/50%25off:h=example.com:qs=id=42:m=GET:"},
			sampleCachedURL: "http://example.com/deal/50%25off?id=42",
			unrelatedURL:    "http://example.com/deal/50%25off?id=43",
		},
		{
			name:            "PathWithEmoji_ExactPurge",
			target:          "http://example.com/shop/🎉/item?id=42",
			cfg:             CacheKey{},
			expectedExact:   true,
			expectedKeys:    []string{"p=/shop/%F0%9F%8E%89/item:h=example.com:qs=id=42:m=GET:"},
			sampleCachedURL: "http://example.com/shop/🎉/item?id=42",
			unrelatedURL:    "http://example.com/shop/🎉/item?id=43",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isExact, keys, err := buildPurgeOperation(tt.target, &tt.cfg)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if isExact != tt.expectedExact {
				t.Errorf("isExact = %v, want %v", isExact, tt.expectedExact)
			}
			if !slices.Equal(keys, tt.expectedKeys) {
				t.Errorf("keys mismatch:\n got:  %v\n want: %v", keys, tt.expectedKeys)
			}

			// Sample request verification
			sampleU, _ := url.Parse(tt.sampleCachedURL)
			sampleReq := &http.Request{
				Method: http.MethodGet,
				Host:   sampleU.Host,
				URL:    sampleU,
			}
			if sampleU.Scheme == "https" {
				sampleReq.Header = http.Header{"X-Forwarded-Proto": []string{"https"}}
			}
			primaryKey := generatePrimaryKey(sampleReq, &tt.cfg)

			if isExact {
				// Exact match must equal primaryKey 100% identically!
				if keys[0] != primaryKey {
					t.Errorf("exact key parity mismatch:\n exact key:  %q\n primaryKey: %q", keys[0], primaryKey)
				}
			} else {
				// Pattern match must match sampleKey via Redis glob matching!
				matched := false
				for _, p := range keys {
					if matchGlob(p, primaryKey) {
						matched = true
						break
					}
				}
				if !matched {
					t.Errorf("pattern failed to match primaryKey:\n patterns:   %v\n primaryKey: %q", keys, primaryKey)
				}

				// Pattern must NOT match unrelated key
				unrelatedU, _ := url.Parse(tt.unrelatedURL)
				unrelatedReq := &http.Request{
					Method: http.MethodGet,
					Host:   unrelatedU.Host,
					URL:    unrelatedU,
				}
				if unrelatedU.Scheme == "https" {
					unrelatedReq.Header = http.Header{"X-Forwarded-Proto": []string{"https"}}
				}
				unrelatedKey := generatePrimaryKey(unrelatedReq, &tt.cfg)
				for _, p := range keys {
					if matchGlob(p, unrelatedKey) && tt.target != "/" {
						t.Errorf("pattern over-matched unrelatedKey:\n pattern:      %q\n unrelatedKey: %q", p, unrelatedKey)
					}
				}
			}
		})
	}
}

func TestBuildPurgeOperation_EmptyTarget(t *testing.T) {
	isExact, keys, err := buildPurgeOperation("", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isExact || len(keys) != 0 {
		t.Errorf("expected false, empty for empty target, got: isExact=%v, keys=%v", isExact, keys)
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
		{"Example.COM:80", "https", "example.com:80"},  // :80 on https is NOT stripped
	}
	for _, tt := range tests {
		got := normalizeHost(tt.host, tt.scheme)
		if got != tt.expected {
			t.Errorf("normalizeHost(%q, %q) = %q, want %q", tt.host, tt.scheme, got, tt.expected)
		}
	}
}

// matchGlob performs Redis-style glob pattern matching (* matches any sequence including /, ? matches single char, \ escapes).
func matchGlob(pattern, s string) bool {
	var sb strings.Builder
	sb.WriteString("^")
	for i := 0; i < len(pattern); i++ {
		c := pattern[i]
		if c == '\\' && i+1 < len(pattern) {
			i++
			sb.WriteString(regexp.QuoteMeta(string(pattern[i])))
			continue
		}
		switch c {
		case '*':
			sb.WriteString(".*")
		case '?':
			sb.WriteString(".")
		default:
			sb.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	sb.WriteString("$")
	re, err := regexp.Compile(sb.String())
	if err != nil {
		return false
	}
	return re.MatchString(s)
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
		keyCfg        CacheKey
	}{
		{
			name:          "PathOnly_AllVariants_PurgesFullHostKey",
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
			name:          "PathAllVariants_PurgesAllQueryVariations",
			cachedURL:     "http://localhost:8080/api/products?page=2&limit=50",
			purgeTarget:   "/api/products",
			expectDeleted: true,
		},
		{
			name:          "QuerySpecificPurge_UnorderedAndMarketingStripped",
			cachedURL:     "http://localhost:8080/api/products?a=1&b=2",
			purgeTarget:   "/api/products?b=2&a=1&utm_source=twitter",
			expectDeleted: true,
			keyCfg:        CacheKey{ExcludeMarketingQueryParams: true},
		},
		{
			name:          "LiteralAsteriskPath_PurgesExactPath",
			cachedURL:     "http://localhost:8080/math/2*2",
			purgeTarget:   "/math/2*2",
			expectDeleted: true,
		},
		{
			name:          "RootPathOnly_PurgesRootURL",
			cachedURL:     "http://localhost:8080/",
			purgeTarget:   "/",
			expectDeleted: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, _, engine := setupTestTitip(t, WithCacheKey(tt.keyCfg))

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
	cfg := &CacheKey{CaseInsensitivePath: true}

	// 1. Exact mode
	isExact, keys, err := buildPurgeOperation("http://example.com/Products/Shoes/Running?token=123", cfg)
	if err != nil {
		t.Fatalf("buildPurgeOperation failed: %v", err)
	}
	if !isExact {
		t.Errorf("expected isExact = true")
	}
	expectedKey := "p=/products/shoes/running:h=example.com:qs=token=123:m=GET:"
	if len(keys) != 1 || keys[0] != expectedKey {
		t.Errorf("expected exact key %q, got %v", expectedKey, keys)
	}

	// 2. Path all-variants mode
	isExact, keys, err = buildPurgeOperation("http://example.com/Products/Shoes/Running", cfg)
	if err != nil {
		t.Fatalf("buildPurgeOperation failed: %v", err)
	}
	if isExact {
		t.Errorf("expected isExact = false")
	}
	expectedPattern := "p=/products/shoes/running:h=example.com:*"
	if len(keys) != 1 || keys[0] != expectedPattern {
		t.Errorf("expected pattern %q, got %v", expectedPattern, keys)
	}

	// 3. Literal asterisk in Purge is escaped
	isExact, keys, err = buildPurgeOperation("http://example.com/Products/*", cfg)
	if err != nil {
		t.Fatalf("buildPurgeOperation failed: %v", err)
	}
	if isExact {
		t.Errorf("expected isExact = false")
	}
	expectedEscaped := "p=/products/%2A:h=example.com:*"
	if len(keys) != 1 || keys[0] != expectedEscaped {
		t.Errorf("expected literal asterisk escaped %q, got %v", expectedEscaped, keys)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// buildPurgePrefixOperation Tests
// ─────────────────────────────────────────────────────────────────────────────

func TestBuildPurgePrefixOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		prefix       string
		cfg          CacheKey
		expectedKeys []string
		expectErr    bool
	}{
		{
			name:      "EmptyPrefix_Error",
			prefix:    "",
			expectErr: true,
		},
		{
			name:         "RootPrefix_WipesEntireNamespace",
			prefix:       "/",
			expectedKeys: []string{"p=/*"},
		},
		{
			name:         "DirectoryPrefix_WithTrailingSlash",
			prefix:       "/assets/",
			expectedKeys: []string{"p=/assets/*"},
		},
		{
			name:         "RawStringPrefix_WithoutTrailingSlash",
			prefix:       "/assets",
			expectedKeys: []string{"p=/assets*"},
		},
		{
			name:         "HostScopedPrefix_WithoutProtocol",
			prefix:       "http://example.com/assets/",
			expectedKeys: []string{"p=/assets/*:h=example.com:*"},
		},
		{
			name:         "HostScopedPrefix_WithProtocol",
			prefix:       "https://example.com/assets/",
			cfg:          CacheKey{IncludeProtocol: true},
			expectedKeys: []string{"p=/assets/*:h=example.com:s=https:*"},
		},
		{
			name:         "CaseInsensitive_Prefix",
			prefix:       "/Assets/Images/",
			cfg:          CacheKey{CaseInsensitivePath: true},
			expectedKeys: []string{"p=/assets/images/*"},
		},
		{
			name:         "ExcludeHost_PrefixIgnoresHost",
			prefix:       "https://example.com/assets/",
			cfg:          CacheKey{ExcludeHost: true},
			expectedKeys: []string{"p=/assets/*"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keys, err := buildPurgePrefixOperation(tt.prefix, &tt.cfg)
			if tt.expectErr {
				if err == nil {
					t.Errorf("expected error for prefix %q, got nil", tt.prefix)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for prefix %q: %v", tt.prefix, err)
			}
			if !slices.Equal(keys, tt.expectedKeys) {
				t.Errorf("keys mismatch:\n got:  %v\n want: %v", keys, tt.expectedKeys)
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// PurgePrefix Integration Test with Titip
// ─────────────────────────────────────────────────────────────────────────────

func TestTitip_PurgePrefix_Integration(t *testing.T) {
	t.Parallel()

	// Step 1: Initialize Titip with teststore
	store := teststore.New()
	titipInst, err := New(
		store,
		WithCacheKey(CacheKey{}),
	)
	if err != nil {
		t.Fatalf("failed to initialize Titip: %v", err)
	}
	defer func() { _ = titipInst.Close(context.Background()) }()

	originCalls := atomic.Int64{}
	handler := titipInst.testHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=3600")
		_, _ = w.Write([]byte("origin body: " + r.URL.Path))
	}))

	seedURL := func(urlStr string) {
		req := httptest.NewRequest(http.MethodGet, urlStr, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}

	isCached := func(urlStr string) bool {
		callsBefore := originCalls.Load()
		req := httptest.NewRequest(http.MethodGet, urlStr, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return originCalls.Load() == callsBefore // no origin call = cache HIT
	}

	// Seed multiple URLs
	seedURL("http://example.com/assets/css/style.css")
	seedURL("http://example.com/assets/img/logo.png")
	seedURL("http://example.com/assets-v2/app.js")
	seedURL("http://example.com/api/products")

	// Verify all 4 are cached
	if !isCached("http://example.com/assets/css/style.css") ||
		!isCached("http://example.com/assets/img/logo.png") ||
		!isCached("http://example.com/assets-v2/app.js") ||
		!isCached("http://example.com/api/products") {
		t.Fatalf("failed to prime cache")
	}

	// Step 2: Purge with trailing slash "/assets/"
	// Must purge /assets/css/style.css and /assets/img/logo.png
	// Must NOT purge /assets-v2/app.js or /api/products
	n, err := titipInst.PurgePrefix(context.Background(), "/assets/")
	if err != nil {
		t.Fatalf("PurgePrefix(/assets/) failed: %v", err)
	}
	if n < 2 {
		t.Errorf("expected at least 2 purged items, got %d", n)
	}

	if isCached("http://example.com/assets/css/style.css") {
		t.Errorf("/assets/css/style.css should have been purged")
	}
	if isCached("http://example.com/assets/img/logo.png") {
		t.Errorf("/assets/img/logo.png should have been purged")
	}
	if !isCached("http://example.com/assets-v2/app.js") {
		t.Errorf("/assets-v2/app.js should NOT have been purged by /assets/")
	}
	if !isCached("http://example.com/api/products") {
		t.Errorf("/api/products should NOT have been purged")
	}

	// Step 3: Purge without trailing slash "/assets"
	// Must purge /assets-v2/app.js (Cloudflare-style raw string prefix)
	n, err = titipInst.PurgePrefix(context.Background(), "/assets")
	if err != nil {
		t.Fatalf("PurgePrefix(/assets) failed: %v", err)
	}
	if n < 1 {
		t.Errorf("expected at least 1 purged item, got %d", n)
	}
	if isCached("http://example.com/assets-v2/app.js") {
		t.Errorf("/assets-v2/app.js should have been purged by raw prefix /assets")
	}

	// Step 4: Global soft-purge with "/"
	// Re-seed /api/products
	seedURL("http://example.com/api/products")
	if !isCached("http://example.com/api/products") {
		t.Fatalf("failed to re-prime /api/products")
	}

	n, err = titipInst.PurgePrefix(context.Background(), "/", WithSoftPurge())
	if err != nil {
		t.Fatalf("PurgePrefix(/, WithSoftPurge) failed: %v", err)
	}
	if n < 1 {
		t.Errorf("expected at least 1 soft-purged item, got %d", n)
	}
}

func TestTitip_Purge_LiteralAsterisk_DoesNotPurgeSiblings(t *testing.T) {
	t.Parallel()

	store := teststore.New()
	titipInst, err := New(
		store,
		WithCacheKey(CacheKey{}),
	)
	if err != nil {
		t.Fatalf("failed to initialize Titip: %v", err)
	}
	defer func() { _ = titipInst.Close(context.Background()) }()

	originCalls := atomic.Int64{}
	handler := titipInst.testHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=3600")
		_, _ = w.Write([]byte("origin body: " + r.URL.Path))
	}))

	seedURL := func(urlStr string) {
		req := httptest.NewRequest(http.MethodGet, urlStr, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}

	isCached := func(urlStr string) bool {
		callsBefore := originCalls.Load()
		req := httptest.NewRequest(http.MethodGet, urlStr, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return originCalls.Load() == callsBefore
	}

	seedURL("http://example.com/math/2*2")
	seedURL("http://example.com/math/2times2")
	seedURL("http://example.com/math/2lesson2")

	if !isCached("http://example.com/math/2*2") ||
		!isCached("http://example.com/math/2times2") ||
		!isCached("http://example.com/math/2lesson2") {
		t.Fatalf("failed to prime cache")
	}

	// Purge exact /math/2*2 - must NOT match 2times2 or 2lesson2 because * is quoted as \*
	n, err := titipInst.Purge(context.Background(), "/math/2*2")
	if err != nil {
		t.Fatalf("Purge(/math/2*2) failed: %v", err)
	}
	if n != 1 {
		t.Errorf("expected exactly 1 purged item, got %d", n)
	}

	if isCached("http://example.com/math/2*2") {
		t.Errorf("/math/2*2 should have been purged")
	}
	if !isCached("http://example.com/math/2times2") {
		t.Errorf("/math/2times2 was accidentally purged by literal asterisk!")
	}
	if !isCached("http://example.com/math/2lesson2") {
		t.Errorf("/math/2lesson2 was accidentally purged by literal asterisk!")
	}
}

func TestTitip_PurgePrefix_Multilingual(t *testing.T) {
	t.Parallel()

	store := teststore.New()
	titipInst, err := New(
		store,
		WithCacheKey(CacheKey{}),
	)
	if err != nil {
		t.Fatalf("failed to initialize Titip: %v", err)
	}
	defer func() { _ = titipInst.Close(context.Background()) }()

	originCalls := atomic.Int64{}
	handler := titipInst.testHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=3600")
		_, _ = w.Write([]byte("origin body: " + r.URL.Path))
	}))

	seedURL := func(urlStr string) {
		req := httptest.NewRequest(http.MethodGet, urlStr, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}

	isCached := func(urlStr string) bool {
		callsBefore := originCalls.Load()
		req := httptest.NewRequest(http.MethodGet, urlStr, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return originCalls.Load() == callsBefore
	}

	seedURL("http://example.com/你好/世界")
	seedURL("http://example.com/你好/朋友")
	seedURL("http://example.com/欢迎/世界")

	if !isCached("http://example.com/你好/世界") ||
		!isCached("http://example.com/你好/朋友") ||
		!isCached("http://example.com/欢迎/世界") {
		t.Fatalf("failed to prime multilingual cache")
	}

	// PurgePrefix on /你好/ directory
	n, err := titipInst.PurgePrefix(context.Background(), "/你好/")
	if err != nil {
		t.Fatalf("PurgePrefix(/你好/) failed: %v", err)
	}
	if n < 2 {
		t.Errorf("expected at least 2 purged multilingual items, got %d", n)
	}

	if isCached("http://example.com/你好/世界") {
		t.Errorf("/你好/世界 should have been purged")
	}
	if isCached("http://example.com/你好/朋友") {
		t.Errorf("/你好/朋友 should have been purged")
	}
	if !isCached("http://example.com/欢迎/世界") {
		t.Errorf("/欢迎/世界 should NOT have been purged")
	}
}

func TestTitip_PurgePrefix_Errors(t *testing.T) {
	t.Parallel()

	store := teststore.New()
	titipInst, err := New(store)
	if err != nil {
		t.Fatalf("failed to initialize Titip: %v", err)
	}
	defer func() { _ = titipInst.Close(context.Background()) }()

	// Empty prefix must return error
	_, err = titipInst.PurgePrefix(context.Background(), "")
	if err == nil {
		t.Errorf("expected error on empty prefix, got nil")
	}

	// Invalid URL scheme must return error
	_, err = titipInst.PurgePrefix(context.Background(), "://invalid-prefix")
	if err == nil {
		t.Errorf("expected error on invalid prefix, got nil")
	}
}
