package titip

import (
	"crypto/tls"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func mustParse(rawURL string) *url.URL {
	u, err := url.Parse(rawURL)
	if err != nil {
		panic(err)
	}
	return u
}

func makeReq(rawURL string) *http.Request {
	u := mustParse(rawURL)
	return &http.Request{
		Method: http.MethodGet,
		Host:   u.Host,
		URL:    u,
		Header: http.Header{},
	}
}

func indexOf(s, substr string) int {
	i := 0
	for i <= len(s)-len(substr) {
		if s[i:i+len(substr)] == substr {
			return i
		}
		i++
	}
	return -1
}

func contains(s, substr string) bool {
	return indexOf(s, substr) != -1
}

// ─────────────────────────────────────────────────────────────────────────────
// Labeled Component Structure & Order
// ─────────────────────────────────────────────────────────────────────────────

func TestGeneratePrimaryKey_LabeledFormat(t *testing.T) {
	t.Parallel()

	t.Run("basic and nil config", func(t *testing.T) {
		req := makeReq("http://example.com/api/items")
		expected := "p=/api/items:h=example.com:m=GET:"

		if got := generatePrimaryKey(req, &CacheKey{}); got != expected {
			t.Errorf("expected %q, got %q", expected, got)
		}
		if got := generatePrimaryKey(req, nil); got != expected {
			t.Errorf("nil config: expected %q, got %q", expected, got)
		}
	})

	t.Run("component order", func(t *testing.T) {
		u := mustParse("https://secure.example.com/store?color=blue&size=m")
		req := &http.Request{
			Method: http.MethodGet,
			Host:   "secure.example.com",
			URL:    u,
			Header: http.Header{"X-Region": []string{"us-west"}},
			TLS:    &tls.ConnectionState{},
		}
		req.AddCookie(&http.Cookie{Name: "theme", Value: "dark"})

		cfg := &CacheKey{
			IncludeProtocol:     true,
			IncludedHeaderNames: []string{"X-Region"},
			IncludedCookieNames: []string{"theme"},
		}
		key := generatePrimaryKey(req, cfg)

		if len(key) < 2 || key[:2] != "p=" {
			t.Fatalf("key must start with 'p=', got: %s", key)
		}

		labels := []string{"p=", ":h=", ":s=", ":qs=", ":m=", ":he=", ":ck="}
		prev := 0
		for _, lbl := range labels {
			idx := indexOf(key, lbl)
			if idx == -1 {
				t.Fatalf("missing label %q in key: %s", lbl, key)
			}
			if idx < prev {
				t.Fatalf("label %q out of order in key: %s", lbl, key)
			}
			prev = idx
		}
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Method Normalization
// ─────────────────────────────────────────────────────────────────────────────

func TestGeneratePrimaryKey_Method(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		expected string
	}{
		{"GET always present", http.MethodGet, ":m=GET"},
		{"HEAD normalises to GET", http.MethodHead, ":m=GET"},
		{"empty normalises to GET", "", ":m=GET"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := makeReq("http://example.com/page")
			req.Method = tc.method
			key := generatePrimaryKey(req, &CacheKey{})
			if !contains(key, tc.expected) {
				t.Errorf("expected %q in key, got: %s", tc.expected, key)
			}
		})
	}

	t.Run("HEAD and GET produce identical keys", func(t *testing.T) {
		rGet := makeReq("http://example.com/page")
		rGet.Method = http.MethodGet
		rHead := makeReq("http://example.com/page")
		rHead.Method = http.MethodHead

		kGet := generatePrimaryKey(rGet, &CacheKey{})
		kHead := generatePrimaryKey(rHead, &CacheKey{})
		if kGet != kHead {
			t.Errorf("HEAD and GET keys differ:\n GET:  %s\n HEAD: %s", kGet, kHead)
		}
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Path Normalization
// ─────────────────────────────────────────────────────────────────────────────

func TestGeneratePrimaryKey_Path(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		req         *http.Request
		cfg         *CacheKey
		wantSegment string
	}{
		{
			name:        "trailing slash preserved",
			req:         makeReq("http://example.com/docs/"),
			cfg:         &CacheKey{},
			wantSegment: "p=/docs/:",
		},
		{
			name:        "root slash preserved",
			req:         makeReq("http://example.com/"),
			cfg:         &CacheKey{},
			wantSegment: "p=/:",
		},
		{
			name: "dot segments resolved",
			req: &http.Request{
				Method: http.MethodGet,
				Host:   "example.com",
				URL:    &url.URL{Path: "/a/b/../c/./d"},
				Header: http.Header{},
			},
			cfg:         &CacheKey{},
			wantSegment: "p=/a/c/d:",
		},
		{
			name: "nil URL falls back to root slash",
			req: &http.Request{
				Method: http.MethodGet,
				Host:   "example.com",
				Header: http.Header{},
			},
			cfg:         &CacheKey{},
			wantSegment: "p=/:",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key := generatePrimaryKey(tc.req, tc.cfg)
			if !contains(key, tc.wantSegment) {
				t.Errorf("expected segment %q in key, got: %s", tc.wantSegment, key)
			}
		})
	}

	t.Run("trailing slash distinctness", func(t *testing.T) {
		kWithout := generatePrimaryKey(makeReq("http://example.com/api"), &CacheKey{})
		kWith := generatePrimaryKey(makeReq("http://example.com/api/"), &CacheKey{})
		if kWithout == kWith {
			t.Errorf("/api and /api/ must have distinct primary keys: %s", kWithout)
		}
	})

	t.Run("case-insensitive path", func(t *testing.T) {
		reqUpper := makeReq("http://example.com/Products/Shoes/Running?token=AbC123")
		reqLower := makeReq("http://example.com/products/shoes/running?token=AbC123")
		cfg := &CacheKey{CaseInsensitivePath: true}

		kUpper := generatePrimaryKey(reqUpper, cfg)
		kLower := generatePrimaryKey(reqLower, cfg)

		expected := "p=/products/shoes/running:h=example.com:qs=token=AbC123:m=GET:"
		if kUpper != expected {
			t.Errorf("expected %q, got %q", expected, kUpper)
		}
		if kUpper != kLower {
			t.Errorf("uppercase and lowercase path keys must match: %q != %q", kUpper, kLower)
		}
		if !contains(kUpper, "token=AbC123") {
			t.Errorf("query parameter values must retain exact casing, got: %s", kUpper)
		}
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Host Normalization
// ─────────────────────────────────────────────────────────────────────────────

func TestGeneratePrimaryKey_Host(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		req      *http.Request
		cfg      *CacheKey
		expected string
	}{
		{
			name:     "host lowercased",
			req:      makeReq("http://Example.COM/api"),
			cfg:      &CacheKey{},
			expected: "p=/api:h=example.com:m=GET:",
		},
		{
			name:     "default HTTP port 80 stripped",
			req:      makeReq("http://example.com:80/api"),
			cfg:      &CacheKey{},
			expected: "p=/api:h=example.com:m=GET:",
		},
		{
			name: "default HTTPS port 443 stripped",
			req: &http.Request{
				Method: http.MethodGet,
				Host:   "secure.example.com:443",
				URL:    mustParse("https://secure.example.com:443/api"),
				TLS:    &tls.ConnectionState{},
				Header: http.Header{},
			},
			cfg:      &CacheKey{},
			expected: "p=/api:h=secure.example.com:m=GET:",
		},
		{
			name:     "non-default port preserved",
			req:      makeReq("http://example.com:8080/api"),
			cfg:      &CacheKey{},
			expected: "p=/api:h=example.com:8080:m=GET:",
		},
		{
			name:     "exclude host",
			req:      makeReq("http://cdn.example.com/assets/style.css"),
			cfg:      &CacheKey{ExcludeHost: true},
			expected: "p=/assets/style.css:m=GET:",
		},
		{
			name: "fallback to URL.Host when req.Host empty",
			req: &http.Request{
				Method: http.MethodGet,
				Host:   "",
				URL:    mustParse("http://fallback.example.com/path"),
				Header: http.Header{},
			},
			cfg:      &CacheKey{},
			expected: "p=/path:h=fallback.example.com:m=GET:",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := generatePrimaryKey(tc.req, tc.cfg)
			if got != tc.expected {
				t.Errorf("got %q, want %q", got, tc.expected)
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Scheme Normalization
// ─────────────────────────────────────────────────────────────────────────────

func TestGeneratePrimaryKey_Scheme(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		req         *http.Request
		cfg         *CacheKey
		wantSegment string
		absent      string
	}{
		{
			name: "TLS connection state yields s=https",
			req: &http.Request{
				Method: http.MethodGet,
				Host:   "secure.example.com",
				URL:    mustParse("https://secure.example.com/profile"),
				TLS:    &tls.ConnectionState{},
				Header: http.Header{},
			},
			cfg:         &CacheKey{IncludeProtocol: true},
			wantSegment: ":s=https",
		},
		{
			name: "X-Forwarded-Proto yields s=https",
			req: func() *http.Request {
				r := makeReq("http://api.example.com/v1/data")
				r.Header.Set("X-Forwarded-Proto", "https")
				return r
			}(),
			cfg:         &CacheKey{IncludeProtocol: true},
			wantSegment: ":s=https",
		},
		{
			name:        "plain HTTP yields s=http",
			req:         makeReq("http://api.example.com/v1/data"),
			cfg:         &CacheKey{IncludeProtocol: true},
			wantSegment: ":s=http",
		},
		{
			name: "URL.Scheme yields s=https",
			req: &http.Request{
				Method: http.MethodGet,
				Host:   "example.com",
				URL:    mustParse("https://example.com/api"),
				Header: http.Header{},
			},
			cfg:         &CacheKey{IncludeProtocol: true},
			wantSegment: ":s=https",
		},
		{
			name:   "scheme omitted by default",
			req:    makeReq("http://example.com/page"),
			cfg:    &CacheKey{},
			absent: ":s=",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key := generatePrimaryKey(tc.req, tc.cfg)
			if tc.wantSegment != "" && !contains(key, tc.wantSegment) {
				t.Errorf("expected %q in key: %s", tc.wantSegment, key)
			}
			if tc.absent != "" && contains(key, tc.absent) {
				t.Errorf("expected %q to be absent in key: %s", tc.absent, key)
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Query String Normalization & Filtering
// ─────────────────────────────────────────────────────────────────────────────

func TestGeneratePrimaryKey_Query(t *testing.T) {
	t.Parallel()

	t.Run("sorted by default across permutations", func(t *testing.T) {
		urls := []string{
			"http://example.com/search?z=1&a=2&m=3",
			"http://example.com/search?a=2&m=3&z=1",
			"http://example.com/search?m=3&a=2&z=1",
		}
		var first string
		for i, rawURL := range urls {
			key := generatePrimaryKey(makeReq(rawURL), &CacheKey{})
			if i == 0 {
				first = key
			} else if key != first {
				t.Errorf("determinism failure for %s: got %s, want %s", rawURL, key, first)
			}
		}
	})

	tests := []struct {
		name     string
		url      string
		cfg      *CacheKey
		mustHave []string
		mustNot  []string
		exactKey string
	}{
		{
			name:     "included params keep only specified",
			url:      "http://example.com/api/items?sort=desc&page=2&id=100&tracking=xyz",
			cfg:      &CacheKey{IncludedQueryParams: []string{"id", "page"}},
			mustHave: []string{"id=100", "page=2"},
			mustNot:  []string{"sort", "tracking"},
		},
		{
			name:     "excluded params stripped",
			url:      "http://example.com/products?utm_source=ad&id=42&fbclid=12345",
			cfg:      &CacheKey{ExcludedQueryParams: []string{"utm_source", "fbclid"}},
			mustHave: []string{"id=42"},
			mustNot:  []string{"utm_source", "fbclid"},
		},
		{
			name:     "marketing params stripped",
			url:      "http://example.com/shoes?utm_campaign=summer&utm_source=google&gclid=999&size=10&color=blue",
			cfg:      &CacheKey{ExcludeMarketingQueryParams: true},
			mustHave: []string{"size=10", "color=blue"},
			mustNot:  []string{"utm_campaign", "utm_source", "gclid"},
		},
		{
			name:     "marketing params case-insensitive stripping",
			url:      "http://example.com/shoes?UTM_CAMPAIGN=summer&Utm_Source=google&GCLID=999&FBCLID=123&size=10&color=blue",
			cfg:      &CacheKey{ExcludeMarketingQueryParams: true},
			mustHave: []string{"size=10", "color=blue"},
			mustNot:  []string{"UTM_CAMPAIGN", "Utm_Source", "GCLID", "FBCLID"},
		},
		{
			name:     "marketing params wildcard utm prefix stripping",
			url:      "http://example.com/shoes?utm_id=camp123&utm_custom_tag=promo&utm_source_platform=search&size=10",
			cfg:      &CacheKey{ExcludeMarketingQueryParams: true},
			mustHave: []string{"size=10"},
			mustNot:  []string{"utm_id", "utm_custom_tag", "utm_source_platform"},
		},
		{
			name:     "marketing params wildcard utm prefix retained when disabled",
			url:      "http://example.com/shoes?utm_id=camp123&utm_custom_tag=promo&size=10",
			cfg:      &CacheKey{ExcludeMarketingQueryParams: false},
			mustHave: []string{"size=10", "utm_id=camp123", "utm_custom_tag=promo"},
		},
		{
			name:     "exclude all query string",
			url:      "http://example.com/articles?id=99&debug=true",
			cfg:      &CacheKey{ExcludeQuery: true},
			exactKey: "p=/articles:h=example.com:m=GET:",
			mustNot:  []string{":qs="},
		},
		{
			name:     "exclude query overrides both included and excluded params",
			url:      "http://example.com/items?id=1&page=2&sort=asc",
			cfg:      &CacheKey{ExcludeQuery: true, IncludedQueryParams: []string{"id"}, ExcludedQueryParams: []string{"page"}},
			exactKey: "p=/items:h=example.com:m=GET:",
			mustNot:  []string{":qs="},
		},
		{
			name:     "included params overrides excluded params for conflicting key",
			url:      "http://example.com/items?id=1&page=2&sort=asc",
			cfg:      &CacheKey{IncludedQueryParams: []string{"id"}, ExcludedQueryParams: []string{"id", "sort"}},
			mustHave: []string{"id=1"},
			mustNot:  []string{"page", "sort"},
		},
		{
			name:     "included params overrides marketing exclusion for explicit utm param",
			url:      "http://example.com/items?utm_source=newsletter&utm_campaign=summer&category=tech&fbclid=999",
			cfg:      &CacheKey{IncludedQueryParams: []string{"utm_source", "category"}, ExcludeMarketingQueryParams: true},
			mustHave: []string{"category=tech", "utm_source=newsletter"},
			mustNot:  []string{"utm_campaign", "fbclid"},
		},
		{
			name:    "empty query after all params filtered",
			url:     "http://example.com/page?utm_source=google",
			cfg:     &CacheKey{ExcludeMarketingQueryParams: true},
			mustNot: []string{":qs="},
		},
		{
			name:    "no query string has no qs label",
			url:     "http://example.com/page",
			cfg:     &CacheKey{},
			mustNot: []string{":qs="},
		},
		{
			name:     "multiple values for same param are sorted",
			url:      "http://example.com/q?tag=b&tag=a&tag=c",
			cfg:      &CacheKey{},
			mustHave: []string{"qs=tag=a&tag=b&tag=c"},
		},
		{
			name:     "param without value retained",
			url:      "http://example.com/page?flag",
			cfg:      &CacheKey{},
			mustHave: []string{":qs=flag"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key := generatePrimaryKey(makeReq(tc.url), tc.cfg)
			if tc.exactKey != "" && key != tc.exactKey {
				t.Errorf("got %q, want %q", key, tc.exactKey)
			}
			for _, have := range tc.mustHave {
				if !contains(key, have) {
					t.Errorf("expected %q in key: %s", have, key)
				}
			}
			for _, not := range tc.mustNot {
				if contains(key, not) {
					t.Errorf("unexpected %q in key: %s", not, key)
				}
			}
		})
	}

	t.Run("unsorted preserves original order", func(t *testing.T) {
		req := makeReq("http://example.com/search?z=3&a=1&m=2")
		key := generatePrimaryKey(req, &CacheKey{PreserveQueryOrder: true})
		qsStart := indexOf(key, ":qs=")
		if qsStart == -1 {
			t.Fatalf("qs= label missing: %s", key)
		}
		qs := key[qsStart:]
		zPos := indexOf(qs, "z=3")
		aPos := indexOf(qs, "a=1")
		if zPos == -1 || aPos == -1 || zPos > aPos {
			t.Fatalf("expected z=3 before a=1, got qs: %s", qs)
		}
	})

	t.Run("included query param values allowlist", func(t *testing.T) {
		cfg := &CacheKey{
			IncludedQueryParamValues: map[string][]string{
				"format": {"json"},
			},
		}

		// Allowed value included
		kJSON := generatePrimaryKey(makeReq("http://example.com/items?format=json&utm_source=fb"), cfg)
		if !contains(kJSON, "qs=format=json") || contains(kJSON, "utm_source") {
			t.Errorf("expected format=json only, got: %s", kJSON)
		}

		// Disallowed value dropped -> matches no-query key
		kXML := generatePrimaryKey(makeReq("http://example.com/items?format=xml"), cfg)
		kNoQ := generatePrimaryKey(makeReq("http://example.com/items"), cfg)
		if kXML != kNoQ {
			t.Errorf("disallowed format value must drop qs: %q != %q", kXML, kNoQ)
		}

		// Combined with IncludedQueryParams
		cfgCombined := &CacheKey{
			IncludedQueryParams: []string{"page", "sort"},
			IncludedQueryParamValues: map[string][]string{
				"format": {"json"},
			},
		}
		kComb := generatePrimaryKey(makeReq("http://example.com/items?page=2&sort=asc&format=json&extra=ignored"), cfgCombined)
		if !contains(kComb, "format=json&page=2&sort=asc") || contains(kComb, "extra") {
			t.Errorf("expected sorted combined query, got: %s", kComb)
		}

		kCombDisallowed := generatePrimaryKey(makeReq("http://example.com/items?page=2&sort=asc&format=xml&extra=ignored"), cfgCombined)
		if !contains(kCombDisallowed, "page=2&sort=asc") || contains(kCombDisallowed, "format") {
			t.Errorf("disallowed format must be pruned, got: %s", kCombDisallowed)
		}
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Delimiter Injection Protection
// ─────────────────────────────────────────────────────────────────────────────

func TestGeneratePrimaryKey_DelimiterInjection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setupReq func() *http.Request
		cfg      *CacheKey
		check    func(t *testing.T, key string)
	}{
		{
			name: "header value colon and equals percent-encoded",
			setupReq: func() *http.Request {
				r := makeReq("http://example.com/api")
				r.Header.Set("X-Region", "apac:sg (east)")
				return r
			},
			cfg: &CacheKey{IncludedHeaderNames: []string{"X-Region"}},
			check: func(t *testing.T, key string) {
				heIdx := indexOf(key, ":he=x-region~")
				if heIdx == -1 {
					t.Fatalf("he= label missing: %s", key)
				}
				tail := key[heIdx+len(":he=x-region~"):]
				if next := indexOf(tail, ":ck="); next != -1 {
					tail = tail[:next]
				}
				tail = strings.TrimSuffix(tail, ":")
				if contains(tail, ":") || contains(tail, "=") {
					t.Errorf("raw colon or equals in header value must be encoded: %s", tail)
				}
			},
		},
		{
			name: "cookie value colon and equals percent-encoded",
			setupReq: func() *http.Request {
				r := makeReq("http://example.com/api")
				r.AddCookie(&http.Cookie{Name: "session", Value: "tok:en=abc"})
				return r
			},
			cfg: &CacheKey{IncludedCookieNames: []string{"session"}},
			check: func(t *testing.T, key string) {
				ckIdx := indexOf(key, ":ck=session~")
				if ckIdx == -1 {
					t.Fatalf("ck= label missing: %s", key)
				}
				tail := key[ckIdx+len(":ck=session~"):]
				tail = strings.TrimSuffix(tail, ":")
				if contains(tail, ":") || contains(tail, "=") {
					t.Errorf("raw colon or equals in cookie value must be encoded: %s", tail)
				}
			},
		},
		{
			name: "space encoded in header value",
			setupReq: func() *http.Request {
				r := makeReq("http://example.com/api")
				r.Header.Set("X-Region", "apac sg (east)")
				return r
			},
			cfg: &CacheKey{IncludedHeaderNames: []string{"X-Region"}},
			check: func(t *testing.T, key string) {
				if !contains(key, "apac+sg") && !contains(key, "apac%20sg") {
					t.Errorf("space in header value must be encoded, got: %s", key)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key := generatePrimaryKey(tc.setupReq(), tc.cfg)
			tc.check(t, key)
		})
	}
}

func TestGeneratePrimaryKey_ColonsInPathAndQuery(t *testing.T) {
	t.Parallel()

	t.Run("colon in path segment", func(t *testing.T) {
		req := makeReq("http://example.com/api/users/id:123")
		expected := "p=/api/users/id:123:h=example.com:m=GET:"
		if got := generatePrimaryKey(req, &CacheKey{}); got != expected {
			t.Errorf("expected %q, got %q", expected, got)
		}
	})

	t.Run("colon in query value percent-encoded", func(t *testing.T) {
		req := makeReq("http://example.com/api?time=12:30:00")
		expected := "p=/api:h=example.com:qs=time=12%3A30%3A00:m=GET:"
		if got := generatePrimaryKey(req, &CacheKey{}); got != expected {
			t.Errorf("expected %q, got %q", expected, got)
		}
	})

	t.Run("colons in both path and query", func(t *testing.T) {
		req := makeReq("http://example.com/api/users/id:123?time=12:30:00")
		expected := "p=/api/users/id:123:h=example.com:qs=time=12%3A30%3A00:m=GET:"
		if got := generatePrimaryKey(req, &CacheKey{}); got != expected {
			t.Errorf("expected %q, got %q", expected, got)
		}
	})
}

func TestGeneratePrimaryKey_NonASCII_Multilingual(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		rawURL     string
		escapedURL string
		wantKey    string
	}{
		{
			name:       "Chinese path",
			rawURL:     "http://example.com/你好/世界?lang=中文",
			escapedURL: "http://example.com/%E4%BD%A0%E5%A5%BD/%E4%B8%96%E7%95%8C?lang=%E4%B8%AD%E6%96%87",
			wantKey:    "p=/%E4%BD%A0%E5%A5%BD/%E4%B8%96%E7%95%8C:h=example.com:qs=lang=%E4%B8%AD%E6%96%87:m=GET:",
		},
		{
			name:       "Thai path",
			rawURL:     "http://example.com/สวัสดี/โลก",
			escapedURL: "http://example.com/%E0%B8%AA%E0%B8%A7%E0%B8%B1%E0%B8%AA%E0%B8%94%E0%B8%B5/%E0%B9%82%E0%B8%A5%E0%B8%81",
		},
		{
			name:   "Arabic path",
			rawURL: "http://example.com/مرحبا/عالم",
		},
		{
			name:   "Emoji path",
			rawURL: "http://example.com/product/🎉?id=42",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reqRaw := makeReq(tt.rawURL)
			keyRaw := generatePrimaryKey(reqRaw, &CacheKey{})

			// Must not contain raw non-ASCII runes in the primary key!
			for i := 0; i < len(keyRaw); i++ {
				if keyRaw[i] > 127 {
					t.Fatalf("primary key contains non-ASCII byte 0x%x: %s", keyRaw[i], keyRaw)
				}
			}

			if tt.wantKey != "" && keyRaw != tt.wantKey {
				t.Errorf("expected %q, got %q", tt.wantKey, keyRaw)
			}

			// If client sends pre-escaped URL, it must produce the exact same primary key
			uEscaped, err := url.Parse(reqRaw.URL.String())
			if err == nil {
				reqEscaped := &http.Request{
					Method: http.MethodGet,
					Host:   uEscaped.Host,
					URL:    uEscaped,
					Header: http.Header{},
				}
				keyEscaped := generatePrimaryKey(reqEscaped, &CacheKey{})
				if keyRaw != keyEscaped {
					t.Errorf("raw URL and escaped URL produced different keys:\n raw:     %s\n escaped: %s", keyRaw, keyEscaped)
				}
			}
		})
	}
}

func TestWriteEscapedPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		expected string
	}{
		{"", "/"},
		{"/", "/"},
		{"/api/v1/users", "/api/v1/users"},
		{"/math/2*2", "/math/2%2A2"},
		{"/search/item[1]", "/search/item%5B1%5D"},
		{"/hello world/test", "/hello%20world/test"},
		{"/assets/", "/assets/"},
		{"/a/b/c/", "/a/b/c/"},
		{"/deal/50%off", "/deal/50%25off"},
		{"/user@domain/file+name", "/user@domain/file+name"},
		{"/你好/世界", "/%E4%BD%A0%E5%A5%BD/%E4%B8%96%E7%95%8C"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			buf := getBuffer()
			defer putBuffer(buf)
			writeEscapedPath(buf, tt.input)
			got := buf.String()
			if got != tt.expected {
				t.Errorf("writeEscapedPath(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Headers and Cookies Inclusion
// ─────────────────────────────────────────────────────────────────────────────

func TestGeneratePrimaryKey_HeadersAndCookies(t *testing.T) {
	t.Parallel()

	t.Run("headers sorted and lowercased", func(t *testing.T) {
		req := makeReq("http://example.com/store")
		req.Header.Set("X-Region", "US-WEST")
		req.Header.Set("Accept-Language", "en-US")

		key := generatePrimaryKey(req, &CacheKey{IncludedHeaderNames: []string{"X-Region", "Accept-Language"}})
		alIdx := indexOf(key, ":he=accept-language~")
		xrIdx := indexOf(key, ":he=x-region~")
		if alIdx == -1 || xrIdx == -1 || alIdx > xrIdx {
			t.Errorf("accept-language must sort before x-region, got: %s", key)
		}
	})

	t.Run("absent header omitted", func(t *testing.T) {
		key := generatePrimaryKey(makeReq("http://example.com/api"), &CacheKey{IncludedHeaderNames: []string{"X-Region"}})
		if contains(key, ":he=") {
			t.Errorf("absent header must be omitted, got: %s", key)
		}
	})

	t.Run("cookies sorted and encoded", func(t *testing.T) {
		req := makeReq("http://example.com/store")
		req.AddCookie(&http.Cookie{Name: "theme", Value: "dark"})
		req.AddCookie(&http.Cookie{Name: "currency", Value: "USD"})

		key := generatePrimaryKey(req, &CacheKey{IncludedCookieNames: []string{"theme", "currency"}})
		curIdx := indexOf(key, ":ck=currency~")
		thIdx := indexOf(key, ":ck=theme~")
		if curIdx == -1 || thIdx == -1 || curIdx > thIdx {
			t.Errorf("currency must sort before theme, got: %s", key)
		}
	})

	t.Run("absent cookie omitted", func(t *testing.T) {
		key := generatePrimaryKey(makeReq("http://example.com/store"), &CacheKey{IncludedCookieNames: []string{"missing_cookie"}})
		if contains(key, ":ck=") {
			t.Errorf("absent cookie must be omitted, got: %s", key)
		}
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Full Format Golden Tests
// ─────────────────────────────────────────────────────────────────────────────

func TestGeneratePrimaryKey_Golden(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setup    func() *http.Request
		cfg      CacheKey
		expected string
	}{
		{
			name:     "default zero-value config with sorted query",
			setup:    func() *http.Request { return makeReq("http://Example.COM/api/items/?sort=desc&page=2&id=100") },
			cfg:      CacheKey{},
			expected: "p=/api/items/:h=example.com:qs=id=100&page=2&sort=desc:m=GET:",
		},
		{
			name: "TLS with protocol included",
			setup: func() *http.Request {
				u := mustParse("https://secure.example.com/user/profile")
				return &http.Request{Method: "GET", Host: "secure.example.com", URL: u, Header: http.Header{}, TLS: &tls.ConnectionState{}}
			},
			cfg:      CacheKey{IncludeProtocol: true},
			expected: "p=/user/profile:h=secure.example.com:s=https:m=GET:",
		},
		{
			name:     "ExcludeHost",
			setup:    func() *http.Request { return makeReq("http://cdn.example.com/assets/style.css") },
			cfg:      CacheKey{ExcludeHost: true},
			expected: "p=/assets/style.css:m=GET:",
		},
		{
			name:     "ExcludeQuery",
			setup:    func() *http.Request { return makeReq("http://example.com/articles?id=99&debug=true") },
			cfg:      CacheKey{ExcludeQuery: true},
			expected: "p=/articles:h=example.com:m=GET:",
		},
		{
			name: "headers and cookies with special chars",
			setup: func() *http.Request {
				u := mustParse("http://example.com/store")
				req := &http.Request{Method: "GET", Host: "example.com", URL: u, Header: http.Header{}}
				req.Header.Set("X-Region", "US-WEST")
				req.AddCookie(&http.Cookie{Name: "currency", Value: "USD"})
				return req
			},
			cfg:      CacheKey{IncludedHeaderNames: []string{"X-Region"}, IncludedCookieNames: []string{"currency"}},
			expected: "p=/store:h=example.com:m=GET:he=x-region~US-WEST:ck=currency~USD:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := generatePrimaryKey(tt.setup(), &tt.cfg)
			if key != tt.expected {
				t.Errorf("\nexpected: %s\n     got: %s", tt.expected, key)
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Variant Key Generation
// ─────────────────────────────────────────────────────────────────────────────

func TestGenerateVariantKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		req      *http.Request
		vary     []string
		expected string
	}{
		{
			name: "basic multi-vary",
			req: &http.Request{
				Header: http.Header{
					"Accept-Encoding": []string{"gzip, deflate, br"},
					"Accept-Language": []string{"en-US,en;q=0.9"},
				},
			},
			vary:     []string{"Accept-Language", "Accept-Encoding"},
			expected: "accept-encoding=br,deflate,gzip|accept-language=en-US,en;q=0.9",
		},
		{
			name: "token order independence",
			req: &http.Request{
				Header: http.Header{
					"Accept-Encoding": []string{"br, gzip, deflate"},
					"Accept-Language": []string{"en-US,en;q=0.9"},
				},
			},
			vary:     []string{"Accept-Language", "Accept-Encoding"},
			expected: "accept-encoding=br,deflate,gzip|accept-language=en-US,en;q=0.9",
		},
		{
			name: "custom header preserves verbatim ordering",
			req: &http.Request{
				Header: http.Header{"X-App-Group": []string{"beta,alpha"}},
			},
			vary:     []string{"X-App-Group"},
			expected: "x-app-group=beta,alpha",
		},
		{
			name:     "nil vary produces empty string",
			req:      &http.Request{Header: http.Header{}},
			vary:     nil,
			expected: "",
		},
		{
			name:     "empty vary produces empty string",
			req:      &http.Request{Header: http.Header{}},
			vary:     []string{},
			expected: "",
		},
		{
			name:     "missing header produces empty value segment",
			req:      &http.Request{Header: http.Header{}},
			vary:     []string{"Accept-Encoding"},
			expected: "accept-encoding=",
		},
		{
			name: "deterministic regardless of vary slice order",
			req: &http.Request{Header: http.Header{
				"Accept-Language": []string{"en-US"},
				"Accept-Encoding": []string{"gzip"},
			}},
			vary:     []string{"Accept-Encoding", "Accept-Language"},
			expected: "accept-encoding=gzip|accept-language=en-US",
		},
		{
			name: "preserves casing for non-normalized custom headers",
			req: &http.Request{
				Header: http.Header{"X-Custom-Vary": []string{"CaseSensitiveValue123"}},
			},
			vary:     []string{"X-Custom-Vary"},
			expected: "x-custom-vary=CaseSensitiveValue123",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := generateVariantKey(tc.req, tc.vary)
			if got != tc.expected {
				t.Errorf("got %q, want %q", got, tc.expected)
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmarks
// ─────────────────────────────────────────────────────────────────────────────

func BenchmarkGeneratePrimaryKey(b *testing.B) {
	u, _ := url.Parse("https://example.com/api/products?id=12345&category=electronics")
	req := &http.Request{
		Method: http.MethodGet,
		Host:   "example.com",
		URL:    u,
		TLS:    &tls.ConnectionState{},
		Header: http.Header{},
	}
	cfg := &CacheKey{}

	for b.Loop() {
		_ = generatePrimaryKey(req, cfg)
	}
}

func BenchmarkGeneratePrimaryKey_WithQuerySort(b *testing.B) {
	u, _ := url.Parse("https://example.com/search?z=last&a=first&m=middle&b=second&y=penult")
	req := &http.Request{
		Method: http.MethodGet,
		Host:   "example.com",
		URL:    u,
		Header: http.Header{},
	}
	cfg := &CacheKey{}

	for b.Loop() {
		_ = generatePrimaryKey(req, cfg)
	}
}

func BenchmarkGeneratePrimaryKey_WithHeadersAndCookies(b *testing.B) {
	u, _ := url.Parse("https://example.com/store?id=42&page=1")
	req := &http.Request{
		Method: http.MethodGet,
		Host:   "example.com",
		URL:    u,
		Header: http.Header{
			"X-Region":        []string{"us-west"},
			"Accept-Language": []string{"en-US"},
		},
	}
	req.AddCookie(&http.Cookie{Name: "theme", Value: "dark"})
	req.AddCookie(&http.Cookie{Name: "currency", Value: "USD"})

	cfg := &CacheKey{
		IncludedHeaderNames: []string{"X-Region", "Accept-Language"},
		IncludedCookieNames: []string{"theme", "currency"},
	}

	for b.Loop() {
		_ = generatePrimaryKey(req, cfg)
	}
}

func BenchmarkGeneratePrimaryKey_AllOptions(b *testing.B) {
	u, _ := url.Parse("https://example.com/api/products?id=12345&category=electronics&utm_source=google&page=1")
	req := &http.Request{
		Method: http.MethodGet,
		Host:   "example.com",
		URL:    u,
		TLS:    &tls.ConnectionState{},
		Header: http.Header{
			"X-Region": []string{"us-west"},
		},
	}
	req.AddCookie(&http.Cookie{Name: "ab_group", Value: "control"})

	cfg := &CacheKey{
		IncludeProtocol:             true,
		ExcludeMarketingQueryParams: true,
		IncludedHeaderNames:         []string{"X-Region"},
		IncludedCookieNames:         []string{"ab_group"},
	}

	for b.Loop() {
		_ = generatePrimaryKey(req, cfg)
	}
}

func BenchmarkGeneratePrimaryKey_CaseInsensitive(b *testing.B) {
	req := makeReq("http://example.com/Products/Shoes/Running?id=123")
	cfg := &CacheKey{CaseInsensitivePath: true}

	for b.Loop() {
		_ = generatePrimaryKey(req, cfg)
	}
}

func BenchmarkGeneratePrimaryKey_QueryParamValues(b *testing.B) {
	req := makeReq("http://example.com/items?format=json&page=2&sort=asc&utm_source=fb")
	cfg := &CacheKey{
		IncludedQueryParams: []string{"page", "sort"},
		IncludedQueryParamValues: map[string][]string{
			"format": {"json"},
		},
	}

	for b.Loop() {
		_ = generatePrimaryKey(req, cfg)
	}
}

func BenchmarkGenerateVariantKey(b *testing.B) {
	req := &http.Request{
		Header: http.Header{
			"Accept-Encoding": []string{"gzip, deflate"},
			"Accept-Language": []string{"en-US"},
		},
	}
	vary := []string{"Accept-Encoding", "Accept-Language"}

	for b.Loop() {
		_ = generateVariantKey(req, vary)
	}
}
