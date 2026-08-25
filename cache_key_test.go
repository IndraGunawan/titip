package titip

import (
	"crypto/tls"
	"net/http"
	"net/url"
	"testing"
)

func TestGeneratePrimaryKey_ZeroValueDefaults(t *testing.T) {
	u, _ := url.Parse("http://Example.COM/api/items/?sort=desc&page=2&id=100")
	req := &http.Request{
		Host: u.Host,
		URL:  u,
	}

	// Zero-value struct: host is included, proto omitted, path normalized (trailing slash stripped), query params sorted
	key := generatePrimaryKey(req, &KeyConfig{})
	expected := "example.com/api/items?id=100&page=2&sort=desc"
	if key != expected {
		t.Fatalf("expected default key %q, got %q", expected, key)
	}

	// nil config should behave identically to &KeyConfig{}
	keyNil := generatePrimaryKey(req, nil)
	if keyNil != expected {
		t.Fatalf("expected nil config key %q, got %q", expected, keyNil)
	}
}

func TestGeneratePrimaryKey_ProtocolAndHost(t *testing.T) {
	// TLS request with IncludeProtocol: true
	u, _ := url.Parse("https://secure.example.com/user/profile")
	req := &http.Request{
		Host: "secure.example.com",
		URL:  u,
		TLS:  &tls.ConnectionState{},
	}
	key := generatePrimaryKey(req, &KeyConfig{IncludeProtocol: true})
	if key != "https://secure.example.com/user/profile" {
		t.Fatalf("expected https protocol in key, got %s", key)
	}

	// Forwarded proto header with IncludeProtocol: true
	u2, _ := url.Parse("http://api.example.com/v1/data")
	req2 := &http.Request{
		Host:   "api.example.com",
		URL:    u2,
		Header: http.Header{"X-Forwarded-Proto": []string{"https"}},
	}
	key2 := generatePrimaryKey(req2, &KeyConfig{IncludeProtocol: true})
	if key2 != "https://api.example.com/v1/data" {
		t.Fatalf("expected forwarded https in key, got %s", key2)
	}

	// ExcludeHost: true
	req3 := &http.Request{
		Host: "cdn.example.com",
		URL:  &url.URL{Path: "/assets/style.css"},
	}
	key3 := generatePrimaryKey(req3, &KeyConfig{ExcludeHost: true})
	if key3 != "/assets/style.css" {
		t.Fatalf("expected host excluded key, got %s", key3)
	}
}

func TestGeneratePrimaryKey_TrailingSlash(t *testing.T) {
	u, _ := url.Parse("http://example.com/docs/")
	req := &http.Request{
		Host: "example.com",
		URL:  u,
	}

	// KeepTrailingSlash: false (normalized)
	key1 := generatePrimaryKey(req, &KeyConfig{KeepTrailingSlash: false})
	if key1 != "example.com/docs" {
		t.Fatalf("expected normalized trailing slash %q, got %q", "example.com/docs", key1)
	}

	// KeepTrailingSlash: true (preserved)
	key2 := generatePrimaryKey(req, &KeyConfig{KeepTrailingSlash: true})
	if key2 != "example.com/docs/" {
		t.Fatalf("expected preserved trailing slash %q, got %q", "example.com/docs/", key2)
	}

	// Root slash is preserved regardless
	rootReq := &http.Request{
		Host: "example.com",
		URL:  &url.URL{Path: "/"},
	}
	keyRoot := generatePrimaryKey(rootReq, &KeyConfig{KeepTrailingSlash: false})
	if keyRoot != "example.com/" {
		t.Fatalf("expected root path %q, got %q", "example.com/", keyRoot)
	}
}

func TestGeneratePrimaryKey_QueryParams(t *testing.T) {
	tests := []struct {
		name     string
		rawURL   string
		cfg      KeyConfig
		expected string
	}{
		{
			name:     "IncludedQueryParams whitelist retains only specified parameters",
			rawURL:   "http://example.com/api/items?sort=desc&page=2&id=100&tracking=xyz",
			cfg:      KeyConfig{IncludedQueryParams: []string{"id", "page"}},
			expected: "example.com/api/items?id=100&page=2",
		},
		{
			name:     "ExcludedQueryParams blacklist removes specified parameters",
			rawURL:   "http://example.com/products?utm_source=ad&id=42&fbclid=12345",
			cfg:      KeyConfig{ExcludedQueryParams: []string{"utm_source", "fbclid"}},
			expected: "example.com/products?id=42",
		},
		{
			name:     "ExcludeMarketingParams removes advertising tracking parameters",
			rawURL:   "http://example.com/shoes?utm_campaign=summer&utm_source=google&gclid=999&size=10&color=blue",
			cfg:      KeyConfig{ExcludeMarketingParams: true},
			expected: "example.com/shoes?color=blue&size=10",
		},
		{
			name:     "ExcludeQueryString strips all query parameters",
			rawURL:   "http://example.com/articles?id=99&debug=true",
			cfg:      KeyConfig{ExcludeQueryString: true},
			expected: "example.com/articles",
		},
		{
			name:     "DisableQueryStringSort preserves original request parameter order",
			rawURL:   "http://example.com/search?z=3&a=1&m=2",
			cfg:      KeyConfig{DisableQueryStringSort: true},
			expected: "example.com/search?z=3&a=1&m=2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsedURL, err := url.Parse(tt.rawURL)
			if err != nil {
				t.Fatalf("url parse error: %v", err)
			}
			req := &http.Request{
				Host: parsedURL.Host,
				URL:  parsedURL,
			}
			key := generatePrimaryKey(req, &tt.cfg)
			if key != tt.expected {
				t.Errorf("expected key %q, got %q", tt.expected, key)
			}
		})
	}
}

func TestGeneratePrimaryKey_Determinism(t *testing.T) {
	urls := []string{
		"http://example.com/search?z=1&a=2&m=3",
		"http://example.com/search?a=2&m=3&z=1",
		"http://example.com/search?m=3&a=2&z=1",
	}

	var firstKey string
	cfg := &KeyConfig{}

	for i, u := range urls {
		parsed, _ := url.Parse(u)
		req := &http.Request{
			Host: parsed.Host,
			URL:  parsed,
		}
		k := generatePrimaryKey(req, cfg)
		if i == 0 {
			firstKey = k
		} else if k != firstKey {
			t.Fatalf("determinism failure: url %s produced %s, expected %s", u, k, firstKey)
		}
	}
}

func TestGeneratePrimaryKey_HeadersAndCookies(t *testing.T) {
	u, _ := url.Parse("http://example.com/store")
	req := &http.Request{
		Host:   "example.com",
		URL:    u,
		Header: http.Header{},
	}
	req.Header.Set("X-Region", "US-WEST")
	req.Header.Set("Accept-Language", "en-US")
	req.AddCookie(&http.Cookie{Name: "theme", Value: "dark"})
	req.AddCookie(&http.Cookie{Name: "currency", Value: "USD"})

	cfg := &KeyConfig{
		IncludedHeaderNames: []string{"X-Region", "Accept-Language"},
		IncludedCookieNames: []string{"theme", "currency"},
	}

	key := generatePrimaryKey(req, cfg)
	expected := "example.com/store|h:accept-language=en-US|h:x-region=US-WEST|c:currency=USD|c:theme=dark"
	if key != expected {
		t.Fatalf("expected %q, got %q", expected, key)
	}
}

func TestGenerateVariantKey(t *testing.T) {
	req := &http.Request{
		Header: http.Header{
			"Accept-Encoding": []string{"gzip, deflate, br"},
			"Accept-Language": []string{"en-US,en;q=0.9"},
		},
	}

	vary := []string{"Accept-Language", "Accept-Encoding"}
	variantKey := generateVariantKey(req, vary)
	expected := "accept-encoding=gzip, deflate, br|accept-language=en-us,en;q=0.9"
	if variantKey != expected {
		t.Fatalf("expected variant key %q, got %q", expected, variantKey)
	}

	// Empty vary headers
	if generateVariantKey(req, nil) != "" {
		t.Fatal("expected empty string for nil vary headers")
	}
}

func BenchmarkGeneratePrimaryKey(b *testing.B) {
	u, _ := url.Parse("https://example.com/api/products?id=12345&category=electronics")
	req := &http.Request{
		Host: "example.com",
		URL:  u,
		TLS:  &tls.ConnectionState{},
	}
	cfg := &KeyConfig{}

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
