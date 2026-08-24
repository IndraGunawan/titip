package titip

import (
	"crypto/tls"
	"net/http"
	"net/url"
	"testing"
)

func TestGeneratePrimaryKey_QueryParamModes(t *testing.T) {
	tests := []struct {
		name     string
		rawURL   string
		cfg      *KeyConfig
		expected string
	}{
		{
			name:     "QueryParamsAll sorts parameters deterministically",
			rawURL:   "http://example.com/api/items?sort=desc&page=2&id=100",
			cfg:      DefaultKeyConfig(),
			expected: "http://example.com/api/items?id=100&page=2&sort=desc",
		},
		{
			name:   "QueryParamsWhitelist retains only specified parameters",
			rawURL: "http://example.com/api/items?sort=desc&page=2&id=100&tracking=xyz",
			cfg: &KeyConfig{
				IncludeProtocol: true,
				IncludeHost:     true,
				IncludePath:     true,
				QueryMode:       QueryParamsWhitelist,
				QueryWhitelist:  []string{"id", "page"},
			},
			expected: "http://example.com/api/items?id=100&page=2",
		},
		{
			name:   "QueryParamsBlacklist removes specified parameters",
			rawURL: "http://example.com/products?utm_source=ad&id=42&fbclid=12345",
			cfg: &KeyConfig{
				IncludeProtocol: true,
				IncludeHost:     true,
				IncludePath:     true,
				QueryMode:       QueryParamsBlacklist,
				QueryBlacklist:  []string{"utm_source", "fbclid"},
			},
			expected: "http://example.com/products?id=42",
		},
		{
			name:   "WithIgnoredMarketingParams ignores marketing tracking params",
			rawURL: "http://example.com/shoes?utm_campaign=summer&utm_source=google&gclid=999&size=10&color=blue",
			cfg:    DefaultKeyConfig().WithIgnoredMarketingParams(),
			expected: "http://example.com/shoes?color=blue&size=10",
		},
		{
			name:   "QueryParamsNone strips all query parameters",
			rawURL: "http://example.com/articles?id=99&debug=true",
			cfg: &KeyConfig{
				IncludeProtocol: true,
				IncludeHost:     true,
				IncludePath:     true,
				QueryMode:       QueryParamsNone,
			},
			expected: "http://example.com/articles",
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
			key := GeneratePrimaryKey(req, tt.cfg)
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
	cfg := DefaultKeyConfig()

	for i, u := range urls {
		parsed, _ := url.Parse(u)
		req := &http.Request{
			Host: parsed.Host,
			URL:  parsed,
		}
		k := GeneratePrimaryKey(req, cfg)
		if i == 0 {
			firstKey = k
		} else if k != firstKey {
			t.Fatalf("determinism failure: url %s produced %s, expected %s", u, k, firstKey)
		}
	}
}

func TestGeneratePrimaryKey_ProtocolAndHost(t *testing.T) {
	// TLS request
	u, _ := url.Parse("https://secure.example.com/user/profile")
	req := &http.Request{
		Host: "secure.example.com",
		URL:  u,
		TLS:  &tls.ConnectionState{},
	}
	key := GeneratePrimaryKey(req, DefaultKeyConfig())
	if key != "https://secure.example.com/user/profile" {
		t.Fatalf("expected https protocol in key, got %s", key)
	}

	// Forwarded proto header
	u2, _ := url.Parse("http://api.example.com/v1/data")
	req2 := &http.Request{
		Host:   "api.example.com",
		URL:    u2,
		Header: http.Header{"X-Forwarded-Proto": []string{"https"}},
	}
	key2 := GeneratePrimaryKey(req2, DefaultKeyConfig())
	if key2 != "https://api.example.com/v1/data" {
		t.Fatalf("expected forwarded https in key, got %s", key2)
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
		IncludeProtocol: true,
		IncludeHost:     true,
		IncludePath:     true,
		IncludeHeaders:  []string{"X-Region", "Accept-Language"},
		IncludeCookies:  []string{"theme", "currency"},
	}

	key := GeneratePrimaryKey(req, cfg)
	expected := "http://example.com/store|h:accept-language=en-US|h:x-region=US-WEST|c:currency=USD|c:theme=dark"
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
	variantKey := GenerateVariantKey(req, vary)
	expected := "accept-encoding=gzip, deflate, br|accept-language=en-us,en;q=0.9"
	if variantKey != expected {
		t.Fatalf("expected variant key %q, got %q", expected, variantKey)
	}

	// Empty vary headers
	if GenerateVariantKey(req, nil) != "" {
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
	cfg := DefaultKeyConfig()

	for b.Loop() {
		_ = GeneratePrimaryKey(req, cfg)
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
		_ = GenerateVariantKey(req, vary)
	}
}
