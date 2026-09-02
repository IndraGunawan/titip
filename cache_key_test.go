package titip

import (
	"crypto/tls"
	"net/http"
	"net/url"
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

// ─────────────────────────────────────────────────────────────────────────────
// AC-1: Key format — labeled component structure
// ─────────────────────────────────────────────────────────────────────────────

func TestGeneratePrimaryKey_LabeledFormat_Basic(t *testing.T) {
	req := makeReq("http://example.com/api/items")
	key := generatePrimaryKey(req, &KeyConfig{})
	expected := "p=/api/items:h=example.com:m=GET"
	if key != expected {
		t.Fatalf("expected %q\n     got %q", expected, key)
	}
}

func TestGeneratePrimaryKey_LabeledFormat_NilConfig(t *testing.T) {
	req := makeReq("http://example.com/api/items")
	key := generatePrimaryKey(req, nil)
	expected := "p=/api/items:h=example.com:m=GET"
	if key != expected {
		t.Fatalf("nil config: expected %q\n     got %q", expected, key)
	}
}

func TestGeneratePrimaryKey_LabeledFormat_ComponentOrder(t *testing.T) {
	// Verify the fixed ordering: meta:p → h → m → s → qs → he → ck
	u := mustParse("https://secure.example.com/store?color=blue&size=m")
	req := &http.Request{
		Method: http.MethodGet,
		Host:   "secure.example.com",
		URL:    u,
		Header: http.Header{"X-Region": []string{"us-west"}},
		TLS:    &tls.ConnectionState{},
	}
	req.AddCookie(&http.Cookie{Name: "theme", Value: "dark"})

	cfg := &KeyConfig{
		IncludeProtocol:     true,
		IncludedHeaderNames: []string{"X-Region"},
		IncludedCookieNames: []string{"theme"},
	}
	key := generatePrimaryKey(req, cfg)

	// Must start with p=
	if len(key) < 2 || key[:2] != "p=" {
		t.Fatalf("key must start with 'p=', got: %s", key)
	}
	// All labeled segments must appear in correct order
	positions := []struct {
		label string
	}{
		{"p="},
		{":h="},
		{":m="},
		{":s="},
		{":qs="},
		{":he="},
		{":ck="},
	}
	prev := 0
	for _, pos := range positions {
		idx := indexOf(key, pos.label)
		if idx == -1 {
			t.Fatalf("missing label %q in key: %s", pos.label, key)
		}
		if idx < prev {
			t.Fatalf("label %q out of order in key: %s", pos.label, key)
		}
		prev = idx
	}
}

// indexOf returns the byte-offset of substr in s, or -1.
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

// ─────────────────────────────────────────────────────────────────────────────
// AC-1: Method — always present, HEAD normalises to GET
// ─────────────────────────────────────────────────────────────────────────────

func TestGeneratePrimaryKey_Method_GetAlwaysPresent(t *testing.T) {
	req := makeReq("http://example.com/page")
	req.Method = http.MethodGet
	key := generatePrimaryKey(req, &KeyConfig{})
	if !contains(key, ":m=GET") {
		t.Fatalf("expected :m=GET in key, got: %s", key)
	}
}

func TestGeneratePrimaryKey_Method_HeadNormalisesToGet(t *testing.T) {
	reqGet := makeReq("http://example.com/page")
	reqGet.Method = http.MethodGet
	keyGet := generatePrimaryKey(reqGet, &KeyConfig{})

	reqHead := makeReq("http://example.com/page")
	reqHead.Method = http.MethodHead
	keyHead := generatePrimaryKey(reqHead, &KeyConfig{})

	if keyGet != keyHead {
		t.Fatalf("HEAD should produce same key as GET:\n GET:  %s\n HEAD: %s", keyGet, keyHead)
	}
}

func TestGeneratePrimaryKey_Method_EmptyNormalisesToGet(t *testing.T) {
	req := makeReq("http://example.com/page")
	req.Method = ""
	key := generatePrimaryKey(req, &KeyConfig{})
	if !contains(key, ":m=GET") {
		t.Fatalf("empty method should normalise to GET, got: %s", key)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// AC-1: Path normalisation
// ─────────────────────────────────────────────────────────────────────────────

func TestGeneratePrimaryKey_Path_TrailingSlashPreserved(t *testing.T) {
	req := makeReq("http://example.com/docs/")
	key := generatePrimaryKey(req, &KeyConfig{})
	if !contains(key, "p=/docs/:") {
		t.Fatalf("trailing slash should be preserved, got: %s", key)
	}
}

func TestGeneratePrimaryKey_Path_RootSlashPreserved(t *testing.T) {
	req := makeReq("http://example.com/")
	key := generatePrimaryKey(req, &KeyConfig{})
	// Root "/" must never be stripped.
	if !contains(key, "p=/:") {
		t.Fatalf("root slash should always be preserved, got: %s", key)
	}
}

func TestGeneratePrimaryKey_Path_DotSegmentsResolved(t *testing.T) {
	u := &url.URL{Path: "/a/b/../c/./d"}
	req := &http.Request{Method: http.MethodGet, Host: "example.com", URL: u, Header: http.Header{}}
	key := generatePrimaryKey(req, &KeyConfig{})
	if !contains(key, "p=/a/c/d:") {
		t.Fatalf("dot segments should be resolved, got: %s", key)
	}
}

func TestGeneratePrimaryKey_Path_NoURL(t *testing.T) {
	req := &http.Request{Method: http.MethodGet, Host: "example.com", Header: http.Header{}}
	key := generatePrimaryKey(req, &KeyConfig{})
	if !contains(key, "p=/:") {
		t.Fatalf("nil URL should use '/' as path, got: %s", key)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// AC-1: Host normalisation
// ─────────────────────────────────────────────────────────────────────────────

func TestGeneratePrimaryKey_Host_Lowercased(t *testing.T) {
	req := makeReq("http://Example.COM/api")
	key := generatePrimaryKey(req, &KeyConfig{})
	if !contains(key, ":h=example.com:") {
		t.Fatalf("host should be lowercased, got: %s", key)
	}
}

func TestGeneratePrimaryKey_Host_DefaultHTTPPortStripped(t *testing.T) {
	req := makeReq("http://example.com:80/api")
	key := generatePrimaryKey(req, &KeyConfig{})
	if contains(key, ":80") {
		t.Fatalf("default HTTP port :80 should be stripped, got: %s", key)
	}
	if !contains(key, ":h=example.com:") {
		t.Fatalf("host should be example.com, got: %s", key)
	}
}

func TestGeneratePrimaryKey_Host_DefaultHTTPSPortStripped(t *testing.T) {
	u := mustParse("https://secure.example.com:443/api")
	req := &http.Request{
		Method: http.MethodGet,
		Host:   "secure.example.com:443",
		URL:    u,
		TLS:    &tls.ConnectionState{},
		Header: http.Header{},
	}
	key := generatePrimaryKey(req, &KeyConfig{})
	if contains(key, ":443") {
		t.Fatalf("default HTTPS port :443 should be stripped, got: %s", key)
	}
}

func TestGeneratePrimaryKey_Host_NonDefaultPortPreserved(t *testing.T) {
	req := makeReq("http://example.com:8080/api")
	key := generatePrimaryKey(req, &KeyConfig{})
	if !contains(key, ":h=example.com:8080:") {
		t.Fatalf("non-default port :8080 should be preserved, got: %s", key)
	}
}

func TestGeneratePrimaryKey_Host_ExcludeHost(t *testing.T) {
	req := makeReq("http://cdn.example.com/assets/style.css")
	key := generatePrimaryKey(req, &KeyConfig{ExcludeHost: true})
	if contains(key, ":h=") {
		t.Fatalf("host should be excluded, got: %s", key)
	}
	expected := "p=/assets/style.css:m=GET"
	if key != expected {
		t.Fatalf("expected %q\n     got %q", expected, key)
	}
}

func TestGeneratePrimaryKey_Host_FallbackToURLHost(t *testing.T) {
	u := mustParse("http://fallback.example.com/path")
	req := &http.Request{
		Method: http.MethodGet,
		Host:   "", // empty — should fall back to URL.Host
		URL:    u,
		Header: http.Header{},
	}
	key := generatePrimaryKey(req, &KeyConfig{})
	if !contains(key, ":h=fallback.example.com:") {
		t.Fatalf("should fall back to URL.Host, got: %s", key)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Scheme (IncludeProtocol)
// ─────────────────────────────────────────────────────────────────────────────

func TestGeneratePrimaryKey_Scheme_TLS(t *testing.T) {
	u := mustParse("https://secure.example.com/user/profile")
	req := &http.Request{
		Method: http.MethodGet,
		Host:   "secure.example.com",
		URL:    u,
		TLS:    &tls.ConnectionState{},
		Header: http.Header{},
	}
	key := generatePrimaryKey(req, &KeyConfig{IncludeProtocol: true})
	if !contains(key, ":s=https") {
		t.Fatalf("TLS request should have s=https, got: %s", key)
	}
}

func TestGeneratePrimaryKey_Scheme_ForwardedProto(t *testing.T) {
	req := makeReq("http://api.example.com/v1/data")
	req.Header.Set("X-Forwarded-Proto", "https")
	key := generatePrimaryKey(req, &KeyConfig{IncludeProtocol: true})
	if !contains(key, ":s=https") {
		t.Fatalf("X-Forwarded-Proto: https should yield s=https, got: %s", key)
	}
}

func TestGeneratePrimaryKey_Scheme_PlainHTTP(t *testing.T) {
	req := makeReq("http://api.example.com/v1/data")
	key := generatePrimaryKey(req, &KeyConfig{IncludeProtocol: true})
	if !contains(key, ":s=http") {
		t.Fatalf("plain HTTP should have s=http, got: %s", key)
	}
}

func TestGeneratePrimaryKey_Scheme_OmittedByDefault(t *testing.T) {
	req := makeReq("http://example.com/page")
	key := generatePrimaryKey(req, &KeyConfig{})
	if contains(key, ":s=") {
		t.Fatalf("scheme should be omitted by default, got: %s", key)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Query string
// ─────────────────────────────────────────────────────────────────────────────

func TestGeneratePrimaryKey_Query_SortedByDefault(t *testing.T) {
	urls := []string{
		"http://example.com/search?z=1&a=2&m=3",
		"http://example.com/search?a=2&m=3&z=1",
		"http://example.com/search?m=3&a=2&z=1",
	}
	cfg := &KeyConfig{}
	var first string
	for i, rawURL := range urls {
		key := generatePrimaryKey(makeReq(rawURL), cfg)
		if i == 0 {
			first = key
		} else if key != first {
			t.Fatalf("determinism failure: url %s\n produced: %s\n expected: %s", rawURL, key, first)
		}
	}
}

func TestGeneratePrimaryKey_Query_IncludedWhitelist(t *testing.T) {
	req := makeReq("http://example.com/api/items?sort=desc&page=2&id=100&tracking=xyz")
	key := generatePrimaryKey(req, &KeyConfig{IncludedQueryParams: []string{"id", "page"}})
	if !contains(key, "id=100") || !contains(key, "page=2") {
		t.Fatalf("expected id=100 and page=2 in key, got: %s", key)
	}
	// tracking and sort must not appear
	if contains(key, "sort") || contains(key, "tracking") {
		t.Fatalf("blacklisted params must not appear in key: %s", key)
	}
}

func TestGeneratePrimaryKey_Query_ExcludedBlacklist(t *testing.T) {
	req := makeReq("http://example.com/products?utm_source=ad&id=42&fbclid=12345")
	key := generatePrimaryKey(req, &KeyConfig{ExcludedQueryParams: []string{"utm_source", "fbclid"}})
	if contains(key, "utm_source") || contains(key, "fbclid") {
		t.Fatalf("blacklisted params must be excluded, got: %s", key)
	}
}

func TestGeneratePrimaryKey_Query_MarketingParamsStripped(t *testing.T) {
	req := makeReq("http://example.com/shoes?utm_campaign=summer&utm_source=google&gclid=999&size=10&color=blue")
	key := generatePrimaryKey(req, &KeyConfig{ExcludeMarketingParams: true})
	for _, mq := range []string{"utm_campaign", "utm_source", "gclid"} {
		if contains(key, mq) {
			t.Fatalf("marketing param %q must be stripped, got: %s", mq, key)
		}
	}
	if !contains(key, "size") || !contains(key, "color") {
		t.Fatalf("non-marketing params must be preserved, got: %s", key)
	}
}

func TestGeneratePrimaryKey_Query_ExcludeAll(t *testing.T) {
	req := makeReq("http://example.com/articles?id=99&debug=true")
	key := generatePrimaryKey(req, &KeyConfig{ExcludeQueryString: true})
	if contains(key, ":qs=") {
		t.Fatalf("query should be excluded, got: %s", key)
	}
	expected := "p=/articles:h=example.com:m=GET"
	if key != expected {
		t.Fatalf("expected %q\n     got %q", expected, key)
	}
}

func TestGeneratePrimaryKey_Query_UnsortedPreservesOrder(t *testing.T) {
	req := makeReq("http://example.com/search?z=3&a=1&m=2")
	key := generatePrimaryKey(req, &KeyConfig{DisableQueryStringSort: true})
	// z must appear before a in the qs= section
	qsStart := indexOf(key, ":qs=")
	if qsStart == -1 {
		t.Fatalf("qs= label missing, got: %s", key)
	}
	qs := key[qsStart:]
	zPos := indexOf(qs, "z=3")
	aPos := indexOf(qs, "a=1")
	if zPos == -1 || aPos == -1 {
		t.Fatalf("expected z and a params, got: %s", key)
	}
	if zPos > aPos {
		t.Fatalf("DisableQueryStringSort: z must appear before a, got: %s", key)
	}
}

func TestGeneratePrimaryKey_Query_EmptyAfterFilter(t *testing.T) {
	// Only marketing params — all filtered → no qs= label
	req := makeReq("http://example.com/page?utm_source=google")
	key := generatePrimaryKey(req, &KeyConfig{ExcludeMarketingParams: true})
	if contains(key, ":qs=") {
		t.Fatalf("qs= should be absent when all params filtered, got: %s", key)
	}
}

func TestGeneratePrimaryKey_Query_NoQueryString(t *testing.T) {
	req := makeReq("http://example.com/page")
	key := generatePrimaryKey(req, &KeyConfig{})
	if contains(key, ":qs=") {
		t.Fatalf("qs= should be absent with no query string, got: %s", key)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// AC-1: Delimiter injection protection — header/cookie value encoding
// ─────────────────────────────────────────────────────────────────────────────

func TestGeneratePrimaryKey_DelimiterInjection_HeaderColonEquals(t *testing.T) {
	// Header value "apac:sg (east)" must not insert raw : or = into the key.
	req := makeReq("http://example.com/api")
	req.Header.Set("X-Region", "apac:sg (east)")
	cfg := &KeyConfig{IncludedHeaderNames: []string{"X-Region"}}
	key := generatePrimaryKey(req, cfg)
	// The raw characters : and = inside a header value must be percent-encoded.
	// After the "he=x-region~" label, there must not be a raw colon from the value.
	heIdx := indexOf(key, ":he=x-region~")
	if heIdx == -1 {
		t.Fatalf("he= label missing, got: %s", key)
	}
	// Everything after the label must be the encoded value segment — no unescaped ":"
	tail := key[heIdx+len(":he=x-region~"):]
	// Trim to next ":ck=" or end
	if nextComp := indexOf(tail, ":ck="); nextComp != -1 {
		tail = tail[:nextComp]
	}
	if contains(tail, ":") {
		t.Fatalf("raw colon in header value must be percent-encoded, tail=%q key=%s", tail, key)
	}
	if contains(tail, "=") {
		t.Fatalf("raw equals in header value must be percent-encoded, tail=%q key=%s", tail, key)
	}
}

func TestGeneratePrimaryKey_DelimiterInjection_CookieColonEquals(t *testing.T) {
	req := makeReq("http://example.com/api")
	req.AddCookie(&http.Cookie{Name: "session", Value: "tok:en=abc"})
	cfg := &KeyConfig{IncludedCookieNames: []string{"session"}}
	key := generatePrimaryKey(req, cfg)

	ckIdx := indexOf(key, ":ck=session~")
	if ckIdx == -1 {
		t.Fatalf("ck= label missing, got: %s", key)
	}
	tail := key[ckIdx+len(":ck=session~"):]
	if contains(tail, ":") {
		t.Fatalf("raw colon in cookie value must be percent-encoded, tail=%q key=%s", tail, key)
	}
	if contains(tail, "=") {
		t.Fatalf("raw equals in cookie value must be percent-encoded, tail=%q key=%s", tail, key)
	}
}

func TestGeneratePrimaryKey_DelimiterInjection_SpaceEncoded(t *testing.T) {
	req := makeReq("http://example.com/api")
	req.Header.Set("X-Region", "apac sg (east)")
	cfg := &KeyConfig{IncludedHeaderNames: []string{"X-Region"}}
	key := generatePrimaryKey(req, cfg)
	// url.QueryEscape encodes space as +
	if !contains(key, "apac+sg") && !contains(key, "apac%20sg") {
		t.Fatalf("space in header value must be encoded, got: %s", key)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Headers and Cookies inclusion
// ─────────────────────────────────────────────────────────────────────────────

func TestGeneratePrimaryKey_Headers_SortedAndLowercased(t *testing.T) {
	u := mustParse("http://example.com/store")
	req := &http.Request{
		Method: http.MethodGet,
		Host:   "example.com",
		URL:    u,
		Header: http.Header{},
	}
	req.Header.Set("X-Region", "US-WEST")
	req.Header.Set("Accept-Language", "en-US")

	cfg := &KeyConfig{IncludedHeaderNames: []string{"X-Region", "Accept-Language"}}
	key := generatePrimaryKey(req, cfg)

	// Accept-Language (a) must appear before X-Region (x) — sorted.
	alIdx := indexOf(key, ":he=accept-language~")
	xrIdx := indexOf(key, ":he=x-region~")
	if alIdx == -1 || xrIdx == -1 {
		t.Fatalf("both headers should be present, got: %s", key)
	}
	if alIdx > xrIdx {
		t.Fatalf("accept-language must sort before x-region, got: %s", key)
	}
}

func TestGeneratePrimaryKey_Headers_AbsentHeaderOmitted(t *testing.T) {
	req := makeReq("http://example.com/api")
	cfg := &KeyConfig{IncludedHeaderNames: []string{"X-Region"}} // not set in request
	key := generatePrimaryKey(req, cfg)
	if contains(key, ":he=") {
		t.Fatalf("absent header must be omitted, got: %s", key)
	}
}

func TestGeneratePrimaryKey_Cookies_SortedAndEncoded(t *testing.T) {
	u := mustParse("http://example.com/store")
	req := &http.Request{
		Method: http.MethodGet,
		Host:   "example.com",
		URL:    u,
		Header: http.Header{},
	}
	req.AddCookie(&http.Cookie{Name: "theme", Value: "dark"})
	req.AddCookie(&http.Cookie{Name: "currency", Value: "USD"})

	cfg := &KeyConfig{IncludedCookieNames: []string{"theme", "currency"}}
	key := generatePrimaryKey(req, cfg)

	// currency (c) must appear before theme (t) — sorted.
	curIdx := indexOf(key, ":ck=currency~")
	thIdx := indexOf(key, ":ck=theme~")
	if curIdx == -1 || thIdx == -1 {
		t.Fatalf("both cookies should be present, got: %s", key)
	}
	if curIdx > thIdx {
		t.Fatalf("currency must sort before theme, got: %s", key)
	}
}

func TestGeneratePrimaryKey_Cookies_AbsentCookieOmitted(t *testing.T) {
	req := makeReq("http://example.com/store")
	cfg := &KeyConfig{IncludedCookieNames: []string{"missing_cookie"}}
	key := generatePrimaryKey(req, cfg)
	if contains(key, ":ck=") {
		t.Fatalf("absent cookie must be omitted, got: %s", key)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Full format golden tests
// ─────────────────────────────────────────────────────────────────────────────

func TestGeneratePrimaryKey_Golden(t *testing.T) {
	tests := []struct {
		name     string
		setup    func() *http.Request
		cfg      KeyConfig
		expected string
	}{
		{
			name:     "default zero-value config with sorted query",
			setup:    func() *http.Request { return makeReq("http://Example.COM/api/items/?sort=desc&page=2&id=100") },
			cfg:      KeyConfig{},
			expected: "p=/api/items/:h=example.com:m=GET:qs=id=100&page=2&sort=desc",
		},
		{
			name: "TLS with protocol included",
			setup: func() *http.Request {
				u := mustParse("https://secure.example.com/user/profile")
				return &http.Request{Method: "GET", Host: "secure.example.com", URL: u, Header: http.Header{}, TLS: &tls.ConnectionState{}}
			},
			cfg:      KeyConfig{IncludeProtocol: true},
			expected: "p=/user/profile:h=secure.example.com:m=GET:s=https",
		},
		{
			name:     "ExcludeHost",
			setup:    func() *http.Request { return makeReq("http://cdn.example.com/assets/style.css") },
			cfg:      KeyConfig{ExcludeHost: true},
			expected: "p=/assets/style.css:m=GET",
		},
		{
			name:     "ExcludeQueryString",
			setup:    func() *http.Request { return makeReq("http://example.com/articles?id=99&debug=true") },
			cfg:      KeyConfig{ExcludeQueryString: true},
			expected: "p=/articles:h=example.com:m=GET",
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
			cfg:      KeyConfig{IncludedHeaderNames: []string{"X-Region"}, IncludedCookieNames: []string{"currency"}},
			expected: "p=/store:h=example.com:m=GET:he=x-region~US-WEST:ck=currency~USD",
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
// generateVariantKey
// ─────────────────────────────────────────────────────────────────────────────

func TestGenerateVariantKey_Basic(t *testing.T) {
	req := &http.Request{
		Header: http.Header{
			"Accept-Encoding": []string{"gzip, deflate, br"},
			"Accept-Language": []string{"en-US,en;q=0.9"},
		},
	}
	vary := []string{"Accept-Language", "Accept-Encoding"}
	key := generateVariantKey(req, vary)
	expected := "accept-encoding=br,deflate,gzip|accept-language=en-US,en;q=0.9"
	if key != expected {
		t.Fatalf("expected %q\n     got %q", expected, key)
	}

	// Verify order-independence for standard list headers (e.g. Accept-Encoding)
	reqDifferentOrder := &http.Request{
		Header: http.Header{
			"Accept-Encoding": []string{"br, gzip, deflate"},
			"Accept-Language": []string{"en-US,en;q=0.9"},
		},
	}
	key2 := generateVariantKey(reqDifferentOrder, vary)
	if key2 != key {
		t.Fatalf("variant key for Accept-Encoding with different token order must match:\n key1=%s\n key2=%s", key, key2)
	}

	// Verify custom non-standard Vary header preserves verbatim ordering
	reqCustom := &http.Request{
		Header: http.Header{
			"X-App-Group": []string{"beta,alpha"},
		},
	}
	customKey := generateVariantKey(reqCustom, []string{"X-App-Group"})
	if customKey != "x-app-group=beta,alpha" {
		t.Fatalf("custom header must preserve verbatim ordering, got %q", customKey)
	}
}

func TestGenerateVariantKey_EmptyVary(t *testing.T) {
	req := &http.Request{Header: http.Header{}}
	if generateVariantKey(req, nil) != "" {
		t.Fatal("nil vary headers must produce empty string")
	}
	if generateVariantKey(req, []string{}) != "" {
		t.Fatal("empty vary headers must produce empty string")
	}
}

func TestGenerateVariantKey_MissingHeader(t *testing.T) {
	req := &http.Request{Header: http.Header{}}
	key := generateVariantKey(req, []string{"Accept-Encoding"})
	// Missing header produces name=<empty>
	if key != "accept-encoding=" {
		t.Fatalf("missing header should still produce name= segment, got: %q", key)
	}
}

func TestGenerateVariantKey_Deterministic(t *testing.T) {
	// Vary headers in different order should produce same variant key.
	req := &http.Request{Header: http.Header{
		"Accept-Language": []string{"en-US"},
		"Accept-Encoding": []string{"gzip"},
	}}
	k1 := generateVariantKey(req, []string{"Accept-Language", "Accept-Encoding"})
	k2 := generateVariantKey(req, []string{"Accept-Encoding", "Accept-Language"})
	if k1 != k2 {
		t.Fatalf("variant key must be deterministic regardless of Vary order:\n k1=%s\n k2=%s", k1, k2)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Edge cases
// ─────────────────────────────────────────────────────────────────────────────

func TestGeneratePrimaryKey_MultipleQueryValues(t *testing.T) {
	// Same key multiple times — sorted values
	req := makeReq("http://example.com/q?tag=b&tag=a&tag=c")
	key := generatePrimaryKey(req, &KeyConfig{})
	if !contains(key, ":qs=") {
		t.Fatalf("expected qs= segment, got: %s", key)
	}
	// Values should be sorted: a, b, c
	qsIdx := indexOf(key, ":qs=")
	qs := key[qsIdx:]
	aPos := indexOf(qs, "tag=a")
	bPos := indexOf(qs, "tag=b")
	cPos := indexOf(qs, "tag=c")
	if aPos == -1 || bPos == -1 || cPos == -1 {
		t.Fatalf("all tag values must appear, got: %s", key)
	}
	if aPos >= bPos || bPos >= cPos {
		t.Fatalf("tag values must be sorted a<b<c, got: %s", key)
	}
}

func TestGeneratePrimaryKey_EmptyQueryValueParam(t *testing.T) {
	req := makeReq("http://example.com/page?flag")
	key := generatePrimaryKey(req, &KeyConfig{})
	// flag with no value should still appear
	if !contains(key, ":qs=") {
		t.Fatalf("flag param should produce qs= segment, got: %s", key)
	}
}

func TestGeneratePrimaryKey_SchemeFromURLField(t *testing.T) {
	u := mustParse("https://example.com/api")
	req := &http.Request{
		Method: http.MethodGet,
		Host:   "example.com",
		URL:    u,
		Header: http.Header{},
		// No TLS, no X-Forwarded-Proto — scheme comes from URL.Scheme
	}
	key := generatePrimaryKey(req, &KeyConfig{IncludeProtocol: true})
	if !contains(key, ":s=https") {
		t.Fatalf("should detect https from URL.Scheme, got: %s", key)
	}
}

// contains is a helper to avoid importing strings in table-driven sub-test comparisons.
func contains(s, substr string) bool {
	return indexOf(s, substr) != -1
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
	cfg := &KeyConfig{}

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
	cfg := &KeyConfig{}

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

	cfg := &KeyConfig{
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

	cfg := &KeyConfig{
		IncludeProtocol:        true,
		ExcludeMarketingParams: true,
		IncludedHeaderNames:    []string{"X-Region"},
		IncludedCookieNames:    []string{"ab_group"},
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

func TestGeneratePrimaryKey_TrailingSlash(t *testing.T) {
	reqWithoutSlash := makeReq("http://example.com/api")
	keyWithout := generatePrimaryKey(reqWithoutSlash, &KeyConfig{})
	if keyWithout != "p=/api:h=example.com:m=GET" {
		t.Errorf("expected p=/api, got %s", keyWithout)
	}

	reqWithSlash := makeReq("http://example.com/api/")
	keyWith := generatePrimaryKey(reqWithSlash, &KeyConfig{})
	if keyWith != "p=/api/:h=example.com:m=GET" {
		t.Errorf("expected p=/api/, got %s", keyWith)
	}

	if keyWithout == keyWith {
		t.Fatalf("expected /api and /api/ to have distinct primary keys")
	}

	reqQueryWithSlash := makeReq("http://example.com/api/?zone=abc")
	keyQueryWith := generatePrimaryKey(reqQueryWithSlash, &KeyConfig{})
	if keyQueryWith != "p=/api/:h=example.com:m=GET:qs=zone=abc" {
		t.Errorf("expected p=/api/:h=example.com:m=GET:qs=zone=abc, got %s", keyQueryWith)
	}
}

func TestGenerateVariantKey_PreservesCasing(t *testing.T) {
	req := &http.Request{
		Header: http.Header{
			"X-Custom-Vary": []string{"CaseSensitiveValue123"},
		},
	}
	vKey := generateVariantKey(req, []string{"X-Custom-Vary"})
	if vKey != "x-custom-vary=CaseSensitiveValue123" {
		t.Errorf("expected x-custom-vary=CaseSensitiveValue123, got %s", vKey)
	}
}
