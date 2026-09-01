package titip

import (
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestParseAge(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input    string
		expected time.Duration
	}{
		{"", 0},
		{"0", 0},
		{"30", 30 * time.Second},
		{"  120  ", 120 * time.Second},
		{"-5", 0},
		{"invalid", 0},
	}

	for _, tt := range tests {
		got := parseAge(tt.input)
		if got != tt.expected {
			t.Errorf("parseAge(%q) = %v, expected %v", tt.input, got, tt.expected)
		}
	}
}

func TestParseDate(t *testing.T) {
	t.Parallel()
	// RFC 1123 format
	d1, err := parseDate("Sun, 06 Nov 1994 08:49:37 GMT")
	if err != nil || d1.Year() != 1994 || d1.Hour() != 8 {
		t.Fatalf("failed to parse RFC1123 date: %v, %v", d1, err)
	}

	// RFC 850 format
	d2, err := parseDate("Sunday, 06-Nov-94 08:49:37 GMT")
	if err != nil || d2.Year() != 1994 {
		t.Fatalf("failed to parse RFC850 date: %v, %v", d2, err)
	}

	// ANSI C format
	d3, err := parseDate("Sun Nov  6 08:49:37 1994")
	if err != nil || d3.Year() != 1994 {
		t.Fatalf("failed to parse ANSIC date: %v, %v", d3, err)
	}

	// Empty string
	d4, err := parseDate("")
	if err != nil || !d4.IsZero() {
		t.Fatalf("expected zero time for empty string, got %v, %v", d4, err)
	}

	// Invalid format
	_, err = parseDate("invalid date string")
	if err == nil {
		t.Fatal("expected error for invalid date")
	}
}

func TestCalculateFreshness_RFC7234_Scenarios(t *testing.T) {
	t.Parallel()
	baseTime := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	reqTime := baseTime
	respTime := baseTime.Add(100 * time.Millisecond)
	now := respTime.Add(5 * time.Second)

	// Scenario 1 (RFC 9111 §4.2.3 & RFC 7234 §4.2.3): Upstream cached response with Age: 30, max-age=120
	// Tests corrected_initial_age, resident_time, and effective_ttl calculation
	headers := http.Header{
		"Date":          []string{baseTime.Add(-30 * time.Second).Format(http.TimeFormat)},
		"Age":           []string{"30"},
		"Cache-Control": []string{"public, max-age=120"},
	}

	info := calculateFreshness(http.StatusOK, nil, headers, reqTime, respTime, now)
	if !info.IsCacheable {
		t.Fatal("expected response to be cacheable")
	}
	if info.FreshnessLifetime != 120*time.Second {
		t.Fatalf("expected freshness lifetime 120s, got %v", info.FreshnessLifetime)
	}
	// Initial corrected age is 30s + responseDelay (~0.1s) = ~30.1s
	if info.CorrectedInitialAge < 30*time.Second || info.CorrectedInitialAge > 31*time.Second {
		t.Fatalf("unexpected corrected initial age: %v", info.CorrectedInitialAge)
	}
	// Effective TTL should be 120s - ~30.1s = ~89.9s
	if info.EffectiveTTL < 89*time.Second || info.EffectiveTTL > 90*time.Second {
		t.Fatalf("unexpected effective TTL: %v", info.EffectiveTTL)
	}

	// Scenario 2 (RFC 9111 §5.2.2.10): s-maxage overrides max-age for shared caches
	headers2 := http.Header{
		"Cache-Control": []string{"max-age=60, s-maxage=300"},
	}
	info2 := calculateFreshness(http.StatusOK, nil, headers2, reqTime, respTime, now)
	if info2.FreshnessLifetime != 300*time.Second {
		t.Fatalf("expected s-maxage 300s to take precedence, got %v", info2.FreshnessLifetime)
	}

	// Scenario 3 (RFC 5861 §3 & §4): stale-while-revalidate and stale-if-error parsing
	headers3 := http.Header{
		"Cache-Control": []string{"public, max-age=60, stale-while-revalidate=30, stale-if-error=120"},
	}
	info3 := calculateFreshness(http.StatusOK, nil, headers3, reqTime, respTime, now)
	if info3.StaleWhileRevalidateTTL != 30*time.Second {
		t.Fatalf("expected SWR 30s, got %v", info3.StaleWhileRevalidateTTL)
	}
	if info3.StaleIfErrorTTL != 120*time.Second {
		t.Fatalf("expected SIE 120s, got %v", info3.StaleIfErrorTTL)
	}

	// Scenario 4 (RFC 9111 Section 4.2.3 & Titip Architecture): MaxCacheTTL clamping (1 year)
	headers4 := http.Header{
		"Cache-Control": []string{"public, max-age=60000000"}, // ~1.9 years
	}
	info4 := calculateFreshness(http.StatusOK, nil, headers4, reqTime, respTime, now)
	if info4.EffectiveTTL != maxCacheTTL {
		t.Fatalf("expected TTL clamped to %v, got %v", maxCacheTTL, info4.EffectiveTTL)
	}
}

func TestIsResponseCacheable_StrictRules(t *testing.T) {
	t.Parallel()
	// Rule 1 (AGENTS.md Rule #4): Set-Cookie presence prevents caching unconditionally to protect user sessions
	hWithCookie := http.Header{
		"Cache-Control": []string{"public, max-age=300"},
		"Set-Cookie":    []string{"session_id=secret123; Path=/"},
	}
	if info := calculateFreshness(http.StatusOK, nil, hWithCookie, time.Now(), time.Now(), time.Now()); info.IsCacheable {
		t.Fatal("expected IsCacheable=false when Set-Cookie is present")
	}

	// Rule 1b: Content-Type: text/event-stream prevents caching SSE streams
	hSSE := http.Header{
		"Cache-Control": []string{"public, max-age=300"},
		"Content-Type":  []string{"text/event-stream; charset=utf-8"},
	}
	if info := calculateFreshness(http.StatusOK, nil, hSSE, time.Now(), time.Now(), time.Now()); info.IsCacheable {
		t.Fatal("expected IsCacheable=false when Content-Type is text/event-stream")
	}

	// Rule 2 (RFC 9111 §5.2.2.7): Cache-Control: private prevents shared caching
	hPrivate := http.Header{
		"Cache-Control": []string{"private, max-age=300"},
	}
	if info := calculateFreshness(http.StatusOK, nil, hPrivate, time.Now(), time.Now(), time.Now()); info.IsCacheable {
		t.Fatal("expected IsCacheable=false when Cache-Control is private")
	}

	// Rule 3 (RFC 9111 §5.2.2.5): Cache-Control: no-store directive prevents storing the response
	hNoStore := http.Header{
		"Cache-Control": []string{"no-store, max-age=300"},
	}
	if info := calculateFreshness(http.StatusOK, nil, hNoStore, time.Now(), time.Now(), time.Now()); info.IsCacheable {
		t.Fatal("expected IsCacheable=false when Cache-Control is no-store")
	}

	// Rule 4 (RFC 9111 §4.2.1): Missing Cache-Control directive without Expires prevents caching
	hEmpty := http.Header{}
	if info := calculateFreshness(http.StatusOK, nil, hEmpty, time.Now(), time.Now(), time.Now()); info.IsCacheable {
		t.Fatal("expected IsCacheable=false when Cache-Control and Expires are absent")
	}

	// Rule 4b (RFC 9111 §4.2.1 / §5.3 / Cloudflare compatibility): Expires with future date without Cache-Control is cacheable
	futureExp := http.Header{
		"Date":    []string{time.Now().Format(http.TimeFormat)},
		"Expires": []string{time.Now().Add(120 * time.Second).Format(http.TimeFormat)},
	}
	infoExp := calculateFreshness(http.StatusOK, nil, futureExp, time.Now(), time.Now(), time.Now())
	if !infoExp.IsCacheable {
		t.Fatal("expected IsCacheable=true for future Expires without Cache-Control")
	}
	if infoExp.EffectiveTTL < 110*time.Second {
		t.Fatalf("expected EffectiveTTL ~120s from Expires header, got %v", infoExp.EffectiveTTL)
	}

	// Rule 4c (RFC 9111 §5.3): Expires with past date or "0" without Cache-Control is not cacheable
	pastExp := http.Header{
		"Expires": []string{"0"},
	}
	if info := calculateFreshness(http.StatusOK, nil, pastExp, time.Now(), time.Now(), time.Now()); info.IsCacheable {
		t.Fatal("expected IsCacheable=false for Expires: 0")
	}

	// Rule 5 (RFC 9110 §15): Uncacheable status codes (e.g. 100 Continue, 205 Reset Content, 206 Partial Content, 303 See Other, 401 Unauthorized, 407 Proxy Auth, 421 Misdirected, 426 Upgrade Required)
	hValidCC := http.Header{
		"Cache-Control": []string{"public, max-age=300"},
	}
	uncacheableStatuses := []int{
		http.StatusContinue,
		http.StatusResetContent,       // 205
		http.StatusPartialContent,     // 206
		http.StatusSeeOther,           // 303
		http.StatusUnauthorized,       // 401
		http.StatusProxyAuthRequired,  // 407
		http.StatusMisdirectedRequest, // 421
		http.StatusUpgradeRequired,    // 426
	}
	for _, st := range uncacheableStatuses {
		if info := calculateFreshness(st, nil, hValidCC, time.Now(), time.Now(), time.Now()); info.IsCacheable {
			t.Fatalf("expected IsCacheable=false for uncacheable status %d", st)
		}
	}

	// Rule 6 (RFC 9110 §15.5 & RFC 9111 §3.1): Cacheable standard error status codes (404, 500, 503, 414) with explicit Cache-Control
	if info := calculateFreshness(http.StatusNotFound, nil, hValidCC, time.Now(), time.Now(), time.Now()); !info.IsCacheable {
		t.Fatal("expected IsCacheable=true for status 404 with explicit Cache-Control")
	}
	if info := calculateFreshness(http.StatusServiceUnavailable, nil, hValidCC, time.Now(), time.Now(), time.Now()); !info.IsCacheable {
		t.Fatal("expected IsCacheable=true for status 503 with explicit Cache-Control")
	}
	if info := calculateFreshness(http.StatusRequestURITooLong, nil, hValidCC, time.Now(), time.Now(), time.Now()); !info.IsCacheable {
		t.Fatal("expected IsCacheable=true for status 414 with explicit Cache-Control")
	}

	// Rule 7 (RFC 9111 §4.1 / RFC 7231 §7.1.4): Vary: * prohibits caching and shared reuse
	hVaryStar := http.Header{
		"Cache-Control": []string{"public, max-age=300"},
		"Vary":          []string{"*"},
	}
	if info := calculateFreshness(http.StatusOK, nil, hVaryStar, time.Now(), time.Now(), time.Now()); info.IsCacheable {
		t.Fatal("expected IsCacheable=false when Vary: * is present")
	}

	// Rule 8 (RFC 9111 §3.5): Request Authorization guard for shared caches
	reqAuth := http.Header{"Authorization": []string{"Bearer secret-token"}}
	// Max-age alone with Auth header must be uncacheable
	hAuthMaxAge := http.Header{"Cache-Control": []string{"max-age=300"}}
	if info := calculateFreshness(http.StatusOK, reqAuth, hAuthMaxAge, time.Now(), time.Now(), time.Now()); info.IsCacheable {
		t.Fatal("expected IsCacheable=false for Authorization request without public/s-maxage")
	}
	// With must-revalidate alone in shared cache: uncacheable without public/s-maxage to prevent cross-user leak
	hAuthMustReval := http.Header{"Cache-Control": []string{"must-revalidate, max-age=300"}}
	if info := calculateFreshness(http.StatusOK, reqAuth, hAuthMustReval, time.Now(), time.Now(), time.Now()); info.IsCacheable {
		t.Fatal("expected IsCacheable=false for Authorization request with only must-revalidate in shared cache")
	}
	// With public: cacheable (RFC 9111 §3.5)
	hAuthPublic := http.Header{"Cache-Control": []string{"public, max-age=300"}}
	if info := calculateFreshness(http.StatusOK, reqAuth, hAuthPublic, time.Now(), time.Now(), time.Now()); !info.IsCacheable {
		t.Fatal("expected IsCacheable=true for Authorization request with public directive")
	}
	// With s-maxage: cacheable (RFC 9111 §3.5)
	hAuthSMaxAge := http.Header{"Cache-Control": []string{"s-maxage=300"}}
	if info := calculateFreshness(http.StatusOK, reqAuth, hAuthSMaxAge, time.Now(), time.Now(), time.Now()); !info.IsCacheable {
		t.Fatal("expected IsCacheable=true for Authorization request with s-maxage directive")
	}

	// Rule 9 (RFC 5861 §3 & §4 / RFC 9111 §5.2.2.1): must-revalidate and proxy-revalidate disable SWR and SIE
	hMustRevalSWR := http.Header{
		"Cache-Control": []string{"public, max-age=60, must-revalidate, stale-while-revalidate=30, stale-if-error=120"},
	}
	infoMustReval := calculateFreshness(http.StatusOK, nil, hMustRevalSWR, time.Now(), time.Now(), time.Now())
	if infoMustReval.StaleWhileRevalidateTTL != 0 || infoMustReval.StaleIfErrorTTL != 0 {
		t.Fatalf("expected SWR and SIE to be 0 when must-revalidate is present, got SWR=%v, SIE=%v", infoMustReval.StaleWhileRevalidateTTL, infoMustReval.StaleIfErrorTTL)
	}

	// Rule 10 (RFC 9111 §5.2.2.4): no-cache resets FreshnessLifetime to 0 (forces revalidation before each use)
	hNoCache := http.Header{
		"Cache-Control": []string{"no-cache, max-age=300"},
	}
	infoNoCache := calculateFreshness(http.StatusOK, nil, hNoCache, time.Now(), time.Now(), time.Now())
	if infoNoCache.FreshnessLifetime != 0 || infoNoCache.EffectiveTTL != 0 {
		t.Fatalf("expected FreshnessLifetime=0 and EffectiveTTL=0 for no-cache, got %v, %v", infoNoCache.FreshnessLifetime, infoNoCache.EffectiveTTL)
	}
}

func TestFreshnessAndKeyGenConcurrency(t *testing.T) {
	t.Parallel()
	const goroutines = 100
	const iterations = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := range goroutines {
		go func(id int) {
			defer wg.Done()

			req, _ := http.NewRequest(http.MethodGet, "https://example.com/api/v1/items?id=42&sort=asc", nil)
			req.Header.Set("Accept-Encoding", "gzip")
			cfg := &KeyConfig{ExcludeMarketingParams: true}

			for range iterations {
				_ = generatePrimaryKey(req, cfg)
				_ = generateVariantKey(req, []string{"Accept-Encoding"})

				headers := http.Header{
					"Date":          []string{time.Now().Add(-10 * time.Second).Format(http.TimeFormat)},
					"Cache-Control": []string{"public, max-age=120, stale-while-revalidate=30"},
				}
				info := calculateFreshness(http.StatusOK, req.Header, headers, time.Now().Add(-100*time.Millisecond), time.Now(), time.Now())
				if !info.IsCacheable {
					t.Errorf("expected cacheable in concurrent loop")
				}
			}
		}(i)
	}

	wg.Wait()
}

// TestRFC_CacheControl_ConflictResolution verifies that conflicting Cache-Control
// directives are handled safely via conservative rejection gates.
// This serves as regression protection if isResponseCacheable() implementation changes.
func TestRFC_CacheControl_ConflictResolution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		header      string
		expectCache bool
		reason      string
	}{
		{
			name:        "public_and_private_explicit",
			header:      "public, private, max-age=60",
			expectCache: false,
			reason:      "private wins over public per RFC conservative choice",
		},
		{
			name:        "private_and_public_reverse_order",
			header:      "private, public, max-age=60",
			expectCache: false,
			reason:      "order-independent: private always prohibits shared caching",
		},
		{
			name:        "no_store_and_public",
			header:      "no-store, public, max-age=60",
			expectCache: false,
			reason:      "no-store overrides any permissive directive",
		},
		{
			name:        "max_age_zero_with_public",
			header:      "public, max-age=0",
			expectCache: true,
			reason:      "valid directive, cache allowed but immediately expires",
		},
		{
			name:        "s_maxage_less_than_max_age",
			header:      "public, max-age=300, s-maxage=60",
			expectCache: true,
			reason:      "RFC-compliant: shared caches use shorter TTL, private caches longer",
		},
		{
			name:        "s_maxage_greater_than_max_age",
			header:      "public, max-age=60, s-maxage=300",
			expectCache: true,
			reason:      "RFC-compliant: different TTLs per cache type",
		},
		{
			name:        "no_cache_and_public",
			header:      "no-cache, public, max-age=60",
			expectCache: true,
			reason:      "cache allowed but requires revalidation before each use",
		},
		{
			name:        "must_revalidate_and_swroverride",
			header:      "public, max-age=60, must-revalidate, stale-while-revalidate=30",
			expectCache: true,
			reason:      "cacheable but SWR disabled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			headers := http.Header{"Cache-Control": []string{tt.header}}
			info := calculateFreshness(http.StatusOK, nil, headers, time.Now(), time.Now(), time.Now())

			if info.IsCacheable != tt.expectCache {
				t.Fatalf("IsCacheable=%v, expected %v (%s)",
					info.IsCacheable, tt.expectCache, tt.reason)
			}

			// Additional validation for specific scenarios
			if tt.name == "public_and_private_explicit" && info.Directives.Public && info.Directives.PrivatePresent {
				t.Log("Library sets both flags - safety net relies on check order")
			}
		})
	}
}

func TestMultipleVaryHeaders(t *testing.T) {
	t.Parallel()

	// 1. Multiple Vary lines where second line is "*" must prohibit caching
	hMultipleVaryStar := http.Header{
		"Cache-Control": []string{"public, max-age=300"},
		"Vary":          []string{"Accept-Encoding", "*"},
	}
	if info := calculateFreshness(http.StatusOK, nil, hMultipleVaryStar, time.Now(), time.Now(), time.Now()); info.IsCacheable {
		t.Fatal("expected IsCacheable=false when second Vary header is *")
	}

	// 2. Multiple Vary lines with valid names must remain cacheable
	hMultipleVaryValid := http.Header{
		"Cache-Control": []string{"public, max-age=300"},
		"Vary":          []string{"Accept-Encoding", "Accept-Language, User-Agent"},
	}
	if info := calculateFreshness(http.StatusOK, nil, hMultipleVaryValid, time.Now(), time.Now(), time.Now()); !info.IsCacheable {
		t.Fatal("expected IsCacheable=true for multiple valid Vary headers")
	}
}

func TestMultipleCacheControlHeaders(t *testing.T) {
	t.Parallel()

	// 1. Multiple Cache-Control lines combined with valid directives
	hMultipleCC := http.Header{
		"Cache-Control": []string{"public", "max-age=300, s-maxage=600"},
	}
	info := calculateFreshness(http.StatusOK, nil, hMultipleCC, time.Now(), time.Now(), time.Now())
	if !info.IsCacheable {
		t.Fatal("expected IsCacheable=true when Cache-Control is split across multiple lines")
	}
	if info.FreshnessLifetime != 600*time.Second {
		t.Fatalf("expected FreshnessLifetime=600s from s-maxage across split headers, got %v", info.FreshnessLifetime)
	}

	// 2. Multiple Cache-Control lines where second line is 'private'
	hMultipleCCPrivate := http.Header{
		"Cache-Control": []string{"s-maxage=60, public", "private"},
	}
	infoPrivate := calculateFreshness(http.StatusOK, nil, hMultipleCCPrivate, time.Now(), time.Now(), time.Now())
	if infoPrivate.IsCacheable {
		t.Fatal("expected IsCacheable=false when second Cache-Control line contains 'private'")
	}

	// 3. Multiple Cache-Control lines where second line is 'no-store'
	hMultipleCCNoStore := http.Header{
		"Cache-Control": []string{"public, max-age=3600", "no-store"},
	}
	infoNoStore := calculateFreshness(http.StatusOK, nil, hMultipleCCNoStore, time.Now(), time.Now(), time.Now())
	if infoNoStore.IsCacheable {
		t.Fatal("expected IsCacheable=false when second Cache-Control line contains 'no-store'")
	}
}

func TestRFC9213_TieredCacheControl(t *testing.T) {
	t.Parallel()

	// 1. Titip-Cache-Control overrides both CDN-Cache-Control and Cache-Control
	hTitipWins := http.Header{
		"Titip-Cache-Control": []string{"public, max-age=100"},
		"CDN-Cache-Control":   []string{"public, max-age=200"},
		"Cache-Control":       []string{"public, max-age=300"},
	}
	info1 := calculateFreshness(http.StatusOK, nil, hTitipWins, time.Now(), time.Now(), time.Now())
	if !info1.IsCacheable || info1.FreshnessLifetime != 100*time.Second {
		t.Fatalf("expected Titip-Cache-Control to win with 100s TTL, got isCacheable=%v, lifetime=%v", info1.IsCacheable, info1.FreshnessLifetime)
	}

	// 2. CDN-Cache-Control overrides standard Cache-Control
	hCDNWins := http.Header{
		"CDN-Cache-Control": []string{"public, max-age=200"},
		"Cache-Control":     []string{"public, max-age=300"},
	}
	info2 := calculateFreshness(http.StatusOK, nil, hCDNWins, time.Now(), time.Now(), time.Now())
	if !info2.IsCacheable || info2.FreshnessLifetime != 200*time.Second {
		t.Fatalf("expected CDN-Cache-Control to win with 200s TTL, got isCacheable=%v, lifetime=%v", info2.IsCacheable, info2.FreshnessLifetime)
	}

	// 3. Fallback to standard Cache-Control when targeted headers are absent
	hStandard := http.Header{
		"Cache-Control": []string{"public, max-age=300"},
	}
	info3 := calculateFreshness(http.StatusOK, nil, hStandard, time.Now(), time.Now(), time.Now())
	if !info3.IsCacheable || info3.FreshnessLifetime != 300*time.Second {
		t.Fatalf("expected Cache-Control fallback with 300s TTL, got isCacheable=%v, lifetime=%v", info3.IsCacheable, info3.FreshnessLifetime)
	}

	// 4. Titip-Cache-Control is cacheable while Cache-Control is private/no-store for browsers
	hDecoupled := http.Header{
		"Titip-Cache-Control": []string{"public, max-age=86400, stale-while-revalidate=3600"},
		"Cache-Control":       []string{"private, no-store"},
	}
	info4 := calculateFreshness(http.StatusOK, nil, hDecoupled, time.Now(), time.Now(), time.Now())
	if !info4.IsCacheable || info4.FreshnessLifetime != 86400*time.Second || info4.StaleWhileRevalidateTTL != 3600*time.Second {
		t.Fatalf("expected Titip to cache decoupled response for 86400s, got isCacheable=%v, lifetime=%v, swr=%v", info4.IsCacheable, info4.FreshnessLifetime, info4.StaleWhileRevalidateTTL)
	}

	// 5. Titip-Cache-Control contains private -> Titip rejects caching even if Cache-Control says public
	hTitipPrivate := http.Header{
		"Titip-Cache-Control": []string{"private"},
		"Cache-Control":       []string{"public, max-age=3600"},
	}
	info5 := calculateFreshness(http.StatusOK, nil, hTitipPrivate, time.Now(), time.Now(), time.Now())
	if info5.IsCacheable {
		t.Fatal("expected Titip to reject caching when Titip-Cache-Control is private")
	}

	// 6. Multi-line Titip-Cache-Control
	hMultiTitip := http.Header{
		"Titip-Cache-Control": []string{"public", "max-age=600"},
	}
	info6 := calculateFreshness(http.StatusOK, nil, hMultiTitip, time.Now(), time.Now(), time.Now())
	if !info6.IsCacheable || info6.FreshnessLifetime != 600*time.Second {
		t.Fatalf("expected multi-line Titip-Cache-Control to be parsed with 600s TTL, got %v", info6.FreshnessLifetime)
	}
}
