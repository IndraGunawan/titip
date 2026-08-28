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

	// Rule 5 (RFC 9110 §15): Uncacheable status codes (e.g. 100 Continue, 205 Reset Content, 206 Partial Content)
	hValidCC := http.Header{
		"Cache-Control": []string{"public, max-age=300"},
	}
	if info := calculateFreshness(http.StatusContinue, nil, hValidCC, time.Now(), time.Now(), time.Now()); info.IsCacheable {
		t.Fatal("expected IsCacheable=false for status 100")
	}
	if info := calculateFreshness(http.StatusResetContent, nil, hValidCC, time.Now(), time.Now(), time.Now()); info.IsCacheable {
		t.Fatal("expected IsCacheable=false for status 205")
	}
	if info := calculateFreshness(http.StatusPartialContent, nil, hValidCC, time.Now(), time.Now(), time.Now()); info.IsCacheable {
		t.Fatal("expected IsCacheable=false for status 206")
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
		t.Fatal("expected IsCacheable=false for Authorization request without public/s-maxage/must-revalidate")
	}
	// With public: cacheable (RFC 9111 §3.5 bullet 1)
	hAuthPublic := http.Header{"Cache-Control": []string{"public, max-age=300"}}
	if info := calculateFreshness(http.StatusOK, reqAuth, hAuthPublic, time.Now(), time.Now(), time.Now()); !info.IsCacheable {
		t.Fatal("expected IsCacheable=true for Authorization request with public directive")
	}
	// With s-maxage: cacheable (RFC 9111 §3.5 bullet 2)
	hAuthSMaxAge := http.Header{"Cache-Control": []string{"s-maxage=300"}}
	if info := calculateFreshness(http.StatusOK, reqAuth, hAuthSMaxAge, time.Now(), time.Now(), time.Now()); !info.IsCacheable {
		t.Fatal("expected IsCacheable=true for Authorization request with s-maxage directive")
	}
	// With must-revalidate: cacheable (RFC 9111 §3.5 bullet 3)
	hAuthMustReval := http.Header{"Cache-Control": []string{"must-revalidate, max-age=300"}}
	if info := calculateFreshness(http.StatusOK, reqAuth, hAuthMustReval, time.Now(), time.Now(), time.Now()); !info.IsCacheable {
		t.Fatal("expected IsCacheable=true for Authorization request with must-revalidate directive")
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

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()

			req, _ := http.NewRequest(http.MethodGet, "https://example.com/api/v1/items?id=42&sort=asc", nil)
			req.Header.Set("Accept-Encoding", "gzip")
			cfg := &KeyConfig{ExcludeMarketingParams: true}

			for j := 0; j < iterations; j++ {
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
