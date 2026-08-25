package titip

import (
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestParseAge(t *testing.T) {
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
	baseTime := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	reqTime := baseTime
	respTime := baseTime.Add(100 * time.Millisecond)
	now := respTime.Add(5 * time.Second)

	// Scenario 1: Upstream cached response with Age: 30, max-age=120
	headers := http.Header{
		"Date":          []string{baseTime.Add(-30 * time.Second).Format(http.TimeFormat)},
		"Age":           []string{"30"},
		"Cache-Control": []string{"public, max-age=120"},
	}

	info := calculateFreshness(http.StatusOK, headers, reqTime, respTime, now, nil)
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

	// Scenario 2: s-maxage overrides max-age for shared cache
	headers2 := http.Header{
		"Cache-Control": []string{"max-age=60, s-maxage=300"},
	}
	info2 := calculateFreshness(http.StatusOK, headers2, reqTime, respTime, now, nil)
	if info2.FreshnessLifetime != 300*time.Second {
		t.Fatalf("expected s-maxage 300s to take precedence, got %v", info2.FreshnessLifetime)
	}

	// Scenario 3: stale-while-revalidate and stale-if-error
	headers3 := http.Header{
		"Cache-Control": []string{"public, max-age=60, stale-while-revalidate=30, stale-if-error=120"},
	}
	info3 := calculateFreshness(http.StatusOK, headers3, reqTime, respTime, now, nil)
	if info3.StaleWhileRevalidateTTL != 30*time.Second {
		t.Fatalf("expected SWR 30s, got %v", info3.StaleWhileRevalidateTTL)
	}
	if info3.StaleIfErrorTTL != 120*time.Second {
		t.Fatalf("expected SIE 120s, got %v", info3.StaleIfErrorTTL)
	}

	// Scenario 4: MaxCacheTTL clamping (1 year)
	headers4 := http.Header{
		"Cache-Control": []string{"public, max-age=60000000"}, // ~1.9 years
	}
	info4 := calculateFreshness(http.StatusOK, headers4, reqTime, respTime, now, nil)
	if info4.EffectiveTTL != maxCacheTTL {
		t.Fatalf("expected TTL clamped to %v, got %v", maxCacheTTL, info4.EffectiveTTL)
	}
}

func TestIsResponseCacheable_StrictRules(t *testing.T) {
	// Rule 1: Set-Cookie presence prevents caching unconditionally
	hWithCookie := http.Header{
		"Cache-Control": []string{"public, max-age=300"},
		"Set-Cookie":    []string{"session_id=secret123; Path=/"},
	}
	if info := calculateFreshness(http.StatusOK, hWithCookie, time.Now(), time.Now(), time.Now(), nil); info.IsCacheable {
		t.Fatal("expected IsCacheable=false when Set-Cookie is present")
	}

	// Rule 2: private directive prevents caching
	hPrivate := http.Header{
		"Cache-Control": []string{"private, max-age=300"},
	}
	if info := calculateFreshness(http.StatusOK, hPrivate, time.Now(), time.Now(), time.Now(), nil); info.IsCacheable {
		t.Fatal("expected IsCacheable=false when Cache-Control is private")
	}

	// Rule 3: no-store directive prevents caching
	hNoStore := http.Header{
		"Cache-Control": []string{"no-store, max-age=300"},
	}
	if info := calculateFreshness(http.StatusOK, hNoStore, time.Now(), time.Now(), time.Now(), nil); info.IsCacheable {
		t.Fatal("expected IsCacheable=false when Cache-Control is no-store")
	}

	// Rule 4: Missing Cache-Control directive prevents caching (no heuristic caching)
	hEmpty := http.Header{}
	if info := calculateFreshness(http.StatusOK, hEmpty, time.Now(), time.Now(), time.Now(), nil); info.IsCacheable {
		t.Fatal("expected IsCacheable=false when Cache-Control is absent")
	}

	// Rule 5: Non-cacheable status code (e.g. 401 Unauthorized)
	hValidCC := http.Header{
		"Cache-Control": []string{"public, max-age=300"},
	}
	if info := calculateFreshness(http.StatusUnauthorized, hValidCC, time.Now(), time.Now(), time.Now(), nil); info.IsCacheable {
		t.Fatal("expected IsCacheable=false for status 401")
	}

	// Rule 6: Cacheable standard error status (e.g. 404, 500, 503) with explicit Cache-Control
	if info := calculateFreshness(http.StatusNotFound, hValidCC, time.Now(), time.Now(), time.Now(), nil); !info.IsCacheable {
		t.Fatal("expected IsCacheable=true for status 404 with explicit Cache-Control")
	}
	if info := calculateFreshness(http.StatusServiceUnavailable, hValidCC, time.Now(), time.Now(), time.Now(), nil); !info.IsCacheable {
		t.Fatal("expected IsCacheable=true for status 503 with explicit Cache-Control")
	}
}

func TestFreshnessAndKeyGenConcurrency(t *testing.T) {
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
				info := calculateFreshness(http.StatusOK, headers, time.Now().Add(-100*time.Millisecond), time.Now(), time.Now(), nil)
				if !info.IsCacheable {
					t.Errorf("expected cacheable in concurrent loop")
				}
			}
		}(i)
	}

	wg.Wait()
}
