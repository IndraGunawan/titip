package titip

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/indragunawan/titip/esi"
)

func TestServerTiming_Disabled(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t, WithServerTiming(false))

	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(headerCacheControl, "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	handler := mw.testHandler(origin)

	req := httptest.NewRequest(http.MethodGet, "http://example.com/disabled", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if val := rec.Header().Get("Server-Timing"); val != "" {
		t.Fatalf("expected no Server-Timing header when disabled, got: %q", val)
	}
}

func TestServerTiming_AlwaysEnabled_HitAndMiss(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t, WithServerTiming(true))

	originCalls := 0
	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls++
		time.Sleep(10 * time.Millisecond) // Ensure measurable origin duration
		w.Header().Set(headerContentType, "text/plain")
		w.Header().Set(headerCacheControl, "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("cached content"))
	})
	handler := mw.testHandler(origin)

	// 1. Cold Miss
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/timing-test", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	timing1 := rec1.Header().Get("Server-Timing")
	if timing1 == "" {
		t.Fatal("expected Server-Timing header on cache miss, got empty")
	}
	if !strings.Contains(timing1, `titip-status;desc="MISS"`) {
		t.Errorf("expected titip-status MISS, got: %q", timing1)
	}
	if !strings.Contains(timing1, "titip-origin;dur=") {
		t.Errorf("expected titip-origin;dur= on miss, got: %q", timing1)
	}
	if !strings.Contains(timing1, "titip-store;dur=") {
		t.Errorf("expected titip-store;dur= on cacheable miss, got: %q", timing1)
	}
	if !strings.Contains(timing1, "titip;dur=") {
		t.Errorf("expected titip;dur= on miss, got: %q", timing1)
	}

	// 2. Cache Hit
	req2 := httptest.NewRequest(http.MethodGet, "http://example.com/timing-test", nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	timing2 := rec2.Header().Get("Server-Timing")
	if timing2 == "" {
		t.Fatal("expected Server-Timing header on cache hit, got empty")
	}
	if !strings.Contains(timing2, `titip-status;desc="HIT"`) {
		t.Errorf("expected titip-status HIT, got: %q", timing2)
	}
	if !strings.Contains(timing2, "titip-meta;dur=") {
		t.Errorf("expected titip-meta;dur= on hit, got: %q", timing2)
	}
	if !strings.Contains(timing2, "titip-body;dur=") {
		t.Errorf("expected titip-body;dur= on hit, got: %q", timing2)
	}
	if !strings.Contains(timing2, "titip;dur=") {
		t.Errorf("expected titip;dur= on hit, got: %q", timing2)
	}
}

func TestServerTiming_CookieGated(t *testing.T) {
	t.Parallel()
	const cookieName = "titip_debug"
	const cookieVal = "secret123"

	_, _, mw := setupTestTitip(t, WithServerTimingCookie(cookieName, cookieVal))

	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(headerCacheControl, "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("cookie test"))
	})
	handler := mw.testHandler(origin)

	// A. No cookie
	reqA := httptest.NewRequest(http.MethodGet, "http://example.com/cookie-test", nil)
	recA := httptest.NewRecorder()
	handler.ServeHTTP(recA, reqA)
	if val := recA.Header().Get("Server-Timing"); val != "" {
		t.Fatalf("expected no timing header when cookie absent, got: %q", val)
	}

	// B. Wrong cookie value
	reqB := httptest.NewRequest(http.MethodGet, "http://example.com/cookie-test", nil)
	reqB.AddCookie(&http.Cookie{Name: cookieName, Value: "wrong_val"})
	recB := httptest.NewRecorder()
	handler.ServeHTTP(recB, reqB)
	if val := recB.Header().Get("Server-Timing"); val != "" {
		t.Fatalf("expected no timing header when cookie value mismatched, got: %q", val)
	}

	// C. Correct cookie name and value
	reqC := httptest.NewRequest(http.MethodGet, "http://example.com/cookie-test", nil)
	reqC.AddCookie(&http.Cookie{Name: cookieName, Value: cookieVal})
	recC := httptest.NewRecorder()
	handler.ServeHTTP(recC, reqC)
	timingC := recC.Header().Get("Server-Timing")
	if timingC == "" {
		t.Fatal("expected timing header when cookie matches, got empty")
	}
	if !strings.Contains(timingC, "titip;dur=") {
		t.Errorf("expected titip;dur= in header, got: %q", timingC)
	}
}

func TestServerTiming_Conditional304(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t, WithServerTiming(true))

	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(headerETag, `"etag-123"`)
		w.Header().Set(headerCacheControl, "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("etag response"))
	})
	handler := mw.testHandler(origin)

	// Warm cache
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/304-timing", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	// Conditional request with matching ETag
	req2 := httptest.NewRequest(http.MethodGet, "http://example.com/304-timing", nil)
	req2.Header.Set(headerIfNoneMatch, `"etag-123"`)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusNotModified {
		t.Fatalf("expected 304 Not Modified, got: %d", rec2.Code)
	}
	timing := rec2.Header().Get("Server-Timing")
	if timing == "" {
		t.Fatal("expected Server-Timing header on 304 response, got empty")
	}
	if !strings.Contains(timing, "titip-meta;dur=") {
		t.Errorf("expected titip-meta;dur= on 304, got: %q", timing)
	}
	if !strings.Contains(timing, "titip;dur=") {
		t.Errorf("expected titip;dur= on 304, got: %q", timing)
	}
}

func TestServerTiming_ESI(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/fragment", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("<span>fragment</span>"))
	})
	mux.HandleFunc("/esi-timing", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(headerContentType, "text/html")
		w.Header().Set(headerSurrogateControl, `content="ESI/1.0"`)
		w.Header().Set(headerCacheControl, "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<div><esi:include src="http://example.com/fragment" /></div>`))
	})

	_, _, mw := setupTestTitip(t,
		WithServerTiming(true),
		WithESI(esi.WithInternalFetcher(esi.HandlerFetcher(mux))),
	)

	handler := mw.testHandler(mux)

	req := httptest.NewRequest(http.MethodGet, "http://example.com/esi-timing", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	timing := rec.Header().Get("Server-Timing")
	if timing == "" {
		t.Fatal("expected Server-Timing header on ESI response, got empty")
	}
	if !strings.Contains(timing, "titip-esi;dur=") {
		t.Errorf("expected titip-esi;dur= in header, got: %q", timing)
	}
	if !strings.Contains(timing, `desc="1 fragments"`) {
		t.Errorf("expected desc=\"1 fragments\" in header, got: %q", timing)
	}
}

func TestServerTiming_ConcurrencyRace(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t, WithServerTiming(true))

	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(headerCacheControl, "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("parallel content"))
	})
	handler := mw.testHandler(origin)

	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			req := httptest.NewRequest(http.MethodGet, "http://example.com/concurrent-timing", nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			timing := rec.Header().Get("Server-Timing")
			if timing == "" {
				t.Errorf("expected Server-Timing header under concurrency")
			}
		})
	}
	wg.Wait()
}

func TestServerTiming_PreserveUpstreamHeader(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t, WithServerTiming(true))

	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Server-Timing", `app;dur=15.2, db;dur=4.1`)
		w.Header().Set(headerCacheControl, "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("preserved"))
	})
	handler := mw.testHandler(origin)

	// Cold Miss: both upstream and titip headers should exist
	req := httptest.NewRequest(http.MethodGet, "http://example.com/preserve-upstream", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	timings := rec.Header().Values("Server-Timing")
	if len(timings) < 2 {
		t.Fatalf("expected at least 2 Server-Timing header values, got: %v", timings)
	}

	joined := strings.Join(timings, ", ")
	if !strings.Contains(joined, "app;dur=15.2") || !strings.Contains(joined, "db;dur=4.1") {
		t.Errorf("expected upstream metrics in Server-Timing, got: %s", joined)
	}
	if !strings.Contains(joined, "titip-status;desc=\"MISS\"") {
		t.Errorf("expected titip metrics in Server-Timing, got: %s", joined)
	}
}

func BenchmarkServerTiming_Active_CacheHit(b *testing.B) {
	_, _, mw := setupTestTitip(b, WithServerTiming(true))

	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(headerContentType, "application/json")
		w.Header().Set(headerCacheControl, "public, max-age=300")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	handler := mw.testHandler(origin)
	req := httptest.NewRequest(http.MethodGet, "http://example.com/bench/timing-active", nil)

	// Prime cache
	recPrime := httptest.NewRecorder()
	handler.ServeHTTP(recPrime, req)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		rec := getResponseRecorder()
		handler.ServeHTTP(rec, req)
		putResponseRecorder(rec)
	}
}
