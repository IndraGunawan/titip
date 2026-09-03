package titip

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/indragunawan/titip/esi"
)

func TestESI_InProcessVirtualSubrequests(t *testing.T) {
	var cartHits, userHits, dashHits atomic.Int32

	mux := http.NewServeMux()

	// Parent page
	mux.HandleFunc("/dashboard", func(w http.ResponseWriter, r *http.Request) {
		dashHits.Add(1)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Header().Set("ETag", `"dash-v1"`)
		w.Header().Set("Surrogate-Control", "content=\"ESI/1.0\"")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<!DOCTYPE html><html><body><h1>Dashboard</h1><div id="cart"><esi:include src="/api/cart" /></div><div id="user"><esi:include src="/api/user" /></div></body></html>`))
	})

	// Fragment 1: Cart (short TTL, sets session cookie)
	mux.HandleFunc("/api/cart", func(w http.ResponseWriter, r *http.Request) {
		cartHits.Add(1)
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=10")
		w.Header().Set("Set-Cookie", "cart_session=xyz123; Path=/")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<span>3 Items ($45.00)</span>`))
	})

	// Fragment 2: User
	mux.HandleFunc("/api/user", func(w http.ResponseWriter, r *http.Request) {
		userHits.Add(1)
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<span>Hello, Alice!</span>`))
	})

	_, _, mw := setupTestTitip(t,
		WithESI(
			esi.WithInternalFetcher(esi.HandlerFetcher(mux)),
			esi.WithMaxTimeout(5*time.Second),
		),
	)

	handler := mw.testHandler(mux)

	// 1. First Request: Cold Miss on Dashboard, invokes in-process fragments
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/dashboard", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Fatalf("expected status 200 OK, got %d", rec1.Code)
	}

	body1 := rec1.Body.String()
	if !strings.Contains(body1, `<span>3 Items ($45.00)</span>`) {
		t.Errorf("expected spliced cart fragment, got: %s", body1)
	}
	if !strings.Contains(body1, `<span>Hello, Alice!</span>`) {
		t.Errorf("expected spliced user fragment, got: %s", body1)
	}
	if strings.Contains(body1, "<esi:include") {
		t.Errorf("body still contains raw esi:include tag: %s", body1)
	}

	// Verify Set-Cookie header forwarded to downstream client
	if cookie := rec1.Header().Get("Set-Cookie"); !strings.Contains(cookie, "cart_session=xyz123") {
		t.Errorf("expected Set-Cookie forwarded from subrequest, got: %s", cookie)
	}

	// Verify ETag was weakened
	if etag := rec1.Header().Get("ETag"); etag != `W/"dash-v1"` {
		t.Errorf("expected weak ETag W/\"dash-v1\", got: %s", etag)
	}

	// Verify Surrogate-Control was stripped
	if surr := rec1.Header().Get("Surrogate-Control"); surr != "" {
		t.Errorf("expected Surrogate-Control to be stripped, got: %s", surr)
	}

	if dashHits.Load() != 1 || cartHits.Load() != 1 || userHits.Load() != 1 {
		t.Errorf("unexpected hit counts: dash=%d, cart=%d, user=%d", dashHits.Load(), cartHits.Load(), userHits.Load())
	}

	// 2. Second Request: Cache Hit on Dashboard (Zero HTML re-scanning, uses pre-compiled metadata)
	req2 := httptest.NewRequest(http.MethodGet, "http://example.com/dashboard", nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("expected status 200 OK on hit, got %d", rec2.Code)
	}

	body2 := rec2.Body.String()
	if !strings.Contains(body2, `<span>3 Items ($45.00)</span>`) || !strings.Contains(body2, `<span>Hello, Alice!</span>`) {
		t.Errorf("cache hit fragment splicing mismatch: %s", body2)
	}

	// Dashboard origin should NOT be re-executed (served from Redis metadata)
	if dashHits.Load() != 1 {
		t.Errorf("dashboard origin re-executed on cache hit: dash=%d", dashHits.Load())
	}
}

func TestESI_SamePageDeduplication(t *testing.T) {
	var navHits atomic.Int32

	mux := http.NewServeMux()
	mux.HandleFunc("/page", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<header><esi:include src="/api/nav" /></header><sidebar><esi:include src="/api/nav" /></sidebar><footer><esi:include src="/api/nav" /></footer>`))
	})

	mux.HandleFunc("/api/nav", func(w http.ResponseWriter, r *http.Request) {
		navHits.Add(1)
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<nav>Navigation Links</nav>`))
	})

	_, _, mw := setupTestTitip(t,
		WithESI(
			esi.WithInternalFetcher(esi.HandlerFetcher(mux)),
		),
	)

	handler := mw.testHandler(mux)
	req := httptest.NewRequest(http.MethodGet, "http://example.com/page", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	expectedCount := 3
	if count := strings.Count(body, `<nav>Navigation Links</nav>`); count != expectedCount {
		t.Errorf("expected %d nav elements in body, got %d. Body: %s", expectedCount, count, body)
	}

	// Same-page deduplication ensures /api/nav origin is fetched once per page render
	if navHits.Load() != 1 {
		t.Errorf("expected exactly 1 nav fetch due to deduplication, got %d", navHits.Load())
	}
}

func TestESI_SSRFProtection(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/ssrf-test", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<div><esi:include src="http://169.254.169.254/latest/meta-data/" onerror="continue"><span>Cloud Metadata Blocked</span></esi:include><esi:include src="file:///etc/passwd" onerror="continue"><span>File Scheme Blocked</span></esi:include></div>`))
	})

	_, _, mw := setupTestTitip(t,
		WithESI(
			esi.WithInternalFetcher(esi.HandlerFetcher(mux)),
		),
	)

	handler := mw.testHandler(mux)
	req := httptest.NewRequest(http.MethodGet, "http://example.com/ssrf-test", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected parent page 200 OK despite SSRF block, got %d", rec.Code)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "Cloud Metadata Blocked") {
		t.Errorf("expected SSRF block fallback for cloud metadata IP, got: %s", body)
	}
	if !strings.Contains(body, "File Scheme Blocked") {
		t.Errorf("expected scheme validation fallback for file:///, got: %s", body)
	}
}

func TestESI_FallbackOnErrorAndAlt(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/fail-test", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<div><esi:include src="/api/broken1" alt="/api/backup" /> | <esi:include src="/api/broken2"><span>Inline Fallback</span></esi:include> | <esi:include src="/api/broken3" onerror="continue"><span>Omit Me</span></esi:include></div>`))
	})

	mux.HandleFunc("/api/broken1", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server error", http.StatusInternalServerError)
	})

	mux.HandleFunc("/api/broken2", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server error", http.StatusInternalServerError)
	})

	mux.HandleFunc("/api/broken3", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server error", http.StatusInternalServerError)
	})

	mux.HandleFunc("/api/backup", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<span>Backup Success</span>`))
	})

	_, _, mw := setupTestTitip(t,
		WithESI(
			esi.WithInternalFetcher(esi.HandlerFetcher(mux)),
		),
	)

	handler := mw.testHandler(mux)
	req := httptest.NewRequest(http.MethodGet, "http://example.com/fail-test", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "Backup Success") {
		t.Errorf("expected alt backup success, got: %s", body)
	}
	if !strings.Contains(body, "Inline Fallback") {
		t.Errorf("expected inline fallback, got: %s", body)
	}
	if strings.Contains(body, "/api/broken3") {
		t.Errorf("expected onerror=continue to omit widget cleanly, got: %s", body)
	}
}

func TestESI_MaxDepthAndCircularLoop(t *testing.T) {
	mux := http.NewServeMux()

	mux.HandleFunc("/page-a", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<div>Page A <esi:include src="/page-b" onerror="continue"><span>Loop Caught</span></esi:include></div>`))
	})

	mux.HandleFunc("/page-b", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<div>Page B <esi:include src="/page-a" onerror="continue"><span>Loop Caught</span></esi:include></div>`))
	})

	_, _, mw := setupTestTitip(t,
		WithESI(
			esi.WithInternalFetcher(esi.HandlerFetcher(mux)),
			esi.WithMaxDepth(3),
		),
	)

	handler := mw.testHandler(mux)
	req := httptest.NewRequest(http.MethodGet, "http://example.com/page-a", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK despite circular loop, got %d", rec.Code)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "Loop Caught") {
		t.Errorf("expected circular loop to be caught and render fallback, got: %s", body)
	}
}

func TestESI_WorkerPanicRecovery(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/panic-page", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<div><esi:include src="/panic-fragment"><span>Panic Fallback</span></esi:include></div>`))
	})

	mux.HandleFunc("/panic-fragment", func(w http.ResponseWriter, r *http.Request) {
		panic("simulated fatal panic in fragment handler")
	})

	_, _, mw := setupTestTitip(t,
		WithESI(
			esi.WithInternalFetcher(esi.HandlerFetcher(mux)),
		),
	)

	handler := mw.testHandler(mux)
	req := httptest.NewRequest(http.MethodGet, "http://example.com/panic-page", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on worker panic recovery, got %d", rec.Code)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "Panic Fallback") {
		t.Errorf("expected panic fallback to render, got: %s", body)
	}
}

func TestESI_FullDocument_ColdMissAndCacheHitIdentical(t *testing.T) {
	var pageCalls, headerCalls, userCalls, footerCalls atomic.Int32

	mux := http.NewServeMux()

	rawTemplate := `<!DOCTYPE html><html><head><title>Full ESI Page</title></head><body>` +
		`<header><esi:include src="/api/header" /></header>` +
		`<main>` +
		`<div><esi:include src="/api/user">Default User</esi:include></div>` +
		`<esi:remove><p>Client JS Fallback</p></esi:remove>` +
		`<esi:comment text="Internal Analytics Comment" />` +
		`<p>Main content paragraph</p>` +
		`</main>` +
		`<!--esi <footer><esi:include src="/api/footer" /></footer> -->` +
		`</body></html>`

	expectedHTML := `<!DOCTYPE html><html><head><title>Full ESI Page</title></head><body>` +
		`<header><nav>Header Menu</nav></header>` +
		`<main>` +
		`<div><span>User Profile: Bob</span></div>` +
		`<p>Main content paragraph</p>` +
		`</main>` +
		` <footer><span>Site Footer &copy; 2026</span></footer> ` +
		`</body></html>`

	mux.HandleFunc("/full-page", func(w http.ResponseWriter, r *http.Request) {
		pageCalls.Add(1)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(rawTemplate))
	})

	mux.HandleFunc("/api/header", func(w http.ResponseWriter, r *http.Request) {
		headerCalls.Add(1)
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<nav>Header Menu</nav>`))
	})

	mux.HandleFunc("/api/user", func(w http.ResponseWriter, r *http.Request) {
		userCalls.Add(1)
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<span>User Profile: Bob</span>`))
	})

	mux.HandleFunc("/api/footer", func(w http.ResponseWriter, r *http.Request) {
		footerCalls.Add(1)
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<span>Site Footer &copy; 2026</span>`))
	})

	_, _, mw := setupTestTitip(t,
		WithESI(
			esi.WithInternalFetcher(esi.HandlerFetcher(mux)),
		),
	)

	handler := mw.testHandler(mux)

	// 1. Cold Request (Miss): Scans HTML once, stores pre-compiled metadata in Redis
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/full-page", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Fatalf("cold request failed with code %d", rec1.Code)
	}

	coldBody := rec1.Body.String()
	if coldBody != expectedHTML {
		t.Fatalf("cold miss body mismatch:\nGot:      %q\nExpected: %q", coldBody, expectedHTML)
	}
	if pageCalls.Load() != 1 || headerCalls.Load() != 1 || userCalls.Load() != 1 || footerCalls.Load() != 1 {
		t.Fatalf("unexpected origin calls on cold miss: page=%d, header=%d, user=%d, footer=%d",
			pageCalls.Load(), headerCalls.Load(), userCalls.Load(), footerCalls.Load())
	}

	// 2. Cache Hit Request: Uses pre-compiled metadata from Redis (Zero HTML re-scanning)
	req2 := httptest.NewRequest(http.MethodGet, "http://example.com/full-page", nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("cache hit request failed with code %d", rec2.Code)
	}

	hitBody := rec2.Body.String()
	// Byte-for-byte exact equality between Cold Miss and Cache Hit!
	if hitBody != expectedHTML {
		t.Fatalf("cache hit body mismatch:\nGot:      %q\nExpected: %q", hitBody, expectedHTML)
	}
	if hitBody != coldBody {
		t.Fatalf("cache hit body differs from cold miss body:\nHit:  %q\nCold: %q", hitBody, coldBody)
	}

	// Parent page origin must NOT be called again
	if pageCalls.Load() != 1 {
		t.Fatalf("parent page origin was re-executed on cache hit: pageCalls=%d", pageCalls.Load())
	}
}

// TestESI_ConcurrentFetchDuration_MaxOfFragments verifies that multiple ESI includes on the same page
// execute concurrently in parallel rather than sequentially (total time ~ max(frag1, frag2, frag3) << sum(frags)).
func TestESI_ConcurrentFetchDuration_MaxOfFragments(t *testing.T) {
	mux := http.NewServeMux()

	mux.HandleFunc("/slow-page", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<div>[<esi:include src="/api/slow1" />][<esi:include src="/api/slow2" />][<esi:include src="/api/slow3" />]</div>`))
	})

	fragmentDuration := 80 * time.Millisecond

	mux.HandleFunc("/api/slow1", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(fragmentDuration)
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`F1`))
	})

	mux.HandleFunc("/api/slow2", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(fragmentDuration)
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`F2`))
	})

	mux.HandleFunc("/api/slow3", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(fragmentDuration)
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`F3`))
	})

	_, _, mw := setupTestTitip(t,
		WithESI(
			esi.WithInternalFetcher(esi.HandlerFetcher(mux)),
		),
	)

	handler := mw.testHandler(mux)

	start := time.Now()
	req := httptest.NewRequest(http.MethodGet, "http://example.com/slow-page", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	if body := rec.Body.String(); body != `<div>[F1][F2][F3]</div>` {
		t.Fatalf("unexpected spliced body: %s", body)
	}

	// Sequential execution would take >= 240ms (3 * 80ms)
	// Concurrent execution takes ~80ms (max of the 3 fragments). We assert < 200ms with margin.
	maxAllowedDuration := 200 * time.Millisecond
	if elapsed >= maxAllowedDuration {
		t.Fatalf("expected parallel execution (< %s), but took %s (indicating sequential fetching)", maxAllowedDuration, elapsed)
	}
}

// TestESI_LargePayloadSplicing_PositionOffsetsExact verifies that when fragment bodies are significantly
// larger than the tag definitions (e.g. 500-1200 bytes replacing a 30-byte tag), the pre-compiled start/end
// offsets in the parent HTML do not suffer offset drift, and all intermediate static sections remain in exact positions.
func TestESI_LargePayloadSplicing_PositionOffsetsExact(t *testing.T) {
	mux := http.NewServeMux()

	rawTemplate := `<div id="app">` +
		`<section id="s1"><esi:include src="/api/large1" /></section>` +
		`<div class="divider">--- SECTION 1 DIVIDER ---</div>` +
		`<section id="s2"><esi:include src="/api/large2" /></section>` +
		`<div class="divider">--- SECTION 2 DIVIDER ---</div>` +
		`<section id="s3"><esi:include src="/api/large3" /></section>` +
		`<footer>--- FOOTER END ---</footer>` +
		`</div>`

	// Large fragment 1: 500 bytes (replacing 30-byte tag)
	payload1 := strings.Repeat("A1_CHUNK_", 55) // 495 bytes
	mux.HandleFunc("/api/large1", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(payload1))
	})

	// Large fragment 2: 1200 bytes (replacing 30-byte tag)
	payload2 := strings.Repeat("B2_PAYLOAD_", 110) // 1210 bytes
	mux.HandleFunc("/api/large2", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(payload2))
	})

	// Large fragment 3: 800 bytes (replacing 30-byte tag)
	payload3 := strings.Repeat("C3_DATA_", 100) // 800 bytes
	mux.HandleFunc("/api/large3", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(payload3))
	})

	expectedOutput := `<div id="app">` +
		`<section id="s1">` + payload1 + `</section>` +
		`<div class="divider">--- SECTION 1 DIVIDER ---</div>` +
		`<section id="s2">` + payload2 + `</section>` +
		`<div class="divider">--- SECTION 2 DIVIDER ---</div>` +
		`<section id="s3">` + payload3 + `</section>` +
		`<footer>--- FOOTER END ---</footer>` +
		`</div>`

	mux.HandleFunc("/large-page", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(rawTemplate))
	})

	_, _, mw := setupTestTitip(t,
		WithESI(
			esi.WithInternalFetcher(esi.HandlerFetcher(mux)),
		),
	)

	handler := mw.testHandler(mux)

	// 1. Cold Request (Miss)
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/large-page", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Fatalf("cold request failed: %d", rec1.Code)
	}

	coldBody := rec1.Body.String()
	if coldBody != expectedOutput {
		t.Fatalf("cold miss body mismatch:\nGot len:      %d\nExpected len: %d", len(coldBody), len(expectedOutput))
	}

	// 2. Cache Hit Request (Uses pre-compiled metadata from Redis)
	req2 := httptest.NewRequest(http.MethodGet, "http://example.com/large-page", nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("cache hit request failed: %d", rec2.Code)
	}

	hitBody := rec2.Body.String()
	if hitBody != expectedOutput {
		t.Fatalf("cache hit body mismatch:\nGot len:      %d\nExpected len: %d", len(hitBody), len(expectedOutput))
	}
	if hitBody != coldBody {
		t.Fatalf("cache hit differed from cold miss")
	}

	// Verify all dividers and sections are in exact expected order
	idx1 := strings.Index(hitBody, "--- SECTION 1 DIVIDER ---")
	idx2 := strings.Index(hitBody, "--- SECTION 2 DIVIDER ---")
	idx3 := strings.Index(hitBody, "--- FOOTER END ---")
	if idx1 == -1 || idx2 == -1 || idx3 == -1 || idx1 >= idx2 || idx2 >= idx3 {
		t.Fatalf("intermediate static sections not in expected sequence: idx1=%d, idx2=%d, idx3=%d", idx1, idx2, idx3)
	}
}

// TestESI_ContentLengthHeaderPreservation verifies that:
// 1. If origin provides Content-Length, it is updated to match the spliced body length.
// 2. If origin omits Content-Length, Titip does NOT inject a Content-Length header.
func TestESI_ContentLengthHeaderPreservation(t *testing.T) {
	mux := http.NewServeMux()

	// Case 1: Origin with explicit Content-Length
	mux.HandleFunc("/with-cl", func(w http.ResponseWriter, r *http.Request) {
		raw := []byte(`<div><esi:include src="/frag-cl" /></div>`)
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Content-Length", strconv.Itoa(len(raw)))
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(raw)
	})

	// Case 2: Origin without Content-Length (streaming / omitted)
	mux.HandleFunc("/without-cl", func(w http.ResponseWriter, r *http.Request) {
		raw := []byte(`<div><esi:include src="/frag-cl" /></div>`)
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(raw)
	})

	mux.HandleFunc("/frag-cl", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<span>Dynamic Fragment Content</span>`))
	})

	_, _, mw := setupTestTitip(t,
		WithESI(
			esi.WithInternalFetcher(esi.HandlerFetcher(mux)),
		),
	)
	handler := mw.testHandler(mux)

	// 1. Test with explicit Content-Length: must be updated to exact spliced body length
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/with-cl", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	expectedBody := `<div><span>Dynamic Fragment Content</span></div>`
	if rec1.Body.String() != expectedBody {
		t.Fatalf("unexpected body: %s", rec1.Body.String())
	}
	expectedCL := strconv.Itoa(len(expectedBody))
	if cl := rec1.Header().Get("Content-Length"); cl != expectedCL {
		t.Fatalf("expected updated Content-Length %s, got: %s", expectedCL, cl)
	}

	// 2. Test without Content-Length: must NOT have Content-Length added
	req2 := httptest.NewRequest(http.MethodGet, "http://example.com/without-cl", nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Body.String() != expectedBody {
		t.Fatalf("unexpected body: %s", rec2.Body.String())
	}
	if cl := rec2.Header().Get("Content-Length"); cl != "" {
		t.Fatalf("expected Content-Length to remain omitted, but got: %s", cl)
	}
}

// TestESI_RootIndex_AllAttributeCombinations tests the root index ("/") containing every ESI tag
// and attribute combination: src, alt, timeout, max-depth, onerror=continue, inline fallbacks,
// esi:remove, esi:comment, and <!--esi ... --> inline unescaping on both Cold Miss and Cache Hit.
func TestESI_RootIndex_AllAttributeCombinations(t *testing.T) {
	var rootCalls, navCalls, altCalls, footerCalls atomic.Int32

	mux := http.NewServeMux()

	rootTemplate := `<!DOCTYPE html><html><head><title>Root Index</title></head><body>` +
		`<div id="nav"><esi:include src="/frag/nav" /></div>` +
		`<div id="alt-test"><esi:include src="/frag/primary-fail" alt="/frag/alt-success" /></div>` +
		`<div id="timeout-test"><esi:include src="/frag/slow" timeout="30ms"><span>Timeout Fallback Body</span></esi:include></div>` +
		`<div id="depth-test"><esi:include src="/frag/depth1" max-depth="1" /></div>` +
		`<div id="missing-continue"><esi:include src="/frag/nonexistent" onerror="continue" /></div>` +
		`<div id="error-fallback"><esi:include src="/frag/server-error" onerror="continue"><span>Error Fallback Body</span></esi:include></div>` +
		`<esi:remove><p>Raw Client Fallback (Stripped)</p></esi:remove>` +
		`<esi:comment text="Root Index Render Checkpoint" />` +
		`<!--esi <div id="footer"><esi:include src="/frag/footer" /></div> -->` +
		`</body></html>`

	expectedResult := `<!DOCTYPE html><html><head><title>Root Index</title></head><body>` +
		`<div id="nav"><nav>Root Navigation Menu</nav></div>` +
		`<div id="alt-test"><span>Alt Backup Succeeded</span></div>` +
		`<div id="timeout-test"><span>Timeout Fallback Body</span></div>` +
		`<div id="depth-test"><div>Depth 1 <span>Depth Max Reached Fallback</span></div></div>` +
		`<div id="missing-continue"></div>` +
		`<div id="error-fallback"><span>Error Fallback Body</span></div>` +
		` <div id="footer"><footer>Root Index Footer 2026</footer></div> ` +
		`</body></html>`

	// Root Index Route
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		rootCalls.Add(1)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(rootTemplate))
	})

	// 1. Normal Nav Fragment
	mux.HandleFunc("/frag/nav", func(w http.ResponseWriter, r *http.Request) {
		navCalls.Add(1)
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<nav>Root Navigation Menu</nav>`))
	})

	// 2. Failing Primary Fragment (500 Internal Server Error)
	mux.HandleFunc("/frag/primary-fail", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "primary database unavailable", http.StatusInternalServerError)
	})

	// 2b. Alt Backup Fragment
	mux.HandleFunc("/frag/alt-success", func(w http.ResponseWriter, r *http.Request) {
		altCalls.Add(1)
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<span>Alt Backup Succeeded</span>`))
	})

	// 3. Slow Fragment (exceeds timeout="30ms")
	mux.HandleFunc("/frag/slow", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(120 * time.Millisecond)
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<span>Slow Response Completed</span>`))
	})

	// 4. Nested Depth Fragment 1
	mux.HandleFunc("/frag/depth1", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<div>Depth 1 <esi:include src="/frag/depth2" onerror="continue"><span>Depth Max Reached Fallback</span></esi:include></div>`))
	})

	// 4b. Nested Depth Fragment 2 (should exceed max-depth=1)
	mux.HandleFunc("/frag/depth2", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<span>Depth 2 Unreachable</span>`))
	})

	// 6. Server Error Fragment with onerror="continue"
	mux.HandleFunc("/frag/server-error", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "downstream error", http.StatusBadGateway)
	})

	// 9. Footer Fragment
	mux.HandleFunc("/frag/footer", func(w http.ResponseWriter, r *http.Request) {
		footerCalls.Add(1)
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<footer>Root Index Footer 2026</footer>`))
	})

	_, _, mw := setupTestTitip(t,
		WithESI(
			esi.WithInternalFetcher(esi.HandlerFetcher(mux)),
			esi.WithMaxTimeout(5*time.Second),
		),
	)

	handler := mw.testHandler(mux)

	// 1. Cold Request (Miss)
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on cold miss, got %d", rec1.Code)
	}

	coldBody := rec1.Body.String()
	if coldBody != expectedResult {
		t.Fatalf("cold miss body mismatch:\nGot:      %q\nExpected: %q", coldBody, expectedResult)
	}

	if rootCalls.Load() != 1 || navCalls.Load() != 1 || altCalls.Load() != 1 || footerCalls.Load() != 1 {
		t.Fatalf("unexpected call counts on cold miss: root=%d, nav=%d, alt=%d, footer=%d",
			rootCalls.Load(), navCalls.Load(), altCalls.Load(), footerCalls.Load())
	}

	// 2. Cache Hit Request (Uses pre-compiled metadata from Redis)
	req2 := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on cache hit, got %d", rec2.Code)
	}

	hitBody := rec2.Body.String()
	if hitBody != expectedResult {
		t.Fatalf("cache hit body mismatch:\nGot:      %q\nExpected: %q", hitBody, expectedResult)
	}

	if hitBody != coldBody {
		t.Fatalf("cache hit body differed from cold miss body")
	}

	// Root origin handler must not be re-executed
	if rootCalls.Load() != 1 {
		t.Fatalf("root origin was re-executed on cache hit: rootCalls=%d", rootCalls.Load())
	}
}

func TestESI_SameHostAndExternalDomainIncludes(t *testing.T) {
	// External HTTP server (represents a 3rd party CDN or partner domain)
	externalSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<div>Partner Widget</div>`))
	}))
	defer externalSrv.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/same-host-test", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		// Template containing:
		// 1. Relative include: src="/frag/relative"
		// 2. Absolute URL with same host: src="http://example.com/frag/same-host"
		// 3. External domain URL: src="http://127.0.0.1:PORT/widget"
		_, _ = fmt.Fprintf(w, `<main><esi:include src="/frag/relative" /><esi:include src="http://example.com/frag/same-host" /><esi:include src="%s/widget" /></main>`, externalSrv.URL)
	})

	mux.HandleFunc("/frag/relative", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<span>Relative Nav</span>`))
	})

	mux.HandleFunc("/frag/same-host", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<span>Same-Host Component</span>`))
	})

	_, _, mw := setupTestTitip(t,
		WithESI(
			esi.WithInternalFetcher(esi.HandlerFetcher(mux)),
			esi.WithAllowPrivateIPs(true), // allow 127.0.0.1 httptest server
		),
	)

	h := mw.testHandler(mux)

	req := httptest.NewRequest("GET", "http://example.com/same-host-test", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	expected := `<main><span>Relative Nav</span><span>Same-Host Component</span><div>Partner Widget</div></main>`
	got := rec.Body.String()
	if got != expected {
		t.Fatalf("spliced body mismatch:\nGot:      %q\nExpected: %q", got, expected)
	}
}

func BenchmarkESI_ColdMiss(b *testing.B) {
	mux := http.NewServeMux()
	mux.HandleFunc("/bench-esi-cold", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "no-store") // always cold
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<div><header><esi:include src="/frag-h" /></header><main><esi:include src="/frag-u" /></main></div>`))
	})
	mux.HandleFunc("/frag-h", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<nav>Navigation Menu</nav>`))
	})
	mux.HandleFunc("/frag-u", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<span>User: Alice</span>`))
	})

	_, _, mw := setupTestTitip(b,
		WithESI(
			esi.WithInternalFetcher(esi.HandlerFetcher(mux)),
		),
	)
	handler := mw.testHandler(mux)

	req := httptest.NewRequest(http.MethodGet, "http://example.com/bench-esi-cold", nil)

	for b.Loop() {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}
}

func BenchmarkESI_CacheHit(b *testing.B) {
	mux := http.NewServeMux()
	mux.HandleFunc("/bench-esi-hit", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=3600") // cached parent
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<div><header><esi:include src="/frag-h2" /></header><main><esi:include src="/frag-u2" /></main></div>`))
	})
	mux.HandleFunc("/frag-h2", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<nav>Navigation Menu</nav>`))
	})
	mux.HandleFunc("/frag-u2", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<span>User: Alice</span>`))
	})

	_, _, mw := setupTestTitip(b,
		WithESI(
			esi.WithInternalFetcher(esi.HandlerFetcher(mux)),
		),
	)
	handler := mw.testHandler(mux)

	// Warm up cache once (Cold Miss compiles template & stores pre-compiled metadata in Redis)
	warmReq := httptest.NewRequest(http.MethodGet, "http://example.com/bench-esi-hit", nil)
	warmRec := httptest.NewRecorder()
	handler.ServeHTTP(warmRec, warmReq)

	req := httptest.NewRequest(http.MethodGet, "http://example.com/bench-esi-hit", nil)

	for b.Loop() {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}
}

func TestESI_CustomInternalFetcher(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/page", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<div><esi:include src="/custom-hook" /></div>`))
	})

	customFetcher := func(ctx context.Context, targetPath string, r *http.Request) ([]byte, http.Header, error) {
		if targetPath == "/custom-hook" {
			h := make(http.Header)
			h.Set("Set-Cookie", "custom_cookie=val123; Path=/")
			return []byte(`<span>Rendered by Custom Fetcher Hook</span>`), h, nil
		}
		return nil, nil, errors.New("not found")
	}

	_, _, mw := setupTestTitip(t,
		WithESI(
			esi.WithInternalFetcher(customFetcher),
		),
	)

	handler := mw.testHandler(mux)

	req := httptest.NewRequest(http.MethodGet, "http://example.com/page", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	expected := `<div><span>Rendered by Custom Fetcher Hook</span></div>`
	if rec.Body.String() != expected {
		t.Fatalf("expected %q, got %q", expected, rec.Body.String())
	}

	cookies := rec.Result().Cookies()
	var foundCustom bool
	for _, c := range cookies {
		if c.Name == "custom_cookie" && c.Value == "val123" {
			foundCustom = true
		}
	}
	if !foundCustom {
		t.Errorf("expected Set-Cookie custom_cookie=val123 from custom fetcher")
	}
}

func TestESI_InternalFetcher_FallbackToOutboundHTTP_On404(t *testing.T) {
	var outboundHits atomic.Int32

	// Outbound HTTP server simulating external service routed by ALB
	extServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/esi-parts" {
			outboundHits.Add(1)
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<span>From External ALB Service</span>`))
			return
		}
		http.NotFound(w, r)
	}))
	defer extServer.Close()

	// Local in-process router (Service A)
	mux := http.NewServeMux()
	mux.HandleFunc("/dashboard", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<div><esi:include src="` + extServer.URL + `/esi-parts" /></div>`))
	})

	// Internal fetcher handles /local but returns esi.ErrFallbackToHTTP on 404
	localFetcher := func(ctx context.Context, targetPath string, r *http.Request) ([]byte, http.Header, error) {
		if targetPath == "/local" {
			return []byte(`<span>Local</span>`), nil, nil
		}
		// 404 in-process -> delegate to outbound HTTP
		return nil, nil, esi.ErrFallbackToHTTP
	}

	_, _, mw := setupTestTitip(t,
		WithESI(
			esi.WithInternalFetcher(localFetcher),
			esi.WithAllowPrivateIPs(true), // allow loopback httptest.Server
			esi.WithMaxTimeout(5*time.Second),
		),
	)

	handler := mw.testHandler(mux)

	req := httptest.NewRequest(http.MethodGet, "http://example.com/dashboard", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	expected := `<div><span>From External ALB Service</span></div>`
	if rec.Body.String() != expected {
		t.Fatalf("expected %q, got %q", expected, rec.Body.String())
	}

	if outboundHits.Load() != 1 {
		t.Errorf("expected 1 outbound HTTP call, got %d", outboundHits.Load())
	}
}

func TestESI_InternalFetcher_Non404Error_DoesNotFallback(t *testing.T) {
	var outboundHits atomic.Int32

	extServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		outboundHits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<span>Outbound</span>`))
	}))
	defer extServer.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/dashboard", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<div><esi:include src="/error-part" onerror="continue" /></div>`))
	})

	// Internal fetcher returns a 500-type error (not esi.ErrFallbackToHTTP)
	errFetcher := func(ctx context.Context, targetPath string, r *http.Request) ([]byte, http.Header, error) {
		return nil, nil, errors.New("subrequest returned status 500")
	}

	_, _, mw := setupTestTitip(t,
		WithESI(
			esi.WithInternalFetcher(errFetcher),
			esi.WithAllowPrivateIPs(true),
			esi.WithMaxTimeout(5*time.Second),
		),
	)

	handler := mw.testHandler(mux)

	extURL := strings.TrimPrefix(extServer.URL, "http://")
	req := httptest.NewRequest(http.MethodGet, "http://"+extURL+"/dashboard", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	expected := `<div></div>`
	if rec.Body.String() != expected {
		t.Fatalf("expected %q, got %q", expected, rec.Body.String())
	}

	if outboundHits.Load() != 0 {
		t.Errorf("expected 0 outbound HTTP calls on 500 error, got %d", outboundHits.Load())
	}
}

func TestESI_ESIHandlerFetcher_404_FallbackToOutbound(t *testing.T) {
	var outboundHits atomic.Int32

	extServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/remote-part" {
			outboundHits.Add(1)
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<span>Remote Via ALB</span>`))
			return
		}
		http.NotFound(w, r)
	}))
	defer extServer.Close()

	// In-process router only knows /dashboard and /local-part
	mux := http.NewServeMux()
	mux.HandleFunc("/dashboard", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<div><esi:include src="/local-part" /> | <esi:include src="/remote-part" /></div>`))
	})
	mux.HandleFunc("/local-part", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<span>Local Part</span>`))
	})

	// Wrap mux with ESIHandlerFetcher which returns esi.ErrFallbackToHTTP on 404
	_, _, mw := setupTestTitip(t,
		WithESI(
			esi.WithInternalFetcher(esi.HandlerFetcher(mux)),
			esi.WithAllowPrivateIPs(true),
			esi.WithMaxTimeout(5*time.Second),
		),
	)

	handler := mw.testHandler(mux)

	// Note: Host matches extServer host so relative /remote-part resolves outbound to extServer
	extURL := strings.TrimPrefix(extServer.URL, "http://")
	req := httptest.NewRequest(http.MethodGet, "http://"+extURL+"/dashboard", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	expected := `<div><span>Local Part</span> | <span>Remote Via ALB</span></div>`
	if rec.Body.String() != expected {
		t.Fatalf("expected %q, got %q", expected, rec.Body.String())
	}

	if outboundHits.Load() != 1 {
		t.Errorf("expected 1 outbound HTTP call for remote-part, got %d", outboundHits.Load())
	}
}

func TestESI_OutboundHTTP_CredentialForwarding_SameHostVsCrossHost(t *testing.T) {
	var sameHostReceivedCookie, sameHostReceivedAuth atomic.Value
	var crossHostReceivedCookie, crossHostReceivedAuth atomic.Value

	// 1. Same-host external server (simulating ALB routing to another backend service for same domain)
	sameHostMux := http.NewServeMux()
	sameHostMux.HandleFunc("/same-fragment", func(w http.ResponseWriter, r *http.Request) {
		sameHostReceivedCookie.Store(r.Header.Get("Cookie"))
		sameHostReceivedAuth.Store(r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<span>Same Host OK</span>`))
	})
	sameHostServer := httptest.NewServer(sameHostMux)
	defer sameHostServer.Close()

	// 2. Cross-host external server (simulating external 3rd-party provider)
	crossHostMux := http.NewServeMux()
	crossHostMux.HandleFunc("/cross-fragment", func(w http.ResponseWriter, r *http.Request) {
		crossHostReceivedCookie.Store(r.Header.Get("Cookie"))
		crossHostReceivedAuth.Store(r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<span>Cross Host OK</span>`))
	})
	crossHostServer := httptest.NewServer(crossHostMux)
	defer crossHostServer.Close()

	// 3. Parent origin
	parentMux := http.NewServeMux()
	parentMux.HandleFunc("/page", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		tmpl := fmt.Sprintf(`<div><esi:include src="/same-fragment" /> | <esi:include src="%s/cross-fragment" /></div>`, crossHostServer.URL)
		_, _ = w.Write([]byte(tmpl))
	})

	// Wrap parentMux with ESIHandlerFetcher that falls back to HTTP on 404
	_, _, mw := setupTestTitip(t,
		WithESI(
			esi.WithInternalFetcher(esi.HandlerFetcher(parentMux)),
			esi.WithAllowPrivateIPs(true),
			esi.WithMaxTimeout(10*time.Second),
		),
	)

	handler := mw.testHandler(parentMux)

	sameHostAddr := strings.TrimPrefix(sameHostServer.URL, "http://")
	req := httptest.NewRequest(http.MethodGet, "http://"+sameHostAddr+"/page", nil)
	req.Header.Set("Cookie", "session_id=user123_secret; theme=dark")
	req.Header.Set("Authorization", "Bearer sensitive_token_xyz")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	expected := `<div><span>Same Host OK</span> | <span>Cross Host OK</span></div>`
	if rec.Body.String() != expected {
		t.Fatalf("expected %q, got %q", expected, rec.Body.String())
	}

	// Verify Same-Host received Cookie and Authorization
	if cookie, _ := sameHostReceivedCookie.Load().(string); cookie != "session_id=user123_secret; theme=dark" {
		t.Errorf("expected same-host to receive client cookie, got %q", cookie)
	}
	if auth, _ := sameHostReceivedAuth.Load().(string); auth != "Bearer sensitive_token_xyz" {
		t.Errorf("expected same-host to receive client authorization, got %q", auth)
	}

	// Verify Cross-Host did NOT receive Cookie or Authorization
	if cookie, _ := crossHostReceivedCookie.Load().(string); cookie != "" {
		t.Errorf("expected cross-host to NOT receive client cookie, got %q", cookie)
	}
	if auth, _ := crossHostReceivedAuth.Load().(string); auth != "" {
		t.Errorf("expected cross-host to NOT receive client authorization, got %q", auth)
	}
}

func TestESI_DefaultsPreservedWithPartialOptions(t *testing.T) {
	_, _, mw := setupTestTitip(t,
		WithESI(
			esi.WithHeaderRequired(true),
		),
	)

	if !mw.cfg.esi.Enabled {
		t.Errorf("expected ESI to be enabled")
	}
	if !mw.cfg.esi.HeaderRequired {
		t.Errorf("expected HeaderRequired to be true")
	}
	if mw.cfg.esi.MaxDepth != 3 {
		t.Errorf("expected default MaxDepth=3, got %d", mw.cfg.esi.MaxDepth)
	}
	if mw.cfg.esi.MaxTimeout != 30*time.Second {
		t.Errorf("expected default MaxTimeout=30s, got %v", mw.cfg.esi.MaxTimeout)
	}
	if mw.cfg.esi.MaxConcurrentRequests != 8 {
		t.Errorf("expected default MaxConcurrentRequests=8, got %d", mw.cfg.esi.MaxConcurrentRequests)
	}
	if mw.cfg.esi.MaxResponseSize != 10*1024*1024 {
		t.Errorf("expected default MaxResponseSize=10MB, got %d", mw.cfg.esi.MaxResponseSize)
	}
	if mw.cfg.esi.AllowPrivateIPs != false {
		t.Errorf("expected default AllowPrivateIPs=false, got true")
	}
	if mw.cfg.esi.DisableForwardCookies != false {
		t.Errorf("expected default DisableForwardCookies=false, got true")
	}
}

