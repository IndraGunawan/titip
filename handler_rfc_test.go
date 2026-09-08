package titip

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Cache-Status header modes (RFC-9211, Simple Token, None).
func TestCacheStatusModes(t *testing.T) {
	t.Parallel()
	// Mode 1: RFC-9211
	_, _, mw1 := setupTestTitip(t, WithCacheStatusMode(CacheStatusRFC9211))
	// Mode 2: Simple Token
	_, _, mw2 := setupTestTitip(t, WithCacheStatusMode(CacheStatusSimpleToken))
	// Mode 3: None
	_, _, mw3 := setupTestTitip(t, WithCacheStatusMode(CacheStatusNone))

	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("status test"))
	})
	// Prime mw1
	h1 := mw1.testHandler(originHandler)
	doGet(t, h1, "http://example.com/status-test")
	doGet(t, h1, "http://example.com/status-test").
		assertCacheStatus("titip; hit")

	// Prime mw2
	h2 := mw2.testHandler(originHandler)
	doGet(t, h2, "http://example.com/status-test")
	doGet(t, h2, "http://example.com/status-test").
		assertHeader("Cache-Status", "HIT")

	// Prime mw3
	h3 := mw3.testHandler(originHandler)
	doGet(t, h3, "http://example.com/status-test")
	doGet(t, h3, "http://example.com/status-test").
		assertHeader("Cache-Status", "")
}


// TestMultiVariant_VaryHeaderLifecycle verifies how variants are detected, evaluated, and stored
func TestMultiVariant_VaryHeaderLifecycle(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	var originExecutions atomic.Int32
	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originExecutions.Add(1)
		lang := r.Header.Get("Accept-Language")
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Vary", "Accept-Language")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)

		switch lang {
		case "es-ES":
			_, _ = w.Write([]byte(`{"msg":"hola"}`))
		case "fr-FR":
			_, _ = w.Write([]byte(`{"msg":"bonjour"}`))
		default:
			_, _ = w.Write([]byte(`{"msg":"hello"}`))
		}
	})

	handler := mw.testHandler(originHandler)
	baseURL := "http://example.com/api/greeting"

	// 1. First Request: English variant (Cold URL Miss -> call #1)
	doGet(t, handler, baseURL, "Accept-Language", "en-US").
		assertBody(`{"msg":"hello"}`).
		assertCacheStatus("fwd=uri-miss")
	if originExecutions.Load() != 1 {
		t.Fatalf("expected 1 origin call, got %d", originExecutions.Load())
	}

	// 2. Second Request: English variant (Cache Hit -> 0 origin calls)
	doGet(t, handler, baseURL, "Accept-Language", "en-US").
		assertBody(`{"msg":"hello"}`).
		assertCacheStatus("hit")
	if originExecutions.Load() != 1 {
		t.Fatalf("cache hit should not invoke origin: %d", originExecutions.Load())
	}

	// 3. Third Request: Spanish variant (URL exists in cache, but Variant is missing -> call #2)
	doGet(t, handler, baseURL, "Accept-Language", "es-ES").
		assertBody(`{"msg":"hola"}`).
		assertCacheStatus("fwd=vary-miss")
	if originExecutions.Load() != 2 {
		t.Fatalf("expected 2 origin calls after new variant, got %d", originExecutions.Load())
	}

	// 4. Fourth Request: Spanish variant (Cache Hit for Spanish -> 0 origin calls)
	doGet(t, handler, baseURL, "Accept-Language", "es-ES").
		assertBody(`{"msg":"hola"}`).
		assertCacheStatus("hit")
	if originExecutions.Load() != 2 {
		t.Fatalf("expected 2 origin calls, got %d", originExecutions.Load())
	}

	// 5. Fifth Request: English variant again (Cache Hit for English -> 0 origin calls)
	doGet(t, handler, baseURL, "Accept-Language", "en-US").
		assertBody(`{"msg":"hello"}`)
	if originExecutions.Load() != 2 {
		t.Fatalf("expected 2 origin calls, got %d", originExecutions.Load())
	}

	// 6. Sixth Request: French variant (URL exists, 3rd Variant missing -> call #3)
	doGet(t, handler, baseURL, "Accept-Language", "fr-FR").
		assertBody(`{"msg":"bonjour"}`)
	if originExecutions.Load() != 3 {
		t.Fatalf("expected 3 origin calls, got %d", originExecutions.Load())
	}

	// 7. Seventh Request: French variant (Cache Hit -> 0 origin calls)
	doGet(t, handler, baseURL, "Accept-Language", "fr-FR").
		assertBody(`{"msg":"bonjour"}`)
	if originExecutions.Load() != 3 {
		t.Fatalf("expected 3 origin calls, got %d", originExecutions.Load())
	}
}


// TestRFC_MandatoryCachedResponseHeaders validates mandatory RFC 9111/7234 cached response headers and hop-by-hop stripping
func TestRFC_MandatoryCachedResponseHeaders(t *testing.T) {
	t.Parallel()
	_, _, engine := setupTestTitip(t)

	originDate := "Sun, 06 Nov 1994 08:49:37 GMT"
	handler := engine.testHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=10, stale-while-revalidate=60")
		w.Header().Set("Date", originDate)
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Connection", "Keep-Alive, X-Custom-Hop")
		w.Header().Set("Keep-Alive", "timeout=5, max=100")
		w.Header().Set("X-Custom-Hop", "strip-me")
		w.Header().Set("X-Regular-Header", "keep-me")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, `{"status":"ok"}`)
	}))

	// 1. Cold miss: Origin response is stored
	doGet(t, handler, "http://example.com/api/rfc-headers").
		assertStatus(http.StatusOK)

	// 2. Sleep for 1.1 seconds so resident age is > 0
	time.Sleep(1100 * time.Millisecond)

	// 3. Cache Hit: Verify Age, Date, and Hop-by-Hop stripping
	rec2 := doGet(t, handler, "http://example.com/api/rfc-headers").
		assertStatus(http.StatusOK).
		assertHeader("Date", originDate).
		assertHeaderMissing("Connection").
		assertHeaderMissing("Keep-Alive").
		assertHeaderMissing("X-Custom-Hop").
		assertHeader("X-Regular-Header", "keep-me")

	// Verify Age header presence and value
	ageStr := rec2.Header().Get("Age")
	if ageStr == "" {
		t.Errorf("expected Age header present on cache hit")
	} else {
		ageSec, err := strconv.Atoi(ageStr)
		if err != nil || ageSec < 1 {
			t.Errorf("expected Age >= 1 second, got %s (err: %v)", ageStr, err)
		}
	}
}


func TestRFC9211_ForwardReasonsAndParameters(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t, WithRespectClientCacheControl())

	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=1, stale-if-error=10")
		w.Header().Set("Vary", "Accept-Language")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, `{"status":"ok"}`)
	})

	handler := mw.testHandler(originHandler)

	// 1. Test fwd=method on mutating POST request
	doPost(t, handler, "http://example.com/api/rfc9211", `{}`).
		assertCacheStatus("fwd=method")

	// 2. Test fwd=method on OPTIONS request
	doReq(t, handler, httptest.NewRequest(http.MethodOptions, "http://example.com/api/rfc9211", nil)).
		assertCacheStatus("fwd=method")

	// 3. Test fwd=request on client Cache-Control: no-store
	doGet(t, handler, "http://example.com/api/rfc9211", "Cache-Control", "no-store").
		assertCacheStatus("fwd=request")

	// 4. Test fwd=uri-miss on fresh URL
	doGet(t, handler, "http://example.com/api/rfc9211", "Accept-Language", "en").
		assertCacheStatus("fwd=uri-miss").
		assertCacheStatus("stored")

	// 5. Test fwd=vary-miss on new variant of existing URL
	doGet(t, handler, "http://example.com/api/rfc9211", "Accept-Language", "fr").
		assertCacheStatus("fwd=vary-miss")

	// 6. Test fwd=stale on expired entry revalidation
	time.Sleep(1100 * time.Millisecond)
	doGet(t, handler, "http://example.com/api/rfc9211", "Accept-Language", "en").
		assertCacheStatus("fwd=stale")
}


func TestSimpleToken_CloudflareCompatible_AllTokens(t *testing.T) {
	t.Parallel()

	// Mock origin handler that supports multiple behavior endpoints
	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/simple/cacheable":
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `{"status":"cacheable"}`)

		case "/api/simple/dynamic":
			// Origin returns Set-Cookie -> Uncacheable Dynamic
			w.Header().Set("Set-Cookie", "session=xyz123; HttpOnly")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `{"status":"dynamic"}`)

		case "/api/simple/swr":
			w.Header().Set("Cache-Control", "public, max-age=1, stale-while-revalidate=10")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `{"status":"swr"}`)

		case "/api/simple/reval":
			w.Header().Set("Cache-Control", "public, max-age=1, stale-if-error=10")
			w.Header().Set("ETag", `"v1.0"`)
			if r.Header.Get("If-None-Match") == `"v1.0"` {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `{"status":"reval"}`)

		case "/api/simple/expired":
			w.Header().Set("Cache-Control", "public, max-age=1, stale-if-error=10")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintf(w, `{"time":%d}`, time.Now().UnixNano())

		case "/api/simple/failover":
			if r.Header.Get("If-None-Match") != "" || r.Header.Get("If-Modified-Since") != "" {
				// Simulate origin 500 failure on revalidation -> fallback to stale
				http.Error(w, "upstream database timeout", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Cache-Control", "public, max-age=1, stale-if-error=10")
			w.Header().Set("ETag", `"v-failover"`)
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `{"status":"initial-healthy"}`)

		default:
			http.NotFound(w, r)
		}
	})

	t.Run("MissAndHit", func(t *testing.T) {
		t.Parallel()
		_, _, mw := setupTestTitip(t, WithCacheStatusMode(CacheStatusSimpleToken))
		handler := mw.testHandler(originHandler)

		doGet(t, handler, "http://example.com/api/simple/cacheable").
			assertHeader("Cache-Status", tokenMiss)

		doGet(t, handler, "http://example.com/api/simple/cacheable").
			assertHeader("Cache-Status", tokenHit)
	})

	t.Run("Bypass", func(t *testing.T) {
		t.Parallel()
		_, _, mw := setupTestTitip(t, WithCacheStatusMode(CacheStatusSimpleToken), WithRespectClientCacheControl())
		handler := mw.testHandler(originHandler)

		doPost(t, handler, "http://example.com/api/simple/cacheable", `{}`).
			assertHeader("Cache-Status", tokenBypass)

		doGet(t, handler, "http://example.com/api/simple/cacheable", "Cache-Control", "no-store").
			assertHeader("Cache-Status", tokenBypass)
	})

	t.Run("Dynamic", func(t *testing.T) {
		t.Parallel()
		_, _, mw := setupTestTitip(t, WithCacheStatusMode(CacheStatusSimpleToken))
		handler := mw.testHandler(originHandler)

		doGet(t, handler, "http://example.com/api/simple/dynamic").
			assertHeader("Cache-Status", tokenDynamic)
	})

	t.Run("Updating", func(t *testing.T) {
		t.Parallel()
		_, _, mw := setupTestTitip(t, WithCacheStatusMode(CacheStatusSimpleToken))
		handler := mw.testHandler(originHandler)

		doGet(t, handler, "http://example.com/api/simple/swr")
		time.Sleep(1100 * time.Millisecond)
		doGet(t, handler, "http://example.com/api/simple/swr").
			assertHeader("Cache-Status", tokenUpdating)
	})

	t.Run("Revalidated", func(t *testing.T) {
		t.Parallel()
		_, _, mw := setupTestTitip(t, WithCacheStatusMode(CacheStatusSimpleToken))
		handler := mw.testHandler(originHandler)

		doGet(t, handler, "http://example.com/api/simple/reval")
		time.Sleep(1100 * time.Millisecond)
		doGet(t, handler, "http://example.com/api/simple/reval").
			assertHeader("Cache-Status", tokenRevalidated)
	})

	t.Run("Expired", func(t *testing.T) {
		t.Parallel()
		_, _, mw := setupTestTitip(t, WithCacheStatusMode(CacheStatusSimpleToken))
		handler := mw.testHandler(originHandler)

		doGet(t, handler, "http://example.com/api/simple/expired")
		time.Sleep(1100 * time.Millisecond)
		doGet(t, handler, "http://example.com/api/simple/expired").
			assertHeader("Cache-Status", tokenExpired)
	})

	t.Run("Stale", func(t *testing.T) {
		t.Parallel()
		_, _, mw := setupTestTitip(t, WithCacheStatusMode(CacheStatusSimpleToken))
		handler := mw.testHandler(originHandler)

		doGet(t, handler, "http://example.com/api/simple/failover")
		time.Sleep(1100 * time.Millisecond)
		doGet(t, handler, "http://example.com/api/simple/failover").
			assertStatus(http.StatusOK).
			assertHeader("Cache-Status", tokenStale)
	})
}


// TestRFC_Authorization_Guards verifies RFC 9111 §3.5:
// Shared caches MUST NOT store responses to requests with Authorization headers
// unless the response contains explicit public, s-maxage, or must-revalidate directives.
func TestRFC_Authorization_Guards(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	var originCalls atomic.Int64
	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		switch r.URL.Path {
		case "/auth-private":
			w.Header().Set("Cache-Control", "max-age=60")
			_, _ = w.Write([]byte("auth-secret-payload"))
		case "/auth-public":
			w.Header().Set("Cache-Control", "public, max-age=60")
			_, _ = w.Write([]byte("auth-public-payload"))
		case "/auth-s-maxage":
			w.Header().Set("Cache-Control", "s-maxage=60")
			_, _ = w.Write([]byte("auth-s-maxage-payload"))
		}
	})
	handler := mw.testHandler(origin)

	// 1. Request with Authorization and origin returning only max-age=60 MUST NOT be cached
	doGet(t, handler, "http://example.com/auth-private", "Authorization", "Bearer user-token").
		assertStatus(http.StatusOK)

	// Subsequent unauthenticated request MUST NOT receive cached response
	doGet(t, handler, "http://example.com/auth-private").
		assertCacheStatusNot("hit")
	if originCalls.Load() != 2 {
		t.Fatalf("expected 2 origin calls for unshared auth, got %d", originCalls.Load())
	}

	// 2. Request with Authorization and origin returning public MUST be cached
	doGet(t, handler, "http://example.com/auth-public", "Authorization", "Bearer user-token")

	doGet(t, handler, "http://example.com/auth-public").
		assertCacheStatus("hit")
}


// TestRFC_MustRevalidate_DisallowsSWR verifies RFC 5861 §3 & §4 / RFC 9111 §5.2.2.1:
// must-revalidate and proxy-revalidate forbid serving stale content under stale-while-revalidate.
func TestRFC_MustRevalidate_DisallowsSWR(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	var originCalls atomic.Int64
	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=1, must-revalidate, stale-while-revalidate=60")
		_, _ = w.Write([]byte("revalidate-data"))
	})
	handler := mw.testHandler(origin)

	doGet(t, handler, "http://example.com/must-reval")

	// Wait for entry to become stale
	time.Sleep(1100 * time.Millisecond)

	// Must NOT serve SWR
	doGet(t, handler, "http://example.com/must-reval").
		assertCacheStatusNot("detail=swr")
	if originCalls.Load() != 2 {
		t.Fatalf("expected synchronous revalidation (2 origin calls), got %d", originCalls.Load())
	}
}


// TestRFC_VaryStar_NoSubsequentMatch verifies RFC 9111 §4.1 / RFC 7231 §7.1.4:
// A response containing Vary: * MUST NOT be stored or served for subsequent requests.
func TestRFC_VaryStar_NoSubsequentMatch(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	var originCalls atomic.Int64
	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("Vary", "*")
		_, _ = w.Write([]byte("vary-star-data"))
	})
	handler := mw.testHandler(origin)

	doGet(t, handler, "http://example.com/vary-star")

	doGet(t, handler, "http://example.com/vary-star").
		assertCacheStatusNot("hit")
	if originCalls.Load() != 2 {
		t.Fatalf("expected 2 origin calls for Vary: *, got %d", originCalls.Load())
	}
}


// TestRFC_ServedAge_PreservesUpstreamAge verifies RFC 9111 §5.1 / §4.2.3:
// Age header served from cache must equal corrected_initial_age + resident_time.
func TestRFC_ServedAge_PreservesUpstreamAge(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=300")
		w.Header().Set("Age", "45")
		_, _ = w.Write([]byte("upstream-age-data"))
	})
	handler := mw.testHandler(origin)

	req := httptest.NewRequest(http.MethodGet, "http://example.com/upstream-age", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req)

	time.Sleep(1100 * time.Millisecond)

	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req)

	ageStr := rec2.Header().Get("Age")
	ageVal, err := strconv.Atoi(ageStr)
	if err != nil {
		t.Fatalf("invalid Age header: %q", ageStr)
	}
	if ageVal < 46 {
		t.Fatalf("expected served Age >= 46 (45 initial + 1 resident), got %d", ageVal)
	}
}


// TestRFC_ClientDirectives_PragmaAndOnlyIfCached verifies RFC 9111 §5.4 and §5.2.1.7:
// 1. Pragma: no-cache acts as Cache-Control: no-cache when RespectClientCacheControl is enabled.
// 2. only-if-cached returns 504 Gateway Timeout when cache is missed or expired.
func TestRFC_ClientDirectives_PragmaAndOnlyIfCached(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t, WithRespectClientCacheControl())

	var originCalls atomic.Int64
	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=60")
		_, _ = w.Write([]byte("client-cc-data"))
	})
	handler := mw.testHandler(origin)

	// 1. Initial request populates cache
	doGet(t, handler, "http://example.com/client-cc")
	if originCalls.Load() != 1 {
		t.Fatalf("expected 1 origin call, got %d", originCalls.Load())
	}

	// 2. Pragma: no-cache forces origin bypass (RFC 9111 §5.4)
	doGet(t, handler, "http://example.com/client-cc", "Pragma", "no-cache").
		assertCacheStatus("pragma-no-cache")
	if originCalls.Load() != 2 {
		t.Fatalf("expected 2 origin calls after Pragma: no-cache, got %d", originCalls.Load())
	}

	// 3. Cache-Control: max-age=0 forces revalidation / bypass (RFC 9111 §5.2.1.1)
	doGet(t, handler, "http://example.com/client-cc", "Cache-Control", "max-age=0").
		assertCacheStatus("max-age-0")

	// 4. only-if-cached on a missing URL returns 504 Gateway Timeout (RFC 9111 §5.2.1.7)
	doGet(t, handler, "http://example.com/missing-url", "Cache-Control", "only-if-cached").
		assertStatus(http.StatusGatewayTimeout)
}


// TestRFC_MutatingMethod_InvalidatesLocation verifies RFC 9111 §4.4:
// A successful non-safe request (POST, PUT, DELETE, PATCH) invalidates both the effective request URI
// and any URIs specified in Location or Content-Location response headers.
func TestRFC_MutatingMethod_InvalidatesLocation(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t, WithAutoInvalidateMutatingMethods())

	var getCalls atomic.Int64
	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/resource/1" {
			getCalls.Add(1)
			w.Header().Set("Cache-Control", "public, max-age=300")
			_, _ = w.Write([]byte("resource-1-data"))
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == "/resource/update" {
			w.Header().Set("Location", "http://example.com/resource/1")
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte("created"))
			return
		}
	})
	handler := mw.testHandler(origin)

	// Step 1: Cache GET /resource/1
	doGet(t, handler, "http://example.com/resource/1")
	if getCalls.Load() != 1 {
		t.Fatalf("expected 1 get call, got %d", getCalls.Load())
	}

	// Verify it is cached
	doGet(t, handler, "http://example.com/resource/1").
		assertCacheStatus("hit")
	if getCalls.Load() != 1 {
		t.Fatalf("expected still 1 get call, got %d", getCalls.Load())
	}

	// Step 2: POST /resource/update with Location: http://example.com/resource/1 (RFC 9111 §4.4)
	doPost(t, handler, "http://example.com/resource/update", "").
		assertStatus(http.StatusCreated)

	// Step 3: GET /resource/1 must now be invalidated and fetch origin
	doGet(t, handler, "http://example.com/resource/1")
	if getCalls.Load() != 2 {
		t.Fatalf("expected GET /resource/1 to be invalidated by Location header, origin calls: %d", getCalls.Load())
	}
}


// TestRFC_IfModifiedSince_SecondsPrecision verifies RFC 7232 §3.3 / RFC 9110 §13.1.3:
// If-Modified-Since evaluates equality with 1-second resolution (sub-second fractions ignored).
func TestRFC_IfModifiedSince_SecondsPrecision(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	lmTime := time.Date(2026, 8, 28, 12, 0, 0, 500000000, time.UTC) // 12:00:00.500
	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=300")
		w.Header().Set("Last-Modified", lmTime.Format(http.TimeFormat))
		_, _ = w.Write([]byte("ims-data"))
	})
	handler := mw.testHandler(origin)

	// Populate cache
	doGet(t, handler, "http://example.com/ims-test")

	// Client sends IMS matching exact second
	doGet(t, handler, "http://example.com/ims-test", "If-Modified-Since", lmTime.Truncate(time.Second).Format(http.TimeFormat)).
		assertStatus(http.StatusNotModified)
}


// TestRFC_Expires_Alone_Cacheable verifies RFC 9111 §4.2.1 / §5.3 / Cloudflare compatibility:
// A response without Cache-Control is cacheable if Expires is set to a future date.
func TestRFC_Expires_Alone_Cacheable(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	var originCalls atomic.Int64
	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		w.Header().Set("Date", time.Now().Format(http.TimeFormat))
		w.Header().Set("Expires", time.Now().Add(60*time.Second).Format(http.TimeFormat))
		_, _ = w.Write([]byte("expires-only-data"))
	})
	handler := mw.testHandler(origin)

	doGet(t, handler, "http://example.com/expires-only")
	if originCalls.Load() != 1 {
		t.Fatalf("expected 1 origin call, got %d", originCalls.Load())
	}

	doGet(t, handler, "http://example.com/expires-only").
		assertCacheStatus("hit")
	if originCalls.Load() != 1 {
		t.Fatalf("expected cached hit without calling origin, got %d calls", originCalls.Load())
	}
}


func TestMultipleVaryHeaders_EndToEnd(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	var originCalls atomic.Int64
	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Add("Vary", "Accept-Language")
		w.Header().Add("Vary", "Accept-Encoding")
		lang := r.Header.Get("Accept-Language")
		enc := r.Header.Get("Accept-Encoding")
		_, _ = w.Write([]byte("lang=" + lang + ",enc=" + enc))
	})
	handler := mw.testHandler(origin)

	// 1. Request with en, gzip -> MISS
	doGet(t, handler, "http://example.com/multi-vary", "Accept-Language", "en", "Accept-Encoding", "gzip").
		assertBody("lang=en,enc=gzip")
	if originCalls.Load() != 1 {
		t.Fatalf("expected 1 origin call, got %d", originCalls.Load())
	}

	// 2. Same variant -> HIT
	doGet(t, handler, "http://example.com/multi-vary", "Accept-Language", "en", "Accept-Encoding", "gzip").
		assertCacheStatus("hit")
	if originCalls.Load() != 1 {
		t.Fatalf("expected cached hit, got %d origin calls", originCalls.Load())
	}

	// 3. Different language (fr) -> MISS (different variant due to 1st Vary header)
	doGet(t, handler, "http://example.com/multi-vary", "Accept-Language", "fr", "Accept-Encoding", "gzip").
		assertBody("lang=fr,enc=gzip")
	if originCalls.Load() != 2 {
		t.Fatalf("expected 2 origin calls, got %d", originCalls.Load())
	}

	// 4. Different encoding (br) -> MISS (different variant due to 2nd Vary header)
	doGet(t, handler, "http://example.com/multi-vary", "Accept-Language", "en", "Accept-Encoding", "br").
		assertBody("lang=en,enc=br")
	if originCalls.Load() != 3 {
		t.Fatalf("expected 3 origin calls, got %d", originCalls.Load())
	}
}


func TestMultipleCacheControlHeaders_EndToEnd(t *testing.T) {
	t.Parallel()

	t.Run("MultiLine_Cacheable_Hit", func(t *testing.T) {
		t.Parallel()
		_, _, mw := setupTestTitip(t)

		var originCalls atomic.Int64
		origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			originCalls.Add(1)
			w.Header().Add("Cache-Control", "public")
			w.Header().Add("Cache-Control", "max-age=60")
			_, _ = w.Write([]byte("multi-cc-hit-data"))
		})
		handler := mw.testHandler(origin)

		// 1. Initial request -> MISS & Cache
		doGet(t, handler, "http://example.com/multi-cc-hit")
		if originCalls.Load() != 1 {
			t.Fatalf("expected 1 origin call, got %d", originCalls.Load())
		}

		// 2. Second request -> HIT
		doGet(t, handler, "http://example.com/multi-cc-hit").
			assertCacheStatus("hit")
		if originCalls.Load() != 1 {
			t.Fatalf("expected 1 origin call on hit, got %d", originCalls.Load())
		}
	})

	t.Run("MultiLine_Conflicting_Private_NeverCached", func(t *testing.T) {
		t.Parallel()
		_, _, mw := setupTestTitip(t)

		var originCalls atomic.Int64
		origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			originCalls.Add(1)
			w.Header().Add("Cache-Control", "s-maxage=60, public")
			w.Header().Add("Cache-Control", "private")
			_, _ = w.Write([]byte("multi-cc-private-data"))
		})
		handler := mw.testHandler(origin)

		// 1. Initial request -> MISS (and should not be stored)
		doGet(t, handler, "http://example.com/multi-cc-private")
		if originCalls.Load() != 1 {
			t.Fatalf("expected 1 origin call, got %d", originCalls.Load())
		}

		// 2. Second request -> must still be MISS because private prevented caching
		doGet(t, handler, "http://example.com/multi-cc-private").
			assertCacheStatusNot("hit")
		if originCalls.Load() != 2 {
			t.Fatalf("expected 2 origin calls (bypassed cache), got %d", originCalls.Load())
		}
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// RFC Compliance & Security Hardening Verification Tests
// ─────────────────────────────────────────────────────────────────────────────


func TestRFC_DownstreamConditional_NeverServedFromExpiredCache(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	var originCalls atomic.Int64
	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls := originCalls.Add(1)
		if calls == 1 {
			w.Header().Set("Cache-Control", "public, max-age=1, stale-if-error=60")
			w.Header().Set("ETag", `"v1"`)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("initial version"))
			return
		}
		// Revalidation call: origin confirms 304 and refreshes max-age=60
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.Header().Set("ETag", `"v1"`)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("ETag", `"v2"`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("updated version"))
	})
	handler := mw.testHandler(origin)

	// 1. Prime cache with 1-second TTL
	doGet(t, handler, "http://example.com/expired-reval").
		assertStatus(http.StatusOK)
	if originCalls.Load() != 1 {
		t.Fatalf("expected initial 200 OK from origin, got code %d, calls %d", http.StatusOK, originCalls.Load())
	}

	// 2. Wait 1.1s for cache to expire
	time.Sleep(1100 * time.Millisecond)

	// 3. Client sends conditional request with If-None-Match on expired cache
	// Must NOT return 304 directly without revalidating with origin (RFC 9111 §4.3.2)
	rec2 := doGet(t, handler, "http://example.com/expired-reval", "If-None-Match", `"v1"`).
		assertStatus(http.StatusNotModified)

	if originCalls.Load() != 2 {
		t.Fatalf("expected origin to be revalidated on expired cache, but origin calls = %d", originCalls.Load())
	}
	if rec2.Header().Get("Age") == "" {
		t.Fatalf("expected Age header on 304 response, got headers: %#v, code: %d", rec2.Header(), rec2.Code)
	}

	// 4. Third request within refreshed TTL (60s) gets fresh cache HIT with 0 origin calls
	doGet(t, handler, "http://example.com/expired-reval").
		assertStatus(http.StatusOK).
		assertBody("initial version")
	if originCalls.Load() != 2 {
		t.Fatalf("expected 0 additional origin calls on fresh hit, got %d", originCalls.Load())
	}
}


func TestRFC_Preconditions_IfMatch_And_IfUnmodifiedSince(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	lastMod := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=300")
		w.Header().Set("ETag", `"strong-etag-1"`)
		w.Header().Set("Last-Modified", lastMod.Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("resource-content"))
	})
	handler := mw.testHandler(origin)

	// 1. Prime cache
	doGet(t, handler, "http://example.com/preconditions").
		assertStatus(http.StatusOK)

	// 2. If-Match: matching strong ETag -> 200 OK
	doGet(t, handler, "http://example.com/preconditions", "If-Match", `"strong-etag-1"`).
		assertStatus(http.StatusOK)

	// 3. If-Match: non-matching ETag -> 412 Precondition Failed (RFC 9110 §13.1.1)
	doGet(t, handler, "http://example.com/preconditions", "If-Match", `"other-etag"`).
		assertStatus(http.StatusPreconditionFailed)

	// 4. If-Match: weak ETag with strong comparison requirement -> 412 Precondition Failed
	doGet(t, handler, "http://example.com/preconditions", "If-Match", `W/"strong-etag-1"`).
		assertStatus(http.StatusPreconditionFailed)

	// 5. If-Unmodified-Since: after Last-Modified date -> 200 OK
	doGet(t, handler, "http://example.com/preconditions", "If-Unmodified-Since", lastMod.Add(1*time.Hour).Format(http.TimeFormat)).
		assertStatus(http.StatusOK)

	// 6. If-Unmodified-Since: before Last-Modified date -> 412 Precondition Failed (RFC 9110 §13.1.4)
	doGet(t, handler, "http://example.com/preconditions", "If-Unmodified-Since", lastMod.Add(-1*time.Hour).Format(http.TimeFormat)).
		assertStatus(http.StatusPreconditionFailed)
}


func TestRFC_Upstream304_PreservesStoredCacheControl(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	var originCalls atomic.Int64
	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls := originCalls.Add(1)
		if calls == 1 {
			w.Header().Set("Cache-Control", "public, s-maxage=300")
			w.Header().Set("ETag", `"stable-v1"`)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("stable-body"))
			return
		}
		// Upstream 304 response omits Cache-Control header
		w.Header().Set("ETag", `"stable-v1"`)
		w.WriteHeader(http.StatusNotModified)
	})
	handler := mw.testHandler(origin)

	// 1. Prime cache
	doGet(t, handler, "http://example.com/upstream-304-merge")
	if originCalls.Load() != 1 {
		t.Fatalf("expected 1 prime call, got %d", originCalls.Load())
	}

	// 2. Soft-purge to force revalidation
	if _, err := mw.Purge(context.Background(), "http://example.com/upstream-304-merge", WithSoftPurge()); err != nil {
		t.Fatalf("soft purge failed: %v", err)
	}

	// 3. Revalidation request -> triggers origin 304 without Cache-Control
	doGet(t, handler, "http://example.com/upstream-304-merge").
		assertStatus(http.StatusOK).
		assertBody("stable-body")
	if originCalls.Load() != 2 {
		t.Fatalf("expected origin revalidation call, got %d", originCalls.Load())
	}

	// 4. Stored Cache-Control must have refreshed Redis TTL, so next request is a fresh HIT
	doGet(t, handler, "http://example.com/upstream-304-merge").
		assertCacheStatus("hit")
	if originCalls.Load() != 2 {
		t.Fatalf("expected 0 origin calls on subsequent fresh HIT, got %d calls", originCalls.Load())
	}
}


func TestRFC_CrossHostPurge_Isolation(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t, WithAutoInvalidateMutatingMethods())

	var hostBCalls atomic.Int64
	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host == "tenant-b.com" {
			hostBCalls.Add(1)
			w.Header().Set("Cache-Control", "public, max-age=300")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("tenant-b-data"))
			return
		}
		// Tenant A mutating POST returns cross-host Location header
		w.Header().Set("Location", "http://tenant-b.com/api/products")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("created on tenant A"))
	})
	handler := mw.testHandler(origin)

	// 1. Prime cache for tenant-b.com/api/products
	reqPrimeB := httptest.NewRequest(http.MethodGet, "http://tenant-b.com/api/products", nil)
	doReq(t, handler, reqPrimeB)
	if hostBCalls.Load() != 1 {
		t.Fatalf("expected 1 call to tenant B, got %d", hostBCalls.Load())
	}

	// 2. Perform mutating POST on tenant-a.com with Location: http://tenant-b.com/api/products
	reqMutateA := httptest.NewRequest(http.MethodPost, "http://tenant-a.com/api/orders", strings.NewReader(`{"order":1}`))
	doReq(t, handler, reqMutateA).
		assertStatus(http.StatusCreated)

	// 3. Request tenant-b.com/api/products again -> must still be a cache HIT (0 new calls to origin)
	doReq(t, handler, reqPrimeB).
		assertCacheStatus("hit")
	if hostBCalls.Load() != 1 {
		t.Fatalf("cross-host Location header purged tenant B cache! origin calls = %d", hostBCalls.Load())
	}
}


func TestRFC_SetCookie_NeverStored_Or_Leaked(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	var originCalls atomic.Int64
	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=300")
		w.Header().Set("Set-Cookie", "session=secret-session-token; Path=/")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("sensitive-user-profile"))
	})
	handler := mw.testHandler(origin)

	// 1. First request receives Set-Cookie
	rec1 := doGet(t, handler, "http://example.com/profile")
	if rec1.Header().Get("Set-Cookie") == "" {
		t.Fatal("expected Set-Cookie on initial origin response")
	}

	// 2. Second request from different user must NOT be served from cache
	doGet(t, handler, "http://example.com/profile").
		assertCacheStatusNot("hit")
	if originCalls.Load() != 2 {
		t.Fatalf("expected Set-Cookie response to bypass cache (2 origin calls), got %d", originCalls.Load())
	}
}


func TestRFC9211_MultiCacheChaining_AppendsHeader(t *testing.T) {
	t.Parallel()

	// 1. RFC 9211 Mode: Appends to existing upstream Cache-Status
	t.Run("RFC9211_Appends_Upstream", func(t *testing.T) {
		t.Parallel()
		_, _, mw := setupTestTitip(t, WithCacheStatusMode(CacheStatusRFC9211))

		origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "public, max-age=300")
			w.Header().Set("Cache-Status", `"Fastly"; hit`)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("fastly-cached-origin-data"))
		})
		handler := mw.testHandler(origin)

		// Initial request (MISS)
		rec1 := doGet(t, handler, "http://example.com/multi-cache-chain")
		statuses := rec1.Header().Values("Cache-Status")
		joined := strings.Join(statuses, ", ")
		if !strings.Contains(joined, `"Fastly"; hit`) || !strings.Contains(joined, "titip;") {
			t.Fatalf("expected both Fastly and titip in Cache-Status, got: %v", statuses)
		}

		// Subsequent request (HIT)
		rec2 := doGet(t, handler, "http://example.com/multi-cache-chain")
		statusesHit := rec2.Header().Values("Cache-Status")
		joinedHit := strings.Join(statusesHit, ", ")
		if !strings.Contains(joinedHit, `"Fastly"; hit`) || !strings.Contains(joinedHit, "titip; hit") {
			t.Fatalf("expected both Fastly and titip hit in Cache-Status on cache hit, got: %v", statusesHit)
		}
	})

	// 2. SimpleToken Mode: Overwrites upstream Cache-Status with single token
	t.Run("SimpleToken_Overwrites_Upstream", func(t *testing.T) {
		t.Parallel()
		_, _, mw := setupTestTitip(t, WithCacheStatusMode(CacheStatusSimpleToken))

		origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "public, max-age=300")
			w.Header().Set("Cache-Status", `"Fastly"; hit`)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("fastly-cached-origin-data"))
		})
		handler := mw.testHandler(origin)

		// Initial request -> MISS
		doGet(t, handler, "http://example.com/multi-cache-simple").
			assertHeader("Cache-Status", "MISS")

		// Subsequent request -> HIT
		doGet(t, handler, "http://example.com/multi-cache-simple").
			assertHeader("Cache-Status", "HIT")
	})
}


func TestRFC_NoCache_ConditionalRevalidation_And_StaleIfErrorFailover(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	var originCalls atomic.Int64
	var originShouldFail atomic.Bool

	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls := originCalls.Add(1)

		// 3. Origin failure simulation (stale-if-error failover)
		if originShouldFail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("500 Internal Server Error"))
			return
		}

		// 1. Initial generation
		if calls == 1 {
			w.Header().Set("Cache-Control", "max-age=31536000, no-cache, stale-if-error=31536000")
			w.Header().Set("ETag", `"resource-v1"`)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("<html>Resource Profile Content</html>"))
			return
		}

		// 2. Revalidation call: conditional check
		if r.Header.Get("If-None-Match") == `"resource-v1"` {
			w.Header().Set("Cache-Control", "max-age=31536000, no-cache, stale-if-error=31536000")
			w.Header().Set("ETag", `"resource-v1"`)
			w.WriteHeader(http.StatusNotModified)
			return
		}

		w.Header().Set("Cache-Control", "max-age=31536000, no-cache, stale-if-error=31536000")
		w.Header().Set("ETag", `"resource-v2"`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("<html>Updated Profile Content</html>"))
	})
	handler := mw.testHandler(origin)

	// Step 1: Initial request -> MISS, stores cache entry
	doGet(t, handler, "http://example.com/profile").
		assertStatus(http.StatusOK).
		assertBody("<html>Resource Profile Content</html>")
	if originCalls.Load() != 1 {
		t.Fatalf("expected 1 origin call, got %d", originCalls.Load())
	}

	// Step 2: Second request -> because of no-cache, must synchronously revalidate with origin
	doGet(t, handler, "http://example.com/profile").
		assertStatus(http.StatusOK).
		assertBody("<html>Resource Profile Content</html>").
		assertCacheStatus("304")
	if originCalls.Load() != 2 {
		t.Fatalf("expected second request to revalidate with origin (2 calls), got %d", originCalls.Load())
	}

	// Step 3: Origin server crashes (500 Internal Server Error) -> stale-if-error fallback
	originShouldFail.Store(true)

	// Client must receive 200 OK with cached profile HTML (never 500 error!)
	doGet(t, handler, "http://example.com/profile").
		assertStatus(http.StatusOK).
		assertBody("<html>Resource Profile Content</html>").
		assertCacheStatus("stale-if-error")
	if originCalls.Load() != 3 {
		t.Fatalf("expected third request to attempt origin revalidation (3 calls), got %d", originCalls.Load())
	}
}


func TestRFC9213_TieredCacheControl_EndToEnd(t *testing.T) {
	t.Parallel()

	// 1. Titip-Cache-Control caches on server while Cache-Control remains private/no-store for browsers
	t.Run("TitipCacheControl_DecoupledFromBrowser", func(t *testing.T) {
		t.Parallel()
		_, _, mw := setupTestTitip(t)

		var originCalls atomic.Int64
		origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			originCalls.Add(1)
			w.Header().Set("Titip-Cache-Control", "public, max-age=300")
			w.Header().Set("Cache-Control", "private, no-store")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("decoupled-payload"))
		})
		handler := mw.testHandler(origin)

		// Request 1 -> MISS, cached by Titip because of Titip-Cache-Control
		doGet(t, handler, "http://example.com/tiered-1").
			assertHeader("Cache-Control", "private, no-store")
		if originCalls.Load() != 1 {
			t.Fatalf("expected 1 origin call, got %d", originCalls.Load())
		}

		// Request 2 -> HIT served from Titip cache
		doGet(t, handler, "http://example.com/tiered-1").
			assertCacheStatus("hit").
			assertHeader("Cache-Control", "private, no-store")
		if originCalls.Load() != 1 {
			t.Fatalf("expected 0 additional origin calls on cache HIT, got %d", originCalls.Load())
		}
	})

	// 2. CDN-Cache-Control (RFC 9213 generic) takes precedence over standard Cache-Control
	t.Run("CDNCacheControl_Overrides_Standard", func(t *testing.T) {
		t.Parallel()
		_, _, mw := setupTestTitip(t)

		var originCalls atomic.Int64
		origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			originCalls.Add(1)
			w.Header().Set("CDN-Cache-Control", "public, max-age=300")
			w.Header().Set("Cache-Control", "max-age=0, must-revalidate")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("cdn-payload"))
		})
		handler := mw.testHandler(origin)

		// Request 1 -> MISS
		doGet(t, handler, "http://example.com/tiered-cdn")

		// Request 2 -> HIT (because CDN-Cache-Control max-age=300 overrides Cache-Control max-age=0)
		doGet(t, handler, "http://example.com/tiered-cdn").
			assertCacheStatus("hit")
		if originCalls.Load() != 1 {
			t.Fatalf("expected 1 origin call (CDN-Cache-Control hit), got %d", originCalls.Load())
		}
	})
}

