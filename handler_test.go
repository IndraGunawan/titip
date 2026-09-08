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

	pb "github.com/indragunawan/titip/proto"
)

// Fail-open on storage outage.
func TestFailOpen_OnStorageOutage(t *testing.T) {
	t.Parallel()
	store, _, mw := setupTestTitip(t, WithStorageTimeout(100*time.Millisecond))

	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("live origin response"))
	})

	handler := mw.testHandler(originHandler)

	// Close storage to simulate outage
	store.SetClosed(true)

	doGet(t, handler, "http://example.com/api/fail-open").
		assertStatus(http.StatusOK).
		assertBody("live origin response").
		assertCacheStatus("bypass")
}

// TestPanicRecovery_ColdMiss_NoCrash tests that an upstream panic on a cold request returns 500 without crashing
func TestPanicRecovery_ColdMiss_NoCrash(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	panickingHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("critical origin failure")
	})

	handler := mw.testHandler(panickingHandler)

	// Must not panic or crash process
	doGet(t, handler, "http://example.com/api/cold-panic").
		assertStatus(http.StatusInternalServerError)
}

// Panic recovery with stale fallback.
func TestPanicRecovery_WithStaleFallback(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	var shouldPanic atomic.Bool
	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if shouldPanic.Load() {
			panic("simulated upstream panic")
		}
		w.Header().Set("Cache-Control", "public, max-age=60, stale-if-error=300")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("panic fallback data"))
	})

	handler := mw.testHandler(originHandler)

	// 1. Initial request caches data
	doGet(t, handler, "http://example.com/api/panic-test").
		assertStatus(http.StatusOK)

	// 2. Soft purge
	_, _ = mw.Purge(context.Background(), "http://example.com/api/panic-test", WithSoftPurge())

	// 3. Trigger panic on origin
	shouldPanic.Store(true)

	doGet(t, handler, "http://example.com/api/panic-test").
		assertStatus(http.StatusOK).
		assertBody("panic fallback data")
}

// Conditional 304 and HEAD requests
func TestConditionalAndHeadRequests(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("ETag", `"v1.0"`)
		w.Header().Set("Last-Modified", "Sun, 06 Nov 1994 08:49:37 GMT")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("full payload content"))
	})

	handler := mw.testHandler(originHandler)

	// 1. Prime cache
	doGet(t, handler, "http://example.com/api/conditional").
		assertStatus(http.StatusOK)

	// 2. Conditional If-None-Match
	doGet(t, handler, "http://example.com/api/conditional", "If-None-Match", `"v1.0"`).
		assertStatus(http.StatusNotModified).
		assertEmptyBody()

	// 3. HEAD request
	doHead(t, handler, "http://example.com/api/conditional").
		assertStatus(http.StatusOK).
		assertEmptyBody()
}

// TestColdHead_ThenGet_Success validates that cold HEAD primes the cache with the response body
func TestColdHead_ThenGet_Success(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	expectedBody := "hello world full body payload"
	originCalls := int32(0)

	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&originCalls, 1)
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Length", strconv.Itoa(len(expectedBody)))
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			_, _ = w.Write([]byte(expectedBody))
		}
	})

	handler := mw.testHandler(originHandler)

	// 1. Cold HEAD request
	doHead(t, handler, "http://example.com/api/test-head-cold").
		assertStatus(http.StatusOK).
		assertEmptyBody().
		assertCacheStatus("stored")

	// 2. Subsequent GET request for the same URL (should be a cache HIT with full body)
	doGet(t, handler, "http://example.com/api/test-head-cold").
		assertStatus(http.StatusOK).
		assertBody(expectedBody).
		assertCacheStatus("hit")

	if calls := atomic.LoadInt32(&originCalls); calls != 1 {
		t.Fatalf("expected exactly 1 origin call (for the HEAD priming), got %d", calls)
	}
}

// TestColdHead_OptOut_ConvertHeadToGetFalse validates that disabling ConvertHeadToGet prevents 0-byte caching
func TestColdHead_OptOut_ConvertHeadToGetFalse(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t, WithConvertHeadToGet(false))

	expectedBody := "hello world payload"
	originCalls := int32(0)

	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&originCalls, 1)
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Length", strconv.Itoa(len(expectedBody)))
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			_, _ = w.Write([]byte(expectedBody))
		}
	})

	handler := mw.testHandler(originHandler)

	// 1. Cold HEAD request with ConvertHeadToGet=false
	doHead(t, handler, "http://example.com/api/test-head-optout").
		assertStatus(http.StatusOK).
		assertEmptyBody().
		assertCacheStatusNot("stored")

	// 2. Subsequent GET request for the same URL (should be a clean MISS fetching full body)
	doGet(t, handler, "http://example.com/api/test-head-optout").
		assertStatus(http.StatusOK).
		assertBody(expectedBody)

	if calls := atomic.LoadInt32(&originCalls); calls != 2 {
		t.Fatalf("expected 2 origin calls (not cached on HEAD), got %d", calls)
	}
}

// TestHeadRevalidation_ConvertHeadToGet validates that expired cache revalidations on HEAD refresh the body
func TestHeadRevalidation_ConvertHeadToGet(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	var currentBody atomic.Value
	currentBody.Store("initial payload v1")
	originCalls := int32(0)

	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&originCalls, 1)
		body := currentBody.Load().(string)
		if body == "initial payload v1" {
			w.Header().Set("Cache-Control", "public, max-age=1")
		} else {
			w.Header().Set("Cache-Control", "public, max-age=60")
		}
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			_, _ = w.Write([]byte(body))
		}
	})

	handler := mw.testHandler(originHandler)

	// 1. Prime cache with GET
	doGet(t, handler, "http://example.com/api/test-reval-head").
		assertBody("initial payload v1")

	// Wait for TTL expiry
	time.Sleep(1100 * time.Millisecond)
	currentBody.Store("updated payload v2")

	// 2. HEAD request on expired entry -> singleflight revalidation as GET
	doHead(t, handler, "http://example.com/api/test-reval-head").
		assertStatus(http.StatusOK).
		assertEmptyBody()

	// 3. GET request -> should serve fresh updated payload v2 from cache
	doGet(t, handler, "http://example.com/api/test-reval-head").
		assertBody("updated payload v2").
		assertCacheStatus("hit")
}

// Unsafe HTTP method auto-invalidation
func TestUnsafeMethodAutoInvalidation_DefaultDisabled(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t) // default: WithAutoInvalidateMutatingMethods(false)

	var state atomic.Int32
	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			state.Store(42)
			w.WriteHeader(http.StatusCreated)
			return
		}
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, "state=%d", state.Load())
	})

	handler := mw.testHandler(originHandler)

	// 1. Prime cache
	doGet(t, handler, "http://example.com/api/state-default").
		assertBody("state=0")

	// 2. Mutating POST request
	doPost(t, handler, "http://example.com/api/state-default", "").
		assertStatus(http.StatusCreated)

	// 3. Subsequent GET must STILL return cached state=0 because auto-invalidation is disabled by default
	doGet(t, handler, "http://example.com/api/state-default").
		assertBody("state=0")
}

func TestUnsafeMethodAutoInvalidation_OptInEnabled(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t, WithAutoInvalidateMutatingMethods())

	var state atomic.Int32
	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			state.Store(42)
			w.WriteHeader(http.StatusCreated)
			return
		}
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, "state=%d", state.Load())
	})

	handler := mw.testHandler(originHandler)

	// 1. Prime cache
	doGet(t, handler, "http://example.com/api/state-enabled").
		assertBody("state=0")

	// 2. Mutating POST request
	doPost(t, handler, "http://example.com/api/state-enabled", "").
		assertStatus(http.StatusCreated)

	// 3. Subsequent GET must fetch new state=42 because auto-invalidation is enabled
	doGet(t, handler, "http://example.com/api/state-enabled").
		assertBody("state=42")
}

// Graceful shutdown awaiting SWR revalidation
func TestGracefulShutdown(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	var revalidations atomic.Int32
	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		revalidations.Add(1)
		time.Sleep(50 * time.Millisecond)
		w.Header().Set("Cache-Control", "public, max-age=1, stale-while-revalidate=10")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("swr response"))
	})

	handler := mw.testHandler(originHandler)

	// 1. Prime cache
	req := httptest.NewRequest(http.MethodGet, "http://example.com/api/swr-shutdown", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req)

	// 2. Wait for max-age (1s) to expire but remain in SWR window (10s)
	time.Sleep(1100 * time.Millisecond)

	// 3. Request triggers SWR background revalidation
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req)
	if status := rec2.Header().Get("Cache-Status"); !containsAny(status, "stale") {
		t.Fatalf("expected stale response, got %s", status)
	}

	// 4. Close middleware
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := mw.Close(ctx); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	if revalidations.Load() != 2 {
		t.Fatalf("expected 2 revalidations, got %d", revalidations.Load())
	}
}

func TestGracefulShutdown_Timeout(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	blockOrigin := make(chan struct{})
	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=1, stale-while-revalidate=10")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("swr response"))
		if r.Header.Get("X-Is-SWR") == "true" {
			<-blockOrigin
		}
	})

	handler := mw.testHandler(originHandler)

	// 1. Prime cache
	req := httptest.NewRequest(http.MethodGet, "http://example.com/api/swr-shutdown-timeout", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req)

	// 2. Wait for max-age (1s) to expire
	time.Sleep(1100 * time.Millisecond)

	// 3. Trigger SWR with header so origin will block
	req2 := httptest.NewRequest(http.MethodGet, "http://example.com/api/swr-shutdown-timeout", nil)
	req2.Header.Set("X-Is-SWR", "true")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	// 4. Close middleware with short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := mw.Close(ctx)
	close(blockOrigin) // unblock background task
	if err == nil {
		t.Fatal("expected Close to fail on context deadline exceeded, got nil")
	}
}

// Protocol & stream bypass guards (WebSocket, SSE, Range).
func TestBypassGuards_WebSocket_SSE_Range(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			w.Header().Set("Upgrade", "websocket")
			w.Header().Set("Connection", "Upgrade")
			w.WriteHeader(http.StatusSwitchingProtocols)
			return
		}
		if r.URL.Path == "/sse" {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("data: event-1\n\n"))
			return
		}
		if r.Header.Get("Range") != "" {
			w.Header().Set("Content-Range", "bytes 0-10/100")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("partial-data"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	handler := mw.testHandler(originHandler)

	// 1. WebSocket Upgrade Bypass
	doGet(t, handler, "http://example.com/ws", "Upgrade", "websocket").
		assertStatus(http.StatusSwitchingProtocols).
		assertCacheStatus("detail=websocket-upgrade")

	// 2. SSE Accept Request Header Bypass
	doGet(t, handler, "http://example.com/events", "Accept", "text/event-stream").
		assertCacheStatus("detail=sse-stream")

	// 3. SSE Content-Type Response Bypass
	doGet(t, handler, "http://example.com/sse").
		assertCacheStatus("detail=sse-response")

	// 4. Range Byte Request Bypass
	doGet(t, handler, "http://example.com/video.mp4", "Range", "bytes=0-100").
		assertStatus(http.StatusPartialContent).
		assertCacheStatus("detail=range-request")
}

// Downstream 304 validation with zero Redis body I/O.
func TestDownstream304_ZeroBodyIO(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=300")
		w.Header().Set("ETag", `"v1.0.0"`)
		w.Header().Set("Last-Modified", "Wed, 21 Oct 2026 07:28:00 GMT")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":"payload-version-1"}`))
	})

	handler := mw.testHandler(originHandler)

	// 1. Prime Cache
	doGet(t, handler, "http://example.com/api/item").
		assertStatus(http.StatusOK)

	// 2. Client sends exact matching ETag -> 304 Not Modified
	doGet(t, handler, "http://example.com/api/item", "If-None-Match", `"v1.0.0"`).
		assertStatus(http.StatusNotModified).
		assertEmptyBody().
		assertHeader("ETag", `"v1.0.0"`)

	// 3. Client sends weak ETag match -> 304 Not Modified
	doGet(t, handler, "http://example.com/api/item", "If-None-Match", `W/"v1.0.0"`).
		assertStatus(http.StatusNotModified)

	// 4. Client sends matching If-Modified-Since -> 304 Not Modified
	doGet(t, handler, "http://example.com/api/item", "If-Modified-Since", "Wed, 21 Oct 2026 07:28:00 GMT").
		assertStatus(http.StatusNotModified)
}

// Upstream 304 revalidation (TTL refresh & body retention).
func TestUpstream304_TTLRefresh(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	var originExecutions atomic.Int32
	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originExecutions.Add(1)
		if r.Header.Get("If-None-Match") == `"v2.0.0"` {
			// Origin validates cached ETag and returns 304
			w.Header().Set("Cache-Control", "public, max-age=600")
			w.Header().Set("ETag", `"v2.0.0"`)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=10")
		w.Header().Set("ETag", `"v2.0.0"`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":"upstream-payload"}`))
	})

	handler := mw.testHandler(originHandler)

	// 1. Prime Cache (Origin Call #1)
	doGet(t, handler, "http://example.com/api/refresh").
		assertBody(`{"data":"upstream-payload"}`)
	if originExecutions.Load() != 1 {
		t.Fatalf("expected 1 origin call, got %d", originExecutions.Load())
	}

	// 2. Soft-purge to trigger synchronous revalidation
	if _, err := mw.Purge(context.Background(), "http://example.com/api/refresh", WithSoftPurge()); err != nil {
		t.Fatalf("failed to soft purge: %v", err)
	}

	// 3. Next request triggers revalidation -> origin returns 304 -> Titip refreshes TTL and serves cached body
	doGet(t, handler, "http://example.com/api/refresh").
		assertStatus(http.StatusOK).
		assertBody(`{"data":"upstream-payload"}`).
		assertCacheStatus("304-refreshed")
	if originExecutions.Load() != 2 {
		t.Fatalf("expected 2 origin calls (including revalidation), got %d", originExecutions.Load())
	}

	// 4. Subsequent request is a direct Cache Hit (0 origin calls)
	doGet(t, handler, "http://example.com/api/refresh").
		assertCacheStatus("hit")
	if originExecutions.Load() != 2 {
		t.Fatalf("cache hit should not invoke origin: %d", originExecutions.Load())
	}
}

// TestFailOpen_MetadataExists_BodyEvictedGlitch verifies that if metadata exists in storage
// but the variant body key was expired/evicted in a microsecond race, Titip seamlessly fails open
// to origin without crashing, returning 500, or dropping the response.
func TestFailOpen_MetadataExists_BodyEvictedGlitch(t *testing.T) {
	t.Parallel()
	store, _, mw := setupTestTitip(t)

	var originExecutions atomic.Int32
	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := originExecutions.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `{"version":%d,"message":"origin data"}`, count)
	})

	handler := mw.testHandler(originHandler)

	targetURL := "http://example.com/api/microsecond-glitch"

	// 1. Prime cache: First request fetches origin and saves both Meta Hash and Body Key
	doGet(t, handler, targetURL).
		assertStatus(http.StatusOK)
	if originExecutions.Load() != 1 {
		t.Fatalf("expected 1 origin execution, got %d", originExecutions.Load())
	}

	// 2. Simulate microsecond glitch: Metadata is present, but GetVariant returns nil (body evicted)
	primaryKey := generatePrimaryKey(httptest.NewRequest(http.MethodGet, targetURL, nil), &CacheKey{})
	meta, _, err := store.GetMeta(context.Background(), primaryKey)
	if err != nil || meta == nil {
		t.Fatalf("expected metadata in storage, got err=%v meta=%v", err, meta)
	}

	store.SetGetVariantHook(func(ctx context.Context, pk, vk string) (*pb.VariantInfo, []byte, error) {
		return nil, nil, nil
	})

	// 3. Second request arrives during this glitch:
	// Titip should discover the missing body, fail open to origin, re-populate cache, and return 200 OK!
	doGet(t, handler, targetURL).
		assertStatus(http.StatusOK).
		assertBodyContains(`"version":2`)
	if originExecutions.Load() != 2 {
		t.Fatalf("expected 2nd origin execution due to transparent fail-open, got %d", originExecutions.Load())
	}

	// Reset hook so subsequent normal variant retrievals succeed
	store.SetGetVariantHook(nil)

	// 4. Third request: Cache should now be fully restored and hit cleanly!
	doGet(t, handler, targetURL).
		assertStatus(http.StatusOK)
	if originExecutions.Load() != 2 {
		t.Fatalf("expected 0 additional origin executions (cache hit), got %d", originExecutions.Load())
	}
}

// TestConditionalMiss_CacheWarming_AndServes304ToClient verifies that when a client sends conditional headers
// on a cold cache miss, Titip strips conditional headers to fetch the full representation from origin,
// warms the cache in storage, and responds with 304 Not Modified downstream to the client.
func TestConditionalMiss_CacheWarming_AndServes304ToClient(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	var originCalls atomic.Int32
	var originReceivedIfNoneMatch atomic.Pointer[string]

	originNow := time.Now().UTC().Truncate(time.Second)
	lastModStr := originNow.Add(-10 * time.Minute).Format(http.TimeFormat)

	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		inm := r.Header.Get("If-None-Match")
		originReceivedIfNoneMatch.Store(&inm)

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("ETag", `"v1.0.0"`)
		w.Header().Set("Last-Modified", lastModStr)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","version":"1.0.0"}`))
	})

	handler := mw.testHandler(origin)

	// 1. First Request: Cold Miss with matching If-None-Match header
	doGet(t, handler, "http://example.com/article", "If-None-Match", `"v1.0.0"`).
		assertStatus(http.StatusNotModified).
		assertEmptyBody().
		assertHeader("ETag", `"v1.0.0"`)

	// Verify origin received request WITHOUT If-None-Match (stripped to fetch full representation)
	if originCalls.Load() != 1 {
		t.Fatalf("expected 1 origin call, got %d", originCalls.Load())
	}
	if inm := originReceivedIfNoneMatch.Load(); inm == nil || *inm != "" {
		t.Errorf("expected origin to receive request with stripped If-None-Match, got %q", *inm)
	}

	// 2. Second Request: Unconditional GET from another client -> must be served from warmed cache!
	doGet(t, handler, "http://example.com/article").
		assertStatus(http.StatusOK).
		assertBody(`{"status":"ok","version":"1.0.0"}`)

	if originCalls.Load() != 1 {
		t.Fatalf("expected cache hit with 0 additional origin calls, got %d calls", originCalls.Load())
	}

	// 3. Third Request: Conditional GET matching ETag -> must be served 304 from storage with 0 origin calls
	doGet(t, handler, "http://example.com/article", "If-None-Match", `"v1.0.0"`).
		assertStatus(http.StatusNotModified)

	if originCalls.Load() != 1 {
		t.Fatalf("expected 304 hit with 0 additional origin calls, got %d calls", originCalls.Load())
	}
}

// TestConditionalMiss_MismatchingETag_Serves200WithBody verifies that when client sends an outdated ETag,
// Titip fetches fresh from origin, warms the cache, and delivers 200 OK with the new body.
func TestConditionalMiss_MismatchingETag_Serves200WithBody(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	var originCalls atomic.Int32

	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("ETag", `"v2.0.0"`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"version":"2.0.0"}`))
	})

	handler := mw.testHandler(origin)

	// Client sends outdated ETag "v1.0.0"
	doGet(t, handler, "http://example.com/resource", "If-None-Match", `"v1.0.0"`).
		assertStatus(http.StatusOK).
		assertBody(`{"version":"2.0.0"}`).
		assertHeader("ETag", `"v2.0.0"`)

	if originCalls.Load() != 1 {
		t.Fatalf("expected 1 origin call, got %d", originCalls.Load())
	}

	// Verify cache was warmed: subsequent request with "v2.0.0" gets 304 from cache
	doGet(t, handler, "http://example.com/resource", "If-None-Match", `"v2.0.0"`).
		assertStatus(http.StatusNotModified)

	if originCalls.Load() != 1 {
		t.Fatalf("expected 0 additional origin calls, got %d", originCalls.Load())
	}
}

// TestConditionalMiss_IfModifiedSince_CacheWarming verifies conditional miss handling for If-Modified-Since.
func TestConditionalMiss_IfModifiedSince_CacheWarming(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	var originCalls atomic.Int32
	var originReceivedIMS atomic.Pointer[string]

	lastModTime := time.Now().UTC().Truncate(time.Second).Add(-1 * time.Hour)
	lastModStr := lastModTime.Format(http.TimeFormat)

	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		ims := r.Header.Get("If-Modified-Since")
		originReceivedIMS.Store(&ims)

		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Cache-Control", "public, max-age=120")
		w.Header().Set("Last-Modified", lastModStr)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("unmodified text content"))
	})

	handler := mw.testHandler(origin)

	// Client sends If-Modified-Since matching or after Last-Modified
	doGet(t, handler, "http://example.com/doc", "If-Modified-Since", lastModStr).
		assertStatus(http.StatusNotModified)

	if originCalls.Load() != 1 {
		t.Fatalf("expected 1 origin call, got %d", originCalls.Load())
	}
	if ims := originReceivedIMS.Load(); ims == nil || *ims != "" {
		t.Errorf("expected stripped If-Modified-Since on origin request, got %q", *ims)
	}

	// Verify subsequent request without conditional headers is served from warmed cache
	doGet(t, handler, "http://example.com/doc").
		assertStatus(http.StatusOK).
		assertBody("unmodified text content")

	if originCalls.Load() != 1 {
		t.Fatalf("expected cache hit with 1 origin call, got %d", originCalls.Load())
	}
}

// TestConditionalMiss_UncacheableOrigin_Serves304WithoutStoring verifies that uncacheable origin responses
// deliver 304 to client if validators match, but do not populate storage.
func TestConditionalMiss_UncacheableOrigin_Serves304WithoutStoring(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	var originCalls atomic.Int32

	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "private, no-store")
		w.Header().Set("ETag", `"secret-v1"`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"private":"data"}`))
	})

	handler := mw.testHandler(origin)

	// Client sends matching ETag
	doGet(t, handler, "http://example.com/private", "If-None-Match", `"secret-v1"`).
		assertStatus(http.StatusNotModified)

	if originCalls.Load() != 1 {
		t.Fatalf("expected 1 origin call, got %d", originCalls.Load())
	}

	// Next request from another user without ETag: must hit origin again because response was uncacheable!
	doGet(t, handler, "http://example.com/private").
		assertStatus(http.StatusOK)

	if originCalls.Load() != 2 {
		t.Fatalf("expected second request to hit origin (uncacheable), got %d origin calls", originCalls.Load())
	}
}
