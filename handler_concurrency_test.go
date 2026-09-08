package titip

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// Singleflight stampede & initiator cancellation resilience on stale revalidations.
func TestSingleflight_StampedeAndInitiatorCancellation(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	var originGate chan struct{}
	var originExecutions atomic.Int32
	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		exec := originExecutions.Add(1)
		if exec == 2 && originGate != nil {
			<-originGate
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("ETag", `"etag-123"`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"message":"fresh origin data"}`))
	})

	handler := mw.testHandler(originHandler)

	// 1. Prime cache entry
	doGet(t, handler, "http://example.com/api/stampede")
	if originExecutions.Load() != 1 {
		t.Fatalf("expected 1 initial origin execution, got %d", originExecutions.Load())
	}

	// 2. Soft-purge entry to trigger synchronous singleflight revalidation
	if _, err := mw.Purge(context.Background(), "http://example.com/api/stampede", WithSoftPurge()); err != nil {
		t.Fatalf("failed to soft purge: %v", err)
	}

	const concurrentRequests = 50
	var wg sync.WaitGroup
	wg.Add(concurrentRequests)

	var activeInFlight atomic.Int32
	originGate = make(chan struct{})
	startBarrier := make(chan struct{})

	// Launch request #0 that cancels its context early
	ctx0, cancel0 := context.WithCancel(context.Background())
	go func() {
		defer wg.Done()
		<-startBarrier
		activeInFlight.Add(1)
		req0 := httptest.NewRequest(http.MethodGet, "http://example.com/api/stampede", nil).WithContext(ctx0)
		rec0 := httptest.NewRecorder()
		// Cancel at t=10ms
		time.AfterFunc(10*time.Millisecond, cancel0)
		handler.ServeHTTP(rec0, req0)
	}()

	// Launch remaining concurrent requests
	responses := make([]*httptest.ResponseRecorder, concurrentRequests-1)
	for i := range concurrentRequests - 1 {
		idx := i
		go func() {
			defer wg.Done()
			<-startBarrier
			activeInFlight.Add(1)
			req := httptest.NewRequest(http.MethodGet, "http://example.com/api/stampede", nil)
			rec := httptest.NewRecorder()
			responses[idx] = rec
			handler.ServeHTTP(rec, req)
		}()
	}

	// Release all 50 concurrent requests simultaneously
	close(startBarrier)

	// Wait until all 50 requests have entered handler and origin revalidation is in-flight
	for activeInFlight.Load() < concurrentRequests || originExecutions.Load() < 2 {
		time.Sleep(1 * time.Millisecond)
	}

	// Give adequate window for all 50 in-flight requests to complete Redis metadata lookup and enter singleflight.Do
	time.Sleep(50 * time.Millisecond)

	// Release origin execution to broadcast response
	close(originGate)

	wg.Wait()

	// Exactly 2 origin executions should have occurred: 1 initial prime + 1 coalesced revalidation
	if originExecutions.Load() != 2 {
		t.Fatalf("expected exactly 2 origin executions, got %d", originExecutions.Load())
	}

	// All other requests received full 200 OK
	for i, rec := range responses {
		if rec == nil {
			continue
		}
		if rec.Code != http.StatusOK {
			t.Errorf("request %d failed with code %d", i+1, rec.Code)
		}
		if body := rec.Body.String(); body != `{"message":"fresh origin data"}` {
			t.Errorf("request %d unexpected body: %s", i+1, body)
		}
	}

	// Request #51 gets immediate fresh cache hit
	doGet(t, handler, "http://example.com/api/stampede").
		assertStatus(http.StatusOK).
		assertCacheStatus("hit")
	if originExecutions.Load() != 2 {
		t.Fatalf("origin executions increased on request 51: %d", originExecutions.Load())
	}
}

// Soft-purge synchronous freshness with fallback.
func TestSoftPurge_SynchronousFreshnessAndFallback(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	var originFail atomic.Bool
	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if originFail.Load() {
			http.Error(w, "503 backend error", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Cache-Control", "public, max-age=60, stale-if-error=300")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("initial cached payload"))
	})

	handler := mw.testHandler(originHandler)

	// 1. Initial request populates cache
	doGet(t, handler, "http://example.com/api/soft-test").
		assertStatus(http.StatusOK)

	// 2. Soft purge URL
	if _, err := mw.Purge(context.Background(), "http://example.com/api/soft-test", WithSoftPurge()); err != nil {
		t.Fatalf("soft purge failed: %v", err)
	}

	// 3. Make origin fail with 503 -> should fallback to stale cached payload!
	originFail.Store(true)

	const concurrent = 10
	var wg sync.WaitGroup
	wg.Add(concurrent)

	for range concurrent {
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "http://example.com/api/soft-test", nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Errorf("expected 200 fallback, got %d", rec.Code)
			}
			if rec.Body.String() != "initial cached payload" {
				t.Errorf("unexpected body on fallback: %s", rec.Body.String())
			}
			if status := rec.Header().Get("Cache-Status"); !containsAny(status, "stale-if-error") && !containsAny(status, "stale") {
				t.Errorf("expected stale-if-error status, got %s", status)
			}
		}()
	}

	wg.Wait()
}

// TestSWR_AsyncRevalidation_OnHead validates that SWR triggered by HEAD revalidates upstream with GET
func TestSWR_AsyncRevalidation_OnHead(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	var currentBody atomic.Value
	currentBody.Store("swr initial payload")
	originCalls := int32(0)

	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&originCalls, 1)
		body := currentBody.Load().(string)
		w.Header().Set("Cache-Control", "public, max-age=1, stale-while-revalidate=10")
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			_, _ = w.Write([]byte(body))
		}
	})

	handler := mw.testHandler(originHandler)

	// 1. Prime cache
	doGet(t, handler, "http://example.com/api/test-swr-head")

	// Wait for entry to become stale (enter SWR window)
	time.Sleep(1100 * time.Millisecond)
	currentBody.Store("swr updated payload v2")

	// 2. HEAD request during SWR window -> serves stale hit, triggers async GET revalidation
	doHead(t, handler, "http://example.com/api/test-swr-head").
		assertStatus(http.StatusOK).
		assertEmptyBody()

	// Close mw to ensure background SWR goroutines complete
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := mw.Close(ctx); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	// 3. New Titip instance using same Redis store to verify updated body
	_, _, mw2 := setupTestTitip(t)
	handler2 := mw2.testHandler(originHandler)

	doGet(t, handler2, "http://example.com/api/test-swr-head").
		assertBody("swr updated payload v2")
}

// TestSWR_PreservesRequestContextValues verifies that custom context values (e.g. tracing IDs, replacers)
// attached to the original HTTP request are preserved and accessible during async SWR revalidation.
func TestSWR_PreservesRequestContextValues(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	type traceCtxKey struct{}
	var receivedTraceVal atomic.Value

	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if val := r.Context().Value(traceCtxKey{}); val != nil {
			receivedTraceVal.Store(val.(string))
		}
		w.Header().Set("Cache-Control", "public, max-age=1, stale-while-revalidate=10")
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("swr trace payload"))
	})

	handler := mw.testHandler(originHandler)

	// 1. Prime cache with request containing context value
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/api/test-swr-ctx", nil)
	doReq(t, handler, req1.WithContext(context.WithValue(req1.Context(), traceCtxKey{}, "trace-id-init")))

	// Wait for entry to enter SWR window
	time.Sleep(1100 * time.Millisecond)

	// 2. Stale request with a new trace context value
	req2 := httptest.NewRequest(http.MethodGet, "http://example.com/api/test-swr-ctx", nil)
	doReq(t, handler, req2.WithContext(context.WithValue(req2.Context(), traceCtxKey{}, "trace-id-swr-reval"))).
		assertStatus(http.StatusOK)

	// Close mw to ensure background SWR goroutine completes
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := mw.Close(closeCtx); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	if got := receivedTraceVal.Load(); got != "trace-id-swr-reval" {
		t.Fatalf("expected background SWR to receive trace value %q, got %v", "trace-id-swr-reval", got)
	}
}

// Cold miss session leak protection (concurrent safety & zero session broadcast).
func TestColdMiss_ConcurrentSafety_ZeroSessionLeak(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	var originExecutions atomic.Int32
	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := originExecutions.Add(1)
		time.Sleep(10 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "private, no-cache")
		w.Header().Set("Set-Cookie", fmt.Sprintf("session_id=user-%d; Path=/; HttpOnly", reqID))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(fmt.Appendf(nil, `{"user_id":%d}`, reqID))
	})

	handler := mw.testHandler(originHandler)
	const concurrentUsers = 30

	var wg sync.WaitGroup
	wg.Add(concurrentUsers)
	userCookies := make([]string, concurrentUsers)

	for i := range concurrentUsers {
		idx := i
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "http://example.com/api/login", nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			cookie := rec.Header().Get("Set-Cookie")
			userCookies[idx] = cookie
		}()
	}

	wg.Wait()

	// All 30 cold requests must have reached origin independently
	if originExecutions.Load() != concurrentUsers {
		t.Fatalf("expected %d origin executions on cold miss, got %d", concurrentUsers, originExecutions.Load())
	}

	// Verify all cookies are unique and no session is shared across users
	seenCookies := make(map[string]struct{})
	for i, cookie := range userCookies {
		if cookie == "" {
			t.Fatalf("user %d received empty cookie", i)
		}
		if _, exists := seenCookies[cookie]; exists {
			t.Fatalf("session cookie leak detected! Duplicate cookie %s received by user %d", cookie, i)
		}
		seenCookies[cookie] = struct{}{}
	}
}

// TestSynctest_ContextDetachmentAndVirtualTimers demonstrates Go 1.24+ synctest bubble
// with zero-millisecond virtual time advancement and context detachment verification.
func TestSynctest_ContextDetachmentAndVirtualTimers(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()

		detached := context.WithoutCancel(ctx)

		// Cancel the parent context
		cancel()

		if ctx.Err() == nil {
			t.Fatal("expected parent context to be canceled")
		}
		if detached.Err() != nil {
			t.Fatalf("expected detached context not to be canceled, got: %v", detached.Err())
		}

		// Virtual timer inside synctest bubble advances in 0 real milliseconds
		var executed atomic.Bool
		time.AfterFunc(5*time.Second, func() {
			executed.Store(true)
		})

		// Fast-forward synthetic time by sleeping in the bubble
		time.Sleep(5 * time.Second)
		synctest.Wait()

		if !executed.Load() {
			t.Fatal("expected virtual timer to have fired instantly in synctest bubble")
		}
	})
}
