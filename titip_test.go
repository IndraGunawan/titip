package titip

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/indragunawan/titip/internal/teststore"
	pb "github.com/indragunawan/titip/proto"
	"github.com/indragunawan/titip/storage"
)

func setupTestTitip(t testing.TB, opts ...Option) (*teststore.Store, storage.Storage, *Titip) {
	store := teststore.New()

	defaultOpts := []Option{
		WithStorage(store),
		WithOriginTimeout(10 * time.Second),
		WithStorageTimeout(10 * time.Second),
		WithCacheStatusMode(CacheStatusRFC9211),
	}
	defaultOpts = append(defaultOpts, opts...)

	mw, err := New(defaultOpts...)
	if err != nil {
		t.Fatalf("failed to create Titip middleware: %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = mw.Close(ctx)
	})

	return store, store, mw
}

func (t *Titip) testHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.ServeHTTP(w, r, next)
	})
}

// AC-1: Singleflight Stampede & Initiator Cancellation Resilience on Stale Revalidations
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
	reqPrime := httptest.NewRequest(http.MethodGet, "http://example.com/api/stampede", nil)
	recPrime := httptest.NewRecorder()
	handler.ServeHTTP(recPrime, reqPrime)
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
	req51 := httptest.NewRequest(http.MethodGet, "http://example.com/api/stampede", nil)
	rec51 := httptest.NewRecorder()
	handler.ServeHTTP(rec51, req51)

	if rec51.Code != http.StatusOK {
		t.Fatalf("request 51 expected 200 OK, got %d", rec51.Code)
	}
	if status := rec51.Header().Get("Cache-Status"); status == "" || !containsAny(status, "hit") {
		t.Fatalf("expected Cache-Status hit on request 51, got %q", status)
	}
	if originExecutions.Load() != 2 {
		t.Fatalf("origin executions increased on request 51: %d", originExecutions.Load())
	}
}

// AC-2: Soft-Purge Synchronous Freshness with Fallback
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
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/api/soft-test", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("initial request failed: %d", rec1.Code)
	}

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

// AC-3: Fail-Open on Storage Outage
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

	req := httptest.NewRequest(http.MethodGet, "http://example.com/api/fail-open", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on storage failure, got %d", rec.Code)
	}
	if rec.Body.String() != "live origin response" {
		t.Fatalf("expected live origin response, got %s", rec.Body.String())
	}
	if status := rec.Header().Get("Cache-Status"); !containsAny(status, "bypass") {
		t.Fatalf("expected bypass status header, got %s", status)
	}
}

// TestPanicRecovery_ColdMiss_NoCrash tests that an upstream panic on a cold request returns 500 without crashing
func TestPanicRecovery_ColdMiss_NoCrash(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	panickingHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("critical origin failure")
	})

	handler := mw.testHandler(panickingHandler)

	req := httptest.NewRequest(http.MethodGet, "http://example.com/api/cold-panic", nil)
	rec := httptest.NewRecorder()

	// Must not panic or crash process
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 Internal Server Error on unhandled origin panic, got %d", rec.Code)
	}
}

// AC-4: Panic Recovery with Stale Fallback
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
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/api/panic-test", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("initial request failed: %d", rec1.Code)
	}

	// 2. Soft purge
	_, _ = mw.Purge(context.Background(), "http://example.com/api/panic-test", WithSoftPurge())

	// 3. Trigger panic on origin
	shouldPanic.Store(true)

	req2 := httptest.NewRequest(http.MethodGet, "http://example.com/api/panic-test", nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK fallback from panic, got %d", rec2.Code)
	}
	if rec2.Body.String() != "panic fallback data" {
		t.Fatalf("unexpected body from panic fallback: %s", rec2.Body.String())
	}
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
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/api/conditional", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	// 2. Conditional If-None-Match
	req2 := httptest.NewRequest(http.MethodGet, "http://example.com/api/conditional", nil)
	req2.Header.Set("If-None-Match", `"v1.0"`)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusNotModified {
		t.Fatalf("expected 304 Not Modified, got %d", rec2.Code)
	}
	if rec2.Body.Len() != 0 {
		t.Fatalf("expected 0 body bytes on 304, got %d", rec2.Body.Len())
	}

	// 3. HEAD request
	reqHead := httptest.NewRequest(http.MethodHead, "http://example.com/api/conditional", nil)
	recHead := httptest.NewRecorder()
	handler.ServeHTTP(recHead, reqHead)

	if recHead.Code != http.StatusOK {
		t.Fatalf("expected 200 on HEAD, got %d", recHead.Code)
	}
	if recHead.Body.Len() != 0 {
		t.Fatalf("expected 0 body bytes on HEAD, got %d", recHead.Body.Len())
	}
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
	reqHead := httptest.NewRequest(http.MethodHead, "http://example.com/api/test-head-cold", nil)
	recHead := httptest.NewRecorder()
	handler.ServeHTTP(recHead, reqHead)

	if recHead.Code != http.StatusOK {
		t.Fatalf("expected 200 on HEAD, got %d", recHead.Code)
	}
	if recHead.Body.Len() != 0 {
		t.Fatalf("expected 0 body bytes on HEAD response, got %d", recHead.Body.Len())
	}
	if status := recHead.Header().Get("Cache-Status"); !strings.Contains(status, "stored") {
		t.Fatalf("expected Cache-Status stored on HEAD, got %q", status)
	}

	// 2. Subsequent GET request for the same URL (should be a cache HIT with full body)
	reqGet := httptest.NewRequest(http.MethodGet, "http://example.com/api/test-head-cold", nil)
	recGet := httptest.NewRecorder()
	handler.ServeHTTP(recGet, reqGet)

	if recGet.Code != http.StatusOK {
		t.Fatalf("expected 200 on GET, got %d", recGet.Code)
	}
	if got := recGet.Body.String(); got != expectedBody {
		t.Fatalf("expected GET body %q, but got %q", expectedBody, got)
	}
	if status := recGet.Header().Get("Cache-Status"); !strings.Contains(status, "hit") {
		t.Fatalf("expected Cache-Status hit on GET, got %q", status)
	}
	if calls := atomic.LoadInt32(&originCalls); calls != 1 {
		t.Fatalf("expected 1 origin call (cached), got %d", calls)
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
	reqHead := httptest.NewRequest(http.MethodHead, "http://example.com/api/test-head-optout", nil)
	recHead := httptest.NewRecorder()
	handler.ServeHTTP(recHead, reqHead)

	if recHead.Code != http.StatusOK {
		t.Fatalf("expected 200 on HEAD, got %d", recHead.Code)
	}
	if recHead.Body.Len() != 0 {
		t.Fatalf("expected 0 body bytes on HEAD response, got %d", recHead.Body.Len())
	}
	if status := recHead.Header().Get("Cache-Status"); strings.Contains(status, "stored") {
		t.Fatalf("expected Cache-Status not to contain 'stored' when ConvertHeadToGet is false, got %q", status)
	}

	// 2. Subsequent GET request for the same URL (should be a clean MISS fetching full body)
	reqGet := httptest.NewRequest(http.MethodGet, "http://example.com/api/test-head-optout", nil)
	recGet := httptest.NewRecorder()
	handler.ServeHTTP(recGet, reqGet)

	if recGet.Code != http.StatusOK {
		t.Fatalf("expected 200 on GET, got %d", recGet.Code)
	}
	if got := recGet.Body.String(); got != expectedBody {
		t.Fatalf("expected GET body %q, but got %q", expectedBody, got)
	}
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
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/api/test-reval-head", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if rec1.Body.String() != "initial payload v1" {
		t.Fatalf("unexpected body: %q", rec1.Body.String())
	}

	// Wait for TTL expiry
	time.Sleep(1100 * time.Millisecond)
	currentBody.Store("updated payload v2")

	// 2. HEAD request on expired entry -> singleflight revalidation as GET
	reqHead := httptest.NewRequest(http.MethodHead, "http://example.com/api/test-reval-head", nil)
	recHead := httptest.NewRecorder()
	handler.ServeHTTP(recHead, reqHead)

	if recHead.Code != http.StatusOK {
		t.Fatalf("expected 200 on HEAD revalidation, got %d", recHead.Code)
	}
	if recHead.Body.Len() != 0 {
		t.Fatalf("expected 0 body bytes on HEAD response, got %d", recHead.Body.Len())
	}

	// 3. GET request -> should serve fresh updated payload v2 from cache
	reqGet := httptest.NewRequest(http.MethodGet, "http://example.com/api/test-reval-head", nil)
	recGet := httptest.NewRecorder()
	handler.ServeHTTP(recGet, reqGet)

	if got := recGet.Body.String(); got != "updated payload v2" {
		t.Fatalf("expected updated body %q, got %q", "updated payload v2", got)
	}
	if status := recGet.Header().Get("Cache-Status"); !strings.Contains(status, "hit") {
		t.Fatalf("expected hit on GET, got %q", status)
	}
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
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/api/test-swr-head", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	// Wait for entry to become stale (enter SWR window)
	time.Sleep(1100 * time.Millisecond)
	currentBody.Store("swr updated payload v2")

	// 2. HEAD request during SWR window -> serves stale hit, triggers async GET revalidation
	reqHead := httptest.NewRequest(http.MethodHead, "http://example.com/api/test-swr-head", nil)
	recHead := httptest.NewRecorder()
	handler.ServeHTTP(recHead, reqHead)

	if recHead.Code != http.StatusOK {
		t.Fatalf("expected 200 on HEAD, got %d", recHead.Code)
	}
	if recHead.Body.Len() != 0 {
		t.Fatalf("expected 0 body bytes on HEAD, got %d", recHead.Body.Len())
	}

	// Close mw to ensure background SWR goroutines complete
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := mw.Close(ctx); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	// 3. New Titip instance using same Redis store to verify updated body
	_, _, mw2 := setupTestTitip(t)
	handler2 := mw2.testHandler(originHandler)

	reqGet := httptest.NewRequest(http.MethodGet, "http://example.com/api/test-swr-head", nil)
	recGet := httptest.NewRecorder()
	handler2.ServeHTTP(recGet, reqGet)

	if got := recGet.Body.String(); got != "swr updated payload v2" {
		t.Fatalf("expected updated SWR body %q, got %q", "swr updated payload v2", got)
	}
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
	reqGet := httptest.NewRequest(http.MethodGet, "http://example.com/api/state-default", nil)
	recGet1 := httptest.NewRecorder()
	handler.ServeHTTP(recGet1, reqGet)
	if recGet1.Body.String() != "state=0" {
		t.Fatalf("expected state=0, got %s", recGet1.Body.String())
	}

	// 2. Mutating POST request
	reqPost := httptest.NewRequest(http.MethodPost, "http://example.com/api/state-default", nil)
	recPost := httptest.NewRecorder()
	handler.ServeHTTP(recPost, reqPost)
	if recPost.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d", recPost.Code)
	}

	// 3. Subsequent GET must STILL return cached state=0 because auto-invalidation is disabled by default
	recGet2 := httptest.NewRecorder()
	handler.ServeHTTP(recGet2, reqGet)
	if recGet2.Body.String() != "state=0" {
		t.Fatalf("expected cached state=0 when auto-invalidation is disabled, got %s", recGet2.Body.String())
	}
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
	reqGet := httptest.NewRequest(http.MethodGet, "http://example.com/api/state-enabled", nil)
	recGet1 := httptest.NewRecorder()
	handler.ServeHTTP(recGet1, reqGet)
	if recGet1.Body.String() != "state=0" {
		t.Fatalf("expected state=0, got %s", recGet1.Body.String())
	}

	// 2. Mutating POST request
	reqPost := httptest.NewRequest(http.MethodPost, "http://example.com/api/state-enabled", nil)
	recPost := httptest.NewRecorder()
	handler.ServeHTTP(recPost, reqPost)
	if recPost.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d", recPost.Code)
	}

	// 3. Subsequent GET must fetch new state=42 because auto-invalidation is enabled
	recGet2 := httptest.NewRecorder()
	handler.ServeHTTP(recGet2, reqGet)
	if recGet2.Body.String() != "state=42" {
		t.Fatalf("expected invalidated state=42 when auto-invalidation is enabled, got %s", recGet2.Body.String())
	}
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := mw.Close(ctx); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	if revalidations.Load() != 2 {
		t.Fatalf("expected 2 revalidations, got %d", revalidations.Load())
	}
}

func BenchmarkCacheHit(b *testing.B) {
	_, _, mw := setupTestTitip(b)

	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(headerContentType, "application/json")
		w.Header().Set(headerCacheControl, "public, max-age=300")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	handler := mw.testHandler(originHandler)
	req := httptest.NewRequest(http.MethodGet, "http://example.com/bench/hit", nil)

	// Prime cache
	recPrime := httptest.NewRecorder()
	handler.ServeHTTP(recPrime, req)

	for b.Loop() {
		rec := getResponseRecorder()
		handler.ServeHTTP(rec, req)
		putResponseRecorder(rec)
	}
}

func BenchmarkMiddleware_ParallelThroughput(b *testing.B) {
	_, _, mw := setupTestTitip(b)

	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(headerCacheControl, "public, max-age=300")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":"parallel"}`))
	})

	handler := mw.testHandler(originHandler)
	req := httptest.NewRequest(http.MethodGet, "http://example.com/bench/par", nil)

	// Prime
	recPrime := httptest.NewRecorder()
	handler.ServeHTTP(recPrime, req)

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rec := getResponseRecorder()
			handler.ServeHTTP(rec, req)
			putResponseRecorder(rec)
		}
	})
}

// AC-2: Prometheus Metrics & PromQL Verification
func TestPrometheusMetrics(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	_, _, mw := setupTestTitip(t, WithMetrics(reg), WithStorageTimeout(5*time.Second))

	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("metric test data"))
	})

	handler := mw.testHandler(originHandler)

	// 20 misses (different URLs)
	for i := range 20 {
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://example.com/metric/%d", i), nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}

	// 80 hits (requesting the same primed URL 80 times)
	reqPrime := httptest.NewRequest(http.MethodGet, "http://example.com/metric/0", nil)
	for range 80 {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, reqPrime)
	}

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	var hitCount, missCount float64
	var hitHistSampleCount, missHistSampleCount uint64
	for _, mf := range mfs {
		if mf.GetName() == "titip_requests_total" {
			for _, m := range mf.GetMetric() {
				for _, label := range m.GetLabel() {
					if label.GetName() == "status" {
						if label.GetValue() == "hit" {
							hitCount += m.GetCounter().GetValue()
						} else if label.GetValue() == "miss" {
							missCount += m.GetCounter().GetValue()
						}
					}
				}
			}
		}
		if mf.GetName() == "titip_request_duration_seconds" {
			for _, m := range mf.GetMetric() {
				for _, label := range m.GetLabel() {
					if label.GetName() == "status" {
						if label.GetValue() == "hit" {
							hitHistSampleCount += m.GetHistogram().GetSampleCount()
						} else if label.GetValue() == "miss" {
							missHistSampleCount += m.GetHistogram().GetSampleCount()
						}
					}
				}
			}
		}
	}

	if hitCount != 80 {
		t.Errorf("expected 80 hits in metrics, got %v", hitCount)
	}
	if missCount != 20 {
		t.Errorf("expected 20 misses in metrics, got %v", missCount)
	}
	if hitHistSampleCount != 80 {
		t.Errorf("expected 80 hit histogram observations, got %v", hitHistSampleCount)
	}
	if missHistSampleCount != 20 {
		t.Errorf("expected 20 miss histogram observations, got %v", missHistSampleCount)
	}
}

// AC-3: Cache-Status Header Modes
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

	req := httptest.NewRequest(http.MethodGet, "http://example.com/status-test", nil)

	// Prime mw1
	h1 := mw1.testHandler(originHandler)
	rec1Prime := httptest.NewRecorder()
	h1.ServeHTTP(rec1Prime, req)

	rec1Hit := httptest.NewRecorder()
	h1.ServeHTTP(rec1Hit, req)
	if status := rec1Hit.Header().Get("Cache-Status"); !containsAny(status, "titip; hit") {
		t.Errorf("expected RFC-9211 header, got %s", status)
	}

	// Prime mw2
	h2 := mw2.testHandler(originHandler)
	rec2Prime := httptest.NewRecorder()
	h2.ServeHTTP(rec2Prime, req)

	rec2Hit := httptest.NewRecorder()
	h2.ServeHTTP(rec2Hit, req)
	if status := rec2Hit.Header().Get("Cache-Status"); status != "HIT" {
		t.Errorf("expected SimpleToken header HIT, got %s", status)
	}

	// Prime mw3
	h3 := mw3.testHandler(originHandler)
	rec3Prime := httptest.NewRecorder()
	h3.ServeHTTP(rec3Prime, req)

	rec3Hit := httptest.NewRecorder()
	h3.ServeHTTP(rec3Hit, req)
	if status := rec3Hit.Header().Get("Cache-Status"); status != "" {
		t.Errorf("expected empty Cache-Status in None mode, got %s", status)
	}
}

func containsAny(s string, sub string) bool {
	return bytes.Contains([]byte(s), []byte(sub))
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
	reqEN := httptest.NewRequest(http.MethodGet, baseURL, nil)
	reqEN.Header.Set("Accept-Language", "en-US")
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, reqEN)

	if rec1.Body.String() != `{"msg":"hello"}` {
		t.Fatalf("expected hello, got %s", rec1.Body.String())
	}
	if originExecutions.Load() != 1 {
		t.Fatalf("expected 1 origin call, got %d", originExecutions.Load())
	}
	if status := rec1.Header().Get("Cache-Status"); !containsAny(status, "fwd=uri-miss") {
		t.Errorf("expected uri-miss, got %s", status)
	}

	// 2. Second Request: English variant (Cache Hit -> 0 origin calls)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, reqEN)

	if rec2.Body.String() != `{"msg":"hello"}` {
		t.Fatalf("expected cached hello, got %s", rec2.Body.String())
	}
	if originExecutions.Load() != 1 {
		t.Fatalf("cache hit should not invoke origin: %d", originExecutions.Load())
	}
	if status := rec2.Header().Get("Cache-Status"); !containsAny(status, "hit") {
		t.Errorf("expected hit, got %s", status)
	}

	// 3. Third Request: Spanish variant (URL exists in cache, but Variant is missing -> call #2)
	reqES := httptest.NewRequest(http.MethodGet, baseURL, nil)
	reqES.Header.Set("Accept-Language", "es-ES")
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, reqES)

	if rec3.Body.String() != `{"msg":"hola"}` {
		t.Fatalf("expected hola, got %s", rec3.Body.String())
	}
	if originExecutions.Load() != 2 {
		t.Fatalf("expected 2 origin calls after new variant, got %d", originExecutions.Load())
	}
	if status := rec3.Header().Get("Cache-Status"); !containsAny(status, "fwd=vary-miss") {
		t.Errorf("expected variant miss fwd=vary-miss, got %s", status)
	}

	// 4. Fourth Request: Spanish variant (Cache Hit for Spanish -> 0 origin calls)
	rec4 := httptest.NewRecorder()
	handler.ServeHTTP(rec4, reqES)

	if rec4.Body.String() != `{"msg":"hola"}` {
		t.Fatalf("expected cached hola, got %s", rec4.Body.String())
	}
	if originExecutions.Load() != 2 {
		t.Fatalf("expected 2 origin calls, got %d", originExecutions.Load())
	}
	if status := rec4.Header().Get("Cache-Status"); !containsAny(status, "hit") {
		t.Errorf("expected hit, got %s", status)
	}

	// 5. Fifth Request: English variant again (Cache Hit for English -> 0 origin calls)
	rec5 := httptest.NewRecorder()
	handler.ServeHTTP(rec5, reqEN)

	if rec5.Body.String() != `{"msg":"hello"}` {
		t.Fatalf("expected cached hello, got %s", rec5.Body.String())
	}
	if originExecutions.Load() != 2 {
		t.Fatalf("expected 2 origin calls, got %d", originExecutions.Load())
	}

	// 6. Sixth Request: French variant (URL exists, 3rd Variant missing -> call #3)
	reqFR := httptest.NewRequest(http.MethodGet, baseURL, nil)
	reqFR.Header.Set("Accept-Language", "fr-FR")
	rec6 := httptest.NewRecorder()
	handler.ServeHTTP(rec6, reqFR)

	if rec6.Body.String() != `{"msg":"bonjour"}` {
		t.Fatalf("expected bonjour, got %s", rec6.Body.String())
	}
	if originExecutions.Load() != 3 {
		t.Fatalf("expected 3 origin calls, got %d", originExecutions.Load())
	}

	// 7. Seventh Request: French variant (Cache Hit -> 0 origin calls)
	rec7 := httptest.NewRecorder()
	handler.ServeHTTP(rec7, reqFR)

	if rec7.Body.String() != `{"msg":"bonjour"}` {
		t.Fatalf("expected cached bonjour, got %s", rec7.Body.String())
	}
	if originExecutions.Load() != 3 {
		t.Fatalf("expected 3 origin calls, got %d", originExecutions.Load())
	}
}

// AC-3: Protocol & Stream Bypass Guards (WebSocket, SSE, Range)
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
	reqWS := httptest.NewRequest(http.MethodGet, "http://example.com/ws", nil)
	reqWS.Header.Set("Upgrade", "websocket")
	recWS := httptest.NewRecorder()
	handler.ServeHTTP(recWS, reqWS)

	if recWS.Code != http.StatusSwitchingProtocols {
		t.Errorf("expected 101 Switching Protocols, got %d", recWS.Code)
	}
	if status := recWS.Header().Get("Cache-Status"); !containsAny(status, "detail=websocket-upgrade") {
		t.Errorf("expected detail=websocket-upgrade, got %s", status)
	}

	// 2. SSE Accept Request Header Bypass
	reqSSE := httptest.NewRequest(http.MethodGet, "http://example.com/events", nil)
	reqSSE.Header.Set("Accept", "text/event-stream")
	recSSE := httptest.NewRecorder()
	handler.ServeHTTP(recSSE, reqSSE)

	if status := recSSE.Header().Get("Cache-Status"); !containsAny(status, "detail=sse-stream") {
		t.Errorf("expected detail=sse-stream, got %s", status)
	}

	// 3. SSE Content-Type Response Bypass
	reqSSEResp := httptest.NewRequest(http.MethodGet, "http://example.com/sse", nil)
	recSSEResp := httptest.NewRecorder()
	handler.ServeHTTP(recSSEResp, reqSSEResp)

	if status := recSSEResp.Header().Get("Cache-Status"); !containsAny(status, "detail=sse-response") {
		t.Errorf("expected detail=sse-response, got %s", status)
	}

	// 4. Range Byte Request Bypass
	reqRange := httptest.NewRequest(http.MethodGet, "http://example.com/video.mp4", nil)
	reqRange.Header.Set("Range", "bytes=0-100")
	recRange := httptest.NewRecorder()
	handler.ServeHTTP(recRange, reqRange)

	if recRange.Code != http.StatusPartialContent {
		t.Errorf("expected 206 Partial Content, got %d", recRange.Code)
	}
	if status := recRange.Header().Get("Cache-Status"); !containsAny(status, "detail=range-request") {
		t.Errorf("expected detail=range-request, got %s", status)
	}
}

// AC-2: Cold Miss Session Leak Protection (Concurrent Safety & Zero Session Broadcast)
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

// AC-4: Downstream 304 Validation with Zero Redis Body I/O
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
	reqPrime := httptest.NewRequest(http.MethodGet, "http://example.com/api/item", nil)
	recPrime := httptest.NewRecorder()
	handler.ServeHTTP(recPrime, reqPrime)

	if recPrime.Code != http.StatusOK {
		t.Fatalf("prime request failed with code %d", recPrime.Code)
	}

	// 2. Client sends exact matching ETag -> 304 Not Modified
	reqETag := httptest.NewRequest(http.MethodGet, "http://example.com/api/item", nil)
	reqETag.Header.Set("If-None-Match", `"v1.0.0"`)
	recETag := httptest.NewRecorder()
	handler.ServeHTTP(recETag, reqETag)

	if recETag.Code != http.StatusNotModified {
		t.Fatalf("expected 304 Not Modified, got %d", recETag.Code)
	}
	if recETag.Body.Len() != 0 {
		t.Fatalf("304 response must have 0 body length, got %d", recETag.Body.Len())
	}
	if recETag.Header().Get("ETag") != `"v1.0.0"` {
		t.Errorf("expected ETag header in 304 response, got %s", recETag.Header().Get("ETag"))
	}

	// 3. Client sends weak ETag match -> 304 Not Modified
	reqWeak := httptest.NewRequest(http.MethodGet, "http://example.com/api/item", nil)
	reqWeak.Header.Set("If-None-Match", `W/"v1.0.0"`)
	recWeak := httptest.NewRecorder()
	handler.ServeHTTP(recWeak, reqWeak)

	if recWeak.Code != http.StatusNotModified {
		t.Fatalf("expected 304 Not Modified for weak ETag, got %d", recWeak.Code)
	}

	// 4. Client sends matching If-Modified-Since -> 304 Not Modified
	reqIMS := httptest.NewRequest(http.MethodGet, "http://example.com/api/item", nil)
	reqIMS.Header.Set("If-Modified-Since", "Wed, 21 Oct 2026 07:28:00 GMT")
	recIMS := httptest.NewRecorder()
	handler.ServeHTTP(recIMS, reqIMS)

	if recIMS.Code != http.StatusNotModified {
		t.Fatalf("expected 304 Not Modified for If-Modified-Since, got %d", recIMS.Code)
	}
}

// AC-4: Upstream 304 Revalidation (TTL Refresh & Body Retention)
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
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/api/refresh", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if originExecutions.Load() != 1 {
		t.Fatalf("expected 1 origin call, got %d", originExecutions.Load())
	}
	if rec1.Body.String() != `{"data":"upstream-payload"}` {
		t.Fatalf("unexpected body: %s", rec1.Body.String())
	}

	// 2. Soft-purge to trigger synchronous revalidation
	if _, err := mw.Purge(context.Background(), "http://example.com/api/refresh", WithSoftPurge()); err != nil {
		t.Fatalf("failed to soft purge: %v", err)
	}

	// 3. Next request triggers revalidation -> origin returns 304 -> Titip refreshes TTL and serves cached body
	req2 := httptest.NewRequest(http.MethodGet, "http://example.com/api/refresh", nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if originExecutions.Load() != 2 {
		t.Fatalf("expected 2 origin calls (including revalidation), got %d", originExecutions.Load())
	}
	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK after 304 revalidation, got %d", rec2.Code)
	}
	if rec2.Body.String() != `{"data":"upstream-payload"}` {
		t.Fatalf("expected cached body retained after 304 revalidation, got %s", rec2.Body.String())
	}
	if status := rec2.Header().Get("Cache-Status"); !containsAny(status, "304-refreshed") {
		t.Errorf("expected 304-refreshed status, got %s", status)
	}

	// 4. Subsequent request is a direct Cache Hit (0 origin calls)
	req3 := httptest.NewRequest(http.MethodGet, "http://example.com/api/refresh", nil)
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)

	if originExecutions.Load() != 2 {
		t.Fatalf("cache hit should not invoke origin: %d", originExecutions.Load())
	}
	if status := rec3.Header().Get("Cache-Status"); !containsAny(status, "hit") {
		t.Errorf("expected hit status, got %s", status)
	}
}

// TestCustomTagHeaderName verifies custom tag header extraction and purging
func TestCustomTagHeaderName(t *testing.T) {
	t.Parallel()
	_, store, engine := setupTestTitip(t, WithTagHeaderName("X-Custom-Tags"))

	handler := engine.testHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("X-Custom-Tags", "catalog products,electronics")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, `{"status":"ok"}`)
	}))

	// 1. Initial request to store entry with tags
	req := httptest.NewRequest(http.MethodGet, "http://example.com/api/products", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// 2. Cache Hit
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req)
	if status := rec2.Header().Get("Cache-Status"); !containsAny(status, "hit") {
		t.Fatalf("expected cache hit, got %s", status)
	}

	// 3. Purge by tag "electronics"
	if _, err := engine.PurgeTag(context.Background(), "electronics"); err != nil {
		t.Fatalf("purge tag failed: %v", err)
	}

	// 4. Request after purge must be a miss
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req)
	if status := rec3.Header().Get("Cache-Status"); !containsAny(status, "uri-miss") {
		t.Fatalf("expected cache miss after tag purge, got %s", status)
	}
	_ = store
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
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/api/rfc-headers", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec1.Code)
	}

	// 2. Sleep for 1.1 seconds so resident age is > 0
	time.Sleep(1100 * time.Millisecond)

	// 3. Cache Hit: Verify Age, Date, and Hop-by-Hop stripping
	req2 := httptest.NewRequest(http.MethodGet, "http://example.com/api/rfc-headers", nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on cache hit, got %d", rec2.Code)
	}

	// Verify Date preservation
	if d := rec2.Header().Get("Date"); d != originDate {
		t.Errorf("expected Date %q preserved from origin, got %q", originDate, d)
	}

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

	// Verify Hop-by-hop headers are stripped
	if h := rec2.Header().Get("Connection"); h != "" {
		t.Errorf("Connection hop-by-hop header should be stripped, got %q", h)
	}
	if h := rec2.Header().Get("Keep-Alive"); h != "" {
		t.Errorf("Keep-Alive hop-by-hop header should be stripped, got %q", h)
	}
	if h := rec2.Header().Get("X-Custom-Hop"); h != "" {
		t.Errorf("X-Custom-Hop connection token header should be stripped, got %q", h)
	}

	// Verify regular end-to-end header is retained
	if h := rec2.Header().Get("X-Regular-Header"); h != "keep-me" {
		t.Errorf("expected X-Regular-Header 'keep-me', got %q", h)
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
	req1 := httptest.NewRequest(http.MethodGet, targetURL, nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on prime request, got %d", rec1.Code)
	}
	if originExecutions.Load() != 1 {
		t.Fatalf("expected 1 origin execution, got %d", originExecutions.Load())
	}

	// 2. Simulate microsecond glitch: Metadata is present, but GetVariant returns nil (body evicted)
	primaryKey := generatePrimaryKey(req1, &KeyConfig{})
	meta, _, err := store.GetMeta(context.Background(), primaryKey)
	if err != nil || meta == nil {
		t.Fatalf("expected metadata in storage, got err=%v meta=%v", err, meta)
	}

	store.SetGetVariantHook(func(ctx context.Context, pk, vk string) (*pb.VariantInfo, []byte, error) {
		return nil, nil, nil
	})

	// 3. Second request arrives during this glitch:
	// Titip should discover the missing body, fail open to origin, re-populate cache, and return 200 OK!
	req2 := httptest.NewRequest(http.MethodGet, targetURL, nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK despite missing body key, got %d (body: %s)", rec2.Code, rec2.Body.String())
	}
	if originExecutions.Load() != 2 {
		t.Fatalf("expected 2nd origin execution due to transparent fail-open, got %d", originExecutions.Load())
	}
	if !strings.Contains(rec2.Body.String(), `"version":2`) {
		t.Fatalf("expected response body with version 2, got: %s", rec2.Body.String())
	}

	// Reset hook so subsequent normal variant retrievals succeed
	store.SetGetVariantHook(nil)

	// 4. Third request: Cache should now be fully restored and hit cleanly!
	req3 := httptest.NewRequest(http.MethodGet, targetURL, nil)
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)

	if rec3.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on 3rd request, got %d", rec3.Code)
	}
	if originExecutions.Load() != 2 {
		t.Fatalf("expected 0 additional origin executions (cache hit), got %d", originExecutions.Load())
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
	reqPost := httptest.NewRequest(http.MethodPost, "http://example.com/api/rfc9211", strings.NewReader(`{}`))
	recPost := httptest.NewRecorder()
	handler.ServeHTTP(recPost, reqPost)
	if cs := recPost.Header().Get("Cache-Status"); !strings.Contains(cs, "fwd=method") {
		t.Errorf("expected POST to have fwd=method, got %q", cs)
	}

	// 2. Test fwd=method on OPTIONS request
	reqOptions := httptest.NewRequest(http.MethodOptions, "http://example.com/api/rfc9211", nil)
	recOptions := httptest.NewRecorder()
	handler.ServeHTTP(recOptions, reqOptions)
	if cs := recOptions.Header().Get("Cache-Status"); !strings.Contains(cs, "fwd=method") {
		t.Errorf("expected OPTIONS to have fwd=method, got %q", cs)
	}

	// 3. Test fwd=request on client Cache-Control: no-store
	reqNoStore := httptest.NewRequest(http.MethodGet, "http://example.com/api/rfc9211", nil)
	reqNoStore.Header.Set("Cache-Control", "no-store")
	recNoStore := httptest.NewRecorder()
	handler.ServeHTTP(recNoStore, reqNoStore)
	if cs := recNoStore.Header().Get("Cache-Status"); !strings.Contains(cs, "fwd=request") {
		t.Errorf("expected no-store request to have fwd=request, got %q", cs)
	}

	// 4. Test fwd=uri-miss on fresh URL
	reqGet := httptest.NewRequest(http.MethodGet, "http://example.com/api/rfc9211", nil)
	reqGet.Header.Set("Accept-Language", "en")
	recGet := httptest.NewRecorder()
	handler.ServeHTTP(recGet, reqGet)
	if cs := recGet.Header().Get("Cache-Status"); !strings.Contains(cs, "fwd=uri-miss") || !strings.Contains(cs, "stored") {
		t.Errorf("expected URI miss with stored, got %q", cs)
	}

	// 5. Test fwd=vary-miss on new variant of existing URL
	reqGetFR := httptest.NewRequest(http.MethodGet, "http://example.com/api/rfc9211", nil)
	reqGetFR.Header.Set("Accept-Language", "fr")
	recGetFR := httptest.NewRecorder()
	handler.ServeHTTP(recGetFR, reqGetFR)
	if cs := recGetFR.Header().Get("Cache-Status"); !strings.Contains(cs, "fwd=vary-miss") {
		t.Errorf("expected variant miss to have fwd=vary-miss, got %q", cs)
	}

	// 6. Test fwd=stale on expired entry revalidation
	time.Sleep(1100 * time.Millisecond)
	reqStale := httptest.NewRequest(http.MethodGet, "http://example.com/api/rfc9211", nil)
	reqStale.Header.Set("Accept-Language", "en")
	recStale := httptest.NewRecorder()
	handler.ServeHTTP(recStale, reqStale)
	if cs := recStale.Header().Get("Cache-Status"); !strings.Contains(cs, "fwd=stale") {
		t.Errorf("expected expired revalidation to have fwd=stale, got %q", cs)
	}
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

		reqMiss := httptest.NewRequest(http.MethodGet, "http://example.com/api/simple/cacheable", nil)
		recMiss := httptest.NewRecorder()
		handler.ServeHTTP(recMiss, reqMiss)
		if cs := recMiss.Header().Get("Cache-Status"); cs != tokenMiss {
			t.Errorf("expected tokenMiss (%q), got %q", tokenMiss, cs)
		}

		reqHit := httptest.NewRequest(http.MethodGet, "http://example.com/api/simple/cacheable", nil)
		recHit := httptest.NewRecorder()
		handler.ServeHTTP(recHit, reqHit)
		if cs := recHit.Header().Get("Cache-Status"); cs != tokenHit {
			t.Errorf("expected tokenHit (%q), got %q", tokenHit, cs)
		}
	})

	t.Run("Bypass", func(t *testing.T) {
		t.Parallel()
		_, _, mw := setupTestTitip(t, WithCacheStatusMode(CacheStatusSimpleToken), WithRespectClientCacheControl())
		handler := mw.testHandler(originHandler)

		reqBypassPost := httptest.NewRequest(http.MethodPost, "http://example.com/api/simple/cacheable", strings.NewReader(`{}`))
		recBypassPost := httptest.NewRecorder()
		handler.ServeHTTP(recBypassPost, reqBypassPost)
		if cs := recBypassPost.Header().Get("Cache-Status"); cs != tokenBypass {
			t.Errorf("expected tokenBypass on POST (%q), got %q", tokenBypass, cs)
		}

		reqBypassNoStore := httptest.NewRequest(http.MethodGet, "http://example.com/api/simple/cacheable", nil)
		reqBypassNoStore.Header.Set("Cache-Control", "no-store")
		recBypassNoStore := httptest.NewRecorder()
		handler.ServeHTTP(recBypassNoStore, reqBypassNoStore)
		if cs := recBypassNoStore.Header().Get("Cache-Status"); cs != tokenBypass {
			t.Errorf("expected tokenBypass on no-store (%q), got %q", tokenBypass, cs)
		}
	})

	t.Run("Dynamic", func(t *testing.T) {
		t.Parallel()
		_, _, mw := setupTestTitip(t, WithCacheStatusMode(CacheStatusSimpleToken))
		handler := mw.testHandler(originHandler)

		reqDynamic := httptest.NewRequest(http.MethodGet, "http://example.com/api/simple/dynamic", nil)
		recDynamic := httptest.NewRecorder()
		handler.ServeHTTP(recDynamic, reqDynamic)
		if cs := recDynamic.Header().Get("Cache-Status"); cs != tokenDynamic {
			t.Errorf("expected tokenDynamic (%q), got %q", tokenDynamic, cs)
		}
	})

	t.Run("Updating", func(t *testing.T) {
		t.Parallel()
		_, _, mw := setupTestTitip(t, WithCacheStatusMode(CacheStatusSimpleToken))
		handler := mw.testHandler(originHandler)

		reqSWR := httptest.NewRequest(http.MethodGet, "http://example.com/api/simple/swr", nil)
		recSWR1 := httptest.NewRecorder()
		handler.ServeHTTP(recSWR1, reqSWR)
		time.Sleep(1100 * time.Millisecond)
		recSWR2 := httptest.NewRecorder()
		handler.ServeHTTP(recSWR2, reqSWR)
		if cs := recSWR2.Header().Get("Cache-Status"); cs != tokenUpdating {
			t.Errorf("expected tokenUpdating (%q), got %q", tokenUpdating, cs)
		}
	})

	t.Run("Revalidated", func(t *testing.T) {
		t.Parallel()
		_, _, mw := setupTestTitip(t, WithCacheStatusMode(CacheStatusSimpleToken))
		handler := mw.testHandler(originHandler)

		reqReval := httptest.NewRequest(http.MethodGet, "http://example.com/api/simple/reval", nil)
		recReval1 := httptest.NewRecorder()
		handler.ServeHTTP(recReval1, reqReval)
		time.Sleep(1100 * time.Millisecond)
		recReval2 := httptest.NewRecorder()
		handler.ServeHTTP(recReval2, reqReval)
		if cs := recReval2.Header().Get("Cache-Status"); cs != tokenRevalidated {
			t.Errorf("expected tokenRevalidated (%q), got %q", tokenRevalidated, cs)
		}
	})

	t.Run("Expired", func(t *testing.T) {
		t.Parallel()
		_, _, mw := setupTestTitip(t, WithCacheStatusMode(CacheStatusSimpleToken))
		handler := mw.testHandler(originHandler)

		reqExp := httptest.NewRequest(http.MethodGet, "http://example.com/api/simple/expired", nil)
		recExp1 := httptest.NewRecorder()
		handler.ServeHTTP(recExp1, reqExp)
		time.Sleep(1100 * time.Millisecond)
		recExp2 := httptest.NewRecorder()
		handler.ServeHTTP(recExp2, reqExp)
		if cs := recExp2.Header().Get("Cache-Status"); cs != tokenExpired {
			t.Errorf("expected tokenExpired (%q), got %q", tokenExpired, cs)
		}
	})

	t.Run("Stale", func(t *testing.T) {
		t.Parallel()
		_, _, mw := setupTestTitip(t, WithCacheStatusMode(CacheStatusSimpleToken))
		handler := mw.testHandler(originHandler)

		reqFailover := httptest.NewRequest(http.MethodGet, "http://example.com/api/simple/failover", nil)
		recFailover1 := httptest.NewRecorder()
		handler.ServeHTTP(recFailover1, reqFailover)
		time.Sleep(1100 * time.Millisecond)
		recFailover2 := httptest.NewRecorder()
		handler.ServeHTTP(recFailover2, reqFailover)
		if cs := recFailover2.Header().Get("Cache-Status"); cs != tokenStale {
			t.Errorf("expected tokenStale (%q), got %q", tokenStale, cs)
		}
		if recFailover2.Code != http.StatusOK {
			t.Errorf("expected 200 OK from stale-if-error fallback, got %d", recFailover2.Code)
		}
	})
}

func TestNew_MissingStorage(t *testing.T) {
	t.Parallel()
	_, err := New()
	if err == nil {
		t.Fatal("expected error when creating Titip without storage, got nil")
	}
	expectedMsg := "titip: storage is required"
	if err.Error() != expectedMsg {
		t.Fatalf("expected error message %q, got %q", expectedMsg, err.Error())
	}
}

func TestNew_MinimalOptions(t *testing.T) {
	t.Parallel()
	store := teststore.New()

	// Initialize with ONLY the single required option
	mw, err := New(WithStorage(store))
	if err != nil {
		t.Fatalf("failed to initialize Titip with minimal options: %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = mw.Close(ctx)
	})

	// 1. Verify default configuration values
	if mw.cfg.cacheStatusMode != CacheStatusSimpleToken {
		t.Errorf("expected CacheStatusSimpleToken (%v), got %v", CacheStatusSimpleToken, mw.cfg.cacheStatusMode)
	}
	if mw.cfg.respectClientCacheControl {
		t.Errorf("expected RespectClientCacheControl to be false by default")
	}
	if mw.cfg.tagHeaderName != headerCacheTag {
		t.Errorf("expected TagHeaderName %q, got %q", headerCacheTag, mw.cfg.tagHeaderName)
	}
	if mw.cfg.originTimeout != 30*time.Second {
		t.Errorf("expected OriginTimeout 30s, got %v", mw.cfg.originTimeout)
	}
	if mw.cfg.storageTimeout != 1*time.Second {
		t.Errorf("expected StorageTimeout 1s, got %v", mw.cfg.storageTimeout)
	}
	if mw.logger == nil {
		t.Errorf("expected non-nil default logger")
	}
	if mw.cfg.esi.MaxDepth != 3 {
		t.Errorf("expected ESI MaxDepth 3, got %d", mw.cfg.esi.MaxDepth)
	}
	if mw.cfg.esi.MaxTimeout != 30*time.Second {
		t.Errorf("expected ESI MaxTimeout 30s, got %v", mw.cfg.esi.MaxTimeout)
	}
	if mw.cfg.esi.MaxConcurrentRequests != 8 {
		t.Errorf("expected ESI MaxConcurrentRequests 8, got %d", mw.cfg.esi.MaxConcurrentRequests)
	}
	if mw.cfg.esi.MaxResponseSize != 10*1024*1024 {
		t.Errorf("expected ESI MaxResponseSize 10MB, got %d", mw.cfg.esi.MaxResponseSize)
	}

	// 2. Execute live HTTP request lifecycle
	var originCalls atomic.Int32
	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("minimal option payload"))
	})

	req := httptest.NewRequest(http.MethodGet, "http://example.com/api/minimal", nil)

	// 1st request: Cold Miss
	rec1 := httptest.NewRecorder()
	mw.ServeHTTP(rec1, req, originHandler)
	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on cold miss, got %d", rec1.Code)
	}
	if cs := rec1.Header().Get("Cache-Status"); cs != tokenMiss {
		t.Errorf("expected Cache-Status %q on cold miss, got %q", tokenMiss, cs)
	}
	if originCalls.Load() != 1 {
		t.Errorf("expected 1 origin call, got %d", originCalls.Load())
	}

	// 2nd request: Cache Hit
	rec2 := httptest.NewRecorder()
	mw.ServeHTTP(rec2, req, originHandler)
	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on cache hit, got %d", rec2.Code)
	}
	if cs := rec2.Header().Get("Cache-Status"); cs != tokenHit {
		t.Errorf("expected Cache-Status %q on cache hit, got %q", tokenHit, cs)
	}
	if originCalls.Load() != 1 {
		t.Errorf("expected still 1 origin call on hit, got %d", originCalls.Load())
	}

	// 3. Verify Purge and Close
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if _, err := mw.Purge(ctx, "http://example.com/api/minimal"); err != nil {
		t.Errorf("expected Purge to succeed on minimal instance, got %v", err)
	}
	if err := mw.Close(ctx); err != nil {
		t.Errorf("expected Close to succeed on minimal instance, got %v", err)
	}
}

func TestNew_NilOptionGuards(t *testing.T) {
	t.Parallel()
	store := teststore.New()

	// Initialize with explicit nil pointers
	mw, err := New(
		WithStorage(store),
		WithLogger(nil),
		WithMetrics(nil),
	)
	if err != nil {
		t.Fatalf("expected New with nil option guards to succeed, got %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = mw.Close(ctx)
	})
	if mw.logger == nil {
		t.Fatal("expected mw.logger to fallback to slog.Default() when passed nil")
	}
	if mw.metrics != nil {
		t.Fatal("expected mw.metrics to be nil when passed nil Registerer")
	}

	// Verify executing request with nil logger/metrics does not panic
	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("nil-safe payload"))
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://example.com/api/nil-safe", nil)
	mw.ServeHTTP(rec, req, origin)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", rec.Code)
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
			w.Write([]byte("auth-secret-payload"))
		case "/auth-public":
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.Write([]byte("auth-public-payload"))
		case "/auth-s-maxage":
			w.Header().Set("Cache-Control", "s-maxage=60")
			w.Write([]byte("auth-s-maxage-payload"))
		}
	})
	handler := mw.testHandler(origin)

	// 1. Request with Authorization and origin returning only max-age=60 MUST NOT be cached
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/auth-private", nil)
	req1.Header.Set("Authorization", "Bearer user-token")
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec1.Code)
	}

	// Subsequent unauthenticated request MUST NOT receive cached response
	req2 := httptest.NewRequest(http.MethodGet, "http://example.com/auth-private", nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if cs := rec2.Header().Get("Cache-Status"); strings.Contains(cs, "hit") {
		t.Fatalf("security violation: unauthenticated request served cached private auth response: %s", cs)
	}
	if originCalls.Load() != 2 {
		t.Fatalf("expected 2 origin calls for unshared auth, got %d", originCalls.Load())
	}

	// 2. Request with Authorization and origin returning public MUST be cached
	req3 := httptest.NewRequest(http.MethodGet, "http://example.com/auth-public", nil)
	req3.Header.Set("Authorization", "Bearer user-token")
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)

	req4 := httptest.NewRequest(http.MethodGet, "http://example.com/auth-public", nil)
	rec4 := httptest.NewRecorder()
	handler.ServeHTTP(rec4, req4)
	if cs := rec4.Header().Get("Cache-Status"); !strings.Contains(cs, "hit") {
		t.Fatalf("expected cache hit for public auth response, got: %s", cs)
	}
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
		w.Write([]byte("revalidate-data"))
	})
	handler := mw.testHandler(origin)

	req := httptest.NewRequest(http.MethodGet, "http://example.com/must-reval", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req)

	// Wait for entry to become stale
	time.Sleep(1100 * time.Millisecond)

	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req)

	// Must NOT serve SWR
	if cs := rec2.Header().Get("Cache-Status"); strings.Contains(cs, "detail=swr") {
		t.Fatalf("expected must-revalidate to forbid SWR, got status: %s", cs)
	}
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
		w.Write([]byte("vary-star-data"))
	})
	handler := mw.testHandler(origin)

	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/vary-star", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	req2 := httptest.NewRequest(http.MethodGet, "http://example.com/vary-star", nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if cs := rec2.Header().Get("Cache-Status"); strings.Contains(cs, "hit") {
		t.Fatalf("expected Vary: * response to never be served as hit, got: %s", cs)
	}
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
		w.Write([]byte("upstream-age-data"))
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
		w.Write([]byte("client-cc-data"))
	})
	handler := mw.testHandler(origin)

	// 1. Initial request populates cache
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/client-cc", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if originCalls.Load() != 1 {
		t.Fatalf("expected 1 origin call, got %d", originCalls.Load())
	}

	// 2. Pragma: no-cache forces origin bypass (RFC 9111 §5.4)
	req2 := httptest.NewRequest(http.MethodGet, "http://example.com/client-cc", nil)
	req2.Header.Set("Pragma", "no-cache")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if cs := rec2.Header().Get("Cache-Status"); !strings.Contains(cs, "pragma-no-cache") {
		t.Fatalf("expected pragma-no-cache bypass status, got: %s", cs)
	}
	if originCalls.Load() != 2 {
		t.Fatalf("expected 2 origin calls after Pragma: no-cache, got %d", originCalls.Load())
	}

	// 3. Cache-Control: max-age=0 forces revalidation / bypass (RFC 9111 §5.2.1.1)
	req3 := httptest.NewRequest(http.MethodGet, "http://example.com/client-cc", nil)
	req3.Header.Set("Cache-Control", "max-age=0")
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)
	if cs := rec3.Header().Get("Cache-Status"); !strings.Contains(cs, "max-age-0") {
		t.Fatalf("expected max-age-0 bypass status, got: %s", cs)
	}

	// 4. only-if-cached on a missing URL returns 504 Gateway Timeout (RFC 9111 §5.2.1.7)
	reqMiss := httptest.NewRequest(http.MethodGet, "http://example.com/missing-url", nil)
	reqMiss.Header.Set("Cache-Control", "only-if-cached")
	recMiss := httptest.NewRecorder()
	handler.ServeHTTP(recMiss, reqMiss)
	if recMiss.Code != http.StatusGatewayTimeout {
		t.Fatalf("expected 504 Gateway Timeout for only-if-cached on miss, got %d", recMiss.Code)
	}
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
			w.Write([]byte("resource-1-data"))
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == "/resource/update" {
			w.Header().Set("Location", "http://example.com/resource/1")
			w.WriteHeader(http.StatusCreated)
			w.Write([]byte("created"))
			return
		}
	})
	handler := mw.testHandler(origin)

	// Step 1: Cache GET /resource/1
	reqGet := httptest.NewRequest(http.MethodGet, "http://example.com/resource/1", nil)
	recGet1 := httptest.NewRecorder()
	handler.ServeHTTP(recGet1, reqGet)
	if getCalls.Load() != 1 {
		t.Fatalf("expected 1 get call, got %d", getCalls.Load())
	}

	// Verify it is cached
	recGet2 := httptest.NewRecorder()
	handler.ServeHTTP(recGet2, reqGet)
	if cs := recGet2.Header().Get("Cache-Status"); !strings.Contains(cs, "hit") {
		t.Fatalf("expected cache hit, got: %s", cs)
	}
	if getCalls.Load() != 1 {
		t.Fatalf("expected still 1 get call, got %d", getCalls.Load())
	}

	// Step 2: POST /resource/update with Location: http://example.com/resource/1 (RFC 9111 §4.4)
	reqPost := httptest.NewRequest(http.MethodPost, "http://example.com/resource/update", nil)
	recPost := httptest.NewRecorder()
	handler.ServeHTTP(recPost, reqPost)
	if recPost.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d", recPost.Code)
	}

	// Step 3: GET /resource/1 must now be invalidated and fetch origin
	recGet3 := httptest.NewRecorder()
	handler.ServeHTTP(recGet3, reqGet)
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
		w.Write([]byte("ims-data"))
	})
	handler := mw.testHandler(origin)

	// Populate cache
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/ims-test", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	// Client sends IMS matching exact second
	req2 := httptest.NewRequest(http.MethodGet, "http://example.com/ims-test", nil)
	req2.Header.Set("If-Modified-Since", lmTime.Truncate(time.Second).Format(http.TimeFormat))
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusNotModified {
		t.Fatalf("expected 304 Not Modified, got %d", rec2.Code)
	}
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
		w.Write([]byte("expires-only-data"))
	})
	handler := mw.testHandler(origin)

	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/expires-only", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if originCalls.Load() != 1 {
		t.Fatalf("expected 1 origin call, got %d", originCalls.Load())
	}

	req2 := httptest.NewRequest(http.MethodGet, "http://example.com/expires-only", nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if cs := rec2.Header().Get("Cache-Status"); !strings.Contains(cs, "hit") {
		t.Fatalf("expected cache hit for Expires-only response, got: %s", cs)
	}
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
		w.Write([]byte("lang=" + lang + ",enc=" + enc))
	})
	handler := mw.testHandler(origin)

	// 1. Request with en, gzip -> MISS
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/multi-vary", nil)
	req1.Header.Set("Accept-Language", "en")
	req1.Header.Set("Accept-Encoding", "gzip")
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if rec1.Body.String() != "lang=en,enc=gzip" {
		t.Fatalf("unexpected body: %s", rec1.Body.String())
	}
	if originCalls.Load() != 1 {
		t.Fatalf("expected 1 origin call, got %d", originCalls.Load())
	}

	// 2. Same variant -> HIT
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req1)
	if !strings.Contains(rec2.Header().Get("Cache-Status"), "hit") {
		t.Fatalf("expected hit, got: %s", rec2.Header().Get("Cache-Status"))
	}
	if originCalls.Load() != 1 {
		t.Fatalf("expected cached hit, got %d origin calls", originCalls.Load())
	}

	// 3. Different language (fr) -> MISS (different variant due to 1st Vary header)
	reqFr := httptest.NewRequest(http.MethodGet, "http://example.com/multi-vary", nil)
	reqFr.Header.Set("Accept-Language", "fr")
	reqFr.Header.Set("Accept-Encoding", "gzip")
	recFr := httptest.NewRecorder()
	handler.ServeHTTP(recFr, reqFr)
	if recFr.Body.String() != "lang=fr,enc=gzip" {
		t.Fatalf("unexpected body for fr: %s", recFr.Body.String())
	}
	if originCalls.Load() != 2 {
		t.Fatalf("expected 2 origin calls, got %d", originCalls.Load())
	}

	// 4. Different encoding (br) -> MISS (different variant due to 2nd Vary header)
	reqBr := httptest.NewRequest(http.MethodGet, "http://example.com/multi-vary", nil)
	reqBr.Header.Set("Accept-Language", "en")
	reqBr.Header.Set("Accept-Encoding", "br")
	recBr := httptest.NewRecorder()
	handler.ServeHTTP(recBr, reqBr)
	if recBr.Body.String() != "lang=en,enc=br" {
		t.Fatalf("unexpected body for br: %s", recBr.Body.String())
	}
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
			w.Write([]byte("multi-cc-hit-data"))
		})
		handler := mw.testHandler(origin)

		// 1. Initial request -> MISS & Cache
		req1 := httptest.NewRequest(http.MethodGet, "http://example.com/multi-cc-hit", nil)
		rec1 := httptest.NewRecorder()
		handler.ServeHTTP(rec1, req1)
		if originCalls.Load() != 1 {
			t.Fatalf("expected 1 origin call, got %d", originCalls.Load())
		}

		// 2. Second request -> HIT
		rec2 := httptest.NewRecorder()
		handler.ServeHTTP(rec2, req1)
		if !strings.Contains(rec2.Header().Get("Cache-Status"), "hit") {
			t.Fatalf("expected cache hit for combined Cache-Control, got: %s", rec2.Header().Get("Cache-Status"))
		}
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
			w.Write([]byte("multi-cc-private-data"))
		})
		handler := mw.testHandler(origin)

		// 1. Initial request -> MISS (and should not be stored)
		req1 := httptest.NewRequest(http.MethodGet, "http://example.com/multi-cc-private", nil)
		rec1 := httptest.NewRecorder()
		handler.ServeHTTP(rec1, req1)
		if originCalls.Load() != 1 {
			t.Fatalf("expected 1 origin call, got %d", originCalls.Load())
		}

		// 2. Second request -> must still be MISS because private prevented caching
		rec2 := httptest.NewRecorder()
		handler.ServeHTTP(rec2, req1)
		if strings.Contains(rec2.Header().Get("Cache-Status"), "hit") {
			t.Fatalf("expected MISS on second call when private is present in 2nd CC header, got: %s", rec2.Header().Get("Cache-Status"))
		}
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
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/expired-reval", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK || originCalls.Load() != 1 {
		t.Fatalf("expected initial 200 OK from origin, got code %d, calls %d", rec1.Code, originCalls.Load())
	}

	// 2. Wait 1.1s for cache to expire
	time.Sleep(1100 * time.Millisecond)

	// 3. Client sends conditional request with If-None-Match on expired cache
	// Must NOT return 304 directly without revalidating with origin (RFC 9111 §4.3.2)
	req2 := httptest.NewRequest(http.MethodGet, "http://example.com/expired-reval", nil)
	req2.Header.Set("If-None-Match", `"v1"`)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if originCalls.Load() != 2 {
		t.Fatalf("expected origin to be revalidated on expired cache, but origin calls = %d", originCalls.Load())
	}
	if rec2.Code != http.StatusNotModified {
		t.Fatalf("expected 304 Not Modified after origin revalidation confirmation, got %d", rec2.Code)
	}
	if rec2.Header().Get("Age") == "" {
		t.Fatalf("expected Age header on 304 response, got headers: %#v, code: %d", rec2.Header(), rec2.Code)
	}

	// 4. Third request within refreshed TTL (60s) gets fresh cache HIT with 0 origin calls
	req3 := httptest.NewRequest(http.MethodGet, "http://example.com/expired-reval", nil)
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)
	if originCalls.Load() != 2 {
		t.Fatalf("expected 0 additional origin calls on fresh hit, got %d", originCalls.Load())
	}
	if rec3.Code != http.StatusOK || rec3.Body.String() != "initial version" {
		t.Fatalf("unexpected body on fresh hit after 304 revalidation: %s", rec3.Body.String())
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
	reqPrime := httptest.NewRequest(http.MethodGet, "http://example.com/preconditions", nil)
	recPrime := httptest.NewRecorder()
	handler.ServeHTTP(recPrime, reqPrime)
	if recPrime.Code != http.StatusOK {
		t.Fatalf("prime failed: %d", recPrime.Code)
	}

	// 2. If-Match: matching strong ETag -> 200 OK
	reqMatch := httptest.NewRequest(http.MethodGet, "http://example.com/preconditions", nil)
	reqMatch.Header.Set("If-Match", `"strong-etag-1"`)
	recMatch := httptest.NewRecorder()
	handler.ServeHTTP(recMatch, reqMatch)
	if recMatch.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on matching If-Match, got %d", recMatch.Code)
	}

	// 3. If-Match: non-matching ETag -> 412 Precondition Failed (RFC 9110 §13.1.1)
	reqNoMatch := httptest.NewRequest(http.MethodGet, "http://example.com/preconditions", nil)
	reqNoMatch.Header.Set("If-Match", `"other-etag"`)
	recNoMatch := httptest.NewRecorder()
	handler.ServeHTTP(recNoMatch, reqNoMatch)
	if recNoMatch.Code != http.StatusPreconditionFailed {
		t.Fatalf("expected 412 Precondition Failed on mismatched If-Match, got %d", recNoMatch.Code)
	}

	// 4. If-Match: weak ETag with strong comparison requirement -> 412 Precondition Failed
	reqWeakMatch := httptest.NewRequest(http.MethodGet, "http://example.com/preconditions", nil)
	reqWeakMatch.Header.Set("If-Match", `W/"strong-etag-1"`)
	recWeakMatch := httptest.NewRecorder()
	handler.ServeHTTP(recWeakMatch, reqWeakMatch)
	if recWeakMatch.Code != http.StatusPreconditionFailed {
		t.Fatalf("expected 412 Precondition Failed on weak If-Match, got %d", recWeakMatch.Code)
	}

	// 5. If-Unmodified-Since: after Last-Modified date -> 200 OK
	reqUnmodAfter := httptest.NewRequest(http.MethodGet, "http://example.com/preconditions", nil)
	reqUnmodAfter.Header.Set("If-Unmodified-Since", lastMod.Add(1*time.Hour).Format(http.TimeFormat))
	recUnmodAfter := httptest.NewRecorder()
	handler.ServeHTTP(recUnmodAfter, reqUnmodAfter)
	if recUnmodAfter.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on valid If-Unmodified-Since, got %d", recUnmodAfter.Code)
	}

	// 6. If-Unmodified-Since: before Last-Modified date -> 412 Precondition Failed (RFC 9110 §13.1.4)
	reqUnmodBefore := httptest.NewRequest(http.MethodGet, "http://example.com/preconditions", nil)
	reqUnmodBefore.Header.Set("If-Unmodified-Since", lastMod.Add(-1*time.Hour).Format(http.TimeFormat))
	recUnmodBefore := httptest.NewRecorder()
	handler.ServeHTTP(recUnmodBefore, reqUnmodBefore)
	if recUnmodBefore.Code != http.StatusPreconditionFailed {
		t.Fatalf("expected 412 Precondition Failed on expired If-Unmodified-Since, got %d", recUnmodBefore.Code)
	}
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
	reqPrime := httptest.NewRequest(http.MethodGet, "http://example.com/upstream-304-merge", nil)
	recPrime := httptest.NewRecorder()
	handler.ServeHTTP(recPrime, reqPrime)
	if originCalls.Load() != 1 {
		t.Fatalf("expected 1 prime call, got %d", originCalls.Load())
	}

	// 2. Soft-purge to force revalidation
	if _, err := mw.Purge(context.Background(), "http://example.com/upstream-304-merge", WithSoftPurge()); err != nil {
		t.Fatalf("soft purge failed: %v", err)
	}

	// 3. Revalidation request -> triggers origin 304 without Cache-Control
	reqReval := httptest.NewRequest(http.MethodGet, "http://example.com/upstream-304-merge", nil)
	recReval := httptest.NewRecorder()
	handler.ServeHTTP(recReval, reqReval)
	if originCalls.Load() != 2 {
		t.Fatalf("expected origin revalidation call, got %d", originCalls.Load())
	}
	if recReval.Code != http.StatusOK || recReval.Body.String() != "stable-body" {
		t.Fatalf("expected 200 OK with stable-body, got %d: %s", recReval.Code, recReval.Body.String())
	}

	// 4. Stored Cache-Control must have refreshed Redis TTL, so next request is a fresh HIT
	reqHit := httptest.NewRequest(http.MethodGet, "http://example.com/upstream-304-merge", nil)
	recHit := httptest.NewRecorder()
	handler.ServeHTTP(recHit, reqHit)
	if originCalls.Load() != 2 {
		t.Fatalf("expected 0 origin calls on subsequent fresh HIT, got %d calls", originCalls.Load())
	}
	if !strings.Contains(recHit.Header().Get("Cache-Status"), "hit") {
		t.Fatalf("expected hit status, got %s", recHit.Header().Get("Cache-Status"))
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
	recPrimeB := httptest.NewRecorder()
	handler.ServeHTTP(recPrimeB, reqPrimeB)
	if hostBCalls.Load() != 1 {
		t.Fatalf("expected 1 call to tenant B, got %d", hostBCalls.Load())
	}

	// 2. Perform mutating POST on tenant-a.com with Location: http://tenant-b.com/api/products
	reqMutateA := httptest.NewRequest(http.MethodPost, "http://tenant-a.com/api/orders", strings.NewReader(`{"order":1}`))
	recMutateA := httptest.NewRecorder()
	handler.ServeHTTP(recMutateA, reqMutateA)
	if recMutateA.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created on tenant A, got %d", recMutateA.Code)
	}

	// 3. Request tenant-b.com/api/products again -> must still be a cache HIT (0 new calls to origin)
	reqCheckB := httptest.NewRequest(http.MethodGet, "http://tenant-b.com/api/products", nil)
	recCheckB := httptest.NewRecorder()
	handler.ServeHTTP(recCheckB, reqCheckB)
	if hostBCalls.Load() != 1 {
		t.Fatalf("cross-host Location header purged tenant B cache! origin calls = %d", hostBCalls.Load())
	}
	if !strings.Contains(recCheckB.Header().Get("Cache-Status"), "hit") {
		t.Fatalf("expected cache HIT for tenant B, got %s", recCheckB.Header().Get("Cache-Status"))
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
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/profile", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if rec1.Header().Get("Set-Cookie") == "" {
		t.Fatal("expected Set-Cookie on initial origin response")
	}

	// 2. Second request from different user must NOT be served from cache
	// (Set-Cookie prevented caching entirely to eliminate data leaks)
	req2 := httptest.NewRequest(http.MethodGet, "http://example.com/profile", nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if originCalls.Load() != 2 {
		t.Fatalf("expected Set-Cookie response to bypass cache (2 origin calls), got %d", originCalls.Load())
	}
	if strings.Contains(rec2.Header().Get("Cache-Status"), "hit") {
		t.Fatalf("response with Set-Cookie was cached! Cache-Status: %s", rec2.Header().Get("Cache-Status"))
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
		req1 := httptest.NewRequest(http.MethodGet, "http://example.com/multi-cache-chain", nil)
		rec1 := httptest.NewRecorder()
		handler.ServeHTTP(rec1, req1)

		statuses := rec1.Header().Values("Cache-Status")
		joined := strings.Join(statuses, ", ")
		if !strings.Contains(joined, `"Fastly"; hit`) || !strings.Contains(joined, "titip;") {
			t.Fatalf("expected both Fastly and titip in Cache-Status, got: %v", statuses)
		}

		// Subsequent request (HIT)
		req2 := httptest.NewRequest(http.MethodGet, "http://example.com/multi-cache-chain", nil)
		rec2 := httptest.NewRecorder()
		handler.ServeHTTP(rec2, req2)

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
		req1 := httptest.NewRequest(http.MethodGet, "http://example.com/multi-cache-simple", nil)
		rec1 := httptest.NewRecorder()
		handler.ServeHTTP(rec1, req1)

		if rec1.Header().Get("Cache-Status") != "MISS" {
			t.Fatalf("expected simple token MISS to overwrite upstream header, got: %s", rec1.Header().Get("Cache-Status"))
		}

		// Subsequent request -> HIT
		req2 := httptest.NewRequest(http.MethodGet, "http://example.com/multi-cache-simple", nil)
		rec2 := httptest.NewRecorder()
		handler.ServeHTTP(rec2, req2)

		if rec2.Header().Get("Cache-Status") != "HIT" {
			t.Fatalf("expected simple token HIT to overwrite upstream header, got: %s", rec2.Header().Get("Cache-Status"))
		}
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
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/profile", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK || rec1.Body.String() != "<html>Resource Profile Content</html>" {
		t.Fatalf("expected initial 200 OK with profile HTML, got code %d: %s", rec1.Code, rec1.Body.String())
	}
	if originCalls.Load() != 1 {
		t.Fatalf("expected 1 origin call, got %d", originCalls.Load())
	}

	// Step 2: Second request -> because of no-cache, must synchronously revalidate with origin
	req2 := httptest.NewRequest(http.MethodGet, "http://example.com/profile", nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if originCalls.Load() != 2 {
		t.Fatalf("expected second request to revalidate with origin (2 calls), got %d", originCalls.Load())
	}
	if rec2.Code != http.StatusOK || rec2.Body.String() != "<html>Resource Profile Content</html>" {
		t.Fatalf("expected 200 OK from 304 refreshed cached body, got code %d: %s", rec2.Code, rec2.Body.String())
	}
	if !strings.Contains(rec2.Header().Get("Cache-Status"), "304") {
		t.Fatalf("expected 304-refreshed cache status, got: %s", rec2.Header().Get("Cache-Status"))
	}

	// Step 3: Origin server crashes (500 Internal Server Error) -> stale-if-error fallback
	originShouldFail.Store(true)

	req3 := httptest.NewRequest(http.MethodGet, "http://example.com/profile", nil)
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)

	if originCalls.Load() != 3 {
		t.Fatalf("expected third request to attempt origin revalidation (3 calls), got %d", originCalls.Load())
	}
	// Client must receive 200 OK with cached profile HTML (never 500 error!)
	if rec3.Code != http.StatusOK || rec3.Body.String() != "<html>Resource Profile Content</html>" {
		t.Fatalf("expected 200 OK fallback from stale-if-error during origin outage, got code %d: %s", rec3.Code, rec3.Body.String())
	}
	if !strings.Contains(rec3.Header().Get("Cache-Status"), "stale-if-error") {
		t.Fatalf("expected stale-if-error detail in Cache-Status, got: %s", rec3.Header().Get("Cache-Status"))
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
		req1 := httptest.NewRequest(http.MethodGet, "http://example.com/tiered-1", nil)
		rec1 := httptest.NewRecorder()
		handler.ServeHTTP(rec1, req1)

		if originCalls.Load() != 1 {
			t.Fatalf("expected 1 origin call, got %d", originCalls.Load())
		}
		if rec1.Header().Get("Cache-Control") != "private, no-store" {
			t.Fatalf("expected downstream client to receive private/no-store, got: %s", rec1.Header().Get("Cache-Control"))
		}

		// Request 2 -> HIT served from Titip cache
		req2 := httptest.NewRequest(http.MethodGet, "http://example.com/tiered-1", nil)
		rec2 := httptest.NewRecorder()
		handler.ServeHTTP(rec2, req2)

		if originCalls.Load() != 1 {
			t.Fatalf("expected 0 additional origin calls on cache HIT, got %d", originCalls.Load())
		}
		if !strings.Contains(rec2.Header().Get("Cache-Status"), "hit") {
			t.Fatalf("expected cache HIT, got: %s", rec2.Header().Get("Cache-Status"))
		}
		if rec2.Header().Get("Cache-Control") != "private, no-store" {
			t.Fatalf("expected downstream client to still receive original Cache-Control on cache HIT, got: %s", rec2.Header().Get("Cache-Control"))
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
		req1 := httptest.NewRequest(http.MethodGet, "http://example.com/tiered-cdn", nil)
		rec1 := httptest.NewRecorder()
		handler.ServeHTTP(rec1, req1)

		// Request 2 -> HIT (because CDN-Cache-Control max-age=300 overrides Cache-Control max-age=0)
		req2 := httptest.NewRequest(http.MethodGet, "http://example.com/tiered-cdn", nil)
		rec2 := httptest.NewRecorder()
		handler.ServeHTTP(rec2, req2)

		if originCalls.Load() != 1 {
			t.Fatalf("expected 1 origin call (CDN-Cache-Control hit), got %d", originCalls.Load())
		}
		if !strings.Contains(rec2.Header().Get("Cache-Status"), "hit") {
			t.Fatalf("expected cache HIT, got: %s", rec2.Header().Get("Cache-Status"))
		}
	})
}

func TestPurge_PathOnly_PurgesCachedKeysWithHost(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	var originCalls atomic.Int64
	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=300")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("cached-payload"))
	})
	handler := mw.testHandler(origin)

	// Step 1: Prime cache for http://localhost:8080/api/time
	req1 := httptest.NewRequest(http.MethodGet, "http://localhost:8080/api/time", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if originCalls.Load() != 1 {
		t.Fatalf("expected 1 origin call, got %d", originCalls.Load())
	}

	// Step 2: Confirm it is a cache HIT
	req2 := httptest.NewRequest(http.MethodGet, "http://localhost:8080/api/time", nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if originCalls.Load() != 1 {
		t.Fatalf("expected 0 additional origin calls (HIT), got %d", originCalls.Load())
	}

	// Step 3: Purge using pure path "/api/time" (without host)
	n, err := mw.Purge(context.Background(), "/api/time")
	if err != nil {
		t.Fatalf("purge failed: %v", err)
	}
	if n == 0 {
		t.Fatalf("expected at least 1 key purged, got %d", n)
	}

	// Step 4: Request after purge -> MISS (calls origin)
	req3 := httptest.NewRequest(http.MethodGet, "http://localhost:8080/api/time", nil)
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)
	if originCalls.Load() != 2 {
		t.Fatalf("expected 2 origin calls after purge (MISS), got %d", originCalls.Load())
	}
}
