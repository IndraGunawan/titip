package titip

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/rueidis"

	"github.com/indragunawan/titip/storage"
	storageRedis "github.com/indragunawan/titip/storage/redis"
)

func getTestRedisAddr() string {
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		return addr
	}
	return "127.0.0.1:6379"
}

func setupTestTitip(t testing.TB, opts ...Option) (rueidis.Client, storage.Storage, *Titip) {
	addr := getTestRedisAddr()
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{addr},
		DisableCache: true,
	})
	if err != nil {
		t.Fatalf("failed to connect to test Redis at %s: %v", addr, err)
	}

	prefix := fmt.Sprintf("test_mw:%d:%d:", time.Now().UnixNano(), rand.Int63())
	store, err := storageRedis.New(client, storageRedis.WithKeyPrefix(prefix))
	if err != nil {
		client.Close()
		t.Fatalf("failed to create RedisStorage: %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp := client.Do(ctx, client.B().Keys().Pattern(prefix+"*").Build())
		if keys, err := resp.AsStrSlice(); err == nil && len(keys) > 0 {
			delCmds := make([]rueidis.Completed, len(keys))
			for i, k := range keys {
				delCmds[i] = client.B().Del().Key(k).Build()
			}
			client.DoMulti(ctx, delCmds...)
		}
		_ = store.Close()
		client.Close()
	})

	defaultOpts := []Option{
		WithStorage(store),
		WithOriginTimeout(5 * time.Second),
		WithStorageTimeout(2 * time.Second),
		WithCacheStatusMode(CacheStatusRFC9211),
	}
	defaultOpts = append(defaultOpts, opts...)

	mw, err := New(defaultOpts...)
	if err != nil {
		t.Fatalf("failed to create Titip middleware: %v", err)
	}

	return client, store, mw
}

func (t *Titip) testHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.ServeHTTP(w, r, next)
	})
}

// AC-1: Singleflight Stampede & Initiator Cancellation Resilience on Stale Revalidations
func TestSingleflight_StampedeAndInitiatorCancellation(t *testing.T) {
	_, _, mw := setupTestTitip(t)

	var originExecutions atomic.Int32
	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originExecutions.Add(1)
		time.Sleep(100 * time.Millisecond)
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
	if err := mw.PurgeURL(context.Background(), "http://example.com/api/stampede", WithSoftPurge()); err != nil {
		t.Fatalf("failed to soft purge: %v", err)
	}

	const concurrentRequests = 50
	var wg sync.WaitGroup
	wg.Add(concurrentRequests)

	// Launch request #0 that cancels its context early
	ctx0, cancel0 := context.WithCancel(context.Background())
	go func() {
		defer wg.Done()
		req0 := httptest.NewRequest(http.MethodGet, "http://example.com/api/stampede", nil).WithContext(ctx0)
		rec0 := httptest.NewRecorder()
		// Cancel at t=10ms
		time.AfterFunc(10*time.Millisecond, cancel0)
		handler.ServeHTTP(rec0, req0)
	}()

	// Launch remaining concurrent requests
	responses := make([]*httptest.ResponseRecorder, concurrentRequests-1)
	for i := 0; i < concurrentRequests-1; i++ {
		idx := i
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "http://example.com/api/stampede", nil)
			rec := httptest.NewRecorder()
			responses[idx] = rec
			handler.ServeHTTP(rec, req)
		}()
	}

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
	if err := mw.PurgeURL(context.Background(), "http://example.com/api/soft-test", WithSoftPurge()); err != nil {
		t.Fatalf("soft purge failed: %v", err)
	}

	// 3. Make origin fail with 503 -> should fallback to stale cached payload!
	originFail.Store(true)

	const concurrent = 10
	var wg sync.WaitGroup
	wg.Add(concurrent)

	for i := 0; i < concurrent; i++ {
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

// AC-3: Fail-Open on Redis Outage
func TestFailOpen_OnRedisOutage(t *testing.T) {
	mr, _, mw := setupTestTitip(t, WithStorageTimeout(100*time.Millisecond))

	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("live origin response"))
	})

	handler := mw.testHandler(originHandler)

	// Close Redis / miniredis to simulate outage
	mr.Close()

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
	_ = mw.PurgeURL(context.Background(), "http://example.com/api/panic-test", WithSoftPurge())

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

// Unsafe HTTP method auto-invalidation
func TestUnsafeMethodAutoInvalidation_DefaultDisabled(t *testing.T) {
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
	_, _, mw := setupTestTitip(t, WithAutoInvalidateMutatingMethods(true))

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
	reg := prometheus.NewRegistry()
	_, _, mw := setupTestTitip(t, WithMetrics(reg))

	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("metric test data"))
	})

	handler := mw.testHandler(originHandler)

	// 20 misses (different URLs)
	for i := 0; i < 20; i++ {
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://example.com/metric/%d", i), nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}

	// 80 hits (requesting the same primed URL 80 times)
	reqPrime := httptest.NewRequest(http.MethodGet, "http://example.com/metric/0", nil)
	for i := 0; i < 80; i++ {
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
	_, _, mw := setupTestTitip(t)

	var originExecutions atomic.Int32
	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := originExecutions.Add(1)
		time.Sleep(10 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "private, no-cache")
		w.Header().Set("Set-Cookie", fmt.Sprintf("session_id=user-%d; Path=/; HttpOnly", reqID))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(fmt.Sprintf(`{"user_id":%d}`, reqID)))
	})

	handler := mw.testHandler(originHandler)
	const concurrentUsers = 30

	var wg sync.WaitGroup
	wg.Add(concurrentUsers)
	userCookies := make([]string, concurrentUsers)

	for i := 0; i < concurrentUsers; i++ {
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
	if err := mw.PurgeURL(context.Background(), "http://example.com/api/refresh", WithSoftPurge()); err != nil {
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
	if err := engine.PurgeTag(context.Background(), "electronics"); err != nil {
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

// TestFailOpen_MetadataExists_BodyEvictedGlitch verifies that if metadata exists in Redis
// but the variant body key was expired/evicted in a microsecond race, Titip seamlessly fails open
// to origin without crashing, returning 500, or dropping the response.
func TestFailOpen_MetadataExists_BodyEvictedGlitch(t *testing.T) {
	client, store, mw := setupTestTitip(t)

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

	// 2. Simulate microsecond glitch: Metadata hash is present in Redis, but Body key was evicted/expired
	primaryKey := generatePrimaryKey(req1, &KeyConfig{})
	meta, err := store.GetMeta(context.Background(), primaryKey)
	if err != nil || meta == nil {
		t.Fatalf("expected metadata in Redis, got err=%v meta=%v", err, meta)
	}

	// Find the exact Redis body key and delete it from Redis
	varInfo, body, err := store.GetVariant(context.Background(), primaryKey, defaultVariantKey)
	if err != nil || varInfo == nil || len(body) == 0 {
		t.Fatalf("expected body in Redis before eviction simulation")
	}

	// Delete ONLY the body key directly from Redis (leaving metadata hash intact)
	resp := client.Do(context.Background(), client.B().Keys().Pattern("*body*"+primaryKey+"*").Build())
	keys, err := resp.AsStrSlice()
	if err != nil || len(keys) == 0 {
		t.Fatalf("expected to find body key in Redis")
	}
	for _, k := range keys {
		if err := client.Do(context.Background(), client.B().Del().Key(k).Build()).Error(); err != nil {
			t.Fatalf("failed to delete body key: %v", err)
		}
	}

	// Verify body is gone but metadata is still present in Redis
	_, missingBody, _ := store.GetVariant(context.Background(), primaryKey, defaultVariantKey)
	if len(missingBody) != 0 {
		t.Fatalf("expected body to be deleted from Redis")
	}
	metaStillThere, _ := store.GetMeta(context.Background(), primaryKey)
	if metaStillThere == nil {
		t.Fatalf("expected metadata hash to still exist in Redis")
	}

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
	_, _, mw := setupTestTitip(t, WithIgnoreClientCacheControl(false))

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
	_, _, mw := setupTestTitip(t, WithCacheStatusMode(CacheStatusSimpleToken), WithIgnoreClientCacheControl(false))

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

	handler := mw.testHandler(originHandler)

	// 1. tokenMiss ("MISS"): First request to cacheable URL
	reqMiss := httptest.NewRequest(http.MethodGet, "http://example.com/api/simple/cacheable", nil)
	recMiss := httptest.NewRecorder()
	handler.ServeHTTP(recMiss, reqMiss)
	if cs := recMiss.Header().Get("Cache-Status"); cs != tokenMiss {
		t.Errorf("expected tokenMiss (%q), got %q", tokenMiss, cs)
	}

	// 2. tokenHit ("HIT"): Second request to cached URL
	reqHit := httptest.NewRequest(http.MethodGet, "http://example.com/api/simple/cacheable", nil)
	recHit := httptest.NewRecorder()
	handler.ServeHTTP(recHit, reqHit)
	if cs := recHit.Header().Get("Cache-Status"); cs != tokenHit {
		t.Errorf("expected tokenHit (%q), got %q", tokenHit, cs)
	}

	// 3. tokenBypass ("BYPASS"): Mutating POST or client no-store
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

	// 4. tokenDynamic ("DYNAMIC"): Response is uncacheable due to origin Set-Cookie
	reqDynamic := httptest.NewRequest(http.MethodGet, "http://example.com/api/simple/dynamic", nil)
	recDynamic := httptest.NewRecorder()
	handler.ServeHTTP(recDynamic, reqDynamic)
	if cs := recDynamic.Header().Get("Cache-Status"); cs != tokenDynamic {
		t.Errorf("expected tokenDynamic (%q), got %q", tokenDynamic, cs)
	}

	// 5. tokenUpdating ("UPDATING"): Stale-while-revalidate background update window
	reqSWR := httptest.NewRequest(http.MethodGet, "http://example.com/api/simple/swr", nil)
	recSWR1 := httptest.NewRecorder()
	handler.ServeHTTP(recSWR1, reqSWR) // prime (MISS)
	time.Sleep(1100 * time.Millisecond) // expire max-age=1, enter SWR=10 window
	recSWR2 := httptest.NewRecorder()
	handler.ServeHTTP(recSWR2, reqSWR)
	if cs := recSWR2.Header().Get("Cache-Status"); cs != tokenUpdating {
		t.Errorf("expected tokenUpdating (%q), got %q", tokenUpdating, cs)
	}

	// 6. tokenRevalidated ("REVALIDATED"): Origin returns 304 Not Modified
	reqReval := httptest.NewRequest(http.MethodGet, "http://example.com/api/simple/reval", nil)
	recReval1 := httptest.NewRecorder()
	handler.ServeHTTP(recReval1, reqReval) // prime (MISS)
	time.Sleep(1100 * time.Millisecond)   // expire max-age=1
	recReval2 := httptest.NewRecorder()
	handler.ServeHTTP(recReval2, reqReval) // revalidates with origin -> 304
	if cs := recReval2.Header().Get("Cache-Status"); cs != tokenRevalidated {
		t.Errorf("expected tokenRevalidated (%q), got %q", tokenRevalidated, cs)
	}

	// 7. tokenExpired ("EXPIRED"): Origin returns fresh 200 OK on revalidation
	reqExp := httptest.NewRequest(http.MethodGet, "http://example.com/api/simple/expired", nil)
	recExp1 := httptest.NewRecorder()
	handler.ServeHTTP(recExp1, reqExp) // prime (MISS)
	time.Sleep(1100 * time.Millisecond) // expire max-age=1
	recExp2 := httptest.NewRecorder()
	handler.ServeHTTP(recExp2, reqExp) // revalidates with origin -> 200 OK
	if cs := recExp2.Header().Get("Cache-Status"); cs != tokenExpired {
		t.Errorf("expected tokenExpired (%q), got %q", tokenExpired, cs)
	}

	// 8. tokenStale ("STALE"): Origin 500 error triggers stale-if-error fallback
	reqFailover := httptest.NewRequest(http.MethodGet, "http://example.com/api/simple/failover", nil)
	recFailover1 := httptest.NewRecorder()
	handler.ServeHTTP(recFailover1, reqFailover) // prime (MISS)
	time.Sleep(1100 * time.Millisecond)          // expire max-age=1
	recFailover2 := httptest.NewRecorder()
	handler.ServeHTTP(recFailover2, reqFailover) // origin fails -> serves stale
	if cs := recFailover2.Header().Get("Cache-Status"); cs != tokenStale {
		t.Errorf("expected tokenStale (%q), got %q", tokenStale, cs)
	}
	if recFailover2.Code != http.StatusOK {
		t.Errorf("expected 200 OK from stale-if-error fallback, got %d", recFailover2.Code)
	}
}

func TestNew_MissingStorage(t *testing.T) {
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
	addr := getTestRedisAddr()
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{addr},
		DisableCache: true,
	})
	if err != nil {
		t.Fatalf("failed to connect to test Redis: %v", err)
	}
	defer client.Close()

	prefix := fmt.Sprintf("test_min:%d:%d:", time.Now().UnixNano(), rand.Int63())
	store, err := storageRedis.New(client, storageRedis.WithKeyPrefix(prefix))
	if err != nil {
		t.Fatalf("failed to create RedisStorage: %v", err)
	}
	defer store.Close()

	// Initialize with ONLY the single required option
	mw, err := New(WithStorage(store))
	if err != nil {
		t.Fatalf("failed to initialize Titip with minimal options: %v", err)
	}

	// 1. Verify default configuration values
	if mw.cfg.CacheStatusMode != CacheStatusSimpleToken {
		t.Errorf("expected CacheStatusSimpleToken (%v), got %v", CacheStatusSimpleToken, mw.cfg.CacheStatusMode)
	}
	if !mw.cfg.IgnoreClientCacheControl {
		t.Errorf("expected IgnoreClientCacheControl to be true by default")
	}
	if mw.cfg.TagHeaderName != headerCacheTag {
		t.Errorf("expected TagHeaderName %q, got %q", headerCacheTag, mw.cfg.TagHeaderName)
	}
	if mw.cfg.OriginTimeout != 30*time.Second {
		t.Errorf("expected OriginTimeout 30s, got %v", mw.cfg.OriginTimeout)
	}
	if mw.cfg.StorageTimeout != 1*time.Second {
		t.Errorf("expected StorageTimeout 1s, got %v", mw.cfg.StorageTimeout)
	}
	if mw.logger == nil {
		t.Errorf("expected non-nil default logger")
	}
	if len(mw.cacheableStatusCodes) == 0 {
		t.Errorf("expected non-empty default cacheable status codes")
	}
	if mw.cfg.ESI.MaxDepth != 3 {
		t.Errorf("expected ESI MaxDepth 3, got %d", mw.cfg.ESI.MaxDepth)
	}
	if mw.cfg.ESI.MaxTimeout != 30*time.Second {
		t.Errorf("expected ESI MaxTimeout 30s, got %v", mw.cfg.ESI.MaxTimeout)
	}
	if mw.cfg.ESI.MaxConcurrentRequests != 8 {
		t.Errorf("expected ESI MaxConcurrentRequests 8, got %d", mw.cfg.ESI.MaxConcurrentRequests)
	}
	if mw.cfg.ESI.MaxResponseSize != 10*1024*1024 {
		t.Errorf("expected ESI MaxResponseSize 10MB, got %d", mw.cfg.ESI.MaxResponseSize)
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

	if err := mw.PurgeURL(ctx, "http://example.com/api/minimal"); err != nil {
		t.Errorf("expected PurgeURL to succeed on minimal instance, got %v", err)
	}
	if err := mw.Close(ctx); err != nil {
		t.Errorf("expected Close to succeed on minimal instance, got %v", err)
	}
}

func TestNew_NilOptionGuards(t *testing.T) {
	addr := getTestRedisAddr()
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{addr},
		DisableCache: true,
	})
	if err != nil {
		t.Fatalf("failed to connect to test Redis: %v", err)
	}
	defer client.Close()

	prefix := fmt.Sprintf("test_nil_opts:%d:%d:", time.Now().UnixNano(), rand.Int63())
	store, err := storageRedis.New(client, storageRedis.WithKeyPrefix(prefix))
	if err != nil {
		t.Fatalf("failed to create RedisStorage: %v", err)
	}
	defer store.Close()

	// Initialize with explicit nil pointers
	mw, err := New(
		WithStorage(store),
		WithLogger(nil),
		WithMetrics(nil),
		WithCacheableStatusCodes(nil...),
	)
	if err != nil {
		t.Fatalf("expected New with nil option guards to succeed, got %v", err)
	}
	if mw.logger == nil {
		t.Fatal("expected mw.logger to fallback to slog.Default() when passed nil")
	}
	if len(mw.cacheableStatusCodes) == 0 {
		t.Fatal("expected mw.cacheableStatusCodes to fallback to default codes when passed nil")
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

