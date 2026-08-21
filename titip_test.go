package titip

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/rueidis"

	"github.com/indragunawan/titip/storage"
	storageRedis "github.com/indragunawan/titip/storage/redis"
)

func setupTestTitip(t *testing.T, opts ...Option) (*miniredis.Miniredis, storage.Storage, *Titip) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{mr.Addr()},
		DisableCache: true,
	})
	if err != nil {
		mr.Close()
		t.Fatalf("failed to create rueidis client: %v", err)
	}

	store, err := storageRedis.New(storageRedis.WithClient(client), storageRedis.WithKeyPrefix("titip_mw_test:"))
	if err != nil {
		client.Close()
		mr.Close()
		t.Fatalf("failed to create RedisStorage: %v", err)
	}

	defaultOpts := []Option{
		WithStorage(store),
		WithOriginTimeout(5 * time.Second),
	}
	defaultOpts = append(defaultOpts, opts...)

	mw, err := New(defaultOpts...)
	if err != nil {
		_ = store.Close()
		mr.Close()
		t.Fatalf("failed to create Titip: %v", err)
	}

	t.Cleanup(func() {
		_ = mw.Close(context.Background())
		mr.Close()
	})

	return mr, store, mw
}

// AC-1: Singleflight Stampede & Initiator Cancellation Resilience
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

	handler := mw.Handler(originHandler)
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

	// Exactly 1 origin execution should have occurred
	if originExecutions.Load() != 1 {
		t.Fatalf("expected exactly 1 origin execution, got %d", originExecutions.Load())
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

	// Request #51 gets immediate cache hit
	req51 := httptest.NewRequest(http.MethodGet, "http://example.com/api/stampede", nil)
	rec51 := httptest.NewRecorder()
	handler.ServeHTTP(rec51, req51)

	if rec51.Code != http.StatusOK {
		t.Fatalf("request 51 expected 200 OK, got %d", rec51.Code)
	}
	if status := rec51.Header().Get("Cache-Status"); status == "" || !containsAny(status, "hit") {
		t.Fatalf("expected Cache-Status hit on request 51, got %q", status)
	}
	if originExecutions.Load() != 1 {
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

	handler := mw.Handler(originHandler)

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

	handler := mw.Handler(originHandler)

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

	handler := mw.Handler(originHandler)

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

	handler := mw.Handler(originHandler)

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
func TestUnsafeMethodAutoInvalidation(t *testing.T) {
	_, _, mw := setupTestTitip(t)

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

	handler := mw.Handler(originHandler)

	// 1. Prime cache
	reqGet := httptest.NewRequest(http.MethodGet, "http://example.com/api/state", nil)
	recGet1 := httptest.NewRecorder()
	handler.ServeHTTP(recGet1, reqGet)
	if recGet1.Body.String() != "state=0" {
		t.Fatalf("expected state=0, got %s", recGet1.Body.String())
	}

	// 2. Mutating POST request
	reqPost := httptest.NewRequest(http.MethodPost, "http://example.com/api/state", nil)
	recPost := httptest.NewRecorder()
	handler.ServeHTTP(recPost, reqPost)
	if recPost.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d", recPost.Code)
	}

	// 3. Subsequent GET must fetch new state
	recGet2 := httptest.NewRecorder()
	handler.ServeHTTP(recGet2, reqGet)
	if recGet2.Body.String() != "state=42" {
		t.Fatalf("expected invalidated state=42, got %s", recGet2.Body.String())
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

	handler := mw.Handler(originHandler)

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
	mr, err := miniredis.Run()
	if err != nil {
		b.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{mr.Addr()},
		DisableCache: true,
	})
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	store, err := storageRedis.New(storageRedis.WithClient(client), storageRedis.WithKeyPrefix("bench_hit:"))
	if err != nil {
		b.Fatalf("failed to create storage: %v", err)
	}

	mw, err := New(WithStorage(store))
	if err != nil {
		b.Fatalf("failed to create titip: %v", err)
	}
	defer func() { _ = mw.Close(context.Background()) }()

	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=300")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	handler := mw.Handler(originHandler)
	req := httptest.NewRequest(http.MethodGet, "http://example.com/bench/hit", nil)

	// Prime cache
	recPrime := httptest.NewRecorder()
	handler.ServeHTTP(recPrime, req)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rec := GetResponseRecorder()
		handler.ServeHTTP(rec, req)
		PutResponseRecorder(rec)
	}
}

func BenchmarkMiddleware_ParallelThroughput(b *testing.B) {
	mr, err := miniredis.Run()
	if err != nil {
		b.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{mr.Addr()},
		DisableCache: true,
	})
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	store, err := storageRedis.New(storageRedis.WithClient(client), storageRedis.WithKeyPrefix("bench_par:"))
	if err != nil {
		b.Fatalf("failed to create storage: %v", err)
	}

	mw, err := New(WithStorage(store))
	if err != nil {
		b.Fatalf("failed to create titip: %v", err)
	}
	defer func() { _ = mw.Close(context.Background()) }()

	originHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=300")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":"parallel"}`))
	})

	handler := mw.Handler(originHandler)
	req := httptest.NewRequest(http.MethodGet, "http://example.com/bench/par", nil)

	// Prime
	recPrime := httptest.NewRecorder()
	handler.ServeHTTP(recPrime, req)

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rec := GetResponseRecorder()
			handler.ServeHTTP(rec, req)
			PutResponseRecorder(rec)
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

	handler := mw.Handler(originHandler)

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
	}

	if hitCount != 80 {
		t.Errorf("expected 80 hits in metrics, got %v", hitCount)
	}
	if missCount != 20 {
		t.Errorf("expected 20 misses in metrics, got %v", missCount)
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
	h1 := mw1.Handler(originHandler)
	rec1Prime := httptest.NewRecorder()
	h1.ServeHTTP(rec1Prime, req)

	rec1Hit := httptest.NewRecorder()
	h1.ServeHTTP(rec1Hit, req)
	if status := rec1Hit.Header().Get("Cache-Status"); !containsAny(status, "titip; hit") {
		t.Errorf("expected RFC-9211 header, got %s", status)
	}

	// Prime mw2
	h2 := mw2.Handler(originHandler)
	rec2Prime := httptest.NewRecorder()
	h2.ServeHTTP(rec2Prime, req)

	rec2Hit := httptest.NewRecorder()
	h2.ServeHTTP(rec2Hit, req)
	if status := rec2Hit.Header().Get("Cache-Status"); status != "HIT" {
		t.Errorf("expected SimpleToken header HIT, got %s", status)
	}

	// Prime mw3
	h3 := mw3.Handler(originHandler)
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


