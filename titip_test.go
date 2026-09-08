package titip

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/indragunawan/titip/internal/teststore"
	"github.com/indragunawan/titip/storage"
)

func setupTestTitip(t testing.TB, opts ...Option) (*teststore.Store, storage.Storage, *Titip) {
	store := teststore.New()

	defaultOpts := []Option{
		WithStorage(store),
		WithBackgroundFetchTimeout(10 * time.Second),
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

// testResponse wraps httptest.ResponseRecorder with fluent assertion methods.
type testResponse struct {
	*httptest.ResponseRecorder
	t testing.TB
}

func doReq(t testing.TB, h http.Handler, req *http.Request) *testResponse {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return &testResponse{ResponseRecorder: rec, t: t}
}

func doGet(t testing.TB, h http.Handler, target string, headers ...string) *testResponse {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	for i := 0; i < len(headers)-1; i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	return doReq(t, h, req)
}

func doHead(t testing.TB, h http.Handler, target string, headers ...string) *testResponse {
	t.Helper()
	req := httptest.NewRequest(http.MethodHead, target, nil)
	for i := 0; i < len(headers)-1; i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	return doReq(t, h, req)
}

func doPost(t testing.TB, h http.Handler, target, body string, headers ...string) *testResponse {
	t.Helper()
	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(http.MethodPost, target, strings.NewReader(body))
	} else {
		req = httptest.NewRequest(http.MethodPost, target, nil)
	}
	for i := 0; i < len(headers)-1; i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	return doReq(t, h, req)
}

func (r *testResponse) assertStatus(want int) *testResponse {
	r.t.Helper()
	if r.Code != want {
		r.t.Fatalf("expected status %d, got %d", want, r.Code)
	}
	return r
}

func (r *testResponse) assertBody(want string) *testResponse {
	r.t.Helper()
	if got := r.Body.String(); got != want {
		r.t.Fatalf("expected body %q, got %q", want, got)
	}
	return r
}

func (r *testResponse) assertBodyContains(substr string) *testResponse {
	r.t.Helper()
	if !strings.Contains(r.Body.String(), substr) {
		r.t.Fatalf("expected body to contain %q, got %q", substr, r.Body.String())
	}
	return r
}

func (r *testResponse) assertEmptyBody() *testResponse {
	r.t.Helper()
	if r.Body.Len() != 0 {
		r.t.Fatalf("expected 0 body bytes, got %d (%q)", r.Body.Len(), r.Body.String())
	}
	return r
}

func (r *testResponse) assertCacheStatus(wantSub string) *testResponse {
	r.t.Helper()
	status := r.Header().Get("Cache-Status")
	if !strings.Contains(status, wantSub) {
		r.t.Fatalf("expected Cache-Status containing %q, got %q", wantSub, status)
	}
	return r
}

func (r *testResponse) assertCacheStatusNot(sub string) *testResponse {
	r.t.Helper()
	status := r.Header().Get("Cache-Status")
	if strings.Contains(status, sub) {
		r.t.Fatalf("expected Cache-Status NOT containing %q, got %q", sub, status)
	}
	return r
}

func (r *testResponse) assertHeader(key, want string) *testResponse {
	r.t.Helper()
	if got := r.Header().Get(key); got != want {
		r.t.Fatalf("expected header %s=%q, got %q", key, want, got)
	}
	return r
}

func (r *testResponse) assertHeaderMissing(key string) *testResponse {
	r.t.Helper()
	if got := r.Header().Get(key); got != "" {
		r.t.Fatalf("expected header %s to be empty/absent, got %q", key, got)
	}
	return r
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

// Prometheus metrics & PromQL verification.
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

func containsAny(s string, sub string) bool {
	return bytes.Contains([]byte(s), []byte(sub))
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

// TestOrigin_MalformedOriginTags validates tag extraction and purging when origin returns duplicate commas and spaces
func TestOrigin_MalformedOriginTags(t *testing.T) {
	t.Parallel()
	_, _, engine := setupTestTitip(t)

	var originCalls atomic.Int64
	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=300")
		w.Header().Set("Cache-Tag", "tag1,,tag2, ,tag3  tag4")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("tagged-payload"))
	})
	handler := engine.testHandler(origin)

	// 1. Prime cache
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/api/item", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if originCalls.Load() != 1 {
		t.Fatalf("expected 1 origin call, got %d", originCalls.Load())
	}

	// 2. Cache hit
	req2 := httptest.NewRequest(http.MethodGet, "http://example.com/api/item", nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if originCalls.Load() != 1 {
		t.Fatalf("expected cache HIT, got %d origin calls", originCalls.Load())
	}

	// 3. Purge by "tag2" (which followed duplicate commas and spaces)
	n, err := engine.PurgeTag(context.Background(), "tag2")
	if err != nil {
		t.Fatalf("PurgeTag failed: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 entry purged for tag2, got %d", n)
	}

	// 4. Verify cache is purged (MISS -> origin called)
	req3 := httptest.NewRequest(http.MethodGet, "http://example.com/api/item", nil)
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)
	if originCalls.Load() != 2 {
		t.Errorf("expected cache to be purged, got %d origin calls", originCalls.Load())
	}

	// 5. Purge by "tag4" (which was space-separated)
	n4, err := engine.PurgeTag(context.Background(), "tag4")
	if err != nil {
		t.Fatalf("PurgeTag failed: %v", err)
	}
	if n4 != 1 {
		t.Errorf("expected 1 entry purged for tag4, got %d", n4)
	}
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
	if mw.config.cacheStatusMode != CacheStatusSimpleToken {
		t.Errorf("expected CacheStatusSimpleToken (%v), got %v", CacheStatusSimpleToken, mw.config.cacheStatusMode)
	}
	if mw.config.respectClientCacheControl {
		t.Errorf("expected RespectClientCacheControl to be false by default")
	}
	if mw.config.tagHeaderName != headerCacheTag {
		t.Errorf("expected TagHeaderName %q, got %q", headerCacheTag, mw.config.tagHeaderName)
	}
	if mw.config.backgroundFetchTimeout != 125*time.Second {
		t.Errorf("expected BackgroundFetchTimeout 125s, got %v", mw.config.backgroundFetchTimeout)
	}
	if mw.config.storageTimeout != 1*time.Second {
		t.Errorf("expected StorageTimeout 1s, got %v", mw.config.storageTimeout)
	}
	if mw.logger == nil {
		t.Errorf("expected non-nil default logger")
	}
	if mw.config.esi.MaxDepth != 3 {
		t.Errorf("expected ESI MaxDepth 3, got %d", mw.config.esi.MaxDepth)
	}
	if mw.config.esi.MaxTimeout != 30*time.Second {
		t.Errorf("expected ESI MaxTimeout 30s, got %v", mw.config.esi.MaxTimeout)
	}
	if mw.config.esi.MaxConcurrentRequests != 8 {
		t.Errorf("expected ESI MaxConcurrentRequests 8, got %d", mw.config.esi.MaxConcurrentRequests)
	}
	if mw.config.esi.MaxResponseSize != 10*1024*1024 {
		t.Errorf("expected ESI MaxResponseSize 10MB, got %d", mw.config.esi.MaxResponseSize)
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

// TestSynchronousFetch_RequestContextPassThrough verifies that synchronous cache misses
// pass the request context directly to the origin handler without injecting artificial timeouts,
// allowing upstream server timeouts or client cancellations to propagate naturally.
func TestSynchronousFetch_RequestContextPassThrough(t *testing.T) {
	t.Parallel()
	_, _, mw := setupTestTitip(t)

	type testContextKey struct{}
	ctxWithVal := context.WithValue(context.Background(), testContextKey{}, "test-val")

	var receivedCtx context.Context
	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedCtx = r.Context()
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	req := httptest.NewRequest(http.MethodGet, "http://example.com/api/ctx-pass-through", nil).WithContext(ctxWithVal)
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, req, origin)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}
	if receivedCtx == nil {
		t.Fatal("expected origin handler to receive request context")
	}
	if val, ok := receivedCtx.Value(testContextKey{}).(string); !ok || val != "test-val" {
		t.Errorf("expected context value 'test-val', got %v", val)
	}
	// Verify no deadline was added by Titip
	if _, hasDeadline := receivedCtx.Deadline(); hasDeadline {
		t.Errorf("expected no artificial deadline on synchronous origin request context")
	}
}

// TestBackgroundFetchTimeout_Configuration verifies WithBackgroundFetchTimeout options.
func TestBackgroundFetchTimeout_Configuration(t *testing.T) {
	t.Parallel()
	store := teststore.New()

	// 1. Custom timeout
	mwCustom, err := New(
		WithStorage(store),
		WithBackgroundFetchTimeout(60*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to create mw: %v", err)
	}
	if mwCustom.config.backgroundFetchTimeout != 60*time.Second {
		t.Errorf("expected 60s, got %v", mwCustom.config.backgroundFetchTimeout)
	}

	// 2. Disabled timeout (0)
	mwDisabled, err := New(
		WithStorage(store),
		WithBackgroundFetchTimeout(0),
	)
	if err != nil {
		t.Fatalf("failed to create mw: %v", err)
	}
	if mwDisabled.config.backgroundFetchTimeout != 0 {
		t.Errorf("expected 0, got %v", mwDisabled.config.backgroundFetchTimeout)
	}
}
