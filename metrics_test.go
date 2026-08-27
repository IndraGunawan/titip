package titip

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func getCounterValue(t *testing.T, cv *prometheus.CounterVec, labelValues ...string) float64 {
	t.Helper()
	c, err := cv.GetMetricWithLabelValues(labelValues...)
	if err != nil {
		t.Fatalf("failed to get metric with labels %v: %v", labelValues, err)
	}
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("failed to write metric: %v", err)
	}
	return m.GetCounter().GetValue()
}

func TestMetrics_NilSafety(t *testing.T) {
	t.Parallel()
	var m *metrics
	// None of these should panic when m is nil
	m.recordRequest("hit", time.Millisecond)
	m.recordESIFragment("success")
	m.recordESIDuration("parallel", time.Millisecond)
	m.recordPurge("url", "hard", "success", 5)
}

func TestPurgeMetrics_URLTagAll(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	_, store, engine := setupTestTitip(t, WithMetrics(reg))

	handler := engine.testHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("Cache-Tag", "catalog,item-category")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, `{"status":"ok"}`)
	}))

	ctx := context.Background()

	// 1. Populate 3 URLs
	for i := 1; i <= 3; i++ {
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://example.com/api/item/%d", i), nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("item %d failed: %d", i, rec.Code)
		}
	}

	// 2. Exact Purge URL 1 (Hard)
	n1, err := engine.Purge(ctx, "http://example.com/api/item/1")
	if err != nil {
		t.Fatalf("purge item 1 failed: %v", err)
	}
	if n1 != 1 {
		t.Errorf("expected 1 entry purged, got %d", n1)
	}

	if val := getCounterValue(t, engine.metrics.purgesTotal, "url", "hard", "success"); val != 1 {
		t.Errorf("expected purgesTotal(url, hard, success)=1, got %v", val)
	}
	if val := getCounterValue(t, engine.metrics.purgedEntriesTotal, "url", "hard"); val != 1 {
		t.Errorf("expected purgedEntriesTotal(url, hard)=1, got %v", val)
	}

	// 3. Exact Purge URL 2 (Soft)
	n2, err := engine.Purge(ctx, "http://example.com/api/item/2", WithSoftPurge())
	if err != nil {
		t.Fatalf("soft purge item 2 failed: %v", err)
	}
	if n2 != 1 {
		t.Errorf("expected 1 entry soft-purged, got %d", n2)
	}

	if val := getCounterValue(t, engine.metrics.purgesTotal, "url", "soft", "success"); val != 1 {
		t.Errorf("expected purgesTotal(url, soft, success)=1, got %v", val)
	}
	if val := getCounterValue(t, engine.metrics.purgedEntriesTotal, "url", "soft"); val != 1 {
		t.Errorf("expected purgedEntriesTotal(url, soft)=1, got %v", val)
	}

	// 4. Tag Purge "catalog" (Hard) -> Remaining URL 3 is purged
	n3, err := engine.PurgeTag(ctx, "catalog")
	if err != nil {
		t.Fatalf("purge tag catalog failed: %v", err)
	}
	if n3 < 1 {
		t.Errorf("expected at least 1 entry purged by tag, got %d", n3)
	}

	if val := getCounterValue(t, engine.metrics.purgesTotal, "tag", "hard", "success"); val != 1 {
		t.Errorf("expected purgesTotal(tag, hard, success)=1, got %v", val)
	}
	if val := getCounterValue(t, engine.metrics.purgedEntriesTotal, "tag", "hard"); val != float64(n3) {
		t.Errorf("expected purgedEntriesTotal(tag, hard)=%v, got %v", n3, val)
	}

	// 5. PurgeAll (Hard)
	n4, err := engine.PurgeAll(ctx)
	if err != nil {
		t.Fatalf("purge all failed: %v", err)
	}

	if val := getCounterValue(t, engine.metrics.purgesTotal, "all", "hard", "success"); val != 1 {
		t.Errorf("expected purgesTotal(all, hard, success)=1, got %v", val)
	}
	if val := getCounterValue(t, engine.metrics.purgedEntriesTotal, "all", "hard"); val != float64(n4) {
		t.Errorf("expected purgedEntriesTotal(all, hard)=%v, got %v", n4, val)
	}

	_ = store
}
