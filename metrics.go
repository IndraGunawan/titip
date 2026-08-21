package titip

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metric status label values for titip_requests_total
const (
	StatusHit         = "hit"
	StatusMiss        = "miss"
	StatusStaleHit    = "stale_hit"
	StatusRevalidated = "revalidated"
	StatusBypass      = "bypass"
	StatusError       = "error"
)

// Metrics encapsulates Prometheus telemetry collectors for Titip.
type Metrics struct {
	requestsTotal   *prometheus.CounterVec
	storageDuration *prometheus.HistogramVec
}

// newMetrics initializes and registers Prometheus collectors with the provided Registerer.
func newMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		return nil
	}

	m := &Metrics{
		requestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "titip_requests_total",
				Help: "Total number of HTTP requests processed by Titip caching middleware partitioned by cache status.",
			},
			[]string{"status"},
		),
		storageDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "titip_storage_duration_seconds",
				Help:    "Latency of cache storage operations in seconds.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"operation", "backend"},
		),
	}

	// Register collectors (ignoring already-registered errors for testing flexibility)
	_ = reg.Register(m.requestsTotal)
	_ = reg.Register(m.storageDuration)

	return m
}

// RecordRequest safely increments the request counter for the given status.
func (m *Metrics) RecordRequest(status string) {
	if m == nil || m.requestsTotal == nil {
		return
	}
	m.requestsTotal.WithLabelValues(status).Inc()
}

// RecordStorage safely observes the latency of a storage operation.
func (m *Metrics) RecordStorage(operation, backend string, dur time.Duration) {
	if m == nil || m.storageDuration == nil {
		return
	}
	m.storageDuration.WithLabelValues(operation, backend).Observe(dur.Seconds())
}
