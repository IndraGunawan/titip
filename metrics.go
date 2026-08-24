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
	esiFragments    *prometheus.CounterVec
	esiDuration     *prometheus.HistogramVec
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
		esiFragments: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "titip_esi_fragments_total",
				Help: "Total number of ESI fragments processed partitioned by status.",
			},
			[]string{"status"},
		),
		esiDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "titip_esi_duration_seconds",
				Help:    "Latency distribution of ESI fragment fetching and splicing in seconds.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"mode"},
		),
	}

	// Register collectors (ignoring already-registered errors for testing flexibility)
	_ = reg.Register(m.requestsTotal)
	_ = reg.Register(m.storageDuration)
	_ = reg.Register(m.esiFragments)
	_ = reg.Register(m.esiDuration)

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

// RecordESIFragment safely increments the ESI fragment counter for the given status.
func (m *Metrics) RecordESIFragment(status string) {
	if m == nil || m.esiFragments == nil {
		return
	}
	m.esiFragments.WithLabelValues(status).Inc()
}

// RecordESIDuration safely observes the latency of an ESI operation.
func (m *Metrics) RecordESIDuration(mode string, dur time.Duration) {
	if m == nil || m.esiDuration == nil {
		return
	}
	m.esiDuration.WithLabelValues(mode).Observe(dur.Seconds())
}
