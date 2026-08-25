package titip

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metric status label values for titip_requests_total
const (
	statusHit         = "hit"
	statusMiss        = "miss"
	statusStaleHit    = "stale_hit"
	statusRevalidated = "revalidated"
	statusBypass      = "bypass"
	statusError       = "error"
)

// metrics encapsulates Prometheus telemetry collectors for Titip.
type metrics struct {
	requestsTotal   *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
	esiFragments    *prometheus.CounterVec
	esiDuration     *prometheus.HistogramVec
}

// newMetrics initializes and registers Prometheus collectors with the provided Registerer.
func newMetrics(reg prometheus.Registerer, enableESI bool) *metrics {
	if reg == nil {
		return nil
	}

	m := &metrics{
		requestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "titip_requests_total",
				Help: "Total number of HTTP requests processed by Titip caching middleware partitioned by cache status.",
			},
			[]string{"status"},
		),
		requestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "titip_request_duration_seconds",
				Help:    "Latency distribution of HTTP requests processed by Titip caching middleware in seconds.",
				Buckets: []float64{.00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			},
			[]string{"status"},
		),
	}

	_ = reg.Register(m.requestsTotal)
	_ = reg.Register(m.requestDuration)

	if enableESI {
		m.esiFragments = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "titip_esi_fragments_total",
				Help: "Total number of ESI fragments processed partitioned by status.",
			},
			[]string{"status"},
		)
		m.esiDuration = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "titip_esi_duration_seconds",
				Help:    "Latency distribution of ESI fragment fetching and splicing in seconds.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"mode"},
		)

		_ = reg.Register(m.esiFragments)
		_ = reg.Register(m.esiDuration)
	}

	return m
}

// recordRequest safely increments the request counter and observes duration for the given status.
func (m *metrics) recordRequest(status string, dur time.Duration) {
	if m == nil {
		return
	}
	if m.requestsTotal != nil {
		m.requestsTotal.WithLabelValues(status).Inc()
	}
	if m.requestDuration != nil {
		m.requestDuration.WithLabelValues(status).Observe(dur.Seconds())
	}
}

// recordESIFragment safely increments the ESI fragment counter for the given status.
func (m *metrics) recordESIFragment(status string) {
	if m == nil || m.esiFragments == nil {
		return
	}
	m.esiFragments.WithLabelValues(status).Inc()
}

// recordESIDuration safely observes the latency of an ESI operation.
func (m *metrics) recordESIDuration(mode string, dur time.Duration) {
	if m == nil || m.esiDuration == nil {
		return
	}
	m.esiDuration.WithLabelValues(mode).Observe(dur.Seconds())
}
