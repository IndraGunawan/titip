package esi

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// metrics encapsulates Prometheus collectors for ESI processing.
type metrics struct {
	fragmentsTotal *prometheus.CounterVec
	duration       *prometheus.HistogramVec
}

func newMetrics(reg prometheus.Registerer) *metrics {
	if reg == nil {
		return nil
	}

	m := &metrics{
		fragmentsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "esi_fragments_total",
				Help: "Total number of ESI fragments processed partitioned by status.",
			},
			[]string{"status"},
		),
		duration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "esi_duration_seconds",
				Help:    "Latency distribution of ESI fragment fetching and splicing in seconds.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"mode"},
		),
	}

	_ = reg.Register(m.fragmentsTotal)
	_ = reg.Register(m.duration)
	return m
}

func (m *metrics) recordFragment(status string) {
	if m == nil || m.fragmentsTotal == nil {
		return
	}
	m.fragmentsTotal.WithLabelValues(status).Inc()
}

func (m *metrics) recordDuration(mode string, dur time.Duration) {
	if m == nil || m.duration == nil {
		return
	}
	m.duration.WithLabelValues(mode).Observe(dur.Seconds())
}
