package conduit

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics are adapter-specific signals (separate from voi_follower_*).
type Metrics struct {
	BlocksDelivered prometheus.Counter
	DeliveryErrors  prometheus.Counter
	DeliveryLatency prometheus.Histogram
	Lag             prometheus.Gauge
}

// NewMetrics registers collectors on reg (or a new registry when reg is nil).
func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	m := &Metrics{
		BlocksDelivered: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "conduit_blocks_delivered_total",
			Help: "Blocks successfully translated and delivered to the Conduit consumer",
		}),
		DeliveryErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "conduit_delivery_errors_total",
			Help: "Failed Conduit adapter deliveries (get/translate/deliver)",
		}),
		DeliveryLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "conduit_delivery_latency_seconds",
			Help:    "Adapter get+translate+deliver latency",
			Buckets: []float64{0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
		}),
		Lag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "conduit_lag",
			Help: "archive_checkpoint - delivered_round (Conduit processing lag vs archive)",
		}),
	}
	reg.MustRegister(m.BlocksDelivered, m.DeliveryErrors, m.DeliveryLatency, m.Lag)
	return m
}
