package importer

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	blocksDelivered = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "conduit_blocks_delivered_total",
		Help: "Blocks successfully returned from voi_archive GetBlock",
	})
	deliveryErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "conduit_delivery_errors_total",
		Help: "voi_archive GetBlock failures",
	})
	deliveryLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "conduit_delivery_latency_seconds",
		Help:    "voi_archive GetBlock latency (wait+read+translate)",
		Buckets: []float64{0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
	})
	conduitLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "conduit_lag",
		Help: "archive_checkpoint - requested_round",
	})
)

func collectors() []prometheus.Collector {
	return []prometheus.Collector{blocksDelivered, deliveryErrors, deliveryLatency, conduitLag}
}
