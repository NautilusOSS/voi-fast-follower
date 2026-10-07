package metrics

import (
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics exposes Prometheus-compatible follower gauges/counters.
type Metrics struct {
	registry           *prometheus.Registry
	CurrentRound       prometheus.Gauge
	TargetRound        prometheus.Gauge
	LastProcessedRound prometheus.Gauge
	CatchupLag         prometheus.Gauge
	BlocksPerSecond    prometheus.Gauge
	BlocksProcessed    prometheus.Counter
	Errors             prometheus.Counter

	processed   atomic.Uint64
	windowStart atomic.Int64
	windowCount atomic.Uint64
}

var (
	defaultOnce sync.Once
	defaultM    *Metrics
)

// Default returns a process-wide metrics instance registered on a private registry.
func Default() *Metrics {
	defaultOnce.Do(func() {
		defaultM = New()
	})
	return defaultM
}

// New creates metrics on an isolated registry (safe for tests).
func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		registry: reg,
		CurrentRound: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "voi_follower_current_round",
			Help: "Round currently being processed / last committed cursor",
		}),
		TargetRound: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "voi_follower_target_round",
			Help: "Network tip round",
		}),
		LastProcessedRound: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "voi_follower_last_processed_round",
			Help: "Last durably committed round",
		}),
		CatchupLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "voi_follower_catchup_lag",
			Help: "target_round - current_round",
		}),
		BlocksPerSecond: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "voi_follower_blocks_per_second",
			Help: "Recent blocks/sec throughput",
		}),
		BlocksProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "voi_follower_blocks_processed_total",
			Help: "Total blocks successfully processed",
		}),
		Errors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "voi_follower_errors_total",
			Help: "Total processing errors",
		}),
	}
	reg.MustRegister(
		m.CurrentRound,
		m.TargetRound,
		m.LastProcessedRound,
		m.CatchupLag,
		m.BlocksPerSecond,
		m.BlocksProcessed,
		m.Errors,
	)
	m.windowStart.Store(time.Now().UnixNano())
	return m
}

// Handler returns the Prometheus HTTP handler for this metrics instance.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// SetRounds updates round-related gauges.
func (m *Metrics) SetRounds(current, target, lastProcessed uint64) {
	m.CurrentRound.Set(float64(current))
	m.TargetRound.Set(float64(target))
	m.LastProcessedRound.Set(float64(lastProcessed))
	var lag float64
	if target >= current {
		lag = float64(target - current)
	}
	m.CatchupLag.Set(lag)
}

// RecordBlock updates throughput counters after a successful commit.
func (m *Metrics) RecordBlock() {
	m.BlocksProcessed.Inc()
	m.processed.Add(1)
	m.windowCount.Add(1)

	now := time.Now().UnixNano()
	start := m.windowStart.Load()
	elapsed := time.Duration(now - start)
	if elapsed >= time.Second {
		count := m.windowCount.Swap(0)
		m.windowStart.Store(now)
		bps := float64(count) / elapsed.Seconds()
		m.BlocksPerSecond.Set(bps)
	}
}

// RecordError increments the error counter.
func (m *Metrics) RecordError() {
	m.Errors.Inc()
}

// ProcessedTotal returns the number of blocks recorded in-process.
func (m *Metrics) ProcessedTotal() uint64 {
	return m.processed.Load()
}
