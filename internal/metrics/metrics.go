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
	FetchLatency       prometheus.Histogram
	CommitLatency      prometheus.Histogram
	WorkersBusy        prometheus.Gauge
	WorkersTotal       prometheus.Gauge
	InFlight           prometheus.Gauge

	processed   atomic.Uint64
	windowStart atomic.Int64
	windowCount atomic.Uint64
	busy        atomic.Int64
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
			Help: "Round currently being processed / next commit cursor",
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
			Help: "Total blocks successfully committed",
		}),
		Errors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "voi_follower_errors_total",
			Help: "Total processing errors",
		}),
		FetchLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "voi_follower_fetch_latency_seconds",
			Help:    "BlockRaw fetch + decode latency",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}),
		CommitLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "voi_follower_commit_latency_seconds",
			Help:    "Sink Commit latency",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
		}),
		WorkersBusy: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "voi_follower_workers_busy",
			Help: "Fetch workers currently in-flight",
		}),
		WorkersTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "voi_follower_workers_total",
			Help: "Configured fetch worker pool size",
		}),
		InFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "voi_follower_inflight_rounds",
			Help: "Rounds fetched or fetching but not yet committed",
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
		m.FetchLatency,
		m.CommitLatency,
		m.WorkersBusy,
		m.WorkersTotal,
		m.InFlight,
	)
	m.windowStart.Store(time.Now().UnixNano())
	return m
}

// Handler returns the Prometheus HTTP handler for this metrics instance.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// SetWorkersTotal records the configured pool size.
func (m *Metrics) SetWorkersTotal(n int) {
	m.WorkersTotal.Set(float64(n))
}

// WorkerStart marks a fetch worker busy.
func (m *Metrics) WorkerStart() {
	m.WorkersBusy.Set(float64(m.busy.Add(1)))
}

// WorkerDone marks a fetch worker idle.
func (m *Metrics) WorkerDone() {
	m.WorkersBusy.Set(float64(m.busy.Add(-1)))
}

// SetInFlight updates the bounded-window occupancy gauge.
func (m *Metrics) SetInFlight(n int) {
	m.InFlight.Set(float64(n))
}

// ObserveFetch records fetch latency.
func (m *Metrics) ObserveFetch(d time.Duration) {
	m.FetchLatency.Observe(d.Seconds())
}

// ObserveCommit records commit latency.
func (m *Metrics) ObserveCommit(d time.Duration) {
	m.CommitLatency.Observe(d.Seconds())
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
