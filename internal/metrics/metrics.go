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
//
// Phase 3 acquisition-vs-persistence signals:
//   - fetch throughput / latency
//   - ordered-buffer depth (pending contiguous backlog)
//   - blocks waiting for commit
//   - commit throughput / latency
//   - checkpoint lag + tip lag
type Metrics struct {
	registry           *prometheus.Registry
	CurrentRound       prometheus.Gauge
	TargetRound        prometheus.Gauge
	LastProcessedRound prometheus.Gauge
	CatchupLag         prometheus.Gauge
	CheckpointLag      prometheus.Gauge
	BlocksPerSecond    prometheus.Gauge
	FetchBlocksPerSec  prometheus.Gauge
	CommitBlocksPerSec prometheus.Gauge
	BlocksProcessed    prometheus.Counter
	BlocksFetched      prometheus.Counter
	Errors             prometheus.Counter
	FetchLatency       prometheus.Histogram
	CommitLatency      prometheus.Histogram
	WorkersBusy        prometheus.Gauge
	WorkersTotal       prometheus.Gauge
	InFlight           prometheus.Gauge
	OrderedBufferDepth prometheus.Gauge
	AwaitingCommit     prometheus.Gauge
	ArchiveBytes       prometheus.Counter
	ArchiveErrors      prometheus.Counter

	processed     atomic.Uint64
	fetched       atomic.Uint64
	windowStart   atomic.Int64
	windowCount   atomic.Uint64
	fetchWinStart atomic.Int64
	fetchWinCount atomic.Uint64
	busy          atomic.Int64
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
			Help: "target_round - current_round (tip lag vs commit cursor)",
		}),
		CheckpointLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "voi_follower_checkpoint_lag",
			Help: "target_round - last_processed_round",
		}),
		BlocksPerSecond: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "voi_follower_blocks_per_second",
			Help: "Recent commit throughput (blocks/sec)",
		}),
		FetchBlocksPerSec: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "voi_follower_fetch_blocks_per_second",
			Help: "Recent successful fetch throughput (blocks/sec)",
		}),
		CommitBlocksPerSec: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "voi_follower_commit_blocks_per_second",
			Help: "Alias of blocks_per_second (commit throughput)",
		}),
		BlocksProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "voi_follower_blocks_processed_total",
			Help: "Total blocks successfully committed",
		}),
		BlocksFetched: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "voi_follower_blocks_fetched_total",
			Help: "Total blocks successfully fetched+decoded",
		}),
		Errors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "voi_follower_errors_total",
			Help: "Total processing errors",
		}),
		FetchLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "voi_follower_fetch_latency_seconds",
			Help:    "BlockRaw fetch + decode latency",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}),
		CommitLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "voi_follower_commit_latency_seconds",
			Help:    "Sink Commit/CommitBatch latency",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5},
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
		OrderedBufferDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "voi_follower_ordered_buffer_depth",
			Help: "Fetched blocks held in the ordered buffer awaiting contiguous commit",
		}),
		AwaitingCommit: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "voi_follower_awaiting_commit",
			Help: "Contiguous ready rounds waiting to be flushed to the sink",
		}),
		ArchiveBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "voi_follower_archive_bytes_written_total",
			Help: "Total bytes appended to the archive sink",
		}),
		ArchiveErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "voi_follower_archive_errors_total",
			Help: "Archive sink errors",
		}),
	}
	reg.MustRegister(
		m.CurrentRound,
		m.TargetRound,
		m.LastProcessedRound,
		m.CatchupLag,
		m.CheckpointLag,
		m.BlocksPerSecond,
		m.FetchBlocksPerSec,
		m.CommitBlocksPerSec,
		m.BlocksProcessed,
		m.BlocksFetched,
		m.Errors,
		m.FetchLatency,
		m.CommitLatency,
		m.WorkersBusy,
		m.WorkersTotal,
		m.InFlight,
		m.OrderedBufferDepth,
		m.AwaitingCommit,
		m.ArchiveBytes,
		m.ArchiveErrors,
	)
	now := time.Now().UnixNano()
	m.windowStart.Store(now)
	m.fetchWinStart.Store(now)
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

// SetBufferDepth records ordered-buffer and contiguous-ready depths.
func (m *Metrics) SetBufferDepth(ordered, awaitingCommit int) {
	m.OrderedBufferDepth.Set(float64(ordered))
	m.AwaitingCommit.Set(float64(awaitingCommit))
}

// ObserveFetch records fetch latency and fetch throughput.
func (m *Metrics) ObserveFetch(d time.Duration) {
	m.FetchLatency.Observe(d.Seconds())
}

// RecordFetch increments successful fetch counters/throughput.
func (m *Metrics) RecordFetch() {
	m.BlocksFetched.Inc()
	m.fetched.Add(1)
	m.fetchWinCount.Add(1)

	now := time.Now().UnixNano()
	start := m.fetchWinStart.Load()
	elapsed := time.Duration(now - start)
	if elapsed >= time.Second {
		count := m.fetchWinCount.Swap(0)
		m.fetchWinStart.Store(now)
		m.FetchBlocksPerSec.Set(float64(count) / elapsed.Seconds())
	}
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
	var ckpt float64
	if target >= lastProcessed {
		ckpt = float64(target - lastProcessed)
	}
	m.CheckpointLag.Set(ckpt)
}

// RecordBlock updates commit throughput counters after a successful commit.
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
		m.CommitBlocksPerSec.Set(bps)
	}
}

// RecordError increments the error counter.
func (m *Metrics) RecordError() {
	m.Errors.Inc()
}

// RecordArchiveBytes increments archive bytes written.
func (m *Metrics) RecordArchiveBytes(n int) {
	if n > 0 {
		m.ArchiveBytes.Add(float64(n))
	}
}

// RecordArchiveError increments archive error counter.
func (m *Metrics) RecordArchiveError() {
	m.ArchiveErrors.Inc()
}

// ProcessedTotal returns the number of blocks recorded in-process.
func (m *Metrics) ProcessedTotal() uint64 {
	return m.processed.Load()
}

// FetchedTotal returns successful fetch count.
func (m *Metrics) FetchedTotal() uint64 {
	return m.fetched.Load()
}
