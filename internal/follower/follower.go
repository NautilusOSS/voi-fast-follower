package follower

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
	"github.com/NautilusOSS/voi-fast-follower/internal/config"
	"github.com/NautilusOSS/voi-fast-follower/internal/health"
	"github.com/NautilusOSS/voi-fast-follower/internal/metrics"
	"github.com/NautilusOSS/voi-fast-follower/internal/storage"
)

// errUntilReached is returned internally when WithUntilRound is satisfied.
var errUntilReached = errors.New("until round reached")

// Engine runs catch-up and live following with concurrent prefetch and
// ordered sequential (optionally batched) commits.
//
// Concurrency model:
//
//	orchestrator ──jobs──► workers ──results──► ordered buffer ──► BatchBlockSink
//
// Invariants:
//   - At most FetchWindow rounds may be in-flight (dispatched but not committed).
//   - Commits are strictly sequential / contiguous: never advance past a gap.
//   - Out-of-order fetch completion is buffered until the next expected round arrives.
//   - Catch-up may CommitBatch multiple contiguous rounds atomically.
//   - Live follow uses batch size 1 for low latency.
//   - every committed round is contiguous; the checkpoint always represents a
//     fully committed round (the final round of the last successful batch).
type Engine struct {
	cfg     *config.Config
	src     BlockSource
	sink    storage.BatchBlockSink
	metrics *metrics.Metrics
	health  *health.Tracker
	log     *slog.Logger

	// untilRound, when non-zero, stops the engine after that round is committed
	// (used by benchmarks; production leaves this at 0 = follow forever).
	untilRound uint64

	// commitTimeout bounds a single CommitBatch during graceful shutdown.
	commitTimeout time.Duration
}

// New creates a follower engine. src is typically *voi.Client.
func New(cfg *config.Config, src BlockSource, sink storage.BlockSink, m *metrics.Metrics, log *slog.Logger) *Engine {
	if log == nil {
		log = slog.Default()
	}
	if m == nil {
		m = metrics.Default()
	}
	return &Engine{
		cfg:           cfg,
		src:           src,
		sink:          storage.AsBatchSink(sink),
		metrics:       m,
		health:        health.New(),
		log:           log,
		commitTimeout: 30 * time.Second,
	}
}

// WithHealth attaches an external health tracker (shared with HTTP handlers).
func (e *Engine) WithHealth(h *health.Tracker) *Engine {
	if h != nil {
		e.health = h
	}
	return e
}

// Health returns the engine health tracker.
func (e *Engine) Health() *health.Tracker { return e.health }

// WithUntilRound configures a finite catch-up that exits after committing round.
func (e *Engine) WithUntilRound(round uint64) *Engine {
	e.untilRound = round
	return e
}

type fetchResult struct {
	round   uint64
	blk     block.Block
	err     error
	latency time.Duration
}

// Run blocks until ctx is cancelled or a fatal error occurs.
func (e *Engine) Run(ctx context.Context) error {
	next, err := e.resolveStartRound(ctx)
	if err != nil {
		return err
	}
	e.log.Info("follower starting",
		"next_round", next,
		"mode", e.cfg.Sync.Mode,
		"workers", e.cfg.Sync.Workers,
		"fetch_window", e.cfg.Sync.FetchWindow,
		"commit_batch_size", e.cfg.Sync.CommitBatchSize,
		"network", e.cfg.Network,
	)

	tip, err := e.src.LastRound(ctx)
	if err != nil {
		return fmt.Errorf("initial tip: %w", err)
	}
	next = e.recoverIfCheckpointAhead(ctx, next, tip)

	last := uint64(0)
	if next > 0 {
		last = next - 1
	}
	e.metrics.SetRounds(next, tip, last)
	e.metrics.SetWorkersTotal(e.cfg.Sync.Workers)

	err = e.loop(ctx, next)
	if errors.Is(err, errUntilReached) {
		return nil
	}
	return err
}

func (e *Engine) recoverIfCheckpointAhead(ctx context.Context, next, tip uint64) uint64 {
	if next <= tip+1000 {
		return next
	}
	if maxRound, ok, mErr := e.sink.MaxBlockRound(ctx); mErr == nil && ok {
		if maxRound > tip {
			e.log.Warn("checkpoint ahead of tip; restarting from tip",
				"checkpoint_next", next, "tip", tip, "max_stored", maxRound)
			return tip
		}
		e.log.Warn("checkpoint ahead of tip; recovering from max stored block",
			"checkpoint_next", next, "tip", tip, "max_stored", maxRound)
		return maxRound + 1
	}
	e.log.Warn("checkpoint ahead of tip; restarting from tip",
		"checkpoint_next", next, "tip", tip)
	return tip
}

func (e *Engine) resolveStartRound(ctx context.Context) (uint64, error) {
	last, ok, err := e.sink.LastProcessedRound(ctx)
	if err != nil {
		return 0, fmt.Errorf("read checkpoint: %w", err)
	}
	if ok {
		e.log.Info("resuming from checkpoint", "last_processed_round", last)
		return last + 1, nil
	}

	if explicit, yes, err := e.cfg.ExplicitStartRound(); err != nil {
		return 0, err
	} else if yes {
		e.log.Info("starting from configured round", "start_round", explicit)
		return explicit, nil
	}

	tip, err := e.src.LastRound(ctx)
	if err != nil {
		return 0, fmt.Errorf("resolve latest start: %w", err)
	}
	e.log.Info("starting from network tip (latest)", "start_round", tip)
	return tip, nil
}

func (e *Engine) loop(ctx context.Context, nextCommit uint64) error {
	workers := e.cfg.Sync.Workers
	window := e.cfg.Sync.FetchWindow
	if workers < 1 {
		workers = 1
	}
	if window < workers {
		window = workers
	}

	jobs := make(chan uint64, window)
	results := make(chan fetchResult, window)

	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.fetchWorker(workerCtx, jobs, results)
		}()
	}
	defer func() {
		workerCancel()
		close(jobs)
		wg.Wait()
	}()

	pending := make(map[uint64]block.Block)
	nextFetch := nextCommit
	inFlight := 0
	var tip uint64
	atTipLogged := false

	var prevHash string
	var havePrev bool
	if nextCommit > 0 {
		if h, ok, err := e.sink.BlockHash(ctx, nextCommit-1); err == nil && ok {
			prevHash = h
			havePrev = true
		}
	}

	// Tracks when the commit head first became available for flush-interval.
	var headReadyAt time.Time

	refreshTip := func() error {
		t, err := e.src.LastRound(ctx)
		if err != nil {
			return err
		}
		tip = t
		if e.untilRound > 0 && e.untilRound < tip {
			tip = e.untilRound
		}
		lastProcessed := uint64(0)
		if nextCommit > 0 {
			lastProcessed = nextCommit - 1
		}
		e.metrics.SetRounds(nextCommit, tip, lastProcessed)
		e.health.UpdateProgress(nextCommit, tip, lastProcessed, nextCommit > 0)
		return nil
	}

	if err := refreshTip(); err != nil {
		e.health.SetFailed(err.Error())
		return fmt.Errorf("initial tip: %w", err)
	}

	updateInFlight := func() {
		e.metrics.SetInFlight(inFlight + len(pending))
		awaiting := 0
		for r := nextCommit; ; r++ {
			if _, ok := pending[r]; !ok {
				break
			}
			awaiting++
		}
		e.metrics.SetBufferDepth(len(pending), awaiting)
	}

	tryFlush := func(force bool) error {
		atTip := nextFetch > tip && inFlight == 0
		batch, newPrev, err := e.collectBatch(pending, nextCommit, prevHash, havePrev, atTip || force)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			headReadyAt = time.Time{}
			return nil
		}

		if !force && !e.shouldFlush(batch, nextCommit, pending, inFlight, nextFetch, tip, headReadyAt) {
			if headReadyAt.IsZero() {
				headReadyAt = time.Now()
			}
			return nil
		}

		for _, blk := range batch {
			delete(pending, blk.Round)
		}
		updateInFlight()

		start := time.Now()
		if err := e.commitBatch(ctx, batch); err != nil {
			// Put the whole batch back; checkpoint must not have advanced.
			for _, blk := range batch {
				pending[blk.Round] = blk
			}
			updateInFlight()
			e.metrics.RecordError()
			e.health.SetSinkError(err.Error())
			e.log.Error("batch commit failed; will retry",
				"from", batch[0].Round, "to", batch[len(batch)-1].Round, "err", err)
			if sleepErr := sleep(ctx, 500*time.Millisecond); sleepErr != nil {
				return sleepErr
			}
			return nil
		}
		e.metrics.ObserveCommit(time.Since(start))
		e.health.ClearSinkError()

		final := batch[len(batch)-1]
		prevHash = newPrev
		havePrev = true
		nextCommit = final.Round + 1
		headReadyAt = time.Time{}
		for range batch {
			e.metrics.RecordBlock()
		}
		e.metrics.SetRounds(nextCommit, tip, final.Round)
		e.health.UpdateProgress(nextCommit, tip, final.Round, true)
		updateInFlight()
		e.log.Debug("committed batch",
			"from", batch[0].Round,
			"to", final.Round,
			"size", len(batch),
			"latency", time.Since(start),
		)

		if e.untilRound > 0 && final.Round >= e.untilRound {
			e.log.Info("reached until_round; stopping", "round", final.Round)
			return errUntilReached
		}
		return nil
	}

	handleResult := func(res fetchResult) error {
		inFlight--
		if inFlight < 0 {
			inFlight = 0
		}
		e.metrics.ObserveFetch(res.latency)

		if res.err != nil {
			e.metrics.RecordError()
			e.log.Error("fetch failed; will retry", "round", res.round, "err", res.err)
			if sleepErr := sleep(ctx, e.cfg.Sync.PollInterval); sleepErr != nil {
				return sleepErr
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case jobs <- res.round:
				inFlight++
			}
			updateInFlight()
			return nil
		}

		if res.blk.Round != res.round {
			e.metrics.RecordError()
			return fmt.Errorf("algod returned round %d for requested %d", res.blk.Round, res.round)
		}
		pending[res.round] = res.blk
		e.metrics.RecordFetch()
		if res.round == nextCommit && headReadyAt.IsZero() {
			headReadyAt = time.Now()
		}
		updateInFlight()

		return tryFlush(false)
	}

	for {
		if err := ctx.Err(); err != nil {
			e.drainOnShutdown(pending, &nextCommit, &prevHash, &havePrev, &inFlight, tip, updateInFlight)
			return err
		}

		outstanding := inFlight + len(pending)
		canDispatch := nextFetch <= tip && outstanding < window

		// Live follow: at tip with an empty pipeline — flush any remainder first.
		if !canDispatch && nextFetch > tip && outstanding == 0 {
			if err := tryFlush(true); err != nil {
				return err
			}
			if !atTipLogged {
				e.log.Info("caught up to tip; entering live follow", "tip", tip)
				atTipLogged = true
			}
			newTip, err := e.src.WaitForBlockAfter(ctx, tip)
			if err != nil {
				e.metrics.RecordError()
				e.log.Warn("wait-for-block failed; polling", "err", err)
				if sleepErr := sleep(ctx, e.cfg.Sync.PollInterval); sleepErr != nil {
					return sleepErr
				}
				if err := refreshTip(); err != nil {
					e.metrics.RecordError()
					e.log.Error("failed to fetch tip", "err", err)
				}
				continue
			}
			if newTip > tip {
				tip = newTip
			} else if err := refreshTip(); err != nil {
				e.metrics.RecordError()
			}
			lastProcessed := uint64(0)
			if nextCommit > 0 {
				lastProcessed = nextCommit - 1
			}
			e.metrics.SetRounds(nextCommit, tip, lastProcessed)
			continue
		}

		// Optional time-based partial flush (catch-up only).
		if e.cfg.Sync.CommitFlushInterval > 0 && !headReadyAt.IsZero() {
			if time.Since(headReadyAt) >= e.cfg.Sync.CommitFlushInterval {
				if err := tryFlush(false); err != nil {
					return err
				}
				continue
			}
		}

		if canDispatch {
			select {
			case <-ctx.Done():
				e.drainOnShutdown(pending, &nextCommit, &prevHash, &havePrev, &inFlight, tip, updateInFlight)
				return ctx.Err()
			case jobs <- nextFetch:
				nextFetch++
				inFlight++
				updateInFlight()
			case res := <-results:
				if err := handleResult(res); err != nil {
					return err
				}
			}
			continue
		}

		// Window full / waiting on fetches. Cap wait so flush-interval can fire.
		wait := 50 * time.Millisecond
		if e.cfg.Sync.CommitFlushInterval > 0 && !headReadyAt.IsZero() {
			if rem := e.cfg.Sync.CommitFlushInterval - time.Since(headReadyAt); rem > 0 && rem < wait {
				wait = rem
			}
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			e.drainOnShutdown(pending, &nextCommit, &prevHash, &havePrev, &inFlight, tip, updateInFlight)
			return ctx.Err()
		case res := <-results:
			timer.Stop()
			if err := handleResult(res); err != nil {
				return err
			}
			if nextFetch > tip {
				if err := refreshTip(); err != nil {
					e.metrics.RecordError()
					e.log.Warn("tip refresh failed", "err", err)
				}
			}
		case <-timer.C:
			if err := tryFlush(false); err != nil {
				return err
			}
		}
	}
}

// collectBatch gathers a contiguous validated prefix from pending.
// When forceOrLive is true, the max size is 1 (live / tip flush).
func (e *Engine) collectBatch(
	pending map[uint64]block.Block,
	nextCommit uint64,
	prevHash string,
	havePrev bool,
	forceOrLive bool,
) ([]block.Block, string, error) {
	max := e.cfg.Sync.CommitBatchSize
	if max < 1 {
		max = 1
	}
	if forceOrLive {
		max = 1
	}

	batch := make([]block.Block, 0, max)
	hash := prevHash
	have := havePrev

	for len(batch) < max {
		r := nextCommit + uint64(len(batch))
		blk, ok := pending[r]
		if !ok {
			break
		}
		if err := validateBlock(blk, hash, have); err != nil {
			return nil, prevHash, err
		}
		batch = append(batch, blk)
		hash = blk.BlockHash
		have = true
		if e.untilRound > 0 && blk.Round >= e.untilRound {
			break
		}
	}
	return batch, hash, nil
}

func (e *Engine) shouldFlush(
	batch []block.Block,
	nextCommit uint64,
	pending map[uint64]block.Block,
	inFlight int,
	nextFetch, tip uint64,
	headReadyAt time.Time,
) bool {
	max := e.cfg.Sync.CommitBatchSize
	if max < 1 {
		max = 1
	}
	if len(batch) >= max {
		return true
	}
	if e.untilRound > 0 && batch[len(batch)-1].Round >= e.untilRound {
		return true
	}

	atTip := nextFetch > tip && inFlight == 0
	if atTip {
		return true
	}

	// Contiguous prefix cannot grow: next round is neither pending nor still fetching.
	nextNeeded := nextCommit + uint64(len(batch))
	if _, ok := pending[nextNeeded]; !ok {
		stillComing := inFlight > 0 || nextFetch <= nextNeeded
		if !stillComing {
			return true
		}
	}

	if e.cfg.Sync.CommitFlushInterval > 0 && !headReadyAt.IsZero() {
		if time.Since(headReadyAt) >= e.cfg.Sync.CommitFlushInterval {
			return true
		}
	}
	return false
}

// commitBatch persists a batch with a timeout detached from fetch cancellation so
// an in-flight durable commit can finish during graceful shutdown.
func (e *Engine) commitBatch(ctx context.Context, batch []block.Block) error {
	timeout := e.commitTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	return e.sink.CommitBatch(cctx, batch)
}

// drainOnShutdown stops dispatching and attempts one best-effort flush of already
// buffered contiguous rounds. Incomplete work is not forced; checkpoint only moves
// for successfully committed batches.
func (e *Engine) drainOnShutdown(
	pending map[uint64]block.Block,
	nextCommit *uint64,
	prevHash *string,
	havePrev *bool,
	inFlight *int,
	tip uint64,
	updateInFlight func(),
) {
	e.log.Info("shutdown: draining ordered buffer", "pending", len(pending), "inflight", *inFlight)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(pending) > 0 {
		batch, newPrev, err := e.collectBatch(pending, *nextCommit, *prevHash, *havePrev, true)
		if err != nil || len(batch) == 0 {
			break
		}
		for _, blk := range batch {
			delete(pending, blk.Round)
		}
		if err := e.commitBatch(context.Background(), batch); err != nil {
			for _, blk := range batch {
				pending[blk.Round] = blk
			}
			e.log.Warn("shutdown: commit aborted; checkpoint unchanged",
				"from", batch[0].Round, "err", err)
			break
		}
		final := batch[len(batch)-1]
		*prevHash = newPrev
		*havePrev = true
		*nextCommit = final.Round + 1
		for range batch {
			e.metrics.RecordBlock()
		}
		e.metrics.SetRounds(*nextCommit, tip, final.Round)
		e.health.UpdateProgress(*nextCommit, tip, final.Round, true)
		updateInFlight()
	}
}

func (e *Engine) fetchWorker(ctx context.Context, jobs <-chan uint64, results chan<- fetchResult) {
	for {
		select {
		case <-ctx.Done():
			return
		case round, ok := <-jobs:
			if !ok {
				return
			}
			e.metrics.WorkerStart()
			start := time.Now()
			blk, err := e.src.GetBlock(ctx, round)
			latency := time.Since(start)
			e.metrics.WorkerDone()

			res := fetchResult{round: round, blk: blk, err: err, latency: latency}
			select {
			case <-ctx.Done():
				return
			case results <- res:
			}
		}
	}
}

func validateBlock(blk block.Block, prevHash string, havePrev bool) error {
	if blk.BlockHash == "" {
		return fmt.Errorf("missing block hash at round %d", blk.Round)
	}
	if havePrev && prevHash != "" && blk.PreviousBlockHash != "" && prevHash != blk.PreviousBlockHash {
		return fmt.Errorf("chain break at round %d: prev=%s stored=%s",
			blk.Round, blk.PreviousBlockHash, prevHash)
	}
	return nil
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		d = 100 * time.Millisecond
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
