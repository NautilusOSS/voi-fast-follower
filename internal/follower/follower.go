package follower

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/nicholasshellabarger/voi-fast-follower/internal/block"
	"github.com/nicholasshellabarger/voi-fast-follower/internal/config"
	"github.com/nicholasshellabarger/voi-fast-follower/internal/metrics"
	"github.com/nicholasshellabarger/voi-fast-follower/internal/storage"
	"github.com/nicholasshellabarger/voi-fast-follower/internal/voi"
)

// Engine runs catch-up and live following with concurrent prefetch
// and ordered sequential commits.
type Engine struct {
	cfg     *config.Config
	client  *voi.Client
	sink    storage.BlockSink
	metrics *metrics.Metrics
	log     *slog.Logger
}

// New creates a follower engine.
func New(cfg *config.Config, client *voi.Client, sink storage.BlockSink, m *metrics.Metrics, log *slog.Logger) *Engine {
	if log == nil {
		log = slog.Default()
	}
	if m == nil {
		m = metrics.Default()
	}
	return &Engine{cfg: cfg, client: client, sink: sink, metrics: m, log: log}
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
		"prefetch_workers", e.cfg.Sync.PrefetchWorkers,
		"network", e.cfg.Network,
	)

	tip, err := e.client.LastRound(ctx)
	if err != nil {
		return fmt.Errorf("initial tip: %w", err)
	}
	if next > tip+1000 {
		// Recover from a polluted checkpoint (e.g. leftover test row) by
		// resuming from the highest real block at/under tip, or the tip.
		if maxRound, ok, mErr := e.sink.MaxBlockRound(ctx); mErr == nil && ok {
			if maxRound > tip {
				// Ignore absurd stored rounds (test fixtures).
				e.log.Warn("checkpoint ahead of tip; restarting from tip",
					"checkpoint_next", next, "tip", tip, "max_stored", maxRound)
				next = tip
			} else {
				e.log.Warn("checkpoint ahead of tip; recovering from max stored block",
					"checkpoint_next", next, "tip", tip, "max_stored", maxRound)
				next = maxRound + 1
			}
		} else {
			e.log.Warn("checkpoint ahead of tip; restarting from tip",
				"checkpoint_next", next, "tip", tip)
			next = tip
		}
	}
	last := uint64(0)
	if next > 0 {
		last = next - 1
	}
	e.metrics.SetRounds(next, tip, last)

	return e.loop(ctx, next)
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

	tip, err := e.client.LastRound(ctx)
	if err != nil {
		return 0, fmt.Errorf("resolve latest start: %w", err)
	}
	e.log.Info("starting from network tip (latest)", "start_round", tip)
	return tip, nil
}

func (e *Engine) loop(ctx context.Context, next uint64) error {
	workers := e.cfg.Sync.PrefetchWorkers
	buffer := e.cfg.Sync.PrefetchBuffer
	if workers < 1 {
		workers = 1
	}
	if buffer < 1 {
		buffer = workers
	}

	atTipLogged := false

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		tip, err := e.client.LastRound(ctx)
		if err != nil {
			e.metrics.RecordError()
			e.log.Error("failed to fetch tip", "err", err)
			if sleepErr := sleep(ctx, e.cfg.Sync.PollInterval); sleepErr != nil {
				return sleepErr
			}
			continue
		}

		lastProcessed := uint64(0)
		if next > 0 {
			lastProcessed = next - 1
		}
		e.metrics.SetRounds(next, tip, lastProcessed)

		if next > tip {
			if !atTipLogged {
				e.log.Info("caught up to tip; entering live follow", "tip", tip)
				atTipLogged = true
			}
			st, err := e.client.WaitForBlockAfter(ctx, tip)
			if err != nil {
				e.metrics.RecordError()
				e.log.Warn("wait-for-block failed; polling", "err", err)
				if sleepErr := sleep(ctx, e.cfg.Sync.PollInterval); sleepErr != nil {
					return sleepErr
				}
				continue
			}
			tip = st.LastRound
			if next > tip {
				continue
			}
		}

		end := next + uint64(buffer) - 1
		if end > tip {
			end = tip
		}

		blocks, err := e.fetchRange(ctx, next, end, workers)
		if err != nil {
			e.metrics.RecordError()
			e.log.Error("prefetch batch failed; will retry",
				"from", next, "to", end, "err", err)
			if sleepErr := sleep(ctx, e.cfg.Sync.PollInterval); sleepErr != nil {
				return sleepErr
			}
			continue
		}

		batchStart := next
		for i, blk := range blocks {
			round := batchStart + uint64(i)
			if blk.Round != round {
				e.metrics.RecordError()
				return fmt.Errorf("unexpected block round %d (want %d)", blk.Round, round)
			}
			if err := e.commit(ctx, blk); err != nil {
				e.metrics.RecordError()
				e.log.Error("commit failed; will retry", "round", round, "err", err)
				if sleepErr := sleep(ctx, 500*time.Millisecond); sleepErr != nil {
					return sleepErr
				}
				break
			}
			next = round + 1
			e.metrics.SetRounds(next, tip, round)
			e.metrics.RecordBlock()
		}
	}
}

func (e *Engine) fetchRange(ctx context.Context, from, to uint64, workers int) ([]block.Block, error) {
	if to < from {
		return nil, nil
	}
	n := int(to - from + 1)
	out := make([]block.Block, n)
	var mu sync.Mutex

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)
	for i := 0; i < n; i++ {
		i := i
		round := from + uint64(i)
		g.Go(func() error {
			blk, err := e.client.FetchBlock(gctx, round)
			if err != nil {
				return fmt.Errorf("fetch round %d: %w", round, err)
			}
			if blk.Round != round {
				return fmt.Errorf("algod returned round %d for requested %d", blk.Round, round)
			}
			mu.Lock()
			out[i] = blk
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return out, nil
}

func (e *Engine) commit(ctx context.Context, blk block.Block) error {
	if blk.Round > 0 {
		prevHash, ok, err := e.sink.BlockHash(ctx, blk.Round-1)
		if err != nil {
			return err
		}
		if ok && prevHash != "" && blk.PreviousBlockHash != "" && prevHash != blk.PreviousBlockHash {
			return fmt.Errorf("chain break at round %d: prev=%s stored=%s",
				blk.Round, blk.PreviousBlockHash, prevHash)
		}
	}

	if err := e.sink.ProcessBlock(ctx, blk); err != nil {
		return err
	}
	e.log.Debug("committed block",
		"round", blk.Round,
		"hash", blk.BlockHash,
		"txns", blk.TxnCount,
	)
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
