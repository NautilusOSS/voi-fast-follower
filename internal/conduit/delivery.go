package conduit

import (
	"context"
	"fmt"
	"time"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
	"github.com/NautilusOSS/voi-fast-follower/internal/stream"
)

// DeliverFunc is invoked with a translated block. Returning an error means the
// round was not successfully delivered (cursor must not advance).
type DeliverFunc func(ctx context.Context, blk block.Block, data BlockData) error

// Adapter pulls ordered blocks from a RoundSource, translates them, and delivers
// them to a Conduit-shaped consumer. It owns translation only — not acquisition.
type Adapter struct {
	Src     stream.RoundSource
	Deliver DeliverFunc
	Metrics *Metrics
}

// DeliverRound fetches one round, translates, and delivers.
// On any error the round is not considered delivered.
func (a *Adapter) DeliverRound(ctx context.Context, round uint64) error {
	if a.Src == nil || a.Deliver == nil {
		return fmt.Errorf("conduit: adapter missing Src or Deliver")
	}
	start := time.Now()
	blk, ok, err := a.Src.Get(ctx, round)
	if err != nil {
		a.observeErr()
		return err
	}
	if !ok {
		a.observeErr()
		return fmt.Errorf("conduit: round %d not available", round)
	}
	data, err := Translate(blk)
	if err != nil {
		a.observeErr()
		return err
	}
	if err := a.Deliver(ctx, blk, data); err != nil {
		a.observeErr()
		return err
	}
	if a.Metrics != nil {
		a.Metrics.BlocksDelivered.Inc()
		a.Metrics.DeliveryLatency.Observe(time.Since(start).Seconds())
		if cp, cpOK, _ := a.Src.Checkpoint(ctx); cpOK && cp >= round {
			a.Metrics.Lag.Set(float64(cp - round))
		}
	}
	return nil
}

// DeliverRange delivers [from, to] inclusive in order. Stops on first error
// without skipping rounds.
func (a *Adapter) DeliverRange(ctx context.Context, from, to uint64) error {
	if to < from {
		return fmt.Errorf("conduit: invalid range %d-%d", from, to)
	}
	for r := from; r <= to; r++ {
		if err := a.DeliverRound(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

func (a *Adapter) observeErr() {
	if a.Metrics != nil {
		a.Metrics.DeliveryErrors.Inc()
	}
}
