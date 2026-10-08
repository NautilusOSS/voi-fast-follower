package consumer

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ProcessOptions configures sequential block processing.
type ProcessOptions struct {
	Source   Source
	Start    uint64
	End      uint64 // 0 = follow until ctx cancel
	Checkpoint CheckpointFile
	OnBlock  func(Block) error
	// WaitPoll is used when Source returns ErrNotYetAvailable (archive follow).
	WaitPoll time.Duration
	// Archive enables GetWait when non-nil and Source is *Archive.
	Archive *Archive
}

// Process reads blocks in order, invoking OnBlock for each.
// Checkpoints advance only after OnBlock succeeds (at-least-once safe on retry).
func Process(ctx context.Context, opts ProcessOptions) (processed uint64, err error) {
	if opts.Source == nil || opts.OnBlock == nil {
		return 0, fmt.Errorf("consumer: Source and OnBlock required")
	}
	start := opts.Start
	if cpStart, err := opts.Checkpoint.ResumeRound(start); err != nil {
		return 0, err
	} else if cpStart > start {
		start = cpStart
	}

	poll := opts.WaitPoll
	if poll <= 0 {
		poll = 200 * time.Millisecond
	}

	cur := NewCursor(opts.Source, start)
	if opts.End == 0 {
		cur = cur.WithWait(waitFunc(opts.Source, opts.Archive, poll))
	}

	for {
		if opts.End > 0 && cur.NextRound() > opts.End {
			break
		}
		select {
		case <-ctx.Done():
			return processed, ctx.Err()
		default:
		}

		blk, err := cur.Next(ctx)
		if err != nil {
			if opts.End == 0 && errors.Is(err, context.Canceled) {
				return processed, nil
			}
			return processed, err
		}
		if err := opts.OnBlock(blk); err != nil {
			return processed, err
		}
		if err := opts.Checkpoint.Save(blk.Round); err != nil {
			return processed, err
		}
		processed++
	}
	return processed, nil
}

func waitFunc(src Source, arch *Archive, poll time.Duration) func(context.Context, uint64) (Block, error) {
	return func(ctx context.Context, round uint64) (Block, error) {
		if arch != nil {
			if _, ok := src.(*Archive); ok {
				return arch.GetWait(ctx, round, WaitOptions{PollInterval: poll})
			}
		}
		ticker := time.NewTicker(poll)
		defer ticker.Stop()
		for {
			blk, err := src.Get(ctx, round)
			if err == nil {
				return blk, nil
			}
			if !errorsIs(err, ErrNotYetAvailable) {
				return Block{}, err
			}
			select {
			case <-ctx.Done():
				return Block{}, ctx.Err()
			case <-ticker.C:
			}
		}
	}
}
