package consumer

import (
	"context"
	"fmt"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
	"github.com/NautilusOSS/voi-fast-follower/internal/stream"
)

// Cursor yields blocks in strict round order from any Source.
type Cursor struct {
	src  Source
	next uint64
	wait func(ctx context.Context, round uint64) (Block, error) // optional; archive follow
}

// NewCursor starts sequential consumption at startRound (inclusive).
func NewCursor(src Source, startRound uint64) *Cursor {
	return &Cursor{src: src, next: startRound}
}

// WithWait enables blocking Get for follow mode (e.g. Archive.GetWait).
func (c *Cursor) WithWait(fn func(ctx context.Context, round uint64) (Block, error)) *Cursor {
	c.wait = fn
	return c
}

// NextRound is the round that will be fetched next.
func (c *Cursor) NextRound() uint64 { return c.next }

// Next returns the next block. On ErrNotYetAvailable with a wait func configured,
// blocks until the round appears or ctx is canceled.
func (c *Cursor) Next(ctx context.Context) (Block, error) {
	for {
		blk, err := c.src.Get(ctx, c.next)
		if err == nil {
			if blk.Round != c.next {
				return Block{}, fmt.Errorf("%w: got %d want %d", ErrGap, blk.Round, c.next)
			}
			c.next++
			return blk, nil
		}
		if c.wait != nil && errorsIs(err, ErrNotYetAvailable) {
			blk, werr := c.wait(ctx, c.next)
			if werr != nil {
				return Block{}, werr
			}
			if blk.Round != c.next {
				return Block{}, fmt.Errorf("%w: got %d want %d", ErrGap, blk.Round, c.next)
			}
			c.next++
			return blk, nil
		}
		return Block{}, err
	}
}

// Range walks [start, end] inclusive using Source.Get.
func Range(ctx context.Context, src Source, start, end uint64, fn func(Block) error) error {
	if end < start {
		return fmt.Errorf("invalid range %d-%d", start, end)
	}
	var prev string
	for r := start; r <= end; r++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		blk, err := src.Get(ctx, r)
		if err != nil {
			return err
		}
		if prev != "" {
			if err := block.ValidateLinkage(prev, block.Block{
				Round:             blk.Round,
				BlockHash:         blk.BlockHash,
				PreviousBlockHash: blk.PreviousBlockHash,
			}); err != nil {
				return fmt.Errorf("%w: %v", ErrCorrupt, err)
			}
		}
		if err := fn(blk); err != nil {
			return err
		}
		prev = blk.BlockHash
	}
	return nil
}

// ArchiveRange walks an archive path without constructing a long-lived Source.
func ArchiveRange(ctx context.Context, archiveRoot string, start, end uint64, fn func(Block) error) error {
	return stream.Range(ctx, archiveRoot, start, end, func(b block.Block) error {
		return fn(fromInternal(b))
	})
}

func errorsIs(err, target error) bool {
	for err != nil {
		if err == target {
			return true
		}
		switch e := err.(type) {
		case *RoundError:
			err = e.Err
		default:
			return false
		}
	}
	return false
}
