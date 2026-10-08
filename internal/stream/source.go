// Package stream exposes a consumer-facing ordered block source boundary.
//
// Downstream adapters (including the optional Conduit importer) consume blocks
// without depending on the follower's fetch worker pool or ordered buffer.
package stream

import (
	"context"
	"fmt"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
)

// RoundSource serves canonical blocks by round from a durable store.
// Implementations must not contact Voi / algod for historical catch-up.
type RoundSource interface {
	// Get returns the block at round. ok=false means the round is not yet
	// durable (or never will be past end-of-range for offline sources).
	Get(ctx context.Context, round uint64) (blk block.Block, ok bool, err error)

	// Checkpoint is the highest durable round, if any.
	Checkpoint(ctx context.Context) (round uint64, ok bool, err error)
}

// Cursor yields blocks in strict round order: N, N+1, N+2, …
type Cursor struct {
	src  RoundSource
	next uint64
}

// NewCursor starts a sequential cursor at startRound (inclusive).
func NewCursor(src RoundSource, startRound uint64) *Cursor {
	return &Cursor{src: src, next: startRound}
}

// NextRound returns the round that will be requested by the next Next call.
func (c *Cursor) NextRound() uint64 { return c.next }

// Next returns the next contiguous block.
// If the round is not yet available, ok=false and the cursor does not advance.
func (c *Cursor) Next(ctx context.Context) (blk block.Block, ok bool, err error) {
	blk, ok, err = c.src.Get(ctx, c.next)
	if err != nil || !ok {
		return blk, ok, err
	}
	if blk.Round != c.next {
		return block.Block{}, false, fmt.Errorf("stream: got round %d want %d", blk.Round, c.next)
	}
	c.next++
	return blk, true, nil
}
