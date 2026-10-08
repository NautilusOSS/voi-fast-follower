package stream

import (
	"context"
	"fmt"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
	"github.com/NautilusOSS/voi-fast-follower/internal/storage"
)

// ErrUnavailable indicates a round is not yet available from the source.
var ErrUnavailable = fmt.Errorf("stream: round unavailable")

// Range walks [start, end] inclusive from an archive root without Voi access.
func Range(ctx context.Context, archiveRoot string, start, end uint64, fn func(block.Block) error) error {
	return storage.IterateArchiveBlocks(ctx, archiveRoot, start, end, fn)
}

// RangeFrom walks [start, end] using Get on src in order. Stops with error on gaps.
func RangeFrom(ctx context.Context, src RoundSource, start, end uint64, fn func(block.Block) error) error {
	for r := start; r <= end; r++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		blk, ok, err := src.Get(ctx, r)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%w: %d", ErrUnavailable, r)
		}
		if err := fn(blk); err != nil {
			return err
		}
	}
	return nil
}
