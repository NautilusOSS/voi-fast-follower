package storage

import (
	"context"

	"github.com/nicholasshellabarger/voi-fast-follower/internal/block"
)

// BlockSink is the consumer abstraction for ingested blocks.
// Implementations must be safe for at-least-once delivery (idempotent writes).
type BlockSink interface {
	ProcessBlock(ctx context.Context, blk block.Block) error
	LastProcessedRound(ctx context.Context) (round uint64, ok bool, err error)
	MaxBlockRound(ctx context.Context) (round uint64, ok bool, err error)
	BlockHash(ctx context.Context, round uint64) (hash string, ok bool, err error)
	Close() error
}
