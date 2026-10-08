package storage

import (
	"context"
	"fmt"

	"github.com/nicholasshellabarger/voi-fast-follower/internal/block"
)

// BlockSink is the storage abstraction for ingested blocks.
//
// Synchronization (fetch/order) stays outside this interface so future
// consumers can swap Postgres for Conduit, Kafka, object storage, etc.
//
// Implementations MUST tolerate at-least-once delivery (idempotent writes).
// Commit MUST atomically persist the block data and advance last_processed_round.
type BlockSink interface {
	// Commit persists blk and advances the checkpoint to blk.Round.
	// It must be safe to call again with the same round (idempotent).
	Commit(ctx context.Context, blk block.Block) error

	// LastProcessedRound returns the durable checkpoint, if any.
	LastProcessedRound(ctx context.Context) (round uint64, ok bool, err error)

	// MaxBlockRound returns the highest stored block round, if any.
	MaxBlockRound(ctx context.Context) (round uint64, ok bool, err error)

	// BlockHash returns the stored block hash for a round, if present.
	BlockHash(ctx context.Context, round uint64) (hash string, ok bool, err error)

	Close() error
}

// BatchBlockSink extends BlockSink with atomic multi-block commits.
//
// CommitBatch MUST persist every block in blocks and advance the checkpoint to
// the final round in a single atomic operation. On failure nothing from the
// batch is durable and the checkpoint MUST NOT advance.
//
// blocks must be non-empty, contiguous, and strictly increasing by one.
type BatchBlockSink interface {
	BlockSink
	CommitBatch(ctx context.Context, blocks []block.Block) error
}

// AsBatchSink returns sink if it already supports batching; otherwise wraps it
// with a sequential adapter (not atomic across blocks — only for sinks that
// lack native batching).
func AsBatchSink(sink BlockSink) BatchBlockSink {
	if b, ok := sink.(BatchBlockSink); ok {
		return b
	}
	return sequentialBatchSink{sink: sink}
}

type sequentialBatchSink struct {
	sink BlockSink
}

func (s sequentialBatchSink) Commit(ctx context.Context, blk block.Block) error {
	return s.sink.Commit(ctx, blk)
}
func (s sequentialBatchSink) LastProcessedRound(ctx context.Context) (uint64, bool, error) {
	return s.sink.LastProcessedRound(ctx)
}
func (s sequentialBatchSink) MaxBlockRound(ctx context.Context) (uint64, bool, error) {
	return s.sink.MaxBlockRound(ctx)
}
func (s sequentialBatchSink) BlockHash(ctx context.Context, round uint64) (string, bool, error) {
	return s.sink.BlockHash(ctx, round)
}
func (s sequentialBatchSink) Close() error { return s.sink.Close() }

func (s sequentialBatchSink) CommitBatch(ctx context.Context, blocks []block.Block) error {
	if err := ValidateBatch(blocks); err != nil {
		return err
	}
	for i := range blocks {
		if err := s.sink.Commit(ctx, blocks[i]); err != nil {
			return err
		}
	}
	return nil
}

// ValidateBatch checks that blocks form a non-empty contiguous round range.
func ValidateBatch(blocks []block.Block) error {
	if len(blocks) == 0 {
		return fmt.Errorf("empty batch")
	}
	for i := 1; i < len(blocks); i++ {
		if blocks[i].Round != blocks[i-1].Round+1 {
			return fmt.Errorf("non-contiguous batch: %d then %d", blocks[i-1].Round, blocks[i].Round)
		}
	}
	return nil
}
