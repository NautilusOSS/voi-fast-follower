package storage

import (
	"context"
	"fmt"

	"github.com/nicholasshellabarger/voi-fast-follower/internal/block"
)

// MultiSink fans a canonical ordered stream out to multiple BatchBlockSinks.
//
// Semantics:
//   - CommitBatch succeeds only if EVERY sink successfully commits the batch.
//   - Failure of any sink fails the overall commit; the follower does not advance.
//   - Sinks must be idempotent so retries after a partial fan-out are safe.
//   - LastProcessedRound returns the MIN checkpoint across sinks (conservative).
//   - MaxBlockRound returns the MIN of sink maxima (data known present everywhere).
//
// Durable cursor invariant:
//
//	checkpoint = highest round durably accepted by all required sinks
type MultiSink struct {
	sinks []BatchBlockSink
	names []string
}

// NewMultiSink wraps sinks. All must be non-nil. names is optional (for errors).
func NewMultiSink(sinks []BatchBlockSink, names []string) (*MultiSink, error) {
	if len(sinks) == 0 {
		return nil, fmt.Errorf("multi-sink: no sinks")
	}
	for i, s := range sinks {
		if s == nil {
			return nil, fmt.Errorf("multi-sink: nil sink at %d", i)
		}
	}
	if names == nil {
		names = make([]string, len(sinks))
		for i := range names {
			names[i] = fmt.Sprintf("sink%d", i)
		}
	}
	if len(names) != len(sinks) {
		return nil, fmt.Errorf("multi-sink: names length mismatch")
	}
	return &MultiSink{sinks: sinks, names: names}, nil
}

func (m *MultiSink) Commit(ctx context.Context, blk block.Block) error {
	return m.CommitBatch(ctx, []block.Block{blk})
}

func (m *MultiSink) CommitBatch(ctx context.Context, blocks []block.Block) error {
	if len(blocks) == 0 {
		return nil
	}
	if err := ValidateBatch(blocks); err != nil {
		return err
	}
	for i, s := range m.sinks {
		if err := s.CommitBatch(ctx, blocks); err != nil {
			return fmt.Errorf("%s: %w", m.names[i], err)
		}
	}
	return nil
}

func (m *MultiSink) LastProcessedRound(ctx context.Context) (uint64, bool, error) {
	var min uint64
	var any bool
	for i, s := range m.sinks {
		r, ok, err := s.LastProcessedRound(ctx)
		if err != nil {
			return 0, false, fmt.Errorf("%s: %w", m.names[i], err)
		}
		if !ok {
			return 0, false, nil
		}
		if !any || r < min {
			min = r
			any = true
		}
	}
	return min, any, nil
}

func (m *MultiSink) MaxBlockRound(ctx context.Context) (uint64, bool, error) {
	var min uint64
	var any bool
	for i, s := range m.sinks {
		r, ok, err := s.MaxBlockRound(ctx)
		if err != nil {
			return 0, false, fmt.Errorf("%s: %w", m.names[i], err)
		}
		if !ok {
			return 0, false, nil
		}
		if !any || r < min {
			min = r
			any = true
		}
	}
	return min, any, nil
}

func (m *MultiSink) BlockHash(ctx context.Context, round uint64) (string, bool, error) {
	// Prefer the first sink that has the hash; verify agreement when multiple do.
	var first string
	var found bool
	for i, s := range m.sinks {
		h, ok, err := s.BlockHash(ctx, round)
		if err != nil {
			return "", false, fmt.Errorf("%s: %w", m.names[i], err)
		}
		if !ok {
			continue
		}
		if !found {
			first = h
			found = true
			continue
		}
		if h != first {
			return "", false, fmt.Errorf("block hash disagreement at round %d: %s vs %s", round, first, h)
		}
	}
	return first, found, nil
}

func (m *MultiSink) Close() error {
	var first error
	for i, s := range m.sinks {
		if err := s.Close(); err != nil && first == nil {
			first = fmt.Errorf("%s: %w", m.names[i], err)
		}
	}
	return first
}

// Sinks returns the underlying sinks (read-only use).
func (m *MultiSink) Sinks() []BatchBlockSink { return m.sinks }
