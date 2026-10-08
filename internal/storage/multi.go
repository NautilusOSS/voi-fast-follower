package storage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
)

// MultiSink fans a canonical ordered stream out to multiple BatchBlockSinks.
//
// Semantics:
//   - CommitBatch runs independent sinks concurrently.
//   - Success requires EVERY sink to successfully commit the batch.
//   - Failure of any sink fails the overall commit; the follower must not
//     treat the batch as durable (composite checkpoint = min across sinks).
//   - Sinks must be idempotent so retries after a partial fan-out are safe
//     (some sinks may have already advanced).
//   - LastProcessedRound / MaxBlockRound return the MIN across sinks.
//
// Durable cursor invariant:
//
//	checkpoint = highest round durably accepted by all required sinks
//
// There is no distributed transaction: we rely on idempotent retries and the
// conservative min checkpoint.
type MultiSink struct {
	sinks []BatchBlockSink
	names []string

	mu       sync.Mutex
	onCommit func(name string, d time.Duration, err error)
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

// OnCommit registers an optional per-sink timing/error callback.
func (m *MultiSink) OnCommit(fn func(name string, d time.Duration, err error)) {
	m.mu.Lock()
	m.onCommit = fn
	m.mu.Unlock()
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

	g, gctx := errgroup.WithContext(ctx)
	for i := range m.sinks {
		i := i
		g.Go(func() error {
			start := time.Now()
			err := m.sinks[i].CommitBatch(gctx, blocks)
			m.mu.Lock()
			cb := m.onCommit
			m.mu.Unlock()
			if cb != nil {
				cb(m.names[i], time.Since(start), err)
			}
			if err != nil {
				return fmt.Errorf("%s: %w", m.names[i], err)
			}
			return nil
		})
	}
	return g.Wait()
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

// Names returns sink names in the same order as Sinks.
func (m *MultiSink) Names() []string { return m.names }
