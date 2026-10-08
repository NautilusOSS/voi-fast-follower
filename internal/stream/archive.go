package stream

import (
	"context"
	"fmt"
	"time"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
	"github.com/NautilusOSS/voi-fast-follower/internal/storage"
)

// ArchiveSource reads canonical blocks from a Fast Follower archive.
// It never contacts a Voi node.
type ArchiveSource struct {
	archive *storage.ArchiveSink
}

// OpenArchive opens an archive directory as a RoundSource.
func OpenArchive(root string) (*ArchiveSource, error) {
	a, err := storage.NewArchive(storage.ArchiveOptions{Root: root})
	if err != nil {
		return nil, err
	}
	return &ArchiveSource{archive: a}, nil
}

// Close releases the underlying archive handle.
func (s *ArchiveSource) Close() error {
	if s.archive == nil {
		return nil
	}
	return s.archive.Close()
}

// Get implements RoundSource.
func (s *ArchiveSource) Get(ctx context.Context, round uint64) (block.Block, bool, error) {
	return s.archive.GetBlock(ctx, round)
}

// Checkpoint implements RoundSource.
func (s *ArchiveSource) Checkpoint(ctx context.Context) (uint64, bool, error) {
	return s.archive.LastProcessedRound(ctx)
}

// WaitOptions configures polling for rounds not yet durable.
type WaitOptions struct {
	PollInterval time.Duration
	Timeout      time.Duration // 0 = wait until ctx cancel only
}

// GetWait blocks until round is durable (or timeout/cancel), then returns it.
// Used by live Conduit follow against a growing archive.
func (s *ArchiveSource) GetWait(ctx context.Context, round uint64, opts WaitOptions) (block.Block, error) {
	interval := opts.PollInterval
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	var deadline <-chan time.Time
	if opts.Timeout > 0 {
		t := time.NewTimer(opts.Timeout)
		defer t.Stop()
		deadline = t.C
	}
	for {
		blk, ok, err := s.Get(ctx, round)
		if err != nil {
			return block.Block{}, err
		}
		if ok {
			return blk, nil
		}
		cp, cpOK, err := s.Checkpoint(ctx)
		if err != nil {
			return block.Block{}, err
		}
		// Offline sources may never grow; still poll until timeout when following.
		_ = cp
		_ = cpOK

		select {
		case <-ctx.Done():
			return block.Block{}, ctx.Err()
		case <-deadline:
			return block.Block{}, fmt.Errorf("stream: timeout waiting for archive round %d", round)
		case <-time.After(interval):
		}
	}
}
