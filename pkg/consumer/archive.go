package consumer

import (
	"context"

	"github.com/NautilusOSS/voi-fast-follower/internal/stream"
)

// Archive reads blocks from a Fast Follower archive directory.
// It never contacts Voi for historical rounds.
type Archive struct {
	inner *stream.ArchiveSource
}

// OpenArchive opens a durable archive as a Source.
func OpenArchive(root string) (*Archive, error) {
	s, err := stream.OpenArchive(root)
	if err != nil {
		return nil, err
	}
	return &Archive{inner: s}, nil
}

// Close releases archive handles.
func (a *Archive) Close() error {
	if a.inner == nil {
		return nil
	}
	return a.inner.Close()
}

// Get implements Source.
func (a *Archive) Get(ctx context.Context, round uint64) (Block, error) {
	blk, ok, err := a.inner.Get(ctx, round)
	if err != nil {
		return Block{}, roundErr(round, err)
	}
	if ok {
		return fromInternal(blk), nil
	}
	cp, cpOK, err := a.inner.Checkpoint(ctx)
	if err != nil {
		return Block{}, roundErr(round, err)
	}
	if cpOK && round > cp {
		return Block{}, roundErr(round, ErrNotYetAvailable)
	}
	return Block{}, roundErr(round, ErrNotFound)
}

// Checkpoint implements Source.
func (a *Archive) Checkpoint(ctx context.Context) (uint64, bool, error) {
	return a.inner.Checkpoint(ctx)
}

// WaitOptions configures polling for rounds not yet in the archive.
type WaitOptions = stream.WaitOptions

// GetWait blocks until round is durable in the archive (for live follow without algod).
func (a *Archive) GetWait(ctx context.Context, round uint64, opts WaitOptions) (Block, error) {
	blk, err := a.inner.GetWait(ctx, round, opts)
	if err != nil {
		return Block{}, roundErr(round, err)
	}
	return fromInternal(blk), nil
}
