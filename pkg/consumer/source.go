package consumer

import "context"

// Source serves canonical blocks by round.
//
// Get returns ErrNotYetAvailable when the round may appear later (growing archive
// or live follow). Get returns ErrNotFound when the round is permanently unavailable
// (pruned history or never captured). Other errors indicate temporary or permanent
// failures (I/O, corrupt data, linkage break).
type Source interface {
	Get(ctx context.Context, round uint64) (Block, error)
	Checkpoint(ctx context.Context) (round uint64, ok bool, err error)
}
