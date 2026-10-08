package follower

import (
	"context"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
)

// BlockSource is the network-facing abstraction used by the follower engine.
// Tests inject fakes; production uses *voi.Client.
type BlockSource interface {
	GetBlock(ctx context.Context, round uint64) (block.Block, error)
	LastRound(ctx context.Context) (uint64, error)
	WaitForBlockAfter(ctx context.Context, round uint64) (uint64, error)
}
