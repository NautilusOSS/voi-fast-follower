package stream

import (
	"context"
	"strings"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
)

// LiveFetcher obtains blocks from a live chain tip source (typically algod).
// It is intentionally small so stream does not depend on the voi package.
type LiveFetcher interface {
	GetBlock(ctx context.Context, round uint64) (block.Block, error)
	LastRound(ctx context.Context) (uint64, error)
}

// LiveSource adapts LiveFetcher to RoundSource.
// Checkpoint returns the network tip (best-effort); ok=false if tip unknown.
type LiveSource struct {
	Live LiveFetcher
}

// Get implements RoundSource. ok=false when the live node cannot serve the round
// (lookback miss / not yet produced) without treating it as a hard error.
func (s *LiveSource) Get(ctx context.Context, round uint64) (block.Block, bool, error) {
	if s.Live == nil {
		return block.Block{}, false, nil
	}
	blk, err := s.Live.GetBlock(ctx, round)
	if err != nil {
		if isUnavailable(err) {
			return block.Block{}, false, nil
		}
		return block.Block{}, false, err
	}
	return blk, true, nil
}

// Checkpoint implements RoundSource as the live tip.
func (s *LiveSource) Checkpoint(ctx context.Context) (uint64, bool, error) {
	if s.Live == nil {
		return 0, false, nil
	}
	tip, err := s.Live.LastRound(ctx)
	if err != nil {
		return 0, false, err
	}
	return tip, true, nil
}

func isUnavailable(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, needle := range []string{
		"failed to retrieve information from the ledger",
		"round not available",
		"not found",
		"lookup failed",
		"errnoentry",
		"404",
	} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}
