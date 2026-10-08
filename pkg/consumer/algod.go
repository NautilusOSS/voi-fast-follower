package consumer

import (
	"context"
	"log/slog"

	"github.com/NautilusOSS/voi-fast-follower/internal/voi"
)

// AlgodLive adapts a Voi algod endpoint for Handoff live segments.
type AlgodLive struct {
	client *voi.Client
}

// OpenAlgod creates a live client (only needed after archive tip or without archive).
func OpenAlgod(url, token string) (*AlgodLive, error) {
	c, err := voi.New(url, token, slog.Default())
	if err != nil {
		return nil, err
	}
	return &AlgodLive{client: c}, nil
}

func (a *AlgodLive) GetBlock(ctx context.Context, round uint64) (Block, error) {
	blk, err := a.client.GetBlock(ctx, round)
	if err != nil {
		return Block{}, err
	}
	return fromInternal(blk), nil
}

func (a *AlgodLive) LastRound(ctx context.Context) (uint64, error) {
	return a.client.LastRound(ctx)
}
