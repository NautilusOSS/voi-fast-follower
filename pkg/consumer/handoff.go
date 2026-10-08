package consumer

import (
	"context"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
	"github.com/NautilusOSS/voi-fast-follower/internal/stream"
)

// LiveClient fetches blocks from a live algod endpoint (used after archive tip).
type LiveClient interface {
	GetBlock(ctx context.Context, round uint64) (Block, error)
	LastRound(ctx context.Context) (uint64, error)
}

type liveAdapter struct {
	c LiveClient
}

func (l liveAdapter) GetBlock(ctx context.Context, round uint64) (block.Block, error) {
	b, err := l.c.GetBlock(ctx, round)
	if err != nil {
		return block.Block{}, err
	}
	return toInternal(b), nil
}

func (l liveAdapter) LastRound(ctx context.Context) (uint64, error) {
	return l.c.LastRound(ctx)
}

// Handoff serves archive history through the archive checkpoint, then live algod.
type Handoff struct {
	inner *stream.HandoffSource
}

// NewHandoff builds archive→live source with verified boundary linkage.
func NewHandoff(archive *Archive, live LiveClient) *Handoff {
	var lf stream.LiveFetcher
	if live != nil {
		lf = liveAdapter{c: live}
	}
	return &Handoff{inner: stream.NewHandoff(archive.inner, lf)}
}

// Phase describes handoff operational state.
type Phase string

const (
	PhaseHistorical Phase = "historical"
	PhaseHandoff    Phase = "handoff"
	PhaseLive       Phase = "live"
	PhaseWaiting    Phase = "waiting"
	PhaseFailed     Phase = "failed"
)

// Phase returns the current handoff phase.
func (h *Handoff) Phase() Phase {
	return Phase(h.inner.Phase())
}

// OnPhase registers a phase change callback.
func (h *Handoff) OnPhase(fn func(Phase)) {
	h.inner.OnPhase(func(p stream.Phase) { fn(Phase(p)) })
}

// Get implements Source.
func (h *Handoff) Get(ctx context.Context, round uint64) (Block, error) {
	blk, ok, err := h.inner.Get(ctx, round)
	if err != nil {
		return Block{}, roundErr(round, err)
	}
	if !ok {
		return Block{}, roundErr(round, ErrNotYetAvailable)
	}
	return fromInternal(blk), nil
}

// Checkpoint implements Source.
func (h *Handoff) Checkpoint(ctx context.Context) (uint64, bool, error) {
	return h.inner.Checkpoint(ctx)
}

// HandoffRound returns the archive tip used at archive→live boundary, if handoff occurred.
func (h *Handoff) HandoffRound() (uint64, bool) {
	return h.inner.HandoffRound()
}
