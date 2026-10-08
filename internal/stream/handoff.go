package stream

import (
	"context"
	"fmt"
	"sync"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
)

// HandoffSource serves rounds from a historical RoundSource through its
// checkpoint, then switches to a live fetcher for subsequent rounds.
//
// Prefer history when both can serve the same round. At the boundary
// (archiveTip+1) previous-hash linkage is verified against the archive tip.
type HandoffSource struct {
	History RoundSource
	Live    LiveFetcher

	mu        sync.Mutex
	phase     Phase
	lastHash  string
	lastRound uint64
	haveLast  bool
	handoffAt uint64 // archive tip when handoff first succeeded
	onPhase   func(Phase)
}

// NewHandoff builds a composite archive+live source.
func NewHandoff(history RoundSource, live LiveFetcher) *HandoffSource {
	return &HandoffSource{
		History: history,
		Live:    live,
		phase:   PhaseHistorical,
	}
}

// OnPhase registers a callback invoked when the operational phase changes.
func (h *HandoffSource) OnPhase(fn func(Phase)) { h.onPhase = fn }

// Phase returns the last observed phase.
func (h *HandoffSource) Phase() Phase {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.phase
}

// HandoffRound returns the archive tip used at handoff, if handoff occurred.
func (h *HandoffSource) HandoffRound() (uint64, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.handoffAt == 0 {
		return 0, false
	}
	return h.handoffAt, true
}

func (h *HandoffSource) setPhase(p Phase) {
	if h.phase == p {
		return
	}
	h.phase = p
	if h.onPhase != nil {
		h.onPhase(p)
	}
}

// Checkpoint returns max(history checkpoint, live tip) when available.
func (h *HandoffSource) Checkpoint(ctx context.Context) (uint64, bool, error) {
	var best uint64
	var ok bool
	if h.History != nil {
		cp, cpOK, err := h.History.Checkpoint(ctx)
		if err != nil {
			return 0, false, err
		}
		if cpOK {
			best, ok = cp, true
		}
	}
	if h.Live != nil {
		tip, err := h.Live.LastRound(ctx)
		if err == nil && (!ok || tip > best) {
			best, ok = tip, true
		}
	}
	return best, ok, nil
}

// Get implements RoundSource with archive→live handoff.
func (h *HandoffSource) Get(ctx context.Context, round uint64) (block.Block, bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	var archCP uint64
	var haveArch bool
	if h.History != nil {
		cp, ok, err := h.History.Checkpoint(ctx)
		if err != nil {
			h.setPhase(PhaseFailed)
			return block.Block{}, false, err
		}
		haveArch = ok
		archCP = cp
	}

	// Prefer archive for rounds at or below its durable tip.
	if haveArch && round <= archCP {
		blk, ok, err := h.History.Get(ctx, round)
		if err != nil {
			h.setPhase(PhaseFailed)
			return block.Block{}, false, err
		}
		if !ok {
			h.setPhase(PhaseFailed)
			return block.Block{}, false, fmt.Errorf("stream: historical gap at round %d (archive cp=%d)", round, archCP)
		}
		if err := h.acceptLocked(blk, PhaseHistorical); err != nil {
			h.setPhase(PhaseFailed)
			return block.Block{}, false, err
		}
		return blk, true, nil
	}

	// Waiting for archive to catch a requested historical round that is not live-only.
	if !haveArch {
		h.setPhase(PhaseWaiting)
		return block.Block{}, false, nil
	}

	// Live path: round >= archCP+1
	if h.Live == nil {
		h.setPhase(PhaseWaiting)
		return block.Block{}, false, nil
	}

	phase := PhaseLive
	if round == archCP+1 {
		phase = PhaseHandoff
	}

	blk, err := h.Live.GetBlock(ctx, round)
	if err != nil {
		if isUnavailable(err) {
			h.setPhase(PhaseWaiting)
			return block.Block{}, false, nil
		}
		h.setPhase(PhaseFailed)
		return block.Block{}, false, err
	}

	if phase == PhaseHandoff {
		// Verify linkage against archive tip hash.
		tipBlk, ok, err := h.History.Get(ctx, archCP)
		if err != nil {
			h.setPhase(PhaseFailed)
			return block.Block{}, false, err
		}
		if !ok {
			h.setPhase(PhaseFailed)
			return block.Block{}, false, fmt.Errorf("stream: missing archive tip %d at handoff", archCP)
		}
		if err := block.ValidateLinkage(tipBlk.BlockHash, blk); err != nil {
			h.setPhase(PhaseFailed)
			return block.Block{}, false, fmt.Errorf("stream: handoff linkage broken at %d→%d: %w", archCP, round, err)
		}
		h.handoffAt = archCP
	} else if h.haveLast && h.lastRound+1 == round {
		if err := block.ValidateLinkage(h.lastHash, blk); err != nil {
			h.setPhase(PhaseFailed)
			return block.Block{}, false, err
		}
	}

	if err := h.acceptLocked(blk, phase); err != nil {
		h.setPhase(PhaseFailed)
		return block.Block{}, false, err
	}
	return blk, true, nil
}

func (h *HandoffSource) acceptLocked(blk block.Block, phase Phase) error {
	if blk.BlockHash == "" {
		return fmt.Errorf("stream: empty block hash round %d", blk.Round)
	}
	// Enforce linkage only for contiguous sequential delivery.
	if h.haveLast && blk.Round == h.lastRound+1 {
		if err := block.ValidateLinkage(h.lastHash, blk); err != nil {
			return err
		}
	}
	h.lastHash = blk.BlockHash
	h.lastRound = blk.Round
	h.haveLast = true
	h.setPhase(phase)
	return nil
}

// ResetCursor clears linkage memory (e.g. after consumer restart bookkeeping).
func (h *HandoffSource) ResetCursor() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.haveLast = false
	h.lastHash = ""
	h.lastRound = 0
}
