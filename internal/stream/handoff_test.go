package stream

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
	"github.com/NautilusOSS/voi-fast-follower/internal/storage"
)

type memHist struct {
	blocks map[uint64]block.Block
	cp     uint64
}

func (m *memHist) Get(_ context.Context, round uint64) (block.Block, bool, error) {
	b, ok := m.blocks[round]
	return b, ok, nil
}
func (m *memHist) Checkpoint(context.Context) (uint64, bool, error) {
	if m.cp == 0 {
		return 0, false, nil
	}
	return m.cp, true, nil
}

type memLive struct {
	blocks map[uint64]block.Block
	tip    uint64
	fail   map[uint64]error
}

func (m *memLive) GetBlock(_ context.Context, round uint64) (block.Block, error) {
	if err, ok := m.fail[round]; ok {
		return block.Block{}, err
	}
	b, ok := m.blocks[round]
	if !ok {
		return block.Block{}, fmt.Errorf("round not available")
	}
	return b, nil
}
func (m *memLive) LastRound(context.Context) (uint64, error) { return m.tip, nil }

func TestHandoffBoundaryLinkage(t *testing.T) {
	chain := fixtureChain(100, 6) // 100-105
	hist := &memHist{blocks: map[uint64]block.Block{}, cp: 103}
	live := &memLive{blocks: map[uint64]block.Block{}, tip: 105}
	for _, b := range chain[:4] {
		hist.blocks[b.Round] = b
	}
	for _, b := range chain[4:] {
		live.blocks[b.Round] = b
	}

	h := NewHandoff(hist, live)
	var phases []Phase
	h.OnPhase(func(p Phase) { phases = append(phases, p) })

	ctx := context.Background()
	for r := uint64(100); r <= 105; r++ {
		blk, ok, err := h.Get(ctx, r)
		if err != nil || !ok {
			t.Fatalf("round %d: ok=%v err=%v", r, ok, err)
		}
		if blk.Round != r {
			t.Fatalf("got %d want %d", blk.Round, r)
		}
	}
	if hr, ok := h.HandoffRound(); !ok || hr != 103 {
		t.Fatalf("handoff=%d ok=%v", hr, ok)
	}
	if h.Phase() != PhaseLive {
		t.Fatalf("phase=%s", h.Phase())
	}
}

func TestHandoffBrokenLinkage(t *testing.T) {
	chain := fixtureChain(10, 4)
	hist := &memHist{blocks: map[uint64]block.Block{10: chain[0], 11: chain[1]}, cp: 11}
	bad := chain[2]
	bad.PreviousBlockHash = "WRONG"
	// Also need Raw to decode with wrong prev - ValidateLinkage uses Block fields from DecodeRaw of live GetBlock.
	// Live returns decoded block; handoff checks tip hash vs blk.PreviousBlockHash from live block.
	liveBlk := chain[2]
	// Corrupt by rebuilding with wrong branch
	live := &memLive{blocks: map[uint64]block.Block{
		12: {
			Round:             12,
			BlockHash:         liveBlk.BlockHash,
			PreviousBlockHash: "NOTPREV",
			Raw:               liveBlk.Raw,
		},
	}, tip: 12}
	h := NewHandoff(hist, live)
	_, ok, err := h.Get(context.Background(), 12)
	if err == nil || ok {
		t.Fatal("expected handoff linkage error")
	}
	if h.Phase() != PhaseFailed {
		t.Fatalf("phase=%s", h.Phase())
	}
}

func TestHandoffPreferArchiveOnOverlap(t *testing.T) {
	chain := fixtureChain(50, 3)
	hist := &memHist{blocks: map[uint64]block.Block{50: chain[0], 51: chain[1]}, cp: 51}
	// Live also has 51 with different raw — archive must win.
	live := &memLive{blocks: map[uint64]block.Block{
		51: {Round: 51, BlockHash: "LIVE", PreviousBlockHash: "X", Raw: []byte("nope")},
		52: chain[2],
	}, tip: 52}
	h := NewHandoff(hist, live)
	blk, ok, err := h.Get(context.Background(), 51)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if blk.BlockHash != chain[1].BlockHash {
		t.Fatalf("expected archive hash, got %s", blk.BlockHash)
	}
}

func TestHandoffMissingLiveWaits(t *testing.T) {
	chain := fixtureChain(1, 2)
	hist := &memHist{blocks: map[uint64]block.Block{1: chain[0]}, cp: 1}
	live := &memLive{blocks: map[uint64]block.Block{}, tip: 1}
	h := NewHandoff(hist, live)
	_, ok, err := h.Get(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected unavailable")
	}
	if h.Phase() != PhaseWaiting {
		t.Fatalf("phase=%s", h.Phase())
	}
}

func TestHandoffRestartAcrossBoundary(t *testing.T) {
	chain := fixtureChain(200, 5)
	hist := &memHist{blocks: map[uint64]block.Block{}, cp: 202}
	live := &memLive{blocks: map[uint64]block.Block{}, tip: 204}
	for _, b := range chain[:3] {
		hist.blocks[b.Round] = b
	}
	for _, b := range chain[3:] {
		live.blocks[b.Round] = b
	}
	h := NewHandoff(hist, live)
	ctx := context.Background()
	// First run through handoff, "crash" before persisting 203.
	for r := uint64(200); r <= 202; r++ {
		if _, ok, err := h.Get(ctx, r); err != nil || !ok {
			t.Fatal(err)
		}
	}
	_, ok, err := h.Get(ctx, 203)
	if err != nil || !ok {
		t.Fatal(err, ok)
	}
	// Restart consumer cursor at 203.
	h2 := NewHandoff(hist, live)
	blk, ok, err := h2.Get(ctx, 203)
	if err != nil || !ok || blk.Round != 203 {
		t.Fatalf("resume handoff: ok=%v err=%v", ok, err)
	}
	if _, ok, err := h2.Get(ctx, 204); err != nil || !ok {
		t.Fatal(err)
	}
}

func TestHandoffDuplicateDelivery(t *testing.T) {
	chain := fixtureChain(5, 3)
	hist := &memHist{blocks: map[uint64]block.Block{5: chain[0], 6: chain[1]}, cp: 6}
	live := &memLive{blocks: map[uint64]block.Block{7: chain[2]}, tip: 7}
	h := NewHandoff(hist, live)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		blk, ok, err := h.Get(ctx, 6)
		if err != nil || !ok || blk.Round != 6 {
			t.Fatalf("dup %d: %v", i, err)
		}
	}
}

func TestArchiveLiveHandoffIntegration(t *testing.T) {
	dir := t.TempDir()
	sink, err := storage.NewArchive(storage.ArchiveOptions{Root: dir, SegmentSize: 3, DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	chain := fixtureChain(1000, 8)
	if err := sink.CommitBatch(context.Background(), chain[:5]); err != nil {
		t.Fatal(err)
	}
	_ = sink.Close()

	hist, err := OpenArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer hist.Close()

	live := &memLive{blocks: map[uint64]block.Block{}, tip: 1007}
	for _, b := range chain[5:] {
		live.blocks[b.Round] = b
	}
	h := NewHandoff(hist, live)
	ctx := context.Background()
	var prev string
	for r := uint64(1000); r <= 1007; r++ {
		blk, ok, err := h.Get(ctx, r)
		if err != nil || !ok {
			t.Fatalf("%d: %v", r, err)
		}
		if prev != "" {
			if err := block.ValidateLinkage(prev, blk); err != nil {
				t.Fatal(err)
			}
		}
		prev = blk.BlockHash
	}
}

func TestHistoricalGapError(t *testing.T) {
	hist := &memHist{blocks: map[uint64]block.Block{}, cp: 10}
	h := NewHandoff(hist, nil)
	_, ok, err := h.Get(context.Background(), 5)
	if err == nil || ok {
		t.Fatal("expected gap error")
	}
}

func TestLiveUnavailableHardError(t *testing.T) {
	chain := fixtureChain(1, 1)
	hist := &memHist{blocks: map[uint64]block.Block{1: chain[0]}, cp: 1}
	live := &memLive{
		blocks: map[uint64]block.Block{},
		tip:    2,
		fail:   map[uint64]error{2: errors.New("connection reset")},
	}
	h := NewHandoff(hist, live)
	_, ok, err := h.Get(context.Background(), 2)
	if err == nil || ok {
		t.Fatal("expected hard error")
	}
}
