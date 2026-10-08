package consumer_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
	"github.com/NautilusOSS/voi-fast-follower/internal/storage"
	"github.com/NautilusOSS/voi-fast-follower/pkg/consumer"
)

func makeChain(base uint64, n int) []block.Block {
	out := make([]block.Block, n)
	var prev string
	for i := 0; i < n; i++ {
		r := base + uint64(i)
		h := fmt.Sprintf("H%d", r)
		out[i] = block.Block{
			Round:             r,
			BlockHash:         h,
			PreviousBlockHash: prev,
			Raw:               []byte(fmt.Sprintf("raw-%d", r)),
		}
		prev = h
	}
	return out
}

func writeArchive(t *testing.T, chain []block.Block) string {
	t.Helper()
	dir := t.TempDir()
	sink, err := storage.NewArchive(storage.ArchiveOptions{Root: dir, SegmentSize: 5, DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.CommitBatch(context.Background(), chain); err != nil {
		t.Fatal(err)
	}
	_ = sink.Close()
	return dir
}

func TestConsumerIndependentCheckpoint(t *testing.T) {
	chain := makeChain(100, 20) // 100-119
	dir := writeArchive(t, chain)

	arch, err := consumer.OpenArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer arch.Close()

	cp, ok, err := arch.Checkpoint(context.Background())
	if err != nil || !ok || cp != 119 {
		t.Fatalf("follower archive cp=%d ok=%v err=%v", cp, ok, err)
	}

	// Consumer lags follower: checkpoint at 109, should resume 110.
	cpFile := consumer.CheckpointFile{Path: filepath.Join(t.TempDir(), "consumer.cp")}
	if err := cpFile.Save(109); err != nil {
		t.Fatal(err)
	}
	start, err := cpFile.ResumeRound(100)
	if err != nil || start != 110 {
		t.Fatalf("resume=%d err=%v", start, err)
	}

	var rounds []uint64
	_, err = consumer.Process(context.Background(), consumer.ProcessOptions{
		Source:     arch,
		Start:      start,
		End:        115,
		Checkpoint: cpFile,
		OnBlock: func(b consumer.Block) error {
			rounds = append(rounds, b.Round)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rounds) != 6 || rounds[0] != 110 || rounds[len(rounds)-1] != 115 {
		t.Fatalf("rounds=%v", rounds)
	}
}

func TestConsumerRestartAtLeastOnce(t *testing.T) {
	chain := makeChain(200, 10)
	dir := writeArchive(t, chain)
	cpPath := filepath.Join(t.TempDir(), "consumer.cp")

	run := func() []uint64 {
		arch, err := consumer.OpenArchive(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer arch.Close()
		var rounds []uint64
		_, err = consumer.Process(context.Background(), consumer.ProcessOptions{
			Source:     arch,
			Start:      200,
			End:        205,
			Checkpoint: consumer.CheckpointFile{Path: cpPath},
			OnBlock: func(b consumer.Block) error {
				rounds = append(rounds, b.Round)
				return nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return rounds
	}

	first := run()
	second := run()

	if len(first) != 6 {
		t.Fatalf("first=%v", first)
	}
	// Second run resumes at 206 — no work unless we rewind checkpoint.
	if len(second) != 0 {
		t.Fatalf("expected no new blocks, got %v", second)
	}

	// Simulate crash before checkpoint persisted on round 203: rewind cp to 202.
	cpRewind := consumer.CheckpointFile{Path: cpPath}
	if err := cpRewind.Save(202); err != nil {
		t.Fatal(err)
	}
	third := run()
	if len(third) != 3 || third[0] != 203 {
		t.Fatalf("replay after crash=%v", third)
	}
}

func TestTwoIndependentConsumers(t *testing.T) {
	chain := makeChain(300, 15)
	dir := writeArchive(t, chain)

	archA, err := consumer.OpenArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer archA.Close()
	archB, err := consumer.OpenArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer archB.Close()

	cpA := consumer.CheckpointFile{Path: filepath.Join(t.TempDir(), "a.cp")}
	cpB := consumer.CheckpointFile{Path: filepath.Join(t.TempDir(), "b.cp")}

	_, err = consumer.Process(context.Background(), consumer.ProcessOptions{
		Source: archA, Start: 300, End: 310, Checkpoint: cpA,
		OnBlock: func(b consumer.Block) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = consumer.Process(context.Background(), consumer.ProcessOptions{
		Source: archB, Start: 300, End: 305, Checkpoint: cpB,
		OnBlock: func(b consumer.Block) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}

	rA, okA, _ := cpA.Load()
	rB, okB, _ := cpB.Load()
	if !okA || rA != 310 || !okB || rB != 305 {
		t.Fatalf("cpA=%d cpB=%d", rA, rB)
	}
}

func TestHistoricalWithoutVoi(t *testing.T) {
	chain := makeChain(400, 8)
	dir := writeArchive(t, chain)

	var count int
	err := consumer.ArchiveRange(context.Background(), dir, 402, 407, func(b consumer.Block) error {
		count++
		if b.Round < 402 || b.Round > 407 {
			t.Fatalf("round=%d", b.Round)
		}
		return nil
	})
	if err != nil || count != 6 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}

type memLive struct {
	blocks map[uint64]block.Block
	tip    uint64
}

func (m *memLive) GetBlock(_ context.Context, round uint64) (consumer.Block, error) {
	b, ok := m.blocks[round]
	if !ok {
		return consumer.Block{}, fmt.Errorf("round not available")
	}
	return consumer.Block{
		Round: b.Round, BlockHash: b.BlockHash, PreviousBlockHash: b.PreviousBlockHash, Raw: b.Raw,
	}, nil
}

func (m *memLive) LastRound(context.Context) (uint64, error) { return m.tip, nil }

func TestHistoricalToLiveHandoff(t *testing.T) {
	chain := makeChain(500, 8) // 500-507
	dir := writeArchive(t, chain[:5]) // archive through 504

	liveBlocks := map[uint64]block.Block{}
	for _, b := range chain[5:] {
		liveBlocks[b.Round] = b
	}
	live := &memLive{blocks: liveBlocks, tip: 507}

	arch, err := consumer.OpenArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer arch.Close()

	h := consumer.NewHandoff(arch, live)
	var phases []consumer.Phase
	h.OnPhase(func(p consumer.Phase) { phases = append(phases, p) })

	var rounds []uint64
	_, err = consumer.Process(context.Background(), consumer.ProcessOptions{
		Source:  h,
		Start:   500,
		End:     507,
		OnBlock: func(b consumer.Block) error { rounds = append(rounds, b.Round); return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rounds) != 8 || rounds[0] != 500 || rounds[7] != 507 {
		t.Fatalf("rounds=%v", rounds)
	}
	hr, ok := h.HandoffRound()
	if !ok || hr != 504 {
		t.Fatalf("handoff=%d ok=%v", hr, ok)
	}
	if h.Phase() != consumer.PhaseLive {
		t.Fatalf("phase=%s", h.Phase())
	}
}

func TestPrunedRangeReturnsNotFound(t *testing.T) {
	chain := makeChain(600, 20)
	dir := writeArchive(t, chain)

	if err := storage.PruneArchive(context.Background(), dir, 610); err != nil {
		t.Fatal(err)
	}

	arch, err := consumer.OpenArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer arch.Close()

	_, err = arch.Get(context.Background(), 605)
	if err == nil {
		t.Fatal("expected error for pruned round")
	}
	var re *consumer.RoundError
	if !errors.As(err, &re) || !errors.Is(re.Err, consumer.ErrNotFound) {
		t.Fatalf("err=%v", err)
	}
}

func TestGrowingArchiveFollow(t *testing.T) {
	dir := t.TempDir()
	sink, err := storage.NewArchive(storage.ArchiveOptions{Root: dir, SegmentSize: 3, DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	chain := makeChain(700, 3)
	if err := sink.CommitBatch(context.Background(), chain); err != nil {
		t.Fatal(err)
	}

	arch, err := consumer.OpenArchive(dir)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var count atomic.Uint64
	done := make(chan error, 1)
	go func() {
		_, err := consumer.Process(ctx, consumer.ProcessOptions{
			Source:   arch,
			Start:    700,
			Archive:  arch,
			WaitPoll: 20 * time.Millisecond,
			OnBlock: func(b consumer.Block) error {
				count.Add(1)
				return nil
			},
		})
		done <- err
	}()

	time.Sleep(80 * time.Millisecond)
	more := makeChain(703, 2)
	if err := sink.CommitBatch(context.Background(), more); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && count.Load() < 5 {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if n := count.Load(); n < 5 {
		t.Fatalf("processed=%d want >=5", n)
	}
	_ = sink.Close()
	_ = arch.Close()
}

func TestExampleBuilds(t *testing.T) {
	if os.Getenv("SKIP_EXAMPLE_BUILD") != "" {
		t.Skip()
	}
}
