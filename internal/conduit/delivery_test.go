package conduit

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
	"github.com/NautilusOSS/voi-fast-follower/internal/storage"
	"github.com/NautilusOSS/voi-fast-follower/internal/stream"
)

type memSource struct {
	blocks map[uint64]block.Block
	cp     uint64
}

func (m *memSource) Get(_ context.Context, round uint64) (block.Block, bool, error) {
	b, ok := m.blocks[round]
	return b, ok, nil
}
func (m *memSource) Checkpoint(context.Context) (uint64, bool, error) {
	if m.cp == 0 {
		return 0, false, nil
	}
	return m.cp, true, nil
}

func TestDeliverRangeOrderingAndFailure(t *testing.T) {
	chain := MakeFixtureChain(50, 5, true)
	src := &memSource{blocks: map[uint64]block.Block{}, cp: 54}
	for _, b := range chain {
		src.blocks[b.Round] = b
	}

	var got []uint64
	failAt := uint64(52)
	a := &Adapter{
		Src: src,
		Deliver: func(_ context.Context, blk block.Block, data BlockData) error {
			if data.Round() != blk.Round {
				t.Fatalf("translate round drift")
			}
			if blk.Round == failAt {
				return errors.New("conduit downstream failed")
			}
			got = append(got, blk.Round)
			return nil
		},
		Metrics: NewMetrics(prometheus.NewRegistry()),
	}

	err := a.DeliverRange(context.Background(), 50, 54)
	if err == nil {
		t.Fatal("expected failure")
	}
	if len(got) != 2 || got[0] != 50 || got[1] != 51 {
		t.Fatalf("partial delivery=%v want [50,51]", got)
	}

	// Retry after "fix": round 52 must be re-offered (at-least-once).
	failAt = 0
	got = nil
	if err := a.DeliverRange(context.Background(), 52, 54); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0] != 52 || got[2] != 54 {
		t.Fatalf("retry got=%v", got)
	}
}

func TestRestartResumesFromCursor(t *testing.T) {
	chain := MakeFixtureChain(70, 4, false)
	src := &memSource{blocks: map[uint64]block.Block{}, cp: 73}
	for _, b := range chain {
		src.blocks[b.Round] = b
	}

	// First process succeeds through 71, then fails on 72 (not durable on consumer).
	delivered := map[uint64]bool{}
	fail72 := true
	a := &Adapter{
		Src: src,
		Deliver: func(_ context.Context, blk block.Block, _ BlockData) error {
			if blk.Round == 72 && fail72 {
				return errors.New("crash before durable accept")
			}
			delivered[blk.Round] = true
			return nil
		},
	}
	if err := a.DeliverRange(context.Background(), 70, 73); err == nil {
		t.Fatal("expected failure at 72")
	}
	if delivered[72] || delivered[73] {
		t.Fatal("72/73 must not be marked delivered after failure")
	}

	// Restart: resume from consumer cursor (72), not from follower archive tip.
	fail72 = false
	resume := uint64(72)
	if err := a.DeliverRange(context.Background(), resume, 73); err != nil {
		t.Fatal(err)
	}
	for r := uint64(70); r <= 73; r++ {
		if !delivered[r] {
			t.Fatalf("missing round %d after restart", r)
		}
	}
}

func TestDuplicateDeliverySafe(t *testing.T) {
	blk := MakeFixtureBlock(9, "", false)
	src := &memSource{blocks: map[uint64]block.Block{9: blk}, cp: 9}
	var n atomic.Int64
	a := &Adapter{
		Src: src,
		Deliver: func(_ context.Context, _ block.Block, _ BlockData) error {
			n.Add(1)
			return nil
		},
	}
	ctx := context.Background()
	if err := a.DeliverRound(ctx, 9); err != nil {
		t.Fatal(err)
	}
	if err := a.DeliverRound(ctx, 9); err != nil {
		t.Fatal(err)
	}
	if n.Load() != 2 {
		t.Fatalf("duplicates not re-delivered: %d", n.Load())
	}
}

func TestArchiveReplayToAdapter(t *testing.T) {
	dir := t.TempDir()
	sink, err := storage.NewArchive(storage.ArchiveOptions{Root: dir, SegmentSize: 10, DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	chain := MakeFixtureChain(200, 8, true)
	if err := sink.CommitBatch(context.Background(), chain); err != nil {
		t.Fatal(err)
	}
	_ = sink.Close()

	src, err := stream.OpenArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	var rounds []uint64
	var hashes []string
	a := &Adapter{
		Src: src,
		Deliver: func(_ context.Context, blk block.Block, data BlockData) error {
			rounds = append(rounds, data.Round())
			hashes = append(hashes, HashHeader(data.BlockHeader))
			if len(data.Payset) != 1 {
				return errors.New("missing txn")
			}
			if blk.BlockHash != HashHeader(data.BlockHeader) {
				return errors.New("hash drift")
			}
			return nil
		},
	}
	if err := a.DeliverRange(context.Background(), 200, 207); err != nil {
		t.Fatal(err)
	}
	if len(rounds) != 8 {
		t.Fatalf("rounds=%d", len(rounds))
	}
	for i := 1; i < len(rounds); i++ {
		if rounds[i] != rounds[i-1]+1 {
			t.Fatalf("order break %v", rounds)
		}
		if chain[i].PreviousBlockHash != hashes[i-1] {
			t.Fatalf("prev-hash linkage broken at %d", rounds[i])
		}
		if hashes[i] != chain[i].BlockHash {
			t.Fatalf("hash not preserved at %d", rounds[i])
		}
	}
	cp, ok, err := src.Checkpoint(context.Background())
	if err != nil || !ok || cp != 207 {
		t.Fatalf("checkpoint=%d ok=%v err=%v", cp, ok, err)
	}
}

func TestCursorOrdering(t *testing.T) {
	chain := MakeFixtureChain(1, 4, false)
	src := &memSource{blocks: map[uint64]block.Block{}, cp: 4}
	for _, b := range chain {
		src.blocks[b.Round] = b
	}
	cur := stream.NewCursor(src, 1)
	for want := uint64(1); want <= 4; want++ {
		blk, ok, err := cur.Next(context.Background())
		if err != nil || !ok {
			t.Fatalf("next %d: ok=%v err=%v", want, ok, err)
		}
		if blk.Round != want {
			t.Fatalf("got %d want %d", blk.Round, want)
		}
	}
	_, ok, err := cur.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected end")
	}
}
