package stream

import (
	"context"
	"fmt"
	"testing"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
	"github.com/NautilusOSS/voi-fast-follower/internal/storage"
)

func TestArchiveSourceAndCursor(t *testing.T) {
	dir := t.TempDir()
	sink, err := storage.NewArchive(storage.ArchiveOptions{Root: dir, SegmentSize: 5, DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	chain := makeSimpleChain(10, 6)
	if err := sink.CommitBatch(context.Background(), chain); err != nil {
		t.Fatal(err)
	}
	_ = sink.Close()

	src, err := OpenArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	cp, ok, err := src.Checkpoint(context.Background())
	if err != nil || !ok || cp != 15 {
		t.Fatalf("cp=%d ok=%v err=%v", cp, ok, err)
	}

	cur := NewCursor(src, 10)
	for want := uint64(10); want <= 15; want++ {
		blk, ok, err := cur.Next(context.Background())
		if err != nil || !ok {
			t.Fatalf("next %d: %v ok=%v", want, err, ok)
		}
		if blk.Round != want {
			t.Fatalf("got %d want %d", blk.Round, want)
		}
		if want > 10 {
			if err := block.ValidateLinkage(chain[want-10-1].BlockHash, blk); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func makeSimpleChain(base uint64, n int) []block.Block {
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
