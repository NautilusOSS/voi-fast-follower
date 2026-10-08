package storage

import (
	"context"
	"encoding/base32"
	"testing"

	"github.com/algorand/go-algorand-sdk/v2/client/v2/common/models"
	"github.com/algorand/go-algorand-sdk/v2/encoding/msgpack"
	sdk "github.com/algorand/go-algorand-sdk/v2/types"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
)

func fixtureBlocks(base uint64, n int) []block.Block {
	out := make([]block.Block, n)
	var prev string
	for i := 0; i < n; i++ {
		var branch sdk.BlockHash
		if prev != "" {
			raw, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(prev)
			if err == nil && len(raw) == len(branch) {
				copy(branch[:], raw)
			}
		}
		r := base + uint64(i)
		resp := models.BlockResponse{
			Block: sdk.Block{BlockHeader: sdk.BlockHeader{Round: sdk.Round(r), Branch: branch, TimeStamp: int64(r)}},
		}
		raw := append([]byte(nil), msgpack.Encode(resp)...)
		blk, err := block.DecodeRaw(raw)
		if err != nil {
			panic(err)
		}
		out[i] = blk
		prev = blk.BlockHash
	}
	return out
}

func TestIterateAndIndexLookup(t *testing.T) {
	dir := t.TempDir()
	sink, err := NewArchive(ArchiveOptions{Root: dir, SegmentSize: 7, DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	chain := fixtureBlocks(500, 25)
	if err := sink.CommitBatch(context.Background(), chain); err != nil {
		t.Fatal(err)
	}
	_ = sink.Close()

	// Reopen with different SegmentSize — index must still find rounds.
	src, err := NewArchive(ArchiveOptions{Root: dir, SegmentSize: 1000})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	blk, ok, err := src.GetBlock(context.Background(), 512)
	if err != nil || !ok || blk.Round != 512 {
		t.Fatalf("lookup: ok=%v err=%v round=%d", ok, err, blk.Round)
	}

	var n int
	err = IterateArchiveBlocks(context.Background(), dir, 500, 524, func(b block.Block) error {
		n++
		if b.Round != 500+uint64(n-1) {
			t.Fatalf("order %d", b.Round)
		}
		return nil
	})
	if err != nil || n != 25 {
		t.Fatalf("iterate n=%d err=%v", n, err)
	}
}

func TestExportImportCompare(t *testing.T) {
	ctx := context.Background()
	srcDir := t.TempDir()
	sink, err := NewArchive(ArchiveOptions{Root: srcDir, SegmentSize: 5, DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	chain := fixtureBlocks(100, 12)
	if err := sink.CommitBatch(ctx, chain); err != nil {
		t.Fatal(err)
	}
	_ = sink.Close()

	out := t.TempDir() + "/bundle"
	if err := ExportArchive(ctx, srcDir, out, 100, 111); err != nil {
		t.Fatal(err)
	}
	rep, err := CompareArchives(ctx, srcDir, out, 100, 111)
	if err != nil || !rep.OK {
		t.Fatalf("compare: %+v err=%v", rep, err)
	}

	dest := t.TempDir() + "/imported"
	if err := ImportArchive(ctx, out, dest); err != nil {
		t.Fatal(err)
	}
	rep, err = CompareArchives(ctx, srcDir, dest, 100, 111)
	if err != nil || !rep.OK {
		t.Fatalf("after import: %+v err=%v", rep, err)
	}

	// Refuse overwrite
	if err := ExportArchive(ctx, srcDir, out, 100, 105); err == nil {
		t.Fatal("expected refuse non-empty dest")
	}
}

func TestCompareDetectsMismatch(t *testing.T) {
	ctx := context.Background()
	a := t.TempDir()
	c := t.TempDir()
	s1, err := NewArchive(ArchiveOptions{Root: a, DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.CommitBatch(ctx, fixtureBlocks(1, 3)); err != nil {
		t.Fatal(err)
	}
	_ = s1.Close()
	s2, err := NewArchive(ArchiveOptions{Root: c, DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.CommitBatch(ctx, fixtureBlocks(10, 3)); err != nil {
		t.Fatal(err)
	}
	_ = s2.Close()
	rep, err := CompareArchives(ctx, a, c, 1, 3)
	if err == nil && (rep == nil || rep.OK) {
		t.Fatal("expected mismatch between disjoint archives")
	}
}

func TestPruneRolling(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := NewArchive(ArchiveOptions{Root: dir, SegmentSize: 4, DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	chain := fixtureBlocks(1000, 20)
	if err := s.CommitBatch(ctx, chain); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()

	if err := PruneArchive(ctx, dir, 1010); err != nil {
		t.Fatal(err)
	}
	info, err := InspectArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	if info.FirstRound != 1010 || info.Checkpoint != 1019 {
		t.Fatalf("first=%d cp=%d", info.FirstRound, info.Checkpoint)
	}
	reopen, err := NewArchive(ArchiveOptions{Root: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer reopen.Close()
	_, ok, err := reopen.GetBlock(ctx, 1005)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("pruned round still present")
	}
}

func TestLongRangeIterate(t *testing.T) {
	if testing.Short() {
		t.Skip("long-range")
	}
	ctx := context.Background()
	dir := t.TempDir()
	s, err := NewArchive(ArchiveOptions{Root: dir, SegmentSize: 500, DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	const n = 5000
	chain := fixtureBlocks(1_000_000, n)
	// commit in chunks
	for i := 0; i < n; i += 200 {
		end := i + 200
		if end > n {
			end = n
		}
		if err := s.CommitBatch(ctx, chain[i:end]); err != nil {
			t.Fatal(err)
		}
	}
	_ = s.Close()

	var count int
	err = IterateArchiveBlocks(ctx, dir, 1_000_000, 1_000_000+n-1, func(b block.Block) error {
		count++
		return nil
	})
	if err != nil || count != n {
		t.Fatalf("count=%d err=%v", count, err)
	}
}
