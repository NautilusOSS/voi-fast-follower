package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/nicholasshellabarger/voi-fast-follower/internal/block"
)

func TestArchiveSequentialAndIdempotent(t *testing.T) {
	dir := t.TempDir()
	sink, err := NewArchive(ArchiveOptions{Root: dir, SegmentSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()

	ctx := context.Background()
	batch := makeBatch(100, 5)
	if err := sink.CommitBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if err := sink.CommitBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	cp, ok, err := sink.LastProcessedRound(ctx)
	if err != nil || !ok || cp != 104 {
		t.Fatalf("checkpoint=%d ok=%v err=%v", cp, ok, err)
	}
	h, ok, err := sink.BlockHash(ctx, 102)
	if err != nil || !ok || h != batch[2].BlockHash {
		t.Fatalf("hash=%q ok=%v", h, ok)
	}
}

func TestArchiveOrderingAndLinkage(t *testing.T) {
	dir := t.TempDir()
	sink, err := NewArchive(ArchiveOptions{Root: dir, SegmentSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	ctx := context.Background()

	b1 := makeBatch(50, 3)
	if err := sink.CommitBatch(ctx, b1); err != nil {
		t.Fatal(err)
	}
	b2 := makeBatch(53, 4) // crosses segment boundary (seg size 3: 51-53, 54-56)
	// fix linkage from prior batch
	b2[0].PreviousBlockHash = b1[2].BlockHash
	if err := sink.CommitBatch(ctx, b2); err != nil {
		t.Fatal(err)
	}

	blocks, err := ReadArchiveBlocks(dir, 50, 56)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 7 {
		t.Fatalf("len=%d", len(blocks))
	}
	for i := 1; i < len(blocks); i++ {
		if blocks[i].Round != blocks[i-1].Round+1 {
			t.Fatalf("order break at %d", i)
		}
		if err := block.ValidateLinkage(blocks[i-1].BlockHash, blocks[i]); err != nil {
			t.Fatal(err)
		}
	}
}

func TestArchiveGapRejected(t *testing.T) {
	dir := t.TempDir()
	sink, err := NewArchive(ArchiveOptions{Root: dir, SegmentSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	ctx := context.Background()
	if err := sink.CommitBatch(ctx, makeBatch(10, 2)); err != nil {
		t.Fatal(err)
	}
	if err := sink.CommitBatch(ctx, makeBatch(13, 1)); err == nil {
		t.Fatal("expected gap error")
	}
}

func TestArchiveCrashBeforeCheckpoint(t *testing.T) {
	dir := t.TempDir()
	sink, err := NewArchive(ArchiveOptions{Root: dir, SegmentSize: 100, DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	batch := makeBatch(200, 5)
	if err := sink.CommitBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	// Simulate crash after data write but before checkpoint: remove checkpoint.
	_ = os.Remove(filepath.Join(dir, checkpointFileName))
	sink.Close()

	// Reopen must not treat unsynced-without-checkpoint data as durable.
	sink2, err := NewArchive(ArchiveOptions{Root: dir, SegmentSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	defer sink2.Close()
	_, ok, err := sink2.LastProcessedRound(ctx)
	if err != nil || ok {
		t.Fatalf("expected empty checkpoint after crash, ok=%v err=%v", ok, err)
	}
	// Rewrite should succeed from scratch.
	if err := sink2.CommitBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
}

func TestArchiveTruncatedSegment(t *testing.T) {
	dir := t.TempDir()
	sink, err := NewArchive(ArchiveOptions{Root: dir, SegmentSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := sink.CommitBatch(ctx, makeBatch(300, 3)); err != nil {
		t.Fatal(err)
	}
	sink.Close()

	seg := filepath.Join(dir, segmentsDirName, fmt.Sprintf("%020d.seg", uint64(300)))
	// 300 with segmentSize 100 → start 300
	seg = filepath.Join(dir, segmentsDirName, fmt.Sprintf("%020d.seg", uint64(300)))
	info, err := os.Stat(seg)
	if err != nil {
		// segment start for 300 with size 100 is 300
		t.Fatal(err)
	}
	f, err := os.OpenFile(seg, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{1, 2, 3, 4, 5}); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	_ = info

	sink2, err := NewArchive(ArchiveOptions{Root: dir, SegmentSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	defer sink2.Close()
	cp, ok, _ := sink2.LastProcessedRound(ctx)
	if !ok || cp != 302 {
		t.Fatalf("checkpoint=%d", cp)
	}
	blocks, err := ReadArchiveBlocks(dir, 300, 302)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 3 {
		t.Fatalf("len=%d", len(blocks))
	}
}

func TestArchiveRestartAfterSuccess(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	sink, err := NewArchive(ArchiveOptions{Root: dir, SegmentSize: 5})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.CommitBatch(ctx, makeBatch(1000, 5)); err != nil {
		t.Fatal(err)
	}
	sink.Close()

	sink2, err := NewArchive(ArchiveOptions{Root: dir, SegmentSize: 5})
	if err != nil {
		t.Fatal(err)
	}
	defer sink2.Close()
	cp, ok, _ := sink2.LastProcessedRound(ctx)
	if !ok || cp != 1004 {
		t.Fatalf("cp=%d", cp)
	}
	next := makeBatch(1005, 3)
	next[0].PreviousBlockHash = fmt.Sprintf("H%d", uint64(1004))
	if err := sink2.CommitBatch(ctx, next); err != nil {
		t.Fatal(err)
	}
}

func TestMultiSinkRequiresAll(t *testing.T) {
	dir := t.TempDir()
	arch, err := NewArchive(ArchiveOptions{Root: dir, SegmentSize: 50})
	if err != nil {
		t.Fatal(err)
	}
	defer arch.Close()

	fail := &failSink{}
	multi, err := NewMultiSink([]BatchBlockSink{arch, fail}, []string{"archive", "fail"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	err = multi.CommitBatch(ctx, makeBatch(1, 2))
	if err == nil {
		t.Fatal("expected failure")
	}
	// Archive may have advanced (first in list). Multi LastProcessedRound is min.
	// failSink has no checkpoint → multi reports none.
	_, ok, _ := multi.LastProcessedRound(ctx)
	if ok {
		t.Fatal("multi should not report checkpoint when a sink has none")
	}
}

func TestMultiSinkMinCheckpoint(t *testing.T) {
	d1 := t.TempDir()
	d2 := t.TempDir()
	a, err := NewArchive(ArchiveOptions{Root: d1, SegmentSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewArchive(ArchiveOptions{Root: d2, SegmentSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	defer b.Close()
	ctx := context.Background()
	_ = a.CommitBatch(ctx, makeBatch(10, 5))
	_ = b.CommitBatch(ctx, makeBatch(10, 3))
	multi, err := NewMultiSink([]BatchBlockSink{a, b}, []string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	cp, ok, err := multi.LastProcessedRound(ctx)
	if err != nil || !ok || cp != 12 {
		t.Fatalf("min checkpoint=%d ok=%v", cp, ok)
	}
}

type failSink struct{}

func (f *failSink) Commit(ctx context.Context, blk block.Block) error {
	return f.CommitBatch(ctx, []block.Block{blk})
}
func (f *failSink) CommitBatch(context.Context, []block.Block) error {
	return fmt.Errorf("injected sink failure")
}
func (f *failSink) LastProcessedRound(context.Context) (uint64, bool, error) {
	return 0, false, nil
}
func (f *failSink) MaxBlockRound(context.Context) (uint64, bool, error) {
	return 0, false, nil
}
func (f *failSink) BlockHash(context.Context, uint64) (string, bool, error) {
	return "", false, nil
}
func (f *failSink) Close() error { return nil }
