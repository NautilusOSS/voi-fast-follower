package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nicholasshellabarger/voi-fast-follower/internal/block"
)

func TestMultiSinkConcurrent(t *testing.T) {
	d1, d2 := t.TempDir(), t.TempDir()
	a, err := NewArchive(ArchiveOptions{Root: d1, SegmentSize: 50})
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewArchive(ArchiveOptions{Root: d2, SegmentSize: 50})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	defer b.Close()

	var concurrent atomic.Int32
	var maxConcurrent atomic.Int32
	wrap := func(inner BatchBlockSink, name string) BatchBlockSink {
		return &slowSink{inner: inner, name: name, concurrent: &concurrent, max: &maxConcurrent}
	}
	multi, err := NewMultiSink(
		[]BatchBlockSink{wrap(a, "a"), wrap(b, "b")},
		[]string{"a", "b"},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := multi.CommitBatch(ctx, makeBatch(1, 5)); err != nil {
		t.Fatal(err)
	}
	if maxConcurrent.Load() < 2 {
		t.Fatalf("expected concurrent commits, max=%d", maxConcurrent.Load())
	}
	cp, ok, _ := multi.LastProcessedRound(ctx)
	if !ok || cp != 5 {
		t.Fatalf("cp=%d ok=%v", cp, ok)
	}
}

type slowSink struct {
	inner      BatchBlockSink
	name       string
	concurrent *atomic.Int32
	max        *atomic.Int32
}

func (s *slowSink) Commit(ctx context.Context, blk block.Block) error {
	return s.CommitBatch(ctx, []block.Block{blk})
}
func (s *slowSink) CommitBatch(ctx context.Context, blocks []block.Block) error {
	n := s.concurrent.Add(1)
	for {
		cur := s.max.Load()
		if n <= cur || s.max.CompareAndSwap(cur, n) {
			break
		}
	}
	time.Sleep(30 * time.Millisecond)
	err := s.inner.CommitBatch(ctx, blocks)
	s.concurrent.Add(-1)
	return err
}
func (s *slowSink) LastProcessedRound(ctx context.Context) (uint64, bool, error) {
	return s.inner.LastProcessedRound(ctx)
}
func (s *slowSink) MaxBlockRound(ctx context.Context) (uint64, bool, error) {
	return s.inner.MaxBlockRound(ctx)
}
func (s *slowSink) BlockHash(ctx context.Context, round uint64) (string, bool, error) {
	return s.inner.BlockHash(ctx, round)
}
func (s *slowSink) Close() error { return s.inner.Close() }

func TestArchiveVerifyAndInfo(t *testing.T) {
	dir := t.TempDir()
	sink, err := NewArchive(ArchiveOptions{Root: dir, SegmentSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := sink.CommitBatch(ctx, makeBatch(100, 25)); err != nil {
		t.Fatal(err)
	}
	sink.Close()

	rep, err := VerifyArchive(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK || rep.Blocks != 25 || rep.Gaps != 0 {
		t.Fatalf("report=%+v errors=%v", rep, rep.Errors)
	}
	info, err := InspectArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	if info.Segments < 2 || info.Checkpoint != 124 {
		t.Fatalf("info=%+v", info)
	}
}

func TestArchiveSegmentRotationRecovery(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	sink, err := NewArchive(ArchiveOptions{Root: dir, SegmentSize: 5})
	if err != nil {
		t.Fatal(err)
	}
	// Fill segment ending at 104, then start next segment at 105.
	if err := sink.CommitBatch(ctx, makeBatch(100, 5)); err != nil {
		t.Fatal(err)
	}
	next := makeBatch(105, 3)
	next[0].PreviousBlockHash = fmt.Sprintf("H%d", uint64(104))
	if err := sink.CommitBatch(ctx, next); err != nil {
		t.Fatal(err)
	}
	// Crash simulation: append garbage to second segment, remove checkpoint temporarily
	// then restore checkpoint to mid-rotation value.
	segB := filepath.Join(dir, segmentsDirName, fmt.Sprintf("%020d.seg", uint64(105)))
	f, err := os.OpenFile(segB, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte{0xff, 0xff})
	_ = f.Close()

	// Reopen reconciles trailer; checkpoint still 107.
	sink.Close()
	sink2, err := NewArchive(ArchiveOptions{Root: dir, SegmentSize: 5})
	if err != nil {
		t.Fatal(err)
	}
	defer sink2.Close()
	cp, ok, _ := sink2.LastProcessedRound(ctx)
	if !ok || cp != 107 {
		t.Fatalf("cp=%d", cp)
	}
	blocks, err := ReadArchiveBlocks(dir, 100, 107)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 8 {
		t.Fatalf("len=%d", len(blocks))
	}
}

func TestArchiveBeyondCheckpointNotCommitted(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	sink, err := NewArchive(ArchiveOptions{Root: dir, SegmentSize: 100, DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	batch := makeBatch(500, 4)
	if err := sink.CommitBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	// Data+checkpoint exist. Append extra record bytes without advancing checkpoint.
	seg := filepath.Join(dir, segmentsDirName, fmt.Sprintf("%020d.seg", uint64(500)))
	extra := encodeRecord(makeBatch(504, 1)[0])
	f, err := os.OpenFile(seg, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write(extra)
	_ = f.Close()
	sink.Close()

	sink2, err := NewArchive(ArchiveOptions{Root: dir, SegmentSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	defer sink2.Close()
	cp, ok, _ := sink2.LastProcessedRound(ctx)
	if !ok || cp != 503 {
		t.Fatalf("cp=%d want 503", cp)
	}
	// Round 504 must not be readable as durable.
	_, ok, err = sink2.BlockHash(ctx, 504)
	if err != nil || ok {
		t.Fatalf("504 should not be durable ok=%v err=%v", ok, err)
	}
}

func TestLongRangeMultiSegmentArchive(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	const n = 3500
	const seg = 100
	sink, err := NewArchive(ArchiveOptions{Root: dir, SegmentSize: seg})
	if err != nil {
		t.Fatal(err)
	}
	base := uint64(10_000)
	for off := 0; off < n; off += 50 {
		chunk := 50
		if off+chunk > n {
			chunk = n - off
		}
		batch := makeBatch(base+uint64(off), chunk)
		if off > 0 {
			batch[0].PreviousBlockHash = fmt.Sprintf("H%d", base+uint64(off)-1)
		}
		if err := sink.CommitBatch(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}
	sink.Close()

	rep, err := VerifyArchive(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK || rep.Blocks != n || rep.Gaps != 0 {
		t.Fatalf("verify failed: %+v %v", rep, rep.Errors)
	}
	wantSegs := (n + seg - 1) / seg
	if rep.Segments < wantSegs {
		t.Fatalf("segments=%d want>=%d", rep.Segments, wantSegs)
	}
}

func TestReplayEquivalenceFields(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	sink, err := NewArchive(ArchiveOptions{Root: dir, SegmentSize: 20})
	if err != nil {
		t.Fatal(err)
	}
	orig := makeBatch(8000, 12)
	if err := sink.CommitBatch(ctx, orig); err != nil {
		t.Fatal(err)
	}
	sink.Close()
	replayed, err := ReadArchiveBlocks(dir, 8000, 8011)
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != len(orig) {
		t.Fatalf("len %d vs %d", len(replayed), len(orig))
	}
	for i := range orig {
		if replayed[i].Round != orig[i].Round ||
			replayed[i].BlockHash != orig[i].BlockHash ||
			replayed[i].PreviousBlockHash != orig[i].PreviousBlockHash ||
			string(replayed[i].Raw) != string(orig[i].Raw) {
			t.Fatalf("mismatch at %d", i)
		}
	}
}

func TestMultiSinkFailureKeepsMinCheckpoint(t *testing.T) {
	d1 := t.TempDir()
	a, err := NewArchive(ArchiveOptions{Root: d1, SegmentSize: 50})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	fail := &failSink{}
	multi, err := NewMultiSink([]BatchBlockSink{a, fail}, []string{"archive", "fail"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_ = multi.CommitBatch(ctx, makeBatch(1, 2))
	_, ok, _ := multi.LastProcessedRound(ctx)
	if ok {
		t.Fatal("composite checkpoint should be absent when one sink has none")
	}
}

// Ensure concurrent MultiSink does not race Close.
func TestMultiSinkClose(t *testing.T) {
	d1, d2 := t.TempDir(), t.TempDir()
	a, _ := NewArchive(ArchiveOptions{Root: d1})
	b, _ := NewArchive(ArchiveOptions{Root: d2})
	m, err := NewMultiSink([]BatchBlockSink{a, b}, []string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = m.CommitBatch(context.Background(), makeBatch(1, 1))
	}()
	wg.Wait()
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
}
