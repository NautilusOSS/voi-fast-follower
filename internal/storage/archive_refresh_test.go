package storage

import (
	"context"
	"testing"
	"time"
)

func TestArchiveReaderSeesExternalWrites(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewArchive(ArchiveOptions{Root: dir, SegmentSize: 3, DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	chain := fixtureBlocks(1, 3)
	if err := writer.CommitBatch(context.Background(), chain); err != nil {
		t.Fatal(err)
	}
	_ = writer.Close()

	reader, err := NewArchive(ArchiveOptions{Root: dir, SegmentSize: 3, DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	writer2, err := NewArchive(ArchiveOptions{Root: dir, SegmentSize: 3, DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	more := fixtureBlocks(4, 2)
	if err := writer2.CommitBatch(context.Background(), more); err != nil {
		t.Fatal(err)
	}
	_ = writer2.Close()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		blk, ok, err := reader.GetBlock(context.Background(), 4)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			if blk.Round != 4 {
				t.Fatalf("round=%d", blk.Round)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("reader never observed external write")
}
