package storage

import (
	"context"
	"os"
	"testing"
)

func TestArchiveReplayToPostgresEquivalence(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("set DATABASE_URL")
	}
	ctx := context.Background()
	migrations := findMigrations(t)

	dir := t.TempDir()
	arch, err := NewArchive(ArchiveOptions{Root: dir, SegmentSize: 20})
	if err != nil {
		t.Fatal(err)
	}
	defer arch.Close()

	base := uint64(9_600_000_000)
	n := 15
	blocks := makeBatch(base, n)
	if err := arch.CommitBatch(ctx, blocks); err != nil {
		t.Fatal(err)
	}

	// Direct postgres path.
	pgDirect, err := NewPostgres(ctx, dsn, migrations)
	if err != nil {
		t.Fatal(err)
	}
	defer pgDirect.Close()
	cleanup := func(pg *PostgresBlockSink) {
		_, _ = pg.Pool().Exec(ctx, `DELETE FROM transactions WHERE round >= $1 AND round < $2`, base, base+uint64(n))
		_, _ = pg.Pool().Exec(ctx, `DELETE FROM blocks WHERE round >= $1 AND round < $2`, base, base+uint64(n))
	}
	cleanup(pgDirect)
	t.Cleanup(func() { cleanup(pgDirect) })

	if err := pgDirect.CommitBatch(ctx, blocks); err != nil {
		t.Fatal(err)
	}

	// Replay archive → separate logical write (same DB after cleanup of replay range offset).
	// Compare by re-reading archive and committing to a second pass after delete.
	cleanup(pgDirect)
	replayed, err := ReadArchiveBlocks(dir, base, base+uint64(n)-1)
	if err != nil {
		t.Fatal(err)
	}
	if err := pgDirect.CommitBatch(ctx, replayed); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < n; i++ {
		r := base + uint64(i)
		h1, ok1, err := pgDirect.BlockHash(ctx, r)
		if err != nil || !ok1 {
			t.Fatalf("pg hash round %d: ok=%v err=%v", r, ok1, err)
		}
		if h1 != blocks[i].BlockHash || h1 != replayed[i].BlockHash {
			t.Fatalf("hash mismatch round %d", r)
		}
		var raw []byte
		if err := pgDirect.Pool().QueryRow(ctx, `SELECT raw_block FROM blocks WHERE round=$1`, r).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		if string(raw) != string(blocks[i].Raw) || string(raw) != string(replayed[i].Raw) {
			t.Fatalf("raw mismatch round %d", r)
		}
	}
	cp, ok, err := pgDirect.LastProcessedRound(ctx)
	if err != nil || !ok || cp < base+uint64(n)-1 {
		t.Fatalf("checkpoint=%d ok=%v", cp, ok)
	}
}

func TestDuplicateReplaySafe(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	a, err := NewArchive(ArchiveOptions{Root: dir, SegmentSize: 50})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	batch := makeBatch(700, 8)
	if err := a.CommitBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	blocks, err := ReadArchiveBlocks(dir, 700, 707)
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	b, err := NewArchive(ArchiveOptions{Root: out, SegmentSize: 50})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.CommitBatch(ctx, blocks); err != nil {
		t.Fatal(err)
	}
	if err := b.CommitBatch(ctx, blocks); err != nil {
		t.Fatal(err)
	}
	cp, ok, _ := b.LastProcessedRound(ctx)
	if !ok || cp != 707 {
		t.Fatalf("cp=%d", cp)
	}
}
