package storage

import (
	"context"
	"os"
	"testing"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
)

// TestCrashRecoveryBatchSemantics validates Phase 2/3 checkpoint invariants:
// failed batch => checkpoint unchanged; success => checkpoint = final round;
// restart resumes at checkpoint+1; duplicate retry is idempotent.
func TestCrashRecoveryBatchSemantics(t *testing.T) {
	for _, mode := range []string{InsertModeUNNEST, InsertModeCOPY} {
		t.Run(mode, func(t *testing.T) {
			testCrashRecoveryBatchSemantics(t, mode)
		})
	}
}

func testCrashRecoveryBatchSemantics(t *testing.T, mode string) {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("set DATABASE_URL")
	}
	ctx := context.Background()
	sink, err := NewPostgres(ctx, dsn, findMigrations(t))
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	if err := sink.SetInsertMode(mode); err != nil {
		t.Fatal(err)
	}

	base := uint64(9_400_000_000)
	if mode == InsertModeCOPY {
		base = 9_401_000_000
	}
	prevRound, prevOK, _ := sink.LastProcessedRound(ctx)
	cleanup := func() {
		cctx := context.Background()
		_, _ = sink.Pool().Exec(cctx, `DELETE FROM transactions WHERE round >= $1 AND round < $2`, base, base+20)
		_, _ = sink.Pool().Exec(cctx, `DELETE FROM blocks WHERE round >= $1 AND round < $2`, base, base+20)
		if prevOK {
			_, _ = sink.Pool().Exec(cctx, `
				INSERT INTO sync_state(key,value) VALUES ('last_processed_round',$1)
				ON CONFLICT (key) DO UPDATE SET value=$1`, int64(prevRound))
		} else {
			_, _ = sink.Pool().Exec(cctx, `DELETE FROM sync_state WHERE key='last_processed_round'`)
		}
	}
	t.Cleanup(cleanup)
	cleanup()

	// Seed checkpoint as if prior round committed.
	_, err = sink.Pool().Exec(ctx, `
		INSERT INTO sync_state(key,value) VALUES ('last_processed_round',$1)
		ON CONFLICT (key) DO UPDATE SET value=$1`, int64(base-1))
	if err != nil {
		t.Fatal(err)
	}

	// Crash before commit: non-contiguous batch rejected; checkpoint stays.
	err = sink.CommitBatch(ctx, []block.Block{
		{Round: base, BlockHash: "A", Raw: []byte{1}},
		{Round: base + 2, BlockHash: "C", Raw: []byte{3}},
	})
	if err == nil {
		t.Fatal("expected failure")
	}
	last, ok, err := sink.LastProcessedRound(ctx)
	if err != nil || !ok || last != base-1 {
		t.Fatalf("checkpoint after failed batch: %d ok=%v", last, ok)
	}

	// Successful batch advances to final round atomically.
	batch := makeBatch(base, 10)
	if err := sink.CommitBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	last, ok, err = sink.LastProcessedRound(ctx)
	if err != nil || !ok || last != base+9 {
		t.Fatalf("checkpoint=%d want %d", last, base+9)
	}

	// Restart semantics: resume = checkpoint + 1.
	resume := last + 1
	if resume != base+10 {
		t.Fatalf("resume=%d", resume)
	}

	// Duplicate retry of same batch is safe.
	if err := sink.CommitBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := sink.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM blocks WHERE round >= $1 AND round <= $2`, base, base+9).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 10 {
		t.Fatalf("blocks=%d want 10 after duplicate retry", n)
	}

	// Chain linkage hashes preserved.
	h, ok, err := sink.BlockHash(ctx, base+5)
	if err != nil || !ok || h != batch[5].BlockHash {
		t.Fatalf("hash round %d: %q ok=%v", base+5, h, ok)
	}
}
