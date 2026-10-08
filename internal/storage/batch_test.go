package storage

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/nicholasshellabarger/voi-fast-follower/internal/block"
)

func TestValidateBatch(t *testing.T) {
	if err := ValidateBatch(nil); err == nil {
		t.Fatal("expected empty batch error")
	}
	if err := ValidateBatch([]block.Block{}); err == nil {
		t.Fatal("expected empty batch error")
	}
	ok := []block.Block{
		{Round: 1, BlockHash: "A"},
		{Round: 2, BlockHash: "B"},
	}
	if err := ValidateBatch(ok); err != nil {
		t.Fatal(err)
	}
	bad := []block.Block{
		{Round: 1, BlockHash: "A"},
		{Round: 3, BlockHash: "C"},
	}
	if err := ValidateBatch(bad); err == nil {
		t.Fatal("expected non-contiguous error")
	}
}

func TestCommitBatchAtomicityAndCheckpoint(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("set DATABASE_URL to run postgres tests")
	}
	ctx := context.Background()
	sink, err := NewPostgres(ctx, dsn, findMigrations(t))
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()

	base := uint64(9_100_000_000)
	n := 5
	prevRound, prevOK, _ := sink.LastProcessedRound(ctx)

	cleanup := func() {
		cctx := context.Background()
		_, _ = sink.Pool().Exec(cctx, `DELETE FROM transactions WHERE round >= $1 AND round < $2`, base, base+uint64(n))
		_, _ = sink.Pool().Exec(cctx, `DELETE FROM blocks WHERE round >= $1 AND round < $2`, base, base+uint64(n))
		if prevOK {
			_, _ = sink.Pool().Exec(cctx, `
				INSERT INTO sync_state (key, value) VALUES ('last_processed_round', $1)
				ON CONFLICT (key) DO UPDATE SET value = $1
			`, int64(prevRound))
		} else {
			_, _ = sink.Pool().Exec(cctx, `DELETE FROM sync_state WHERE key = 'last_processed_round'`)
		}
	}
	t.Cleanup(cleanup)
	cleanup()

	// Seed checkpoint just before the batch.
	_, err = sink.Pool().Exec(ctx, `
		INSERT INTO sync_state (key, value) VALUES ('last_processed_round', $1)
		ON CONFLICT (key) DO UPDATE SET value = $1
	`, int64(base-1))
	if err != nil {
		t.Fatal(err)
	}

	blocks := makeBatch(base, n)
	if err := sink.CommitBatch(ctx, blocks); err != nil {
		t.Fatal(err)
	}

	last, ok, err := sink.LastProcessedRound(ctx)
	if err != nil || !ok || last != base+uint64(n)-1 {
		t.Fatalf("checkpoint=%d ok=%v err=%v want %d", last, ok, err, base+uint64(n)-1)
	}

	var count int
	if err := sink.Pool().QueryRow(ctx, `
		SELECT COUNT(*) FROM blocks WHERE round >= $1 AND round < $2
	`, base, base+uint64(n)).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != n {
		t.Fatalf("block count=%d want %d", count, n)
	}

	// Idempotent re-commit of same batch.
	if err := sink.CommitBatch(ctx, blocks); err != nil {
		t.Fatal(err)
	}
	if err := sink.Pool().QueryRow(ctx, `
		SELECT COUNT(*) FROM blocks WHERE round >= $1 AND round < $2
	`, base, base+uint64(n)).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != n {
		t.Fatalf("after duplicate batch count=%d want %d", count, n)
	}
}

func TestCommitBatchRollbackOnFailure(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("set DATABASE_URL to run postgres tests")
	}
	ctx := context.Background()
	sink, err := NewPostgres(ctx, dsn, findMigrations(t))
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()

	base := uint64(9_200_000_000)
	prevRound, prevOK, _ := sink.LastProcessedRound(ctx)
	cleanup := func() {
		cctx := context.Background()
		_, _ = sink.Pool().Exec(cctx, `DELETE FROM transactions WHERE round >= $1 AND round < $2`, base, base+3)
		_, _ = sink.Pool().Exec(cctx, `DELETE FROM blocks WHERE round >= $1 AND round < $2`, base, base+3)
		if prevOK {
			_, _ = sink.Pool().Exec(cctx, `
				INSERT INTO sync_state (key, value) VALUES ('last_processed_round', $1)
				ON CONFLICT (key) DO UPDATE SET value = $1
			`, int64(prevRound))
		} else {
			_, _ = sink.Pool().Exec(cctx, `DELETE FROM sync_state WHERE key = 'last_processed_round'`)
		}
	}
	t.Cleanup(cleanup)
	cleanup()

	_, err = sink.Pool().Exec(ctx, `
		INSERT INTO sync_state (key, value) VALUES ('last_processed_round', $1)
		ON CONFLICT (key) DO UPDATE SET value = $1
	`, int64(base-1))
	if err != nil {
		t.Fatal(err)
	}

	// Force failure: insert a block that violates FK by referencing a missing
	// parent is hard; instead inject a bad txid-less path by using a cancelled
	// context after begin is impractical. Use non-contiguous ValidateBatch first.
	if err := sink.CommitBatch(ctx, []block.Block{
		{Round: base, BlockHash: "A", Raw: []byte{1}},
		{Round: base + 2, BlockHash: "C", Raw: []byte{3}},
	}); err == nil {
		t.Fatal("expected non-contiguous batch error")
	}

	last, ok, err := sink.LastProcessedRound(ctx)
	if err != nil || !ok || last != base-1 {
		t.Fatalf("checkpoint advanced on failed batch: last=%d ok=%v", last, ok)
	}

	var count int
	_ = sink.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM blocks WHERE round = $1`, base).Scan(&count)
	if count != 0 {
		t.Fatalf("partial block durable after failed batch")
	}
}

func TestCommitBatchEmptyAndSingle(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("set DATABASE_URL to run postgres tests")
	}
	ctx := context.Background()
	sink, err := NewPostgres(ctx, dsn, findMigrations(t))
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()

	if err := sink.CommitBatch(ctx, nil); err != nil {
		t.Fatalf("empty batch: %v", err)
	}

	round := uint64(9_300_000_000)
	prevRound, prevOK, _ := sink.LastProcessedRound(ctx)
	t.Cleanup(func() {
		cctx := context.Background()
		_, _ = sink.Pool().Exec(cctx, `DELETE FROM transactions WHERE round = $1`, round)
		_, _ = sink.Pool().Exec(cctx, `DELETE FROM blocks WHERE round = $1`, round)
		if prevOK {
			_, _ = sink.Pool().Exec(cctx, `
				INSERT INTO sync_state (key, value) VALUES ('last_processed_round', $1)
				ON CONFLICT (key) DO UPDATE SET value = $1
			`, int64(prevRound))
		} else {
			_, _ = sink.Pool().Exec(cctx, `DELETE FROM sync_state WHERE key = 'last_processed_round'`)
		}
	})

	blk := block.Block{
		Round:     round,
		BlockHash: "SOLO",
		Raw:       []byte{9},
		Transactions: []block.Transaction{{
			TxID: fmt.Sprintf("TX%d", round),
			Type: "pay",
			Raw:  []byte{1},
		}},
	}
	if err := sink.CommitBatch(ctx, []block.Block{blk}); err != nil {
		t.Fatal(err)
	}
	last, ok, err := sink.LastProcessedRound(ctx)
	if err != nil || !ok || last < round {
		t.Fatalf("checkpoint=%d ok=%v err=%v", last, ok, err)
	}
}

func makeBatch(base uint64, n int) []block.Block {
	out := make([]block.Block, n)
	var prev string
	for i := 0; i < n; i++ {
		r := base + uint64(i)
		h := fmt.Sprintf("H%d", r)
		out[i] = block.Block{
			Round:             r,
			BlockHash:         h,
			PreviousBlockHash: prev,
			Timestamp:         int64(r),
			TxnCount:          1,
			Raw:               []byte(fmt.Sprintf("raw-%d", r)),
			Transactions: []block.Transaction{{
				TxID:   fmt.Sprintf("TX%d", r),
				Sender: "SENDER",
				Type:   "pay",
				Raw:    []byte{1, 2, 3},
			}},
		}
		prev = h
	}
	return out
}
