package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/nicholasshellabarger/voi-fast-follower/internal/block"
)

func TestPostgresIdempotentProcessBlock(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("set DATABASE_URL to run postgres tests")
	}
	ctx := context.Background()
	migrations := findMigrations(t)
	sink, err := NewPostgres(ctx, dsn, migrations)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()

	// Use a disposable round far from tip, but never advance the real checkpoint
	// past tip — ProcessBlock upserts sync_state. Save/restore around the test.
	round := uint64(9_000_000_001)
	txid := "TXTEST9000000001"
	prevRound, prevOK, _ := sink.LastProcessedRound(ctx)
	cleanup := func() {
		cctx := context.Background()
		_, _ = sink.Pool().Exec(cctx, `DELETE FROM transactions WHERE txid = $1 OR round = $2`, txid, round)
		_, _ = sink.Pool().Exec(cctx, `DELETE FROM blocks WHERE round = $1`, round)
		// Force checkpoint restore (normal upsert only moves forward).
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
	cleanup() // ensure clean slate before insert

	blk := block.Block{
		Round:             round,
		BlockHash:         "TESTHASH",
		PreviousBlockHash: "PREV",
		Timestamp:         123,
		TxnCount:          1,
		Raw:               []byte{1, 2, 3},
		Transactions: []block.Transaction{{
			TxID:   txid,
			Sender: "SENDER",
			Type:   "pay",
			Raw:    []byte{9, 9},
		}},
	}

	if err := sink.ProcessBlock(ctx, blk); err != nil {
		t.Fatal(err)
	}
	if err := sink.ProcessBlock(ctx, blk); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := sink.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM blocks WHERE round = $1`, round).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("block count=%d want 1", count)
	}

	last, ok, err := sink.LastProcessedRound(ctx)
	if err != nil || !ok {
		t.Fatalf("checkpoint err=%v ok=%v", err, ok)
	}
	if last < round {
		t.Fatalf("checkpoint=%d want >= %d", last, round)
	}
}

func findMigrations(t *testing.T) string {
	t.Helper()
	candidates := []string{
		"migrations",
		filepath.Join("..", "..", "migrations"),
	}
	for _, c := range candidates {
		if st, err := os.Stat(c); err == nil && st.IsDir() {
			return c
		}
	}
	t.Fatal("migrations directory not found")
	return ""
}
