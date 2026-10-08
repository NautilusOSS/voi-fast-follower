package storage

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
)

func TestInsertModesEquivalence(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("set DATABASE_URL to run postgres tests")
	}
	ctx := context.Background()
	migrations := findMigrations(t)

	base := uint64(9_500_000_000)
	n := 25
	blocks := makeBatch(base, n)
	// Larger payload + multi-tx block to exercise COPY paths.
	blocks[0].Raw = make([]byte, 4096)
	for i := range blocks[0].Raw {
		blocks[0].Raw[i] = byte(i)
	}
	blocks[3].Transactions = append(blocks[3].Transactions, block.Transaction{
		TxID:   fmt.Sprintf("TX%d-B", base+3),
		Sender: "SENDER2",
		Type:   "appl",
		AppID:  42,
		Raw:    []byte{7, 7, 7},
	})
	blocks[3].TxnCount = 2

	type snap struct {
		checkpoint uint64
		blocks     []blockRow
		txs        []txRow
	}
	load := func(t *testing.T, mode string) snap {
		t.Helper()
		sink, err := NewPostgres(ctx, dsn, migrations)
		if err != nil {
			t.Fatal(err)
		}
		defer sink.Close()
		if err := sink.SetInsertMode(mode); err != nil {
			t.Fatal(err)
		}

		_, _ = sink.Pool().Exec(ctx, `DELETE FROM transactions WHERE round >= $1 AND round < $2`, base, base+uint64(n))
		_, _ = sink.Pool().Exec(ctx, `DELETE FROM blocks WHERE round >= $1 AND round < $2`, base, base+uint64(n))
		_, _ = sink.Pool().Exec(ctx, `
			INSERT INTO sync_state(key,value) VALUES ('last_processed_round',$1)
			ON CONFLICT (key) DO UPDATE SET value=$1`, int64(base-1))

		if err := sink.CommitBatch(ctx, blocks); err != nil {
			t.Fatalf("%s commit: %v", mode, err)
		}
		// Idempotent retry.
		if err := sink.CommitBatch(ctx, blocks); err != nil {
			t.Fatalf("%s retry: %v", mode, err)
		}

		cp, ok, err := sink.LastProcessedRound(ctx)
		if err != nil || !ok {
			t.Fatalf("%s checkpoint: ok=%v err=%v", mode, ok, err)
		}

		brows, err := sink.Pool().Query(ctx, `
			SELECT round, block_hash, previous_block_hash, timestamp, txn_count, raw_block
			FROM blocks WHERE round >= $1 AND round < $2 ORDER BY round`, base, base+uint64(n))
		if err != nil {
			t.Fatal(err)
		}
		defer brows.Close()
		var br []blockRow
		for brows.Next() {
			var r blockRow
			if err := brows.Scan(&r.round, &r.hash, &r.prev, &r.ts, &r.txnCount, &r.raw); err != nil {
				t.Fatal(err)
			}
			br = append(br, r)
		}

		trows, err := sink.Pool().Query(ctx, `
			SELECT round, txid, sender, type, COALESCE(app_id,0), raw_transaction
			FROM transactions WHERE round >= $1 AND round < $2
			ORDER BY round, txid`, base, base+uint64(n))
		if err != nil {
			t.Fatal(err)
		}
		defer trows.Close()
		var tr []txRow
		for trows.Next() {
			var r txRow
			if err := trows.Scan(&r.round, &r.txid, &r.sender, &r.typ, &r.appID, &r.raw); err != nil {
				t.Fatal(err)
			}
			tr = append(tr, r)
		}
		return snap{checkpoint: cp, blocks: br, txs: tr}
	}

	a := load(t, InsertModeUNNEST)
	b := load(t, InsertModeCOPY)
	if a.checkpoint != b.checkpoint {
		t.Fatalf("checkpoint unnest=%d copy=%d", a.checkpoint, b.checkpoint)
	}
	if len(a.blocks) != len(b.blocks) || len(a.blocks) != n {
		t.Fatalf("block rows unnest=%d copy=%d want %d", len(a.blocks), len(b.blocks), n)
	}
	for i := range a.blocks {
		if a.blocks[i].round != b.blocks[i].round ||
			a.blocks[i].hash != b.blocks[i].hash ||
			a.blocks[i].prev != b.blocks[i].prev ||
			a.blocks[i].ts != b.blocks[i].ts ||
			a.blocks[i].txnCount != b.blocks[i].txnCount ||
			string(a.blocks[i].raw) != string(b.blocks[i].raw) {
			t.Fatalf("block[%d] mismatch\nunnest=%+v\ncopy=%+v", i, a.blocks[i], b.blocks[i])
		}
	}
	if len(a.txs) != len(b.txs) {
		t.Fatalf("tx rows unnest=%d copy=%d", len(a.txs), len(b.txs))
	}
	for i := range a.txs {
		if a.txs[i].round != b.txs[i].round ||
			a.txs[i].txid != b.txs[i].txid ||
			a.txs[i].sender != b.txs[i].sender ||
			a.txs[i].typ != b.txs[i].typ ||
			a.txs[i].appID != b.txs[i].appID ||
			string(a.txs[i].raw) != string(b.txs[i].raw) {
			t.Fatalf("tx[%d] mismatch\nunnest=%+v\ncopy=%+v", i, a.txs[i], b.txs[i])
		}
	}
}

type blockRow struct {
	round    int64
	hash     string
	prev     string
	ts       int64
	txnCount int32
	raw      []byte
}

type txRow struct {
	round  int64
	txid   string
	sender string
	typ    string
	appID  int64
	raw    []byte
}

func TestCOPYEmptySingleLargeBatch(t *testing.T) {
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
	_ = sink.SetInsertMode(InsertModeCOPY)

	if err := sink.CommitBatch(ctx, nil); err != nil {
		t.Fatal(err)
	}

	base := uint64(9_510_000_000)
	prevRound, prevOK, _ := sink.LastProcessedRound(ctx)
	t.Cleanup(func() {
		cctx := context.Background()
		_, _ = sink.Pool().Exec(cctx, `DELETE FROM transactions WHERE round >= $1 AND round < $2`, base, base+200)
		_, _ = sink.Pool().Exec(cctx, `DELETE FROM blocks WHERE round >= $1 AND round < $2`, base, base+200)
		if prevOK {
			_, _ = sink.Pool().Exec(cctx, `
				INSERT INTO sync_state(key,value) VALUES ('last_processed_round',$1)
				ON CONFLICT (key) DO UPDATE SET value=$1`, int64(prevRound))
		} else {
			_, _ = sink.Pool().Exec(cctx, `DELETE FROM sync_state WHERE key='last_processed_round'`)
		}
	})
	_, _ = sink.Pool().Exec(ctx, `
		INSERT INTO sync_state(key,value) VALUES ('last_processed_round',$1)
		ON CONFLICT (key) DO UPDATE SET value=$1`, int64(base-1))

	solo := makeBatch(base, 1)
	if err := sink.CommitBatch(ctx, solo); err != nil {
		t.Fatal(err)
	}
	large := makeBatch(base+1, 100)
	if err := sink.CommitBatch(ctx, large); err != nil {
		t.Fatal(err)
	}
	last, ok, err := sink.LastProcessedRound(ctx)
	if err != nil || !ok || last != base+100 {
		t.Fatalf("checkpoint=%d ok=%v want %d", last, ok, base+100)
	}
	var count int
	_ = sink.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM blocks WHERE round >= $1 AND round <= $2`, base, base+100).Scan(&count)
	if count != 101 {
		t.Fatalf("blocks=%d want 101", count)
	}
}

func TestCrashPointsRollback(t *testing.T) {
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
	_ = sink.SetInsertMode(InsertModeCOPY)

	base := uint64(9_520_000_000)
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
		sink.SetCrashPoint("")
	}
	t.Cleanup(cleanup)
	cleanup()

	_, err = sink.Pool().Exec(ctx, `
		INSERT INTO sync_state(key,value) VALUES ('last_processed_round',$1)
		ON CONFLICT (key) DO UPDATE SET value=$1`, int64(base-1))
	if err != nil {
		t.Fatal(err)
	}

	batch := makeBatch(base, 5)
	for _, point := range []string{"before_begin", "during_writes", "during_checkpoint", "before_commit"} {
		sink.SetCrashPoint(point)
		if err := sink.CommitBatch(ctx, batch); err == nil {
			t.Fatalf("expected crash at %s", point)
		}
		last, ok, err := sink.LastProcessedRound(ctx)
		if err != nil || !ok || last != base-1 {
			t.Fatalf("after %s checkpoint=%d ok=%v want %d", point, last, ok, base-1)
		}
		var n int
		_ = sink.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM blocks WHERE round >= $1 AND round < $2`, base, base+5).Scan(&n)
		if n != 0 {
			t.Fatalf("after %s durable blocks=%d", point, n)
		}
		sink.SetCrashPoint("")
	}

	// after_commit: rows + checkpoint durable, but CommitBatch returns error (lost ACK).
	sink.SetCrashPoint("after_commit")
	if err := sink.CommitBatch(ctx, batch); err == nil {
		t.Fatal("expected after_commit error")
	}
	last, ok, err := sink.LastProcessedRound(ctx)
	if err != nil || !ok || last != base+4 {
		t.Fatalf("after_commit checkpoint=%d want %d", last, base+4)
	}
	// Retry is idempotent; follower would resume at checkpoint+1 after observing durable state.
	sink.SetCrashPoint("")
	if err := sink.CommitBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	var n int
	_ = sink.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM blocks WHERE round >= $1 AND round <= $2`, base, base+4).Scan(&n)
	if n != 5 {
		t.Fatalf("blocks after retry=%d", n)
	}
	if resume := last + 1; resume != base+5 {
		t.Fatalf("resume=%d", resume)
	}
}

func TestCommitPhaseTimingsRecorded(t *testing.T) {
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
	_ = sink.SetInsertMode(InsertModeCOPY)

	base := uint64(9_530_000_000)
	t.Cleanup(func() {
		cctx := context.Background()
		_, _ = sink.Pool().Exec(cctx, `DELETE FROM transactions WHERE round = $1`, base)
		_, _ = sink.Pool().Exec(cctx, `DELETE FROM blocks WHERE round = $1`, base)
	})
	if err := sink.CommitBatch(ctx, makeBatch(base, 1)); err != nil {
		t.Fatal(err)
	}
	tm := sink.LastTimings()
	if tm.Mode != InsertModeCOPY || tm.BatchSize != 1 || tm.Total <= 0 {
		t.Fatalf("timings=%+v", tm)
	}
	if tm.Begin < 0 || tm.Writes <= 0 || tm.Checkpoint <= 0 || tm.Commit < 0 {
		t.Fatalf("phase timings incomplete: %+v", tm)
	}
}
