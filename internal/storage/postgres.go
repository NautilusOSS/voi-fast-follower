package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nicholasshellabarger/voi-fast-follower/internal/block"
)

const checkpointKey = "last_processed_round"

// Insert modes for CommitBatch.
const (
	InsertModeUNNEST = "unnest"
	InsertModeCOPY   = "copy"
)

// CommitPhaseTimings breaks down time spent inside CommitBatch.
type CommitPhaseTimings struct {
	Mode       string
	BatchSize  int
	Begin      time.Duration
	Writes     time.Duration
	Checkpoint time.Duration
	Commit     time.Duration
	Total      time.Duration
}

// PostgresBlockSink persists blocks, transactions, and sync checkpoints.
//
// Per-batch work (UNNEST mode):
//
//	BEGIN
//	1× INSERT…SELECT UNNEST blocks   (+ ON CONFLICT DO NOTHING)
//	1× INSERT…SELECT UNNEST txs      (+ ON CONFLICT DO NOTHING)  [if any]
//	1× UPSERT sync_state checkpoint
//	COMMIT
//
// COPY mode replaces the two UNNEST inserts with:
//
//	session temp stage_* (prepared Once via pool AfterConnect; ON COMMIT DELETE ROWS)
//	COPY stage_blocks / stage_txs
//	INSERT…SELECT…ON CONFLICT into destination tables
//
// All of the above remains one atomic transaction. Checkpoint never advances
// without the corresponding block/tx rows.
type PostgresBlockSink struct {
	pool *pgxpool.Pool

	insertMode  string
	asyncCommit bool // experimental: SET LOCAL synchronous_commit=off

	// crashPoint is a test-only fault injector (empty in production).
	// Values: before_begin | during_writes | during_checkpoint | before_commit | after_commit
	crashPoint string

	mu          sync.Mutex
	lastTimings CommitPhaseTimings
	onTimings   func(CommitPhaseTimings)
}

// NewPostgres opens a pool and applies migrations from migrationsPath.
func NewPostgres(ctx context.Context, databaseURL, migrationsPath string) (*PostgresBlockSink, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	// Keep the pool small; the follower commits on a single goroutine.
	if cfg.MaxConns < 4 {
		cfg.MaxConns = 4
	}
	// Prepare session-scoped staging tables once per pooled connection so the
	// COPY hot path only pays TRUNCATE + COPY + INSERT (no CREATE each batch).
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		return prepareStagingTables(ctx, conn)
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	s := &PostgresBlockSink{
		pool:       pool,
		insertMode: InsertModeUNNEST, // Phase 4 measured default; overridden via SetInsertMode
	}
	if migrationsPath == "" {
		migrationsPath = "migrations"
	}
	if err := s.migrate(ctx, migrationsPath); err != nil {
		pool.Close()
		return nil, err
	}
	if err := s.backfillAppIDs(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// SetInsertMode selects "unnest" or "copy" for CommitBatch.
func (s *PostgresBlockSink) SetInsertMode(mode string) error {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", InsertModeUNNEST:
		s.insertMode = InsertModeUNNEST
	case InsertModeCOPY:
		s.insertMode = InsertModeCOPY
	default:
		return fmt.Errorf("unknown insert mode %q (want unnest|copy)", mode)
	}
	return nil
}

// InsertMode returns the active bulk-insert strategy.
func (s *PostgresBlockSink) InsertMode() string { return s.insertMode }

// SetAsyncCommit enables an experimental SET LOCAL synchronous_commit=off
// inside each batch transaction. Disabled by default; not for production.
func (s *PostgresBlockSink) SetAsyncCommit(enabled bool) {
	s.asyncCommit = enabled
}

// OnTimings registers an optional callback invoked after each successful CommitBatch.
func (s *PostgresBlockSink) OnTimings(fn func(CommitPhaseTimings)) {
	s.onTimings = fn
}

// LastTimings returns the most recent successful CommitBatch phase timings.
func (s *PostgresBlockSink) LastTimings() CommitPhaseTimings {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastTimings
}

// SetCrashPoint arms a test-only fault injector. Pass "" to clear.
func (s *PostgresBlockSink) SetCrashPoint(point string) {
	s.crashPoint = point
}

func (s *PostgresBlockSink) crash(point string) error {
	if s.crashPoint == point {
		return fmt.Errorf("injected crash at %s", point)
	}
	return nil
}

func (s *PostgresBlockSink) migrate(ctx context.Context, migrationsPath string) error {
	files, err := loadMigrationSQL(migrationsPath)
	if err != nil {
		return err
	}
	for _, f := range files {
		if _, err := s.pool.Exec(ctx, f); err != nil {
			return fmt.Errorf("apply migration: %w", err)
		}
	}
	return nil
}

func loadMigrationSQL(migrationsPath string) ([]string, error) {
	entries, err := os.ReadDir(migrationsPath)
	if err != nil {
		return nil, fmt.Errorf("read migrations dir %q: %w", migrationsPath, err)
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		return nil, fmt.Errorf("no .sql migrations in %q", migrationsPath)
	}
	out := make([]string, 0, len(names))
	for _, name := range names {
		b, err := os.ReadFile(filepath.Join(migrationsPath, name))
		if err != nil {
			return nil, err
		}
		out = append(out, string(b))
	}
	return out, nil
}

// Commit persists a single block. Equivalent to CommitBatch of one element.
func (s *PostgresBlockSink) Commit(ctx context.Context, blk block.Block) error {
	return s.CommitBatch(ctx, []block.Block{blk})
}

// CommitBatch inserts all blocks and their transactions, then advances the
// checkpoint to the final round, in one PostgreSQL transaction.
//
// Inserts are idempotent (ON CONFLICT DO NOTHING). On any error the
// transaction rolls back and the checkpoint is unchanged.
func (s *PostgresBlockSink) CommitBatch(ctx context.Context, blocks []block.Block) error {
	if len(blocks) == 0 {
		return nil
	}
	if err := ValidateBatch(blocks); err != nil {
		return err
	}

	totalStart := time.Now()
	var timings CommitPhaseTimings
	timings.Mode = s.insertMode
	timings.BatchSize = len(blocks)

	if err := s.crash("before_begin"); err != nil {
		return err
	}

	t0 := time.Now()
	tx, err := s.pool.Begin(ctx)
	timings.Begin = time.Since(t0)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if s.asyncCommit {
		if _, err := tx.Exec(ctx, `SET LOCAL synchronous_commit = off`); err != nil {
			return fmt.Errorf("set async commit: %w", err)
		}
	}

	t1 := time.Now()
	if err := s.crash("during_writes"); err != nil {
		return err
	}
	switch s.insertMode {
	case InsertModeCOPY:
		err = insertBatchCOPY(ctx, tx, blocks)
	default:
		err = insertBatchUNNEST(ctx, tx, blocks)
	}
	timings.Writes = time.Since(t1)
	if err != nil {
		return err
	}

	final := blocks[len(blocks)-1].Round
	t2 := time.Now()
	if err := s.crash("during_checkpoint"); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO sync_state (key, value)
		VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE
		SET value = EXCLUDED.value
		WHERE sync_state.value < EXCLUDED.value
	`, checkpointKey, int64(final))
	timings.Checkpoint = time.Since(t2)
	if err != nil {
		return fmt.Errorf("update checkpoint: %w", err)
	}

	if err := s.crash("before_commit"); err != nil {
		return err
	}

	t3 := time.Now()
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit batch %d-%d: %w", blocks[0].Round, final, err)
	}
	timings.Commit = time.Since(t3)
	timings.Total = time.Since(totalStart)

	s.mu.Lock()
	s.lastTimings = timings
	s.mu.Unlock()
	if s.onTimings != nil {
		s.onTimings(timings)
	}
	if err := s.crash("after_commit"); err != nil {
		// Data + checkpoint are durable; caller must treat this like a lost ACK.
		return err
	}
	return nil
}

func insertBatchUNNEST(ctx context.Context, tx pgx.Tx, blocks []block.Block) error {
	if err := insertBlocksUNNEST(ctx, tx, blocks); err != nil {
		return err
	}
	return insertTransactionsUNNEST(ctx, tx, blocks)
}

func insertBlocksUNNEST(ctx context.Context, tx pgx.Tx, blocks []block.Block) error {
	n := len(blocks)
	rounds := make([]int64, n)
	hashes := make([]string, n)
	prevs := make([]string, n)
	timestamps := make([]int64, n)
	txnCounts := make([]int32, n)
	raws := make([][]byte, n)
	for i, blk := range blocks {
		rounds[i] = int64(blk.Round)
		hashes[i] = blk.BlockHash
		prevs[i] = blk.PreviousBlockHash
		timestamps[i] = blk.Timestamp
		txnCounts[i] = int32(blk.TxnCount)
		raws[i] = blk.Raw
	}

	_, err := tx.Exec(ctx, `
		INSERT INTO blocks (round, block_hash, previous_block_hash, timestamp, txn_count, raw_block)
		SELECT * FROM UNNEST(
			$1::bigint[],
			$2::text[],
			$3::text[],
			$4::bigint[],
			$5::int[],
			$6::bytea[]
		) AS t(round, block_hash, previous_block_hash, timestamp, txn_count, raw_block)
		ON CONFLICT (round) DO NOTHING
	`, rounds, hashes, prevs, timestamps, txnCounts, raws)
	if err != nil {
		return fmt.Errorf("unnest insert blocks %d-%d: %w", blocks[0].Round, blocks[n-1].Round, err)
	}
	return nil
}

func insertTransactionsUNNEST(ctx context.Context, tx pgx.Tx, blocks []block.Block) error {
	total := 0
	for _, blk := range blocks {
		total += len(blk.Transactions)
	}
	if total == 0 {
		return nil
	}

	rounds := make([]int64, 0, total)
	txids := make([]string, 0, total)
	senders := make([]string, 0, total)
	types := make([]string, 0, total)
	appIDs := make([]int64, 0, total)
	raws := make([][]byte, 0, total)

	for _, blk := range blocks {
		for _, txn := range blk.Transactions {
			rounds = append(rounds, int64(blk.Round))
			txids = append(txids, txn.TxID)
			senders = append(senders, txn.Sender)
			types = append(types, txn.Type)
			appIDs = append(appIDs, int64(txn.AppID))
			raws = append(raws, txn.Raw)
		}
	}

	_, err := tx.Exec(ctx, `
		INSERT INTO transactions (round, txid, sender, type, app_id, raw_transaction)
		SELECT
			t.round,
			t.txid,
			t.sender,
			t.type,
			NULLIF(t.app_id, 0),
			t.raw_transaction
		FROM UNNEST(
			$1::bigint[],
			$2::text[],
			$3::text[],
			$4::text[],
			$5::bigint[],
			$6::bytea[]
		) AS t(round, txid, sender, type, app_id, raw_transaction)
		ON CONFLICT (txid) DO NOTHING
	`, rounds, txids, senders, types, appIDs, raws)
	if err != nil {
		return fmt.Errorf("unnest insert transactions for blocks %d-%d: %w",
			blocks[0].Round, blocks[len(blocks)-1].Round, err)
	}
	return nil
}

func prepareStagingTables(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, `
		CREATE TEMP TABLE IF NOT EXISTS stage_blocks (
			round               BIGINT NOT NULL,
			block_hash          TEXT   NOT NULL,
			previous_block_hash TEXT   NOT NULL,
			timestamp           BIGINT NOT NULL,
			txn_count           INT    NOT NULL,
			raw_block           BYTEA  NOT NULL
		) ON COMMIT DELETE ROWS
	`)
	if err != nil {
		return fmt.Errorf("prepare stage_blocks: %w", err)
	}
	_, err = conn.Exec(ctx, `
		CREATE TEMP TABLE IF NOT EXISTS stage_transactions (
			round           BIGINT NOT NULL,
			txid            TEXT   NOT NULL,
			sender          TEXT   NOT NULL,
			type            TEXT   NOT NULL,
			app_id          BIGINT NOT NULL,
			raw_transaction BYTEA  NOT NULL
		) ON COMMIT DELETE ROWS
	`)
	if err != nil {
		return fmt.Errorf("prepare stage_transactions: %w", err)
	}
	return nil
}

func insertBatchCOPY(ctx context.Context, tx pgx.Tx, blocks []block.Block) error {
	if err := copyBlocks(ctx, tx, blocks); err != nil {
		return err
	}
	return copyTransactions(ctx, tx, blocks)
}

func copyBlocks(ctx context.Context, tx pgx.Tx, blocks []block.Block) error {
	_, err := tx.CopyFrom(ctx,
		pgx.Identifier{"stage_blocks"},
		[]string{"round", "block_hash", "previous_block_hash", "timestamp", "txn_count", "raw_block"},
		pgx.CopyFromSlice(len(blocks), func(i int) ([]any, error) {
			b := blocks[i]
			return []any{
				int64(b.Round),
				b.BlockHash,
				b.PreviousBlockHash,
				b.Timestamp,
				int32(b.TxnCount),
				b.Raw,
			}, nil
		}),
	)
	if err != nil {
		return fmt.Errorf("copy stage_blocks: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO blocks (round, block_hash, previous_block_hash, timestamp, txn_count, raw_block)
		SELECT round, block_hash, previous_block_hash, timestamp, txn_count, raw_block
		FROM stage_blocks
		ON CONFLICT (round) DO NOTHING
	`)
	if err != nil {
		return fmt.Errorf("upsert blocks from stage: %w", err)
	}
	return nil
}

func copyTransactions(ctx context.Context, tx pgx.Tx, blocks []block.Block) error {
	total := 0
	for _, blk := range blocks {
		total += len(blk.Transactions)
	}
	if total == 0 {
		return nil
	}

	type row struct {
		round  int64
		txid   string
		sender string
		typ    string
		appID  int64
		raw    []byte
	}
	rows := make([]row, 0, total)
	for _, blk := range blocks {
		for _, txn := range blk.Transactions {
			rows = append(rows, row{
				round:  int64(blk.Round),
				txid:   txn.TxID,
				sender: txn.Sender,
				typ:    txn.Type,
				appID:  int64(txn.AppID),
				raw:    txn.Raw,
			})
		}
	}

	_, err := tx.CopyFrom(ctx,
		pgx.Identifier{"stage_transactions"},
		[]string{"round", "txid", "sender", "type", "app_id", "raw_transaction"},
		pgx.CopyFromSlice(len(rows), func(i int) ([]any, error) {
			r := rows[i]
			return []any{r.round, r.txid, r.sender, r.typ, r.appID, r.raw}, nil
		}),
	)
	if err != nil {
		return fmt.Errorf("copy stage_transactions: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO transactions (round, txid, sender, type, app_id, raw_transaction)
		SELECT
			round,
			txid,
			sender,
			type,
			NULLIF(app_id, 0),
			raw_transaction
		FROM stage_transactions
		ON CONFLICT (txid) DO NOTHING
	`)
	if err != nil {
		return fmt.Errorf("upsert transactions from stage: %w", err)
	}
	return nil
}

// MaxBlockRound returns the highest stored block round, if any.
func (s *PostgresBlockSink) MaxBlockRound(ctx context.Context) (uint64, bool, error) {
	var value *int64
	err := s.pool.QueryRow(ctx, `SELECT MAX(round) FROM blocks`).Scan(&value)
	if err != nil {
		return 0, false, err
	}
	if value == nil || *value < 0 {
		return 0, false, nil
	}
	return uint64(*value), true, nil
}

// LastProcessedRound returns the durable checkpoint.
func (s *PostgresBlockSink) LastProcessedRound(ctx context.Context) (uint64, bool, error) {
	var value int64
	err := s.pool.QueryRow(ctx, `
		SELECT value FROM sync_state WHERE key = $1
	`, checkpointKey).Scan(&value)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, false, nil
		}
		return 0, false, err
	}
	if value < 0 {
		return 0, false, nil
	}
	return uint64(value), true, nil
}

// BlockHash returns a stored block hash if present.
func (s *PostgresBlockSink) BlockHash(ctx context.Context, round uint64) (string, bool, error) {
	var hash string
	err := s.pool.QueryRow(ctx, `
		SELECT block_hash FROM blocks WHERE round = $1
	`, round).Scan(&hash)
	if err != nil {
		if err == pgx.ErrNoRows {
			return "", false, nil
		}
		return "", false, err
	}
	return hash, true, nil
}

// Close releases the pool.
func (s *PostgresBlockSink) Close() error {
	s.pool.Close()
	return nil
}

// Pool exposes the underlying pool for tests/metrics helpers.
func (s *PostgresBlockSink) Pool() *pgxpool.Pool {
	return s.pool
}

// backfillAppIDs fills app_id for existing appl rows that were ingested before the column existed.
func (s *PostgresBlockSink) backfillAppIDs(ctx context.Context) error {
	rows, err := s.pool.Query(ctx, `
		SELECT txid, raw_transaction
		FROM transactions
		WHERE type = 'appl' AND (app_id IS NULL OR app_id = 0)
	`)
	if err != nil {
		if strings.Contains(err.Error(), "app_id") {
			return nil
		}
		return fmt.Errorf("backfill app_id query: %w", err)
	}
	defer rows.Close()

	type row struct {
		txid string
		raw  []byte
	}
	var pending []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.txid, &r.raw); err != nil {
			return err
		}
		pending = append(pending, r)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, r := range pending {
		appID, ok := block.AppIDFromRaw(r.raw)
		if !ok || appID == 0 {
			continue
		}
		if _, err := s.pool.Exec(ctx, `
			UPDATE transactions SET app_id = $1 WHERE txid = $2
		`, int64(appID), r.txid); err != nil {
			return fmt.Errorf("backfill app_id %s: %w", r.txid, err)
		}
	}
	return nil
}
