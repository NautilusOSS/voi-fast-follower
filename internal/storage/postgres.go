package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nicholasshellabarger/voi-fast-follower/internal/block"
)

const checkpointKey = "last_processed_round"

// PostgresBlockSink persists blocks, transactions, and sync checkpoints.
type PostgresBlockSink struct {
	pool *pgxpool.Pool
}

// NewPostgres opens a pool and applies migrations from migrationsPath.
func NewPostgres(ctx context.Context, databaseURL, migrationsPath string) (*PostgresBlockSink, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	s := &PostgresBlockSink{pool: pool}
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

// ProcessBlock inserts the block and transactions, then advances the checkpoint
// in a single transaction.
func (s *PostgresBlockSink) ProcessBlock(ctx context.Context, blk block.Block) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO blocks (round, block_hash, previous_block_hash, timestamp, txn_count, raw_block)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (round) DO NOTHING
	`, blk.Round, blk.BlockHash, blk.PreviousBlockHash, blk.Timestamp, blk.TxnCount, blk.Raw)
	if err != nil {
		return fmt.Errorf("insert block %d: %w", blk.Round, err)
	}

	for _, txn := range blk.Transactions {
		var appID any
		if txn.AppID > 0 {
			appID = int64(txn.AppID)
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO transactions (round, txid, sender, type, app_id, raw_transaction)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (txid) DO NOTHING
		`, blk.Round, txn.TxID, txn.Sender, txn.Type, appID, txn.Raw)
		if err != nil {
			return fmt.Errorf("insert tx %s round %d: %w", txn.TxID, blk.Round, err)
		}
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO sync_state (key, value)
		VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE
		SET value = EXCLUDED.value
		WHERE sync_state.value < EXCLUDED.value
	`, checkpointKey, int64(blk.Round))
	if err != nil {
		return fmt.Errorf("update checkpoint: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit block %d: %w", blk.Round, err)
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
		// Column may not exist yet on very old DBs mid-migrate; ignore.
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
