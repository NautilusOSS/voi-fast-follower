package storage

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// BlockRow is a JSON-friendly block summary for the explorer API.
type BlockRow struct {
	Round             uint64    `json:"round"`
	BlockHash         string    `json:"block_hash"`
	PreviousBlockHash string    `json:"previous_block_hash"`
	Timestamp         int64     `json:"timestamp"`
	TxnCount          int       `json:"txn_count"`
	CreatedAt         time.Time `json:"created_at"`
}

// TxRow is a JSON-friendly transaction summary.
type TxRow struct {
	Round     uint64    `json:"round"`
	TxID      string    `json:"txid"`
	Sender    string    `json:"sender"`
	Type      string    `json:"type"`
	AppID     uint64    `json:"app_id,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// SenderStat ranks senders by activity.
type SenderStat struct {
	Sender       string `json:"sender"`
	TxCount      int64  `json:"tx_count"`
	RoundsActive int64  `json:"rounds_active"`
}

// AppStat ranks applications by call count.
type AppStat struct {
	AppID          uint64 `json:"app_id"`
	CallCount      int64  `json:"call_count"`
	UniqueSenders  int64  `json:"unique_senders"`
	RoundsActive   int64  `json:"rounds_active"`
	LastRound      uint64 `json:"last_round"`
}

// ExplorerStatus is a compact sync snapshot for the UI.
type ExplorerStatus struct {
	LastProcessedRound uint64 `json:"last_processed_round"`
	BlockCount         int64  `json:"block_count"`
	TxCount            int64  `json:"tx_count"`
	MinRound           uint64 `json:"min_round"`
	MaxRound           uint64 `json:"max_round"`
	UniqueSenders      int64  `json:"unique_senders"`
}

// Status returns aggregate explorer stats.
func (s *PostgresBlockSink) Status(ctx context.Context) (ExplorerStatus, error) {
	var st ExplorerStatus
	var last *int64
	_ = s.pool.QueryRow(ctx, `SELECT value FROM sync_state WHERE key = $1`, checkpointKey).Scan(&last)
	if last != nil && *last >= 0 {
		st.LastProcessedRound = uint64(*last)
	}
	err := s.pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM blocks),
			(SELECT COUNT(*) FROM transactions),
			COALESCE((SELECT MIN(round) FROM blocks), 0),
			COALESCE((SELECT MAX(round) FROM blocks), 0),
			(SELECT COUNT(DISTINCT sender) FROM transactions)
	`).Scan(&st.BlockCount, &st.TxCount, &st.MinRound, &st.MaxRound, &st.UniqueSenders)
	return st, err
}

// ListBlocks returns recent blocks, optionally before a round.
func (s *PostgresBlockSink) ListBlocks(ctx context.Context, limit int, before uint64) ([]BlockRow, error) {
	if limit <= 0 || limit > 200 {
		limit = 40
	}
	var (
		rows pgx.Rows
		err  error
	)
	if before > 0 {
		rows, err = s.pool.Query(ctx, `
			SELECT round, block_hash, previous_block_hash, timestamp, txn_count, created_at
			FROM blocks WHERE round < $1
			ORDER BY round DESC LIMIT $2
		`, before, limit)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT round, block_hash, previous_block_hash, timestamp, txn_count, created_at
			FROM blocks
			ORDER BY round DESC LIMIT $1
		`, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanBlocks(rows)
}

// GetBlock returns one block by round.
func (s *PostgresBlockSink) GetBlock(ctx context.Context, round uint64) (BlockRow, bool, error) {
	var b BlockRow
	err := s.pool.QueryRow(ctx, `
		SELECT round, block_hash, previous_block_hash, timestamp, txn_count, created_at
		FROM blocks WHERE round = $1
	`, round).Scan(&b.Round, &b.BlockHash, &b.PreviousBlockHash, &b.Timestamp, &b.TxnCount, &b.CreatedAt)
	if err == pgx.ErrNoRows {
		return BlockRow{}, false, nil
	}
	if err != nil {
		return BlockRow{}, false, err
	}
	return b, true, nil
}

// ListTransactions lists recent txs, optionally filtered.
func (s *PostgresBlockSink) ListTransactions(ctx context.Context, limit int, round uint64, sender string, appID uint64) ([]TxRow, error) {
	if limit <= 0 || limit > 200 {
		limit = 40
	}
	rows, err := s.pool.Query(ctx, `
		SELECT round, txid, sender, type, COALESCE(app_id, 0), created_at
		FROM transactions
		WHERE ($1::bigint = 0 OR round = $1)
		  AND ($2::text = '' OR sender = $2)
		  AND ($3::bigint = 0 OR app_id = $3)
		ORDER BY round DESC, id DESC
		LIMIT $4
	`, int64(round), sender, int64(appID), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTxs(rows)
}

// TopSenders returns most active senders.
func (s *PostgresBlockSink) TopSenders(ctx context.Context, limit int) ([]SenderStat, error) {
	if limit <= 0 || limit > 50 {
		limit = 10
	}
	rows, err := s.pool.Query(ctx, `
		SELECT sender, COUNT(*) AS tx_count, COUNT(DISTINCT round) AS rounds_active
		FROM transactions
		GROUP BY sender
		ORDER BY tx_count DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]SenderStat, 0, limit)
	for rows.Next() {
		var sstat SenderStat
		if err := rows.Scan(&sstat.Sender, &sstat.TxCount, &sstat.RoundsActive); err != nil {
			return nil, err
		}
		out = append(out, sstat)
	}
	return out, rows.Err()
}

// TopApps returns most-called application IDs.
func (s *PostgresBlockSink) TopApps(ctx context.Context, limit int) ([]AppStat, error) {
	if limit <= 0 || limit > 50 {
		limit = 10
	}
	rows, err := s.pool.Query(ctx, `
		SELECT app_id,
		       COUNT(*) AS call_count,
		       COUNT(DISTINCT sender) AS unique_senders,
		       COUNT(DISTINCT round) AS rounds_active,
		       MAX(round) AS last_round
		FROM transactions
		WHERE app_id IS NOT NULL AND app_id > 0
		GROUP BY app_id
		ORDER BY call_count DESC, last_round DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]AppStat, 0, limit)
	for rows.Next() {
		var a AppStat
		if err := rows.Scan(&a.AppID, &a.CallCount, &a.UniqueSenders, &a.RoundsActive, &a.LastRound); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// BusyBlocks returns blocks with the most transactions.
func (s *PostgresBlockSink) BusyBlocks(ctx context.Context, limit int) ([]BlockRow, error) {
	if limit <= 0 || limit > 50 {
		limit = 10
	}
	rows, err := s.pool.Query(ctx, `
		SELECT round, block_hash, previous_block_hash, timestamp, txn_count, created_at
		FROM blocks
		WHERE txn_count > 0
		ORDER BY txn_count DESC, round DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanBlocks(rows)
}

func scanBlocks(rows pgx.Rows) ([]BlockRow, error) {
	out := make([]BlockRow, 0, 40)
	for rows.Next() {
		var b BlockRow
		if err := rows.Scan(&b.Round, &b.BlockHash, &b.PreviousBlockHash, &b.Timestamp, &b.TxnCount, &b.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func scanTxs(rows pgx.Rows) ([]TxRow, error) {
	out := make([]TxRow, 0, 40)
	for rows.Next() {
		var t TxRow
		var appID int64
		if err := rows.Scan(&t.Round, &t.TxID, &t.Sender, &t.Type, &appID, &t.CreatedAt); err != nil {
			return nil, err
		}
		if appID > 0 {
			t.AppID = uint64(appID)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
