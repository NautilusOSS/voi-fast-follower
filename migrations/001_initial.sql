-- Voi Fast Follower Phase 1 schema
-- Intentionally small: raw blocks first, not an indexer.

CREATE TABLE IF NOT EXISTS blocks (
    round               BIGINT PRIMARY KEY,
    block_hash          TEXT NOT NULL,
    previous_block_hash TEXT NOT NULL DEFAULT '',
    timestamp           BIGINT NOT NULL,
    txn_count           INTEGER NOT NULL DEFAULT 0,
    raw_block           BYTEA NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transactions (
    id               BIGSERIAL PRIMARY KEY,
    round            BIGINT NOT NULL REFERENCES blocks(round),
    txid             TEXT NOT NULL,
    sender           TEXT NOT NULL DEFAULT '',
    type             TEXT NOT NULL DEFAULT '',
    raw_transaction  BYTEA NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (txid)
);

CREATE INDEX IF NOT EXISTS idx_transactions_round ON transactions(round);

CREATE TABLE IF NOT EXISTS sync_state (
    key   TEXT PRIMARY KEY,
    value BIGINT NOT NULL
);
