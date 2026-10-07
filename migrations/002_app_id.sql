-- Application ID extracted from appl txs for explorer aggregations.
ALTER TABLE transactions
    ADD COLUMN IF NOT EXISTS app_id BIGINT;

CREATE INDEX IF NOT EXISTS idx_transactions_app_id
    ON transactions (app_id)
    WHERE app_id IS NOT NULL AND app_id > 0;
