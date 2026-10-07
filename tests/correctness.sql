-- Gap check: no missing rounds between min and max stored block.
WITH bounds AS (
  SELECT MIN(round) AS lo, MAX(round) AS hi, COUNT(*) AS n FROM blocks
),
expected AS (
  SELECT hi - lo + 1 AS want, n AS have FROM bounds
)
SELECT * FROM expected;
-- want should equal have

-- Duplicate rounds should be impossible (PK); verify txn uniqueness sample:
SELECT txid, COUNT(*) FROM transactions GROUP BY txid HAVING COUNT(*) > 1;
