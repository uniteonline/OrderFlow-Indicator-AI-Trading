-- Remove historical duplicate snapshots before enforcing idempotent upserts.
WITH ranked AS (
    SELECT
        ctid,
        row_number() OVER (
            PARTITION BY ts_snapshot, symbol, indicator_code, window_code
            ORDER BY created_at DESC, ctid DESC
        ) AS rn
    FROM feat.indicator_snapshot
)
DELETE FROM feat.indicator_snapshot t
USING ranked r
WHERE t.ctid = r.ctid
  AND r.rn > 1;

CREATE UNIQUE INDEX IF NOT EXISTS idx_indicator_snapshot_idempotent
ON feat.indicator_snapshot(ts_snapshot, symbol, indicator_code, window_code);
