-- =============================================================
-- fix_slow_ops_queries.sql
-- Target: Market Data Ingestor slow ops SQL (checkpoint/heartbeat/backfill logs)
--
-- Safe to run repeatedly:
--   psql -h 127.0.0.1 -U postgres -d orderflow_ops -f sql/fix_slow_ops_queries.sql
--
-- Notes:
-- - No schema contract changes; only planner stats + table storage/autovac tuning.
-- - fillfactor affects new/updated tuples progressively.
-- =============================================================

-- 1) Refresh planner stats for ops hot tables.
ANALYZE ops.ingest_checkpoint;
ANALYZE ops.service_heartbeat;
ANALYZE ops.rest_backfill_job_run;

-- 2) Tune update-heavy checkpoint table:
--    - lower fillfactor to leave room for HOT updates
--    - tighter autovacuum/analyze cadence to control bloat/WAL churn
ALTER TABLE ops.ingest_checkpoint SET (
    fillfactor = 80,
    autovacuum_vacuum_scale_factor  = 0.02,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_vacuum_threshold     = 100,
    autovacuum_analyze_threshold    = 100
);

-- 3) Heartbeat is tiny but very frequently updated.
ALTER TABLE ops.service_heartbeat SET (
    fillfactor = 90,
    autovacuum_vacuum_scale_factor  = 0.05,
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_vacuum_threshold     = 20,
    autovacuum_analyze_threshold    = 20
);

-- 4) Backfill job log is insert-heavy; keep stats fresh under bursts.
ALTER TABLE ops.rest_backfill_job_run SET (
    autovacuum_vacuum_scale_factor  = 0.05,
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_vacuum_threshold     = 1000,
    autovacuum_analyze_threshold    = 1000
);

-- 5) Re-analyze after reloption changes.
ANALYZE ops.ingest_checkpoint;
ANALYZE ops.service_heartbeat;
ANALYZE ops.rest_backfill_job_run;
