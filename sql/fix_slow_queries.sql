-- =============================================================
-- fix_slow_queries.sql
-- Addresses Indicator Engine slow-log issues:
--   1) SELECT slow (11s): stale planner stats on md.agg_*_1m
--   2) INSERT slow (50s / 12s): GIN index flush spikes on feat hypertables
--
-- Run idempotently against the live 'orderflow' database:
--   psql -h 127.0.0.1 -U postgres -d orderflow -f sql/fix_slow_queries.sql
--
-- The ANALYZE commands are safe on a live database.
-- The index / ALTER commands use CONCURRENTLY / IF NOT EXISTS where possible.
-- =============================================================

-- 1) Refresh planner statistics (planner was 40-80x off before fix)
-- md tables are regular (non-hypertable) so ANALYZE runs on the full table.
ANALYZE md.agg_trade_1m;
ANALYZE md.agg_orderbook_1m;
ANALYZE md.agg_liq_1m;
ANALYZE md.agg_funding_mark_1m;

-- feat hypertables: ANALYZE on the parent propagates to chunks in PG14+
ANALYZE feat.indicator_snapshot;
ANALYZE feat.indicator_level_value;

-- 2) Ensure backfill read indexes exist (symbol-first for backfill WHERE pattern)
--    These are already in optimize_indicator_backfill_indexes.sql; we
--    duplicate here with IF NOT EXISTS so this script is safe to re-run.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_agg_trade_1m_backfill
    ON md.agg_trade_1m (symbol, ts_event, market);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_agg_orderbook_1m_backfill
    ON md.agg_orderbook_1m (symbol, ts_event, market);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_agg_liq_1m_backfill
    ON md.agg_liq_1m (symbol, ts_event, market);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_agg_funding_mark_1m_backfill
    ON md.agg_funding_mark_1m (symbol, ts_event, market);

-- 3) Autovacuum tuning for write-heavy feat hypertable chunks.
--    The parent-table settings propagate to new chunks automatically
--    via TimescaleDB chunk creation; the chunk-level settings below
--    cover the current large chunk immediately.
--
--    Lower thresholds ensure autovacuum runs frequently on large tables,
--    which keeps the GIN pending list small and prevents flush spikes.
ALTER TABLE feat.indicator_snapshot SET (
    autovacuum_vacuum_scale_factor   = 0.01,
    autovacuum_analyze_scale_factor  = 0.005,
    autovacuum_vacuum_threshold      = 1000,
    autovacuum_analyze_threshold     = 1000
);

ALTER TABLE feat.indicator_level_value SET (
    autovacuum_vacuum_scale_factor   = 0.01,
    autovacuum_analyze_scale_factor  = 0.005,
    autovacuum_vacuum_threshold      = 1000,
    autovacuum_analyze_threshold     = 1000
);

-- 3.1) Remove payload_json GIN index from write-hot snapshot table.
--      In production this index showed 0 scans while causing heavy random
--      reads and WAL pressure during INSERT. Dropping it preserves data
--      correctness and significantly reduces write latency.
--      Note: on Timescale hypertables, CONCURRENTLY is not supported when
--      dropping the parent index; use plain DROP INDEX.
DROP INDEX IF EXISTS feat.idx_indicator_snapshot_payload_gin;

-- Apply same settings to existing chunks directly
DO $$
DECLARE
    chunk_relid regclass;
BEGIN
    FOR chunk_relid IN
        SELECT format('%I.%I', chunk_schema, chunk_name)::regclass
        FROM timescaledb_information.chunks
        WHERE hypertable_schema = 'feat'
          AND hypertable_name IN ('indicator_snapshot', 'indicator_level_value')
    LOOP
        EXECUTE format(
            'ALTER TABLE %s SET (
                autovacuum_vacuum_scale_factor  = 0.01,
                autovacuum_analyze_scale_factor = 0.005,
                autovacuum_vacuum_threshold     = 1000,
                autovacuum_analyze_threshold    = 1000
            )',
            chunk_relid
        );
    END LOOP;
END
$$;

-- 4) Re-analyze after index changes
ANALYZE md.agg_trade_1m;
ANALYZE md.agg_orderbook_1m;
ANALYZE md.agg_liq_1m;
ANALYZE md.agg_funding_mark_1m;
ANALYZE feat.indicator_snapshot;
ANALYZE feat.indicator_level_value;

-- 5) Optional maintenance to immediately clear existing pending-list pressure.
-- VACUUM (ANALYZE) feat.indicator_snapshot;
