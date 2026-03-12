-- Startup backfill query optimization indexes for indicator_engine.
-- Apply in production during low traffic:
--   psql -h 127.0.0.1 -U postgres -d orderflow -f sql/optimize_indicator_backfill_indexes.sql
--
-- Notes:
-- 1) CONCURRENTLY avoids long ACCESS EXCLUSIVE table locks.
-- 2) Must run outside an explicit transaction block.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_agg_trade_1m_backfill
ON md.agg_trade_1m(symbol, ts_event, market);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_agg_orderbook_1m_backfill
ON md.agg_orderbook_1m(symbol, ts_event, market);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_agg_liq_1m_backfill
ON md.agg_liq_1m(symbol, ts_event, market);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_agg_funding_mark_1m_backfill
ON md.agg_funding_mark_1m(symbol, ts_event, market);

ANALYZE md.agg_trade_1m;
ANALYZE md.agg_orderbook_1m;
ANALYZE md.agg_liq_1m;
ANALYZE md.agg_funding_mark_1m;
