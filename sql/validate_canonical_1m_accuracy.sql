-- Validate the storage invariants required to preserve docs/指标.md #1 accuracy.
--
-- Coverage by source table:
--   md.agg_trade_1m         -> price_volume_structure, footprint, divergence,
--                              cvd_pack, whale_trades, vpin, avwap,
--                              volume/price derived indicators
--   md.agg_orderbook_1m     -> orderbook_depth
--   md.agg_liq_1m           -> liquidation-driven indicators
--   md.agg_funding_mark_1m  -> funding / mark derived indicators
--   md.kline_bar            -> kline_history, plus kline-backed supplements
--
-- This script does not rewrite data. It shows the current write-amplification
-- and the minute keys that a canonical 1m table must preserve exactly.
--
-- Usage:
--   sudo -u postgres psql -d orderflow -v symbol="'ETHUSDT'" -f sql/validate_canonical_1m_accuracy.sql

\pset tuples_only off
\pset pager off

WITH params AS (
    SELECT COALESCE(NULLIF(:symbol, ''), 'ETHUSDT')::text AS symbol
)
SELECT
    'agg_trade_1m' AS table_name,
    count(*) AS physical_rows,
    count(DISTINCT (t.market, t.symbol, t.stream_name, t.ts_bucket)) AS canonical_minute_rows,
    count(*) - count(DISTINCT (t.market, t.symbol, t.stream_name, t.ts_bucket)) AS duplicate_rows,
    round(
        count(*)::numeric
        / NULLIF(count(DISTINCT (t.market, t.symbol, t.stream_name, t.ts_bucket)), 0),
        2
    ) AS write_amplification
FROM md.agg_trade_1m t
JOIN params p ON t.symbol = p.symbol

UNION ALL

SELECT
    'agg_orderbook_1m' AS table_name,
    count(*) AS physical_rows,
    count(DISTINCT (o.market, o.symbol, o.stream_name, o.ts_bucket)) AS canonical_minute_rows,
    count(*) - count(DISTINCT (o.market, o.symbol, o.stream_name, o.ts_bucket)) AS duplicate_rows,
    round(
        count(*)::numeric
        / NULLIF(count(DISTINCT (o.market, o.symbol, o.stream_name, o.ts_bucket)), 0),
        2
    ) AS write_amplification
FROM md.agg_orderbook_1m o
JOIN params p ON o.symbol = p.symbol

UNION ALL

SELECT
    'agg_liq_1m' AS table_name,
    count(*) AS physical_rows,
    count(DISTINCT (l.market, l.symbol, l.stream_name, l.ts_bucket)) AS canonical_minute_rows,
    count(*) - count(DISTINCT (l.market, l.symbol, l.stream_name, l.ts_bucket)) AS duplicate_rows,
    round(
        count(*)::numeric
        / NULLIF(count(DISTINCT (l.market, l.symbol, l.stream_name, l.ts_bucket)), 0),
        2
    ) AS write_amplification
FROM md.agg_liq_1m l
JOIN params p ON l.symbol = p.symbol

UNION ALL

SELECT
    'agg_funding_mark_1m' AS table_name,
    count(*) AS physical_rows,
    count(DISTINCT (f.market, f.symbol, f.stream_name, f.ts_bucket)) AS canonical_minute_rows,
    count(*) - count(DISTINCT (f.market, f.symbol, f.stream_name, f.ts_bucket)) AS duplicate_rows,
    round(
        count(*)::numeric
        / NULLIF(count(DISTINCT (f.market, f.symbol, f.stream_name, f.ts_bucket)), 0),
        2
    ) AS write_amplification
FROM md.agg_funding_mark_1m f
JOIN params p ON f.symbol = p.symbol
ORDER BY table_name;

WITH params AS (
    SELECT COALESCE(NULLIF(:symbol, ''), 'ETHUSDT')::text AS symbol
)
SELECT
    'trade_duplicate_minutes' AS check_name,
    t.market,
    t.symbol,
    t.stream_name,
    t.ts_bucket,
    count(*) AS row_count,
    sum(trade_count) AS merged_trade_count,
    sum(buy_qty) AS merged_buy_qty,
    sum(sell_qty) AS merged_sell_qty
FROM md.agg_trade_1m t
JOIN params p ON t.symbol = p.symbol
GROUP BY t.market, t.symbol, t.stream_name, t.ts_bucket
HAVING count(*) > 1
ORDER BY row_count DESC, t.ts_bucket DESC
LIMIT 20;

WITH params AS (
    SELECT COALESCE(NULLIF(:symbol, ''), 'ETHUSDT')::text AS symbol
)
SELECT
    'orderbook_duplicate_minutes' AS check_name,
    o.market,
    o.symbol,
    o.stream_name,
    o.ts_bucket,
    count(*) AS row_count,
    sum(sample_count) AS merged_sample_count,
    sum(bbo_updates) AS merged_bbo_updates
FROM md.agg_orderbook_1m o
JOIN params p ON o.symbol = p.symbol
GROUP BY o.market, o.symbol, o.stream_name, o.ts_bucket
HAVING count(*) > 1
ORDER BY row_count DESC, o.ts_bucket DESC
LIMIT 20;

WITH params AS (
    SELECT COALESCE(NULLIF(:symbol, ''), 'ETHUSDT')::text AS symbol
)
SELECT
    'indicator_snapshot_storage' AS check_name,
    indicator_code,
    count(*) AS rows,
    pg_size_pretty(sum(pg_column_size(payload_json))::bigint) AS payload_size,
    round(avg(pg_column_size(payload_json))::numeric, 1) AS avg_payload_bytes
FROM feat.indicator_snapshot s
JOIN params p ON s.symbol = p.symbol
GROUP BY indicator_code
ORDER BY sum(pg_column_size(payload_json)) DESC NULLS LAST;
