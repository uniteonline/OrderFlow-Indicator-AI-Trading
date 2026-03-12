-- Compact historical non-trade 1m aggregate tables into canonical minute rows.
-- Safe for docs/指标.md #1 accuracy because it only rewrites:
--   md.agg_orderbook_1m
--   md.agg_liq_1m
--   md.agg_funding_mark_1m
-- and preserves the exact minute-level statistics / level payloads that
-- indicator_engine replays from those tables.
--
-- Trade rows are intentionally excluded here because historical md.agg_trade_1m
-- still lacks vpin_state for most minutes and must be rebuilt from raw, not DB-merged.
--
-- Usage:
--   sudo -u postgres psql -d orderflow \
--     -v symbol="'ETHUSDT'" \
--     -v market="'all'" \
--     -v from_ts="'2026-03-05 02:52:00+00'" \
--     -v to_ts="'2026-03-06 04:26:00+00'" \
--     -f sql/compact_historical_non_trade_1m.sql

\pset pager off
\set ON_ERROR_STOP on

\echo == Precheck ==
WITH params AS (
    SELECT
        COALESCE(NULLIF(:symbol, ''), 'ETHUSDT')::text AS symbol,
        COALESCE(NULLIF(:market, ''), 'all')::text AS market,
        :from_ts::timestamptz AS from_ts,
        :to_ts::timestamptz AS to_ts
)
SELECT
    'agg_orderbook_1m' AS table_name,
    count(*) AS physical_rows,
    count(DISTINCT (o.market, o.symbol, o.stream_name, o.ts_bucket)) AS canonical_rows
FROM md.agg_orderbook_1m o
JOIN params p ON o.symbol = p.symbol
WHERE o.ts_event >= p.from_ts
  AND o.ts_event < p.to_ts
  AND (p.market = 'all' OR o.market::text = p.market)
UNION ALL
SELECT
    'agg_liq_1m',
    count(*),
    count(DISTINCT (l.market, l.symbol, l.stream_name, l.ts_bucket))
FROM md.agg_liq_1m l
JOIN params p ON l.symbol = p.symbol
WHERE l.ts_event >= p.from_ts
  AND l.ts_event < p.to_ts
  AND (p.market = 'all' OR l.market::text = p.market)
UNION ALL
SELECT
    'agg_funding_mark_1m',
    count(*),
    count(DISTINCT (f.market, f.symbol, f.stream_name, f.ts_bucket))
FROM md.agg_funding_mark_1m f
JOIN params p ON f.symbol = p.symbol
WHERE f.ts_event >= p.from_ts
  AND f.ts_event < p.to_ts
  AND (p.market = 'all' OR f.market::text = p.market)
ORDER BY table_name;

\echo == Compact md.agg_orderbook_1m ==
BEGIN;
CREATE TEMP TABLE tmp_compact_orderbook_1m ON COMMIT DROP AS
WITH params AS (
    SELECT
        COALESCE(NULLIF(:symbol, ''), 'ETHUSDT')::text AS symbol,
        COALESCE(NULLIF(:market, ''), 'all')::text AS market,
        :from_ts::timestamptz AS from_ts,
        :to_ts::timestamptz AS to_ts
), base AS (
    SELECT
        o.market,
        o.symbol,
        o.stream_name,
        o.ts_bucket,
        min(o.chunk_start_ts) AS source_start_ts,
        max(o.chunk_end_ts) AS source_end_ts,
        CASE
            WHEN bool_or(o.source_event_count IS NOT NULL) THEN sum(COALESCE(o.source_event_count, 0))
            ELSE NULL
        END AS source_event_count,
        sum(o.sample_count) AS sample_count,
        sum(o.bbo_updates) AS bbo_updates,
        sum(o.spread_sum) AS spread_sum,
        sum(o.topk_depth_sum) AS topk_depth_sum,
        sum(o.obi_sum) AS obi_sum,
        sum(o.obi_l1_sum) AS obi_l1_sum,
        sum(o.obi_k_sum) AS obi_k_sum,
        sum(o.obi_k_dw_sum) AS obi_k_dw_sum,
        sum(o.obi_k_dw_change_sum) AS obi_k_dw_change_sum,
        sum(o.obi_k_dw_adj_sum) AS obi_k_dw_adj_sum,
        sum(o.microprice_sum) AS microprice_sum,
        sum(o.microprice_classic_sum) AS microprice_classic_sum,
        sum(o.microprice_kappa_sum) AS microprice_kappa_sum,
        sum(o.microprice_adj_sum) AS microprice_adj_sum,
        sum(o.ofi_sum) AS ofi_sum,
        (ARRAY_AGG(o.obi_k_dw_close ORDER BY o.chunk_start_ts DESC, o.chunk_end_ts DESC)
            FILTER (WHERE o.obi_k_dw_close IS NOT NULL))[1] AS obi_k_dw_close
    FROM md.agg_orderbook_1m o
    JOIN params p ON o.symbol = p.symbol
    WHERE o.ts_event >= p.from_ts
      AND o.ts_event < p.to_ts
      AND (p.market = 'all' OR o.market::text = p.market)
    GROUP BY o.market, o.symbol, o.stream_name, o.ts_bucket
), heatmap AS (
    SELECT
        x.market,
        x.symbol,
        x.stream_name,
        x.ts_bucket,
        jsonb_agg(
            jsonb_build_array(
                (x.price_tick::double precision / 100.0),
                x.bid_liquidity,
                x.ask_liquidity
            )
            ORDER BY x.price_tick
        ) AS heatmap_levels
    FROM (
        SELECT
            o.market,
            o.symbol,
            o.stream_name,
            o.ts_bucket,
            round(((lvl->>0)::double precision) * 100.0)::bigint AS price_tick,
            sum((lvl->>1)::double precision) AS bid_liquidity,
            sum((lvl->>2)::double precision) AS ask_liquidity
        FROM md.agg_orderbook_1m o
        JOIN params p ON o.symbol = p.symbol
        CROSS JOIN LATERAL jsonb_array_elements(o.heatmap_levels) AS lvl
        WHERE o.ts_event >= p.from_ts
          AND o.ts_event < p.to_ts
          AND (p.market = 'all' OR o.market::text = p.market)
        GROUP BY o.market, o.symbol, o.stream_name, o.ts_bucket, round(((lvl->>0)::double precision) * 100.0)::bigint
    ) x
    GROUP BY x.market, x.symbol, x.stream_name, x.ts_bucket
)
SELECT
    b.ts_bucket + interval '1 minute' - interval '1 millisecond' AS ts_event,
    b.market,
    b.symbol,
    'derived'::cfg.source_type AS source_kind,
    'agg.orderbook.1m'::text AS stream_name,
    b.ts_bucket,
    b.ts_bucket AS chunk_start_ts,
    b.ts_bucket + interval '1 minute' - interval '1 millisecond' AS chunk_end_ts,
    b.source_event_count,
    b.sample_count,
    b.bbo_updates,
    b.spread_sum,
    b.topk_depth_sum,
    b.obi_sum,
    b.obi_l1_sum,
    b.obi_k_sum,
    b.obi_k_dw_sum,
    b.obi_k_dw_change_sum,
    b.obi_k_dw_adj_sum,
    b.microprice_sum,
    b.microprice_classic_sum,
    b.microprice_kappa_sum,
    b.microprice_adj_sum,
    b.ofi_sum,
    b.obi_k_dw_close,
    COALESCE(h.heatmap_levels, '[]'::jsonb) AS heatmap_levels,
    jsonb_build_object(
        'agg_mode', 'canonical_1m',
        'source_start_ts', b.source_start_ts,
        'source_end_ts', b.source_end_ts
    ) AS payload_json
FROM base b
LEFT JOIN heatmap h
  ON h.market = b.market
 AND h.symbol = b.symbol
 AND h.stream_name = b.stream_name
 AND h.ts_bucket = b.ts_bucket;

INSERT INTO md.agg_orderbook_1m (
    ts_event, ts_bucket, market, symbol, source_kind, stream_name,
    chunk_start_ts, chunk_end_ts, source_event_count,
    sample_count, bbo_updates,
    spread_sum, topk_depth_sum, obi_sum, obi_l1_sum, obi_k_sum,
    obi_k_dw_sum, obi_k_dw_change_sum, obi_k_dw_adj_sum,
    microprice_sum, microprice_classic_sum, microprice_kappa_sum, microprice_adj_sum,
    ofi_sum, obi_k_dw_close, heatmap_levels, payload_json
)
SELECT
    ts_event, ts_bucket, market, symbol, source_kind, stream_name,
    chunk_start_ts, chunk_end_ts, source_event_count,
    sample_count, bbo_updates,
    spread_sum, topk_depth_sum, obi_sum, obi_l1_sum, obi_k_sum,
    obi_k_dw_sum, obi_k_dw_change_sum, obi_k_dw_adj_sum,
    microprice_sum, microprice_classic_sum, microprice_kappa_sum, microprice_adj_sum,
    ofi_sum, obi_k_dw_close, heatmap_levels, payload_json
FROM tmp_compact_orderbook_1m
ON CONFLICT (market, symbol, stream_name, ts_bucket, chunk_start_ts, chunk_end_ts)
DO UPDATE SET
    ts_event = EXCLUDED.ts_event,
    source_event_count = EXCLUDED.source_event_count,
    sample_count = EXCLUDED.sample_count,
    bbo_updates = EXCLUDED.bbo_updates,
    spread_sum = EXCLUDED.spread_sum,
    topk_depth_sum = EXCLUDED.topk_depth_sum,
    obi_sum = EXCLUDED.obi_sum,
    obi_l1_sum = EXCLUDED.obi_l1_sum,
    obi_k_sum = EXCLUDED.obi_k_sum,
    obi_k_dw_sum = EXCLUDED.obi_k_dw_sum,
    obi_k_dw_change_sum = EXCLUDED.obi_k_dw_change_sum,
    obi_k_dw_adj_sum = EXCLUDED.obi_k_dw_adj_sum,
    microprice_sum = EXCLUDED.microprice_sum,
    microprice_classic_sum = EXCLUDED.microprice_classic_sum,
    microprice_kappa_sum = EXCLUDED.microprice_kappa_sum,
    microprice_adj_sum = EXCLUDED.microprice_adj_sum,
    ofi_sum = EXCLUDED.ofi_sum,
    obi_k_dw_close = EXCLUDED.obi_k_dw_close,
    heatmap_levels = EXCLUDED.heatmap_levels,
    payload_json = EXCLUDED.payload_json;

DELETE FROM md.agg_orderbook_1m o
WHERE o.symbol = COALESCE(NULLIF(:symbol, ''), 'ETHUSDT')::text
  AND o.ts_event >= :from_ts::timestamptz
  AND o.ts_event < :to_ts::timestamptz
  AND (
        COALESCE(NULLIF(:market, ''), 'all')::text = 'all'
        OR o.market::text = COALESCE(NULLIF(:market, ''), 'all')::text
  )
  AND NOT (
        o.chunk_start_ts = o.ts_bucket
    AND o.chunk_end_ts = o.ts_bucket + interval '1 minute' - interval '1 millisecond'
  );
COMMIT;

\echo == Compact md.agg_liq_1m ==
BEGIN;
CREATE TEMP TABLE tmp_compact_liq_1m ON COMMIT DROP AS
WITH params AS (
    SELECT
        COALESCE(NULLIF(:symbol, ''), 'ETHUSDT')::text AS symbol,
        COALESCE(NULLIF(:market, ''), 'all')::text AS market,
        :from_ts::timestamptz AS from_ts,
        :to_ts::timestamptz AS to_ts
), base AS (
    SELECT
        l.market,
        l.symbol,
        l.stream_name,
        l.ts_bucket,
        min(l.chunk_start_ts) AS source_start_ts,
        max(l.chunk_end_ts) AS source_end_ts,
        CASE
            WHEN bool_or(l.source_event_count IS NOT NULL) THEN sum(COALESCE(l.source_event_count, 0))
            ELSE NULL
        END AS source_event_count
    FROM md.agg_liq_1m l
    JOIN params p ON l.symbol = p.symbol
    WHERE l.ts_event >= p.from_ts
      AND l.ts_event < p.to_ts
      AND (p.market = 'all' OR l.market::text = p.market)
    GROUP BY l.market, l.symbol, l.stream_name, l.ts_bucket
), levels AS (
    SELECT
        x.market,
        x.symbol,
        x.stream_name,
        x.ts_bucket,
        jsonb_agg(
            jsonb_build_array(
                (x.price_tick::double precision / 100.0),
                x.long_liq,
                x.short_liq
            )
            ORDER BY x.price_tick
        ) AS force_liq_levels
    FROM (
        SELECT
            l.market,
            l.symbol,
            l.stream_name,
            l.ts_bucket,
            round(((lvl->>0)::double precision) * 100.0)::bigint AS price_tick,
            sum((lvl->>1)::double precision) AS long_liq,
            sum((lvl->>2)::double precision) AS short_liq
        FROM md.agg_liq_1m l
        JOIN params p ON l.symbol = p.symbol
        CROSS JOIN LATERAL jsonb_array_elements(l.force_liq_levels) AS lvl
        WHERE l.ts_event >= p.from_ts
          AND l.ts_event < p.to_ts
          AND (p.market = 'all' OR l.market::text = p.market)
        GROUP BY l.market, l.symbol, l.stream_name, l.ts_bucket, round(((lvl->>0)::double precision) * 100.0)::bigint
    ) x
    GROUP BY x.market, x.symbol, x.stream_name, x.ts_bucket
)
SELECT
    b.ts_bucket + interval '1 minute' - interval '1 millisecond' AS ts_event,
    b.market,
    b.symbol,
    'derived'::cfg.source_type AS source_kind,
    'agg.liq.1m'::text AS stream_name,
    b.ts_bucket,
    b.ts_bucket AS chunk_start_ts,
    b.ts_bucket + interval '1 minute' - interval '1 millisecond' AS chunk_end_ts,
    b.source_event_count,
    COALESCE(levels.force_liq_levels, '[]'::jsonb) AS force_liq_levels,
    jsonb_build_object(
        'agg_mode', 'canonical_1m',
        'source_start_ts', b.source_start_ts,
        'source_end_ts', b.source_end_ts
    ) AS payload_json
FROM base b
LEFT JOIN levels
  ON levels.market = b.market
 AND levels.symbol = b.symbol
 AND levels.stream_name = b.stream_name
 AND levels.ts_bucket = b.ts_bucket;

INSERT INTO md.agg_liq_1m (
    ts_event, ts_bucket, market, symbol, source_kind, stream_name,
    chunk_start_ts, chunk_end_ts, source_event_count,
    force_liq_levels, payload_json
)
SELECT
    ts_event, ts_bucket, market, symbol, source_kind, stream_name,
    chunk_start_ts, chunk_end_ts, source_event_count,
    force_liq_levels, payload_json
FROM tmp_compact_liq_1m
ON CONFLICT (market, symbol, stream_name, ts_bucket, chunk_start_ts, chunk_end_ts)
DO UPDATE SET
    ts_event = EXCLUDED.ts_event,
    source_event_count = EXCLUDED.source_event_count,
    force_liq_levels = EXCLUDED.force_liq_levels,
    payload_json = EXCLUDED.payload_json;

DELETE FROM md.agg_liq_1m l
WHERE l.symbol = COALESCE(NULLIF(:symbol, ''), 'ETHUSDT')::text
  AND l.ts_event >= :from_ts::timestamptz
  AND l.ts_event < :to_ts::timestamptz
  AND (
        COALESCE(NULLIF(:market, ''), 'all')::text = 'all'
        OR l.market::text = COALESCE(NULLIF(:market, ''), 'all')::text
  )
  AND NOT (
        l.chunk_start_ts = l.ts_bucket
    AND l.chunk_end_ts = l.ts_bucket + interval '1 minute' - interval '1 millisecond'
  );
COMMIT;

\echo == Compact md.agg_funding_mark_1m ==
BEGIN;
CREATE TEMP TABLE tmp_compact_funding_mark_1m ON COMMIT DROP AS
WITH params AS (
    SELECT
        COALESCE(NULLIF(:symbol, ''), 'ETHUSDT')::text AS symbol,
        COALESCE(NULLIF(:market, ''), 'all')::text AS market,
        :from_ts::timestamptz AS from_ts,
        :to_ts::timestamptz AS to_ts
), base AS (
    SELECT
        f.market,
        f.symbol,
        f.stream_name,
        f.ts_bucket,
        min(f.chunk_start_ts) AS source_start_ts,
        max(f.chunk_end_ts) AS source_end_ts,
        CASE
            WHEN bool_or(f.source_event_count IS NOT NULL) THEN sum(COALESCE(f.source_event_count, 0))
            ELSE NULL
        END AS source_event_count
    FROM md.agg_funding_mark_1m f
    JOIN params p ON f.symbol = p.symbol
    WHERE f.ts_event >= p.from_ts
      AND f.ts_event < p.to_ts
      AND (p.market = 'all' OR f.market::text = p.market)
    GROUP BY f.market, f.symbol, f.stream_name, f.ts_bucket
), mark_points AS (
    SELECT
        f.market,
        f.symbol,
        f.stream_name,
        f.ts_bucket,
        jsonb_agg(point ORDER BY (point->>'ts')::timestamptz) AS mark_points
    FROM md.agg_funding_mark_1m f
    JOIN params p ON f.symbol = p.symbol
    CROSS JOIN LATERAL jsonb_array_elements(f.mark_points) AS point
    WHERE f.ts_event >= p.from_ts
      AND f.ts_event < p.to_ts
      AND (p.market = 'all' OR f.market::text = p.market)
    GROUP BY f.market, f.symbol, f.stream_name, f.ts_bucket
), funding_points AS (
    SELECT
        f.market,
        f.symbol,
        f.stream_name,
        f.ts_bucket,
        jsonb_agg(point ORDER BY (point->>'ts')::timestamptz) AS funding_points
    FROM md.agg_funding_mark_1m f
    JOIN params p ON f.symbol = p.symbol
    CROSS JOIN LATERAL jsonb_array_elements(f.funding_points) AS point
    WHERE f.ts_event >= p.from_ts
      AND f.ts_event < p.to_ts
      AND (p.market = 'all' OR f.market::text = p.market)
    GROUP BY f.market, f.symbol, f.stream_name, f.ts_bucket
)
SELECT
    b.ts_bucket + interval '1 minute' - interval '1 millisecond' AS ts_event,
    b.market,
    b.symbol,
    'derived'::cfg.source_type AS source_kind,
    'agg.funding_mark.1m'::text AS stream_name,
    b.ts_bucket,
    b.ts_bucket AS chunk_start_ts,
    b.ts_bucket + interval '1 minute' - interval '1 millisecond' AS chunk_end_ts,
    b.source_event_count,
    COALESCE(mp.mark_points, '[]'::jsonb) AS mark_points,
    COALESCE(fp.funding_points, '[]'::jsonb) AS funding_points,
    jsonb_build_object(
        'agg_mode', 'canonical_1m',
        'source_start_ts', b.source_start_ts,
        'source_end_ts', b.source_end_ts
    ) AS payload_json
FROM base b
LEFT JOIN mark_points mp
  ON mp.market = b.market
 AND mp.symbol = b.symbol
 AND mp.stream_name = b.stream_name
 AND mp.ts_bucket = b.ts_bucket
LEFT JOIN funding_points fp
  ON fp.market = b.market
 AND fp.symbol = b.symbol
 AND fp.stream_name = b.stream_name
 AND fp.ts_bucket = b.ts_bucket;

INSERT INTO md.agg_funding_mark_1m (
    ts_event, ts_bucket, market, symbol, source_kind, stream_name,
    chunk_start_ts, chunk_end_ts, source_event_count,
    mark_points, funding_points, payload_json
)
SELECT
    ts_event, ts_bucket, market, symbol, source_kind, stream_name,
    chunk_start_ts, chunk_end_ts, source_event_count,
    mark_points, funding_points, payload_json
FROM tmp_compact_funding_mark_1m
ON CONFLICT (market, symbol, stream_name, ts_bucket, chunk_start_ts, chunk_end_ts)
DO UPDATE SET
    ts_event = EXCLUDED.ts_event,
    source_event_count = EXCLUDED.source_event_count,
    mark_points = EXCLUDED.mark_points,
    funding_points = EXCLUDED.funding_points,
    payload_json = EXCLUDED.payload_json;

DELETE FROM md.agg_funding_mark_1m f
WHERE f.symbol = COALESCE(NULLIF(:symbol, ''), 'ETHUSDT')::text
  AND f.ts_event >= :from_ts::timestamptz
  AND f.ts_event < :to_ts::timestamptz
  AND (
        COALESCE(NULLIF(:market, ''), 'all')::text = 'all'
        OR f.market::text = COALESCE(NULLIF(:market, ''), 'all')::text
  )
  AND NOT (
        f.chunk_start_ts = f.ts_bucket
    AND f.chunk_end_ts = f.ts_bucket + interval '1 minute' - interval '1 millisecond'
  );
COMMIT;

\echo == Postcheck ==
WITH params AS (
    SELECT
        COALESCE(NULLIF(:symbol, ''), 'ETHUSDT')::text AS symbol,
        COALESCE(NULLIF(:market, ''), 'all')::text AS market,
        :from_ts::timestamptz AS from_ts,
        :to_ts::timestamptz AS to_ts
)
SELECT
    'agg_orderbook_1m' AS table_name,
    count(*) AS physical_rows,
    count(DISTINCT (o.market, o.symbol, o.stream_name, o.ts_bucket)) AS canonical_rows
FROM md.agg_orderbook_1m o
JOIN params p ON o.symbol = p.symbol
WHERE o.ts_event >= p.from_ts
  AND o.ts_event < p.to_ts
  AND (p.market = 'all' OR o.market::text = p.market)
UNION ALL
SELECT
    'agg_liq_1m',
    count(*),
    count(DISTINCT (l.market, l.symbol, l.stream_name, l.ts_bucket))
FROM md.agg_liq_1m l
JOIN params p ON l.symbol = p.symbol
WHERE l.ts_event >= p.from_ts
  AND l.ts_event < p.to_ts
  AND (p.market = 'all' OR l.market::text = p.market)
UNION ALL
SELECT
    'agg_funding_mark_1m',
    count(*),
    count(DISTINCT (f.market, f.symbol, f.stream_name, f.ts_bucket))
FROM md.agg_funding_mark_1m f
JOIN params p ON f.symbol = p.symbol
WHERE f.ts_event >= p.from_ts
  AND f.ts_event < p.to_ts
  AND (p.market = 'all' OR f.market::text = p.market)
ORDER BY table_name;
