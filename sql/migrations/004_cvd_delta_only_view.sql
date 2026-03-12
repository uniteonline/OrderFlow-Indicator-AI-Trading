-- Migration 004: CVD delta-only storage — backward-compatible reconstruction view
--
-- After this change, feat.indicator_snapshot for indicator_code='cvd_pack' no longer
-- stores cvd_fut / cvd_spot / cvd_diff_fs / xmk_cvd_gap_s_minus_f at the root level.
-- CVD is computed at query time from the stored delta values, which are always accurate.
--
-- Usage:
--   SELECT * FROM feat.v_cvd_reconstructed WHERE symbol='ETHUSDT' ORDER BY ts_snapshot DESC LIMIT 10;

CREATE OR REPLACE VIEW feat.v_cvd_reconstructed AS
SELECT
    ts_snapshot,
    symbol,
    window_code,
    (payload_json->>'delta_fut')::double precision  AS delta_fut,
    (payload_json->>'delta_spot')::double precision AS delta_spot,
    SUM((payload_json->>'delta_fut')::double precision) OVER (
        PARTITION BY symbol, window_code
        ORDER BY ts_snapshot
        ROWS UNBOUNDED PRECEDING
    ) AS cvd_fut,
    SUM((payload_json->>'delta_spot')::double precision) OVER (
        PARTITION BY symbol, window_code
        ORDER BY ts_snapshot
        ROWS UNBOUNDED PRECEDING
    ) AS cvd_spot,
    -- Derived: cross-market gap (spot CVD minus futures CVD)
    SUM((payload_json->>'delta_spot')::double precision) OVER (
        PARTITION BY symbol, window_code
        ORDER BY ts_snapshot
        ROWS UNBOUNDED PRECEDING
    )
    -
    SUM((payload_json->>'delta_fut')::double precision) OVER (
        PARTITION BY symbol, window_code
        ORDER BY ts_snapshot
        ROWS UNBOUNDED PRECEDING
    ) AS cvd_diff_spot_minus_fut
FROM feat.indicator_snapshot
WHERE indicator_code = 'cvd_pack';

-- Rolling 7-day window CVD (mirrors the by_window.series.cvd_7d_fut logic in the engine)
CREATE OR REPLACE VIEW feat.v_cvd_7d_rolling AS
SELECT
    ts_snapshot,
    symbol,
    (payload_json->>'delta_fut')::double precision  AS delta_fut,
    (payload_json->>'delta_spot')::double precision AS delta_spot,
    SUM((payload_json->>'delta_fut')::double precision) OVER (
        PARTITION BY symbol
        ORDER BY ts_snapshot
        ROWS BETWEEN 10079 PRECEDING AND CURRENT ROW  -- 7 days * 1440 min/day - 1
    ) AS cvd_7d_fut,
    SUM((payload_json->>'delta_spot')::double precision) OVER (
        PARTITION BY symbol
        ORDER BY ts_snapshot
        ROWS BETWEEN 10079 PRECEDING AND CURRENT ROW
    ) AS cvd_7d_spot
FROM feat.indicator_snapshot
WHERE indicator_code = 'cvd_pack';
