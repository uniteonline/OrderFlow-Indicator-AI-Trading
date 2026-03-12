CREATE TABLE IF NOT EXISTS evt.exhaustion_event (
    event_id                   TEXT NOT NULL,
    ts_event_start             TIMESTAMPTZ NOT NULL,
    ts_event_end               TIMESTAMPTZ NOT NULL,
    confirm_ts                 TIMESTAMPTZ NOT NULL,
    event_available_ts         TIMESTAMPTZ NOT NULL,
    ts_recorded                TIMESTAMPTZ NOT NULL DEFAULT now(),
    venue                      TEXT NOT NULL DEFAULT 'binance',
    symbol                     TEXT NOT NULL,
    event_type                 TEXT NOT NULL,
    direction                  SMALLINT NOT NULL,
    pivot_ts_1                 TIMESTAMPTZ,
    pivot_ts_2                 TIMESTAMPTZ,
    pivot_confirm_ts_1         TIMESTAMPTZ,
    pivot_confirm_ts_2         TIMESTAMPTZ,
    price_push_ticks           DOUBLE PRECISION,
    delta_change               DOUBLE PRECISION,
    rdelta_change              DOUBLE PRECISION,
    reject_ratio               DOUBLE PRECISION,
    confirm_speed              DOUBLE PRECISION,
    spot_cvd_push_post_pivot   DOUBLE PRECISION,
    spot_whale_push_post_pivot DOUBLE PRECISION,
    spot_continuation_risk     BOOLEAN,
    spot_exhaustion_confirm    BOOLEAN,
    score                      DOUBLE PRECISION,
    confidence                 DOUBLE PRECISION,
    window_code                TEXT,
    param_set_id               UUID,
    calc_version               TEXT,
    payload_json               JSONB NOT NULL DEFAULT '{}'::jsonb
);

SELECT create_hypertable('evt.exhaustion_event', 'ts_event_start', if_not_exists => TRUE);

CREATE UNIQUE INDEX IF NOT EXISTS uq_exhaustion_event_event_id
ON evt.exhaustion_event(symbol, event_id, ts_event_start);

CREATE INDEX IF NOT EXISTS idx_exhaustion_event_lookup
ON evt.exhaustion_event(symbol, event_type, ts_event_start DESC);

CREATE INDEX IF NOT EXISTS idx_exhaustion_event_available_ts
ON evt.exhaustion_event(symbol, event_available_ts DESC);
