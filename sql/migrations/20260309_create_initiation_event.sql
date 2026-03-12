CREATE TABLE IF NOT EXISTS evt.initiation_event (
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
    pivot_price                DOUBLE PRECISION,
    break_mag_ticks            DOUBLE PRECISION,
    z_delta                    DOUBLE PRECISION,
    rdelta_mean                DOUBLE PRECISION,
    min_follow_required_minutes INTEGER,
    follow_through_minutes     INTEGER,
    follow_through_end_ts      TIMESTAMPTZ,
    follow_through_delta_sum   DOUBLE PRECISION,
    follow_through_hold_ok     BOOLEAN,
    follow_through_max_adverse_excursion_ticks DOUBLE PRECISION,
    spot_break_confirm         BOOLEAN,
    spot_rdelta_1m_mean        DOUBLE PRECISION,
    spot_cvd_change            DOUBLE PRECISION,
    spot_whale_break_confirm   BOOLEAN,
    score                      DOUBLE PRECISION,
    confidence                 DOUBLE PRECISION,
    window_code                TEXT,
    param_set_id               UUID,
    calc_version               TEXT,
    payload_json               JSONB NOT NULL DEFAULT '{}'::jsonb
);

ALTER TABLE IF EXISTS evt.initiation_event
    ADD COLUMN IF NOT EXISTS min_follow_required_minutes INTEGER,
    ADD COLUMN IF NOT EXISTS follow_through_delta_sum DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS follow_through_hold_ok BOOLEAN,
    ADD COLUMN IF NOT EXISTS follow_through_max_adverse_excursion_ticks DOUBLE PRECISION;

SELECT create_hypertable('evt.initiation_event', 'ts_event_start', if_not_exists => TRUE);

CREATE UNIQUE INDEX IF NOT EXISTS uq_initiation_event_event_id
ON evt.initiation_event(symbol, event_id, ts_event_start);

CREATE INDEX IF NOT EXISTS idx_initiation_event_lookup
ON evt.initiation_event(symbol, event_type, ts_event_start DESC);

CREATE INDEX IF NOT EXISTS idx_initiation_event_available_ts
ON evt.initiation_event(symbol, event_available_ts DESC);
