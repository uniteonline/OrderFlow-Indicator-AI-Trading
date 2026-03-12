CREATE TABLE IF NOT EXISTS evt.absorption_event (
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
    trigger_side               TEXT,
    pivot_price                DOUBLE PRECISION,
    price_low                  DOUBLE PRECISION,
    price_high                 DOUBLE PRECISION,
    delta_sum                  DOUBLE PRECISION,
    rdelta_mean                DOUBLE PRECISION,
    reject_ratio               DOUBLE PRECISION,
    key_distance_ticks         DOUBLE PRECISION,
    stacked_buy_imbalance      BOOLEAN,
    stacked_sell_imbalance     BOOLEAN,
    spot_rdelta_1m_mean        DOUBLE PRECISION,
    spot_cvd_1m_change         DOUBLE PRECISION,
    spot_flow_confirm_score    DOUBLE PRECISION,
    spot_whale_confirm_score   DOUBLE PRECISION,
    spot_confirm               BOOLEAN,
    score_base                 DOUBLE PRECISION,
    score                      DOUBLE PRECISION,
    confidence                 DOUBLE PRECISION,
    window_code                TEXT,
    param_set_id               UUID,
    calc_version               TEXT,
    payload_json               JSONB NOT NULL DEFAULT '{}'::jsonb
);

ALTER TABLE IF EXISTS evt.absorption_event
    ADD COLUMN IF NOT EXISTS trigger_side TEXT;

SELECT create_hypertable('evt.absorption_event', 'ts_event_start', if_not_exists => TRUE);

CREATE UNIQUE INDEX IF NOT EXISTS uq_absorption_event_event_id
ON evt.absorption_event(symbol, event_id, ts_event_start);

CREATE INDEX IF NOT EXISTS idx_absorption_event_lookup
ON evt.absorption_event(symbol, event_type, ts_event_start DESC);

CREATE INDEX IF NOT EXISTS idx_absorption_event_available_ts
ON evt.absorption_event(symbol, event_available_ts DESC);
