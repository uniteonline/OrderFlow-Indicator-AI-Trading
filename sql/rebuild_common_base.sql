-- =============================================================
-- Shared rebuild baseline for orderflow / orderflow_ops
-- Execute via sql/rebuild_orderflow.sql or sql/rebuild_orderflow_ops.sql.
-- =============================================================

\if :{?expected_db}
\else
\echo Missing required psql variable: expected_db
DO $$ BEGIN RAISE EXCEPTION 'expected_db variable is required'; END $$;
\endif

\if :{?is_orderflow}
\else
\echo Missing required psql variable: is_orderflow
DO $$ BEGIN RAISE EXCEPTION 'is_orderflow variable is required'; END $$;
\endif

\if :{?outbox_partition_days_ahead}
\else
\set outbox_partition_days_ahead 7
\endif

\if :{?outbox_partition_days_back}
\else
\set outbox_partition_days_back 2
\endif

SELECT current_database() AS current_db \gset
SELECT CASE WHEN current_database() = :'expected_db' THEN 1 ELSE 0 END AS expected_db_ok \gset
\if :expected_db_ok
\else
\echo Refused to rebuild database :current_db. Expected :expected_db.
SELECT 1 / 0;
\endif

DROP SCHEMA IF EXISTS archive CASCADE;
DROP SCHEMA IF EXISTS evt CASCADE;
DROP SCHEMA IF EXISTS feat CASCADE;
DROP SCHEMA IF EXISTS md CASCADE;
DROP SCHEMA IF EXISTS ops CASCADE;
DROP SCHEMA IF EXISTS cfg CASCADE;

BEGIN;

-- 0) Extensions
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS btree_gist;
DO $$
BEGIN
    CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'skip pg_stat_statements extension: %', SQLERRM;
END
$$;

-- 1) Schemas
CREATE SCHEMA IF NOT EXISTS cfg;      -- config / catalog / parameter versions
CREATE SCHEMA IF NOT EXISTS ops;      -- runtime ops / checkpoints / gaps / backfill jobs
CREATE SCHEMA IF NOT EXISTS md;       -- normalized market data (raw-ish event store)
CREATE SCHEMA IF NOT EXISTS feat;     -- indicator features / snapshots / matrices
CREATE SCHEMA IF NOT EXISTS evt;      -- indicator events / signals (divergence, absorption...)
CREATE SCHEMA IF NOT EXISTS archive;  -- parquet object registry / manifests

-- 2) Enum types (idempotent via DO blocks)
DO $$ BEGIN
    CREATE TYPE cfg.market_type AS ENUM ('spot', 'futures');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE cfg.source_type AS ENUM ('ws', 'rest', 'replay', 'derived');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE cfg.stream_kind AS ENUM (
        'agg_trade', 'trade', 'depth_l2', 'book_ticker', 'kline', 'mark_price', 'force_order', 'other'
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE cfg.side_type AS ENUM ('buy', 'sell', 'unknown');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE cfg.event_severity AS ENUM ('debug', 'info', 'warn', 'error', 'critical');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE cfg.gap_status AS ENUM ('open', 'backfilling', 'closed', 'ignored', 'failed');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE cfg.job_status AS ENUM ('queued', 'running', 'success', 'failed', 'cancelled');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE cfg.parquet_dataset_kind AS ENUM ('raw', 'feature', 'event', 'snapshot', 'replay_checkpoint');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- 3) Config / Catalog (small relational tables)
CREATE TABLE IF NOT EXISTS cfg.instrument (
    instrument_id      BIGSERIAL PRIMARY KEY,
    venue              TEXT NOT NULL DEFAULT 'binance',
    symbol             TEXT NOT NULL,                  -- ETHUSDT
    market             cfg.market_type NOT NULL,       -- spot / futures
    contract_type      TEXT,                           -- perpetual for futures
    quote_asset        TEXT NOT NULL DEFAULT 'USDT',
    base_asset         TEXT NOT NULL DEFAULT 'ETH',
    contract_multiplier DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    tick_size          DOUBLE PRECISION,
    lot_size           DOUBLE PRECISION,
    price_scale        INTEGER,
    qty_scale          INTEGER,
    is_active          BOOLEAN NOT NULL DEFAULT TRUE,
    metadata           JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (venue, symbol, market)
);

CREATE TABLE IF NOT EXISTS cfg.ws_stream_catalog (
    stream_id          BIGSERIAL PRIMARY KEY,
    venue              TEXT NOT NULL DEFAULT 'binance',
    market             cfg.market_type NOT NULL,
    symbol             TEXT NOT NULL,
    stream_name        TEXT NOT NULL,     -- e.g. ethusdt@aggTrade
    stream_kind        cfg.stream_kind NOT NULL,
    endpoint_base      TEXT NOT NULL,
    is_required        BOOLEAN NOT NULL DEFAULT TRUE,
    enabled            BOOLEAN NOT NULL DEFAULT TRUE,
    qos_config         JSONB NOT NULL DEFAULT '{}'::jsonb,
    dependency_indicators TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    note               TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (venue, market, symbol, stream_name)
);

CREATE TABLE IF NOT EXISTS cfg.rest_job_catalog (
    rest_job_id        BIGSERIAL PRIMARY KEY,
    venue              TEXT NOT NULL DEFAULT 'binance',
    market             cfg.market_type,
    symbol             TEXT,
    endpoint_path      TEXT NOT NULL,
    purpose            TEXT NOT NULL,     -- init/backfill/reconcile/metadata
    schedule_mode      TEXT NOT NULL,     -- event_triggered/cron/disabled
    cron_expr          TEXT,
    trigger_rules      JSONB NOT NULL DEFAULT '{}'::jsonb,
    dependency_indicators TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    enabled            BOOLEAN NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- expression-based uniqueness added by index below
    CHECK (schedule_mode <> '' )
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_rest_job_catalog_nnd
ON cfg.rest_job_catalog (venue, symbol, market, endpoint_path, purpose) NULLS NOT DISTINCT;

CREATE TABLE IF NOT EXISTS cfg.indicator_catalog (
    indicator_code     TEXT PRIMARY KEY,
    display_name       TEXT NOT NULL,
    category           TEXT NOT NULL,        -- structure/orderbook/flow/event/risk/anchor
    output_kind        TEXT NOT NULL,        -- timeseries/event/matrix/profile/pack
    primary_market     TEXT NOT NULL,        -- futures / mixed / spot
    needs_spot_confirm BOOLEAN NOT NULL DEFAULT FALSE,
    notes              TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS cfg.indicator_param_set (
    param_set_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    indicator_code     TEXT NOT NULL REFERENCES cfg.indicator_catalog(indicator_code),
    version_tag        TEXT NOT NULL,
    is_active          BOOLEAN NOT NULL DEFAULT TRUE,
    description        TEXT,
    params_json        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by         TEXT,
    UNIQUE (indicator_code, version_tag)
);

CREATE TABLE IF NOT EXISTS cfg.runtime_profile (
    runtime_profile_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name               TEXT NOT NULL UNIQUE,
    description        TEXT,
    -- 统一配置字典（endpoint -> 指标依赖、WS/REST 调度、路由等），语言无关
    config_json        JSONB NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 4) Ops / Control Plane
CREATE TABLE IF NOT EXISTS ops.service_heartbeat (
    service_name       TEXT NOT NULL,
    instance_id        TEXT NOT NULL,
    role               TEXT NOT NULL,  -- ingestor / indicator-engine / replay
    ts_heartbeat       TIMESTAMPTZ NOT NULL DEFAULT now(),
    status             TEXT NOT NULL DEFAULT 'ok',
    metrics_json       JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (service_name, instance_id)
);

CREATE TABLE IF NOT EXISTS ops.ws_session_log (
    session_uuid       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    venue              TEXT NOT NULL DEFAULT 'binance',
    market             cfg.market_type NOT NULL,
    symbol             TEXT NOT NULL,
    stream_name        TEXT NOT NULL,
    started_at         TIMESTAMPTZ NOT NULL,
    ended_at           TIMESTAMPTZ,
    disconnect_reason  TEXT,
    reconnect_count    INTEGER NOT NULL DEFAULT 0,
    message_count      BIGINT NOT NULL DEFAULT 0,
    stats_json         JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.ingest_checkpoint (
    checkpoint_id      BIGSERIAL PRIMARY KEY,
    venue              TEXT NOT NULL DEFAULT 'binance',
    market             cfg.market_type NOT NULL,
    symbol             TEXT NOT NULL,
    stream_name        TEXT NOT NULL,
    source_kind        cfg.source_type NOT NULL,
    last_event_ts      TIMESTAMPTZ,
    last_recv_ts       TIMESTAMPTZ,
    last_update_id     BIGINT,
    last_trade_id      BIGINT,
    last_agg_trade_id  BIGINT,
    checkpoint_json    JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (venue, market, symbol, stream_name, source_kind)
);

CREATE TABLE IF NOT EXISTS ops.data_gap (
    gap_id             BIGSERIAL PRIMARY KEY,
    venue              TEXT NOT NULL DEFAULT 'binance',
    market             cfg.market_type NOT NULL,
    symbol             TEXT NOT NULL,
    dataset_name       TEXT NOT NULL, -- md.trade_event / md.depth_delta_l2 / ...
    stream_name        TEXT,
    gap_start_ts       TIMESTAMPTZ NOT NULL,
    gap_end_ts         TIMESTAMPTZ NOT NULL,
    detected_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    status             cfg.gap_status NOT NULL DEFAULT 'open',
    cause              TEXT,
    detail_json        JSONB NOT NULL DEFAULT '{}'::jsonb,
    CHECK (gap_end_ts > gap_start_ts)
);
CREATE INDEX IF NOT EXISTS idx_data_gap_status ON ops.data_gap(status, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_data_gap_lookup ON ops.data_gap(dataset_name, market, symbol, gap_start_ts);
CREATE INDEX IF NOT EXISTS idx_data_gap_dedup_window
ON ops.data_gap (
    venue,
    market,
    symbol,
    dataset_name,
    COALESCE(stream_name, ''),
    COALESCE(cause, ''),
    gap_start_ts,
    gap_end_ts,
    detected_at DESC
);

CREATE TABLE IF NOT EXISTS ops.rest_backfill_job_run (
    job_run_id         BIGSERIAL PRIMARY KEY,
    venue              TEXT NOT NULL DEFAULT 'binance',
    market             cfg.market_type,
    symbol             TEXT,
    endpoint_path      TEXT NOT NULL,
    purpose            TEXT NOT NULL,
    trigger_type       TEXT NOT NULL, -- startup/reconnect/gap/manual/reconcile
    request_params     JSONB NOT NULL DEFAULT '{}'::jsonb,
    requested_start_ts TIMESTAMPTZ,
    requested_end_ts   TIMESTAMPTZ,
    started_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at           TIMESTAMPTZ,
    status             cfg.job_status NOT NULL DEFAULT 'queued',
    http_status        INTEGER,
    rows_written       BIGINT,
    error_message      TEXT,
    result_json        JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_rest_backfill_job_run_status ON ops.rest_backfill_job_run(status, started_at DESC);

-- 4.1 Transactional outbox (reliable DB -> MQ delivery)
CREATE TABLE IF NOT EXISTS ops.outbox_event (
    outbox_id         BIGINT GENERATED BY DEFAULT AS IDENTITY,
    bucket_date       DATE NOT NULL DEFAULT (timezone('UTC', now()))::date,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    available_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at           TIMESTAMPTZ,
    status            TEXT NOT NULL DEFAULT 'pending', -- pending/sending/failed/dead
    retry_count       INTEGER NOT NULL DEFAULT 0,
    exchange_name     TEXT NOT NULL,
    routing_key       TEXT NOT NULL,
    message_id        UUID NOT NULL,
    schema_version    INTEGER NOT NULL,
    headers_json      JSONB NOT NULL DEFAULT '{}'::jsonb,
    payload_json      JSONB NOT NULL,
    error_text        TEXT,
    CONSTRAINT outbox_event_pkey PRIMARY KEY (bucket_date, outbox_id),
    CONSTRAINT outbox_event_retry_count_nonneg_chk
        CHECK (retry_count >= 0),
    CONSTRAINT outbox_event_schema_version_pos_chk
        CHECK (schema_version > 0),
    CONSTRAINT outbox_event_status_chk
        CHECK (status IN ('pending', 'sending', 'failed', 'dead'))
)
PARTITION BY RANGE (bucket_date);

ALTER TABLE ops.outbox_event
    ADD COLUMN IF NOT EXISTS bucket_date DATE NOT NULL DEFAULT (timezone('UTC', now()))::date;
ALTER TABLE ops.outbox_event
    ADD COLUMN IF NOT EXISTS error_text TEXT;

UPDATE ops.outbox_event
SET bucket_date = (created_at AT TIME ZONE 'UTC')::date
WHERE bucket_date IS NULL;

UPDATE ops.outbox_event
SET status = 'dead',
    error_text = COALESCE(error_text, 'migrated from legacy sent status')
WHERE status = 'sent';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'ops'
          AND t.relname = 'outbox_event'
          AND c.conname = 'outbox_event_retry_count_nonneg_chk'
    ) THEN
        ALTER TABLE ops.outbox_event
            ADD CONSTRAINT outbox_event_retry_count_nonneg_chk
            CHECK (retry_count >= 0);
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'ops'
          AND t.relname = 'outbox_event'
          AND c.conname = 'outbox_event_schema_version_pos_chk'
    ) THEN
        ALTER TABLE ops.outbox_event
            ADD CONSTRAINT outbox_event_schema_version_pos_chk
            CHECK (schema_version > 0);
    END IF;
END
$$;

DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT c.conname
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'ops'
          AND t.relname = 'outbox_event'
          AND c.contype = 'c'
          AND pg_get_constraintdef(c.oid) ILIKE '%status%'
    LOOP
        EXECUTE format(
            'ALTER TABLE ops.outbox_event DROP CONSTRAINT IF EXISTS %I',
            constraint_name
        );
    END LOOP;

    ALTER TABLE ops.outbox_event
        ADD CONSTRAINT outbox_event_status_chk
        CHECK (status IN ('pending', 'sending', 'failed', 'dead'));
END
$$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_outbox_bucket_message
ON ops.outbox_event(bucket_date, message_id);
DROP INDEX IF EXISTS ops.idx_outbox_created_at;
DROP INDEX IF EXISTS ops.idx_outbox_sent_at;
DROP INDEX IF EXISTS ops.idx_outbox_pending;
CREATE INDEX IF NOT EXISTS idx_outbox_ready_partial
ON ops.outbox_event (available_at, outbox_id)
WHERE status IN ('pending', 'failed', 'sending');
CREATE INDEX IF NOT EXISTS idx_outbox_ready_exchange
ON ops.outbox_event (exchange_name, available_at, outbox_id)
WHERE status IN ('pending', 'failed', 'sending');
CREATE INDEX IF NOT EXISTS idx_outbox_ready_trade_priority
ON ops.outbox_event (exchange_name, available_at, outbox_id)
WHERE status IN ('pending', 'failed', 'sending')
  AND routing_key LIKE 'md.%.trade.%';
CREATE INDEX IF NOT EXISTS idx_outbox_dead_gc
ON ops.outbox_event (available_at)
WHERE status = 'dead';

-- 4.2 Quarantine bucket for malformed derived agg.orderbook.1m payloads
\if :is_orderflow
CREATE TABLE IF NOT EXISTS ops.bad_agg_orderbook_1m_quarantine (
    ts_event           TIMESTAMPTZ NOT NULL,
    ts_recv            TIMESTAMPTZ NOT NULL DEFAULT now(),
    venue              TEXT NOT NULL DEFAULT 'binance',
    market             cfg.market_type NOT NULL,
    symbol             TEXT NOT NULL,
    source_kind        cfg.source_type NOT NULL DEFAULT 'derived',
    stream_name        TEXT NOT NULL DEFAULT 'agg.orderbook.1m',
    ts_bucket          TIMESTAMPTZ NOT NULL,
    chunk_start_ts     TIMESTAMPTZ NOT NULL,
    chunk_end_ts       TIMESTAMPTZ NOT NULL,
    source_event_count BIGINT,
    sample_count       BIGINT NOT NULL DEFAULT 0,
    bbo_updates        BIGINT NOT NULL DEFAULT 0,
    spread_sum         DOUBLE PRECISION NOT NULL DEFAULT 0,
    topk_depth_sum     DOUBLE PRECISION NOT NULL DEFAULT 0,
    obi_sum            DOUBLE PRECISION NOT NULL DEFAULT 0,
    obi_l1_sum         DOUBLE PRECISION NOT NULL DEFAULT 0,
    obi_k_sum          DOUBLE PRECISION NOT NULL DEFAULT 0,
    obi_k_dw_sum       DOUBLE PRECISION NOT NULL DEFAULT 0,
    obi_k_dw_change_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    obi_k_dw_adj_sum   DOUBLE PRECISION NOT NULL DEFAULT 0,
    microprice_sum     DOUBLE PRECISION NOT NULL DEFAULT 0,
    microprice_classic_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    microprice_kappa_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    microprice_adj_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    ofi_sum            DOUBLE PRECISION NOT NULL DEFAULT 0,
    obi_k_dw_close     DOUBLE PRECISION,
    heatmap_levels     JSONB NOT NULL DEFAULT '[]'::jsonb,
    payload_json       JSONB NOT NULL DEFAULT '{}'::jsonb,
    quarantined_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    quarantine_reason  TEXT NOT NULL DEFAULT '',
    CONSTRAINT agg_orderbook_1m_check CHECK (chunk_end_ts >= chunk_start_ts)
);
\endif

CREATE OR REPLACE FUNCTION ops.ensure_outbox_event_partitions(
    days_ahead INT DEFAULT 7,
    days_back INT DEFAULT 2
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    start_d DATE;
    end_d DATE;
    d DATE;
    i INT;
    part_name TEXT;
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_partitioned_table pt
        JOIN pg_class c ON c.oid = pt.partrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'ops'
          AND c.relname = 'outbox_event'
    ) THEN
        RETURN;
    END IF;

    start_d := CURRENT_DATE - GREATEST(days_back, 0);
    end_d := CURRENT_DATE + GREATEST(days_ahead, 0);

    FOR i IN 0..(end_d - start_d) LOOP
        d := start_d + i;
        part_name := format('outbox_event_p%s', to_char(d, 'YYYYMMDD'));
        IF to_regclass(format('ops.%I', part_name)) IS NULL THEN
            BEGIN
                EXECUTE format(
                    'CREATE TABLE ops.%I PARTITION OF ops.outbox_event FOR VALUES FROM (%L) TO (%L)',
                    part_name,
                    d,
                    d + 1
                );
            EXCEPTION
                WHEN duplicate_table THEN
                    NULL;
            END;
        END IF;
        -- PG18 does not allow storage parameters on partitioned parents.
        EXECUTE format(
            'ALTER TABLE ops.%I SET (autovacuum_vacuum_scale_factor = 0.01, autovacuum_analyze_scale_factor = 0.005, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_threshold = 2000)',
            part_name
        );
    END LOOP;
END
$$;

SELECT ops.ensure_outbox_event_partitions(:outbox_partition_days_ahead, :outbox_partition_days_back);

-- 5) Parquet archive registry (Parquet files live outside PG; this stores metadata/index)
CREATE TABLE IF NOT EXISTS archive.parquet_dataset (
    dataset_id         BIGSERIAL PRIMARY KEY,
    dataset_name       TEXT NOT NULL UNIQUE,  -- e.g. raw_trade_event, feat_orderbook_feature_100ms
    dataset_kind       cfg.parquet_dataset_kind NOT NULL,
    schema_version     TEXT NOT NULL,
    partition_spec     JSONB NOT NULL,        -- {"keys":["date","market","symbol"]}
    logical_table      TEXT,                  -- optional DB logical mapping e.g. md.trade_event
    retention_days     INTEGER,
    compression_codec  TEXT DEFAULT 'zstd',
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata           JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS archive.parquet_object (
    object_id          BIGSERIAL PRIMARY KEY,
    dataset_id         BIGINT NOT NULL REFERENCES archive.parquet_dataset(dataset_id),
    object_uri         TEXT NOT NULL UNIQUE,  -- s3://... or local path
    market             cfg.market_type,
    symbol             TEXT,
    date_partition     DATE,
    min_ts             TIMESTAMPTZ,
    max_ts             TIMESTAMPTZ,
    row_count          BIGINT,
    file_size_bytes    BIGINT,
    checksum           TEXT,
    schema_fingerprint TEXT,
    partition_values   JSONB NOT NULL DEFAULT '{}'::jsonb,
    stats_json         JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (max_ts IS NULL OR min_ts IS NULL OR max_ts >= min_ts)
);
CREATE INDEX IF NOT EXISTS idx_parquet_object_lookup ON archive.parquet_object(dataset_id, market, symbol, date_partition);
CREATE INDEX IF NOT EXISTS idx_parquet_object_time ON archive.parquet_object(dataset_id, min_ts, max_ts);

-- 6) Normalized Market Data (high-volume) -----------------------------------

-- 6.1 Trades (aggTrade / trade normalized into one schema)
CREATE TABLE IF NOT EXISTS md.trade_event (
    ts_event           TIMESTAMPTZ NOT NULL,  -- exchange event/trade time
    ts_recv            TIMESTAMPTZ NOT NULL DEFAULT now(),
    venue              TEXT NOT NULL DEFAULT 'binance',
    market             cfg.market_type NOT NULL,
    symbol             TEXT NOT NULL,
    source_kind        cfg.source_type NOT NULL DEFAULT 'ws',
    stream_name        TEXT NOT NULL,         -- ethusdt@aggTrade / ethusdt@trade / rest_backfill
    event_type         TEXT NOT NULL DEFAULT 'agg_trade',
    exchange_trade_id  BIGINT,
    agg_trade_id       BIGINT,
    first_trade_id     BIGINT,
    last_trade_id      BIGINT,
    price              DOUBLE PRECISION NOT NULL,
    qty_raw            DOUBLE PRECISION NOT NULL,
    qty_eth            DOUBLE PRECISION NOT NULL,
    notional_usdt      DOUBLE PRECISION NOT NULL,
    is_buyer_maker     BOOLEAN,
    aggressor_side     SMALLINT,              -- +1 buy, -1 sell
    is_best_match      BOOLEAN,
    payload_json       JSONB,                 -- raw payload for audit/reparse
    ingest_batch_id    UUID,
    CHECK (aggressor_side IN (-1,1) OR aggressor_side IS NULL)
);
SELECT create_hypertable('md.trade_event', 'ts_event', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_trade_event_mkt_sym_ts ON md.trade_event(market, symbol, ts_event DESC);
CREATE INDEX IF NOT EXISTS idx_trade_event_recv ON md.trade_event(ts_recv DESC);
CREATE INDEX IF NOT EXISTS idx_trade_event_ids ON md.trade_event(market, symbol, agg_trade_id, exchange_trade_id);
CREATE INDEX IF NOT EXISTS idx_trade_event_stream_ts ON md.trade_event(stream_name, ts_event DESC);
-- idempotency index (TimescaleDB unique index must include partition column ts_event)
CREATE UNIQUE INDEX IF NOT EXISTS uq_md_trade_event_agg
ON md.trade_event (market, symbol, agg_trade_id, ts_event)
WHERE agg_trade_id IS NOT NULL;

-- 6.2 Best Bid/Ask rollup (bookTicker normalized, DB-only low-frequency aggregation)
CREATE TABLE IF NOT EXISTS md.bbo_event_live_rollup (
    ts_event           TIMESTAMPTZ NOT NULL,
    ts_window_start    TIMESTAMPTZ NOT NULL,
    ts_window_end      TIMESTAMPTZ NOT NULL,
    ts_recv            TIMESTAMPTZ NOT NULL DEFAULT now(),
    venue              TEXT NOT NULL DEFAULT 'binance',
    market             cfg.market_type NOT NULL,
    symbol             TEXT NOT NULL,
    source_kind        cfg.source_type NOT NULL DEFAULT 'ws',
    stream_name        TEXT NOT NULL,
    last_update_id     BIGINT,
    sample_count       INTEGER NOT NULL,
    bid_price          DOUBLE PRECISION NOT NULL,
    bid_qty            DOUBLE PRECISION NOT NULL,
    ask_price          DOUBLE PRECISION NOT NULL,
    ask_qty            DOUBLE PRECISION NOT NULL,
    mid_price          DOUBLE PRECISION GENERATED ALWAYS AS ((bid_price + ask_price) / 2.0) STORED,
    spread             DOUBLE PRECISION GENERATED ALWAYS AS ((ask_price - bid_price)) STORED,
    spread_min         DOUBLE PRECISION NOT NULL,
    spread_max         DOUBLE PRECISION NOT NULL,
    spread_avg         DOUBLE PRECISION NOT NULL,
    mid_avg            DOUBLE PRECISION NOT NULL,
    payload_json       JSONB,
    CHECK (ask_price >= bid_price),
    CHECK (sample_count > 0),
    CHECK (ts_window_end > ts_window_start)
);
SELECT create_hypertable('md.bbo_event_live_rollup', 'ts_window_end', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_bbo_rollup_mkt_sym_event_ts
ON md.bbo_event_live_rollup(market, symbol, ts_event DESC);
CREATE INDEX IF NOT EXISTS idx_bbo_rollup_mkt_sym_window_start
ON md.bbo_event_live_rollup(market, symbol, ts_window_start DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_md_bbo_event_live_rollup_window
ON md.bbo_event_live_rollup (market, symbol, stream_name, ts_window_start, ts_window_end);

-- 6.3 Depth L2 deltas (Binance depth@100ms snapshots/diffs normalized)
CREATE TABLE IF NOT EXISTS md.depth_delta_l2 (
    ts_event           TIMESTAMPTZ NOT NULL,
    ts_recv            TIMESTAMPTZ NOT NULL DEFAULT now(),
    venue              TEXT NOT NULL DEFAULT 'binance',
    market             cfg.market_type NOT NULL,
    symbol             TEXT NOT NULL,
    source_kind        cfg.source_type NOT NULL DEFAULT 'ws',
    stream_name        TEXT NOT NULL,
    first_update_id    BIGINT,
    final_update_id    BIGINT,
    prev_final_update_id BIGINT,
    bids_delta         JSONB NOT NULL DEFAULT '[]'::jsonb, -- [[price, qty], ...]
    asks_delta         JSONB NOT NULL DEFAULT '[]'::jsonb,
    event_count        INTEGER,
    payload_json       JSONB,
    CHECK (final_update_id IS NULL OR first_update_id IS NULL OR final_update_id >= first_update_id)
);
SELECT create_hypertable('md.depth_delta_l2', 'ts_event', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_depth_delta_lookup ON md.depth_delta_l2(market, symbol, ts_event DESC);
CREATE INDEX IF NOT EXISTS idx_depth_delta_uid ON md.depth_delta_l2(market, symbol, final_update_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_md_depth_delta_l2_uid
ON md.depth_delta_l2 (market, symbol, final_update_id, ts_event)
WHERE final_update_id IS NOT NULL;

-- 6.4 Periodic L2 orderbook snapshots (for replay fast-forward / warm start)
CREATE TABLE IF NOT EXISTS md.orderbook_snapshot_l2 (
    ts_snapshot        TIMESTAMPTZ NOT NULL,
    ts_recv            TIMESTAMPTZ NOT NULL DEFAULT now(),
    venue              TEXT NOT NULL DEFAULT 'binance',
    market             cfg.market_type NOT NULL,
    symbol             TEXT NOT NULL,
    source_kind        cfg.source_type NOT NULL DEFAULT 'derived',
    depth_levels       INTEGER NOT NULL,          -- snapshot depth (e.g. 50/200)
    last_update_id     BIGINT,
    bids               JSONB NOT NULL,            -- [[price,qty], ...]
    asks               JSONB NOT NULL,
    checksum           TEXT,
    metadata           JSONB NOT NULL DEFAULT '{}'::jsonb
);
SELECT create_hypertable('md.orderbook_snapshot_l2', 'ts_snapshot', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_ob_snapshot_lookup ON md.orderbook_snapshot_l2(market, symbol, ts_snapshot DESC);

-- 6.5 Exchange kline bars (WS优先 + REST补洞，支持 1m/15m/1h/4h/1d/3d)
CREATE TABLE IF NOT EXISTS md.kline_bar (
    open_time          TIMESTAMPTZ NOT NULL,
    close_time         TIMESTAMPTZ NOT NULL,
    ts_recv            TIMESTAMPTZ NOT NULL DEFAULT now(),
    venue              TEXT NOT NULL DEFAULT 'binance',
    market             cfg.market_type NOT NULL,
    symbol             TEXT NOT NULL,
    interval_code      TEXT NOT NULL DEFAULT '1m', -- 1m/15m/1h/4h/1d/3d
    source_kind        cfg.source_type NOT NULL DEFAULT 'ws',
    stream_name        TEXT NOT NULL,
    open_price         DOUBLE PRECISION NOT NULL,
    high_price         DOUBLE PRECISION NOT NULL,
    low_price          DOUBLE PRECISION NOT NULL,
    close_price        DOUBLE PRECISION NOT NULL,
    volume_base        DOUBLE PRECISION,
    quote_volume       DOUBLE PRECISION,
    trade_count        BIGINT,
    taker_buy_base     DOUBLE PRECISION,
    taker_buy_quote    DOUBLE PRECISION,
    is_closed          BOOLEAN NOT NULL DEFAULT TRUE,
    payload_json       JSONB,
    CHECK (close_time > open_time),
    CHECK (high_price >= low_price),
    CHECK (interval_code IN ('1m','15m','1h','4h','1d','3d'))
);
SELECT create_hypertable('md.kline_bar', 'open_time', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS uq_kline_bar ON md.kline_bar(market, symbol, interval_code, open_time);
CREATE INDEX IF NOT EXISTS idx_kline_bar_lookup ON md.kline_bar(market, symbol, interval_code, open_time DESC);

-- 6.6 Mark price + funding (1s stream snapshots)
CREATE TABLE IF NOT EXISTS md.mark_price_funding_1s (
    ts_event           TIMESTAMPTZ NOT NULL,
    ts_recv            TIMESTAMPTZ NOT NULL DEFAULT now(),
    venue              TEXT NOT NULL DEFAULT 'binance',
    market             cfg.market_type NOT NULL DEFAULT 'futures',
    symbol             TEXT NOT NULL,
    source_kind        cfg.source_type NOT NULL DEFAULT 'ws',
    stream_name        TEXT NOT NULL,
    mark_price         DOUBLE PRECISION,
    index_price        DOUBLE PRECISION,
    estimated_settle_price DOUBLE PRECISION,
    funding_rate       DOUBLE PRECISION,
    next_funding_time  TIMESTAMPTZ,
    payload_json       JSONB
);
SELECT create_hypertable('md.mark_price_funding_1s', 'ts_event', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_mark_funding_lookup ON md.mark_price_funding_1s(symbol, ts_event DESC);


-- 6.6b Funding rate settlement/history events (REST回填 + 可选WS派生落表)
CREATE TABLE IF NOT EXISTS md.funding_rate_event (
    funding_time        TIMESTAMPTZ NOT NULL,
    ts_recv             TIMESTAMPTZ NOT NULL DEFAULT now(),
    venue               TEXT NOT NULL DEFAULT 'binance',
    market              cfg.market_type NOT NULL DEFAULT 'futures',
    symbol              TEXT NOT NULL,
    source_kind         cfg.source_type NOT NULL DEFAULT 'rest',
    stream_name         TEXT NOT NULL DEFAULT 'fapi/v1/fundingRate',
    funding_rate        DOUBLE PRECISION NOT NULL,
    mark_price          DOUBLE PRECISION,
    next_funding_time   TIMESTAMPTZ,
    payload_json        JSONB,
    UNIQUE (symbol, funding_time, source_kind)
);
SELECT create_hypertable('md.funding_rate_event', 'funding_time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_funding_rate_event_lookup ON md.funding_rate_event(symbol, funding_time DESC);

-- 6.7 Force order / liquidation events (futures only)
CREATE TABLE IF NOT EXISTS md.force_order_event (
    ts_event           TIMESTAMPTZ NOT NULL,
    ts_recv            TIMESTAMPTZ NOT NULL DEFAULT now(),
    venue              TEXT NOT NULL DEFAULT 'binance',
    market             cfg.market_type NOT NULL DEFAULT 'futures',
    symbol             TEXT NOT NULL,
    source_kind        cfg.source_type NOT NULL DEFAULT 'ws',
    stream_name        TEXT NOT NULL,
    order_side         cfg.side_type NOT NULL,      -- buy/sell liquidation order direction
    position_side      TEXT,                        -- LONG/SHORT/BOTH if available
    order_type         TEXT,
    tif                TEXT,
    original_qty       DOUBLE PRECISION,
    price              DOUBLE PRECISION,
    average_price      DOUBLE PRECISION,
    filled_qty         DOUBLE PRECISION,
    last_filled_qty    DOUBLE PRECISION,
    trade_time         TIMESTAMPTZ,
    notional_usdt      DOUBLE PRECISION,
    liq_side           TEXT,                        -- normalized long_liq / short_liq
    payload_json       JSONB
);
SELECT create_hypertable('md.force_order_event', 'ts_event', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_force_order_lookup ON md.force_order_event(symbol, ts_event DESC);
CREATE INDEX IF NOT EXISTS idx_force_order_liq_side ON md.force_order_event(liq_side, ts_event DESC);

-- 6.8 Minute aggregate tables (aggregate-only ingest path)
CREATE TABLE IF NOT EXISTS md.agg_trade_1m (
    ts_event            TIMESTAMPTZ NOT NULL,
    ts_recv             TIMESTAMPTZ NOT NULL DEFAULT now(),
    venue               TEXT NOT NULL DEFAULT 'binance',
    market              cfg.market_type NOT NULL,
    symbol              TEXT NOT NULL,
    source_kind         cfg.source_type NOT NULL DEFAULT 'derived',
    stream_name         TEXT NOT NULL DEFAULT 'agg.trade.1m',
    ts_bucket           TIMESTAMPTZ NOT NULL,
    chunk_start_ts      TIMESTAMPTZ NOT NULL,
    chunk_end_ts        TIMESTAMPTZ NOT NULL,
    source_event_count  BIGINT,
    trade_count         BIGINT NOT NULL DEFAULT 0,
    buy_qty             DOUBLE PRECISION NOT NULL DEFAULT 0,
    sell_qty            DOUBLE PRECISION NOT NULL DEFAULT 0,
    buy_notional        DOUBLE PRECISION NOT NULL DEFAULT 0,
    sell_notional       DOUBLE PRECISION NOT NULL DEFAULT 0,
    first_price         DOUBLE PRECISION,
    last_price          DOUBLE PRECISION,
    high_price          DOUBLE PRECISION,
    low_price           DOUBLE PRECISION,
    profile_levels      JSONB NOT NULL DEFAULT '[]'::jsonb,
    whale_json          JSONB NOT NULL DEFAULT '{}'::jsonb,
    payload_json        JSONB NOT NULL DEFAULT '{}'::jsonb,
    CHECK (chunk_end_ts >= chunk_start_ts)
);
-- Keep as regular table: current unique key used by ON CONFLICT
-- does not include partition column, which Timescale hypertables require.
CREATE INDEX IF NOT EXISTS idx_agg_trade_1m_lookup
ON md.agg_trade_1m(market, symbol, ts_bucket DESC, ts_event DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_agg_trade_1m_chunk
ON md.agg_trade_1m(market, symbol, stream_name, ts_bucket, chunk_start_ts, chunk_end_ts);
-- Backfill read index: symbol-first matches indicator_engine startup backfill pattern
-- WHERE symbol = $3 AND ts_event RANGE AND optional market filter
\if :is_orderflow
CREATE INDEX IF NOT EXISTS idx_agg_trade_1m_backfill
ON md.agg_trade_1m(symbol, ts_event, market);
\endif

CREATE TABLE IF NOT EXISTS md.agg_orderbook_1m (
    ts_event                 TIMESTAMPTZ NOT NULL,
    ts_recv                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    venue                    TEXT NOT NULL DEFAULT 'binance',
    market                   cfg.market_type NOT NULL,
    symbol                   TEXT NOT NULL,
    source_kind              cfg.source_type NOT NULL DEFAULT 'derived',
    stream_name              TEXT NOT NULL DEFAULT 'agg.orderbook.1m',
    ts_bucket                TIMESTAMPTZ NOT NULL,
    chunk_start_ts           TIMESTAMPTZ NOT NULL,
    chunk_end_ts             TIMESTAMPTZ NOT NULL,
    source_event_count       BIGINT,
    sample_count             BIGINT NOT NULL DEFAULT 0,
    bbo_updates              BIGINT NOT NULL DEFAULT 0,
    spread_sum               DOUBLE PRECISION NOT NULL DEFAULT 0,
    topk_depth_sum           DOUBLE PRECISION NOT NULL DEFAULT 0,
    obi_sum                  DOUBLE PRECISION NOT NULL DEFAULT 0,
    obi_l1_sum               DOUBLE PRECISION NOT NULL DEFAULT 0,
    obi_k_sum                DOUBLE PRECISION NOT NULL DEFAULT 0,
    obi_k_dw_sum             DOUBLE PRECISION NOT NULL DEFAULT 0,
    obi_k_dw_change_sum      DOUBLE PRECISION NOT NULL DEFAULT 0,
    obi_k_dw_adj_sum         DOUBLE PRECISION NOT NULL DEFAULT 0,
    microprice_sum           DOUBLE PRECISION NOT NULL DEFAULT 0,
    microprice_classic_sum   DOUBLE PRECISION NOT NULL DEFAULT 0,
    microprice_kappa_sum     DOUBLE PRECISION NOT NULL DEFAULT 0,
    microprice_adj_sum       DOUBLE PRECISION NOT NULL DEFAULT 0,
    ofi_sum                  DOUBLE PRECISION NOT NULL DEFAULT 0,
    obi_k_dw_close           DOUBLE PRECISION,
    heatmap_levels           JSONB NOT NULL DEFAULT '[]'::jsonb,
    payload_json             JSONB NOT NULL DEFAULT '{}'::jsonb,
    CHECK (chunk_end_ts >= chunk_start_ts)
);
-- Keep as regular table: current unique key used by ON CONFLICT
-- does not include partition column, which Timescale hypertables require.
CREATE INDEX IF NOT EXISTS idx_agg_orderbook_1m_lookup
ON md.agg_orderbook_1m(market, symbol, ts_bucket DESC, ts_event DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_agg_orderbook_1m_chunk
ON md.agg_orderbook_1m(market, symbol, stream_name, ts_bucket, chunk_start_ts, chunk_end_ts);
\if :is_orderflow
CREATE INDEX IF NOT EXISTS idx_agg_orderbook_1m_backfill
ON md.agg_orderbook_1m(symbol, ts_event, market);
\endif

CREATE TABLE IF NOT EXISTS md.agg_liq_1m (
    ts_event            TIMESTAMPTZ NOT NULL,
    ts_recv             TIMESTAMPTZ NOT NULL DEFAULT now(),
    venue               TEXT NOT NULL DEFAULT 'binance',
    market              cfg.market_type NOT NULL,
    symbol              TEXT NOT NULL,
    source_kind         cfg.source_type NOT NULL DEFAULT 'derived',
    stream_name         TEXT NOT NULL DEFAULT 'agg.liq.1m',
    ts_bucket           TIMESTAMPTZ NOT NULL,
    chunk_start_ts      TIMESTAMPTZ NOT NULL,
    chunk_end_ts        TIMESTAMPTZ NOT NULL,
    source_event_count  BIGINT,
    force_liq_levels    JSONB NOT NULL DEFAULT '[]'::jsonb,
    payload_json        JSONB NOT NULL DEFAULT '{}'::jsonb,
    CHECK (chunk_end_ts >= chunk_start_ts)
);
-- Keep as regular table: current unique key used by ON CONFLICT
-- does not include partition column, which Timescale hypertables require.
CREATE INDEX IF NOT EXISTS idx_agg_liq_1m_lookup
ON md.agg_liq_1m(market, symbol, ts_bucket DESC, ts_event DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_agg_liq_1m_chunk
ON md.agg_liq_1m(market, symbol, stream_name, ts_bucket, chunk_start_ts, chunk_end_ts);
\if :is_orderflow
CREATE INDEX IF NOT EXISTS idx_agg_liq_1m_backfill
ON md.agg_liq_1m(symbol, ts_event, market);
\endif

CREATE TABLE IF NOT EXISTS md.agg_funding_mark_1m (
    ts_event            TIMESTAMPTZ NOT NULL,
    ts_recv             TIMESTAMPTZ NOT NULL DEFAULT now(),
    venue               TEXT NOT NULL DEFAULT 'binance',
    market              cfg.market_type NOT NULL,
    symbol              TEXT NOT NULL,
    source_kind         cfg.source_type NOT NULL DEFAULT 'derived',
    stream_name         TEXT NOT NULL DEFAULT 'agg.funding_mark.1m',
    ts_bucket           TIMESTAMPTZ NOT NULL,
    chunk_start_ts      TIMESTAMPTZ NOT NULL,
    chunk_end_ts        TIMESTAMPTZ NOT NULL,
    source_event_count  BIGINT,
    mark_points         JSONB NOT NULL DEFAULT '[]'::jsonb,
    funding_points      JSONB NOT NULL DEFAULT '[]'::jsonb,
    payload_json        JSONB NOT NULL DEFAULT '{}'::jsonb,
    CHECK (chunk_end_ts >= chunk_start_ts)
);
-- Keep as regular table: current unique key used by ON CONFLICT
-- does not include partition column, which Timescale hypertables require.
CREATE INDEX IF NOT EXISTS idx_agg_funding_mark_1m_lookup
ON md.agg_funding_mark_1m(market, symbol, ts_bucket DESC, ts_event DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_agg_funding_mark_1m_chunk
ON md.agg_funding_mark_1m(market, symbol, stream_name, ts_bucket, chunk_start_ts, chunk_end_ts);
\if :is_orderflow
CREATE INDEX IF NOT EXISTS idx_agg_funding_mark_1m_backfill
ON md.agg_funding_mark_1m(symbol, ts_event, market);
\endif

-- 7) Feature / Indicator Store ------------------------------------------------

-- 7.1 Trade-flow base features (100ms / 1s / 1m) for both markets
-- Supports: Delta / RelativeDelta / CVD / Trades Burst / CVD slope / VPIN / AVWAP rolling helpers
CREATE TABLE IF NOT EXISTS feat.trade_flow_feature (
    ts_bucket          TIMESTAMPTZ NOT NULL,
    bar_interval       INTERVAL NOT NULL,           -- 100ms / 1s / 1m
    venue              TEXT NOT NULL DEFAULT 'binance',
    market             cfg.market_type NOT NULL,
    symbol             TEXT NOT NULL,
    source_trade_stream TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    qty_mode           TEXT NOT NULL DEFAULT 'qty_eth', -- qty_eth / notional_usdt
    trade_count        BIGINT,
    buy_qty            DOUBLE PRECISION,
    sell_qty           DOUBLE PRECISION,
    total_qty          DOUBLE PRECISION,
    delta              DOUBLE PRECISION,
    relative_delta     DOUBLE PRECISION,
    cvd                DOUBLE PRECISION,
    cvd_rolling_7d     DOUBLE PRECISION,
    cvd_slope_ols      DOUBLE PRECISION,
    ema_delta_fast     DOUBLE PRECISION,
    ema_delta_slow     DOUBLE PRECISION,
    cvd_burst_signal   DOUBLE PRECISION,
    cvd_burst_z        DOUBLE PRECISION,
    vpin               DOUBLE PRECISION,
    vpin_bucket_fill   DOUBLE PRECISION,
    avwap_value        DOUBLE PRECISION,
    avwap_anchor_ts    TIMESTAMPTZ,
    extra_json         JSONB NOT NULL DEFAULT '{}'::jsonb,
    param_set_id       UUID,
    calc_version       TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('feat.trade_flow_feature', 'ts_bucket', if_not_exists => TRUE);
\if :is_orderflow
CREATE UNIQUE INDEX IF NOT EXISTS uq_trade_flow_feature_natural
ON feat.trade_flow_feature(venue, market, symbol, bar_interval, ts_bucket);
\endif
CREATE INDEX IF NOT EXISTS idx_trade_flow_feature_lookup ON feat.trade_flow_feature(market, symbol, bar_interval, ts_bucket DESC);
CREATE INDEX IF NOT EXISTS idx_trade_flow_feature_anchor ON feat.trade_flow_feature(market, symbol, avwap_anchor_ts, ts_bucket DESC);

-- 7.2 Orderbook / BBO continuous features (100ms / 1s)
-- Supports: OBI/OFI/Microprice/Heatmap-sidecar confirmation/fake-order risk/CVD divergence factor sidecar
CREATE TABLE IF NOT EXISTS feat.orderbook_feature (
    ts_bucket          TIMESTAMPTZ NOT NULL,
    bar_interval       INTERVAL NOT NULL,           -- 100ms / 1s
    venue              TEXT NOT NULL DEFAULT 'binance',
    symbol             TEXT NOT NULL,
    depth_k            INTEGER,
    -- Futures primary
    spread_twa_fut     DOUBLE PRECISION,
    topk_depth_twa_fut DOUBLE PRECISION,
    obi_fut            DOUBLE PRECISION,            -- compat -> obi_k_twa_fut
    obi_l1_twa_fut     DOUBLE PRECISION,
    obi_k_twa_fut      DOUBLE PRECISION,
    obi_k_dw_twa_fut   DOUBLE PRECISION,
    obi_k_dw_close_fut DOUBLE PRECISION,
    obi_k_dw_change_fut DOUBLE PRECISION,
    obi_k_dw_slope_fut DOUBLE PRECISION,
    obi_k_dw_adj_twa_fut DOUBLE PRECISION,
    ofi_fut            DOUBLE PRECISION,
    ofi_norm_fut       DOUBLE PRECISION,
    bbo_updates_fut    INTEGER,
    microprice_classic_fut DOUBLE PRECISION,
    microprice_kappa_fut   DOUBLE PRECISION,
    microprice_adj_fut     DOUBLE PRECISION,
    heatmap_summary_fut JSONB,                     -- optional compressed summaries; full matrix in indicator tables
    -- Spot sidecar (confirm / attribution)
    spread_twa_spot     DOUBLE PRECISION,
    topk_depth_twa_spot DOUBLE PRECISION,
    obi_k_dw_twa_spot   DOUBLE PRECISION,
    ofi_spot            DOUBLE PRECISION,
    ofi_norm_spot       DOUBLE PRECISION,
    trade_delta_spot    DOUBLE PRECISION,
    relative_delta_spot DOUBLE PRECISION,
    cvd_slope_spot      DOUBLE PRECISION,
    -- Cross-market confirmation / fake order risk
    exec_confirm_fut      BOOLEAN,
    spot_confirm          BOOLEAN,
    obi_shock_fut         DOUBLE PRECISION,
    weak_price_resp_fut   BOOLEAN,
    fake_order_risk_fut   DOUBLE PRECISION,
    spot_driven_divergence_flag BOOLEAN,
    cross_cvd_attribution DOUBLE PRECISION,
    extra_json          JSONB NOT NULL DEFAULT '{}'::jsonb,
    param_set_id        UUID,
    calc_version        TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('feat.orderbook_feature', 'ts_bucket', if_not_exists => TRUE);
\if :is_orderflow
CREATE UNIQUE INDEX IF NOT EXISTS uq_orderbook_feature_natural
ON feat.orderbook_feature(venue, symbol, bar_interval, ts_bucket);
\endif
CREATE INDEX IF NOT EXISTS idx_orderbook_feature_lookup ON feat.orderbook_feature(symbol, bar_interval, ts_bucket DESC);

-- 7.3 CVD Pack (dual-market attribution pack, 多窗口)
-- Supports: indicator 14) CVD pack (Rolling 7D), delta slant / relative delta / xmk fields for 1m/15m/1h/4h/1d/3d
CREATE TABLE IF NOT EXISTS feat.cvd_pack (
    ts_bucket          TIMESTAMPTZ NOT NULL,
    window_code        TEXT NOT NULL DEFAULT '1m',
    venue              TEXT NOT NULL DEFAULT 'binance',
    symbol             TEXT NOT NULL,
    -- futures
    delta_fut          DOUBLE PRECISION,
    relative_delta_fut DOUBLE PRECISION,
    cvd_fut            DOUBLE PRECISION,
    cvd_rolling_7d_fut DOUBLE PRECISION,
    cvd_slope_fut      DOUBLE PRECISION,
    delta_slant_fut    DOUBLE PRECISION,
    -- spot
    delta_spot         DOUBLE PRECISION,
    relative_delta_spot DOUBLE PRECISION,
    cvd_spot           DOUBLE PRECISION,
    cvd_rolling_7d_spot DOUBLE PRECISION,
    cvd_slope_spot     DOUBLE PRECISION,
    delta_slant_spot   DOUBLE PRECISION,
    -- cross-market attribution
    cvd_diff_fs        DOUBLE PRECISION,
    cvd_slope_diff_fs  DOUBLE PRECISION,
    spot_lead_score    DOUBLE PRECISION,
    likely_driver      TEXT,
    extra_json         JSONB NOT NULL DEFAULT '{}'::jsonb,
    param_set_id       UUID,
    calc_version       TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (window_code IN ('1m','15m','1h','4h','1d','3d')),
    UNIQUE (symbol, window_code, ts_bucket)
);
SELECT create_hypertable('feat.cvd_pack', 'ts_bucket', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_cvd_pack_lookup ON feat.cvd_pack(symbol, window_code, ts_bucket DESC);
CREATE OR REPLACE VIEW feat.cvd_pack_1m AS
SELECT * FROM feat.cvd_pack WHERE window_code = '1m';

-- 7.4 AVWAP feature table (supports futures AVWAP + spot AVWAP + gap attribution)
CREATE TABLE IF NOT EXISTS feat.avwap_feature (
    ts_bucket          TIMESTAMPTZ NOT NULL,
    bar_interval       INTERVAL NOT NULL,
    venue              TEXT NOT NULL DEFAULT 'binance',
    symbol             TEXT NOT NULL,
    anchor_ts          TIMESTAMPTZ NOT NULL,
    anchor_label       TEXT,
    avwap_fut          DOUBLE PRECISION,
    avwap_spot         DOUBLE PRECISION,
    fut_last_price     DOUBLE PRECISION,
    fut_mark_price     DOUBLE PRECISION,
    price_minus_avwap_fut DOUBLE PRECISION,
    price_minus_spot_avwap_fut DOUBLE PRECISION,
    price_minus_spot_avwap_futmark DOUBLE PRECISION,
    avwap_gap_fs       DOUBLE PRECISION,
    xmk_avwap_gap_f_minus_s DOUBLE PRECISION,
    zavwap_gap         DOUBLE PRECISION,
    extra_json         JSONB NOT NULL DEFAULT '{}'::jsonb,
    param_set_id       UUID,
    calc_version       TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE IF EXISTS feat.avwap_feature
    ADD COLUMN IF NOT EXISTS price_minus_spot_avwap_futmark DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS xmk_avwap_gap_f_minus_s DOUBLE PRECISION;
SELECT create_hypertable('feat.avwap_feature', 'ts_bucket', if_not_exists => TRUE);
\if :is_orderflow
CREATE UNIQUE INDEX IF NOT EXISTS uq_avwap_feature_natural
ON feat.avwap_feature(venue, symbol, anchor_ts, bar_interval, ts_bucket);
\endif
CREATE INDEX IF NOT EXISTS idx_avwap_feature_lookup ON feat.avwap_feature(symbol, anchor_ts, bar_interval, ts_bucket DESC);

-- 7.5 Funding feature (1s/1m rollups can be written here from markPrice stream)
CREATE TABLE IF NOT EXISTS feat.funding_feature (
    ts_bucket          TIMESTAMPTZ NOT NULL,
    bar_interval       INTERVAL NOT NULL,
    venue              TEXT NOT NULL DEFAULT 'binance',
    symbol             TEXT NOT NULL,
    funding_current    DOUBLE PRECISION,
    funding_current_effective_ts TIMESTAMPTZ,
    funding_twa        DOUBLE PRECISION,
    mark_price_last    DOUBLE PRECISION,
    mark_price_last_ts TIMESTAMPTZ,
    mark_price_twap    DOUBLE PRECISION,
    index_price_last   DOUBLE PRECISION,
    change_count       INTEGER,
    changes_json       JSONB NOT NULL DEFAULT '[]'::jsonb,
    extra_json         JSONB NOT NULL DEFAULT '{}'::jsonb,
    param_set_id       UUID,
    calc_version       TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE IF EXISTS feat.funding_feature
    ADD COLUMN IF NOT EXISTS funding_current_effective_ts TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS mark_price_last_ts TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS change_count INTEGER,
    ADD COLUMN IF NOT EXISTS changes_json JSONB NOT NULL DEFAULT '[]'::jsonb;
SELECT create_hypertable('feat.funding_feature', 'ts_bucket', if_not_exists => TRUE);
\if :is_orderflow
CREATE UNIQUE INDEX IF NOT EXISTS uq_funding_feature_natural
ON feat.funding_feature(venue, symbol, bar_interval, ts_bucket);
\endif
CREATE INDEX IF NOT EXISTS idx_funding_feature_lookup ON feat.funding_feature(symbol, bar_interval, ts_bucket DESC);
CREATE INDEX IF NOT EXISTS idx_funding_feature_effective_ts ON feat.funding_feature(symbol, funding_current_effective_ts DESC);

-- 7.5b Funding change detail (window-internal change list, query-friendly)
CREATE TABLE IF NOT EXISTS feat.funding_change_event (
    ts_change             TIMESTAMPTZ NOT NULL,
    ts_recorded           TIMESTAMPTZ NOT NULL DEFAULT now(),
    venue                 TEXT NOT NULL DEFAULT 'binance',
    symbol                TEXT NOT NULL,
    source_kind           cfg.source_type NOT NULL DEFAULT 'derived',
    funding_prev          DOUBLE PRECISION,
    funding_new           DOUBLE PRECISION NOT NULL,
    funding_delta         DOUBLE PRECISION,
    mark_price_at_change  DOUBLE PRECISION,
    window_code           TEXT,
    bar_interval          INTERVAL,
    param_set_id          UUID,
    calc_version          TEXT,
    extra_json            JSONB NOT NULL DEFAULT '{}'::jsonb
);
SELECT create_hypertable('feat.funding_change_event', 'ts_change', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_funding_change_event_lookup ON feat.funding_change_event(symbol, ts_change DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_funding_change_event_symbol_ts_rate ON feat.funding_change_event(symbol, ts_change, funding_new);

-- 7.6 Whale trade rollups (market-separated by design) + threshold tracking
CREATE TABLE IF NOT EXISTS feat.whale_trade_rollup (
    ts_bucket          TIMESTAMPTZ NOT NULL,
    bar_interval       INTERVAL NOT NULL,
    venue              TEXT NOT NULL DEFAULT 'binance',
    market             cfg.market_type NOT NULL,
    symbol             TEXT NOT NULL,
    threshold_usdt     DOUBLE PRECISION NOT NULL,
    whale_trade_count  BIGINT NOT NULL DEFAULT 0,
    whale_buy_count    BIGINT NOT NULL DEFAULT 0,
    whale_sell_count   BIGINT NOT NULL DEFAULT 0,
    whale_notional_total DOUBLE PRECISION NOT NULL DEFAULT 0,
    whale_notional_buy   DOUBLE PRECISION NOT NULL DEFAULT 0,
    whale_notional_sell  DOUBLE PRECISION NOT NULL DEFAULT 0,
    whale_qty_eth_total  DOUBLE PRECISION NOT NULL DEFAULT 0,
    max_single_trade_notional DOUBLE PRECISION,
    extra_json         JSONB NOT NULL DEFAULT '{}'::jsonb,
    param_set_id       UUID,
    calc_version       TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('feat.whale_trade_rollup', 'ts_bucket', if_not_exists => TRUE);
\if :is_orderflow
CREATE UNIQUE INDEX IF NOT EXISTS uq_whale_trade_rollup_natural
ON feat.whale_trade_rollup(venue, market, symbol, bar_interval, threshold_usdt, ts_bucket);
\endif
CREATE INDEX IF NOT EXISTS idx_whale_trade_rollup_lookup ON feat.whale_trade_rollup(market, symbol, bar_interval, ts_bucket DESC);

-- 7.7 Generic indicator snapshot (JSONB, for summary outputs of any indicator)
-- Covers all indicators without forcing column explosion. Good for versioned payloads.
\if :is_orderflow
CREATE TABLE IF NOT EXISTS feat.indicator_progress (
    symbol             TEXT PRIMARY KEY,
    last_success_ts    TIMESTAMPTZ NOT NULL,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
\endif

CREATE TABLE IF NOT EXISTS feat.indicator_snapshot (
    ts_snapshot        TIMESTAMPTZ NOT NULL,
    bar_interval       INTERVAL,                    -- NULL for event-driven snapshots
    venue              TEXT NOT NULL DEFAULT 'binance',
    symbol             TEXT NOT NULL,
    market_scope       TEXT NOT NULL DEFAULT 'futures_primary', -- futures/spot/dual
    indicator_code     TEXT NOT NULL REFERENCES cfg.indicator_catalog(indicator_code),
    window_code        TEXT,                        -- 100ms/1s/15m/1h/4h/1d/3d/7d
    primary_market     cfg.market_type,
    param_set_id       UUID,
    calc_version       TEXT,
    payload_json       JSONB NOT NULL,
    tags               TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('feat.indicator_snapshot', 'ts_snapshot', if_not_exists => TRUE);
\if :is_orderflow
CREATE UNIQUE INDEX IF NOT EXISTS idx_indicator_snapshot_idempotent
ON feat.indicator_snapshot(ts_snapshot, symbol, indicator_code, window_code);
\endif
CREATE INDEX IF NOT EXISTS idx_indicator_snapshot_lookup ON feat.indicator_snapshot(indicator_code, symbol, ts_snapshot DESC);
CREATE INDEX IF NOT EXISTS idx_indicator_snapshot_window ON feat.indicator_snapshot(window_code, ts_snapshot DESC);
-- Do NOT create payload_json GIN index by default on this write-hot table.
-- It is extremely expensive for large JSON payloads (footprint/orderbook_depth)
-- and was not used by runtime queries.
-- Optional (ad-hoc analytics only):
-- CREATE INDEX CONCURRENTLY idx_indicator_snapshot_payload_gin
-- ON feat.indicator_snapshot USING GIN (payload_json jsonb_path_ops);
-- Aggressive autovacuum: keeps GIN pending list small to prevent write-latency spikes.
-- Each indicator minute writes ~100+ rows; without frequent vacuum the GIN pending
-- list grows until a single INSERT triggers a full cleanup (observed: 50-second inserts).
ALTER TABLE feat.indicator_snapshot SET (
    autovacuum_vacuum_scale_factor  = 0.01,
    autovacuum_analyze_scale_factor = 0.005,
    autovacuum_vacuum_threshold     = 1000,
    autovacuum_analyze_threshold    = 1000
);

\if :is_orderflow
CREATE OR REPLACE VIEW feat.v_cvd_7d_rolling AS
SELECT
    ts_snapshot,
    symbol,
    (payload_json ->> 'delta_fut')::DOUBLE PRECISION AS delta_fut,
    (payload_json ->> 'delta_spot')::DOUBLE PRECISION AS delta_spot,
    SUM((payload_json ->> 'delta_fut')::DOUBLE PRECISION) OVER (
        PARTITION BY symbol
        ORDER BY ts_snapshot
        ROWS BETWEEN 10079 PRECEDING AND CURRENT ROW
    ) AS cvd_7d_fut,
    SUM((payload_json ->> 'delta_spot')::DOUBLE PRECISION) OVER (
        PARTITION BY symbol
        ORDER BY ts_snapshot
        ROWS BETWEEN 10079 PRECEDING AND CURRENT ROW
    ) AS cvd_7d_spot
FROM feat.indicator_snapshot
WHERE indicator_code = 'cvd_pack';
\endif

\if :is_orderflow
CREATE OR REPLACE VIEW feat.v_cvd_reconstructed AS
SELECT
    ts_snapshot,
    symbol,
    window_code,
    (payload_json ->> 'delta_fut')::DOUBLE PRECISION AS delta_fut,
    (payload_json ->> 'delta_spot')::DOUBLE PRECISION AS delta_spot,
    SUM((payload_json ->> 'delta_fut')::DOUBLE PRECISION) OVER (
        PARTITION BY symbol, window_code
        ORDER BY ts_snapshot
        ROWS UNBOUNDED PRECEDING
    ) AS cvd_fut,
    SUM((payload_json ->> 'delta_spot')::DOUBLE PRECISION) OVER (
        PARTITION BY symbol, window_code
        ORDER BY ts_snapshot
        ROWS UNBOUNDED PRECEDING
    ) AS cvd_spot,
    (
        SUM((payload_json ->> 'delta_spot')::DOUBLE PRECISION) OVER (
            PARTITION BY symbol, window_code
            ORDER BY ts_snapshot
            ROWS UNBOUNDED PRECEDING
        )
        - SUM((payload_json ->> 'delta_fut')::DOUBLE PRECISION) OVER (
            PARTITION BY symbol, window_code
            ORDER BY ts_snapshot
            ROWS UNBOUNDED PRECEDING
        )
    ) AS cvd_diff_spot_minus_fut
FROM feat.indicator_snapshot
WHERE indicator_code = 'cvd_pack';
\endif

-- 7.8 Generic indicator level matrix (price-level grids for profile/footprint/heatmap/liq density)
-- One row per (indicator snapshot, price level), query-friendly and replayable.
CREATE TABLE IF NOT EXISTS feat.indicator_level_value (
    ts_snapshot        TIMESTAMPTZ NOT NULL,
    venue              TEXT NOT NULL DEFAULT 'binance',
    symbol             TEXT NOT NULL,
    indicator_code     TEXT NOT NULL REFERENCES cfg.indicator_catalog(indicator_code),
    window_code        TEXT,
    market_scope       TEXT NOT NULL DEFAULT 'futures_primary',
    price_level        DOUBLE PRECISION NOT NULL,
    level_rank         INTEGER,
    metrics_json       JSONB NOT NULL,              -- buy/sell/total/delta/is_hvn/... or heatmap/liq density fields
    param_set_id       UUID,
    calc_version       TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('feat.indicator_level_value', 'ts_snapshot', if_not_exists => TRUE);
\if :is_orderflow
CREATE UNIQUE INDEX IF NOT EXISTS uq_indicator_level_value_natural
ON feat.indicator_level_value(
    ts_snapshot, venue, symbol, indicator_code, window_code, market_scope, price_level
);
\endif
CREATE INDEX IF NOT EXISTS idx_indicator_level_lookup ON feat.indicator_level_value(indicator_code, symbol, window_code, ts_snapshot DESC, price_level);
CREATE INDEX IF NOT EXISTS idx_indicator_level_metrics_gin ON feat.indicator_level_value USING GIN (metrics_json jsonb_path_ops);
ALTER TABLE feat.indicator_level_value SET (
    autovacuum_vacuum_scale_factor  = 0.01,
    autovacuum_analyze_scale_factor = 0.005,
    autovacuum_vacuum_threshold     = 1000,
    autovacuum_analyze_threshold    = 1000
);

-- 8) Event Store (signals / pattern detections) --------------------------------

-- 8.1 Generic indicator events
-- Covers divergence, absorption, initiation, buying/selling exhaustion, bullish/bearish variants, etc.
\if :is_orderflow
CREATE TABLE IF NOT EXISTS evt.indicator_event (
    ts_event_start      TIMESTAMPTZ NOT NULL,
    ts_event_end        TIMESTAMPTZ,
    event_available_ts  TIMESTAMPTZ,
    ts_recorded         TIMESTAMPTZ NOT NULL DEFAULT now(),
    venue               TEXT NOT NULL DEFAULT 'binance',
    symbol              TEXT NOT NULL,
    indicator_code      TEXT NOT NULL REFERENCES cfg.indicator_catalog(indicator_code),
    event_type          TEXT NOT NULL,             -- bearish_divergence / bullish_absorption / ...
    severity            cfg.event_severity NOT NULL DEFAULT 'info',
    direction           SMALLINT,                  -- +1 bullish, -1 bearish, 0 neutral
    bar_interval        INTERVAL,
    window_code         TEXT,
    pivot_ts_1          TIMESTAMPTZ,
    pivot_ts_2          TIMESTAMPTZ,
    pivot_confirm_ts_1  TIMESTAMPTZ,
    pivot_confirm_ts_2  TIMESTAMPTZ,
    sig_pass            BOOLEAN,
    p_value             DOUBLE PRECISION,
    primary_market      cfg.market_type,
    market_scope        TEXT NOT NULL DEFAULT 'futures_primary',
    score               DOUBLE PRECISION,
    confidence          DOUBLE PRECISION,
    param_set_id        UUID,
    calc_version        TEXT,
    payload_json        JSONB NOT NULL,
    tags                TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    event_id            TEXT NOT NULL
);
ALTER TABLE IF EXISTS evt.indicator_event
    ADD COLUMN IF NOT EXISTS event_id TEXT,
    ADD COLUMN IF NOT EXISTS event_available_ts TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS pivot_ts_1 TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS pivot_ts_2 TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS pivot_confirm_ts_1 TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS pivot_confirm_ts_2 TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS sig_pass BOOLEAN,
    ADD COLUMN IF NOT EXISTS p_value DOUBLE PRECISION;
SELECT create_hypertable('evt.indicator_event', 'ts_event_start', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS uq_indicator_event_event_id
ON evt.indicator_event(event_id, ts_event_start);
CREATE INDEX IF NOT EXISTS idx_indicator_event_lookup ON evt.indicator_event(indicator_code, symbol, ts_event_start DESC);
CREATE INDEX IF NOT EXISTS idx_indicator_event_type_lookup ON evt.indicator_event(event_type, symbol, ts_event_start DESC);
CREATE INDEX IF NOT EXISTS idx_indicator_event_available_ts ON evt.indicator_event(symbol, indicator_code, event_available_ts DESC);
CREATE INDEX IF NOT EXISTS idx_indicator_event_payload_gin ON evt.indicator_event USING GIN (payload_json jsonb_path_ops);

-- 8.2 Divergence event detail (typed columns for fast filtering and attribution)
CREATE TABLE IF NOT EXISTS evt.divergence_event (
    ts_event_start      TIMESTAMPTZ NOT NULL,
    ts_event_end        TIMESTAMPTZ NOT NULL,
    event_available_ts  TIMESTAMPTZ,
    ts_recorded         TIMESTAMPTZ NOT NULL DEFAULT now(),
    venue               TEXT NOT NULL DEFAULT 'binance',
    symbol              TEXT NOT NULL,
    divergence_type     TEXT NOT NULL,             -- bearish / hidden_bearish / bullish / hidden_bullish
    pivot_side          TEXT NOT NULL,             -- high / low
    pivot_ts_1          TIMESTAMPTZ,
    pivot_ts_2          TIMESTAMPTZ,
    pivot_confirm_ts_1  TIMESTAMPTZ,
    pivot_confirm_ts_2  TIMESTAMPTZ,
    leg_minutes         INTEGER,
    price_start         DOUBLE PRECISION,
    price_end           DOUBLE PRECISION,
    price_diff          DOUBLE PRECISION,
    price_norm_diff     DOUBLE PRECISION,
    cvd_start_1m_fut    DOUBLE PRECISION,
    cvd_end_1m_fut      DOUBLE PRECISION,
    cvd_diff_fut        DOUBLE PRECISION,
    cvd_norm_diff_fut   DOUBLE PRECISION,
    cvd_start_1m_spot   DOUBLE PRECISION,
    cvd_end_1m_spot     DOUBLE PRECISION,
    cvd_diff_spot       DOUBLE PRECISION,
    cvd_norm_diff_spot  DOUBLE PRECISION,
    price_effect_z      DOUBLE PRECISION,
    cvd_effect_z        DOUBLE PRECISION,
    sig_pass            BOOLEAN,
    p_value_price       DOUBLE PRECISION,
    p_value_cvd         DOUBLE PRECISION,
    spot_price_flow_confirm BOOLEAN,
    fut_divergence_sign SMALLINT,
    spot_lead_score     DOUBLE PRECISION,
    likely_driver       TEXT,
    score               DOUBLE PRECISION,
    confidence          DOUBLE PRECISION,
    window_code         TEXT,
    param_set_id        UUID,
    calc_version        TEXT,
    payload_json        JSONB NOT NULL DEFAULT '{}'::jsonb,
    event_id            TEXT NOT NULL
);
ALTER TABLE IF EXISTS evt.divergence_event
    ADD COLUMN IF NOT EXISTS event_id TEXT,
    ADD COLUMN IF NOT EXISTS pivot_ts_1 TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS pivot_ts_2 TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS pivot_confirm_ts_1 TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS pivot_confirm_ts_2 TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS event_available_ts TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS price_norm_diff DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS cvd_norm_diff_fut DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS cvd_norm_diff_spot DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS price_effect_z DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS cvd_effect_z DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS sig_pass BOOLEAN,
    ADD COLUMN IF NOT EXISTS p_value_price DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS p_value_cvd DOUBLE PRECISION;
SELECT create_hypertable('evt.divergence_event', 'ts_event_start', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS uq_divergence_event_event_id
ON evt.divergence_event(event_id, ts_event_start);
CREATE INDEX IF NOT EXISTS idx_divergence_event_lookup ON evt.divergence_event(symbol, divergence_type, ts_event_start DESC);
CREATE INDEX IF NOT EXISTS idx_divergence_event_driver ON evt.divergence_event(likely_driver, ts_event_start DESC);
CREATE INDEX IF NOT EXISTS idx_divergence_event_available_ts ON evt.divergence_event(symbol, event_available_ts DESC);
CREATE INDEX IF NOT EXISTS idx_divergence_event_sig_pass ON evt.divergence_event(sig_pass, ts_event_start DESC);

-- 8.3 Absorption event detail (typed columns for audit/replay)
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
    payload_json               JSONB NOT NULL DEFAULT '{}'::jsonb,
    trigger_side               TEXT
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

-- 8.4 Initiation event detail (typed columns for audit/replay)
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
    follow_through_minutes     INTEGER,
    follow_through_end_ts      TIMESTAMPTZ,
    spot_break_confirm         BOOLEAN,
    spot_rdelta_1m_mean        DOUBLE PRECISION,
    spot_cvd_change            DOUBLE PRECISION,
    spot_whale_break_confirm   BOOLEAN,
    score                      DOUBLE PRECISION,
    confidence                 DOUBLE PRECISION,
    window_code                TEXT,
    param_set_id               UUID,
    calc_version               TEXT,
    payload_json               JSONB NOT NULL DEFAULT '{}'::jsonb,
    min_follow_required_minutes INTEGER,
    follow_through_delta_sum   DOUBLE PRECISION,
    follow_through_hold_ok     BOOLEAN,
    follow_through_max_adverse_excursion_ticks DOUBLE PRECISION
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

-- 8.5 Exhaustion event detail (typed columns for audit/replay)
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
\else
CREATE TABLE IF NOT EXISTS evt.indicator_event (
    ts_event_start      TIMESTAMPTZ NOT NULL,
    ts_event_end        TIMESTAMPTZ,
    event_available_ts  TIMESTAMPTZ,
    ts_recorded         TIMESTAMPTZ NOT NULL DEFAULT now(),
    venue               TEXT NOT NULL DEFAULT 'binance',
    symbol              TEXT NOT NULL,
    indicator_code      TEXT NOT NULL REFERENCES cfg.indicator_catalog(indicator_code),
    event_type          TEXT NOT NULL,
    severity            cfg.event_severity NOT NULL DEFAULT 'info',
    direction           SMALLINT,
    bar_interval        INTERVAL,
    window_code         TEXT,
    pivot_ts_1          TIMESTAMPTZ,
    pivot_ts_2          TIMESTAMPTZ,
    pivot_confirm_ts_1  TIMESTAMPTZ,
    pivot_confirm_ts_2  TIMESTAMPTZ,
    sig_pass            BOOLEAN,
    p_value             DOUBLE PRECISION,
    primary_market      cfg.market_type,
    market_scope        TEXT NOT NULL DEFAULT 'futures_primary',
    score               DOUBLE PRECISION,
    confidence          DOUBLE PRECISION,
    param_set_id        UUID,
    calc_version        TEXT,
    payload_json        JSONB NOT NULL,
    tags                TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[]
);
ALTER TABLE IF EXISTS evt.indicator_event
    ADD COLUMN IF NOT EXISTS event_available_ts TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS pivot_ts_1 TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS pivot_ts_2 TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS pivot_confirm_ts_1 TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS pivot_confirm_ts_2 TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS sig_pass BOOLEAN,
    ADD COLUMN IF NOT EXISTS p_value DOUBLE PRECISION;
SELECT create_hypertable('evt.indicator_event', 'ts_event_start', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_indicator_event_lookup ON evt.indicator_event(indicator_code, symbol, ts_event_start DESC);
CREATE INDEX IF NOT EXISTS idx_indicator_event_type_lookup ON evt.indicator_event(event_type, symbol, ts_event_start DESC);
CREATE INDEX IF NOT EXISTS idx_indicator_event_available_ts ON evt.indicator_event(symbol, indicator_code, event_available_ts DESC);
CREATE INDEX IF NOT EXISTS idx_indicator_event_payload_gin ON evt.indicator_event USING GIN (payload_json jsonb_path_ops);

-- 8.2 Divergence event detail (typed columns for fast filtering and attribution)
CREATE TABLE IF NOT EXISTS evt.divergence_event (
    ts_event_start      TIMESTAMPTZ NOT NULL,
    ts_event_end        TIMESTAMPTZ NOT NULL,
    event_available_ts  TIMESTAMPTZ,
    ts_recorded         TIMESTAMPTZ NOT NULL DEFAULT now(),
    venue               TEXT NOT NULL DEFAULT 'binance',
    symbol              TEXT NOT NULL,
    divergence_type     TEXT NOT NULL,
    pivot_side          TEXT NOT NULL,
    pivot_ts_1          TIMESTAMPTZ,
    pivot_ts_2          TIMESTAMPTZ,
    pivot_confirm_ts_1  TIMESTAMPTZ,
    pivot_confirm_ts_2  TIMESTAMPTZ,
    leg_minutes         INTEGER,
    price_start         DOUBLE PRECISION,
    price_end           DOUBLE PRECISION,
    price_diff          DOUBLE PRECISION,
    price_norm_diff     DOUBLE PRECISION,
    cvd_start_1m_fut    DOUBLE PRECISION,
    cvd_end_1m_fut      DOUBLE PRECISION,
    cvd_diff_fut        DOUBLE PRECISION,
    cvd_norm_diff_fut   DOUBLE PRECISION,
    cvd_start_1m_spot   DOUBLE PRECISION,
    cvd_end_1m_spot     DOUBLE PRECISION,
    cvd_diff_spot       DOUBLE PRECISION,
    cvd_norm_diff_spot  DOUBLE PRECISION,
    price_effect_z      DOUBLE PRECISION,
    cvd_effect_z        DOUBLE PRECISION,
    sig_pass            BOOLEAN,
    p_value_price       DOUBLE PRECISION,
    p_value_cvd         DOUBLE PRECISION,
    spot_price_flow_confirm BOOLEAN,
    fut_divergence_sign SMALLINT,
    spot_lead_score     DOUBLE PRECISION,
    likely_driver       TEXT,
    score               DOUBLE PRECISION,
    confidence          DOUBLE PRECISION,
    window_code         TEXT,
    param_set_id        UUID,
    calc_version        TEXT,
    payload_json        JSONB NOT NULL DEFAULT '{}'::jsonb
);
ALTER TABLE IF EXISTS evt.divergence_event
    ADD COLUMN IF NOT EXISTS pivot_ts_1 TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS pivot_ts_2 TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS pivot_confirm_ts_1 TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS pivot_confirm_ts_2 TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS event_available_ts TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS price_norm_diff DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS cvd_norm_diff_fut DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS cvd_norm_diff_spot DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS price_effect_z DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS cvd_effect_z DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS sig_pass BOOLEAN,
    ADD COLUMN IF NOT EXISTS p_value_price DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS p_value_cvd DOUBLE PRECISION;
SELECT create_hypertable('evt.divergence_event', 'ts_event_start', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_divergence_event_lookup ON evt.divergence_event(symbol, divergence_type, ts_event_start DESC);
CREATE INDEX IF NOT EXISTS idx_divergence_event_driver ON evt.divergence_event(likely_driver, ts_event_start DESC);
CREATE INDEX IF NOT EXISTS idx_divergence_event_available_ts ON evt.divergence_event(symbol, event_available_ts DESC);
CREATE INDEX IF NOT EXISTS idx_divergence_event_sig_pass ON evt.divergence_event(sig_pass, ts_event_start DESC);
\endif

-- 8.6 Liquidation density outputs (price-level matrix + peaks)
CREATE TABLE IF NOT EXISTS feat.liquidation_density_level (
    ts_snapshot        TIMESTAMPTZ NOT NULL,
    window_code        TEXT NOT NULL,
    venue              TEXT NOT NULL DEFAULT 'binance',
    symbol             TEXT NOT NULL,
    price_level        DOUBLE PRECISION NOT NULL,
    long_liq_density   DOUBLE PRECISION,
    short_liq_density  DOUBLE PRECISION,
    net_liq_density    DOUBLE PRECISION,
    is_long_peak       BOOLEAN,
    is_short_peak      BOOLEAN,
    long_peak_score    DOUBLE PRECISION,
    short_peak_score   DOUBLE PRECISION,
    param_set_id       UUID,
    calc_version       TEXT,
    extra_json         JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT create_hypertable('feat.liquidation_density_level', 'ts_snapshot', if_not_exists => TRUE);
\if :is_orderflow
CREATE UNIQUE INDEX IF NOT EXISTS uq_liquidation_density_level_natural
ON feat.liquidation_density_level(ts_snapshot, venue, symbol, window_code, price_level);
\endif
CREATE INDEX IF NOT EXISTS idx_liq_density_lookup ON feat.liquidation_density_level(symbol, window_code, ts_snapshot DESC, price_level);

-- 9) (Optional but useful) materialized latest views for fast strategy reads -----
CREATE MATERIALIZED VIEW IF NOT EXISTS feat.latest_orderbook_feature_1s AS
SELECT DISTINCT ON (symbol)
    symbol, ts_bucket, bar_interval,
    obi_k_dw_twa_fut, ofi_norm_fut, exec_confirm_fut, spot_confirm,
    fake_order_risk_fut, cross_cvd_attribution,
    created_at
FROM feat.orderbook_feature
WHERE bar_interval = INTERVAL '1 second'
ORDER BY symbol, ts_bucket DESC;
CREATE UNIQUE INDEX IF NOT EXISTS uq_latest_orderbook_feature_1s_symbol ON feat.latest_orderbook_feature_1s(symbol);

-- 10) Seed indicator catalog (aligned to your indicator体系; JSON payload tables cover version drift)
INSERT INTO cfg.indicator_catalog (indicator_code, display_name, category, output_kind, primary_market, needs_spot_confirm, notes)
VALUES
('price_volume_structure', 'Price Volume Structure', 'structure', 'matrix+summary', 'futures', FALSE, 'Value Area/POC/HVN/LVN/Volume Dry-up'),
('footprint', 'Footprint', 'structure', 'matrix+summary', 'futures', FALSE, 'Footprint/Imbalances/Stacked Imbalances/UA'),
('divergence', 'CVD Divergence (event pack)', 'event', 'event', 'futures', TRUE, 'Futures price+futures CVD main detection with spot CVD attribution'),
('liquidation_density', 'Liquidation Density', 'risk', 'matrix+summary', 'futures', FALSE, 'Force order based density and peak zones'),
('orderbook_depth', 'Orderbook Depth Pack', 'orderbook', 'timeseries+matrix', 'futures', TRUE, 'Heatmap/OBI/Microprice/OFI/Burst/CVD divergence sidecar'),
('absorption', 'Absorption', 'event', 'event', 'futures', TRUE, 'Futures main + spot flow/whale confirm'),
('initiation', 'Initiation', 'event', 'event', 'futures', TRUE, 'Breakout initiation with spot confirmation'),
('bullish_absorption', 'Bullish Absorption', 'event', 'event', 'futures', FALSE, 'Derived from absorption'),
('bullish_initiation', 'Bullish Initiation', 'event', 'event', 'futures', FALSE, 'Derived from initiation'),
('bearish_absorption', 'Bearish Absorption', 'event', 'event', 'futures', FALSE, 'Derived from absorption'),
('bearish_initiation', 'Bearish Initiation', 'event', 'event', 'futures', FALSE, 'Derived from initiation'),
('buying_exhaustion', 'Buying Exhaustion', 'event', 'event', 'futures', TRUE, 'Add spot CVD + whale confirmation'),
('selling_exhaustion', 'Selling Exhaustion', 'event', 'event', 'futures', TRUE, 'Add spot CVD + whale confirmation'),
('cvd_pack', 'CVD Pack Rolling 7D', 'flow', 'timeseries+pack', 'mixed', TRUE, 'Dual-market CVD pack + attribution fields'),
('whale_trades', 'Whale Trades', 'flow', 'timeseries+event', 'mixed', TRUE, 'Spot/Futures split statistics'),
('funding_rate', 'Funding Rate', 'risk', 'timeseries', 'futures', FALSE, 'Funding + mark price'),
('vpin', 'VPIN', 'flow', 'timeseries', 'mixed', TRUE, 'VPIN_fut vs VPIN_spot toxicity diff'),
('avwap', 'AVWAP', 'anchor', 'timeseries', 'mixed', TRUE, 'Futures AVWAP + Spot AVWAP cash anchor'),
('kline_history', 'Kline History', 'context', 'timeseries+pack', 'mixed', FALSE, 'Multi-timeframe OHLCV history pack for snapshot/LLM context'),
('tpo_market_profile', 'TPO Market Profile', 'structure', 'timeseries+summary', 'futures', FALSE, 'Session-level TPO POC/VA/IB/single-prints'),
('rvwap_sigma_bands', 'RVWAP Sigma Bands', 'flow', 'timeseries+bands', 'futures', FALSE, 'Rolling VWAP with weighted sigma bands and z-score'),
('high_volume_pulse', 'High Volume Pulse', 'flow', 'timeseries+event', 'futures', FALSE, 'Rolling volume z-score and intrabar POC pulse'),
('ema_trend_regime', 'EMA Trend Regime', 'trend', 'timeseries', 'futures', FALSE, 'EMA13/21/34 band and 4h/1d EMA100/200 regime'),
('fvg', 'Fair Value Gap', 'structure', 'timeseries+zones', 'futures', FALSE, 'HTF fair value gap structure zones for 1h/4h/1d filtering')
ON CONFLICT (indicator_code) DO NOTHING;

-- 11) Seed instruments (ETH only, per your scope)
INSERT INTO cfg.instrument (venue, symbol, market, contract_type, quote_asset, base_asset, contract_multiplier, is_active)
VALUES
('binance', 'ETHUSDT', 'spot', NULL, 'USDT', 'ETH', 1.0, TRUE),
('binance', 'ETHUSDT', 'futures', 'perpetual', 'USDT', 'ETH', 1.0, TRUE)
ON CONFLICT (venue, symbol, market) DO NOTHING;

COMMIT;
