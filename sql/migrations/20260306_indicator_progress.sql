BEGIN;

CREATE SCHEMA IF NOT EXISTS feat;

CREATE TABLE IF NOT EXISTS feat.indicator_progress (
    symbol text PRIMARY KEY,
    last_success_ts timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

COMMIT;
