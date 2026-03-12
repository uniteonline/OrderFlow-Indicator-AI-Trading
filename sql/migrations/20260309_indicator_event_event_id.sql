ALTER TABLE IF EXISTS evt.indicator_event
    ADD COLUMN IF NOT EXISTS event_id TEXT,
    ADD COLUMN IF NOT EXISTS event_available_ts TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS pivot_ts_1 TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS pivot_ts_2 TIMESTAMPTZ;

UPDATE evt.indicator_event
SET event_id = concat_ws(
        ':',
        upper(coalesce(symbol, '')),
        coalesce(indicator_code, 'na'),
        coalesce(event_type, 'na'),
        (extract(epoch FROM ts_event_start) * 1000)::bigint::text,
        coalesce((extract(epoch FROM ts_event_end) * 1000)::bigint::text, 'na'),
        coalesce(direction, 0)::text,
        coalesce((extract(epoch FROM pivot_ts_1) * 1000)::bigint::text, 'na'),
        coalesce((extract(epoch FROM pivot_ts_2) * 1000)::bigint::text, 'na')
    )
WHERE event_id IS NULL
   OR btrim(event_id) = '';

WITH ranked AS (
    SELECT
        ctid,
        row_number() OVER (
            PARTITION BY event_id, ts_event_start
            ORDER BY
                ts_recorded DESC NULLS LAST,
                event_available_ts DESC NULLS LAST,
                ctid DESC
        ) AS rn
    FROM evt.indicator_event
)
DELETE FROM evt.indicator_event AS target
USING ranked
WHERE target.ctid = ranked.ctid
  AND ranked.rn > 1;

ALTER TABLE IF EXISTS evt.indicator_event
    ALTER COLUMN event_id SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_indicator_event_event_id
ON evt.indicator_event(event_id, ts_event_start);
