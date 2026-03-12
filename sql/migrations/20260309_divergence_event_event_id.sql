ALTER TABLE IF EXISTS evt.divergence_event
    ADD COLUMN IF NOT EXISTS event_id TEXT,
    ADD COLUMN IF NOT EXISTS event_available_ts TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS pivot_ts_1 TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS pivot_ts_2 TIMESTAMPTZ;

UPDATE evt.divergence_event
SET event_id = concat_ws(
        ':',
        upper(coalesce(symbol, '')),
        'divergence',
        coalesce(divergence_type, 'na'),
        coalesce(pivot_side, 'na'),
        (extract(epoch FROM ts_event_start) * 1000)::bigint::text,
        (extract(epoch FROM ts_event_end) * 1000)::bigint::text,
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
    FROM evt.divergence_event
)
DELETE FROM evt.divergence_event AS target
USING ranked
WHERE target.ctid = ranked.ctid
  AND ranked.rn > 1;

ALTER TABLE IF EXISTS evt.divergence_event
    ALTER COLUMN event_id SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_divergence_event_event_id
ON evt.divergence_event(event_id, ts_event_start);
