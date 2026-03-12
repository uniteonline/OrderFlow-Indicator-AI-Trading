ALTER TABLE IF EXISTS evt.absorption_event
    ADD COLUMN IF NOT EXISTS trigger_side TEXT;

UPDATE evt.absorption_event
SET trigger_side = CASE
        WHEN direction > 0 THEN 'sell'
        WHEN direction < 0 THEN 'buy'
        ELSE NULL
    END
WHERE trigger_side IS NULL;

UPDATE evt.absorption_event
SET payload_json = jsonb_set(
        COALESCE(payload_json, '{}'::jsonb),
        '{trigger_side}',
        to_jsonb(trigger_side),
        TRUE
    )
WHERE trigger_side IS NOT NULL
  AND (payload_json ->> 'trigger_side') IS DISTINCT FROM trigger_side;
