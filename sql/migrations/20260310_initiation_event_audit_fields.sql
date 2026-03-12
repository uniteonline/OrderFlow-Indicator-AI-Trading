ALTER TABLE IF EXISTS evt.initiation_event
    ADD COLUMN IF NOT EXISTS min_follow_required_minutes INTEGER,
    ADD COLUMN IF NOT EXISTS follow_through_delta_sum DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS follow_through_hold_ok BOOLEAN,
    ADD COLUMN IF NOT EXISTS follow_through_max_adverse_excursion_ticks DOUBLE PRECISION;

UPDATE evt.initiation_event
SET min_follow_required_minutes = COALESCE(
        min_follow_required_minutes,
        NULLIF(payload_json ->> 'min_follow_required_minutes', '')::INTEGER,
        5
    ),
    follow_through_delta_sum = COALESCE(
        follow_through_delta_sum,
        NULLIF(payload_json ->> 'follow_through_delta_sum', '')::DOUBLE PRECISION
    ),
    follow_through_hold_ok = COALESCE(
        follow_through_hold_ok,
        NULLIF(payload_json ->> 'follow_through_hold_ok', '')::BOOLEAN
    ),
    follow_through_max_adverse_excursion_ticks = COALESCE(
        follow_through_max_adverse_excursion_ticks,
        NULLIF(
            payload_json ->> 'follow_through_max_adverse_excursion_ticks',
            ''
        )::DOUBLE PRECISION
    );

UPDATE evt.initiation_event
SET payload_json = jsonb_set(
        jsonb_set(
            jsonb_set(
                jsonb_set(
                    COALESCE(payload_json, '{}'::jsonb),
                    '{min_follow_required_minutes}',
                    COALESCE(
                        to_jsonb(min_follow_required_minutes),
                        'null'::jsonb
                    ),
                    TRUE
                ),
                '{follow_through_delta_sum}',
                COALESCE(to_jsonb(follow_through_delta_sum), 'null'::jsonb),
                TRUE
            ),
            '{follow_through_hold_ok}',
            COALESCE(to_jsonb(follow_through_hold_ok), 'null'::jsonb),
            TRUE
        ),
        '{follow_through_max_adverse_excursion_ticks}',
        COALESCE(
            to_jsonb(follow_through_max_adverse_excursion_ticks),
            'null'::jsonb
        ),
        TRUE
    )
WHERE min_follow_required_minutes IS NOT NULL
   OR follow_through_delta_sum IS NOT NULL
   OR follow_through_hold_ok IS NOT NULL
   OR follow_through_max_adverse_excursion_ticks IS NOT NULL;
