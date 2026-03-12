INSERT INTO cfg.indicator_catalog (
    indicator_code,
    display_name,
    category,
    output_kind,
    primary_market,
    needs_spot_confirm,
    notes
)
VALUES (
    'fvg',
    'Fair Value Gap',
    'structure',
    'timeseries+zones',
    'futures',
    FALSE,
    'HTF fair value gap structure zones for 1h/4h/1d filtering'
)
ON CONFLICT (indicator_code) DO NOTHING;
