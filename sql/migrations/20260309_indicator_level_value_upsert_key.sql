CREATE UNIQUE INDEX IF NOT EXISTS uq_indicator_level_value_natural
ON feat.indicator_level_value(
    ts_snapshot, venue, symbol, indicator_code, window_code, market_scope, price_level
);
