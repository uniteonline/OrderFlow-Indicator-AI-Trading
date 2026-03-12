CREATE UNIQUE INDEX IF NOT EXISTS uq_liquidation_density_level_natural
ON feat.liquidation_density_level(ts_snapshot, venue, symbol, window_code, price_level);
