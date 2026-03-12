BEGIN;

CREATE UNIQUE INDEX IF NOT EXISTS uq_trade_flow_feature_natural
ON feat.trade_flow_feature(venue, market, symbol, bar_interval, ts_bucket);

CREATE UNIQUE INDEX IF NOT EXISTS uq_orderbook_feature_natural
ON feat.orderbook_feature(venue, symbol, bar_interval, ts_bucket);

CREATE UNIQUE INDEX IF NOT EXISTS uq_avwap_feature_natural
ON feat.avwap_feature(venue, symbol, anchor_ts, bar_interval, ts_bucket);

CREATE UNIQUE INDEX IF NOT EXISTS uq_funding_feature_natural
ON feat.funding_feature(venue, symbol, bar_interval, ts_bucket);

CREATE UNIQUE INDEX IF NOT EXISTS uq_whale_trade_rollup_natural
ON feat.whale_trade_rollup(venue, market, symbol, bar_interval, threshold_usdt, ts_bucket);

COMMIT;
