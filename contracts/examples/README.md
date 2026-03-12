# Ingestor -> Indicator Examples (V1)

This folder contains 9 standard message examples.
Each file includes:
- `headers`: AMQP headers (validate with `ingestor_to_indicator_headers.v1.schema.json`)
- `body`: JSON payload (validate with `ingestor_to_indicator_contract.v1.schema.json`)

Files:
1. `01_md_trade_example.v1.json`
2. `02_md_depth_example.v1.json`
3. `03_md_bbo_example.v1.json`
4. `04_md_kline_example.v1.json`
5. `05_md_mark_price_example.v1.json`
6. `06_md_funding_rate_example.v1.json`
7. `07_md_force_order_example.v1.json`
8. `08_md_orderbook_snapshot_l2_example.v1.json`
9. `09_ind_minute_bundle_example.v1.json`

Consistency rule:
If a field exists in both `headers` and `body` (`schema_version`, `message_id`, `trace_id`, `source_kind`, `backfill_in_progress`, `routing_key`, `market`, `symbol`, `msg_type`, `published_at`), values must be equal.
