Trading-pair switching is now config-driven.

Use one global symbol in `config/config.yaml`:

```yaml
instrument:
  symbol: BTCUSDT
```

Each service can either define its own symbol explicitly or inherit from
`instrument.symbol`.

Supported placeholders in YAML string values:

- `{symbol}` -> upper-case symbol, for example `BTCUSDT`
- `{symbol_lower}` -> lower-case symbol, for example `btcusdt`

Recommended pattern:

```yaml
instrument:
  symbol: BTCUSDT

market_data:
  symbol: "{symbol}"

indicator:
  symbol: "{symbol}"
  snapshot_file_path: "/tmp/indicator_engine_{symbol}.json.gz"

llm:
  symbol: "{symbol}"

replayer:
  symbol: "{symbol}"

mq:
  queues:
    indicator_group_flow:
      bind:
        - exchange: x.md.live
          routing_key: "md.agg.*.trade.1m.{symbol_lower}"
```

Runtime behavior:

- `instrument.symbol` is required.
- If `market_data.symbol`, `indicator.symbol`, `llm.symbol`, or
  `replayer.symbol` is omitted, the loader falls back to `instrument.symbol`.
- Queue bindings and path-like config values can use placeholders directly.
- No `config.local.yaml` overlay is used.

Compatibility notes:

- Legacy payload and schema field names such as `qty_eth` are still accepted for
  backward compatibility.
- New generic payload aliases such as `qty_base`, `qty_base_total`,
  `delta_qty_base`, and `vpin_bucket_size_base` are now also supported.
