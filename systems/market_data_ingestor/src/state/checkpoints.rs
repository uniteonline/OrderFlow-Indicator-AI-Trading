use crate::normalize::NormalizedMdEvent;
use crate::state::gap_detector::GapDetector;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sqlx::{FromRow, PgPool};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct CheckpointUpdate {
    pub last_update_id: Option<i64>,
    pub last_trade_id: Option<i64>,
    pub last_agg_trade_id: Option<i64>,
    pub checkpoint_json: Value,
}

#[derive(Debug, Clone, FromRow)]
pub struct CheckpointSeed {
    pub stream_name: String,
    pub source_kind: String,
    pub last_event_ts: Option<DateTime<Utc>>,
    pub last_update_id: Option<i64>,
    pub last_trade_id: Option<i64>,
    pub last_agg_trade_id: Option<i64>,
    pub checkpoint_json: Value,
}

pub fn build_checkpoint(event: &NormalizedMdEvent) -> CheckpointUpdate {
    let d = &event.data;
    let last_update_id = d
        .get("final_update_id")
        .and_then(Value::as_i64)
        .or_else(|| d.get("update_id").and_then(Value::as_i64));
    let last_trade_id = d
        .get("exchange_trade_id")
        .and_then(Value::as_i64)
        .or_else(|| d.get("last_trade_id").and_then(Value::as_i64));
    let last_agg_trade_id = d.get("agg_trade_id").and_then(Value::as_i64);

    let interval_code = d
        .get("interval_code")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let open_time_ms = d
        .get("open_time")
        .and_then(Value::as_str)
        .and_then(parse_rfc3339_millis);

    let checkpoint_json = json!({
        "msg_type": event.msg_type,
        "routing_key": event.routing_key,
        "ts": Utc::now().to_rfc3339(),
        "interval_code": interval_code,
        "open_time_ms": open_time_ms,
    });

    CheckpointUpdate {
        last_update_id,
        last_trade_id,
        last_agg_trade_id,
        checkpoint_json,
    }
}

pub async fn load_checkpoint_seeds(
    pool: &PgPool,
    market: &str,
    symbol: &str,
) -> Result<Vec<CheckpointSeed>> {
    sqlx::query_as(
        r#"
        SELECT
            stream_name,
            source_kind::text AS source_kind,
            last_event_ts,
            last_update_id,
            last_trade_id,
            last_agg_trade_id,
            checkpoint_json
        FROM ops.ingest_checkpoint
        WHERE venue = 'binance'
          AND market = $1::cfg.market_type
          AND symbol = $2
        "#,
    )
    .bind(market)
    .bind(symbol)
    .fetch_all(pool)
    .await
    .context("load ops.ingest_checkpoint seeds")
}

pub fn hydrate_runtime_state(
    market: &str,
    symbol: &str,
    seeds: &[CheckpointSeed],
    gap_detector: &mut GapDetector,
    last_closed_kline_open_ms: &mut HashMap<String, i64>,
) {
    for seed in seeds {
        let msg_type = seed.checkpoint_json.get("msg_type").and_then(Value::as_str);
        let ts = seed.last_event_ts.unwrap_or_else(Utc::now);

        if msg_type == Some("md.trade") {
            if let Some(id) = seed.last_agg_trade_id {
                gap_detector.seed_trade(market, symbol, id, ts);
            }
        }

        if matches!(msg_type, Some("md.depth" | "md.orderbook_snapshot_l2")) {
            if let Some(id) = seed.last_update_id {
                gap_detector.seed_depth(market, symbol, id, ts);
            }
        }

        if let Some((interval, open_time_ms)) = extract_kline_state(seed) {
            last_closed_kline_open_ms
                .entry(interval)
                .and_modify(|v| {
                    if open_time_ms > *v {
                        *v = open_time_ms;
                    }
                })
                .or_insert(open_time_ms);
        }
    }
}

fn extract_kline_state(seed: &CheckpointSeed) -> Option<(String, i64)> {
    let msg_type = seed
        .checkpoint_json
        .get("msg_type")
        .and_then(Value::as_str)?;
    if msg_type != "md.kline" {
        return None;
    }

    let interval = seed
        .checkpoint_json
        .get("interval_code")
        .and_then(Value::as_str)?
        .to_string();
    let open_time_ms = seed
        .checkpoint_json
        .get("open_time_ms")
        .and_then(Value::as_i64)?;

    Some((interval, open_time_ms))
}

fn parse_rfc3339_millis(raw: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|dt| dt.with_timezone(&Utc).timestamp_millis())
}
