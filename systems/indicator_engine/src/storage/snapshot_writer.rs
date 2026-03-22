use crate::indicators::context::IndicatorSnapshotRow;
use crate::publish::ind_publisher::OutboxMessage;
use anyhow::{Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde_json::{json, Map, Value};
use sqlx::{PgPool, Postgres, QueryBuilder, Transaction};
use std::time::Instant;
use tracing::warn;

const OUTBOX_PROGRESS_TX_WARN_MS: u128 = 1_000;

#[derive(Clone)]
pub struct SnapshotWriter {
    pool: PgPool,
}

impl SnapshotWriter {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn write_snapshots(
        &self,
        ts_bucket: DateTime<Utc>,
        symbol: &str,
        rows: &[IndicatorSnapshotRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        // One snapshot row per indicator per minute; do not expand by_window /
        // series_by_window into extra rows, which would multiply storage.
        let mut ts_snapshots: Vec<DateTime<Utc>> = Vec::new();
        let mut bar_intervals: Vec<String> = Vec::new();
        let mut symbols: Vec<String> = Vec::new();
        let mut indicator_codes: Vec<String> = Vec::new();
        let mut window_codes: Vec<String> = Vec::new();
        let mut primary_markets: Vec<Option<String>> = Vec::new();
        let mut payload_jsons: Vec<Value> = Vec::new();

        for row in rows {
            let primary_market = primary_market_for_code(row.indicator_code);
            let bar_interval = interval_text_by_window(row.window_code);
            ts_snapshots.push(ts_bucket);
            bar_intervals.push(bar_interval);
            symbols.push(symbol.to_string());
            indicator_codes.push(row.indicator_code.to_string());
            window_codes.push(row.window_code.to_string());
            primary_markets.push(primary_market.map(|s| s.to_string()));
            payload_jsons.push(compact_snapshot_payload(
                row.indicator_code,
                &row.payload_json,
            ));
        }

        if ts_snapshots.is_empty() {
            return Ok(());
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .context("begin indicator snapshot tx")?;

        // Single round-trip: batch all rows with UNNEST.
        sqlx::query(
            r#"
            INSERT INTO feat.indicator_snapshot (
                ts_snapshot, bar_interval, symbol, market_scope,
                indicator_code, window_code, primary_market,
                calc_version, payload_json
            )
            SELECT
                ts, bi::interval, sym, 'futures_primary',
                ic, wc,
                CASE WHEN pm IS NULL THEN NULL ELSE pm::cfg.market_type END,
                'indicator_engine.v1', pj
            FROM UNNEST(
                $1::timestamptz[],
                $2::text[],
                $3::text[],
                $4::text[],
                $5::text[],
                $6::text[],
                $7::jsonb[]
            ) AS t(ts, bi, sym, ic, wc, pm, pj)
            ON CONFLICT (ts_snapshot, symbol, indicator_code, window_code)
            DO UPDATE SET
                bar_interval = EXCLUDED.bar_interval,
                market_scope = EXCLUDED.market_scope,
                primary_market = EXCLUDED.primary_market,
                calc_version = EXCLUDED.calc_version,
                payload_json = EXCLUDED.payload_json
            "#,
        )
        .bind(&ts_snapshots)
        .bind(&bar_intervals)
        .bind(&symbols)
        .bind(&indicator_codes)
        .bind(&window_codes)
        .bind(&primary_markets)
        .bind(&payload_jsons)
        .execute(&mut *tx)
        .await
        .with_context(|| {
            format!(
                "batch insert indicator_snapshot: {} rows, symbol={}",
                ts_snapshots.len(),
                symbol
            )
        })?;

        tx.commit().await.context("commit indicator snapshot tx")?;

        Ok(())
    }

    pub async fn advance_progress(&self, symbol: &str, ts_bucket: DateTime<Utc>) -> Result<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("begin indicator progress tx")?;
        upsert_indicator_progress(&mut tx, symbol, ts_bucket).await?;
        tx.commit().await.context("commit indicator progress tx")?;
        Ok(())
    }

    pub async fn advance_progress_with_outbox(
        &self,
        symbol: &str,
        ts_bucket: DateTime<Utc>,
        messages: &[OutboxMessage],
    ) -> Result<()> {
        let total_started_at = Instant::now();
        let begin_started_at = Instant::now();
        let mut tx = self
            .pool
            .begin()
            .await
            .context("begin indicator progress+outbox tx")?;
        let begin_ms = begin_started_at.elapsed().as_millis();
        let enqueue_started_at = Instant::now();
        enqueue_outbox_batch_in_tx(&mut tx, messages).await?;
        let enqueue_ms = enqueue_started_at.elapsed().as_millis();
        let progress_started_at = Instant::now();
        upsert_indicator_progress(&mut tx, symbol, ts_bucket).await?;
        let progress_ms = progress_started_at.elapsed().as_millis();
        let commit_started_at = Instant::now();
        tx.commit()
            .await
            .context("commit indicator progress+outbox tx")?;
        let commit_ms = commit_started_at.elapsed().as_millis();
        let total_ms = total_started_at.elapsed().as_millis();
        if total_ms >= OUTBOX_PROGRESS_TX_WARN_MS {
            warn!(
                symbol = symbol,
                ts_bucket = %ts_bucket,
                message_count = messages.len(),
                begin_ms = begin_ms,
                enqueue_ms = enqueue_ms,
                progress_ms = progress_ms,
                commit_ms = commit_ms,
                total_ms = total_ms,
                "slow indicator progress+outbox transaction"
            );
        }
        Ok(())
    }

    pub async fn rewind_progress(&self, symbol: &str, ts_bucket: DateTime<Utc>) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO feat.indicator_progress (symbol, last_success_ts)
            VALUES ($1, $2)
            ON CONFLICT (symbol)
            DO UPDATE SET
                last_success_ts = EXCLUDED.last_success_ts,
                updated_at = now()
            "#,
        )
        .bind(symbol.to_uppercase())
        .bind(ts_bucket)
        .execute(&self.pool)
        .await
        .context("rewind feat.indicator_progress")?;
        Ok(())
    }

    pub async fn rewind_persisted_tail(
        &self,
        symbol: &str,
        repair_start_ts: DateTime<Utc>,
        exchange_name: &str,
    ) -> Result<()> {
        let rewind_target_ts = repair_start_ts - ChronoDuration::minutes(1);
        let symbol_upper = symbol.to_uppercase();
        let mut tx = self
            .pool
            .begin()
            .await
            .context("begin indicator persisted tail rewind tx")?;

        sqlx::query(
            r#"
            DELETE FROM ops.outbox_event
            WHERE exchange_name = $1
              AND upper(payload_json->>'symbol') = $2
              AND COALESCE(
                    NULLIF(payload_json->>'ts_bucket', '')::timestamptz,
                    NULLIF(payload_json->>'event_ts', '')::timestamptz
                  ) >= $3
            "#,
        )
        .bind(exchange_name)
        .bind(&symbol_upper)
        .bind(repair_start_ts)
        .execute(&mut *tx)
        .await
        .context("delete indicator outbox tail for overlap repair")?;

        sqlx::query(
            r#"
            DELETE FROM feat.indicator_snapshot
            WHERE symbol = $1
              AND ts_snapshot >= $2
            "#,
        )
        .bind(&symbol_upper)
        .bind(repair_start_ts)
        .execute(&mut *tx)
        .await
        .context("delete feat.indicator_snapshot tail for overlap repair")?;

        for (table, ts_col) in [
            ("feat.indicator_level_value", "ts_snapshot"),
            ("feat.liquidation_density_level", "ts_snapshot"),
            ("feat.trade_flow_feature", "ts_bucket"),
            ("feat.orderbook_feature", "ts_bucket"),
            ("feat.funding_feature", "ts_bucket"),
            ("feat.avwap_feature", "ts_bucket"),
            ("feat.cvd_pack", "ts_bucket"),
            ("feat.whale_trade_rollup", "ts_bucket"),
            ("feat.funding_change_event", "ts_change"),
        ] {
            let query = format!("DELETE FROM {table} WHERE symbol = $1 AND {ts_col} >= $2");
            sqlx::query(&query)
                .bind(&symbol_upper)
                .bind(repair_start_ts)
                .execute(&mut *tx)
                .await
                .with_context(|| format!("delete {table} tail for overlap repair"))?;
        }

        for table in [
            "evt.indicator_event",
            "evt.divergence_event",
            "evt.absorption_event",
            "evt.initiation_event",
            "evt.exhaustion_event",
        ] {
            let query = format!(
                "DELETE FROM {table} WHERE symbol = $1 AND COALESCE(event_available_ts, ts_event_end, ts_event_start) >= $2"
            );
            sqlx::query(&query)
                .bind(&symbol_upper)
                .bind(repair_start_ts)
                .execute(&mut *tx)
                .await
                .with_context(|| format!("delete {table} tail for overlap repair"))?;
        }

        set_indicator_progress_exact(&mut tx, &symbol_upper, rewind_target_ts).await?;
        tx.commit()
            .await
            .context("commit indicator persisted tail rewind tx")?;
        Ok(())
    }
}

async fn enqueue_outbox_batch_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    messages: &[OutboxMessage],
) -> Result<()> {
    if messages.is_empty() {
        return Ok(());
    }

    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        INSERT INTO ops.outbox_event (
            exchange_name, routing_key, message_id, schema_version, headers_json, payload_json
        )
        "#,
    );

    builder.push_values(messages, |mut b, message| {
        b.push_bind(&message.exchange_name)
            .push_bind(&message.routing_key)
            .push_bind(message.message_id)
            .push_bind(message.schema_version)
            .push_bind(&message.headers_json)
            .push_bind(&message.payload_json);
    });

    builder.push(" ON CONFLICT DO NOTHING");
    builder
        .build()
        .execute(tx.as_mut())
        .await
        .with_context(|| {
            format!(
                "batch insert indicator outbox messages count={}",
                messages.len()
            )
        })?;

    Ok(())
}

fn interval_text_by_window(window_code: &str) -> String {
    match window_code {
        "15m" => "15 minutes".to_string(),
        "1h" => "1 hour".to_string(),
        "4h" => "4 hours".to_string(),
        "1d" => "1 day".to_string(),
        "3d" => "3 days".to_string(),
        _ => "1 minute".to_string(),
    }
}

fn primary_market_for_code(code: &str) -> Option<&'static str> {
    match code {
        "cvd_pack" | "whale_trades" | "vpin" | "avwap" => None,
        _ => Some("futures"),
    }
}

async fn upsert_indicator_progress(
    tx: &mut Transaction<'_, Postgres>,
    symbol: &str,
    ts_bucket: DateTime<Utc>,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO feat.indicator_progress (symbol, last_success_ts)
        VALUES ($1, $2)
        ON CONFLICT (symbol)
        DO UPDATE SET
            last_success_ts = GREATEST(feat.indicator_progress.last_success_ts, EXCLUDED.last_success_ts),
            updated_at = now()
        "#,
    )
    .bind(symbol.to_uppercase())
    .bind(ts_bucket)
    .execute(&mut **tx)
    .await
    .context("upsert feat.indicator_progress")?;
    Ok(())
}

async fn set_indicator_progress_exact(
    tx: &mut Transaction<'_, Postgres>,
    symbol: &str,
    ts_bucket: DateTime<Utc>,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO feat.indicator_progress (symbol, last_success_ts)
        VALUES ($1, $2)
        ON CONFLICT (symbol)
        DO UPDATE SET
            last_success_ts = EXCLUDED.last_success_ts,
            updated_at = now()
        "#,
    )
    .bind(symbol)
    .bind(ts_bucket)
    .execute(&mut **tx)
    .await
    .context("set feat.indicator_progress exact")?;
    Ok(())
}

fn compact_snapshot_payload(indicator_code: &str, payload: &Value) -> Value {
    let targeted = match indicator_code {
        "price_volume_structure" => compact_pvs_payload(payload),
        "footprint" => compact_footprint_payload(payload),
        "orderbook_depth" => compact_orderbook_depth_payload(payload),
        "kline_history" => compact_kline_history_payload(payload),
        _ => payload.clone(),
    };
    compact_payload_value(None, &targeted)
}

fn compact_pvs_payload(payload: &Value) -> Value {
    let Some(obj) = payload.as_object() else {
        return payload.clone();
    };
    let mut out = obj.clone();
    if let Some(levels) = out.remove("levels").and_then(|v| v.as_array().cloned()) {
        out.insert("levels_count".to_string(), json!(levels.len()));
    }
    if let Some(levels) = out
        .remove("value_area_levels")
        .and_then(|v| v.as_array().cloned())
    {
        out.insert("value_area_levels_count".to_string(), json!(levels.len()));
    }
    if let Some(by_window) = out.remove("by_window") {
        out.insert("by_window".to_string(), compact_pvs_by_window(&by_window));
    }
    Value::Object(out)
}

fn compact_pvs_by_window(value: &Value) -> Value {
    let Some(obj) = value.as_object() else {
        return value.clone();
    };
    let mut out = Map::new();
    for (window_code, window_value) in obj {
        let Some(window_obj) = window_value.as_object() else {
            out.insert(window_code.clone(), window_value.clone());
            continue;
        };
        let mut compacted = window_obj.clone();
        if let Some(levels) = compacted
            .remove("levels")
            .and_then(|v| v.as_array().cloned())
        {
            compacted.insert("levels_count".to_string(), json!(levels.len()));
        }
        if let Some(levels) = compacted
            .remove("value_area_levels")
            .and_then(|v| v.as_array().cloned())
        {
            compacted.insert("value_area_levels_count".to_string(), json!(levels.len()));
        }
        out.insert(window_code.clone(), Value::Object(compacted));
    }
    Value::Object(out)
}

fn compact_footprint_payload(payload: &Value) -> Value {
    let Some(obj) = payload.as_object() else {
        return payload.clone();
    };
    let mut out = obj.clone();
    if let Some(levels) = out.remove("levels").and_then(|v| v.as_array().cloned()) {
        out.insert("levels_count".to_string(), json!(levels.len()));
    }
    if let Some(by_window) = out.remove("by_window") {
        out.insert(
            "by_window".to_string(),
            compact_levels_only_by_window(&by_window),
        );
    }
    Value::Object(out)
}

fn compact_orderbook_depth_payload(payload: &Value) -> Value {
    let Some(obj) = payload.as_object() else {
        return payload.clone();
    };
    let mut out = obj.clone();
    if let Some(levels) = out.remove("levels").and_then(|v| v.as_array().cloned()) {
        out.insert("levels_count".to_string(), json!(levels.len()));
    }
    Value::Object(out)
}

fn compact_kline_history_payload(payload: &Value) -> Value {
    let Some(obj) = payload.as_object() else {
        return payload.clone();
    };
    let mut out = Map::new();
    for (key, value) in obj {
        if key != "intervals" {
            out.insert(key.clone(), value.clone());
            continue;
        }
        let Some(intervals_obj) = value.as_object() else {
            out.insert(key.clone(), value.clone());
            continue;
        };
        let mut compact_intervals = Map::new();
        for (interval_code, interval_value) in intervals_obj {
            let Some(interval_obj) = interval_value.as_object() else {
                compact_intervals.insert(interval_code.clone(), interval_value.clone());
                continue;
            };
            let mut compact_interval = Map::new();
            for (interval_key, interval_field) in interval_obj {
                if interval_key != "markets" {
                    compact_interval.insert(interval_key.clone(), interval_field.clone());
                    continue;
                }
                let Some(markets_obj) = interval_field.as_object() else {
                    compact_interval.insert(interval_key.clone(), interval_field.clone());
                    continue;
                };
                let mut compact_markets = Map::new();
                for (market_code, market_value) in markets_obj {
                    let Some(market_obj) = market_value.as_object() else {
                        compact_markets.insert(market_code.clone(), market_value.clone());
                        continue;
                    };
                    let mut compact_market = market_obj.clone();
                    if let Some(bars) = compact_market
                        .remove("bars")
                        .and_then(|v| v.as_array().cloned())
                    {
                        compact_market.insert("bars_count".to_string(), json!(bars.len()));
                        compact_market.insert(
                            "latest_bar".to_string(),
                            bars.last().cloned().unwrap_or(Value::Null),
                        );
                    }
                    compact_markets.insert(market_code.clone(), Value::Object(compact_market));
                }
                compact_interval.insert("markets".to_string(), Value::Object(compact_markets));
            }
            compact_intervals.insert(interval_code.clone(), Value::Object(compact_interval));
        }
        out.insert("intervals".to_string(), Value::Object(compact_intervals));
    }
    Value::Object(out)
}

fn compact_levels_only_by_window(value: &Value) -> Value {
    let Some(obj) = value.as_object() else {
        return value.clone();
    };
    let mut out = Map::new();
    for (window_code, window_value) in obj {
        let Some(window_obj) = window_value.as_object() else {
            out.insert(window_code.clone(), window_value.clone());
            continue;
        };
        let mut compacted = window_obj.clone();
        if let Some(levels) = compacted
            .remove("levels")
            .and_then(|v| v.as_array().cloned())
        {
            compacted.insert("levels_count".to_string(), json!(levels.len()));
        }
        out.insert(window_code.clone(), Value::Object(compacted));
    }
    Value::Object(out)
}

fn compact_payload_value(parent_key: Option<&str>, value: &Value) -> Value {
    match value {
        Value::Object(obj) => {
            let mut out = Map::new();
            for (key, child) in obj {
                if let Some(summary) = compact_array_field(parent_key, key, child) {
                    match summary {
                        CompactField::Replace(replacement) => {
                            out.insert(key.clone(), replacement);
                        }
                        CompactField::Expand(fields) => {
                            for (field_key, field_value) in fields {
                                out.insert(field_key, field_value);
                            }
                        }
                    }
                } else {
                    out.insert(
                        key.clone(),
                        compact_payload_value(Some(key.as_str()), child),
                    );
                }
            }
            Value::Object(out)
        }
        Value::Array(arr) => {
            if let Some(parent) = parent_key {
                if is_series_parent(parent) {
                    return compact_window_series_array(arr);
                }
            }
            Value::Array(arr.clone())
        }
        _ => value.clone(),
    }
}

enum CompactField {
    Replace(Value),
    Expand(Vec<(String, Value)>),
}

fn compact_array_field(parent_key: Option<&str>, key: &str, value: &Value) -> Option<CompactField> {
    let arr = value.as_array()?;
    if is_count_only_array_key(key) {
        return Some(CompactField::Expand(vec![(
            format!("{key}_count"),
            json!(arr.len()),
        )]));
    }
    if key == "bars" {
        return Some(CompactField::Expand(vec![
            ("bars_count".to_string(), json!(arr.len())),
            (
                "latest_bar".to_string(),
                arr.last().cloned().unwrap_or(Value::Null),
            ),
        ]));
    }
    if matches!(key, "series" | "changes") {
        return Some(CompactField::Expand(vec![
            (format!("{key}_count"), json!(arr.len())),
            (
                format!("latest_{}", singularize_array_key(key)),
                arr.last().cloned().unwrap_or(Value::Null),
            ),
        ]));
    }
    if parent_key.is_some_and(is_series_parent) {
        return Some(CompactField::Replace(compact_window_series_array(arr)));
    }
    None
}

fn compact_window_series_array(arr: &[Value]) -> Value {
    json!({
        "count": arr.len(),
        "latest_point": arr.last().cloned().unwrap_or(Value::Null),
    })
}

fn is_count_only_array_key(key: &str) -> bool {
    matches!(
        key,
        "levels"
            | "value_area_levels"
            | "hvn_levels"
            | "lvn_levels"
            | "peak_levels"
            | "tpo_single_print_zones"
    )
}

fn is_series_parent(key: &str) -> bool {
    matches!(
        key,
        "series_by_window"
            | "series_by_output_window"
            | "ffill_series_by_output_window"
            | "dev_series"
    )
}

fn singularize_array_key(key: &str) -> &'static str {
    match key {
        "series" => "point",
        "changes" => "change",
        _ => "item",
    }
}

#[cfg(test)]
mod tests {
    use super::compact_snapshot_payload;
    use serde_json::json;

    #[test]
    fn compacts_price_volume_structure_levels() {
        let payload = json!({
            "levels": [1, 2, 3],
            "value_area_levels": [4, 5],
            "by_window": {
                "15m": {
                    "levels": [1, 2],
                    "value_area_levels": [3]
                }
            }
        });
        let compacted = compact_snapshot_payload("price_volume_structure", &payload);
        assert_eq!(compacted["levels_count"], json!(3));
        assert_eq!(compacted["value_area_levels_count"], json!(2));
        assert!(compacted.get("levels").is_none());
        assert_eq!(compacted["by_window"]["15m"]["levels_count"], json!(2));
        assert_eq!(
            compacted["by_window"]["15m"]["value_area_levels_count"],
            json!(1)
        );
    }

    #[test]
    fn compacts_kline_history_bars() {
        let payload = json!({
            "intervals": {
                "1m": {
                    "interval_code": "1m",
                    "markets": {
                        "futures": {
                            "returned_count": 2,
                            "bars": [{"close": 1.0}, {"close": 2.0}]
                        }
                    }
                }
            }
        });
        let compacted = compact_snapshot_payload("kline_history", &payload);
        assert_eq!(
            compacted["intervals"]["1m"]["markets"]["futures"]["bars_count"],
            json!(2)
        );
        assert_eq!(
            compacted["intervals"]["1m"]["markets"]["futures"]["latest_bar"]["close"],
            json!(2.0)
        );
        assert!(compacted["intervals"]["1m"]["markets"]["futures"]
            .get("bars")
            .is_none());
    }

    #[test]
    fn compacts_footprint_levels() {
        let payload = json!({
            "levels": [1, 2, 3, 4],
            "by_window": {
                "15m": {
                    "levels": [1, 2]
                }
            }
        });
        let compacted = compact_snapshot_payload("footprint", &payload);
        assert_eq!(compacted["levels_count"], json!(4));
        assert_eq!(compacted["by_window"]["15m"]["levels_count"], json!(2));
        assert!(compacted.get("levels").is_none());
    }

    #[test]
    fn compacts_orderbook_depth_levels() {
        let payload = json!({
            "levels": [{"price": 1.0}, {"price": 2.0}],
            "obi": 0.5
        });
        let compacted = compact_snapshot_payload("orderbook_depth", &payload);
        assert_eq!(compacted["levels_count"], json!(2));
        assert_eq!(compacted["obi"], json!(0.5));
        assert!(compacted.get("levels").is_none());
    }

    #[test]
    fn compacts_series_and_changes_for_other_indicators() {
        let payload = json!({
            "changes": [{"ts": "a"}, {"ts": "b"}],
            "series_by_window": {
                "15m": [{"ts": "1"}, {"ts": "2"}]
            },
            "series_by_output_window": {
                "1h": [{"ts": "3"}]
            },
            "ffill_series_by_output_window": {
                "4h": [{"ts": "4"}, {"ts": "5"}]
            },
            "dev_series": {
                "1d": [{"ts": "6"}]
            }
        });

        let compacted = compact_snapshot_payload("avwap", &payload);
        assert_eq!(compacted["changes_count"], json!(2));
        assert_eq!(compacted["latest_change"]["ts"], json!("b"));
        assert_eq!(compacted["series_by_window"]["15m"]["count"], json!(2));
        assert_eq!(
            compacted["series_by_window"]["15m"]["latest_point"]["ts"],
            json!("2")
        );
        assert_eq!(
            compacted["series_by_output_window"]["1h"]["count"],
            json!(1)
        );
        assert_eq!(
            compacted["ffill_series_by_output_window"]["4h"]["latest_point"]["ts"],
            json!("5")
        );
        assert_eq!(compacted["dev_series"]["1d"]["count"], json!(1));
    }

    #[test]
    fn compacts_tpo_and_liq_level_arrays() {
        let payload = json!({
            "peak_levels": [1, 2, 3],
            "tpo_single_print_zones": [{"lo": 1.0}, {"lo": 2.0}],
        });

        let compacted = compact_snapshot_payload("tpo_market_profile", &payload);
        assert_eq!(compacted["peak_levels_count"], json!(3));
        assert_eq!(compacted["tpo_single_print_zones_count"], json!(2));
        assert!(compacted.get("peak_levels").is_none());
        assert!(compacted.get("tpo_single_print_zones").is_none());
    }
}
