pub mod bbo_normalizer;
pub mod depth_normalizer;
pub mod force_order_normalizer;
pub mod funding_rate_normalizer;
pub mod kline_normalizer;
pub mod mark_price_normalizer;
pub mod trade_normalizer;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use serde_json::{json, Value};

#[derive(Debug, Clone, Copy)]
pub enum Market {
    Spot,
    Futures,
}

impl Market {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::Futures => "futures",
        }
    }
}

#[derive(Debug, Clone)]
pub struct NormalizedMdEvent {
    pub msg_type: String,
    pub market: String,
    pub symbol: String,
    pub source_kind: String,
    pub backfill_in_progress: bool,
    pub routing_key: String,
    pub stream_name: String,
    pub event_ts: DateTime<Utc>,
    pub data: Value,
}

pub fn normalize_ws_event(
    market: Market,
    stream: &str,
    data: &Value,
) -> Result<Option<NormalizedMdEvent>> {
    if stream.ends_with("@aggTrade") {
        return trade_normalizer::normalize(market, stream, data).map(Some);
    }
    if stream.contains("@depth") {
        return depth_normalizer::normalize(market, stream, data).map(Some);
    }
    if stream.ends_with("@bookTicker") {
        return bbo_normalizer::normalize(market, stream, data).map(Some);
    }
    if stream.contains("@kline_") {
        return kline_normalizer::normalize(market, stream, data).map(Some);
    }
    if stream.contains("@markPrice") {
        return mark_price_normalizer::normalize(market, stream, data).map(Some);
    }
    if stream.ends_with("@forceOrder") {
        return force_order_normalizer::normalize(market, stream, data).map(Some);
    }
    Ok(None)
}

pub fn normalize_orderbook_snapshot_l2(
    market: Market,
    symbol: &str,
    stream_name: &str,
    ts_snapshot: DateTime<Utc>,
    ts_recv: DateTime<Utc>,
    depth_levels: i64,
    last_update_id: Option<i64>,
    bids: Vec<(String, String)>,
    asks: Vec<(String, String)>,
    checksum: Option<String>,
    metadata: Value,
) -> NormalizedMdEvent {
    let symbol_up = symbol.to_uppercase();
    let symbol_low = symbol.to_lowercase();
    let data = json!({
        "stream_name": stream_name,
        "ts_recv": ts_recv.to_rfc3339(),
        "ts_snapshot": ts_snapshot.to_rfc3339(),
        "depth_levels": depth_levels,
        "last_update_id": last_update_id,
        "bids": bids,
        "asks": asks,
        "checksum": checksum,
        "metadata": metadata,
    });

    NormalizedMdEvent {
        msg_type: "md.orderbook_snapshot_l2".to_string(),
        market: market.as_str().to_string(),
        symbol: symbol_up,
        source_kind: "derived".to_string(),
        backfill_in_progress: false,
        routing_key: format!(
            "md.{}.orderbook_snapshot_l2.{}",
            market.as_str(),
            symbol_low
        ),
        stream_name: stream_name.to_string(),
        event_ts: ts_snapshot,
        data,
    }
}

pub fn normalize_funding_rate_rest(
    symbol: &str,
    funding_time_ms: i64,
    funding_rate: &str,
    mark_price: Option<&str>,
    next_funding_time_ms: Option<i64>,
    backfill_in_progress: bool,
) -> Result<NormalizedMdEvent> {
    let event_ts = utc_from_millis(funding_time_ms)?;
    let ts_recv = Utc::now();
    let symbol_up = symbol.to_uppercase();
    let symbol_low = symbol.to_lowercase();

    let next_time = next_funding_time_ms
        .map(utc_from_millis)
        .transpose()?
        .map(|t| t.to_rfc3339());

    let data = json!({
        "stream_name": "fapi/v1/fundingRate",
        "ts_recv": ts_recv.to_rfc3339(),
        "funding_time": event_ts.to_rfc3339(),
        "funding_rate": funding_rate,
        "mark_price": mark_price,
        "next_funding_time": next_time,
        "payload_json": {},
    });

    Ok(NormalizedMdEvent {
        msg_type: "md.funding_rate".to_string(),
        market: "futures".to_string(),
        symbol: symbol_up,
        source_kind: "rest".to_string(),
        backfill_in_progress,
        routing_key: format!("md.futures.funding_rate.{}", symbol_low),
        stream_name: "fapi/v1/fundingRate".to_string(),
        event_ts,
        data,
    })
}

pub fn parse_required_i64(value: &Value, key: &str) -> Result<i64> {
    value
        .get(key)
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow!("missing int field: {}", key))
}

pub fn parse_optional_i64(value: &Value, key: &str) -> Option<i64> {
    value.get(key).and_then(Value::as_i64)
}

pub fn parse_required_str(value: &Value, key: &str) -> Result<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("missing string field: {}", key))
}

pub fn parse_optional_str(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

pub fn parse_optional_bool(value: &Value, key: &str) -> Option<bool> {
    value.get(key).and_then(Value::as_bool)
}

pub fn utc_from_millis(ts_ms: i64) -> Result<DateTime<Utc>> {
    Utc.timestamp_millis_opt(ts_ms)
        .single()
        .ok_or_else(|| anyhow!("invalid epoch millis: {}", ts_ms))
}

pub fn price_times_qty(price: &str, qty: &str) -> Result<String> {
    let p: f64 = price
        .parse()
        .with_context(|| format!("invalid price: {}", price))?;
    let q: f64 = qty
        .parse()
        .with_context(|| format!("invalid qty: {}", qty))?;
    Ok(format!("{:.12}", p * q))
}
