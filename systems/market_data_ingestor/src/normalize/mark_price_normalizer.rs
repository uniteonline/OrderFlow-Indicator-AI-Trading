use crate::exchange::binance::rest::premium_index::BinancePremiumIndex;
use crate::normalize::{
    parse_optional_i64, parse_optional_str, parse_required_i64, parse_required_str,
    utc_from_millis, Market, NormalizedMdEvent,
};
use anyhow::Result;
use chrono::Utc;
use serde_json::{json, Value};

pub fn normalize(market: Market, stream: &str, data: &Value) -> Result<NormalizedMdEvent> {
    let symbol = parse_required_str(data, "s")?.to_uppercase();
    let symbol_low = symbol.to_lowercase();

    let event_ts = match parse_required_i64(data, "E") {
        Ok(ts_ms) => utc_from_millis(ts_ms)?,
        Err(_) => Utc::now(),
    };
    let ts_recv = Utc::now();

    let mark_price = parse_optional_str(data, "p");
    let index_price = parse_optional_str(data, "i");
    let estimated_settle_price = parse_optional_str(data, "P");
    let funding_rate = parse_optional_str(data, "r");
    let next_funding_time = parse_optional_i64(data, "T")
        .map(utc_from_millis)
        .transpose()?
        .map(|dt| dt.to_rfc3339());

    let payload_json = json!({ "raw": data });

    let normalized = json!({
        "stream_name": stream,
        "ts_recv": ts_recv.to_rfc3339(),
        "mark_price": mark_price,
        "index_price": index_price,
        "estimated_settle_price": estimated_settle_price,
        "funding_rate": funding_rate,
        "next_funding_time": next_funding_time,
        "payload_json": payload_json,
    });

    Ok(NormalizedMdEvent {
        msg_type: "md.mark_price".to_string(),
        market: market.as_str().to_string(),
        symbol,
        source_kind: "ws".to_string(),
        backfill_in_progress: false,
        routing_key: format!("md.{}.mark_price.{}", market.as_str(), symbol_low),
        stream_name: stream.to_string(),
        event_ts,
        data: normalized,
    })
}

pub fn normalize_premium_index_rest(
    symbol: &str,
    premium: &BinancePremiumIndex,
    source_kind: &str,
    backfill_in_progress: bool,
) -> Result<NormalizedMdEvent> {
    let symbol_up = symbol.to_uppercase();
    let symbol_low = symbol.to_lowercase();
    let event_ts = premium
        .time
        .map(utc_from_millis)
        .transpose()?
        .unwrap_or_else(Utc::now);
    let next_funding_time = premium
        .next_funding_time
        .map(utc_from_millis)
        .transpose()?
        .map(|dt| dt.to_rfc3339());

    let normalized = json!({
        "stream_name": "fapi/v1/premiumIndex",
        "ts_recv": Utc::now().to_rfc3339(),
        "mark_price": premium.mark_price.clone(),
        "index_price": premium.index_price.clone(),
        "estimated_settle_price": premium.estimated_settle_price.clone(),
        "funding_rate": premium.last_funding_rate.clone(),
        "next_funding_time": next_funding_time,
        "payload_json": {},
    });

    Ok(NormalizedMdEvent {
        msg_type: "md.mark_price".to_string(),
        market: "futures".to_string(),
        symbol: symbol_up,
        source_kind: source_kind.to_string(),
        backfill_in_progress,
        routing_key: format!("md.futures.mark_price.{}", symbol_low),
        stream_name: "fapi/v1/premiumIndex".to_string(),
        event_ts,
        data: normalized,
    })
}
