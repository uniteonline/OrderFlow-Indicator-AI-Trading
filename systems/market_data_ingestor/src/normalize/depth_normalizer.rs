use crate::normalize::{
    parse_optional_i64, parse_required_i64, parse_required_str, utc_from_millis, Market,
    NormalizedMdEvent,
};
use anyhow::Result;
use chrono::Utc;
use serde_json::{json, Value};

pub fn normalize(market: Market, stream: &str, data: &Value) -> Result<NormalizedMdEvent> {
    let symbol = parse_required_str(data, "s")?.to_uppercase();
    let symbol_low = symbol.to_lowercase();

    let event_ts_ms = parse_required_i64(data, "E").or_else(|_| parse_required_i64(data, "T"))?;
    let event_ts = utc_from_millis(event_ts_ms)?;
    let ts_recv = Utc::now();

    let first_update_id = parse_optional_i64(data, "U");
    let final_update_id = parse_optional_i64(data, "u");
    let prev_final_update_id = parse_optional_i64(data, "pu");

    let bids_delta = parse_price_qty_levels(data, "b");
    let asks_delta = parse_price_qty_levels(data, "a");
    let event_count = (bids_delta.len() + asks_delta.len()) as i64;

    let payload_json = json!({ "raw": data });

    let normalized = json!({
        "stream_name": stream,
        "ts_recv": ts_recv.to_rfc3339(),
        "first_update_id": first_update_id,
        "final_update_id": final_update_id,
        "prev_final_update_id": prev_final_update_id,
        "bids_delta": bids_delta,
        "asks_delta": asks_delta,
        "event_count": event_count,
        "payload_json": payload_json,
    });

    Ok(NormalizedMdEvent {
        msg_type: "md.depth".to_string(),
        market: market.as_str().to_string(),
        symbol,
        source_kind: "ws".to_string(),
        backfill_in_progress: false,
        routing_key: format!("md.{}.depth.{}", market.as_str(), symbol_low),
        stream_name: stream.to_string(),
        event_ts,
        data: normalized,
    })
}

fn parse_price_qty_levels(value: &Value, key: &str) -> Vec<(String, String)> {
    value
        .get(key)
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .filter_map(|row| {
                    let pair = row.as_array()?;
                    if pair.len() < 2 {
                        return None;
                    }
                    let price = pair[0].as_str()?.to_string();
                    let qty = pair[1].as_str()?.to_string();
                    Some((price, qty))
                })
                .collect()
        })
        .unwrap_or_default()
}
