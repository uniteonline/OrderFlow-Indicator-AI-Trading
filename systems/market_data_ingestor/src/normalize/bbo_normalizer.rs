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

    let event_ts = match parse_required_i64(data, "E").or_else(|_| parse_required_i64(data, "T")) {
        Ok(ts_ms) => utc_from_millis(ts_ms)?,
        Err(_) => Utc::now(),
    };
    let ts_recv = Utc::now();

    let update_id = parse_optional_i64(data, "u");
    let bid_price = parse_required_str(data, "b")?;
    let bid_qty = parse_required_str(data, "B")?;
    let ask_price = parse_required_str(data, "a")?;
    let ask_qty = parse_required_str(data, "A")?;

    let payload_json = json!({ "raw": data });

    let normalized = json!({
        "stream_name": stream,
        "ts_recv": ts_recv.to_rfc3339(),
        "update_id": update_id,
        "bid_price": bid_price,
        "bid_qty": bid_qty,
        "ask_price": ask_price,
        "ask_qty": ask_qty,
        "payload_json": payload_json,
    });

    Ok(NormalizedMdEvent {
        msg_type: "md.bbo".to_string(),
        market: market.as_str().to_string(),
        symbol,
        source_kind: "ws".to_string(),
        backfill_in_progress: false,
        routing_key: format!("md.{}.bbo.{}", market.as_str(), symbol_low),
        stream_name: stream.to_string(),
        event_ts,
        data: normalized,
    })
}
