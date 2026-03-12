use crate::normalize::{
    parse_optional_i64, parse_optional_str, parse_required_i64, parse_required_str,
    price_times_qty, utc_from_millis, Market, NormalizedMdEvent,
};
use anyhow::{anyhow, Result};
use chrono::Utc;
use serde_json::{json, Value};

pub fn normalize(market: Market, stream: &str, data: &Value) -> Result<NormalizedMdEvent> {
    let order = data
        .get("o")
        .ok_or_else(|| anyhow!("missing forceOrder payload: o"))?;

    let symbol = parse_required_str(order, "s")?.to_uppercase();
    let symbol_low = symbol.to_lowercase();

    let event_ts_ms = parse_required_i64(data, "E").or_else(|_| parse_required_i64(order, "T"))?;
    let event_ts = utc_from_millis(event_ts_ms)?;
    let ts_recv = Utc::now();

    let order_side = normalize_order_side(parse_optional_str(order, "S").as_deref());
    let position_side = parse_optional_str(order, "ps");
    let order_type = parse_optional_str(order, "o");
    let tif = parse_optional_str(order, "f");
    let original_qty = parse_optional_str(order, "q");
    let price = parse_optional_str(order, "p");
    let average_price = parse_optional_str(order, "ap");
    let filled_qty = parse_optional_str(order, "z");
    let last_filled_qty = parse_optional_str(order, "l");
    let trade_time = parse_optional_i64(order, "T")
        .map(utc_from_millis)
        .transpose()?
        .map(|dt| dt.to_rfc3339());

    let notional_usdt = calc_notional_usdt(
        average_price.as_deref().or(price.as_deref()),
        filled_qty.as_deref().or(original_qty.as_deref()),
    );

    let liq_side = match order_side {
        "buy" => "short_liq",
        "sell" => "long_liq",
        _ => "unknown",
    };

    let payload_json = json!({ "raw": data });

    let normalized = json!({
        "stream_name": stream,
        "ts_recv": ts_recv.to_rfc3339(),
        "order_side": order_side,
        "position_side": position_side,
        "order_type": order_type,
        "tif": tif,
        "original_qty": original_qty,
        "price": price,
        "average_price": average_price,
        "filled_qty": filled_qty,
        "last_filled_qty": last_filled_qty,
        "trade_time": trade_time,
        "notional_usdt": notional_usdt,
        "liq_side": liq_side,
        "payload_json": payload_json,
    });

    Ok(NormalizedMdEvent {
        msg_type: "md.force_order".to_string(),
        market: market.as_str().to_string(),
        symbol,
        source_kind: "ws".to_string(),
        backfill_in_progress: false,
        routing_key: format!("md.{}.force_order.{}", market.as_str(), symbol_low),
        stream_name: stream.to_string(),
        event_ts,
        data: normalized,
    })
}

fn normalize_order_side(raw: Option<&str>) -> &'static str {
    match raw.unwrap_or("unknown").to_ascii_lowercase().as_str() {
        "buy" => "buy",
        "sell" => "sell",
        _ => "unknown",
    }
}

fn calc_notional_usdt(price: Option<&str>, qty: Option<&str>) -> Option<String> {
    match (price, qty) {
        (Some(p), Some(q)) => price_times_qty(p, q).ok(),
        _ => None,
    }
}
