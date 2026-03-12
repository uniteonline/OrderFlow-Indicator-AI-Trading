use crate::exchange::binance::rest::agg_trades::BinanceAggTrade;
use crate::normalize::{
    parse_optional_bool, parse_optional_i64, parse_required_i64, parse_required_str,
    price_times_qty, utc_from_millis, Market, NormalizedMdEvent,
};
use anyhow::Result;
use chrono::Utc;
use serde_json::{json, Value};

pub fn normalize(market: Market, stream: &str, data: &Value) -> Result<NormalizedMdEvent> {
    let symbol = parse_required_str(data, "s")?.to_uppercase();
    let symbol_low = symbol.to_lowercase();

    let event_ts_ms = parse_required_i64(data, "T").or_else(|_| parse_required_i64(data, "E"))?;
    let event_ts = utc_from_millis(event_ts_ms)?;
    let ts_recv = Utc::now();

    let price = parse_required_str(data, "p")?;
    let qty_raw = parse_required_str(data, "q")?;
    let qty_eth = qty_raw.clone();
    let notional_usdt = price_times_qty(&price, &qty_raw)?;

    let is_buyer_maker = parse_optional_bool(data, "m");
    let aggressor_side = if matches!(is_buyer_maker, Some(true)) {
        -1
    } else {
        1
    };

    let agg_trade_id = parse_optional_i64(data, "a");
    let first_trade_id = parse_optional_i64(data, "f");
    let last_trade_id = parse_optional_i64(data, "l");
    let exchange_trade_id = parse_optional_i64(data, "t").or(last_trade_id);
    let is_best_match = parse_optional_bool(data, "M");

    let payload_json = json!({ "raw": data });

    let normalized = json!({
        "stream_name": stream,
        "ts_recv": ts_recv.to_rfc3339(),
        "agg_trade_id": agg_trade_id,
        "exchange_trade_id": exchange_trade_id,
        "first_trade_id": first_trade_id,
        "last_trade_id": last_trade_id,
        "price": price,
        "qty_raw": qty_raw,
        "qty_eth": qty_eth,
        "notional_usdt": notional_usdt,
        "is_buyer_maker": is_buyer_maker,
        "aggressor_side": aggressor_side,
        "is_best_match": is_best_match,
        "payload_json": payload_json,
    });

    Ok(NormalizedMdEvent {
        msg_type: "md.trade".to_string(),
        market: market.as_str().to_string(),
        symbol,
        source_kind: "ws".to_string(),
        backfill_in_progress: false,
        routing_key: format!("md.{}.trade.{}", market.as_str(), symbol_low),
        stream_name: stream.to_string(),
        event_ts,
        data: normalized,
    })
}

pub fn normalize_rest_agg_trade(
    market: Market,
    symbol: &str,
    row: &BinanceAggTrade,
    source_kind: &str,
    backfill_in_progress: bool,
) -> Result<NormalizedMdEvent> {
    let symbol_up = symbol.to_uppercase();
    let symbol_low = symbol.to_lowercase();
    let event_ts = utc_from_millis(row.trade_time)?;
    let ts_recv = Utc::now();
    let notional_usdt = price_times_qty(&row.price, &row.qty)?;
    let aggressor_side = if matches!(row.is_buyer_maker, Some(true)) {
        -1
    } else {
        1
    };

    let normalized = json!({
        "stream_name": "aggTrades/rest_backfill",
        "ts_recv": ts_recv.to_rfc3339(),
        "agg_trade_id": row.agg_trade_id,
        "exchange_trade_id": row.last_trade_id,
        "first_trade_id": row.first_trade_id,
        "last_trade_id": row.last_trade_id,
        "price": row.price.clone(),
        "qty_raw": row.qty.clone(),
        "qty_eth": row.qty.clone(),
        "notional_usdt": notional_usdt,
        "is_buyer_maker": row.is_buyer_maker,
        "aggressor_side": aggressor_side,
        "is_best_match": row.is_best_match,
        "payload_json": {},
    });

    Ok(NormalizedMdEvent {
        msg_type: "md.trade".to_string(),
        market: market.as_str().to_string(),
        symbol: symbol_up,
        source_kind: source_kind.to_string(),
        backfill_in_progress,
        routing_key: format!("md.{}.trade.{}", market.as_str(), symbol_low),
        stream_name: "aggTrades/rest_backfill".to_string(),
        event_ts,
        data: normalized,
    })
}
