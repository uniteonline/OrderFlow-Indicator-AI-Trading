use crate::exchange::binance::rest::klines::BinanceKlineRow;
use crate::normalize::{
    parse_optional_bool, parse_optional_i64, parse_optional_str, parse_required_i64,
    parse_required_str, utc_from_millis, Market, NormalizedMdEvent,
};
use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use serde_json::{json, Value};

pub fn normalize(market: Market, stream: &str, data: &Value) -> Result<NormalizedMdEvent> {
    let kline = data
        .get("k")
        .ok_or_else(|| anyhow!("missing kline payload: k"))?;

    let symbol = parse_required_str(kline, "s")
        .or_else(|_| parse_required_str(data, "s"))
        .context("parse kline symbol")?
        .to_uppercase();
    let symbol_low = symbol.to_lowercase();

    let open_time = utc_from_millis(parse_required_i64(kline, "t")?)?;
    let close_time = utc_from_millis(parse_required_i64(kline, "T")?)?;
    let interval_code = parse_required_str(kline, "i")?;
    let ts_recv = Utc::now();
    let event_ts = match parse_optional_i64(data, "E") {
        Some(ms) => utc_from_millis(ms).unwrap_or(ts_recv),
        None => ts_recv,
    };

    let open_price = parse_required_str(kline, "o")?;
    let high_price = parse_required_str(kline, "h")?;
    let low_price = parse_required_str(kline, "l")?;
    let close_price = parse_required_str(kline, "c")?;
    let volume_base = parse_optional_str(kline, "v");
    let quote_volume = parse_optional_str(kline, "q");
    let trade_count = parse_optional_i64(kline, "n");
    let taker_buy_base = parse_optional_str(kline, "V");
    let taker_buy_quote = parse_optional_str(kline, "Q");
    let is_closed = parse_optional_bool(kline, "x").unwrap_or(false);

    let payload_json = json!({ "raw": data });

    let normalized = json!({
        "stream_name": stream,
        "ts_recv": ts_recv.to_rfc3339(),
        "interval_code": interval_code,
        "open_time": open_time.to_rfc3339(),
        "close_time": close_time.to_rfc3339(),
        "open_price": open_price,
        "high_price": high_price,
        "low_price": low_price,
        "close_price": close_price,
        "volume_base": volume_base,
        "quote_volume": quote_volume,
        "trade_count": trade_count,
        "taker_buy_base": taker_buy_base,
        "taker_buy_quote": taker_buy_quote,
        "is_closed": is_closed,
        "payload_json": payload_json,
    });

    Ok(NormalizedMdEvent {
        msg_type: "md.kline".to_string(),
        market: market.as_str().to_string(),
        symbol,
        source_kind: "ws".to_string(),
        backfill_in_progress: false,
        routing_key: format!(
            "md.{}.kline.{}.{}",
            market.as_str(),
            interval_code,
            symbol_low
        ),
        stream_name: stream.to_string(),
        event_ts,
        data: normalized,
    })
}

pub fn normalize_rest_kline(
    market: Market,
    symbol: &str,
    interval_code: &str,
    row: &BinanceKlineRow,
    source_kind: &str,
    backfill_in_progress: bool,
) -> Result<NormalizedMdEvent> {
    let symbol_up = symbol.to_uppercase();
    let symbol_low = symbol.to_lowercase();
    let open_time = utc_from_millis(row.open_time_ms)?;
    let close_time = utc_from_millis(row.close_time_ms)?;

    let data = json!({
        "stream_name": format!("kline_{}/rest_backfill", interval_code),
        "ts_recv": Utc::now().to_rfc3339(),
        "interval_code": interval_code,
        "open_time": open_time.to_rfc3339(),
        "close_time": close_time.to_rfc3339(),
        "open_price": row.open_price.clone(),
        "high_price": row.high_price.clone(),
        "low_price": row.low_price.clone(),
        "close_price": row.close_price.clone(),
        "volume_base": row.volume_base.clone(),
        "quote_volume": row.quote_volume.clone(),
        "trade_count": row.trade_count,
        "taker_buy_base": row.taker_buy_base.clone(),
        "taker_buy_quote": row.taker_buy_quote.clone(),
        "is_closed": true,
        "payload_json": {},
    });

    Ok(NormalizedMdEvent {
        msg_type: "md.kline".to_string(),
        market: market.as_str().to_string(),
        symbol: symbol_up,
        source_kind: source_kind.to_string(),
        backfill_in_progress,
        routing_key: format!(
            "md.{}.kline.{}.{}",
            market.as_str(),
            interval_code,
            symbol_low
        ),
        stream_name: format!("{}@kline_{}", symbol_low, interval_code),
        event_ts: close_time,
        data,
    })
}
