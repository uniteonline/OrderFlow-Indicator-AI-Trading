use crate::normalize::NormalizedMdEvent;
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};

#[derive(Clone, Default)]
pub struct Kline3dAggregator {
    windows: HashMap<(String, String), VecDeque<OneDayBar>>,
}

impl Kline3dAggregator {
    pub fn on_kline(&mut self, event: &NormalizedMdEvent) -> Result<Option<NormalizedMdEvent>> {
        if event.msg_type != "md.kline" {
            return Ok(None);
        }

        let d = &event.data;
        let interval = required_str(d, "interval_code")?;
        let is_closed = required_bool(d, "is_closed")?;
        if interval != "1d" || !is_closed {
            return Ok(None);
        }

        let one_day = OneDayBar::from_data(d)?;
        let key = (event.market.clone(), event.symbol.clone());
        let cache = self.windows.entry(key.clone()).or_default();

        if cache
            .back()
            .map(|last| last.open_time == one_day.open_time)
            .unwrap_or(false)
        {
            cache.pop_back();
        }

        cache.push_back(one_day);
        while cache.len() > 3 {
            cache.pop_front();
        }

        if cache.len() < 3 {
            return Ok(None);
        }

        let bars: Vec<OneDayBar> = cache.iter().cloned().collect();
        let first = bars
            .first()
            .ok_or_else(|| anyhow!("missing first 1d bar"))?;
        let last = bars.last().ok_or_else(|| anyhow!("missing last 1d bar"))?;

        let high_price = bars
            .iter()
            .map(|b| b.high_price.parse::<f64>().unwrap_or(0.0))
            .fold(f64::MIN, f64::max);
        let low_price = bars
            .iter()
            .map(|b| b.low_price.parse::<f64>().unwrap_or(f64::MAX))
            .fold(f64::MAX, f64::min);
        let volume_base: f64 = bars
            .iter()
            .map(|b| b.volume_base.parse::<f64>().unwrap_or(0.0))
            .sum();
        let quote_volume: f64 = bars
            .iter()
            .map(|b| b.quote_volume.parse::<f64>().unwrap_or(0.0))
            .sum();
        let trade_count: i64 = bars.iter().map(|b| b.trade_count).sum();
        let taker_buy_base: f64 = bars
            .iter()
            .map(|b| b.taker_buy_base.parse::<f64>().unwrap_or(0.0))
            .sum();
        let taker_buy_quote: f64 = bars
            .iter()
            .map(|b| b.taker_buy_quote.parse::<f64>().unwrap_or(0.0))
            .sum();

        let symbol_low = event.symbol.to_lowercase();
        let stream_name = format!("{}_3d@derived", symbol_low);
        let data = json!({
            "stream_name": stream_name.clone(),
            "ts_recv": Utc::now().to_rfc3339(),
            "interval_code": "3d",
            "open_time": first.open_time.to_rfc3339(),
            "close_time": last.close_time.to_rfc3339(),
            "open_price": first.open_price.clone(),
            "high_price": format!("{:.8}", high_price),
            "low_price": format!("{:.8}", low_price),
            "close_price": last.close_price.clone(),
            "volume_base": format!("{:.8}", volume_base),
            "quote_volume": format!("{:.8}", quote_volume),
            "trade_count": trade_count,
            "taker_buy_base": format!("{:.8}", taker_buy_base),
            "taker_buy_quote": format!("{:.8}", taker_buy_quote),
            "is_closed": true,
            "payload_json": {
                "derived_from": "md.kline.1d",
                "window_days": 3
            },
        });

        Ok(Some(NormalizedMdEvent {
            msg_type: "md.kline".to_string(),
            market: event.market.clone(),
            symbol: event.symbol.clone(),
            source_kind: "derived".to_string(),
            backfill_in_progress: event.backfill_in_progress,
            routing_key: format!("md.{}.kline.3d.{}", event.market, symbol_low),
            stream_name: stream_name.to_string(),
            event_ts: last.close_time,
            data,
        }))
    }
}

#[derive(Clone)]
struct OneDayBar {
    open_time: DateTime<Utc>,
    close_time: DateTime<Utc>,
    open_price: String,
    high_price: String,
    low_price: String,
    close_price: String,
    volume_base: String,
    quote_volume: String,
    trade_count: i64,
    taker_buy_base: String,
    taker_buy_quote: String,
}

impl OneDayBar {
    fn from_data(d: &Value) -> Result<Self> {
        Ok(Self {
            open_time: parse_ts(required_str(d, "open_time")?.as_str())?,
            close_time: parse_ts(required_str(d, "close_time")?.as_str())?,
            open_price: required_str(d, "open_price")?,
            high_price: required_str(d, "high_price")?,
            low_price: required_str(d, "low_price")?,
            close_price: required_str(d, "close_price")?,
            volume_base: optional_str(d, "volume_base").unwrap_or_else(|| "0".to_string()),
            quote_volume: optional_str(d, "quote_volume").unwrap_or_else(|| "0".to_string()),
            trade_count: d.get("trade_count").and_then(Value::as_i64).unwrap_or(0),
            taker_buy_base: optional_str(d, "taker_buy_base").unwrap_or_else(|| "0".to_string()),
            taker_buy_quote: optional_str(d, "taker_buy_quote").unwrap_or_else(|| "0".to_string()),
        })
    }
}

fn required_str(v: &Value, key: &str) -> Result<String> {
    v.get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("missing string field: {}", key))
}

fn optional_str(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(Value::as_str).map(ToOwned::to_owned)
}

fn required_bool(v: &Value, key: &str) -> Result<bool> {
    v.get(key)
        .and_then(Value::as_bool)
        .ok_or_else(|| anyhow!("missing bool field: {}", key))
}

fn parse_ts(raw: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| anyhow!("invalid timestamp {}: {}", raw, e))
}
