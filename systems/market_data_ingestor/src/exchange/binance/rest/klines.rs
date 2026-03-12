use anyhow::{anyhow, Result};
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct BinanceKlineRow {
    pub open_time_ms: i64,
    pub open_price: String,
    pub high_price: String,
    pub low_price: String,
    pub close_price: String,
    pub volume_base: String,
    pub close_time_ms: i64,
    pub quote_volume: String,
    pub trade_count: i64,
    pub taker_buy_base: String,
    pub taker_buy_quote: String,
}

impl BinanceKlineRow {
    pub fn from_row(row: &[Value]) -> Result<Self> {
        if row.len() < 11 {
            return Err(anyhow!("invalid kline row len: {}", row.len()));
        }

        Ok(Self {
            open_time_ms: as_i64(&row[0])?,
            open_price: as_str(&row[1])?,
            high_price: as_str(&row[2])?,
            low_price: as_str(&row[3])?,
            close_price: as_str(&row[4])?,
            volume_base: as_str(&row[5])?,
            close_time_ms: as_i64(&row[6])?,
            quote_volume: as_str(&row[7])?,
            trade_count: as_i64(&row[8])?,
            taker_buy_base: as_str(&row[9])?,
            taker_buy_quote: as_str(&row[10])?,
        })
    }
}

fn as_i64(v: &Value) -> Result<i64> {
    v.as_i64().ok_or_else(|| anyhow!("expected int, got {}", v))
}

fn as_str(v: &Value) -> Result<String> {
    v.as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("expected string, got {}", v))
}
