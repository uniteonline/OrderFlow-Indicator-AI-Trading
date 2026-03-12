use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceExchangeInfo {
    pub timezone: Option<String>,
    pub symbols: Vec<BinanceSymbolInfo>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceSymbolInfo {
    pub symbol: String,
    pub status: Option<String>,
    pub contract_type: Option<String>,
    pub base_asset: Option<String>,
    pub quote_asset: Option<String>,
    pub price_precision: Option<i32>,
    pub quantity_precision: Option<i32>,
    pub filters: Option<Vec<Value>>,
}
