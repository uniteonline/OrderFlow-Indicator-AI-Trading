use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceFundingRateRecord {
    pub symbol: String,
    pub funding_rate: String,
    pub funding_time: i64,
    pub mark_price: Option<String>,
}
