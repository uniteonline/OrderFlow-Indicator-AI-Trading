use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinancePremiumIndex {
    pub symbol: String,
    pub mark_price: Option<String>,
    pub index_price: Option<String>,
    pub estimated_settle_price: Option<String>,
    pub last_funding_rate: Option<String>,
    pub next_funding_time: Option<i64>,
    pub time: Option<i64>,
}
