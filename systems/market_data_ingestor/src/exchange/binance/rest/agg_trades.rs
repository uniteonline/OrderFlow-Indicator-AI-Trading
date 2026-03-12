use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceAggTrade {
    #[serde(rename = "a")]
    pub agg_trade_id: i64,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "q")]
    pub qty: String,
    #[serde(rename = "f")]
    pub first_trade_id: Option<i64>,
    #[serde(rename = "l")]
    pub last_trade_id: Option<i64>,
    #[serde(rename = "T")]
    pub trade_time: i64,
    #[serde(rename = "m")]
    pub is_buyer_maker: Option<bool>,
    #[serde(rename = "M")]
    pub is_best_match: Option<bool>,
}
