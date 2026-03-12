pub fn required_streams(symbol: &str) -> Vec<String> {
    let s = symbol.to_lowercase();
    vec![
        format!("{}@aggTrade", s),
        format!("{}@depth@100ms", s),
        format!("{}@bookTicker", s),
        format!("{}@markPrice@1s", s),
        format!("{}@forceOrder", s),
        format!("{}@kline_1m", s),
        format!("{}@kline_15m", s),
        format!("{}@kline_1h", s),
        format!("{}@kline_4h", s),
        format!("{}@kline_1d", s),
    ]
}
