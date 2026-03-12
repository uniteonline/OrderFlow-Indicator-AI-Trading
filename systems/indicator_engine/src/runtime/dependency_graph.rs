use std::collections::HashMap;

pub fn default_dependencies() -> HashMap<&'static str, Vec<&'static str>> {
    let mut g = HashMap::new();
    g.insert("bullish_absorption", vec!["absorption"]);
    g.insert("bearish_absorption", vec!["absorption"]);
    g.insert("bullish_initiation", vec!["initiation"]);
    g.insert("bearish_initiation", vec!["initiation"]);
    g.insert("buying_exhaustion", vec!["orderbook_depth", "cvd_pack"]);
    g.insert("selling_exhaustion", vec!["orderbook_depth", "cvd_pack"]);
    g
}
