use super::core_shared::{
    self, filter_avwap, filter_cvd_pack, filter_divergence, filter_ema_trend_regime,
    filter_event_indicator, filter_footprint, filter_funding_rate, filter_fvg,
    filter_kline_history, filter_liquidation_density, filter_orderbook_depth,
    filter_price_volume_structure, filter_rvwap_sigma_bands, filter_tpo_market_profile,
    resolve_reference_mark_price, FootprintMode, EVENT_INDICATOR_RULES,
};
use serde_json::{Map, Value};

const AVWAP_LIMITS: &[(&str, usize)] = &[("15m", 3), ("4h", 2), ("1d", 2)];
const FVG_WINDOWS: &[&str] = &["15m", "4h"];
const KLINE_LIMITS: &[(&str, usize)] = &[("15m", 16), ("4h", 15), ("1d", 10)];
const CVD_LIMITS: &[(&str, usize)] = &[("15m", 12), ("4h", 10), ("1d", 6)];

pub(super) fn filter_indicators(source: &Map<String, Value>) -> Map<String, Value> {
    let mut indicators = Map::new();

    for code in ["vpin", "whale_trades", "high_volume_pulse"] {
        core_shared::insert_full_indicator(&mut indicators, source, code);
    }

    core_shared::insert_filtered_indicator(
        &mut indicators,
        source,
        "tpo_market_profile",
        filter_tpo_market_profile,
    );
    core_shared::insert_filtered_indicator(
        &mut indicators,
        source,
        "price_volume_structure",
        filter_price_volume_structure,
    );
    core_shared::insert_filtered_indicator(&mut indicators, source, "fvg", |payload| {
        filter_fvg(payload, FVG_WINDOWS, 4)
    });
    core_shared::insert_filtered_indicator(&mut indicators, source, "avwap", |payload| {
        filter_avwap(payload, AVWAP_LIMITS)
    });
    core_shared::insert_filtered_indicator(
        &mut indicators,
        source,
        "rvwap_sigma_bands",
        filter_rvwap_sigma_bands,
    );
    core_shared::insert_filtered_indicator(&mut indicators, source, "footprint", |payload| {
        filter_footprint(payload, FootprintMode::Defensive)
    });
    core_shared::insert_filtered_indicator(&mut indicators, source, "kline_history", |payload| {
        filter_kline_history(payload, KLINE_LIMITS)
    });
    core_shared::insert_filtered_indicator(
        &mut indicators,
        source,
        "ema_trend_regime",
        filter_ema_trend_regime,
    );
    core_shared::insert_filtered_indicator(&mut indicators, source, "cvd_pack", |payload| {
        filter_cvd_pack(payload, CVD_LIMITS)
    });
    core_shared::insert_filtered_indicator(
        &mut indicators,
        source,
        "funding_rate",
        filter_funding_rate,
    );
    core_shared::insert_filtered_indicator(
        &mut indicators,
        source,
        "liquidation_density",
        filter_liquidation_density,
    );
    core_shared::insert_filtered_indicator(&mut indicators, source, "orderbook_depth", |payload| {
        filter_orderbook_depth(payload, resolve_reference_mark_price(source), 60)
    });

    for (code, keep_last) in EVENT_INDICATOR_RULES {
        core_shared::insert_filtered_indicator(&mut indicators, source, code, |payload| {
            filter_event_indicator(payload, *keep_last)
        });
    }

    core_shared::insert_filtered_indicator(
        &mut indicators,
        source,
        "divergence",
        filter_divergence,
    );

    indicators
}
