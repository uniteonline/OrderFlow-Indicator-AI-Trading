use super::core_shared::{
    self, filter_avwap, filter_cvd_pack_entry_v3, filter_divergence, filter_ema_trend_regime,
    filter_event_indicator_entry_v3, filter_footprint_entry_v3, filter_funding_rate_entry_v3,
    filter_fvg, filter_kline_history, filter_liquidation_density_entry_v3,
    filter_orderbook_depth_entry_v3, filter_price_volume_structure_entry_v3,
    filter_rvwap_sigma_bands, filter_tpo_market_profile, resolve_reference_mark_price,
    CORE_WINDOWS_WITH_3D,
};
use serde_json::{Map, Value};

const AVWAP_LIMITS: &[(&str, usize)] = &[("15m", 5), ("4h", 3), ("1d", 2)];
const KLINE_LIMITS: &[(&str, usize)] = &[("15m", 20), ("4h", 12), ("1d", 8)];
const CVD_LIMITS: &[(&str, usize)] = &[("15m", 15), ("4h", 8), ("1d", 5)];
const ENTRY_EVENT_INDICATOR_RULES: &[(&str, usize)] = &[
    ("absorption", 10),
    ("buying_exhaustion", 10),
    ("selling_exhaustion", 10),
    ("initiation", 10),
    ("bullish_absorption", 10),
    ("bearish_absorption", 10),
    ("bullish_initiation", 10),
    ("bearish_initiation", 10),
];

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
        filter_price_volume_structure_entry_v3,
    );
    core_shared::insert_filtered_indicator(&mut indicators, source, "fvg", |payload| {
        filter_fvg(payload, CORE_WINDOWS_WITH_3D, 6)
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
    core_shared::insert_filtered_indicator(
        &mut indicators,
        source,
        "footprint",
        filter_footprint_entry_v3,
    );
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
        filter_cvd_pack_entry_v3(payload, CVD_LIMITS)
    });
    core_shared::insert_filtered_indicator(
        &mut indicators,
        source,
        "funding_rate",
        filter_funding_rate_entry_v3,
    );
    core_shared::insert_filtered_indicator(
        &mut indicators,
        source,
        "liquidation_density",
        filter_liquidation_density_entry_v3,
    );
    core_shared::insert_filtered_indicator(&mut indicators, source, "orderbook_depth", |payload| {
        filter_orderbook_depth_entry_v3(payload, resolve_reference_mark_price(source), 80)
    });

    for (code, keep_last) in ENTRY_EVENT_INDICATOR_RULES {
        core_shared::insert_filtered_indicator(&mut indicators, source, code, |payload| {
            filter_event_indicator_entry_v3(payload, *keep_last)
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
