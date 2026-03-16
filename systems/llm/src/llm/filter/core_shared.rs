use chrono::{DateTime, SecondsFormat, Timelike, Utc};
use serde_json::{json, Map, Value};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

pub(super) const CORE_WINDOWS: &[&str] = &["15m", "4h", "1d"];
pub(super) const CORE_WINDOWS_WITH_3D: &[&str] = &["15m", "4h", "1d", "3d"];
pub(super) const EVENT_INDICATOR_RULES: &[(&str, usize)] = &[
    ("absorption", 10),
    ("buying_exhaustion", 10),
    ("selling_exhaustion", 10),
    ("initiation", 10),
    ("bullish_absorption", 10),
    ("bearish_absorption", 10),
    ("bullish_initiation", 17),
    ("bearish_initiation", 10),
];

const HTF_IMBALANCE_CLUSTER_STEP_RATIO: f64 = 0.005;
const ORDERBOOK_NEAR_MARK_PCT: f64 = 0.20;
const ENTRY_ORDERBOOK_TOP_LEVEL_PRIMARY_PCT: f64 = 0.02;
const ENTRY_ORDERBOOK_TOP_LEVEL_FALLBACK_PCT: f64 = 0.03;
const ENTRY_ORDERBOOK_TOP_LEVEL_MIN_COUNT: usize = 10;
const DETAILED_TOP_VOLUME_LEVELS: usize = 10;
const DETAILED_IMBALANCE_LEVELS_PER_SIDE: usize = 20;
const DEFENSIVE_IMBALANCE_LEVELS_PER_SIDE: usize = 10;
const ENTRY_TOP_LEVEL_VA_TOP_LEVELS: usize = 15;
const ENTRY_15M_VA_TOP_LEVELS: usize = 15;
const ENTRY_HTF_VA_TOP_LEVELS: usize = 10;
const ENTRY_FUNDING_TREND_HOURLY_LIMIT: usize = 24;
const ENTRY_LIQ_TREND_HOURLY_LIMIT: usize = 48;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FootprintMode {
    Detailed,
    Defensive,
}

pub(super) fn insert_full_indicator(
    target: &mut Map<String, Value>,
    source: &Map<String, Value>,
    code: &str,
) {
    if let Some(indicator) = source.get(code) {
        let payload = indicator.get("payload").cloned().unwrap_or(Value::Null);
        target.insert(code.to_string(), rebuild_indicator(indicator, payload));
    }
}

pub(super) fn insert_filtered_indicator<F>(
    target: &mut Map<String, Value>,
    source: &Map<String, Value>,
    code: &str,
    filter_fn: F,
) where
    F: FnOnce(&Value) -> Value,
{
    if let Some(indicator) = source.get(code) {
        let payload = indicator
            .get("payload")
            .map(filter_fn)
            .unwrap_or(Value::Null);
        target.insert(code.to_string(), rebuild_indicator(indicator, payload));
    }
}

pub(super) fn rebuild_indicator(indicator: &Value, payload: Value) -> Value {
    let mut rebuilt = indicator
        .as_object()
        .map(|map| {
            map.iter()
                .filter(|(key, _)| matches!(key.as_str(), "payload"))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<Map<String, Value>>()
        })
        .unwrap_or_default();
    rebuilt.insert("payload".to_string(), payload);
    Value::Object(rebuilt)
}

pub(super) fn filter_tpo_market_profile(payload: &Value) -> Value {
    let mut payload = payload.clone();
    reverse_dev_series_map(payload.get_mut("dev_series"));
    if let Some(by_session) = payload.get_mut("by_session").and_then(Value::as_object_mut) {
        for session in by_session.values_mut() {
            reverse_dev_series_map(session.get_mut("dev_series"));
        }
    }
    payload
}

pub(super) fn filter_price_volume_structure(payload: &Value) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();
    copy_fields(
        &mut result,
        payload,
        &[
            "poc_price",
            "poc_volume",
            "vah",
            "val",
            "hvn_levels",
            "lvn_levels",
            "bar_volume",
            "volume_zscore",
            "volume_dryup",
        ],
    );
    result.insert(
        "va_top_levels".to_string(),
        Value::Array(build_va_top_levels(
            payload.get("value_area_levels").and_then(Value::as_array),
            ENTRY_TOP_LEVEL_VA_TOP_LEVELS,
        )),
    );

    if let Some(by_window) = payload.get("by_window").and_then(Value::as_object) {
        let mut filtered_windows = Map::new();
        for window in CORE_WINDOWS_WITH_3D {
            let Some(window_value) = by_window.get(*window).and_then(Value::as_object) else {
                continue;
            };
            let mut filtered_window = Map::new();
            copy_fields(
                &mut filtered_window,
                window_value,
                &[
                    "poc_price",
                    "poc_volume",
                    "vah",
                    "val",
                    "hvn_levels",
                    "lvn_levels",
                    "bar_volume",
                    "volume_zscore",
                    "volume_dryup",
                    "window_bars_used",
                ],
            );
            let limit = if *window == "15m" {
                ENTRY_15M_VA_TOP_LEVELS
            } else {
                ENTRY_HTF_VA_TOP_LEVELS
            };
            filtered_window.insert(
                "va_top_levels".to_string(),
                Value::Array(build_va_top_levels(
                    window_value
                        .get("value_area_levels")
                        .and_then(Value::as_array),
                    limit,
                )),
            );
            filtered_windows.insert((*window).to_string(), Value::Object(filtered_window));
        }
        result.insert("by_window".to_string(), Value::Object(filtered_windows));
    }

    Value::Object(result)
}

pub(super) fn filter_price_volume_structure_entry_v3(payload: &Value) -> Value {
    filter_price_volume_structure(payload)
}

pub(super) fn filter_fvg(payload: &Value, windows: &[&str], keep_recent: usize) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();
    copy_fields(
        &mut result,
        payload,
        &["base_detection_uses_spot", "source_market"],
    );

    if let Some(by_window) = payload.get("by_window").and_then(Value::as_object) {
        let mut filtered_windows = Map::new();
        for window in windows {
            let Some(window_value) = by_window.get(*window).and_then(Value::as_object) else {
                continue;
            };
            let mut filtered_window = Map::new();
            copy_fields(
                &mut filtered_window,
                window_value,
                &[
                    "nearest_bull_fvg",
                    "nearest_bear_fvg",
                    "is_ready",
                    "coverage_ratio",
                ],
            );
            sanitize_fvg_object_field(&mut filtered_window, "nearest_bull_fvg");
            sanitize_fvg_object_field(&mut filtered_window, "nearest_bear_fvg");
            reverse_array_field(
                &mut filtered_window,
                window_value,
                "active_bull_fvgs",
                usize::MAX,
            );
            sanitize_fvg_array_field(&mut filtered_window, "active_bull_fvgs");
            reverse_array_field(
                &mut filtered_window,
                window_value,
                "active_bear_fvgs",
                usize::MAX,
            );
            sanitize_fvg_array_field(&mut filtered_window, "active_bear_fvgs");
            reverse_array_field(&mut filtered_window, window_value, "fvgs", keep_recent);
            sanitize_fvg_array_field(&mut filtered_window, "fvgs");
            filtered_windows.insert((*window).to_string(), Value::Object(filtered_window));
        }
        result.insert("by_window".to_string(), Value::Object(filtered_windows));
    }

    Value::Object(result)
}

pub(super) fn filter_avwap(payload: &Value, limits: &[(&str, usize)]) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();
    copy_fields(
        &mut result,
        payload,
        &[
            "anchor_ts",
            "avwap_fut",
            "avwap_spot",
            "fut_last_price",
            "fut_mark_price",
            "lookback",
            "price_minus_avwap_fut",
            "price_minus_spot_avwap_fut",
            "price_minus_spot_avwap_futmark",
            "xmk_avwap_gap_f_minus_s",
            "zavwap_gap",
        ],
    );

    if let Some(series_by_window) = payload.get("series_by_window").and_then(Value::as_object) {
        let mut filtered_series = Map::new();
        for (window, limit) in limits {
            let Some(series) = series_by_window.get(*window).and_then(Value::as_array) else {
                continue;
            };
            let filtered = take_last_n(series, *limit)
                .into_iter()
                .filter_map(|entry| entry.as_object().cloned())
                .map(|entry| {
                    let mut filtered_entry = Map::new();
                    copy_fields(
                        &mut filtered_entry,
                        &entry,
                        &["ts", "avwap_fut", "avwap_spot", "xmk_avwap_gap_f_minus_s"],
                    );
                    Value::Object(filtered_entry)
                })
                .collect::<Vec<_>>();
            filtered_series.insert((*window).to_string(), Value::Array(filtered));
        }
        result.insert(
            "series_by_window".to_string(),
            Value::Object(filtered_series),
        );
    }

    Value::Object(result)
}

pub(super) fn filter_rvwap_sigma_bands(payload: &Value) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();
    copy_fields(&mut result, payload, &["as_of_ts", "source_mode"]);

    if let Some(by_window) = payload.get("by_window").and_then(Value::as_object) {
        let mut filtered_windows = Map::new();
        for window in CORE_WINDOWS {
            let Some(window_value) = by_window.get(*window).and_then(Value::as_object) else {
                continue;
            };
            let mut filtered_window = Map::new();
            copy_fields(
                &mut filtered_window,
                window_value,
                &[
                    "rvwap_w",
                    "rvwap_sigma_w",
                    "rvwap_band_minus_1",
                    "rvwap_band_minus_2",
                    "rvwap_band_plus_1",
                    "rvwap_band_plus_2",
                    "z_price_minus_rvwap",
                    "samples_used",
                ],
            );
            filtered_windows.insert((*window).to_string(), Value::Object(filtered_window));
        }
        result.insert("by_window".to_string(), Value::Object(filtered_windows));
    }

    Value::Object(result)
}

pub(super) fn filter_footprint(payload: &Value, mode: FootprintMode) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let Some(by_window) = payload.get("by_window").and_then(Value::as_object) else {
        return Value::Object(Map::new());
    };

    let mut filtered_windows = Map::new();

    if let Some(window_value) = by_window.get("15m").and_then(Value::as_object) {
        filtered_windows.insert(
            "15m".to_string(),
            Value::Object(filter_footprint_15m_window(window_value, mode)),
        );
    }

    for window in ["4h", "1d"] {
        let Some(window_value) = by_window.get(window).and_then(Value::as_object) else {
            continue;
        };
        filtered_windows.insert(
            window.to_string(),
            Value::Object(filter_footprint_htf_window(window_value)),
        );
    }

    let mut result = Map::new();
    result.insert("by_window".to_string(), Value::Object(filtered_windows));
    Value::Object(result)
}

pub(super) fn filter_orderbook_depth(
    payload: &Value,
    mark_price: Option<f64>,
    top_limit: usize,
) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();
    copy_fields(
        &mut result,
        payload,
        &[
            "obi_fut",
            "obi_k_dw_twa_fut",
            "obi_k_dw_adj_twa_fut",
            "obi_k_dw_change_fut",
            "obi_k_dw_slope_fut",
            "obi_l1_twa_fut",
            "obi_shock_fut",
            "ofi_fut",
            "ofi_norm_fut",
            "ofi_spot",
            "ofi_norm_spot",
            "microprice_fut",
            "microprice_adj_fut",
            "microprice_classic_fut",
            "microprice_kappa_fut",
            "spot_confirm",
            "spot_driven_divergence_flag",
            "exec_confirm_fut",
            "fake_order_risk_fut",
            "heatmap_summary_fut",
            "spread_twa_fut",
            "spread_twa_spot",
            "topk_depth_twa_fut",
            "topk_depth_twa_spot",
            "weak_price_resp_fut",
            "cross_cvd_attribution",
            "depth_k",
        ],
    );

    let top_liquidity_levels = payload
        .get("levels")
        .and_then(Value::as_array)
        .map(|levels| build_top_liquidity_levels(levels, mark_price, top_limit))
        .unwrap_or_default();
    result.insert(
        "top_liquidity_levels".to_string(),
        Value::Array(top_liquidity_levels),
    );

    if let Some(by_window) = payload.get("by_window").and_then(Value::as_object) {
        let mut filtered_windows = Map::new();
        for window in ["15m", "1h"] {
            if let Some(window_value) = by_window.get(window) {
                filtered_windows.insert(window.to_string(), window_value.clone());
            }
        }
        result.insert("by_window".to_string(), Value::Object(filtered_windows));
    }

    Value::Object(result)
}

pub(super) fn filter_orderbook_depth_entry_v3(
    payload: &Value,
    mark_price: Option<f64>,
    top_limit: usize,
) -> Value {
    let Some(payload_obj) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = filter_orderbook_depth(payload, mark_price, top_limit);
    if let Some(result_obj) = result.as_object_mut() {
        let top_liquidity_levels = payload_obj
            .get("levels")
            .and_then(Value::as_array)
            .map(|levels| build_entry_top_liquidity_levels(levels, mark_price, top_limit))
            .unwrap_or_default();
        result_obj.insert(
            "top_liquidity_levels".to_string(),
            Value::Array(top_liquidity_levels),
        );
        let liquidity_walls = payload_obj
            .get("levels")
            .and_then(Value::as_array)
            .map(|levels| build_liquidity_walls(levels, mark_price))
            .unwrap_or_else(|| {
                json!({
                    "bid_walls": [],
                    "ask_walls": [],
                    "depth_imbalance_1pct": Value::Null,
                    "depth_imbalance_3pct": Value::Null,
                })
            });
        result_obj.insert("liquidity_walls".to_string(), liquidity_walls);
    }
    result
}

pub(super) fn filter_kline_history(payload: &Value, limits: &[(&str, usize)]) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();
    copy_fields(&mut result, payload, &["as_of_ts"]);

    if let Some(intervals) = payload.get("intervals").and_then(Value::as_object) {
        let mut filtered_intervals = Map::new();
        for (window, limit) in limits {
            let Some(interval_value) = intervals.get(*window).and_then(Value::as_object) else {
                continue;
            };
            let futures_bars = extract_futures_bars(interval_value);
            let bars = take_last_n(&futures_bars, *limit)
                .into_iter()
                .filter_map(|entry| entry.as_object().cloned())
                .map(|entry| {
                    let mut filtered_entry = Map::new();
                    copy_fields(
                        &mut filtered_entry,
                        &entry,
                        &[
                            "open",
                            "high",
                            "low",
                            "close",
                            "volume_base",
                            "open_time",
                            "is_closed",
                        ],
                    );
                    Value::Object(filtered_entry)
                })
                .collect::<Vec<_>>();

            filtered_intervals.insert(
                (*window).to_string(),
                json!({
                    "markets": {
                        "futures": bars
                    }
                }),
            );
        }
        result.insert("intervals".to_string(), Value::Object(filtered_intervals));
    }

    Value::Object(result)
}

pub(super) fn filter_ema_trend_regime(payload: &Value) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();
    copy_fields(
        &mut result,
        payload,
        &[
            "as_of_ts",
            "output_sampling",
            "ema_13",
            "ema_21",
            "ema_34",
            "ema_band_high",
            "ema_band_low",
            "ema_100_htf",
            "ema_200_htf",
        ],
    );
    Value::Object(result)
}

pub(super) fn filter_cvd_pack(payload: &Value, limits: &[(&str, usize)]) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();
    copy_fields(
        &mut result,
        payload,
        &[
            "delta_fut",
            "delta_spot",
            "relative_delta_fut",
            "relative_delta_spot",
            "likely_driver",
            "spot_flow_dominance",
            "spot_lead_score",
            "xmk_delta_gap_s_minus_f",
            "cvd_slope_fut",
            "cvd_slope_spot",
        ],
    );

    if let Some(by_window) = payload.get("by_window").and_then(Value::as_object) {
        let mut filtered_windows = Map::new();
        for (window, limit) in limits {
            let Some(window_value) = by_window.get(*window).and_then(Value::as_object) else {
                continue;
            };
            let series = window_value
                .get("series")
                .and_then(Value::as_array)
                .map(|series| take_last_n(series, *limit))
                .unwrap_or_default()
                .into_iter()
                .filter_map(|entry| entry.as_object().cloned())
                .map(|entry| {
                    let mut filtered_entry = Map::new();
                    copy_fields(
                        &mut filtered_entry,
                        &entry,
                        &[
                            "ts",
                            "close_fut",
                            "close_spot",
                            "delta_fut",
                            "delta_spot",
                            "relative_delta_fut",
                            "relative_delta_spot",
                            "cvd_7d_fut",
                            "spot_flow_dominance",
                            "xmk_delta_gap_s_minus_f",
                        ],
                    );
                    Value::Object(filtered_entry)
                })
                .collect::<Vec<_>>();
            filtered_windows.insert((*window).to_string(), json!({ "series": series }));
        }
        result.insert("by_window".to_string(), Value::Object(filtered_windows));
    }

    Value::Object(result)
}

pub(super) fn filter_cvd_pack_entry_v3(payload: &Value, limits: &[(&str, usize)]) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();
    copy_fields(
        &mut result,
        payload,
        &[
            "delta_fut",
            "delta_spot",
            "relative_delta_fut",
            "relative_delta_spot",
            "likely_driver",
            "spot_flow_dominance",
            "spot_lead_score",
            "xmk_delta_gap_s_minus_f",
            "cvd_slope_fut",
            "cvd_slope_spot",
        ],
    );

    if let Some(by_window) = payload.get("by_window").and_then(Value::as_object) {
        let mut filtered_windows = Map::new();
        for (window, limit) in limits {
            let Some(window_value) = by_window.get(*window).and_then(Value::as_object) else {
                continue;
            };
            let series = window_value
                .get("series")
                .and_then(Value::as_array)
                .map(|series| take_last_n(series, *limit))
                .unwrap_or_default()
                .into_iter()
                .filter_map(|entry| entry.as_object().cloned())
                .map(|entry| {
                    let mut filtered_entry = Map::new();
                    copy_fields(
                        &mut filtered_entry,
                        &entry,
                        &[
                            "ts",
                            "delta_fut",
                            "delta_spot",
                            "cvd_7d_fut",
                            "xmk_delta_gap_s_minus_f",
                        ],
                    );
                    Value::Object(filtered_entry)
                })
                .collect::<Vec<_>>();
            filtered_windows.insert((*window).to_string(), json!({ "series": series }));
        }
        result.insert("by_window".to_string(), Value::Object(filtered_windows));
    }

    Value::Object(result)
}

pub(super) fn filter_funding_rate(payload: &Value) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();
    copy_fields(
        &mut result,
        payload,
        &[
            "funding_current",
            "funding_twa",
            "mark_price_last",
            "mark_price_last_ts",
            "mark_price_twap",
            "funding_current_effective_ts",
        ],
    );

    if let Some(by_window) = payload.get("by_window").and_then(Value::as_object) {
        let mut filtered_windows = Map::new();
        for window in CORE_WINDOWS {
            let Some(window_value) = by_window.get(*window).and_then(Value::as_object) else {
                continue;
            };
            let mut filtered_window = Map::new();
            copy_fields(
                &mut filtered_window,
                window_value,
                &["funding_twa", "change_count"],
            );
            let changes = window_value
                .get("changes")
                .and_then(Value::as_array)
                .map(|changes| take_last_n(changes, 5))
                .unwrap_or_default()
                .into_iter()
                .filter_map(|entry| entry.as_object().cloned())
                .map(|entry| {
                    let mut filtered_entry = Map::new();
                    copy_fields(
                        &mut filtered_entry,
                        &entry,
                        &["change_ts", "funding_delta", "funding_new", "funding_prev"],
                    );
                    Value::Object(filtered_entry)
                })
                .collect::<Vec<_>>();
            filtered_window.insert("changes".to_string(), Value::Array(changes));
            filtered_windows.insert((*window).to_string(), Value::Object(filtered_window));
        }
        result.insert("by_window".to_string(), Value::Object(filtered_windows));
    }

    let funding_trend_hourly = payload
        .get("recent_7d")
        .and_then(Value::as_array)
        .map(|recent| aggregate_funding_hourly(recent))
        .unwrap_or_default();
    result.insert(
        "funding_trend_hourly".to_string(),
        Value::Array(funding_trend_hourly),
    );

    Value::Object(result)
}

pub(super) fn filter_funding_rate_entry_v3(payload: &Value) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();
    copy_fields(
        &mut result,
        payload,
        &[
            "funding_current",
            "funding_twa",
            "mark_price_last",
            "mark_price_last_ts",
            "mark_price_twap",
            "funding_current_effective_ts",
        ],
    );

    if let Some(by_window) = payload.get("by_window").and_then(Value::as_object) {
        let mut filtered_windows = Map::new();
        for window in CORE_WINDOWS {
            let Some(window_value) = by_window.get(*window).and_then(Value::as_object) else {
                continue;
            };
            let mut filtered_window = Map::new();
            copy_fields(
                &mut filtered_window,
                window_value,
                &["funding_twa", "change_count"],
            );
            let changes = window_value
                .get("changes")
                .and_then(Value::as_array)
                .map(|changes| take_last_n(changes, 5))
                .unwrap_or_default()
                .into_iter()
                .filter_map(|entry| entry.as_object().cloned())
                .map(|entry| {
                    let mut filtered_entry = Map::new();
                    copy_fields(
                        &mut filtered_entry,
                        &entry,
                        &["change_ts", "funding_delta", "funding_new", "funding_prev"],
                    );
                    Value::Object(filtered_entry)
                })
                .collect::<Vec<_>>();
            filtered_window.insert("changes".to_string(), Value::Array(changes));
            filtered_windows.insert((*window).to_string(), Value::Object(filtered_window));
        }
        result.insert("by_window".to_string(), Value::Object(filtered_windows));
    }

    let funding_trend_hourly = payload
        .get("recent_7d")
        .and_then(Value::as_array)
        .map(|recent| aggregate_funding_hourly(recent))
        .unwrap_or_default();
    result.insert(
        "funding_trend_hourly".to_string(),
        Value::Array(
            funding_trend_hourly
                .iter()
                .take(ENTRY_FUNDING_TREND_HOURLY_LIMIT)
                .cloned()
                .collect(),
        ),
    );
    result.insert(
        "funding_trend_stats".to_string(),
        build_funding_trend_stats(&funding_trend_hourly),
    );
    result.insert(
        "funding_summary".to_string(),
        build_funding_summary_alias(payload.get("recent_7d").and_then(Value::as_array)),
    );

    Value::Object(result)
}

pub(super) fn filter_liquidation_density(payload: &Value) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();

    if let Some(by_window) = payload.get("by_window").and_then(Value::as_object) {
        let mut filtered_windows = Map::new();
        for window in CORE_WINDOWS {
            let Some(window_value) = by_window.get(*window).and_then(Value::as_object) else {
                continue;
            };
            let mut filtered_window = Map::new();
            copy_fields(
                &mut filtered_window,
                window_value,
                &[
                    "long_total",
                    "short_total",
                    "peak_levels",
                    "coverage_ratio",
                    "is_ready",
                    "levels_count",
                ],
            );
            filtered_windows.insert((*window).to_string(), Value::Object(filtered_window));
        }
        result.insert("by_window".to_string(), Value::Object(filtered_windows));
    }

    let liq_trend_hourly = payload
        .get("recent_7d")
        .and_then(Value::as_array)
        .map(|recent| aggregate_liquidation_hourly(recent))
        .unwrap_or_default();
    result.insert(
        "liq_trend_hourly".to_string(),
        Value::Array(liq_trend_hourly),
    );

    Value::Object(result)
}

pub(super) fn filter_liquidation_density_entry_v3(payload: &Value) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();

    if let Some(by_window) = payload.get("by_window").and_then(Value::as_object) {
        let mut filtered_windows = Map::new();
        for window in CORE_WINDOWS {
            let Some(window_value) = by_window.get(*window).and_then(Value::as_object) else {
                continue;
            };
            let mut filtered_window = Map::new();
            copy_fields(
                &mut filtered_window,
                window_value,
                &[
                    "long_total",
                    "short_total",
                    "peak_levels",
                    "coverage_ratio",
                    "is_ready",
                    "levels_count",
                ],
            );
            filtered_windows.insert((*window).to_string(), Value::Object(filtered_window));
        }
        result.insert("by_window".to_string(), Value::Object(filtered_windows));
    }

    let liq_trend_hourly = payload
        .get("recent_7d")
        .and_then(Value::as_array)
        .map(|recent| aggregate_liquidation_hourly(recent))
        .unwrap_or_default();
    result.insert(
        "liq_trend_hourly".to_string(),
        Value::Array(
            liq_trend_hourly
                .iter()
                .take(ENTRY_LIQ_TREND_HOURLY_LIMIT)
                .cloned()
                .collect(),
        ),
    );
    result.insert(
        "liq_spike_events".to_string(),
        Value::Array(build_liquidation_spike_events(&liq_trend_hourly, 10)),
    );

    Value::Object(result)
}

pub(super) fn filter_event_indicator(payload: &Value, keep_last: usize) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();
    let Some(recent_7d) = payload.get("recent_7d").and_then(Value::as_object) else {
        return Value::Object(result);
    };
    let mut filtered_recent = Map::new();
    copy_fields(
        &mut filtered_recent,
        recent_7d,
        &["event_count", "lookback_coverage_ratio"],
    );
    let events = recent_7d
        .get("events")
        .and_then(Value::as_array)
        .map(|events| take_last_n(events, keep_last))
        .unwrap_or_default();
    filtered_recent.insert(
        "events".to_string(),
        Value::Array(
            events
                .into_iter()
                .map(|entry| sanitize_event_value(&entry))
                .collect(),
        ),
    );
    result.insert("recent_7d".to_string(), Value::Object(filtered_recent));
    Value::Object(result)
}

pub(super) fn filter_event_indicator_entry_v3(payload: &Value, keep_last: usize) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();
    let Some(recent_7d) = payload.get("recent_7d").and_then(Value::as_object) else {
        return Value::Object(result);
    };
    let mut filtered_recent = Map::new();
    copy_fields(
        &mut filtered_recent,
        recent_7d,
        &["event_count", "lookback_coverage_ratio"],
    );
    let events = recent_7d
        .get("events")
        .and_then(Value::as_array)
        .map(|events| take_last_n(events, keep_last))
        .unwrap_or_default();
    filtered_recent.insert(
        "events".to_string(),
        Value::Array(
            events
                .into_iter()
                .map(|entry| sanitize_event_value_entry_v3(&entry))
                .collect(),
        ),
    );
    result.insert("recent_7d".to_string(), Value::Object(filtered_recent));
    Value::Object(result)
}

pub(super) fn filter_divergence(payload: &Value) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();
    copy_fields(
        &mut result,
        payload,
        &[
            "event_count",
            "divergence_type",
            "likely_driver",
            "spot_lead_score",
            "pivot_side",
            "reason",
        ],
    );
    if let Some(latest) = payload.get("latest_7d") {
        result.insert("latest_7d".to_string(), sanitize_event_value(latest));
    }

    if let Some(recent_7d) = payload.get("recent_7d").and_then(Value::as_object) {
        let mut filtered_recent = Map::new();
        copy_fields(&mut filtered_recent, recent_7d, &["event_count"]);
        let events = recent_7d
            .get("events")
            .and_then(Value::as_array)
            .map(|events| take_last_n(events, 10))
            .unwrap_or_default();
        filtered_recent.insert(
            "events".to_string(),
            Value::Array(
                events
                    .into_iter()
                    .map(|entry| sanitize_event_value(&entry))
                    .collect(),
            ),
        );
        result.insert("recent_7d".to_string(), Value::Object(filtered_recent));
    }

    let candidates = payload
        .get("candidates")
        .and_then(Value::as_array)
        .map(|candidates| top_divergence_candidates(candidates, 5))
        .unwrap_or_default();
    result.insert("candidates".to_string(), Value::Array(candidates));

    Value::Object(result)
}

pub(super) fn filter_footprint_entry_v3(payload: &Value) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let Some(by_window) = payload.get("by_window").and_then(Value::as_object) else {
        return Value::Object(Map::new());
    };

    let mut filtered_windows = Map::new();

    if let Some(window_value) = by_window.get("15m").and_then(Value::as_object) {
        filtered_windows.insert(
            "15m".to_string(),
            Value::Object(filter_footprint_15m_window_entry_v3(window_value)),
        );
    }

    for window in ["4h", "1d"] {
        let Some(window_value) = by_window.get(window).and_then(Value::as_object) else {
            continue;
        };
        filtered_windows.insert(
            window.to_string(),
            Value::Object(filter_footprint_htf_window(window_value)),
        );
    }

    let mut result = Map::new();
    result.insert("by_window".to_string(), Value::Object(filtered_windows));
    Value::Object(result)
}

pub(super) fn resolve_reference_mark_price(source: &Map<String, Value>) -> Option<f64> {
    source
        .get("avwap")
        .and_then(|indicator| indicator.pointer("/payload/fut_mark_price"))
        .and_then(Value::as_f64)
        .or_else(|| {
            source
                .get("funding_rate")
                .and_then(|indicator| indicator.pointer("/payload/mark_price_last"))
                .and_then(Value::as_f64)
        })
}

pub(super) fn copy_fields(
    target: &mut Map<String, Value>,
    source: &Map<String, Value>,
    fields: &[&str],
) {
    for field in fields {
        if let Some(value) = source.get(*field) {
            target.insert((*field).to_string(), value.clone());
        }
    }
}

pub(super) fn take_last_n(values: &[Value], n: usize) -> Vec<Value> {
    let mut result = if n == usize::MAX || values.len() <= n {
        values.to_vec()
    } else {
        values[values.len() - n..].to_vec()
    };
    result.reverse();
    result
}

fn reverse_dev_series_map(value: Option<&mut Value>) {
    if let Some(dev_series) = value.and_then(Value::as_object_mut) {
        for series in dev_series.values_mut() {
            if let Some(items) = series.as_array_mut() {
                items.reverse();
            }
        }
    }
}

fn reverse_array_field(
    target: &mut Map<String, Value>,
    source: &Map<String, Value>,
    field: &str,
    limit: usize,
) {
    if let Some(items) = source.get(field).and_then(Value::as_array) {
        target.insert(field.to_string(), Value::Array(take_last_n(items, limit)));
    }
}

fn sanitize_fvg_object_field(target: &mut Map<String, Value>, field: &str) {
    if let Some(value) = target.get(field).cloned() {
        target.insert(field.to_string(), sanitize_fvg_value(&value));
    }
}

fn sanitize_fvg_array_field(target: &mut Map<String, Value>, field: &str) {
    if let Some(items) = target.get(field).and_then(Value::as_array).cloned() {
        target.insert(
            field.to_string(),
            Value::Array(
                items
                    .into_iter()
                    .map(|entry| sanitize_fvg_value(&entry))
                    .collect(),
            ),
        );
    }
}

fn sanitize_fvg_value(value: &Value) -> Value {
    let Some(entry) = value.as_object() else {
        return value.clone();
    };
    let mut filtered = entry.clone();
    filtered.remove("fvg_id");
    filtered.remove("tf");
    if let Some(upper) = filtered.get("upper").cloned() {
        filtered.insert("fvg_top".to_string(), upper);
    }
    if let Some(lower) = filtered.get("lower").cloned() {
        filtered.insert("fvg_bottom".to_string(), lower);
    }
    Value::Object(filtered)
}

fn sanitize_event_value(value: &Value) -> Value {
    let Some(entry) = value.as_object() else {
        return value.clone();
    };
    let mut filtered = entry.clone();
    for field in [
        "event_id",
        "indicator_code",
        "start_ts",
        "end_ts",
        "event_available_ts",
        "price_norm_diff",
        "cvd_norm_diff_fut",
        "cvd_norm_diff_spot",
        "sig_test_mode",
    ] {
        filtered.remove(field);
    }
    Value::Object(filtered)
}

fn sanitize_event_value_entry_v3(value: &Value) -> Value {
    let Some(entry) = value.as_object() else {
        return value.clone();
    };
    let mut filtered = entry.clone();
    for field in [
        "event_id",
        "indicator_code",
        "start_ts",
        "end_ts",
        "event_available_ts",
        "price_norm_diff",
        "cvd_norm_diff_fut",
        "cvd_norm_diff_spot",
        "sig_test_mode",
        "score_base",
        "strength_score_xmk",
        "spot_flow_confirm_score",
        "spot_whale_confirm_score",
        "spot_rdelta_1m_mean",
        "spot_cvd_1m_change",
    ] {
        filtered.remove(field);
    }
    Value::Object(filtered)
}

fn filter_footprint_15m_window(
    window_value: &Map<String, Value>,
    mode: FootprintMode,
) -> Map<String, Value> {
    let mut filtered_window = Map::new();
    copy_fields(
        &mut filtered_window,
        window_value,
        &[
            "buy_imbalance_prices",
            "sell_imbalance_prices",
            "buy_stacks",
            "sell_stacks",
            "max_buy_stack_len",
            "max_sell_stack_len",
            "stacked_buy",
            "stacked_sell",
            "ua_top",
            "ua_bottom",
            "unfinished_auction",
            "window_delta",
            "window_total_qty",
        ],
    );

    let levels = window_value
        .get("levels")
        .and_then(Value::as_array)
        .map(|levels| filter_footprint_15m_levels(levels, mode))
        .unwrap_or_default();
    filtered_window.insert("levels".to_string(), Value::Array(levels));
    filtered_window
}

fn filter_footprint_15m_window_entry_v3(window_value: &Map<String, Value>) -> Map<String, Value> {
    let mut filtered_window = Map::new();
    copy_fields(
        &mut filtered_window,
        window_value,
        &[
            "buy_stacks",
            "sell_stacks",
            "max_buy_stack_len",
            "max_sell_stack_len",
            "stacked_buy",
            "stacked_sell",
            "ua_top",
            "ua_bottom",
            "unfinished_auction",
            "window_delta",
            "window_total_qty",
        ],
    );

    let levels = window_value
        .get("levels")
        .and_then(Value::as_array)
        .map(|levels| filter_footprint_15m_levels(levels, FootprintMode::Detailed))
        .unwrap_or_default();
    filtered_window.insert("levels".to_string(), Value::Array(levels));
    filtered_window
}

fn filter_footprint_htf_window(window_value: &Map<String, Value>) -> Map<String, Value> {
    let mut filtered_window = Map::new();
    copy_fields(
        &mut filtered_window,
        window_value,
        &[
            "window_delta",
            "window_total_qty",
            "unfinished_auction",
            "ua_top",
            "ua_bottom",
            "stacked_buy",
            "stacked_sell",
            "buy_stacks",
            "sell_stacks",
            "max_buy_stack_len",
            "max_sell_stack_len",
        ],
    );
    let buy_clusters = window_value
        .get("buy_imbalance_prices")
        .and_then(Value::as_array)
        .map(|prices| aggregate_price_clusters(prices, HTF_IMBALANCE_CLUSTER_STEP_RATIO, 20))
        .unwrap_or_default();
    let sell_clusters = window_value
        .get("sell_imbalance_prices")
        .and_then(Value::as_array)
        .map(|prices| aggregate_price_clusters(prices, HTF_IMBALANCE_CLUSTER_STEP_RATIO, 20))
        .unwrap_or_default();
    filtered_window.insert("buy_imb_clusters".to_string(), Value::Array(buy_clusters));
    filtered_window.insert("sell_imb_clusters".to_string(), Value::Array(sell_clusters));
    filtered_window
}

fn filter_footprint_15m_levels(levels: &[Value], mode: FootprintMode) -> Vec<Value> {
    let (top_volume_count, imbalance_levels_per_side) = match mode {
        FootprintMode::Detailed => (
            DETAILED_TOP_VOLUME_LEVELS,
            DETAILED_IMBALANCE_LEVELS_PER_SIDE,
        ),
        FootprintMode::Defensive => (0, DEFENSIVE_IMBALANCE_LEVELS_PER_SIDE),
    };

    let top_volume_prices = collect_top_level_prices(levels, top_volume_count, |_| true);
    let buy_imbalance_prices =
        collect_top_level_prices(levels, imbalance_levels_per_side, |level| {
            level
                .get("buy_imbalance")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        });
    let sell_imbalance_prices =
        collect_top_level_prices(levels, imbalance_levels_per_side, |level| {
            level
                .get("sell_imbalance")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        });

    let mut filtered = levels
        .iter()
        .filter_map(Value::as_object)
        .filter(|level| {
            let flag = |name: &str| level.get(name).and_then(Value::as_bool).unwrap_or(false);
            let price_bits = level
                .get("price_level")
                .and_then(Value::as_f64)
                .map(f64::to_bits);

            flag("is_open")
                || flag("is_high")
                || flag("is_low")
                || flag("is_close")
                || flag("ua_top_flag")
                || flag("ua_bottom_flag")
                || price_bits
                    .map(|bits| top_volume_prices.contains(&bits))
                    .unwrap_or(false)
                || price_bits
                    .map(|bits| buy_imbalance_prices.contains(&bits))
                    .unwrap_or(false)
                || price_bits
                    .map(|bits| sell_imbalance_prices.contains(&bits))
                    .unwrap_or(false)
        })
        .map(|level| {
            let mut filtered_level = Map::new();
            copy_fields(
                &mut filtered_level,
                level,
                &[
                    "price_level",
                    "buy",
                    "sell",
                    "delta",
                    "total",
                    "buy_imbalance",
                    "sell_imbalance",
                    "is_open",
                    "is_high",
                    "is_low",
                    "is_close",
                    "ua_top_flag",
                    "ua_bottom_flag",
                ],
            );
            Value::Object(filtered_level)
        })
        .collect::<Vec<_>>();

    filtered.sort_by(|left, right| {
        let left_price = left
            .get("price_level")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        let right_price = right
            .get("price_level")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        right_price
            .partial_cmp(&left_price)
            .unwrap_or(Ordering::Equal)
    });
    filtered
}

fn collect_top_level_prices<F>(levels: &[Value], limit: usize, predicate: F) -> BTreeSet<u64>
where
    F: Fn(&Map<String, Value>) -> bool,
{
    if limit == 0 {
        return BTreeSet::new();
    }

    let mut ranked = levels
        .iter()
        .filter_map(Value::as_object)
        .filter(|level| predicate(level))
        .filter_map(|level| {
            Some((
                level.get("price_level")?.as_f64()?.to_bits(),
                level
                    .get("total")
                    .and_then(Value::as_f64)
                    .unwrap_or_default(),
                level
                    .get("delta")
                    .and_then(Value::as_f64)
                    .unwrap_or_default()
                    .abs(),
            ))
        })
        .collect::<Vec<_>>();

    ranked.sort_by(|left, right| {
        right
            .1
            .partial_cmp(&left.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| right.2.partial_cmp(&left.2).unwrap_or(Ordering::Equal))
    });

    ranked
        .into_iter()
        .take(limit)
        .map(|(price_bits, _, _)| price_bits)
        .collect()
}

fn extract_futures_bars(interval_value: &Map<String, Value>) -> Vec<Value> {
    interval_value
        .get("markets")
        .and_then(Value::as_object)
        .and_then(|markets| markets.get("futures"))
        .and_then(Value::as_object)
        .and_then(|market| market.get("bars"))
        .and_then(Value::as_array)
        .cloned()
        .or_else(|| {
            interval_value
                .get("futures")
                .and_then(Value::as_object)
                .and_then(|market| market.get("bars"))
                .and_then(Value::as_array)
                .cloned()
        })
        .unwrap_or_default()
}

fn build_top_liquidity_levels(
    levels: &[Value],
    mark_price: Option<f64>,
    limit: usize,
) -> Vec<Value> {
    build_top_liquidity_levels_with_distance(levels, mark_price, limit, ORDERBOOK_NEAR_MARK_PCT)
}

fn build_top_liquidity_levels_with_distance(
    levels: &[Value],
    mark_price: Option<f64>,
    limit: usize,
    distance_pct: f64,
) -> Vec<Value> {
    let lower_bound = mark_price.map(|price| price * (1.0 - distance_pct));
    let upper_bound = mark_price.map(|price| price * (1.0 + distance_pct));

    build_top_liquidity_levels_in_bounds(levels, lower_bound, upper_bound, limit)
}

fn build_top_liquidity_levels_in_bounds(
    levels: &[Value],
    lower_bound: Option<f64>,
    upper_bound: Option<f64>,
    limit: usize,
) -> Vec<Value> {
    let mut sorted = levels
        .iter()
        .filter_map(|entry| entry.as_object().cloned())
        .filter(|entry| {
            let price_level = entry
                .get("price_level")
                .and_then(Value::as_f64)
                .unwrap_or_default();
            match (lower_bound, upper_bound) {
                (Some(low), Some(high)) => (low..=high).contains(&price_level),
                _ => true,
            }
        })
        .collect::<Vec<_>>();
    sorted.sort_by(|left, right| {
        let left_total = left
            .get("total_liquidity")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        let right_total = right
            .get("total_liquidity")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        right_total
            .partial_cmp(&left_total)
            .unwrap_or(Ordering::Equal)
    });
    sorted
        .into_iter()
        .take(limit)
        .map(|entry| {
            json!({
                "price_level": entry.get("price_level").cloned().unwrap_or(Value::Null),
                "bid_liquidity": entry.get("bid_liquidity").cloned().unwrap_or(Value::Null),
                "ask_liquidity": entry.get("ask_liquidity").cloned().unwrap_or(Value::Null),
                "total_liquidity": entry.get("total_liquidity").cloned().unwrap_or(Value::Null),
                "net_liquidity": entry.get("net_liquidity").cloned().unwrap_or(Value::Null),
                "level_imbalance": entry.get("level_imbalance").cloned().unwrap_or(Value::Null),
            })
        })
        .collect()
}

fn build_entry_top_liquidity_levels(
    levels: &[Value],
    mark_price: Option<f64>,
    limit: usize,
) -> Vec<Value> {
    let primary = build_top_liquidity_levels_with_distance(
        levels,
        mark_price,
        limit,
        ENTRY_ORDERBOOK_TOP_LEVEL_PRIMARY_PCT,
    );
    if mark_price.is_none() || primary.len() >= ENTRY_ORDERBOOK_TOP_LEVEL_MIN_COUNT {
        return primary;
    }
    let fallback = build_top_liquidity_levels_with_distance(
        levels,
        mark_price,
        limit,
        ENTRY_ORDERBOOK_TOP_LEVEL_FALLBACK_PCT,
    );
    if fallback.is_empty() {
        primary
    } else {
        fallback
    }
}

fn build_liquidity_walls(levels: &[Value], mid_price: Option<f64>) -> Value {
    let Some(mid_price) = mid_price.filter(|price| price.is_finite() && price.abs() > f64::EPSILON)
    else {
        return json!({
            "bid_walls": [],
            "ask_walls": [],
            "depth_imbalance_1pct": Value::Null,
            "depth_imbalance_3pct": Value::Null,
        });
    };
    let bin_size = (mid_price.abs() * 0.005).max(1e-9);
    let mut grouped: BTreeMap<i64, (f64, f64, f64, f64)> = BTreeMap::new();
    for level in levels.iter().filter_map(Value::as_object) {
        let price = level
            .get("price_level")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        let bid = level
            .get("bid_liquidity")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        let ask = level
            .get("ask_liquidity")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        let total = level
            .get("total_liquidity")
            .and_then(Value::as_f64)
            .unwrap_or(bid + ask);
        if !price.is_finite() {
            continue;
        }
        let distance_pct = ((price - mid_price) / mid_price) * 100.0;
        if !(-15.0..=15.0).contains(&distance_pct) {
            continue;
        }
        let bucket = ((price - mid_price) / bin_size).floor() as i64;
        let entry = grouped.entry(bucket).or_insert((0.0, 0.0, 0.0, 0.0));
        entry.0 += price * total.max(0.0);
        entry.1 += total.max(0.0);
        entry.2 += bid.max(0.0);
        entry.3 += ask.max(0.0);
    }

    let clusters = grouped
        .into_iter()
        .map(|(_, (weighted_price, total, bid, ask))| {
            let price_level = if total > f64::EPSILON {
                weighted_price / total
            } else {
                mid_price
            };
            let distance_pct = ((price_level - mid_price) / mid_price) * 100.0;
            json!({
                "price_level": price_level,
                "total_liquidity": total,
                "bid_liquidity": bid,
                "ask_liquidity": ask,
                "distance_pct": distance_pct,
            })
        })
        .collect::<Vec<_>>();

    let mut bid_walls = clusters
        .iter()
        .filter(|wall| {
            let distance_pct = wall
                .get("distance_pct")
                .and_then(Value::as_f64)
                .unwrap_or_default();
            let bid = wall
                .get("bid_liquidity")
                .and_then(Value::as_f64)
                .unwrap_or_default();
            (-15.0..=0.0).contains(&distance_pct) && bid > f64::EPSILON
        })
        .cloned()
        .collect::<Vec<_>>();
    bid_walls.sort_by(|left, right| {
        let left_bid = left
            .get("bid_liquidity")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        let right_bid = right
            .get("bid_liquidity")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        right_bid.partial_cmp(&left_bid).unwrap_or(Ordering::Equal)
    });

    let mut ask_walls = clusters
        .into_iter()
        .filter(|wall| {
            let distance_pct = wall
                .get("distance_pct")
                .and_then(Value::as_f64)
                .unwrap_or_default();
            let ask = wall
                .get("ask_liquidity")
                .and_then(Value::as_f64)
                .unwrap_or_default();
            (0.0..=15.0).contains(&distance_pct) && ask > f64::EPSILON
        })
        .collect::<Vec<_>>();
    ask_walls.sort_by(|left, right| {
        let left_ask = left
            .get("ask_liquidity")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        let right_ask = right
            .get("ask_liquidity")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        right_ask.partial_cmp(&left_ask).unwrap_or(Ordering::Equal)
    });

    json!({
        "bid_walls": bid_walls.into_iter().take(5).map(|wall| {
            json!({
                "price_level": wall.get("price_level").cloned().unwrap_or(Value::Null),
                "total_liquidity": wall.get("total_liquidity").cloned().unwrap_or(Value::Null),
                "distance_pct": wall.get("distance_pct").cloned().unwrap_or(Value::Null),
            })
        }).collect::<Vec<_>>(),
        "ask_walls": ask_walls.into_iter().take(5).map(|wall| {
            json!({
                "price_level": wall.get("price_level").cloned().unwrap_or(Value::Null),
                "total_liquidity": wall.get("total_liquidity").cloned().unwrap_or(Value::Null),
                "distance_pct": wall.get("distance_pct").cloned().unwrap_or(Value::Null),
            })
        }).collect::<Vec<_>>(),
        "depth_imbalance_1pct": depth_imbalance(levels, mid_price, 0.01),
        "depth_imbalance_3pct": depth_imbalance(levels, mid_price, 0.03),
    })
}

fn depth_imbalance(levels: &[Value], mid_price: f64, pct: f64) -> Value {
    let lower = mid_price * (1.0 - pct);
    let upper = mid_price * (1.0 + pct);
    let mut bid_sum = 0.0;
    let mut ask_sum = 0.0;
    for level in levels.iter().filter_map(Value::as_object) {
        let price_level = level
            .get("price_level")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        if !(lower..=upper).contains(&price_level) {
            continue;
        }
        bid_sum += level
            .get("bid_liquidity")
            .and_then(Value::as_f64)
            .unwrap_or_default()
            .max(0.0);
        ask_sum += level
            .get("ask_liquidity")
            .and_then(Value::as_f64)
            .unwrap_or_default()
            .max(0.0);
    }
    let total = bid_sum + ask_sum;
    if total <= f64::EPSILON {
        Value::Null
    } else {
        json!(bid_sum / total)
    }
}

fn top_divergence_candidates(candidates: &[Value], limit: usize) -> Vec<Value> {
    let mut sorted = candidates
        .iter()
        .filter_map(|entry| entry.as_object().cloned())
        .collect::<Vec<_>>();
    sorted.sort_by(|left, right| {
        let left_score = left
            .get("score")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        let right_score = right
            .get("score")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        right_score
            .partial_cmp(&left_score)
            .unwrap_or(Ordering::Equal)
    });
    sorted
        .into_iter()
        .take(limit)
        .map(|entry| {
            let mut filtered = Map::new();
            copy_fields(
                &mut filtered,
                &entry,
                &[
                    "type",
                    "score",
                    "sig_pass",
                    "price_start",
                    "price_end",
                    "likely_driver",
                ],
            );
            Value::Object(filtered)
        })
        .collect()
}

fn build_va_top_levels(levels: Option<&Vec<Value>>, limit: usize) -> Vec<Value> {
    let Some(levels) = levels else {
        return Vec::new();
    };

    let mut ranked = levels
        .iter()
        .filter_map(|entry| {
            if let Some(object) = entry.as_object() {
                let price = object
                    .get("price_level")
                    .or_else(|| object.get("price"))
                    .and_then(Value::as_f64)?;
                let volume = object
                    .get("volume")
                    .and_then(Value::as_f64)
                    .unwrap_or_default();
                Some((price, volume))
            } else {
                entry.as_f64().map(|price| (price, 0.0))
            }
        })
        .collect::<Vec<_>>();
    let total_volume = ranked
        .iter()
        .map(|(_, volume)| volume.max(0.0))
        .sum::<f64>();
    ranked.sort_by(|left, right| {
        right
            .1
            .partial_cmp(&left.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| right.0.partial_cmp(&left.0).unwrap_or(Ordering::Equal))
    });

    let mut top_levels = ranked
        .into_iter()
        .take(limit)
        .enumerate()
        .map(|(idx, (price, volume))| {
            json!({
                "price": price,
                "volume": volume,
                "vol_pct": if total_volume > f64::EPSILON { volume / total_volume } else { 0.0 },
                "vol_rank": idx + 1,
            })
        })
        .collect::<Vec<_>>();
    top_levels.sort_by(|left, right| {
        let left_price = left
            .get("price")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        let right_price = right
            .get("price")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        right_price
            .partial_cmp(&left_price)
            .unwrap_or(Ordering::Equal)
    });
    top_levels
}

fn aggregate_funding_hourly(entries: &[Value]) -> Vec<Value> {
    let mut grouped: BTreeMap<String, (f64, usize)> = BTreeMap::new();
    for entry in entries {
        let Some(entry) = entry.as_object() else {
            continue;
        };
        let Some(ts) = entry
            .get("change_ts")
            .and_then(Value::as_str)
            .and_then(hour_bucket_iso)
        else {
            continue;
        };
        let Some(funding_new) = entry.get("funding_new").and_then(Value::as_f64) else {
            continue;
        };
        let bucket = grouped.entry(ts).or_insert((0.0, 0));
        bucket.0 += funding_new;
        bucket.1 += 1;
    }

    grouped
        .into_iter()
        .rev()
        .map(|(ts, (sum, count))| {
            json!({
                "ts": ts,
                "avg_funding": if count > 0 { sum / count as f64 } else { 0.0 }
            })
        })
        .collect()
}

fn build_funding_trend_stats(hourly: &[Value]) -> Value {
    let series = hourly
        .iter()
        .rev()
        .filter_map(|entry| entry.get("avg_funding").and_then(Value::as_f64))
        .collect::<Vec<_>>();
    let avg_7d = if series.is_empty() {
        0.0
    } else {
        series.iter().sum::<f64>() / series.len() as f64
    };
    let pct_negative_hours = if series.is_empty() {
        0.0
    } else {
        series.iter().filter(|value| **value < 0.0).count() as f64 / series.len() as f64
    };
    let slope_per_hour = least_squares_slope(&series);

    json!({
        "avg_7d": avg_7d,
        "slope_per_hour": slope_per_hour,
        "pct_negative_hours": pct_negative_hours,
    })
}

fn build_funding_summary_alias(recent_events: Option<&Vec<Value>>) -> Value {
    let Some(events) = recent_events else {
        return json!({
            "ema_8h": Value::Null,
            "ema_24h": Value::Null,
            "z_score_7d": Value::Null,
            "consecutive_direction_hours": 0,
        });
    };

    let mut parsed = events
        .iter()
        .filter_map(Value::as_object)
        .filter_map(|entry| {
            Some((
                entry.get("change_ts")?.as_str()?.to_string(),
                entry.get("funding_new")?.as_f64()?,
            ))
        })
        .collect::<Vec<_>>();
    parsed.sort_by(|left, right| left.0.cmp(&right.0));

    let mut hourly: BTreeMap<String, (f64, usize)> = BTreeMap::new();
    let mut all_values = Vec::new();
    for (ts, funding_new) in &parsed {
        all_values.push(*funding_new);
        let Some(bucket_key) = hour_bucket_iso(ts) else {
            continue;
        };
        let bucket = hourly.entry(bucket_key).or_insert((0.0, 0));
        bucket.0 += *funding_new;
        bucket.1 += 1;
    }

    let hourly_values = hourly
        .into_iter()
        .map(|(_, (sum, count))| if count > 0 { sum / count as f64 } else { 0.0 })
        .collect::<Vec<_>>();
    let last_8h = take_tail_values(&hourly_values, 8);
    let last_24h = take_tail_values(&hourly_values, 24);
    let ema_8h = ema_value(&last_8h);
    let ema_24h = ema_value(&last_24h);
    let latest = all_values.last().copied();
    let z_score_7d = z_score_value(latest, &all_values);
    let consecutive_direction_hours = consecutive_direction_hours_value(&hourly_values);

    json!({
        "ema_8h": ema_8h,
        "ema_24h": ema_24h,
        "z_score_7d": z_score_7d,
        "consecutive_direction_hours": consecutive_direction_hours,
    })
}

fn take_tail_values(values: &[f64], limit: usize) -> Vec<f64> {
    if values.len() <= limit {
        values.to_vec()
    } else {
        values[values.len() - limit..].to_vec()
    }
}

fn ema_value(values: &[f64]) -> Value {
    if values.is_empty() {
        return Value::Null;
    }
    let alpha = 2.0 / (values.len() as f64 + 1.0);
    let mut ema_value = values[0];
    for value in values.iter().skip(1) {
        ema_value = alpha * *value + (1.0 - alpha) * ema_value;
    }
    json!(ema_value)
}

fn z_score_value(current: Option<f64>, values: &[f64]) -> Value {
    let Some(current) = current else {
        return Value::Null;
    };
    if values.is_empty() {
        return Value::Null;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / values.len() as f64;
    let std_dev = variance.sqrt();
    if std_dev > f64::EPSILON {
        json!((current - mean) / std_dev)
    } else {
        json!(0.0)
    }
}

fn consecutive_direction_hours_value(values: &[f64]) -> i64 {
    let Some(&latest) = values.last() else {
        return 0;
    };
    let sign = latest.partial_cmp(&0.0).unwrap_or(Ordering::Equal);
    if sign == Ordering::Equal {
        return 0;
    }
    let mut count = 0_i64;
    for value in values.iter().rev() {
        if value.partial_cmp(&0.0).unwrap_or(Ordering::Equal) == sign {
            count += 1;
        } else {
            break;
        }
    }
    if sign == Ordering::Less {
        -count
    } else {
        count
    }
}

fn aggregate_liquidation_hourly(entries: &[Value]) -> Vec<Value> {
    let mut grouped: BTreeMap<String, (f64, f64)> = BTreeMap::new();
    for entry in entries {
        let Some(entry) = entry.as_object() else {
            continue;
        };
        let Some(ts) = entry
            .get("ts_snapshot")
            .and_then(Value::as_str)
            .and_then(hour_bucket_iso)
        else {
            continue;
        };
        let long_total = entry
            .get("long_total")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        let short_total = entry
            .get("short_total")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        let bucket = grouped.entry(ts).or_insert((0.0, 0.0));
        bucket.0 += long_total;
        bucket.1 += short_total;
    }

    grouped
        .into_iter()
        .rev()
        .filter(|(_, (long_total, short_total))| {
            long_total.abs() + short_total.abs() > f64::EPSILON
        })
        .map(|(ts, (long_total, short_total))| {
            json!({
                "ts": ts,
                "long_total": long_total,
                "short_total": short_total
            })
        })
        .collect()
}

fn build_liquidation_spike_events(hourly: &[Value], limit: usize) -> Vec<Value> {
    if hourly.is_empty() {
        return Vec::new();
    }

    let totals = hourly
        .iter()
        .filter_map(|entry| {
            let long_total = entry.get("long_total").and_then(Value::as_f64)?;
            let short_total = entry.get("short_total").and_then(Value::as_f64)?;
            Some(long_total + short_total)
        })
        .collect::<Vec<_>>();
    if totals.is_empty() {
        return Vec::new();
    }
    let mean_total = totals.iter().sum::<f64>() / totals.len() as f64;

    hourly
        .iter()
        .filter_map(|entry| {
            let ts = entry.get("ts")?.clone();
            let long_total = entry.get("long_total").and_then(Value::as_f64)?;
            let short_total = entry.get("short_total").and_then(Value::as_f64)?;
            let total = long_total + short_total;
            (total > mean_total * 3.0).then(|| {
                json!({
                    "ts": ts,
                    "long_total": long_total,
                    "short_total": short_total,
                    "dominant": if long_total > short_total {
                        "long"
                    } else if short_total > long_total {
                        "short"
                    } else {
                        "mixed"
                    },
                })
            })
        })
        .take(limit)
        .collect()
}

fn least_squares_slope(series: &[f64]) -> f64 {
    if series.len() < 2 {
        return 0.0;
    }

    let n = series.len() as f64;
    let mean_x = (n - 1.0) / 2.0;
    let mean_y = series.iter().sum::<f64>() / n;

    let mut numerator = 0.0;
    let mut denominator = 0.0;
    for (idx, value) in series.iter().enumerate() {
        let x = idx as f64;
        numerator += (x - mean_x) * (value - mean_y);
        denominator += (x - mean_x).powi(2);
    }

    if denominator > f64::EPSILON {
        numerator / denominator
    } else {
        0.0
    }
}

fn hour_bucket_iso(ts: &str) -> Option<String> {
    let dt = chrono::DateTime::parse_from_rfc3339(ts).ok()?;
    let dt = dt.with_minute(0)?.with_second(0)?.with_nanosecond(0)?;
    Some(
        dt.with_timezone(&Utc)
            .to_rfc3339_opts(SecondsFormat::Secs, true),
    )
}

fn aggregate_price_clusters(prices: &[Value], step_ratio: f64, limit: usize) -> Vec<Value> {
    let raw_prices = prices
        .iter()
        .filter_map(Value::as_f64)
        .filter(|price| price.is_finite())
        .collect::<Vec<_>>();
    if raw_prices.is_empty() {
        return Vec::new();
    }

    let reference_price = raw_prices.iter().sum::<f64>() / raw_prices.len() as f64;
    let step = (reference_price.abs() * step_ratio).max(1e-9);

    let mut buckets: BTreeMap<i64, Vec<f64>> = BTreeMap::new();
    for price in raw_prices {
        let bucket = (price / step).round() as i64;
        buckets.entry(bucket).or_default().push(price);
    }

    let mut clusters = buckets
        .into_values()
        .map(|prices| {
            let level_count = prices.len();
            let min_price = prices
                .iter()
                .fold(f64::INFINITY, |acc, price| acc.min(*price));
            let max_price = prices
                .iter()
                .fold(f64::NEG_INFINITY, |acc, price| acc.max(*price));
            let center_price = (min_price + max_price) / 2.0;
            json!({
                "center_price": center_price,
                "level_count": level_count,
                "price_range": [min_price, max_price]
            })
        })
        .collect::<Vec<_>>();

    clusters.sort_by(|left, right| {
        let left_center = left
            .get("center_price")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        let right_center = right
            .get("center_price")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        right_center
            .partial_cmp(&left_center)
            .unwrap_or(Ordering::Equal)
    });
    clusters.truncate(limit);
    clusters
}

// ── v4 shared helpers (used by management_core and pending_core) ──────────────

pub(super) const MGMT_EVENT_INDICATOR_RULES: &[(&str, usize)] = &[
    ("absorption", 10),
    ("buying_exhaustion", 10),
    ("selling_exhaustion", 10),
    ("initiation", 10),
];

pub(super) const MGMT_EVENT_SUMMARY_FIELDS: &[(&str, &str)] = &[
    ("absorption", "most_recent_absorption"),
    ("initiation", "most_recent_initiation"),
    ("buying_exhaustion", "most_recent_buying_exhaustion"),
    ("selling_exhaustion", "most_recent_selling_exhaustion"),
    ("divergence", "most_recent_divergence"),
];

pub(super) const FVG_V4_KEEP_FIELDS: &[&str] = &[
    "fvg_bottom",
    "fvg_top",
    "side",
    "state",
    "fill_pct",
    "touch_count",
    "displacement_score",
    "age_bars",
    "width",
];

pub(super) const TPO_DEV_SERIES_LIMITS: &[(&str, usize)] = &[("15m", 8), ("1h", 5)];

pub(super) const FOOTPRINT_MIN_STACK_LENGTHS: &[(&str, usize)] =
    &[("15m", 3), ("4h", 4), ("1d", 7)];

pub(super) fn prune_nulls(value: Value) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .into_iter()
                .filter_map(|(key, value)| {
                    let pruned = prune_nulls(value);
                    if pruned.is_null() {
                        None
                    } else {
                        Some((key, pruned))
                    }
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(prune_nulls)
                .filter(|value| !value.is_null())
                .collect(),
        ),
        other => other,
    }
}

pub(super) fn parse_rfc3339_utc(ts: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(ts)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

pub(super) fn round2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

pub(super) fn truncate_tpo_dev_series_v4(value: Option<&mut Value>) {
    let Some(dev_series) = value.and_then(Value::as_object_mut) else {
        return;
    };
    for (window, limit) in TPO_DEV_SERIES_LIMITS {
        if let Some(series) = dev_series.get_mut(*window).and_then(Value::as_array_mut) {
            series.truncate(*limit);
        }
    }
}

pub(super) fn filter_stack_field_v4(
    window_value: &mut Map<String, Value>,
    field: &str,
    min_length: usize,
) {
    let Some(items) = window_value.get_mut(field).and_then(Value::as_array_mut) else {
        return;
    };
    items.retain(|entry| {
        entry
            .get("length")
            .and_then(Value::as_u64)
            .map(|length| length as usize >= min_length)
            .unwrap_or(false)
    });
}

pub(super) fn recompute_max_stack_len_v4(
    window_value: &mut Map<String, Value>,
    field: &str,
    max_field: &str,
) {
    let max_len = window_value
        .get(field)
        .and_then(Value::as_array)
        .and_then(|items| {
            items
                .iter()
                .filter_map(|entry| entry.get("length").and_then(Value::as_u64))
                .max()
        });
    match max_len {
        Some(value) => {
            window_value.insert(max_field.to_string(), Value::from(value));
        }
        None => {
            window_value.remove(max_field);
        }
    }
}

pub(super) fn sanitize_fvg_value_v4(value: Value) -> Value {
    let Some(entry) = value.as_object() else {
        return Value::Null;
    };
    let mut filtered = Map::new();

    if let Some(bottom) = entry
        .get("fvg_bottom")
        .cloned()
        .or_else(|| entry.get("lower").cloned())
    {
        filtered.insert("fvg_bottom".to_string(), bottom);
    }
    if let Some(top) = entry
        .get("fvg_top")
        .cloned()
        .or_else(|| entry.get("upper").cloned())
    {
        filtered.insert("fvg_top".to_string(), top);
    }

    for field in FVG_V4_KEEP_FIELDS {
        if matches!(*field, "fvg_bottom" | "fvg_top") {
            continue;
        }
        if let Some(field_value) = entry.get(*field) {
            filtered.insert((*field).to_string(), field_value.clone());
        }
    }

    let filtered = prune_nulls(Value::Object(filtered));
    match filtered {
        Value::Object(object) if object.is_empty() => Value::Null,
        other => other,
    }
}

pub(super) fn sanitize_fvg_object_field_v4(target: &mut Map<String, Value>, field: &str) {
    let Some(value) = target.remove(field) else {
        return;
    };
    let sanitized = sanitize_fvg_value_v4(value);
    if !sanitized.is_null() {
        target.insert(field.to_string(), sanitized);
    }
}

pub(super) fn sanitize_fvg_array_field_v4(target: &mut Map<String, Value>, field: &str) {
    let Some(items) = target
        .remove(field)
        .and_then(|value| value.as_array().cloned())
    else {
        return;
    };
    let sanitized: Vec<Value> = items
        .into_iter()
        .map(sanitize_fvg_value_v4)
        .filter(|value| !value.is_null())
        .collect();
    target.insert(field.to_string(), Value::Array(sanitized));
}

pub(super) fn extract_filtered_futures_bars_v4(
    indicators: &Map<String, Value>,
    tf: &str,
) -> Option<Vec<Map<String, Value>>> {
    indicators
        .get("kline_history")
        .and_then(|indicator| indicator.get("payload"))
        .and_then(|payload| payload.get("intervals"))
        .and_then(Value::as_object)
        .and_then(|intervals| intervals.get(tf))
        .and_then(Value::as_object)
        .and_then(|interval| interval.get("markets"))
        .and_then(Value::as_object)
        .and_then(|markets| markets.get("futures"))
        .and_then(Value::as_array)
        .map(|bars| {
            bars.iter()
                .filter_map(Value::as_object)
                .cloned()
                .collect::<Vec<_>>()
        })
}

pub(super) fn compute_atr14_v4(bars: &[Map<String, Value>]) -> Option<f64> {
    if bars.is_empty() {
        return None;
    }
    let mut true_ranges = Vec::new();
    let mut prev_close: Option<f64> = None;
    for bar in bars {
        let Some(high) = bar.get("high").and_then(Value::as_f64) else {
            continue;
        };
        let Some(low) = bar.get("low").and_then(Value::as_f64) else {
            continue;
        };
        let Some(close) = bar.get("close").and_then(Value::as_f64) else {
            continue;
        };
        let tr = if let Some(prev_close) = prev_close {
            (high - low)
                .max((high - prev_close).abs())
                .max((low - prev_close).abs())
        } else {
            high - low
        };
        true_ranges.push(tr);
        prev_close = Some(close);
    }
    if true_ranges.is_empty() {
        return None;
    }
    let window = true_ranges.len().min(14);
    Some(
        true_ranges[true_ranges.len() - window..]
            .iter()
            .sum::<f64>()
            / window as f64,
    )
}

fn event_reference_ts_v4(event: &Map<String, Value>) -> Option<DateTime<Utc>> {
    event_reference_ts_str_v4(event).and_then(parse_rfc3339_utc)
}

fn event_reference_ts_str_v4<'a>(event: &'a Map<String, Value>) -> Option<&'a str> {
    [
        "confirm_ts",
        "event_end_ts",
        "pivot_confirm_ts_2",
        "pivot_ts_2",
        "event_start_ts",
        "pivot_confirm_ts_1",
        "pivot_ts_1",
    ]
    .into_iter()
    .find_map(|field| event.get(field).and_then(Value::as_str))
}

fn most_recent_event_from_array_v4(events: &[Value]) -> Option<&Map<String, Value>> {
    events
        .iter()
        .filter_map(Value::as_object)
        .max_by_key(|event| event_reference_ts_v4(*event))
}

fn direction_label_from_sign_v4(value: f64) -> &'static str {
    if value > 0.0 {
        "bullish"
    } else if value < 0.0 {
        "bearish"
    } else {
        "neutral"
    }
}

fn event_direction_from_type_v4(event_type: &str) -> &'static str {
    let normalized = event_type.to_ascii_lowercase();
    if normalized.contains("bullish") || normalized == "selling_exhaustion" {
        "bullish"
    } else if normalized.contains("bearish") || normalized == "buying_exhaustion" {
        "bearish"
    } else {
        "neutral"
    }
}

fn event_direction_label_v4(event: &Map<String, Value>) -> &'static str {
    if let Some(direction) = event.get("direction") {
        if let Some(value) = direction.as_i64() {
            return direction_label_from_sign_v4(value as f64);
        }
        if let Some(value) = direction.as_f64() {
            return direction_label_from_sign_v4(value);
        }
        if let Some(value) = direction.as_str() {
            let normalized = value.to_ascii_lowercase();
            if normalized.contains("bull") {
                return "bullish";
            }
            if normalized.contains("bear") {
                return "bearish";
            }
        }
    }

    event
        .get("type")
        .and_then(Value::as_str)
        .map(event_direction_from_type_v4)
        .unwrap_or("neutral")
}

fn event_price_v4(event: &Map<String, Value>) -> Option<f64> {
    [
        "pivot_price",
        "price_end",
        "price_start",
        "price_high",
        "price_low",
    ]
    .into_iter()
    .find_map(|field| event.get(field).and_then(Value::as_f64))
}

fn build_most_recent_event_summary_v4(
    indicator: Option<&Value>,
    current_ts: Option<DateTime<Utc>>,
    current_price: Option<f64>,
    atr14: Option<f64>,
) -> Option<Value> {
    let payload = indicator?.get("payload")?.as_object()?;
    let event = payload
        .get("recent_7d")
        .and_then(Value::as_object)
        .and_then(|recent| recent.get("events"))
        .and_then(Value::as_array)
        .and_then(|events| most_recent_event_from_array_v4(events))
        .or_else(|| payload.get("latest_7d").and_then(Value::as_object))?;

    let mut summary = Map::new();
    if let Some(confirm_ts) = event_reference_ts_str_v4(event) {
        summary.insert(
            "confirm_ts".to_string(),
            Value::String(confirm_ts.to_string()),
        );
    }
    if let (Some(event_ts), Some(current_ts)) = (event_reference_ts_v4(event), current_ts) {
        let minutes_ago = (current_ts - event_ts).num_minutes().max(0);
        summary.insert("minutes_ago".to_string(), Value::from(minutes_ago));
    }
    summary.insert(
        "direction".to_string(),
        Value::String(event_direction_label_v4(event).to_string()),
    );
    if let Some(event_type) = event.get("type").cloned() {
        summary.insert("type".to_string(), event_type);
    }
    if let Some(price) = event_price_v4(event) {
        summary.insert("price".to_string(), Value::from(price));
        if let (Some(current_price), Some(atr14)) = (current_price, atr14) {
            if atr14 > 0.0 {
                summary.insert(
                    "price_distance_atr".to_string(),
                    Value::from(round2((price - current_price).abs() / atr14)),
                );
            }
        }
    }
    if let Some(score) = event.get("score").cloned() {
        summary.insert("score".to_string(), score);
    }
    if let Some(sig_pass) = event.get("sig_pass").cloned() {
        summary.insert("sig_pass".to_string(), sig_pass);
    }

    match prune_nulls(Value::Object(summary)) {
        Value::Object(object) if !object.is_empty() => Some(Value::Object(object)),
        _ => None,
    }
}

pub(super) fn build_events_summary_v4(
    indicators: &Map<String, Value>,
    current_ts: Option<DateTime<Utc>>,
    current_price: Option<f64>,
) -> Option<Map<String, Value>> {
    let atr14 = extract_filtered_futures_bars_v4(indicators, "15m")
        .as_deref()
        .and_then(compute_atr14_v4);
    let mut payload = Map::new();

    for (indicator_code, output_key) in MGMT_EVENT_SUMMARY_FIELDS {
        let summary = build_most_recent_event_summary_v4(
            indicators.get(*indicator_code),
            current_ts,
            current_price,
            atr14,
        );
        if let Some(summary) = summary {
            payload.insert((*output_key).to_string(), summary);
        }
    }

    (!payload.is_empty()).then_some(payload)
}
