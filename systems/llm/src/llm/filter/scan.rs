use crate::llm::provider::ModelInvocationInput;
use anyhow::{Context, Result};
use serde_json::{json, Map, Value};
use std::cmp::Ordering;
use std::collections::BTreeMap;

const SCAN_WINDOWS: &[&str] = &["15m", "4h", "1d"];
const SCAN_VA_TOP_LEVELS: usize = 5;
const EVENT_INDICATOR_RULES: &[(&str, usize)] = &[
    ("absorption", 20),
    ("buying_exhaustion", 20),
    ("selling_exhaustion", 20),
    ("initiation", usize::MAX),
    ("bullish_absorption", 20),
    ("bearish_absorption", 20),
    ("bullish_initiation", usize::MAX),
    ("bearish_initiation", 20),
];
const EVENT_DROP_FIELDS: &[&str] = &[
    "event_id",
    "end_ts",
    "start_ts",
    "event_available_ts",
    "indicator_code",
    "min_follow_required_minutes",
    "strength_score_xmk",
    "spot_rdelta_mean",
    "score_base",
    "spot_flow_confirm_score",
    "spot_whale_confirm_score",
    "spot_cvd_1m_change",
];
const INITIATION_EVENT_DROP_FIELDS: &[&str] = &[
    "follow_through_delta_sum",
    "follow_through_hold_ok",
    "follow_through_minutes",
    "follow_through_max_adverse_excursion_ticks",
    "spot_cvd_change",
    "spot_rdelta_1m_mean",
];
const DIVERGENCE_EVENT_DROP_FIELDS: &[&str] = &[
    "event_id",
    "end_ts",
    "start_ts",
    "event_available_ts",
    "price_norm_diff",
    "cvd_norm_diff_fut",
    "cvd_norm_diff_spot",
    "sig_test_mode",
];
const FVG_DROP_FIELDS: &[&str] = &["fvg_id", "event_available_ts", "tf"];

pub(crate) struct ScanFilter;

impl ScanFilter {
    pub(crate) fn build_value(input: &ModelInvocationInput) -> Result<Value> {
        let mut root =
            serde_json::to_value(input).context("serialize scan invocation input value")?;
        // Scan follows the docs: start from temp_indicator-equivalent rounded data,
        // then apply structural filtering/compression for the shared stage-1 scan.
        super::TempIndicatorInputOptimizer::round_derived_fields(&mut root);
        Ok(build_scan_root(&root))
    }

    pub(crate) fn serialize_minified_input(input: &ModelInvocationInput) -> Result<String> {
        super::serialize_prompt_value(
            Self::build_value(input)?,
            "serialize scan prompt input json",
        )
    }
}

fn build_scan_root(root: &Value) -> Value {
    let mut result = Map::new();
    if let Some(symbol) = root.get("symbol") {
        result.insert("symbol".to_string(), symbol.clone());
    }
    if let Some(ts_bucket) = root.get("ts_bucket") {
        result.insert("ts_bucket".to_string(), ts_bucket.clone());
    }

    let filtered_indicators = root
        .get("indicators")
        .and_then(Value::as_object)
        .map(filter_indicators)
        .unwrap_or_default();
    result.insert("indicators".to_string(), Value::Object(filtered_indicators));
    Value::Object(result)
}

fn filter_indicators(source: &Map<String, Value>) -> Map<String, Value> {
    let mut indicators = Map::new();

    for code in ["vpin", "whale_trades", "high_volume_pulse"] {
        insert_full_indicator(&mut indicators, source, code);
    }
    insert_filtered_indicator(
        &mut indicators,
        source,
        "tpo_market_profile",
        filter_tpo_market_profile,
    );

    insert_filtered_indicator(
        &mut indicators,
        source,
        "price_volume_structure",
        filter_price_volume_structure,
    );
    insert_filtered_indicator(&mut indicators, source, "fvg", filter_fvg);
    insert_filtered_indicator(
        &mut indicators,
        source,
        "kline_history",
        filter_kline_history,
    );
    insert_filtered_indicator(&mut indicators, source, "cvd_pack", filter_cvd_pack);
    insert_filtered_indicator(&mut indicators, source, "avwap", filter_avwap);
    insert_filtered_indicator(
        &mut indicators,
        source,
        "rvwap_sigma_bands",
        filter_rvwap_sigma_bands,
    );
    insert_filtered_indicator(
        &mut indicators,
        source,
        "ema_trend_regime",
        filter_ema_trend_regime,
    );
    insert_filtered_indicator(&mut indicators, source, "funding_rate", filter_funding_rate);
    insert_filtered_indicator(
        &mut indicators,
        source,
        "liquidation_density",
        filter_liquidation_density,
    );
    insert_filtered_indicator(&mut indicators, source, "footprint", filter_footprint);
    insert_filtered_indicator(
        &mut indicators,
        source,
        "orderbook_depth",
        filter_orderbook_depth,
    );

    for (code, keep_last) in EVENT_INDICATOR_RULES {
        if let Some(indicator) = source.get(*code) {
            let payload = indicator
                .get("payload")
                .map(|value| filter_event_indicator(value, code, *keep_last))
                .unwrap_or(Value::Null);
            indicators.insert(code.to_string(), rebuild_indicator(indicator, payload));
        }
    }

    insert_filtered_indicator(&mut indicators, source, "divergence", filter_divergence);

    indicators
}

fn insert_full_indicator(target: &mut Map<String, Value>, source: &Map<String, Value>, code: &str) {
    if let Some(indicator) = source.get(code) {
        let payload = indicator.get("payload").cloned().unwrap_or(Value::Null);
        target.insert(code.to_string(), rebuild_indicator(indicator, payload));
    }
}

fn insert_filtered_indicator(
    target: &mut Map<String, Value>,
    source: &Map<String, Value>,
    code: &str,
    filter_fn: fn(&Value) -> Value,
) {
    if let Some(indicator) = source.get(code) {
        let payload = indicator
            .get("payload")
            .map(filter_fn)
            .unwrap_or(Value::Null);
        target.insert(code.to_string(), rebuild_indicator(indicator, payload));
    }
}

fn rebuild_indicator(indicator: &Value, payload: Value) -> Value {
    let mut rebuilt = indicator
        .as_object()
        .map(|map| {
            map.iter()
                .filter(|(key, _)| key.as_str() == "payload")
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<Map<String, Value>>()
        })
        .unwrap_or_default();
    rebuilt.insert("payload".to_string(), payload);
    Value::Object(rebuilt)
}

fn filter_price_volume_structure(payload: &Value) -> Value {
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
            "bar_volume",
            "hvn_levels",
            "lvn_levels",
        ],
    );

    if let Some(by_window) = payload.get("by_window").and_then(Value::as_object) {
        let mut filtered_windows = Map::new();
        for window in SCAN_WINDOWS {
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
                    "bar_volume",
                    "hvn_levels",
                    "lvn_levels",
                    "window_bars_used",
                    "volume_zscore",
                    "volume_dryup",
                ],
            );
            filtered_window.insert(
                "va_top_levels".to_string(),
                Value::Array(build_va_top_levels(
                    window_value
                        .get("value_area_levels")
                        .and_then(Value::as_array),
                    SCAN_VA_TOP_LEVELS,
                )),
            );
            filtered_windows.insert((*window).to_string(), Value::Object(filtered_window));
        }
        result.insert("by_window".to_string(), Value::Object(filtered_windows));
    }

    Value::Object(result)
}

fn filter_tpo_market_profile(payload: &Value) -> Value {
    let mut payload = payload.clone();
    // Keep the full TPO payload, but normalize dev_series ordering to newest-first
    // so every retained scan timeseries follows the same prompt-facing convention.
    if let Some(dev_series) = payload.get_mut("dev_series").and_then(Value::as_object_mut) {
        for series in dev_series.values_mut() {
            if let Some(items) = series.as_array_mut() {
                items.reverse();
            }
        }
    }
    payload
}

fn filter_fvg(payload: &Value) -> Value {
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
        for window in SCAN_WINDOWS {
            let Some(window_value) = by_window.get(*window).and_then(Value::as_object) else {
                continue;
            };
            let mut filtered_window = Map::new();
            copy_fields(
                &mut filtered_window,
                window_value,
                &[
                    "fvgs",
                    "active_bull_fvgs",
                    "active_bear_fvgs",
                    "nearest_bull_fvg",
                    "nearest_bear_fvg",
                    "is_ready",
                    "coverage_ratio",
                ],
            );
            sanitize_fvg_object_field(&mut filtered_window, "nearest_bull_fvg");
            sanitize_fvg_object_field(&mut filtered_window, "nearest_bear_fvg");
            sanitize_fvg_array_field(&mut filtered_window, "fvgs");
            sanitize_fvg_array_field(&mut filtered_window, "active_bull_fvgs");
            sanitize_fvg_array_field(&mut filtered_window, "active_bear_fvgs");
            filtered_windows.insert((*window).to_string(), Value::Object(filtered_window));
        }
        result.insert("by_window".to_string(), Value::Object(filtered_windows));
    }

    Value::Object(result)
}

fn filter_kline_history(payload: &Value) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();
    copy_fields(&mut result, payload, &["as_of_ts"]);

    let Some(intervals) = payload.get("intervals").and_then(Value::as_object) else {
        return Value::Object(result);
    };

    let mut filtered_intervals = Map::new();
    for (interval, limit) in [("15m", 30usize), ("4h", 20usize), ("1d", 14usize)] {
        let Some(interval_obj) = intervals.get(interval).and_then(Value::as_object) else {
            continue;
        };
        let Some(markets) = interval_obj.get("markets").and_then(Value::as_object) else {
            continue;
        };
        let Some(futures_bars) = markets
            .get("futures")
            .and_then(Value::as_object)
            .and_then(|futures| futures.get("bars"))
            .and_then(Value::as_array)
        else {
            continue;
        };

        let bars = take_last_n(futures_bars, limit)
            .into_iter()
            .filter_map(|bar| bar.as_object().cloned())
            .map(|bar| {
                json!({
                    "o": bar.get("open").cloned().unwrap_or(Value::Null),
                    "h": bar.get("high").cloned().unwrap_or(Value::Null),
                    "l": bar.get("low").cloned().unwrap_or(Value::Null),
                    "c": bar.get("close").cloned().unwrap_or(Value::Null),
                    "v": bar.get("volume_base").cloned().unwrap_or(Value::Null),
                    "t": bar.get("open_time").cloned().unwrap_or(Value::Null),
                    "closed": bar.get("is_closed").cloned().unwrap_or(Value::Null),
                })
            })
            .collect::<Vec<_>>();

        filtered_intervals.insert(
            interval.to_string(),
            json!({
                "markets": {
                    "futures": bars
                }
            }),
        );
    }
    result.insert("intervals".to_string(), Value::Object(filtered_intervals));
    Value::Object(result)
}

fn filter_cvd_pack(payload: &Value) -> Value {
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
        for (window, limit) in [("15m", 30usize), ("4h", 20usize), ("1d", usize::MAX)] {
            let Some(window_value) = by_window.get(window).and_then(Value::as_object) else {
                continue;
            };
            let series = window_value
                .get("series")
                .and_then(Value::as_array)
                .map(|series| take_last_n(series, limit))
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
                            "cvd_7d_spot",
                            "spot_flow_dominance",
                            "xmk_delta_gap_s_minus_f",
                        ],
                    );
                    Value::Object(filtered_entry)
                })
                .collect::<Vec<_>>();
            filtered_windows.insert(window.to_string(), json!({ "series": series }));
        }
        result.insert("by_window".to_string(), Value::Object(filtered_windows));
    }

    Value::Object(result)
}

fn filter_avwap(payload: &Value) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result =
        clone_object_without_keys(payload, &["series_by_window", "indicator", "window"]);

    if let Some(series_by_window) = payload.get("series_by_window").and_then(Value::as_object) {
        let mut filtered_windows = Map::new();
        for (window, limit) in [("15m", 10usize), ("4h", 5usize), ("1d", usize::MAX)] {
            let Some(series) = series_by_window.get(window).and_then(Value::as_array) else {
                continue;
            };
            let filtered_series = take_last_n(series, limit)
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
            filtered_windows.insert(window.to_string(), Value::Array(filtered_series));
        }
        result.insert(
            "series_by_window".to_string(),
            Value::Object(filtered_windows),
        );
    }

    Value::Object(result)
}

fn filter_rvwap_sigma_bands(payload: &Value) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = clone_object_without_keys(
        payload,
        &[
            "by_window",
            "series_by_output_window",
            "indicator",
            "window",
        ],
    );

    if let Some(by_window) = payload.get("by_window").and_then(Value::as_object) {
        let mut filtered_windows = Map::new();
        for window in SCAN_WINDOWS {
            if let Some(window_value) = by_window.get(*window) {
                filtered_windows.insert((*window).to_string(), window_value.clone());
            }
        }
        result.insert("by_window".to_string(), Value::Object(filtered_windows));
    }

    let z_series = payload
        .get("series_by_output_window")
        .and_then(Value::as_object)
        .and_then(|series_by_output| series_by_output.get("15m"))
        .and_then(Value::as_array)
        .map(|series| take_last_n(series, 20))
        .unwrap_or_default()
        .into_iter()
        .filter_map(|entry| entry.as_object().cloned())
        .map(|entry| {
            let mut z = Map::new();
            if let Some(by_window) = entry.get("by_window").and_then(Value::as_object) {
                for window in SCAN_WINDOWS {
                    if let Some(z_value) = by_window
                        .get(*window)
                        .and_then(Value::as_object)
                        .and_then(|window_obj| window_obj.get("z_price_minus_rvwap"))
                    {
                        z.insert((*window).to_string(), z_value.clone());
                    }
                }
            }
            json!({
                "ts": entry.get("ts").cloned().unwrap_or(Value::Null),
                "z": z
            })
        })
        .collect::<Vec<_>>();
    result.insert("z_series_15m".to_string(), Value::Array(z_series));

    Value::Object(result)
}

fn filter_ema_trend_regime(payload: &Value) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = clone_object_without_keys(
        payload,
        &["ffill_series_by_output_window", "indicator", "window"],
    );

    let regime_series = payload
        .get("ffill_series_by_output_window")
        .and_then(Value::as_object)
        .and_then(|series_by_output| series_by_output.get("15m"))
        .and_then(Value::as_array)
        .map(|series| take_last_n(series, 20))
        .unwrap_or_default()
        .into_iter()
        .filter_map(|entry| entry.as_object().cloned())
        .map(|entry| {
            let by_tf = entry.get("by_tf").and_then(Value::as_object);
            json!({
                "ts": entry.get("ts").cloned().unwrap_or(Value::Null),
                "trend_regime": entry.get("trend_regime").cloned().unwrap_or(Value::Null),
                "trend_regime_4h": by_tf
                    .and_then(|value| value.get("4h"))
                    .and_then(Value::as_object)
                    .and_then(|value| value.get("trend_regime"))
                    .cloned()
                    .unwrap_or(Value::Null),
                "trend_regime_1d": by_tf
                    .and_then(|value| value.get("1d"))
                    .and_then(Value::as_object)
                    .and_then(|value| value.get("trend_regime"))
                    .cloned()
                    .unwrap_or(Value::Null),
            })
        })
        .collect::<Vec<_>>();
    result.insert("regime_series_15m".to_string(), Value::Array(regime_series));

    Value::Object(result)
}

fn filter_funding_rate(payload: &Value) -> Value {
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
        for window in SCAN_WINDOWS {
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
                .map(|changes| take_last_n(changes, 10))
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

    result.insert(
        "funding_summary".to_string(),
        build_funding_summary(payload.get("recent_7d").and_then(Value::as_array)),
    );

    Value::Object(result)
}

fn filter_liquidation_density(payload: &Value) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();

    if let Some(by_window) = payload.get("by_window").and_then(Value::as_object) {
        let mut filtered_windows = Map::new();
        for window in SCAN_WINDOWS {
            let Some(window_value) = by_window.get(*window).and_then(Value::as_object) else {
                continue;
            };
            let mut filtered_window = Map::new();
            copy_fields(
                &mut filtered_window,
                window_value,
                &["long_total", "short_total", "peak_levels", "coverage_ratio"],
            );
            filtered_windows.insert((*window).to_string(), Value::Object(filtered_window));
        }
        result.insert("by_window".to_string(), Value::Object(filtered_windows));
    }

    result.insert(
        "liq_summary".to_string(),
        build_liq_summary(payload.get("recent_7d").and_then(Value::as_array)),
    );

    Value::Object(result)
}

fn filter_footprint(payload: &Value) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();
    let Some(by_window) = payload.get("by_window").and_then(Value::as_object) else {
        return Value::Object(result);
    };

    let mut filtered_windows = Map::new();
    for (window, bin_size) in [("15m", 0.1_f64), ("4h", 0.5_f64), ("1d", 1.0_f64)] {
        let Some(window_value) = by_window.get(window).and_then(Value::as_object) else {
            continue;
        };
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
            ],
        );
        let buy_clusters = window_value
            .get("buy_imbalance_prices")
            .and_then(Value::as_array)
            .map(|prices| aggregate_price_clusters(prices, bin_size))
            .unwrap_or_default();
        let sell_clusters = window_value
            .get("sell_imbalance_prices")
            .and_then(Value::as_array)
            .map(|prices| aggregate_price_clusters(prices, bin_size))
            .unwrap_or_default();
        filtered_window.insert("buy_imb_clusters".to_string(), Value::Array(buy_clusters));
        filtered_window.insert("sell_imb_clusters".to_string(), Value::Array(sell_clusters));
        filtered_windows.insert(window.to_string(), Value::Object(filtered_window));
    }
    result.insert("by_window".to_string(), Value::Object(filtered_windows));

    Value::Object(result)
}

fn filter_orderbook_depth(payload: &Value) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = clone_object_without_keys(payload, &["levels", "by_window"]);

    result.insert(
        "liquidity_walls".to_string(),
        build_liquidity_walls(
            payload
                .get("levels")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or(&[]),
            payload.get("microprice_fut").and_then(Value::as_f64),
        ),
    );

    if let Some(by_window) = payload.get("by_window").and_then(Value::as_object) {
        let mut filtered_windows = Map::new();
        if let Some(window_value) = by_window.get("15m").and_then(Value::as_object) {
            filtered_windows.insert(
                "15m".to_string(),
                Value::Object(clone_object_without_keys(window_value, &[])),
            );
        }
        result.insert("by_window".to_string(), Value::Object(filtered_windows));
    }

    Value::Object(result)
}

fn filter_event_indicator(payload: &Value, code: &str, keep_last: usize) -> Value {
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
        &[
            "event_count",
            "history_source",
            "lookback_coverage_ratio",
            "lookback_covered_minutes",
            "lookback_missing_minutes",
            "lookback_requested_minutes",
        ],
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
                .map(|event| prune_event_fields(&event, code))
                .collect(),
        ),
    );
    result.insert("recent_7d".to_string(), Value::Object(filtered_recent));
    Value::Object(result)
}

fn filter_divergence(payload: &Value) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();
    copy_fields(
        &mut result,
        payload,
        &[
            "signal",
            "signals",
            "event_count",
            "divergence_type",
            "likely_driver",
            "spot_lead_score",
            "pivot_side",
            "reason",
        ],
    );
    if let Some(latest) = payload.get("latest_7d") {
        result.insert(
            "latest_7d".to_string(),
            prune_object_fields(latest, DIVERGENCE_EVENT_DROP_FIELDS),
        );
    }

    if let Some(recent_7d) = payload.get("recent_7d").and_then(Value::as_object) {
        let mut filtered_recent = Map::new();
        copy_fields(&mut filtered_recent, recent_7d, &["event_count"]);
        let events = recent_7d
            .get("events")
            .and_then(Value::as_array)
            .map(|events| take_last_n(events, 20))
            .unwrap_or_default();
        filtered_recent.insert(
            "events".to_string(),
            Value::Array(
                events
                    .into_iter()
                    .map(|event| prune_object_fields(&event, DIVERGENCE_EVENT_DROP_FIELDS))
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

fn copy_fields(target: &mut Map<String, Value>, source: &Map<String, Value>, fields: &[&str]) {
    for field in fields {
        if let Some(value) = source.get(*field) {
            target.insert((*field).to_string(), value.clone());
        }
    }
}

fn clone_object_without_keys(source: &Map<String, Value>, skipped: &[&str]) -> Map<String, Value> {
    source
        .iter()
        .filter(|(key, _)| !skipped.contains(&key.as_str()))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn prune_object_fields(value: &Value, skipped: &[&str]) -> Value {
    let Some(source) = value.as_object() else {
        return value.clone();
    };
    Value::Object(clone_object_without_keys(source, skipped))
}

fn prune_event_fields(value: &Value, code: &str) -> Value {
    let mut pruned = prune_object_fields(value, EVENT_DROP_FIELDS);
    if matches!(
        code,
        "initiation" | "bullish_initiation" | "bearish_initiation"
    ) {
        pruned = prune_object_fields(&pruned, INITIATION_EVENT_DROP_FIELDS);
    }
    pruned
}

fn take_last_n(values: &[Value], n: usize) -> Vec<Value> {
    let mut result = if n == usize::MAX || values.len() <= n {
        values.to_vec()
    } else {
        values[values.len() - n..].to_vec()
    };
    // Scan keeps the latest retained item closest to the top of the prompt input.
    result.reverse();
    result
}

fn hour_bucket_key(ts: &str) -> String {
    ts.chars().take(13).collect()
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
                    .map(|item| sanitize_fvg_value(&item))
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
    for field in FVG_DROP_FIELDS {
        filtered.remove(*field);
    }
    if let Some(upper) = filtered.get("upper").cloned() {
        filtered.insert("fvg_top".to_string(), upper);
    }
    if let Some(lower) = filtered.get("lower").cloned() {
        filtered.insert("fvg_bottom".to_string(), lower);
    }
    Value::Object(filtered)
}

fn build_funding_summary(recent_events: Option<&Vec<Value>>) -> Value {
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
        let bucket = hourly.entry(hour_bucket_key(ts)).or_insert((0.0, 0));
        bucket.0 += *funding_new;
        bucket.1 += 1;
    }

    let hourly_values = hourly
        .into_iter()
        .map(|(_, (sum, count))| if count > 0 { sum / count as f64 } else { 0.0 })
        .collect::<Vec<_>>();
    let last_8h = take_tail(&hourly_values, 8);
    let last_24h = take_tail(&hourly_values, 24);
    let ema_8h = ema(&last_8h);
    let ema_24h = ema(&last_24h);
    let latest = all_values.last().copied();
    let z_score_7d = z_score(latest, &all_values);
    let consecutive_direction_hours = consecutive_direction_hours(&hourly_values);

    json!({
        "ema_8h": ema_8h,
        "ema_24h": ema_24h,
        "z_score_7d": z_score_7d,
        "consecutive_direction_hours": consecutive_direction_hours,
    })
}

fn build_liq_summary(recent_events: Option<&Vec<Value>>) -> Value {
    let Some(events) = recent_events else {
        return json!({
            "long_24h": 0.0,
            "short_24h": 0.0,
            "ratio_24h": Value::Null,
            "long_7d": 0.0,
            "short_7d": 0.0,
            "intensity_z7d": Value::Null,
        });
    };

    let mut parsed = events
        .iter()
        .filter_map(Value::as_object)
        .filter_map(|entry| {
            let ts = entry.get("ts_snapshot").and_then(Value::as_str)?;
            let long_total = entry
                .get("long_total")
                .and_then(Value::as_f64)
                .unwrap_or_default()
                .abs();
            let short_total = entry
                .get("short_total")
                .and_then(Value::as_f64)
                .unwrap_or_default()
                .abs();
            Some((ts.to_string(), long_total, short_total))
        })
        .collect::<Vec<_>>();
    parsed.sort_by(|left, right| left.0.cmp(&right.0));

    let Some((latest_ts, _, _)) = parsed.last() else {
        return json!({
            "long_24h": 0.0,
            "short_24h": 0.0,
            "ratio_24h": Value::Null,
            "long_7d": 0.0,
            "short_7d": 0.0,
            "intensity_z7d": Value::Null,
        });
    };
    let latest_dt = match chrono::DateTime::parse_from_rfc3339(latest_ts) {
        Ok(dt) => dt.with_timezone(&chrono::Utc),
        Err(_) => {
            return json!({
                "long_24h": 0.0,
                "short_24h": 0.0,
                "ratio_24h": Value::Null,
                "long_7d": 0.0,
                "short_7d": 0.0,
                "intensity_z7d": Value::Null,
            });
        }
    };

    let cutoff_24h = latest_dt - chrono::Duration::hours(24);
    let mut long_24h = 0.0;
    let mut short_24h = 0.0;
    let mut long_7d = 0.0;
    let mut short_7d = 0.0;
    let mut daily_totals: BTreeMap<String, f64> = BTreeMap::new();
    for (ts, long_total, short_total) in &parsed {
        let dt = match chrono::DateTime::parse_from_rfc3339(ts) {
            Ok(dt) => dt.with_timezone(&chrono::Utc),
            Err(_) => continue,
        };
        long_7d += *long_total;
        short_7d += *short_total;
        if dt >= cutoff_24h {
            long_24h += *long_total;
            short_24h += *short_total;
        }
        *daily_totals
            .entry(dt.format("%Y-%m-%d").to_string())
            .or_default() += long_total + short_total;
    }
    let ratio_24h = ratio(long_24h, short_24h);
    let intensity_z7d = z_score(
        Some(long_24h + short_24h),
        &daily_totals.into_values().collect::<Vec<_>>(),
    );

    json!({
        "long_24h": long_24h,
        "short_24h": short_24h,
        "ratio_24h": ratio_24h,
        "long_7d": long_7d,
        "short_7d": short_7d,
        "intensity_z7d": intensity_z7d,
    })
}

fn aggregate_price_clusters(prices: &[Value], bin_size: f64) -> Vec<Value> {
    let mut bins: BTreeMap<i64, usize> = BTreeMap::new();
    for price in prices.iter().filter_map(Value::as_f64) {
        let index = (price / bin_size).round() as i64;
        *bins.entry(index).or_default() += 1;
    }

    bins.into_iter()
        .map(|(index, count)| {
            json!({
                "p": bucket_index_to_price(index, bin_size),
                "n": count
            })
        })
        .collect()
}

fn bucket_index_to_price(index: i64, bin_size: f64) -> f64 {
    let decimals = if (bin_size - 1.0).abs() < f64::EPSILON {
        0
    } else {
        1
    };
    round_to_decimals(index as f64 * bin_size, decimals)
}

fn round_to_decimals(value: f64, decimals: usize) -> f64 {
    let factor = 10_f64.powi(decimals as i32);
    (value * factor).round() / factor
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
        .map(|(price, volume)| {
            json!({
                "price": price,
                "volume": volume,
                "vol_pct": if total_volume > f64::EPSILON { volume / total_volume } else { 0.0 },
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

fn take_tail(values: &[f64], limit: usize) -> Vec<f64> {
    if values.len() <= limit {
        values.to_vec()
    } else {
        values[values.len() - limit..].to_vec()
    }
}

fn ema(values: &[f64]) -> Value {
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

fn z_score(current: Option<f64>, values: &[f64]) -> Value {
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

fn consecutive_direction_hours(values: &[f64]) -> i64 {
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

fn ratio(long_total: f64, short_total: f64) -> Value {
    let total = long_total + short_total;
    if total > f64::EPSILON {
        json!(long_total / total)
    } else {
        Value::Null
    }
}

fn depth_imbalance(levels: &[Value], mid_price: f64, pct: f64) -> Value {
    let lower = mid_price * (1.0 - pct);
    let upper = mid_price * (1.0 + pct);
    let mut bid_total = 0.0;
    let mut ask_total = 0.0;
    for level in levels.iter().filter_map(Value::as_object) {
        let Some(price_level) = level.get("price_level").and_then(Value::as_f64) else {
            continue;
        };
        if !(lower..=upper).contains(&price_level) {
            continue;
        }
        bid_total += level
            .get("bid_liquidity")
            .and_then(Value::as_f64)
            .unwrap_or_default()
            .max(0.0);
        ask_total += level
            .get("ask_liquidity")
            .and_then(Value::as_f64)
            .unwrap_or_default()
            .max(0.0);
    }
    ratio(bid_total, ask_total)
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
                    "fut_divergence_sign",
                ],
            );
            Value::Object(filtered)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::ScanFilter;
    use crate::llm::provider::ModelInvocationInput;
    use chrono::{Duration, Utc};
    use serde_json::{json, Value};
    use std::fs;
    use std::path::PathBuf;

    fn sample_input(indicators: Value) -> ModelInvocationInput {
        let now = Utc::now();
        ModelInvocationInput {
            symbol: "ETHUSDT".to_string(),
            ts_bucket: now,
            window_code: "15m".to_string(),
            indicator_count: indicators.as_object().map(|obj| obj.len()).unwrap_or(0),
            source_routing_key: "llm_indicator_minute".to_string(),
            source_published_at: None,
            received_at: now,
            indicators,
            missing_indicator_codes: vec![],
            management_mode: false,
            pending_order_mode: false,
            trading_state: None,
            management_snapshot: None,
        }
    }

    fn assert_newest_first(scan_value: &Value, array_pointer: &str, ts_field: &str) {
        let series = scan_value
            .pointer(array_pointer)
            .and_then(Value::as_array)
            .unwrap_or_else(|| panic!("missing array at {array_pointer}"));
        let first_ts = series
            .first()
            .and_then(|entry| entry.get(ts_field))
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("missing first {ts_field} at {array_pointer}"));
        let last_ts = series
            .last()
            .and_then(|entry| entry.get(ts_field))
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("missing last {ts_field} at {array_pointer}"));
        assert!(
            first_ts >= last_ts,
            "expected newest-first ordering at {array_pointer}, got first={first_ts}, last={last_ts}"
        );
    }

    fn latest_temp_indicator_sample_path() -> Option<PathBuf> {
        fs::read_dir("/data/systems/llm/temp_indicator")
            .ok()?
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .map(|name| {
                        name.ends_with("_ETHUSDT.json")
                            && name.chars().next().is_some_and(|ch| ch.is_ascii_digit())
                    })
                    .unwrap_or(false)
            })
            .max()
    }

    fn sample_value_area_levels(start_price: f64, step: f64, count: usize) -> Vec<Value> {
        (0..count)
            .map(|idx| {
                json!({
                    "price_level": start_price + step * idx as f64,
                    "volume": (count - idx) as f64 * 10.0 + 1.0
                })
            })
            .collect()
    }

    fn sample_scan_events(start: &str, step_minutes: i64, count: usize) -> Vec<Value> {
        let start = chrono::DateTime::parse_from_rfc3339(start)
            .expect("parse event start")
            .with_timezone(&Utc);
        (0..count)
            .map(|idx| {
                let ts = (start + Duration::minutes(step_minutes * idx as i64))
                    .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
                json!({
                    "event_id": format!("evt-{idx}"),
                    "indicator_code": "absorption",
                    "start_ts": ts,
                    "end_ts": ts,
                    "event_start_ts": ts,
                    "event_end_ts": ts,
                    "event_available_ts": ts,
                    "min_follow_required_minutes": 5,
                    "strength_score_xmk": idx as f64 / 10.0,
                    "spot_rdelta_mean": 1.0,
                    "score_base": idx as f64 / 20.0,
                    "spot_flow_confirm_score": 0.7,
                    "spot_whale_confirm_score": 0.6,
                    "spot_cvd_1m_change": 2.0,
                    "pivot_price": 2100.0 + idx as f64,
                    "score": idx as f64 / 10.0
                })
            })
            .collect()
    }

    fn sample_initiation_events(start: &str, step_minutes: i64, count: usize) -> Vec<Value> {
        sample_scan_events(start, step_minutes, count)
            .into_iter()
            .enumerate()
            .map(|(idx, event)| {
                let mut object = event.as_object().cloned().expect("initiation event object");
                object.insert(
                    "follow_through_delta_sum".to_string(),
                    json!(idx as f64 + 10.0),
                );
                object.insert("follow_through_hold_ok".to_string(), json!(idx % 2 == 0));
                object.insert("follow_through_minutes".to_string(), json!(idx as i64 + 5));
                object.insert(
                    "follow_through_max_adverse_excursion_ticks".to_string(),
                    json!(idx as i64 + 2),
                );
                object.insert("spot_cvd_change".to_string(), json!(idx as f64 / 5.0));
                object.insert("spot_rdelta_1m_mean".to_string(), json!(idx as f64 / 7.0));
                Value::Object(object)
            })
            .collect()
    }

    fn sample_divergence_events(start: &str, step_minutes: i64, count: usize) -> Vec<Value> {
        let start = chrono::DateTime::parse_from_rfc3339(start)
            .expect("parse divergence start")
            .with_timezone(&Utc);
        (0..count)
            .map(|idx| {
                let ts = (start + Duration::minutes(step_minutes * idx as i64))
                    .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
                json!({
                    "event_id": format!("div-{idx}"),
                    "start_ts": ts,
                    "end_ts": ts,
                    "event_available_ts": ts,
                    "event_start_ts": ts,
                    "event_end_ts": ts,
                    "price_diff": idx as f64 + 1.0,
                    "price_norm_diff": idx as f64 + 1.0,
                    "score": idx as f64 / 10.0,
                    "cvd_norm_diff_fut": 1.0,
                    "cvd_norm_diff_spot": 2.0,
                    "cvd_diff_fut": 1.0,
                    "cvd_diff_spot": 2.0,
                    "sig_test_mode": "derived"
                })
            })
            .collect()
    }

    fn sample_fvg_items(count: usize) -> Vec<Value> {
        (0..count)
            .map(|idx| {
                json!({
                    "birth_ts": format!("2026-03-14T0{}:00:00Z", idx),
                    "upper": 2000.0 + idx as f64,
                    "lower": 1999.0 + idx as f64,
                    "fvg_id": format!("fvg-{idx}"),
                    "event_available_ts": "2026-03-14T00:00:00Z",
                    "tf": "15m"
                })
            })
            .collect()
    }

    fn sample_fvg_item() -> Value {
        sample_fvg_items(1)
            .into_iter()
            .next()
            .expect("sample fvg item")
    }

    #[test]
    fn build_value_applies_doc_rules_for_scan_input() {
        let input = sample_input(json!({
            "price_volume_structure": {
                "window_code": "1m",
                "payload": {
                    "poc_price": 2100.1234,
                    "poc_volume": 12.0,
                    "vah": 2110.0,
                    "val": 2090.0,
                    "bar_volume": 100.0,
                    "hvn_levels": [2101.0],
                    "lvn_levels": [2098.0],
                    "value_area_levels": sample_value_area_levels(2090.0, 0.5, 7),
                    "levels": [{"price": 2100.0}],
                    "by_window": {
                        "15m": {"window_bars_used": 10, "poc_price": 2100.0, "poc_volume": 12.0, "vah": 2110.0, "val": 2090.0, "bar_volume": 100.0, "hvn_levels": [2101.0], "lvn_levels": [2098.0], "value_area_levels": sample_value_area_levels(2090.0, 0.25, 8), "volume_zscore": 1.2, "volume_dryup": false},
                        "4h": {"window_bars_used": 20, "poc_price": 2100.0, "poc_volume": 12.0, "vah": 2110.0, "val": 2090.0, "bar_volume": 100.0, "hvn_levels": [2101.0], "lvn_levels": [2098.0], "value_area_levels": sample_value_area_levels(2088.0, 0.5, 7), "volume_zscore": 1.2, "volume_dryup": false},
                        "1d": {"window_bars_used": 14, "poc_price": 2100.0, "poc_volume": 12.0, "vah": 2110.0, "val": 2090.0, "bar_volume": 100.0, "hvn_levels": [2101.0], "lvn_levels": [2098.0], "value_area_levels": sample_value_area_levels(2085.0, 1.0, 9), "volume_zscore": 1.2, "volume_dryup": false},
                        "3d": {"window_bars_used": 30}
                    }
                }
            },
            "fvg": {
                "window_code": "1m",
                "payload": {
                    "base_detection_uses_spot": true,
                    "source_market": "futures",
                    "by_window": {
                        "15m": {
                            "fvgs": sample_fvg_items(2),
                            "active_bull_fvgs": sample_fvg_items(1),
                            "active_bear_fvgs": sample_fvg_items(1),
                            "nearest_bull_fvg": sample_fvg_item(),
                            "nearest_bear_fvg": sample_fvg_item(),
                            "is_ready": true,
                            "coverage_ratio": 1.0
                        },
                        "4h": {
                            "fvgs": sample_fvg_items(1),
                            "active_bull_fvgs": [],
                            "active_bear_fvgs": [],
                            "nearest_bull_fvg": sample_fvg_item(),
                            "nearest_bear_fvg": sample_fvg_item(),
                            "is_ready": true,
                            "coverage_ratio": 1.0
                        },
                        "1d": {
                            "fvgs": sample_fvg_items(1),
                            "active_bull_fvgs": [],
                            "active_bear_fvgs": [],
                            "nearest_bull_fvg": sample_fvg_item(),
                            "nearest_bear_fvg": sample_fvg_item(),
                            "is_ready": true,
                            "coverage_ratio": 1.0
                        }
                    }
                }
            },
            "kline_history": {
                "window_code": "1m",
                "payload": {
                    "as_of_ts": "2026-03-14T08:00:00Z",
                    "intervals": {
                        "15m": {
                            "markets": {
                                "futures": {
                                    "bars": [
                                        {"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume_base": 100.0, "open_time": "2026-03-14T07:30:00Z", "is_closed": true},
                                        {"open": 1.1, "high": 2.1, "low": 0.6, "close": 1.6, "volume_base": 101.0, "open_time": "2026-03-14T07:45:00Z", "is_closed": true}
                                    ]
                                },
                                "spot": {
                                    "bars": [
                                        {"open": 9.0}
                                    ]
                                }
                            }
                        },
                        "4h": {
                            "markets": {
                                "futures": {
                                    "bars": [
                                        {"open": 3.0, "high": 4.0, "low": 2.0, "close": 3.5, "volume_base": 200.0, "open_time": "2026-03-13T20:00:00Z", "is_closed": true}
                                    ]
                                }
                            }
                        },
                        "1d": {
                            "markets": {
                                "futures": {
                                    "bars": [
                                        {"open": 5.0, "high": 6.0, "low": 4.0, "close": 5.5, "volume_base": 300.0, "open_time": "2026-03-13T00:00:00Z", "is_closed": true}
                                    ]
                                }
                            }
                        },
                        "1m": {
                            "markets": {
                                "futures": {
                                    "bars": [
                                        {"open": 7.0}
                                    ]
                                }
                            }
                        }
                    }
                }
            },
            "funding_rate": {
                "window_code": "1m",
                "payload": {
                    "funding_current": -0.00004527,
                    "funding_twa": -0.00003111,
                    "mark_price_last": 2001.5678,
                    "mark_price_last_ts": "2026-03-14T08:00:00Z",
                    "mark_price_twap": 2000.1111,
                    "funding_current_effective_ts": "2026-03-14T08:00:00Z",
                    "by_window": {
                        "15m": {
                            "funding_twa": -0.0000123,
                            "change_count": 2,
                            "changes": [
                                {"change_ts": "2026-03-14T07:00:00Z", "funding_delta": -0.0000002, "funding_new": -0.0000123, "funding_prev": -0.0000121},
                                {"change_ts": "2026-03-14T07:15:00Z", "funding_delta": -0.0000003, "funding_new": -0.0000126, "funding_prev": -0.0000123}
                            ]
                        },
                        "1h": {"funding_twa": -0.1},
                        "4h": {"funding_twa": -0.0000222, "change_count": 1, "changes": []},
                        "1d": {"funding_twa": -0.0000333, "change_count": 1, "changes": []},
                        "3d": {"funding_twa": -0.0000444, "change_count": 1, "changes": []}
                    },
                    "recent_7d": [
                        {"change_ts": "2026-03-14T07:01:00Z", "funding_new": -0.00001},
                        {"change_ts": "2026-03-14T07:22:00Z", "funding_new": -0.00003},
                        {"change_ts": "2026-03-14T08:05:00Z", "funding_new": 0.00002}
                    ]
                }
            },
            "liquidation_density": {
                "window_code": "1m",
                "payload": {
                    "by_window": {
                        "15m": {"long_total": 1.0, "short_total": 2.0, "peak_levels": [{"price": 2100.0}], "coverage_ratio": 1.0},
                        "4h": {"long_total": 3.0, "short_total": 4.0, "peak_levels": [], "coverage_ratio": 1.0},
                        "1d": {"long_total": 5.0, "short_total": 6.0, "peak_levels": [], "coverage_ratio": 1.0}
                    },
                    "recent_7d": [
                        {"ts_snapshot": "2026-03-13T08:00:00Z", "long_total": 10.0, "short_total": 20.0},
                        {"ts_snapshot": "2026-03-14T08:00:00Z", "long_total": 15.0, "short_total": 30.0},
                        {"ts_snapshot": "2026-03-15T08:00:00Z", "long_total": 20.0, "short_total": 40.0}
                    ]
                }
            },
            "footprint": {
                "window_code": "1m",
                "payload": {
                    "by_window": {
                        "15m": {
                            "window_delta": 10.0,
                            "window_total_qty": 100.0,
                            "unfinished_auction": true,
                            "ua_top": 2102.0,
                            "ua_bottom": 2098.0,
                            "stacked_buy": true,
                            "stacked_sell": false,
                            "buy_stacks": [2100.1],
                            "sell_stacks": [2101.2],
                            "buy_imbalance_prices": [2100.11, 2100.14, 2100.26],
                            "sell_imbalance_prices": [2101.21, 2101.24],
                            "levels": [{"price_level": 2100.1}]
                        },
                        "4h": {
                            "window_delta": 20.0,
                            "window_total_qty": 200.0,
                            "unfinished_auction": false,
                            "ua_top": 2110.0,
                            "ua_bottom": 2080.0,
                            "stacked_buy": false,
                            "stacked_sell": true,
                            "buy_stacks": [],
                            "sell_stacks": [],
                            "buy_imbalance_prices": [2100.1, 2100.4, 2100.6],
                            "sell_imbalance_prices": [2101.1],
                            "levels": [{"price_level": 2100.1}]
                        },
                        "1d": {
                            "window_delta": 30.0,
                            "window_total_qty": 300.0,
                            "unfinished_auction": true,
                            "ua_top": 2120.0,
                            "ua_bottom": 2070.0,
                            "stacked_buy": true,
                            "stacked_sell": true,
                            "buy_stacks": [],
                            "sell_stacks": [],
                            "buy_imbalance_prices": [2100.0, 2101.0],
                            "sell_imbalance_prices": [2102.0],
                            "levels": [{"price_level": 2100.1}]
                        }
                    }
                }
            },
            "orderbook_depth": {
                "window_code": "1m",
                "payload": {
                    "obi_fut": 0.027879672288433154,
                    "microprice_fut": 2105.0,
                    "heatmap_summary_fut": {"wall_bias": "ask"},
                    "levels": [
                        {"price_level": 2100.0, "bid_liquidity": 4.0, "ask_liquidity": 0.0, "net_liquidity": 4.0, "level_imbalance": 1.0, "total_liquidity": 4.0},
                        {"price_level": 2102.0, "bid_liquidity": 2.0, "ask_liquidity": 0.0, "net_liquidity": 2.0, "level_imbalance": 1.0, "total_liquidity": 2.0},
                        {"price_level": 2110.0, "bid_liquidity": 0.0, "ask_liquidity": 5.0, "net_liquidity": -5.0, "level_imbalance": -1.0, "total_liquidity": 5.0},
                        {"price_level": 2190.0, "bid_liquidity": 0.0, "ask_liquidity": 8.0, "net_liquidity": -8.0, "level_imbalance": -1.0, "total_liquidity": 8.0},
                        {"price_level": 1500.0, "bid_liquidity": 1000.0, "ask_liquidity": 0.0, "net_liquidity": 1000.0, "level_imbalance": 1.0, "total_liquidity": 1000.0},
                        {"price_level": 2600.0, "bid_liquidity": 0.0, "ask_liquidity": 800.0, "net_liquidity": -800.0, "level_imbalance": -1.0, "total_liquidity": 800.0}
                    ],
                    "by_window": {
                        "15m": {"obi_fut": 0.02, "spread_twa_fut": 0.003},
                        "1h": {"obi_fut": 0.03, "spread_twa_fut": 0.004}
                    }
                }
            },
            "absorption": {
                "window_code": "1m",
                "payload": {
                    "recent_7d": {
                        "event_count": 3,
                        "history_source": "db",
                        "lookback_coverage_ratio": 1.0,
                        "events": sample_scan_events("2026-03-14T06:00:00Z", 15, 3)
                    }
                }
            },
            "initiation": {
                "window_code": "1m",
                "payload": {
                    "recent_7d": {
                        "event_count": 3,
                        "history_source": "db",
                        "lookback_coverage_ratio": 1.0,
                        "events": sample_initiation_events("2026-03-14T05:00:00Z", 15, 3)
                    }
                }
            },
            "bullish_initiation": {
                "window_code": "1m",
                "payload": {
                    "recent_7d": {
                        "event_count": 3,
                        "history_source": "db",
                        "lookback_coverage_ratio": 1.0,
                        "events": sample_initiation_events("2026-03-14T05:30:00Z", 15, 3)
                    }
                }
            },
            "bearish_initiation": {
                "window_code": "1m",
                "payload": {
                    "recent_7d": {
                        "event_count": 3,
                        "history_source": "db",
                        "lookback_coverage_ratio": 1.0,
                        "events": sample_initiation_events("2026-03-14T04:30:00Z", 15, 3)
                    }
                }
            },
            "divergence": {
                "window_code": "1m",
                "payload": {
                    "signal": false,
                    "signals": {"bearish_divergence": false},
                    "latest_7d": sample_divergence_events("2026-03-14T06:30:00Z", 15, 1)
                        .into_iter()
                        .next()
                        .expect("latest divergence event"),
                    "event_count": 0,
                    "divergence_type": null,
                    "likely_driver": "spot_led",
                    "spot_lead_score": 0.9,
                    "pivot_side": "high",
                    "reason": "no_candidate",
                    "recent_7d": {
                        "event_count": 3,
                        "events": sample_divergence_events("2026-03-14T06:00:00Z", 15, 3)
                    },
                    "candidates": [
                        {"type": "bearish", "score": 0.2, "sig_pass": false, "price_start": 2090.0, "price_end": 2100.0, "likely_driver": "fut_led", "fut_divergence_sign": -1},
                        {"type": "bearish", "score": 0.9, "sig_pass": true, "price_start": 2100.0, "price_end": 2110.0, "likely_driver": "spot_led", "fut_divergence_sign": 1}
                    ]
                }
            }
        }));

        let value = ScanFilter::build_value(&input).expect("build scan value");

        assert_eq!(value.as_object().map(|obj| obj.len()), Some(3));
        assert!(value.pointer("/indicator_count").is_none());
        assert!(value.pointer("/management_snapshot").is_none());
        assert!(value.pointer("/indicators/vpin/window_code").is_none());
        assert!(value
            .pointer("/indicators/price_volume_structure/payload/by_window/3d")
            .is_none());
        assert!(value
            .pointer("/indicators/price_volume_structure/payload/value_area_levels")
            .is_none());
        assert!(value
            .pointer("/indicators/price_volume_structure/payload/va_top_levels")
            .is_none());
        assert_eq!(
            value
                .pointer("/indicators/price_volume_structure/payload/by_window/15m/va_top_levels")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(5)
        );
        assert!(value
            .pointer("/indicators/kline_history/payload/intervals/1m")
            .is_none());
        assert!(value
            .pointer("/indicators/kline_history/payload/intervals/15m/markets/spot")
            .is_none());
        assert_eq!(
            value
                .pointer("/indicators/kline_history/payload/intervals/15m/markets/futures/0/t")
                .and_then(Value::as_str),
            Some("2026-03-14T07:45:00Z")
        );
        assert!(value
            .pointer("/indicators/kline_history/payload/intervals/15m/markets/futures/0/open")
            .is_none());
        assert!(value
            .pointer("/indicators/funding_rate/payload/by_window/1h")
            .is_none());
        assert!(value
            .pointer("/indicators/funding_rate/payload/funding_trend_hourly")
            .is_none());
        assert!(value
            .pointer("/indicators/funding_rate/payload/funding_summary/ema_8h")
            .is_some());
        assert!(value
            .pointer("/indicators/liquidation_density/payload/liq_trend_hourly")
            .is_none());
        assert!(value
            .pointer("/indicators/liquidation_density/payload/liq_summary/ratio_24h")
            .is_some());
        assert!(value
            .pointer("/indicators/footprint/payload/by_window/15m/levels")
            .is_none());
        assert_eq!(
            value
                .pointer("/indicators/footprint/payload/by_window/15m/buy_imb_clusters/0/n")
                .and_then(Value::as_u64),
            Some(2)
        );
        assert_eq!(
            value
                .pointer(
                    "/indicators/orderbook_depth/payload/liquidity_walls/bid_walls/0/price_level"
                )
                .and_then(Value::as_f64),
            Some(2100.6666666666665)
        );
        assert_eq!(
            value
                .pointer("/indicators/orderbook_depth/payload/liquidity_walls/depth_imbalance_1pct")
                .and_then(Value::as_f64),
            Some(6.0 / 11.0)
        );
        assert!(value
            .pointer("/indicators/orderbook_depth/payload/liquidity_walls/bid_walls")
            .and_then(Value::as_array)
            .map(|walls| walls.iter().all(|wall| {
                wall.get("distance_pct")
                    .and_then(Value::as_f64)
                    .map(|distance| (-15.0..=0.0).contains(&distance))
                    .unwrap_or(false)
            }))
            .unwrap_or(false));
        assert!(value
            .pointer("/indicators/orderbook_depth/payload/liquidity_walls/ask_walls")
            .and_then(Value::as_array)
            .map(|walls| walls.iter().all(|wall| {
                wall.get("distance_pct")
                    .and_then(Value::as_f64)
                    .map(|distance| (0.0..=15.0).contains(&distance))
                    .unwrap_or(false)
            }))
            .unwrap_or(false));
        assert!(value
            .pointer("/indicators/orderbook_depth/payload/top_liquidity_levels")
            .is_none());
        assert!(value
            .pointer("/indicators/orderbook_depth/payload/by_window/1h")
            .is_none());
        assert!(value
            .pointer("/indicators/fvg/payload/by_window/15m/fvgs/0/fvg_id")
            .is_none());
        assert!(value
            .pointer("/indicators/fvg/payload/by_window/15m/fvgs/0/tf")
            .is_none());
        assert_eq!(
            value.pointer("/indicators/fvg/payload/by_window/15m/fvgs/0/fvg_top"),
            value.pointer("/indicators/fvg/payload/by_window/15m/fvgs/0/upper")
        );
        assert_eq!(
            value.pointer("/indicators/fvg/payload/by_window/15m/fvgs/0/fvg_bottom"),
            value.pointer("/indicators/fvg/payload/by_window/15m/fvgs/0/lower")
        );
        assert_eq!(
            value.pointer("/indicators/fvg/payload/by_window/15m/nearest_bull_fvg/fvg_top"),
            value.pointer("/indicators/fvg/payload/by_window/15m/nearest_bull_fvg/upper")
        );
        assert_eq!(
            value.pointer("/indicators/fvg/payload/by_window/15m/nearest_bull_fvg/fvg_bottom"),
            value.pointer("/indicators/fvg/payload/by_window/15m/nearest_bull_fvg/lower")
        );
        assert!(value
            .pointer("/indicators/absorption/payload/recent_7d/events/0/event_id")
            .is_none());
        assert!(value
            .pointer("/indicators/absorption/payload/recent_7d/events/0/score_base")
            .is_none());
        assert!(value
            .pointer("/indicators/initiation/payload/recent_7d/events/0/follow_through_delta_sum")
            .is_none());
        assert!(value
            .pointer("/indicators/initiation/payload/recent_7d/events/0/follow_through_hold_ok")
            .is_none());
        assert!(value
            .pointer("/indicators/initiation/payload/recent_7d/events/0/follow_through_minutes")
            .is_none());
        assert!(value
            .pointer("/indicators/initiation/payload/recent_7d/events/0/follow_through_max_adverse_excursion_ticks")
            .is_none());
        assert!(value
            .pointer("/indicators/initiation/payload/recent_7d/events/0/spot_cvd_change")
            .is_none());
        assert!(value
            .pointer("/indicators/initiation/payload/recent_7d/events/0/spot_rdelta_1m_mean")
            .is_none());
        assert_eq!(
            value
                .pointer("/indicators/absorption/payload/recent_7d/events/0/event_end_ts")
                .and_then(Value::as_str),
            Some("2026-03-14T06:30:00Z")
        );
        assert!(value
            .pointer("/indicators/divergence/payload/recent_7d/events/0/cvd_norm_diff_fut")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/recent_7d/events/0/event_id")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/recent_7d/events/0/price_norm_diff")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/recent_7d/events/0/sig_test_mode")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/latest_7d/event_id")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/latest_7d/price_norm_diff")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/latest_7d/sig_test_mode")
            .is_none());
        assert_eq!(
            value
                .pointer("/indicators/divergence/payload/candidates/0/price_end")
                .and_then(Value::as_f64),
            Some(2110.0)
        );
    }

    #[test]
    fn build_value_is_mode_agnostic_and_excludes_management_context() {
        let now = Utc::now();
        let input = ModelInvocationInput {
            symbol: "ETHUSDT".to_string(),
            ts_bucket: now,
            window_code: "15m".to_string(),
            indicator_count: 1,
            source_routing_key: "llm_indicator_minute".to_string(),
            source_published_at: None,
            received_at: now,
            indicators: json!({
                "kline_history": {
                    "payload": {
                        "intervals": {
                            "15m": {
                                "markets": {
                                    "futures": {
                                        "bars": [
                                            {"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume_base": 100.0, "open_time": "2026-03-14T07:30:00Z", "is_closed": true}
                                        ]
                                    }
                                }
                            }
                        }
                    }
                }
            }),
            missing_indicator_codes: vec![],
            management_mode: true,
            pending_order_mode: false,
            trading_state: None,
            management_snapshot: None,
        };

        let value = ScanFilter::build_value(&input).expect("build management scan value");

        assert!(value.pointer("/management_snapshot").is_none());
        assert_eq!(value.as_object().map(|obj| obj.len()), Some(3));
        assert_eq!(
            value
                .pointer("/indicators/kline_history/payload/intervals/15m/markets/futures/0/t")
                .and_then(Value::as_str),
            Some("2026-03-14T07:30:00Z")
        );
    }

    #[test]
    fn real_snapshot_scan_output_stays_under_size_budget() {
        let Some(path) = latest_temp_indicator_sample_path() else {
            return;
        };

        let raw = fs::read_to_string(&path).expect("read real temp_indicator sample");
        let root: Value = serde_json::from_str(&raw).expect("parse real temp_indicator sample");
        let indicators = root.get("indicators").cloned().expect("indicators field");
        let input = sample_input(indicators);

        let scan_value = ScanFilter::build_value(&input).expect("build real scan value");
        let serialized = serde_json::to_vec(&scan_value).expect("serialize real scan value");

        assert!(serialized.len() < 260_000);
        assert_eq!(scan_value.as_object().map(|obj| obj.len()), Some(3));
        assert!(scan_value.pointer("/indicator_count").is_none());
        assert!(scan_value
            .pointer("/indicators/price_volume_structure/payload/by_window/3d")
            .is_none());
        assert!(scan_value
            .pointer("/indicators/price_volume_structure/payload/value_area_levels")
            .is_none());
        assert!(scan_value
            .pointer("/indicators/price_volume_structure/payload/by_window/15m/value_area_levels")
            .is_none());
        assert!(scan_value
            .pointer("/indicators/price_volume_structure/payload/by_window/15m/va_top_levels")
            .and_then(Value::as_array)
            .map(|levels| levels.len() <= 5)
            .unwrap_or(false));
        assert!(scan_value
            .pointer("/indicators/orderbook_depth/payload/levels")
            .is_none());
        assert!(scan_value
            .pointer("/indicators/orderbook_depth/payload/top_liquidity_levels")
            .is_none());
        assert!(scan_value
            .pointer("/indicators/orderbook_depth/payload/liquidity_walls/bid_walls")
            .and_then(Value::as_array)
            .map(|levels| levels.len() <= 5)
            .unwrap_or(false));
        assert!(scan_value
            .pointer("/indicators/orderbook_depth/payload/liquidity_walls/bid_walls")
            .and_then(Value::as_array)
            .map(|walls| walls.iter().all(|wall| {
                wall.get("distance_pct")
                    .and_then(Value::as_f64)
                    .map(|distance| (-15.0..=0.0).contains(&distance))
                    .unwrap_or(false)
            }))
            .unwrap_or(false));
        assert!(scan_value
            .pointer("/indicators/orderbook_depth/payload/liquidity_walls/ask_walls")
            .and_then(Value::as_array)
            .map(|walls| walls.iter().all(|wall| {
                wall.get("distance_pct")
                    .and_then(Value::as_f64)
                    .map(|distance| (0.0..=15.0).contains(&distance))
                    .unwrap_or(false)
            }))
            .unwrap_or(false));
        assert!(scan_value
            .pointer("/indicators/footprint/payload/by_window/15m/levels")
            .is_none());
        assert!(scan_value
            .pointer("/indicators/funding_rate/payload/funding_trend_hourly")
            .is_none());
        assert!(scan_value
            .pointer("/indicators/funding_rate/payload/funding_summary/ema_8h")
            .is_some());
        assert!(scan_value
            .pointer("/indicators/liquidation_density/payload/liq_trend_hourly")
            .is_none());
        assert!(scan_value
            .pointer("/indicators/liquidation_density/payload/liq_summary/long_24h")
            .is_some());
        assert!(scan_value
            .pointer("/indicators/divergence/payload/recent_7d/events/0/event_id")
            .is_none());
        assert!(scan_value
            .pointer("/indicators/divergence/payload/recent_7d/events/0/price_norm_diff")
            .is_none());
        assert!(scan_value
            .pointer("/indicators/divergence/payload/recent_7d/events/0/sig_test_mode")
            .is_none());
        assert_newest_first(
            &scan_value,
            "/indicators/kline_history/payload/intervals/15m/markets/futures",
            "t",
        );
        assert_newest_first(
            &scan_value,
            "/indicators/avwap/payload/series_by_window/15m",
            "ts",
        );
        assert_newest_first(
            &scan_value,
            "/indicators/absorption/payload/recent_7d/events",
            "event_end_ts",
        );
        assert_newest_first(
            &scan_value,
            "/indicators/tpo_market_profile/payload/dev_series/15m",
            "ts",
        );
    }
}
