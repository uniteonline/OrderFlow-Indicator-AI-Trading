use crate::llm::{prompt, provider::ModelInvocationInput};
use anyhow::{anyhow, Context, Result};
use chrono::{SecondsFormat, Timelike, Utc};
use serde_json::{json, Map, Value};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

const CORE_WINDOWS: &[&str] = &["15m", "4h", "1d"];
const CORE_WINDOWS_WITH_3D: &[&str] = &["15m", "4h", "1d", "3d"];
const EVENT_INDICATOR_RULES: &[(&str, usize)] = &[
    ("absorption", 10),
    ("buying_exhaustion", 10),
    ("selling_exhaustion", 10),
    ("initiation", 10),
    ("bullish_absorption", 10),
    ("bearish_absorption", 10),
    ("bullish_initiation", usize::MAX),
    ("bearish_initiation", 10),
];
const ORDERBOOK_NEAR_MARK_PCT: f64 = 0.20;

pub(crate) struct CoreFilter;

impl CoreFilter {
    pub(crate) fn build_value(input: &ModelInvocationInput) -> Result<Value> {
        let mut root =
            serde_json::to_value(input).context("serialize core invocation input value")?;
        // Core follows the docs: start from rounded temp_indicator-equivalent data,
        // then keep only the entry / management / pending-order stage-2 inputs.
        super::TempIndicatorInputOptimizer::round_derived_fields(&mut root);
        Ok(build_core_root(&root))
    }

    pub(crate) fn build_finalize_value(
        input: &ModelInvocationInput,
        prior_scan: &Value,
    ) -> Result<Value> {
        let mut value = Self::build_value(input)?;
        if let Some(root) = value.as_object_mut() {
            root.insert(
                "finalize_focus".to_string(),
                json!({
                    "scan_15m_trend": prior_scan.pointer("/timeframe_analysis/15m/trend").and_then(Value::as_str).unwrap_or("unknown"),
                    "scan_4h_trend": prior_scan.pointer("/timeframe_analysis/4h/trend").and_then(Value::as_str).unwrap_or("unknown"),
                    "scan_1d_trend": prior_scan.pointer("/timeframe_analysis/1d/trend").and_then(Value::as_str).unwrap_or("unknown"),
                    "scan_dominant_bias": prior_scan.pointer("/flow_context/dominant_bias").and_then(Value::as_str).unwrap_or("unknown"),
                }),
            );
        }
        Ok(value)
    }

    pub(crate) fn serialize_minified_input(input: &ModelInvocationInput) -> Result<String> {
        super::serialize_prompt_value(
            Self::build_value(input)?,
            "serialize core prompt input json",
        )
    }

    pub(crate) fn serialize_finalize_input(
        input: &ModelInvocationInput,
        prior_scan: &Value,
    ) -> Result<String> {
        super::serialize_prompt_value(
            Self::build_finalize_value(input, prior_scan)?,
            "serialize entry finalize prompt input json",
        )
    }

    pub(crate) fn serialize_prompt_input_minified(
        input: &ModelInvocationInput,
        entry_stage: prompt::EntryPromptStage,
        prior_scan: Option<&Value>,
    ) -> Result<String> {
        if !input.management_mode
            && !input.pending_order_mode
            && matches!(entry_stage, prompt::EntryPromptStage::Finalize)
        {
            let scan =
                prior_scan.ok_or_else(|| anyhow!("missing prior scan for entry finalize"))?;
            return Self::serialize_finalize_input(input, scan);
        }

        Self::serialize_minified_input(input)
    }
}

fn build_core_root(root: &Value) -> Value {
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

    let management_mode = root
        .get("management_mode")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let pending_order_mode = root
        .get("pending_order_mode")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if management_mode || pending_order_mode {
        if let Some(trading_state) = root
            .get("trading_state")
            .filter(|value| !value.is_null())
            .cloned()
        {
            result.insert("trading_state".to_string(), trading_state);
        }
        if let Some(snapshot) = root
            .get("management_snapshot")
            .filter(|value| !value.is_null())
            .cloned()
        {
            result.insert("management_snapshot".to_string(), snapshot);
        }
    }

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
    insert_filtered_indicator(&mut indicators, source, "avwap", filter_avwap);
    insert_filtered_indicator(
        &mut indicators,
        source,
        "rvwap_sigma_bands",
        filter_rvwap_sigma_bands,
    );
    insert_filtered_indicator(&mut indicators, source, "footprint", filter_footprint);
    insert_filtered_indicator(
        &mut indicators,
        source,
        "kline_history",
        filter_kline_history,
    );
    insert_filtered_indicator(
        &mut indicators,
        source,
        "ema_trend_regime",
        filter_ema_trend_regime,
    );
    insert_filtered_indicator(&mut indicators, source, "cvd_pack", filter_cvd_pack);
    insert_filtered_indicator(&mut indicators, source, "funding_rate", filter_funding_rate);
    insert_filtered_indicator(
        &mut indicators,
        source,
        "liquidation_density",
        filter_liquidation_density,
    );

    if let Some(indicator) = source.get("orderbook_depth") {
        let payload = indicator
            .get("payload")
            .map(|value| filter_orderbook_depth(value, resolve_reference_mark_price(source)))
            .unwrap_or(Value::Null);
        indicators.insert(
            "orderbook_depth".to_string(),
            rebuild_indicator(indicator, payload),
        );
    }

    for (code, keep_last) in EVENT_INDICATOR_RULES {
        if let Some(indicator) = source.get(*code) {
            let payload = indicator
                .get("payload")
                .map(|value| filter_event_indicator(value, *keep_last))
                .unwrap_or(Value::Null);
            indicators.insert(code.to_string(), rebuild_indicator(indicator, payload));
        }
    }

    insert_filtered_indicator(&mut indicators, source, "divergence", filter_divergence);

    indicators
}

fn insert_full_indicator(target: &mut Map<String, Value>, source: &Map<String, Value>, code: &str) {
    if let Some(indicator) = source.get(code) {
        target.insert(code.to_string(), indicator.clone());
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
                .filter(|(key, _)| key.as_str() != "payload")
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<Map<String, Value>>()
        })
        .unwrap_or_default();
    rebuilt.insert("payload".to_string(), payload);
    Value::Object(rebuilt)
}

fn filter_tpo_market_profile(payload: &Value) -> Value {
    let mut payload = payload.clone();
    reverse_dev_series_map(payload.get_mut("dev_series"));
    if let Some(by_session) = payload.get_mut("by_session").and_then(Value::as_object_mut) {
        for session in by_session.values_mut() {
            reverse_dev_series_map(session.get_mut("dev_series"));
        }
    }
    payload
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
            "hvn_levels",
            "lvn_levels",
            "levels",
            "value_area_levels",
            "bar_volume",
            "volume_zscore",
            "volume_dryup",
        ],
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
                    "levels",
                    "value_area_levels",
                    "volume_zscore",
                    "volume_dryup",
                    "bar_volume",
                    "window_bars_used",
                ],
            );
            filtered_windows.insert((*window).to_string(), Value::Object(filtered_window));
        }
        result.insert("by_window".to_string(), Value::Object(filtered_windows));
    }

    Value::Object(result)
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
        for window in CORE_WINDOWS_WITH_3D {
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
            reverse_array_field(&mut filtered_window, window_value, "fvgs");
            reverse_array_field(&mut filtered_window, window_value, "active_bull_fvgs");
            reverse_array_field(&mut filtered_window, window_value, "active_bear_fvgs");
            filtered_windows.insert((*window).to_string(), Value::Object(filtered_window));
        }
        result.insert("by_window".to_string(), Value::Object(filtered_windows));
    }

    Value::Object(result)
}

fn reverse_array_field(target: &mut Map<String, Value>, source: &Map<String, Value>, field: &str) {
    if let Some(items) = source.get(field).and_then(Value::as_array) {
        target.insert(
            field.to_string(),
            Value::Array(items.iter().cloned().rev().collect()),
        );
    }
}

fn filter_avwap(payload: &Value) -> Value {
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
        for (window, limit) in [("15m", 5_usize), ("4h", 3), ("1d", 2)] {
            let Some(series) = series_by_window.get(window).and_then(Value::as_array) else {
                continue;
            };
            let filtered = take_last_n(series, limit)
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
            filtered_series.insert(window.to_string(), Value::Array(filtered));
        }
        result.insert(
            "series_by_window".to_string(),
            Value::Object(filtered_series),
        );
    }

    Value::Object(result)
}

fn filter_rvwap_sigma_bands(payload: &Value) -> Value {
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

fn filter_footprint(payload: &Value) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let Some(by_window) = payload.get("by_window").and_then(Value::as_object) else {
        return Value::Object(Map::new());
    };

    let mut result = Map::new();
    let mut filtered_windows = Map::new();

    if let Some(window_value) = by_window.get("15m").and_then(Value::as_object) {
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
                "buy_imbalance_prices",
                "sell_imbalance_prices",
                "buy_stacks",
                "sell_stacks",
                "max_buy_stack_len",
                "max_sell_stack_len",
            ],
        );
        let levels = window_value
            .get("levels")
            .and_then(Value::as_array)
            .map(|levels| filter_footprint_15m_levels(levels))
            .unwrap_or_default();
        filtered_window.insert("levels".to_string(), Value::Array(levels));
        filtered_windows.insert("15m".to_string(), Value::Object(filtered_window));
    }

    for (window, bin_size) in [("4h", 0.5_f64), ("1d", 1.0_f64)] {
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
                "buy_imbalance_prices",
                "sell_imbalance_prices",
                "buy_stacks",
                "sell_stacks",
                "max_buy_stack_len",
                "max_sell_stack_len",
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

fn filter_footprint_15m_levels(levels: &[Value]) -> Vec<Value> {
    let mut top_volume_levels = levels
        .iter()
        .filter_map(Value::as_object)
        .filter_map(|level| {
            Some((
                level.get("price_level")?.as_f64()?.to_bits(),
                level
                    .get("total")
                    .and_then(Value::as_f64)
                    .unwrap_or_default(),
            ))
        })
        .collect::<Vec<_>>();
    top_volume_levels
        .sort_by(|left, right| right.1.partial_cmp(&left.1).unwrap_or(Ordering::Equal));
    let top_volume_prices = top_volume_levels
        .into_iter()
        .take(20)
        .map(|(price_bits, _)| price_bits)
        .collect::<BTreeSet<_>>();

    let mut filtered = levels
        .iter()
        .filter_map(Value::as_object)
        .filter(|level| {
            let flag = |name: &str| level.get(name).and_then(Value::as_bool).unwrap_or(false);
            let price_bits = level
                .get("price_level")
                .and_then(Value::as_f64)
                .map(f64::to_bits);
            flag("buy_imbalance")
                || flag("sell_imbalance")
                || flag("is_open")
                || flag("is_high")
                || flag("is_low")
                || flag("is_close")
                || flag("ua_top_flag")
                || flag("ua_bottom_flag")
                || price_bits
                    .map(|bits| top_volume_prices.contains(&bits))
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
                    "level_rank",
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
        left_price
            .partial_cmp(&right_price)
            .unwrap_or(Ordering::Equal)
    });
    filtered
}

fn filter_orderbook_depth(payload: &Value, mark_price: Option<f64>) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();
    copy_fields(
        &mut result,
        payload,
        &[
            "obi_fut",
            "obi",
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
        .map(|levels| build_top_liquidity_levels(levels, mark_price, 100))
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

fn resolve_reference_mark_price(source: &Map<String, Value>) -> Option<f64> {
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

fn build_top_liquidity_levels(
    levels: &[Value],
    mark_price: Option<f64>,
    limit: usize,
) -> Vec<Value> {
    let lower_bound = mark_price.map(|price| price * (1.0 - ORDERBOOK_NEAR_MARK_PCT));
    let upper_bound = mark_price.map(|price| price * (1.0 + ORDERBOOK_NEAR_MARK_PCT));

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

fn filter_kline_history(payload: &Value) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();
    copy_fields(&mut result, payload, &["as_of_ts"]);

    if let Some(intervals) = payload.get("intervals").and_then(Value::as_object) {
        let mut filtered_intervals = Map::new();
        for (window, limit) in [("15m", 20_usize), ("4h", 15), ("1d", 10)] {
            let Some(interval_value) = intervals.get(window).and_then(Value::as_object) else {
                continue;
            };
            let Some(markets) = interval_value.get("markets").and_then(Value::as_object) else {
                continue;
            };
            let futures_bars = markets
                .get("futures")
                .and_then(Value::as_object)
                .and_then(|market| market.get("bars"))
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let bars = take_last_n(&futures_bars, limit)
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
                window.to_string(),
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

fn filter_ema_trend_regime(payload: &Value) -> Value {
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
            "trend_regime",
            "ema_100_htf",
            "ema_200_htf",
            "trend_regime_by_tf",
        ],
    );
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
        for (window, limit) in [("15m", 15_usize), ("4h", 10), ("1d", 5)] {
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

fn filter_liquidation_density(payload: &Value) -> Value {
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

fn filter_event_indicator(payload: &Value, keep_last: usize) -> Value {
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
    filtered_recent.insert("events".to_string(), Value::Array(events));
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
            "latest_7d",
            "event_count",
            "divergence_type",
            "likely_driver",
            "spot_lead_score",
            "pivot_side",
            "reason",
        ],
    );

    if let Some(recent_7d) = payload.get("recent_7d").and_then(Value::as_object) {
        let mut filtered_recent = Map::new();
        copy_fields(&mut filtered_recent, recent_7d, &["event_count"]);
        let events = recent_7d
            .get("events")
            .and_then(Value::as_array)
            .map(|events| take_last_n(events, 10))
            .unwrap_or_default();
        filtered_recent.insert("events".to_string(), Value::Array(events));
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

fn copy_fields(target: &mut Map<String, Value>, source: &Map<String, Value>, fields: &[&str]) {
    for field in fields {
        if let Some(value) = source.get(*field) {
            target.insert((*field).to_string(), value.clone());
        }
    }
}

fn take_last_n(values: &[Value], n: usize) -> Vec<Value> {
    let mut result = if n == usize::MAX || values.len() <= n {
        values.to_vec()
    } else {
        values[values.len() - n..].to_vec()
    };
    result.reverse();
    result
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

fn hour_bucket_iso(ts: &str) -> Option<String> {
    let dt = chrono::DateTime::parse_from_rfc3339(ts).ok()?;
    let dt = dt.with_minute(0)?.with_second(0)?.with_nanosecond(0)?;
    Some(
        dt.with_timezone(&Utc)
            .to_rfc3339_opts(SecondsFormat::Secs, true),
    )
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

#[cfg(test)]
mod tests {
    use super::CoreFilter;
    use crate::execution::binance::{
        ActivePositionSnapshot, OpenOrderSnapshot, TradingStateSnapshot,
    };
    use crate::llm::provider::{
        ManagementSnapshotForLlm, ModelInvocationInput, PendingOrderSummaryForLlm,
        PositionContextForLlm, PositionSummaryForLlm,
    };
    use chrono::Utc;
    use serde_json::{json, Map, Value};
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

    fn sample_management_snapshot() -> ManagementSnapshotForLlm {
        ManagementSnapshotForLlm {
            context_state: "ACTIVE_POSITION".to_string(),
            has_active_positions: true,
            has_open_orders: true,
            active_position_count: 1,
            open_order_count: 2,
            positions: vec![PositionSummaryForLlm {
                position_side: "LONG".to_string(),
                direction: "LONG".to_string(),
                quantity: 1.0,
                leverage: 10,
                entry_price: 2010.0,
                mark_price: 2025.0,
                unrealized_pnl: 15.0,
                pnl_by_latest_price: 15.0,
                current_tp_price: Some(2055.0),
                current_sl_price: Some(1998.0),
            }],
            pending_order: Some(PendingOrderSummaryForLlm {
                position_side: "LONG".to_string(),
                direction: "LONG".to_string(),
                quantity: 1.0,
                leverage: Some(10),
                entry_price: Some(2010.0),
                current_tp_price: Some(2055.0),
                current_sl_price: Some(1998.0),
            }),
            last_management_reason: Some("keep structure".to_string()),
            position_context: Some(PositionContextForLlm {
                original_qty: 1.0,
                current_qty: 1.0,
                current_pct_of_original: 100.0,
                effective_leverage: Some(10),
                effective_entry_price: Some(2010.0),
                effective_take_profit: Some(2055.0),
                effective_stop_loss: Some(1998.0),
                reduction_history: vec![],
                times_reduced_at_current_level: 0,
                last_management_action: Some("HOLD".to_string()),
                last_management_reason: Some("keep structure".to_string()),
                entry_context: None,
            }),
        }
    }

    fn sample_trading_state() -> TradingStateSnapshot {
        TradingStateSnapshot {
            symbol: "ETHUSDT".to_string(),
            has_active_context: true,
            has_active_positions: true,
            has_open_orders: true,
            active_positions: vec![ActivePositionSnapshot {
                position_side: "LONG".to_string(),
                position_amt: 1.0,
                entry_price: 2010.0,
                mark_price: 2025.0,
                unrealized_pnl: 15.0,
                leverage: 10,
            }],
            open_orders: vec![OpenOrderSnapshot {
                order_id: 1,
                side: "BUY".to_string(),
                position_side: "LONG".to_string(),
                order_type: "LIMIT".to_string(),
                status: "NEW".to_string(),
                orig_qty: 1.0,
                executed_qty: 0.0,
                price: 2005.0,
                stop_price: 0.0,
                close_position: false,
                reduce_only: false,
            }],
            total_wallet_balance: 1000.0,
            available_balance: 900.0,
        }
    }

    fn assert_newest_first(value: &Value, pointer: &str, ts_field: &str) {
        let series = value
            .pointer(pointer)
            .and_then(Value::as_array)
            .unwrap_or_else(|| panic!("missing array at {pointer}"));
        let first_ts = series
            .first()
            .and_then(|entry| entry.get(ts_field))
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("missing first {ts_field} at {pointer}"));
        let last_ts = series
            .last()
            .and_then(|entry| entry.get(ts_field))
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("missing last {ts_field} at {pointer}"));
        assert!(
            first_ts >= last_ts,
            "expected newest-first ordering at {pointer}, got first={first_ts}, last={last_ts}"
        );
    }

    fn insert_indicator(indicators: &mut Map<String, Value>, code: &str, value: Value) {
        indicators.insert(code.to_string(), value);
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

    #[test]
    fn build_value_applies_doc_rules_for_core_input() {
        let mut indicators = Map::new();
        insert_indicator(
            &mut indicators,
            "price_volume_structure",
            json!({
                "window_code": "1m",
                "payload": {
                    "poc_price": 2100.1234,
                    "poc_volume": 12.0,
                    "vah": 2110.0,
                    "val": 2090.0,
                    "bar_volume": 100.0,
                    "hvn_levels": [2101.0],
                    "lvn_levels": [2098.0],
                    "value_area_levels": [2090.0, 2110.0],
                    "levels": [{"price": 2100.0}],
                    "volume_zscore": 1.2,
                    "volume_dryup": false,
                    "by_window": {
                        "15m": {"window_bars_used": 10, "levels": [{"price": 2100.0}], "poc_price": 2100.0, "poc_volume": 12.0, "vah": 2110.0, "val": 2090.0, "hvn_levels": [2101.0], "lvn_levels": [2098.0], "value_area_levels": [2090.0, 2110.0], "volume_zscore": 1.2, "volume_dryup": false, "bar_volume": 100.0},
                        "4h": {"window_bars_used": 20, "levels": [{"price": 2100.0}], "poc_price": 2100.0, "poc_volume": 12.0, "vah": 2110.0, "val": 2090.0, "hvn_levels": [2101.0], "lvn_levels": [2098.0], "value_area_levels": [2090.0, 2110.0], "volume_zscore": 1.2, "volume_dryup": false, "bar_volume": 100.0},
                        "1d": {"window_bars_used": 14, "levels": [{"price": 2100.0}], "poc_price": 2100.0, "poc_volume": 12.0, "vah": 2110.0, "val": 2090.0, "hvn_levels": [2101.0], "lvn_levels": [2098.0], "value_area_levels": [2090.0, 2110.0], "volume_zscore": 1.2, "volume_dryup": false, "bar_volume": 100.0},
                        "3d": {"window_bars_used": 30, "levels": [{"price": 2100.0}], "poc_price": 2100.0, "poc_volume": 12.0, "vah": 2110.0, "val": 2090.0, "hvn_levels": [2101.0], "lvn_levels": [2098.0], "value_area_levels": [2090.0, 2110.0], "volume_zscore": 1.2, "volume_dryup": false, "bar_volume": 100.0}
                    }
                }
            }),
        );
        insert_indicator(
            &mut indicators,
            "fvg",
            json!({
                "window_code": "1m",
                "payload": {
                    "base_detection_uses_spot": true,
                    "source_market": "futures",
                    "by_window": {
                        "15m": {
                            "fvgs": [
                                {"birth_ts": "2026-03-14T07:00:00Z", "upper": 1.0},
                                {"birth_ts": "2026-03-14T07:15:00Z", "upper": 2.0}
                            ],
                            "active_bull_fvgs": [{"birth_ts": "2026-03-14T07:00:00Z"}],
                            "active_bear_fvgs": [{"birth_ts": "2026-03-14T07:15:00Z"}],
                            "nearest_bull_fvg": {"upper": 1.0},
                            "nearest_bear_fvg": {"upper": 2.0},
                            "is_ready": true,
                            "coverage_ratio": 1.0,
                            "min_required_bars": 3
                        },
                        "4h": {"fvgs": [], "active_bull_fvgs": [], "active_bear_fvgs": [], "nearest_bull_fvg": null, "nearest_bear_fvg": null, "is_ready": true, "coverage_ratio": 1.0},
                        "1d": {"fvgs": [], "active_bull_fvgs": [], "active_bear_fvgs": [], "nearest_bull_fvg": null, "nearest_bear_fvg": null, "is_ready": true, "coverage_ratio": 1.0},
                        "3d": {"fvgs": [], "active_bull_fvgs": [], "active_bear_fvgs": [], "nearest_bull_fvg": null, "nearest_bear_fvg": null, "is_ready": true, "coverage_ratio": 1.0}
                    }
                }
            }),
        );
        insert_indicator(
            &mut indicators,
            "avwap",
            json!({
                "payload": {
                    "anchor_ts": "2026-03-14T08:00:00Z",
                    "avwap_fut": 2000.0,
                    "avwap_spot": 2001.0,
                    "fut_last_price": 2002.0,
                    "fut_mark_price": 2002.0,
                    "lookback": "7d",
                    "price_minus_avwap_fut": 2.0,
                    "price_minus_spot_avwap_fut": 1.0,
                    "price_minus_spot_avwap_futmark": 1.5,
                    "xmk_avwap_gap_f_minus_s": -1.0,
                    "zavwap_gap": -0.2,
                    "series_by_window": {
                        "15m": [
                            {"ts": "2026-03-14T07:00:00Z", "avwap_fut": 1.0, "avwap_spot": 1.1, "xmk_avwap_gap_f_minus_s": -0.1},
                            {"ts": "2026-03-14T07:15:00Z", "avwap_fut": 1.1, "avwap_spot": 1.2, "xmk_avwap_gap_f_minus_s": -0.1},
                            {"ts": "2026-03-14T07:30:00Z", "avwap_fut": 1.2, "avwap_spot": 1.3, "xmk_avwap_gap_f_minus_s": -0.1},
                            {"ts": "2026-03-14T07:45:00Z", "avwap_fut": 1.3, "avwap_spot": 1.4, "xmk_avwap_gap_f_minus_s": -0.1},
                            {"ts": "2026-03-14T08:00:00Z", "avwap_fut": 1.4, "avwap_spot": 1.5, "xmk_avwap_gap_f_minus_s": -0.1},
                            {"ts": "2026-03-14T08:15:00Z", "avwap_fut": 1.5, "avwap_spot": 1.6, "xmk_avwap_gap_f_minus_s": -0.1}
                        ],
                        "4h": [
                            {"ts": "2026-03-13T16:00:00Z", "avwap_fut": 1.0},
                            {"ts": "2026-03-13T20:00:00Z", "avwap_fut": 1.1},
                            {"ts": "2026-03-14T00:00:00Z", "avwap_fut": 1.2},
                            {"ts": "2026-03-14T04:00:00Z", "avwap_fut": 1.3}
                        ],
                        "1d": [
                            {"ts": "2026-03-13T00:00:00Z", "avwap_fut": 1.0},
                            {"ts": "2026-03-14T00:00:00Z", "avwap_fut": 1.1},
                            {"ts": "2026-03-15T00:00:00Z", "avwap_fut": 1.2}
                        ]
                    }
                }
            }),
        );
        insert_indicator(
            &mut indicators,
            "rvwap_sigma_bands",
            json!({
                "payload": {
                    "as_of_ts": "2026-03-14T08:00:00Z",
                    "source_mode": "futures",
                    "by_window": {
                        "15m": {"rvwap_w": 2000.0, "rvwap_sigma_w": 4.0, "rvwap_band_minus_1": 1996.0, "rvwap_band_minus_2": 1992.0, "rvwap_band_plus_1": 2004.0, "rvwap_band_plus_2": 2008.0, "z_price_minus_rvwap": 0.5, "samples_used": 10, "window_minutes": 15},
                        "4h": {"rvwap_w": 2001.0, "rvwap_sigma_w": 5.0, "rvwap_band_minus_1": 1996.0, "rvwap_band_minus_2": 1991.0, "rvwap_band_plus_1": 2006.0, "rvwap_band_plus_2": 2011.0, "z_price_minus_rvwap": 0.2, "samples_used": 10},
                        "1d": {"rvwap_w": 2002.0, "rvwap_sigma_w": 6.0, "rvwap_band_minus_1": 1996.0, "rvwap_band_minus_2": 1990.0, "rvwap_band_plus_1": 2008.0, "rvwap_band_plus_2": 2014.0, "z_price_minus_rvwap": 0.1, "samples_used": 10}
                    },
                    "series_by_output_window": {"15m": [{"ts": "2026-03-14T07:00:00Z"}]}
                }
            }),
        );
        insert_indicator(
            &mut indicators,
            "footprint",
            json!({
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
                            "buy_imbalance_prices": [2100.11, 2100.14, 2100.26],
                            "sell_imbalance_prices": [2101.21, 2101.24],
                            "buy_stacks": [2100.1],
                            "sell_stacks": [2101.2],
                            "max_buy_stack_len": 2,
                            "max_sell_stack_len": 1,
                            "levels": [
                                {"price_level": 2099.9, "buy": 1.0, "sell": 2.0, "delta": -1.0, "total": 3.0, "buy_imbalance": false, "sell_imbalance": false, "is_open": false, "is_high": false, "is_low": false, "is_close": false, "ua_top_flag": false, "ua_bottom_flag": false, "level_rank": 1},
                                {"price_level": 2100.0, "buy": 2.0, "sell": 1.0, "delta": 1.0, "total": 3.0, "buy_imbalance": true, "sell_imbalance": false, "is_open": false, "is_high": false, "is_low": false, "is_close": false, "ua_top_flag": false, "ua_bottom_flag": false, "level_rank": 2},
                                {"price_level": 2100.1, "buy": 2.0, "sell": 1.0, "delta": 1.0, "total": 200.0, "buy_imbalance": false, "sell_imbalance": false, "is_open": false, "is_high": false, "is_low": false, "is_close": false, "ua_top_flag": false, "ua_bottom_flag": false, "level_rank": 3},
                                {"price_level": 2100.2, "buy": 1.0, "sell": 2.0, "delta": -1.0, "total": 3.0, "buy_imbalance": false, "sell_imbalance": false, "is_open": false, "is_high": true, "is_low": false, "is_close": false, "ua_top_flag": false, "ua_bottom_flag": false, "level_rank": 4}
                            ]
                        },
                        "4h": {
                            "window_delta": 20.0,
                            "window_total_qty": 200.0,
                            "unfinished_auction": false,
                            "ua_top": 2110.0,
                            "ua_bottom": 2080.0,
                            "stacked_buy": false,
                            "stacked_sell": true,
                            "buy_imbalance_prices": [2100.1, 2100.4, 2100.6],
                            "sell_imbalance_prices": [2101.1],
                            "buy_stacks": [],
                            "sell_stacks": [],
                            "max_buy_stack_len": 0,
                            "max_sell_stack_len": 0
                        },
                        "1d": {
                            "window_delta": 30.0,
                            "window_total_qty": 300.0,
                            "unfinished_auction": true,
                            "ua_top": 2120.0,
                            "ua_bottom": 2070.0,
                            "stacked_buy": true,
                            "stacked_sell": true,
                            "buy_imbalance_prices": [2100.0, 2101.0],
                            "sell_imbalance_prices": [2102.0],
                            "buy_stacks": [],
                            "sell_stacks": [],
                            "max_buy_stack_len": 0,
                            "max_sell_stack_len": 0
                        },
                        "1h": {"window_delta": 1.0}
                    }
                }
            }),
        );
        insert_indicator(
            &mut indicators,
            "orderbook_depth",
            json!({
                "payload": {
                    "obi_fut": 0.02,
                    "obi_k_dw_twa_fut": 0.12,
                    "microprice_fut": 2100.0,
                    "bbo_updates_fut": 42,
                    "relative_delta_spot": -0.33,
                    "levels": [
                        {"price_level": 1500.0, "bid_liquidity": 10.0, "ask_liquidity": 2.0, "total_liquidity": 12.0, "net_liquidity": 8.0, "level_imbalance": 0.66},
                        {"price_level": 2050.0, "bid_liquidity": 10.0, "ask_liquidity": 2.0, "total_liquidity": 12.0, "net_liquidity": 8.0, "level_imbalance": 0.66},
                        {"price_level": 2100.0, "bid_liquidity": 15.0, "ask_liquidity": 1.0, "total_liquidity": 16.0, "net_liquidity": 14.0, "level_imbalance": 0.88}
                    ],
                    "by_window": {
                        "15m": {"obi_fut": 0.02, "spread_twa_fut": 0.003},
                        "1h": {"obi_fut": 0.03, "spread_twa_fut": 0.004}
                    }
                }
            }),
        );
        insert_indicator(
            &mut indicators,
            "kline_history",
            json!({
                "payload": {
                    "as_of_ts": "2026-03-14T08:00:00Z",
                    "intervals": {
                        "15m": {
                            "markets": {
                                "futures": {
                                    "bars": [
                                        {"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume_base": 100.0, "open_time": "2026-03-14T06:00:00Z", "is_closed": true},
                                        {"open": 1.1, "high": 2.1, "low": 0.6, "close": 1.6, "volume_base": 101.0, "open_time": "2026-03-14T06:15:00Z", "is_closed": true}
                                    ]
                                }
                            }
                        },
                        "4h": {"markets": {"futures": {"bars": [{"open": 3.0, "high": 4.0, "low": 2.0, "close": 3.5, "volume_base": 200.0, "open_time": "2026-03-13T20:00:00Z", "is_closed": true}]}}},
                        "1d": {"markets": {"futures": {"bars": [{"open": 5.0, "high": 6.0, "low": 4.0, "close": 5.5, "volume_base": 300.0, "open_time": "2026-03-13T00:00:00Z", "is_closed": true}]}}},
                        "1m": {"markets": {"futures": {"bars": [{"open": 7.0}]}}}
                    }
                }
            }),
        );
        insert_indicator(
            &mut indicators,
            "ema_trend_regime",
            json!({
                "payload": {
                    "as_of_ts": "2026-03-14T08:00:00Z",
                    "output_sampling": "15m",
                    "ema_13": 1.0,
                    "ema_21": 2.0,
                    "ema_34": 3.0,
                    "ema_band_high": 4.0,
                    "ema_band_low": 5.0,
                    "trend_regime": "bull",
                    "ema_100_htf": {"4h": 6.0},
                    "ema_200_htf": {"4h": 7.0},
                    "trend_regime_by_tf": {"4h": "bull", "1d": "bull"},
                    "ffill_series_by_output_window": {"15m": [{"ts": "2026-03-14T07:00:00Z"}]}
                }
            }),
        );
        insert_indicator(
            &mut indicators,
            "cvd_pack",
            json!({
                "payload": {
                    "delta_fut": 1.0,
                    "delta_spot": 2.0,
                    "relative_delta_fut": 3.0,
                    "relative_delta_spot": 4.0,
                    "likely_driver": "spot_led",
                    "spot_flow_dominance": 0.7,
                    "spot_lead_score": 0.8,
                    "xmk_delta_gap_s_minus_f": 0.9,
                    "cvd_slope_fut": 1.1,
                    "cvd_slope_spot": 1.2,
                    "by_window": {
                        "15m": {
                            "series": [
                                {"ts": "2026-03-14T07:00:00Z", "close_fut": 1.0, "close_spot": 1.1, "delta_fut": 1.2, "delta_spot": 1.3, "relative_delta_fut": 1.4, "relative_delta_spot": 1.5, "cvd_7d_fut": 1.6, "cvd_7d_spot": 1.7, "spot_flow_dominance": 1.8, "xmk_delta_gap_s_minus_f": 1.9},
                                {"ts": "2026-03-14T07:15:00Z", "close_fut": 2.0, "close_spot": 2.1, "delta_fut": 2.2, "delta_spot": 2.3, "relative_delta_fut": 2.4, "relative_delta_spot": 2.5, "cvd_7d_fut": 2.6, "cvd_7d_spot": 2.7, "spot_flow_dominance": 2.8, "xmk_delta_gap_s_minus_f": 2.9}
                            ]
                        },
                        "4h": {"series": [{"ts": "2026-03-13T20:00:00Z"}]},
                        "1d": {"series": [{"ts": "2026-03-13T00:00:00Z"}]}
                    }
                }
            }),
        );
        insert_indicator(
            &mut indicators,
            "funding_rate",
            json!({
                "payload": {
                    "funding_current": -0.00004527,
                    "funding_twa": -0.00003111,
                    "mark_price_last": 2001.5678,
                    "mark_price_last_ts": "2026-03-14T08:00:00Z",
                    "mark_price_twap": 2000.1111,
                    "funding_current_effective_ts": "2026-03-14T08:00:00Z",
                    "by_window": {
                        "15m": {"funding_twa": -0.0000123, "change_count": 2, "changes": [
                            {"change_ts": "2026-03-14T07:00:00Z", "funding_delta": -0.0000002, "funding_new": -0.0000123, "funding_prev": -0.0000121},
                            {"change_ts": "2026-03-14T07:15:00Z", "funding_delta": -0.0000003, "funding_new": -0.0000126, "funding_prev": -0.0000123}
                        ]},
                        "4h": {"funding_twa": -0.0000222, "change_count": 1, "changes": [{"change_ts": "2026-03-14T06:00:00Z", "funding_new": -0.1}]},
                        "1d": {"funding_twa": -0.0000333, "change_count": 1, "changes": [{"change_ts": "2026-03-13T06:00:00Z", "funding_new": -0.2}]},
                        "1h": {"funding_twa": -0.1}
                    },
                    "recent_7d": [
                        {"change_ts": "2026-03-14T07:01:00Z", "funding_new": -0.00001},
                        {"change_ts": "2026-03-14T07:22:00Z", "funding_new": -0.00003},
                        {"change_ts": "2026-03-14T08:05:00Z", "funding_new": 0.00002}
                    ]
                }
            }),
        );
        insert_indicator(
            &mut indicators,
            "liquidation_density",
            json!({
                "payload": {
                    "by_window": {
                        "15m": {"long_total": 1.0, "short_total": 2.0, "peak_levels": [{"price": 2100.0}], "coverage_ratio": 1.0, "is_ready": true, "levels_count": 1},
                        "4h": {"long_total": 3.0, "short_total": 4.0, "peak_levels": [], "coverage_ratio": 1.0, "is_ready": true, "levels_count": 0},
                        "1d": {"long_total": 5.0, "short_total": 6.0, "peak_levels": [], "coverage_ratio": 1.0, "is_ready": true, "levels_count": 0},
                        "1h": {"long_total": 7.0, "short_total": 8.0}
                    },
                    "recent_7d": [
                        {"ts_snapshot": "2026-03-14T07:01:00Z", "long_total": 0.0, "short_total": 0.0},
                        {"ts_snapshot": "2026-03-14T08:05:00Z", "long_total": 10.0, "short_total": 2.0}
                    ]
                }
            }),
        );
        insert_indicator(
            &mut indicators,
            "absorption",
            json!({
                "payload": {
                    "recent_7d": {
                        "event_count": 3,
                        "lookback_coverage_ratio": 1.0,
                        "events": [
                            {"event_end_ts": "2026-03-14T06:00:00Z", "score": 0.1},
                            {"event_end_ts": "2026-03-14T06:15:00Z", "score": 0.2},
                            {"event_end_ts": "2026-03-14T06:30:00Z", "score": 0.3}
                        ]
                    }
                }
            }),
        );
        insert_indicator(
            &mut indicators,
            "initiation",
            json!({
                "payload": {
                    "recent_7d": {
                        "event_count": 2,
                        "lookback_coverage_ratio": 1.0,
                        "events": [
                            {"event_end_ts": "2026-03-14T06:00:00Z", "score": 0.1},
                            {"event_end_ts": "2026-03-14T06:15:00Z", "score": 0.2}
                        ]
                    }
                }
            }),
        );
        insert_indicator(
            &mut indicators,
            "bullish_initiation",
            json!({
                "payload": {
                    "recent_7d": {
                        "event_count": 2,
                        "lookback_coverage_ratio": 1.0,
                        "events": [
                            {"event_end_ts": "2026-03-14T06:00:00Z", "score": 0.1},
                            {"event_end_ts": "2026-03-14T06:15:00Z", "score": 0.2}
                        ]
                    }
                }
            }),
        );
        insert_indicator(
            &mut indicators,
            "buying_exhaustion",
            json!({"payload": {"recent_7d": {"event_count": 0, "lookback_coverage_ratio": 1.0, "events": []}}}),
        );
        insert_indicator(
            &mut indicators,
            "selling_exhaustion",
            json!({"payload": {"recent_7d": {"event_count": 0, "lookback_coverage_ratio": 1.0, "events": []}}}),
        );
        insert_indicator(
            &mut indicators,
            "bullish_absorption",
            json!({"payload": {"recent_7d": {"event_count": 0, "lookback_coverage_ratio": 1.0, "events": []}}}),
        );
        insert_indicator(
            &mut indicators,
            "bearish_absorption",
            json!({"payload": {"recent_7d": {"event_count": 0, "lookback_coverage_ratio": 1.0, "events": []}}}),
        );
        insert_indicator(
            &mut indicators,
            "bearish_initiation",
            json!({"payload": {"recent_7d": {"event_count": 0, "lookback_coverage_ratio": 1.0, "events": []}}}),
        );
        insert_indicator(
            &mut indicators,
            "divergence",
            json!({
                "payload": {
                    "signal": false,
                    "signals": {"bearish_divergence": false},
                    "latest_7d": {"event_end_ts": "2026-03-14T06:30:00Z", "score": 0.4},
                    "event_count": 0,
                    "divergence_type": null,
                    "likely_driver": "spot_led",
                    "spot_lead_score": 0.9,
                    "pivot_side": "high",
                    "reason": "no_candidate",
                    "recent_7d": {
                        "event_count": 3,
                        "events": [
                            {"event_end_ts": "2026-03-14T06:00:00Z", "score": 0.1},
                            {"event_end_ts": "2026-03-14T06:15:00Z", "score": 0.2},
                            {"event_end_ts": "2026-03-14T06:30:00Z", "score": 0.3}
                        ]
                    },
                    "candidates": [
                        {"type": "bearish", "score": 0.2, "sig_pass": false, "price_start": 2090.0, "price_end": 2100.0, "likely_driver": "fut_led", "fut_divergence_sign": -1},
                        {"type": "bearish", "score": 0.9, "sig_pass": true, "price_start": 2100.0, "price_end": 2110.0, "likely_driver": "spot_led", "fut_divergence_sign": 1}
                    ]
                }
            }),
        );
        insert_indicator(
            &mut indicators,
            "tpo_market_profile",
            json!({
                "payload": {
                    "tpo_poc": 2100.0,
                    "tpo_vah": 2110.0,
                    "tpo_val": 2090.0,
                    "initial_balance_high": 2112.0,
                    "initial_balance_low": 2088.0,
                    "by_session": {
                        "1d": {
                            "tpo_poc": 2100.0,
                            "tpo_vah": 2110.0,
                            "tpo_val": 2090.0,
                            "initial_balance_high": 2112.0,
                            "initial_balance_low": 2088.0,
                            "dev_series": {
                                "15m": [
                                    {"ts": "2026-03-14T07:45:00Z", "tpo_dev_poc": 1.0},
                                    {"ts": "2026-03-14T08:00:00Z", "tpo_dev_poc": 2.0}
                                ]
                            }
                        }
                    },
                    "dev_series": {
                        "15m": [
                            {"ts": "2026-03-14T07:45:00Z", "tpo_dev_poc": 1.0},
                            {"ts": "2026-03-14T08:00:00Z", "tpo_dev_poc": 2.0}
                        ]
                    }
                }
            }),
        );
        let input = sample_input(Value::Object(indicators));

        let value = CoreFilter::build_value(&input).expect("build core value");

        assert_eq!(value.as_object().map(|obj| obj.len()), Some(3));
        assert!(value
            .pointer("/indicators/price_volume_structure/payload/by_window/3d")
            .is_some());
        assert!(value
            .pointer("/indicators/fvg/payload/by_window/3d")
            .is_some());
        assert!(value
            .pointer("/indicators/fvg/payload/by_window/15m/min_required_bars")
            .is_none());
        assert_eq!(
            value
                .pointer("/indicators/fvg/payload/by_window/15m/fvgs/0/birth_ts")
                .and_then(Value::as_str),
            Some("2026-03-14T07:15:00Z")
        );
        assert_eq!(
            value
                .pointer("/indicators/avwap/payload/series_by_window/15m")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(5)
        );
        assert!(value
            .pointer("/indicators/rvwap_sigma_bands/payload/series_by_output_window")
            .is_none());
        assert!(value
            .pointer("/indicators/ema_trend_regime/payload/ffill_series_by_output_window")
            .is_none());
        assert!(value
            .pointer("/indicators/footprint/payload/by_window/15m/levels")
            .and_then(Value::as_array)
            .map(|levels| !levels.is_empty())
            .unwrap_or(false));
        assert!(value
            .pointer("/indicators/footprint/payload/by_window/4h/buy_imb_clusters")
            .is_some());
        assert!(value
            .pointer("/indicators/orderbook_depth/payload/by_window/1h")
            .is_some());
        assert_eq!(
            value.pointer("/indicators/orderbook_depth/payload/obi_k_dw_twa_fut"),
            Some(&json!(0.12))
        );
        assert!(value
            .pointer("/indicators/orderbook_depth/payload/bbo_updates_fut")
            .is_none());
        assert!(value
            .pointer("/indicators/orderbook_depth/payload/relative_delta_spot")
            .is_none());
        assert_eq!(
            value
                .pointer("/indicators/orderbook_depth/payload/top_liquidity_levels")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(2)
        );
        assert!(value
            .pointer("/indicators/orderbook_depth/payload/top_liquidity_levels/0/total_liquidity")
            .is_some());
        assert!(value
            .pointer("/indicators/kline_history/payload/intervals/15m/markets/futures/0/open_time")
            .is_some());
        assert!(value
            .pointer("/indicators/kline_history/payload/intervals/15m/markets/futures/0/t")
            .is_none());
        assert!(value
            .pointer("/indicators/funding_rate/payload/by_window/15m/changes/0/funding_prev")
            .is_some());
        assert!(value
            .pointer("/indicators/funding_rate/payload/funding_trend_hourly/0/avg_funding")
            .is_some());
        assert!(value
            .pointer("/indicators/liquidation_density/payload/liq_trend_hourly/0/long_total")
            .is_some());
        assert_eq!(
            value
                .pointer("/indicators/divergence/payload/recent_7d/events")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(3)
        );
        assert!(value
            .pointer("/indicators/divergence/payload/candidates/0/fut_divergence_sign")
            .is_none());
        assert_eq!(
            value
                .pointer("/indicators/divergence/payload/candidates/0/price_end")
                .and_then(Value::as_f64),
            Some(2110.0)
        );
        assert_eq!(
            value
                .pointer("/indicators/bullish_initiation/payload/recent_7d/events")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(2)
        );
        assert_eq!(
            value
                .pointer("/indicators/initiation/payload/recent_7d/events")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(2)
        );
        assert_newest_first(
            &value,
            "/indicators/avwap/payload/series_by_window/15m",
            "ts",
        );
        assert_newest_first(
            &value,
            "/indicators/cvd_pack/payload/by_window/15m/series",
            "ts",
        );
        assert_newest_first(
            &value,
            "/indicators/funding_rate/payload/by_window/15m/changes",
            "change_ts",
        );
        assert_newest_first(
            &value,
            "/indicators/absorption/payload/recent_7d/events",
            "event_end_ts",
        );
        assert_newest_first(
            &value,
            "/indicators/tpo_market_profile/payload/dev_series/15m",
            "ts",
        );
    }

    #[test]
    fn build_value_management_mode_keeps_live_context_and_filtered_indicators() {
        let now = Utc::now();
        let input = ModelInvocationInput {
            symbol: "ETHUSDT".to_string(),
            ts_bucket: now,
            window_code: "15m".to_string(),
            indicator_count: 2,
            source_routing_key: "llm_indicator_minute".to_string(),
            source_published_at: None,
            received_at: now,
            indicators: json!({
                "avwap": {
                    "payload": {
                        "fut_mark_price": 2025.0,
                        "fut_last_price": 2024.5,
                        "series_by_window": {"15m": [{"ts": "2026-03-14T07:00:00Z", "avwap_fut": 2024.0}]}
                    }
                },
                "price_volume_structure": {
                    "payload": {
                        "val": 2000.0,
                        "vah": 2040.0,
                        "poc_price": 2018.0,
                        "levels": [{"price": 2018.0}],
                        "by_window": {"3d": {"levels": [{"price": 2018.0}], "val": 2000.0, "vah": 2040.0, "poc_price": 2018.0}}
                    }
                }
            }),
            missing_indicator_codes: vec![],
            management_mode: true,
            pending_order_mode: false,
            trading_state: Some(sample_trading_state()),
            management_snapshot: Some(sample_management_snapshot()),
        };

        let value = CoreFilter::build_value(&input).expect("build management core value");

        assert!(value.pointer("/management_snapshot").is_some());
        assert!(value.pointer("/trading_state").is_some());
        assert!(value
            .pointer("/indicators/pre_computed_management_state")
            .is_none());
        assert!(value
            .pointer("/management_snapshot/position_context/entry_context/entry_v")
            .is_none());
        assert!(value
            .pointer("/indicators/price_volume_structure/payload/by_window/3d")
            .is_some());
    }

    #[test]
    fn real_snapshot_core_output_stays_under_size_budget() {
        let Some(path) = latest_temp_indicator_sample_path() else {
            return;
        };

        let raw = fs::read_to_string(&path).expect("read real temp_indicator sample");
        let root: Value = serde_json::from_str(&raw).expect("parse real temp_indicator sample");
        let indicators = root.get("indicators").cloned().expect("indicators field");
        let input = sample_input(indicators);

        let core_value = CoreFilter::build_value(&input).expect("build real core value");
        let serialized = serde_json::to_vec(&core_value).expect("serialize real core value");

        assert!(serialized.len() < 500_000);
        assert_eq!(core_value.as_object().map(|obj| obj.len()), Some(3));
        assert!(core_value
            .pointer("/indicators/price_volume_structure/payload/by_window/3d")
            .is_some());
        assert!(core_value
            .pointer("/indicators/orderbook_depth/payload/by_window/1h")
            .is_some());
        assert!(core_value
            .pointer("/indicators/orderbook_depth/payload/top_liquidity_levels")
            .and_then(Value::as_array)
            .map(|levels| levels.len() <= 100)
            .unwrap_or(false));
        assert!(core_value
            .pointer("/indicators/orderbook_depth/payload/bbo_updates_fut")
            .is_none());
        assert!(core_value
            .pointer("/indicators/orderbook_depth/payload/relative_delta_spot")
            .is_none());
        assert!(core_value
            .pointer("/indicators/rvwap_sigma_bands/payload/series_by_output_window")
            .is_none());
        assert!(core_value
            .pointer("/indicators/ema_trend_regime/payload/ffill_series_by_output_window")
            .is_none());
        assert_newest_first(
            &core_value,
            "/indicators/kline_history/payload/intervals/15m/markets/futures",
            "open_time",
        );
        assert_newest_first(
            &core_value,
            "/indicators/absorption/payload/recent_7d/events",
            "event_end_ts",
        );
        assert_newest_first(
            &core_value,
            "/indicators/tpo_market_profile/payload/dev_series/15m",
            "ts",
        );
    }
}
