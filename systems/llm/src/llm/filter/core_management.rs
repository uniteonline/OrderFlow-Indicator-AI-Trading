use super::core_shared::{
    self, build_events_summary_v4, filter_avwap, filter_cvd_pack_entry_v3, filter_divergence,
    filter_ema_trend_regime, filter_event_indicator_entry_v3, filter_footprint,
    filter_funding_rate_entry_v3, filter_fvg, filter_kline_history,
    filter_liquidation_density_entry_v3, filter_orderbook_depth_entry_v3,
    filter_price_volume_structure_entry_v3, filter_rvwap_sigma_bands, filter_stack_field_v4,
    filter_tpo_market_profile, parse_rfc3339_utc, prune_nulls, recompute_max_stack_len_v4,
    resolve_reference_mark_price, sanitize_fvg_array_field_v4, sanitize_fvg_object_field_v4,
    truncate_tpo_dev_series_v4, FootprintMode, FOOTPRINT_MIN_STACK_LENGTHS,
    MGMT_EVENT_INDICATOR_RULES,
};
use chrono::{DateTime, Utc};
use serde_json::{Map, Value};

const AVWAP_LIMITS: &[(&str, usize)] = &[("15m", 3), ("4h", 2), ("1d", 2)];
const KLINE_LIMITS: &[(&str, usize)] = &[("15m", 16), ("4h", 15), ("1d", 10)];
const CVD_LIMITS: &[(&str, usize)] = &[("15m", 12), ("4h", 10), ("1d", 6)];
const MGMT_TOP_LIQUIDITY_LEVELS_LIMIT: usize = 60;

pub(super) fn filter_indicators(
    source: &Map<String, Value>,
    ts_bucket: Option<&str>,
    management_snapshot: Option<&Value>,
) -> Map<String, Value> {
    let mut indicators = Map::new();
    let current_ts = ts_bucket.and_then(parse_rfc3339_utc);
    let current_price = resolve_reference_mark_price(source);

    core_shared::insert_filtered_indicator(
        &mut indicators,
        source,
        "vpin",
        filter_vpin_management_v4,
    );
    core_shared::insert_full_indicator(&mut indicators, source, "whale_trades");
    core_shared::insert_filtered_indicator(
        &mut indicators,
        source,
        "high_volume_pulse",
        filter_hvp_management_v4,
    );

    core_shared::insert_filtered_indicator(
        &mut indicators,
        source,
        "tpo_market_profile",
        filter_tpo_management_v4,
    );
    core_shared::insert_filtered_indicator(
        &mut indicators,
        source,
        "price_volume_structure",
        |payload| prune_nulls(filter_price_volume_structure_entry_v3(payload)),
    );
    core_shared::insert_filtered_indicator(&mut indicators, source, "fvg", |payload| {
        filter_fvg_management_v4(payload)
    });
    core_shared::insert_filtered_indicator(&mut indicators, source, "avwap", |payload| {
        prune_nulls(filter_avwap(payload, AVWAP_LIMITS))
    });
    core_shared::insert_filtered_indicator(
        &mut indicators,
        source,
        "rvwap_sigma_bands",
        |payload| prune_nulls(filter_rvwap_sigma_bands(payload)),
    );
    core_shared::insert_filtered_indicator(
        &mut indicators,
        source,
        "footprint",
        filter_footprint_management_v4,
    );
    core_shared::insert_filtered_indicator(&mut indicators, source, "kline_history", |payload| {
        prune_nulls(filter_kline_history(payload, KLINE_LIMITS))
    });
    core_shared::insert_filtered_indicator(
        &mut indicators,
        source,
        "ema_trend_regime",
        filter_ema_management_v4,
    );
    core_shared::insert_filtered_indicator(&mut indicators, source, "cvd_pack", |payload| {
        prune_nulls(filter_cvd_pack_entry_v3(payload, CVD_LIMITS))
    });
    core_shared::insert_filtered_indicator(&mut indicators, source, "funding_rate", |payload| {
        prune_nulls(filter_funding_rate_entry_v3(payload))
    });
    core_shared::insert_filtered_indicator(
        &mut indicators,
        source,
        "liquidation_density",
        |payload| prune_nulls(filter_liquidation_density_entry_v3(payload)),
    );
    core_shared::insert_filtered_indicator(&mut indicators, source, "orderbook_depth", |payload| {
        prune_nulls(filter_orderbook_depth_entry_v3(
            payload,
            resolve_reference_mark_price(source),
            MGMT_TOP_LIQUIDITY_LEVELS_LIMIT,
        ))
    });

    for (code, keep_last) in MGMT_EVENT_INDICATOR_RULES {
        core_shared::insert_filtered_indicator(&mut indicators, source, code, |payload| {
            prune_nulls(filter_event_indicator_entry_v3(payload, *keep_last))
        });
    }

    core_shared::insert_filtered_indicator(
        &mut indicators,
        source,
        "divergence",
        filter_divergence_management_v4,
    );

    if let Some(events_summary) = build_events_summary_v4(&indicators, current_ts, current_price) {
        let mut payload = Map::new();
        payload.insert("payload".to_string(), Value::Object(events_summary));
        indicators.insert("events_summary".to_string(), Value::Object(payload));
    }

    if let Some(position_evidence) =
        build_position_evidence(&indicators, management_snapshot, current_ts, current_price)
    {
        let mut payload = Map::new();
        payload.insert("payload".to_string(), Value::Object(position_evidence));
        indicators.insert("position_evidence".to_string(), Value::Object(payload));
    }

    indicators
}

fn filter_tpo_management_v4(payload: &Value) -> Value {
    let mut filtered = filter_tpo_market_profile(payload);
    truncate_tpo_dev_series_v4(filtered.pointer_mut("/dev_series"));
    if let Some(by_session) = filtered
        .get_mut("by_session")
        .and_then(Value::as_object_mut)
    {
        for session in by_session.values_mut() {
            truncate_tpo_dev_series_v4(session.pointer_mut("/dev_series"));
        }
    }
    prune_nulls(filtered)
}

/// FVG for management:
/// - 15m / 4h: full data (active arrays, fvgs last 4, nearest, coverage_ratio, is_ready)
/// - 1d: only nearest_bull_fvg / nearest_bear_fvg (no active arrays, no fvgs history)
fn filter_fvg_management_v4(payload: &Value) -> Value {
    let mut filtered = filter_fvg(payload, &["15m", "4h", "1d"], 4);

    if let Some(by_window) = filtered.get_mut("by_window").and_then(Value::as_object_mut) {
        // 1d: strip arrays, keep only nearest FVGs
        if let Some(d1_window) = by_window.get_mut("1d").and_then(Value::as_object_mut) {
            d1_window.remove("active_bull_fvgs");
            d1_window.remove("active_bear_fvgs");
            d1_window.remove("fvgs");
        }

        // All windows: apply v4 9-field sanitization
        for window_value in by_window.values_mut() {
            let Some(window_object) = window_value.as_object_mut() else {
                continue;
            };
            sanitize_fvg_object_field_v4(window_object, "nearest_bull_fvg");
            sanitize_fvg_object_field_v4(window_object, "nearest_bear_fvg");
            sanitize_fvg_array_field_v4(window_object, "active_bull_fvgs");
            sanitize_fvg_array_field_v4(window_object, "active_bear_fvgs");
            sanitize_fvg_array_field_v4(window_object, "fvgs");
        }
    }
    prune_nulls(filtered)
}

fn filter_footprint_management_v4(payload: &Value) -> Value {
    let mut filtered = filter_footprint(payload, FootprintMode::Defensive);
    if let Some(by_window) = filtered.get_mut("by_window").and_then(Value::as_object_mut) {
        for (window, min_length) in FOOTPRINT_MIN_STACK_LENGTHS {
            let Some(window_value) = by_window.get_mut(*window).and_then(Value::as_object_mut)
            else {
                continue;
            };
            filter_stack_field_v4(window_value, "buy_stacks", *min_length);
            filter_stack_field_v4(window_value, "sell_stacks", *min_length);
            recompute_max_stack_len_v4(window_value, "buy_stacks", "max_buy_stack_len");
            recompute_max_stack_len_v4(window_value, "sell_stacks", "max_sell_stack_len");
        }
    }
    prune_nulls(filtered)
}

fn filter_ema_management_v4(payload: &Value) -> Value {
    let mut filtered = filter_ema_trend_regime(payload);
    if let Some(object) = filtered.as_object_mut() {
        object.remove("trend_regime");
        object.remove("trend_regime_by_tf");
        if let Some(output_sampling) = object
            .get_mut("output_sampling")
            .and_then(Value::as_object_mut)
        {
            for sample in output_sampling.values_mut() {
                if let Some(sample_object) = sample.as_object_mut() {
                    sample_object.remove("trend_regime");
                    sample_object.remove("by_tf");
                }
            }
        }
    }
    prune_nulls(filtered)
}

fn filter_divergence_management_v4(payload: &Value) -> Value {
    let mut filtered = filter_divergence(payload);
    if let Some(object) = filtered.as_object_mut() {
        object.remove("signal");
        object.remove("signals");
    }
    prune_nulls(filtered)
}

fn filter_vpin_management_v4(payload: &Value) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();
    core_shared::copy_fields(
        &mut result,
        payload,
        &[
            "vpin_bucket_size_eth",
            "vpin_fut",
            "vpin_model",
            "vpin_ratio_s_over_f",
            "vpin_rolling_bucket_count",
            "vpin_spot",
            "vpin_unit",
            "xmk_vpin_gap_s_minus_f",
            "z_vpin_fut",
            "z_vpin_gap_s_minus_f",
            "z_vpin_spot",
        ],
    );

    if let Some(by_window) = payload.get("by_window").and_then(Value::as_object) {
        let mut filtered_windows = Map::new();
        for window in ["15m", "1h", "4h", "1d", "3d"] {
            let Some(window_value) = by_window.get(window).and_then(Value::as_object) else {
                continue;
            };
            let mut filtered_window = Map::new();
            core_shared::copy_fields(
                &mut filtered_window,
                window_value,
                &[
                    "window",
                    "vpin_fut",
                    "vpin_ratio_s_over_f",
                    "vpin_spot",
                    "xmk_vpin_gap_s_minus_f",
                    "z_vpin_fut",
                    "z_vpin_gap_s_minus_f",
                    "z_vpin_spot",
                ],
            );
            filtered_windows.insert(window.to_string(), Value::Object(filtered_window));
        }
        result.insert("by_window".to_string(), Value::Object(filtered_windows));
    }

    prune_nulls(Value::Object(result))
}

fn build_position_evidence(
    indicators: &Map<String, Value>,
    management_snapshot: Option<&Value>,
    current_ts: Option<DateTime<Utc>>,
    current_price: Option<f64>,
) -> Option<Map<String, Value>> {
    let snapshot = management_snapshot?.as_object()?;
    let position = snapshot
        .get("positions")
        .and_then(Value::as_array)
        .and_then(|positions| positions.iter().find_map(Value::as_object));
    let position_context = snapshot.get("position_context").and_then(Value::as_object);

    let direction = position
        .and_then(|item| item.get("direction").and_then(Value::as_str))
        .or_else(|| position.and_then(|item| item.get("position_side").and_then(Value::as_str)))
        .unwrap_or("LONG");

    let entry_price = position
        .and_then(|item| item.get("entry_price").and_then(Value::as_f64))
        .or_else(|| {
            position_context
                .and_then(|ctx| ctx.get("effective_entry_price").and_then(Value::as_f64))
        })?;
    let current_sl_price = position
        .and_then(|item| item.get("current_sl_price").and_then(Value::as_f64))
        .or_else(|| {
            position_context.and_then(|ctx| ctx.get("effective_stop_loss").and_then(Value::as_f64))
        });
    let current_tp_price = position
        .and_then(|item| item.get("current_tp_price").and_then(Value::as_f64))
        .or_else(|| {
            position_context
                .and_then(|ctx| ctx.get("effective_take_profit").and_then(Value::as_f64))
        });

    let atr14 = extract_filtered_futures_bars(indicators, "15m")
        .as_deref()
        .and_then(compute_atr14)?;
    if atr14 <= 0.0 {
        return None;
    }

    let mut payload = Map::new();

    if let Some(current_price) = current_price {
        payload.insert(
            "price_to_entry_atr".to_string(),
            Value::from(round2((current_price - entry_price).abs() / atr14)),
        );

        if let Some(current_sl_price) = current_sl_price {
            payload.insert(
                "price_to_current_sl_atr".to_string(),
                Value::from(round2((current_price - current_sl_price).abs() / atr14)),
            );
        }

        if let Some(current_tp_price) = current_tp_price {
            payload.insert(
                "price_to_current_tp_atr".to_string(),
                Value::from(round2((current_tp_price - current_price).abs() / atr14)),
            );
        }

        if let Some(value) = position_in_tpo_pct(indicators, "4h", current_price) {
            payload.insert("position_in_4h_tpo_pct".to_string(), Value::from(value));
        }
        if let Some(value) = position_in_tpo_pct(indicators, "1d", current_price) {
            payload.insert("position_in_1d_range_pct".to_string(), Value::from(value));
        }
    }

    if let Some(current_sl_price) = current_sl_price {
        if let Some(value) =
            current_sl_beyond_nearest_structure(indicators, direction, current_sl_price, atr14)
        {
            payload.insert(
                "current_sl_beyond_nearest_structure".to_string(),
                Value::Bool(value),
            );
        }
    }

    if let Some(current_tp_price) = current_tp_price {
        if let Some(value) =
            current_tp_before_next_major_resistance(indicators, direction, current_tp_price, atr14)
        {
            payload.insert(
                "current_tp_before_next_major_resistance".to_string(),
                Value::Bool(value),
            );
        }
    }

    if let (Some(current_ts), Some(thesis_start_ts)) =
        (current_ts, thesis_start_ts_from_snapshot(snapshot))
    {
        let elapsed = (current_ts - thesis_start_ts).num_hours();
        if elapsed >= 0 {
            payload.insert(
                "thesis_bars_elapsed_4h".to_string(),
                Value::from(elapsed / 4),
            );
        }
    }

    match prune_nulls(Value::Object(payload)) {
        Value::Object(object) if !object.is_empty() => Some(object),
        _ => None,
    }
}

fn current_sl_beyond_nearest_structure(
    indicators: &Map<String, Value>,
    direction: &str,
    current_sl_price: f64,
    atr14: f64,
) -> Option<bool> {
    let is_short = is_short_direction(direction);
    let anchor = if is_short {
        nearest_hvn_anchor(indicators, "4h", current_sl_price, atr14)
            .or_else(|| extract_tpo_range(indicators, "4h").map(|(_, vah)| vah))
            .or_else(|| nearest_orderbook_wall(indicators, "ask_walls", current_sl_price, 0.003))
    } else {
        nearest_hvn_anchor(indicators, "4h", current_sl_price, atr14)
            .or_else(|| extract_tpo_range(indicators, "4h").map(|(val, _)| val))
            .or_else(|| nearest_orderbook_wall(indicators, "bid_walls", current_sl_price, 0.003))
    }?;

    Some(if is_short {
        current_sl_price > anchor
    } else {
        current_sl_price < anchor
    })
}

fn current_tp_before_next_major_resistance(
    indicators: &Map<String, Value>,
    direction: &str,
    current_tp_price: f64,
    atr14: f64,
) -> Option<bool> {
    let is_short = is_short_direction(direction);
    let anchor = if is_short {
        nearest_active_fvg_boundary(indicators, "bull", current_tp_price, atr14)
            .or_else(|| nearest_orderbook_wall(indicators, "bid_walls", current_tp_price, 0.005))
            .or_else(|| extract_tpo_range(indicators, "4h").map(|(val, _)| val))
    } else {
        nearest_active_fvg_boundary(indicators, "bear", current_tp_price, atr14)
            .or_else(|| nearest_orderbook_wall(indicators, "ask_walls", current_tp_price, 0.005))
            .or_else(|| extract_tpo_range(indicators, "4h").map(|(_, vah)| vah))
    }?;

    Some(if is_short {
        current_tp_price > anchor
    } else {
        current_tp_price < anchor
    })
}

fn is_short_direction(direction: &str) -> bool {
    direction.to_ascii_uppercase().contains("SHORT")
}

fn nearest_hvn_anchor(
    indicators: &Map<String, Value>,
    window: &str,
    target_price: f64,
    atr14: f64,
) -> Option<f64> {
    indicators
        .get("price_volume_structure")
        .and_then(|indicator| indicator.pointer(&format!("/payload/by_window/{window}/hvn_levels")))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_f64)
        .filter(|level| (level - target_price).abs() <= atr14)
        .min_by(|left, right| {
            (left - target_price)
                .abs()
                .partial_cmp(&(right - target_price).abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
}

fn nearest_orderbook_wall(
    indicators: &Map<String, Value>,
    side: &str,
    target_price: f64,
    threshold_pct: f64,
) -> Option<f64> {
    let threshold = target_price.abs() * threshold_pct;
    indicators
        .get("orderbook_depth")
        .and_then(|indicator| indicator.pointer(&format!("/payload/liquidity_walls/{side}")))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|wall| wall.get("price_level").and_then(Value::as_f64))
        .filter(|price| (price - target_price).abs() <= threshold)
        .min_by(|left, right| {
            (left - target_price)
                .abs()
                .partial_cmp(&(right - target_price).abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
}

fn nearest_active_fvg_boundary(
    indicators: &Map<String, Value>,
    side: &str,
    target_price: f64,
    atr14: f64,
) -> Option<f64> {
    let field = if side == "bear" {
        "active_bear_fvgs"
    } else {
        "active_bull_fvgs"
    };
    let boundary = if side == "bear" {
        "fvg_top"
    } else {
        "fvg_bottom"
    };

    ["15m", "4h"]
        .into_iter()
        .filter_map(|window| {
            indicators
                .get("fvg")
                .and_then(|indicator| {
                    indicator.pointer(&format!("/payload/by_window/{window}/{field}"))
                })
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| item.get(boundary).and_then(Value::as_f64))
                        .filter(|price| (price - target_price).abs() <= atr14)
                        .min_by(|left, right| {
                            (left - target_price)
                                .abs()
                                .partial_cmp(&(right - target_price).abs())
                                .unwrap_or(std::cmp::Ordering::Equal)
                        })
                })
                .flatten()
        })
        .min_by(|left, right| {
            (left - target_price)
                .abs()
                .partial_cmp(&(right - target_price).abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
}

fn position_in_tpo_pct(
    indicators: &Map<String, Value>,
    window: &str,
    current_price: f64,
) -> Option<f64> {
    let (val, vah) = extract_tpo_range(indicators, window)?;
    if vah <= val {
        return None;
    }
    Some(round2((current_price - val) / (vah - val)))
}

fn extract_tpo_range(indicators: &Map<String, Value>, window: &str) -> Option<(f64, f64)> {
    let payload = indicators
        .get("tpo_market_profile")
        .and_then(|indicator| indicator.get("payload"))?;
    let session = payload
        .pointer(&format!("/by_session/{window}"))
        .and_then(Value::as_object)
        .or_else(|| payload.as_object())?;
    let val = session.get("tpo_val").and_then(Value::as_f64)?;
    let vah = session.get("tpo_vah").and_then(Value::as_f64)?;
    Some((val, vah))
}

fn thesis_start_ts_from_snapshot(snapshot: &Map<String, Value>) -> Option<DateTime<Utc>> {
    snapshot
        .get("positions")
        .and_then(Value::as_array)
        .and_then(|positions| positions.first())
        .and_then(Value::as_object)
        .and_then(|position| position.get("opened_at").and_then(Value::as_str))
        .and_then(parse_rfc3339_utc)
        .or_else(|| {
            snapshot
                .get("position_context")
                .and_then(Value::as_object)
                .and_then(|ctx| ctx.get("entry_context"))
                .and_then(Value::as_object)
                .and_then(|entry_ctx| entry_ctx.get("analysis_ts").and_then(Value::as_str))
                .and_then(parse_rfc3339_utc)
        })
}

fn extract_filtered_futures_bars(
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

fn compute_atr14(bars: &[Map<String, Value>]) -> Option<f64> {
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

fn round2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

fn filter_hvp_management_v4(payload: &Value) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();
    core_shared::copy_fields(
        &mut result,
        payload,
        &[
            "as_of_ts",
            "indicator",
            "intrabar_poc_price",
            "intrabar_poc_volume",
            "intrabar_poc_max_by_window",
            "window",
        ],
    );

    if let Some(by_z_window) = payload.get("by_z_window").and_then(Value::as_object) {
        let mut filtered_windows = Map::new();
        for window in ["1h", "4h", "1d"] {
            let Some(window_value) = by_z_window.get(window).and_then(Value::as_object) else {
                continue;
            };
            let mut filtered_window = Map::new();
            core_shared::copy_fields(
                &mut filtered_window,
                window_value,
                &[
                    "lookback_samples",
                    "rolling_volume_w",
                    "volume_spike_z_w",
                    "window_minutes",
                ],
            );
            filtered_windows.insert(window.to_string(), Value::Object(filtered_window));
        }
        result.insert("by_z_window".to_string(), Value::Object(filtered_windows));
    }

    prune_nulls(Value::Object(result))
}
