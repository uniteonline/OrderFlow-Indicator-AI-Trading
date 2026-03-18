use super::core_shared::{
    self, copy_fields, filter_avwap, filter_cvd_pack, filter_divergence, filter_ema_trend_regime,
    filter_event_indicator_entry_v3, filter_footprint, filter_funding_rate, filter_fvg,
    filter_kline_history, filter_liquidation_density, filter_orderbook_depth_entry_v3,
    filter_price_volume_structure, filter_rvwap_sigma_bands, filter_tpo_market_profile,
    resolve_reference_mark_price, FootprintMode,
};
use chrono::{DateTime, Utc};
use serde_json::{Map, Value};

const AVWAP_LIMITS: &[(&str, usize)] = &[("15m", 8), ("4h", 3), ("1d", 2)];
const KLINE_LIMITS: &[(&str, usize)] = &[("15m", 24), ("4h", 12), ("1d", 8)];
const CVD_LIMITS: &[(&str, usize)] = &[("15m", 18), ("4h", 8), ("1d", 5)];
const PENDING_TOP_LIQUIDITY_LEVELS_LIMIT: usize = 100;
const PENDING_EVENT_INDICATOR_RULES: &[(&str, usize)] = &[
    ("absorption", 10),
    ("buying_exhaustion", 10),
    ("selling_exhaustion", 10),
    ("initiation", 10),
];
const PENDING_EVENT_SUMMARY_FIELDS: &[(&str, &str)] = &[
    ("absorption", "most_recent_absorption"),
    ("initiation", "most_recent_initiation"),
    ("buying_exhaustion", "most_recent_buying_exhaustion"),
    ("selling_exhaustion", "most_recent_selling_exhaustion"),
    ("divergence", "most_recent_divergence"),
];
const PENDING_FVG_KEEP_FIELDS: &[&str] = &[
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
const TPO_DEV_SERIES_LIMITS: &[(&str, usize)] = &[("15m", 8), ("1h", 5)];
const FOOTPRINT_MIN_STACK_LENGTHS: &[(&str, usize)] = &[("15m", 3), ("4h", 4), ("1d", 7)];

pub(super) fn filter_indicators(
    source: &Map<String, Value>,
    ts_bucket: Option<&str>,
    management_snapshot: Option<&Value>,
) -> Map<String, Value> {
    let mut indicators = Map::new();
    let current_ts = ts_bucket.and_then(parse_rfc3339_utc);
    let current_price = resolve_reference_mark_price(source);

    core_shared::insert_filtered_indicator(&mut indicators, source, "vpin", filter_vpin_pending_v1);
    core_shared::insert_filtered_indicator(&mut indicators, source, "whale_trades", |payload| {
        prune_nulls(payload.clone())
    });
    core_shared::insert_filtered_indicator(
        &mut indicators,
        source,
        "high_volume_pulse",
        filter_high_volume_pulse_pending_v1,
    );
    core_shared::insert_filtered_indicator(
        &mut indicators,
        source,
        "tpo_market_profile",
        filter_tpo_market_profile_pending_v1,
    );
    core_shared::insert_filtered_indicator(
        &mut indicators,
        source,
        "price_volume_structure",
        filter_price_volume_structure_pending_v1,
    );
    core_shared::insert_filtered_indicator(&mut indicators, source, "fvg", filter_fvg_pending_v1);
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
        filter_footprint_pending_v1,
    );
    core_shared::insert_filtered_indicator(&mut indicators, source, "kline_history", |payload| {
        prune_nulls(filter_kline_history(payload, KLINE_LIMITS))
    });
    core_shared::insert_filtered_indicator(
        &mut indicators,
        source,
        "ema_trend_regime",
        filter_ema_trend_regime_pending_v1,
    );
    core_shared::insert_filtered_indicator(&mut indicators, source, "cvd_pack", |payload| {
        prune_nulls(filter_cvd_pack(payload, CVD_LIMITS))
    });
    core_shared::insert_filtered_indicator(&mut indicators, source, "funding_rate", |payload| {
        prune_nulls(filter_funding_rate(payload))
    });
    core_shared::insert_filtered_indicator(
        &mut indicators,
        source,
        "liquidation_density",
        |payload| prune_nulls(filter_liquidation_density(payload)),
    );
    core_shared::insert_filtered_indicator(&mut indicators, source, "orderbook_depth", |payload| {
        prune_nulls(filter_orderbook_depth_entry_v3(
            payload,
            resolve_reference_mark_price(source),
            PENDING_TOP_LIQUIDITY_LEVELS_LIMIT,
        ))
    });

    for (code, keep_last) in PENDING_EVENT_INDICATOR_RULES {
        core_shared::insert_filtered_indicator(&mut indicators, source, code, |payload| {
            prune_nulls(filter_event_indicator_entry_v3(payload, *keep_last))
        });
    }

    core_shared::insert_filtered_indicator(
        &mut indicators,
        source,
        "divergence",
        filter_divergence_pending_v1,
    );

    if let Some(events_summary) = build_events_summary(&indicators, current_ts, current_price) {
        indicators.insert(
            "events_summary".to_string(),
            Value::Object(Map::from_iter([(
                "payload".to_string(),
                Value::Object(events_summary),
            )])),
        );
    }

    if let Some(pending_entry_evidence) =
        build_pending_entry_evidence(&indicators, management_snapshot, current_price)
    {
        indicators.insert(
            "pending_entry_evidence".to_string(),
            Value::Object(Map::from_iter([(
                "payload".to_string(),
                Value::Object(pending_entry_evidence),
            )])),
        );
    }

    indicators
}

fn filter_tpo_market_profile_pending_v1(payload: &Value) -> Value {
    let mut filtered = filter_tpo_market_profile(payload);
    truncate_tpo_dev_series(filtered.pointer_mut("/dev_series"));
    if let Some(by_session) = filtered
        .get_mut("by_session")
        .and_then(Value::as_object_mut)
    {
        for session in by_session.values_mut() {
            truncate_tpo_dev_series(session.pointer_mut("/dev_series"));
        }
    }
    prune_nulls(filtered)
}

fn filter_price_volume_structure_pending_v1(payload: &Value) -> Value {
    let mut filtered = filter_price_volume_structure(payload);
    if let Some(by_window) = filtered.get_mut("by_window").and_then(Value::as_object_mut) {
        by_window.remove("3d");
    }
    prune_nulls(filtered)
}

fn filter_fvg_pending_v1(payload: &Value) -> Value {
    let mut filtered = filter_fvg(payload, &["15m", "4h", "1d"], 6);
    if let Some(by_window) = filtered.get_mut("by_window").and_then(Value::as_object_mut) {
        for window_value in by_window.values_mut() {
            let Some(window_object) = window_value.as_object_mut() else {
                continue;
            };
            sanitize_fvg_object_field_v1(window_object, "nearest_bull_fvg");
            sanitize_fvg_object_field_v1(window_object, "nearest_bear_fvg");
            sanitize_fvg_array_field_v1(window_object, "active_bull_fvgs");
            sanitize_fvg_array_field_v1(window_object, "active_bear_fvgs");
            sanitize_fvg_array_field_v1(window_object, "fvgs");
        }
    }
    prune_nulls(filtered)
}

fn filter_footprint_pending_v1(payload: &Value) -> Value {
    let mut filtered = filter_footprint(payload, FootprintMode::Detailed);
    if let Some(by_window) = filtered.get_mut("by_window").and_then(Value::as_object_mut) {
        for (window, min_length) in FOOTPRINT_MIN_STACK_LENGTHS {
            let Some(window_value) = by_window.get_mut(*window).and_then(Value::as_object_mut)
            else {
                continue;
            };
            filter_stack_field(window_value, "buy_stacks", *min_length);
            filter_stack_field(window_value, "sell_stacks", *min_length);
            recompute_max_stack_len(window_value, "buy_stacks", "max_buy_stack_len");
            recompute_max_stack_len(window_value, "sell_stacks", "max_sell_stack_len");
        }
    }
    prune_nulls(filtered)
}

fn filter_ema_trend_regime_pending_v1(payload: &Value) -> Value {
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

fn filter_divergence_pending_v1(payload: &Value) -> Value {
    let mut filtered = filter_divergence(payload);
    if let Some(object) = filtered.as_object_mut() {
        object.remove("signal");
        object.remove("signals");
    }
    prune_nulls(filtered)
}

fn filter_vpin_pending_v1(payload: &Value) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();
    copy_fields(
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
            copy_fields(
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

fn filter_high_volume_pulse_pending_v1(payload: &Value) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();
    copy_fields(
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
            copy_fields(
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

fn build_events_summary(
    indicators: &Map<String, Value>,
    current_ts: Option<DateTime<Utc>>,
    current_price: Option<f64>,
) -> Option<Map<String, Value>> {
    let atr14 = extract_filtered_futures_bars(indicators, "15m")
        .as_deref()
        .and_then(compute_atr14);
    let mut payload = Map::new();

    for (indicator_code, output_key) in PENDING_EVENT_SUMMARY_FIELDS {
        let summary = build_most_recent_event_summary(
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

fn build_most_recent_event_summary(
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
        .and_then(|events| most_recent_event_from_array(events))
        .or_else(|| payload.get("latest_7d").and_then(Value::as_object))?;

    let mut summary = Map::new();
    if let Some(confirm_ts) = event_reference_ts_str(event) {
        summary.insert(
            "confirm_ts".to_string(),
            Value::String(confirm_ts.to_string()),
        );
    }
    if let (Some(event_ts), Some(current_ts)) = (event_reference_ts(event), current_ts) {
        summary.insert(
            "minutes_ago".to_string(),
            Value::from((current_ts - event_ts).num_minutes().max(0)),
        );
    }
    summary.insert(
        "direction".to_string(),
        Value::String(event_direction_label(event).to_string()),
    );
    if let Some(event_type) = event.get("type").cloned() {
        summary.insert("type".to_string(), event_type);
    }
    if let Some(price) = event_price(event) {
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

    match prune_nulls(Value::Object(summary)) {
        Value::Object(object) if !object.is_empty() => Some(Value::Object(object)),
        _ => None,
    }
}

fn build_pending_entry_evidence(
    indicators: &Map<String, Value>,
    management_snapshot: Option<&Value>,
    current_price: Option<f64>,
) -> Option<Map<String, Value>> {
    let snapshot = management_snapshot?.as_object()?;
    let pending_order = snapshot.get("pending_order").and_then(Value::as_object)?;
    let entry_price = pending_order
        .get("entry_price")
        .and_then(Value::as_f64)
        .or_else(|| {
            snapshot
                .get("position_context")
                .and_then(Value::as_object)
                .and_then(|ctx| ctx.get("effective_entry_price"))
                .and_then(Value::as_f64)
        })?;
    let direction = pending_order
        .get("direction")
        .and_then(Value::as_str)
        .or_else(|| pending_order.get("position_side").and_then(Value::as_str))
        .unwrap_or("LONG");
    let atr14 = extract_filtered_futures_bars(indicators, "15m")
        .as_deref()
        .and_then(compute_atr14)?;
    if atr14 <= 0.0 {
        return None;
    }

    let mut payload = Map::new();
    if let Some(current_price) = current_price {
        payload.insert(
            "entry_to_current_price_atr".to_string(),
            Value::from(round2((current_price - entry_price).abs() / atr14)),
        );
        payload.insert(
            "spent_move_pct_of_atr".to_string(),
            Value::from(round2(spent_move_pct_of_atr(
                direction,
                current_price,
                entry_price,
                atr14,
            ))),
        );
    }

    if let Some(orderbook) = indicators
        .get("orderbook_depth")
        .and_then(|indicator| indicator.get("payload"))
        .and_then(Value::as_object)
    {
        if let Some(support) = nearest_bid_wall(orderbook, entry_price) {
            payload.insert(
                "entry_to_nearest_support_atr".to_string(),
                Value::from(round2((entry_price - support).abs() / atr14)),
            );
        }
        if let Some(resistance) = nearest_ask_wall(orderbook, entry_price) {
            payload.insert(
                "entry_to_nearest_resistance_atr".to_string(),
                Value::from(round2((resistance - entry_price).abs() / atr14)),
            );
        }
        payload.insert(
            "entry_near_orderbook_wall".to_string(),
            Value::Bool(entry_near_any_orderbook_wall(orderbook, entry_price, 0.002)),
        );
        payload.insert(
            "entry_anchor_wall_still_present".to_string(),
            Value::Bool(anchor_wall_still_present(
                orderbook,
                entry_price,
                direction,
                0.003,
            )),
        );
    }

    payload.insert(
        "entry_inside_fvg".to_string(),
        Value::Bool(entry_inside_any_active_fvg(indicators, entry_price)),
    );
    payload.insert(
        "entry_inside_value_area".to_string(),
        Value::Bool(entry_inside_4h_value_area(indicators, entry_price)),
    );

    if let Some(z_value) = indicators
        .get("rvwap_sigma_bands")
        .and_then(|indicator| indicator.pointer("/payload/by_window/15m/z_price_minus_rvwap"))
        .and_then(Value::as_f64)
    {
        payload.insert("rvwap_15m_z_current".to_string(), Value::from(z_value));
    }

    match prune_nulls(Value::Object(payload)) {
        Value::Object(object) if !object.is_empty() => Some(object),
        _ => None,
    }
}

fn spent_move_pct_of_atr(direction: &str, current_price: f64, entry_price: f64, atr14: f64) -> f64 {
    let normalized = direction.to_ascii_uppercase();
    if normalized.contains("SHORT") {
        (entry_price - current_price) / atr14
    } else {
        (current_price - entry_price) / atr14
    }
}

fn nearest_bid_wall(orderbook: &Map<String, Value>, entry_price: f64) -> Option<f64> {
    liquidity_wall_array(orderbook, "bid_walls")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|wall| wall.get("price_level").and_then(Value::as_f64))
        .filter(|price| *price <= entry_price)
        .min_by(|left, right| {
            (entry_price - *left)
                .partial_cmp(&(entry_price - *right))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
}

fn nearest_ask_wall(orderbook: &Map<String, Value>, entry_price: f64) -> Option<f64> {
    liquidity_wall_array(orderbook, "ask_walls")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|wall| wall.get("price_level").and_then(Value::as_f64))
        .filter(|price| *price >= entry_price)
        .min_by(|left, right| {
            (*left - entry_price)
                .partial_cmp(&(*right - entry_price))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
}

fn entry_near_any_orderbook_wall(
    orderbook: &Map<String, Value>,
    entry_price: f64,
    threshold_pct: f64,
) -> bool {
    let threshold = entry_price.abs() * threshold_pct;
    orderbook
        .get("top_liquidity_levels")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|level| level.get("price_level").and_then(Value::as_f64))
        .any(|price| (price - entry_price).abs() <= threshold)
}

fn anchor_wall_still_present(
    orderbook: &Map<String, Value>,
    entry_price: f64,
    direction: &str,
    threshold_pct: f64,
) -> bool {
    let threshold = entry_price.abs() * threshold_pct;
    let path = if direction.to_ascii_uppercase().contains("SHORT") {
        "ask_walls"
    } else {
        "bid_walls"
    };
    liquidity_wall_array(orderbook, path)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|wall| wall.get("price_level").and_then(Value::as_f64))
        .any(|price| (price - entry_price).abs() <= threshold)
}

fn liquidity_wall_array<'a>(orderbook: &'a Map<String, Value>, side: &str) -> Option<&'a Value> {
    orderbook
        .get("liquidity_walls")
        .and_then(Value::as_object)
        .and_then(|walls| walls.get(side))
}

fn entry_inside_any_active_fvg(indicators: &Map<String, Value>, entry_price: f64) -> bool {
    let Some(by_window) = indicators
        .get("fvg")
        .and_then(|indicator| indicator.pointer("/payload/by_window"))
        .and_then(Value::as_object)
    else {
        return false;
    };

    ["15m", "4h", "1d"].iter().any(|window| {
        by_window
            .get(*window)
            .and_then(Value::as_object)
            .into_iter()
            .flat_map(|window_obj| {
                ["active_bull_fvgs", "active_bear_fvgs"]
                    .into_iter()
                    .filter_map(move |field| window_obj.get(field).and_then(Value::as_array))
                    .flatten()
            })
            .any(|fvg| fvg_contains_price(fvg, entry_price))
    })
}

fn entry_inside_4h_value_area(indicators: &Map<String, Value>, entry_price: f64) -> bool {
    let Some(window) = indicators
        .get("price_volume_structure")
        .and_then(|indicator| indicator.pointer("/payload/by_window/4h"))
        .and_then(Value::as_object)
    else {
        return false;
    };
    let vah = window.get("vah").and_then(Value::as_f64);
    let val = window.get("val").and_then(Value::as_f64);
    matches!((val, vah), (Some(val), Some(vah)) if val <= entry_price && entry_price <= vah)
}

fn fvg_contains_price(fvg: &Value, price: f64) -> bool {
    let Some(object) = fvg.as_object() else {
        return false;
    };
    let bottom = object.get("fvg_bottom").and_then(Value::as_f64);
    let top = object.get("fvg_top").and_then(Value::as_f64);
    matches!((bottom, top), (Some(bottom), Some(top)) if bottom <= price && price <= top)
}

fn truncate_tpo_dev_series(value: Option<&mut Value>) {
    let Some(dev_series) = value.and_then(Value::as_object_mut) else {
        return;
    };
    for (window, limit) in TPO_DEV_SERIES_LIMITS {
        if let Some(series) = dev_series.get_mut(*window).and_then(Value::as_array_mut) {
            series.truncate(*limit);
        }
    }
}

fn sanitize_fvg_object_field_v1(target: &mut Map<String, Value>, field: &str) {
    let Some(value) = target.remove(field) else {
        return;
    };
    let sanitized = sanitize_fvg_value_v1(value);
    if !sanitized.is_null() {
        target.insert(field.to_string(), sanitized);
    }
}

fn sanitize_fvg_array_field_v1(target: &mut Map<String, Value>, field: &str) {
    let Some(items) = target
        .remove(field)
        .and_then(|value| value.as_array().cloned())
    else {
        return;
    };
    let sanitized = items
        .into_iter()
        .map(sanitize_fvg_value_v1)
        .filter(|value| !value.is_null())
        .collect::<Vec<_>>();
    target.insert(field.to_string(), Value::Array(sanitized));
}

fn sanitize_fvg_value_v1(value: Value) -> Value {
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
    for field in PENDING_FVG_KEEP_FIELDS {
        if matches!(*field, "fvg_bottom" | "fvg_top") {
            continue;
        }
        if let Some(field_value) = entry.get(*field) {
            filtered.insert((*field).to_string(), field_value.clone());
        }
    }
    match prune_nulls(Value::Object(filtered)) {
        Value::Object(object) if object.is_empty() => Value::Null,
        other => other,
    }
}

fn filter_stack_field(window_value: &mut Map<String, Value>, field: &str, min_length: usize) {
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

fn recompute_max_stack_len(window_value: &mut Map<String, Value>, field: &str, max_field: &str) {
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

fn event_reference_ts(event: &Map<String, Value>) -> Option<DateTime<Utc>> {
    event_reference_ts_str(event).and_then(parse_rfc3339_utc)
}

fn event_reference_ts_str<'a>(event: &'a Map<String, Value>) -> Option<&'a str> {
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

fn most_recent_event_from_array(events: &[Value]) -> Option<&Map<String, Value>> {
    events
        .iter()
        .filter_map(Value::as_object)
        .max_by_key(|event| event_reference_ts(*event))
}

fn event_direction_label(event: &Map<String, Value>) -> &'static str {
    if let Some(direction) = event.get("direction") {
        if let Some(value) = direction.as_i64() {
            return direction_label_from_sign(value as f64);
        }
        if let Some(value) = direction.as_f64() {
            return direction_label_from_sign(value);
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
        .map(event_direction_from_type)
        .unwrap_or("neutral")
}

fn direction_label_from_sign(value: f64) -> &'static str {
    if value > 0.0 {
        "bullish"
    } else if value < 0.0 {
        "bearish"
    } else {
        "neutral"
    }
}

fn event_direction_from_type(event_type: &str) -> &'static str {
    let normalized = event_type.to_ascii_lowercase();
    if normalized.contains("bullish") || normalized == "selling_exhaustion" {
        "bullish"
    } else if normalized.contains("bearish") || normalized == "buying_exhaustion" {
        "bearish"
    } else {
        "neutral"
    }
}

fn event_price(event: &Map<String, Value>) -> Option<f64> {
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

fn parse_rfc3339_utc(ts: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(ts)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn round2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

fn prune_nulls(value: Value) -> Value {
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
