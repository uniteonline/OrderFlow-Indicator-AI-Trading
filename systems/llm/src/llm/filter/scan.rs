use crate::llm::provider::ModelInvocationInput;
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Map, Value};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs;

const SCAN_WINDOWS: &[&str] = &["15m", "4h", "1d"];
const SCAN_VA_TOP_LEVELS: usize = 5;
const EVENT_INDICATOR_RULES: &[(&str, usize)] = &[
    ("absorption", 20),
    ("buying_exhaustion", 20),
    ("selling_exhaustion", 20),
    ("bullish_absorption", 20),
    ("bearish_absorption", 20),
    ("bullish_initiation", 10),
    ("bearish_initiation", 10),
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

#[derive(Debug, Clone)]
struct EvidenceSnapshot {
    ts_bucket: String,
    trend_balance_score: f64,
    flow_balance_score: f64,
    evidence_balance_score: f64,
    primary_support: f64,
    primary_resistance: f64,
}

#[derive(Debug, Clone, Copy)]
struct BiasEvidence {
    trend_balance_score: f64,
    flow_balance_score: f64,
    structure_balance_score: f64,
    evidence_balance_score: f64,
    conflict_score: f64,
    compression_score: f64,
}

#[derive(Debug, Clone)]
struct LevelMember {
    price: f64,
    weight: f64,
    source: &'static str,
}

#[derive(Debug, Clone)]
struct LevelCluster {
    price: f64,
    strength: f64,
    sources: Vec<&'static str>,
}

#[derive(Debug, Clone)]
struct LevelAnchor {
    price: f64,
    strength: f64,
    source_count: usize,
}

#[derive(Debug, Clone)]
struct WeightedEvent {
    event: Map<String, Value>,
    weighted_score: f64,
}

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
    let indicators = root
        .get("indicators")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let symbol = root
        .get("symbol")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let ts_bucket = root
        .get("ts_bucket")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let current_ts = parse_rfc3339_utc(&ts_bucket);
    let current_price = extract_current_price(&indicators);
    let kline_derived = build_kline_derived(&indicators);
    let level_book = current_price
        .map(|price| build_level_book(&indicators, price, &kline_derived))
        .unwrap_or_else(empty_tf_object);
    let cvd_summary = build_cvd_summary(
        indicators
            .get("cvd_pack")
            .and_then(|indicator| indicator.get("payload")),
    );
    let absorption_summary = build_event_summary(
        indicators
            .get("absorption")
            .and_then(|indicator| indicator.get("payload")),
        current_ts.clone(),
        current_price,
        &kline_derived,
        event_direction_default,
    );
    let buying_exhaustion_summary = build_event_summary(
        indicators
            .get("buying_exhaustion")
            .and_then(|indicator| indicator.get("payload")),
        current_ts.clone(),
        current_price,
        &kline_derived,
        event_direction_default,
    );
    let selling_exhaustion_summary = build_event_summary(
        indicators
            .get("selling_exhaustion")
            .and_then(|indicator| indicator.get("payload")),
        current_ts.clone(),
        current_price,
        &kline_derived,
        event_direction_default,
    );
    let divergence_summary = build_event_summary(
        indicators
            .get("divergence")
            .and_then(|indicator| indicator.get("payload")),
        current_ts.clone(),
        current_price,
        &kline_derived,
        event_direction_divergence,
    );
    let initiation_summary = build_combined_initiation_summary(
        indicators
            .get("bullish_initiation")
            .and_then(|indicator| indicator.get("payload")),
        indicators
            .get("bearish_initiation")
            .and_then(|indicator| indicator.get("payload")),
        current_ts.clone(),
        current_price,
        &kline_derived,
    );
    let by_timeframe = build_by_timeframe(
        &indicators,
        &kline_derived,
        &level_book,
        &cvd_summary,
        &absorption_summary,
        &buying_exhaustion_summary,
        &selling_exhaustion_summary,
        &divergence_summary,
        &initiation_summary,
    );
    let history = load_recent_scan_evidence(&symbol, &ts_bucket);
    let timeframe_evidence =
        build_timeframe_evidence(&symbol, &ts_bucket, current_price, &by_timeframe, &history);
    let multi_timeframe_evidence =
        build_multi_timeframe_evidence(current_price, &timeframe_evidence);

    if let Some(symbol) = root.get("symbol") {
        result.insert("symbol".to_string(), symbol.clone());
    }
    if let Some(ts_bucket) = root.get("ts_bucket") {
        result.insert("ts_bucket".to_string(), ts_bucket.clone());
    }
    result.insert(
        "current_price".to_string(),
        current_price.map(Value::from).unwrap_or(Value::Null),
    );
    result.insert("by_timeframe".to_string(), Value::Object(by_timeframe));
    result.insert(
        "timeframe_evidence".to_string(),
        Value::Object(timeframe_evidence),
    );
    result.insert(
        "multi_timeframe_evidence".to_string(),
        multi_timeframe_evidence,
    );
    result.insert(
        "indicators".to_string(),
        Value::Object(filter_indicators(&indicators, current_price)),
    );
    Value::Object(result)
}

fn filter_indicators(
    source: &Map<String, Value>,
    current_price: Option<f64>,
) -> Map<String, Value> {
    let mut indicators = Map::new();

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
    insert_filtered_indicator(
        &mut indicators,
        source,
        "ema_trend_regime",
        filter_ema_trend_regime,
    );
    insert_filtered_indicator(&mut indicators, source, "footprint", |payload| {
        filter_footprint_with_price(payload, current_price)
    });
    insert_filtered_indicator(
        &mut indicators,
        source,
        "orderbook_depth",
        filter_orderbook_depth,
    );

    indicators
}

fn insert_full_indicator(target: &mut Map<String, Value>, source: &Map<String, Value>, code: &str) {
    if let Some(indicator) = source.get(code) {
        let payload = indicator.get("payload").cloned().unwrap_or(Value::Null);
        target.insert(code.to_string(), rebuild_indicator(indicator, payload));
    }
}

fn insert_filtered_indicator<F>(
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

fn empty_tf_object() -> Value {
    let mut map = Map::new();
    for tf in SCAN_WINDOWS {
        map.insert((*tf).to_string(), Value::Object(Map::new()));
    }
    Value::Object(map)
}

fn parse_rfc3339_utc(ts: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(ts)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn extract_current_price(source: &Map<String, Value>) -> Option<f64> {
    source
        .get("avwap")
        .and_then(|indicator| indicator.get("payload"))
        .and_then(|payload| payload.get("fut_last_price"))
        .and_then(Value::as_f64)
        .or_else(|| {
            source
                .get("orderbook_depth")
                .and_then(|indicator| indicator.get("payload"))
                .and_then(|payload| payload.get("microprice_fut"))
                .and_then(Value::as_f64)
        })
        .or_else(|| {
            source
                .get("funding_rate")
                .and_then(|indicator| indicator.get("payload"))
                .and_then(|payload| payload.get("mark_price_last"))
                .and_then(Value::as_f64)
        })
        .or_else(|| latest_kline_close(source, "15m"))
}

fn latest_kline_close(source: &Map<String, Value>, tf: &str) -> Option<f64> {
    source
        .get("kline_history")
        .and_then(|indicator| indicator.get("payload"))
        .and_then(|payload| payload.get("intervals"))
        .and_then(Value::as_object)
        .and_then(|intervals| intervals.get(tf))
        .and_then(Value::as_object)
        .and_then(|interval| interval.get("markets"))
        .and_then(Value::as_object)
        .and_then(|markets| markets.get("futures"))
        .and_then(Value::as_object)
        .and_then(|futures| futures.get("bars"))
        .and_then(Value::as_array)
        .and_then(|bars| bars.last())
        .and_then(Value::as_object)
        .and_then(|bar| bar.get("close"))
        .and_then(Value::as_f64)
}

fn round2(value: f64) -> f64 {
    round_to_decimals(value, 2)
}

fn round3(value: f64) -> f64 {
    round_to_decimals(value, 3)
}

fn sign_i8(value: f64) -> i8 {
    match value.partial_cmp(&0.0).unwrap_or(Ordering::Equal) {
        Ordering::Greater => 1,
        Ordering::Less => -1,
        Ordering::Equal => 0,
    }
}

fn sign_from_option(value: Option<f64>) -> i8 {
    value.map(sign_i8).unwrap_or(0)
}

fn bias_from_sign(sign: i8) -> &'static str {
    match sign {
        1 => "bullish",
        -1 => "bearish",
        _ => "neutral",
    }
}

fn timeframe_duration(tf: &str) -> Duration {
    match tf {
        "15m" => Duration::hours(4),
        "4h" => Duration::hours(24),
        _ => Duration::days(7),
    }
}

fn tf_window_limit(tf: &str) -> usize {
    match tf {
        "15m" => 30,
        "4h" => 20,
        _ => usize::MAX,
    }
}

fn extract_indicator_payload<'a>(
    source: &'a Map<String, Value>,
    code: &str,
) -> Option<&'a Map<String, Value>> {
    source
        .get(code)
        .and_then(|indicator| indicator.get("payload"))
        .and_then(Value::as_object)
}

fn extract_futures_bars(source: &Map<String, Value>, tf: &str) -> Vec<Map<String, Value>> {
    source
        .get("kline_history")
        .and_then(|indicator| indicator.get("payload"))
        .and_then(|payload| payload.get("intervals"))
        .and_then(Value::as_object)
        .and_then(|intervals| intervals.get(tf))
        .and_then(Value::as_object)
        .and_then(|interval| interval.get("markets"))
        .and_then(Value::as_object)
        .and_then(|markets| markets.get("futures"))
        .and_then(Value::as_object)
        .and_then(|futures| futures.get("bars"))
        .and_then(Value::as_array)
        .map(|bars| {
            bars.iter()
                .filter_map(Value::as_object)
                .cloned()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
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
    let tail = true_ranges.len().saturating_sub(14);
    let sample = &true_ranges[tail..];
    Some(sample.iter().sum::<f64>() / sample.len() as f64)
}

fn build_kline_derived(source: &Map<String, Value>) -> Value {
    let pvs_by_window = extract_indicator_payload(source, "price_volume_structure")
        .and_then(|payload| payload.get("by_window"))
        .and_then(Value::as_object);
    let ema_payload = extract_indicator_payload(source, "ema_trend_regime");
    let ema21 = ema_payload
        .and_then(|payload| payload.get("ema_21"))
        .and_then(Value::as_f64);
    let ema100_htf = ema_payload
        .and_then(|payload| payload.get("ema_100_htf"))
        .and_then(Value::as_object);

    let mut by_window = Map::new();
    for tf in SCAN_WINDOWS {
        let bars = extract_futures_bars(source, tf);
        let latest = bars.last();
        let close = latest
            .and_then(|bar| bar.get("close"))
            .and_then(Value::as_f64);
        let atr14 = compute_atr14(&bars);
        let n_swing = match *tf {
            "15m" => 10,
            "4h" => 5,
            _ => 3,
        };
        let swing_high = bars
            .iter()
            .rev()
            .take(n_swing)
            .filter_map(|bar| bar.get("high").and_then(Value::as_f64))
            .max_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
        let swing_low = bars
            .iter()
            .rev()
            .take(n_swing)
            .filter_map(|bar| bar.get("low").and_then(Value::as_f64))
            .min_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
        let pvs_window = pvs_by_window
            .and_then(|windows| windows.get(*tf))
            .and_then(Value::as_object);
        let vah = pvs_window
            .and_then(|window| window.get("vah"))
            .and_then(Value::as_f64);
        let val = pvs_window
            .and_then(|window| window.get("val"))
            .and_then(Value::as_f64);
        let value_area_position = match (close, vah, val) {
            (Some(close), Some(vah), _) if close > vah => Value::String("above_vah".to_string()),
            (Some(close), _, Some(val)) if close < val => Value::String("below_val".to_string()),
            (Some(_), Some(_), Some(_)) => Value::String("inside_va".to_string()),
            _ => Value::Null,
        };
        let close_vs_ema21_pct = if *tf == "15m" {
            pct_delta(close, ema21)
        } else {
            None
        };
        let close_vs_ema100_htf_pct = if *tf == "15m" {
            None
        } else {
            ema100_htf
                .and_then(|htf| htf.get(*tf))
                .and_then(Value::as_f64)
                .and_then(|ema| pct_delta(close, Some(ema)))
        };
        let close_vs_vah_pct = pct_delta(close, vah);

        by_window.insert(
            (*tf).to_string(),
            json!({
                "atr14": atr14.map(round2),
                "atr14_pct": atr14.and_then(|atr| close.filter(|close| close.abs() > f64::EPSILON).map(|close| round2(atr / close * 100.0))),
                "swing_high": swing_high,
                "swing_low": swing_low,
                "close_vs_ema21_pct": close_vs_ema21_pct.map(round2),
                "close_vs_ema100_htf_pct": close_vs_ema100_htf_pct.map(round2),
                "close_vs_vah_pct": close_vs_vah_pct.map(round2),
                "value_area_position": value_area_position,
            }),
        );
    }

    Value::Object(by_window)
}

fn pct_delta(current: Option<f64>, reference: Option<f64>) -> Option<f64> {
    let current = current?;
    let reference = reference?;
    if reference.abs() <= f64::EPSILON {
        return None;
    }
    Some((current - reference) / reference * 100.0)
}

fn build_cvd_summary(payload: Option<&Value>) -> Value {
    let Some(payload) = payload.and_then(Value::as_object) else {
        return empty_tf_object();
    };
    let mut result = Map::new();
    result.insert(
        "snapshot".to_string(),
        json!({
            "delta_fut": payload.get("delta_fut").and_then(Value::as_f64).map(round2),
            "delta_spot": payload.get("delta_spot").and_then(Value::as_f64).map(round2),
            "relative_delta_fut": payload.get("relative_delta_fut").and_then(Value::as_f64).map(round3),
            "relative_delta_spot": payload.get("relative_delta_spot").and_then(Value::as_f64).map(round3),
        }),
    );

    let mut by_window = Map::new();
    let source_windows = payload.get("by_window").and_then(Value::as_object);
    for tf in SCAN_WINDOWS {
        let summary = source_windows
            .and_then(|windows| windows.get(*tf))
            .and_then(Value::as_object)
            .map(|window| build_cvd_window_summary(window, tf_window_limit(tf)))
            .unwrap_or_else(|| {
                json!({
                    "delta_sign": 0,
                    "relative_delta": Value::Null,
                    "slope_sign": 0,
                    "lead": "balanced",
                    "bias": "neutral",
                    "last_ts": Value::Null,
                })
            });
        by_window.insert((*tf).to_string(), summary);
    }
    result.insert("by_window".to_string(), Value::Object(by_window));
    Value::Object(result)
}

fn build_cvd_window_summary(window: &Map<String, Value>, limit: usize) -> Value {
    let series = window
        .get("series")
        .and_then(Value::as_array)
        .map(|series| {
            let tail = if series.len() <= limit {
                series.to_vec()
            } else {
                series[series.len() - limit..].to_vec()
            };
            tail.into_iter()
                .filter_map(|entry| entry.as_object().cloned())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let latest = series.last();
    let oldest = series.first();
    let delta_sign = latest
        .and_then(|entry| entry.get("delta_fut"))
        .and_then(Value::as_f64)
        .map(sign_i8)
        .unwrap_or(0);
    let slope_sign = match (
        latest
            .and_then(|entry| entry.get("cvd_7d_fut"))
            .and_then(Value::as_f64),
        oldest
            .and_then(|entry| entry.get("cvd_7d_fut"))
            .and_then(Value::as_f64),
    ) {
        (Some(latest), Some(oldest)) => sign_i8(latest - oldest),
        _ => 0,
    };
    let bias = if delta_sign != 0 && delta_sign == slope_sign {
        bias_from_sign(delta_sign)
    } else {
        "neutral"
    };

    json!({
        "delta_sign": delta_sign,
        "relative_delta": latest
            .and_then(|entry| entry.get("relative_delta_fut"))
            .and_then(Value::as_f64)
            .map(round3),
        "slope_sign": slope_sign,
        "lead": latest
            .and_then(|entry| entry.get("spot_flow_dominance"))
            .and_then(Value::as_str)
            .unwrap_or("balanced"),
        "bias": bias,
        "last_ts": latest
            .and_then(|entry| entry.get("ts"))
            .cloned()
            .unwrap_or(Value::Null),
    })
}

fn event_direction_default(event: &Map<String, Value>) -> i8 {
    event
        .get("direction")
        .and_then(Value::as_i64)
        .map(|value| value.clamp(-1, 1) as i8)
        .unwrap_or_else(|| {
            let kind = event
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if kind.contains("bullish") {
                1
            } else if kind.contains("bearish") {
                -1
            } else {
                0
            }
        })
}

fn event_direction_divergence(event: &Map<String, Value>) -> i8 {
    let kind = event
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if kind.contains("bullish") {
        1
    } else if kind.contains("bearish") {
        -1
    } else {
        sign_from_option(event.get("fut_divergence_sign").and_then(Value::as_f64))
    }
}

fn build_event_summary(
    payload: Option<&Value>,
    current_ts: Option<DateTime<Utc>>,
    current_price: Option<f64>,
    atr14_by_tf: &Value,
    direction_fn: fn(&Map<String, Value>) -> i8,
) -> Value {
    let events = payload
        .and_then(Value::as_object)
        .and_then(|payload| payload.get("recent_7d"))
        .and_then(Value::as_object)
        .and_then(|recent| recent.get("events"))
        .and_then(Value::as_array)
        .map(|events| {
            events
                .iter()
                .filter_map(Value::as_object)
                .cloned()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    build_event_summary_from_events(
        &events,
        current_ts,
        current_price,
        atr14_by_tf,
        direction_fn,
    )
}

fn build_combined_initiation_summary(
    bullish_payload: Option<&Value>,
    bearish_payload: Option<&Value>,
    current_ts: Option<DateTime<Utc>>,
    current_price: Option<f64>,
    atr14_by_tf: &Value,
) -> Value {
    let mut events = Vec::new();
    for payload in [bullish_payload, bearish_payload] {
        let mut selected = payload
            .and_then(Value::as_object)
            .and_then(|payload| payload.get("recent_7d"))
            .and_then(Value::as_object)
            .and_then(|recent| recent.get("events"))
            .and_then(Value::as_array)
            .map(|events| {
                events
                    .iter()
                    .filter_map(Value::as_object)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        events.append(&mut selected);
    }
    build_event_summary_from_events(
        &events,
        current_ts,
        current_price,
        atr14_by_tf,
        event_direction_default,
    )
}

fn build_event_summary_from_events(
    events: &[Map<String, Value>],
    current_ts: Option<DateTime<Utc>>,
    current_price: Option<f64>,
    atr14_by_tf: &Value,
    direction_fn: fn(&Map<String, Value>) -> i8,
) -> Value {
    let mut result = Map::new();
    for tf in SCAN_WINDOWS {
        let filtered = select_events_for_timeframe(events, current_ts, tf, direction_fn);
        let atr14 = atr14_for_tf(atr14_by_tf, tf);
        let bullish = filtered
            .iter()
            .filter(|event| direction_fn(event) > 0)
            .cloned()
            .collect::<Vec<_>>();
        let bearish = filtered
            .iter()
            .filter(|event| direction_fn(event) < 0)
            .cloned()
            .collect::<Vec<_>>();
        let bullish_score =
            weighted_event_side_score(&bullish, current_ts.clone(), current_price, atr14, tf);
        let bearish_score =
            weighted_event_side_score(&bearish, current_ts.clone(), current_price, atr14, tf);
        let net_score = round3(bullish_score - bearish_score);
        let bias = weighted_event_bias(bullish_score, bearish_score);
        let bullish_events =
            pick_representative_events(&bullish, current_ts.clone(), current_price, atr14, tf);
        let bearish_events =
            pick_representative_events(&bearish, current_ts.clone(), current_price, atr14, tf);
        result.insert(
            (*tf).to_string(),
            json!({
                "bullish_count": bullish.len(),
                "bearish_count": bearish.len(),
                "bullish_score": round3(bullish_score),
                "bearish_score": round3(bearish_score),
                "net_score": net_score,
                "bias": bias,
                "bullish_events": bullish_events,
                "bearish_events": bearish_events,
            }),
        );
    }
    Value::Object(result)
}

fn select_events_for_timeframe(
    events: &[Map<String, Value>],
    current_ts: Option<DateTime<Utc>>,
    tf: &str,
    direction_fn: fn(&Map<String, Value>) -> i8,
) -> Vec<Map<String, Value>> {
    let Some(current_ts) = current_ts else {
        return events
            .iter()
            .filter(|event| direction_fn(event) != 0)
            .cloned()
            .collect();
    };
    let cutoff = current_ts - timeframe_duration(tf);
    events
        .iter()
        .filter(|event| {
            direction_fn(event) != 0
                && event_reference_ts(event)
                    .map(|event_ts| event_ts >= cutoff)
                    .unwrap_or(false)
        })
        .cloned()
        .collect()
}

fn event_reference_ts(event: &Map<String, Value>) -> Option<DateTime<Utc>> {
    ["event_start_ts", "confirm_ts", "event_end_ts", "end_ts"]
        .iter()
        .find_map(|key| event.get(*key).and_then(Value::as_str))
        .and_then(parse_rfc3339_utc)
}

fn atr14_for_tf(source: &Value, tf: &str) -> Option<f64> {
    source
        .get(tf)
        .and_then(Value::as_object)
        .and_then(|window| window.get("atr14"))
        .and_then(Value::as_f64)
        .filter(|value| *value > 0.0)
}

fn weighted_event_side_score(
    events: &[Map<String, Value>],
    current_ts: Option<DateTime<Utc>>,
    current_price: Option<f64>,
    atr14: Option<f64>,
    tf: &str,
) -> f64 {
    events
        .iter()
        .map(|event| weighted_event_score(event, current_ts, current_price, atr14, tf))
        .sum()
}

fn weighted_event_bias(bullish_score: f64, bearish_score: f64) -> &'static str {
    let total = bullish_score + bearish_score;
    if total <= 0.0 {
        return "neutral";
    }
    let normalized = (bullish_score - bearish_score) / total;
    if normalized > 0.15 {
        "bullish"
    } else if normalized < -0.15 {
        "bearish"
    } else {
        "neutral"
    }
}

fn weighted_event_score(
    event: &Map<String, Value>,
    current_ts: Option<DateTime<Utc>>,
    current_price: Option<f64>,
    atr14: Option<f64>,
    tf: &str,
) -> f64 {
    let base_score = event_score(event).max(0.25);
    let recency_weight = event_reference_ts(event)
        .zip(current_ts)
        .map(|(event_ts, now)| {
            let age_hours = (now - event_ts).num_seconds().max(0) as f64 / 3600.0;
            (-age_hours / timeframe_decay_hours(tf)).exp()
        })
        .unwrap_or(1.0);
    let proximity_weight = match (event_pivot_price(event), current_price, atr14) {
        (Some(pivot_price), Some(current_price), Some(atr14)) if atr14 > 0.0 => {
            (-(pivot_price - current_price).abs() / (timeframe_distance_atr(tf) * atr14)).exp()
        }
        _ => 1.0,
    };
    base_score * recency_weight * proximity_weight
}

fn timeframe_decay_hours(tf: &str) -> f64 {
    match tf {
        "15m" => 5.0,
        "4h" => 24.0,
        _ => 60.0,
    }
}

fn timeframe_distance_atr(tf: &str) -> f64 {
    match tf {
        "15m" => 1.0,
        "4h" => 1.5,
        _ => 2.0,
    }
}

fn pick_representative_events(
    events: &[Map<String, Value>],
    current_ts: Option<DateTime<Utc>>,
    current_price: Option<f64>,
    atr14: Option<f64>,
    tf: &str,
) -> Vec<Value> {
    let mut selected: Vec<WeightedEvent> = Vec::new();
    if let Some(recent) = events
        .iter()
        .max_by(|left, right| event_reference_ts(left).cmp(&event_reference_ts(right)))
    {
        selected.push(WeightedEvent {
            event: recent.clone(),
            weighted_score: weighted_event_score(recent, current_ts, current_price, atr14, tf),
        });
    }
    if let Some(strongest) = events.iter().max_by(|left, right| {
        weighted_event_score(left, current_ts, current_price, atr14, tf)
            .partial_cmp(&weighted_event_score(
                right,
                current_ts,
                current_price,
                atr14,
                tf,
            ))
            .unwrap_or(Ordering::Equal)
    }) {
        selected.push(WeightedEvent {
            event: strongest.clone(),
            weighted_score: weighted_event_score(strongest, current_ts, current_price, atr14, tf),
        });
    }
    if let Some(current_price) = current_price {
        if let Some(nearest) = events
            .iter()
            .filter(|event| event_pivot_price(event).is_some())
            .min_by(|left, right| {
                let left_dist =
                    (event_pivot_price(left).unwrap_or(current_price) - current_price).abs();
                let right_dist =
                    (event_pivot_price(right).unwrap_or(current_price) - current_price).abs();
                left_dist
                    .partial_cmp(&right_dist)
                    .unwrap_or(Ordering::Equal)
            })
        {
            selected.push(WeightedEvent {
                event: nearest.clone(),
                weighted_score: weighted_event_score(
                    nearest,
                    current_ts,
                    Some(current_price),
                    atr14,
                    tf,
                ),
            });
        }
    }
    selected.sort_by(|left, right| {
        left.weighted_score
            .partial_cmp(&right.weighted_score)
            .unwrap_or(Ordering::Equal)
            .reverse()
            .then_with(|| event_reference_ts(&right.event).cmp(&event_reference_ts(&left.event)))
    });
    let mut deduped = Vec::new();
    for event in selected {
        let key = format!(
            "{}|{}|{}",
            event_time_string(&event.event),
            event
                .event
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or_default(),
            event_pivot_price(&event.event)
                .map(|price| round2(price).to_string())
                .unwrap_or_default()
        );
        if deduped
            .iter()
            .all(|existing: &WeightedEvent| event_dedupe_key(&existing.event) != key)
        {
            deduped.push(event);
        }
    }
    deduped
        .into_iter()
        .take(3)
        .map(|event| compact_event(event.event, event.weighted_score))
        .collect()
}

fn event_dedupe_key(event: &Map<String, Value>) -> String {
    format!(
        "{}|{}|{}",
        event_time_string(event),
        event
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        event_pivot_price(event)
            .map(|price| round2(price).to_string())
            .unwrap_or_default()
    )
}

fn event_time_string(event: &Map<String, Value>) -> String {
    ["confirm_ts", "event_start_ts", "event_end_ts", "end_ts"]
        .iter()
        .find_map(|key| event.get(*key).and_then(Value::as_str))
        .unwrap_or_default()
        .to_string()
}

fn event_score(event: &Map<String, Value>) -> f64 {
    event
        .get("score")
        .and_then(Value::as_f64)
        .or_else(|| event.get("strength_score_xmk").and_then(Value::as_f64))
        .unwrap_or(0.0)
}

fn event_pivot_price(event: &Map<String, Value>) -> Option<f64> {
    event
        .get("pivot_price")
        .and_then(Value::as_f64)
        .or_else(|| event.get("price_high").and_then(Value::as_f64))
        .or_else(|| event.get("price_low").and_then(Value::as_f64))
}

fn compact_event(event: Map<String, Value>, weighted_score: f64) -> Value {
    let mut compact = Map::new();
    compact.insert(
        "confirm_ts".to_string(),
        Value::String(event_time_string(&event)),
    );
    if let Some(price) = event_pivot_price(&event) {
        compact.insert("pivot_price".to_string(), Value::from(price));
    }
    compact.insert(
        "score".to_string(),
        Value::from(round3(event_score(&event))),
    );
    compact.insert(
        "weighted_score".to_string(),
        Value::from(round3(weighted_score)),
    );
    if let Some(kind) = event.get("type").and_then(Value::as_str) {
        compact.insert("type".to_string(), Value::String(kind.to_string()));
    }
    if let Some(pivot_side) = event.get("pivot_side").and_then(Value::as_str) {
        compact.insert(
            "pivot_side".to_string(),
            Value::String(pivot_side.to_string()),
        );
    }
    if let Some(driver) = event.get("likely_driver").and_then(Value::as_str) {
        compact.insert(
            "likely_driver".to_string(),
            Value::String(driver.to_string()),
        );
    }
    Value::Object(compact)
}

fn build_level_book(
    source: &Map<String, Value>,
    current_price: f64,
    kline_derived: &Value,
) -> Value {
    let mut by_window = Map::new();
    for tf in SCAN_WINDOWS {
        let atr14 = kline_derived
            .get(*tf)
            .and_then(Value::as_object)
            .and_then(|window| window.get("atr14"))
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let levels = collect_levels_for_tf(source, tf);
        let clusters = cluster_levels(
            levels,
            bin_size_for_tf(tf, current_price, atr14),
            current_price,
        );
        let support = clusters
            .iter()
            .filter(|cluster| cluster.price < current_price)
            .take(2)
            .map(level_cluster_to_value)
            .collect::<Vec<_>>();
        let resistance = clusters
            .iter()
            .filter(|cluster| cluster.price > current_price)
            .take(2)
            .map(level_cluster_to_value)
            .collect::<Vec<_>>();
        by_window.insert(
            (*tf).to_string(),
            json!({
                "support": support,
                "resistance": resistance,
            }),
        );
    }
    Value::Object(by_window)
}

fn bin_size_for_tf(tf: &str, current_price: f64, atr14: f64) -> f64 {
    let base = match tf {
        "15m" => current_price * 0.002,
        "4h" => current_price * 0.004,
        _ => current_price * 0.008,
    };
    let atr_component = if atr14 > 0.0 { atr14 * 0.3 } else { 0.0 };
    base.max(atr_component).max(0.1)
}

fn collect_levels_for_tf(source: &Map<String, Value>, tf: &str) -> Vec<LevelMember> {
    let mut levels = Vec::new();

    if let Some(pvs_window) = extract_indicator_payload(source, "price_volume_structure")
        .and_then(|payload| payload.get("by_window"))
        .and_then(Value::as_object)
        .and_then(|windows| windows.get(tf))
        .and_then(Value::as_object)
    {
        extend_level(
            &mut levels,
            pvs_window.get("poc_price").and_then(Value::as_f64),
            1.5,
            "pvs_poc",
        );
        extend_level(
            &mut levels,
            pvs_window.get("vah").and_then(Value::as_f64),
            1.2,
            "pvs_vah",
        );
        extend_level(
            &mut levels,
            pvs_window.get("val").and_then(Value::as_f64),
            1.2,
            "pvs_val",
        );
        extend_level_list(&mut levels, pvs_window.get("hvn_levels"), 1.3, "pvs_hvn");
    }

    if let Some(tpo_payload) = extract_indicator_payload(source, "tpo_market_profile") {
        if let Some(session) = tpo_payload
            .get("by_session")
            .and_then(Value::as_object)
            .and_then(|sessions| sessions.get(tf))
            .and_then(Value::as_object)
        {
            extend_level(
                &mut levels,
                session.get("tpo_poc").and_then(Value::as_f64),
                1.5,
                "tpo_poc",
            );
            extend_level(
                &mut levels,
                session.get("tpo_vah").and_then(Value::as_f64),
                1.2,
                "tpo_vah",
            );
            extend_level(
                &mut levels,
                session.get("tpo_val").and_then(Value::as_f64),
                1.2,
                "tpo_val",
            );
        } else {
            extend_level(
                &mut levels,
                tpo_payload.get("tpo_poc").and_then(Value::as_f64),
                1.5,
                "tpo_poc",
            );
            extend_level(
                &mut levels,
                tpo_payload.get("tpo_vah").and_then(Value::as_f64),
                1.2,
                "tpo_vah",
            );
            extend_level(
                &mut levels,
                tpo_payload.get("tpo_val").and_then(Value::as_f64),
                1.2,
                "tpo_val",
            );
        }
    }

    if let Some(rvwap_window) = extract_indicator_payload(source, "rvwap_sigma_bands")
        .and_then(|payload| payload.get("by_window"))
        .and_then(Value::as_object)
        .and_then(|windows| windows.get(tf))
        .and_then(Value::as_object)
    {
        extend_level(
            &mut levels,
            rvwap_window
                .get("rvwap_band_plus_1")
                .and_then(Value::as_f64),
            1.0,
            "rvwap_plus1",
        );
        extend_level(
            &mut levels,
            rvwap_window
                .get("rvwap_band_minus_1")
                .and_then(Value::as_f64),
            1.0,
            "rvwap_minus1",
        );
        extend_level(
            &mut levels,
            rvwap_window
                .get("rvwap_band_plus_2")
                .and_then(Value::as_f64),
            0.7,
            "rvwap_plus2",
        );
        extend_level(
            &mut levels,
            rvwap_window
                .get("rvwap_band_minus_2")
                .and_then(Value::as_f64),
            0.7,
            "rvwap_minus2",
        );
    }

    if let Some(fvg_window) = extract_indicator_payload(source, "fvg")
        .and_then(|payload| payload.get("by_window"))
        .and_then(Value::as_object)
        .and_then(|windows| windows.get(tf))
        .and_then(Value::as_object)
    {
        for label in ["nearest_bull_fvg", "nearest_bear_fvg"] {
            if let Some(fvg) = fvg_window.get(label).and_then(Value::as_object) {
                extend_level(
                    &mut levels,
                    fvg.get("upper")
                        .or_else(|| fvg.get("fvg_top"))
                        .and_then(Value::as_f64),
                    0.9,
                    "fvg_edge",
                );
                extend_level(
                    &mut levels,
                    fvg.get("lower")
                        .or_else(|| fvg.get("fvg_bottom"))
                        .and_then(Value::as_f64),
                    0.9,
                    "fvg_edge",
                );
            }
        }
    }

    if let Some(orderbook_payload) = extract_indicator_payload(source, "orderbook_depth") {
        let liquidity_walls = build_liquidity_walls(
            orderbook_payload
                .get("levels")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or(&[]),
            orderbook_payload
                .get("microprice_fut")
                .and_then(Value::as_f64),
        );
        if let Some(walls) = liquidity_walls.as_object() {
            extend_nearest_orderbook_walls(&mut levels, walls.get("bid_walls"), 0.8, "ob_bid_wall");
            extend_nearest_orderbook_walls(&mut levels, walls.get("ask_walls"), 0.8, "ob_ask_wall");
        }
    }

    levels
}

fn extend_level(
    levels: &mut Vec<LevelMember>,
    price: Option<f64>,
    weight: f64,
    source: &'static str,
) {
    if let Some(price) = price.filter(|price| price.is_finite()) {
        levels.push(LevelMember {
            price,
            weight,
            source,
        });
    }
}

fn extend_level_list(
    levels: &mut Vec<LevelMember>,
    values: Option<&Value>,
    weight: f64,
    source: &'static str,
) {
    let Some(values) = values else {
        return;
    };
    match values {
        Value::Array(items) => {
            for item in items {
                if let Some(price) = item.as_f64() {
                    extend_level(levels, Some(price), weight, source);
                } else if let Some(object) = item.as_object() {
                    extend_level(
                        levels,
                        object
                            .get("price_level")
                            .or_else(|| object.get("price"))
                            .and_then(Value::as_f64),
                        weight,
                        source,
                    );
                }
            }
        }
        Value::Object(object) => {
            extend_level(
                levels,
                object
                    .get("price_level")
                    .or_else(|| object.get("price"))
                    .and_then(Value::as_f64),
                weight,
                source,
            );
        }
        _ => {}
    }
}

fn extend_nearest_orderbook_walls(
    levels: &mut Vec<LevelMember>,
    values: Option<&Value>,
    weight: f64,
    source: &'static str,
) {
    let Some(items) = values.and_then(Value::as_array) else {
        return;
    };
    let mut ranked = items
        .iter()
        .filter_map(Value::as_object)
        .map(|item| {
            (
                item.get("distance_pct")
                    .and_then(Value::as_f64)
                    .map(|distance| distance.abs())
                    .unwrap_or(f64::INFINITY),
                item,
            )
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| left.0.partial_cmp(&right.0).unwrap_or(Ordering::Equal));
    for (_, item) in ranked.into_iter().take(2) {
        extend_level(
            levels,
            item.get("price_level")
                .or_else(|| item.get("price"))
                .and_then(Value::as_f64),
            weight,
            source,
        );
    }
}

fn cluster_levels(
    levels: Vec<LevelMember>,
    bin_size: f64,
    current_price: f64,
) -> Vec<LevelCluster> {
    let mut bins: BTreeMap<i64, Vec<LevelMember>> = BTreeMap::new();
    for level in levels {
        let index = (level.price / bin_size).floor() as i64;
        bins.entry(index).or_default().push(level);
    }
    let mut clusters = bins
        .into_values()
        .filter_map(|members| {
            let representative = members.iter().min_by(|left, right| {
                (left.price - current_price)
                    .abs()
                    .partial_cmp(&(right.price - current_price).abs())
                    .unwrap_or(Ordering::Equal)
            })?;
            let mut sources = members
                .iter()
                .map(|member| member.source)
                .collect::<Vec<_>>();
            sources.sort_unstable();
            sources.dedup();
            Some(LevelCluster {
                price: representative.price,
                strength: round2(members.iter().map(|member| member.weight).sum()),
                sources,
            })
        })
        .collect::<Vec<_>>();
    clusters.sort_by(|left, right| {
        (left.price - current_price)
            .abs()
            .partial_cmp(&(right.price - current_price).abs())
            .unwrap_or(Ordering::Equal)
    });
    clusters
}

fn level_cluster_to_value(cluster: &LevelCluster) -> Value {
    json!({
        "price": cluster.price,
        "strength": cluster.strength,
        "sources": cluster.sources,
    })
}

fn extract_level_anchor(
    structure_range: &Map<String, Value>,
    side: &str,
    index: usize,
) -> Option<LevelAnchor> {
    structure_range
        .get(side)
        .and_then(Value::as_array)
        .and_then(|items| items.get(index))
        .and_then(Value::as_object)
        .and_then(level_anchor_from_value)
}

fn level_anchor_from_value(value: &Map<String, Value>) -> Option<LevelAnchor> {
    Some(LevelAnchor {
        price: value.get("price").and_then(Value::as_f64)?,
        strength: value.get("strength").and_then(Value::as_f64).unwrap_or(0.0),
        source_count: value
            .get("sources")
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or(0),
    })
}

#[allow(clippy::too_many_arguments)]
fn build_display_range(
    tf: &str,
    current_price: Option<f64>,
    atr14: f64,
    primary_support: Option<f64>,
    primary_resistance: Option<f64>,
    primary_support_anchor: Option<&LevelAnchor>,
    secondary_support_anchor: Option<&LevelAnchor>,
    primary_resistance_anchor: Option<&LevelAnchor>,
    secondary_resistance_anchor: Option<&LevelAnchor>,
    breakout_above: Option<f64>,
    breakdown_below: Option<f64>,
    evidence: BiasEvidence,
) -> (Option<f64>, Option<f64>, &'static str) {
    let mut display_support = primary_support;
    let mut display_resistance = primary_resistance;
    let mut mode = "primary";

    let Some(current_price) = current_price else {
        return (display_support, display_resistance, mode);
    };
    if atr14 <= 0.0 {
        return (display_support, display_resistance, mode);
    }
    let Some(primary_support_price) = primary_support else {
        return (display_support, display_resistance, mode);
    };
    let Some(primary_resistance_price) = primary_resistance else {
        return (display_support, display_resistance, mode);
    };

    if (primary_resistance_price - primary_support_price)
        >= narrow_display_range_threshold(tf) * atr14
    {
        return (display_support, display_resistance, mode);
    }

    let bullish_context =
        evidence.evidence_balance_score > 0.15 && evidence.compression_score < 0.75;
    let bearish_context =
        evidence.evidence_balance_score < -0.15 && evidence.compression_score < 0.75;

    if bullish_context {
        if let (Some(primary_anchor), Some(candidate_anchor)) =
            (primary_resistance_anchor, secondary_resistance_anchor)
        {
            if qualifies_expanded_anchor(
                current_price,
                atr14,
                primary_anchor,
                candidate_anchor,
                breakout_above,
            ) {
                display_resistance = Some(candidate_anchor.price);
                mode = "expanded_resistance";
            }
        }
    }

    if bearish_context {
        if let (Some(primary_anchor), Some(candidate_anchor)) =
            (primary_support_anchor, secondary_support_anchor)
        {
            if qualifies_expanded_anchor(
                current_price,
                atr14,
                primary_anchor,
                candidate_anchor,
                breakdown_below,
            ) {
                display_support = Some(candidate_anchor.price);
                mode = "expanded_support";
            }
        }
    }

    if mode == "primary"
        && (primary_resistance_price - primary_support_price)
            < ultra_narrow_display_range_threshold() * atr14
    {
        let current_span = primary_resistance_price - primary_support_price;
        let mut best_span = current_span;

        if let (Some(primary_anchor), Some(candidate_anchor)) =
            (primary_resistance_anchor, secondary_resistance_anchor)
        {
            if qualifies_structural_fallback_anchor(
                current_price,
                atr14,
                primary_anchor,
                candidate_anchor,
            ) {
                let candidate_span = candidate_anchor.price - primary_support_price;
                if candidate_span > best_span {
                    display_resistance = Some(candidate_anchor.price);
                    mode = "expanded_resistance";
                    best_span = candidate_span;
                }
            }
        }

        if let (Some(primary_anchor), Some(candidate_anchor)) =
            (primary_support_anchor, secondary_support_anchor)
        {
            if qualifies_structural_fallback_anchor(
                current_price,
                atr14,
                primary_anchor,
                candidate_anchor,
            ) {
                let candidate_span = primary_resistance_price - candidate_anchor.price;
                if candidate_span > best_span {
                    display_support = Some(candidate_anchor.price);
                    mode = "expanded_support";
                }
            }
        }
    }

    (display_support, display_resistance, mode)
}

fn narrow_display_range_threshold(tf: &str) -> f64 {
    match tf {
        "15m" => 0.5,
        "4h" => 0.4,
        "1d" => 0.3,
        _ => 0.4,
    }
}

fn ultra_narrow_display_range_threshold() -> f64 {
    0.25
}

fn qualifies_expanded_anchor(
    current_price: f64,
    atr14: f64,
    primary_anchor: &LevelAnchor,
    candidate_anchor: &LevelAnchor,
    directional_target: Option<f64>,
) -> bool {
    if (candidate_anchor.price - current_price).abs() > atr14 {
        return false;
    }
    if candidate_anchor.strength >= primary_anchor.strength {
        return true;
    }
    if candidate_anchor.source_count > primary_anchor.source_count {
        return true;
    }
    directional_target
        .map(|target| (candidate_anchor.price - target).abs() <= 0.25)
        .unwrap_or(false)
}

fn qualifies_structural_fallback_anchor(
    current_price: f64,
    atr14: f64,
    primary_anchor: &LevelAnchor,
    candidate_anchor: &LevelAnchor,
) -> bool {
    qualifies_expanded_anchor(current_price, atr14, primary_anchor, candidate_anchor, None)
}

fn resolve_selected_anchor(
    selected_price: Option<f64>,
    primary_anchor: Option<&LevelAnchor>,
    secondary_anchor: Option<&LevelAnchor>,
) -> Option<LevelAnchor> {
    let selected_price = selected_price?;
    for anchor in [primary_anchor, secondary_anchor].into_iter().flatten() {
        if (anchor.price - selected_price).abs() <= 1e-6 {
            return Some(anchor.clone());
        }
    }
    None
}

fn build_display_range_value(
    support: Option<f64>,
    resistance: Option<f64>,
    mode: &str,
    range_role: &str,
    current_price: Option<f64>,
    atr14: f64,
    support_anchor: Option<&LevelAnchor>,
    resistance_anchor: Option<&LevelAnchor>,
) -> Value {
    let width_atr = match (support, resistance, atr14 > 0.0) {
        (Some(support), Some(resistance), true) if resistance > support => {
            Some(round3((resistance - support) / atr14))
        }
        _ => None,
    };
    let position_in_range_pct = match (current_price, support, resistance) {
        (Some(current_price), Some(support), Some(resistance)) if resistance > support => Some(
            round3(((current_price - support) / (resistance - support)).clamp(0.0, 1.0)),
        ),
        _ => None,
    };
    let quality = classify_display_range_quality(width_atr, support_anchor, resistance_anchor);
    let current_price_location = range_current_price_location(current_price, support, resistance);
    json!({
        "support": support,
        "resistance": resistance,
        "mode": mode,
        "range_role": range_role,
        "range_width_atr": width_atr,
        "position_in_range_pct": position_in_range_pct,
        "current_price_location": current_price_location,
        "quality": quality,
        "support_strength": support_anchor.map(|anchor| round2(anchor.strength)),
        "support_source_count": support_anchor.map(|anchor| anchor.source_count),
        "resistance_strength": resistance_anchor.map(|anchor| round2(anchor.strength)),
        "resistance_source_count": resistance_anchor.map(|anchor| anchor.source_count),
    })
}

fn build_session_value_area_range_value(
    support: Option<f64>,
    resistance: Option<f64>,
    current_price: Option<f64>,
    atr14: f64,
    source_window: Option<&str>,
    source_kind: Option<&str>,
) -> Value {
    let width_atr = match (support, resistance, atr14 > 0.0) {
        (Some(support), Some(resistance), true) if resistance > support => {
            Some(round3((resistance - support) / atr14))
        }
        _ => None,
    };
    let position_in_range_pct = match (current_price, support, resistance) {
        (Some(current_price), Some(support), Some(resistance)) if resistance > support => Some(
            round3(((current_price - support) / (resistance - support)).clamp(0.0, 1.0)),
        ),
        _ => None,
    };
    let quality = match width_atr {
        Some(width) if width < 0.15 => "compressed",
        Some(_) => "clean",
        None => "fragile",
    };
    let current_price_location = range_current_price_location(current_price, support, resistance);
    json!({
        "support": support,
        "resistance": resistance,
        "mode": "session_value_area",
        "range_role": "context",
        "range_width_atr": width_atr,
        "position_in_range_pct": position_in_range_pct,
        "current_price_location": current_price_location,
        "quality": quality,
        "source_window": source_window,
        "source_kind": source_kind,
    })
}

fn range_current_price_location(
    current_price: Option<f64>,
    support: Option<f64>,
    resistance: Option<f64>,
) -> Value {
    match (current_price, support, resistance) {
        (Some(current_price), Some(support), Some(resistance)) if resistance > support => {
            if current_price < support {
                Value::String("below".to_string())
            } else if current_price > resistance {
                Value::String("above".to_string())
            } else {
                Value::String("inside".to_string())
            }
        }
        _ => Value::Null,
    }
}

fn classify_display_range_quality(
    width_atr: Option<f64>,
    support_anchor: Option<&LevelAnchor>,
    resistance_anchor: Option<&LevelAnchor>,
) -> &'static str {
    let Some(width_atr) = width_atr else {
        return "fragile";
    };
    if width_atr < 0.25 {
        return "compressed";
    }
    if support_anchor
        .map(|anchor| anchor.source_count <= 1 || anchor.strength < 1.0)
        .unwrap_or(true)
        || resistance_anchor
            .map(|anchor| anchor.source_count <= 1 || anchor.strength < 1.0)
            .unwrap_or(true)
    {
        return "fragile";
    }
    "clean"
}

fn weighted_average_scores(values: &[(f64, f64)]) -> f64 {
    let mut weighted_sum = 0.0;
    let mut total_weight = 0.0;
    for (value, weight) in values {
        if value.is_finite() && *weight > 0.0 {
            weighted_sum += value * weight;
            total_weight += weight;
        }
    }
    if total_weight > 0.0 {
        weighted_sum / total_weight
    } else {
        0.0
    }
}

fn directional_string_score(value: &str) -> f64 {
    match value {
        "bull" | "bullish" | "Bullish" | "above_vah" => 1.0,
        "bear" | "bearish" | "Bearish" | "below_val" => -1.0,
        _ => 0.0,
    }
}

fn normalized_metric_score(value: Option<f64>, scale: f64) -> f64 {
    value
        .map(|value| (value / scale).clamp(-1.0, 1.0))
        .unwrap_or(0.0)
}

fn normalized_event_score(summary: Option<&Map<String, Value>>, tf: &str) -> f64 {
    let net_score = summary
        .and_then(|summary| summary.get("net_score"))
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    (net_score / event_score_scale(tf)).clamp(-1.0, 1.0)
}

fn event_score_scale(tf: &str) -> f64 {
    match tf {
        "15m" => 1.0,
        "4h" => 1.5,
        _ => 2.5,
    }
}

fn range_position_score(
    current_price: Option<f64>,
    support: Option<f64>,
    resistance: Option<f64>,
) -> f64 {
    match (current_price, support, resistance) {
        (Some(current_price), Some(support), Some(resistance)) if resistance > support => {
            (((current_price - support) / (resistance - support)).clamp(0.0, 1.0) - 0.5) * 2.0
        }
        _ => 0.0,
    }
}

fn anchor_strength_balance(
    support_anchor: Option<&LevelAnchor>,
    resistance_anchor: Option<&LevelAnchor>,
) -> f64 {
    match (support_anchor, resistance_anchor) {
        (Some(support_anchor), Some(resistance_anchor)) => {
            let total = support_anchor.strength + resistance_anchor.strength;
            if total > 0.0 {
                ((support_anchor.strength - resistance_anchor.strength) / total).clamp(-1.0, 1.0)
            } else {
                0.0
            }
        }
        _ => 0.0,
    }
}

fn timeframe_component_weights(tf: &str) -> (f64, f64, f64) {
    match tf {
        "15m" => (0.2, 0.45, 0.35),
        "4h" => (0.35, 0.30, 0.35),
        _ => (0.45, 0.25, 0.30),
    }
}

fn score_from_position(value: Option<&Value>) -> f64 {
    value
        .and_then(Value::as_str)
        .map(directional_string_score)
        .unwrap_or(0.0)
}

fn compute_bias_evidence(
    tf: &str,
    sideways_votes: usize,
    trend: &Map<String, Value>,
    flow: &Map<String, Value>,
    current_price: Option<f64>,
    display_support: Option<f64>,
    display_resistance: Option<f64>,
    display_support_anchor: Option<&LevelAnchor>,
    display_resistance_anchor: Option<&LevelAnchor>,
) -> BiasEvidence {
    let trend_component = weighted_average_scores(&[
        (
            directional_string_score(
                trend
                    .get("ema_regime")
                    .and_then(Value::as_str)
                    .unwrap_or("neutral"),
            ),
            0.4,
        ),
        (
            normalized_metric_score(
                trend
                    .get("close_vs_ema21_pct")
                    .and_then(Value::as_f64)
                    .or_else(|| trend.get("close_vs_ema100_htf_pct").and_then(Value::as_f64)),
                2.0,
            ),
            0.3,
        ),
        (score_from_position(trend.get("value_area_position")), 0.2),
        (
            normalized_metric_score(trend.get("close_vs_vah_pct").and_then(Value::as_f64), 1.5),
            0.1,
        ),
    ]);

    let flow_component = weighted_average_scores(&[
        (
            weighted_average_scores(&[
                (
                    directional_string_score(
                        flow.get("cvd")
                            .and_then(Value::as_object)
                            .and_then(|value| value.get("bias"))
                            .and_then(Value::as_str)
                            .unwrap_or("neutral"),
                    ),
                    0.5,
                ),
                (
                    flow.get("cvd")
                        .and_then(Value::as_object)
                        .and_then(|value| value.get("delta_sign"))
                        .and_then(Value::as_i64)
                        .unwrap_or(0) as f64,
                    0.25,
                ),
                (
                    flow.get("cvd")
                        .and_then(Value::as_object)
                        .and_then(|value| value.get("slope_sign"))
                        .and_then(Value::as_i64)
                        .unwrap_or(0) as f64,
                    0.15,
                ),
                (
                    normalized_metric_score(
                        flow.get("cvd")
                            .and_then(Value::as_object)
                            .and_then(|value| value.get("relative_delta"))
                            .and_then(Value::as_f64),
                        0.15,
                    ),
                    0.10,
                ),
            ]),
            0.30,
        ),
        (
            normalized_event_score(flow.get("absorption").and_then(Value::as_object), tf),
            0.15,
        ),
        (
            normalized_event_score(flow.get("initiation").and_then(Value::as_object), tf),
            0.10,
        ),
        (
            normalized_event_score(flow.get("buying_exhaustion").and_then(Value::as_object), tf),
            0.10,
        ),
        (
            normalized_event_score(
                flow.get("selling_exhaustion").and_then(Value::as_object),
                tf,
            ),
            0.10,
        ),
        (
            normalized_event_score(flow.get("divergence").and_then(Value::as_object), tf),
            0.10,
        ),
        (
            directional_string_score(
                flow.get("whale")
                    .and_then(Value::as_object)
                    .and_then(|value| value.get("bias"))
                    .and_then(Value::as_str)
                    .unwrap_or("neutral"),
            ),
            0.10,
        ),
        (
            weighted_average_scores(&[
                (
                    flow.get("funding")
                        .and_then(Value::as_object)
                        .and_then(|value| value.get("rate_sign"))
                        .and_then(Value::as_i64)
                        .unwrap_or(0) as f64,
                    0.6,
                ),
                (
                    flow.get("funding")
                        .and_then(Value::as_object)
                        .and_then(|value| value.get("trend_sign"))
                        .and_then(Value::as_i64)
                        .unwrap_or(0) as f64,
                    0.4,
                ),
            ]),
            0.05,
        ),
    ]);

    let structure_component = weighted_average_scores(&[
        (
            range_position_score(current_price, display_support, display_resistance),
            0.65,
        ),
        (
            anchor_strength_balance(display_support_anchor, display_resistance_anchor),
            0.35,
        ),
    ]);

    let (trend_weight, flow_weight, structure_weight) = timeframe_component_weights(tf);
    let evidence_balance = weighted_average_scores(&[
        (trend_component, trend_weight),
        (flow_component, flow_weight),
        (structure_component, structure_weight),
    ]);
    let conflict_score = round2(compute_conflict_score(
        trend_component,
        flow_component,
        structure_component,
    ));
    let compression_score = round2((sideways_votes as f64 / 4.0).clamp(0.0, 1.0));

    BiasEvidence {
        trend_balance_score: round3(trend_component),
        flow_balance_score: round3(flow_component),
        structure_balance_score: round3(structure_component),
        evidence_balance_score: round3(evidence_balance),
        conflict_score,
        compression_score,
    }
}

fn compute_conflict_score(
    trend_component: f64,
    flow_component: f64,
    structure_component: f64,
) -> f64 {
    let components = [trend_component, flow_component, structure_component];
    let max_component = components.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let min_component = components.iter().copied().fold(f64::INFINITY, f64::min);
    let positive = components.iter().filter(|value| **value > 0.15).count();
    let negative = components.iter().filter(|value| **value < -0.15).count();
    let disagreement = if positive > 0 && negative > 0 {
        1.0
    } else if positive + negative <= 1 {
        0.25
    } else {
        0.0
    };
    let spread = ((max_component - min_component) / 2.0).clamp(0.0, 1.0);
    (spread * 0.6 + disagreement * 0.4).clamp(0.0, 1.0)
}

fn upside_pressure(
    current_price: Option<f64>,
    nearest_resistance: Option<f64>,
    atr14: f64,
    evidence: BiasEvidence,
) -> f64 {
    let proximity = current_price
        .zip(nearest_resistance)
        .filter(|(_, resistance)| atr14 > 0.0 && *resistance >= current_price.unwrap_or_default())
        .map(|(price, resistance)| {
            let distance = (resistance - price) / atr14;
            (1.0 - (distance / 1.5)).clamp(0.0, 1.0)
        })
        .unwrap_or(0.0);
    round2(weighted_average_scores(&[
        (
            ((evidence.evidence_balance_score).clamp(-1.0, 1.0) + 1.0) / 2.0,
            0.45,
        ),
        (
            ((evidence.flow_balance_score).clamp(-1.0, 1.0) + 1.0) / 2.0,
            0.25,
        ),
        (proximity, 0.20),
        (evidence.compression_score * 0.25, 0.10),
    ]))
}

fn downside_pressure(
    current_price: Option<f64>,
    nearest_support: Option<f64>,
    atr14: f64,
    evidence: BiasEvidence,
) -> f64 {
    let proximity = current_price
        .zip(nearest_support)
        .filter(|(_, support)| atr14 > 0.0 && *support <= current_price.unwrap_or_default())
        .map(|(price, support)| {
            let distance = (price - support) / atr14;
            (1.0 - (distance / 1.5)).clamp(0.0, 1.0)
        })
        .unwrap_or(0.0);
    round2(weighted_average_scores(&[
        (
            ((-evidence.evidence_balance_score).clamp(-1.0, 1.0) + 1.0) / 2.0,
            0.45,
        ),
        (
            ((-evidence.flow_balance_score).clamp(-1.0, 1.0) + 1.0) / 2.0,
            0.25,
        ),
        (proximity, 0.20),
        (evidence.compression_score * 0.25, 0.10),
    ]))
}

fn evidence_bucket(score: f64) -> &'static str {
    if score > 0.2 {
        "bullish"
    } else if score < -0.2 {
        "bearish"
    } else {
        "neutral"
    }
}

#[allow(clippy::too_many_arguments)]
fn build_by_timeframe(
    source: &Map<String, Value>,
    kline_derived: &Value,
    level_book: &Value,
    cvd_summary: &Value,
    absorption_summary: &Value,
    buying_exhaustion_summary: &Value,
    selling_exhaustion_summary: &Value,
    divergence_summary: &Value,
    initiation_summary: &Value,
) -> Map<String, Value> {
    let mut result = Map::new();
    let ema_payload = extract_indicator_payload(source, "ema_trend_regime");
    let whale_windows = extract_indicator_payload(source, "whale_trades")
        .and_then(|payload| payload.get("by_window"))
        .and_then(Value::as_object);
    let vpin_windows = extract_indicator_payload(source, "vpin")
        .and_then(|payload| payload.get("by_window"))
        .and_then(Value::as_object);
    let funding_windows = extract_indicator_payload(source, "funding_rate")
        .and_then(|payload| payload.get("by_window"))
        .and_then(Value::as_object);

    for tf in SCAN_WINDOWS {
        let kline = kline_derived
            .get(*tf)
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        let mut trend = Map::new();
        trend.insert(
            "ema_regime".to_string(),
            extract_ema_regime_current(ema_payload, tf),
        );
        if let Some(value) = kline.get("close_vs_ema21_pct").cloned() {
            if !value.is_null() {
                trend.insert("close_vs_ema21_pct".to_string(), value);
            }
        }
        if let Some(value) = kline.get("close_vs_ema100_htf_pct").cloned() {
            if !value.is_null() {
                trend.insert("close_vs_ema100_htf_pct".to_string(), value);
            }
        }
        if let Some(value) = kline.get("close_vs_vah_pct").cloned() {
            if !value.is_null() {
                trend.insert("close_vs_vah_pct".to_string(), value);
            }
        }
        if let Some(value) = kline.get("value_area_position").cloned() {
            if !value.is_null() {
                trend.insert("value_area_position".to_string(), value);
            }
        }
        let flow = json!({
            "cvd": cvd_summary
                .get("by_window")
                .and_then(Value::as_object)
                .and_then(|by_window| by_window.get(*tf))
                .cloned()
                .unwrap_or(Value::Null),
            "absorption": absorption_summary.get(*tf).cloned().unwrap_or(Value::Null),
            "buying_exhaustion": buying_exhaustion_summary.get(*tf).cloned().unwrap_or(Value::Null),
            "selling_exhaustion": selling_exhaustion_summary.get(*tf).cloned().unwrap_or(Value::Null),
            "divergence": divergence_summary.get(*tf).cloned().unwrap_or(Value::Null),
            "initiation": initiation_summary.get(*tf).cloned().unwrap_or(Value::Null),
            "whale": build_whale_summary(whale_windows.and_then(|windows| windows.get(*tf)).and_then(Value::as_object)),
            "vpin": build_vpin_summary(vpin_windows.and_then(|windows| windows.get(*tf)).and_then(Value::as_object)),
            "funding": build_funding_window_summary(
                funding_windows
                    .and_then(|windows| windows.get(*tf))
                    .and_then(Value::as_object)
            ),
        });
        result.insert(
            (*tf).to_string(),
            json!({
                "structure": {
                    "swing_high": kline.get("swing_high").cloned().unwrap_or(Value::Null),
                    "swing_low": kline.get("swing_low").cloned().unwrap_or(Value::Null),
                    "level_book": level_book.get(*tf).cloned().unwrap_or_else(|| json!({"support": [], "resistance": []})),
                    "session_value_area": build_session_value_area(source, tf),
                },
                "trend": Value::Object(trend),
                "flow": flow,
                "volatility": {
                    "atr14": kline.get("atr14").cloned().unwrap_or(Value::Null),
                    "atr14_pct": kline.get("atr14_pct").cloned().unwrap_or(Value::Null),
                }
            }),
        );
    }

    result
}

fn build_session_value_area(source: &Map<String, Value>, tf: &str) -> Value {
    if let Some((support, resistance, source_window, source_kind)) =
        extract_session_value_area(source, tf)
    {
        return json!({
            "support": support,
            "resistance": resistance,
            "source_window": source_window,
            "source_kind": source_kind,
        });
    }
    Value::Null
}

fn extract_session_value_area(
    source: &Map<String, Value>,
    tf: &str,
) -> Option<(f64, f64, String, &'static str)> {
    if let Some(tpo_payload) = extract_indicator_payload(source, "tpo_market_profile") {
        if let Some((support, resistance, source_window)) = tpo_value_area_bounds(tpo_payload, tf) {
            return Some((support, resistance, source_window, "tpo_value_area"));
        }
    }
    extract_indicator_payload(source, "price_volume_structure")
        .and_then(|payload| payload.get("by_window"))
        .and_then(Value::as_object)
        .and_then(|windows| windows.get(tf))
        .and_then(Value::as_object)
        .and_then(|window| {
            Some((
                window.get("val").and_then(Value::as_f64)?,
                window.get("vah").and_then(Value::as_f64)?,
                tf.to_string(),
                "pvs_value_area",
            ))
        })
}

fn tpo_value_area_bounds(payload: &Map<String, Value>, tf: &str) -> Option<(f64, f64, String)> {
    let top_level_bounds = payload
        .get("tpo_val")
        .and_then(Value::as_f64)
        .zip(payload.get("tpo_vah").and_then(Value::as_f64));
    let top_level_window = payload
        .get("session_window")
        .and_then(Value::as_str)
        .map(str::to_string);
    let by_session = payload.get("by_session").and_then(Value::as_object);
    let session_bounds = |session_tf: &str| {
        by_session
            .and_then(|sessions| sessions.get(session_tf))
            .and_then(Value::as_object)
            .and_then(|session| {
                Some((
                    session.get("tpo_val").and_then(Value::as_f64)?,
                    session.get("tpo_vah").and_then(Value::as_f64)?,
                    session_tf.to_string(),
                ))
            })
    };

    match tf {
        "15m" => top_level_bounds
            .map(|(support, resistance)| {
                (
                    support,
                    resistance,
                    top_level_window.unwrap_or_else(|| "active".to_string()),
                )
            })
            .or_else(|| session_bounds("4h")),
        "4h" | "1d" => session_bounds(tf).or_else(|| {
            if top_level_window.as_deref() == Some(tf) {
                top_level_bounds.map(|(support, resistance)| (support, resistance, tf.to_string()))
            } else {
                None
            }
        }),
        _ => None,
    }
}

fn extract_ema_regime_current(payload: Option<&Map<String, Value>>, tf: &str) -> Value {
    let series = payload
        .and_then(|payload| payload.get("ffill_series_by_output_window"))
        .and_then(Value::as_object)
        .and_then(|series| series.get("15m"))
        .and_then(Value::as_array);
    let normalized = series
        .map(|series| take_last_n(series, 20))
        .unwrap_or_default();
    let Some(latest) = normalized.first().and_then(Value::as_object) else {
        return Value::Null;
    };
    match tf {
        "15m" => latest.get("trend_regime").cloned().unwrap_or(Value::Null),
        "4h" | "1d" => latest
            .get("by_tf")
            .and_then(Value::as_object)
            .and_then(|by_tf| by_tf.get(tf))
            .and_then(Value::as_object)
            .and_then(|value| value.get("trend_regime"))
            .cloned()
            .unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

fn build_whale_summary(window: Option<&Map<String, Value>>) -> Value {
    let net_delta_notional = window
        .and_then(|window| window.get("fut_whale_delta_notional"))
        .and_then(Value::as_f64);
    json!({
        "net_delta_notional": net_delta_notional.map(round2),
        "spot_fut_gap": window
            .and_then(|window| window.get("xmk_whale_delta_notional_gap_s_minus_f"))
            .and_then(Value::as_f64)
            .map(round2),
        "bias": bias_from_sign(sign_from_option(net_delta_notional)),
    })
}

fn build_vpin_summary(window: Option<&Map<String, Value>>) -> Value {
    let vpin = window
        .and_then(|window| window.get("vpin_fut"))
        .and_then(Value::as_f64)
        .or_else(|| {
            window
                .and_then(|window| window.get("vpin_spot"))
                .and_then(Value::as_f64)
        });
    json!({
        "vpin": vpin.map(round3),
        "elevated": vpin.map(|value| value > 0.5).unwrap_or(false),
    })
}

fn build_funding_window_summary(window: Option<&Map<String, Value>>) -> Value {
    let current_rate = window
        .and_then(|window| window.get("funding_current"))
        .and_then(Value::as_f64)
        .or_else(|| {
            window
                .and_then(|window| window.get("funding_twa"))
                .and_then(Value::as_f64)
        });
    let trend_sign = window
        .and_then(|window| window.get("changes"))
        .and_then(Value::as_array)
        .and_then(|changes| {
            let latest = changes.last()?.as_object()?;
            let value = latest
                .get("funding_delta")
                .and_then(Value::as_f64)
                .or_else(|| {
                    let new = latest.get("funding_new").and_then(Value::as_f64)?;
                    let prev = latest.get("funding_prev").and_then(Value::as_f64)?;
                    Some(new - prev)
                })?;
            Some(sign_i8(value))
        })
        .unwrap_or(0);
    json!({
        "current_rate": current_rate.map(round6),
        "rate_sign": sign_from_option(current_rate),
        "trend_sign": trend_sign,
    })
}

fn round6(value: f64) -> f64 {
    round_to_decimals(value, 6)
}

fn build_balance_scores_value(evidence: BiasEvidence) -> Value {
    json!({
        "trend_balance_score": evidence.trend_balance_score,
        "flow_balance_score": evidence.flow_balance_score,
        "structure_balance_score": evidence.structure_balance_score,
        "evidence_balance_score": evidence.evidence_balance_score,
        "conflict_score": evidence.conflict_score,
        "compression_score": evidence.compression_score,
    })
}

fn build_timeframe_evidence(
    _symbol: &str,
    _ts_bucket: &str,
    current_price: Option<f64>,
    by_timeframe: &Map<String, Value>,
    history: &BTreeMap<String, Vec<EvidenceSnapshot>>,
) -> Map<String, Value> {
    let mut result = Map::new();
    for tf in SCAN_WINDOWS {
        let tf_data = by_timeframe
            .get(*tf)
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        let trend = tf_data
            .get("trend")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        let flow = tf_data
            .get("flow")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        let structure = tf_data
            .get("structure")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        let volatility = tf_data
            .get("volatility")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();

        let ema_regime = trend
            .get("ema_regime")
            .and_then(Value::as_str)
            .unwrap_or("ranging")
            .to_string();
        let cvd_bias = flow
            .get("cvd")
            .and_then(Value::as_object)
            .and_then(|value| value.get("bias"))
            .and_then(Value::as_str)
            .unwrap_or("neutral")
            .to_string();
        let absorption_bias = flow
            .get("absorption")
            .and_then(Value::as_object)
            .and_then(|value| value.get("bias"))
            .and_then(Value::as_str)
            .unwrap_or("neutral")
            .to_string();
        let whale_bias = flow
            .get("whale")
            .and_then(Value::as_object)
            .and_then(|value| value.get("bias"))
            .and_then(Value::as_str)
            .unwrap_or("neutral")
            .to_string();
        let value_area_position = trend
            .get("value_area_position")
            .and_then(Value::as_str)
            .unwrap_or("inside_va")
            .to_string();
        let structure_range = structure
            .get("level_book")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        let session_value_area = structure
            .get("session_value_area")
            .and_then(Value::as_object)
            .cloned();
        let primary_support_anchor = extract_level_anchor(&structure_range, "support", 0);
        let secondary_support_anchor = extract_level_anchor(&structure_range, "support", 1);
        let primary_resistance_anchor = extract_level_anchor(&structure_range, "resistance", 0);
        let secondary_resistance_anchor = extract_level_anchor(&structure_range, "resistance", 1);
        let primary_support = primary_support_anchor
            .as_ref()
            .map(|anchor| anchor.price)
            .or_else(|| structure.get("swing_low").and_then(Value::as_f64));
        let primary_resistance = primary_resistance_anchor
            .as_ref()
            .map(|anchor| anchor.price)
            .or_else(|| structure.get("swing_high").and_then(Value::as_f64));
        let atr14 = volatility
            .get("atr14")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let sideways_votes = [
            (ema_regime == "ranging") as usize,
            (value_area_position == "inside_va") as usize,
            (cvd_bias == "neutral") as usize,
            match (primary_support, primary_resistance) {
                (Some(support), Some(resistance)) if atr14 > 0.0 => {
                    ((resistance - support) < 2.0 * atr14) as usize
                }
                _ => 0,
            },
        ]
        .iter()
        .sum::<usize>();
        let breakout_above = structure.get("swing_high").and_then(Value::as_f64);
        let breakdown_below = structure.get("swing_low").and_then(Value::as_f64);
        let preliminary_evidence = compute_bias_evidence(
            tf,
            sideways_votes,
            &trend,
            &flow,
            current_price,
            primary_support,
            primary_resistance,
            primary_support_anchor.as_ref(),
            primary_resistance_anchor.as_ref(),
        );
        let (display_support, display_resistance, display_range_mode) = build_display_range(
            tf,
            current_price,
            atr14,
            primary_support,
            primary_resistance,
            primary_support_anchor.as_ref(),
            secondary_support_anchor.as_ref(),
            primary_resistance_anchor.as_ref(),
            secondary_resistance_anchor.as_ref(),
            breakout_above,
            breakdown_below,
            preliminary_evidence,
        );
        let display_support_anchor = resolve_selected_anchor(
            display_support,
            primary_support_anchor.as_ref(),
            secondary_support_anchor.as_ref(),
        );
        let display_resistance_anchor = resolve_selected_anchor(
            display_resistance,
            primary_resistance_anchor.as_ref(),
            secondary_resistance_anchor.as_ref(),
        );
        let display_range = build_display_range_value(
            display_support,
            display_resistance,
            display_range_mode,
            "default_bracket",
            current_price,
            atr14,
            display_support_anchor.as_ref(),
            display_resistance_anchor.as_ref(),
        );
        let micro_range = build_display_range_value(
            primary_support,
            primary_resistance,
            "micro_bracket",
            "compression",
            current_price,
            atr14,
            primary_support_anchor.as_ref(),
            primary_resistance_anchor.as_ref(),
        );
        let session_value_area_range = build_session_value_area_range_value(
            session_value_area
                .as_ref()
                .and_then(|range| range.get("support"))
                .and_then(Value::as_f64),
            session_value_area
                .as_ref()
                .and_then(|range| range.get("resistance"))
                .and_then(Value::as_f64),
            current_price,
            atr14,
            session_value_area
                .as_ref()
                .and_then(|range| range.get("source_window"))
                .and_then(Value::as_str),
            session_value_area
                .as_ref()
                .and_then(|range| range.get("source_kind"))
                .and_then(Value::as_str),
        );
        let evidence = compute_bias_evidence(
            tf,
            sideways_votes,
            &trend,
            &flow,
            current_price,
            display_support,
            display_resistance,
            display_support_anchor.as_ref(),
            display_resistance_anchor.as_ref(),
        );
        let mut evidence_entry = Map::new();
        evidence_entry.insert(
            "primary_support".to_string(),
            primary_support.map(Value::from).unwrap_or(Value::Null),
        );
        evidence_entry.insert(
            "primary_resistance".to_string(),
            primary_resistance.map(Value::from).unwrap_or(Value::Null),
        );
        evidence_entry.insert("display_range".to_string(), display_range.clone());
        evidence_entry.insert(
            "ranges".to_string(),
            json!({
                "micro_range": micro_range,
                "local_structure_range": display_range,
                "session_value_area_range": session_value_area_range,
            }),
        );
        evidence_entry.insert(
            "breakout_above".to_string(),
            breakout_above.map(Value::from).unwrap_or(Value::Null),
        );
        evidence_entry.insert(
            "breakdown_below".to_string(),
            breakdown_below.map(Value::from).unwrap_or(Value::Null),
        );
        evidence_entry.insert(
            "balance_scores".to_string(),
            build_balance_scores_value(evidence),
        );
        evidence_entry.insert(
            "risk_opportunity".to_string(),
            json!({
                "upside_opportunity_score": upside_pressure(current_price, display_resistance, atr14, evidence),
                "downside_risk_score": downside_pressure(current_price, display_support, atr14, evidence),
            }),
        );
        evidence_entry.insert(
            "clearance_atr".to_string(),
            json!({
                "to_support": current_price
                    .zip(display_support)
                    .filter(|(price, support)| atr14 > 0.0 && *support <= *price)
                    .map(|(price, support)| round2((price - support) / atr14))
                    .map(Value::from)
                    .unwrap_or(Value::Null),
                "to_resistance": current_price
                    .zip(display_resistance)
                    .filter(|(price, resistance)| atr14 > 0.0 && *resistance >= *price)
                    .map(|(price, resistance)| round2((resistance - price) / atr14))
                    .map(Value::from)
                    .unwrap_or(Value::Null),
                "to_breakdown": current_price
                    .zip(breakdown_below)
                    .filter(|(price, level)| atr14 > 0.0 && *level <= *price)
                    .map(|(price, level)| round2((price - level) / atr14))
                    .map(Value::from)
                    .unwrap_or(Value::Null),
                "to_breakout": current_price
                    .zip(breakout_above)
                    .filter(|(price, level)| atr14 > 0.0 && *level >= *price)
                    .map(|(price, level)| round2((level - price) / atr14))
                    .map(Value::from)
                    .unwrap_or(Value::Null),
            }),
        );
        evidence_entry.insert(
            "signal_snapshot".to_string(),
            json!({
                "ema_regime": ema_regime,
                "cvd_bias": cvd_bias,
                "absorption_bias": absorption_bias,
                "initiation_bias": flow
                    .get("initiation")
                    .and_then(Value::as_object)
                    .and_then(|value| value.get("bias"))
                    .and_then(Value::as_str)
                    .unwrap_or("neutral"),
                "whale_bias": whale_bias,
                "value_area_position": value_area_position,
                "funding_rate_sign": flow
                    .get("funding")
                    .and_then(Value::as_object)
                    .and_then(|value| value.get("rate_sign"))
                    .and_then(Value::as_i64)
                    .unwrap_or(0),
                "vpin_elevated": flow
                    .get("vpin")
                    .and_then(Value::as_object)
                    .and_then(|value| value.get("elevated"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            }),
        );
        if let Some(stability) = build_temporal_stability(
            history.get(*tf).map(Vec::as_slice).unwrap_or(&[]),
            evidence,
            primary_support,
            primary_resistance,
            atr14,
        ) {
            evidence_entry.insert("temporal_stability".to_string(), stability);
        }
        result.insert((*tf).to_string(), Value::Object(evidence_entry));
    }
    result
}

fn build_multi_timeframe_evidence(
    current_price: Option<f64>,
    timeframe_evidence: &Map<String, Value>,
) -> Value {
    let tf15 = timeframe_evidence.get("15m").and_then(Value::as_object);
    let tf4h = timeframe_evidence.get("4h").and_then(Value::as_object);
    let tf1d = timeframe_evidence.get("1d").and_then(Value::as_object);

    let score15 = tf15
        .and_then(|value| value.get("balance_scores"))
        .and_then(Value::as_object)
        .and_then(|value| value.get("evidence_balance_score"))
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let score4h = tf4h
        .and_then(|value| value.get("balance_scores"))
        .and_then(Value::as_object)
        .and_then(|value| value.get("evidence_balance_score"))
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let score1d = tf1d
        .and_then(|value| value.get("balance_scores"))
        .and_then(Value::as_object)
        .and_then(|value| value.get("evidence_balance_score"))
        .and_then(Value::as_f64)
        .unwrap_or(0.0);

    json!({
        "macro_tf": "1d",
        "setup_tf": "4h",
        "trigger_tf": "15m",
        "balance_scores": {
            "15m": round3(score15),
            "4h": round3(score4h),
            "1d": round3(score1d),
        },
        "pair_coherence": {
            "1d_4h": round3((score1d * score4h).clamp(-1.0, 1.0)),
            "4h_15m": round3((score4h * score15).clamp(-1.0, 1.0)),
        },
        "range_nesting": {
            "15m_inside_4h": ranges_nested(tf15, tf4h),
            "4h_inside_1d": ranges_nested(tf4h, tf1d),
        },
        "current_price": current_price,
    })
}

fn ranges_nested(inner: Option<&Map<String, Value>>, outer: Option<&Map<String, Value>>) -> Value {
    let inner_support = inner
        .and_then(|value| value.get("display_range"))
        .and_then(Value::as_object)
        .and_then(|value| value.get("support"))
        .and_then(Value::as_f64);
    let inner_resistance = inner
        .and_then(|value| value.get("display_range"))
        .and_then(Value::as_object)
        .and_then(|value| value.get("resistance"))
        .and_then(Value::as_f64);
    let outer_support = outer
        .and_then(|value| value.get("display_range"))
        .and_then(Value::as_object)
        .and_then(|value| value.get("support"))
        .and_then(Value::as_f64);
    let outer_resistance = outer
        .and_then(|value| value.get("display_range"))
        .and_then(Value::as_object)
        .and_then(|value| value.get("resistance"))
        .and_then(Value::as_f64);

    match (
        inner_support,
        inner_resistance,
        outer_support,
        outer_resistance,
    ) {
        (
            Some(inner_support),
            Some(inner_resistance),
            Some(outer_support),
            Some(outer_resistance),
        ) => Value::Bool(inner_support >= outer_support && inner_resistance <= outer_resistance),
        _ => Value::Null,
    }
}

fn build_temporal_stability(
    history: &[EvidenceSnapshot],
    evidence: BiasEvidence,
    primary_support: Option<f64>,
    primary_resistance: Option<f64>,
    atr14: f64,
) -> Option<Value> {
    if history.is_empty() {
        return None;
    }
    let mut samples = history.to_vec();
    samples.sort_by(|left, right| left.ts_bucket.cmp(&right.ts_bucket));
    let mut trend_balance_labels = samples
        .iter()
        .map(|snapshot| evidence_bucket(snapshot.trend_balance_score))
        .collect::<Vec<_>>();
    trend_balance_labels.push(evidence_bucket(evidence.trend_balance_score));
    let mut flow_balance_labels = samples
        .iter()
        .map(|snapshot| evidence_bucket(snapshot.flow_balance_score))
        .collect::<Vec<_>>();
    flow_balance_labels.push(evidence_bucket(evidence.flow_balance_score));
    let mut evidence_balance_labels = samples
        .iter()
        .map(|snapshot| evidence_bucket(snapshot.evidence_balance_score))
        .collect::<Vec<_>>();
    evidence_balance_labels.push(evidence_bucket(evidence.evidence_balance_score));
    let oldest = samples.first()?;
    let support_drift_atr = match (primary_support, atr14 > 0.0) {
        (Some(primary_support), true) => {
            round2((primary_support - oldest.primary_support).abs() / atr14)
        }
        _ => 0.0,
    };
    let resistance_drift_atr = match (primary_resistance, atr14 > 0.0) {
        (Some(primary_resistance), true) => {
            round2((primary_resistance - oldest.primary_resistance).abs() / atr14)
        }
        _ => 0.0,
    };
    let balance_flip_count = evidence_balance_labels
        .windows(2)
        .filter(|pair| pair[0] != pair[1])
        .count();
    let evidence_balance_drift =
        round2((evidence.evidence_balance_score - oldest.evidence_balance_score).abs());
    Some(json!({
        "samples_used": samples.len() + 1,
        "trend_balance_consensus": consensus_label(&trend_balance_labels),
        "flow_balance_consensus": consensus_label(&flow_balance_labels),
        "evidence_balance_consensus": consensus_label(&evidence_balance_labels),
        "range_stability": range_stability_label(support_drift_atr, resistance_drift_atr),
        "balance_flip_count": balance_flip_count,
        "support_drift_atr": support_drift_atr,
        "resistance_drift_atr": resistance_drift_atr,
        "evidence_balance_drift": evidence_balance_drift,
    }))
}

fn consensus_label(values: &[&str]) -> &'static str {
    if values.len() == 2 {
        return if values[0] == values[1] {
            "stable"
        } else {
            "unstable"
        };
    }
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for value in values {
        *counts.entry(*value).or_default() += 1;
    }
    let best = counts.values().copied().max().unwrap_or_default();
    match best {
        3 => "stable",
        2 => "mixed",
        _ => "unstable",
    }
}

fn range_stability_label(support_drift_atr: f64, resistance_drift_atr: f64) -> &'static str {
    let max_drift = support_drift_atr.max(resistance_drift_atr);
    if max_drift < 0.5 {
        "stable"
    } else if max_drift < 1.5 {
        "shifting"
    } else {
        "unstable"
    }
}

fn load_recent_scan_evidence(
    symbol: &str,
    current_ts_bucket: &str,
) -> BTreeMap<String, Vec<EvidenceSnapshot>> {
    let mut per_tf: BTreeMap<String, Vec<EvidenceSnapshot>> = SCAN_WINDOWS
        .iter()
        .map(|tf| ((*tf).to_string(), Vec::new()))
        .collect();

    let Ok(entries) = fs::read_dir("/data/systems/llm/temp_model_input") else {
        return per_tf;
    };
    let mut paths = entries
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.contains(&format!("_{symbol}_scan_")) && name.ends_with(".json"))
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    paths.sort();
    for path in paths.into_iter().rev().take(12) {
        let Ok(raw) = fs::read_to_string(&path) else {
            continue;
        };
        let Ok(root) = serde_json::from_str::<Value>(&raw) else {
            continue;
        };
        let file_symbol = root
            .get("symbol")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let file_ts_bucket = root
            .get("ts_bucket")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if file_symbol != symbol || file_ts_bucket == current_ts_bucket {
            continue;
        }
        for tf in SCAN_WINDOWS {
            let snapshot = root
                .get("timeframe_evidence")
                .and_then(Value::as_object)
                .and_then(|evidence| evidence.get(*tf))
                .and_then(Value::as_object)
                .map(|evidence| EvidenceSnapshot {
                    ts_bucket: file_ts_bucket.to_string(),
                    trend_balance_score: evidence
                        .get("balance_scores")
                        .and_then(Value::as_object)
                        .and_then(|value| value.get("trend_balance_score"))
                        .and_then(Value::as_f64)
                        .unwrap_or(0.0),
                    flow_balance_score: evidence
                        .get("balance_scores")
                        .and_then(Value::as_object)
                        .and_then(|value| value.get("flow_balance_score"))
                        .and_then(Value::as_f64)
                        .unwrap_or(0.0),
                    evidence_balance_score: evidence
                        .get("balance_scores")
                        .and_then(Value::as_object)
                        .and_then(|value| value.get("evidence_balance_score"))
                        .and_then(Value::as_f64)
                        .unwrap_or(0.0),
                    primary_support: evidence
                        .get("primary_support")
                        .and_then(Value::as_f64)
                        .unwrap_or(0.0),
                    primary_resistance: evidence
                        .get("primary_resistance")
                        .and_then(Value::as_f64)
                        .unwrap_or(0.0),
                })
                .or_else(|| {
                    root.get("timeframe_decision")
                        .and_then(Value::as_object)
                        .and_then(|decisions| decisions.get(*tf))
                        .and_then(Value::as_object)
                        .map(|decision| {
                            let trend_balance_score = decision
                                .get("signals")
                                .and_then(Value::as_object)
                                .and_then(|value| value.get("ema_regime"))
                                .and_then(Value::as_str)
                                .map(directional_string_score)
                                .unwrap_or(0.0);
                            let flow_balance_score = weighted_average_scores(&[
                                (
                                    decision
                                        .get("signals")
                                        .and_then(Value::as_object)
                                        .and_then(|value| value.get("cvd_bias"))
                                        .and_then(Value::as_str)
                                        .map(directional_string_score)
                                        .unwrap_or(0.0),
                                    0.4,
                                ),
                                (
                                    decision
                                        .get("signals")
                                        .and_then(Value::as_object)
                                        .and_then(|value| value.get("absorption_bias"))
                                        .and_then(Value::as_str)
                                        .map(directional_string_score)
                                        .unwrap_or(0.0),
                                    0.3,
                                ),
                                (
                                    decision
                                        .get("signals")
                                        .and_then(Value::as_object)
                                        .and_then(|value| value.get("whale_bias"))
                                        .and_then(Value::as_str)
                                        .map(directional_string_score)
                                        .unwrap_or(0.0),
                                    0.3,
                                ),
                            ]);
                            let evidence_balance_score = decision
                                .get("direction")
                                .and_then(Value::as_str)
                                .map(directional_string_score)
                                .unwrap_or(0.0);
                            EvidenceSnapshot {
                                ts_bucket: file_ts_bucket.to_string(),
                                trend_balance_score: round3(trend_balance_score),
                                flow_balance_score: round3(flow_balance_score),
                                evidence_balance_score: round3(evidence_balance_score),
                                primary_support: decision
                                    .get("primary_support")
                                    .and_then(Value::as_f64)
                                    .unwrap_or(0.0),
                                primary_resistance: decision
                                    .get("primary_resistance")
                                    .and_then(Value::as_f64)
                                    .unwrap_or(0.0),
                            }
                        })
                });
            let Some(snapshot) = snapshot else {
                continue;
            };
            let items = per_tf.entry((*tf).to_string()).or_default();
            items.push(snapshot);
            items.sort_by(|left, right| left.ts_bucket.cmp(&right.ts_bucket));
            if items.len() > 2 {
                let drain = items.len() - 2;
                items.drain(0..drain);
            }
        }
    }
    per_tf
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
                items.truncate(5);
            }
        }
    }
    if let Some(by_session) = payload.get_mut("by_session").and_then(Value::as_object_mut) {
        for session in by_session.values_mut() {
            if let Some(dev_series) = session.get_mut("dev_series").and_then(Value::as_object_mut) {
                for series in dev_series.values_mut() {
                    if let Some(items) = series.as_array_mut() {
                        items.reverse();
                        items.truncate(5);
                    }
                }
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
                    "active_bull_fvgs",
                    "active_bear_fvgs",
                    "is_ready",
                    "coverage_ratio",
                ],
            );
            if let Some(value) = window_value.get("nearest_bull_fvg") {
                let sanitized = sanitize_fvg_value(value);
                if !sanitized.is_null() {
                    filtered_window.insert("nearest_bull_fvg".to_string(), sanitized);
                }
            }
            if let Some(value) = window_value.get("nearest_bear_fvg") {
                let sanitized = sanitize_fvg_value(value);
                if !sanitized.is_null() {
                    filtered_window.insert("nearest_bear_fvg".to_string(), sanitized);
                }
            }
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
        for (window, limit) in [("15m", 3usize), ("4h", 3usize), ("1d", 3usize)] {
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
    filter_footprint_with_price(payload, None)
}

fn filter_footprint_with_price(payload: &Value, current_price: Option<f64>) -> Value {
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
            ],
        );
        let selected_price = current_price
            .or_else(|| window_value.get("ua_top").and_then(Value::as_f64))
            .or_else(|| window_value.get("ua_bottom").and_then(Value::as_f64))
            .unwrap_or(0.0);
        let buy_stacks = window_value
            .get("buy_stacks")
            .and_then(Value::as_array)
            .map(|items| footprint_nearest_stacks(items, selected_price, 2))
            .unwrap_or_else(|| json!({"above": [], "below": [], "cross": []}));
        let sell_stacks = window_value
            .get("sell_stacks")
            .and_then(Value::as_array)
            .map(|items| footprint_nearest_stacks(items, selected_price, 2))
            .unwrap_or_else(|| json!({"above": [], "below": [], "cross": []}));
        let buy_clusters = window_value
            .get("buy_imbalance_prices")
            .and_then(Value::as_array)
            .map(|prices| aggregate_price_clusters(prices, bin_size))
            .map(|items| footprint_nearest_clusters(&items, selected_price, 2))
            .unwrap_or_else(|| json!({"above": [], "below": []}));
        let sell_clusters = window_value
            .get("sell_imbalance_prices")
            .and_then(Value::as_array)
            .map(|prices| aggregate_price_clusters(prices, bin_size))
            .map(|items| footprint_nearest_clusters(&items, selected_price, 2))
            .unwrap_or_else(|| json!({"above": [], "below": []}));
        filtered_window.insert("buy_stacks".to_string(), buy_stacks);
        filtered_window.insert("sell_stacks".to_string(), sell_stacks);
        filtered_window.insert("buy_imb_clusters".to_string(), buy_clusters);
        filtered_window.insert("sell_imb_clusters".to_string(), sell_clusters);
        filtered_windows.insert(window.to_string(), Value::Object(filtered_window));
    }
    result.insert("by_window".to_string(), Value::Object(filtered_windows));

    Value::Object(result)
}

fn footprint_nearest_stacks(items: &[Value], current_price: f64, limit: usize) -> Value {
    let to_compact = |item: &Value, dist: f64| -> Value {
        let mut compact = Map::new();
        if let Some(object) = item.as_object() {
            copy_fields(
                &mut compact,
                object,
                &["start_price", "end_price", "length"],
            );
        }
        compact.insert("dist".to_string(), Value::from(round2(dist)));
        Value::Object(compact)
    };
    let span_dist = |item: &Value| -> f64 {
        let start = item
            .get("start_price")
            .and_then(Value::as_f64)
            .unwrap_or(current_price);
        let end = item
            .get("end_price")
            .and_then(Value::as_f64)
            .unwrap_or(current_price);
        (start - current_price)
            .abs()
            .min((end - current_price).abs())
    };
    let is_cross = |item: &Value| -> bool {
        let start = item
            .get("start_price")
            .and_then(Value::as_f64)
            .unwrap_or(f64::INFINITY);
        let end = item
            .get("end_price")
            .and_then(Value::as_f64)
            .unwrap_or(f64::NEG_INFINITY);
        start <= current_price && current_price <= end
    };
    let is_above = |item: &Value| -> bool {
        item.get("start_price")
            .and_then(Value::as_f64)
            .map(|value| value > current_price)
            .unwrap_or(false)
    };
    let mut above = items
        .iter()
        .filter(|item| !is_cross(item) && is_above(item))
        .collect::<Vec<_>>();
    let mut below = items
        .iter()
        .filter(|item| !is_cross(item) && !is_above(item))
        .collect::<Vec<_>>();
    above.sort_by(|left, right| {
        span_dist(left)
            .partial_cmp(&span_dist(right))
            .unwrap_or(Ordering::Equal)
    });
    below.sort_by(|left, right| {
        span_dist(left)
            .partial_cmp(&span_dist(right))
            .unwrap_or(Ordering::Equal)
    });
    json!({
        "above": above
            .into_iter()
            .take(limit)
            .map(|item| to_compact(item, span_dist(item)))
            .collect::<Vec<_>>(),
        "below": below
            .into_iter()
            .take(limit)
            .map(|item| to_compact(item, span_dist(item)))
            .collect::<Vec<_>>(),
        "cross": items
            .iter()
            .filter(|item| is_cross(item))
            .map(|item| to_compact(item, span_dist(item)))
            .collect::<Vec<_>>(),
    })
}

fn footprint_nearest_clusters(items: &[Value], current_price: f64, limit: usize) -> Value {
    let to_compact = |item: &Value| -> Value {
        let mut compact = Map::new();
        if let Some(object) = item.as_object() {
            copy_fields(&mut compact, object, &["p", "n"]);
            if let Some(price) = object.get("p").and_then(Value::as_f64) {
                compact.insert(
                    "dist".to_string(),
                    Value::from(round2((price - current_price).abs())),
                );
            }
        }
        Value::Object(compact)
    };
    let mut above = items
        .iter()
        .filter(|item| {
            item.get("p")
                .and_then(Value::as_f64)
                .map(|price| price > current_price)
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    let mut below = items
        .iter()
        .filter(|item| {
            item.get("p")
                .and_then(Value::as_f64)
                .map(|price| price < current_price)
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    above.sort_by(|left, right| {
        let left_dist = left
            .get("p")
            .and_then(Value::as_f64)
            .map(|price| (price - current_price).abs())
            .unwrap_or(f64::INFINITY);
        let right_dist = right
            .get("p")
            .and_then(Value::as_f64)
            .map(|price| (price - current_price).abs())
            .unwrap_or(f64::INFINITY);
        left_dist
            .partial_cmp(&right_dist)
            .unwrap_or(Ordering::Equal)
    });
    below.sort_by(|left, right| {
        let left_dist = left
            .get("p")
            .and_then(Value::as_f64)
            .map(|price| (price - current_price).abs())
            .unwrap_or(f64::INFINITY);
        let right_dist = right
            .get("p")
            .and_then(Value::as_f64)
            .map(|price| (price - current_price).abs())
            .unwrap_or(f64::INFINITY);
        left_dist
            .partial_cmp(&right_dist)
            .unwrap_or(Ordering::Equal)
    });
    json!({
        "above": above.into_iter().take(limit).map(to_compact).collect::<Vec<_>>(),
        "below": below.into_iter().take(limit).map(to_compact).collect::<Vec<_>>(),
    })
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
        return if value.is_null() {
            Value::Null
        } else {
            value.clone()
        };
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
    filtered.retain(|_, value| !value.is_null());
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

#[cfg(test)]
mod tests {
    use super::{
        build_display_range, build_event_summary_from_events, compute_atr14,
        extract_ema_regime_current, filter_fvg, weighted_event_score, BiasEvidence, LevelAnchor,
        ScanFilter,
    };
    use crate::llm::provider::ModelInvocationInput;
    use chrono::{DateTime, Utc};
    use serde_json::{json, Value};
    use std::fs;
    use std::path::PathBuf;

    fn sample_input(indicators: Value, ts_bucket: DateTime<Utc>) -> ModelInvocationInput {
        ModelInvocationInput {
            symbol: "ETHUSDT".to_string(),
            ts_bucket,
            window_code: "15m".to_string(),
            indicator_count: indicators.as_object().map(|obj| obj.len()).unwrap_or(0),
            source_routing_key: "llm_indicator_minute".to_string(),
            source_published_at: None,
            received_at: ts_bucket,
            indicators,
            missing_indicator_codes: vec![],
            management_mode: false,
            pending_order_mode: false,
            trading_state: None,
            management_snapshot: None,
        }
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

    fn sample_event(ts: &str, direction: i64, kind: &str, pivot_price: f64, score: f64) -> Value {
        json!({
            "event_start_ts": ts,
            "event_end_ts": ts,
            "confirm_ts": ts,
            "direction": direction,
            "pivot_price": pivot_price,
            "score": score,
            "type": kind,
        })
    }

    fn sample_divergence_event(ts: &str, kind: &str, score: f64) -> Value {
        json!({
            "event_start_ts": ts,
            "event_end_ts": ts,
            "pivot_side": if kind.contains("bullish") { "low" } else { "high" },
            "likely_driver": "mixed",
            "score": score,
            "type": kind,
        })
    }

    fn fixture_indicators() -> Value {
        json!({
            "price_volume_structure": {
                "payload": {
                    "poc_price": 2104.5,
                    "poc_volume": 12.0,
                    "vah": 2106.0,
                    "val": 2101.5,
                    "bar_volume": 100.0,
                    "hvn_levels": [2102.0, 2105.0],
                    "lvn_levels": [2100.5],
                    "by_window": {
                        "15m": {"window_bars_used": 20, "poc_price": 2104.5, "poc_volume": 12.0, "vah": 2106.0, "val": 2101.5, "bar_volume": 100.0, "hvn_levels": [2102.0, 2105.0], "lvn_levels": [2100.5], "value_area_levels": sample_value_area_levels(2101.0, 0.5, 6), "volume_zscore": 1.2, "volume_dryup": false},
                        "4h": {"window_bars_used": 20, "poc_price": 2106.0, "poc_volume": 12.0, "vah": 2111.0, "val": 2098.0, "bar_volume": 200.0, "hvn_levels": [2100.0, 2108.0], "lvn_levels": [2095.0], "value_area_levels": sample_value_area_levels(2097.0, 1.0, 6), "volume_zscore": 0.7, "volume_dryup": false},
                        "1d": {"window_bars_used": 20, "poc_price": 2108.0, "poc_volume": 12.0, "vah": 2120.0, "val": 2088.0, "bar_volume": 300.0, "hvn_levels": [2090.0, 2115.0], "lvn_levels": [2082.0], "value_area_levels": sample_value_area_levels(2088.0, 2.0, 6), "volume_zscore": 0.4, "volume_dryup": false}
                    }
                }
            },
            "tpo_market_profile": {
                "payload": {
                    "tpo_poc": 2105.0,
                    "tpo_vah": 2109.0,
                    "tpo_val": 2100.0,
                    "dev_series": {
                        "15m": [
                            {"ts": "2026-03-14T07:00:00Z"},
                            {"ts": "2026-03-14T07:15:00Z"},
                            {"ts": "2026-03-14T07:30:00Z"},
                            {"ts": "2026-03-14T07:45:00Z"},
                            {"ts": "2026-03-14T08:00:00Z"},
                            {"ts": "2026-03-14T08:15:00Z"}
                        ]
                    },
                    "by_session": {
                        "4h": {"tpo_poc": 2106.0, "tpo_vah": 2110.0, "tpo_val": 2099.0},
                        "1d": {"tpo_poc": 2108.0, "tpo_vah": 2118.0, "tpo_val": 2089.0}
                    }
                }
            },
            "fvg": {
                "payload": {
                    "base_detection_uses_spot": true,
                    "source_market": "futures",
                    "by_window": {
                        "15m": {
                            "active_bull_fvgs": [{"upper": 2102.5, "lower": 2101.5, "fvg_id": "a", "tf": "15m"}],
                            "active_bear_fvgs": [{"upper": 2108.5, "lower": 2107.5, "fvg_id": "b", "tf": "15m"}],
                            "nearest_bull_fvg": {"upper": 2102.5, "lower": 2101.5, "fvg_id": "c", "tf": "15m"},
                            "nearest_bear_fvg": {"upper": 2108.5, "lower": 2107.5, "fvg_id": "d", "tf": "15m"},
                            "is_ready": true,
                            "coverage_ratio": 1.0
                        },
                        "4h": {
                            "active_bull_fvgs": [],
                            "active_bear_fvgs": [],
                            "nearest_bull_fvg": {"upper": 2098.0, "lower": 2095.0},
                            "nearest_bear_fvg": {"upper": 2115.0, "lower": 2112.0},
                            "is_ready": true,
                            "coverage_ratio": 1.0
                        },
                        "1d": {
                            "active_bull_fvgs": [],
                            "active_bear_fvgs": [],
                            "nearest_bull_fvg": {"upper": 2088.0, "lower": 2080.0},
                            "nearest_bear_fvg": {"upper": 2124.0, "lower": 2119.0},
                            "is_ready": true,
                            "coverage_ratio": 1.0
                        }
                    }
                }
            },
            "kline_history": {
                "payload": {
                    "intervals": {
                        "15m": {"markets": {"futures": {"bars": [
                            {"open": 2100.0, "high": 2102.0, "low": 2099.0, "close": 2101.0, "volume_base": 100.0, "open_time": "2026-03-14T07:15:00Z", "is_closed": true},
                            {"open": 2101.0, "high": 2104.0, "low": 2100.0, "close": 2103.5, "volume_base": 110.0, "open_time": "2026-03-14T07:30:00Z", "is_closed": true},
                            {"open": 2103.5, "high": 2105.5, "low": 2102.0, "close": 2104.0, "volume_base": 120.0, "open_time": "2026-03-14T07:45:00Z", "is_closed": true}
                        ]}}},
                        "4h": {"markets": {"futures": {"bars": [
                            {"open": 2088.0, "high": 2098.0, "low": 2085.0, "close": 2095.0, "volume_base": 200.0, "open_time": "2026-03-13T20:00:00Z", "is_closed": true},
                            {"open": 2095.0, "high": 2106.0, "low": 2090.0, "close": 2102.0, "volume_base": 220.0, "open_time": "2026-03-14T00:00:00Z", "is_closed": true},
                            {"open": 2102.0, "high": 2108.0, "low": 2098.0, "close": 2104.0, "volume_base": 240.0, "open_time": "2026-03-14T04:00:00Z", "is_closed": true}
                        ]}}},
                        "1d": {"markets": {"futures": {"bars": [
                            {"open": 2060.0, "high": 2088.0, "low": 2055.0, "close": 2075.0, "volume_base": 300.0, "open_time": "2026-03-11T00:00:00Z", "is_closed": true},
                            {"open": 2075.0, "high": 2100.0, "low": 2070.0, "close": 2092.0, "volume_base": 320.0, "open_time": "2026-03-12T00:00:00Z", "is_closed": true},
                            {"open": 2092.0, "high": 2108.0, "low": 2088.0, "close": 2104.0, "volume_base": 340.0, "open_time": "2026-03-13T00:00:00Z", "is_closed": true}
                        ]}}}
                    }
                }
            },
            "ema_trend_regime": {
                "payload": {
                    "ema_21": 2103.0,
                    "ema_100_htf": {"4h": 2092.0, "1d": 2050.0},
                    "ffill_series_by_output_window": {
                        "15m": [
                            {"ts": "2026-03-14T07:15:00Z", "trend_regime": "bear", "by_tf": {"4h": {"trend_regime": "bull"}, "1d": {"trend_regime": "bull"}}},
                            {"ts": "2026-03-14T07:30:00Z", "trend_regime": "bull", "by_tf": {"4h": {"trend_regime": "bull"}, "1d": {"trend_regime": "bull"}}},
                            {"ts": "2026-03-14T07:45:00Z", "trend_regime": "bull", "by_tf": {"4h": {"trend_regime": "bull"}, "1d": {"trend_regime": "bull"}}}
                        ]
                    }
                }
            },
            "cvd_pack": {
                "payload": {
                    "delta_fut": 1200.0,
                    "delta_spot": 200.0,
                    "relative_delta_fut": 0.32,
                    "relative_delta_spot": 0.06,
                    "by_window": {
                        "15m": {"series": [
                            {"ts": "2026-03-14T07:15:00Z", "delta_fut": 100.0, "relative_delta_fut": 0.10, "cvd_7d_fut": 1000.0, "spot_flow_dominance": "balanced"},
                            {"ts": "2026-03-14T07:30:00Z", "delta_fut": 300.0, "relative_delta_fut": 0.20, "cvd_7d_fut": 1200.0, "spot_flow_dominance": "futures"},
                            {"ts": "2026-03-14T07:45:00Z", "delta_fut": 500.0, "relative_delta_fut": 0.32, "cvd_7d_fut": 1500.0, "spot_flow_dominance": "futures"}
                        ]},
                        "4h": {"series": [
                            {"ts": "2026-03-13T20:00:00Z", "delta_fut": 200.0, "relative_delta_fut": 0.08, "cvd_7d_fut": 900.0, "spot_flow_dominance": "balanced"},
                            {"ts": "2026-03-14T00:00:00Z", "delta_fut": 240.0, "relative_delta_fut": 0.10, "cvd_7d_fut": 1100.0, "spot_flow_dominance": "futures"},
                            {"ts": "2026-03-14T04:00:00Z", "delta_fut": 280.0, "relative_delta_fut": 0.12, "cvd_7d_fut": 1300.0, "spot_flow_dominance": "futures"}
                        ]},
                        "1d": {"series": [
                            {"ts": "2026-03-11T00:00:00Z", "delta_fut": 150.0, "relative_delta_fut": 0.05, "cvd_7d_fut": 700.0, "spot_flow_dominance": "balanced"},
                            {"ts": "2026-03-12T00:00:00Z", "delta_fut": 180.0, "relative_delta_fut": 0.06, "cvd_7d_fut": 850.0, "spot_flow_dominance": "futures"},
                            {"ts": "2026-03-13T00:00:00Z", "delta_fut": 220.0, "relative_delta_fut": 0.07, "cvd_7d_fut": 980.0, "spot_flow_dominance": "futures"}
                        ]}
                    }
                }
            },
            "whale_trades": {
                "payload": {
                    "by_window": {
                        "15m": {"fut_whale_delta_notional": 1200000.0, "xmk_whale_delta_notional_gap_s_minus_f": -200000.0},
                        "4h": {"fut_whale_delta_notional": 2400000.0, "xmk_whale_delta_notional_gap_s_minus_f": -400000.0},
                        "1d": {"fut_whale_delta_notional": 4800000.0, "xmk_whale_delta_notional_gap_s_minus_f": -800000.0}
                    }
                }
            },
            "vpin": {
                "payload": {
                    "by_window": {
                        "15m": {"vpin_fut": 0.42},
                        "4h": {"vpin_fut": 0.46},
                        "1d": {"vpin_fut": 0.48}
                    }
                }
            },
            "funding_rate": {
                "payload": {
                    "mark_price_last": 2104.0,
                    "by_window": {
                        "15m": {"funding_current": 0.0001, "funding_twa": 0.00008, "changes": [{"change_ts": "2026-03-14T07:45:00Z", "funding_delta": 0.00001, "funding_new": 0.0001, "funding_prev": 0.00009}]},
                        "4h": {"funding_current": 0.00011, "funding_twa": 0.00009, "changes": [{"change_ts": "2026-03-14T04:00:00Z", "funding_delta": 0.00001, "funding_new": 0.00011, "funding_prev": 0.00010}]},
                        "1d": {"funding_current": 0.00012, "funding_twa": 0.00010, "changes": [{"change_ts": "2026-03-13T00:00:00Z", "funding_delta": 0.00001, "funding_new": 0.00012, "funding_prev": 0.00011}]}
                    }
                }
            },
            "footprint": {
                "payload": {
                    "by_window": {
                        "15m": {
                            "window_delta": 100.0,
                            "window_total_qty": 1000.0,
                            "unfinished_auction": true,
                            "ua_top": 2105.0,
                            "ua_bottom": 2102.0,
                            "stacked_buy": true,
                            "stacked_sell": false,
                            "buy_stacks": [
                                {"start_price": 2101.5, "end_price": 2102.2, "length": 6},
                                {"start_price": 2105.2, "end_price": 2105.8, "length": 4}
                            ],
                            "sell_stacks": [
                                {"start_price": 2106.0, "end_price": 2106.4, "length": 5},
                                {"start_price": 2102.6, "end_price": 2103.1, "length": 3}
                            ],
                            "buy_imbalance_prices": [2102.0, 2102.1, 2105.4, 2105.5],
                            "sell_imbalance_prices": [2103.0, 2106.2]
                        },
                        "4h": {
                            "window_delta": 200.0,
                            "window_total_qty": 2000.0,
                            "unfinished_auction": false,
                            "ua_top": 2108.0,
                            "ua_bottom": 2098.0,
                            "stacked_buy": true,
                            "stacked_sell": true,
                            "buy_stacks": [{"start_price": 2098.0, "end_price": 2099.0, "length": 5}],
                            "sell_stacks": [{"start_price": 2110.0, "end_price": 2111.0, "length": 4}],
                            "buy_imbalance_prices": [2098.5, 2098.8],
                            "sell_imbalance_prices": [2110.4, 2110.5]
                        },
                        "1d": {
                            "window_delta": 300.0,
                            "window_total_qty": 3000.0,
                            "unfinished_auction": false,
                            "ua_top": 2118.0,
                            "ua_bottom": 2089.0,
                            "stacked_buy": true,
                            "stacked_sell": true,
                            "buy_stacks": [{"start_price": 2090.0, "end_price": 2092.0, "length": 4}],
                            "sell_stacks": [{"start_price": 2118.0, "end_price": 2120.0, "length": 4}],
                            "buy_imbalance_prices": [2090.5, 2091.0],
                            "sell_imbalance_prices": [2118.5, 2119.0]
                        }
                    }
                }
            },
            "orderbook_depth": {
                "payload": {
                    "microprice_fut": 2104.0,
                    "obi_fut": 0.1,
                    "levels": [
                        {"price_level": 2101.0, "bid_liquidity": 50.0, "ask_liquidity": 0.0, "total_liquidity": 50.0},
                        {"price_level": 2102.0, "bid_liquidity": 40.0, "ask_liquidity": 0.0, "total_liquidity": 40.0},
                        {"price_level": 2107.0, "bid_liquidity": 0.0, "ask_liquidity": 55.0, "total_liquidity": 55.0},
                        {"price_level": 2108.0, "bid_liquidity": 0.0, "ask_liquidity": 65.0, "total_liquidity": 65.0}
                    ],
                    "by_window": {
                        "15m": {"obi_fut": 0.02, "spread_twa_fut": 0.003},
                        "1h": {"obi_fut": 0.03, "spread_twa_fut": 0.004}
                    }
                }
            },
            "rvwap_sigma_bands": {
                "payload": {
                    "by_window": {
                        "15m": {"rvwap_band_minus_1": 2102.0, "rvwap_band_minus_2": 2100.0, "rvwap_band_plus_1": 2106.0, "rvwap_band_plus_2": 2108.0, "rvwap_sigma_w": 2.0, "rvwap_w": 2104.0, "samples_used": 15, "z_price_minus_rvwap": 0.2},
                        "4h": {"rvwap_band_minus_1": 2099.0, "rvwap_band_minus_2": 2095.0, "rvwap_band_plus_1": 2111.0, "rvwap_band_plus_2": 2115.0, "rvwap_sigma_w": 4.0, "rvwap_w": 2105.0, "samples_used": 20, "z_price_minus_rvwap": 0.1},
                        "1d": {"rvwap_band_minus_1": 2090.0, "rvwap_band_minus_2": 2082.0, "rvwap_band_plus_1": 2118.0, "rvwap_band_plus_2": 2124.0, "rvwap_sigma_w": 8.0, "rvwap_w": 2106.0, "samples_used": 20, "z_price_minus_rvwap": 0.05}
                    },
                    "series_by_output_window": {
                        "15m": [
                            {"ts": "2026-03-14T07:15:00Z", "by_window": {"15m": {"z_price_minus_rvwap": 0.1}, "4h": {"z_price_minus_rvwap": 0.05}, "1d": {"z_price_minus_rvwap": 0.02}}},
                            {"ts": "2026-03-14T07:30:00Z", "by_window": {"15m": {"z_price_minus_rvwap": 0.2}, "4h": {"z_price_minus_rvwap": 0.1}, "1d": {"z_price_minus_rvwap": 0.04}}}
                        ]
                    }
                }
            },
            "avwap": {
                "payload": {
                    "anchor_ts": "2026-03-10T00:00:00Z",
                    "avwap_fut": 2090.0,
                    "avwap_spot": 2089.0,
                    "fut_last_price": 2104.0,
                    "fut_mark_price": 2104.0,
                    "price_minus_avwap_fut": 14.0,
                    "xmk_avwap_gap_f_minus_s": 1.0,
                    "series_by_window": {
                        "15m": [
                            {"ts": "2026-03-14T07:00:00Z", "avwap_fut": 2088.0, "avwap_spot": 2087.0, "xmk_avwap_gap_f_minus_s": 1.0},
                            {"ts": "2026-03-14T07:15:00Z", "avwap_fut": 2089.0, "avwap_spot": 2088.0, "xmk_avwap_gap_f_minus_s": 1.0},
                            {"ts": "2026-03-14T07:30:00Z", "avwap_fut": 2089.5, "avwap_spot": 2088.5, "xmk_avwap_gap_f_minus_s": 1.0},
                            {"ts": "2026-03-14T07:45:00Z", "avwap_fut": 2090.0, "avwap_spot": 2089.0, "xmk_avwap_gap_f_minus_s": 1.0}
                        ],
                        "4h": [
                            {"ts": "2026-03-13T20:00:00Z", "avwap_fut": 2085.0, "avwap_spot": 2084.0, "xmk_avwap_gap_f_minus_s": 1.0},
                            {"ts": "2026-03-14T00:00:00Z", "avwap_fut": 2088.0, "avwap_spot": 2087.0, "xmk_avwap_gap_f_minus_s": 1.0},
                            {"ts": "2026-03-14T04:00:00Z", "avwap_fut": 2090.0, "avwap_spot": 2089.0, "xmk_avwap_gap_f_minus_s": 1.0},
                            {"ts": "2026-03-14T08:00:00Z", "avwap_fut": 2091.0, "avwap_spot": 2090.0, "xmk_avwap_gap_f_minus_s": 1.0}
                        ],
                        "1d": [
                            {"ts": "2026-03-11T00:00:00Z", "avwap_fut": 2070.0, "avwap_spot": 2069.0, "xmk_avwap_gap_f_minus_s": 1.0},
                            {"ts": "2026-03-12T00:00:00Z", "avwap_fut": 2080.0, "avwap_spot": 2079.0, "xmk_avwap_gap_f_minus_s": 1.0},
                            {"ts": "2026-03-13T00:00:00Z", "avwap_fut": 2090.0, "avwap_spot": 2089.0, "xmk_avwap_gap_f_minus_s": 1.0},
                            {"ts": "2026-03-14T00:00:00Z", "avwap_fut": 2095.0, "avwap_spot": 2094.0, "xmk_avwap_gap_f_minus_s": 1.0}
                        ]
                    }
                }
            },
            "absorption": {
                "payload": {
                    "recent_7d": {
                        "events": [
                            sample_event("2026-03-14T06:30:00Z", 1, "bullish_absorption", 2102.0, 0.9),
                            sample_event("2026-03-14T07:20:00Z", 1, "bullish_absorption", 2103.0, 0.8),
                            sample_event("2026-03-14T07:40:00Z", -1, "bearish_absorption", 2105.0, 0.6)
                        ]
                    }
                }
            },
            "buying_exhaustion": {
                "payload": {
                    "recent_7d": {
                        "events": [sample_event("2026-03-14T07:10:00Z", -1, "buying_exhaustion", 2105.0, 0.7)]
                    }
                }
            },
            "selling_exhaustion": {
                "payload": {
                    "recent_7d": {
                        "events": [sample_event("2026-03-14T07:35:00Z", 1, "selling_exhaustion", 2101.5, 0.8)]
                    }
                }
            },
            "divergence": {
                "payload": {
                    "recent_7d": {
                        "events": [sample_divergence_event("2026-03-14T07:25:00Z", "hidden_bullish_divergence", 0.9)]
                    }
                }
            },
            "bullish_initiation": {
                "payload": {
                    "recent_7d": {
                        "events": [sample_event("2026-03-14T07:45:00Z", 1, "bullish_initiation", 2104.5, 0.8)]
                    }
                }
            },
            "bearish_initiation": {
                "payload": {
                    "recent_7d": {
                        "events": [sample_event("2026-03-14T06:10:00Z", -1, "bearish_initiation", 2106.5, 0.6)]
                    }
                }
            }
        })
    }

    #[test]
    fn build_value_applies_v4_scan_schema() {
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-14T08:00:00Z")
            .expect("parse ts bucket")
            .with_timezone(&Utc);
        let input = sample_input(fixture_indicators(), ts_bucket);
        let value = ScanFilter::build_value(&input).expect("build scan value");

        assert_eq!(
            value.pointer("/current_price").and_then(Value::as_f64),
            Some(2104.0)
        );
        assert!(value
            .pointer("/by_timeframe/15m/structure/level_book/support")
            .is_some());
        assert!(value.pointer("/by_timeframe/4h/trend/ema_regime").is_some());
        assert!(value.pointer("/by_timeframe/15m/flow/cvd/bias").is_some());
        assert!(value
            .pointer("/by_timeframe/15m/flow/initiation/bias")
            .is_some());
        assert!(value
            .pointer("/timeframe_evidence/15m/primary_support")
            .is_some());
        assert!(value
            .pointer("/timeframe_evidence/15m/ranges/micro_range/support")
            .is_some());
        assert_eq!(
            value
                .pointer("/timeframe_evidence/15m/ranges/micro_range/range_role")
                .and_then(Value::as_str),
            Some("compression")
        );
        assert!(value
            .pointer("/timeframe_evidence/15m/ranges/local_structure_range/support")
            .is_some());
        assert_eq!(
            value
                .pointer("/timeframe_evidence/15m/ranges/local_structure_range/range_role")
                .and_then(Value::as_str),
            Some("default_bracket")
        );
        assert!(value
            .pointer("/timeframe_evidence/15m/ranges/session_value_area_range/source_window")
            .is_some());
        assert_eq!(
            value
                .pointer("/timeframe_evidence/15m/ranges/session_value_area_range/range_role")
                .and_then(Value::as_str),
            Some("context")
        );
        assert!(value
            .pointer(
                "/timeframe_evidence/15m/ranges/session_value_area_range/current_price_location"
            )
            .is_some());
        assert!(value
            .pointer("/timeframe_evidence/15m/display_range/support")
            .is_some());
        assert!(value
            .pointer("/timeframe_evidence/15m/display_range/range_width_atr")
            .is_some());
        assert!(value
            .pointer("/timeframe_evidence/15m/balance_scores/evidence_balance_score")
            .is_some());
        assert!(value
            .pointer("/timeframe_evidence/15m/signal_snapshot/ema_regime")
            .is_some());
        assert!(value
            .pointer("/timeframe_evidence/15m/risk_opportunity/upside_opportunity_score")
            .is_some());
        assert!(value.pointer("/multi_timeframe_evidence").is_some());

        assert!(value.pointer("/indicators/kline_history").is_none());
        assert!(value.pointer("/indicators/cvd_pack").is_none());
        assert!(value.pointer("/indicators/absorption").is_none());
        assert!(value.pointer("/indicators/whale_trades").is_none());
        assert!(value.pointer("/indicators/vpin").is_none());
        assert!(value.pointer("/indicators/funding_rate").is_none());

        assert_eq!(
            value
                .pointer("/indicators/avwap/payload/series_by_window/15m")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(3)
        );
        assert!(value
            .pointer("/indicators/tpo_market_profile/payload/dev_series/15m")
            .and_then(Value::as_array)
            .map(|series| series.len() <= 5)
            .unwrap_or(false));
        assert!(value
            .pointer("/indicators/footprint/payload/by_window/15m/buy_stacks/above")
            .is_some());
        assert!(value
            .pointer("/indicators/footprint/payload/by_window/15m/buy_imb_clusters/below")
            .is_some());
        assert!(value
            .pointer("/indicators/orderbook_depth/payload/liquidity_walls/bid_walls")
            .is_some());
        assert!(value
            .pointer("/by_timeframe/4h/flow/absorption/net_score")
            .is_some());
        assert_eq!(
            value
                .pointer("/by_timeframe/15m/structure/session_value_area/source_kind")
                .and_then(Value::as_str),
            Some("tpo_value_area")
        );
        assert_eq!(
            value
                .pointer("/timeframe_evidence/15m/ranges/local_structure_range/support")
                .and_then(Value::as_f64),
            value
                .pointer("/timeframe_evidence/15m/display_range/support")
                .and_then(Value::as_f64)
        );
    }

    #[test]
    fn build_value_omits_non_applicable_trend_fields() {
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-14T08:00:00Z")
            .expect("parse ts bucket")
            .with_timezone(&Utc);
        let input = sample_input(fixture_indicators(), ts_bucket);
        let value = ScanFilter::build_value(&input).expect("build scan value");

        assert!(value
            .pointer("/by_timeframe/15m/trend/close_vs_ema21_pct")
            .is_some());
        assert!(value
            .pointer("/by_timeframe/15m/trend/close_vs_ema100_htf_pct")
            .is_none());
        assert!(value
            .pointer("/by_timeframe/4h/trend/close_vs_ema21_pct")
            .is_none());
        assert!(value
            .pointer("/by_timeframe/4h/trend/close_vs_ema100_htf_pct")
            .is_some());
        assert!(value
            .pointer("/by_timeframe/1d/trend/close_vs_ema21_pct")
            .is_none());
        assert!(value
            .pointer("/by_timeframe/1d/trend/close_vs_ema100_htf_pct")
            .is_some());
    }

    #[test]
    fn filter_fvg_omits_null_placeholders_but_keeps_present_structure() {
        let payload = json!({
            "source_market": "futures",
            "base_detection_uses_spot": true,
            "by_window": {
                "15m": {
                    "active_bull_fvgs": [{
                        "upper": 2102.5,
                        "lower": 2101.5,
                        "distance_to_avwap": null,
                        "spot_confirm_at_birth": null,
                        "state": "fresh"
                    }],
                    "active_bear_fvgs": [],
                    "nearest_bull_fvg": {
                        "upper": 2102.5,
                        "lower": 2101.5,
                        "distance_to_avwap": null,
                        "spot_confirm_at_birth": null,
                        "state": "fresh"
                    },
                    "nearest_bear_fvg": null,
                    "is_ready": true,
                    "coverage_ratio": 1.0
                }
            }
        });

        let value = filter_fvg(&payload);

        assert!(value.pointer("/by_window/15m/nearest_bear_fvg").is_none());
        assert!(value
            .pointer("/by_window/15m/nearest_bull_fvg/fvg_top")
            .is_some());
        assert!(value
            .pointer("/by_window/15m/nearest_bull_fvg/distance_to_avwap")
            .is_none());
        assert!(value
            .pointer("/by_window/15m/nearest_bull_fvg/spot_confirm_at_birth")
            .is_none());
        assert_eq!(
            value
                .pointer("/by_window/15m/nearest_bull_fvg/state")
                .and_then(Value::as_str),
            Some("fresh")
        );
        assert_eq!(
            value
                .pointer("/by_window/15m/active_bull_fvgs/0/state")
                .and_then(Value::as_str),
            Some("fresh")
        );
    }

    #[test]
    fn extract_ema_regime_current_uses_newest_after_normalization() {
        let payload = json!({
            "ffill_series_by_output_window": {
                "15m": [
                    {"ts": "2026-03-14T07:00:00Z", "trend_regime": "bear", "by_tf": {"4h": {"trend_regime": "bear"}, "1d": {"trend_regime": "bear"}}},
                    {"ts": "2026-03-14T07:15:00Z", "trend_regime": "bull", "by_tf": {"4h": {"trend_regime": "bull"}, "1d": {"trend_regime": "bull"}}}
                ]
            }
        });
        let payload = payload.as_object().expect("ema payload object");
        assert_eq!(
            extract_ema_regime_current(Some(payload), "15m"),
            json!("bull")
        );
        assert_eq!(
            extract_ema_regime_current(Some(payload), "4h"),
            json!("bull")
        );
    }

    #[test]
    fn compute_atr14_ignores_gap_bars_with_missing_ohlc() {
        let bars = vec![
            json!({"high": 105.0, "low": 95.0, "close": 100.0}),
            json!({"high": null, "low": null, "close": null}),
            json!({"high": 112.0, "low": 101.0, "close": 110.0}),
            json!({"high": 118.0, "low": 108.0, "close": 115.0}),
        ]
        .into_iter()
        .map(|value| value.as_object().expect("bar object").clone())
        .collect::<Vec<_>>();

        let atr14 = compute_atr14(&bars).expect("atr14 computed");
        assert!((atr14 - 10.666666666666666).abs() < 1e-9);
    }

    #[test]
    fn weighted_event_score_penalizes_old_and_far_events() {
        let now = DateTime::parse_from_rfc3339("2026-03-16T08:47:00Z")
            .expect("parse now")
            .with_timezone(&Utc);
        let near_recent = json!({
            "confirm_ts": "2026-03-16T08:29:00Z",
            "pivot_price": 2244.18,
            "score": 0.94,
            "type": "selling_exhaustion"
        })
        .as_object()
        .expect("near event")
        .clone();
        let old_far = json!({
            "confirm_ts": "2026-03-15T10:13:00Z",
            "pivot_price": 2113.65,
            "score": 0.768,
            "type": "bullish_absorption"
        })
        .as_object()
        .expect("far event")
        .clone();

        let near_score =
            weighted_event_score(&near_recent, Some(now), Some(2241.17), Some(35.43), "4h");
        let far_score = weighted_event_score(&old_far, Some(now), Some(2241.17), Some(35.43), "4h");

        assert!(near_score > far_score);
    }

    #[test]
    fn build_event_summary_uses_weighted_scores_for_bias() {
        let now = DateTime::parse_from_rfc3339("2026-03-16T08:47:00Z")
            .expect("parse now")
            .with_timezone(&Utc);
        let indicators = fixture_indicators();
        let atr14_by_tf = json!({
            "15m": {"atr14": 12.0},
            "4h": {"atr14": 35.0},
            "1d": {"atr14": 100.0}
        });
        let events = vec![
            json!({
                "confirm_ts": "2026-03-16T08:40:00Z",
                "pivot_price": 2103.0,
                "score": 0.9,
                "type": "bullish_absorption",
                "direction": 1
            })
            .as_object()
            .expect("event one")
            .clone(),
            json!({
                "confirm_ts": "2026-03-15T00:00:00Z",
                "pivot_price": 2000.0,
                "score": 1.0,
                "type": "bearish_absorption",
                "direction": -1
            })
            .as_object()
            .expect("event two")
            .clone(),
        ];
        let summary = build_event_summary_from_events(
            &events,
            Some(now),
            indicators
                .get("price_volume_structure")
                .and_then(|_| Some(2104.0)),
            &atr14_by_tf,
            super::event_direction_default,
        );
        assert_eq!(
            summary.pointer("/15m/bias").and_then(Value::as_str),
            Some("bullish")
        );
        assert!(
            summary
                .pointer("/15m/bullish_score")
                .and_then(Value::as_f64)
                .unwrap_or_default()
                > summary
                    .pointer("/15m/bearish_score")
                    .and_then(Value::as_f64)
                    .unwrap_or_default()
        );
    }

    #[test]
    fn build_value_exposes_session_value_area_range_for_intraday_context() {
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-14T08:00:00Z")
            .expect("parse ts bucket")
            .with_timezone(&Utc);
        let input = sample_input(fixture_indicators(), ts_bucket);
        let value = ScanFilter::build_value(&input).expect("build scan value");

        assert_eq!(
            value
                .pointer("/timeframe_evidence/15m/ranges/session_value_area_range/support")
                .and_then(Value::as_f64),
            Some(2100.0)
        );
        assert_eq!(
            value
                .pointer("/timeframe_evidence/15m/ranges/session_value_area_range/resistance")
                .and_then(Value::as_f64),
            Some(2109.0)
        );
        assert_eq!(
            value
                .pointer("/timeframe_evidence/15m/ranges/session_value_area_range/source_kind")
                .and_then(Value::as_str),
            Some("tpo_value_area")
        );
    }

    #[test]
    fn build_display_range_expands_resistance_for_narrow_bullish_breakout() {
        let primary_support = LevelAnchor {
            price: 2253.02,
            strength: 4.2,
            source_count: 4,
        };
        let primary_resistance = LevelAnchor {
            price: 2260.0,
            strength: 1.5,
            source_count: 1,
        };
        let secondary_resistance = LevelAnchor {
            price: 2271.72,
            strength: 2.2,
            source_count: 2,
        };

        let (display_support, display_resistance, mode) = build_display_range(
            "4h",
            Some(2253.35),
            34.08,
            Some(primary_support.price),
            Some(primary_resistance.price),
            Some(&primary_support),
            None,
            Some(&primary_resistance),
            Some(&secondary_resistance),
            Some(2288.0),
            Some(2087.65),
            BiasEvidence {
                trend_balance_score: 0.55,
                flow_balance_score: 0.48,
                structure_balance_score: 0.22,
                evidence_balance_score: 0.43,
                conflict_score: 0.12,
                compression_score: 0.25,
            },
        );

        assert_eq!(display_support, Some(2253.02));
        assert_eq!(display_resistance, Some(2271.72));
        assert_eq!(mode, "expanded_resistance");
    }

    #[test]
    fn build_display_range_keeps_primary_when_secondary_is_weak_far_wall() {
        let primary_support = LevelAnchor {
            price: 2253.02,
            strength: 6.9,
            source_count: 6,
        };
        let primary_resistance = LevelAnchor {
            price: 2257.35,
            strength: 0.8,
            source_count: 1,
        };
        let secondary_resistance = LevelAnchor {
            price: 2280.04,
            strength: 0.8,
            source_count: 1,
        };

        let (display_support, display_resistance, mode) = build_display_range(
            "15m",
            Some(2253.35),
            12.47,
            Some(primary_support.price),
            Some(primary_resistance.price),
            Some(&primary_support),
            None,
            Some(&primary_resistance),
            Some(&secondary_resistance),
            Some(2288.0),
            Some(2244.68),
            BiasEvidence {
                trend_balance_score: 0.41,
                flow_balance_score: 0.18,
                structure_balance_score: 0.11,
                evidence_balance_score: 0.26,
                conflict_score: 0.24,
                compression_score: 0.18,
            },
        );

        assert_eq!(display_support, Some(2253.02));
        assert_eq!(display_resistance, Some(2257.35));
        assert_eq!(mode, "primary");
    }

    #[test]
    fn build_display_range_expands_resistance_for_ultra_narrow_transition_when_second_level_is_stronger(
    ) {
        let primary_support = LevelAnchor {
            price: 2239.13,
            strength: 1.7,
            source_count: 2,
        };
        let primary_resistance = LevelAnchor {
            price: 2246.88,
            strength: 4.2,
            source_count: 4,
        };
        let secondary_resistance = LevelAnchor {
            price: 2255.38,
            strength: 5.7,
            source_count: 5,
        };
        let secondary_support = LevelAnchor {
            price: 2229.45,
            strength: 0.8,
            source_count: 1,
        };

        let (display_support, display_resistance, mode) = build_display_range(
            "4h",
            Some(2246.38),
            34.63,
            Some(primary_support.price),
            Some(primary_resistance.price),
            Some(&primary_support),
            Some(&secondary_support),
            Some(&primary_resistance),
            Some(&secondary_resistance),
            Some(2288.0),
            Some(2087.65),
            BiasEvidence {
                trend_balance_score: -0.18,
                flow_balance_score: -0.14,
                structure_balance_score: 0.05,
                evidence_balance_score: -0.22,
                conflict_score: 0.47,
                compression_score: 0.31,
            },
        );

        assert_eq!(display_support, Some(2239.13));
        assert_eq!(display_resistance, Some(2255.38));
        assert_eq!(mode, "expanded_resistance");
    }

    #[test]
    fn real_snapshot_scan_output_stays_under_v4_size_budget() {
        let Some(path) = latest_temp_indicator_sample_path() else {
            return;
        };

        let raw = fs::read_to_string(&path).expect("read real temp_indicator sample");
        let root: Value = serde_json::from_str(&raw).expect("parse real temp_indicator sample");
        let indicators = root.get("indicators").cloned().expect("indicators field");
        let ts_bucket = root
            .get("ts_bucket")
            .and_then(Value::as_str)
            .and_then(|ts| DateTime::parse_from_rfc3339(ts).ok())
            .map(|ts| ts.with_timezone(&Utc))
            .unwrap_or_else(Utc::now);
        let input = sample_input(indicators, ts_bucket);

        let scan_value = ScanFilter::build_value(&input).expect("build real scan value");
        let serialized = serde_json::to_vec(&scan_value).expect("serialize real scan value");

        assert!(
            serialized.len() < 130 * 1024,
            "serialized scan too large: {}",
            serialized.len()
        );
        assert!(scan_value.pointer("/current_price").is_some());
        assert!(scan_value.pointer("/by_timeframe/15m").is_some());
        assert!(scan_value.pointer("/timeframe_evidence/15m").is_some());
        assert!(scan_value
            .pointer("/by_timeframe/4h/volatility/atr14")
            .and_then(Value::as_f64)
            .is_some());
        assert!(scan_value
            .pointer("/by_timeframe/4h/volatility/atr14_pct")
            .and_then(Value::as_f64)
            .is_some());
        assert!(scan_value
            .pointer("/indicators/footprint/payload/by_window/15m/buy_stacks")
            .is_some());
        assert!(scan_value.pointer("/indicators/kline_history").is_none());
        assert!(scan_value.pointer("/indicators/cvd_pack").is_none());
        assert!(scan_value.pointer("/indicators/absorption").is_none());
        assert!(scan_value.pointer("/indicators/whale_trades").is_none());
    }
}
