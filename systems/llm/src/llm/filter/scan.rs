use crate::llm::provider::ModelInvocationInput;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

const SCAN_WINDOWS: &[&str] = &["15m", "4h", "1d"];
const SCAN_CONTEXT_WINDOWS: &[&str] = &["15m", "1h", "4h", "1d", "3d"];
const CLUSTER_MERGE_TICK_MULTIPLIER: f64 = 2.0;
const PVS_CLUSTER_NEAR_PER_SIDE: usize = 2;
const PVS_CLUSTER_STRONG_PER_SIDE: usize = 2;
const PVS_CLUSTER_MID_PER_SIDE: usize = 1;
const PVS_CLUSTER_DEEP_PER_SIDE: usize = 1;
const FOOTPRINT_CLUSTER_NEAR_PER_SIDE: usize = 2;
const FOOTPRINT_CLUSTER_STRONG_PER_SIDE: usize = 1;
const FOOTPRINT_CLUSTER_MID_PER_SIDE: usize = 1;
const FOOTPRINT_CLUSTER_DEEP_PER_SIDE: usize = 1;
const ORDERBOOK_CLUSTER_NEAR_PER_SIDE: usize = 2;
const ORDERBOOK_CLUSTER_STRONG_PER_SIDE: usize = 1;
const ORDERBOOK_CLUSTER_MID_PER_SIDE: usize = 1;
const ORDERBOOK_CLUSTER_DEEP_PER_SIDE: usize = 1;
const PVS_PEAK_MIN_PERCENTILE: f64 = 0.90;
const PVS_PEAK_NEIGHBOR_STEPS: usize = 2;
const PATH_15M_DETAIL_BARS: usize = 12;
const PATH_4H_CONTEXT_BARS: usize = 8;
const PATH_1D_BACKGROUND_BARS: usize = 10;
const BRACKET_BOARD_LIMIT: usize = 8;
const STRUCTURE_NODE_LIMIT_PER_SIDE: usize = 10;
const ACCEPTANCE_BASIS_BARS: usize = 3;
const EVENTS_7D_MAJOR_LIMIT: usize = 16;
const EVENT_DEDUPE_WINDOW_MINUTES: i64 = 30;
const EVENT_DEDUPE_PRICE_PCT: f64 = 0.15;
const RAW_OVERFLOW_AGGREGATION_PCT: f64 = 0.02;
const RAW_OVERFLOW_NEAR_MAX_ZONES: usize = 36;
const RAW_OVERFLOW_MID_MAX_ZONES: usize = 18;
const RAW_OVERFLOW_FAR_MAX_ZONES: usize = 12;
const RAW_OVERFLOW_FAR_ABSORPTION_MAX_ZONES: usize = 3;
const RAW_OVERFLOW_FAR_ABSORPTION_SCORE_MIN: f64 = 0.70;
const RAW_OVERFLOW_ABSORPTION_MAX_PER_SCOPE_DIRECTION: usize = 10;

pub(crate) struct ScanFilter;

#[derive(Clone, Debug)]
struct PriceClusterCandidate {
    low: f64,
    high: f64,
    mid: f64,
    count: usize,
    weight: f64,
}

#[derive(Default)]
struct ClusterSelection {
    above: Vec<PriceClusterCandidate>,
    below: Vec<PriceClusterCandidate>,
    cross: Vec<PriceClusterCandidate>,
}

#[derive(Clone, Debug)]
struct BracketCandidate {
    scopes: Vec<String>,
    kind: String,
    source_indicators: Vec<String>,
    reference_ts: Option<String>,
    support: f64,
    resistance: f64,
    support_evidence: Vec<String>,
    resistance_evidence: Vec<String>,
}

#[derive(Clone, Debug)]
struct RawStructureZone {
    source_kind: String,
    source_window: String,
    direction: Option<String>,
    side: Option<String>,
    state: Option<String>,
    event_type: Option<String>,
    count: usize,
    price_low: f64,
    price_high: f64,
    max_score: f64,
    total_touch_count: f64,
    reference_ts_latest: Option<String>,
    representative_direction: String,
}

#[derive(Clone, Debug)]
struct NormalizedEvent {
    indicator_code: String,
    event_type: String,
    direction: String,
    scope: Option<String>,
    event_start_ts: Option<String>,
    event_end_ts: Option<String>,
    confirm_ts: Option<String>,
    age_minutes: Option<i64>,
    pivot_price: Option<f64>,
    price_low: Option<f64>,
    price_high: Option<f64>,
    score: Option<f64>,
    spot_confirm: Option<bool>,
    trigger_side: Option<String>,
    distance_to_current_pct: Option<f64>,
    event_count: usize,
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
    let _symbol = root
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
    let raw_price_structures = build_price_structures(
        &indicators,
        current_price,
        current_ts.as_ref(),
        &kline_derived,
    );
    let overflow_price_structures =
        build_raw_overflow_price_structures(&raw_price_structures, current_price);
    let raw_price_levels = build_price_levels(&indicators, current_price, current_ts.as_ref());

    result.insert(
        "version".to_string(),
        Value::String("scan_v6_2".to_string()),
    );
    if let Some(symbol) = root.get("symbol") {
        result.insert("symbol".to_string(), symbol.clone());
    }
    if let Some(ts_bucket) = root.get("ts_bucket") {
        result.insert("ts_bucket".to_string(), ts_bucket.clone());
    }
    if let Some(current_price) = current_price {
        result.insert("current_price".to_string(), Value::from(current_price));
    }
    result.insert(
        "now".to_string(),
        build_now(
            &indicators,
            current_price,
            current_ts.as_ref(),
            &kline_derived,
            &raw_price_structures,
        ),
    );
    result.insert(
        "path_newest_to_oldest".to_string(),
        build_path_newest_to_oldest(&indicators),
    );
    result.insert(
        "events_newest_to_oldest".to_string(),
        build_events_newest_to_oldest(&indicators, current_price, current_ts.as_ref()),
    );
    result.insert(
        "supporting_context".to_string(),
        build_supporting_context(&indicators),
    );
    result.insert(
        "raw_overflow".to_string(),
        json!({
            "price_structures": overflow_price_structures,
            "price_levels": raw_price_levels,
        }),
    );
    Value::Object(result)
}

fn build_now(
    source: &Map<String, Value>,
    current_price: Option<f64>,
    current_ts: Option<&DateTime<Utc>>,
    kline_derived: &Value,
    raw_price_structures: &[Value],
) -> Value {
    json!({
        "price_anchor": build_price_anchor(source, current_price),
        "location_snapshot": build_location_snapshot(source, current_price),
        "value_state_board": build_value_state_board(source, current_price),
        "momentum_snapshot": build_momentum_snapshot(source),
        "bracket_board": build_bracket_board(
            source,
            current_price,
            current_ts,
            kline_derived,
            raw_price_structures,
        ),
        "structure_nodes_near_current": build_structure_nodes_near_current(
            raw_price_structures,
            current_price,
        ),
        "current_flow_snapshot": build_current_flow_snapshot(source, current_price),
        "current_volume_nodes": build_current_volume_nodes(source),
    })
}

fn build_price_anchor(source: &Map<String, Value>, current_price: Option<f64>) -> Value {
    let avwap = extract_indicator_payload(source, "avwap");
    let funding = extract_indicator_payload(source, "funding_rate");

    json!({
        "futures_last_price": avwap
            .and_then(|payload| payload.get("fut_last_price"))
            .and_then(Value::as_f64)
            .or(current_price),
        "futures_mark_price": avwap
            .and_then(|payload| payload.get("fut_mark_price"))
            .and_then(Value::as_f64)
            .or_else(|| funding.and_then(|payload| payload.get("mark_price_last")).and_then(Value::as_f64)),
        "spot_proxy_price": avwap
            .and_then(|payload| payload.get("avwap_spot"))
            .and_then(Value::as_f64),
    })
}

fn build_location_snapshot(source: &Map<String, Value>, current_price: Option<f64>) -> Value {
    json!({
        "vs_avwap": build_vs_avwap_snapshot(source, current_price),
        "vs_ema_band": build_vs_ema_band_snapshot(source, current_price),
        "vs_rvwap": build_vs_rvwap_snapshot(source),
    })
}

fn build_vs_avwap_snapshot(source: &Map<String, Value>, current_price: Option<f64>) -> Value {
    let avwap = extract_indicator_payload(source, "avwap");
    let avwap_fut = avwap
        .and_then(|payload| payload.get("avwap_fut"))
        .and_then(Value::as_f64);
    let price_minus_avwap_fut = avwap
        .and_then(|payload| payload.get("price_minus_avwap_fut"))
        .and_then(Value::as_f64)
        .or_else(|| match (current_price, avwap_fut) {
            (Some(price), Some(anchor)) => Some(price - anchor),
            _ => None,
        });
    json!({
        "avwap_fut": avwap_fut,
        "price_minus_avwap_fut": price_minus_avwap_fut.map(round2),
        "price_minus_avwap_pct": pct_delta(current_price, avwap_fut).map(round2),
        "xmk_avwap_gap_f_minus_s": avwap
            .and_then(|payload| payload.get("xmk_avwap_gap_f_minus_s"))
            .and_then(Value::as_f64)
            .map(round2),
    })
}

fn build_vs_ema_band_snapshot(source: &Map<String, Value>, current_price: Option<f64>) -> Value {
    let ema = extract_indicator_payload(source, "ema_trend_regime");
    let ema_13 = ema
        .and_then(|payload| payload.get("ema_13"))
        .and_then(Value::as_f64);
    let ema_21 = ema
        .and_then(|payload| payload.get("ema_21"))
        .and_then(Value::as_f64);
    let ema_34 = ema
        .and_then(|payload| payload.get("ema_34"))
        .and_then(Value::as_f64);
    let ema_band_low = ema
        .and_then(|payload| payload.get("ema_band_low"))
        .and_then(Value::as_f64)
        .or_else(|| match (ema_13, ema_21, ema_34) {
            (Some(a), Some(b), Some(c)) => Some(a.min(b).min(c)),
            _ => None,
        });
    let ema_band_high = ema
        .and_then(|payload| payload.get("ema_band_high"))
        .and_then(Value::as_f64)
        .or_else(|| match (ema_13, ema_21, ema_34) {
            (Some(a), Some(b), Some(c)) => Some(a.max(b).max(c)),
            _ => None,
        });

    json!({
        "ema_13": ema_13,
        "ema_21": ema_21,
        "ema_34": ema_34,
        "ema_band_low": ema_band_low,
        "ema_band_high": ema_band_high,
        "position": classify_price_position(current_price, ema_band_low, ema_band_high),
        "distance_to_band_low_pct": pct_delta(current_price, ema_band_low).map(round2),
        "distance_to_band_high_pct": pct_delta(current_price, ema_band_high).map(round2),
    })
}

fn build_vs_rvwap_snapshot(source: &Map<String, Value>) -> Vec<Value> {
    let mut snapshots = Vec::new();
    let Some(by_window) = extract_indicator_payload(source, "rvwap_sigma_bands")
        .and_then(|payload| payload.get("by_window"))
        .and_then(Value::as_object)
    else {
        return snapshots;
    };

    for scope in SCAN_WINDOWS {
        let Some(window) = by_window.get(*scope).and_then(Value::as_object) else {
            continue;
        };
        snapshots.push(json!({
            "scope": scope,
            "rvwap": window.get("rvwap_w").and_then(Value::as_f64),
            "sigma_w": window.get("rvwap_sigma_w").and_then(Value::as_f64),
            "z_price_minus_rvwap": window.get("z_price_minus_rvwap").and_then(Value::as_f64),
            "band_minus_1": window.get("rvwap_band_minus_1").and_then(Value::as_f64),
            "band_plus_1": window.get("rvwap_band_plus_1").and_then(Value::as_f64),
            "band_minus_2": window.get("rvwap_band_minus_2").and_then(Value::as_f64),
            "band_plus_2": window.get("rvwap_band_plus_2").and_then(Value::as_f64),
        }));
    }

    snapshots
}

fn build_value_state_board(source: &Map<String, Value>, current_price: Option<f64>) -> Vec<Value> {
    let mut board = Vec::new();

    if let Some(by_window) = extract_indicator_payload(source, "price_volume_structure")
        .and_then(|payload| payload.get("by_window"))
        .and_then(Value::as_object)
    {
        for scope in SCAN_WINDOWS {
            let Some(window) = by_window.get(*scope).and_then(Value::as_object) else {
                continue;
            };
            board.push(build_value_state_entry(
                source,
                scope,
                "price_volume_structure",
                window.get("val").and_then(Value::as_f64),
                window.get("vah").and_then(Value::as_f64),
                window.get("poc_price").and_then(Value::as_f64),
                current_price,
            ));
        }
    }

    if let Some(by_session) = extract_indicator_payload(source, "tpo_market_profile")
        .and_then(|payload| payload.get("by_session"))
        .and_then(Value::as_object)
    {
        for scope in collect_available_windows(by_session) {
            let Some(session) = by_session.get(&scope).and_then(Value::as_object) else {
                continue;
            };
            board.push(build_value_state_entry(
                source,
                &scope,
                "tpo_market_profile",
                session.get("tpo_val").and_then(Value::as_f64),
                session.get("tpo_vah").and_then(Value::as_f64),
                session.get("tpo_poc").and_then(Value::as_f64),
                current_price,
            ));
        }
    }

    if let Some(tpo) = extract_indicator_payload(source, "tpo_market_profile") {
        let value_low = tpo.get("tpo_val").and_then(Value::as_f64);
        let value_high = tpo.get("tpo_vah").and_then(Value::as_f64);
        if value_low.is_some() && value_high.is_some() {
            board.push(build_value_state_entry(
                source,
                "15m",
                "tpo_market_profile",
                value_low,
                value_high,
                tpo.get("tpo_poc").and_then(Value::as_f64),
                current_price,
            ));
        }
    }

    board
}

fn build_value_state_entry(
    source: &Map<String, Value>,
    scope: &str,
    source_name: &str,
    value_low: Option<f64>,
    value_high: Option<f64>,
    poc: Option<f64>,
    current_price: Option<f64>,
) -> Value {
    let closed_bars = extract_closed_futures_bars(source, scope);
    let recent_closes = closed_bars
        .iter()
        .rev()
        .take(ACCEPTANCE_BASIS_BARS)
        .filter_map(|bar| bar.get("close").and_then(Value::as_f64))
        .collect::<Vec<_>>();
    let recent_closes_inside_count = recent_closes
        .iter()
        .filter(|close| is_inside_range(**close, value_low, value_high))
        .count();

    json!({
        "scope": scope,
        "source": source_name,
        "value_low": value_low,
        "value_high": value_high,
        "poc": poc,
        "position": classify_price_position(current_price, value_low, value_high),
        "distance_to_low_pct": pct_delta(current_price, value_low).map(round2),
        "distance_to_high_pct": pct_delta(current_price, value_high).map(round2),
        "recent_closes_inside_count": recent_closes_inside_count,
        "basis_bars": recent_closes.len(),
        "state": classify_acceptance_state(current_price, &recent_closes, value_low, value_high),
    })
}

fn classify_acceptance_state(
    current_price: Option<f64>,
    recent_closes: &[f64],
    value_low: Option<f64>,
    value_high: Option<f64>,
) -> &'static str {
    let current_position = classify_price_position(current_price, value_low, value_high);
    let last_close = recent_closes.first().copied();
    let prev_close = recent_closes.get(1).copied();

    match current_position {
        "inside" => {
            if matches!(
                classify_price_position(prev_close, value_low, value_high),
                "above" | "below"
            ) {
                "reentered_value"
            } else {
                "inside_value"
            }
        }
        "above" => {
            if matches!(
                classify_price_position(prev_close, value_low, value_high),
                "inside"
            ) || matches!(
                classify_price_position(last_close, value_low, value_high),
                "inside"
            ) {
                "rejected_from_above"
            } else {
                "accepted_above"
            }
        }
        "below" => {
            if matches!(
                classify_price_position(prev_close, value_low, value_high),
                "inside"
            ) || matches!(
                classify_price_position(last_close, value_low, value_high),
                "inside"
            ) {
                "rejected_from_below"
            } else {
                "accepted_below"
            }
        }
        _ => "inside_value",
    }
}

fn classify_price_position(
    price: Option<f64>,
    low: Option<f64>,
    high: Option<f64>,
) -> &'static str {
    let (Some(price), Some(low), Some(high)) = (price, low, high) else {
        return "unknown";
    };
    if price < low {
        "below"
    } else if price > high {
        "above"
    } else {
        "inside"
    }
}

fn is_inside_range(price: f64, low: Option<f64>, high: Option<f64>) -> bool {
    let (Some(low), Some(high)) = (low, high) else {
        return false;
    };
    low <= price && price <= high
}

fn build_bracket_board(
    source: &Map<String, Value>,
    current_price: Option<f64>,
    current_ts: Option<&DateTime<Utc>>,
    kline_derived: &Value,
    raw_price_structures: &[Value],
) -> Value {
    let mut candidates = build_bracket_candidates(
        source,
        current_price,
        current_ts,
        kline_derived,
        raw_price_structures,
    );
    candidates = merge_bracket_candidates(candidates);
    candidates.sort_by(|left, right| {
        bracket_distance_to_current(left, current_price)
            .partial_cmp(&bracket_distance_to_current(right, current_price))
            .unwrap_or(Ordering::Equal)
    });

    let mut current_inside = Vec::new();
    let mut nearest_above = Vec::new();
    let mut nearest_below = Vec::new();
    let mut higher_context = Vec::new();

    for candidate in candidates {
        if current_price
            .is_some_and(|price| candidate.support <= price && price <= candidate.resistance)
        {
            current_inside.push(bracket_candidate_to_value(&candidate, current_price));
            continue;
        }

        if candidate
            .scopes
            .iter()
            .any(|scope| matches!(scope.as_str(), "1d" | "3d"))
        {
            higher_context.push(bracket_candidate_to_value(&candidate, current_price));
            continue;
        }

        if current_price.is_some_and(|price| candidate.support > price) {
            nearest_above.push(bracket_candidate_to_value(&candidate, current_price));
        } else {
            nearest_below.push(bracket_candidate_to_value(&candidate, current_price));
        }
    }

    json!({
        "current_inside": limit_values(current_inside, BRACKET_BOARD_LIMIT),
        "nearest_above": limit_values(nearest_above, BRACKET_BOARD_LIMIT),
        "nearest_below": limit_values(nearest_below, BRACKET_BOARD_LIMIT),
        "higher_context": limit_values(higher_context, BRACKET_BOARD_LIMIT),
    })
}

fn build_bracket_candidates(
    source: &Map<String, Value>,
    current_price: Option<f64>,
    current_ts: Option<&DateTime<Utc>>,
    kline_derived: &Value,
    raw_price_structures: &[Value],
) -> Vec<BracketCandidate> {
    let mut candidates = Vec::new();
    let mut seen = BTreeSet::new();

    for structure in raw_price_structures.iter().filter_map(Value::as_object) {
        let Some(kind) = structure.get("source_kind").and_then(Value::as_str) else {
            continue;
        };
        let Some(scope) = structure.get("source_window").and_then(Value::as_str) else {
            continue;
        };
        let Some(support) = structure.get("price_low").and_then(Value::as_f64) else {
            continue;
        };
        let Some(resistance) = structure.get("price_high").and_then(Value::as_f64) else {
            continue;
        };

        let Some((candidate_kind, source_indicator)) = structure_kind_to_bracket_kind(kind) else {
            continue;
        };
        let candidate = BracketCandidate {
            scopes: vec![scope.to_string()],
            kind: candidate_kind.to_string(),
            source_indicators: vec![source_indicator.to_string()],
            reference_ts: structure
                .get("reference_ts")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            support,
            resistance,
            support_evidence: vec![format!("{kind} support {:.2}", support)],
            resistance_evidence: vec![format!("{kind} resistance {:.2}", resistance)],
        };
        insert_bracket_candidate(&mut candidates, &mut seen, candidate);
    }

    if let Some(by_window) = kline_derived.as_object() {
        for scope in SCAN_WINDOWS {
            let Some(window) = by_window.get(*scope).and_then(Value::as_object) else {
                continue;
            };
            let Some(support) = window.get("swing_low").and_then(Value::as_f64) else {
                continue;
            };
            let Some(resistance) = window.get("swing_high").and_then(Value::as_f64) else {
                continue;
            };
            let candidate = BracketCandidate {
                scopes: vec![(*scope).to_string()],
                kind: "recent_swing_bracket".to_string(),
                source_indicators: vec!["kline_history".to_string()],
                reference_ts: current_ts.map(DateTime::<Utc>::to_rfc3339),
                support,
                resistance,
                support_evidence: vec![format!("recent swing low {:.2}", support)],
                resistance_evidence: vec![format!("recent swing high {:.2}", resistance)],
            };
            insert_bracket_candidate(&mut candidates, &mut seen, candidate);
        }
    }

    if let Some(by_window) = extract_indicator_payload(source, "fvg")
        .and_then(|payload| payload.get("by_window"))
        .and_then(Value::as_object)
    {
        for scope in SCAN_WINDOWS {
            let Some(window) = by_window.get(*scope).and_then(Value::as_object) else {
                continue;
            };
            for (field, label) in [
                ("nearest_bull_fvg", "bull_fvg"),
                ("nearest_bear_fvg", "bear_fvg"),
            ] {
                let Some(fvg) = window.get(field).and_then(Value::as_object) else {
                    continue;
                };
                let support = fvg
                    .get("lower")
                    .or_else(|| fvg.get("fvg_bottom"))
                    .and_then(Value::as_f64);
                let resistance = fvg
                    .get("upper")
                    .or_else(|| fvg.get("fvg_top"))
                    .and_then(Value::as_f64);
                let (Some(support), Some(resistance)) = (support, resistance) else {
                    continue;
                };
                let candidate = BracketCandidate {
                    scopes: vec![(*scope).to_string()],
                    kind: "active_imbalance_bracket".to_string(),
                    source_indicators: vec!["fvg".to_string()],
                    reference_ts: current_ts.map(DateTime::<Utc>::to_rfc3339),
                    support,
                    resistance,
                    support_evidence: vec![format!("{label} lower {:.2}", support)],
                    resistance_evidence: vec![format!("{label} upper {:.2}", resistance)],
                };
                insert_bracket_candidate(&mut candidates, &mut seen, candidate);
            }
        }
    }

    if let Some(by_session) = extract_indicator_payload(source, "tpo_market_profile")
        .and_then(|payload| payload.get("by_session"))
        .and_then(Value::as_object)
    {
        for scope in collect_available_windows(by_session) {
            let Some(session) = by_session.get(&scope).and_then(Value::as_object) else {
                continue;
            };
            let support = session.get("initial_balance_low").and_then(Value::as_f64);
            let resistance = session.get("initial_balance_high").and_then(Value::as_f64);
            let (Some(support), Some(resistance)) = (support, resistance) else {
                continue;
            };
            let candidate = BracketCandidate {
                scopes: vec![scope],
                kind: "session_ib_bracket".to_string(),
                source_indicators: vec!["tpo_market_profile".to_string()],
                reference_ts: current_ts.map(DateTime::<Utc>::to_rfc3339),
                support,
                resistance,
                support_evidence: vec![format!("initial balance low {:.2}", support)],
                resistance_evidence: vec![format!("initial balance high {:.2}", resistance)],
            };
            insert_bracket_candidate(&mut candidates, &mut seen, candidate);
        }
    }

    attach_absorption_evidence(&mut candidates, source, current_price, current_ts);
    candidates.sort_by(|left, right| {
        bracket_distance_to_current(left, current_price)
            .partial_cmp(&bracket_distance_to_current(right, current_price))
            .unwrap_or(Ordering::Equal)
    });
    candidates
}

fn insert_bracket_candidate(
    target: &mut Vec<BracketCandidate>,
    seen: &mut BTreeSet<String>,
    candidate: BracketCandidate,
) {
    let key = format!(
        "{}:{}:{:.4}:{:.4}",
        candidate
            .scopes
            .first()
            .map(String::as_str)
            .unwrap_or_default(),
        candidate.kind,
        candidate.support,
        candidate.resistance
    );
    if seen.insert(key) {
        target.push(candidate);
    }
}

fn merge_bracket_candidates(candidates: Vec<BracketCandidate>) -> Vec<BracketCandidate> {
    let mut merged: BTreeMap<String, BracketCandidate> = BTreeMap::new();

    for candidate in candidates {
        let key = format!(
            "{}:{:.4}:{:.4}",
            candidate.kind, candidate.support, candidate.resistance
        );
        if let Some(existing) = merged.get_mut(&key) {
            extend_unique_strings(&mut existing.scopes, candidate.scopes);
            existing.scopes.sort_by_key(|scope| scope_sort_key(scope));
            extend_unique_strings(&mut existing.source_indicators, candidate.source_indicators);
            merge_option_timestamp_max(&mut existing.reference_ts, candidate.reference_ts);
            extend_unique_strings(&mut existing.support_evidence, candidate.support_evidence);
            extend_unique_strings(
                &mut existing.resistance_evidence,
                candidate.resistance_evidence,
            );
        } else {
            let mut candidate = candidate;
            candidate.scopes.sort_by_key(|scope| scope_sort_key(scope));
            merged.insert(key, candidate);
        }
    }

    merged.into_values().collect()
}

fn extend_unique_strings(target: &mut Vec<String>, mut incoming: Vec<String>) {
    for item in incoming.drain(..) {
        if !target.iter().any(|existing| existing == &item) {
            target.push(item);
        }
    }
}

fn merge_option_timestamp_max(target: &mut Option<String>, incoming: Option<String>) {
    match (target.as_deref(), incoming.as_deref()) {
        (None, Some(_)) => *target = incoming,
        (Some(existing), Some(candidate)) => {
            let existing_ts = parse_rfc3339_utc(existing);
            let candidate_ts = parse_rfc3339_utc(candidate);
            if candidate_ts > existing_ts {
                *target = incoming;
            }
        }
        _ => {}
    }
}

fn scope_sort_key(scope: &str) -> usize {
    SCAN_CONTEXT_WINDOWS
        .iter()
        .position(|candidate| *candidate == scope)
        .unwrap_or(SCAN_CONTEXT_WINDOWS.len())
}

fn structure_kind_to_bracket_kind(kind: &str) -> Option<(&'static str, &'static str)> {
    match kind {
        "pvs_value_area" => Some(("pvs_value_area", "price_volume_structure")),
        "tpo_value_area" => Some(("tpo_value_area", "tpo_market_profile")),
        "initial_balance" => Some(("session_ib_bracket", "tpo_market_profile")),
        "rvwap_sigma1" | "rvwap_sigma2" => Some(("active_imbalance_bracket", "rvwap_sigma_bands")),
        "fvg_boundary" => Some(("active_imbalance_bracket", "fvg")),
        "liquidation_density_zone" => Some(("liquidation_bracket", "liquidation_density")),
        "orderbook_wall_band" => Some(("wall_bracket", "orderbook_depth")),
        _ => None,
    }
}

fn attach_absorption_evidence(
    candidates: &mut [BracketCandidate],
    source: &Map<String, Value>,
    current_price: Option<f64>,
    current_ts: Option<&DateTime<Utc>>,
) {
    let absorption_events = collect_absorption_events(source, current_price, current_ts);
    if absorption_events.is_empty() {
        return;
    }

    for candidate in candidates {
        if let Some(event) =
            nearest_absorption_event(&absorption_events, "bullish", candidate.support)
        {
            let evidence = format_absorption_evidence("support", event);
            if !candidate
                .support_evidence
                .iter()
                .any(|item| item == &evidence)
            {
                candidate.support_evidence.push(evidence);
            }
        }
        if let Some(event) =
            nearest_absorption_event(&absorption_events, "bearish", candidate.resistance)
        {
            let evidence = format_absorption_evidence("resistance", event);
            if !candidate
                .resistance_evidence
                .iter()
                .any(|item| item == &evidence)
            {
                candidate.resistance_evidence.push(evidence);
            }
        }
    }
}

fn collect_absorption_events(
    source: &Map<String, Value>,
    current_price: Option<f64>,
    current_ts: Option<&DateTime<Utc>>,
) -> Vec<NormalizedEvent> {
    let mut specific = Vec::new();
    specific.extend(collect_events_from_indicator(
        source,
        "bullish_absorption",
        current_price,
        current_ts,
    ));
    specific.extend(collect_events_from_indicator(
        source,
        "bearish_absorption",
        current_price,
        current_ts,
    ));
    if specific.is_empty() {
        collect_events_from_indicator(source, "absorption", current_price, current_ts)
    } else {
        specific
    }
}

fn nearest_absorption_event<'a>(
    events: &'a [NormalizedEvent],
    direction: &str,
    reference_price: f64,
) -> Option<&'a NormalizedEvent> {
    events
        .iter()
        .filter(|event| event.direction == direction)
        .filter_map(|event| {
            let level = absorption_event_level(event)?;
            let pct = ((level - reference_price).abs() / reference_price.abs().max(1e-9)) * 100.0;
            (pct <= 1.0).then_some((pct, event))
        })
        .min_by(|left, right| left.0.partial_cmp(&right.0).unwrap_or(Ordering::Equal))
        .map(|(_, event)| event)
}

fn absorption_event_level(event: &NormalizedEvent) -> Option<f64> {
    event
        .pivot_price
        .or_else(|| match (event.price_low, event.price_high) {
            (Some(low), Some(high)) => Some((low + high) / 2.0),
            (Some(low), None) => Some(low),
            (None, Some(high)) => Some(high),
            _ => None,
        })
}

fn format_absorption_evidence(side: &str, event: &NormalizedEvent) -> String {
    let level = absorption_event_level(event)
        .map(round2)
        .unwrap_or_default();
    match event.age_minutes {
        Some(age) => format!("{} {} {:.2} ({}m ago)", event.event_type, side, level, age),
        None => format!("{} {} {:.2}", event.event_type, side, level),
    }
}

fn bracket_candidate_to_value(candidate: &BracketCandidate, current_price: Option<f64>) -> Value {
    let mid_price = (candidate.support + candidate.resistance) / 2.0;
    let current_inside = current_price
        .map(|price| candidate.support <= price && price <= candidate.resistance)
        .unwrap_or(false);
    json!({
        "scopes": candidate.scopes,
        "kind": candidate.kind,
        "source_indicators": candidate.source_indicators,
        "reference_ts": candidate.reference_ts,
        "support": round2(candidate.support),
        "resistance": round2(candidate.resistance),
        "width_pct": band_width_pct(candidate.support, candidate.resistance),
        "current_inside": current_inside,
        "mid_price": round2(mid_price),
        "distance_to_support_pct": pct_delta(current_price, Some(candidate.support)).map(round2),
        "distance_to_resistance_pct": pct_delta(current_price, Some(candidate.resistance)).map(round2),
        "support_evidence": candidate.support_evidence,
        "resistance_evidence": candidate.resistance_evidence,
    })
}

fn band_width_pct(low: f64, high: f64) -> Option<f64> {
    let mid = (low + high) / 2.0;
    if mid.abs() <= f64::EPSILON {
        None
    } else {
        Some(round2((high - low) / mid * 100.0))
    }
}

fn bracket_distance_to_current(candidate: &BracketCandidate, current_price: Option<f64>) -> f64 {
    let Some(price) = current_price else {
        return f64::INFINITY;
    };
    if candidate.support <= price && price <= candidate.resistance {
        0.0
    } else if price < candidate.support {
        ((candidate.support - price) / price.abs().max(1e-9)) * 100.0
    } else {
        ((price - candidate.resistance) / price.abs().max(1e-9)) * 100.0
    }
}

fn build_structure_nodes_near_current(
    raw_price_structures: &[Value],
    current_price: Option<f64>,
) -> Value {
    let Some(current_price) = current_price else {
        return json!({"above": [], "below": []});
    };

    let mut above = Vec::new();
    let mut below = Vec::new();

    for structure in raw_price_structures.iter().filter_map(Value::as_object) {
        let Some(price_low) = structure.get("price_low").and_then(Value::as_f64) else {
            continue;
        };
        let Some(price_high) = structure.get("price_high").and_then(Value::as_f64) else {
            continue;
        };
        let position =
            classify_price_position(Some(current_price), Some(price_low), Some(price_high));
        if position == "inside" {
            continue;
        }
        let simplified = simplify_structure_node(structure, current_price, position);
        if position == "above" {
            above.push(simplified);
        } else if position == "below" {
            below.push(simplified);
        }
    }

    above.sort_by(compare_distance_field);
    below.sort_by(compare_distance_field);

    json!({
        "above": limit_values(above, STRUCTURE_NODE_LIMIT_PER_SIDE),
        "below": limit_values(below, STRUCTURE_NODE_LIMIT_PER_SIDE),
    })
}

fn simplify_structure_node(
    structure: &Map<String, Value>,
    current_price: f64,
    position_vs_current: &str,
) -> Value {
    let price_low = structure.get("price_low").and_then(Value::as_f64);
    let price_high = structure.get("price_high").and_then(Value::as_f64);
    let mid_price = structure
        .get("price_mid")
        .and_then(Value::as_f64)
        .or_else(|| match (price_low, price_high) {
            (Some(low), Some(high)) => Some((low + high) / 2.0),
            _ => None,
        });

    json!({
        "scope": structure.get("source_window").and_then(Value::as_str),
        "kind": structure.get("source_kind").and_then(Value::as_str),
        "source_indicator": source_indicator_for_structure_kind(
            structure.get("source_kind").and_then(Value::as_str).unwrap_or_default()
        ),
        "reference_ts": structure.get("reference_ts").and_then(Value::as_str),
        "price_low": price_low.map(round2),
        "price_high": price_high.map(round2),
        "mid_price": mid_price.map(round2),
        "distance_to_current_pct": mid_price
            .and_then(|mid| pct_delta(Some(mid), Some(current_price)))
            .map(round2),
        "position_vs_current": position_vs_current,
        "status": structure
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or("active"),
    })
}

fn source_indicator_for_structure_kind(kind: &str) -> &'static str {
    match kind {
        "pvs_value_area" | "pvs_value_cluster" => "price_volume_structure",
        "tpo_value_area" | "initial_balance" | "tpo_single_print_zone" => "tpo_market_profile",
        "recent_swing_bracket" => "kline_history",
        "rvwap_sigma1" | "rvwap_sigma2" => "rvwap_sigma_bands",
        "fvg_boundary" => "fvg",
        "footprint_price_cluster" => "footprint",
        "orderbook_depth_cluster" | "orderbook_wall_band" => "orderbook_depth",
        "liquidation_density_zone" => "liquidation_density",
        "absorption_zone" => "absorption",
        _ => "unknown",
    }
}

fn compare_distance_field(left: &Value, right: &Value) -> Ordering {
    let left_dist = left
        .get("distance_to_current_pct")
        .and_then(Value::as_f64)
        .map(f64::abs)
        .unwrap_or(f64::INFINITY);
    let right_dist = right
        .get("distance_to_current_pct")
        .and_then(Value::as_f64)
        .map(f64::abs)
        .unwrap_or(f64::INFINITY);
    left_dist
        .partial_cmp(&right_dist)
        .unwrap_or(Ordering::Equal)
}

fn limit_values(mut values: Vec<Value>, limit: usize) -> Vec<Value> {
    values.truncate(limit);
    values
}

fn build_current_flow_snapshot(source: &Map<String, Value>, current_price: Option<f64>) -> Value {
    let cvd = extract_indicator_payload(source, "cvd_pack")
        .map(|payload| {
            json!({
                "delta_fut": payload.get("delta_fut").and_then(Value::as_f64),
                "delta_spot": payload.get("delta_spot").and_then(Value::as_f64),
                "relative_delta_fut": payload.get("relative_delta_fut").and_then(Value::as_f64),
                "relative_delta_spot": payload.get("relative_delta_spot").and_then(Value::as_f64),
                "likely_driver": payload.get("likely_driver").and_then(Value::as_str),
                "spot_flow_dominance": payload.get("spot_flow_dominance").cloned().unwrap_or(Value::Null),
                "window_latest": build_cvd_latest_by_window(payload),
            })
        })
        .unwrap_or(Value::Null);
    let orderbook = extract_indicator_payload(source, "orderbook_depth")
        .map(|payload| build_current_orderbook_snapshot(payload, current_price))
        .unwrap_or(Value::Null);
    let footprint = extract_indicator_payload(source, "footprint")
        .map(|payload| filter_footprint_with_price(&Value::Object(payload.clone()), current_price))
        .unwrap_or(Value::Null);
    let whales = extract_indicator_payload(source, "whale_trades")
        .map(|payload| filter_whale_trades(&Value::Object(payload.clone())))
        .unwrap_or(Value::Null);

    json!({
        "cvd": cvd,
        "orderbook": orderbook,
        "footprint": footprint,
        "whales": whales,
    })
}

fn build_momentum_snapshot(source: &Map<String, Value>) -> Value {
    let mut result = Map::new();
    for tf in ["15m", "4h", "1d"] {
        result.insert(
            tf.to_string(),
            build_timeframe_momentum_snapshot(source, tf),
        );
    }
    Value::Object(result)
}

fn build_timeframe_momentum_snapshot(source: &Map<String, Value>, tf: &str) -> Value {
    let mut result = Map::new();

    let closed_bars = extract_closed_futures_bars(source, tf);
    let atr14 = compute_atr14(&closed_bars);
    let recent_bars = if closed_bars.len() > 3 {
        &closed_bars[closed_bars.len() - 3..]
    } else {
        closed_bars.as_slice()
    };
    let bar_return_last_3_pct = recent_bars
        .iter()
        .filter_map(|bar| {
            let open = bar.get("open").and_then(Value::as_f64);
            let close = bar.get("close").and_then(Value::as_f64);
            pct_delta(close, open).map(round2)
        })
        .collect::<Vec<_>>();
    result.insert(
        "bar_return_last_3_pct".to_string(),
        Value::Array(bar_return_last_3_pct.into_iter().map(Value::from).collect()),
    );
    if let (Some(oldest_open), Some(latest_close)) = (
        recent_bars
            .first()
            .and_then(|bar| bar.get("open"))
            .and_then(Value::as_f64),
        recent_bars
            .last()
            .and_then(|bar| bar.get("close"))
            .and_then(Value::as_f64),
    ) {
        if let Some(value) = pct_delta(Some(latest_close), Some(oldest_open)).map(round2) {
            result.insert("bar_return_sum_last_3_pct".to_string(), Value::from(value));
        }
    }

    let last_closed_bar_range_vs_atr = closed_bars.last().and_then(|bar| {
        let high = bar.get("high").and_then(Value::as_f64);
        let low = bar.get("low").and_then(Value::as_f64);
        match (high, low, atr14) {
            (Some(high), Some(low), Some(atr14)) if atr14.abs() > f64::EPSILON => {
                Some(round2((high - low) / atr14))
            }
            _ => None,
        }
    });
    result.insert(
        "last_closed_bar_range_vs_atr".to_string(),
        last_closed_bar_range_vs_atr
            .map(Value::from)
            .unwrap_or(Value::Null),
    );

    let last_closed_bar_close_location_pct = closed_bars.last().and_then(|bar| {
        let low = bar.get("low").and_then(Value::as_f64);
        let high = bar.get("high").and_then(Value::as_f64);
        let close = bar.get("close").and_then(Value::as_f64);
        let range = match (high, low) {
            (Some(high), Some(low)) if (high - low).abs() > f64::EPSILON => Some(high - low),
            _ => None,
        };
        match (close, low, range) {
            (Some(close), Some(low), Some(range)) => Some(round2((close - low) / range * 100.0)),
            _ => None,
        }
    });
    result.insert(
        "last_closed_bar_close_location_pct".to_string(),
        last_closed_bar_close_location_pct
            .map(Value::from)
            .unwrap_or(Value::Null),
    );

    let cvd_delta_last_3 = extract_indicator_payload(source, "cvd_pack")
        .and_then(|payload| payload.get("by_window").and_then(Value::as_object))
        .and_then(|by_window| by_window.get(tf).and_then(Value::as_object))
        .and_then(|window| window.get("series").and_then(Value::as_array))
        .map(|series| {
            let start = series.len().saturating_sub(3);
            series[start..]
                .iter()
                .filter_map(|entry| entry.get("delta_fut").and_then(Value::as_f64))
                .map(round2)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    result.insert(
        "cvd_delta_last_3".to_string(),
        Value::Array(cvd_delta_last_3.iter().copied().map(Value::from).collect()),
    );
    if cvd_delta_last_3.len() >= 2 {
        let last = cvd_delta_last_3[cvd_delta_last_3.len() - 1];
        let prev = cvd_delta_last_3[cvd_delta_last_3.len() - 2];
        result.insert("cvd_slope".to_string(), Value::from(round2(last - prev)));
    }

    if let Some(whale_delta_notional) = extract_indicator_payload(source, "whale_trades")
        .and_then(|payload| payload.get("by_window").and_then(Value::as_object))
        .and_then(|by_window| by_window.get(tf).and_then(Value::as_object))
        .and_then(|window| {
            window
                .get("fut_whale_delta_notional")
                .and_then(Value::as_f64)
        })
        .map(round2)
    {
        result.insert(
            "whale_delta_notional".to_string(),
            Value::from(whale_delta_notional),
        );
    }

    Value::Object(result)
}

fn build_current_orderbook_snapshot(
    payload: &Map<String, Value>,
    current_price: Option<f64>,
) -> Value {
    let filtered = filter_orderbook_depth(&Value::Object(payload.clone()), current_price);
    let mut result = filtered.as_object().cloned().unwrap_or_default();
    if let Some(by_window) = payload.get("by_window").and_then(Value::as_object) {
        let mut current_window = Map::new();
        if let Some(window_15m) = by_window.get("15m").and_then(Value::as_object) {
            copy_fields(
                &mut current_window,
                window_15m,
                &[
                    "obi_fut",
                    "spread_twa_fut",
                    "microprice_adj_fut",
                    "topk_depth_twa_fut",
                    "ofi_norm_fut",
                ],
            );
        }
        if !current_window.is_empty() {
            result.insert(
                "current_window_15m".to_string(),
                Value::Object(current_window),
            );
        }
    }
    Value::Object(result)
}

fn build_cvd_latest_by_window(payload: &Map<String, Value>) -> Value {
    let mut latest = Map::new();
    let Some(by_window) = payload.get("by_window").and_then(Value::as_object) else {
        return Value::Object(latest);
    };
    for scope in SCAN_WINDOWS {
        let Some(entry) = by_window.get(*scope).and_then(Value::as_object) else {
            continue;
        };
        let Some(series) = entry.get("series").and_then(Value::as_array) else {
            continue;
        };
        let Some(last) = series.last().and_then(Value::as_object) else {
            continue;
        };
        latest.insert(
            (*scope).to_string(),
            object_subset_value(
                last,
                &[
                    "ts",
                    "close_fut",
                    "close_spot",
                    "delta_fut",
                    "delta_spot",
                    "relative_delta_fut",
                    "relative_delta_spot",
                    "spot_flow_dominance",
                    "xmk_delta_gap_s_minus_f",
                ],
            ),
        );
    }
    Value::Object(latest)
}

fn build_current_volume_nodes(source: &Map<String, Value>) -> Value {
    let high_volume_pulse = extract_indicator_payload(source, "high_volume_pulse")
        .map(|payload| filter_high_volume_pulse(&Value::Object(payload.clone())))
        .unwrap_or(Value::Null);
    json!({
        "high_volume_pulse": high_volume_pulse,
    })
}

fn build_path_newest_to_oldest(source: &Map<String, Value>) -> Value {
    json!({
        "latest_15m_detail": build_path_block(source, "15m", PATH_15M_DETAIL_BARS, true),
        "latest_4h_context": build_path_block(source, "4h", PATH_4H_CONTEXT_BARS, true),
        "latest_1d_background": build_path_block(source, "1d", PATH_1D_BACKGROUND_BARS, true),
    })
}

fn build_path_block(
    source: &Map<String, Value>,
    tf: &str,
    limit: usize,
    include_partial_bar: bool,
) -> Value {
    let bars = extract_futures_bars(source, tf);
    let mut closed_bars = extract_closed_futures_bars(source, tf);
    let atr14 = compute_atr14(&closed_bars);
    let current_partial_bar = if include_partial_bar {
        bars.last()
            .filter(|bar| {
                !bar.get("is_closed")
                    .and_then(Value::as_bool)
                    .unwrap_or(true)
            })
            .map(compact_bar_with_derived)
            .unwrap_or(Value::Null)
    } else {
        Value::Null
    };

    if closed_bars.len() > limit {
        let keep_from = closed_bars.len() - limit;
        closed_bars = closed_bars.split_off(keep_from);
    }
    let newest_to_oldest = closed_bars
        .iter()
        .rev()
        .map(compact_bar_with_derived)
        .collect::<Vec<_>>();

    json!({
        "summary": build_path_summary(&closed_bars, atr14),
        "current_partial_bar": current_partial_bar,
        "bars_newest_to_oldest": newest_to_oldest,
    })
}

fn build_path_summary(bars: &[Map<String, Value>], atr14: Option<f64>) -> Value {
    let Some(oldest) = bars.first() else {
        return json!({
            "bars_count": 0,
            "oldest_open": Value::Null,
            "latest_close": Value::Null,
            "atr14": Value::Null,
            "atr14_pct": Value::Null,
            "net_change_pct": Value::Null,
            "range_high": Value::Null,
            "range_low": Value::Null,
            "range_pct": Value::Null,
        });
    };
    let Some(latest) = bars.last() else {
        return Value::Null;
    };

    let oldest_open = oldest.get("open").and_then(Value::as_f64);
    let latest_close = latest.get("close").and_then(Value::as_f64);
    let range_high = bars
        .iter()
        .filter_map(|bar| bar.get("high").and_then(Value::as_f64))
        .max_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    let range_low = bars
        .iter()
        .filter_map(|bar| bar.get("low").and_then(Value::as_f64))
        .min_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    let range_pct = match (range_high, range_low, oldest_open) {
        (Some(high), Some(low), Some(open)) if open.abs() > f64::EPSILON => {
            Some(round2((high - low) / open * 100.0))
        }
        _ => None,
    };
    let atr14_pct = match (atr14, latest_close) {
        (Some(atr14), Some(close)) if close.abs() > f64::EPSILON => {
            Some(round2(atr14 / close * 100.0))
        }
        _ => None,
    };

    json!({
        "bars_count": bars.len(),
        "oldest_open": oldest_open,
        "latest_close": latest_close,
        "atr14": atr14.map(round2),
        "atr14_pct": atr14_pct,
        "net_change_pct": pct_delta(latest_close, oldest_open).map(round2),
        "range_high": range_high,
        "range_low": range_low,
        "range_pct": range_pct,
    })
}

fn compact_bar_with_derived(bar: &Map<String, Value>) -> Value {
    let open = bar.get("open").and_then(Value::as_f64);
    let high = bar.get("high").and_then(Value::as_f64);
    let low = bar.get("low").and_then(Value::as_f64);
    let close = bar.get("close").and_then(Value::as_f64);
    let bar_range = match (high, low) {
        (Some(high), Some(low)) if (high - low).abs() > f64::EPSILON => Some(high - low),
        _ => None,
    };
    let close_location_pct = match (close, low, bar_range) {
        (Some(close), Some(low), Some(range)) => Some(round2((close - low) / range * 100.0)),
        _ => None,
    };
    let upper_wick_pct_of_range = match (open, close, high, bar_range) {
        (Some(open), Some(close), Some(high), Some(range)) => {
            Some(round2((high - open.max(close)) / range * 100.0))
        }
        _ => None,
    };
    let lower_wick_pct_of_range = match (open, close, low, bar_range) {
        (Some(open), Some(close), Some(low), Some(range)) => {
            Some(round2((open.min(close) - low) / range * 100.0))
        }
        _ => None,
    };
    let range_pct = match (high, low, open) {
        (Some(high), Some(low), Some(open)) if open.abs() > f64::EPSILON => {
            Some(round2((high - low) / open * 100.0))
        }
        _ => None,
    };

    json!({
        "ts": bar
            .get("open_time")
            .or_else(|| bar.get("ts"))
            .cloned()
            .unwrap_or(Value::Null),
        "open": open,
        "high": high,
        "low": low,
        "close": close,
        "volume_base": bar.get("volume_base").and_then(Value::as_f64),
        "is_closed": bar.get("is_closed").and_then(Value::as_bool),
        "net_return_pct": pct_delta(close, open).map(round2),
        "close_location_pct": close_location_pct,
        "upper_wick_pct_of_range": upper_wick_pct_of_range,
        "lower_wick_pct_of_range": lower_wick_pct_of_range,
        "range_pct": range_pct,
    })
}

fn extract_closed_futures_bars(source: &Map<String, Value>, tf: &str) -> Vec<Map<String, Value>> {
    extract_futures_bars(source, tf)
        .into_iter()
        .filter(|bar| {
            bar.get("is_closed")
                .and_then(Value::as_bool)
                .unwrap_or(true)
        })
        .collect()
}

fn build_events_newest_to_oldest(
    source: &Map<String, Value>,
    current_price: Option<f64>,
    current_ts: Option<&DateTime<Utc>>,
) -> Value {
    let mut events = Vec::new();

    let mut specific_absorption = Vec::new();
    specific_absorption.extend(collect_events_from_indicator(
        source,
        "bullish_absorption",
        current_price,
        current_ts,
    ));
    specific_absorption.extend(collect_events_from_indicator(
        source,
        "bearish_absorption",
        current_price,
        current_ts,
    ));
    if specific_absorption.is_empty() {
        events.extend(collect_events_from_indicator(
            source,
            "absorption",
            current_price,
            current_ts,
        ));
    } else {
        events.extend(specific_absorption);
    }

    let mut specific_initiation = Vec::new();
    specific_initiation.extend(collect_events_from_indicator(
        source,
        "bullish_initiation",
        current_price,
        current_ts,
    ));
    specific_initiation.extend(collect_events_from_indicator(
        source,
        "bearish_initiation",
        current_price,
        current_ts,
    ));
    if specific_initiation.is_empty() {
        events.extend(collect_events_from_indicator(
            source,
            "initiation",
            current_price,
            current_ts,
        ));
    } else {
        events.extend(specific_initiation);
    }

    for code in ["buying_exhaustion", "selling_exhaustion", "divergence"] {
        events.extend(collect_events_from_indicator(
            source,
            code,
            current_price,
            current_ts,
        ));
    }

    events.sort_by(|left, right| {
        normalized_event_confirm_dt(right)
            .cmp(&normalized_event_confirm_dt(left))
            .then_with(|| {
                right
                    .score
                    .partial_cmp(&left.score)
                    .unwrap_or(Ordering::Equal)
            })
    });
    let deduped = dedupe_normalized_events(events);

    let mut latest_24h_detail = deduped
        .iter()
        .filter(|event| event.age_minutes.unwrap_or(i64::MAX) <= 24 * 60)
        .cloned()
        .collect::<Vec<_>>();
    sort_events_newest_to_oldest_by_display_time(&mut latest_24h_detail);

    let mut latest_7d_major = deduped
        .into_iter()
        .filter(|event| {
            let age = event.age_minutes.unwrap_or(i64::MAX);
            age > 24 * 60 && age <= 7 * 24 * 60
        })
        .collect::<Vec<_>>();
    sort_events_newest_to_oldest_by_display_time(&mut latest_7d_major);
    latest_7d_major.truncate(EVENTS_7D_MAJOR_LIMIT);

    json!({
        "latest_24h_detail": latest_24h_detail
            .into_iter()
            .map(normalized_event_to_value)
            .collect::<Vec<_>>(),
        "latest_7d_major": latest_7d_major
            .into_iter()
            .map(|event| normalized_event_to_value(event))
            .collect::<Vec<_>>(),
    })
}

fn collect_events_from_indicator(
    source: &Map<String, Value>,
    indicator_code: &str,
    current_price: Option<f64>,
    current_ts: Option<&DateTime<Utc>>,
) -> Vec<NormalizedEvent> {
    let Some(indicator_node) = source.get(indicator_code).and_then(Value::as_object) else {
        return Vec::new();
    };
    let Some(payload) = indicator_node.get("payload").and_then(Value::as_object) else {
        return Vec::new();
    };
    let events = payload
        .get("recent_7d")
        .and_then(Value::as_object)
        .and_then(|recent_7d| recent_7d.get("events"))
        .and_then(Value::as_array)
        .or_else(|| payload.get("events").and_then(Value::as_array));
    let Some(events) = events else {
        return Vec::new();
    };
    let default_scope = indicator_node
        .get("window_code")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .or_else(|| default_event_scope(indicator_code).map(ToString::to_string));
    let one_minute_bars = extract_futures_bars(source, "1m");

    events
        .iter()
        .filter_map(Value::as_object)
        .map(|event| {
            normalize_event(
                indicator_code,
                event,
                current_price,
                current_ts,
                default_scope.as_deref(),
                &one_minute_bars,
            )
        })
        .collect()
}

fn default_event_scope(indicator_code: &str) -> Option<&'static str> {
    match indicator_code {
        "selling_exhaustion" | "buying_exhaustion" | "bullish_absorption"
        | "bearish_absorption" | "bullish_initiation" | "bearish_initiation" | "divergence" => {
            Some("1m")
        }
        _ => None,
    }
}

fn normalize_event(
    indicator_code: &str,
    event: &Map<String, Value>,
    current_price: Option<f64>,
    current_ts: Option<&DateTime<Utc>>,
    default_scope: Option<&str>,
    one_minute_bars: &[Map<String, Value>],
) -> NormalizedEvent {
    let event_start_ts = event
        .get("event_start_ts")
        .or_else(|| event.get("start_ts"))
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let event_end_ts = event
        .get("event_end_ts")
        .or_else(|| event.get("end_ts"))
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let confirm_ts = event
        .get("confirm_ts")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .or_else(|| event_end_ts.clone())
        .or_else(|| event_start_ts.clone());
    let pivot_price = event
        .get("pivot_price")
        .or_else(|| event.get("price"))
        .or_else(|| event.get("event_price"))
        .and_then(Value::as_f64);
    let price_low = event
        .get("price_low")
        .or_else(|| event.get("lower"))
        .or_else(|| event.get("fvg_bottom"))
        .and_then(Value::as_f64)
        .or(pivot_price);
    let price_high = event
        .get("price_high")
        .or_else(|| event.get("upper"))
        .or_else(|| event.get("fvg_top"))
        .and_then(Value::as_f64)
        .or(pivot_price);
    let reference_ts = confirm_ts
        .as_deref()
        .or(event_end_ts.as_deref())
        .or(event_start_ts.as_deref())
        .and_then(parse_rfc3339_utc);
    let age_minutes = match (reference_ts, current_ts) {
        (Some(reference_ts), Some(now)) => {
            Some(((now.to_owned() - reference_ts).num_seconds().max(0)) / 60)
        }
        _ => None,
    };
    let direction = infer_event_direction(indicator_code, event);
    let event_type = event
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or(indicator_code)
        .to_string();
    let scope = event
        .get("scope")
        .or_else(|| event.get("source_window"))
        .or_else(|| event.get("window"))
        .or_else(|| event.get("tf"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .or_else(|| default_scope.map(ToString::to_string));
    let score = event
        .get("score")
        .or_else(|| event.get("strength_score_xmk"))
        .and_then(Value::as_f64);
    let spot_confirm = [
        "spot_confirm",
        "spot_confirm_at_birth",
        "spot_flow_confirm",
        "spot_confirmed",
        "spot_exhaustion_confirm",
        "spot_price_flow_confirm",
        "spot_break_confirm",
        "spot_whale_break_confirm",
    ]
    .iter()
    .find_map(|key| event.get(*key).and_then(Value::as_bool));
    let trigger_side = event
        .get("trigger_side")
        .or_else(|| event.get("pivot_side"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .or_else(|| derive_event_trigger_side(event));
    let (pivot_price, price_low, price_high) = backfill_event_price_fields(
        indicator_code,
        event,
        pivot_price,
        price_low,
        price_high,
        one_minute_bars,
    );
    let distance_to_current_pct = match (pivot_price, current_price) {
        (Some(pivot), Some(current)) => pct_delta(Some(pivot), Some(current)).map(round2),
        _ => None,
    };

    NormalizedEvent {
        indicator_code: indicator_code.to_string(),
        event_type,
        direction,
        scope,
        event_start_ts,
        event_end_ts,
        confirm_ts,
        age_minutes,
        pivot_price,
        price_low,
        price_high,
        score,
        spot_confirm,
        trigger_side,
        distance_to_current_pct,
        event_count: 1,
    }
}

fn derive_event_trigger_side(event: &Map<String, Value>) -> Option<String> {
    let event_type = event
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if event_type.contains("buying_exhaustion") || event_type.contains("bullish_initiation") {
        return Some("buy".to_string());
    }
    if event_type.contains("selling_exhaustion") || event_type.contains("bearish_initiation") {
        return Some("sell".to_string());
    }
    None
}

fn backfill_event_price_fields(
    indicator_code: &str,
    event: &Map<String, Value>,
    pivot_price: Option<f64>,
    price_low: Option<f64>,
    price_high: Option<f64>,
    one_minute_bars: &[Map<String, Value>],
) -> (Option<f64>, Option<f64>, Option<f64>) {
    if indicator_code != "divergence"
        || (pivot_price.is_some() && price_low.is_some() && price_high.is_some())
    {
        return (pivot_price, price_low, price_high);
    }

    let Some(event_start) = event
        .get("event_start_ts")
        .or_else(|| event.get("start_ts"))
        .and_then(Value::as_str)
        .and_then(parse_rfc3339_utc)
    else {
        return (pivot_price, price_low, price_high);
    };
    let Some(event_end) = event
        .get("event_end_ts")
        .or_else(|| event.get("end_ts"))
        .and_then(Value::as_str)
        .and_then(parse_rfc3339_utc)
    else {
        return (pivot_price, price_low, price_high);
    };

    let relevant_bars = one_minute_bars
        .iter()
        .filter(|bar| {
            bar.get("open_time")
                .and_then(Value::as_str)
                .and_then(parse_rfc3339_utc)
                .map(|open_time| open_time >= event_start && open_time <= event_end)
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    if relevant_bars.is_empty() {
        return (pivot_price, price_low, price_high);
    }

    let derived_low = relevant_bars
        .iter()
        .filter_map(|bar| bar.get("low").and_then(Value::as_f64))
        .min_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    let derived_high = relevant_bars
        .iter()
        .filter_map(|bar| bar.get("high").and_then(Value::as_f64))
        .max_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    let derived_pivot = match event.get("pivot_side").and_then(Value::as_str) {
        Some("low") => derived_low,
        Some("high") => derived_high,
        _ => pivot_price.or(derived_low).or(derived_high),
    };

    (
        pivot_price.or(derived_pivot),
        price_low.or(derived_low),
        price_high.or(derived_high),
    )
}

fn infer_event_direction(indicator_code: &str, event: &Map<String, Value>) -> String {
    if let Some(direction) = event.get("direction").and_then(Value::as_i64) {
        return if direction > 0 {
            "bullish".to_string()
        } else if direction < 0 {
            "bearish".to_string()
        } else {
            "neutral".to_string()
        };
    }

    let event_type = event
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or(indicator_code)
        .to_ascii_lowercase();
    if event_type.contains("bull") {
        return "bullish".to_string();
    }
    if event_type.contains("bear") {
        return "bearish".to_string();
    }

    if let Some(pivot_side) = event.get("pivot_side").and_then(Value::as_str) {
        return match pivot_side {
            "low" => "bullish".to_string(),
            "high" => "bearish".to_string(),
            _ => "neutral".to_string(),
        };
    }

    "neutral".to_string()
}

fn dedupe_normalized_events(events: Vec<NormalizedEvent>) -> Vec<NormalizedEvent> {
    let mut deduped = Vec::new();
    for event in events {
        if let Some(existing) = deduped
            .iter_mut()
            .find(|existing| should_merge_events(existing, &event))
        {
            merge_normalized_events(existing, event);
        } else {
            deduped.push(event);
        }
    }
    deduped
}

fn should_merge_events(existing: &NormalizedEvent, incoming: &NormalizedEvent) -> bool {
    if existing.indicator_code != incoming.indicator_code
        || existing.direction != incoming.direction
    {
        return false;
    }
    let (Some(existing_ts), Some(incoming_ts)) = (
        normalized_event_confirm_dt(existing),
        normalized_event_confirm_dt(incoming),
    ) else {
        return false;
    };
    if (existing_ts - incoming_ts).num_minutes().abs() > EVENT_DEDUPE_WINDOW_MINUTES {
        return false;
    }

    match (existing.pivot_price, incoming.pivot_price) {
        (Some(existing_price), Some(incoming_price)) => {
            let reference = existing_price.abs().max(1e-9);
            ((existing_price - incoming_price).abs() / reference) * 100.0 <= EVENT_DEDUPE_PRICE_PCT
        }
        _ => false,
    }
}

fn merge_normalized_events(existing: &mut NormalizedEvent, incoming: NormalizedEvent) {
    existing.event_count += incoming.event_count;
    existing.event_start_ts = min_ts(existing.event_start_ts.take(), incoming.event_start_ts);
    existing.event_end_ts = max_ts(existing.event_end_ts.take(), incoming.event_end_ts);
    existing.score = match (existing.score, incoming.score) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (left, right) => left.or(right),
    };
    existing.price_low = match (existing.price_low, incoming.price_low) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (left, right) => left.or(right),
    };
    existing.price_high = match (existing.price_high, incoming.price_high) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (left, right) => left.or(right),
    };
}

fn min_ts(left: Option<String>, right: Option<String>) -> Option<String> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (left, right) => left.or(right),
    }
}

fn max_ts(left: Option<String>, right: Option<String>) -> Option<String> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (left, right) => left.or(right),
    }
}

fn normalized_event_confirm_dt(event: &NormalizedEvent) -> Option<DateTime<Utc>> {
    event
        .confirm_ts
        .as_deref()
        .or(event.event_end_ts.as_deref())
        .or(event.event_start_ts.as_deref())
        .and_then(parse_rfc3339_utc)
}

fn normalized_event_display_dt(event: &NormalizedEvent) -> Option<DateTime<Utc>> {
    event
        .event_start_ts
        .as_deref()
        .or(event.confirm_ts.as_deref())
        .or(event.event_end_ts.as_deref())
        .and_then(parse_rfc3339_utc)
}

fn sort_events_newest_to_oldest_by_display_time(events: &mut [NormalizedEvent]) {
    events.sort_by(|left, right| {
        normalized_event_display_dt(right)
            .cmp(&normalized_event_display_dt(left))
            .then_with(|| {
                right
                    .score
                    .partial_cmp(&left.score)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| {
                normalized_event_confirm_dt(right).cmp(&normalized_event_confirm_dt(left))
            })
    });
}

fn normalized_event_to_value(event: NormalizedEvent) -> Value {
    json!({
        "indicator_code": event.indicator_code,
        "event_type": event.event_type,
        "direction": event.direction,
        "scope": event.scope,
        "event_start_ts": event.event_start_ts,
        "event_end_ts": event.event_end_ts,
        "confirm_ts": event.confirm_ts,
        "age_minutes": event.age_minutes,
        "pivot_price": event.pivot_price.map(round2),
        "price_low": event.price_low.map(round2),
        "price_high": event.price_high.map(round2),
        "score": event.score.map(round3),
        "spot_confirm": event.spot_confirm,
        "trigger_side": event.trigger_side,
        "distance_to_current_pct": event.distance_to_current_pct,
        "event_count": event.event_count,
    })
}

fn build_supporting_context(source: &Map<String, Value>) -> Value {
    let cvd_path_snapshot = extract_indicator_payload(source, "cvd_pack")
        .map(|payload| filter_cvd_pack(&Value::Object(payload.clone())))
        .unwrap_or(Value::Null);
    let vpin = extract_indicator_payload(source, "vpin")
        .map(|payload| filter_vpin(&Value::Object(payload.clone())))
        .unwrap_or(Value::Null);
    let funding_rate = extract_indicator_payload(source, "funding_rate")
        .map(|payload| filter_funding_rate(&Value::Object(payload.clone())))
        .unwrap_or(Value::Null);
    let liquidation_density = extract_indicator_payload(source, "liquidation_density")
        .map(|payload| filter_liquidation_density(&Value::Object(payload.clone())))
        .unwrap_or(Value::Null);
    let high_volume_pulse_full = extract_indicator_payload(source, "high_volume_pulse")
        .map(|payload| filter_high_volume_pulse(&Value::Object(payload.clone())))
        .unwrap_or(Value::Null);

    json!({
        "cvd_path_snapshot": cvd_path_snapshot,
        "indicator_snapshots": {
            "vpin": vpin,
            "funding_rate": funding_rate,
            "liquidation_density": liquidation_density,
            "high_volume_pulse_full": high_volume_pulse_full,
        }
    })
}

fn build_price_structures(
    source: &Map<String, Value>,
    current_price: Option<f64>,
    current_ts: Option<&DateTime<Utc>>,
    kline_derived: &Value,
) -> Vec<Value> {
    let mut structures = Vec::new();
    let mut seen = BTreeSet::new();

    if let Some(by_window) = extract_indicator_payload(source, "price_volume_structure")
        .and_then(|payload| payload.get("by_window"))
        .and_then(Value::as_object)
    {
        for window in collect_available_windows(by_window) {
            let Some(window_value) = by_window.get(&window).and_then(Value::as_object) else {
                continue;
            };
            push_band_structure(
                &mut structures,
                &mut seen,
                format!("pvs_value_area_{window}"),
                "pvs_value_area",
                &window,
                window_value.get("val").and_then(Value::as_f64),
                window_value.get("vah").and_then(Value::as_f64),
                current_price,
                current_ts,
                structure_extras(&[
                    (
                        "poc_price",
                        window_value.get("poc_price").and_then(Value::as_f64),
                    ),
                    (
                        "poc_volume",
                        window_value.get("poc_volume").and_then(Value::as_f64),
                    ),
                    (
                        "volume_base",
                        window_value.get("bar_volume").and_then(Value::as_f64),
                    ),
                    (
                        "volume_zscore",
                        window_value.get("volume_zscore").and_then(Value::as_f64),
                    ),
                ]),
            );

            let level_candidates = window_value
                .get("value_area_levels")
                .and_then(Value::as_array)
                .map(|levels| build_value_area_peak_clusters(levels))
                .unwrap_or_default();
            let selection = select_layered_clusters(
                &level_candidates,
                current_price,
                PVS_CLUSTER_NEAR_PER_SIDE,
                PVS_CLUSTER_STRONG_PER_SIDE,
                PVS_CLUSTER_MID_PER_SIDE,
                PVS_CLUSTER_DEEP_PER_SIDE,
            );
            for (idx, cluster) in flatten_cluster_selection(selection).into_iter().enumerate() {
                push_band_structure(
                    &mut structures,
                    &mut seen,
                    format!("pvs_value_cluster_{window}_{idx}"),
                    "pvs_value_cluster",
                    &window,
                    Some(cluster.low),
                    Some(cluster.high),
                    current_price,
                    current_ts,
                    structure_extras(&[
                        ("volume_base", Some(cluster.weight)),
                        ("tick_count", Some(cluster.count as f64)),
                    ]),
                );
            }
        }
    }

    if let Some(by_session) = extract_indicator_payload(source, "tpo_market_profile")
        .and_then(|payload| payload.get("by_session"))
        .and_then(Value::as_object)
    {
        for window in collect_available_windows(by_session) {
            let Some(session) = by_session.get(&window).and_then(Value::as_object) else {
                continue;
            };
            push_band_structure(
                &mut structures,
                &mut seen,
                format!("tpo_value_area_{window}"),
                "tpo_value_area",
                &window,
                session.get("tpo_val").and_then(Value::as_f64),
                session.get("tpo_vah").and_then(Value::as_f64),
                current_price,
                current_ts,
                structure_extras(&[("poc_price", session.get("tpo_poc").and_then(Value::as_f64))]),
            );
            push_band_structure(
                &mut structures,
                &mut seen,
                format!("initial_balance_{window}"),
                "initial_balance",
                &window,
                session.get("initial_balance_low").and_then(Value::as_f64),
                session.get("initial_balance_high").and_then(Value::as_f64),
                current_price,
                current_ts,
                Map::new(),
            );
            if let Some(zones) = session
                .get("tpo_single_print_zones")
                .and_then(Value::as_array)
            {
                for (idx, zone) in zones
                    .iter()
                    .filter_map(Value::as_object)
                    .take(5)
                    .enumerate()
                {
                    push_band_structure(
                        &mut structures,
                        &mut seen,
                        format!("tpo_single_print_zone_{window}_{idx}"),
                        "tpo_single_print_zone",
                        &window,
                        zone.get("low").and_then(Value::as_f64),
                        zone.get("high").and_then(Value::as_f64),
                        current_price,
                        current_ts,
                        structure_extras(&[("score", zone.get("score").and_then(Value::as_f64))]),
                    );
                }
            }
        }
    }

    if let Some(tpo_payload) = extract_indicator_payload(source, "tpo_market_profile") {
        push_band_structure(
            &mut structures,
            &mut seen,
            "tpo_value_area_15m".to_string(),
            "tpo_value_area",
            "15m",
            tpo_payload.get("tpo_val").and_then(Value::as_f64),
            tpo_payload.get("tpo_vah").and_then(Value::as_f64),
            current_price,
            current_ts,
            structure_extras(&[(
                "poc_price",
                tpo_payload.get("tpo_poc").and_then(Value::as_f64),
            )]),
        );
        push_band_structure(
            &mut structures,
            &mut seen,
            "initial_balance_15m".to_string(),
            "initial_balance",
            "15m",
            tpo_payload
                .get("initial_balance_low")
                .and_then(Value::as_f64),
            tpo_payload
                .get("initial_balance_high")
                .and_then(Value::as_f64),
            current_price,
            current_ts,
            Map::new(),
        );
    }

    if let Some(by_window) = kline_derived.as_object() {
        for window in SCAN_WINDOWS {
            let Some(window_value) = by_window.get(*window).and_then(Value::as_object) else {
                continue;
            };
            push_band_structure(
                &mut structures,
                &mut seen,
                format!("recent_swing_bracket_{window}"),
                "recent_swing_bracket",
                window,
                window_value.get("swing_low").and_then(Value::as_f64),
                window_value.get("swing_high").and_then(Value::as_f64),
                current_price,
                current_ts,
                Map::new(),
            );
        }
    }

    if let Some(by_window) = extract_indicator_payload(source, "rvwap_sigma_bands")
        .and_then(|payload| payload.get("by_window"))
        .and_then(Value::as_object)
    {
        for window in collect_available_windows(by_window) {
            let Some(window_value) = by_window.get(&window).and_then(Value::as_object) else {
                continue;
            };
            push_band_structure(
                &mut structures,
                &mut seen,
                format!("rvwap_sigma1_{window}"),
                "rvwap_sigma1",
                &window,
                window_value
                    .get("rvwap_band_minus_1")
                    .and_then(Value::as_f64),
                window_value
                    .get("rvwap_band_plus_1")
                    .and_then(Value::as_f64),
                current_price,
                current_ts,
                structure_extras(&[(
                    "rvwap_center",
                    window_value.get("rvwap_w").and_then(Value::as_f64),
                )]),
            );
            push_band_structure(
                &mut structures,
                &mut seen,
                format!("rvwap_sigma2_{window}"),
                "rvwap_sigma2",
                &window,
                window_value
                    .get("rvwap_band_minus_2")
                    .and_then(Value::as_f64),
                window_value
                    .get("rvwap_band_plus_2")
                    .and_then(Value::as_f64),
                current_price,
                current_ts,
                structure_extras(&[(
                    "rvwap_center",
                    window_value.get("rvwap_w").and_then(Value::as_f64),
                )]),
            );
        }
    }

    if let Some(by_window) = extract_indicator_payload(source, "fvg")
        .and_then(|payload| payload.get("by_window"))
        .and_then(Value::as_object)
    {
        for window in collect_available_windows(by_window) {
            let Some(window_value) = by_window.get(&window).and_then(Value::as_object) else {
                continue;
            };
            for (field, side) in [("active_bull_fvgs", "bull"), ("active_bear_fvgs", "bear")] {
                if let Some(items) = window_value.get(field).and_then(Value::as_array) {
                    for (idx, fvg) in items
                        .iter()
                        .filter_map(Value::as_object)
                        .take(5)
                        .enumerate()
                    {
                        push_band_structure(
                            &mut structures,
                            &mut seen,
                            format!("fvg_boundary_{window}_{field}_{idx}"),
                            "fvg_boundary",
                            &window,
                            fvg.get("lower")
                                .or_else(|| fvg.get("fvg_bottom"))
                                .and_then(Value::as_f64),
                            fvg.get("upper")
                                .or_else(|| fvg.get("fvg_top"))
                                .and_then(Value::as_f64),
                            current_price,
                            current_ts,
                            structure_extras_with_text(
                                &[
                                    ("side", side),
                                    (
                                        "state",
                                        fvg.get("state")
                                            .and_then(Value::as_str)
                                            .unwrap_or_default(),
                                    ),
                                ],
                                &[
                                    (
                                        "touch_count",
                                        fvg.get("touch_count").and_then(Value::as_f64),
                                    ),
                                    ("age_bars", fvg.get("age_bars").and_then(Value::as_f64)),
                                    ("fill_pct", fvg.get("fill_pct").and_then(Value::as_f64)),
                                ],
                            ),
                        );
                    }
                }
            }
            for field in ["nearest_bull_fvg", "nearest_bear_fvg"] {
                if let Some(fvg) = window_value.get(field).and_then(Value::as_object) {
                    let side = fvg
                        .get("side")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    push_band_structure(
                        &mut structures,
                        &mut seen,
                        format!("fvg_boundary_{window}_{field}"),
                        "fvg_boundary",
                        &window,
                        fvg.get("lower")
                            .or_else(|| fvg.get("fvg_bottom"))
                            .and_then(Value::as_f64),
                        fvg.get("upper")
                            .or_else(|| fvg.get("fvg_top"))
                            .and_then(Value::as_f64),
                        current_price,
                        current_ts,
                        structure_extras_with_text(
                            &[
                                ("side", side.as_str()),
                                (
                                    "state",
                                    fvg.get("state").and_then(Value::as_str).unwrap_or_default(),
                                ),
                            ],
                            &[
                                (
                                    "touch_count",
                                    fvg.get("touch_count").and_then(Value::as_f64),
                                ),
                                ("age_bars", fvg.get("age_bars").and_then(Value::as_f64)),
                                ("fill_pct", fvg.get("fill_pct").and_then(Value::as_f64)),
                            ],
                        ),
                    );
                }
            }
        }
    }

    for event in collect_absorption_events(source, current_price, current_ts) {
        let scope = event.scope.as_deref().unwrap_or("15m");
        let reference_ts = event
            .confirm_ts
            .as_deref()
            .or(event.event_end_ts.as_deref())
            .or(event.event_start_ts.as_deref())
            .and_then(parse_rfc3339_utc);
        push_band_structure(
            &mut structures,
            &mut seen,
            format!(
                "absorption_zone_{}_{}_{}",
                scope,
                event.direction,
                event
                    .confirm_ts
                    .as_deref()
                    .or(event.event_start_ts.as_deref())
                    .unwrap_or("na")
            ),
            "absorption_zone",
            scope,
            event.price_low.or(event.pivot_price),
            event.price_high.or(event.pivot_price),
            current_price,
            reference_ts.as_ref().or(current_ts),
            structure_extras_with_text(
                &[
                    ("direction", event.direction.as_str()),
                    ("event_type", event.event_type.as_str()),
                ],
                &[
                    ("score", event.score),
                    ("age_minutes", event.age_minutes.map(|value| value as f64)),
                    ("event_count", Some(event.event_count as f64)),
                ],
            ),
        );
    }

    let footprint_filtered = source
        .get("footprint")
        .and_then(|indicator| indicator.get("payload"))
        .map(|payload| filter_footprint_with_price(payload, current_price))
        .unwrap_or(Value::Null);
    if let Some(by_window) = footprint_filtered
        .get("by_window")
        .and_then(Value::as_object)
    {
        for window in collect_available_windows(by_window) {
            let Some(window_value) = by_window.get(&window).and_then(Value::as_object) else {
                continue;
            };
            for (cluster_field, kind) in [
                ("buy_imb_clusters", "footprint_price_cluster"),
                ("sell_imb_clusters", "footprint_price_cluster"),
            ] {
                if let Some(cluster_groups) =
                    window_value.get(cluster_field).and_then(Value::as_object)
                {
                    for side in ["below", "above"] {
                        if let Some(items) = cluster_groups.get(side).and_then(Value::as_array) {
                            for (idx, item) in items.iter().filter_map(Value::as_object).enumerate()
                            {
                                let price_low = item
                                    .get("price_low")
                                    .or_else(|| item.get("p"))
                                    .and_then(Value::as_f64);
                                let price_high = item
                                    .get("price_high")
                                    .or_else(|| item.get("p"))
                                    .and_then(Value::as_f64);
                                if price_low.is_some() && price_high.is_some() {
                                    push_band_structure(
                                        &mut structures,
                                        &mut seen,
                                        format!("{kind}_{window}_{cluster_field}_{side}_{idx}"),
                                        kind,
                                        &window,
                                        price_low,
                                        price_high,
                                        current_price,
                                        current_ts,
                                        structure_extras(&[(
                                            "touch_count",
                                            item.get("n").and_then(Value::as_f64),
                                        )]),
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let orderbook_filtered = filter_orderbook_depth(
        source
            .get("orderbook_depth")
            .and_then(|indicator| indicator.get("payload"))
            .unwrap_or(&Value::Null),
        current_price,
    );
    if let Some(clusters) = orderbook_filtered
        .get("level_clusters")
        .and_then(Value::as_object)
    {
        for (field, side) in [("bid_clusters", "bid"), ("ask_clusters", "ask")] {
            if let Some(items) = clusters.get(field).and_then(Value::as_array) {
                for (idx, item) in items.iter().filter_map(Value::as_object).enumerate() {
                    let price_low = item
                        .get("price_low")
                        .or_else(|| item.get("p"))
                        .and_then(Value::as_f64);
                    let price_high = item
                        .get("price_high")
                        .or_else(|| item.get("p"))
                        .and_then(Value::as_f64);
                    if price_low.is_some() && price_high.is_some() {
                        push_band_structure(
                            &mut structures,
                            &mut seen,
                            format!("orderbook_depth_cluster_{side}_{idx}"),
                            "orderbook_depth_cluster",
                            "15m",
                            price_low,
                            price_high,
                            current_price,
                            current_ts,
                            structure_extras(&[
                                (
                                    "volume_quote",
                                    item.get("volume_quote").and_then(Value::as_f64),
                                ),
                                ("order_count", item.get("n").and_then(Value::as_f64)),
                            ]),
                        );
                    }
                }
            }
        }
    }
    if let Some(walls) = orderbook_filtered
        .get("liquidity_walls")
        .and_then(Value::as_object)
    {
        for (field, kind) in [
            ("bid_walls", "orderbook_wall_band"),
            ("ask_walls", "orderbook_wall_band"),
        ] {
            if let Some(items) = walls.get(field).and_then(Value::as_array) {
                for (idx, item) in items
                    .iter()
                    .filter_map(Value::as_object)
                    .take(5)
                    .enumerate()
                {
                    if let Some(price) = item.get("price_level").and_then(Value::as_f64) {
                        push_point_structure(
                            &mut structures,
                            &mut seen,
                            format!("{kind}_{field}_{idx}"),
                            kind,
                            "15m",
                            price,
                            current_price,
                            current_ts,
                            structure_extras(&[
                                (
                                    "volume_quote",
                                    item.get("total_liquidity").and_then(Value::as_f64),
                                ),
                                (
                                    "distance_pct",
                                    item.get("distance_pct").and_then(Value::as_f64),
                                ),
                            ]),
                        );
                    }
                }
            }
        }
    }

    if let Some(by_window) = extract_indicator_payload(source, "liquidation_density")
        .and_then(|payload| payload.get("by_window"))
        .and_then(Value::as_object)
    {
        for window in collect_available_windows(by_window) {
            let Some(window_value) = by_window.get(&window).and_then(Value::as_object) else {
                continue;
            };
            if let Some(peaks) = window_value.get("peak_levels").and_then(Value::as_array) {
                for (idx, peak) in peaks
                    .iter()
                    .filter_map(Value::as_object)
                    .take(5)
                    .enumerate()
                {
                    if let Some(price) = peak
                        .get("price_level")
                        .or_else(|| peak.get("price"))
                        .and_then(Value::as_f64)
                    {
                        push_point_structure(
                            &mut structures,
                            &mut seen,
                            format!("liquidation_density_zone_{window}_{idx}"),
                            "liquidation_density_zone",
                            &window,
                            price,
                            current_price,
                            current_ts,
                            structure_extras(&[
                                ("long_density", peak.get("long_liq").and_then(Value::as_f64)),
                                (
                                    "short_density",
                                    peak.get("short_liq").and_then(Value::as_f64),
                                ),
                                (
                                    "volume_quote",
                                    peak.get("total_liq").and_then(Value::as_f64),
                                ),
                            ]),
                        );
                    }
                }
            }
        }
    }

    structures.sort_by(|left, right| {
        structure_distance_key(left)
            .partial_cmp(&structure_distance_key(right))
            .unwrap_or(Ordering::Equal)
    });
    structures
}

fn collect_available_windows(by_window: &Map<String, Value>) -> Vec<String> {
    let mut ordered = Vec::new();
    for window in SCAN_CONTEXT_WINDOWS {
        if by_window.contains_key(*window) {
            ordered.push((*window).to_string());
        }
    }
    for key in by_window.keys() {
        if !ordered.iter().any(|existing| existing == key) {
            ordered.push(key.clone());
        }
    }
    ordered
}

fn push_band_structure(
    target: &mut Vec<Value>,
    seen: &mut BTreeSet<String>,
    structure_id: String,
    source_kind: &str,
    source_window: &str,
    price_low: Option<f64>,
    price_high: Option<f64>,
    current_price: Option<f64>,
    current_ts: Option<&DateTime<Utc>>,
    extras: Map<String, Value>,
) {
    let (Some(price_low), Some(price_high)) = (price_low, price_high) else {
        return;
    };
    if !price_low.is_finite() || !price_high.is_finite() {
        return;
    }
    if !seen.insert(structure_id.clone()) {
        return;
    }
    let low = price_low.min(price_high);
    let high = price_low.max(price_high);
    let mid = (low + high) / 2.0;
    let mut value = Map::new();
    value.insert("structure_id".to_string(), Value::String(structure_id));
    value.insert(
        "source_kind".to_string(),
        Value::String(source_kind.to_string()),
    );
    value.insert(
        "source_window".to_string(),
        Value::String(source_window.to_string()),
    );
    value.insert("price_low".to_string(), Value::from(round2(low)));
    value.insert("price_high".to_string(), Value::from(round2(high)));
    value.insert("price_mid".to_string(), Value::from(round2(mid)));
    if let Some(current_price) = current_price {
        value.insert(
            "distance_to_low".to_string(),
            Value::from(round2(low - current_price)),
        );
        value.insert(
            "distance_to_high".to_string(),
            Value::from(round2(high - current_price)),
        );
        value.insert(
            "distance_to_mid".to_string(),
            Value::from(round2(mid - current_price)),
        );
    }
    if let Some(current_ts) = current_ts {
        value.insert(
            "reference_ts".to_string(),
            Value::String(current_ts.to_rfc3339()),
        );
    }
    for (key, field) in extras {
        value.insert(key, field);
    }
    target.push(Value::Object(value));
}

fn push_point_structure(
    target: &mut Vec<Value>,
    seen: &mut BTreeSet<String>,
    structure_id: String,
    source_kind: &str,
    source_window: &str,
    price: f64,
    current_price: Option<f64>,
    current_ts: Option<&DateTime<Utc>>,
    extras: Map<String, Value>,
) {
    push_band_structure(
        target,
        seen,
        structure_id,
        source_kind,
        source_window,
        Some(price),
        Some(price),
        current_price,
        current_ts,
        extras,
    );
}

fn structure_extras(fields: &[(&str, Option<f64>)]) -> Map<String, Value> {
    let mut map = Map::new();
    for (key, value) in fields {
        if let Some(value) = value.filter(|value| value.is_finite()) {
            map.insert((*key).to_string(), Value::from(round3_or_2(*key, value)));
        }
    }
    map
}

fn structure_extras_with_text(
    text_fields: &[(&str, &str)],
    numeric_fields: &[(&str, Option<f64>)],
) -> Map<String, Value> {
    let mut map = structure_extras(numeric_fields);
    for (key, value) in text_fields {
        if !value.is_empty() {
            map.insert((*key).to_string(), Value::String((*value).to_string()));
        }
    }
    map
}

fn round3_or_2(key: &str, value: f64) -> f64 {
    if key.contains("ratio")
        || key.contains("pct")
        || key.contains("zscore")
        || key.contains("score")
        || key.contains("share")
        || key.contains("gap")
    {
        round3(value)
    } else {
        round2(value)
    }
}

fn structure_distance_key(value: &Value) -> f64 {
    value
        .get("distance_to_mid")
        .and_then(Value::as_f64)
        .map(|value| value.abs())
        .unwrap_or(f64::INFINITY)
}

fn build_raw_overflow_price_structures(
    raw_price_structures: &[Value],
    current_price: Option<f64>,
) -> Vec<Value> {
    let filtered = limit_absorption_structures_for_overflow(raw_price_structures);
    let mut zones = aggregate_raw_structure_zones(&filtered, current_price);
    zones = retain_raw_overflow_zones(zones, current_price);
    zones
        .into_iter()
        .map(|zone| raw_structure_zone_to_value(zone, current_price))
        .collect()
}

fn limit_absorption_structures_for_overflow(raw_price_structures: &[Value]) -> Vec<Value> {
    let mut retained = Vec::new();
    let mut absorption_by_bucket: BTreeMap<String, Vec<Value>> = BTreeMap::new();

    for structure in raw_price_structures {
        let Some(object) = structure.as_object() else {
            continue;
        };
        let kind = object
            .get("source_kind")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if kind != "absorption_zone" {
            retained.push(structure.clone());
            continue;
        }
        let scope = object
            .get("source_window")
            .and_then(Value::as_str)
            .unwrap_or("15m");
        let direction = object
            .get("direction")
            .and_then(Value::as_str)
            .unwrap_or("none");
        let key = format!("{scope}:{direction}");
        absorption_by_bucket
            .entry(key)
            .or_default()
            .push(structure.clone());
    }

    for (_, mut bucket) in absorption_by_bucket {
        bucket.sort_by(compare_absorption_overflow_priority);
        bucket.truncate(RAW_OVERFLOW_ABSORPTION_MAX_PER_SCOPE_DIRECTION);
        retained.extend(bucket);
    }

    retained
}

fn compare_absorption_overflow_priority(left: &Value, right: &Value) -> Ordering {
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
        .then_with(|| compare_value_timestamps_desc(left, right))
}

fn compare_value_timestamps_desc(left: &Value, right: &Value) -> Ordering {
    let left_ts = left
        .get("reference_ts")
        .and_then(Value::as_str)
        .and_then(parse_rfc3339_utc);
    let right_ts = right
        .get("reference_ts")
        .and_then(Value::as_str)
        .and_then(parse_rfc3339_utc);
    right_ts.cmp(&left_ts)
}

fn aggregate_raw_structure_zones(
    raw_price_structures: &[Value],
    current_price: Option<f64>,
) -> Vec<RawStructureZone> {
    let mut bucketed: BTreeMap<String, Vec<Value>> = BTreeMap::new();

    for structure in raw_price_structures {
        let Some(object) = structure.as_object() else {
            continue;
        };
        let Some(source_kind) = object.get("source_kind").and_then(Value::as_str) else {
            continue;
        };
        let Some(source_window) = object.get("source_window").and_then(Value::as_str) else {
            continue;
        };
        let direction = object
            .get("direction")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let side = object
            .get("side")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let state = object
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let event_type = object
            .get("event_type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let key = format!(
            "{}|{}|{}|{}|{}|{}",
            source_kind, source_window, direction, side, state, event_type
        );
        bucketed.entry(key).or_default().push(structure.clone());
    }

    let mut zones = Vec::new();
    for (_, mut bucket) in bucketed {
        bucket.sort_by(|left, right| {
            raw_structure_mid(left)
                .partial_cmp(&raw_structure_mid(right))
                .unwrap_or(Ordering::Equal)
        });

        let threshold = raw_overflow_aggregation_threshold(current_price, &bucket);
        let mut bucket_zones: Vec<RawStructureZone> = Vec::new();

        for structure in bucket {
            let Some(object) = structure.as_object() else {
                continue;
            };
            let Some(price_low) = object.get("price_low").and_then(Value::as_f64) else {
                continue;
            };
            let Some(price_high) = object.get("price_high").and_then(Value::as_f64) else {
                continue;
            };
            let source_kind = object
                .get("source_kind")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let source_window = object
                .get("source_window")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let direction = object
                .get("direction")
                .and_then(Value::as_str)
                .map(ToString::to_string);
            let side = object
                .get("side")
                .and_then(Value::as_str)
                .map(ToString::to_string);
            let state = object
                .get("state")
                .and_then(Value::as_str)
                .map(ToString::to_string);
            let event_type = object
                .get("event_type")
                .and_then(Value::as_str)
                .map(ToString::to_string);
            let price_mid = raw_structure_mid(&structure);
            let score = object
                .get("score")
                .and_then(Value::as_f64)
                .unwrap_or_default();
            let touch_count = object
                .get("touch_count")
                .and_then(Value::as_f64)
                .or_else(|| object.get("event_count").and_then(Value::as_f64))
                .unwrap_or_default();
            let reference_ts = object
                .get("reference_ts")
                .and_then(Value::as_str)
                .map(ToString::to_string);
            let representative_direction = infer_raw_structure_direction(object);

            if let Some(last) = bucket_zones.last_mut() {
                let within_threshold = price_mid
                    .zip(Some(raw_structure_zone_mid(last)))
                    .is_some_and(|(mid, zone_mid)| (mid - zone_mid).abs() <= threshold);
                if within_threshold {
                    last.count += 1;
                    last.price_low = last.price_low.min(price_low);
                    last.price_high = last.price_high.max(price_high);
                    last.max_score = last.max_score.max(score);
                    last.total_touch_count += touch_count;
                    merge_option_timestamp_max(&mut last.reference_ts_latest, reference_ts);
                    last.representative_direction = merge_representative_direction(
                        &last.representative_direction,
                        &representative_direction,
                    );
                    continue;
                }
            }

            bucket_zones.push(RawStructureZone {
                source_kind,
                source_window,
                direction,
                side,
                state,
                event_type,
                count: 1,
                price_low,
                price_high,
                max_score: score,
                total_touch_count: touch_count,
                reference_ts_latest: reference_ts,
                representative_direction,
            });
        }

        zones.extend(bucket_zones);
    }

    zones
}

fn raw_overflow_aggregation_threshold(current_price: Option<f64>, bucket: &[Value]) -> f64 {
    let reference = current_price
        .or_else(|| bucket.iter().find_map(raw_structure_mid))
        .unwrap_or(1.0)
        .abs()
        .max(1e-9);
    reference * (RAW_OVERFLOW_AGGREGATION_PCT / 100.0)
}

fn raw_structure_mid(value: &Value) -> Option<f64> {
    value.get("price_mid").and_then(Value::as_f64).or_else(|| {
        match (
            value.get("price_low").and_then(Value::as_f64),
            value.get("price_high").and_then(Value::as_f64),
        ) {
            (Some(low), Some(high)) => Some((low + high) / 2.0),
            _ => None,
        }
    })
}

fn raw_structure_zone_mid(zone: &RawStructureZone) -> f64 {
    (zone.price_low + zone.price_high) / 2.0
}

fn infer_raw_structure_direction(object: &Map<String, Value>) -> String {
    if let Some(direction) = object.get("direction").and_then(Value::as_str) {
        return normalize_direction_label(direction);
    }
    if let Some(side) = object.get("side").and_then(Value::as_str) {
        return normalize_direction_label(side);
    }
    "none".to_string()
}

fn normalize_direction_label(label: &str) -> String {
    let normalized = label.to_ascii_lowercase();
    if normalized.contains("bull") || normalized == "buy" || normalized == "bid" {
        "bullish".to_string()
    } else if normalized.contains("bear") || normalized == "sell" || normalized == "ask" {
        "bearish".to_string()
    } else {
        "none".to_string()
    }
}

fn merge_representative_direction(existing: &str, incoming: &str) -> String {
    if existing == incoming {
        return existing.to_string();
    }
    if existing == "none" {
        return incoming.to_string();
    }
    if incoming == "none" {
        return existing.to_string();
    }
    "mixed".to_string()
}

fn retain_raw_overflow_zones(
    zones: Vec<RawStructureZone>,
    current_price: Option<f64>,
) -> Vec<RawStructureZone> {
    let mut near = Vec::new();
    let mut mid = Vec::new();
    let mut far = Vec::new();

    for zone in zones {
        match classify_raw_overflow_distance_layer(&zone, current_price) {
            RawOverflowDistanceLayer::Near => near.push(zone),
            RawOverflowDistanceLayer::Mid => mid.push(zone),
            RawOverflowDistanceLayer::Far => far.push(zone),
        }
    }

    near.sort_by(|left, right| compare_near_zone_priority(left, right, current_price));
    mid.sort_by(|left, right| compare_mid_zone_priority(left, right, current_price));
    far = retain_far_zones(far, current_price);

    near.truncate(RAW_OVERFLOW_NEAR_MAX_ZONES);
    mid.truncate(RAW_OVERFLOW_MID_MAX_ZONES);

    let mut retained = Vec::new();
    retained.extend(near);
    retained.extend(mid);
    retained.extend(far);
    retained
}

#[derive(Clone, Copy)]
enum RawOverflowDistanceLayer {
    Near,
    Mid,
    Far,
}

fn classify_raw_overflow_distance_layer(
    zone: &RawStructureZone,
    current_price: Option<f64>,
) -> RawOverflowDistanceLayer {
    let distance_pct = raw_structure_zone_distance_pct(zone, current_price)
        .map(f64::abs)
        .unwrap_or_default();
    if distance_pct <= 0.30 {
        RawOverflowDistanceLayer::Near
    } else if distance_pct <= 1.00 {
        RawOverflowDistanceLayer::Mid
    } else {
        RawOverflowDistanceLayer::Far
    }
}

fn raw_structure_zone_distance_pct(
    zone: &RawStructureZone,
    current_price: Option<f64>,
) -> Option<f64> {
    pct_delta(Some(raw_structure_zone_mid(zone)), current_price).map(round2)
}

fn compare_near_zone_priority(
    left: &RawStructureZone,
    right: &RawStructureZone,
    current_price: Option<f64>,
) -> Ordering {
    let left_dist = raw_structure_zone_distance_pct(left, current_price)
        .map(f64::abs)
        .unwrap_or(f64::INFINITY);
    let right_dist = raw_structure_zone_distance_pct(right, current_price)
        .map(f64::abs)
        .unwrap_or(f64::INFINITY);
    left_dist
        .partial_cmp(&right_dist)
        .unwrap_or(Ordering::Equal)
        .then_with(|| {
            right
                .max_score
                .partial_cmp(&left.max_score)
                .unwrap_or(Ordering::Equal)
        })
        .then_with(|| {
            right
                .total_touch_count
                .partial_cmp(&left.total_touch_count)
                .unwrap_or(Ordering::Equal)
        })
        .then_with(|| right.count.cmp(&left.count))
}

fn compare_mid_zone_priority(
    left: &RawStructureZone,
    right: &RawStructureZone,
    current_price: Option<f64>,
) -> Ordering {
    right
        .max_score
        .partial_cmp(&left.max_score)
        .unwrap_or(Ordering::Equal)
        .then_with(|| {
            right
                .total_touch_count
                .partial_cmp(&left.total_touch_count)
                .unwrap_or(Ordering::Equal)
        })
        .then_with(|| right.count.cmp(&left.count))
        .then_with(|| {
            let left_dist = raw_structure_zone_distance_pct(left, current_price)
                .map(f64::abs)
                .unwrap_or(f64::INFINITY);
            let right_dist = raw_structure_zone_distance_pct(right, current_price)
                .map(f64::abs)
                .unwrap_or(f64::INFINITY);
            left_dist
                .partial_cmp(&right_dist)
                .unwrap_or(Ordering::Equal)
        })
}

fn retain_far_zones(
    zones: Vec<RawStructureZone>,
    current_price: Option<f64>,
) -> Vec<RawStructureZone> {
    let mut absorption = Vec::new();
    let mut others = Vec::new();

    for zone in zones
        .into_iter()
        .filter(|zone| raw_overflow_far_zone_allowed(zone))
    {
        if zone.source_kind == "absorption_zone" {
            if zone.max_score >= RAW_OVERFLOW_FAR_ABSORPTION_SCORE_MIN {
                absorption.push(zone);
            }
        } else {
            others.push(zone);
        }
    }

    absorption.sort_by(|left, right| {
        right
            .max_score
            .partial_cmp(&left.max_score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| compare_age_minutes_for_zone(left, right))
            .then_with(|| {
                let left_dist = raw_structure_zone_distance_pct(left, current_price)
                    .map(f64::abs)
                    .unwrap_or(f64::INFINITY);
                let right_dist = raw_structure_zone_distance_pct(right, current_price)
                    .map(f64::abs)
                    .unwrap_or(f64::INFINITY);
                left_dist
                    .partial_cmp(&right_dist)
                    .unwrap_or(Ordering::Equal)
            })
    });
    absorption.truncate(RAW_OVERFLOW_FAR_ABSORPTION_MAX_ZONES);

    others.sort_by(|left, right| compare_mid_zone_priority(left, right, current_price));

    let remaining = RAW_OVERFLOW_FAR_MAX_ZONES.saturating_sub(absorption.len());
    others.truncate(remaining);

    let mut far = Vec::new();
    far.extend(others);
    far.extend(absorption);
    far.sort_by(|left, right| compare_mid_zone_priority(left, right, current_price));
    far
}

fn raw_overflow_far_zone_allowed(zone: &RawStructureZone) -> bool {
    matches!(
        zone.source_kind.as_str(),
        "fvg_boundary"
            | "recent_swing_bracket"
            | "pvs_value_area"
            | "tpo_value_area"
            | "orderbook_wall_band"
            | "liquidation_density_zone"
            | "absorption_zone"
    )
}

fn compare_age_minutes_for_zone(left: &RawStructureZone, right: &RawStructureZone) -> Ordering {
    let left_age = left
        .reference_ts_latest
        .as_deref()
        .and_then(parse_rfc3339_utc);
    let right_age = right
        .reference_ts_latest
        .as_deref()
        .and_then(parse_rfc3339_utc);
    right_age.cmp(&left_age)
}

fn raw_structure_zone_to_value(zone: RawStructureZone, current_price: Option<f64>) -> Value {
    let mut value = Map::new();
    let price_mid = raw_structure_zone_mid(&zone);
    value.insert("source_kind".to_string(), Value::String(zone.source_kind));
    value.insert(
        "source_window".to_string(),
        Value::String(zone.source_window),
    );
    value.insert("count".to_string(), Value::from(zone.count as u64));
    value.insert("price_low".to_string(), Value::from(round2(zone.price_low)));
    value.insert(
        "price_high".to_string(),
        Value::from(round2(zone.price_high)),
    );
    value.insert("price_mid".to_string(), Value::from(round2(price_mid)));
    if let Some(distance_to_mid_pct) = pct_delta(Some(price_mid), current_price).map(round2) {
        value.insert(
            "distance_to_mid_pct".to_string(),
            Value::from(distance_to_mid_pct),
        );
    }
    if let Some(reference_ts_latest) = zone.reference_ts_latest {
        value.insert(
            "reference_ts_latest".to_string(),
            Value::String(reference_ts_latest),
        );
    }
    if let Some(direction) = zone.direction.filter(|direction| !direction.is_empty()) {
        value.insert("direction".to_string(), Value::String(direction));
    }
    if let Some(side) = zone.side.filter(|side| !side.is_empty()) {
        value.insert("side".to_string(), Value::String(side));
    }
    if let Some(state) = zone.state.filter(|state| !state.is_empty()) {
        value.insert("state".to_string(), Value::String(state));
    }
    if let Some(event_type) = zone.event_type.filter(|event_type| !event_type.is_empty()) {
        value.insert("event_type".to_string(), Value::String(event_type));
    }
    value.insert("max_score".to_string(), Value::from(round3(zone.max_score)));
    value.insert(
        "total_touch_count".to_string(),
        Value::from(round2(zone.total_touch_count)),
    );
    value.insert(
        "representative_direction".to_string(),
        Value::String(zone.representative_direction),
    );
    Value::Object(value)
}

fn build_price_levels(
    source: &Map<String, Value>,
    current_price: Option<f64>,
    current_ts: Option<&DateTime<Utc>>,
) -> Vec<Value> {
    let mut levels = Vec::new();
    let mut seen = BTreeSet::new();

    if let Some(by_window) = extract_indicator_payload(source, "price_volume_structure")
        .and_then(|payload| payload.get("by_window"))
        .and_then(Value::as_object)
    {
        for window in collect_available_windows(by_window) {
            let Some(window_value) = by_window.get(&window).and_then(Value::as_object) else {
                continue;
            };
            push_price_level(
                &mut levels,
                &mut seen,
                window_value.get("poc_price").and_then(Value::as_f64),
                current_price,
                &[format!("pvs_poc_{window}")],
                current_ts,
                level_extras(&[(
                    "poc_volume",
                    window_value.get("poc_volume").and_then(Value::as_f64),
                )]),
            );
        }
    }

    if let Some(by_session) = extract_indicator_payload(source, "tpo_market_profile")
        .and_then(|payload| payload.get("by_session"))
        .and_then(Value::as_object)
    {
        for window in collect_available_windows(by_session) {
            let Some(session) = by_session.get(&window).and_then(Value::as_object) else {
                continue;
            };
            push_price_level(
                &mut levels,
                &mut seen,
                session.get("tpo_poc").and_then(Value::as_f64),
                current_price,
                &[format!("tpo_poc_{window}")],
                current_ts,
                Map::new(),
            );
        }
    }

    if let Some(tpo_payload) = extract_indicator_payload(source, "tpo_market_profile") {
        push_price_level(
            &mut levels,
            &mut seen,
            tpo_payload.get("tpo_poc").and_then(Value::as_f64),
            current_price,
            &[String::from("tpo_poc_15m")],
            current_ts,
            Map::new(),
        );
    }

    if let Some(payload) = extract_indicator_payload(source, "ema_trend_regime") {
        for field in ["ema_13", "ema_21", "ema_34"] {
            push_price_level(
                &mut levels,
                &mut seen,
                payload.get(field).and_then(Value::as_f64),
                current_price,
                &[field.to_string()],
                current_ts,
                Map::new(),
            );
        }
        if let Some(ema_100_htf) = payload.get("ema_100_htf").and_then(Value::as_object) {
            for window in ["4h", "1d"] {
                push_price_level(
                    &mut levels,
                    &mut seen,
                    ema_100_htf.get(window).and_then(Value::as_f64),
                    current_price,
                    &[format!("ema_100_htf_{window}")],
                    current_ts,
                    Map::new(),
                );
            }
        }
        if let Some(ema_200_htf) = payload.get("ema_200_htf").and_then(Value::as_object) {
            for window in ["4h", "1d"] {
                push_price_level(
                    &mut levels,
                    &mut seen,
                    ema_200_htf.get(window).and_then(Value::as_f64),
                    current_price,
                    &[format!("ema_200_htf_{window}")],
                    current_ts,
                    Map::new(),
                );
            }
        }
    }

    if let Some(payload) = extract_indicator_payload(source, "avwap") {
        for field in ["avwap_fut", "avwap_spot"] {
            push_price_level(
                &mut levels,
                &mut seen,
                payload.get(field).and_then(Value::as_f64),
                current_price,
                &[field.to_string()],
                current_ts,
                Map::new(),
            );
        }
        if let Some(series_by_window) = payload.get("series_by_window").and_then(Value::as_object) {
            for window in ["15m", "4h", "1d"] {
                if let Some(last) = series_by_window
                    .get(window)
                    .and_then(Value::as_array)
                    .and_then(|items| items.last())
                    .and_then(Value::as_object)
                {
                    push_price_level(
                        &mut levels,
                        &mut seen,
                        last.get("avwap_fut").and_then(Value::as_f64),
                        current_price,
                        &[format!("avwap_fut_{window}")],
                        last.get("ts")
                            .and_then(Value::as_str)
                            .and_then(parse_rfc3339_utc)
                            .as_ref(),
                        Map::new(),
                    );
                }
            }
        }
    }

    if let Some(payload) = extract_indicator_payload(source, "high_volume_pulse") {
        if let Some(by_window) = payload
            .get("intrabar_poc_max_by_window")
            .and_then(Value::as_object)
        {
            for window in collect_available_windows(by_window) {
                let Some(entry) = by_window.get(&window).and_then(Value::as_object) else {
                    continue;
                };
                let ref_ts = entry
                    .get("ts")
                    .and_then(Value::as_str)
                    .and_then(parse_rfc3339_utc);
                push_price_level(
                    &mut levels,
                    &mut seen,
                    entry.get("intrabar_poc_price").and_then(Value::as_f64),
                    current_price,
                    &[format!("intrabar_poc_{window}")],
                    ref_ts.as_ref(),
                    level_extras(&[(
                        "poc_volume",
                        entry.get("intrabar_poc_volume").and_then(Value::as_f64),
                    )]),
                );
            }
        }
    }

    if let Some(payload) = extract_indicator_payload(source, "orderbook_depth") {
        if let Some(heatmap) = payload
            .get("heatmap_summary_fut")
            .and_then(Value::as_object)
        {
            for (field, source_name) in [
                ("peak_bid", "heatmap_peak_bid"),
                ("peak_ask", "heatmap_peak_ask"),
                ("peak_total", "heatmap_peak_total"),
            ] {
                if let Some(peak) = heatmap.get(field).and_then(Value::as_object) {
                    push_price_level(
                        &mut levels,
                        &mut seen,
                        peak.get("price_level").and_then(Value::as_f64),
                        current_price,
                        &[source_name.to_string()],
                        current_ts,
                        level_extras(&[(
                            "volume_quote",
                            peak.get("total_liquidity")
                                .or_else(|| peak.get("bid_liquidity"))
                                .or_else(|| peak.get("ask_liquidity"))
                                .and_then(Value::as_f64),
                        )]),
                    );
                }
            }
        }
    }

    levels.sort_by(|left, right| {
        left.get("distance_to_current")
            .and_then(Value::as_f64)
            .map(|value| value.abs())
            .unwrap_or(f64::INFINITY)
            .partial_cmp(
                &right
                    .get("distance_to_current")
                    .and_then(Value::as_f64)
                    .map(|value| value.abs())
                    .unwrap_or(f64::INFINITY),
            )
            .unwrap_or(Ordering::Equal)
    });
    levels
}

fn push_price_level(
    target: &mut Vec<Value>,
    seen: &mut BTreeSet<String>,
    price: Option<f64>,
    current_price: Option<f64>,
    sources: &[String],
    current_ts: Option<&DateTime<Utc>>,
    extras: Map<String, Value>,
) {
    let Some(price) = price.filter(|price| price.is_finite()) else {
        return;
    };
    let dedupe_key = format!("{:.2}|{}", round2(price), sources.join("|"));
    if !seen.insert(dedupe_key) {
        return;
    }
    let mut value = Map::new();
    value.insert("price".to_string(), Value::from(round2(price)));
    if let Some(current_price) = current_price {
        value.insert(
            "distance_to_current".to_string(),
            Value::from(round2(price - current_price)),
        );
    }
    value.insert(
        "source_count".to_string(),
        Value::from(sources.len() as u64),
    );
    value.insert(
        "sources".to_string(),
        Value::Array(sources.iter().cloned().map(Value::String).collect()),
    );
    if let Some(current_ts) = current_ts {
        value.insert(
            "reference_ts".to_string(),
            Value::String(current_ts.to_rfc3339()),
        );
    }
    for (key, field) in extras {
        value.insert(key, field);
    }
    target.push(Value::Object(value));
}

fn level_extras(fields: &[(&str, Option<f64>)]) -> Map<String, Value> {
    structure_extras(fields)
}

fn object_subset_value(source: &Map<String, Value>, fields: &[&str]) -> Value {
    let mut result = Map::new();
    copy_fields(&mut result, source, fields);
    result.retain(|_, value| !value.is_null());
    if result.is_empty() {
        Value::Null
    } else {
        Value::Object(result)
    }
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
    result.retain(|_, value| !value.is_null());

    if let Some(by_window) = payload.get("by_window").and_then(Value::as_object) {
        let mut filtered_windows = Map::new();
        for (window, limit) in [("15m", 12usize), ("4h", 8usize), ("1d", 5usize)] {
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
                    filtered_entry.retain(|_, value| !value.is_null());
                    Value::Object(filtered_entry)
                })
                .collect::<Vec<_>>();
            filtered_windows.insert(window.to_string(), json!({ "series": series }));
        }
        result.insert("by_window".to_string(), Value::Object(filtered_windows));
    }

    Value::Object(result)
}

fn filter_whale_trades(payload: &Value) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();
    copy_fields(&mut result, payload, &["threshold_usdt"]);
    if let Some(by_window) = payload.get("by_window").and_then(Value::as_object) {
        let mut filtered_windows = Map::new();
        for window in collect_available_windows(by_window) {
            let Some(window_value) = by_window.get(&window).and_then(Value::as_object) else {
                continue;
            };
            filtered_windows.insert(
                window,
                object_subset_value(
                    window_value,
                    &[
                        "window",
                        "fut_whale_delta_notional",
                        "spot_whale_delta_notional",
                        "xmk_whale_delta_notional_gap_s_minus_f",
                        "fut_notional_sum_usd",
                        "spot_notional_sum_usd",
                        "fut_buy_count",
                        "fut_sell_count",
                        "spot_buy_count",
                        "spot_sell_count",
                    ],
                ),
            );
        }
        result.insert("by_window".to_string(), Value::Object(filtered_windows));
    }
    Value::Object(result)
}

fn filter_vpin(payload: &Value) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();
    copy_fields(
        &mut result,
        payload,
        &[
            "vpin_bucket_size_base",
            "vpin_bucket_size_eth",
            "vpin_model",
            "vpin_unit",
            "vpin_rolling_bucket_count",
        ],
    );
    if let Some(by_window) = payload.get("by_window").and_then(Value::as_object) {
        let mut filtered_windows = Map::new();
        for window in collect_available_windows(by_window) {
            let Some(window_value) = by_window.get(&window).and_then(Value::as_object) else {
                continue;
            };
            filtered_windows.insert(
                window,
                object_subset_value(
                    window_value,
                    &[
                        "window",
                        "vpin_fut",
                        "vpin_spot",
                        "vpin_ratio_s_over_f",
                        "xmk_vpin_gap_s_minus_f",
                        "z_vpin_fut",
                        "z_vpin_spot",
                        "z_vpin_gap_s_minus_f",
                    ],
                ),
            );
        }
        result.insert("by_window".to_string(), Value::Object(filtered_windows));
    }
    Value::Object(result)
}

fn filter_high_volume_pulse(payload: &Value) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();
    copy_fields(
        &mut result,
        payload,
        &[
            "as_of_ts",
            "intrabar_poc_price",
            "intrabar_poc_volume",
            "window",
        ],
    );
    if let Some(by_window) = payload.get("by_z_window").and_then(Value::as_object) {
        let mut filtered = Map::new();
        for window in collect_available_windows(by_window) {
            let Some(entry) = by_window.get(&window).and_then(Value::as_object) else {
                continue;
            };
            filtered.insert(
                window,
                object_subset_value(
                    entry,
                    &[
                        "window_minutes",
                        "lookback_samples",
                        "rolling_volume_w",
                        "volume_spike_z_w",
                    ],
                ),
            );
        }
        result.insert("by_z_window".to_string(), Value::Object(filtered));
    }
    if let Some(by_window) = payload
        .get("intrabar_poc_max_by_window")
        .and_then(Value::as_object)
    {
        let mut filtered = Map::new();
        for window in collect_available_windows(by_window) {
            let Some(entry) = by_window.get(&window).and_then(Value::as_object) else {
                continue;
            };
            filtered.insert(
                window,
                object_subset_value(
                    entry,
                    &[
                        "intrabar_poc_price",
                        "intrabar_poc_volume",
                        "ts",
                        "window_minutes",
                    ],
                ),
            );
        }
        result.insert(
            "intrabar_poc_max_by_window".to_string(),
            Value::Object(filtered),
        );
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

fn filter_footprint_with_price(payload: &Value, current_price: Option<f64>) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = Map::new();
    let Some(by_window) = payload.get("by_window").and_then(Value::as_object) else {
        return Value::Object(result);
    };

    let mut filtered_windows = Map::new();
    for window in collect_available_windows(by_window) {
        if !matches!(window.as_str(), "15m" | "4h" | "1d" | "3d") {
            continue;
        }
        let Some(window_value) = by_window.get(&window).and_then(Value::as_object) else {
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
        if let Some(max_buy_stack_len) = window_value
            .get("max_buy_stack_len")
            .and_then(Value::as_u64)
            .or_else(|| max_stack_len(window_value.get("buy_stacks").and_then(Value::as_array)))
        {
            filtered_window.insert(
                "max_buy_stack_len".to_string(),
                Value::from(max_buy_stack_len),
            );
        }
        if let Some(max_sell_stack_len) = window_value
            .get("max_sell_stack_len")
            .and_then(Value::as_u64)
            .or_else(|| max_stack_len(window_value.get("sell_stacks").and_then(Value::as_array)))
        {
            filtered_window.insert(
                "max_sell_stack_len".to_string(),
                Value::from(max_sell_stack_len),
            );
        }
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
            .map(|prices| build_price_clusters(&collect_numeric_price_entries(prices)))
            .map(|items| {
                footprint_nearest_clusters(
                    &items,
                    selected_price,
                    FOOTPRINT_CLUSTER_NEAR_PER_SIDE,
                    FOOTPRINT_CLUSTER_STRONG_PER_SIDE,
                    FOOTPRINT_CLUSTER_MID_PER_SIDE,
                    FOOTPRINT_CLUSTER_DEEP_PER_SIDE,
                )
            })
            .unwrap_or_else(|| json!({"above": [], "below": []}));
        let sell_clusters = window_value
            .get("sell_imbalance_prices")
            .and_then(Value::as_array)
            .map(|prices| build_price_clusters(&collect_numeric_price_entries(prices)))
            .map(|items| {
                footprint_nearest_clusters(
                    &items,
                    selected_price,
                    FOOTPRINT_CLUSTER_NEAR_PER_SIDE,
                    FOOTPRINT_CLUSTER_STRONG_PER_SIDE,
                    FOOTPRINT_CLUSTER_MID_PER_SIDE,
                    FOOTPRINT_CLUSTER_DEEP_PER_SIDE,
                )
            })
            .unwrap_or_else(|| json!({"above": [], "below": []}));
        filtered_window.insert("buy_stacks".to_string(), buy_stacks);
        filtered_window.insert("sell_stacks".to_string(), sell_stacks);
        filtered_window.insert("buy_imb_clusters".to_string(), buy_clusters);
        filtered_window.insert("sell_imb_clusters".to_string(), sell_clusters);
        filtered_windows.insert(window, Value::Object(filtered_window));
    }
    result.insert("by_window".to_string(), Value::Object(filtered_windows));

    Value::Object(result)
}

fn max_stack_len(items: Option<&Vec<Value>>) -> Option<u64> {
    items.and_then(|items| {
        items
            .iter()
            .filter_map(Value::as_object)
            .filter_map(|item| item.get("length").and_then(Value::as_u64))
            .max()
    })
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

fn footprint_nearest_clusters(
    items: &[PriceClusterCandidate],
    current_price: f64,
    near_limit: usize,
    strong_limit: usize,
    mid_limit: usize,
    deep_limit: usize,
) -> Value {
    let to_compact = |item: &PriceClusterCandidate| -> Value {
        let mut compact = Map::new();
        compact.insert("p".to_string(), Value::from(round2(item.mid)));
        compact.insert("price_low".to_string(), Value::from(round2(item.low)));
        compact.insert("price_high".to_string(), Value::from(round2(item.high)));
        compact.insert("n".to_string(), Value::from(item.count as u64));
        compact.insert(
            "dist".to_string(),
            Value::from(round2(cluster_distance_to_price(item, current_price))),
        );
        Value::Object(compact)
    };
    let selection = select_layered_clusters(
        items,
        Some(current_price),
        near_limit,
        strong_limit,
        mid_limit,
        deep_limit,
    );
    json!({
        "above": selection.above.iter().map(to_compact).collect::<Vec<_>>(),
        "below": selection.below.iter().map(to_compact).collect::<Vec<_>>(),
        "cross": selection.cross.iter().map(to_compact).collect::<Vec<_>>(),
    })
}

fn filter_orderbook_depth(payload: &Value, current_price: Option<f64>) -> Value {
    let Some(payload) = payload.as_object() else {
        return Value::Null;
    };
    let mut result = clone_object_without_keys(payload, &["levels", "by_window"]);
    let mid_price = payload
        .get("microprice_fut")
        .and_then(Value::as_f64)
        .or(current_price);

    result.insert(
        "liquidity_walls".to_string(),
        build_liquidity_walls(
            payload
                .get("levels")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or(&[]),
            mid_price,
        ),
    );
    result.insert(
        "level_clusters".to_string(),
        build_orderbook_level_clusters(
            payload
                .get("levels")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or(&[]),
            mid_price,
        ),
    );

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

fn round_to_decimals(value: f64, decimals: usize) -> f64 {
    let factor = 10_f64.powi(decimals as i32);
    (value * factor).round() / factor
}

fn collect_numeric_price_entries(values: &[Value]) -> Vec<(f64, f64)> {
    values
        .iter()
        .filter_map(Value::as_f64)
        .filter(|price| price.is_finite())
        .map(|price| (price, 1.0))
        .collect()
}

fn collect_weighted_price_entries(
    values: &[Value],
    price_keys: &[&str],
    weight_key: Option<&str>,
) -> Vec<(f64, f64)> {
    values
        .iter()
        .filter_map(Value::as_object)
        .filter_map(|entry| {
            let price = price_keys
                .iter()
                .find_map(|key| entry.get(*key).and_then(Value::as_f64))?;
            let weight = weight_key
                .and_then(|key| entry.get(key).and_then(Value::as_f64))
                .unwrap_or(1.0);
            Some((price, weight))
        })
        .collect()
}

fn percentile_threshold(values: &[f64], percentile: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    if sorted.is_empty() {
        return None;
    }
    sorted.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    let clamped = percentile.clamp(0.0, 1.0);
    let index = ((sorted.len() - 1) as f64 * clamped).round() as usize;
    sorted.get(index).copied()
}

fn infer_price_tick(entries: &[(f64, f64)]) -> Option<f64> {
    let mut prices = entries
        .iter()
        .map(|(price, _)| *price)
        .filter(|price| price.is_finite())
        .collect::<Vec<_>>();
    prices.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    prices.dedup_by(|left, right| (*left - *right).abs() < 1e-9);
    prices
        .windows(2)
        .filter_map(|pair| {
            let diff = pair[1] - pair[0];
            (diff > 1e-9).then_some(diff)
        })
        .min_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal))
}

fn build_value_area_peak_clusters(levels: &[Value]) -> Vec<PriceClusterCandidate> {
    let entries = collect_weighted_price_entries(levels, &["price_level", "price"], Some("volume"));
    if entries.is_empty() {
        return Vec::new();
    }
    let mut sorted = entries
        .iter()
        .copied()
        .filter(|(price, weight)| price.is_finite() && weight.is_finite())
        .collect::<Vec<_>>();
    if sorted.is_empty() {
        return Vec::new();
    }
    sorted.sort_by(|left, right| left.0.partial_cmp(&right.0).unwrap_or(Ordering::Equal));
    let tick = infer_price_tick(&sorted).unwrap_or(0.01);
    let max_gap = (tick * CLUSTER_MERGE_TICK_MULTIPLIER).max(1e-9);
    let weights = sorted.iter().map(|(_, weight)| *weight).collect::<Vec<_>>();
    let min_peak_weight =
        percentile_threshold(&weights, PVS_PEAK_MIN_PERCENTILE).unwrap_or_default();
    let mut kept_indices = BTreeSet::new();

    for idx in 0..sorted.len() {
        let weight = sorted[idx].1.max(0.0);
        let prev_weight = if idx > 0 {
            sorted[idx - 1].1.max(0.0)
        } else {
            f64::NEG_INFINITY
        };
        let next_weight = if idx + 1 < sorted.len() {
            sorted[idx + 1].1.max(0.0)
        } else {
            f64::NEG_INFINITY
        };
        let is_local_peak = weight + 1e-9 >= prev_weight && weight + 1e-9 >= next_weight;
        if !is_local_peak || weight + 1e-9 < min_peak_weight {
            continue;
        }

        kept_indices.insert(idx);

        let mut left = idx;
        for _ in 0..PVS_PEAK_NEIGHBOR_STEPS {
            if left == 0 {
                break;
            }
            if sorted[left].0 - sorted[left - 1].0 <= max_gap + 1e-9 {
                left -= 1;
                kept_indices.insert(left);
            } else {
                break;
            }
        }

        let mut right = idx;
        for _ in 0..PVS_PEAK_NEIGHBOR_STEPS {
            if right + 1 >= sorted.len() {
                break;
            }
            if sorted[right + 1].0 - sorted[right].0 <= max_gap + 1e-9 {
                right += 1;
                kept_indices.insert(right);
            } else {
                break;
            }
        }
    }

    if kept_indices.is_empty() {
        if let Some((peak_idx, _)) = sorted.iter().enumerate().max_by(|(_, left), (_, right)| {
            left.1
                .partial_cmp(&right.1)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.0.partial_cmp(&right.0).unwrap_or(Ordering::Equal))
        }) {
            kept_indices.insert(peak_idx);
        }
    }

    let filtered = kept_indices
        .into_iter()
        .filter_map(|idx| sorted.get(idx).copied())
        .collect::<Vec<_>>();
    build_price_clusters(&filtered)
}

fn build_price_clusters(entries: &[(f64, f64)]) -> Vec<PriceClusterCandidate> {
    if entries.is_empty() {
        return Vec::new();
    }
    let mut sorted = entries
        .iter()
        .copied()
        .filter(|(price, _)| price.is_finite())
        .collect::<Vec<_>>();
    sorted.sort_by(|left, right| left.0.partial_cmp(&right.0).unwrap_or(Ordering::Equal));
    let tick = infer_price_tick(&sorted).unwrap_or(0.01);
    let max_gap = (tick * CLUSTER_MERGE_TICK_MULTIPLIER).max(1e-9);

    let mut clusters = Vec::new();
    let mut low = sorted[0].0;
    let mut high = sorted[0].0;
    let mut weighted_sum = sorted[0].0 * sorted[0].1.max(1.0);
    let mut weight_total = sorted[0].1.max(0.0);
    let mut effective_weight_total = sorted[0].1.max(1.0);
    let mut count = 1_usize;

    for (price, weight) in sorted.into_iter().skip(1) {
        let effective_weight = weight.max(1.0);
        if price - high <= max_gap + 1e-9 {
            high = price;
            weight_total += weight.max(0.0);
            effective_weight_total += effective_weight;
            weighted_sum += price * effective_weight;
            count += 1;
        } else {
            let mid = if effective_weight_total > f64::EPSILON {
                weighted_sum / effective_weight_total
            } else {
                (low + high) / 2.0
            };
            clusters.push(PriceClusterCandidate {
                low,
                high,
                mid,
                count,
                weight: weight_total,
            });
            low = price;
            high = price;
            weighted_sum = price * effective_weight;
            weight_total = weight.max(0.0);
            effective_weight_total = effective_weight;
            count = 1;
        }
    }

    let mid = if effective_weight_total > f64::EPSILON {
        weighted_sum / effective_weight_total
    } else {
        (low + high) / 2.0
    };
    clusters.push(PriceClusterCandidate {
        low,
        high,
        mid,
        count,
        weight: weight_total,
    });
    clusters
}

fn cluster_distance_to_price(cluster: &PriceClusterCandidate, current_price: f64) -> f64 {
    if cluster.low <= current_price && current_price <= cluster.high {
        0.0
    } else if cluster.high < current_price {
        current_price - cluster.high
    } else {
        cluster.low - current_price
    }
}

fn cluster_key(cluster: &PriceClusterCandidate) -> String {
    format!("{:.8}:{:.8}", cluster.low, cluster.high)
}

fn select_cluster_side(
    candidates: &[PriceClusterCandidate],
    current_price: f64,
    near_limit: usize,
    strong_limit: usize,
    mid_limit: usize,
    deep_limit: usize,
) -> Vec<PriceClusterCandidate> {
    let mut selected = Vec::new();
    let mut seen = BTreeSet::new();

    let mut nearest = candidates.to_vec();
    nearest.sort_by(|left, right| {
        cluster_distance_to_price(left, current_price)
            .partial_cmp(&cluster_distance_to_price(right, current_price))
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                right
                    .weight
                    .partial_cmp(&left.weight)
                    .unwrap_or(Ordering::Equal)
            })
    });
    for cluster in nearest.into_iter().take(near_limit) {
        if seen.insert(cluster_key(&cluster)) {
            selected.push(cluster);
        }
    }

    let mut strongest = candidates
        .iter()
        .filter(|cluster| !seen.contains(&cluster_key(cluster)))
        .cloned()
        .collect::<Vec<_>>();
    strongest.sort_by(|left, right| {
        right
            .weight
            .partial_cmp(&left.weight)
            .unwrap_or(Ordering::Equal)
            .then_with(|| right.count.cmp(&left.count))
            .then_with(|| {
                cluster_distance_to_price(left, current_price)
                    .partial_cmp(&cluster_distance_to_price(right, current_price))
                    .unwrap_or(Ordering::Equal)
            })
    });
    for cluster in strongest.into_iter().take(strong_limit) {
        if seen.insert(cluster_key(&cluster)) {
            selected.push(cluster);
        }
    }

    let mut middle = candidates
        .iter()
        .filter(|cluster| !seen.contains(&cluster_key(cluster)))
        .cloned()
        .collect::<Vec<_>>();
    middle.sort_by(|left, right| {
        cluster_distance_to_price(left, current_price)
            .partial_cmp(&cluster_distance_to_price(right, current_price))
            .unwrap_or(Ordering::Equal)
    });
    if !middle.is_empty() && mid_limit > 0 {
        let start = middle.len() / 3;
        let end = ((middle.len() * 2) / 3).max(start + 1).min(middle.len());
        let mut middle_band = middle[start..end].to_vec();
        middle_band.sort_by(|left, right| {
            right
                .weight
                .partial_cmp(&left.weight)
                .unwrap_or(Ordering::Equal)
                .then_with(|| right.count.cmp(&left.count))
                .then_with(|| {
                    cluster_distance_to_price(left, current_price)
                        .partial_cmp(&cluster_distance_to_price(right, current_price))
                        .unwrap_or(Ordering::Equal)
                })
        });
        for cluster in middle_band.into_iter().take(mid_limit) {
            if seen.insert(cluster_key(&cluster)) {
                selected.push(cluster);
            }
        }
    }

    let mut deepest = candidates
        .iter()
        .filter(|cluster| !seen.contains(&cluster_key(cluster)))
        .cloned()
        .collect::<Vec<_>>();
    deepest.sort_by(|left, right| {
        cluster_distance_to_price(right, current_price)
            .partial_cmp(&cluster_distance_to_price(left, current_price))
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                right
                    .weight
                    .partial_cmp(&left.weight)
                    .unwrap_or(Ordering::Equal)
            })
    });
    for cluster in deepest.into_iter().take(deep_limit) {
        if seen.insert(cluster_key(&cluster)) {
            selected.push(cluster);
        }
    }

    selected.sort_by(|left, right| {
        cluster_distance_to_price(left, current_price)
            .partial_cmp(&cluster_distance_to_price(right, current_price))
            .unwrap_or(Ordering::Equal)
    });
    selected
}

fn select_layered_clusters(
    candidates: &[PriceClusterCandidate],
    current_price: Option<f64>,
    near_limit: usize,
    strong_limit: usize,
    mid_limit: usize,
    deep_limit: usize,
) -> ClusterSelection {
    let Some(current_price) = current_price else {
        let mut ranked = candidates.to_vec();
        ranked.sort_by(|left, right| {
            right
                .weight
                .partial_cmp(&left.weight)
                .unwrap_or(Ordering::Equal)
                .then_with(|| right.count.cmp(&left.count))
        });
        ranked.truncate(near_limit + strong_limit + mid_limit + deep_limit);
        return ClusterSelection {
            cross: ranked,
            ..ClusterSelection::default()
        };
    };

    let cross = candidates
        .iter()
        .filter(|cluster| cluster.low <= current_price && current_price <= cluster.high)
        .cloned()
        .collect::<Vec<_>>();
    let below = candidates
        .iter()
        .filter(|cluster| cluster.high < current_price)
        .cloned()
        .collect::<Vec<_>>();
    let above = candidates
        .iter()
        .filter(|cluster| cluster.low > current_price)
        .cloned()
        .collect::<Vec<_>>();

    ClusterSelection {
        above: select_cluster_side(
            &above,
            current_price,
            near_limit,
            strong_limit,
            mid_limit,
            deep_limit,
        ),
        below: select_cluster_side(
            &below,
            current_price,
            near_limit,
            strong_limit,
            mid_limit,
            deep_limit,
        ),
        cross,
    }
}

fn flatten_cluster_selection(selection: ClusterSelection) -> Vec<PriceClusterCandidate> {
    let mut combined = Vec::new();
    combined.extend(selection.cross);
    combined.extend(selection.below);
    combined.extend(selection.above);
    combined
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

fn build_orderbook_level_clusters(levels: &[Value], current_price: Option<f64>) -> Value {
    let bid_entries = levels
        .iter()
        .filter_map(Value::as_object)
        .filter_map(|level| {
            Some((
                level.get("price_level").and_then(Value::as_f64)?,
                level
                    .get("bid_liquidity")
                    .and_then(Value::as_f64)
                    .unwrap_or_default(),
            ))
        })
        .filter(|(_, liquidity)| *liquidity > f64::EPSILON)
        .collect::<Vec<_>>();
    let ask_entries = levels
        .iter()
        .filter_map(Value::as_object)
        .filter_map(|level| {
            Some((
                level.get("price_level").and_then(Value::as_f64)?,
                level
                    .get("ask_liquidity")
                    .and_then(Value::as_f64)
                    .unwrap_or_default(),
            ))
        })
        .filter(|(_, liquidity)| *liquidity > f64::EPSILON)
        .collect::<Vec<_>>();

    let bid_clusters = flatten_cluster_selection(select_layered_clusters(
        &build_price_clusters(&bid_entries),
        current_price,
        ORDERBOOK_CLUSTER_NEAR_PER_SIDE,
        ORDERBOOK_CLUSTER_STRONG_PER_SIDE,
        ORDERBOOK_CLUSTER_MID_PER_SIDE,
        ORDERBOOK_CLUSTER_DEEP_PER_SIDE,
    ));
    let ask_clusters = flatten_cluster_selection(select_layered_clusters(
        &build_price_clusters(&ask_entries),
        current_price,
        ORDERBOOK_CLUSTER_NEAR_PER_SIDE,
        ORDERBOOK_CLUSTER_STRONG_PER_SIDE,
        ORDERBOOK_CLUSTER_MID_PER_SIDE,
        ORDERBOOK_CLUSTER_DEEP_PER_SIDE,
    ));

    json!({
        "bid_clusters": bid_clusters
            .into_iter()
            .map(|cluster| {
                json!({
                    "p": round2(cluster.mid),
                    "price_low": round2(cluster.low),
                    "price_high": round2(cluster.high),
                    "n": cluster.count,
                    "volume_quote": round2(cluster.weight),
                })
            })
            .collect::<Vec<_>>(),
        "ask_clusters": ask_clusters
            .into_iter()
            .map(|cluster| {
                json!({
                    "p": round2(cluster.mid),
                    "price_low": round2(cluster.low),
                    "price_high": round2(cluster.high),
                    "n": cluster.count,
                    "volume_quote": round2(cluster.weight),
                })
            })
            .collect::<Vec<_>>(),
    })
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
    use super::{compute_atr14, filter_cvd_pack, filter_high_volume_pulse, ScanFilter};
    use crate::llm::provider::ModelInvocationInput;
    use chrono::{DateTime, Utc};
    use serde_json::{json, Value};
    use std::fs;
    use std::path::PathBuf;

    fn sample_input(indicators: Value, ts_bucket: DateTime<Utc>) -> ModelInvocationInput {
        ModelInvocationInput {
            symbol: "TESTUSDT".to_string(),
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
                        name.ends_with("_TESTUSDT.json")
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
                            {"open": 2103.5, "high": 2105.5, "low": 2102.0, "close": 2104.0, "volume_base": 120.0, "open_time": "2026-03-14T07:45:00Z", "is_closed": true},
                            {"open": 2104.0, "high": 2106.0, "low": 2103.2, "close": 2105.2, "volume_base": 80.0, "open_time": "2026-03-14T08:00:00Z", "is_closed": false}
                        ]}}},
                        "4h": {"markets": {"futures": {"bars": [
                            {"open": 2088.0, "high": 2098.0, "low": 2085.0, "close": 2095.0, "volume_base": 200.0, "open_time": "2026-03-13T20:00:00Z", "is_closed": true},
                            {"open": 2095.0, "high": 2106.0, "low": 2090.0, "close": 2102.0, "volume_base": 220.0, "open_time": "2026-03-14T00:00:00Z", "is_closed": true},
                            {"open": 2102.0, "high": 2108.0, "low": 2098.0, "close": 2104.0, "volume_base": 240.0, "open_time": "2026-03-14T04:00:00Z", "is_closed": true},
                            {"open": 2104.0, "high": 2110.0, "low": 2101.0, "close": 2106.5, "volume_base": 160.0, "open_time": "2026-03-14T08:00:00Z", "is_closed": false}
                        ]}}},
                        "1d": {"markets": {"futures": {"bars": [
                            {"open": 2060.0, "high": 2088.0, "low": 2055.0, "close": 2075.0, "volume_base": 300.0, "open_time": "2026-03-11T00:00:00Z", "is_closed": true},
                            {"open": 2075.0, "high": 2100.0, "low": 2070.0, "close": 2092.0, "volume_base": 320.0, "open_time": "2026-03-12T00:00:00Z", "is_closed": true},
                            {"open": 2092.0, "high": 2108.0, "low": 2088.0, "close": 2104.0, "volume_base": 340.0, "open_time": "2026-03-13T00:00:00Z", "is_closed": true},
                            {"open": 2104.0, "high": 2112.0, "low": 2100.0, "close": 2107.0, "volume_base": 180.0, "open_time": "2026-03-14T00:00:00Z", "is_closed": false}
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
    fn build_value_applies_v6_scan_schema() {
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-14T08:00:00Z")
            .expect("parse ts bucket")
            .with_timezone(&Utc);
        let input = sample_input(fixture_indicators(), ts_bucket);
        let value = ScanFilter::build_value(&input).expect("build scan value");

        assert_eq!(
            value.pointer("/version").and_then(Value::as_str),
            Some("scan_v6_2")
        );
        assert_eq!(
            value.pointer("/current_price").and_then(Value::as_f64),
            Some(2104.0)
        );
        assert!(value.pointer("/now/price_anchor").is_some());
        assert!(value.pointer("/now/value_state_board/0").is_some());
        assert!(value.pointer("/now/bracket_board/current_inside").is_some());
        assert!(value
            .pointer("/path_newest_to_oldest/latest_15m_detail/bars_newest_to_oldest/0")
            .is_some());
        assert!(value
            .pointer("/events_newest_to_oldest/latest_24h_detail/0")
            .is_some());
        assert!(value
            .pointer("/supporting_context/cvd_path_snapshot/by_window/15m/series/0")
            .is_some());
        assert!(value.pointer("/raw_overflow/price_structures/0").is_some());
        assert!(value.pointer("/raw_overflow/price_levels/0").is_some());
        assert!(value.pointer("/by_timeframe").is_none());
        assert!(value.pointer("/timeframe_evidence").is_none());
        assert!(value.pointer("/multi_timeframe_evidence").is_none());
        assert!(value.pointer("/indicators").is_none());
        assert!(value.pointer("/now/location_snapshot/vs_value").is_none());
        assert!(value.pointer("/now/acceptance_board").is_none());
        assert!(value
            .pointer("/now/current_volume_nodes/high_volume_pulse")
            .is_some());
        assert!(value
            .pointer("/supporting_context/indicator_snapshots/high_volume_pulse_full")
            .is_some());
    }

    #[test]
    fn build_value_preserves_documented_v6_key_order() {
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-14T08:00:00Z")
            .expect("parse ts bucket")
            .with_timezone(&Utc);
        let input = sample_input(fixture_indicators(), ts_bucket);
        let value = ScanFilter::build_value(&input).expect("build scan value");

        let top_level_keys = value
            .as_object()
            .expect("top-level object")
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            top_level_keys,
            vec![
                "version",
                "symbol",
                "ts_bucket",
                "current_price",
                "now",
                "path_newest_to_oldest",
                "events_newest_to_oldest",
                "supporting_context",
                "raw_overflow",
            ]
        );

        let path_keys = value
            .pointer("/path_newest_to_oldest")
            .and_then(Value::as_object)
            .expect("path object")
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            path_keys,
            vec![
                "latest_15m_detail",
                "latest_4h_context",
                "latest_1d_background",
            ]
        );

        let serialized = serde_json::to_string(&value).expect("serialize scan value");
        let ordered_markers = [
            "\"version\"",
            "\"symbol\"",
            "\"ts_bucket\"",
            "\"current_price\"",
            "\"now\"",
            "\"path_newest_to_oldest\"",
            "\"events_newest_to_oldest\"",
            "\"supporting_context\"",
            "\"raw_overflow\"",
        ];
        let mut last_index = 0usize;
        for marker in ordered_markers {
            let index = serialized
                .find(marker)
                .unwrap_or_else(|| panic!("missing serialized marker: {marker}"));
            assert!(
                index >= last_index,
                "marker {marker} was serialized out of order"
            );
            last_index = index;
        }
    }

    #[test]
    fn build_value_emits_value_state_and_path_context() {
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-14T08:00:00Z")
            .expect("parse ts bucket")
            .with_timezone(&Utc);
        let input = sample_input(fixture_indicators(), ts_bucket);
        let value = ScanFilter::build_value(&input).expect("build scan value");

        let value_state_board = value
            .pointer("/now/value_state_board")
            .and_then(Value::as_array)
            .expect("value state board array");
        assert!(value_state_board.iter().any(|entry| {
            entry.get("scope").and_then(Value::as_str) == Some("15m")
                && entry.get("source").and_then(Value::as_str) == Some("price_volume_structure")
                && entry.get("state").and_then(Value::as_str) == Some("inside_value")
                && entry.get("position").and_then(Value::as_str) == Some("inside")
        }));

        assert_eq!(
            value
                .pointer("/path_newest_to_oldest/latest_15m_detail/summary/bars_count")
                .and_then(Value::as_u64),
            Some(3)
        );
        assert_eq!(
            value
                .pointer("/path_newest_to_oldest/latest_15m_detail/bars_newest_to_oldest/0/close")
                .and_then(Value::as_f64),
            Some(2104.0)
        );
        assert_eq!(
            value
                .pointer("/path_newest_to_oldest/latest_15m_detail/summary/atr14")
                .and_then(Value::as_f64),
            Some(3.5)
        );
        assert_eq!(
            value
                .pointer("/path_newest_to_oldest/latest_15m_detail/summary/atr14_pct")
                .and_then(Value::as_f64),
            Some(0.17)
        );
        assert_eq!(
            value
                .pointer("/path_newest_to_oldest/latest_15m_detail/current_partial_bar/ts")
                .and_then(Value::as_str),
            Some("2026-03-14T08:00:00Z")
        );
        assert_eq!(
            value
                .pointer("/path_newest_to_oldest/latest_4h_context/summary/atr14")
                .and_then(Value::as_f64),
            Some(13.0)
        );
        assert_eq!(
            value
                .pointer("/path_newest_to_oldest/latest_4h_context/summary/atr14_pct")
                .and_then(Value::as_f64),
            Some(0.62)
        );
        assert_eq!(
            value
                .pointer("/path_newest_to_oldest/latest_4h_context/current_partial_bar/ts")
                .and_then(Value::as_str),
            Some("2026-03-14T08:00:00Z")
        );
        assert_eq!(
            value
                .pointer("/path_newest_to_oldest/latest_1d_background/summary/atr14")
                .and_then(Value::as_f64),
            Some(27.67)
        );
        assert_eq!(
            value
                .pointer("/path_newest_to_oldest/latest_1d_background/summary/atr14_pct")
                .and_then(Value::as_f64),
            Some(1.31)
        );
        assert_eq!(
            value
                .pointer("/path_newest_to_oldest/latest_1d_background/current_partial_bar/ts")
                .and_then(Value::as_str),
            Some("2026-03-14T00:00:00Z")
        );
    }

    #[test]
    fn build_value_emits_numeric_momentum_snapshot() {
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-14T08:00:00Z")
            .expect("parse ts bucket")
            .with_timezone(&Utc);
        let input = sample_input(fixture_indicators(), ts_bucket);
        let value = ScanFilter::build_value(&input).expect("build scan value");

        assert_eq!(
            value
                .pointer("/now/momentum_snapshot/15m/bar_return_last_3_pct")
                .and_then(Value::as_array)
                .map(|v| {
                    v.iter()
                        .map(|x| x.as_f64().expect("15m bar return number"))
                        .collect::<Vec<_>>()
                }),
            Some(vec![0.05, 0.12, 0.02])
        );
        assert_eq!(
            value
                .pointer("/now/momentum_snapshot/15m/bar_return_sum_last_3_pct")
                .and_then(Value::as_f64),
            Some(0.19)
        );
        assert_eq!(
            value
                .pointer("/now/momentum_snapshot/15m/last_closed_bar_range_vs_atr")
                .and_then(Value::as_f64),
            Some(1.0)
        );
        assert_eq!(
            value
                .pointer("/now/momentum_snapshot/15m/last_closed_bar_close_location_pct")
                .and_then(Value::as_f64),
            Some(57.14)
        );
        assert_eq!(
            value
                .pointer("/now/momentum_snapshot/15m/cvd_delta_last_3")
                .and_then(Value::as_array)
                .map(|v| {
                    v.iter()
                        .map(|x| x.as_f64().expect("15m cvd number"))
                        .collect::<Vec<_>>()
                }),
            Some(vec![100.0, 300.0, 500.0])
        );
        assert_eq!(
            value
                .pointer("/now/momentum_snapshot/15m/cvd_slope")
                .and_then(Value::as_f64),
            Some(200.0)
        );
        assert_eq!(
            value
                .pointer("/now/momentum_snapshot/15m/whale_delta_notional")
                .and_then(Value::as_f64),
            Some(1200000.0)
        );

        assert_eq!(
            value
                .pointer("/now/momentum_snapshot/4h/bar_return_last_3_pct")
                .and_then(Value::as_array)
                .map(|v| {
                    v.iter()
                        .map(|x| x.as_f64().expect("4h bar return number"))
                        .collect::<Vec<_>>()
                }),
            Some(vec![0.34, 0.33, 0.1])
        );
        assert_eq!(
            value
                .pointer("/now/momentum_snapshot/4h/bar_return_sum_last_3_pct")
                .and_then(Value::as_f64),
            Some(0.77)
        );
        assert_eq!(
            value
                .pointer("/now/momentum_snapshot/4h/last_closed_bar_range_vs_atr")
                .and_then(Value::as_f64),
            Some(0.77)
        );
        assert_eq!(
            value
                .pointer("/now/momentum_snapshot/4h/last_closed_bar_close_location_pct")
                .and_then(Value::as_f64),
            Some(60.0)
        );
        assert_eq!(
            value
                .pointer("/now/momentum_snapshot/4h/cvd_slope")
                .and_then(Value::as_f64),
            Some(40.0)
        );
        assert_eq!(
            value
                .pointer("/now/momentum_snapshot/4h/whale_delta_notional")
                .and_then(Value::as_f64),
            Some(2400000.0)
        );

        assert_eq!(
            value
                .pointer("/now/momentum_snapshot/1d/bar_return_last_3_pct")
                .and_then(Value::as_array)
                .map(|v| {
                    v.iter()
                        .map(|x| x.as_f64().expect("1d bar return number"))
                        .collect::<Vec<_>>()
                }),
            Some(vec![0.73, 0.82, 0.57])
        );
        assert_eq!(
            value
                .pointer("/now/momentum_snapshot/1d/bar_return_sum_last_3_pct")
                .and_then(Value::as_f64),
            Some(2.14)
        );
        assert_eq!(
            value
                .pointer("/now/momentum_snapshot/1d/last_closed_bar_range_vs_atr")
                .and_then(Value::as_f64),
            Some(0.72)
        );
        assert_eq!(
            value
                .pointer("/now/momentum_snapshot/1d/last_closed_bar_close_location_pct")
                .and_then(Value::as_f64),
            Some(80.0)
        );
        assert_eq!(
            value
                .pointer("/now/momentum_snapshot/1d/cvd_slope")
                .and_then(Value::as_f64),
            Some(40.0)
        );
        assert_eq!(
            value
                .pointer("/now/momentum_snapshot/1d/whale_delta_notional")
                .and_then(Value::as_f64),
            Some(4800000.0)
        );
    }

    #[test]
    fn build_value_keeps_fine_grained_clusters_and_3d_sources() {
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-14T08:00:00Z")
            .expect("parse ts bucket")
            .with_timezone(&Utc);
        let mut indicators = fixture_indicators();
        indicators["price_volume_structure"]["payload"]["by_window"]["3d"] = json!({
            "window_bars_used": 20,
            "poc_price": 2092.2,
            "poc_volume": 80.0,
            "vah": 2118.0,
            "val": 2088.0,
            "bar_volume": 500.0,
            "volume_zscore": 1.1,
            "value_area_levels": [
                {"price_level": 2102.0, "volume": 20.0},
                {"price_level": 2102.1, "volume": 18.0},
                {"price_level": 2092.18, "volume": 55.0},
                {"price_level": 2092.19, "volume": 65.0},
                {"price_level": 2092.20, "volume": 75.0},
                {"price_level": 2092.21, "volume": 68.0},
                {"price_level": 2092.22, "volume": 58.0}
            ]
        });
        indicators["footprint"]["payload"]["by_window"]["3d"] = json!({
            "window_delta": 420.0,
            "window_total_qty": 4200.0,
            "unfinished_auction": false,
            "ua_top": 2112.0,
            "ua_bottom": 2088.0,
            "stacked_buy": true,
            "stacked_sell": true,
            "buy_stacks": [{"start_price": 2092.18, "end_price": 2092.22, "length": 5}],
            "sell_stacks": [{"start_price": 2110.0, "end_price": 2111.0, "length": 3}],
            "buy_imbalance_prices": [2102.0, 2102.1, 2092.18, 2092.19, 2092.20, 2092.21, 2092.22],
            "sell_imbalance_prices": [2110.4, 2110.5]
        });
        indicators["orderbook_depth"]["payload"]["levels"] = json!([
            {"price_level": 2092.18, "bid_liquidity": 80.0, "ask_liquidity": 0.0, "total_liquidity": 80.0},
            {"price_level": 2092.19, "bid_liquidity": 82.0, "ask_liquidity": 0.0, "total_liquidity": 82.0},
            {"price_level": 2092.20, "bid_liquidity": 84.0, "ask_liquidity": 0.0, "total_liquidity": 84.0},
            {"price_level": 2092.21, "bid_liquidity": 83.0, "ask_liquidity": 0.0, "total_liquidity": 83.0},
            {"price_level": 2092.22, "bid_liquidity": 81.0, "ask_liquidity": 0.0, "total_liquidity": 81.0},
            {"price_level": 2102.0, "bid_liquidity": 30.0, "ask_liquidity": 0.0, "total_liquidity": 30.0},
            {"price_level": 2102.1, "bid_liquidity": 28.0, "ask_liquidity": 0.0, "total_liquidity": 28.0},
            {"price_level": 2107.0, "bid_liquidity": 0.0, "ask_liquidity": 55.0, "total_liquidity": 55.0},
            {"price_level": 2108.0, "bid_liquidity": 0.0, "ask_liquidity": 65.0, "total_liquidity": 65.0}
        ]);

        let input = sample_input(indicators, ts_bucket);
        let value = ScanFilter::build_value(&input).expect("build scan value");
        let structures = value
            .pointer("/raw_overflow/price_structures")
            .and_then(Value::as_array)
            .expect("price structures");

        assert!(structures.iter().any(|entry| {
            entry.get("source_kind").and_then(Value::as_str) == Some("pvs_value_cluster")
                && entry.get("source_window").and_then(Value::as_str) == Some("3d")
                && entry.get("price_low").and_then(Value::as_f64) == Some(2092.18)
                && entry.get("price_high").and_then(Value::as_f64) == Some(2092.22)
        }));
        assert!(structures.iter().any(|entry| {
            entry.get("source_kind").and_then(Value::as_str) == Some("footprint_price_cluster")
                && entry.get("source_window").and_then(Value::as_str) == Some("3d")
                && entry.get("price_low").and_then(Value::as_f64) == Some(2092.18)
                && entry.get("price_high").and_then(Value::as_f64) == Some(2092.22)
        }));
        assert!(structures.iter().any(|entry| {
            entry.get("source_kind").and_then(Value::as_str) == Some("orderbook_depth_cluster")
                && entry.get("price_low").and_then(Value::as_f64) == Some(2092.18)
                && entry.get("price_high").and_then(Value::as_f64) == Some(2092.22)
        }));
    }

    #[test]
    fn build_value_includes_active_fvg_absorption_and_footprint_stack_summaries() {
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-14T08:00:00Z")
            .expect("parse ts bucket")
            .with_timezone(&Utc);
        let mut indicators = fixture_indicators();
        indicators["fvg"]["payload"]["by_window"]["15m"]["nearest_bull_fvg"] = Value::Null;
        indicators["fvg"]["payload"]["by_window"]["15m"]["nearest_bear_fvg"] = Value::Null;
        indicators["fvg"]["payload"]["by_window"]["15m"]["active_bull_fvgs"] = json!([
            {"upper": 2101.9, "lower": 2100.8, "state": "active", "touch_count": 2, "age_bars": 3}
        ]);

        let input = sample_input(indicators, ts_bucket);
        let value = ScanFilter::build_value(&input).expect("build scan value");
        let structures = value
            .pointer("/raw_overflow/price_structures")
            .and_then(Value::as_array)
            .expect("price structures");

        assert!(structures.iter().any(|entry| {
            entry.get("source_kind").and_then(Value::as_str) == Some("fvg_boundary")
                && entry.get("price_low").and_then(Value::as_f64) == Some(2100.8)
                && entry.get("price_high").and_then(Value::as_f64) == Some(2101.9)
        }));
        assert!(structures.iter().any(|entry| {
            entry.get("source_kind").and_then(Value::as_str) == Some("recent_swing_bracket")
                && entry.get("source_window").and_then(Value::as_str) == Some("15m")
        }));
        assert!(structures.iter().any(|entry| {
            entry.get("source_kind").and_then(Value::as_str) == Some("absorption_zone")
        }));

        let bracket_board = value
            .pointer("/now/bracket_board")
            .and_then(Value::as_object)
            .expect("bracket board");
        let has_absorption_evidence = [
            "current_inside",
            "nearest_above",
            "nearest_below",
            "higher_context",
        ]
        .iter()
        .filter_map(|key| bracket_board.get(*key).and_then(Value::as_array))
        .flat_map(|entries| entries.iter())
        .any(|entry| {
            entry
                .get("support_evidence")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .chain(
                    entry
                        .get("resistance_evidence")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten(),
                )
                .filter_map(Value::as_str)
                .any(|text| text.contains("absorption"))
        });
        assert!(has_absorption_evidence);

        assert_eq!(
            value
                .pointer("/now/current_flow_snapshot/footprint/by_window/15m/max_buy_stack_len")
                .and_then(Value::as_u64),
            Some(6)
        );
        assert_eq!(
            value
                .pointer("/now/current_flow_snapshot/footprint/by_window/15m/max_sell_stack_len")
                .and_then(Value::as_u64),
            Some(5)
        );
    }

    #[test]
    fn build_value_merges_bracket_scopes_for_identical_boundaries() {
        let merged = super::merge_bracket_candidates(vec![
            super::BracketCandidate {
                scopes: vec!["15m".to_string()],
                kind: "tpo_value_area".to_string(),
                source_indicators: vec!["tpo_market_profile".to_string()],
                reference_ts: Some("2026-03-14T08:00:00Z".to_string()),
                support: 2101.0,
                resistance: 2105.0,
                support_evidence: vec!["tpo support 2101.00".to_string()],
                resistance_evidence: vec!["tpo resistance 2105.00".to_string()],
            },
            super::BracketCandidate {
                scopes: vec!["4h".to_string()],
                kind: "tpo_value_area".to_string(),
                source_indicators: vec!["tpo_market_profile".to_string()],
                reference_ts: Some("2026-03-14T08:05:00Z".to_string()),
                support: 2101.0,
                resistance: 2105.0,
                support_evidence: vec!["tpo support 2101.00".to_string()],
                resistance_evidence: vec!["tpo resistance 2105.00".to_string()],
            },
        ]);

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].scopes, vec!["15m".to_string(), "4h".to_string()]);
        assert_eq!(
            merged[0].reference_ts.as_deref(),
            Some("2026-03-14T08:05:00Z")
        );
    }

    #[test]
    fn build_value_aggregates_raw_overflow_and_limits_absorption_per_scope_direction() {
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-14T08:00:00Z")
            .expect("parse ts bucket")
            .with_timezone(&Utc);
        let mut indicators = fixture_indicators();
        indicators["bullish_absorption"]["payload"]["recent_7d"]["events"] = json!((0..15)
            .map(|idx| json!({
                "event_start_ts": format!("2026-03-14T07:{:02}:00Z", idx),
                "event_end_ts": format!("2026-03-14T07:{:02}:00Z", idx),
                "confirm_ts": format!("2026-03-14T07:{:02}:00Z", idx),
                "direction": 1,
                "pivot_price": 2102.0 + (idx as f64 * 0.01),
                "score": 0.50 + (idx as f64 * 0.01),
                "type": "bullish_absorption",
                "scope": "15m"
            }))
            .collect::<Vec<_>>());
        indicators["bearish_absorption"]["payload"]["recent_7d"]["events"] = json!([]);

        let input = sample_input(indicators, ts_bucket);
        let value = ScanFilter::build_value(&input).expect("build scan value");
        let structures = value
            .pointer("/raw_overflow/price_structures")
            .and_then(Value::as_array)
            .expect("price structures");

        assert!(structures
            .iter()
            .all(|entry| entry.get("source_kinds").is_none()));
        assert!(structures
            .iter()
            .all(|entry| entry.get("source_windows").is_none()));
        assert!(structures
            .iter()
            .all(|entry| entry.get("source_kind").is_some()));
        assert!(structures
            .iter()
            .all(|entry| entry.get("source_window").is_some()));

        let absorption_count = structures
            .iter()
            .filter(|entry| {
                entry.get("source_kind").and_then(Value::as_str) == Some("absorption_zone")
            })
            .count();
        assert!(absorption_count <= super::RAW_OVERFLOW_ABSORPTION_MAX_PER_SCOPE_DIRECTION);

        let pvs_clusters = structures
            .iter()
            .filter(|entry| {
                entry.get("source_kind").and_then(Value::as_str) == Some("pvs_value_cluster")
            })
            .collect::<Vec<_>>();
        assert!(pvs_clusters
            .iter()
            .all(|entry| entry.get("count").is_some()));
        assert!(pvs_clusters
            .iter()
            .all(|entry| entry.get("distance_to_mid_pct").is_some()));
    }

    #[test]
    fn filter_cvd_pack_omits_null_large_window_cross_market_fields() {
        let payload = json!({
            "delta_fut": -244.476,
            "delta_spot": 157.0657,
            "relative_delta_fut": -0.0481,
            "relative_delta_spot": 0.3447,
            "xmk_delta_gap_s_minus_f": 401.5417,
            "spot_flow_dominance": 0.3911,
            "by_window": {
                "15m": {
                    "series": [{
                        "ts": "2026-03-17T09:30:00+00:00",
                        "close_fut": 2323.77,
                        "close_spot": 2324.10,
                        "delta_fut": 6207.6969,
                        "delta_spot": -647.7018,
                        "relative_delta_fut": 0.0800557,
                        "relative_delta_spot": -0.1139836,
                        "cvd_7d_fut": 1000.0,
                        "cvd_7d_spot": 200.0,
                        "spot_flow_dominance": 0.6867,
                        "xmk_delta_gap_s_minus_f": -0.8306
                    }]
                },
                "4h": {
                    "series": [{
                        "ts": "2026-03-17T08:00:00+00:00",
                        "close_fut": 2326.75,
                        "close_spot": 2327.12,
                        "delta_fut": -8834.352,
                        "delta_spot": -2316.9561,
                        "relative_delta_fut": -0.0089874,
                        "relative_delta_spot": -0.0268821,
                        "cvd_7d_fut": 1500.0,
                        "cvd_7d_spot": 300.0,
                        "spot_flow_dominance": null,
                        "xmk_delta_gap_s_minus_f": null
                    }]
                },
                "1d": {
                    "series": [{
                        "ts": "2026-03-17T00:00:00+00:00",
                        "close_fut": 2351.66,
                        "close_spot": 2352.02,
                        "delta_fut": 620534.926,
                        "delta_spot": 46099.4636,
                        "relative_delta_fut": 0.0672433,
                        "relative_delta_spot": 0.0551008,
                        "cvd_7d_fut": 9000.0,
                        "cvd_7d_spot": 700.0,
                        "spot_flow_dominance": null,
                        "xmk_delta_gap_s_minus_f": null
                    }]
                }
            }
        });

        let value = filter_cvd_pack(&payload);

        assert!(value
            .pointer("/by_window/15m/series/0/xmk_delta_gap_s_minus_f")
            .is_some());
        assert!(value
            .pointer("/by_window/15m/series/0/spot_flow_dominance")
            .is_some());
        assert!(value
            .pointer("/by_window/4h/series/0/xmk_delta_gap_s_minus_f")
            .is_none());
        assert!(value
            .pointer("/by_window/4h/series/0/spot_flow_dominance")
            .is_none());
        assert!(value
            .pointer("/by_window/1d/series/0/xmk_delta_gap_s_minus_f")
            .is_none());
        assert!(value
            .pointer("/by_window/1d/series/0/spot_flow_dominance")
            .is_none());
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
    fn filter_high_volume_pulse_keeps_source_available_windows() {
        let payload = json!({
            "as_of_ts": "2026-03-17T08:15:00+00:00",
            "by_z_window": {
                "1d": {"window_minutes": 1440, "lookback_samples": 1440, "rolling_volume_w": 8509443.86, "volume_spike_z_w": -0.12, "is_volume_spike_z2": false, "is_volume_spike_z3": false},
                "1h": {"window_minutes": 60, "lookback_samples": 60, "rolling_volume_w": 213636.91, "volume_spike_z_w": 1.27, "is_volume_spike_z2": false, "is_volume_spike_z3": false},
                "4h": {"window_minutes": 240, "lookback_samples": 240, "rolling_volume_w": 917561.19, "volume_spike_z_w": -2.94, "is_volume_spike_z2": false, "is_volume_spike_z3": false}
            },
            "intrabar_poc_max_by_window": {
                "15m": {"intrabar_poc_price": 2325.0, "intrabar_poc_volume": 602.083, "ts": "2026-03-17T08:11:00+00:00", "window_minutes": 15},
                "1h": {"intrabar_poc_price": 2335.0, "intrabar_poc_volume": 2314.556, "ts": "2026-03-17T07:20:00+00:00", "window_minutes": 60}
            }
        });

        let value = filter_high_volume_pulse(&payload);

        assert!(value.pointer("/by_z_window/15m").is_none());
        assert!(value.pointer("/by_z_window/1h").is_some());
        assert!(value.pointer("/by_z_window/4h").is_some());
        assert!(value.pointer("/intrabar_poc_max_by_window/15m").is_some());
        assert!(value.pointer("/intrabar_poc_max_by_window/1h").is_some());
        assert!(value
            .pointer("/by_z_window/1h/is_volume_spike_z2")
            .is_none());
    }

    #[test]
    fn build_events_reads_divergence_top_level_events_fallback() {
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-14T08:00:00Z")
            .expect("parse ts bucket")
            .with_timezone(&Utc);
        let mut indicators = fixture_indicators();
        indicators["divergence"]["payload"] = json!({
            "events": [sample_divergence_event("2026-03-14T07:25:00Z", "hidden_bullish_divergence", 0.9)]
        });

        let input = sample_input(indicators, ts_bucket);
        let value = ScanFilter::build_value(&input).expect("build scan value");
        let events = value
            .pointer("/events_newest_to_oldest/latest_24h_detail")
            .and_then(Value::as_array)
            .expect("events array");

        assert!(events.iter().any(|entry| {
            entry.get("indicator_code").and_then(Value::as_str) == Some("divergence")
                && entry.get("event_type").and_then(Value::as_str)
                    == Some("hidden_bullish_divergence")
        }));
    }

    #[test]
    fn build_events_backfills_scope_spot_confirm_trigger_side_and_divergence_prices() {
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-14T08:00:00Z")
            .expect("parse ts bucket")
            .with_timezone(&Utc);
        let mut indicators = fixture_indicators();
        indicators["kline_history"]["payload"]["intervals"]["1m"] = json!({
            "markets": {"futures": {"bars": [
                {"open": 2100.0, "high": 2101.5, "low": 2099.5, "close": 2101.0, "volume_base": 10.0, "open_time": "2026-03-14T07:15:00Z", "is_closed": true},
                {"open": 2101.0, "high": 2102.5, "low": 2098.0, "close": 2100.5, "volume_base": 10.0, "open_time": "2026-03-14T07:16:00Z", "is_closed": true},
                {"open": 2100.5, "high": 2103.0, "low": 2098.5, "close": 2102.0, "volume_base": 10.0, "open_time": "2026-03-14T07:17:00Z", "is_closed": true}
            ]}}
        });
        indicators["selling_exhaustion"]["payload"]["recent_7d"]["events"] = json!([
            {
                "type": "selling_exhaustion",
                "event_start_ts": "2026-03-14T07:03:00Z",
                "event_end_ts": "2026-03-14T07:10:00Z",
                "confirm_ts": "2026-03-14T07:10:00Z",
                "direction": 1,
                "pivot_price": 2105.0,
                "score": 0.8,
                "spot_exhaustion_confirm": false
            }
        ]);
        indicators["buying_exhaustion"]["payload"]["recent_7d"]["events"] = json!([]);
        indicators["bullish_initiation"]["payload"]["recent_7d"]["events"] = json!([
            {
                "type": "bullish_initiation",
                "event_start_ts": "2026-03-14T07:11:00Z",
                "event_end_ts": "2026-03-14T07:14:00Z",
                "confirm_ts": "2026-03-14T07:14:00Z",
                "direction": 1,
                "pivot_price": 2102.0,
                "price_low": 2100.0,
                "price_high": 2104.0,
                "score": 0.7,
                "spot_break_confirm": true
            }
        ]);
        indicators["bearish_initiation"]["payload"]["recent_7d"]["events"] = json!([]);
        indicators["divergence"]["payload"]["recent_7d"]["events"] = json!([
            {
                "type": "bullish_divergence",
                "event_start_ts": "2026-03-14T07:15:00Z",
                "event_end_ts": "2026-03-14T07:17:00Z",
                "pivot_side": "low",
                "score": 0.9,
                "spot_price_flow_confirm": false
            }
        ]);
        indicators["bullish_absorption"]["payload"]["recent_7d"]["events"] = json!([]);
        indicators["bearish_absorption"]["payload"]["recent_7d"]["events"] = json!([]);

        let input = sample_input(indicators, ts_bucket);
        let value = ScanFilter::build_value(&input).expect("build scan value");
        let events = value
            .pointer("/events_newest_to_oldest/latest_24h_detail")
            .and_then(Value::as_array)
            .expect("events array");

        let selling_exhaustion = events
            .iter()
            .find(|event| {
                event.get("indicator_code").and_then(Value::as_str) == Some("selling_exhaustion")
            })
            .expect("selling exhaustion event");
        assert_eq!(
            selling_exhaustion.get("scope").and_then(Value::as_str),
            Some("1m")
        );
        assert_eq!(
            selling_exhaustion
                .get("spot_confirm")
                .and_then(Value::as_bool),
            Some(false)
        );
        assert_eq!(
            selling_exhaustion
                .get("trigger_side")
                .and_then(Value::as_str),
            Some("sell")
        );

        let bullish_initiation = events
            .iter()
            .find(|event| {
                event.get("indicator_code").and_then(Value::as_str) == Some("bullish_initiation")
            })
            .expect("bullish initiation event");
        assert_eq!(
            bullish_initiation.get("scope").and_then(Value::as_str),
            Some("1m")
        );
        assert_eq!(
            bullish_initiation
                .get("spot_confirm")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            bullish_initiation
                .get("trigger_side")
                .and_then(Value::as_str),
            Some("buy")
        );

        let divergence = events
            .iter()
            .find(|event| event.get("indicator_code").and_then(Value::as_str) == Some("divergence"))
            .expect("divergence event");
        assert_eq!(divergence.get("scope").and_then(Value::as_str), Some("1m"));
        assert_eq!(
            divergence.get("spot_confirm").and_then(Value::as_bool),
            Some(false)
        );
        assert_eq!(
            divergence.get("pivot_price").and_then(Value::as_f64),
            Some(2098.0)
        );
        assert_eq!(
            divergence.get("price_low").and_then(Value::as_f64),
            Some(2098.0)
        );
        assert_eq!(
            divergence.get("price_high").and_then(Value::as_f64),
            Some(2103.0)
        );
        assert!(divergence
            .get("distance_to_current_pct")
            .and_then(Value::as_f64)
            .is_some());
    }

    #[test]
    fn build_events_orders_latest_24h_by_visible_event_time_desc() {
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-14T08:00:00Z")
            .expect("parse ts bucket")
            .with_timezone(&Utc);
        let mut indicators = fixture_indicators();
        indicators["selling_exhaustion"]["payload"]["recent_7d"]["events"] = json!([
            {
                "event_start_ts": "2026-03-14T07:00:00Z",
                "event_end_ts": "2026-03-14T07:29:00Z",
                "confirm_ts": "2026-03-14T07:30:00Z",
                "pivot_price": 2101.0,
                "score": 0.8,
                "type": "selling_exhaustion"
            }
        ]);
        indicators["bullish_absorption"]["payload"]["recent_7d"]["events"] = json!([
            {
                "event_start_ts": "2026-03-14T07:10:00Z",
                "event_end_ts": "2026-03-14T07:19:00Z",
                "confirm_ts": "2026-03-14T07:20:00Z",
                "pivot_price": 2100.5,
                "score": 0.7,
                "type": "bullish_absorption"
            }
        ]);
        indicators["bearish_absorption"]["payload"]["recent_7d"]["events"] = json!([]);
        indicators["buying_exhaustion"]["payload"]["recent_7d"]["events"] = json!([]);
        indicators["divergence"]["payload"]["recent_7d"]["events"] = json!([]);
        indicators["bullish_initiation"]["payload"]["recent_7d"]["events"] = json!([]);
        indicators["bearish_initiation"]["payload"]["recent_7d"]["events"] = json!([]);

        let input = sample_input(indicators, ts_bucket);
        let value = ScanFilter::build_value(&input).expect("build scan value");
        let events = value
            .pointer("/events_newest_to_oldest/latest_24h_detail")
            .and_then(Value::as_array)
            .expect("events array");

        assert_eq!(
            events
                .first()
                .and_then(|event| event.get("event_start_ts"))
                .and_then(Value::as_str),
            Some("2026-03-14T07:10:00Z")
        );
        assert_eq!(
            events
                .get(1)
                .and_then(|event| event.get("event_start_ts"))
                .and_then(Value::as_str),
            Some("2026-03-14T07:00:00Z")
        );
    }

    #[test]
    fn real_snapshot_scan_output_emits_v6_sections() {
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
        assert!(scan_value.pointer("/current_price").is_some());
        assert!(scan_value.pointer("/now").is_some());
        assert!(scan_value
            .pointer("/path_newest_to_oldest/latest_15m_detail")
            .is_some());
        assert!(scan_value.pointer("/events_newest_to_oldest").is_some());
        assert!(scan_value.pointer("/supporting_context").is_some());
        assert!(scan_value
            .pointer("/raw_overflow/price_structures/0")
            .is_some());
        assert!(scan_value
            .pointer("/now/current_flow_snapshot/footprint/by_window/15m/buy_stacks")
            .is_some());
        assert!(scan_value.pointer("/indicators").is_none());
        assert!(scan_value.pointer("/by_timeframe").is_none());
        assert!(scan_value.pointer("/timeframe_evidence").is_none());
    }
}
