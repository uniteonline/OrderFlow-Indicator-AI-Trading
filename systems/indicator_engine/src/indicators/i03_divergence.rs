use crate::indicators::context::{
    clip01, robust_z_at, DivergenceEventRow, DivergenceSigTestMode, IndicatorComputation,
    IndicatorContext, IndicatorEventRow, IndicatorSnapshotRow,
};
use crate::indicators::indicator_trait::Indicator;
use crate::indicators::shared::event_ids::{build_divergence_event_id, build_indicator_event_id};
use crate::indicators::shared::event_views::{
    build_event_window_view, build_recent_7d_payload, merge_payload_fields,
};
use chrono::Duration;
use serde_json::{json, Map, Value};

pub struct I03Divergence;

const PIVOT_K: usize = 3;
const MIN_LEG_GAP_MINUTES: i64 = 3;
const MAX_LEG_GAP_MINUTES: i64 = 180;
const ETA_LEG: f64 = 0.5;
const DETREND_LEN_PRICE: usize = 60;
const DETREND_LEN_CVD: usize = 120;
const ROBUST_Z_LOOKBACK: usize = 120;
const ATR_LOOKBACK: usize = 60;
const EPS_CVD_Z: f64 = 0.5;
const ZP_MIN: f64 = 1.0;
const ZC_MIN: f64 = 1.0;
const BOOTSTRAP_MIN_RET_SAMPLES: usize = 32;

#[derive(Debug, Clone)]
struct DivergenceCandidate {
    divergence_type: String,
    pivot_side: String,
    i1: usize,
    i2: usize,
    confirm_i1: usize,
    confirm_i2: usize,
    available_i: usize,
    price_start: f64,
    price_end: f64,
    cvd_start_fut: f64,
    cvd_end_fut: f64,
    cvd_start_spot: f64,
    cvd_end_spot: f64,
    price_diff: f64,
    cvd_diff_fut: f64,
    cvd_diff_spot: f64,
    price_effect_z: f64,
    cvd_effect_z: f64,
    sig_pass: bool,
    p_value_price: f64,
    p_value_cvd: f64,
    score: f64,
    spot_price_flow_confirm: bool,
    fut_divergence_sign: i16,
    spot_lead_score: f64,
    likely_driver: String,
}

impl Indicator for I03Divergence {
    fn code(&self) -> &'static str {
        "divergence"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation {
        let n = ctx.history_futures.len().min(ctx.history_spot.len());
        if n < (PIVOT_K * 4).max(ROBUST_Z_LOOKBACK + 5) {
            return IndicatorComputation {
                snapshot: Some(IndicatorSnapshotRow {
                    indicator_code: self.code(),
                    window_code: "1m",
                    payload_json: empty_snapshot_payload(ctx, "insufficient_history"),
                }),
                ..Default::default()
            };
        }

        let fut = ctx.history_futures[ctx.history_futures.len() - n..].to_vec();
        let spot = ctx.history_spot[ctx.history_spot.len() - n..].to_vec();
        let last_idx = n - 1;

        let closes = fut
            .iter()
            .map(|h| h.close_price.or(h.last_price).unwrap_or(0.0))
            .collect::<Vec<_>>();
        let highs = fut
            .iter()
            .map(|h| h.high_price.or(h.last_price).unwrap_or(0.0))
            .collect::<Vec<_>>();
        let lows = fut
            .iter()
            .map(|h| h.low_price.or(h.last_price).unwrap_or(0.0))
            .collect::<Vec<_>>();
        let cvd_fut = fut.iter().map(|h| h.cvd).collect::<Vec<_>>();
        let cvd_spot = spot.iter().map(|h| h.cvd).collect::<Vec<_>>();

        let detrended_price = detrend_rolling_ols(
            &closes.iter().map(|v| v.ln()).collect::<Vec<_>>(),
            DETREND_LEN_PRICE,
        );
        let detrended_cvd_fut = detrend_rolling_ols(&cvd_fut, DETREND_LEN_CVD);
        let detrended_cvd_spot = detrend_rolling_ols(&cvd_spot, DETREND_LEN_CVD);

        let z_cvd_fut = robust_z_series(&detrended_cvd_fut, ROBUST_Z_LOOKBACK);
        let z_cvd_spot = robust_z_series(&detrended_cvd_spot, ROBUST_Z_LOOKBACK);

        let pivots_high = confirmed_high_pivots(&highs, PIVOT_K, last_idx);
        let pivots_low = confirmed_low_pivots(&lows, PIVOT_K, last_idx);
        let atr = rolling_atr(&highs, &lows, &closes, ATR_LOOKBACK);

        let mut candidates = all_candidates(
            true,
            &pivots_high,
            &fut,
            &spot,
            &closes,
            &highs,
            &lows,
            &detrended_price,
            &z_cvd_fut,
            &z_cvd_spot,
            &atr,
            ctx.divergence_sig_test_mode,
            ctx.divergence_bootstrap_b,
            ctx.divergence_bootstrap_block_len,
            ctx.divergence_p_value_threshold,
        );
        candidates.extend(all_candidates(
            false,
            &pivots_low,
            &fut,
            &spot,
            &closes,
            &highs,
            &lows,
            &detrended_price,
            &z_cvd_fut,
            &z_cvd_spot,
            &atr,
            ctx.divergence_sig_test_mode,
            ctx.divergence_bootstrap_b,
            ctx.divergence_bootstrap_block_len,
            ctx.divergence_p_value_threshold,
        ));
        candidates.sort_by_key(|c| c.available_i);

        let current_candidates = candidates
            .iter()
            .filter(|c| c.available_i == last_idx)
            .cloned()
            .collect::<Vec<_>>();
        let latest_current = current_candidates
            .iter()
            .max_by_key(|c| c.available_i)
            .cloned();
        let window_view = build_event_window_view(
            ctx.ts_bucket,
            candidates
                .iter()
                .map(|candidate| divergence_event_json(&ctx.symbol, candidate, &fut))
                .collect(),
        );
        let lookback_covered_minutes = ctx.history_futures.len().min(ctx.history_spot.len()) as i64;

        let candidates_json = candidates.iter().map(candidate_payload).collect::<Vec<_>>();
        let latest_payload = latest_current.as_ref().map(candidate_payload);
        let signal = latest_current
            .as_ref()
            .map(|c| c.available_i == last_idx)
            .unwrap_or(false);

        let mut out = IndicatorComputation {
            snapshot: Some(IndicatorSnapshotRow {
                indicator_code: self.code(),
                window_code: "1m",
                payload_json: Value::Null,
            }),
            ..Default::default()
        };

        if let Some(snapshot) = out.snapshot.as_mut() {
            snapshot.payload_json = if let Some(latest_ref) = latest_current.as_ref() {
                json!({
                    "signal": signal,
                    "reason": if signal { Value::Null } else { json!("candidate_not_yet_available") },
                    "sig_test_mode": ctx.divergence_sig_test_mode.as_str(),
                    "bootstrap_b": ctx.divergence_bootstrap_b,
                    "bootstrap_block_len": ctx.divergence_bootstrap_block_len,
                    "p_value_threshold": ctx.divergence_p_value_threshold,
                    "divergence_type": divergence_label(&latest_ref.divergence_type),
                    "pivot_side": latest_ref.pivot_side.clone(),
                    "signals": signal_flags(latest_ref),
                    "fut_divergence_sign": latest_ref.fut_divergence_sign,
                    "spot_price_flow_confirm": latest_ref.spot_price_flow_confirm,
                    "spot_lead_score": latest_ref.spot_lead_score,
                    "likely_driver": latest_ref.likely_driver.clone(),
                    "latest": latest_payload,
                    "candidates": candidates_json,
                    "event_count": window_view.current_events.len(),
                    "events": window_view.current_events,
                    "recent_7d": build_recent_7d_payload(
                        window_view.recent_events,
                        lookback_covered_minutes,
                        "in_memory_minute_history"
                    ),
                    "latest_7d": window_view.latest_recent
                })
            } else {
                let mut payload = empty_snapshot_payload(ctx, "no_candidate");
                if let Some(obj) = payload.as_object_mut() {
                    obj.insert("candidates".to_string(), json!(candidates_json));
                    obj.insert(
                        "event_count".to_string(),
                        json!(window_view.current_events.len()),
                    );
                    obj.insert("events".to_string(), json!(window_view.current_events));
                    obj.insert(
                        "recent_7d".to_string(),
                        build_recent_7d_payload(
                            window_view.recent_events,
                            lookback_covered_minutes,
                            "in_memory_minute_history",
                        ),
                    );
                    obj.insert("latest_7d".to_string(), json!(window_view.latest_recent));
                }
                payload
            };
        }

        for c in candidates {
            let start_ts = fut[c.i1].ts_bucket;
            let end_ts = fut[c.i2].ts_bucket + Duration::minutes(1);
            let available_ts = fut[c.available_i].ts_bucket + Duration::minutes(1);
            let direction = if c.divergence_type.contains("bearish") {
                -1
            } else {
                1
            };
            let payload = json!({
                "event_start_ts": start_ts.to_rfc3339(),
                "event_end_ts": end_ts.to_rfc3339(),
                "event_available_ts": available_ts.to_rfc3339(),
                "pivot_ts_1": fut[c.i1].ts_bucket.to_rfc3339(),
                "pivot_ts_2": fut[c.i2].ts_bucket.to_rfc3339(),
                "pivot_confirm_ts_1": fut[c.confirm_i1].ts_bucket.to_rfc3339(),
                "pivot_confirm_ts_2": fut[c.confirm_i2].ts_bucket.to_rfc3339(),
                "leg_minutes": (fut[c.i2].ts_bucket - fut[c.i1].ts_bucket).num_minutes(),
                "price_diff": c.price_diff,
                "price_norm_diff": c.price_diff,
                "cvd_diff_fut": c.cvd_diff_fut,
                "cvd_norm_diff_fut": c.cvd_diff_fut,
                "cvd_diff_spot": c.cvd_diff_spot,
                "cvd_norm_diff_spot": c.cvd_diff_spot,
                "price_effect_z": c.price_effect_z,
                "cvd_effect_z": c.cvd_effect_z,
                "sig_pass": c.sig_pass,
                "p_value_price": c.p_value_price,
                "p_value_cvd": c.p_value_cvd,
                "sig_test_mode": ctx.divergence_sig_test_mode.as_str(),
                "spot_price_flow_confirm": c.spot_price_flow_confirm,
                "fut_divergence_sign": c.fut_divergence_sign,
                "spot_lead_score": c.spot_lead_score,
                "likely_driver": c.likely_driver.clone()
            });
            let event_id = build_indicator_event_id(
                &ctx.symbol,
                self.code(),
                &format!("{}_divergence", c.divergence_type),
                start_ts,
                Some(end_ts),
                direction,
                Some(fut[c.i1].ts_bucket),
                Some(fut[c.i2].ts_bucket),
            );
            let divergence_event_id = build_divergence_event_id(
                &ctx.symbol,
                &c.divergence_type,
                &c.pivot_side,
                start_ts,
                end_ts,
                Some(fut[c.i1].ts_bucket),
                Some(fut[c.i2].ts_bucket),
            );
            let payload = divergence_payload_json(&ctx.symbol, &c, &fut, payload);

            out.event_rows.push(IndicatorEventRow::new(
                event_id,
                self.code(),
                format!("{}_divergence", c.divergence_type),
                "warn".to_string(),
                direction,
                start_ts,
                Some(end_ts),
                available_ts,
                "1m",
                Some(fut[c.i1].ts_bucket),
                Some(fut[c.i2].ts_bucket),
                Some(fut[c.confirm_i1].ts_bucket),
                Some(fut[c.confirm_i2].ts_bucket),
                Some(c.sig_pass),
                Some(c.p_value_price.max(c.p_value_cvd)),
                Some(c.score),
                Some(c.score),
                payload.clone(),
            ));

            out.divergence_rows.push(DivergenceEventRow::new(
                divergence_event_id,
                c.divergence_type,
                c.pivot_side,
                Some(available_ts),
                Some(fut[c.i1].ts_bucket),
                Some(fut[c.i2].ts_bucket),
                Some(fut[c.confirm_i1].ts_bucket),
                Some(fut[c.confirm_i2].ts_bucket),
                Some((fut[c.i2].ts_bucket - fut[c.i1].ts_bucket).num_minutes() as i32),
                start_ts,
                end_ts,
                Some(c.price_start),
                Some(c.price_end),
                Some(c.price_diff),
                Some(c.cvd_start_fut),
                Some(c.cvd_end_fut),
                Some(c.cvd_diff_fut),
                Some(c.cvd_start_spot),
                Some(c.cvd_end_spot),
                Some(c.cvd_diff_spot),
                Some(c.price_effect_z),
                Some(c.cvd_effect_z),
                Some(c.sig_pass),
                Some(c.p_value_price),
                Some(c.p_value_cvd),
                Some(c.spot_price_flow_confirm),
                Some(c.fut_divergence_sign),
                Some(c.spot_lead_score),
                Some(c.likely_driver.clone()),
                Some(c.score),
                Some(c.score),
                "1m",
                payload,
            ));
        }

        out
    }
}

fn divergence_label(kind: &str) -> &'static str {
    match kind {
        "bearish" => "bearish_divergence",
        "hidden_bearish" => "hidden_bearish_divergence",
        "bullish" => "bullish_divergence",
        "hidden_bullish" => "hidden_bullish_divergence",
        _ => "unknown",
    }
}

fn signal_flags(candidate: &DivergenceCandidate) -> serde_json::Value {
    let label = divergence_label(&candidate.divergence_type);
    json!({
        "bearish_divergence": label == "bearish_divergence",
        "hidden_bearish_divergence": label == "hidden_bearish_divergence",
        "bullish_divergence": label == "bullish_divergence",
        "hidden_bullish_divergence": label == "hidden_bullish_divergence"
    })
}

fn candidate_payload(candidate: &DivergenceCandidate) -> serde_json::Value {
    json!({
        "type": divergence_label(&candidate.divergence_type),
        "pivot_side": candidate.pivot_side.clone(),
        "i1": candidate.i1,
        "i2": candidate.i2,
        "confirm_i1": candidate.confirm_i1,
        "confirm_i2": candidate.confirm_i2,
        "available_i": candidate.available_i,
        "price_start": candidate.price_start,
        "price_end": candidate.price_end,
        "cvd_start_fut": candidate.cvd_start_fut,
        "cvd_end_fut": candidate.cvd_end_fut,
        "cvd_start_spot": candidate.cvd_start_spot,
        "cvd_end_spot": candidate.cvd_end_spot,
        "price_diff": candidate.price_diff,
        "cvd_diff_fut": candidate.cvd_diff_fut,
        "cvd_diff_spot": candidate.cvd_diff_spot,
        "price_effect_z": candidate.price_effect_z,
        "cvd_effect_z": candidate.cvd_effect_z,
        "sig_pass": candidate.sig_pass,
        "p_value_price": candidate.p_value_price,
        "p_value_cvd": candidate.p_value_cvd,
        "score": candidate.score,
        "spot_price_flow_confirm": candidate.spot_price_flow_confirm,
        "fut_divergence_sign": candidate.fut_divergence_sign,
        "spot_lead_score": candidate.spot_lead_score,
        "likely_driver": candidate.likely_driver.clone(),
        "signals": signal_flags(candidate)
    })
}

fn divergence_payload_json(
    symbol: &str,
    candidate: &DivergenceCandidate,
    fut: &[crate::runtime::state_store::MinuteHistory],
    payload: Value,
) -> Value {
    let event_id = build_divergence_event_id(
        symbol,
        &candidate.divergence_type,
        &candidate.pivot_side,
        fut[candidate.i1].ts_bucket,
        fut[candidate.i2].ts_bucket + Duration::minutes(1),
        Some(fut[candidate.i1].ts_bucket),
        Some(fut[candidate.i2].ts_bucket),
    );
    let mut base = Map::new();
    base.insert("event_id".to_string(), json!(event_id));
    base.insert(
        "type".to_string(),
        json!(format!("{}_divergence", candidate.divergence_type)),
    );
    base.insert("pivot_side".to_string(), json!(candidate.pivot_side));
    base.insert(
        "start_ts".to_string(),
        json!(fut[candidate.i1].ts_bucket.to_rfc3339()),
    );
    base.insert(
        "end_ts".to_string(),
        json!((fut[candidate.i2].ts_bucket + Duration::minutes(1)).to_rfc3339()),
    );
    base.insert(
        "event_available_ts".to_string(),
        json!((fut[candidate.available_i].ts_bucket + Duration::minutes(1)).to_rfc3339()),
    );
    base.insert("score".to_string(), json!(candidate.score));
    merge_payload_fields(base, &payload)
}

fn divergence_event_json(
    symbol: &str,
    candidate: &DivergenceCandidate,
    fut: &[crate::runtime::state_store::MinuteHistory],
) -> (chrono::DateTime<chrono::Utc>, Value) {
    let start_ts = fut[candidate.i1].ts_bucket;
    let end_ts = fut[candidate.i2].ts_bucket + Duration::minutes(1);
    let available_ts = fut[candidate.available_i].ts_bucket + Duration::minutes(1);
    let payload = json!({
        "event_start_ts": start_ts.to_rfc3339(),
        "event_end_ts": end_ts.to_rfc3339(),
        "event_available_ts": available_ts.to_rfc3339(),
        "pivot_ts_1": fut[candidate.i1].ts_bucket.to_rfc3339(),
        "pivot_ts_2": fut[candidate.i2].ts_bucket.to_rfc3339(),
        "pivot_confirm_ts_1": fut[candidate.confirm_i1].ts_bucket.to_rfc3339(),
        "pivot_confirm_ts_2": fut[candidate.confirm_i2].ts_bucket.to_rfc3339(),
        "leg_minutes": (fut[candidate.i2].ts_bucket - fut[candidate.i1].ts_bucket).num_minutes(),
        "price_diff": candidate.price_diff,
        "price_norm_diff": candidate.price_diff,
        "cvd_diff_fut": candidate.cvd_diff_fut,
        "cvd_norm_diff_fut": candidate.cvd_diff_fut,
        "cvd_diff_spot": candidate.cvd_diff_spot,
        "cvd_norm_diff_spot": candidate.cvd_diff_spot,
        "price_effect_z": candidate.price_effect_z,
        "cvd_effect_z": candidate.cvd_effect_z,
        "sig_pass": candidate.sig_pass,
        "p_value_price": candidate.p_value_price,
        "p_value_cvd": candidate.p_value_cvd,
        "sig_test_mode": "derived",
        "spot_price_flow_confirm": candidate.spot_price_flow_confirm,
        "fut_divergence_sign": candidate.fut_divergence_sign,
        "spot_lead_score": candidate.spot_lead_score,
        "likely_driver": candidate.likely_driver
    });
    (
        available_ts,
        divergence_payload_json(symbol, candidate, fut, payload),
    )
}

fn empty_snapshot_payload(ctx: &IndicatorContext, reason: &str) -> serde_json::Value {
    json!({
        "signal": false,
        "reason": reason,
        "sig_test_mode": ctx.divergence_sig_test_mode.as_str(),
        "bootstrap_b": ctx.divergence_bootstrap_b,
        "bootstrap_block_len": ctx.divergence_bootstrap_block_len,
        "p_value_threshold": ctx.divergence_p_value_threshold,
        "divergence_type": Value::Null,
        "pivot_side": Value::Null,
        "signals": {
            "bearish_divergence": false,
            "hidden_bearish_divergence": false,
            "bullish_divergence": false,
            "hidden_bullish_divergence": false
        },
        "fut_divergence_sign": Value::Null,
        "spot_price_flow_confirm": Value::Null,
        "spot_lead_score": Value::Null,
        "likely_driver": Value::Null,
        "latest": Value::Null,
        "candidates": []
    })
}

fn detrend_rolling_ols(values: &[f64], window: usize) -> Vec<Option<f64>> {
    let mut out = vec![None; values.len()];
    for i in 0..values.len() {
        if i + 1 < window {
            continue;
        }
        let start = i + 1 - window;
        let slice = &values[start..=i];
        let n = slice.len() as f64;
        let x_mean = (n - 1.0) / 2.0;
        let y_mean = slice.iter().sum::<f64>() / n;
        let mut num = 0.0;
        let mut den = 0.0;
        for (idx, y) in slice.iter().enumerate() {
            let x = idx as f64;
            num += (x - x_mean) * (y - y_mean);
            den += (x - x_mean) * (x - x_mean);
        }
        let slope = if den > 1e-12 { num / den } else { 0.0 };
        let intercept = y_mean - slope * x_mean;
        let pred = intercept + slope * (window as f64 - 1.0);
        out[i] = Some(values[i] - pred);
    }
    out
}

fn robust_z_series(values: &[Option<f64>], lookback: usize) -> Vec<Option<f64>> {
    let mut raw = Vec::with_capacity(values.len());
    let mut out = vec![None; values.len()];
    for (i, v) in values.iter().enumerate() {
        raw.push(v.unwrap_or(0.0));
        out[i] = robust_z_at(&raw, i, lookback);
    }
    out
}

fn confirmed_high_pivots(highs: &[f64], k: usize, last_idx: usize) -> Vec<(usize, usize)> {
    let mut pivots = Vec::new();
    if highs.len() <= k * 2 {
        return pivots;
    }
    for i in k..(highs.len() - k) {
        if i + k > last_idx {
            break;
        }
        let left_max = highs[i - k..i]
            .iter()
            .fold(f64::NEG_INFINITY, |a, b| a.max(*b));
        let right_max = highs[i + 1..=i + k]
            .iter()
            .fold(f64::NEG_INFINITY, |a, b| a.max(*b));
        if highs[i] > left_max && highs[i] >= right_max {
            pivots.push((i, i + k));
        }
    }
    pivots
}

fn confirmed_low_pivots(lows: &[f64], k: usize, last_idx: usize) -> Vec<(usize, usize)> {
    let mut pivots = Vec::new();
    if lows.len() <= k * 2 {
        return pivots;
    }
    for i in k..(lows.len() - k) {
        if i + k > last_idx {
            break;
        }
        let left_min = lows[i - k..i].iter().fold(f64::INFINITY, |a, b| a.min(*b));
        let right_min = lows[i + 1..=i + k]
            .iter()
            .fold(f64::INFINITY, |a, b| a.min(*b));
        if lows[i] < left_min && lows[i] <= right_min {
            pivots.push((i, i + k));
        }
    }
    pivots
}

fn rolling_atr(highs: &[f64], lows: &[f64], closes: &[f64], lookback: usize) -> Vec<Option<f64>> {
    let mut out = vec![None; highs.len()];
    if highs.is_empty() || lows.len() != highs.len() || closes.len() != highs.len() {
        return out;
    }
    let mut trs = Vec::with_capacity(highs.len());
    for i in 0..highs.len() {
        let prev_close = if i > 0 { closes[i - 1] } else { closes[i] };
        let tr = (highs[i] - lows[i])
            .max((highs[i] - prev_close).abs())
            .max((lows[i] - prev_close).abs());
        trs.push(tr);
        if i + 1 >= lookback {
            let start = i + 1 - lookback;
            let m = trs[start..=i].iter().sum::<f64>() / lookback as f64;
            out[i] = Some(m);
        }
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn all_candidates(
    is_high_side: bool,
    pivots: &[(usize, usize)],
    fut: &[crate::runtime::state_store::MinuteHistory],
    spot: &[crate::runtime::state_store::MinuteHistory],
    _closes: &[f64],
    highs: &[f64],
    lows: &[f64],
    detrended_price: &[Option<f64>],
    z_cvd_fut: &[Option<f64>],
    z_cvd_spot: &[Option<f64>],
    atr: &[Option<f64>],
    sig_test_mode: DivergenceSigTestMode,
    bootstrap_b: usize,
    bootstrap_block_len: usize,
    p_value_threshold: f64,
) -> Vec<DivergenceCandidate> {
    if pivots.len() < 2 {
        return Vec::new();
    }
    let mut out = Vec::new();
    for pair in pivots.windows(2) {
        let (i1, c1) = pair[0];
        let (i2, c2) = pair[1];
        let leg = (fut[i2].ts_bucket - fut[i1].ts_bucket).num_minutes();
        if !(MIN_LEG_GAP_MINUTES..=MAX_LEG_GAP_MINUTES).contains(&leg) {
            continue;
        }
        let atr_v = atr[i2].unwrap_or(0.0);
        if atr_v <= 1e-12 {
            continue;
        }
        let close1 = fut[i1]
            .close_price
            .or(fut[i1].last_price)
            .unwrap_or_default();
        let close2 = fut[i2]
            .close_price
            .or(fut[i2].last_price)
            .unwrap_or_default();
        let leg_eff = (close2 - close1).abs() / (atr_v + 1e-12);
        if leg_eff < ETA_LEG {
            continue;
        }
        let (Some(zc1), Some(zc2), Some(zs1), Some(zs2)) =
            (z_cvd_fut[i1], z_cvd_fut[i2], z_cvd_spot[i1], z_cvd_spot[i2])
        else {
            continue;
        };

        let (price_start, price_end, price_diff) = if is_high_side {
            (highs[i1], highs[i2], highs[i2] - highs[i1])
        } else {
            (lows[i1], lows[i2], lows[i2] - lows[i1])
        };
        let cvd_diff_fut = zc2 - zc1;
        let cvd_diff_spot = zs2 - zs1;
        let eps_price = 0.5 * atr_v;
        let price_effect_z = price_diff / (atr_v + 1e-12);
        let cvd_effect_z = cvd_diff_fut;
        let divergence_type = if is_high_side {
            if price_diff >= eps_price && cvd_diff_fut <= -EPS_CVD_Z {
                Some("bearish")
            } else if price_diff <= -eps_price && cvd_diff_fut >= EPS_CVD_Z {
                Some("hidden_bearish")
            } else {
                None
            }
        } else if price_diff <= -eps_price && cvd_diff_fut >= EPS_CVD_Z {
            Some("bullish")
        } else if price_diff >= eps_price && cvd_diff_fut <= -EPS_CVD_Z {
            Some("hidden_bullish")
        } else {
            None
        };
        let Some(divergence_type) = divergence_type else {
            continue;
        };

        let mut p_value_price = if price_effect_z.abs() >= ZP_MIN {
            0.0
        } else {
            1.0
        };
        let mut p_value_cvd = if cvd_effect_z.abs() >= ZC_MIN {
            0.0
        } else {
            1.0
        };
        let mut sig_pass = price_effect_z.abs() >= ZP_MIN && cvd_effect_z.abs() >= ZC_MIN;

        if sig_test_mode == DivergenceSigTestMode::BlockBootstrap {
            let leg_len = (i2 - i1 + 1).max(2);
            let price_returns = build_returns_from_option_series(detrended_price, i2);
            let cvd_returns = build_returns_from_option_series(z_cvd_fut, i2);

            if price_returns.len() >= BOOTSTRAP_MIN_RET_SAMPLES {
                if let Some(p) = block_bootstrap_pvalue(
                    &price_returns,
                    leg_len,
                    price_effect_z.abs(),
                    bootstrap_b,
                    bootstrap_block_len,
                    ((i1 as u64) << 32) ^ (i2 as u64) ^ 0xA5A5_5A5A_u64,
                ) {
                    p_value_price = p;
                }
            }

            if cvd_returns.len() >= BOOTSTRAP_MIN_RET_SAMPLES {
                if let Some(p) = block_bootstrap_pvalue(
                    &cvd_returns,
                    leg_len,
                    cvd_effect_z.abs(),
                    bootstrap_b,
                    bootstrap_block_len,
                    ((i1 as u64) << 32) ^ (i2 as u64) ^ 0x5AA5_A55A_u64,
                ) {
                    p_value_cvd = p;
                }
            }

            sig_pass = p_value_price <= p_value_threshold && p_value_cvd <= p_value_threshold;
        }

        if !sig_pass {
            continue;
        }

        let delta_p_sign = price_diff.signum() as i16;
        let spot_flow_confirm = delta_p_sign == (cvd_diff_spot.signum() as i16);
        let fut_div_sign = (price_diff.signum() * cvd_diff_fut.signum()) as i16;
        let spot_lead_score = if (cvd_diff_spot.abs() + cvd_diff_fut.abs()) > 1e-12
            && (price_diff.signum() == cvd_diff_spot.signum())
            && (price_diff.signum() != cvd_diff_fut.signum())
        {
            cvd_diff_spot.abs() / (cvd_diff_spot.abs() + cvd_diff_fut.abs() + 1e-12)
        } else {
            0.0
        };
        let likely_driver = if spot_lead_score >= 0.6 {
            "spot_led"
        } else if spot_lead_score < 0.3 && fut_div_sign != -1 {
            "futures_led"
        } else {
            "mixed"
        };
        let score =
            clip01(1.0 * (0.5 * (price_effect_z.abs() / 3.0) + 0.5 * (cvd_effect_z.abs() / 3.0)));
        let available_i = c1.max(c2);

        out.push(DivergenceCandidate {
            divergence_type: divergence_type.to_string(),
            pivot_side: if is_high_side {
                "high".to_string()
            } else {
                "low".to_string()
            },
            i1,
            i2,
            confirm_i1: c1,
            confirm_i2: c2,
            available_i,
            price_start,
            price_end,
            cvd_start_fut: fut[i1].cvd,
            cvd_end_fut: fut[i2].cvd,
            cvd_start_spot: spot[i1].cvd,
            cvd_end_spot: spot[i2].cvd,
            price_diff,
            cvd_diff_fut,
            cvd_diff_spot,
            price_effect_z,
            cvd_effect_z,
            sig_pass,
            p_value_price,
            p_value_cvd,
            score,
            spot_price_flow_confirm: spot_flow_confirm,
            fut_divergence_sign: fut_div_sign,
            spot_lead_score,
            likely_driver: likely_driver.to_string(),
        });
    }
    out
}

fn build_returns_from_option_series(series: &[Option<f64>], end_idx: usize) -> Vec<f64> {
    if end_idx == 0 || series.is_empty() {
        return Vec::new();
    }
    let upper = end_idx.min(series.len() - 1);
    let mut vals = Vec::new();
    for v in series.iter().take(upper + 1).flatten() {
        vals.push(*v);
    }
    if vals.len() < 3 {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(vals.len().saturating_sub(1));
    for i in 1..vals.len() {
        out.push(vals[i] - vals[i - 1]);
    }
    out
}

fn block_bootstrap_pvalue(
    returns: &[f64],
    leg_len: usize,
    observed_abs_z: f64,
    bootstrap_b: usize,
    block_len: usize,
    seed: u64,
) -> Option<f64> {
    if returns.len() < BOOTSTRAP_MIN_RET_SAMPLES || leg_len < 2 || bootstrap_b == 0 {
        return None;
    }
    let std = stddev_slice(returns)?;
    if std <= 1e-12 {
        return Some(1.0);
    }

    let block = block_len.clamp(1, returns.len().saturating_sub(1).max(1));
    let mut rng = XorShift64::new(seed.max(1));
    let mut hit = 0usize;

    for _ in 0..bootstrap_b {
        let mut sampled = Vec::with_capacity(leg_len);
        while sampled.len() < leg_len {
            let max_start = returns.len().saturating_sub(block);
            let start = if max_start == 0 {
                0
            } else {
                rng.next_usize(max_start + 1)
            };
            for j in 0..block {
                if sampled.len() >= leg_len {
                    break;
                }
                sampled.push(returns[start + j]);
            }
        }

        let sum = sampled.iter().sum::<f64>();
        let z = sum.abs() / (std * (leg_len as f64).sqrt() + 1e-12);
        if z >= observed_abs_z {
            hit += 1;
        }
    }

    Some((hit as f64 + 1.0) / (bootstrap_b as f64 + 1.0))
}

fn stddev_slice(values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let var = values
        .iter()
        .map(|v| {
            let d = *v - mean;
            d * d
        })
        .sum::<f64>()
        / values.len() as f64;
    Some(var.sqrt())
}

#[derive(Clone, Copy)]
struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn next_usize(&mut self, bound: usize) -> usize {
        if bound <= 1 {
            0
        } else {
            (self.next_u64() as usize) % bound
        }
    }
}
