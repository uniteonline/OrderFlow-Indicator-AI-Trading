use crate::indicators::context::{
    clip01, ExhaustionEventRow, IndicatorComputation, IndicatorContext, IndicatorSnapshotRow,
};
use crate::indicators::indicator_trait::Indicator;
use crate::indicators::shared::event_ids::build_exhaustion_event_id;
use crate::indicators::shared::event_views::{
    build_event_window_view, build_recent_7d_payload, merge_payload_fields,
};
use crate::runtime::state_store::MinuteHistory;
use chrono::Duration;
use serde_json::{json, Map, Value};

const TICK_SIZE: f64 = 0.01;
const PIVOT_LEFT: usize = 3;
const PIVOT_RIGHT: usize = 3;
const MIN_LEG_GAP_MINUTES: i64 = 3;
const MAX_LEG_GAP_MINUTES: i64 = 180;
const EPSILON_PRICE_TICKS: f64 = 2.0;
const EPSILON_CONFIRM_TICKS: f64 = 2.0;
const EPSILON_DELTA: f64 = 20.0;
const EPSILON_RDELTA: f64 = 0.05;
const ETA_REJECT: f64 = 0.65;
const CONFIRM_BARS: usize = 5;
const LAMBDA_SPOT_PENALTY: f64 = 0.20;
const THETA_BUY_CVD: f64 = 0.0;
const THETA_BUY_WHALE: f64 = 100_000.0;
const THETA_SELL_CVD: f64 = 0.0;
const THETA_SELL_WHALE: f64 = -100_000.0;

#[derive(Debug, Clone)]
pub(crate) struct ExhaustionEventData {
    pub direction: i16,
    pub event_type: String,
    pub start_ts: chrono::DateTime<chrono::Utc>,
    pub end_ts: chrono::DateTime<chrono::Utc>,
    pub confirm_ts: chrono::DateTime<chrono::Utc>,
    pub pivot_price: f64,
    pub pivot_ts_1: chrono::DateTime<chrono::Utc>,
    pub pivot_ts_2: chrono::DateTime<chrono::Utc>,
    pub pivot_confirm_ts_1: chrono::DateTime<chrono::Utc>,
    pub pivot_confirm_ts_2: chrono::DateTime<chrono::Utc>,
    pub price_push_ticks: f64,
    pub delta_change: f64,
    pub rdelta_change: f64,
    pub reject_ratio: f64,
    pub confirm_speed: f64,
    pub spot_cvd_push_post_pivot: f64,
    pub spot_whale_push_post_pivot: f64,
    pub spot_continuation_risk: bool,
    pub spot_exhaustion_confirm: bool,
    pub score: f64,
    pub payload: Value,
}

pub(crate) fn detect_exhaustion_all_history(ctx: &IndicatorContext) -> Vec<ExhaustionEventData> {
    let n = ctx.history_futures.len().min(ctx.history_spot.len());
    if n < (PIVOT_LEFT + PIVOT_RIGHT + CONFIRM_BARS + 3) {
        return Vec::new();
    }

    let fut = &ctx.history_futures[ctx.history_futures.len() - n..];
    let spot = &ctx.history_spot[ctx.history_spot.len() - n..];
    let last_idx = n - 1;

    let highs = fut
        .iter()
        .map(|h| h.high_price.or(h.last_price).unwrap_or(0.0))
        .collect::<Vec<_>>();
    let lows = fut
        .iter()
        .map(|h| h.low_price.or(h.last_price).unwrap_or(0.0))
        .collect::<Vec<_>>();
    let closes = fut
        .iter()
        .map(|h| h.close_price.or(h.last_price).unwrap_or(0.0))
        .collect::<Vec<_>>();
    let deltas = fut.iter().map(|h| h.delta).collect::<Vec<_>>();
    let rdeltas = fut.iter().map(|h| h.relative_delta).collect::<Vec<_>>();

    let spot_cvd = spot.iter().map(|h| h.cvd).collect::<Vec<_>>();
    let spot_whale_notional = spot
        .iter()
        .map(|h| {
            let px = h.close_price.or(h.last_price).unwrap_or(0.0);
            h.delta * px
        })
        .collect::<Vec<_>>();

    let high_pivots = confirmed_high_pivots(&highs);
    let low_pivots = confirmed_low_pivots(&lows);

    let mut out = Vec::new();
    out.extend(detect_buying_events(
        fut,
        &highs,
        &lows,
        &closes,
        &deltas,
        &rdeltas,
        &spot_cvd,
        &spot_whale_notional,
        &high_pivots,
        last_idx,
    ));
    out.extend(detect_selling_events(
        fut,
        &highs,
        &lows,
        &closes,
        &deltas,
        &rdeltas,
        &spot_cvd,
        &spot_whale_notional,
        &low_pivots,
        last_idx,
    ));
    out
}

pub(crate) fn exhaustion_event_json(
    symbol: &str,
    indicator_code: &'static str,
    event: &ExhaustionEventData,
) -> (chrono::DateTime<chrono::Utc>, Value) {
    let event_id = build_exhaustion_event_id(
        symbol,
        indicator_code,
        &event.event_type,
        event.direction,
        event.confirm_ts,
        event.pivot_ts_1,
        event.pivot_ts_2,
    );
    let mut base = Map::new();
    base.insert("event_id".to_string(), json!(event_id));
    base.insert("type".to_string(), json!(event.event_type));
    base.insert("direction".to_string(), json!(event.direction));
    base.insert("start_ts".to_string(), json!(event.start_ts.to_rfc3339()));
    base.insert("end_ts".to_string(), json!(event.end_ts.to_rfc3339()));
    base.insert(
        "event_available_ts".to_string(),
        json!(event.confirm_ts.to_rfc3339()),
    );
    base.insert(
        "confirm_ts".to_string(),
        json!(event.confirm_ts.to_rfc3339()),
    );
    base.insert("score".to_string(), json!(event.score));
    base.insert("indicator_code".to_string(), json!(indicator_code));
    base.insert("pivot_price".to_string(), json!(event.pivot_price));
    match event.event_type.as_str() {
        "buying_exhaustion" => {
            base.insert("delta_drop".to_string(), json!(-event.delta_change));
            base.insert("rdelta_drop".to_string(), json!(-event.rdelta_change));
        }
        "selling_exhaustion" => {
            base.insert("delta_lift".to_string(), json!(event.delta_change));
            base.insert("rdelta_lift".to_string(), json!(event.rdelta_change));
        }
        _ => {}
    }
    (event.confirm_ts, merge_payload_fields(base, &event.payload))
}

pub(crate) fn append_exhaustion_rows(
    out: &mut IndicatorComputation,
    symbol: &str,
    indicator_code: &'static str,
    events: &[ExhaustionEventData],
) {
    for event in events {
        let event_id = build_exhaustion_event_id(
            symbol,
            indicator_code,
            &event.event_type,
            event.direction,
            event.confirm_ts,
            event.pivot_ts_1,
            event.pivot_ts_2,
        );
        let payload_json = exhaustion_event_json(symbol, indicator_code, event).1;
        out.exhaustion_rows.push(ExhaustionEventRow {
            event_id,
            event_type: event.event_type.clone(),
            direction: event.direction,
            ts_event_start: event.start_ts,
            ts_event_end: event.end_ts,
            confirm_ts: event.confirm_ts,
            event_available_ts: event.confirm_ts,
            pivot_ts_1: Some(event.pivot_ts_1),
            pivot_ts_2: Some(event.pivot_ts_2),
            pivot_confirm_ts_1: Some(event.pivot_confirm_ts_1),
            pivot_confirm_ts_2: Some(event.pivot_confirm_ts_2),
            price_push_ticks: Some(event.price_push_ticks),
            delta_change: Some(event.delta_change),
            rdelta_change: Some(event.rdelta_change),
            reject_ratio: Some(event.reject_ratio),
            confirm_speed: Some(event.confirm_speed),
            spot_cvd_push_post_pivot: Some(event.spot_cvd_push_post_pivot),
            spot_whale_push_post_pivot: Some(event.spot_whale_push_post_pivot),
            spot_continuation_risk: Some(event.spot_continuation_risk),
            spot_exhaustion_confirm: Some(event.spot_exhaustion_confirm),
            score: Some(event.score),
            confidence: Some(event.score),
            window_code: "1m",
            payload_json,
        });
    }
}

#[allow(clippy::too_many_arguments)]
fn detect_buying_events(
    fut: &[MinuteHistory],
    highs: &[f64],
    lows: &[f64],
    closes: &[f64],
    deltas: &[f64],
    rdeltas: &[f64],
    spot_cvd: &[f64],
    spot_whale_notional: &[f64],
    pivots: &[usize],
    last_idx: usize,
) -> Vec<ExhaustionEventData> {
    let mut events = Vec::new();
    let eps_price = EPSILON_PRICE_TICKS * TICK_SIZE;
    let eps_confirm = EPSILON_CONFIRM_TICKS * TICK_SIZE;

    for pair in pivots.windows(2) {
        let t1 = pair[0];
        let t2 = pair[1];
        if t2 + 1 >= fut.len() {
            continue;
        }

        let leg_minutes = (fut[t2].ts_bucket - fut[t1].ts_bucket).num_minutes();
        if !(MIN_LEG_GAP_MINUTES..=MAX_LEG_GAP_MINUTES).contains(&leg_minutes) {
            continue;
        }

        let range = (highs[t2] - lows[t2]).max(TICK_SIZE);
        let reject_top = (highs[t2] - closes[t2]) / (range + 1e-12);

        let cond = highs[t2] >= highs[t1] + eps_price
            && deltas[t2] <= deltas[t1] - EPSILON_DELTA
            && rdeltas[t2] <= rdeltas[t1] - EPSILON_RDELTA
            && reject_top >= ETA_REJECT;
        if !cond {
            continue;
        }

        let end = (t2 + CONFIRM_BARS).min(last_idx);
        let confirm_idx = ((t2 + 1)..=end).find(|&t| closes[t] <= lows[t2] - eps_confirm);
        let Some(tc) = confirm_idx else {
            continue;
        };

        let price_push = (highs[t2] - highs[t1]) / TICK_SIZE;
        let delta_drop = deltas[t1] - deltas[t2];
        let rdelta_drop = rdeltas[t1] - rdeltas[t2];
        let delta_change = deltas[t2] - deltas[t1];
        let rdelta_change = rdeltas[t2] - rdeltas[t1];
        let confirm_speed = 1.0 / ((tc - t2) as f64).max(1.0);
        let base_score = 0.30 * clip01(price_push / 10.0)
            + 0.25 * clip01(delta_drop / (3.0 * EPSILON_DELTA))
            + 0.20 * clip01(rdelta_drop / (3.0 * EPSILON_RDELTA))
            + 0.15 * clip01((reject_top - ETA_REJECT) / (1.0 - ETA_REJECT))
            + 0.10 * clip01(confirm_speed * CONFIRM_BARS as f64);

        let spot_cvd_push = spot_cvd[tc] - spot_cvd[t2];
        let spot_whale_push = spot_whale_notional[t2..=tc].iter().sum::<f64>();
        let spot_continuation_risk =
            spot_cvd_push > THETA_BUY_CVD || spot_whale_push > THETA_BUY_WHALE;
        let spot_exhaustion_confirm =
            spot_cvd_push <= THETA_BUY_CVD && spot_whale_push <= THETA_BUY_WHALE;
        let final_score = clip01(
            base_score
                * (1.0 - LAMBDA_SPOT_PENALTY * if spot_continuation_risk { 1.0 } else { 0.0 })
                + (1.0 - base_score) * 0.10 * if spot_exhaustion_confirm { 1.0 } else { 0.0 },
        );

        let start_ts = fut[t1].ts_bucket;
        let confirm_ts = fut[tc].ts_bucket + Duration::minutes(1);
        let end_ts = confirm_ts;

        events.push(ExhaustionEventData {
            direction: -1,
            event_type: "buying_exhaustion".to_string(),
            start_ts,
            end_ts,
            confirm_ts,
            pivot_price: highs[t2],
            pivot_ts_1: fut[t1].ts_bucket,
            pivot_ts_2: fut[t2].ts_bucket,
            pivot_confirm_ts_1: fut[t1].ts_bucket + Duration::minutes(PIVOT_RIGHT as i64),
            pivot_confirm_ts_2: fut[t2].ts_bucket + Duration::minutes(PIVOT_RIGHT as i64),
            price_push_ticks: price_push,
            delta_change,
            rdelta_change,
            reject_ratio: reject_top,
            confirm_speed,
            spot_cvd_push_post_pivot: spot_cvd_push,
            spot_whale_push_post_pivot: spot_whale_push,
            spot_continuation_risk,
            spot_exhaustion_confirm,
            score: final_score,
            payload: json!({
                "event_start_ts": start_ts.to_rfc3339(),
                "event_end_ts": end_ts.to_rfc3339(),
                "event_available_ts": confirm_ts.to_rfc3339(),
                "pivot_price": highs[t2],
                "pivot_ts_1": fut[t1].ts_bucket.to_rfc3339(),
                "pivot_ts_2": fut[t2].ts_bucket.to_rfc3339(),
                "pivot_confirm_ts_1": (fut[t1].ts_bucket + Duration::minutes(PIVOT_RIGHT as i64)).to_rfc3339(),
                "pivot_confirm_ts_2": (fut[t2].ts_bucket + Duration::minutes(PIVOT_RIGHT as i64)).to_rfc3339(),
                "price_push_ticks": price_push,
                "delta_drop": delta_drop,
                "rdelta_drop": rdelta_drop,
                "reject_ratio": reject_top,
                "confirm_speed": confirm_speed,
                "spot_cvd_push_post_pivot": spot_cvd_push,
                "spot_whale_push_post_pivot": spot_whale_push,
                "spot_continuation_risk": spot_continuation_risk,
                "spot_exhaustion_confirm": spot_exhaustion_confirm,
                "strength_score_xmk": final_score,
                "exhaustion_quality_score": final_score,
                "sig_pass": true
            }),
        });
    }

    events
}

#[allow(clippy::too_many_arguments)]
fn detect_selling_events(
    fut: &[MinuteHistory],
    highs: &[f64],
    lows: &[f64],
    closes: &[f64],
    deltas: &[f64],
    rdeltas: &[f64],
    spot_cvd: &[f64],
    spot_whale_notional: &[f64],
    pivots: &[usize],
    last_idx: usize,
) -> Vec<ExhaustionEventData> {
    let mut events = Vec::new();
    let eps_price = EPSILON_PRICE_TICKS * TICK_SIZE;
    let eps_confirm = EPSILON_CONFIRM_TICKS * TICK_SIZE;

    for pair in pivots.windows(2) {
        let t1 = pair[0];
        let t2 = pair[1];
        if t2 + 1 >= fut.len() {
            continue;
        }

        let leg_minutes = (fut[t2].ts_bucket - fut[t1].ts_bucket).num_minutes();
        if !(MIN_LEG_GAP_MINUTES..=MAX_LEG_GAP_MINUTES).contains(&leg_minutes) {
            continue;
        }

        let range = (highs[t2] - lows[t2]).max(TICK_SIZE);
        let reject_bottom = (closes[t2] - lows[t2]) / (range + 1e-12);

        let cond = lows[t2] <= lows[t1] - eps_price
            && deltas[t2] >= deltas[t1] + EPSILON_DELTA
            && rdeltas[t2] >= rdeltas[t1] + EPSILON_RDELTA
            && reject_bottom >= ETA_REJECT;
        if !cond {
            continue;
        }

        let end = (t2 + CONFIRM_BARS).min(last_idx);
        let confirm_idx = ((t2 + 1)..=end).find(|&t| closes[t] >= highs[t2] + eps_confirm);
        let Some(tc) = confirm_idx else {
            continue;
        };

        let price_push = (lows[t1] - lows[t2]) / TICK_SIZE;
        let delta_lift = deltas[t2] - deltas[t1];
        let rdelta_lift = rdeltas[t2] - rdeltas[t1];
        let delta_change = deltas[t2] - deltas[t1];
        let rdelta_change = rdeltas[t2] - rdeltas[t1];
        let confirm_speed = 1.0 / ((tc - t2) as f64).max(1.0);
        let base_score = 0.30 * clip01(price_push / 10.0)
            + 0.25 * clip01(delta_lift / (3.0 * EPSILON_DELTA))
            + 0.20 * clip01(rdelta_lift / (3.0 * EPSILON_RDELTA))
            + 0.15 * clip01((reject_bottom - ETA_REJECT) / (1.0 - ETA_REJECT))
            + 0.10 * clip01(confirm_speed * CONFIRM_BARS as f64);

        let spot_cvd_push = spot_cvd[tc] - spot_cvd[t2];
        let spot_whale_push = spot_whale_notional[t2..=tc].iter().sum::<f64>();
        let spot_continuation_risk =
            spot_cvd_push < THETA_SELL_CVD || spot_whale_push < THETA_SELL_WHALE;
        let spot_exhaustion_confirm =
            spot_cvd_push >= THETA_SELL_CVD && spot_whale_push >= THETA_SELL_WHALE;
        let final_score = clip01(
            base_score
                * (1.0 - LAMBDA_SPOT_PENALTY * if spot_continuation_risk { 1.0 } else { 0.0 })
                + (1.0 - base_score) * 0.10 * if spot_exhaustion_confirm { 1.0 } else { 0.0 },
        );

        let start_ts = fut[t1].ts_bucket;
        let confirm_ts = fut[tc].ts_bucket + Duration::minutes(1);
        let end_ts = confirm_ts;

        events.push(ExhaustionEventData {
            direction: 1,
            event_type: "selling_exhaustion".to_string(),
            start_ts,
            end_ts,
            confirm_ts,
            pivot_price: lows[t2],
            pivot_ts_1: fut[t1].ts_bucket,
            pivot_ts_2: fut[t2].ts_bucket,
            pivot_confirm_ts_1: fut[t1].ts_bucket + Duration::minutes(PIVOT_RIGHT as i64),
            pivot_confirm_ts_2: fut[t2].ts_bucket + Duration::minutes(PIVOT_RIGHT as i64),
            price_push_ticks: price_push,
            delta_change,
            rdelta_change,
            reject_ratio: reject_bottom,
            confirm_speed,
            spot_cvd_push_post_pivot: spot_cvd_push,
            spot_whale_push_post_pivot: spot_whale_push,
            spot_continuation_risk,
            spot_exhaustion_confirm,
            score: final_score,
            payload: json!({
                "event_start_ts": start_ts.to_rfc3339(),
                "event_end_ts": end_ts.to_rfc3339(),
                "event_available_ts": confirm_ts.to_rfc3339(),
                "pivot_price": lows[t2],
                "pivot_ts_1": fut[t1].ts_bucket.to_rfc3339(),
                "pivot_ts_2": fut[t2].ts_bucket.to_rfc3339(),
                "pivot_confirm_ts_1": (fut[t1].ts_bucket + Duration::minutes(PIVOT_RIGHT as i64)).to_rfc3339(),
                "pivot_confirm_ts_2": (fut[t2].ts_bucket + Duration::minutes(PIVOT_RIGHT as i64)).to_rfc3339(),
                "price_push_ticks": price_push,
                "delta_lift": delta_lift,
                "rdelta_lift": rdelta_lift,
                "reject_ratio": reject_bottom,
                "confirm_speed": confirm_speed,
                "spot_cvd_push_post_pivot": spot_cvd_push,
                "spot_whale_push_post_pivot": spot_whale_push,
                "spot_continuation_risk": spot_continuation_risk,
                "spot_exhaustion_confirm": spot_exhaustion_confirm,
                "strength_score_xmk": final_score,
                "exhaustion_quality_score": final_score,
                "sig_pass": true
            }),
        });
    }

    events
}

fn confirmed_high_pivots(highs: &[f64]) -> Vec<usize> {
    let mut out = Vec::new();
    if highs.len() <= PIVOT_LEFT + PIVOT_RIGHT {
        return out;
    }
    for i in PIVOT_LEFT..(highs.len() - PIVOT_RIGHT) {
        let left_max = highs[i - PIVOT_LEFT..i]
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        let right_max = highs[i + 1..=i + PIVOT_RIGHT]
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        if highs[i] > left_max && highs[i] >= right_max {
            out.push(i);
        }
    }
    out
}

fn confirmed_low_pivots(lows: &[f64]) -> Vec<usize> {
    let mut out = Vec::new();
    if lows.len() <= PIVOT_LEFT + PIVOT_RIGHT {
        return out;
    }
    for i in PIVOT_LEFT..(lows.len() - PIVOT_RIGHT) {
        let left_min = lows[i - PIVOT_LEFT..i]
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min);
        let right_min = lows[i + 1..=i + PIVOT_RIGHT]
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min);
        if lows[i] < left_min && lows[i] <= right_min {
            out.push(i);
        }
    }
    out
}

pub struct I12BuyingExhaustion;

impl Indicator for I12BuyingExhaustion {
    fn code(&self) -> &'static str {
        "buying_exhaustion"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation {
        let all_events = detect_exhaustion_all_history(ctx)
            .into_iter()
            .filter(|e| e.event_type == "buying_exhaustion")
            .collect::<Vec<_>>();
        let window_view = build_event_window_view(
            ctx.ts_bucket,
            all_events
                .iter()
                .map(|event| exhaustion_event_json(&ctx.symbol, self.code(), event))
                .collect(),
        );
        let lookback_covered_minutes = ctx.history_futures.len().min(ctx.history_spot.len()) as i64;

        let mut out = IndicatorComputation {
            snapshot: Some(IndicatorSnapshotRow {
                indicator_code: self.code(),
                window_code: "1m",
                payload_json: json!({
                    "recent_7d": build_recent_7d_payload(
                        window_view.recent_events,
                        lookback_covered_minutes,
                        "in_memory_minute_history"
                    )
                }),
            }),
            ..Default::default()
        };

        append_exhaustion_rows(&mut out, &ctx.symbol, self.code(), &all_events);

        out
    }
}

#[cfg(test)]
mod tests {
    use super::{
        detect_buying_events, detect_selling_events, exhaustion_event_json, ExhaustionEventData,
    };
    use crate::ingest::decoder::MarketKind;
    use crate::runtime::state_store::MinuteHistory;
    use chrono::{Duration, TimeZone, Utc};
    use serde_json::json;
    use std::collections::BTreeMap;

    fn sample_minute(ts_bucket: chrono::DateTime<Utc>) -> MinuteHistory {
        MinuteHistory {
            ts_bucket,
            market: MarketKind::Futures,
            open_price: None,
            high_price: None,
            low_price: None,
            close_price: None,
            last_price: None,
            buy_qty: 0.0,
            sell_qty: 0.0,
            total_qty: 0.0,
            total_notional: 0.0,
            delta: 0.0,
            relative_delta: 0.0,
            force_liq: BTreeMap::new(),
            ofi: 0.0,
            spread_twa: None,
            topk_depth_twa: None,
            obi_twa: None,
            obi_l1_twa: None,
            obi_k_twa: None,
            obi_k_dw_twa: None,
            obi_k_dw_close: None,
            obi_k_dw_change: None,
            obi_k_dw_adj_twa: None,
            bbo_updates: 0,
            microprice_twa: None,
            microprice_classic_twa: None,
            microprice_kappa_twa: None,
            microprice_adj_twa: None,
            cvd: 0.0,
            vpin: 0.0,
            avwap_minute: None,
            whale_trade_count: 0,
            whale_buy_count: 0,
            whale_sell_count: 0,
            whale_notional_total: 0.0,
            whale_notional_buy: 0.0,
            whale_notional_sell: 0.0,
            whale_qty_eth_total: 0.0,
            whale_qty_eth_buy: 0.0,
            whale_qty_eth_sell: 0.0,
            profile: BTreeMap::new(),
        }
    }

    #[test]
    fn buying_exhaustion_all_history_keeps_historical_confirmed_event() {
        let base = Utc.with_ymd_and_hms(2026, 3, 9, 8, 0, 0).unwrap();
        let fut = (0..15)
            .map(|i| sample_minute(base + Duration::minutes(i as i64)))
            .collect::<Vec<_>>();

        let mut highs = vec![100.0; 15];
        let mut lows = vec![99.0; 15];
        let mut closes = vec![99.5; 15];
        let mut deltas = vec![0.0; 15];
        let mut rdeltas = vec![0.0; 15];
        let spot_cvd = vec![0.0; 15];
        let spot_whale_notional = vec![0.0; 15];

        highs[3] = 101.0;
        highs[8] = 104.0;
        lows[8] = 102.0;
        closes[8] = 102.3;
        closes[9] = 102.1;
        closes[10] = 101.9;
        deltas[3] = 120.0;
        deltas[8] = 80.0;
        rdeltas[3] = 0.20;
        rdeltas[8] = 0.10;

        let events = detect_buying_events(
            &fut,
            &highs,
            &lows,
            &closes,
            &deltas,
            &rdeltas,
            &spot_cvd,
            &spot_whale_notional,
            &[3, 8],
            14,
        );

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "buying_exhaustion");
        assert_eq!(events[0].pivot_price, highs[8]);
        assert_eq!(
            events[0].confirm_ts,
            fut[10].ts_bucket + Duration::minutes(1)
        );
    }

    #[test]
    fn selling_exhaustion_all_history_keeps_historical_confirmed_event() {
        let base = Utc.with_ymd_and_hms(2026, 3, 9, 10, 0, 0).unwrap();
        let fut = (0..15)
            .map(|i| sample_minute(base + Duration::minutes(i as i64)))
            .collect::<Vec<_>>();

        let mut highs = vec![101.0; 15];
        let mut lows = vec![99.0; 15];
        let mut closes = vec![100.5; 15];
        let mut deltas = vec![0.0; 15];
        let mut rdeltas = vec![0.0; 15];
        let spot_cvd = vec![0.0; 15];
        let spot_whale_notional = vec![0.0; 15];

        lows[3] = 99.0;
        highs[8] = 97.0;
        lows[8] = 96.0;
        closes[8] = 96.8;
        closes[9] = 96.9;
        closes[10] = 97.2;
        deltas[3] = -120.0;
        deltas[8] = -80.0;
        rdeltas[3] = -0.20;
        rdeltas[8] = -0.10;

        let events = detect_selling_events(
            &fut,
            &highs,
            &lows,
            &closes,
            &deltas,
            &rdeltas,
            &spot_cvd,
            &spot_whale_notional,
            &[3, 8],
            14,
        );

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "selling_exhaustion");
        assert_eq!(events[0].pivot_price, lows[8]);
        assert_eq!(
            events[0].confirm_ts,
            fut[10].ts_bucket + Duration::minutes(1)
        );
    }

    #[test]
    fn exhaustion_event_json_exposes_pivot_price() {
        let start_ts = Utc.with_ymd_and_hms(2026, 3, 10, 10, 51, 0).unwrap();
        let confirm_ts = Utc.with_ymd_and_hms(2026, 3, 10, 11, 9, 0).unwrap();
        let event = ExhaustionEventData {
            direction: -1,
            event_type: "buying_exhaustion".to_string(),
            start_ts,
            end_ts: confirm_ts,
            confirm_ts,
            pivot_price: 2069.37,
            pivot_ts_1: start_ts,
            pivot_ts_2: Utc.with_ymd_and_hms(2026, 3, 10, 11, 7, 0).unwrap(),
            pivot_confirm_ts_1: Utc.with_ymd_and_hms(2026, 3, 10, 10, 54, 0).unwrap(),
            pivot_confirm_ts_2: Utc.with_ymd_and_hms(2026, 3, 10, 11, 10, 0).unwrap(),
            price_push_ticks: 337.0,
            delta_change: -1845.619,
            rdelta_change: -0.2268,
            reject_ratio: 0.7883,
            confirm_speed: 1.0,
            spot_cvd_push_post_pivot: -138.0695,
            spot_whale_push_post_pivot: 22258.808455,
            spot_continuation_risk: false,
            spot_exhaustion_confirm: true,
            score: 0.918381277425552,
            payload: json!({
                "event_start_ts": start_ts.to_rfc3339(),
                "event_end_ts": confirm_ts.to_rfc3339(),
                "event_available_ts": confirm_ts.to_rfc3339(),
                "pivot_price": 2069.37
            }),
        };

        let (_, payload) = exhaustion_event_json("TESTUSDT", "buying_exhaustion", &event);
        assert_eq!(
            payload.get("pivot_price").and_then(|v| v.as_f64()),
            Some(2069.37)
        );
        assert_eq!(
            payload.get("delta_drop").and_then(|v| v.as_f64()),
            Some(1845.619)
        );
        assert_eq!(
            payload.get("rdelta_drop").and_then(|v| v.as_f64()),
            Some(0.2268)
        );
        assert!(payload.get("delta_change").is_none());
        assert!(payload.get("rdelta_change").is_none());
    }

    #[test]
    fn selling_exhaustion_event_json_uses_lift_field_names() {
        let start_ts = Utc.with_ymd_and_hms(2026, 3, 10, 3, 38, 0).unwrap();
        let confirm_ts = Utc.with_ymd_and_hms(2026, 3, 10, 3, 49, 0).unwrap();
        let event = ExhaustionEventData {
            direction: 1,
            event_type: "selling_exhaustion".to_string(),
            start_ts,
            end_ts: confirm_ts,
            confirm_ts,
            pivot_price: 2014.52,
            pivot_ts_1: start_ts,
            pivot_ts_2: Utc.with_ymd_and_hms(2026, 3, 10, 3, 47, 0).unwrap(),
            pivot_confirm_ts_1: Utc.with_ymd_and_hms(2026, 3, 10, 3, 41, 0).unwrap(),
            pivot_confirm_ts_2: Utc.with_ymd_and_hms(2026, 3, 10, 3, 50, 0).unwrap(),
            price_push_ticks: 1471.0,
            delta_change: 248.556,
            rdelta_change: 0.2271223558,
            reject_ratio: 0.7790697674,
            confirm_speed: 1.0,
            spot_cvd_push_post_pivot: -44.1198,
            spot_whale_push_post_pivot: 283346.18833,
            spot_continuation_risk: true,
            spot_exhaustion_confirm: false,
            score: 0.7242524917,
            payload: json!({
                "event_start_ts": start_ts.to_rfc3339(),
                "event_end_ts": confirm_ts.to_rfc3339(),
                "event_available_ts": confirm_ts.to_rfc3339(),
                "pivot_price": 2014.52,
                "delta_lift": 248.556,
                "rdelta_lift": 0.2271223558
            }),
        };

        let (_, payload) = exhaustion_event_json("TESTUSDT", "selling_exhaustion", &event);
        assert_eq!(
            payload.get("delta_lift").and_then(|v| v.as_f64()),
            Some(248.556)
        );
        assert_eq!(
            payload.get("rdelta_lift").and_then(|v| v.as_f64()),
            Some(0.2271223558)
        );
        assert!(payload.get("delta_change").is_none());
        assert!(payload.get("rdelta_change").is_none());
    }
}
