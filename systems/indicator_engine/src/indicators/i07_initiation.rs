use crate::indicators::context::{
    clip01, IndicatorComputation, IndicatorContext, IndicatorSnapshotRow, InitiationEventRow,
};
use crate::indicators::indicator_trait::Indicator;
use crate::indicators::shared::event_ids::build_initiation_event_id;
use crate::indicators::shared::event_views::{
    build_event_window_view, build_recent_7d_payload, merge_payload_fields,
};
use crate::indicators::shared::market_structure::{
    stacked_imbalance_flags, value_area_key_levels_ticks,
};
use crate::runtime::state_store::tick_to_price;
use chrono::Duration;
use serde_json::{json, Map, Value};

const TICK_SIZE: f64 = 0.01;
const EPSILON_BREAK_TICKS: f64 = 2.0;
const ZDELTA_LOOKBACK: usize = 60;
const ZDELTA_MIN: f64 = 1.5;
const RDELTA_MIN: f64 = 0.20;
const MIN_FOLLOW_MINUTES: usize = 5;
const HOLD_BREAK_TICKS: f64 = 1.0;

#[derive(Debug, Clone)]
pub(crate) struct InitiationEventData {
    pub direction: i16,
    pub event_type: String,
    pub start_ts: chrono::DateTime<chrono::Utc>,
    pub end_ts: chrono::DateTime<chrono::Utc>,
    pub confirm_ts: chrono::DateTime<chrono::Utc>,
    pub pivot_price: f64,
    pub price_low: f64,
    pub price_high: f64,
    pub z_delta: f64,
    pub rdelta_mean: f64,
    pub break_mag_ticks: f64,
    pub min_follow_required_minutes: i32,
    pub follow_through_minutes: i32,
    pub follow_through_end_ts: chrono::DateTime<chrono::Utc>,
    pub follow_through_delta_sum: f64,
    pub follow_through_hold_ok: bool,
    pub follow_through_max_adverse_excursion_ticks: f64,
    pub spot_break_confirm: bool,
    pub spot_rdelta_1m_mean: f64,
    pub spot_cvd_change: f64,
    pub spot_whale_break_confirm: bool,
    pub score: f64,
    pub payload: Value,
}

pub(crate) fn detect_initiation_all_history(ctx: &IndicatorContext) -> Vec<InitiationEventData> {
    let n = ctx.history_futures.len().min(ctx.history_spot.len());
    if n < ZDELTA_LOOKBACK + MIN_FOLLOW_MINUTES + 3 {
        return Vec::new();
    }
    let fut = &ctx.history_futures[ctx.history_futures.len() - n..];
    let spot = &ctx.history_spot[ctx.history_spot.len() - n..];
    let high = fut
        .iter()
        .map(|h| h.high_price.or(h.last_price).unwrap_or(0.0))
        .collect::<Vec<_>>();
    let low = fut
        .iter()
        .map(|h| h.low_price.or(h.last_price).unwrap_or(0.0))
        .collect::<Vec<_>>();
    let close = fut
        .iter()
        .map(|h| h.close_price.or(h.last_price).unwrap_or(0.0))
        .collect::<Vec<_>>();
    let delta = fut.iter().map(|h| h.delta).collect::<Vec<_>>();
    let rdelta = fut.iter().map(|h| h.relative_delta).collect::<Vec<_>>();
    let spot_rdelta = spot.iter().map(|h| h.relative_delta).collect::<Vec<_>>();
    let spot_cvd = spot.iter().map(|h| h.cvd).collect::<Vec<_>>();

    let mut zdelta = vec![None; n];
    for i in (ZDELTA_LOOKBACK - 1)..n {
        let win = &delta[i + 1 - ZDELTA_LOOKBACK..=i];
        let mean = win.iter().sum::<f64>() / ZDELTA_LOOKBACK as f64;
        let var = win
            .iter()
            .map(|v| {
                let d = v - mean;
                d * d
            })
            .sum::<f64>()
            / ZDELTA_LOOKBACK as f64;
        let sd = var.sqrt() + 1e-12;
        zdelta[i] = Some((delta[i] - mean) / sd);
    }

    let mut vah = vec![0.0; n];
    let mut val = vec![0.0; n];
    let mut stacked_buy_flags = vec![false; n];
    let mut stacked_sell_flags = vec![false; n];
    let mut vah_ffill: Option<f64> = None;
    let mut val_ffill: Option<f64> = None;
    for i in 0..n {
        if let Some((val_tick, vah_tick, _)) = value_area_key_levels_ticks(&fut[i].profile) {
            val_ffill = Some(tick_to_price(val_tick));
            vah_ffill = Some(tick_to_price(vah_tick));
        }
        let (stacked_buy, stacked_sell) = stacked_imbalance_flags(&fut[i].profile);
        stacked_buy_flags[i] = stacked_buy;
        stacked_sell_flags[i] = stacked_sell;
        vah[i] = vah_ffill.unwrap_or(high[i]);
        val[i] = val_ffill.unwrap_or(low[i]);
    }

    let eps_break = EPSILON_BREAK_TICKS * TICK_SIZE;
    let eps_hold = HOLD_BREAK_TICKS * TICK_SIZE;

    let mut out = Vec::new();
    for i in 1..n {
        if i + MIN_FOLLOW_MINUTES >= n {
            break;
        }
        let range = (high[i] - low[i]).max(TICK_SIZE);
        let clv_bull = (close[i] - low[i]) / (range + 1e-12);
        let clv_bear = (high[i] - close[i]) / (range + 1e-12);
        let z = zdelta[i].unwrap_or(0.0);

        let cand_bull = close[i] > vah[i] + eps_break
            && close[i - 1] <= vah[i - 1] + eps_break
            && z >= ZDELTA_MIN
            && rdelta[i] >= RDELTA_MIN
            && stacked_buy_flags[i]
            && clv_bull >= 0.70;
        let cand_bear = close[i] < val[i] - eps_break
            && close[i - 1] >= val[i - 1] - eps_break
            && z <= -ZDELTA_MIN
            && rdelta[i] <= -RDELTA_MIN
            && stacked_sell_flags[i]
            && clv_bear >= 0.70;
        let dir = match (cand_bull, cand_bear) {
            (true, false) => 1_i16,
            (false, true) => -1_i16,
            _ => 0_i16,
        };
        if dir == 0 {
            continue;
        }

        let follow_through_end_idx = i + MIN_FOLLOW_MINUTES;
        let follow_through_delta_sum = delta[i..=follow_through_end_idx].iter().sum::<f64>();
        let follow_through_hold_ok = if dir > 0 {
            let min_close = close[i + 1..=i + MIN_FOLLOW_MINUTES]
                .iter()
                .copied()
                .fold(f64::INFINITY, f64::min);
            min_close >= vah[i] - eps_hold && follow_through_delta_sum > 0.0
        } else {
            let max_close = close[i + 1..=i + MIN_FOLLOW_MINUTES]
                .iter()
                .copied()
                .fold(f64::NEG_INFINITY, f64::max);
            max_close <= val[i] + eps_hold && follow_through_delta_sum < 0.0
        };
        if !follow_through_hold_ok {
            continue;
        }

        let confirm_idx = follow_through_end_idx;
        let end_idx = confirm_idx;
        let event_price_low = low[i..=end_idx]
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min);
        let event_price_high = high[i..=end_idx]
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        let follow_through_max_adverse_excursion_ticks = if dir > 0 {
            let min_low = low[i..=confirm_idx]
                .iter()
                .copied()
                .fold(f64::INFINITY, f64::min);
            ((vah[i] - min_low).max(0.0)) / TICK_SIZE
        } else {
            let max_high = high[i..=confirm_idx]
                .iter()
                .copied()
                .fold(f64::NEG_INFINITY, f64::max);
            ((max_high - val[i]).max(0.0)) / TICK_SIZE
        };

        let n_g = (end_idx - i + 1) as f64;
        let rd_mean = rdelta[i..=end_idx].iter().sum::<f64>() / n_g;
        let break_mag = if dir > 0 {
            (close[i] - vah[i]) / TICK_SIZE
        } else {
            (val[i] - close[i]) / TICK_SIZE
        };
        let score = 0.30 * clip01(z.abs() / 3.0)
            + 0.30 * clip01(rd_mean.abs() / 0.5)
            + 0.20 * clip01(break_mag.abs() / 6.0)
            + 0.20 * clip01(n_g / (MIN_FOLLOW_MINUTES as f64 + 1.0));

        let spot_rd_mean = spot_rdelta[i..=end_idx].iter().sum::<f64>() / n_g;
        let spot_cvd_change = spot_cvd[end_idx] - spot_cvd[i];
        let spot_break_confirm =
            (dir as f64 * spot_rd_mean) >= 0.05 && (dir as f64 * spot_cvd_change) >= 0.0;
        let spot_whale_confirm = spot[i..=end_idx]
            .iter()
            .map(|h| {
                let px = h.close_price.or(h.last_price).unwrap_or_default();
                h.delta * px
            })
            .sum::<f64>()
            * dir as f64
            > 0.0;
        let strength_score_xmk = 0.80 * score
            + 0.15 * clip01(dir as f64 * spot_rd_mean)
            + 0.05 * if spot_whale_confirm { 1.0 } else { 0.0 };

        let start_ts = fut[i].ts_bucket;
        let confirm_ts = fut[confirm_idx].ts_bucket + Duration::minutes(1);
        let end_ts = fut[end_idx].ts_bucket + Duration::minutes(1);
        let event_type = if dir > 0 {
            "bullish_initiation"
        } else {
            "bearish_initiation"
        };

        out.push(InitiationEventData {
            direction: dir,
            event_type: event_type.to_string(),
            start_ts,
            end_ts,
            confirm_ts,
            pivot_price: if dir > 0 { vah[i] } else { val[i] },
            price_low: event_price_low,
            price_high: event_price_high,
            z_delta: z,
            rdelta_mean: rd_mean,
            break_mag_ticks: break_mag,
            min_follow_required_minutes: MIN_FOLLOW_MINUTES as i32,
            follow_through_minutes: (confirm_idx - i) as i32,
            follow_through_end_ts: fut[confirm_idx].ts_bucket + Duration::minutes(1),
            follow_through_delta_sum,
            follow_through_hold_ok,
            follow_through_max_adverse_excursion_ticks,
            spot_break_confirm,
            spot_rdelta_1m_mean: spot_rd_mean,
            spot_cvd_change,
            spot_whale_break_confirm: spot_whale_confirm,
            score: strength_score_xmk,
            payload: json!({
                "event_start_ts": start_ts.to_rfc3339(),
                "event_end_ts": end_ts.to_rfc3339(),
                "event_available_ts": confirm_ts.to_rfc3339(),
                "pivot_price": if dir > 0 { vah[i] } else { val[i] },
                "price_low": event_price_low,
                "price_high": event_price_high,
                "z_delta": z,
                "rdelta_mean": rd_mean,
                "break_mag_ticks": break_mag,
                "min_follow_required_minutes": MIN_FOLLOW_MINUTES as i32,
                "follow_through_minutes": (confirm_idx - i) as i32,
                "follow_through_delta_sum": follow_through_delta_sum,
                "follow_through_hold_ok": follow_through_hold_ok,
                "follow_through_max_adverse_excursion_ticks": follow_through_max_adverse_excursion_ticks,
                "spot_break_confirm": spot_break_confirm,
                "spot_rdelta_mean": spot_rd_mean,
                "spot_rdelta_1m_mean": spot_rd_mean,
                "spot_cvd_change": spot_cvd_change,
                "spot_whale_break_confirm": spot_whale_confirm,
                "strength_score_xmk": strength_score_xmk,
                "sig_pass": true
            }),
        });
    }

    out
}

pub(crate) fn detect_initiation_events(ctx: &IndicatorContext) -> Vec<InitiationEventData> {
    let current_available_ts = ctx.ts_bucket + Duration::minutes(1);
    detect_initiation_all_history(ctx)
        .into_iter()
        .filter(|event| event.confirm_ts == current_available_ts)
        .collect()
}

pub(crate) fn initiation_event_json(
    symbol: &str,
    indicator_code: &'static str,
    event: &InitiationEventData,
) -> (chrono::DateTime<chrono::Utc>, Value) {
    let event_id = build_initiation_event_id(
        symbol,
        indicator_code,
        &event.event_type,
        event.direction,
        event.confirm_ts,
        event.start_ts,
        event.end_ts,
        event.pivot_price,
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
    base.insert(
        "event_start_ts".to_string(),
        json!(event.start_ts.to_rfc3339()),
    );
    base.insert("event_end_ts".to_string(), json!(event.end_ts.to_rfc3339()));
    base.insert("pivot_price".to_string(), json!(event.pivot_price));
    base.insert("price_low".to_string(), json!(event.price_low));
    base.insert("price_high".to_string(), json!(event.price_high));
    base.insert("break_mag_ticks".to_string(), json!(event.break_mag_ticks));
    base.insert("z_delta".to_string(), json!(event.z_delta));
    base.insert("rdelta_mean".to_string(), json!(event.rdelta_mean));
    base.insert(
        "min_follow_required_minutes".to_string(),
        json!(event.min_follow_required_minutes),
    );
    base.insert(
        "follow_through_minutes".to_string(),
        json!(event.follow_through_minutes),
    );
    base.insert(
        "follow_through_delta_sum".to_string(),
        json!(event.follow_through_delta_sum),
    );
    base.insert(
        "follow_through_hold_ok".to_string(),
        json!(event.follow_through_hold_ok),
    );
    base.insert(
        "follow_through_max_adverse_excursion_ticks".to_string(),
        json!(event.follow_through_max_adverse_excursion_ticks),
    );
    base.insert(
        "spot_break_confirm".to_string(),
        json!(event.spot_break_confirm),
    );
    base.insert(
        "spot_rdelta_mean".to_string(),
        json!(event.spot_rdelta_1m_mean),
    );
    base.insert(
        "spot_rdelta_1m_mean".to_string(),
        json!(event.spot_rdelta_1m_mean),
    );
    base.insert("spot_cvd_change".to_string(), json!(event.spot_cvd_change));
    base.insert(
        "spot_whale_break_confirm".to_string(),
        json!(event.spot_whale_break_confirm),
    );
    (event.confirm_ts, merge_payload_fields(base, &event.payload))
}

fn append_initiation_rows(
    out: &mut IndicatorComputation,
    symbol: &str,
    indicator_code: &'static str,
    events: &[InitiationEventData],
) {
    for event in events {
        let event_id = build_initiation_event_id(
            symbol,
            indicator_code,
            &event.event_type,
            event.direction,
            event.confirm_ts,
            event.start_ts,
            event.end_ts,
            event.pivot_price,
        );
        let payload_json = initiation_event_json(symbol, indicator_code, event).1;
        out.initiation_rows.push(InitiationEventRow {
            event_id,
            event_type: event.event_type.clone(),
            direction: event.direction,
            ts_event_start: event.start_ts,
            ts_event_end: event.end_ts,
            confirm_ts: event.confirm_ts,
            event_available_ts: event.confirm_ts,
            pivot_price: Some(event.pivot_price),
            break_mag_ticks: Some(event.break_mag_ticks),
            z_delta: Some(event.z_delta),
            rdelta_mean: Some(event.rdelta_mean),
            min_follow_required_minutes: Some(event.min_follow_required_minutes),
            follow_through_minutes: Some(event.follow_through_minutes),
            follow_through_end_ts: Some(event.follow_through_end_ts),
            follow_through_delta_sum: Some(event.follow_through_delta_sum),
            follow_through_hold_ok: Some(event.follow_through_hold_ok),
            follow_through_max_adverse_excursion_ticks: Some(
                event.follow_through_max_adverse_excursion_ticks,
            ),
            spot_break_confirm: Some(event.spot_break_confirm),
            spot_rdelta_1m_mean: Some(event.spot_rdelta_1m_mean),
            spot_cvd_change: Some(event.spot_cvd_change),
            spot_whale_break_confirm: Some(event.spot_whale_break_confirm),
            score: Some(event.score),
            confidence: Some(event.score),
            window_code: "1m",
            payload_json,
        });
    }
}

pub struct I07Initiation;

impl Indicator for I07Initiation {
    fn code(&self) -> &'static str {
        "initiation"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation {
        let all_events = detect_initiation_all_history(ctx);
        let window_view = build_event_window_view(
            ctx.ts_bucket,
            all_events
                .iter()
                .map(|event| initiation_event_json(&ctx.symbol, self.code(), event))
                .collect(),
        );
        let lookback_covered_minutes = ctx.history_futures.len().min(ctx.history_spot.len()) as i64;
        let mut out = IndicatorComputation {
            snapshot: Some(IndicatorSnapshotRow {
                indicator_code: self.code(),
                window_code: "1m",
                payload_json: json!({
                    "event_count": window_view.current_events.len(),
                    "events": window_view.current_events,
                    "latest": window_view.latest_current,
                    "recent_7d": build_recent_7d_payload(
                        window_view.recent_events,
                        lookback_covered_minutes,
                        "in_memory_minute_history"
                    ),
                    "latest_7d": window_view.latest_recent
                }),
            }),
            ..Default::default()
        };

        append_initiation_rows(&mut out, &ctx.symbol, self.code(), &all_events);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::{initiation_event_json, InitiationEventData};
    use crate::indicators::shared::event_ids::build_initiation_event_id;
    use chrono::{TimeZone, Utc};
    use serde_json::json;

    #[test]
    fn initiation_event_json_exposes_follow_through_fields() {
        let start_ts = Utc.with_ymd_and_hms(2026, 3, 9, 1, 10, 0).unwrap();
        let confirm_ts = Utc.with_ymd_and_hms(2026, 3, 9, 1, 16, 0).unwrap();
        let event = InitiationEventData {
            direction: -1,
            event_type: "bearish_initiation".to_string(),
            start_ts,
            end_ts: confirm_ts,
            confirm_ts,
            pivot_price: 1950.4,
            price_low: 1948.8,
            price_high: 1953.1,
            z_delta: -2.18,
            rdelta_mean: -0.29,
            break_mag_ticks: 14.0,
            min_follow_required_minutes: 5,
            follow_through_minutes: 5,
            follow_through_end_ts: confirm_ts,
            follow_through_delta_sum: -312.8,
            follow_through_hold_ok: true,
            follow_through_max_adverse_excursion_ticks: 1.5,
            spot_break_confirm: true,
            spot_rdelta_1m_mean: -0.11,
            spot_cvd_change: -201.8,
            spot_whale_break_confirm: false,
            score: 0.81,
            payload: json!({
                "event_start_ts": start_ts.to_rfc3339(),
                "event_end_ts": confirm_ts.to_rfc3339(),
                "event_available_ts": confirm_ts.to_rfc3339(),
                "price_low": 1948.8,
                "price_high": 1953.1,
                "min_follow_required_minutes": 5,
                "follow_through_minutes": 5,
                "follow_through_delta_sum": -312.8,
                "follow_through_hold_ok": true,
                "follow_through_max_adverse_excursion_ticks": 1.5,
                "score": 0.81
            }),
        };

        let (_, payload) = initiation_event_json("ETHUSDT", "initiation", &event);
        let expected_event_id = build_initiation_event_id(
            "ETHUSDT",
            "initiation",
            "bearish_initiation",
            -1,
            confirm_ts,
            start_ts,
            confirm_ts,
            1950.4,
        );
        assert_eq!(
            payload.get("event_id").and_then(|v| v.as_str()),
            Some(expected_event_id.as_str())
        );
        assert_eq!(
            payload
                .get("follow_through_delta_sum")
                .and_then(|v| v.as_f64()),
            Some(-312.8)
        );
        assert_eq!(
            payload
                .get("min_follow_required_minutes")
                .and_then(|v| v.as_i64()),
            Some(5)
        );
        assert_eq!(
            payload.get("price_low").and_then(|v| v.as_f64()),
            Some(1948.8)
        );
        assert_eq!(
            payload.get("price_high").and_then(|v| v.as_f64()),
            Some(1953.1)
        );
        assert_eq!(
            payload
                .get("follow_through_hold_ok")
                .and_then(|v| v.as_bool()),
            Some(true)
        );
        assert_eq!(
            payload
                .get("follow_through_max_adverse_excursion_ticks")
                .and_then(|v| v.as_f64()),
            Some(1.5)
        );
    }
}
