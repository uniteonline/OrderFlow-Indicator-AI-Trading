use crate::indicators::context::{
    clip01, AbsorptionEventRow, IndicatorComputation, IndicatorContext, IndicatorSnapshotRow,
};
use crate::indicators::indicator_trait::Indicator;
use crate::indicators::shared::event_ids::build_indicator_event_id;
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
const ETA_REJECT: f64 = 0.70;
const MIN_RANGE_TICKS: f64 = 4.0;
const RDELTA_ABS_MIN: f64 = 0.15;
const KEY_DIST_TICKS: f64 = 6.0;
const MERGE_GAP_MINUTES: i64 = 1;
const CONFIRM_BARS: usize = 3;
const MAX_EVENT_MINUTES: usize = 30;
const THETA_RD_SPOT: f64 = 0.15;
const THETA_WHALE_SPOT: f64 = 100_000.0;

#[derive(Debug, Clone)]
pub(crate) struct AbsorptionEventData {
    pub direction: i16,
    pub event_type: String,
    pub trigger_side: String,
    pub start_ts: chrono::DateTime<chrono::Utc>,
    pub end_ts: chrono::DateTime<chrono::Utc>,
    pub confirm_ts: chrono::DateTime<chrono::Utc>,
    pub pivot_price: f64,
    pub price_low: f64,
    pub price_high: f64,
    pub delta_sum: f64,
    pub rdelta_mean: f64,
    pub reject_ratio: f64,
    pub key_distance_ticks: f64,
    pub stacked_buy_imbalance: bool,
    pub stacked_sell_imbalance: bool,
    pub spot_rdelta_1m_mean: f64,
    pub spot_cvd_1m_change: f64,
    pub spot_flow_confirm_score: f64,
    pub spot_whale_confirm_score: f64,
    pub spot_confirm: bool,
    pub score_base: f64,
    pub score: f64,
    pub payload: Value,
}

pub(crate) fn detect_absorption_all_history(ctx: &IndicatorContext) -> Vec<AbsorptionEventData> {
    let n = ctx.history_futures.len().min(ctx.history_spot.len());
    if n < CONFIRM_BARS + 5 {
        return Vec::new();
    }

    let fut = &ctx.history_futures[ctx.history_futures.len() - n..];
    let spot = &ctx.history_spot[ctx.history_spot.len() - n..];

    let open = fut
        .iter()
        .map(|h| {
            h.open_price
                .or(h.close_price)
                .or(h.last_price)
                .unwrap_or(0.0)
        })
        .collect::<Vec<_>>();
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

    // 文档要求 key levels 逐分钟前向填充，这里按分钟 profile/avwap 生成并 ffill。
    let mut avwap = vec![0.0; n];
    let mut val_levels = vec![0.0; n];
    let mut vah_levels = vec![0.0; n];
    let mut poc_levels = vec![0.0; n];
    let mut stacked_buy = vec![false; n];
    let mut stacked_sell = vec![false; n];

    let mut avwap_ffill: Option<f64> = None;
    let mut val_ffill: Option<f64> = None;
    let mut vah_ffill: Option<f64> = None;
    let mut poc_ffill: Option<f64> = None;

    for (i, bar) in fut.iter().enumerate() {
        if let Some(v) = bar.avwap_minute {
            avwap_ffill = Some(v);
        }
        if let Some((val_tick, vah_tick, poc_tick)) = value_area_key_levels_ticks(&bar.profile) {
            val_ffill = Some(tick_to_price(val_tick));
            vah_ffill = Some(tick_to_price(vah_tick));
            poc_ffill = Some(tick_to_price(poc_tick));
        }
        let (buy_flag, sell_flag) = stacked_imbalance_flags(&bar.profile);

        avwap[i] = avwap_ffill.unwrap_or(close[i]);
        val_levels[i] = val_ffill.unwrap_or(low[i]);
        vah_levels[i] = vah_ffill.unwrap_or(high[i]);
        poc_levels[i] = poc_ffill.unwrap_or(close[i]);
        stacked_buy[i] = buy_flag;
        stacked_sell[i] = sell_flag;
    }

    let mut prev_session_high = vec![None; n];
    let mut prev_session_low = vec![None; n];
    for i in 0..n {
        if i == 0 {
            continue;
        }
        let start = i.saturating_sub(1440);
        let hs = &high[start..i];
        let ls = &low[start..i];
        if !hs.is_empty() {
            prev_session_high[i] = hs.iter().copied().reduce(f64::max);
            prev_session_low[i] = ls.iter().copied().reduce(f64::min);
        }
    }

    let mut sign = vec![0_i16; n];
    let mut reject_bull = vec![0.0; n];
    let mut reject_bear = vec![0.0; n];
    let mut d_low_key = vec![f64::INFINITY; n];
    let mut d_high_key = vec![f64::INFINITY; n];

    for i in 0..n {
        let range = (high[i] - low[i]).max(0.0);
        let rbull = (close[i] - low[i]) / (range + 1e-12);
        let rbear = (high[i] - close[i]) / (range + 1e-12);
        reject_bull[i] = rbull;
        reject_bear[i] = rbear;

        let vah = vah_levels[i];
        let val = val_levels[i];
        let poc = poc_levels[i];
        let keys = [
            vah,
            val,
            poc,
            avwap[i],
            prev_session_high[i].unwrap_or(vah),
            prev_session_low[i].unwrap_or(val),
        ];

        d_low_key[i] = keys
            .iter()
            .map(|k| (low[i] - *k).abs() / TICK_SIZE)
            .fold(f64::INFINITY, f64::min);
        d_high_key[i] = keys
            .iter()
            .map(|k| (high[i] - *k).abs() / TICK_SIZE)
            .fold(f64::INFINITY, f64::min);

        let cand_bull = stacked_sell[i]
            && range >= MIN_RANGE_TICKS * TICK_SIZE
            && rdelta[i] <= -RDELTA_ABS_MIN
            && close[i] > open[i]
            && rbull >= ETA_REJECT
            && d_low_key[i] <= KEY_DIST_TICKS;
        let cand_bear = stacked_buy[i]
            && range >= MIN_RANGE_TICKS * TICK_SIZE
            && rdelta[i] >= RDELTA_ABS_MIN
            && close[i] < open[i]
            && rbear >= ETA_REJECT
            && d_high_key[i] <= KEY_DIST_TICKS;

        sign[i] = match (cand_bull, cand_bear) {
            (true, false) => 1,
            (false, true) => -1,
            _ => 0,
        };
    }

    let mut groups: Vec<(i16, usize, usize)> = Vec::new();
    for i in 0..n {
        if sign[i] == 0 {
            continue;
        }
        if let Some(last) = groups.last_mut() {
            let gap = (fut[i].ts_bucket - fut[last.2].ts_bucket).num_minutes();
            if last.0 == sign[i]
                && gap <= MERGE_GAP_MINUTES
                && (i - last.1 + 1) <= MAX_EVENT_MINUTES
            {
                last.2 = i;
                continue;
            }
        }
        groups.push((sign[i], i, i));
    }

    let mut out = Vec::new();
    for (dir, s, e) in groups {
        if e + 1 + CONFIRM_BARS > n {
            continue;
        }
        let l_g = low[s..=e].iter().copied().fold(f64::INFINITY, f64::min);
        let h_g = high[s..=e]
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        let r_g = (h_g - l_g).max(TICK_SIZE);

        let lows_future_min = low[e + 1..=e + CONFIRM_BARS]
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min);
        let highs_future_max = high[e + 1..=e + CONFIRM_BARS]
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);

        let mut confirm_idx = None;
        for j in 1..=CONFIRM_BARS {
            let idx = e + j;
            let ok = if dir > 0 {
                close[idx] >= l_g + 0.5 * r_g && lows_future_min >= l_g - TICK_SIZE
            } else {
                close[idx] <= h_g - 0.5 * r_g && highs_future_max <= h_g + TICK_SIZE
            };
            if ok {
                confirm_idx = Some(idx);
                break;
            }
        }
        let Some(cidx) = confirm_idx else {
            continue;
        };
        let n_g = (e - s + 1) as f64;
        let rd_mean = rdelta[s..=e].iter().sum::<f64>() / n_g;
        let reject_g = if dir > 0 {
            reject_bull[s..=e].iter().sum::<f64>() / n_g
        } else {
            reject_bear[s..=e].iter().sum::<f64>() / n_g
        };
        let d_key = if dir > 0 {
            d_low_key[s..=e]
                .iter()
                .copied()
                .fold(f64::INFINITY, f64::min)
        } else {
            d_high_key[s..=e]
                .iter()
                .copied()
                .fold(f64::INFINITY, f64::min)
        };

        let score = 0.35 * clip01(rd_mean.abs() / 0.5)
            + 0.35 * clip01((reject_g - ETA_REJECT) / (1.0 - ETA_REJECT))
            + 0.20 * clip01(1.0 - d_key / KEY_DIST_TICKS)
            + 0.10 * clip01(n_g / 5.0);

        let spot_rd_mean = spot_rdelta[s..=e].iter().sum::<f64>() / n_g;
        let spot_cvd_push = spot_cvd[cidx] - spot_cvd[e];
        let spot_whale_push = (s..=cidx)
            .map(|i| {
                let px = spot[i]
                    .close_price
                    .or(spot[i].last_price)
                    .unwrap_or_default();
                spot[i].delta * px
            })
            .sum::<f64>();

        let spot_flow_confirm = clip01((dir as f64 * spot_rd_mean) / (THETA_RD_SPOT + 1e-12));
        let spot_whale_confirm =
            clip01((dir as f64 * spot_whale_push) / (THETA_WHALE_SPOT + 1e-12));
        let score_xmk = 0.85 * score + 0.10 * spot_flow_confirm + 0.05 * spot_whale_confirm;

        let event_type = if dir > 0 {
            "bullish_absorption"
        } else {
            "bearish_absorption"
        };
        let pivot_price = if dir > 0 { l_g } else { h_g };
        let delta_sum = delta[s..=e].iter().sum::<f64>();
        let stacked_buy_imbalance = stacked_buy[s..=e].iter().any(|flag| *flag);
        let stacked_sell_imbalance = stacked_sell[s..=e].iter().any(|flag| *flag);
        let spot_confirm = spot_flow_confirm > 0.0 || spot_whale_confirm > 0.0;
        let start_ts = fut[s].ts_bucket;
        let end_ts = fut[e].ts_bucket + Duration::minutes(1);
        let confirm_ts = fut[cidx].ts_bucket + Duration::minutes(1);
        let trigger_side = if dir > 0 { "sell" } else { "buy" };

        out.push(AbsorptionEventData {
            direction: dir,
            event_type: event_type.to_string(),
            trigger_side: trigger_side.to_string(),
            start_ts,
            end_ts,
            confirm_ts,
            pivot_price,
            price_low: l_g,
            price_high: h_g,
            delta_sum,
            rdelta_mean: rd_mean,
            reject_ratio: reject_g,
            key_distance_ticks: d_key,
            stacked_buy_imbalance,
            stacked_sell_imbalance,
            spot_rdelta_1m_mean: spot_rd_mean,
            spot_cvd_1m_change: spot_cvd_push,
            spot_flow_confirm_score: spot_flow_confirm,
            spot_whale_confirm_score: spot_whale_confirm,
            spot_confirm,
            score_base: score,
            score: score_xmk,
            payload: json!({
                "event_start_ts": start_ts.to_rfc3339(),
                "event_end_ts": end_ts.to_rfc3339(),
                "event_available_ts": confirm_ts.to_rfc3339(),
                "pivot_price": pivot_price,
                "price_low": l_g,
                "price_high": h_g,
                "trigger_side": trigger_side,
                "delta_sum": delta_sum,
                "rdelta_mean": rd_mean,
                "reject_ratio": reject_g,
                "key_distance_ticks": d_key,
                "stacked_buy_imbalance": stacked_buy_imbalance,
                "stacked_sell_imbalance": stacked_sell_imbalance,
                "spot_rdelta_1m_mean": spot_rd_mean,
                "spot_cvd_1m_change": spot_cvd_push,
                "spot_flow_confirm_score": spot_flow_confirm,
                "spot_whale_confirm_score": spot_whale_confirm,
                "spot_confirm": spot_confirm,
                "score_base": score,
                "strength_score_xmk": score_xmk,
                "sig_pass": true
            }),
        });
    }

    out
}

pub(crate) fn detect_absorption_events(ctx: &IndicatorContext) -> Vec<AbsorptionEventData> {
    let current_available_ts = ctx.ts_bucket + Duration::minutes(1);
    detect_absorption_all_history(ctx)
        .into_iter()
        .filter(|event| event.confirm_ts == current_available_ts)
        .collect()
}

pub(crate) fn absorption_event_json(
    symbol: &str,
    indicator_code: &'static str,
    event: &AbsorptionEventData,
) -> (chrono::DateTime<chrono::Utc>, Value) {
    let event_id = build_indicator_event_id(
        symbol,
        indicator_code,
        &event.event_type,
        event.start_ts,
        Some(event.end_ts),
        event.direction,
        None,
        None,
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
    base.insert("trigger_side".to_string(), json!(event.trigger_side));
    (event.confirm_ts, merge_payload_fields(base, &event.payload))
}

fn append_absorption_rows(
    out: &mut IndicatorComputation,
    symbol: &str,
    indicator_code: &'static str,
    events: &[AbsorptionEventData],
) {
    for event in events {
        let event_id = build_indicator_event_id(
            symbol,
            indicator_code,
            &event.event_type,
            event.start_ts,
            Some(event.end_ts),
            event.direction,
            None,
            None,
        );
        let payload_json = absorption_event_json(symbol, indicator_code, event).1;
        out.absorption_rows.push(AbsorptionEventRow {
            event_id,
            event_type: event.event_type.clone(),
            direction: event.direction,
            ts_event_start: event.start_ts,
            ts_event_end: event.end_ts,
            confirm_ts: event.confirm_ts,
            event_available_ts: event.confirm_ts,
            trigger_side: Some(event.trigger_side.clone()),
            pivot_price: Some(event.pivot_price),
            price_low: Some(event.price_low),
            price_high: Some(event.price_high),
            delta_sum: Some(event.delta_sum),
            rdelta_mean: Some(event.rdelta_mean),
            reject_ratio: Some(event.reject_ratio),
            key_distance_ticks: Some(event.key_distance_ticks),
            stacked_buy_imbalance: Some(event.stacked_buy_imbalance),
            stacked_sell_imbalance: Some(event.stacked_sell_imbalance),
            spot_rdelta_1m_mean: Some(event.spot_rdelta_1m_mean),
            spot_cvd_1m_change: Some(event.spot_cvd_1m_change),
            spot_flow_confirm_score: Some(event.spot_flow_confirm_score),
            spot_whale_confirm_score: Some(event.spot_whale_confirm_score),
            spot_confirm: Some(event.spot_confirm),
            score_base: Some(event.score_base),
            score: Some(event.score),
            confidence: Some(event.score),
            window_code: "1m",
            payload_json,
        });
    }
}

pub struct I06Absorption;

impl Indicator for I06Absorption {
    fn code(&self) -> &'static str {
        "absorption"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation {
        let all_events = detect_absorption_all_history(ctx);
        let window_view = build_event_window_view(
            ctx.ts_bucket,
            all_events
                .iter()
                .map(|event| absorption_event_json(&ctx.symbol, self.code(), event))
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

        append_absorption_rows(&mut out, &ctx.symbol, self.code(), &all_events);

        out
    }
}

#[cfg(test)]
mod tests {
    use super::{absorption_event_json, AbsorptionEventData};
    use chrono::{TimeZone, Utc};
    use serde_json::json;

    #[test]
    fn absorption_event_json_exposes_trigger_side() {
        let start_ts = Utc.with_ymd_and_hms(2026, 3, 9, 1, 10, 0).unwrap();
        let end_ts = Utc.with_ymd_and_hms(2026, 3, 9, 1, 12, 0).unwrap();
        let confirm_ts = Utc.with_ymd_and_hms(2026, 3, 9, 1, 13, 0).unwrap();
        let event = AbsorptionEventData {
            direction: 1,
            event_type: "bullish_absorption".to_string(),
            trigger_side: "sell".to_string(),
            start_ts,
            end_ts,
            confirm_ts,
            pivot_price: 1950.4,
            price_low: 1948.8,
            price_high: 1953.1,
            delta_sum: -312.8,
            rdelta_mean: -0.22,
            reject_ratio: 0.91,
            key_distance_ticks: 2.0,
            stacked_buy_imbalance: false,
            stacked_sell_imbalance: true,
            spot_rdelta_1m_mean: -0.11,
            spot_cvd_1m_change: -201.8,
            spot_flow_confirm_score: 0.3,
            spot_whale_confirm_score: 0.0,
            spot_confirm: true,
            score_base: 0.72,
            score: 0.81,
            payload: json!({
                "event_start_ts": start_ts.to_rfc3339(),
                "event_end_ts": end_ts.to_rfc3339(),
                "event_available_ts": confirm_ts.to_rfc3339(),
                "trigger_side": "sell",
                "score": 0.81
            }),
        };

        let (_, payload) = absorption_event_json("ETHUSDT", "absorption", &event);
        assert_eq!(
            payload.get("trigger_side").and_then(|v| v.as_str()),
            Some("sell")
        );
    }
}
