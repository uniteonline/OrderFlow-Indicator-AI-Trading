use crate::indicators::context::{
    clip01, IndicatorComputation, IndicatorContext, IndicatorLevelRow, IndicatorSnapshotRow,
    KlineHistoryBar,
};
use crate::indicators::i19_kline_history::build_interval_bar_records;
use crate::indicators::indicator_trait::Indicator;
use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

const TICK_SIZE: f64 = 0.01;
const MIN_CLOSE_LOCATION: f64 = 0.70;

pub struct I24Fvg;

#[derive(Debug, Clone)]
struct FvgZone {
    fvg_id: String,
    tf: &'static str,
    side: &'static str,
    left_ts: DateTime<Utc>,
    mid_ts: DateTime<Utc>,
    birth_ts: DateTime<Utc>,
    event_available_ts: DateTime<Utc>,
    lower: f64,
    upper: f64,
    mid: f64,
    width: f64,
    width_atr_ratio: f64,
    body_ratio_mid: f64,
    impulse_atr_ratio: f64,
    close_location_mid: f64,
    displacement_score: f64,
    fill_pct: f64,
    touch_count: i64,
    state: &'static str,
    age_bars: i64,
}

impl Indicator for I24Fvg {
    fn code(&self) -> &'static str {
        "fvg"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation {
        let current_minute_close = ctx.ts_bucket + Duration::minutes(1);
        let current_price = ctx
            .futures
            .last_price
            .or_else(|| {
                ctx.history_futures
                    .last()
                    .and_then(|row| row.close_price.or(row.last_price).or(row.open_price))
            })
            .unwrap_or(0.0);

        let mut by_window = Map::new();
        let mut level_rows = Vec::new();
        for tf_code in &ctx.fvg_windows {
            let Some(tf) = as_fvg_window(tf_code) else {
                continue;
            };

            let bars = closed_htf_bars(ctx, tf, current_minute_close);
            let min_required_bars = ctx.fvg_atr_lookback + 2;
            let returned_bars = bars.len();
            let coverage_ratio = clip01(returned_bars as f64 / min_required_bars.max(1) as f64);
            let is_ready = returned_bars >= min_required_bars;

            if !is_ready {
                by_window.insert(
                    tf.to_string(),
                    json!({
                        "requested_bars": min_required_bars,
                        "returned_bars": returned_bars,
                        "missing_bars": min_required_bars.saturating_sub(returned_bars),
                        "coverage_ratio": coverage_ratio,
                        "min_required_bars": min_required_bars,
                        "is_ready": false,
                        "fvgs": [],
                        "active_bull_fvgs": [],
                        "active_bear_fvgs": [],
                        "nearest_bull_fvg": null,
                        "nearest_bear_fvg": null,
                    }),
                );
                continue;
            }

            let zones = scan_fvgs(ctx, tf, &bars);
            for zone in &zones {
                level_rows.push(IndicatorLevelRow {
                    indicator_code: self.code(),
                    window_code: tf,
                    price_level: zone.mid,
                    level_rank: None,
                    metrics_json: zone_to_json(zone),
                });
            }

            let fvgs = zones.iter().map(zone_to_json).collect::<Vec<_>>();
            let active_bull = zones
                .iter()
                .filter(|zone| zone.side == "bull" && is_active_state(zone.state))
                .map(zone_to_json)
                .collect::<Vec<_>>();
            let active_bear = zones
                .iter()
                .filter(|zone| zone.side == "bear" && is_active_state(zone.state))
                .map(zone_to_json)
                .collect::<Vec<_>>();
            let nearest_bull = nearest_zone(
                zones
                    .iter()
                    .filter(|zone| zone.side == "bull" && is_active_state(zone.state)),
                current_price,
            )
            .map(zone_to_json)
            .unwrap_or(Value::Null);
            let nearest_bear = nearest_zone(
                zones
                    .iter()
                    .filter(|zone| zone.side == "bear" && is_active_state(zone.state)),
                current_price,
            )
            .map(zone_to_json)
            .unwrap_or(Value::Null);

            by_window.insert(
                tf.to_string(),
                json!({
                    "requested_bars": min_required_bars,
                    "returned_bars": returned_bars,
                    "missing_bars": min_required_bars.saturating_sub(returned_bars),
                    "coverage_ratio": coverage_ratio,
                    "min_required_bars": min_required_bars,
                    "is_ready": true,
                    "fvgs": fvgs,
                    "active_bull_fvgs": active_bull,
                    "active_bear_fvgs": active_bear,
                    "nearest_bull_fvg": nearest_bull,
                    "nearest_bear_fvg": nearest_bear,
                }),
            );
        }

        IndicatorComputation {
            snapshot: Some(IndicatorSnapshotRow {
                indicator_code: self.code(),
                window_code: "1m",
                payload_json: json!({
                    "source_market": "futures",
                    "base_detection_uses_spot": false,
                    "by_window": by_window,
                }),
            }),
            level_rows,
            ..Default::default()
        }
    }
}

fn closed_htf_bars(
    ctx: &IndicatorContext,
    tf: &'static str,
    current_minute_close: DateTime<Utc>,
) -> Vec<KlineHistoryBar> {
    let tf_minutes = match tf {
        "15m" => 15,
        "4h" => 240,
        "1d" => 1440,
        "3d" => 4320,
        _ => return Vec::new(),
    };
    let mut merged = BTreeMap::new();
    for bar in fvg_db_bars(ctx, tf) {
        merged.insert(bar.open_time, bar.clone());
    }
    for bar in build_interval_bar_records(
        &ctx.history_futures,
        tf_minutes,
        usize::MAX,
        current_minute_close,
    ) {
        merged.insert(bar.open_time, bar);
    }
    merged
        .into_values()
        .filter(|bar| {
            bar.is_closed
                && bar.open.is_some()
                && bar.high.is_some()
                && bar.low.is_some()
                && bar.close.is_some()
        })
        .collect()
}

fn fvg_db_bars<'a>(ctx: &'a IndicatorContext, tf: &str) -> &'a [KlineHistoryBar] {
    match tf {
        "4h" => &ctx.kline_history_futures_4h_db,
        "1d" => &ctx.kline_history_futures_1d_db,
        _ => &[],
    }
}

fn scan_fvgs(ctx: &IndicatorContext, tf: &'static str, bars: &[KlineHistoryBar]) -> Vec<FvgZone> {
    if bars.len() < 3 {
        return Vec::new();
    }

    let trs = bars
        .iter()
        .enumerate()
        .map(|(idx, bar)| true_range(bar, idx.checked_sub(1).and_then(|prev| bars[prev].close)))
        .collect::<Vec<_>>();
    let epsilon_gap = ctx.fvg_epsilon_gap_ticks.max(0) as f64 * TICK_SIZE;
    let mut zones = Vec::new();

    for birth_idx in 2..bars.len() {
        let left = &bars[birth_idx - 2];
        let mid = &bars[birth_idx - 1];
        let birth = &bars[birth_idx];

        let (left_high, left_low, mid_open, mid_high, mid_low, mid_close, birth_high, birth_low) =
            match (
                left.high, left.low, mid.open, mid.high, mid.low, mid.close, birth.high, birth.low,
            ) {
                (
                    Some(left_high),
                    Some(left_low),
                    Some(mid_open),
                    Some(mid_high),
                    Some(mid_low),
                    Some(mid_close),
                    Some(birth_high),
                    Some(birth_low),
                ) => (
                    left_high, left_low, mid_open, mid_high, mid_low, mid_close, birth_high,
                    birth_low,
                ),
                _ => continue,
            };

        let atr_for_impulse = simple_atr(&trs, birth_idx - 2, ctx.fvg_atr_lookback);
        let atr_for_gap = simple_atr(&trs, birth_idx - 1, ctx.fvg_atr_lookback);
        let Some(atr_impulse) = atr_for_impulse else {
            continue;
        };
        let Some(atr_gap) = atr_for_gap else {
            continue;
        };

        let bar_range_mid = (mid_high - mid_low).abs();
        let body_ratio_mid = (mid_close - mid_open).abs() / (bar_range_mid + 1e-12);
        let impulse_atr_ratio = trs[birth_idx - 1] / (atr_impulse + 1e-12);

        if body_ratio_mid < ctx.fvg_min_body_ratio
            || impulse_atr_ratio < ctx.fvg_min_impulse_atr_ratio
        {
            continue;
        }

        if birth_low > left_high + epsilon_gap {
            let close_location_mid = (mid_close - mid_low) / (bar_range_mid + 1e-12);
            if close_location_mid < MIN_CLOSE_LOCATION {
                continue;
            }
            let lower = left_high;
            let upper = birth_low;
            if let Some(zone) = build_zone(
                ctx,
                tf,
                "bull",
                birth_idx,
                bars,
                lower,
                upper,
                body_ratio_mid,
                impulse_atr_ratio,
                close_location_mid,
                atr_gap,
                epsilon_gap,
            ) {
                zones.push(zone);
            }
        }

        if birth_high < left_low - epsilon_gap {
            let close_location_mid = (mid_high - mid_close) / (bar_range_mid + 1e-12);
            if close_location_mid < MIN_CLOSE_LOCATION {
                continue;
            }
            let lower = birth_high;
            let upper = left_low;
            if let Some(zone) = build_zone(
                ctx,
                tf,
                "bear",
                birth_idx,
                bars,
                lower,
                upper,
                body_ratio_mid,
                impulse_atr_ratio,
                close_location_mid,
                atr_gap,
                epsilon_gap,
            ) {
                zones.push(zone);
            }
        }
    }

    zones
}

#[allow(clippy::too_many_arguments)]
fn build_zone(
    ctx: &IndicatorContext,
    tf: &'static str,
    side: &'static str,
    birth_idx: usize,
    bars: &[KlineHistoryBar],
    lower: f64,
    upper: f64,
    body_ratio_mid: f64,
    impulse_atr_ratio: f64,
    close_location_mid: f64,
    atr_gap: f64,
    epsilon_gap: f64,
) -> Option<FvgZone> {
    let width = upper - lower;
    if width <= 0.0 {
        return None;
    }
    let width_atr_ratio = width / (atr_gap + 1e-12);
    if width_atr_ratio < ctx.fvg_min_gap_atr_ratio || width_atr_ratio > ctx.fvg_max_gap_atr_ratio {
        return None;
    }

    let birth = &bars[birth_idx];
    let left = &bars[birth_idx - 2];
    let mid_bar = &bars[birth_idx - 1];
    let displacement_score = clip01(
        0.4 * body_ratio_mid + 0.3 * (impulse_atr_ratio / 2.0).min(1.0) + 0.3 * close_location_mid,
    );

    let fill_pct = match side {
        "bull" => {
            let min_low = bars[birth_idx..]
                .iter()
                .filter_map(|bar| bar.low)
                .fold(f64::INFINITY, f64::min);
            clip01((upper - min_low) / (width + 1e-12))
        }
        "bear" => {
            let max_high = bars[birth_idx..]
                .iter()
                .filter_map(|bar| bar.high)
                .fold(f64::NEG_INFINITY, f64::max);
            clip01((max_high - lower) / (width + 1e-12))
        }
        _ => 0.0,
    };

    let touch_count = bars[birth_idx..]
        .iter()
        .filter(|bar| match side {
            "bull" => bar.low.map(|low| low <= upper).unwrap_or(false),
            "bear" => bar.high.map(|high| high >= lower).unwrap_or(false),
            _ => false,
        })
        .count() as i64;

    let invalid_run = bars[birth_idx..]
        .iter()
        .rev()
        .take_while(|bar| {
            let close = bar.close.unwrap_or_default();
            match side {
                "bull" => close < lower - epsilon_gap,
                "bear" => close > upper + epsilon_gap,
                _ => false,
            }
        })
        .count();

    let state = if invalid_run >= ctx.fvg_invalid_close_bars {
        "invalidated"
    } else if fill_pct >= 1.0 {
        "fully_filled"
    } else if fill_pct >= ctx.fvg_mitigated_fill_threshold {
        "mitigated"
    } else if fill_pct > 0.0 {
        "partially_filled"
    } else {
        "fresh"
    };

    Some(FvgZone {
        fvg_id: format!(
            "fvg|{}|{}|{}|{}|{}|{}",
            ctx.symbol,
            tf,
            side,
            birth.open_time.to_rfc3339(),
            price_key(lower),
            price_key(upper),
        ),
        tf,
        side,
        left_ts: left.open_time,
        mid_ts: mid_bar.open_time,
        birth_ts: birth.open_time,
        event_available_ts: birth.close_time,
        lower,
        upper,
        mid: (lower + upper) / 2.0,
        width,
        width_atr_ratio,
        body_ratio_mid,
        impulse_atr_ratio,
        close_location_mid,
        displacement_score,
        fill_pct,
        touch_count,
        state,
        age_bars: (bars.len() - 1 - birth_idx) as i64,
    })
}

fn simple_atr(trs: &[f64], end_idx: usize, lookback: usize) -> Option<f64> {
    if lookback == 0 || end_idx + 1 < lookback {
        return None;
    }
    let start_idx = end_idx + 1 - lookback;
    Some(trs[start_idx..=end_idx].iter().sum::<f64>() / lookback as f64)
}

fn true_range(bar: &KlineHistoryBar, prev_close: Option<f64>) -> f64 {
    let high = bar.high.unwrap_or_default();
    let low = bar.low.unwrap_or_default();
    let hl = (high - low).abs();
    match prev_close {
        Some(prev_close) => hl
            .max((high - prev_close).abs())
            .max((low - prev_close).abs()),
        None => hl,
    }
}

fn nearest_zone<'a, I>(zones: I, current_price: f64) -> Option<&'a FvgZone>
where
    I: Iterator<Item = &'a FvgZone>,
{
    zones.min_by(|a, b| {
        zone_distance(a, current_price)
            .partial_cmp(&zone_distance(b, current_price))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.birth_ts.cmp(&b.birth_ts))
    })
}

fn zone_distance(zone: &FvgZone, current_price: f64) -> f64 {
    if current_price < zone.lower {
        zone.lower - current_price
    } else if current_price > zone.upper {
        current_price - zone.upper
    } else {
        0.0
    }
}

fn zone_to_json(zone: &FvgZone) -> Value {
    json!({
        "fvg_id": zone.fvg_id,
        "tf": zone.tf,
        "side": zone.side,
        "left_ts": zone.left_ts.to_rfc3339(),
        "mid_ts": zone.mid_ts.to_rfc3339(),
        "birth_ts": zone.birth_ts.to_rfc3339(),
        "event_available_ts": zone.event_available_ts.to_rfc3339(),
        "lower": zone.lower,
        "upper": zone.upper,
        "mid": zone.mid,
        "width": zone.width,
        "width_atr_ratio": zone.width_atr_ratio,
        "body_ratio_mid": zone.body_ratio_mid,
        "impulse_atr_ratio": zone.impulse_atr_ratio,
        "close_location_mid": zone.close_location_mid,
        "displacement_score": zone.displacement_score,
        "fill_pct": zone.fill_pct,
        "touch_count": zone.touch_count,
        "state": zone.state,
        "age_bars": zone.age_bars,
        "overlap_lvn": false,
        "inside_value_area": false,
        "distance_to_avwap": Value::Null,
        "spot_confirm_at_birth": Value::Null,
    })
}

fn is_active_state(state: &str) -> bool {
    matches!(state, "fresh" | "partially_filled" | "mitigated")
}

fn as_fvg_window(code: &str) -> Option<&'static str> {
    match code {
        "15m" => Some("15m"),
        "4h" => Some("4h"),
        "1d" => Some("1d"),
        "3d" => Some("3d"),
        _ => None,
    }
}

fn price_key(value: f64) -> String {
    format!("{value:.8}")
}
