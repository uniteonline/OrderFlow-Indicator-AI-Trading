use crate::indicators::context::{IndicatorComputation, IndicatorContext, IndicatorSnapshotRow};
use crate::indicators::indicator_trait::Indicator;
use chrono::Duration;
use serde_json::{json, Value};

const LOOKBACK_DAYS: i64 = 7;
const WINDOWS: [(&str, i64); 5] = [
    ("15m", 15),
    ("1h", 60),
    ("4h", 240),
    ("1d", 1440),
    ("3d", 4320),
];

pub struct I18Avwap;

impl Indicator for I18Avwap {
    fn code(&self) -> &'static str {
        "avwap"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation {
        let lookback_start = ctx.ts_bucket - Duration::days(LOOKBACK_DAYS);
        let fut_window = ctx
            .history_futures
            .iter()
            .filter(|h| h.ts_bucket > lookback_start && h.ts_bucket <= ctx.ts_bucket)
            .cloned()
            .collect::<Vec<_>>();
        let spot_window = ctx
            .history_spot
            .iter()
            .filter(|h| h.ts_bucket > lookback_start && h.ts_bucket <= ctx.ts_bucket)
            .cloned()
            .collect::<Vec<_>>();

        let fut_avwap = avwap_of_slice(&fut_window);
        let spot_avwap = avwap_of_slice(&spot_window);
        let fut_last_price = ctx.futures.last_price;
        let minute_end = ctx.ts_bucket + Duration::minutes(1);
        let fut_mark_price = ctx
            .latest_mark_pair_at_or_before(minute_end)
            .map(|(_, mark_price)| mark_price);

        let price_minus_avwap_fut = fut_last_price.zip(fut_avwap).map(|(p, a)| p - a);
        let price_minus_spot_avwap_fut = fut_last_price.zip(spot_avwap).map(|(p, a)| p - a);
        let price_minus_spot_avwap_futmark = fut_mark_price.zip(spot_avwap).map(|(p, a)| p - a);
        let avwap_gap = fut_avwap.zip(spot_avwap).map(|(f, s)| f - s);
        let z_avwap_gap = zscore_gap(&fut_window, &spot_window, avwap_gap);

        let series = WINDOWS
            .iter()
            .map(|(label, mins)| (*label, build_series(&fut_window, &spot_window, *mins)))
            .collect::<Vec<_>>();

        IndicatorComputation {
            snapshot: Some(IndicatorSnapshotRow {
                indicator_code: self.code(),
                window_code: "1m",
                payload_json: json!({
                    "indicator": "avwap_dual_market",
                    "anchor_ts": lookback_start.to_rfc3339(),
                    "lookback": "7d",
                    "window": "1m",
                    "avwap_fut": fut_avwap,
                    "avwap_spot": spot_avwap,
                    "fut_last_price": fut_last_price,
                    "fut_mark_price": fut_mark_price,
                    "price_minus_avwap_fut": price_minus_avwap_fut,
                    "price_minus_spot_avwap_fut": price_minus_spot_avwap_fut,
                    "price_minus_spot_avwap_futmark": price_minus_spot_avwap_futmark,
                    "xmk_avwap_gap_f_minus_s": avwap_gap,
                    "zavwap_gap": z_avwap_gap,
                    "series_by_window": {
                        "15m": find_series(&series, "15m"),
                        "1h": find_series(&series, "1h"),
                        "4h": find_series(&series, "4h"),
                        "1d": find_series(&series, "1d"),
                        "3d": find_series(&series, "3d")
                    }
                }),
            }),
            ..Default::default()
        }
    }
}

fn avwap_of_slice(history: &[crate::runtime::state_store::MinuteHistory]) -> Option<f64> {
    let num = history.iter().map(|h| h.total_notional).sum::<f64>();
    let den = history.iter().map(|h| h.total_qty).sum::<f64>();
    if den > 0.0 {
        Some(num / den)
    } else {
        None
    }
}

fn build_series(
    fut: &[crate::runtime::state_store::MinuteHistory],
    spot: &[crate::runtime::state_store::MinuteHistory],
    interval_mins: i64,
) -> Vec<Value> {
    let n = fut.len().min(spot.len());
    if n == 0 || interval_mins <= 0 {
        return Vec::new();
    }

    let fut = &fut[fut.len() - n..];
    let spot = &spot[spot.len() - n..];

    let mut fut_num = Vec::with_capacity(n);
    let mut fut_den = Vec::with_capacity(n);
    let mut spot_num = Vec::with_capacity(n);
    let mut spot_den = Vec::with_capacity(n);
    let mut ts = Vec::with_capacity(n);

    let mut fn_acc = 0.0;
    let mut fd_acc = 0.0;
    let mut sn_acc = 0.0;
    let mut sd_acc = 0.0;
    for i in 0..n {
        fn_acc += fut[i].total_notional;
        fd_acc += fut[i].total_qty;
        sn_acc += spot[i].total_notional;
        sd_acc += spot[i].total_qty;
        fut_num.push(fn_acc);
        fut_den.push(fd_acc);
        spot_num.push(sn_acc);
        spot_den.push(sd_acc);
        ts.push(fut[i].ts_bucket);
    }

    let mut out = Vec::new();
    for i in 0..n {
        let t = ts[i];
        if t.timestamp().rem_euclid(interval_mins * 60) != 0 {
            continue;
        }

        let start_ts = t - Duration::days(LOOKBACK_DAYS);
        let j = lower_bound_ts(&ts, start_ts + Duration::minutes(1));

        let fn_prev = if j > 0 { fut_num[j - 1] } else { 0.0 };
        let fd_prev = if j > 0 { fut_den[j - 1] } else { 0.0 };
        let sn_prev = if j > 0 { spot_num[j - 1] } else { 0.0 };
        let sd_prev = if j > 0 { spot_den[j - 1] } else { 0.0 };

        let fn_seg = fut_num[i] - fn_prev;
        let fd_seg = fut_den[i] - fd_prev;
        let sn_seg = spot_num[i] - sn_prev;
        let sd_seg = spot_den[i] - sd_prev;

        let avwap_fut = if fd_seg > 0.0 {
            Some(fn_seg / fd_seg)
        } else {
            None
        };
        let avwap_spot = if sd_seg > 0.0 {
            Some(sn_seg / sd_seg)
        } else {
            None
        };
        let gap = avwap_fut.zip(avwap_spot).map(|(f, s)| f - s);

        out.push(json!({
            "ts": t.to_rfc3339(),
            "avwap_fut": avwap_fut,
            "avwap_spot": avwap_spot,
            "xmk_avwap_gap_f_minus_s": gap
        }));
    }

    out
}

fn lower_bound_ts(
    values: &[chrono::DateTime<chrono::Utc>],
    target: chrono::DateTime<chrono::Utc>,
) -> usize {
    let mut l = 0usize;
    let mut r = values.len();
    while l < r {
        let m = (l + r) / 2;
        if values[m] < target {
            l = m + 1;
        } else {
            r = m;
        }
    }
    l
}

fn zscore_gap(
    fut: &[crate::runtime::state_store::MinuteHistory],
    spot: &[crate::runtime::state_store::MinuteHistory],
    current_gap: Option<f64>,
) -> Option<f64> {
    let current = current_gap?;
    let n = fut.len().min(spot.len());
    if n < 10 {
        return None;
    }

    let mut gaps = Vec::new();
    for i in 0..n {
        let f = &fut[n - 1 - i];
        let s = &spot[n - 1 - i];
        if f.total_qty <= 0.0 || s.total_qty <= 0.0 {
            continue;
        }
        gaps.push((f.total_notional / f.total_qty) - (s.total_notional / s.total_qty));
    }
    if gaps.len() < 10 {
        return None;
    }

    let mean = gaps.iter().sum::<f64>() / gaps.len() as f64;
    let var = gaps
        .iter()
        .map(|v| {
            let d = v - mean;
            d * d
        })
        .sum::<f64>()
        / gaps.len() as f64;
    let sd = var.sqrt();
    if sd <= 1e-12 {
        return Some(0.0);
    }
    Some((current - mean) / sd)
}

fn find_series(series: &[(&str, Vec<Value>)], key: &str) -> Vec<Value> {
    series
        .iter()
        .find(|(k, _)| *k == key)
        .map(|(_, v)| v.clone())
        .unwrap_or_default()
}
