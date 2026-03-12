use crate::indicators::context::{IndicatorComputation, IndicatorContext, IndicatorSnapshotRow};
use crate::indicators::indicator_trait::Indicator;
use crate::runtime::state_store::MinuteHistory;
use chrono::{DateTime, Duration, TimeZone, Utc};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, VecDeque};

const WINDOWS: [(&str, i64); 5] = [
    ("15m", 15),
    ("1h", 60),
    ("4h", 240),
    ("1d", 1440),
    ("3d", 4320),
];
const LOOKBACK_DAYS: i64 = 7;
const DELTA_Z_LOOKBACK: usize = 64;
const CVD_Z_LOOKBACK: usize = 64;
const MIN_RANGE: f64 = 0.01;

#[derive(Debug, Clone, Copy)]
struct BarPoint {
    ts: DateTime<Utc>,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    delta: f64,
    relative_delta: f64,
    delta_slant: Option<f64>,
    cvd_7d: f64,
}

#[derive(Debug, Clone, Copy, Default)]
struct BarBuild {
    open: Option<f64>,
    high: f64,
    low: f64,
    close: Option<f64>,
    volume: f64,
    delta: f64,
}

pub struct I14CvdPack;

impl Indicator for I14CvdPack {
    fn code(&self) -> &'static str {
        "cvd_pack"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation {
        let delta_fut = ctx.futures.delta;
        let delta_spot = ctx.spot.delta;
        let fut_slope = ctx.cvd_slope_futures(30);
        let spot_slope = ctx.cvd_slope_spot(30);
        let spot_dom = delta_spot.abs() / (delta_spot.abs() + delta_fut.abs() + 1e-12);

        let mut by_window = serde_json::Map::new();
        for (label, mins) in WINDOWS {
            let fut_bars = aggregate_bars(&ctx.history_futures, ctx.ts_bucket, mins);
            let spot_bars = aggregate_bars(&ctx.history_spot, ctx.ts_bucket, mins);
            let series = build_dual_series(&fut_bars, &spot_bars);
            let series_count = series.len();
            by_window.insert(
                label.to_string(),
                json!({
                    "window": label,
                    "series_count": series_count,
                    "series": series
                }),
            );
        }

        IndicatorComputation {
            snapshot: Some(IndicatorSnapshotRow {
                indicator_code: self.code(),
                window_code: "1m",
                payload_json: json!({
                    "delta_fut": delta_fut,
                    "delta_spot": delta_spot,
                    "relative_delta_fut": ctx.futures.relative_delta,
                    "relative_delta_spot": ctx.spot.relative_delta,
                    "xmk_delta_gap_s_minus_f": delta_spot - delta_fut,
                    "cvd_slope_fut": fut_slope,
                    "cvd_slope_spot": spot_slope,
                    "spot_flow_dominance": spot_dom,
                    "spot_lead_score": if delta_spot.abs() > delta_fut.abs() {1.0} else {0.5},
                    "likely_driver": if delta_spot.abs() > delta_fut.abs() {"spot"} else {"futures"},
                    "by_window": Value::Object(by_window)
                }),
            }),
            ..Default::default()
        }
    }
}

fn build_dual_series(fut_bars: &[BarPoint], spot_bars: &[BarPoint]) -> Vec<Value> {
    if fut_bars.is_empty() || spot_bars.is_empty() {
        return Vec::new();
    }

    let mut spot_idx = HashMap::new();
    for (idx, b) in spot_bars.iter().enumerate() {
        spot_idx.insert(b.ts.timestamp(), idx);
    }

    let fut_delta_z = rolling_z(
        &fut_bars.iter().map(|b| b.delta).collect::<Vec<_>>(),
        DELTA_Z_LOOKBACK,
    );
    let fut_cvd_z = rolling_z(
        &fut_bars.iter().map(|b| b.cvd_7d).collect::<Vec<_>>(),
        CVD_Z_LOOKBACK,
    );
    let spot_delta_z = rolling_z(
        &spot_bars.iter().map(|b| b.delta).collect::<Vec<_>>(),
        DELTA_Z_LOOKBACK,
    );
    let spot_cvd_z = rolling_z(
        &spot_bars.iter().map(|b| b.cvd_7d).collect::<Vec<_>>(),
        CVD_Z_LOOKBACK,
    );

    let mut out = Vec::new();
    for (fi, fb) in fut_bars.iter().enumerate() {
        let Some(si) = spot_idx.get(&fb.ts.timestamp()).copied() else {
            continue;
        };
        let sb = spot_bars[si];

        let xmk_delta_gap = spot_delta_z[si].zip(fut_delta_z[fi]).map(|(s, f)| s - f);
        let xmk_cvd_gap = spot_cvd_z[si].zip(fut_cvd_z[fi]).map(|(s, f)| s - f);
        let spot_dom = spot_delta_z[si]
            .zip(fut_delta_z[fi])
            .map(|(s, f)| s.abs() / (s.abs() + f.abs() + 1e-12));

        out.push(json!({
            "ts": fb.ts.to_rfc3339(),
            "open_fut": fb.open,
            "high_fut": fb.high,
            "low_fut": fb.low,
            "close_fut": fb.close,
            "volume_fut": fb.volume,
            "delta_fut": fb.delta,
            "relative_delta_fut": fb.relative_delta,
            "delta_slant_fut": fb.delta_slant,
            "cvd_7d_fut": fb.cvd_7d,

            "open_spot": sb.open,
            "high_spot": sb.high,
            "low_spot": sb.low,
            "close_spot": sb.close,
            "volume_spot": sb.volume,
            "delta_spot": sb.delta,
            "relative_delta_spot": sb.relative_delta,
            "delta_slant_spot": sb.delta_slant,
            "cvd_7d_spot": sb.cvd_7d,

            "xmk_delta_gap_s_minus_f": xmk_delta_gap,
            "xmk_cvd_gap_s_minus_f": xmk_cvd_gap,
            "spot_flow_dominance": spot_dom
        }));
    }
    out
}

fn aggregate_bars(
    history: &[MinuteHistory],
    ts_bucket: DateTime<Utc>,
    interval_mins: i64,
) -> Vec<BarPoint> {
    let start = ts_bucket - Duration::days(LOOKBACK_DAYS);
    // `ts_bucket` is minute-labeled, so the effective cutoff is this minute's close.
    // Keep only bars with bar-end <= cutoff to avoid future-dated samples.
    let cutoff = ts_bucket + Duration::minutes(1);
    let mut bars: BTreeMap<i64, BarBuild> = BTreeMap::new();

    for h in history {
        if h.ts_bucket <= start || h.ts_bucket > ts_bucket {
            continue;
        }
        let end_ts = align_bar_end(h.ts_bucket + Duration::minutes(1), interval_mins);
        if end_ts > cutoff {
            continue;
        }
        let key = end_ts.timestamp();
        let build = bars.entry(key).or_insert_with(|| BarBuild {
            open: None,
            high: f64::NEG_INFINITY,
            low: f64::INFINITY,
            close: None,
            volume: 0.0,
            delta: 0.0,
        });

        let o = h
            .open_price
            .or(h.close_price)
            .or(h.last_price)
            .unwrap_or(0.0);
        let c = h.close_price.or(h.last_price).unwrap_or(o);
        let hi = h.high_price.or(Some(c)).unwrap_or(c);
        let lo = h.low_price.or(Some(c)).unwrap_or(c);

        if build.open.is_none() {
            build.open = Some(o);
        }
        build.close = Some(c);
        build.high = build.high.max(hi);
        build.low = build.low.min(lo);
        build.volume += h.total_qty;
        build.delta += h.delta;
    }

    let mut points = bars
        .into_iter()
        .map(|(ts, b)| {
            let ts = Utc.timestamp_opt(ts, 0).single().unwrap_or(ts_bucket);
            let open = b.open.unwrap_or(0.0);
            let close = b.close.unwrap_or(open);
            let high = if b.high.is_finite() { b.high } else { close };
            let low = if b.low.is_finite() { b.low } else { close };
            let relative_delta = if b.volume > 0.0 {
                b.delta / b.volume
            } else {
                0.0
            };
            let range = high - low;
            let delta_slant = if range >= MIN_RANGE {
                Some(b.delta / (range + 1e-12))
            } else {
                None
            };

            BarPoint {
                ts,
                open,
                high,
                low,
                close,
                volume: b.volume,
                delta: b.delta,
                relative_delta,
                delta_slant,
                cvd_7d: 0.0,
            }
        })
        .collect::<Vec<_>>();

    // rolling 7d CVD across bars
    let mut q: VecDeque<(DateTime<Utc>, f64)> = VecDeque::new();
    let mut sum = 0.0;
    for p in &mut points {
        let bound = p.ts - Duration::days(LOOKBACK_DAYS);
        while let Some((ts, d)) = q.front().copied() {
            if ts <= bound {
                q.pop_front();
                sum -= d;
            } else {
                break;
            }
        }
        sum += p.delta;
        q.push_back((p.ts, p.delta));
        p.cvd_7d = sum;
    }

    points
}

fn align_bar_end(ts: DateTime<Utc>, interval_mins: i64) -> DateTime<Utc> {
    let sec = ts.timestamp();
    let span = interval_mins * 60;
    // Ceiling alignment: if `ts` is already on a boundary return it as-is;
    // otherwise round up to the next boundary.
    // The previous `((sec/span)+1)*span` always advanced one extra span when
    // `sec` was exactly divisible, which caused every bar's window to shift
    // two minutes early (BUG-1) and excluded the last minute of the preceding
    // window (BUG-2).
    let rem = sec.rem_euclid(span);
    let end = if rem == 0 { sec } else { sec - rem + span };
    Utc.timestamp_opt(end, 0).single().unwrap_or(ts)
}

fn rolling_z(values: &[f64], lookback: usize) -> Vec<Option<f64>> {
    let mut out = vec![None; values.len()];
    for i in 0..values.len() {
        if i < lookback {
            continue;
        }
        let hist = &values[i - lookback..i];
        let mean = hist.iter().sum::<f64>() / hist.len() as f64;
        let var = hist
            .iter()
            .map(|v| {
                let d = *v - mean;
                d * d
            })
            .sum::<f64>()
            / hist.len() as f64;
        let sd = var.sqrt();
        out[i] = if sd <= 1e-12 {
            Some(0.0)
        } else {
            Some((values[i] - mean) / sd)
        };
    }
    out
}
