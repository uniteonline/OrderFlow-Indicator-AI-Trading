use crate::indicators::context::{
    mean, stddev, IndicatorComputation, IndicatorContext, IndicatorSnapshotRow,
};
use crate::indicators::indicator_trait::Indicator;
use crate::runtime::state_store::MinuteHistory;
use chrono::Duration;
use serde_json::{json, Value};

const VPIN_Z_LOOKBACK: usize = 120;
const VPIN_BUCKET_SIZE_ETH: f64 = 50.0;
const VPIN_ROLLING_BUCKET_COUNT: usize = 50;
const WINDOWS: [(&str, i64); 5] = [
    ("15m", 15),
    ("1h", 60),
    ("4h", 240),
    ("1d", 1440),
    ("3d", 4320),
];

pub struct I17Vpin;

impl Indicator for I17Vpin {
    fn code(&self) -> &'static str {
        "vpin"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation {
        let fut_series = ctx
            .history_futures
            .iter()
            .map(|h| h.vpin.clamp(0.0, 1.0))
            .collect::<Vec<_>>();
        let spot_series = ctx
            .history_spot
            .iter()
            .map(|h| h.vpin.clamp(0.0, 1.0))
            .collect::<Vec<_>>();

        let vpin_fut = fut_series.last().copied();
        let vpin_spot = spot_series.last().copied();
        let vpin_gap = vpin_spot.zip(vpin_fut).map(|(s, f)| s - f);
        let vpin_ratio = vpin_spot.zip(vpin_fut).map(|(s, f)| s / (f + 1e-12));
        let z_vpin_fut = z_last(&fut_series, VPIN_Z_LOOKBACK);
        let z_vpin_spot = z_last(&spot_series, VPIN_Z_LOOKBACK);
        let z_vpin_gap = z_vpin_spot.zip(z_vpin_fut).map(|(s, f)| s - f);

        let toxicity_state = match vpin_fut.unwrap_or(0.0) {
            v if v >= 0.65 => "high",
            v if v >= 0.45 => "medium",
            _ => "low",
        };

        let mut by_window = serde_json::Map::new();
        for (label, mins) in WINDOWS {
            let fut = filter_window(&ctx.history_futures, ctx.ts_bucket, mins);
            let spot = filter_window(&ctx.history_spot, ctx.ts_bucket, mins);
            let f_series = fut
                .iter()
                .map(|h| h.vpin.clamp(0.0, 1.0))
                .collect::<Vec<_>>();
            let s_series = spot
                .iter()
                .map(|h| h.vpin.clamp(0.0, 1.0))
                .collect::<Vec<_>>();
            let f_last = f_series.last().copied();
            let s_last = s_series.last().copied();
            let f_z = z_last(&f_series, VPIN_Z_LOOKBACK);
            let s_z = z_last(&s_series, VPIN_Z_LOOKBACK);

            by_window.insert(
                label.to_string(),
                json!({
                    "window": label,
                    "vpin_fut": f_last,
                    "vpin_spot": s_last,
                    "xmk_vpin_gap_s_minus_f": s_last.zip(f_last).map(|(s, f)| s - f),
                    "vpin_ratio_s_over_f": s_last.zip(f_last).map(|(s, f)| s / (f + 1e-12)),
                    "z_vpin_fut": f_z,
                    "z_vpin_spot": s_z,
                    "z_vpin_gap_s_minus_f": s_z.zip(f_z).map(|(s, f)| s - f),
                }),
            );
        }

        IndicatorComputation {
            snapshot: Some(IndicatorSnapshotRow {
                indicator_code: self.code(),
                window_code: "1m",
                payload_json: json!({
                    "vpin_model": "volume_bucket",
                    "vpin_unit": "ratio",
                    "vpin_bucket_size_eth": VPIN_BUCKET_SIZE_ETH,
                    "vpin_rolling_bucket_count": VPIN_ROLLING_BUCKET_COUNT,
                    "vpin_fut": vpin_fut,
                    "vpin_spot": vpin_spot,
                    "xmk_vpin_gap_s_minus_f": vpin_gap,
                    "vpin_ratio_s_over_f": vpin_ratio,
                    "z_vpin_fut": z_vpin_fut,
                    "z_vpin_spot": z_vpin_spot,
                    "z_vpin_gap_s_minus_f": z_vpin_gap,
                    "toxicity_state": toxicity_state,
                    "by_window": Value::Object(by_window)
                }),
            }),
            ..Default::default()
        }
    }
}

fn filter_window(
    history: &[MinuteHistory],
    ts_bucket: chrono::DateTime<chrono::Utc>,
    mins: i64,
) -> Vec<MinuteHistory> {
    let start = ts_bucket - Duration::minutes(mins);
    history
        .iter()
        .filter(|h| h.ts_bucket > start && h.ts_bucket <= ts_bucket)
        .cloned()
        .collect()
}

fn z_last(values: &[f64], lookback: usize) -> Option<f64> {
    if values.len() < 5 {
        return None;
    }
    let current = *values.last()?;
    let win_len = values.len().min(lookback).saturating_sub(1);
    if win_len < 5 {
        return None;
    }
    let start = values.len() - 1 - win_len;
    let history = &values[start..values.len() - 1];
    let m = mean(history)?;
    let sd = stddev(history)?;
    if sd <= 1e-12 {
        // All history values are zero; engine is still in warmup, signal is absent.
        // Return None to distinguish from a genuine z-score of 0.0.
        let all_zero = history.iter().all(|v| *v <= 1e-12);
        return if all_zero { None } else { Some(0.0) };
    }
    Some((current - m) / sd)
}
