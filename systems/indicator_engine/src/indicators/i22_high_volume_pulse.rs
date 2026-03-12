use crate::indicators::context::IndicatorContext;
use crate::indicators::indicator_trait::Indicator;
use crate::indicators::shared::output_mapper::snapshot_only;
use crate::runtime::state_store::tick_to_price;
use chrono::Duration;
use serde_json::{json, Map};

const EPS: f64 = 1e-12;

pub struct I22HighVolumePulse;

impl Indicator for I22HighVolumePulse {
    fn code(&self) -> &'static str {
        "high_volume_pulse"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> crate::indicators::context::IndicatorComputation {
        let points = collect_minute_points(ctx);
        let Some(last_idx) = points.len().checked_sub(1) else {
            return snapshot_only(
                self.code(),
                json!({
                    "indicator": self.code(),
                    "window": "1m",
                    "as_of_ts": (ctx.ts_bucket + Duration::minutes(1)).to_rfc3339(),
                    "intrabar_poc_price": null,
                    "intrabar_poc_volume": null,
                    "by_z_window": {},
                    "intrabar_poc_max_by_window": {},
                }),
            );
        };

        let current = &points[last_idx];

        let mut by_z_window = Map::new();
        for code in &ctx.high_volume_pulse_z_windows {
            let Some(window_minutes) = window_to_minutes(code) else {
                continue;
            };
            let value = volume_spike_stats(
                &points,
                last_idx,
                window_minutes,
                ctx.high_volume_pulse_min_samples,
            )
            .map(|stats| {
                json!({
                    "window_minutes": stats.window_minutes,
                    "rolling_volume_w": stats.rolling_volume,
                    "volume_spike_z_w": stats.z,
                    "is_volume_spike_z2": stats.z >= 2.0,
                    "is_volume_spike_z3": stats.z >= 3.0,
                    "lookback_samples": stats.lookback_samples,
                })
            })
            .unwrap_or_else(|| {
                json!({
                    "window_minutes": window_minutes,
                    "rolling_volume_w": null,
                    "volume_spike_z_w": null,
                    "is_volume_spike_z2": false,
                    "is_volume_spike_z3": false,
                    "lookback_samples": 0,
                })
            });
            by_z_window.insert(code.clone(), value);
        }

        let mut poc_max_by_window = Map::new();
        for code in &ctx.high_volume_pulse_summary_windows {
            let Some(window_minutes) = window_to_minutes(code) else {
                continue;
            };
            let value = intrabar_poc_max_in_window(&points, last_idx, window_minutes)
                .map(|row| {
                    json!({
                        "window_minutes": window_minutes,
                        "ts": row.ts_bucket.to_rfc3339(),
                        "intrabar_poc_price": row.poc_price,
                        "intrabar_poc_volume": row.poc_volume,
                    })
                })
                .unwrap_or_else(|| {
                    json!({
                        "window_minutes": window_minutes,
                        "ts": null,
                        "intrabar_poc_price": null,
                        "intrabar_poc_volume": null,
                    })
                });
            poc_max_by_window.insert(code.clone(), value);
        }

        snapshot_only(
            self.code(),
            json!({
                "indicator": self.code(),
                "window": "1m",
                "as_of_ts": (ctx.ts_bucket + Duration::minutes(1)).to_rfc3339(),
                "intrabar_poc_price": current.poc_price,
                "intrabar_poc_volume": current.poc_volume,
                "by_z_window": by_z_window,
                "intrabar_poc_max_by_window": poc_max_by_window,
            }),
        )
    }
}

#[derive(Debug, Clone)]
struct MinutePulsePoint {
    ts_bucket: chrono::DateTime<chrono::Utc>,
    volume: f64,
    poc_price: Option<f64>,
    poc_volume: f64,
}

#[derive(Debug, Clone, Copy)]
struct VolumeSpikeStats {
    window_minutes: i64,
    rolling_volume: f64,
    z: f64,
    lookback_samples: usize,
}

fn collect_minute_points(ctx: &IndicatorContext) -> Vec<MinutePulsePoint> {
    ctx.history_futures
        .iter()
        .map(|h| {
            let (poc_price, poc_volume) = intrabar_poc_from_profile(&h.profile);
            MinutePulsePoint {
                ts_bucket: h.ts_bucket + Duration::minutes(1),
                volume: h.total_qty.max(0.0),
                poc_price,
                poc_volume,
            }
        })
        .collect()
}

fn intrabar_poc_from_profile(
    profile: &std::collections::BTreeMap<i64, crate::runtime::state_store::LevelAgg>,
) -> (Option<f64>, f64) {
    profile
        .iter()
        .max_by(|a, b| {
            a.1.total()
                .partial_cmp(&b.1.total())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|(tick, level)| (Some(tick_to_price(*tick)), level.total()))
        .unwrap_or((None, 0.0))
}

fn rolling_volume(points: &[MinutePulsePoint], end_idx: usize, window_minutes: i64) -> f64 {
    let end_ts = points[end_idx].ts_bucket;
    let start_ts = end_ts - Duration::minutes(window_minutes.max(1));
    let mut sum = 0.0;

    for idx in (0..=end_idx).rev() {
        let p = &points[idx];
        if p.ts_bucket <= start_ts {
            break;
        }
        sum += p.volume;
    }

    sum
}

fn volume_spike_stats(
    points: &[MinutePulsePoint],
    end_idx: usize,
    window_minutes: i64,
    min_samples: usize,
) -> Option<VolumeSpikeStats> {
    if points.is_empty() {
        return None;
    }

    let required = (window_minutes as usize).max(min_samples.max(5));
    if end_idx < required {
        return None;
    }

    let current = rolling_volume(points, end_idx, window_minutes);
    let mut history = Vec::with_capacity(required);
    for step in 1..=required {
        history.push(rolling_volume(points, end_idx - step, window_minutes));
    }

    let mean = history.iter().sum::<f64>() / history.len() as f64;
    let variance = history
        .iter()
        .map(|v| {
            let d = *v - mean;
            d * d
        })
        .sum::<f64>()
        / history.len() as f64;
    let sigma = variance.sqrt();
    let z = (current - mean) / (sigma + EPS);

    Some(VolumeSpikeStats {
        window_minutes,
        rolling_volume: current,
        z,
        lookback_samples: history.len(),
    })
}

fn intrabar_poc_max_in_window(
    points: &[MinutePulsePoint],
    end_idx: usize,
    window_minutes: i64,
) -> Option<MinutePulsePoint> {
    let end_ts = points[end_idx].ts_bucket;
    let start_ts = end_ts - Duration::minutes(window_minutes.max(1));

    points
        .iter()
        .take(end_idx + 1)
        .filter(|p| p.ts_bucket > start_ts && p.ts_bucket <= end_ts)
        .max_by(|a, b| {
            a.poc_volume
                .partial_cmp(&b.poc_volume)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .cloned()
}

fn window_to_minutes(code: &str) -> Option<i64> {
    match code {
        "15m" => Some(15),
        "1h" => Some(60),
        "4h" => Some(240),
        "1d" => Some(1440),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::intrabar_poc_from_profile;
    use crate::runtime::state_store::LevelAgg;
    use std::collections::BTreeMap;

    #[test]
    fn intrabar_poc_selects_max_volume_tick() {
        let mut profile = BTreeMap::new();
        profile.insert(
            100,
            LevelAgg {
                buy_qty: 1.0,
                sell_qty: 1.0,
            },
        );
        profile.insert(
            101,
            LevelAgg {
                buy_qty: 3.0,
                sell_qty: 2.0,
            },
        );

        let (price, volume) = intrabar_poc_from_profile(&profile);
        assert_eq!(price, Some(1.01));
        assert!((volume - 5.0).abs() < 1e-12);
    }
}
