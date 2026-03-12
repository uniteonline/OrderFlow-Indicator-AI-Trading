use crate::indicators::context::IndicatorContext;
use crate::indicators::indicator_trait::Indicator;
use crate::indicators::shared::output_mapper::snapshot_only;
use chrono::Duration;
use serde_json::{json, Map, Value};

const EPS: f64 = 1e-12;

pub struct I21RvwapSigmaBands;

impl Indicator for I21RvwapSigmaBands {
    fn code(&self) -> &'static str {
        "rvwap_sigma_bands"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> crate::indicators::context::IndicatorComputation {
        let points = collect_weighted_points(ctx);
        let Some(last_idx) = points.len().checked_sub(1) else {
            return snapshot_only(
                self.code(),
                json!({
                    "indicator": self.code(),
                    "window": "1m",
                    "as_of_ts": (ctx.ts_bucket + Duration::minutes(1)).to_rfc3339(),
                    "source_mode": "ohlcv_approx_1m",
                    "by_window": {},
                    "series_by_output_window": {},
                }),
            );
        };

        let mut by_window = Map::new();
        for window_code in &ctx.rvwap_windows {
            let Some(window_minutes) = window_to_minutes(window_code) else {
                continue;
            };
            let value = compute_stats_at(&points, last_idx, window_minutes, ctx.rvwap_min_samples)
                .map(stats_to_json)
                .unwrap_or_else(|| null_stats_json(window_minutes));
            by_window.insert(window_code.clone(), value);
        }

        let mut series_by_output_window = Map::new();
        for out_code in &ctx.rvwap_output_windows {
            let Some(out_minutes) = window_to_minutes(out_code) else {
                continue;
            };
            let mut series = Vec::new();
            for idx in 0..points.len() {
                let anchor = points[idx].ts_bucket + Duration::minutes(1);
                if anchor.timestamp().rem_euclid(out_minutes * 60) != 0 {
                    continue;
                }

                let mut row_windows = Map::new();
                for rolling_code in &ctx.rvwap_windows {
                    let Some(rolling_minutes) = window_to_minutes(rolling_code) else {
                        continue;
                    };
                    let value =
                        compute_stats_at(&points, idx, rolling_minutes, ctx.rvwap_min_samples)
                            .map(stats_to_json)
                            .unwrap_or_else(|| null_stats_json(rolling_minutes));
                    row_windows.insert(rolling_code.clone(), value);
                }

                series.push(json!({
                    "ts": anchor.to_rfc3339(),
                    "by_window": row_windows,
                }));
            }
            series_by_output_window.insert(out_code.clone(), Value::Array(series));
        }

        snapshot_only(
            self.code(),
            json!({
                "indicator": self.code(),
                "window": "1m",
                "as_of_ts": (ctx.ts_bucket + Duration::minutes(1)).to_rfc3339(),
                "source_mode": "ohlcv_approx_1m",
                "by_window": by_window,
                "series_by_output_window": series_by_output_window,
            }),
        )
    }
}

#[derive(Debug, Clone)]
struct WeightedPoint {
    ts_bucket: chrono::DateTime<chrono::Utc>,
    price: f64,
    weight: f64,
}

#[derive(Debug, Clone, Copy)]
struct RvwapStats {
    window_minutes: i64,
    rvwap: f64,
    sigma: f64,
    z: f64,
    sample_count: usize,
}

fn collect_weighted_points(ctx: &IndicatorContext) -> Vec<WeightedPoint> {
    ctx.history_futures
        .iter()
        .filter_map(|h| {
            let price = ohlcv_typical_price(h.high_price, h.low_price, h.close_price)?;
            Some(WeightedPoint {
                ts_bucket: h.ts_bucket,
                price,
                weight: h.total_qty.max(0.0),
            })
        })
        .collect()
}

fn ohlcv_typical_price(high: Option<f64>, low: Option<f64>, close: Option<f64>) -> Option<f64> {
    Some((high? + low? + close?) / 3.0)
}

fn compute_stats_at(
    points: &[WeightedPoint],
    end_idx: usize,
    window_minutes: i64,
    min_samples: usize,
) -> Option<RvwapStats> {
    if points.is_empty() || end_idx >= points.len() {
        return None;
    }

    let end_ts = points[end_idx].ts_bucket;
    let start_ts = end_ts - Duration::minutes(window_minutes.max(1));

    let mut sum_w = 0.0;
    let mut sum_pv = 0.0;
    let mut sum_p2v = 0.0;
    let mut sample_count = 0usize;

    for idx in (0..=end_idx).rev() {
        let p = &points[idx];
        if p.ts_bucket <= start_ts {
            break;
        }
        if p.weight > 0.0 {
            sum_w += p.weight;
            sum_pv += p.price * p.weight;
            sum_p2v += p.price * p.price * p.weight;
            sample_count += 1;
        }
    }

    if sample_count < min_samples || sum_w <= EPS {
        return None;
    }

    let rvwap = sum_pv / sum_w;
    let variance = (sum_p2v / sum_w - rvwap * rvwap).max(0.0);
    let sigma = variance.sqrt();
    let current_price = points[end_idx].price;
    let z = (current_price - rvwap) / (sigma + EPS);

    Some(RvwapStats {
        window_minutes,
        rvwap,
        sigma,
        z,
        sample_count,
    })
}

fn stats_to_json(stats: RvwapStats) -> Value {
    json!({
        "window_minutes": stats.window_minutes,
        "rvwap_w": stats.rvwap,
        "rvwap_sigma_w": stats.sigma,
        "rvwap_band_plus_1": stats.rvwap + stats.sigma,
        "rvwap_band_plus_2": stats.rvwap + 2.0 * stats.sigma,
        "rvwap_band_minus_1": stats.rvwap - stats.sigma,
        "rvwap_band_minus_2": stats.rvwap - 2.0 * stats.sigma,
        "z_price_minus_rvwap": stats.z,
        "samples_used": stats.sample_count,
    })
}

fn null_stats_json(window_minutes: i64) -> Value {
    json!({
        "window_minutes": window_minutes,
        "rvwap_w": null,
        "rvwap_sigma_w": null,
        "rvwap_band_plus_1": null,
        "rvwap_band_plus_2": null,
        "rvwap_band_minus_1": null,
        "rvwap_band_minus_2": null,
        "z_price_minus_rvwap": null,
        "samples_used": 0,
    })
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
    use super::{compute_stats_at, ohlcv_typical_price, WeightedPoint};
    use chrono::{TimeZone, Utc};

    #[test]
    fn rvwap_weighted_stats_match_expected() {
        let ts = Utc
            .with_ymd_and_hms(2026, 3, 5, 0, 0, 0)
            .single()
            .expect("valid ts");
        let points = vec![
            WeightedPoint {
                ts_bucket: ts,
                price: 100.0,
                weight: 1.0,
            },
            WeightedPoint {
                ts_bucket: ts + chrono::Duration::minutes(1),
                price: 102.0,
                weight: 1.0,
            },
            WeightedPoint {
                ts_bucket: ts + chrono::Duration::minutes(2),
                price: 104.0,
                weight: 2.0,
            },
        ];

        let stats = compute_stats_at(&points, 2, 15, 2).expect("stats");
        assert!((stats.rvwap - 102.5).abs() < 1e-9);
        assert!(stats.sigma > 0.0);
    }

    #[test]
    fn ohlcv_typical_price_matches_formula() {
        let p = ohlcv_typical_price(Some(12.0), Some(6.0), Some(9.0)).expect("price");
        assert!((p - 9.0).abs() < 1e-12);
        assert!(ohlcv_typical_price(Some(12.0), None, Some(9.0)).is_none());
    }
}
