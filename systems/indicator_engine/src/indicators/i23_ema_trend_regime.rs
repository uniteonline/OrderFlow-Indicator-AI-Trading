use crate::indicators::context::{IndicatorContext, KlineHistoryBar};
use crate::indicators::i19_kline_history::build_interval_bar_records;
use crate::indicators::indicator_trait::Indicator;
use crate::indicators::shared::output_mapper::snapshot_only;
use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

pub struct I23EmaTrendRegime;

#[derive(Debug, Clone)]
struct HtfRegimePoint {
    close_time: DateTime<Utc>,
    ema_fast: Option<f64>,
    ema_slow: Option<f64>,
    regime: &'static str,
}

impl Indicator for I23EmaTrendRegime {
    fn code(&self) -> &'static str {
        "ema_trend_regime"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> crate::indicators::context::IndicatorComputation {
        let close_1m = collect_1m_closes(ctx);
        let close_1m_values = close_1m.iter().map(|(_, v)| *v).collect::<Vec<_>>();

        let mut base_ema_values = Map::new();
        for period in &ctx.ema_base_periods {
            let value = ema_last(&close_1m_values, *period);
            base_ema_values.insert(period.to_string(), json!(value));
        }

        let ema13 = pick_or_compute_base_ema(&base_ema_values, &close_1m, 13);
        let ema21 = pick_or_compute_base_ema(&base_ema_values, &close_1m, 21);
        let ema34 = pick_or_compute_base_ema(&base_ema_values, &close_1m, 34);

        let mut band_candidates = Vec::new();
        if let Some(v) = ema13 {
            band_candidates.push(v);
        }
        if let Some(v) = ema21 {
            band_candidates.push(v);
        }
        if let Some(v) = ema34 {
            band_candidates.push(v);
        }
        let ema_band_low = band_candidates.iter().copied().reduce(f64::min);
        let ema_band_high = band_candidates.iter().copied().reduce(f64::max);

        let period_fast = pick_period(&ctx.ema_htf_periods, 100, 0);
        let period_slow = pick_period(&ctx.ema_htf_periods, 200, 1);

        let current_minute_close = ctx.ts_bucket + Duration::minutes(1);
        let mut ema_100_htf = Map::new();
        let mut ema_200_htf = Map::new();
        let mut trend_regime_by_tf = Map::new();
        let mut htf_series_by_tf: BTreeMap<String, Vec<HtfRegimePoint>> = BTreeMap::new();

        for tf_code in &ctx.ema_htf_windows {
            let Some(tf_minutes) = window_to_minutes(tf_code) else {
                continue;
            };

            let mut merged = merge_htf_bars(
                build_interval_bar_records(
                    &ctx.history_futures,
                    tf_minutes,
                    usize::MAX,
                    current_minute_close,
                ),
                htf_db_bars(ctx, tf_code),
            );
            merged.sort_by_key(|bar| bar.open_time);

            let series = build_htf_regime_series(&merged, period_fast, period_slow);
            let current_state = htf_state_at(&series, current_minute_close);
            ema_100_htf.insert(
                tf_code.clone(),
                json!(current_state.and_then(|s| s.ema_fast)),
            );
            ema_200_htf.insert(
                tf_code.clone(),
                json!(current_state.and_then(|s| s.ema_slow)),
            );
            trend_regime_by_tf.insert(
                tf_code.clone(),
                json!(current_state.map(|s| s.regime).unwrap_or("neutral")),
            );
            htf_series_by_tf.insert(tf_code.clone(), series);
        }

        let regimes = trend_regime_by_tf
            .values()
            .filter_map(|v| v.as_str())
            .collect::<Vec<_>>();
        let overall_regime = combine_regimes(&regimes);

        let mut output_sampling = Map::new();
        let mut ffill_series_by_output_window = Map::new();
        for out_code in &ctx.ema_output_windows {
            let Some(out_minutes) = window_to_minutes(out_code) else {
                continue;
            };
            let mut series = Vec::new();
            for (ts, _) in &close_1m {
                let anchor = *ts + Duration::minutes(1);
                if anchor.timestamp().rem_euclid(out_minutes * 60) != 0 {
                    continue;
                }

                let mut by_tf = Map::new();
                let mut row_regimes = Vec::new();
                for tf_code in &ctx.ema_htf_windows {
                    let state = htf_series_by_tf
                        .get(tf_code)
                        .and_then(|rows| htf_state_at(rows, anchor));
                    let regime = state.map(|s| s.regime).unwrap_or("neutral");
                    row_regimes.push(regime);
                    by_tf.insert(
                        tf_code.clone(),
                        json!({
                            "ema_100_htf": state.and_then(|s| s.ema_fast),
                            "ema_200_htf": state.and_then(|s| s.ema_slow),
                            "trend_regime": regime,
                            "source_close_ts": state.map(|s| s.close_time.to_rfc3339()),
                        }),
                    );
                }
                series.push(json!({
                    "ts": anchor.to_rfc3339(),
                    "trend_regime": combine_regimes(&row_regimes),
                    "by_tf": by_tf,
                }));
            }

            let (sample_ts, sample_regime) = series
                .last()
                .map(|row| {
                    (
                        row.get("ts").cloned().unwrap_or(Value::Null),
                        row.get("trend_regime")
                            .cloned()
                            .unwrap_or_else(|| json!("neutral")),
                    )
                })
                .unwrap_or((Value::Null, json!(overall_regime)));
            output_sampling.insert(
                out_code.clone(),
                json!({
                    "ts": sample_ts,
                    "trend_regime": sample_regime,
                }),
            );
            ffill_series_by_output_window.insert(out_code.clone(), Value::Array(series));
        }

        snapshot_only(
            self.code(),
            json!({
                "indicator": self.code(),
                "window": "1m",
                "as_of_ts": current_minute_close.to_rfc3339(),
                "ema_13": ema13,
                "ema_21": ema21,
                "ema_34": ema34,
                "ema_band_low": ema_band_low,
                "ema_band_high": ema_band_high,
                "ema_100_htf": ema_100_htf,
                "ema_200_htf": ema_200_htf,
                "trend_regime_by_tf": trend_regime_by_tf,
                "trend_regime": overall_regime,
                "output_sampling": output_sampling,
                "ffill_series_by_output_window": ffill_series_by_output_window,
            }),
        )
    }
}

fn collect_1m_closes(ctx: &IndicatorContext) -> Vec<(DateTime<Utc>, f64)> {
    ctx.history_futures
        .iter()
        .filter_map(|h| {
            h.close_price
                .or(h.last_price)
                .or(h.open_price)
                .map(|close| (h.ts_bucket, close))
        })
        .collect()
}

fn pick_or_compute_base_ema(
    base_map: &Map<String, Value>,
    closes: &[(DateTime<Utc>, f64)],
    period: usize,
) -> Option<f64> {
    base_map
        .get(&period.to_string())
        .and_then(|v| v.as_f64())
        .or_else(|| {
            let values = closes.iter().map(|(_, v)| *v).collect::<Vec<_>>();
            ema_last(&values, period)
        })
}

fn merge_htf_bars(
    mut in_mem: Vec<KlineHistoryBar>,
    db_rows: &[KlineHistoryBar],
) -> Vec<KlineHistoryBar> {
    let mut merged = BTreeMap::new();
    for bar in db_rows {
        merged.insert(bar.open_time, bar.clone());
    }
    for bar in in_mem.drain(..) {
        merged.insert(bar.open_time, bar);
    }
    merged.into_values().collect()
}

fn htf_db_bars<'a>(ctx: &'a IndicatorContext, tf_code: &str) -> &'a [KlineHistoryBar] {
    match tf_code {
        "4h" => &ctx.kline_history_futures_4h_db,
        "1d" => &ctx.kline_history_futures_1d_db,
        _ => &[],
    }
}

fn pick_period(periods: &[usize], preferred: usize, fallback_idx: usize) -> usize {
    periods
        .iter()
        .copied()
        .find(|v| *v == preferred)
        .or_else(|| periods.get(fallback_idx).copied())
        .unwrap_or(preferred)
}

fn build_htf_regime_series(
    bars: &[KlineHistoryBar],
    period_fast: usize,
    period_slow: usize,
) -> Vec<HtfRegimePoint> {
    let mut closes = Vec::new();
    let mut out = Vec::new();
    for bar in bars.iter().filter(|bar| bar.is_closed) {
        let Some(close) = bar.close else {
            continue;
        };
        closes.push(close);
        let ema_fast = ema_last(&closes, period_fast);
        let ema_slow = ema_last(&closes, period_slow);
        out.push(HtfRegimePoint {
            close_time: bar.close_time,
            ema_fast,
            ema_slow,
            regime: regime_of_pair(ema_fast, ema_slow),
        });
    }
    out
}

fn htf_state_at(series: &[HtfRegimePoint], anchor: DateTime<Utc>) -> Option<&HtfRegimePoint> {
    series.iter().rev().find(|row| row.close_time <= anchor)
}

fn ema_last(values: &[f64], period: usize) -> Option<f64> {
    if period == 0 || values.is_empty() {
        return None;
    }
    let alpha = 2.0 / (period as f64 + 1.0);
    let mut ema = values[0];
    for value in values.iter().skip(1) {
        ema = alpha * *value + (1.0 - alpha) * ema;
    }
    Some(ema)
}

fn regime_of_pair(fast: Option<f64>, slow: Option<f64>) -> &'static str {
    match fast.zip(slow) {
        Some((f, s)) if f > s => "bull",
        Some((f, s)) if f < s => "bear",
        _ => "neutral",
    }
}

fn combine_regimes(regimes: &[&str]) -> &'static str {
    if regimes.is_empty() {
        return "neutral";
    }
    if regimes.iter().all(|v| *v == "bull") {
        "bull"
    } else if regimes.iter().all(|v| *v == "bear") {
        "bear"
    } else {
        "neutral"
    }
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
    use super::{combine_regimes, ema_last, htf_state_at, regime_of_pair, HtfRegimePoint};
    use chrono::{Duration, TimeZone, Utc};

    #[test]
    fn ema_computation_is_stable() {
        let values = [1.0, 2.0, 3.0];
        let ema = ema_last(&values, 2).expect("ema");
        assert!((ema - 2.555_555_555).abs() < 1e-6);
    }

    #[test]
    fn regime_classification_matches_cross() {
        assert_eq!(regime_of_pair(Some(101.0), Some(100.0)), "bull");
        assert_eq!(regime_of_pair(Some(99.0), Some(100.0)), "bear");
        assert_eq!(regime_of_pair(None, Some(100.0)), "neutral");
    }

    #[test]
    fn htf_state_lookup_forward_fills_between_closes() {
        let t0 = Utc
            .with_ymd_and_hms(2026, 3, 5, 0, 0, 0)
            .single()
            .expect("valid ts");
        let series = vec![
            HtfRegimePoint {
                close_time: t0,
                ema_fast: Some(101.0),
                ema_slow: Some(100.0),
                regime: "bull",
            },
            HtfRegimePoint {
                close_time: t0 + Duration::hours(4),
                ema_fast: Some(99.0),
                ema_slow: Some(100.0),
                regime: "bear",
            },
        ];

        let s1 = htf_state_at(&series, t0 + Duration::hours(2)).expect("state");
        assert_eq!(s1.regime, "bull");

        let s2 = htf_state_at(&series, t0 + Duration::hours(5)).expect("state");
        assert_eq!(s2.regime, "bear");
    }

    #[test]
    fn combine_regimes_requires_consensus() {
        assert_eq!(combine_regimes(&["bull", "bull"]), "bull");
        assert_eq!(combine_regimes(&["bear", "bear"]), "bear");
        assert_eq!(combine_regimes(&["bull", "bear"]), "neutral");
    }
}
