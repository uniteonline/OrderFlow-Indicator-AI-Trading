use serde_json::Value;

pub(crate) struct PreComputedVHelper;

impl PreComputedVHelper {
    pub(crate) fn compute_for_timeframe(
        indicators: &Value,
        timeframe: &str,
    ) -> Option<(f64, String)> {
        let base = format!("/kline_history/payload/intervals/{timeframe}");
        let bars = indicators
            .pointer(&format!("{base}/futures/bars"))
            .or_else(|| indicators.pointer(&format!("{base}/markets/futures/bars")))?
            .as_array()?;

        let mut closed_ranges: Vec<(String, f64)> = bars
            .iter()
            .filter(|bar| {
                bar.get("is_closed")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
            })
            .filter_map(|bar| {
                let high = bar.get("high").and_then(Value::as_f64)?;
                let low = bar.get("low").and_then(Value::as_f64)?;
                let open_time = bar
                    .get("open_time")
                    .and_then(Value::as_str)
                    .unwrap_or("?")
                    .to_string();
                Some((open_time, high - low))
            })
            .collect();

        if closed_ranges.len() < 3 {
            return None;
        }

        // Raw temp_indicator arrays are oldest -> newest; some downstream paths may already
        // be newest -> oldest. Sort explicitly so helper output is correct in either case.
        closed_ranges.sort_by(|a, b| b.0.cmp(&a.0));
        closed_ranges.truncate(5);

        let mut sorted: Vec<f64> = closed_ranges.iter().map(|(_, range)| *range).collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median = sorted[sorted.len() / 2];
        let bars_desc = closed_ranges
            .iter()
            .map(|(time, range)| format!("{}:{:.4}", time, range))
            .collect::<Vec<_>>()
            .join(",");
        let basis = format!(
            "tf={}; n={}; bars=[{}]; sorted=[{}]; median={:.4}",
            timeframe,
            closed_ranges.len(),
            bars_desc,
            sorted
                .iter()
                .map(|range| format!("{:.4}", range))
                .collect::<Vec<_>>()
                .join(","),
            median
        );
        Some((median, basis))
    }
}

#[cfg(test)]
mod tests {
    use super::PreComputedVHelper;
    use serde_json::json;

    #[test]
    fn compute_for_timeframe_uses_most_recent_closed_bars_from_oldest_first_series() {
        let indicators = json!({
            "kline_history": {
                "payload": {
                    "intervals": {
                        "4h": {
                            "futures": {
                                "bars": [
                                    {"open_time": "2026-03-13T00:00:00Z", "high": 110.0, "low": 100.0, "is_closed": true},
                                    {"open_time": "2026-03-13T04:00:00Z", "high": 118.73, "low": 100.0, "is_closed": true},
                                    {"open_time": "2026-03-13T08:00:00Z", "high": 122.0, "low": 100.0, "is_closed": true},
                                    {"open_time": "2026-03-13T12:00:00Z", "high": 117.0, "low": 100.0, "is_closed": true},
                                    {"open_time": "2026-03-13T16:00:00Z", "high": 121.0, "low": 100.0, "is_closed": true}
                                ]
                            }
                        }
                    }
                }
            }
        });

        let (value, basis) =
            PreComputedVHelper::compute_for_timeframe(&indicators, "4h").expect("compute 4h V");

        assert!((value - 18.73).abs() < 1e-9);
        assert!(basis.contains("2026-03-13T16:00:00Z"));
        assert!(basis.contains("median=18.7300"));
    }
}
