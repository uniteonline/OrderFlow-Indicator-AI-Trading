pub mod core;
pub(crate) mod core_entry;
pub(crate) mod core_management;
pub(crate) mod core_pending;
pub(crate) mod core_shared;
pub mod scan;

use crate::llm::provider::ModelInvocationInput;
use anyhow::{Context, Result};
use serde_json::{Number, Value};

// Preserve full precision for indicators whose sub-1.00 metrics or scores would
// materially change trend/entry/tp/sl interpretation if rounded to 2 decimals.
const PRECISION_PRESERVED_INDICATOR_CODES: &[&str] = &[
    "absorption",
    "bearish_absorption",
    "bearish_initiation",
    "bullish_absorption",
    "bullish_initiation",
    "buying_exhaustion",
    "cvd_pack",
    "divergence",
    "footprint",
    "funding_rate",
    "fvg",
    "high_volume_pulse",
    "initiation",
    "liquidation_density",
    "orderbook_depth",
    "price_volume_structure",
    "rvwap_sigma_bands",
    "selling_exhaustion",
    "vpin",
    "whale_trades",
];

const DIRECT_TIMESERIES_ARRAY_FIELDS: &[&str] = &[
    "active_bear_fvgs",
    "active_bull_fvgs",
    "bars",
    "candidates",
    "changes",
    "events",
    "fvgs",
    "series",
];

pub(crate) struct TempIndicatorInputOptimizer;

impl TempIndicatorInputOptimizer {
    pub(crate) fn build_raw_value(input: &ModelInvocationInput) -> Result<Value> {
        let mut value =
            serde_json::to_value(input).context("serialize llm invocation input value")?;
        Self::normalize_raw_temp_indicator(&mut value);
        Ok(value)
    }

    pub(crate) fn normalize_raw_temp_indicator(value: &mut Value) {
        reverse_timeseries_newest_first(value);
        round_non_whitelisted_floats_in_place(value);
    }

    pub(crate) fn round_derived_fields(value: &mut Value) {
        round_non_whitelisted_floats_in_place(value);
    }
}

pub(crate) fn serialize_prompt_value(mut value: Value, context: &'static str) -> Result<String> {
    TempIndicatorInputOptimizer::round_derived_fields(&mut value);
    serde_json::to_string(&value).context(context)
}

pub(crate) fn reverse_timeseries_newest_first(value: &mut Value) {
    let mut path = Vec::new();
    reverse_timeseries_arrays_with_path(value, &mut path);
}

fn reverse_timeseries_arrays_with_path(value: &mut Value, path: &mut Vec<String>) {
    match value {
        Value::Array(items) => {
            if should_reverse_array(path) {
                items.reverse();
            }
            for (idx, item) in items.iter_mut().enumerate() {
                path.push(idx.to_string());
                reverse_timeseries_arrays_with_path(item, path);
                path.pop();
            }
        }
        Value::Object(map) => {
            for (key, item) in map.iter_mut() {
                path.push(key.clone());
                reverse_timeseries_arrays_with_path(item, path);
                path.pop();
            }
        }
        _ => {}
    }
}

fn should_reverse_array(path: &[String]) -> bool {
    if path.len() < 3 || path.first().map(String::as_str) != Some("indicators") {
        return false;
    }

    let Some(field_name) = path.last().map(String::as_str) else {
        return false;
    };

    if DIRECT_TIMESERIES_ARRAY_FIELDS.contains(&field_name) {
        return true;
    }

    if field_name == "recent_7d"
        && matches!(
            path.get(1).map(String::as_str),
            Some("funding_rate" | "liquidation_density")
        )
    {
        return true;
    }

    path.windows(2).any(|window| {
        matches!(
            window,
            [parent, _]
                if parent == "series_by_window"
                    || parent == "series_by_output_window"
                    || parent == "ffill_series_by_output_window"
                    || parent == "dev_series"
        )
    })
}

pub(crate) fn round_non_whitelisted_floats_in_place(value: &mut Value) {
    let mut path = Vec::new();
    round_non_whitelisted_floats_with_path(value, &mut path);
}

fn round_non_whitelisted_floats_with_path(value: &mut Value, path: &mut Vec<String>) {
    match value {
        Value::Array(items) => {
            for (idx, item) in items.iter_mut().enumerate() {
                path.push(idx.to_string());
                round_non_whitelisted_floats_with_path(item, path);
                path.pop();
            }
        }
        Value::Object(map) => {
            for (key, item) in map.iter_mut() {
                path.push(key.clone());
                round_non_whitelisted_floats_with_path(item, path);
                path.pop();
            }
        }
        Value::Number(number) if number.is_f64() => {
            if should_preserve_float_precision(path) {
                return;
            }
            if let Some(v) = number.as_f64() {
                let rounded = (v * 100.0).round() / 100.0;
                if let Some(rebuilt) = Number::from_f64(rounded) {
                    *value = Value::Number(rebuilt);
                }
            }
        }
        _ => {}
    }
}

fn should_preserve_float_precision(path: &[String]) -> bool {
    // Price fields are always rounded even within whitelisted indicators
    if let Some(leaf) = path.last() {
        if leaf.contains("price") {
            return false;
        }
    }
    matches!(
        path,
        [root, indicator, ..]
            if root == "indicators"
                && PRECISION_PRESERVED_INDICATOR_CODES.contains(&indicator.as_str())
    )
}

#[cfg(test)]
mod tests {
    use super::TempIndicatorInputOptimizer;
    use serde_json::json;

    #[test]
    fn optimizer_reverses_known_timeseries_and_rounds_non_whitelisted_indicators() {
        let mut value = json!({
            "indicators": {
                "avwap": {
                    "payload": {
                        "fut_mark_price": 2077.27946512,
                        "series_by_window": {
                            "15m": [
                                {"ts": "2026-03-07T07:00:00+00:00", "avwap_fut": 1971.1180703628977},
                                {"ts": "2026-03-07T07:15:00+00:00", "avwap_fut": 1970.476951629652}
                            ]
                        }
                    }
                },
                "funding_rate": {
                    "payload": {
                        "funding_current": -0.00004527,
                        "by_window": {
                            "15m": {
                                "changes": [
                                    {"change_ts": "2026-03-07T07:00:00+00:00", "funding_delta": 0.00000034},
                                    {"change_ts": "2026-03-07T07:15:00+00:00", "funding_delta": -0.0000002}
                                ]
                            }
                        }
                    }
                },
                "absorption": {
                    "payload": {
                        "recent_7d": {
                            "events": [
                                {"event_end_ts": "2026-03-07T07:00:00+00:00", "score": 0.6971667409666932},
                                {"event_end_ts": "2026-03-07T07:15:00+00:00", "score": 0.5521759390122744}
                            ]
                        }
                    }
                }
            }
        });

        TempIndicatorInputOptimizer::normalize_raw_temp_indicator(&mut value);

        assert_eq!(
            value.pointer("/indicators/avwap/payload/fut_mark_price"),
            Some(&json!(2077.28))
        );
        assert_eq!(
            value
                .pointer("/indicators/avwap/payload/series_by_window/15m/0/ts")
                .and_then(|v| v.as_str()),
            Some("2026-03-07T07:15:00+00:00")
        );
        assert_eq!(
            value.pointer("/indicators/funding_rate/payload/funding_current"),
            Some(&json!(-0.00004527))
        );
        assert_eq!(
            value.pointer("/indicators/funding_rate/payload/by_window/15m/changes/0/funding_delta"),
            Some(&json!(-0.0000002))
        );
        assert_eq!(
            value.pointer("/indicators/absorption/payload/recent_7d/events/0/score"),
            Some(&json!(0.5521759390122744_f64))
        );
    }
}
