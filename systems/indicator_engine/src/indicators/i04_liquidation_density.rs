use crate::indicators::context::{
    clip01, IndicatorComputation, IndicatorContext, IndicatorSnapshotRow, LiquidationLevelRow,
};
use crate::indicators::indicator_trait::Indicator;
use crate::runtime::state_store::{tick_to_price, LiqAgg, MinuteHistory};
use chrono::Duration;
use serde_json::{json, Map, Value};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};

const LONG_PEAK_PROMINENCE_FRACTION: f64 = 0.03;
const SHORT_PEAK_PROMINENCE_FRACTION: f64 = 0.03;
const WINDOW_SPECS: [(&str, i64); 5] = [
    ("15m", 15),
    ("1h", 60),
    ("4h", 240),
    ("1d", 1440),
    ("3d", 4320),
];
const RAW_AUDIT_TOP_LONG_LEVELS: usize = 16;
const RAW_AUDIT_TOP_SHORT_LEVELS: usize = 16;
const RAW_AUDIT_TOP_ABS_NET_LEVELS: usize = 24;

pub struct I04LiquidationDensity;

impl Indicator for I04LiquidationDensity {
    fn code(&self) -> &'static str {
        "liquidation_density"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation {
        let (current_payload, current_rows) = build_window_view("1m", &ctx.futures.force_liq, 1, 1);

        let mut by_window = Map::new();
        let mut liquidation_rows = current_rows;
        for (window_code, window_minutes) in WINDOW_SPECS {
            let minute_rows = window_rows(&ctx.history_futures, ctx.ts_bucket, window_minutes);
            let agg = aggregate_force_liq(&minute_rows);
            let (window_payload, window_rows) =
                build_window_view(window_code, &agg, window_minutes, minute_rows.len() as i64);
            by_window.insert(window_code.to_string(), window_payload);
            liquidation_rows.extend(window_rows);
        }

        let mut payload = current_payload;
        if let Some(obj) = payload.as_object_mut() {
            obj.insert("by_window".to_string(), Value::Object(by_window));
        }

        IndicatorComputation {
            snapshot: Some(IndicatorSnapshotRow {
                indicator_code: self.code(),
                window_code: "1m",
                payload_json: payload,
            }),
            liquidation_rows,
            ..Default::default()
        }
    }
}

fn build_window_view(
    window_code: &'static str,
    force_liq: &BTreeMap<i64, LiqAgg>,
    requested_minutes: i64,
    returned_minutes: i64,
) -> (Value, Vec<LiquidationLevelRow>) {
    let mut rows = force_liq
        .iter()
        .map(|(tick, agg)| {
            let long = agg.long_liq;
            let short = agg.short_liq;
            let net = short - long;
            (*tick, long, short, net)
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|(tick, _, _, _)| *tick);

    let total_long = rows.iter().map(|(_, long, _, _)| *long).sum::<f64>();
    let total_short = rows.iter().map(|(_, _, short, _)| *short).sum::<f64>();
    let long_peak_threshold = total_long * LONG_PEAK_PROMINENCE_FRACTION;
    let short_peak_threshold = total_short * SHORT_PEAK_PROMINENCE_FRACTION;

    let top_long = rows
        .iter()
        .map(|(_, long, _, _)| *long)
        .fold(0.0_f64, |a, b| a.max(b));
    let top_short = rows
        .iter()
        .map(|(_, _, short, _)| *short)
        .fold(0.0_f64, |a, b| a.max(b));

    let long_peaks = detect_local_peaks(&rows, true, long_peak_threshold);
    let short_peaks = detect_local_peaks(&rows, false, short_peak_threshold);

    let raw_audit_ticks = select_liquidation_audit_ticks(&rows, &long_peaks, &short_peaks);
    let liq_rows = rows
        .iter()
        .filter(|(tick, _, _, _)| raw_audit_ticks.contains(tick))
        .map(|(tick, long, short, net)| LiquidationLevelRow {
            window_code,
            price_level: tick_to_price(*tick),
            long_liq_density: *long,
            short_liq_density: *short,
            net_liq_density: *net,
            is_long_peak: long_peaks.contains(tick),
            is_short_peak: short_peaks.contains(tick),
            long_peak_score: if top_long > 0.0 {
                Some(*long / top_long)
            } else {
                None
            },
            short_peak_score: if top_short > 0.0 {
                Some(*short / top_short)
            } else {
                None
            },
            extra_json: json!({
                "audit_capture_policy": "peak_intensity_subset"
            }),
        })
        .collect::<Vec<_>>();

    let peaks = rows
        .iter()
        .filter(|(tick, _, _, _)| long_peaks.contains(tick) || short_peaks.contains(tick))
        .map(|(tick, long, short, net)| {
            json!({
                "price": tick_to_price(*tick),
                "long": *long,
                "short": *short,
                "net": *net,
                "is_long_peak": long_peaks.contains(tick),
                "is_short_peak": short_peaks.contains(tick),
                "long_peak_score": if top_long > 0.0 { Some(*long / top_long) } else { None },
                "short_peak_score": if top_short > 0.0 { Some(*short / top_short) } else { None },
            })
        })
        .collect::<Vec<_>>();

    (
        json!({
            "requested_minutes": requested_minutes,
            "returned_minutes": returned_minutes,
            "missing_minutes": requested_minutes.saturating_sub(returned_minutes),
            "coverage_ratio": clip01(returned_minutes as f64 / requested_minutes.max(1) as f64),
            "is_ready": returned_minutes >= requested_minutes,
            "levels_count": rows.len(),
            "peak_levels": peaks,
            "long_total": total_long,
            "short_total": total_short,
        }),
        liq_rows,
    )
}

fn select_liquidation_audit_ticks(
    rows: &[(i64, f64, f64, f64)],
    long_peaks: &HashSet<i64>,
    short_peaks: &HashSet<i64>,
) -> HashSet<i64> {
    let mut out = HashSet::new();
    out.extend(long_peaks.iter().copied());
    out.extend(short_peaks.iter().copied());

    let mut by_long = rows
        .iter()
        .map(|(tick, long, _, _)| (*tick, *long))
        .collect::<Vec<_>>();
    by_long.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    for (tick, _) in by_long.into_iter().take(RAW_AUDIT_TOP_LONG_LEVELS) {
        out.insert(tick);
    }

    let mut by_short = rows
        .iter()
        .map(|(tick, _, short, _)| (*tick, *short))
        .collect::<Vec<_>>();
    by_short.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    for (tick, _) in by_short.into_iter().take(RAW_AUDIT_TOP_SHORT_LEVELS) {
        out.insert(tick);
    }

    let mut by_abs_net = rows
        .iter()
        .map(|(tick, _, _, net)| (*tick, net.abs()))
        .collect::<Vec<_>>();
    by_abs_net.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    for (tick, _) in by_abs_net.into_iter().take(RAW_AUDIT_TOP_ABS_NET_LEVELS) {
        out.insert(tick);
    }

    out
}

fn window_rows<'a>(
    history: &'a [MinuteHistory],
    ts_bucket: chrono::DateTime<chrono::Utc>,
    window_minutes: i64,
) -> Vec<&'a MinuteHistory> {
    let start = ts_bucket - Duration::minutes(window_minutes);
    history
        .iter()
        .filter(|row| row.ts_bucket > start && row.ts_bucket <= ts_bucket)
        .collect()
}

fn aggregate_force_liq(rows: &[&MinuteHistory]) -> BTreeMap<i64, LiqAgg> {
    let mut out = BTreeMap::new();
    for row in rows {
        for (tick, agg) in &row.force_liq {
            let dst = out.entry(*tick).or_insert_with(LiqAgg::default);
            dst.long_liq += agg.long_liq;
            dst.short_liq += agg.short_liq;
        }
    }
    out
}

fn detect_local_peaks(
    rows: &[(i64, f64, f64, f64)],
    long_side: bool,
    prominence_threshold: f64,
) -> HashSet<i64> {
    let mut out = HashSet::new();
    if rows.len() < 3 {
        return out;
    }

    let values = rows
        .iter()
        .map(|(_, long, short, _)| if long_side { *long } else { *short })
        .collect::<Vec<_>>();

    for i in 1..rows.len().saturating_sub(1) {
        let prev_tick = rows[i - 1].0;
        let cur_tick = rows[i].0;
        let next_tick = rows[i + 1].0;
        if prev_tick + 1 != cur_tick || cur_tick + 1 != next_tick {
            continue;
        }

        let prev = values[i - 1];
        let cur = values[i];
        let next = values[i + 1];
        if cur <= 0.0 {
            continue;
        }
        if cur > prev && cur > next && prominence_max(i, &values) >= prominence_threshold.max(0.0) {
            out.insert(cur_tick);
        }
    }
    out
}

fn prominence_max(idx: usize, vals: &[f64]) -> f64 {
    let v = vals[idx];

    let left_col = {
        let mut min_so_far = v;
        let mut col = 0.0_f64;
        for j in (0..idx).rev() {
            if vals[j] < min_so_far {
                min_so_far = vals[j];
            }
            if vals[j] > v {
                col = min_so_far;
                break;
            }
        }
        col
    };

    let right_col = {
        let mut min_so_far = v;
        let mut col = 0.0_f64;
        for j in (idx + 1)..vals.len() {
            if vals[j] < min_so_far {
                min_so_far = vals[j];
            }
            if vals[j] > v {
                col = min_so_far;
                break;
            }
        }
        col
    };

    (v - left_col.max(right_col)).max(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn liquidation_raw_audit_ticks_are_bounded() {
        let rows = (1..=200_i64)
            .map(|tick| {
                (
                    tick,
                    (tick % 17 + 1) as f64,
                    (201 - tick) as f64,
                    (201 - tick) as f64 - (tick % 17 + 1) as f64,
                )
            })
            .collect::<Vec<_>>();
        let long_peaks = HashSet::from([7_i64, 101_i64]);
        let short_peaks = HashSet::from([12_i64, 199_i64]);

        let ticks = select_liquidation_audit_ticks(&rows, &long_peaks, &short_peaks);
        assert!(ticks.contains(&7));
        assert!(ticks.contains(&199));
        assert!(
            ticks.len()
                <= RAW_AUDIT_TOP_LONG_LEVELS
                    + RAW_AUDIT_TOP_SHORT_LEVELS
                    + RAW_AUDIT_TOP_ABS_NET_LEVELS
                    + long_peaks.len()
                    + short_peaks.len()
        );
    }
}
