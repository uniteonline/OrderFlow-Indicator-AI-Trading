use crate::indicators::context::{IndicatorComputation, IndicatorContext, IndicatorSnapshotRow};
use crate::indicators::indicator_trait::Indicator;
use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Value};

const WINDOWS: [(&str, i64); 5] = [
    ("15m", 15),
    ("1h", 60),
    ("4h", 240),
    ("1d", 1440),
    ("3d", 4320),
];

pub struct I16FundingRate;

impl Indicator for I16FundingRate {
    fn code(&self) -> &'static str {
        "funding_rate"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation {
        let mut by_window = serde_json::Map::new();
        for (label, mins) in WINDOWS {
            by_window.insert(label.to_string(), compute_window_metrics(ctx, mins, label));
        }

        let end = ctx.ts_bucket + Duration::minutes(1);
        let funding_current_pair = ctx.latest_funding_pair_at_or_before(end);
        let funding_current = funding_current_pair.map(|(_, v)| v);
        let funding_current_effective_ts = funding_current_pair.map(|(ts, _)| ts.to_rfc3339());
        let funding_twa = ctx.funding_twa_1m().or(funding_current);
        let mark_price_last_pair = ctx.latest_mark_pair_at_or_before(end);
        let mark_price_last = mark_price_last_pair.map(|(_, v)| v);
        let mark_price_last_ts = mark_price_last_pair.map(|(ts, _)| ts.to_rfc3339());
        let mark_price_twap = ctx.mark_twap_1m().or(mark_price_last);
        let mut changes_sorted = ctx.funding_changes_in_window.iter().collect::<Vec<_>>();
        changes_sorted.sort_by_key(|c| c.ts_change);
        let changes = changes_sorted
            .iter()
            .map(|c| {
                json!({
                    "change_ts": c.ts_change.to_rfc3339(),
                    "funding_prev": c.prev,
                    "funding_new": c.new,
                    "funding_delta": c.delta,
                    "mark_price_at_change": c.mark_price_at_change
                })
            })
            .collect::<Vec<_>>();

        IndicatorComputation {
            snapshot: Some(IndicatorSnapshotRow {
                indicator_code: self.code(),
                window_code: "1m",
                payload_json: json!({
                    "funding_current": funding_current,
                    "funding_current_effective_ts": funding_current_effective_ts,
                    "funding_twa": funding_twa,
                    "mark_price_last": mark_price_last,
                    "mark_price_last_ts": mark_price_last_ts,
                    "mark_price_twap": mark_price_twap,
                    "change_count": changes.len(),
                    "changes": changes,
                    "by_window": Value::Object(by_window)
                }),
            }),
            ..Default::default()
        }
    }
}

fn compute_window_metrics(ctx: &IndicatorContext, mins: i64, label: &str) -> Value {
    let end = ctx.ts_bucket + Duration::minutes(1);
    let start = end - Duration::minutes(mins);

    let mut funding_points = ctx
        .funding_points_recent
        .iter()
        .map(|p| (p.ts, p.funding_rate))
        .collect::<Vec<_>>();
    funding_points.sort_by_key(|(ts, _)| *ts);

    let mut mark_points = ctx
        .mark_points_recent
        .iter()
        .filter_map(|p| p.mark_price.map(|v| (p.ts, v)))
        .collect::<Vec<_>>();
    mark_points.sort_by_key(|(ts, _)| *ts);

    let funding_current = funding_points
        .iter()
        .rev()
        .find(|(ts, _)| *ts <= end)
        .map(|(_, v)| *v);
    let funding_current_effective_ts = funding_points
        .iter()
        .rev()
        .find(|(ts, _)| *ts <= end)
        .map(|(ts, _)| ts.to_rfc3339());
    let funding_fallback = funding_points
        .iter()
        .rev()
        .find(|(ts, _)| *ts <= start)
        .map(|(_, v)| *v)
        .or(funding_current);
    let funding_twa = time_weighted_avg(start, end, &funding_points, funding_fallback);

    let mark_last_pair = mark_points.iter().rev().find(|(ts, _)| *ts <= end).copied();
    let mark_price_last = mark_last_pair.map(|(_, v)| v);
    let mark_price_last_ts = mark_last_pair.map(|(ts, _)| ts.to_rfc3339());
    let mark_fallback = mark_points
        .iter()
        .rev()
        .find(|(ts, _)| *ts <= start)
        .map(|(_, v)| *v)
        .or(mark_price_last);
    let mark_price_twap = time_weighted_avg(start, end, &mark_points, mark_fallback);

    let mut changes_in_win = ctx
        .funding_changes_recent
        .iter()
        .filter(|c| c.ts_change >= start && c.ts_change < end)
        .collect::<Vec<_>>();
    changes_in_win.sort_by_key(|c| c.ts_change);
    let changes = changes_in_win
        .iter()
        .map(|c| {
            json!({
                "change_ts": c.ts_change.to_rfc3339(),
                "funding_prev": c.prev,
                "funding_new": c.new,
                "funding_delta": c.delta,
                "mark_price_at_change": c.mark_price_at_change
            })
        })
        .collect::<Vec<_>>();

    json!({
        "window": label,
        "funding_current": funding_current,
        "funding_current_effective_ts": funding_current_effective_ts,
        "funding_twa": funding_twa,
        "mark_price_last": mark_price_last,
        "mark_price_last_ts": mark_price_last_ts,
        "mark_price_twap": mark_price_twap,
        "change_count": changes.len(),
        "changes": changes
    })
}

fn time_weighted_avg(
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    points: &[(DateTime<Utc>, f64)],
    fallback: Option<f64>,
) -> Option<f64> {
    if end <= start {
        return None;
    }
    if points.is_empty() {
        return fallback;
    }

    let mut weighted = 0.0;
    let mut total = 0.0;
    let mut cursor = start;
    let mut last_val = fallback.unwrap_or(points[0].1);

    for (ts, v) in points {
        if *ts <= start {
            last_val = *v;
            continue;
        }
        if *ts > end {
            break;
        }
        let dt = (*ts - cursor).num_milliseconds().max(0) as f64 / 1000.0;
        if dt > 0.0 {
            weighted += last_val * dt;
            total += dt;
        }
        cursor = *ts;
        last_val = *v;
    }

    if cursor < end {
        let dt = (end - cursor).num_milliseconds().max(0) as f64 / 1000.0;
        weighted += last_val * dt;
        total += dt;
    }

    if total <= 0.0 {
        fallback.or(Some(last_val))
    } else {
        Some(weighted / total)
    }
}
