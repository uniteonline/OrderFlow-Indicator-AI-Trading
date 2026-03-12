use crate::indicators::context::IndicatorContext;
use crate::indicators::indicator_trait::Indicator;
use crate::indicators::shared::output_mapper::snapshot_only;
use chrono::{DateTime, Duration, TimeZone, Utc};
use serde_json::{json, Map, Value};

const EPS: f64 = 1e-12;

pub struct I20TpoMarketProfile;

impl Indicator for I20TpoMarketProfile {
    fn code(&self) -> &'static str {
        "tpo_market_profile"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> crate::indicators::context::IndicatorComputation {
        let mut by_session = Map::new();
        for session_code in &ctx.tpo_session_windows {
            let Some(session_minutes) = session_window_minutes(session_code) else {
                continue;
            };
            let session_start = floor_to_interval(ctx.ts_bucket, session_minutes);
            let session_end = session_start + Duration::minutes(session_minutes);
            let session_bars = collect_session_bars(ctx, session_start, session_end);

            let payload = build_session_payload(
                session_code,
                session_start,
                session_end,
                &session_bars,
                ctx.tpo_rows_nb,
                ctx.tpo_value_area_pct,
                ctx.tpo_ib_minutes,
                &ctx.tpo_dev_output_windows,
            );
            by_session.insert(session_code.clone(), payload);
        }

        let mut out = Map::new();
        out.insert("indicator".to_string(), json!(self.code()));
        out.insert("window".to_string(), json!("1m"));
        out.insert(
            "as_of_ts".to_string(),
            json!((ctx.ts_bucket + Duration::minutes(1)).to_rfc3339()),
        );
        out.insert("rows_nb".to_string(), json!(ctx.tpo_rows_nb));
        out.insert("value_area_pct".to_string(), json!(ctx.tpo_value_area_pct));
        merge_primary_session_fields(&mut out, &by_session, &ctx.tpo_session_windows);
        out.insert("by_session".to_string(), Value::Object(by_session));

        snapshot_only(self.code(), Value::Object(out))
    }
}

#[derive(Debug, Clone)]
struct MinuteRangeBar {
    ts_bucket: DateTime<Utc>,
    high: f64,
    low: f64,
}

#[derive(Debug, Clone)]
struct SessionProfile {
    p_min: f64,
    p_max: f64,
    bin_width: f64,
    scores: Vec<i64>,
    poc_idx: usize,
    vah_idx: usize,
    val_idx: usize,
}

fn collect_session_bars(
    ctx: &IndicatorContext,
    session_start: DateTime<Utc>,
    session_end: DateTime<Utc>,
) -> Vec<MinuteRangeBar> {
    ctx.history_futures
        .iter()
        .filter(|h| h.ts_bucket >= session_start && h.ts_bucket < session_end)
        .filter_map(|h| {
            let high = h
                .high_price
                .or(h.close_price)
                .or(h.last_price)
                .or(h.open_price)?;
            let low = h
                .low_price
                .or(h.close_price)
                .or(h.last_price)
                .or(h.open_price)?;
            Some(MinuteRangeBar {
                ts_bucket: h.ts_bucket,
                high,
                low,
            })
        })
        .collect()
}

fn build_session_payload(
    session_code: &str,
    session_start: DateTime<Utc>,
    session_end: DateTime<Utc>,
    bars: &[MinuteRangeBar],
    rows_nb: usize,
    value_area_pct: f64,
    ib_minutes: i64,
    dev_output_windows: &[String],
) -> Value {
    let Some(profile) = build_profile(bars, rows_nb.max(1), value_area_pct) else {
        return json!({
            "session_window": session_code,
            "session_start": session_start.to_rfc3339(),
            "session_end": session_end.to_rfc3339(),
            "rows_nb": rows_nb,
            "value_area_pct": value_area_pct,
            "tpo_poc": null,
            "tpo_vah": null,
            "tpo_val": null,
            "initial_balance_high": null,
            "initial_balance_low": null,
            "tpo_single_print_zones": [],
            "dev_series": {},
        });
    };

    let (ib_high, ib_low) = initial_balance(bars, session_start, ib_minutes);

    let single_print_zones = single_print_zones(&profile)
        .into_iter()
        .map(|(start_idx, end_idx)| {
            json!({
                "low": bin_low(&profile, start_idx),
                "high": bin_high(&profile, end_idx),
                "score": 1,
            })
        })
        .collect::<Vec<_>>();

    let mut dev_series = Map::new();
    for out_code in dev_output_windows {
        let Some(out_minutes) = output_window_minutes(out_code) else {
            continue;
        };
        let series = developing_profile_series(
            bars,
            session_start,
            out_minutes,
            rows_nb.max(1),
            value_area_pct,
        );
        dev_series.insert(out_code.clone(), Value::Array(series));
    }

    json!({
        "session_window": session_code,
        "session_start": session_start.to_rfc3339(),
        "session_end": session_end.to_rfc3339(),
        "rows_nb": rows_nb,
        "value_area_pct": value_area_pct,
        "tpo_poc": bin_center(&profile, profile.poc_idx),
        "tpo_vah": bin_high(&profile, profile.vah_idx),
        "tpo_val": bin_low(&profile, profile.val_idx),
        "initial_balance_high": ib_high,
        "initial_balance_low": ib_low,
        "tpo_single_print_zones": single_print_zones,
        "dev_series": dev_series,
    })
}

fn build_profile(
    bars: &[MinuteRangeBar],
    rows_nb: usize,
    value_area_pct: f64,
) -> Option<SessionProfile> {
    if bars.is_empty() {
        return None;
    }

    let p_min = bars.iter().map(|b| b.low).fold(f64::INFINITY, f64::min);
    let p_max = bars
        .iter()
        .map(|b| b.high)
        .fold(f64::NEG_INFINITY, f64::max);

    if !p_min.is_finite() || !p_max.is_finite() {
        return None;
    }

    let bin_width = (p_max - p_min) / rows_nb as f64;
    if !bin_width.is_finite() {
        return None;
    }
    let mut scores = vec![0_i64; rows_nb];

    for bar in bars {
        let from_idx = price_to_bin_idx(bar.low, p_min, bin_width, rows_nb);
        let to_idx = price_to_bin_idx(bar.high, p_min, bin_width, rows_nb);
        for idx in from_idx.min(to_idx)..=from_idx.max(to_idx) {
            scores[idx] += 1;
        }
    }

    let poc_idx = scores
        .iter()
        .enumerate()
        .max_by(|a, b| a.1.cmp(b.1).then_with(|| b.0.cmp(&a.0)))
        .map(|(idx, _)| idx)?;

    let total_score = scores.iter().sum::<i64>() as f64;
    let target = (total_score * value_area_pct.clamp(0.0, 1.0)).max(1.0);

    let mut in_va = vec![false; rows_nb];
    in_va[poc_idx] = true;
    let mut acc = scores[poc_idx] as f64;
    let mut left = poc_idx as i64 - 1;
    let mut right = poc_idx + 1;

    while acc + EPS < target && (left >= 0 || right < rows_nb) {
        let lv = if left >= 0 {
            scores[left as usize] as f64
        } else {
            -1.0
        };
        let rv = if right < rows_nb {
            scores[right] as f64
        } else {
            -1.0
        };

        if rv >= lv && right < rows_nb {
            in_va[right] = true;
            acc += rv.max(0.0);
            right += 1;
        } else if left >= 0 {
            in_va[left as usize] = true;
            acc += lv.max(0.0);
            left -= 1;
        } else {
            break;
        }
    }

    let val_idx = in_va.iter().position(|v| *v).unwrap_or(poc_idx);
    let vah_idx = in_va.iter().rposition(|v| *v).unwrap_or(poc_idx);

    Some(SessionProfile {
        p_min,
        p_max,
        bin_width,
        scores,
        poc_idx,
        vah_idx,
        val_idx,
    })
}

fn initial_balance(
    bars: &[MinuteRangeBar],
    session_start: DateTime<Utc>,
    ib_minutes: i64,
) -> (Option<f64>, Option<f64>) {
    let end_ts = session_start + Duration::minutes(ib_minutes.max(1));
    let mut highs = Vec::new();
    let mut lows = Vec::new();
    for bar in bars {
        if bar.ts_bucket >= session_start && bar.ts_bucket < end_ts {
            highs.push(bar.high);
            lows.push(bar.low);
        }
    }
    (
        highs.into_iter().reduce(f64::max),
        lows.into_iter().reduce(f64::min),
    )
}

fn developing_profile_series(
    bars: &[MinuteRangeBar],
    session_start: DateTime<Utc>,
    out_minutes: i64,
    rows_nb: usize,
    value_area_pct: f64,
) -> Vec<Value> {
    if bars.is_empty() {
        return Vec::new();
    }

    let mut out = Vec::new();
    for (idx, bar) in bars.iter().enumerate() {
        let anchor = bar.ts_bucket + Duration::minutes(1);
        if anchor <= session_start {
            continue;
        }
        if anchor.timestamp().rem_euclid(out_minutes * 60) != 0 {
            continue;
        }

        let upto = &bars[..=idx];
        let Some(profile) = build_profile(upto, rows_nb, value_area_pct) else {
            continue;
        };
        out.push(json!({
            "ts": anchor.to_rfc3339(),
            "tpo_dev_poc": bin_center(&profile, profile.poc_idx),
            "tpo_dev_vah": bin_high(&profile, profile.vah_idx),
            "tpo_dev_val": bin_low(&profile, profile.val_idx),
        }));
    }

    out
}

fn single_print_zones(profile: &SessionProfile) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut start: Option<usize> = None;

    for (idx, score) in profile.scores.iter().enumerate() {
        if *score == 1 {
            if start.is_none() {
                start = Some(idx);
            }
        } else if let Some(s) = start.take() {
            out.push((s, idx - 1));
        }
    }

    if let Some(s) = start {
        out.push((s, profile.scores.len().saturating_sub(1)));
    }

    out
}

fn merge_primary_session_fields(
    out: &mut Map<String, Value>,
    by_session: &Map<String, Value>,
    session_preference: &[String],
) {
    let primary = session_preference
        .iter()
        .find_map(|code| by_session.get(code))
        .or_else(|| by_session.values().next());
    let Some(primary_obj) = primary.and_then(Value::as_object) else {
        return;
    };
    for key in [
        "session_window",
        "session_start",
        "session_end",
        "tpo_poc",
        "tpo_vah",
        "tpo_val",
        "initial_balance_high",
        "initial_balance_low",
        "tpo_single_print_zones",
        "dev_series",
    ] {
        if let Some(value) = primary_obj.get(key) {
            out.insert(key.to_string(), value.clone());
        }
    }
}

fn price_to_bin_idx(price: f64, p_min: f64, bin_width: f64, rows_nb: usize) -> usize {
    if rows_nb <= 1 || bin_width <= EPS || !bin_width.is_finite() {
        return 0;
    }
    let raw = ((price - p_min) / bin_width).floor();
    if !raw.is_finite() {
        return 0;
    }
    raw.max(0.0).min((rows_nb - 1) as f64) as usize
}

fn bin_center(profile: &SessionProfile, idx: usize) -> f64 {
    profile.p_min + (idx as f64 + 0.5) * profile.bin_width
}

fn bin_low(profile: &SessionProfile, idx: usize) -> f64 {
    profile.p_min + idx as f64 * profile.bin_width
}

fn bin_high(profile: &SessionProfile, idx: usize) -> f64 {
    profile.p_min + (idx as f64 + 1.0) * profile.bin_width
}

fn floor_to_interval(ts: DateTime<Utc>, interval_minutes: i64) -> DateTime<Utc> {
    let secs = interval_minutes * 60;
    let aligned = ts.timestamp().div_euclid(secs) * secs;
    Utc.timestamp_opt(aligned, 0).single().unwrap_or(ts)
}

fn session_window_minutes(code: &str) -> Option<i64> {
    match code {
        "4h" => Some(240),
        "1d" => Some(1440),
        _ => None,
    }
}

fn output_window_minutes(code: &str) -> Option<i64> {
    match code {
        "15m" => Some(15),
        "1h" => Some(60),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{build_profile, merge_primary_session_fields, MinuteRangeBar};
    use chrono::{TimeZone, Utc};
    use serde_json::{json, Map, Value};

    #[test]
    fn value_area_covers_majority_around_poc() {
        let ts0 = Utc
            .with_ymd_and_hms(2026, 3, 5, 0, 0, 0)
            .single()
            .expect("valid ts");
        let bars = vec![
            MinuteRangeBar {
                ts_bucket: ts0,
                high: 101.0,
                low: 100.0,
            },
            MinuteRangeBar {
                ts_bucket: ts0,
                high: 101.0,
                low: 100.0,
            },
            MinuteRangeBar {
                ts_bucket: ts0,
                high: 102.0,
                low: 101.0,
            },
        ];

        let profile = build_profile(&bars, 8, 0.70).expect("profile");
        assert!(profile.vah_idx >= profile.poc_idx);
        assert!(profile.val_idx <= profile.poc_idx);
        assert!(profile.p_max > profile.p_min);
    }

    #[test]
    fn zero_range_profile_uses_exact_zero_bin_width() {
        let ts0 = Utc
            .with_ymd_and_hms(2026, 3, 5, 0, 0, 0)
            .single()
            .expect("valid ts");
        let bars = vec![
            MinuteRangeBar {
                ts_bucket: ts0,
                high: 100.0,
                low: 100.0,
            },
            MinuteRangeBar {
                ts_bucket: ts0 + chrono::Duration::minutes(1),
                high: 100.0,
                low: 100.0,
            },
        ];

        let profile = build_profile(&bars, 64, 0.70).expect("profile");
        assert_eq!(profile.bin_width, 0.0);
        assert_eq!(profile.poc_idx, 0);
        assert_eq!(profile.vah_idx, 0);
        assert_eq!(profile.val_idx, 0);
    }

    #[test]
    fn primary_session_is_flattened_to_top_level() {
        let mut by_session = Map::new();
        by_session.insert(
            "4h".to_string(),
            json!({
                "session_window": "4h",
                "session_start": "2026-03-05T00:00:00Z",
                "session_end": "2026-03-05T04:00:00Z",
                "tpo_poc": 101.0,
                "tpo_vah": 102.0,
                "tpo_val": 100.0,
                "initial_balance_high": 101.5,
                "initial_balance_low": 100.5,
                "tpo_single_print_zones": [],
                "dev_series": [],
            }),
        );
        let mut out = Map::<String, Value>::new();
        merge_primary_session_fields(&mut out, &by_session, &["4h".to_string()]);
        assert_eq!(
            out.get("session_window").and_then(Value::as_str),
            Some("4h")
        );
        assert_eq!(out.get("tpo_poc").and_then(Value::as_f64), Some(101.0));
    }
}
