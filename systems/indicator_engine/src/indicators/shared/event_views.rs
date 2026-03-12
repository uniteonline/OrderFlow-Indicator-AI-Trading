use chrono::{DateTime, Duration, Utc};
use serde_json::{Map, Value};
use std::collections::HashSet;

pub struct EventWindowView {
    pub current_events: Vec<Value>,
    pub recent_events: Vec<Value>,
    pub latest_current: Option<Value>,
    pub latest_recent: Option<Value>,
}

pub fn build_event_window_view(
    ts_bucket: DateTime<Utc>,
    mut events: Vec<(DateTime<Utc>, Value)>,
) -> EventWindowView {
    events.sort_by_key(|(available_ts, _)| *available_ts);

    let current_available_ts = ts_bucket + Duration::minutes(1);
    let recent_cutoff = current_available_ts - Duration::days(7);

    let mut seen_ids = HashSet::new();
    let mut deduped = Vec::with_capacity(events.len());
    for (available_ts, event) in events {
        let event_id = event
            .get("event_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        if let Some(ref event_id) = event_id {
            if !seen_ids.insert(event_id.clone()) {
                continue;
            }
        }
        deduped.push((available_ts, event));
    }

    let current_events = deduped
        .iter()
        .filter(|(available_ts, _)| *available_ts == current_available_ts)
        .map(|(_, event)| event.clone())
        .collect::<Vec<_>>();
    let recent_events = deduped
        .iter()
        .filter(|(available_ts, _)| *available_ts >= recent_cutoff)
        .map(|(_, event)| event.clone())
        .collect::<Vec<_>>();

    EventWindowView {
        latest_current: current_events.last().cloned(),
        latest_recent: recent_events.last().cloned(),
        current_events,
        recent_events,
    }
}

pub fn merge_payload_fields(mut base: Map<String, Value>, payload: &Value) -> Value {
    if let Some(obj) = payload.as_object() {
        for (key, value) in obj {
            base.entry(key.clone()).or_insert_with(|| value.clone());
        }
    }
    Value::Object(base)
}

pub fn build_recent_7d_payload(
    events: Vec<Value>,
    lookback_covered_minutes: i64,
    history_source: &str,
) -> Value {
    let lookback_requested_minutes = 7 * 24 * 60;
    let covered = lookback_covered_minutes
        .max(0)
        .min(lookback_requested_minutes);
    let missing = lookback_requested_minutes.saturating_sub(covered);
    let coverage_ratio = if lookback_requested_minutes > 0 {
        covered as f64 / lookback_requested_minutes as f64
    } else {
        0.0
    };
    serde_json::json!({
        "event_count": events.len(),
        "events": events,
        "lookback_requested_minutes": lookback_requested_minutes,
        "lookback_covered_minutes": covered,
        "lookback_missing_minutes": missing,
        "lookback_coverage_ratio": coverage_ratio,
        "history_source": history_source,
    })
}
