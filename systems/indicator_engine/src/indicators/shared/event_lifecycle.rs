use chrono::{DateTime, Utc};

pub fn is_new_event(last: Option<DateTime<Utc>>, current: DateTime<Utc>) -> bool {
    last.map(|v| current > v).unwrap_or(true)
}
