use chrono::{DateTime, Utc};

pub fn build_indicator_event_id(
    symbol: &str,
    indicator_code: &str,
    event_type: &str,
    ts_event_start: DateTime<Utc>,
    ts_event_end: Option<DateTime<Utc>>,
    direction: i16,
    pivot_ts_1: Option<DateTime<Utc>>,
    pivot_ts_2: Option<DateTime<Utc>>,
) -> String {
    format!(
        "{}:{}:{}:{}:{}:{}:{}:{}",
        symbol.to_uppercase(),
        indicator_code,
        event_type,
        ts_event_start.timestamp_millis(),
        ts_event_end
            .map(|ts| ts.timestamp_millis().to_string())
            .unwrap_or_else(|| "na".to_string()),
        direction,
        pivot_ts_1
            .map(|ts| ts.timestamp_millis().to_string())
            .unwrap_or_else(|| "na".to_string()),
        pivot_ts_2
            .map(|ts| ts.timestamp_millis().to_string())
            .unwrap_or_else(|| "na".to_string()),
    )
}

fn encode_price_anchor(price: f64) -> String {
    format!("{price:.8}")
}

pub fn build_initiation_event_id(
    symbol: &str,
    indicator_code: &str,
    event_type: &str,
    direction: i16,
    event_available_ts: DateTime<Utc>,
    ts_event_start: DateTime<Utc>,
    ts_event_end: DateTime<Utc>,
    pivot_price: f64,
) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}|{}",
        indicator_code,
        symbol.to_uppercase(),
        event_type,
        direction,
        event_available_ts.timestamp_millis(),
        ts_event_start.timestamp_millis(),
        ts_event_end.timestamp_millis(),
        encode_price_anchor(pivot_price),
    )
}

pub fn build_exhaustion_event_id(
    symbol: &str,
    indicator_code: &str,
    event_type: &str,
    direction: i16,
    event_available_ts: DateTime<Utc>,
    pivot_ts_1: DateTime<Utc>,
    pivot_ts_2: DateTime<Utc>,
) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}",
        indicator_code,
        symbol.to_uppercase(),
        event_type,
        direction,
        event_available_ts.timestamp_millis(),
        pivot_ts_1.timestamp_millis(),
        pivot_ts_2.timestamp_millis(),
    )
}

pub fn build_divergence_event_id(
    symbol: &str,
    divergence_type: &str,
    pivot_side: &str,
    ts_event_start: DateTime<Utc>,
    ts_event_end: DateTime<Utc>,
    pivot_ts_1: Option<DateTime<Utc>>,
    pivot_ts_2: Option<DateTime<Utc>>,
) -> String {
    format!(
        "{}:divergence:{}:{}:{}:{}:{}:{}",
        symbol.to_uppercase(),
        divergence_type,
        pivot_side,
        ts_event_start.timestamp_millis(),
        ts_event_end.timestamp_millis(),
        pivot_ts_1
            .map(|ts| ts.timestamp_millis().to_string())
            .unwrap_or_else(|| "na".to_string()),
        pivot_ts_2
            .map(|ts| ts.timestamp_millis().to_string())
            .unwrap_or_else(|| "na".to_string()),
    )
}

#[cfg(test)]
mod tests {
    use super::{build_exhaustion_event_id, build_initiation_event_id};
    use chrono::{TimeZone, Utc};

    #[test]
    fn initiation_event_id_is_stable_for_same_confirmed_event() {
        let start_ts = Utc.with_ymd_and_hms(2026, 3, 9, 1, 10, 0).unwrap();
        let end_ts = Utc.with_ymd_and_hms(2026, 3, 9, 1, 16, 0).unwrap();
        let event_available_ts = end_ts;

        let left = build_initiation_event_id(
            "ETHUSDT",
            "initiation",
            "bearish_initiation",
            -1,
            event_available_ts,
            start_ts,
            end_ts,
            1950.4,
        );
        let right = build_initiation_event_id(
            "ethusdt",
            "initiation",
            "bearish_initiation",
            -1,
            event_available_ts,
            start_ts,
            end_ts,
            1950.4,
        );

        assert_eq!(left, right);
    }

    #[test]
    fn exhaustion_event_id_is_stable_for_same_confirmed_event() {
        let event_available_ts = Utc.with_ymd_and_hms(2026, 3, 9, 23, 44, 0).unwrap();
        let pivot_ts_1 = Utc.with_ymd_and_hms(2026, 3, 9, 23, 31, 0).unwrap();
        let pivot_ts_2 = Utc.with_ymd_and_hms(2026, 3, 9, 23, 39, 0).unwrap();

        let left = build_exhaustion_event_id(
            "ETHUSDT",
            "buying_exhaustion",
            "buying_exhaustion",
            -1,
            event_available_ts,
            pivot_ts_1,
            pivot_ts_2,
        );
        let right = build_exhaustion_event_id(
            "ETHUSDT",
            "buying_exhaustion",
            "buying_exhaustion",
            -1,
            event_available_ts,
            pivot_ts_1,
            pivot_ts_2,
        );

        assert_eq!(left, right);
    }
}
