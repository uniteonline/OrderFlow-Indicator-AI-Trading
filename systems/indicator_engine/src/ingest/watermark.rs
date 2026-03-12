use chrono::{DateTime, Duration, TimeZone, Utc};

#[derive(Debug, Clone, Copy)]
pub struct Watermark {
    pub lateness: Duration,
}

impl Default for Watermark {
    fn default() -> Self {
        Self {
            lateness: Duration::seconds(5),
        }
    }
}

impl Watermark {
    pub fn with_lateness_secs(secs: i64) -> Self {
        Self {
            lateness: Duration::seconds(secs.max(0)),
        }
    }

    pub fn closed_minute(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        let cutoff = now - self.lateness;
        floor_minute(cutoff) - Duration::minutes(1)
    }
}

pub fn floor_minute(ts: DateTime<Utc>) -> DateTime<Utc> {
    let sec = ts.timestamp();
    let floored = sec - (sec.rem_euclid(60));
    Utc.timestamp_opt(floored, 0).single().unwrap_or(ts)
}
