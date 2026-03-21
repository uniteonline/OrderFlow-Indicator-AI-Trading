use crate::ingest::watermark::{floor_minute, Watermark};
use chrono::{DateTime, Duration, Utc};

pub struct WindowScheduler {
    watermark: Watermark,
    last_emitted: Option<DateTime<Utc>>,
}

impl WindowScheduler {
    pub fn new(watermark_lateness_secs: i64) -> Self {
        Self {
            watermark: Watermark::with_lateness_secs(watermark_lateness_secs),
            last_emitted: None,
        }
    }

    /// Prime the scheduler's start point so the next `ready_minutes` call begins
    /// from `bucket_ts` rather than skipping ahead to `latest_closed`.
    ///
    /// This must be called **after** all backfill events have been ingested into
    /// the state-store but **before** the first `ready_minutes` call.  It is a
    /// no-op if the scheduler has already emitted at least one minute (i.e. it
    /// only takes effect when `last_emitted` is still `None`).
    pub fn prime_start_from(&mut self, bucket_ts: DateTime<Utc>) {
        if self.last_emitted.is_none() {
            // Set last_emitted to the minute *before* bucket_ts so that the
            // very next ready_minutes() starts from bucket_ts itself.
            self.last_emitted = Some(floor_minute(bucket_ts) - Duration::minutes(1));
        }
    }

    pub fn next_minute_to_emit(&self) -> Option<DateTime<Utc>> {
        self.last_emitted.map(|last| last + Duration::minutes(1))
    }

    pub fn mark_emitted_through(&mut self, bucket_ts: DateTime<Utc>) {
        self.last_emitted = Some(floor_minute(bucket_ts));
    }

    pub fn closed_minute(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        self.watermark.closed_minute(now)
    }

    pub fn ready_minutes_through(&mut self, bucket_ts: DateTime<Utc>) -> Vec<DateTime<Utc>> {
        let latest_ready = floor_minute(bucket_ts);

        let start = match self.last_emitted {
            Some(last) => last + Duration::minutes(1),
            None => latest_ready,
        };

        if start > latest_ready {
            return Vec::new();
        }

        let mut out = Vec::new();
        let mut cur = floor_minute(start);
        while cur <= latest_ready {
            out.push(cur);
            cur += Duration::minutes(1);
        }

        self.last_emitted = out.last().cloned();
        out
    }

    pub fn ready_minutes(&mut self, now: DateTime<Utc>) -> Vec<DateTime<Utc>> {
        let latest_closed = self.watermark.closed_minute(now);
        self.ready_minutes_through(latest_closed)
    }
}
