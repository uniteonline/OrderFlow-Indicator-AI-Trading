use chrono::{DateTime, Utc};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

#[derive(Default)]
pub struct AppMetrics {
    processed_events: AtomicU64,
    db_errors: AtomicU64,
    mq_errors: AtomicU64,
    normalize_errors: AtomicU64,
    last_event_ts_ms: AtomicI64,
    queue_lag: AtomicI64,
}

impl AppMetrics {
    pub fn inc_processed(&self, event_ts: DateTime<Utc>) {
        self.processed_events.fetch_add(1, Ordering::Relaxed);
        self.last_event_ts_ms
            .store(event_ts.timestamp_millis(), Ordering::Relaxed);
    }

    pub fn inc_db_error(&self) {
        self.db_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_mq_error(&self) {
        self.mq_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_normalize_error(&self) {
        self.normalize_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_queue_lag(&self, queue_lag: i64) {
        self.queue_lag.store(queue_lag.max(0), Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            processed_events: self.processed_events.load(Ordering::Relaxed),
            db_errors: self.db_errors.load(Ordering::Relaxed),
            mq_errors: self.mq_errors.load(Ordering::Relaxed),
            normalize_errors: self.normalize_errors.load(Ordering::Relaxed),
            last_event_ts_ms: self.last_event_ts_ms.load(Ordering::Relaxed),
            queue_lag: self.queue_lag.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub processed_events: u64,
    pub db_errors: u64,
    pub mq_errors: u64,
    pub normalize_errors: u64,
    pub last_event_ts_ms: i64,
    pub queue_lag: i64,
}
