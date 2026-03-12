use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Mutex;

#[derive(Default)]
pub struct AppMetrics {
    processed_events: AtomicU64,
    exported_windows: AtomicU64,
    db_errors: AtomicU64,
    mq_errors: AtomicU64,
    decode_errors: AtomicU64,
    last_event_ts_ms: AtomicI64,
    queue_lag: AtomicI64,
    event_writer_consecutive_failures: AtomicU64,
    event_writer_last_failure_ts_ms: AtomicI64,
    event_writer_last_failure_reason: Mutex<Option<String>>,
}

#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub processed_events: u64,
    pub exported_windows: u64,
    pub db_errors: u64,
    pub mq_errors: u64,
    pub decode_errors: u64,
    pub last_event_ts_ms: i64,
    pub queue_lag: i64,
    pub event_writer_consecutive_failures: u64,
    pub event_writer_last_failure_ts_ms: i64,
    pub event_writer_last_failure_reason: Option<String>,
}

impl AppMetrics {
    pub fn inc_processed(&self, event_ts_ms: i64) {
        self.processed_events.fetch_add(1, Ordering::Relaxed);
        self.last_event_ts_ms.store(event_ts_ms, Ordering::Relaxed);
    }

    pub fn inc_exported_window(&self) {
        self.exported_windows.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_db_error(&self) {
        self.db_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_mq_error(&self) {
        self.mq_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_decode_error(&self) {
        self.decode_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_queue_lag(&self, queue_lag: i64) {
        self.queue_lag.store(queue_lag.max(0), Ordering::Relaxed);
    }

    pub fn record_event_writer_failure(&self, failure_ts_ms: i64, reason: String) {
        self.inc_db_error();
        self.event_writer_consecutive_failures
            .fetch_add(1, Ordering::Relaxed);
        self.event_writer_last_failure_ts_ms
            .store(failure_ts_ms, Ordering::Relaxed);
        if let Ok(mut slot) = self.event_writer_last_failure_reason.lock() {
            *slot = Some(reason);
        }
    }

    pub fn record_event_writer_success(&self) {
        self.event_writer_consecutive_failures
            .store(0, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            processed_events: self.processed_events.load(Ordering::Relaxed),
            exported_windows: self.exported_windows.load(Ordering::Relaxed),
            db_errors: self.db_errors.load(Ordering::Relaxed),
            mq_errors: self.mq_errors.load(Ordering::Relaxed),
            decode_errors: self.decode_errors.load(Ordering::Relaxed),
            last_event_ts_ms: self.last_event_ts_ms.load(Ordering::Relaxed),
            queue_lag: self.queue_lag.load(Ordering::Relaxed),
            event_writer_consecutive_failures: self
                .event_writer_consecutive_failures
                .load(Ordering::Relaxed),
            event_writer_last_failure_ts_ms: self
                .event_writer_last_failure_ts_ms
                .load(Ordering::Relaxed),
            event_writer_last_failure_reason: self
                .event_writer_last_failure_reason
                .lock()
                .ok()
                .and_then(|value| value.clone()),
        }
    }
}
