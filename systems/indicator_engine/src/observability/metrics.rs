use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Mutex;

#[derive(Default)]
pub struct AppMetrics {
    backfill_mode: AtomicBool,
    processed_events: AtomicU64,
    exported_windows: AtomicU64,
    db_errors: AtomicU64,
    mq_errors: AtomicU64,
    decode_errors: AtomicU64,
    last_event_ts_ms: AtomicI64,
    last_persisted_ts_ms: AtomicI64,
    next_minute_to_emit_ts_ms: AtomicI64,
    ready_through_ts_ms: AtomicI64,
    latest_contiguous_complete_minute_ts_ms: AtomicI64,
    dirty_recompute_from_ts_ms: AtomicI64,
    dirty_recompute_end_ts_ms: AtomicI64,
    trade_channel_len: AtomicI64,
    non_trade_channel_len: AtomicI64,
    frontier_trade_futures_ts_ms: AtomicI64,
    frontier_trade_spot_ts_ms: AtomicI64,
    frontier_orderbook_futures_ts_ms: AtomicI64,
    frontier_orderbook_spot_ts_ms: AtomicI64,
    frontier_liq_futures_ts_ms: AtomicI64,
    frontier_funding_futures_ts_ms: AtomicI64,
    queue_lag: AtomicI64,
    event_writer_consecutive_failures: AtomicU64,
    event_writer_last_failure_ts_ms: AtomicI64,
    event_writer_last_failure_reason: Mutex<Option<String>>,
}

#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub backfill_mode: bool,
    pub processed_events: u64,
    pub exported_windows: u64,
    pub db_errors: u64,
    pub mq_errors: u64,
    pub decode_errors: u64,
    pub last_event_ts_ms: i64,
    pub last_persisted_ts_ms: i64,
    pub next_minute_to_emit_ts_ms: i64,
    pub ready_through_ts_ms: i64,
    pub latest_contiguous_complete_minute_ts_ms: i64,
    pub dirty_recompute_from_ts_ms: i64,
    pub dirty_recompute_end_ts_ms: i64,
    pub trade_channel_len: i64,
    pub non_trade_channel_len: i64,
    pub frontier_trade_futures_ts_ms: i64,
    pub frontier_trade_spot_ts_ms: i64,
    pub frontier_orderbook_futures_ts_ms: i64,
    pub frontier_orderbook_spot_ts_ms: i64,
    pub frontier_liq_futures_ts_ms: i64,
    pub frontier_funding_futures_ts_ms: i64,
    pub queue_lag: i64,
    pub event_writer_consecutive_failures: u64,
    pub event_writer_last_failure_ts_ms: i64,
    pub event_writer_last_failure_reason: Option<String>,
}

impl AppMetrics {
    pub fn set_backfill_mode(&self, backfill_mode: bool) {
        self.backfill_mode.store(backfill_mode, Ordering::Relaxed);
    }

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

    pub fn set_last_persisted_ts(&self, ts_ms: Option<i64>) {
        self.last_persisted_ts_ms
            .store(ts_ms.unwrap_or(0), Ordering::Relaxed);
    }

    pub fn set_runtime_window_state(
        &self,
        next_minute_to_emit_ts_ms: Option<i64>,
        ready_through_ts_ms: Option<i64>,
        latest_contiguous_complete_minute_ts_ms: Option<i64>,
    ) {
        self.next_minute_to_emit_ts_ms
            .store(next_minute_to_emit_ts_ms.unwrap_or(0), Ordering::Relaxed);
        self.ready_through_ts_ms
            .store(ready_through_ts_ms.unwrap_or(0), Ordering::Relaxed);
        self.latest_contiguous_complete_minute_ts_ms.store(
            latest_contiguous_complete_minute_ts_ms.unwrap_or(0),
            Ordering::Relaxed,
        );
    }

    pub fn set_dirty_recompute_bounds(
        &self,
        dirty_recompute_from_ts_ms: Option<i64>,
        dirty_recompute_end_ts_ms: Option<i64>,
    ) {
        self.dirty_recompute_from_ts_ms
            .store(dirty_recompute_from_ts_ms.unwrap_or(0), Ordering::Relaxed);
        self.dirty_recompute_end_ts_ms
            .store(dirty_recompute_end_ts_ms.unwrap_or(0), Ordering::Relaxed);
    }

    pub fn set_channel_lengths(&self, trade_channel_len: usize, non_trade_channel_len: usize) {
        self.trade_channel_len
            .store(trade_channel_len as i64, Ordering::Relaxed);
        self.non_trade_channel_len
            .store(non_trade_channel_len as i64, Ordering::Relaxed);
    }

    pub fn set_canonical_frontiers(
        &self,
        trade_futures_ts_ms: Option<i64>,
        trade_spot_ts_ms: Option<i64>,
        orderbook_futures_ts_ms: Option<i64>,
        orderbook_spot_ts_ms: Option<i64>,
        liq_futures_ts_ms: Option<i64>,
        funding_futures_ts_ms: Option<i64>,
    ) {
        self.frontier_trade_futures_ts_ms
            .store(trade_futures_ts_ms.unwrap_or(0), Ordering::Relaxed);
        self.frontier_trade_spot_ts_ms
            .store(trade_spot_ts_ms.unwrap_or(0), Ordering::Relaxed);
        self.frontier_orderbook_futures_ts_ms
            .store(orderbook_futures_ts_ms.unwrap_or(0), Ordering::Relaxed);
        self.frontier_orderbook_spot_ts_ms
            .store(orderbook_spot_ts_ms.unwrap_or(0), Ordering::Relaxed);
        self.frontier_liq_futures_ts_ms
            .store(liq_futures_ts_ms.unwrap_or(0), Ordering::Relaxed);
        self.frontier_funding_futures_ts_ms
            .store(funding_futures_ts_ms.unwrap_or(0), Ordering::Relaxed);
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
            backfill_mode: self.backfill_mode.load(Ordering::Relaxed),
            processed_events: self.processed_events.load(Ordering::Relaxed),
            exported_windows: self.exported_windows.load(Ordering::Relaxed),
            db_errors: self.db_errors.load(Ordering::Relaxed),
            mq_errors: self.mq_errors.load(Ordering::Relaxed),
            decode_errors: self.decode_errors.load(Ordering::Relaxed),
            last_event_ts_ms: self.last_event_ts_ms.load(Ordering::Relaxed),
            last_persisted_ts_ms: self.last_persisted_ts_ms.load(Ordering::Relaxed),
            next_minute_to_emit_ts_ms: self.next_minute_to_emit_ts_ms.load(Ordering::Relaxed),
            ready_through_ts_ms: self.ready_through_ts_ms.load(Ordering::Relaxed),
            latest_contiguous_complete_minute_ts_ms: self
                .latest_contiguous_complete_minute_ts_ms
                .load(Ordering::Relaxed),
            dirty_recompute_from_ts_ms: self.dirty_recompute_from_ts_ms.load(Ordering::Relaxed),
            dirty_recompute_end_ts_ms: self.dirty_recompute_end_ts_ms.load(Ordering::Relaxed),
            trade_channel_len: self.trade_channel_len.load(Ordering::Relaxed),
            non_trade_channel_len: self.non_trade_channel_len.load(Ordering::Relaxed),
            frontier_trade_futures_ts_ms: self.frontier_trade_futures_ts_ms.load(Ordering::Relaxed),
            frontier_trade_spot_ts_ms: self.frontier_trade_spot_ts_ms.load(Ordering::Relaxed),
            frontier_orderbook_futures_ts_ms: self
                .frontier_orderbook_futures_ts_ms
                .load(Ordering::Relaxed),
            frontier_orderbook_spot_ts_ms: self
                .frontier_orderbook_spot_ts_ms
                .load(Ordering::Relaxed),
            frontier_liq_futures_ts_ms: self.frontier_liq_futures_ts_ms.load(Ordering::Relaxed),
            frontier_funding_futures_ts_ms: self
                .frontier_funding_futures_ts_ms
                .load(Ordering::Relaxed),
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
