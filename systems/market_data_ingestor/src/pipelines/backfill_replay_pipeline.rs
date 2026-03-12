use crate::pipelines::persist_async::registered_persist_queue_stats;
use anyhow::Result;
use std::time::Instant;
use tokio::time::{sleep, Duration};
use tracing::warn;

const BACKFILL_QUEUE_CHECK_INTERVAL_MS: u64 = 50;
const BACKFILL_STALL_LOG_INTERVAL_SECS: u64 = 5;

#[derive(Debug, Clone, Copy)]
pub struct BackfillThrottleConfig {
    pub max_events_per_sec: usize,
    pub queue_high_watermark: usize,
    pub queue_low_watermark: usize,
}

pub struct BackfillThrottle {
    market: &'static str,
    config: BackfillThrottleConfig,
    window_started_at: Instant,
    emitted_in_window: usize,
    last_stall_log_at: Instant,
}

impl BackfillThrottle {
    pub fn new(market: &'static str, config: BackfillThrottleConfig) -> Self {
        Self {
            market,
            config,
            window_started_at: Instant::now(),
            emitted_in_window: 0,
            last_stall_log_at: Instant::now(),
        }
    }

    pub async fn before_emit(&mut self) {
        self.wait_for_queue_headroom().await;
        self.apply_rate_limit().await;
    }

    async fn wait_for_queue_headroom(&mut self) {
        let high = self.config.queue_high_watermark;
        let low = self.config.queue_low_watermark.min(high);
        if high == 0 {
            return;
        }

        loop {
            let Some(stats) = registered_persist_queue_stats(self.market) else {
                return;
            };
            if stats.pending <= high {
                return;
            }

            if self.last_stall_log_at.elapsed()
                >= Duration::from_secs(BACKFILL_STALL_LOG_INTERVAL_SECS)
            {
                warn!(
                    market = self.market,
                    pending = stats.pending,
                    trade_pending = stats.trade_pending,
                    non_trade_pending = stats.non_trade_pending,
                    high_watermark = high,
                    low_watermark = low,
                    "backfill throttle engaged: persist queue backlog high, pause replay/backfill"
                );
                self.last_stall_log_at = Instant::now();
            }

            sleep(Duration::from_millis(BACKFILL_QUEUE_CHECK_INTERVAL_MS)).await;

            let Some(next_stats) = registered_persist_queue_stats(self.market) else {
                return;
            };
            if next_stats.pending <= low {
                return;
            }
        }
    }

    async fn apply_rate_limit(&mut self) {
        let max_events = self.config.max_events_per_sec;
        if max_events == 0 {
            return;
        }

        let elapsed = self.window_started_at.elapsed();
        if elapsed >= Duration::from_secs(1) {
            self.window_started_at = Instant::now();
            self.emitted_in_window = 0;
        }

        if self.emitted_in_window >= max_events {
            let sleep_for = Duration::from_secs(1).saturating_sub(elapsed);
            if !sleep_for.is_zero() {
                sleep(sleep_for).await;
            }
            self.window_started_at = Instant::now();
            self.emitted_in_window = 0;
        }

        self.emitted_in_window = self.emitted_in_window.saturating_add(1);
    }
}

pub async fn run() -> Result<()> {
    // Reserved for offline replay from historical files into the same normalize->store->mq path.
    Ok(())
}
