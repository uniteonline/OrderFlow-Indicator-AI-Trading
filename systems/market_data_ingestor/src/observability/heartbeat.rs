use crate::app::bootstrap::AppContext;
use crate::observability::metrics::AppMetrics;
use crate::sinks::ops_db_writer::{HeartbeatPayload, OpsDbWriter};
use anyhow::Result;
use chrono::{TimeZone, Utc};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{info, warn};

const HEARTBEAT_INTERVAL_SECS: u64 = 5;

pub async fn run_heartbeat_loop(
    ctx: Arc<AppContext>,
    ops_writer: Arc<OpsDbWriter>,
    metrics: Arc<AppMetrics>,
) -> Result<()> {
    let mut ticker = interval(Duration::from_secs(HEARTBEAT_INTERVAL_SECS));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    info!(
        interval_secs = HEARTBEAT_INTERVAL_SECS,
        "ops heartbeat loop started"
    );

    loop {
        ticker.tick().await;

        let snapshot = metrics.snapshot();
        let last_event_ts = if snapshot.last_event_ts_ms > 0 {
            Utc.timestamp_millis_opt(snapshot.last_event_ts_ms).single()
        } else {
            None
        };
        let event_lag_ms = last_event_ts.map(|ts| (Utc::now() - ts).num_milliseconds());
        let total_errors = snapshot.db_errors + snapshot.mq_errors + snapshot.normalize_errors;
        let error_rate = if snapshot.processed_events > 0 {
            Some(total_errors as f64 / snapshot.processed_events as f64)
        } else {
            Some(0.0)
        };

        let payload = HeartbeatPayload {
            role: "ingestor".to_string(),
            last_processed_event_ts: last_event_ts,
            event_lag_ms,
            queue_lag: Some(snapshot.queue_lag),
            error_rate,
            backfill_mode: false,
        };

        if let Err(err) = ops_writer
            .upsert_heartbeat(
                "market_data_ingestor",
                &ctx.producer_instance_id,
                "ok",
                payload,
            )
            .await
        {
            warn!(error = %err, "write heartbeat failed");
        }
    }
}
