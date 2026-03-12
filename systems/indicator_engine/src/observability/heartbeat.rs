use crate::app::bootstrap::AppContext;
use crate::observability::metrics::AppMetrics;
use anyhow::Result;
use chrono::{TimeZone, Utc};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{info, warn};

const HEARTBEAT_INTERVAL_SECS: u64 = 5;

pub async fn run_heartbeat_loop(ctx: Arc<AppContext>, metrics: Arc<AppMetrics>) -> Result<()> {
    let mut ticker = interval(Duration::from_secs(HEARTBEAT_INTERVAL_SECS));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    info!(
        interval_secs = HEARTBEAT_INTERVAL_SECS,
        "indicator heartbeat loop started"
    );

    loop {
        ticker.tick().await;

        let snapshot = metrics.snapshot();
        let last_event_ts = if snapshot.last_event_ts_ms > 0 {
            Utc.timestamp_millis_opt(snapshot.last_event_ts_ms).single()
        } else {
            None
        };
        let event_writer_last_failure_ts = if snapshot.event_writer_last_failure_ts_ms > 0 {
            Utc.timestamp_millis_opt(snapshot.event_writer_last_failure_ts_ms)
                .single()
        } else {
            None
        };

        let event_lag_ms = last_event_ts.map(|ts| (Utc::now() - ts).num_milliseconds());
        let total_errors = snapshot.db_errors + snapshot.mq_errors + snapshot.decode_errors;
        let error_rate = if snapshot.processed_events > 0 {
            Some(total_errors as f64 / snapshot.processed_events as f64)
        } else {
            Some(0.0)
        };

        let metrics_json = serde_json::json!({
            "role": "indicator-engine",
            "instance_id": ctx.producer_instance_id,
            "last_processed_event_ts": last_event_ts.map(|ts| ts.to_rfc3339()),
            "event_lag_ms": event_lag_ms,
            "queue_lag": snapshot.queue_lag,
            "error_rate": error_rate,
            "backfill_mode": false,
            "processed_events": snapshot.processed_events,
            "exported_windows": snapshot.exported_windows,
            "db_errors": snapshot.db_errors,
            "mq_errors": snapshot.mq_errors,
            "decode_errors": snapshot.decode_errors,
            "event_writer_consecutive_failures": snapshot.event_writer_consecutive_failures,
            "event_writer_last_failure_ts": event_writer_last_failure_ts.map(|ts| ts.to_rfc3339()),
            "event_writer_last_failure_reason": snapshot.event_writer_last_failure_reason
        });

        if let Err(err) = sqlx::query(
            r#"
            INSERT INTO ops.service_heartbeat (
                service_name, instance_id, role, ts_heartbeat, status, metrics_json
            )
            VALUES ($1, $2, $3, now(), 'ok', $4)
            ON CONFLICT (service_name, instance_id)
            DO UPDATE SET
                role = EXCLUDED.role,
                ts_heartbeat = EXCLUDED.ts_heartbeat,
                status = EXCLUDED.status,
                metrics_json = EXCLUDED.metrics_json
            "#,
        )
        .bind("indicator_engine")
        .bind(&ctx.producer_instance_id)
        .bind("indicator-engine")
        .bind(metrics_json)
        .execute(&ctx.db_pool)
        .await
        {
            warn!(error = %err, "write indicator heartbeat failed");
        }
    }
}
