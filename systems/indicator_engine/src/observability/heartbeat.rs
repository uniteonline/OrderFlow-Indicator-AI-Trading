use crate::app::bootstrap::AppContext;
use crate::observability::metrics::AppMetrics;
use anyhow::Result;
use chrono::{TimeZone, Utc};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{info, warn};

const HEARTBEAT_INTERVAL_SECS: u64 = 5;

fn optional_ts_from_ms(ts_ms: i64) -> Option<chrono::DateTime<Utc>> {
    if ts_ms > 0 {
        Utc.timestamp_millis_opt(ts_ms).single()
    } else {
        None
    }
}

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
        let last_event_ts = optional_ts_from_ms(snapshot.last_event_ts_ms);
        let last_persisted_ts = optional_ts_from_ms(snapshot.last_persisted_ts_ms);
        let next_minute_to_emit_ts = optional_ts_from_ms(snapshot.next_minute_to_emit_ts_ms);
        let ready_through_ts = optional_ts_from_ms(snapshot.ready_through_ts_ms);
        let latest_contiguous_complete_minute_ts =
            optional_ts_from_ms(snapshot.latest_contiguous_complete_minute_ts_ms);
        let dirty_recompute_from_ts = optional_ts_from_ms(snapshot.dirty_recompute_from_ts_ms);
        let dirty_recompute_end_ts = optional_ts_from_ms(snapshot.dirty_recompute_end_ts_ms);
        let frontier_trade_futures_ts = optional_ts_from_ms(snapshot.frontier_trade_futures_ts_ms);
        let frontier_trade_spot_ts = optional_ts_from_ms(snapshot.frontier_trade_spot_ts_ms);
        let frontier_orderbook_futures_ts =
            optional_ts_from_ms(snapshot.frontier_orderbook_futures_ts_ms);
        let frontier_orderbook_spot_ts =
            optional_ts_from_ms(snapshot.frontier_orderbook_spot_ts_ms);
        let frontier_liq_futures_ts = optional_ts_from_ms(snapshot.frontier_liq_futures_ts_ms);
        let frontier_funding_futures_ts =
            optional_ts_from_ms(snapshot.frontier_funding_futures_ts_ms);
        let event_writer_last_failure_ts =
            optional_ts_from_ms(snapshot.event_writer_last_failure_ts_ms);

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
            "last_persisted_ts": last_persisted_ts.map(|ts| ts.to_rfc3339()),
            "next_minute_to_emit_ts": next_minute_to_emit_ts.map(|ts| ts.to_rfc3339()),
            "ready_through_ts": ready_through_ts.map(|ts| ts.to_rfc3339()),
            "latest_contiguous_complete_minute_ts": latest_contiguous_complete_minute_ts.map(|ts| ts.to_rfc3339()),
            "dirty_recompute_from_ts": dirty_recompute_from_ts.map(|ts| ts.to_rfc3339()),
            "dirty_recompute_end_ts": dirty_recompute_end_ts.map(|ts| ts.to_rfc3339()),
            "event_lag_ms": event_lag_ms,
            "queue_lag": snapshot.queue_lag,
            "trade_channel_len": snapshot.trade_channel_len,
            "non_trade_channel_len": snapshot.non_trade_channel_len,
            "error_rate": error_rate,
            "backfill_mode": snapshot.backfill_mode,
            "processed_events": snapshot.processed_events,
            "exported_windows": snapshot.exported_windows,
            "db_errors": snapshot.db_errors,
            "mq_errors": snapshot.mq_errors,
            "decode_errors": snapshot.decode_errors,
            "canonical_frontiers": {
                "trade_futures_ts": frontier_trade_futures_ts.map(|ts| ts.to_rfc3339()),
                "trade_spot_ts": frontier_trade_spot_ts.map(|ts| ts.to_rfc3339()),
                "orderbook_futures_ts": frontier_orderbook_futures_ts.map(|ts| ts.to_rfc3339()),
                "orderbook_spot_ts": frontier_orderbook_spot_ts.map(|ts| ts.to_rfc3339()),
                "liq_futures_ts": frontier_liq_futures_ts.map(|ts| ts.to_rfc3339()),
                "funding_futures_ts": frontier_funding_futures_ts.map(|ts| ts.to_rfc3339())
            },
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
