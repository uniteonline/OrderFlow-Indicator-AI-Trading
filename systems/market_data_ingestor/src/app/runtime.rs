use crate::app::bootstrap::AppContext;
use crate::exchange::binance::rest::client::{BinanceRestClient, RestRetryPolicy};
use crate::observability::{heartbeat, metrics::AppMetrics};
use crate::pipelines::{futures_pipeline, persist_async, spot_pipeline};
use crate::sinks::{
    md_db_writer::MdDbWriter, mq_publisher::MqPublisher, ops_db_writer::OpsDbWriter,
    outbox_dispatcher::OutboxDispatcher, outbox_writer::OutboxWriter, parquet_sink::ParquetSink,
};
use crate::state::{backfill_scheduler, depth_rebuilder};
use anyhow::{Context, Result};
use futures_util::StreamExt;
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, BasicQosOptions, ConfirmSelectOptions},
    types::FieldTable,
};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

const OUTBOX_DISPATCH_WORKERS: usize = 3;

pub async fn run(ctx: AppContext) -> Result<()> {
    let ctx = Arc::new(ctx);

    let publisher = Arc::new(MqPublisher::new(
        ctx.mq_publish_channel.clone(),
        ctx.config.mq.exchanges.md_live.name.clone(),
        ctx.config.mq.exchanges.md_replay.name.clone(),
        ctx.producer_instance_id.clone(),
    ));
    let db_writer = Arc::new(MdDbWriter::new(ctx.md_db_pool.clone()));
    let ops_writer = Arc::new(OpsDbWriter::new(ctx.ops_db_pool.clone()));
    let outbox_writer = Arc::new(OutboxWriter::new(ctx.ops_db_pool.clone()));
    if ctx.config.parquet.enabled {
        let parquet_sink = Arc::new(ParquetSink::from_config(&ctx.config.parquet));
        persist_async::configure_parquet_sink(parquet_sink);
    } else {
        info!("cold-store parquet sink disabled by config");
    }
    let metrics = Arc::new(AppMetrics::default());

    let rest_client = Arc::new(BinanceRestClient::new(
        ctx.http_client.clone(),
        RestRetryPolicy {
            enabled: ctx.config.network.http.retry.enabled,
            max_retries: ctx.config.network.http.retry.max_retries,
            base_backoff_ms: ctx.config.network.http.retry.base_backoff_ms,
            max_backoff_ms: ctx.config.network.http.retry.max_backoff_ms,
        },
    ));

    let mut handles = vec![];
    handles.push(tokio::spawn(run_selfcheck_consumer(ctx.clone())));
    handles.push(tokio::spawn(heartbeat::run_heartbeat_loop(
        ctx.clone(),
        ops_writer.clone(),
        metrics.clone(),
    )));
    // Give each outbox dispatcher its own AMQP channel so that a channel error or
    // backpressure on one worker cannot affect the others, and concurrent basic_publish
    // calls are fully serialized per channel rather than competing on a shared one.
    for worker_id in 0..OUTBOX_DISPATCH_WORKERS {
        let worker_channel = ctx.mq_connection.create_channel().await.with_context(|| {
            format!("create outbox dispatcher channel for worker {}", worker_id)
        })?;
        worker_channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .with_context(|| {
                format!(
                    "enable publisher confirms for outbox dispatcher worker {}",
                    worker_id
                )
            })?;
        let outbox_dispatcher = OutboxDispatcher::new(
            ctx.ops_db_pool.clone(),
            worker_channel,
            ctx.config.mq.exchanges.md_live.name.clone(),
        );
        handles.push(tokio::spawn(async move {
            let perform_housekeeping = worker_id == 0;
            info!(
                worker_id = worker_id,
                workers = OUTBOX_DISPATCH_WORKERS,
                perform_housekeeping = perform_housekeeping,
                "starting outbox dispatcher worker"
            );
            outbox_dispatcher
                .run_loop_worker(perform_housekeeping)
                .await
        }));
    }

    handles.push(tokio::spawn(spot_pipeline::run(
        ctx.clone(),
        rest_client.clone(),
        publisher.clone(),
        outbox_writer.clone(),
        db_writer.clone(),
        ops_writer.clone(),
        metrics.clone(),
    )));

    handles.push(tokio::spawn(futures_pipeline::run(
        ctx.clone(),
        rest_client.clone(),
        publisher.clone(),
        outbox_writer.clone(),
        db_writer.clone(),
        ops_writer.clone(),
        metrics.clone(),
    )));

    handles.push(tokio::spawn(depth_rebuilder::run_depth_snapshot_loop(
        ctx.clone(),
        rest_client.clone(),
        db_writer.clone(),
        ops_writer.clone(),
        publisher.clone(),
        outbox_writer.clone(),
        metrics.clone(),
    )));

    handles.push(tokio::spawn(
        backfill_scheduler::run_funding_rate_backfill_loop(
            ctx.clone(),
            rest_client,
            db_writer,
            ops_writer,
            publisher,
            outbox_writer,
            metrics,
        ),
    ));

    info!("market_data_ingestor started; press Ctrl+C to stop");
    tokio::signal::ctrl_c().await?;

    info!("shutdown signal received, stopping tasks");
    for handle in handles {
        handle.abort();
    }
    Ok(())
}

async fn run_selfcheck_consumer(ctx: Arc<AppContext>) -> Result<()> {
    let channel = ctx.mq_consume_channel.clone();

    channel.basic_qos(200, BasicQosOptions::default()).await?;

    let mut consumer = channel
        .basic_consume(
            &ctx.selfcheck_queue,
            "market_data_ingestor_selfcheck_consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    info!(queue = %ctx.selfcheck_queue, "selfcheck mq consumer started");

    while let Some(delivery_result) = consumer.next().await {
        match delivery_result {
            Ok(delivery) => {
                let payload = std::str::from_utf8(&delivery.data).unwrap_or("<non-utf8>");
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(payload) {
                    let msg_type = value
                        .get("msg_type")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unknown");
                    let routing_key = value
                        .get("routing_key")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unknown");
                    debug!(msg_type, routing_key, "selfcheck received mq message");
                } else {
                    warn!("selfcheck received non-json message");
                }

                if let Err(err) = delivery.ack(BasicAckOptions::default()).await {
                    error!(error = %err, "selfcheck ack failed");
                }
            }
            Err(err) => {
                error!(error = %err, "selfcheck consumer error");
            }
        }
    }

    Ok(())
}
