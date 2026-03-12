use crate::app::bootstrap::AppContext;
use crate::ingest::decoder::{decode_contract_body, EngineEvent};
use crate::ingest::demux::{classify, StreamLane};
use crate::observability::metrics::AppMetrics;
use anyhow::Result;
use futures_util::StreamExt;
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions},
    types::FieldTable,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

pub fn spawn_consumers(
    ctx: Arc<AppContext>,
    trade_tx: mpsc::Sender<EngineEvent>,
    non_trade_tx: mpsc::Sender<EngineEvent>,
    metrics: Arc<AppMetrics>,
) -> Vec<JoinHandle<Result<()>>> {
    let mut handles = Vec::new();
    for queue_name in &ctx.indicator_queues {
        let queue = queue_name.clone();
        let ctx = ctx.clone();
        let trade_tx = trade_tx.clone();
        let non_trade_tx = non_trade_tx.clone();
        let metrics = metrics.clone();
        handles.push(tokio::spawn(async move {
            run_single_consumer(ctx, queue, trade_tx, non_trade_tx, metrics).await
        }));
    }
    handles
}

/// Outer retry loop: reconnect the AMQP channel whenever the broker closes it
/// (e.g. due to a missed heartbeat during a long recompute cycle).
/// Exits only when the downstream mpsc channel is closed (main task exiting).
async fn run_single_consumer(
    ctx: Arc<AppContext>,
    queue_name: String,
    trade_tx: mpsc::Sender<EngineEvent>,
    non_trade_tx: mpsc::Sender<EngineEvent>,
    metrics: Arc<AppMetrics>,
) -> Result<()> {
    let mut attempt: u32 = 0;
    loop {
        if attempt > 0 {
            // Exponential back-off capped at 30 s so we reconnect quickly after
            // a brief heartbeat timeout but don't spam on a hard broker failure.
            let backoff = Duration::from_secs((2u64.pow(attempt.min(4))).min(30));
            warn!(
                queue = %queue_name,
                attempt,
                backoff_secs = backoff.as_secs(),
                "AMQP consumer disconnected, reconnecting"
            );
            tokio::time::sleep(backoff).await;
        }
        attempt += 1;

        match run_consumer_session(&ctx, &queue_name, &trade_tx, &non_trade_tx, &metrics).await {
            // Session ended because the downstream mpsc receiver was dropped
            // (main task is shutting down). Exit cleanly.
            ConsumerExit::DownstreamClosed => {
                info!(queue = %queue_name, "consumer stopping: downstream channel closed");
                return Ok(());
            }
            // AMQP channel closed by the broker (consumer.next() → None).
            // Retry after back-off.
            ConsumerExit::BrokerClosed => {
                // Reset back-off counter on a successful reconnect attempt;
                // keep counting if the session was very short (instant failure).
                attempt = 1; // will double on next failure, reset on success
            }
            // Channel / consume setup failed.  Retry with back-off.
            ConsumerExit::SetupError(err) => {
                error!(queue = %queue_name, error = %err, "consumer setup failed");
            }
        }
    }
}

enum ConsumerExit {
    DownstreamClosed,
    BrokerClosed,
    SetupError(anyhow::Error),
}

/// One AMQP consumer session: create a channel, register a consumer, drain
/// messages until the broker closes the channel or the downstream mpsc is gone.
async fn run_consumer_session(
    ctx: &Arc<AppContext>,
    queue_name: &str,
    trade_tx: &mpsc::Sender<EngineEvent>,
    non_trade_tx: &mpsc::Sender<EngineEvent>,
    metrics: &Arc<AppMetrics>,
) -> ConsumerExit {
    let channel = match ctx.mq_connection.create_channel().await {
        Ok(ch) => ch,
        Err(err) => return ConsumerExit::SetupError(anyhow::Error::from(err)),
    };

    if let Err(err) = channel.basic_qos(500, BasicQosOptions::default()).await {
        return ConsumerExit::SetupError(anyhow::Error::from(err));
    }

    let consumer_tag = format!("indicator_engine_{}", queue_name.replace('.', "_"));
    let mut consumer = match channel
        .basic_consume(
            queue_name,
            &consumer_tag,
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
    {
        Ok(c) => c,
        Err(err) => return ConsumerExit::SetupError(anyhow::Error::from(err)),
    };

    info!(queue = %queue_name, "indicator mq consumer started");

    while let Some(delivery_result) = consumer.next().await {
        match delivery_result {
            Ok(delivery) => {
                let decoded = decode_contract_body(&delivery.data);
                match decoded {
                    Ok(event) => {
                        let lane = classify(&event);
                        let target_tx = if matches!(lane, StreamLane::Trade) {
                            trade_tx
                        } else {
                            non_trade_tx
                        };
                        if target_tx.send(event).await.is_err() {
                            error!(queue = %queue_name, "indicator channel closed, stopping consumer");
                            return ConsumerExit::DownstreamClosed;
                        }
                        if let Err(err) = delivery.ack(BasicAckOptions::default()).await {
                            error!(queue = %queue_name, error = %err, "ack failed");
                        }
                    }
                    Err(err) => {
                        metrics.inc_decode_error();
                        warn!(
                            queue = %queue_name,
                            error = %err,
                            payload = %String::from_utf8_lossy(&delivery.data),
                            "decode md event failed, sending to dead-letter"
                        );
                        // NACK with requeue=false: routes to dead-letter queue instead of
                        // silently dropping. Previously ACK'd here which permanently discarded
                        // malformed messages from the broker.
                        if let Err(nack_err) = delivery
                            .nack(BasicNackOptions {
                                requeue: false,
                                multiple: false,
                            })
                            .await
                        {
                            error!(queue = %queue_name, error = %nack_err, "nack failed");
                        }
                    }
                }
            }
            Err(err) => {
                error!(queue = %queue_name, error = %err, "consumer stream error");
            }
        }
    }

    // consumer.next() returned None: broker closed the channel.
    warn!(queue = %queue_name, "AMQP consumer stream ended (broker closed channel)");
    ConsumerExit::BrokerClosed
}
