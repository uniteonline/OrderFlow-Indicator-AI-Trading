use crate::aggregate::kline_3d_from_1d::Kline3dAggregator;
use crate::app::bootstrap::AppContext;
use crate::exchange::binance::rest::client::BinanceRestClient;
use crate::exchange::binance::ws::client::{
    connect_combined_stream, decode_text_message, is_terminal_message,
};
use crate::exchange::binance::ws::{reconnect, spot_streams};
use crate::normalize::{self, kline_normalizer, trade_normalizer, Market, NormalizedMdEvent};
use crate::observability::metrics::AppMetrics;
use crate::pipelines::backfill_replay_pipeline::{BackfillThrottle, BackfillThrottleConfig};
use crate::pipelines::persist_async::{
    mirror_raw_event_to_cold_store, persist_event, try_enqueue_registered_persist_event,
    AsyncPersistQueue,
};
use crate::pipelines::ws_preagg::WsOneSecondPreAggregator;
use crate::sinks::{
    md_db_writer::MdDbWriter, mq_publisher::MqPublisher, ops_db_writer::OpsDbWriter,
    outbox_writer::OutboxWriter,
};
use crate::state::{checkpoints, depth_rebuilder, gap_detector::GapDetector};
use anyhow::Result;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::{hash_map::Entry, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{Duration, Instant, MissedTickBehavior};
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{debug, error, info, warn};

const SPOT_WS_BASE: &str = "wss://stream.binance.com:443";
const REST_PAGE_LIMIT: u16 = 1000;
const KLINE_INTERVALS: [&str; 5] = ["1m", "15m", "1h", "4h", "1d"];
const WS_KEEPALIVE_PING_SECS: u64 = 20;
const WS_IDLE_TIMEOUT_SECS: u64 = 90;
const WS_STATS_INTERVAL_SECS: u64 = 1;
// Congestion warning threshold aligned to ~5s acceptable latency budget.
// Spot rates are lower than futures, so use a smaller but still practical value.
const WS_PERSIST_QUEUE_CONGESTED_THRESHOLD: usize = 4_000;
const WS_PRE_ENQUEUE_CONFLATION_ENABLED: bool = false;
const WS_PRE_ENQUEUE_CONFLATION_MS: i64 = 100;
const WS_PRE_ENQUEUE_SUPPRESS_LOG_EVERY: u64 = 10_000;
const SOURCE_LAG_WARN_SECS: i64 = 5;
const SOURCE_LAG_FORCE_RECONNECT_SECS: i64 = 20;
const SOURCE_LAG_FORCE_RECONNECT_STREAK: u64 = 200;
const SOURCE_LAG_LOG_EVERY: u64 = 100;
const KLINE_GAP_WORKER_QUEUE_CAPACITY: usize = 1024;
const BACKFILL_TRADE_THROTTLE: BackfillThrottleConfig = BackfillThrottleConfig {
    max_events_per_sec: 180,
    queue_high_watermark: 3_000,
    queue_low_watermark: 1_500,
};
const BACKFILL_KLINE_THROTTLE: BackfillThrottleConfig = BackfillThrottleConfig {
    max_events_per_sec: 80,
    queue_high_watermark: 3_000,
    queue_low_watermark: 1_500,
};

#[derive(Debug, Clone)]
struct KlineGapReplayJob {
    market: String,
    symbol: String,
    interval_code: String,
    start_ms: i64,
    end_ms: i64,
}

type ConflationKey = (String, String, String, String);

struct PreEnqueueConflator {
    pending: HashMap<ConflationKey, (i64, NormalizedMdEvent, u64)>,
    suppressed: u64,
    emitted: u64,
}

fn mark_pre_enqueue_conflated(event: &mut NormalizedMdEvent, event_count: u64) {
    if let Some(obj) = event.data.as_object_mut() {
        obj.insert("pre_enqueue_conflated".to_string(), Value::Bool(true));
        obj.insert("event_count".to_string(), Value::from(event_count as i64));
    }
}

async fn maybe_mirror_raw_orderbook_to_outbox(
    event: &NormalizedMdEvent,
    publisher: &Arc<MqPublisher>,
    outbox_writer: &Arc<OutboxWriter>,
    metrics: &Arc<AppMetrics>,
    market: &'static str,
) {
    if !WS_PRE_ENQUEUE_CONFLATION_ENABLED
        || event.backfill_in_progress
        || !matches!(event.msg_type.as_str(), "md.depth" | "md.bbo")
    {
        return;
    }

    let outbox_records = match publisher.build_outbox_records_force_live(event) {
        Ok(v) => v,
        Err(err) => {
            metrics.inc_mq_error();
            warn!(
                error = %err,
                market = market,
                msg_type = %event.msg_type,
                routing_key = %event.routing_key,
                "build raw orderbook outbox record failed"
            );
            return;
        }
    };

    for outbox in outbox_records {
        if let Err(err) = outbox_writer
            .enqueue_without_ack(
                &outbox.exchange_name,
                &outbox.routing_key,
                outbox.message_id,
                outbox.schema_version,
                outbox.headers_json,
                outbox.payload_json,
            )
            .await
        {
            metrics.inc_mq_error();
            warn!(
                error = %err,
                market = market,
                msg_type = %event.msg_type,
                routing_key = %event.routing_key,
                "enqueue raw orderbook outbox failed"
            );
            return;
        }
    }
}

impl PreEnqueueConflator {
    fn new() -> Self {
        Self {
            pending: HashMap::new(),
            suppressed: 0,
            emitted: 0,
        }
    }

    fn offer(&mut self, event: NormalizedMdEvent) -> Option<NormalizedMdEvent> {
        if !WS_PRE_ENQUEUE_CONFLATION_ENABLED
            || !matches!(event.msg_type.as_str(), "md.depth" | "md.bbo")
        {
            return Some(event);
        }

        let slot = event
            .event_ts
            .timestamp_millis()
            .div_euclid(WS_PRE_ENQUEUE_CONFLATION_MS);
        let key = (
            event.market.clone(),
            event.symbol.clone(),
            event.msg_type.clone(),
            event.stream_name.clone(),
        );

        match self.pending.entry(key) {
            Entry::Vacant(v) => {
                v.insert((slot, event, 1));
                None
            }
            Entry::Occupied(mut o) => {
                let (last_slot, last_event, event_count) = o.get_mut();
                if slot > *last_slot {
                    let mut flushed = std::mem::replace(last_event, event);
                    let flushed_count = *event_count;
                    *last_slot = slot;
                    *event_count = 1;
                    mark_pre_enqueue_conflated(&mut flushed, flushed_count);
                    self.emitted = self.emitted.saturating_add(1);
                    Some(flushed)
                } else {
                    if slot == *last_slot {
                        *last_event = event;
                        *event_count = event_count.saturating_add(1);
                    }
                    self.suppressed = self.suppressed.saturating_add(1);
                    if self.suppressed % WS_PRE_ENQUEUE_SUPPRESS_LOG_EVERY == 0 {
                        info!(
                            suppressed = self.suppressed,
                            emitted = self.emitted,
                            window_ms = WS_PRE_ENQUEUE_CONFLATION_MS,
                            "ws pre-enqueue conflation stats"
                        );
                    }
                    None
                }
            }
        }
    }

    fn drain_expired(&mut self, now: DateTime<Utc>) -> Vec<NormalizedMdEvent> {
        let now_slot = now
            .timestamp_millis()
            .div_euclid(WS_PRE_ENQUEUE_CONFLATION_MS);
        let keys = self
            .pending
            .iter()
            .filter_map(|(k, (slot, _, _))| {
                if *slot < now_slot {
                    Some(k.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some((_, mut event, event_count)) = self.pending.remove(&key) {
                mark_pre_enqueue_conflated(&mut event, event_count);
                self.emitted = self.emitted.saturating_add(1);
                out.push(event);
            }
        }
        out
    }

    fn drain_all(&mut self) -> Vec<NormalizedMdEvent> {
        let mut drained = self
            .pending
            .drain()
            .map(|(_, (_, mut event, event_count))| {
                mark_pre_enqueue_conflated(&mut event, event_count);
                event
            })
            .collect::<Vec<_>>();
        drained.shrink_to_fit();
        self.emitted = self.emitted.saturating_add(drained.len() as u64);
        drained
    }
}

async fn enqueue_persist_event(
    persist_queue: &AsyncPersistQueue,
    market: &'static str,
    event: NormalizedMdEvent,
) {
    if let Err(err) = persist_queue.enqueue(event).await {
        error!(
            error = %err,
            market = market,
            "async persist enqueue failed"
        );
    }
}

async fn enqueue_or_persist_event(
    event: &NormalizedMdEvent,
    db_writer: &Arc<MdDbWriter>,
    publisher: &Arc<MqPublisher>,
    outbox_writer: &Arc<OutboxWriter>,
    ops_writer: &Arc<OpsDbWriter>,
    metrics: &Arc<AppMetrics>,
) -> Result<()> {
    match try_enqueue_registered_persist_event(event.market.as_str(), event.clone()).await {
        Ok(true) => Ok(()),
        Ok(false) => {
            persist_event(
                event,
                db_writer,
                publisher,
                outbox_writer,
                ops_writer,
                metrics,
            )
            .await
        }
        Err(err) => {
            warn!(
                error = %err,
                market = %event.market,
                symbol = %event.symbol,
                msg_type = %event.msg_type,
                "registered async persist enqueue failed, fallback to direct persist"
            );
            persist_event(
                event,
                db_writer,
                publisher,
                outbox_writer,
                ops_writer,
                metrics,
            )
            .await
        }
    }
}

pub async fn run(
    ctx: Arc<AppContext>,
    rest_client: Arc<BinanceRestClient>,
    publisher: Arc<MqPublisher>,
    outbox_writer: Arc<OutboxWriter>,
    db_writer: Arc<MdDbWriter>,
    ops_writer: Arc<OpsDbWriter>,
    metrics: Arc<AppMetrics>,
) -> Result<()> {
    let symbol = ctx.config.market_data.symbol.clone();
    let streams = spot_streams::required_streams(&symbol);
    let mut gap_detector = GapDetector::default();
    let kline_3d_aggregator = Arc::new(Mutex::new(Kline3dAggregator::default()));
    let mut last_closed_kline_open_ms: HashMap<String, i64> = HashMap::new();
    let mut reconnect_attempt: u32 = 0;
    let trade_gapfill_inflight = Arc::new(AtomicBool::new(false));
    let reconnect_reconcile_inflight = Arc::new(AtomicBool::new(false));
    let (kline_gap_tx, mut kline_gap_rx) =
        mpsc::channel::<KlineGapReplayJob>(KLINE_GAP_WORKER_QUEUE_CAPACITY);

    {
        let rest_client = Arc::clone(&rest_client);
        let db_writer = Arc::clone(&db_writer);
        let publisher = Arc::clone(&publisher);
        let outbox_writer = Arc::clone(&outbox_writer);
        let ops_writer = Arc::clone(&ops_writer);
        let metrics = Arc::clone(&metrics);
        let kline_3d_aggregator = Arc::clone(&kline_3d_aggregator);
        tokio::spawn(async move {
            while let Some(job) = kline_gap_rx.recv().await {
                if let Err(err) = replay_kline_gap(
                    &job,
                    &rest_client,
                    &db_writer,
                    &publisher,
                    &outbox_writer,
                    &ops_writer,
                    &metrics,
                    &kline_3d_aggregator,
                )
                .await
                {
                    warn!(
                        error = %err,
                        market = %job.market,
                        symbol = %job.symbol,
                        interval = %job.interval_code,
                        start_ms = job.start_ms,
                        end_ms = job.end_ms,
                        "background kline gap replay failed"
                    );
                }
            }
        });
    }

    match checkpoints::load_checkpoint_seeds(&ctx.ops_db_pool, "spot", &symbol).await {
        Ok(seeds) => {
            checkpoints::hydrate_runtime_state(
                "spot",
                &symbol,
                &seeds,
                &mut gap_detector,
                &mut last_closed_kline_open_ms,
            );
            info!(
                count = seeds.len(),
                market = "spot",
                "checkpoint seeds loaded"
            );
        }
        Err(err) => {
            warn!(error = %err, market = "spot", "failed to load checkpoint seeds");
        }
    }

    let persist_queue = AsyncPersistQueue::spawn(
        "spot",
        Arc::clone(&db_writer),
        Arc::clone(&publisher),
        Arc::clone(&outbox_writer),
        Arc::clone(&ops_writer),
        Arc::clone(&metrics),
    );

    loop {
        info!(market = "spot", ?streams, "connecting spot websocket");
        let was_reconnect = reconnect_attempt > 0;
        let mut ws = match connect_combined_stream(
            SPOT_WS_BASE,
            &streams,
            ctx.ws_proxy_url.as_deref(),
        )
        .await
        {
            Ok(ws) => {
                reconnect_attempt = 0;
                ws
            }
            Err(err) => {
                error!(error = %err, "spot websocket connect failed, retrying");
                let sleep_for = reconnect::next_backoff(reconnect_attempt);
                reconnect_attempt = reconnect_attempt.saturating_add(1);
                tokio::time::sleep(sleep_for).await;
                continue;
            }
        };

        info!(market = "spot", "spot websocket connected");

        if was_reconnect {
            let last_trade_agg_id = gap_detector.last_trade_agg_id("spot", &symbol);
            let kline_state_snapshot = last_closed_kline_open_ms.clone();
            let kline_state_count = kline_state_snapshot.len();
            let kline_3d_snapshot = {
                let aggregator = kline_3d_aggregator.lock().await;
                aggregator.clone()
            };

            if reconnect_reconcile_inflight
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let rest_client = Arc::clone(&rest_client);
                let db_writer = Arc::clone(&db_writer);
                let publisher = Arc::clone(&publisher);
                let outbox_writer = Arc::clone(&outbox_writer);
                let ops_writer = Arc::clone(&ops_writer);
                let metrics = Arc::clone(&metrics);
                let inflight = Arc::clone(&reconnect_reconcile_inflight);
                let symbol = symbol.clone();
                tokio::spawn(async move {
                    let started_at = Instant::now();
                    info!(
                        market = "spot",
                        last_trade_agg_id = last_trade_agg_id,
                        kline_state_count = kline_state_count,
                        "starting background reconnect reconcile"
                    );
                    let result = reconcile_after_ws_reconnect(
                        &symbol,
                        &rest_client,
                        &db_writer,
                        &publisher,
                        &outbox_writer,
                        &ops_writer,
                        &metrics,
                        last_trade_agg_id,
                        kline_state_snapshot,
                        kline_3d_snapshot,
                    )
                    .await;
                    match result {
                        Ok(()) => {
                            info!(
                                market = "spot",
                                elapsed_ms = started_at.elapsed().as_millis(),
                                "background reconnect reconcile completed"
                            );
                        }
                        Err(err) => {
                            warn!(
                                error = %err,
                                market = "spot",
                                elapsed_ms = started_at.elapsed().as_millis(),
                                "background reconnect reconcile failed"
                            );
                        }
                    }
                    inflight.store(false, Ordering::Release);
                });
            } else {
                warn!(
                    market = "spot",
                    "background reconnect reconcile already running, skip new schedule"
                );
            }
        }

        let mut ping_interval = tokio::time::interval(Duration::from_secs(WS_KEEPALIVE_PING_SECS));
        ping_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ping_interval.tick().await;
        let mut stats_interval = tokio::time::interval(Duration::from_secs(WS_STATS_INTERVAL_SECS));
        stats_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        stats_interval.tick().await;
        let mut last_recv_at = Instant::now();
        let mut stale_source_streak: u64 = 0;
        let mut stale_source_seen: u64 = 0;
        let mut ws_read_total: u64 = 0;
        let mut ws_drop_total: u64 = 0;
        let mut ws_read_per_sec: u64 = 0;
        let mut ws_drop_per_sec: u64 = 0;
        let mut ws_source_lag_per_sec: u64 = 0;
        let mut ws_drop_decode_per_sec: u64 = 0;
        let mut ws_drop_normalize_per_sec: u64 = 0;
        let mut ws_preagg = WsOneSecondPreAggregator::default();
        let mut reconnect_reason = "unknown";

        loop {
            tokio::select! {
                _ = stats_interval.tick() => {
                    for event in ws_preagg.flush_expired(Utc::now()) {
                        enqueue_persist_event(&persist_queue, "spot", event).await;
                    }
                    let persist_queue_pending = persist_queue.pending();
                    let persist_trade_pending = persist_queue.trade_pending();
                    let persist_non_trade_pending = persist_queue.non_trade_pending();
                    metrics.set_queue_lag(persist_queue_pending as i64);
                    if ws_drop_per_sec > 0
                        || stale_source_streak > 0
                        || persist_queue_pending >= WS_PERSIST_QUEUE_CONGESTED_THRESHOLD
                    {
                        warn!(
                            market = "spot",
                            ws_read_per_sec = ws_read_per_sec,
                            ws_drop_per_sec = ws_drop_per_sec,
                            ws_source_lag_per_sec = ws_source_lag_per_sec,
                            ws_drop_decode_per_sec = ws_drop_decode_per_sec,
                            ws_drop_normalize_per_sec = ws_drop_normalize_per_sec,
                            stale_source_streak = stale_source_streak,
                            stale_source_seen = stale_source_seen,
                            persist_queue_pending = persist_queue_pending,
                            persist_trade_pending = persist_trade_pending,
                            persist_non_trade_pending = persist_non_trade_pending,
                            "ws main loop per-second stats"
                        );
                    }
                    ws_read_per_sec = 0;
                    ws_drop_per_sec = 0;
                    ws_source_lag_per_sec = 0;
                    ws_drop_decode_per_sec = 0;
                    ws_drop_normalize_per_sec = 0;
                }
                _ = ping_interval.tick() => {
                    if let Err(err) = ws.send(Message::Ping(Vec::new().into())).await {
                        reconnect_reason = "keepalive_ping_failed";
                        warn!(error = %err, market = "spot", "spot websocket keepalive ping failed");
                        break;
                    }
                    debug!(market = "spot", "spot websocket keepalive ping sent");

                    let idle = last_recv_at.elapsed();
                    if idle >= Duration::from_secs(WS_IDLE_TIMEOUT_SECS) {
                        reconnect_reason = "idle_timeout";
                        warn!(
                            market = "spot",
                            idle_secs = idle.as_secs(),
                            timeout_secs = WS_IDLE_TIMEOUT_SECS,
                            "spot websocket idle timeout reached, reconnecting"
                        );
                        break;
                    }
                }
                frame = ws.next() => {
                    let Some(frame) = frame else {
                        reconnect_reason = "stream_ended";
                        warn!(market = "spot", "spot websocket stream ended");
                        break;
                    };

                    match frame {
                        Ok(Message::Text(text)) => {
                            last_recv_at = Instant::now();
                            ws_read_total = ws_read_total.saturating_add(1);
                            ws_read_per_sec = ws_read_per_sec.saturating_add(1);
                            let combined = match decode_text_message(&text) {
                                Ok(v) => v,
                                Err(err) => {
                                    metrics.inc_normalize_error();
                                    ws_drop_total = ws_drop_total.saturating_add(1);
                                    ws_drop_per_sec = ws_drop_per_sec.saturating_add(1);
                                    ws_drop_decode_per_sec = ws_drop_decode_per_sec.saturating_add(1);
                                    warn!(error = %err, "spot decode message failed");
                            continue;
                        }
                    };

                    let event = match normalize::normalize_ws_event(
                        Market::Spot,
                        &combined.stream,
                        &combined.data,
                    ) {
                        Ok(Some(event)) => event,
                        Ok(None) => continue,
                        Err(err) => {
                            metrics.inc_normalize_error();
                            ws_drop_total = ws_drop_total.saturating_add(1);
                            ws_drop_per_sec = ws_drop_per_sec.saturating_add(1);
                            ws_drop_normalize_per_sec =
                                ws_drop_normalize_per_sec.saturating_add(1);
                            warn!(error = %err, stream = %combined.stream, "spot normalize failed");
                            continue;
                        }
                    };

                    let source_lag_secs = (Utc::now() - event.event_ts).num_seconds();
                    let mut force_reconnect_after_event = false;
                    if !event.backfill_in_progress && source_lag_secs > SOURCE_LAG_WARN_SECS {
                        stale_source_streak = stale_source_streak.saturating_add(1);
                        stale_source_seen = stale_source_seen.saturating_add(1);
                        ws_source_lag_per_sec = ws_source_lag_per_sec.saturating_add(1);
                        if stale_source_streak == 1
                            || stale_source_streak % SOURCE_LAG_LOG_EVERY == 0
                        {
                            warn!(
                                market = %event.market,
                                symbol = %event.symbol,
                                msg_type = %event.msg_type,
                                stream_name = %event.stream_name,
                                source_kind = %event.source_kind,
                                source_lag_secs = source_lag_secs,
                                stale_source_streak = stale_source_streak,
                                stale_source_seen = stale_source_seen,
                                event_ts = %event.event_ts,
                                now_ts = %Utc::now(),
                                "source lag before persist"
                            );
                        }
                        if source_lag_secs >= SOURCE_LAG_FORCE_RECONNECT_SECS
                            && stale_source_streak >= SOURCE_LAG_FORCE_RECONNECT_STREAK
                        {
                            force_reconnect_after_event = true;
                            warn!(
                                market = %event.market,
                                symbol = %event.symbol,
                                source_lag_secs = source_lag_secs,
                                stale_source_streak = stale_source_streak,
                                stale_source_seen = stale_source_seen,
                                force_reconnect_after_secs = SOURCE_LAG_FORCE_RECONNECT_SECS,
                                "stale ws source detected, schedule reconnect after current event persistence"
                            );
                        }
                    } else if stale_source_streak > 0 {
                        info!(
                            market = %event.market,
                            symbol = %event.symbol,
                            recovered_source_lag_secs = source_lag_secs,
                            stale_source_seen = stale_source_seen,
                            "ws source lag recovered"
                        );
                        stale_source_streak = 0;
                        stale_source_seen = 0;
                    }

                    handle_trade_gap(
                        &event,
                        &mut gap_detector,
                        &rest_client,
                        &db_writer,
                        &publisher,
                        &outbox_writer,
                        &ops_writer,
                        &metrics,
                        &trade_gapfill_inflight,
                    )
                    .await;

                    handle_depth_gap(&event, &mut gap_detector, &ops_writer).await;

                    handle_kline_gap(
                        &event,
                        &mut last_closed_kline_open_ms,
                        &kline_3d_aggregator,
                        &kline_gap_tx,
                        &db_writer,
                        &publisher,
                        &outbox_writer,
                        &ops_writer,
                        &metrics,
                    )
                    .await;

                    maybe_mirror_raw_orderbook_to_outbox(
                        &event,
                        &publisher,
                        &outbox_writer,
                        &metrics,
                        "spot",
                    )
                    .await;

                    mirror_raw_event_to_cold_store(&event);
                    for queued in ws_preagg.ingest_raw_event(event) {
                        enqueue_persist_event(&persist_queue, "spot", queued).await;
                    }
                    if force_reconnect_after_event {
                        reconnect_reason = "stale_source_force_reconnect";
                        break;
                    }
                        }
                        Ok(Message::Ping(payload)) => {
                            last_recv_at = Instant::now();
                            if let Err(err) = ws.send(Message::Pong(payload)).await {
                                warn!(error = %err, "spot pong failed");
                                break;
                            }
                            debug!(market = "spot", "spot websocket pong sent");
                        }
                        Ok(Message::Pong(_)) => {
                            last_recv_at = Instant::now();
                            debug!(market = "spot", "spot websocket pong received");
                        }
                        Ok(msg) if is_terminal_message(&msg) => {
                            last_recv_at = Instant::now();
                            reconnect_reason = "server_close_frame";
                            warn!(market = "spot", "spot websocket closed by server");
                            break;
                        }
                        Ok(_) => {
                            last_recv_at = Instant::now();
                        }
                        Err(err) => {
                            let err_text = err.to_string();
                            reconnect_reason = "ws_receive_error";
                            let likely_local_stack_abort =
                                err_text.contains("10053")
                                || err_text.contains("10054")
                                || err_text.contains("Connection reset without closing handshake")
                                || err_text.contains("close_notify");
                            warn!(
                                market = "spot",
                                proxy_enabled = ctx.ws_proxy_url.is_some(),
                                reconnect_attempt = reconnect_attempt,
                                idle_secs = last_recv_at.elapsed().as_secs(),
                                keepalive_ping_secs = WS_KEEPALIVE_PING_SECS,
                                idle_timeout_secs = WS_IDLE_TIMEOUT_SECS,
                                likely_local_stack_abort = likely_local_stack_abort,
                                error = %err,
                                error_text = %err_text,
                                "spot websocket receive error"
                            );
                            break;
                        }
                    }
                }
            }
        }

        for event in ws_preagg.flush_all() {
            enqueue_persist_event(&persist_queue, "spot", event).await;
        }

        let sleep_for = reconnect::next_backoff(reconnect_attempt);
        info!(
            market = "spot",
            reconnect_reason = reconnect_reason,
            ws_read_total = ws_read_total,
            ws_drop_total = ws_drop_total,
            stale_source_seen = stale_source_seen,
            reconnect_attempt = reconnect_attempt,
            backoff_secs = sleep_for.as_secs(),
            "websocket reconnect scheduled"
        );
        reconnect_attempt = reconnect_attempt.saturating_add(1);
        tokio::time::sleep(sleep_for).await;
    }
}

async fn handle_trade_gap(
    event: &NormalizedMdEvent,
    gap_detector: &mut GapDetector,
    rest_client: &Arc<BinanceRestClient>,
    db_writer: &Arc<MdDbWriter>,
    publisher: &Arc<MqPublisher>,
    outbox_writer: &Arc<OutboxWriter>,
    ops_writer: &Arc<OpsDbWriter>,
    metrics: &Arc<AppMetrics>,
    trade_gapfill_inflight: &Arc<AtomicBool>,
) {
    if event.msg_type != "md.trade" {
        return;
    }

    let agg_trade_id = event
        .data
        .get("agg_trade_id")
        .and_then(serde_json::Value::as_i64);
    if let Some(gap) =
        gap_detector.detect_trade_gap(&event.market, &event.symbol, agg_trade_id, event.event_ts)
    {
        {
            let ops_writer = Arc::clone(ops_writer);
            let market = event.market.clone();
            let symbol = event.symbol.clone();
            let stream_name = event.stream_name.clone();
            let (last_event_ts, current_event_ts) = (gap.last_event_ts, gap.current_event_ts);
            let (gap_from, gap_to) = (gap.gap_from, gap.gap_to);
            let dataset_name = gap.dataset_name.clone();
            tokio::spawn(async move {
                if let Err(err) = ops_writer
                    .insert_data_gap(
                        &market,
                        &symbol,
                        &dataset_name,
                        &stream_name,
                        last_event_ts,
                        current_event_ts,
                        "agg_trade_id_gap_detected",
                        json!({ "gap_from": gap_from, "gap_to": gap_to }),
                    )
                    .await
                {
                    warn!("insert trade gap failed: {:#}", err);
                }
            });
        }

        if trade_gapfill_inflight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            warn!(
                market = %event.market,
                symbol = %event.symbol,
                gap_from = gap.gap_from,
                gap_to = gap.gap_to,
                "trade gap replay already running, skip overlapping replay request"
            );
            return;
        }

        let event_owned = event.clone();
        let rest_client = Arc::clone(rest_client);
        let db_writer = Arc::clone(db_writer);
        let publisher = Arc::clone(publisher);
        let outbox_writer = Arc::clone(outbox_writer);
        let ops_writer = Arc::clone(ops_writer);
        let metrics = Arc::clone(metrics);
        let inflight = Arc::clone(trade_gapfill_inflight);
        let gap_from = gap.gap_from;
        let gap_to = gap.gap_to;

        tokio::spawn(async move {
            let result = replay_trade_gap(
                &event_owned,
                gap_from,
                gap_to,
                &rest_client,
                &db_writer,
                &publisher,
                &outbox_writer,
                &ops_writer,
                &metrics,
            )
            .await;
            if let Err(err) = result {
                warn!(
                    error = %err,
                    market = %event_owned.market,
                    symbol = %event_owned.symbol,
                    gap_from = gap_from,
                    gap_to = gap_to,
                    "replay trade gap failed"
                );
            }
            inflight.store(false, Ordering::Release);
        });
    }
}

async fn replay_trade_gap(
    event: &NormalizedMdEvent,
    gap_from: i64,
    gap_to: i64,
    rest_client: &Arc<BinanceRestClient>,
    db_writer: &Arc<MdDbWriter>,
    publisher: &Arc<MqPublisher>,
    outbox_writer: &Arc<OutboxWriter>,
    ops_writer: &Arc<OpsDbWriter>,
    metrics: &Arc<AppMetrics>,
) -> Result<()> {
    let mut next = gap_from;
    let mut rows_written: i64 = 0;
    let mut throttle = BackfillThrottle::new("spot", BACKFILL_TRADE_THROTTLE);

    while next <= gap_to {
        let rows = rest_client
            .fetch_agg_trades(&event.market, &event.symbol, Some(next), REST_PAGE_LIMIT)
            .await?;
        if rows.is_empty() {
            break;
        }

        for row in &rows {
            if row.agg_trade_id < gap_from {
                continue;
            }
            if row.agg_trade_id > gap_to {
                next = gap_to + 1;
                break;
            }

            throttle.before_emit().await;
            let market = if event.market == "spot" {
                Market::Spot
            } else {
                Market::Futures
            };
            let replay_event = trade_normalizer::normalize_rest_agg_trade(
                market,
                &event.symbol,
                row,
                "replay",
                true,
            )?;
            enqueue_or_persist_event(
                &replay_event,
                db_writer,
                publisher,
                outbox_writer,
                ops_writer,
                metrics,
            )
            .await?;
            rows_written += 1;
            next = row.agg_trade_id + 1;
        }

        if rows.len() < REST_PAGE_LIMIT as usize {
            break;
        }
    }

    ops_writer
        .insert_backfill_job_run(
            Some(&event.market),
            Some(&event.symbol),
            "/api/v3/aggTrades",
            "trade_gapfill",
            "gap",
            json!({ "from_id": gap_from, "to_id": gap_to }),
            Some(rows_written),
            "success",
            None,
        )
        .await?;

    Ok(())
}

async fn handle_depth_gap(
    event: &NormalizedMdEvent,
    gap_detector: &mut GapDetector,
    ops_writer: &Arc<OpsDbWriter>,
) {
    if event.msg_type != "md.depth" {
        return;
    }

    let first_update_id = event
        .data
        .get("first_update_id")
        .and_then(serde_json::Value::as_i64);
    let final_update_id = event
        .data
        .get("final_update_id")
        .and_then(serde_json::Value::as_i64);
    let prev_final_update_id = event
        .data
        .get("prev_final_update_id")
        .and_then(serde_json::Value::as_i64);
    if let Some(gap) = gap_detector.detect_depth_gap(
        &event.market,
        &event.symbol,
        first_update_id,
        final_update_id,
        prev_final_update_id,
        event.event_ts,
    ) {
        let ops_writer = Arc::clone(ops_writer);
        let market = event.market.clone();
        let symbol = event.symbol.clone();
        let stream_name = event.stream_name.clone();
        let (last_event_ts, current_event_ts) = (gap.last_event_ts, gap.current_event_ts);
        let (gap_from, gap_to) = (gap.gap_from, gap.gap_to);
        let dataset_name = gap.dataset_name.clone();
        tokio::spawn(async move {
            if let Err(err) = ops_writer
                .insert_data_gap(
                    &market,
                    &symbol,
                    &dataset_name,
                    &stream_name,
                    last_event_ts,
                    current_event_ts,
                    "depth_update_id_gap_detected",
                    json!({ "gap_from": gap_from, "gap_to": gap_to }),
                )
                .await
            {
                warn!("insert depth gap failed: {:#}", err);
            }
        });

        let requested = depth_rebuilder::request_immediate_depth_reconcile(Market::Spot);
        if requested {
            info!(
                market = %event.market,
                symbol = %event.symbol,
                stream_name = %event.stream_name,
                "depth gap detected, requested immediate snapshot reconcile"
            );
        } else {
            debug!(
                market = %event.market,
                symbol = %event.symbol,
                stream_name = %event.stream_name,
                "depth gap detected, immediate snapshot reconcile request already pending/unavailable"
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_kline_gap(
    event: &NormalizedMdEvent,
    last_closed_kline_open_ms: &mut HashMap<String, i64>,
    kline_3d_aggregator: &Arc<Mutex<Kline3dAggregator>>,
    kline_gap_tx: &mpsc::Sender<KlineGapReplayJob>,
    db_writer: &Arc<MdDbWriter>,
    publisher: &Arc<MqPublisher>,
    outbox_writer: &Arc<OutboxWriter>,
    ops_writer: &Arc<OpsDbWriter>,
    metrics: &Arc<AppMetrics>,
) {
    if event.msg_type != "md.kline" {
        return;
    }

    let interval_code = match event
        .data
        .get("interval_code")
        .and_then(serde_json::Value::as_str)
    {
        Some(v) => v.to_string(),
        None => return,
    };
    let is_closed = event
        .data
        .get("is_closed")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    if !is_closed {
        return;
    }
    if interval_code == "3d" {
        return;
    }

    let open_time = match parse_rfc3339(
        event
            .data
            .get("open_time")
            .and_then(serde_json::Value::as_str),
    ) {
        Some(ts) => ts.timestamp_millis(),
        None => return,
    };

    if let Some(last_open) = last_closed_kline_open_ms.get(&interval_code).copied() {
        let step = interval_ms(&interval_code).unwrap_or(0);
        if step > 0 && open_time > last_open + step {
            let start = last_open + step;
            let end = open_time - step;
            let job = KlineGapReplayJob {
                market: event.market.clone(),
                symbol: event.symbol.clone(),
                interval_code: interval_code.clone(),
                start_ms: start,
                end_ms: end,
            };
            match kline_gap_tx.try_send(job) {
                Ok(()) => {}
                Err(TrySendError::Full(job)) => {
                    warn!(
                        market = %job.market,
                        symbol = %job.symbol,
                        interval = %job.interval_code,
                        start_ms = job.start_ms,
                        end_ms = job.end_ms,
                        queue_capacity = KLINE_GAP_WORKER_QUEUE_CAPACITY,
                        "background kline gap replay queue full, skip hot-path enqueue"
                    );
                }
                Err(TrySendError::Closed(job)) => {
                    warn!(
                        market = %job.market,
                        symbol = %job.symbol,
                        interval = %job.interval_code,
                        start_ms = job.start_ms,
                        end_ms = job.end_ms,
                        "background kline gap replay queue closed, skip hot-path enqueue"
                    );
                }
            }
        }
    }
    last_closed_kline_open_ms.insert(interval_code, open_time);

    let maybe_derived = {
        let mut aggregator = kline_3d_aggregator.lock().await;
        aggregator.on_kline(event)
    };
    match maybe_derived {
        Ok(Some(derived_3d)) => {
            if let Err(err) = persist_event(
                &derived_3d,
                db_writer,
                publisher,
                outbox_writer,
                ops_writer,
                metrics,
            )
            .await
            {
                warn!(error = %err, "persist derived 3d kline failed");
            }
        }
        Ok(None) => {}
        Err(err) => {
            warn!(error = %err, "update 3d kline aggregator failed");
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn replay_kline_gap(
    job: &KlineGapReplayJob,
    rest_client: &Arc<BinanceRestClient>,
    db_writer: &Arc<MdDbWriter>,
    publisher: &Arc<MqPublisher>,
    outbox_writer: &Arc<OutboxWriter>,
    ops_writer: &Arc<OpsDbWriter>,
    metrics: &Arc<AppMetrics>,
    kline_3d_aggregator: &Arc<Mutex<Kline3dAggregator>>,
) -> Result<()> {
    let step = interval_ms(&job.interval_code).unwrap_or(60_000);
    let mut cursor_ms = job.start_ms;
    let mut written = 0i64;
    let mut throttle = BackfillThrottle::new("spot", BACKFILL_KLINE_THROTTLE);
    while cursor_ms <= job.end_ms {
        let rows = rest_client
            .fetch_klines(
                &job.market,
                &job.symbol,
                &job.interval_code,
                Some(cursor_ms),
                Some(job.end_ms),
                REST_PAGE_LIMIT,
            )
            .await?;
        if rows.is_empty() {
            break;
        }

        let mut advanced = false;
        for row in &rows {
            if row.open_time_ms < cursor_ms {
                continue;
            }
            if row.open_time_ms > job.end_ms {
                cursor_ms = job.end_ms + step;
                break;
            }

            let market = if job.market == "spot" {
                Market::Spot
            } else {
                Market::Futures
            };
            let replay_event = kline_normalizer::normalize_rest_kline(
                market,
                &job.symbol,
                &job.interval_code,
                row,
                "replay",
                true,
            )?;
            throttle.before_emit().await;
            persist_event(
                &replay_event,
                db_writer,
                publisher,
                outbox_writer,
                ops_writer,
                metrics,
            )
            .await?;
            let maybe_derived = {
                let mut aggregator = kline_3d_aggregator.lock().await;
                aggregator.on_kline(&replay_event)
            };
            match maybe_derived {
                Ok(Some(derived_3d)) => {
                    throttle.before_emit().await;
                    persist_event(
                        &derived_3d,
                        db_writer,
                        publisher,
                        outbox_writer,
                        ops_writer,
                        metrics,
                    )
                    .await?;
                }
                Ok(None) => {}
                Err(err) => {
                    warn!(error = %err, "update 3d kline aggregator failed during gap replay");
                }
            }
            cursor_ms = row.open_time_ms + step;
            written += 1;
            advanced = true;
        }

        if rows.len() < REST_PAGE_LIMIT as usize || !advanced {
            break;
        }
    }

    ops_writer
        .insert_backfill_job_run(
            Some(&job.market),
            Some(&job.symbol),
            "/api/v3/klines",
            "kline_gapfill",
            "gap",
            json!({ "interval": job.interval_code, "start_ms": job.start_ms, "end_ms": job.end_ms }),
            Some(written),
            "success",
            None,
        )
        .await?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn reconcile_after_ws_reconnect(
    symbol: &str,
    rest_client: &Arc<BinanceRestClient>,
    db_writer: &Arc<MdDbWriter>,
    publisher: &Arc<MqPublisher>,
    outbox_writer: &Arc<OutboxWriter>,
    ops_writer: &Arc<OpsDbWriter>,
    metrics: &Arc<AppMetrics>,
    last_trade_agg_id: Option<i64>,
    mut last_closed_kline_open_ms: HashMap<String, i64>,
    mut kline_3d_aggregator: Kline3dAggregator,
) -> Result<()> {
    if let Some(last_agg_trade_id) = last_trade_agg_id {
        let from_id = last_agg_trade_id + 1;
        let mut next_id = from_id;
        let mut written = 0i64;
        let mut trade_throttle = BackfillThrottle::new("spot", BACKFILL_TRADE_THROTTLE);
        loop {
            let rows = rest_client
                .fetch_agg_trades("spot", symbol, Some(next_id), REST_PAGE_LIMIT)
                .await?;
            if rows.is_empty() {
                break;
            }

            let mut advanced = false;
            for row in &rows {
                if row.agg_trade_id < next_id {
                    continue;
                }

                trade_throttle.before_emit().await;
                let replay_event = trade_normalizer::normalize_rest_agg_trade(
                    Market::Spot,
                    symbol,
                    row,
                    "replay",
                    true,
                )?;
                enqueue_or_persist_event(
                    &replay_event,
                    db_writer,
                    publisher,
                    outbox_writer,
                    ops_writer,
                    metrics,
                )
                .await?;
                next_id = row.agg_trade_id + 1;
                written += 1;
                advanced = true;
            }

            if rows.len() < REST_PAGE_LIMIT as usize || !advanced {
                break;
            }
        }

        if written > 0 {
            ops_writer
                .insert_backfill_job_run(
                    Some("spot"),
                    Some(symbol),
                    "/api/v3/aggTrades",
                    "trade_gapfill",
                    "reconnect",
                    json!({ "from_id": from_id, "to_id": next_id - 1 }),
                    Some(written),
                    "success",
                    None,
                )
                .await?;
        }
    }

    for interval_code in KLINE_INTERVALS {
        let Some(last_open) = last_closed_kline_open_ms.get(interval_code).copied() else {
            continue;
        };
        let Some(step) = interval_ms(interval_code) else {
            continue;
        };
        let start_ms = last_open + step;
        let end_ms = Utc::now().timestamp_millis();
        if start_ms >= end_ms {
            continue;
        }

        let mut cursor_ms = start_ms;
        let mut written = 0i64;
        let mut kline_throttle = BackfillThrottle::new("spot", BACKFILL_KLINE_THROTTLE);
        while cursor_ms <= end_ms {
            let rows = rest_client
                .fetch_klines(
                    "spot",
                    symbol,
                    interval_code,
                    Some(cursor_ms),
                    Some(end_ms),
                    REST_PAGE_LIMIT,
                )
                .await?;
            if rows.is_empty() {
                break;
            }

            let mut advanced = false;
            for row in &rows {
                if row.open_time_ms < cursor_ms {
                    continue;
                }
                if row.open_time_ms > end_ms {
                    cursor_ms = end_ms + step;
                    break;
                }

                let replay_event = kline_normalizer::normalize_rest_kline(
                    Market::Spot,
                    symbol,
                    interval_code,
                    row,
                    "replay",
                    true,
                )?;
                kline_throttle.before_emit().await;
                persist_event(
                    &replay_event,
                    db_writer,
                    publisher,
                    outbox_writer,
                    ops_writer,
                    metrics,
                )
                .await?;
                let maybe_derived = kline_3d_aggregator.on_kline(&replay_event);
                if let Ok(Some(derived_3d)) = maybe_derived {
                    kline_throttle.before_emit().await;
                    persist_event(
                        &derived_3d,
                        db_writer,
                        publisher,
                        outbox_writer,
                        ops_writer,
                        metrics,
                    )
                    .await?;
                }

                last_closed_kline_open_ms.insert(interval_code.to_string(), row.open_time_ms);
                cursor_ms = row.open_time_ms + step;
                written += 1;
                advanced = true;
            }

            if rows.len() < REST_PAGE_LIMIT as usize || !advanced {
                break;
            }
        }

        if written > 0 {
            ops_writer
                .insert_backfill_job_run(
                    Some("spot"),
                    Some(symbol),
                    "/api/v3/klines",
                    "kline_gapfill",
                    "reconnect",
                    json!({ "interval": interval_code, "start_ms": start_ms, "end_ms": end_ms }),
                    Some(written),
                    "success",
                    None,
                )
                .await?;
        }
    }

    Ok(())
}

fn interval_ms(interval: &str) -> Option<i64> {
    match interval {
        "1m" => Some(60_000),
        "15m" => Some(900_000),
        "1h" => Some(3_600_000),
        "4h" => Some(14_400_000),
        "1d" => Some(86_400_000),
        "3d" => Some(259_200_000),
        _ => None,
    }
}

fn parse_rfc3339(raw: Option<&str>) -> Option<DateTime<Utc>> {
    raw.and_then(|s| {
        DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
    })
}
