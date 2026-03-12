use crate::app::bootstrap::AppContext;
use crate::exchange::binance::rest::client::BinanceRestClient;
use crate::normalize::{normalize_orderbook_snapshot_l2, Market};
use crate::observability::metrics::AppMetrics;
use crate::pipelines::persist_async;
use crate::sinks::{
    md_db_writer::MdDbWriter, mq_publisher::MqPublisher, ops_db_writer::OpsDbWriter,
    outbox_writer::OutboxWriter,
};
use anyhow::Result;
use chrono::Utc;
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{interval, sleep, MissedTickBehavior};
use tracing::{debug, error, info, warn};

const SYMBOL: &str = "ETHUSDT";
const SNAPSHOT_DEPTH_LEVELS: u16 = 200;
const SNAPSHOT_INTERVAL_SECS: u64 = 30;
const DEPTH_GAP_RECONCILE_REASON: &str = "depth_gap_reconcile";

#[derive(Debug, Clone, Copy)]
struct DepthSnapshotRequest {
    market: Market,
    reason: &'static str,
}

static DEPTH_SNAPSHOT_REQUEST_TX: OnceLock<mpsc::UnboundedSender<DepthSnapshotRequest>> =
    OnceLock::new();
static DEPTH_SNAPSHOT_PENDING_SPOT: AtomicBool = AtomicBool::new(false);
static DEPTH_SNAPSHOT_PENDING_FUTURES: AtomicBool = AtomicBool::new(false);

fn pending_flag_for_market(market: Market) -> &'static AtomicBool {
    match market {
        Market::Spot => &DEPTH_SNAPSHOT_PENDING_SPOT,
        Market::Futures => &DEPTH_SNAPSHOT_PENDING_FUTURES,
    }
}

pub fn request_immediate_depth_reconcile(market: Market) -> bool {
    let pending_flag = pending_flag_for_market(market);
    if pending_flag
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return false;
    }

    let Some(tx) = DEPTH_SNAPSHOT_REQUEST_TX.get() else {
        pending_flag.store(false, Ordering::Release);
        return false;
    };

    if tx
        .send(DepthSnapshotRequest {
            market,
            reason: DEPTH_GAP_RECONCILE_REASON,
        })
        .is_err()
    {
        pending_flag.store(false, Ordering::Release);
        return false;
    }

    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DepthSyncState {
    Init,
    SnapshotSyncing,
    LiveSynced,
    Desync,
}

impl DepthSyncState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Init => "INIT",
            Self::SnapshotSyncing => "SNAPSHOT_SYNCING",
            Self::LiveSynced => "LIVE_SYNCED",
            Self::Desync => "DESYNC",
        }
    }
}

pub async fn run_depth_snapshot_loop(
    _ctx: Arc<AppContext>,
    rest_client: Arc<BinanceRestClient>,
    db_writer: Arc<MdDbWriter>,
    ops_writer: Arc<OpsDbWriter>,
    publisher: Arc<MqPublisher>,
    outbox_writer: Arc<OutboxWriter>,
    metrics: Arc<AppMetrics>,
) -> Result<()> {
    // Prefer queue-path snapshots so orderbook 1s pre-aggregator state is kept in sync.
    // On startup, pipelines register queues quickly; wait briefly to avoid falling back
    // to direct persist path which bypasses queue state.
    let wait_started = std::time::Instant::now();
    while (!persist_async::has_registered_persist_queue("spot")
        || !persist_async::has_registered_persist_queue("futures"))
        && wait_started.elapsed() < Duration::from_secs(5)
    {
        sleep(Duration::from_millis(200)).await;
    }

    let mut spot_state = DepthSyncState::Init;
    let mut futures_state = DepthSyncState::Init;
    let mut snapshot_seq: i64 = 0;
    let (request_tx, mut request_rx) = mpsc::unbounded_channel::<DepthSnapshotRequest>();
    if DEPTH_SNAPSHOT_REQUEST_TX.set(request_tx).is_err() {
        warn!("depth snapshot request channel already initialized");
    }

    let mut ticker = interval(Duration::from_secs(SNAPSHOT_INTERVAL_SECS));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    info!(
        symbol = SYMBOL,
        depth_levels = SNAPSHOT_DEPTH_LEVELS,
        interval_secs = SNAPSHOT_INTERVAL_SECS,
        "depth snapshot rebuilder started"
    );

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                run_one_market(
                    Market::Spot,
                    "periodic_refresh",
                    &mut spot_state,
                    &mut snapshot_seq,
                    &rest_client,
                    &db_writer,
                    &ops_writer,
                    &publisher,
                    &outbox_writer,
                    &metrics,
                )
                .await;

                run_one_market(
                    Market::Futures,
                    "periodic_refresh",
                    &mut futures_state,
                    &mut snapshot_seq,
                    &rest_client,
                    &db_writer,
                    &ops_writer,
                    &publisher,
                    &outbox_writer,
                    &metrics,
                )
                .await;
            }
            maybe_req = request_rx.recv() => {
                let Some(req) = maybe_req else {
                    continue;
                };
                let state_ref = match req.market {
                    Market::Spot => &mut spot_state,
                    Market::Futures => &mut futures_state,
                };
                run_one_market(
                    req.market,
                    req.reason,
                    state_ref,
                    &mut snapshot_seq,
                    &rest_client,
                    &db_writer,
                    &ops_writer,
                    &publisher,
                    &outbox_writer,
                    &metrics,
                )
                .await;
                pending_flag_for_market(req.market).store(false, Ordering::Release);
            }
        }
    }
}

async fn run_one_market(
    market: Market,
    trigger_reason: &'static str,
    state: &mut DepthSyncState,
    snapshot_seq: &mut i64,
    rest_client: &Arc<BinanceRestClient>,
    _db_writer: &Arc<MdDbWriter>,
    ops_writer: &Arc<OpsDbWriter>,
    _publisher: &Arc<MqPublisher>,
    _outbox_writer: &Arc<OutboxWriter>,
    _metrics: &Arc<AppMetrics>,
) {
    let prev_state = *state;
    let was_desync = prev_state == DepthSyncState::Desync;
    let is_periodic_refresh =
        trigger_reason == "periodic_refresh" && prev_state == DepthSyncState::LiveSynced;
    transition_state(
        market,
        state,
        DepthSyncState::SnapshotSyncing,
        if trigger_reason == DEPTH_GAP_RECONCILE_REASON {
            "start snapshot sync (gap-triggered)"
        } else {
            "start snapshot sync"
        },
        is_periodic_refresh,
    );

    let fetch_result = rest_client
        .fetch_depth_snapshot(
            market.as_str(),
            SYMBOL,
            request_depth_limit(market, SNAPSHOT_DEPTH_LEVELS),
        )
        .await;

    match fetch_result {
        Ok(mut snapshot) => {
            *snapshot_seq += 1;
            let request_limit = request_depth_limit(market, SNAPSHOT_DEPTH_LEVELS);
            let desired_levels = SNAPSHOT_DEPTH_LEVELS as usize;

            if snapshot.bids.len() > desired_levels {
                snapshot.bids.truncate(desired_levels);
            }
            if snapshot.asks.len() > desired_levels {
                snapshot.asks.truncate(desired_levels);
            }

            let now = Utc::now();
            let build_reason = if was_desync {
                "desync_rebuild"
            } else {
                trigger_reason
            };
            let metadata = json!({
                "build_reason": build_reason,
                "source_depth_stream": format!("{}@depth@100ms", SYMBOL.to_lowercase()),
                "snapshot_seq": *snapshot_seq,
                "state_before_sync": prev_state.as_str(),
                "request_limit": request_limit,
                "target_depth_levels": SNAPSHOT_DEPTH_LEVELS,
                "actual_bid_levels": snapshot.bids.len(),
                "actual_ask_levels": snapshot.asks.len(),
            });

            let event = normalize_orderbook_snapshot_l2(
                market,
                SYMBOL,
                &format!("orderbook_snapshot_l2@derived_{}", SNAPSHOT_DEPTH_LEVELS),
                now,
                now,
                SNAPSHOT_DEPTH_LEVELS as i64,
                Some(snapshot.last_update_id),
                snapshot.bids,
                snapshot.asks,
                None,
                metadata,
            );

            let enqueued_into_async = match persist_async::try_enqueue_registered_persist_event(
                market.as_str(),
                event.clone(),
            )
            .await
            {
                Ok(v) => v,
                Err(err) => {
                    error!(
                        error = %err,
                        market = market.as_str(),
                        symbol = SYMBOL,
                        "orderbook snapshot async enqueue failed"
                    );
                    false
                }
            };

            if !enqueued_into_async {
                warn!(
                    market = market.as_str(),
                    symbol = SYMBOL,
                    "registered persist queue unavailable; skip direct snapshot persist to avoid duplicate canonical orderbook aggregation"
                );
                transition_state(
                    market,
                    state,
                    DepthSyncState::Desync,
                    "registered persist queue unavailable",
                    false,
                );
                return;
            }

            if let Err(err) = ops_writer
                .insert_backfill_job_run(
                    Some(market.as_str()),
                    Some(SYMBOL),
                    if market.as_str() == "spot" {
                        "/api/v3/depth"
                    } else {
                        "/fapi/v1/depth"
                    },
                    "orderbook_snapshot",
                    if was_desync {
                        "reconnect"
                    } else if trigger_reason == DEPTH_GAP_RECONCILE_REASON {
                        "gap"
                    } else {
                        "reconcile"
                    },
                    json!({
                        "limit": request_limit,
                        "target_depth_levels": SNAPSHOT_DEPTH_LEVELS,
                        "trigger_reason": trigger_reason
                    }),
                    Some(1),
                    "success",
                    None,
                )
                .await
            {
                warn!(error = %err, "write depth backfill job run failed");
            }

            transition_state(
                market,
                state,
                DepthSyncState::LiveSynced,
                "snapshot published",
                is_periodic_refresh,
            );
        }
        Err(err) => {
            warn!(
                error = %err,
                market = market.as_str(),
                symbol = SYMBOL,
                "fetch depth snapshot failed"
            );
            transition_state(
                market,
                state,
                DepthSyncState::Desync,
                "snapshot fetch failed",
                false,
            );
            let err_message = err.to_string();

            if let Err(e) = ops_writer
                .insert_backfill_job_run(
                    Some(market.as_str()),
                    Some(SYMBOL),
                    if market.as_str() == "spot" {
                        "/api/v3/depth"
                    } else {
                        "/fapi/v1/depth"
                    },
                    "orderbook_snapshot",
                    "reconnect",
                    json!({
                        "limit": request_depth_limit(market, SNAPSHOT_DEPTH_LEVELS),
                        "target_depth_levels": SNAPSHOT_DEPTH_LEVELS
                    }),
                    None,
                    "failed",
                    Some(err_message.as_str()),
                )
                .await
            {
                warn!(error = %e, "write failed depth backfill job run failed");
            }
        }
    }
}

fn transition_state(
    market: Market,
    state: &mut DepthSyncState,
    next: DepthSyncState,
    reason: &str,
    log_as_debug: bool,
) {
    if *state == next {
        return;
    }

    if log_as_debug {
        debug!(
            market = market.as_str(),
            from = state.as_str(),
            to = next.as_str(),
            reason,
            "depth state transition"
        );
    } else {
        info!(
            market = market.as_str(),
            from = state.as_str(),
            to = next.as_str(),
            reason,
            "depth state transition"
        );
    }
    *state = next;
}

fn request_depth_limit(market: Market, desired_levels: u16) -> u16 {
    match market {
        Market::Spot => desired_levels,
        Market::Futures => {
            const FUTURES_ALLOWED_LIMITS: [u16; 7] = [5, 10, 20, 50, 100, 500, 1000];
            for limit in FUTURES_ALLOWED_LIMITS {
                if desired_levels <= limit {
                    return limit;
                }
            }
            1000
        }
    }
}
