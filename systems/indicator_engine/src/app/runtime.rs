use crate::app::bootstrap::AppContext;
use crate::indicators::context::{
    DivergenceSigTestMode, IndicatorContext, IndicatorRuntimeOptions, IndicatorSnapshotRow,
    KlineHistoryBar, KlineHistorySupplement,
};
use crate::indicators::i19_kline_history::build_interval_bar_records;
use crate::ingest::decoder::{decode_contract_body, EngineEvent, MdData};
use crate::ingest::mq_consumer;
use crate::ingest::watermark::floor_minute;
use crate::observability::heartbeat;
use crate::observability::metrics::AppMetrics;
use crate::publish::ind_publisher::IndPublisher;
use crate::runtime::dispatcher::Dispatcher;
use crate::runtime::state_store::{
    MinuteHistory, StateSnapshot, StateStore, HISTORY_LIMIT_MINUTES, STATE_SNAPSHOT_VERSION,
};
use crate::runtime::window_scheduler::WindowScheduler;
use crate::storage::event_writer::EventWriter;
use crate::storage::feature_writer::FeatureWriter;
use crate::storage::level_writer::LevelWriter;
use crate::storage::snapshot_writer::SnapshotWriter;
use anyhow::{Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde_json::{json, Value};
use sqlx::{postgres::PgRow, PgPool, Row};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{interval, Instant, MissedTickBehavior};
use tracing::{error, info, warn};
use uuid::Uuid;

const STARTUP_BACKFILL_FALLBACK_LOOKBACK_MINUTES: i64 = 180;
const STARTUP_BACKFILL_OVERLAP_MINUTES: i64 = 180;
const STARTUP_BACKFILL_SAFETY_LAG_SECS: i64 = 10;
const STARTUP_BACKFILL_MARKET: &str = "all";
const STALE_DROP_REPORT_INTERVAL_SECS: u64 = 10;
const INGEST_TRADE_CHANNEL_CAPACITY: usize = 50_000;
const INGEST_NON_TRADE_CHANNEL_CAPACITY: usize = 100_000;

#[derive(Debug, Clone)]
pub struct ReplayRow {
    pub event_ts: DateTime<Utc>,
    pub msg_type: String,
    pub market: String,
    pub symbol: String,
    pub routing_key: String,
    pub data_json: Value,
}

#[derive(Debug, Clone)]
pub struct BackfillCursor {
    pub event_ts: DateTime<Utc>,
    pub msg_type: String,
    pub market: String,
    pub symbol: String,
    pub routing_key: String,
}

pub fn build_indicator_runtime_options(
    config: &crate::app::bootstrap::RootConfig,
) -> IndicatorRuntimeOptions {
    IndicatorRuntimeOptions {
        whale_threshold_usdt: config.indicator.whale_threshold_usdt,
        kline_history_bars_1m: config.indicator.kline_history.bars_1m,
        kline_history_bars_15m: config.indicator.kline_history.bars_15m,
        kline_history_bars_4h: config.indicator.kline_history.bars_4h,
        kline_history_bars_1d: config.indicator.kline_history.bars_1d,
        kline_history_fill_1d_from_db: config.indicator.kline_history.fill_1d_from_db,
        fvg_windows: config.indicator.fvg.windows.clone(),
        fvg_fill_from_db: config.indicator.fvg.fill_from_db,
        fvg_db_bars_4h: config.indicator.fvg.db_bars_4h,
        fvg_db_bars_1d: config.indicator.fvg.db_bars_1d,
        fvg_epsilon_gap_ticks: config.indicator.fvg.epsilon_gap_ticks,
        fvg_atr_lookback: config.indicator.fvg.atr_lookback,
        fvg_min_body_ratio: config.indicator.fvg.min_body_ratio,
        fvg_min_impulse_atr_ratio: config.indicator.fvg.min_impulse_atr_ratio,
        fvg_min_gap_atr_ratio: config.indicator.fvg.min_gap_atr_ratio,
        fvg_max_gap_atr_ratio: config.indicator.fvg.max_gap_atr_ratio,
        fvg_mitigated_fill_threshold: config.indicator.fvg.mitigated_fill_threshold,
        fvg_invalid_close_bars: config.indicator.fvg.invalid_close_bars,
        tpo_rows_nb: config.indicator.tpo_market_profile.rows_nb,
        tpo_value_area_pct: config.indicator.tpo_market_profile.value_area_pct,
        tpo_session_windows: config.indicator.tpo_market_profile.session_windows.clone(),
        tpo_ib_minutes: config.indicator.tpo_market_profile.ib_minutes,
        tpo_dev_output_windows: config
            .indicator
            .tpo_market_profile
            .dev_output_windows
            .clone(),
        rvwap_windows: config.indicator.rvwap_sigma_bands.windows.clone(),
        rvwap_output_windows: config.indicator.rvwap_sigma_bands.output_windows.clone(),
        rvwap_min_samples: config.indicator.rvwap_sigma_bands.min_samples,
        high_volume_pulse_z_windows: config.indicator.high_volume_pulse.z_windows.clone(),
        high_volume_pulse_summary_windows: config
            .indicator
            .high_volume_pulse
            .summary_windows
            .clone(),
        high_volume_pulse_min_samples: config.indicator.high_volume_pulse.min_samples,
        ema_base_periods: config.indicator.ema_trend_regime.base_periods.clone(),
        ema_htf_periods: config.indicator.ema_trend_regime.htf_periods.clone(),
        ema_htf_windows: config.indicator.ema_trend_regime.htf_windows.clone(),
        ema_output_windows: config.indicator.ema_trend_regime.output_windows.clone(),
        ema_fill_from_db: config.indicator.ema_trend_regime.fill_from_db,
        ema_db_bars_4h: config.indicator.ema_trend_regime.db_bars_4h,
        ema_db_bars_1d: config.indicator.ema_trend_regime.db_bars_1d,
        divergence_sig_test_mode: DivergenceSigTestMode::from_str(
            &config.indicator.divergence.sig_test_mode,
        ),
        divergence_bootstrap_b: config.indicator.divergence.bootstrap_b,
        divergence_bootstrap_block_len: config.indicator.divergence.bootstrap_block_len,
        divergence_p_value_threshold: config.indicator.divergence.p_value_threshold,
        window_codes: config.indicator.window_codes.clone(),
    }
}

pub async fn run(ctx: AppContext) -> Result<()> {
    let ctx = Arc::new(ctx);

    let metrics = Arc::new(AppMetrics::default());
    let (trade_tx, mut trade_rx) = mpsc::channel(INGEST_TRADE_CHANNEL_CAPACITY);
    let (non_trade_tx, mut non_trade_rx) = mpsc::channel(INGEST_NON_TRADE_CHANNEL_CAPACITY);

    let consumer_handles =
        mq_consumer::spawn_consumers(ctx.clone(), trade_tx, non_trade_tx, metrics.clone());
    let heartbeat_ctx = ctx.clone();
    let heartbeat_metrics = metrics.clone();
    let heartbeat_handle = tokio::spawn(async move {
        heartbeat::run_heartbeat_loop(heartbeat_ctx, heartbeat_metrics).await
    });

    let feature_writer = FeatureWriter::new(ctx.db_pool.clone());
    let snapshot_writer = SnapshotWriter::new(ctx.db_pool.clone());
    let level_writer = LevelWriter::new(ctx.db_pool.clone());
    let event_writer = EventWriter::new(ctx.db_pool.clone(), metrics.clone());
    let publisher = IndPublisher::new(
        ctx.mq_publish_channel.clone(),
        ctx.config.mq.exchanges.ind.name.clone(),
        ctx.producer_instance_id.clone(),
    );

    let dispatcher = Dispatcher::new(
        feature_writer,
        snapshot_writer,
        level_writer,
        event_writer,
        publisher,
    );

    let mut state_store = StateStore::new(
        ctx.config.indicator.symbol.to_uppercase(),
        ctx.config.indicator.whale_threshold_usdt,
    );
    let mut scheduler = WindowScheduler::new(ctx.config.indicator.watermark_lateness_secs);
    let consume_mode_live = ctx
        .config
        .indicator
        .consume_mode
        .eq_ignore_ascii_case("live");
    let live_drop_stale_enabled = ctx.config.indicator.live_drop_stale_enabled;
    let stale_limit_secs = ctx.config.indicator.live_drop_stale_event_secs;
    let mut max_seen_event_ts: Option<chrono::DateTime<Utc>> = None;
    let mut stale_drop_count: u64 = 0;
    let mut stale_drop_max_lag_secs: i64 = 0;
    let mut stale_drop_max_publish_delay_secs: i64 = 0;
    let mut stale_drop_max_transport_lag_secs: i64 = 0;
    let mut stale_drop_oldest_ts: Option<DateTime<Utc>> = None;
    let mut stale_drop_newest_ts: Option<DateTime<Utc>> = None;
    let mut stale_drop_by_msg_type: HashMap<String, u64> = HashMap::new();
    let mut stale_drop_last_report = Instant::now();
    let runtime_options = build_indicator_runtime_options(&ctx.config);

    let mut tick = interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

    // Register signal handlers BEFORE backfill so SIGTERM/Ctrl+C during
    // the long backfill phase is also caught and triggers a clean snapshot save.
    use tokio::signal::unix::{signal, SignalKind};
    let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler setup failed");

    info!(
        symbol = %ctx.config.indicator.symbol,
        queue_count = ctx.indicator_queues.len(),
        export_interval_secs = ctx.config.indicator.export_interval_secs,
        startup_mode = "live_plus_auto_backfill",
        startup_max_catchup_minutes = ctx.config.indicator.startup_max_catchup_minutes,
        stale_limit_secs = stale_limit_secs,
        watermark_lateness_secs = ctx.config.indicator.watermark_lateness_secs,
        "indicator_engine started; press Ctrl+C to stop"
    );

    // Race backfill against shutdown signals. If a signal arrives during backfill,
    // save the partial snapshot (valid — next start gap-fills missing minutes) and exit.
    let mut got_signal_before_live = false;
    let startup_replay_cutoff_bucket = tokio::select! {
        biased;
        _ = tokio::signal::ctrl_c() => {
            info!("Ctrl+C received during backfill, saving snapshot and shutting down");
            got_signal_before_live = true;
            None
        }
        _ = sigterm.recv() => {
            info!("SIGTERM received during backfill, saving snapshot and shutting down");
            got_signal_before_live = true;
            None
        }
        result = run_startup_backfill(
            &ctx,
            metrics.clone(),
            &dispatcher,
            &mut state_store,
            &mut scheduler,
            &runtime_options,
        ) => {
            match result {
                Ok(v) => v,
                Err(err) => {
                    warn!(
                        error = %err,
                        error_chain = %format!("{err:#}"),
                        "startup historical backfill failed, continue with live stream"
                    );
                    None
                }
            }
        }
    };

    if got_signal_before_live {
        // Only save snapshot if backfill progressed far enough to be useful.
        // A snapshot with insufficient history would cause the next restart to
        // skip the full backfill it still needs, producing wrong indicator results.
        // Require at least 1 day (1440 bars) of history before saving.
        let snapshot_path = ctx
            .config
            .indicator
            .snapshot_file_path
            .replace("{symbol}", &ctx.config.indicator.symbol);
        let history_len = state_store.history_futures_len();
        if !snapshot_path.is_empty() && history_len >= 1440 {
            info!(
                history_bars = history_len,
                "Saving state snapshot before exit (mid-backfill)..."
            );
            let snap = state_store.extract_snapshot();
            match save_state_snapshot(&snap, &snapshot_path).await {
                Ok(()) => {
                    info!(path = %snapshot_path, history_bars = history_len, "State snapshot saved (mid-backfill)")
                }
                Err(e) => warn!(error = %e, "Failed to save state snapshot"),
            }
        } else if history_len < 1440 {
            info!(
                history_bars = history_len,
                "Skipping snapshot save — too little history from early backfill (< 1440 bars)"
            );
        }
        heartbeat_handle.abort();
        for h in consumer_handles {
            h.abort();
        }
        return Ok(());
    }

    let mut startup_cutover_completed = startup_replay_cutoff_bucket.is_none();

    let mut trade_channel_closed = false;
    let mut non_trade_channel_closed = false;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Ctrl+C received, shutting down");
                break;
            }
            _ = sigterm.recv() => {
                info!("SIGTERM received, shutting down");
                break;
            }
            maybe_event = trade_rx.recv(), if !trade_channel_closed => {
                if let Some(event) = maybe_event {
                    handle_ingest_event(
                        event,
                        startup_replay_cutoff_bucket,
                        &mut startup_cutover_completed,
                        consume_mode_live,
                        live_drop_stale_enabled,
                        stale_limit_secs,
                        &mut stale_drop_count,
                        &mut stale_drop_max_lag_secs,
                        &mut stale_drop_max_publish_delay_secs,
                        &mut stale_drop_max_transport_lag_secs,
                        &mut stale_drop_oldest_ts,
                        &mut stale_drop_newest_ts,
                        &mut stale_drop_by_msg_type,
                        &metrics,
                        &mut max_seen_event_ts,
                        &mut state_store,
                    );
                } else {
                    trade_channel_closed = true;
                    if non_trade_channel_closed {
                        warn!("all mq consumers ended, stopping indicator engine");
                        break;
                    }
                }
            }
            maybe_event = non_trade_rx.recv(), if !non_trade_channel_closed => {
                if let Some(event) = maybe_event {
                    handle_ingest_event(
                        event,
                        startup_replay_cutoff_bucket,
                        &mut startup_cutover_completed,
                        consume_mode_live,
                        live_drop_stale_enabled,
                        stale_limit_secs,
                        &mut stale_drop_count,
                        &mut stale_drop_max_lag_secs,
                        &mut stale_drop_max_publish_delay_secs,
                        &mut stale_drop_max_transport_lag_secs,
                        &mut stale_drop_oldest_ts,
                        &mut stale_drop_newest_ts,
                        &mut stale_drop_by_msg_type,
                        &metrics,
                        &mut max_seen_event_ts,
                        &mut state_store,
                    );
                } else {
                    non_trade_channel_closed = true;
                    if trade_channel_closed {
                        warn!("all mq consumers ended, stopping indicator engine");
                        break;
                    }
                }
            }
            _ = tick.tick() => {
                let queue_lag = (trade_rx.len() + non_trade_rx.len()) as i64;
                metrics.set_queue_lag(queue_lag);
                if stale_drop_count > 0
                    && stale_drop_last_report.elapsed()
                        >= Duration::from_secs(STALE_DROP_REPORT_INTERVAL_SECS)
                {
                    warn!(
                        dropped = stale_drop_count,
                        oldest_event_ts = ?stale_drop_oldest_ts,
                        newest_event_ts = ?stale_drop_newest_ts,
                        max_lag_secs = stale_drop_max_lag_secs,
                        max_publish_delay_secs = stale_drop_max_publish_delay_secs,
                        max_transport_lag_secs = stale_drop_max_transport_lag_secs,
                        stale_limit_secs = stale_limit_secs,
                        trade_channel_len = trade_rx.len(),
                        non_trade_channel_len = non_trade_rx.len(),
                        drop_by_msg_type = %format_stale_msg_type_distribution(&stale_drop_by_msg_type),
                        "drop stale md event in live mode (aggregated)"
                    );
                    stale_drop_count = 0;
                    stale_drop_max_lag_secs = 0;
                    stale_drop_max_publish_delay_secs = 0;
                    stale_drop_max_transport_lag_secs = 0;
                    stale_drop_oldest_ts = None;
                    stale_drop_newest_ts = None;
                    stale_drop_by_msg_type.clear();
                    stale_drop_last_report = Instant::now();
                }

                let Some(watermark_ts) = max_seen_event_ts else {
                    continue;
                };
                if let Err(err) = process_ready_minutes(
                    &ctx,
                    metrics.clone(),
                    &dispatcher,
                    &mut state_store,
                    &mut scheduler,
                    &runtime_options,
                    watermark_ts,
                )
                .await
                {
                    error!(error = %err, "process ready indicator minutes failed");
                    return Err(err).context("process ready indicator minutes failed");
                }
            }
        }
    }

    // Save state snapshot for fast next startup
    // Support {symbol} placeholder in path (e.g. "/tmp/indicator_engine_{symbol}.json.gz")
    let snapshot_path = ctx
        .config
        .indicator
        .snapshot_file_path
        .replace("{symbol}", &ctx.config.indicator.symbol);
    if !snapshot_path.is_empty() {
        info!("Saving state snapshot before exit...");
        let snap = state_store.extract_snapshot();
        match save_state_snapshot(&snap, &snapshot_path).await {
            Ok(()) => info!(
                path = %snapshot_path,
                futures_bars = snap.history_futures.len(),
                spot_bars = snap.history_spot.len(),
                last_ts = %snap.last_finalized_ts,
                "State snapshot saved successfully"
            ),
            Err(e) => warn!(error = %e, "Failed to save state snapshot"),
        }
    }

    heartbeat_handle.abort();
    for h in consumer_handles {
        h.abort();
    }

    Ok(())
}

const INDICATOR_COVERAGE_ORDER: [(&str, &str); 24] = [
    ("i01", "price_volume_structure"),
    ("i02", "footprint"),
    ("i03", "divergence"),
    ("i04", "liquidation_density"),
    ("i05", "orderbook_depth"),
    ("i06", "absorption"),
    ("i07", "initiation"),
    ("i08", "bullish_absorption"),
    ("i09", "bullish_initiation"),
    ("i10", "bearish_absorption"),
    ("i11", "bearish_initiation"),
    ("i12", "buying_exhaustion"),
    ("i13", "selling_exhaustion"),
    ("i14", "cvd_pack"),
    ("i15", "whale_trades"),
    ("i16", "funding_rate"),
    ("i17", "vpin"),
    ("i18", "avwap"),
    ("i19", "kline_history"),
    ("i20", "tpo_market_profile"),
    ("i21", "rvwap_sigma_bands"),
    ("i22", "high_volume_pulse"),
    ("i23", "ema_trend_regime"),
    ("i24", "fvg"),
];

fn indicator_coverage(snapshots: &[IndicatorSnapshotRow]) -> (Vec<String>, Vec<String>) {
    let snapshot_codes: HashSet<&str> = snapshots.iter().map(|s| s.indicator_code).collect();
    let mut computed = Vec::with_capacity(INDICATOR_COVERAGE_ORDER.len());
    let mut missing = Vec::new();

    for (prefix, code) in INDICATOR_COVERAGE_ORDER {
        let label = format!("{}:{}", prefix, code);
        if snapshot_codes.contains(code) {
            computed.push(label);
        } else {
            missing.push(label);
        }
    }

    (computed, missing)
}

fn format_stale_msg_type_distribution(counter: &HashMap<String, u64>) -> String {
    if counter.is_empty() {
        return "none".to_string();
    }
    let mut pairs = counter.iter().collect::<Vec<_>>();
    pairs.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
    pairs
        .into_iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join(",")
}

fn try_load_state_snapshot(path: &str, symbol: &str, max_age_hours: u64) -> Option<StateSnapshot> {
    use flate2::read::GzDecoder;
    use std::fs::File;

    const MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT: usize = 60;

    if path.is_empty() {
        return None;
    }
    let file = File::open(path).ok()?;
    let gz = GzDecoder::new(file);
    let snap: StateSnapshot = serde_json::from_reader(gz).ok()?;
    // Version check
    if snap.version != STATE_SNAPSHOT_VERSION {
        warn!(
            found = snap.version,
            expected = STATE_SNAPSHOT_VERSION,
            "State snapshot version mismatch, ignoring"
        );
        return None;
    }
    // Symbol check
    if snap.symbol != symbol {
        warn!(snap_symbol = %snap.symbol, "State snapshot symbol mismatch, ignoring");
        return None;
    }
    // Age check
    let age = Utc::now() - snap.saved_at;
    if age > chrono::Duration::hours(max_age_hours as i64) {
        warn!(
            age_hours = age.num_hours(),
            max_age_hours, "State snapshot too old, ignoring"
        );
        return None;
    }
    // Reject snapshots with insufficient history (< 1 day = 1440 bars).
    // Such snapshots are saved mid-early-backfill and would cause the next
    // restart to skip the full history rebuild it still needs.
    const MIN_SNAPSHOT_BARS: usize = 1440;
    if snap.history_futures.len() < MIN_SNAPSHOT_BARS {
        warn!(
            bars = snap.history_futures.len(),
            min = MIN_SNAPSHOT_BARS,
            "State snapshot has insufficient history, ignoring"
        );
        return None;
    }
    if !minute_history_is_strictly_contiguous(&snap.history_futures, snap.last_finalized_ts) {
        warn!("State snapshot futures history is not a strict contiguous minute series, ignoring");
        return None;
    }
    if let Some((start, end, len)) = find_long_null_price_run(
        &snap.history_futures,
        MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT,
    ) {
        warn!(
            run_start = %start,
            run_end = %end,
            run_len = len,
            max_allowed = MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT,
            "State snapshot futures history contains long null-price run, ignoring"
        );
        return None;
    }
    if !snap.history_spot.is_empty()
        && !minute_history_is_strictly_contiguous(&snap.history_spot, snap.last_finalized_ts)
    {
        warn!("State snapshot spot history is not a strict contiguous minute series, ignoring");
        return None;
    }
    if let Some((start, end, len)) = find_long_null_price_run(
        &snap.history_spot,
        MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT,
    ) {
        warn!(
            run_start = %start,
            run_end = %end,
            run_len = len,
            max_allowed = MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT,
            "State snapshot spot history contains long null-price run, ignoring"
        );
        return None;
    }
    Some(snap)
}

fn minute_history_is_strictly_contiguous(
    history: &[MinuteHistory],
    last_finalized_ts: DateTime<Utc>,
) -> bool {
    let Some(first) = history.first() else {
        return false;
    };
    let Some(last) = history.last() else {
        return false;
    };
    if last.ts_bucket != last_finalized_ts {
        return false;
    }
    if first.ts_bucket > last.ts_bucket {
        return false;
    }
    history.windows(2).all(|pair| {
        let prev = &pair[0];
        let next = &pair[1];
        next.ts_bucket > prev.ts_bucket
            && (next.ts_bucket - prev.ts_bucket) == ChronoDuration::minutes(1)
    })
}

fn find_long_null_price_run(
    history: &[MinuteHistory],
    min_run_len: usize,
) -> Option<(DateTime<Utc>, DateTime<Utc>, usize)> {
    if history.is_empty() || min_run_len == 0 {
        return None;
    }

    let mut run_start: Option<DateTime<Utc>> = None;
    let mut run_end: Option<DateTime<Utc>> = None;
    let mut run_len = 0usize;

    for row in history {
        if minute_history_has_any_price(row) {
            if run_len >= min_run_len {
                return Some((run_start?, run_end?, run_len));
            }
            run_start = None;
            run_end = None;
            run_len = 0;
            continue;
        }

        if run_start.is_none() {
            run_start = Some(row.ts_bucket);
        }
        run_end = Some(row.ts_bucket);
        run_len += 1;
    }

    if run_len >= min_run_len {
        Some((run_start?, run_end?, run_len))
    } else {
        None
    }
}

fn minute_history_has_any_price(row: &MinuteHistory) -> bool {
    row.open_price.is_some()
        || row.high_price.is_some()
        || row.low_price.is_some()
        || row.close_price.is_some()
        || row.last_price.is_some()
}

async fn save_state_snapshot(snap: &StateSnapshot, path: &str) -> anyhow::Result<()> {
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::fs::File;
    use std::path::Path;

    if path.is_empty() {
        return Ok(());
    }
    // Ensure parent directory exists
    if let Some(parent) = Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let tmp_path = format!("{}.tmp", path);
    let file = File::create(&tmp_path)?;
    let gz = GzEncoder::new(file, Compression::fast());
    serde_json::to_writer(gz, snap)?;
    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

pub async fn load_kline_history_supplement(
    pool: &PgPool,
    symbol: &str,
    history_futures: &[MinuteHistory],
    history_spot: &[MinuteHistory],
    bars_4h: usize,
    bars_1d: usize,
    fill_1d_from_db: bool,
    ema_fill_from_db: bool,
    ema_db_bars_4h: usize,
    ema_db_bars_1d: usize,
    fvg_fill_from_db: bool,
    fvg_windows: &[String],
    fvg_db_bars_4h: usize,
    fvg_db_bars_1d: usize,
    current_minute_close: DateTime<Utc>,
) -> KlineHistorySupplement {
    if !fill_1d_from_db && !ema_fill_from_db && !fvg_fill_from_db && bars_4h == 0 {
        return KlineHistorySupplement::default();
    }

    let fvg_needs_4h = fvg_fill_from_db && fvg_windows.iter().any(|code| code == "4h");
    let fvg_needs_1d = fvg_fill_from_db && fvg_windows.iter().any(|code| code == "1d");

    let in_mem_futures_1d =
        build_interval_bar_records(history_futures, 1440, usize::MAX, current_minute_close);
    let in_mem_spot_1d =
        build_interval_bar_records(history_spot, 1440, usize::MAX, current_minute_close);
    let in_mem_futures_4h =
        build_interval_bar_records(history_futures, 240, usize::MAX, current_minute_close);
    let in_mem_spot_4h =
        build_interval_bar_records(history_spot, 240, usize::MAX, current_minute_close);

    let required_futures_1d = if fill_1d_from_db { bars_1d } else { 0 }
        .max(if ema_fill_from_db { ema_db_bars_1d } else { 0 })
        .max(if fvg_needs_1d { fvg_db_bars_1d } else { 0 });
    let required_spot_1d = if fill_1d_from_db { bars_1d } else { 0 };
    let required_futures_4h = bars_4h
        .max(if ema_fill_from_db { ema_db_bars_4h } else { 0 })
        .max(if fvg_needs_4h { fvg_db_bars_4h } else { 0 });
    let required_spot_4h = bars_4h;

    let in_mem_futures_count = in_mem_futures_1d.len();
    let in_mem_spot_count = in_mem_spot_1d.len();
    let in_mem_futures_4h_count = in_mem_futures_4h.len();
    let in_mem_spot_4h_count = in_mem_spot_4h.len();

    let futures_1d_missing = required_futures_1d.saturating_sub(in_mem_futures_count);
    let spot_1d_missing = required_spot_1d.saturating_sub(in_mem_spot_count);
    let futures_4h_missing = required_futures_4h.saturating_sub(in_mem_futures_4h_count);
    let spot_4h_missing = required_spot_4h.saturating_sub(in_mem_spot_4h_count);

    if futures_1d_missing == 0
        && spot_1d_missing == 0
        && futures_4h_missing == 0
        && spot_4h_missing == 0
    {
        return KlineHistorySupplement::default();
    }

    let futures_oldest_open = in_mem_futures_1d
        .first()
        .map(|b| b.open_time)
        .unwrap_or(current_minute_close);
    let spot_oldest_open = in_mem_spot_1d
        .first()
        .map(|b| b.open_time)
        .unwrap_or(current_minute_close);
    let futures_4h_oldest_open = in_mem_futures_4h
        .first()
        .map(|b| b.open_time)
        .unwrap_or(current_minute_close);
    let spot_4h_oldest_open = in_mem_spot_4h
        .first()
        .map(|b| b.open_time)
        .unwrap_or(current_minute_close);

    let futures_1d_db = if futures_1d_missing > 0 {
        match fetch_older_interval_bars(
            pool,
            symbol,
            "futures",
            "1d",
            futures_oldest_open,
            futures_1d_missing,
        )
        .await
        {
            Ok(rows) => rows,
            Err(err) => {
                warn!(
                    error = %err,
                    symbol = %symbol,
                    market = "futures",
                    interval_code = "1d",
                    limit = futures_1d_missing,
                    "load kline supplement from DB failed"
                );
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    let spot_1d_db = if spot_1d_missing > 0 {
        match fetch_older_interval_bars(
            pool,
            symbol,
            "spot",
            "1d",
            spot_oldest_open,
            spot_1d_missing,
        )
        .await
        {
            Ok(rows) => rows,
            Err(err) => {
                warn!(
                    error = %err,
                    symbol = %symbol,
                    market = "spot",
                    interval_code = "1d",
                    limit = spot_1d_missing,
                    "load kline supplement from DB failed"
                );
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    let futures_4h_db = if futures_4h_missing > 0 {
        match fetch_older_interval_bars(
            pool,
            symbol,
            "futures",
            "4h",
            futures_4h_oldest_open,
            futures_4h_missing,
        )
        .await
        {
            Ok(rows) => rows,
            Err(err) => {
                warn!(
                    error = %err,
                    symbol = %symbol,
                    market = "futures",
                    interval_code = "4h",
                    limit = futures_4h_missing,
                    "load kline supplement from DB failed"
                );
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    let spot_4h_db = if spot_4h_missing > 0 {
        match fetch_older_interval_bars(
            pool,
            symbol,
            "spot",
            "4h",
            spot_4h_oldest_open,
            spot_4h_missing,
        )
        .await
        {
            Ok(rows) => rows,
            Err(err) => {
                warn!(
                    error = %err,
                    symbol = %symbol,
                    market = "spot",
                    interval_code = "4h",
                    limit = spot_4h_missing,
                    "load kline supplement from DB failed"
                );
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    KlineHistorySupplement {
        futures_4h_db,
        futures_1d_db,
        spot_4h_db,
        spot_1d_db,
    }
}

async fn fetch_older_interval_bars(
    pool: &PgPool,
    symbol: &str,
    market: &str,
    interval_code: &str,
    older_than_open_time: DateTime<Utc>,
    limit: usize,
) -> Result<Vec<KlineHistoryBar>> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT
            open_time,
            close_time,
            open_price,
            high_price,
            low_price,
            close_price,
            COALESCE(volume_base, 0.0) AS volume_base,
            COALESCE(quote_volume, 0.0) AS volume_quote
        FROM md.kline_bar
        WHERE market = $1::cfg.market_type
          AND symbol = $2
          AND interval_code = $3
          AND is_closed = true
          AND open_time < $4
        ORDER BY open_time DESC
        LIMIT $5
        "#,
    )
    .bind(market)
    .bind(symbol.to_uppercase())
    .bind(interval_code)
    .bind(older_than_open_time)
    .bind(limit as i64)
    .fetch_all(pool)
    .await
    .context("query kline supplement")?;

    let expected_minutes = interval_code_to_minutes(interval_code);

    let mut out = rows
        .into_iter()
        .map(|row| {
            let open_time: DateTime<Utc> = row.get("open_time");
            let close_time: DateTime<Utc> = row.get("close_time");
            KlineHistoryBar {
                open_time,
                close_time,
                open: Some(row.get::<f64, _>("open_price")),
                high: Some(row.get::<f64, _>("high_price")),
                low: Some(row.get::<f64, _>("low_price")),
                close: Some(row.get::<f64, _>("close_price")),
                volume_base: row.get::<f64, _>("volume_base"),
                volume_quote: row.get::<f64, _>("volume_quote"),
                is_closed: close_time <= older_than_open_time,
                minutes_covered: expected_minutes,
                expected_minutes,
            }
        })
        .collect::<Vec<_>>();

    out.reverse();
    Ok(out)
}

fn interval_code_to_minutes(interval_code: &str) -> i64 {
    match interval_code {
        "1h" => 60,
        "4h" => 240,
        "1d" => 1440,
        _ => 1,
    }
}

fn handle_ingest_event(
    event: EngineEvent,
    startup_replay_cutoff_bucket: Option<DateTime<Utc>>,
    startup_cutover_completed: &mut bool,
    consume_mode_live: bool,
    live_drop_stale_enabled: bool,
    stale_limit_secs: i64,
    stale_drop_count: &mut u64,
    stale_drop_max_lag_secs: &mut i64,
    stale_drop_max_publish_delay_secs: &mut i64,
    stale_drop_max_transport_lag_secs: &mut i64,
    stale_drop_oldest_ts: &mut Option<DateTime<Utc>>,
    stale_drop_newest_ts: &mut Option<DateTime<Utc>>,
    stale_drop_by_msg_type: &mut HashMap<String, u64>,
    metrics: &Arc<AppMetrics>,
    max_seen_event_ts: &mut Option<DateTime<Utc>>,
    state_store: &mut StateStore,
) {
    let event_bucket_ts = logical_event_bucket_ts(&event);
    if let Some(cutoff_bucket_ts) = startup_replay_cutoff_bucket {
        if event_bucket_ts < cutoff_bucket_ts {
            return;
        }
        if !*startup_cutover_completed {
            info!(
                cutoff_bucket_ts = %cutoff_bucket_ts,
                first_live_event_ts = %event.event_ts,
                first_live_bucket_ts = %event_bucket_ts,
                "startup replay cutover completed; switch to pure live processing"
            );
            *startup_cutover_completed = true;
        }
    }

    if live_drop_stale_enabled && consume_mode_live && *startup_cutover_completed {
        let lag_secs = (Utc::now() - event.event_ts).num_seconds();
        if lag_secs > stale_limit_secs {
            let publish_delay_secs = (event.published_at - event.event_ts).num_seconds().max(0);
            let transport_lag_secs = (Utc::now() - event.published_at).num_seconds().max(0);

            *stale_drop_count += 1;
            *stale_drop_max_lag_secs = (*stale_drop_max_lag_secs).max(lag_secs);
            *stale_drop_max_publish_delay_secs =
                (*stale_drop_max_publish_delay_secs).max(publish_delay_secs);
            *stale_drop_max_transport_lag_secs =
                (*stale_drop_max_transport_lag_secs).max(transport_lag_secs);
            *stale_drop_by_msg_type
                .entry(event.msg_type.clone())
                .or_insert(0) += 1;

            let next_oldest = stale_drop_oldest_ts
                .as_ref()
                .cloned()
                .map_or(event.event_ts, |ts| ts.min(event.event_ts));
            let next_newest = stale_drop_newest_ts
                .as_ref()
                .cloned()
                .map_or(event.event_ts, |ts| ts.max(event.event_ts));
            *stale_drop_oldest_ts = Some(next_oldest);
            *stale_drop_newest_ts = Some(next_newest);
            return;
        }
    }

    metrics.inc_processed(event.event_ts.timestamp_millis());
    *max_seen_event_ts = Some(match max_seen_event_ts.as_ref().cloned() {
        Some(prev) => prev.max(event.event_ts),
        None => event.event_ts,
    });
    // EventBuffer was a no-op wrapper (push immediately followed by pop with no watermark
    // gating). Ingest directly to avoid the unnecessary allocation round-trip.
    state_store.ingest(event);
}

fn logical_event_bucket_ts(event: &EngineEvent) -> DateTime<Utc> {
    match &event.data {
        MdData::AggTrade1m(v) => v.ts_bucket,
        MdData::AggOrderbook1m(v) => v.ts_bucket,
        MdData::AggLiq1m(v) => v.ts_bucket,
        MdData::AggFundingMark1m(v) => v.ts_bucket,
        _ => floor_minute(event.event_ts),
    }
}

async fn process_ready_minutes(
    ctx: &Arc<AppContext>,
    metrics: Arc<AppMetrics>,
    dispatcher: &Dispatcher,
    state_store: &mut StateStore,
    scheduler: &mut WindowScheduler,
    runtime_options: &IndicatorRuntimeOptions,
    watermark_ts: DateTime<Utc>,
) -> Result<()> {
    let mut windows_processed = 0usize;
    let mut first_bucket: Option<DateTime<Utc>> = None;
    let mut last_bucket: Option<DateTime<Utc>> = None;
    let mut last_computed: Vec<String> = Vec::new();
    let mut missing_union: BTreeSet<String> = BTreeSet::new();

    // Accuracy first: dirty recompute truncates the finalized suffix before rebuilding it.
    // If we publish new live minutes while that suffix is only partially rebuilt, any
    // indicator that reads rolling minute history (for example orderbook_depth/liquidation
    // windows or recent_7d event coverage) can observe a temporary tail gap and emit
    // false low-coverage snapshots. Drain dirty recompute to completion before releasing
    // additional ready minutes.
    loop {
        let dirty_batch = state_store.recompute_dirty_finalized_minutes(5);
        if dirty_batch.is_empty() {
            break;
        }
        for window in dirty_batch {
            let minute = window.ts_bucket;
            let snapshots =
                process_window_bundle(ctx, dispatcher, runtime_options, &window).await?;
            metrics.inc_exported_window();
            let (computed, missing) = indicator_coverage(&snapshots);
            windows_processed += 1;
            if first_bucket.is_none() {
                first_bucket = Some(minute);
            }
            last_bucket = Some(minute);
            last_computed = computed;
            for item in missing {
                missing_union.insert(item);
            }
            if ctx.config.indicator.enable_file_export {
                if let Err(err) = export_snapshots(
                    &ctx.config.indicator.export_dir,
                    minute,
                    &ctx.config.indicator.symbol,
                    &snapshots,
                )
                .await
                {
                    warn!(error = %err, "export indicator snapshot file failed");
                }
            }
        }
    }

    for minute in scheduler.ready_minutes(watermark_ts) {
        let window = state_store.finalize_minute(minute);
        match process_window_bundle(ctx, dispatcher, runtime_options, &window).await {
            Ok(snapshots) => {
                metrics.inc_exported_window();
                let (computed, missing) = indicator_coverage(&snapshots);
                windows_processed += 1;
                if first_bucket.is_none() {
                    first_bucket = Some(minute);
                }
                last_bucket = Some(minute);
                last_computed = computed;
                for item in missing {
                    missing_union.insert(item);
                }
                if ctx.config.indicator.enable_file_export {
                    if let Err(err) = export_snapshots(
                        &ctx.config.indicator.export_dir,
                        minute,
                        &ctx.config.indicator.symbol,
                        &snapshots,
                    )
                    .await
                    {
                        warn!(error = %err, "export indicator snapshot file failed");
                    }
                }
            }
            Err(err) => {
                metrics.inc_db_error();
                error!(
                    error = %err,
                    error_chain = %format!("{err:#}"),
                    minute = %minute,
                    "process indicator window failed"
                );
            }
        }
    }

    if let Some(last_bucket) = last_bucket {
        let first_bucket = first_bucket.unwrap_or(last_bucket);
        if missing_union.is_empty() {
            info!(
                ts_bucket_from = %first_bucket,
                ts_bucket_to = %last_bucket,
                processed_windows = windows_processed,
                computed_count = last_computed.len(),
                missing_count = 0,
                computed_indicators = %last_computed.join(","),
                missing_indicators = "none",
                "indicator coverage"
            );
        } else {
            let missing_indicators = missing_union.iter().cloned().collect::<Vec<_>>().join(",");
            // Warn but do not crash: missing indicators are expected during warmup (e.g. VPIN
            // needs 120 minutes of history) and after transient data gaps. A fatal return here
            // would restart the whole engine on every cold start, causing a restart loop.
            warn!(
                ts_bucket_from = %first_bucket,
                ts_bucket_to = %last_bucket,
                processed_windows = windows_processed,
                computed_count = last_computed.len(),
                missing_count = missing_union.len(),
                computed_indicators = %last_computed.join(","),
                missing_indicators = %missing_indicators,
                "indicator coverage has missing indicators (warmup or data gap)"
            );
        }
    }

    Ok(())
}

async fn process_window_bundle(
    ctx: &Arc<AppContext>,
    dispatcher: &Dispatcher,
    runtime_options: &IndicatorRuntimeOptions,
    window: &crate::runtime::state_store::WindowBundle,
) -> Result<Vec<IndicatorSnapshotRow>> {
    let minute = window.ts_bucket;
    let kline_history_supplement = load_kline_history_supplement(
        &ctx.db_pool,
        &ctx.config.indicator.symbol,
        &window.history_futures,
        &window.history_spot,
        runtime_options.kline_history_bars_4h,
        runtime_options.kline_history_bars_1d,
        runtime_options.kline_history_fill_1d_from_db,
        runtime_options.ema_fill_from_db,
        runtime_options.ema_db_bars_4h,
        runtime_options.ema_db_bars_1d,
        runtime_options.fvg_fill_from_db,
        &runtime_options.fvg_windows,
        runtime_options.fvg_db_bars_4h,
        runtime_options.fvg_db_bars_1d,
        minute + ChronoDuration::minutes(1),
    )
    .await;
    let ictx = IndicatorContext::from_bundle(window, runtime_options, kline_history_supplement);

    dispatcher.process_window(&ictx).await
}

#[allow(dead_code)]
fn compare_minute_histories(
    snap_history: &[MinuteHistory],
    full_history: &[MinuteHistory],
    tolerance: f64,
) -> Vec<String> {
    let mut diffs = Vec::new();
    for (snap, full) in snap_history.iter().zip(full_history.iter()) {
        let ts = snap.ts_bucket;
        let check = |name: &str, a: f64, b: f64| {
            if (a - b).abs() > tolerance {
                format!(
                    "{} ts={} snap={:.6} full={:.6} diff={:.6}",
                    name,
                    ts,
                    a,
                    b,
                    a - b
                )
            } else {
                String::new()
            }
        };
        for s in [
            check("cvd", snap.cvd, full.cvd),
            check("vpin", snap.vpin, full.vpin),
            check("delta", snap.delta, full.delta),
            check("relative_delta", snap.relative_delta, full.relative_delta),
            check("buy_qty", snap.buy_qty, full.buy_qty),
            check("sell_qty", snap.sell_qty, full.sell_qty),
            check(
                "obi_twa",
                snap.obi_twa.unwrap_or(0.0),
                full.obi_twa.unwrap_or(0.0),
            ),
            check(
                "whale_notional_buy",
                snap.whale_notional_buy,
                full.whale_notional_buy,
            ),
            check(
                "whale_notional_sell",
                snap.whale_notional_sell,
                full.whale_notional_sell,
            ),
        ] {
            if !s.is_empty() {
                diffs.push(s);
            }
        }
    }
    diffs
}

async fn run_startup_backfill(
    ctx: &Arc<AppContext>,
    metrics: Arc<AppMetrics>,
    dispatcher: &Dispatcher,
    state_store: &mut StateStore,
    scheduler: &mut WindowScheduler,
    runtime_options: &IndicatorRuntimeOptions,
) -> Result<Option<DateTime<Utc>>> {
    let now = Utc::now();
    let latest_resume_ts = match latest_indicator_progress_ts(
        &ctx.db_pool,
        &ctx.config.indicator.symbol,
    )
    .await
    {
        Ok(ts) => ts,
        Err(progress_err) => {
            match latest_indicator_snapshot_ts(&ctx.db_pool, &ctx.config.indicator.symbol).await {
                Ok(ts) => {
                    warn!(
                        error = %progress_err,
                        error_chain = %format!("{progress_err:#}"),
                        symbol = %ctx.config.indicator.symbol,
                        "query latest indicator progress ts failed; fallback to indicator_snapshot"
                    );
                    ts
                }
                Err(snapshot_err) => {
                    warn!(
                        error = %snapshot_err,
                        error_chain = %format!("{snapshot_err:#}"),
                        symbol = %ctx.config.indicator.symbol,
                        fallback_lookback_minutes = STARTUP_BACKFILL_FALLBACK_LOOKBACK_MINUTES,
                        "query latest resume ts failed; fallback to fixed lookback window"
                    );
                    None
                }
            }
        }
    };
    let mut from_ts = match latest_resume_ts {
        Some(ts) => ts - ChronoDuration::minutes(STARTUP_BACKFILL_OVERLAP_MINUTES),
        None => now - ChronoDuration::minutes(STARTUP_BACKFILL_FALLBACK_LOOKBACK_MINUTES),
    };
    let raw_to_ts = now - ChronoDuration::seconds(STARTUP_BACKFILL_SAFETY_LAG_SECS);
    // Backfill must stop at a full-minute boundary so we never ingest a partial
    // minute from replay and then mix it with live stream data.
    let to_ts = floor_minute(raw_to_ts);
    let mut startup_max_catchup_minutes = ctx.config.indicator.startup_max_catchup_minutes;
    const STRICT_MIN_CATCHUP_MINUTES_7D: i64 = 7 * 24 * 60;
    let strict_history_floor_minutes = HISTORY_LIMIT_MINUTES as i64;
    if startup_max_catchup_minutes > 0
        && startup_max_catchup_minutes < STRICT_MIN_CATCHUP_MINUTES_7D
    {
        warn!(
            configured_startup_max_catchup_minutes = startup_max_catchup_minutes,
            enforced_startup_max_catchup_minutes = STRICT_MIN_CATCHUP_MINUTES_7D,
            "startup catch-up window raised to 7d to satisfy rolling-7d indicators"
        );
        startup_max_catchup_minutes = STRICT_MIN_CATCHUP_MINUTES_7D;
    }
    if startup_max_catchup_minutes > 0 {
        let catchup_floor = to_ts - ChronoDuration::minutes(startup_max_catchup_minutes);
        if from_ts < catchup_floor {
            info!(
                original_from_ts = %from_ts,
                capped_from_ts = %catchup_floor,
                to_ts = %to_ts,
                startup_max_catchup_minutes = startup_max_catchup_minutes,
                "capped startup historical backfill window by configured max catch-up"
            );
            from_ts = catchup_floor;
        }
    }
    let strict_history_floor = to_ts - ChronoDuration::minutes(strict_history_floor_minutes);
    if from_ts > strict_history_floor {
        info!(
            original_from_ts = %from_ts,
            strict_history_floor = %strict_history_floor,
            history_limit_minutes = strict_history_floor_minutes,
            "startup historical backfill expanded to rebuild full in-memory retention"
        );
        from_ts = strict_history_floor;
    }
    // Startup replay is bucket-based for canonical 1m rows. Floor the lower bound so
    // we never drop a completed ts_bucket just because the resume timestamp carried
    // non-zero seconds.
    from_ts = floor_minute(from_ts);

    // Try to load state snapshot for fast startup
    // Support {symbol} placeholder in path (e.g. "/tmp/indicator_engine_{symbol}.json.gz")
    let snapshot_file_path = ctx
        .config
        .indicator
        .snapshot_file_path
        .replace("{symbol}", &ctx.config.indicator.symbol);
    let mut snapshot_was_loaded = false;
    if !snapshot_file_path.is_empty() {
        if let Some(snap) = try_load_state_snapshot(
            &snapshot_file_path,
            &ctx.config.indicator.symbol,
            ctx.config.indicator.snapshot_max_age_hours,
        ) {
            let snap_ts = snap.last_finalized_ts;
            state_store.restore_from_snapshot(snap);
            snapshot_was_loaded = true;
            // CRITICAL: start from snap_ts + 1min, NO overlap.
            // CVD/VPIN from the snapshot already include all minutes up to snap_ts;
            // using overlap would double-count those delta/volume values.
            from_ts = snap_ts + ChronoDuration::minutes(1);
            info!(
                snap_ts = %snap_ts,
                new_from_ts = %from_ts,
                "State snapshot loaded successfully, running gap-only backfill"
            );
        } else {
            info!("No valid state snapshot found, running full backfill");
        }
    }
    if from_ts >= to_ts {
        info!(
            from_ts = %from_ts,
            to_ts = %to_ts,
            "skip startup historical backfill due to invalid range"
        );
        return Ok(None);
    }

    info!(
        from_ts = %from_ts,
        to_ts = %to_ts,
        raw_to_ts = %raw_to_ts,
        symbol = %ctx.config.indicator.symbol,
        fallback_lookback_minutes = STARTUP_BACKFILL_FALLBACK_LOOKBACK_MINUTES,
        overlap_minutes = STARTUP_BACKFILL_OVERLAP_MINUTES,
        startup_max_catchup_minutes = startup_max_catchup_minutes,
        startup_backfill_batch_size = ctx.config.indicator.startup_backfill_batch_size.max(100),
        backfill_source = "md.agg.*.1m",
        "startup historical backfill begin"
    );

    let mut cursor: Option<BackfillCursor> = None;
    let mut total_rows = 0_u64;
    let mut max_event_ts: Option<DateTime<Utc>> = None;
    let startup_backfill_batch_size = ctx.config.indicator.startup_backfill_batch_size.max(100);
    // Track the earliest ts_bucket seen across all backfill rows so we can
    // prime the scheduler to start from there instead of the default
    // latest_closed.  Without priming, ready_minutes(last_emitted=None) jumps
    // straight to latest_closed and silently abandons all older accumulated
    // buckets — the root cause of the 4-minute kline gap and the delayed
    // divergence activation on restart.
    let mut min_event_bucket_ts: Option<DateTime<Utc>> = None;

    loop {
        let rows = fetch_backfill_batch(
            &ctx.db_pool,
            from_ts,
            to_ts,
            &ctx.config.indicator.symbol,
            STARTUP_BACKFILL_MARKET,
            startup_backfill_batch_size,
            cursor.as_ref(),
        )
        .await?;
        if rows.is_empty() {
            break;
        }

        if let Some(last) = rows.last() {
            cursor = Some(BackfillCursor {
                event_ts: last.event_ts,
                msg_type: last.msg_type.clone(),
                market: last.market.clone(),
                symbol: last.symbol.clone(),
                routing_key: last.routing_key.clone(),
            });
        }

        for row in rows {
            // Extract ts_bucket before consuming the row.  All backfill event
            // types (trade, orderbook, liq, funding) include a "ts_bucket"
            // field formatted as RFC 3339 by the fetch SQL.
            if let Some(ts) = row
                .data_json
                .get("ts_bucket")
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&Utc))
            {
                min_event_bucket_ts = Some(min_event_bucket_ts.map_or(ts, |prev| prev.min(ts)));
            }

            match replay_row_to_engine_event(row) {
                Ok(event) => {
                    metrics.inc_processed(event.event_ts.timestamp_millis());
                    max_event_ts =
                        Some(max_event_ts.map_or(event.event_ts, |prev| prev.max(event.event_ts)));
                    state_store.ingest(event);
                    total_rows += 1;
                }
                Err(err) => {
                    warn!(error = %err, "decode startup backfill row failed");
                }
            }
        }
    }

    // Prime the scheduler so that process_ready_minutes starts from the oldest
    // backfilled bucket rather than jumping to latest_closed.  This ensures
    // every accumulated minute bucket is finalized in chronological order,
    // rebuilding full in-memory history on restart.
    if let Some(min_bucket) = min_event_bucket_ts {
        scheduler.prime_start_from(min_bucket);
        info!(
            min_bucket_ts = %min_bucket,
            "primed window scheduler from earliest backfill bucket"
        );
    }

    if max_event_ts.is_some() {
        // We queried replay rows with ts_event < to_ts, where to_ts is a minute boundary.
        // Drive scheduler watermark past this boundary so the last complete minute
        // (to_ts - 1m) is finalized.
        let backfill_watermark_ts = to_ts
            + ChronoDuration::seconds(ctx.config.indicator.watermark_lateness_secs.max(0) + 1);
        process_ready_minutes(
            ctx,
            metrics,
            dispatcher,
            state_store,
            scheduler,
            runtime_options,
            backfill_watermark_ts,
        )
        .await?;
    }

    info!(
        total_rows = total_rows,
        has_backfill = max_event_ts.is_some(),
        replay_cutoff_bucket_ts = %to_ts,
        "startup historical backfill completed"
    );

    if ctx.config.indicator.snapshot_verify_full_recompute {
        // TODO: Run a separate full in-memory backfill and compare MinuteHistory fields
        // with the snapshot-based path for the gap period. This is a development-only
        // validation feature. Not implemented in this iteration.
        info!("snapshot_verify_full_recompute is configured but not yet implemented");
    }
    let _ = snapshot_was_loaded; // used by snapshot_verify_full_recompute when implemented

    if max_event_ts.is_some() {
        Ok(Some(to_ts))
    } else {
        Ok(None)
    }
}

async fn latest_indicator_snapshot_ts(
    pool: &PgPool,
    symbol: &str,
) -> Result<Option<DateTime<Utc>>> {
    let row = sqlx::query(
        r#"
        SELECT max(ts_snapshot) AS max_ts
        FROM feat.indicator_snapshot
        WHERE symbol = $1
        "#,
    )
    .bind(symbol.to_uppercase())
    .fetch_one(pool)
    .await
    .context("query latest indicator snapshot ts")?;

    let max_ts: Option<DateTime<Utc>> = row.get("max_ts");
    Ok(max_ts)
}

async fn latest_indicator_progress_ts(
    pool: &PgPool,
    symbol: &str,
) -> Result<Option<DateTime<Utc>>> {
    let row = sqlx::query(
        r#"
        SELECT last_success_ts
        FROM feat.indicator_progress
        WHERE symbol = $1
        "#,
    )
    .bind(symbol.to_uppercase())
    .fetch_optional(pool)
    .await
    .context("query latest indicator progress ts")?;
    Ok(row.map(|r| r.get("last_success_ts")))
}

fn build_backfill_sql(filter_market: bool, with_cursor: bool) -> String {
    const SQL_TEMPLATE: &str = r#"
    WITH events AS (
        (
            SELECT
                t.ctid AS row_tid,
                'trade'::text AS src,
                t.ts_event AS event_ts,
                'md.agg.trade.1m'::text AS msg_type,
                t.market::text AS market,
                t.symbol AS symbol,
                format('md.agg.%s.trade.1m.%s', t.market::text, lower(t.symbol)) AS routing_key
            FROM md.agg_trade_1m t
            WHERE t.ts_bucket >= $1
              AND t.ts_bucket < $2
              AND t.symbol = $3
__TRADE_MARKET_FILTER__
__TRADE_CURSOR_FILTER__
            ORDER BY t.ts_event ASC, t.market ASC, t.symbol ASC
            LIMIT $__LIMIT_PARAM__
        )

        UNION ALL

        (
            SELECT
                b.ctid AS row_tid,
                'orderbook'::text AS src,
                b.ts_event AS event_ts,
                'md.agg.orderbook.1m'::text AS msg_type,
                b.market::text AS market,
                b.symbol AS symbol,
                format('md.agg.%s.orderbook.1m.%s', b.market::text, lower(b.symbol)) AS routing_key
            FROM md.agg_orderbook_1m b
            WHERE b.ts_bucket >= $1
              AND b.ts_bucket < $2
              AND b.symbol = $3
__ORDERBOOK_MARKET_FILTER__
__ORDERBOOK_CURSOR_FILTER__
            ORDER BY b.ts_event ASC, b.market ASC, b.symbol ASC
            LIMIT $__LIMIT_PARAM__
        )

        UNION ALL

        (
            SELECT
                l.ctid AS row_tid,
                'liq'::text AS src,
                l.ts_event AS event_ts,
                'md.agg.liq.1m'::text AS msg_type,
                l.market::text AS market,
                l.symbol AS symbol,
                format('md.agg.%s.liq.1m.%s', l.market::text, lower(l.symbol)) AS routing_key
            FROM md.agg_liq_1m l
            WHERE l.ts_bucket >= $1
              AND l.ts_bucket < $2
              AND l.symbol = $3
__LIQ_MARKET_FILTER__
__LIQ_CURSOR_FILTER__
            ORDER BY l.ts_event ASC, l.market ASC, l.symbol ASC
            LIMIT $__LIMIT_PARAM__
        )

        UNION ALL

        (
            SELECT
                f.ctid AS row_tid,
                'funding_mark'::text AS src,
                f.ts_event AS event_ts,
                'md.agg.funding_mark.1m'::text AS msg_type,
                f.market::text AS market,
                f.symbol AS symbol,
                format('md.agg.%s.funding_mark.1m.%s', f.market::text, lower(f.symbol)) AS routing_key
            FROM md.agg_funding_mark_1m f
            WHERE f.ts_bucket >= $1
              AND f.ts_bucket < $2
              AND f.symbol = $3
__FUNDING_MARKET_FILTER__
__FUNDING_CURSOR_FILTER__
            ORDER BY f.ts_event ASC, f.market ASC, f.symbol ASC
            LIMIT $__LIMIT_PARAM__
        )
    )
    , picked AS (
        SELECT row_tid, src, event_ts, msg_type, market, symbol, routing_key
        FROM events
__OUTER_WHERE__
        ORDER BY event_ts ASC, msg_type ASC, market ASC, symbol ASC, routing_key ASC
        LIMIT $__LIMIT_PARAM__
    )
    , expanded AS (
        SELECT
            p.event_ts,
            p.msg_type,
            p.market,
            p.symbol,
            p.routing_key,
            p.src,
            t.ts_bucket AS t_ts_bucket,
            t.chunk_start_ts AS t_chunk_start_ts,
            t.chunk_end_ts AS t_chunk_end_ts,
            t.source_event_count AS t_source_event_count,
            t.trade_count AS t_trade_count,
            t.buy_qty AS t_buy_qty,
            t.sell_qty AS t_sell_qty,
            t.buy_notional AS t_buy_notional,
            t.sell_notional AS t_sell_notional,
            t.first_price AS t_first_price,
            t.last_price AS t_last_price,
            t.high_price AS t_high_price,
            t.low_price AS t_low_price,
            t.profile_levels AS t_profile_levels,
            t.whale_json AS t_whale_json,
            t.payload_json AS t_payload_json,
            NULL::timestamptz AS b_ts_bucket,
            NULL::timestamptz AS b_chunk_start_ts,
            NULL::timestamptz AS b_chunk_end_ts,
            NULL::bigint AS b_source_event_count,
            NULL::bigint AS b_sample_count,
            NULL::bigint AS b_bbo_updates,
            NULL::double precision AS b_spread_sum,
            NULL::double precision AS b_topk_depth_sum,
            NULL::double precision AS b_obi_sum,
            NULL::double precision AS b_obi_l1_sum,
            NULL::double precision AS b_obi_k_sum,
            NULL::double precision AS b_obi_k_dw_sum,
            NULL::double precision AS b_obi_k_dw_change_sum,
            NULL::double precision AS b_obi_k_dw_adj_sum,
            NULL::double precision AS b_microprice_sum,
            NULL::double precision AS b_microprice_classic_sum,
            NULL::double precision AS b_microprice_kappa_sum,
            NULL::double precision AS b_microprice_adj_sum,
            NULL::double precision AS b_ofi_sum,
            NULL::double precision AS b_obi_k_dw_close,
            NULL::jsonb AS b_heatmap_levels,
            NULL::timestamptz AS l_ts_bucket,
            NULL::timestamptz AS l_chunk_start_ts,
            NULL::timestamptz AS l_chunk_end_ts,
            NULL::bigint AS l_source_event_count,
            NULL::jsonb AS l_force_liq_levels,
            NULL::timestamptz AS f_ts_bucket,
            NULL::timestamptz AS f_chunk_start_ts,
            NULL::timestamptz AS f_chunk_end_ts,
            NULL::bigint AS f_source_event_count,
            NULL::jsonb AS f_mark_points,
            NULL::jsonb AS f_funding_points
        FROM picked p
        JOIN md.agg_trade_1m t
          ON t.ctid = p.row_tid
        WHERE p.src = 'trade'

        UNION ALL

        SELECT
            p.event_ts,
            p.msg_type,
            p.market,
            p.symbol,
            p.routing_key,
            p.src,
            NULL::timestamptz AS t_ts_bucket,
            NULL::timestamptz AS t_chunk_start_ts,
            NULL::timestamptz AS t_chunk_end_ts,
            NULL::bigint AS t_source_event_count,
            NULL::bigint AS t_trade_count,
            NULL::double precision AS t_buy_qty,
            NULL::double precision AS t_sell_qty,
            NULL::double precision AS t_buy_notional,
            NULL::double precision AS t_sell_notional,
            NULL::double precision AS t_first_price,
            NULL::double precision AS t_last_price,
            NULL::double precision AS t_high_price,
            NULL::double precision AS t_low_price,
            NULL::jsonb AS t_profile_levels,
            NULL::jsonb AS t_whale_json,
            NULL::jsonb AS t_payload_json,
            b.ts_bucket AS b_ts_bucket,
            b.chunk_start_ts AS b_chunk_start_ts,
            b.chunk_end_ts AS b_chunk_end_ts,
            b.source_event_count AS b_source_event_count,
            b.sample_count AS b_sample_count,
            b.bbo_updates AS b_bbo_updates,
            b.spread_sum AS b_spread_sum,
            b.topk_depth_sum AS b_topk_depth_sum,
            b.obi_sum AS b_obi_sum,
            b.obi_l1_sum AS b_obi_l1_sum,
            b.obi_k_sum AS b_obi_k_sum,
            b.obi_k_dw_sum AS b_obi_k_dw_sum,
            b.obi_k_dw_change_sum AS b_obi_k_dw_change_sum,
            b.obi_k_dw_adj_sum AS b_obi_k_dw_adj_sum,
            b.microprice_sum AS b_microprice_sum,
            b.microprice_classic_sum AS b_microprice_classic_sum,
            b.microprice_kappa_sum AS b_microprice_kappa_sum,
            b.microprice_adj_sum AS b_microprice_adj_sum,
            b.ofi_sum AS b_ofi_sum,
            b.obi_k_dw_close AS b_obi_k_dw_close,
            b.heatmap_levels AS b_heatmap_levels,
            NULL::timestamptz AS l_ts_bucket,
            NULL::timestamptz AS l_chunk_start_ts,
            NULL::timestamptz AS l_chunk_end_ts,
            NULL::bigint AS l_source_event_count,
            NULL::jsonb AS l_force_liq_levels,
            NULL::timestamptz AS f_ts_bucket,
            NULL::timestamptz AS f_chunk_start_ts,
            NULL::timestamptz AS f_chunk_end_ts,
            NULL::bigint AS f_source_event_count,
            NULL::jsonb AS f_mark_points,
            NULL::jsonb AS f_funding_points
        FROM picked p
        JOIN md.agg_orderbook_1m b
          ON b.ctid = p.row_tid
        WHERE p.src = 'orderbook'

        UNION ALL

        SELECT
            p.event_ts,
            p.msg_type,
            p.market,
            p.symbol,
            p.routing_key,
            p.src,
            NULL::timestamptz AS t_ts_bucket,
            NULL::timestamptz AS t_chunk_start_ts,
            NULL::timestamptz AS t_chunk_end_ts,
            NULL::bigint AS t_source_event_count,
            NULL::bigint AS t_trade_count,
            NULL::double precision AS t_buy_qty,
            NULL::double precision AS t_sell_qty,
            NULL::double precision AS t_buy_notional,
            NULL::double precision AS t_sell_notional,
            NULL::double precision AS t_first_price,
            NULL::double precision AS t_last_price,
            NULL::double precision AS t_high_price,
            NULL::double precision AS t_low_price,
            NULL::jsonb AS t_profile_levels,
            NULL::jsonb AS t_whale_json,
            NULL::jsonb AS t_payload_json,
            NULL::timestamptz AS b_ts_bucket,
            NULL::timestamptz AS b_chunk_start_ts,
            NULL::timestamptz AS b_chunk_end_ts,
            NULL::bigint AS b_source_event_count,
            NULL::bigint AS b_sample_count,
            NULL::bigint AS b_bbo_updates,
            NULL::double precision AS b_spread_sum,
            NULL::double precision AS b_topk_depth_sum,
            NULL::double precision AS b_obi_sum,
            NULL::double precision AS b_obi_l1_sum,
            NULL::double precision AS b_obi_k_sum,
            NULL::double precision AS b_obi_k_dw_sum,
            NULL::double precision AS b_obi_k_dw_change_sum,
            NULL::double precision AS b_obi_k_dw_adj_sum,
            NULL::double precision AS b_microprice_sum,
            NULL::double precision AS b_microprice_classic_sum,
            NULL::double precision AS b_microprice_kappa_sum,
            NULL::double precision AS b_microprice_adj_sum,
            NULL::double precision AS b_ofi_sum,
            NULL::double precision AS b_obi_k_dw_close,
            NULL::jsonb AS b_heatmap_levels,
            l.ts_bucket AS l_ts_bucket,
            l.chunk_start_ts AS l_chunk_start_ts,
            l.chunk_end_ts AS l_chunk_end_ts,
            l.source_event_count AS l_source_event_count,
            l.force_liq_levels AS l_force_liq_levels,
            NULL::timestamptz AS f_ts_bucket,
            NULL::timestamptz AS f_chunk_start_ts,
            NULL::timestamptz AS f_chunk_end_ts,
            NULL::bigint AS f_source_event_count,
            NULL::jsonb AS f_mark_points,
            NULL::jsonb AS f_funding_points
        FROM picked p
        JOIN md.agg_liq_1m l
          ON l.ctid = p.row_tid
        WHERE p.src = 'liq'

        UNION ALL

        SELECT
            p.event_ts,
            p.msg_type,
            p.market,
            p.symbol,
            p.routing_key,
            p.src,
            NULL::timestamptz AS t_ts_bucket,
            NULL::timestamptz AS t_chunk_start_ts,
            NULL::timestamptz AS t_chunk_end_ts,
            NULL::bigint AS t_source_event_count,
            NULL::bigint AS t_trade_count,
            NULL::double precision AS t_buy_qty,
            NULL::double precision AS t_sell_qty,
            NULL::double precision AS t_buy_notional,
            NULL::double precision AS t_sell_notional,
            NULL::double precision AS t_first_price,
            NULL::double precision AS t_last_price,
            NULL::double precision AS t_high_price,
            NULL::double precision AS t_low_price,
            NULL::jsonb AS t_profile_levels,
            NULL::jsonb AS t_whale_json,
            NULL::jsonb AS t_payload_json,
            NULL::timestamptz AS b_ts_bucket,
            NULL::timestamptz AS b_chunk_start_ts,
            NULL::timestamptz AS b_chunk_end_ts,
            NULL::bigint AS b_source_event_count,
            NULL::bigint AS b_sample_count,
            NULL::bigint AS b_bbo_updates,
            NULL::double precision AS b_spread_sum,
            NULL::double precision AS b_topk_depth_sum,
            NULL::double precision AS b_obi_sum,
            NULL::double precision AS b_obi_l1_sum,
            NULL::double precision AS b_obi_k_sum,
            NULL::double precision AS b_obi_k_dw_sum,
            NULL::double precision AS b_obi_k_dw_change_sum,
            NULL::double precision AS b_obi_k_dw_adj_sum,
            NULL::double precision AS b_microprice_sum,
            NULL::double precision AS b_microprice_classic_sum,
            NULL::double precision AS b_microprice_kappa_sum,
            NULL::double precision AS b_microprice_adj_sum,
            NULL::double precision AS b_ofi_sum,
            NULL::double precision AS b_obi_k_dw_close,
            NULL::jsonb AS b_heatmap_levels,
            NULL::timestamptz AS l_ts_bucket,
            NULL::timestamptz AS l_chunk_start_ts,
            NULL::timestamptz AS l_chunk_end_ts,
            NULL::bigint AS l_source_event_count,
            NULL::jsonb AS l_force_liq_levels,
            f.ts_bucket AS f_ts_bucket,
            f.chunk_start_ts AS f_chunk_start_ts,
            f.chunk_end_ts AS f_chunk_end_ts,
            f.source_event_count AS f_source_event_count,
            f.mark_points AS f_mark_points,
            f.funding_points AS f_funding_points
        FROM picked p
        JOIN md.agg_funding_mark_1m f
          ON f.ctid = p.row_tid
        WHERE p.src = 'funding_mark'
    )
    SELECT *
    FROM expanded
    ORDER BY event_ts ASC, msg_type ASC, market ASC, symbol ASC, routing_key ASC
    "#;

    let market_param = 4usize;
    let limit_param = if filter_market { 5usize } else { 4usize };
    let cursor_ts_param = if filter_market { 6usize } else { 5usize };
    let cursor_msg_type_param = cursor_ts_param + 1;
    let cursor_market_param = cursor_ts_param + 2;
    let cursor_symbol_param = cursor_ts_param + 3;
    let cursor_routing_key_param = cursor_ts_param + 4;

    let mk_market_filter = |alias: &str| -> String {
        if filter_market {
            format!("              AND {alias}.market::text = ${market_param}")
        } else {
            String::new()
        }
    };

    let mk_cursor_filter = |alias: &str| -> String {
        if with_cursor {
            format!("              AND {alias}.ts_event >= ${cursor_ts_param}")
        } else {
            String::new()
        }
    };

    let outer_where = match (filter_market, with_cursor) {
        (false, false) => String::new(),
        (true, false) => format!("    WHERE market = ${market_param}\n"),
        (false, true) => format!(
            "    WHERE (event_ts, msg_type, market, symbol, routing_key)\n        > (${cursor_ts_param}::timestamptz, ${cursor_msg_type_param}::text, ${cursor_market_param}::text, ${cursor_symbol_param}::text, ${cursor_routing_key_param}::text)\n"
        ),
        (true, true) => format!(
            "    WHERE market = ${market_param}\n      AND (event_ts, msg_type, market, symbol, routing_key)\n        > (${cursor_ts_param}::timestamptz, ${cursor_msg_type_param}::text, ${cursor_market_param}::text, ${cursor_symbol_param}::text, ${cursor_routing_key_param}::text)\n"
        ),
    };

    SQL_TEMPLATE
        .replace("__TRADE_MARKET_FILTER__", &mk_market_filter("t"))
        .replace("__ORDERBOOK_MARKET_FILTER__", &mk_market_filter("b"))
        .replace("__LIQ_MARKET_FILTER__", &mk_market_filter("l"))
        .replace("__FUNDING_MARKET_FILTER__", &mk_market_filter("f"))
        .replace("__TRADE_CURSOR_FILTER__", &mk_cursor_filter("t"))
        .replace("__ORDERBOOK_CURSOR_FILTER__", &mk_cursor_filter("b"))
        .replace("__LIQ_CURSOR_FILTER__", &mk_cursor_filter("l"))
        .replace("__FUNDING_CURSOR_FILTER__", &mk_cursor_filter("f"))
        .replace("__OUTER_WHERE__", &outer_where)
        .replace("__LIMIT_PARAM__", &limit_param.to_string())
}

fn require_backfill_field<T>(src: &str, field: &'static str, value: Option<T>) -> Result<T> {
    value.with_context(|| {
        format!("startup backfill row missing required field {field} for src={src}")
    })
}

fn build_backfill_data_json(row: &PgRow, src: &str) -> Result<Value> {
    match src {
        "trade" => {
            let ts_bucket = require_backfill_field(
                src,
                "t_ts_bucket",
                row.get::<Option<DateTime<Utc>>, _>("t_ts_bucket"),
            )?;
            let chunk_start_ts = require_backfill_field(
                src,
                "t_chunk_start_ts",
                row.get::<Option<DateTime<Utc>>, _>("t_chunk_start_ts"),
            )?;
            let chunk_end_ts = require_backfill_field(
                src,
                "t_chunk_end_ts",
                row.get::<Option<DateTime<Utc>>, _>("t_chunk_end_ts"),
            )?;
            let trade_count = require_backfill_field(
                src,
                "t_trade_count",
                row.get::<Option<i64>, _>("t_trade_count"),
            )?;
            let buy_qty =
                require_backfill_field(src, "t_buy_qty", row.get::<Option<f64>, _>("t_buy_qty"))?;
            let sell_qty =
                require_backfill_field(src, "t_sell_qty", row.get::<Option<f64>, _>("t_sell_qty"))?;
            let buy_notional = require_backfill_field(
                src,
                "t_buy_notional",
                row.get::<Option<f64>, _>("t_buy_notional"),
            )?;
            let sell_notional = require_backfill_field(
                src,
                "t_sell_notional",
                row.get::<Option<f64>, _>("t_sell_notional"),
            )?;
            let profile_levels = require_backfill_field(
                src,
                "t_profile_levels",
                row.get::<Option<Value>, _>("t_profile_levels"),
            )?;
            let whale = require_backfill_field(
                src,
                "t_whale_json",
                row.get::<Option<Value>, _>("t_whale_json"),
            )?;
            let payload_json = row
                .get::<Option<Value>, _>("t_payload_json")
                .unwrap_or_else(|| json!({}));

            Ok(json!({
                "ts_bucket": ts_bucket,
                "chunk_start_ts": chunk_start_ts,
                "chunk_end_ts": chunk_end_ts,
                "source_event_count": row.get::<Option<i64>, _>("t_source_event_count"),
                "trade_count": trade_count,
                "buy_qty": buy_qty,
                "sell_qty": sell_qty,
                "buy_notional": buy_notional,
                "sell_notional": sell_notional,
                "first_price": row.get::<Option<f64>, _>("t_first_price"),
                "last_price": row.get::<Option<f64>, _>("t_last_price"),
                "high_price": row.get::<Option<f64>, _>("t_high_price"),
                "low_price": row.get::<Option<f64>, _>("t_low_price"),
                "profile_levels": profile_levels,
                "whale": whale,
                "payload_json": payload_json
            }))
        }
        "orderbook" => {
            let ts_bucket = require_backfill_field(
                src,
                "b_ts_bucket",
                row.get::<Option<DateTime<Utc>>, _>("b_ts_bucket"),
            )?;
            let chunk_start_ts = require_backfill_field(
                src,
                "b_chunk_start_ts",
                row.get::<Option<DateTime<Utc>>, _>("b_chunk_start_ts"),
            )?;
            let chunk_end_ts = require_backfill_field(
                src,
                "b_chunk_end_ts",
                row.get::<Option<DateTime<Utc>>, _>("b_chunk_end_ts"),
            )?;
            let sample_count = require_backfill_field(
                src,
                "b_sample_count",
                row.get::<Option<i64>, _>("b_sample_count"),
            )?;
            let bbo_updates = require_backfill_field(
                src,
                "b_bbo_updates",
                row.get::<Option<i64>, _>("b_bbo_updates"),
            )?;
            let spread_sum = require_backfill_field(
                src,
                "b_spread_sum",
                row.get::<Option<f64>, _>("b_spread_sum"),
            )?;
            let topk_depth_sum = require_backfill_field(
                src,
                "b_topk_depth_sum",
                row.get::<Option<f64>, _>("b_topk_depth_sum"),
            )?;
            let obi_sum =
                require_backfill_field(src, "b_obi_sum", row.get::<Option<f64>, _>("b_obi_sum"))?;
            let obi_l1_sum = require_backfill_field(
                src,
                "b_obi_l1_sum",
                row.get::<Option<f64>, _>("b_obi_l1_sum"),
            )?;
            let obi_k_sum = require_backfill_field(
                src,
                "b_obi_k_sum",
                row.get::<Option<f64>, _>("b_obi_k_sum"),
            )?;
            let obi_k_dw_sum = require_backfill_field(
                src,
                "b_obi_k_dw_sum",
                row.get::<Option<f64>, _>("b_obi_k_dw_sum"),
            )?;
            let obi_k_dw_change_sum = require_backfill_field(
                src,
                "b_obi_k_dw_change_sum",
                row.get::<Option<f64>, _>("b_obi_k_dw_change_sum"),
            )?;
            let obi_k_dw_adj_sum = require_backfill_field(
                src,
                "b_obi_k_dw_adj_sum",
                row.get::<Option<f64>, _>("b_obi_k_dw_adj_sum"),
            )?;
            let microprice_sum = require_backfill_field(
                src,
                "b_microprice_sum",
                row.get::<Option<f64>, _>("b_microprice_sum"),
            )?;
            let microprice_classic_sum = require_backfill_field(
                src,
                "b_microprice_classic_sum",
                row.get::<Option<f64>, _>("b_microprice_classic_sum"),
            )?;
            let microprice_kappa_sum = require_backfill_field(
                src,
                "b_microprice_kappa_sum",
                row.get::<Option<f64>, _>("b_microprice_kappa_sum"),
            )?;
            let microprice_adj_sum = require_backfill_field(
                src,
                "b_microprice_adj_sum",
                row.get::<Option<f64>, _>("b_microprice_adj_sum"),
            )?;
            let ofi_sum =
                require_backfill_field(src, "b_ofi_sum", row.get::<Option<f64>, _>("b_ofi_sum"))?;
            let heatmap_levels = require_backfill_field(
                src,
                "b_heatmap_levels",
                row.get::<Option<Value>, _>("b_heatmap_levels"),
            )?;

            Ok(json!({
                "ts_bucket": ts_bucket,
                "chunk_start_ts": chunk_start_ts,
                "chunk_end_ts": chunk_end_ts,
                "source_event_count": row.get::<Option<i64>, _>("b_source_event_count"),
                "sample_count": sample_count,
                "bbo_updates": bbo_updates,
                "spread_sum": spread_sum,
                "topk_depth_sum": topk_depth_sum,
                "obi_sum": obi_sum,
                "obi_l1_sum": obi_l1_sum,
                "obi_k_sum": obi_k_sum,
                "obi_k_dw_sum": obi_k_dw_sum,
                "obi_k_dw_change_sum": obi_k_dw_change_sum,
                "obi_k_dw_adj_sum": obi_k_dw_adj_sum,
                "microprice_sum": microprice_sum,
                "microprice_classic_sum": microprice_classic_sum,
                "microprice_kappa_sum": microprice_kappa_sum,
                "microprice_adj_sum": microprice_adj_sum,
                "ofi_sum": ofi_sum,
                "obi_k_dw_close": row.get::<Option<f64>, _>("b_obi_k_dw_close"),
                "heatmap_levels": heatmap_levels
            }))
        }
        "liq" => {
            let ts_bucket = require_backfill_field(
                src,
                "l_ts_bucket",
                row.get::<Option<DateTime<Utc>>, _>("l_ts_bucket"),
            )?;
            let chunk_start_ts = require_backfill_field(
                src,
                "l_chunk_start_ts",
                row.get::<Option<DateTime<Utc>>, _>("l_chunk_start_ts"),
            )?;
            let chunk_end_ts = require_backfill_field(
                src,
                "l_chunk_end_ts",
                row.get::<Option<DateTime<Utc>>, _>("l_chunk_end_ts"),
            )?;
            let force_liq_levels = require_backfill_field(
                src,
                "l_force_liq_levels",
                row.get::<Option<Value>, _>("l_force_liq_levels"),
            )?;

            Ok(json!({
                "ts_bucket": ts_bucket,
                "chunk_start_ts": chunk_start_ts,
                "chunk_end_ts": chunk_end_ts,
                "source_event_count": row.get::<Option<i64>, _>("l_source_event_count"),
                "force_liq_levels": force_liq_levels
            }))
        }
        "funding_mark" => {
            let ts_bucket = require_backfill_field(
                src,
                "f_ts_bucket",
                row.get::<Option<DateTime<Utc>>, _>("f_ts_bucket"),
            )?;
            let chunk_start_ts = require_backfill_field(
                src,
                "f_chunk_start_ts",
                row.get::<Option<DateTime<Utc>>, _>("f_chunk_start_ts"),
            )?;
            let chunk_end_ts = require_backfill_field(
                src,
                "f_chunk_end_ts",
                row.get::<Option<DateTime<Utc>>, _>("f_chunk_end_ts"),
            )?;
            let mark_points = require_backfill_field(
                src,
                "f_mark_points",
                row.get::<Option<Value>, _>("f_mark_points"),
            )?;
            let funding_points = require_backfill_field(
                src,
                "f_funding_points",
                row.get::<Option<Value>, _>("f_funding_points"),
            )?;

            Ok(json!({
                "ts_bucket": ts_bucket,
                "chunk_start_ts": chunk_start_ts,
                "chunk_end_ts": chunk_end_ts,
                "source_event_count": row.get::<Option<i64>, _>("f_source_event_count"),
                "mark_points": mark_points,
                "funding_points": funding_points
            }))
        }
        _ => anyhow::bail!("unsupported startup backfill source: {src}"),
    }
}

pub async fn fetch_backfill_batch(
    pool: &PgPool,
    from_ts: DateTime<Utc>,
    to_ts: DateTime<Utc>,
    symbol: &str,
    market: &str,
    limit: i64,
    cursor: Option<&BackfillCursor>,
) -> Result<Vec<ReplayRow>> {
    let filter_market = !market.eq_ignore_ascii_case("all");
    let with_cursor = cursor.is_some();
    let sql = build_backfill_sql(filter_market, with_cursor);
    let symbol_upper = symbol.to_uppercase();

    let rows = match (filter_market, cursor) {
        (false, None) => {
            sqlx::query(&sql)
                .bind(from_ts)
                .bind(to_ts)
                .bind(&symbol_upper)
                .bind(limit)
                .fetch_all(pool)
                .await
        }
        (false, Some(c)) => {
            sqlx::query(&sql)
                .bind(from_ts)
                .bind(to_ts)
                .bind(&symbol_upper)
                .bind(limit)
                .bind(c.event_ts)
                .bind(&c.msg_type)
                .bind(&c.market)
                .bind(&c.symbol)
                .bind(&c.routing_key)
                .fetch_all(pool)
                .await
        }
        (true, None) => {
            sqlx::query(&sql)
                .bind(from_ts)
                .bind(to_ts)
                .bind(&symbol_upper)
                .bind(market)
                .bind(limit)
                .fetch_all(pool)
                .await
        }
        (true, Some(c)) => {
            sqlx::query(&sql)
                .bind(from_ts)
                .bind(to_ts)
                .bind(&symbol_upper)
                .bind(market)
                .bind(limit)
                .bind(c.event_ts)
                .bind(&c.msg_type)
                .bind(&c.market)
                .bind(&c.symbol)
                .bind(&c.routing_key)
                .fetch_all(pool)
                .await
        }
    }
    .context("fetch startup backfill batch")?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let src: String = row.get("src");
        let data_json = build_backfill_data_json(&row, &src)
            .with_context(|| format!("build startup backfill payload for src={src}"))?;

        out.push(ReplayRow {
            event_ts: row.get("event_ts"),
            msg_type: row.get("msg_type"),
            market: row.get("market"),
            symbol: row.get("symbol"),
            routing_key: row.get("routing_key"),
            data_json,
        });
    }

    Ok(out)
}

pub fn replay_row_to_engine_event(row: ReplayRow) -> Result<EngineEvent> {
    let payload = json!({
        "schema_version": 1,
        "msg_type": row.msg_type,
        "message_id": Uuid::new_v4(),
        "trace_id": Uuid::new_v4(),
        "routing_key": row.routing_key,
        "market": row.market,
        "symbol": row.symbol,
        "source_kind": "replay",
        "backfill_in_progress": true,
        "event_ts": row.event_ts.to_rfc3339(),
        "published_at": Utc::now().to_rfc3339(),
        "data": row.data_json
    });

    let bytes = serde_json::to_vec(&payload).context("encode startup replay payload")?;
    decode_contract_body(&bytes).context("decode startup replay payload")
}

async fn export_snapshots(
    export_dir: &str,
    ts_bucket: chrono::DateTime<Utc>,
    symbol: &str,
    snapshots: &[IndicatorSnapshotRow],
) -> Result<()> {
    let path = Path::new(export_dir);
    tokio::fs::create_dir_all(path).await?;

    let filename = format!(
        "indicators_{}_{}.json",
        symbol.to_lowercase(),
        ts_bucket.format("%Y%m%d_%H%M")
    );
    let full_path = path.join(filename);

    let body = serde_json::json!({
        "ts_bucket": ts_bucket.to_rfc3339(),
        "symbol": symbol,
        "count": snapshots.len(),
        "indicators": snapshots.iter().map(|s| {
            serde_json::json!({
                "indicator_code": s.indicator_code,
                "window_code": s.window_code,
                "payload": s.payload_json
            })
        }).collect::<Vec<_>>()
    });

    let buf = serde_json::to_vec_pretty(&body)?;
    tokio::fs::write(full_path, buf).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        build_backfill_sql, find_long_null_price_run, minute_history_is_strictly_contiguous,
    };
    use crate::ingest::decoder::MarketKind;
    use crate::runtime::state_store::MinuteHistory;
    use chrono::{Duration as ChronoDuration, TimeZone, Utc};
    use std::collections::BTreeMap;

    const MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT: usize = 60;

    fn history_row(ts_bucket: chrono::DateTime<Utc>) -> MinuteHistory {
        MinuteHistory {
            ts_bucket,
            market: MarketKind::Futures,
            open_price: None,
            high_price: None,
            low_price: None,
            close_price: None,
            last_price: None,
            buy_qty: 0.0,
            sell_qty: 0.0,
            total_qty: 0.0,
            total_notional: 0.0,
            delta: 0.0,
            relative_delta: 0.0,
            force_liq: BTreeMap::new(),
            ofi: 0.0,
            spread_twa: None,
            topk_depth_twa: None,
            obi_twa: None,
            obi_l1_twa: None,
            obi_k_twa: None,
            obi_k_dw_twa: None,
            obi_k_dw_close: None,
            obi_k_dw_change: None,
            obi_k_dw_adj_twa: None,
            bbo_updates: 0,
            microprice_twa: None,
            microprice_classic_twa: None,
            microprice_kappa_twa: None,
            microprice_adj_twa: None,
            cvd: 0.0,
            vpin: 0.0,
            avwap_minute: None,
            whale_trade_count: 0,
            whale_buy_count: 0,
            whale_sell_count: 0,
            whale_notional_total: 0.0,
            whale_notional_buy: 0.0,
            whale_notional_sell: 0.0,
            whale_qty_eth_total: 0.0,
            whale_qty_eth_buy: 0.0,
            whale_qty_eth_sell: 0.0,
            profile: BTreeMap::new(),
        }
    }

    fn priced_history_row(ts_bucket: chrono::DateTime<Utc>, price: f64) -> MinuteHistory {
        MinuteHistory {
            open_price: Some(price),
            high_price: Some(price),
            low_price: Some(price),
            close_price: Some(price),
            last_price: Some(price),
            total_qty: 1.0,
            total_notional: price,
            ..history_row(ts_bucket)
        }
    }

    #[test]
    fn startup_backfill_sql_filters_canonical_rows_by_ts_bucket() {
        let sql = build_backfill_sql(false, false);
        assert!(sql.contains("WHERE t.ts_bucket >= $1"));
        assert!(sql.contains("AND t.ts_bucket < $2"));
        assert!(sql.contains("WHERE b.ts_bucket >= $1"));
        assert!(sql.contains("AND b.ts_bucket < $2"));
        assert!(sql.contains("WHERE l.ts_bucket >= $1"));
        assert!(sql.contains("AND l.ts_bucket < $2"));
        assert!(sql.contains("WHERE f.ts_bucket >= $1"));
        assert!(sql.contains("AND f.ts_bucket < $2"));
        assert!(!sql.contains("WHERE t.ts_event >= $1"));
        assert!(!sql.contains("AND t.ts_event < $2"));
    }

    #[test]
    fn startup_backfill_sql_expands_rows_by_source_without_left_join_hash_fanout() {
        let sql = build_backfill_sql(false, true);
        assert!(sql.contains(", expanded AS ("));
        assert!(sql.contains("FROM picked p\n        JOIN md.agg_trade_1m t"));
        assert!(sql.contains("FROM picked p\n        JOIN md.agg_orderbook_1m b"));
        assert!(sql.contains("FROM picked p\n        JOIN md.agg_liq_1m l"));
        assert!(sql.contains("FROM picked p\n        JOIN md.agg_funding_mark_1m f"));
        assert!(!sql.contains("LEFT JOIN md.agg_trade_1m"));
        assert!(!sql.contains("LEFT JOIN md.agg_orderbook_1m"));
        assert!(!sql.contains("LEFT JOIN md.agg_liq_1m"));
        assert!(!sql.contains("LEFT JOIN md.agg_funding_mark_1m"));
    }

    #[test]
    fn snapshot_history_contiguity_accepts_strict_minute_series() {
        let start = Utc.with_ymd_and_hms(2026, 3, 10, 5, 0, 0).single().unwrap();
        let history = vec![
            history_row(start),
            history_row(start + ChronoDuration::minutes(1)),
            history_row(start + ChronoDuration::minutes(2)),
        ];
        assert!(minute_history_is_strictly_contiguous(
            &history,
            start + ChronoDuration::minutes(2)
        ));
    }

    #[test]
    fn snapshot_history_contiguity_rejects_gapped_or_out_of_order_series() {
        let start = Utc.with_ymd_and_hms(2026, 3, 10, 5, 0, 0).single().unwrap();
        let gapped = vec![
            history_row(start),
            history_row(start + ChronoDuration::minutes(2)),
        ];
        assert!(!minute_history_is_strictly_contiguous(
            &gapped,
            start + ChronoDuration::minutes(2)
        ));

        let out_of_order = vec![
            history_row(start + ChronoDuration::minutes(1)),
            history_row(start),
        ];
        assert!(!minute_history_is_strictly_contiguous(&out_of_order, start));
    }

    #[test]
    fn snapshot_null_price_guard_accepts_short_run() {
        let start = Utc.with_ymd_and_hms(2026, 3, 10, 5, 0, 0).single().unwrap();
        let mut history = vec![priced_history_row(start, 2000.0)];
        for offset in 1..MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT {
            history.push(history_row(start + ChronoDuration::minutes(offset as i64)));
        }
        history.push(priced_history_row(
            start + ChronoDuration::minutes(MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT as i64),
            2001.0,
        ));

        assert_eq!(
            find_long_null_price_run(&history, MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT),
            None
        );
    }

    #[test]
    fn snapshot_null_price_guard_rejects_long_run() {
        let start = Utc.with_ymd_and_hms(2026, 3, 10, 5, 0, 0).single().unwrap();
        let mut history = vec![priced_history_row(start, 2000.0)];
        for offset in 1..=MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT {
            history.push(history_row(start + ChronoDuration::minutes(offset as i64)));
        }

        let run =
            find_long_null_price_run(&history, MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT)
                .expect("expected long null-price run");
        assert_eq!(run.0, start + ChronoDuration::minutes(1));
        assert_eq!(
            run.1,
            start + ChronoDuration::minutes(MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT as i64)
        );
        assert_eq!(run.2, MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT);
    }
}
