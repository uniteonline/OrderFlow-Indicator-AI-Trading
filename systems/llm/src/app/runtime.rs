use crate::app::bootstrap::AppContext;
use crate::app::config::RootConfig;
use crate::app::telegram::{TelegramOperator, TradeSignalNotification};
use crate::app::x::XOperator;
use crate::execution::binance::{
    execute_management_intent, execute_pending_order_intent, execute_trade_intent,
    fetch_pending_order_leverage, fetch_symbol_trading_state,
};
use crate::llm::decision::{
    pending_order_management_intent_from_value_with_context,
    position_management_intent_from_value_with_context, trade_intent_from_value,
    PendingOrderContext, PendingOrderManagementDecision, PositionManagementDecision, TradeDecision,
    TradeIntent,
};
use crate::llm::filter::{core::CoreFilter, scan::ScanFilter};
use crate::llm::helper::PreComputedVHelper;
use crate::llm::provider::{
    invoke_models, EntryContextForLlm, EntryStagePromptInputCapture, ManagementSnapshotForLlm,
    ModelInvocationInput, PendingOrderSummaryForLlm, PositionContextForLlm, PositionSummaryForLlm,
    ReductionHistoryItemForLlm,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, NaiveDateTime, Timelike, Utc};
use futures_util::StreamExt;
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, BasicQosOptions, QueuePurgeOptions},
    types::FieldTable,
};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::{PgPool, Row};
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep_until, Duration, Instant, Sleep};
use tracing::{debug, error, info, warn};

const TEMP_INDICATOR_DIR: &str = "systems/llm/temp_indicator";
const TEMP_MODEL_INPUT_DIR: &str = "systems/llm/temp_model_input";
const TEMP_MODEL_OUTPUT_DIR: &str = "systems/llm/temp_model_output";
const LLM_JOURNAL_DIR: &str = "systems/llm/journal";
const LLM_JOURNAL_FILE: &str = "systems/llm/journal/llm_trade_journal.jsonl";
const MANAGEMENT_REDUCTION_LEVEL_THRESHOLD: f64 = 0.5;
const KLINE_DB_BACKFILL_INTERVALS: [(&str, i64); 2] = [("4h", 240), ("1d", 1440)];

#[derive(Debug, Default)]
struct InvokeThrottleState {
    last_invoke_at: Option<Instant>,
}

#[derive(Debug, Clone)]
struct ReductionHistoryEntry {
    time: String,
    qty_ratio: f64,
    reason_summary: String,
    price: f64,
}

#[derive(Debug, Clone, Default)]
struct PositionContextState {
    original_qty: f64,
    last_management_action: Option<String>,
    last_management_reason: Option<String>,
    reduction_history: Vec<ReductionHistoryEntry>,
    /// Effective entry used by execution path after remap + maker-price resolution.
    effective_entry_price: Option<f64>,
    /// Effective stop loss used by execution path after final RR-based recomputation.
    effective_stop_loss: Option<f64>,
    /// Effective take profit used by execution path after final quantization.
    effective_take_profit: Option<f64>,
    /// Effective leverage used by the execution path.
    effective_leverage: Option<u32>,
    /// Captured from the entry model response when the position was opened.
    entry_context: Option<EntryContextForState>,
}

#[derive(Debug, Clone, Default)]
struct SymbolLifecycleState {
    last_management_reason: Option<String>,
    contexts: HashMap<String, PositionContextState>,
}

#[derive(Debug, Default)]
struct RuntimeLifecycleStore {
    symbols: HashMap<String, SymbolLifecycleState>,
}

impl RuntimeLifecycleStore {
    fn symbol_state(&self, symbol: &str) -> Option<&SymbolLifecycleState> {
        self.symbols.get(&symbol.to_ascii_uppercase())
    }

    fn symbol_state_mut(&mut self, symbol: &str) -> &mut SymbolLifecycleState {
        self.symbols.entry(symbol.to_ascii_uppercase()).or_default()
    }

    fn last_management_reason(&self, symbol: &str) -> Option<String> {
        self.symbol_state(symbol)
            .and_then(|state| state.last_management_reason.clone())
    }

    fn set_last_management_reason(&mut self, symbol: &str, reason: Option<String>) {
        let key = symbol.to_ascii_uppercase();
        let state = self.symbols.entry(key.clone()).or_default();
        state.last_management_reason = reason;
        if state.last_management_reason.is_none() && state.contexts.is_empty() {
            self.symbols.remove(&key);
        }
    }
}

/// Internal (non-serialized) mirror of EntryContextForLlm, stored in memory.
#[derive(Debug, Clone)]
struct EntryContextForState {
    entry_strategy: Option<String>,
    stop_model: Option<String>,
    entry_mode: Option<String>,
    original_tp: Option<f64>,
    original_sl: Option<f64>,
    sweep_wick_extreme: Option<f64>,
    horizon: Option<String>,
    entry_reason: String,
    /// Helper-derived V captured at entry time for internal audit/journal continuity.
    /// This is not serialized into management-mode prompt inputs.
    entry_v: Option<f64>,
}

#[derive(Debug, Clone, Copy)]
struct TradeSignalFields {
    entry_price: Option<f64>,
    leverage: Option<f64>,
    risk_reward_ratio: Option<f64>,
    take_profit: Option<f64>,
    stop_loss: Option<f64>,
}

impl TradeSignalFields {
    fn from_intent(intent: &TradeIntent) -> Self {
        Self {
            entry_price: intent.entry_price,
            leverage: intent.leverage,
            risk_reward_ratio: intent.risk_reward_ratio,
            take_profit: intent.take_profit,
            stop_loss: intent.stop_loss,
        }
    }

    fn from_report(report: &crate::execution::binance::ExecutionReport) -> Self {
        Self {
            entry_price: Some(report.maker_entry_price),
            leverage: Some(report.leverage as f64),
            risk_reward_ratio: Some(report.actual_risk_reward_ratio),
            take_profit: Some(report.actual_take_profit),
            stop_loss: Some(report.actual_stop_loss),
        }
    }
}

#[derive(Debug, Clone)]
struct ResolvedEntryV {
    value: f64,
    timeframe: &'static str,
    basis: String,
}

#[derive(Debug, Clone)]
struct EntryVGateResult {
    resolved_v: ResolvedEntryV,
    take_profit_distance: f64,
    take_profit_distance_v: f64,
    min_distance_v: f64,
    passed: bool,
}

#[derive(Debug, Clone)]
struct EntryRrGateResult {
    risk_reward_ratio: f64,
    reward_distance: f64,
    risk_distance: f64,
    min_rr: f64,
    passed: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct MinuteBundleEnvelope {
    msg_type: String,
    routing_key: String,
    symbol: String,
    ts_bucket: DateTime<Utc>,
    window_code: String,
    indicator_count: usize,
    published_at: Option<DateTime<Utc>>,
    indicators: Value,
}

#[derive(Debug, Clone)]
struct LatestBundle {
    raw: MinuteBundleEnvelope,
    indicators: Value,
    missing_indicator_codes: Vec<String>,
    received_at: DateTime<Utc>,
}

pub async fn run(ctx: AppContext) -> Result<()> {
    ensure_temp_indicator_dir().await?;
    ensure_temp_model_input_dir().await?;
    ensure_llm_journal_dir().await?;

    if ctx.config.llm.purge_queue_on_start {
        let purged = ctx
            .mq_consume_channel
            .queue_purge(&ctx.consume_queue_name, QueuePurgeOptions::default())
            .await
            .with_context(|| format!("purge llm consume queue {}", ctx.consume_queue_name))?;
        debug!(
            queue = %ctx.consume_queue_name,
            purged = purged,
            "llm startup queue purge completed"
        );
    } else {
        debug!(
            queue = %ctx.consume_queue_name,
            "llm startup queue purge disabled (llm.purge_queue_on_start=false)"
        );
    }

    ctx.mq_consume_channel
        .basic_qos(500, BasicQosOptions::default())
        .await
        .context("set llm queue qos")?;

    let consumer_tag = format!(
        "llm_{}_{}",
        ctx.consume_queue_name.replace('.', "_"),
        &ctx.producer_instance_id
    );
    let mut consumer = ctx
        .mq_consume_channel
        .basic_consume(
            &ctx.consume_queue_name,
            &consumer_tag,
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .with_context(|| format!("consume queue {}", ctx.consume_queue_name))?;

    let mut pending_invoke_bundle: Option<LatestBundle> = None;
    let mut last_invoked_ts_bucket: Option<DateTime<Utc>> = None;
    let runtime_lifecycle_state = Arc::new(Mutex::new(RuntimeLifecycleStore::default()));
    restore_last_management_reasons_from_journal(&runtime_lifecycle_state).await?;
    let invoke_inflight = Arc::new(AtomicBool::new(false));
    let invoke_throttle = Arc::new(Mutex::new(InvokeThrottleState::default()));
    let active_provider = ctx.config.active_default_model();
    let schedule_minutes = effective_schedule_minutes(&ctx.config, &active_provider);
    let min_invoke_interval_secs =
        effective_min_invoke_interval_secs(&ctx.config, &active_provider);
    let min_invoke_interval = Duration::from_secs(min_invoke_interval_secs);
    let apply_min_invoke_interval_throttle = schedule_minutes.is_empty();
    let disabled_deadline = Instant::now() + Duration::from_secs(365 * 24 * 60 * 60);
    let mut settle_timer: Pin<Box<Sleep>> = Box::pin(sleep_until(disabled_deadline));

    debug!(
        queue = %ctx.consume_queue_name,
        symbol = %ctx.config.llm.symbol,
        request_enabled = ctx.config.llm.request_enabled,
        active_provider = %active_provider,
        prompt_template = %ctx.config.llm.prompt_template,
        purge_queue_on_start = ctx.config.llm.purge_queue_on_start,
        call_schedule_minutes = ?schedule_minutes,
        bundle_settle_ms = ctx.config.llm.bundle_settle_ms,
        min_invoke_interval_secs = min_invoke_interval_secs,
        apply_min_invoke_interval_throttle = apply_min_invoke_interval_throttle,
        "llm runtime started"
    );

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                debug!("shutdown signal received, stopping llm runtime");
                break;
            }
            _ = &mut settle_timer => {
                queue_latest_bundle_invoke(
                    &ctx,
                    &pending_invoke_bundle,
                    &mut last_invoked_ts_bucket,
                    &invoke_inflight,
                    &invoke_throttle,
                    &runtime_lifecycle_state,
                    min_invoke_interval,
                    apply_min_invoke_interval_throttle,
                    "scheduled_bundle",
                );
                pending_invoke_bundle = None;
                settle_timer.as_mut().reset(disabled_deadline);
            }
            maybe_delivery = consumer.next() => {
                let Some(delivery_result) = maybe_delivery else {
                    warn!("llm mq consumer stream ended");
                    break;
                };
                match delivery_result {
                    Ok(delivery) => {
                        let now = Utc::now();
                        let parse_result = serde_json::from_slice::<MinuteBundleEnvelope>(&delivery.data);
                        match parse_result {
                            Ok(bundle) => {
                                if bundle.msg_type != "ind.minute_bundle" || bundle.window_code != "1m" {
                                    if let Err(err) = delivery.ack(BasicAckOptions::default()).await {
                                        warn!(error = %err, "llm ack non-target message failed");
                                    }
                                    continue;
                                }
                                if !bundle.symbol.eq_ignore_ascii_case(&ctx.config.llm.symbol) {
                                    if let Err(err) = delivery.ack(BasicAckOptions::default()).await {
                                        warn!(error = %err, "llm ack non-symbol message failed");
                                    }
                                    continue;
                                }

                                let consume_staleness_secs = now
                                    .signed_duration_since(bundle.ts_bucket)
                                    .num_seconds();
                                let max_consume_stale =
                                    ctx.config.llm.bundle_consume_stale_secs as i64;
                                if consume_staleness_secs > max_consume_stale {
                                    debug!(
                                        symbol = %bundle.symbol,
                                        ts_bucket = %bundle.ts_bucket,
                                        staleness_secs = consume_staleness_secs,
                                        max_consume_stale_secs = max_consume_stale,
                                        "llm consumer discarding stale bundle (backfill/replay data)"
                                    );
                                    if let Err(err) =
                                        delivery.ack(BasicAckOptions::default()).await
                                    {
                                        warn!(error = %err, "llm ack stale bundle failed");
                                    }
                                    continue;
                                }

                                let missing = Vec::new();

                                if let Err(err) = persist_bundle_to_disk(
                                    &bundle,
                                    &delivery.data,
                                    ctx.config.llm.temp_cache_retention_minutes,
                                )
                                .await
                                {
                                    warn!(
                                        symbol = %bundle.symbol,
                                        ts_bucket = %bundle.ts_bucket,
                                        error = %err,
                                        "persist minute bundle to temp_indicator failed"
                                    );
                                }

                                let current_bundle = LatestBundle {
                                    raw: bundle.clone(),
                                    indicators: bundle.indicators.clone(),
                                    missing_indicator_codes: missing.clone(),
                                    received_at: now,
                                };

                                debug!(
                                    symbol = %bundle.symbol,
                                    ts_bucket = %bundle.ts_bucket,
                                    indicator_count = bundle.indicator_count,
                                    missing_count = missing.len(),
                                    routing_key = %bundle.routing_key,
                                    "llm received minute indicator bundle"
                                );

                                if bundle_matches_call_schedule(&bundle, &schedule_minutes) {
                                    pending_invoke_bundle = Some(current_bundle);
                                    settle_timer.as_mut().reset(
                                        Instant::now() + Duration::from_millis(ctx.config.llm.bundle_settle_ms)
                                    );
                                }
                            }
                            Err(err) => {
                                warn!(
                                    error = %err,
                                    payload = %String::from_utf8_lossy(&delivery.data),
                                    "llm decode minute bundle failed"
                                );
                            }
                        }

                        if let Err(err) = delivery.ack(BasicAckOptions::default()).await {
                            warn!(error = %err, "llm ack failed");
                        }
                    }
                    Err(err) => {
                        error!(error = %err, "llm consumer stream error");
                    }
                }
            }
        }
    }

    Ok(())
}

fn effective_schedule_minutes(config: &RootConfig, provider: &str) -> Vec<u8> {
    config
        .llm
        .call_schedule_minutes_by_model
        .iter()
        .find_map(|(key, v)| key.eq_ignore_ascii_case(provider).then(|| v.clone()))
        .unwrap_or_else(|| config.llm.call_schedule_minutes.clone())
}

fn effective_min_invoke_interval_secs(config: &RootConfig, provider: &str) -> u64 {
    config
        .llm
        .min_invoke_interval_secs_by_model
        .iter()
        .find_map(|(key, v)| key.eq_ignore_ascii_case(provider).then_some(*v))
        .unwrap_or(config.llm.call_interval_secs.max(1))
}

fn bundle_matches_call_schedule(bundle: &MinuteBundleEnvelope, schedule_minutes: &[u8]) -> bool {
    let minute = bundle.ts_bucket.minute() as u8;
    schedule_minutes.contains(&minute)
}

fn build_invocation_input(
    bundle: &LatestBundle,
    management_mode: bool,
    pending_order_mode: bool,
    trading_state: Option<crate::execution::binance::TradingStateSnapshot>,
    last_management_reason: Option<String>,
    position_context: Option<PositionContextForLlm>,
) -> ModelInvocationInput {
    let management_snapshot = build_management_snapshot_for_llm(
        trading_state.as_ref(),
        last_management_reason,
        position_context,
    );
    ModelInvocationInput {
        symbol: bundle.raw.symbol.clone(),
        ts_bucket: bundle.raw.ts_bucket,
        window_code: bundle.raw.window_code.clone(),
        indicator_count: bundle.raw.indicator_count,
        source_routing_key: bundle.raw.routing_key.clone(),
        source_published_at: bundle.raw.published_at,
        received_at: bundle.received_at,
        indicators: bundle.indicators.clone(),
        missing_indicator_codes: bundle.missing_indicator_codes.clone(),
        management_mode,
        pending_order_mode,
        trading_state,
        management_snapshot,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct MissingKlineBarRequest {
    market: String,
    interval_code: String,
    open_time: DateTime<Utc>,
}

async fn hydrate_missing_kline_history_from_db(
    pool: &PgPool,
    input: &mut ModelInvocationInput,
) -> Result<usize> {
    let requests = collect_missing_kline_bar_requests(&input.indicators);
    if requests.is_empty() {
        return Ok(0);
    }

    let mut replacements = Vec::new();
    for request in &requests {
        if let Some(bar) = fetch_kline_bar_from_db(
            pool,
            &input.symbol,
            &request.market,
            &request.interval_code,
            request.open_time,
        )
        .await?
        {
            replacements.push((request.clone(), bar));
        }
    }

    Ok(apply_backfilled_kline_bars(
        &mut input.indicators,
        &replacements,
    ))
}

fn collect_missing_kline_bar_requests(indicators: &Value) -> Vec<MissingKlineBarRequest> {
    let mut requests = BTreeSet::new();

    let Some(intervals) = indicators
        .get("kline_history")
        .and_then(|indicator| indicator.get("payload"))
        .and_then(|payload| payload.get("intervals"))
        .and_then(Value::as_object)
    else {
        return Vec::new();
    };

    for (interval_code, _) in KLINE_DB_BACKFILL_INTERVALS {
        let Some(markets) = intervals
            .get(interval_code)
            .and_then(|interval| interval.get("markets"))
            .and_then(Value::as_object)
        else {
            continue;
        };

        for market in ["futures", "spot"] {
            let Some(bars) = markets
                .get(market)
                .and_then(|market_value| market_value.get("bars"))
                .and_then(Value::as_array)
            else {
                continue;
            };

            for bar in bars {
                let Some(open_time) = empty_kline_bar_open_time(bar) else {
                    continue;
                };
                requests.insert(MissingKlineBarRequest {
                    market: market.to_string(),
                    interval_code: interval_code.to_string(),
                    open_time,
                });
            }
        }
    }

    requests.into_iter().collect()
}

fn empty_kline_bar_open_time(bar: &Value) -> Option<DateTime<Utc>> {
    let object = bar.as_object()?;
    let all_prices_empty = ["open", "high", "low", "close"]
        .iter()
        .all(|field| object.get(*field).map(Value::is_null).unwrap_or(true));
    if !all_prices_empty {
        return None;
    }
    object
        .get("open_time")
        .and_then(Value::as_str)
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

async fn fetch_kline_bar_from_db(
    pool: &PgPool,
    symbol: &str,
    market: &str,
    interval_code: &str,
    open_time: DateTime<Utc>,
) -> Result<Option<Value>> {
    let row = sqlx::query(
        r#"
        SELECT
            open_time,
            close_time,
            open_price,
            high_price,
            low_price,
            close_price,
            COALESCE(volume_base, 0.0) AS volume_base,
            COALESCE(quote_volume, 0.0) AS volume_quote,
            is_closed
        FROM md.kline_bar
        WHERE market = $1::cfg.market_type
          AND symbol = $2
          AND interval_code = $3
          AND open_time = $4
        LIMIT 1
        "#,
    )
    .bind(market)
    .bind(symbol.to_uppercase())
    .bind(interval_code)
    .bind(open_time)
    .fetch_optional(pool)
    .await
    .context("query missing kline bar from db")?;

    let Some(row) = row else {
        return Ok(None);
    };

    let open_time: DateTime<Utc> = row.get("open_time");
    let close_time: DateTime<Utc> = row.get("close_time");
    let expected_minutes = interval_minutes(interval_code);
    Ok(Some(json!({
        "open_time": open_time.to_rfc3339(),
        "close_time": close_time.to_rfc3339(),
        "open": row.get::<f64, _>("open_price"),
        "high": row.get::<f64, _>("high_price"),
        "low": row.get::<f64, _>("low_price"),
        "close": row.get::<f64, _>("close_price"),
        "volume_base": row.get::<f64, _>("volume_base"),
        "volume_quote": row.get::<f64, _>("volume_quote"),
        "is_closed": row.get::<bool, _>("is_closed"),
        "minutes_covered": expected_minutes,
        "expected_minutes": expected_minutes,
    })))
}

fn apply_backfilled_kline_bars(
    indicators: &mut Value,
    replacements: &[(MissingKlineBarRequest, Value)],
) -> usize {
    if replacements.is_empty() {
        return 0;
    }

    let Some(intervals) = indicators
        .get_mut("kline_history")
        .and_then(Value::as_object_mut)
        .and_then(|indicator| indicator.get_mut("payload"))
        .and_then(Value::as_object_mut)
        .and_then(|payload| payload.get_mut("intervals"))
        .and_then(Value::as_object_mut)
    else {
        return 0;
    };

    let mut patched = 0usize;
    for (request, replacement) in replacements {
        let Some(bars) = intervals
            .get_mut(&request.interval_code)
            .and_then(Value::as_object_mut)
            .and_then(|interval| interval.get_mut("markets"))
            .and_then(Value::as_object_mut)
            .and_then(|markets| markets.get_mut(&request.market))
            .and_then(Value::as_object_mut)
            .and_then(|market| market.get_mut("bars"))
            .and_then(Value::as_array_mut)
        else {
            continue;
        };

        for bar in bars.iter_mut() {
            let Some(open_time) = empty_kline_bar_open_time(bar) else {
                continue;
            };
            if open_time == request.open_time {
                *bar = replacement.clone();
                patched += 1;
                break;
            }
        }
    }

    patched
}

fn interval_minutes(interval_code: &str) -> i64 {
    match interval_code {
        "4h" => 240,
        "1d" => 1440,
        _ => 1,
    }
}

async fn patch_input_kline_history_from_db(
    pool: &PgPool,
    input: &mut ModelInvocationInput,
    trigger: &str,
) -> Result<()> {
    let patched = hydrate_missing_kline_history_from_db(pool, input).await?;
    if patched > 0 {
        info!(
            symbol = %input.symbol,
            ts_bucket = %input.ts_bucket,
            trigger = trigger,
            patched_bars = patched,
            "patched missing kline_history bars from db before llm invocation"
        );
    }
    Ok(())
}

#[cfg(test)]
fn build_persist_only_input(bundle: &LatestBundle) -> ModelInvocationInput {
    ModelInvocationInput {
        symbol: bundle.raw.symbol.clone(),
        ts_bucket: bundle.raw.ts_bucket,
        window_code: bundle.raw.window_code.clone(),
        indicator_count: bundle.raw.indicator_count,
        source_routing_key: bundle.raw.routing_key.clone(),
        source_published_at: bundle.raw.published_at,
        received_at: bundle.received_at,
        indicators: bundle.indicators.clone(),
        missing_indicator_codes: bundle.missing_indicator_codes.clone(),
        management_mode: false,
        pending_order_mode: false,
        trading_state: None,
        management_snapshot: None,
    }
}

#[derive(Debug, Clone)]
struct InvocationRoutingContext {
    trading_state: Option<crate::execution::binance::TradingStateSnapshot>,
    management_mode: bool,
    pending_order_mode: bool,
    active_position_count: usize,
    open_order_count: usize,
    context_state: &'static str,
    invoke_management_reason: Option<String>,
    position_context: Option<PositionContextForLlm>,
}

impl InvocationRoutingContext {
    fn persist_fallback() -> Self {
        Self {
            trading_state: None,
            management_mode: false,
            pending_order_mode: false,
            active_position_count: 0,
            open_order_count: 0,
            context_state: "NO_ACTIVE_CONTEXT",
            invoke_management_reason: None,
            position_context: None,
        }
    }
}

fn context_state_from_trading_state(
    trading_state: Option<&crate::execution::binance::TradingStateSnapshot>,
) -> &'static str {
    trading_state
        .map(
            |state| match (state.has_active_positions, state.has_open_orders) {
                (true, true) => "POSITION_AND_ORDERS",
                (true, false) => "POSITION_ACTIVE",
                (false, true) => "OPEN_ORDERS_ONLY",
                (false, false) => "NO_ACTIVE_CONTEXT",
            },
        )
        .unwrap_or("NO_ACTIVE_CONTEXT")
}

async fn resolve_invocation_routing_context(
    config: &RootConfig,
    http_client: &Client,
    bundle: &LatestBundle,
    runtime_lifecycle_state: &Arc<Mutex<RuntimeLifecycleStore>>,
) -> Result<InvocationRoutingContext> {
    let trading_state = if config.llm.execution.enabled {
        Some(
            fetch_symbol_trading_state(
                http_client,
                &config.api.binance,
                &config.llm.execution,
                &bundle.raw.symbol,
            )
            .await?,
        )
    } else {
        None
    };

    let management_mode = trading_state
        .as_ref()
        .map(|state| state.has_active_context)
        .unwrap_or(false);
    let active_position_count = trading_state
        .as_ref()
        .map(|state| state.active_positions.len())
        .unwrap_or(0);
    let open_order_count = trading_state
        .as_ref()
        .map(|state| state.open_orders.len())
        .unwrap_or(0);
    let pending_order_mode = active_position_count == 0 && open_order_count > 0;
    let context_state = context_state_from_trading_state(trading_state.as_ref());
    let global_management_reason = {
        let guard = runtime_lifecycle_state.lock().await;
        if active_position_count == 0 && open_order_count == 0 {
            None
        } else {
            guard.last_management_reason(&bundle.raw.symbol)
        }
    };

    hydrate_position_context_from_live_state(
        http_client,
        &config.api.binance,
        &config.llm.execution,
        trading_state.as_ref(),
        runtime_lifecycle_state,
    )
    .await;
    if let Err(err) = restore_position_context_from_journal_for_live_state(
        trading_state.as_ref(),
        runtime_lifecycle_state,
    )
    .await
    {
        warn!(
            symbol = %bundle.raw.symbol,
            error = %err,
            "restore position context from journal failed"
        );
    }
    let position_context =
        sync_and_build_position_context_snapshot(trading_state.as_ref(), runtime_lifecycle_state)
            .await;
    let invoke_management_reason = resolve_invoke_management_reason(
        active_position_count,
        open_order_count,
        global_management_reason,
        position_context.as_ref(),
    );

    Ok(InvocationRoutingContext {
        trading_state,
        management_mode,
        pending_order_mode,
        active_position_count,
        open_order_count,
        context_state,
        invoke_management_reason,
        position_context,
    })
}

/// Mirror of `extract_existing_exit_trigger` in binance.rs (private there).
/// Returns the actual trigger price for the first matching TP or SL order on Binance.
fn find_exit_trigger_price(
    open_orders: &[crate::execution::binance::OpenOrderSnapshot],
    position_side: &str,
    order_type: &str,
) -> Option<f64> {
    open_orders
        .iter()
        .filter(|o| o.order_type.eq_ignore_ascii_case(order_type))
        .filter(|o| {
            o.position_side.eq_ignore_ascii_case(position_side)
                || o.position_side.eq_ignore_ascii_case("BOTH")
                || o.position_side.trim().is_empty()
                || o.position_side == "-"
        })
        .find_map(|o| {
            if o.stop_price > 0.0 {
                Some(o.stop_price)
            } else if o.price > 0.0 {
                Some(o.price)
            } else {
                None
            }
        })
}

fn primary_pending_entry_order(
    state: &crate::execution::binance::TradingStateSnapshot,
) -> Option<&crate::execution::binance::OpenOrderSnapshot> {
    state
        .open_orders
        .iter()
        .filter(|o| {
            !o.close_position
                && !o.reduce_only
                && (o.side.eq_ignore_ascii_case("BUY") || o.side.eq_ignore_ascii_case("SELL"))
        })
        .max_by(|a, b| {
            let a_qty = (a.orig_qty - a.executed_qty).max(0.0);
            let b_qty = (b.orig_qty - b.executed_qty).max(0.0);
            a_qty.total_cmp(&b_qty)
        })
}

fn build_pending_order_summary_for_llm(
    state: &crate::execution::binance::TradingStateSnapshot,
    position_context: Option<&PositionContextForLlm>,
) -> Option<PendingOrderSummaryForLlm> {
    let order = primary_pending_entry_order(state)?;
    let direction = if order.side.eq_ignore_ascii_case("BUY") {
        "LONG".to_string()
    } else {
        "SHORT".to_string()
    };
    let position_side = {
        let raw = order.position_side.trim();
        if raw.is_empty() || raw == "-" || raw.eq_ignore_ascii_case("BOTH") {
            direction.clone()
        } else {
            raw.to_ascii_uppercase()
        }
    };
    let quantity = (order.orig_qty - order.executed_qty).max(0.0);
    Some(PendingOrderSummaryForLlm {
        position_side: position_side.clone(),
        direction,
        quantity,
        leverage: position_context.and_then(|ctx| ctx.effective_leverage),
        entry_price: if order.price > 0.0 {
            Some(order.price)
        } else {
            position_context.and_then(|ctx| ctx.effective_entry_price)
        },
        current_tp_price: find_exit_trigger_price(
            &state.open_orders,
            &position_side,
            "TAKE_PROFIT_MARKET",
        )
        .or_else(|| position_context.and_then(|ctx| ctx.effective_take_profit))
        .or_else(|| {
            position_context
                .and_then(|ctx| ctx.entry_context.as_ref())
                .and_then(|ctx| ctx.original_tp)
        }),
        current_sl_price: find_exit_trigger_price(
            &state.open_orders,
            &position_side,
            "STOP_MARKET",
        )
        .or_else(|| position_context.and_then(|ctx| ctx.effective_stop_loss))
        .or_else(|| {
            position_context
                .and_then(|ctx| ctx.entry_context.as_ref())
                .and_then(|ctx| ctx.original_sl)
        }),
    })
}

#[derive(Debug, Clone, Copy, Default)]
struct TelegramPositionFields {
    entry_price: Option<f64>,
    leverage: Option<f64>,
    risk_reward_ratio: Option<f64>,
    take_profit: Option<f64>,
    stop_loss: Option<f64>,
}

fn derive_management_telegram_fields(
    trading_state: Option<&crate::execution::binance::TradingStateSnapshot>,
) -> TelegramPositionFields {
    let Some(state) = trading_state else {
        return TelegramPositionFields::default();
    };
    let Some(pos) = state
        .active_positions
        .iter()
        .max_by(|a, b| a.position_amt.abs().total_cmp(&b.position_amt.abs()))
    else {
        return TelegramPositionFields::default();
    };

    let take_profit =
        find_exit_trigger_price(&state.open_orders, &pos.position_side, "TAKE_PROFIT_MARKET");
    let stop_loss = find_exit_trigger_price(&state.open_orders, &pos.position_side, "STOP_MARKET");
    let risk_reward_ratio = take_profit
        .zip(stop_loss)
        .and_then(|(tp, sl)| compute_rr_from_levels(pos.entry_price, tp, sl));

    TelegramPositionFields {
        entry_price: Some(pos.entry_price),
        leverage: Some(pos.leverage as f64),
        risk_reward_ratio,
        take_profit,
        stop_loss,
    }
}

async fn derive_pending_order_telegram_fields(
    trading_state: Option<&crate::execution::binance::TradingStateSnapshot>,
    runtime_lifecycle_state: &Arc<Mutex<RuntimeLifecycleStore>>,
) -> TelegramPositionFields {
    let Some(state) = trading_state else {
        return TelegramPositionFields::default();
    };
    let Some(order) = primary_pending_entry_order(state) else {
        return TelegramPositionFields::default();
    };
    let position_side = {
        let raw = order.position_side.trim();
        if raw.is_empty() || raw == "-" || raw.eq_ignore_ascii_case("BOTH") {
            if order.side.eq_ignore_ascii_case("BUY") {
                "LONG".to_string()
            } else {
                "SHORT".to_string()
            }
        } else {
            raw.to_ascii_uppercase()
        }
    };
    let key = format!(
        "{}:{}",
        state.symbol.to_ascii_uppercase(),
        position_side.to_ascii_uppercase()
    );
    let guard = runtime_lifecycle_state.lock().await;
    let ctx = guard
        .symbol_state(&state.symbol)
        .and_then(|symbol_state| symbol_state.contexts.get(&key));
    let entry_price = if order.price > 0.0 {
        Some(order.price)
    } else {
        ctx.and_then(|v| v.effective_entry_price)
    };
    let take_profit =
        find_exit_trigger_price(&state.open_orders, &position_side, "TAKE_PROFIT_MARKET")
            .or_else(|| ctx.and_then(|v| v.effective_take_profit));
    let stop_loss = find_exit_trigger_price(&state.open_orders, &position_side, "STOP_MARKET")
        .or_else(|| ctx.and_then(|v| v.effective_stop_loss));
    let risk_reward_ratio = entry_price
        .zip(take_profit)
        .zip(stop_loss)
        .and_then(|((entry, tp), sl)| compute_rr_from_levels(entry, tp, sl));

    TelegramPositionFields {
        entry_price,
        leverage: ctx.and_then(|v| v.effective_leverage.map(|x| x as f64)),
        risk_reward_ratio,
        take_profit,
        stop_loss,
    }
}

fn compute_rr_from_levels(entry: f64, tp: f64, sl: f64) -> Option<f64> {
    let risk = (entry - sl).abs();
    let reward = (tp - entry).abs();
    if risk <= f64::EPSILON || reward <= f64::EPSILON {
        None
    } else {
        Some(reward / risk)
    }
}

fn with_telegram_field_overrides(
    mut fields: TelegramPositionFields,
    entry_price: Option<f64>,
    leverage: Option<f64>,
    take_profit: Option<f64>,
    stop_loss: Option<f64>,
) -> TelegramPositionFields {
    fields.entry_price = entry_price.or(fields.entry_price);
    fields.leverage = leverage.or(fields.leverage);
    if take_profit.is_some() {
        fields.take_profit = take_profit;
    }
    if stop_loss.is_some() {
        fields.stop_loss = stop_loss;
    }
    fields.risk_reward_ratio = fields
        .entry_price
        .zip(fields.take_profit)
        .zip(fields.stop_loss)
        .and_then(|((entry, tp), sl)| compute_rr_from_levels(entry, tp, sl));
    fields
}

fn build_management_snapshot_for_llm(
    trading_state: Option<&crate::execution::binance::TradingStateSnapshot>,
    last_management_reason: Option<String>,
    position_context: Option<PositionContextForLlm>,
) -> Option<ManagementSnapshotForLlm> {
    let state = trading_state?;
    let pending_order = if !state.has_active_positions && state.has_open_orders {
        build_pending_order_summary_for_llm(state, position_context.as_ref())
    } else {
        None
    };
    let context_state = match (state.has_active_positions, state.has_open_orders) {
        (true, true) => "POSITION_AND_ORDERS",
        (true, false) => "POSITION_ACTIVE",
        (false, true) => "OPEN_ORDERS_ONLY",
        (false, false) => "NO_ACTIVE_CONTEXT",
    }
    .to_string();
    let positions = state
        .active_positions
        .iter()
        .map(|p| {
            let current_tp_price =
                find_exit_trigger_price(&state.open_orders, &p.position_side, "TAKE_PROFIT_MARKET");
            let current_sl_price =
                find_exit_trigger_price(&state.open_orders, &p.position_side, "STOP_MARKET");
            PositionSummaryForLlm {
                position_side: p.position_side.clone(),
                direction: if p.position_amt > 0.0 {
                    "LONG".to_string()
                } else if p.position_amt < 0.0 {
                    "SHORT".to_string()
                } else {
                    "FLAT".to_string()
                },
                quantity: p.position_amt.abs(),
                leverage: p.leverage,
                entry_price: p.entry_price,
                mark_price: p.mark_price,
                unrealized_pnl: p.unrealized_pnl,
                pnl_by_latest_price: compute_pnl_by_latest_price(p),
                current_tp_price,
                current_sl_price,
            }
        })
        .collect::<Vec<_>>();
    Some(ManagementSnapshotForLlm {
        context_state,
        has_active_positions: state.has_active_positions,
        has_open_orders: state.has_open_orders,
        active_position_count: state.active_positions.len(),
        open_order_count: state.open_orders.len(),
        positions,
        pending_order,
        last_management_reason,
        position_context,
    })
}

fn compute_pnl_by_latest_price(
    position: &crate::execution::binance::ActivePositionSnapshot,
) -> f64 {
    if position.entry_price > 0.0 && position.mark_price > 0.0 && position.position_amt != 0.0 {
        (position.mark_price - position.entry_price) * position.position_amt
    } else {
        position.unrealized_pnl
    }
}

fn primary_position_key(
    state: &crate::execution::binance::TradingStateSnapshot,
) -> Option<(String, f64, f64)> {
    let pos = state
        .active_positions
        .iter()
        .max_by(|a, b| a.position_amt.abs().total_cmp(&b.position_amt.abs()))?;
    let key = format!(
        "{}:{}",
        state.symbol.to_ascii_uppercase(),
        pos.position_side.to_ascii_uppercase()
    );
    Some((key, pos.position_amt.abs(), pos.mark_price))
}

fn primary_pending_order_key(
    state: &crate::execution::binance::TradingStateSnapshot,
) -> Option<(String, f64, f64)> {
    let order = primary_pending_entry_order(state)?;
    let position_side = {
        let raw = order.position_side.trim();
        if raw.is_empty() || raw == "-" || raw.eq_ignore_ascii_case("BOTH") {
            if order.side.eq_ignore_ascii_case("BUY") {
                "LONG".to_string()
            } else {
                "SHORT".to_string()
            }
        } else {
            raw.to_ascii_uppercase()
        }
    };
    let key = format!(
        "{}:{}",
        state.symbol.to_ascii_uppercase(),
        position_side.to_ascii_uppercase()
    );
    let remaining_qty = (order.orig_qty - order.executed_qty).max(0.0);
    let entry_price = if order.price > 0.0 { order.price } else { 0.0 };
    Some((key, remaining_qty, entry_price))
}

fn directional_context_key(symbol: &str, direction: &str) -> Option<String> {
    let direction = direction.trim();
    if direction.eq_ignore_ascii_case("LONG") || direction.eq_ignore_ascii_case("SHORT") {
        Some(format!(
            "{}:{}",
            symbol.to_ascii_uppercase(),
            direction.to_ascii_uppercase()
        ))
    } else {
        None
    }
}

fn directional_context_key_from_trade_decision(
    symbol: &str,
    decision: TradeDecision,
) -> Option<String> {
    match decision {
        TradeDecision::Long => directional_context_key(symbol, "LONG"),
        TradeDecision::Short => directional_context_key(symbol, "SHORT"),
        TradeDecision::NoTrade => None,
    }
}

fn both_context_alias_for_directional_key(key: &str) -> Option<String> {
    let (symbol, suffix) = key.rsplit_once(':')?;
    if suffix.eq_ignore_ascii_case("LONG") || suffix.eq_ignore_ascii_case("SHORT") {
        Some(format!("{}:BOTH", symbol.to_ascii_uppercase()))
    } else {
        None
    }
}

fn execution_context_keys_for_report(
    symbol: &str,
    position_side: &str,
    decision: TradeDecision,
) -> Vec<String> {
    let mut keys = Vec::new();
    let raw = position_side.trim();
    if !raw.is_empty() && raw != "-" {
        keys.push(format!(
            "{}:{}",
            symbol.to_ascii_uppercase(),
            raw.to_ascii_uppercase()
        ));
    }
    if raw.is_empty() || raw == "-" || raw.eq_ignore_ascii_case("BOTH") {
        if let Some(key) = directional_context_key_from_trade_decision(symbol, decision) {
            if !keys.contains(&key) {
                keys.push(key);
            }
        }
    }
    keys
}

fn execution_context_keys_for_journal_restore(
    symbol: &str,
    position_side: Option<&str>,
    decision: Option<&str>,
    active_key: Option<&String>,
    pending_key: Option<&String>,
) -> Vec<String> {
    let mut keys = Vec::new();
    if let Some(key) = active_key {
        if !keys.contains(key) {
            keys.push(key.clone());
        }
    }
    if let Some(key) = pending_key {
        if !keys.contains(key) {
            keys.push(key.clone());
        }
    }
    let raw = position_side.unwrap_or_default().trim();
    if !raw.is_empty() && raw != "-" {
        let key = format!(
            "{}:{}",
            symbol.to_ascii_uppercase(),
            raw.to_ascii_uppercase()
        );
        if !keys.contains(&key) {
            keys.push(key);
        }
    }
    if raw.is_empty() || raw == "-" || raw.eq_ignore_ascii_case("BOTH") {
        if let Some(direction) = decision {
            if let Some(key) = directional_context_key(symbol, direction) {
                if !keys.contains(&key) {
                    keys.push(key);
                }
            }
        }
    }
    keys
}

fn remap_trade_entry_and_stop_loss(
    _provider: &str,
    execution_config: &crate::app::config::LlmExecutionConfig,
    intent: &crate::llm::decision::TradeIntent,
) -> Option<(f64, f64)> {
    if !execution_config.entry_sl_remap.enabled {
        return None;
    }
    if !matches!(intent.decision, TradeDecision::Long | TradeDecision::Short) {
        return None;
    }

    let entry = intent.entry_price?;
    let take_profit = intent.take_profit?;
    let stop_loss = intent.stop_loss?;
    let rr = intent.risk_reward_ratio?;
    if rr <= f64::EPSILON {
        return None;
    }

    let pct = (execution_config.entry_sl_remap.entry_to_sl_distance_pct / 100.0).clamp(0.0, 1.0);
    // Move entry toward the model stop by the configured percentage:
    // remapped_entry = entry - (entry - stop_loss) * pct
    let remapped_entry = entry - (entry - stop_loss) * pct;
    let reward_distance = (take_profit - remapped_entry).abs();
    if reward_distance <= f64::EPSILON {
        return None;
    }
    let risk_distance = reward_distance / rr;
    let remapped_stop_loss = match intent.decision {
        TradeDecision::Long => remapped_entry - risk_distance,
        TradeDecision::Short => remapped_entry + risk_distance,
        TradeDecision::NoTrade => return None,
    };

    Some((remapped_entry, remapped_stop_loss))
}

fn resolve_entry_v_from_helper(
    trace: Option<&[Value]>,
    input: &ModelInvocationInput,
    horizon: Option<&str>,
) -> Option<ResolvedEntryV> {
    let entry_style = entry_stage_trace_entry_style(trace);
    let preferred = preferred_v_timeframe(entry_style.as_deref(), horizon);
    let fallback = if preferred == "1d" { "4h" } else { "1d" };

    PreComputedVHelper::compute_for_timeframe(&input.indicators, preferred)
        .map(|(value, basis)| ResolvedEntryV {
            value,
            timeframe: preferred,
            basis,
        })
        .or_else(|| {
            PreComputedVHelper::compute_for_timeframe(&input.indicators, fallback).map(
                |(value, basis)| ResolvedEntryV {
                    value,
                    timeframe: fallback,
                    basis,
                },
            )
        })
}

fn evaluate_trade_entry_v_gate(
    execution_config: &crate::app::config::LlmExecutionConfig,
    intent: &TradeIntent,
    input: &ModelInvocationInput,
    trace: Option<&[Value]>,
) -> Result<Option<EntryVGateResult>> {
    if !matches!(intent.decision, TradeDecision::Long | TradeDecision::Short) {
        return Ok(None);
    }

    let entry = intent
        .entry_price
        .ok_or_else(|| anyhow::anyhow!("entry V gate requires entry price"))?;
    let take_profit = intent
        .take_profit
        .ok_or_else(|| anyhow::anyhow!("entry V gate requires take profit"))?;
    let resolved_v = resolve_entry_v_from_helper(trace, input, intent.horizon.as_deref())
        .ok_or_else(|| {
            anyhow::anyhow!("entry V gate could not compute helper V from kline_history")
        })?;
    if resolved_v.value <= f64::EPSILON {
        return Err(anyhow::anyhow!(
            "entry V gate resolved non-positive helper V={}",
            resolved_v.value
        ));
    }

    let take_profit_distance = (take_profit - entry).abs();
    let take_profit_distance_v = take_profit_distance / resolved_v.value;
    let min_distance_v = execution_config.min_distance_v;
    let passed = take_profit_distance_v + f64::EPSILON >= min_distance_v;

    Ok(Some(EntryVGateResult {
        resolved_v,
        take_profit_distance,
        take_profit_distance_v,
        min_distance_v,
        passed,
    }))
}

fn evaluate_trade_rr_gate(
    execution_config: &crate::app::config::LlmExecutionConfig,
    intent: &TradeIntent,
) -> Result<Option<EntryRrGateResult>> {
    if !matches!(intent.decision, TradeDecision::Long | TradeDecision::Short) {
        return Ok(None);
    }

    let entry = intent
        .entry_price
        .ok_or_else(|| anyhow::anyhow!("entry RR gate requires entry price"))?;
    let take_profit = intent
        .take_profit
        .ok_or_else(|| anyhow::anyhow!("entry RR gate requires take profit"))?;
    let stop_loss = intent
        .stop_loss
        .ok_or_else(|| anyhow::anyhow!("entry RR gate requires stop loss"))?;
    let reward_distance = (take_profit - entry).abs();
    let risk_distance = (entry - stop_loss).abs();
    let risk_reward_ratio =
        compute_rr_from_levels(entry, take_profit, stop_loss).ok_or_else(|| {
            anyhow::anyhow!("entry RR gate could not compute risk_reward_ratio from entry/tp/sl")
        })?;
    let min_rr = execution_config.min_rr;
    let passed = risk_reward_ratio + f64::EPSILON >= min_rr;

    Ok(Some(EntryRrGateResult {
        risk_reward_ratio,
        reward_distance,
        risk_distance,
        min_rr,
        passed,
    }))
}

async fn preview_trade_signal_fields(
    http_client: &Client,
    config: &RootConfig,
    symbol: &str,
    intent: &TradeIntent,
) -> TradeSignalFields {
    let mut preview_exec_config = config.llm.execution.clone();
    preview_exec_config.dry_run = true;
    match execute_trade_intent(
        http_client,
        &config.api.binance,
        &preview_exec_config,
        symbol,
        intent,
    )
    .await
    {
        Ok(report) => TradeSignalFields::from_report(&report),
        Err(err) => {
            warn!(
                symbol = %symbol,
                decision = intent.decision.as_str(),
                error = %err,
                "trade signal preview failed, falling back to intent values"
            );
            TradeSignalFields::from_intent(intent)
        }
    }
}

async fn enrich_telegram_fields_from_entry_context(
    mut fields: TelegramPositionFields,
    trading_state: Option<&crate::execution::binance::TradingStateSnapshot>,
    runtime_lifecycle_state: &Arc<Mutex<RuntimeLifecycleStore>>,
) -> TelegramPositionFields {
    let Some(state) = trading_state else {
        return fields;
    };
    let Some((key, _, _)) = primary_position_key(state) else {
        return fields;
    };
    let guard = runtime_lifecycle_state.lock().await;
    let Some(ctx) = guard
        .symbol_state(&state.symbol)
        .and_then(|symbol_state| symbol_state.contexts.get(&key))
    else {
        return fields;
    };
    let Some(entry_ctx) = ctx.entry_context.as_ref() else {
        return fields;
    };

    fields.entry_price = fields.entry_price.or(ctx.effective_entry_price);
    fields.take_profit = fields.take_profit.or(entry_ctx.original_tp);
    fields.stop_loss = fields
        .stop_loss
        .or(ctx.effective_stop_loss)
        .or(entry_ctx.original_sl);
    if fields.risk_reward_ratio.is_none() {
        if let (Some(entry), Some(tp), Some(sl)) =
            (fields.entry_price, fields.take_profit, fields.stop_loss)
        {
            fields.risk_reward_ratio = compute_rr_from_levels(entry, tp, sl);
        }
    }
    fields
}

async fn sync_and_build_position_context_snapshot(
    trading_state: Option<&crate::execution::binance::TradingStateSnapshot>,
    runtime_lifecycle_state: &Arc<Mutex<RuntimeLifecycleStore>>,
) -> Option<PositionContextForLlm> {
    let state = trading_state?;
    let (key, current_qty, current_price) =
        primary_position_key(state).or_else(|| primary_pending_order_key(state))?;
    if current_qty <= f64::EPSILON {
        return None;
    }
    let mut guard = runtime_lifecycle_state.lock().await;
    let symbol_state = guard.symbol_state_mut(&state.symbol);
    let fallback_both_context = both_context_alias_for_directional_key(&key)
        .and_then(|alias_key| symbol_state.contexts.get(&alias_key).cloned());
    let entry = symbol_state.contexts.entry(key).or_default();
    if let Some(fallback) = fallback_both_context.as_ref() {
        if entry.effective_entry_price.is_none() {
            entry.effective_entry_price = fallback.effective_entry_price;
        }
        if entry.effective_take_profit.is_none() {
            entry.effective_take_profit = fallback.effective_take_profit;
        }
        if entry.effective_stop_loss.is_none() {
            entry.effective_stop_loss = fallback.effective_stop_loss;
        }
        if entry.entry_context.is_none() {
            entry.entry_context = fallback.entry_context.clone();
        }
        if entry.effective_leverage.is_none() {
            entry.effective_leverage = fallback.effective_leverage;
        }
    }
    if entry.original_qty <= f64::EPSILON {
        entry.original_qty = current_qty;
    } else if current_qty > entry.original_qty {
        entry.original_qty = current_qty;
    }
    if entry.last_management_reason.is_none() {
        entry.last_management_reason = symbol_state.last_management_reason.clone();
    }
    let current_pct = if entry.original_qty > f64::EPSILON {
        current_qty / entry.original_qty * 100.0
    } else {
        100.0
    };
    let times_reduced_at_current_level = entry
        .reduction_history
        .iter()
        .filter(|h| (h.price - current_price).abs() <= MANAGEMENT_REDUCTION_LEVEL_THRESHOLD)
        .count();

    Some(PositionContextForLlm {
        original_qty: entry.original_qty,
        current_qty,
        current_pct_of_original: current_pct,
        effective_leverage: entry.effective_leverage,
        effective_entry_price: entry.effective_entry_price,
        effective_take_profit: entry.effective_take_profit,
        effective_stop_loss: entry.effective_stop_loss,
        reduction_history: entry
            .reduction_history
            .iter()
            .map(|h| ReductionHistoryItemForLlm {
                time: h.time.clone(),
                qty_ratio: h.qty_ratio,
                reason_summary: h.reason_summary.clone(),
            })
            .collect(),
        times_reduced_at_current_level,
        last_management_action: entry.last_management_action.clone(),
        last_management_reason: entry.last_management_reason.clone(),
        entry_context: entry.entry_context.as_ref().map(|ec| EntryContextForLlm {
            entry_strategy: ec.entry_strategy.clone(),
            stop_model: ec.stop_model.clone(),
            entry_mode: ec.entry_mode.clone(),
            original_tp: ec.original_tp,
            original_sl: ec.original_sl,
            sweep_wick_extreme: ec.sweep_wick_extreme,
            horizon: ec.horizon.clone(),
            entry_reason: ec.entry_reason.clone(),
        }),
    })
}

async fn hydrate_position_context_from_live_state(
    http_client: &Client,
    api_config: &crate::app::config::BinanceApiConfig,
    exec_config: &crate::app::config::LlmExecutionConfig,
    trading_state: Option<&crate::execution::binance::TradingStateSnapshot>,
    runtime_lifecycle_state: &Arc<Mutex<RuntimeLifecycleStore>>,
) {
    let Some(state) = trading_state else {
        return;
    };

    let pending_leverage = if !state.has_active_positions && state.has_open_orders {
        fetch_pending_order_leverage(http_client, api_config, exec_config, &state.symbol)
            .await
            .ok()
    } else {
        None
    };

    let mut guard = runtime_lifecycle_state.lock().await;
    let symbol_state = guard.symbol_state_mut(&state.symbol);

    if let Some(pos) = state
        .active_positions
        .iter()
        .max_by(|a, b| a.position_amt.abs().total_cmp(&b.position_amt.abs()))
    {
        let key = format!(
            "{}:{}",
            state.symbol.to_ascii_uppercase(),
            pos.position_side.to_ascii_uppercase()
        );
        let entry = symbol_state.contexts.entry(key).or_default();
        let qty = pos.position_amt.abs();
        if entry.original_qty <= f64::EPSILON || qty > entry.original_qty {
            entry.original_qty = qty;
        }
        entry.effective_entry_price = Some(pos.entry_price);
        entry.effective_leverage = Some(pos.leverage);
        if let Some(tp) =
            find_exit_trigger_price(&state.open_orders, &pos.position_side, "TAKE_PROFIT_MARKET")
        {
            entry.effective_take_profit = Some(tp);
        }
        if let Some(sl) =
            find_exit_trigger_price(&state.open_orders, &pos.position_side, "STOP_MARKET")
        {
            entry.effective_stop_loss = Some(sl);
        }
    }

    if !state.has_active_positions {
        if let Some(order) = primary_pending_entry_order(state) {
            let position_side = {
                let raw = order.position_side.trim();
                if raw.is_empty() || raw == "-" || raw.eq_ignore_ascii_case("BOTH") {
                    if order.side.eq_ignore_ascii_case("BUY") {
                        "LONG".to_string()
                    } else {
                        "SHORT".to_string()
                    }
                } else {
                    raw.to_ascii_uppercase()
                }
            };
            let key = format!(
                "{}:{}",
                state.symbol.to_ascii_uppercase(),
                position_side.to_ascii_uppercase()
            );
            let entry = symbol_state.contexts.entry(key).or_default();
            let remaining_qty = (order.orig_qty - order.executed_qty).max(0.0);
            if entry.original_qty <= f64::EPSILON || remaining_qty > entry.original_qty {
                entry.original_qty = remaining_qty;
            }
            if order.price > 0.0 {
                entry.effective_entry_price = Some(order.price);
            }
            if let Some(leverage) = pending_leverage {
                entry.effective_leverage = Some(leverage);
            }
            if let Some(tp) =
                find_exit_trigger_price(&state.open_orders, &position_side, "TAKE_PROFIT_MARKET")
            {
                entry.effective_take_profit = Some(tp);
            }
            if let Some(sl) =
                find_exit_trigger_price(&state.open_orders, &position_side, "STOP_MARKET")
            {
                entry.effective_stop_loss = Some(sl);
            }
        }
    }
}

async fn restore_position_context_from_journal_for_live_state(
    trading_state: Option<&crate::execution::binance::TradingStateSnapshot>,
    runtime_lifecycle_state: &Arc<Mutex<RuntimeLifecycleStore>>,
) -> Result<()> {
    let Some(state) = trading_state else {
        return Ok(());
    };
    let path = Path::new(LLM_JOURNAL_FILE);
    if !path.exists() {
        return Ok(());
    }

    let active_key = primary_position_key(state).map(|(key, _, _)| key);
    let pending_key = primary_pending_order_key(state).map(|(key, _, _)| key);
    if active_key.is_none() && pending_key.is_none() {
        return Ok(());
    }

    let symbol = state.symbol.to_ascii_uppercase();
    let contents = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let mut guard = runtime_lifecycle_state.lock().await;
    let symbol_state = guard.symbol_state_mut(&symbol);
    for line in contents.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let event_symbol = value
            .get("symbol")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_uppercase();
        if event_symbol != symbol {
            continue;
        }
        let event_type = value
            .get("event_type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let reason = value
            .get("reason")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        if reason.is_some() {
            symbol_state.last_management_reason = reason
                .clone()
                .or(symbol_state.last_management_reason.clone());
        }
        match event_type {
            "llm_order_execution" => {
                let keys = execution_context_keys_for_journal_restore(
                    &symbol,
                    value.get("position_side").and_then(Value::as_str),
                    value.get("decision").and_then(Value::as_str),
                    active_key.as_ref(),
                    pending_key.as_ref(),
                );
                if keys.is_empty() {
                    continue;
                }
                let has_entry_context = value.get("entry_strategy").is_some()
                    || value.get("stop_model").is_some()
                    || value.get("horizon").is_some();
                let restored_entry_context = if has_entry_context {
                    Some(EntryContextForState {
                        entry_strategy: value
                            .get("entry_strategy")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        stop_model: value
                            .get("stop_model")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        entry_mode: value
                            .get("entry_mode")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        original_tp: value.get("entry_original_tp").and_then(Value::as_f64),
                        original_sl: value.get("entry_original_sl").and_then(Value::as_f64),
                        sweep_wick_extreme: value.get("sweep_wick_extreme").and_then(Value::as_f64),
                        horizon: value
                            .get("horizon")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        entry_reason: reason.clone().unwrap_or_default(),
                        entry_v: value.get("entry_v").and_then(Value::as_f64),
                    })
                } else {
                    None
                };
                for key in keys {
                    let entry = symbol_state.contexts.entry(key).or_default();
                    entry.last_management_reason =
                        reason.clone().or(entry.last_management_reason.clone());
                    entry.effective_entry_price = value
                        .get("maker_entry_price")
                        .and_then(Value::as_f64)
                        .or(entry.effective_entry_price);
                    entry.effective_take_profit = value
                        .get("effective_take_profit")
                        .and_then(Value::as_f64)
                        .or(entry.effective_take_profit);
                    entry.effective_stop_loss = value
                        .get("effective_stop_loss")
                        .and_then(Value::as_f64)
                        .or(entry.effective_stop_loss);
                    entry.effective_leverage = value
                        .get("leverage")
                        .and_then(Value::as_u64)
                        .map(|v| v as u32)
                        .or(entry.effective_leverage);
                    if entry.entry_context.is_none() {
                        entry.entry_context = restored_entry_context.clone();
                    }
                }
            }
            "llm_management_execution" | "llm_management_execution_error" => {
                let Some(key) = active_key.as_ref() else {
                    continue;
                };
                let entry = symbol_state.contexts.entry(key.clone()).or_default();
                entry.last_management_action = value
                    .get("action")
                    .and_then(Value::as_str)
                    .map(|s| s.to_string())
                    .or(entry.last_management_action.clone());
                entry.last_management_reason =
                    reason.clone().or(entry.last_management_reason.clone());
                entry.effective_take_profit = value
                    .get("new_tp")
                    .and_then(Value::as_f64)
                    .or(entry.effective_take_profit);
                entry.effective_stop_loss = value
                    .get("new_sl")
                    .and_then(Value::as_f64)
                    .or(entry.effective_stop_loss);
            }
            "llm_pending_order_execution" | "llm_pending_order_execution_error" => {
                let Some(key) = pending_key.as_ref() else {
                    continue;
                };
                let entry = symbol_state.contexts.entry(key.clone()).or_default();
                entry.last_management_action = value
                    .get("action")
                    .and_then(Value::as_str)
                    .map(|s| s.to_string())
                    .or(entry.last_management_action.clone());
                entry.last_management_reason =
                    reason.clone().or(entry.last_management_reason.clone());
                entry.effective_entry_price = value
                    .get("maker_entry_price")
                    .and_then(Value::as_f64)
                    .or_else(|| value.get("new_entry").and_then(Value::as_f64))
                    .or(entry.effective_entry_price);
                entry.effective_take_profit = value
                    .get("effective_take_profit")
                    .and_then(Value::as_f64)
                    .or_else(|| value.get("new_tp").and_then(Value::as_f64))
                    .or(entry.effective_take_profit);
                entry.effective_stop_loss = value
                    .get("effective_stop_loss")
                    .and_then(Value::as_f64)
                    .or_else(|| value.get("new_sl").and_then(Value::as_f64))
                    .or(entry.effective_stop_loss);
                entry.effective_leverage = value
                    .get("leverage")
                    .and_then(Value::as_u64)
                    .map(|v| v as u32)
                    .or(entry.effective_leverage);
            }
            _ => {}
        }
    }
    Ok(())
}

async fn restore_last_management_reasons_from_journal(
    runtime_lifecycle_state: &Arc<Mutex<RuntimeLifecycleStore>>,
) -> Result<()> {
    let path = Path::new(LLM_JOURNAL_FILE);
    if !path.exists() {
        return Ok(());
    }
    let contents = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let mut guard = runtime_lifecycle_state.lock().await;
    for line in contents.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let event_type = value
            .get("event_type")
            .and_then(Value::as_str)
            .unwrap_or("");
        if !matches!(
            event_type,
            "llm_management_execution"
                | "llm_pending_order_execution"
                | "llm_order_execution"
                | "llm_management_execution_error"
                | "llm_pending_order_execution_error"
        ) {
            continue;
        }
        let symbol = value
            .get("symbol")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let Some(symbol) = symbol else {
            continue;
        };
        let reason = value
            .get("reason")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        if reason.is_some() {
            guard.set_last_management_reason(symbol, reason);
        }
    }
    Ok(())
}

async fn validate_reduce_anti_repetition(
    symbol: &str,
    intent: &crate::llm::decision::PositionManagementIntent,
    trading_state: Option<&crate::execution::binance::TradingStateSnapshot>,
    runtime_lifecycle_state: &Arc<Mutex<RuntimeLifecycleStore>>,
) -> Result<()> {
    if !matches!(
        intent.decision,
        crate::llm::decision::PositionManagementDecision::Reduce
    ) {
        return Ok(());
    }
    let state = trading_state
        .ok_or_else(|| anyhow::anyhow!("trading_state missing for REDUCE validation"))?;
    let (key, current_qty, current_price) = primary_position_key(state)
        .ok_or_else(|| anyhow::anyhow!("REDUCE validation requires active position"))?;
    if current_qty <= f64::EPSILON {
        return Err(anyhow::anyhow!(
            "REDUCE validation failed: current_qty is zero"
        ));
    }
    let guard = runtime_lifecycle_state.lock().await;
    let ctx = guard
        .symbol_state(symbol)
        .and_then(|symbol_state| symbol_state.contexts.get(&key));
    let original_qty = ctx
        .map(|c| c.original_qty)
        .unwrap_or(current_qty)
        .max(current_qty);
    let current_pct = if original_qty > f64::EPSILON {
        current_qty / original_qty * 100.0
    } else {
        100.0
    };
    if current_pct <= 30.0 {
        return Err(anyhow::anyhow!(
            "HC-11 violation: current_pct_of_original={:.2} <= 30, REDUCE is forbidden",
            current_pct
        ));
    }
    let reduce_ratio = if let Some(r) = intent.qty_ratio {
        r
    } else if let Some(q) = intent.qty {
        (q / current_qty).clamp(0.0, 1.0)
    } else {
        0.0
    };
    if reduce_ratio > 0.5 {
        return Err(anyhow::anyhow!(
            "REDUCE qty_ratio {:.4} exceeds 0.5 single-action cap",
            reduce_ratio
        ));
    }
    if current_pct < 50.0 && reduce_ratio > 0.25 {
        return Err(anyhow::anyhow!(
            "REDUCE qty_ratio {:.4} exceeds 0.25 cap when current_pct_of_original={:.2} < 50",
            reduce_ratio,
            current_pct
        ));
    }
    if let Some(ctx) = ctx {
        if ctx.last_management_action.as_deref() == Some("REDUCE") {
            if let Some(last) = ctx.reduction_history.last() {
                if (current_price - last.price).abs() <= MANAGEMENT_REDUCTION_LEVEL_THRESHOLD {
                    return Err(anyhow::anyhow!(
                        "HC-10 violation: repeated REDUCE near same level symbol={} current_price={} last_reduce_price={} threshold={}",
                        symbol,
                        current_price,
                        last.price,
                        MANAGEMENT_REDUCTION_LEVEL_THRESHOLD
                    ));
                }
                let current_reason_summary = summarize_reason(&intent.reason);
                if current_reason_summary.eq_ignore_ascii_case(&last.reason_summary) {
                    return Err(anyhow::anyhow!(
                        "repeated REDUCE reason without new structural justification"
                    ));
                }
            }
        }
    }
    Ok(())
}

async fn update_position_context_after_management_action(
    symbol: &str,
    trading_state: Option<&crate::execution::binance::TradingStateSnapshot>,
    intent: &crate::llm::decision::PositionManagementIntent,
    runtime_lifecycle_state: &Arc<Mutex<RuntimeLifecycleStore>>,
) {
    let Some(state) = trading_state else {
        return;
    };
    let Some((key, current_qty, current_price)) = primary_position_key(state) else {
        return;
    };
    let mut guard = runtime_lifecycle_state.lock().await;
    let symbol_state = guard.symbol_state_mut(symbol);
    if matches!(
        intent.decision,
        crate::llm::decision::PositionManagementDecision::Close
    ) {
        symbol_state.contexts.remove(&key);
        return;
    }
    symbol_state.last_management_reason = Some(intent.reason.clone());
    let entry = symbol_state.contexts.entry(key).or_default();
    if entry.original_qty <= f64::EPSILON {
        entry.original_qty = current_qty;
    } else if current_qty > entry.original_qty {
        entry.original_qty = current_qty;
    }
    entry.last_management_action = Some(intent.decision.as_str().to_string());
    entry.last_management_reason = Some(intent.reason.clone());
    if let Some(new_tp) = intent.new_tp {
        entry.effective_take_profit = Some(new_tp);
    }
    if let Some(new_sl) = intent.new_sl {
        entry.effective_stop_loss = Some(new_sl);
    }
    if matches!(
        intent.decision,
        crate::llm::decision::PositionManagementDecision::Reduce
    ) {
        let qty_ratio = if let Some(v) = intent.qty_ratio {
            v
        } else if let Some(q) = intent.qty {
            (q / current_qty).clamp(0.0, 1.0)
        } else {
            0.0
        };
        if qty_ratio > 0.0 {
            entry.reduction_history.push(ReductionHistoryEntry {
                time: Utc::now().format("%H:%M").to_string(),
                qty_ratio,
                reason_summary: summarize_reason(&intent.reason),
                price: current_price,
            });
            if entry.reduction_history.len() > 30 {
                let drop = entry.reduction_history.len() - 30;
                entry.reduction_history.drain(0..drop);
            }
            info!(
                symbol = %symbol,
                qty_ratio = qty_ratio,
                current_price = current_price,
                "recorded_reduce_history_entry"
            );
        }
    }
}

async fn update_position_context_after_pending_order_action(
    symbol: &str,
    trading_state: Option<&crate::execution::binance::TradingStateSnapshot>,
    intent: &crate::llm::decision::PendingOrderManagementIntent,
    runtime_lifecycle_state: &Arc<Mutex<RuntimeLifecycleStore>>,
) {
    let Some(state) = trading_state else {
        return;
    };
    let Some((key, current_qty, _)) = primary_pending_order_key(state) else {
        if matches!(
            intent.decision,
            crate::llm::decision::PendingOrderManagementDecision::Close
        ) {
            let mut guard = runtime_lifecycle_state.lock().await;
            let prefix = format!("{}:", symbol.to_ascii_uppercase());
            let symbol_state = guard.symbol_state_mut(symbol);
            symbol_state
                .contexts
                .retain(|existing_key, _| !existing_key.starts_with(&prefix));
        }
        return;
    };

    let mut guard = runtime_lifecycle_state.lock().await;
    let symbol_state = guard.symbol_state_mut(symbol);
    let fallback_both_context = both_context_alias_for_directional_key(&key)
        .and_then(|alias_key| symbol_state.contexts.get(&alias_key).cloned());
    if matches!(
        intent.decision,
        crate::llm::decision::PendingOrderManagementDecision::Close
    ) {
        symbol_state.contexts.remove(&key);
        return;
    }

    symbol_state.last_management_reason = Some(intent.reason.clone());
    let entry = symbol_state.contexts.entry(key).or_default();
    if let Some(fallback) = fallback_both_context.as_ref() {
        if entry.effective_entry_price.is_none() {
            entry.effective_entry_price = fallback.effective_entry_price;
        }
        if entry.effective_take_profit.is_none() {
            entry.effective_take_profit = fallback.effective_take_profit;
        }
        if entry.effective_stop_loss.is_none() {
            entry.effective_stop_loss = fallback.effective_stop_loss;
        }
        if entry.entry_context.is_none() {
            entry.entry_context = fallback.entry_context.clone();
        }
        if entry.effective_leverage.is_none() {
            entry.effective_leverage = fallback.effective_leverage;
        }
    }
    if entry.original_qty <= f64::EPSILON {
        entry.original_qty = current_qty;
    } else if current_qty > entry.original_qty {
        entry.original_qty = current_qty;
    }
    entry.last_management_action = Some(intent.decision.as_str().to_string());
    entry.last_management_reason = Some(intent.reason.clone());
    if let Some(new_entry) = intent.new_entry {
        entry.effective_entry_price = Some(new_entry);
    }
    if let Some(new_tp) = intent.new_tp {
        entry.effective_take_profit = Some(new_tp);
    }
    if let Some(new_sl) = intent.new_sl {
        entry.effective_stop_loss = Some(new_sl);
    }
    if let Some(new_leverage) = intent.new_leverage {
        entry.effective_leverage = Some(new_leverage as u32);
    }
}

fn summarize_reason(reason: &str) -> String {
    let compact = reason.replace('\n', " ").trim().to_string();
    if compact.len() <= 80 {
        return compact;
    }
    compact.chars().take(80).collect()
}

fn resolve_invoke_management_reason(
    active_position_count: usize,
    open_order_count: usize,
    global_reason: Option<String>,
    position_context: Option<&PositionContextForLlm>,
) -> Option<String> {
    if active_position_count == 0 && open_order_count == 0 {
        return None;
    }
    position_context
        .and_then(|ctx| ctx.last_management_reason.clone())
        .or(global_reason)
}

fn queue_latest_bundle_invoke(
    ctx: &AppContext,
    latest_bundle: &Option<LatestBundle>,
    last_invoked_ts_bucket: &mut Option<DateTime<Utc>>,
    invoke_inflight: &Arc<AtomicBool>,
    invoke_throttle: &Arc<Mutex<InvokeThrottleState>>,
    runtime_lifecycle_state: &Arc<Mutex<RuntimeLifecycleStore>>,
    min_invoke_interval: Duration,
    apply_min_invoke_interval_throttle: bool,
    trigger: &str,
) {
    let Some(bundle) = latest_bundle.clone() else {
        return;
    };
    if last_invoked_ts_bucket
        .map(|ts| ts >= bundle.raw.ts_bucket)
        .unwrap_or(false)
    {
        debug!(
            ts_bucket = %bundle.raw.ts_bucket,
            trigger = trigger,
            "llm invoke skipped: latest bundle unchanged"
        );
        return;
    }
    let staleness_secs = Utc::now()
        .signed_duration_since(bundle.raw.ts_bucket)
        .num_seconds();
    let max_stale = ctx.config.llm.bundle_stale_secs as i64;
    if staleness_secs > max_stale {
        warn!(
            ts_bucket = %bundle.raw.ts_bucket,
            staleness_secs = staleness_secs,
            max_stale_secs = max_stale,
            trigger = trigger,
            "llm invoke skipped: indicator bundle is stale (backfill/replay data)"
        );
        return;
    }
    if invoke_inflight
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        debug!(
            ts_bucket = %bundle.raw.ts_bucket,
            trigger = trigger,
            "llm invoke skipped: previous invocation still in flight"
        );
        return;
    }

    *last_invoked_ts_bucket = Some(bundle.raw.ts_bucket);
    let config = Arc::clone(&ctx.config);
    let db_pool = ctx.db_pool.clone();
    let http_client = ctx.http_client.clone();
    let print_response = ctx.config.llm.print_response;
    let invoke_inflight = Arc::clone(invoke_inflight);
    let invoke_throttle = Arc::clone(invoke_throttle);
    let runtime_lifecycle_state = Arc::clone(runtime_lifecycle_state);
    let trigger = Arc::<str>::from(trigger.to_string());
    let ts_bucket = bundle.raw.ts_bucket;
    tokio::spawn(async move {
        let should_invoke = if apply_min_invoke_interval_throttle {
            let mut gate = invoke_throttle.lock().await;
            let now = Instant::now();
            match gate.last_invoke_at {
                Some(last) => {
                    if now.duration_since(last) < min_invoke_interval {
                        false
                    } else {
                        gate.last_invoke_at = Some(now);
                        true
                    }
                }
                None => {
                    gate.last_invoke_at = Some(now);
                    true
                }
            }
        } else {
            true
        };
        if should_invoke {
            invoke_bundle_models(
                config,
                db_pool,
                http_client,
                print_response,
                bundle,
                trigger,
                runtime_lifecycle_state,
            )
            .await;
        } else {
            debug!(
                ts_bucket = %ts_bucket,
                trigger = %trigger,
                min_invoke_interval_secs = min_invoke_interval.as_secs(),
                "llm invoke skipped: min_invoke_interval throttle active"
            );
        }
        invoke_inflight.store(false, Ordering::Release);
    });
}

async fn invoke_bundle_models(
    config: Arc<RootConfig>,
    db_pool: PgPool,
    http_client: Client,
    print_response: bool,
    bundle: LatestBundle,
    trigger: Arc<str>,
    runtime_lifecycle_state: Arc<Mutex<RuntimeLifecycleStore>>,
) {
    let mut execution_done = false;
    let mut execution_blocked_due_to_stale = false;
    let routing_context = match resolve_invocation_routing_context(
        &config,
        &http_client,
        &bundle,
        &runtime_lifecycle_state,
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(err) if !config.llm.request_enabled => {
            warn!(
                symbol = %bundle.raw.symbol,
                ts_bucket = %bundle.raw.ts_bucket,
                trigger = %trigger,
                error = %err,
                "llm.request_enabled=false fallback to entry-shaped temp_model_input because trading state fetch failed"
            );
            InvocationRoutingContext::persist_fallback()
        }
        Err(err) => {
            let error_chain = format!("{err:#}");
            error!(
                symbol = %bundle.raw.symbol,
                error = %error_chain,
                "llm invoke aborted: failed to fetch trading state"
            );
            return;
        }
    };

    if !config.llm.request_enabled {
        let input = build_invocation_input(
            &bundle,
            routing_context.management_mode,
            routing_context.pending_order_mode,
            routing_context.trading_state.clone(),
            routing_context.invoke_management_reason.clone(),
            routing_context.position_context.clone(),
        );
        let mut input = input;
        if let Err(err) =
            patch_input_kline_history_from_db(&db_pool, &mut input, trigger.as_ref()).await
        {
            warn!(
                symbol = %bundle.raw.symbol,
                ts_bucket = %bundle.raw.ts_bucket,
                trigger = %trigger,
                error = %err,
                "failed to patch missing kline_history bars from db"
            );
        }
        let source_file = minute_bundle_path(&bundle.raw);
        let model_input_files = match persist_model_input_to_disk(
            &bundle.raw,
            trigger.as_ref(),
            routing_context.management_mode,
            routing_context.pending_order_mode,
            &input,
            config.llm.temp_cache_retention_minutes,
        )
        .await
        {
            Ok(path) => Some(path),
            Err(err) => {
                warn!(
                    symbol = %bundle.raw.symbol,
                    ts_bucket = %bundle.raw.ts_bucket,
                    trigger = %trigger,
                    error = %err,
                    "persist llm model input to temp_model_input failed while llm.request_enabled=false"
                );
                None
            }
        };
        let scan_input_path = model_input_files
            .as_ref()
            .map(|files| files.scan.display().to_string())
            .unwrap_or_else(|| "-".to_string());
        let core_input_path = model_input_files
            .as_ref()
            .map(|files| files.core.display().to_string())
            .unwrap_or_else(|| "-".to_string());
        let scan_input_exists = model_input_files
            .as_ref()
            .map(|files| files.scan.exists())
            .unwrap_or(false);
        let core_input_exists = model_input_files
            .as_ref()
            .map(|files| files.core.exists())
            .unwrap_or(false);
        println!(
            "LLM_INPUT_SOURCE ts_bucket={} trigger={} symbol={} source_temp_indicator_file={} source_file_exists={} source_scan_input_file={} scan_input_file_exists={} source_core_input_file={} core_input_file_exists={} indicator_count={} missing_count={} management_mode={} pending_order_mode={}",
            bundle.raw.ts_bucket,
            &*trigger,
            bundle.raw.symbol,
            source_file.display(),
            source_file.exists(),
            scan_input_path,
            scan_input_exists,
            core_input_path,
            core_input_exists,
            bundle.raw.indicator_count,
            bundle.missing_indicator_codes.len(),
            routing_context.management_mode,
            routing_context.pending_order_mode,
        );
        info!(
            ts_bucket = %bundle.raw.ts_bucket,
            trigger = %trigger,
            indicator_count = bundle.raw.indicator_count,
            missing_count = bundle.missing_indicator_codes.len(),
            management_mode = routing_context.management_mode,
            pending_order_mode = routing_context.pending_order_mode,
            "llm request skipped because llm.request_enabled=false; persisted temp_indicator and temp_model_input only"
        );
        return;
    }
    let trading_state = routing_context.trading_state.clone();
    let management_mode = routing_context.management_mode;
    let active_position_count = routing_context.active_position_count;
    let open_order_count = routing_context.open_order_count;
    let pending_order_mode = routing_context.pending_order_mode;
    let context_state = routing_context.context_state;
    let invoke_management_reason = routing_context.invoke_management_reason.clone();
    println!(
        "LLM_INVOKE_CONTEXT ts_bucket={} trigger={} symbol={} management_mode={} pending_order_mode={} context_state={} active_position_count={} open_order_count={} default_model={} prompt_template={} last_management_reason={}",
        bundle.raw.ts_bucket,
        &*trigger,
        bundle.raw.symbol,
        management_mode,
        pending_order_mode,
        context_state,
        active_position_count,
        open_order_count,
        config.active_default_model(),
        config.llm.prompt_template,
        invoke_management_reason.as_deref().unwrap_or("-"),
    );
    if let Some(state) = trading_state.as_ref() {
        let total_unrealized_pnl: f64 = state
            .active_positions
            .iter()
            .map(|p| p.unrealized_pnl)
            .sum();
        let event = json!({
            "event_type": "llm_invoke_context",
            "event_ts": Utc::now().to_rfc3339(),
            "ts_bucket": bundle.raw.ts_bucket.to_rfc3339(),
            "trigger": &*trigger,
            "symbol": bundle.raw.symbol,
            "management_mode": management_mode,
            "pending_order_mode": pending_order_mode,
            "context_state": context_state,
            "active_position_count": active_position_count,
            "open_order_count": open_order_count,
            "default_model": config.active_default_model(),
            "prompt_template": config.llm.prompt_template,
            "last_management_reason": invoke_management_reason.clone(),
            "total_wallet_balance": state.total_wallet_balance,
            "available_balance": state.available_balance,
            "total_unrealized_pnl": total_unrealized_pnl,
        });
        if let Err(err) = append_journal_event(event) {
            warn!(error = %err, "append llm_invoke_context journal failed");
        }
    }
    let input = build_invocation_input(
        &bundle,
        management_mode,
        pending_order_mode,
        trading_state.clone(),
        invoke_management_reason,
        routing_context.position_context.clone(),
    );
    let mut input = input;
    if let Err(err) =
        patch_input_kline_history_from_db(&db_pool, &mut input, trigger.as_ref()).await
    {
        warn!(
            symbol = %bundle.raw.symbol,
            ts_bucket = %bundle.raw.ts_bucket,
            trigger = %trigger,
            error = %err,
            "failed to patch missing kline_history bars from db"
        );
    }

    debug!(
        ts_bucket = %bundle.raw.ts_bucket,
        trigger = %trigger,
        management_mode = management_mode,
        active_position_count = active_position_count,
        open_order_count = open_order_count,
        indicator_count = bundle.raw.indicator_count,
        missing_count = bundle.missing_indicator_codes.len(),
        "llm invoking models"
    );
    let source_file = minute_bundle_path(&bundle.raw);
    let model_input_files = match persist_model_input_to_disk(
        &bundle.raw,
        trigger.as_ref(),
        management_mode,
        pending_order_mode,
        &input,
        config.llm.temp_cache_retention_minutes,
    )
    .await
    {
        Ok(path) => Some(path),
        Err(err) => {
            warn!(
                symbol = %bundle.raw.symbol,
                ts_bucket = %bundle.raw.ts_bucket,
                trigger = %trigger,
                error = %err,
                "persist llm model input to temp_model_input failed"
            );
            None
        }
    };
    let scan_input_path = model_input_files
        .as_ref()
        .map(|files| files.scan.display().to_string())
        .unwrap_or_else(|| "-".to_string());
    let core_input_path = model_input_files
        .as_ref()
        .map(|files| files.core.display().to_string())
        .unwrap_or_else(|| "-".to_string());
    let scan_input_exists = model_input_files
        .as_ref()
        .map(|files| files.scan.exists())
        .unwrap_or(false);
    let core_input_exists = model_input_files
        .as_ref()
        .map(|files| files.core.exists())
        .unwrap_or(false);
    println!(
        "LLM_INPUT_SOURCE ts_bucket={} trigger={} symbol={} source_temp_indicator_file={} source_file_exists={} source_scan_input_file={} scan_input_file_exists={} source_core_input_file={} core_input_file_exists={} indicator_count={} missing_count={} management_mode={} pending_order_mode={}",
        bundle.raw.ts_bucket,
        &*trigger,
        bundle.raw.symbol,
        source_file.display(),
        source_file.exists(),
        scan_input_path,
        scan_input_exists,
        core_input_path,
        core_input_exists,
        bundle.raw.indicator_count,
        bundle.missing_indicator_codes.len(),
        management_mode,
        pending_order_mode,
    );
    let telegram_operator = TelegramOperator::from_config(&config.api.telegram);
    let x_operator = XOperator::from_config(&config.api.x);

    let outputs = invoke_models(&http_client, &config, &input).await;

    // Post-invocation freshness gate. For Claude batch mode, invoke_models blocks
    // while polling (30 s intervals) until results arrive, which can take 60–300 s.
    // The indicator data keeps aging during that wait. If execution is enabled, block
    // it when the data has grown too stale to act on safely.
    let post_invoke_data_age_secs = Utc::now()
        .signed_duration_since(bundle.raw.ts_bucket)
        .num_seconds();
    let max_exec_stale_secs = config.llm.bundle_execution_stale_secs as i64;
    if config.llm.execution.enabled {
        if post_invoke_data_age_secs > max_exec_stale_secs {
            warn!(
                ts_bucket = %bundle.raw.ts_bucket,
                post_invoke_data_age_secs = post_invoke_data_age_secs,
                max_execution_stale_secs = max_exec_stale_secs,
                "llm execution skipped: indicator data too stale after model invocation (batch latency)"
            );
            execution_blocked_due_to_stale = true;
        } else {
            debug!(
                ts_bucket = %bundle.raw.ts_bucket,
                post_invoke_data_age_secs = post_invoke_data_age_secs,
                max_execution_stale_secs = max_exec_stale_secs,
                "llm post-invoke data freshness ok"
            );
        }
    }

    for out in outputs {
        if out.batch_id.is_some() || out.batch_status.is_some() {
            println!(
                "LLM_BATCH_INFO ts_bucket={} trigger={} model={} provider={} model_id={} batch_id={} batch_status={}",
                bundle.raw.ts_bucket,
                &*trigger,
                out.model_name,
                out.provider,
                out.model,
                out.batch_id.as_deref().unwrap_or("-"),
                out.batch_status.as_deref().unwrap_or("-")
            );
        }
        if out.provider_finish_reason.is_some() || out.provider_usage.is_some() {
            println!(
                "LLM_PROVIDER_TRACE ts_bucket={} trigger={} model={} provider={} model_id={} finish_reason={} usage={}",
                bundle.raw.ts_bucket,
                &*trigger,
                out.model_name,
                out.provider,
                out.model,
                out.provider_finish_reason.as_deref().unwrap_or("-"),
                out.provider_usage
                    .as_ref()
                    .map(Value::to_string)
                    .unwrap_or_else(|| "-".to_string())
            );
        }
        if let Some(captures) = out.entry_stage_prompt_inputs.as_deref() {
            match persist_entry_stage_prompt_inputs_to_disk(
                &bundle.raw,
                trigger.as_ref(),
                management_mode,
                pending_order_mode,
                &out.model_name,
                &out.provider,
                &out.model,
                captures,
                config.llm.temp_cache_retention_minutes,
            )
            .await
            {
                Ok(files) => {
                    for (stage, path) in files {
                        println!(
                            "LLM_STAGE_INPUT_SOURCE ts_bucket={} trigger={} symbol={} model={} provider={} model_id={} stage={} source_stage_input_file={} stage_input_file_exists={}",
                            bundle.raw.ts_bucket,
                            &*trigger,
                            bundle.raw.symbol,
                            out.model_name,
                            out.provider,
                            out.model,
                            stage,
                            path.display(),
                            path.exists(),
                        );
                    }
                }
                Err(err) => {
                    warn!(
                        symbol = %bundle.raw.symbol,
                        ts_bucket = %bundle.raw.ts_bucket,
                        trigger = %trigger,
                        model_name = %out.model_name,
                        provider = %out.provider,
                        model_id = %out.model,
                        error = %err,
                        "persist entry stage prompt inputs to temp_model_output failed"
                    );
                }
            }
        }
        if let Some(err) = &out.error {
            error!(
                model_name = %out.model_name,
                provider = %out.provider,
                model = %out.model,
                batch_id = out.batch_id.as_deref().unwrap_or(""),
                batch_status = out.batch_status.as_deref().unwrap_or(""),
                provider_finish_reason = out.provider_finish_reason.as_deref().unwrap_or(""),
                provider_usage = ?out.provider_usage,
                error = %err,
                trigger = %trigger,
                "llm model call failed"
            );
            let text = format!(
                "model={} provider={} model_id={} batch_id={} batch_status={} finish_reason={} usage={}\nERROR: {}",
                out.model_name,
                out.provider,
                out.model,
                out.batch_id.as_deref().unwrap_or("-"),
                out.batch_status.as_deref().unwrap_or("-"),
                out.provider_finish_reason.as_deref().unwrap_or("-"),
                out.provider_usage
                    .as_ref()
                    .map(Value::to_string)
                    .unwrap_or_else(|| "-".to_string()),
                err
            );
            if let Some(stage_trace) = out.entry_stage_trace.as_ref() {
                print_entry_stage_timing(
                    &bundle.raw.ts_bucket,
                    &trigger,
                    &out.model_name,
                    &out.provider,
                    &out.model,
                    management_mode,
                    pending_order_mode,
                    out.latency_ms,
                    stage_trace,
                );
            }
            if print_response {
                if let Some(stage_trace) = out.entry_stage_trace.as_ref() {
                    print_entry_stage_trace(
                        &bundle.raw.ts_bucket,
                        &trigger,
                        &out.model_name,
                        &out.provider,
                        &out.model,
                        stage_trace,
                    );
                }
                println!(
                    "LLM_RESPONSE_ERROR ts_bucket={} trigger={}:\n{}",
                    bundle.raw.ts_bucket, &*trigger, text
                );
            }
            let event = json!({
                "event_type": "llm_response_error",
                "event_ts": Utc::now().to_rfc3339(),
                "ts_bucket": bundle.raw.ts_bucket.to_rfc3339(),
                "trigger": &*trigger,
                "symbol": bundle.raw.symbol,
                "management_mode": management_mode,
                "model_name": out.model_name.clone(),
                "provider": out.provider.clone(),
                "model_id": out.model.clone(),
                "batch_id": out.batch_id.clone(),
                "batch_status": out.batch_status.clone(),
                "provider_finish_reason": out.provider_finish_reason.clone(),
                "provider_usage": out.provider_usage.clone(),
                "entry_stage_trace": out.entry_stage_trace.clone(),
                "error": err,
            });
            if let Err(err) = append_journal_event(event) {
                warn!(error = %err, "append llm_response_error journal failed");
            }
            continue;
        }
        debug!(
            model_name = %out.model_name,
            provider = %out.provider,
            model = %out.model,
            batch_id = out.batch_id.as_deref().unwrap_or(""),
            batch_status = out.batch_status.as_deref().unwrap_or(""),
            provider_finish_reason = out.provider_finish_reason.as_deref().unwrap_or(""),
            provider_usage = ?out.provider_usage,
            latency_ms = out.latency_ms,
            validation_warning = ?out.validation_warning,
            trigger = %trigger,
            "llm model call completed"
        );
        if let Some(stage_trace) = out.entry_stage_trace.as_ref() {
            print_entry_stage_timing(
                &bundle.raw.ts_bucket,
                &trigger,
                &out.model_name,
                &out.provider,
                &out.model,
                management_mode,
                pending_order_mode,
                out.latency_ms,
                stage_trace,
            );
        }
        let should_print_response = print_response || management_mode;
        if should_print_response {
            if let Some(stage_trace) = out.entry_stage_trace.as_ref() {
                print_entry_stage_trace(
                    &bundle.raw.ts_bucket,
                    &trigger,
                    &out.model_name,
                    &out.provider,
                    &out.model,
                    stage_trace,
                );
            }
            let text = out.raw_response_text.clone().unwrap_or_default();
            println!(
                "LLM_RESPONSE ts_bucket={} trigger={} management_mode={} model={} provider={} model_id={} batch_id={} batch_status={} finish_reason={}:\n{}",
                bundle.raw.ts_bucket,
                &*trigger,
                management_mode,
                out.model_name,
                out.provider,
                out.model,
                out.batch_id.as_deref().unwrap_or("-"),
                out.batch_status.as_deref().unwrap_or("-"),
                out.provider_finish_reason.as_deref().unwrap_or("-"),
                text
            );
        }
        if let Some(parsed) = out.parsed_decision.as_ref() {
            println!(
                "LLM_DECISION_METRICS ts_bucket={} trigger={} model={} provider={} model_id={} decision={} expected_move_m={} volatility_unit_v={} m_over_v={}",
                bundle.raw.ts_bucket,
                &*trigger,
                out.model_name,
                out.provider,
                out.model,
                parsed
                    .get("decision")
                    .and_then(Value::as_str)
                    .unwrap_or("-"),
                format_metric_number(extract_nested_f64(
                    parsed,
                    &["analysis", "expected_move_m"]
                )),
                format_metric_number(extract_nested_f64(
                    parsed,
                    &["analysis", "volatility_unit_v"]
                )),
                format_metric_number(extract_nested_f64(parsed, &["analysis", "m_over_v"])),
            );
        } else {
            println!(
                "LLM_DECISION_METRICS ts_bucket={} trigger={} model={} provider={} model_id={} decision=- expected_move_m=- volatility_unit_v=- m_over_v=-",
                bundle.raw.ts_bucket,
                &*trigger,
                out.model_name,
                out.provider,
                out.model,
            );
        }
        let response_event = json!({
            "event_type": "llm_response",
            "event_ts": Utc::now().to_rfc3339(),
            "ts_bucket": bundle.raw.ts_bucket.to_rfc3339(),
            "trigger": &*trigger,
            "symbol": bundle.raw.symbol,
            "management_mode": management_mode,
            "pending_order_mode": pending_order_mode,
            "model_name": out.model_name.clone(),
            "provider": out.provider.clone(),
            "model_id": out.model.clone(),
            "batch_id": out.batch_id.clone(),
            "batch_status": out.batch_status.clone(),
            "provider_finish_reason": out.provider_finish_reason.clone(),
            "provider_usage": out.provider_usage.clone(),
            "parsed_decision": out.parsed_decision.clone(),
            "validation_warning": out.validation_warning.clone(),
            "entry_stage_trace": out.entry_stage_trace.clone(),
            "raw_response_text": out.raw_response_text.clone(),
        });
        if let Err(err) = append_journal_event(response_event) {
            warn!(error = %err, "append llm_response journal failed");
        }

        if execution_done || !config.llm.execution.enabled {
            continue;
        }
        if let Some(warning) = &out.validation_warning {
            warn!(
                model_name = %out.model_name,
                symbol = %bundle.raw.symbol,
                warning = %warning,
                "skip llm execution: parsed decision failed validation"
            );
            println!(
                "LLM_VALIDATION_FAILED ts_bucket={} trigger={} symbol={} model={} management_mode={} pending_order_mode={} warning={}",
                bundle.raw.ts_bucket,
                &*trigger,
                bundle.raw.symbol,
                out.model_name,
                management_mode,
                pending_order_mode,
                warning.replace('\n', " "),
            );
            let event = json!({
                "event_type": "llm_validation_failed",
                "event_ts": Utc::now().to_rfc3339(),
                "ts_bucket": bundle.raw.ts_bucket.to_rfc3339(),
                "trigger": &*trigger,
                "symbol": bundle.raw.symbol,
                "management_mode": management_mode,
                "pending_order_mode": pending_order_mode,
                "model_name": out.model_name.clone(),
                "provider": out.provider.clone(),
                "model_id": out.model.clone(),
                "provider_finish_reason": out.provider_finish_reason.clone(),
                "provider_usage": out.provider_usage.clone(),
                "warning": warning,
            });
            if let Err(err) = append_journal_event(event) {
                warn!(error = %err, "append llm_validation_failed journal failed");
            }
            continue;
        }
        let Some(parsed_decision) = out.parsed_decision.as_ref() else {
            continue;
        };
        if pending_order_mode {
            let pending_ctx = {
                let po = input
                    .management_snapshot
                    .as_ref()
                    .and_then(|ms| ms.pending_order.as_ref());
                PendingOrderContext {
                    has_open_orders: open_order_count > 0,
                    current_entry: po.and_then(|p| p.entry_price),
                    current_tp: po.and_then(|p| p.current_tp_price),
                    current_sl: po.and_then(|p| p.current_sl_price),
                    current_leverage: po.and_then(|p| p.leverage.map(|v| v as f64)),
                }
            };
            let intent = match pending_order_management_intent_from_value_with_context(
                parsed_decision,
                &pending_ctx,
            ) {
                Ok(intent) => intent,
                Err(err) => {
                    warn!(
                        model_name = %out.model_name,
                        symbol = %bundle.raw.symbol,
                        error = %err,
                        "skip llm execution: parse pending-order intent failed"
                    );
                    println!(
                        "LLM_PENDING_ORDER_VALIDATION_FAILED ts_bucket={} trigger={} symbol={} model={} error={}",
                        bundle.raw.ts_bucket,
                        &*trigger,
                        bundle.raw.symbol,
                        out.model_name,
                        err.to_string().replace('\n', " "),
                    );
                    let event = json!({
                        "event_type": "llm_pending_order_validation_failed",
                        "event_ts": Utc::now().to_rfc3339(),
                        "ts_bucket": bundle.raw.ts_bucket.to_rfc3339(),
                        "trigger": &*trigger,
                        "symbol": bundle.raw.symbol,
                        "model_name": out.model_name.clone(),
                        "error": err.to_string(),
                        "parsed_decision": parsed_decision.clone(),
                    });
                    if let Err(err) = append_journal_event(event) {
                        warn!(error = %err, "append llm_pending_order_validation_failed journal failed");
                    }
                    continue;
                }
            };
            if matches!(intent.decision, PendingOrderManagementDecision::Hold) {
                {
                    let mut guard = runtime_lifecycle_state.lock().await;
                    guard.set_last_management_reason(
                        &bundle.raw.symbol,
                        Some(intent.reason.clone()),
                    );
                }
                execution_done = true;
                let fields = derive_pending_order_telegram_fields(
                    trading_state.as_ref(),
                    &runtime_lifecycle_state,
                )
                .await;
                println!(
                    "LLM_PENDING_ORDER_MANAGEMENT ts_bucket={} trigger={} symbol={} model={} action={} new_entry=- new_tp=- new_sl=- dry_run={} open_order_count={} canceled_open_orders=false replacement_order_id=- reason={}",
                    bundle.raw.ts_bucket,
                    &*trigger,
                    bundle.raw.symbol,
                    out.model_name,
                    intent.decision.as_str(),
                    config.llm.execution.dry_run,
                    open_order_count,
                    intent.reason.replace('\n', " "),
                );
                let event = json!({
                    "event_type": "llm_pending_order_execution",
                    "event_ts": Utc::now().to_rfc3339(),
                    "ts_bucket": bundle.raw.ts_bucket.to_rfc3339(),
                    "trigger": &*trigger,
                    "symbol": bundle.raw.symbol,
                    "model_name": out.model_name.clone(),
                    "action": intent.decision.as_str(),
                    "new_entry": intent.new_entry,
                    "new_tp": intent.new_tp,
                    "new_sl": intent.new_sl,
                    "dry_run": config.llm.execution.dry_run,
                    "open_order_count": open_order_count,
                    "canceled_open_orders": false,
                    "replacement_order_id": Option::<i64>::None,
                    "reason": intent.reason,
                });
                if let Err(err) = append_journal_event(event) {
                    warn!(error = %err, "append llm_pending_order_execution(HOLD) journal failed");
                }
                send_trade_signal_notifications(
                    telegram_operator.as_ref(),
                    x_operator.as_ref(),
                    &config.llm.telegram_signal_decisions,
                    &config.llm.x_signal_decisions,
                    &http_client,
                    TradeSignalNotification {
                        ts_bucket: bundle.raw.ts_bucket,
                        trigger: &trigger,
                        symbol: &bundle.raw.symbol,
                        model_name: &out.model_name,
                        decision: intent.decision.as_str(),
                        entry_price: fields.entry_price,
                        leverage: fields.leverage,
                        risk_reward_ratio: fields.risk_reward_ratio,
                        take_profit: fields.take_profit,
                        stop_loss: fields.stop_loss,
                        reason: &intent.reason,
                    },
                )
                .await;
                update_position_context_after_pending_order_action(
                    &bundle.raw.symbol,
                    trading_state.as_ref(),
                    &intent,
                    &runtime_lifecycle_state,
                )
                .await;
                continue;
            }
            if execution_blocked_due_to_stale {
                execution_done = true;
                let fields = with_telegram_field_overrides(
                    derive_pending_order_telegram_fields(
                        trading_state.as_ref(),
                        &runtime_lifecycle_state,
                    )
                    .await,
                    intent.new_entry,
                    None,
                    intent.new_tp,
                    intent.new_sl,
                );
                warn!(
                    model_name = %out.model_name,
                    symbol = %bundle.raw.symbol,
                    action = intent.decision.as_str(),
                    post_invoke_data_age_secs = post_invoke_data_age_secs,
                    max_execution_stale_secs = max_exec_stale_secs,
                    reason = %intent.reason,
                    "llm pending-order execution skipped: indicator data too stale after model invocation"
                );
                println!(
                    "LLM_PENDING_ORDER_SIGNAL_ONLY ts_bucket={} trigger={} symbol={} model={} action={} stale=true post_invoke_data_age_secs={} max_execution_stale_secs={} new_entry={} new_tp={} new_sl={} reason={}",
                    bundle.raw.ts_bucket,
                    &*trigger,
                    bundle.raw.symbol,
                    out.model_name,
                    intent.decision.as_str(),
                    post_invoke_data_age_secs,
                    max_exec_stale_secs,
                    format_metric_number(intent.new_entry),
                    format_metric_number(intent.new_tp),
                    format_metric_number(intent.new_sl),
                    intent.reason.replace('\n', " "),
                );
                let event = json!({
                    "event_type": "llm_pending_order_signal_only_stale",
                    "event_ts": Utc::now().to_rfc3339(),
                    "ts_bucket": bundle.raw.ts_bucket.to_rfc3339(),
                    "trigger": &*trigger,
                    "symbol": bundle.raw.symbol,
                    "model_name": out.model_name.clone(),
                    "action": intent.decision.as_str(),
                    "new_entry": intent.new_entry,
                    "new_tp": intent.new_tp,
                    "new_sl": intent.new_sl,
                    "open_order_count": open_order_count,
                    "post_invoke_data_age_secs": post_invoke_data_age_secs,
                    "max_execution_stale_secs": max_exec_stale_secs,
                    "reason": intent.reason.clone(),
                });
                if let Err(err) = append_journal_event(event) {
                    warn!(error = %err, "append llm_pending_order_signal_only_stale journal failed");
                }
                send_trade_signal_notifications(
                    telegram_operator.as_ref(),
                    x_operator.as_ref(),
                    &config.llm.telegram_signal_decisions,
                    &config.llm.x_signal_decisions,
                    &http_client,
                    TradeSignalNotification {
                        ts_bucket: bundle.raw.ts_bucket,
                        trigger: &trigger,
                        symbol: &bundle.raw.symbol,
                        model_name: &out.model_name,
                        decision: intent.decision.as_str(),
                        entry_price: fields.entry_price,
                        leverage: fields.leverage,
                        risk_reward_ratio: fields.risk_reward_ratio,
                        take_profit: fields.take_profit,
                        stop_loss: fields.stop_loss,
                        reason: &intent.reason,
                    },
                )
                .await;
                continue;
            }
            {
                let mut guard = runtime_lifecycle_state.lock().await;
                let next_reason =
                    if matches!(intent.decision, PendingOrderManagementDecision::Close) {
                        None
                    } else {
                        Some(intent.reason.clone())
                    };
                guard.set_last_management_reason(&bundle.raw.symbol, next_reason);
            }

            match execute_pending_order_intent(
                &http_client,
                &config.api.binance,
                &config.llm.execution,
                &bundle.raw.symbol,
                &intent,
                input
                    .management_snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.pending_order.as_ref())
                    .and_then(|pending| pending.current_tp_price),
                input
                    .management_snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.pending_order.as_ref())
                    .and_then(|pending| pending.current_sl_price),
            )
            .await
            {
                Ok(report) => {
                    execution_done = true;
                    println!(
                        "LLM_PENDING_ORDER_MANAGEMENT ts_bucket={} trigger={} symbol={} model={} action={} new_entry={} new_tp={} new_sl={} dry_run={} open_order_count={} canceled_open_orders={} replacement_order_id={} maker_entry_price={} best_bid_price={} best_ask_price={} leverage={} reason={}",
                        bundle.raw.ts_bucket,
                        &*trigger,
                        bundle.raw.symbol,
                        out.model_name,
                        report.action,
                        format_metric_number(intent.new_entry),
                        format_metric_number(intent.new_tp),
                        format_metric_number(intent.new_sl),
                        report.dry_run,
                        report.open_order_count,
                        report.canceled_open_orders,
                        report.replacement_order_id.map(|id| id.to_string()).unwrap_or_else(|| "-".to_string()),
                        format_metric_number(report.maker_entry_price),
                        format_metric_number(report.best_bid_price),
                        format_metric_number(report.best_ask_price),
                        report.leverage.map(|v| v.to_string()).unwrap_or_else(|| "-".to_string()),
                        intent.reason.replace('\n', " "),
                    );
                    let event = json!({
                        "event_type": "llm_pending_order_execution",
                        "event_ts": Utc::now().to_rfc3339(),
                        "ts_bucket": bundle.raw.ts_bucket.to_rfc3339(),
                        "trigger": &*trigger,
                        "symbol": bundle.raw.symbol,
                        "model_name": out.model_name.clone(),
                        "action": report.action,
                        "new_entry": intent.new_entry,
                        "new_tp": intent.new_tp,
                        "new_sl": intent.new_sl,
                        "dry_run": report.dry_run,
                        "open_order_count": report.open_order_count,
                        "canceled_open_orders": report.canceled_open_orders,
                        "replacement_order_id": report.replacement_order_id,
                        "maker_entry_price": report.maker_entry_price,
                        "effective_take_profit": report.effective_take_profit,
                        "effective_stop_loss": report.effective_stop_loss,
                        "best_bid_price": report.best_bid_price,
                        "best_ask_price": report.best_ask_price,
                        "leverage": report.leverage,
                        "reason": intent.reason,
                    });
                    if let Err(err) = append_journal_event(event) {
                        warn!(error = %err, "append llm_pending_order_execution journal failed");
                    }
                    send_trade_signal_notifications(
                        telegram_operator.as_ref(),
                        x_operator.as_ref(),
                        &config.llm.telegram_signal_decisions,
                        &config.llm.x_signal_decisions,
                        &http_client,
                        TradeSignalNotification {
                            ts_bucket: bundle.raw.ts_bucket,
                            trigger: &trigger,
                            symbol: &bundle.raw.symbol,
                            model_name: &out.model_name,
                            decision: report.action,
                            entry_price: report.maker_entry_price,
                            leverage: report.leverage.map(|v| v as f64),
                            risk_reward_ratio: report
                                .maker_entry_price
                                .zip(report.effective_take_profit)
                                .zip(report.effective_stop_loss)
                                .and_then(|((entry, tp), sl)| {
                                    compute_rr_from_levels(entry, tp, sl)
                                }),
                            take_profit: report.effective_take_profit,
                            stop_loss: report.effective_stop_loss,
                            reason: &intent.reason,
                        },
                    )
                    .await;
                    update_position_context_after_pending_order_action(
                        &bundle.raw.symbol,
                        trading_state.as_ref(),
                        &intent,
                        &runtime_lifecycle_state,
                    )
                    .await;
                }
                Err(err) => {
                    execution_done = true;
                    error!(
                        model_name = %out.model_name,
                        symbol = %bundle.raw.symbol,
                        reason = %intent.reason,
                        error = %err,
                        "llm pending-order execution failed"
                    );
                    println!(
                        "LLM_PENDING_ORDER_EXECUTION_ERROR ts_bucket={} trigger={} symbol={} model={} action={} reason={} error={}",
                        bundle.raw.ts_bucket,
                        &*trigger,
                        bundle.raw.symbol,
                        out.model_name,
                        intent.decision.as_str(),
                        intent.reason.replace('\n', " "),
                        err.to_string().replace('\n', " "),
                    );
                }
            }
            continue;
        }
        if management_mode {
            let intent = match position_management_intent_from_value_with_context(
                parsed_decision,
                active_position_count > 0,
                open_order_count > 0,
            ) {
                Ok(intent) => intent,
                Err(err) => {
                    warn!(
                        model_name = %out.model_name,
                        symbol = %bundle.raw.symbol,
                        error = %err,
                        "skip llm execution: parse management intent failed"
                    );
                    println!(
                        "LLM_POSITION_MANAGEMENT_VALIDATION_FAILED ts_bucket={} trigger={} symbol={} model={} error={}",
                        bundle.raw.ts_bucket,
                        &*trigger,
                        bundle.raw.symbol,
                        out.model_name,
                        err.to_string().replace('\n', " "),
                    );
                    let event = json!({
                        "event_type": "llm_management_validation_failed",
                        "event_ts": Utc::now().to_rfc3339(),
                        "ts_bucket": bundle.raw.ts_bucket.to_rfc3339(),
                        "trigger": &*trigger,
                        "symbol": bundle.raw.symbol,
                        "model_name": out.model_name.clone(),
                        "error": err.to_string(),
                        "parsed_decision": parsed_decision.clone(),
                    });
                    if let Err(err) = append_journal_event(event) {
                        warn!(error = %err, "append llm_management_validation_failed journal failed");
                    }
                    continue;
                }
            };
            if let Err(err) = validate_reduce_anti_repetition(
                &bundle.raw.symbol,
                &intent,
                trading_state.as_ref(),
                &runtime_lifecycle_state,
            )
            .await
            {
                println!(
                    "LLM_POSITION_MANAGEMENT_VALIDATION_FAILED ts_bucket={} trigger={} symbol={} model={} error={}",
                    bundle.raw.ts_bucket,
                    &*trigger,
                    bundle.raw.symbol,
                    out.model_name,
                    err.to_string().replace('\n', " "),
                );
                let event = json!({
                    "event_type": "llm_management_validation_failed",
                    "event_ts": Utc::now().to_rfc3339(),
                    "ts_bucket": bundle.raw.ts_bucket.to_rfc3339(),
                    "trigger": &*trigger,
                    "symbol": bundle.raw.symbol,
                    "model_name": out.model_name.clone(),
                    "error": err.to_string(),
                    "parsed_decision": parsed_decision.clone(),
                });
                if let Err(err) = append_journal_event(event) {
                    warn!(error = %err, "append llm_management_validation_failed journal failed");
                }
                continue;
            }
            if matches!(intent.decision, PositionManagementDecision::Hold) {
                {
                    let mut guard = runtime_lifecycle_state.lock().await;
                    guard.set_last_management_reason(
                        &bundle.raw.symbol,
                        Some(intent.reason.clone()),
                    );
                }
                execution_done = true;
                let fallback_fields = enrich_telegram_fields_from_entry_context(
                    derive_management_telegram_fields(trading_state.as_ref()),
                    trading_state.as_ref(),
                    &runtime_lifecycle_state,
                )
                .await;
                debug!(
                    model_name = %out.model_name,
                    symbol = %bundle.raw.symbol,
                    decision = intent.decision.as_str(),
                    reason = %intent.reason,
                    "llm management decision is HOLD, keep current position/order state"
                );
                println!(
                    "LLM_POSITION_MANAGEMENT ts_bucket={} trigger={} symbol={} model={} action={} qty=- qty_ratio=- is_full_exit={} new_tp=- new_sl=- dry_run={} position_count={} open_order_count={} canceled_open_orders=false add_order_id=- reduce_order_ids=- close_order_ids=- modify_take_profit_order_ids=- modify_stop_loss_order_ids=- reason={}",
                    bundle.raw.ts_bucket,
                    &*trigger,
                    bundle.raw.symbol,
                    out.model_name,
                    intent.decision.as_str(),
                    intent
                        .is_full_exit
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                    config.llm.execution.dry_run,
                    active_position_count,
                    open_order_count,
                    intent.reason.replace('\n', " "),
                );
                let event = json!({
                    "event_type": "llm_management_execution",
                    "event_ts": Utc::now().to_rfc3339(),
                    "ts_bucket": bundle.raw.ts_bucket.to_rfc3339(),
                    "trigger": &*trigger,
                    "symbol": bundle.raw.symbol,
                    "model_name": out.model_name.clone(),
                    "action": intent.decision.as_str(),
                    "qty": intent.qty,
                    "qty_ratio": intent.qty_ratio,
                    "is_full_exit": intent.is_full_exit,
                    "new_tp": intent.new_tp,
                    "new_sl": intent.new_sl,
                    "dry_run": config.llm.execution.dry_run,
                    "position_count": active_position_count,
                    "open_order_count": open_order_count,
                    "canceled_open_orders": false,
                    "add_order_id": Option::<i64>::None,
                    "reduce_order_ids": Vec::<i64>::new(),
                    "close_order_ids": Vec::<i64>::new(),
                    "modify_take_profit_order_ids": Vec::<i64>::new(),
                    "modify_stop_loss_order_ids": Vec::<i64>::new(),
                    "realized_pnl_usdt": 0.0,
                    "pnl_outcome": "flat",
                    "reason": intent.reason,
                });
                if let Err(err) = append_journal_event(event) {
                    warn!(error = %err, "append llm_management_execution(HOLD) journal failed");
                }
                send_trade_signal_notifications(
                    telegram_operator.as_ref(),
                    x_operator.as_ref(),
                    &config.llm.telegram_signal_decisions,
                    &config.llm.x_signal_decisions,
                    &http_client,
                    TradeSignalNotification {
                        ts_bucket: bundle.raw.ts_bucket,
                        trigger: &trigger,
                        symbol: &bundle.raw.symbol,
                        model_name: &out.model_name,
                        decision: intent.decision.as_str(),
                        entry_price: fallback_fields.entry_price,
                        leverage: fallback_fields.leverage,
                        risk_reward_ratio: fallback_fields.risk_reward_ratio,
                        take_profit: intent.new_tp.or(fallback_fields.take_profit),
                        stop_loss: intent.new_sl.or(fallback_fields.stop_loss),
                        reason: &intent.reason,
                    },
                )
                .await;
                update_position_context_after_management_action(
                    &bundle.raw.symbol,
                    trading_state.as_ref(),
                    &intent,
                    &runtime_lifecycle_state,
                )
                .await;
                continue;
            }
            if execution_blocked_due_to_stale {
                execution_done = true;
                let notification_fields = with_telegram_field_overrides(
                    enrich_telegram_fields_from_entry_context(
                        derive_management_telegram_fields(trading_state.as_ref()),
                        trading_state.as_ref(),
                        &runtime_lifecycle_state,
                    )
                    .await,
                    None,
                    None,
                    intent.new_tp,
                    intent.new_sl,
                );
                warn!(
                    model_name = %out.model_name,
                    symbol = %bundle.raw.symbol,
                    action = intent.decision.as_str(),
                    post_invoke_data_age_secs = post_invoke_data_age_secs,
                    max_execution_stale_secs = max_exec_stale_secs,
                    reason = %intent.reason,
                    "llm management execution skipped: indicator data too stale after model invocation"
                );
                println!(
                    "LLM_POSITION_SIGNAL_ONLY ts_bucket={} trigger={} symbol={} model={} action={} stale=true post_invoke_data_age_secs={} max_execution_stale_secs={} qty={} qty_ratio={} is_full_exit={} new_tp={} new_sl={} reason={}",
                    bundle.raw.ts_bucket,
                    &*trigger,
                    bundle.raw.symbol,
                    out.model_name,
                    intent.decision.as_str(),
                    post_invoke_data_age_secs,
                    max_exec_stale_secs,
                    format_metric_number(intent.qty),
                    format_metric_number(intent.qty_ratio),
                    intent
                        .is_full_exit
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                    format_metric_number(intent.new_tp),
                    format_metric_number(intent.new_sl),
                    intent.reason.replace('\n', " "),
                );
                let event = json!({
                    "event_type": "llm_management_signal_only_stale",
                    "event_ts": Utc::now().to_rfc3339(),
                    "ts_bucket": bundle.raw.ts_bucket.to_rfc3339(),
                    "trigger": &*trigger,
                    "symbol": bundle.raw.symbol,
                    "model_name": out.model_name.clone(),
                    "action": intent.decision.as_str(),
                    "qty": intent.qty,
                    "qty_ratio": intent.qty_ratio,
                    "is_full_exit": intent.is_full_exit,
                    "new_tp": intent.new_tp,
                    "new_sl": intent.new_sl,
                    "position_count": active_position_count,
                    "open_order_count": open_order_count,
                    "post_invoke_data_age_secs": post_invoke_data_age_secs,
                    "max_execution_stale_secs": max_exec_stale_secs,
                    "reason": intent.reason.clone(),
                });
                if let Err(err) = append_journal_event(event) {
                    warn!(error = %err, "append llm_management_signal_only_stale journal failed");
                }
                send_trade_signal_notifications(
                    telegram_operator.as_ref(),
                    x_operator.as_ref(),
                    &config.llm.telegram_signal_decisions,
                    &config.llm.x_signal_decisions,
                    &http_client,
                    TradeSignalNotification {
                        ts_bucket: bundle.raw.ts_bucket,
                        trigger: &trigger,
                        symbol: &bundle.raw.symbol,
                        model_name: &out.model_name,
                        decision: intent.decision.as_str(),
                        entry_price: notification_fields.entry_price,
                        leverage: notification_fields.leverage,
                        risk_reward_ratio: notification_fields.risk_reward_ratio,
                        take_profit: notification_fields.take_profit,
                        stop_loss: notification_fields.stop_loss,
                        reason: &intent.reason,
                    },
                )
                .await;
                continue;
            }
            {
                let mut guard = runtime_lifecycle_state.lock().await;
                let next_reason = if matches!(intent.decision, PositionManagementDecision::Close) {
                    None
                } else {
                    Some(intent.reason.clone())
                };
                guard.set_last_management_reason(&bundle.raw.symbol, next_reason);
            }

            match execute_management_intent(
                &http_client,
                &config.api.binance,
                &config.llm.execution,
                &bundle.raw.symbol,
                &intent,
            )
            .await
            {
                Ok(report) => {
                    execution_done = true;
                    debug!(
                        model_name = %out.model_name,
                        symbol = %bundle.raw.symbol,
                        action = report.action,
                        qty = intent.qty.unwrap_or_default(),
                        qty_ratio = intent.qty_ratio.unwrap_or_default(),
                        new_tp = intent.new_tp.unwrap_or_default(),
                        new_sl = intent.new_sl.unwrap_or_default(),
                        dry_run = report.dry_run,
                        position_count = report.position_count,
                        open_order_count = report.open_order_count,
                        canceled_open_orders = report.canceled_open_orders,
                        add_order_id = report.add_order_id.unwrap_or_default(),
                        reduce_order_count = report.reduce_order_ids.len(),
                        close_order_count = report.close_order_ids.len(),
                        modify_tp_order_count = report.modify_take_profit_order_ids.len(),
                        modify_sl_order_count = report.modify_stop_loss_order_ids.len(),
                        reason = %intent.reason,
                        "llm management execution completed"
                    );
                    println!(
                        "LLM_POSITION_MANAGEMENT ts_bucket={} trigger={} symbol={} model={} action={} qty={} qty_ratio={} is_full_exit={} new_tp={} new_sl={} dry_run={} position_count={} open_order_count={} canceled_open_orders={} add_order_id={} reduce_order_ids={} close_order_ids={} modify_take_profit_order_ids={} modify_stop_loss_order_ids={} realized_pnl_usdt={} pnl_outcome={} reason={}",
                        bundle.raw.ts_bucket,
                        &*trigger,
                        bundle.raw.symbol,
                        out.model_name,
                        report.action,
                        intent.qty.map(|v| v.to_string()).unwrap_or_else(|| "-".to_string()),
                        intent.qty_ratio.map(|v| v.to_string()).unwrap_or_else(|| "-".to_string()),
                        intent
                            .is_full_exit
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "-".to_string()),
                        intent.new_tp.map(|v| v.to_string()).unwrap_or_else(|| "-".to_string()),
                        intent.new_sl.map(|v| v.to_string()).unwrap_or_else(|| "-".to_string()),
                        report.dry_run,
                        report.position_count,
                        report.open_order_count,
                        report.canceled_open_orders,
                        report
                            .add_order_id
                            .map(|id| id.to_string())
                            .unwrap_or_else(|| "-".to_string()),
                        if report.reduce_order_ids.is_empty() {
                            "-".to_string()
                        } else {
                            report
                                .reduce_order_ids
                                .iter()
                                .map(|id| id.to_string())
                                .collect::<Vec<_>>()
                                .join(",")
                        },
                        if report.close_order_ids.is_empty() {
                            "-".to_string()
                        } else {
                            report
                                .close_order_ids
                                .iter()
                                .map(|id| id.to_string())
                                .collect::<Vec<_>>()
                                .join(",")
                        },
                        if report.modify_take_profit_order_ids.is_empty() {
                            "-".to_string()
                        } else {
                            report
                                .modify_take_profit_order_ids
                                .iter()
                                .map(|id| id.to_string())
                                .collect::<Vec<_>>()
                                .join(",")
                        },
                        if report.modify_stop_loss_order_ids.is_empty() {
                            "-".to_string()
                        } else {
                            report
                                .modify_stop_loss_order_ids
                                .iter()
                                .map(|id| id.to_string())
                                .collect::<Vec<_>>()
                                .join(",")
                        },
                        report.realized_pnl_usdt,
                        pnl_outcome_label(report.realized_pnl_usdt),
                        intent.reason.replace('\n', " "),
                    );
                    let event = json!({
                        "event_type": "llm_management_execution",
                        "event_ts": Utc::now().to_rfc3339(),
                        "ts_bucket": bundle.raw.ts_bucket.to_rfc3339(),
                        "trigger": &*trigger,
                        "symbol": bundle.raw.symbol,
                        "model_name": out.model_name.clone(),
                        "action": report.action,
                        "qty": intent.qty,
                        "qty_ratio": intent.qty_ratio,
                        "is_full_exit": intent.is_full_exit,
                        "new_tp": intent.new_tp,
                        "new_sl": intent.new_sl,
                        "dry_run": report.dry_run,
                        "position_count": report.position_count,
                        "open_order_count": report.open_order_count,
                        "canceled_open_orders": report.canceled_open_orders,
                        "add_order_id": report.add_order_id,
                        "reduce_order_ids": report.reduce_order_ids,
                        "close_order_ids": report.close_order_ids,
                        "modify_take_profit_order_ids": report.modify_take_profit_order_ids,
                        "modify_stop_loss_order_ids": report.modify_stop_loss_order_ids,
                        "realized_pnl_usdt": report.realized_pnl_usdt,
                        "pnl_outcome": pnl_outcome_label(report.realized_pnl_usdt),
                        "reason": intent.reason,
                    });
                    if let Err(err) = append_journal_event(event) {
                        warn!(error = %err, "append llm_management_execution journal failed");
                    }
                    let notification_fields = with_telegram_field_overrides(
                        enrich_telegram_fields_from_entry_context(
                            derive_management_telegram_fields(trading_state.as_ref()),
                            trading_state.as_ref(),
                            &runtime_lifecycle_state,
                        )
                        .await,
                        None,
                        None,
                        intent.new_tp,
                        intent.new_sl,
                    );
                    send_trade_signal_notifications(
                        telegram_operator.as_ref(),
                        x_operator.as_ref(),
                        &config.llm.telegram_signal_decisions,
                        &config.llm.x_signal_decisions,
                        &http_client,
                        TradeSignalNotification {
                            ts_bucket: bundle.raw.ts_bucket,
                            trigger: &trigger,
                            symbol: &bundle.raw.symbol,
                            model_name: &out.model_name,
                            decision: report.action,
                            entry_price: notification_fields.entry_price,
                            leverage: notification_fields.leverage,
                            risk_reward_ratio: notification_fields.risk_reward_ratio,
                            take_profit: notification_fields.take_profit,
                            stop_loss: notification_fields.stop_loss,
                            reason: &intent.reason,
                        },
                    )
                    .await;
                    update_position_context_after_management_action(
                        &bundle.raw.symbol,
                        trading_state.as_ref(),
                        &intent,
                        &runtime_lifecycle_state,
                    )
                    .await;
                    if report.realized_pnl_usdt != 0.0 {
                        let pnl_event = json!({
                            "event_type": "llm_trade_pnl",
                            "event_ts": Utc::now().to_rfc3339(),
                            "ts_bucket": bundle.raw.ts_bucket.to_rfc3339(),
                            "trigger": &*trigger,
                            "symbol": bundle.raw.symbol,
                            "model_name": out.model_name.clone(),
                            "action": report.action,
                            "realized_pnl_usdt": report.realized_pnl_usdt,
                            "pnl_outcome": pnl_outcome_label(report.realized_pnl_usdt),
                            "reason": intent.reason.clone(),
                        });
                        if let Err(err) = append_journal_event(pnl_event) {
                            warn!(error = %err, "append llm_trade_pnl journal failed");
                        }
                    }
                }
                Err(err) => {
                    execution_done = true;
                    error!(
                        model_name = %out.model_name,
                        symbol = %bundle.raw.symbol,
                        reason = %intent.reason,
                        error = %err,
                        "llm management execution failed"
                    );
                    println!(
                        "LLM_POSITION_MANAGEMENT_ERROR ts_bucket={} trigger={} symbol={} model={} action={} qty={} qty_ratio={} is_full_exit={} new_tp={} new_sl={} reason={} error={}",
                        bundle.raw.ts_bucket,
                        &*trigger,
                        bundle.raw.symbol,
                        out.model_name,
                        intent.decision.as_str(),
                        intent.qty.map(|v| v.to_string()).unwrap_or_else(|| "-".to_string()),
                        intent.qty_ratio.map(|v| v.to_string()).unwrap_or_else(|| "-".to_string()),
                        intent
                            .is_full_exit
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "-".to_string()),
                        intent.new_tp.map(|v| v.to_string()).unwrap_or_else(|| "-".to_string()),
                        intent.new_sl.map(|v| v.to_string()).unwrap_or_else(|| "-".to_string()),
                        intent.reason.replace('\n', " "),
                        err.to_string().replace('\n', " "),
                    );
                    let event = json!({
                        "event_type": "llm_management_execution_error",
                        "event_ts": Utc::now().to_rfc3339(),
                        "ts_bucket": bundle.raw.ts_bucket.to_rfc3339(),
                        "trigger": &*trigger,
                        "symbol": bundle.raw.symbol,
                        "model_name": out.model_name.clone(),
                        "action": intent.decision.as_str(),
                        "qty": intent.qty,
                        "qty_ratio": intent.qty_ratio,
                        "is_full_exit": intent.is_full_exit,
                        "new_tp": intent.new_tp,
                        "new_sl": intent.new_sl,
                        "reason": intent.reason,
                        "error": err.to_string(),
                    });
                    if let Err(err) = append_journal_event(event) {
                        warn!(error = %err, "append llm_management_execution_error journal failed");
                    }
                    let notification_fields = with_telegram_field_overrides(
                        enrich_telegram_fields_from_entry_context(
                            derive_management_telegram_fields(trading_state.as_ref()),
                            trading_state.as_ref(),
                            &runtime_lifecycle_state,
                        )
                        .await,
                        None,
                        None,
                        intent.new_tp,
                        intent.new_sl,
                    );
                    send_trade_signal_notifications(
                        telegram_operator.as_ref(),
                        x_operator.as_ref(),
                        &config.llm.telegram_signal_decisions,
                        &config.llm.x_signal_decisions,
                        &http_client,
                        TradeSignalNotification {
                            ts_bucket: bundle.raw.ts_bucket,
                            trigger: &trigger,
                            symbol: &bundle.raw.symbol,
                            model_name: &out.model_name,
                            decision: intent.decision.as_str(),
                            entry_price: notification_fields.entry_price,
                            leverage: notification_fields.leverage,
                            risk_reward_ratio: notification_fields.risk_reward_ratio,
                            take_profit: notification_fields.take_profit,
                            stop_loss: notification_fields.stop_loss,
                            reason: &intent.reason,
                        },
                    )
                    .await;
                }
            }
        } else {
            let intent = match trade_intent_from_value(parsed_decision) {
                Ok(intent) => intent,
                Err(err) => {
                    warn!(
                        model_name = %out.model_name,
                        symbol = %bundle.raw.symbol,
                        error = %err,
                        "skip llm execution: parse trade intent failed"
                    );
                    println!(
                        "LLM_ORDER_VALIDATION_FAILED ts_bucket={} trigger={} symbol={} model={} error={}",
                        bundle.raw.ts_bucket,
                        &*trigger,
                        bundle.raw.symbol,
                        out.model_name,
                        err.to_string().replace('\n', " "),
                    );
                    let event = json!({
                        "event_type": "llm_trade_validation_failed",
                        "event_ts": Utc::now().to_rfc3339(),
                        "ts_bucket": bundle.raw.ts_bucket.to_rfc3339(),
                        "trigger": &*trigger,
                        "symbol": bundle.raw.symbol,
                        "model_name": out.model_name.clone(),
                        "error": err.to_string(),
                        "parsed_decision": parsed_decision.clone(),
                    });
                    if let Err(err) = append_journal_event(event) {
                        warn!(error = %err, "append llm_trade_validation_failed journal failed");
                    }
                    continue;
                }
            };
            let mut execution_intent = intent.clone();
            if matches!(
                execution_intent.decision,
                TradeDecision::Long | TradeDecision::Short
            ) {
                let computed_rr = execution_intent
                    .entry_price
                    .zip(execution_intent.take_profit)
                    .zip(execution_intent.stop_loss)
                    .and_then(|((entry, tp), sl)| compute_rr_from_levels(entry, tp, sl));
                if let Some(geometry_rr) = computed_rr {
                    if execution_intent
                        .risk_reward_ratio
                        .map(|model_rr| (model_rr - geometry_rr).abs() > 1e-6)
                        .unwrap_or(false)
                    {
                        info!(
                            model_name = %out.model_name,
                            symbol = %bundle.raw.symbol,
                            decision = execution_intent.decision.as_str(),
                            model_rr = execution_intent.risk_reward_ratio.unwrap_or_default(),
                            geometry_rr = geometry_rr,
                            "overriding model rr with geometry-derived rr from entry/tp/sl"
                        );
                    }
                    execution_intent.risk_reward_ratio = Some(geometry_rr);
                }
            }
            if let Some((remapped_entry, remapped_stop_loss)) = remap_trade_entry_and_stop_loss(
                &out.provider,
                &config.llm.execution,
                &execution_intent,
            ) {
                execution_intent.entry_price = Some(remapped_entry);
                execution_intent.stop_loss = Some(remapped_stop_loss);
                info!(
                    model_name = %out.model_name,
                    provider = %out.provider,
                    symbol = %bundle.raw.symbol,
                    decision = execution_intent.decision.as_str(),
                    entry_to_sl_distance_pct = config.llm.execution.entry_sl_remap.entry_to_sl_distance_pct,
                    model_entry = intent.entry_price.unwrap_or_default(),
                    model_stop_loss = intent.stop_loss.unwrap_or_default(),
                    remapped_entry = remapped_entry,
                    remapped_stop_loss = remapped_stop_loss,
                    "applied entry/sl remap for execution"
                );
            }

            if matches!(intent.decision, TradeDecision::NoTrade) {
                debug!(
                    model_name = %out.model_name,
                    symbol = %bundle.raw.symbol,
                    reason = %intent.reason,
                    "llm decision is NO_TRADE, skip exchange execution"
                );
                send_trade_signal_notifications(
                    telegram_operator.as_ref(),
                    x_operator.as_ref(),
                    &config.llm.telegram_signal_decisions,
                    &config.llm.x_signal_decisions,
                    &http_client,
                    TradeSignalNotification {
                        ts_bucket: bundle.raw.ts_bucket,
                        trigger: &trigger,
                        symbol: &bundle.raw.symbol,
                        model_name: &out.model_name,
                        decision: intent.decision.as_str(),
                        entry_price: intent.entry_price,
                        leverage: intent.leverage,
                        risk_reward_ratio: intent.risk_reward_ratio,
                        take_profit: intent.take_profit,
                        stop_loss: intent.stop_loss,
                        reason: &intent.reason,
                    },
                )
                .await;
                execution_done = true;
                continue;
            }
            if execution_blocked_due_to_stale {
                let notification_fields = preview_trade_signal_fields(
                    &http_client,
                    &config,
                    &bundle.raw.symbol,
                    &execution_intent,
                )
                .await;
                execution_done = true;
                warn!(
                    model_name = %out.model_name,
                    symbol = %bundle.raw.symbol,
                    decision = intent.decision.as_str(),
                    post_invoke_data_age_secs = post_invoke_data_age_secs,
                    max_execution_stale_secs = max_exec_stale_secs,
                    reason = %intent.reason,
                    "llm trade execution skipped: indicator data too stale after model invocation"
                );
                println!(
                    "LLM_ORDER_SIGNAL_ONLY ts_bucket={} trigger={} symbol={} model={} decision={} stale=true post_invoke_data_age_secs={} max_execution_stale_secs={} entry={} leverage={} rr={} tp={} sl={} reason={}",
                    bundle.raw.ts_bucket,
                    &*trigger,
                    bundle.raw.symbol,
                    out.model_name,
                    intent.decision.as_str(),
                    post_invoke_data_age_secs,
                    max_exec_stale_secs,
                    format_metric_number(notification_fields.entry_price),
                    format_metric_number(notification_fields.leverage),
                    format_metric_number(notification_fields.risk_reward_ratio),
                    format_metric_number(notification_fields.take_profit),
                    format_metric_number(notification_fields.stop_loss),
                    intent.reason.replace('\n', " "),
                );
                let event = json!({
                    "event_type": "llm_order_signal_only_stale",
                    "event_ts": Utc::now().to_rfc3339(),
                    "ts_bucket": bundle.raw.ts_bucket.to_rfc3339(),
                    "trigger": &*trigger,
                    "symbol": bundle.raw.symbol,
                    "model_name": out.model_name.clone(),
                    "decision": intent.decision.as_str(),
                    "entry_price": notification_fields.entry_price,
                    "leverage": notification_fields.leverage,
                    "risk_reward_ratio": notification_fields.risk_reward_ratio,
                    "take_profit": notification_fields.take_profit,
                    "stop_loss": notification_fields.stop_loss,
                    "post_invoke_data_age_secs": post_invoke_data_age_secs,
                    "max_execution_stale_secs": max_exec_stale_secs,
                    "reason": intent.reason.clone(),
                });
                if let Err(err) = append_journal_event(event) {
                    warn!(error = %err, "append llm_order_signal_only_stale journal failed");
                }
                send_trade_signal_notifications(
                    telegram_operator.as_ref(),
                    x_operator.as_ref(),
                    &config.llm.telegram_signal_decisions,
                    &config.llm.x_signal_decisions,
                    &http_client,
                    TradeSignalNotification {
                        ts_bucket: bundle.raw.ts_bucket,
                        trigger: &trigger,
                        symbol: &bundle.raw.symbol,
                        model_name: &out.model_name,
                        decision: intent.decision.as_str(),
                        entry_price: notification_fields.entry_price,
                        leverage: notification_fields.leverage,
                        risk_reward_ratio: notification_fields.risk_reward_ratio,
                        take_profit: notification_fields.take_profit,
                        stop_loss: notification_fields.stop_loss,
                        reason: &intent.reason,
                    },
                )
                .await;
                continue;
            }
            let entry_v_gate = match evaluate_trade_entry_v_gate(
                &config.llm.execution,
                &execution_intent,
                &input,
                out.entry_stage_trace.as_deref(),
            ) {
                Ok(result) => result,
                Err(err) => {
                    warn!(
                        model_name = %out.model_name,
                        symbol = %bundle.raw.symbol,
                        decision = execution_intent.decision.as_str(),
                        entry_price = execution_intent.entry_price.unwrap_or_default(),
                        take_profit = execution_intent.take_profit.unwrap_or_default(),
                        stop_loss = execution_intent.stop_loss.unwrap_or_default(),
                        reason = %intent.reason,
                        error = %err,
                        "llm trade execution blocked by entry V gate"
                    );
                    println!(
                        "LLM_ORDER_V_GATE_FAILED ts_bucket={} trigger={} symbol={} model={} decision={} entry={} tp={} sl={} min_distance_v={} reason={} error={}",
                        bundle.raw.ts_bucket,
                        &*trigger,
                        bundle.raw.symbol,
                        out.model_name,
                        execution_intent.decision.as_str(),
                        format_metric_number(execution_intent.entry_price),
                        format_metric_number(execution_intent.take_profit),
                        format_metric_number(execution_intent.stop_loss),
                        config.llm.execution.min_distance_v,
                        intent.reason.replace('\n', " "),
                        err.to_string().replace('\n', " "),
                    );
                    let event = json!({
                        "event_type": "llm_order_v_gate_failed",
                        "event_ts": Utc::now().to_rfc3339(),
                        "ts_bucket": bundle.raw.ts_bucket.to_rfc3339(),
                        "trigger": &*trigger,
                        "symbol": bundle.raw.symbol,
                        "model_name": out.model_name.clone(),
                        "decision": execution_intent.decision.as_str(),
                        "entry_price": execution_intent.entry_price,
                        "take_profit": execution_intent.take_profit,
                        "stop_loss": execution_intent.stop_loss,
                        "min_distance_v": config.llm.execution.min_distance_v,
                        "reason": intent.reason.clone(),
                        "error": err.to_string(),
                    });
                    if let Err(err) = append_journal_event(event) {
                        warn!(error = %err, "append llm_order_v_gate_failed journal failed");
                    }
                    continue;
                }
            };
            if let Some(gate) = entry_v_gate.as_ref() {
                if !gate.passed {
                    warn!(
                        model_name = %out.model_name,
                        symbol = %bundle.raw.symbol,
                        decision = execution_intent.decision.as_str(),
                        entry_price = execution_intent.entry_price.unwrap_or_default(),
                        take_profit = execution_intent.take_profit.unwrap_or_default(),
                        stop_loss = execution_intent.stop_loss.unwrap_or_default(),
                        selected_v = gate.resolved_v.value,
                        v_timeframe = gate.resolved_v.timeframe,
                        v_basis = %gate.resolved_v.basis,
                        take_profit_distance = gate.take_profit_distance,
                        take_profit_distance_v = gate.take_profit_distance_v,
                        min_distance_v = gate.min_distance_v,
                        reason = %intent.reason,
                        "llm trade execution blocked by entry V gate"
                    );
                    println!(
                        "LLM_ORDER_V_GATE_FAILED ts_bucket={} trigger={} symbol={} model={} decision={} entry={} tp={} sl={} selected_v={} v_timeframe={} v_basis={} tp_distance={} tp_in_v={} min_distance_v={} gate_passed=false reason={}",
                        bundle.raw.ts_bucket,
                        &*trigger,
                        bundle.raw.symbol,
                        out.model_name,
                        execution_intent.decision.as_str(),
                        format_metric_number(execution_intent.entry_price),
                        format_metric_number(execution_intent.take_profit),
                        format_metric_number(execution_intent.stop_loss),
                        gate.resolved_v.value,
                        gate.resolved_v.timeframe,
                        gate.resolved_v.basis,
                        gate.take_profit_distance,
                        gate.take_profit_distance_v,
                        gate.min_distance_v,
                        intent.reason.replace('\n', " "),
                    );
                    let event = json!({
                        "event_type": "llm_order_v_gate_failed",
                        "event_ts": Utc::now().to_rfc3339(),
                        "ts_bucket": bundle.raw.ts_bucket.to_rfc3339(),
                        "trigger": &*trigger,
                        "symbol": bundle.raw.symbol,
                        "model_name": out.model_name.clone(),
                        "decision": execution_intent.decision.as_str(),
                        "entry_price": execution_intent.entry_price,
                        "take_profit": execution_intent.take_profit,
                        "stop_loss": execution_intent.stop_loss,
                        "selected_v": gate.resolved_v.value,
                        "v_timeframe": gate.resolved_v.timeframe,
                        "v_basis": gate.resolved_v.basis.clone(),
                        "take_profit_distance": gate.take_profit_distance,
                        "take_profit_distance_v": gate.take_profit_distance_v,
                        "min_distance_v": gate.min_distance_v,
                        "gate_passed": false,
                        "reason": intent.reason.clone(),
                    });
                    if let Err(err) = append_journal_event(event) {
                        warn!(error = %err, "append llm_order_v_gate_failed journal failed");
                    }
                    continue;
                }
                info!(
                    model_name = %out.model_name,
                    symbol = %bundle.raw.symbol,
                    decision = execution_intent.decision.as_str(),
                    selected_v = gate.resolved_v.value,
                    v_timeframe = gate.resolved_v.timeframe,
                    v_basis = %gate.resolved_v.basis,
                    take_profit_distance = gate.take_profit_distance,
                    take_profit_distance_v = gate.take_profit_distance_v,
                    min_distance_v = gate.min_distance_v,
                    "llm trade entry V gate passed"
                );
                println!(
                    "LLM_ORDER_V_GATE_PASSED ts_bucket={} trigger={} symbol={} model={} decision={} entry={} tp={} sl={} selected_v={} v_timeframe={} v_basis={} tp_distance={} tp_in_v={} min_distance_v={} gate_passed=true",
                    bundle.raw.ts_bucket,
                    &*trigger,
                    bundle.raw.symbol,
                    out.model_name,
                    execution_intent.decision.as_str(),
                    format_metric_number(execution_intent.entry_price),
                    format_metric_number(execution_intent.take_profit),
                    format_metric_number(execution_intent.stop_loss),
                    gate.resolved_v.value,
                    gate.resolved_v.timeframe,
                    gate.resolved_v.basis,
                    gate.take_profit_distance,
                    gate.take_profit_distance_v,
                    gate.min_distance_v,
                );
            }
            let entry_rr_gate = match evaluate_trade_rr_gate(
                &config.llm.execution,
                &execution_intent,
            ) {
                Ok(result) => result,
                Err(err) => {
                    warn!(
                        model_name = %out.model_name,
                        symbol = %bundle.raw.symbol,
                        decision = execution_intent.decision.as_str(),
                        entry_price = execution_intent.entry_price.unwrap_or_default(),
                        take_profit = execution_intent.take_profit.unwrap_or_default(),
                        stop_loss = execution_intent.stop_loss.unwrap_or_default(),
                        reason = %intent.reason,
                        error = %err,
                        "llm trade execution blocked by entry RR gate"
                    );
                    println!(
                        "LLM_ORDER_RR_GATE_FAILED ts_bucket={} trigger={} symbol={} model={} decision={} entry={} tp={} sl={} min_rr={} reason={} error={}",
                        bundle.raw.ts_bucket,
                        &*trigger,
                        bundle.raw.symbol,
                        out.model_name,
                        execution_intent.decision.as_str(),
                        format_metric_number(execution_intent.entry_price),
                        format_metric_number(execution_intent.take_profit),
                        format_metric_number(execution_intent.stop_loss),
                        config.llm.execution.min_rr,
                        intent.reason.replace('\n', " "),
                        err.to_string().replace('\n', " "),
                    );
                    let event = json!({
                        "event_type": "llm_order_rr_gate_failed",
                        "event_ts": Utc::now().to_rfc3339(),
                        "ts_bucket": bundle.raw.ts_bucket.to_rfc3339(),
                        "trigger": &*trigger,
                        "symbol": bundle.raw.symbol,
                        "model_name": out.model_name.clone(),
                        "decision": execution_intent.decision.as_str(),
                        "entry_price": execution_intent.entry_price,
                        "take_profit": execution_intent.take_profit,
                        "stop_loss": execution_intent.stop_loss,
                        "min_rr": config.llm.execution.min_rr,
                        "reason": intent.reason.clone(),
                        "error": err.to_string(),
                    });
                    if let Err(err) = append_journal_event(event) {
                        warn!(error = %err, "append llm_order_rr_gate_failed journal failed");
                    }
                    continue;
                }
            };
            if let Some(gate) = entry_rr_gate.as_ref() {
                if !gate.passed {
                    warn!(
                        model_name = %out.model_name,
                        symbol = %bundle.raw.symbol,
                        decision = execution_intent.decision.as_str(),
                        entry_price = execution_intent.entry_price.unwrap_or_default(),
                        take_profit = execution_intent.take_profit.unwrap_or_default(),
                        stop_loss = execution_intent.stop_loss.unwrap_or_default(),
                        risk_reward_ratio = gate.risk_reward_ratio,
                        reward_distance = gate.reward_distance,
                        risk_distance = gate.risk_distance,
                        min_rr = gate.min_rr,
                        reason = %intent.reason,
                        "llm trade execution blocked by entry RR gate"
                    );
                    println!(
                        "LLM_ORDER_RR_GATE_FAILED ts_bucket={} trigger={} symbol={} model={} decision={} entry={} tp={} sl={} rr={} reward_distance={} risk_distance={} min_rr={} gate_passed=false reason={}",
                        bundle.raw.ts_bucket,
                        &*trigger,
                        bundle.raw.symbol,
                        out.model_name,
                        execution_intent.decision.as_str(),
                        format_metric_number(execution_intent.entry_price),
                        format_metric_number(execution_intent.take_profit),
                        format_metric_number(execution_intent.stop_loss),
                        gate.risk_reward_ratio,
                        gate.reward_distance,
                        gate.risk_distance,
                        gate.min_rr,
                        intent.reason.replace('\n', " "),
                    );
                    let event = json!({
                        "event_type": "llm_order_rr_gate_failed",
                        "event_ts": Utc::now().to_rfc3339(),
                        "ts_bucket": bundle.raw.ts_bucket.to_rfc3339(),
                        "trigger": &*trigger,
                        "symbol": bundle.raw.symbol,
                        "model_name": out.model_name.clone(),
                        "decision": execution_intent.decision.as_str(),
                        "entry_price": execution_intent.entry_price,
                        "take_profit": execution_intent.take_profit,
                        "stop_loss": execution_intent.stop_loss,
                        "risk_reward_ratio": gate.risk_reward_ratio,
                        "reward_distance": gate.reward_distance,
                        "risk_distance": gate.risk_distance,
                        "min_rr": gate.min_rr,
                        "gate_passed": false,
                        "reason": intent.reason.clone(),
                    });
                    if let Err(err) = append_journal_event(event) {
                        warn!(error = %err, "append llm_order_rr_gate_failed journal failed");
                    }
                    continue;
                }
                info!(
                    model_name = %out.model_name,
                    symbol = %bundle.raw.symbol,
                    decision = execution_intent.decision.as_str(),
                    risk_reward_ratio = gate.risk_reward_ratio,
                    reward_distance = gate.reward_distance,
                    risk_distance = gate.risk_distance,
                    min_rr = gate.min_rr,
                    "llm trade entry RR gate passed"
                );
                println!(
                    "LLM_ORDER_RR_GATE_PASSED ts_bucket={} trigger={} symbol={} model={} decision={} entry={} tp={} sl={} rr={} reward_distance={} risk_distance={} min_rr={} gate_passed=true",
                    bundle.raw.ts_bucket,
                    &*trigger,
                    bundle.raw.symbol,
                    out.model_name,
                    execution_intent.decision.as_str(),
                    format_metric_number(execution_intent.entry_price),
                    format_metric_number(execution_intent.take_profit),
                    format_metric_number(execution_intent.stop_loss),
                    gate.risk_reward_ratio,
                    gate.reward_distance,
                    gate.risk_distance,
                    gate.min_rr,
                );
            }

            match execute_trade_intent(
                &http_client,
                &config.api.binance,
                &config.llm.execution,
                &bundle.raw.symbol,
                &execution_intent,
            )
            .await
            {
                Ok(report) => {
                    execution_done = true;
                    debug!(
                        model_name = %out.model_name,
                        symbol = %bundle.raw.symbol,
                        decision = report.decision,
                        quantity = %report.quantity,
                        leverage = report.leverage,
                        leverage_source = report.leverage_source,
                        margin_budget_usdt = report.margin_budget_usdt,
                        margin_budget_source = report.margin_budget_source,
                        account_total_wallet_balance = report.account_total_wallet_balance,
                        account_available_balance = report.account_available_balance,
                        position_side = report.position_side,
                        dry_run = report.dry_run,
                        maker_entry_price = report.maker_entry_price,
                        best_bid_price = report.best_bid_price,
                        best_ask_price = report.best_ask_price,
                        effective_take_profit = report.actual_take_profit,
                        effective_stop_loss = report.actual_stop_loss,
                        effective_risk_reward_ratio = report.actual_risk_reward_ratio,
                        horizon = execution_intent.horizon.as_deref().unwrap_or(""),
                        risk_reward_ratio = execution_intent.risk_reward_ratio.unwrap_or_default(),
                        swing_logic = execution_intent.swing_logic.as_deref().unwrap_or(""),
                        reason = %intent.reason,
                        entry_order_id = report.entry_order_id.unwrap_or_default(),
                        take_profit_order_id = report.take_profit_order_id.unwrap_or_default(),
                        stop_loss_order_id = report.stop_loss_order_id.unwrap_or_default(),
                        exit_orders_deferred = report.exit_orders_deferred,
                        "llm execution completed"
                    );
                    println!(
                        "LLM_ORDER_EXECUTION ts_bucket={} trigger={} symbol={} model={} decision={} quantity={} leverage={} leverage_source={} margin_budget_usdt={} margin_budget_source={} account_total_wallet_balance={} account_available_balance={} position_side={} dry_run={} maker_entry_price={} best_bid_price={} best_ask_price={} effective_tp={} effective_sl={} effective_rr={} entry_order_id={} take_profit_order_id={} stop_loss_order_id={} exit_orders_deferred={} reason={}",
                        bundle.raw.ts_bucket,
                        &*trigger,
                        bundle.raw.symbol,
                        out.model_name,
                        report.decision,
                        report.quantity,
                        report.leverage,
                        report.leverage_source,
                        report.margin_budget_usdt,
                        report.margin_budget_source,
                        report.account_total_wallet_balance,
                        report.account_available_balance,
                        report.position_side,
                        report.dry_run,
                        report.maker_entry_price,
                        report.best_bid_price,
                        report.best_ask_price,
                        report.actual_take_profit,
                        report.actual_stop_loss,
                        report.actual_risk_reward_ratio,
                        report.entry_order_id.map(|id| id.to_string()).unwrap_or_else(|| "-".to_string()),
                        report.take_profit_order_id.map(|id| id.to_string()).unwrap_or_else(|| "-".to_string()),
                        report.stop_loss_order_id.map(|id| id.to_string()).unwrap_or_else(|| "-".to_string()),
                        report.exit_orders_deferred,
                        intent.reason.replace('\n', " "),
                    );
                    let captured_entry_context = build_entry_context_from_fallbacks(
                        parsed_decision,
                        out.entry_stage_trace.as_deref(),
                        &input,
                        intent.horizon.as_deref(),
                        &intent.reason,
                    );
                    let event = json!({
                        "event_type": "llm_order_execution",
                        "event_ts": Utc::now().to_rfc3339(),
                        "ts_bucket": bundle.raw.ts_bucket.to_rfc3339(),
                        "trigger": &*trigger,
                        "symbol": bundle.raw.symbol,
                        "model_name": out.model_name.clone(),
                        "decision": report.decision,
                        "quantity": report.quantity,
                        "leverage": report.leverage,
                        "leverage_source": report.leverage_source,
                        "margin_budget_usdt": report.margin_budget_usdt,
                        "margin_budget_source": report.margin_budget_source,
                        "account_total_wallet_balance": report.account_total_wallet_balance,
                        "account_available_balance": report.account_available_balance,
                        "position_side": report.position_side,
                        "dry_run": report.dry_run,
                        "maker_entry_price": report.maker_entry_price,
                        "best_bid_price": report.best_bid_price,
                        "best_ask_price": report.best_ask_price,
                        "effective_take_profit": report.actual_take_profit,
                        "effective_stop_loss": report.actual_stop_loss,
                        "effective_risk_reward_ratio": report.actual_risk_reward_ratio,
                        "entry_order_id": report.entry_order_id,
                        "take_profit_order_id": report.take_profit_order_id,
                        "stop_loss_order_id": report.stop_loss_order_id,
                        "exit_orders_deferred": report.exit_orders_deferred,
                        "reason": intent.reason,
                        // entry_context fields — persisted for service-restart restoration
                        "entry_strategy": captured_entry_context.entry_strategy.clone(),
                        "stop_model": captured_entry_context.stop_model.clone(),
                        "entry_mode": captured_entry_context.entry_mode.clone(),
                        "entry_original_tp": captured_entry_context.original_tp,
                        "entry_original_sl": captured_entry_context.original_sl,
                        "sweep_wick_extreme": captured_entry_context.sweep_wick_extreme,
                        "horizon": captured_entry_context.horizon.clone(),
                        "entry_v": captured_entry_context.entry_v,
                    });
                    if let Err(err) = append_journal_event(event) {
                        warn!(error = %err, "append llm_order_execution journal failed");
                    }
                    send_trade_signal_notifications(
                        telegram_operator.as_ref(),
                        x_operator.as_ref(),
                        &config.llm.telegram_signal_decisions,
                        &config.llm.x_signal_decisions,
                        &http_client,
                        TradeSignalNotification {
                            ts_bucket: bundle.raw.ts_bucket,
                            trigger: &trigger,
                            symbol: &bundle.raw.symbol,
                            model_name: &out.model_name,
                            decision: report.decision,
                            entry_price: Some(report.maker_entry_price),
                            leverage: Some(report.leverage as f64),
                            risk_reward_ratio: Some(report.actual_risk_reward_ratio),
                            take_profit: Some(report.actual_take_profit),
                            stop_loss: Some(report.actual_stop_loss),
                            reason: &intent.reason,
                        },
                    )
                    .await;
                    {
                        let mut guard = runtime_lifecycle_state.lock().await;
                        guard.set_last_management_reason(
                            &bundle.raw.symbol,
                            Some(intent.reason.clone()),
                        );
                    }
                    // Capture entry context so management cycles can continue the strategy.
                    let context_keys = execution_context_keys_for_report(
                        &bundle.raw.symbol,
                        &report.position_side,
                        intent.decision,
                    );
                    if !context_keys.is_empty() {
                        let mut guard = runtime_lifecycle_state.lock().await;
                        let symbol_state = guard.symbol_state_mut(&bundle.raw.symbol);
                        for key in context_keys {
                            let ctx_entry = symbol_state.contexts.entry(key).or_default();
                            ctx_entry.effective_entry_price = Some(report.maker_entry_price);
                            ctx_entry.effective_stop_loss = Some(report.actual_stop_loss);
                            ctx_entry.effective_take_profit = Some(report.actual_take_profit);
                            ctx_entry.effective_leverage = Some(report.leverage);
                            ctx_entry.entry_context = Some(captured_entry_context.clone());
                        }
                    }
                }
                Err(err) => {
                    execution_done = true;
                    error!(
                        model_name = %out.model_name,
                        symbol = %bundle.raw.symbol,
                        reason = %intent.reason,
                        error = %err,
                        "llm execution failed"
                    );
                    println!(
                        "LLM_ORDER_EXECUTION_ERROR ts_bucket={} trigger={} symbol={} model={} decision={} reason={} error={}",
                        bundle.raw.ts_bucket,
                        &*trigger,
                        bundle.raw.symbol,
                        out.model_name,
                        intent.decision.as_str(),
                        intent.reason.replace('\n', " "),
                        err.to_string().replace('\n', " "),
                    );
                    let event = json!({
                        "event_type": "llm_order_execution_error",
                        "event_ts": Utc::now().to_rfc3339(),
                        "ts_bucket": bundle.raw.ts_bucket.to_rfc3339(),
                        "trigger": &*trigger,
                        "symbol": bundle.raw.symbol,
                        "model_name": out.model_name.clone(),
                        "decision": intent.decision.as_str(),
                        "reason": intent.reason,
                        "error": err.to_string(),
                    });
                    if let Err(err) = append_journal_event(event) {
                        warn!(error = %err, "append llm_order_execution_error journal failed");
                    }
                    let notification_fields = preview_trade_signal_fields(
                        &http_client,
                        &config,
                        &bundle.raw.symbol,
                        &execution_intent,
                    )
                    .await;
                    send_trade_signal_notifications(
                        telegram_operator.as_ref(),
                        x_operator.as_ref(),
                        &config.llm.telegram_signal_decisions,
                        &config.llm.x_signal_decisions,
                        &http_client,
                        TradeSignalNotification {
                            ts_bucket: bundle.raw.ts_bucket,
                            trigger: &trigger,
                            symbol: &bundle.raw.symbol,
                            model_name: &out.model_name,
                            decision: intent.decision.as_str(),
                            entry_price: notification_fields.entry_price,
                            leverage: notification_fields.leverage,
                            risk_reward_ratio: notification_fields.risk_reward_ratio,
                            take_profit: notification_fields.take_profit,
                            stop_loss: notification_fields.stop_loss,
                            reason: &intent.reason,
                        },
                    )
                    .await;
                }
            }
        }
    }
}

async fn ensure_temp_indicator_dir() -> Result<()> {
    fs::create_dir_all(TEMP_INDICATOR_DIR)
        .with_context(|| format!("create {}", TEMP_INDICATOR_DIR))?;
    Ok(())
}

async fn ensure_temp_model_input_dir() -> Result<()> {
    fs::create_dir_all(TEMP_MODEL_INPUT_DIR)
        .with_context(|| format!("create {}", TEMP_MODEL_INPUT_DIR))?;
    Ok(())
}

async fn ensure_temp_model_output_dir() -> Result<()> {
    fs::create_dir_all(TEMP_MODEL_OUTPUT_DIR)
        .with_context(|| format!("create {}", TEMP_MODEL_OUTPUT_DIR))?;
    Ok(())
}

async fn ensure_llm_journal_dir() -> Result<()> {
    fs::create_dir_all(LLM_JOURNAL_DIR).with_context(|| format!("create {}", LLM_JOURNAL_DIR))?;
    Ok(())
}

fn append_journal_event(event: Value) -> Result<()> {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(LLM_JOURNAL_FILE)
        .with_context(|| format!("open {}", LLM_JOURNAL_FILE))?;
    let mut line = serde_json::to_vec(&event).context("serialize journal event")?;
    line.push(b'\n');
    file.write_all(&line)
        .with_context(|| format!("write {}", LLM_JOURNAL_FILE))?;
    file.flush()
        .with_context(|| format!("flush {}", LLM_JOURNAL_FILE))?;
    Ok(())
}

fn pnl_outcome_label(pnl: f64) -> &'static str {
    if pnl > 0.0 {
        "profit"
    } else if pnl < 0.0 {
        "loss"
    } else {
        "flat"
    }
}

async fn persist_bundle_to_disk(
    bundle: &MinuteBundleEnvelope,
    raw: &[u8],
    retention_minutes: u64,
) -> Result<()> {
    ensure_temp_indicator_dir().await?;

    let raw_json: Value = serde_json::from_slice(raw).context("parse minute bundle as json")?;
    write_pretty_json_file(&minute_bundle_path(bundle), &raw_json)
        .context("write raw minute bundle")?;
    let removed = prune_expired_temp_indicator_files(
        Path::new(TEMP_INDICATOR_DIR),
        bundle.ts_bucket,
        retention_minutes_i64(retention_minutes),
    )
    .context("prune expired temp_indicator cache")?;
    if removed > 0 {
        debug!(
            ts_bucket = %bundle.ts_bucket,
            removed,
            retention_minutes = retention_minutes,
            "pruned expired temp_indicator cache"
        );
    }

    Ok(())
}

fn prune_expired_temp_indicator_files(
    dir: &Path,
    current_ts_bucket: DateTime<Utc>,
    retention_minutes: i64,
) -> Result<usize> {
    prune_expired_timestamped_json_files(dir, current_ts_bucket, retention_minutes)
}

fn prune_expired_temp_model_input_files(
    dir: &Path,
    current_ts_bucket: DateTime<Utc>,
    retention_minutes: i64,
) -> Result<usize> {
    prune_expired_timestamped_json_files(dir, current_ts_bucket, retention_minutes)
}

fn prune_expired_temp_model_output_files(
    dir: &Path,
    current_ts_bucket: DateTime<Utc>,
    retention_minutes: i64,
) -> Result<usize> {
    prune_expired_timestamped_json_files(dir, current_ts_bucket, retention_minutes)
}

fn prune_expired_timestamped_json_files(
    dir: &Path,
    current_ts_bucket: DateTime<Utc>,
    retention_minutes: i64,
) -> Result<usize> {
    let cutoff = current_ts_bucket - ChronoDuration::minutes(retention_minutes);
    let mut removed = 0usize;

    for entry in fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let entry = entry.with_context(|| format!("iterate {}", dir.display()))?;
        let path = entry.path();
        let keep = path
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| name.eq_ignore_ascii_case(".gitignore"))
            .unwrap_or(false);
        if keep {
            continue;
        }

        let file_type = entry
            .file_type()
            .with_context(|| format!("stat {}", path.display()))?;
        if !file_type.is_file() {
            continue;
        }

        let Some(file_ts_bucket) = temp_indicator_ts_bucket_from_path(&path) else {
            continue;
        };
        if file_ts_bucket < cutoff {
            fs::remove_file(&path).with_context(|| format!("remove file {}", path.display()))?;
            removed += 1;
        }
    }

    Ok(removed)
}

fn temp_indicator_ts_bucket_from_path(path: &Path) -> Option<DateTime<Utc>> {
    let file_name = path.file_name()?.to_str()?;
    let ts = file_name.split('_').next()?;
    let naive = NaiveDateTime::parse_from_str(ts, "%Y%m%dT%H%M%SZ").ok()?;
    Some(DateTime::from_naive_utc_and_offset(naive, Utc))
}

async fn persist_model_input_to_disk(
    bundle: &MinuteBundleEnvelope,
    _trigger: &str,
    management_mode: bool,
    pending_order_mode: bool,
    input: &ModelInvocationInput,
    retention_minutes: u64,
) -> Result<PersistedModelInputFiles> {
    ensure_temp_model_input_dir().await?;

    let scan_value = ScanFilter::build_value(input).context("build scan model input")?;
    let core_value = CoreFilter::build_value(input).context("build core model input")?;
    let scan_path = llm_filtered_model_input_path(bundle, "scan");
    let core_stage = CoreFilter::stage_label_for_flags(management_mode, pending_order_mode);
    let core_path = llm_filtered_model_input_path(bundle, core_stage);

    write_pretty_json_file(&scan_path, &scan_value)
        .with_context(|| format!("write {}", scan_path.display()))?;
    write_pretty_json_file(&core_path, &core_value)
        .with_context(|| format!("write {}", core_path.display()))?;
    let removed = prune_expired_temp_model_input_files(
        Path::new(TEMP_MODEL_INPUT_DIR),
        bundle.ts_bucket,
        retention_minutes_i64(retention_minutes),
    )
    .context("prune expired temp_model_input cache")?;
    if removed > 0 {
        debug!(
            ts_bucket = %bundle.ts_bucket,
            removed,
            retention_minutes = retention_minutes,
            "pruned expired temp_model_input cache"
        );
    }

    Ok(PersistedModelInputFiles {
        scan: scan_path,
        core: core_path,
    })
}

async fn persist_entry_stage_prompt_inputs_to_disk(
    bundle: &MinuteBundleEnvelope,
    trigger: &str,
    management_mode: bool,
    pending_order_mode: bool,
    model_name: &str,
    provider: &str,
    model_id: &str,
    captures: &[EntryStagePromptInputCapture],
    retention_minutes: u64,
) -> Result<Vec<(String, PathBuf)>> {
    ensure_temp_model_output_dir().await?;

    let mut files = Vec::with_capacity(captures.len());
    for capture in captures {
        let path = llm_stage_prompt_output_path(
            bundle,
            trigger,
            management_mode,
            pending_order_mode,
            provider,
            model_name,
            &capture.stage,
        );
        let value = json!({
            "ts_bucket": bundle.ts_bucket,
            "symbol": &bundle.symbol,
            "trigger": trigger,
            "management_mode": management_mode,
            "pending_order_mode": pending_order_mode,
            "model_name": model_name,
            "provider": provider,
            "model_id": model_id,
            "stage": &capture.stage,
            "captured_at": Utc::now().to_rfc3339(),
            "prompt_input": &capture.prompt_input,
            "stage_1_setup_scan_json": &capture.stage_1_setup_scan_json,
        });
        write_pretty_json_file(&path, &value)?;
        files.push((capture.stage.clone(), path));
    }

    let removed = prune_expired_temp_model_output_files(
        Path::new(TEMP_MODEL_OUTPUT_DIR),
        bundle.ts_bucket,
        retention_minutes_i64(retention_minutes),
    )
    .context("prune expired temp_model_output cache")?;
    if removed > 0 {
        debug!(
            ts_bucket = %bundle.ts_bucket,
            removed,
            retention_minutes = retention_minutes,
            "pruned expired temp_model_output cache"
        );
    }

    Ok(files)
}

fn retention_minutes_i64(retention_minutes: u64) -> i64 {
    i64::try_from(retention_minutes).unwrap_or(i64::MAX)
}

fn minute_bundle_path(bundle: &MinuteBundleEnvelope) -> PathBuf {
    let ts = bundle.ts_bucket.format("%Y%m%dT%H%M%SZ").to_string();
    let symbol = sanitize_filename_component(&bundle.symbol);
    Path::new(TEMP_INDICATOR_DIR).join(format!("{}_{}.json", ts, symbol))
}

#[derive(Debug, Clone)]
struct PersistedModelInputFiles {
    scan: PathBuf,
    core: PathBuf,
}

fn write_pretty_json_file(path: &Path, value: &Value) -> Result<()> {
    let pretty = serde_json::to_vec_pretty(value).context("serialize pretty json")?;
    let mut file = fs::File::create(path).with_context(|| format!("create {}", path.display()))?;
    file.write_all(&pretty)
        .with_context(|| format!("write {}", path.display()))?;
    file.flush()
        .with_context(|| format!("flush {}", path.display()))?;
    Ok(())
}

fn llm_filtered_model_input_path(bundle: &MinuteBundleEnvelope, stage: &str) -> PathBuf {
    let bucket_ts = bundle.ts_bucket.format("%Y%m%dT%H%M%SZ").to_string();
    let invoke_ts = Utc::now()
        .format("%Y%m%dT%H%M%S%.3fZ")
        .to_string()
        .replace('.', "");
    let symbol = sanitize_filename_component(&bundle.symbol);
    let stage = sanitize_filename_component(stage);
    Path::new(TEMP_MODEL_INPUT_DIR).join(format!(
        "{}_{}_{}_{}.json",
        bucket_ts, symbol, stage, invoke_ts
    ))
}

fn llm_stage_prompt_output_path(
    bundle: &MinuteBundleEnvelope,
    _trigger: &str,
    management_mode: bool,
    pending_order_mode: bool,
    provider: &str,
    model_name: &str,
    stage: &str,
) -> PathBuf {
    let bucket_ts = bundle.ts_bucket.format("%Y%m%dT%H%M%SZ").to_string();
    let invoke_ts = Utc::now()
        .format("%Y%m%dT%H%M%S%.3fZ")
        .to_string()
        .replace('.', "");
    let symbol = sanitize_filename_component(&bundle.symbol);
    let provider = sanitize_filename_component(provider);
    let model_name = sanitize_filename_component(model_name);
    let stage = sanitize_filename_component(stage);
    let mode = if pending_order_mode {
        "pending_management"
    } else if management_mode {
        "management"
    } else {
        "entry"
    };
    Path::new(TEMP_MODEL_OUTPUT_DIR).join(format!(
        "{}_{}_{}_{}_{}_{}_prompt_input_{}.json",
        bucket_ts, symbol, mode, provider, model_name, stage, invoke_ts
    ))
}

fn sanitize_filename_component(raw: &str) -> String {
    let cleaned = raw
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if cleaned.is_empty() {
        "unknown".to_string()
    } else {
        cleaned
    }
}

fn extract_nested_f64(value: &Value, path: &[&str]) -> Option<f64> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_f64()
}

fn extract_nested_str(value: &Value, path: &[&str]) -> Option<String> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_str().map(|s| s.to_string())
}

fn entry_stage_event<'a>(trace: Option<&'a [Value]>, stage: &str) -> Option<&'a Value> {
    trace?.iter().rev().find(|event| {
        event
            .get("stage")
            .and_then(Value::as_str)
            .map(|value| value.eq_ignore_ascii_case(stage))
            .unwrap_or(false)
    })
}

fn entry_stage_latency_ms(trace: Option<&[Value]>, stage: &str) -> Option<u64> {
    entry_stage_event(trace, stage)
        .and_then(|event| event.get("latency_ms"))
        .and_then(Value::as_u64)
}

fn print_entry_stage_timing(
    ts_bucket: &DateTime<Utc>,
    trigger: &str,
    model_name: &str,
    provider: &str,
    model_id: &str,
    management_mode: bool,
    pending_order_mode: bool,
    total_latency_ms: u128,
    stage_trace: &[Value],
) {
    let scan_latency_ms = entry_stage_latency_ms(Some(stage_trace), "scan")
        .map(|v| v.to_string())
        .unwrap_or_else(|| "-".to_string());
    let finalize_latency_ms = entry_stage_latency_ms(Some(stage_trace), "finalize")
        .map(|v| v.to_string())
        .unwrap_or_else(|| "-".to_string());
    let stage2_mode = if pending_order_mode {
        "pending_order"
    } else if management_mode {
        "management"
    } else {
        "entry"
    };
    println!(
        "LLM_STAGE_TIMING ts_bucket={} trigger={} model={} provider={} model_id={} stage2_mode={} scan_latency_ms={} finalize_latency_ms={} total_latency_ms={}",
        ts_bucket,
        trigger,
        model_name,
        provider,
        model_id,
        stage2_mode,
        scan_latency_ms,
        finalize_latency_ms,
        total_latency_ms,
    );
}

fn entry_stage_trace_str(
    trace: Option<&[Value]>,
    finalize_pointer: &str,
    scan_pointer: &str,
) -> Option<String> {
    entry_stage_event(trace, "finalize")
        .and_then(|event| event.pointer(finalize_pointer))
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            entry_stage_event(trace, "scan")
                .and_then(|event| event.pointer(scan_pointer))
                .and_then(Value::as_str)
                .map(str::to_string)
        })
}

fn entry_stage_trace_primary_strategy(trace: Option<&[Value]>) -> Option<String> {
    entry_stage_trace_str(
        trace,
        "/primary_strategy",
        "/parsed_scan/scan/primary_strategy",
    )
}

fn entry_stage_trace_entry_style(trace: Option<&[Value]>) -> Option<String> {
    entry_stage_trace_str(trace, "/entry_style", "/parsed_scan/scan/entry_style")
}

fn entry_stage_trace_stop_model_hint(trace: Option<&[Value]>) -> Option<String> {
    entry_stage_trace_str(
        trace,
        "/stop_model_hint",
        "/parsed_scan/scan/stop_model_hint",
    )
}

fn normalize_entry_style(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
}

fn preferred_v_timeframe(entry_style: Option<&str>, horizon: Option<&str>) -> &'static str {
    if let Some(style) = entry_style {
        match normalize_entry_style(style).as_str() {
            "patient_retest" | "post_sweep_reclaim" | "market_after_flip" | "reversal"
            | "mean_reversion" | "value_area_refill" | "reversal_sequence" => return "4h",
            "trend_continuation"
            | "spot_led_continuation"
            | "hidden_divergence_continuation"
            | "htf_expansion" => return "1d",
            _ => {}
        }
    }

    match horizon.map(|value| value.to_ascii_lowercase()) {
        Some(value) if value.contains("1d") || value.contains("3d") => "1d",
        _ => "4h",
    }
}

fn resolve_entry_v_from_sources(
    trace: Option<&[Value]>,
    input: &ModelInvocationInput,
    horizon: Option<&str>,
) -> Option<f64> {
    resolve_entry_v_from_helper(trace, input, horizon).map(|resolved| resolved.value)
}

fn build_entry_context_from_fallbacks(
    parsed_decision: &Value,
    trace: Option<&[Value]>,
    input: &ModelInvocationInput,
    horizon: Option<&str>,
    entry_reason: &str,
) -> EntryContextForState {
    EntryContextForState {
        entry_strategy: extract_nested_str(parsed_decision, &["analysis", "entry_strategy"])
            .or_else(|| entry_stage_trace_primary_strategy(trace)),
        stop_model: extract_nested_str(parsed_decision, &["analysis", "stop_model"])
            .or_else(|| entry_stage_trace_stop_model_hint(trace)),
        entry_mode: extract_nested_str(parsed_decision, &["params", "entry_mode"])
            .or_else(|| entry_stage_trace_entry_style(trace)),
        original_tp: extract_nested_f64(parsed_decision, &["params", "tp"]),
        original_sl: extract_nested_f64(parsed_decision, &["params", "sl"]),
        sweep_wick_extreme: extract_nested_f64(parsed_decision, &["params", "sweep_wick_extreme"]),
        horizon: horizon.map(str::to_string),
        entry_reason: entry_reason.to_string(),
        entry_v: resolve_entry_v_from_sources(trace, input, horizon),
    }
}

async fn send_trade_signal_notifications(
    telegram_operator: Option<&TelegramOperator>,
    x_operator: Option<&XOperator>,
    telegram_allowed_decisions: &[String],
    x_allowed_decisions: &[String],
    http_client: &Client,
    signal: TradeSignalNotification<'_>,
) {
    send_telegram_signal(
        telegram_operator,
        telegram_allowed_decisions,
        http_client,
        &signal,
    )
    .await;
    send_x_signal(x_operator, x_allowed_decisions, http_client, &signal).await;
}

async fn send_telegram_signal(
    telegram_operator: Option<&TelegramOperator>,
    allowed_decisions: &[String],
    http_client: &Client,
    signal: &TradeSignalNotification<'_>,
) {
    if !decision_is_signal_allowed(signal.decision, allowed_decisions) {
        debug!(
            symbol = %signal.symbol,
            decision = %signal.decision,
            allowed_decisions = ?allowed_decisions,
            "telegram signal skipped: decision is not enabled by llm.telegram_signal_decisions"
        );
        return;
    }
    let Some(operator) = telegram_operator else {
        debug!(
            symbol = %signal.symbol,
            decision = %signal.decision,
            "telegram signal skipped: telegram not configured"
        );
        return;
    };
    match operator.send_trade_signal(http_client, signal).await {
        Ok(()) => {
            println!(
                "LLM_TELEGRAM_SIGNAL ts_bucket={} trigger={} symbol={} model={} decision={} leverage(model)={} rr={} tp={} sl={}",
                signal.ts_bucket,
                signal.trigger,
                signal.symbol,
                signal.model_name,
                signal.decision,
                signal
                    .leverage
                    .map(|v| {
                        if (v - v.round()).abs() < f64::EPSILON {
                            format!("{}", v.round() as i64)
                        } else {
                            format!("{:.2}", v)
                        }
                    })
                    .unwrap_or_else(|| "-".to_string()),
                signal
                    .risk_reward_ratio
                    .map(|v| format!("{:.2}", v))
                    .unwrap_or_else(|| "-".to_string()),
                signal
                    .take_profit
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                signal
                    .stop_loss
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            );
        }
        Err(err) => {
            warn!(
                symbol = %signal.symbol,
                model_name = %signal.model_name,
                decision = signal.decision,
                error = %err,
                "send telegram trade signal failed"
            );
            println!(
                "LLM_TELEGRAM_SIGNAL_ERROR ts_bucket={} trigger={} symbol={} model={} decision={} error={}",
                signal.ts_bucket,
                signal.trigger,
                signal.symbol,
                signal.model_name,
                signal.decision,
                err.to_string().replace('\n', " "),
            );
        }
    }
}

async fn send_x_signal(
    x_operator: Option<&XOperator>,
    allowed_decisions: &[String],
    http_client: &Client,
    signal: &TradeSignalNotification<'_>,
) {
    if !decision_is_signal_allowed(signal.decision, allowed_decisions) {
        debug!(
            symbol = %signal.symbol,
            decision = %signal.decision,
            allowed_decisions = ?allowed_decisions,
            "x signal skipped: decision is not enabled by llm.x_signal_decisions"
        );
        return;
    }
    let Some(operator) = x_operator else {
        debug!(
            symbol = %signal.symbol,
            decision = %signal.decision,
            "x signal skipped: x not configured"
        );
        return;
    };
    match operator.send_trade_signal(http_client, signal).await {
        Ok(()) => {
            println!(
                "LLM_X_SIGNAL ts_bucket={} trigger={} symbol={} model={} decision={} leverage(model)={} rr={} tp={} sl={}",
                signal.ts_bucket,
                signal.trigger,
                signal.symbol,
                signal.model_name,
                signal.decision,
                signal
                    .leverage
                    .map(|v| {
                        if (v - v.round()).abs() < f64::EPSILON {
                            format!("{}", v.round() as i64)
                        } else {
                            format!("{:.2}", v)
                        }
                    })
                    .unwrap_or_else(|| "-".to_string()),
                signal
                    .risk_reward_ratio
                    .map(|v| format!("{:.2}", v))
                    .unwrap_or_else(|| "-".to_string()),
                signal
                    .take_profit
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                signal
                    .stop_loss
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            );
        }
        Err(err) => {
            warn!(
                symbol = %signal.symbol,
                model_name = %signal.model_name,
                decision = signal.decision,
                error = %err,
                "send x trade signal failed"
            );
            println!(
                "LLM_X_SIGNAL_ERROR ts_bucket={} trigger={} symbol={} model={} decision={} error={}",
                signal.ts_bucket,
                signal.trigger,
                signal.symbol,
                signal.model_name,
                signal.decision,
                err.to_string().replace('\n', " "),
            );
        }
    }
}

fn decision_is_signal_allowed(decision: &str, allowed_decisions: &[String]) -> bool {
    allowed_decisions
        .iter()
        .any(|v| v.trim().eq_ignore_ascii_case(decision.trim()))
}

fn format_metric_number(value: Option<f64>) -> String {
    match value {
        Some(v) if v.is_finite() => format!("{:.6}", v),
        _ => "-".to_string(),
    }
}

fn print_entry_stage_trace(
    ts_bucket: &DateTime<Utc>,
    trigger: &str,
    model_name: &str,
    provider: &str,
    model_id: &str,
    stage_trace: &[Value],
) {
    for event in stage_trace {
        let label = match event.get("stage").and_then(Value::as_str) {
            Some("scan") => "ENTRY_SCAN",
            Some("finalize") => "ENTRY_FINALIZE",
            Some("finalize_skipped") => "ENTRY_FINALIZE_SKIPPED",
            _ => "ENTRY_STAGE",
        };
        println!(
            "{} ts_bucket={} trigger={} model={} provider={} model_id={}:\n{}",
            label, ts_bucket, trigger, model_name, provider, model_id, event
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::config::LlmExecutionConfig;
    use crate::execution::binance::{
        ActivePositionSnapshot, OpenOrderSnapshot, TradingStateSnapshot,
    };
    use crate::llm::decision::{TradeDecision, TradeIntent};
    use crate::llm::provider::serialize_llm_input_minified;

    fn sample_model_input(indicators: Value) -> ModelInvocationInput {
        ModelInvocationInput {
            symbol: "ETHUSDT".to_string(),
            ts_bucket: Utc::now(),
            window_code: "1m".to_string(),
            indicator_count: indicators.as_object().map(|obj| obj.len()).unwrap_or(0),
            source_routing_key: "test.route".to_string(),
            source_published_at: None,
            received_at: Utc::now(),
            indicators,
            missing_indicator_codes: vec![],
            management_mode: false,
            pending_order_mode: false,
            trading_state: None,
            management_snapshot: None,
        }
    }

    fn empty_kline_bar(open_time: &str, close_time: &str) -> Value {
        json!({
            "open_time": open_time,
            "close_time": close_time,
            "open": Value::Null,
            "high": Value::Null,
            "low": Value::Null,
            "close": Value::Null,
            "volume_base": 0.0,
            "volume_quote": 0.0,
            "is_closed": true,
            "minutes_covered": 240,
            "expected_minutes": 240
        })
    }

    fn sample_trading_state() -> TradingStateSnapshot {
        TradingStateSnapshot {
            symbol: "ETHUSDT".to_string(),
            has_active_context: true,
            has_active_positions: true,
            has_open_orders: true,
            active_positions: vec![ActivePositionSnapshot {
                position_side: "LONG".to_string(),
                position_amt: 1.0,
                entry_price: 2010.0,
                mark_price: 2025.0,
                unrealized_pnl: 15.0,
                leverage: 10,
            }],
            open_orders: vec![OpenOrderSnapshot {
                order_id: 1,
                side: "BUY".to_string(),
                position_side: "LONG".to_string(),
                order_type: "LIMIT".to_string(),
                status: "NEW".to_string(),
                orig_qty: 1.0,
                executed_qty: 0.0,
                price: 2005.0,
                stop_price: 0.0,
                close_position: false,
                reduce_only: false,
            }],
            total_wallet_balance: 1000.0,
            available_balance: 900.0,
        }
    }

    #[test]
    fn collect_missing_kline_bar_requests_finds_empty_scan_bars() {
        let indicators = json!({
            "kline_history": {
                "payload": {
                    "intervals": {
                        "4h": {
                            "markets": {
                                "futures": {
                                    "bars": [
                                        empty_kline_bar("2026-03-08T16:00:00+00:00", "2026-03-08T20:00:00+00:00")
                                    ]
                                }
                            }
                        }
                    }
                }
            }
        });

        let requests = collect_missing_kline_bar_requests(&indicators);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].market, "futures");
        assert_eq!(requests[0].interval_code, "4h");
        assert_eq!(
            requests[0].open_time,
            DateTime::parse_from_rfc3339("2026-03-08T16:00:00+00:00")
                .expect("parse open time")
                .with_timezone(&Utc)
        );
    }

    #[test]
    fn apply_backfilled_kline_bars_replaces_empty_bar_in_place() {
        let mut indicators = json!({
            "kline_history": {
                "payload": {
                    "intervals": {
                        "4h": {
                            "markets": {
                                "futures": {
                                    "bars": [
                                        empty_kline_bar("2026-03-08T16:00:00+00:00", "2026-03-08T20:00:00+00:00")
                                    ]
                                }
                            }
                        }
                    }
                }
            }
        });
        let request = MissingKlineBarRequest {
            market: "futures".to_string(),
            interval_code: "4h".to_string(),
            open_time: DateTime::parse_from_rfc3339("2026-03-08T16:00:00+00:00")
                .expect("parse open time")
                .with_timezone(&Utc),
        };
        let replacement = json!({
            "open_time": "2026-03-08T16:00:00+00:00",
            "close_time": "2026-03-08T20:00:00+00:00",
            "open": 1942.01,
            "high": 1969.19,
            "low": 1926.73,
            "close": 1963.20,
            "volume_base": 583922.748,
            "volume_quote": 1.0,
            "is_closed": true,
            "minutes_covered": 240,
            "expected_minutes": 240
        });

        let patched = apply_backfilled_kline_bars(&mut indicators, &[(request, replacement)]);
        assert_eq!(patched, 1);
        assert_eq!(
            indicators.pointer("/kline_history/payload/intervals/4h/markets/futures/bars/0/open"),
            Some(&json!(1942.01))
        );
    }

    fn sample_indicators_with_known_v() -> Value {
        json!({
            "kline_history": {
                "payload": {
                    "intervals": {
                        "4h": {
                            "futures": {
                                "bars": [
                                    {"open_time": "2026-03-13T00:00:00Z", "high": 110.0, "low": 100.0, "is_closed": true},
                                    {"open_time": "2026-03-13T04:00:00Z", "high": 118.73, "low": 100.0, "is_closed": true},
                                    {"open_time": "2026-03-13T08:00:00Z", "high": 122.0, "low": 100.0, "is_closed": true},
                                    {"open_time": "2026-03-13T12:00:00Z", "high": 117.0, "low": 100.0, "is_closed": true},
                                    {"open_time": "2026-03-13T16:00:00Z", "high": 121.0, "low": 100.0, "is_closed": true}
                                ]
                            }
                        },
                        "1d": {
                            "futures": {
                                "bars": [
                                    {"open_time": "2026-03-10T00:00:00Z", "high": 160.0, "low": 100.0, "is_closed": true},
                                    {"open_time": "2026-03-11T00:00:00Z", "high": 172.83, "low": 100.0, "is_closed": true},
                                    {"open_time": "2026-03-12T00:00:00Z", "high": 180.0, "low": 100.0, "is_closed": true},
                                    {"open_time": "2026-03-13T00:00:00Z", "high": 170.0, "low": 100.0, "is_closed": true},
                                    {"open_time": "2026-03-14T00:00:00Z", "high": 190.0, "low": 100.0, "is_closed": true}
                                ]
                            }
                        }
                    }
                }
            }
        })
    }

    #[test]
    fn compact_sample_input_size_snapshot() {
        let path = Path::new("/data/systems/llm/temp_indicator/20260304T055500Z_ETHUSDT.json");
        if !path.exists() {
            return;
        }

        let raw = fs::read_to_string(path).expect("read sample indicator file");
        let root: Value = serde_json::from_str(&raw).expect("parse sample indicator json");
        let indicators = root.get("indicators").cloned().expect("indicators field");
        let input = sample_model_input(indicators);
        let compact_minified = serialize_llm_input_minified(&input)
            .expect("serialize prompt input")
            .into_bytes();
        eprintln!("compact_minified_bytes={}", compact_minified.len());
        assert!(!compact_minified.is_empty());
    }

    #[test]
    fn persist_model_input_to_disk_writes_scan_and_mode_specific_core_files() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build tokio runtime");

        runtime.block_on(async {
            let ts_bucket = DateTime::parse_from_rfc3339("2026-03-14T06:29:00Z")
                .expect("parse ts bucket")
                .with_timezone(&Utc);
            let indicators = json!({
                "funding_rate": {
                    "payload": {
                        "funding_current": -0.00004527,
                        "recent_7d": [
                            {
                                "change_ts": "2026-03-14T06:00:00Z",
                                "funding_delta": -0.00000063
                            },
                            {
                                "change_ts": "2026-03-14T06:15:00Z",
                                "funding_delta": -0.00000011
                            }
                        ]
                    }
                },
                "avwap": {
                    "payload": {
                        "fut_mark_price": 2001.5678,
                        "series_by_window": {
                            "15m": [
                                {"ts": "2026-03-14T06:00:00Z", "avwap_fut": 2000.1234},
                                {"ts": "2026-03-14T06:15:00Z", "avwap_fut": 2000.9876}
                            ]
                        }
                    }
                }
            });
            let bundle = MinuteBundleEnvelope {
                msg_type: "indicator_bundle".to_string(),
                routing_key: "test.route".to_string(),
                symbol: "ETHUSDT".to_string(),
                ts_bucket,
                window_code: "15m".to_string(),
                indicator_count: indicators.as_object().map(|obj| obj.len()).unwrap_or(0),
                published_at: None,
                indicators: indicators.clone(),
            };
            let input = sample_model_input(indicators);

            let persisted =
                persist_model_input_to_disk(&bundle, "unit_test", false, false, &input, 5)
                    .await
                    .expect("persist filtered model input");

            assert!(persisted.scan.exists());
            assert!(persisted.core.exists());
            assert!(persisted
                .scan
                .file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.contains("_scan_"))
                .unwrap_or(false));
            assert!(persisted
                .core
                .file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.contains("_entry_core_"))
                .unwrap_or(false));

            let scan_value: Value = serde_json::from_str(
                &fs::read_to_string(&persisted.scan).expect("read scan model input"),
            )
            .expect("parse scan model input");
            let core_value: Value = serde_json::from_str(
                &fs::read_to_string(&persisted.core).expect("read core model input"),
            )
            .expect("parse core model input");

            assert_eq!(
                scan_value.pointer("/indicators/funding_rate/payload/funding_current"),
                Some(&json!(-0.00004527))
            );
            assert_eq!(
                scan_value.pointer("/indicators/avwap/payload/fut_mark_price"),
                Some(&json!(2001.57))
            );
            assert_eq!(
                scan_value
                    .pointer("/indicators/avwap/payload/series_by_window/15m/0/ts")
                    .and_then(Value::as_str),
                Some("2026-03-14T06:15:00Z")
            );
            assert_eq!(
                core_value
                    .pointer("/indicators/avwap/payload/series_by_window/15m/0/ts")
                    .and_then(Value::as_str),
                Some("2026-03-14T06:15:00Z")
            );
            assert_eq!(scan_value.as_object().map(|obj| obj.len()), Some(3));
            assert!(scan_value.pointer("/indicator_count").is_none());
            assert!(scan_value.pointer("/indicators/pre_computed_v").is_none());
            assert!(core_value.pointer("/indicators/pre_computed_v").is_none());

            fs::remove_file(&persisted.scan).expect("cleanup scan model input");
            fs::remove_file(&persisted.core).expect("cleanup core model input");

            let management_input = ModelInvocationInput {
                management_mode: true,
                pending_order_mode: false,
                trading_state: Some(sample_trading_state()),
                management_snapshot: Some(
                    build_management_snapshot_for_llm(Some(&sample_trading_state()), None, None)
                        .expect("management snapshot"),
                ),
                ..sample_model_input(json!({
                    "avwap": {
                        "payload": {
                            "fut_mark_price": 2001.5678,
                            "series_by_window": {
                                "15m": [
                                    {"ts": "2026-03-14T06:00:00Z", "avwap_fut": 2000.1234}
                                ]
                            }
                        }
                    }
                }))
            };
            let management_persisted = persist_model_input_to_disk(
                &bundle,
                "unit_test",
                true,
                false,
                &management_input,
                5,
            )
            .await
            .expect("persist management model input");
            assert!(management_persisted
                .scan
                .file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.contains("_scan_"))
                .unwrap_or(false));
            assert!(management_persisted
                .core
                .file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.contains("_management_core_"))
                .unwrap_or(false));
            fs::remove_file(&management_persisted.scan)
                .expect("cleanup management scan model input");
            fs::remove_file(&management_persisted.core)
                .expect("cleanup management core model input");

            let pending_input = ModelInvocationInput {
                management_mode: false,
                pending_order_mode: true,
                trading_state: Some(sample_trading_state()),
                management_snapshot: Some(
                    build_management_snapshot_for_llm(Some(&sample_trading_state()), None, None)
                        .expect("pending snapshot"),
                ),
                ..sample_model_input(json!({
                    "avwap": {
                        "payload": {
                            "fut_mark_price": 2001.5678,
                            "series_by_window": {
                                "15m": [
                                    {"ts": "2026-03-14T06:00:00Z", "avwap_fut": 2000.1234}
                                ]
                            }
                        }
                    }
                }))
            };
            let pending_persisted =
                persist_model_input_to_disk(&bundle, "unit_test", false, true, &pending_input, 5)
                    .await
                    .expect("persist pending model input");
            assert!(pending_persisted
                .scan
                .file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.contains("_scan_"))
                .unwrap_or(false));
            assert!(pending_persisted
                .core
                .file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.contains("_pending_core_"))
                .unwrap_or(false));
            fs::remove_file(&pending_persisted.scan).expect("cleanup pending scan model input");
            fs::remove_file(&pending_persisted.core).expect("cleanup pending core model input");
        });
    }

    #[test]
    fn persist_entry_stage_prompt_inputs_to_disk_writes_to_temp_model_output() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build tokio runtime");

        runtime.block_on(async {
            let ts_bucket = DateTime::parse_from_rfc3339("2026-03-14T11:15:00Z")
                .expect("parse ts bucket")
                .with_timezone(&Utc);
            let bundle = MinuteBundleEnvelope {
                msg_type: "indicator_bundle".to_string(),
                routing_key: "test.route".to_string(),
                symbol: "ETHUSDT".to_string(),
                ts_bucket,
                window_code: "15m".to_string(),
                indicator_count: 1,
                published_at: None,
                indicators: json!({
                    "kline_history": {
                        "payload": {
                            "intervals": {
                                "15m": {
                                    "futures": {
                                        "bars": [
                                            {"open_time": "2026-03-14T11:00:00Z", "high": 2010.0, "low": 2000.0, "is_closed": true}
                                        ]
                                    }
                                }
                            }
                        }
                    }
                }),
            };
            let captures = vec![
                EntryStagePromptInputCapture {
                    stage: "scan".to_string(),
                    prompt_input: json!({"symbol": "ETHUSDT", "stage": "scan"}),
                    stage_1_setup_scan_json: None,
                },
                EntryStagePromptInputCapture {
                    stage: "finalize".to_string(),
                    prompt_input: json!({"symbol": "ETHUSDT", "stage": "finalize"}),
                    stage_1_setup_scan_json: Some(json!({"scan_bias": "bullish"})),
                },
            ];

            let files = persist_entry_stage_prompt_inputs_to_disk(
                &bundle,
                "unit_test",
                false,
                false,
                "custom_llm",
                "custom_llm",
                "custom_llm",
                &captures,
                5,
            )
            .await
            .expect("persist entry stage prompt inputs");

            assert_eq!(files.len(), 2);
            for (stage, path) in files {
                assert!(path.exists(), "expected {stage} prompt input file to exist");
                assert!(path.starts_with(TEMP_MODEL_OUTPUT_DIR));
                assert!(!path.starts_with(TEMP_MODEL_INPUT_DIR));
                assert!(path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .map(|name| name.contains("_prompt_input_"))
                    .unwrap_or(false));

                let saved: Value = serde_json::from_str(
                    &fs::read_to_string(&path).expect("read prompt input artifact"),
                )
                .expect("parse prompt input artifact");
                assert_eq!(
                    saved.get("stage").and_then(Value::as_str),
                    Some(stage.as_str())
                );

                fs::remove_file(&path).expect("cleanup prompt input artifact");
            }
        });
    }

    #[test]
    fn hold_telegram_fields_use_live_position_and_exit_orders() {
        let state = TradingStateSnapshot {
            symbol: "ETHUSDT".to_string(),
            has_active_context: true,
            has_active_positions: true,
            has_open_orders: true,
            active_positions: vec![ActivePositionSnapshot {
                position_side: "SHORT".to_string(),
                position_amt: -0.2,
                entry_price: 2100.0,
                mark_price: 2095.0,
                unrealized_pnl: 1.0,
                leverage: 50,
            }],
            open_orders: vec![
                OpenOrderSnapshot {
                    order_id: 1,
                    side: "BUY".to_string(),
                    position_side: "SHORT".to_string(),
                    order_type: "TAKE_PROFIT_MARKET".to_string(),
                    status: "NEW".to_string(),
                    orig_qty: 0.2,
                    executed_qty: 0.0,
                    price: 0.0,
                    stop_price: 2050.0,
                    close_position: true,
                    reduce_only: true,
                },
                OpenOrderSnapshot {
                    order_id: 2,
                    side: "BUY".to_string(),
                    position_side: "SHORT".to_string(),
                    order_type: "STOP_MARKET".to_string(),
                    status: "NEW".to_string(),
                    orig_qty: 0.2,
                    executed_qty: 0.0,
                    price: 0.0,
                    stop_price: 2125.0,
                    close_position: true,
                    reduce_only: true,
                },
            ],
            total_wallet_balance: 1000.0,
            available_balance: 800.0,
        };

        let fields = derive_management_telegram_fields(Some(&state));
        assert_eq!(fields.entry_price, Some(2100.0));
        assert_eq!(fields.leverage, Some(50.0));
        assert_eq!(fields.take_profit, Some(2050.0));
        assert_eq!(fields.stop_loss, Some(2125.0));
        assert_eq!(fields.risk_reward_ratio, Some(2.0));
    }

    #[test]
    fn hold_telegram_fields_are_empty_without_active_position() {
        let state = TradingStateSnapshot {
            symbol: "ETHUSDT".to_string(),
            has_active_context: false,
            has_active_positions: false,
            has_open_orders: true,
            active_positions: Vec::new(),
            open_orders: Vec::new(),
            total_wallet_balance: 1000.0,
            available_balance: 800.0,
        };

        let fields = derive_management_telegram_fields(Some(&state));
        assert!(fields.entry_price.is_none());
        assert!(fields.leverage.is_none());
        assert!(fields.risk_reward_ratio.is_none());
        assert!(fields.take_profit.is_none());
        assert!(fields.stop_loss.is_none());
    }

    #[test]
    fn remap_trade_entry_and_stop_loss_follows_configured_pct() {
        let mut execution_config = LlmExecutionConfig::default();
        execution_config.entry_sl_remap.enabled = true;
        execution_config.entry_sl_remap.entry_to_sl_distance_pct = 50.0;
        let intent = TradeIntent {
            decision: TradeDecision::Short,
            entry_price: Some(100.0),
            take_profit: Some(90.0),
            stop_loss: Some(110.0),
            leverage: Some(5.0),
            risk_reward_ratio: Some(2.0),
            horizon: Some("4h".to_string()),
            swing_logic: Some("test".to_string()),
            reason: "test".to_string(),
        };
        let (entry, sl) = remap_trade_entry_and_stop_loss("qwen", &execution_config, &intent)
            .expect("should remap");
        assert!((entry - 105.0).abs() < 1e-9);
        assert!((sl - 112.5).abs() < 1e-9);

        execution_config.entry_sl_remap.entry_to_sl_distance_pct = 100.0;
        let (entry_full, sl_full) =
            remap_trade_entry_and_stop_loss("qwen", &execution_config, &intent)
                .expect("should remap at 100%");
        assert!((entry_full - 110.0).abs() < 1e-9);
        assert!((sl_full - 120.0).abs() < 1e-9);

        let (entry_other_provider, sl_other_provider) =
            remap_trade_entry_and_stop_loss("claude", &execution_config, &intent)
                .expect("all providers should remap");
        assert!((entry_other_provider - 110.0).abs() < 1e-9);
        assert!((sl_other_provider - 120.0).abs() < 1e-9);

        execution_config.entry_sl_remap.entry_to_sl_distance_pct = 20.0;
        let long_example = TradeIntent {
            decision: TradeDecision::Long,
            entry_price: Some(2039.3),
            take_profit: Some(2080.0),
            stop_loss: Some(2020.0),
            leverage: Some(2.0),
            risk_reward_ratio: Some(2.11),
            horizon: Some("4h".to_string()),
            swing_logic: Some("test".to_string()),
            reason: "test".to_string(),
        };
        let (example_entry, _) =
            remap_trade_entry_and_stop_loss("custom_llm", &execution_config, &long_example)
                .expect("20 pct long example should remap");
        assert!((example_entry - 2035.44).abs() < 1e-9);
    }

    #[test]
    fn trade_signal_fields_from_intent_use_model_values() {
        let intent = TradeIntent {
            decision: TradeDecision::Long,
            entry_price: Some(2039.3),
            take_profit: Some(2080.0),
            stop_loss: Some(2020.0),
            leverage: Some(2.0),
            risk_reward_ratio: Some(2.11),
            horizon: Some("4h".to_string()),
            swing_logic: Some("test".to_string()),
            reason: "test".to_string(),
        };

        let fields = TradeSignalFields::from_intent(&intent);
        assert_eq!(fields.entry_price, Some(2039.3));
        assert_eq!(fields.leverage, Some(2.0));
        assert_eq!(fields.risk_reward_ratio, Some(2.11));
        assert_eq!(fields.take_profit, Some(2080.0));
        assert_eq!(fields.stop_loss, Some(2020.0));
    }

    #[test]
    fn trade_signal_fields_from_report_use_effective_execution_values() {
        let report = crate::execution::binance::ExecutionReport {
            decision: "LONG",
            quantity: "0.100".to_string(),
            leverage: 16,
            leverage_source: "model_ratio",
            margin_budget_usdt: 50.0,
            margin_budget_source: "fixed_usdt",
            account_total_wallet_balance: 1000.0,
            account_available_balance: 900.0,
            position_side: "LONG",
            dry_run: true,
            entry_order_id: None,
            take_profit_order_id: None,
            stop_loss_order_id: None,
            exit_orders_deferred: false,
            maker_entry_price: 2020.0,
            actual_take_profit: 2080.0,
            actual_stop_loss: 1991.56,
            actual_risk_reward_ratio: 2.1108647450110867,
            best_bid_price: 2020.12,
            best_ask_price: 2020.13,
        };

        let fields = TradeSignalFields::from_report(&report);
        assert_eq!(fields.entry_price, Some(2020.0));
        assert_eq!(fields.leverage, Some(16.0));
        assert_eq!(fields.take_profit, Some(2080.0));
        assert_eq!(fields.stop_loss, Some(1991.56));
        assert_eq!(fields.risk_reward_ratio, Some(2.1108647450110867));
    }

    #[test]
    fn management_snapshot_uses_live_binance_entry_and_exit_levels() {
        let state = TradingStateSnapshot {
            symbol: "ETHUSDT".to_string(),
            has_active_context: true,
            has_active_positions: true,
            has_open_orders: true,
            active_positions: vec![ActivePositionSnapshot {
                position_side: "SHORT".to_string(),
                position_amt: -0.2,
                entry_price: 2100.0,
                mark_price: 2095.0,
                unrealized_pnl: 1.0,
                leverage: 50,
            }],
            open_orders: vec![
                OpenOrderSnapshot {
                    order_id: 1,
                    side: "BUY".to_string(),
                    position_side: "SHORT".to_string(),
                    order_type: "TAKE_PROFIT_MARKET".to_string(),
                    status: "NEW".to_string(),
                    orig_qty: 0.2,
                    executed_qty: 0.0,
                    price: 0.0,
                    stop_price: 2050.0,
                    close_position: true,
                    reduce_only: true,
                },
                OpenOrderSnapshot {
                    order_id: 2,
                    side: "BUY".to_string(),
                    position_side: "SHORT".to_string(),
                    order_type: "STOP_MARKET".to_string(),
                    status: "NEW".to_string(),
                    orig_qty: 0.2,
                    executed_qty: 0.0,
                    price: 0.0,
                    stop_price: 2125.0,
                    close_position: true,
                    reduce_only: true,
                },
            ],
            total_wallet_balance: 1000.0,
            available_balance: 800.0,
        };

        let snapshot =
            build_management_snapshot_for_llm(Some(&state), None, None).expect("snapshot exists");
        assert_eq!(snapshot.positions.len(), 1);
        let position = &snapshot.positions[0];
        assert_eq!(position.entry_price, 2100.0);
        assert_eq!(position.pnl_by_latest_price, 1.0);
        assert_eq!(position.current_tp_price, Some(2050.0));
        assert_eq!(position.current_sl_price, Some(2125.0));
    }

    #[test]
    fn management_snapshot_includes_pending_order_context_for_open_orders_only() {
        let state = TradingStateSnapshot {
            symbol: "ETHUSDT".to_string(),
            has_active_context: true,
            has_active_positions: false,
            has_open_orders: true,
            active_positions: vec![],
            open_orders: vec![
                OpenOrderSnapshot {
                    order_id: 11,
                    side: "BUY".to_string(),
                    position_side: "LONG".to_string(),
                    order_type: "LIMIT".to_string(),
                    status: "NEW".to_string(),
                    orig_qty: 0.06,
                    executed_qty: 0.0,
                    price: 1965.5,
                    stop_price: 0.0,
                    close_position: false,
                    reduce_only: false,
                },
                OpenOrderSnapshot {
                    order_id: 12,
                    side: "SELL".to_string(),
                    position_side: "LONG".to_string(),
                    order_type: "TAKE_PROFIT_MARKET".to_string(),
                    status: "NEW".to_string(),
                    orig_qty: 0.06,
                    executed_qty: 0.0,
                    price: 0.0,
                    stop_price: 1974.0,
                    close_position: true,
                    reduce_only: true,
                },
                OpenOrderSnapshot {
                    order_id: 13,
                    side: "SELL".to_string(),
                    position_side: "LONG".to_string(),
                    order_type: "STOP_MARKET".to_string(),
                    status: "NEW".to_string(),
                    orig_qty: 0.06,
                    executed_qty: 0.0,
                    price: 0.0,
                    stop_price: 1961.2,
                    close_position: true,
                    reduce_only: true,
                },
            ],
            total_wallet_balance: 1000.0,
            available_balance: 800.0,
        };

        let position_context = PositionContextForLlm {
            original_qty: 0.06,
            current_qty: 0.06,
            current_pct_of_original: 100.0,
            effective_leverage: Some(20),
            effective_entry_price: Some(1965.5),
            effective_take_profit: Some(1974.0),
            effective_stop_loss: Some(1961.2),
            reduction_history: vec![],
            times_reduced_at_current_level: 0,
            last_management_action: Some("MODIFY_MAKER".to_string()),
            last_management_reason: Some("same strategy, better maker price".to_string()),
            entry_context: Some(EntryContextForLlm {
                entry_strategy: Some("Dual-Market AVWAP Z-Score".to_string()),
                stop_model: Some("Sweep & Flip Stop".to_string()),
                entry_mode: Some("limit_below_zone".to_string()),
                original_tp: Some(1974.0),
                original_sl: Some(1961.2),
                sweep_wick_extreme: None,
                horizon: Some("4h".to_string()),
                entry_reason: "rvwap extreme, keep pending long".to_string(),
            }),
        };

        let snapshot = build_management_snapshot_for_llm(
            Some(&state),
            Some("same strategy, better maker price".to_string()),
            Some(position_context),
        )
        .expect("snapshot exists");

        let pending = snapshot.pending_order.expect("pending order exists");
        assert_eq!(pending.direction, "LONG");
        assert_eq!(pending.entry_price, Some(1965.5));
        assert_eq!(pending.current_tp_price, Some(1974.0));
        assert_eq!(pending.current_sl_price, Some(1961.2));
        assert_eq!(pending.leverage, Some(20));
        assert!(snapshot.positions.is_empty());
        assert!(snapshot.position_context.is_some());
    }

    #[test]
    fn execution_context_keys_for_report_include_directional_alias_for_both() {
        let keys = execution_context_keys_for_report("ETHUSDT", "BOTH", TradeDecision::Long);
        assert_eq!(
            keys,
            vec!["ETHUSDT:BOTH".to_string(), "ETHUSDT:LONG".to_string()]
        );
    }

    #[test]
    fn resolve_invoke_management_reason_drops_stale_global_reason_when_flat() {
        let resolved =
            resolve_invoke_management_reason(0, 0, Some("stale old reason".to_string()), None);
        assert!(resolved.is_none());
    }

    #[test]
    fn resolve_invoke_management_reason_prefers_position_context_reason() {
        let position_context = PositionContextForLlm {
            original_qty: 0.11,
            current_qty: 0.11,
            current_pct_of_original: 100.0,
            effective_leverage: Some(42),
            effective_entry_price: Some(2122.33),
            effective_take_profit: Some(2148.0),
            effective_stop_loss: Some(2109.63),
            reduction_history: vec![],
            times_reduced_at_current_level: 0,
            last_management_action: Some("HOLD".to_string()),
            last_management_reason: Some("current context reason".to_string()),
            entry_context: None,
        };
        let resolved = resolve_invoke_management_reason(
            0,
            3,
            Some("stale old reason".to_string()),
            Some(&position_context),
        );
        assert_eq!(resolved.as_deref(), Some("current context reason"));
    }

    #[test]
    fn build_entry_context_from_fallbacks_uses_stage_trace_and_helper_v() {
        let parsed_decision = json!({
            "decision": "LONG",
            "params": {
                "tp": 2148.0,
                "sl": 2106.91
            }
        });
        let stage_trace = vec![
            json!({
                "stage": "scan",
                "parsed_scan": {
                    "scan": {
                        "primary_strategy": "Absorption Re-test Continuation",
                        "entry_style": "patient_retest",
                        "stop_model_hint": "Value Area Invalidation Stop"
                    }
                }
            }),
            json!({
                "stage": "finalize",
                "primary_strategy": "Absorption Re-test Continuation",
                "entry_style": "patient_retest",
                "stop_model_hint": "Value Area Invalidation Stop"
            }),
        ];
        let input = sample_model_input(sample_indicators_with_known_v());

        let captured = build_entry_context_from_fallbacks(
            &parsed_decision,
            Some(&stage_trace),
            &input,
            Some("4h"),
            "fallback reason",
        );

        assert_eq!(
            captured.entry_strategy.as_deref(),
            Some("Absorption Re-test Continuation")
        );
        assert_eq!(captured.entry_mode.as_deref(), Some("patient_retest"));
        assert_eq!(
            captured.stop_model.as_deref(),
            Some("Value Area Invalidation Stop")
        );
        assert_eq!(captured.original_tp, Some(2148.0));
        assert_eq!(captured.original_sl, Some(2106.91));
        assert!(captured
            .entry_v
            .map(|value| (value - 18.73).abs() < 1e-9)
            .unwrap_or(false));
        assert_eq!(captured.horizon.as_deref(), Some("4h"));
    }

    #[test]
    fn resolve_entry_v_from_sources_uses_entry_style_to_pick_1d_v() {
        let stage_trace = vec![json!({
            "stage": "scan",
            "parsed_scan": {
                "scan": {
                    "entry_style": "trend_continuation"
                }
            }
        })];
        let input = sample_model_input(sample_indicators_with_known_v());

        let entry_v = resolve_entry_v_from_sources(Some(&stage_trace), &input, Some("4h"));

        assert!(entry_v
            .map(|value| (value - 72.83).abs() < 1e-9)
            .unwrap_or(false));
    }

    #[test]
    fn build_entry_context_from_fallbacks_prefers_helper_v_over_model_output() {
        let parsed_decision = json!({
            "decision": "LONG",
            "analysis": {
                "entry_strategy": "Model Strategy",
                "stop_model": "Model Stop",
                "volatility_unit_v": 55.5
            },
            "params": {
                "entry_mode": "market_after_flip",
                "tp": 2148.0,
                "sl": 2106.91
            }
        });
        let stage_trace = vec![json!({
            "stage": "finalize",
            "primary_strategy": "Fallback Strategy",
            "entry_style": "patient_retest",
            "stop_model_hint": "Fallback Stop"
        })];
        let input = sample_model_input(sample_indicators_with_known_v());

        let captured = build_entry_context_from_fallbacks(
            &parsed_decision,
            Some(&stage_trace),
            &input,
            Some("4h"),
            "model reason",
        );

        assert_eq!(captured.entry_strategy.as_deref(), Some("Model Strategy"));
        assert_eq!(captured.stop_model.as_deref(), Some("Model Stop"));
        assert_eq!(captured.entry_mode.as_deref(), Some("market_after_flip"));
        assert!(captured
            .entry_v
            .map(|value| (value - 18.73).abs() < 1e-9)
            .unwrap_or(false));
    }

    #[test]
    fn evaluate_trade_entry_v_gate_blocks_when_distances_are_below_threshold() {
        let mut execution_config = LlmExecutionConfig::default();
        execution_config.min_distance_v = 1.0;
        let intent = TradeIntent {
            decision: TradeDecision::Long,
            entry_price: Some(2000.0),
            take_profit: Some(2015.0),
            stop_loss: Some(1990.0),
            leverage: Some(5.0),
            risk_reward_ratio: Some(1.5),
            horizon: Some("4h".to_string()),
            swing_logic: Some("test".to_string()),
            reason: "test".to_string(),
        };
        let stage_trace = vec![json!({
            "stage": "scan",
            "parsed_scan": {
                "scan": {
                    "entry_style": "patient_retest"
                }
            }
        })];
        let input = sample_model_input(sample_indicators_with_known_v());

        let gate =
            evaluate_trade_entry_v_gate(&execution_config, &intent, &input, Some(&stage_trace))
                .expect("gate should evaluate")
                .expect("gate should apply");

        assert!(!gate.passed);
        assert!((gate.take_profit_distance_v - (15.0 / 18.73)).abs() < 1e-9);
        assert!((gate.min_distance_v - 1.0).abs() < 1e-9);
    }

    #[test]
    fn evaluate_trade_entry_v_gate_passes_with_helper_based_v() {
        let mut execution_config = LlmExecutionConfig::default();
        execution_config.min_distance_v = 1.0;
        let intent = TradeIntent {
            decision: TradeDecision::Long,
            entry_price: Some(2000.0),
            take_profit: Some(2025.0),
            stop_loss: Some(1990.0),
            leverage: Some(5.0),
            risk_reward_ratio: Some(2.5),
            horizon: Some("4h".to_string()),
            swing_logic: Some("test".to_string()),
            reason: "test".to_string(),
        };
        let stage_trace = vec![json!({
            "stage": "scan",
            "parsed_scan": {
                "scan": {
                    "entry_style": "patient_retest"
                }
            }
        })];
        let input = sample_model_input(sample_indicators_with_known_v());

        let gate =
            evaluate_trade_entry_v_gate(&execution_config, &intent, &input, Some(&stage_trace))
                .expect("gate should evaluate")
                .expect("gate should be enabled");

        assert!(gate.passed);
        assert!((gate.resolved_v.value - 18.73).abs() < 1e-9);
        assert_eq!(gate.resolved_v.timeframe, "4h");
        assert!(gate.take_profit_distance_v > 1.0);
    }

    #[test]
    fn evaluate_trade_rr_gate_blocks_when_rr_is_below_threshold() {
        let mut execution_config = LlmExecutionConfig::default();
        execution_config.min_rr = 2.0;
        let intent = TradeIntent {
            decision: TradeDecision::Long,
            entry_price: Some(2000.0),
            take_profit: Some(2015.0),
            stop_loss: Some(1990.0),
            leverage: Some(5.0),
            risk_reward_ratio: Some(1.5),
            horizon: Some("4h".to_string()),
            swing_logic: Some("test".to_string()),
            reason: "test".to_string(),
        };

        let gate = evaluate_trade_rr_gate(&execution_config, &intent)
            .expect("rr gate should evaluate")
            .expect("rr gate should apply");

        assert!(!gate.passed);
        assert!((gate.risk_reward_ratio - 1.5).abs() < 1e-9);
        assert!((gate.min_rr - 2.0).abs() < 1e-9);
    }

    #[test]
    fn evaluate_trade_rr_gate_passes_when_rr_meets_threshold() {
        let mut execution_config = LlmExecutionConfig::default();
        execution_config.min_rr = 2.0;
        let intent = TradeIntent {
            decision: TradeDecision::Long,
            entry_price: Some(2000.0),
            take_profit: Some(2020.0),
            stop_loss: Some(1990.0),
            leverage: Some(5.0),
            risk_reward_ratio: Some(2.0),
            horizon: Some("4h".to_string()),
            swing_logic: Some("test".to_string()),
            reason: "test".to_string(),
        };

        let gate = evaluate_trade_rr_gate(&execution_config, &intent)
            .expect("rr gate should evaluate")
            .expect("rr gate should apply");

        assert!(gate.passed);
        assert!((gate.risk_reward_ratio - 2.0).abs() < 1e-9);
        assert!((gate.reward_distance - 20.0).abs() < 1e-9);
        assert!((gate.risk_distance - 10.0).abs() < 1e-9);
    }

    #[test]
    fn build_invocation_input_preserves_raw_indicators_without_injection() {
        let bundle = LatestBundle {
            raw: MinuteBundleEnvelope {
                msg_type: "bundle".to_string(),
                routing_key: "test.route".to_string(),
                symbol: "ETHUSDT".to_string(),
                ts_bucket: Utc::now(),
                window_code: "1m".to_string(),
                indicator_count: 1,
                published_at: None,
                indicators: json!({}),
            },
            indicators: json!({
                "kline_history": {
                    "payload": {
                        "intervals": {
                            "4h": {
                                "futures": {
                                    "bars": [
                                        {"open_time": "2026-03-13T08:00:00Z", "high": 110.0, "low": 100.0, "is_closed": true},
                                        {"open_time": "2026-03-13T04:00:00Z", "high": 108.0, "low": 100.0, "is_closed": true},
                                        {"open_time": "2026-03-13T00:00:00Z", "high": 112.0, "low": 101.0, "is_closed": true}
                                    ]
                                }
                            },
                            "1d": {
                                "futures": {
                                    "bars": [
                                        {"open_time": "2026-03-12T00:00:00Z", "high": 140.0, "low": 100.0, "is_closed": true},
                                        {"open_time": "2026-03-11T00:00:00Z", "high": 138.0, "low": 102.0, "is_closed": true},
                                        {"open_time": "2026-03-10T00:00:00Z", "high": 142.0, "low": 101.0, "is_closed": true}
                                    ]
                                }
                            }
                        }
                    }
                }
            }),
            missing_indicator_codes: vec![],
            received_at: Utc::now(),
        };

        let input = build_invocation_input(&bundle, false, false, None, None, None);

        assert!(input.indicators.get("pre_computed_v").is_none());
        assert_eq!(input.indicators, bundle.indicators);
    }

    #[test]
    fn build_persist_only_input_preserves_full_raw_indicator_bundle() {
        let bundle = LatestBundle {
            raw: MinuteBundleEnvelope {
                msg_type: "bundle".to_string(),
                routing_key: "test.route".to_string(),
                symbol: "ETHUSDT".to_string(),
                ts_bucket: Utc::now(),
                window_code: "1m".to_string(),
                indicator_count: 3,
                published_at: None,
                indicators: json!({}),
            },
            indicators: json!({
                "absorption": {"payload": {"recent_7d": {"events": []}}},
                "fvg": {"payload": {"by_window": {"15m": {}, "4h": {}}}},
                "cvd_pack": {"payload": {"by_window": {"15m": {"series": []}}}}
            }),
            missing_indicator_codes: vec![],
            received_at: Utc::now(),
        };

        let input = build_persist_only_input(&bundle);

        let mut keys = input
            .indicators
            .as_object()
            .expect("object")
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        keys.sort();
        assert_eq!(
            keys,
            vec![
                "absorption".to_string(),
                "cvd_pack".to_string(),
                "fvg".to_string()
            ]
        );
        assert_eq!(
            input
                .indicators
                .pointer("/fvg/payload/by_window/15m")
                .map(|_| true),
            Some(true)
        );
        assert!(input.missing_indicator_codes.is_empty());
        assert!(!input.management_mode);
        assert!(!input.pending_order_mode);
        assert!(input.trading_state.is_none());
        assert!(input.management_snapshot.is_none());
    }

    #[test]
    fn sync_snapshot_backfills_pending_entry_context_from_both_alias() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build tokio runtime");
        runtime.block_on(async {
            let state = TradingStateSnapshot {
                symbol: "ETHUSDT".to_string(),
                has_active_context: false,
                has_active_positions: false,
                has_open_orders: true,
                active_positions: vec![],
                open_orders: vec![OpenOrderSnapshot {
                    order_id: 1,
                    side: "BUY".to_string(),
                    position_side: "BOTH".to_string(),
                    order_type: "LIMIT".to_string(),
                    status: "NEW".to_string(),
                    orig_qty: 0.05,
                    executed_qty: 0.0,
                    price: 2109.94,
                    stop_price: 0.0,
                    close_position: false,
                    reduce_only: false,
                }],
                total_wallet_balance: 1000.0,
                available_balance: 800.0,
            };

            let runtime_lifecycle_state = Arc::new(Mutex::new(RuntimeLifecycleStore::default()));
            {
                let mut guard = runtime_lifecycle_state.lock().await;
                let symbol_state = guard.symbol_state_mut("ETHUSDT");
                symbol_state.last_management_reason = Some("keep pending".to_string());
                symbol_state.contexts.insert(
                    "ETHUSDT:BOTH".to_string(),
                    PositionContextState {
                        original_qty: 0.05,
                        last_management_action: None,
                        last_management_reason: Some("keep pending".to_string()),
                        reduction_history: vec![],
                        effective_entry_price: Some(2109.94),
                        effective_stop_loss: Some(2098.55),
                        effective_take_profit: Some(2148.0),
                        effective_leverage: Some(42),
                        entry_context: Some(EntryContextForState {
                            entry_strategy: Some("Break-Retest".to_string()),
                            stop_model: None,
                            entry_mode: Some("patient_retest".to_string()),
                            original_tp: Some(2148.0),
                            original_sl: Some(2106.91),
                            sweep_wick_extreme: None,
                            horizon: Some("4h".to_string()),
                            entry_reason: "original entry contract".to_string(),
                            entry_v: Some(39.13),
                        }),
                    },
                );
            }

            let snapshot =
                sync_and_build_position_context_snapshot(Some(&state), &runtime_lifecycle_state)
                    .await
                    .expect("position context snapshot");

            let entry_context = snapshot.entry_context.expect("backfilled entry_context");
            assert_eq!(
                entry_context.entry_strategy.as_deref(),
                Some("Break-Retest")
            );
            assert_eq!(entry_context.horizon.as_deref(), Some("4h"));
            assert_eq!(snapshot.effective_entry_price, Some(2109.94));
            assert_eq!(snapshot.effective_take_profit, Some(2148.0));
            assert_eq!(snapshot.effective_stop_loss, Some(2098.55));
            assert_eq!(snapshot.effective_leverage, Some(42));
        });
    }

    #[test]
    fn prune_temp_indicator_dir_removes_files_older_than_configured_minutes() {
        let dir = std::env::temp_dir().join(format!("llm-temp-indicator-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).expect("create temp indicator dir");
        fs::write(dir.join(".gitignore"), "").expect("write .gitignore");
        fs::write(dir.join("20260307T105900Z_ETHUSDT.json"), "{}").expect("write old file");
        fs::write(dir.join("20260307T110000Z_ETHUSDT.json"), "{}").expect("write edge file");
        fs::write(dir.join("20260307T111500Z_ETHUSDT.json"), "{}").expect("write fresh file");
        fs::write(dir.join("not_a_bundle.json"), "{}").expect("write invalid file");

        let removed = prune_expired_temp_indicator_files(
            &dir,
            DateTime::parse_from_rfc3339("2026-03-07T11:30:00Z")
                .expect("parse current ts")
                .with_timezone(&Utc),
            30,
        )
        .expect("prune temp indicator dir");

        assert_eq!(removed, 1);
        assert!(!dir.join("20260307T105900Z_ETHUSDT.json").exists());
        assert!(dir.join("20260307T110000Z_ETHUSDT.json").exists());
        assert!(dir.join("20260307T111500Z_ETHUSDT.json").exists());
        assert!(dir.join("not_a_bundle.json").exists());
        assert!(dir.join(".gitignore").exists());

        fs::remove_dir_all(&dir).expect("cleanup temp indicator dir");
    }

    #[test]
    fn prune_temp_model_input_dir_removes_files_older_than_configured_minutes() {
        let dir =
            std::env::temp_dir().join(format!("llm-temp-model-input-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).expect("create temp model input dir");
        fs::write(dir.join(".gitignore"), "").expect("write .gitignore");
        fs::write(
            dir.join("20260307T105900Z_ETHUSDT_entry_20260307T110001000Z.json"),
            "{}",
        )
        .expect("write old file");
        fs::write(
            dir.join("20260307T110000Z_ETHUSDT_management_20260307T110101000Z.json"),
            "{}",
        )
        .expect("write edge file");
        fs::write(
            dir.join("20260307T111500Z_ETHUSDT_pending_management_20260307T111601000Z.json"),
            "{}",
        )
        .expect("write fresh file");
        fs::write(dir.join("not_a_model_input.json"), "{}").expect("write invalid file");

        let removed = prune_expired_temp_model_input_files(
            &dir,
            DateTime::parse_from_rfc3339("2026-03-07T11:30:00Z")
                .expect("parse current ts")
                .with_timezone(&Utc),
            30,
        )
        .expect("prune temp model input dir");

        assert_eq!(removed, 1);
        assert!(!dir
            .join("20260307T105900Z_ETHUSDT_entry_20260307T110001000Z.json")
            .exists());
        assert!(dir
            .join("20260307T110000Z_ETHUSDT_management_20260307T110101000Z.json")
            .exists());
        assert!(dir
            .join("20260307T111500Z_ETHUSDT_pending_management_20260307T111601000Z.json")
            .exists());
        assert!(dir.join("not_a_model_input.json").exists());
        assert!(dir.join(".gitignore").exists());

        fs::remove_dir_all(&dir).expect("cleanup temp model input dir");
    }
}
