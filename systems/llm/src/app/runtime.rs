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
    PendingOrderManagementDecision, PositionManagementDecision, TradeDecision,
};
use crate::llm::provider::{
    invoke_models, serialize_llm_input_minified, EntryContextForLlm, ManagementSnapshotForLlm,
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
use serde_json::{json, Map, Value};
use std::collections::HashMap;
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
const LLM_JOURNAL_DIR: &str = "systems/llm/journal";
const LLM_JOURNAL_FILE: &str = "systems/llm/journal/llm_trade_journal.jsonl";
const LLM_ALLOWED_WINDOWS: &[&str] = &["15m", "1h", "4h", "1d"];
const LLM_FVG_ALLOWED_WINDOWS: &[&str] = &["1h", "4h", "1d"];
const GEMINI_MAX_INPUT_TOKENS: usize = 1_048_576;
const APPROX_BYTES_PER_TOKEN: usize = 4;
const LLM_INPUT_INDICATORS_BUDGET_BYTES: usize = GEMINI_MAX_INPUT_TOKENS * APPROX_BYTES_PER_TOKEN;
const LLM_ORDERBOOK_TOP_LEVELS: usize = 12;
const LLM_ORDERBOOK_SIDE_LEVELS: usize = 8;
const LLM_PROFILE_TOP_LEVELS: usize = 16;
const LLM_VALUE_AREA_TOP_LEVELS: usize = 12;
const LLM_FOOTPRINT_TOP_LEVELS: usize = 16;
const LLM_PROFILE_TOP_LEVELS_15M: usize = 6;
const LLM_PROFILE_TOP_LEVELS_1D: usize = 8;
const LLM_PROFILE_TOP_LEVELS_4H: usize = 24;
const LLM_VALUE_AREA_TOP_LEVELS_15M: usize = 6;
const LLM_VALUE_AREA_TOP_LEVELS_1D: usize = 8;
const LLM_VALUE_AREA_TOP_LEVELS_4H: usize = 20;
const LLM_FOOTPRINT_TOP_LEVELS_15M: usize = 8;
const LLM_FOOTPRINT_TOP_LEVELS_1D: usize = 12;
const LLM_FOOTPRINT_TOP_LEVELS_4H: usize = 16;
const LLM_FOOTPRINT_ZONE_STRENGTH_TOP_15M: usize = 12;
const LLM_FOOTPRINT_ZONE_STRENGTH_TOP_1D: usize = 24;
const LLM_FOOTPRINT_ZONE_STRENGTH_TOP_4H: usize = 120;
const LLM_FOOTPRINT_ZONE_NEAR_TOP_15M: usize = 24;
const LLM_FOOTPRINT_ZONE_NEAR_TOP_4H: usize = 120;
const LLM_FOOTPRINT_STACK_TOP_15M: usize = 12;
const LLM_FOOTPRINT_STACK_TOP_1D: usize = 16;
const LLM_FOOTPRINT_STACK_TOP_4H: usize = 40;
const LLM_LEVELS_NEAR_PRICE_PCT_4H: f64 = 0.003;
const LLM_PRICE_BAND_MIN_SIZE: f64 = 0.25;
const LLM_PRICE_BAND_RATIO: f64 = 0.00025;
const LLM_PRICE_BAND_MAX_BANDS: usize = 120;
const LLM_TIME_BUCKET_SPAN_SECS: i64 = 900;
const LLM_TIME_BUCKET_RECENT_KEEP: usize = 8;
const LLM_CVD_KEEP_15M: usize = 96;
const LLM_CORE_ANCHOR_TOP_N: usize = 8;
const LLM_KLINE_KEEP_1M: usize = 120;
const LLM_KLINE_KEEP_15M: usize = 96;
const LLM_KLINE_KEEP_4H: usize = 40;
const LLM_KLINE_KEEP_1D: usize = 30;
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
    volatility_unit_v: Option<f64>,
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
    /// V captured at entry time; passed to management model to ensure
    /// consistent V-based threshold calculations throughout the trade.
    entry_v: Option<f64>,
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
    filtered_indicators: Value,
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
    let last_management_reason = Arc::new(Mutex::new(None::<String>));
    let position_context_state =
        Arc::new(Mutex::new(HashMap::<String, PositionContextState>::new()));
    restore_last_management_reason_from_journal(&last_management_reason).await?;
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
                    &last_management_reason,
                    &position_context_state,
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

                                let (filtered, missing) = filter_indicators(
                                    &bundle.indicators,
                                    &ctx.config.llm.indicator_codes,
                                );

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
                                    filtered_indicators: Value::Object(filtered),
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
        indicators: bundle.filtered_indicators.clone(),
        missing_indicator_codes: bundle.missing_indicator_codes.clone(),
        management_mode,
        pending_order_mode,
        trading_state,
        management_snapshot,
    }
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
    context_state: &Arc<Mutex<HashMap<String, PositionContextState>>>,
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
    let guard = context_state.lock().await;
    let ctx = guard.get(&key);
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
    let remapped_entry = entry + (stop_loss - entry) * pct;
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

async fn enrich_telegram_fields_from_entry_context(
    mut fields: TelegramPositionFields,
    trading_state: Option<&crate::execution::binance::TradingStateSnapshot>,
    context_state: &Arc<Mutex<HashMap<String, PositionContextState>>>,
) -> TelegramPositionFields {
    let Some(state) = trading_state else {
        return fields;
    };
    let Some((key, _, _)) = primary_position_key(state) else {
        return fields;
    };
    let guard = context_state.lock().await;
    let Some(ctx) = guard.get(&key) else {
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
    context_state: &Arc<Mutex<HashMap<String, PositionContextState>>>,
    last_management_reason: Option<String>,
) -> Option<PositionContextForLlm> {
    let state = trading_state?;
    let (key, current_qty, current_price) =
        primary_position_key(state).or_else(|| primary_pending_order_key(state))?;
    if current_qty <= f64::EPSILON {
        return None;
    }
    let mut guard = context_state.lock().await;
    let entry = guard.entry(key).or_default();
    if entry.original_qty <= f64::EPSILON {
        entry.original_qty = current_qty;
    } else if current_qty > entry.original_qty {
        entry.original_qty = current_qty;
    }
    if entry.last_management_reason.is_none() {
        entry.last_management_reason = last_management_reason;
    }
    let current_pct = if entry.original_qty > f64::EPSILON {
        current_qty / entry.original_qty * 100.0
    } else {
        100.0
    };
    let base_v = entry
        .reduction_history
        .iter()
        .rev()
        .find_map(|h| h.volatility_unit_v)
        .unwrap_or(0.0);
    let level_threshold = if base_v > 0.0 { base_v * 0.5 } else { 0.5 };
    let times_reduced_at_current_level = entry
        .reduction_history
        .iter()
        .filter(|h| (h.price - current_price).abs() <= level_threshold)
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
            entry_v: ec.entry_v,
        }),
    })
}

async fn hydrate_position_context_from_live_state(
    http_client: &Client,
    api_config: &crate::app::config::BinanceApiConfig,
    exec_config: &crate::app::config::LlmExecutionConfig,
    trading_state: Option<&crate::execution::binance::TradingStateSnapshot>,
    context_state: &Arc<Mutex<HashMap<String, PositionContextState>>>,
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

    let mut guard = context_state.lock().await;

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
        let entry = guard.entry(key).or_default();
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
            let entry = guard.entry(key).or_default();
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
    context_state: &Arc<Mutex<HashMap<String, PositionContextState>>>,
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
    let mut guard = context_state.lock().await;
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
        match event_type {
            "llm_order_execution" => {
                let Some(key) = active_key.as_ref() else {
                    continue;
                };
                let entry = guard.entry(key.clone()).or_default();
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
            }
            "llm_management_execution" | "llm_management_execution_error" => {
                let Some(key) = active_key.as_ref() else {
                    continue;
                };
                let entry = guard.entry(key.clone()).or_default();
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
                let entry = guard.entry(key.clone()).or_default();
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

async fn restore_last_management_reason_from_journal(
    last_management_reason: &Arc<Mutex<Option<String>>>,
) -> Result<()> {
    let path = Path::new(LLM_JOURNAL_FILE);
    if !path.exists() {
        return Ok(());
    }
    let mut restored: Option<String> = None;
    let contents = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
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
        let reason = value
            .get("reason")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        if reason.is_some() {
            restored = reason;
        }
    }
    if restored.is_some() {
        let mut guard = last_management_reason.lock().await;
        *guard = restored;
    }
    Ok(())
}

async fn validate_reduce_anti_repetition(
    symbol: &str,
    intent: &crate::llm::decision::PositionManagementIntent,
    parsed_decision: &Value,
    trading_state: Option<&crate::execution::binance::TradingStateSnapshot>,
    context_state: &Arc<Mutex<HashMap<String, PositionContextState>>>,
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
    let guard = context_state.lock().await;
    let ctx = guard.get(&key);
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
                let v = extract_nested_f64(parsed_decision, &["analysis", "volatility_unit_v"])
                    .or(last.volatility_unit_v)
                    .unwrap_or(0.0);
                let threshold = if v > 0.0 { 0.5 * v } else { 0.5 };
                if (current_price - last.price).abs() <= threshold {
                    return Err(anyhow::anyhow!(
                        "HC-10 violation: repeated REDUCE near same level symbol={} current_price={} last_reduce_price={} threshold={}",
                        symbol,
                        current_price,
                        last.price,
                        threshold
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
    parsed_decision: &Value,
    context_state: &Arc<Mutex<HashMap<String, PositionContextState>>>,
) {
    let Some(state) = trading_state else {
        return;
    };
    let Some((key, current_qty, current_price)) = primary_position_key(state) else {
        return;
    };
    let mut guard = context_state.lock().await;
    if matches!(
        intent.decision,
        crate::llm::decision::PositionManagementDecision::Close
    ) {
        guard.remove(&key);
        return;
    }
    let entry = guard.entry(key).or_default();
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
                volatility_unit_v: extract_nested_f64(
                    parsed_decision,
                    &["analysis", "volatility_unit_v"],
                ),
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
    context_state: &Arc<Mutex<HashMap<String, PositionContextState>>>,
) {
    let Some(state) = trading_state else {
        return;
    };
    let Some((key, current_qty, _)) = primary_pending_order_key(state) else {
        if matches!(
            intent.decision,
            crate::llm::decision::PendingOrderManagementDecision::Close
        ) {
            let mut guard = context_state.lock().await;
            let prefix = format!("{}:", symbol.to_ascii_uppercase());
            guard.retain(|existing_key, _| !existing_key.starts_with(&prefix));
        }
        return;
    };

    let mut guard = context_state.lock().await;
    if matches!(
        intent.decision,
        crate::llm::decision::PendingOrderManagementDecision::Close
    ) {
        guard.remove(&key);
        return;
    }

    let entry = guard.entry(key).or_default();
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
}

fn summarize_reason(reason: &str) -> String {
    let compact = reason.replace('\n', " ").trim().to_string();
    if compact.len() <= 80 {
        return compact;
    }
    compact.chars().take(80).collect()
}

fn queue_latest_bundle_invoke(
    ctx: &AppContext,
    latest_bundle: &Option<LatestBundle>,
    last_invoked_ts_bucket: &mut Option<DateTime<Utc>>,
    invoke_inflight: &Arc<AtomicBool>,
    invoke_throttle: &Arc<Mutex<InvokeThrottleState>>,
    last_management_reason: &Arc<Mutex<Option<String>>>,
    position_context_state: &Arc<Mutex<HashMap<String, PositionContextState>>>,
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
    let http_client = ctx.http_client.clone();
    let print_response = ctx.config.llm.print_response;
    let invoke_inflight = Arc::clone(invoke_inflight);
    let invoke_throttle = Arc::clone(invoke_throttle);
    let last_management_reason = Arc::clone(last_management_reason);
    let position_context_state = Arc::clone(position_context_state);
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
                http_client,
                print_response,
                bundle,
                trigger,
                last_management_reason,
                position_context_state,
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
    http_client: Client,
    print_response: bool,
    bundle: LatestBundle,
    trigger: Arc<str>,
    last_management_reason: Arc<Mutex<Option<String>>>,
    position_context_state: Arc<Mutex<HashMap<String, PositionContextState>>>,
) {
    let mut execution_done = false;
    let mut execution_blocked_due_to_stale = false;
    let trading_state = if config.llm.execution.enabled {
        match fetch_symbol_trading_state(
            &http_client,
            &config.api.binance,
            &config.llm.execution,
            &bundle.raw.symbol,
        )
        .await
        {
            Ok(state) => Some(state),
            Err(err) => {
                let error_chain = format!("{err:#}");
                error!(
                    symbol = %bundle.raw.symbol,
                    error = %error_chain,
                    "llm invoke aborted: failed to fetch trading state"
                );
                return;
            }
        }
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
    let open_orders_only_mode = active_position_count == 0 && open_order_count > 0;
    let pending_order_mode = open_orders_only_mode;
    let context_state = trading_state
        .as_ref()
        .map(
            |state| match (state.has_active_positions, state.has_open_orders) {
                (true, true) => "POSITION_AND_ORDERS",
                (true, false) => "POSITION_ACTIVE",
                (false, true) => "OPEN_ORDERS_ONLY",
                (false, false) => "NO_ACTIVE_CONTEXT",
            },
        )
        .unwrap_or("NO_ACTIVE_CONTEXT");
    let previous_management_reason = {
        let guard = last_management_reason.lock().await;
        guard.clone()
    };
    hydrate_position_context_from_live_state(
        &http_client,
        &config.api.binance,
        &config.llm.execution,
        trading_state.as_ref(),
        &position_context_state,
    )
    .await;
    if let Err(err) = restore_position_context_from_journal_for_live_state(
        trading_state.as_ref(),
        &position_context_state,
    )
    .await
    {
        warn!(
            symbol = %bundle.raw.symbol,
            error = %err,
            "restore position context from journal failed"
        );
    }
    let position_context = sync_and_build_position_context_snapshot(
        trading_state.as_ref(),
        &position_context_state,
        previous_management_reason.clone(),
    )
    .await;
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
        previous_management_reason.as_deref().unwrap_or("-"),
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
            "last_management_reason": previous_management_reason.clone(),
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
        previous_management_reason,
        position_context,
    );

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
    let model_input_file = match persist_model_input_to_disk(
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
    let model_input_path = model_input_file
        .as_ref()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| "-".to_string());
    let model_input_exists = model_input_file
        .as_ref()
        .map(|p| p.exists())
        .unwrap_or(false);
    println!(
        "LLM_INPUT_SOURCE ts_bucket={} trigger={} symbol={} source_temp_indicator_file={} source_file_exists={} source_model_input_file={} model_input_file_exists={} indicator_count={} missing_count={} management_mode={} pending_order_mode={}",
        bundle.raw.ts_bucket,
        &*trigger,
        bundle.raw.symbol,
        source_file.display(),
        source_file.exists(),
        model_input_path,
        model_input_exists,
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
            let intent = match pending_order_management_intent_from_value_with_context(
                parsed_decision,
                open_order_count > 0,
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
                    let mut guard = last_management_reason.lock().await;
                    if matches!(intent.decision, PendingOrderManagementDecision::Close) {
                        *guard = None;
                    } else {
                        *guard = Some(intent.reason.clone());
                    }
                }
                execution_done = true;
                let fields = derive_pending_order_telegram_fields(
                    trading_state.as_ref(),
                    &position_context_state,
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
                    &position_context_state,
                )
                .await;
                continue;
            }
            if execution_blocked_due_to_stale {
                execution_done = true;
                let fields = with_telegram_field_overrides(
                    derive_pending_order_telegram_fields(
                        trading_state.as_ref(),
                        &position_context_state,
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
                let mut guard = last_management_reason.lock().await;
                if matches!(intent.decision, PendingOrderManagementDecision::Close) {
                    *guard = None;
                } else {
                    *guard = Some(intent.reason.clone());
                }
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
                        &position_context_state,
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
                parsed_decision,
                trading_state.as_ref(),
                &position_context_state,
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
                    let mut guard = last_management_reason.lock().await;
                    if matches!(intent.decision, PositionManagementDecision::Close) {
                        // Reset on close — don't carry this reason into the next trade's management context.
                        *guard = None;
                    } else {
                        *guard = Some(intent.reason.clone());
                    }
                }
                execution_done = true;
                let fallback_fields = enrich_telegram_fields_from_entry_context(
                    derive_management_telegram_fields(trading_state.as_ref()),
                    trading_state.as_ref(),
                    &position_context_state,
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
                    parsed_decision,
                    &position_context_state,
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
                        &position_context_state,
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
                let mut guard = last_management_reason.lock().await;
                if matches!(intent.decision, PositionManagementDecision::Close) {
                    // Reset on close — don't carry this reason into the next trade's management context.
                    *guard = None;
                } else {
                    *guard = Some(intent.reason.clone());
                }
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
                            &position_context_state,
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
                        parsed_decision,
                        &position_context_state,
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
                            &position_context_state,
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
                    format_metric_number(intent.entry_price),
                    format_metric_number(intent.leverage),
                    format_metric_number(intent.risk_reward_ratio),
                    format_metric_number(intent.take_profit),
                    format_metric_number(intent.stop_loss),
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
                    "entry_price": intent.entry_price,
                    "leverage": intent.leverage,
                    "risk_reward_ratio": intent.risk_reward_ratio,
                    "take_profit": intent.take_profit,
                    "stop_loss": intent.stop_loss,
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
                        entry_price: intent.entry_price,
                        leverage: intent.leverage,
                        risk_reward_ratio: intent.risk_reward_ratio,
                        take_profit: intent.take_profit,
                        stop_loss: intent.stop_loss,
                        reason: &intent.reason,
                    },
                )
                .await;
                continue;
            }

            let mut execution_intent = intent.clone();
            if let Some((remapped_entry, remapped_stop_loss)) =
                remap_trade_entry_and_stop_loss(&out.provider, &config.llm.execution, &intent)
            {
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
                    // Capture entry context so management cycles can continue the strategy.
                    let position_side = report.position_side.to_ascii_uppercase();
                    if !position_side.is_empty() && position_side != "-" {
                        let key = format!(
                            "{}:{}",
                            bundle.raw.symbol.to_ascii_uppercase(),
                            position_side
                        );
                        let mut guard = position_context_state.lock().await;
                        let ctx_entry = guard.entry(key).or_default();
                        ctx_entry.effective_entry_price = Some(report.maker_entry_price);
                        ctx_entry.effective_stop_loss = Some(report.actual_stop_loss);
                        ctx_entry.effective_take_profit = Some(report.actual_take_profit);
                        ctx_entry.effective_leverage = Some(report.leverage);
                        ctx_entry.entry_context = Some(EntryContextForState {
                            entry_strategy: extract_nested_str(
                                parsed_decision,
                                &["analysis", "entry_strategy"],
                            ),
                            stop_model: extract_nested_str(
                                parsed_decision,
                                &["analysis", "stop_model"],
                            ),
                            entry_mode: extract_nested_str(
                                parsed_decision,
                                &["params", "entry_mode"],
                            ),
                            original_tp: extract_nested_f64(parsed_decision, &["params", "tp"]),
                            original_sl: extract_nested_f64(parsed_decision, &["params", "sl"]),
                            sweep_wick_extreme: extract_nested_f64(
                                parsed_decision,
                                &["params", "sweep_wick_extreme"],
                            ),
                            horizon: intent.horizon.clone(),
                            entry_reason: intent.reason.clone(),
                            entry_v: extract_nested_f64(
                                parsed_decision,
                                &["analysis", "volatility_unit_v"],
                            ),
                        });
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
                }
            }
        }
    }
}

fn filter_indicators(
    raw_indicators: &Value,
    required_codes: &[String],
) -> (Map<String, Value>, Vec<String>) {
    let source = raw_indicators.as_object().cloned().unwrap_or_default();
    let mut out = Map::new();
    let mut missing = Vec::new();

    for code in required_codes {
        if let Some(val) = source.get(code) {
            let mut filtered = val.clone();
            // FVG uses 1h/4h/1d windows (no 15m); all others use the standard 15m/4h/1d set.
            if code == "fvg" {
                retain_fvg_windows_in_place(&mut filtered);
            } else {
                retain_llm_windows_in_place(&mut filtered);
            }
            out.insert(code.clone(), filtered);
        } else {
            missing.push(code.clone());
        }
    }

    compact_llm_indicators_in_place(&mut out);
    (out, missing)
}

fn retain_llm_windows_in_place(value: &mut Value) {
    match value {
        Value::Object(obj) => {
            if let Some(by_window) = obj.get_mut("by_window").and_then(Value::as_object_mut) {
                by_window.retain(|window, _| LLM_ALLOWED_WINDOWS.contains(&window.as_str()));
            }
            if let Some(series_by_window) = obj
                .get_mut("series_by_window")
                .and_then(Value::as_object_mut)
            {
                series_by_window.retain(|window, _| LLM_ALLOWED_WINDOWS.contains(&window.as_str()));
            }
            for child in obj.values_mut() {
                retain_llm_windows_in_place(child);
            }
        }
        Value::Array(items) => {
            for item in items {
                retain_llm_windows_in_place(item);
            }
        }
        _ => {}
    }
}

/// FVG has windows 1h/4h/1d (no 15m timeframe produced by the engine).
fn retain_fvg_windows_in_place(value: &mut Value) {
    match value {
        Value::Object(obj) => {
            if let Some(by_window) = obj.get_mut("by_window").and_then(Value::as_object_mut) {
                by_window.retain(|window, _| LLM_FVG_ALLOWED_WINDOWS.contains(&window.as_str()));
            }
            for child in obj.values_mut() {
                retain_fvg_windows_in_place(child);
            }
        }
        Value::Array(items) => {
            for item in items {
                retain_fvg_windows_in_place(item);
            }
        }
        _ => {}
    }
}

fn compact_llm_indicators_in_place(indicators: &mut Map<String, Value>) {
    let reference_price = extract_reference_price(indicators);

    if let Some(payload) = indicator_payload_mut(indicators, "orderbook_depth") {
        compact_orderbook_depth_payload(payload);
    }
    if let Some(payload) = indicator_payload_mut(indicators, "kline_history") {
        compact_kline_history_payload(payload);
    }
    if let Some(payload) = indicator_payload_mut(indicators, "funding_rate") {
        compact_funding_rate_payload_with_time_buckets(payload);
    }
    if let Some(payload) = indicator_payload_mut(indicators, "cvd_pack") {
        compact_cvd_pack_payload_with_time_buckets(payload);
    }
    if let Some(payload) = indicator_payload_mut(indicators, "price_volume_structure") {
        compact_price_volume_structure_payload_with_price_bands(payload, reference_price);
    }

    let pvs_value_areas = extract_pvs_value_area_ranges(indicators);
    if let Some(payload) = indicator_payload_mut(indicators, "footprint") {
        compact_footprint_payload_with_price_bands(payload, reference_price, &pvs_value_areas);
    }
    if let Some(payload) = indicator_payload_mut(indicators, "ema_trend_regime") {
        compact_ema_trend_regime_payload(payload);
    }
    if let Some(payload) = indicator_payload_mut(indicators, "tpo_market_profile") {
        compact_tpo_market_profile_payload(payload);
    }
    if let Some(payload) = indicator_payload_mut(indicators, "rvwap_sigma_bands") {
        compact_rvwap_sigma_bands_payload(payload);
    }
    if let Some(payload) = indicator_payload_mut(indicators, "high_volume_pulse") {
        compact_high_volume_pulse_payload(payload);
    }

    // Compress event indicators: keep last 24h in full, aggregate older bars by day.
    for code in [
        "absorption",
        "bearish_absorption",
        "bullish_absorption",
        "buying_exhaustion",
        "selling_exhaustion",
        "initiation",
        "bearish_initiation",
        "bullish_initiation",
    ] {
        if let Some(payload) = indicator_payload_mut(indicators, code) {
            compact_event_payload(payload);
        }
    }

    apply_llm_token_governor(indicators);
    build_core_price_anchors(indicators, reference_price);
}

fn indicator_payload_mut<'a>(
    indicators: &'a mut Map<String, Value>,
    code: &str,
) -> Option<&'a mut Map<String, Value>> {
    indicators
        .get_mut(code)
        .and_then(Value::as_object_mut)
        .and_then(|indicator| indicator.get_mut("payload"))
        .and_then(Value::as_object_mut)
}

fn indicator_payload_ref<'a>(
    indicators: &'a Map<String, Value>,
    code: &str,
) -> Option<&'a Map<String, Value>> {
    indicators
        .get(code)
        .and_then(Value::as_object)
        .and_then(|indicator| indicator.get("payload"))
        .and_then(Value::as_object)
}

fn extract_reference_price(indicators: &Map<String, Value>) -> Option<f64> {
    indicators
        .get("avwap")
        .and_then(Value::as_object)
        .and_then(|indicator| indicator.get("payload"))
        .and_then(Value::as_object)
        .and_then(|payload| {
            payload
                .get("fut_last_price")
                .and_then(Value::as_f64)
                .or_else(|| payload.get("fut_mark_price").and_then(Value::as_f64))
        })
        .or_else(|| {
            indicators
                .get("orderbook_depth")
                .and_then(Value::as_object)
                .and_then(|indicator| indicator.get("payload"))
                .and_then(Value::as_object)
                .and_then(|payload| {
                    payload
                        .get("depth_band_reference_price")
                        .and_then(Value::as_f64)
                })
        })
}

fn extract_pvs_value_area_ranges(indicators: &Map<String, Value>) -> HashMap<String, (f64, f64)> {
    let mut out = HashMap::new();
    let Some(by_window) = indicators
        .get("price_volume_structure")
        .and_then(Value::as_object)
        .and_then(|indicator| indicator.get("payload"))
        .and_then(Value::as_object)
        .and_then(|payload| payload.get("by_window"))
        .and_then(Value::as_object)
    else {
        return out;
    };

    for (window, value) in by_window {
        let Some(obj) = value.as_object() else {
            continue;
        };
        let val = obj.get("val").and_then(Value::as_f64);
        let vah = obj.get("vah").and_then(Value::as_f64);
        if let (Some(val), Some(vah)) = (val, vah) {
            let lo = val.min(vah);
            let hi = val.max(vah);
            out.insert(window.clone(), (lo, hi));
        }
    }
    out
}

fn compact_kline_history_payload(payload: &mut Map<String, Value>) {
    let Some(intervals_obj) = payload.get_mut("intervals").and_then(Value::as_object_mut) else {
        return;
    };

    let mut compacted_intervals = Map::new();
    for interval in ["1m", "15m", "4h", "1d"] {
        let keep = match interval {
            "1m" => LLM_KLINE_KEEP_1M,
            "15m" => LLM_KLINE_KEEP_15M,
            "4h" => LLM_KLINE_KEEP_4H,
            "1d" => LLM_KLINE_KEEP_1D,
            _ => 60,
        };

        let (futures_bars, spot_bars) = (
            read_kline_market_bars(intervals_obj, interval, "futures"),
            read_kline_market_bars(intervals_obj, interval, "spot"),
        );
        if futures_bars.is_empty() && spot_bars.is_empty() {
            continue;
        }

        let mut compacted = Map::new();
        compacted.insert(
            "interval_code".to_string(),
            Value::String(interval.to_string()),
        );

        let futures_trimmed = if futures_bars.len() > keep {
            futures_bars[futures_bars.len() - keep..].to_vec()
        } else {
            futures_bars
        };
        let spot_trimmed = if spot_bars.len() > keep {
            spot_bars[spot_bars.len() - keep..].to_vec()
        } else {
            spot_bars
        };

        let latest_close_gap_f_minus_s = futures_trimmed
            .last()
            .and_then(|f| f.get("close").and_then(Value::as_f64))
            .zip(
                spot_trimmed
                    .last()
                    .and_then(|s| s.get("close").and_then(Value::as_f64)),
            )
            .map(|(f, s)| f - s);

        let latest_high_gap_f_minus_s = futures_trimmed
            .last()
            .and_then(|f| f.get("high").and_then(Value::as_f64))
            .zip(
                spot_trimmed
                    .last()
                    .and_then(|s| s.get("high").and_then(Value::as_f64)),
            )
            .map(|(f, s)| f - s);

        let latest_low_gap_f_minus_s = futures_trimmed
            .last()
            .and_then(|f| f.get("low").and_then(Value::as_f64))
            .zip(
                spot_trimmed
                    .last()
                    .and_then(|s| s.get("low").and_then(Value::as_f64)),
            )
            .map(|(f, s)| f - s);

        compacted.insert(
            "futures".to_string(),
            json!({
                "returned_count": futures_trimmed.len(),
                "bars": futures_trimmed,
            }),
        );
        // Include actual spot bars so the LLM can compare spot vs futures price
        // action directly — required for strategy 7 (spot-led flow continuation).
        if !spot_trimmed.is_empty() {
            compacted.insert(
                "spot".to_string(),
                json!({
                    "returned_count": spot_trimmed.len(),
                    "bars": spot_trimmed,
                }),
            );
        }
        compacted.insert(
            "spot_gap".to_string(),
            json!({
                "latest_close_gap_f_minus_s": latest_close_gap_f_minus_s,
                "latest_high_gap_f_minus_s": latest_high_gap_f_minus_s,
                "latest_low_gap_f_minus_s": latest_low_gap_f_minus_s,
            }),
        );

        compacted_intervals.insert(interval.to_string(), Value::Object(compacted));
    }

    payload.insert("intervals".to_string(), Value::Object(compacted_intervals));
}

fn read_kline_market_bars(
    intervals_obj: &Map<String, Value>,
    interval_code: &str,
    market: &str,
) -> Vec<Value> {
    intervals_obj
        .get(interval_code)
        .and_then(Value::as_object)
        .and_then(|interval| interval.get("markets"))
        .and_then(Value::as_object)
        .and_then(|markets| markets.get(market))
        .and_then(Value::as_object)
        .and_then(|market_obj| market_obj.get("bars"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn compact_funding_rate_payload_with_time_buckets(payload: &mut Map<String, Value>) {
    let Some(by_window) = payload.get_mut("by_window").and_then(Value::as_object_mut) else {
        return;
    };

    for window_payload in by_window.values_mut() {
        let Some(window_obj) = window_payload.as_object_mut() else {
            continue;
        };

        let changes = window_obj
            .remove("changes")
            .and_then(|v| v.as_array().cloned())
            .unwrap_or_default();
        window_obj.insert("change_count".to_string(), json!(changes.len()));
        if changes.is_empty() {
            window_obj.insert("changes_recent".to_string(), Value::Array(Vec::new()));
            window_obj.insert(
                "changes_bucketed".to_string(),
                json!({
                    "bucket_span_secs": LLM_TIME_BUCKET_SPAN_SECS,
                    "bucket_count": 0,
                    "buckets": [],
                }),
            );
            continue;
        }

        let buckets = bucket_funding_changes(&changes, LLM_TIME_BUCKET_SPAN_SECS);
        let recent =
            select_recent_records_by_timestamp(&changes, "change_ts", LLM_TIME_BUCKET_RECENT_KEEP);
        window_obj.insert("changes_recent".to_string(), Value::Array(recent));
        window_obj.insert(
            "changes_bucketed".to_string(),
            json!({
                "bucket_span_secs": LLM_TIME_BUCKET_SPAN_SECS,
                "bucket_count": buckets.len(),
                "buckets": buckets,
            }),
        );
    }
}

fn compact_cvd_pack_payload_with_time_buckets(payload: &mut Map<String, Value>) {
    let Some(by_window) = payload.get_mut("by_window").and_then(Value::as_object_mut) else {
        return;
    };

    for (window, window_payload) in by_window.iter_mut() {
        let Some(window_obj) = window_payload.as_object_mut() else {
            continue;
        };
        let series = window_obj
            .remove("series")
            .and_then(|v| v.as_array().cloned())
            .unwrap_or_default();
        let series_count = series.len();
        window_obj.insert("series_count".to_string(), json!(series_count));
        if series_count <= 1 {
            window_obj.insert("series".to_string(), Value::Array(series));
            continue;
        }

        // 15m needs more history for S2 delta divergence detection (96 bars = 24h)
        let keep = if window == "15m" {
            LLM_CVD_KEEP_15M
        } else {
            LLM_TIME_BUCKET_RECENT_KEEP
        };
        let recent = select_recent_records_by_timestamp(&series, "ts", keep);
        window_obj.insert("series".to_string(), Value::Array(recent));
    }
}

fn compact_orderbook_depth_payload(payload: &mut Map<String, Value>) {
    let Some(levels) = payload
        .remove("levels")
        .and_then(|value| value.as_array().cloned())
    else {
        return;
    };

    let reference_price = infer_reference_price(&levels);
    payload.insert("levels_total".to_string(), json!(levels.len()));
    payload.insert(
        "top_bid_walls".to_string(),
        Value::Array(select_top_levels_by_metric(
            &levels,
            "bid_liquidity",
            LLM_ORDERBOOK_SIDE_LEVELS,
            false,
        )),
    );
    payload.insert(
        "top_ask_walls".to_string(),
        Value::Array(select_top_levels_by_metric(
            &levels,
            "ask_liquidity",
            LLM_ORDERBOOK_SIDE_LEVELS,
            false,
        )),
    );
    payload.insert(
        "top_total_liquidity_levels".to_string(),
        Value::Array(select_top_levels_by_metric(
            &levels,
            "total_liquidity",
            LLM_ORDERBOOK_TOP_LEVELS,
            false,
        )),
    );
    payload.insert(
        "top_abs_net_liquidity_levels".to_string(),
        Value::Array(select_top_levels_by_metric(
            &levels,
            "net_liquidity",
            LLM_ORDERBOOK_TOP_LEVELS,
            true,
        )),
    );
    if let Some(reference_price) = reference_price {
        payload.insert(
            "depth_band_reference_price".to_string(),
            json!(reference_price),
        );
    }
    payload.insert(
        "depth_bands".to_string(),
        Value::Array(build_depth_bands(&levels, reference_price)),
    );

    // Remove redundant microprice variants — all four carry the same value;
    // keep only microprice_fut as the canonical field.
    for field in [
        "microprice_adj_fut",
        "microprice_classic_fut",
        "microprice_kappa_fut",
    ] {
        payload.remove(field);
    }

    // Remove redundant OBI variants — keep obi_k_dw_twa_fut (time-weighted, distance-weighted),
    // obi_k_dw_twa_spot (spot counterpart), obi_k_dw_change_fut (momentum),
    // obi_k_dw_slope_fut (DOM slope for S3 liquidity wall), and
    // obi_shock_fut (shock signal); discard the aliases and lesser variants.
    for field in [
        "obi",
        "obi_fut",
        "obi_k_twa_fut",
        "obi_l1_twa_fut",
        "obi_k_dw_adj_twa_fut",
        "obi_k_dw_close_fut",
    ] {
        payload.remove(field);
    }
}

fn compact_footprint_payload_with_price_bands(
    payload: &mut Map<String, Value>,
    reference_price: Option<f64>,
    pvs_value_areas: &HashMap<String, (f64, f64)>,
) {
    compact_price_points_to_zones(payload, "buy_imbalance_prices", "buy_imbalance");
    compact_price_points_to_zones(payload, "sell_imbalance_prices", "sell_imbalance");

    if let Some(levels) = payload
        .remove("levels")
        .and_then(|value| value.as_array().cloned())
    {
        payload.insert("levels_total".to_string(), json!(levels.len()));
        payload.insert(
            "top_levels_by_total".to_string(),
            Value::Array(select_top_levels_by_metric(
                &levels,
                "total",
                LLM_FOOTPRINT_TOP_LEVELS,
                false,
            )),
        );
        payload.insert(
            "top_levels_by_abs_delta".to_string(),
            Value::Array(select_top_levels_by_metric(
                &levels,
                "delta",
                LLM_FOOTPRINT_TOP_LEVELS,
                true,
            )),
        );
    }

    let Some(by_window) = payload.get_mut("by_window").and_then(Value::as_object_mut) else {
        return;
    };

    for (window, window_payload) in by_window.iter_mut() {
        let Some(obj) = window_payload.as_object_mut() else {
            continue;
        };
        compact_footprint_window_payload(
            window,
            obj,
            reference_price,
            pvs_value_areas.get(window).copied(),
        );
    }
}

fn compact_price_volume_structure_payload_with_price_bands(
    payload: &mut Map<String, Value>,
    reference_price: Option<f64>,
) {
    compact_price_volume_structure_window_summary(
        payload,
        LLM_PROFILE_TOP_LEVELS,
        LLM_VALUE_AREA_TOP_LEVELS,
    );

    let Some(by_window) = payload.get_mut("by_window").and_then(Value::as_object_mut) else {
        return;
    };

    for (window, window_payload) in by_window.iter_mut() {
        let Some(window_obj) = window_payload.as_object_mut() else {
            continue;
        };
        compact_price_volume_structure_window_payload(window, window_obj, reference_price);
    }
}

fn compact_price_volume_structure_window_summary(
    payload: &mut Map<String, Value>,
    profile_top_levels: usize,
    value_area_top_levels: usize,
) {
    if let Some(value_area_levels) = payload
        .remove("value_area_levels")
        .and_then(|value| value.as_array().cloned())
    {
        payload.insert(
            "value_area_level_count".to_string(),
            json!(value_area_levels.len()),
        );
        payload.insert(
            "top_value_area_levels".to_string(),
            Value::Array(select_top_levels_by_metric(
                &value_area_levels,
                "volume",
                value_area_top_levels,
                false,
            )),
        );
    }

    if let Some(levels) = payload
        .remove("levels")
        .and_then(|value| value.as_array().cloned())
    {
        payload.insert("levels_total".to_string(), json!(levels.len()));
        payload.insert(
            "top_volume_levels".to_string(),
            Value::Array(select_top_levels_by_metric(
                &levels,
                "volume",
                profile_top_levels,
                false,
            )),
        );
        payload.insert(
            "top_abs_delta_levels".to_string(),
            Value::Array(select_top_levels_by_metric(
                &levels,
                "delta",
                profile_top_levels,
                true,
            )),
        );
    }
}

fn compact_price_volume_structure_window_payload(
    window: &str,
    payload: &mut Map<String, Value>,
    reference_price: Option<f64>,
) {
    // Drop raw heavy arrays and stale derived keys, then rebuild compacted fields.
    for key in [
        "levels_in_va_corridor",
        "levels_near_price",
        "levels_in_va_corridor_bucketed",
        "levels_near_price_bucketed",
        "levels_in_va_corridor_raw_level_count",
        "levels_near_price_raw_level_count",
    ] {
        payload.remove(key);
    }

    let (profile_top_levels, value_area_top_levels) = match window {
        "4h" => (LLM_PROFILE_TOP_LEVELS_4H, LLM_VALUE_AREA_TOP_LEVELS_4H),
        "1d" => (LLM_PROFILE_TOP_LEVELS_1D, LLM_VALUE_AREA_TOP_LEVELS_1D),
        "15m" => (LLM_PROFILE_TOP_LEVELS_15M, LLM_VALUE_AREA_TOP_LEVELS_15M),
        _ => (LLM_PROFILE_TOP_LEVELS_1D, LLM_VALUE_AREA_TOP_LEVELS_1D),
    };

    let value_area_levels = payload
        .remove("value_area_levels")
        .and_then(|value| value.as_array().cloned())
        .unwrap_or_default();
    let levels = payload
        .remove("levels")
        .and_then(|value| value.as_array().cloned())
        .unwrap_or_default();

    payload.insert(
        "value_area_level_count".to_string(),
        json!(value_area_levels.len()),
    );
    payload.insert("levels_total".to_string(), json!(levels.len()));
    payload.insert(
        "top_value_area_levels".to_string(),
        Value::Array(select_top_levels_by_metric(
            &value_area_levels,
            "volume",
            value_area_top_levels,
            false,
        )),
    );
    payload.insert(
        "top_volume_levels".to_string(),
        Value::Array(select_top_levels_by_metric(
            &levels,
            "volume",
            profile_top_levels,
            false,
        )),
    );
    payload.insert(
        "top_abs_delta_levels".to_string(),
        Value::Array(select_top_levels_by_metric(
            &levels,
            "delta",
            profile_top_levels,
            true,
        )),
    );

    if window == "4h" && !levels.is_empty() {
        let effective_reference = reference_price.or_else(|| infer_reference_price(&levels));
        let band_size = dynamic_price_band_size(effective_reference);

        if let (Some(val), Some(vah)) = (
            payload.get("val").and_then(Value::as_f64),
            payload.get("vah").and_then(Value::as_f64),
        ) {
            let corridor = select_levels_in_price_range(&levels, val, vah);
            payload.insert(
                "levels_in_va_corridor_raw_level_count".to_string(),
                json!(corridor.len()),
            );
            payload.insert(
                "levels_in_va_corridor_bucketed".to_string(),
                build_pvs_level_bands_summary(&corridor, band_size, effective_reference),
            );
        }

        if let Some(reference) = effective_reference {
            let near = select_levels_near_price(&levels, reference, LLM_LEVELS_NEAR_PRICE_PCT_4H);
            payload.insert(
                "levels_near_price_raw_level_count".to_string(),
                json!(near.len()),
            );
            payload.insert(
                "levels_near_price_bucketed".to_string(),
                build_pvs_level_bands_summary(&near, band_size, Some(reference)),
            );
        }
    }
}

fn compact_footprint_window_payload(
    window: &str,
    payload: &mut Map<String, Value>,
    reference_price: Option<f64>,
    pvs_value_area: Option<(f64, f64)>,
) {
    // Drop raw heavy arrays and stale derived keys, then rebuild compacted fields.
    for key in [
        "levels_in_va_corridor",
        "levels_near_price",
        "levels_in_va_corridor_bucketed",
        "levels_near_price_bucketed",
        "levels_in_va_corridor_raw_level_count",
        "levels_near_price_raw_level_count",
    ] {
        payload.remove(key);
    }

    let levels = payload
        .remove("levels")
        .and_then(|value| value.as_array().cloned())
        .unwrap_or_default();
    let buy_zones = take_price_points_as_zones(payload, "buy_imbalance_prices", "buy_imbalance");
    let sell_zones = take_price_points_as_zones(payload, "sell_imbalance_prices", "sell_imbalance");
    let buy_stacks = payload
        .remove("buy_stacks")
        .and_then(|value| value.as_array().cloned())
        .unwrap_or_default();
    let sell_stacks = payload
        .remove("sell_stacks")
        .and_then(|value| value.as_array().cloned())
        .unwrap_or_default();

    // Extract unfinished-auction prices before levels are discarded (S4: TP targets).
    // ua_top_flag marks the highest unfilled price (supply gap top);
    // ua_bottom_flag marks the lowest unfilled price (demand gap bottom).
    if let Some(ua_top_price) = levels
        .iter()
        .filter(|l| {
            l.get("ua_top_flag")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        })
        .filter_map(|l| l.get("price_level").and_then(Value::as_f64))
        .reduce(f64::max)
    {
        payload.insert("ua_top_price".to_string(), json!(ua_top_price));
    }
    if let Some(ua_bottom_price) = levels
        .iter()
        .filter(|l| {
            l.get("ua_bottom_flag")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        })
        .filter_map(|l| l.get("price_level").and_then(Value::as_f64))
        .reduce(f64::min)
    {
        payload.insert("ua_bottom_price".to_string(), json!(ua_bottom_price));
    }

    payload.insert("levels_total".to_string(), json!(levels.len()));
    payload.insert("buy_stack_count".to_string(), json!(buy_stacks.len()));
    payload.insert("sell_stack_count".to_string(), json!(sell_stacks.len()));

    let (top_levels_keep, stack_keep) = match window {
        "4h" => (LLM_FOOTPRINT_TOP_LEVELS_4H, LLM_FOOTPRINT_STACK_TOP_4H),
        "1d" => (LLM_FOOTPRINT_TOP_LEVELS_1D, LLM_FOOTPRINT_STACK_TOP_1D),
        "15m" => (LLM_FOOTPRINT_TOP_LEVELS_15M, LLM_FOOTPRINT_STACK_TOP_15M),
        _ => (LLM_FOOTPRINT_TOP_LEVELS_1D, LLM_FOOTPRINT_STACK_TOP_1D),
    };

    payload.insert(
        "top_levels_by_total".to_string(),
        Value::Array(select_top_levels_by_metric(
            &levels,
            "total",
            top_levels_keep,
            false,
        )),
    );
    payload.insert(
        "top_levels_by_abs_delta".to_string(),
        Value::Array(select_top_levels_by_metric(
            &levels,
            "delta",
            top_levels_keep,
            true,
        )),
    );
    payload.insert(
        "buy_stacks_top".to_string(),
        Value::Array(select_top_levels_by_metric(
            &buy_stacks,
            "length",
            stack_keep,
            false,
        )),
    );
    payload.insert(
        "sell_stacks_top".to_string(),
        Value::Array(select_top_levels_by_metric(
            &sell_stacks,
            "length",
            stack_keep,
            false,
        )),
    );

    let effective_reference = reference_price.or_else(|| infer_reference_price(&levels));

    // Annotate zones with `intact` flag so LLM (S1) can assess zone validity.
    // Buy zones are intact while price is still above them (supply not yet revisited).
    // Sell zones are intact while price is still below them (demand not yet revisited).
    let buy_zones = if let Some(ref_price) = effective_reference {
        annotate_zones_intact(buy_zones, ref_price, true)
    } else {
        buy_zones
    };
    let sell_zones = if let Some(ref_price) = effective_reference {
        annotate_zones_intact(sell_zones, ref_price, false)
    } else {
        sell_zones
    };

    match window {
        "4h" => {
            payload.insert(
                "buy_imbalance_zones_top_strength".to_string(),
                Value::Array(select_top_levels_by_metric(
                    &buy_zones,
                    "count",
                    LLM_FOOTPRINT_ZONE_STRENGTH_TOP_4H,
                    false,
                )),
            );
            payload.insert(
                "sell_imbalance_zones_top_strength".to_string(),
                Value::Array(select_top_levels_by_metric(
                    &sell_zones,
                    "count",
                    LLM_FOOTPRINT_ZONE_STRENGTH_TOP_4H,
                    false,
                )),
            );
            payload.insert(
                "buy_imbalance_zones_top".to_string(),
                Value::Array(select_top_levels_by_metric(
                    &buy_zones,
                    "count",
                    LLM_FOOTPRINT_ZONE_STRENGTH_TOP_4H,
                    false,
                )),
            );
            payload.insert(
                "sell_imbalance_zones_top".to_string(),
                Value::Array(select_top_levels_by_metric(
                    &sell_zones,
                    "count",
                    LLM_FOOTPRINT_ZONE_STRENGTH_TOP_4H,
                    false,
                )),
            );

            if let Some(reference) = effective_reference {
                payload.insert(
                    "buy_imbalance_zones_near_price".to_string(),
                    Value::Array(select_nearest_zones(
                        &buy_zones,
                        reference,
                        LLM_FOOTPRINT_ZONE_NEAR_TOP_4H,
                    )),
                );
                payload.insert(
                    "sell_imbalance_zones_near_price".to_string(),
                    Value::Array(select_nearest_zones(
                        &sell_zones,
                        reference,
                        LLM_FOOTPRINT_ZONE_NEAR_TOP_4H,
                    )),
                );
                payload.insert(
                    "buy_imbalance_zone_nearest_above".to_string(),
                    nearest_zone_above(&buy_zones, reference).unwrap_or(Value::Null),
                );
                payload.insert(
                    "buy_imbalance_zone_nearest_below".to_string(),
                    nearest_zone_below(&buy_zones, reference).unwrap_or(Value::Null),
                );
                payload.insert(
                    "sell_imbalance_zone_nearest_above".to_string(),
                    nearest_zone_above(&sell_zones, reference).unwrap_or(Value::Null),
                );
                payload.insert(
                    "sell_imbalance_zone_nearest_below".to_string(),
                    nearest_zone_below(&sell_zones, reference).unwrap_or(Value::Null),
                );

                let near =
                    select_levels_near_price(&levels, reference, LLM_LEVELS_NEAR_PRICE_PCT_4H);
                payload.insert(
                    "levels_near_price_raw_level_count".to_string(),
                    json!(near.len()),
                );
                payload.insert(
                    "levels_near_price_bucketed".to_string(),
                    build_footprint_level_bands_summary(
                        &near,
                        dynamic_price_band_size(Some(reference)),
                        Some(reference),
                    ),
                );
            }

            if let Some((val, vah)) = pvs_value_area {
                let corridor = select_levels_in_price_range(&levels, val, vah);
                payload.insert(
                    "levels_in_va_corridor_raw_level_count".to_string(),
                    json!(corridor.len()),
                );
                payload.insert(
                    "levels_in_va_corridor_bucketed".to_string(),
                    build_footprint_level_bands_summary(
                        &corridor,
                        dynamic_price_band_size(effective_reference),
                        effective_reference,
                    ),
                );
            }
        }
        "1d" => {
            payload.insert(
                "buy_imbalance_zones_top_strength".to_string(),
                Value::Array(select_top_levels_by_metric(
                    &buy_zones,
                    "count",
                    LLM_FOOTPRINT_ZONE_STRENGTH_TOP_1D,
                    false,
                )),
            );
            payload.insert(
                "sell_imbalance_zones_top_strength".to_string(),
                Value::Array(select_top_levels_by_metric(
                    &sell_zones,
                    "count",
                    LLM_FOOTPRINT_ZONE_STRENGTH_TOP_1D,
                    false,
                )),
            );
            payload.insert(
                "buy_imbalance_zones_top".to_string(),
                Value::Array(select_top_levels_by_metric(
                    &buy_zones,
                    "count",
                    LLM_FOOTPRINT_ZONE_STRENGTH_TOP_1D,
                    false,
                )),
            );
            payload.insert(
                "sell_imbalance_zones_top".to_string(),
                Value::Array(select_top_levels_by_metric(
                    &sell_zones,
                    "count",
                    LLM_FOOTPRINT_ZONE_STRENGTH_TOP_1D,
                    false,
                )),
            );
        }
        "15m" => {
            let buy_near = if let Some(reference) = effective_reference {
                select_nearest_zones(&buy_zones, reference, LLM_FOOTPRINT_ZONE_NEAR_TOP_15M)
            } else {
                select_top_levels_by_metric(
                    &buy_zones,
                    "count",
                    LLM_FOOTPRINT_ZONE_STRENGTH_TOP_15M,
                    false,
                )
            };
            let sell_near = if let Some(reference) = effective_reference {
                select_nearest_zones(&sell_zones, reference, LLM_FOOTPRINT_ZONE_NEAR_TOP_15M)
            } else {
                select_top_levels_by_metric(
                    &sell_zones,
                    "count",
                    LLM_FOOTPRINT_ZONE_STRENGTH_TOP_15M,
                    false,
                )
            };
            payload.insert(
                "buy_imbalance_zones_near_price".to_string(),
                Value::Array(buy_near.clone()),
            );
            payload.insert(
                "sell_imbalance_zones_near_price".to_string(),
                Value::Array(sell_near.clone()),
            );
            payload.insert(
                "buy_imbalance_zones_top".to_string(),
                Value::Array(buy_near),
            );
            payload.insert(
                "sell_imbalance_zones_top".to_string(),
                Value::Array(sell_near),
            );
        }
        _ => {
            payload.insert(
                "buy_imbalance_zones_top".to_string(),
                Value::Array(select_top_levels_by_metric(
                    &buy_zones,
                    "count",
                    LLM_FOOTPRINT_ZONE_STRENGTH_TOP_1D,
                    false,
                )),
            );
            payload.insert(
                "sell_imbalance_zones_top".to_string(),
                Value::Array(select_top_levels_by_metric(
                    &sell_zones,
                    "count",
                    LLM_FOOTPRINT_ZONE_STRENGTH_TOP_1D,
                    false,
                )),
            );
        }
    }
}

fn take_price_points_as_zones(
    payload: &mut Map<String, Value>,
    source_key: &str,
    prefix: &str,
) -> Vec<Value> {
    let Some(points) = payload
        .remove(source_key)
        .and_then(|value| value.as_array().cloned())
    else {
        return Vec::new();
    };
    let zones = price_points_to_zones(&points);
    payload.insert(format!("{}_count", prefix), json!(points.len()));
    payload.insert(format!("{}_zone_count", prefix), json!(zones.len()));
    zones
}

#[derive(Debug, Clone)]
struct FundingBucketAgg {
    start_ts: i64,
    end_ts: i64,
    count: usize,
    funding_delta_sum: f64,
    funding_delta_abs_max: f64,
    funding_new_last: Option<f64>,
    funding_new_last_ts: i64,
    mark_price_weighted_sum: f64,
    mark_price_weight_sum: f64,
    mark_price_simple_sum: f64,
    mark_price_simple_count: usize,
}

impl FundingBucketAgg {
    fn new(start_ts: i64, end_ts: i64) -> Self {
        Self {
            start_ts,
            end_ts,
            count: 0,
            funding_delta_sum: 0.0,
            funding_delta_abs_max: 0.0,
            funding_new_last: None,
            funding_new_last_ts: i64::MIN,
            mark_price_weighted_sum: 0.0,
            mark_price_weight_sum: 0.0,
            mark_price_simple_sum: 0.0,
            mark_price_simple_count: 0,
        }
    }
}

#[derive(Debug, Clone)]
struct PriceBandAgg {
    price_start: f64,
    price_end: f64,
    level_count: usize,
    volume_sum: f64,
    delta_sum: f64,
    buy_sum: f64,
    sell_sum: f64,
    imbalance_buy_count: usize,
    imbalance_sell_count: usize,
    hvn_count: usize,
    lvn_count: usize,
    max_total_metric: f64,
    max_total_level_price: Option<f64>,
    max_abs_delta_metric: f64,
    max_abs_delta_level_price: Option<f64>,
}

impl PriceBandAgg {
    fn new(price_start: f64, price_end: f64) -> Self {
        Self {
            price_start,
            price_end,
            level_count: 0,
            volume_sum: 0.0,
            delta_sum: 0.0,
            buy_sum: 0.0,
            sell_sum: 0.0,
            imbalance_buy_count: 0,
            imbalance_sell_count: 0,
            hvn_count: 0,
            lvn_count: 0,
            max_total_metric: f64::NEG_INFINITY,
            max_total_level_price: None,
            max_abs_delta_metric: f64::NEG_INFINITY,
            max_abs_delta_level_price: None,
        }
    }
}

fn parse_rfc3339_to_utc(raw: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

fn unix_ts_to_rfc3339(ts: i64) -> Option<String> {
    DateTime::<Utc>::from_timestamp(ts, 0).map(|dt| dt.to_rfc3339())
}

fn select_recent_records_by_timestamp(records: &[Value], ts_key: &str, keep: usize) -> Vec<Value> {
    let mut ranked = records
        .iter()
        .map(|record| {
            let ts = record
                .get(ts_key)
                .and_then(Value::as_str)
                .and_then(parse_rfc3339_to_utc)
                .map(|dt| dt.timestamp())
                .unwrap_or(i64::MIN);
            (ts, record.clone())
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|a, b| b.0.cmp(&a.0));
    ranked.truncate(keep);
    ranked.into_iter().map(|(_, record)| record).collect()
}

fn bucket_funding_changes(changes: &[Value], bucket_span_secs: i64) -> Vec<Value> {
    let mut buckets: HashMap<i64, FundingBucketAgg> = HashMap::new();
    for change in changes {
        let Some(obj) = change.as_object() else {
            continue;
        };
        let Some(ts_raw) = obj.get("change_ts").and_then(Value::as_str) else {
            continue;
        };
        let Some(ts) = parse_rfc3339_to_utc(ts_raw).map(|dt| dt.timestamp()) else {
            continue;
        };
        let start_ts = ts.div_euclid(bucket_span_secs) * bucket_span_secs;
        let end_ts = start_ts + bucket_span_secs;
        let entry = buckets
            .entry(start_ts)
            .or_insert_with(|| FundingBucketAgg::new(start_ts, end_ts));

        entry.count += 1;
        let funding_delta = obj
            .get("funding_delta")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        entry.funding_delta_sum += funding_delta;
        entry.funding_delta_abs_max = entry.funding_delta_abs_max.max(funding_delta.abs());

        if ts >= entry.funding_new_last_ts {
            entry.funding_new_last = obj.get("funding_new").and_then(Value::as_f64);
            entry.funding_new_last_ts = ts;
        }

        if let Some(mark_price) = obj.get("mark_price_at_change").and_then(Value::as_f64) {
            let weight = funding_delta.abs();
            if weight > 0.0 {
                entry.mark_price_weighted_sum += mark_price * weight;
                entry.mark_price_weight_sum += weight;
            }
            entry.mark_price_simple_sum += mark_price;
            entry.mark_price_simple_count += 1;
        }
    }

    let mut ranked = buckets.into_values().collect::<Vec<_>>();
    ranked.sort_by(|a, b| b.start_ts.cmp(&a.start_ts));
    ranked
        .into_iter()
        .map(|bucket| {
            let mark_price_vwap = if bucket.mark_price_weight_sum > 0.0 {
                Some(bucket.mark_price_weighted_sum / bucket.mark_price_weight_sum)
            } else if bucket.mark_price_simple_count > 0 {
                Some(bucket.mark_price_simple_sum / bucket.mark_price_simple_count as f64)
            } else {
                None
            };
            json!({
                "bucket_start_ts": unix_ts_to_rfc3339(bucket.start_ts),
                "bucket_end_ts": unix_ts_to_rfc3339(bucket.end_ts),
                "count": bucket.count,
                "funding_delta_sum": bucket.funding_delta_sum,
                "funding_delta_abs_max": bucket.funding_delta_abs_max,
                "funding_new_last": bucket.funding_new_last,
                "mark_price_vwap": mark_price_vwap,
            })
        })
        .collect()
}

fn dynamic_price_band_size(reference_price: Option<f64>) -> f64 {
    let base = reference_price
        .filter(|v| v.is_finite() && *v > 0.0)
        .map(|v| v * LLM_PRICE_BAND_RATIO)
        .unwrap_or(LLM_PRICE_BAND_MIN_SIZE);
    let raw = base.max(LLM_PRICE_BAND_MIN_SIZE);
    (raw * 10_000.0).round() / 10_000.0
}

fn build_pvs_level_bands_summary(
    levels: &[Value],
    band_size: f64,
    reference_price: Option<f64>,
) -> Value {
    let mut bands: HashMap<i64, PriceBandAgg> = HashMap::new();
    for level in levels {
        let Some(obj) = level.as_object() else {
            continue;
        };
        let Some(price) = obj.get("price_level").and_then(Value::as_f64) else {
            continue;
        };
        let band_idx = (price / band_size).floor() as i64;
        let price_start = band_idx as f64 * band_size;
        let price_end = price_start + band_size;
        let agg = bands
            .entry(band_idx)
            .or_insert_with(|| PriceBandAgg::new(price_start, price_end));

        let volume = obj.get("volume").and_then(Value::as_f64).unwrap_or(0.0);
        let delta = obj.get("delta").and_then(Value::as_f64).unwrap_or(0.0);
        let buy = obj.get("buy_volume").and_then(Value::as_f64).unwrap_or(0.0);
        let sell = obj
            .get("sell_volume")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);

        agg.level_count += 1;
        agg.volume_sum += volume;
        agg.delta_sum += delta;
        agg.buy_sum += buy;
        agg.sell_sum += sell;
        if delta > 0.0 {
            agg.imbalance_buy_count += 1;
        } else if delta < 0.0 {
            agg.imbalance_sell_count += 1;
        }
        if obj.get("is_hvn").and_then(Value::as_bool).unwrap_or(false) {
            agg.hvn_count += 1;
        }
        if obj.get("is_lvn").and_then(Value::as_bool).unwrap_or(false) {
            agg.lvn_count += 1;
        }
        if volume > agg.max_total_metric {
            agg.max_total_metric = volume;
            agg.max_total_level_price = Some(price);
        }
        if delta.abs() > agg.max_abs_delta_metric {
            agg.max_abs_delta_metric = delta.abs();
            agg.max_abs_delta_level_price = Some(price);
        }
    }

    build_price_band_summary_from_map(bands, levels.len(), band_size, reference_price)
}

fn build_footprint_level_bands_summary(
    levels: &[Value],
    band_size: f64,
    reference_price: Option<f64>,
) -> Value {
    let mut bands: HashMap<i64, PriceBandAgg> = HashMap::new();
    for level in levels {
        let Some(obj) = level.as_object() else {
            continue;
        };
        let Some(price) = obj.get("price_level").and_then(Value::as_f64) else {
            continue;
        };
        let band_idx = (price / band_size).floor() as i64;
        let price_start = band_idx as f64 * band_size;
        let price_end = price_start + band_size;
        let agg = bands
            .entry(band_idx)
            .or_insert_with(|| PriceBandAgg::new(price_start, price_end));

        let total = obj.get("total").and_then(Value::as_f64).unwrap_or(0.0);
        let delta = obj.get("delta").and_then(Value::as_f64).unwrap_or(0.0);
        let buy = obj.get("buy").and_then(Value::as_f64).unwrap_or(0.0);
        let sell = obj.get("sell").and_then(Value::as_f64).unwrap_or(0.0);

        agg.level_count += 1;
        agg.volume_sum += total;
        agg.delta_sum += delta;
        agg.buy_sum += buy;
        agg.sell_sum += sell;

        if obj
            .get("buy_imbalance")
            .and_then(Value::as_bool)
            .or_else(|| {
                obj.get("buy_imbalance")
                    .and_then(Value::as_i64)
                    .map(|v| v > 0)
            })
            .unwrap_or(false)
        {
            agg.imbalance_buy_count += 1;
        }
        if obj
            .get("sell_imbalance")
            .and_then(Value::as_bool)
            .or_else(|| {
                obj.get("sell_imbalance")
                    .and_then(Value::as_i64)
                    .map(|v| v > 0)
            })
            .unwrap_or(false)
        {
            agg.imbalance_sell_count += 1;
        }

        if total > agg.max_total_metric {
            agg.max_total_metric = total;
            agg.max_total_level_price = Some(price);
        }
        if delta.abs() > agg.max_abs_delta_metric {
            agg.max_abs_delta_metric = delta.abs();
            agg.max_abs_delta_level_price = Some(price);
        }
    }

    build_price_band_summary_from_map(bands, levels.len(), band_size, reference_price)
}

fn build_price_band_summary_from_map(
    bands_map: HashMap<i64, PriceBandAgg>,
    source_level_count: usize,
    band_size: f64,
    reference_price: Option<f64>,
) -> Value {
    let mut entries = bands_map.into_iter().collect::<Vec<_>>();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    let source_band_count = entries.len();
    if entries.len() > LLM_PRICE_BAND_MAX_BANDS {
        entries = trim_price_band_entries(
            entries,
            reference_price,
            band_size,
            LLM_PRICE_BAND_MAX_BANDS,
        );
    }

    let bands = entries
        .into_iter()
        .map(|(_, band)| {
            json!({
                "price_start": band.price_start,
                "price_end": band.price_end,
                "level_count": band.level_count,
                "volume_sum": band.volume_sum,
                "delta_sum": band.delta_sum,
                "buy_sum": band.buy_sum,
                "sell_sum": band.sell_sum,
                "imbalance_buy_count": band.imbalance_buy_count,
                "imbalance_sell_count": band.imbalance_sell_count,
                "hvn_count": band.hvn_count,
                "lvn_count": band.lvn_count,
                "max_total_level_price": band.max_total_level_price,
                "max_abs_delta_level_price": band.max_abs_delta_level_price,
            })
        })
        .collect::<Vec<_>>();

    json!({
        "band_size": band_size,
        "band_count": bands.len(),
        "source_band_count": source_band_count,
        "source_level_count": source_level_count,
        "bands": bands,
    })
}

fn trim_price_band_entries(
    mut entries: Vec<(i64, PriceBandAgg)>,
    reference_price: Option<f64>,
    band_size: f64,
    keep: usize,
) -> Vec<(i64, PriceBandAgg)> {
    if entries.len() <= keep {
        return entries;
    }

    if let Some(reference) = reference_price.filter(|v| v.is_finite()) {
        let ref_idx = (reference / band_size).floor() as i64;
        entries.sort_by(|a, b| {
            (a.0 - ref_idx)
                .abs()
                .cmp(&(b.0 - ref_idx).abs())
                .then_with(|| a.0.cmp(&b.0))
        });
    } else {
        entries.sort_by(|a, b| {
            b.1.level_count
                .cmp(&a.1.level_count)
                .then_with(|| a.0.cmp(&b.0))
        });
    }
    entries.truncate(keep);
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    entries
}

/// Compress event-type indicators (absorption, exhaustion, initiation).
/// Events from the last 24 h are kept in full.
/// Older events are aggregated per calendar day into a compact summary so the
/// LLM can still see historical context without the full event list.
fn compact_event_payload(payload: &mut Map<String, Value>) {
    let Some(events_val) = payload.remove("events") else {
        return;
    };
    let Some(events) = events_val.as_array() else {
        payload.insert("events".to_string(), events_val);
        return;
    };
    if events.is_empty() {
        payload.insert("events".to_string(), Value::Array(Vec::new()));
        return;
    }

    let cutoff = Utc::now() - chrono::Duration::hours(24);

    let mut recent: Vec<Value> = Vec::new();
    // day_key → (count, score_sum, best_score, best_event_summary)
    let mut daily: std::collections::BTreeMap<String, (usize, f64, f64, Value)> =
        std::collections::BTreeMap::new();

    for event in events {
        let ts_str = event
            .get("start_ts")
            .and_then(Value::as_str)
            .or_else(|| event.get("event_start_ts").and_then(Value::as_str))
            .unwrap_or("");
        let is_recent = parse_rfc3339_to_utc(ts_str)
            .map(|t| t >= cutoff)
            .unwrap_or(true); // unknown ts → keep full

        if is_recent {
            recent.push(event.clone());
        } else {
            // Derive a YYYY-MM-DD day key from the ts string (first 10 chars).
            let day_key = if ts_str.len() >= 10 {
                ts_str[..10].to_string()
            } else {
                "unknown".to_string()
            };
            let score = event.get("score").and_then(Value::as_f64).unwrap_or(0.0);
            let entry = daily
                .entry(day_key)
                .or_insert_with(|| (0, 0.0, f64::NEG_INFINITY, Value::Null));
            entry.0 += 1;
            entry.1 += score;
            if score > entry.2 {
                entry.2 = score;
                // Compact summary of the strongest event for that day.
                let mut summary = Map::new();
                if let Some(v) = event.get("start_ts") {
                    summary.insert("start_ts".to_string(), v.clone());
                }
                if let Some(v) = event.get("direction") {
                    summary.insert("direction".to_string(), v.clone());
                }
                if let Some(v) = event.get("pivot_price") {
                    summary.insert("pivot_price".to_string(), v.clone());
                }
                if let Some(v) = event.get("score") {
                    summary.insert("score".to_string(), v.clone());
                }
                if let Some(v) = event.get("type") {
                    summary.insert("type".to_string(), v.clone());
                }
                entry.3 = Value::Object(summary);
            }
        }
    }

    // Rebuild: compressed daily summaries first (oldest→newest), then full recent events.
    let mut result: Vec<Value> = daily
        .into_iter()
        .map(|(day, (count, score_sum, _best_score, best_event))| {
            let avg_score = if count > 0 {
                score_sum / count as f64
            } else {
                0.0
            };
            json!({
                "_compressed_day": day,
                "event_count": count,
                "avg_score": avg_score,
                "strongest_event": best_event,
            })
        })
        .collect();
    result.extend(recent);

    payload.insert("event_count".to_string(), json!(result.len()));
    payload.insert("events".to_string(), Value::Array(result));
}

/// Strips the heavy `ffill_series_by_output_window` time-series from ema_trend_regime.
/// Keeps all current scalar EMAs and the `trend_regime` state map — exactly what
/// strategies 2/8/10 need for directional anchoring.
fn compact_ema_trend_regime_payload(payload: &mut Map<String, Value>) {
    payload.remove("ffill_series_by_output_window");
}

/// Strips `dev_series` (developing TPO bar history) from every session inside `by_session`.
/// Keeps the session-level TPO poc/vah/val scalars plus top-level `tpo_vah/val/poc`
/// and `tpo_single_print_zones` — required by strategies 4 (unfinished auction) and
/// 6 (value area re-fill).
fn compact_tpo_market_profile_payload(payload: &mut Map<String, Value>) {
    if let Some(by_session) = payload.get_mut("by_session").and_then(Value::as_object_mut) {
        for session_payload in by_session.values_mut() {
            if let Some(session_obj) = session_payload.as_object_mut() {
                session_obj.remove("dev_series");
            }
        }
    }
}

/// Strips time-series arrays inside each `by_window` entry of rvwap_sigma_bands,
/// keeping only the current scalar band values (rvwap_w, band_±1/±2, sigma, z-score).
/// Strategies 6 and 10 need band values for precise entry triggers.
fn compact_rvwap_sigma_bands_payload(payload: &mut Map<String, Value>) {
    if let Some(by_window) = payload.get_mut("by_window").and_then(Value::as_object_mut) {
        for window_payload in by_window.values_mut() {
            if let Some(window_obj) = window_payload.as_object_mut() {
                window_obj.remove("series");
            }
        }
    }

    // Trim series_by_output_window: keep last 15 bars per output window,
    // reduce each bar to {ts, z: {15m, 4h, 1d}} for S10 z-score direction.
    if let Some(sbw) = payload
        .get_mut("series_by_output_window")
        .and_then(Value::as_object_mut)
    {
        for series in sbw.values_mut() {
            let Some(arr) = series.as_array_mut() else {
                continue;
            };
            // Sort ascending by ts and keep the most recent 15 points.
            arr.sort_by(|a, b| {
                let ta = a.get("ts").and_then(Value::as_str).unwrap_or("");
                let tb = b.get("ts").and_then(Value::as_str).unwrap_or("");
                ta.cmp(tb)
            });
            let skip = arr.len().saturating_sub(15);
            let trimmed: Vec<Value> = arr
                .drain(skip..)
                .map(|pt| {
                    // Reduce to {ts, z: {15m, 4h, 1d}} to minimise payload.
                    let ts = pt.get("ts").cloned().unwrap_or(Value::Null);
                    let mut z = Map::new();
                    if let Some(bw) = pt.get("by_window").and_then(Value::as_object) {
                        for win in ["15m", "4h", "1d"] {
                            if let Some(z_val) = bw
                                .get(win)
                                .and_then(|w| w.get("z_price_minus_rvwap"))
                                .cloned()
                            {
                                z.insert(win.to_string(), z_val);
                            }
                        }
                    }
                    json!({ "ts": ts, "z": Value::Object(z) })
                })
                .collect();
            *series = Value::Array(trimmed);
        }
    }
}

/// Strips `intrabar_poc_max_by_window` (per-window POC history series) from
/// high_volume_pulse, keeping `by_z_window` boolean spike flags and the current
/// bar's `intrabar_poc_price/volume` — sufficient for strategy 9 volume confirmation.
fn compact_high_volume_pulse_payload(payload: &mut Map<String, Value>) {
    payload.remove("intrabar_poc_max_by_window");
}

fn build_core_price_anchors(indicators: &mut Map<String, Value>, reference_price: Option<f64>) {
    let mut anchors = Map::new();
    anchors.insert("reference_price".to_string(), json!(reference_price));

    if let Some(pvs_payload) = indicator_payload_ref(indicators, "price_volume_structure") {
        let mut pvs = Map::new();
        if let Some(v) = pvs_payload.get("poc_price") {
            pvs.insert("poc".to_string(), v.clone());
        }
        if let Some(v) = pvs_payload.get("val") {
            pvs.insert("val".to_string(), v.clone());
        }
        if let Some(v) = pvs_payload.get("vah") {
            pvs.insert("vah".to_string(), v.clone());
        }
        if let Some(levels) = pvs_payload.get("hvn_levels").and_then(Value::as_array) {
            pvs.insert(
                "hvn_top".to_string(),
                Value::Array(select_numeric_levels_near_reference(
                    levels,
                    reference_price,
                    LLM_CORE_ANCHOR_TOP_N,
                )),
            );
        }
        if let Some(levels) = pvs_payload.get("lvn_levels").and_then(Value::as_array) {
            pvs.insert(
                "lvn_top".to_string(),
                Value::Array(select_numeric_levels_near_reference(
                    levels,
                    reference_price,
                    LLM_CORE_ANCHOR_TOP_N,
                )),
            );
        }
        if !pvs.is_empty() {
            anchors.insert("pvs".to_string(), Value::Object(pvs));
        }
    }

    if let Some(footprint_payload) = indicator_payload_ref(indicators, "footprint") {
        if let Some(window_4h) = footprint_payload
            .get("by_window")
            .and_then(Value::as_object)
            .and_then(|by_window| by_window.get("4h"))
            .and_then(Value::as_object)
        {
            let mut footprint = Map::new();
            if let Some(v) = window_4h.get("ua_top") {
                footprint.insert("ua_top".to_string(), v.clone());
            }
            if let Some(v) = window_4h.get("ua_bottom") {
                footprint.insert("ua_bottom".to_string(), v.clone());
            }
            for (src, dst) in [
                ("buy_imbalance_zone_nearest_above", "nearest_buy_zone_above"),
                ("buy_imbalance_zone_nearest_below", "nearest_buy_zone_below"),
                (
                    "sell_imbalance_zone_nearest_above",
                    "nearest_sell_zone_above",
                ),
                (
                    "sell_imbalance_zone_nearest_below",
                    "nearest_sell_zone_below",
                ),
            ] {
                if let Some(v) = window_4h.get(src) {
                    footprint.insert(dst.to_string(), v.clone());
                }
            }
            if !footprint.is_empty() {
                anchors.insert("footprint".to_string(), Value::Object(footprint));
            }
        }
    }

    if let Some(orderbook_payload) = indicator_payload_ref(indicators, "orderbook_depth") {
        let mut orderbook = Map::new();
        let top_bid = orderbook_payload
            .get("top_bid_walls")
            .and_then(Value::as_array)
            .and_then(|arr| arr.first())
            .cloned()
            .unwrap_or(Value::Null);
        let top_ask = orderbook_payload
            .get("top_ask_walls")
            .and_then(Value::as_array)
            .and_then(|arr| arr.first())
            .cloned()
            .unwrap_or(Value::Null);
        orderbook.insert("top_bid_wall".to_string(), top_bid);
        orderbook.insert("top_ask_wall".to_string(), top_ask);
        anchors.insert("orderbook".to_string(), Value::Object(orderbook));
    }

    if let Some(liq_payload) = indicator_payload_ref(indicators, "liquidation_density") {
        let mut liquidation = Map::new();
        if let Some(peak_levels) = liq_payload.get("peak_levels").and_then(Value::as_array) {
            liquidation.insert(
                "peak_long".to_string(),
                select_peak_by_metric(peak_levels, &["long", "long_notional", "long_qty"])
                    .unwrap_or(Value::Null),
            );
            liquidation.insert(
                "peak_short".to_string(),
                select_peak_by_metric(peak_levels, &["short", "short_notional", "short_qty"])
                    .unwrap_or(Value::Null),
            );
        }
        if let Some(v) = liq_payload.get("long_total") {
            liquidation.insert("long_total".to_string(), v.clone());
        }
        if let Some(v) = liq_payload.get("short_total") {
            liquidation.insert("short_total".to_string(), v.clone());
        }
        if !liquidation.is_empty() {
            anchors.insert("liquidation".to_string(), Value::Object(liquidation));
        }
    }

    indicators.insert("core_price_anchors".to_string(), Value::Object(anchors));
}

fn select_numeric_levels_near_reference(
    levels: &[Value],
    reference_price: Option<f64>,
    keep: usize,
) -> Vec<Value> {
    let mut nums = levels
        .iter()
        .filter_map(Value::as_f64)
        .filter(|v| v.is_finite())
        .collect::<Vec<_>>();
    if nums.is_empty() {
        return Vec::new();
    }
    if let Some(reference) = reference_price.filter(|v| v.is_finite()) {
        nums.sort_by(|a, b| {
            (a - reference)
                .abs()
                .partial_cmp(&(b - reference).abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        nums.truncate(keep);
        nums.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    } else {
        nums.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        nums.truncate(keep);
    }
    nums.into_iter().map(Value::from).collect()
}

fn select_peak_by_metric(levels: &[Value], metric_keys: &[&str]) -> Option<Value> {
    let mut ranked = levels
        .iter()
        .filter_map(|level| {
            let obj = level.as_object()?;
            let metric = metric_keys
                .iter()
                .find_map(|key| obj.get(*key).and_then(Value::as_f64))?;
            Some((metric, level.clone()))
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    ranked.into_iter().next().map(|(_, level)| level)
}

fn apply_llm_token_governor(indicators: &mut Map<String, Value>) {
    if serialized_indicator_size(indicators) <= LLM_INPUT_INDICATORS_BUDGET_BYTES {
        return;
    }
    governor_trim_15m(indicators);
    if serialized_indicator_size(indicators) <= LLM_INPUT_INDICATORS_BUDGET_BYTES {
        return;
    }
    governor_trim_4h(indicators);
    if serialized_indicator_size(indicators) <= LLM_INPUT_INDICATORS_BUDGET_BYTES {
        return;
    }
    governor_trim_1d(indicators);
}

fn governor_trim_15m(indicators: &mut Map<String, Value>) {
    if let Some(payload) = by_window_payload_mut(indicators, "footprint", "15m") {
        truncate_array_field(payload, "buy_imbalance_zones_near_price", 12);
        truncate_array_field(payload, "sell_imbalance_zones_near_price", 12);
        truncate_array_field(payload, "buy_imbalance_zones_top", 12);
        truncate_array_field(payload, "sell_imbalance_zones_top", 12);
        truncate_array_field(payload, "buy_stacks_top", 8);
        truncate_array_field(payload, "sell_stacks_top", 8);
        truncate_array_field(payload, "top_levels_by_total", 8);
        truncate_array_field(payload, "top_levels_by_abs_delta", 8);
    }
    if let Some(payload) = by_window_payload_mut(indicators, "price_volume_structure", "15m") {
        truncate_array_field(payload, "top_volume_levels", 6);
        truncate_array_field(payload, "top_abs_delta_levels", 6);
        truncate_array_field(payload, "top_value_area_levels", 6);
    }
    truncate_kline_bars(indicators, "15m", 48);
}

fn governor_trim_1d(indicators: &mut Map<String, Value>) {
    if let Some(payload) = by_window_payload_mut(indicators, "footprint", "1d") {
        truncate_array_field(payload, "buy_imbalance_zones_top_strength", 16);
        truncate_array_field(payload, "sell_imbalance_zones_top_strength", 16);
        truncate_array_field(payload, "buy_imbalance_zones_top", 16);
        truncate_array_field(payload, "sell_imbalance_zones_top", 16);
        truncate_array_field(payload, "buy_stacks_top", 10);
        truncate_array_field(payload, "sell_stacks_top", 10);
        truncate_array_field(payload, "top_levels_by_total", 8);
        truncate_array_field(payload, "top_levels_by_abs_delta", 8);
    }
    if let Some(payload) = by_window_payload_mut(indicators, "price_volume_structure", "1d") {
        truncate_array_field(payload, "top_volume_levels", 6);
        truncate_array_field(payload, "top_abs_delta_levels", 6);
        truncate_array_field(payload, "top_value_area_levels", 6);
    }
    truncate_kline_bars(indicators, "1d", 20);
}

fn governor_trim_4h(indicators: &mut Map<String, Value>) {
    if let Some(payload) = by_window_payload_mut(indicators, "footprint", "4h") {
        truncate_array_field(
            payload,
            "buy_imbalance_zones_top_strength",
            LLM_FOOTPRINT_ZONE_STRENGTH_TOP_1D,
        );
        truncate_array_field(
            payload,
            "sell_imbalance_zones_top_strength",
            LLM_FOOTPRINT_ZONE_STRENGTH_TOP_1D,
        );
        truncate_array_field(payload, "buy_imbalance_zones_near_price", 80);
        truncate_array_field(payload, "sell_imbalance_zones_near_price", 80);
        truncate_array_field(payload, "buy_imbalance_zones_top", 80);
        truncate_array_field(payload, "sell_imbalance_zones_top", 80);
        truncate_bucketed_bands(payload, "levels_near_price_bucketed", 80);
        truncate_bucketed_bands(payload, "levels_in_va_corridor_bucketed", 80);
        truncate_array_field(payload, "buy_stacks_top", 24);
        truncate_array_field(payload, "sell_stacks_top", 24);
    }
    if let Some(payload) = by_window_payload_mut(indicators, "price_volume_structure", "4h") {
        truncate_bucketed_bands(payload, "levels_near_price_bucketed", 80);
        truncate_bucketed_bands(payload, "levels_in_va_corridor_bucketed", 80);
        truncate_array_field(payload, "top_volume_levels", 16);
        truncate_array_field(payload, "top_abs_delta_levels", 16);
        truncate_array_field(payload, "top_value_area_levels", 12);
    }
    truncate_kline_bars(indicators, "4h", 40);
}

fn serialized_indicator_size(indicators: &Map<String, Value>) -> usize {
    serde_json::to_vec(indicators)
        .map(|buf| buf.len())
        .unwrap_or(usize::MAX)
}

fn by_window_payload_mut<'a>(
    indicators: &'a mut Map<String, Value>,
    code: &str,
    window: &str,
) -> Option<&'a mut Map<String, Value>> {
    indicators
        .get_mut(code)
        .and_then(Value::as_object_mut)
        .and_then(|indicator| indicator.get_mut("payload"))
        .and_then(Value::as_object_mut)
        .and_then(|payload| payload.get_mut("by_window"))
        .and_then(Value::as_object_mut)
        .and_then(|by_window| by_window.get_mut(window))
        .and_then(Value::as_object_mut)
}

fn truncate_kline_bars(indicators: &mut Map<String, Value>, interval: &str, max_len: usize) {
    let Some(bars) = indicators
        .get_mut("kline_history")
        .and_then(Value::as_object_mut)
        .and_then(|indicator| indicator.get_mut("payload"))
        .and_then(Value::as_object_mut)
        .and_then(|payload| payload.get_mut("intervals"))
        .and_then(Value::as_object_mut)
        .and_then(|intervals| intervals.get_mut(interval))
        .and_then(Value::as_object_mut)
        .and_then(|interval_obj| interval_obj.get_mut("futures"))
        .and_then(Value::as_object_mut)
        .and_then(|futures| futures.get_mut("bars"))
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    if bars.len() > max_len {
        let drop_count = bars.len() - max_len;
        bars.drain(0..drop_count);
    }
}

fn truncate_array_field(payload: &mut Map<String, Value>, key: &str, max_len: usize) {
    let Some(arr) = payload.get_mut(key).and_then(Value::as_array_mut) else {
        return;
    };
    if arr.len() > max_len {
        arr.truncate(max_len);
    }
}

fn truncate_bucketed_bands(payload: &mut Map<String, Value>, key: &str, max_len: usize) {
    let Some(obj) = payload.get_mut(key).and_then(Value::as_object_mut) else {
        return;
    };
    let Some(arr) = obj.get_mut("bands").and_then(Value::as_array_mut) else {
        return;
    };
    if arr.len() > max_len {
        arr.truncate(max_len);
    }
    let band_count = arr.len();
    obj.insert("band_count".to_string(), json!(band_count));
}

fn select_levels_in_price_range(levels: &[Value], p1: f64, p2: f64) -> Vec<Value> {
    let lo = p1.min(p2);
    let hi = p1.max(p2);
    let mut out = levels
        .iter()
        .filter(|level| {
            level
                .get("price_level")
                .and_then(Value::as_f64)
                .map(|price| price >= lo && price <= hi)
                .unwrap_or(false)
        })
        .cloned()
        .collect::<Vec<_>>();
    out.sort_by(|a, b| {
        let lhs = a
            .get("price_level")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        let rhs = b
            .get("price_level")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        lhs.partial_cmp(&rhs).unwrap_or(std::cmp::Ordering::Equal)
    });
    out
}

fn select_levels_near_price(levels: &[Value], reference_price: f64, pct_band: f64) -> Vec<Value> {
    let band = (reference_price.abs() * pct_band).max(1.0);
    let mut out = levels
        .iter()
        .filter(|level| {
            level
                .get("price_level")
                .and_then(Value::as_f64)
                .map(|price| (price - reference_price).abs() <= band)
                .unwrap_or(false)
        })
        .cloned()
        .collect::<Vec<_>>();
    if out.is_empty() {
        let mut ranked = levels
            .iter()
            .filter_map(|level| {
                let price = level.get("price_level").and_then(Value::as_f64)?;
                Some(((price - reference_price).abs(), level.clone()))
            })
            .collect::<Vec<_>>();
        ranked.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        ranked.truncate(64);
        out = ranked.into_iter().map(|(_, level)| level).collect();
    }
    out.sort_by(|a, b| {
        let lhs = a
            .get("price_level")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        let rhs = b
            .get("price_level")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        lhs.partial_cmp(&rhs).unwrap_or(std::cmp::Ordering::Equal)
    });
    out
}

fn select_nearest_zones(zones: &[Value], reference_price: f64, max_items: usize) -> Vec<Value> {
    let mut ranked = zones
        .iter()
        .filter_map(|zone| {
            let start = zone.get("start_price").and_then(Value::as_f64)?;
            let end = zone.get("end_price").and_then(Value::as_f64)?;
            let distance = if reference_price < start {
                start - reference_price
            } else if reference_price > end {
                reference_price - end
            } else {
                0.0
            };
            Some((distance, start, zone.clone()))
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|a, b| {
        a.0.partial_cmp(&b.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
    });
    ranked.truncate(max_items);
    ranked.into_iter().map(|(_, _, zone)| zone).collect()
}

fn nearest_zone_above(zones: &[Value], reference_price: f64) -> Option<Value> {
    let mut candidates = zones
        .iter()
        .filter_map(|zone| {
            let start = zone.get("start_price").and_then(Value::as_f64)?;
            let end = zone.get("end_price").and_then(Value::as_f64)?;
            let key = if reference_price <= start {
                start
            } else if reference_price <= end {
                reference_price
            } else {
                return None;
            };
            Some((key, zone.clone()))
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    candidates.into_iter().next().map(|(_, zone)| zone)
}

fn nearest_zone_below(zones: &[Value], reference_price: f64) -> Option<Value> {
    let mut candidates = zones
        .iter()
        .filter_map(|zone| {
            let start = zone.get("start_price").and_then(Value::as_f64)?;
            let end = zone.get("end_price").and_then(Value::as_f64)?;
            let key = if reference_price >= end {
                end
            } else if reference_price >= start {
                reference_price
            } else {
                return None;
            };
            Some((key, zone.clone()))
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    candidates.into_iter().next().map(|(_, zone)| zone)
}

/// Add an `intact` boolean to each zone.
/// Buy zones: intact when price is still ABOVE the zone (zone hasn't been retested from below).
/// Sell zones: intact when price is still BELOW the zone (zone hasn't been retested from above).
fn annotate_zones_intact(zones: Vec<Value>, reference_price: f64, is_buy: bool) -> Vec<Value> {
    zones
        .into_iter()
        .map(|mut zone| {
            if let Some(obj) = zone.as_object_mut() {
                let intact = if is_buy {
                    // Buy zone is intact if price is above the zone top (end_price)
                    obj.get("end_price")
                        .and_then(Value::as_f64)
                        .map(|end| reference_price > end)
                        .unwrap_or(false)
                } else {
                    // Sell zone is intact if price is below the zone bottom (start_price)
                    obj.get("start_price")
                        .and_then(Value::as_f64)
                        .map(|start| reference_price < start)
                        .unwrap_or(false)
                };
                obj.insert("intact".to_string(), json!(intact));
            }
            zone
        })
        .collect()
}

fn select_top_levels_by_metric(
    levels: &[Value],
    metric_key: &str,
    max_items: usize,
    by_abs: bool,
) -> Vec<Value> {
    let mut ranked = levels
        .iter()
        .filter_map(|level| {
            let metric = level.get(metric_key).and_then(Value::as_f64)?;
            Some((metric, level.clone()))
        })
        .collect::<Vec<_>>();

    ranked.sort_by(|a, b| {
        let lhs = if by_abs { a.0.abs() } else { a.0 };
        let rhs = if by_abs { b.0.abs() } else { b.0 };
        rhs.partial_cmp(&lhs).unwrap_or(std::cmp::Ordering::Equal)
    });
    ranked.truncate(max_items);
    ranked.into_iter().map(|(_, level)| level).collect()
}

fn compact_price_points_to_zones(payload: &mut Map<String, Value>, source_key: &str, prefix: &str) {
    let Some(points) = payload
        .remove(source_key)
        .and_then(|value| value.as_array().cloned())
    else {
        return;
    };

    payload.insert(format!("{}_count", prefix), json!(points.len()));
    payload.insert(
        format!("{}_zones", prefix),
        Value::Array(price_points_to_zones(&points)),
    );
}

fn price_points_to_zones(points: &[Value]) -> Vec<Value> {
    let mut prices = points
        .iter()
        .filter_map(Value::as_f64)
        .filter(|price| price.is_finite())
        .collect::<Vec<_>>();
    if prices.is_empty() {
        return Vec::new();
    }

    prices.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mut zones = Vec::new();
    let mut start = prices[0];
    let mut prev = prices[0];
    let mut count = 1_usize;

    for price in prices.into_iter().skip(1) {
        if (price - prev).abs() <= 0.011 {
            prev = price;
            count += 1;
            continue;
        }

        zones.push(json!({
            "start_price": start,
            "end_price": prev,
            "count": count,
        }));
        start = price;
        prev = price;
        count = 1;
    }

    zones.push(json!({
        "start_price": start,
        "end_price": prev,
        "count": count,
    }));
    zones
}

fn infer_reference_price(levels: &[Value]) -> Option<f64> {
    let mut prices = levels
        .iter()
        .filter_map(|level| level.get("price_level").and_then(Value::as_f64))
        .filter(|price| price.is_finite())
        .collect::<Vec<_>>();
    if prices.is_empty() {
        return None;
    }

    prices.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = prices.len() / 2;
    if prices.len() % 2 == 1 {
        Some(prices[mid])
    } else {
        Some((prices[mid - 1] + prices[mid]) / 2.0)
    }
}

fn build_depth_bands(levels: &[Value], reference_price: Option<f64>) -> Vec<Value> {
    let Some(reference_price) = reference_price.filter(|price| *price > 0.0) else {
        return Vec::new();
    };

    struct BandAccumulator {
        label: &'static str,
        min_pct: f64,
        max_pct: f64,
        bid_liquidity: f64,
        ask_liquidity: f64,
        level_count: usize,
        peak_bid_level: Option<(f64, f64)>,
        peak_ask_level: Option<(f64, f64)>,
    }

    let mut bands = vec![
        BandAccumulator {
            label: "0-0.10%",
            min_pct: 0.0,
            max_pct: 0.10,
            bid_liquidity: 0.0,
            ask_liquidity: 0.0,
            level_count: 0,
            peak_bid_level: None,
            peak_ask_level: None,
        },
        BandAccumulator {
            label: "0.10-0.25%",
            min_pct: 0.10,
            max_pct: 0.25,
            bid_liquidity: 0.0,
            ask_liquidity: 0.0,
            level_count: 0,
            peak_bid_level: None,
            peak_ask_level: None,
        },
        BandAccumulator {
            label: "0.25-0.50%",
            min_pct: 0.25,
            max_pct: 0.50,
            bid_liquidity: 0.0,
            ask_liquidity: 0.0,
            level_count: 0,
            peak_bid_level: None,
            peak_ask_level: None,
        },
        BandAccumulator {
            label: "0.50-1.00%",
            min_pct: 0.50,
            max_pct: 1.00,
            bid_liquidity: 0.0,
            ask_liquidity: 0.0,
            level_count: 0,
            peak_bid_level: None,
            peak_ask_level: None,
        },
        BandAccumulator {
            label: "1.00-2.00%",
            min_pct: 1.00,
            max_pct: 2.00,
            bid_liquidity: 0.0,
            ask_liquidity: 0.0,
            level_count: 0,
            peak_bid_level: None,
            peak_ask_level: None,
        },
    ];

    for level in levels {
        let Some(price_level) = level.get("price_level").and_then(Value::as_f64) else {
            continue;
        };
        let distance_pct = ((price_level - reference_price).abs() / reference_price) * 100.0;
        let Some(band) = bands.iter_mut().find(|band| {
            distance_pct >= band.min_pct
                && (distance_pct < band.max_pct
                    || (band.max_pct - 2.00).abs() < f64::EPSILON && distance_pct <= band.max_pct)
        }) else {
            continue;
        };

        let bid_liquidity = level
            .get("bid_liquidity")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let ask_liquidity = level
            .get("ask_liquidity")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);

        band.level_count += 1;
        band.bid_liquidity += bid_liquidity;
        band.ask_liquidity += ask_liquidity;

        if band
            .peak_bid_level
            .map(|(_, liq)| bid_liquidity > liq)
            .unwrap_or(bid_liquidity > 0.0)
        {
            band.peak_bid_level = Some((price_level, bid_liquidity));
        }
        if band
            .peak_ask_level
            .map(|(_, liq)| ask_liquidity > liq)
            .unwrap_or(ask_liquidity > 0.0)
        {
            band.peak_ask_level = Some((price_level, ask_liquidity));
        }
    }

    bands
        .into_iter()
        .filter(|band| band.level_count > 0)
        .map(|band| {
            json!({
                "band": band.label,
                "level_count": band.level_count,
                "bid_liquidity": band.bid_liquidity,
                "ask_liquidity": band.ask_liquidity,
                "total_liquidity": band.bid_liquidity + band.ask_liquidity,
                "net_liquidity": band.bid_liquidity - band.ask_liquidity,
                "peak_bid_level": band.peak_bid_level.map(|(price_level, bid_liquidity)| json!({
                    "price_level": price_level,
                    "bid_liquidity": bid_liquidity,
                })),
                "peak_ask_level": band.peak_ask_level.map(|(price_level, ask_liquidity)| json!({
                    "price_level": price_level,
                    "ask_liquidity": ask_liquidity,
                })),
            })
        })
        .collect()
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
    trigger: &str,
    management_mode: bool,
    pending_order_mode: bool,
    input: &ModelInvocationInput,
    retention_minutes: u64,
) -> Result<PathBuf> {
    ensure_temp_model_input_dir().await?;

    let input_json = serialize_llm_input_minified(input).context("serialize llm model input")?;
    let raw_json: Value =
        serde_json::from_str(&input_json).context("parse llm model input as json")?;
    let pretty =
        serde_json::to_vec_pretty(&raw_json).context("serialize llm model input pretty json")?;
    let path = llm_model_input_path(bundle, trigger, management_mode, pending_order_mode);

    let mut file = fs::File::create(&path).with_context(|| format!("create {}", path.display()))?;
    file.write_all(&pretty)
        .with_context(|| format!("write {}", path.display()))?;
    file.flush()
        .with_context(|| format!("flush {}", path.display()))?;
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

    Ok(path)
}

fn retention_minutes_i64(retention_minutes: u64) -> i64 {
    i64::try_from(retention_minutes).unwrap_or(i64::MAX)
}

fn minute_bundle_path(bundle: &MinuteBundleEnvelope) -> PathBuf {
    let ts = bundle.ts_bucket.format("%Y%m%dT%H%M%SZ").to_string();
    let symbol = sanitize_filename_component(&bundle.symbol);
    Path::new(TEMP_INDICATOR_DIR).join(format!("{}_{}.json", ts, symbol))
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

fn llm_model_input_path(
    bundle: &MinuteBundleEnvelope,
    trigger: &str,
    management_mode: bool,
    pending_order_mode: bool,
) -> PathBuf {
    let bucket_ts = bundle.ts_bucket.format("%Y%m%dT%H%M%SZ").to_string();
    let invoke_ts = Utc::now()
        .format("%Y%m%dT%H%M%S%.3fZ")
        .to_string()
        .replace('.', "");
    let symbol = sanitize_filename_component(&bundle.symbol);
    let trigger = sanitize_filename_component(trigger);
    let mode = if pending_order_mode {
        "pending_management"
    } else if management_mode {
        "management"
    } else {
        "entry"
    };
    Path::new(TEMP_MODEL_INPUT_DIR).join(format!(
        "{}_{}_{}_{}_{}.json",
        bucket_ts, symbol, mode, trigger, invoke_ts
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

    #[test]
    fn compact_sample_input_size_snapshot() {
        let path = Path::new("/data/systems/llm/temp_indicator/20260304T055500Z_ETHUSDT.json");
        if !path.exists() {
            return;
        }

        let raw = fs::read_to_string(path).expect("read sample indicator file");
        let root: Value = serde_json::from_str(&raw).expect("parse sample indicator json");
        let indicators = root.get("indicators").cloned().expect("indicators field");
        let required = indicators
            .as_object()
            .expect("indicators object")
            .keys()
            .cloned()
            .collect::<Vec<_>>();

        let (filtered, missing) = filter_indicators(&indicators, &required);
        assert!(missing.is_empty(), "missing indicators in sample");

        let compact_root = json!({ "indicators": filtered });
        let compact_minified = serde_json::to_vec(&compact_root).expect("serialize compact");
        eprintln!("compact_minified_bytes={}", compact_minified.len());
        assert!(!compact_minified.is_empty());
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
                entry_v: None,
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
            dir.join("20260307T105900Z_ETHUSDT_entry_scheduled_bundle_20260307T110001000Z.json"),
            "{}",
        )
        .expect("write old file");
        fs::write(
            dir.join(
                "20260307T110000Z_ETHUSDT_management_scheduled_bundle_20260307T110101000Z.json",
            ),
            "{}",
        )
        .expect("write edge file");
        fs::write(
            dir.join("20260307T111500Z_ETHUSDT_pending_management_scheduled_bundle_20260307T111601000Z.json"),
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
            .join("20260307T105900Z_ETHUSDT_entry_scheduled_bundle_20260307T110001000Z.json")
            .exists());
        assert!(dir
            .join("20260307T110000Z_ETHUSDT_management_scheduled_bundle_20260307T110101000Z.json")
            .exists());
        assert!(dir
            .join("20260307T111500Z_ETHUSDT_pending_management_scheduled_bundle_20260307T111601000Z.json")
            .exists());
        assert!(dir.join("not_a_model_input.json").exists());
        assert!(dir.join(".gitignore").exists());

        fs::remove_dir_all(&dir).expect("cleanup temp model input dir");
    }
}
