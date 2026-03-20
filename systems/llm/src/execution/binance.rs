use crate::app::config::{BinanceApiConfig, LlmExecutionConfig};
use crate::llm::decision::{
    PendingOrderManagementDecision, PendingOrderManagementIntent, PositionManagementDecision,
    PositionManagementIntent, TradeDecision, TradeIntent,
};
use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use futures_util::StreamExt;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;
const ACCOUNT_WS_RECONNECT_DELAY_SECS: u64 = 3;
const ACCOUNT_WS_LOG_VALUE_PREVIEW_CHARS: usize = 240;
const ACCOUNT_WS_BALANCE_SUMMARY_LIMIT: usize = 4;
const ACCOUNT_WS_POSITION_SUMMARY_LIMIT: usize = 4;
const POST_ONLY_MAKER_REPRICE_RETRY_COUNT: usize = 3;
static ACCOUNT_WS_LISTENER_STARTED: AtomicBool = AtomicBool::new(false);
static ACCOUNT_WS_STATE: OnceLock<Arc<Mutex<AccountWsState>>> = OnceLock::new();

#[derive(Debug, Clone)]
pub struct ExecutionReport {
    pub decision: &'static str,
    pub quantity: String,
    pub leverage: u32,
    pub leverage_source: &'static str,
    pub margin_budget_usdt: f64,
    pub margin_budget_source: &'static str,
    pub account_total_wallet_balance: f64,
    pub account_available_balance: f64,
    pub position_side: &'static str,
    pub dry_run: bool,
    pub entry_order_id: Option<i64>,
    pub take_profit_order_id: Option<i64>,
    pub stop_loss_order_id: Option<i64>,
    pub exit_orders_deferred: bool,
    pub maker_entry_price: f64,
    pub actual_take_profit: f64,
    pub actual_stop_loss: f64,
    pub actual_risk_reward_ratio: f64,
    pub best_bid_price: f64,
    pub best_ask_price: f64,
}

#[derive(Debug, Clone)]
pub struct TradeExecutionBlockedByCurrentPriceBeyondStopLoss {
    pub decision: TradeDecision,
    pub current_reference_price: f64,
    pub current_price_source: &'static str,
    pub entry_price: f64,
    pub stop_loss: f64,
    pub best_bid_price: f64,
    pub best_ask_price: f64,
}

impl fmt::Display for TradeExecutionBlockedByCurrentPriceBeyondStopLoss {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "trade entry blocked: current {}={} already beyond stop_loss={} for decision={} entry_price={} best_bid_price={} best_ask_price={}",
            self.current_price_source,
            self.current_reference_price,
            self.stop_loss,
            self.decision.as_str(),
            self.entry_price,
            self.best_bid_price,
            self.best_ask_price,
        )
    }
}

impl std::error::Error for TradeExecutionBlockedByCurrentPriceBeyondStopLoss {}

#[derive(Debug, Clone, Serialize)]
pub struct TradingStateSnapshot {
    pub symbol: String,
    pub has_active_context: bool,
    pub has_active_positions: bool,
    pub has_open_orders: bool,
    pub active_positions: Vec<ActivePositionSnapshot>,
    pub open_orders: Vec<OpenOrderSnapshot>,
    pub total_wallet_balance: f64,
    pub available_balance: f64,
}

impl TradingStateSnapshot {
    pub fn single_active_position(&self) -> Option<&ActivePositionSnapshot> {
        if self.active_positions.len() == 1 {
            self.active_positions.first()
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ActivePositionSnapshot {
    pub position_side: String,
    pub position_amt: f64,
    pub entry_price: f64,
    pub mark_price: f64,
    pub unrealized_pnl: f64,
    pub leverage: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenOrderSnapshot {
    pub order_id: i64,
    pub side: String,
    pub position_side: String,
    pub order_type: String,
    pub status: String,
    pub orig_qty: f64,
    pub executed_qty: f64,
    pub price: f64,
    pub stop_price: f64,
    pub close_position: bool,
    pub reduce_only: bool,
    #[serde(skip_serializing)]
    pub is_algo_order: bool,
}

#[derive(Debug, Clone)]
pub struct ManagementExecutionReport {
    pub action: &'static str,
    pub dry_run: bool,
    pub position_count: usize,
    pub open_order_count: usize,
    pub canceled_open_orders: bool,
    pub add_order_id: Option<i64>,
    pub reduce_order_ids: Vec<i64>,
    pub close_order_ids: Vec<i64>,
    pub modify_take_profit_order_ids: Vec<i64>,
    pub modify_stop_loss_order_ids: Vec<i64>,
    pub realized_pnl_usdt: f64,
}

#[derive(Debug, Clone)]
pub struct PendingOrderExecutionReport {
    pub action: &'static str,
    pub dry_run: bool,
    pub open_order_count: usize,
    pub canceled_open_orders: bool,
    pub replacement_order_id: Option<i64>,
    pub maker_entry_price: Option<f64>,
    pub effective_take_profit: Option<f64>,
    pub effective_stop_loss: Option<f64>,
    pub best_bid_price: Option<f64>,
    pub best_ask_price: Option<f64>,
    pub leverage: Option<u32>,
}

#[derive(Debug, Clone, Copy)]
struct TrackedExitOrder {
    order_id: i64,
    is_algo_order: bool,
}

#[derive(Default)]
struct AccountWsState {
    has_account_update: bool,
    total_wallet_balance: Option<f64>,
    available_balance: Option<f64>,
    positions: HashMap<(String, String), ActivePositionSnapshot>,
    flattened_at: HashMap<(String, String), Instant>,
}

fn account_ws_state() -> Arc<Mutex<AccountWsState>> {
    ACCOUNT_WS_STATE
        .get_or_init(|| Arc::new(Mutex::new(AccountWsState::default())))
        .clone()
}

fn ws_state_has_recent_flatten_event(
    state: &AccountWsState,
    symbol: &str,
    position_side: &str,
    watch_started_at: Instant,
) -> bool {
    state
        .flattened_at
        .iter()
        .any(|((sym, side), flattened_at)| {
            sym.eq_ignore_ascii_case(symbol)
                && side.eq_ignore_ascii_case(position_side)
                && *flattened_at >= watch_started_at
        })
}

fn has_recent_account_flatten_event(
    symbol: &str,
    position_side: &str,
    watch_started_at: Instant,
) -> bool {
    let state = account_ws_state();
    state.lock().ok().is_some_and(|guard| {
        ws_state_has_recent_flatten_event(&guard, symbol, position_side, watch_started_at)
    })
}

fn is_exit_like_order(order: &OpenOrderSnapshot) -> bool {
    order.reduce_only || order.close_position
}

fn is_entry_like_order(order: &OpenOrderSnapshot) -> bool {
    !is_exit_like_order(order)
        && (order.side.eq_ignore_ascii_case("BUY") || order.side.eq_ignore_ascii_case("SELL"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExitKind {
    TakeProfit,
    StopLoss,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActivePositionSource {
    AccountWs,
    Rest,
}

fn strict_hedge_position_side(value: &str) -> Option<&'static str> {
    let raw = value.trim();
    if raw.eq_ignore_ascii_case("LONG") {
        Some("LONG")
    } else if raw.eq_ignore_ascii_case("SHORT") {
        Some("SHORT")
    } else {
        None
    }
}

fn tracked_exit_order(order: &OpenOrderSnapshot) -> TrackedExitOrder {
    TrackedExitOrder {
        order_id: order.order_id,
        is_algo_order: order.is_algo_order,
    }
}

fn effective_order_position_side(
    order: &OpenOrderSnapshot,
    hedge_mode: bool,
) -> Option<&'static str> {
    if !hedge_mode {
        return Some("BOTH");
    }
    if let Some(side) = strict_hedge_position_side(&order.position_side) {
        return Some(side);
    }
    if is_entry_like_order(order) {
        if order.side.eq_ignore_ascii_case("BUY") {
            Some("LONG")
        } else if order.side.eq_ignore_ascii_case("SELL") {
            Some("SHORT")
        } else {
            None
        }
    } else if is_exit_like_order(order) {
        if order.side.eq_ignore_ascii_case("SELL") {
            Some("LONG")
        } else if order.side.eq_ignore_ascii_case("BUY") {
            Some("SHORT")
        } else {
            None
        }
    } else {
        None
    }
}

fn order_matches_position_side(
    order: &OpenOrderSnapshot,
    position_side: &str,
    hedge_mode: bool,
) -> bool {
    if !hedge_mode {
        return true;
    }
    effective_order_position_side(order, hedge_mode)
        .is_some_and(|side| side.eq_ignore_ascii_case(position_side))
}

fn collect_tracked_orders_for_position_side(
    open_orders: &[OpenOrderSnapshot],
    position_side: &str,
    hedge_mode: bool,
    include_entry_like: bool,
    include_exit_like: bool,
) -> Vec<TrackedExitOrder> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for order in open_orders {
        let include = (include_entry_like && is_entry_like_order(order))
            || (include_exit_like && is_exit_like_order(order));
        if !include || !order_matches_position_side(order, position_side, hedge_mode) {
            continue;
        }
        if seen.insert(order.order_id) {
            out.push(tracked_exit_order(order));
        }
    }
    out
}

fn exit_order_trigger_price(order: &OpenOrderSnapshot) -> Option<f64> {
    if order.stop_price > 0.0 {
        Some(order.stop_price)
    } else if order.price > 0.0 {
        Some(order.price)
    } else {
        None
    }
}

fn exact_order_type_matches_exit_kind(order_type: &str, exit_kind: ExitKind) -> bool {
    match exit_kind {
        ExitKind::TakeProfit => {
            order_type.eq_ignore_ascii_case("TAKE_PROFIT_MARKET")
                || order_type.eq_ignore_ascii_case("TAKE_PROFIT")
                || order_type.eq_ignore_ascii_case("TAKE_PROFIT_MARKET_ALGO")
        }
        ExitKind::StopLoss => {
            order_type.eq_ignore_ascii_case("STOP_MARKET")
                || order_type.eq_ignore_ascii_case("STOP")
                || order_type.eq_ignore_ascii_case("STOP_MARKET_ALGO")
        }
    }
}

fn price_matches_exit_kind(
    position_side: &str,
    entry_price: f64,
    candidate_price: f64,
    exit_kind: ExitKind,
) -> bool {
    if candidate_price <= 0.0 {
        return false;
    }
    if position_side.eq_ignore_ascii_case("LONG") {
        match exit_kind {
            ExitKind::TakeProfit => candidate_price > entry_price,
            ExitKind::StopLoss => candidate_price < entry_price,
        }
    } else if position_side.eq_ignore_ascii_case("SHORT") {
        match exit_kind {
            ExitKind::TakeProfit => candidate_price < entry_price,
            ExitKind::StopLoss => candidate_price > entry_price,
        }
    } else {
        false
    }
}

fn matching_exit_prices_for_order(
    order: &OpenOrderSnapshot,
    position_side: &str,
    entry_price: f64,
    exit_kind: ExitKind,
) -> Vec<f64> {
    if !is_exit_like_order(order) {
        return Vec::new();
    }
    let mut out: Vec<f64> = Vec::new();
    for candidate_price in [order.price, order.stop_price] {
        if price_matches_exit_kind(position_side, entry_price, candidate_price, exit_kind)
            && !out
                .iter()
                .any(|existing| (*existing - candidate_price).abs() < f64::EPSILON)
        {
            out.push(candidate_price);
        }
    }
    out
}

fn choose_preferred_exit_price(
    prices: impl IntoIterator<Item = f64>,
    entry_price: f64,
    position_side: &str,
    exit_kind: ExitKind,
) -> Option<f64> {
    let mut candidates = prices.into_iter().filter(|v| *v > 0.0).collect::<Vec<_>>();
    if candidates.is_empty() {
        return None;
    }
    match (position_side.eq_ignore_ascii_case("LONG"), exit_kind) {
        (true, ExitKind::TakeProfit) => candidates.sort_by(|a, b| a.total_cmp(b)),
        (true, ExitKind::StopLoss) => candidates.sort_by(|a, b| b.total_cmp(a)),
        (false, ExitKind::TakeProfit) => candidates.sort_by(|a, b| b.total_cmp(a)),
        (false, ExitKind::StopLoss) => candidates.sort_by(|a, b| a.total_cmp(b)),
    }
    candidates
        .into_iter()
        .min_by(|a, b| (a - entry_price).abs().total_cmp(&(b - entry_price).abs()))
}

fn find_live_exit_trigger_price(
    open_orders: &[OpenOrderSnapshot],
    position_side: &str,
    hedge_mode: bool,
    entry_price: Option<f64>,
    exit_kind: ExitKind,
) -> Option<f64> {
    if let Some(entry_price) = entry_price {
        let exact_matches = open_orders
            .iter()
            .filter(|order| order_matches_position_side(order, position_side, hedge_mode))
            .filter(|order| exact_order_type_matches_exit_kind(&order.order_type, exit_kind))
            .flat_map(|order| {
                matching_exit_prices_for_order(order, position_side, entry_price, exit_kind)
            })
            .collect::<Vec<_>>();
        if !exact_matches.is_empty() {
            return choose_preferred_exit_price(
                exact_matches,
                entry_price,
                position_side,
                exit_kind,
            );
        }

        let inferred_matches = open_orders
            .iter()
            .filter(|order| order_matches_position_side(order, position_side, hedge_mode))
            .flat_map(|order| {
                matching_exit_prices_for_order(order, position_side, entry_price, exit_kind)
            })
            .collect::<Vec<_>>();
        return choose_preferred_exit_price(
            inferred_matches,
            entry_price,
            position_side,
            exit_kind,
        );
    }

    open_orders
        .iter()
        .filter(|order| order_matches_position_side(order, position_side, hedge_mode))
        .filter(|order| exact_order_type_matches_exit_kind(&order.order_type, exit_kind))
        .filter_map(exit_order_trigger_price)
        .next()
}

fn ensure_account_ws_listener_started(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
) {
    if ACCOUNT_WS_LISTENER_STARTED.swap(true, Ordering::SeqCst) {
        return;
    }
    let client = http_client.clone();
    let api = api_config.clone();
    let exec = exec_config.clone();
    let state = account_ws_state();
    tokio::spawn(async move {
        run_account_ws_listener_loop(client, api, exec, state).await;
    });
}

async fn run_account_ws_listener_loop(
    http_client: Client,
    api_config: BinanceApiConfig,
    exec_config: LlmExecutionConfig,
    state: Arc<Mutex<AccountWsState>>,
) {
    let mut connection_attempt: u64 = 0;
    loop {
        connection_attempt += 1;
        info!(
            connection_attempt = connection_attempt,
            reconnect_delay_secs = ACCOUNT_WS_RECONNECT_DELAY_SECS,
            "account ws listener: establishing user stream connection"
        );
        let listen_key = match create_user_data_listen_key(&http_client, &api_config).await {
            Ok(v) => v,
            Err(err) => {
                warn!(
                    connection_attempt = connection_attempt,
                    error = %err,
                    reconnect_delay_secs = ACCOUNT_WS_RECONNECT_DELAY_SECS,
                    "account ws listener: create listenKey failed"
                );
                sleep(Duration::from_secs(ACCOUNT_WS_RECONNECT_DELAY_SECS)).await;
                continue;
            }
        };
        info!(
            connection_attempt = connection_attempt,
            listen_key = %redact_secret_for_log(&listen_key),
            "account ws listener: listenKey created"
        );
        let ws_url = match build_futures_user_stream_ws_url(&api_config, &listen_key) {
            Ok(v) => v,
            Err(err) => {
                warn!(
                    connection_attempt = connection_attempt,
                    listen_key = %redact_secret_for_log(&listen_key),
                    error = %err,
                    reconnect_delay_secs = ACCOUNT_WS_RECONNECT_DELAY_SECS,
                    "account ws listener: build ws url failed"
                );
                log_listen_key_delete_result(
                    connection_attempt,
                    &listen_key,
                    delete_user_data_listen_key(&http_client, &api_config, &listen_key).await,
                );
                sleep(Duration::from_secs(ACCOUNT_WS_RECONNECT_DELAY_SECS)).await;
                continue;
            }
        };

        let ws_conn = connect_async(ws_url.as_str()).await;
        let (mut ws_stream, _) = match ws_conn {
            Ok(v) => v,
            Err(err) => {
                warn!(
                    connection_attempt = connection_attempt,
                    listen_key = %redact_secret_for_log(&listen_key),
                    error = %err,
                    reconnect_delay_secs = ACCOUNT_WS_RECONNECT_DELAY_SECS,
                    "account ws listener: connect failed"
                );
                log_listen_key_delete_result(
                    connection_attempt,
                    &listen_key,
                    delete_user_data_listen_key(&http_client, &api_config, &listen_key).await,
                );
                sleep(Duration::from_secs(ACCOUNT_WS_RECONNECT_DELAY_SECS)).await;
                continue;
            }
        };
        info!(
            connection_attempt = connection_attempt,
            listen_key = %redact_secret_for_log(&listen_key),
            "account ws listener: websocket connected"
        );

        let mut message_index: u64 = 0;
        loop {
            let next = ws_stream.next().await;
            let Some(frame) = next else {
                warn!(
                    connection_attempt = connection_attempt,
                    message_index = message_index,
                    listen_key = %redact_secret_for_log(&listen_key),
                    reconnect_delay_secs = ACCOUNT_WS_RECONNECT_DELAY_SECS,
                    "account ws listener: stream ended unexpectedly"
                );
                break;
            };
            let Ok(msg) = frame else {
                let err = frame.err().expect("frame error available");
                warn!(
                    connection_attempt = connection_attempt,
                    message_index = message_index,
                    listen_key = %redact_secret_for_log(&listen_key),
                    error = %err,
                    reconnect_delay_secs = ACCOUNT_WS_RECONNECT_DELAY_SECS,
                    "account ws listener: frame read failed"
                );
                break;
            };
            message_index += 1;
            let text = match msg {
                Message::Text(t) => t.to_string(),
                Message::Binary(b) => match String::from_utf8(b.to_vec()) {
                    Ok(text) => {
                        info!(
                            connection_attempt = connection_attempt,
                            message_index = message_index,
                            byte_len = b.len(),
                            "account ws listener: decoded binary websocket frame as utf8 text"
                        );
                        text
                    }
                    Err(err) => {
                        warn!(
                            connection_attempt = connection_attempt,
                            message_index = message_index,
                            byte_len = b.len(),
                            error = %err,
                            "account ws listener: ignored non-utf8 binary websocket frame"
                        );
                        continue;
                    }
                },
                Message::Close(frame) => {
                    info!(
                        connection_attempt = connection_attempt,
                        message_index = message_index,
                        close_code = frame
                            .as_ref()
                            .map(|value| u16::from(value.code))
                            .unwrap_or_default(),
                        close_reason = frame
                            .as_ref()
                            .map(|value| value.reason.to_string())
                            .unwrap_or_default(),
                        reconnect_delay_secs = ACCOUNT_WS_RECONNECT_DELAY_SECS,
                        "account ws listener: close frame received"
                    );
                    break;
                }
                Message::Ping(_) | Message::Pong(_) => continue,
                _ => {
                    info!(
                        connection_attempt = connection_attempt,
                        message_index = message_index,
                        "account ws listener: ignored unsupported websocket frame"
                    );
                    continue;
                }
            };
            let payload = match serde_json::from_str::<Value>(&text) {
                Ok(value) => value,
                Err(err) => {
                    warn!(
                        connection_attempt = connection_attempt,
                        message_index = message_index,
                        error = %err,
                        payload_preview = %truncate_for_log(&text, ACCOUNT_WS_LOG_VALUE_PREVIEW_CHARS),
                        "account ws listener: json decode failed"
                    );
                    continue;
                }
            };
            let event_type = payload.get("e").and_then(Value::as_str).unwrap_or_default();
            if event_type.is_empty() {
                warn!(
                    connection_attempt = connection_attempt,
                    message_index = message_index,
                    payload_preview = %truncate_for_log(&text, ACCOUNT_WS_LOG_VALUE_PREVIEW_CHARS),
                    "account ws listener: websocket event missing type"
                );
                continue;
            }
            match event_type {
                "ACCOUNT_UPDATE" => {
                    let event_summary = summarize_account_update_event(&payload);
                    let flattened_symbols = apply_account_update_event(&payload, &state);
                    info!(
                        connection_attempt = connection_attempt,
                        message_index = message_index,
                        event_type = event_type,
                        flattened_symbols = %join_or_dash(&flattened_symbols),
                        event_summary = %event_summary,
                        "account ws listener: applied account update"
                    );
                    for symbol in flattened_symbols {
                        let client = http_client.clone();
                        let api = api_config.clone();
                        let exec = exec_config.clone();
                        tokio::spawn(async move {
                            match cleanup_orphan_exit_orders_for_symbol(
                                &client, &api, &exec, &symbol,
                            )
                            .await
                            {
                                Ok(true) => info!(
                                    symbol = %symbol,
                                    "account_update_cleanup: canceled orphan exit orders after flatten"
                                ),
                                Ok(false) => {}
                                Err(err) => warn!(
                                    symbol = %symbol,
                                    error = %err,
                                    "account_update_cleanup: failed to cancel orphan exit orders after flatten"
                                ),
                            }
                        });
                    }
                }
                "ORDER_TRADE_UPDATE" => {
                    info!(
                        connection_attempt = connection_attempt,
                        message_index = message_index,
                        event_type = event_type,
                        event_summary = %summarize_order_trade_update_event(&payload),
                        "account ws listener: observed order trade update"
                    );
                }
                _ => {
                    info!(
                        connection_attempt = connection_attempt,
                        message_index = message_index,
                        event_type = event_type,
                        event_summary = %truncate_for_log(
                            &payload.to_string(),
                            ACCOUNT_WS_LOG_VALUE_PREVIEW_CHARS,
                        ),
                        "account ws listener: ignored unsupported user-stream event"
                    );
                }
            }
        }

        log_listen_key_delete_result(
            connection_attempt,
            &listen_key,
            delete_user_data_listen_key(&http_client, &api_config, &listen_key).await,
        );
        info!(
            connection_attempt = connection_attempt,
            reconnect_delay_secs = ACCOUNT_WS_RECONNECT_DELAY_SECS,
            "account ws listener: reconnect scheduled"
        );
        sleep(Duration::from_secs(ACCOUNT_WS_RECONNECT_DELAY_SECS)).await;
    }
}

fn redact_secret_for_log(value: &str) -> String {
    let chars = value.chars().collect::<Vec<_>>();
    if chars.len() <= 8 {
        return "*".repeat(chars.len().max(1));
    }
    let head = chars.iter().take(4).collect::<String>();
    let tail = chars[chars.len().saturating_sub(4)..]
        .iter()
        .collect::<String>();
    format!("{head}...{tail}")
}

fn truncate_for_log(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let mut out = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        out.push_str("...");
    }
    out
}

fn join_or_dash(values: &[String]) -> String {
    if values.is_empty() {
        "-".to_string()
    } else {
        values.join(",")
    }
}

fn json_log_value(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(v)) => v.clone(),
        Some(Value::Number(v)) => v.to_string(),
        Some(Value::Bool(v)) => v.to_string(),
        Some(Value::Null) | None => "-".to_string(),
        Some(other) => truncate_for_log(&other.to_string(), ACCOUNT_WS_LOG_VALUE_PREVIEW_CHARS),
    }
}

fn format_log_list(values: &[String]) -> String {
    format!("[{}]", values.join(","))
}

fn summarize_account_balance_entries(balances: &[Value]) -> (String, String, String) {
    let mut balance_assets = Vec::new();
    let mut balance_summaries = Vec::new();
    let mut usdt_balance_summary = None;
    for balance in balances.iter().take(ACCOUNT_WS_BALANCE_SUMMARY_LIMIT) {
        let asset = balance.get("a").and_then(Value::as_str).unwrap_or("-");
        balance_assets.push(asset.to_string());
        balance_summaries.push(format!(
            "{}:wb={}:cw={}:bc={}",
            asset,
            json_log_value(balance.get("wb")),
            json_log_value(balance.get("cw")),
            json_log_value(balance.get("bc"))
        ));
        if asset.eq_ignore_ascii_case("USDT") {
            usdt_balance_summary = Some(format!(
                "wb={} cw={} bc={}",
                json_log_value(balance.get("wb")),
                json_log_value(balance.get("cw")),
                json_log_value(balance.get("bc"))
            ));
        }
    }
    if balances.len() > ACCOUNT_WS_BALANCE_SUMMARY_LIMIT {
        let remaining = balances.len() - ACCOUNT_WS_BALANCE_SUMMARY_LIMIT;
        let more_summary = format!("+{} more", remaining);
        balance_assets.push(more_summary.clone());
        balance_summaries.push(more_summary);
    }
    (
        format_log_list(&balance_assets),
        format_log_list(&balance_summaries),
        usdt_balance_summary.unwrap_or_else(|| "not_present_in_event".to_string()),
    )
}

fn summarize_account_update_event(payload: &Value) -> String {
    let Some(account_obj) = payload.get("a").and_then(Value::as_object) else {
        return "missing_account_object=true".to_string();
    };
    let reason = account_obj.get("m").and_then(Value::as_str).unwrap_or("-");
    let (balance_assets, balances_summary, usdt_balance_summary) = account_obj
        .get("B")
        .and_then(Value::as_array)
        .map(|balances| summarize_account_balance_entries(balances))
        .unwrap_or_else(|| {
            (
                "-".to_string(),
                "-".to_string(),
                "balances_missing".to_string(),
            )
        });
    let positions = account_obj
        .get("P")
        .and_then(Value::as_array)
        .map(|positions| {
            let mut summaries = positions
                .iter()
                .take(ACCOUNT_WS_POSITION_SUMMARY_LIMIT)
                .map(|position| {
                    format!(
                        "{}:{}:pa={}:ep={}:up={}",
                        json_log_value(position.get("s")),
                        json_log_value(position.get("ps")),
                        json_log_value(position.get("pa")),
                        json_log_value(position.get("ep")),
                        json_log_value(position.get("up")),
                    )
                })
                .collect::<Vec<_>>();
            if positions.len() > ACCOUNT_WS_POSITION_SUMMARY_LIMIT {
                summaries.push(format!(
                    "+{} more",
                    positions.len() - ACCOUNT_WS_POSITION_SUMMARY_LIMIT
                ));
            }
            summaries.join(",")
        })
        .unwrap_or_else(|| "-".to_string());
    format!(
        "reason={} event_time={} balance_assets={} balances={} usdt_balance={} positions=[{}]",
        reason,
        json_log_value(payload.get("E")),
        balance_assets,
        balances_summary,
        usdt_balance_summary,
        positions,
    )
}

fn summarize_order_trade_update_event(payload: &Value) -> String {
    let Some(order_obj) = payload.get("o").and_then(Value::as_object) else {
        return format!(
            "missing_order_object=true event_time={}",
            json_log_value(payload.get("E"))
        );
    };
    format!(
        "event_time={} symbol={} order_id={} client_order_id={} side={} position_side={} order_type={} exec_type={} order_status={} last_fill_qty={} cum_fill_qty={} realized_pnl={} avg_price={}",
        json_log_value(payload.get("E")),
        json_log_value(order_obj.get("s")),
        json_log_value(order_obj.get("i")),
        json_log_value(order_obj.get("c")),
        json_log_value(order_obj.get("S")),
        json_log_value(order_obj.get("ps")),
        json_log_value(order_obj.get("o")),
        json_log_value(order_obj.get("x")),
        json_log_value(order_obj.get("X")),
        json_log_value(order_obj.get("l")),
        json_log_value(order_obj.get("z")),
        json_log_value(order_obj.get("rp")),
        json_log_value(order_obj.get("ap")),
    )
}

fn log_listen_key_delete_result(connection_attempt: u64, listen_key: &str, result: Result<()>) {
    match result {
        Ok(()) => info!(
            connection_attempt = connection_attempt,
            listen_key = %redact_secret_for_log(listen_key),
            "account ws listener: listenKey deleted"
        ),
        Err(err) => warn!(
            connection_attempt = connection_attempt,
            listen_key = %redact_secret_for_log(listen_key),
            error = %err,
            "account ws listener: listenKey delete failed"
        ),
    }
}

fn apply_account_update_event(payload: &Value, state: &Arc<Mutex<AccountWsState>>) -> Vec<String> {
    let event_type = payload.get("e").and_then(Value::as_str).unwrap_or_default();
    if event_type != "ACCOUNT_UPDATE" {
        return Vec::new();
    }
    let Some(account_obj) = payload.get("a").and_then(Value::as_object) else {
        return Vec::new();
    };

    let mut total_wallet_balance: Option<f64> = None;
    let mut available_balance: Option<f64> = None;
    if let Some(balances) = account_obj.get("B").and_then(Value::as_array) {
        for b in balances {
            let asset = b.get("a").and_then(Value::as_str).unwrap_or_default();
            if !asset.eq_ignore_ascii_case("USDT") {
                continue;
            }
            total_wallet_balance = parse_optional_f64(b, &["wb"]);
            available_balance = parse_optional_f64(b, &["cw"]).or(total_wallet_balance);
            break;
        }
    }

    let mut guard = match state.lock() {
        Ok(g) => g,
        Err(_) => return Vec::new(),
    };
    let mut flattened_symbols = HashSet::new();
    if let Some(v) = total_wallet_balance {
        guard.total_wallet_balance = Some(v);
    }
    if let Some(v) = available_balance {
        guard.available_balance = Some(v);
    }

    if let Some(positions) = account_obj.get("P").and_then(Value::as_array) {
        for p in positions {
            let symbol = p
                .get("s")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            if symbol.is_empty() {
                continue;
            }
            let position_side = p
                .get("ps")
                .and_then(Value::as_str)
                .unwrap_or("BOTH")
                .to_string();
            let position_amt = parse_optional_f64(p, &["pa"]).unwrap_or(0.0);
            let key = (symbol.clone(), position_side.clone());
            let had_live_position = guard
                .positions
                .get(&key)
                .is_some_and(|v| v.position_amt.abs() > f64::EPSILON);
            if position_amt.abs() <= f64::EPSILON {
                if had_live_position {
                    flattened_symbols.insert(symbol.clone());
                    guard.flattened_at.insert(key.clone(), Instant::now());
                }
                guard.positions.remove(&key);
                continue;
            }
            guard.flattened_at.remove(&key);

            let entry_price = parse_optional_f64(p, &["ep"]).unwrap_or(0.0);
            let unrealized_pnl = parse_optional_f64(p, &["up"]).unwrap_or(0.0);
            let mark_price = if entry_price > 0.0 && position_amt.abs() > f64::EPSILON {
                entry_price + unrealized_pnl / position_amt
            } else {
                entry_price
            };
            let leverage = guard.positions.get(&key).map(|v| v.leverage).unwrap_or(1);
            guard.positions.insert(
                key,
                ActivePositionSnapshot {
                    position_side,
                    position_amt,
                    entry_price,
                    mark_price,
                    unrealized_pnl,
                    leverage,
                },
            );
        }
    }
    guard.has_account_update = true;
    flattened_symbols.into_iter().collect()
}

pub async fn execute_trade_intent(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    symbol: &str,
    intent: &TradeIntent,
) -> Result<ExecutionReport> {
    if matches!(intent.decision, TradeDecision::NoTrade) {
        return Err(anyhow!("NO_TRADE does not produce an exchange order"));
    }

    let symbol_filters = fetch_symbol_filters(http_client, api_config, symbol).await?;
    let account_balance = fetch_account_balance(http_client, api_config, exec_config).await?;
    let (leverage, leverage_source) = select_leverage(exec_config, intent)?;
    let (margin_budget_usdt, margin_budget_source) =
        select_margin_budget(exec_config, &account_balance)?;
    let position_side = resolve_position_side(exec_config, intent.decision);
    let entry_price = intent
        .entry_price
        .ok_or_else(|| anyhow!("entry price is missing"))?;
    let take_profit = intent.take_profit.ok_or_else(|| anyhow!("tp is missing"))?;
    let stop_loss = intent.stop_loss.ok_or_else(|| anyhow!("sl is missing"))?;
    let target_rr = intent
        .risk_reward_ratio
        .ok_or_else(|| anyhow!("rr is missing"))?;
    let book_ticker = fetch_book_ticker(http_client, api_config, symbol).await?;
    let best_bid_price = parse_book_ticker_price(&book_ticker.bid_price, entry_price);
    let best_ask_price = parse_book_ticker_price(&book_ticker.ask_price, entry_price);
    if let Some(blocked) = build_trade_blocked_by_current_price_beyond_stop_loss(
        intent.decision,
        entry_price,
        stop_loss,
        best_bid_price,
        best_ask_price,
    ) {
        return Err(blocked.into());
    }
    let maker_entry_price =
        derive_maker_entry_price(intent.decision, entry_price, &book_ticker, &symbol_filters);
    let quantity = compute_order_quantity(
        &symbol_filters,
        margin_budget_usdt,
        maker_entry_price,
        leverage,
    )?;
    let quantity_str = format_decimal(quantity, symbol_filters.qty_precision);
    let maker_entry_price_str = format_decimal(maker_entry_price, symbol_filters.price_precision);

    let tp_price = quantize_exit_price(
        take_profit,
        symbol_filters.tick_size,
        symbol_filters.price_precision,
        intent.decision,
        true,
    );
    let sl_price = recompute_stop_loss_from_target_rr(
        intent.decision,
        maker_entry_price,
        tp_price,
        target_rr,
        symbol_filters.tick_size,
        symbol_filters.price_precision,
    )?;
    let final_rr = validate_final_execution_rr(
        intent.decision,
        maker_entry_price,
        tp_price,
        sl_price,
        target_rr,
    )?;

    info!(
        symbol = %symbol,
        decision = intent.decision.as_str(),
        model_entry_price = entry_price,
        model_take_profit = take_profit,
        model_stop_loss = stop_loss,
        model_risk_reward_ratio = target_rr,
        best_bid_price = best_bid_price,
        best_ask_price = best_ask_price,
        maker_entry_price = maker_entry_price,
        final_take_profit = tp_price,
        final_stop_loss = sl_price,
        final_risk_reward_ratio = final_rr,
        "resolved final execution levels"
    );

    if exec_config.dry_run {
        return Ok(ExecutionReport {
            decision: intent.decision.as_str(),
            quantity: quantity_str,
            leverage,
            leverage_source,
            margin_budget_usdt,
            margin_budget_source,
            account_total_wallet_balance: account_balance.total_wallet_balance,
            account_available_balance: account_balance.available_balance,
            position_side,
            dry_run: true,
            entry_order_id: None,
            take_profit_order_id: None,
            stop_loss_order_id: None,
            exit_orders_deferred: false,
            maker_entry_price,
            actual_take_profit: tp_price,
            actual_stop_loss: sl_price,
            actual_risk_reward_ratio: final_rr,
            best_bid_price,
            best_ask_price,
        });
    }

    let exit_side = match intent.decision {
        TradeDecision::Long => "SELL",
        TradeDecision::Short => "BUY",
        TradeDecision::NoTrade => unreachable!(),
    };
    // A maker entry is a limit order that sits below (long) or above (short) the current
    // market price waiting to be filled. In this case Binance rejects a TAKE_PROFIT_MARKET
    // algo-order placed at the TP level because no position exists yet and the trigger
    // is already beyond the current market. We detect this and use a STOP limit algo-order
    // triggered at the entry price instead (see place_staged_exit_orders).
    let is_maker_entry = match intent.decision {
        TradeDecision::Long => maker_entry_price < best_bid_price,
        TradeDecision::Short => maker_entry_price > best_ask_price,
        TradeDecision::NoTrade => unreachable!(),
    };
    let tp_trigger_price = format_decimal(tp_price, symbol_filters.price_precision);
    let sl_trigger_price = format_decimal(sl_price, symbol_filters.price_precision);
    set_futures_leverage(http_client, api_config, exec_config, symbol, leverage).await?;
    let entry_order_id = place_entry_market_order(
        http_client,
        api_config,
        exec_config,
        symbol,
        intent.decision,
        position_side,
        &quantity_str,
        &maker_entry_price_str,
        symbol_filters.tick_size,
        symbol_filters.price_precision,
        !exec_config.place_exit_orders,
    )
    .await?;

    let (take_profit_order_id, stop_loss_order_id, exit_orders_deferred) = if exec_config
        .place_exit_orders
    {
        let (take_profit_order_id, tp_is_algo_order, stop_loss_order_id) =
            match place_staged_exit_orders(
                http_client,
                api_config,
                exec_config,
                symbol,
                position_side,
                exit_side,
                &quantity_str,
                &tp_trigger_price,
                &sl_trigger_price,
                is_maker_entry,
                &maker_entry_price_str,
            )
            .await
            {
                Ok(v) => v,
                Err(err) => {
                    let cancel_err = cancel_order_by_id(
                        http_client,
                        api_config,
                        exec_config,
                        symbol,
                        entry_order_id,
                    )
                    .await
                    .err();
                    return Err(match cancel_err {
                        Some(cancel_err) => anyhow!(
                            "entry order {} placed but staging synchronized exits failed: {}; canceling entry also failed: {}",
                            entry_order_id,
                            err,
                            cancel_err
                        ),
                        None => anyhow!(
                            "entry order {} placed but staging synchronized exits failed; entry order canceled: {}",
                            entry_order_id,
                            err
                        ),
                    });
                }
            };
        info!(
            symbol = %symbol,
            entry_order_id = entry_order_id,
            take_profit_order_id = take_profit_order_id,
            stop_loss_order_id = stop_loss_order_id,
            tp_trigger_price = %tp_trigger_price,
            sl_trigger_price = %sl_trigger_price,
            is_maker_entry = is_maker_entry,
            tp_is_algo_order = tp_is_algo_order,
            "entry_order_placed_with_synchronized_exit_orders"
        );
        tokio::spawn(watch_staged_exit_orders(
            http_client.clone(),
            api_config.clone(),
            exec_config.clone(),
            symbol.to_string(),
            position_side.to_string(),
            entry_order_id,
            Some(TrackedExitOrder {
                order_id: take_profit_order_id,
                is_algo_order: tp_is_algo_order,
            }),
            Some(TrackedExitOrder {
                order_id: stop_loss_order_id,
                is_algo_order: true,
            }),
        ));
        (Some(take_profit_order_id), Some(stop_loss_order_id), false)
    } else {
        (None, None, false)
    };

    Ok(ExecutionReport {
        decision: intent.decision.as_str(),
        quantity: quantity_str,
        leverage,
        leverage_source,
        margin_budget_usdt,
        margin_budget_source,
        account_total_wallet_balance: account_balance.total_wallet_balance,
        account_available_balance: account_balance.available_balance,
        position_side,
        dry_run: false,
        entry_order_id: Some(entry_order_id),
        take_profit_order_id,
        stop_loss_order_id,
        exit_orders_deferred,
        maker_entry_price,
        actual_take_profit: tp_price,
        actual_stop_loss: sl_price,
        actual_risk_reward_ratio: final_rr,
        best_bid_price,
        best_ask_price,
    })
}

pub async fn fetch_symbol_trading_state(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    symbol: &str,
) -> Result<TradingStateSnapshot> {
    let account_balance = fetch_account_balance(http_client, api_config, exec_config).await?;
    let (mut active_positions, active_position_source) =
        fetch_active_positions_with_source(http_client, api_config, exec_config, symbol).await?;
    let mut open_orders = fetch_open_orders(http_client, api_config, exec_config, symbol).await?;
    let mut open_algo_orders =
        fetch_open_algo_orders(http_client, api_config, exec_config, symbol).await?;
    open_orders.append(&mut open_algo_orders);
    if should_reconcile_ws_positions_with_rest(
        active_position_source,
        &active_positions,
        &open_orders,
    ) {
        let ws_position_count = active_positions.len();
        let rest_positions =
            fetch_active_positions_from_rest(http_client, api_config, exec_config, symbol).await?;
        if rest_positions.len() != ws_position_count {
            info!(
                symbol = %symbol,
                ws_position_count = ws_position_count,
                rest_position_count = rest_positions.len(),
                "reconciled account-ws position snapshot against rest because no exit orders were open"
            );
        }
        active_positions = rest_positions;
    }
    let has_active_positions = !active_positions.is_empty();
    let has_open_orders = !open_orders.is_empty();
    Ok(TradingStateSnapshot {
        symbol: symbol.to_string(),
        has_active_context: has_active_positions || has_open_orders,
        has_active_positions,
        has_open_orders,
        active_positions,
        open_orders,
        total_wallet_balance: account_balance.total_wallet_balance,
        available_balance: account_balance.available_balance,
    })
}

fn should_cleanup_orphan_exit_orders_when_flat(state: &TradingStateSnapshot) -> bool {
    !state.has_active_positions
        && state.has_open_orders
        && state.open_orders.iter().all(is_exit_like_order)
}

fn has_pending_entry_order_for_side(
    state: &TradingStateSnapshot,
    position_side: &str,
    hedge_mode: bool,
) -> bool {
    state.open_orders.iter().any(|order| {
        is_entry_like_order(order) && order_matches_position_side(order, position_side, hedge_mode)
    })
}

fn collect_orphan_exit_orders_to_cancel(
    state: &TradingStateSnapshot,
    hedge_mode: bool,
) -> Vec<TrackedExitOrder> {
    if should_cleanup_orphan_exit_orders_when_flat(state) {
        return state
            .open_orders
            .iter()
            .filter(|order| is_exit_like_order(order))
            .map(tracked_exit_order)
            .collect();
    }
    if !hedge_mode {
        return Vec::new();
    }

    let active_sides: HashSet<&'static str> = state
        .active_positions
        .iter()
        .filter(|position| position.position_amt.abs() > f64::EPSILON)
        .filter_map(|position| strict_hedge_position_side(&position.position_side))
        .collect();

    let mut out = Vec::new();
    for position_side in ["LONG", "SHORT"] {
        if active_sides.contains(position_side)
            || has_pending_entry_order_for_side(state, position_side, hedge_mode)
        {
            continue;
        }
        out.extend(
            state
                .open_orders
                .iter()
                .filter(|order| is_exit_like_order(order))
                .filter(|order| order_matches_position_side(order, position_side, hedge_mode))
                .map(tracked_exit_order),
        );
    }
    out
}

fn is_order_already_gone_error(err: &anyhow::Error) -> bool {
    let text = err.to_string();
    text.contains("\"code\":-2011")
        || text.contains("\"code\":-2013")
        || text.contains("Unknown order sent")
        || text.contains("Order does not exist")
        || text.contains("order not found")
}

async fn cancel_tracked_exit_orders(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    symbol: &str,
    orders: &[TrackedExitOrder],
) -> Result<bool> {
    if orders.is_empty() {
        return Ok(false);
    }

    for order in orders {
        let cancel_result = if order.is_algo_order {
            cancel_algo_order_by_id(http_client, api_config, exec_config, symbol, order.order_id)
                .await
        } else {
            cancel_order_by_id(http_client, api_config, exec_config, symbol, order.order_id).await
        };
        if let Err(err) = cancel_result {
            if is_order_already_gone_error(&err) {
                continue;
            }
            return Err(err);
        }
    }
    Ok(true)
}

fn collect_exit_orders_for_side(
    open_orders: &[OpenOrderSnapshot],
    position_side: &str,
    hedge_mode: bool,
) -> Vec<TrackedExitOrder> {
    collect_tracked_orders_for_position_side(open_orders, position_side, hedge_mode, false, true)
}

fn collect_pending_context_orders_for_side(
    open_orders: &[OpenOrderSnapshot],
    position_side: &str,
    hedge_mode: bool,
) -> Vec<TrackedExitOrder> {
    collect_tracked_orders_for_position_side(open_orders, position_side, hedge_mode, true, true)
}

pub(crate) async fn cleanup_orphan_exit_orders_for_symbol(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    symbol: &str,
) -> Result<bool> {
    let state = fetch_symbol_trading_state(http_client, api_config, exec_config, symbol).await?;
    let orphan_orders = collect_orphan_exit_orders_to_cancel(&state, exec_config.hedge_mode);
    cancel_tracked_exit_orders(http_client, api_config, exec_config, symbol, &orphan_orders).await
}

pub async fn execute_management_intent(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    symbol: &str,
    intent: &PositionManagementIntent,
) -> Result<ManagementExecutionReport> {
    let state = fetch_symbol_trading_state(http_client, api_config, exec_config, symbol).await?;
    let mut report = ManagementExecutionReport {
        action: intent.decision.as_str(),
        dry_run: exec_config.dry_run,
        position_count: state.active_positions.len(),
        open_order_count: state.open_orders.len(),
        canceled_open_orders: false,
        add_order_id: None,
        reduce_order_ids: Vec::new(),
        close_order_ids: Vec::new(),
        modify_take_profit_order_ids: Vec::new(),
        modify_stop_loss_order_ids: Vec::new(),
        realized_pnl_usdt: 0.0,
    };

    match intent.decision {
        PositionManagementDecision::Hold => Ok(report),
        PositionManagementDecision::Close => {
            if exec_config.dry_run {
                report.canceled_open_orders = state.has_open_orders;
                return Ok(report);
            }

            if state.has_open_orders {
                cancel_all_open_orders(http_client, api_config, exec_config, symbol).await?;
                cancel_all_open_algo_orders(http_client, api_config, exec_config, symbol).await?;
                report.canceled_open_orders = true;
            }

            if state.has_active_positions {
                let symbol_filters = fetch_symbol_filters(http_client, api_config, symbol).await?;
                for position in &state.active_positions {
                    let position_side = if exec_config.hedge_mode {
                        position.position_side.as_str()
                    } else {
                        "BOTH"
                    };
                    let close_qty =
                        round_down_to_step(position.position_amt.abs(), symbol_filters.step_size);
                    if close_qty < symbol_filters.min_qty {
                        continue;
                    }
                    let close_qty_str = format_decimal(close_qty, symbol_filters.qty_precision);
                    let close_side = if position.position_amt > 0.0 {
                        "SELL"
                    } else {
                        "BUY"
                    };
                    let close_order_id = place_market_order_with_side(
                        http_client,
                        api_config,
                        exec_config,
                        symbol,
                        close_side,
                        position_side,
                        &close_qty_str,
                        "close",
                    )
                    .await?;
                    report.close_order_ids.push(close_order_id);
                    match fetch_order_realized_pnl(
                        http_client,
                        api_config,
                        exec_config,
                        symbol,
                        close_order_id,
                    )
                    .await
                    {
                        Ok(pnl) => {
                            report.realized_pnl_usdt += pnl;
                        }
                        Err(err) => {
                            warn!(
                                symbol = %symbol,
                                order_id = close_order_id,
                                error = %err,
                                "fetch close order realized pnl failed"
                            );
                        }
                    }
                }
            }

            Ok(report)
        }
        PositionManagementDecision::Add => {
            let symbol_filters = fetch_symbol_filters(http_client, api_config, symbol).await?;
            validate_reanchor_request(intent, &state, &symbol_filters, exec_config)?;
            let account_balance = FuturesAccountBalance {
                total_wallet_balance: state.total_wallet_balance,
                available_balance: state.available_balance,
            };
            let (margin_budget_usdt, _) = select_margin_budget(exec_config, &account_balance)?;
            let fallback_leverage =
                normalize_leverage(exec_config.default_leverage_ratio, exec_config.max_leverage);

            let (side, position_side, reference_price, leverage, position_amt_for_ratio) =
                if let Some(active_position) = state.single_active_position() {
                    let reference_price = if active_position.mark_price > 0.0 {
                        active_position.mark_price
                    } else {
                        active_position.entry_price
                    };
                    let side = if active_position.position_amt > 0.0 {
                        "BUY".to_string()
                    } else {
                        "SELL".to_string()
                    };
                    let position_side = if exec_config.hedge_mode {
                        active_position.position_side.clone()
                    } else {
                        "BOTH".to_string()
                    };
                    let leverage = normalize_leverage(
                        active_position.leverage as f64,
                        exec_config.max_leverage,
                    );
                    (
                        side,
                        position_side,
                        reference_price,
                        leverage,
                        Some(active_position.position_amt.abs()),
                    )
                } else {
                    // No active position but there are open orders: allow ADD by following entry-like order direction.
                    let base_order = state
                        .open_orders
                        .iter()
                        .find(|o| {
                            !o.close_position
                                && !o.reduce_only
                                && (o.side.eq_ignore_ascii_case("BUY")
                                    || o.side.eq_ignore_ascii_case("SELL"))
                        })
                        .ok_or_else(|| {
                            anyhow!(
                                "ADD requires one active position or at least one entry-like open order"
                            )
                        })?;
                    let side = if base_order.side.eq_ignore_ascii_case("BUY") {
                        "BUY".to_string()
                    } else {
                        "SELL".to_string()
                    };
                    let position_side = if exec_config.hedge_mode {
                        let ps = base_order.position_side.trim().to_ascii_uppercase();
                        if ps.is_empty() || ps == "-" {
                            if side == "BUY" {
                                "LONG".to_string()
                            } else {
                                "SHORT".to_string()
                            }
                        } else {
                            ps
                        }
                    } else {
                        "BOTH".to_string()
                    };
                    let reference_price = if base_order.price > 0.0 {
                        base_order.price
                    } else {
                        let ticker = fetch_book_ticker(http_client, api_config, symbol).await?;
                        if side.eq_ignore_ascii_case("BUY") {
                            ticker.ask_price.parse::<f64>().unwrap_or(0.0)
                        } else {
                            ticker.bid_price.parse::<f64>().unwrap_or(0.0)
                        }
                    };
                    (
                        side,
                        position_side,
                        reference_price,
                        fallback_leverage,
                        None,
                    )
                };
            let open_orders_only_mode = position_amt_for_ratio.is_none();

            if reference_price <= 0.0 {
                return Err(anyhow!(
                    "ADD reference price is invalid (<=0) in current management context"
                ));
            }
            let add_qty = match (intent.qty, intent.qty_ratio) {
                (Some(qty), _) => qty,
                (None, Some(ratio)) => {
                    if let Some(position_qty) = position_amt_for_ratio {
                        position_qty * ratio
                    } else {
                        let base_qty = compute_order_quantity(
                            &symbol_filters,
                            margin_budget_usdt,
                            reference_price,
                            leverage,
                        )?;
                        base_qty * ratio
                    }
                }
                (None, None) => compute_order_quantity(
                    &symbol_filters,
                    margin_budget_usdt,
                    reference_price,
                    leverage,
                )?,
            };
            let add_qty = round_down_to_step(add_qty, symbol_filters.step_size);
            if add_qty < symbol_filters.min_qty {
                return Err(anyhow!(
                    "ADD qty {:.8} is below exchange minQty {:.8}",
                    add_qty,
                    symbol_filters.min_qty
                ));
            }
            let add_qty_str = format_decimal(add_qty, symbol_filters.qty_precision);
            let book_ticker = fetch_book_ticker(http_client, api_config, symbol).await?;
            let add_maker_price = if side.eq_ignore_ascii_case("BUY") {
                derive_maker_entry_price(
                    TradeDecision::Long,
                    reference_price,
                    &book_ticker,
                    &symbol_filters,
                )
            } else {
                derive_maker_entry_price(
                    TradeDecision::Short,
                    reference_price,
                    &book_ticker,
                    &symbol_filters,
                )
            };
            let add_maker_price_str =
                format_decimal(add_maker_price, symbol_filters.price_precision);

            if exec_config.dry_run {
                if open_orders_only_mode && state.has_open_orders {
                    report.canceled_open_orders = true;
                }
                return Ok(report);
            }

            // In open-orders-only mode, treat ADD as "re-price/replace pending entry":
            // cancel existing open orders first to avoid duplicated entry orders.
            if open_orders_only_mode && state.has_open_orders {
                let replace_candidates = collect_pending_context_orders_for_side(
                    &state.open_orders,
                    &position_side,
                    exec_config.hedge_mode,
                );
                report.canceled_open_orders = cancel_tracked_exit_orders(
                    http_client,
                    api_config,
                    exec_config,
                    symbol,
                    &replace_candidates,
                )
                .await?;
            }

            let order_id = place_limit_post_only_order_with_side(
                http_client,
                api_config,
                exec_config,
                symbol,
                &side,
                &position_side,
                &add_qty_str,
                &add_maker_price_str,
                symbol_filters.tick_size,
                symbol_filters.price_precision,
                true,
                "add",
            )
            .await?;
            report.add_order_id = Some(order_id);
            if intent.new_tp.is_some() || intent.new_sl.is_some() {
                let (tp_ids, sl_ids) = apply_reanchor_exits(
                    http_client,
                    api_config,
                    exec_config,
                    symbol,
                    intent,
                    &symbol_filters,
                )
                .await?;
                report.modify_take_profit_order_ids.extend(tp_ids);
                report.modify_stop_loss_order_ids.extend(sl_ids);
            }
            Ok(report)
        }
        PositionManagementDecision::Reduce => {
            if !state.has_active_positions {
                return Err(anyhow!("REDUCE requested but no active positions found"));
            }
            let symbol_filters = fetch_symbol_filters(http_client, api_config, symbol).await?;
            validate_reanchor_request(intent, &state, &symbol_filters, exec_config)?;
            if exec_config.dry_run {
                return Ok(report);
            }

            if intent.qty.is_some() && state.active_positions.len() != 1 {
                return Err(anyhow!(
                    "REDUCE with absolute qty requires exactly one active position side; found {}",
                    state.active_positions.len()
                ));
            }

            let mut expected_remaining_positions = Vec::new();
            for position in &state.active_positions {
                let position_side = if exec_config.hedge_mode {
                    position.position_side.as_str()
                } else {
                    "BOTH"
                };
                let desired_qty = match (intent.qty, intent.qty_ratio) {
                    (Some(qty), _) => qty,
                    (None, Some(ratio)) => position.position_amt.abs() * ratio,
                    (None, None) => position.position_amt.abs() * 0.5,
                };
                let reduce_qty = round_down_to_step(
                    desired_qty.min(position.position_amt.abs()),
                    symbol_filters.step_size,
                );
                if reduce_qty < symbol_filters.min_qty {
                    continue;
                }
                expected_remaining_positions.push((
                    position_side.to_string(),
                    (position.position_amt.abs() - reduce_qty).max(0.0),
                ));
                let reduce_qty_str = format_decimal(reduce_qty, symbol_filters.qty_precision);
                let reduce_side = if position.position_amt > 0.0 {
                    "SELL"
                } else {
                    "BUY"
                };
                let reduce_order_id = place_market_order_with_side_reduce(
                    http_client,
                    api_config,
                    exec_config,
                    symbol,
                    reduce_side,
                    position_side,
                    &reduce_qty_str,
                    "reduce",
                )
                .await?;
                report.reduce_order_ids.push(reduce_order_id);
                match fetch_order_realized_pnl(
                    http_client,
                    api_config,
                    exec_config,
                    symbol,
                    reduce_order_id,
                )
                .await
                {
                    Ok(pnl) => {
                        report.realized_pnl_usdt += pnl;
                    }
                    Err(err) => {
                        warn!(
                            symbol = %symbol,
                            order_id = reduce_order_id,
                            error = %err,
                            "fetch reduce order realized pnl failed"
                        );
                    }
                }
            }
            if intent.new_tp.is_some() || intent.new_sl.is_some() {
                if !expected_remaining_positions.is_empty() {
                    wait_for_position_reconcile_after_reduce(
                        http_client,
                        api_config,
                        exec_config,
                        symbol,
                        &expected_remaining_positions,
                        symbol_filters.step_size,
                    )
                    .await?;
                }
                if !expected_remaining_positions.is_empty()
                    && expected_remaining_positions
                        .iter()
                        .all(|(_, expected_qty)| *expected_qty <= symbol_filters.step_size)
                {
                    return Ok(report);
                }
                let (tp_ids, sl_ids) = apply_reanchor_exits(
                    http_client,
                    api_config,
                    exec_config,
                    symbol,
                    intent,
                    &symbol_filters,
                )
                .await?;
                report.modify_take_profit_order_ids.extend(tp_ids);
                report.modify_stop_loss_order_ids.extend(sl_ids);
            }
            Ok(report)
        }
        PositionManagementDecision::ModifyTpSl => {
            if !state.has_active_positions {
                return Err(anyhow!(
                    "MODIFY_TPSL requested but no active positions found"
                ));
            }
            let symbol_filters = fetch_symbol_filters(http_client, api_config, symbol).await?;
            struct ModifyTpSlPlan {
                position_side: String,
                quantity: String,
                current_tp: Option<f64>,
                current_sl: Option<f64>,
                new_tp: f64,
                new_sl: f64,
                is_long: bool,
            }

            if exec_config.dry_run {
                return Ok(report);
            }

            let mut plans = Vec::with_capacity(state.active_positions.len());

            for position in &state.active_positions {
                let position_side = if exec_config.hedge_mode {
                    position.position_side.as_str()
                } else {
                    "BOTH"
                };
                let semantic_side = if position.position_amt > 0.0 {
                    "LONG"
                } else {
                    "SHORT"
                };
                let current_tp = find_live_exit_trigger_price(
                    &state.open_orders,
                    semantic_side,
                    exec_config.hedge_mode,
                    Some(position.entry_price),
                    ExitKind::TakeProfit,
                );
                let current_sl = find_live_exit_trigger_price(
                    &state.open_orders,
                    semantic_side,
                    exec_config.hedge_mode,
                    Some(position.entry_price),
                    ExitKind::StopLoss,
                );
                let is_long = position.position_amt > 0.0;
                let (new_tp, new_sl) = resolve_modify_tpsl_targets(
                    intent,
                    current_tp,
                    current_sl,
                    symbol_filters.tick_size,
                    is_long,
                )?;
                let quantity = format_exit_quantity(
                    position.position_amt.abs(),
                    symbol_filters.step_size,
                    symbol_filters.qty_precision,
                )?;

                plans.push(ModifyTpSlPlan {
                    position_side: position_side.to_string(),
                    quantity,
                    current_tp,
                    current_sl,
                    new_tp,
                    new_sl,
                    is_long,
                });
            }

            let cancel_candidates = plans
                .iter()
                .flat_map(|plan| {
                    collect_exit_orders_for_side(
                        &state.open_orders,
                        &plan.position_side,
                        exec_config.hedge_mode,
                    )
                })
                .collect::<Vec<_>>();
            report.canceled_open_orders = cancel_tracked_exit_orders(
                http_client,
                api_config,
                exec_config,
                symbol,
                &cancel_candidates,
            )
            .await?;

            for plan in plans {
                let exit_side = if plan.is_long { "SELL" } else { "BUY" };
                info!(
                    symbol = %symbol,
                    position_side = %plan.position_side,
                    old_tp = plan.current_tp.unwrap_or_default(),
                    old_sl = plan.current_sl.unwrap_or_default(),
                    new_tp = plan.new_tp,
                    new_sl = plan.new_sl,
                    "modify_tpsl_replace_exits"
                );
                let decision_for_quantize = if plan.is_long {
                    TradeDecision::Long
                } else {
                    TradeDecision::Short
                };
                let tp_price = quantize_exit_price(
                    plan.new_tp,
                    symbol_filters.tick_size,
                    symbol_filters.price_precision,
                    decision_for_quantize,
                    true,
                );
                let sl_price = quantize_exit_price(
                    plan.new_sl,
                    symbol_filters.tick_size,
                    symbol_filters.price_precision,
                    decision_for_quantize,
                    false,
                );
                let tp_trigger_price = format_decimal(
                    tp_price.max(symbol_filters.tick_size),
                    symbol_filters.price_precision,
                );
                let sl_trigger_price = format_decimal(
                    sl_price.max(symbol_filters.tick_size),
                    symbol_filters.price_precision,
                );
                let tp_algo_id = place_close_order(
                    http_client,
                    api_config,
                    exec_config,
                    symbol,
                    exit_side,
                    &plan.position_side,
                    "TAKE_PROFIT_MARKET",
                    &plan.quantity,
                    &tp_trigger_price,
                )
                .await?;
                let sl_algo_id = place_close_order(
                    http_client,
                    api_config,
                    exec_config,
                    symbol,
                    exit_side,
                    &plan.position_side,
                    "STOP_MARKET",
                    &plan.quantity,
                    &sl_trigger_price,
                )
                .await?;
                report.modify_take_profit_order_ids.push(tp_algo_id);
                report.modify_stop_loss_order_ids.push(sl_algo_id);
            }

            Ok(report)
        }
    }
}

pub async fn execute_pending_order_intent(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    symbol: &str,
    intent: &PendingOrderManagementIntent,
    planned_take_profit: Option<f64>,
    planned_stop_loss: Option<f64>,
) -> Result<PendingOrderExecutionReport> {
    let state = fetch_symbol_trading_state(http_client, api_config, exec_config, symbol).await?;
    if state.has_active_positions {
        return Err(anyhow!(
            "pending-order management is invalid while active positions exist"
        ));
    }
    if !state.has_open_orders {
        return Err(anyhow!(
            "pending-order management requested but no open orders found"
        ));
    }

    let mut report = PendingOrderExecutionReport {
        action: intent.decision.as_str(),
        dry_run: exec_config.dry_run,
        open_order_count: state.open_orders.len(),
        canceled_open_orders: false,
        replacement_order_id: None,
        maker_entry_price: None,
        effective_take_profit: None,
        effective_stop_loss: None,
        best_bid_price: None,
        best_ask_price: None,
        leverage: None,
    };

    match intent.decision {
        PendingOrderManagementDecision::Hold => Ok(report),
        PendingOrderManagementDecision::Close => {
            if exec_config.dry_run {
                report.canceled_open_orders = true;
                return Ok(report);
            }
            cancel_all_open_orders(http_client, api_config, exec_config, symbol).await?;
            cancel_all_open_algo_orders(http_client, api_config, exec_config, symbol).await?;
            report.canceled_open_orders = true;
            Ok(report)
        }
        PendingOrderManagementDecision::ModifyMaker => {
            let symbol_filters = fetch_symbol_filters(http_client, api_config, symbol).await?;
            let base_order = state
                .open_orders
                .iter()
                .find(|o| {
                    !o.close_position
                        && !o.reduce_only
                        && (o.side.eq_ignore_ascii_case("BUY")
                            || o.side.eq_ignore_ascii_case("SELL"))
                })
                .ok_or_else(|| anyhow!("MODIFY_MAKER requires one entry-like open order"))?;

            let is_long = base_order.side.eq_ignore_ascii_case("BUY");
            let semantic_side = if is_long { "LONG" } else { "SHORT" };
            let decision = if is_long {
                TradeDecision::Long
            } else {
                TradeDecision::Short
            };
            let position_side = if exec_config.hedge_mode {
                let ps = base_order.position_side.trim().to_ascii_uppercase();
                if ps.is_empty() || ps == "-" || ps == "BOTH" {
                    if is_long {
                        "LONG".to_string()
                    } else {
                        "SHORT".to_string()
                    }
                } else {
                    ps
                }
            } else {
                "BOTH".to_string()
            };
            let remaining_qty = round_down_to_step(
                (base_order.orig_qty - base_order.executed_qty).max(0.0),
                symbol_filters.step_size,
            );
            if remaining_qty < symbol_filters.min_qty {
                return Err(anyhow!(
                    "MODIFY_MAKER remaining qty {:.8} is below exchange minQty {:.8}",
                    remaining_qty,
                    symbol_filters.min_qty
                ));
            }

            let current_entry = if base_order.price > 0.0 {
                base_order.price
            } else {
                let book_ticker = fetch_book_ticker(http_client, api_config, symbol).await?;
                if is_long {
                    parse_book_ticker_price(&book_ticker.bid_price, 0.0)
                } else {
                    parse_book_ticker_price(&book_ticker.ask_price, 0.0)
                }
            };
            if current_entry <= 0.0 {
                return Err(anyhow!("MODIFY_MAKER current entry price is invalid"));
            }

            let leverage =
                fetch_pending_order_leverage(http_client, api_config, exec_config, symbol)
                    .await
                    .ok();
            report.leverage = leverage;

            let book_ticker = fetch_book_ticker(http_client, api_config, symbol).await?;
            let best_bid_price = parse_book_ticker_price(&book_ticker.bid_price, current_entry);
            let best_ask_price = parse_book_ticker_price(&book_ticker.ask_price, current_entry);
            let requested_entry = intent.new_entry.unwrap_or(current_entry);
            let maker_entry_price =
                derive_maker_entry_price(decision, requested_entry, &book_ticker, &symbol_filters);
            report.best_bid_price = Some(best_bid_price);
            report.best_ask_price = Some(best_ask_price);
            report.maker_entry_price = Some(maker_entry_price);

            let current_tp = find_live_exit_trigger_price(
                &state.open_orders,
                semantic_side,
                exec_config.hedge_mode,
                Some(current_entry),
                ExitKind::TakeProfit,
            );
            let current_sl = find_live_exit_trigger_price(
                &state.open_orders,
                semantic_side,
                exec_config.hedge_mode,
                Some(current_entry),
                ExitKind::StopLoss,
            );
            if !pending_modify_maker_requests_effective_change(
                intent,
                current_entry,
                current_tp,
                current_sl,
                leverage.map(|value| value as f64),
                symbol_filters.tick_size,
            ) {
                report.action = PendingOrderManagementDecision::Hold.as_str();
                report.maker_entry_price = Some(current_entry);
                report.effective_take_profit = current_tp;
                report.effective_stop_loss = current_sl;
                return Ok(report);
            }
            let (effective_tp, effective_sl) = resolve_pending_order_exit_levels(
                intent,
                current_tp,
                current_sl,
                planned_take_profit,
                planned_stop_loss,
            )?;
            if let (Some(tp), Some(sl)) = (effective_tp, effective_sl) {
                if is_long {
                    if !(tp > maker_entry_price && sl < maker_entry_price) {
                        return Err(anyhow!(
                            "MODIFY_MAKER for LONG requires new_tp > maker_entry_price and new_sl < maker_entry_price"
                        ));
                    }
                } else if !(tp < maker_entry_price && sl > maker_entry_price) {
                    return Err(anyhow!(
                        "MODIFY_MAKER for SHORT requires new_tp < maker_entry_price and new_sl > maker_entry_price"
                    ));
                }
                report.effective_take_profit = Some(tp);
                report.effective_stop_loss = Some(sl);
            }

            if exec_config.dry_run {
                report.canceled_open_orders = true;
                return Ok(report);
            }

            let replace_candidates = collect_pending_context_orders_for_side(
                &state.open_orders,
                &position_side,
                exec_config.hedge_mode,
            );
            report.canceled_open_orders = cancel_tracked_exit_orders(
                http_client,
                api_config,
                exec_config,
                symbol,
                &replace_candidates,
            )
            .await?;

            let replacement_qty = format_decimal(remaining_qty, symbol_filters.qty_precision);
            let replacement_price =
                format_decimal(maker_entry_price, symbol_filters.price_precision);
            let order_id = place_limit_post_only_order_with_side(
                http_client,
                api_config,
                exec_config,
                symbol,
                if is_long { "BUY" } else { "SELL" },
                &position_side,
                &replacement_qty,
                &replacement_price,
                symbol_filters.tick_size,
                symbol_filters.price_precision,
                !exec_config.place_exit_orders,
                "modify_maker",
            )
            .await?;
            report.replacement_order_id = Some(order_id);

            if exec_config.place_exit_orders {
                if let (Some(tp), Some(sl)) =
                    (report.effective_take_profit, report.effective_stop_loss)
                {
                    let tp_trigger_price = format_decimal(
                        quantize_exit_price(
                            tp,
                            symbol_filters.tick_size,
                            symbol_filters.price_precision,
                            decision,
                            true,
                        ),
                        symbol_filters.price_precision,
                    );
                    let sl_trigger_price = format_decimal(
                        quantize_exit_price(
                            sl,
                            symbol_filters.tick_size,
                            symbol_filters.price_precision,
                            decision,
                            false,
                        ),
                        symbol_filters.price_precision,
                    );
                    let exit_side = if is_long { "SELL" } else { "BUY" };
                    // MODIFY_MAKER always operates on a pending limit entry — is_maker_entry=true
                    let (tp_order_id, tp_is_algo_order, sl_order_id) =
                        match place_staged_exit_orders(
                            http_client,
                            api_config,
                            exec_config,
                            symbol,
                            &position_side,
                            exit_side,
                            &replacement_qty,
                            &tp_trigger_price,
                            &sl_trigger_price,
                            true,
                            &replacement_price,
                        )
                        .await
                        {
                            Ok(v) => v,
                            Err(err) => {
                                let cancel_err = cancel_order_by_id(
                                    http_client,
                                    api_config,
                                    exec_config,
                                    symbol,
                                    order_id,
                                )
                                .await
                                .err();
                                return Err(match cancel_err {
                                    Some(cancel_err) => anyhow!(
                                        "replacement entry order {} placed but staging synchronized exits failed: {}; canceling replacement entry also failed: {}",
                                        order_id,
                                        err,
                                        cancel_err
                                    ),
                                    None => anyhow!(
                                        "replacement entry order {} placed but staging synchronized exits failed; replacement entry canceled: {}",
                                        order_id,
                                        err
                                    ),
                                });
                            }
                        };
                    info!(
                        symbol = %symbol,
                        replacement_order_id = order_id,
                        take_profit_order_id = tp_order_id,
                        stop_loss_order_id = sl_order_id,
                        tp_trigger_price = %tp_trigger_price,
                        sl_trigger_price = %sl_trigger_price,
                        tp_is_algo_order = tp_is_algo_order,
                        "modify_maker_order_placed_with_synchronized_exit_orders"
                    );
                    tokio::spawn(watch_staged_exit_orders(
                        http_client.clone(),
                        api_config.clone(),
                        exec_config.clone(),
                        symbol.to_string(),
                        position_side.clone(),
                        order_id,
                        Some(TrackedExitOrder {
                            order_id: tp_order_id,
                            is_algo_order: tp_is_algo_order,
                        }),
                        Some(TrackedExitOrder {
                            order_id: sl_order_id,
                            is_algo_order: true,
                        }),
                    ));
                }
            }

            Ok(report)
        }
    }
}

fn pending_modify_maker_requests_effective_change(
    intent: &PendingOrderManagementIntent,
    current_entry: f64,
    current_tp: Option<f64>,
    current_sl: Option<f64>,
    current_leverage: Option<f64>,
    tick_size: f64,
) -> bool {
    pending_numeric_level_changed(intent.new_entry, Some(current_entry), tick_size)
        || pending_numeric_level_changed(intent.new_tp, current_tp, tick_size)
        || pending_numeric_level_changed(intent.new_sl, current_sl, tick_size)
        || pending_scalar_level_changed(intent.new_leverage, current_leverage)
}

fn pending_numeric_level_changed(
    requested: Option<f64>,
    current: Option<f64>,
    tick_size: f64,
) -> bool {
    match (requested, current) {
        (None, _) => false,
        (Some(_), None) => true,
        (Some(requested), Some(current)) => {
            price_changed_by_at_least_one_tick(requested, current, tick_size)
        }
    }
}

fn pending_scalar_level_changed(requested: Option<f64>, current: Option<f64>) -> bool {
    match (requested, current) {
        (None, _) => false,
        (Some(_), None) => true,
        (Some(requested), Some(current)) => (requested - current).abs() > f64::EPSILON,
    }
}

fn resolve_pending_order_exit_levels(
    intent: &PendingOrderManagementIntent,
    current_tp: Option<f64>,
    current_sl: Option<f64>,
    planned_tp: Option<f64>,
    planned_sl: Option<f64>,
) -> Result<(Option<f64>, Option<f64>)> {
    let effective_tp = intent.new_tp.or(current_tp).or(planned_tp);
    let effective_sl = intent.new_sl.or(current_sl).or(planned_sl);
    if effective_tp.is_some() ^ effective_sl.is_some() {
        return Err(anyhow!(
            "MODIFY_MAKER requires both TP and SL when either one is set"
        ));
    }
    Ok((effective_tp, effective_sl))
}

fn validate_reanchor_request(
    intent: &PositionManagementIntent,
    state: &TradingStateSnapshot,
    symbol_filters: &SymbolFilters,
    exec_config: &LlmExecutionConfig,
) -> Result<()> {
    if intent.new_tp.is_none() && intent.new_sl.is_none() {
        return Ok(());
    }
    if !state.has_active_positions {
        return Err(anyhow!(
            "new_tp/new_sl provided but no active positions exist for re-anchor"
        ));
    }
    if exec_config.dry_run {
        return Ok(());
    }
    for position in &state.active_positions {
        let position_side = if exec_config.hedge_mode {
            position.position_side.as_str()
        } else {
            "BOTH"
        };
        let semantic_side = if position.position_amt > 0.0 {
            "LONG"
        } else {
            "SHORT"
        };
        let current_tp = find_live_exit_trigger_price(
            &state.open_orders,
            semantic_side,
            exec_config.hedge_mode,
            Some(position.entry_price),
            ExitKind::TakeProfit,
        );
        let current_sl = find_live_exit_trigger_price(
            &state.open_orders,
            semantic_side,
            exec_config.hedge_mode,
            Some(position.entry_price),
            ExitKind::StopLoss,
        );
        let effective_tp = intent
            .new_tp
            .or(current_tp)
            .ok_or_else(|| anyhow!("re-anchor missing TP for position_side={}", position_side))?;
        let effective_sl = intent
            .new_sl
            .or(current_sl)
            .ok_or_else(|| anyhow!("re-anchor missing SL for position_side={}", position_side))?;
        let is_long = position.position_amt > 0.0;
        if is_long {
            if !(effective_tp > effective_sl) {
                return Err(anyhow!(
                    "re-anchor LONG requires new_tp > new_sl for position_side={}",
                    position_side
                ));
            }
        } else if !(effective_tp < effective_sl) {
            return Err(anyhow!(
                "re-anchor SHORT requires new_tp < new_sl for position_side={}",
                position_side
            ));
        }
        if intent.new_tp.is_some_and(|v| {
            current_tp.is_some_and(|old| {
                !price_changed_by_at_least_one_tick(v, old, symbol_filters.tick_size)
            })
        }) && intent.new_sl.is_some_and(|v| {
            current_sl.is_some_and(|old| {
                !price_changed_by_at_least_one_tick(v, old, symbol_filters.tick_size)
            })
        }) {
            return Err(anyhow!(
                "re-anchor request did not change TP/SL for position_side={}",
                position_side
            ));
        }
    }
    Ok(())
}

async fn apply_reanchor_exits(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    symbol: &str,
    intent: &PositionManagementIntent,
    symbol_filters: &SymbolFilters,
) -> Result<(Vec<i64>, Vec<i64>)> {
    let state = fetch_symbol_trading_state(http_client, api_config, exec_config, symbol).await?;
    if !state.has_active_positions {
        return Err(anyhow!(
            "re-anchor requested but active positions are gone while applying exits"
        ));
    }
    let mut tp_ids = Vec::new();
    let mut sl_ids = Vec::new();
    let cancel_candidates = state
        .active_positions
        .iter()
        .flat_map(|position| {
            let position_side = if exec_config.hedge_mode {
                position.position_side.as_str()
            } else {
                "BOTH"
            };
            collect_exit_orders_for_side(&state.open_orders, position_side, exec_config.hedge_mode)
        })
        .collect::<Vec<_>>();
    cancel_tracked_exit_orders(
        http_client,
        api_config,
        exec_config,
        symbol,
        &cancel_candidates,
    )
    .await?;
    for position in &state.active_positions {
        let position_side = if exec_config.hedge_mode {
            position.position_side.as_str()
        } else {
            "BOTH"
        };
        let semantic_side = if position.position_amt > 0.0 {
            "LONG"
        } else {
            "SHORT"
        };
        let current_tp = find_live_exit_trigger_price(
            &state.open_orders,
            semantic_side,
            exec_config.hedge_mode,
            Some(position.entry_price),
            ExitKind::TakeProfit,
        );
        let current_sl = find_live_exit_trigger_price(
            &state.open_orders,
            semantic_side,
            exec_config.hedge_mode,
            Some(position.entry_price),
            ExitKind::StopLoss,
        );
        let new_tp = intent
            .new_tp
            .or(current_tp)
            .ok_or_else(|| anyhow!("re-anchor missing TP for position_side={}", position_side))?;
        let new_sl = intent
            .new_sl
            .or(current_sl)
            .ok_or_else(|| anyhow!("re-anchor missing SL for position_side={}", position_side))?;
        let is_long = position.position_amt > 0.0;
        let decision_for_quantize = if is_long {
            TradeDecision::Long
        } else {
            TradeDecision::Short
        };
        let exit_side = if is_long { "SELL" } else { "BUY" };
        let tp_price = quantize_exit_price(
            new_tp,
            symbol_filters.tick_size,
            symbol_filters.price_precision,
            decision_for_quantize,
            true,
        );
        let sl_price = quantize_exit_price(
            new_sl,
            symbol_filters.tick_size,
            symbol_filters.price_precision,
            decision_for_quantize,
            false,
        );
        let tp_trigger_price = format_decimal(
            tp_price.max(symbol_filters.tick_size),
            symbol_filters.price_precision,
        );
        let sl_trigger_price = format_decimal(
            sl_price.max(symbol_filters.tick_size),
            symbol_filters.price_precision,
        );
        info!(
            symbol = %symbol,
            position_side = %position_side,
            old_tp = current_tp.unwrap_or_default(),
            old_sl = current_sl.unwrap_or_default(),
            new_tp = new_tp,
            new_sl = new_sl,
            "reanchor_exit_orders"
        );
        let tp_algo_id = place_close_order(
            http_client,
            api_config,
            exec_config,
            symbol,
            exit_side,
            position_side,
            "TAKE_PROFIT_MARKET",
            &format_exit_quantity(
                position.position_amt.abs(),
                symbol_filters.step_size,
                symbol_filters.qty_precision,
            )?,
            &tp_trigger_price,
        )
        .await?;
        let sl_algo_id = place_close_order(
            http_client,
            api_config,
            exec_config,
            symbol,
            exit_side,
            position_side,
            "STOP_MARKET",
            &format_exit_quantity(
                position.position_amt.abs(),
                symbol_filters.step_size,
                symbol_filters.qty_precision,
            )?,
            &sl_trigger_price,
        )
        .await?;
        tp_ids.push(tp_algo_id);
        sl_ids.push(sl_algo_id);
    }
    Ok((tp_ids, sl_ids))
}

async fn fetch_symbol_filters(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    symbol: &str,
) -> Result<SymbolFilters> {
    let url = format!(
        "{}/fapi/v1/exchangeInfo",
        api_config.futures_rest_api_url.trim_end_matches('/')
    );
    let response = http_client
        .get(&url)
        .query(&[("symbol", symbol)])
        .send()
        .await
        .context("fetch binance futures exchangeInfo")?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!(
            "binance exchangeInfo failed status={} body={}",
            status,
            body
        ));
    }

    let info: FuturesExchangeInfo = response
        .json()
        .await
        .context("decode binance futures exchangeInfo")?;
    let symbol_info = info
        .symbols
        .into_iter()
        .find(|item| item.symbol.eq_ignore_ascii_case(symbol))
        .ok_or_else(|| anyhow!("symbol {} not found in binance exchangeInfo", symbol))?;

    parse_symbol_filters(symbol, &symbol_info)
}

async fn fetch_book_ticker(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    symbol: &str,
) -> Result<FuturesBookTicker> {
    let url = format!(
        "{}/fapi/v1/ticker/bookTicker",
        api_config.futures_rest_api_url.trim_end_matches('/')
    );
    let response = http_client
        .get(&url)
        .query(&[("symbol", symbol)])
        .send()
        .await
        .context("fetch binance futures bookTicker")?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!(
            "binance bookTicker failed status={} body={}",
            status,
            body
        ));
    }
    let row: FuturesBookTicker = response
        .json()
        .await
        .context("decode binance futures bookTicker")?;
    Ok(row)
}

fn derive_maker_entry_price(
    decision: TradeDecision,
    model_entry_price: f64,
    book_ticker: &FuturesBookTicker,
    filters: &SymbolFilters,
) -> f64 {
    let bid = book_ticker
        .bid_price
        .parse::<f64>()
        .unwrap_or(model_entry_price);
    let ask = book_ticker
        .ask_price
        .parse::<f64>()
        .unwrap_or(model_entry_price);
    let target = match decision {
        TradeDecision::Long => model_entry_price.min(bid.max(filters.tick_size)),
        TradeDecision::Short => model_entry_price.max(ask.max(filters.tick_size)),
        TradeDecision::NoTrade => model_entry_price,
    };
    let steps = target / filters.tick_size;
    let stepped = match decision {
        TradeDecision::Long => steps.floor() * filters.tick_size,
        TradeDecision::Short => steps.ceil() * filters.tick_size,
        TradeDecision::NoTrade => target,
    };
    let formatted = format!(
        "{:.*}",
        filters.price_precision,
        stepped.max(filters.tick_size)
    );
    formatted
        .parse::<f64>()
        .unwrap_or(stepped.max(filters.tick_size))
}

fn parse_book_ticker_price(raw: &str, fallback: f64) -> f64 {
    raw.parse::<f64>().unwrap_or(fallback)
}

fn build_trade_blocked_by_current_price_beyond_stop_loss(
    decision: TradeDecision,
    entry_price: f64,
    stop_loss: f64,
    best_bid_price: f64,
    best_ask_price: f64,
) -> Option<TradeExecutionBlockedByCurrentPriceBeyondStopLoss> {
    let (current_reference_price, current_price_source, crossed_stop_loss) = match decision {
        TradeDecision::Long => (
            best_bid_price,
            "best_bid_price",
            best_bid_price <= stop_loss + f64::EPSILON,
        ),
        TradeDecision::Short => (
            best_ask_price,
            "best_ask_price",
            best_ask_price >= stop_loss - f64::EPSILON,
        ),
        TradeDecision::NoTrade => return None,
    };

    if !crossed_stop_loss {
        return None;
    }

    Some(TradeExecutionBlockedByCurrentPriceBeyondStopLoss {
        decision,
        current_reference_price,
        current_price_source,
        entry_price,
        stop_loss,
        best_bid_price,
        best_ask_price,
    })
}

fn price_changed_by_at_least_one_tick(requested: f64, current: f64, tick_size: f64) -> bool {
    if tick_size <= 0.0 {
        return (requested - current).abs() > f64::EPSILON;
    }
    ((requested - current).abs() / tick_size) + 1e-9 >= 1.0
}

fn recompute_stop_loss_from_target_rr(
    decision: TradeDecision,
    maker_entry_price: f64,
    take_profit_price: f64,
    target_rr: f64,
    tick_size: f64,
    precision: usize,
) -> Result<f64> {
    if target_rr <= f64::EPSILON {
        return Err(anyhow!("rr must be > 0"));
    }

    let reward = match decision {
        TradeDecision::Long => take_profit_price - maker_entry_price,
        TradeDecision::Short => maker_entry_price - take_profit_price,
        TradeDecision::NoTrade => {
            return Err(anyhow!("NO_TRADE does not produce an exchange order"));
        }
    };
    if reward <= f64::EPSILON {
        return Err(anyhow!(
            "final execution geometry invalid: maker_entry_price={} tp={} decision={}",
            maker_entry_price,
            take_profit_price,
            decision.as_str()
        ));
    }

    let risk = reward / target_rr;
    if risk <= f64::EPSILON {
        return Err(anyhow!(
            "computed execution risk is invalid: reward={} rr={}",
            reward,
            target_rr
        ));
    }

    let raw_stop_loss = match decision {
        TradeDecision::Long => maker_entry_price - risk,
        TradeDecision::Short => maker_entry_price + risk,
        TradeDecision::NoTrade => unreachable!(),
    };

    let quantized_stop_loss = match decision {
        // Tighten toward entry so tick quantization preserves or improves the model RR.
        TradeDecision::Long => quantize_price(raw_stop_loss, tick_size, precision, true),
        TradeDecision::Short => quantize_price(raw_stop_loss, tick_size, precision, false),
        TradeDecision::NoTrade => unreachable!(),
    };

    Ok(quantized_stop_loss)
}

fn compute_execution_rr(
    decision: TradeDecision,
    maker_entry_price: f64,
    take_profit_price: f64,
    stop_loss_price: f64,
) -> Result<f64> {
    let (reward, risk) = match decision {
        TradeDecision::Long => {
            if !(take_profit_price > maker_entry_price && stop_loss_price < maker_entry_price) {
                return Err(anyhow!(
                    "LONG final execution requires tp > maker_entry_price and sl < maker_entry_price"
                ));
            }
            (
                take_profit_price - maker_entry_price,
                maker_entry_price - stop_loss_price,
            )
        }
        TradeDecision::Short => {
            if !(take_profit_price < maker_entry_price && stop_loss_price > maker_entry_price) {
                return Err(anyhow!(
                    "SHORT final execution requires tp < maker_entry_price and sl > maker_entry_price"
                ));
            }
            (
                maker_entry_price - take_profit_price,
                stop_loss_price - maker_entry_price,
            )
        }
        TradeDecision::NoTrade => {
            return Err(anyhow!("NO_TRADE does not produce an exchange order"));
        }
    };

    if reward <= f64::EPSILON || risk <= f64::EPSILON {
        return Err(anyhow!(
            "final execution reward/risk must be > 0 (reward={}, risk={})",
            reward,
            risk
        ));
    }

    Ok(reward / risk)
}

fn validate_final_execution_rr(
    decision: TradeDecision,
    maker_entry_price: f64,
    take_profit_price: f64,
    stop_loss_price: f64,
    target_rr: f64,
) -> Result<f64> {
    let final_rr = compute_execution_rr(
        decision,
        maker_entry_price,
        take_profit_price,
        stop_loss_price,
    )?;
    if final_rr + 1e-9 < target_rr {
        return Err(anyhow!(
            "final execution rr {:.6} fell below model rr {:.6}",
            final_rr,
            target_rr
        ));
    }
    Ok(final_rr)
}

fn quantize_price(raw_price: f64, tick_size: f64, precision: usize, round_up: bool) -> f64 {
    let steps = raw_price / tick_size;
    let stepped = if round_up {
        steps.ceil() * tick_size
    } else {
        steps.floor() * tick_size
    };
    let formatted = format!("{:.*}", precision, stepped.max(tick_size));
    formatted.parse::<f64>().unwrap_or(stepped.max(tick_size))
}

fn parse_symbol_filters(symbol: &str, symbol_info: &FuturesSymbolInfo) -> Result<SymbolFilters> {
    let mut market_step_size = None;
    let mut lot_step_size = None;
    let mut market_min_qty = None;
    let mut lot_min_qty = None;
    let mut tick_size = None;

    for filter in &symbol_info.filters {
        let filter_type = filter
            .get("filterType")
            .and_then(Value::as_str)
            .unwrap_or_default();
        match filter_type {
            "MARKET_LOT_SIZE" => {
                market_step_size = parse_filter_f64(filter, "stepSize");
                market_min_qty = parse_filter_f64(filter, "minQty");
            }
            "LOT_SIZE" => {
                lot_step_size = parse_filter_f64(filter, "stepSize");
                lot_min_qty = parse_filter_f64(filter, "minQty");
            }
            "PRICE_FILTER" => {
                tick_size = parse_filter_f64(filter, "tickSize");
            }
            _ => {}
        }
    }

    let step_size = market_step_size
        .or(lot_step_size)
        .ok_or_else(|| anyhow!("{} missing MARKET_LOT_SIZE/LOT_SIZE.stepSize", symbol))?;
    let min_qty = market_min_qty
        .or(lot_min_qty)
        .ok_or_else(|| anyhow!("{} missing MARKET_LOT_SIZE/LOT_SIZE.minQty", symbol))?;
    let tick_size = tick_size.ok_or_else(|| anyhow!("{} missing PRICE_FILTER.tickSize", symbol))?;

    Ok(SymbolFilters {
        step_size,
        min_qty,
        tick_size,
        qty_precision: precision_from_step(step_size),
        price_precision: precision_from_step(tick_size),
    })
}

fn parse_filter_f64(filter: &Value, key: &str) -> Option<f64> {
    filter
        .get(key)
        .and_then(Value::as_str)
        .and_then(|raw| raw.parse::<f64>().ok())
}

fn parse_optional_str(value: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(v) = value.get(*key).and_then(Value::as_str) {
            return Some(v.to_string());
        }
    }
    None
}

fn parse_optional_f64(value: &Value, keys: &[&str]) -> Option<f64> {
    for key in keys {
        if let Some(v) = value.get(*key) {
            if let Some(n) = v.as_f64() {
                return Some(n);
            }
            if let Some(raw) = v.as_str() {
                if let Ok(parsed) = raw.parse::<f64>() {
                    return Some(parsed);
                }
            }
        }
    }
    None
}

fn parse_optional_bool(value: &Value, keys: &[&str]) -> Option<bool> {
    for key in keys {
        if let Some(v) = value.get(*key) {
            if let Some(b) = v.as_bool() {
                return Some(b);
            }
            if let Some(raw) = v.as_str() {
                if let Ok(parsed) = raw.parse::<bool>() {
                    return Some(parsed);
                }
            }
        }
    }
    None
}

fn parse_optional_id(value: &Value, keys: &[&str]) -> Option<i64> {
    for key in keys {
        if let Some(v) = value.get(*key) {
            if let Some(id) = v.as_i64() {
                return Some(id);
            }
            if let Some(raw) = v.as_str() {
                if let Ok(parsed) = raw.parse::<i64>() {
                    return Some(parsed);
                }
            }
        }
    }
    None
}

fn has_active_position_for_side(
    state: &TradingStateSnapshot,
    position_side: &str,
    hedge_mode: bool,
) -> bool {
    positions_have_active_position_for_side(&state.active_positions, position_side, hedge_mode)
}

fn positions_have_active_position_for_side(
    positions: &[ActivePositionSnapshot],
    position_side: &str,
    hedge_mode: bool,
) -> bool {
    if hedge_mode {
        positions.iter().any(|position| {
            position.position_amt.abs() > f64::EPSILON
                && position.position_side.eq_ignore_ascii_case(position_side)
        })
    } else {
        positions
            .iter()
            .any(|position| position.position_amt.abs() > f64::EPSILON)
    }
}

fn active_position_abs_qty_for_side(
    state: &TradingStateSnapshot,
    position_side: &str,
    hedge_mode: bool,
) -> f64 {
    if hedge_mode {
        state
            .active_positions
            .iter()
            .filter(|position| {
                position.position_amt.abs() > f64::EPSILON
                    && position.position_side.eq_ignore_ascii_case(position_side)
            })
            .map(|position| position.position_amt.abs())
            .next()
            .unwrap_or(0.0)
    } else {
        state
            .active_positions
            .iter()
            .filter(|position| position.position_amt.abs() > f64::EPSILON)
            .map(|position| position.position_amt.abs())
            .sum()
    }
}

async fn wait_for_position_reconcile_after_reduce(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    symbol: &str,
    expected_remaining_positions: &[(String, f64)],
    qty_tolerance: f64,
) -> Result<()> {
    const MAX_ATTEMPTS: usize = 8;
    const RETRY_DELAY_MS: u64 = 250;

    if expected_remaining_positions.is_empty() {
        return Ok(());
    }

    let tolerance = qty_tolerance.max(f64::EPSILON);
    let mut last_mismatch = String::new();
    for attempt in 0..MAX_ATTEMPTS {
        let state =
            fetch_symbol_trading_state(http_client, api_config, exec_config, symbol).await?;
        let mismatches = expected_remaining_positions
            .iter()
            .filter_map(|(position_side, expected_qty)| {
                let actual_qty =
                    active_position_abs_qty_for_side(&state, position_side, exec_config.hedge_mode);
                let matches = if *expected_qty <= tolerance {
                    actual_qty <= tolerance
                } else {
                    (actual_qty - expected_qty).abs() <= tolerance
                };
                (!matches).then(|| {
                    format!(
                        "{} expected {:.8} actual {:.8}",
                        position_side, expected_qty, actual_qty
                    )
                })
            })
            .collect::<Vec<_>>();
        if mismatches.is_empty() {
            return Ok(());
        }
        last_mismatch = mismatches.join(", ");
        if attempt + 1 < MAX_ATTEMPTS {
            sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
        }
    }

    Err(anyhow!(
        "position state did not reconcile after reduce for {}: {}",
        symbol,
        last_mismatch
    ))
}

fn should_cleanup_staged_exit_orders(
    state: &TradingStateSnapshot,
    entry_order_id: i64,
    position_side: &str,
    hedge_mode: bool,
) -> bool {
    !has_active_position_for_side(state, position_side, hedge_mode)
        && !state
            .open_orders
            .iter()
            .any(|o| o.order_id == entry_order_id)
}

async fn should_cleanup_staged_exit_orders_after_confirmation(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    symbol: &str,
    position_side: &str,
    watch_started_at: Instant,
) -> Result<Option<&'static str>> {
    if has_recent_account_flatten_event(symbol, position_side, watch_started_at) {
        return Ok(Some("account_update_flatten"));
    }

    if is_position_side_flat_on_rest(http_client, api_config, exec_config, symbol, position_side)
        .await?
    {
        return Ok(Some("rest_confirmed_flat"));
    }

    Ok(None)
}

fn resolve_modify_tpsl_targets(
    intent: &PositionManagementIntent,
    current_tp: Option<f64>,
    current_sl: Option<f64>,
    tick_size: f64,
    is_long: bool,
) -> Result<(f64, f64)> {
    let new_tp = intent.new_tp.or(current_tp).ok_or_else(|| {
        anyhow!("MODIFY_TPSL missing TP: provide params.new_tp or keep an existing TP order")
    })?;
    let new_sl = intent.new_sl.or(current_sl).ok_or_else(|| {
        anyhow!("MODIFY_TPSL missing SL: provide params.new_sl or keep an existing SL order")
    })?;

    let tp_changed =
        current_tp.is_none_or(|v| price_changed_by_at_least_one_tick(new_tp, v, tick_size));
    let sl_changed =
        current_sl.is_none_or(|v| price_changed_by_at_least_one_tick(new_sl, v, tick_size));
    if !tp_changed && !sl_changed {
        return Err(anyhow!(
            "MODIFY_TPSL new_tp/new_sl equal current live exits; no change requested"
        ));
    }

    if is_long {
        if !(new_tp > new_sl) {
            return Err(anyhow!("MODIFY_TPSL for LONG requires new_tp > new_sl"));
        }
    } else if !(new_tp < new_sl) {
        return Err(anyhow!("MODIFY_TPSL for SHORT requires new_tp < new_sl"));
    }

    Ok((new_tp, new_sl))
}

fn parse_numeric_id(value: &Value, keys: &[&str], label: &str) -> Result<i64> {
    parse_optional_id(value, keys)
        .ok_or_else(|| anyhow!("{} missing from response body={}", label, value))
}

fn select_leverage(
    exec_config: &LlmExecutionConfig,
    intent: &TradeIntent,
) -> Result<(u32, &'static str)> {
    let model_leverage = intent
        .leverage
        .ok_or_else(|| anyhow!("model leverage is missing"))?;
    let scaled_leverage = model_leverage * exec_config.default_leverage_ratio;
    Ok((
        normalize_leverage(scaled_leverage, exec_config.max_leverage),
        "model_ratio",
    ))
}

fn normalize_leverage(leverage: f64, max_leverage: u32) -> u32 {
    leverage.round().clamp(1.0, max_leverage as f64) as u32
}

fn resolve_position_side(
    exec_config: &LlmExecutionConfig,
    decision: TradeDecision,
) -> &'static str {
    if !exec_config.hedge_mode {
        return "BOTH";
    }
    match decision {
        TradeDecision::Long => "LONG",
        TradeDecision::Short => "SHORT",
        TradeDecision::NoTrade => "BOTH",
    }
}

fn select_margin_budget(
    exec_config: &LlmExecutionConfig,
    account_balance: &FuturesAccountBalance,
) -> Result<(f64, &'static str)> {
    if exec_config.account_margin_ratio > 0.0 {
        let budget = account_balance.total_wallet_balance * exec_config.account_margin_ratio;
        if budget <= 0.0 {
            return Err(anyhow!(
                "computed margin budget from account balance is <= 0: total_wallet_balance={} ratio={}",
                account_balance.total_wallet_balance,
                exec_config.account_margin_ratio
            ));
        }
        return Ok((budget, "account_ratio"));
    }

    Ok((exec_config.margin_usdt, "fixed_usdt"))
}

fn compute_order_quantity(
    filters: &SymbolFilters,
    margin_budget_usdt: f64,
    entry_price: f64,
    leverage: u32,
) -> Result<f64> {
    let target_notional = margin_budget_usdt * leverage as f64;
    let raw_qty = target_notional / entry_price;
    let normalized_qty = round_down_to_step(raw_qty, filters.step_size);
    if normalized_qty < filters.min_qty {
        return Err(anyhow!(
            "computed quantity {:.8} is below exchange minQty {:.8}",
            normalized_qty,
            filters.min_qty
        ));
    }
    Ok(normalized_qty)
}

async fn fetch_account_balance(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
) -> Result<FuturesAccountBalance> {
    ensure_account_ws_listener_started(http_client, api_config, exec_config);
    if let Ok(guard) = account_ws_state().lock() {
        if guard.has_account_update {
            if let (Some(total_wallet_balance), Some(available_balance)) =
                (guard.total_wallet_balance, guard.available_balance)
            {
                return Ok(FuturesAccountBalance {
                    total_wallet_balance,
                    available_balance,
                });
            }
        }
    }

    let account: FuturesAccountResponse = signed_get_json(
        http_client,
        api_config,
        exec_config,
        "/fapi/v2/account",
        Vec::new(),
    )
    .await?;
    let total_wallet_balance = account
        .total_wallet_balance
        .parse::<f64>()
        .context("parse totalWalletBalance")?;
    let available_balance = account
        .available_balance
        .parse::<f64>()
        .context("parse availableBalance")?;
    if let Ok(mut guard) = account_ws_state().lock() {
        guard.total_wallet_balance = Some(total_wallet_balance);
        guard.available_balance = Some(available_balance);
    }
    Ok(FuturesAccountBalance {
        total_wallet_balance,
        available_balance,
    })
}

async fn fetch_active_positions_with_source(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    symbol: &str,
) -> Result<(Vec<ActivePositionSnapshot>, ActivePositionSource)> {
    ensure_account_ws_listener_started(http_client, api_config, exec_config);
    if let Ok(guard) = account_ws_state().lock() {
        if guard.has_account_update {
            let mut out = Vec::new();
            for ((sym, _side), pos) in &guard.positions {
                if sym.eq_ignore_ascii_case(symbol) && pos.position_amt.abs() > f64::EPSILON {
                    out.push(pos.clone());
                }
            }
            return Ok((out, ActivePositionSource::AccountWs));
        }
    }

    fetch_active_positions_from_rest(http_client, api_config, exec_config, symbol)
        .await
        .map(|positions| (positions, ActivePositionSource::Rest))
}

async fn fetch_active_positions_from_rest(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    symbol: &str,
) -> Result<Vec<ActivePositionSnapshot>> {
    let positions: Vec<FuturesPositionRiskRow> = signed_get_json(
        http_client,
        api_config,
        exec_config,
        "/fapi/v2/positionRisk",
        vec![("symbol".to_string(), symbol.to_string())],
    )
    .await?;

    let mut out = Vec::new();
    for row in positions {
        let position_amt = row.position_amt.parse::<f64>().unwrap_or(0.0);
        if position_amt.abs() <= f64::EPSILON {
            continue;
        }
        let entry_price = row.entry_price.parse::<f64>().unwrap_or(0.0);
        let mark_price = row.mark_price.parse::<f64>().unwrap_or(0.0);
        let unrealized_pnl = row.un_realized_profit.parse::<f64>().unwrap_or(0.0);
        let leverage = row.leverage.parse::<u32>().unwrap_or(1);
        out.push(ActivePositionSnapshot {
            position_side: row.position_side,
            position_amt,
            entry_price,
            mark_price,
            unrealized_pnl,
            leverage,
        });
    }
    sync_account_ws_positions_from_rest(symbol, &out);
    Ok(out)
}

fn sync_account_ws_positions_from_rest(symbol: &str, positions: &[ActivePositionSnapshot]) {
    let state = account_ws_state();
    let Ok(mut guard) = state.lock() else {
        return;
    };

    let stale_keys = guard
        .positions
        .keys()
        .filter(|(sym, _)| sym.eq_ignore_ascii_case(symbol))
        .cloned()
        .collect::<Vec<_>>();
    for key in stale_keys {
        guard.positions.remove(&key);
    }

    for position in positions {
        let key = (symbol.to_string(), position.position_side.clone());
        guard.flattened_at.remove(&key);
        guard.positions.insert(key, position.clone());
    }
}

fn should_reconcile_ws_positions_with_rest(
    source: ActivePositionSource,
    active_positions: &[ActivePositionSnapshot],
    open_orders: &[OpenOrderSnapshot],
) -> bool {
    matches!(source, ActivePositionSource::AccountWs)
        && !active_positions.is_empty()
        && open_orders.is_empty()
}

async fn is_position_side_flat_on_rest(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    symbol: &str,
    position_side: &str,
) -> Result<bool> {
    let positions =
        fetch_active_positions_from_rest(http_client, api_config, exec_config, symbol).await?;
    Ok(!positions_have_active_position_for_side(
        &positions,
        position_side,
        exec_config.hedge_mode,
    ))
}

pub(crate) async fn fetch_pending_order_leverage(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    symbol: &str,
) -> Result<u32> {
    let positions: Vec<FuturesPositionRiskRow> = signed_get_json(
        http_client,
        api_config,
        exec_config,
        "/fapi/v2/positionRisk",
        vec![("symbol".to_string(), symbol.to_string())],
    )
    .await?;

    positions
        .into_iter()
        .find_map(|row| row.leverage.parse::<u32>().ok())
        .ok_or_else(|| anyhow!("positionRisk leverage missing for {}", symbol))
}

/// Returns `(take_profit_order_id, tp_is_algo_order, stop_loss_order_id)`.
///
/// When `is_maker_entry` is true the TP is placed as a conditional STOP algo-order
/// (triggered at entry_price_str, limit at tp_trigger_price) so that Binance accepts
/// the order type on the algo endpoint even before a position exists.
async fn place_staged_exit_orders(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    symbol: &str,
    position_side: &str,
    exit_side: &str,
    quantity: &str,
    tp_trigger_price: &str,
    sl_trigger_price: &str,
    is_maker_entry: bool,
    entry_price_str: &str,
) -> Result<(i64, bool, i64)> {
    let stop_loss_order_id = match place_close_order(
        http_client,
        api_config,
        exec_config,
        symbol,
        exit_side,
        position_side,
        "STOP_MARKET",
        quantity,
        sl_trigger_price,
    )
    .await
    {
        Ok(order_id) => order_id,
        Err(err) => {
            let err_chain = format!("{:#}", err);
            warn!(
                symbol = %symbol,
                position_side = %position_side,
                exit_side = %exit_side,
                sl_trigger_price = %sl_trigger_price,
                tp_trigger_price = %tp_trigger_price,
                error = %err_chain,
                "stage stop-loss rejected by binance while staging synchronized exits"
            );
            println!(
                "LLM_STAGE_STOP_LOSS_ERROR symbol={} position_side={} exit_side={} sl_trigger_price={} tp_trigger_price={} error={}",
                symbol,
                position_side,
                exit_side,
                sl_trigger_price,
                tp_trigger_price,
                err_chain.replace('\n', " | "),
            );
            return Err(err).with_context(|| format!("stage stop-loss for {}", symbol));
        }
    };

    // For maker (pending limit) entries the TP trigger price may already be beyond the
    // current market price, causing Binance to reject TAKE_PROFIT_MARKET algo-orders
    // because there is no open position yet. Use a STOP limit algo-order instead: it
    // activates at the maker entry price and leaves a GTC reduce-only limit exit at the
    // take-profit level once the entry has filled.
    let tp_result = if is_maker_entry {
        place_tp_as_stop_limit_algo_order(
            http_client,
            api_config,
            exec_config,
            symbol,
            exit_side,
            position_side,
            quantity,
            entry_price_str,
            tp_trigger_price,
        )
        .await
        .map(|id| (id, true))
    } else {
        place_close_order(
            http_client,
            api_config,
            exec_config,
            symbol,
            exit_side,
            position_side,
            "TAKE_PROFIT_MARKET",
            quantity,
            tp_trigger_price,
        )
        .await
        .map(|id| (id, true))
    };

    match tp_result {
        Ok((take_profit_order_id, tp_is_algo)) => {
            Ok((take_profit_order_id, tp_is_algo, stop_loss_order_id))
        }
        Err(err) => {
            let err_chain = format!("{:#}", err);
            let tp_order_mode = if is_maker_entry {
                "STOP_LIMIT_PENDING_EXIT_ALGO"
            } else {
                "TAKE_PROFIT_MARKET_ALGO"
            };
            warn!(
                symbol = %symbol,
                position_side = %position_side,
                exit_side = %exit_side,
                quantity = %quantity,
                entry_price = %entry_price_str,
                tp_trigger_price = %tp_trigger_price,
                sl_trigger_price = %sl_trigger_price,
                stop_loss_order_id = stop_loss_order_id,
                is_maker_entry = is_maker_entry,
                tp_order_mode = tp_order_mode,
                error = %err_chain,
                "stage take-profit rejected by binance while staging synchronized exits"
            );
            println!(
                "LLM_STAGE_TAKE_PROFIT_ERROR symbol={} position_side={} exit_side={} quantity={} entry_price={} tp_trigger_price={} sl_trigger_price={} stop_loss_order_id={} is_maker_entry={} tp_order_mode={} error={}",
                symbol,
                position_side,
                exit_side,
                quantity,
                entry_price_str,
                tp_trigger_price,
                sl_trigger_price,
                stop_loss_order_id,
                is_maker_entry,
                tp_order_mode,
                err_chain.replace('\n', " | "),
            );
            // SL is always an algo order — cancel it to avoid orphaned exits
            if let Err(cancel_err) = cancel_algo_order_by_id(
                http_client,
                api_config,
                exec_config,
                symbol,
                stop_loss_order_id,
            )
            .await
            {
                warn!(
                    symbol = %symbol,
                    stop_loss_order_id = stop_loss_order_id,
                    error = %cancel_err,
                    "cleanup after staged take-profit failure could not cancel stop-loss"
                );
            }
            Err(err).with_context(|| format!("stage take-profit for {}", symbol))
        }
    }
}

async fn watch_staged_exit_orders(
    http_client: Client,
    api_config: BinanceApiConfig,
    exec_config: LlmExecutionConfig,
    symbol: String,
    position_side: String,
    entry_order_id: i64,
    take_profit_order: Option<TrackedExitOrder>,
    stop_loss_order: Option<TrackedExitOrder>,
) {
    const STAGED_EXIT_CLEANUP_INTERVAL_SECS: u64 = 15;

    if take_profit_order.is_none() && stop_loss_order.is_none() {
        return;
    }

    let watch_started_at = Instant::now();
    let mut ticker = tokio::time::interval(Duration::from_secs(STAGED_EXIT_CLEANUP_INTERVAL_SECS));
    ticker.tick().await; // skip the immediate first tick

    loop {
        ticker.tick().await;
        let state = match fetch_symbol_trading_state(
            &http_client,
            &api_config,
            &exec_config,
            &symbol,
        )
        .await
        {
            Ok(s) => s,
            Err(err) => {
                warn!(
                    symbol = %symbol,
                    entry_order_id = entry_order_id,
                    error = %err,
                    "staged_exit_cleanup: state_fetch_failed"
                );
                continue;
            }
        };

        if !should_cleanup_staged_exit_orders(
            &state,
            entry_order_id,
            &position_side,
            exec_config.hedge_mode,
        ) {
            continue;
        }

        let cleanup_confirmation = match should_cleanup_staged_exit_orders_after_confirmation(
            &http_client,
            &api_config,
            &exec_config,
            &symbol,
            &position_side,
            watch_started_at,
        )
        .await
        {
            Ok(Some(source)) => source,
            Ok(None) => continue,
            Err(err) => {
                warn!(
                    symbol = %symbol,
                    entry_order_id = entry_order_id,
                    error = %err,
                    "staged_exit_cleanup: flat_confirmation_failed"
                );
                continue;
            }
        };

        for (label, order) in [
            ("take_profit", take_profit_order),
            ("stop_loss", stop_loss_order),
        ] {
            let Some(order) = order else {
                continue;
            };
            if !state
                .open_orders
                .iter()
                .any(|o| o.order_id == order.order_id)
            {
                continue;
            }
            let cancel_result = if order.is_algo_order {
                cancel_algo_order_by_id(
                    &http_client,
                    &api_config,
                    &exec_config,
                    &symbol,
                    order.order_id,
                )
                .await
            } else {
                cancel_order_by_id(
                    &http_client,
                    &api_config,
                    &exec_config,
                    &symbol,
                    order.order_id,
                )
                .await
            };
            if let Err(err) = cancel_result {
                if is_order_already_gone_error(&err) {
                    info!(
                        symbol = %symbol,
                        entry_order_id = entry_order_id,
                        exit_order_kind = label,
                        exit_order_id = order.order_id,
                        "staged_exit_cleanup: exit already gone"
                    );
                    continue;
                }
                warn!(
                    symbol = %symbol,
                    entry_order_id = entry_order_id,
                    exit_order_kind = label,
                    exit_order_id = order.order_id,
                    error = %err,
                    "staged_exit_cleanup: cancel_exit_failed"
                );
            } else {
                info!(
                    symbol = %symbol,
                    entry_order_id = entry_order_id,
                    exit_order_kind = label,
                    exit_order_id = order.order_id,
                    "staged_exit_cleanup: exit_order_canceled"
                );
            }
        }

        info!(
            symbol = %symbol,
            entry_order_id = entry_order_id,
            cleanup_confirmation = cleanup_confirmation,
            "staged_exit_cleanup: entry_gone_and_flat"
        );
        return;
    }
}

async fn create_user_data_listen_key(
    http_client: &Client,
    api_config: &BinanceApiConfig,
) -> Result<String> {
    let api_key = api_config.resolved_api_key();
    let url = format!(
        "{}/fapi/v1/listenKey",
        api_config.futures_rest_api_url.trim_end_matches('/')
    );
    let response = http_client
        .post(url)
        .header("X-MBX-APIKEY", api_key.as_str())
        .send()
        .await
        .context("request futures user data listenKey")?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!(
            "create futures listenKey failed status={} body={}",
            status,
            body
        ));
    }
    let value: Value = serde_json::from_str(&body).context("decode futures listenKey response")?;
    value
        .get("listenKey")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| anyhow!("listenKey missing in futures listenKey response"))
}

async fn delete_user_data_listen_key(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    listen_key: &str,
) -> Result<()> {
    let api_key = api_config.resolved_api_key();
    let url = format!(
        "{}/fapi/v1/listenKey",
        api_config.futures_rest_api_url.trim_end_matches('/')
    );
    let response = http_client
        .delete(url)
        .header("X-MBX-APIKEY", api_key.as_str())
        .query(&[("listenKey", listen_key)])
        .send()
        .await
        .context("delete futures user data listenKey")?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!(
            "delete futures listenKey failed status={} body={}",
            status,
            body
        ));
    }
    Ok(())
}

fn build_futures_user_stream_ws_url(
    api_config: &BinanceApiConfig,
    listen_key: &str,
) -> Result<String> {
    let rest_base = api_config.futures_rest_api_url.trim_end_matches('/');
    let ws_base = if rest_base.contains("testnet.binancefuture.com") {
        "wss://stream.binancefuture.com/ws"
    } else {
        "wss://fstream.binance.com/ws"
    };
    Ok(format!("{}/{}", ws_base, listen_key))
}

async fn fetch_open_orders(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    symbol: &str,
) -> Result<Vec<OpenOrderSnapshot>> {
    let orders: Vec<FuturesOpenOrderRow> = signed_get_json(
        http_client,
        api_config,
        exec_config,
        "/fapi/v1/openOrders",
        vec![("symbol".to_string(), symbol.to_string())],
    )
    .await?;
    Ok(orders
        .into_iter()
        .map(|row| OpenOrderSnapshot {
            order_id: row.order_id,
            side: row.side,
            position_side: row.position_side,
            order_type: row.order_type,
            status: row.status,
            orig_qty: row.orig_qty.parse::<f64>().unwrap_or(0.0),
            executed_qty: row.executed_qty.parse::<f64>().unwrap_or(0.0),
            price: row.price.parse::<f64>().unwrap_or(0.0),
            stop_price: row.stop_price.parse::<f64>().unwrap_or(0.0),
            close_position: row.close_position,
            reduce_only: row.reduce_only,
            is_algo_order: false,
        })
        .collect())
}

async fn fetch_order_realized_pnl(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    symbol: &str,
    order_id: i64,
) -> Result<f64> {
    let rows: Vec<FuturesUserTradeRow> = signed_get_json(
        http_client,
        api_config,
        exec_config,
        "/fapi/v1/userTrades",
        vec![
            ("symbol".to_string(), symbol.to_string()),
            ("orderId".to_string(), order_id.to_string()),
        ],
    )
    .await?;
    Ok(rows
        .iter()
        .map(|row| row.realized_pnl.parse::<f64>().unwrap_or(0.0))
        .sum())
}

async fn fetch_open_algo_orders(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    symbol: &str,
) -> Result<Vec<OpenOrderSnapshot>> {
    let rows: Vec<Value> = signed_get_json(
        http_client,
        api_config,
        exec_config,
        "/fapi/v1/openAlgoOrders",
        vec![("symbol".to_string(), symbol.to_string())],
    )
    .await?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        if !row.is_object() {
            continue;
        }
        let order_id = parse_optional_id(&row, &["algoId", "orderId"]).unwrap_or_default();
        out.push(OpenOrderSnapshot {
            order_id,
            side: parse_optional_str(&row, &["side"]).unwrap_or_else(|| "-".to_string()),
            position_side: parse_optional_str(&row, &["positionSide"])
                .unwrap_or_else(|| "-".to_string()),
            order_type: parse_optional_str(&row, &["type", "algoType", "strategyType"])
                .unwrap_or_else(|| "ALGO".to_string()),
            status: parse_optional_str(&row, &["status"]).unwrap_or_else(|| "NEW".to_string()),
            orig_qty: parse_optional_f64(&row, &["quantity", "origQty"]).unwrap_or(0.0),
            executed_qty: parse_optional_f64(&row, &["executedQty"]).unwrap_or(0.0),
            price: parse_optional_f64(&row, &["price"]).unwrap_or(0.0),
            stop_price: parse_optional_f64(&row, &["triggerPrice", "stopPrice"]).unwrap_or(0.0),
            close_position: parse_optional_bool(&row, &["closePosition"]).unwrap_or(false),
            reduce_only: parse_optional_bool(&row, &["reduceOnly"]).unwrap_or(false),
            is_algo_order: true,
        });
    }
    Ok(out)
}

fn quantize_exit_price(
    raw_price: f64,
    tick_size: f64,
    precision: usize,
    decision: TradeDecision,
    is_take_profit: bool,
) -> f64 {
    let steps = raw_price / tick_size;
    let stepped = match (decision, is_take_profit) {
        (TradeDecision::Long, true) | (TradeDecision::Short, false) => steps.ceil() * tick_size,
        (TradeDecision::Long, false) | (TradeDecision::Short, true) => steps.floor() * tick_size,
        (TradeDecision::NoTrade, _) => raw_price,
    };
    let formatted = format!("{:.*}", precision, stepped.max(tick_size));
    formatted.parse::<f64>().unwrap_or(stepped.max(tick_size))
}

async fn set_futures_leverage(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    symbol: &str,
    leverage: u32,
) -> Result<()> {
    signed_post_json::<Value>(
        http_client,
        api_config,
        exec_config,
        "/fapi/v1/leverage",
        vec![
            ("symbol".to_string(), symbol.to_string()),
            ("leverage".to_string(), leverage.to_string()),
        ],
    )
    .await
    .map(|_| ())
}

async fn place_entry_market_order(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    symbol: &str,
    decision: TradeDecision,
    position_side: &str,
    quantity: &str,
    price: &str,
    tick_size: f64,
    price_precision: usize,
    allow_taker_fallback: bool,
) -> Result<i64> {
    let side = match decision {
        TradeDecision::Long => "BUY",
        TradeDecision::Short => "SELL",
        TradeDecision::NoTrade => unreachable!(),
    };
    place_limit_post_only_order_with_side(
        http_client,
        api_config,
        exec_config,
        symbol,
        side,
        position_side,
        quantity,
        price,
        tick_size,
        price_precision,
        allow_taker_fallback,
        "entry",
    )
    .await
}

fn format_exit_quantity(raw_qty: f64, step_size: f64, qty_precision: usize) -> Result<String> {
    let normalized_qty = round_down_to_step(raw_qty.abs(), step_size);
    if normalized_qty < step_size {
        return Err(anyhow!(
            "normalized exit quantity {:.8} is below stepSize {:.8}",
            normalized_qty,
            step_size
        ));
    }
    Ok(format_decimal(normalized_qty, qty_precision))
}

fn build_close_order_params(
    symbol: &str,
    side: &str,
    position_side: &str,
    order_type: &str,
    quantity: &str,
    stop_price: &str,
) -> Vec<(String, String)> {
    let mut params = vec![
        ("algoType".to_string(), "CONDITIONAL".to_string()),
        ("symbol".to_string(), symbol.to_string()),
        ("side".to_string(), side.to_string()),
        ("positionSide".to_string(), position_side.to_string()),
        ("type".to_string(), order_type.to_string()),
        ("quantity".to_string(), quantity.to_string()),
        ("triggerPrice".to_string(), stop_price.to_string()),
        ("workingType".to_string(), "MARK_PRICE".to_string()),
    ];
    if position_side.eq_ignore_ascii_case("BOTH") {
        params.push(("reduceOnly".to_string(), "true".to_string()));
    }
    params
}

fn build_limit_algo_close_order_params(
    symbol: &str,
    side: &str,
    position_side: &str,
    order_type: &str,
    quantity: &str,
    trigger_price: &str,
    limit_price: &str,
    time_in_force: &str,
    client_algo_id_prefix: &str,
) -> Vec<(String, String)> {
    let mut params = build_close_order_params(
        symbol,
        side,
        position_side,
        order_type,
        quantity,
        trigger_price,
    );
    params.push(("price".to_string(), limit_price.to_string()));
    params.push(("timeInForce".to_string(), time_in_force.to_string()));
    params.push((
        "clientAlgoId".to_string(),
        build_client_order_id(client_algo_id_prefix),
    ));
    params
}

async fn place_close_order(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    symbol: &str,
    side: &str,
    position_side: &str,
    order_type: &str,
    quantity: &str,
    stop_price: &str,
) -> Result<i64> {
    let response: Value = signed_post_json(
        http_client,
        api_config,
        exec_config,
        "/fapi/v1/algoOrder",
        build_close_order_params(
            symbol,
            side,
            position_side,
            order_type,
            quantity,
            stop_price,
        ),
    )
    .await?;
    parse_numeric_id(
        &response,
        &["algoId", "orderId", "id"],
        "algo close order id",
    )
}

/// Place a take-profit order for a *pending maker entry* (limit buy/sell not yet filled).
///
/// Problem: Binance rejects a normal TAKE_PROFIT_MARKET algo-order when no position exists
/// and the trigger price is already beyond the current market (e.g. tp=2129 while market=2125
/// for a long pending at 2111). The exchange sees no position to reduce and rejects.
///
/// Solution: place a STOP algo-order on /fapi/v1/algoOrder with
///   triggerPrice = maker_entry_price  (same level the entry fills)
///   price        = tp_price           (GTC limit sell placed the moment entry triggers)
///
/// When price drops to the entry level both the limit-buy entry and this stop-limit TP
/// activate simultaneously. The limit sell at tp_price then sits open until price rebounds.
async fn place_tp_as_stop_limit_algo_order(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    symbol: &str,
    side: &str,
    position_side: &str,
    quantity: &str,
    entry_price: &str,
    tp_limit_price: &str,
) -> Result<i64> {
    let response: Value = signed_post_json(
        http_client,
        api_config,
        exec_config,
        "/fapi/v1/algoOrder",
        build_limit_algo_close_order_params(
            symbol,
            side,
            position_side,
            "STOP",
            quantity,
            entry_price,
            tp_limit_price,
            "GTC",
            "tp_maker",
        ),
    )
    .await?;
    parse_numeric_id(
        &response,
        &["algoId", "orderId", "id"],
        "algo close order id",
    )
}

async fn place_market_order_with_side(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    symbol: &str,
    side: &str,
    position_side: &str,
    quantity: &str,
    order_id_prefix: &str,
) -> Result<i64> {
    let response: FuturesOrderResponse = signed_post_json(
        http_client,
        api_config,
        exec_config,
        "/fapi/v1/order",
        vec![
            ("symbol".to_string(), symbol.to_string()),
            ("side".to_string(), side.to_string()),
            ("positionSide".to_string(), position_side.to_string()),
            ("type".to_string(), "MARKET".to_string()),
            ("quantity".to_string(), quantity.to_string()),
            (
                "newClientOrderId".to_string(),
                build_client_order_id(order_id_prefix),
            ),
        ],
    )
    .await?;
    Ok(response.order_id)
}

async fn place_market_order_with_side_reduce(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    symbol: &str,
    side: &str,
    position_side: &str,
    quantity: &str,
    order_id_prefix: &str,
) -> Result<i64> {
    let mut params = vec![
        ("symbol".to_string(), symbol.to_string()),
        ("side".to_string(), side.to_string()),
        ("positionSide".to_string(), position_side.to_string()),
        ("type".to_string(), "MARKET".to_string()),
        ("quantity".to_string(), quantity.to_string()),
        (
            "newClientOrderId".to_string(),
            build_client_order_id(order_id_prefix),
        ),
    ];
    // In one-way mode (BOTH), reduceOnly protects against accidental position flip.
    // In hedge mode with explicit positionSide LONG/SHORT, Binance may reject reduceOnly.
    if position_side.eq_ignore_ascii_case("BOTH") {
        params.push(("reduceOnly".to_string(), "true".to_string()));
    }
    let response: FuturesOrderResponse = signed_post_json(
        http_client,
        api_config,
        exec_config,
        "/fapi/v1/order",
        params,
    )
    .await?;
    Ok(response.order_id)
}

async fn place_limit_post_only_order_with_side(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    symbol: &str,
    side: &str,
    position_side: &str,
    quantity: &str,
    price: &str,
    tick_size: f64,
    price_precision: usize,
    allow_taker_fallback: bool,
    order_id_prefix: &str,
) -> Result<i64> {
    let current_price = price
        .parse::<f64>()
        .with_context(|| format!("invalid post-only price: {}", price))?;
    let attempt_prices = build_post_only_attempt_prices(
        current_price,
        side,
        tick_size,
        price_precision,
        POST_ONLY_MAKER_REPRICE_RETRY_COUNT,
    )?;
    let attempted_prices = attempt_prices
        .iter()
        .map(|attempt_price| format_decimal(*attempt_price, price_precision))
        .collect::<Vec<_>>();
    let mut last_post_only_reject = None;

    for (attempt_idx, attempt_price) in attempt_prices.iter().enumerate() {
        let price_text = format_decimal(*attempt_price, price_precision);
        let response: Result<FuturesOrderResponse> = signed_post_json(
            http_client,
            api_config,
            exec_config,
            "/fapi/v1/order",
            vec![
                ("symbol".to_string(), symbol.to_string()),
                ("side".to_string(), side.to_string()),
                ("positionSide".to_string(), position_side.to_string()),
                ("type".to_string(), "LIMIT".to_string()),
                ("timeInForce".to_string(), "GTX".to_string()),
                ("quantity".to_string(), quantity.to_string()),
                ("price".to_string(), price_text.clone()),
                (
                    "newClientOrderId".to_string(),
                    build_client_order_id(order_id_prefix),
                ),
            ],
        )
        .await;

        match response {
            Ok(ok) => {
                if attempt_idx > 0 {
                    info!(
                        symbol = %symbol,
                        side = %side,
                        position_side = %position_side,
                        quantity = %quantity,
                        price = %price_text,
                        attempt = attempt_idx + 1,
                        total_attempts = attempted_prices.len(),
                        "post-only maker order accepted after repricing retry"
                    );
                }
                return Ok(ok.order_id);
            }
            Err(err) => {
                if !is_post_only_reject_error(&err) {
                    return Err(err);
                }
                last_post_only_reject = Some(err);

                if attempt_idx + 1 < attempt_prices.len() {
                    let next_price_text = &attempted_prices[attempt_idx + 1];
                    warn!(
                        symbol = %symbol,
                        side = %side,
                        position_side = %position_side,
                        quantity = %quantity,
                        rejected_price = %price_text,
                        next_price = %next_price_text,
                        attempt = attempt_idx + 1,
                        total_attempts = attempted_prices.len(),
                        "post-only rejected with -5022; repricing maker order by 1 tick away from market"
                    );
                    continue;
                }
            }
        }
    }

    let err = last_post_only_reject
        .expect("post-only retry loop should return or capture the final -5022 rejection");
    let latest_book_ticker = fetch_book_ticker(http_client, api_config, symbol)
        .await
        .ok();
    let attempts_context = format!(
        "attempted_prices=[{}] retries={}",
        attempted_prices.join(","),
        POST_ONLY_MAKER_REPRICE_RETRY_COUNT
    );

    if !allow_taker_fallback {
        let context = if let Some(book) = latest_book_ticker.as_ref() {
            format!(
                "post-only rejected with -5022 after repricing maker {} and taker fallback is disabled while synchronized exits are required. latest bid={} ask={}",
                attempts_context, book.bid_price, book.ask_price
            )
        } else {
            format!(
                "post-only rejected with -5022 after repricing maker {} and taker fallback is disabled while synchronized exits are required. latest bookTicker unavailable",
                attempts_context
            )
        };
        return Err(err).with_context(|| context);
    }

    let fallback_context = if let Some(book) = latest_book_ticker.as_ref() {
        format!(
            "post-only rejected with -5022 after repricing maker {}, fallback to taker. latest bid={} ask={}",
            attempts_context, book.bid_price, book.ask_price
        )
    } else {
        format!(
            "post-only rejected with -5022 after repricing maker {}, fallback to taker. latest bookTicker unavailable",
            attempts_context
        )
    };
    let taker_order_prefix = format!("{}-taker", order_id_prefix);
    place_market_order_with_side(
        http_client,
        api_config,
        exec_config,
        symbol,
        side,
        position_side,
        quantity,
        &taker_order_prefix,
    )
    .await
    .with_context(|| fallback_context)
}

fn build_post_only_attempt_prices(
    initial_price: f64,
    side: &str,
    tick_size: f64,
    price_precision: usize,
    retry_count: usize,
) -> Result<Vec<f64>> {
    if tick_size <= 0.0 {
        return Err(anyhow!(
            "tick size must be positive for post-only repricing"
        ));
    }

    let mut prices = Vec::with_capacity(retry_count + 1);
    let mut current_price = normalize_post_only_retry_price(initial_price, price_precision)?;
    prices.push(current_price);

    for _ in 0..retry_count {
        current_price = shift_post_only_price_away_from_market(
            current_price,
            side,
            tick_size,
            price_precision,
        )?;
        prices.push(current_price);
    }

    Ok(prices)
}

fn shift_post_only_price_away_from_market(
    current_price: f64,
    side: &str,
    tick_size: f64,
    price_precision: usize,
) -> Result<f64> {
    let shifted = if side.eq_ignore_ascii_case("SELL") {
        current_price + tick_size
    } else if side.eq_ignore_ascii_case("BUY") {
        (current_price - tick_size).max(tick_size)
    } else {
        return Err(anyhow!("unsupported side {} for post-only repricing", side));
    };
    normalize_post_only_retry_price(shifted, price_precision)
}

fn normalize_post_only_retry_price(price: f64, price_precision: usize) -> Result<f64> {
    let normalized_text = format_decimal(price, price_precision);
    normalized_text
        .parse::<f64>()
        .with_context(|| format!("invalid post-only retry price: {}", normalized_text))
}

fn is_post_only_reject_error(err: &anyhow::Error) -> bool {
    let text = err.to_string();
    text.contains("\"code\":-5022")
        || text.contains("Post Only order will be rejected")
        || text.contains("could not be executed as maker")
}

async fn cancel_all_open_orders(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    symbol: &str,
) -> Result<()> {
    let _: Value = signed_delete_json(
        http_client,
        api_config,
        exec_config,
        "/fapi/v1/allOpenOrders",
        vec![("symbol".to_string(), symbol.to_string())],
    )
    .await?;
    Ok(())
}

async fn cancel_order_by_id(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    symbol: &str,
    order_id: i64,
) -> Result<()> {
    let _: Value = signed_delete_json(
        http_client,
        api_config,
        exec_config,
        "/fapi/v1/order",
        vec![
            ("symbol".to_string(), symbol.to_string()),
            ("orderId".to_string(), order_id.to_string()),
        ],
    )
    .await?;
    Ok(())
}

async fn cancel_all_open_algo_orders(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    symbol: &str,
) -> Result<()> {
    let _: Value = signed_delete_json(
        http_client,
        api_config,
        exec_config,
        "/fapi/v1/algoOpenOrders",
        vec![("symbol".to_string(), symbol.to_string())],
    )
    .await?;
    Ok(())
}

async fn cancel_algo_order_by_id(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    symbol: &str,
    algo_id: i64,
) -> Result<()> {
    let _: Value = signed_delete_json(
        http_client,
        api_config,
        exec_config,
        "/fapi/v1/algoOrder",
        vec![
            ("symbol".to_string(), symbol.to_string()),
            ("algoId".to_string(), algo_id.to_string()),
        ],
    )
    .await?;
    Ok(())
}

async fn signed_post_json<T: for<'de> Deserialize<'de>>(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    path: &str,
    params: Vec<(String, String)>,
) -> Result<T> {
    let url = signed_url(api_config, exec_config, path, params)?;
    let response = http_client
        .post(&url)
        .header("X-MBX-APIKEY", api_config.resolved_api_key())
        .send()
        .await
        .with_context(|| format!("binance signed POST {} failed", path))?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!(
            "binance {} failed status={} body={}",
            path,
            status,
            body
        ));
    }
    response
        .json()
        .await
        .with_context(|| format!("decode binance {} response", path))
}

async fn signed_get_json<T: for<'de> Deserialize<'de>>(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    path: &str,
    params: Vec<(String, String)>,
) -> Result<T> {
    let url = signed_url(api_config, exec_config, path, params)?;
    let response = http_client
        .get(&url)
        .header("X-MBX-APIKEY", api_config.resolved_api_key())
        .send()
        .await
        .with_context(|| format!("binance signed GET {} failed", path))?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!(
            "binance {} failed status={} body={}",
            path,
            status,
            body
        ));
    }
    response
        .json()
        .await
        .with_context(|| format!("decode binance {} response", path))
}

async fn signed_delete_json<T: for<'de> Deserialize<'de>>(
    http_client: &Client,
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    path: &str,
    params: Vec<(String, String)>,
) -> Result<T> {
    let url = signed_url(api_config, exec_config, path, params)?;
    let response = http_client
        .delete(&url)
        .header("X-MBX-APIKEY", api_config.resolved_api_key())
        .send()
        .await
        .with_context(|| format!("binance signed DELETE {} failed", path))?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!(
            "binance {} failed status={} body={}",
            path,
            status,
            body
        ));
    }
    response
        .json()
        .await
        .with_context(|| format!("decode binance {} response", path))
}

fn signed_url(
    api_config: &BinanceApiConfig,
    exec_config: &LlmExecutionConfig,
    path: &str,
    mut params: Vec<(String, String)>,
) -> Result<String> {
    params.push((
        "recvWindow".to_string(),
        exec_config.recv_window_ms.to_string(),
    ));
    params.push((
        "timestamp".to_string(),
        Utc::now().timestamp_millis().to_string(),
    ));

    let query = build_query(&params);
    let signature = sign_query(&api_config.resolved_api_secret(), &query)?;
    Ok(format!(
        "{}{}?{}&signature={}",
        api_config.futures_rest_api_url.trim_end_matches('/'),
        path,
        query,
        signature
    ))
}

fn build_query(params: &[(String, String)]) -> String {
    params
        .iter()
        .map(|(key, value)| format!("{}={}", key, urlencoding::encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}

fn sign_query(secret: &str, query: &str) -> Result<String> {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).context("invalid binance api secret")?;
    mac.update(query.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

fn build_client_order_id(prefix: &str) -> String {
    let random = Uuid::new_v4().simple().to_string();
    format!("llm_{}_{}", prefix, &random[..18])
}

fn round_down_to_step(value: f64, step: f64) -> f64 {
    // Binary floating-point can turn exact step multiples like 0.071 / 0.001
    // into 70.99999999999999, which would incorrectly floor to the next-lower step.
    ((value / step) + 1e-9).floor() * step
}

fn precision_from_step(step: f64) -> usize {
    let text = format!("{:.12}", step);
    text.trim_end_matches('0')
        .split('.')
        .nth(1)
        .map(|fraction| fraction.len())
        .unwrap_or(0)
}

fn format_decimal(value: f64, precision: usize) -> String {
    format!("{:.*}", precision, value)
}

#[derive(Debug, Deserialize)]
struct FuturesExchangeInfo {
    symbols: Vec<FuturesSymbolInfo>,
}

#[derive(Debug, Deserialize)]
struct FuturesSymbolInfo {
    symbol: String,
    filters: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FuturesBookTicker {
    bid_price: String,
    ask_price: String,
}

#[derive(Debug, Deserialize)]
struct FuturesOrderResponse {
    #[serde(rename = "orderId")]
    order_id: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FuturesAccountResponse {
    total_wallet_balance: String,
    available_balance: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FuturesPositionRiskRow {
    position_side: String,
    position_amt: String,
    entry_price: String,
    mark_price: String,
    un_realized_profit: String,
    leverage: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FuturesOpenOrderRow {
    order_id: i64,
    side: String,
    position_side: String,
    #[serde(rename = "type")]
    order_type: String,
    status: String,
    orig_qty: String,
    executed_qty: String,
    price: String,
    stop_price: String,
    close_position: bool,
    reduce_only: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FuturesUserTradeRow {
    realized_pnl: String,
}

struct FuturesAccountBalance {
    total_wallet_balance: f64,
    available_balance: f64,
}

struct SymbolFilters {
    step_size: f64,
    min_qty: f64,
    tick_size: f64,
    qty_precision: usize,
    price_precision: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    fn sample_trade_intent(leverage: f64) -> TradeIntent {
        TradeIntent {
            decision: TradeDecision::Long,
            entry_price: Some(100.0),
            take_profit: Some(110.0),
            stop_loss: Some(95.0),
            leverage: Some(leverage),
            risk_reward_ratio: Some(2.0),
            horizon: Some("1h".to_string()),
            swing_logic: Some("test".to_string()),
            reason: "test".to_string(),
        }
    }

    #[test]
    fn select_leverage_uses_model_times_ratio_with_max_clamp() {
        let mut exec = LlmExecutionConfig::default();
        exec.default_leverage_ratio = 30.0;
        exec.max_leverage = 150;

        let (lv1, src1) = select_leverage(&exec, &sample_trade_intent(1.0)).expect("lv1");
        assert_eq!(lv1, 30);
        assert_eq!(src1, "model_ratio");

        let (lv2, src2) = select_leverage(&exec, &sample_trade_intent(5.0)).expect("lv2");
        assert_eq!(lv2, 150);
        assert_eq!(src2, "model_ratio");

        let (lv3, src3) = select_leverage(&exec, &sample_trade_intent(10.0)).expect("lv3");
        assert_eq!(lv3, 150);
        assert_eq!(src3, "model_ratio");
    }

    #[test]
    fn has_active_position_for_side_respects_hedge_mode() {
        let state = TradingStateSnapshot {
            symbol: "TESTUSDT".to_string(),
            has_active_context: true,
            has_active_positions: true,
            has_open_orders: true,
            active_positions: vec![
                ActivePositionSnapshot {
                    position_side: "LONG".to_string(),
                    position_amt: 0.12,
                    entry_price: 100.0,
                    mark_price: 101.0,
                    unrealized_pnl: 0.12,
                    leverage: 5,
                },
                ActivePositionSnapshot {
                    position_side: "SHORT".to_string(),
                    position_amt: 0.0,
                    entry_price: 0.0,
                    mark_price: 0.0,
                    unrealized_pnl: 0.0,
                    leverage: 5,
                },
            ],
            open_orders: Vec::new(),
            total_wallet_balance: 10.0,
            available_balance: 9.0,
        };

        assert!(has_active_position_for_side(&state, "LONG", true));
        assert!(!has_active_position_for_side(&state, "SHORT", true));
        assert!(has_active_position_for_side(&state, "BOTH", false));
    }

    #[test]
    fn active_position_abs_qty_for_side_respects_mode() {
        let state = TradingStateSnapshot {
            symbol: "TESTUSDT".to_string(),
            has_active_context: true,
            has_active_positions: true,
            has_open_orders: false,
            active_positions: vec![
                ActivePositionSnapshot {
                    position_side: "LONG".to_string(),
                    position_amt: 0.12,
                    entry_price: 100.0,
                    mark_price: 101.0,
                    unrealized_pnl: 0.12,
                    leverage: 5,
                },
                ActivePositionSnapshot {
                    position_side: "SHORT".to_string(),
                    position_amt: -0.07,
                    entry_price: 102.0,
                    mark_price: 101.5,
                    unrealized_pnl: 0.04,
                    leverage: 5,
                },
            ],
            open_orders: Vec::new(),
            total_wallet_balance: 10.0,
            available_balance: 9.0,
        };

        assert!((active_position_abs_qty_for_side(&state, "LONG", true) - 0.12).abs() < 1e-9);
        assert!((active_position_abs_qty_for_side(&state, "SHORT", true) - 0.07).abs() < 1e-9);
        assert!((active_position_abs_qty_for_side(&state, "BOTH", false) - 0.19).abs() < 1e-9);
    }

    #[test]
    fn staged_exit_cleanup_only_runs_when_entry_gone_and_flat() {
        let state_with_entry = TradingStateSnapshot {
            symbol: "TESTUSDT".to_string(),
            has_active_context: true,
            has_active_positions: false,
            has_open_orders: true,
            active_positions: Vec::new(),
            open_orders: vec![OpenOrderSnapshot {
                order_id: 42,
                side: "BUY".to_string(),
                position_side: "LONG".to_string(),
                order_type: "LIMIT".to_string(),
                status: "NEW".to_string(),
                orig_qty: 0.02,
                executed_qty: 0.0,
                price: 2000.0,
                stop_price: 0.0,
                close_position: false,
                reduce_only: false,
                is_algo_order: false,
            }],
            total_wallet_balance: 10.0,
            available_balance: 9.0,
        };
        assert!(!should_cleanup_staged_exit_orders(
            &state_with_entry,
            42,
            "LONG",
            true
        ));

        let state_with_position = TradingStateSnapshot {
            symbol: "TESTUSDT".to_string(),
            has_active_context: true,
            has_active_positions: true,
            has_open_orders: false,
            active_positions: vec![ActivePositionSnapshot {
                position_side: "LONG".to_string(),
                position_amt: 0.02,
                entry_price: 2000.0,
                mark_price: 2001.0,
                unrealized_pnl: 0.02,
                leverage: 10,
            }],
            open_orders: Vec::new(),
            total_wallet_balance: 10.0,
            available_balance: 9.0,
        };
        assert!(!should_cleanup_staged_exit_orders(
            &state_with_position,
            42,
            "LONG",
            true
        ));

        let flat_state = TradingStateSnapshot {
            symbol: "TESTUSDT".to_string(),
            has_active_context: false,
            has_active_positions: false,
            has_open_orders: false,
            active_positions: Vec::new(),
            open_orders: Vec::new(),
            total_wallet_balance: 10.0,
            available_balance: 9.0,
        };
        assert!(should_cleanup_staged_exit_orders(
            &flat_state,
            42,
            "LONG",
            true
        ));
    }

    #[test]
    fn recent_flatten_event_must_happen_after_watcher_started() {
        let flatten_at = Instant::now();
        let mut state = AccountWsState::default();
        state
            .flattened_at
            .insert(("TESTUSDT".to_string(), "LONG".to_string()), flatten_at);

        assert!(ws_state_has_recent_flatten_event(
            &state, "TESTUSDT", "LONG", flatten_at
        ));
        assert!(!ws_state_has_recent_flatten_event(
            &state,
            "TESTUSDT",
            "LONG",
            flatten_at.checked_add(Duration::from_secs(1)).unwrap()
        ));
    }

    #[test]
    fn orphan_exit_cleanup_requires_flat_exit_only_state() {
        let flat_exit_only_state = TradingStateSnapshot {
            symbol: "TESTUSDT".to_string(),
            has_active_context: true,
            has_active_positions: false,
            has_open_orders: true,
            active_positions: Vec::new(),
            open_orders: vec![
                OpenOrderSnapshot {
                    order_id: 101,
                    side: "BUY".to_string(),
                    position_side: "BOTH".to_string(),
                    order_type: "TAKE_PROFIT_MARKET".to_string(),
                    status: "NEW".to_string(),
                    orig_qty: 0.0,
                    executed_qty: 0.0,
                    price: 0.0,
                    stop_price: 2162.25,
                    close_position: true,
                    reduce_only: true,
                    is_algo_order: false,
                },
                OpenOrderSnapshot {
                    order_id: 102,
                    side: "BUY".to_string(),
                    position_side: "BOTH".to_string(),
                    order_type: "STOP_MARKET".to_string(),
                    status: "NEW".to_string(),
                    orig_qty: 0.0,
                    executed_qty: 0.0,
                    price: 0.0,
                    stop_price: 2219.0,
                    close_position: true,
                    reduce_only: true,
                    is_algo_order: true,
                },
            ],
            total_wallet_balance: 10.0,
            available_balance: 9.0,
        };
        assert!(should_cleanup_orphan_exit_orders_when_flat(
            &flat_exit_only_state
        ));

        let state_with_entry = TradingStateSnapshot {
            symbol: "TESTUSDT".to_string(),
            has_active_context: true,
            has_active_positions: false,
            has_open_orders: true,
            active_positions: Vec::new(),
            open_orders: vec![OpenOrderSnapshot {
                order_id: 103,
                side: "SELL".to_string(),
                position_side: "SHORT".to_string(),
                order_type: "LIMIT".to_string(),
                status: "NEW".to_string(),
                orig_qty: 0.05,
                executed_qty: 0.0,
                price: 2202.87,
                stop_price: 0.0,
                close_position: false,
                reduce_only: false,
                is_algo_order: false,
            }],
            total_wallet_balance: 10.0,
            available_balance: 9.0,
        };
        assert!(!should_cleanup_orphan_exit_orders_when_flat(
            &state_with_entry
        ));
    }

    #[test]
    fn account_update_reports_flattened_symbols() {
        let state = Arc::new(Mutex::new(AccountWsState {
            has_account_update: true,
            total_wallet_balance: Some(100.0),
            available_balance: Some(90.0),
            positions: HashMap::from([(
                ("TESTUSDT".to_string(), "BOTH".to_string()),
                ActivePositionSnapshot {
                    position_side: "BOTH".to_string(),
                    position_amt: -0.25,
                    entry_price: 2200.0,
                    mark_price: 2190.0,
                    unrealized_pnl: 2.5,
                    leverage: 10,
                },
            )]),
            flattened_at: HashMap::new(),
        }));
        let payload = json!({
            "e": "ACCOUNT_UPDATE",
            "a": {
                "B": [{"a": "USDT", "wb": "100.0", "cw": "95.0"}],
                "P": [{"s": "TESTUSDT", "ps": "BOTH", "pa": "0", "ep": "0", "up": "0"}]
            }
        });

        let flattened = apply_account_update_event(&payload, &state);
        assert_eq!(flattened, vec!["TESTUSDT".to_string()]);

        let guard = state.lock().expect("lock state");
        assert!(guard
            .positions
            .get(&("TESTUSDT".to_string(), "BOTH".to_string()))
            .is_none());
        assert_eq!(guard.available_balance, Some(95.0));
        assert!(guard
            .flattened_at
            .contains_key(&("TESTUSDT".to_string(), "BOTH".to_string())));
    }

    #[test]
    fn account_update_summary_reports_non_usdt_balance_assets() {
        let payload = json!({
            "E": 1773910470546u64,
            "a": {
                "m": "DEPOSIT",
                "B": [{"a": "BNB", "wb": "1.5", "cw": "1.4", "bc": "0.1"}],
                "P": []
            }
        });

        let summary = summarize_account_update_event(&payload);
        assert!(summary.contains("reason=DEPOSIT"));
        assert!(summary.contains("balance_assets=[BNB]"));
        assert!(summary.contains("balances=[BNB:wb=1.5:cw=1.4:bc=0.1]"));
        assert!(summary.contains("usdt_balance=not_present_in_event"));
        assert!(summary.contains("positions=[]"));
    }

    #[test]
    fn account_update_clears_flatten_marker_after_position_reopens() {
        let state = Arc::new(Mutex::new(AccountWsState {
            has_account_update: true,
            total_wallet_balance: Some(100.0),
            available_balance: Some(90.0),
            positions: HashMap::from([(
                ("TESTUSDT".to_string(), "LONG".to_string()),
                ActivePositionSnapshot {
                    position_side: "LONG".to_string(),
                    position_amt: 0.25,
                    entry_price: 2200.0,
                    mark_price: 2190.0,
                    unrealized_pnl: 2.5,
                    leverage: 10,
                },
            )]),
            flattened_at: HashMap::new(),
        }));
        let flatten_payload = json!({
            "e": "ACCOUNT_UPDATE",
            "a": {
                "P": [{"s": "TESTUSDT", "ps": "LONG", "pa": "0", "ep": "0", "up": "0"}]
            }
        });
        let reopen_payload = json!({
            "e": "ACCOUNT_UPDATE",
            "a": {
                "P": [{"s": "TESTUSDT", "ps": "LONG", "pa": "0.30", "ep": "2210", "up": "3.0"}]
            }
        });

        let flattened = apply_account_update_event(&flatten_payload, &state);
        assert_eq!(flattened, vec!["TESTUSDT".to_string()]);
        let reopened = apply_account_update_event(&reopen_payload, &state);
        assert!(reopened.is_empty());

        let guard = state.lock().expect("lock state");
        assert!(!guard
            .flattened_at
            .contains_key(&("TESTUSDT".to_string(), "LONG".to_string())));
        assert_eq!(
            guard
                .positions
                .get(&("TESTUSDT".to_string(), "LONG".to_string()))
                .map(|position| position.position_amt),
            Some(0.30)
        );
    }

    #[test]
    fn orphan_exit_cleanup_collects_only_flattened_hedge_side_orders() {
        let state = TradingStateSnapshot {
            symbol: "TESTUSDT".to_string(),
            has_active_context: true,
            has_active_positions: true,
            has_open_orders: true,
            active_positions: vec![ActivePositionSnapshot {
                position_side: "SHORT".to_string(),
                position_amt: -0.18,
                entry_price: 2200.0,
                mark_price: 2194.0,
                unrealized_pnl: 1.08,
                leverage: 8,
            }],
            open_orders: vec![
                OpenOrderSnapshot {
                    order_id: 201,
                    side: "SELL".to_string(),
                    position_side: "LONG".to_string(),
                    order_type: "TAKE_PROFIT_MARKET".to_string(),
                    status: "NEW".to_string(),
                    orig_qty: 0.12,
                    executed_qty: 0.0,
                    price: 0.0,
                    stop_price: 2235.0,
                    close_position: true,
                    reduce_only: true,
                    is_algo_order: false,
                },
                OpenOrderSnapshot {
                    order_id: 202,
                    side: "SELL".to_string(),
                    position_side: "LONG".to_string(),
                    order_type: "STOP_MARKET".to_string(),
                    status: "NEW".to_string(),
                    orig_qty: 0.12,
                    executed_qty: 0.0,
                    price: 0.0,
                    stop_price: 2178.0,
                    close_position: true,
                    reduce_only: true,
                    is_algo_order: true,
                },
                OpenOrderSnapshot {
                    order_id: 203,
                    side: "BUY".to_string(),
                    position_side: "SHORT".to_string(),
                    order_type: "TAKE_PROFIT_MARKET".to_string(),
                    status: "NEW".to_string(),
                    orig_qty: 0.18,
                    executed_qty: 0.0,
                    price: 0.0,
                    stop_price: 2162.0,
                    close_position: true,
                    reduce_only: true,
                    is_algo_order: false,
                },
                OpenOrderSnapshot {
                    order_id: 204,
                    side: "BUY".to_string(),
                    position_side: "SHORT".to_string(),
                    order_type: "STOP_MARKET".to_string(),
                    status: "NEW".to_string(),
                    orig_qty: 0.18,
                    executed_qty: 0.0,
                    price: 0.0,
                    stop_price: 2219.0,
                    close_position: true,
                    reduce_only: true,
                    is_algo_order: true,
                },
                OpenOrderSnapshot {
                    order_id: 205,
                    side: "BUY".to_string(),
                    position_side: "BOTH".to_string(),
                    order_type: "STOP_MARKET".to_string(),
                    status: "NEW".to_string(),
                    orig_qty: 0.18,
                    executed_qty: 0.0,
                    price: 0.0,
                    stop_price: 2221.0,
                    close_position: true,
                    reduce_only: true,
                    is_algo_order: true,
                },
            ],
            total_wallet_balance: 100.0,
            available_balance: 90.0,
        };

        let orphan_orders = collect_orphan_exit_orders_to_cancel(&state, true);
        assert_eq!(orphan_orders.len(), 2);
        assert_eq!(orphan_orders[0].order_id, 201);
        assert!(!orphan_orders[0].is_algo_order);
        assert_eq!(orphan_orders[1].order_id, 202);
        assert!(orphan_orders[1].is_algo_order);
    }

    #[test]
    fn orphan_exit_cleanup_skips_side_when_pending_entry_still_exists() {
        let state = TradingStateSnapshot {
            symbol: "TESTUSDT".to_string(),
            has_active_context: true,
            has_active_positions: true,
            has_open_orders: true,
            active_positions: vec![ActivePositionSnapshot {
                position_side: "SHORT".to_string(),
                position_amt: -0.18,
                entry_price: 2200.0,
                mark_price: 2194.0,
                unrealized_pnl: 1.08,
                leverage: 8,
            }],
            open_orders: vec![
                OpenOrderSnapshot {
                    order_id: 301,
                    side: "BUY".to_string(),
                    position_side: "LONG".to_string(),
                    order_type: "LIMIT".to_string(),
                    status: "NEW".to_string(),
                    orig_qty: 0.11,
                    executed_qty: 0.0,
                    price: 2189.0,
                    stop_price: 0.0,
                    close_position: false,
                    reduce_only: false,
                    is_algo_order: false,
                },
                OpenOrderSnapshot {
                    order_id: 302,
                    side: "SELL".to_string(),
                    position_side: "LONG".to_string(),
                    order_type: "TAKE_PROFIT_MARKET".to_string(),
                    status: "NEW".to_string(),
                    orig_qty: 0.11,
                    executed_qty: 0.0,
                    price: 0.0,
                    stop_price: 2235.0,
                    close_position: true,
                    reduce_only: true,
                    is_algo_order: false,
                },
                OpenOrderSnapshot {
                    order_id: 303,
                    side: "SELL".to_string(),
                    position_side: "LONG".to_string(),
                    order_type: "STOP_MARKET".to_string(),
                    status: "NEW".to_string(),
                    orig_qty: 0.11,
                    executed_qty: 0.0,
                    price: 0.0,
                    stop_price: 2178.0,
                    close_position: true,
                    reduce_only: true,
                    is_algo_order: true,
                },
            ],
            total_wallet_balance: 100.0,
            available_balance: 90.0,
        };

        let orphan_orders = collect_orphan_exit_orders_to_cancel(&state, true);
        assert!(orphan_orders.is_empty());
    }

    #[test]
    fn ws_positions_without_open_orders_require_rest_reconciliation() {
        let active_positions = vec![ActivePositionSnapshot {
            position_side: "BOTH".to_string(),
            position_amt: -0.07,
            entry_price: 2193.07,
            mark_price: 2161.55,
            unrealized_pnl: 2.21,
            leverage: 1,
        }];

        assert!(should_reconcile_ws_positions_with_rest(
            ActivePositionSource::AccountWs,
            &active_positions,
            &[],
        ));
        assert!(!should_reconcile_ws_positions_with_rest(
            ActivePositionSource::Rest,
            &active_positions,
            &[],
        ));
        assert!(!should_reconcile_ws_positions_with_rest(
            ActivePositionSource::AccountWs,
            &active_positions,
            &[OpenOrderSnapshot {
                order_id: 501,
                side: "BUY".to_string(),
                position_side: "BOTH".to_string(),
                order_type: "TAKE_PROFIT_MARKET".to_string(),
                status: "NEW".to_string(),
                orig_qty: 0.07,
                executed_qty: 0.0,
                price: 0.0,
                stop_price: 2144.0,
                close_position: true,
                reduce_only: true,
                is_algo_order: true,
            }],
        ));
    }

    #[test]
    fn find_live_exit_trigger_price_recovers_reduce_only_limit_tp_and_sl_by_side() {
        let open_orders = vec![
            OpenOrderSnapshot {
                order_id: 401,
                side: "SELL".to_string(),
                position_side: "BOTH".to_string(),
                order_type: "LIMIT".to_string(),
                status: "NEW".to_string(),
                orig_qty: 0.1,
                executed_qty: 0.0,
                price: 2235.0,
                stop_price: 0.0,
                close_position: false,
                reduce_only: true,
                is_algo_order: false,
            },
            OpenOrderSnapshot {
                order_id: 402,
                side: "SELL".to_string(),
                position_side: "BOTH".to_string(),
                order_type: "CONDITIONAL".to_string(),
                status: "NEW".to_string(),
                orig_qty: 0.1,
                executed_qty: 0.0,
                price: 0.0,
                stop_price: 2178.0,
                close_position: false,
                reduce_only: true,
                is_algo_order: true,
            },
        ];

        assert_eq!(
            find_live_exit_trigger_price(
                &open_orders,
                "LONG",
                true,
                Some(2200.0),
                ExitKind::TakeProfit,
            ),
            Some(2235.0)
        );
        assert_eq!(
            find_live_exit_trigger_price(
                &open_orders,
                "LONG",
                true,
                Some(2200.0),
                ExitKind::StopLoss,
            ),
            Some(2178.0)
        );
    }

    #[test]
    fn find_live_exit_trigger_price_recovers_pending_maker_tp_stop_limit_from_limit_price() {
        let open_orders = vec![
            OpenOrderSnapshot {
                order_id: 411,
                side: "SELL".to_string(),
                position_side: "LONG".to_string(),
                order_type: "STOP".to_string(),
                status: "NEW".to_string(),
                orig_qty: 0.1,
                executed_qty: 0.0,
                price: 2235.0,
                stop_price: 2200.0,
                close_position: false,
                reduce_only: true,
                is_algo_order: true,
            },
            OpenOrderSnapshot {
                order_id: 412,
                side: "SELL".to_string(),
                position_side: "LONG".to_string(),
                order_type: "STOP_MARKET".to_string(),
                status: "NEW".to_string(),
                orig_qty: 0.1,
                executed_qty: 0.0,
                price: 0.0,
                stop_price: 2178.0,
                close_position: true,
                reduce_only: true,
                is_algo_order: true,
            },
        ];

        assert_eq!(
            find_live_exit_trigger_price(
                &open_orders,
                "LONG",
                true,
                Some(2200.0),
                ExitKind::TakeProfit,
            ),
            Some(2235.0)
        );
        assert_eq!(
            find_live_exit_trigger_price(
                &open_orders,
                "LONG",
                true,
                Some(2200.0),
                ExitKind::StopLoss,
            ),
            Some(2178.0)
        );
    }

    #[test]
    fn collect_pending_context_orders_for_side_uses_inferred_hedge_direction() {
        let open_orders = vec![
            OpenOrderSnapshot {
                order_id: 421,
                side: "BUY".to_string(),
                position_side: "BOTH".to_string(),
                order_type: "LIMIT".to_string(),
                status: "NEW".to_string(),
                orig_qty: 0.1,
                executed_qty: 0.0,
                price: 2200.0,
                stop_price: 0.0,
                close_position: false,
                reduce_only: false,
                is_algo_order: false,
            },
            OpenOrderSnapshot {
                order_id: 422,
                side: "SELL".to_string(),
                position_side: "BOTH".to_string(),
                order_type: "STOP".to_string(),
                status: "NEW".to_string(),
                orig_qty: 0.1,
                executed_qty: 0.0,
                price: 2235.0,
                stop_price: 2200.0,
                close_position: false,
                reduce_only: true,
                is_algo_order: true,
            },
            OpenOrderSnapshot {
                order_id: 423,
                side: "SELL".to_string(),
                position_side: "BOTH".to_string(),
                order_type: "LIMIT".to_string(),
                status: "NEW".to_string(),
                orig_qty: 0.1,
                executed_qty: 0.0,
                price: 2205.0,
                stop_price: 0.0,
                close_position: false,
                reduce_only: false,
                is_algo_order: false,
            },
            OpenOrderSnapshot {
                order_id: 424,
                side: "BUY".to_string(),
                position_side: "BOTH".to_string(),
                order_type: "STOP_MARKET".to_string(),
                status: "NEW".to_string(),
                orig_qty: 0.1,
                executed_qty: 0.0,
                price: 0.0,
                stop_price: 2219.0,
                close_position: true,
                reduce_only: true,
                is_algo_order: true,
            },
        ];

        let long_orders = collect_pending_context_orders_for_side(&open_orders, "LONG", true);
        let short_orders = collect_pending_context_orders_for_side(&open_orders, "SHORT", true);

        assert_eq!(
            long_orders
                .iter()
                .map(|order| order.order_id)
                .collect::<Vec<_>>(),
            vec![421, 422]
        );
        assert_eq!(
            short_orders
                .iter()
                .map(|order| order.order_id)
                .collect::<Vec<_>>(),
            vec![423, 424]
        );
    }

    #[test]
    fn resolve_modify_tpsl_targets_allows_tp_unchanged_when_sl_changes() {
        let intent = PositionManagementIntent {
            decision: PositionManagementDecision::ModifyTpSl,
            qty: None,
            qty_ratio: None,
            is_full_exit: Some(false),
            new_tp: Some(2100.0),
            new_sl: Some(2047.0),
            close_price: None,
            reason: "test".to_string(),
        };

        let (new_tp, new_sl) = resolve_modify_tpsl_targets(&intent, Some(2100.0), None, 0.01, true)
            .expect("tp unchanged + new sl should be allowed");

        assert_eq!(new_tp, 2100.0);
        assert_eq!(new_sl, 2047.0);
    }

    #[test]
    fn resolve_modify_tpsl_targets_allows_sl_unchanged_when_tp_changes() {
        let intent = PositionManagementIntent {
            decision: PositionManagementDecision::ModifyTpSl,
            qty: None,
            qty_ratio: None,
            is_full_exit: Some(false),
            new_tp: Some(2105.0),
            new_sl: Some(2047.0),
            close_price: None,
            reason: "test".to_string(),
        };

        let (new_tp, new_sl) =
            resolve_modify_tpsl_targets(&intent, Some(2100.0), Some(2047.0), 0.01, true)
                .expect("new tp + unchanged sl should be allowed");

        assert_eq!(new_tp, 2105.0);
        assert_eq!(new_sl, 2047.0);
    }

    #[test]
    fn resolve_modify_tpsl_targets_rejects_when_both_exits_unchanged() {
        let intent = PositionManagementIntent {
            decision: PositionManagementDecision::ModifyTpSl,
            qty: None,
            qty_ratio: None,
            is_full_exit: Some(false),
            new_tp: Some(2100.0),
            new_sl: Some(2047.0),
            close_price: None,
            reason: "test".to_string(),
        };

        let err = resolve_modify_tpsl_targets(&intent, Some(2100.0), Some(2047.0), 0.01, true)
            .expect_err("unchanged tp/sl should be rejected");

        assert!(err
            .to_string()
            .contains("new_tp/new_sl equal current live exits"));
    }

    #[test]
    fn resolve_modify_tpsl_targets_allows_exact_one_tick_change() {
        let intent = PositionManagementIntent {
            decision: PositionManagementDecision::ModifyTpSl,
            qty: None,
            qty_ratio: None,
            is_full_exit: Some(false),
            new_tp: Some(2100.1),
            new_sl: Some(2047.0),
            close_price: None,
            reason: "test".to_string(),
        };

        let (new_tp, new_sl) =
            resolve_modify_tpsl_targets(&intent, Some(2100.0), Some(2047.0), 0.1, true)
                .expect("exact one-tick tp change should be allowed");

        assert_eq!(new_tp, 2100.1);
        assert_eq!(new_sl, 2047.0);
    }

    #[test]
    fn recompute_stop_loss_uses_actual_maker_entry_price() {
        let sl = recompute_stop_loss_from_target_rr(
            TradeDecision::Long,
            1963.11,
            1974.50,
            2.33,
            0.01,
            2,
        )
        .expect("recompute stop loss");
        let rr = validate_final_execution_rr(TradeDecision::Long, 1963.11, 1974.50, sl, 2.33)
            .expect("validate rr");

        assert!((sl - 1958.23).abs() < 1e-9);
        assert!((rr - 2.3340163934425556).abs() < 1e-9);
    }

    #[test]
    fn blocks_long_when_current_bid_is_already_below_stop_loss() {
        let blocked = build_trade_blocked_by_current_price_beyond_stop_loss(
            TradeDecision::Long,
            2000.0,
            1990.0,
            1989.5,
            1990.2,
        )
        .expect("long should be blocked");

        assert_eq!(blocked.current_price_source, "best_bid_price");
        assert!((blocked.current_reference_price - 1989.5).abs() < 1e-9);
    }

    #[test]
    fn blocks_short_when_current_ask_is_already_above_stop_loss() {
        let blocked = build_trade_blocked_by_current_price_beyond_stop_loss(
            TradeDecision::Short,
            2000.0,
            2010.0,
            2009.8,
            2010.4,
        )
        .expect("short should be blocked");

        assert_eq!(blocked.current_price_source, "best_ask_price");
        assert!((blocked.current_reference_price - 2010.4).abs() < 1e-9);
    }

    #[test]
    fn allows_trade_when_current_price_remains_between_entry_and_stop_loss() {
        let blocked = build_trade_blocked_by_current_price_beyond_stop_loss(
            TradeDecision::Long,
            2000.0,
            1990.0,
            1994.0,
            1994.2,
        );

        assert!(blocked.is_none());
    }

    #[test]
    fn validate_final_execution_rr_rejects_degraded_geometry() {
        let err = validate_final_execution_rr(TradeDecision::Long, 1963.11, 1974.50, 1960.0, 4.0)
            .expect_err("rr should fail");
        assert!(err.to_string().contains("fell below model rr"));
    }

    #[test]
    fn resolve_pending_order_exit_levels_falls_back_to_planned_shadow_levels() {
        let intent = PendingOrderManagementIntent {
            decision: PendingOrderManagementDecision::ModifyMaker,
            new_entry: Some(1947.5),
            new_tp: None,
            new_sl: Some(1941.5),
            new_leverage: None,
            reason: "test".to_string(),
        };

        let (effective_tp, effective_sl) =
            resolve_pending_order_exit_levels(&intent, None, None, Some(1969.59), Some(1934.18))
                .expect("shadow tp/sl should complete modify-maker request");

        assert_eq!(effective_tp, Some(1969.59));
        assert_eq!(effective_sl, Some(1941.5));
    }

    #[test]
    fn pending_modify_maker_detects_entry_change_across_multiple_ticks() {
        let intent = PendingOrderManagementIntent {
            decision: PendingOrderManagementDecision::ModifyMaker,
            new_entry: Some(2165.0),
            new_tp: Some(2141.79),
            new_sl: Some(2170.21),
            new_leverage: Some(16.0),
            reason: "test".to_string(),
        };

        assert!(pending_modify_maker_requests_effective_change(
            &intent,
            2164.22,
            Some(2141.79),
            Some(2170.21),
            Some(16.0),
            0.1,
        ));
    }

    #[test]
    fn pending_modify_maker_detects_exact_one_tick_change() {
        let intent = PendingOrderManagementIntent {
            decision: PendingOrderManagementDecision::ModifyMaker,
            new_entry: Some(2164.3),
            new_tp: Some(2141.79),
            new_sl: Some(2170.21),
            new_leverage: Some(16.0),
            reason: "test".to_string(),
        };

        assert!(pending_modify_maker_requests_effective_change(
            &intent,
            2164.2,
            Some(2141.79),
            Some(2170.21),
            Some(16.0),
            0.1,
        ));
    }

    #[test]
    fn pending_modify_maker_ignores_sub_tick_tail_differences() {
        let intent = PendingOrderManagementIntent {
            decision: PendingOrderManagementDecision::ModifyMaker,
            new_entry: Some(2164.2200000001),
            new_tp: Some(2141.7900000001),
            new_sl: Some(2170.2100000001),
            new_leverage: Some(16.0),
            reason: "test".to_string(),
        };

        assert!(!pending_modify_maker_requests_effective_change(
            &intent,
            2164.22,
            Some(2141.79),
            Some(2170.21),
            Some(16.0),
            0.01,
        ));
    }

    #[test]
    fn build_close_order_params_use_reduce_only_in_one_way_mode() {
        let params = build_close_order_params(
            "TESTUSDT",
            "SELL",
            "BOTH",
            "TAKE_PROFIT_MARKET",
            "0.01",
            "2128.28",
        );

        let params_map = params
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(
            params_map.get("symbol").map(String::as_str),
            Some("TESTUSDT")
        );
        assert_eq!(params_map.get("side").map(String::as_str), Some("SELL"));
        assert_eq!(
            params_map.get("positionSide").map(String::as_str),
            Some("BOTH")
        );
        assert_eq!(
            params_map.get("type").map(String::as_str),
            Some("TAKE_PROFIT_MARKET")
        );
        assert_eq!(params_map.get("quantity").map(String::as_str), Some("0.01"));
        assert_eq!(
            params_map.get("reduceOnly").map(String::as_str),
            Some("true")
        );
        assert!(!params_map.contains_key("closePosition"));
    }

    #[test]
    fn build_close_order_params_omit_reduce_only_in_hedge_mode() {
        let params =
            build_close_order_params("TESTUSDT", "SELL", "LONG", "STOP_MARKET", "0.01", "2078.77");
        let params_map = params
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>();
        assert!(!params_map.contains_key("reduceOnly"));
    }

    #[test]
    fn build_limit_algo_close_order_params_include_limit_fields_for_one_way_mode() {
        let params = build_limit_algo_close_order_params(
            "TESTUSDT", "SELL", "BOTH", "STOP", "0.01", "2305.00", "2345.34", "GTC", "tp_maker",
        );

        let params_map = params
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(
            params_map.get("algoType").map(String::as_str),
            Some("CONDITIONAL")
        );
        assert_eq!(params_map.get("type").map(String::as_str), Some("STOP"));
        assert_eq!(
            params_map.get("triggerPrice").map(String::as_str),
            Some("2305.00")
        );
        assert_eq!(params_map.get("price").map(String::as_str), Some("2345.34"));
        assert_eq!(
            params_map.get("timeInForce").map(String::as_str),
            Some("GTC")
        );
        assert_eq!(
            params_map.get("reduceOnly").map(String::as_str),
            Some("true")
        );
        assert!(params_map.contains_key("clientAlgoId"));
    }

    #[test]
    fn format_exit_quantity_rounds_down_to_step() {
        let qty = format_exit_quantity(0.01234, 0.001, 3).expect("format qty");
        assert_eq!(qty, "0.012");
    }

    #[test]
    fn format_exit_quantity_preserves_exact_step_multiple() {
        let qty = format_exit_quantity(0.071, 0.001, 3).expect("format qty");
        assert_eq!(qty, "0.071");
    }

    #[test]
    fn build_post_only_attempt_prices_moves_short_away_from_market_each_retry() {
        let prices =
            build_post_only_attempt_prices(2196.31, "SELL", 0.1, 2, 3).expect("short retry prices");
        let formatted = prices
            .iter()
            .map(|price| format_decimal(*price, 2))
            .collect::<Vec<_>>();

        assert_eq!(formatted, vec!["2196.31", "2196.41", "2196.51", "2196.61"]);
    }

    #[test]
    fn build_post_only_attempt_prices_moves_long_away_from_market_each_retry() {
        let prices =
            build_post_only_attempt_prices(2196.31, "BUY", 0.1, 2, 3).expect("long retry prices");
        let formatted = prices
            .iter()
            .map(|price| format_decimal(*price, 2))
            .collect::<Vec<_>>();

        assert_eq!(formatted, vec!["2196.31", "2196.21", "2196.11", "2196.01"]);
    }

    #[test]
    fn build_post_only_attempt_prices_clamps_long_retries_at_tick_size_floor() {
        let prices =
            build_post_only_attempt_prices(0.15, "BUY", 0.1, 2, 3).expect("clamped long prices");
        let formatted = prices
            .iter()
            .map(|price| format_decimal(*price, 2))
            .collect::<Vec<_>>();

        assert_eq!(formatted, vec!["0.15", "0.10", "0.10", "0.10"]);
    }
}
