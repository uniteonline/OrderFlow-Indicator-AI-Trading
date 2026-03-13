use crate::app::config::{
    ClaudeApiConfig, CustomLlmApiConfig, GeminiApiConfig, GrokApiConfig, LlmModelConfig,
    OpenRouterApiConfig, QwenApiConfig, RootConfig,
};
use crate::execution::binance::TradingStateSnapshot;
use crate::llm::prompt;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use futures_util::future::join_all;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Number, Value};
use std::time::Instant;
use tokio::time::{sleep, Duration};
use tracing::warn;
use uuid::Uuid;

const CLAUDE_EXTENDED_CACHE_TTL_BETA: &str = "extended-cache-ttl-2025-04-11";
const CLAUDE_DECISION_TOOL_NAME: &str = "emit_decision";
const ENTRY_SCAN_MAX_BARS_PER_INTERVAL: usize = 120;
const ENTRY_SCAN_MAX_FUNDING_CHANGES: usize = 30;
const ENTRY_SCAN_MAX_AVWAP_15M_POINTS: usize = 30;
const ENTRY_SCAN_MAX_CVD_15M_POINTS: usize = 30;
const ENTRY_SCAN_MAX_RECENT_EVENTS: usize = 25;
const ENTRY_SCAN_MAX_FOOTPRINT_TOP_LEVELS: usize = 16;
const ENTRY_SCAN_MAX_FOOTPRINT_TOP_STRENGTH: usize = 12;
const ENTRY_SCAN_MAX_PVS_TOP_LEVELS: usize = 8;
const ENTRY_SCAN_MAX_PVS_HVN_LEVELS: usize = 8;
const ENTRY_SCAN_MAX_PVS_LVN_LEVELS: usize = 12;

#[derive(Debug, Clone, Serialize)]
pub struct ModelInvocationInput {
    pub symbol: String,
    pub ts_bucket: DateTime<Utc>,
    pub window_code: String,
    pub indicator_count: usize,
    pub source_routing_key: String,
    pub source_published_at: Option<DateTime<Utc>>,
    pub received_at: DateTime<Utc>,
    pub indicators: Value,
    pub missing_indicator_codes: Vec<String>,
    pub management_mode: bool,
    pub pending_order_mode: bool,
    pub trading_state: Option<TradingStateSnapshot>,
    pub management_snapshot: Option<ManagementSnapshotForLlm>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ManagementSnapshotForLlm {
    pub context_state: String,
    pub has_active_positions: bool,
    pub has_open_orders: bool,
    pub active_position_count: usize,
    pub open_order_count: usize,
    pub positions: Vec<PositionSummaryForLlm>,
    pub pending_order: Option<PendingOrderSummaryForLlm>,
    pub last_management_reason: Option<String>,
    pub position_context: Option<PositionContextForLlm>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PositionSummaryForLlm {
    pub position_side: String,
    pub direction: String,
    pub quantity: f64,
    pub leverage: u32,
    pub entry_price: f64,
    pub mark_price: f64,
    pub unrealized_pnl: f64,
    /// Actual TAKE_PROFIT_MARKET order stop_price from Binance (None if no TP order placed).
    pub current_tp_price: Option<f64>,
    /// Actual STOP_MARKET order stop_price from Binance (None if no SL order placed).
    pub current_sl_price: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PendingOrderSummaryForLlm {
    pub position_side: String,
    pub direction: String,
    pub quantity: f64,
    pub leverage: Option<u32>,
    pub entry_price: Option<f64>,
    pub current_tp_price: Option<f64>,
    pub current_sl_price: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PositionContextForLlm {
    pub original_qty: f64,
    pub current_qty: f64,
    pub current_pct_of_original: f64,
    pub effective_leverage: Option<u32>,
    pub effective_entry_price: Option<f64>,
    pub effective_take_profit: Option<f64>,
    pub effective_stop_loss: Option<f64>,
    pub reduction_history: Vec<ReductionHistoryItemForLlm>,
    pub times_reduced_at_current_level: usize,
    pub last_management_action: Option<String>,
    pub last_management_reason: Option<String>,
    /// Entry decision context captured when the position was opened.
    pub entry_context: Option<EntryContextForLlm>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReductionHistoryItemForLlm {
    pub time: String,
    pub qty_ratio: f64,
    pub reason_summary: String,
}

/// Context from the original entry decision, passed to the management model
/// so it can continue the active strategy rather than re-select one.
#[derive(Debug, Clone, Serialize)]
pub struct EntryContextForLlm {
    /// Strategy name chosen at entry (e.g., "Imbalance Re-test").
    pub entry_strategy: Option<String>,
    /// Defense model chosen at entry (e.g., "Sweep & Flip Stop").
    pub stop_model: Option<String>,
    /// Entry mode: "limit_below_zone" | "post_sweep_market" | null.
    pub entry_mode: Option<String>,
    /// Original TP price set at entry.
    pub original_tp: Option<f64>,
    /// Original SL price set at entry.
    pub original_sl: Option<f64>,
    /// Sweep wick extreme from entry candle (anchor for Sweep & Flip Stop).
    pub sweep_wick_extreme: Option<f64>,
    /// Time horizon chosen at entry (e.g., "15m", "1h", "4h", "1d", "3d").
    /// Used by the management model to calibrate noise tolerance: signals
    /// shorter than this horizon should not alone trigger CLOSE.
    pub horizon: Option<String>,
    /// Full reason text from the entry model's decision.
    pub entry_reason: String,
    /// Volatility unit V computed at entry time (median 4h/1d bar range).
    /// Management model MUST use this value for all V-based thresholds
    /// (trail triggers, TP distance checks) instead of recomputing V from
    /// current kline_history, ensuring consistency throughout the trade.
    pub entry_v: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ModelCallOutput {
    pub model_name: String,
    pub provider: String,
    pub model: String,
    pub latency_ms: u128,
    pub batch_id: Option<String>,
    pub batch_status: Option<String>,
    pub provider_finish_reason: Option<String>,
    pub provider_usage: Option<Value>,
    pub raw_response_text: Option<String>,
    pub parsed_decision: Option<Value>,
    pub validation_warning: Option<String>,
    pub entry_stage_trace: Option<Vec<Value>>,
    pub entry_stage_prompt_inputs: Option<Vec<EntryStagePromptInputCapture>>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EntryStagePromptInputCapture {
    pub stage: String,
    pub prompt_input: Value,
    pub stage_1_setup_scan_json: Option<Value>,
}

#[derive(Debug, Default, Clone)]
struct ProviderTrace {
    batch_id: Option<String>,
    batch_status: Option<String>,
    provider_finish_reason: Option<String>,
    provider_usage: Option<Value>,
    entry_stage_trace: Vec<Value>,
    entry_stage_prompt_inputs: Vec<EntryStagePromptInputCapture>,
}

#[derive(Debug)]
struct ProviderSuccess {
    raw_text: String,
    trace: ProviderTrace,
}

#[derive(Debug)]
struct ProviderFailure {
    error: anyhow::Error,
    trace: ProviderTrace,
}

impl ProviderTrace {
    fn push_entry_stage_event(&mut self, event: Value) {
        self.entry_stage_trace.push(event);
    }

    fn prepend_entry_stage_events(mut self, events: &[Value]) -> Self {
        if !events.is_empty() {
            let mut combined = events.to_vec();
            combined.extend(self.entry_stage_trace);
            self.entry_stage_trace = combined;
        }
        self
    }

    fn push_entry_stage_prompt_input(&mut self, capture: EntryStagePromptInputCapture) {
        self.entry_stage_prompt_inputs.push(capture);
    }

    fn prepend_entry_stage_prompt_inputs(
        mut self,
        captures: &[EntryStagePromptInputCapture],
    ) -> Self {
        if !captures.is_empty() {
            let mut combined = captures.to_vec();
            combined.extend(self.entry_stage_prompt_inputs);
            self.entry_stage_prompt_inputs = combined;
        }
        self
    }

    fn output_entry_stage_trace(&self) -> Option<Vec<Value>> {
        if self.entry_stage_trace.is_empty() {
            None
        } else {
            Some(self.entry_stage_trace.clone())
        }
    }

    fn output_entry_stage_prompt_inputs(&self) -> Option<Vec<EntryStagePromptInputCapture>> {
        if self.entry_stage_prompt_inputs.is_empty() {
            None
        } else {
            Some(self.entry_stage_prompt_inputs.clone())
        }
    }
}

fn entry_stage_name(stage: prompt::EntryPromptStage) -> &'static str {
    match stage {
        prompt::EntryPromptStage::Scan => "scan",
        prompt::EntryPromptStage::Finalize => "finalize",
    }
}

fn build_entry_scan_trace_event(
    provider: &str,
    raw_text: &str,
    parsed_scan: Option<&Value>,
) -> Value {
    json!({
        "stage": entry_stage_name(prompt::EntryPromptStage::Scan),
        "provider": provider,
        "raw_response_text": raw_text,
        "parsed_scan": parsed_scan.cloned(),
    })
}

fn build_entry_finalize_skipped_trace_event(reason: &str, scan_value: &Value) -> Value {
    json!({
        "stage": "finalize_skipped",
        "reason": reason,
        "scan_decision": scan_value.get("decision").and_then(Value::as_str).unwrap_or("UNKNOWN"),
    })
}

// Codes that are injected at serialization time and are never present in input.indicators.
// Exclude these from the "missing" report so the model does not think they are unavailable.
const INJECTED_INDICATOR_CODES: &[&str] = &["pre_computed_v", "pre_computed_management_state"];

fn build_entry_finalize_trace_event(input: &ModelInvocationInput, prior_scan: &Value) -> Value {
    let indicators = input.indicators.as_object();
    let selected_indicator_codes = finalize_indicator_codes_for_scan(prior_scan)
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let missing_selected_indicator_codes = selected_indicator_codes
        .iter()
        .filter(|code| {
            if INJECTED_INDICATOR_CODES.contains(&code.as_str()) {
                return false; // always injected at serialization time — never truly missing
            }
            indicators
                .map(|obj| !obj.contains_key(code.as_str()))
                .unwrap_or(true)
        })
        .cloned()
        .collect::<Vec<_>>();
    let selected_indicator_count = selected_indicator_codes
        .len()
        .saturating_sub(missing_selected_indicator_codes.len());
    let setup_quality = prior_scan
        .pointer("/scan/setup_quality")
        .or_else(|| prior_scan.pointer("/scan/conviction"))
        .and_then(Value::as_str);
    let invalidation_basis = prior_scan
        .pointer("/scan/invalidation_basis")
        .cloned()
        .or_else(|| prior_scan.pointer("/scan/invalidation").cloned())
        .unwrap_or(Value::Null);

    json!({
        "stage": entry_stage_name(prompt::EntryPromptStage::Finalize),
        "primary_strategy": extract_scan_primary_strategy(prior_scan).unwrap_or("UNKNOWN"),
        "scan_decision": prior_scan.get("decision").and_then(Value::as_str),
        "setup_quality": setup_quality,
        "conviction": prior_scan.pointer("/scan/conviction").and_then(Value::as_str),
        "market_story": prior_scan.pointer("/scan/market_story").cloned().unwrap_or(Value::Null),
        "hypothesis": prior_scan.pointer("/scan/hypothesis").cloned().unwrap_or(Value::Null),
        "entry_style": prior_scan.pointer("/scan/entry_style").cloned().unwrap_or(Value::Null),
        "candidate_zone": prior_scan.pointer("/scan/candidate_zone").cloned().unwrap_or(Value::Null),
        "entry_ladder": prior_scan.pointer("/scan/entry_ladder").cloned().unwrap_or(Value::Null),
        "target_zone": prior_scan.pointer("/scan/target_zone").cloned().unwrap_or(Value::Null),
        "target_ladder": prior_scan.pointer("/scan/target_ladder").cloned().unwrap_or(Value::Null),
        "stop_ladder": prior_scan.pointer("/scan/stop_ladder").cloned().unwrap_or(Value::Null),
        "invalidation_basis": invalidation_basis,
        "invalidation": prior_scan.pointer("/scan/invalidation").cloned().unwrap_or(Value::Null),
        "stop_model_hint": prior_scan.pointer("/scan/stop_model_hint").cloned().unwrap_or(Value::Null),
        "selected_indicator_count": selected_indicator_count,
        "selected_indicator_codes": selected_indicator_codes,
        "missing_selected_indicator_codes": missing_selected_indicator_codes,
    })
}

fn is_entry_mode(input: &ModelInvocationInput) -> bool {
    !input.management_mode && !input.pending_order_mode
}

fn provider_trace_with_prompt_input_capture(
    capture: Option<EntryStagePromptInputCapture>,
) -> ProviderTrace {
    let mut trace = ProviderTrace::default();
    if let Some(capture) = capture {
        trace.push_entry_stage_prompt_input(capture);
    }
    trace
}

struct BuiltPromptPair {
    system: String,
    user: String,
    prompt_input_capture: Option<EntryStagePromptInputCapture>,
}

fn build_prompt_pair(
    input: &ModelInvocationInput,
    prompt_template: &str,
    entry_stage: prompt::EntryPromptStage,
    prior_scan: Option<&Value>,
) -> Result<BuiltPromptPair, ProviderFailure> {
    let input_json = serialize_prompt_input_minified(input, entry_stage, prior_scan)
        .map_err(provider_failure_plain)?;
    let prompt_input = serde_json::from_str::<Value>(&input_json)
        .context("parse serialized prompt input json")
        .map_err(provider_failure_plain)?;
    let system = prompt::system_prompt(
        input.management_mode,
        input.pending_order_mode,
        prompt_template,
        entry_stage,
    );
    let mut user = format!(
        "{}{}",
        prompt::user_prompt_prefix(input.management_mode, input.pending_order_mode, entry_stage,),
        input_json
    );
    if is_entry_mode(input) && matches!(entry_stage, prompt::EntryPromptStage::Finalize) {
        let scan = prior_scan.ok_or_else(|| {
            provider_failure_plain(anyhow!(
                "entry finalize stage requires prior setup scan json"
            ))
        })?;
        let scan_json = serde_json::to_string(scan)
            .context("serialize entry scan json")
            .map_err(provider_failure_plain)?;
        user.push_str("\n\nSTAGE_1_SETUP_SCAN_JSON:\n");
        user.push_str(&scan_json);
    }
    let prompt_input_capture = if is_entry_mode(input) {
        Some(EntryStagePromptInputCapture {
            stage: entry_stage_name(entry_stage).to_string(),
            prompt_input,
            stage_1_setup_scan_json: if matches!(entry_stage, prompt::EntryPromptStage::Finalize) {
                prior_scan.cloned()
            } else {
                None
            },
        })
    } else {
        None
    };
    Ok(BuiltPromptPair {
        system,
        user,
        prompt_input_capture,
    })
}

fn serialize_prompt_input_minified(
    input: &ModelInvocationInput,
    entry_stage: prompt::EntryPromptStage,
    prior_scan: Option<&Value>,
) -> Result<String> {
    if is_entry_mode(input) && matches!(entry_stage, prompt::EntryPromptStage::Finalize) {
        let scan = prior_scan.ok_or_else(|| anyhow!("missing prior scan for entry finalize"))?;
        return serialize_entry_finalize_input_minified(input, scan);
    }
    serialize_llm_input_minified(input)
}

fn serialize_entry_finalize_input_minified(
    input: &ModelInvocationInput,
    prior_scan: &Value,
) -> Result<String> {
    // Build a mutable copy of the full input value so we can inject pre-computed fields
    // before selecting the finalize subset. inject_precomputed_v writes into this value,
    // not into `input.indicators` directly, so we must run injection here too.
    let mut full_value = serde_json::to_value(input).context("serialize finalize full value")?;
    inject_precomputed_v(&mut full_value);

    let indicators = full_value
        .pointer("/indicators")
        .and_then(|v| v.as_object())
        .ok_or_else(|| anyhow!("entry finalize indicators must be an object"))?;
    let selected_codes = finalize_indicator_codes_for_scan(prior_scan);
    let reduced = selected_codes
        .iter()
        .filter_map(|code| {
            indicators
                .get(*code)
                .cloned()
                .map(|value| ((*code).to_string(), value))
        })
        .collect::<Map<String, Value>>();

    let missing_selected_codes = selected_codes
        .iter()
        .filter(|code| !reduced.contains_key(**code))
        .map(|code| (*code).to_string())
        .collect::<Vec<_>>();

    let mut value = json!({
        "symbol": input.symbol,
        "ts_bucket": input.ts_bucket,
        "window_code": input.window_code,
        "indicator_count": reduced.len(),
        "selected_indicator_codes": selected_codes,
        "missing_selected_indicator_codes": missing_selected_codes,
        "finalize_focus": {
            "primary_strategy": extract_scan_primary_strategy(prior_scan).unwrap_or("UNKNOWN"),
            "selected_indicator_count": reduced.len(),
        },
        "indicators": reduced,
    });
    reverse_timeseries_newest_first(&mut value);
    round_json_floats_in_place(&mut value);
    serde_json::to_string(&value).context("serialize entry finalize prompt input json")
}

fn extract_scan_primary_strategy(scan: &Value) -> Option<&str> {
    scan.get("scan")
        .and_then(Value::as_object)
        .and_then(|obj| obj.get("primary_strategy"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn is_scan_identifier_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn scan_references_indicator_code(scan_text: &str, code: &str) -> bool {
    let haystack = scan_text.to_ascii_lowercase();
    let needle = code.to_ascii_lowercase();

    haystack.match_indices(&needle).any(|(idx, _)| {
        let before = haystack[..idx].chars().next_back();
        let after = haystack[idx + needle.len()..].chars().next();
        let before_ok = before.map_or(true, |ch| !is_scan_identifier_char(ch));
        let after_ok = after.map_or(true, |ch| !is_scan_identifier_char(ch));
        before_ok && after_ok
    })
}

fn scan_stop_model_hint(scan: &Value) -> Option<&str> {
    scan.pointer("/scan/stop_model_hint")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn referenced_finalize_indicator_codes(scan: &Value) -> Vec<&'static str> {
    let scan_text = match serde_json::to_string(scan) {
        Ok(text) => text,
        Err(_) => return Vec::new(),
    };

    let mut codes = vec![];
    for code in [
        "orderbook_depth",
        "liquidation_density",
        "absorption",
        "bullish_absorption",
        "bearish_absorption",
        "buying_exhaustion",
        "selling_exhaustion",
        "fvg",
        "divergence",
        "whale_trades",
        "high_volume_pulse",
        "ema_trend_regime",
        "initiation",
        "bullish_initiation",
        "bearish_initiation",
        "vpin",
    ] {
        if scan_references_indicator_code(&scan_text, code) {
            codes.push(code);
        }
    }

    if matches!(
        scan_stop_model_hint(scan),
        Some("Limit Order Penetration Stop")
    ) {
        codes.push("orderbook_depth");
    }

    codes
}

fn finalize_indicator_codes_for_scan(scan: &Value) -> Vec<&'static str> {
    let mut codes = vec![
        "core_price_anchors",
        "price_volume_structure",
        "footprint",
        "cvd_pack",
        "avwap",
        "kline_history",
        "pre_computed_v", // always included: required for V-based geometry checks in finalize
        "tpo_market_profile",
        "rvwap_sigma_bands",
    ];

    match extract_scan_primary_strategy(scan).unwrap_or("UNKNOWN") {
        "Imbalance Re-test" => codes.extend([
            "bullish_absorption",
            "bearish_absorption",
            "absorption",
            "liquidation_density",
            "fvg",
        ]),
        "Delta Divergence Reversal" => {
            codes.extend(["divergence", "whale_trades", "liquidation_density"])
        }
        "DOM Liquidity Wall" => codes.extend(["orderbook_depth", "liquidation_density"]),
        "Unfinished Auction Target" => codes.extend(["liquidation_density", "fvg"]),
        "Trapped Traders Squeeze" => codes.extend([
            "buying_exhaustion",
            "selling_exhaustion",
            "whale_trades",
            "liquidation_density",
        ]),
        "Value Area Re-fill" => codes.extend(["liquidation_density"]),
        "Spot-Led Flow Continuation" => {
            codes.extend(["whale_trades", "ema_trend_regime", "high_volume_pulse"])
        }
        "Spot-Confirmed Hidden Divergence" => codes.extend([
            "divergence",
            "whale_trades",
            "ema_trend_regime",
            "high_volume_pulse",
        ]),
        "Order-Flow Reversal Sequence" => codes.extend([
            "absorption",
            "bullish_absorption",
            "bearish_absorption",
            "initiation",
            "bullish_initiation",
            "bearish_initiation",
            "buying_exhaustion",
            "selling_exhaustion",
            "high_volume_pulse",
            "whale_trades",
        ]),
        "Dual-Market AVWAP Z-Score" => codes.extend(["ema_trend_regime", "fvg"]),
        "NO_SETUP" | "UNKNOWN" => codes.extend([
            "liquidation_density",
            "orderbook_depth",
            "divergence",
            "whale_trades",
        ]),
        _ => codes.extend([
            "liquidation_density",
            "orderbook_depth",
            "divergence",
            "whale_trades",
            "fvg",
            "ema_trend_regime",
            "high_volume_pulse",
        ]),
    }

    codes.extend(referenced_finalize_indicator_codes(scan));

    let mut seen = std::collections::BTreeSet::new();
    codes.retain(|code| seen.insert(*code));
    codes
}

fn parse_entry_scan_output(
    provider: &str,
    raw_text: &str,
    prompt_template: &str,
) -> Result<Value, ProviderFailure> {
    let value = parse_json_from_text(raw_text)
        .map(|value| normalize_provider_decision_shape(provider, value, false, false))
        .ok_or_else(|| provider_failure_plain(anyhow!("entry scan output is not valid JSON")))?;
    validate_entry_scan_output(&value, prompt_template).map_err(provider_failure_plain)?;
    Ok(value)
}

fn entry_scan_decision(scan: &Value) -> Option<&str> {
    scan.get("decision")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn scan_requires_entry_finalize(scan: &Value) -> bool {
    !matches!(
        entry_scan_decision(scan),
        Some(decision) if decision.eq_ignore_ascii_case(prompt::DECISION_NO_TRADE)
    )
}

fn build_entry_finalize_skipped_no_trade_raw(scan: &Value) -> String {
    let scan_reason = scan
        .get("reason")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let reason = match scan_reason {
        Some(reason) => format!(
            "Stage 1 returned NO_TRADE, so finalize was skipped. {}",
            reason
        ),
        None => "Stage 1 returned NO_TRADE, so finalize was skipped.".to_string(),
    };
    json!({
        "decision": prompt::DECISION_NO_TRADE,
        "params": {
            "entry": Value::Null,
            "tp": Value::Null,
            "sl": Value::Null,
            "leverage": Value::Null,
            "rr": Value::Null,
            "horizon": Value::Null,
        },
        "reason": reason,
    })
    .to_string()
}

fn maybe_short_circuit_entry_finalize(
    provider: &str,
    scan: &ProviderSuccess,
    scan_value: &Value,
) -> Option<ProviderSuccess> {
    if scan_requires_entry_finalize(scan_value) {
        return None;
    }

    let scan_event = build_entry_scan_trace_event(provider, &scan.raw_text, Some(scan_value));
    let finalize_skipped_event =
        build_entry_finalize_skipped_trace_event("scan_decision_no_trade", scan_value);
    Some(ProviderSuccess {
        raw_text: build_entry_finalize_skipped_no_trade_raw(scan_value),
        trace: scan
            .trace
            .clone()
            .prepend_entry_stage_events(&[scan_event, finalize_skipped_event]),
    })
}

fn validate_entry_scan_output(value: &Value, prompt_template: &str) -> Result<()> {
    let decision = value
        .get("decision")
        .and_then(Value::as_str)
        .map(str::trim)
        .ok_or_else(|| anyhow!("entry scan decision is missing"))?;
    if !matches!(decision, "LONG" | "SHORT" | "NO_TRADE") {
        return Err(anyhow!("entry scan decision must be LONG/SHORT/NO_TRADE"));
    }
    value
        .get("reason")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .ok_or_else(|| anyhow!("entry scan reason must be non-empty"))?;
    let scan = value
        .get("scan")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("entry scan.scan object is missing"))?;
    let required_keys: &[&str] = if is_medium_large(prompt_template) {
        &[
            "primary_strategy",
            "market_story",
            "hypothesis",
            "conviction",
            "entry_style",
            "candidate_zone",
            "entry_ladder",
            "target_zone",
            "target_ladder",
            "stop_ladder",
            "invalidation",
        ]
    } else {
        &[
            "primary_strategy",
            "setup_quality",
            "order_flow_bias",
            "entry_style",
            "candidate_zone",
            "entry_ladder",
            "target_zone",
            "target_ladder",
            "stop_ladder",
            "invalidation_basis",
            "stop_model_hint",
            "key_signals",
            "risk_flags",
        ]
    };
    for key in required_keys {
        if !scan.contains_key(*key) {
            return Err(anyhow!("entry scan.scan.{} is missing", key));
        }
    }
    for key in ["entry_ladder", "target_ladder", "stop_ladder"] {
        validate_entry_scan_ladder(
            scan.get(key)
                .ok_or_else(|| anyhow!("entry scan.scan.{} is missing", key))?,
            key,
        )?;
    }
    Ok(())
}

fn validate_entry_scan_ladder(value: &Value, field_name: &str) -> Result<()> {
    let ladder = value
        .as_array()
        .ok_or_else(|| anyhow!("entry scan.scan.{} must be an array", field_name))?;
    for (idx, item) in ladder.iter().enumerate() {
        let obj = item
            .as_object()
            .ok_or_else(|| anyhow!("entry scan.scan.{}[{}] must be an object", field_name, idx))?;
        for key in ["price", "label", "anchor_field", "role", "skip_reason"] {
            if !obj.contains_key(key) {
                return Err(anyhow!(
                    "entry scan.scan.{}[{}].{} is missing",
                    field_name,
                    idx,
                    key
                ));
            }
        }
        let price = obj
            .get("price")
            .ok_or_else(|| anyhow!("entry scan.scan.{}[{}].price is missing", field_name, idx))?;
        if !(price.is_null() || price.is_number()) {
            return Err(anyhow!(
                "entry scan.scan.{}[{}].price must be number|null",
                field_name,
                idx
            ));
        }
        for key in ["label", "anchor_field", "role"] {
            let field = obj.get(key).ok_or_else(|| {
                anyhow!("entry scan.scan.{}[{}].{} is missing", field_name, idx, key)
            })?;
            if !field.is_string() {
                return Err(anyhow!(
                    "entry scan.scan.{}[{}].{} must be string",
                    field_name,
                    idx,
                    key
                ));
            }
        }
        let skip_reason = obj.get("skip_reason").ok_or_else(|| {
            anyhow!(
                "entry scan.scan.{}[{}].skip_reason is missing",
                field_name,
                idx
            )
        })?;
        if !(skip_reason.is_null() || skip_reason.is_string()) {
            return Err(anyhow!(
                "entry scan.scan.{}[{}].skip_reason must be string|null",
                field_name,
                idx
            ));
        }
    }
    Ok(())
}

pub async fn invoke_models(
    http_client: &Client,
    config: &RootConfig,
    input: &ModelInvocationInput,
) -> Vec<ModelCallOutput> {
    let default_provider = config.active_default_model();
    let prompt_template = config.llm.prompt_template.clone();
    let mut enabled = config.selected_enabled_models_for_default();
    if enabled.is_empty() && default_provider == "qwen" {
        enabled.push(LlmModelConfig {
            name: "qwen_default".to_string(),
            provider: "qwen".to_string(),
            model: config.api.qwen.model.clone(),
            use_openrouter: None,
            enabled: true,
            temperature: 0.1,
            max_tokens: 1200,
            enable_thinking: None,
            reasoning: None,
        });
    }
    if enabled.is_empty() && default_provider == "custom_llm" {
        enabled.push(LlmModelConfig {
            name: "custom_llm_default".to_string(),
            provider: "custom_llm".to_string(),
            model: config.api.custom_llm.model.clone(),
            use_openrouter: None,
            enabled: true,
            temperature: 0.1,
            max_tokens: 1200,
            enable_thinking: None,
            reasoning: None,
        });
    }
    if enabled.is_empty() && default_provider == "gemini" {
        enabled.push(LlmModelConfig {
            name: "gemini_default".to_string(),
            provider: "gemini".to_string(),
            model: config.api.gemini.model.clone(),
            use_openrouter: Some(true),
            enabled: true,
            temperature: 0.1,
            max_tokens: 1200,
            enable_thinking: None,
            reasoning: None,
        });
    }
    if enabled.is_empty() && default_provider == "grok" {
        enabled.push(LlmModelConfig {
            name: "grok_default".to_string(),
            provider: "grok".to_string(),
            model: config.api.grok.model.clone(),
            use_openrouter: None,
            enabled: true,
            temperature: 0.1,
            max_tokens: 1200,
            enable_thinking: None,
            reasoning: None,
        });
    }
    if enabled.is_empty() {
        return vec![ModelCallOutput {
            model_name: format!("{}_default", default_provider),
            provider: default_provider,
            model: "-".to_string(),
            latency_ms: 0,
            batch_id: None,
            batch_status: None,
            provider_finish_reason: None,
            provider_usage: None,
            raw_response_text: None,
            parsed_decision: None,
            validation_warning: None,
            entry_stage_trace: None,
            entry_stage_prompt_inputs: None,
            error: Some("no enabled model matches llm.default_model".to_string()),
        }];
    }

    let mut tasks = Vec::with_capacity(enabled.len());
    for model in enabled {
        let http_client = http_client.clone();
        let claude_cfg = config.api.claude.clone();
        let qwen_cfg = config.api.qwen.clone();
        let custom_llm_cfg = config.api.custom_llm.clone();
        let gemini_cfg = config.api.gemini.clone();
        let openrouter_cfg = config.api.openrouter.clone();
        let grok_cfg = config.api.grok.clone();
        let input = input.clone();
        let prompt_template = prompt_template.clone();
        tasks.push(async move {
            invoke_one_model(
                &http_client,
                &claude_cfg,
                &qwen_cfg,
                &custom_llm_cfg,
                &gemini_cfg,
                &openrouter_cfg,
                &grok_cfg,
                model,
                &input,
                &prompt_template,
            )
            .await
        });
    }
    join_all(tasks).await
}

async fn invoke_one_model(
    http_client: &Client,
    claude_cfg: &ClaudeApiConfig,
    qwen_cfg: &QwenApiConfig,
    custom_llm_cfg: &CustomLlmApiConfig,
    gemini_cfg: &GeminiApiConfig,
    openrouter_cfg: &OpenRouterApiConfig,
    grok_cfg: &GrokApiConfig,
    model: LlmModelConfig,
    input: &ModelInvocationInput,
    prompt_template: &str,
) -> ModelCallOutput {
    let started = Instant::now();
    let provider = model.provider.clone();
    let model_name = model.name.clone();
    let model_id = model.model.clone();

    let output: Result<ProviderSuccess, ProviderFailure> = match provider
        .to_ascii_lowercase()
        .as_str()
    {
        "claude" => invoke_claude(http_client, claude_cfg, &model, input, prompt_template).await,
        "qwen" => invoke_qwen(http_client, qwen_cfg, &model, input, prompt_template).await,
        "custom_llm" => {
            invoke_custom_llm(http_client, custom_llm_cfg, &model, input, prompt_template).await
        }
        "gemini" => {
            invoke_gemini(
                http_client,
                gemini_cfg,
                openrouter_cfg,
                &model,
                input,
                prompt_template,
            )
            .await
        }
        "grok" => invoke_grok(http_client, grok_cfg, &model, input, prompt_template).await,
        other => Err(ProviderFailure {
            error: anyhow!("unsupported provider: {}", other),
            trace: ProviderTrace::default(),
        }),
    };

    match output {
        Ok(success) => {
            let ProviderSuccess { raw_text, trace } = success;
            let parsed_decision = parse_json_from_text(&raw_text).map(|value| {
                normalize_provider_decision_shape(
                    &provider,
                    value,
                    input.management_mode,
                    input.pending_order_mode,
                )
            });
            let validation_warning = parsed_decision
                .as_ref()
                .and_then(|value| {
                    crate::llm::decision::validate_model_output(
                        value,
                        input.management_mode,
                        input.pending_order_mode,
                    )
                })
                .or_else(|| {
                    if parsed_decision.is_none() {
                        Some("model output is not valid JSON".to_string())
                    } else {
                        None
                    }
                });
            let entry_stage_trace = trace.output_entry_stage_trace();
            let entry_stage_prompt_inputs = trace.output_entry_stage_prompt_inputs();
            ModelCallOutput {
                model_name,
                provider,
                model: model_id,
                latency_ms: started.elapsed().as_millis(),
                batch_id: trace.batch_id,
                batch_status: trace.batch_status,
                provider_finish_reason: trace.provider_finish_reason,
                provider_usage: trace.provider_usage,
                raw_response_text: Some(raw_text),
                parsed_decision,
                validation_warning,
                entry_stage_trace,
                entry_stage_prompt_inputs,
                error: None,
            }
        }
        Err(failure) => {
            let ProviderFailure { error, trace } = failure;
            let entry_stage_trace = trace.output_entry_stage_trace();
            let entry_stage_prompt_inputs = trace.output_entry_stage_prompt_inputs();
            ModelCallOutput {
                model_name,
                provider,
                model: model_id,
                latency_ms: started.elapsed().as_millis(),
                batch_id: trace.batch_id,
                batch_status: trace.batch_status,
                provider_finish_reason: trace.provider_finish_reason,
                provider_usage: trace.provider_usage,
                raw_response_text: None,
                parsed_decision: None,
                validation_warning: None,
                entry_stage_trace,
                entry_stage_prompt_inputs,
                error: Some(format!("{:#}", error)),
            }
        }
    }
}

async fn invoke_qwen(
    http_client: &Client,
    qwen_cfg: &QwenApiConfig,
    model: &LlmModelConfig,
    input: &ModelInvocationInput,
    prompt_template: &str,
) -> Result<ProviderSuccess, ProviderFailure> {
    if is_entry_mode(input) {
        return invoke_qwen_entry_two_stage(http_client, qwen_cfg, model, input, prompt_template)
            .await;
    }
    invoke_qwen_stage(
        http_client,
        qwen_cfg,
        model,
        input,
        prompt_template,
        prompt::EntryPromptStage::Finalize,
        None,
    )
    .await
}

async fn invoke_qwen_entry_two_stage(
    http_client: &Client,
    qwen_cfg: &QwenApiConfig,
    model: &LlmModelConfig,
    input: &ModelInvocationInput,
    prompt_template: &str,
) -> Result<ProviderSuccess, ProviderFailure> {
    let scan = invoke_qwen_stage(
        http_client,
        qwen_cfg,
        model,
        input,
        prompt_template,
        prompt::EntryPromptStage::Scan,
        None,
    )
    .await?;
    let scan_value =
        parse_entry_scan_output("qwen", &scan.raw_text, prompt_template).map_err(|failure| {
            let mut trace = scan.trace.clone();
            trace.push_entry_stage_event(build_entry_scan_trace_event(
                "qwen",
                &scan.raw_text,
                None,
            ));
            ProviderFailure {
                error: failure.error.context("entry scan stage failed"),
                trace,
            }
        })?;
    if let Some(short_circuit) = maybe_short_circuit_entry_finalize("qwen", &scan, &scan_value) {
        return Ok(short_circuit);
    }
    let scan_event = build_entry_scan_trace_event("qwen", &scan.raw_text, Some(&scan_value));
    let scan_prompt_inputs = scan
        .trace
        .output_entry_stage_prompt_inputs()
        .unwrap_or_default();
    let finalize_event = build_entry_finalize_trace_event(input, &scan_value);
    let finalize = invoke_qwen_stage(
        http_client,
        qwen_cfg,
        model,
        input,
        prompt_template,
        prompt::EntryPromptStage::Finalize,
        Some(&scan_value),
    )
    .await
    .map_err(|failure| ProviderFailure {
        error: failure.error,
        trace: failure
            .trace
            .prepend_entry_stage_events(&[scan_event.clone(), finalize_event.clone()])
            .prepend_entry_stage_prompt_inputs(&scan_prompt_inputs),
    })?;

    Ok(ProviderSuccess {
        raw_text: finalize.raw_text,
        trace: finalize
            .trace
            .prepend_entry_stage_events(&[scan_event, finalize_event])
            .prepend_entry_stage_prompt_inputs(&scan_prompt_inputs),
    })
}

async fn invoke_qwen_stage(
    http_client: &Client,
    qwen_cfg: &QwenApiConfig,
    model: &LlmModelConfig,
    input: &ModelInvocationInput,
    prompt_template: &str,
    entry_stage: prompt::EntryPromptStage,
    prior_scan: Option<&Value>,
) -> Result<ProviderSuccess, ProviderFailure> {
    let resolved_model = if model.model.trim().is_empty() {
        qwen_cfg.model.clone()
    } else {
        model.model.clone()
    };
    let response_format = qwen_response_format(
        &resolved_model,
        input.management_mode,
        input.pending_order_mode,
        entry_stage,
        prompt_template,
    );
    let BuiltPromptPair {
        mut system,
        user,
        prompt_input_capture,
    } = build_prompt_pair(input, prompt_template, entry_stage, prior_scan)?;
    let trace = provider_trace_with_prompt_input_capture(prompt_input_capture);
    system.push_str(&qwen_output_contract(
        input.management_mode,
        input.pending_order_mode,
        entry_stage,
        prompt_template,
    ));
    let req = QwenChatCompletionsRequest {
        model: resolved_model.clone(),
        temperature: model.temperature,
        max_tokens: model.max_tokens,
        messages: vec![
            QwenChatMessage {
                role: "system".to_string(),
                content: system,
            },
            QwenChatMessage {
                role: "user".to_string(),
                content: user,
            },
        ],
        response_format,
        // Qwen structured output does not support thinking mode. When schema
        // constraints are active, force thinking off so the model actually
        // follows the requested top-level JSON shape.
        enable_thinking: qwen_enable_thinking(
            &resolved_model,
            model.enable_thinking,
            input.management_mode,
            input.pending_order_mode,
            entry_stage,
            prompt_template,
        ),
        reasoning: None,
        stream: None,
    };

    let url = chat_completions_url(&qwen_cfg.base_api_url);
    let response = http_client
        .post(&url)
        .bearer_auth(qwen_cfg.resolved_api_key())
        .json(&req)
        .send()
        .await
        .context("call qwen chat completions api")
        .map_err(|error| ProviderFailure {
            error,
            trace: trace.clone(),
        })?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(ProviderFailure {
            error: anyhow!(
                "qwen chat completions failed status={} body={}",
                status,
                body
            ),
            trace,
        });
    }

    let body: QwenChatCompletionsResponse = response
        .json()
        .await
        .context("decode qwen chat completions response body")
        .map_err(|error| ProviderFailure {
            error,
            trace: trace.clone(),
        })?;

    let text = body
        .choices
        .first()
        .map(|c| c.message.content.trim().to_string())
        .unwrap_or_default();
    if text.is_empty() {
        return Err(ProviderFailure {
            error: anyhow!("qwen chat completions response text is empty"),
            trace,
        });
    }

    Ok(ProviderSuccess {
        raw_text: text,
        trace,
    })
}

async fn invoke_custom_llm(
    http_client: &Client,
    custom_llm_cfg: &CustomLlmApiConfig,
    model: &LlmModelConfig,
    input: &ModelInvocationInput,
    prompt_template: &str,
) -> Result<ProviderSuccess, ProviderFailure> {
    if is_entry_mode(input) {
        return invoke_custom_llm_entry_two_stage(
            http_client,
            custom_llm_cfg,
            model,
            input,
            prompt_template,
        )
        .await;
    }
    invoke_custom_llm_stage(
        http_client,
        custom_llm_cfg,
        model,
        input,
        prompt_template,
        prompt::EntryPromptStage::Finalize,
        None,
    )
    .await
}

async fn invoke_custom_llm_entry_two_stage(
    http_client: &Client,
    custom_llm_cfg: &CustomLlmApiConfig,
    model: &LlmModelConfig,
    input: &ModelInvocationInput,
    prompt_template: &str,
) -> Result<ProviderSuccess, ProviderFailure> {
    let scan = invoke_custom_llm_stage(
        http_client,
        custom_llm_cfg,
        model,
        input,
        prompt_template,
        prompt::EntryPromptStage::Scan,
        None,
    )
    .await?;
    let scan_value = parse_entry_scan_output("custom_llm", &scan.raw_text, prompt_template)
        .map_err(|failure| {
            let mut trace = scan.trace.clone();
            trace.push_entry_stage_event(build_entry_scan_trace_event(
                "custom_llm",
                &scan.raw_text,
                None,
            ));
            ProviderFailure {
                error: failure.error.context("entry scan stage failed"),
                trace,
            }
        })?;
    if let Some(short_circuit) =
        maybe_short_circuit_entry_finalize("custom_llm", &scan, &scan_value)
    {
        return Ok(short_circuit);
    }
    let scan_event = build_entry_scan_trace_event("custom_llm", &scan.raw_text, Some(&scan_value));
    let scan_prompt_inputs = scan
        .trace
        .output_entry_stage_prompt_inputs()
        .unwrap_or_default();
    let finalize_event = build_entry_finalize_trace_event(input, &scan_value);
    let finalize = invoke_custom_llm_stage(
        http_client,
        custom_llm_cfg,
        model,
        input,
        prompt_template,
        prompt::EntryPromptStage::Finalize,
        Some(&scan_value),
    )
    .await
    .map_err(|failure| ProviderFailure {
        error: failure.error,
        trace: failure
            .trace
            .prepend_entry_stage_events(&[scan_event.clone(), finalize_event.clone()])
            .prepend_entry_stage_prompt_inputs(&scan_prompt_inputs),
    })?;

    Ok(ProviderSuccess {
        raw_text: finalize.raw_text,
        trace: finalize
            .trace
            .prepend_entry_stage_events(&[scan_event, finalize_event])
            .prepend_entry_stage_prompt_inputs(&scan_prompt_inputs),
    })
}

async fn invoke_custom_llm_stage(
    http_client: &Client,
    custom_llm_cfg: &CustomLlmApiConfig,
    model: &LlmModelConfig,
    input: &ModelInvocationInput,
    prompt_template: &str,
    entry_stage: prompt::EntryPromptStage,
    prior_scan: Option<&Value>,
) -> Result<ProviderSuccess, ProviderFailure> {
    let resolved_model = if model.model.trim().is_empty() {
        custom_llm_cfg.model.clone()
    } else {
        model.model.clone()
    };
    let BuiltPromptPair {
        system,
        user,
        prompt_input_capture,
    } = build_prompt_pair(input, prompt_template, entry_stage, prior_scan)?;
    let trace = provider_trace_with_prompt_input_capture(prompt_input_capture);
    let req = QwenChatCompletionsRequest {
        model: resolved_model,
        temperature: model.temperature,
        max_tokens: model.max_tokens,
        messages: vec![
            QwenChatMessage {
                role: "system".to_string(),
                content: system,
            },
            QwenChatMessage {
                role: "user".to_string(),
                content: user,
            },
        ],
        response_format: Some(custom_llm_json_schema_response_format(
            input.management_mode,
            input.pending_order_mode,
            entry_stage,
            prompt_template,
        )),
        enable_thinking: None,
        reasoning: custom_llm_reasoning(model),
        stream: None,
    };

    let url = chat_completions_url(&custom_llm_cfg.base_api_url);
    let response = http_client
        .post(&url)
        .bearer_auth(custom_llm_cfg.resolved_api_key())
        .json(&req)
        .send()
        .await
        .context("call custom_llm chat completions api")
        .map_err(|error| ProviderFailure {
            error,
            trace: trace.clone(),
        })?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(ProviderFailure {
            error: anyhow!(
                "custom_llm chat completions failed status={} body={}",
                status,
                body
            ),
            trace,
        });
    }

    let body: QwenChatCompletionsResponse = response
        .json()
        .await
        .context("decode custom_llm chat completions response body")
        .map_err(|error| ProviderFailure {
            error,
            trace: trace.clone(),
        })?;

    let text = body
        .choices
        .first()
        .map(|c| c.message.content.trim().to_string())
        .unwrap_or_default();
    if text.is_empty() {
        return Err(ProviderFailure {
            error: anyhow!("custom_llm chat completions response text is empty"),
            trace,
        });
    }

    Ok(ProviderSuccess {
        raw_text: text,
        trace,
    })
}

fn chat_completions_url(base_api_url: &str) -> String {
    let trimmed = base_api_url.trim().trim_end_matches('/');
    if trimmed.ends_with("/chat/completions") {
        trimmed.to_string()
    } else {
        format!("{}/chat/completions", trimmed)
    }
}

fn custom_llm_reasoning(model: &LlmModelConfig) -> Option<OpenAiCompatibleReasoningConfig> {
    let effort = model.reasoning.as_deref()?.trim();
    if effort.is_empty() {
        return None;
    }
    Some(OpenAiCompatibleReasoningConfig {
        effort: effort.to_string(),
    })
}

fn qwen_response_format(
    model: &str,
    management_mode: bool,
    pending_order_mode: bool,
    entry_stage: prompt::EntryPromptStage,
    prompt_template: &str,
) -> Option<QwenResponseFormat> {
    if !qwen_supports_json_schema(model) {
        return None;
    }
    Some(openai_json_schema_response_format(
        management_mode,
        pending_order_mode,
        entry_stage,
        prompt_template,
    ))
}

fn qwen_supports_json_schema(model: &str) -> bool {
    let model_id = model.trim().to_ascii_lowercase();
    model_id.starts_with("qwen3-max")
        || model_id.starts_with("qwen-plus")
        || model_id.starts_with("qwen-flash")
}

fn qwen_enable_thinking(
    model: &str,
    requested: Option<bool>,
    management_mode: bool,
    pending_order_mode: bool,
    entry_stage: prompt::EntryPromptStage,
    prompt_template: &str,
) -> Option<bool> {
    Some(
        requested.unwrap_or(false)
            && qwen_response_format(
                model,
                management_mode,
                pending_order_mode,
                entry_stage,
                prompt_template,
            )
            .is_none(),
    )
}

fn openai_json_schema_response_format(
    management_mode: bool,
    pending_order_mode: bool,
    entry_stage: prompt::EntryPromptStage,
    prompt_template: &str,
) -> QwenResponseFormat {
    QwenResponseFormat {
        kind: "json_schema".to_string(),
        json_schema: QwenResponseJsonSchema {
            name: if pending_order_mode {
                "pending_order_decision".to_string()
            } else if management_mode {
                "management_decision".to_string()
            } else if matches!(entry_stage, prompt::EntryPromptStage::Scan) {
                "entry_scan_decision".to_string()
            } else {
                "entry_decision".to_string()
            },
            strict: true,
            schema: qwen_response_schema(
                management_mode,
                pending_order_mode,
                entry_stage,
                prompt_template,
            ),
        },
    }
}

fn custom_llm_json_schema_response_format(
    management_mode: bool,
    pending_order_mode: bool,
    entry_stage: prompt::EntryPromptStage,
    prompt_template: &str,
) -> QwenResponseFormat {
    QwenResponseFormat {
        kind: "json_schema".to_string(),
        json_schema: QwenResponseJsonSchema {
            name: if pending_order_mode {
                "pending_order_decision".to_string()
            } else if management_mode {
                "management_decision".to_string()
            } else if matches!(entry_stage, prompt::EntryPromptStage::Scan) {
                "entry_scan_decision".to_string()
            } else {
                "entry_decision".to_string()
            },
            strict: true,
            schema: custom_llm_response_schema(
                management_mode,
                pending_order_mode,
                entry_stage,
                prompt_template,
            ),
        },
    }
}

fn is_medium_large(prompt_template: &str) -> bool {
    prompt_template
        .trim()
        .eq_ignore_ascii_case("medium_large_opportunity")
}

fn scan_target_ladder_schema() -> Value {
    json!({
        "type": "array",
        "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["price", "label", "anchor_field", "role", "skip_reason"],
            "properties": {
                "price": { "type": ["number", "null"] },
                "label": { "type": "string" },
                "anchor_field": { "type": "string" },
                "role": { "type": "string" },
                "skip_reason": { "type": ["string", "null"] }
            }
        }
    })
}

fn scan_ladder_schema() -> Value {
    scan_target_ladder_schema()
}

fn gemini_scan_target_ladder_schema() -> Value {
    json!({
        "type": "ARRAY",
        "items": {
            "type": "OBJECT",
            "properties": {
                "price": { "type": "NUMBER", "nullable": true },
                "label": { "type": "STRING" },
                "anchor_field": { "type": "STRING" },
                "role": { "type": "STRING" },
                "skip_reason": { "type": "STRING", "nullable": true }
            },
            "required": ["price", "label", "anchor_field", "role", "skip_reason"]
        }
    })
}

fn gemini_scan_ladder_schema() -> Value {
    gemini_scan_target_ladder_schema()
}

fn qwen_output_contract(
    management_mode: bool,
    pending_order_mode: bool,
    entry_stage: prompt::EntryPromptStage,
    prompt_template: &str,
) -> String {
    if pending_order_mode {
        "\n\nQWEN OUTPUT CONTRACT:\n- Return exactly one JSON object.\n- Keep `reason` as a top-level string. Do not place `reason` inside `analysis`.\n- Top-level keys must be `decision`, `reason`, and `params`. `analysis` and `self_check` may be present as extra objects.\n- Allowed pending-order decisions: HOLD, CLOSE, MODIFY_MAKER.\n- For MODIFY_MAKER, params must use `new_entry`, `new_tp`, and `new_sl` (set unchanged fields to null).\n".to_string()
    } else if management_mode {
        "\n\nQWEN OUTPUT CONTRACT:\n- Return exactly one JSON object.\n- Keep `reason` as a top-level string. Do not place `reason` inside `analysis`.\n- Top-level keys must be `decision`, `reason`, and `params`. `analysis` and `self_check` may be present as extra objects.\n- Allowed management decisions: CLOSE, ADD, REDUCE, HOLD, MODIFY_TPSL.\n".to_string()
    } else if matches!(entry_stage, prompt::EntryPromptStage::Scan) {
        if is_medium_large(prompt_template) {
            "\n\nQWEN OUTPUT CONTRACT:\n- Return exactly one JSON object.\n- Keep `reason` as a top-level string.\n- Top-level keys must be `decision`, `reason`, and `scan`.\n- Allowed entry scan decisions: LONG, SHORT, NO_TRADE.\n- `scan` must include: primary_strategy, market_story, hypothesis, conviction, entry_style, candidate_zone, entry_ladder, target_zone, target_ladder, stop_ladder, invalidation.\n".to_string()
        } else {
            "\n\nQWEN OUTPUT CONTRACT:\n- Return exactly one JSON object.\n- Keep `reason` as a top-level string.\n- Top-level keys must be `decision`, `reason`, and `scan`.\n- Allowed entry scan decisions: LONG, SHORT, NO_TRADE.\n- `scan` must include: primary_strategy, setup_quality, order_flow_bias, entry_style, candidate_zone, entry_ladder, target_zone, target_ladder, stop_ladder, invalidation_basis, stop_model_hint, key_signals, risk_flags.\n".to_string()
        }
    } else {
        "\n\nQWEN OUTPUT CONTRACT:\n- Return exactly one JSON object.\n- Keep `reason` as a top-level string. Do not place `reason` inside `analysis`.\n- Top-level keys must be `decision`, `reason`, and `params`. `analysis` and `self_check` may be present as extra objects.\n- Allowed entry decisions: LONG, SHORT, NO_TRADE. Never use HOLD in entry mode.\n- For LONG or SHORT, params must include `entry`, `tp`, `sl`, `leverage`, `rr`, and `horizon`.\n- For NO_TRADE, set `params.entry`, `params.tp`, `params.sl`, `params.leverage`, `params.rr`, and `params.horizon` to null.\n".to_string()
    }
}

fn normalize_provider_decision_shape(
    provider: &str,
    value: Value,
    management_mode: bool,
    pending_order_mode: bool,
) -> Value {
    if provider.eq_ignore_ascii_case("qwen") || provider.eq_ignore_ascii_case("custom_llm") {
        normalize_qwen_decision_shape(value, management_mode, pending_order_mode)
    } else {
        value
    }
}

fn normalize_qwen_decision_shape(
    mut value: Value,
    management_mode: bool,
    pending_order_mode: bool,
) -> Value {
    let Some(obj) = value.as_object_mut() else {
        return value;
    };

    let top_level_reason_missing = obj
        .get("reason")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .is_none();

    if top_level_reason_missing {
        let lifted_reason = obj
            .get("analysis")
            .and_then(Value::as_object)
            .and_then(|analysis| {
                analysis
                    .get("reason")
                    .and_then(Value::as_str)
                    .or_else(|| analysis.get("no_trade_rationale").and_then(Value::as_str))
                    .or_else(|| analysis.get("strategy_evaluation").and_then(Value::as_str))
            })
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(ToOwned::to_owned);
        if let Some(reason) = lifted_reason {
            obj.insert("reason".to_string(), Value::String(reason));
        }
    }

    if !management_mode && !pending_order_mode {
        if let Some(decision) = obj.get("decision").and_then(Value::as_str).map(str::trim) {
            if decision.eq_ignore_ascii_case("hold") {
                obj.insert(
                    "decision".to_string(),
                    Value::String("NO_TRADE".to_string()),
                );
            }
        }
    }

    value
}

fn qwen_response_schema(
    management_mode: bool,
    pending_order_mode: bool,
    entry_stage: prompt::EntryPromptStage,
    prompt_template: &str,
) -> Value {
    if pending_order_mode {
        qwen_pending_order_response_schema()
    } else if management_mode {
        if is_medium_large(prompt_template) {
            ml_qwen_management_schema()
        } else {
            qwen_management_response_schema()
        }
    } else if matches!(entry_stage, prompt::EntryPromptStage::Scan) {
        if is_medium_large(prompt_template) {
            ml_qwen_entry_scan_schema()
        } else {
            qwen_entry_scan_response_schema()
        }
    } else {
        if is_medium_large(prompt_template) {
            ml_qwen_entry_schema()
        } else {
            qwen_entry_response_schema()
        }
    }
}

fn custom_llm_response_schema(
    management_mode: bool,
    pending_order_mode: bool,
    entry_stage: prompt::EntryPromptStage,
    prompt_template: &str,
) -> Value {
    if pending_order_mode {
        custom_llm_pending_order_response_schema()
    } else if management_mode {
        if is_medium_large(prompt_template) {
            ml_custom_llm_management_schema()
        } else {
            custom_llm_management_response_schema()
        }
    } else if matches!(entry_stage, prompt::EntryPromptStage::Scan) {
        if is_medium_large(prompt_template) {
            ml_custom_llm_entry_scan_schema()
        } else {
            custom_llm_entry_scan_response_schema()
        }
    } else {
        if is_medium_large(prompt_template) {
            ml_custom_llm_entry_schema()
        } else {
            custom_llm_entry_response_schema()
        }
    }
}

fn qwen_entry_scan_response_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": true,
        "required": ["decision", "reason", "scan"],
        "properties": {
            "decision": {
                "type": "string",
                "enum": ["LONG", "SHORT", "NO_TRADE"]
            },
            "reason": {
                "type": "string",
                "minLength": 1
            },
            "scan": {
                "type": "object",
                "additionalProperties": true,
                "required": [
                    "primary_strategy",
                    "setup_quality",
                    "order_flow_bias",
                    "entry_style",
                    "candidate_zone",
                    "entry_ladder",
                    "target_zone",
                    "target_ladder",
                    "stop_ladder",
                    "invalidation_basis",
                    "stop_model_hint",
                    "key_signals",
                    "risk_flags"
                ],
                "properties": {
                    "entry_ladder": scan_ladder_schema(),
                    "target_ladder": scan_ladder_schema(),
                    "stop_ladder": scan_ladder_schema()
                }
            }
        }
    })
}

fn qwen_entry_response_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": true,
        "required": ["decision", "reason", "params"],
        "properties": {
            "decision": {
                "type": "string",
                "enum": ["LONG", "SHORT", "NO_TRADE"]
            },
            "reason": {
                "type": "string",
                "minLength": 1
            },
            "params": {
                "type": "object",
                "additionalProperties": true
            },
            "analysis": {
                "type": ["object", "null"],
                "additionalProperties": true
            },
            "self_check": {
                "type": ["object", "null"],
                "additionalProperties": true
            }
        }
    })
}

fn custom_llm_entry_scan_response_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["decision", "reason", "scan"],
        "properties": {
            "decision": {
                "type": "string",
                "enum": ["LONG", "SHORT", "NO_TRADE"]
            },
            "reason": {
                "type": "string",
                "minLength": 1
            },
            "scan": {
                "type": "object",
                "additionalProperties": false,
                "required": [
                    "primary_strategy",
                    "setup_quality",
                    "order_flow_bias",
                    "entry_style",
                    "candidate_zone",
                    "entry_ladder",
                    "target_zone",
                    "target_ladder",
                    "stop_ladder",
                    "invalidation_basis",
                    "stop_model_hint",
                    "key_signals",
                    "risk_flags"
                ],
                "properties": {
                    "primary_strategy": {"type": "string"},
                    "setup_quality": {"type": "string"},
                    "order_flow_bias": {"type": "string"},
                    "entry_style": {"type": ["string", "null"]},
                    "candidate_zone": {"type": ["string", "null"]},
                    "entry_ladder": scan_ladder_schema(),
                    "target_zone": {"type": ["string", "null"]},
                    "target_ladder": scan_ladder_schema(),
                    "stop_ladder": scan_ladder_schema(),
                    "invalidation_basis": {"type": ["string", "null"]},
                    "stop_model_hint": {"type": ["string", "null"]},
                    "key_signals": {"type": "string"},
                    "risk_flags": {"type": "string"}
                }
            }
        }
    })
}

fn custom_llm_entry_response_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["decision", "reason", "params"],
        "properties": {
            "decision": {
                "type": "string",
                "enum": ["LONG", "SHORT", "NO_TRADE"]
            },
            "reason": {
                "type": "string",
                "minLength": 1
            },
            "params": {
                "type": "object",
                "additionalProperties": false,
                "required": ["entry", "tp", "sl", "leverage", "rr", "horizon"],
                "properties": {
                    "entry": {"type": ["number", "null"]},
                    "tp": {"type": ["number", "null"]},
                    "sl": {"type": ["number", "null"]},
                    "leverage": {"type": ["number", "null"]},
                    "rr": {"type": ["number", "null"]},
                    "horizon": {"type": ["string", "null"]}
                }
            }
        }
    })
}

fn qwen_management_response_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": true,
        "required": ["decision", "reason", "params"],
        "properties": {
            "decision": {
                "type": "string",
                "enum": ["CLOSE", "ADD", "REDUCE", "HOLD", "MODIFY_TPSL"]
            },
            "reason": {
                "type": "string",
                "minLength": 1
            },
            "params": {
                "type": "object",
                "additionalProperties": true
            },
            "analysis": {
                "type": ["object", "null"],
                "additionalProperties": true
            },
            "self_check": {
                "type": ["object", "null"],
                "additionalProperties": true
            }
        }
    })
}

fn custom_llm_management_response_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["decision", "reason", "params"],
        "properties": {
            "decision": {
                "type": "string",
                "enum": ["CLOSE", "ADD", "REDUCE", "HOLD", "MODIFY_TPSL"]
            },
            "reason": {
                "type": "string",
                "minLength": 1
            },
            "params": {
                "type": "object",
                "additionalProperties": false,
                "required": ["qty", "qty_ratio", "is_full_exit", "new_tp", "new_sl"],
                "properties": {
                    "qty": {"type": ["number", "null"]},
                    "qty_ratio": {"type": ["number", "null"]},
                    "is_full_exit": {"type": ["boolean", "null"]},
                    "new_tp": {"type": ["number", "null"]},
                    "new_sl": {"type": ["number", "null"]}
                }
            }
        }
    })
}

fn qwen_pending_order_response_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": true,
        "required": ["decision", "reason", "params"],
        "properties": {
            "decision": {
                "type": "string",
                "enum": ["HOLD", "CLOSE", "MODIFY_MAKER"]
            },
            "reason": {
                "type": "string",
                "minLength": 1
            },
            "params": {
                "type": "object",
                "additionalProperties": true
            },
            "analysis": {
                "type": ["object", "null"],
                "additionalProperties": true
            },
            "self_check": {
                "type": ["object", "null"],
                "additionalProperties": true
            }
        }
    })
}

fn custom_llm_pending_order_response_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["decision", "reason", "params"],
        "properties": {
            "decision": {
                "type": "string",
                "enum": ["HOLD", "CLOSE", "MODIFY_MAKER"]
            },
            "reason": {
                "type": "string",
                "minLength": 1
            },
            "params": {
                "type": "object",
                "additionalProperties": false,
                "required": ["new_entry", "new_tp", "new_sl"],
                "properties": {
                    "new_entry": {"type": ["number", "null"]},
                    "new_tp": {"type": ["number", "null"]},
                    "new_sl": {"type": ["number", "null"]}
                }
            }
        }
    })
}

async fn invoke_gemini(
    http_client: &Client,
    gemini_cfg: &GeminiApiConfig,
    openrouter_cfg: &OpenRouterApiConfig,
    model: &LlmModelConfig,
    input: &ModelInvocationInput,
    prompt_template: &str,
) -> Result<ProviderSuccess, ProviderFailure> {
    if is_entry_mode(input) {
        return invoke_gemini_entry_two_stage(
            http_client,
            gemini_cfg,
            openrouter_cfg,
            model,
            input,
            prompt_template,
        )
        .await;
    }
    if model.should_use_openrouter() {
        return invoke_gemini_via_openrouter(
            http_client,
            openrouter_cfg,
            model,
            input,
            prompt_template,
            prompt::EntryPromptStage::Finalize,
            None,
        )
        .await;
    }

    invoke_gemini_direct(
        http_client,
        gemini_cfg,
        model,
        input,
        prompt_template,
        prompt::EntryPromptStage::Finalize,
        None,
    )
    .await
}

async fn invoke_gemini_entry_two_stage(
    http_client: &Client,
    gemini_cfg: &GeminiApiConfig,
    openrouter_cfg: &OpenRouterApiConfig,
    model: &LlmModelConfig,
    input: &ModelInvocationInput,
    prompt_template: &str,
) -> Result<ProviderSuccess, ProviderFailure> {
    let scan = if model.should_use_openrouter() {
        invoke_gemini_via_openrouter(
            http_client,
            openrouter_cfg,
            model,
            input,
            prompt_template,
            prompt::EntryPromptStage::Scan,
            None,
        )
        .await?
    } else {
        invoke_gemini_direct(
            http_client,
            gemini_cfg,
            model,
            input,
            prompt_template,
            prompt::EntryPromptStage::Scan,
            None,
        )
        .await?
    };
    let scan_value =
        parse_entry_scan_output("gemini", &scan.raw_text, prompt_template).map_err(|failure| {
            let mut trace = scan.trace.clone();
            trace.push_entry_stage_event(build_entry_scan_trace_event(
                "gemini",
                &scan.raw_text,
                None,
            ));
            ProviderFailure {
                error: failure.error.context("entry scan stage failed"),
                trace,
            }
        })?;
    if let Some(short_circuit) = maybe_short_circuit_entry_finalize("gemini", &scan, &scan_value) {
        return Ok(short_circuit);
    }
    let scan_event = build_entry_scan_trace_event("gemini", &scan.raw_text, Some(&scan_value));
    let scan_prompt_inputs = scan
        .trace
        .output_entry_stage_prompt_inputs()
        .unwrap_or_default();
    let finalize_event = build_entry_finalize_trace_event(input, &scan_value);
    let finalize = if model.should_use_openrouter() {
        invoke_gemini_via_openrouter(
            http_client,
            openrouter_cfg,
            model,
            input,
            prompt_template,
            prompt::EntryPromptStage::Finalize,
            Some(&scan_value),
        )
        .await
    } else {
        invoke_gemini_direct(
            http_client,
            gemini_cfg,
            model,
            input,
            prompt_template,
            prompt::EntryPromptStage::Finalize,
            Some(&scan_value),
        )
        .await
    }
    .map_err(|failure| ProviderFailure {
        error: failure.error,
        trace: failure
            .trace
            .prepend_entry_stage_events(&[scan_event.clone(), finalize_event.clone()])
            .prepend_entry_stage_prompt_inputs(&scan_prompt_inputs),
    })?;

    Ok(ProviderSuccess {
        raw_text: finalize.raw_text,
        trace: finalize
            .trace
            .prepend_entry_stage_events(&[scan_event, finalize_event])
            .prepend_entry_stage_prompt_inputs(&scan_prompt_inputs),
    })
}

async fn invoke_gemini_direct(
    http_client: &Client,
    gemini_cfg: &GeminiApiConfig,
    model: &LlmModelConfig,
    input: &ModelInvocationInput,
    prompt_template: &str,
    entry_stage: prompt::EntryPromptStage,
    prior_scan: Option<&Value>,
) -> Result<ProviderSuccess, ProviderFailure> {
    let BuiltPromptPair {
        system,
        user,
        prompt_input_capture,
    } = build_prompt_pair(input, prompt_template, entry_stage, prior_scan)?;
    let mut trace = provider_trace_with_prompt_input_capture(prompt_input_capture);

    let model_id = if model.model.trim().is_empty() {
        gemini_cfg.model.clone()
    } else {
        model.model.clone()
    };

    let base = gemini_cfg.base_api_url.trim_end_matches('/');
    let endpoint = format!("{}/{}:generateContent", base, gemini_model_path(&model_id));
    let mut url = reqwest::Url::parse(&endpoint)
        .context("parse gemini generateContent url")
        .map_err(|error| ProviderFailure {
            error,
            trace: trace.clone(),
        })?;
    url.query_pairs_mut()
        .append_pair("key", &gemini_cfg.resolved_api_key());

    let req = GeminiGenerateContentRequest {
        system_instruction: Some(GeminiInstruction {
            parts: vec![GeminiPart { text: system }],
        }),
        contents: vec![GeminiContent {
            role: "user".to_string(),
            parts: vec![GeminiPart { text: user }],
        }],
        generation_config: Some(GeminiGenerationConfig {
            temperature: model.temperature,
            max_output_tokens: model.max_tokens,
            response_mime_type: Some("application/json".to_string()),
            response_schema: Some(gemini_response_schema(
                input.management_mode,
                input.pending_order_mode,
                entry_stage,
                prompt_template,
            )),
        }),
    };

    let response = http_client
        .post(url)
        .json(&req)
        .send()
        .await
        .context("call gemini generateContent api")
        .map_err(|error| ProviderFailure {
            error,
            trace: trace.clone(),
        })?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(ProviderFailure {
            error: anyhow!(
                "gemini generateContent failed status={} body={}",
                status,
                body
            ),
            trace,
        });
    }

    let body: GeminiGenerateContentResponse = response
        .json()
        .await
        .context("decode gemini generateContent response body")
        .map_err(|error| ProviderFailure {
            error,
            trace: trace.clone(),
        })?;

    let finish_reason = body
        .candidates
        .iter()
        .find_map(|candidate| candidate.finish_reason.clone());

    let text = body
        .candidates
        .iter()
        .filter_map(|candidate| candidate.content.as_ref())
        .flat_map(|content| content.parts.iter())
        .filter_map(|part| part.text.as_deref())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    if text.is_empty() {
        return Err(ProviderFailure {
            error: anyhow!("gemini generateContent response text is empty"),
            trace,
        });
    }

    trace.provider_finish_reason = finish_reason;
    trace.provider_usage = body.usage_metadata;
    Ok(ProviderSuccess {
        raw_text: text,
        trace,
    })
}

async fn invoke_gemini_via_openrouter(
    http_client: &Client,
    openrouter_cfg: &OpenRouterApiConfig,
    model: &LlmModelConfig,
    input: &ModelInvocationInput,
    prompt_template: &str,
    entry_stage: prompt::EntryPromptStage,
    prior_scan: Option<&Value>,
) -> Result<ProviderSuccess, ProviderFailure> {
    let BuiltPromptPair {
        system,
        user,
        prompt_input_capture,
    } = build_prompt_pair(input, prompt_template, entry_stage, prior_scan)?;
    let mut trace = provider_trace_with_prompt_input_capture(prompt_input_capture);
    let req = OpenRouterChatCompletionsRequest {
        model: openrouter_gemini_model_name(&model.model),
        temperature: model.temperature,
        max_tokens: model.max_tokens,
        messages: vec![
            OpenRouterChatMessage {
                role: "system".to_string(),
                // Array format with cache_control so OpenRouter passes the caching hint
                // to Gemini. The system prompt is static across invocations and is the
                // ideal candidate for provider-side prompt caching (≥1024 tokens threshold).
                content: json!([{
                    "type": "text",
                    "text": system,
                    "cache_control": {"type": "ephemeral"}
                }]),
            },
            OpenRouterChatMessage {
                role: "user".to_string(),
                content: Value::String(user),
            },
        ],
        reasoning: Some(OpenRouterReasoning { enabled: true }),
    };

    let base = openrouter_cfg.base_api_url.trim_end_matches('/');
    let url = format!("{}/chat/completions", base);
    let mut request = http_client
        .post(&url)
        .bearer_auth(openrouter_cfg.resolved_api_key())
        .json(&req);
    let site_url = openrouter_cfg.site_url.trim();
    if !site_url.is_empty() {
        request = request.header("HTTP-Referer", site_url);
    }
    let app_name = openrouter_cfg.app_name.trim();
    if !app_name.is_empty() {
        request = request.header("X-Title", app_name);
    }
    let response = request
        .send()
        .await
        .context("call openrouter chat completions api")
        .map_err(|error| ProviderFailure {
            error,
            trace: trace.clone(),
        })?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(ProviderFailure {
            error: anyhow!(
                "openrouter chat completions failed status={} body={}",
                status,
                body
            ),
            trace,
        });
    }

    let body: OpenRouterChatCompletionsResponse = response
        .json()
        .await
        .context("decode openrouter chat completions response body")
        .map_err(|error| ProviderFailure {
            error,
            trace: trace.clone(),
        })?;

    let finish_reason = body
        .choices
        .iter()
        .find_map(|choice| choice.finish_reason.clone());

    let text = body
        .choices
        .iter()
        .filter_map(|choice| choice.message.as_ref())
        .filter_map(|message| extract_chat_message_text(&message.content))
        .collect::<Vec<_>>()
        .join("\n");
    if text.trim().is_empty() {
        return Err(ProviderFailure {
            error: anyhow!("openrouter chat completions response text is empty"),
            trace,
        });
    }

    trace.provider_finish_reason = finish_reason;
    trace.provider_usage = body.usage;
    Ok(ProviderSuccess {
        raw_text: text,
        trace,
    })
}

async fn invoke_grok(
    http_client: &Client,
    grok_cfg: &GrokApiConfig,
    model: &LlmModelConfig,
    input: &ModelInvocationInput,
    prompt_template: &str,
) -> Result<ProviderSuccess, ProviderFailure> {
    if is_entry_mode(input) {
        return invoke_grok_entry_two_stage(http_client, grok_cfg, model, input, prompt_template)
            .await;
    }
    invoke_grok_stage(
        http_client,
        grok_cfg,
        model,
        input,
        prompt_template,
        prompt::EntryPromptStage::Finalize,
        None,
    )
    .await
}

async fn invoke_grok_entry_two_stage(
    http_client: &Client,
    grok_cfg: &GrokApiConfig,
    model: &LlmModelConfig,
    input: &ModelInvocationInput,
    prompt_template: &str,
) -> Result<ProviderSuccess, ProviderFailure> {
    let scan = invoke_grok_stage(
        http_client,
        grok_cfg,
        model,
        input,
        prompt_template,
        prompt::EntryPromptStage::Scan,
        None,
    )
    .await?;
    let scan_value =
        parse_entry_scan_output("grok", &scan.raw_text, prompt_template).map_err(|failure| {
            let mut trace = scan.trace.clone();
            trace.push_entry_stage_event(build_entry_scan_trace_event(
                "grok",
                &scan.raw_text,
                None,
            ));
            ProviderFailure {
                error: failure.error.context("entry scan stage failed"),
                trace,
            }
        })?;
    if let Some(short_circuit) = maybe_short_circuit_entry_finalize("grok", &scan, &scan_value) {
        return Ok(short_circuit);
    }
    let scan_event = build_entry_scan_trace_event("grok", &scan.raw_text, Some(&scan_value));
    let scan_prompt_inputs = scan
        .trace
        .output_entry_stage_prompt_inputs()
        .unwrap_or_default();
    let finalize_event = build_entry_finalize_trace_event(input, &scan_value);
    let finalize = invoke_grok_stage(
        http_client,
        grok_cfg,
        model,
        input,
        prompt_template,
        prompt::EntryPromptStage::Finalize,
        Some(&scan_value),
    )
    .await
    .map_err(|failure| ProviderFailure {
        error: failure.error,
        trace: failure
            .trace
            .prepend_entry_stage_events(&[scan_event.clone(), finalize_event.clone()])
            .prepend_entry_stage_prompt_inputs(&scan_prompt_inputs),
    })?;

    Ok(ProviderSuccess {
        raw_text: finalize.raw_text,
        trace: finalize
            .trace
            .prepend_entry_stage_events(&[scan_event, finalize_event])
            .prepend_entry_stage_prompt_inputs(&scan_prompt_inputs),
    })
}

async fn invoke_grok_stage(
    http_client: &Client,
    grok_cfg: &GrokApiConfig,
    model: &LlmModelConfig,
    input: &ModelInvocationInput,
    prompt_template: &str,
    entry_stage: prompt::EntryPromptStage,
    prior_scan: Option<&Value>,
) -> Result<ProviderSuccess, ProviderFailure> {
    let BuiltPromptPair {
        system,
        user,
        prompt_input_capture,
    } = build_prompt_pair(input, prompt_template, entry_stage, prior_scan)?;
    let mut trace = provider_trace_with_prompt_input_capture(prompt_input_capture);
    let req = GrokResponsesRequest {
        model: if model.model.trim().is_empty() {
            grok_cfg.model.clone()
        } else {
            model.model.clone()
        },
        temperature: model.temperature,
        max_output_tokens: model.max_tokens,
        store: false,
        input: vec![
            GrokResponsesInputMessage {
                role: "system".to_string(),
                content: system,
            },
            GrokResponsesInputMessage {
                role: "user".to_string(),
                content: user,
            },
        ],
        text: grok_text_config(
            input.management_mode,
            input.pending_order_mode,
            entry_stage,
            prompt_template,
        ),
    };

    let base = grok_cfg.base_api_url.trim_end_matches('/');
    let url = format!("{}/responses", base);
    let response = http_client
        .post(&url)
        .bearer_auth(grok_cfg.resolved_api_key())
        .json(&req)
        .send()
        .await
        .context("call grok responses api")
        .map_err(|error| ProviderFailure {
            error,
            trace: trace.clone(),
        })?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(ProviderFailure {
            error: anyhow!("grok responses api failed status={} body={}", status, body),
            trace,
        });
    }

    let body: GrokResponsesApiResponse = response
        .json()
        .await
        .context("decode grok responses api response body")
        .map_err(|error| ProviderFailure {
            error,
            trace: trace.clone(),
        })?;

    let finish_reason = body.status.clone();
    let text = extract_grok_response_text(&body).ok_or_else(|| ProviderFailure {
        error: anyhow!(
            "grok responses api response text is empty status={} incomplete_details={}",
            body.status.as_deref().unwrap_or("-"),
            body.incomplete_details
                .as_ref()
                .map(Value::to_string)
                .unwrap_or_else(|| "null".to_string())
        ),
        trace: trace.clone(),
    })?;
    if text.trim().is_empty() {
        return Err(ProviderFailure {
            error: anyhow!("grok responses api response text is empty"),
            trace,
        });
    }

    trace.provider_finish_reason = finish_reason;
    trace.provider_usage = body.usage;
    Ok(ProviderSuccess {
        raw_text: text,
        trace,
    })
}

fn gemini_model_path(model: &str) -> String {
    let trimmed = model.trim();
    if trimmed.starts_with("models/") {
        trimmed.to_string()
    } else {
        format!("models/{}", trimmed)
    }
}

fn openrouter_gemini_model_name(model: &str) -> String {
    let trimmed = model.trim();
    if trimmed.is_empty() {
        return "google/gemini-3.1-pro-preview".to_string();
    }
    if trimmed.contains('/') {
        return trimmed.to_string();
    }
    if let Some(stripped) = trimmed.strip_prefix("models/") {
        return format!("google/{}", stripped);
    }
    format!("google/{}", trimmed)
}

fn grok_text_config(
    management_mode: bool,
    pending_order_mode: bool,
    entry_stage: prompt::EntryPromptStage,
    prompt_template: &str,
) -> GrokResponsesTextConfig {
    GrokResponsesTextConfig {
        format: GrokResponsesFormat {
            kind: "json_schema".to_string(),
            name: if pending_order_mode {
                "pending_order_decision".to_string()
            } else if management_mode {
                "management_decision".to_string()
            } else if matches!(entry_stage, prompt::EntryPromptStage::Scan) {
                "entry_scan_decision".to_string()
            } else {
                "entry_decision".to_string()
            },
            strict: true,
            schema: if pending_order_mode {
                grok_pending_order_response_schema()
            } else if management_mode {
                if is_medium_large(prompt_template) {
                    ml_grok_management_schema()
                } else {
                    grok_management_response_schema()
                }
            } else if matches!(entry_stage, prompt::EntryPromptStage::Scan) {
                if is_medium_large(prompt_template) {
                    ml_grok_entry_scan_schema()
                } else {
                    grok_entry_scan_response_schema()
                }
            } else {
                if is_medium_large(prompt_template) {
                    ml_grok_entry_schema()
                } else {
                    grok_entry_response_schema()
                }
            },
        },
    }
}

fn grok_entry_scan_response_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["decision", "reason", "scan"],
        "properties": {
            "decision": {
                "type": "string",
                "enum": ["LONG", "SHORT", "NO_TRADE"]
            },
            "reason": { "type": "string" },
            "scan": {
                "type": "object",
                "additionalProperties": false,
                "required": [
                    "primary_strategy",
                    "setup_quality",
                    "order_flow_bias",
                    "entry_style",
                    "candidate_zone",
                    "entry_ladder",
                    "target_zone",
                    "target_ladder",
                    "stop_ladder",
                    "invalidation_basis",
                    "stop_model_hint",
                    "key_signals",
                    "risk_flags"
                ],
                "properties": {
                    "primary_strategy": { "type": "string" },
                    "setup_quality": { "type": "string" },
                    "order_flow_bias": { "type": "string" },
                    "entry_style": { "type": ["string", "null"] },
                    "candidate_zone": { "type": ["string", "null"] },
                    "entry_ladder": scan_ladder_schema(),
                    "target_zone": { "type": ["string", "null"] },
                    "target_ladder": scan_ladder_schema(),
                    "stop_ladder": scan_ladder_schema(),
                    "invalidation_basis": { "type": ["string", "null"] },
                    "stop_model_hint": { "type": ["string", "null"] },
                    "key_signals": { "type": "string" },
                    "risk_flags": { "type": "string" }
                }
            }
        }
    })
}

fn grok_entry_response_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["analysis", "decision", "params", "self_check", "reason"],
        "properties": {
            "analysis": {
                "type": "object",
                "additionalProperties": false,
                "required": [
                    "volatility_unit_v",
                    "v_calculation",
                    "expected_move_m",
                    "m_over_v",
                    "entry_strategy",
                    "stop_model",
                    "key_signals",
                    "htf_alignment"
                ],
                "properties": {
                    "volatility_unit_v": { "type": ["number", "null"] },
                    "v_calculation": { "type": "string" },
                    "expected_move_m": { "type": ["number", "null"] },
                    "m_over_v": { "type": ["number", "null"] },
                    "entry_strategy": { "type": "string" },
                    "stop_model": { "type": "string" },
                    "key_signals": { "type": "string" },
                    "htf_alignment": { "type": "string" }
                }
            },
            "decision": {
                "type": "string",
                "enum": ["LONG", "SHORT", "NO_TRADE"]
            },
            "params": {
                "type": "object",
                "additionalProperties": true,
                "required": [
                    "entry",
                    "tp",
                    "sl",
                    "leverage",
                    "rr",
                    "horizon",
                    "entry_mode",
                    "sweep_status",
                    "sweep_wick_extreme",
                    "liquidity_near_entry",
                    "swing_logic",
                    "data_provenance"
                ],
                "properties": {
                    "entry": { "type": ["number", "null"] },
                    "tp": { "type": ["number", "null"] },
                    "sl": { "type": ["number", "null"] },
                    "leverage": { "type": ["number", "null"] },
                    "rr": { "type": ["number", "null"] },
                    "horizon": { "type": ["string", "null"] },
                    "entry_mode": { "type": ["string", "null"] },
                    "sweep_status": { "type": ["string", "null"] },
                    "sweep_wick_extreme": { "type": ["number", "null"] },
                    "liquidity_near_entry": { "type": ["string", "null"] },
                    "swing_logic": { "type": ["string", "null"] },
                    "data_provenance": { "type": ["object", "null"] }
                }
            },
            "self_check": {
                "type": "object",
                "additionalProperties": false,
                "required": [
                    "is_sl_below_sweep_wick",
                    "is_htf_aligned",
                    "hard_constraint_violations"
                ],
                "properties": {
                    "is_sl_below_sweep_wick": {
                        "type": ["boolean", "string", "null"]
                    },
                    "is_htf_aligned": { "type": ["boolean", "null"] },
                    "hard_constraint_violations": {
                        "type": "array",
                        "items": { "type": "string" }
                    }
                }
            },
            "reason": { "type": "string" }
        }
    })
}

fn grok_management_response_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["analysis", "decision", "params", "self_check", "reason"],
        "properties": {
            "analysis": {
                "type": "object",
                "additionalProperties": false,
                "required": [
                    "volatility_unit_v",
                    "m_over_v",
                    "active_strategy",
                    "strategy_health",
                    "delta_efficiency",
                    "order_flow_bias",
                    "key_signals",
                    "current_sl_level",
                    "current_tp_level"
                ],
                "properties": {
                    "volatility_unit_v": { "type": ["number", "null"] },
                    "m_over_v": { "type": ["number", "null"] },
                    "active_strategy": { "type": "string" },
                    "strategy_health": { "type": "string" },
                    "delta_efficiency": { "type": "string" },
                    "order_flow_bias": { "type": "string" },
                    "key_signals": { "type": "string" },
                    "current_sl_level": { "type": ["number", "null"] },
                    "current_tp_level": { "type": ["number", "null"] }
                }
            },
            "decision": {
                "type": "string",
                "enum": ["CLOSE", "ADD", "REDUCE", "HOLD", "MODIFY_TPSL"]
            },
            "params": {
                "type": "object",
                "additionalProperties": false,
                "required": [
                    "qty",
                    "qty_ratio",
                    "is_full_exit",
                    "new_sl",
                    "new_tp"
                ],
                "properties": {
                    "qty": { "type": ["number", "null"] },
                    "qty_ratio": { "type": ["number", "null"] },
                    "is_full_exit": { "type": ["boolean", "null"] },
                    "new_sl": { "type": ["number", "null"] },
                    "new_tp": { "type": ["number", "null"] }
                }
            },
            "self_check": {
                "type": "object",
                "additionalProperties": false,
                "required": [
                    "is_stop_model_intact",
                    "is_htf_cvd_aligned",
                    "remaining_rr_after_action",
                    "hard_constraint_violations"
                ],
                "properties": {
                    "is_stop_model_intact": { "type": ["boolean", "null"] },
                    "is_htf_cvd_aligned": { "type": ["boolean", "null"] },
                    "remaining_rr_after_action": { "type": ["number", "null"] },
                    "hard_constraint_violations": {
                        "type": "array",
                        "items": { "type": "string" }
                    }
                }
            },
            "reason": { "type": "string" }
        }
    })
}

fn grok_pending_order_response_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["decision", "params", "reason"],
        "properties": {
            "decision": {
                "type": "string",
                "enum": ["HOLD", "CLOSE", "MODIFY_MAKER"]
            },
            "params": {
                "type": "object",
                "additionalProperties": false,
                "required": ["new_entry", "new_tp", "new_sl"],
                "properties": {
                    "new_entry": { "type": ["number", "null"] },
                    "new_tp": { "type": ["number", "null"] },
                    "new_sl": { "type": ["number", "null"] }
                }
            },
            "analysis": {
                "type": ["object", "null"],
                "additionalProperties": true
            },
            "self_check": {
                "type": ["object", "null"],
                "additionalProperties": true
            },
            "reason": { "type": "string" }
        }
    })
}

// ── Medium/Large Opportunity schemas ─────────────────────────────────────────

fn ml_grok_entry_scan_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["decision", "reason", "scan"],
        "properties": {
            "decision": { "type": "string", "enum": ["LONG", "SHORT", "NO_TRADE"] },
            "reason": { "type": "string" },
            "scan": {
                "type": "object",
                "additionalProperties": false,
                "required": [
                    "primary_strategy", "market_story", "hypothesis", "conviction",
                    "entry_style", "candidate_zone", "entry_ladder", "target_zone", "target_ladder", "stop_ladder", "invalidation"
                ],
                "properties": {
                    "primary_strategy": { "type": "string" },
                    "market_story": { "type": "string" },
                    "hypothesis": { "type": "string" },
                    "conviction": { "type": "string" },
                    "entry_style": { "type": ["string", "null"] },
                    "candidate_zone": { "type": ["string", "null"] },
                    "entry_ladder": scan_ladder_schema(),
                    "target_zone": { "type": ["string", "null"] },
                    "target_ladder": scan_ladder_schema(),
                    "stop_ladder": scan_ladder_schema(),
                    "invalidation": { "type": "string" }
                }
            }
        }
    })
}

fn ml_grok_entry_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["analysis", "decision", "params", "reason"],
        "properties": {
            "analysis": {
                "type": "object",
                "additionalProperties": false,
                "required": [
                    "volatility_unit_v", "v_calculation",
                    "entry_strategy", "stop_model",
                    "market_thesis", "trade_logic",
                    "sl_thesis_check", "sl_noise_check"
                ],
                "properties": {
                    "volatility_unit_v": { "type": ["number", "null"] },
                    "v_calculation": { "type": "string" },
                    "entry_strategy": { "type": "string" },
                    "stop_model": { "type": "string" },
                    "market_thesis": { "type": "string" },
                    "trade_logic": { "type": "string" },
                    "sl_thesis_check": { "type": "string" },
                    "sl_noise_check": { "type": "string" }
                }
            },
            "decision": { "type": "string", "enum": ["LONG", "SHORT", "NO_TRADE"] },
            "params": {
                "type": "object",
                "additionalProperties": false,
                "required": ["entry", "tp", "sl", "leverage", "rr", "horizon", "entry_mode", "sweep_wick_extreme"],
                "properties": {
                    "entry": { "type": ["number", "null"] },
                    "tp": { "type": ["number", "null"] },
                    "sl": { "type": ["number", "null"] },
                    "leverage": { "type": ["number", "null"] },
                    "rr": { "type": ["number", "null"] },
                    "horizon": { "type": ["string", "null"] },
                    "entry_mode": { "type": ["string", "null"] },
                    "sweep_wick_extreme": { "type": ["number", "null"] }
                }
            },
            "reason": { "type": "string" }
        }
    })
}

fn ml_grok_management_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["analysis", "decision", "params", "reason"],
        "properties": {
            "analysis": {
                "type": "object",
                "additionalProperties": false,
                "required": [
                    "volatility_unit_v", "thesis_status",
                    "thesis_evidence", "add_signal", "action_logic"
                ],
                "properties": {
                    "volatility_unit_v": { "type": ["number", "null"] },
                    "thesis_status": { "type": "string" },
                    "thesis_evidence": { "type": "string" },
                    "add_signal": { "type": ["string", "null"] },
                    "action_logic": { "type": "string" }
                }
            },
            "decision": { "type": "string", "enum": ["CLOSE", "ADD", "REDUCE", "HOLD", "MODIFY_TPSL"] },
            "params": {
                "type": "object",
                "additionalProperties": false,
                "required": ["qty", "qty_ratio", "is_full_exit", "new_tp", "new_sl"],
                "properties": {
                    "qty": { "type": ["number", "null"] },
                    "qty_ratio": { "type": ["number", "null"] },
                    "is_full_exit": { "type": ["boolean", "null"] },
                    "new_tp": { "type": ["number", "null"] },
                    "new_sl": { "type": ["number", "null"] }
                }
            },
            "reason": { "type": "string" }
        }
    })
}

fn ml_gemini_entry_scan_schema() -> Value {
    json!({
        "type": "OBJECT",
        "properties": {
            "decision": { "type": "STRING", "enum": ["LONG", "SHORT", "NO_TRADE"] },
            "reason": { "type": "STRING" },
            "scan": {
                "type": "OBJECT",
                "properties": {
                    "primary_strategy": { "type": "STRING" },
                    "market_story": { "type": "STRING" },
                    "hypothesis": { "type": "STRING" },
                    "conviction": { "type": "STRING" },
                    "entry_style": { "type": "STRING", "nullable": true },
                    "candidate_zone": { "type": "STRING", "nullable": true },
                    "entry_ladder": gemini_scan_ladder_schema(),
                    "target_zone": { "type": "STRING", "nullable": true },
                    "target_ladder": gemini_scan_ladder_schema(),
                    "stop_ladder": gemini_scan_ladder_schema(),
                    "invalidation": { "type": "STRING" }
                },
                "required": [
                    "primary_strategy", "market_story", "hypothesis", "conviction",
                    "entry_style", "candidate_zone", "entry_ladder", "target_zone", "target_ladder", "stop_ladder", "invalidation"
                ]
            }
        },
        "required": ["decision", "reason", "scan"]
    })
}

fn ml_gemini_entry_schema() -> Value {
    json!({
        "type": "OBJECT",
        "properties": {
            "analysis": {
                "type": "OBJECT",
                "properties": {
                    "volatility_unit_v": { "type": "NUMBER", "nullable": true },
                    "v_calculation": { "type": "STRING" },
                    "entry_strategy": { "type": "STRING" },
                    "stop_model": { "type": "STRING" },
                    "market_thesis": { "type": "STRING" },
                    "trade_logic": { "type": "STRING" },
                    "sl_thesis_check": { "type": "STRING" },
                    "sl_noise_check": { "type": "STRING" }
                },
                "required": [
                    "volatility_unit_v", "v_calculation",
                    "entry_strategy", "stop_model",
                    "market_thesis", "trade_logic",
                    "sl_thesis_check", "sl_noise_check"
                ]
            },
            "decision": { "type": "STRING", "enum": ["LONG", "SHORT", "NO_TRADE"] },
            "params": {
                "type": "OBJECT",
                "properties": {
                    "entry": { "type": "NUMBER", "nullable": true },
                    "tp": { "type": "NUMBER", "nullable": true },
                    "sl": { "type": "NUMBER", "nullable": true },
                    "leverage": { "type": "NUMBER", "nullable": true },
                    "rr": { "type": "NUMBER", "nullable": true },
                    "horizon": { "type": "STRING", "nullable": true },
                    "entry_mode": { "type": "STRING", "nullable": true },
                    "sweep_wick_extreme": { "type": "NUMBER", "nullable": true }
                },
                "required": ["entry", "tp", "sl", "leverage", "rr", "horizon", "entry_mode", "sweep_wick_extreme"]
            },
            "reason": { "type": "STRING" }
        },
        "required": ["analysis", "decision", "params", "reason"]
    })
}

fn ml_gemini_management_schema() -> Value {
    json!({
        "type": "OBJECT",
        "properties": {
            "analysis": {
                "type": "OBJECT",
                "properties": {
                    "volatility_unit_v": { "type": "NUMBER", "nullable": true },
                    "thesis_status": { "type": "STRING" },
                    "thesis_evidence": { "type": "STRING" },
                    "add_signal": { "type": "STRING", "nullable": true },
                    "action_logic": { "type": "STRING" }
                },
                "required": ["volatility_unit_v", "thesis_status", "thesis_evidence", "add_signal", "action_logic"]
            },
            "decision": { "type": "STRING", "enum": ["CLOSE", "ADD", "REDUCE", "HOLD", "MODIFY_TPSL"] },
            "params": {
                "type": "OBJECT",
                "properties": {
                    "qty": { "type": "NUMBER", "nullable": true },
                    "qty_ratio": { "type": "NUMBER", "nullable": true },
                    "is_full_exit": { "type": "BOOLEAN", "nullable": true },
                    "new_sl": { "type": "NUMBER", "nullable": true },
                    "new_tp": { "type": "NUMBER", "nullable": true }
                },
                "required": ["qty", "qty_ratio", "is_full_exit", "new_sl", "new_tp"]
            },
            "reason": { "type": "STRING" }
        },
        "required": ["analysis", "decision", "params", "reason"]
    })
}

fn ml_qwen_entry_scan_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": true,
        "required": ["decision", "reason", "scan"],
        "properties": {
            "decision": { "type": "string", "enum": ["LONG", "SHORT", "NO_TRADE"] },
            "reason": { "type": "string", "minLength": 1 },
            "scan": {
                "type": "object",
                "additionalProperties": true,
                "required": [
                    "primary_strategy", "market_story", "hypothesis", "conviction",
                    "entry_style", "candidate_zone", "entry_ladder", "target_zone", "target_ladder", "stop_ladder", "invalidation"
                ],
                "properties": {
                    "entry_ladder": scan_ladder_schema(),
                    "target_ladder": scan_ladder_schema(),
                    "stop_ladder": scan_ladder_schema()
                }
            }
        }
    })
}

fn ml_qwen_entry_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": true,
        "required": ["decision", "reason", "params"],
        "properties": {
            "decision": { "type": "string", "enum": ["LONG", "SHORT", "NO_TRADE"] },
            "reason": { "type": "string", "minLength": 1 },
            "params": { "type": "object", "additionalProperties": true },
            "analysis": { "type": ["object", "null"], "additionalProperties": true }
        }
    })
}

fn ml_qwen_management_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": true,
        "required": ["decision", "reason", "params"],
        "properties": {
            "decision": { "type": "string", "enum": ["CLOSE", "ADD", "REDUCE", "HOLD", "MODIFY_TPSL"] },
            "reason": { "type": "string", "minLength": 1 },
            "params": { "type": "object", "additionalProperties": true },
            "analysis": { "type": ["object", "null"], "additionalProperties": true }
        }
    })
}

fn ml_custom_llm_entry_scan_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["decision", "reason", "scan"],
        "properties": {
            "decision": { "type": "string", "enum": ["LONG", "SHORT", "NO_TRADE"] },
            "reason": { "type": "string", "minLength": 1 },
            "scan": {
                "type": "object",
                "additionalProperties": false,
                "required": [
                    "primary_strategy", "market_story", "hypothesis", "conviction",
                    "entry_style", "candidate_zone", "entry_ladder", "target_zone", "target_ladder", "stop_ladder", "invalidation"
                ],
                "properties": {
                    "primary_strategy": { "type": "string" },
                    "market_story": { "type": "string" },
                    "hypothesis": { "type": "string" },
                    "conviction": { "type": "string" },
                    "entry_style": { "type": ["string", "null"] },
                    "candidate_zone": { "type": ["string", "null"] },
                    "entry_ladder": scan_ladder_schema(),
                    "target_zone": { "type": ["string", "null"] },
                    "target_ladder": scan_ladder_schema(),
                    "stop_ladder": scan_ladder_schema(),
                    "invalidation": { "type": "string" }
                }
            }
        }
    })
}

fn ml_custom_llm_entry_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["decision", "reason", "params"],
        "properties": {
            "decision": { "type": "string", "enum": ["LONG", "SHORT", "NO_TRADE"] },
            "reason": { "type": "string", "minLength": 1 },
            "params": {
                "type": "object",
                "additionalProperties": false,
                "required": ["entry", "tp", "sl", "leverage", "rr", "horizon"],
                "properties": {
                    "entry": { "type": ["number", "null"] },
                    "tp": { "type": ["number", "null"] },
                    "sl": { "type": ["number", "null"] },
                    "leverage": { "type": ["number", "null"] },
                    "rr": { "type": ["number", "null"] },
                    "horizon": { "type": ["string", "null"] }
                }
            }
        }
    })
}

fn ml_custom_llm_management_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["decision", "reason", "params"],
        "properties": {
            "decision": { "type": "string", "enum": ["CLOSE", "ADD", "REDUCE", "HOLD", "MODIFY_TPSL"] },
            "reason": { "type": "string", "minLength": 1 },
            "params": {
                "type": "object",
                "additionalProperties": false,
                "required": ["qty", "qty_ratio", "is_full_exit", "new_tp", "new_sl"],
                "properties": {
                    "qty": { "type": ["number", "null"] },
                    "qty_ratio": { "type": ["number", "null"] },
                    "is_full_exit": { "type": ["boolean", "null"] },
                    "new_tp": { "type": ["number", "null"] },
                    "new_sl": { "type": ["number", "null"] }
                }
            }
        }
    })
}

fn gemini_response_schema(
    management_mode: bool,
    pending_order_mode: bool,
    entry_stage: prompt::EntryPromptStage,
    prompt_template: &str,
) -> Value {
    if pending_order_mode {
        gemini_pending_order_response_schema()
    } else if management_mode {
        if is_medium_large(prompt_template) {
            ml_gemini_management_schema()
        } else {
            gemini_management_response_schema()
        }
    } else if matches!(entry_stage, prompt::EntryPromptStage::Scan) {
        if is_medium_large(prompt_template) {
            ml_gemini_entry_scan_schema()
        } else {
            gemini_entry_scan_response_schema()
        }
    } else {
        if is_medium_large(prompt_template) {
            ml_gemini_entry_schema()
        } else {
            gemini_entry_response_schema()
        }
    }
}

fn gemini_entry_scan_response_schema() -> Value {
    json!({
        "type": "OBJECT",
        "properties": {
            "decision": {
                "type": "STRING",
                "enum": ["LONG", "SHORT", "NO_TRADE"]
            },
            "reason": { "type": "STRING" },
            "scan": {
                "type": "OBJECT",
                "properties": {
                    "primary_strategy": { "type": "STRING" },
                    "setup_quality": { "type": "STRING" },
                    "order_flow_bias": { "type": "STRING" },
                    "entry_style": { "type": "STRING", "nullable": true },
                    "candidate_zone": { "type": "STRING", "nullable": true },
                    "entry_ladder": gemini_scan_ladder_schema(),
                    "target_zone": { "type": "STRING", "nullable": true },
                    "target_ladder": gemini_scan_ladder_schema(),
                    "stop_ladder": gemini_scan_ladder_schema(),
                    "invalidation_basis": { "type": "STRING", "nullable": true },
                    "stop_model_hint": { "type": "STRING", "nullable": true },
                    "key_signals": { "type": "STRING" },
                    "risk_flags": { "type": "STRING" }
                },
                "required": [
                    "primary_strategy",
                    "setup_quality",
                    "order_flow_bias",
                    "entry_style",
                    "candidate_zone",
                    "entry_ladder",
                    "target_zone",
                    "target_ladder",
                    "stop_ladder",
                    "invalidation_basis",
                    "stop_model_hint",
                    "key_signals",
                    "risk_flags"
                ]
            }
        },
        "required": ["decision", "reason", "scan"]
    })
}

fn gemini_entry_response_schema() -> Value {
    json!({
        "type": "OBJECT",
        "properties": {
            "analysis": {
                "type": "OBJECT",
                "properties": {
                    "volatility_unit_v": { "type": "NUMBER", "nullable": true },
                    "v_calculation": { "type": "STRING" },
                    "expected_move_m": { "type": "NUMBER", "nullable": true },
                    "m_over_v": { "type": "NUMBER", "nullable": true },
                    "entry_strategy": { "type": "STRING" },
                    "stop_model": { "type": "STRING" },
                    "key_signals": { "type": "STRING" },
                    "htf_alignment": { "type": "STRING" }
                },
                "required": [
                    "volatility_unit_v",
                    "v_calculation",
                    "expected_move_m",
                    "m_over_v",
                    "entry_strategy",
                    "stop_model",
                    "key_signals",
                    "htf_alignment"
                ]
            },
            "decision": {
                "type": "STRING",
                "enum": ["LONG", "SHORT", "NO_TRADE"]
            },
            "params": {
                "type": "OBJECT",
                "properties": {
                    "entry": { "type": "NUMBER", "nullable": true },
                    "tp": { "type": "NUMBER", "nullable": true },
                    "sl": { "type": "NUMBER", "nullable": true },
                    "leverage": { "type": "NUMBER", "nullable": true },
                    "rr": { "type": "NUMBER", "nullable": true },
                    "horizon": { "type": "STRING", "nullable": true },
                    "entry_mode": { "type": "STRING", "nullable": true },
                    "sweep_status": { "type": "STRING", "nullable": true },
                    "sweep_wick_extreme": { "type": "NUMBER", "nullable": true },
                    "liquidity_near_entry": { "type": "STRING", "nullable": true },
                    "swing_logic": { "type": "STRING", "nullable": true },
                    "data_provenance": {
                        "type": "OBJECT",
                        "properties": {
                            "entry_derived_from": { "type": "STRING", "nullable": true },
                            "tp_derived_from": { "type": "STRING", "nullable": true },
                            "v_derived_from": { "type": "STRING", "nullable": true }
                        },
                        "required": ["entry_derived_from", "tp_derived_from", "v_derived_from"]
                    }
                },
                "required": [
                    "entry",
                    "tp",
                    "sl",
                    "leverage",
                    "rr",
                    "horizon",
                    "entry_mode",
                    "sweep_status",
                    "sweep_wick_extreme",
                    "liquidity_near_entry",
                    "swing_logic",
                    "data_provenance"
                ]
            },
            "self_check": {
                "type": "OBJECT",
                "properties": {
                    "is_sl_below_sweep_wick": { "type": "STRING" },
                    "is_htf_aligned": { "type": "BOOLEAN" },
                    "hard_constraint_violations": {
                        "type": "ARRAY",
                        "items": { "type": "STRING" }
                    }
                },
                "required": [
                    "is_sl_below_sweep_wick",
                    "is_htf_aligned",
                    "hard_constraint_violations"
                ]
            },
            "reason": { "type": "STRING" }
        },
        "required": ["analysis", "decision", "params", "self_check", "reason"]
    })
}

fn gemini_management_response_schema() -> Value {
    json!({
        "type": "OBJECT",
        "properties": {
            "analysis": {
                "type": "OBJECT",
                "properties": {
                    "volatility_unit_v": { "type": "NUMBER", "nullable": true },
                    "m_over_v": { "type": "NUMBER", "nullable": true },
                    "active_strategy": { "type": "STRING" },
                    "strategy_health": { "type": "STRING" },
                    "key_signals": { "type": "STRING" },
                    "current_sl_level": { "type": "NUMBER", "nullable": true },
                    "current_tp_level": { "type": "NUMBER", "nullable": true }
                },
                "required": [
                    "volatility_unit_v",
                    "m_over_v",
                    "active_strategy",
                    "strategy_health",
                    "key_signals",
                    "current_sl_level",
                    "current_tp_level"
                ]
            },
            "decision": {
                "type": "STRING",
                "enum": ["CLOSE", "ADD", "REDUCE", "HOLD", "MODIFY_TPSL"]
            },
            "params": {
                "type": "OBJECT",
                "properties": {
                    "qty": { "type": "NUMBER", "nullable": true },
                    "qty_ratio": { "type": "NUMBER", "nullable": true },
                    "is_full_exit": { "type": "BOOLEAN", "nullable": true },
                    "new_sl": { "type": "NUMBER", "nullable": true },
                    "new_tp": { "type": "NUMBER", "nullable": true }
                },
                "required": ["qty", "qty_ratio", "is_full_exit", "new_sl", "new_tp"]
            },
            "self_check": {
                "type": "OBJECT",
                "properties": {
                    "is_stop_model_intact": { "type": "BOOLEAN" },
                    "is_htf_cvd_aligned": { "type": "BOOLEAN" },
                    "remaining_rr_after_action": { "type": "NUMBER", "nullable": true },
                    "hard_constraint_violations": {
                        "type": "ARRAY",
                        "items": { "type": "STRING" }
                    }
                },
                "required": [
                    "is_stop_model_intact",
                    "is_htf_cvd_aligned",
                    "remaining_rr_after_action",
                    "hard_constraint_violations"
                ]
            },
            "reason": { "type": "STRING" }
        },
        "required": ["analysis", "decision", "params", "self_check", "reason"]
    })
}

fn gemini_pending_order_response_schema() -> Value {
    json!({
        "type": "OBJECT",
        "properties": {
            "decision": {
                "type": "STRING",
                "enum": ["HOLD", "CLOSE", "MODIFY_MAKER"]
            },
            "params": {
                "type": "OBJECT",
                "properties": {
                    "new_entry": { "type": "NUMBER", "nullable": true },
                    "new_tp": { "type": "NUMBER", "nullable": true },
                    "new_sl": { "type": "NUMBER", "nullable": true }
                },
                "required": ["new_entry", "new_tp", "new_sl"]
            },
            "analysis": {
                "type": "OBJECT",
                "nullable": true
            },
            "self_check": {
                "type": "OBJECT",
                "nullable": true
            },
            "reason": { "type": "STRING" }
        },
        "required": ["decision", "params", "reason"]
    })
}

async fn invoke_claude(
    http_client: &Client,
    claude_cfg: &ClaudeApiConfig,
    model: &LlmModelConfig,
    input: &ModelInvocationInput,
    prompt_template: &str,
) -> Result<ProviderSuccess, ProviderFailure> {
    if is_entry_mode(input) {
        return invoke_claude_entry_two_stage(
            http_client,
            claude_cfg,
            model,
            input,
            prompt_template,
        )
        .await;
    }
    // Enforce batch mode for lower unit cost and non-real-time decision workflow.
    invoke_claude_batch(
        http_client,
        claude_cfg,
        model,
        input,
        prompt_template,
        prompt::EntryPromptStage::Finalize,
        None,
    )
    .await
}

async fn invoke_claude_entry_two_stage(
    http_client: &Client,
    claude_cfg: &ClaudeApiConfig,
    model: &LlmModelConfig,
    input: &ModelInvocationInput,
    prompt_template: &str,
) -> Result<ProviderSuccess, ProviderFailure> {
    let scan = invoke_claude_batch(
        http_client,
        claude_cfg,
        model,
        input,
        prompt_template,
        prompt::EntryPromptStage::Scan,
        None,
    )
    .await?;
    let scan_value =
        parse_entry_scan_output("claude", &scan.raw_text, prompt_template).map_err(|failure| {
            let mut trace = scan.trace.clone();
            trace.push_entry_stage_event(build_entry_scan_trace_event(
                "claude",
                &scan.raw_text,
                None,
            ));
            ProviderFailure {
                error: failure.error.context("entry scan stage failed"),
                trace,
            }
        })?;
    if let Some(short_circuit) = maybe_short_circuit_entry_finalize("claude", &scan, &scan_value) {
        return Ok(short_circuit);
    }
    let scan_event = build_entry_scan_trace_event("claude", &scan.raw_text, Some(&scan_value));
    let scan_prompt_inputs = scan
        .trace
        .output_entry_stage_prompt_inputs()
        .unwrap_or_default();
    let finalize_event = build_entry_finalize_trace_event(input, &scan_value);
    let finalize = invoke_claude_batch(
        http_client,
        claude_cfg,
        model,
        input,
        prompt_template,
        prompt::EntryPromptStage::Finalize,
        Some(&scan_value),
    )
    .await
    .map_err(|failure| ProviderFailure {
        error: failure.error,
        trace: failure
            .trace
            .prepend_entry_stage_events(&[scan_event.clone(), finalize_event.clone()])
            .prepend_entry_stage_prompt_inputs(&scan_prompt_inputs),
    })?;

    Ok(ProviderSuccess {
        raw_text: finalize.raw_text,
        trace: finalize
            .trace
            .prepend_entry_stage_events(&[scan_event, finalize_event])
            .prepend_entry_stage_prompt_inputs(&scan_prompt_inputs),
    })
}

async fn invoke_claude_batch(
    http_client: &Client,
    claude_cfg: &ClaudeApiConfig,
    model: &LlmModelConfig,
    input: &ModelInvocationInput,
    prompt_template: &str,
    entry_stage: prompt::EntryPromptStage,
    prior_scan: Option<&Value>,
) -> Result<ProviderSuccess, ProviderFailure> {
    let BuiltPromptPair {
        system,
        user,
        prompt_input_capture,
    } = build_prompt_pair(input, prompt_template, entry_stage, prior_scan)?;
    let mut trace = provider_trace_with_prompt_input_capture(prompt_input_capture);
    let system = vec![ClaudeTextBlock::cacheable(system, "1h")];
    let messages = vec![ClaudeInputMessage {
        role: "user".to_string(),
        content: vec![
            ClaudeTextBlock::cacheable(
                prompt::user_prompt_prefix(
                    input.management_mode,
                    input.pending_order_mode,
                    entry_stage,
                )
                .to_string(),
                "1h",
            ),
            ClaudeTextBlock::plain(
                user.trim_start_matches(prompt::user_prompt_prefix(
                    input.management_mode,
                    input.pending_order_mode,
                    entry_stage,
                ))
                .to_string(),
            ),
        ],
    }];
    log_claude_token_stats(http_client, claude_cfg, model, input, &system, &messages).await;
    let params = ClaudeMessageRequest {
        model: model.model.clone(),
        max_tokens: model.max_tokens,
        temperature: model.temperature,
        system,
        messages,
        tools: Some(vec![claude_response_tool(
            input.management_mode,
            input.pending_order_mode,
            entry_stage,
            prompt_template,
        )]),
        tool_choice: Some(ClaudeToolChoice::tool(CLAUDE_DECISION_TOOL_NAME)),
    };
    let custom_id = build_batch_custom_id(input);
    let create_req = ClaudeBatchCreateRequest {
        requests: vec![ClaudeBatchRequest {
            custom_id: custom_id.clone(),
            params,
        }],
    };

    let create_response = http_client
        .post(&claude_cfg.batch_api_url)
        .header("x-api-key", claude_cfg.resolved_api_key())
        .header("anthropic-version", &claude_cfg.api_version)
        .header("anthropic-beta", CLAUDE_EXTENDED_CACHE_TTL_BETA)
        .json(&create_req)
        .send()
        .await
        .context("call claude messages batch create api")
        .map_err(|error| ProviderFailure {
            error,
            trace: trace.clone(),
        })?;

    let status = create_response.status();
    if !status.is_success() {
        let body = create_response.text().await.unwrap_or_default();
        return Err(ProviderFailure {
            error: anyhow!("claude batch create failed status={} body={}", status, body),
            trace,
        });
    }

    let mut batch: ClaudeBatchEnvelope = create_response
        .json()
        .await
        .context("decode claude batch create response body")
        .map_err(|error| ProviderFailure {
            error,
            trace: trace.clone(),
        })?;

    let batch_id = batch.id.clone();
    trace.batch_id = Some(batch_id.clone());
    trace.batch_status = Some(batch.processing_status.clone());
    let started = Instant::now();
    let poll_every = Duration::from_secs(claude_cfg.batch_poll_interval_secs);
    let wait_timeout = Duration::from_secs(claude_cfg.batch_wait_timeout_secs);

    while !batch_is_terminal(&batch) {
        if started.elapsed() >= wait_timeout {
            return Err(ProviderFailure {
                error: anyhow!(
                    "claude batch timed out batch_id={} wait_timeout_secs={}",
                    batch_id,
                    claude_cfg.batch_wait_timeout_secs
                ),
                trace,
            });
        }

        sleep(poll_every).await;
        batch = poll_claude_batch(http_client, claude_cfg, &batch_id)
            .await
            .map_err(|error| ProviderFailure {
                error,
                trace: trace.clone(),
            })?;
        trace.batch_status = Some(batch.processing_status.clone());
    }

    if !batch.processing_status.eq_ignore_ascii_case("ended") {
        return Err(ProviderFailure {
            error: anyhow!(
                "claude batch finished in unexpected status batch_id={} processing_status={}",
                batch.id,
                batch.processing_status
            ),
            trace,
        });
    }

    let results_url = batch
        .results_url
        .as_deref()
        .ok_or_else(|| ProviderFailure {
            error: anyhow!(
                "claude batch ended without results_url batch_id={}",
                batch.id
            ),
            trace: trace.clone(),
        })?;
    let result = fetch_claude_batch_result(http_client, claude_cfg, results_url, &custom_id)
        .await
        .map_err(|error| ProviderFailure {
            error,
            trace: trace.clone(),
        })?;

    match result.result.kind.as_str() {
        "succeeded" => {
            let mut message = result.result.message.ok_or_else(|| ProviderFailure {
                error: anyhow!("claude batch result missing message for successful request"),
                trace: trace.clone(),
            })?;
            log_claude_cache_stats(input, model, &trace, message.usage.as_ref());
            if let Some(stop_reason) = message.stop_reason.clone() {
                trace.provider_finish_reason = Some(stop_reason);
            }

            let tool_input = message
                .content
                .iter()
                .find(|item| {
                    item.kind == "tool_use"
                        && item.name.as_deref() == Some(CLAUDE_DECISION_TOOL_NAME)
                })
                .and_then(|item| item.input.clone())
                .or_else(|| {
                    message
                        .content
                        .iter()
                        .find(|item| item.kind == "tool_use")
                        .and_then(|item| item.input.clone())
                });
            if let Some(value) = tool_input {
                let raw_text = serde_json::to_string(&value).map_err(|err| ProviderFailure {
                    error: anyhow!("serialize claude tool_use input to json failed: {}", err),
                    trace: trace.clone(),
                })?;
                return Ok(ProviderSuccess { raw_text, trace });
            }

            let text = message
                .content
                .drain(..)
                .filter(|item| item.kind == "text")
                .filter_map(|item| item.text)
                .collect::<Vec<_>>()
                .join("\n")
                .trim()
                .to_string();
            if text.is_empty() {
                return Err(ProviderFailure {
                    error: anyhow!("claude batch response text is empty"),
                    trace,
                });
            }
            Ok(ProviderSuccess {
                raw_text: text,
                trace,
            })
        }
        "errored" => Err(ProviderFailure {
            error: anyhow!(
                "claude batch request errored custom_id={} error={}",
                custom_id,
                format_batch_error(result.result.error)
            ),
            trace,
        }),
        "canceled" | "expired" => Err(ProviderFailure {
            error: anyhow!(
                "claude batch request {} custom_id={}",
                result.result.kind,
                custom_id
            ),
            trace,
        }),
        other => Err(ProviderFailure {
            error: anyhow!(
                "unknown claude batch result type={} custom_id={}",
                other,
                custom_id
            ),
            trace,
        }),
    }
}

fn provider_failure_plain(error: anyhow::Error) -> ProviderFailure {
    ProviderFailure {
        error,
        trace: ProviderTrace::default(),
    }
}

async fn log_claude_token_stats(
    http_client: &Client,
    claude_cfg: &ClaudeApiConfig,
    model: &LlmModelConfig,
    input: &ModelInvocationInput,
    system: &[ClaudeTextBlock],
    messages: &[ClaudeInputMessage],
) {
    let prompt_chars = system.iter().map(|block| block.text.len()).sum::<usize>()
        + messages
            .iter()
            .flat_map(|m| m.content.iter())
            .map(|block| block.text.len())
            .sum::<usize>();
    match count_claude_input_tokens(http_client, claude_cfg, model, system, messages).await {
        Ok(input_tokens) => {
            println!(
                "LLM_TOKEN_STATS ts_bucket={} model={} provider=claude model_id={} mode={} input_tokens={} prompt_chars={}",
                input.ts_bucket,
                model.name,
                model.model,
                if claude_cfg.use_batch_api() { "batch" } else { "messages" },
                input_tokens,
                prompt_chars
            );
        }
        Err(err) => {
            warn!(
                model_name = %model.name,
                provider = "claude",
                model = %model.model,
                mode = if claude_cfg.use_batch_api() { "batch" } else { "messages" },
                prompt_chars = prompt_chars,
                error = %format!("{err:#}"),
                "llm token count failed"
            );
        }
    }
}

async fn count_claude_input_tokens(
    http_client: &Client,
    claude_cfg: &ClaudeApiConfig,
    model: &LlmModelConfig,
    system: &[ClaudeTextBlock],
    messages: &[ClaudeInputMessage],
) -> Result<u64> {
    let req = ClaudeCountTokensRequest {
        model: model.model.clone(),
        system: system.to_vec(),
        messages: messages.to_vec(),
    };
    let response = http_client
        .post(claude_count_tokens_api_url(claude_cfg))
        .header("x-api-key", claude_cfg.resolved_api_key())
        .header("anthropic-version", &claude_cfg.api_version)
        .header("anthropic-beta", CLAUDE_EXTENDED_CACHE_TTL_BETA)
        .json(&req)
        .send()
        .await
        .context("call claude count_tokens api")?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!(
            "claude count_tokens failed status={} body={}",
            status,
            body
        ));
    }

    let body: ClaudeCountTokensResponse = response
        .json()
        .await
        .context("decode claude count_tokens response body")?;
    Ok(body.input_tokens)
}

fn claude_count_tokens_api_url(claude_cfg: &ClaudeApiConfig) -> String {
    let base = claude_cfg.api_url.trim_end_matches('/');
    if let Some(prefix) = base.strip_suffix("/messages") {
        format!("{}/messages/count_tokens", prefix)
    } else {
        format!("{}/count_tokens", base)
    }
}

fn log_claude_cache_stats(
    input: &ModelInvocationInput,
    model: &LlmModelConfig,
    trace: &ProviderTrace,
    usage: Option<&ClaudeUsage>,
) {
    let Some(usage) = usage else {
        return;
    };
    let input_tokens = usage.input_tokens.unwrap_or(0);
    let read_tokens = usage.cache_read_input_tokens.unwrap_or(0);
    let create_tokens = usage
        .cache_creation
        .as_ref()
        .and_then(|c| c.ephemeral_1h_input_tokens)
        .or(usage.cache_creation_input_tokens)
        .unwrap_or(0);

    let total_input_tokens = input_tokens + read_tokens + create_tokens;
    let cache_hit_ratio = if total_input_tokens > 0 {
        read_tokens as f64 / total_input_tokens as f64
    } else {
        0.0
    };

    // Estimate relative input-cost savings against no-cache baseline (1.0x input token cost).
    // Assumes 1h cache pricing multipliers: write 2.0x, read 0.1x.
    let effective_factor = if total_input_tokens > 0 {
        (input_tokens as f64 + read_tokens as f64 * 0.1 + create_tokens as f64 * 2.0)
            / total_input_tokens as f64
    } else {
        1.0
    };
    let estimated_saving_ratio = (1.0 - effective_factor).clamp(-10.0, 1.0);

    println!(
        "LLM_CACHE_STATS ts_bucket={} model={} provider=claude model_id={} mode=batch batch_id={} batch_status={} input_tokens={} cache_read_input_tokens={} cache_creation_input_tokens={} total_input_tokens={} cache_hit_ratio={:.4} est_input_cost_saving_ratio={:.4}",
        input.ts_bucket,
        model.name,
        model.model,
        trace.batch_id.as_deref().unwrap_or("-"),
        trace.batch_status.as_deref().unwrap_or("-"),
        input_tokens,
        read_tokens,
        create_tokens,
        total_input_tokens,
        cache_hit_ratio,
        estimated_saving_ratio
    );
}

pub(crate) fn serialize_llm_input_minified(input: &ModelInvocationInput) -> Result<String> {
    let mut value = serde_json::to_value(input).context("serialize llm invocation input value")?;
    reverse_timeseries_newest_first(&mut value);
    inject_precomputed_v(&mut value);
    if !is_entry_mode(input) && !input.pending_order_mode {
        inject_precomputed_management_state(&mut value);
    }
    if is_entry_mode(input) {
        compact_entry_scan_input_in_place(&mut value);
    }
    round_json_floats_in_place(&mut value);
    serde_json::to_string(&value).context("serialize llm invocation input json")
}

/// Compute V = median(high − low) from the 3–5 most-recent CLOSED bars for a given timeframe.
/// Returns (v, basis_string) or None if fewer than 3 closed bars are available.
fn compute_v_for_timeframe(value: &Value, timeframe: &str) -> Option<(f64, String)> {
    // Two possible paths in the serialised JSON:
    //   .indicators.kline_history.payload.intervals[tf].futures.bars
    //   .indicators.kline_history.payload.intervals[tf].markets.futures.bars
    let base = format!("/indicators/kline_history/payload/intervals/{timeframe}");
    let bars = value
        .pointer(&format!("{base}/futures/bars"))
        .or_else(|| value.pointer(&format!("{base}/markets/futures/bars")))?
        .as_array()?;

    // After reverse_timeseries_newest_first, bars[0] is the most-recent bar.
    let closed_ranges: Vec<(String, f64)> = bars
        .iter()
        .filter(|bar| {
            bar.get("is_closed")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
        })
        .filter_map(|bar| {
            let high = bar.get("high").and_then(|v| v.as_f64())?;
            let low = bar.get("low").and_then(|v| v.as_f64())?;
            let open_time = bar
                .get("open_time")
                .and_then(|v| v.as_str())
                .unwrap_or("?")
                .to_string();
            Some((open_time, high - low))
        })
        .take(5)
        .collect();

    if closed_ranges.len() < 3 {
        return None;
    }

    let mut sorted: Vec<f64> = closed_ranges.iter().map(|(_, r)| *r).collect();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let median = sorted[sorted.len() / 2];

    let bars_desc = closed_ranges
        .iter()
        .map(|(t, r)| format!("{}:{:.4}", t, r))
        .collect::<Vec<_>>()
        .join(",");
    let basis = format!(
        "tf={}; n={}; bars=[{}]; sorted=[{}]; median={:.4}",
        timeframe,
        closed_ranges.len(),
        bars_desc,
        sorted
            .iter()
            .map(|r| format!("{:.4}", r))
            .collect::<Vec<_>>()
            .join(","),
        median
    );

    Some((median, basis))
}

/// Inject pre-computed V values into the indicators object so the model never needs to
/// derive V from raw bars itself.
fn inject_precomputed_v(value: &mut Value) {
    let v_4h = compute_v_for_timeframe(value, "4h");
    let v_1d = compute_v_for_timeframe(value, "1d");

    let status = match (v_4h.is_some(), v_1d.is_some()) {
        (true, true) => "ok",
        (true, false) => "v_4h_only",
        (false, true) => "v_1d_only",
        (false, false) => "unavailable",
    };

    let mut pre_computed_v = Map::new();
    pre_computed_v.insert("status".to_string(), Value::String(status.to_string()));
    if let Some((v, basis)) = v_4h {
        pre_computed_v.insert("v_4h".to_string(), json!(v));
        pre_computed_v.insert("v_4h_basis".to_string(), Value::String(basis));
    }
    if let Some((v, basis)) = v_1d {
        pre_computed_v.insert("v_1d".to_string(), json!(v));
        pre_computed_v.insert("v_1d_basis".to_string(), Value::String(basis));
    }

    if let Some(indicators) = value
        .pointer_mut("/indicators")
        .and_then(|v| v.as_object_mut())
    {
        indicators.insert("pre_computed_v".to_string(), Value::Object(pre_computed_v));
    }
}

/// Pre-compute all management-mode derived metrics so the model can read one flat object
/// instead of excavating nested JSON and doing arithmetic itself.
fn inject_precomputed_management_state(value: &mut Value) {
    // ── helpers ──────────────────────────────────────────────────────────────
    let f64_at = |ptr: &str| -> Option<f64> { value.pointer(ptr).and_then(|v| v.as_f64()) };
    let str_at = |ptr: &str| -> Option<String> {
        value
            .pointer(ptr)
            .and_then(|v| v.as_str())
            .map(String::from)
    };

    // ── source values ────────────────────────────────────────────────────────
    // Prices
    let mark_price = f64_at("/management_snapshot/positions/0/mark_price")
        .or_else(|| f64_at("/indicators/avwap/payload/fut_mark_price"));
    let last_price = f64_at("/indicators/avwap/payload/fut_last_price");
    let entry_price = f64_at("/management_snapshot/positions/0/entry_price");
    let current_sl = f64_at("/management_snapshot/positions/0/current_sl_price");
    let current_tp = f64_at("/management_snapshot/positions/0/current_tp_price");
    let effective_sl = f64_at("/management_snapshot/position_context/effective_stop_loss");
    let effective_tp = f64_at("/management_snapshot/position_context/effective_take_profit");
    let unrealized_pnl = f64_at("/management_snapshot/positions/0/unrealized_pnl");
    let current_pct = f64_at("/management_snapshot/position_context/current_pct_of_original");

    // Direction: "LONG" | "SHORT"
    let direction = str_at("/management_snapshot/positions/0/direction");
    let is_long = direction.as_deref() == Some("LONG");

    // Value area (from pvs)
    let val = f64_at("/indicators/price_volume_structure/payload/val");
    let vah = f64_at("/indicators/price_volume_structure/payload/vah");
    let poc = f64_at("/indicators/price_volume_structure/payload/poc_price");
    let va_width = val.zip(vah).map(|(l, h)| (h - l).abs());

    // Entry context
    let entry_v = f64_at("/management_snapshot/position_context/entry_context/entry_v");
    let entry_strategy =
        str_at("/management_snapshot/position_context/entry_context/entry_strategy");

    // V: priority — entry_v → v_1d → v_4h
    let v_1d = f64_at("/indicators/pre_computed_v/v_1d");
    let v_4h = f64_at("/indicators/pre_computed_v/v_4h");
    let (v, v_source) = if let Some(ev) = entry_v {
        (Some(ev), "entry_v")
    } else if let Some(v1) = v_1d {
        (Some(v1), "pre_computed_v.v_1d")
    } else if let Some(v4) = v_4h {
        (Some(v4), "pre_computed_v.v_4h")
    } else {
        (None, "unavailable")
    };

    // Operative SL: live exchange SL first, fallback to effective_stop_loss context
    let sl_operative = current_sl.or(effective_sl);
    // Operative TP: max of live TP and effective TP (effective_tp from context)
    let tp_operative = match (current_tp, effective_tp) {
        (Some(a), Some(b)) => Some(if is_long { a.max(b) } else { a.min(b) }),
        (a, b) => a.or(b),
    };

    // ── derived metrics ──────────────────────────────────────────────────────
    let (remaining_buffer, original_risk, buffer_pct, risk_state) =
        match (mark_price, entry_price, sl_operative) {
            (Some(mp), Some(ep), Some(sl)) => {
                let buf = (mp - sl).abs();
                let risk = (ep - sl).abs();
                let pct = if risk > 0.0 {
                    buf / risk * 100.0
                } else {
                    f64::INFINITY
                };
                let state = if pct >= 60.0 {
                    "safe"
                } else if pct >= 25.0 {
                    "exposed"
                } else {
                    "critical"
                };
                (Some(buf), Some(risk), Some(pct), state)
            }
            _ => (None, None, None, "unprotected"),
        };

    let profit_in_v = v.zip(mark_price).zip(entry_price).map(|((v_val, mp), ep)| {
        let raw_profit = if is_long { mp - ep } else { ep - mp };
        raw_profit / v_val
    });

    // SL trail trigger prices (in trade direction)
    let sl_trail_1_5v = v.zip(entry_price).map(|(v_val, ep)| {
        if is_long {
            ep + 1.5 * v_val
        } else {
            ep - 1.5 * v_val
        }
    });
    let sl_trail_2_5v = v.zip(entry_price).map(|(v_val, ep)| {
        if is_long {
            ep + 2.5 * v_val
        } else {
            ep - 2.5 * v_val
        }
    });

    // M: distance from current mark to operative TP, in V units
    let m_in_v = v
        .zip(mark_price)
        .zip(tp_operative)
        .map(|((v_val, mp), tp)| (tp - mp).abs() / v_val);
    let add_gate = m_in_v.map(|m| {
        if m < 1.0 {
            "blocked (<1.0V)"
        } else if m < 1.3 {
            "marginal (1.0–1.3V, needs 2 named conditions)"
        } else {
            "clear (≥1.3V)"
        }
    });

    // ── assemble object ──────────────────────────────────────────────────────
    let mut obj = Map::new();
    let insert_f = |obj: &mut Map<String, Value>, k: &str, v: Option<f64>| {
        obj.insert(k.to_string(), v.map(|x| json!(x)).unwrap_or(Value::Null));
    };
    let insert_s = |obj: &mut Map<String, Value>, k: &str, v: Option<String>| {
        obj.insert(k.to_string(), v.map(Value::String).unwrap_or(Value::Null));
    };

    insert_f(&mut obj, "mark_price", mark_price);
    insert_f(&mut obj, "last_price", last_price);
    insert_f(&mut obj, "val", val);
    insert_f(&mut obj, "vah", vah);
    insert_f(&mut obj, "poc", poc);
    insert_f(&mut obj, "va_width", va_width);
    insert_s(&mut obj, "direction", direction);
    insert_f(&mut obj, "entry_price", entry_price);
    insert_f(&mut obj, "current_tp_price", current_tp);
    insert_f(&mut obj, "current_sl_price", current_sl);
    insert_f(&mut obj, "sl_effective", sl_operative);
    insert_f(&mut obj, "unrealized_pnl", unrealized_pnl);
    insert_f(&mut obj, "current_pct_of_original", current_pct);
    insert_s(&mut obj, "entry_strategy", entry_strategy);
    insert_f(&mut obj, "entry_v", entry_v);
    obj.insert(
        "risk_state".to_string(),
        Value::String(risk_state.to_string()),
    );
    insert_f(&mut obj, "buffer_pct", buffer_pct);
    insert_f(
        &mut obj,
        "buffer_in_v",
        remaining_buffer.zip(v).map(|(b, v_val)| b / v_val),
    );
    insert_f(&mut obj, "remaining_buffer", remaining_buffer);
    insert_f(&mut obj, "original_risk", original_risk);
    insert_f(&mut obj, "V", v);
    obj.insert("V_source".to_string(), Value::String(v_source.to_string()));
    insert_f(&mut obj, "profit_in_v", profit_in_v);
    insert_f(&mut obj, "sl_trail_1_5v_price", sl_trail_1_5v);
    insert_f(&mut obj, "sl_trail_2_5v_price", sl_trail_2_5v);
    insert_f(&mut obj, "M_current_in_v", m_in_v);
    obj.insert(
        "add_gate_status".to_string(),
        add_gate
            .map(|s| Value::String(s.to_string()))
            .unwrap_or(Value::Null),
    );

    if let Some(indicators) = value
        .pointer_mut("/indicators")
        .and_then(|v| v.as_object_mut())
    {
        indicators.insert(
            "pre_computed_management_state".to_string(),
            Value::Object(obj),
        );
    }
}

fn compact_entry_scan_input_in_place(value: &mut Value) {
    let Some(indicators) = value
        .pointer_mut("/indicators")
        .and_then(|v| v.as_object_mut())
    else {
        return;
    };

    for (code, indicator) in indicators.iter_mut() {
        match code.as_str() {
            "funding_rate" => compact_funding_rate_indicator(indicator),
            "kline_history" => compact_kline_history_indicator(indicator),
            "divergence" => compact_divergence_indicator(indicator),
            "absorption" => compact_recent_history_indicator(indicator),
            "buying_exhaustion" => compact_recent_history_indicator(indicator),
            "selling_exhaustion" => compact_recent_history_indicator(indicator),
            "initiation" => compact_recent_history_indicator(indicator),
            "avwap" => compact_avwap_indicator(indicator),
            "cvd_pack" => compact_cvd_pack_indicator(indicator),
            "footprint" => compact_footprint_indicator(indicator),
            "price_volume_structure" => compact_price_volume_structure_indicator(indicator),
            _ => {}
        }
    }
}

fn compact_funding_rate_indicator(indicator: &mut Value) {
    let Some(payload) = indicator.get("payload").and_then(|v| v.as_object()) else {
        return;
    };

    let mut compacted = Map::new();
    insert_present_fields(
        &mut compacted,
        payload,
        &[
            "funding_current",
            "funding_current_effective_ts",
            "funding_twa",
            "mark_price_last",
            "mark_price_last_ts",
            "mark_price_twap",
            "change_count",
        ],
    );

    if let Some(changes) = payload.get("changes").and_then(|v| v.as_array()) {
        compacted.insert(
            "changes".to_string(),
            Value::Array(take_last_n_newest_first(
                changes,
                ENTRY_SCAN_MAX_FUNDING_CHANGES,
            )),
        );
    }

    if let Some(by_window) = payload.get("by_window").and_then(|v| v.as_object()) {
        let mut compact_windows = Map::new();
        for (window_name, window_value) in by_window {
            let Some(window_obj) = window_value.as_object() else {
                continue;
            };
            let mut compact_window = Map::new();
            for (key, value) in window_obj {
                if key == "changes_recent" || key == "changes_bucketed" {
                    continue;
                }
                if !value.is_array() && !value.is_object() {
                    compact_window.insert(key.clone(), value.clone());
                }
            }
            if let Some(changes_recent) =
                window_obj.get("changes_recent").and_then(|v| v.as_array())
            {
                compact_window.insert(
                    "changes_recent".to_string(),
                    Value::Array(take_first_n(changes_recent, ENTRY_SCAN_MAX_FUNDING_CHANGES)),
                );
            }
            compact_windows.insert(window_name.clone(), Value::Object(compact_window));
        }
        compacted.insert("by_window".to_string(), Value::Object(compact_windows));
    }

    if let Some(obj) = indicator.as_object_mut() {
        obj.insert("payload".to_string(), Value::Object(compacted));
    }
}

fn compact_kline_history_indicator(indicator: &mut Value) {
    let Some(payload) = indicator.get("payload").and_then(|v| v.as_object()) else {
        return;
    };

    let mut compacted = Map::new();
    insert_present_fields(
        &mut compacted,
        payload,
        &["as_of_ts", "indicator", "window"],
    );

    if let Some(intervals) = payload.get("intervals").and_then(|v| v.as_object()) {
        let mut compact_intervals = Map::new();
        for (interval_name, interval_value) in intervals {
            let Some(interval_obj) = interval_value.as_object() else {
                continue;
            };

            let mut compact_interval = Map::new();
            if let Some(futures_obj) = interval_obj.get("futures").and_then(|v| v.as_object()) {
                let mut compact_futures = Map::new();
                if let Some(bars) = futures_obj.get("bars").and_then(|v| v.as_array()) {
                    compact_futures.insert(
                        "bars".to_string(),
                        Value::Array(
                            bars.iter()
                                .take(ENTRY_SCAN_MAX_BARS_PER_INTERVAL)
                                .filter_map(|bar| bar.as_object())
                                .map(compact_kline_bar)
                                .map(Value::Object)
                                .collect(),
                        ),
                    );
                }
                if !compact_futures.is_empty() {
                    compact_interval.insert("futures".to_string(), Value::Object(compact_futures));
                }
            } else if let Some(markets_obj) =
                interval_obj.get("markets").and_then(|v| v.as_object())
            {
                if let Some(futures_obj) = markets_obj.get("futures").and_then(|v| v.as_object()) {
                    let mut compact_futures = Map::new();
                    if let Some(bars) = futures_obj.get("bars").and_then(|v| v.as_array()) {
                        compact_futures.insert(
                            "bars".to_string(),
                            Value::Array(
                                bars.iter()
                                    .take(ENTRY_SCAN_MAX_BARS_PER_INTERVAL)
                                    .filter_map(|bar| bar.as_object())
                                    .map(compact_kline_bar)
                                    .map(Value::Object)
                                    .collect(),
                            ),
                        );
                    }
                    if !compact_futures.is_empty() {
                        compact_interval
                            .insert("futures".to_string(), Value::Object(compact_futures));
                    }
                }
            }

            if let Some(spot_gap) = interval_obj.get("spot_gap") {
                compact_interval.insert("spot_gap".to_string(), spot_gap.clone());
            }

            if !compact_interval.is_empty() {
                compact_intervals.insert(interval_name.clone(), Value::Object(compact_interval));
            }
        }
        compacted.insert("intervals".to_string(), Value::Object(compact_intervals));
    }

    if let Some(obj) = indicator.as_object_mut() {
        obj.insert("payload".to_string(), Value::Object(compacted));
    }
}

fn compact_kline_bar(bar: &Map<String, Value>) -> Map<String, Value> {
    let mut compact = Map::new();
    insert_present_fields(
        &mut compact,
        bar,
        &[
            "open_time",
            "is_closed",
            "open",
            "high",
            "low",
            "close",
            "volume_base",
        ],
    );
    compact
}

fn compact_divergence_indicator(indicator: &mut Value) {
    let Some(payload) = indicator.get("payload").and_then(|v| v.as_object()) else {
        return;
    };

    let mut compacted = Map::new();
    insert_present_fields(
        &mut compacted,
        payload,
        &[
            "signal",
            "signals",
            "reason",
            "divergence_type",
            "fut_divergence_sign",
            "likely_driver",
            "pivot_side",
            "spot_lead_score",
            "spot_price_flow_confirm",
            "event_count",
            "latest",
            "latest_7d",
        ],
    );

    if let Some(recent) = payload.get("recent_7d").and_then(|v| v.as_object()) {
        let mut compact_recent = Map::new();
        insert_present_fields(
            &mut compact_recent,
            recent,
            &[
                "event_count",
                "history_source",
                "lookback_coverage_ratio",
                "lookback_covered_minutes",
                "lookback_missing_minutes",
                "lookback_requested_minutes",
            ],
        );
        if let Some(events) = recent.get("events").and_then(|v| v.as_array()) {
            compact_recent.insert(
                "events".to_string(),
                Value::Array(take_last_n_newest_first(
                    events,
                    ENTRY_SCAN_MAX_RECENT_EVENTS,
                )),
            );
            compact_recent.insert("summary".to_string(), summarize_divergence_events(events));
        }
        compacted.insert("recent_7d".to_string(), Value::Object(compact_recent));
    }

    if let Some(obj) = indicator.as_object_mut() {
        obj.insert("payload".to_string(), Value::Object(compacted));
    }
}

fn compact_recent_history_indicator(indicator: &mut Value) {
    let Some(payload) = indicator.get("payload").and_then(|v| v.as_object()) else {
        return;
    };

    let mut compacted = Map::new();
    insert_present_fields(
        &mut compacted,
        payload,
        &["event_count", "events", "latest", "latest_7d"],
    );

    if let Some(recent) = payload.get("recent_7d").and_then(|v| v.as_object()) {
        let mut compact_recent = Map::new();
        insert_present_fields(
            &mut compact_recent,
            recent,
            &[
                "event_count",
                "history_source",
                "lookback_coverage_ratio",
                "lookback_covered_minutes",
                "lookback_missing_minutes",
                "lookback_requested_minutes",
            ],
        );
        if let Some(events) = recent.get("events").and_then(|v| v.as_array()) {
            compact_recent.insert(
                "events".to_string(),
                Value::Array(take_last_n_newest_first(
                    events,
                    ENTRY_SCAN_MAX_RECENT_EVENTS,
                )),
            );
            compact_recent.insert("summary".to_string(), summarize_scored_events(events));
        }
        compacted.insert("recent_7d".to_string(), Value::Object(compact_recent));
    }

    if let Some(obj) = indicator.as_object_mut() {
        obj.insert("payload".to_string(), Value::Object(compacted));
    }
}

fn compact_avwap_indicator(indicator: &mut Value) {
    let Some(payload) = indicator.get("payload").and_then(|v| v.as_object()) else {
        return;
    };

    let mut compacted = Map::new();
    insert_present_fields(
        &mut compacted,
        payload,
        &[
            "anchor_ts",
            "avwap_fut",
            "avwap_spot",
            "fut_last_price",
            "fut_mark_price",
            "indicator",
            "lookback",
            "price_minus_avwap_fut",
            "price_minus_spot_avwap_fut",
            "price_minus_spot_avwap_futmark",
            "window",
            "xmk_avwap_gap_f_minus_s",
            "zavwap_gap",
        ],
    );

    if let Some(series_by_window) = payload.get("series_by_window").and_then(|v| v.as_object()) {
        let mut compact_series_by_window = Map::new();
        for (window_name, series_value) in series_by_window {
            let Some(series) = series_value.as_array() else {
                continue;
            };
            let limit = if window_name == "15m" {
                ENTRY_SCAN_MAX_AVWAP_15M_POINTS
            } else {
                series.len()
            };
            compact_series_by_window.insert(
                window_name.clone(),
                Value::Array(take_first_n(series, limit)),
            );
        }
        compacted.insert(
            "series_by_window".to_string(),
            Value::Object(compact_series_by_window),
        );
    }

    if let Some(obj) = indicator.as_object_mut() {
        obj.insert("payload".to_string(), Value::Object(compacted));
    }
}

fn compact_cvd_pack_indicator(indicator: &mut Value) {
    let Some(payload) = indicator.get("payload").and_then(|v| v.as_object()) else {
        return;
    };

    let mut compacted = Map::new();
    insert_present_fields(
        &mut compacted,
        payload,
        &[
            "cvd_slope_fut",
            "cvd_slope_spot",
            "delta_fut",
            "delta_spot",
            "likely_driver",
            "relative_delta_fut",
            "relative_delta_spot",
            "spot_flow_dominance",
            "spot_lead_score",
            "xmk_delta_gap_s_minus_f",
        ],
    );

    if let Some(by_window) = payload.get("by_window").and_then(|v| v.as_object()) {
        let mut compact_windows = Map::new();
        for (window_name, window_value) in by_window {
            let Some(window_obj) = window_value.as_object() else {
                continue;
            };
            let mut compact_window = Map::new();
            for (key, value) in window_obj {
                if key == "series" {
                    continue;
                }
                compact_window.insert(key.clone(), value.clone());
            }
            if let Some(series) = window_obj.get("series").and_then(|v| v.as_array()) {
                let limit = if window_name == "15m" {
                    ENTRY_SCAN_MAX_CVD_15M_POINTS
                } else {
                    series.len()
                };
                compact_window.insert(
                    "series".to_string(),
                    Value::Array(take_first_n(series, limit)),
                );
            }
            compact_windows.insert(window_name.clone(), Value::Object(compact_window));
        }
        compacted.insert("by_window".to_string(), Value::Object(compact_windows));
    }

    if let Some(obj) = indicator.as_object_mut() {
        obj.insert("payload".to_string(), Value::Object(compacted));
    }
}

fn compact_footprint_indicator(indicator: &mut Value) {
    let Some(payload) = indicator.get("payload").and_then(|v| v.as_object()) else {
        return;
    };

    let mut compacted = payload.clone();
    if let Some(by_window) = payload.get("by_window").and_then(|v| v.as_object()) {
        let mut compact_windows = by_window.clone();
        if let Some(window_4h) = by_window.get("4h").and_then(|v| v.as_object()) {
            compact_windows.insert(
                "4h".to_string(),
                Value::Object(compact_footprint_window(window_4h)),
            );
        }
        compacted.insert("by_window".to_string(), Value::Object(compact_windows));
    }

    if let Some(obj) = indicator.as_object_mut() {
        obj.insert("payload".to_string(), Value::Object(compacted));
    }
}

fn compact_footprint_window(window_obj: &Map<String, Value>) -> Map<String, Value> {
    let mut compact = Map::new();
    insert_present_fields(
        &mut compact,
        window_obj,
        &[
            "buy_imbalance_count",
            "buy_imbalance_zone_count",
            "buy_imbalance_zone_nearest_above",
            "buy_imbalance_zone_nearest_below",
            "buy_stack_count",
            "levels_total",
            "max_buy_stack_len",
            "max_sell_stack_len",
            "sell_imbalance_count",
            "sell_imbalance_zone_count",
            "sell_imbalance_zone_nearest_above",
            "sell_imbalance_zone_nearest_below",
            "sell_stack_count",
            "stacked_buy",
            "stacked_sell",
            "ua_bottom",
            "ua_top",
            "unfinished_auction",
            "window_delta",
            "window_total_qty",
        ],
    );

    insert_truncated_array_field(
        &mut compact,
        window_obj,
        "buy_imbalance_zones_top_strength",
        ENTRY_SCAN_MAX_FOOTPRINT_TOP_STRENGTH,
    );
    insert_truncated_array_field(
        &mut compact,
        window_obj,
        "sell_imbalance_zones_top_strength",
        ENTRY_SCAN_MAX_FOOTPRINT_TOP_STRENGTH,
    );
    insert_truncated_array_field(
        &mut compact,
        window_obj,
        "top_levels_by_abs_delta",
        ENTRY_SCAN_MAX_FOOTPRINT_TOP_LEVELS,
    );
    insert_truncated_array_field(
        &mut compact,
        window_obj,
        "top_levels_by_total",
        ENTRY_SCAN_MAX_FOOTPRINT_TOP_LEVELS,
    );

    compact
}

fn compact_price_volume_structure_indicator(indicator: &mut Value) {
    let Some(payload) = indicator.get("payload").and_then(|v| v.as_object()) else {
        return;
    };

    let mut compacted = payload.clone();
    if let Some(by_window) = payload.get("by_window").and_then(|v| v.as_object()) {
        let mut compact_windows = by_window.clone();
        if let Some(window_4h) = by_window.get("4h").and_then(|v| v.as_object()) {
            compact_windows.insert(
                "4h".to_string(),
                Value::Object(compact_price_volume_window(window_4h)),
            );
        }
        compacted.insert("by_window".to_string(), Value::Object(compact_windows));
    }

    if let Some(obj) = indicator.as_object_mut() {
        obj.insert("payload".to_string(), Value::Object(compacted));
    }
}

fn compact_price_volume_window(window_obj: &Map<String, Value>) -> Map<String, Value> {
    let mut compact = Map::new();
    insert_present_fields(
        &mut compact,
        window_obj,
        &[
            "bar_volume",
            "levels_total",
            "poc_price",
            "poc_volume",
            "vah",
            "val",
            "value_area_level_count",
            "volume_dryup",
            "volume_zscore",
            "window_bars_used",
        ],
    );
    insert_truncated_array_field(
        &mut compact,
        window_obj,
        "hvn_levels",
        ENTRY_SCAN_MAX_PVS_HVN_LEVELS,
    );
    insert_truncated_array_field(
        &mut compact,
        window_obj,
        "lvn_levels",
        ENTRY_SCAN_MAX_PVS_LVN_LEVELS,
    );
    insert_truncated_array_field(
        &mut compact,
        window_obj,
        "top_abs_delta_levels",
        ENTRY_SCAN_MAX_PVS_TOP_LEVELS,
    );
    insert_truncated_array_field(
        &mut compact,
        window_obj,
        "top_value_area_levels",
        ENTRY_SCAN_MAX_PVS_TOP_LEVELS,
    );
    insert_truncated_array_field(
        &mut compact,
        window_obj,
        "top_volume_levels",
        ENTRY_SCAN_MAX_PVS_TOP_LEVELS,
    );
    compact
}

fn insert_present_fields(
    target: &mut Map<String, Value>,
    source: &Map<String, Value>,
    keys: &[&str],
) {
    for key in keys {
        if let Some(value) = source.get(*key) {
            target.insert((*key).to_string(), value.clone());
        }
    }
}

fn insert_truncated_array_field(
    target: &mut Map<String, Value>,
    source: &Map<String, Value>,
    key: &str,
    limit: usize,
) {
    if let Some(values) = source.get(key).and_then(|v| v.as_array()) {
        target.insert(key.to_string(), Value::Array(take_first_n(values, limit)));
    }
}

fn take_first_n(values: &[Value], limit: usize) -> Vec<Value> {
    values.iter().take(limit).cloned().collect()
}

fn take_last_n_newest_first(values: &[Value], limit: usize) -> Vec<Value> {
    values.iter().rev().take(limit).cloned().collect()
}

fn summarize_divergence_events(events: &[Value]) -> Value {
    let mut bullish = 0;
    let mut bearish = 0;
    let mut hidden_bullish = 0;
    let mut hidden_bearish = 0;
    let mut confirmed = 0;
    let mut score_sum = 0.0;
    let mut score_count = 0usize;
    let mut max_score: Option<f64> = None;
    let mut last_confirm_ts: Option<String> = None;
    let mut mixed = 0;
    let mut spot = 0;
    let mut fut = 0;

    for event in events.iter().filter_map(Value::as_object) {
        match event.get("type").and_then(Value::as_str) {
            Some("bullish_divergence") => bullish += 1,
            Some("bearish_divergence") => bearish += 1,
            Some("hidden_bullish_divergence") => hidden_bullish += 1,
            Some("hidden_bearish_divergence") => hidden_bearish += 1,
            _ => {}
        }
        if event
            .get("sig_pass")
            .and_then(Value::as_bool)
            .unwrap_or(false)
            && event
                .get("score")
                .and_then(Value::as_f64)
                .map(|score| score > 0.5)
                .unwrap_or(false)
        {
            confirmed += 1;
            if let Some(ts) = event
                .get("confirm_ts")
                .or_else(|| event.get("event_end_ts"))
                .and_then(Value::as_str)
            {
                last_confirm_ts = Some(ts.to_string());
            }
        }
        if let Some(score) = event.get("score").and_then(Value::as_f64) {
            score_sum += score;
            score_count += 1;
            max_score = Some(max_score.map_or(score, |current| current.max(score)));
        }
        match event.get("likely_driver").and_then(Value::as_str) {
            Some("mixed") => mixed += 1,
            Some("spot") => spot += 1,
            Some("futures") => fut += 1,
            _ => {}
        }
    }

    let dominant_driver = if mixed >= spot && mixed >= fut {
        "mixed"
    } else if spot >= fut {
        "spot"
    } else {
        "futures"
    };

    json!({
        "bullish_count": bullish,
        "bearish_count": bearish,
        "hidden_bullish_count": hidden_bullish,
        "hidden_bearish_count": hidden_bearish,
        "confirmed_count": confirmed,
        "max_score": max_score,
        "avg_score": if score_count == 0 { None } else { Some(score_sum / score_count as f64) },
        "last_confirm_ts": last_confirm_ts,
        "dominant_driver": dominant_driver,
    })
}

fn summarize_scored_events(events: &[Value]) -> Value {
    let mut confirmed = 0;
    let mut positive_direction = 0;
    let mut negative_direction = 0;
    let mut score_sum = 0.0;
    let mut score_count = 0usize;
    let mut max_score: Option<f64> = None;
    let mut last_confirm_ts: Option<String> = None;

    for event in events.iter().filter_map(Value::as_object) {
        match event.get("direction").and_then(Value::as_i64) {
            Some(dir) if dir > 0 => positive_direction += 1,
            Some(dir) if dir < 0 => negative_direction += 1,
            _ => {}
        }
        if event
            .get("sig_pass")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            confirmed += 1;
            if let Some(ts) = event
                .get("confirm_ts")
                .or_else(|| event.get("event_end_ts"))
                .and_then(Value::as_str)
            {
                last_confirm_ts = Some(ts.to_string());
            }
        }
        if let Some(score) = event.get("score").and_then(Value::as_f64) {
            score_sum += score;
            score_count += 1;
            max_score = Some(max_score.map_or(score, |current| current.max(score)));
        }
    }

    json!({
        "positive_direction_count": positive_direction,
        "negative_direction_count": negative_direction,
        "confirmed_count": confirmed,
        "max_score": max_score,
        "avg_score": if score_count == 0 { None } else { Some(score_sum / score_count as f64) },
        "last_confirm_ts": last_confirm_ts,
    })
}

/// Reverse all time-ordered arrays in the LLM input so index 0 is always the most recent entry.
/// Only the serialization layer is modified — DB storage is unchanged.
///
/// Arrays reversed:
///   kline_history.payload.intervals.*.markets.*.bars          (legacy shape)
///   kline_history.payload.intervals.*.futures.bars            (compacted shape)
///   avwap.payload.series_by_window.*                          (ts oldest→newest)
///   cvd_pack.payload.by_window.*.series                       (ts oldest→newest)
///   funding_rate.payload.by_window.*.changes                  (change_ts oldest→newest)
fn reverse_timeseries_newest_first(value: &mut Value) {
    // kline_history bars:
    // 1) legacy shape: intervals -> markets -> {futures|spot} -> bars[]
    // 2) compacted shape: intervals -> futures -> bars[]
    if let Some(intervals) = value
        .pointer_mut("/indicators/kline_history/payload/intervals")
        .and_then(|v| v.as_object_mut())
    {
        for interval in intervals.values_mut() {
            if let Some(markets) = interval.get_mut("markets").and_then(|v| v.as_object_mut()) {
                for market in markets.values_mut() {
                    if let Some(bars) = market.get_mut("bars").and_then(|v| v.as_array_mut()) {
                        bars.reverse();
                    }
                }
            }
            if let Some(futures) = interval.get_mut("futures").and_then(|v| v.as_object_mut()) {
                if let Some(bars) = futures.get_mut("bars").and_then(|v| v.as_array_mut()) {
                    bars.reverse();
                }
            }
            if let Some(spot) = interval.get_mut("spot").and_then(|v| v.as_object_mut()) {
                if let Some(bars) = spot.get_mut("bars").and_then(|v| v.as_array_mut()) {
                    bars.reverse();
                }
            }
        }
    }

    // avwap series_by_window: each window value is a direct array
    if let Some(windows) = value
        .pointer_mut("/indicators/avwap/payload/series_by_window")
        .and_then(|v| v.as_object_mut())
    {
        for series in windows.values_mut() {
            if let Some(arr) = series.as_array_mut() {
                arr.reverse();
            }
        }
    }

    // cvd_pack by_window: each window -> series[]
    if let Some(windows) = value
        .pointer_mut("/indicators/cvd_pack/payload/by_window")
        .and_then(|v| v.as_object_mut())
    {
        for window in windows.values_mut() {
            if let Some(series) = window.get_mut("series").and_then(|v| v.as_array_mut()) {
                series.reverse();
            }
        }
    }

    // funding_rate by_window: each window -> changes[]
    if let Some(windows) = value
        .pointer_mut("/indicators/funding_rate/payload/by_window")
        .and_then(|v| v.as_object_mut())
    {
        for window in windows.values_mut() {
            if let Some(changes) = window.get_mut("changes").and_then(|v| v.as_array_mut()) {
                changes.reverse();
            }
        }
    }
}

fn round_json_floats_in_place(value: &mut Value) {
    let mut path = Vec::new();
    round_json_floats_in_place_with_path(value, &mut path);
}

fn round_json_floats_in_place_with_path(value: &mut Value, path: &mut Vec<String>) {
    match value {
        Value::Array(items) => {
            for (idx, item) in items.iter_mut().enumerate() {
                path.push(idx.to_string());
                round_json_floats_in_place_with_path(item, path);
                path.pop();
            }
        }
        Value::Object(map) => {
            for (key, item) in map.iter_mut() {
                path.push(key.clone());
                round_json_floats_in_place_with_path(item, path);
                path.pop();
            }
        }
        Value::Number(number) if number.is_f64() => {
            if should_preserve_float_precision(path) {
                return;
            }
            if let Some(v) = number.as_f64() {
                let rounded = (v * 100.0).round() / 100.0;
                if let Some(rebuilt) = Number::from_f64(rounded) {
                    *value = Value::Number(rebuilt);
                }
            }
        }
        _ => {}
    }
}

fn should_preserve_float_precision(path: &[String]) -> bool {
    is_funding_rate_value(path)
}

fn is_funding_rate_value(path: &[String]) -> bool {
    matches!(
        path,
        [root, indicator, _, .., leaf]
            if root == "indicators"
                && indicator == "funding_rate"
                && leaf.starts_with("funding_")
    )
}

async fn poll_claude_batch(
    http_client: &Client,
    claude_cfg: &ClaudeApiConfig,
    batch_id: &str,
) -> Result<ClaudeBatchEnvelope> {
    let url = format!(
        "{}/{}",
        claude_cfg.batch_api_url.trim_end_matches('/'),
        batch_id
    );
    let response = http_client
        .get(url)
        .header("x-api-key", claude_cfg.resolved_api_key())
        .header("anthropic-version", &claude_cfg.api_version)
        .send()
        .await
        .context("call claude messages batch retrieve api")?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!(
            "claude batch retrieve failed status={} body={}",
            status,
            body
        ));
    }

    response
        .json()
        .await
        .context("decode claude batch retrieve response body")
}

async fn fetch_claude_batch_result(
    http_client: &Client,
    claude_cfg: &ClaudeApiConfig,
    results_url: &str,
    custom_id: &str,
) -> Result<ClaudeBatchResultLine> {
    let response = http_client
        .get(results_url)
        .header("x-api-key", claude_cfg.resolved_api_key())
        .header("anthropic-version", &claude_cfg.api_version)
        .send()
        .await
        .context("call claude batch results url")?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!(
            "claude batch results fetch failed status={} body={}",
            status,
            body
        ));
    }

    let body = response
        .text()
        .await
        .context("read claude batch results body")?;
    for line in body.lines().filter(|line| !line.trim().is_empty()) {
        let parsed = serde_json::from_str::<ClaudeBatchResultLine>(line)
            .with_context(|| format!("decode claude batch result line: {}", line))?;
        if parsed.custom_id == custom_id {
            return Ok(parsed);
        }
    }

    if !body.trim().is_empty() {
        let parsed = serde_json::from_str::<ClaudeBatchResultLine>(&body)
            .context("decode claude single-line batch result body")?;
        if parsed.custom_id == custom_id {
            return Ok(parsed);
        }
    }

    Err(anyhow!(
        "claude batch results missing custom_id={} body_len={}",
        custom_id,
        body.len()
    ))
}

fn batch_is_terminal(batch: &ClaudeBatchEnvelope) -> bool {
    batch.processing_status.eq_ignore_ascii_case("ended")
}

fn format_batch_error(error: Option<ClaudeApiErrorResponse>) -> String {
    match error {
        Some(err) => {
            let message = err
                .error
                .as_ref()
                .map(|inner| inner.message.clone())
                .unwrap_or_else(|| "unknown batch error".to_string());
            let kind = err
                .error
                .as_ref()
                .map(|inner| inner.kind.clone())
                .unwrap_or_else(|| err.kind.clone());
            if let Some(request_id) = err.request_id {
                format!("{}: {} request_id={}", kind, message, request_id)
            } else {
                format!("{}: {}", kind, message)
            }
        }
        None => "unknown batch error".to_string(),
    }
}

fn sanitize_batch_component(raw: &str) -> String {
    raw.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn build_batch_custom_id(input: &ModelInvocationInput) -> String {
    const MAX_CUSTOM_ID_LEN: usize = 64;
    const RANDOM_HEX_LEN: usize = 12;

    let symbol = sanitize_batch_component(&input.symbol);
    let ts = input.ts_bucket.format("%Y%m%dT%H%M%SZ").to_string();
    let random = Uuid::new_v4().simple().to_string();
    let random = &random[..RANDOM_HEX_LEN];

    let mut custom_id = format!("{}_{}_{}", symbol, ts, random);
    if custom_id.len() > MAX_CUSTOM_ID_LEN {
        custom_id.truncate(MAX_CUSTOM_ID_LEN);
    }
    custom_id
}

fn parse_json_from_text(raw: &str) -> Option<Value> {
    let trimmed = raw.trim();
    if let Ok(v) = serde_json::from_str::<Value>(trimmed) {
        return Some(v);
    }

    let start = trimmed.find('{')?;
    let end = trimmed.rfind('}')?;
    if end <= start {
        return None;
    }

    let candidate = &trimmed[start..=end];
    if let Ok(v) = serde_json::from_str::<Value>(candidate) {
        return Some(v);
    }

    // Compatibility fallback:
    // Some models occasionally emit near-valid JSON with bracket mismatches
    // (e.g. missing ']' before '}' in long arrays). Repair obvious structural
    // issues and retry once so we do not skip an otherwise usable decision.
    let repaired = repair_json_candidate(candidate);
    if repaired == candidate {
        return None;
    }
    serde_json::from_str::<Value>(&repaired).ok()
}

fn repair_json_candidate(input: &str) -> String {
    let mut out = String::with_capacity(input.len() + 32);
    let mut closers: Vec<char> = Vec::new();
    let mut in_string = false;
    let mut escaped = false;

    for ch in input.chars() {
        if in_string {
            out.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        match ch {
            '"' => {
                in_string = true;
                out.push(ch);
            }
            '{' => {
                closers.push('}');
                out.push(ch);
            }
            '[' => {
                closers.push(']');
                out.push(ch);
            }
            '}' | ']' => {
                trim_trailing_comma(&mut out);

                if let Some(pos) = closers.iter().rposition(|expected| *expected == ch) {
                    while closers.len() > pos + 1 {
                        if let Some(missing) = closers.pop() {
                            trim_trailing_comma(&mut out);
                            out.push(missing);
                        }
                    }
                    let _ = closers.pop();
                    out.push(ch);
                }
            }
            _ => out.push(ch),
        }
    }

    if in_string {
        out.push('"');
    }
    while let Some(missing) = closers.pop() {
        trim_trailing_comma(&mut out);
        out.push(missing);
    }

    out
}

fn trim_trailing_comma(out: &mut String) {
    let mut end = out.len();

    while let Some(ch) = out[..end].chars().next_back() {
        if ch.is_whitespace() {
            end -= ch.len_utf8();
        } else {
            break;
        }
    }

    if let Some(ch) = out[..end].chars().next_back() {
        if ch == ',' {
            end -= ch.len_utf8();
            while let Some(ws) = out[..end].chars().next_back() {
                if ws.is_whitespace() {
                    end -= ws.len_utf8();
                } else {
                    break;
                }
            }
        }
    }

    out.truncate(end);
}

#[cfg(test)]
mod tests {
    use super::{
        build_entry_finalize_skipped_no_trade_raw, extract_grok_response_text,
        finalize_indicator_codes_for_scan, maybe_short_circuit_entry_finalize,
        parse_json_from_text, serialize_entry_finalize_input_minified,
        serialize_llm_input_minified, EntryStagePromptInputCapture, GrokResponsesApiResponse,
        ModelInvocationInput, ProviderSuccess, ProviderTrace,
    };
    use chrono::Utc;
    use serde_json::{json, Value};

    #[test]
    fn parse_json_from_text_keeps_valid_json() {
        let raw = r#"{"decision":"NO_TRADE","reason":"ok"}"#;
        let parsed = parse_json_from_text(raw).expect("should parse valid json");
        assert_eq!(
            parsed.get("decision").and_then(|v| v.as_str()),
            Some("NO_TRADE")
        );
    }

    #[test]
    fn parse_json_from_text_repairs_missing_array_bracket() {
        let raw = r#"{
  "decision": "SHORT",
  "params": {
    "data_provenance": {
      "v_derived_from": [
        "a",
        "b"
    }
  },
  "reason": "x"
}"#;

        let parsed = parse_json_from_text(raw).expect("should repair near-valid json");
        assert_eq!(
            parsed.get("decision").and_then(|v| v.as_str()),
            Some("SHORT")
        );
        assert_eq!(
            parsed
                .get("params")
                .and_then(|v| v.get("data_provenance"))
                .and_then(|v| v.get("v_derived_from"))
                .and_then(|v| v.get(1))
                .and_then(|v| v.as_str()),
            Some("b")
        );
    }

    #[test]
    fn qwen_json_schema_model_support_detection() {
        assert!(super::qwen_supports_json_schema("qwen3-max"));
        assert!(super::qwen_supports_json_schema("qwen-plus-latest"));
        assert!(super::qwen_supports_json_schema("qwen-flash"));
        assert!(!super::qwen_supports_json_schema("qwen3.5-plus"));
        assert!(!super::qwen_supports_json_schema("qwen-max"));
    }

    #[test]
    fn qwen_schema_requests_disable_thinking() {
        assert_eq!(
            super::qwen_enable_thinking(
                "qwen3-max",
                Some(true),
                false,
                false,
                super::prompt::EntryPromptStage::Finalize,
                "big_opportunity",
            ),
            Some(false)
        );
        assert_eq!(
            super::qwen_enable_thinking(
                "qwen-max",
                Some(true),
                false,
                false,
                super::prompt::EntryPromptStage::Finalize,
                "big_opportunity",
            ),
            Some(true)
        );
        assert_eq!(
            super::qwen_enable_thinking(
                "qwen3.5-plus",
                Some(true),
                false,
                false,
                super::prompt::EntryPromptStage::Finalize,
                "big_opportunity",
            ),
            Some(true)
        );
        assert_eq!(
            super::qwen_enable_thinking(
                "qwen3-max",
                Some(false),
                false,
                false,
                super::prompt::EntryPromptStage::Finalize,
                "big_opportunity",
            ),
            Some(false)
        );
    }

    #[test]
    fn normalize_qwen_shape_lifts_analysis_reason_to_top_level() {
        let value = json!({
            "decision": "NO_TRADE",
            "params": {"entry": null, "tp": null, "sl": null, "rr": null, "horizon": null},
            "analysis": {
                "reason": "wait for cleaner setup",
                "v_calculation": "bars=[0:1.0]; sorted=[1.0]; median=1.0"
            }
        });
        let normalized = super::normalize_qwen_decision_shape(value, false, false);
        assert_eq!(
            normalized.get("reason").and_then(|v| v.as_str()),
            Some("wait for cleaner setup")
        );
    }

    #[test]
    fn normalize_qwen_shape_maps_hold_to_no_trade_in_entry_mode() {
        let value = json!({
            "decision": "HOLD",
            "params": {"entry": null, "tp": null, "sl": null, "rr": null, "horizon": null},
            "analysis": {"reason": "no setup"}
        });
        let normalized = super::normalize_qwen_decision_shape(value, false, false);
        assert_eq!(
            normalized.get("decision").and_then(|v| v.as_str()),
            Some("NO_TRADE")
        );
        assert_eq!(
            normalized.get("reason").and_then(|v| v.as_str()),
            Some("no setup")
        );
    }

    #[test]
    fn chat_completions_url_accepts_full_endpoint_or_base_url() {
        assert_eq!(
            super::chat_completions_url("https://llm2.uniteonline.cn/v1/chat/completions"),
            "https://llm2.uniteonline.cn/v1/chat/completions"
        );
        assert_eq!(
            super::chat_completions_url("https://dashscope-intl.aliyuncs.com/compatible-mode/v1"),
            "https://dashscope-intl.aliyuncs.com/compatible-mode/v1/chat/completions"
        );
    }

    #[test]
    fn custom_llm_shape_uses_same_reason_lift_as_qwen() {
        let value = json!({
            "decision": "NO_TRADE",
            "params": {"entry": null, "tp": null, "sl": null, "rr": null, "horizon": null},
            "analysis": {"reason": "wait for cleaner setup"}
        });
        let normalized =
            super::normalize_provider_decision_shape("custom_llm", value, false, false);
        assert_eq!(
            normalized.get("reason").and_then(|v| v.as_str()),
            Some("wait for cleaner setup")
        );
    }

    #[test]
    fn build_entry_finalize_skipped_no_trade_raw_returns_valid_entry_decision_json() {
        let scan = json!({
            "decision": "NO_TRADE",
            "reason": "No directional edge while price is pinned to value.",
            "scan": {
                "primary_strategy": "NO_SETUP"
            }
        });

        let raw = build_entry_finalize_skipped_no_trade_raw(&scan);
        let value: Value = serde_json::from_str(&raw).expect("parse synthesized no-trade");

        assert_eq!(
            value.get("decision").and_then(Value::as_str),
            Some("NO_TRADE")
        );
        assert_eq!(value.pointer("/params/entry"), Some(&Value::Null));
        assert_eq!(value.pointer("/params/tp"), Some(&Value::Null));
        assert_eq!(value.pointer("/params/sl"), Some(&Value::Null));
        assert_eq!(value.pointer("/params/leverage"), Some(&Value::Null));
        assert_eq!(value.pointer("/params/rr"), Some(&Value::Null));
        assert_eq!(value.pointer("/params/horizon"), Some(&Value::Null));
        assert!(value
            .get("reason")
            .and_then(Value::as_str)
            .is_some_and(|reason| reason.contains("finalize was skipped")));
        crate::llm::decision::trade_intent_from_value(&value).expect("valid no-trade intent");
    }

    #[test]
    fn maybe_short_circuit_entry_finalize_skips_stage_two_for_scan_no_trade() {
        let scan_value = json!({
            "decision": "NO_TRADE",
            "reason": "No setup.",
            "scan": {
                "primary_strategy": "NO_SETUP",
                "candidate_zone": null,
                "entry_ladder": [],
                "target_ladder": [],
                "stop_ladder": []
            }
        });
        let scan = ProviderSuccess {
            raw_text: "{\"decision\":\"NO_TRADE\"}".to_string(),
            trace: ProviderTrace {
                entry_stage_prompt_inputs: vec![EntryStagePromptInputCapture {
                    stage: "scan".to_string(),
                    prompt_input: json!({"symbol": "ETHUSDT"}),
                    stage_1_setup_scan_json: None,
                }],
                ..ProviderTrace::default()
            },
        };

        let short_circuit =
            maybe_short_circuit_entry_finalize("custom_llm", &scan, &scan_value).expect("skip");
        let value: Value =
            serde_json::from_str(&short_circuit.raw_text).expect("parse short-circuit json");

        assert_eq!(
            value.get("decision").and_then(Value::as_str),
            Some("NO_TRADE")
        );
        let stage_trace = short_circuit
            .trace
            .output_entry_stage_trace()
            .expect("scan trace should exist");
        assert_eq!(stage_trace.len(), 2);
        assert_eq!(
            stage_trace[0].get("stage").and_then(Value::as_str),
            Some("scan")
        );
        assert_eq!(
            stage_trace[1].get("stage").and_then(Value::as_str),
            Some("finalize_skipped")
        );
        assert_eq!(
            stage_trace[1].get("reason").and_then(Value::as_str),
            Some("scan_decision_no_trade")
        );
        let prompt_inputs = short_circuit
            .trace
            .output_entry_stage_prompt_inputs()
            .expect("scan prompt input should be kept");
        assert_eq!(prompt_inputs.len(), 1);
        assert_eq!(prompt_inputs[0].stage, "scan");
    }

    #[test]
    fn openai_compatible_request_omits_enable_thinking_when_none() {
        let req = super::QwenChatCompletionsRequest {
            model: "qwen3.5-plus".to_string(),
            temperature: 0.1,
            max_tokens: 1000,
            messages: vec![super::QwenChatMessage {
                role: "user".to_string(),
                content: "hi".to_string(),
            }],
            response_format: None,
            enable_thinking: None,
            reasoning: None,
            stream: None,
        };
        let value = serde_json::to_value(req).expect("serialize request");
        assert!(value.get("enable_thinking").is_none());
    }

    #[test]
    fn openai_compatible_request_includes_reasoning_when_present() {
        let req = super::QwenChatCompletionsRequest {
            model: "qwen-3.5".to_string(),
            temperature: 0.1,
            max_tokens: 1000,
            messages: vec![super::QwenChatMessage {
                role: "user".to_string(),
                content: "hi".to_string(),
            }],
            response_format: Some(super::openai_json_schema_response_format(
                false,
                false,
                super::prompt::EntryPromptStage::Finalize,
                "big_opportunity",
            )),
            enable_thinking: None,
            reasoning: Some(super::OpenAiCompatibleReasoningConfig {
                effort: "xhigh".to_string(),
            }),
            stream: None,
        };
        let value = serde_json::to_value(req).expect("serialize request");
        assert_eq!(
            value
                .pointer("/response_format/type")
                .and_then(|v| v.as_str()),
            Some("json_schema")
        );
        assert_eq!(
            value.pointer("/reasoning/effort").and_then(|v| v.as_str()),
            Some("xhigh")
        );
        assert!(value.get("enable_thinking").is_none());
    }

    #[test]
    fn custom_llm_entry_schema_disables_additional_properties() {
        let schema = super::custom_llm_response_schema(
            false,
            false,
            super::prompt::EntryPromptStage::Finalize,
            "big_opportunity",
        );
        assert_eq!(
            schema.get("additionalProperties").and_then(|v| v.as_bool()),
            Some(false)
        );
        assert_eq!(
            schema
                .pointer("/properties/params/additionalProperties")
                .and_then(|v| v.as_bool()),
            Some(false)
        );
    }

    #[test]
    fn medium_large_finalize_trace_keeps_new_scan_fields() {
        let input = ModelInvocationInput {
            symbol: "ETHUSDT".to_string(),
            ts_bucket: Utc::now(),
            window_code: "15m".to_string(),
            indicator_count: 2,
            source_routing_key: "llm_indicator_minute".to_string(),
            source_published_at: None,
            received_at: Utc::now(),
            indicators: json!({
                "core_price_anchors": {"payload": {"reference_price": 2000.0}},
                "kline_history": {"payload": {"intervals": {"4h": {"futures": {"bars": []}}}}}
            }),
            missing_indicator_codes: vec![],
            management_mode: false,
            pending_order_mode: false,
            trading_state: None,
            management_snapshot: None,
        };
        let scan = json!({
            "decision": "LONG",
            "reason": "x",
            "scan": {
                "primary_strategy": "Order-Flow Reversal Sequence",
                "market_story": "flow shifted",
                "hypothesis": "reclaim should hold",
                "conviction": "medium",
                "entry_style": "post_sweep_reclaim",
                "candidate_zone": "reclaim zone",
                "entry_ladder": [
                    {
                        "price": 2045.71,
                        "label": "sweep reclaim trigger",
                        "anchor_field": "core_price_anchors.payload.reference_price",
                        "role": "preferred_entry",
                        "skip_reason": null
                    }
                ],
                "target_zone": "value-area refill",
                "target_ladder": [
                    {
                        "price": 2052.08,
                        "label": "first barrier",
                        "anchor_field": "price_volume_structure.by_window.4h.vah",
                        "role": "first_barrier",
                        "skip_reason": "too close to satisfy 1.0V"
                    },
                    {
                        "price": 2084.98,
                        "label": "1d rvwap +2sigma",
                        "anchor_field": "rvwap_sigma_bands.by_window.1d.rvwap_band_plus_2",
                        "role": "candidate_tp",
                        "skip_reason": null
                    }
                ],
                "stop_ladder": [
                    {
                        "price": 2038.2,
                        "label": "sweep low invalidation",
                        "anchor_field": "footprint.payload.by_window.4h.buy_imbalance_zone_nearest_below",
                        "role": "structural_invalidation",
                        "skip_reason": null
                    }
                ],
                "invalidation": "lose the sweep reclaim"
            }
        });

        let event = super::build_entry_finalize_trace_event(&input, &scan);
        assert_eq!(
            event.get("conviction").and_then(Value::as_str),
            Some("medium")
        );
        assert_eq!(
            event.get("setup_quality").and_then(Value::as_str),
            Some("medium")
        );
        assert_eq!(
            event.get("invalidation").and_then(Value::as_str),
            Some("lose the sweep reclaim")
        );
        assert_eq!(
            event.get("invalidation_basis").and_then(Value::as_str),
            Some("lose the sweep reclaim")
        );
        assert_eq!(
            event
                .pointer("/target_ladder/0/price")
                .and_then(Value::as_f64),
            Some(2052.08)
        );
        assert_eq!(
            event
                .pointer("/entry_ladder/0/price")
                .and_then(Value::as_f64),
            Some(2045.71)
        );
        assert_eq!(
            event
                .pointer("/stop_ladder/0/price")
                .and_then(Value::as_f64),
            Some(2038.2)
        );
    }

    #[test]
    fn finalize_indicator_codes_expand_for_selected_strategy() {
        let scan = json!({
            "decision": "LONG",
            "reason": "x",
            "scan": {
                "primary_strategy": "DOM Liquidity Wall",
                "setup_quality": "high",
                "order_flow_bias": "bullish",
                "entry_style": "patient_retest",
                "candidate_zone": "bid wall",
                "entry_ladder": [
                    {
                        "price": 1998.0,
                        "label": "bid wall retest",
                        "anchor_field": "orderbook_depth.payload.top_bid_walls[0].price",
                        "role": "preferred_entry",
                        "skip_reason": null
                    }
                ],
                "target_zone": "ask wall",
                "target_ladder": [
                    {
                        "price": 2024.5,
                        "label": "ask wall",
                        "anchor_field": "orderbook_depth.payload.top_ask_walls[0].price",
                        "role": "candidate_tp",
                        "skip_reason": null
                    }
                ],
                "stop_ladder": [
                    {
                        "price": 1992.0,
                        "label": "wall failure",
                        "anchor_field": "orderbook_depth.payload.top_bid_walls[0].price",
                        "role": "structural_invalidation",
                        "skip_reason": null
                    }
                ],
                "invalidation_basis": "wall fails",
                "stop_model_hint": "Limit Order Penetration Stop",
                "key_signals": "dom wall + obi",
                "risk_flags": "none"
            }
        });
        let codes = finalize_indicator_codes_for_scan(&scan);
        assert!(codes.contains(&"core_price_anchors"));
        assert!(codes.contains(&"orderbook_depth"));
        assert!(codes.contains(&"kline_history"));
        assert!(codes.contains(&"tpo_market_profile"));
        assert!(codes.contains(&"rvwap_sigma_bands"));
    }

    #[test]
    fn finalize_indicator_codes_include_scan_referenced_raw_indicators() {
        let scan = json!({
            "decision": "NO_TRADE",
            "reason": "x",
            "scan": {
                "primary_strategy": "NO_SETUP",
                "market_story": "ema_trend_regime still bull, but absorption at 2100.73, buying_exhaustion at 2113.77, and no active 4h/1d FVG keep the auction rotational.",
                "hypothesis": "No trade unless selling_exhaustion absorbs the next dip and whale_trades stop pressing lower.",
                "conviction": "low",
                "entry_style": null,
                "candidate_zone": null,
                "entry_ladder": [],
                "target_zone": null,
                "target_ladder": [],
                "stop_ladder": [],
                "invalidation": "Stand aside until orderbook_depth improves and divergence resolves."
            }
        });

        let codes = finalize_indicator_codes_for_scan(&scan);
        assert!(codes.contains(&"ema_trend_regime"));
        assert!(codes.contains(&"absorption"));
        assert!(codes.contains(&"buying_exhaustion"));
        assert!(codes.contains(&"selling_exhaustion"));
        assert!(codes.contains(&"whale_trades"));
        assert!(codes.contains(&"orderbook_depth"));
        assert!(codes.contains(&"divergence"));
        assert!(codes.contains(&"fvg"));
    }

    #[test]
    fn serialize_entry_finalize_input_uses_strategy_focused_indicator_subset() {
        let now = Utc::now();
        let input = ModelInvocationInput {
            symbol: "ETHUSDT".to_string(),
            ts_bucket: now,
            window_code: "15m".to_string(),
            indicator_count: 11,
            source_routing_key: "llm_indicator_minute".to_string(),
            source_published_at: None,
            received_at: now,
            indicators: json!({
                "core_price_anchors": {"payload": {"reference_price": 2000.0}},
                "price_volume_structure": {"payload": {"val": 1990.0, "vah": 2010.0}},
                "footprint": {"payload": {"by_window": {"4h": {"ua_top": 2020.0}}}},
                "cvd_pack": {"payload": {"delta_fut": 1000.0}},
                "avwap": {"payload": {"fut_last_price": 2001.0}},
                "kline_history": {"payload": {"intervals": {"1d": {"futures": {"bars": []}}}}},
                "orderbook_depth": {"payload": {"obi_k_dw_twa_fut": 0.7}},
                "liquidation_density": {"payload": {"peak_levels": []}},
                "tpo_market_profile": {"payload": {"by_window": {"4h": {"tpo_poc": 2004.0}}}},
                "rvwap_sigma_bands": {"payload": {"by_window": {"1d": {"rvwap_w": 1998.0}}}},
                "divergence": {"payload": {"signal": "none"}}
            }),
            missing_indicator_codes: vec![],
            management_mode: false,
            pending_order_mode: false,
            trading_state: None,
            management_snapshot: None,
        };
        let scan = json!({
            "decision": "LONG",
            "reason": "x",
            "scan": {
                "primary_strategy": "DOM Liquidity Wall",
                "setup_quality": "high",
                "order_flow_bias": "bullish",
                "entry_style": "patient_retest",
                "candidate_zone": "bid wall",
                "entry_ladder": [
                    {
                        "price": 1998.0,
                        "label": "bid wall retest",
                        "anchor_field": "orderbook_depth.payload.top_bid_walls[0].price",
                        "role": "preferred_entry",
                        "skip_reason": null
                    }
                ],
                "target_zone": "ask wall",
                "target_ladder": [
                    {
                        "price": 2024.5,
                        "label": "ask wall",
                        "anchor_field": "orderbook_depth.payload.top_ask_walls[0].price",
                        "role": "candidate_tp",
                        "skip_reason": null
                    }
                ],
                "stop_ladder": [
                    {
                        "price": 1992.0,
                        "label": "wall failure",
                        "anchor_field": "orderbook_depth.payload.top_bid_walls[0].price",
                        "role": "structural_invalidation",
                        "skip_reason": null
                    }
                ],
                "invalidation_basis": "wall fails",
                "stop_model_hint": "Limit Order Penetration Stop",
                "key_signals": "dom wall + obi",
                "risk_flags": "none"
            }
        });

        let serialized =
            serialize_entry_finalize_input_minified(&input, &scan).expect("serialize finalize");
        let value: Value = serde_json::from_str(&serialized).expect("parse finalize input");

        assert!(value.pointer("/indicators/orderbook_depth").is_some());
        assert!(value.pointer("/indicators/core_price_anchors").is_some());
        assert!(value
            .pointer("/indicators/price_volume_structure")
            .is_some());
        assert!(value.pointer("/indicators/footprint").is_some());
        assert!(value.pointer("/indicators/avwap").is_some());
        assert!(value.pointer("/indicators/kline_history").is_some());
        assert!(value.pointer("/indicators/pre_computed_v").is_some());
        assert!(value.pointer("/indicators/tpo_market_profile").is_some());
        assert!(value.pointer("/indicators/rvwap_sigma_bands").is_some());
        assert_eq!(
            value
                .pointer("/finalize_focus/primary_strategy")
                .and_then(|v| v.as_str()),
            Some("DOM Liquidity Wall")
        );
    }

    #[test]
    fn serialize_entry_finalize_input_keeps_scan_referenced_supporting_indicators() {
        let now = Utc::now();
        let input = ModelInvocationInput {
            symbol: "ETHUSDT".to_string(),
            ts_bucket: now,
            window_code: "15m".to_string(),
            indicator_count: 14,
            source_routing_key: "llm_indicator_minute".to_string(),
            source_published_at: None,
            received_at: now,
            indicators: json!({
                "core_price_anchors": {"payload": {"reference_price": 2105.08}},
                "price_volume_structure": {"payload": {"by_window": {"4h": {"val": 2103.04}}}},
                "footprint": {"payload": {"by_window": {"4h": {"buy_imbalance_zone_nearest_below": 2100.5}}}},
                "cvd_pack": {"payload": {"delta_fut": 863.89}},
                "avwap": {"payload": {"avwap_fut": 2046.49}},
                "kline_history": {"payload": {"intervals": {"4h": {"futures": {"bars": []}}}}},
                "tpo_market_profile": {"payload": {"tpo_val": 2108.16, "tpo_poc": 2110.92}},
                "rvwap_sigma_bands": {"payload": {"by_window": {"15m": {"rvwap_w": 2107.5}}}},
                "absorption": {"payload": {"latest_7d": {"price_low": 2100.73}}},
                "buying_exhaustion": {"payload": {"latest_7d": {"pivot_price": 2113.77}}},
                "selling_exhaustion": {"payload": {"latest_7d": {"pivot_price": 2103.46}}},
                "ema_trend_regime": {"payload": {"trend_regime_by_tf": {"4h": "bull", "1d": "bull"}}},
                "fvg": {"payload": {"by_window": {"4h": {"active_bear_fvgs": [], "active_bull_fvgs": []}}}},
                "orderbook_depth": {"payload": {"by_window": {"15m": {"ofi_norm_fut": -0.53}}}},
                "whale_trades": {"payload": {"by_window": {"15m": {"fut_whale_delta_notional": -906355.57}}}}
            }),
            missing_indicator_codes: vec![],
            management_mode: false,
            pending_order_mode: false,
            trading_state: None,
            management_snapshot: None,
        };
        let scan = json!({
            "decision": "NO_TRADE",
            "reason": "x",
            "scan": {
                "primary_strategy": "NO_SETUP",
                "market_story": "ema_trend_regime is still bull, but buying_exhaustion at 2113.77, absorption at 2100.73, and no active 4h/1d FVG keep the auction rotational.",
                "hypothesis": "Stand aside unless selling_exhaustion stops pressing and whale_trades stop leaning short.",
                "conviction": "low",
                "entry_style": null,
                "candidate_zone": null,
                "entry_ladder": [],
                "target_zone": null,
                "target_ladder": [],
                "stop_ladder": [],
                "invalidation": "No directional edge while orderbook_depth stays weak."
            }
        });

        let serialized =
            serialize_entry_finalize_input_minified(&input, &scan).expect("serialize finalize");
        let value: Value = serde_json::from_str(&serialized).expect("parse finalize input");

        assert!(value.pointer("/indicators/ema_trend_regime").is_some());
        assert!(value.pointer("/indicators/absorption").is_some());
        assert!(value.pointer("/indicators/buying_exhaustion").is_some());
        assert!(value.pointer("/indicators/selling_exhaustion").is_some());
        assert!(value.pointer("/indicators/fvg").is_some());
        assert!(value.pointer("/indicators/orderbook_depth").is_some());
        assert!(value.pointer("/indicators/whale_trades").is_some());
        assert!(value.pointer("/indicators/tpo_market_profile").is_some());
        assert!(value.pointer("/indicators/rvwap_sigma_bands").is_some());
    }

    #[test]
    fn serialize_entry_finalize_input_keeps_tp_relevant_htf_indicators_for_value_area_refill() {
        let now = Utc::now();
        let input = ModelInvocationInput {
            symbol: "ETHUSDT".to_string(),
            ts_bucket: now,
            window_code: "15m".to_string(),
            indicator_count: 12,
            source_routing_key: "llm_indicator_minute".to_string(),
            source_published_at: None,
            received_at: now,
            indicators: json!({
                "core_price_anchors": {"payload": {"reference_price": 2000.0}},
                "price_volume_structure": {"payload": {"val": 1990.0, "vah": 2010.0, "hvn_levels": []}},
                "footprint": {"payload": {"by_window": {"1d": {"buy_imbalance_zones_top": [2084.98]}}}},
                "cvd_pack": {"payload": {"delta_fut": 1000.0}},
                "avwap": {"payload": {"fut_last_price": 2001.0}},
                "kline_history": {"payload": {"intervals": {"1d": {"futures": {"bars": []}}}}},
                "rvwap_sigma_bands": {"payload": {"by_window": {"1d": {"rvwap_band_plus_2": 2084.98}}}},
                "tpo_market_profile": {"payload": {"by_window": {"1d": {"tpo_vah": 2078.16}}}},
                "liquidation_density": {"payload": {"by_window": {"1d": {"peak_levels": [{"price": 2088.29}]}}}},
                "vpin": {"payload": {"z_vpin_fut": 0.4}},
                "orderbook_depth": {"payload": {"obi_k_dw_twa_fut": 0.1}},
                "fvg": {"payload": {"by_window": {"1d": {"active_bull_fvgs": []}}}}
            }),
            missing_indicator_codes: vec![],
            management_mode: false,
            pending_order_mode: false,
            trading_state: None,
            management_snapshot: None,
        };
        let scan = json!({
            "decision": "LONG",
            "reason": "x",
            "scan": {
                "primary_strategy": "Value Area Re-fill",
                "setup_quality": "high",
                "order_flow_bias": "bullish",
                "entry_style": "patient_retest",
                "candidate_zone": "value area low",
                "entry_ladder": [
                    {
                        "price": 1994.5,
                        "label": "value area low retest",
                        "anchor_field": "price_volume_structure.payload.val",
                        "role": "preferred_entry",
                        "skip_reason": null
                    }
                ],
                "target_zone": "value area high",
                "target_ladder": [
                    {
                        "price": 2078.16,
                        "label": "1d vah",
                        "anchor_field": "tpo_market_profile.payload.by_window.1d.tpo_vah",
                        "role": "first_barrier",
                        "skip_reason": "below 1.0V"
                    },
                    {
                        "price": 2084.98,
                        "label": "1d rvwap +2sigma",
                        "anchor_field": "rvwap_sigma_bands.payload.by_window.1d.rvwap_band_plus_2",
                        "role": "candidate_tp",
                        "skip_reason": null
                    }
                ],
                "stop_ladder": [
                    {
                        "price": 1989.0,
                        "label": "value area low loss",
                        "anchor_field": "price_volume_structure.payload.val",
                        "role": "structural_invalidation",
                        "skip_reason": null
                    }
                ],
                "invalidation_basis": "lose value area low",
                "stop_model_hint": "Value Area Invalidation Stop",
                "key_signals": "value migration",
                "risk_flags": "none"
            }
        });

        let serialized =
            serialize_entry_finalize_input_minified(&input, &scan).expect("serialize finalize");
        let value: Value = serde_json::from_str(&serialized).expect("parse finalize input");

        assert!(value.pointer("/indicators/rvwap_sigma_bands").is_some());
        assert!(value.pointer("/indicators/tpo_market_profile").is_some());
        assert!(value.pointer("/indicators/liquidation_density").is_some());
        assert!(value
            .pointer("/indicators/price_volume_structure")
            .is_some());
        assert!(value.pointer("/indicators/footprint").is_some());
        assert!(value.pointer("/indicators/avwap").is_some());
        assert!(value.pointer("/indicators/kline_history").is_some());
        assert!(value.pointer("/indicators/pre_computed_v").is_some());
    }

    #[test]
    fn build_prompt_pair_captures_finalize_prompt_input_and_stage1_scan() {
        let now = Utc::now();
        let input = ModelInvocationInput {
            symbol: "ETHUSDT".to_string(),
            ts_bucket: now,
            window_code: "15m".to_string(),
            indicator_count: 3,
            source_routing_key: "llm_indicator_minute".to_string(),
            source_published_at: None,
            received_at: now,
            indicators: json!({
                "core_price_anchors": {"payload": {"reference_price": 2000.0}},
                "price_volume_structure": {"payload": {"val": 1990.0, "vah": 2010.0}},
                "kline_history": {"payload": {"intervals": {"4h": {"futures": {"bars": []}}}}}
            }),
            missing_indicator_codes: vec![],
            management_mode: false,
            pending_order_mode: false,
            trading_state: None,
            management_snapshot: None,
        };
        let scan = json!({
            "decision": "LONG",
            "reason": "x",
            "scan": {
                "primary_strategy": "Value Area Re-fill",
                "setup_quality": "high",
                "order_flow_bias": "bullish",
                "entry_style": "patient_retest",
                "candidate_zone": "value area low",
                "entry_ladder": [
                    {
                        "price": 1994.5,
                        "label": "value area low retest",
                        "anchor_field": "price_volume_structure.payload.val",
                        "role": "preferred_entry",
                        "skip_reason": null
                    }
                ],
                "target_zone": "value area high",
                "target_ladder": [
                    {
                        "price": 2010.0,
                        "label": "1d vah",
                        "anchor_field": "price_volume_structure.payload.by_window.1d.vah",
                        "role": "candidate_tp",
                        "skip_reason": null
                    }
                ],
                "stop_ladder": [
                    {
                        "price": 1989.0,
                        "label": "value area loss",
                        "anchor_field": "price_volume_structure.payload.val",
                        "role": "structural_invalidation",
                        "skip_reason": null
                    }
                ],
                "invalidation_basis": "lose value area low",
                "stop_model_hint": "Value Area Invalidation Stop",
                "key_signals": "value migration",
                "risk_flags": "none"
            }
        });

        let prompt = super::build_prompt_pair(
            &input,
            "big_opportunity",
            super::prompt::EntryPromptStage::Finalize,
            Some(&scan),
        )
        .expect("build finalize prompt pair");

        let capture = prompt
            .prompt_input_capture
            .expect("capture should exist for entry finalize");
        assert_eq!(capture.stage, "finalize");
        assert_eq!(
            capture
                .stage_1_setup_scan_json
                .as_ref()
                .and_then(|value| value.pointer("/scan/target_ladder/0/price"))
                .and_then(Value::as_f64),
            Some(2010.0)
        );
        assert_eq!(
            capture
                .stage_1_setup_scan_json
                .as_ref()
                .and_then(|value| value.pointer("/scan/entry_ladder/0/price"))
                .and_then(Value::as_f64),
            Some(1994.5)
        );
        assert_eq!(
            capture
                .stage_1_setup_scan_json
                .as_ref()
                .and_then(|value| value.pointer("/scan/stop_ladder/0/price"))
                .and_then(Value::as_f64),
            Some(1989.0)
        );
        assert_eq!(
            capture
                .prompt_input
                .pointer("/finalize_focus/primary_strategy")
                .and_then(Value::as_str),
            Some("Value Area Re-fill")
        );
    }

    #[test]
    fn serialize_entry_scan_input_compacts_large_indicator_payloads() {
        let now = Utc::now();

        let funding_changes = (0..35)
            .map(|idx| {
                json!({
                    "change_ts": format!("2026-03-{:02}T{:02}:00:00Z", (idx % 28) + 1, idx % 24),
                    "funding_new": -0.00004 - (idx as f64 * 0.000001),
                    "funding_prev": -0.000039 - (idx as f64 * 0.000001),
                    "funding_delta": -0.000001,
                    "mark_price_at_change": 2000.0 + idx as f64,
                })
            })
            .collect::<Vec<_>>();
        let funding_recent = funding_changes.clone();

        let bars = (0..130)
            .map(|idx| {
                json!({
                    "open_time": format!("2026-03-11T{:02}:00:00Z", idx % 24),
                    "close_time": format!("2026-03-11T{:02}:14:59Z", idx % 24),
                    "is_closed": true,
                    "open": 2000.0 + idx as f64,
                    "high": 2001.0 + idx as f64,
                    "low": 1999.0 + idx as f64,
                    "close": 2000.5 + idx as f64,
                    "volume_base": 100.0 + idx as f64,
                    "volume_quote": 200000.0 + idx as f64,
                    "expected_minutes": 15,
                    "minutes_covered": 15,
                })
            })
            .collect::<Vec<_>>();

        let divergence_events = (0..12)
            .map(|idx| {
                json!({
                    "type": if idx % 2 == 0 { "bullish_divergence" } else { "hidden_bearish_divergence" },
                    "score": 0.55 + idx as f64 * 0.01,
                    "sig_pass": idx % 3 != 0,
                    "likely_driver": if idx % 2 == 0 { "spot" } else { "futures" },
                    "confirm_ts": format!("2026-03-10T{:02}:00:00Z", idx),
                    "event_end_ts": format!("2026-03-10T{:02}:05:00Z", idx),
                })
            })
            .collect::<Vec<_>>();

        let scored_events = (0..12)
            .map(|idx| {
                json!({
                    "direction": if idx % 2 == 0 { 1 } else { -1 },
                    "score": 0.6 + idx as f64 * 0.01,
                    "sig_pass": idx % 4 != 0,
                    "confirm_ts": format!("2026-03-09T{:02}:00:00Z", idx),
                    "event_end_ts": format!("2026-03-09T{:02}:05:00Z", idx),
                    "event_id": format!("event-{}", idx),
                })
            })
            .collect::<Vec<_>>();

        let avwap_15m = (0..40)
            .map(|idx| {
                json!({
                    "ts": format!("2026-03-11T{:02}:00:00Z", idx % 24),
                    "avwap_fut": 2000.0 + idx as f64,
                    "avwap_spot": 1999.5 + idx as f64,
                    "price_minus_avwap_fut": idx as f64 * 0.25,
                })
            })
            .collect::<Vec<_>>();
        let avwap_4h = (0..5)
            .map(|idx| {
                json!({
                    "ts": format!("2026-03-{}T00:00:00Z", idx + 1),
                    "avwap_fut": 2000.0 + idx as f64,
                })
            })
            .collect::<Vec<_>>();

        let cvd_15m = (0..40)
            .map(|idx| {
                json!({
                    "ts": format!("2026-03-11T{:02}:00:00Z", idx % 24),
                    "delta_fut": 1000.0 + idx as f64,
                    "delta_spot": 800.0 + idx as f64,
                    "relative_delta_fut": 0.1 + idx as f64 * 0.001,
                    "relative_delta_spot": 0.08 + idx as f64 * 0.001,
                    "spot_flow_dominance": 0.5,
                    "xmk_delta_gap_s_minus_f": -0.02,
                    "ignored_large_blob": {"foo": "bar"}
                })
            })
            .collect::<Vec<_>>();
        let cvd_4h = (0..5)
            .map(|idx| {
                json!({
                    "ts": format!("2026-03-{}T00:00:00Z", idx + 1),
                    "delta_fut": 100.0 + idx as f64,
                })
            })
            .collect::<Vec<_>>();

        let footprint_levels = (0..24)
            .map(|idx| {
                json!({
                    "price": 2000.0 + idx as f64,
                    "delta": 100.0 + idx as f64,
                    "total": 200.0 + idx as f64,
                })
            })
            .collect::<Vec<_>>();
        let footprint_strength = (0..20)
            .map(|idx| {
                json!({
                    "price": 2000.0 + idx as f64,
                    "strength": 0.7 + idx as f64 * 0.01,
                })
            })
            .collect::<Vec<_>>();

        let pvs_levels = (0..16)
            .map(|idx| {
                json!({
                    "price": 2000.0 + idx as f64,
                    "volume": 1000.0 + idx as f64,
                })
            })
            .collect::<Vec<_>>();

        let input = ModelInvocationInput {
            symbol: "ETHUSDT".to_string(),
            ts_bucket: now,
            window_code: "15m".to_string(),
            indicator_count: 9,
            source_routing_key: "llm_indicator_minute".to_string(),
            source_published_at: None,
            received_at: now,
            indicators: json!({
                "funding_rate": {
                    "payload": {
                        "funding_current": -0.00004527,
                        "funding_current_effective_ts": "2026-03-11T06:00:00Z",
                        "funding_twa": -0.00004346,
                        "mark_price_last": 1982.6936124,
                        "mark_price_last_ts": "2026-03-11T06:00:00Z",
                        "mark_price_twap": 1980.12345,
                        "change_count": 35,
                        "changes": funding_changes,
                        "by_window": {
                            "15m": {
                                "funding_current": -0.00004527,
                                "changes_recent": funding_recent,
                                "changes_bucketed": {
                                    "buckets": [{"funding_delta_sum": -0.00000373}]
                                }
                            }
                        }
                    }
                },
                "kline_history": {
                    "payload": {
                        "as_of_ts": "2026-03-11T06:20:00Z",
                        "indicator": "kline_history",
                        "window": "15m",
                        "intervals": {
                            "15m": {
                                "futures": {"bars": bars},
                                "spot": {"bars": bars},
                                "spot_gap": {"gap_bps": 1.23}
                            }
                        }
                    }
                },
                "divergence": {
                    "payload": {
                        "signal": "bullish_divergence",
                        "signals": ["bullish_divergence"],
                        "reason": "spot confirms",
                        "event_count": 12,
                        "latest": {"score": 0.82},
                        "latest_7d": {"score": 0.92},
                        "candidates": [{"score": 0.3}],
                        "recent_7d": {
                            "event_count": 12,
                            "history_source": "cache",
                            "lookback_coverage_ratio": 1.0,
                            "lookback_covered_minutes": 10080,
                            "lookback_missing_minutes": 0,
                            "lookback_requested_minutes": 10080,
                            "events": divergence_events
                        }
                    }
                },
                "absorption": {
                    "payload": {
                        "event_count": 12,
                        "events": [{"id": "top-level-event"}],
                        "latest": {"score": 0.7},
                        "latest_7d": {"score": 0.8},
                        "recent_7d": {
                            "event_count": 12,
                            "history_source": "cache",
                            "lookback_coverage_ratio": 1.0,
                            "lookback_covered_minutes": 10080,
                            "lookback_missing_minutes": 0,
                            "lookback_requested_minutes": 10080,
                            "events": scored_events
                        }
                    }
                },
                "avwap": {
                    "payload": {
                        "anchor_ts": "2026-03-11T00:00:00Z",
                        "avwap_fut": 2001.0,
                        "avwap_spot": 1999.0,
                        "fut_last_price": 2002.0,
                        "fut_mark_price": 2001.5,
                        "indicator": "avwap",
                        "lookback": "7d",
                        "price_minus_avwap_fut": 1.0,
                        "price_minus_spot_avwap_fut": 2.0,
                        "price_minus_spot_avwap_futmark": 2.5,
                        "window": "15m",
                        "xmk_avwap_gap_f_minus_s": 0.1,
                        "zavwap_gap": 1.5,
                        "series_by_window": {
                            "15m": avwap_15m,
                            "4h": avwap_4h,
                            "1d": avwap_4h
                        }
                    }
                },
                "cvd_pack": {
                    "payload": {
                        "cvd_slope_fut": 0.5,
                        "cvd_slope_spot": 0.6,
                        "delta_fut": 1100.0,
                        "delta_spot": 900.0,
                        "likely_driver": "spot",
                        "relative_delta_fut": 0.11,
                        "relative_delta_spot": 0.13,
                        "spot_flow_dominance": 0.57,
                        "spot_lead_score": 0.61,
                        "xmk_delta_gap_s_minus_f": 0.02,
                        "by_window": {
                            "15m": {
                                "delta_fut": 1100.0,
                                "series": cvd_15m
                            },
                            "4h": {
                                "delta_fut": 4000.0,
                                "series": cvd_4h
                            }
                        }
                    }
                },
                "footprint": {
                    "payload": {
                        "by_window": {
                            "4h": {
                                "buy_imbalance_count": 4,
                                "buy_imbalance_zone_count": 3,
                                "buy_imbalance_zone_nearest_above": 2010.0,
                                "buy_imbalance_zone_nearest_below": 1990.0,
                                "buy_stack_count": 5,
                                "levels_total": 300,
                                "max_buy_stack_len": 4,
                                "max_sell_stack_len": 3,
                                "sell_imbalance_count": 3,
                                "sell_imbalance_zone_count": 2,
                                "sell_imbalance_zone_nearest_above": 2020.0,
                                "sell_imbalance_zone_nearest_below": 1980.0,
                                "sell_stack_count": 4,
                                "stacked_buy": true,
                                "stacked_sell": false,
                                "ua_bottom": 1988.0,
                                "ua_top": 2012.0,
                                "unfinished_auction": true,
                                "window_delta": 2300.0,
                                "window_total_qty": 5400.0,
                                "buy_imbalance_zones_top_strength": footprint_strength,
                                "sell_imbalance_zones_top_strength": footprint_strength,
                                "top_levels_by_abs_delta": footprint_levels,
                                "top_levels_by_total": footprint_levels,
                                "levels_near_price_bucketed": [{"price": 2001.0}]
                            },
                            "15m": {
                                "levels_near_price_bucketed": [{"price": 2000.0}]
                            }
                        }
                    }
                },
                "price_volume_structure": {
                    "payload": {
                        "by_window": {
                            "4h": {
                                "bar_volume": 12000.0,
                                "levels_total": 250,
                                "poc_price": 2001.0,
                                "poc_volume": 500.0,
                                "vah": 2010.0,
                                "val": 1990.0,
                                "value_area_level_count": 40,
                                "volume_dryup": false,
                                "volume_zscore": 1.2,
                                "window_bars_used": 24,
                                "hvn_levels": pvs_levels,
                                "lvn_levels": pvs_levels,
                                "top_abs_delta_levels": pvs_levels,
                                "top_value_area_levels": pvs_levels,
                                "top_volume_levels": pvs_levels,
                                "levels_in_va_corridor_bucketed": [{"price": 2000.0}]
                            },
                            "15m": {
                                "levels_in_va_corridor_bucketed": [{"price": 2000.0}]
                            }
                        }
                    }
                }
            }),
            missing_indicator_codes: vec![],
            management_mode: false,
            pending_order_mode: false,
            trading_state: None,
            management_snapshot: None,
        };

        let original = serde_json::to_string(&input).expect("serialize original");
        let serialized = serialize_llm_input_minified(&input).expect("serialize compacted");
        let value: Value = serde_json::from_str(&serialized).expect("parse compacted");

        assert!(serialized.len() < original.len());
        assert_eq!(
            value
                .pointer("/indicators/funding_rate/payload/changes")
                .and_then(|v| v.as_array())
                .map(Vec::len),
            Some(30)
        );
        assert!(value
            .pointer("/indicators/funding_rate/payload/by_window/15m/changes_bucketed")
            .is_none());
        assert_eq!(
            value
                .pointer("/indicators/funding_rate/payload/by_window/15m/changes_recent")
                .and_then(|v| v.as_array())
                .map(Vec::len),
            Some(30)
        );
        assert_eq!(
            value
                .pointer("/indicators/kline_history/payload/intervals/15m/futures/bars")
                .and_then(|v| v.as_array())
                .map(Vec::len),
            Some(120)
        );
        assert!(value
            .pointer("/indicators/kline_history/payload/intervals/15m/spot")
            .is_none());
        assert!(value
            .pointer("/indicators/kline_history/payload/intervals/15m/futures/bars/0/close_time")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/candidates")
            .is_none());
        assert_eq!(
            value
                .pointer("/indicators/divergence/payload/recent_7d/events")
                .and_then(|v| v.as_array())
                .map(Vec::len),
            Some(12)
        );
        assert!(value
            .pointer("/indicators/divergence/payload/recent_7d/summary/confirmed_count")
            .is_some());
        assert_eq!(
            value
                .pointer("/indicators/absorption/payload/recent_7d/events")
                .and_then(|v| v.as_array())
                .map(Vec::len),
            Some(12)
        );
        assert!(value
            .pointer("/indicators/absorption/payload/recent_7d/summary/confirmed_count")
            .is_some());
        assert_eq!(
            value
                .pointer("/indicators/avwap/payload/series_by_window/15m")
                .and_then(|v| v.as_array())
                .map(Vec::len),
            Some(30)
        );
        assert_eq!(
            value
                .pointer("/indicators/avwap/payload/series_by_window/4h")
                .and_then(|v| v.as_array())
                .map(Vec::len),
            Some(5)
        );
        assert_eq!(
            value
                .pointer("/indicators/cvd_pack/payload/by_window/15m/series")
                .and_then(|v| v.as_array())
                .map(Vec::len),
            Some(30)
        );
        assert_eq!(
            value
                .pointer("/indicators/cvd_pack/payload/by_window/4h/series")
                .and_then(|v| v.as_array())
                .map(Vec::len),
            Some(5)
        );
        assert_eq!(
            value
                .pointer(
                    "/indicators/footprint/payload/by_window/4h/buy_imbalance_zones_top_strength"
                )
                .and_then(|v| v.as_array())
                .map(Vec::len),
            Some(12)
        );
        assert_eq!(
            value
                .pointer("/indicators/footprint/payload/by_window/4h/top_levels_by_abs_delta")
                .and_then(|v| v.as_array())
                .map(Vec::len),
            Some(16)
        );
        assert!(value
            .pointer("/indicators/footprint/payload/by_window/4h/levels_near_price_bucketed")
            .is_none());
        assert!(value
            .pointer("/indicators/footprint/payload/by_window/15m/levels_near_price_bucketed")
            .is_some());
        assert_eq!(
            value
                .pointer("/indicators/price_volume_structure/payload/by_window/4h/hvn_levels")
                .and_then(|v| v.as_array())
                .map(Vec::len),
            Some(8)
        );
        assert_eq!(
            value
                .pointer("/indicators/price_volume_structure/payload/by_window/4h/lvn_levels")
                .and_then(|v| v.as_array())
                .map(Vec::len),
            Some(12)
        );
        assert_eq!(
            value
                .pointer(
                    "/indicators/price_volume_structure/payload/by_window/4h/top_abs_delta_levels"
                )
                .and_then(|v| v.as_array())
                .map(Vec::len),
            Some(8)
        );
        assert!(value
            .pointer("/indicators/price_volume_structure/payload/by_window/4h/levels_in_va_corridor_bucketed")
            .is_none());
        assert!(value
            .pointer("/indicators/price_volume_structure/payload/by_window/15m/levels_in_va_corridor_bucketed")
            .is_some());
    }

    #[test]
    fn funding_rate_fields_keep_precision_during_input_rounding() {
        let mut value = json!({
            "indicators": {
                "funding_rate": {
                    "payload": {
                        "funding_current": -0.00004527,
                        "funding_twa": -0.00004346224245555564,
                        "mark_price_last": 1982.6936124,
                        "by_window": {
                            "15m": {
                                "changes_recent": [
                                    {
                                        "funding_delta": -6.29999999999999e-7,
                                        "funding_new": -0.0000415,
                                        "funding_prev": -0.00004087,
                                        "mark_price_at_change": 1982.9
                                    }
                                ],
                                "changes_bucketed": {
                                    "buckets": [
                                        {
                                            "funding_delta_abs_max": 0.00000111,
                                            "funding_delta_sum": -0.00000373,
                                            "funding_new_last": -0.00004527,
                                            "mark_price_vwap": 1983.3546190479492
                                        }
                                    ]
                                }
                            }
                        }
                    }
                },
                "vpin": {
                    "payload": {
                        "vpin_fut": 0.724567
                    }
                }
            }
        });

        super::round_json_floats_in_place(&mut value);

        assert_eq!(
            value.pointer("/indicators/funding_rate/payload/funding_current"),
            Some(&json!(-0.00004527))
        );
        assert_eq!(
            value.pointer("/indicators/funding_rate/payload/funding_twa"),
            Some(&json!(-0.00004346224245555564_f64))
        );
        assert_eq!(
            value.pointer(
                "/indicators/funding_rate/payload/by_window/15m/changes_recent/0/funding_delta"
            ),
            Some(&json!(-6.29999999999999e-7_f64))
        );
        assert_eq!(
            value.pointer("/indicators/funding_rate/payload/by_window/15m/changes_bucketed/buckets/0/funding_delta_sum"),
            Some(&json!(-0.00000373_f64))
        );
        assert_eq!(
            value.pointer("/indicators/funding_rate/payload/mark_price_last"),
            Some(&json!(1982.69))
        );
        assert_eq!(
            value.pointer("/indicators/vpin/payload/vpin_fut"),
            Some(&json!(0.72))
        );
    }

    #[test]
    fn grok_responses_output_text_extraction_reads_message_content() {
        let body: GrokResponsesApiResponse = serde_json::from_value(json!({
            "status": "completed",
            "usage": {"output_tokens": 5},
            "output": [
                {
                    "type": "message",
                    "role": "assistant",
                    "status": "completed",
                    "content": [
                        {
                            "type": "output_text",
                            "text": "{\"decision\":\"NO_TRADE\"}"
                        }
                    ]
                }
            ]
        }))
        .expect("response should deserialize");

        assert_eq!(
            extract_grok_response_text(&body).as_deref(),
            Some("{\"decision\":\"NO_TRADE\"}")
        );
    }
}

fn extract_chat_message_text(content: &Value) -> Option<String> {
    match content {
        Value::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        Value::Array(parts) => {
            let text = parts
                .iter()
                .filter_map(|part| {
                    part.as_str()
                        .or_else(|| part.get("text").and_then(Value::as_str))
                })
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
                .join("\n");
            if text.is_empty() {
                None
            } else {
                Some(text)
            }
        }
        _ => None,
    }
}

fn extract_grok_response_text(body: &GrokResponsesApiResponse) -> Option<String> {
    let text = body
        .output
        .iter()
        .flat_map(|item| item.content.iter())
        .filter(|content| content.kind.as_deref() == Some("output_text") || content.text.is_some())
        .filter_map(|content| content.text.as_deref())
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n");

    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

fn claude_response_tool(
    management_mode: bool,
    pending_order_mode: bool,
    entry_stage: prompt::EntryPromptStage,
    prompt_template: &str,
) -> ClaudeTool {
    ClaudeTool {
        name: CLAUDE_DECISION_TOOL_NAME.to_string(),
        description: "Emit final decision JSON payload only; do not include narrative text."
            .to_string(),
        input_schema: if pending_order_mode {
            grok_pending_order_response_schema()
        } else if management_mode {
            if is_medium_large(prompt_template) {
                ml_grok_management_schema()
            } else {
                grok_management_response_schema()
            }
        } else if matches!(entry_stage, prompt::EntryPromptStage::Scan) {
            if is_medium_large(prompt_template) {
                ml_grok_entry_scan_schema()
            } else {
                grok_entry_scan_response_schema()
            }
        } else {
            if is_medium_large(prompt_template) {
                ml_grok_entry_schema()
            } else {
                grok_entry_response_schema()
            }
        },
    }
}

#[derive(Debug, Serialize)]
struct ClaudeMessageRequest {
    model: String,
    max_tokens: u32,
    temperature: f64,
    system: Vec<ClaudeTextBlock>,
    messages: Vec<ClaudeInputMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<ClaudeTool>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_choice: Option<ClaudeToolChoice>,
}

#[derive(Debug, Serialize, Clone)]
struct ClaudeInputMessage {
    role: String,
    content: Vec<ClaudeTextBlock>,
}

#[derive(Debug, Serialize)]
struct ClaudeCountTokensRequest {
    model: String,
    system: Vec<ClaudeTextBlock>,
    messages: Vec<ClaudeInputMessage>,
}

#[derive(Debug, Deserialize)]
struct ClaudeCountTokensResponse {
    input_tokens: u64,
}

#[derive(Debug, Deserialize)]
struct ClaudeMessageResponse {
    content: Vec<ClaudeContentBlock>,
    usage: Option<ClaudeUsage>,
    #[serde(default)]
    stop_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ClaudeContentBlock {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    name: Option<String>,
    text: Option<String>,
    #[serde(default)]
    input: Option<Value>,
}

#[derive(Debug, Serialize)]
struct ClaudeTool {
    name: String,
    description: String,
    input_schema: Value,
}

#[derive(Debug, Serialize)]
struct ClaudeToolChoice {
    #[serde(rename = "type")]
    kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
}

impl ClaudeToolChoice {
    fn tool(name: &str) -> Self {
        Self {
            kind: "tool".to_string(),
            name: Some(name.to_string()),
        }
    }
}

#[derive(Debug, Serialize, Clone)]
struct ClaudeTextBlock {
    #[serde(rename = "type")]
    kind: String,
    text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_control: Option<ClaudeCacheControl>,
}

impl ClaudeTextBlock {
    fn plain(text: String) -> Self {
        Self {
            kind: "text".to_string(),
            text,
            cache_control: None,
        }
    }

    fn cacheable(text: String, ttl: &str) -> Self {
        Self {
            kind: "text".to_string(),
            text,
            cache_control: Some(ClaudeCacheControl::ephemeral(ttl)),
        }
    }
}

#[derive(Debug, Serialize, Clone)]
struct ClaudeCacheControl {
    #[serde(rename = "type")]
    kind: String,
    ttl: String,
}

impl ClaudeCacheControl {
    fn ephemeral(ttl: &str) -> Self {
        Self {
            kind: "ephemeral".to_string(),
            ttl: ttl.to_string(),
        }
    }
}

#[derive(Debug, Serialize)]
struct QwenChatCompletionsRequest {
    model: String,
    temperature: f64,
    max_tokens: u32,
    messages: Vec<QwenChatMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_format: Option<QwenResponseFormat>,
    #[serde(skip_serializing_if = "Option::is_none")]
    enable_thinking: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning: Option<OpenAiCompatibleReasoningConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream: Option<bool>,
}

#[derive(Debug, Serialize)]
struct QwenChatMessage {
    role: String,
    content: String,
}

#[derive(Debug, Serialize)]
struct OpenAiCompatibleReasoningConfig {
    effort: String,
}

#[derive(Debug, Deserialize)]
struct QwenChatCompletionsResponse {
    choices: Vec<QwenChatChoice>,
}

#[derive(Debug, Deserialize)]
struct QwenChatChoice {
    message: QwenChatChoiceMessage,
}

#[derive(Debug, Deserialize)]
struct QwenChatChoiceMessage {
    content: String,
}

#[derive(Debug, Serialize)]
struct QwenResponseFormat {
    #[serde(rename = "type")]
    kind: String,
    json_schema: QwenResponseJsonSchema,
}

#[derive(Debug, Serialize)]
struct QwenResponseJsonSchema {
    name: String,
    strict: bool,
    schema: Value,
}

#[derive(Debug, Serialize)]
struct GrokResponsesRequest {
    model: String,
    temperature: f64,
    max_output_tokens: u32,
    store: bool,
    input: Vec<GrokResponsesInputMessage>,
    text: GrokResponsesTextConfig,
}

#[derive(Debug, Serialize)]
struct GrokResponsesInputMessage {
    role: String,
    content: String,
}

#[derive(Debug, Deserialize)]
struct GrokResponsesApiResponse {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    usage: Option<Value>,
    #[serde(default)]
    incomplete_details: Option<Value>,
    #[serde(default)]
    output: Vec<GrokResponseOutputItem>,
}

#[derive(Debug, Deserialize)]
struct GrokResponseOutputItem {
    #[serde(default)]
    content: Vec<GrokResponseContentItem>,
}

#[derive(Debug, Deserialize)]
struct GrokResponseContentItem {
    #[serde(default)]
    #[serde(rename = "type")]
    kind: Option<String>,
    #[serde(default)]
    text: Option<String>,
}

#[derive(Debug, Serialize)]
struct OpenRouterChatCompletionsRequest {
    model: String,
    temperature: f64,
    max_tokens: u32,
    messages: Vec<OpenRouterChatMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning: Option<OpenRouterReasoning>,
}

#[derive(Debug, Serialize)]
struct OpenRouterChatMessage {
    role: String,
    /// Either a plain JSON string (user message) or an array of content parts
    /// with optional `cache_control` annotations (system message).
    content: Value,
}

#[derive(Debug, Serialize)]
struct OpenRouterReasoning {
    enabled: bool,
}

#[derive(Debug, Deserialize)]
struct OpenRouterChatCompletionsResponse {
    #[serde(default)]
    choices: Vec<OpenRouterChatChoice>,
    #[serde(default)]
    usage: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct OpenRouterChatChoice {
    #[serde(default)]
    message: Option<OpenRouterChatChoiceMessage>,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OpenRouterChatChoiceMessage {
    #[serde(default)]
    content: Value,
}

#[derive(Debug, Serialize)]
struct GrokResponsesTextConfig {
    format: GrokResponsesFormat,
}

#[derive(Debug, Serialize)]
struct GrokResponsesFormat {
    #[serde(rename = "type")]
    kind: String,
    name: String,
    strict: bool,
    schema: Value,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GeminiGenerateContentRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    system_instruction: Option<GeminiInstruction>,
    contents: Vec<GeminiContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    generation_config: Option<GeminiGenerationConfig>,
}

#[derive(Debug, Serialize)]
struct GeminiInstruction {
    parts: Vec<GeminiPart>,
}

#[derive(Debug, Serialize)]
struct GeminiContent {
    role: String,
    parts: Vec<GeminiPart>,
}

#[derive(Debug, Serialize)]
struct GeminiPart {
    text: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GeminiGenerationConfig {
    temperature: f64,
    max_output_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_schema: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiGenerateContentResponse {
    #[serde(default)]
    candidates: Vec<GeminiCandidate>,
    #[serde(default)]
    usage_metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiCandidate {
    content: Option<GeminiContentResponse>,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GeminiContentResponse {
    #[serde(default)]
    parts: Vec<GeminiPartResponse>,
}

#[derive(Debug, Deserialize)]
struct GeminiPartResponse {
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ClaudeUsage {
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    cache_read_input_tokens: Option<u64>,
    #[serde(default)]
    cache_creation_input_tokens: Option<u64>,
    #[serde(default)]
    cache_creation: Option<ClaudeUsageCacheCreation>,
}

#[derive(Debug, Deserialize)]
struct ClaudeUsageCacheCreation {
    #[serde(default)]
    ephemeral_1h_input_tokens: Option<u64>,
}

#[derive(Debug, Serialize)]
struct ClaudeBatchCreateRequest {
    requests: Vec<ClaudeBatchRequest>,
}

#[derive(Debug, Serialize)]
struct ClaudeBatchRequest {
    custom_id: String,
    params: ClaudeMessageRequest,
}

#[derive(Debug, Deserialize)]
struct ClaudeBatchEnvelope {
    id: String,
    processing_status: String,
    results_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ClaudeBatchResultLine {
    custom_id: String,
    result: ClaudeBatchResult,
}

#[derive(Debug, Deserialize)]
struct ClaudeBatchResult {
    #[serde(rename = "type")]
    kind: String,
    message: Option<ClaudeMessageResponse>,
    error: Option<ClaudeApiErrorResponse>,
}

#[derive(Debug, Deserialize)]
struct ClaudeApiErrorResponse {
    #[serde(rename = "type")]
    kind: String,
    error: Option<ClaudeApiErrorInner>,
    request_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ClaudeApiErrorInner {
    #[serde(rename = "type")]
    kind: String,
    message: String,
}
