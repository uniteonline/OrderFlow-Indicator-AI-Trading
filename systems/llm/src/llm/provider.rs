use crate::app::config::{
    ClaudeApiConfig, CustomLlmApiConfig, GeminiApiConfig, GrokApiConfig, LlmModelConfig,
    OpenRouterApiConfig, QwenApiConfig, RootConfig,
};
use crate::execution::binance::TradingStateSnapshot;
use crate::llm::{filter, prompt};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use futures_util::future::join_all;
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Instant;
use tokio::time::{sleep, Duration};
use tracing::warn;
use uuid::Uuid;

const CLAUDE_EXTENDED_CACHE_TTL_BETA: &str = "extended-cache-ttl-2025-04-11";
const CLAUDE_DECISION_TOOL_NAME: &str = "emit_decision";
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
    /// Recomputed from signed position size, entry price, and latest mark price.
    pub pnl_by_latest_price: f64,
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
    /// Actual live TP order trigger price from Binance open orders (None if no live TP is found).
    pub current_tp_price: Option<f64>,
    /// Actual live SL order trigger price from Binance open orders (None if no live SL is found).
    pub current_sl_price: Option<f64>,
    /// Shadow/reference TP from lifecycle context or original entry context. This is not proof
    /// that a live TP order currently exists on Binance.
    pub planned_tp_price: Option<f64>,
    pub planned_tp_source: Option<String>,
    /// Shadow/reference SL from lifecycle context or original entry context. This is not proof
    /// that a live SL order currently exists on Binance.
    pub planned_sl_price: Option<f64>,
    pub planned_sl_source: Option<String>,
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

#[derive(Debug, Clone, Copy, Serialize)]
pub struct FinalizeStageContext {
    pub stage1_scan_ts_bucket: DateTime<Utc>,
    pub stage2_core_ts_bucket: DateTime<Utc>,
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
    latency_ms: u64,
) -> Value {
    json!({
        "stage": entry_stage_name(prompt::EntryPromptStage::Scan),
        "provider": provider,
        "latency_ms": latency_ms,
        "raw_response_text": raw_text,
        "parsed_scan": parsed_scan.cloned(),
    })
}

fn build_entry_finalize_trace_event(
    input: &ModelInvocationInput,
    prior_scan: &Value,
    latency_ms: Option<u64>,
) -> Value {
    json!({
        "stage": entry_stage_name(prompt::EntryPromptStage::Finalize),
        "stage_mode": if input.pending_order_mode {
            "pending_order"
        } else if input.management_mode {
            "management"
        } else {
            "entry"
        },
        "latency_ms": latency_ms,
        "scan_15m_trend": prior_scan.pointer("/15m/trend").cloned().unwrap_or(Value::Null),
        "scan_4h_trend": prior_scan.pointer("/4h/trend").cloned().unwrap_or(Value::Null),
        "scan_1d_trend": prior_scan.pointer("/1d/trend").cloned().unwrap_or(Value::Null),
        "stage_1_scan_ts_bucket": prior_scan.get("stage_1_scan_ts_bucket").cloned().unwrap_or(Value::Null),
        "stage_2_core_ts_bucket": prior_scan
            .get("stage_2_core_ts_bucket")
            .cloned()
            .unwrap_or_else(|| Value::String(input.ts_bucket.to_rfc3339())),
        "stage_1_to_stage_2_gap_seconds": prior_scan
            .get("stage_1_to_stage_2_gap_seconds")
            .cloned()
            .unwrap_or(Value::Null),
        "stage_1_to_stage_2_gap_minutes": prior_scan
            .get("stage_1_to_stage_2_gap_minutes")
            .cloned()
            .unwrap_or(Value::Null),
        "stage_2_filter": "core",
        "raw_indicator_count": input.indicators.as_object().map(|obj| obj.len()).unwrap_or(0),
        "missing_indicator_codes": &input.missing_indicator_codes,
    })
}

fn is_entry_mode(input: &ModelInvocationInput) -> bool {
    !input.management_mode && !input.pending_order_mode
}

fn elapsed_ms_u64(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
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
        &input.symbol,
    );
    let mut user = format!(
        "{}{}",
        prompt::user_prompt_prefix(input.management_mode, input.pending_order_mode, entry_stage,),
        input_json
    );
    let sanitized_prior_scan = if matches!(entry_stage, prompt::EntryPromptStage::Finalize) {
        Some(sanitize_scan_for_stage2(prior_scan.ok_or_else(|| {
            provider_failure_plain(anyhow!("finalize stage requires prior market scan json"))
        })?))
    } else {
        None
    };
    if matches!(entry_stage, prompt::EntryPromptStage::Finalize) {
        if let Some(scan_ts_bucket) = sanitized_prior_scan
            .as_ref()
            .and_then(|value| value.get("stage_1_scan_ts_bucket"))
            .and_then(Value::as_str)
        {
            user.push_str("\n\nSTAGE_1_SCAN_TS_BUCKET: ");
            user.push_str(scan_ts_bucket);
        }
        if let Some(core_ts_bucket) = sanitized_prior_scan
            .as_ref()
            .and_then(|value| value.get("stage_2_core_ts_bucket"))
            .and_then(Value::as_str)
        {
            user.push_str("\nSTAGE_2_CORE_TS_BUCKET: ");
            user.push_str(core_ts_bucket);
        }
        if let Some(gap_minutes) = sanitized_prior_scan
            .as_ref()
            .and_then(|value| value.get("stage_1_to_stage_2_gap_minutes"))
            .and_then(Value::as_f64)
        {
            user.push_str("\nSTAGE_1_TO_STAGE_2_GAP_MINUTES: ");
            user.push_str(&format!("{gap_minutes:.2}"));
        }
        if let Some(gap_seconds) = sanitized_prior_scan
            .as_ref()
            .and_then(|value| value.get("stage_1_to_stage_2_gap_seconds"))
            .and_then(Value::as_i64)
        {
            user.push_str("\nSTAGE_1_TO_STAGE_2_GAP_SECONDS: ");
            user.push_str(&gap_seconds.to_string());
        }
        let scan_json = serde_json::to_string(
            sanitized_prior_scan
                .as_ref()
                .expect("finalize stage should produce sanitized scan"),
        )
        .context("serialize market scan json")
        .map_err(provider_failure_plain)?;
        user.push_str("\n\nSTAGE_1_MARKET_SCAN_JSON:\n");
        user.push_str(&scan_json);
    }
    let prompt_input_capture = Some(EntryStagePromptInputCapture {
        stage: entry_stage_name(entry_stage).to_string(),
        prompt_input,
        stage_1_setup_scan_json: if matches!(entry_stage, prompt::EntryPromptStage::Finalize) {
            sanitized_prior_scan
        } else {
            None
        },
    });
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
    if matches!(entry_stage, prompt::EntryPromptStage::Scan) {
        return filter::scan::ScanFilter::serialize_minified_input(input);
    }

    filter::core::CoreFilter::serialize_prompt_input_minified(input, entry_stage, prior_scan)
}

#[cfg_attr(not(test), allow(dead_code))]
fn serialize_entry_finalize_input_minified(
    input: &ModelInvocationInput,
    prior_scan: &Value,
) -> Result<String> {
    filter::core::CoreFilter::serialize_finalize_input(input, prior_scan)
}

fn parse_entry_scan_output(provider: &str, raw_text: &str) -> Result<Value, ProviderFailure> {
    let value = parse_json_from_text(raw_text)
        .map(|value| normalize_provider_decision_shape(provider, value, false, false))
        .ok_or_else(|| provider_failure_plain(anyhow!("entry scan output is not valid JSON")))?;
    validate_scan_output(&value).map_err(provider_failure_plain)?;
    Ok(value)
}

fn validate_scan_output(value: &Value) -> Result<()> {
    for tf in ["15m", "4h", "1d"] {
        let tf_obj = value
            .get(tf)
            .and_then(Value::as_object)
            .ok_or_else(|| anyhow!("scan {} is missing or not an object", tf))?;
        let trend = tf_obj
            .get("trend")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("scan {}.trend is missing", tf))?;
        if !matches!(trend, "Bullish" | "Bearish" | "Sideways") {
            return Err(anyhow!(
                "scan {}.trend must be Bullish/Bearish/Sideways",
                tf
            ));
        }
        tf_obj
            .get("range")
            .and_then(Value::as_object)
            .ok_or_else(|| anyhow!("scan {}.range is missing or not an object", tf))?;
        let agreement = tf_obj
            .get("signal_agreement")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("scan {}.signal_agreement is missing", tf))?;
        if !matches!(agreement, "strong" | "mixed" | "conflicted") {
            return Err(anyhow!(
                "scan {}.signal_agreement must be strong/mixed/conflicted",
                tf
            ));
        }
        tf_obj
            .get("supporting_signals")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("scan {}.supporting_signals is missing", tf))?;
        tf_obj
            .get("conflicting_signals")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("scan {}.conflicting_signals is missing", tf))?;
        tf_obj
            .get("opportunity")
            .and_then(Value::as_str)
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| anyhow!("scan {}.opportunity is missing", tf))?;
        tf_obj
            .get("risk")
            .and_then(Value::as_str)
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| anyhow!("scan {}.risk is missing", tf))?;
    }
    let scan_audit = value
        .get("scan_audit")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("scan scan_audit is missing or not an object"))?;
    for tf in ["15m", "4h", "1d"] {
        let tf_audit = scan_audit
            .get(tf)
            .and_then(Value::as_object)
            .ok_or_else(|| anyhow!("scan scan_audit.{} is missing or not an object", tf))?;
        let direction_basis = tf_audit
            .get("direction_basis")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("scan scan_audit.{}.direction_basis is missing", tf))?;
        if !matches!(
            direction_basis,
            "closed_bar_continuation"
                | "live_flow_reversal"
                | "exhaustion_inference"
                | "structural_inference"
                | "mixed"
        ) {
            return Err(anyhow!(
                "scan scan_audit.{}.direction_basis must be closed_bar_continuation/live_flow_reversal/exhaustion_inference/structural_inference/mixed",
                tf
            ));
        }
        tf_audit
            .get("recent_closed_bars_align_with_trend")
            .and_then(Value::as_bool)
            .ok_or_else(|| {
                anyhow!(
                    "scan scan_audit.{}.recent_closed_bars_align_with_trend is missing",
                    tf
                )
            })?;
        tf_audit
            .get("cvd_slope_aligns_with_trend")
            .and_then(Value::as_bool)
            .ok_or_else(|| {
                anyhow!(
                    "scan scan_audit.{}.cvd_slope_aligns_with_trend is missing",
                    tf
                )
            })?;
        tf_audit
            .get("current_partial_bar_aligns_with_trend")
            .and_then(Value::as_bool)
            .ok_or_else(|| {
                anyhow!(
                    "scan scan_audit.{}.current_partial_bar_aligns_with_trend is missing",
                    tf
                )
            })?;
        match tf_audit.get("invalidation_level") {
            Some(Value::Number(_)) | Some(Value::Null) => {}
            _ => {
                return Err(anyhow!(
                    "scan scan_audit.{}.invalidation_level must be a number or null",
                    tf
                ));
            }
        }
        let range_width_vs_atr = tf_audit
            .get("range_width_vs_atr")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("scan scan_audit.{}.range_width_vs_atr is missing", tf))?;
        if !matches!(range_width_vs_atr, "narrow" | "normal" | "wide") {
            return Err(anyhow!(
                "scan scan_audit.{}.range_width_vs_atr must be narrow/normal/wide",
                tf
            ));
        }
    }
    Ok(())
}

fn sanitize_scan_for_stage2(value: &Value) -> Value {
    let mut sanitized = value.clone();
    if let Some(root) = sanitized.as_object_mut() {
        root.remove("market_narrative");
        root.remove("scan_audit");
    }
    for tf in ["15m", "4h", "1d"] {
        if let Some(tf_obj) = sanitized.get_mut(tf).and_then(Value::as_object_mut) {
            tf_obj.remove("story");
        }
    }
    sanitized
}

fn annotate_scan_for_stage2(value: &Value, context: FinalizeStageContext) -> Value {
    let mut annotated = sanitize_scan_for_stage2(value);
    let gap_seconds = context
        .stage2_core_ts_bucket
        .signed_duration_since(context.stage1_scan_ts_bucket)
        .num_seconds();
    let gap_minutes = (gap_seconds as f64) / 60.0;
    if let Some(root) = annotated.as_object_mut() {
        root.insert(
            "stage_1_scan_ts_bucket".to_string(),
            Value::String(context.stage1_scan_ts_bucket.to_rfc3339()),
        );
        root.insert(
            "stage_2_core_ts_bucket".to_string(),
            Value::String(context.stage2_core_ts_bucket.to_rfc3339()),
        );
        root.insert(
            "stage_1_to_stage_2_gap_seconds".to_string(),
            json!(gap_seconds),
        );
        root.insert(
            "stage_1_to_stage_2_gap_minutes".to_string(),
            json!(gap_minutes),
        );
    }
    annotated
}

fn enabled_models_for_default(config: &RootConfig) -> Vec<LlmModelConfig> {
    let default_provider = config.active_default_model();
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
            stage1_reasoning: None,
            stage2_reasoning: None,
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
            stage1_reasoning: None,
            stage2_reasoning: None,
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
            stage1_reasoning: None,
            stage2_reasoning: None,
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
            stage1_reasoning: None,
            stage2_reasoning: None,
            reasoning: None,
        });
    }
    enabled
}

fn no_enabled_models_output(default_provider: &str) -> ModelCallOutput {
    ModelCallOutput {
        model_name: format!("{}_default", default_provider),
        provider: default_provider.to_string(),
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
    }
}

pub async fn invoke_models_scan_stage(
    http_client: &Client,
    loopback_http_client: &Client,
    config: &RootConfig,
    input: &ModelInvocationInput,
) -> Vec<ModelCallOutput> {
    let default_provider = config.active_default_model();
    let prompt_template = config.llm.prompt_template.clone();
    let enabled = enabled_models_for_default(config);
    if enabled.is_empty() {
        return vec![no_enabled_models_output(&default_provider)];
    }

    let mut tasks = Vec::with_capacity(enabled.len());
    for model in enabled {
        let http_client = http_client.clone();
        let loopback_http_client = loopback_http_client.clone();
        let claude_cfg = config.api.claude.clone();
        let qwen_cfg = config.api.qwen.clone();
        let custom_llm_cfg = config.api.custom_llm.clone();
        let gemini_cfg = config.api.gemini.clone();
        let openrouter_cfg = config.api.openrouter.clone();
        let grok_cfg = config.api.grok.clone();
        let input = input.clone();
        let prompt_template = prompt_template.clone();
        tasks.push(async move {
            invoke_one_model_scan_stage(
                &http_client,
                &loopback_http_client,
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

pub async fn invoke_models_finalize_stage(
    http_client: &Client,
    loopback_http_client: &Client,
    config: &RootConfig,
    input: &ModelInvocationInput,
    stage1_scan_outputs: &[ModelCallOutput],
    context: FinalizeStageContext,
) -> Vec<ModelCallOutput> {
    let prompt_template = config.llm.prompt_template.clone();
    let enabled = enabled_models_for_default(config);
    let mut tasks = Vec::new();

    for model in enabled {
        let Some(stage1_output) = stage1_scan_outputs.iter().find(|output| {
            output.model_name == model.name
                && output.provider == model.provider
                && output.model == model.model
        }) else {
            continue;
        };
        let Some(scan_value) = stage1_output.parsed_decision.as_ref() else {
            continue;
        };

        let http_client = http_client.clone();
        let loopback_http_client = loopback_http_client.clone();
        let claude_cfg = config.api.claude.clone();
        let qwen_cfg = config.api.qwen.clone();
        let custom_llm_cfg = config.api.custom_llm.clone();
        let gemini_cfg = config.api.gemini.clone();
        let openrouter_cfg = config.api.openrouter.clone();
        let grok_cfg = config.api.grok.clone();
        let input = input.clone();
        let prompt_template = prompt_template.clone();
        let annotated_scan = annotate_scan_for_stage2(scan_value, context);
        let prior_stage_trace = stage1_output.entry_stage_trace.clone().unwrap_or_default();
        tasks.push(async move {
            invoke_one_model_finalize_stage(
                &http_client,
                &loopback_http_client,
                &claude_cfg,
                &qwen_cfg,
                &custom_llm_cfg,
                &gemini_cfg,
                &openrouter_cfg,
                &grok_cfg,
                model,
                &input,
                &prompt_template,
                annotated_scan,
                prior_stage_trace,
            )
            .await
        });
    }

    join_all(tasks).await
}

async fn invoke_provider_stage(
    http_client: &Client,
    loopback_http_client: &Client,
    claude_cfg: &ClaudeApiConfig,
    qwen_cfg: &QwenApiConfig,
    custom_llm_cfg: &CustomLlmApiConfig,
    gemini_cfg: &GeminiApiConfig,
    openrouter_cfg: &OpenRouterApiConfig,
    grok_cfg: &GrokApiConfig,
    model: &LlmModelConfig,
    input: &ModelInvocationInput,
    prompt_template: &str,
    entry_stage: prompt::EntryPromptStage,
    prior_scan: Option<&Value>,
) -> Result<ProviderSuccess, ProviderFailure> {
    match model.provider.to_ascii_lowercase().as_str() {
        "claude" => {
            invoke_claude_batch(
                http_client,
                claude_cfg,
                model,
                input,
                prompt_template,
                entry_stage,
                prior_scan,
            )
            .await
        }
        "qwen" => {
            invoke_qwen_stage(
                http_client,
                qwen_cfg,
                model,
                input,
                prompt_template,
                entry_stage,
                prior_scan,
            )
            .await
        }
        "custom_llm" => {
            invoke_custom_llm_stage(
                http_client,
                loopback_http_client,
                custom_llm_cfg,
                model,
                input,
                prompt_template,
                entry_stage,
                prior_scan,
            )
            .await
        }
        "gemini" => {
            if model.should_use_openrouter() {
                invoke_gemini_via_openrouter(
                    http_client,
                    openrouter_cfg,
                    model,
                    input,
                    prompt_template,
                    entry_stage,
                    prior_scan,
                )
                .await
            } else {
                invoke_gemini_direct(
                    http_client,
                    gemini_cfg,
                    model,
                    input,
                    prompt_template,
                    entry_stage,
                    prior_scan,
                )
                .await
            }
        }
        "grok" => {
            invoke_grok_stage(
                http_client,
                grok_cfg,
                model,
                input,
                prompt_template,
                entry_stage,
                prior_scan,
            )
            .await
        }
        other => Err(ProviderFailure {
            error: anyhow!("unsupported provider: {}", other),
            trace: ProviderTrace::default(),
        }),
    }
}

async fn invoke_one_model_scan_stage(
    http_client: &Client,
    loopback_http_client: &Client,
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

    let output = invoke_provider_stage(
        http_client,
        loopback_http_client,
        claude_cfg,
        qwen_cfg,
        custom_llm_cfg,
        gemini_cfg,
        openrouter_cfg,
        grok_cfg,
        &model,
        input,
        prompt_template,
        prompt::EntryPromptStage::Scan,
        None,
    )
    .await;

    match output {
        Ok(success) => {
            let ProviderSuccess {
                raw_text,
                mut trace,
            } = success;
            let scan_latency_ms = elapsed_ms_u64(started);
            match parse_entry_scan_output(&provider, &raw_text) {
                Ok(scan_value) => {
                    trace.push_entry_stage_event(build_entry_scan_trace_event(
                        &provider,
                        &raw_text,
                        Some(&scan_value),
                        scan_latency_ms,
                    ));
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
                        parsed_decision: Some(scan_value),
                        validation_warning: None,
                        entry_stage_trace,
                        entry_stage_prompt_inputs,
                        error: None,
                    }
                }
                Err(failure) => {
                    trace.push_entry_stage_event(build_entry_scan_trace_event(
                        &provider,
                        &raw_text,
                        None,
                        scan_latency_ms,
                    ));
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
                        parsed_decision: None,
                        validation_warning: None,
                        entry_stage_trace,
                        entry_stage_prompt_inputs,
                        error: Some(format!(
                            "{:#}",
                            failure.error.context("market scan stage failed")
                        )),
                    }
                }
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

async fn invoke_one_model_finalize_stage(
    http_client: &Client,
    loopback_http_client: &Client,
    claude_cfg: &ClaudeApiConfig,
    qwen_cfg: &QwenApiConfig,
    custom_llm_cfg: &CustomLlmApiConfig,
    gemini_cfg: &GeminiApiConfig,
    openrouter_cfg: &OpenRouterApiConfig,
    grok_cfg: &GrokApiConfig,
    model: LlmModelConfig,
    input: &ModelInvocationInput,
    prompt_template: &str,
    annotated_scan: Value,
    prior_stage_trace: Vec<Value>,
) -> ModelCallOutput {
    let started = Instant::now();
    let provider = model.provider.clone();
    let model_name = model.name.clone();
    let model_id = model.model.clone();

    let output = invoke_provider_stage(
        http_client,
        loopback_http_client,
        claude_cfg,
        qwen_cfg,
        custom_llm_cfg,
        gemini_cfg,
        openrouter_cfg,
        grok_cfg,
        &model,
        input,
        prompt_template,
        prompt::EntryPromptStage::Finalize,
        Some(&annotated_scan),
    )
    .await;

    let finalize_latency_ms = elapsed_ms_u64(started);
    let finalize_event =
        build_entry_finalize_trace_event(input, &annotated_scan, Some(finalize_latency_ms));
    let prior_stage_latency_ms = prior_stage_trace
        .iter()
        .filter_map(|event| event.get("latency_ms").and_then(Value::as_u64))
        .fold(0u128, |acc, latency| acc.saturating_add(latency as u128));
    let total_latency_ms = prior_stage_latency_ms.saturating_add(started.elapsed().as_millis());

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
            let trace = trace
                .prepend_entry_stage_events(&[finalize_event])
                .prepend_entry_stage_events(&prior_stage_trace);
            let entry_stage_trace = trace.output_entry_stage_trace();
            let entry_stage_prompt_inputs = trace.output_entry_stage_prompt_inputs();
            ModelCallOutput {
                model_name,
                provider,
                model: model_id,
                latency_ms: total_latency_ms,
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
            let trace = trace
                .prepend_entry_stage_events(&[finalize_event])
                .prepend_entry_stage_events(&prior_stage_trace);
            let entry_stage_trace = trace.output_entry_stage_trace();
            let entry_stage_prompt_inputs = trace.output_entry_stage_prompt_inputs();
            ModelCallOutput {
                model_name,
                provider,
                model: model_id,
                latency_ms: total_latency_ms,
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

async fn invoke_custom_llm_stage(
    http_client: &Client,
    loopback_http_client: &Client,
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
        reasoning: custom_llm_reasoning(model, entry_stage),
        stream: None,
    };

    let url = chat_completions_url(&custom_llm_cfg.base_api_url);
    let client = if is_loopback_chat_completions_url(&url) {
        loopback_http_client
    } else {
        http_client
    };
    let is_loopback = is_loopback_chat_completions_url(&url);
    let stage_name = entry_stage_name(entry_stage);

    let max_attempts = 2u32;
    let mut last_err: Option<ProviderFailure> = None;
    for attempt in 0..max_attempts {
        if attempt > 0 {
            tracing::warn!(
                "custom_llm retry attempt {} after retryable error stage={}",
                attempt,
                stage_name
            );
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
        let mut request = client
            .post(&url)
            .bearer_auth(custom_llm_cfg.resolved_api_key())
            .json(&req);
        if is_loopback {
            request = request.header("Connection", "close");
        }
        let response = match request.send().await {
            Ok(r) => r,
            Err(e) => {
                last_err = Some(ProviderFailure {
                    error: anyhow::Error::from(e).context("call custom_llm chat completions api"),
                    trace: trace.clone(),
                });
                continue;
            }
        };

        let status = response.status();
        if status.is_server_error() {
            let body = response.text().await.unwrap_or_default();
            tracing::warn!(
                "custom_llm chat completions server error stage={} status={} body={}",
                stage_name,
                status,
                body
            );
            last_err = Some(ProviderFailure {
                error: anyhow!(
                    "custom_llm chat completions failed status={} body={}",
                    status,
                    body
                ),
                trace: trace.clone(),
            });
            continue;
        }

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

        let response_text = match response.text().await {
            Ok(text) => text,
            Err(error) => {
                tracing::warn!(
                    "custom_llm chat completions body read failed stage={} status={} error={}",
                    stage_name,
                    status,
                    error
                );
                last_err = Some(ProviderFailure {
                    error: anyhow::Error::from(error)
                        .context("read custom_llm chat completions response body"),
                    trace: trace.clone(),
                });
                continue;
            }
        };

        if response_text.trim().is_empty() {
            tracing::warn!(
                "custom_llm chat completions returned empty body stage={} status={}",
                stage_name,
                status
            );
            last_err = Some(ProviderFailure {
                error: anyhow!("custom_llm chat completions response body is empty"),
                trace: trace.clone(),
            });
            continue;
        }

        let body: QwenChatCompletionsResponse =
            match serde_json::from_str::<QwenChatCompletionsResponse>(&response_text) {
                Ok(body) => body,
                Err(error) => {
                    tracing::warn!(
                    "custom_llm chat completions decode failed stage={} status={} error={} body={}",
                    stage_name,
                    status,
                    error,
                    response_text
                );
                    last_err = Some(ProviderFailure {
                        error: anyhow::Error::from(error)
                            .context("decode custom_llm chat completions response body"),
                        trace: trace.clone(),
                    });
                    continue;
                }
            };

        let text = body
            .choices
            .first()
            .map(|c| c.message.content.trim().to_string())
            .unwrap_or_default();
        if text.is_empty() {
            tracing::warn!(
                "custom_llm chat completions response text is empty stage={} status={}",
                stage_name,
                status
            );
            last_err = Some(ProviderFailure {
                error: anyhow!("custom_llm chat completions response text is empty"),
                trace: trace.clone(),
            });
            continue;
        }

        return Ok(ProviderSuccess {
            raw_text: text,
            trace,
        });
    }

    Err(last_err.unwrap_or_else(|| ProviderFailure {
        error: anyhow!(
            "custom_llm chat completions failed after {} attempts",
            max_attempts
        ),
        trace,
    }))
}

fn is_loopback_chat_completions_url(url: &str) -> bool {
    let Ok(parsed) = Url::parse(url) else {
        return false;
    };
    matches!(parsed.host_str(), Some("127.0.0.1" | "localhost" | "::1"))
}

fn chat_completions_url(base_api_url: &str) -> String {
    let trimmed = base_api_url.trim().trim_end_matches('/');
    if trimmed.ends_with("/chat/completions") {
        trimmed.to_string()
    } else {
        format!("{}/chat/completions", trimmed)
    }
}

fn custom_llm_reasoning(
    model: &LlmModelConfig,
    entry_stage: prompt::EntryPromptStage,
) -> Option<OpenAiCompatibleReasoningConfig> {
    let effort =
        model.reasoning_for_stage(matches!(entry_stage, prompt::EntryPromptStage::Scan))?;
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
            name: if matches!(entry_stage, prompt::EntryPromptStage::Scan) {
                "market_scan".to_string()
            } else if pending_order_mode {
                "pending_order_decision".to_string()
            } else if management_mode {
                "management_decision".to_string()
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
            name: if matches!(entry_stage, prompt::EntryPromptStage::Scan) {
                "market_scan".to_string()
            } else if pending_order_mode {
                "pending_order_decision".to_string()
            } else if management_mode {
                "management_decision".to_string()
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

fn qwen_output_contract(
    management_mode: bool,
    pending_order_mode: bool,
    entry_stage: prompt::EntryPromptStage,
    prompt_template: &str,
) -> String {
    if matches!(entry_stage, prompt::EntryPromptStage::Scan) {
        if is_medium_large(prompt_template) {
            "\n\nQWEN OUTPUT CONTRACT:\n- Return exactly one JSON object.\n- No extra top-level keys.\n- Top-level keys must be `15m`, `4h`, `1d`, and `scan_audit`.\n- Each timeframe key must include `trend`, `signal_agreement`, `range`, `supporting_signals`, `conflicting_signals`, `opportunity`, and `risk`.\n- `scan_audit` must include `15m`, `4h`, and `1d`, and each audit object must include `direction_basis`, `recent_closed_bars_align_with_trend`, `cvd_slope_aligns_with_trend`, `current_partial_bar_aligns_with_trend`, `invalidation_level`, and `range_width_vs_atr`.\n".to_string()
        } else {
            "\n\nQWEN OUTPUT CONTRACT:\n- Return exactly one JSON object.\n- No extra top-level keys.\n- Top-level keys must be `15m`, `4h`, `1d`, and `scan_audit`.\n- Each timeframe key must include `trend`, `signal_agreement`, `range`, `supporting_signals`, `conflicting_signals`, `opportunity`, and `risk`.\n- `scan_audit` must include `15m`, `4h`, and `1d`, and each audit object must include `direction_basis`, `recent_closed_bars_align_with_trend`, `cvd_slope_aligns_with_trend`, `current_partial_bar_aligns_with_trend`, `invalidation_level`, and `range_width_vs_atr`.\n".to_string()
        }
    } else if pending_order_mode {
        "\n\nQWEN OUTPUT CONTRACT:\n- Return exactly one JSON object.\n- Top-level keys must be `reason` and `params`. `analysis` and `self_check` may be present as extra objects.\n- `reason` must be a non-empty top-level string. Do not place `reason` inside `analysis`.\n- `params` must contain exactly: `entry`, `tp`, `sl`, `leverage` — each a number or null.\n- Set all params to null if there is no valid setup.\n".to_string()
    } else if management_mode {
        "\n\nQWEN OUTPUT CONTRACT:\n- Return exactly one JSON object.\n- Top-level keys must be `decision`, `reason`, `management_context`, and `params`. `analysis` may be present as an extra object.\n- `reason` must be a non-empty top-level string.\n- `management_context` must include `direction_state`, `ltf_move_meaning`, `sl_noise_risk_15m`, `tp_state`, and `key_condition`.\n- Allowed decisions: VALID, INVALID, ADJUST.\n- `params` must always be present.\n- For VALID: keep `params` present; action fields may be null.\n- For INVALID: set `params.close_price` to a number or null.\n- For ADJUST: set `params.adjust_fields` (array: [\"tp\"], [\"sl\"], [\"tp\",\"sl\"], [\"add\"], or [\"reduce\"]), and corresponding values: `new_tp`/`new_sl` for tp/sl adjustments, `qty_ratio` (number 0-1) for add/reduce.\n".to_string()
    } else {
        "\n\nQWEN OUTPUT CONTRACT:\n- Return exactly one JSON object.\n- Keep `reason` as a top-level string. Do not place `reason` inside `analysis`.\n- Top-level keys must be `decision`, `reason`, `decision_context`, and `params`. `analysis` and `self_check` may be present as extra objects.\n- `decision_context` must include `thesis_flow_alignment`, `entry_readiness`, `entry_exposure`, and `key_condition`.\n- Allowed entry decisions: LONG, SHORT, NO_TRADE. Never use HOLD in entry mode.\n- For LONG or SHORT, params must include `entry`, `tp`, `sl`, `leverage`, and `horizon`.\n- For NO_TRADE, set `params.entry`, `params.tp`, `params.sl`, `params.leverage`, and `params.horizon` to null.\n".to_string()
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
    if matches!(entry_stage, prompt::EntryPromptStage::Scan) {
        if is_medium_large(prompt_template) {
            ml_qwen_entry_scan_schema()
        } else {
            qwen_entry_scan_response_schema()
        }
    } else if pending_order_mode {
        qwen_pending_order_response_schema()
    } else if management_mode {
        if is_medium_large(prompt_template) {
            ml_qwen_management_schema()
        } else {
            qwen_management_response_schema()
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
    if matches!(entry_stage, prompt::EntryPromptStage::Scan) {
        if is_medium_large(prompt_template) {
            ml_custom_llm_entry_scan_schema()
        } else {
            custom_llm_entry_scan_response_schema()
        }
    } else if pending_order_mode {
        custom_llm_pending_order_response_schema()
    } else if management_mode {
        if is_medium_large(prompt_template) {
            ml_custom_llm_management_schema()
        } else {
            custom_llm_management_response_schema()
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
        "required": ["15m", "4h", "1d", "scan_audit"],
        "properties": {
            "15m": scan_timeframe_schema_openai(),
            "4h": scan_timeframe_schema_openai(),
            "1d": scan_timeframe_schema_openai(),
            "scan_audit": scan_audit_schema_openai()
        }
    })
}

fn management_context_schema_openai() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "direction_state",
            "ltf_move_meaning",
            "sl_noise_risk_15m",
            "tp_state",
            "key_condition"
        ],
        "properties": {
            "direction_state": {
                "type": "string",
                "enum": ["aligned", "challenged", "reversed"]
            },
            "ltf_move_meaning": {
                "type": "string",
                "enum": ["noise", "caution", "invalidation"]
            },
            "sl_noise_risk_15m": {
                "type": "string",
                "enum": ["low", "medium", "high"]
            },
            "tp_state": {
                "type": "string",
                "enum": ["keep", "revise_closer", "revise_farther", "obsolete"]
            },
            "key_condition": {
                "type": "string",
                "minLength": 1
            }
        }
    })
}

fn management_context_schema_gemini() -> Value {
    json!({
        "type": "OBJECT",
        "properties": {
            "direction_state": {
                "type": "STRING",
                "enum": ["aligned", "challenged", "reversed"]
            },
            "ltf_move_meaning": {
                "type": "STRING",
                "enum": ["noise", "caution", "invalidation"]
            },
            "sl_noise_risk_15m": {
                "type": "STRING",
                "enum": ["low", "medium", "high"]
            },
            "tp_state": {
                "type": "STRING",
                "enum": ["keep", "revise_closer", "revise_farther", "obsolete"]
            },
            "key_condition": { "type": "STRING" }
        },
        "required": [
            "direction_state",
            "ltf_move_meaning",
            "sl_noise_risk_15m",
            "tp_state",
            "key_condition"
        ]
    })
}

fn qwen_entry_response_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": true,
        "required": ["decision", "reason", "decision_context", "params"],
        "properties": {
            "decision": {
                "type": "string",
                "enum": ["LONG", "SHORT", "NO_TRADE"]
            },
            "reason": {
                "type": "string",
                "minLength": 1
            },
            "decision_context": {
                "type": "object",
                "additionalProperties": false,
                "required": ["thesis_flow_alignment", "entry_readiness", "entry_exposure", "key_condition"],
                "properties": {
                    "thesis_flow_alignment": {
                        "type": "string",
                        "enum": ["aligned", "mixed", "opposed"]
                    },
                    "entry_readiness": {
                        "type": "string",
                        "enum": ["ready", "developing", "not_ready"]
                    },
                    "entry_exposure": {
                        "type": "string",
                        "enum": ["favorable", "vulnerable", "poor"]
                    },
                    "key_condition": {
                        "type": "string",
                        "minLength": 1
                    }
                }
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
        "required": ["15m", "4h", "1d", "scan_audit"],
        "properties": {
            "15m": scan_timeframe_schema_openai(),
            "4h": scan_timeframe_schema_openai(),
            "1d": scan_timeframe_schema_openai(),
            "scan_audit": scan_audit_schema_openai()
        }
    })
}

fn custom_llm_entry_response_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["decision", "reason", "decision_context", "params"],
        "properties": {
            "decision": {
                "type": "string",
                "enum": ["LONG", "SHORT", "NO_TRADE"]
            },
            "reason": {
                "type": "string",
                "minLength": 1
            },
            "decision_context": {
                "type": "object",
                "additionalProperties": false,
                "required": ["thesis_flow_alignment", "entry_readiness", "entry_exposure", "key_condition"],
                "properties": {
                    "thesis_flow_alignment": {
                        "type": "string",
                        "enum": ["aligned", "mixed", "opposed"]
                    },
                    "entry_readiness": {
                        "type": "string",
                        "enum": ["ready", "developing", "not_ready"]
                    },
                    "entry_exposure": {
                        "type": "string",
                        "enum": ["favorable", "vulnerable", "poor"]
                    },
                    "key_condition": {
                        "type": "string",
                        "minLength": 1
                    }
                }
            },
            "params": {
                "type": "object",
                "additionalProperties": false,
                "required": ["entry", "tp", "sl", "leverage", "horizon"],
                "properties": {
                    "entry": {"type": ["number", "null"]},
                    "tp": {"type": ["number", "null"]},
                    "sl": {"type": ["number", "null"]},
                    "leverage": {"type": ["number", "null"]},
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
        "required": ["decision", "reason", "management_context", "params"],
        "properties": {
            "decision": {
                "type": "string",
                "enum": ["VALID", "INVALID", "ADJUST"]
            },
            "reason": {
                "type": "string",
                "minLength": 1
            },
            "management_context": management_context_schema_openai(),
            "params": {
                "type": "object",
                "additionalProperties": true
            },
            "analysis": {
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
        "required": ["decision", "reason", "management_context", "params"],
        "properties": {
            "decision": {
                "type": "string",
                "enum": ["VALID", "INVALID", "ADJUST"]
            },
            "reason": {
                "type": "string",
                "minLength": 1
            },
            "management_context": management_context_schema_openai(),
            "params": {
                "type": "object",
                "additionalProperties": false,
                "required": ["close_price", "adjust_fields", "qty_ratio", "new_tp", "new_sl"],
                "properties": {
                    "close_price":    {"type": ["number", "null"]},
                    "adjust_fields":  {"type": ["array", "null"], "items": {"type": "string", "enum": ["tp", "sl", "add", "reduce"]}},
                    "qty_ratio":      {"type": ["number", "null"]},
                    "new_tp":         {"type": ["number", "null"]},
                    "new_sl":         {"type": ["number", "null"]}
                }
            }
        }
    })
}

fn qwen_pending_order_response_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": true,
        "required": ["reason", "params"],
        "properties": {
            "reason": {
                "type": "string",
                "minLength": 1
            },
            "params": {
                "type": "object",
                "additionalProperties": true
            }
        }
    })
}

fn custom_llm_pending_order_response_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["reason", "params"],
        "properties": {
            "reason": {
                "type": "string",
                "minLength": 1
            },
            "params": {
                "type": "object",
                "additionalProperties": false,
                "required": ["entry", "tp", "sl", "leverage"],
                "properties": {
                    "entry":    {"type": ["number", "null"]},
                    "tp":       {"type": ["number", "null"]},
                    "sl":       {"type": ["number", "null"]},
                    "leverage": {"type": ["number", "null"]}
                }
            }
        }
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
            name: if matches!(entry_stage, prompt::EntryPromptStage::Scan) {
                "market_scan".to_string()
            } else if pending_order_mode {
                "pending_order_decision".to_string()
            } else if management_mode {
                "management_decision".to_string()
            } else {
                "entry_decision".to_string()
            },
            strict: true,
            schema: if matches!(entry_stage, prompt::EntryPromptStage::Scan) {
                if is_medium_large(prompt_template) {
                    ml_grok_entry_scan_schema()
                } else {
                    grok_entry_scan_response_schema()
                }
            } else if pending_order_mode {
                grok_pending_order_response_schema()
            } else if management_mode {
                if is_medium_large(prompt_template) {
                    ml_grok_management_schema()
                } else {
                    grok_management_response_schema()
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
        "required": ["15m", "4h", "1d", "scan_audit"],
        "properties": {
            "15m": scan_timeframe_schema_grok(),
            "4h": scan_timeframe_schema_grok(),
            "1d": scan_timeframe_schema_grok(),
            "scan_audit": scan_audit_schema_openai()
        }
    })
}

fn grok_entry_response_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["analysis", "decision", "decision_context", "params", "reason"],
        "properties": {
            "analysis": {
                "type": "object",
                "additionalProperties": false,
                "required": ["market_thesis", "trade_logic"],
                "properties": {
                    "market_thesis": { "type": "string" },
                    "trade_logic": { "type": "string" }
                }
            },
            "decision": {
                "type": "string",
                "enum": ["LONG", "SHORT", "NO_TRADE"]
            },
            "decision_context": {
                "type": "object",
                "additionalProperties": false,
                "required": ["thesis_flow_alignment", "entry_readiness", "entry_exposure", "key_condition"],
                "properties": {
                    "thesis_flow_alignment": { "type": "string", "enum": ["aligned", "mixed", "opposed"] },
                    "entry_readiness": { "type": "string", "enum": ["ready", "developing", "not_ready"] },
                    "entry_exposure": { "type": "string", "enum": ["favorable", "vulnerable", "poor"] },
                    "key_condition": { "type": "string" }
                }
            },
            "params": {
                "type": "object",
                "additionalProperties": false,
                "required": ["entry", "tp", "sl", "leverage", "horizon"],
                "properties": {
                    "entry": { "type": ["number", "null"] },
                    "tp": { "type": ["number", "null"] },
                    "sl": { "type": ["number", "null"] },
                    "leverage": { "type": ["number", "null"] },
                    "horizon": { "type": ["string", "null"] }
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
        "required": ["decision", "management_context", "params", "reason"],
        "properties": {
            "decision": {
                "type": "string",
                "enum": ["VALID", "INVALID", "ADJUST"]
            },
            "management_context": management_context_schema_openai(),
            "params": {
                "type": "object",
                "additionalProperties": false,
                "required": ["close_price", "adjust_fields", "qty_ratio", "new_tp", "new_sl"],
                "properties": {
                    "close_price":    { "type": ["number", "null"] },
                    "adjust_fields":  { "type": ["array", "null"], "items": { "type": "string", "enum": ["tp", "sl", "add", "reduce"] } },
                    "qty_ratio":      { "type": ["number", "null"] },
                    "new_tp":         { "type": ["number", "null"] },
                    "new_sl":         { "type": ["number", "null"] }
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
        "required": ["params", "reason"],
        "properties": {
            "params": {
                "type": "object",
                "additionalProperties": false,
                "required": ["entry", "tp", "sl", "leverage"],
                "properties": {
                    "entry":    { "type": ["number", "null"] },
                    "tp":       { "type": ["number", "null"] },
                    "sl":       { "type": ["number", "null"] },
                    "leverage": { "type": ["number", "null"] }
                }
            },
            "reason": { "type": "string" }
        }
    })
}

// ── Medium/Large Opportunity schemas ─────────────────────────────────────────

fn scan_timeframe_schema_grok() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "trend",
            "signal_agreement",
            "range",
            "supporting_signals",
            "conflicting_signals",
            "opportunity",
            "risk"
        ],
        "properties": {
            "trend": { "type": "string", "enum": ["Bullish", "Bearish", "Sideways"] },
            "signal_agreement": { "type": "string", "enum": ["strong", "mixed", "conflicted"] },
            "range": {
                "type": "object",
                "additionalProperties": false,
                "required": ["support", "resistance"],
                "properties": {
                    "support": { "type": "number" },
                    "resistance": { "type": "number" }
                }
            },
            "supporting_signals": {
                "type": "array",
                "items": { "type": "string" }
            },
            "conflicting_signals": {
                "type": "array",
                "items": { "type": "string" }
            },
            "opportunity": { "type": "string" },
            "risk": { "type": "string" }
        }
    })
}

fn scan_audit_timeframe_schema_openai() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "direction_basis",
            "recent_closed_bars_align_with_trend",
            "cvd_slope_aligns_with_trend",
            "current_partial_bar_aligns_with_trend",
            "invalidation_level",
            "range_width_vs_atr"
        ],
        "properties": {
            "direction_basis": {
                "type": "string",
                "enum": [
                    "closed_bar_continuation",
                    "live_flow_reversal",
                    "exhaustion_inference",
                    "structural_inference",
                    "mixed"
                ]
            },
            "recent_closed_bars_align_with_trend": { "type": "boolean" },
            "cvd_slope_aligns_with_trend": { "type": "boolean" },
            "current_partial_bar_aligns_with_trend": { "type": "boolean" },
            "invalidation_level": { "type": ["number", "null"] },
            "range_width_vs_atr": {
                "type": "string",
                "enum": ["narrow", "normal", "wide"]
            }
        }
    })
}

fn scan_audit_schema_openai() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["15m", "4h", "1d"],
        "properties": {
            "15m": scan_audit_timeframe_schema_openai(),
            "4h": scan_audit_timeframe_schema_openai(),
            "1d": scan_audit_timeframe_schema_openai()
        }
    })
}

fn scan_timeframe_schema_openai() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "trend",
            "signal_agreement",
            "range",
            "supporting_signals",
            "conflicting_signals",
            "opportunity",
            "risk"
        ],
        "properties": {
            "trend": { "type": "string", "enum": ["Bullish", "Bearish", "Sideways"] },
            "signal_agreement": { "type": "string", "enum": ["strong", "mixed", "conflicted"] },
            "range": {
                "type": "object",
                "additionalProperties": false,
                "required": ["support", "resistance"],
                "properties": {
                    "support": { "type": "number" },
                    "resistance": { "type": "number" }
                }
            },
            "supporting_signals": {
                "type": "array",
                "items": { "type": "string" }
            },
            "conflicting_signals": {
                "type": "array",
                "items": { "type": "string" }
            },
            "opportunity": { "type": "string" },
            "risk": { "type": "string" }
        }
    })
}

fn scan_audit_timeframe_schema_gemini() -> Value {
    json!({
        "type": "OBJECT",
        "properties": {
            "direction_basis": {
                "type": "STRING",
                "enum": [
                    "closed_bar_continuation",
                    "live_flow_reversal",
                    "exhaustion_inference",
                    "structural_inference",
                    "mixed"
                ]
            },
            "recent_closed_bars_align_with_trend": { "type": "BOOLEAN" },
            "cvd_slope_aligns_with_trend": { "type": "BOOLEAN" },
            "current_partial_bar_aligns_with_trend": { "type": "BOOLEAN" },
            "invalidation_level": { "type": "NUMBER", "nullable": true },
            "range_width_vs_atr": {
                "type": "STRING",
                "enum": ["narrow", "normal", "wide"]
            }
        },
        "required": [
            "direction_basis",
            "recent_closed_bars_align_with_trend",
            "cvd_slope_aligns_with_trend",
            "current_partial_bar_aligns_with_trend",
            "invalidation_level",
            "range_width_vs_atr"
        ]
    })
}

fn scan_audit_schema_gemini() -> Value {
    json!({
        "type": "OBJECT",
        "properties": {
            "15m": scan_audit_timeframe_schema_gemini(),
            "4h": scan_audit_timeframe_schema_gemini(),
            "1d": scan_audit_timeframe_schema_gemini()
        },
        "required": ["15m", "4h", "1d"]
    })
}

fn scan_timeframe_schema_gemini() -> Value {
    json!({
        "type": "OBJECT",
        "properties": {
            "trend": { "type": "STRING", "enum": ["Bullish", "Bearish", "Sideways"] },
            "signal_agreement": { "type": "STRING", "enum": ["strong", "mixed", "conflicted"] },
            "range": {
                "type": "OBJECT",
                "properties": {
                    "support": { "type": "NUMBER" },
                    "resistance": { "type": "NUMBER" }
                },
                "required": ["support", "resistance"]
            },
            "supporting_signals": {
                "type": "ARRAY",
                "items": { "type": "STRING" }
            },
            "conflicting_signals": {
                "type": "ARRAY",
                "items": { "type": "STRING" }
            },
            "opportunity": { "type": "STRING" },
            "risk": { "type": "STRING" }
        },
        "required": [
            "trend",
            "signal_agreement",
            "range",
            "supporting_signals",
            "conflicting_signals",
            "opportunity",
            "risk"
        ]
    })
}

fn ml_grok_entry_scan_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["15m", "4h", "1d", "scan_audit"],
        "properties": {
            "15m": scan_timeframe_schema_grok(),
            "4h": scan_timeframe_schema_grok(),
            "1d": scan_timeframe_schema_grok(),
            "scan_audit": scan_audit_schema_openai()
        }
    })
}

fn ml_grok_entry_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["analysis", "decision", "decision_context", "params", "reason"],
        "properties": {
            "analysis": {
                "type": "object",
                "additionalProperties": false,
                "required": ["market_thesis", "trade_logic"],
                "properties": {
                    "market_thesis": { "type": "string" },
                    "trade_logic": { "type": "string" }
                }
            },
            "decision": { "type": "string", "enum": ["LONG", "SHORT", "NO_TRADE"] },
            "decision_context": {
                "type": "object",
                "additionalProperties": false,
                "required": ["thesis_flow_alignment", "entry_readiness", "entry_exposure", "key_condition"],
                "properties": {
                    "thesis_flow_alignment": { "type": "string", "enum": ["aligned", "mixed", "opposed"] },
                    "entry_readiness": { "type": "string", "enum": ["ready", "developing", "not_ready"] },
                    "entry_exposure": { "type": "string", "enum": ["favorable", "vulnerable", "poor"] },
                    "key_condition": { "type": "string" }
                }
            },
            "params": {
                "type": "object",
                "additionalProperties": false,
                "required": ["entry", "tp", "sl", "leverage", "horizon"],
                "properties": {
                    "entry": { "type": ["number", "null"] },
                    "tp": { "type": ["number", "null"] },
                    "sl": { "type": ["number", "null"] },
                    "leverage": { "type": ["number", "null"] },
                    "horizon": { "type": ["string", "null"] }
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
        "required": ["decision", "management_context", "params", "reason"],
        "properties": {
            "decision": { "type": "string", "enum": ["VALID", "INVALID", "ADJUST"] },
            "management_context": management_context_schema_openai(),
            "params": {
                "type": "object",
                "additionalProperties": false,
                "required": ["close_price", "adjust_fields", "qty_ratio", "new_tp", "new_sl"],
                "properties": {
                    "close_price":    { "type": ["number", "null"] },
                    "adjust_fields":  { "type": ["array", "null"], "items": { "type": "string", "enum": ["tp", "sl", "add", "reduce"] } },
                    "qty_ratio":      { "type": ["number", "null"] },
                    "new_tp":         { "type": ["number", "null"] },
                    "new_sl":         { "type": ["number", "null"] }
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
            "15m": scan_timeframe_schema_gemini(),
            "4h": scan_timeframe_schema_gemini(),
            "1d": scan_timeframe_schema_gemini(),
            "scan_audit": scan_audit_schema_gemini()
        },
        "required": ["15m", "4h", "1d", "scan_audit"]
    })
}

fn ml_gemini_entry_schema() -> Value {
    json!({
        "type": "OBJECT",
        "properties": {
            "analysis": {
                "type": "OBJECT",
                "properties": {
                    "market_thesis": { "type": "STRING" },
                    "trade_logic": { "type": "STRING" }
                },
                "required": ["market_thesis", "trade_logic"]
            },
            "decision": { "type": "STRING", "enum": ["LONG", "SHORT", "NO_TRADE"] },
            "decision_context": {
                "type": "OBJECT",
                "properties": {
                    "thesis_flow_alignment": {
                        "type": "STRING",
                        "enum": ["aligned", "mixed", "opposed"]
                    },
                    "entry_readiness": {
                        "type": "STRING",
                        "enum": ["ready", "developing", "not_ready"]
                    },
                    "entry_exposure": {
                        "type": "STRING",
                        "enum": ["favorable", "vulnerable", "poor"]
                    },
                    "key_condition": { "type": "STRING" }
                },
                "required": ["thesis_flow_alignment", "entry_readiness", "entry_exposure", "key_condition"]
            },
            "params": {
                "type": "OBJECT",
                "properties": {
                    "entry": { "type": "NUMBER", "nullable": true },
                    "tp": { "type": "NUMBER", "nullable": true },
                    "sl": { "type": "NUMBER", "nullable": true },
                    "leverage": { "type": "NUMBER", "nullable": true },
                    "horizon": { "type": "STRING", "nullable": true }
                },
                "required": ["entry", "tp", "sl", "leverage", "horizon"]
            },
            "reason": { "type": "STRING" }
        },
        "required": ["analysis", "decision", "decision_context", "params", "reason"]
    })
}

fn ml_gemini_management_schema() -> Value {
    json!({
        "type": "OBJECT",
        "properties": {
            "decision": { "type": "STRING", "enum": ["VALID", "INVALID", "ADJUST"] },
            "management_context": management_context_schema_gemini(),
            "params": {
                "type": "OBJECT",
                "properties": {
                    "close_price":   { "type": "NUMBER", "nullable": true },
                    "adjust_fields": { "type": "ARRAY", "nullable": true, "items": { "type": "STRING" } },
                    "qty_ratio":     { "type": "NUMBER", "nullable": true },
                    "new_tp":        { "type": "NUMBER", "nullable": true },
                    "new_sl":        { "type": "NUMBER", "nullable": true }
                },
                "required": ["close_price", "adjust_fields", "qty_ratio", "new_tp", "new_sl"]
            },
            "reason": { "type": "STRING" }
        },
        "required": ["decision", "management_context", "params", "reason"]
    })
}

fn ml_qwen_entry_scan_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": true,
        "required": ["15m", "4h", "1d", "scan_audit"],
        "properties": {
            "15m": scan_timeframe_schema_openai(),
            "4h": scan_timeframe_schema_openai(),
            "1d": scan_timeframe_schema_openai(),
            "scan_audit": scan_audit_schema_openai()
        }
    })
}

fn ml_qwen_entry_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": true,
        "required": ["decision", "reason", "decision_context", "params"],
        "properties": {
            "decision": { "type": "string", "enum": ["LONG", "SHORT", "NO_TRADE"] },
            "reason": { "type": "string", "minLength": 1 },
            "decision_context": {
                "type": "object",
                "additionalProperties": false,
                "required": ["thesis_flow_alignment", "entry_readiness", "entry_exposure", "key_condition"],
                "properties": {
                    "thesis_flow_alignment": {
                        "type": "string",
                        "enum": ["aligned", "mixed", "opposed"]
                    },
                    "entry_readiness": {
                        "type": "string",
                        "enum": ["ready", "developing", "not_ready"]
                    },
                    "entry_exposure": {
                        "type": "string",
                        "enum": ["favorable", "vulnerable", "poor"]
                    },
                    "key_condition": {
                        "type": "string",
                        "minLength": 1
                    }
                }
            },
            "params": { "type": "object", "additionalProperties": true },
            "analysis": { "type": ["object", "null"], "additionalProperties": true }
        }
    })
}

fn ml_qwen_management_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": true,
        "required": ["decision", "reason", "management_context", "params"],
        "properties": {
            "decision": { "type": "string", "enum": ["VALID", "INVALID", "ADJUST"] },
            "reason": { "type": "string", "minLength": 1 },
            "management_context": management_context_schema_openai(),
            "params": { "type": "object", "additionalProperties": true },
            "analysis": { "type": ["object", "null"], "additionalProperties": true }
        }
    })
}

fn ml_custom_llm_entry_scan_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["15m", "4h", "1d", "scan_audit"],
        "properties": {
            "15m": scan_timeframe_schema_openai(),
            "4h": scan_timeframe_schema_openai(),
            "1d": scan_timeframe_schema_openai(),
            "scan_audit": scan_audit_schema_openai()
        }
    })
}

fn ml_custom_llm_entry_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["decision", "reason", "decision_context", "params"],
        "properties": {
            "decision": { "type": "string", "enum": ["LONG", "SHORT", "NO_TRADE"] },
            "reason": { "type": "string", "minLength": 1 },
            "decision_context": {
                "type": "object",
                "additionalProperties": false,
                "required": ["thesis_flow_alignment", "entry_readiness", "entry_exposure", "key_condition"],
                "properties": {
                    "thesis_flow_alignment": {
                        "type": "string",
                        "enum": ["aligned", "mixed", "opposed"]
                    },
                    "entry_readiness": {
                        "type": "string",
                        "enum": ["ready", "developing", "not_ready"]
                    },
                    "entry_exposure": {
                        "type": "string",
                        "enum": ["favorable", "vulnerable", "poor"]
                    },
                    "key_condition": {
                        "type": "string",
                        "minLength": 1
                    }
                }
            },
            "params": {
                "type": "object",
                "additionalProperties": false,
                "required": ["entry", "tp", "sl", "leverage", "horizon"],
                "properties": {
                    "entry": { "type": ["number", "null"] },
                    "tp": { "type": ["number", "null"] },
                    "sl": { "type": ["number", "null"] },
                    "leverage": { "type": ["number", "null"] },
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
        "required": ["decision", "reason", "management_context", "params"],
        "properties": {
            "decision": { "type": "string", "enum": ["VALID", "INVALID", "ADJUST"] },
            "reason": { "type": "string", "minLength": 1 },
            "management_context": management_context_schema_openai(),
            "params": {
                "type": "object",
                "additionalProperties": false,
                "required": ["close_price", "adjust_fields", "qty_ratio", "new_tp", "new_sl"],
                "properties": {
                    "close_price":    { "type": ["number", "null"] },
                    "adjust_fields":  { "type": ["array", "null"], "items": { "type": "string", "enum": ["tp", "sl", "add", "reduce"] } },
                    "qty_ratio":      { "type": ["number", "null"] },
                    "new_tp":         { "type": ["number", "null"] },
                    "new_sl":         { "type": ["number", "null"] }
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
            "15m": scan_timeframe_schema_gemini(),
            "4h": scan_timeframe_schema_gemini(),
            "1d": scan_timeframe_schema_gemini(),
            "scan_audit": scan_audit_schema_gemini()
        },
        "required": ["15m", "4h", "1d", "scan_audit"]
    })
}

fn gemini_entry_response_schema() -> Value {
    json!({
        "type": "OBJECT",
        "properties": {
            "analysis": {
                "type": "OBJECT",
                "properties": {
                    "market_thesis": { "type": "STRING" },
                    "trade_logic": { "type": "STRING" }
                },
                "required": ["market_thesis", "trade_logic"]
            },
            "decision": {
                "type": "STRING",
                "enum": ["LONG", "SHORT", "NO_TRADE"]
            },
            "decision_context": {
                "type": "OBJECT",
                "properties": {
                    "thesis_flow_alignment": {
                        "type": "STRING",
                        "enum": ["aligned", "mixed", "opposed"]
                    },
                    "entry_readiness": {
                        "type": "STRING",
                        "enum": ["ready", "developing", "not_ready"]
                    },
                    "entry_exposure": {
                        "type": "STRING",
                        "enum": ["favorable", "vulnerable", "poor"]
                    },
                    "key_condition": { "type": "STRING" }
                },
                "required": ["thesis_flow_alignment", "entry_readiness", "entry_exposure", "key_condition"]
            },
            "params": {
                "type": "OBJECT",
                "properties": {
                    "entry": { "type": "NUMBER", "nullable": true },
                    "tp": { "type": "NUMBER", "nullable": true },
                    "sl": { "type": "NUMBER", "nullable": true },
                    "leverage": { "type": "NUMBER", "nullable": true },
                    "horizon": { "type": "STRING", "nullable": true }
                },
                "required": ["entry", "tp", "sl", "leverage", "horizon"]
            },
            "reason": { "type": "STRING" }
        },
        "required": ["analysis", "decision", "decision_context", "params", "reason"]
    })
}

fn gemini_management_response_schema() -> Value {
    json!({
        "type": "OBJECT",
        "properties": {
            "decision": {
                "type": "STRING",
                "enum": ["VALID", "INVALID", "ADJUST"]
            },
            "management_context": management_context_schema_gemini(),
            "params": {
                "type": "OBJECT",
                "properties": {
                    "close_price":   { "type": "NUMBER", "nullable": true },
                    "adjust_fields": { "type": "ARRAY", "nullable": true, "items": { "type": "STRING" } },
                    "qty_ratio":     { "type": "NUMBER", "nullable": true },
                    "new_tp":        { "type": "NUMBER", "nullable": true },
                    "new_sl":        { "type": "NUMBER", "nullable": true }
                },
                "required": ["close_price", "adjust_fields", "qty_ratio", "new_tp", "new_sl"]
            },
            "reason": { "type": "STRING" }
        },
        "required": ["decision", "management_context", "params", "reason"]
    })
}

fn gemini_pending_order_response_schema() -> Value {
    json!({
        "type": "OBJECT",
        "properties": {
            "params": {
                "type": "OBJECT",
                "properties": {
                    "entry":    { "type": "NUMBER", "nullable": true },
                    "tp":       { "type": "NUMBER", "nullable": true },
                    "sl":       { "type": "NUMBER", "nullable": true },
                    "leverage": { "type": "NUMBER", "nullable": true }
                },
                "required": ["entry", "tp", "sl", "leverage"]
            },
            "reason": { "type": "STRING" }
        },
        "required": ["params", "reason"]
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

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn serialize_llm_input_minified(input: &ModelInvocationInput) -> Result<String> {
    if is_entry_mode(input) {
        return filter::scan::ScanFilter::serialize_minified_input(input);
    }

    filter::core::CoreFilter::serialize_minified_input(input)
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
        extract_grok_response_text, parse_json_from_text, serialize_entry_finalize_input_minified,
        serialize_llm_input_minified, EntryContextForLlm, GrokResponsesApiResponse,
        ManagementSnapshotForLlm, ModelInvocationInput, PendingOrderSummaryForLlm,
        PositionContextForLlm, PositionSummaryForLlm,
    };
    use chrono::{DateTime, Utc};
    use serde_json::{json, Value};

    fn sample_scan_tf(
        trend: &str,
        signal_agreement: &str,
        support: f64,
        resistance: f64,
        supporting_signals: &[&str],
        conflicting_signals: &[&str],
        opportunity: &str,
        risk: &str,
    ) -> Value {
        json!({
            "trend": trend,
            "signal_agreement": signal_agreement,
            "range": {"support": support, "resistance": resistance},
            "supporting_signals": supporting_signals,
            "conflicting_signals": conflicting_signals,
            "opportunity": opportunity,
            "risk": risk
        })
    }

    fn sample_scan_audit_tf(
        direction_basis: &str,
        recent_closed_bars_align_with_trend: bool,
        cvd_slope_aligns_with_trend: bool,
        current_partial_bar_aligns_with_trend: bool,
        invalidation_level: Option<f64>,
        range_width_vs_atr: &str,
    ) -> Value {
        json!({
            "direction_basis": direction_basis,
            "recent_closed_bars_align_with_trend": recent_closed_bars_align_with_trend,
            "cvd_slope_aligns_with_trend": cvd_slope_aligns_with_trend,
            "current_partial_bar_aligns_with_trend": current_partial_bar_aligns_with_trend,
            "invalidation_level": invalidation_level,
            "range_width_vs_atr": range_width_vs_atr
        })
    }

    fn sample_stage_1_scan() -> Value {
        json!({
            "15m": sample_scan_tf(
                "Bullish",
                "strong",
                1994.0,
                2018.0,
                &["15m ema bull", "cvd bullish"],
                &["absorption bearish"],
                "Break above 2018 extends local impulse",
                "Loss of 1994 weakens trigger context"
            ),
            "4h": sample_scan_tf(
                "Bullish",
                "mixed",
                1988.0,
                2035.0,
                &["4h ema bull", "higher support intact"],
                &["whale bias bearish"],
                "Acceptance above 2035 opens continuation",
                "Failure back through 1988 breaks setup"
            ),
            "1d": sample_scan_tf(
                "Sideways",
                "mixed",
                1960.0,
                2050.0,
                &["daily value area intact"],
                &["trend/flow not fully aligned"],
                "Breakout above 2050 improves macro structure",
                "Loss of 1960 reopens downside auction"
            ),
            "scan_audit": {
                "15m": sample_scan_audit_tf(
                    "closed_bar_continuation",
                    true,
                    true,
                    false,
                    Some(1994.0),
                    "normal"
                ),
                "4h": sample_scan_audit_tf(
                    "mixed",
                    true,
                    true,
                    true,
                    Some(1988.0),
                    "normal"
                ),
                "1d": sample_scan_audit_tf(
                    "structural_inference",
                    false,
                    false,
                    true,
                    None,
                    "wide"
                )
            }
        })
    }

    fn prompt_route_test_input(
        management_mode: bool,
        pending_order_mode: bool,
    ) -> ModelInvocationInput {
        let now = Utc::now();
        let avwap_15m = (0..10)
            .map(|idx| {
                json!({
                    "ts": format!("2026-03-14T{:02}:00:00Z", idx),
                    "avwap_fut": 2000.0 + idx as f64,
                    "avwap_spot": 1999.5 + idx as f64,
                    "xmk_avwap_gap_f_minus_s": 0.5,
                })
            })
            .collect::<Vec<_>>();
        let avwap_4h = (0..4)
            .map(|idx| {
                json!({
                    "ts": format!("2026-03-1{}T00:00:00Z", idx + 1),
                    "avwap_fut": 2000.0 + idx as f64,
                    "avwap_spot": 1999.0 + idx as f64,
                    "xmk_avwap_gap_f_minus_s": 1.0,
                })
            })
            .collect::<Vec<_>>();
        let avwap_1d = (0..3)
            .map(|idx| {
                json!({
                    "ts": format!("2026-03-1{}T00:00:00Z", idx + 4),
                    "avwap_fut": 2005.0 + idx as f64,
                    "avwap_spot": 2004.0 + idx as f64,
                    "xmk_avwap_gap_f_minus_s": 1.0,
                })
            })
            .collect::<Vec<_>>();
        let fvgs = (0..8)
            .map(|idx| {
                json!({
                    "start_ts": format!("2026-03-14T{:02}:00:00Z", idx),
                    "gap_low": 1990.0 + idx as f64,
                    "gap_high": 1990.5 + idx as f64,
                })
            })
            .collect::<Vec<_>>();
        let orderbook_levels = (0..140)
            .map(|idx| {
                let price = 1930.0 + idx as f64;
                json!({
                    "price_level": price,
                    "bid_liquidity": 5000.0 + idx as f64,
                    "ask_liquidity": 4000.0 + idx as f64,
                    "total_liquidity": 9000.0 + (idx as f64 * 10.0),
                    "net_liquidity": 1000.0,
                    "level_imbalance": 0.1,
                })
            })
            .collect::<Vec<_>>();

        ModelInvocationInput {
            symbol: "TESTUSDT".to_string(),
            ts_bucket: now,
            window_code: "15m".to_string(),
            indicator_count: 4,
            source_routing_key: "llm_indicator_minute".to_string(),
            source_published_at: None,
            received_at: now,
            indicators: json!({
                "fvg": {
                    "payload": {
                        "source_market": "futures",
                        "by_window": {
                            "15m": {
                                "nearest_bull_fvg": {"gap_low": 1998.0, "gap_high": 1999.0},
                                "nearest_bear_fvg": {"gap_low": 2004.0, "gap_high": 2005.0},
                                "is_ready": true,
                                "coverage_ratio": 0.8,
                                "active_bull_fvgs": [{"gap_low": 1998.0, "gap_high": 1999.0}],
                                "active_bear_fvgs": [{"gap_low": 2004.0, "gap_high": 2005.0}],
                                "fvgs": fvgs
                            },
                            "4h": {
                                "nearest_bull_fvg": {"gap_low": 1988.0, "gap_high": 1990.0},
                                "nearest_bear_fvg": {"gap_low": 2010.0, "gap_high": 2012.0},
                                "is_ready": true,
                                "coverage_ratio": 0.6,
                                "active_bull_fvgs": [],
                                "active_bear_fvgs": [],
                                "fvgs": [
                                    {"start_ts": "2026-03-13T00:00:00Z", "gap_low": 1988.0, "gap_high": 1990.0}
                                ]
                            },
                            "1d": {
                                "nearest_bull_fvg": {"gap_low": 1972.0, "gap_high": 1976.0},
                                "nearest_bear_fvg": {"gap_low": 2032.0, "gap_high": 2036.0},
                                "is_ready": false,
                                "coverage_ratio": 0.4,
                                "active_bull_fvgs": [],
                                "active_bear_fvgs": [],
                                "fvgs": [
                                    {"start_ts": "2026-03-10T00:00:00Z", "gap_low": 1972.0, "gap_high": 1976.0}
                                ]
                            },
                            "3d": {
                                "nearest_bull_fvg": {"gap_low": 1950.0, "gap_high": 1958.0},
                                "nearest_bear_fvg": {"gap_low": 2050.0, "gap_high": 2058.0},
                                "is_ready": false,
                                "coverage_ratio": 0.3,
                                "active_bull_fvgs": [],
                                "active_bear_fvgs": [],
                                "fvgs": [
                                    {"start_ts": "2026-03-01T00:00:00Z", "gap_low": 1950.0, "gap_high": 1958.0}
                                ]
                            }
                        }
                    }
                },
                "avwap": {
                    "payload": {
                        "fut_mark_price": 2000.0,
                        "fut_last_price": 2000.5,
                        "series_by_window": {
                            "15m": avwap_15m,
                            "4h": avwap_4h,
                            "1d": avwap_1d
                        }
                    }
                },
                "orderbook_depth": {
                    "payload": {
                        "obi_fut": 0.12,
                        "obi_k_dw_twa_fut": 0.09,
                        "spread_twa_fut": 0.01,
                        "levels": orderbook_levels,
                        "by_window": {
                            "15m": {"obi_fut": 0.12},
                            "1h": {"obi_fut": 0.08}
                        }
                    }
                },
                "price_volume_structure": {
                    "payload": {
                        "val": 1994.0,
                        "vah": 2014.0,
                        "poc_price": 2003.0,
                        "by_window": {
                            "15m": {"val": 1994.0, "vah": 2014.0},
                            "4h": {"val": 1988.0, "vah": 2022.0},
                            "1d": {"val": 1970.0, "vah": 2040.0}
                        }
                    }
                }
            }),
            missing_indicator_codes: vec![],
            management_mode,
            pending_order_mode,
            trading_state: None,
            management_snapshot: if management_mode || pending_order_mode {
                Some(ManagementSnapshotForLlm {
                    context_state: if pending_order_mode {
                        "OPEN_ORDERS_ONLY".to_string()
                    } else {
                        "POSITION_ACTIVE".to_string()
                    },
                    has_active_positions: true,
                    has_open_orders: pending_order_mode,
                    active_position_count: 1,
                    open_order_count: if pending_order_mode { 1 } else { 0 },
                    positions: vec![PositionSummaryForLlm {
                        position_side: "LONG".to_string(),
                        direction: "LONG".to_string(),
                        quantity: 1.0,
                        leverage: 10,
                        entry_price: 1998.0,
                        mark_price: 2006.0,
                        unrealized_pnl: 8.0,
                        pnl_by_latest_price: 8.0,
                        current_tp_price: Some(2028.0),
                        current_sl_price: Some(1988.0),
                    }],
                    pending_order: pending_order_mode.then(|| PendingOrderSummaryForLlm {
                        position_side: "LONG".to_string(),
                        direction: "LONG".to_string(),
                        quantity: 1.0,
                        leverage: Some(10),
                        entry_price: Some(1999.0),
                        current_tp_price: Some(2028.0),
                        current_sl_price: Some(1988.0),
                        planned_tp_price: Some(2028.0),
                        planned_tp_source: Some("effective_context".to_string()),
                        planned_sl_price: Some(1988.0),
                        planned_sl_source: Some("effective_context".to_string()),
                    }),
                    last_management_reason: Some("test".to_string()),
                    position_context: Some(PositionContextForLlm {
                        original_qty: 1.0,
                        current_qty: 1.0,
                        current_pct_of_original: 100.0,
                        effective_leverage: Some(10),
                        effective_entry_price: Some(1998.0),
                        effective_take_profit: Some(2028.0),
                        effective_stop_loss: Some(1988.0),
                        reduction_history: vec![],
                        times_reduced_at_current_level: 0,
                        last_management_action: Some("HOLD".to_string()),
                        last_management_reason: Some("test".to_string()),
                        entry_context: Some(EntryContextForLlm {
                            entry_strategy: Some("patient_retest".to_string()),
                            stop_model: Some("Value Area Invalidation Stop".to_string()),
                            entry_mode: Some("limit_below_zone".to_string()),
                            original_tp: Some(2028.0),
                            original_sl: Some(1988.0),
                            sweep_wick_extreme: None,
                            horizon: Some("4h".to_string()),
                            entry_reason: "test".to_string(),
                        }),
                    }),
                })
            } else {
                None
            },
        }
    }

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
    fn custom_llm_reasoning_uses_stage_specific_settings() {
        let model = super::LlmModelConfig {
            name: "custom_llm".to_string(),
            provider: "custom_llm".to_string(),
            model: "gpt-5.4-xhigh".to_string(),
            use_openrouter: None,
            enabled: true,
            temperature: 0.1,
            max_tokens: 1000,
            enable_thinking: None,
            stage1_reasoning: Some("high".to_string()),
            stage2_reasoning: Some("low".to_string()),
            reasoning: Some("medium".to_string()),
        };

        assert_eq!(
            super::custom_llm_reasoning(&model, super::prompt::EntryPromptStage::Scan)
                .as_ref()
                .map(|cfg| cfg.effort.as_str()),
            Some("high")
        );
        assert_eq!(
            super::custom_llm_reasoning(&model, super::prompt::EntryPromptStage::Finalize)
                .as_ref()
                .map(|cfg| cfg.effort.as_str()),
            Some("low")
        );
    }

    #[test]
    fn custom_llm_reasoning_falls_back_to_legacy_reasoning() {
        let model = super::LlmModelConfig {
            name: "custom_llm".to_string(),
            provider: "custom_llm".to_string(),
            model: "gpt-5.4-xhigh".to_string(),
            use_openrouter: None,
            enabled: true,
            temperature: 0.1,
            max_tokens: 1000,
            enable_thinking: None,
            stage1_reasoning: None,
            stage2_reasoning: None,
            reasoning: Some("xhigh".to_string()),
        };

        assert_eq!(
            super::custom_llm_reasoning(&model, super::prompt::EntryPromptStage::Scan)
                .as_ref()
                .map(|cfg| cfg.effort.as_str()),
            Some("xhigh")
        );
        assert_eq!(
            super::custom_llm_reasoning(&model, super::prompt::EntryPromptStage::Finalize)
                .as_ref()
                .map(|cfg| cfg.effort.as_str()),
            Some("xhigh")
        );
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
        assert_eq!(
            schema
                .pointer("/properties/decision_context/additionalProperties")
                .and_then(|v| v.as_bool()),
            Some(false)
        );
        assert!(schema
            .get("required")
            .and_then(Value::as_array)
            .map(|required| required
                .iter()
                .any(|v| v.as_str() == Some("decision_context")))
            .unwrap_or(false));
    }

    #[test]
    fn custom_llm_management_schema_disables_additional_properties() {
        let schema = super::custom_llm_response_schema(
            true,
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
                .pointer("/properties/management_context/additionalProperties")
                .and_then(|v| v.as_bool()),
            Some(false)
        );
        assert!(schema
            .get("required")
            .and_then(Value::as_array)
            .map(|required| required
                .iter()
                .any(|v| v.as_str() == Some("management_context")))
            .unwrap_or(false));
    }

    #[test]
    fn custom_llm_entry_scan_schema_disables_additional_properties() {
        for prompt_template in ["big_opportunity", "medium_large_opportunity"] {
            let schema = super::custom_llm_response_schema(
                false,
                false,
                super::prompt::EntryPromptStage::Scan,
                prompt_template,
            );
            assert_eq!(
                schema.get("additionalProperties").and_then(|v| v.as_bool()),
                Some(false),
                "root should be strict for {prompt_template}"
            );
            assert_eq!(
                schema
                    .pointer("/properties/15m/additionalProperties")
                    .and_then(|v| v.as_bool()),
                Some(false),
                "15m should be strict for {prompt_template}"
            );
            assert_eq!(
                schema
                    .pointer("/properties/15m/properties/range/additionalProperties")
                    .and_then(|v| v.as_bool()),
                Some(false),
                "range should be strict for {prompt_template}"
            );
            assert_eq!(
                schema
                    .pointer("/properties/15m/properties/supporting_signals/type")
                    .and_then(|v| v.as_str()),
                Some("array"),
                "supporting_signals should exist for {prompt_template}"
            );
            assert_eq!(
                schema
                    .pointer("/properties/scan_audit/additionalProperties")
                    .and_then(|v| v.as_bool()),
                Some(false),
                "scan_audit should be strict for {prompt_template}"
            );
            assert_eq!(
                schema
                    .pointer(
                        "/properties/scan_audit/properties/15m/properties/direction_basis/type"
                    )
                    .and_then(|v| v.as_str()),
                Some("string"),
                "scan_audit.direction_basis should exist for {prompt_template}"
            );
        }
    }

    #[test]
    fn qwen_scan_output_contract_matches_scan_schema_shape() {
        for prompt_template in ["big_opportunity", "medium_large_opportunity"] {
            let contract = super::qwen_output_contract(
                false,
                false,
                super::prompt::EntryPromptStage::Scan,
                prompt_template,
            );
            assert!(contract.contains("`15m`, `4h`, `1d`, and `scan_audit`"));
            assert!(contract.contains("`supporting_signals`"));
            assert!(contract.contains("direction_basis"));
            assert!(!contract.contains("`range_basis`"));
            assert!(!contract.contains("`range_role_used`"));
            assert!(!contract.contains("`decision`, `reason`, and `scan`"));
            assert!(!contract.contains("story"));
        }
    }

    #[test]
    fn qwen_entry_output_contract_mentions_decision_context() {
        let contract = super::qwen_output_contract(
            false,
            false,
            super::prompt::EntryPromptStage::Finalize,
            "medium_large_opportunity",
        );
        assert!(contract.contains("decision_context"));
        assert!(contract.contains("thesis_flow_alignment"));
        assert!(contract.contains("entry_readiness"));
        assert!(contract.contains("entry_exposure"));
        assert!(contract.contains("key_condition"));
    }

    #[test]
    fn qwen_management_output_contract_mentions_management_context() {
        let contract = super::qwen_output_contract(
            true,
            false,
            super::prompt::EntryPromptStage::Finalize,
            "medium_large_opportunity",
        );
        assert!(contract.contains("management_context"));
        assert!(contract.contains("direction_state"));
        assert!(contract.contains("ltf_move_meaning"));
        assert!(contract.contains("sl_noise_risk_15m"));
        assert!(contract.contains("tp_state"));
        assert!(contract.contains("key_condition"));
    }

    #[test]
    fn medium_large_finalize_trace_keeps_new_scan_fields() {
        let input = ModelInvocationInput {
            symbol: "TESTUSDT".to_string(),
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
        let scan = super::annotate_scan_for_stage2(
            &json!({
                "15m": sample_scan_tf("Bullish", "mixed", 2030.0, 2060.0, &["positive cvd", "range support held"], &["funding stretched"], "Push through 2060 extends 15m trend", "Loss of 2030 weakens trigger structure"),
                "4h": sample_scan_tf("Bullish", "strong", 2020.0, 2080.0, &["4h trend up", "support defended"], &[], "Acceptance above 2080 opens continuation", "Failure below 2020 damages setup"),
                "1d": sample_scan_tf("Sideways", "mixed", 2000.0, 2100.0, &["daily auction balanced"], &["macro trend unresolved"], "Breakout above 2100 improves daily backdrop", "Loss of 2000 reopens downside risk"),
            }),
            super::FinalizeStageContext {
                stage1_scan_ts_bucket: DateTime::parse_from_rfc3339("2026-03-18T07:00:00Z")
                    .expect("parse stage1 ts")
                    .with_timezone(&Utc),
                stage2_core_ts_bucket: DateTime::parse_from_rfc3339("2026-03-18T07:05:00Z")
                    .expect("parse stage2 ts")
                    .with_timezone(&Utc),
            },
        );

        let event = super::build_entry_finalize_trace_event(&input, &scan, Some(123));
        assert_eq!(
            event.get("scan_15m_trend").and_then(Value::as_str),
            Some("Bullish")
        );
        assert_eq!(
            event.get("scan_4h_trend").and_then(Value::as_str),
            Some("Bullish")
        );
        assert_eq!(
            event.get("scan_1d_trend").and_then(Value::as_str),
            Some("Sideways")
        );
        assert_eq!(event.get("latency_ms").and_then(Value::as_u64), Some(123));
        assert_eq!(
            event.get("stage_mode").and_then(Value::as_str),
            Some("entry")
        );
        assert_eq!(
            event.get("stage_1_scan_ts_bucket").and_then(Value::as_str),
            Some("2026-03-18T07:00:00+00:00")
        );
        assert_eq!(
            event.get("stage_2_core_ts_bucket").and_then(Value::as_str),
            Some("2026-03-18T07:05:00+00:00")
        );
    }

    #[test]
    fn serialize_entry_finalize_input_uses_strategy_focused_indicator_subset() {
        let now = Utc::now();
        let input = ModelInvocationInput {
            symbol: "TESTUSDT".to_string(),
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
            "15m": sample_scan_tf("Bullish", "strong", 1995.0, 2025.0, &["bid wall holding", "positive obi"], &[], "Break through 2025 releases short-term upside", "Loss of 1995 invalidates trigger"),
            "4h": sample_scan_tf("Bullish", "strong", 1990.0, 2030.0, &["4h value accepted", "trend support intact"], &[], "Acceptance above 2030 confirms continuation", "Close back below 1990 weakens 4h structure"),
            "1d": sample_scan_tf("Sideways", "mixed", 1980.0, 2040.0, &["daily rotation intact"], &["top-down breakout not confirmed"], "Breakout above 2040 improves macro context", "Loss of 1980 reopens downside auction"),
        });

        let serialized =
            serialize_entry_finalize_input_minified(&input, &scan).expect("serialize finalize");
        let value: Value = serde_json::from_str(&serialized).expect("parse finalize input");

        assert!(value.pointer("/indicators/orderbook_depth").is_some());
        assert!(value.pointer("/indicators/core_price_anchors").is_none());
        assert!(value
            .pointer("/indicators/price_volume_structure")
            .is_some());
        assert!(value.pointer("/indicators/footprint").is_some());
        assert!(value.pointer("/indicators/avwap").is_some());
        assert!(value.pointer("/indicators/kline_history").is_some());
        assert!(value.pointer("/indicators/pre_computed_v").is_none());
        assert!(value.pointer("/indicators/tpo_market_profile").is_some());
        assert!(value.pointer("/indicators/rvwap_sigma_bands").is_some());
        assert_eq!(
            value
                .pointer("/finalize_focus/scan_4h_trend")
                .and_then(|v| v.as_str()),
            Some("Bullish")
        );
    }

    #[test]
    fn serialize_entry_finalize_input_keeps_scan_referenced_supporting_indicators() {
        let now = Utc::now();
        let input = ModelInvocationInput {
            symbol: "TESTUSDT".to_string(),
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
            symbol: "TESTUSDT".to_string(),
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
        assert!(value.pointer("/indicators/pre_computed_v").is_none());
    }

    #[test]
    fn build_prompt_pair_captures_finalize_prompt_input_and_stage1_scan() {
        let now = Utc::now();
        let input = ModelInvocationInput {
            symbol: "TESTUSDT".to_string(),
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
            "15m": sample_scan_tf("Bullish", "mixed", 1990.0, 2015.0, &["value area re-fill", "flow long"], &["short-term overhead supply"], "Break above 2015 improves trigger quality", "Loss of 1990 weakens immediate setup"),
            "4h": sample_scan_tf("Bullish", "strong", 1985.0, 2020.0, &["4h structure up", "support held"], &[], "Acceptance above 2020 confirms continuation", "Failure below 1985 damages setup"),
            "1d": sample_scan_tf("Bullish", "mixed", 1980.0, 2025.0, &["daily backdrop constructive"], &["macro breakout not complete"], "Daily acceptance above 2025 improves macro trend", "Loss of 1980 increases downside risk"),
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
                .and_then(|value| value.pointer("/4h/range/support"))
                .and_then(Value::as_f64),
            Some(1985.0)
        );
        assert_eq!(
            capture
                .stage_1_setup_scan_json
                .as_ref()
                .and_then(|value| value.pointer("/15m/range/support"))
                .and_then(Value::as_f64),
            Some(1990.0)
        );
        assert!(capture
            .stage_1_setup_scan_json
            .as_ref()
            .and_then(|value| value.pointer("/15m/story"))
            .is_none());
        assert!(capture
            .stage_1_setup_scan_json
            .as_ref()
            .and_then(|value| value.get("scan_audit"))
            .is_none());
        assert_eq!(
            capture
                .prompt_input
                .pointer("/finalize_focus/scan_4h_trend")
                .and_then(Value::as_str),
            Some("Bullish")
        );
    }

    #[test]
    fn serialize_entry_scan_input_applies_scan_filter_rules() {
        let now = Utc::now();

        let input = ModelInvocationInput {
            symbol: "TESTUSDT".to_string(),
            ts_bucket: now,
            window_code: "15m".to_string(),
            indicator_count: 5,
            source_routing_key: "llm_indicator_minute".to_string(),
            source_published_at: None,
            received_at: now,
            indicators: json!({
                "funding_rate": {
                    "payload": {
                        "funding_current": -0.00004527,
                        "mark_price_last": 1982.6936124,
                        "changes": [
                            {
                                "change_ts": "2026-03-11T06:00:00Z",
                                "funding_delta": -0.00000111
                            },
                            {
                                "change_ts": "2026-03-11T06:15:00Z",
                                "funding_delta": -0.00000063
                            }
                        ],
                        "by_window": {
                            "15m": {
                                "changes": [
                                    {"change_ts": "2026-03-11T06:00:00Z", "funding_delta": -0.00000111},
                                    {"change_ts": "2026-03-11T06:15:00Z", "funding_delta": -0.00000063}
                                ]
                            }
                        }
                    }
                },
                "kline_history": {
                    "payload": {
                        "intervals": {
                            "15m": {
                                "markets": {
                                    "futures": {
                                        "bars": [
                                            {"open_time": "2026-03-11T06:00:00Z", "high": 2001.1234, "low": 1999.8765, "close": 2000.5544, "volume_base": 100.0, "is_closed": true},
                                            {"open_time": "2026-03-11T06:15:00Z", "high": 2002.1234, "low": 2000.8765, "close": 2001.5544, "volume_base": 120.0, "is_closed": true}
                                        ]
                                    }
                                }
                            }
                        }
                    }
                },
                "absorption": {
                    "payload": {
                        "recent_7d": {
                            "events": [
                                {"event_end_ts": "2026-03-09T06:00:00Z", "score": 0.6971667409666932},
                                {"event_end_ts": "2026-03-09T06:15:00Z", "score": 0.5521759390122744}
                            ]
                        }
                    }
                },
                "avwap": {
                    "payload": {
                        "fut_mark_price": 2001.5678,
                        "series_by_window": {
                            "15m": [
                                {"ts": "2026-03-11T06:00:00Z", "avwap_fut": 2000.1234},
                                {"ts": "2026-03-11T06:15:00Z", "avwap_fut": 2000.9876}
                            ]
                        }
                    }
                },
                "orderbook_depth": {
                    "payload": {
                        "by_window": {
                            "15m": {
                                "obi_fut": 0.027879672288433154,
                                "spread_twa_fut": 0.003691128132476528
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

        let serialized = serialize_llm_input_minified(&input).expect("serialize optimized");
        let value: Value = serde_json::from_str(&serialized).expect("parse optimized");

        assert_eq!(value.as_object().map(|obj| obj.len()), Some(9));
        assert_eq!(
            value.get("version").and_then(Value::as_str),
            Some("scan_v6_1")
        );
        assert!(value.pointer("/now").is_some());
        assert!(value
            .pointer("/path_newest_to_oldest/latest_15m_detail")
            .is_some());
        assert!(value.pointer("/events_newest_to_oldest").is_some());
        assert!(value.pointer("/supporting_context").is_some());
        assert!(value.pointer("/raw_overflow").is_some());
        assert!(value.pointer("/indicators").is_none());
        assert!(value.pointer("/by_timeframe").is_none());
        assert!(value.pointer("/timeframe_evidence").is_none());
    }

    #[test]
    fn serialize_management_scan_stage_uses_shared_scan_filter() {
        let now = Utc::now();
        let input = ModelInvocationInput {
            symbol: "TESTUSDT".to_string(),
            ts_bucket: now,
            window_code: "15m".to_string(),
            indicator_count: 1,
            source_routing_key: "llm_indicator_minute".to_string(),
            source_published_at: None,
            received_at: now,
            indicators: json!({
                "kline_history": {
                    "payload": {
                        "intervals": {
                            "15m": {
                                "markets": {
                                    "futures": {
                                        "bars": [
                                            {"open_time": "2026-03-11T06:00:00Z", "high": 2001.1234, "low": 1999.8765, "close": 2000.5544, "volume_base": 100.0, "is_closed": true}
                                        ]
                                    }
                                }
                            }
                        }
                    }
                }
            }),
            missing_indicator_codes: vec![],
            management_mode: true,
            pending_order_mode: false,
            trading_state: None,
            management_snapshot: Some(ManagementSnapshotForLlm {
                context_state: "ACTIVE_POSITION".to_string(),
                has_active_positions: true,
                has_open_orders: true,
                active_position_count: 1,
                open_order_count: 1,
                positions: vec![],
                position_context: None,
                pending_order: None,
                last_management_reason: Some("test".to_string()),
            }),
        };

        let serialized = super::serialize_prompt_input_minified(
            &input,
            super::prompt::EntryPromptStage::Scan,
            None,
        )
        .expect("serialize management scan input");
        let value: Value = serde_json::from_str(&serialized).expect("parse management scan input");

        assert_eq!(value.as_object().map(|obj| obj.len()), Some(9));
        assert!(value.pointer("/management_snapshot").is_none());
        assert!(value.pointer("/now").is_some());
        assert!(value
            .pointer("/path_newest_to_oldest/latest_15m_detail")
            .is_some());
        assert!(value.pointer("/events_newest_to_oldest").is_some());
        assert!(value.pointer("/indicators/kline_history").is_none());
        assert!(value.pointer("/timeframe_evidence").is_none());
    }

    #[test]
    fn serialize_pending_order_scan_stage_uses_shared_scan_filter() {
        let now = Utc::now();
        let input = ModelInvocationInput {
            symbol: "TESTUSDT".to_string(),
            ts_bucket: now,
            window_code: "15m".to_string(),
            indicator_count: 1,
            source_routing_key: "llm_indicator_minute".to_string(),
            source_published_at: None,
            received_at: now,
            indicators: json!({
                "kline_history": {
                    "payload": {
                        "intervals": {
                            "15m": {
                                "markets": {
                                    "futures": {
                                        "bars": [
                                            {"open_time": "2026-03-11T06:00:00Z", "high": 2001.1234, "low": 1999.8765, "close": 2000.5544, "volume_base": 100.0, "is_closed": true}
                                        ]
                                    }
                                }
                            }
                        }
                    }
                }
            }),
            missing_indicator_codes: vec![],
            management_mode: false,
            pending_order_mode: true,
            trading_state: None,
            management_snapshot: None,
        };

        let serialized = super::serialize_prompt_input_minified(
            &input,
            super::prompt::EntryPromptStage::Scan,
            None,
        )
        .expect("serialize pending-order scan input");
        let value: Value =
            serde_json::from_str(&serialized).expect("parse pending-order scan input");

        assert_eq!(value.as_object().map(|obj| obj.len()), Some(9));
        assert!(value.pointer("/management_snapshot").is_none());
        assert!(value.pointer("/now").is_some());
        assert!(value
            .pointer("/path_newest_to_oldest/latest_15m_detail")
            .is_some());
        assert!(value.pointer("/events_newest_to_oldest").is_some());
        assert!(value.pointer("/indicators/kline_history").is_none());
        assert!(value.pointer("/timeframe_evidence").is_none());
    }

    #[test]
    fn entry_prompt_uses_entry_core() {
        let input = prompt_route_test_input(false, false);
        let scan = sample_stage_1_scan();

        let prompt = super::build_prompt_pair(
            &input,
            "medium_large_opportunity",
            super::prompt::EntryPromptStage::Finalize,
            Some(&scan),
        )
        .expect("build entry prompt");
        let capture = prompt
            .prompt_input_capture
            .expect("capture entry prompt input");

        assert!(prompt.user.contains("STAGE_1_MARKET_SCAN_JSON:"));
        assert!(capture
            .prompt_input
            .pointer("/management_snapshot")
            .is_none());
        assert!(capture.prompt_input.pointer("/trading_state").is_none());
        assert!(capture
            .prompt_input
            .pointer("/indicators/fvg/payload/by_window/3d")
            .is_some());
        assert_eq!(
            capture
                .prompt_input
                .pointer("/indicators/avwap/payload/series_by_window/15m")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(5)
        );
        assert_eq!(
            capture
                .prompt_input
                .pointer("/indicators/orderbook_depth/payload/top_liquidity_levels")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(20)
        );
        assert!(capture.prompt_input.pointer("/finalize_focus").is_some());
        assert!(capture
            .stage_1_setup_scan_json
            .as_ref()
            .and_then(|value| value.pointer("/15m/story"))
            .is_none());
    }

    #[test]
    fn finalize_prompt_includes_stage_timestamps_when_scan_is_annotated() {
        let input = prompt_route_test_input(false, false);
        let stage1_scan_ts_bucket = DateTime::parse_from_rfc3339("2026-03-18T07:00:00Z")
            .expect("parse stage1 ts")
            .with_timezone(&Utc);
        let stage2_core_ts_bucket = DateTime::parse_from_rfc3339("2026-03-18T07:05:00Z")
            .expect("parse stage2 ts")
            .with_timezone(&Utc);
        let scan = super::annotate_scan_for_stage2(
            &sample_stage_1_scan(),
            super::FinalizeStageContext {
                stage1_scan_ts_bucket,
                stage2_core_ts_bucket,
            },
        );

        let prompt = super::build_prompt_pair(
            &input,
            "medium_large_opportunity",
            super::prompt::EntryPromptStage::Finalize,
            Some(&scan),
        )
        .expect("build entry prompt");
        let capture = prompt
            .prompt_input_capture
            .expect("capture finalize prompt input");

        assert!(prompt
            .user
            .contains("STAGE_1_SCAN_TS_BUCKET: 2026-03-18T07:00:00+00:00"));
        assert!(prompt
            .user
            .contains("STAGE_2_CORE_TS_BUCKET: 2026-03-18T07:05:00+00:00"));
        assert!(prompt.user.contains("STAGE_1_TO_STAGE_2_GAP_MINUTES: 5.00"));
        assert!(prompt.user.contains("STAGE_1_MARKET_SCAN_JSON:"));
        assert_eq!(
            capture
                .stage_1_setup_scan_json
                .as_ref()
                .and_then(|value| value.get("stage_1_scan_ts_bucket"))
                .and_then(Value::as_str),
            Some("2026-03-18T07:00:00+00:00")
        );
        assert_eq!(
            capture
                .stage_1_setup_scan_json
                .as_ref()
                .and_then(|value| value.get("stage_2_core_ts_bucket"))
                .and_then(Value::as_str),
            Some("2026-03-18T07:05:00+00:00")
        );
        assert!(capture
            .stage_1_setup_scan_json
            .as_ref()
            .and_then(|value| value.get("scan_audit"))
            .is_none());
    }

    #[test]
    fn management_prompt_uses_management_core() {
        let input = prompt_route_test_input(true, false);
        let scan = sample_stage_1_scan();

        let prompt = super::build_prompt_pair(
            &input,
            "medium_large_opportunity",
            super::prompt::EntryPromptStage::Finalize,
            Some(&scan),
        )
        .expect("build management prompt");
        let capture = prompt
            .prompt_input_capture
            .expect("capture management prompt input");

        assert!(prompt.user.contains("STAGE_1_MARKET_SCAN_JSON:"));
        assert!(capture
            .prompt_input
            .pointer("/management_snapshot")
            .is_some());
        assert!(capture.prompt_input.pointer("/trading_state").is_none());
        assert!(capture
            .prompt_input
            .pointer("/management_snapshot/positions/0/pnl_by_latest_price")
            .is_some());
        assert!(capture
            .prompt_input
            .pointer("/indicators/fvg/payload/by_window/1d")
            .is_some());
        assert!(capture
            .prompt_input
            .pointer("/indicators/fvg/payload/by_window/1d/active_bull_fvgs")
            .is_none());
        assert!(capture
            .prompt_input
            .pointer("/indicators/fvg/payload/by_window/1d/active_bear_fvgs")
            .is_none());
        assert!(capture
            .prompt_input
            .pointer("/indicators/fvg/payload/by_window/1d/fvgs")
            .is_none());
        assert!(capture
            .prompt_input
            .pointer("/indicators/fvg/payload/by_window/3d")
            .is_none());
        assert_eq!(
            capture
                .prompt_input
                .pointer("/indicators/avwap/payload/series_by_window/15m")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(3)
        );
        assert_eq!(
            capture
                .prompt_input
                .pointer("/indicators/orderbook_depth/payload/top_liquidity_levels")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(60)
        );
        assert!(capture.prompt_input.pointer("/finalize_focus").is_none());
    }

    #[test]
    fn pending_prompt_uses_pending_core() {
        let input = prompt_route_test_input(false, true);
        let scan = sample_stage_1_scan();

        let prompt = super::build_prompt_pair(
            &input,
            "medium_large_opportunity",
            super::prompt::EntryPromptStage::Finalize,
            Some(&scan),
        )
        .expect("build pending prompt");
        let capture = prompt
            .prompt_input_capture
            .expect("capture pending prompt input");

        assert!(prompt.user.contains("STAGE_1_MARKET_SCAN_JSON:"));
        assert!(capture
            .prompt_input
            .pointer("/management_snapshot")
            .is_some());
        assert!(capture
            .prompt_input
            .pointer("/management_snapshot/positions/0/pnl_by_latest_price")
            .is_some());
        assert!(capture
            .prompt_input
            .pointer("/management_snapshot/pending_order")
            .is_some());
        assert!(capture
            .prompt_input
            .pointer("/indicators/fvg/payload/by_window/3d")
            .is_none());
        assert_eq!(
            capture
                .prompt_input
                .pointer("/indicators/avwap/payload/series_by_window/15m")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(8)
        );
        assert_eq!(
            capture
                .prompt_input
                .pointer("/indicators/orderbook_depth/payload/top_liquidity_levels")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(81)
        );
        assert!(capture.prompt_input.pointer("/finalize_focus").is_none());
    }

    #[test]
    fn serialize_management_input_omits_entry_v_and_v_derived_management_fields() {
        let now = Utc::now();
        let input = ModelInvocationInput {
            symbol: "TESTUSDT".to_string(),
            ts_bucket: now,
            window_code: "15m".to_string(),
            indicator_count: 2,
            source_routing_key: "llm_indicator_minute".to_string(),
            source_published_at: None,
            received_at: now,
            indicators: json!({
                "avwap": {
                    "payload": {
                        "fut_mark_price": 2025.0,
                        "fut_last_price": 2024.5
                    }
                },
                "price_volume_structure": {
                    "payload": {
                        "val": 2000.0,
                        "vah": 2040.0,
                        "poc_price": 2018.0
                    }
                }
            }),
            missing_indicator_codes: vec![],
            management_mode: true,
            pending_order_mode: false,
            trading_state: None,
            management_snapshot: Some(ManagementSnapshotForLlm {
                context_state: "ACTIVE_POSITION".to_string(),
                has_active_positions: true,
                has_open_orders: true,
                active_position_count: 1,
                open_order_count: 2,
                positions: vec![PositionSummaryForLlm {
                    position_side: "LONG".to_string(),
                    direction: "LONG".to_string(),
                    quantity: 1.0,
                    leverage: 10,
                    entry_price: 2010.0,
                    mark_price: 2025.0,
                    unrealized_pnl: 15.0,
                    pnl_by_latest_price: 15.0,
                    current_tp_price: Some(2055.0),
                    current_sl_price: Some(1998.0),
                }],
                position_context: Some(PositionContextForLlm {
                    original_qty: 1.0,
                    current_qty: 1.0,
                    current_pct_of_original: 100.0,
                    effective_leverage: Some(10),
                    effective_entry_price: Some(2010.0),
                    effective_take_profit: Some(2055.0),
                    effective_stop_loss: Some(1998.0),
                    reduction_history: vec![],
                    times_reduced_at_current_level: 0,
                    last_management_action: Some("HOLD".to_string()),
                    last_management_reason: Some("test".to_string()),
                    entry_context: Some(EntryContextForLlm {
                        entry_strategy: Some("patient_retest".to_string()),
                        stop_model: Some("Value Area Invalidation Stop".to_string()),
                        entry_mode: Some("limit_below_zone".to_string()),
                        original_tp: Some(2055.0),
                        original_sl: Some(1998.0),
                        sweep_wick_extreme: None,
                        horizon: Some("4h".to_string()),
                        entry_reason: "test".to_string(),
                    }),
                }),
                pending_order: None,
                last_management_reason: Some("test".to_string()),
            }),
        };

        let serialized = serialize_llm_input_minified(&input).expect("serialize management input");
        let value: Value = serde_json::from_str(&serialized).expect("parse management input");

        assert!(value
            .pointer("/management_snapshot/position_context/entry_context/entry_v")
            .is_none());
        assert!(value
            .pointer("/indicators/pre_computed_management_state")
            .is_none());
        assert_eq!(
            value.pointer("/management_snapshot/positions/0/pnl_by_latest_price"),
            Some(&json!(15.0))
        );
    }

    #[test]
    fn whitelist_keeps_sensitive_precision_while_non_whitelisted_fields_round() {
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
                "avwap": {
                    "payload": {
                        "fut_mark_price": 1982.6936124
                    }
                }
            }
        });

        crate::llm::filter::TempIndicatorInputOptimizer::round_derived_fields(&mut value);

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
            value.pointer("/indicators/avwap/payload/fut_mark_price"),
            Some(&json!(1982.69))
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
        input_schema: if matches!(entry_stage, prompt::EntryPromptStage::Scan) {
            if is_medium_large(prompt_template) {
                ml_grok_entry_scan_schema()
            } else {
                grok_entry_scan_response_schema()
            }
        } else if pending_order_mode {
            grok_pending_order_response_schema()
        } else if management_mode {
            if is_medium_large(prompt_template) {
                ml_grok_management_schema()
            } else {
                grok_management_response_schema()
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
