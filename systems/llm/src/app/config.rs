use anyhow::{anyhow, Result};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize)]
pub struct RootConfig {
    pub app: AppSection,
    pub api: ApiConfig,
    #[serde(default)]
    pub network: NetworkConfig,
    pub mq: MqConfig,
    #[serde(default)]
    pub database: DatabaseConfig,
    #[serde(default)]
    pub llm: LlmConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AppSection {
    pub name: String,
    pub env: String,
    pub timezone: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApiConfig {
    pub claude: ClaudeApiConfig,
    #[serde(default)]
    pub qwen: QwenApiConfig,
    #[serde(default)]
    pub custom_llm: CustomLlmApiConfig,
    #[serde(default)]
    pub gemini: GeminiApiConfig,
    #[serde(default)]
    pub openrouter: OpenRouterApiConfig,
    #[serde(default)]
    pub grok: GrokApiConfig,
    #[serde(default)]
    pub telegram: TelegramApiConfig,
    #[serde(default)]
    pub x: XApiConfig,
    pub binance: BinanceApiConfig,
    #[serde(default)]
    pub default_model: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClaudeApiConfig {
    pub api_key: String,
    #[serde(default = "default_claude_api_url")]
    pub api_url: String,
    #[serde(default = "default_claude_api_version")]
    pub api_version: String,
    #[serde(default = "default_claude_api_mode")]
    pub mode: String,
    #[serde(default = "default_claude_batch_api_url")]
    pub batch_api_url: String,
    #[serde(default = "default_claude_batch_poll_interval_secs")]
    pub batch_poll_interval_secs: u64,
    #[serde(default = "default_claude_batch_wait_timeout_secs")]
    pub batch_wait_timeout_secs: u64,
}

impl ClaudeApiConfig {
    pub fn resolved_api_key(&self) -> String {
        resolve_secret(&self.api_key)
    }

    pub fn use_batch_api(&self) -> bool {
        self.mode.eq_ignore_ascii_case("batch")
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct QwenApiConfig {
    #[serde(default)]
    pub api_key: String,
    #[serde(default = "default_qwen_base_api_url")]
    pub base_api_url: String,
    #[serde(default = "default_qwen_model")]
    pub model: String,
}

impl QwenApiConfig {
    pub fn resolved_api_key(&self) -> String {
        resolve_secret(&self.api_key)
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct CustomLlmApiConfig {
    #[serde(default)]
    pub api_key: String,
    #[serde(default)]
    pub base_api_url: String,
    #[serde(default)]
    pub model: String,
}

impl CustomLlmApiConfig {
    pub fn resolved_api_key(&self) -> String {
        resolve_secret(&self.api_key)
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct GeminiApiConfig {
    #[serde(default)]
    pub api_key: String,
    #[serde(default = "default_gemini_base_api_url")]
    pub base_api_url: String,
    #[serde(default = "default_gemini_model")]
    pub model: String,
}

impl GeminiApiConfig {
    pub fn resolved_api_key(&self) -> String {
        resolve_secret(&self.api_key)
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct OpenRouterApiConfig {
    #[serde(default)]
    pub api_key: String,
    #[serde(default = "default_openrouter_base_api_url")]
    pub base_api_url: String,
    #[serde(default)]
    pub site_url: String,
    #[serde(default)]
    pub app_name: String,
}

impl OpenRouterApiConfig {
    pub fn resolved_api_key(&self) -> String {
        resolve_secret(&self.api_key)
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct GrokApiConfig {
    #[serde(default)]
    pub api_key: String,
    #[serde(default = "default_grok_base_api_url")]
    pub base_api_url: String,
    #[serde(default = "default_grok_model")]
    pub model: String,
}

impl GrokApiConfig {
    pub fn resolved_api_key(&self) -> String {
        resolve_secret(&self.api_key)
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct TelegramApiConfig {
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub chat_id: String,
    #[serde(default = "default_telegram_base_api_url")]
    pub base_api_url: String,
}

impl TelegramApiConfig {
    pub fn resolved_token(&self) -> String {
        resolve_secret(&self.token)
    }

    pub fn resolved_chat_id(&self) -> String {
        resolve_secret(&self.chat_id)
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct XApiConfig {
    #[serde(default)]
    pub consumer_key: String,
    #[serde(default)]
    pub secret_key: String,
    #[serde(default)]
    pub access_token: String,
    #[serde(default)]
    pub access_token_secret: String,
    #[serde(default = "default_x_base_api_url")]
    pub base_api_url: String,
}

impl XApiConfig {
    pub fn resolved_consumer_key(&self) -> String {
        resolve_secret(&self.consumer_key)
    }

    pub fn resolved_secret_key(&self) -> String {
        resolve_secret(&self.secret_key)
    }

    pub fn resolved_access_token(&self) -> String {
        resolve_secret(&self.access_token)
    }

    pub fn resolved_access_token_secret(&self) -> String {
        resolve_secret(&self.access_token_secret)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceApiConfig {
    pub api_key: String,
    pub api_secret: String,
    #[serde(default = "default_binance_futures_rest_api_url")]
    pub futures_rest_api_url: String,
}

impl BinanceApiConfig {
    pub fn resolved_api_key(&self) -> String {
        resolve_secret(&self.api_key)
    }

    pub fn resolved_api_secret(&self) -> String {
        resolve_secret(&self.api_secret)
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct NetworkConfig {
    #[serde(default)]
    pub proxy: ProxyConfig,
    #[serde(default)]
    pub rest_proxy: ProxyConfig,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct DatabaseConfig {
    #[serde(default = "default_database_host")]
    pub host: String,
    #[serde(default = "default_database_port")]
    pub port: u16,
    #[serde(default)]
    pub database: String,
    #[serde(default)]
    pub user: String,
    #[serde(default)]
    pub password_env: String,
    #[serde(default = "default_database_sslmode")]
    pub sslmode: String,
    #[serde(default)]
    pub connect_timeout_secs: Option<u64>,
    #[serde(default)]
    pub application_name: Option<String>,
    #[serde(default)]
    pub options: Option<String>,
    #[serde(default)]
    pub pool: Option<DatabasePoolConfig>,
}

impl DatabaseConfig {
    pub fn resolved_password(&self) -> String {
        resolve_secret(&self.password_env)
    }

    pub fn postgres_uri(&self) -> String {
        let user = urlencoding::encode(&self.user);
        let resolved_password = self.resolved_password();
        let password = urlencoding::encode(&resolved_password);
        let database = urlencoding::encode(&self.database);
        let mut uri = format!(
            "postgres://{}:{}@{}:{}/{}",
            user, password, self.host, self.port, database
        );

        let mut params = Vec::new();
        let sslmode = self.sslmode.trim();
        if !sslmode.is_empty() {
            params.push(format!("sslmode={}", urlencoding::encode(sslmode)));
        }
        if let Some(timeout) = self.connect_timeout_secs {
            params.push(format!("connect_timeout={}", timeout));
        }
        if let Some(app_name) = self.application_name.as_deref() {
            let app_name = app_name.trim();
            if !app_name.is_empty() {
                params.push(format!(
                    "application_name={}",
                    urlencoding::encode(app_name)
                ));
            }
        }
        if let Some(options) = self.options.as_deref() {
            let options = options.trim();
            if !options.is_empty() {
                params.push(format!("options={}", urlencoding::encode(options)));
            }
        }
        if !params.is_empty() {
            uri.push('?');
            uri.push_str(&params.join("&"));
        }

        uri
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct DatabasePoolConfig {
    #[serde(default)]
    pub min_connections: Option<u32>,
    #[serde(default)]
    pub max_connections: Option<u32>,
    #[serde(default)]
    pub acquire_timeout_secs: Option<u64>,
    #[serde(default)]
    pub idle_timeout_secs: Option<u64>,
    #[serde(default)]
    pub max_lifetime_secs: Option<u64>,
    #[serde(default)]
    pub test_before_acquire: Option<bool>,
}

impl NetworkConfig {
    pub fn effective_rest_proxy_url(&self) -> Option<String> {
        self.rest_proxy
            .effective_url()
            .or_else(|| self.proxy.effective_url())
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ProxyConfig {
    #[serde(default)]
    pub enabled: bool,
    pub address: Option<String>,
}

impl ProxyConfig {
    pub fn effective_url(&self) -> Option<String> {
        if !self.enabled {
            return None;
        }
        let raw = self.address.as_deref()?.trim();
        if raw.is_empty() {
            return None;
        }
        if raw.contains("://") {
            Some(raw.to_string())
        } else {
            Some(format!("http://{}", raw))
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MqConfig {
    pub host: String,
    pub port: u16,
    pub vhost: String,
    pub user: String,
    pub password_env: String,
    pub heartbeat_secs: Option<u16>,
    pub connection_timeout_secs: Option<u16>,
    pub exchanges: MqExchanges,
    pub queues: HashMap<String, MqQueueConfig>,
}

impl MqConfig {
    pub fn resolved_password(&self) -> String {
        resolve_secret(&self.password_env)
    }

    pub fn amqp_uri(&self) -> String {
        let user = urlencoding::encode(&self.user);
        let resolved_password = self.resolved_password();
        let password = urlencoding::encode(&resolved_password);
        let raw_vhost = self.vhost.trim();
        let vhost = if raw_vhost.is_empty() {
            urlencoding::encode("/")
        } else {
            urlencoding::encode(raw_vhost)
        };
        let mut uri = format!(
            "amqp://{}:{}@{}:{}/{}",
            user, password, self.host, self.port, vhost
        );

        let mut params = Vec::new();
        if let Some(v) = self.heartbeat_secs {
            params.push(format!("heartbeat={}", v));
        }
        if let Some(v) = self.connection_timeout_secs {
            params.push(format!("connection_timeout={}", v));
        }
        if !params.is_empty() {
            uri.push('?');
            uri.push_str(&params.join("&"));
        }

        uri
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MqExchanges {
    pub md_live: MqExchangeConfig,
    pub md_replay: MqExchangeConfig,
    pub ind: MqExchangeConfig,
    pub dlx: MqExchangeConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MqExchangeConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub durable: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MqQueueConfig {
    pub name: String,
    pub bind: Vec<MqBinding>,
    /// x-message-ttl in milliseconds. Messages older than this are dropped.
    #[serde(default)]
    pub message_ttl_ms: Option<u32>,
    /// x-max-length (message count). Combined with x-overflow=drop-head.
    #[serde(default)]
    pub max_length: Option<u32>,
    /// x-max-length-bytes. Combined with x-overflow=drop-head.
    #[serde(default)]
    pub max_length_bytes: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MqBinding {
    pub exchange: String,
    pub routing_key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LlmConfig {
    #[serde(default = "default_llm_request_enabled")]
    pub request_enabled: bool,
    #[serde(default = "default_llm_default_model")]
    pub default_model: String,
    #[serde(default = "default_llm_prompt_template")]
    pub prompt_template: String,
    #[serde(default = "default_symbol")]
    pub symbol: String,
    #[serde(default = "default_queue_key")]
    pub queue_key: String,
    #[serde(default = "default_llm_purge_queue_on_start")]
    pub purge_queue_on_start: bool,
    #[serde(default = "default_call_interval_secs")]
    pub call_interval_secs: u64,
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,
    #[serde(default = "default_bundle_settle_ms")]
    pub bundle_settle_ms: u64,
    #[serde(default = "default_bundle_stale_secs")]
    pub bundle_stale_secs: u64,
    #[serde(default = "default_bundle_consume_stale_secs")]
    pub bundle_consume_stale_secs: u64,
    #[serde(default = "default_bundle_execution_stale_secs")]
    pub bundle_execution_stale_secs: u64,
    #[serde(default)]
    pub temp_cache_retention_hours: Option<u64>,
    #[serde(default, rename = "temp_cache_retention_minutes")]
    pub temp_cache_retention_minutes_legacy: Option<u64>,
    #[serde(default = "default_call_schedule_minutes")]
    pub call_schedule_minutes: Vec<u8>,
    #[serde(default)]
    pub call_schedule_minutes_by_model: HashMap<String, Vec<u8>>,
    #[serde(default)]
    pub min_invoke_interval_secs_by_model: HashMap<String, u64>,
    #[serde(default = "default_print_response")]
    pub print_response: bool,
    #[serde(default = "default_telegram_signal_decisions")]
    pub telegram_signal_decisions: Vec<String>,
    #[serde(default = "default_x_signal_decisions")]
    pub x_signal_decisions: Vec<String>,
    #[serde(default = "default_indicator_codes")]
    pub indicator_codes: Vec<String>,
    #[serde(default = "default_models")]
    pub models: Vec<LlmModelConfig>,
    #[serde(default)]
    pub execution: LlmExecutionConfig,
}

impl Default for LlmConfig {
    fn default() -> Self {
        Self {
            request_enabled: default_llm_request_enabled(),
            default_model: default_llm_default_model(),
            prompt_template: default_llm_prompt_template(),
            symbol: default_symbol(),
            queue_key: default_queue_key(),
            purge_queue_on_start: default_llm_purge_queue_on_start(),
            call_interval_secs: default_call_interval_secs(),
            request_timeout_secs: default_request_timeout_secs(),
            bundle_settle_ms: default_bundle_settle_ms(),
            bundle_stale_secs: default_bundle_stale_secs(),
            bundle_consume_stale_secs: default_bundle_consume_stale_secs(),
            bundle_execution_stale_secs: default_bundle_execution_stale_secs(),
            temp_cache_retention_hours: Some(default_temp_cache_retention_hours()),
            temp_cache_retention_minutes_legacy: None,
            call_schedule_minutes: default_call_schedule_minutes(),
            call_schedule_minutes_by_model: HashMap::new(),
            min_invoke_interval_secs_by_model: HashMap::new(),
            print_response: default_print_response(),
            telegram_signal_decisions: default_telegram_signal_decisions(),
            x_signal_decisions: default_x_signal_decisions(),
            indicator_codes: default_indicator_codes(),
            models: default_models(),
            execution: LlmExecutionConfig::default(),
        }
    }
}

impl LlmConfig {
    pub fn temp_cache_retention_minutes(&self) -> u64 {
        self.temp_cache_retention_hours
            .and_then(|hours| hours.checked_mul(60))
            .or(self.temp_cache_retention_minutes_legacy)
            .unwrap_or_else(default_temp_cache_retention_minutes)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct LlmExecutionConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub account_margin_ratio: f64,
    #[serde(default = "default_execution_margin_usdt")]
    pub margin_usdt: f64,
    #[serde(
        default = "default_execution_default_leverage_ratio",
        alias = "default_leverage"
    )]
    pub default_leverage_ratio: f64,
    #[serde(default = "default_execution_max_leverage")]
    pub max_leverage: u32,
    #[serde(default = "default_execution_hedge_mode")]
    pub hedge_mode: bool,
    #[serde(default = "default_execution_recv_window_ms")]
    pub recv_window_ms: u64,
    #[serde(default = "default_execution_place_exit_orders")]
    pub place_exit_orders: bool,
    #[serde(default)]
    pub entry_sl_remap: ExecutionEntrySlRemapConfig,
    #[serde(default = "default_execution_min_distance_v")]
    pub min_distance_v: f64,
    #[serde(default = "default_execution_min_rr")]
    pub min_rr: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExecutionEntrySlRemapConfig {
    #[serde(default = "default_execution_entry_sl_remap_enabled")]
    pub enabled: bool,
    #[serde(default = "default_execution_entry_to_sl_distance_pct")]
    pub entry_to_sl_distance_pct: f64,
}

impl Default for ExecutionEntrySlRemapConfig {
    fn default() -> Self {
        Self {
            enabled: default_execution_entry_sl_remap_enabled(),
            entry_to_sl_distance_pct: default_execution_entry_to_sl_distance_pct(),
        }
    }
}

impl Default for LlmExecutionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            dry_run: false,
            account_margin_ratio: 0.0,
            margin_usdt: default_execution_margin_usdt(),
            default_leverage_ratio: default_execution_default_leverage_ratio(),
            max_leverage: default_execution_max_leverage(),
            hedge_mode: default_execution_hedge_mode(),
            recv_window_ms: default_execution_recv_window_ms(),
            place_exit_orders: default_execution_place_exit_orders(),
            entry_sl_remap: ExecutionEntrySlRemapConfig::default(),
            min_distance_v: default_execution_min_distance_v(),
            min_rr: default_execution_min_rr(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct LlmModelConfig {
    pub name: String,
    pub provider: String,
    pub model: String,
    #[serde(default)]
    pub use_openrouter: Option<bool>,
    #[serde(default = "default_model_enabled")]
    pub enabled: bool,
    #[serde(default = "default_model_temperature")]
    pub temperature: f64,
    #[serde(default = "default_model_max_tokens")]
    pub max_tokens: u32,
    /// Qwen only: set to true to enable thinking (extended reasoning) mode.
    #[serde(default)]
    pub enable_thinking: Option<bool>,
    /// Optional stage-1 scan reasoning effort hint for OpenAI-compatible custom_llm backends.
    #[serde(default)]
    pub stage1_reasoning: Option<String>,
    /// Optional stage-2 finalize reasoning effort hint for OpenAI-compatible custom_llm backends.
    #[serde(default)]
    pub stage2_reasoning: Option<String>,
    /// Legacy fallback reasoning effort hint. Used when stage1/stage2 reasoning is not set.
    #[serde(default)]
    pub reasoning: Option<String>,
}

fn default_symbol() -> String {
    "ETHUSDT".to_string()
}

fn default_queue_key() -> String {
    "llm_indicator_minute".to_string()
}

fn default_llm_purge_queue_on_start() -> bool {
    true
}

fn default_llm_request_enabled() -> bool {
    true
}

fn default_call_interval_secs() -> u64 {
    900
}

fn default_request_timeout_secs() -> u64 {
    45
}

fn default_bundle_settle_ms() -> u64 {
    1000
}

fn default_bundle_stale_secs() -> u64 {
    59
}

fn default_bundle_consume_stale_secs() -> u64 {
    300
}

fn default_bundle_execution_stale_secs() -> u64 {
    300
}

fn default_temp_cache_retention_hours() -> u64 {
    12
}

fn default_temp_cache_retention_minutes() -> u64 {
    default_temp_cache_retention_hours() * 60
}

fn default_call_schedule_minutes() -> Vec<u8> {
    vec![0, 15, 30, 45]
}

fn validate_schedule_minutes(minutes: &[u8], field_name: &str) -> Result<()> {
    if minutes.is_empty() {
        return Err(anyhow!("{} cannot be empty", field_name));
    }
    let mut seen = std::collections::HashSet::new();
    for minute in minutes {
        if *minute > 59 {
            return Err(anyhow!(
                "{} contains invalid minute {}; expected 0..=59",
                field_name,
                minute
            ));
        }
        if !seen.insert(*minute) {
            return Err(anyhow!(
                "{} contains duplicate minute {}",
                field_name,
                minute
            ));
        }
    }
    Ok(())
}

fn validate_temp_cache_retention_config(llm: &LlmConfig) -> Result<()> {
    if llm.temp_cache_retention_hours.is_some() && llm.temp_cache_retention_minutes_legacy.is_some()
    {
        return Err(anyhow!(
            "llm.temp_cache_retention_hours and llm.temp_cache_retention_minutes cannot both be set"
        ));
    }
    if llm.temp_cache_retention_hours == Some(0) {
        return Err(anyhow!("llm.temp_cache_retention_hours must be > 0"));
    }
    if llm.temp_cache_retention_minutes_legacy == Some(0) {
        return Err(anyhow!("llm.temp_cache_retention_minutes must be > 0"));
    }
    Ok(())
}

fn default_print_response() -> bool {
    true
}

fn default_telegram_signal_decisions() -> Vec<String> {
    vec!["long".to_string(), "short".to_string()]
}

fn default_x_signal_decisions() -> Vec<String> {
    default_telegram_signal_decisions()
}

fn default_model_enabled() -> bool {
    true
}

fn default_model_temperature() -> f64 {
    0.1
}

fn default_model_max_tokens() -> u32 {
    1200
}

fn default_claude_api_url() -> String {
    "https://api.anthropic.com/v1/messages".to_string()
}

fn default_llm_default_model() -> String {
    "claude".to_string()
}

fn default_llm_prompt_template() -> String {
    "big_opportunity".to_string()
}

fn default_qwen_base_api_url() -> String {
    "https://dashscope-intl.aliyuncs.com/compatible-mode/v1".to_string()
}

fn default_qwen_model() -> String {
    "qwen3-max".to_string()
}

fn default_gemini_base_api_url() -> String {
    "https://generativelanguage.googleapis.com/v1beta".to_string()
}

fn default_gemini_model() -> String {
    "gemini-2.5-pro".to_string()
}

fn default_openrouter_base_api_url() -> String {
    "https://openrouter.ai/api/v1".to_string()
}

fn default_grok_base_api_url() -> String {
    "https://api.x.ai/v1".to_string()
}

fn default_grok_model() -> String {
    "grok-4-1-fast-non-reasoning".to_string()
}

fn default_telegram_base_api_url() -> String {
    "https://api.telegram.org".to_string()
}

fn default_x_base_api_url() -> String {
    "https://api.x.com/2".to_string()
}

fn default_database_host() -> String {
    "127.0.0.1".to_string()
}

fn default_database_port() -> u16 {
    5432
}

fn default_database_sslmode() -> String {
    "disable".to_string()
}

fn default_claude_api_version() -> String {
    "2023-06-01".to_string()
}

fn default_claude_api_mode() -> String {
    "batch".to_string()
}

fn default_claude_batch_api_url() -> String {
    "https://api.anthropic.com/v1/messages/batches".to_string()
}

fn default_claude_batch_poll_interval_secs() -> u64 {
    30
}

fn default_claude_batch_wait_timeout_secs() -> u64 {
    3600
}

fn default_binance_futures_rest_api_url() -> String {
    "https://fapi.binance.com".to_string()
}

fn default_indicator_codes() -> Vec<String> {
    vec![
        "price_volume_structure".to_string(),
        "footprint".to_string(),
        "divergence".to_string(),
        "liquidation_density".to_string(),
        "orderbook_depth".to_string(),
        "fvg".to_string(),
        "absorption".to_string(),
        "initiation".to_string(),
        "bullish_absorption".to_string(),
        "bullish_initiation".to_string(),
        "bearish_absorption".to_string(),
        "bearish_initiation".to_string(),
        "buying_exhaustion".to_string(),
        "selling_exhaustion".to_string(),
        "cvd_pack".to_string(),
        "whale_trades".to_string(),
        "funding_rate".to_string(),
        "vpin".to_string(),
        "avwap".to_string(),
        "kline_history".to_string(),
        "ema_trend_regime".to_string(),
        "tpo_market_profile".to_string(),
        "rvwap_sigma_bands".to_string(),
        "high_volume_pulse".to_string(),
    ]
}

#[cfg(test)]
mod tests {
    use super::{
        default_indicator_codes, validate_temp_cache_retention_config, LlmConfig, LlmModelConfig,
    };

    #[test]
    fn default_indicator_codes_include_fvg() {
        let codes = default_indicator_codes();
        assert!(codes.iter().any(|code| code == "fvg"));
    }

    #[test]
    fn default_llm_request_enabled_is_true() {
        assert!(LlmConfig::default().request_enabled);
    }

    #[test]
    fn default_temp_cache_retention_is_twelve_hours() {
        let config = LlmConfig::default();
        assert_eq!(config.temp_cache_retention_hours, Some(12));
        assert_eq!(config.temp_cache_retention_minutes(), 12 * 60);
    }

    #[test]
    fn temp_cache_retention_uses_configured_hours() {
        let config: LlmConfig =
            serde_yaml::from_str("temp_cache_retention_hours: 6").expect("parse llm config");
        assert_eq!(config.temp_cache_retention_hours, Some(6));
        assert_eq!(config.temp_cache_retention_minutes(), 6 * 60);
    }

    #[test]
    fn temp_cache_retention_supports_legacy_minutes_field() {
        let config: LlmConfig =
            serde_yaml::from_str("temp_cache_retention_minutes: 90").expect("parse llm config");
        assert_eq!(config.temp_cache_retention_hours, None);
        assert_eq!(config.temp_cache_retention_minutes_legacy, Some(90));
        assert_eq!(config.temp_cache_retention_minutes(), 90);
    }

    #[test]
    fn temp_cache_retention_rejects_conflicting_units() {
        let config: LlmConfig = serde_yaml::from_str(
            "temp_cache_retention_hours: 12\ntemp_cache_retention_minutes: 60",
        )
        .expect("parse llm config");
        let err =
            validate_temp_cache_retention_config(&config).expect_err("expected conflict error");
        assert!(err
            .to_string()
            .contains("temp_cache_retention_hours and llm.temp_cache_retention_minutes"));
    }

    #[test]
    fn default_execution_trade_gates_are_expected_values() {
        let execution = LlmConfig::default().execution;
        assert!((execution.min_distance_v - 1.0).abs() < f64::EPSILON);
        assert!((execution.min_rr - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn model_reasoning_for_stage_prefers_stage_specific_then_legacy() {
        let model = LlmModelConfig {
            name: "custom_llm".to_string(),
            provider: "custom_llm".to_string(),
            model: "gpt-5.4-xhigh".to_string(),
            use_openrouter: None,
            enabled: true,
            temperature: 0.1,
            max_tokens: 1000,
            enable_thinking: None,
            stage1_reasoning: Some("high".to_string()),
            stage2_reasoning: Some("medium".to_string()),
            reasoning: Some("low".to_string()),
        };
        assert_eq!(model.reasoning_for_stage(true), Some("high"));
        assert_eq!(model.reasoning_for_stage(false), Some("medium"));

        let legacy_only = LlmModelConfig {
            stage1_reasoning: None,
            stage2_reasoning: None,
            reasoning: Some("xhigh".to_string()),
            ..model
        };
        assert_eq!(legacy_only.reasoning_for_stage(true), Some("xhigh"));
        assert_eq!(legacy_only.reasoning_for_stage(false), Some("xhigh"));
    }
}

fn default_models() -> Vec<LlmModelConfig> {
    vec![LlmModelConfig {
        name: "claude46_sonnet".to_string(),
        provider: "claude".to_string(),
        model: "claude-sonnet-4-6".to_string(),
        use_openrouter: None,
        enabled: true,
        temperature: default_model_temperature(),
        max_tokens: default_model_max_tokens(),
        enable_thinking: None,
        stage1_reasoning: None,
        stage2_reasoning: None,
        reasoning: None,
    }]
}

fn default_execution_margin_usdt() -> f64 {
    50.0
}

fn default_execution_max_leverage() -> u32 {
    10
}

fn default_execution_default_leverage_ratio() -> f64 {
    30.0
}

fn default_execution_hedge_mode() -> bool {
    true
}

fn default_execution_recv_window_ms() -> u64 {
    5000
}

fn default_execution_place_exit_orders() -> bool {
    true
}

fn default_execution_entry_sl_remap_enabled() -> bool {
    true
}

fn default_execution_entry_to_sl_distance_pct() -> f64 {
    100.0
}

fn default_execution_min_distance_v() -> f64 {
    1.0
}

fn default_execution_min_rr() -> f64 {
    2.0
}

pub fn load_config(path: &str) -> Result<RootConfig> {
    let text = std::fs::read_to_string(path)?;
    let cfg: RootConfig = serde_yaml::from_str(&text)?;
    validate_config(&cfg)?;
    Ok(cfg)
}

impl RootConfig {
    fn active_default_model_selector(&self) -> String {
        let llm_default = self.llm.default_model.trim().to_ascii_lowercase();
        if !llm_default.is_empty() {
            return llm_default;
        }
        self.api.default_model.trim().to_ascii_lowercase()
    }

    pub fn active_default_model(&self) -> String {
        let selector = self.active_default_model_selector();
        if is_supported_provider_name(&selector) {
            return selector;
        }
        if let Some(model) = self
            .llm
            .models
            .iter()
            .find(|m| m.enabled && m.name.trim().eq_ignore_ascii_case(&selector))
        {
            return model.provider.trim().to_ascii_lowercase();
        }
        selector
    }

    pub fn selected_enabled_models_for_default(&self) -> Vec<LlmModelConfig> {
        let selector = self.active_default_model_selector();
        if !is_supported_provider_name(&selector) {
            let by_name = self
                .llm
                .models
                .iter()
                .filter(|m| m.enabled && m.name.trim().eq_ignore_ascii_case(&selector))
                .cloned()
                .collect::<Vec<_>>();
            if !by_name.is_empty() {
                return by_name;
            }
        }

        let provider = self.active_default_model();
        self.llm
            .models
            .iter()
            .filter(|m| m.enabled && m.provider.eq_ignore_ascii_case(&provider))
            .cloned()
            .collect::<Vec<_>>()
    }
}

impl LlmModelConfig {
    pub fn should_use_openrouter(&self) -> bool {
        if self.provider.eq_ignore_ascii_case("gemini") {
            self.use_openrouter.unwrap_or(true)
        } else {
            self.use_openrouter.unwrap_or(false)
        }
    }

    pub fn reasoning_for_stage(&self, is_stage1: bool) -> Option<&str> {
        let stage_specific = if is_stage1 {
            self.stage1_reasoning.as_deref()
        } else {
            self.stage2_reasoning.as_deref()
        };
        stage_specific
            .or(self.reasoning.as_deref())
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }
}

fn validate_config(cfg: &RootConfig) -> Result<()> {
    let default_selector = cfg.active_default_model_selector();
    let default_provider = cfg.active_default_model();
    if !is_supported_provider_name(&default_provider) {
        return Err(anyhow!(
            "llm.default_model must be one of [claude, qwen, custom_llm, gemini, grok] or an enabled llm.models[].name, got {}",
            default_selector
        ));
    }
    let prompt_template = cfg.llm.prompt_template.trim().to_ascii_lowercase();
    if prompt_template != "big_opportunity" && prompt_template != "medium_large_opportunity" {
        return Err(anyhow!(
            "llm.prompt_template must be one of [big_opportunity, medium_large_opportunity]"
        ));
    }

    if cfg.llm.call_interval_secs == 0 {
        return Err(anyhow!("llm.call_interval_secs must be > 0"));
    }
    if cfg.llm.request_timeout_secs == 0 {
        return Err(anyhow!("llm.request_timeout_secs must be > 0"));
    }
    if cfg.llm.bundle_settle_ms == 0 {
        return Err(anyhow!("llm.bundle_settle_ms must be > 0"));
    }
    validate_temp_cache_retention_config(&cfg.llm)?;
    validate_schedule_minutes(&cfg.llm.call_schedule_minutes, "llm.call_schedule_minutes")?;
    for (provider, minutes) in &cfg.llm.call_schedule_minutes_by_model {
        let key = provider.trim().to_ascii_lowercase();
        if key != "claude"
            && key != "qwen"
            && key != "custom_llm"
            && key != "gemini"
            && key != "grok"
        {
            return Err(anyhow!(
                "llm.call_schedule_minutes_by_model key must be one of [claude, qwen, custom_llm, gemini, grok], got {}",
                provider
            ));
        }
        validate_schedule_minutes(
            minutes,
            &format!("llm.call_schedule_minutes_by_model.{}", key),
        )?;
    }
    for (provider, secs) in &cfg.llm.min_invoke_interval_secs_by_model {
        let key = provider.trim().to_ascii_lowercase();
        if key != "claude"
            && key != "qwen"
            && key != "custom_llm"
            && key != "gemini"
            && key != "grok"
        {
            return Err(anyhow!(
                "llm.min_invoke_interval_secs_by_model key must be one of [claude, qwen, custom_llm, gemini, grok], got {}",
                provider
            ));
        }
        if *secs == 0 {
            return Err(anyhow!(
                "llm.min_invoke_interval_secs_by_model.{} must be > 0",
                key
            ));
        }
    }
    validate_telegram_signal_decisions(&cfg.llm.telegram_signal_decisions)?;
    validate_x_signal_decisions(&cfg.llm.x_signal_decisions)?;
    if !cfg.mq.queues.contains_key(&cfg.llm.queue_key) {
        return Err(anyhow!(
            "llm.queue_key={} not found in mq.queues",
            cfg.llm.queue_key
        ));
    }

    let enabled_models = cfg
        .llm
        .models
        .iter()
        .filter(|m| m.enabled)
        .collect::<Vec<_>>();
    if enabled_models.is_empty() && default_provider == "claude" {
        return Err(anyhow!(
            "llm.models has no enabled model; claude requires at least one enabled llm.models item"
        ));
    }
    for model in &enabled_models {
        if model.name.trim().is_empty() {
            return Err(anyhow!("llm.models[].name is empty"));
        }
        if model.provider.trim().is_empty() {
            return Err(anyhow!("llm.models[].provider is empty"));
        }
        if model.model.trim().is_empty() {
            return Err(anyhow!("llm.models[].model is empty"));
        }
        if model.max_tokens == 0 {
            return Err(anyhow!("llm.models[].max_tokens must be > 0"));
        }
    }

    let selected_enabled_models = cfg.selected_enabled_models_for_default();

    let claude_needed = default_provider == "claude";
    if claude_needed && cfg.api.claude.resolved_api_key().trim().is_empty() {
        return Err(anyhow!("api.claude.api_key is empty"));
    }
    if claude_needed {
        if !cfg.api.claude.use_batch_api() {
            return Err(anyhow!("api.claude.mode must be batch for llm subsystem"));
        }
        if cfg.api.claude.batch_poll_interval_secs == 0 {
            return Err(anyhow!("api.claude.batch_poll_interval_secs must be > 0"));
        }
        if cfg.api.claude.batch_wait_timeout_secs == 0 {
            return Err(anyhow!("api.claude.batch_wait_timeout_secs must be > 0"));
        }
    }

    if default_provider == "qwen" {
        if cfg.api.qwen.resolved_api_key().trim().is_empty() {
            return Err(anyhow!("api.qwen.api_key is empty"));
        }
        if cfg.api.qwen.base_api_url.trim().is_empty() {
            return Err(anyhow!("api.qwen.base_api_url is empty"));
        }
        if cfg.api.qwen.model.trim().is_empty() {
            return Err(anyhow!("api.qwen.model is empty"));
        }
    }

    if default_provider == "custom_llm" {
        if cfg.api.custom_llm.resolved_api_key().trim().is_empty() {
            return Err(anyhow!("api.custom_llm.api_key is empty"));
        }
        if cfg.api.custom_llm.base_api_url.trim().is_empty() {
            return Err(anyhow!("api.custom_llm.base_api_url is empty"));
        }
        if cfg.api.custom_llm.model.trim().is_empty() {
            return Err(anyhow!("api.custom_llm.model is empty"));
        }
    }

    if default_provider == "gemini" {
        let selected_uses_openrouter = selected_enabled_models
            .iter()
            .any(|m| m.should_use_openrouter());
        let selected_uses_direct_gemini = selected_enabled_models
            .iter()
            .any(|m| !m.should_use_openrouter());

        if selected_uses_openrouter {
            if cfg.api.openrouter.resolved_api_key().trim().is_empty() {
                return Err(anyhow!("api.openrouter.api_key is empty"));
            }
            if cfg.api.openrouter.base_api_url.trim().is_empty() {
                return Err(anyhow!("api.openrouter.base_api_url is empty"));
            }
        }

        if selected_uses_direct_gemini || !selected_uses_openrouter {
            if cfg.api.gemini.resolved_api_key().trim().is_empty() {
                return Err(anyhow!("api.gemini.api_key is empty"));
            }
            if cfg.api.gemini.base_api_url.trim().is_empty() {
                return Err(anyhow!("api.gemini.base_api_url is empty"));
            }
            if cfg.api.gemini.model.trim().is_empty() {
                return Err(anyhow!("api.gemini.model is empty"));
            }
        }
    }

    if default_provider == "grok" {
        if cfg.api.grok.resolved_api_key().trim().is_empty() {
            return Err(anyhow!("api.grok.api_key is empty"));
        }
        if cfg.api.grok.base_api_url.trim().is_empty() {
            return Err(anyhow!("api.grok.base_api_url is empty"));
        }
        if cfg.api.grok.model.trim().is_empty() {
            return Err(anyhow!("api.grok.model is empty"));
        }
    }

    let telegram_token = cfg.api.telegram.resolved_token();
    let telegram_chat_id = cfg.api.telegram.resolved_chat_id();
    if telegram_token.trim().is_empty() != telegram_chat_id.trim().is_empty() {
        return Err(anyhow!(
            "api.telegram.token and api.telegram.chat_id must both be set or both empty"
        ));
    }

    let x_consumer_key = cfg.api.x.resolved_consumer_key();
    let x_secret_key = cfg.api.x.resolved_secret_key();
    let x_access_token = cfg.api.x.resolved_access_token();
    let x_access_token_secret = cfg.api.x.resolved_access_token_secret();
    let x_required_values = [
        x_consumer_key.trim(),
        x_secret_key.trim(),
        x_access_token.trim(),
        x_access_token_secret.trim(),
    ];
    let x_all_empty = x_required_values.iter().all(|v| v.is_empty());
    let x_all_set = x_required_values.iter().all(|v| !v.is_empty());
    if !x_all_empty && !x_all_set {
        return Err(anyhow!(
            "api.x.consumer_key, api.x.secret_key, api.x.access_token and api.x.access_token_secret must all be set or all be empty"
        ));
    }
    if x_all_set && cfg.api.x.base_api_url.trim().is_empty() {
        return Err(anyhow!("api.x.base_api_url is empty"));
    }

    if !cfg
        .llm
        .execution
        .entry_sl_remap
        .entry_to_sl_distance_pct
        .is_finite()
    {
        return Err(anyhow!(
            "llm.execution.entry_sl_remap.entry_to_sl_distance_pct must be finite"
        ));
    }
    if !(0.0..=100.0).contains(&cfg.llm.execution.entry_sl_remap.entry_to_sl_distance_pct) {
        return Err(anyhow!(
            "llm.execution.entry_sl_remap.entry_to_sl_distance_pct must be between 0 and 100"
        ));
    }
    if !cfg.llm.execution.min_distance_v.is_finite() {
        return Err(anyhow!("llm.execution.min_distance_v must be finite"));
    }
    if cfg.llm.execution.min_distance_v < 0.0 {
        return Err(anyhow!("llm.execution.min_distance_v must be >= 0"));
    }
    if !cfg.llm.execution.min_rr.is_finite() {
        return Err(anyhow!("llm.execution.min_rr must be finite"));
    }
    if cfg.llm.execution.min_rr <= 0.0 {
        return Err(anyhow!("llm.execution.min_rr must be > 0"));
    }

    if cfg.llm.execution.enabled {
        if default_provider == "claude" && selected_enabled_models.len() != 1 {
            return Err(anyhow!(
                "llm.execution.enabled with llm.default_model=claude requires exactly one enabled claude model"
            ));
        }
        if default_provider == "qwen" && selected_enabled_models.len() > 1 {
            return Err(anyhow!(
                "llm.execution.enabled with llm.default_model=qwen supports at most one enabled qwen model"
            ));
        }
        if default_provider == "custom_llm" && selected_enabled_models.len() > 1 {
            return Err(anyhow!(
                "llm.execution.enabled with llm.default_model=custom_llm supports at most one enabled custom_llm model"
            ));
        }
        if default_provider == "gemini" && selected_enabled_models.len() > 1 {
            return Err(anyhow!(
                "llm.execution.enabled with llm.default_model=gemini supports at most one enabled gemini model"
            ));
        }
        if default_provider == "grok" && selected_enabled_models.len() > 1 {
            return Err(anyhow!(
                "llm.execution.enabled with llm.default_model=grok supports at most one enabled grok model"
            ));
        }
        if cfg.api.binance.resolved_api_key().trim().is_empty() {
            return Err(anyhow!("api.binance.api_key is empty"));
        }
        if cfg.api.binance.resolved_api_secret().trim().is_empty() {
            return Err(anyhow!("api.binance.api_secret is empty"));
        }
        if cfg.llm.execution.margin_usdt <= 0.0 {
            return Err(anyhow!("llm.execution.margin_usdt must be > 0"));
        }
        if !(0.0..=1.0).contains(&cfg.llm.execution.account_margin_ratio) {
            return Err(anyhow!(
                "llm.execution.account_margin_ratio must be between 0.0 and 1.0"
            ));
        }
        if cfg.llm.execution.max_leverage == 0 {
            return Err(anyhow!("llm.execution.max_leverage must be > 0"));
        }
        if !cfg.llm.execution.default_leverage_ratio.is_finite() {
            return Err(anyhow!(
                "llm.execution.default_leverage_ratio must be finite"
            ));
        }
        if cfg.llm.execution.default_leverage_ratio <= 0.0 {
            return Err(anyhow!("llm.execution.default_leverage_ratio must be > 0"));
        }
        if cfg.llm.execution.recv_window_ms == 0 {
            return Err(anyhow!("llm.execution.recv_window_ms must be > 0"));
        }
    }

    Ok(())
}

fn resolve_secret(raw: &str) -> String {
    std::env::var(raw).unwrap_or_else(|_| raw.to_string())
}

fn is_supported_provider_name(value: &str) -> bool {
    matches!(value, "claude" | "qwen" | "custom_llm" | "gemini" | "grok")
}

fn validate_telegram_signal_decisions(decisions: &[String]) -> Result<()> {
    validate_signal_decisions(
        decisions,
        "llm.telegram_signal_decisions",
        "llm.telegram_signal_decisions",
    )
}

fn validate_x_signal_decisions(decisions: &[String]) -> Result<()> {
    validate_signal_decisions(
        decisions,
        "llm.x_signal_decisions",
        "llm.x_signal_decisions",
    )
}

fn validate_signal_decisions(
    decisions: &[String],
    field_label: &str,
    duplicate_field_label: &str,
) -> Result<()> {
    let mut seen = std::collections::HashSet::new();
    for raw in decisions {
        let normalized = raw.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            return Err(anyhow!("{} cannot contain empty values", field_label));
        }
        if !is_supported_signal_decision(&normalized) {
            return Err(anyhow!(
                "{} contains unsupported value {}; supported: [long, short, no_trade, close, add, reduce, hold, modify_tpsl, modify_maker]",
                field_label,
                raw
            ));
        }
        if !seen.insert(normalized.clone()) {
            return Err(anyhow!(
                "{} contains duplicate value {}",
                duplicate_field_label,
                normalized
            ));
        }
    }
    Ok(())
}

fn is_supported_signal_decision(value: &str) -> bool {
    matches!(
        value,
        "long"
            | "short"
            | "no_trade"
            | "close"
            | "add"
            | "reduce"
            | "hold"
            | "modify_tpsl"
            | "modify_maker"
    )
}
