use anyhow::{anyhow, Context, Result};
use lapin::{
    options::{
        ConfirmSelectOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
        QueuePurgeOptions,
    },
    types::{AMQPValue, FieldTable, LongString, ShortString},
    Channel, Connection, ConnectionProperties, ExchangeKind,
};
use log::LevelFilter;
use serde::Deserialize;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    ConnectOptions, PgPool,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use uuid::Uuid;

const CONFIG_PATH: &str = "config/config.yaml";
const GROUP_QUEUE_OB_HEAVY: &str = "indicator_group_ob_heavy";
const GROUP_QUEUE_FLOW: &str = "indicator_group_flow";
const GROUP_QUEUE_DERIV: &str = "indicator_group_deriv";

#[derive(Debug, Clone, Deserialize)]
pub struct RootConfig {
    pub app: AppSection,
    pub database: DatabaseConfig,
    #[serde(default)]
    pub indicator: IndicatorConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    pub mq: MqConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AppSection {
    pub name: String,
    pub env: String,
    pub timezone: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password_env: String,
    pub connect_timeout_secs: Option<u64>,
    pub application_name: Option<String>,
    pub sslmode: Option<String>,
    pub options: Option<String>,
    pub pool: Option<DbPoolConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DbPoolConfig {
    pub min_connections: Option<u32>,
    pub max_connections: Option<u32>,
    pub acquire_timeout_secs: Option<u64>,
    pub idle_timeout_secs: Option<u64>,
    pub max_lifetime_secs: Option<u64>,
    pub test_before_acquire: Option<bool>,
}

impl DatabaseConfig {
    pub fn resolved_password(&self) -> String {
        resolve_secret(&self.password_env)
    }

    pub fn postgres_uri(&self) -> String {
        let user = urlencoding::encode(&self.user);
        let resolved_password = self.resolved_password();
        let password = urlencoding::encode(&resolved_password);
        let db = urlencoding::encode(&self.database);
        let mut uri = format!(
            "postgres://{}:{}@{}:{}/{}",
            user, password, self.host, self.port, db
        );

        let mut params = Vec::new();
        if let Some(v) = &self.application_name {
            params.push(format!("application_name={}", urlencoding::encode(v)));
        }
        if let Some(v) = &self.sslmode {
            params.push(format!("sslmode={}", urlencoding::encode(v)));
        }
        if let Some(v) = &self.options {
            params.push(format!("options={}", urlencoding::encode(v)));
        }
        if !params.is_empty() {
            uri.push('?');
            uri.push_str(&params.join("&"));
        }

        uri
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
    /// x-message-ttl in milliseconds. Messages older than this are dropped
    /// instead of triggering broker flow control back to publishers.
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
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default = "default_sql_log_enabled")]
    pub sql_log: bool,
    #[serde(default = "default_slow_sql_threshold_ms")]
    pub slow_sql_threshold_ms: u64,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            sql_log: default_sql_log_enabled(),
            slow_sql_threshold_ms: default_slow_sql_threshold_ms(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct IndicatorConfig {
    #[serde(default = "default_symbol")]
    pub symbol: String,
    #[serde(default = "default_export_interval_secs")]
    pub export_interval_secs: u64,
    #[serde(default = "default_export_dir")]
    pub export_dir: String,
    #[serde(default = "default_enable_file_export")]
    pub enable_file_export: bool,
    #[serde(default = "default_whale_threshold_usdt")]
    pub whale_threshold_usdt: f64,
    #[serde(default)]
    pub kline_history: KlineHistoryConfig,
    #[serde(default)]
    pub tpo_market_profile: TpoMarketProfileConfig,
    #[serde(default)]
    pub rvwap_sigma_bands: RvwapSigmaBandsConfig,
    #[serde(default)]
    pub high_volume_pulse: HighVolumePulseConfig,
    #[serde(default)]
    pub ema_trend_regime: EmaTrendRegimeConfig,
    #[serde(default)]
    pub fvg: FvgConfig,
    #[serde(default)]
    pub divergence: DivergenceConfig,
    #[serde(default = "default_window_codes")]
    pub window_codes: Vec<String>,
    #[serde(default = "default_consume_mode")]
    #[allow(dead_code)]
    pub consume_mode: String, // live | replay
    #[serde(default = "default_live_drop_stale_event_secs")]
    pub live_drop_stale_event_secs: i64,
    #[serde(default = "default_live_drop_stale_enabled")]
    pub live_drop_stale_enabled: bool,
    #[serde(default = "default_watermark_lateness_secs")]
    pub watermark_lateness_secs: i64,
    #[serde(default = "default_live_purge_on_start")]
    pub live_purge_on_start: bool,
    /// Cap startup historical catch-up window in minutes.
    /// 0 means no cap (keep existing behavior).
    #[serde(default = "default_startup_max_catchup_minutes")]
    pub startup_max_catchup_minutes: i64,
    /// Startup backfill query batch size (rows per SQL fetch).
    /// Lower values reduce single-query latency under heavy orderbook payloads.
    #[serde(default = "default_startup_backfill_batch_size")]
    pub startup_backfill_batch_size: i64,
    /// Path to gzip-compressed state snapshot file.  Empty string = disabled.
    #[serde(default)]
    pub snapshot_file_path: String,
    /// Maximum age in hours for a snapshot to be considered valid.
    #[serde(default = "default_snapshot_max_age_hours")]
    pub snapshot_max_age_hours: u64,
    /// When true, run full recompute alongside snapshot path and compare results (dev only).
    #[serde(default)]
    pub snapshot_verify_full_recompute: bool,
}

impl Default for IndicatorConfig {
    fn default() -> Self {
        Self {
            symbol: default_symbol(),
            export_interval_secs: default_export_interval_secs(),
            export_dir: default_export_dir(),
            enable_file_export: default_enable_file_export(),
            whale_threshold_usdt: default_whale_threshold_usdt(),
            kline_history: KlineHistoryConfig::default(),
            tpo_market_profile: TpoMarketProfileConfig::default(),
            rvwap_sigma_bands: RvwapSigmaBandsConfig::default(),
            high_volume_pulse: HighVolumePulseConfig::default(),
            ema_trend_regime: EmaTrendRegimeConfig::default(),
            fvg: FvgConfig::default(),
            divergence: DivergenceConfig::default(),
            window_codes: default_window_codes(),
            consume_mode: default_consume_mode(),
            live_drop_stale_event_secs: default_live_drop_stale_event_secs(),
            live_drop_stale_enabled: default_live_drop_stale_enabled(),
            watermark_lateness_secs: default_watermark_lateness_secs(),
            live_purge_on_start: default_live_purge_on_start(),
            startup_max_catchup_minutes: default_startup_max_catchup_minutes(),
            startup_backfill_batch_size: default_startup_backfill_batch_size(),
            snapshot_file_path: String::new(),
            snapshot_max_age_hours: default_snapshot_max_age_hours(),
            snapshot_verify_full_recompute: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct KlineHistoryConfig {
    #[serde(default = "default_kline_history_bars_1m")]
    pub bars_1m: usize,
    #[serde(default = "default_kline_history_bars_15m")]
    pub bars_15m: usize,
    #[serde(default = "default_kline_history_bars_4h", alias = "bars_1h")]
    pub bars_4h: usize,
    #[serde(default = "default_kline_history_bars_1d")]
    pub bars_1d: usize,
    #[serde(default = "default_kline_history_fill_1d_from_db")]
    pub fill_1d_from_db: bool,
}

impl Default for KlineHistoryConfig {
    fn default() -> Self {
        Self {
            bars_1m: default_kline_history_bars_1m(),
            bars_15m: default_kline_history_bars_15m(),
            bars_4h: default_kline_history_bars_4h(),
            bars_1d: default_kline_history_bars_1d(),
            fill_1d_from_db: default_kline_history_fill_1d_from_db(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TpoMarketProfileConfig {
    #[serde(default = "default_tpo_rows_nb")]
    pub rows_nb: usize,
    #[serde(default = "default_tpo_value_area_pct")]
    pub value_area_pct: f64,
    #[serde(default = "default_tpo_session_windows")]
    pub session_windows: Vec<String>,
    #[serde(default = "default_tpo_ib_minutes")]
    pub ib_minutes: i64,
    #[serde(default = "default_tpo_dev_output_windows")]
    pub dev_output_windows: Vec<String>,
}

impl Default for TpoMarketProfileConfig {
    fn default() -> Self {
        Self {
            rows_nb: default_tpo_rows_nb(),
            value_area_pct: default_tpo_value_area_pct(),
            session_windows: default_tpo_session_windows(),
            ib_minutes: default_tpo_ib_minutes(),
            dev_output_windows: default_tpo_dev_output_windows(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RvwapSigmaBandsConfig {
    #[serde(default = "default_rvwap_windows")]
    pub windows: Vec<String>,
    #[serde(default = "default_rvwap_output_windows")]
    pub output_windows: Vec<String>,
    #[serde(default = "default_rvwap_min_samples")]
    pub min_samples: usize,
}

impl Default for RvwapSigmaBandsConfig {
    fn default() -> Self {
        Self {
            windows: default_rvwap_windows(),
            output_windows: default_rvwap_output_windows(),
            min_samples: default_rvwap_min_samples(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct HighVolumePulseConfig {
    #[serde(default = "default_high_volume_pulse_z_windows")]
    pub z_windows: Vec<String>,
    #[serde(default = "default_high_volume_pulse_summary_windows")]
    pub summary_windows: Vec<String>,
    #[serde(default = "default_high_volume_pulse_min_samples")]
    pub min_samples: usize,
}

impl Default for HighVolumePulseConfig {
    fn default() -> Self {
        Self {
            z_windows: default_high_volume_pulse_z_windows(),
            summary_windows: default_high_volume_pulse_summary_windows(),
            min_samples: default_high_volume_pulse_min_samples(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct EmaTrendRegimeConfig {
    #[serde(default = "default_ema_base_periods")]
    pub base_periods: Vec<usize>,
    #[serde(default = "default_ema_htf_periods")]
    pub htf_periods: Vec<usize>,
    #[serde(default = "default_ema_htf_windows")]
    pub htf_windows: Vec<String>,
    #[serde(default = "default_ema_output_windows")]
    pub output_windows: Vec<String>,
    #[serde(default = "default_ema_fill_from_db")]
    pub fill_from_db: bool,
    #[serde(default = "default_ema_db_bars_4h")]
    pub db_bars_4h: usize,
    #[serde(default = "default_ema_db_bars_1d")]
    pub db_bars_1d: usize,
}

impl Default for EmaTrendRegimeConfig {
    fn default() -> Self {
        Self {
            base_periods: default_ema_base_periods(),
            htf_periods: default_ema_htf_periods(),
            htf_windows: default_ema_htf_windows(),
            output_windows: default_ema_output_windows(),
            fill_from_db: default_ema_fill_from_db(),
            db_bars_4h: default_ema_db_bars_4h(),
            db_bars_1d: default_ema_db_bars_1d(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct FvgConfig {
    #[serde(default = "default_fvg_windows")]
    pub windows: Vec<String>,
    #[serde(default = "default_fvg_fill_from_db")]
    pub fill_from_db: bool,
    #[serde(default = "default_fvg_db_bars_4h")]
    pub db_bars_4h: usize,
    #[serde(default = "default_fvg_db_bars_1d")]
    pub db_bars_1d: usize,
    #[serde(default = "default_fvg_epsilon_gap_ticks")]
    pub epsilon_gap_ticks: i64,
    #[serde(default = "default_fvg_atr_lookback")]
    pub atr_lookback: usize,
    #[serde(default = "default_fvg_min_body_ratio")]
    pub min_body_ratio: f64,
    #[serde(default = "default_fvg_min_impulse_atr_ratio")]
    pub min_impulse_atr_ratio: f64,
    #[serde(default = "default_fvg_min_gap_atr_ratio")]
    pub min_gap_atr_ratio: f64,
    #[serde(default = "default_fvg_max_gap_atr_ratio")]
    pub max_gap_atr_ratio: f64,
    #[serde(default = "default_fvg_mitigated_fill_threshold")]
    pub mitigated_fill_threshold: f64,
    #[serde(default = "default_fvg_invalid_close_bars")]
    pub invalid_close_bars: usize,
}

impl Default for FvgConfig {
    fn default() -> Self {
        Self {
            windows: default_fvg_windows(),
            fill_from_db: default_fvg_fill_from_db(),
            db_bars_4h: default_fvg_db_bars_4h(),
            db_bars_1d: default_fvg_db_bars_1d(),
            epsilon_gap_ticks: default_fvg_epsilon_gap_ticks(),
            atr_lookback: default_fvg_atr_lookback(),
            min_body_ratio: default_fvg_min_body_ratio(),
            min_impulse_atr_ratio: default_fvg_min_impulse_atr_ratio(),
            min_gap_atr_ratio: default_fvg_min_gap_atr_ratio(),
            max_gap_atr_ratio: default_fvg_max_gap_atr_ratio(),
            mitigated_fill_threshold: default_fvg_mitigated_fill_threshold(),
            invalid_close_bars: default_fvg_invalid_close_bars(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct DivergenceConfig {
    #[serde(default = "default_div_sig_test_mode")]
    pub sig_test_mode: String,
    #[serde(default = "default_div_bootstrap_b")]
    pub bootstrap_b: usize,
    #[serde(default = "default_div_bootstrap_block_len")]
    pub bootstrap_block_len: usize,
    #[serde(default = "default_div_p_value_threshold")]
    pub p_value_threshold: f64,
}

impl Default for DivergenceConfig {
    fn default() -> Self {
        Self {
            sig_test_mode: default_div_sig_test_mode(),
            bootstrap_b: default_div_bootstrap_b(),
            bootstrap_block_len: default_div_bootstrap_block_len(),
            p_value_threshold: default_div_p_value_threshold(),
        }
    }
}

fn default_symbol() -> String {
    String::new()
}

fn default_export_interval_secs() -> u64 {
    60
}

fn default_export_dir() -> String {
    "exports/indicator_engine".to_string()
}

fn default_enable_file_export() -> bool {
    true
}

fn default_whale_threshold_usdt() -> f64 {
    300_000.0
}

fn default_kline_history_bars_1m() -> usize {
    1024
}

fn default_kline_history_bars_15m() -> usize {
    120
}

fn default_kline_history_bars_4h() -> usize {
    120
}

fn default_kline_history_bars_1d() -> usize {
    120
}

fn default_kline_history_fill_1d_from_db() -> bool {
    true
}

fn default_tpo_rows_nb() -> usize {
    64
}

fn default_tpo_value_area_pct() -> f64 {
    0.70
}

fn default_tpo_session_windows() -> Vec<String> {
    vec!["4h".to_string(), "1d".to_string()]
}

fn default_tpo_ib_minutes() -> i64 {
    60
}

fn default_tpo_dev_output_windows() -> Vec<String> {
    vec!["15m".to_string(), "1h".to_string()]
}

fn default_rvwap_windows() -> Vec<String> {
    vec!["15m".to_string(), "4h".to_string(), "1d".to_string()]
}

fn default_rvwap_output_windows() -> Vec<String> {
    vec!["15m".to_string(), "1h".to_string()]
}

fn default_rvwap_min_samples() -> usize {
    5
}

fn default_high_volume_pulse_z_windows() -> Vec<String> {
    vec!["1h".to_string(), "4h".to_string(), "1d".to_string()]
}

fn default_high_volume_pulse_summary_windows() -> Vec<String> {
    vec!["15m".to_string(), "1h".to_string()]
}

fn default_high_volume_pulse_min_samples() -> usize {
    5
}

fn default_ema_base_periods() -> Vec<usize> {
    vec![13, 21, 34]
}

fn default_ema_htf_periods() -> Vec<usize> {
    vec![100, 200]
}

fn default_ema_htf_windows() -> Vec<String> {
    vec!["4h".to_string(), "1d".to_string()]
}

fn default_ema_output_windows() -> Vec<String> {
    vec!["15m".to_string(), "1h".to_string()]
}

fn default_ema_fill_from_db() -> bool {
    true
}

fn default_ema_db_bars_4h() -> usize {
    256
}

fn default_ema_db_bars_1d() -> usize {
    256
}

fn default_fvg_windows() -> Vec<String> {
    vec![
        "15m".to_string(),
        "4h".to_string(),
        "1d".to_string(),
        "3d".to_string(),
    ]
}

fn default_fvg_fill_from_db() -> bool {
    true
}

fn default_fvg_db_bars_4h() -> usize {
    256
}

fn default_fvg_db_bars_1d() -> usize {
    256
}

fn default_fvg_epsilon_gap_ticks() -> i64 {
    2
}

fn default_fvg_atr_lookback() -> usize {
    14
}

fn default_fvg_min_body_ratio() -> f64 {
    0.60
}

fn default_fvg_min_impulse_atr_ratio() -> f64 {
    1.30
}

fn default_fvg_min_gap_atr_ratio() -> f64 {
    0.15
}

fn default_fvg_max_gap_atr_ratio() -> f64 {
    1.20
}

fn default_fvg_mitigated_fill_threshold() -> f64 {
    0.80
}

fn default_fvg_invalid_close_bars() -> usize {
    1
}

fn default_window_codes() -> Vec<String> {
    vec![
        "1m".to_string(),
        "15m".to_string(),
        "1h".to_string(),
        "4h".to_string(),
        "1d".to_string(),
        "3d".to_string(),
    ]
}

fn default_div_sig_test_mode() -> String {
    "threshold".to_string()
}

fn default_div_bootstrap_b() -> usize {
    200
}

fn default_div_bootstrap_block_len() -> usize {
    5
}

fn default_div_p_value_threshold() -> f64 {
    0.05
}

fn default_consume_mode() -> String {
    "live".to_string()
}

fn default_live_drop_stale_event_secs() -> i64 {
    300
}

fn default_live_drop_stale_enabled() -> bool {
    false
}

fn default_watermark_lateness_secs() -> i64 {
    5
}

fn default_live_purge_on_start() -> bool {
    false
}

fn default_startup_max_catchup_minutes() -> i64 {
    0
}

fn default_startup_backfill_batch_size() -> i64 {
    1_000
}

fn default_snapshot_max_age_hours() -> u64 {
    24
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_sql_log_enabled() -> bool {
    false
}

fn default_slow_sql_threshold_ms() -> u64 {
    3_000
}

#[derive(Clone)]
pub struct AppContext {
    pub config: Arc<RootConfig>,
    pub db_pool: PgPool,
    pub mq_connection: Arc<Connection>,
    pub mq_publish_channel: Channel,
    pub indicator_queues: Vec<String>,
    pub producer_instance_id: String,
}

pub async fn bootstrap() -> Result<AppContext> {
    let config = Arc::new(load_config(CONFIG_PATH).context("load config/config.yaml")?);

    info!(
        app_name = %config.app.name,
        env = %config.app.env,
        timezone = %config.app.timezone,
        symbol = %config.indicator.symbol,
        export_interval_secs = config.indicator.export_interval_secs,
        export_dir = %config.indicator.export_dir,
        whale_threshold_usdt = config.indicator.whale_threshold_usdt,
        kline_history_bars_1m = config.indicator.kline_history.bars_1m,
        kline_history_bars_15m = config.indicator.kline_history.bars_15m,
        kline_history_bars_4h = config.indicator.kline_history.bars_4h,
        kline_history_bars_1d = config.indicator.kline_history.bars_1d,
        kline_history_fill_1d_from_db = config.indicator.kline_history.fill_1d_from_db,
        tpo_rows_nb = config.indicator.tpo_market_profile.rows_nb,
        tpo_value_area_pct = config.indicator.tpo_market_profile.value_area_pct,
        tpo_session_windows = ?config.indicator.tpo_market_profile.session_windows,
        tpo_ib_minutes = config.indicator.tpo_market_profile.ib_minutes,
        tpo_dev_output_windows = ?config.indicator.tpo_market_profile.dev_output_windows,
        rvwap_windows = ?config.indicator.rvwap_sigma_bands.windows,
        rvwap_output_windows = ?config.indicator.rvwap_sigma_bands.output_windows,
        rvwap_min_samples = config.indicator.rvwap_sigma_bands.min_samples,
        high_volume_pulse_z_windows = ?config.indicator.high_volume_pulse.z_windows,
        high_volume_pulse_summary_windows = ?config.indicator.high_volume_pulse.summary_windows,
        high_volume_pulse_min_samples = config.indicator.high_volume_pulse.min_samples,
        ema_base_periods = ?config.indicator.ema_trend_regime.base_periods,
        ema_htf_periods = ?config.indicator.ema_trend_regime.htf_periods,
        ema_htf_windows = ?config.indicator.ema_trend_regime.htf_windows,
        ema_output_windows = ?config.indicator.ema_trend_regime.output_windows,
        ema_fill_from_db = config.indicator.ema_trend_regime.fill_from_db,
        ema_db_bars_4h = config.indicator.ema_trend_regime.db_bars_4h,
        ema_db_bars_1d = config.indicator.ema_trend_regime.db_bars_1d,
        fvg_windows = ?config.indicator.fvg.windows,
        fvg_fill_from_db = config.indicator.fvg.fill_from_db,
        fvg_db_bars_4h = config.indicator.fvg.db_bars_4h,
        fvg_db_bars_1d = config.indicator.fvg.db_bars_1d,
        fvg_epsilon_gap_ticks = config.indicator.fvg.epsilon_gap_ticks,
        fvg_atr_lookback = config.indicator.fvg.atr_lookback,
        fvg_min_body_ratio = config.indicator.fvg.min_body_ratio,
        fvg_min_impulse_atr_ratio = config.indicator.fvg.min_impulse_atr_ratio,
        fvg_min_gap_atr_ratio = config.indicator.fvg.min_gap_atr_ratio,
        fvg_max_gap_atr_ratio = config.indicator.fvg.max_gap_atr_ratio,
        fvg_mitigated_fill_threshold = config.indicator.fvg.mitigated_fill_threshold,
        fvg_invalid_close_bars = config.indicator.fvg.invalid_close_bars,
        div_sig_test_mode = %config.indicator.divergence.sig_test_mode,
        div_bootstrap_b = config.indicator.divergence.bootstrap_b,
        div_bootstrap_block_len = config.indicator.divergence.bootstrap_block_len,
        div_p_value_threshold = config.indicator.divergence.p_value_threshold,
        window_codes = ?config.indicator.window_codes,
        live_drop_stale_event_secs = config.indicator.live_drop_stale_event_secs,
        live_drop_stale_enabled = config.indicator.live_drop_stale_enabled,
        watermark_lateness_secs = config.indicator.watermark_lateness_secs,
        live_purge_on_start = config.indicator.live_purge_on_start,
        startup_max_catchup_minutes = config.indicator.startup_max_catchup_minutes,
        startup_backfill_batch_size = config.indicator.startup_backfill_batch_size,
        logging_level = %config.logging.level,
        sql_log = config.logging.sql_log,
        slow_sql_threshold_ms = config.logging.slow_sql_threshold_ms,
        "loaded indicator engine config"
    );

    let db_pool = build_db_pool(&config).await?;

    let mq_connection = Connection::connect(&config.mq.amqp_uri(), ConnectionProperties::default())
        .await
        .context("connect rabbitmq")?;

    let topology_channel = mq_connection
        .create_channel()
        .await
        .context("create topology channel")?;
    declare_topology(&topology_channel, &config.mq).await?;

    let mq_publish_channel = mq_connection
        .create_channel()
        .await
        .context("create indicator publish channel")?;
    mq_publish_channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .context("enable publisher confirms for indicator publish channel")?;

    let indicator_queue_cfgs = collect_indicator_queue_configs(&config)?;

    let producer_instance_id = format!("indicator-engine-{}", Uuid::new_v4());
    let indicator_queues = indicator_queue_cfgs
        .iter()
        .map(|(_, q)| q.name.clone())
        .collect::<Vec<_>>();
    if config.indicator.live_purge_on_start {
        purge_indicator_queues(&topology_channel, &indicator_queues).await?;
    }

    let md_consume_exchange = config.mq.exchanges.md_live.name.as_str();

    info!(
        queue_count = indicator_queues.len(),
        startup_mode = "live_plus_auto_backfill",
        grouped_queue_mode = true,
        md_consume_exchange = %md_consume_exchange,
        ind_exchange = %config.mq.exchanges.ind.name,
        "indicator bootstrap completed"
    );

    Ok(AppContext {
        config,
        db_pool,
        mq_connection: Arc::new(mq_connection),
        mq_publish_channel,
        indicator_queues,
        producer_instance_id,
    })
}

fn collect_indicator_queue_configs(config: &RootConfig) -> Result<Vec<(String, MqQueueConfig)>> {
    let group_keys = [GROUP_QUEUE_OB_HEAVY, GROUP_QUEUE_FLOW, GROUP_QUEUE_DERIV];
    let mut items: Vec<(String, MqQueueConfig)> = Vec::with_capacity(group_keys.len());
    for key in group_keys {
        let q = config.mq.queues.get(key).ok_or_else(|| {
            anyhow!(
                "missing required group-only queue config: mq.queues.{}",
                key
            )
        })?;
        items.push((key.to_string(), q.clone()));
    }

    items.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(items)
}

pub async fn build_db_pool(config: &RootConfig) -> Result<PgPool> {
    let connect_timeout = config.database.connect_timeout_secs.unwrap_or(10);
    let uri = config.database.postgres_uri();
    let pool_cfg = config.database.pool.as_ref();
    let slow_sql_threshold_ms = config.logging.slow_sql_threshold_ms.max(1);

    let max_connections = pool_cfg.and_then(|p| p.max_connections).unwrap_or(20);
    let min_connections = pool_cfg.and_then(|p| p.min_connections).unwrap_or(2);
    let acquire_timeout_secs = pool_cfg
        .and_then(|p| p.acquire_timeout_secs)
        .unwrap_or(connect_timeout);
    let idle_timeout_secs = pool_cfg.and_then(|p| p.idle_timeout_secs);
    let max_lifetime_secs = pool_cfg.and_then(|p| p.max_lifetime_secs);
    let test_before_acquire = pool_cfg.and_then(|p| p.test_before_acquire).unwrap_or(true);

    let mut pool_options = PgPoolOptions::new()
        .max_connections(max_connections)
        .min_connections(min_connections)
        .acquire_timeout(Duration::from_secs(acquire_timeout_secs))
        .test_before_acquire(test_before_acquire);

    if let Some(v) = idle_timeout_secs {
        pool_options = pool_options.idle_timeout(Some(Duration::from_secs(v)));
    }
    if let Some(v) = max_lifetime_secs {
        pool_options = pool_options.max_lifetime(Some(Duration::from_secs(v)));
    }

    let mut connect_options: PgConnectOptions = uri.parse().context("parse postgres uri")?;
    connect_options = connect_options.log_slow_statements(
        LevelFilter::Warn,
        Duration::from_millis(slow_sql_threshold_ms),
    );
    connect_options = if config.logging.sql_log {
        connect_options.log_statements(LevelFilter::Debug)
    } else {
        connect_options.log_statements(LevelFilter::Off)
    };

    let pool = pool_options
        .connect_with(connect_options)
        .await
        .context("connect postgres")?;

    sqlx::query("SELECT 1")
        .execute(&pool)
        .await
        .context("postgres health query")?;

    info!(
        slow_sql_threshold_ms = slow_sql_threshold_ms,
        sql_log = config.logging.sql_log,
        "indicator db sql logging policy applied"
    );

    Ok(pool)
}

async fn declare_topology(channel: &Channel, mq: &MqConfig) -> Result<()> {
    for exchange in [
        &mq.exchanges.md_live,
        &mq.exchanges.md_replay,
        &mq.exchanges.ind,
        &mq.exchanges.dlx,
    ] {
        channel
            .exchange_declare(
                &exchange.name,
                exchange_kind(&exchange.kind),
                ExchangeDeclareOptions {
                    durable: exchange.durable,
                    auto_delete: false,
                    internal: false,
                    nowait: false,
                    passive: false,
                },
                FieldTable::default(),
            )
            .await
            .with_context(|| format!("declare exchange {}", exchange.name))?;
    }

    for (queue_key, queue_cfg) in &mq.queues {
        if is_legacy_indicator_queue_key(queue_key) {
            continue;
        }
        channel
            .queue_declare(
                &queue_cfg.name,
                QueueDeclareOptions {
                    durable: true,
                    exclusive: false,
                    auto_delete: false,
                    nowait: false,
                    passive: false,
                },
                build_queue_args(
                    queue_cfg.message_ttl_ms,
                    queue_cfg.max_length,
                    queue_cfg.max_length_bytes,
                ),
            )
            .await
            .with_context(|| format!("declare queue {}", queue_cfg.name))?;

        for bind in &queue_cfg.bind {
            channel
                .queue_bind(
                    &queue_cfg.name,
                    &bind.exchange,
                    &bind.routing_key,
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await
                .with_context(|| {
                    format!(
                        "bind queue {} to {} with {}",
                        queue_cfg.name, bind.exchange, bind.routing_key
                    )
                })?;
        }
    }

    Ok(())
}

#[allow(dead_code)]
async fn create_replay_consume_queues(
    channel: &Channel,
    mq: &MqConfig,
    producer_instance_id: &str,
    indicator_queue_cfgs: &[(String, MqQueueConfig)],
) -> Result<Vec<String>> {
    let mut created = Vec::with_capacity(indicator_queue_cfgs.len());
    let instance_suffix = producer_instance_id
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>();

    for (idx, (key, queue_cfg)) in indicator_queue_cfgs.iter().enumerate() {
        let queue_name = format!("q.indicator.replay.{}.{}.{}", key, instance_suffix, idx);
        channel
            .queue_declare(
                &queue_name,
                QueueDeclareOptions {
                    durable: false,
                    exclusive: false,
                    auto_delete: true,
                    nowait: false,
                    passive: false,
                },
                FieldTable::default(),
            )
            .await
            .with_context(|| format!("declare replay queue {}", queue_name))?;

        for bind in &queue_cfg.bind {
            channel
                .queue_bind(
                    &queue_name,
                    &mq.exchanges.md_replay.name,
                    &bind.routing_key,
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await
                .with_context(|| {
                    format!(
                        "bind replay queue {} to {} with {}",
                        queue_name, mq.exchanges.md_replay.name, bind.routing_key
                    )
                })?;
        }

        info!(
            queue = %queue_name,
            source_queue = %queue_cfg.name,
            "created indicator replay consume queue"
        );
        created.push(queue_name);
    }

    Ok(created)
}

async fn purge_indicator_queues(channel: &Channel, queues: &[String]) -> Result<()> {
    for queue in queues {
        let result = channel
            .queue_purge(queue, QueuePurgeOptions::default())
            .await
            .with_context(|| format!("purge queue {}", queue))?;
        info!(
            queue = %queue,
            purged = result,
            "purged indicator queue for live consume mode"
        );
    }
    Ok(())
}

fn exchange_kind(kind: &str) -> ExchangeKind {
    match kind {
        "fanout" => ExchangeKind::Fanout,
        "direct" => ExchangeKind::Direct,
        "topic" => ExchangeKind::Topic,
        _ => ExchangeKind::Topic,
    }
}

fn is_legacy_indicator_queue_key(queue_key: &str) -> bool {
    queue_key.starts_with("indicator_") && !queue_key.starts_with("indicator_group_")
}

/// Build AMQP queue arguments for optional TTL and max-length.
///
/// When `message_ttl_ms` is set, messages older than that are expired by the
/// broker and dropped rather than accumulating and triggering flow control back
/// to publishers. Any length cap uses `x-overflow=drop-head` so the queue
/// evicts the oldest messages (instead of blocking publishers) if the cap is hit.
fn build_queue_args(
    message_ttl_ms: Option<u32>,
    max_length: Option<u32>,
    max_length_bytes: Option<u64>,
) -> FieldTable {
    let mut args = FieldTable::default();
    if let Some(ttl) = message_ttl_ms {
        args.insert(ShortString::from("x-message-ttl"), AMQPValue::LongUInt(ttl));
    }
    if let Some(len) = max_length {
        args.insert(ShortString::from("x-max-length"), AMQPValue::LongUInt(len));
    }
    if let Some(bytes) = max_length_bytes {
        args.insert(
            ShortString::from("x-max-length-bytes"),
            AMQPValue::LongLongInt(bytes as i64),
        );
    }
    if max_length.is_some() || max_length_bytes.is_some() {
        args.insert(
            ShortString::from("x-overflow"),
            AMQPValue::LongString(LongString::from("drop-head")),
        );
    }
    args
}

pub fn load_config(path: &str) -> Result<RootConfig> {
    let mut doc = load_config_document(path)?;
    let symbol = extract_global_symbol(&doc)
        .ok_or_else(|| anyhow!("config.instrument.symbol or indicator.symbol is required"))?;
    ensure_yaml_string_path(&mut doc, &["indicator", "symbol"], &symbol);
    let symbol_lower = symbol.to_ascii_lowercase();
    apply_symbol_placeholders(&mut doc, &symbol, &symbol_lower);
    let cfg: RootConfig = serde_yaml::from_value(doc)?;
    validate_config(&cfg)?;
    Ok(cfg)
}

fn load_config_document(path: &str) -> Result<serde_yaml::Value> {
    let text = std::fs::read_to_string(path)?;
    Ok(serde_yaml::from_str(&text)?)
}

fn extract_global_symbol(doc: &serde_yaml::Value) -> Option<String> {
    for path in [
        &["instrument", "symbol"][..],
        &["indicator", "symbol"][..],
        &["llm", "symbol"][..],
        &["replayer", "symbol"][..],
        &["market_data", "symbol"][..],
    ] {
        let Some(value) = lookup_yaml_string(doc, path) else {
            continue;
        };
        let trimmed = value.trim();
        if !trimmed.is_empty() && !trimmed.contains("{symbol") {
            return Some(trimmed.to_ascii_uppercase());
        }
    }
    None
}

fn lookup_yaml_string(doc: &serde_yaml::Value, path: &[&str]) -> Option<String> {
    let mut node = doc;
    for segment in path {
        let map = node.as_mapping()?;
        node = map.get(serde_yaml::Value::String((*segment).to_string()))?;
    }
    node.as_str().map(ToOwned::to_owned)
}

fn ensure_yaml_string_path(doc: &mut serde_yaml::Value, path: &[&str], value: &str) {
    if path.is_empty() {
        return;
    }

    let mut node = doc;
    for segment in &path[..path.len() - 1] {
        let Some(map) = node.as_mapping_mut() else {
            return;
        };
        node = map
            .entry(serde_yaml::Value::String((*segment).to_string()))
            .or_insert_with(|| serde_yaml::Value::Mapping(Default::default()));
    }

    if let Some(map) = node.as_mapping_mut() {
        map.entry(serde_yaml::Value::String(path[path.len() - 1].to_string()))
            .or_insert_with(|| serde_yaml::Value::String(value.to_string()));
    }
}

fn apply_symbol_placeholders(node: &mut serde_yaml::Value, symbol: &str, symbol_lower: &str) {
    match node {
        serde_yaml::Value::String(text) => {
            *text = text
                .replace("{symbol_lower}", symbol_lower)
                .replace("{symbol}", symbol);
        }
        serde_yaml::Value::Sequence(items) => {
            for item in items {
                apply_symbol_placeholders(item, symbol, symbol_lower);
            }
        }
        serde_yaml::Value::Mapping(map) => {
            for value in map.values_mut() {
                apply_symbol_placeholders(value, symbol, symbol_lower);
            }
        }
        _ => {}
    }
}

fn validate_config(cfg: &RootConfig) -> Result<()> {
    if cfg.mq.exchanges.md_live.name.trim().is_empty() {
        return Err(anyhow!("mq.exchanges.md_live.name is empty"));
    }
    if cfg.mq.exchanges.md_replay.name.trim().is_empty() {
        return Err(anyhow!("mq.exchanges.md_replay.name is empty"));
    }
    if cfg.mq.exchanges.ind.name.trim().is_empty() {
        return Err(anyhow!("mq.exchanges.ind.name is empty"));
    }
    if cfg.indicator.symbol.trim().is_empty() {
        return Err(anyhow!("indicator.symbol is empty"));
    }
    if cfg.indicator.export_interval_secs == 0 {
        return Err(anyhow!("indicator.export_interval_secs must be > 0"));
    }
    if cfg.indicator.whale_threshold_usdt <= 0.0 {
        return Err(anyhow!("indicator.whale_threshold_usdt must be > 0"));
    }
    if cfg.indicator.kline_history.bars_1m == 0 {
        return Err(anyhow!("indicator.kline_history.bars_1m must be > 0"));
    }
    if cfg.indicator.kline_history.bars_15m == 0 {
        return Err(anyhow!("indicator.kline_history.bars_15m must be > 0"));
    }
    if cfg.indicator.kline_history.bars_4h == 0 {
        return Err(anyhow!("indicator.kline_history.bars_4h must be > 0"));
    }
    if cfg.indicator.kline_history.bars_1d == 0 {
        return Err(anyhow!("indicator.kline_history.bars_1d must be > 0"));
    }
    if cfg.indicator.tpo_market_profile.rows_nb == 0 {
        return Err(anyhow!("indicator.tpo_market_profile.rows_nb must be > 0"));
    }
    if !(0.0..=1.0).contains(&cfg.indicator.tpo_market_profile.value_area_pct)
        || cfg.indicator.tpo_market_profile.value_area_pct <= 0.0
    {
        return Err(anyhow!(
            "indicator.tpo_market_profile.value_area_pct must be in (0,1]"
        ));
    }
    if cfg.indicator.tpo_market_profile.ib_minutes <= 0 {
        return Err(anyhow!(
            "indicator.tpo_market_profile.ib_minutes must be > 0"
        ));
    }
    if cfg.indicator.tpo_market_profile.session_windows.is_empty() {
        return Err(anyhow!(
            "indicator.tpo_market_profile.session_windows cannot be empty"
        ));
    }
    for code in &cfg.indicator.tpo_market_profile.session_windows {
        match code.as_str() {
            "4h" | "1d" => {}
            other => {
                return Err(anyhow!(
                    "unsupported indicator.tpo_market_profile.session_windows value: {}",
                    other
                ));
            }
        }
    }
    if cfg
        .indicator
        .tpo_market_profile
        .dev_output_windows
        .is_empty()
    {
        return Err(anyhow!(
            "indicator.tpo_market_profile.dev_output_windows cannot be empty"
        ));
    }
    for code in &cfg.indicator.tpo_market_profile.dev_output_windows {
        match code.as_str() {
            "15m" | "1h" => {}
            other => {
                return Err(anyhow!(
                    "unsupported indicator.tpo_market_profile.dev_output_windows value: {}",
                    other
                ));
            }
        }
    }
    if cfg.indicator.rvwap_sigma_bands.windows.is_empty() {
        return Err(anyhow!(
            "indicator.rvwap_sigma_bands.windows cannot be empty"
        ));
    }
    for code in &cfg.indicator.rvwap_sigma_bands.windows {
        match code.as_str() {
            "15m" | "4h" | "1d" => {}
            other => {
                return Err(anyhow!(
                    "unsupported indicator.rvwap_sigma_bands.windows value: {}",
                    other
                ));
            }
        }
    }
    if cfg.indicator.rvwap_sigma_bands.output_windows.is_empty() {
        return Err(anyhow!(
            "indicator.rvwap_sigma_bands.output_windows cannot be empty"
        ));
    }
    for code in &cfg.indicator.rvwap_sigma_bands.output_windows {
        match code.as_str() {
            "15m" | "1h" => {}
            other => {
                return Err(anyhow!(
                    "unsupported indicator.rvwap_sigma_bands.output_windows value: {}",
                    other
                ));
            }
        }
    }
    if cfg.indicator.rvwap_sigma_bands.min_samples == 0 {
        return Err(anyhow!(
            "indicator.rvwap_sigma_bands.min_samples must be > 0"
        ));
    }
    if cfg.indicator.high_volume_pulse.z_windows.is_empty() {
        return Err(anyhow!(
            "indicator.high_volume_pulse.z_windows cannot be empty"
        ));
    }
    for code in &cfg.indicator.high_volume_pulse.z_windows {
        match code.as_str() {
            "1h" | "4h" | "1d" => {}
            other => {
                return Err(anyhow!(
                    "unsupported indicator.high_volume_pulse.z_windows value: {}",
                    other
                ));
            }
        }
    }
    if cfg.indicator.high_volume_pulse.summary_windows.is_empty() {
        return Err(anyhow!(
            "indicator.high_volume_pulse.summary_windows cannot be empty"
        ));
    }
    for code in &cfg.indicator.high_volume_pulse.summary_windows {
        match code.as_str() {
            "15m" | "1h" => {}
            other => {
                return Err(anyhow!(
                    "unsupported indicator.high_volume_pulse.summary_windows value: {}",
                    other
                ));
            }
        }
    }
    if cfg.indicator.high_volume_pulse.min_samples == 0 {
        return Err(anyhow!(
            "indicator.high_volume_pulse.min_samples must be > 0"
        ));
    }
    if cfg.indicator.ema_trend_regime.base_periods.is_empty() {
        return Err(anyhow!(
            "indicator.ema_trend_regime.base_periods cannot be empty"
        ));
    }
    if cfg
        .indicator
        .ema_trend_regime
        .base_periods
        .iter()
        .any(|v| *v == 0)
    {
        return Err(anyhow!(
            "indicator.ema_trend_regime.base_periods must be > 0"
        ));
    }
    if cfg.indicator.ema_trend_regime.htf_periods.is_empty() {
        return Err(anyhow!(
            "indicator.ema_trend_regime.htf_periods cannot be empty"
        ));
    }
    if cfg
        .indicator
        .ema_trend_regime
        .htf_periods
        .iter()
        .any(|v| *v == 0)
    {
        return Err(anyhow!(
            "indicator.ema_trend_regime.htf_periods must be > 0"
        ));
    }
    if cfg.indicator.ema_trend_regime.htf_windows.is_empty() {
        return Err(anyhow!(
            "indicator.ema_trend_regime.htf_windows cannot be empty"
        ));
    }
    for code in &cfg.indicator.ema_trend_regime.htf_windows {
        match code.as_str() {
            "4h" | "1d" => {}
            other => {
                return Err(anyhow!(
                    "unsupported indicator.ema_trend_regime.htf_windows value: {}",
                    other
                ));
            }
        }
    }
    if cfg.indicator.ema_trend_regime.output_windows.is_empty() {
        return Err(anyhow!(
            "indicator.ema_trend_regime.output_windows cannot be empty"
        ));
    }
    for code in &cfg.indicator.ema_trend_regime.output_windows {
        match code.as_str() {
            "15m" | "1h" => {}
            other => {
                return Err(anyhow!(
                    "unsupported indicator.ema_trend_regime.output_windows value: {}",
                    other
                ));
            }
        }
    }
    if cfg.indicator.ema_trend_regime.db_bars_4h == 0 {
        return Err(anyhow!("indicator.ema_trend_regime.db_bars_4h must be > 0"));
    }
    if cfg.indicator.ema_trend_regime.db_bars_1d == 0 {
        return Err(anyhow!("indicator.ema_trend_regime.db_bars_1d must be > 0"));
    }
    if cfg.indicator.fvg.windows.is_empty() {
        return Err(anyhow!("indicator.fvg.windows cannot be empty"));
    }
    for code in &cfg.indicator.fvg.windows {
        match code.as_str() {
            "15m" | "4h" | "1d" | "3d" => {}
            other => {
                return Err(anyhow!(
                    "unsupported indicator.fvg.windows value: {}",
                    other
                ));
            }
        }
    }
    if cfg.indicator.fvg.db_bars_4h == 0 {
        return Err(anyhow!("indicator.fvg.db_bars_4h must be > 0"));
    }
    if cfg.indicator.fvg.db_bars_1d == 0 {
        return Err(anyhow!("indicator.fvg.db_bars_1d must be > 0"));
    }
    if cfg.indicator.fvg.epsilon_gap_ticks < 0 {
        return Err(anyhow!("indicator.fvg.epsilon_gap_ticks must be >= 0"));
    }
    if cfg.indicator.fvg.atr_lookback == 0 {
        return Err(anyhow!("indicator.fvg.atr_lookback must be > 0"));
    }
    if !(0.0..=1.0).contains(&cfg.indicator.fvg.min_body_ratio) {
        return Err(anyhow!("indicator.fvg.min_body_ratio must be in [0,1]"));
    }
    if cfg.indicator.fvg.min_impulse_atr_ratio <= 0.0 {
        return Err(anyhow!("indicator.fvg.min_impulse_atr_ratio must be > 0"));
    }
    if cfg.indicator.fvg.min_gap_atr_ratio < 0.0 {
        return Err(anyhow!("indicator.fvg.min_gap_atr_ratio must be >= 0"));
    }
    if cfg.indicator.fvg.max_gap_atr_ratio < cfg.indicator.fvg.min_gap_atr_ratio {
        return Err(anyhow!(
            "indicator.fvg.max_gap_atr_ratio must be >= indicator.fvg.min_gap_atr_ratio"
        ));
    }
    if !(0.0..=1.0).contains(&cfg.indicator.fvg.mitigated_fill_threshold) {
        return Err(anyhow!(
            "indicator.fvg.mitigated_fill_threshold must be in [0,1]"
        ));
    }
    if cfg.indicator.fvg.invalid_close_bars == 0 {
        return Err(anyhow!("indicator.fvg.invalid_close_bars must be > 0"));
    }
    let mode = cfg.indicator.divergence.sig_test_mode.as_str();
    if mode != "threshold" && mode != "block_bootstrap" {
        return Err(anyhow!(
            "indicator.divergence.sig_test_mode must be threshold or block_bootstrap"
        ));
    }
    if cfg.indicator.divergence.bootstrap_b == 0 {
        return Err(anyhow!("indicator.divergence.bootstrap_b must be > 0"));
    }
    if cfg.indicator.divergence.bootstrap_block_len == 0 {
        return Err(anyhow!(
            "indicator.divergence.bootstrap_block_len must be > 0"
        ));
    }
    if !(0.0..=1.0).contains(&cfg.indicator.divergence.p_value_threshold) {
        return Err(anyhow!(
            "indicator.divergence.p_value_threshold must be in [0,1]"
        ));
    }
    if cfg.indicator.window_codes.is_empty() {
        return Err(anyhow!("indicator.window_codes cannot be empty"));
    }
    for code in &cfg.indicator.window_codes {
        match code.as_str() {
            "1m" | "15m" | "1h" | "4h" | "1d" | "3d" => {}
            other => {
                return Err(anyhow!(
                    "unsupported indicator.window_codes value: {}",
                    other
                ));
            }
        }
    }
    if cfg.indicator.live_drop_stale_event_secs <= 0 {
        return Err(anyhow!("indicator.live_drop_stale_event_secs must be > 0"));
    }
    if cfg.indicator.watermark_lateness_secs < 0 {
        return Err(anyhow!("indicator.watermark_lateness_secs must be >= 0"));
    }
    if cfg.indicator.startup_max_catchup_minutes < 0 {
        return Err(anyhow!(
            "indicator.startup_max_catchup_minutes must be >= 0"
        ));
    }
    Ok(())
}

fn resolve_secret(raw: &str) -> String {
    std::env::var(raw).unwrap_or_else(|_| raw.to_string())
}
