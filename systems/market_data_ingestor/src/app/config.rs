use anyhow::{anyhow, Result};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize)]
pub struct RootConfig {
    pub app: AppSection,
    pub database: DatabaseConfig,
    pub api: ApiConfig,
    #[serde(default)]
    pub network: NetworkConfig,
    #[serde(default)]
    pub parquet: ParquetConfig,
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
    #[serde(default)]
    pub md: Option<DatabaseTargetConfig>,
    #[serde(default)]
    pub ops: Option<DatabaseTargetConfig>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct DbPoolConfig {
    pub enabled: Option<bool>,
    pub min_connections: Option<u32>,
    pub max_connections: Option<u32>,
    pub acquire_timeout_secs: Option<u64>,
    pub idle_timeout_secs: Option<u64>,
    pub max_lifetime_secs: Option<u64>,
    pub test_before_acquire: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct DatabaseTargetConfig {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub database: Option<String>,
    pub user: Option<String>,
    pub password_env: Option<String>,
    pub connect_timeout_secs: Option<u64>,
    pub application_name: Option<String>,
    pub sslmode: Option<String>,
    pub options: Option<String>,
    pub pool: Option<DbPoolConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedDatabaseConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password_raw: String,
    pub connect_timeout_secs: Option<u64>,
    pub application_name: Option<String>,
    pub sslmode: Option<String>,
    pub options: Option<String>,
    pub pool: Option<DbPoolConfig>,
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

    pub fn resolve_md(&self) -> ResolvedDatabaseConfig {
        self.resolve_target(self.md.as_ref())
    }

    pub fn resolve_ops(&self) -> ResolvedDatabaseConfig {
        self.resolve_target(self.ops.as_ref())
    }

    fn resolve_target(&self, target: Option<&DatabaseTargetConfig>) -> ResolvedDatabaseConfig {
        let host = target
            .and_then(|t| t.host.as_ref())
            .cloned()
            .unwrap_or_else(|| self.host.clone());
        let port = target.and_then(|t| t.port).unwrap_or(self.port);
        let database = target
            .and_then(|t| t.database.as_ref())
            .cloned()
            .unwrap_or_else(|| self.database.clone());
        let user = target
            .and_then(|t| t.user.as_ref())
            .cloned()
            .unwrap_or_else(|| self.user.clone());
        let password_raw = target
            .and_then(|t| t.password_env.as_deref())
            .map(resolve_secret)
            .unwrap_or_else(|| self.resolved_password());
        let connect_timeout_secs = target
            .and_then(|t| t.connect_timeout_secs)
            .or(self.connect_timeout_secs);
        let application_name = target
            .and_then(|t| t.application_name.as_ref())
            .cloned()
            .or_else(|| self.application_name.clone());
        let sslmode = target
            .and_then(|t| t.sslmode.as_ref())
            .cloned()
            .or_else(|| self.sslmode.clone());
        let options = target
            .and_then(|t| t.options.as_ref())
            .cloned()
            .or_else(|| self.options.clone());
        let pool = target
            .and_then(|t| t.pool.as_ref())
            .cloned()
            .or_else(|| self.pool.clone());

        ResolvedDatabaseConfig {
            host,
            port,
            database,
            user,
            password_raw,
            connect_timeout_secs,
            application_name,
            sslmode,
            options,
            pool,
        }
    }
}

impl ResolvedDatabaseConfig {
    pub fn postgres_uri(&self) -> String {
        let user = urlencoding::encode(&self.user);
        let password = urlencoding::encode(&self.password_raw);
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
pub struct ApiConfig {
    pub binance: BinanceApiConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceApiConfig {
    pub api_key: String,
    pub api_secret: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct NetworkConfig {
    #[serde(default)]
    pub proxy: ProxyConfig,
    #[serde(default)]
    pub rest_proxy: ProxyConfig,
    #[serde(default)]
    pub ws_proxy: ProxyConfig,
    #[serde(default)]
    pub http: HttpConfig,
}

impl NetworkConfig {
    pub fn effective_rest_proxy_url(&self) -> Option<String> {
        self.rest_proxy
            .effective_url()
            .or_else(|| self.proxy.effective_url())
    }

    pub fn effective_ws_proxy_url(&self) -> Option<String> {
        self.ws_proxy
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
pub struct HttpConfig {
    #[serde(default = "default_http_timeout_secs")]
    pub timeout_secs: u64,
    #[serde(default)]
    pub retry: HttpRetryConfig,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            timeout_secs: default_http_timeout_secs(),
            retry: HttpRetryConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct HttpRetryConfig {
    #[serde(default = "default_http_retry_enabled")]
    pub enabled: bool,
    #[serde(default = "default_http_retry_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_http_retry_base_backoff_ms")]
    pub base_backoff_ms: u64,
    #[serde(default = "default_http_retry_max_backoff_ms")]
    pub max_backoff_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ParquetConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_parquet_root_dir")]
    pub root_dir: String,
    #[serde(default = "default_parquet_compression")]
    pub compression: String,
}

impl Default for ParquetConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            root_dir: default_parquet_root_dir(),
            compression: default_parquet_compression(),
        }
    }
}

impl Default for HttpRetryConfig {
    fn default() -> Self {
        Self {
            enabled: default_http_retry_enabled(),
            max_retries: default_http_retry_max_retries(),
            base_backoff_ms: default_http_retry_base_backoff_ms(),
            max_backoff_ms: default_http_retry_max_backoff_ms(),
        }
    }
}

fn default_http_timeout_secs() -> u64 {
    30
}

fn default_http_retry_enabled() -> bool {
    true
}

fn default_http_retry_max_retries() -> u32 {
    3
}

fn default_http_retry_base_backoff_ms() -> u64 {
    500
}

fn default_http_retry_max_backoff_ms() -> u64 {
    8_000
}

fn default_parquet_root_dir() -> String {
    "c:/orderflow/orderflow_data/parquet".to_string()
}

fn default_parquet_compression() -> String {
    "uncompressed".to_string()
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
    #[serde(default = "default_mq_purge_startup_queues")]
    pub purge_startup_queues: bool,
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

fn default_mq_purge_startup_queues() -> bool {
    false
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

pub fn load_config(path: &str) -> Result<RootConfig> {
    let text = std::fs::read_to_string(path)?;
    let cfg: RootConfig = serde_yaml::from_str(&text)?;
    validate_config(&cfg)?;
    Ok(cfg)
}

fn validate_config(cfg: &RootConfig) -> Result<()> {
    if cfg.api.binance.api_key.trim().is_empty() {
        return Err(anyhow!("config.api.binance.api_key is empty"));
    }
    if cfg.api.binance.api_secret.trim().is_empty() {
        return Err(anyhow!("config.api.binance.api_secret is empty"));
    }
    if cfg.mq.exchanges.md_live.name.trim().is_empty() {
        return Err(anyhow!("mq.exchanges.md_live.name is empty"));
    }
    if cfg.mq.exchanges.md_replay.name.trim().is_empty() {
        return Err(anyhow!("mq.exchanges.md_replay.name is empty"));
    }
    if cfg.network.proxy.enabled && cfg.network.proxy.effective_url().is_none() {
        return Err(anyhow!(
            "config.network.proxy.enabled=true but network.proxy.address is missing"
        ));
    }
    if cfg.network.rest_proxy.enabled && cfg.network.rest_proxy.effective_url().is_none() {
        return Err(anyhow!(
            "config.network.rest_proxy.enabled=true but network.rest_proxy.address is missing"
        ));
    }
    if cfg.network.ws_proxy.enabled && cfg.network.ws_proxy.effective_url().is_none() {
        return Err(anyhow!(
            "config.network.ws_proxy.enabled=true but network.ws_proxy.address is missing"
        ));
    }
    if cfg.network.http.timeout_secs == 0 {
        return Err(anyhow!("config.network.http.timeout_secs must be > 0"));
    }
    if cfg.network.http.retry.enabled {
        if cfg.network.http.retry.base_backoff_ms == 0 {
            return Err(anyhow!(
                "config.network.http.retry.base_backoff_ms must be > 0 when retry is enabled"
            ));
        }
        if cfg.network.http.retry.max_backoff_ms < cfg.network.http.retry.base_backoff_ms {
            return Err(anyhow!(
                "config.network.http.retry.max_backoff_ms must be >= base_backoff_ms"
            ));
        }
    }
    if cfg.parquet.enabled && cfg.parquet.root_dir.trim().is_empty() {
        return Err(anyhow!(
            "config.parquet.enabled=true but parquet.root_dir is empty"
        ));
    }
    Ok(())
}

fn resolve_secret(raw: &str) -> String {
    std::env::var(raw).unwrap_or_else(|_| raw.to_string())
}
