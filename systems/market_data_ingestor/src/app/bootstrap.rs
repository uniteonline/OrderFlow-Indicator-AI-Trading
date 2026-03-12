use crate::app::config::{load_config, MqConfig, ResolvedDatabaseConfig, RootConfig};
use anyhow::{Context, Result};
use lapin::{
    options::{
        ConfirmSelectOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
        QueuePurgeOptions,
    },
    types::{AMQPValue, FieldTable, LongString, ShortString},
    Channel, Connection, ConnectionProperties, ExchangeKind,
};
use reqwest::Client;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use uuid::Uuid;

const CONFIG_PATH: &str = "config/config.yaml";
const INGESTOR_SELFCHECK_QUEUE: &str = "q.ingestor.selfcheck";

#[derive(Clone)]
pub struct AppContext {
    pub config: Arc<RootConfig>,
    pub md_db_pool: PgPool,
    pub ops_db_pool: PgPool,
    pub mq_connection: Arc<Connection>,
    pub mq_publish_channel: Channel,
    pub mq_consume_channel: Channel,
    pub http_client: Client,
    pub rest_proxy_url: Option<String>,
    pub ws_proxy_url: Option<String>,
    pub producer_instance_id: String,
    pub selfcheck_queue: String,
}

pub async fn bootstrap() -> Result<AppContext> {
    let config = Arc::new(load_config(CONFIG_PATH).context("load config/config.yaml")?);

    info!(
        app_name = %config.app.name,
        env = %config.app.env,
        timezone = %config.app.timezone,
        "loaded config"
    );

    let md_db_cfg = config.database.resolve_md();
    let ops_db_cfg = config.database.resolve_ops();
    let md_db_pool = build_db_pool("md", &md_db_cfg).await?;
    if md_db_cfg == ops_db_cfg {
        info!("md/ops database endpoints are identical; creating dedicated pools to reduce contention");
    }
    let ops_db_pool = build_db_pool("ops", &ops_db_cfg).await?;
    let mq_connection = Arc::new(
        Connection::connect(&config.mq.amqp_uri(), ConnectionProperties::default())
            .await
            .context("connect rabbitmq")?,
    );

    let topology_channel = mq_connection
        .create_channel()
        .await
        .context("create topology channel")?;
    declare_topology(&topology_channel, &config.mq).await?;
    if config.mq.purge_startup_queues {
        purge_startup_queues(&topology_channel, &config.mq).await?;
    } else {
        info!("startup queue purge disabled (mq.purge_startup_queues=false)");
    }

    let mq_publish_channel = mq_connection
        .create_channel()
        .await
        .context("create mq publish channel")?;
    mq_publish_channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .context("enable publisher confirms for mq publish channel")?;

    let mq_consume_channel = mq_connection
        .create_channel()
        .await
        .context("create mq consume channel")?;

    let rest_proxy_url = config.network.effective_rest_proxy_url();
    let ws_proxy_url = config.network.effective_ws_proxy_url();
    let http_timeout_secs = config.network.http.timeout_secs;
    // pool_idle_timeout: reclaim idle connections after 20 s so that reqwest never
    // reuses a connection that the upstream proxy has already closed (proxy idle
    // timeouts typically fire around 30–60 s, which coincides with our 30-second
    // depth-snapshot interval and produces "connection closed before message
    // completed" transport errors).
    let mut http_builder = Client::builder()
        .timeout(Duration::from_secs(http_timeout_secs))
        .pool_idle_timeout(Duration::from_secs(20))
        .user_agent("orderflow-market-data-ingestor/0.1.0");
    if let Some(proxy) = &rest_proxy_url {
        let proxy_setting = reqwest::Proxy::all(proxy).with_context(|| {
            format!(
                "invalid proxy url for network.rest_proxy/network.proxy.address: {}",
                proxy
            )
        })?;
        http_builder = http_builder.proxy(proxy_setting);
        info!(proxy = %proxy, "http rest proxy enabled");
    }
    if let Some(proxy) = &ws_proxy_url {
        info!(proxy = %proxy, "websocket proxy enabled");
    }

    let http_client = http_builder.build().context("build http client")?;

    info!(
        timeout_secs = http_timeout_secs,
        retry_enabled = config.network.http.retry.enabled,
        retry_max_retries = config.network.http.retry.max_retries,
        retry_base_backoff_ms = config.network.http.retry.base_backoff_ms,
        retry_max_backoff_ms = config.network.http.retry.max_backoff_ms,
        "http rest policy loaded"
    );

    let producer_instance_id = format!("ingestor-{}", Uuid::new_v4());

    info!(
        mq_live_exchange = %config.mq.exchanges.md_live.name,
        mq_replay_exchange = %config.mq.exchanges.md_replay.name,
        selfcheck_queue = INGESTOR_SELFCHECK_QUEUE,
        "bootstrap completed"
    );

    Ok(AppContext {
        config,
        md_db_pool,
        ops_db_pool,
        mq_connection,
        mq_publish_channel,
        mq_consume_channel,
        http_client,
        rest_proxy_url,
        ws_proxy_url,
        producer_instance_id,
        selfcheck_queue: INGESTOR_SELFCHECK_QUEUE.to_string(),
    })
}

async fn build_db_pool(role: &'static str, config: &ResolvedDatabaseConfig) -> Result<PgPool> {
    let connect_timeout = config.connect_timeout_secs.unwrap_or(10);
    let uri = config.postgres_uri();
    let pool_cfg = config.pool.as_ref();

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

    let pool = pool_options
        .connect(&uri)
        .await
        .with_context(|| format!("connect postgres for {role}"))?;

    sqlx::query("SELECT 1")
        .execute(&pool)
        .await
        .with_context(|| format!("postgres health query for {role}"))?;

    info!(
        role = role,
        max_connections = max_connections,
        min_connections = min_connections,
        acquire_timeout_secs = acquire_timeout_secs,
        "database pool ready"
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

    channel
        .queue_declare(
            INGESTOR_SELFCHECK_QUEUE,
            QueueDeclareOptions {
                durable: true,
                exclusive: false,
                auto_delete: false,
                nowait: false,
                passive: false,
            },
            FieldTable::default(),
        )
        .await
        .context("declare selfcheck queue")?;

    channel
        .queue_bind(
            INGESTOR_SELFCHECK_QUEUE,
            &mq.exchanges.md_live.name,
            "md.#",
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .context("bind selfcheck queue to x.md.live")?;

    Ok(())
}

async fn purge_startup_queues(channel: &Channel, mq: &MqConfig) -> Result<()> {
    let mut queue_names = BTreeSet::new();
    for (queue_key, queue_cfg) in &mq.queues {
        if is_legacy_indicator_queue_key(queue_key) {
            continue;
        }
        queue_names.insert(queue_cfg.name.clone());
    }
    queue_names.insert(INGESTOR_SELFCHECK_QUEUE.to_string());

    for queue_name in &queue_names {
        let purged = channel
            .queue_purge(queue_name, QueuePurgeOptions::default())
            .await
            .with_context(|| format!("purge queue {}", queue_name))?;
        info!(
            queue = %queue_name,
            purged = purged,
            "purged startup legacy messages"
        );
    }

    info!(
        queue_count = queue_names.len(),
        "startup queue purge completed"
    );

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
        // drop-head: evict oldest messages when at capacity instead of
        // rejecting or blocking publishers.
        args.insert(
            ShortString::from("x-overflow"),
            AMQPValue::LongString(LongString::from("drop-head")),
        );
    }
    args
}
