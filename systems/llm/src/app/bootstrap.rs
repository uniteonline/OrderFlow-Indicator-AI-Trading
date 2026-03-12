use crate::app::config::{load_config, MqConfig, MqQueueConfig, RootConfig};
use anyhow::{anyhow, Context, Result};
use lapin::{
    options::{ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions},
    types::{AMQPValue, FieldTable, LongString, ShortString},
    Channel, Connection, ConnectionProperties, ExchangeKind,
};
use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info};
use uuid::Uuid;

const CONFIG_PATH: &str = "config/config.yaml";

#[derive(Clone)]
pub struct AppContext {
    pub config: Arc<RootConfig>,
    pub mq_consume_channel: Channel,
    pub consume_queue_name: String,
    pub http_client: Client,
    pub producer_instance_id: String,
}

pub async fn bootstrap() -> Result<AppContext> {
    let config = Arc::new(load_config(CONFIG_PATH).context("load config/config.yaml")?);
    let default_provider = config.active_default_model();
    let effective_models = resolve_effective_models_summary(&config);

    let enabled_models = config
        .llm
        .models
        .iter()
        .filter(|m| m.enabled)
        .map(|m| format!("{}:{}:{}", m.name, m.provider, m.model))
        .collect::<Vec<_>>();

    debug!(
        app_name = %config.app.name,
        env = %config.app.env,
        timezone = %config.app.timezone,
        symbol = %config.llm.symbol,
        prompt_template = %config.llm.prompt_template,
        queue_key = %config.llm.queue_key,
        purge_queue_on_start = config.llm.purge_queue_on_start,
        call_interval_secs = config.llm.call_interval_secs,
        bundle_settle_ms = config.llm.bundle_settle_ms,
        call_schedule_minutes = ?config.llm.call_schedule_minutes,
        claude_mode = %config.api.claude.mode,
        execution_enabled = config.llm.execution.enabled,
        execution_dry_run = config.llm.execution.dry_run,
        execution_account_margin_ratio = config.llm.execution.account_margin_ratio,
        execution_margin_usdt = config.llm.execution.margin_usdt,
        execution_default_leverage_ratio = config.llm.execution.default_leverage_ratio,
        execution_max_leverage = config.llm.execution.max_leverage,
        execution_hedge_mode = config.llm.execution.hedge_mode,
        execution_entry_sl_remap_enabled = config.llm.execution.entry_sl_remap.enabled,
        execution_entry_to_sl_distance_pct = config.llm.execution.entry_sl_remap.entry_to_sl_distance_pct,
        claude_batch_poll_interval_secs = config.api.claude.batch_poll_interval_secs,
        model_count = enabled_models.len(),
        models = %enabled_models.join(","),
        "loaded llm config"
    );
    info!(
        default_model = %default_provider,
        prompt_template = %config.llm.prompt_template,
        effective_models = %effective_models,
        "llm model provider selection resolved"
    );

    let mq_connection = Connection::connect(&config.mq.amqp_uri(), ConnectionProperties::default())
        .await
        .context("connect rabbitmq")?;

    let topology_channel = mq_connection
        .create_channel()
        .await
        .context("create topology channel")?;
    declare_topology_for_llm(&topology_channel, &config.mq, &config.llm.queue_key).await?;

    let mq_consume_channel = mq_connection
        .create_channel()
        .await
        .context("create mq consume channel")?;

    let rest_proxy_url = config.network.effective_rest_proxy_url();
    let mut http_builder = Client::builder()
        .timeout(Duration::from_secs(config.llm.request_timeout_secs))
        .user_agent("orderflow-llm/0.1.0");
    if let Some(proxy) = rest_proxy_url.as_deref() {
        let proxy_setting = reqwest::Proxy::all(proxy)
            .with_context(|| format!("invalid proxy url for llm rest client: {}", proxy))?;
        http_builder = http_builder.proxy(proxy_setting);
        info!(
            rest_proxy_enabled = true,
            rest_proxy = %proxy,
            "llm rest proxy configured"
        );
    }
    let http_client = http_builder.build().context("build llm http client")?;

    let queue_cfg = config
        .mq
        .queues
        .get(&config.llm.queue_key)
        .ok_or_else(|| anyhow!("missing llm queue config"))?;
    let consume_queue_name = queue_cfg.name.clone();

    let producer_instance_id = format!("llm-{}", Uuid::new_v4());
    debug!(
        consume_queue = %consume_queue_name,
        request_timeout_secs = config.llm.request_timeout_secs,
        claude_mode = %config.api.claude.mode,
        execution_enabled = config.llm.execution.enabled,
        execution_dry_run = config.llm.execution.dry_run,
        execution_account_margin_ratio = config.llm.execution.account_margin_ratio,
        execution_default_leverage_ratio = config.llm.execution.default_leverage_ratio,
        execution_hedge_mode = config.llm.execution.hedge_mode,
        execution_entry_sl_remap_enabled = config.llm.execution.entry_sl_remap.enabled,
        execution_entry_to_sl_distance_pct = config.llm.execution.entry_sl_remap.entry_to_sl_distance_pct,
        rest_proxy_enabled = rest_proxy_url.is_some(),
        producer_instance_id = %producer_instance_id,
        "llm bootstrap completed"
    );

    Ok(AppContext {
        config,
        mq_consume_channel,
        consume_queue_name,
        http_client,
        producer_instance_id,
    })
}

async fn declare_topology_for_llm(channel: &Channel, mq: &MqConfig, queue_key: &str) -> Result<()> {
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

    let queue_cfg = mq
        .queues
        .get(queue_key)
        .ok_or_else(|| anyhow!("llm queue key={} not found", queue_key))?;
    declare_and_bind_queue(channel, queue_cfg).await?;
    Ok(())
}

async fn declare_and_bind_queue(channel: &Channel, queue_cfg: &MqQueueConfig) -> Result<()> {
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

    Ok(())
}

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

fn exchange_kind(kind: &str) -> ExchangeKind {
    match kind {
        "fanout" => ExchangeKind::Fanout,
        "direct" => ExchangeKind::Direct,
        "topic" => ExchangeKind::Topic,
        _ => ExchangeKind::Topic,
    }
}

fn resolve_effective_models_summary(config: &RootConfig) -> String {
    let default_provider = config.active_default_model();
    let selected = config
        .selected_enabled_models_for_default()
        .into_iter()
        .map(|m| {
            if m.provider.eq_ignore_ascii_case("gemini") && m.should_use_openrouter() {
                format!("{}:{}(via openrouter)", m.name, m.model)
            } else {
                format!("{}:{}", m.name, m.model)
            }
        })
        .collect::<Vec<_>>();

    if !selected.is_empty() {
        return selected.join(",");
    }

    if default_provider == "qwen" {
        return format!(
            "qwen_default:{}(from api.qwen.model)",
            config.api.qwen.model
        );
    }
    if default_provider == "custom_llm" {
        return format!(
            "custom_llm_default:{}(from api.custom_llm.model)",
            config.api.custom_llm.model
        );
    }
    if default_provider == "gemini" {
        return format!(
            "gemini_default:{}(from api.gemini.model)",
            config.api.gemini.model
        );
    }
    if default_provider == "grok" {
        return format!(
            "grok_default:{}(from api.grok.model)",
            config.api.grok.model
        );
    }

    "-".to_string()
}
