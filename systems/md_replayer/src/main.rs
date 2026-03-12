use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use lapin::{
    options::{BasicPublishOptions, ExchangeDeclareOptions},
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties, ExchangeKind,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::time::Duration as StdDuration;
use tracing::{info, warn};
use tracing_subscriber::{fmt, EnvFilter};
use uuid::Uuid;

const CONFIG_PATH: &str = "config/config.yaml";

#[derive(Debug, Clone, Deserialize)]
struct RootConfig {
    app: AppSection,
    database: DatabaseConfig,
    mq: MqConfig,
    #[serde(default)]
    replayer: ReplayerConfig,
}

#[derive(Debug, Clone, Deserialize)]
struct AppSection {
    name: String,
    env: String,
    timezone: String,
}

#[derive(Debug, Clone, Deserialize)]
struct DatabaseConfig {
    host: String,
    port: u16,
    database: String,
    user: String,
    password_env: String,
    connect_timeout_secs: Option<u64>,
    application_name: Option<String>,
    sslmode: Option<String>,
    options: Option<String>,
}

impl DatabaseConfig {
    fn resolved_password(&self) -> String {
        resolve_secret(&self.password_env)
    }

    fn postgres_uri(&self) -> String {
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
struct MqConfig {
    host: String,
    port: u16,
    vhost: String,
    user: String,
    password_env: String,
    heartbeat_secs: Option<u16>,
    connection_timeout_secs: Option<u16>,
    exchanges: MqExchanges,
}

impl MqConfig {
    fn resolved_password(&self) -> String {
        resolve_secret(&self.password_env)
    }

    fn amqp_uri(&self) -> String {
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
struct MqExchanges {
    md_live: MqExchangeConfig,
    md_replay: MqExchangeConfig,
}

#[derive(Debug, Clone, Deserialize)]
struct MqExchangeConfig {
    name: String,
    #[serde(rename = "type")]
    kind: String,
    durable: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct ReplayerConfig {
    #[serde(default = "default_replay_symbol")]
    symbol: String,
    from_ts: Option<String>,
    to_ts: Option<String>,
    #[serde(default = "default_replay_market")]
    market: String, // all | spot | futures
    #[serde(default = "default_replay_batch_size")]
    batch_size: i64,
    #[serde(default)]
    publish_interval_ms: u64,
    #[serde(default)]
    dry_run: bool,
    #[serde(default = "default_replay_source_kind")]
    source_kind: String,
    #[serde(default = "default_backfill_flag")]
    backfill_in_progress: bool,
}

impl Default for ReplayerConfig {
    fn default() -> Self {
        Self {
            symbol: default_replay_symbol(),
            from_ts: None,
            to_ts: None,
            market: default_replay_market(),
            batch_size: default_replay_batch_size(),
            publish_interval_ms: 0,
            dry_run: false,
            source_kind: default_replay_source_kind(),
            backfill_in_progress: default_backfill_flag(),
        }
    }
}

fn default_replay_symbol() -> String {
    "ETHUSDT".to_string()
}

fn default_replay_market() -> String {
    "all".to_string()
}

fn default_replay_batch_size() -> i64 {
    5000
}

fn default_replay_source_kind() -> String {
    "replay".to_string()
}

fn default_backfill_flag() -> bool {
    true
}

#[derive(Debug)]
struct ReplayRow {
    event_ts: DateTime<Utc>,
    msg_type: String,
    market: String,
    symbol: String,
    routing_key: String,
    data_json: Value,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cfg = load_config(CONFIG_PATH).context("load config/config.yaml")?;
    validate_config(&cfg)?;

    let now = Utc::now();
    let from_ts = parse_or_default_from(cfg.replayer.from_ts.as_deref(), now)?;
    let to_ts = parse_or_default_to(cfg.replayer.to_ts.as_deref(), now)?;
    if from_ts >= to_ts {
        return Err(anyhow!("replayer.from_ts must be < replayer.to_ts"));
    }

    info!(
        app = %cfg.app.name,
        env = %cfg.app.env,
        timezone = %cfg.app.timezone,
        symbol = %cfg.replayer.symbol,
        from_ts = %from_ts,
        to_ts = %to_ts,
        market = %cfg.replayer.market,
        batch_size = cfg.replayer.batch_size,
        dry_run = cfg.replayer.dry_run,
        md_live_exchange = %cfg.mq.exchanges.md_live.name,
        md_replay_exchange = %cfg.mq.exchanges.md_replay.name,
        "md_replayer starting"
    );

    let db_pool = PgPoolOptions::new()
        .max_connections(5)
        .min_connections(1)
        .acquire_timeout(StdDuration::from_secs(
            cfg.database.connect_timeout_secs.unwrap_or(10),
        ))
        .connect(&cfg.database.postgres_uri())
        .await
        .context("connect postgres")?;

    let mq_conn = Connection::connect(&cfg.mq.amqp_uri(), ConnectionProperties::default())
        .await
        .context("connect rabbitmq")?;
    let producer_instance_id = format!("md-replayer-{}", Uuid::new_v4());
    let mq_ch = mq_conn
        .create_channel()
        .await
        .context("create mq channel")?;
    mq_ch
        .exchange_declare(
            &cfg.mq.exchanges.md_replay.name,
            exchange_kind(&cfg.mq.exchanges.md_replay.kind),
            ExchangeDeclareOptions {
                durable: cfg.mq.exchanges.md_replay.durable,
                auto_delete: false,
                internal: false,
                nowait: false,
                passive: false,
            },
            FieldTable::default(),
        )
        .await
        .with_context(|| {
            format!(
                "declare replay exchange {}",
                cfg.mq.exchanges.md_replay.name
            )
        })?;

    let mut offset = 0_i64;
    let mut total = 0_u64;
    loop {
        let rows = fetch_batch(
            &db_pool,
            from_ts,
            to_ts,
            &cfg.replayer.symbol.to_uppercase(),
            &cfg.replayer.market,
            cfg.replayer.batch_size,
            offset,
        )
        .await?;
        if rows.is_empty() {
            break;
        }

        for row in rows {
            total += 1;
            if cfg.replayer.dry_run {
                continue;
            }
            publish_row(
                &mq_ch,
                &cfg.mq.exchanges.md_replay.name,
                &cfg.replayer.source_kind,
                cfg.replayer.backfill_in_progress,
                &producer_instance_id,
                row,
            )
            .await?;
            if cfg.replayer.publish_interval_ms > 0 {
                tokio::time::sleep(StdDuration::from_millis(cfg.replayer.publish_interval_ms))
                    .await;
            }
        }

        offset += cfg.replayer.batch_size;
        info!(offset, total, "md_replayer progress");
    }

    info!(
        total,
        dry_run = cfg.replayer.dry_run,
        "md_replayer finished"
    );
    Ok(())
}

fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .with_thread_ids(true)
        .with_thread_names(true)
        .try_init();
}

fn load_config(path: &str) -> Result<RootConfig> {
    let text = std::fs::read_to_string(path)?;
    Ok(serde_yaml::from_str(&text)?)
}

fn validate_config(cfg: &RootConfig) -> Result<()> {
    if cfg.mq.exchanges.md_live.name.trim().is_empty() {
        return Err(anyhow!("mq.exchanges.md_live.name is empty"));
    }
    if cfg.mq.exchanges.md_replay.name.trim().is_empty() {
        return Err(anyhow!("mq.exchanges.md_replay.name is empty"));
    }
    match cfg.replayer.market.as_str() {
        "all" | "spot" | "futures" => {}
        other => {
            return Err(anyhow!(
                "replayer.market must be all|spot|futures, got {}",
                other
            ))
        }
    }
    if cfg.replayer.batch_size <= 0 {
        return Err(anyhow!("replayer.batch_size must be > 0"));
    }
    Ok(())
}

fn resolve_secret(raw: &str) -> String {
    std::env::var(raw).unwrap_or_else(|_| raw.to_string())
}

fn parse_or_default_from(value: Option<&str>, now: DateTime<Utc>) -> Result<DateTime<Utc>> {
    match value {
        Some(v) => parse_rfc3339(v),
        None => Ok(now - Duration::minutes(15)),
    }
}

fn parse_or_default_to(value: Option<&str>, now: DateTime<Utc>) -> Result<DateTime<Utc>> {
    match value {
        Some(v) => parse_rfc3339(v),
        None => Ok(now),
    }
}

fn parse_rfc3339(v: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(v)
        .with_context(|| format!("invalid RFC3339 timestamp: {}", v))?
        .with_timezone(&Utc))
}

fn exchange_kind(kind: &str) -> ExchangeKind {
    match kind {
        "fanout" => ExchangeKind::Fanout,
        "direct" => ExchangeKind::Direct,
        "topic" => ExchangeKind::Topic,
        _ => ExchangeKind::Topic,
    }
}

async fn fetch_batch(
    pool: &PgPool,
    from_ts: DateTime<Utc>,
    to_ts: DateTime<Utc>,
    symbol: &str,
    market: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<ReplayRow>> {
    let sql = r#"
    WITH events AS (
        SELECT
            t.ts_event AS event_ts,
            'md.trade'::text AS msg_type,
            t.market::text AS market,
            t.symbol AS symbol,
            format('md.%s.trade.%s', t.market::text, lower(t.symbol)) AS routing_key,
            jsonb_build_object(
                'price', t.price,
                'qty_eth', t.qty_eth,
                'notional_usdt', t.notional_usdt,
                'aggressor_side', t.aggressor_side
            ) AS data_json
        FROM md.trade_event t
        WHERE t.ts_event >= $1 AND t.ts_event < $2 AND t.symbol = $3

        UNION ALL

        SELECT
            d.ts_event AS event_ts,
            'md.depth'::text AS msg_type,
            d.market::text AS market,
            d.symbol AS symbol,
            format('md.%s.depth.%s', d.market::text, lower(d.symbol)) AS routing_key,
            jsonb_build_object(
                'first_update_id', d.first_update_id,
                'final_update_id', d.final_update_id,
                'prev_final_update_id', d.prev_final_update_id,
                'bids_delta', d.bids_delta,
                'asks_delta', d.asks_delta
            ) AS data_json
        FROM md.depth_delta_l2 d
        WHERE d.ts_event >= $1 AND d.ts_event < $2 AND d.symbol = $3

        UNION ALL

        SELECT
            s.ts_snapshot AS event_ts,
            'md.orderbook_snapshot_l2'::text AS msg_type,
            s.market::text AS market,
            s.symbol AS symbol,
            format('md.%s.orderbook_snapshot_l2.%s', s.market::text, lower(s.symbol)) AS routing_key,
            jsonb_build_object(
                'depth_levels', s.depth_levels,
                'last_update_id', s.last_update_id,
                'bids', s.bids,
                'asks', s.asks
            ) AS data_json
        FROM md.orderbook_snapshot_l2 s
        WHERE s.ts_snapshot >= $1 AND s.ts_snapshot < $2 AND s.symbol = $3

        UNION ALL

        SELECT
            b.ts_event AS event_ts,
            'md.bbo'::text AS msg_type,
            b.market::text AS market,
            b.symbol AS symbol,
            format('md.%s.bbo.%s', b.market::text, lower(b.symbol)) AS routing_key,
            jsonb_build_object(
                'bid_price', b.bid_price,
                'bid_qty', b.bid_qty,
                'ask_price', b.ask_price,
                'ask_qty', b.ask_qty,
                'event_count', b.sample_count,
                'sample_count', b.sample_count,
                'window_start_ts', to_char(b.ts_window_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                'window_end_ts', to_char(b.ts_window_end AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                'spread_min', b.spread_min,
                'spread_max', b.spread_max,
                'spread_avg', b.spread_avg,
                'mid_avg', b.mid_avg
            ) AS data_json
        FROM md.bbo_event_live_rollup b
        WHERE b.ts_event >= $1 AND b.ts_event < $2 AND b.symbol = $3

        UNION ALL

        SELECT
            k.close_time AS event_ts,
            'md.kline'::text AS msg_type,
            k.market::text AS market,
            k.symbol AS symbol,
            format('md.%s.kline.%s.%s', k.market::text, k.interval_code, lower(k.symbol)) AS routing_key,
            jsonb_build_object(
                'interval_code', k.interval_code,
                'open_time', to_char(k.open_time AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                'close_time', to_char(k.close_time AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                'open_price', k.open_price,
                'high_price', k.high_price,
                'low_price', k.low_price,
                'close_price', k.close_price,
                'volume_base', k.volume_base,
                'quote_volume', k.quote_volume,
                'trade_count', k.trade_count,
                'is_closed', k.is_closed
            ) AS data_json
        FROM md.kline_bar k
        WHERE k.close_time >= $1 AND k.close_time < $2 AND k.symbol = $3

        UNION ALL

        SELECT
            m.ts_event AS event_ts,
            'md.mark_price'::text AS msg_type,
            m.market::text AS market,
            m.symbol AS symbol,
            format('md.%s.mark_price.%s', m.market::text, lower(m.symbol)) AS routing_key,
            jsonb_build_object(
                'mark_price', m.mark_price,
                'index_price', m.index_price,
                'estimated_settle_price', m.estimated_settle_price,
                'funding_rate', m.funding_rate,
                'next_funding_time', CASE
                    WHEN m.next_funding_time IS NULL THEN NULL
                    ELSE to_char(m.next_funding_time AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
                END
            ) AS data_json
        FROM md.mark_price_funding_1s m
        WHERE m.ts_event >= $1 AND m.ts_event < $2 AND m.symbol = $3

        UNION ALL

        SELECT
            f.funding_time AS event_ts,
            'md.funding_rate'::text AS msg_type,
            f.market::text AS market,
            f.symbol AS symbol,
            format('md.futures.funding_rate.%s', lower(f.symbol)) AS routing_key,
            jsonb_build_object(
                'funding_time', to_char(f.funding_time AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                'funding_rate', f.funding_rate,
                'mark_price', f.mark_price,
                'next_funding_time', CASE
                    WHEN f.next_funding_time IS NULL THEN NULL
                    ELSE to_char(f.next_funding_time AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
                END
            ) AS data_json
        FROM md.funding_rate_event f
        WHERE f.funding_time >= $1 AND f.funding_time < $2 AND f.symbol = $3

        UNION ALL

        SELECT
            o.ts_event AS event_ts,
            'md.force_order'::text AS msg_type,
            o.market::text AS market,
            o.symbol AS symbol,
            format('md.futures.force_order.%s', lower(o.symbol)) AS routing_key,
            jsonb_build_object(
                'order_side', o.order_side::text,
                'price', o.price,
                'filled_qty', o.filled_qty,
                'notional_usdt', o.notional_usdt,
                'liq_side', o.liq_side
            ) AS data_json
        FROM md.force_order_event o
        WHERE o.ts_event >= $1 AND o.ts_event < $2 AND o.symbol = $3
    )
    SELECT event_ts, msg_type, market, symbol, routing_key, data_json
    FROM events
    WHERE ($4 = 'all' OR market = $4)
    ORDER BY event_ts ASC, msg_type ASC
    LIMIT $5 OFFSET $6
    "#;

    let rows = sqlx::query(sql)
        .bind(from_ts)
        .bind(to_ts)
        .bind(symbol)
        .bind(market)
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await
        .context("fetch replay batch")?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(ReplayRow {
            event_ts: row.get("event_ts"),
            msg_type: row.get("msg_type"),
            market: row.get("market"),
            symbol: row.get("symbol"),
            routing_key: row.get("routing_key"),
            data_json: row.get("data_json"),
        });
    }
    Ok(out)
}

async fn publish_row(
    channel: &lapin::Channel,
    replay_exchange: &str,
    source_kind: &str,
    backfill_in_progress: bool,
    producer_instance_id: &str,
    row: ReplayRow,
) -> Result<()> {
    let payload = json!({
        "schema_version": 1,
        "msg_type": row.msg_type,
        "message_id": Uuid::new_v4(),
        "trace_id": Uuid::new_v4(),
        "routing_key": row.routing_key,
        "market": row.market,
        "symbol": row.symbol,
        "source_kind": source_kind,
        "backfill_in_progress": backfill_in_progress,
        "event_ts": row.event_ts.to_rfc3339(),
        "published_at": Utc::now().to_rfc3339(),
        "producer": {
            "service": "md_replayer",
            "instance_id": producer_instance_id,
        },
        "data": row.data_json
    });
    let bytes = serde_json::to_vec(&payload).context("encode replay payload")?;

    let confirm = channel
        .basic_publish(
            replay_exchange,
            payload
                .get("routing_key")
                .and_then(Value::as_str)
                .unwrap_or_default(),
            BasicPublishOptions::default(),
            &bytes,
            BasicProperties::default()
                .with_content_type("application/json".into())
                .with_delivery_mode(2),
        )
        .await
        .context("publish replay message")?;
    if let Err(err) = confirm.await {
        warn!(error = %err, "publisher confirm failed");
    }
    Ok(())
}
