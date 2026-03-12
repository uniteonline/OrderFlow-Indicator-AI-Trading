use anyhow::{anyhow, Context, Result};
use chrono::{NaiveDate, Utc};
use lapin::{
    options::BasicPublishOptions,
    publisher_confirm::{Confirmation, PublisherConfirm},
    types::{AMQPValue, FieldTable, LongString, ShortString},
    BasicProperties, Channel,
};
use serde_json::Value;
use sqlx::postgres::PgListener;
use sqlx::{FromRow, PgPool};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::Instant;
use tracing::{error, info, warn};

const OUTBOX_DISPATCH_BATCH_SIZE: i64 = 1_000;
const OUTBOX_NOTIFY_CHANNEL: &str = "outbox_ready";
const OUTBOX_NOTIFY_TIMEOUT_SECS: u64 = 5;
const OUTBOX_HOUSEKEEPING_INTERVAL_SECS: u64 = 600;
const OUTBOX_DEAD_RETENTION_HOURS: i32 = 24;
const OUTBOX_GC_BATCH_SIZE: i64 = 10_000;
const OUTBOX_GC_MAX_BATCHES_PER_ROUND: usize = 3;
const OUTBOX_PARTITION_PRECREATE_DAYS_AHEAD: i32 = 7;
const OUTBOX_PARTITION_PRECREATE_DAYS_BACK: i32 = 2;
const OUTBOX_BACKLOG_WARN_SECS: i64 = 30;
const OUTBOX_BACKLOG_TOP_KEYS: usize = 8;

#[derive(Clone)]
pub struct OutboxDispatcher {
    pool: PgPool,
    channel: Channel,
    live_exchange_name: String,
}

impl OutboxDispatcher {
    pub fn new(pool: PgPool, channel: Channel, live_exchange_name: String) -> Self {
        Self {
            pool,
            channel,
            live_exchange_name,
        }
    }

    pub async fn run_loop(&self) -> Result<()> {
        self.run_loop_worker(true).await
    }

    pub async fn run_loop_worker(&self, perform_housekeeping: bool) -> Result<()> {
        let mut listener = PgListener::connect_with(&self.pool)
            .await
            .context("create outbox PgListener")?;
        listener
            .listen(OUTBOX_NOTIFY_CHANNEL)
            .await
            .context("listen outbox_ready")?;

        let mut next_housekeeping_at =
            Instant::now() + Duration::from_secs(OUTBOX_HOUSEKEEPING_INTERVAL_SECS);
        if perform_housekeeping {
            if let Err(err) = self.ensure_partitions().await {
                warn!(
                    error = %err,
                    debug_error = ?err,
                    "outbox partition maintenance init failed"
                );
            }
        }
        info!(
            perform_housekeeping = perform_housekeeping,
            notify_channel = OUTBOX_NOTIFY_CHANNEL,
            notify_timeout_secs = OUTBOX_NOTIFY_TIMEOUT_SECS,
            batch_size = OUTBOX_DISPATCH_BATCH_SIZE,
            housekeeping_interval_secs = OUTBOX_HOUSEKEEPING_INTERVAL_SECS,
            dead_retention_hours = OUTBOX_DEAD_RETENTION_HOURS,
            gc_batch_size = OUTBOX_GC_BATCH_SIZE,
            partition_days_ahead = OUTBOX_PARTITION_PRECREATE_DAYS_AHEAD,
            partition_days_back = OUTBOX_PARTITION_PRECREATE_DAYS_BACK,
            "outbox dispatcher started (listen/notify mode)"
        );

        loop {
            // Wait for INSERT notification or timeout (fallback poll).
            let notified = tokio::time::timeout(
                Duration::from_secs(OUTBOX_NOTIFY_TIMEOUT_SECS),
                listener.recv(),
            )
            .await;

            match notified {
                Ok(Ok(_notification)) => { /* triggered by INSERT — dispatch immediately */ }
                Ok(Err(err)) => {
                    // sqlx PgListener auto-reconnects; log and continue.
                    warn!(error = %err, "outbox listener recv error, will retry");
                }
                Err(_timeout) => { /* periodic fallback — dispatch anyway */ }
            }

            if let Err(err) = self.dispatch_batch(OUTBOX_DISPATCH_BATCH_SIZE).await {
                warn!(error = %err, debug_error = ?err, "outbox dispatch batch failed");
            }

            if perform_housekeeping && Instant::now() >= next_housekeeping_at {
                if let Err(err) = self.prune_dead_rows().await {
                    warn!(error = %err, debug_error = ?err, "outbox dead-row gc failed");
                }
                if let Err(err) = self.ensure_partitions().await {
                    warn!(
                        error = %err,
                        debug_error = ?err,
                        "outbox partition maintenance failed"
                    );
                }
                next_housekeeping_at =
                    Instant::now() + Duration::from_secs(OUTBOX_HOUSEKEEPING_INTERVAL_SECS);
            }
        }
    }

    async fn dispatch_batch(&self, batch_size: i64) -> Result<()> {
        let rows = self.claim_batch(batch_size).await?;
        let mut sent_keys: Vec<(NaiveDate, i64)> = Vec::with_capacity(rows.len());
        let mut pending_confirms: Vec<(NaiveDate, i64, PublisherConfirm)> =
            Vec::with_capacity(rows.len());
        let mut max_outbox_age_secs: i64 = 0;
        let mut backlog_by_key: HashMap<(String, String), OutboxBacklogStat> = HashMap::new();

        for row in rows {
            let outbox_age_secs = (Utc::now() - row.created_at).num_seconds();
            max_outbox_age_secs = max_outbox_age_secs.max(outbox_age_secs);
            let bucket = backlog_by_key
                .entry((row.exchange_name.clone(), row.routing_key.clone()))
                .or_default();
            bucket.count = bucket.count.saturating_add(1);
            bucket.max_outbox_age_secs = bucket.max_outbox_age_secs.max(outbox_age_secs);

            match self.publish_row(&row).await {
                Ok(confirm) => {
                    pending_confirms.push((row.bucket_date, row.outbox_id, confirm));
                }
                Err(err) => {
                    error!(error = %err, outbox_id = row.outbox_id, "outbox publish failed");
                    self.mark_failed(row.bucket_date, row.outbox_id, err.to_string())
                        .await?;
                }
            }
        }

        for (bucket_date, outbox_id, confirm) in pending_confirms {
            let confirmation = match confirm.await.context("wait publisher confirm") {
                Ok(c) => c,
                Err(err) => {
                    error!(error = %err, outbox_id, "outbox publisher confirm failed");
                    self.mark_failed(bucket_date, outbox_id, err.to_string())
                        .await?;
                    continue;
                }
            };

            match confirmation {
                Confirmation::Ack(_) => sent_keys.push((bucket_date, outbox_id)),
                Confirmation::Nack(returned) => {
                    let err_text = format!(
                        "broker nack for outbox_id={} returned={}",
                        outbox_id,
                        returned.is_some()
                    );
                    error!(outbox_id, "outbox broker nack");
                    self.mark_failed(bucket_date, outbox_id, err_text).await?;
                }
                Confirmation::NotRequested => {
                    let err_text = "publisher confirm not requested on channel".to_string();
                    error!(outbox_id, "outbox publish confirmation missing");
                    self.mark_failed(bucket_date, outbox_id, err_text).await?;
                }
            }
        }

        if max_outbox_age_secs >= OUTBOX_BACKLOG_WARN_SECS && !backlog_by_key.is_empty() {
            let mut ranked: Vec<((String, String), OutboxBacklogStat)> =
                backlog_by_key.into_iter().collect();
            ranked.sort_by(|a, b| b.1.max_outbox_age_secs.cmp(&a.1.max_outbox_age_secs));
            let top_keys = ranked
                .into_iter()
                .take(OUTBOX_BACKLOG_TOP_KEYS)
                .map(|((exchange, routing_key), stat)| {
                    format!(
                        "{}|{}:count={},max_age_secs={}",
                        exchange, routing_key, stat.count, stat.max_outbox_age_secs
                    )
                })
                .collect::<Vec<_>>()
                .join(";");
            warn!(
                max_outbox_age_secs = max_outbox_age_secs,
                delivered_count = sent_keys.len(),
                top_backlog_keys = %top_keys,
                "outbox backlog by exchange/routing_key"
            );
        }

        if max_outbox_age_secs >= 60 {
            warn!(
                max_outbox_age_secs,
                delivered_count = sent_keys.len(),
                "outbox dispatch lag detected"
            );
        }

        self.delete_sent_batch(&sent_keys).await?;
        Ok(())
    }

    async fn claim_batch(&self, batch_size: i64) -> Result<Vec<OutboxRow>> {
        if batch_size <= 0 {
            return Ok(Vec::new());
        }

        let mut claimed = Vec::with_capacity(batch_size as usize);
        let mut remaining = batch_size;

        let mut live_trade = self
            .claim_batch_tier(remaining, ClaimTier::LiveTrade)
            .await?;
        remaining -= live_trade.len() as i64;
        claimed.append(&mut live_trade);
        if remaining <= 0 {
            return Ok(claimed);
        }

        let mut live_non_trade = self
            .claim_batch_tier(remaining, ClaimTier::LiveNonTrade)
            .await?;
        remaining -= live_non_trade.len() as i64;
        claimed.append(&mut live_non_trade);
        if remaining <= 0 {
            return Ok(claimed);
        }

        let mut fallback = self
            .claim_batch_tier(remaining, ClaimTier::FallbackAnyExchange)
            .await?;
        claimed.append(&mut fallback);

        Ok(claimed)
    }

    async fn claim_batch_tier(&self, batch_size: i64, tier: ClaimTier) -> Result<Vec<OutboxRow>> {
        let (sql, context_text) = match tier {
            ClaimTier::LiveTrade => (
                r#"
                WITH picked AS (
                    SELECT bucket_date, outbox_id
                    FROM ops.outbox_event
                    WHERE status IN ('pending', 'failed', 'sending')
                      AND available_at <= now()
                      AND exchange_name = $2
                      AND routing_key LIKE 'md.%.trade.%'
                    ORDER BY available_at, outbox_id
                    LIMIT $1
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE ops.outbox_event o
                SET status = 'sending',
                    available_at = now() + interval '30 seconds'
                FROM picked
                WHERE o.bucket_date = picked.bucket_date
                  AND o.outbox_id = picked.outbox_id
                RETURNING
                    o.bucket_date,
                    o.outbox_id,
                    o.created_at,
                    o.exchange_name,
                    o.routing_key,
                    o.message_id,
                    o.headers_json,
                    o.payload_json
                "#,
                "claim live trade outbox rows",
            ),
            ClaimTier::LiveNonTrade => (
                r#"
                WITH picked AS (
                    SELECT bucket_date, outbox_id
                    FROM ops.outbox_event
                    WHERE status IN ('pending', 'failed', 'sending')
                      AND available_at <= now()
                      AND exchange_name = $2
                      AND routing_key NOT LIKE 'md.%.trade.%'
                    ORDER BY available_at, outbox_id
                    LIMIT $1
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE ops.outbox_event o
                SET status = 'sending',
                    available_at = now() + interval '30 seconds'
                FROM picked
                WHERE o.bucket_date = picked.bucket_date
                  AND o.outbox_id = picked.outbox_id
                RETURNING
                    o.bucket_date,
                    o.outbox_id,
                    o.created_at,
                    o.exchange_name,
                    o.routing_key,
                    o.message_id,
                    o.headers_json,
                    o.payload_json
                "#,
                "claim live non-trade outbox rows",
            ),
            ClaimTier::FallbackAnyExchange => (
                r#"
                WITH picked AS (
                    SELECT bucket_date, outbox_id
                    FROM ops.outbox_event
                    WHERE status IN ('pending', 'failed', 'sending')
                      AND available_at <= now()
                      AND exchange_name <> $2
                    ORDER BY available_at, outbox_id
                    LIMIT $1
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE ops.outbox_event o
                SET status = 'sending',
                    available_at = now() + interval '30 seconds'
                FROM picked
                WHERE o.bucket_date = picked.bucket_date
                  AND o.outbox_id = picked.outbox_id
                RETURNING
                    o.bucket_date,
                    o.outbox_id,
                    o.created_at,
                    o.exchange_name,
                    o.routing_key,
                    o.message_id,
                    o.headers_json,
                    o.payload_json
                "#,
                "claim fallback outbox rows",
            ),
        };

        sqlx::query_as(sql)
            .bind(batch_size)
            .bind(&self.live_exchange_name)
            .fetch_all(&self.pool)
            .await
            .context(context_text)
    }

    async fn publish_row(&self, row: &OutboxRow) -> Result<PublisherConfirm> {
        let payload = serde_json::to_vec(&row.payload_json).context("serialize outbox payload")?;
        let headers = json_to_field_table(&row.headers_json).context("build amqp headers")?;

        let properties = BasicProperties::default()
            .with_content_type(ShortString::from("application/json"))
            .with_delivery_mode(2)
            .with_headers(headers)
            .with_message_id(ShortString::from(row.message_id.to_string()));

        let confirm = self
            .channel
            .basic_publish(
                &row.exchange_name,
                &row.routing_key,
                BasicPublishOptions::default(),
                &payload,
                properties,
            )
            .await
            .with_context(|| {
                format!(
                    "basic_publish exchange={} routing_key={}",
                    row.exchange_name, row.routing_key
                )
            })?;

        Ok(confirm)
    }

    async fn delete_sent_batch(&self, outbox_keys: &[(NaiveDate, i64)]) -> Result<()> {
        if outbox_keys.is_empty() {
            return Ok(());
        }

        let mut bucket_dates = Vec::with_capacity(outbox_keys.len());
        let mut outbox_ids = Vec::with_capacity(outbox_keys.len());
        for (bucket_date, outbox_id) in outbox_keys {
            bucket_dates.push(*bucket_date);
            outbox_ids.push(*outbox_id);
        }

        sqlx::query(
            r#"
            DELETE FROM ops.outbox_event o
            USING UNNEST($1::DATE[], $2::BIGINT[]) AS t(bucket_date, outbox_id)
            WHERE o.bucket_date = t.bucket_date
              AND o.outbox_id = t.outbox_id
            "#,
        )
        .bind(bucket_dates)
        .bind(outbox_ids)
        .execute(&self.pool)
        .await
        .context("delete delivered outbox rows")?;
        Ok(())
    }

    async fn mark_failed(
        &self,
        bucket_date: NaiveDate,
        outbox_id: i64,
        err_text: String,
    ) -> Result<()> {
        let result = sqlx::query(
            r#"
            UPDATE ops.outbox_event
            SET status = CASE WHEN retry_count + 1 >= 10 THEN 'dead' ELSE 'failed' END,
                retry_count = retry_count + 1,
                available_at = now() + make_interval(secs => LEAST(3 * (retry_count + 1), 60)),
                error_text = $3
            WHERE bucket_date = $1
              AND outbox_id = $2
            "#,
        )
        .bind(bucket_date)
        .bind(outbox_id)
        .bind(&err_text)
        .execute(&self.pool)
        .await;

        match result {
            Ok(_) => Ok(()),
            Err(err) if is_undefined_column(&err, "error_text") => {
                sqlx::query(
                    r#"
                    UPDATE ops.outbox_event
                    SET status = CASE WHEN retry_count + 1 >= 10 THEN 'dead' ELSE 'failed' END,
                        retry_count = retry_count + 1,
                        available_at = now() + make_interval(secs => LEAST(3 * (retry_count + 1), 60))
                    WHERE bucket_date = $1
                      AND outbox_id = $2
                    "#,
                )
                .bind(bucket_date)
                .bind(outbox_id)
                .execute(&self.pool)
                .await
                .context("mark outbox failed (legacy schema)")?;
                Ok(())
            }
            Err(err) => Err(err).context("mark outbox failed"),
        }
    }

    async fn prune_dead_rows(&self) -> Result<()> {
        let mut total_deleted: u64 = 0;
        for _ in 0..OUTBOX_GC_MAX_BATCHES_PER_ROUND {
            let result = sqlx::query(
                r#"
                WITH doomed AS (
                    SELECT outbox_id
                    FROM ops.outbox_event
                    WHERE status = 'dead'
                      AND available_at < now() - ($1::INT * interval '1 hour')
                    ORDER BY available_at
                    LIMIT $2
                )
                DELETE FROM ops.outbox_event o
                USING doomed d
                WHERE o.outbox_id = d.outbox_id
                "#,
            )
            .bind(OUTBOX_DEAD_RETENTION_HOURS)
            .bind(OUTBOX_GC_BATCH_SIZE)
            .execute(&self.pool)
            .await
            .context("delete old dead outbox rows")?;

            let deleted = result.rows_affected();
            total_deleted += deleted;
            if deleted < OUTBOX_GC_BATCH_SIZE as u64 {
                break;
            }
        }

        if total_deleted > 0 {
            info!(
                deleted_rows = total_deleted,
                retention_hours = OUTBOX_DEAD_RETENTION_HOURS,
                "outbox gc deleted old dead rows"
            );
        }
        Ok(())
    }

    async fn ensure_partitions(&self) -> Result<()> {
        let result = sqlx::query(
            r#"
            SELECT ops.ensure_outbox_event_partitions($1, $2)
            "#,
        )
        .bind(OUTBOX_PARTITION_PRECREATE_DAYS_AHEAD)
        .bind(OUTBOX_PARTITION_PRECREATE_DAYS_BACK)
        .execute(&self.pool)
        .await;

        match result {
            Ok(_) => Ok(()),
            Err(err) if is_undefined_function(&err, "ensure_outbox_event_partitions") => Ok(()),
            Err(err) => Err(err).context("ensure outbox partitions"),
        }
    }
}

#[derive(Debug, Clone, FromRow)]
struct OutboxRow {
    bucket_date: chrono::NaiveDate,
    outbox_id: i64,
    created_at: chrono::DateTime<Utc>,
    exchange_name: String,
    routing_key: String,
    message_id: uuid::Uuid,
    headers_json: Value,
    payload_json: Value,
}

#[derive(Debug, Clone, Default)]
struct OutboxBacklogStat {
    count: u64,
    max_outbox_age_secs: i64,
}

#[derive(Debug, Clone, Copy)]
enum ClaimTier {
    LiveTrade,
    LiveNonTrade,
    FallbackAnyExchange,
}

fn json_to_field_table(raw: &Value) -> Result<FieldTable> {
    let map = raw
        .as_object()
        .ok_or_else(|| anyhow!("headers_json is not object"))?;

    let mut table = FieldTable::default();
    for (k, v) in map {
        let key = ShortString::from(k.as_str());
        let value = json_to_amqp_value(v);
        table.insert(key, value);
    }
    Ok(table)
}

fn json_to_amqp_value(v: &Value) -> AMQPValue {
    match v {
        Value::Bool(b) => AMQPValue::Boolean(*b),
        Value::Number(n) => AMQPValue::LongString(LongString::from(n.to_string())),
        Value::String(s) => AMQPValue::LongString(LongString::from(s.clone())),
        Value::Null => AMQPValue::LongString(LongString::from("")),
        other => AMQPValue::LongString(LongString::from(other.to_string())),
    }
}

fn is_undefined_column(err: &sqlx::Error, column_name: &str) -> bool {
    if let sqlx::Error::Database(db_err) = err {
        if db_err.code().as_deref() == Some("42703") && db_err.message().contains(column_name) {
            return true;
        }
    }
    let pattern = format!("column \"{}\" does not exist", column_name);
    err.to_string().contains(&pattern)
}

fn is_undefined_function(err: &sqlx::Error, fn_name: &str) -> bool {
    if let sqlx::Error::Database(db_err) = err {
        if db_err.code().as_deref() == Some("42883") && db_err.message().contains(fn_name) {
            return true;
        }
    }
    false
}
