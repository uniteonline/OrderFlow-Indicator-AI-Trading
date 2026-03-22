use anyhow::{anyhow, Context, Result};
use chrono::NaiveDate;
use lapin::{
    options::BasicPublishOptions,
    publisher_confirm::{Confirmation, PublisherConfirm},
    types::{AMQPValue, FieldTable, LongString, ShortString},
    BasicProperties, Channel,
};
use serde_json::Value;
use sqlx::postgres::PgListener;
use sqlx::{FromRow, PgPool};
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
const OUTBOX_CLAIM_WARN_MS: u128 = 1_000;
const OUTBOX_DISPATCH_WARN_MS: u128 = 2_000;
const OUTBOX_MAX_BATCHES_PER_WAKE: usize = 32;

#[derive(Clone)]
pub struct OutboxDispatcher {
    pool: PgPool,
    channel: Channel,
    exchange_name: String,
}

impl OutboxDispatcher {
    pub fn new(pool: PgPool, channel: Channel, exchange_name: String) -> Self {
        Self {
            pool,
            channel,
            exchange_name,
        }
    }

    pub async fn run_loop(&self) -> Result<()> {
        let mut listener = PgListener::connect_with(&self.pool)
            .await
            .context("create indicator outbox PgListener")?;
        listener
            .listen(OUTBOX_NOTIFY_CHANNEL)
            .await
            .context("listen outbox_ready")?;

        if let Err(err) = self.ensure_partitions().await {
            warn!(
                error = %err,
                debug_error = ?err,
                "indicator outbox partition maintenance init failed"
            );
        }

        let mut next_housekeeping_at =
            Instant::now() + Duration::from_secs(OUTBOX_HOUSEKEEPING_INTERVAL_SECS);

        info!(
            exchange_name = %self.exchange_name,
            notify_channel = OUTBOX_NOTIFY_CHANNEL,
            notify_timeout_secs = OUTBOX_NOTIFY_TIMEOUT_SECS,
            batch_size = OUTBOX_DISPATCH_BATCH_SIZE,
            "indicator outbox dispatcher started"
        );

        loop {
            let notified = tokio::time::timeout(
                Duration::from_secs(OUTBOX_NOTIFY_TIMEOUT_SECS),
                listener.recv(),
            )
            .await;

            match notified {
                Ok(Ok(_)) => {}
                Ok(Err(err)) => {
                    warn!(
                        error = %err,
                        "indicator outbox listener recv error, will retry"
                    );
                }
                Err(_) => {}
            }

            for batch_idx in 0..OUTBOX_MAX_BATCHES_PER_WAKE {
                match self.dispatch_batch(OUTBOX_DISPATCH_BATCH_SIZE).await {
                    Ok(0) => break,
                    Ok(_) => {
                        if batch_idx + 1 == OUTBOX_MAX_BATCHES_PER_WAKE {
                            warn!(
                                exchange_name = %self.exchange_name,
                                max_batches_per_wake = OUTBOX_MAX_BATCHES_PER_WAKE,
                                "indicator outbox dispatcher hit per-wake drain cap"
                            );
                        }
                    }
                    Err(err) => {
                        warn!(
                            error = %err,
                            debug_error = ?err,
                            exchange_name = %self.exchange_name,
                            "indicator outbox dispatch batch failed"
                        );
                        break;
                    }
                }
            }

            if Instant::now() >= next_housekeeping_at {
                if let Err(err) = self.prune_dead_rows().await {
                    warn!(
                        error = %err,
                        debug_error = ?err,
                        "indicator outbox dead-row gc failed"
                    );
                }
                if let Err(err) = self.ensure_partitions().await {
                    warn!(
                        error = %err,
                        debug_error = ?err,
                        "indicator outbox partition maintenance failed"
                    );
                }
                next_housekeeping_at =
                    Instant::now() + Duration::from_secs(OUTBOX_HOUSEKEEPING_INTERVAL_SECS);
            }
        }
    }

    async fn dispatch_batch(&self, batch_size: i64) -> Result<usize> {
        let started_at = Instant::now();
        let claim_started_at = Instant::now();
        let rows = self.claim_batch(batch_size).await?;
        let claim_ms = claim_started_at.elapsed().as_millis();
        let row_count = rows.len();
        if row_count == 0 {
            return Ok(0);
        }
        let mut sent_keys: Vec<(NaiveDate, i64)> = Vec::with_capacity(rows.len());

        let publish_started_at = Instant::now();
        for row in rows {
            match self.publish_row(&row).await {
                Ok(confirm) => match confirm.await.context("wait publisher confirm")? {
                    Confirmation::Ack(_) => sent_keys.push((row.bucket_date, row.outbox_id)),
                    Confirmation::Nack(returned) => {
                        let err_text = format!(
                            "broker nack for outbox_id={} returned={}",
                            row.outbox_id,
                            returned.is_some()
                        );
                        error!(outbox_id = row.outbox_id, "indicator outbox broker nack");
                        self.mark_failed(row.bucket_date, row.outbox_id, err_text)
                            .await?;
                    }
                    Confirmation::NotRequested => {
                        self.mark_failed(
                            row.bucket_date,
                            row.outbox_id,
                            "publisher confirm not requested on channel".to_string(),
                        )
                        .await?;
                    }
                },
                Err(err) => {
                    error!(
                        error = %err,
                        outbox_id = row.outbox_id,
                        "indicator outbox publish failed"
                    );
                    self.mark_failed(row.bucket_date, row.outbox_id, err.to_string())
                        .await?;
                }
            }
        }
        let publish_and_confirm_ms = publish_started_at.elapsed().as_millis();

        let delete_started_at = Instant::now();
        self.delete_sent_batch(&sent_keys).await?;
        let delete_ms = delete_started_at.elapsed().as_millis();
        let total_ms = started_at.elapsed().as_millis();
        if claim_ms >= OUTBOX_CLAIM_WARN_MS || total_ms >= OUTBOX_DISPATCH_WARN_MS {
            warn!(
                exchange_name = %self.exchange_name,
                batch_size = batch_size,
                claimed_rows = row_count,
                sent_rows = sent_keys.len(),
                claim_ms = claim_ms,
                publish_and_confirm_ms = publish_and_confirm_ms,
                delete_ms = delete_ms,
                total_ms = total_ms,
                "slow indicator outbox dispatch batch"
            );
        }
        Ok(row_count)
    }

    async fn claim_batch(&self, batch_size: i64) -> Result<Vec<OutboxRow>> {
        if batch_size <= 0 {
            return Ok(Vec::new());
        }

        sqlx::query_as(
            r#"
            WITH picked AS (
                SELECT bucket_date, outbox_id
                FROM ops.outbox_event
                WHERE status IN ('pending', 'failed', 'sending')
                  AND available_at <= now()
                  AND exchange_name = $2
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
                o.exchange_name,
                o.routing_key,
                o.message_id,
                o.headers_json,
                o.payload_json
            "#,
        )
        .bind(batch_size)
        .bind(&self.exchange_name)
        .fetch_all(&self.pool)
        .await
        .with_context(|| {
            format!(
                "claim indicator outbox rows exchange={}",
                self.exchange_name
            )
        })
    }

    async fn publish_row(&self, row: &OutboxRow) -> Result<PublisherConfirm> {
        let payload = serde_json::to_vec(&row.payload_json).context("serialize outbox payload")?;
        let headers = json_to_field_table(&row.headers_json).context("build amqp headers")?;

        let properties = BasicProperties::default()
            .with_content_type(ShortString::from("application/json"))
            .with_delivery_mode(2)
            .with_headers(headers)
            .with_message_id(ShortString::from(row.message_id.to_string()));

        self.channel
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
            })
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
        .context("delete delivered indicator outbox rows")?;
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
                .context("mark indicator outbox failed (legacy schema)")?;
                Ok(())
            }
            Err(err) => Err(err).context("mark indicator outbox failed"),
        }
    }

    async fn prune_dead_rows(&self) -> Result<()> {
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
            .context("delete old dead indicator outbox rows")?;

            if result.rows_affected() < OUTBOX_GC_BATCH_SIZE as u64 {
                break;
            }
        }
        Ok(())
    }

    async fn ensure_partitions(&self) -> Result<()> {
        let result = sqlx::query("SELECT ops.ensure_outbox_event_partitions($1, $2)")
            .bind(OUTBOX_PARTITION_PRECREATE_DAYS_AHEAD)
            .bind(OUTBOX_PARTITION_PRECREATE_DAYS_BACK)
            .execute(&self.pool)
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(err) if is_undefined_function(&err, "ensure_outbox_event_partitions") => Ok(()),
            Err(err) => Err(err).context("ensure indicator outbox partitions"),
        }
    }
}

#[derive(Debug, Clone, FromRow)]
struct OutboxRow {
    bucket_date: chrono::NaiveDate,
    outbox_id: i64,
    exchange_name: String,
    routing_key: String,
    message_id: uuid::Uuid,
    headers_json: Value,
    payload_json: Value,
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
