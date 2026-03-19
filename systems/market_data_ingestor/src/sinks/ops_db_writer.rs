use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

const CHECKPOINT_MIN_UPSERT_INTERVAL_MS: i64 = 10_000;
const CHECKPOINT_RECV_HEARTBEAT_SECS: i64 = 30;
const DATA_GAP_DEDUP_WINDOW_SECS: i64 = 300;
const DATA_GAP_LOCAL_CACHE_MAX: usize = 20_000;

#[derive(Debug, Clone)]
pub struct CheckpointUpsert {
    pub market: String,
    pub symbol: String,
    pub stream_name: String,
    pub source_kind: String,
    pub last_event_ts: DateTime<Utc>,
    pub last_recv_ts: DateTime<Utc>,
    pub last_update_id: Option<i64>,
    pub last_trade_id: Option<i64>,
    pub last_agg_trade_id: Option<i64>,
    pub checkpoint_json: Value,
}

#[derive(Clone)]
pub struct OpsDbWriter {
    pool: PgPool,
    checkpoint_last_upsert_ms: Arc<Mutex<HashMap<String, i64>>>,
    data_gap_last_insert_ms: Arc<Mutex<HashMap<String, i64>>>,
}

#[derive(Debug, Clone)]
pub struct HeartbeatPayload {
    pub role: String,
    pub last_processed_event_ts: Option<DateTime<Utc>>,
    pub event_lag_ms: Option<i64>,
    pub queue_lag: Option<i64>,
    pub error_rate: Option<f64>,
    pub backfill_mode: bool,
}

impl OpsDbWriter {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            checkpoint_last_upsert_ms: Arc::new(Mutex::new(HashMap::new())),
            data_gap_last_insert_ms: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn upsert_heartbeat(
        &self,
        service_name: &str,
        instance_id: &str,
        status: &str,
        payload: HeartbeatPayload,
    ) -> Result<()> {
        let role = payload.role.clone();
        let metrics_json = json!({
            "role": role,
            "instance_id": instance_id,
            "last_processed_event_ts": payload.last_processed_event_ts.map(|ts| ts.to_rfc3339()),
            "event_lag_ms": payload.event_lag_ms,
            "queue_lag": payload.queue_lag,
            "error_rate": payload.error_rate,
            "backfill_mode": payload.backfill_mode,
        });

        sqlx::query(
            r#"
            INSERT INTO ops.service_heartbeat (
                service_name, instance_id, role, ts_heartbeat, status, metrics_json
            )
            VALUES ($1, $2, $3, now(), $4, $5)
            ON CONFLICT (service_name, instance_id)
            DO UPDATE SET
                role = EXCLUDED.role,
                ts_heartbeat = EXCLUDED.ts_heartbeat,
                status = EXCLUDED.status,
                metrics_json = EXCLUDED.metrics_json
            "#,
        )
        .bind(service_name)
        .bind(instance_id)
        .bind(role)
        .bind(status)
        .bind(metrics_json)
        .execute(&self.pool)
        .await
        .context("upsert ops.service_heartbeat")?;

        Ok(())
    }

    pub async fn upsert_checkpoint(
        &self,
        market: &str,
        symbol: &str,
        stream_name: &str,
        source_kind: &str,
        last_event_ts: DateTime<Utc>,
        last_recv_ts: DateTime<Utc>,
        last_update_id: Option<i64>,
        last_trade_id: Option<i64>,
        last_agg_trade_id: Option<i64>,
        checkpoint_json: Value,
    ) -> Result<()> {
        let one = CheckpointUpsert {
            market: market.to_string(),
            symbol: symbol.to_string(),
            stream_name: stream_name.to_string(),
            source_kind: source_kind.to_string(),
            last_event_ts,
            last_recv_ts,
            last_update_id,
            last_trade_id,
            last_agg_trade_id,
            checkpoint_json,
        };
        self.upsert_checkpoints_batch(std::slice::from_ref(&one))
            .await
    }

    pub async fn upsert_checkpoints_batch(&self, checkpoints: &[CheckpointUpsert]) -> Result<()> {
        if checkpoints.is_empty() {
            return Ok(());
        }

        let now_ms = Utc::now().timestamp_millis();
        let mut filtered: Vec<&CheckpointUpsert> = Vec::with_capacity(checkpoints.len());
        for cp in checkpoints {
            if self
                .should_skip_checkpoint_upsert_at(
                    &cp.market,
                    &cp.symbol,
                    &cp.stream_name,
                    &cp.source_kind,
                    now_ms,
                )
                .await
            {
                continue;
            }
            filtered.push(cp);
        }

        if filtered.is_empty() {
            return Ok(());
        }

        let mut markets: Vec<String> = Vec::with_capacity(filtered.len());
        let mut symbols: Vec<String> = Vec::with_capacity(filtered.len());
        let mut stream_names: Vec<String> = Vec::with_capacity(filtered.len());
        let mut source_kinds: Vec<String> = Vec::with_capacity(filtered.len());
        let mut last_event_tss: Vec<DateTime<Utc>> = Vec::with_capacity(filtered.len());
        let mut last_recv_tss: Vec<DateTime<Utc>> = Vec::with_capacity(filtered.len());
        let mut last_update_ids: Vec<Option<i64>> = Vec::with_capacity(filtered.len());
        let mut last_trade_ids: Vec<Option<i64>> = Vec::with_capacity(filtered.len());
        let mut last_agg_trade_ids: Vec<Option<i64>> = Vec::with_capacity(filtered.len());
        let mut checkpoint_jsons: Vec<Value> = Vec::with_capacity(filtered.len());

        for cp in filtered {
            markets.push(cp.market.clone());
            symbols.push(cp.symbol.clone());
            stream_names.push(cp.stream_name.clone());
            source_kinds.push(cp.source_kind.clone());
            last_event_tss.push(cp.last_event_ts);
            last_recv_tss.push(cp.last_recv_ts);
            last_update_ids.push(cp.last_update_id);
            last_trade_ids.push(cp.last_trade_id);
            last_agg_trade_ids.push(cp.last_agg_trade_id);
            checkpoint_jsons.push(cp.checkpoint_json.clone());
        }

        sqlx::query(
            r#"
            INSERT INTO ops.ingest_checkpoint (
                venue, market, symbol, stream_name, source_kind,
                last_event_ts, last_recv_ts, last_update_id, last_trade_id, last_agg_trade_id,
                checkpoint_json, updated_at
            )
            SELECT
                'binance',
                market::cfg.market_type,
                symbol,
                stream_name,
                source_kind::cfg.source_type,
                last_event_ts,
                last_recv_ts,
                last_update_id,
                last_trade_id,
                last_agg_trade_id,
                checkpoint_json,
                now()
            FROM UNNEST(
                $1::text[],
                $2::text[],
                $3::text[],
                $4::text[],
                $5::timestamptz[],
                $6::timestamptz[],
                $7::bigint[],
                $8::bigint[],
                $9::bigint[],
                $10::jsonb[]
            ) AS t(
                market,
                symbol,
                stream_name,
                source_kind,
                last_event_ts,
                last_recv_ts,
                last_update_id,
                last_trade_id,
                last_agg_trade_id,
                checkpoint_json
            )
            ON CONFLICT (venue, market, symbol, stream_name, source_kind)
            DO UPDATE SET
                last_event_ts = EXCLUDED.last_event_ts,
                last_recv_ts = EXCLUDED.last_recv_ts,
                last_update_id = EXCLUDED.last_update_id,
                last_trade_id = EXCLUDED.last_trade_id,
                last_agg_trade_id = EXCLUDED.last_agg_trade_id,
                checkpoint_json = EXCLUDED.checkpoint_json,
                updated_at = now()
            WHERE
                ops.ingest_checkpoint.last_event_ts IS DISTINCT FROM EXCLUDED.last_event_ts
                OR ops.ingest_checkpoint.last_update_id IS DISTINCT FROM EXCLUDED.last_update_id
                OR ops.ingest_checkpoint.last_trade_id IS DISTINCT FROM EXCLUDED.last_trade_id
                OR ops.ingest_checkpoint.last_agg_trade_id IS DISTINCT FROM EXCLUDED.last_agg_trade_id
                OR ops.ingest_checkpoint.checkpoint_json IS DISTINCT FROM EXCLUDED.checkpoint_json
                OR ops.ingest_checkpoint.last_recv_ts IS NULL
                OR ops.ingest_checkpoint.last_recv_ts
                    < (EXCLUDED.last_recv_ts - ($11::bigint * INTERVAL '1 second'))
            "#,
        )
        .bind(&markets)
        .bind(&symbols)
        .bind(&stream_names)
        .bind(&source_kinds)
        .bind(&last_event_tss)
        .bind(&last_recv_tss)
        .bind(&last_update_ids)
        .bind(&last_trade_ids)
        .bind(&last_agg_trade_ids)
        .bind(&checkpoint_jsons)
        .bind(CHECKPOINT_RECV_HEARTBEAT_SECS)
        .execute(&self.pool)
        .await
        .context("upsert ops.ingest_checkpoint batch")?;

        Ok(())
    }

    async fn should_skip_checkpoint_upsert_at(
        &self,
        market: &str,
        symbol: &str,
        stream_name: &str,
        source_kind: &str,
        now_ms: i64,
    ) -> bool {
        // WS checkpoint is high-frequency; throttle to reduce write amplification.
        if source_kind != "ws" {
            return false;
        }

        let key = format!("{market}|{symbol}|{stream_name}|{source_kind}");
        let mut guard = self.checkpoint_last_upsert_ms.lock().await;

        if let Some(last_ms) = guard.get(&key).copied() {
            if now_ms - last_ms < CHECKPOINT_MIN_UPSERT_INTERVAL_MS {
                return true;
            }
        }

        guard.insert(key, now_ms);
        false
    }

    async fn should_skip_data_gap_insert_at(
        &self,
        market: &str,
        symbol: &str,
        dataset_name: &str,
        stream_name: &str,
        cause: &str,
        now_ms: i64,
    ) -> bool {
        let key = format!("{market}|{symbol}|{dataset_name}|{stream_name}|{cause}");
        let mut guard = self.data_gap_last_insert_ms.lock().await;
        let window_ms = DATA_GAP_DEDUP_WINDOW_SECS * 1_000;

        if let Some(last_ms) = guard.get(&key).copied() {
            if now_ms - last_ms < window_ms {
                return true;
            }
        }

        guard.insert(key, now_ms);

        // Avoid unbounded growth under prolonged gap storms.
        if guard.len() > DATA_GAP_LOCAL_CACHE_MAX {
            let cutoff_ms = now_ms - (window_ms * 2);
            guard.retain(|_, ts| *ts >= cutoff_ms);
        }

        false
    }

    pub async fn insert_data_gap(
        &self,
        market: &str,
        symbol: &str,
        dataset_name: &str,
        stream_name: &str,
        gap_start_ts: DateTime<Utc>,
        gap_end_ts: DateTime<Utc>,
        cause: &str,
        detail_json: Value,
    ) -> Result<()> {
        let now_ms = Utc::now().timestamp_millis();
        if self
            .should_skip_data_gap_insert_at(
                market,
                symbol,
                dataset_name,
                stream_name,
                cause,
                now_ms,
            )
            .await
        {
            return Ok(());
        }

        // In-memory dedup (should_skip_data_gap_insert_at) is the sole guard; no DB-side
        // NOT EXISTS scan needed. Simple INSERT reduces per-gap DB cost to a single row write.
        sqlx::query(
            r#"
            INSERT INTO ops.data_gap (
                venue, market, symbol, dataset_name, stream_name,
                gap_start_ts, gap_end_ts, status, cause, detail_json
            )
            VALUES ('binance', $1::cfg.market_type, $2, $3, $4, $5, $6, 'open', $7, $8)
            "#,
        )
        .bind(market)
        .bind(symbol)
        .bind(dataset_name)
        .bind(stream_name)
        .bind(gap_start_ts)
        .bind(gap_end_ts)
        .bind(cause)
        .bind(detail_json)
        .execute(&self.pool)
        .await
        .context("insert ops.data_gap")?;

        Ok(())
    }

    pub async fn insert_backfill_job_run(
        &self,
        market: Option<&str>,
        symbol: Option<&str>,
        endpoint_path: &str,
        purpose: &str,
        trigger_type: &str,
        request_params: Value,
        rows_written: Option<i64>,
        status: &str,
        error_message: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO ops.rest_backfill_job_run (
                venue, market, symbol, endpoint_path, purpose, trigger_type,
                request_params, started_at, ended_at, status, rows_written, error_message
            )
            VALUES (
                'binance',
                CASE WHEN $1 IS NULL THEN NULL ELSE $1::cfg.market_type END,
                $2,
                $3, $4, $5,
                $6, now(), now(), $7::cfg.job_status, $8, $9
            )
            "#,
        )
        .bind(market)
        .bind(symbol)
        .bind(endpoint_path)
        .bind(purpose)
        .bind(trigger_type)
        .bind(request_params)
        .bind(status)
        .bind(rows_written)
        .bind(error_message)
        .execute(&self.pool)
        .await
        .context("insert ops.rest_backfill_job_run")?;

        Ok(())
    }

    pub async fn upsert_instrument_metadata(
        &self,
        market: &str,
        symbol: &str,
        contract_type: Option<&str>,
        quote_asset: Option<&str>,
        base_asset: Option<&str>,
        price_scale: Option<i32>,
        qty_scale: Option<i32>,
        is_active: bool,
        metadata: Value,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO cfg.instrument (
                venue, symbol, market, contract_type,
                quote_asset, base_asset, price_scale, qty_scale, is_active,
                metadata, updated_at
            )
            VALUES (
                'binance', $1, $2::cfg.market_type, $3,
                COALESCE($4, ''), COALESCE($5, ''),
                $6, $7, $8,
                $9, now()
            )
            ON CONFLICT (venue, symbol, market)
            DO UPDATE SET
                contract_type = EXCLUDED.contract_type,
                quote_asset = EXCLUDED.quote_asset,
                base_asset = EXCLUDED.base_asset,
                price_scale = EXCLUDED.price_scale,
                qty_scale = EXCLUDED.qty_scale,
                is_active = EXCLUDED.is_active,
                metadata = EXCLUDED.metadata,
                updated_at = now()
            "#,
        )
        .bind(symbol)
        .bind(market)
        .bind(contract_type)
        .bind(quote_asset)
        .bind(base_asset)
        .bind(price_scale)
        .bind(qty_scale)
        .bind(is_active)
        .bind(metadata)
        .execute(&self.pool)
        .await
        .context("upsert cfg.instrument metadata")?;

        Ok(())
    }
}
