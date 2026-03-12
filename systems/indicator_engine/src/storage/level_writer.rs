use crate::indicators::context::{IndicatorLevelRow, LiquidationLevelRow};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::PgPool;

#[derive(Clone)]
pub struct LevelWriter {
    pool: PgPool,
}

impl LevelWriter {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn write_indicator_levels(
        &self,
        ts_bucket: DateTime<Utc>,
        symbol: &str,
        rows: &[IndicatorLevelRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let n = rows.len();
        let ts_snapshots: Vec<DateTime<Utc>> = vec![ts_bucket; n];
        let symbols: Vec<&str> = vec![symbol; n];
        let indicator_codes: Vec<&str> = rows.iter().map(|r| r.indicator_code).collect();
        let window_codes: Vec<&str> = rows.iter().map(|r| r.window_code).collect();
        let price_levels: Vec<f64> = rows.iter().map(|r| r.price_level).collect();
        let level_ranks: Vec<Option<i32>> = rows.iter().map(|r| r.level_rank).collect();
        let metrics_jsons: Vec<&Value> = rows.iter().map(|r| &r.metrics_json).collect();

        // Single round-trip: batch all level rows with UNNEST.
        // Previously this was a per-row loop causing N separate index updates.
        sqlx::query(
            r#"
            INSERT INTO feat.indicator_level_value (
                ts_snapshot, symbol, indicator_code, window_code,
                market_scope, price_level, level_rank, metrics_json, calc_version
            )
            SELECT ts, sym, ic, wc, 'futures_primary', pl, lr, mj, 'indicator_engine.v1'
            FROM UNNEST(
                $1::timestamptz[],
                $2::text[],
                $3::text[],
                $4::text[],
                $5::float8[],
                $6::int4[],
                $7::jsonb[]
            ) AS t(ts, sym, ic, wc, pl, lr, mj)
            ON CONFLICT (
                ts_snapshot, venue, symbol, indicator_code, window_code, market_scope, price_level
            )
            DO UPDATE SET
                level_rank = EXCLUDED.level_rank,
                metrics_json = EXCLUDED.metrics_json,
                calc_version = EXCLUDED.calc_version
            "#,
        )
        .bind(&ts_snapshots)
        .bind(&symbols)
        .bind(&indicator_codes)
        .bind(&window_codes)
        .bind(&price_levels)
        .bind(&level_ranks)
        .bind(&metrics_jsons)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "batch insert indicator_level_value: {} rows, symbol={}",
                n, symbol
            )
        })?;

        Ok(())
    }

    pub async fn write_liquidation_levels(
        &self,
        ts_bucket: DateTime<Utc>,
        symbol: &str,
        rows: &[LiquidationLevelRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let n = rows.len();
        let ts_snapshots: Vec<DateTime<Utc>> = vec![ts_bucket; n];
        let window_codes: Vec<&str> = rows.iter().map(|r| r.window_code).collect();
        let symbols: Vec<&str> = vec![symbol; n];
        let price_levels: Vec<f64> = rows.iter().map(|r| r.price_level).collect();
        let long_liq_densities: Vec<f64> = rows.iter().map(|r| r.long_liq_density).collect();
        let short_liq_densities: Vec<f64> = rows.iter().map(|r| r.short_liq_density).collect();
        let net_liq_densities: Vec<f64> = rows.iter().map(|r| r.net_liq_density).collect();
        let is_long_peaks: Vec<bool> = rows.iter().map(|r| r.is_long_peak).collect();
        let is_short_peaks: Vec<bool> = rows.iter().map(|r| r.is_short_peak).collect();
        let long_peak_scores: Vec<Option<f64>> = rows.iter().map(|r| r.long_peak_score).collect();
        let short_peak_scores: Vec<Option<f64>> = rows.iter().map(|r| r.short_peak_score).collect();
        let extra_jsons: Vec<&Value> = rows.iter().map(|r| &r.extra_json).collect();

        sqlx::query(
            r#"
            INSERT INTO feat.liquidation_density_level (
                ts_snapshot, window_code, symbol, price_level,
                long_liq_density, short_liq_density, net_liq_density,
                is_long_peak, is_short_peak, long_peak_score, short_peak_score,
                calc_version, extra_json
            )
            SELECT ts, wc, sym, pl, lld, sld, nld, ilp, isp, lps, sps,
                   'indicator_engine.v1', ej
            FROM UNNEST(
                $1::timestamptz[],
                $2::text[],
                $3::text[],
                $4::float8[],
                $5::float8[],
                $6::float8[],
                $7::float8[],
                $8::bool[],
                $9::bool[],
                $10::float8[],
                $11::float8[],
                $12::jsonb[]
            ) AS t(ts, wc, sym, pl, lld, sld, nld, ilp, isp, lps, sps, ej)
            ON CONFLICT (ts_snapshot, venue, symbol, window_code, price_level)
            DO UPDATE SET
                long_liq_density = EXCLUDED.long_liq_density,
                short_liq_density = EXCLUDED.short_liq_density,
                net_liq_density = EXCLUDED.net_liq_density,
                is_long_peak = EXCLUDED.is_long_peak,
                is_short_peak = EXCLUDED.is_short_peak,
                long_peak_score = EXCLUDED.long_peak_score,
                short_peak_score = EXCLUDED.short_peak_score,
                calc_version = EXCLUDED.calc_version,
                extra_json = EXCLUDED.extra_json
            "#,
        )
        .bind(&ts_snapshots)
        .bind(&window_codes)
        .bind(&symbols)
        .bind(&price_levels)
        .bind(&long_liq_densities)
        .bind(&short_liq_densities)
        .bind(&net_liq_densities)
        .bind(&is_long_peaks)
        .bind(&is_short_peaks)
        .bind(&long_peak_scores)
        .bind(&short_peak_scores)
        .bind(&extra_jsons)
        .execute(&self.pool)
        .await
        .context("batch insert liquidation_density_level")?;

        Ok(())
    }
}
