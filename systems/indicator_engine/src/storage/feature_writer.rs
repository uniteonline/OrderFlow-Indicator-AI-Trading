use crate::indicators::context::{zscore, IndicatorContext};
use crate::ingest::decoder::MarketKind;
use crate::runtime::state_store::{MinuteHistory, WhaleStats};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use serde_json::json;
use sqlx::PgPool;

const CVD_BURST_EMA_FAST: usize = 5;
const CVD_BURST_EMA_SLOW: usize = 20;
const CVD_BURST_Z_LOOKBACK: usize = 60;
const VPIN_BUCKET_BASE: f64 = 50.0;

#[derive(Clone)]
pub struct FeatureWriter {
    pool: PgPool,
}

#[derive(Debug, Clone)]
struct WindowAgg {
    rows: Vec<MinuteHistory>,
    open_price: Option<f64>,
    high_price: Option<f64>,
    low_price: Option<f64>,
    close_price: Option<f64>,
    trade_count: i64,
    buy_qty: f64,
    sell_qty: f64,
    total_qty: f64,
    total_notional: f64,
    delta: f64,
    relative_delta: f64,
    cvd_last: f64,
    cvd_slope: Option<f64>,
    vpin_last: Option<f64>,
    avwap: Option<f64>,
    ofi_sum: f64,
    obi_twa: Option<f64>,
    whale: WhaleStats,
}

#[derive(Debug, Clone)]
struct FundingWindowMetrics {
    funding_current: Option<f64>,
    funding_current_effective_ts: Option<DateTime<Utc>>,
    funding_twa: Option<f64>,
    mark_price_last: Option<f64>,
    mark_price_last_ts: Option<DateTime<Utc>>,
    mark_price_twap: Option<f64>,
    index_price_last: Option<f64>,
    changes_json: serde_json::Value,
}

impl FeatureWriter {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn write_all(&self, ctx: &IndicatorContext) -> Result<()> {
        let mut windows = ctx.enabled_windows();
        if !windows.iter().any(|(_, m)| *m == 1) {
            windows.push(("1m".to_string(), 1));
        }
        windows.sort_by_key(|(_, mins)| *mins);
        windows.dedup_by(|a, b| a.0 == b.0);

        for (window_code, mins) in &windows {
            self.insert_trade_flow_for_market_window(
                &ctx.symbol,
                &ctx.history_futures,
                MarketKind::Futures,
                ctx.ts_bucket,
                *mins,
            )
            .await?;
            self.insert_trade_flow_for_market_window(
                &ctx.symbol,
                &ctx.history_spot,
                MarketKind::Spot,
                ctx.ts_bucket,
                *mins,
            )
            .await?;

            self.insert_cvd_pack_window(ctx, window_code, *mins).await?;
            self.insert_avwap_feature_window(ctx, *mins).await?;
            self.insert_funding_feature_window(ctx, window_code, *mins)
                .await?;

            self.insert_whale_rollup_window(
                &ctx.symbol,
                &ctx.history_futures,
                MarketKind::Futures,
                ctx.whale_threshold_usdt,
                ctx.ts_bucket,
                *mins,
            )
            .await?;
            self.insert_whale_rollup_window(
                &ctx.symbol,
                &ctx.history_spot,
                MarketKind::Spot,
                ctx.whale_threshold_usdt,
                ctx.ts_bucket,
                *mins,
            )
            .await?;
        }

        self.insert_orderbook_feature(ctx).await?;
        self.insert_funding_changes(ctx).await?;
        Ok(())
    }

    async fn insert_trade_flow_for_market_window(
        &self,
        symbol: &str,
        history: &[MinuteHistory],
        market: MarketKind,
        ts_bucket: DateTime<Utc>,
        mins: i64,
    ) -> Result<()> {
        let Some(agg) = aggregate_window(history, ts_bucket, mins) else {
            return Ok(());
        };

        let cvd_rolling_7d = rolling_cvd_7d(history, ts_bucket).unwrap_or(agg.cvd_last);
        let ema_fast = ema(
            &agg.rows.iter().map(|h| h.delta).collect::<Vec<_>>(),
            CVD_BURST_EMA_FAST,
        );
        let ema_slow = ema(
            &agg.rows.iter().map(|h| h.delta).collect::<Vec<_>>(),
            CVD_BURST_EMA_SLOW,
        );
        let cvd_burst_signal = ema_fast.zip(ema_slow).map(|(f, s)| f - s);
        let cvd_burst_z = burst_zscore(
            &agg.rows.iter().map(|h| h.delta).collect::<Vec<_>>(),
            CVD_BURST_EMA_FAST,
            CVD_BURST_EMA_SLOW,
            CVD_BURST_Z_LOOKBACK,
        );
        let vpin_bucket_fill = Some((agg.total_qty / VPIN_BUCKET_BASE).clamp(0.0, 1.0));
        let bar_interval = interval_text(mins);
        let anchor_ts = Some(ts_bucket - Duration::days(7));

        sqlx::query(
            r#"
            INSERT INTO feat.trade_flow_feature (
                ts_bucket, bar_interval, market, symbol,
                trade_count, buy_qty, sell_qty, total_qty,
                delta, relative_delta,
                cvd, cvd_rolling_7d, cvd_slope_ols,
                ema_delta_fast, ema_delta_slow, cvd_burst_signal, cvd_burst_z,
                vpin, vpin_bucket_fill, avwap_value, avwap_anchor_ts,
                calc_version, extra_json
            )
            VALUES (
                $1, $2::interval, $3::cfg.market_type, $4,
                $5, $6, $7, $8,
                $9, $10,
                $11, $12, $13,
                $14, $15, $16, $17,
                $18, $19, $20, $21,
                'indicator_engine.v1', $22
            )
            ON CONFLICT (venue, market, symbol, bar_interval, ts_bucket)
            DO UPDATE SET
                trade_count = EXCLUDED.trade_count,
                buy_qty = EXCLUDED.buy_qty,
                sell_qty = EXCLUDED.sell_qty,
                total_qty = EXCLUDED.total_qty,
                delta = EXCLUDED.delta,
                relative_delta = EXCLUDED.relative_delta,
                cvd = EXCLUDED.cvd,
                cvd_rolling_7d = EXCLUDED.cvd_rolling_7d,
                cvd_slope_ols = EXCLUDED.cvd_slope_ols,
                ema_delta_fast = EXCLUDED.ema_delta_fast,
                ema_delta_slow = EXCLUDED.ema_delta_slow,
                cvd_burst_signal = EXCLUDED.cvd_burst_signal,
                cvd_burst_z = EXCLUDED.cvd_burst_z,
                vpin = EXCLUDED.vpin,
                vpin_bucket_fill = EXCLUDED.vpin_bucket_fill,
                avwap_value = EXCLUDED.avwap_value,
                avwap_anchor_ts = EXCLUDED.avwap_anchor_ts,
                calc_version = EXCLUDED.calc_version,
                extra_json = EXCLUDED.extra_json
            "#,
        )
        .bind(ts_bucket)
        .bind(bar_interval)
        .bind(market.as_str())
        .bind(symbol)
        .bind(agg.trade_count)
        .bind(agg.buy_qty)
        .bind(agg.sell_qty)
        .bind(agg.total_qty)
        .bind(agg.delta)
        .bind(agg.relative_delta)
        .bind(agg.cvd_last)
        .bind(cvd_rolling_7d)
        .bind(agg.cvd_slope)
        .bind(ema_fast)
        .bind(ema_slow)
        .bind(cvd_burst_signal)
        .bind(cvd_burst_z)
        .bind(agg.vpin_last)
        .bind(vpin_bucket_fill)
        .bind(agg.avwap)
        .bind(anchor_ts)
        .bind(json!({ "window_minutes": mins }))
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "insert trade_flow_feature market={} mins={}",
                market.as_str(),
                mins
            )
        })?;

        Ok(())
    }

    async fn insert_orderbook_feature(&self, ctx: &IndicatorContext) -> Result<()> {
        let spot_confirm = ctx
            .spot
            .obi_twa
            .zip(ctx.futures.obi_twa)
            .map(|(s, f)| s.signum() == f.signum())
            .unwrap_or(false);

        let fake_order_risk = (ctx.futures.obi_twa.unwrap_or(0.0).abs()
            * (1.0 - ctx.futures.relative_delta.abs()))
        .max(0.0);
        let exec_confirm =
            ctx.futures.delta.signum() == ctx.futures.obi_twa.unwrap_or(0.0).signum();

        let fut_obi = ctx.futures.obi_k_twa.or(ctx.futures.obi_twa);
        let spot_obi = ctx.spot.obi_k_dw_twa.or(ctx.spot.obi_twa);
        let fut_ofi_norm = ctx
            .history_futures
            .iter()
            .rev()
            .skip(1)
            .take(60)
            .map(|h| h.ofi)
            .collect::<Vec<_>>();
        let spot_ofi_norm_hist = ctx
            .history_spot
            .iter()
            .rev()
            .skip(1)
            .take(60)
            .map(|h| h.ofi)
            .collect::<Vec<_>>();
        let fut_ofi_norm = zscore(ctx.futures.ofi, fut_ofi_norm);
        let spot_ofi_norm = zscore(ctx.spot.ofi, spot_ofi_norm_hist);
        let spot_cvd_slope = ols_slope(&ctx.history_spot.iter().map(|h| h.cvd).collect::<Vec<_>>());

        let obi_change = ctx.futures.obi_k_dw_change;
        let obi_slope = ols_slope(
            &ctx.history_futures
                .iter()
                .filter_map(|h| h.obi_k_dw_twa)
                .collect::<Vec<_>>(),
        );

        let prev_close = ctx
            .history_futures
            .iter()
            .rev()
            .skip(1)
            .find_map(|h| h.close_price.or(h.last_price));
        let weak_price_resp = prev_close
            .zip(ctx.futures.last_price)
            .map(|(p, c)| {
                (c - p).abs() <= 0.02 && ctx.futures.obi_k_dw_twa.unwrap_or(0.0).abs() >= 0.4
            })
            .unwrap_or(false);

        let spot_driven_divergence_flag = ctx.spot.delta.abs() > (ctx.futures.delta.abs() * 1.2)
            && ctx.spot.delta.signum() != ctx.futures.delta.signum();

        sqlx::query(
            r#"
            INSERT INTO feat.orderbook_feature (
                ts_bucket, bar_interval, symbol,
                spread_twa_fut, topk_depth_twa_fut,
                obi_fut, obi_l1_twa_fut, obi_k_twa_fut, obi_k_dw_twa_fut, obi_k_dw_close_fut, obi_k_dw_change_fut, obi_k_dw_slope_fut, obi_k_dw_adj_twa_fut,
                ofi_fut, ofi_norm_fut,
                microprice_classic_fut, microprice_kappa_fut, microprice_adj_fut,
                spread_twa_spot, topk_depth_twa_spot,
                obi_k_dw_twa_spot, ofi_spot, ofi_norm_spot,
                trade_delta_spot, relative_delta_spot, cvd_slope_spot,
                exec_confirm_fut, spot_confirm,
                obi_shock_fut, weak_price_resp_fut, fake_order_risk_fut, spot_driven_divergence_flag,
                cross_cvd_attribution,
                calc_version, extra_json
            )
            VALUES (
                $1, INTERVAL '1 minute', $2,
                $3, $4,
                $5, $6, $7, $8, $9, $10, $11, $12,
                $13, $14,
                $15, $16, $17,
                $18, $19,
                $20, $21, $22,
                $23, $24, $25,
                $26, $27,
                $28, $29, $30, $31,
                $32,
                'indicator_engine.v1', $33
            )
            ON CONFLICT (venue, symbol, bar_interval, ts_bucket)
            DO UPDATE SET
                spread_twa_fut = EXCLUDED.spread_twa_fut,
                topk_depth_twa_fut = EXCLUDED.topk_depth_twa_fut,
                obi_fut = EXCLUDED.obi_fut,
                obi_l1_twa_fut = EXCLUDED.obi_l1_twa_fut,
                obi_k_twa_fut = EXCLUDED.obi_k_twa_fut,
                obi_k_dw_twa_fut = EXCLUDED.obi_k_dw_twa_fut,
                obi_k_dw_close_fut = EXCLUDED.obi_k_dw_close_fut,
                obi_k_dw_change_fut = EXCLUDED.obi_k_dw_change_fut,
                obi_k_dw_slope_fut = EXCLUDED.obi_k_dw_slope_fut,
                obi_k_dw_adj_twa_fut = EXCLUDED.obi_k_dw_adj_twa_fut,
                ofi_fut = EXCLUDED.ofi_fut,
                ofi_norm_fut = EXCLUDED.ofi_norm_fut,
                microprice_classic_fut = EXCLUDED.microprice_classic_fut,
                microprice_kappa_fut = EXCLUDED.microprice_kappa_fut,
                microprice_adj_fut = EXCLUDED.microprice_adj_fut,
                spread_twa_spot = EXCLUDED.spread_twa_spot,
                topk_depth_twa_spot = EXCLUDED.topk_depth_twa_spot,
                obi_k_dw_twa_spot = EXCLUDED.obi_k_dw_twa_spot,
                ofi_spot = EXCLUDED.ofi_spot,
                ofi_norm_spot = EXCLUDED.ofi_norm_spot,
                trade_delta_spot = EXCLUDED.trade_delta_spot,
                relative_delta_spot = EXCLUDED.relative_delta_spot,
                cvd_slope_spot = EXCLUDED.cvd_slope_spot,
                exec_confirm_fut = EXCLUDED.exec_confirm_fut,
                spot_confirm = EXCLUDED.spot_confirm,
                obi_shock_fut = EXCLUDED.obi_shock_fut,
                weak_price_resp_fut = EXCLUDED.weak_price_resp_fut,
                fake_order_risk_fut = EXCLUDED.fake_order_risk_fut,
                spot_driven_divergence_flag = EXCLUDED.spot_driven_divergence_flag,
                cross_cvd_attribution = EXCLUDED.cross_cvd_attribution,
                calc_version = EXCLUDED.calc_version,
                extra_json = EXCLUDED.extra_json
            "#,
        )
        .bind(ctx.ts_bucket)
        .bind(&ctx.symbol)
        .bind(ctx.futures.spread_twa)
        .bind(ctx.futures.topk_depth_twa)
        .bind(fut_obi)
        .bind(ctx.futures.obi_l1_twa)
        .bind(ctx.futures.obi_k_twa.or(ctx.futures.obi_twa))
        .bind(ctx.futures.obi_k_dw_twa)
        .bind(ctx.futures.obi_k_dw_close)
        .bind(obi_change)
        .bind(obi_slope)
        .bind(ctx.futures.obi_k_dw_adj_twa)
        .bind(ctx.futures.ofi)
        .bind(fut_ofi_norm)
        .bind(ctx.futures.microprice_classic_twa)
        .bind(ctx.futures.microprice_kappa_twa)
        .bind(ctx.futures.microprice_adj_twa)
        .bind(ctx.spot.spread_twa)
        .bind(ctx.spot.topk_depth_twa)
        .bind(spot_obi)
        .bind(ctx.spot.ofi)
        .bind(spot_ofi_norm)
        .bind(ctx.spot.delta)
        .bind(ctx.spot.relative_delta)
        .bind(spot_cvd_slope)
        .bind(exec_confirm)
        .bind(spot_confirm)
        .bind(obi_change.map(f64::abs))
        .bind(weak_price_resp)
        .bind(fake_order_risk)
        .bind(spot_driven_divergence_flag)
        .bind(ctx.spot.delta - ctx.futures.delta)
        .bind(json!({}))
        .execute(&self.pool)
        .await
        .context("insert orderbook_feature")?;

        Ok(())
    }
    async fn insert_cvd_pack_window(
        &self,
        ctx: &IndicatorContext,
        window_code: &str,
        mins: i64,
    ) -> Result<()> {
        let Some(fut) = aggregate_window(&ctx.history_futures, ctx.ts_bucket, mins) else {
            return Ok(());
        };
        let Some(spot) = aggregate_window(&ctx.history_spot, ctx.ts_bucket, mins) else {
            return Ok(());
        };

        let fut_cvd_7d =
            rolling_cvd_7d(&ctx.history_futures, ctx.ts_bucket).unwrap_or(fut.cvd_last);
        let spot_cvd_7d = rolling_cvd_7d(&ctx.history_spot, ctx.ts_bucket).unwrap_or(spot.cvd_last);
        let fut_delta_slant = delta_slant(fut.delta, fut.high_price, fut.low_price);
        let spot_delta_slant = delta_slant(spot.delta, spot.high_price, spot.low_price);
        let spot_lead_score = spot.delta.abs() / (spot.delta.abs() + fut.delta.abs() + 1e-12);
        let likely_driver = if spot_lead_score >= 0.6 {
            "spot"
        } else if spot_lead_score < 0.3 {
            "futures"
        } else {
            "mixed"
        };

        sqlx::query(
            r#"
            INSERT INTO feat.cvd_pack (
                ts_bucket, window_code, symbol,
                delta_fut, relative_delta_fut, cvd_fut, cvd_rolling_7d_fut, cvd_slope_fut, delta_slant_fut,
                delta_spot, relative_delta_spot, cvd_spot, cvd_rolling_7d_spot, cvd_slope_spot, delta_slant_spot,
                cvd_diff_fs, cvd_slope_diff_fs, spot_lead_score, likely_driver,
                calc_version, extra_json
            )
            VALUES (
                $1, $2, $3,
                $4, $5, $6, $7, $8, $9,
                $10, $11, $12, $13, $14, $15,
                $16, $17, $18, $19,
                'indicator_engine.v1', $20
            )
            ON CONFLICT (symbol, window_code, ts_bucket)
            DO UPDATE SET
                delta_fut = EXCLUDED.delta_fut,
                relative_delta_fut = EXCLUDED.relative_delta_fut,
                cvd_fut = EXCLUDED.cvd_fut,
                cvd_rolling_7d_fut = EXCLUDED.cvd_rolling_7d_fut,
                cvd_slope_fut = EXCLUDED.cvd_slope_fut,
                delta_slant_fut = EXCLUDED.delta_slant_fut,
                delta_spot = EXCLUDED.delta_spot,
                relative_delta_spot = EXCLUDED.relative_delta_spot,
                cvd_spot = EXCLUDED.cvd_spot,
                cvd_rolling_7d_spot = EXCLUDED.cvd_rolling_7d_spot,
                cvd_slope_spot = EXCLUDED.cvd_slope_spot,
                delta_slant_spot = EXCLUDED.delta_slant_spot,
                cvd_diff_fs = EXCLUDED.cvd_diff_fs,
                cvd_slope_diff_fs = EXCLUDED.cvd_slope_diff_fs,
                spot_lead_score = EXCLUDED.spot_lead_score,
                likely_driver = EXCLUDED.likely_driver,
                extra_json = EXCLUDED.extra_json,
                calc_version = EXCLUDED.calc_version
            "#,
        )
        .bind(ctx.ts_bucket)
        .bind(window_code)
        .bind(&ctx.symbol)
        .bind(fut.delta)
        .bind(fut.relative_delta)
        .bind(fut.cvd_last)
        .bind(fut_cvd_7d)
        .bind(fut.cvd_slope)
        .bind(fut_delta_slant)
        .bind(spot.delta)
        .bind(spot.relative_delta)
        .bind(spot.cvd_last)
        .bind(spot_cvd_7d)
        .bind(spot.cvd_slope)
        .bind(spot_delta_slant)
        .bind(fut.cvd_last - spot.cvd_last)
        .bind(fut.cvd_slope.unwrap_or(0.0) - spot.cvd_slope.unwrap_or(0.0))
        .bind(spot_lead_score)
        .bind(likely_driver)
        .bind(json!({ "window_minutes": mins }))
        .execute(&self.pool)
        .await
        .with_context(|| format!("insert cvd_pack {}", window_code))?;

        Ok(())
    }

    async fn insert_avwap_feature_window(&self, ctx: &IndicatorContext, mins: i64) -> Result<()> {
        let (fut_avwap, spot_avwap) = cumulative_avwap_7d(ctx, ctx.ts_bucket);
        let fut_last = ctx.futures.last_price;
        let fut_mark = ctx.latest_mark.as_ref().and_then(|m| m.mark_price);

        let price_minus_fut = fut_last.zip(fut_avwap).map(|(p, a)| p - a);
        let price_minus_spot = fut_last.zip(spot_avwap).map(|(p, a)| p - a);
        let mark_minus_spot = fut_mark.zip(spot_avwap).map(|(p, a)| p - a);
        let avwap_gap = fut_avwap.zip(spot_avwap).map(|(f, s)| f - s);
        let anchor_ts = Some(ctx.ts_bucket - Duration::days(7));
        let bar_interval = interval_text(mins);

        sqlx::query(
            r#"
            INSERT INTO feat.avwap_feature (
                ts_bucket, bar_interval, symbol,
                anchor_ts, anchor_label,
                avwap_fut, avwap_spot,
                fut_last_price, fut_mark_price,
                price_minus_avwap_fut,
                price_minus_spot_avwap_fut,
                price_minus_spot_avwap_futmark,
                avwap_gap_fs,
                xmk_avwap_gap_f_minus_s,
                zavwap_gap,
                calc_version, extra_json
            )
            VALUES (
                $1, $2::interval, $3,
                $4, 'rolling_7d',
                $5, $6,
                $7, $8,
                $9,
                $10,
                $11,
                $12,
                $13,
                $14,
                'indicator_engine.v1', $15
            )
            ON CONFLICT (venue, symbol, anchor_ts, bar_interval, ts_bucket)
            DO UPDATE SET
                avwap_fut = EXCLUDED.avwap_fut,
                avwap_spot = EXCLUDED.avwap_spot,
                fut_last_price = EXCLUDED.fut_last_price,
                fut_mark_price = EXCLUDED.fut_mark_price,
                price_minus_avwap_fut = EXCLUDED.price_minus_avwap_fut,
                price_minus_spot_avwap_fut = EXCLUDED.price_minus_spot_avwap_fut,
                price_minus_spot_avwap_futmark = EXCLUDED.price_minus_spot_avwap_futmark,
                avwap_gap_fs = EXCLUDED.avwap_gap_fs,
                xmk_avwap_gap_f_minus_s = EXCLUDED.xmk_avwap_gap_f_minus_s,
                zavwap_gap = EXCLUDED.zavwap_gap,
                calc_version = EXCLUDED.calc_version,
                extra_json = EXCLUDED.extra_json
            "#,
        )
        .bind(ctx.ts_bucket)
        .bind(bar_interval)
        .bind(&ctx.symbol)
        .bind(anchor_ts)
        .bind(fut_avwap)
        .bind(spot_avwap)
        .bind(fut_last)
        .bind(fut_mark)
        .bind(price_minus_fut)
        .bind(price_minus_spot)
        .bind(mark_minus_spot)
        .bind(avwap_gap)
        .bind(avwap_gap)
        .bind(zscore_gap(ctx, avwap_gap))
        .bind(json!({ "window_minutes": mins }))
        .execute(&self.pool)
        .await
        .with_context(|| format!("insert avwap_feature mins={}", mins))?;

        Ok(())
    }

    async fn insert_funding_feature_window(
        &self,
        ctx: &IndicatorContext,
        window_code: &str,
        mins: i64,
    ) -> Result<()> {
        let metrics = funding_metrics(ctx, mins);
        let bar_interval = interval_text(mins);

        sqlx::query(
            r#"
            INSERT INTO feat.funding_feature (
                ts_bucket, bar_interval, symbol,
                funding_current, funding_current_effective_ts,
                funding_twa,
                mark_price_last, mark_price_last_ts,
                mark_price_twap,
                index_price_last,
                change_count, changes_json,
                calc_version, extra_json
            )
            VALUES (
                $1, $2::interval, $3,
                $4, $5,
                $6,
                $7, $8,
                $9,
                $10,
                $11, $12,
                'indicator_engine.v1', $13
            )
            ON CONFLICT (venue, symbol, bar_interval, ts_bucket)
            DO UPDATE SET
                funding_current = EXCLUDED.funding_current,
                funding_current_effective_ts = EXCLUDED.funding_current_effective_ts,
                funding_twa = EXCLUDED.funding_twa,
                mark_price_last = EXCLUDED.mark_price_last,
                mark_price_last_ts = EXCLUDED.mark_price_last_ts,
                mark_price_twap = EXCLUDED.mark_price_twap,
                index_price_last = EXCLUDED.index_price_last,
                change_count = EXCLUDED.change_count,
                changes_json = EXCLUDED.changes_json,
                calc_version = EXCLUDED.calc_version,
                extra_json = EXCLUDED.extra_json
            "#,
        )
        .bind(ctx.ts_bucket)
        .bind(bar_interval)
        .bind(&ctx.symbol)
        .bind(metrics.funding_current)
        .bind(metrics.funding_current_effective_ts)
        .bind(metrics.funding_twa)
        .bind(metrics.mark_price_last)
        .bind(metrics.mark_price_last_ts)
        .bind(metrics.mark_price_twap)
        .bind(metrics.index_price_last)
        .bind(
            metrics
                .changes_json
                .as_array()
                .map(|a| a.len() as i32)
                .unwrap_or(0),
        )
        .bind(metrics.changes_json)
        .bind(json!({ "window_code": window_code, "window_minutes": mins }))
        .execute(&self.pool)
        .await
        .with_context(|| format!("insert funding_feature {}", window_code))?;

        Ok(())
    }

    async fn insert_funding_changes(&self, ctx: &IndicatorContext) -> Result<()> {
        for ch in &ctx.funding_changes_in_window {
            sqlx::query(
                r#"
                INSERT INTO feat.funding_change_event (
                    ts_change, symbol, source_kind,
                    funding_prev, funding_new, funding_delta,
                    mark_price_at_change,
                    window_code, bar_interval,
                    calc_version, extra_json
                )
                VALUES (
                    $1, $2, 'derived'::cfg.source_type,
                    $3, $4, $5,
                    $6,
                    '1m', INTERVAL '1 minute',
                    'indicator_engine.v1', $7
                )
                ON CONFLICT (symbol, ts_change, funding_new) DO NOTHING
                "#,
            )
            .bind(ch.ts_change)
            .bind(&ctx.symbol)
            .bind(ch.prev)
            .bind(ch.new)
            .bind(ch.delta)
            .bind(ch.mark_price_at_change)
            .bind(json!({}))
            .execute(&self.pool)
            .await
            .context("insert funding_change_event")?;
        }
        Ok(())
    }

    async fn insert_whale_rollup_window(
        &self,
        symbol: &str,
        history: &[MinuteHistory],
        market: MarketKind,
        threshold_usdt: f64,
        ts_bucket: DateTime<Utc>,
        mins: i64,
    ) -> Result<()> {
        let Some(agg) = aggregate_window(history, ts_bucket, mins) else {
            return Ok(());
        };

        let bar_interval = interval_text(mins);
        sqlx::query(
            r#"
            INSERT INTO feat.whale_trade_rollup (
                ts_bucket, bar_interval, market, symbol,
                threshold_usdt,
                whale_trade_count, whale_buy_count, whale_sell_count,
                whale_notional_total, whale_notional_buy, whale_notional_sell,
                whale_qty_eth_total, max_single_trade_notional,
                calc_version, extra_json
            )
            VALUES (
                $1, $2::interval, $3::cfg.market_type, $4,
                $5,
                $6, $7, $8,
                $9, $10, $11,
                $12, $13,
                'indicator_engine.v1', $14
            )
            ON CONFLICT (venue, market, symbol, bar_interval, threshold_usdt, ts_bucket)
            DO UPDATE SET
                whale_trade_count = EXCLUDED.whale_trade_count,
                whale_buy_count = EXCLUDED.whale_buy_count,
                whale_sell_count = EXCLUDED.whale_sell_count,
                whale_notional_total = EXCLUDED.whale_notional_total,
                whale_notional_buy = EXCLUDED.whale_notional_buy,
                whale_notional_sell = EXCLUDED.whale_notional_sell,
                whale_qty_eth_total = EXCLUDED.whale_qty_eth_total,
                max_single_trade_notional = EXCLUDED.max_single_trade_notional,
                calc_version = EXCLUDED.calc_version,
                extra_json = EXCLUDED.extra_json
            "#,
        )
        .bind(ts_bucket)
        .bind(bar_interval)
        .bind(market.as_str())
        .bind(symbol)
        .bind(threshold_usdt)
        .bind(agg.whale.trade_count)
        .bind(agg.whale.buy_count)
        .bind(agg.whale.sell_count)
        .bind(agg.whale.notional_total)
        .bind(agg.whale.notional_buy)
        .bind(agg.whale.notional_sell)
        .bind(agg.whale.qty_eth_total)
        .bind(Some(agg.whale.max_single_notional).filter(|v| *v > 0.0))
        .bind(json!({ "window_minutes": mins }))
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "insert whale_trade_rollup market={} mins={}",
                market.as_str(),
                mins
            )
        })?;

        Ok(())
    }
}
fn interval_text(mins: i64) -> String {
    if mins % 1440 == 0 {
        format!("{} days", mins / 1440)
    } else if mins % 60 == 0 {
        format!("{} hours", mins / 60)
    } else {
        format!("{} minutes", mins)
    }
}

fn aggregate_window(
    history: &[MinuteHistory],
    ts_bucket: DateTime<Utc>,
    mins: i64,
) -> Option<WindowAgg> {
    let start = ts_bucket - Duration::minutes(mins);
    let rows = history
        .iter()
        .filter(|h| h.ts_bucket > start && h.ts_bucket <= ts_bucket)
        .cloned()
        .collect::<Vec<_>>();
    if rows.is_empty() {
        return None;
    }

    let open_price = rows
        .first()
        .and_then(|r| r.open_price.or(r.close_price).or(r.last_price));
    let close_price = rows.last().and_then(|r| r.close_price.or(r.last_price));
    let high_price = rows
        .iter()
        .filter_map(|r| r.high_price.or(r.last_price))
        .reduce(f64::max);
    let low_price = rows
        .iter()
        .filter_map(|r| r.low_price.or(r.last_price))
        .reduce(f64::min);

    let buy_qty = rows.iter().map(|r| r.buy_qty).sum::<f64>();
    let sell_qty = rows.iter().map(|r| r.sell_qty).sum::<f64>();
    let total_qty = rows.iter().map(|r| r.total_qty).sum::<f64>();
    let total_notional = rows.iter().map(|r| r.total_notional).sum::<f64>();
    let delta = rows.iter().map(|r| r.delta).sum::<f64>();
    let relative_delta = if total_qty > 0.0 {
        delta / total_qty
    } else {
        0.0
    };
    let cvd_last = rows.last().map(|r| r.cvd).unwrap_or(0.0);
    let vpin_last = rows.last().map(|r| r.vpin);
    let avwap = if total_qty > 0.0 {
        Some(total_notional / total_qty)
    } else {
        None
    };
    let ofi_sum = rows.iter().map(|r| r.ofi).sum::<f64>();
    let obi_twa = mean(&rows.iter().filter_map(|r| r.obi_twa).collect::<Vec<_>>());
    let cvd_slope = ols_slope(&rows.iter().map(|r| r.cvd).collect::<Vec<_>>());

    let mut whale = WhaleStats::default();
    for r in &rows {
        whale.trade_count += r.whale_trade_count;
        whale.buy_count += r.whale_buy_count;
        whale.sell_count += r.whale_sell_count;
        whale.notional_total += r.whale_notional_total;
        whale.notional_buy += r.whale_notional_buy;
        whale.notional_sell += r.whale_notional_sell;
        whale.qty_eth_total += r.whale_qty_eth_total;
        whale.qty_eth_buy += r.whale_qty_eth_buy;
        whale.qty_eth_sell += r.whale_qty_eth_sell;
    }

    Some(WindowAgg {
        rows,
        open_price,
        high_price,
        low_price,
        close_price,
        trade_count: total_trade_count(history, ts_bucket, mins),
        buy_qty,
        sell_qty,
        total_qty,
        total_notional,
        delta,
        relative_delta,
        cvd_last,
        cvd_slope,
        vpin_last,
        avwap,
        ofi_sum,
        obi_twa,
        whale,
    })
}

fn total_trade_count(history: &[MinuteHistory], ts_bucket: DateTime<Utc>, mins: i64) -> i64 {
    let start = ts_bucket - Duration::minutes(mins);
    history
        .iter()
        .filter(|h| h.ts_bucket > start && h.ts_bucket <= ts_bucket)
        .map(|h| (h.buy_qty + h.sell_qty > 0.0) as i64)
        .sum()
}

fn rolling_cvd_7d(history: &[MinuteHistory], ts_bucket: DateTime<Utc>) -> Option<f64> {
    let end_cvd = history
        .iter()
        .rev()
        .find(|h| h.ts_bucket <= ts_bucket)
        .map(|h| h.cvd)?;
    let start = ts_bucket - Duration::days(7);
    let start_cvd = history
        .iter()
        .find(|h| h.ts_bucket > start)
        .map(|h| h.cvd)
        .unwrap_or(0.0);
    Some(end_cvd - start_cvd)
}

fn cumulative_avwap_7d(
    ctx: &IndicatorContext,
    ts_bucket: DateTime<Utc>,
) -> (Option<f64>, Option<f64>) {
    let start = ts_bucket - Duration::days(7);
    let fut = ctx
        .history_futures
        .iter()
        .filter(|h| h.ts_bucket > start && h.ts_bucket <= ts_bucket)
        .collect::<Vec<_>>();
    let spot = ctx
        .history_spot
        .iter()
        .filter(|h| h.ts_bucket > start && h.ts_bucket <= ts_bucket)
        .collect::<Vec<_>>();
    let fut_num = fut.iter().map(|h| h.total_notional).sum::<f64>();
    let fut_den = fut.iter().map(|h| h.total_qty).sum::<f64>();
    let spot_num = spot.iter().map(|h| h.total_notional).sum::<f64>();
    let spot_den = spot.iter().map(|h| h.total_qty).sum::<f64>();
    (
        if fut_den > 0.0 {
            Some(fut_num / fut_den)
        } else {
            None
        },
        if spot_den > 0.0 {
            Some(spot_num / spot_den)
        } else {
            None
        },
    )
}

fn funding_metrics(ctx: &IndicatorContext, mins: i64) -> FundingWindowMetrics {
    let end = ctx.ts_bucket + Duration::minutes(1);
    let start = end - Duration::minutes(mins);

    let mut funding_points = ctx
        .funding_points_recent
        .iter()
        .map(|p| (p.ts, p.funding_rate))
        .collect::<Vec<_>>();
    funding_points.sort_by_key(|(ts, _)| *ts);

    let mut mark_points = ctx
        .mark_points_recent
        .iter()
        .filter_map(|p| p.mark_price.map(|v| (p.ts, v)))
        .collect::<Vec<_>>();
    mark_points.sort_by_key(|(ts, _)| *ts);

    let funding_pair = funding_points
        .iter()
        .rev()
        .find(|(ts, _)| *ts <= end)
        .copied();
    let funding_current = funding_pair.map(|(_, v)| v);
    let funding_current_effective_ts = funding_pair.map(|(ts, _)| ts);
    let funding_fallback = funding_points
        .iter()
        .rev()
        .find(|(ts, _)| *ts <= start)
        .map(|(_, v)| *v)
        .or(funding_current);
    let funding_twa = time_weighted_avg(start, end, &funding_points, funding_fallback);

    let mark_last_pair = mark_points.iter().rev().find(|(ts, _)| *ts <= end).copied();
    let mark_price_last = mark_last_pair.map(|(_, v)| v);
    let mark_price_last_ts = mark_last_pair.map(|(ts, _)| ts);
    let mark_fallback = mark_points
        .iter()
        .rev()
        .find(|(ts, _)| *ts <= start)
        .map(|(_, v)| *v)
        .or(mark_price_last);
    let mark_price_twap = time_weighted_avg(start, end, &mark_points, mark_fallback);
    let index_price_last = ctx
        .mark_points_recent
        .iter()
        .rev()
        .find_map(|p| (p.ts <= end).then_some(p.index_price).flatten())
        .or_else(|| {
            ctx.mark_points_in_window
                .iter()
                .rev()
                .find_map(|p| (p.ts <= end).then_some(p.index_price).flatten())
        });

    let changes_json = json!(ctx
        .funding_changes_recent
        .iter()
        .filter(|c| c.ts_change >= start && c.ts_change < end)
        .map(|c| json!({
            "change_ts": c.ts_change.to_rfc3339(),
            "funding_prev": c.prev,
            "funding_new": c.new,
            "funding_delta": c.delta,
            "mark_price_at_change": c.mark_price_at_change
        }))
        .collect::<Vec<_>>());

    FundingWindowMetrics {
        funding_current,
        funding_current_effective_ts,
        funding_twa,
        mark_price_last,
        mark_price_last_ts,
        mark_price_twap,
        index_price_last,
        changes_json,
    }
}
fn time_weighted_avg(
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    points: &[(DateTime<Utc>, f64)],
    fallback: Option<f64>,
) -> Option<f64> {
    if end <= start {
        return None;
    }
    if points.is_empty() {
        return fallback;
    }

    let mut weighted = 0.0;
    let mut total = 0.0;
    let mut cursor = start;
    let mut last_val = fallback.unwrap_or(points[0].1);

    for (ts, v) in points {
        if *ts <= start {
            last_val = *v;
            continue;
        }
        if *ts > end {
            break;
        }
        let dt = (*ts - cursor).num_milliseconds().max(0) as f64 / 1000.0;
        if dt > 0.0 {
            weighted += last_val * dt;
            total += dt;
        }
        cursor = *ts;
        last_val = *v;
    }

    if cursor < end {
        let dt = (end - cursor).num_milliseconds().max(0) as f64 / 1000.0;
        weighted += last_val * dt;
        total += dt;
    }

    if total <= 0.0 {
        fallback.or(Some(last_val))
    } else {
        Some(weighted / total)
    }
}

fn delta_slant(delta: f64, high: Option<f64>, low: Option<f64>) -> Option<f64> {
    let range = high.zip(low).map(|(h, l)| h - l)?;
    if range.abs() < 1e-12 {
        None
    } else {
        Some(delta / (range + 1e-12))
    }
}

fn mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    Some(values.iter().sum::<f64>() / values.len() as f64)
}

fn stddev(values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    let m = mean(values)?;
    let var = values
        .iter()
        .map(|v| {
            let d = *v - m;
            d * d
        })
        .sum::<f64>()
        / values.len() as f64;
    Some(var.sqrt())
}

fn ema(values: &[f64], period: usize) -> Option<f64> {
    if values.is_empty() || period == 0 {
        return None;
    }
    let alpha = 2.0 / (period as f64 + 1.0);
    let mut v = values[0];
    for x in values.iter().skip(1) {
        v = alpha * *x + (1.0 - alpha) * v;
    }
    Some(v)
}

fn burst_zscore(values: &[f64], fast: usize, slow: usize, lookback: usize) -> Option<f64> {
    if values.len() < slow + 2 {
        return None;
    }
    let mut bursts = Vec::with_capacity(values.len());
    let mut ema_fast = values[0];
    let mut ema_slow = values[0];
    let a_fast = 2.0 / (fast as f64 + 1.0);
    let a_slow = 2.0 / (slow as f64 + 1.0);
    for v in values {
        ema_fast = a_fast * *v + (1.0 - a_fast) * ema_fast;
        ema_slow = a_slow * *v + (1.0 - a_slow) * ema_slow;
        bursts.push(ema_fast - ema_slow);
    }
    let current = *bursts.last()?;
    let hist_len = bursts.len().saturating_sub(1).min(lookback);
    if hist_len < 5 {
        return None;
    }
    let hist = &bursts[bursts.len() - 1 - hist_len..bursts.len() - 1];
    let m = mean(hist)?;
    let s = stddev(hist)?;
    if s <= 1e-12 {
        Some(0.0)
    } else {
        Some((current - m) / s)
    }
}

fn ols_slope(values: &[f64]) -> Option<f64> {
    if values.len() < 3 {
        return None;
    }
    let n = values.len() as f64;
    let x_mean = (n - 1.0) / 2.0;
    let y_mean = values.iter().sum::<f64>() / n;
    let mut num = 0.0;
    let mut den = 0.0;
    for (i, y) in values.iter().enumerate() {
        let x = i as f64;
        num += (x - x_mean) * (y - y_mean);
        den += (x - x_mean) * (x - x_mean);
    }
    if den <= 1e-12 {
        None
    } else {
        Some(num / den)
    }
}

fn zscore_gap(ctx: &IndicatorContext, current_gap: Option<f64>) -> Option<f64> {
    let current = current_gap?;
    let n = ctx.history_futures.len().min(ctx.history_spot.len());
    if n < 10 {
        return None;
    }
    let mut gaps = Vec::new();
    for i in 0..n {
        let f = &ctx.history_futures[n - 1 - i];
        let s = &ctx.history_spot[n - 1 - i];
        if f.total_qty <= 0.0 || s.total_qty <= 0.0 {
            continue;
        }
        gaps.push((f.total_notional / f.total_qty) - (s.total_notional / s.total_qty));
    }
    if gaps.len() < 10 {
        return None;
    }
    let mean = gaps.iter().sum::<f64>() / gaps.len() as f64;
    let var = gaps
        .iter()
        .map(|v| {
            let d = *v - mean;
            d * d
        })
        .sum::<f64>()
        / gaps.len() as f64;
    let sd = var.sqrt();
    if sd <= 1e-12 {
        Some(0.0)
    } else {
        Some((current - mean) / sd)
    }
}
