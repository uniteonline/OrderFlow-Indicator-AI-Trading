use crate::indicators::context::{
    AbsorptionEventRow, DivergenceEventRow, ExhaustionEventRow, IndicatorEventRow,
    InitiationEventRow,
};
use crate::indicators::shared::event_ids::{build_divergence_event_id, build_indicator_event_id};
use crate::observability::metrics::AppMetrics;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tracing::warn;

const EVENT_WRITE_TIMEOUT: Duration = Duration::from_secs(3);
const EVENT_WRITE_WARN_INTERVAL_MS: i64 = 30_000;
const GENERIC_RECONCILE_INDICATOR_CODES: [&str; 5] = [
    "divergence",
    "absorption",
    "initiation",
    "buying_exhaustion",
    "selling_exhaustion",
];
const TYPED_EVENT_SCOPE_TS_EXPR: &str =
    "COALESCE(event_available_ts, ts_event_end, ts_event_start)";

fn should_write_generic_indicator_event(indicator_code: &str) -> bool {
    matches!(
        indicator_code,
        "divergence"
            | "bullish_absorption"
            | "bearish_absorption"
            | "bullish_initiation"
            | "bearish_initiation"
    )
}

#[cfg(test)]
mod tests {
    use super::{should_write_generic_indicator_event, TYPED_EVENT_SCOPE_TS_EXPR};

    #[test]
    fn typed_history_indicators_do_not_write_generic_rows() {
        assert!(!should_write_generic_indicator_event("absorption"));
        assert!(!should_write_generic_indicator_event("initiation"));
        assert!(!should_write_generic_indicator_event("buying_exhaustion"));
        assert!(!should_write_generic_indicator_event("selling_exhaustion"));
        assert!(should_write_generic_indicator_event("bullish_absorption"));
        assert!(should_write_generic_indicator_event("bearish_initiation"));
        assert!(should_write_generic_indicator_event("divergence"));
    }

    #[test]
    fn typed_reconcile_scope_uses_shared_available_ts_expression() {
        assert_eq!(
            TYPED_EVENT_SCOPE_TS_EXPR,
            "COALESCE(event_available_ts, ts_event_end, ts_event_start)"
        );
        assert!(!TYPED_EVENT_SCOPE_TS_EXPR.contains("confirm_ts"));
    }
}

#[derive(Clone)]
pub struct EventWriter {
    pool: PgPool,
    metrics: Arc<AppMetrics>,
    last_warn_ts_ms: Arc<AtomicI64>,
    suppressed_warns: Arc<AtomicU64>,
}

impl EventWriter {
    pub fn new(pool: PgPool, metrics: Arc<AppMetrics>) -> Self {
        Self {
            pool,
            metrics,
            last_warn_ts_ms: Arc::new(AtomicI64::new(0)),
            suppressed_warns: Arc::new(AtomicU64::new(0)),
        }
    }

    pub async fn write_indicator_events(
        &self,
        symbol: &str,
        history_start_ts: DateTime<Utc>,
        history_end_ts: DateTime<Utc>,
        rows: &[IndicatorEventRow],
    ) {
        let mut all_ok = true;
        let generic_rows = rows
            .iter()
            .filter(|row| should_write_generic_indicator_event(row.indicator_code))
            .collect::<Vec<_>>();
        let expected_event_ids = generic_rows
            .iter()
            .filter(|row| {
                GENERIC_RECONCILE_INDICATOR_CODES
                    .iter()
                    .any(|code| *code == row.indicator_code)
            })
            .map(|row| self.resolve_indicator_event_id(symbol, row))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let scoped_indicator_codes = GENERIC_RECONCILE_INDICATOR_CODES
            .iter()
            .map(|code| (*code).to_string())
            .collect::<Vec<_>>();
        all_ok &= self
            .reconcile_indicator_event_scope(
                symbol,
                history_start_ts,
                history_end_ts,
                &scoped_indicator_codes,
                &expected_event_ids,
            )
            .await;
        for row in generic_rows {
            let event_id = self.resolve_indicator_event_id(symbol, row);

            let ok = self
                .execute_with_timeout(
                    format!(
                        "indicator_event type={} code={} event_id={}",
                        row.event_type, row.indicator_code, event_id
                    ),
                    sqlx::query(
                        r#"
                        INSERT INTO evt.indicator_event (
                            event_id,
                            ts_event_start, ts_event_end, event_available_ts,
                            symbol, indicator_code, event_type,
                            severity, direction, bar_interval, window_code,
                            pivot_ts_1, pivot_ts_2, pivot_confirm_ts_1, pivot_confirm_ts_2,
                            sig_pass, p_value, primary_market, market_scope,
                            score, confidence, calc_version, payload_json
                        )
                        VALUES (
                            $1,
                            $2, $3, $4,
                            $5, $6, $7,
                            $8::cfg.event_severity, $9, INTERVAL '1 minute', $10,
                            $11, $12, $13, $14,
                            $15, $16, 'futures'::cfg.market_type, 'futures_primary',
                            $17, $18, 'indicator_engine.v1', $19
                        )
                        ON CONFLICT (event_id, ts_event_start)
                        DO UPDATE SET
                            ts_event_start = EXCLUDED.ts_event_start,
                            ts_event_end = EXCLUDED.ts_event_end,
                            event_available_ts = EXCLUDED.event_available_ts,
                            symbol = EXCLUDED.symbol,
                            indicator_code = EXCLUDED.indicator_code,
                            event_type = EXCLUDED.event_type,
                            severity = EXCLUDED.severity,
                            direction = EXCLUDED.direction,
                            bar_interval = EXCLUDED.bar_interval,
                            window_code = EXCLUDED.window_code,
                            pivot_ts_1 = EXCLUDED.pivot_ts_1,
                            pivot_ts_2 = EXCLUDED.pivot_ts_2,
                            pivot_confirm_ts_1 = EXCLUDED.pivot_confirm_ts_1,
                            pivot_confirm_ts_2 = EXCLUDED.pivot_confirm_ts_2,
                            sig_pass = EXCLUDED.sig_pass,
                            p_value = EXCLUDED.p_value,
                            primary_market = EXCLUDED.primary_market,
                            market_scope = EXCLUDED.market_scope,
                            score = EXCLUDED.score,
                            confidence = EXCLUDED.confidence,
                            calc_version = EXCLUDED.calc_version,
                            payload_json = EXCLUDED.payload_json
                        "#,
                    )
                    .bind(&event_id)
                    .bind(row.ts_event_start)
                    .bind(row.ts_event_end)
                    .bind(row.event_available_ts)
                    .bind(symbol)
                    .bind(row.indicator_code)
                    .bind(&row.event_type)
                    .bind(&row.severity)
                    .bind(row.direction as i32)
                    .bind(row.window_code)
                    .bind(row.pivot_ts_1)
                    .bind(row.pivot_ts_2)
                    .bind(row.pivot_confirm_ts_1)
                    .bind(row.pivot_confirm_ts_2)
                    .bind(row.sig_pass.unwrap_or(true))
                    .bind(row.p_value)
                    .bind(row.score)
                    .bind(row.confidence)
                    .bind(&row.payload_json)
                    .execute(&self.pool),
                )
                .await;
            all_ok &= ok;
        }

        if all_ok {
            self.metrics.record_event_writer_success();
        }
    }

    pub async fn write_divergence_events(
        &self,
        symbol: &str,
        history_start_ts: DateTime<Utc>,
        history_end_ts: DateTime<Utc>,
        rows: &[DivergenceEventRow],
    ) {
        let mut all_ok = true;
        let expected_event_ids = rows
            .iter()
            .map(|row| row.event_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        all_ok &= self
            .reconcile_event_scope(
                "evt.divergence_event",
                symbol,
                history_start_ts,
                history_end_ts,
                &expected_event_ids,
            )
            .await;
        for row in rows {
            let price_diff = match (row.price_start, row.price_end) {
                (Some(a), Some(b)) => Some(b - a),
                _ => None,
            };
            let cvd_diff_fut = match (row.cvd_start_fut, row.cvd_end_fut) {
                (Some(a), Some(b)) => Some(b - a),
                _ => None,
            };
            let cvd_diff_spot = match (row.cvd_start_spot, row.cvd_end_spot) {
                (Some(a), Some(b)) => Some(b - a),
                _ => None,
            };
            let event_id = if row.event_id.is_empty() {
                build_divergence_event_id(
                    symbol,
                    &row.divergence_type,
                    &row.pivot_side,
                    row.ts_event_start,
                    row.ts_event_end,
                    row.pivot_ts_1,
                    row.pivot_ts_2,
                )
            } else {
                row.event_id.clone()
            };

            let ok = self
                .execute_with_timeout(
                    format!(
                        "divergence_event type={} event_id={}",
                        row.divergence_type, event_id
                    ),
                    sqlx::query(
                        r#"
                        INSERT INTO evt.divergence_event (
                            event_id,
                            ts_event_start, ts_event_end, event_available_ts,
                            symbol, divergence_type, pivot_side,
                            pivot_ts_1, pivot_ts_2, pivot_confirm_ts_1, pivot_confirm_ts_2,
                            leg_minutes,
                            price_start, price_end,
                            price_diff, price_norm_diff,
                            cvd_start_1m_fut, cvd_end_1m_fut,
                            cvd_diff_fut, cvd_norm_diff_fut,
                            cvd_start_1m_spot, cvd_end_1m_spot,
                            cvd_diff_spot, cvd_norm_diff_spot,
                            price_effect_z, cvd_effect_z,
                            sig_pass, p_value_price, p_value_cvd,
                            spot_price_flow_confirm, fut_divergence_sign, spot_lead_score, likely_driver,
                            score, confidence, window_code,
                            calc_version, payload_json
                        )
                        VALUES (
                            $1,
                            $2, $3, $4,
                            $5, $6, $7,
                            $8, $9, $10, $11,
                            $12,
                            $13, $14,
                            $15, $16,
                            $17, $18,
                            $19, $20,
                            $21, $22,
                            $23, $24,
                            $25, $26,
                            $27, $28, $29,
                            $30, $31, $32, $33,
                            $34, $35, $36,
                            'indicator_engine.v1', $37
                        )
                        ON CONFLICT (event_id, ts_event_start)
                        DO UPDATE SET
                            ts_event_start = EXCLUDED.ts_event_start,
                            ts_event_end = EXCLUDED.ts_event_end,
                            event_available_ts = EXCLUDED.event_available_ts,
                            symbol = EXCLUDED.symbol,
                            divergence_type = EXCLUDED.divergence_type,
                            pivot_side = EXCLUDED.pivot_side,
                            pivot_ts_1 = EXCLUDED.pivot_ts_1,
                            pivot_ts_2 = EXCLUDED.pivot_ts_2,
                            pivot_confirm_ts_1 = EXCLUDED.pivot_confirm_ts_1,
                            pivot_confirm_ts_2 = EXCLUDED.pivot_confirm_ts_2,
                            leg_minutes = EXCLUDED.leg_minutes,
                            price_start = EXCLUDED.price_start,
                            price_end = EXCLUDED.price_end,
                            price_diff = EXCLUDED.price_diff,
                            price_norm_diff = EXCLUDED.price_norm_diff,
                            cvd_start_1m_fut = EXCLUDED.cvd_start_1m_fut,
                            cvd_end_1m_fut = EXCLUDED.cvd_end_1m_fut,
                            cvd_diff_fut = EXCLUDED.cvd_diff_fut,
                            cvd_norm_diff_fut = EXCLUDED.cvd_norm_diff_fut,
                            cvd_start_1m_spot = EXCLUDED.cvd_start_1m_spot,
                            cvd_end_1m_spot = EXCLUDED.cvd_end_1m_spot,
                            cvd_diff_spot = EXCLUDED.cvd_diff_spot,
                            cvd_norm_diff_spot = EXCLUDED.cvd_norm_diff_spot,
                            price_effect_z = EXCLUDED.price_effect_z,
                            cvd_effect_z = EXCLUDED.cvd_effect_z,
                            sig_pass = EXCLUDED.sig_pass,
                            p_value_price = EXCLUDED.p_value_price,
                            p_value_cvd = EXCLUDED.p_value_cvd,
                            spot_price_flow_confirm = EXCLUDED.spot_price_flow_confirm,
                            fut_divergence_sign = EXCLUDED.fut_divergence_sign,
                            spot_lead_score = EXCLUDED.spot_lead_score,
                            likely_driver = EXCLUDED.likely_driver,
                            score = EXCLUDED.score,
                            confidence = EXCLUDED.confidence,
                            window_code = EXCLUDED.window_code,
                            calc_version = EXCLUDED.calc_version,
                            payload_json = EXCLUDED.payload_json
                        "#,
                    )
                    .bind(&event_id)
                    .bind(row.ts_event_start)
                    .bind(row.ts_event_end)
                    .bind(row.event_available_ts.or(Some(row.ts_event_end)))
                    .bind(symbol)
                    .bind(&row.divergence_type)
                    .bind(&row.pivot_side)
                    .bind(row.pivot_ts_1)
                    .bind(row.pivot_ts_2)
                    .bind(row.pivot_confirm_ts_1)
                    .bind(row.pivot_confirm_ts_2)
                    .bind(row.leg_minutes)
                    .bind(row.price_start)
                    .bind(row.price_end)
                    .bind(price_diff)
                    .bind(row.price_norm_diff)
                    .bind(row.cvd_start_fut)
                    .bind(row.cvd_end_fut)
                    .bind(cvd_diff_fut)
                    .bind(row.cvd_norm_diff_fut)
                    .bind(row.cvd_start_spot)
                    .bind(row.cvd_end_spot)
                    .bind(cvd_diff_spot)
                    .bind(row.cvd_norm_diff_spot)
                    .bind(row.price_effect_z)
                    .bind(row.cvd_effect_z)
                    .bind(row.sig_pass)
                    .bind(row.p_value_price)
                    .bind(row.p_value_cvd)
                    .bind(row.spot_price_flow_confirm)
                    .bind(row.fut_divergence_sign)
                    .bind(row.spot_lead_score)
                    .bind(&row.likely_driver)
                    .bind(row.score)
                    .bind(row.confidence)
                    .bind(row.window_code)
                    .bind(&row.payload_json)
                    .execute(&self.pool),
                )
                .await;
            all_ok &= ok;
        }

        if all_ok {
            self.metrics.record_event_writer_success();
        }
    }

    pub async fn write_absorption_events(
        &self,
        symbol: &str,
        history_start_ts: DateTime<Utc>,
        history_end_ts: DateTime<Utc>,
        rows: &[AbsorptionEventRow],
    ) {
        let mut all_ok = true;
        let expected_event_ids = rows
            .iter()
            .map(|row| row.event_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        all_ok &= self
            .reconcile_event_scope(
                "evt.absorption_event",
                symbol,
                history_start_ts,
                history_end_ts,
                &expected_event_ids,
            )
            .await;
        for row in rows {
            let ok = self
                .execute_with_timeout(
                    format!(
                        "absorption_event type={} event_id={}",
                        row.event_type, row.event_id
                    ),
                    sqlx::query(
                        r#"
                        INSERT INTO evt.absorption_event (
                            event_id,
                            ts_event_start, ts_event_end, confirm_ts, event_available_ts,
                            symbol, event_type, direction,
                            trigger_side,
                            pivot_price, price_low, price_high,
                            delta_sum, rdelta_mean, reject_ratio, key_distance_ticks,
                            stacked_buy_imbalance, stacked_sell_imbalance,
                            spot_rdelta_1m_mean, spot_cvd_1m_change,
                            spot_flow_confirm_score, spot_whale_confirm_score, spot_confirm,
                            score_base, score, confidence, window_code,
                            calc_version, payload_json
                        )
                        VALUES (
                            $1,
                            $2, $3, $4, $5,
                            $6, $7, $8,
                            $9,
                            $10, $11, $12,
                            $13, $14, $15, $16,
                            $17, $18,
                            $19, $20,
                            $21, $22, $23,
                            $24, $25, $26, $27,
                            'indicator_engine.v1', $28
                        )
                        ON CONFLICT (symbol, event_id, ts_event_start)
                        DO UPDATE SET
                            ts_event_end = EXCLUDED.ts_event_end,
                            confirm_ts = EXCLUDED.confirm_ts,
                            event_available_ts = EXCLUDED.event_available_ts,
                            event_type = EXCLUDED.event_type,
                            direction = EXCLUDED.direction,
                            trigger_side = EXCLUDED.trigger_side,
                            pivot_price = EXCLUDED.pivot_price,
                            price_low = EXCLUDED.price_low,
                            price_high = EXCLUDED.price_high,
                            delta_sum = EXCLUDED.delta_sum,
                            rdelta_mean = EXCLUDED.rdelta_mean,
                            reject_ratio = EXCLUDED.reject_ratio,
                            key_distance_ticks = EXCLUDED.key_distance_ticks,
                            stacked_buy_imbalance = EXCLUDED.stacked_buy_imbalance,
                            stacked_sell_imbalance = EXCLUDED.stacked_sell_imbalance,
                            spot_rdelta_1m_mean = EXCLUDED.spot_rdelta_1m_mean,
                            spot_cvd_1m_change = EXCLUDED.spot_cvd_1m_change,
                            spot_flow_confirm_score = EXCLUDED.spot_flow_confirm_score,
                            spot_whale_confirm_score = EXCLUDED.spot_whale_confirm_score,
                            spot_confirm = EXCLUDED.spot_confirm,
                            score_base = EXCLUDED.score_base,
                            score = EXCLUDED.score,
                            confidence = EXCLUDED.confidence,
                            window_code = EXCLUDED.window_code,
                            calc_version = EXCLUDED.calc_version,
                            payload_json = EXCLUDED.payload_json
                        "#,
                    )
                    .bind(&row.event_id)
                    .bind(row.ts_event_start)
                    .bind(row.ts_event_end)
                    .bind(row.confirm_ts)
                    .bind(row.event_available_ts)
                    .bind(symbol)
                    .bind(&row.event_type)
                    .bind(row.direction as i32)
                    .bind(row.trigger_side.as_deref())
                    .bind(row.pivot_price)
                    .bind(row.price_low)
                    .bind(row.price_high)
                    .bind(row.delta_sum)
                    .bind(row.rdelta_mean)
                    .bind(row.reject_ratio)
                    .bind(row.key_distance_ticks)
                    .bind(row.stacked_buy_imbalance)
                    .bind(row.stacked_sell_imbalance)
                    .bind(row.spot_rdelta_1m_mean)
                    .bind(row.spot_cvd_1m_change)
                    .bind(row.spot_flow_confirm_score)
                    .bind(row.spot_whale_confirm_score)
                    .bind(row.spot_confirm)
                    .bind(row.score_base)
                    .bind(row.score)
                    .bind(row.confidence)
                    .bind(row.window_code)
                    .bind(&row.payload_json)
                    .execute(&self.pool),
                )
                .await;
            all_ok &= ok;
        }

        if all_ok {
            self.metrics.record_event_writer_success();
        }
    }

    pub async fn write_initiation_events(
        &self,
        symbol: &str,
        history_start_ts: DateTime<Utc>,
        history_end_ts: DateTime<Utc>,
        rows: &[InitiationEventRow],
    ) {
        let mut all_ok = true;
        let expected_event_ids = rows
            .iter()
            .map(|row| row.event_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        all_ok &= self
            .reconcile_event_scope(
                "evt.initiation_event",
                symbol,
                history_start_ts,
                history_end_ts,
                &expected_event_ids,
            )
            .await;
        for row in rows {
            let ok = self
                .execute_with_timeout(
                    format!("initiation_event type={} event_id={}", row.event_type, row.event_id),
                    sqlx::query(
                        r#"
                        INSERT INTO evt.initiation_event (
                            event_id,
                            ts_event_start, ts_event_end, confirm_ts, event_available_ts,
                            symbol, event_type, direction,
                            pivot_price, break_mag_ticks, z_delta, rdelta_mean,
                            min_follow_required_minutes,
                            follow_through_minutes, follow_through_end_ts,
                            follow_through_delta_sum, follow_through_hold_ok,
                            follow_through_max_adverse_excursion_ticks,
                            spot_break_confirm, spot_rdelta_1m_mean, spot_cvd_change, spot_whale_break_confirm,
                            score, confidence, window_code,
                            calc_version, payload_json
                        )
                        VALUES (
                            $1,
                            $2, $3, $4, $5,
                            $6, $7, $8,
                            $9, $10, $11, $12,
                            $13,
                            $14, $15,
                            $16, $17, $18,
                            $19, $20, $21, $22,
                            $23, $24, $25,
                            'indicator_engine.v1', $26
                        )
                        ON CONFLICT (symbol, event_id, ts_event_start)
                        DO UPDATE SET
                            ts_event_end = EXCLUDED.ts_event_end,
                            confirm_ts = EXCLUDED.confirm_ts,
                            event_available_ts = EXCLUDED.event_available_ts,
                            event_type = EXCLUDED.event_type,
                            direction = EXCLUDED.direction,
                            pivot_price = EXCLUDED.pivot_price,
                            break_mag_ticks = EXCLUDED.break_mag_ticks,
                            z_delta = EXCLUDED.z_delta,
                            rdelta_mean = EXCLUDED.rdelta_mean,
                            min_follow_required_minutes = EXCLUDED.min_follow_required_minutes,
                            follow_through_minutes = EXCLUDED.follow_through_minutes,
                            follow_through_end_ts = EXCLUDED.follow_through_end_ts,
                            follow_through_delta_sum = EXCLUDED.follow_through_delta_sum,
                            follow_through_hold_ok = EXCLUDED.follow_through_hold_ok,
                            follow_through_max_adverse_excursion_ticks =
                                EXCLUDED.follow_through_max_adverse_excursion_ticks,
                            spot_break_confirm = EXCLUDED.spot_break_confirm,
                            spot_rdelta_1m_mean = EXCLUDED.spot_rdelta_1m_mean,
                            spot_cvd_change = EXCLUDED.spot_cvd_change,
                            spot_whale_break_confirm = EXCLUDED.spot_whale_break_confirm,
                            score = EXCLUDED.score,
                            confidence = EXCLUDED.confidence,
                            window_code = EXCLUDED.window_code,
                            calc_version = EXCLUDED.calc_version,
                            payload_json = EXCLUDED.payload_json
                        "#,
                    )
                    .bind(&row.event_id)
                    .bind(row.ts_event_start)
                    .bind(row.ts_event_end)
                    .bind(row.confirm_ts)
                    .bind(row.event_available_ts)
                    .bind(symbol)
                    .bind(&row.event_type)
                    .bind(row.direction as i32)
                    .bind(row.pivot_price)
                    .bind(row.break_mag_ticks)
                    .bind(row.z_delta)
                    .bind(row.rdelta_mean)
                    .bind(row.min_follow_required_minutes)
                    .bind(row.follow_through_minutes)
                    .bind(row.follow_through_end_ts)
                    .bind(row.follow_through_delta_sum)
                    .bind(row.follow_through_hold_ok)
                    .bind(row.follow_through_max_adverse_excursion_ticks)
                    .bind(row.spot_break_confirm)
                    .bind(row.spot_rdelta_1m_mean)
                    .bind(row.spot_cvd_change)
                    .bind(row.spot_whale_break_confirm)
                    .bind(row.score)
                    .bind(row.confidence)
                    .bind(row.window_code)
                    .bind(&row.payload_json)
                    .execute(&self.pool),
                )
                .await;
            all_ok &= ok;
        }

        if all_ok {
            self.metrics.record_event_writer_success();
        }
    }

    pub async fn write_exhaustion_events(
        &self,
        symbol: &str,
        history_start_ts: DateTime<Utc>,
        history_end_ts: DateTime<Utc>,
        rows: &[ExhaustionEventRow],
    ) {
        let mut all_ok = true;
        let expected_event_ids = rows
            .iter()
            .map(|row| row.event_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        all_ok &= self
            .reconcile_event_scope(
                "evt.exhaustion_event",
                symbol,
                history_start_ts,
                history_end_ts,
                &expected_event_ids,
            )
            .await;
        for row in rows {
            let ok = self
                .execute_with_timeout(
                    format!("exhaustion_event type={} event_id={}", row.event_type, row.event_id),
                    sqlx::query(
                        r#"
                        INSERT INTO evt.exhaustion_event (
                            event_id,
                            ts_event_start, ts_event_end, confirm_ts, event_available_ts,
                            symbol, event_type, direction,
                            pivot_ts_1, pivot_ts_2, pivot_confirm_ts_1, pivot_confirm_ts_2,
                            price_push_ticks, delta_change, rdelta_change, reject_ratio, confirm_speed,
                            spot_cvd_push_post_pivot, spot_whale_push_post_pivot,
                            spot_continuation_risk, spot_exhaustion_confirm,
                            score, confidence, window_code,
                            calc_version, payload_json
                        )
                        VALUES (
                            $1,
                            $2, $3, $4, $5,
                            $6, $7, $8,
                            $9, $10, $11, $12,
                            $13, $14, $15, $16, $17,
                            $18, $19,
                            $20, $21,
                            $22, $23, $24,
                            'indicator_engine.v1', $25
                        )
                        ON CONFLICT (symbol, event_id, ts_event_start)
                        DO UPDATE SET
                            ts_event_end = EXCLUDED.ts_event_end,
                            confirm_ts = EXCLUDED.confirm_ts,
                            event_available_ts = EXCLUDED.event_available_ts,
                            event_type = EXCLUDED.event_type,
                            direction = EXCLUDED.direction,
                            pivot_ts_1 = EXCLUDED.pivot_ts_1,
                            pivot_ts_2 = EXCLUDED.pivot_ts_2,
                            pivot_confirm_ts_1 = EXCLUDED.pivot_confirm_ts_1,
                            pivot_confirm_ts_2 = EXCLUDED.pivot_confirm_ts_2,
                            price_push_ticks = EXCLUDED.price_push_ticks,
                            delta_change = EXCLUDED.delta_change,
                            rdelta_change = EXCLUDED.rdelta_change,
                            reject_ratio = EXCLUDED.reject_ratio,
                            confirm_speed = EXCLUDED.confirm_speed,
                            spot_cvd_push_post_pivot = EXCLUDED.spot_cvd_push_post_pivot,
                            spot_whale_push_post_pivot = EXCLUDED.spot_whale_push_post_pivot,
                            spot_continuation_risk = EXCLUDED.spot_continuation_risk,
                            spot_exhaustion_confirm = EXCLUDED.spot_exhaustion_confirm,
                            score = EXCLUDED.score,
                            confidence = EXCLUDED.confidence,
                            window_code = EXCLUDED.window_code,
                            calc_version = EXCLUDED.calc_version,
                            payload_json = EXCLUDED.payload_json
                        "#,
                    )
                    .bind(&row.event_id)
                    .bind(row.ts_event_start)
                    .bind(row.ts_event_end)
                    .bind(row.confirm_ts)
                    .bind(row.event_available_ts)
                    .bind(symbol)
                    .bind(&row.event_type)
                    .bind(row.direction as i32)
                    .bind(row.pivot_ts_1)
                    .bind(row.pivot_ts_2)
                    .bind(row.pivot_confirm_ts_1)
                    .bind(row.pivot_confirm_ts_2)
                    .bind(row.price_push_ticks)
                    .bind(row.delta_change)
                    .bind(row.rdelta_change)
                    .bind(row.reject_ratio)
                    .bind(row.confirm_speed)
                    .bind(row.spot_cvd_push_post_pivot)
                    .bind(row.spot_whale_push_post_pivot)
                    .bind(row.spot_continuation_risk)
                    .bind(row.spot_exhaustion_confirm)
                    .bind(row.score)
                    .bind(row.confidence)
                    .bind(row.window_code)
                    .bind(&row.payload_json)
                    .execute(&self.pool),
                )
                .await;
            all_ok &= ok;
        }

        if all_ok {
            self.metrics.record_event_writer_success();
        }
    }

    async fn reconcile_event_scope(
        &self,
        table: &str,
        symbol: &str,
        history_start_ts: DateTime<Utc>,
        history_end_ts: DateTime<Utc>,
        expected_event_ids: &[String],
    ) -> bool {
        if history_start_ts > history_end_ts {
            return true;
        }

        let op = format!(
            "reconcile {table} scope={}..{} ids={}",
            history_start_ts.to_rfc3339(),
            history_end_ts.to_rfc3339(),
            expected_event_ids.len()
        );

        if expected_event_ids.is_empty() {
            let delete_sql = format!(
                r#"
                DELETE FROM {table}
                WHERE symbol = $1
                  AND {TYPED_EVENT_SCOPE_TS_EXPR} >= $2
                  AND {TYPED_EVENT_SCOPE_TS_EXPR} <= $3
                "#
            );
            return self
                .execute_with_timeout(
                    op,
                    sqlx::query(&delete_sql)
                        .bind(symbol)
                        .bind(history_start_ts)
                        .bind(history_end_ts)
                        .execute(&self.pool),
                )
                .await;
        }

        let delete_sql = format!(
            r#"
            DELETE FROM {table}
            WHERE symbol = $1
              AND {TYPED_EVENT_SCOPE_TS_EXPR} >= $2
              AND {TYPED_EVENT_SCOPE_TS_EXPR} <= $3
              AND NOT (event_id = ANY($4))
            "#
        );
        self.execute_with_timeout(
            op,
            sqlx::query(&delete_sql)
                .bind(symbol)
                .bind(history_start_ts)
                .bind(history_end_ts)
                .bind(expected_event_ids)
                .execute(&self.pool),
        )
        .await
    }

    async fn reconcile_indicator_event_scope(
        &self,
        symbol: &str,
        history_start_ts: DateTime<Utc>,
        history_end_ts: DateTime<Utc>,
        indicator_codes: &[String],
        expected_event_ids: &[String],
    ) -> bool {
        if history_start_ts > history_end_ts || indicator_codes.is_empty() {
            return true;
        }

        let op = format!(
            "reconcile evt.indicator_event scope={}..{} codes={} ids={}",
            history_start_ts.to_rfc3339(),
            history_end_ts.to_rfc3339(),
            indicator_codes.len(),
            expected_event_ids.len()
        );

        if expected_event_ids.is_empty() {
            return self
                .execute_with_timeout(
                    op,
                    sqlx::query(
                        r#"
                        DELETE FROM evt.indicator_event
                        WHERE symbol = $1
                          AND indicator_code = ANY($2)
                          AND COALESCE(event_available_ts, ts_event_end, ts_event_start) >= $3
                          AND COALESCE(event_available_ts, ts_event_end, ts_event_start) <= $4
                        "#,
                    )
                    .bind(symbol)
                    .bind(indicator_codes)
                    .bind(history_start_ts)
                    .bind(history_end_ts)
                    .execute(&self.pool),
                )
                .await;
        }

        self.execute_with_timeout(
            op,
            sqlx::query(
                r#"
                DELETE FROM evt.indicator_event
                WHERE symbol = $1
                  AND indicator_code = ANY($2)
                  AND COALESCE(event_available_ts, ts_event_end, ts_event_start) >= $3
                  AND COALESCE(event_available_ts, ts_event_end, ts_event_start) <= $4
                  AND NOT (event_id = ANY($5))
                "#,
            )
            .bind(symbol)
            .bind(indicator_codes)
            .bind(history_start_ts)
            .bind(history_end_ts)
            .bind(expected_event_ids)
            .execute(&self.pool),
        )
        .await
    }

    fn resolve_indicator_event_id(&self, symbol: &str, row: &IndicatorEventRow) -> String {
        if row.event_id.is_empty() {
            build_indicator_event_id(
                symbol,
                row.indicator_code,
                &row.event_type,
                row.ts_event_start,
                row.ts_event_end,
                row.direction,
                row.pivot_ts_1,
                row.pivot_ts_2,
            )
        } else {
            row.event_id.clone()
        }
    }

    async fn execute_with_timeout<F, T, E>(&self, op: String, future: F) -> bool
    where
        F: Future<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        match timeout(EVENT_WRITE_TIMEOUT, future).await {
            Ok(Ok(_)) => true,
            Ok(Err(err)) => {
                self.record_failure(op, format!("db_error: {err}"));
                false
            }
            Err(_) => {
                self.record_failure(
                    op,
                    format!("timeout after {}ms", EVENT_WRITE_TIMEOUT.as_millis()),
                );
                false
            }
        }
    }

    fn record_failure(&self, op: String, reason: String) {
        let failure_ts_ms = Utc::now().timestamp_millis();
        let combined_reason = format!("{op}: {reason}");
        self.metrics
            .record_event_writer_failure(failure_ts_ms, combined_reason.clone());

        let last_warn_ts_ms = self.last_warn_ts_ms.load(Ordering::Relaxed);
        if failure_ts_ms - last_warn_ts_ms >= EVENT_WRITE_WARN_INTERVAL_MS
            && self
                .last_warn_ts_ms
                .compare_exchange(
                    last_warn_ts_ms,
                    failure_ts_ms,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
        {
            let suppressed = self.suppressed_warns.swap(0, Ordering::Relaxed);
            let snapshot = self.metrics.snapshot();
            warn!(
                consecutive_failures = snapshot.event_writer_consecutive_failures,
                last_failure_reason = %combined_reason,
                suppressed_warns = suppressed,
                "event writer failure suppressed from blocking indicator persistence/publish chain"
            );
        } else {
            self.suppressed_warns.fetch_add(1, Ordering::Relaxed);
        }
    }
}
