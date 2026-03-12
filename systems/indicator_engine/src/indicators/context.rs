use crate::runtime::state_store::{
    FundingChange, LatestFundingState, LatestMarkState, LevelAgg, MinuteHistory, MinuteWindowData,
    WindowBundle,
};
use chrono::{DateTime, Duration, Utc};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DivergenceSigTestMode {
    Threshold,
    BlockBootstrap,
}

impl DivergenceSigTestMode {
    pub fn from_str(value: &str) -> Self {
        match value {
            "block_bootstrap" => Self::BlockBootstrap,
            _ => Self::Threshold,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Threshold => "threshold",
            Self::BlockBootstrap => "block_bootstrap",
        }
    }
}

#[derive(Debug, Clone)]
pub struct IndicatorRuntimeOptions {
    pub whale_threshold_usdt: f64,
    pub kline_history_bars_1m: usize,
    pub kline_history_bars_15m: usize,
    pub kline_history_bars_4h: usize,
    pub kline_history_bars_1d: usize,
    pub kline_history_fill_1d_from_db: bool,
    pub fvg_windows: Vec<String>,
    pub fvg_fill_from_db: bool,
    pub fvg_db_bars_1h: usize,
    pub fvg_db_bars_4h: usize,
    pub fvg_db_bars_1d: usize,
    pub fvg_epsilon_gap_ticks: i64,
    pub fvg_atr_lookback: usize,
    pub fvg_min_body_ratio: f64,
    pub fvg_min_impulse_atr_ratio: f64,
    pub fvg_min_gap_atr_ratio: f64,
    pub fvg_max_gap_atr_ratio: f64,
    pub fvg_mitigated_fill_threshold: f64,
    pub fvg_invalid_close_bars: usize,
    pub tpo_rows_nb: usize,
    pub tpo_value_area_pct: f64,
    pub tpo_session_windows: Vec<String>,
    pub tpo_ib_minutes: i64,
    pub tpo_dev_output_windows: Vec<String>,
    pub rvwap_windows: Vec<String>,
    pub rvwap_output_windows: Vec<String>,
    pub rvwap_min_samples: usize,
    pub high_volume_pulse_z_windows: Vec<String>,
    pub high_volume_pulse_summary_windows: Vec<String>,
    pub high_volume_pulse_min_samples: usize,
    pub ema_base_periods: Vec<usize>,
    pub ema_htf_periods: Vec<usize>,
    pub ema_htf_windows: Vec<String>,
    pub ema_output_windows: Vec<String>,
    pub ema_fill_from_db: bool,
    pub ema_db_bars_4h: usize,
    pub ema_db_bars_1d: usize,
    pub divergence_sig_test_mode: DivergenceSigTestMode,
    pub divergence_bootstrap_b: usize,
    pub divergence_bootstrap_block_len: usize,
    pub divergence_p_value_threshold: f64,
    pub window_codes: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct IndicatorContext {
    pub ts_bucket: DateTime<Utc>,
    pub symbol: String,
    pub futures: MinuteWindowData,
    pub spot: MinuteWindowData,
    pub history_futures: Vec<MinuteHistory>,
    pub history_spot: Vec<MinuteHistory>,
    pub trade_history_futures: Vec<MinuteHistory>,
    pub trade_history_spot: Vec<MinuteHistory>,
    pub latest_mark: Option<LatestMarkState>,
    pub latest_funding: Option<LatestFundingState>,
    pub funding_changes_in_window: Vec<FundingChange>,
    pub funding_points_in_window: Vec<LatestFundingState>,
    pub mark_points_in_window: Vec<LatestMarkState>,
    pub funding_changes_recent: Vec<FundingChange>,
    pub funding_points_recent: Vec<LatestFundingState>,
    pub mark_points_recent: Vec<LatestMarkState>,
    pub whale_threshold_usdt: f64,
    pub kline_history_bars_1m: usize,
    pub kline_history_bars_15m: usize,
    pub kline_history_bars_4h: usize,
    pub kline_history_bars_1d: usize,
    pub kline_history_fill_1d_from_db: bool,
    pub fvg_windows: Vec<String>,
    pub fvg_fill_from_db: bool,
    pub fvg_db_bars_1h: usize,
    pub fvg_db_bars_4h: usize,
    pub fvg_db_bars_1d: usize,
    pub fvg_epsilon_gap_ticks: i64,
    pub fvg_atr_lookback: usize,
    pub fvg_min_body_ratio: f64,
    pub fvg_min_impulse_atr_ratio: f64,
    pub fvg_min_gap_atr_ratio: f64,
    pub fvg_max_gap_atr_ratio: f64,
    pub fvg_mitigated_fill_threshold: f64,
    pub fvg_invalid_close_bars: usize,
    pub kline_history_futures_1h_db: Vec<KlineHistoryBar>,
    pub kline_history_futures_4h_db: Vec<KlineHistoryBar>,
    pub kline_history_futures_1d_db: Vec<KlineHistoryBar>,
    pub kline_history_spot_4h_db: Vec<KlineHistoryBar>,
    pub kline_history_spot_1d_db: Vec<KlineHistoryBar>,
    pub tpo_rows_nb: usize,
    pub tpo_value_area_pct: f64,
    pub tpo_session_windows: Vec<String>,
    pub tpo_ib_minutes: i64,
    pub tpo_dev_output_windows: Vec<String>,
    pub rvwap_windows: Vec<String>,
    pub rvwap_output_windows: Vec<String>,
    pub rvwap_min_samples: usize,
    pub high_volume_pulse_z_windows: Vec<String>,
    pub high_volume_pulse_summary_windows: Vec<String>,
    pub high_volume_pulse_min_samples: usize,
    pub ema_base_periods: Vec<usize>,
    pub ema_htf_periods: Vec<usize>,
    pub ema_htf_windows: Vec<String>,
    pub ema_output_windows: Vec<String>,
    pub ema_fill_from_db: bool,
    pub ema_db_bars_4h: usize,
    pub ema_db_bars_1d: usize,
    pub divergence_sig_test_mode: DivergenceSigTestMode,
    pub divergence_bootstrap_b: usize,
    pub divergence_bootstrap_block_len: usize,
    pub divergence_p_value_threshold: f64,
    pub window_codes: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct KlineHistoryBar {
    pub open_time: DateTime<Utc>,
    pub close_time: DateTime<Utc>,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: Option<f64>,
    pub volume_base: f64,
    pub volume_quote: f64,
    pub is_closed: bool,
    pub minutes_covered: i64,
    pub expected_minutes: i64,
}

#[derive(Debug, Clone, Default)]
pub struct KlineHistorySupplement {
    pub futures_1h_db: Vec<KlineHistoryBar>,
    pub futures_4h_db: Vec<KlineHistoryBar>,
    pub futures_1d_db: Vec<KlineHistoryBar>,
    pub spot_4h_db: Vec<KlineHistoryBar>,
    pub spot_1d_db: Vec<KlineHistoryBar>,
}

impl IndicatorContext {
    pub fn from_bundle(
        bundle: &WindowBundle,
        options: &IndicatorRuntimeOptions,
        kline_history_supplement: KlineHistorySupplement,
    ) -> Self {
        Self {
            ts_bucket: bundle.ts_bucket,
            symbol: bundle.symbol.clone(),
            futures: bundle.futures.clone(),
            spot: bundle.spot.clone(),
            history_futures: bundle.history_futures.clone(),
            history_spot: bundle.history_spot.clone(),
            trade_history_futures: bundle.trade_history_futures.clone(),
            trade_history_spot: bundle.trade_history_spot.clone(),
            latest_mark: bundle.latest_mark.clone(),
            latest_funding: bundle.latest_funding.clone(),
            funding_changes_in_window: bundle.funding_changes_in_window.clone(),
            funding_points_in_window: bundle.funding_points_in_window.clone(),
            mark_points_in_window: bundle.mark_points_in_window.clone(),
            funding_changes_recent: bundle.funding_changes_recent.clone(),
            funding_points_recent: bundle.funding_points_recent.clone(),
            mark_points_recent: bundle.mark_points_recent.clone(),
            whale_threshold_usdt: options.whale_threshold_usdt,
            kline_history_bars_1m: options.kline_history_bars_1m,
            kline_history_bars_15m: options.kline_history_bars_15m,
            kline_history_bars_4h: options.kline_history_bars_4h,
            kline_history_bars_1d: options.kline_history_bars_1d,
            kline_history_fill_1d_from_db: options.kline_history_fill_1d_from_db,
            fvg_windows: options.fvg_windows.clone(),
            fvg_fill_from_db: options.fvg_fill_from_db,
            fvg_db_bars_1h: options.fvg_db_bars_1h,
            fvg_db_bars_4h: options.fvg_db_bars_4h,
            fvg_db_bars_1d: options.fvg_db_bars_1d,
            fvg_epsilon_gap_ticks: options.fvg_epsilon_gap_ticks,
            fvg_atr_lookback: options.fvg_atr_lookback,
            fvg_min_body_ratio: options.fvg_min_body_ratio,
            fvg_min_impulse_atr_ratio: options.fvg_min_impulse_atr_ratio,
            fvg_min_gap_atr_ratio: options.fvg_min_gap_atr_ratio,
            fvg_max_gap_atr_ratio: options.fvg_max_gap_atr_ratio,
            fvg_mitigated_fill_threshold: options.fvg_mitigated_fill_threshold,
            fvg_invalid_close_bars: options.fvg_invalid_close_bars,
            kline_history_futures_1h_db: kline_history_supplement.futures_1h_db,
            kline_history_futures_4h_db: kline_history_supplement.futures_4h_db,
            kline_history_futures_1d_db: kline_history_supplement.futures_1d_db,
            kline_history_spot_4h_db: kline_history_supplement.spot_4h_db,
            kline_history_spot_1d_db: kline_history_supplement.spot_1d_db,
            tpo_rows_nb: options.tpo_rows_nb,
            tpo_value_area_pct: options.tpo_value_area_pct,
            tpo_session_windows: options.tpo_session_windows.clone(),
            tpo_ib_minutes: options.tpo_ib_minutes,
            tpo_dev_output_windows: options.tpo_dev_output_windows.clone(),
            rvwap_windows: options.rvwap_windows.clone(),
            rvwap_output_windows: options.rvwap_output_windows.clone(),
            rvwap_min_samples: options.rvwap_min_samples,
            high_volume_pulse_z_windows: options.high_volume_pulse_z_windows.clone(),
            high_volume_pulse_summary_windows: options.high_volume_pulse_summary_windows.clone(),
            high_volume_pulse_min_samples: options.high_volume_pulse_min_samples,
            ema_base_periods: options.ema_base_periods.clone(),
            ema_htf_periods: options.ema_htf_periods.clone(),
            ema_htf_windows: options.ema_htf_windows.clone(),
            ema_output_windows: options.ema_output_windows.clone(),
            ema_fill_from_db: options.ema_fill_from_db,
            ema_db_bars_4h: options.ema_db_bars_4h,
            ema_db_bars_1d: options.ema_db_bars_1d,
            divergence_sig_test_mode: options.divergence_sig_test_mode,
            divergence_bootstrap_b: options.divergence_bootstrap_b,
            divergence_bootstrap_block_len: options.divergence_bootstrap_block_len,
            divergence_p_value_threshold: options.divergence_p_value_threshold,
            window_codes: options.window_codes.clone(),
        }
    }

    pub fn enabled_windows(&self) -> Vec<(String, i64)> {
        fn window_to_minutes(code: &str) -> Option<i64> {
            match code {
                "1m" => Some(1),
                "15m" => Some(15),
                "1h" => Some(60),
                "4h" => Some(240),
                "1d" => Some(1440),
                "3d" => Some(4320),
                _ => None,
            }
        }

        let mut out = self
            .window_codes
            .iter()
            .filter_map(|w| window_to_minutes(w.as_str()).map(|m| (w.clone(), m)))
            .collect::<Vec<_>>();
        if out.is_empty() {
            out.push(("1m".to_string(), 1));
        }
        out
    }

    pub fn volume_zscore_futures(&self, lookback: usize) -> Option<f64> {
        zscore(
            self.futures.total_qty,
            self.history_futures
                .iter()
                .rev()
                .skip(1)
                .take(lookback)
                .map(|h| h.total_qty)
                .collect(),
        )
    }

    pub fn cvd_slope_futures(&self, lookback: usize) -> Option<f64> {
        // Use the last `lookback` entries in chronological order (oldest→newest = index 0→N-1).
        // Previously called .rev().take() which flipped the time axis and inverted the slope sign:
        // a rising CVD trend would appear as a negative slope.
        let start = self.history_futures.len().saturating_sub(lookback);
        ols_slope(
            &self.history_futures[start..]
                .iter()
                .map(|h| h.cvd)
                .collect::<Vec<_>>(),
        )
    }

    pub fn cvd_slope_spot(&self, lookback: usize) -> Option<f64> {
        let start = self.history_spot.len().saturating_sub(lookback);
        ols_slope(
            &self.history_spot[start..]
                .iter()
                .map(|h| h.cvd)
                .collect::<Vec<_>>(),
        )
    }

    pub fn rolling_vpin(&self, lookback: usize) -> Option<f64> {
        let vals = self
            .history_futures
            .iter()
            .rev()
            .take(lookback)
            .map(|h| h.vpin)
            .collect::<Vec<_>>();
        mean(&vals)
    }

    pub fn rolling_avwap_gap(&self, lookback: usize) -> Option<f64> {
        let fut = self
            .history_futures
            .iter()
            .rev()
            .take(lookback)
            .filter_map(|h| h.avwap_minute)
            .collect::<Vec<_>>();
        let spot = self
            .history_spot
            .iter()
            .rev()
            .take(lookback)
            .filter_map(|h| h.avwap_minute)
            .collect::<Vec<_>>();
        if fut.is_empty() || spot.is_empty() {
            return None;
        }
        Some(
            fut.iter().sum::<f64>() / fut.len() as f64
                - spot.iter().sum::<f64>() / spot.len() as f64,
        )
    }

    pub fn cvd_pack_values(&self) -> (f64, f64, f64, f64) {
        let fut = self.history_futures.last().map(|v| v.cvd).unwrap_or(0.0);
        let spot = self.history_spot.last().map(|v| v.cvd).unwrap_or(0.0);
        let fut_delta = self.futures.delta;
        let spot_delta = self.spot.delta;
        (fut, spot, fut_delta, spot_delta)
    }

    pub fn atr_futures(&self, lookback: usize) -> Option<f64> {
        atr(&self.history_futures, lookback)
    }

    pub fn atr_spot(&self, lookback: usize) -> Option<f64> {
        atr(&self.history_spot, lookback)
    }

    pub fn window_minutes(&self, minutes: i64) -> (Vec<MinuteHistory>, Vec<MinuteHistory>) {
        let start = self.ts_bucket - Duration::minutes(minutes);
        let fut = self
            .history_futures
            .iter()
            .filter(|h| h.ts_bucket > start && h.ts_bucket <= self.ts_bucket)
            .cloned()
            .collect::<Vec<_>>();
        let spot = self
            .history_spot
            .iter()
            .filter(|h| h.ts_bucket > start && h.ts_bucket <= self.ts_bucket)
            .cloned()
            .collect::<Vec<_>>();
        (fut, spot)
    }

    pub fn funding_twa_1m(&self) -> Option<f64> {
        let start = self.ts_bucket;
        let end = self.ts_bucket + Duration::minutes(1);
        let mut points = self
            .funding_points_in_window
            .iter()
            .map(|p| (p.ts, p.funding_rate))
            .collect::<Vec<_>>();
        points.sort_by_key(|(ts, _)| *ts);
        let fallback = self
            .funding_points_recent
            .iter()
            .rev()
            .find(|p| p.ts <= start)
            .map(|p| p.funding_rate)
            .or_else(|| {
                self.funding_points_in_window
                    .iter()
                    .rev()
                    .find(|p| p.ts <= start)
                    .map(|p| p.funding_rate)
            });
        time_weighted_avg(start, end, &points, fallback)
    }

    pub fn mark_twap_1m(&self) -> Option<f64> {
        let start = self.ts_bucket;
        let end = self.ts_bucket + Duration::minutes(1);
        let mut points = self
            .mark_points_in_window
            .iter()
            .filter_map(|p| p.mark_price.map(|v| (p.ts, v)))
            .collect::<Vec<_>>();
        points.sort_by_key(|(ts, _)| *ts);
        let fallback = self
            .mark_points_recent
            .iter()
            .rev()
            .find_map(|p| (p.ts <= start).then_some(p.mark_price).flatten())
            .or_else(|| {
                self.mark_points_in_window
                    .iter()
                    .rev()
                    .find_map(|p| (p.ts <= start).then_some(p.mark_price).flatten())
            });
        time_weighted_avg(start, end, &points, fallback)
    }

    pub fn latest_funding_pair_at_or_before(
        &self,
        cutoff: DateTime<Utc>,
    ) -> Option<(DateTime<Utc>, f64)> {
        self.funding_points_recent
            .iter()
            .rev()
            .find(|p| p.ts <= cutoff)
            .map(|p| (p.ts, p.funding_rate))
            .or_else(|| {
                self.funding_points_in_window
                    .iter()
                    .rev()
                    .find(|p| p.ts <= cutoff)
                    .map(|p| (p.ts, p.funding_rate))
            })
    }

    pub fn latest_mark_pair_at_or_before(
        &self,
        cutoff: DateTime<Utc>,
    ) -> Option<(DateTime<Utc>, f64)> {
        self.mark_points_recent
            .iter()
            .rev()
            .find_map(|p| {
                (p.ts <= cutoff)
                    .then_some(p.mark_price.map(|v| (p.ts, v)))
                    .flatten()
            })
            .or_else(|| {
                self.mark_points_in_window.iter().rev().find_map(|p| {
                    (p.ts <= cutoff)
                        .then_some(p.mark_price.map(|v| (p.ts, v)))
                        .flatten()
                })
            })
    }
}

#[derive(Debug, Clone)]
pub struct IndicatorSnapshotRow {
    pub indicator_code: &'static str,
    pub window_code: &'static str,
    pub payload_json: Value,
}

#[derive(Debug, Clone)]
pub struct IndicatorLevelRow {
    pub indicator_code: &'static str,
    pub window_code: &'static str,
    pub price_level: f64,
    pub level_rank: Option<i32>,
    pub metrics_json: Value,
}

#[derive(Debug, Clone)]
pub struct IndicatorEventRow {
    pub event_id: String,
    pub indicator_code: &'static str,
    pub event_type: String,
    pub severity: String,
    pub direction: i16,
    pub ts_event_start: DateTime<Utc>,
    pub ts_event_end: Option<DateTime<Utc>>,
    pub event_available_ts: DateTime<Utc>,
    pub window_code: &'static str,
    pub pivot_ts_1: Option<DateTime<Utc>>,
    pub pivot_ts_2: Option<DateTime<Utc>>,
    pub pivot_confirm_ts_1: Option<DateTime<Utc>>,
    pub pivot_confirm_ts_2: Option<DateTime<Utc>>,
    pub sig_pass: Option<bool>,
    pub p_value: Option<f64>,
    pub score: Option<f64>,
    pub confidence: Option<f64>,
    pub payload_json: Value,
}

impl IndicatorEventRow {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        event_id: String,
        indicator_code: &'static str,
        event_type: String,
        severity: String,
        direction: i16,
        ts_event_start: DateTime<Utc>,
        ts_event_end: Option<DateTime<Utc>>,
        event_available_ts: DateTime<Utc>,
        window_code: &'static str,
        pivot_ts_1: Option<DateTime<Utc>>,
        pivot_ts_2: Option<DateTime<Utc>>,
        pivot_confirm_ts_1: Option<DateTime<Utc>>,
        pivot_confirm_ts_2: Option<DateTime<Utc>>,
        sig_pass: Option<bool>,
        p_value: Option<f64>,
        score: Option<f64>,
        confidence: Option<f64>,
        payload_json: Value,
    ) -> Self {
        Self {
            event_id,
            indicator_code,
            event_type,
            severity,
            direction,
            ts_event_start,
            ts_event_end,
            event_available_ts,
            window_code,
            pivot_ts_1,
            pivot_ts_2,
            pivot_confirm_ts_1,
            pivot_confirm_ts_2,
            sig_pass,
            p_value,
            score,
            confidence,
            payload_json,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DivergenceEventRow {
    pub event_id: String,
    pub divergence_type: String,
    pub pivot_side: String,
    pub event_available_ts: Option<DateTime<Utc>>,
    pub pivot_ts_1: Option<DateTime<Utc>>,
    pub pivot_ts_2: Option<DateTime<Utc>>,
    pub pivot_confirm_ts_1: Option<DateTime<Utc>>,
    pub pivot_confirm_ts_2: Option<DateTime<Utc>>,
    pub leg_minutes: Option<i32>,
    pub ts_event_start: DateTime<Utc>,
    pub ts_event_end: DateTime<Utc>,
    pub price_start: Option<f64>,
    pub price_end: Option<f64>,
    pub price_norm_diff: Option<f64>,
    pub cvd_start_fut: Option<f64>,
    pub cvd_end_fut: Option<f64>,
    pub cvd_norm_diff_fut: Option<f64>,
    pub cvd_start_spot: Option<f64>,
    pub cvd_end_spot: Option<f64>,
    pub cvd_norm_diff_spot: Option<f64>,
    pub price_effect_z: Option<f64>,
    pub cvd_effect_z: Option<f64>,
    pub sig_pass: Option<bool>,
    pub p_value_price: Option<f64>,
    pub p_value_cvd: Option<f64>,
    pub spot_price_flow_confirm: Option<bool>,
    pub fut_divergence_sign: Option<i16>,
    pub spot_lead_score: Option<f64>,
    pub likely_driver: Option<String>,
    pub score: Option<f64>,
    pub confidence: Option<f64>,
    pub window_code: &'static str,
    pub payload_json: Value,
}

impl DivergenceEventRow {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        event_id: String,
        divergence_type: String,
        pivot_side: String,
        event_available_ts: Option<DateTime<Utc>>,
        pivot_ts_1: Option<DateTime<Utc>>,
        pivot_ts_2: Option<DateTime<Utc>>,
        pivot_confirm_ts_1: Option<DateTime<Utc>>,
        pivot_confirm_ts_2: Option<DateTime<Utc>>,
        leg_minutes: Option<i32>,
        ts_event_start: DateTime<Utc>,
        ts_event_end: DateTime<Utc>,
        price_start: Option<f64>,
        price_end: Option<f64>,
        price_norm_diff: Option<f64>,
        cvd_start_fut: Option<f64>,
        cvd_end_fut: Option<f64>,
        cvd_norm_diff_fut: Option<f64>,
        cvd_start_spot: Option<f64>,
        cvd_end_spot: Option<f64>,
        cvd_norm_diff_spot: Option<f64>,
        price_effect_z: Option<f64>,
        cvd_effect_z: Option<f64>,
        sig_pass: Option<bool>,
        p_value_price: Option<f64>,
        p_value_cvd: Option<f64>,
        spot_price_flow_confirm: Option<bool>,
        fut_divergence_sign: Option<i16>,
        spot_lead_score: Option<f64>,
        likely_driver: Option<String>,
        score: Option<f64>,
        confidence: Option<f64>,
        window_code: &'static str,
        payload_json: Value,
    ) -> Self {
        Self {
            event_id,
            divergence_type,
            pivot_side,
            event_available_ts,
            pivot_ts_1,
            pivot_ts_2,
            pivot_confirm_ts_1,
            pivot_confirm_ts_2,
            leg_minutes,
            ts_event_start,
            ts_event_end,
            price_start,
            price_end,
            price_norm_diff,
            cvd_start_fut,
            cvd_end_fut,
            cvd_norm_diff_fut,
            cvd_start_spot,
            cvd_end_spot,
            cvd_norm_diff_spot,
            price_effect_z,
            cvd_effect_z,
            sig_pass,
            p_value_price,
            p_value_cvd,
            spot_price_flow_confirm,
            fut_divergence_sign,
            spot_lead_score,
            likely_driver,
            score,
            confidence,
            window_code,
            payload_json,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AbsorptionEventRow {
    pub event_id: String,
    pub event_type: String,
    pub direction: i16,
    pub ts_event_start: DateTime<Utc>,
    pub ts_event_end: DateTime<Utc>,
    pub confirm_ts: DateTime<Utc>,
    pub event_available_ts: DateTime<Utc>,
    pub trigger_side: Option<String>,
    pub pivot_price: Option<f64>,
    pub price_low: Option<f64>,
    pub price_high: Option<f64>,
    pub delta_sum: Option<f64>,
    pub rdelta_mean: Option<f64>,
    pub reject_ratio: Option<f64>,
    pub key_distance_ticks: Option<f64>,
    pub stacked_buy_imbalance: Option<bool>,
    pub stacked_sell_imbalance: Option<bool>,
    pub spot_rdelta_1m_mean: Option<f64>,
    pub spot_cvd_1m_change: Option<f64>,
    pub spot_flow_confirm_score: Option<f64>,
    pub spot_whale_confirm_score: Option<f64>,
    pub spot_confirm: Option<bool>,
    pub score_base: Option<f64>,
    pub score: Option<f64>,
    pub confidence: Option<f64>,
    pub window_code: &'static str,
    pub payload_json: Value,
}

#[derive(Debug, Clone)]
pub struct InitiationEventRow {
    pub event_id: String,
    pub event_type: String,
    pub direction: i16,
    pub ts_event_start: DateTime<Utc>,
    pub ts_event_end: DateTime<Utc>,
    pub confirm_ts: DateTime<Utc>,
    pub event_available_ts: DateTime<Utc>,
    pub pivot_price: Option<f64>,
    pub break_mag_ticks: Option<f64>,
    pub z_delta: Option<f64>,
    pub rdelta_mean: Option<f64>,
    pub min_follow_required_minutes: Option<i32>,
    pub follow_through_minutes: Option<i32>,
    pub follow_through_end_ts: Option<DateTime<Utc>>,
    pub follow_through_delta_sum: Option<f64>,
    pub follow_through_hold_ok: Option<bool>,
    pub follow_through_max_adverse_excursion_ticks: Option<f64>,
    pub spot_break_confirm: Option<bool>,
    pub spot_rdelta_1m_mean: Option<f64>,
    pub spot_cvd_change: Option<f64>,
    pub spot_whale_break_confirm: Option<bool>,
    pub score: Option<f64>,
    pub confidence: Option<f64>,
    pub window_code: &'static str,
    pub payload_json: Value,
}

#[derive(Debug, Clone)]
pub struct ExhaustionEventRow {
    pub event_id: String,
    pub event_type: String,
    pub direction: i16,
    pub ts_event_start: DateTime<Utc>,
    pub ts_event_end: DateTime<Utc>,
    pub confirm_ts: DateTime<Utc>,
    pub event_available_ts: DateTime<Utc>,
    pub pivot_ts_1: Option<DateTime<Utc>>,
    pub pivot_ts_2: Option<DateTime<Utc>>,
    pub pivot_confirm_ts_1: Option<DateTime<Utc>>,
    pub pivot_confirm_ts_2: Option<DateTime<Utc>>,
    pub price_push_ticks: Option<f64>,
    pub delta_change: Option<f64>,
    pub rdelta_change: Option<f64>,
    pub reject_ratio: Option<f64>,
    pub confirm_speed: Option<f64>,
    pub spot_cvd_push_post_pivot: Option<f64>,
    pub spot_whale_push_post_pivot: Option<f64>,
    pub spot_continuation_risk: Option<bool>,
    pub spot_exhaustion_confirm: Option<bool>,
    pub score: Option<f64>,
    pub confidence: Option<f64>,
    pub window_code: &'static str,
    pub payload_json: Value,
}

#[derive(Debug, Clone)]
pub struct LiquidationLevelRow {
    pub window_code: &'static str,
    pub price_level: f64,
    pub long_liq_density: f64,
    pub short_liq_density: f64,
    pub net_liq_density: f64,
    pub is_long_peak: bool,
    pub is_short_peak: bool,
    pub long_peak_score: Option<f64>,
    pub short_peak_score: Option<f64>,
    pub extra_json: Value,
}

#[derive(Debug, Clone, Default)]
pub struct IndicatorComputation {
    pub snapshot: Option<IndicatorSnapshotRow>,
    pub level_rows: Vec<IndicatorLevelRow>,
    pub event_rows: Vec<IndicatorEventRow>,
    pub divergence_rows: Vec<DivergenceEventRow>,
    pub absorption_rows: Vec<AbsorptionEventRow>,
    pub initiation_rows: Vec<InitiationEventRow>,
    pub exhaustion_rows: Vec<ExhaustionEventRow>,
    pub liquidation_rows: Vec<LiquidationLevelRow>,
}

pub fn mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    Some(values.iter().sum::<f64>() / values.len() as f64)
}

pub fn median(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut v = values.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = v.len();
    if n % 2 == 1 {
        Some(v[n / 2])
    } else {
        Some((v[n / 2 - 1] + v[n / 2]) / 2.0)
    }
}

pub fn clip01(x: f64) -> f64 {
    x.clamp(0.0, 1.0)
}

pub fn stddev(values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    let m = mean(values)?;
    let var = values
        .iter()
        .map(|v| {
            let d = v - m;
            d * d
        })
        .sum::<f64>()
        / values.len() as f64;
    Some(var.sqrt())
}

pub fn zscore(current: f64, history: Vec<f64>) -> Option<f64> {
    if history.len() < 5 {
        return None;
    }
    let m = mean(&history)?;
    let sd = stddev(&history)?;
    if sd <= 1e-12 {
        return Some(0.0);
    }
    Some((current - m) / sd)
}

pub fn robust_z_at(values: &[f64], idx: usize, lookback: usize) -> Option<f64> {
    if idx + 1 < lookback || lookback < 5 {
        return None;
    }
    let start = idx + 1 - lookback;
    let win = &values[start..=idx];
    let med = median(win)?;
    let abs_dev = win.iter().map(|v| (v - med).abs()).collect::<Vec<_>>();
    let mad = median(&abs_dev)?;
    let denom = 1.4826 * mad + 1e-12;
    Some((values[idx] - med) / denom)
}

pub fn atr(history: &[MinuteHistory], lookback: usize) -> Option<f64> {
    if history.len() < 2 || lookback < 2 {
        return None;
    }
    let tail = history
        .iter()
        .rev()
        .take(lookback)
        .cloned()
        .collect::<Vec<_>>();
    let mut rows = tail.into_iter().rev().collect::<Vec<_>>();
    if rows.len() < 2 {
        return None;
    }
    let mut trs = Vec::with_capacity(rows.len());
    let mut prev_close = rows
        .first()
        .and_then(|h| h.close_price)
        .or_else(|| rows.first().and_then(|h| h.last_price))?;
    for row in rows.drain(..) {
        let h = row.high_price.or(row.last_price)?;
        let l = row.low_price.or(row.last_price)?;
        let tr = (h - l)
            .max((h - prev_close).abs())
            .max((l - prev_close).abs());
        trs.push(tr);
        prev_close = row.close_price.or(row.last_price).unwrap_or(prev_close);
    }
    mean(&trs)
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

pub fn ols_slope(values: &[f64]) -> Option<f64> {
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
        return None;
    }
    Some(num / den)
}

pub fn top_profile_levels(
    profile: &std::collections::BTreeMap<i64, LevelAgg>,
    n: usize,
) -> Vec<(i64, LevelAgg)> {
    const PROFILE_RANK_REL_EPSILON: f64 = 1e-9;

    let mut rows = profile
        .iter()
        .map(|(p, l)| (*p, l.clone()))
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| {
        let a_total = a.1.total();
        let b_total = b.1.total();
        let scale = a_total.abs().max(b_total.abs()).max(1.0);
        if (a_total - b_total).abs() <= PROFILE_RANK_REL_EPSILON * scale {
            a.0.cmp(&b.0)
        } else {
            b_total
                .partial_cmp(&a_total)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        }
    });
    rows.into_iter().take(n).collect()
}
