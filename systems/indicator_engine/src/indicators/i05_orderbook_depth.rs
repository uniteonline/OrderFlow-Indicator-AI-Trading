use crate::indicators::context::{
    clip01, mean, ols_slope, zscore, IndicatorComputation, IndicatorContext, IndicatorLevelRow,
    IndicatorSnapshotRow,
};
use crate::indicators::indicator_trait::Indicator;
use crate::runtime::state_store::{tick_to_price, BookLevelAgg, MinuteHistory};
use chrono::Duration;
use serde_json::{json, Map, Value};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};

const OFI_NORM_LOOKBACK: usize = 60;
const WINDOW_SPECS: [(&str, i64); 2] = [("15m", 15), ("1h", 60)];
const RAW_AUDIT_TOP_TOTAL_LEVELS: usize = 64;
const RAW_AUDIT_TOP_ABS_NET_LEVELS: usize = 32;
const RAW_AUDIT_TOP_BID_LEVELS: usize = 16;
const RAW_AUDIT_TOP_ASK_LEVELS: usize = 16;

pub struct I05OrderbookDepth;

impl Indicator for I05OrderbookDepth {
    fn code(&self) -> &'static str {
        "orderbook_depth"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation {
        let fut_obi_k_twa = ctx.futures.obi_k_twa.or(ctx.futures.obi_twa);
        let spot_obi_k_dw_twa = ctx.spot.obi_k_dw_twa.or(ctx.spot.obi_twa);
        let spot_confirms = spot_obi_k_dw_twa
            .zip(ctx.futures.obi_k_dw_twa.or(ctx.futures.obi_twa))
            .map(|(s, f)| s.signum() == f.signum())
            .unwrap_or(false);
        let exec_confirm = ctx.futures.delta.signum() == fut_obi_k_twa.unwrap_or(0.0).signum();
        let fake_order_risk = (fut_obi_k_twa.unwrap_or(0.0).abs()
            * (1.0 - ctx.futures.relative_delta.abs()))
        .max(0.0);
        let weak_price_resp = previous_close(&ctx.history_futures)
            .zip(ctx.futures.last_price)
            .map(|(prev_close, last_price)| {
                (last_price - prev_close).abs() <= 0.02
                    && ctx.futures.obi_k_dw_twa.unwrap_or(0.0).abs() >= 0.4
            })
            .unwrap_or(false);
        let spot_driven_divergence_flag = ctx.spot.delta.abs() > (ctx.futures.delta.abs() * 1.2)
            && ctx.spot.delta.signum() != ctx.futures.delta.signum();
        let cross_cvd_attribution = ctx.spot.delta - ctx.futures.delta;
        let obi_shock_fut = ctx.futures.obi_k_dw_change.map(f64::abs);
        let ofi_norm_fut = ofi_norm(ctx.futures.ofi, &ctx.history_futures);
        let ofi_norm_spot = ofi_norm(ctx.spot.ofi, &ctx.history_spot);
        let obi_k_dw_slope_fut = ols_slope(
            &ctx.history_futures
                .iter()
                .filter_map(|h| h.obi_k_dw_twa)
                .collect::<Vec<_>>(),
        );
        let heatmap_summary_fut = heatmap_summary(&ctx.futures.heatmap);

        let heatmap_entries = ctx
            .futures
            .heatmap
            .iter()
            .enumerate()
            .map(|(idx, (tick, level))| (*tick, (idx + 1) as i32, level.clone()))
            .collect::<Vec<_>>();
        let raw_audit_ticks = select_heatmap_audit_ticks(&heatmap_entries);
        let peak_bid_tick = heatmap_entries
            .iter()
            .max_by(|a, b| {
                a.2.bid_liquidity
                    .partial_cmp(&b.2.bid_liquidity)
                    .unwrap_or(Ordering::Equal)
            })
            .map(|(tick, _, _)| *tick);
        let peak_ask_tick = heatmap_entries
            .iter()
            .max_by(|a, b| {
                a.2.ask_liquidity
                    .partial_cmp(&b.2.ask_liquidity)
                    .unwrap_or(Ordering::Equal)
            })
            .map(|(tick, _, _)| *tick);
        let peak_total_tick = heatmap_entries
            .iter()
            .max_by(|a, b| {
                a.2.total()
                    .partial_cmp(&b.2.total())
                    .unwrap_or(Ordering::Equal)
            })
            .map(|(tick, _, _)| *tick);
        let peak_abs_net_tick = heatmap_entries
            .iter()
            .max_by(|a, b| {
                a.2.net()
                    .abs()
                    .partial_cmp(&b.2.net().abs())
                    .unwrap_or(Ordering::Equal)
            })
            .map(|(tick, _, _)| *tick);

        let level_rows = heatmap_entries
            .iter()
            .filter(|(tick, _, _)| raw_audit_ticks.contains(tick))
            .map(|(tick, rank, level)| IndicatorLevelRow {
                indicator_code: self.code(),
                window_code: "1m",
                price_level: tick_to_price(*tick),
                level_rank: Some(*rank),
                metrics_json: json!({
                    "bid_liquidity": level.bid_liquidity,
                    "ask_liquidity": level.ask_liquidity,
                    "total_liquidity": level.total(),
                    "net_liquidity": level.net(),
                    "level_imbalance": level.imbalance(),
                    "is_peak_bid": Some(*tick) == peak_bid_tick,
                    "is_peak_ask": Some(*tick) == peak_ask_tick,
                    "is_peak_total": Some(*tick) == peak_total_tick,
                    "is_peak_abs_net": Some(*tick) == peak_abs_net_tick,
                    "audit_capture_policy": "top_liquidity_structural_subset"
                }),
            })
            .collect::<Vec<_>>();

        let levels = heatmap_entries
            .iter()
            .map(|(tick, rank, level)| {
                json!({
                    "price_level": tick_to_price(*tick),
                    "level_rank": rank,
                    "bid_liquidity": level.bid_liquidity,
                    "ask_liquidity": level.ask_liquidity,
                    "total_liquidity": level.total(),
                    "net_liquidity": level.net(),
                    "level_imbalance": level.imbalance()
                })
            })
            .collect::<Vec<_>>();

        let mut by_window = Map::new();
        for (window_code, window_minutes) in WINDOW_SPECS {
            let fut_rows = window_rows(&ctx.history_futures, ctx.ts_bucket, window_minutes);
            let spot_rows = window_rows(&ctx.history_spot, ctx.ts_bucket, window_minutes);
            by_window.insert(
                window_code.to_string(),
                build_window_payload(ctx, &fut_rows, &spot_rows, window_minutes),
            );
        }

        IndicatorComputation {
            snapshot: Some(IndicatorSnapshotRow {
                indicator_code: self.code(),
                window_code: "1m",
                payload_json: json!({
                    "depth_k": ctx.futures.depth_k,
                    "levels": levels,
                    "heatmap_summary_fut": heatmap_summary_fut,
                    "spread_twa_fut": ctx.futures.spread_twa,
                    "topk_depth_twa_fut": ctx.futures.topk_depth_twa,
                    "obi": fut_obi_k_twa,
                    "obi_fut": fut_obi_k_twa,
                    "obi_l1_twa_fut": ctx.futures.obi_l1_twa,
                    "obi_k_twa_fut": fut_obi_k_twa,
                    "obi_k_dw_twa_fut": ctx.futures.obi_k_dw_twa,
                    "obi_k_dw_close_fut": ctx.futures.obi_k_dw_close,
                    "obi_k_dw_change_fut": ctx.futures.obi_k_dw_change,
                    "obi_k_dw_slope_fut": obi_k_dw_slope_fut,
                    "obi_k_dw_adj_twa_fut": ctx.futures.obi_k_dw_adj_twa,
                    "ofi_fut": ctx.futures.ofi,
                    "ofi_norm_fut": ofi_norm_fut,
                    "bbo_updates_fut": ctx.futures.bbo_updates,
                    "microprice_fut": ctx.futures.microprice_twa,
                    "microprice_classic_fut": ctx.futures.microprice_classic_twa,
                    "microprice_kappa_fut": ctx.futures.microprice_kappa_twa,
                    "microprice_adj_fut": ctx.futures.microprice_adj_twa,
                    "spread_twa_spot": ctx.spot.spread_twa,
                    "topk_depth_twa_spot": ctx.spot.topk_depth_twa,
                    "obi_k_dw_twa_spot": spot_obi_k_dw_twa,
                    "ofi_spot": ctx.spot.ofi,
                    "ofi_norm_spot": ofi_norm_spot,
                    "trade_delta_spot": ctx.spot.delta,
                    "relative_delta_spot": ctx.spot.relative_delta,
                    "cvd_slope_spot": ctx.cvd_slope_spot(20),
                    "exec_confirm_fut": exec_confirm,
                    "spot_confirm": spot_confirms,
                    "obi_shock_fut": obi_shock_fut,
                    "weak_price_resp_fut": weak_price_resp,
                    "fake_order_risk_fut": fake_order_risk,
                    "spot_driven_divergence_flag": spot_driven_divergence_flag,
                    "cross_cvd_attribution": cross_cvd_attribution,
                    "by_window": by_window,
                }),
            }),
            level_rows,
            ..Default::default()
        }
    }
}

fn select_heatmap_audit_ticks(entries: &[(i64, i32, BookLevelAgg)]) -> HashSet<i64> {
    let mut out = HashSet::new();

    let mut by_total = entries
        .iter()
        .map(|(tick, _, level)| (*tick, level.total()))
        .collect::<Vec<_>>();
    by_total.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    for (tick, _) in by_total.into_iter().take(RAW_AUDIT_TOP_TOTAL_LEVELS) {
        out.insert(tick);
    }

    let mut by_abs_net = entries
        .iter()
        .map(|(tick, _, level)| (*tick, level.net().abs()))
        .collect::<Vec<_>>();
    by_abs_net.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    for (tick, _) in by_abs_net.into_iter().take(RAW_AUDIT_TOP_ABS_NET_LEVELS) {
        out.insert(tick);
    }

    let mut by_bid = entries
        .iter()
        .map(|(tick, _, level)| (*tick, level.bid_liquidity))
        .collect::<Vec<_>>();
    by_bid.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    for (tick, _) in by_bid.into_iter().take(RAW_AUDIT_TOP_BID_LEVELS) {
        out.insert(tick);
    }

    let mut by_ask = entries
        .iter()
        .map(|(tick, _, level)| (*tick, level.ask_liquidity))
        .collect::<Vec<_>>();
    by_ask.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    for (tick, _) in by_ask.into_iter().take(RAW_AUDIT_TOP_ASK_LEVELS) {
        out.insert(tick);
    }

    out
}

fn build_window_payload(
    ctx: &IndicatorContext,
    futures_rows: &[&MinuteHistory],
    spot_rows: &[&MinuteHistory],
    window_minutes: i64,
) -> Value {
    let requested_minutes = window_minutes.max(1);
    let returned_minutes = futures_rows.len() as i64;
    let missing_minutes = requested_minutes.saturating_sub(returned_minutes);
    let coverage_ratio = clip01(returned_minutes as f64 / requested_minutes as f64);
    let current_window_ofi_fut = futures_rows.iter().map(|row| row.ofi).sum::<f64>();
    let current_window_ofi_spot = spot_rows.iter().map(|row| row.ofi).sum::<f64>();
    let fut_obi = avg_metric(futures_rows, |row| row.obi_k_dw_twa.or(row.obi_twa));
    let spot_obi = avg_metric(spot_rows, |row| row.obi_k_dw_twa.or(row.obi_twa));
    let fut_delta = futures_rows.iter().map(|row| row.delta).sum::<f64>();
    let spot_delta = spot_rows.iter().map(|row| row.delta).sum::<f64>();
    let fut_qty = futures_rows.iter().map(|row| row.total_qty).sum::<f64>();
    let spot_qty = spot_rows.iter().map(|row| row.total_qty).sum::<f64>();
    let relative_delta_spot = if spot_qty > 0.0 {
        spot_delta / spot_qty
    } else {
        0.0
    };
    let fake_order_risk_fut = (fut_obi.unwrap_or(0.0).abs()
        * (1.0
            - if fut_qty > 0.0 {
                (fut_delta / fut_qty).abs()
            } else {
                0.0
            }))
    .max(0.0);

    let first_close_fut = futures_rows
        .first()
        .and_then(|row| row.close_price.or(row.last_price).or(row.open_price));
    let last_close_fut = futures_rows
        .last()
        .and_then(|row| row.close_price.or(row.last_price).or(row.open_price));
    let weak_price_resp_fut = first_close_fut
        .zip(last_close_fut)
        .map(|(open, close)| (close - open).abs() <= 0.02 && fut_obi.unwrap_or(0.0).abs() >= 0.4)
        .unwrap_or(false);

    let obi_series = futures_rows
        .iter()
        .filter_map(|row| row.obi_k_dw_twa.or(row.obi_twa))
        .collect::<Vec<_>>();
    let obi_k_dw_change_fut = obi_series
        .first()
        .zip(obi_series.last())
        .map(|(first, last)| last - first);
    let obi_shock_fut = obi_k_dw_change_fut.map(f64::abs);

    json!({
        "requested_minutes": requested_minutes,
        "returned_minutes": returned_minutes,
        "missing_minutes": missing_minutes,
        "coverage_ratio": coverage_ratio,
        "is_ready": returned_minutes >= requested_minutes,
        "spread_twa_fut": avg_metric(futures_rows, |row| row.spread_twa),
        "topk_depth_twa_fut": avg_metric(futures_rows, |row| row.topk_depth_twa),
        "obi_fut": fut_obi,
        "obi_l1_twa_fut": avg_metric(futures_rows, |row| row.obi_l1_twa),
        "obi_k_twa_fut": avg_metric(futures_rows, |row| row.obi_k_twa.or(row.obi_twa)),
        "obi_k_dw_twa_fut": fut_obi,
        "obi_k_dw_close_fut": futures_rows.last().and_then(|row| row.obi_k_dw_close.or(row.obi_k_dw_twa).or(row.obi_twa)),
        "obi_k_dw_change_fut": obi_k_dw_change_fut,
        "obi_k_dw_slope_fut": ols_slope(&obi_series),
        "obi_k_dw_adj_twa_fut": avg_metric(futures_rows, |row| row.obi_k_dw_adj_twa),
        "ofi_fut": current_window_ofi_fut,
        "ofi_norm_fut": window_ofi_norm(&ctx.history_futures, ctx.ts_bucket, window_minutes, current_window_ofi_fut),
        "bbo_updates_fut": futures_rows.iter().map(|row| row.bbo_updates).sum::<i64>(),
        "microprice_fut": avg_metric(futures_rows, |row| row.microprice_twa),
        "microprice_classic_fut": avg_metric(futures_rows, |row| row.microprice_classic_twa),
        "microprice_kappa_fut": avg_metric(futures_rows, |row| row.microprice_kappa_twa),
        "microprice_adj_fut": avg_metric(futures_rows, |row| row.microprice_adj_twa),
        "spread_twa_spot": avg_metric(spot_rows, |row| row.spread_twa),
        "topk_depth_twa_spot": avg_metric(spot_rows, |row| row.topk_depth_twa),
        "obi_k_dw_twa_spot": spot_obi,
        "ofi_spot": current_window_ofi_spot,
        "ofi_norm_spot": window_ofi_norm(&ctx.history_spot, ctx.ts_bucket, window_minutes, current_window_ofi_spot),
        "trade_delta_spot": spot_delta,
        "relative_delta_spot": relative_delta_spot,
        "cvd_slope_spot": ols_slope(&spot_rows.iter().map(|row| row.cvd).collect::<Vec<_>>()),
        "exec_confirm_fut": fut_delta.signum() == fut_obi.unwrap_or(0.0).signum(),
        "spot_confirm": spot_obi
            .zip(fut_obi)
            .map(|(spot, fut)| spot.signum() == fut.signum())
            .unwrap_or(false),
        "obi_shock_fut": obi_shock_fut,
        "weak_price_resp_fut": weak_price_resp_fut,
        "fake_order_risk_fut": fake_order_risk_fut,
        "spot_driven_divergence_flag": spot_delta.abs() > (fut_delta.abs() * 1.2)
            && spot_delta.signum() != fut_delta.signum(),
        "cross_cvd_attribution": spot_delta - fut_delta,
    })
}

fn previous_close(history: &[MinuteHistory]) -> Option<f64> {
    history
        .iter()
        .rev()
        .skip(1)
        .find_map(|h| h.close_price.or(h.last_price))
}

fn ofi_norm(current_ofi: f64, history: &[MinuteHistory]) -> Option<f64> {
    let values = history
        .iter()
        .rev()
        .skip(1)
        .take(OFI_NORM_LOOKBACK)
        .map(|h| h.ofi)
        .collect::<Vec<_>>();
    zscore(current_ofi, values)
}

fn window_ofi_norm(
    history: &[MinuteHistory],
    ts_bucket: chrono::DateTime<chrono::Utc>,
    window_minutes: i64,
    current_sum: f64,
) -> Option<f64> {
    let current_end = ts_bucket;
    let current_start = current_end - Duration::minutes(window_minutes);
    let mut samples = Vec::new();

    for idx in 1..=OFI_NORM_LOOKBACK {
        let sample_end = current_start - Duration::minutes(window_minutes * (idx as i64 - 1));
        let sample_start = sample_end - Duration::minutes(window_minutes);
        let sample = history
            .iter()
            .filter(|row| row.ts_bucket > sample_start && row.ts_bucket <= sample_end)
            .map(|row| row.ofi)
            .sum::<f64>();
        if sample.abs() > 1e-12 {
            samples.push(sample);
        }
    }

    zscore(current_sum, samples)
}

fn avg_metric<T>(rows: &[&MinuteHistory], extractor: T) -> Option<f64>
where
    T: Fn(&MinuteHistory) -> Option<f64>,
{
    let values = rows
        .iter()
        .filter_map(|row| extractor(row))
        .collect::<Vec<_>>();
    mean(&values)
}

fn window_rows<'a>(
    history: &'a [MinuteHistory],
    ts_bucket: chrono::DateTime<chrono::Utc>,
    window_minutes: i64,
) -> Vec<&'a MinuteHistory> {
    let start = ts_bucket - Duration::minutes(window_minutes);
    history
        .iter()
        .filter(|row| row.ts_bucket > start && row.ts_bucket <= ts_bucket)
        .collect()
}

fn heatmap_summary(heatmap: &BTreeMap<i64, BookLevelAgg>) -> Value {
    let levels = heatmap.len();
    let peak_bid = heatmap
        .iter()
        .max_by(|a, b| {
            a.1.bid_liquidity
                .partial_cmp(&b.1.bid_liquidity)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|(tick, level)| {
            json!({
                "price_level": tick_to_price(*tick),
                "bid_liquidity": level.bid_liquidity
            })
        });
    let peak_ask = heatmap
        .iter()
        .max_by(|a, b| {
            a.1.ask_liquidity
                .partial_cmp(&b.1.ask_liquidity)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|(tick, level)| {
            json!({
                "price_level": tick_to_price(*tick),
                "ask_liquidity": level.ask_liquidity
            })
        });
    let peak_total = heatmap
        .iter()
        .max_by(|a, b| {
            a.1.total()
                .partial_cmp(&b.1.total())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|(tick, level)| {
            json!({
                "price_level": tick_to_price(*tick),
                "total_liquidity": level.total()
            })
        });
    let max_abs_net = heatmap
        .iter()
        .max_by(|a, b| {
            a.1.net()
                .abs()
                .partial_cmp(&b.1.net().abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|(tick, level)| {
            json!({
                "price_level": tick_to_price(*tick),
                "net_liquidity": level.net(),
                "level_imbalance": level.imbalance()
            })
        });

    json!({
        "levels": levels,
        "peak_bid": peak_bid,
        "peak_ask": peak_ask,
        "peak_total": peak_total,
        "max_abs_net": max_abs_net,
        "coverage_ratio": clip01((levels as f64) / 50.0)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orderbook_raw_audit_ticks_are_bounded() {
        let entries = (1..=300_i64)
            .enumerate()
            .map(|(idx, tick)| {
                (
                    tick,
                    (idx + 1) as i32,
                    BookLevelAgg {
                        bid_liquidity: (301 - tick) as f64,
                        ask_liquidity: (tick % 23 + 1) as f64,
                    },
                )
            })
            .collect::<Vec<_>>();

        let ticks = select_heatmap_audit_ticks(&entries);
        assert!(ticks.contains(&1));
        assert!(ticks.contains(&22));
        assert!(
            ticks.len()
                <= RAW_AUDIT_TOP_TOTAL_LEVELS
                    + RAW_AUDIT_TOP_ABS_NET_LEVELS
                    + RAW_AUDIT_TOP_BID_LEVELS
                    + RAW_AUDIT_TOP_ASK_LEVELS
        );
    }
}
