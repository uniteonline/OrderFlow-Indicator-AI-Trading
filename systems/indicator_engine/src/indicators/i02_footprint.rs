use crate::indicators::context::{
    IndicatorComputation, IndicatorContext, IndicatorLevelRow, IndicatorSnapshotRow,
};
use crate::indicators::indicator_trait::Indicator;
use crate::runtime::state_store::{tick_to_price, LevelAgg};
use serde_json::json;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};

const MULTI_WINDOWS: &[(&str, usize)] = &[
    ("15m", 15),
    ("1h", 60),
    ("4h", 240),
    ("1d", 1440),
    ("3d", 4320),
];
const RAW_AUDIT_TOP_TOTAL_LEVELS: usize = 64;
const RAW_AUDIT_TOP_ABS_DELTA_LEVELS: usize = 32;
const RAW_AUDIT_TOP_BUY_LEVELS: usize = 16;
const RAW_AUDIT_TOP_SELL_LEVELS: usize = 16;

pub struct I02Footprint;

#[derive(Clone)]
struct FootprintAuditLevel {
    tick: i64,
    rank: i32,
    agg: LevelAgg,
    is_buy_imbalance: bool,
    is_sell_imbalance: bool,
    is_open: bool,
    is_close: bool,
    is_high: bool,
    is_low: bool,
    ua_top_flag: bool,
    ua_bottom_flag: bool,
}

impl Indicator for I02Footprint {
    fn code(&self) -> &'static str {
        "footprint"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation {
        const IMBALANCE_RATIO_THRESHOLD: f64 = 3.0;
        const MIN_STACK_LEVELS: i32 = 3;
        const EPS: f64 = 1e-12;

        let mut ticks = ctx.futures.profile.keys().copied().collect::<Vec<_>>();
        ticks.sort_unstable();

        let mut buy_imb = Vec::new();
        let mut sell_imb = Vec::new();

        let open_tick = ctx.futures.first_price.map(|v| (v * 100.0).round() as i64);
        let close_tick = ctx.futures.last_price.map(|v| (v * 100.0).round() as i64);
        let high_trade_tick = ctx.futures.high_price.map(|v| (v * 100.0).round() as i64);
        let low_trade_tick = ctx.futures.low_price.map(|v| (v * 100.0).round() as i64);
        let high_tick = ticks.iter().max().copied();
        let low_tick = ticks.iter().min().copied();
        let ua_top = high_tick
            .and_then(|t| ctx.futures.profile.get(&t))
            .map(|l| l.buy_qty > 0.0 && l.sell_qty > 0.0)
            .unwrap_or(false);
        let ua_bottom = low_tick
            .and_then(|t| ctx.futures.profile.get(&t))
            .map(|l| l.buy_qty > 0.0 && l.sell_qty > 0.0)
            .unwrap_or(false);
        let mut levels = Vec::new();
        let mut audit_levels = Vec::new();

        for (idx, tick) in ticks.iter().enumerate() {
            let cur = ctx.futures.profile.get(tick).cloned().unwrap_or_default();
            let prev = ctx
                .futures
                .profile
                .get(&(tick - 1))
                .cloned()
                .unwrap_or_default();
            let next = ctx
                .futures
                .profile
                .get(&(tick + 1))
                .cloned()
                .unwrap_or_default();

            let buy_ratio = cur.buy_qty / (prev.sell_qty + EPS);
            let sell_ratio = cur.sell_qty / (next.buy_qty + EPS);

            let is_buy_imb = buy_ratio >= IMBALANCE_RATIO_THRESHOLD && cur.buy_qty > 0.0;
            let is_sell_imb = sell_ratio >= IMBALANCE_RATIO_THRESHOLD && cur.sell_qty > 0.0;

            if is_buy_imb {
                buy_imb.push(*tick);
            }
            if is_sell_imb {
                sell_imb.push(*tick);
            }

            let is_open = open_tick.map(|v| *tick == v).unwrap_or(false);
            let is_close = close_tick.map(|v| *tick == v).unwrap_or(false);
            let is_high = high_trade_tick.map(|v| *tick == v).unwrap_or(false);
            let is_low = low_trade_tick.map(|v| *tick == v).unwrap_or(false);
            let ua_top_flag = high_tick.map(|h| h == *tick).unwrap_or(false) && ua_top;
            let ua_bottom_flag = low_tick.map(|l| l == *tick).unwrap_or(false) && ua_bottom;

            levels.push(json!({
                "price_level": tick_to_price(*tick),
                "level_rank": (idx + 1) as i32,
                "buy": cur.buy_qty,
                "sell": cur.sell_qty,
                "delta": cur.delta(),
                "total": cur.total(),
                "buy_imbalance": is_buy_imb,
                "sell_imbalance": is_sell_imb,
                "is_open": is_open,
                "is_close": is_close,
                "is_high": is_high,
                "is_low": is_low,
                "ua_top_flag": ua_top_flag,
                "ua_bottom_flag": ua_bottom_flag,
            }));

            audit_levels.push(FootprintAuditLevel {
                tick: *tick,
                rank: (idx + 1) as i32,
                agg: cur,
                is_buy_imbalance: is_buy_imb,
                is_sell_imbalance: is_sell_imb,
                is_open,
                is_close,
                is_high,
                is_low,
                ua_top_flag,
                ua_bottom_flag,
            });
        }

        let max_buy_stack = longest_consecutive(&buy_imb);
        let max_sell_stack = longest_consecutive(&sell_imb);
        let buy_stacks = contiguous_segments(&buy_imb, MIN_STACK_LEVELS);
        let sell_stacks = contiguous_segments(&sell_imb, MIN_STACK_LEVELS);
        let raw_audit_ticks = select_footprint_audit_ticks(&audit_levels);
        let level_rows = audit_levels
            .iter()
            .filter(|level| raw_audit_ticks.contains(&level.tick))
            .map(|level| IndicatorLevelRow {
                indicator_code: self.code(),
                window_code: "1m",
                price_level: tick_to_price(level.tick),
                level_rank: Some(level.rank),
                metrics_json: json!({
                    "buy": level.agg.buy_qty,
                    "sell": level.agg.sell_qty,
                    "delta": level.agg.delta(),
                    "total": level.agg.total(),
                    "buy_imbalance": level.is_buy_imbalance,
                    "sell_imbalance": level.is_sell_imbalance,
                    "is_open": level.is_open,
                    "is_close": level.is_close,
                    "is_high": level.is_high,
                    "is_low": level.is_low,
                    "ua_top_flag": level.ua_top_flag,
                    "ua_bottom_flag": level.ua_bottom_flag,
                    "audit_capture_policy": "structural_imbalance_subset",
                }),
            })
            .collect::<Vec<_>>();

        let mut by_window = serde_json::Map::new();
        let avail = ctx.history_futures.len();

        for &(wcode, bar_count) in MULTI_WINDOWS {
            if avail == 0 {
                continue;
            }
            // `history_futures` includes the current minute as its last element.
            // Exclude that element because current bar values are already sourced from
            // `ctx.futures` in this computation.
            let hist_end = avail.saturating_sub(1);
            let history_take = bar_count.saturating_sub(1).min(hist_end);
            let hist_start = hist_end.saturating_sub(history_take);
            let history_window = &ctx.history_futures[hist_start..hist_end];

            let mut agg: BTreeMap<i64, LevelAgg> = BTreeMap::new();
            for (&t, l) in &ctx.futures.profile {
                let e = agg.entry(t).or_default();
                e.buy_qty += l.buy_qty;
                e.sell_qty += l.sell_qty;
            }
            for hist in history_window.iter() {
                for (&t, l) in &hist.profile {
                    let e = agg.entry(t).or_default();
                    e.buy_qty += l.buy_qty;
                    e.sell_qty += l.sell_qty;
                }
            }

            let w_open_tick: Option<i64> = history_window
                .iter()
                .find_map(|h| h.open_price.map(|v| (v * 100.0).round() as i64))
                .or(open_tick);
            let w_close_tick = close_tick;
            let hist_high = history_window
                .iter()
                .filter_map(|h| h.high_price)
                .reduce(f64::max)
                .map(|v| (v * 100.0).round() as i64);
            let w_high_tick = match (hist_high, high_trade_tick) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (a, b) => a.or(b),
            };
            let hist_low = history_window
                .iter()
                .filter_map(|h| h.low_price)
                .reduce(f64::min)
                .map(|v| (v * 100.0).round() as i64);
            let w_low_tick = match (hist_low, low_trade_tick) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };

            let w_val = build_footprint_snapshot_value(
                &agg,
                w_open_tick,
                w_close_tick,
                w_high_tick,
                w_low_tick,
                IMBALANCE_RATIO_THRESHOLD,
                MIN_STACK_LEVELS,
                EPS,
            );
            by_window.insert(wcode.to_string(), w_val);
        }

        IndicatorComputation {
            snapshot: Some(IndicatorSnapshotRow {
                indicator_code: self.code(),
                window_code: "1m",
                payload_json: json!({
                    "window_delta": ctx.futures.delta,
                    "window_total_qty": ctx.futures.total_qty,
                    "buy_imbalance_prices": buy_imb.iter().map(|t| tick_to_price(*t)).collect::<Vec<_>>(),
                    "sell_imbalance_prices": sell_imb.iter().map(|t| tick_to_price(*t)).collect::<Vec<_>>(),
                    "stacked_buy": max_buy_stack >= MIN_STACK_LEVELS,
                    "stacked_sell": max_sell_stack >= MIN_STACK_LEVELS,
                    "max_buy_stack_len": max_buy_stack,
                    "max_sell_stack_len": max_sell_stack,
                    "buy_stacks": buy_stacks,
                    "sell_stacks": sell_stacks,
                    "levels": levels,
                    "ua_top": ua_top,
                    "ua_bottom": ua_bottom,
                    "unfinished_auction": (ua_top as i32) - (ua_bottom as i32),
                    "by_window": by_window,
                }),
            }),
            level_rows,
            ..Default::default()
        }
    }
}

fn select_footprint_audit_ticks(levels: &[FootprintAuditLevel]) -> HashSet<i64> {
    let mut out = HashSet::new();

    for level in levels {
        if level.is_buy_imbalance
            || level.is_sell_imbalance
            || level.is_open
            || level.is_close
            || level.is_high
            || level.is_low
            || level.ua_top_flag
            || level.ua_bottom_flag
        {
            out.insert(level.tick);
        }
    }

    let mut by_total = levels
        .iter()
        .map(|level| (level.tick, level.agg.total()))
        .collect::<Vec<_>>();
    by_total.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    for (tick, _) in by_total.into_iter().take(RAW_AUDIT_TOP_TOTAL_LEVELS) {
        out.insert(tick);
    }

    let mut by_abs_delta = levels
        .iter()
        .map(|level| (level.tick, level.agg.delta().abs()))
        .collect::<Vec<_>>();
    by_abs_delta.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    for (tick, _) in by_abs_delta
        .into_iter()
        .take(RAW_AUDIT_TOP_ABS_DELTA_LEVELS)
    {
        out.insert(tick);
    }

    let mut by_buy = levels
        .iter()
        .map(|level| (level.tick, level.agg.buy_qty))
        .collect::<Vec<_>>();
    by_buy.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    for (tick, _) in by_buy.into_iter().take(RAW_AUDIT_TOP_BUY_LEVELS) {
        out.insert(tick);
    }

    let mut by_sell = levels
        .iter()
        .map(|level| (level.tick, level.agg.sell_qty))
        .collect::<Vec<_>>();
    by_sell.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    for (tick, _) in by_sell.into_iter().take(RAW_AUDIT_TOP_SELL_LEVELS) {
        out.insert(tick);
    }

    out
}

fn build_footprint_snapshot_value(
    profile: &BTreeMap<i64, LevelAgg>,
    open_tick: Option<i64>,
    close_tick: Option<i64>,
    high_tick: Option<i64>,
    low_tick: Option<i64>,
    imb_threshold: f64,
    min_stack_levels: i32,
    eps: f64,
) -> serde_json::Value {
    let mut ticks: Vec<i64> = profile.keys().copied().collect();
    ticks.sort_unstable();

    let high_t = ticks.last().copied();
    let low_t = ticks.first().copied();

    let ua_top = high_t
        .and_then(|t| profile.get(&t))
        .map(|l| l.buy_qty > 0.0 && l.sell_qty > 0.0)
        .unwrap_or(false);
    let ua_bottom = low_t
        .and_then(|t| profile.get(&t))
        .map(|l| l.buy_qty > 0.0 && l.sell_qty > 0.0)
        .unwrap_or(false);

    let mut buy_imb: Vec<i64> = Vec::new();
    let mut sell_imb: Vec<i64> = Vec::new();
    let mut levels: Vec<serde_json::Value> = Vec::new();

    for (idx, &tick) in ticks.iter().enumerate() {
        let cur = profile.get(&tick).cloned().unwrap_or_default();
        let prev = profile.get(&(tick - 1)).cloned().unwrap_or_default();
        let next = profile.get(&(tick + 1)).cloned().unwrap_or_default();

        let buy_ratio = cur.buy_qty / (prev.sell_qty + eps);
        let sell_ratio = cur.sell_qty / (next.buy_qty + eps);
        let is_bi = buy_ratio >= imb_threshold && cur.buy_qty > 0.0;
        let is_si = sell_ratio >= imb_threshold && cur.sell_qty > 0.0;

        if is_bi {
            buy_imb.push(tick);
        }
        if is_si {
            sell_imb.push(tick);
        }

        let top_flag = high_t.map(|h| h == tick).unwrap_or(false) && ua_top;
        let bot_flag = low_t.map(|l| l == tick).unwrap_or(false) && ua_bottom;

        levels.push(json!({
            "price_level": tick_to_price(tick),
            "level_rank": (idx + 1) as i32,
            "buy": cur.buy_qty,
            "sell": cur.sell_qty,
            "delta": cur.delta(),
            "total": cur.total(),
            "buy_imbalance": is_bi,
            "sell_imbalance": is_si,
            "is_open": open_tick.map(|v| tick == v).unwrap_or(false),
            "is_close": close_tick.map(|v| tick == v).unwrap_or(false),
            "is_high": high_tick.map(|v| tick == v).unwrap_or(false),
            "is_low": low_tick.map(|v| tick == v).unwrap_or(false),
            "ua_top_flag": top_flag,
            "ua_bottom_flag": bot_flag,
        }));
    }

    let max_buy_stack = longest_consecutive(&buy_imb);
    let max_sell_stack = longest_consecutive(&sell_imb);
    let buy_stacks = contiguous_segments(&buy_imb, min_stack_levels);
    let sell_stacks = contiguous_segments(&sell_imb, min_stack_levels);

    let total_qty: f64 = profile.values().map(|l| l.total()).sum::<f64>() + 0.0;
    let delta: f64 = profile.values().map(|l| l.delta()).sum::<f64>() + 0.0;

    json!({
        "window_delta": delta,
        "window_total_qty": total_qty,
        "buy_imbalance_prices": buy_imb.iter().map(|t| tick_to_price(*t)).collect::<Vec<_>>(),
        "sell_imbalance_prices": sell_imb.iter().map(|t| tick_to_price(*t)).collect::<Vec<_>>(),
        "stacked_buy": max_buy_stack >= min_stack_levels,
        "stacked_sell": max_sell_stack >= min_stack_levels,
        "max_buy_stack_len": max_buy_stack,
        "max_sell_stack_len": max_sell_stack,
        "buy_stacks": buy_stacks,
        "sell_stacks": sell_stacks,
        "levels": levels,
        "ua_top": ua_top,
        "ua_bottom": ua_bottom,
        "unfinished_auction": (ua_top as i32) - (ua_bottom as i32),
    })
}

fn contiguous_segments(values: &[i64], min_len: i32) -> Vec<serde_json::Value> {
    if values.is_empty() {
        return Vec::new();
    }
    let mut vals = values.to_vec();
    vals.sort_unstable();

    let mut out = Vec::new();
    let mut start = vals[0];
    let mut prev = vals[0];
    for v in vals.iter().skip(1) {
        if *v != prev + 1 {
            let len = prev - start + 1;
            if len as i32 >= min_len {
                out.push(json!({
                    "start_price": tick_to_price(start),
                    "end_price": tick_to_price(prev),
                    "length": len
                }));
            }
            start = *v;
        }
        prev = *v;
    }
    let len = prev - start + 1;
    if len as i32 >= min_len {
        out.push(json!({
            "start_price": tick_to_price(start),
            "end_price": tick_to_price(prev),
            "length": len
        }));
    }
    out
}

fn longest_consecutive(values: &[i64]) -> i32 {
    if values.is_empty() {
        return 0;
    }
    let mut vals = values.to_vec();
    vals.sort_unstable();
    let mut best = 1;
    let mut cur = 1;
    for i in 1..vals.len() {
        if vals[i] == vals[i - 1] + 1 {
            cur += 1;
        } else {
            cur = 1;
        }
        if cur > best {
            best = cur;
        }
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn footprint_raw_audit_ticks_are_bounded() {
        let levels = (1..=300_i64)
            .map(|tick| FootprintAuditLevel {
                tick,
                rank: tick as i32,
                agg: LevelAgg {
                    buy_qty: (tick % 19 + 1) as f64,
                    sell_qty: (301 - tick) as f64,
                },
                is_buy_imbalance: tick % 41 == 0,
                is_sell_imbalance: tick % 53 == 0,
                is_open: tick == 3,
                is_close: tick == 297,
                is_high: tick == 300,
                is_low: tick == 1,
                ua_top_flag: tick == 300,
                ua_bottom_flag: tick == 1,
            })
            .collect::<Vec<_>>();

        let ticks = select_footprint_audit_ticks(&levels);
        assert!(ticks.contains(&1));
        assert!(ticks.contains(&300));
        assert!(
            ticks.len()
                <= RAW_AUDIT_TOP_TOTAL_LEVELS
                    + RAW_AUDIT_TOP_ABS_DELTA_LEVELS
                    + RAW_AUDIT_TOP_BUY_LEVELS
                    + RAW_AUDIT_TOP_SELL_LEVELS
                    + 8
        );
    }
}
