use crate::runtime::state_store::LevelAgg;
use std::collections::{BTreeMap, HashSet};

const VALUE_AREA_RATIO: f64 = 0.70;
const IMBALANCE_RATIO_THRESHOLD: f64 = 3.0;
const MIN_STACK_LEVELS: i32 = 3;
const EPS: f64 = 1e-12;

pub fn value_area_key_levels_ticks(profile: &BTreeMap<i64, LevelAgg>) -> Option<(i64, i64, i64)> {
    if profile.is_empty() {
        return None;
    }

    let total_volume = profile.values().map(LevelAgg::total).sum::<f64>();
    if total_volume <= 0.0 {
        return None;
    }

    let mut ticks = profile.keys().copied().collect::<Vec<_>>();
    ticks.sort_unstable();

    let volumes = profile
        .iter()
        .map(|(tick, level)| (*tick, level.total()))
        .collect::<BTreeMap<_, _>>();

    let (poc_tick, poc_volume) = volumes
        .iter()
        .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(tick, vol)| (*tick, *vol))
        .unwrap_or((0_i64, 0.0));

    let target = total_volume * VALUE_AREA_RATIO;
    let mut acc = poc_volume;
    let mut va_ticks = HashSet::new();
    va_ticks.insert(poc_tick);

    if let (Some(min_tick), Some(max_tick)) = (ticks.first().copied(), ticks.last().copied()) {
        let mut left = poc_tick - 1;
        let mut right = poc_tick + 1;
        while acc < target && (left >= min_tick || right <= max_tick) {
            let lv = volumes.get(&left).copied().unwrap_or(0.0);
            let rv = volumes.get(&right).copied().unwrap_or(0.0);
            if rv >= lv && right <= max_tick {
                acc += rv;
                va_ticks.insert(right);
                right += 1;
            } else if left >= min_tick {
                acc += lv;
                va_ticks.insert(left);
                left -= 1;
            } else {
                break;
            }
        }
    }

    let val_tick = va_ticks.iter().min().copied().unwrap_or(poc_tick);
    let vah_tick = va_ticks.iter().max().copied().unwrap_or(poc_tick);
    Some((val_tick, vah_tick, poc_tick))
}

pub fn stacked_imbalance_flags(profile: &BTreeMap<i64, LevelAgg>) -> (bool, bool) {
    if profile.is_empty() {
        return (false, false);
    }

    let mut ticks = profile.keys().copied().collect::<Vec<_>>();
    ticks.sort_unstable();

    let mut buy_run = 0_i32;
    let mut sell_run = 0_i32;
    let mut max_buy_run = 0_i32;
    let mut max_sell_run = 0_i32;
    let mut last_buy_tick: Option<i64> = None;
    let mut last_sell_tick: Option<i64> = None;

    for tick in ticks {
        let cur = profile.get(&tick).cloned().unwrap_or_default();
        let prev = profile.get(&(tick - 1)).cloned().unwrap_or_default();
        let next = profile.get(&(tick + 1)).cloned().unwrap_or_default();

        let buy_ratio = cur.buy_qty / (prev.sell_qty + EPS);
        let sell_ratio = cur.sell_qty / (next.buy_qty + EPS);

        let is_buy_imb = buy_ratio >= IMBALANCE_RATIO_THRESHOLD && cur.buy_qty > 0.0;
        let is_sell_imb = sell_ratio >= IMBALANCE_RATIO_THRESHOLD && cur.sell_qty > 0.0;

        if is_buy_imb {
            buy_run = if last_buy_tick.map(|t| tick == t + 1).unwrap_or(false) {
                buy_run + 1
            } else {
                1
            };
            last_buy_tick = Some(tick);
        } else {
            buy_run = 0;
            last_buy_tick = None;
        }
        max_buy_run = max_buy_run.max(buy_run);

        if is_sell_imb {
            sell_run = if last_sell_tick.map(|t| tick == t + 1).unwrap_or(false) {
                sell_run + 1
            } else {
                1
            };
            last_sell_tick = Some(tick);
        } else {
            sell_run = 0;
            last_sell_tick = None;
        }
        max_sell_run = max_sell_run.max(sell_run);
    }

    (
        max_buy_run >= MIN_STACK_LEVELS,
        max_sell_run >= MIN_STACK_LEVELS,
    )
}
