use crate::indicators::context::{
    zscore, IndicatorComputation, IndicatorContext, IndicatorLevelRow, IndicatorSnapshotRow,
};
use crate::indicators::indicator_trait::Indicator;
use crate::runtime::state_store::{tick_to_price, LevelAgg};
use serde_json::json;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};

const VALUE_AREA_RATIO: f64 = 0.70;
const DRYUP_LOOKBACK: usize = 30;
const DRYUP_Z_THRESHOLD: f64 = 1.0;

/// Prominence thresholds expressed as a fraction of the window's total volume.
const HVN_PROMINENCE_FRACTION: f64 = 0.03;
const LVN_PROMINENCE_FRACTION: f64 = 0.01;
const VOLUME_RANK_REL_EPSILON: f64 = 1e-9;
const RAW_AUDIT_TOP_RANKS_MULTI_WINDOW: usize = 96;
const RAW_AUDIT_HVN_CAP: usize = 24;
const RAW_AUDIT_LVN_CAP: usize = 24;

/// Multi-window specs: (window_code, bar_count).
const MULTI_WINDOWS: &[(&str, usize)] = &[("15m", 15), ("4h", 240), ("1d", 1440), ("3d", 4320)];

#[derive(Clone, Copy)]
struct PvsV2WindowConfig {
    rows: usize,
    hvn_pct_of_poc: f64,
    lvn_pct_of_poc: f64,
    strength: usize,
    min_zone_width_bins: usize,
}

#[derive(Clone)]
struct BinnedProfileLevel {
    index: usize,
    lower_tick: i64,
    upper_tick: i64,
    buy_volume: f64,
    sell_volume: f64,
}

impl BinnedProfileLevel {
    fn midpoint_price(&self) -> f64 {
        (self.lower_tick + self.upper_tick) as f64 / 200.0
    }

    fn volume(&self) -> f64 {
        self.buy_volume + self.sell_volume
    }

    fn delta(&self) -> f64 {
        self.buy_volume - self.sell_volume
    }
}

#[derive(Clone, Copy)]
struct BinZone {
    start_idx: usize,
    end_idx: usize,
}

#[derive(Clone)]
struct PvsV2Analysis {
    bins: Vec<BinnedProfileLevel>,
    poc_idx: usize,
    poc_volume: f64,
    value_area_bins: HashSet<usize>,
    hvn_zones: Vec<BinZone>,
    lvn_zones: Vec<BinZone>,
}

pub struct I01PriceVolumeStructure;

impl Indicator for I01PriceVolumeStructure {
    fn code(&self) -> &'static str {
        "price_volume_structure"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation {
        let trade_history = if ctx.trade_history_futures.is_empty() {
            &ctx.history_futures
        } else {
            &ctx.trade_history_futures
        };

        // ── Current 1m bar (top-level snapshot, unchanged) ────────────────────
        let total_volume = ctx.futures.total_qty;
        let volume_z = zscore(
            total_volume,
            trade_history
                .iter()
                .rev()
                .take(DRYUP_LOOKBACK)
                .map(|h| h.total_qty)
                .collect(),
        );
        let volume_dryup = volume_z.map(|z| z <= -DRYUP_Z_THRESHOLD).unwrap_or(false);

        let mut payload =
            build_pvs_payload(&ctx.futures.profile, total_volume, volume_z, volume_dryup);

        let mut level_rows =
            build_level_rows(self.code(), "1m", &ctx.futures.profile, total_volume);

        // ── Multi-window by_window ─────────────────────────────────────────────
        let avail = trade_history.len();
        let mut by_window = serde_json::Map::new();

        for &(wcode, window_bars) in MULTI_WINDOWS {
            // `history_futures` already contains the current minute as the tail element.
            // Exclude that tail here because the current bar is explicitly added from
            // `ctx.futures.profile` below.
            let hist_end = avail.saturating_sub(1);
            let history_take = window_bars.saturating_sub(1).min(hist_end);
            let hist_start = hist_end.saturating_sub(history_take);

            let mut agg: BTreeMap<i64, LevelAgg> = BTreeMap::new();
            for (&t, l) in &ctx.futures.profile {
                let e = agg.entry(t).or_default();
                e.buy_qty += l.buy_qty;
                e.sell_qty += l.sell_qty;
            }
            for hist in &trade_history[hist_start..hist_end] {
                for (&t, l) in &hist.profile {
                    let e = agg.entry(t).or_default();
                    e.buy_qty += l.buy_qty;
                    e.sell_qty += l.sell_qty;
                }
            }

            let window_total: f64 = agg.values().map(|l| l.total()).sum();
            let window_bars_used = history_take + 1;

            // Volume zscore: use non-overlapping prior windows from history before
            // the current window.  Each prior sample is the sum of window_bars 1m bars.
            let prior_vols: Vec<f64> = if hist_start >= window_bars {
                let n_chunks = (hist_start / window_bars).min(DRYUP_LOOKBACK);
                (0..n_chunks)
                    .map(|i| {
                        let end = hist_start - i * window_bars;
                        let start = end.saturating_sub(window_bars);
                        trade_history[start..end]
                            .iter()
                            .map(|h| h.total_qty)
                            .sum::<f64>()
                    })
                    .collect()
            } else {
                Vec::new()
            };

            let w_zscore = zscore(window_total, prior_vols);
            let w_dryup = w_zscore.map(|z| z <= -DRYUP_Z_THRESHOLD).unwrap_or(false);

            let mut w_payload = build_pvs_v2_payload(wcode, &agg, window_total, w_zscore, w_dryup);
            if let Some(obj) = w_payload.as_object_mut() {
                obj.insert("window_bars_used".into(), json!(window_bars_used));
            }
            level_rows.extend(build_level_rows(self.code(), wcode, &agg, window_total));
            by_window.insert(wcode.to_string(), w_payload);
        }

        if let Some(obj) = payload.as_object_mut() {
            obj.insert("by_window".into(), serde_json::Value::Object(by_window));
        }

        IndicatorComputation {
            snapshot: Some(IndicatorSnapshotRow {
                indicator_code: self.code(),
                window_code: "1m",
                payload_json: payload,
            }),
            level_rows,
            ..Default::default()
        }
    }
}

/// Compute the full PVS payload (POC, VA, HVN/LVN, zscore, dryup, levels, value_area_levels)
/// for a given aggregated price profile.
fn build_pvs_payload(
    profile: &BTreeMap<i64, LevelAgg>,
    total_volume: f64,
    volume_z: Option<f64>,
    volume_dryup: bool,
) -> serde_json::Value {
    if profile.is_empty() || total_volume <= 0.0 {
        return json!({
            "bar_volume": total_volume,
            "poc_price": null,
            "poc_volume": null,
            "val": null,
            "vah": null,
            "value_area_levels": [],
            "hvn_levels": [],
            "lvn_levels": [],
            "volume_zscore": volume_z,
            "volume_dryup": volume_dryup,
            "levels": []
        });
    }

    let mut by_volume: Vec<(i64, &LevelAgg)> = profile.iter().map(|(t, l)| (*t, l)).collect();
    by_volume.sort_by_key(|(t, _)| *t);

    // POC
    let (poc_tick, poc_volume) = by_volume
        .iter()
        .max_by(|a, b| {
            a.1.total()
                .partial_cmp(&b.1.total())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|(t, l)| (*t, l.total()))
        .unwrap_or((0, 0.0));

    // Value Area: expand from POC until 70% of total volume is covered.
    let volumes: BTreeMap<i64, f64> = by_volume.iter().map(|(t, l)| (*t, l.total())).collect();
    let target = total_volume * VALUE_AREA_RATIO;
    let mut acc = poc_volume;
    let mut va_ticks = HashSet::new();
    va_ticks.insert(poc_tick);

    let min_tick = by_volume.first().map(|(t, _)| *t).unwrap_or(poc_tick);
    let max_tick = by_volume.last().map(|(t, _)| *t).unwrap_or(poc_tick);
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

    // Smoothed profile for HVN/LVN: [1,2,1]/4 kernel.
    let smoothed_vals: Vec<f64> = by_volume
        .iter()
        .enumerate()
        .map(|(i, _)| {
            let prev = if i > 0 {
                by_volume[i - 1].1.total()
            } else {
                by_volume[i].1.total()
            };
            let cur = by_volume[i].1.total();
            let next = if i + 1 < by_volume.len() {
                by_volume[i + 1].1.total()
            } else {
                by_volume[i].1.total()
            };
            (prev + 2.0 * cur + next) / 4.0
        })
        .collect();

    let hvn_threshold = HVN_PROMINENCE_FRACTION * total_volume;
    let lvn_threshold = LVN_PROMINENCE_FRACTION * total_volume;

    let mut hvn: Vec<i64> = Vec::new();
    let mut lvn: Vec<i64> = Vec::new();
    for i in 1..by_volume.len().saturating_sub(1) {
        let t_prev = by_volume[i - 1].0;
        let t_cur = by_volume[i].0;
        let t_next = by_volume[i + 1].0;
        // Require adjacent ticks (no price gap).
        if t_prev + 1 != t_cur || t_cur + 1 != t_next {
            continue;
        }
        let v = smoothed_vals[i];
        if v > smoothed_vals[i - 1] && v > smoothed_vals[i + 1] {
            if prominence_max(i, &smoothed_vals) >= hvn_threshold {
                hvn.push(t_cur);
            }
        }
        if v < smoothed_vals[i - 1] && v < smoothed_vals[i + 1] {
            if prominence_min(i, &smoothed_vals) >= lvn_threshold {
                lvn.push(t_cur);
            }
        }
    }

    let val = va_ticks.iter().min().copied().map(tick_to_price);
    let vah = va_ticks.iter().max().copied().map(tick_to_price);

    // Volume rank (by descending total volume).
    let mut rank_order: Vec<(i64, f64)> = by_volume.iter().map(|(t, l)| (*t, l.total())).collect();
    rank_order.sort_by(|a, b| compare_ranked_volume(*a, *b));
    let rank_map: BTreeMap<i64, i32> = rank_order
        .iter()
        .enumerate()
        .map(|(i, (t, _))| (*t, (i + 1) as i32))
        .collect();

    let levels: Vec<serde_json::Value> = by_volume
        .iter()
        .map(|(tick, level)| {
            json!({
                "price_level": tick_to_price(*tick),
                "level_rank": rank_map.get(tick).copied(),
                "buy_volume": level.buy_qty,
                "sell_volume": level.sell_qty,
                "volume": level.total(),
                "delta": level.delta(),
                "is_hvn": hvn.contains(tick),
                "is_lvn": lvn.contains(tick),
                "is_in_value_area": va_ticks.contains(tick)
            })
        })
        .collect();

    let value_area_levels: Vec<serde_json::Value> = by_volume
        .iter()
        .filter(|(tick, _)| va_ticks.contains(tick))
        .map(|(tick, level)| {
            json!({
                "price_level": tick_to_price(*tick),
                "volume": level.total()
            })
        })
        .collect();

    json!({
        "bar_volume": total_volume,
        "poc_price": tick_to_price(poc_tick),
        "poc_volume": poc_volume,
        "val": val,
        "vah": vah,
        "value_area_levels": value_area_levels,
        "hvn_levels": hvn.iter().map(|t| tick_to_price(*t)).collect::<Vec<_>>(),
        "lvn_levels": lvn.iter().map(|t| tick_to_price(*t)).collect::<Vec<_>>(),
        "volume_zscore": volume_z,
        "volume_dryup": volume_dryup,
        "levels": levels
    })
}

fn pvs_v2_window_config(window_code: &str) -> Option<PvsV2WindowConfig> {
    match window_code {
        "15m" => Some(PvsV2WindowConfig {
            rows: 24,
            hvn_pct_of_poc: 0.82,
            lvn_pct_of_poc: 0.18,
            strength: 9,
            min_zone_width_bins: 2,
        }),
        "4h" => Some(PvsV2WindowConfig {
            rows: 20,
            hvn_pct_of_poc: 0.76,
            lvn_pct_of_poc: 0.24,
            strength: 11,
            min_zone_width_bins: 2,
        }),
        "1d" => Some(PvsV2WindowConfig {
            rows: 16,
            hvn_pct_of_poc: 0.72,
            lvn_pct_of_poc: 0.28,
            strength: 13,
            min_zone_width_bins: 1,
        }),
        "3d" => Some(PvsV2WindowConfig {
            rows: 14,
            hvn_pct_of_poc: 0.68,
            lvn_pct_of_poc: 0.32,
            strength: 16,
            min_zone_width_bins: 1,
        }),
        _ => None,
    }
}

fn build_pvs_v2_payload(
    window_code: &str,
    profile: &BTreeMap<i64, LevelAgg>,
    total_volume: f64,
    volume_z: Option<f64>,
    volume_dryup: bool,
) -> serde_json::Value {
    let Some(analysis) = analyze_pvs_v2(window_code, profile) else {
        return json!({
            "bar_volume": total_volume,
            "poc_price": null,
            "poc_volume": null,
            "val": null,
            "vah": null,
            "value_area_levels": [],
            "hvn_levels": [],
            "lvn_levels": [],
            "volume_zscore": volume_z,
            "volume_dryup": volume_dryup,
            "levels": []
        });
    };

    let val = analysis
        .value_area_bins
        .iter()
        .min()
        .map(|idx| analysis.bins[*idx].lower_tick as f64 / 100.0);
    let vah = analysis
        .value_area_bins
        .iter()
        .max()
        .map(|idx| analysis.bins[*idx].upper_tick as f64 / 100.0);

    let mut rank_order = analysis
        .bins
        .iter()
        .map(|bin| (bin.index as i64, bin.volume()))
        .collect::<Vec<_>>();
    rank_order.sort_by(|a, b| compare_ranked_volume(*a, *b));
    let rank_map: BTreeMap<usize, i32> = rank_order
        .iter()
        .enumerate()
        .map(|(i, (idx, _))| (*idx as usize, (i + 1) as i32))
        .collect();

    let levels = analysis
        .bins
        .iter()
        .map(|bin| {
            json!({
                "price_level": bin.midpoint_price(),
                "level_rank": rank_map.get(&bin.index).copied(),
                "buy_volume": bin.buy_volume,
                "sell_volume": bin.sell_volume,
                "volume": bin.volume(),
                "delta": bin.delta(),
                "is_hvn": zone_contains_index(&analysis.hvn_zones, bin.index),
                "is_lvn": zone_contains_index(&analysis.lvn_zones, bin.index),
                "is_in_value_area": analysis.value_area_bins.contains(&bin.index)
            })
        })
        .collect::<Vec<_>>();

    let value_area_levels = analysis
        .bins
        .iter()
        .filter(|bin| analysis.value_area_bins.contains(&bin.index))
        .map(|bin| {
            json!({
                "price_level": bin.midpoint_price(),
                "volume": bin.volume()
            })
        })
        .collect::<Vec<_>>();

    json!({
        "bar_volume": total_volume,
        "poc_price": analysis.bins[analysis.poc_idx].midpoint_price(),
        "poc_volume": analysis.poc_volume,
        "val": val,
        "vah": vah,
        "value_area_levels": value_area_levels,
        "hvn_levels": zone_midpoints(&analysis.bins, &analysis.hvn_zones),
        "lvn_levels": zone_midpoints(&analysis.bins, &analysis.lvn_zones),
        "volume_zscore": volume_z,
        "volume_dryup": volume_dryup,
        "levels": levels
    })
}

fn analyze_pvs_v2(window_code: &str, profile: &BTreeMap<i64, LevelAgg>) -> Option<PvsV2Analysis> {
    let config = pvs_v2_window_config(window_code)?;
    let bins = build_binned_profile(profile, config.rows);
    if bins.is_empty() {
        return None;
    }

    let smoothed = smooth_bin_volumes(&bins);
    let (poc_idx, poc_volume) = smoothed
        .iter()
        .copied()
        .enumerate()
        .max_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        })
        .unwrap_or((0, 0.0));

    let value_area_bins = build_value_area_bins(&bins, poc_idx);
    let hvn_zones = build_node_zones(&smoothed, poc_volume, config, true);
    let lvn_zones = build_node_zones(&smoothed, poc_volume, config, false);

    Some(PvsV2Analysis {
        bins,
        poc_idx,
        poc_volume,
        value_area_bins,
        hvn_zones,
        lvn_zones,
    })
}

fn build_binned_profile(
    profile: &BTreeMap<i64, LevelAgg>,
    requested_rows: usize,
) -> Vec<BinnedProfileLevel> {
    if profile.is_empty() || requested_rows == 0 {
        return Vec::new();
    }

    let min_tick = *profile.keys().next().unwrap_or(&0);
    let max_tick = *profile.keys().next_back().unwrap_or(&min_tick);
    let span_ticks = (max_tick - min_tick + 1).max(1) as usize;
    let actual_rows = requested_rows.min(span_ticks.max(1));
    let bin_width_ticks = span_ticks.div_ceil(actual_rows).max(1);

    let mut bins = (0..actual_rows)
        .map(|index| {
            let lower_tick = min_tick + (index * bin_width_ticks) as i64;
            let upper_tick = if index + 1 == actual_rows {
                max_tick
            } else {
                (lower_tick + bin_width_ticks as i64 - 1).min(max_tick)
            };
            BinnedProfileLevel {
                index,
                lower_tick,
                upper_tick,
                buy_volume: 0.0,
                sell_volume: 0.0,
            }
        })
        .collect::<Vec<_>>();

    for (&tick, level) in profile {
        let bin_index = (((tick - min_tick) as usize) / bin_width_ticks).min(actual_rows - 1);
        bins[bin_index].buy_volume += level.buy_qty;
        bins[bin_index].sell_volume += level.sell_qty;
    }

    bins
}

fn smooth_bin_volumes(bins: &[BinnedProfileLevel]) -> Vec<f64> {
    bins.iter()
        .enumerate()
        .map(|(index, _)| {
            let prev = if index > 0 {
                bins[index - 1].volume()
            } else {
                bins[index].volume()
            };
            let cur = bins[index].volume();
            let next = if index + 1 < bins.len() {
                bins[index + 1].volume()
            } else {
                bins[index].volume()
            };
            (prev + 2.0 * cur + next) / 4.0
        })
        .collect()
}

fn build_value_area_bins(bins: &[BinnedProfileLevel], poc_idx: usize) -> HashSet<usize> {
    let total_volume = bins.iter().map(BinnedProfileLevel::volume).sum::<f64>();
    let target = total_volume * VALUE_AREA_RATIO;
    let mut acc = bins
        .get(poc_idx)
        .map(BinnedProfileLevel::volume)
        .unwrap_or(0.0);
    let mut out = HashSet::new();
    out.insert(poc_idx);

    let mut left = poc_idx.checked_sub(1);
    let mut right = poc_idx + 1;
    while acc < target && (left.is_some() || right < bins.len()) {
        let lv = left.map(|idx| bins[idx].volume()).unwrap_or(-1.0);
        let rv = if right < bins.len() {
            bins[right].volume()
        } else {
            -1.0
        };
        if rv >= lv && right < bins.len() {
            acc += bins[right].volume();
            out.insert(right);
            right += 1;
        } else if let Some(left_idx) = left {
            acc += bins[left_idx].volume();
            out.insert(left_idx);
            left = left_idx.checked_sub(1);
        } else {
            break;
        }
    }

    out
}

fn build_node_zones(
    smoothed: &[f64],
    poc_volume: f64,
    config: PvsV2WindowConfig,
    is_hvn: bool,
) -> Vec<BinZone> {
    if smoothed.is_empty() || poc_volume <= 0.0 {
        return Vec::new();
    }

    let threshold = if is_hvn {
        config.hvn_pct_of_poc * poc_volume
    } else {
        config.lvn_pct_of_poc * poc_volume
    };

    let candidate_bins = (0..smoothed.len())
        .filter(|&idx| {
            if is_hvn {
                smoothed[idx] >= threshold && is_strict_local_max(smoothed, idx, config.strength)
            } else {
                smoothed[idx] <= threshold && is_strict_local_min(smoothed, idx, config.strength)
            }
        })
        .collect::<Vec<_>>();

    let mut zones: Vec<BinZone> = Vec::new();
    for idx in candidate_bins {
        if let Some(last) = zones.last_mut() {
            if idx == last.end_idx + 1 {
                last.end_idx = idx;
                continue;
            }
        }
        zones.push(BinZone {
            start_idx: idx,
            end_idx: idx,
        });
    }

    zones
        .into_iter()
        .filter(|zone| zone.end_idx + 1 - zone.start_idx >= config.min_zone_width_bins)
        .collect()
}

fn is_strict_local_max(values: &[f64], idx: usize, strength: usize) -> bool {
    let start = idx.saturating_sub(strength);
    let end = (idx + strength).min(values.len().saturating_sub(1));
    if start == idx && end == idx {
        return false;
    }
    let value = values[idx];
    (start..=end).all(|other| other == idx || value > values[other])
}

fn is_strict_local_min(values: &[f64], idx: usize, strength: usize) -> bool {
    let start = idx.saturating_sub(strength);
    let end = (idx + strength).min(values.len().saturating_sub(1));
    if start == idx && end == idx {
        return false;
    }
    let value = values[idx];
    (start..=end).all(|other| other == idx || value < values[other])
}

fn zone_midpoints(bins: &[BinnedProfileLevel], zones: &[BinZone]) -> Vec<f64> {
    zones
        .iter()
        .map(|zone| {
            let lower_tick = bins[zone.start_idx].lower_tick;
            let upper_tick = bins[zone.end_idx].upper_tick;
            (lower_tick + upper_tick) as f64 / 200.0
        })
        .collect()
}

fn zone_contains_index(zones: &[BinZone], index: usize) -> bool {
    zones
        .iter()
        .any(|zone| index >= zone.start_idx && index <= zone.end_idx)
}

fn build_pvs_v2_level_rows(
    indicator_code: &'static str,
    window_code: &'static str,
    profile: &BTreeMap<i64, LevelAgg>,
    _total_volume: f64,
) -> Vec<IndicatorLevelRow> {
    let Some(analysis) = analyze_pvs_v2(window_code, profile) else {
        return Vec::new();
    };

    let val_idx = analysis.value_area_bins.iter().min().copied();
    let vah_idx = analysis.value_area_bins.iter().max().copied();

    let mut rank_order = analysis
        .bins
        .iter()
        .map(|bin| (bin.index, bin.volume()))
        .collect::<Vec<_>>();
    rank_order.sort_by(|a, b| compare_ranked_volume((a.0 as i64, a.1), (b.0 as i64, b.1)));
    let rank_map: BTreeMap<usize, i32> = rank_order
        .iter()
        .enumerate()
        .map(|(i, (idx, _))| (*idx, (i + 1) as i32))
        .collect();

    let audit_subset = select_raw_audit_bins(
        &rank_order,
        &analysis.bins,
        &analysis.hvn_zones,
        &analysis.lvn_zones,
        analysis.poc_idx,
        val_idx,
        vah_idx,
    );

    analysis
        .bins
        .iter()
        .filter(|bin| audit_subset.contains(&bin.index))
        .map(|bin| IndicatorLevelRow {
            indicator_code,
            window_code,
            price_level: bin.midpoint_price(),
            level_rank: rank_map.get(&bin.index).copied(),
            metrics_json: json!({
                "buy_volume": bin.buy_volume,
                "sell_volume": bin.sell_volume,
                "volume": bin.volume(),
                "delta": bin.delta(),
                "is_poc": bin.index == analysis.poc_idx,
                "is_val": Some(bin.index) == val_idx,
                "is_vah": Some(bin.index) == vah_idx,
                "is_hvn": zone_contains_index(&analysis.hvn_zones, bin.index),
                "is_lvn": zone_contains_index(&analysis.lvn_zones, bin.index),
                "is_in_value_area": analysis.value_area_bins.contains(&bin.index),
                "audit_capture_policy": "structural_top_ranked_subset"
            }),
        })
        .collect()
}

fn select_raw_audit_bins(
    rank_order: &[(usize, f64)],
    bins: &[BinnedProfileLevel],
    hvn_zones: &[BinZone],
    lvn_zones: &[BinZone],
    poc_idx: usize,
    val_idx: Option<usize>,
    vah_idx: Option<usize>,
) -> HashSet<usize> {
    let mut out = HashSet::new();
    out.insert(poc_idx);
    if let Some(idx) = val_idx {
        out.insert(idx);
    }
    if let Some(idx) = vah_idx {
        out.insert(idx);
    }

    for (idx, _) in rank_order.iter().take(RAW_AUDIT_TOP_RANKS_MULTI_WINDOW) {
        out.insert(*idx);
    }

    for zone in hvn_zones.iter().take(RAW_AUDIT_HVN_CAP) {
        for idx in zone.start_idx..=zone.end_idx {
            out.insert(idx);
        }
    }

    let mut lvn_ranked = lvn_zones
        .iter()
        .map(|zone| {
            let min_volume = (zone.start_idx..=zone.end_idx)
                .map(|idx| bins[idx].volume())
                .fold(f64::INFINITY, f64::min);
            (*zone, min_volume)
        })
        .collect::<Vec<_>>();
    lvn_ranked.sort_by(|a, b| {
        a.1.partial_cmp(&b.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.start_idx.cmp(&b.0.start_idx))
    });
    for (zone, _) in lvn_ranked.into_iter().take(RAW_AUDIT_LVN_CAP) {
        for idx in zone.start_idx..=zone.end_idx {
            out.insert(idx);
        }
    }

    out
}

/// Build per-level DB rows for the 1m snapshot window.
fn build_level_rows(
    indicator_code: &'static str,
    window_code: &'static str,
    profile: &BTreeMap<i64, LevelAgg>,
    total_volume: f64,
) -> Vec<IndicatorLevelRow> {
    if pvs_v2_window_config(window_code).is_some() {
        return build_pvs_v2_level_rows(indicator_code, window_code, profile, total_volume);
    }
    build_raw_level_rows(indicator_code, window_code, profile, total_volume)
}

fn build_raw_level_rows(
    indicator_code: &'static str,
    window_code: &'static str,
    profile: &BTreeMap<i64, LevelAgg>,
    total_volume: f64,
) -> Vec<IndicatorLevelRow> {
    let mut by_volume: Vec<(i64, &LevelAgg)> = profile.iter().map(|(t, l)| (*t, l)).collect();
    by_volume.sort_by_key(|(t, _)| *t);

    let mut rank_order: Vec<(i64, f64)> = by_volume.iter().map(|(t, l)| (*t, l.total())).collect();
    rank_order.sort_by(|a, b| compare_ranked_volume(*a, *b));
    let rank_map: BTreeMap<i64, i32> = rank_order
        .iter()
        .enumerate()
        .map(|(i, (t, _))| (*t, (i + 1) as i32))
        .collect();

    // Recompute VA and HVN/LVN for correct level flags (mirrors build_pvs_payload).
    let poc_tick = by_volume
        .iter()
        .max_by(|a, b| {
            a.1.total()
                .partial_cmp(&b.1.total())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|(t, _)| *t)
        .unwrap_or(0);

    let volumes: BTreeMap<i64, f64> = by_volume.iter().map(|(t, l)| (*t, l.total())).collect();
    let target = total_volume * VALUE_AREA_RATIO;
    let mut acc = volumes.get(&poc_tick).copied().unwrap_or(0.0);
    let mut va_ticks = HashSet::new();
    va_ticks.insert(poc_tick);
    let min_tick = by_volume.first().map(|(t, _)| *t).unwrap_or(poc_tick);
    let max_tick = by_volume.last().map(|(t, _)| *t).unwrap_or(poc_tick);
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

    let smoothed_vals: Vec<f64> = by_volume
        .iter()
        .enumerate()
        .map(|(i, _)| {
            let prev = if i > 0 {
                by_volume[i - 1].1.total()
            } else {
                by_volume[i].1.total()
            };
            let cur = by_volume[i].1.total();
            let next = if i + 1 < by_volume.len() {
                by_volume[i + 1].1.total()
            } else {
                by_volume[i].1.total()
            };
            (prev + 2.0 * cur + next) / 4.0
        })
        .collect();

    let hvn_threshold = HVN_PROMINENCE_FRACTION * total_volume;
    let lvn_threshold = LVN_PROMINENCE_FRACTION * total_volume;
    let mut hvn: Vec<i64> = Vec::new();
    let mut lvn: Vec<i64> = Vec::new();
    for i in 1..by_volume.len().saturating_sub(1) {
        let t_prev = by_volume[i - 1].0;
        let t_cur = by_volume[i].0;
        let t_next = by_volume[i + 1].0;
        if t_prev + 1 != t_cur || t_cur + 1 != t_next {
            continue;
        }
        let v = smoothed_vals[i];
        if v > smoothed_vals[i - 1] && v > smoothed_vals[i + 1] {
            if prominence_max(i, &smoothed_vals) >= hvn_threshold {
                hvn.push(t_cur);
            }
        }
        if v < smoothed_vals[i - 1] && v < smoothed_vals[i + 1] {
            if prominence_min(i, &smoothed_vals) >= lvn_threshold {
                lvn.push(t_cur);
            }
        }
    }

    let val_tick = va_ticks.iter().min().copied();
    let vah_tick = va_ticks.iter().max().copied();
    let audit_subset = select_raw_audit_ticks(
        &rank_order,
        &volumes,
        &hvn,
        &lvn,
        poc_tick,
        val_tick,
        vah_tick,
    );

    by_volume
        .iter()
        .filter(|(tick, _)| audit_subset.contains(tick))
        .map(|(tick, level)| IndicatorLevelRow {
            indicator_code,
            window_code,
            price_level: tick_to_price(*tick),
            level_rank: rank_map.get(tick).copied(),
            metrics_json: json!({
                "buy_volume": level.buy_qty,
                "sell_volume": level.sell_qty,
                "volume": level.total(),
                "delta": level.delta(),
                "is_poc": *tick == poc_tick,
                "is_val": Some(*tick) == val_tick,
                "is_vah": Some(*tick) == vah_tick,
                "is_hvn": hvn.contains(tick),
                "is_lvn": lvn.contains(tick),
                "is_in_value_area": va_ticks.contains(tick),
                "audit_capture_policy": "structural_top_ranked_subset"
            }),
        })
        .collect()
}

fn select_raw_audit_ticks(
    rank_order: &[(i64, f64)],
    volumes: &BTreeMap<i64, f64>,
    hvn: &[i64],
    lvn: &[i64],
    poc_tick: i64,
    val_tick: Option<i64>,
    vah_tick: Option<i64>,
) -> HashSet<i64> {
    let mut out = HashSet::new();
    out.insert(poc_tick);
    if let Some(tick) = val_tick {
        out.insert(tick);
    }
    if let Some(tick) = vah_tick {
        out.insert(tick);
    }

    for (tick, _) in rank_order.iter().take(RAW_AUDIT_TOP_RANKS_MULTI_WINDOW) {
        out.insert(*tick);
    }

    let mut hvn_ranked = hvn
        .iter()
        .map(|tick| (*tick, volumes.get(tick).copied().unwrap_or(0.0)))
        .collect::<Vec<_>>();
    hvn_ranked.sort_by(|a, b| compare_ranked_volume(*a, *b));
    for (tick, _) in hvn_ranked.into_iter().take(RAW_AUDIT_HVN_CAP) {
        out.insert(tick);
    }

    let mut lvn_ranked = lvn
        .iter()
        .map(|tick| (*tick, volumes.get(tick).copied().unwrap_or(0.0)))
        .collect::<Vec<_>>();
    lvn_ranked.sort_by(|a, b| {
        a.1.partial_cmp(&b.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    for (tick, _) in lvn_ranked.into_iter().take(RAW_AUDIT_LVN_CAP) {
        out.insert(tick);
    }

    out
}

/// Topographic prominence of a local maximum at `idx` in the smoothed values array.
fn prominence_max(idx: usize, vals: &[f64]) -> f64 {
    let v = vals[idx];
    let left_col = {
        let mut min_so_far = v;
        let mut col = 0.0_f64;
        for j in (0..idx).rev() {
            if vals[j] < min_so_far {
                min_so_far = vals[j];
            }
            if vals[j] > v {
                col = min_so_far;
                break;
            }
        }
        col
    };
    let right_col = {
        let mut min_so_far = v;
        let mut col = 0.0_f64;
        for j in (idx + 1)..vals.len() {
            if vals[j] < min_so_far {
                min_so_far = vals[j];
            }
            if vals[j] > v {
                col = min_so_far;
                break;
            }
        }
        col
    };
    v - left_col.max(right_col)
}

/// Topographic prominence of a local minimum (valley) at `idx` in the smoothed values array.
fn prominence_min(idx: usize, vals: &[f64]) -> f64 {
    let v = vals[idx];
    let left_ridge = {
        let mut max_so_far = v;
        for j in (0..idx).rev() {
            if vals[j] > max_so_far {
                max_so_far = vals[j];
            }
            if vals[j] < v {
                break;
            }
        }
        max_so_far
    };
    let right_ridge = {
        let mut max_so_far = v;
        for j in (idx + 1)..vals.len() {
            if vals[j] > max_so_far {
                max_so_far = vals[j];
            }
            if vals[j] < v {
                break;
            }
        }
        max_so_far
    };
    left_ridge.min(right_ridge) - v
}

fn compare_ranked_volume(
    (tick_a, volume_a): (i64, f64),
    (tick_b, volume_b): (i64, f64),
) -> Ordering {
    let scale = volume_a.abs().max(volume_b.abs()).max(1.0);
    if (volume_a - volume_b).abs() <= VOLUME_RANK_REL_EPSILON * scale {
        tick_a.cmp(&tick_b)
    } else {
        volume_b
            .partial_cmp(&volume_a)
            .unwrap_or(Ordering::Equal)
            .then_with(|| tick_a.cmp(&tick_b))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_level_rows, build_node_zones, build_pvs_payload, I01PriceVolumeStructure,
        PvsV2WindowConfig, MULTI_WINDOWS, RAW_AUDIT_HVN_CAP, RAW_AUDIT_LVN_CAP,
        RAW_AUDIT_TOP_RANKS_MULTI_WINDOW,
    };
    use crate::indicators::context::{DivergenceSigTestMode, IndicatorContext};
    use crate::indicators::indicator_trait::Indicator;
    use crate::ingest::decoder::MarketKind;
    use crate::runtime::state_store::{LevelAgg, MinuteHistory, MinuteWindowData};
    use chrono::{TimeZone, Utc};
    use std::collections::BTreeMap;

    #[test]
    fn pvs_multi_windows_are_exactly_15m_4h_1d_3d() {
        assert_eq!(
            MULTI_WINDOWS,
            &[("15m", 15), ("4h", 240), ("1d", 1440), ("3d", 4320)]
        );
    }

    #[test]
    fn pvs_output_contains_exact_multi_windows() {
        let ts_bucket = Utc
            .with_ymd_and_hms(2026, 3, 4, 0, 0, 0)
            .single()
            .expect("valid timestamp");

        let mut futures = MinuteWindowData::empty(MarketKind::Futures, ts_bucket);
        futures.total_qty = 10.0;
        futures.profile.insert(
            100,
            LevelAgg {
                buy_qty: 6.0,
                sell_qty: 4.0,
            },
        );

        let spot = MinuteWindowData::empty(MarketKind::Spot, ts_bucket);
        let ctx = IndicatorContext {
            ts_bucket,
            symbol: "ETHUSDT".to_string(),
            futures,
            spot,
            history_futures: Vec::new(),
            history_spot: Vec::new(),
            trade_history_futures: Vec::new(),
            trade_history_spot: Vec::new(),
            latest_mark: None,
            latest_funding: None,
            funding_changes_in_window: Vec::new(),
            funding_points_in_window: Vec::new(),
            mark_points_in_window: Vec::new(),
            funding_changes_recent: Vec::new(),
            funding_points_recent: Vec::new(),
            mark_points_recent: Vec::new(),
            whale_threshold_usdt: 300_000.0,
            kline_history_bars_1m: 1024,
            kline_history_bars_15m: 120,
            kline_history_bars_4h: 120,
            kline_history_bars_1d: 120,
            kline_history_fill_1d_from_db: true,
            fvg_windows: vec![
                "15m".to_string(),
                "4h".to_string(),
                "1d".to_string(),
                "3d".to_string(),
            ],
            fvg_fill_from_db: true,
            fvg_db_bars_4h: 256,
            fvg_db_bars_1d: 256,
            fvg_epsilon_gap_ticks: 2,
            fvg_atr_lookback: 14,
            fvg_min_body_ratio: 0.60,
            fvg_min_impulse_atr_ratio: 1.30,
            fvg_min_gap_atr_ratio: 0.15,
            fvg_max_gap_atr_ratio: 1.20,
            fvg_mitigated_fill_threshold: 0.80,
            fvg_invalid_close_bars: 1,
            kline_history_futures_4h_db: Vec::new(),
            kline_history_futures_1d_db: Vec::new(),
            kline_history_spot_4h_db: Vec::new(),
            kline_history_spot_1d_db: Vec::new(),
            tpo_rows_nb: 64,
            tpo_value_area_pct: 0.70,
            tpo_session_windows: vec!["4h".to_string(), "1d".to_string()],
            tpo_ib_minutes: 60,
            tpo_dev_output_windows: vec!["15m".to_string(), "1h".to_string()],
            rvwap_windows: vec!["15m".to_string(), "4h".to_string(), "1d".to_string()],
            rvwap_output_windows: vec!["15m".to_string(), "1h".to_string()],
            rvwap_min_samples: 5,
            high_volume_pulse_z_windows: vec!["1h".to_string(), "4h".to_string(), "1d".to_string()],
            high_volume_pulse_summary_windows: vec!["15m".to_string(), "1h".to_string()],
            high_volume_pulse_min_samples: 5,
            ema_base_periods: vec![13, 21, 34],
            ema_htf_periods: vec![100, 200],
            ema_htf_windows: vec!["4h".to_string(), "1d".to_string()],
            ema_output_windows: vec!["15m".to_string(), "1h".to_string()],
            ema_fill_from_db: true,
            ema_db_bars_4h: 256,
            ema_db_bars_1d: 256,
            divergence_sig_test_mode: DivergenceSigTestMode::Threshold,
            divergence_bootstrap_b: 200,
            divergence_bootstrap_block_len: 5,
            divergence_p_value_threshold: 0.05,
            window_codes: vec!["1m".to_string()],
        };

        let computation = I01PriceVolumeStructure.evaluate(&ctx);
        let snapshot = computation.snapshot.expect("snapshot must exist");
        let payload = snapshot
            .payload_json
            .as_object()
            .expect("payload must be object");

        let by_window = payload
            .get("by_window")
            .and_then(|v| v.as_object())
            .expect("by_window must exist");

        assert_eq!(by_window.len(), 4);
        assert!(by_window.contains_key("15m"));
        assert!(by_window.contains_key("4h"));
        assert!(by_window.contains_key("1d"));
        assert!(by_window.contains_key("3d"));
        assert!(!by_window.contains_key("1h"));

        for window in ["15m", "4h", "1d"] {
            let window_payload = by_window
                .get(window)
                .and_then(|v| v.as_object())
                .expect("window payload must be object");
            assert_eq!(
                window_payload
                    .get("window_bars_used")
                    .and_then(|v| v.as_u64()),
                Some(1)
            );
        }
    }

    #[test]
    fn pvs_raw_audit_rows_are_bounded_for_multi_window_profiles() {
        let mut profile = BTreeMap::new();
        let mut total_volume = 0.0;
        for tick in 1..=400_i64 {
            let buy_qty = (401 - tick) as f64;
            let sell_qty = (tick % 17 + 1) as f64;
            total_volume += buy_qty + sell_qty;
            profile.insert(tick, LevelAgg { buy_qty, sell_qty });
        }

        let rows_1m = build_level_rows("price_volume_structure", "1m", &profile, total_volume);
        assert!(
            rows_1m.len()
                <= RAW_AUDIT_TOP_RANKS_MULTI_WINDOW + RAW_AUDIT_HVN_CAP + RAW_AUDIT_LVN_CAP + 3
        );
        assert!(rows_1m.iter().all(|row| {
            row.metrics_json
                .get("audit_capture_policy")
                .and_then(|v| v.as_str())
                == Some("structural_top_ranked_subset")
        }));

        let rows_3d = build_level_rows("price_volume_structure", "3d", &profile, total_volume);
        assert!(
            rows_3d.len()
                <= RAW_AUDIT_TOP_RANKS_MULTI_WINDOW + RAW_AUDIT_HVN_CAP + RAW_AUDIT_LVN_CAP + 3
        );
        assert!(rows_3d.iter().any(|row| {
            row.metrics_json
                .get("is_poc")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
        }));
        assert!(rows_3d.iter().all(|row| {
            row.metrics_json
                .get("audit_capture_policy")
                .and_then(|v| v.as_str())
                == Some("structural_top_ranked_subset")
        }));
    }

    #[test]
    fn pvs_by_window_excludes_current_bar_from_history_tail() {
        fn history_bar(
            ts_bucket: chrono::DateTime<Utc>,
            total_qty: f64,
            tick: i64,
            buy_qty: f64,
            sell_qty: f64,
        ) -> MinuteHistory {
            let mut profile = std::collections::BTreeMap::new();
            profile.insert(tick, LevelAgg { buy_qty, sell_qty });
            MinuteHistory {
                ts_bucket,
                market: MarketKind::Futures,
                open_price: Some(tick as f64 / 100.0),
                high_price: Some(tick as f64 / 100.0),
                low_price: Some(tick as f64 / 100.0),
                close_price: Some(tick as f64 / 100.0),
                last_price: Some(tick as f64 / 100.0),
                buy_qty,
                sell_qty,
                total_qty,
                total_notional: 0.0,
                delta: buy_qty - sell_qty,
                relative_delta: 0.0,
                force_liq: std::collections::BTreeMap::new(),
                ofi: 0.0,
                spread_twa: None,
                topk_depth_twa: None,
                obi_twa: None,
                obi_l1_twa: None,
                obi_k_twa: None,
                obi_k_dw_twa: None,
                obi_k_dw_close: None,
                obi_k_dw_change: None,
                obi_k_dw_adj_twa: None,
                bbo_updates: 0,
                microprice_twa: None,
                microprice_classic_twa: None,
                microprice_kappa_twa: None,
                microprice_adj_twa: None,
                cvd: 0.0,
                vpin: 0.0,
                avwap_minute: None,
                whale_trade_count: 0,
                whale_buy_count: 0,
                whale_sell_count: 0,
                whale_notional_total: 0.0,
                whale_notional_buy: 0.0,
                whale_notional_sell: 0.0,
                whale_qty_eth_total: 0.0,
                whale_qty_eth_buy: 0.0,
                whale_qty_eth_sell: 0.0,
                profile,
            }
        }

        let ts_bucket = Utc
            .with_ymd_and_hms(2026, 3, 4, 0, 2, 0)
            .single()
            .expect("valid timestamp");

        let mut futures = MinuteWindowData::empty(MarketKind::Futures, ts_bucket);
        futures.total_qty = 10.0;
        futures.profile.insert(
            300,
            LevelAgg {
                buy_qty: 10.0,
                sell_qty: 0.0,
            },
        );

        let spot = MinuteWindowData::empty(MarketKind::Spot, ts_bucket);
        let ctx = IndicatorContext {
            ts_bucket,
            symbol: "ETHUSDT".to_string(),
            futures,
            spot,
            // NOTE: history includes the current minute at the tail.
            history_futures: vec![
                history_bar(ts_bucket - chrono::Duration::minutes(2), 1.0, 100, 1.0, 0.0),
                history_bar(ts_bucket - chrono::Duration::minutes(1), 2.0, 200, 2.0, 0.0),
                history_bar(ts_bucket, 10.0, 300, 10.0, 0.0),
            ],
            history_spot: Vec::new(),
            trade_history_futures: vec![
                history_bar(ts_bucket - chrono::Duration::minutes(2), 1.0, 100, 1.0, 0.0),
                history_bar(ts_bucket - chrono::Duration::minutes(1), 2.0, 200, 2.0, 0.0),
                history_bar(ts_bucket, 10.0, 300, 10.0, 0.0),
            ],
            trade_history_spot: Vec::new(),
            latest_mark: None,
            latest_funding: None,
            funding_changes_in_window: Vec::new(),
            funding_points_in_window: Vec::new(),
            mark_points_in_window: Vec::new(),
            funding_changes_recent: Vec::new(),
            funding_points_recent: Vec::new(),
            mark_points_recent: Vec::new(),
            whale_threshold_usdt: 300_000.0,
            kline_history_bars_1m: 1024,
            kline_history_bars_15m: 120,
            kline_history_bars_4h: 120,
            kline_history_bars_1d: 120,
            kline_history_fill_1d_from_db: true,
            fvg_windows: vec![
                "15m".to_string(),
                "4h".to_string(),
                "1d".to_string(),
                "3d".to_string(),
            ],
            fvg_fill_from_db: true,
            fvg_db_bars_4h: 256,
            fvg_db_bars_1d: 256,
            fvg_epsilon_gap_ticks: 2,
            fvg_atr_lookback: 14,
            fvg_min_body_ratio: 0.60,
            fvg_min_impulse_atr_ratio: 1.30,
            fvg_min_gap_atr_ratio: 0.15,
            fvg_max_gap_atr_ratio: 1.20,
            fvg_mitigated_fill_threshold: 0.80,
            fvg_invalid_close_bars: 1,
            kline_history_futures_4h_db: Vec::new(),
            kline_history_futures_1d_db: Vec::new(),
            kline_history_spot_4h_db: Vec::new(),
            kline_history_spot_1d_db: Vec::new(),
            tpo_rows_nb: 64,
            tpo_value_area_pct: 0.70,
            tpo_session_windows: vec!["4h".to_string(), "1d".to_string()],
            tpo_ib_minutes: 60,
            tpo_dev_output_windows: vec!["15m".to_string(), "1h".to_string()],
            rvwap_windows: vec!["15m".to_string(), "4h".to_string(), "1d".to_string()],
            rvwap_output_windows: vec!["15m".to_string(), "1h".to_string()],
            rvwap_min_samples: 5,
            high_volume_pulse_z_windows: vec!["1h".to_string(), "4h".to_string(), "1d".to_string()],
            high_volume_pulse_summary_windows: vec!["15m".to_string(), "1h".to_string()],
            high_volume_pulse_min_samples: 5,
            ema_base_periods: vec![13, 21, 34],
            ema_htf_periods: vec![100, 200],
            ema_htf_windows: vec!["4h".to_string(), "1d".to_string()],
            ema_output_windows: vec!["15m".to_string(), "1h".to_string()],
            ema_fill_from_db: true,
            ema_db_bars_4h: 256,
            ema_db_bars_1d: 256,
            divergence_sig_test_mode: DivergenceSigTestMode::Threshold,
            divergence_bootstrap_b: 200,
            divergence_bootstrap_block_len: 5,
            divergence_p_value_threshold: 0.05,
            window_codes: vec!["1m".to_string()],
        };

        let computation = I01PriceVolumeStructure.evaluate(&ctx);
        let payload = computation
            .snapshot
            .expect("snapshot")
            .payload_json
            .as_object()
            .cloned()
            .expect("payload object");
        let by_window = payload
            .get("by_window")
            .and_then(|v| v.as_object())
            .expect("by_window object");
        let w15 = by_window
            .get("15m")
            .and_then(|v| v.as_object())
            .expect("15m object");

        let bar_volume = w15
            .get("bar_volume")
            .and_then(|v| v.as_f64())
            .expect("bar_volume");
        let bars_used = w15
            .get("window_bars_used")
            .and_then(|v| v.as_u64())
            .expect("window_bars_used");

        // expected = 1 + 2 + 10, not 1 + 2 + 10 + 10
        assert!((bar_volume - 13.0).abs() < 1e-9);
        assert_eq!(bars_used, 3);
    }

    #[test]
    fn pvs_by_window_uses_trade_history_backfill_when_general_history_has_gap() {
        fn history_bar(
            ts_bucket: chrono::DateTime<Utc>,
            total_qty: f64,
            tick: i64,
            buy_qty: f64,
            sell_qty: f64,
        ) -> MinuteHistory {
            let mut profile = std::collections::BTreeMap::new();
            profile.insert(tick, LevelAgg { buy_qty, sell_qty });
            MinuteHistory {
                ts_bucket,
                market: MarketKind::Futures,
                open_price: Some(tick as f64 / 100.0),
                high_price: Some(tick as f64 / 100.0),
                low_price: Some(tick as f64 / 100.0),
                close_price: Some(tick as f64 / 100.0),
                last_price: Some(tick as f64 / 100.0),
                buy_qty,
                sell_qty,
                total_qty,
                total_notional: 0.0,
                delta: buy_qty - sell_qty,
                relative_delta: 0.0,
                force_liq: std::collections::BTreeMap::new(),
                ofi: 0.0,
                spread_twa: None,
                topk_depth_twa: None,
                obi_twa: None,
                obi_l1_twa: None,
                obi_k_twa: None,
                obi_k_dw_twa: None,
                obi_k_dw_close: None,
                obi_k_dw_change: None,
                obi_k_dw_adj_twa: None,
                bbo_updates: 0,
                microprice_twa: None,
                microprice_classic_twa: None,
                microprice_kappa_twa: None,
                microprice_adj_twa: None,
                cvd: 0.0,
                vpin: 0.0,
                avwap_minute: None,
                whale_trade_count: 0,
                whale_buy_count: 0,
                whale_sell_count: 0,
                whale_notional_total: 0.0,
                whale_notional_buy: 0.0,
                whale_notional_sell: 0.0,
                whale_qty_eth_total: 0.0,
                whale_qty_eth_buy: 0.0,
                whale_qty_eth_sell: 0.0,
                profile,
            }
        }

        let ts_bucket = Utc
            .with_ymd_and_hms(2026, 3, 4, 0, 2, 0)
            .single()
            .expect("valid timestamp");

        let mut futures = MinuteWindowData::empty(MarketKind::Futures, ts_bucket);
        futures.total_qty = 10.0;
        futures.profile.insert(
            300,
            LevelAgg {
                buy_qty: 10.0,
                sell_qty: 0.0,
            },
        );

        let spot = MinuteWindowData::empty(MarketKind::Spot, ts_bucket);
        let ctx = IndicatorContext {
            ts_bucket,
            symbol: "ETHUSDT".to_string(),
            futures,
            spot,
            history_futures: vec![
                history_bar(ts_bucket - chrono::Duration::minutes(2), 1.0, 100, 1.0, 0.0),
                history_bar(ts_bucket, 10.0, 300, 10.0, 0.0),
            ],
            history_spot: Vec::new(),
            trade_history_futures: vec![
                history_bar(ts_bucket - chrono::Duration::minutes(2), 1.0, 100, 1.0, 0.0),
                history_bar(ts_bucket - chrono::Duration::minutes(1), 4.0, 200, 4.0, 0.0),
                history_bar(ts_bucket, 10.0, 300, 10.0, 0.0),
            ],
            trade_history_spot: Vec::new(),
            latest_mark: None,
            latest_funding: None,
            funding_changes_in_window: Vec::new(),
            funding_points_in_window: Vec::new(),
            mark_points_in_window: Vec::new(),
            funding_changes_recent: Vec::new(),
            funding_points_recent: Vec::new(),
            mark_points_recent: Vec::new(),
            whale_threshold_usdt: 300_000.0,
            kline_history_bars_1m: 1024,
            kline_history_bars_15m: 120,
            kline_history_bars_4h: 120,
            kline_history_bars_1d: 120,
            kline_history_fill_1d_from_db: true,
            fvg_windows: vec![
                "15m".to_string(),
                "4h".to_string(),
                "1d".to_string(),
                "3d".to_string(),
            ],
            fvg_fill_from_db: true,
            fvg_db_bars_4h: 256,
            fvg_db_bars_1d: 256,
            fvg_epsilon_gap_ticks: 2,
            fvg_atr_lookback: 14,
            fvg_min_body_ratio: 0.60,
            fvg_min_impulse_atr_ratio: 1.30,
            fvg_min_gap_atr_ratio: 0.15,
            fvg_max_gap_atr_ratio: 1.20,
            fvg_mitigated_fill_threshold: 0.80,
            fvg_invalid_close_bars: 1,
            kline_history_futures_4h_db: Vec::new(),
            kline_history_futures_1d_db: Vec::new(),
            kline_history_spot_4h_db: Vec::new(),
            kline_history_spot_1d_db: Vec::new(),
            tpo_rows_nb: 64,
            tpo_value_area_pct: 0.70,
            tpo_session_windows: vec!["4h".to_string(), "1d".to_string()],
            tpo_ib_minutes: 60,
            tpo_dev_output_windows: vec!["15m".to_string(), "1h".to_string()],
            rvwap_windows: vec!["15m".to_string(), "4h".to_string(), "1d".to_string()],
            rvwap_output_windows: vec!["15m".to_string(), "1h".to_string()],
            rvwap_min_samples: 5,
            high_volume_pulse_z_windows: vec!["1h".to_string(), "4h".to_string(), "1d".to_string()],
            high_volume_pulse_summary_windows: vec!["15m".to_string(), "1h".to_string()],
            high_volume_pulse_min_samples: 5,
            ema_base_periods: vec![13, 21, 34],
            ema_htf_periods: vec![100, 200],
            ema_htf_windows: vec!["4h".to_string(), "1d".to_string()],
            ema_output_windows: vec!["15m".to_string(), "1h".to_string()],
            ema_fill_from_db: true,
            ema_db_bars_4h: 256,
            ema_db_bars_1d: 256,
            divergence_sig_test_mode: DivergenceSigTestMode::Threshold,
            divergence_bootstrap_b: 200,
            divergence_bootstrap_block_len: 5,
            divergence_p_value_threshold: 0.05,
            window_codes: vec!["1m".to_string()],
        };

        let computation = I01PriceVolumeStructure.evaluate(&ctx);
        let payload = computation
            .snapshot
            .expect("snapshot")
            .payload_json
            .as_object()
            .cloned()
            .expect("payload object");
        let by_window = payload
            .get("by_window")
            .and_then(|v| v.as_object())
            .expect("by_window object");
        let w15 = by_window
            .get("15m")
            .and_then(|v| v.as_object())
            .expect("15m object");

        let bar_volume = w15
            .get("bar_volume")
            .and_then(|v| v.as_f64())
            .expect("bar_volume");
        let bars_used = w15
            .get("window_bars_used")
            .and_then(|v| v.as_u64())
            .expect("window_bars_used");

        assert!((bar_volume - 15.0).abs() < 1e-9);
        assert_eq!(bars_used, 3);
    }

    #[test]
    fn pvs_rank_is_stable_for_nearly_equal_levels() {
        let profile = BTreeMap::from([
            (
                100_i64,
                LevelAgg {
                    buy_qty: 5.0,
                    sell_qty: 5.0,
                },
            ),
            (
                101_i64,
                LevelAgg {
                    buy_qty: 5.0,
                    sell_qty: 5.0 + 1e-12,
                },
            ),
        ]);

        let payload = build_pvs_payload(
            &profile,
            profile.values().map(LevelAgg::total).sum(),
            None,
            false,
        );
        let levels = payload["levels"].as_array().expect("levels array");

        assert_eq!(levels[0]["price_level"].as_f64(), Some(1.0));
        assert_eq!(levels[0]["level_rank"].as_i64(), Some(1));
        assert_eq!(levels[1]["price_level"].as_f64(), Some(1.01));
        assert_eq!(levels[1]["level_rank"].as_i64(), Some(2));
    }

    #[test]
    fn pvs_v2_zones_do_not_expand_beyond_candidate_bins() {
        let smoothed = vec![0.1, 1.0, 0.85, 0.1];
        let zones = build_node_zones(
            &smoothed,
            1.0,
            PvsV2WindowConfig {
                rows: 4,
                hvn_pct_of_poc: 0.8,
                lvn_pct_of_poc: 0.2,
                strength: 1,
                min_zone_width_bins: 1,
            },
            true,
        );

        assert_eq!(zones.len(), 1);
        assert_eq!(zones[0].start_idx, 1);
        assert_eq!(zones[0].end_idx, 1);
    }
}
