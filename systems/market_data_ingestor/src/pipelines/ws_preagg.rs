use crate::aggregate::minute_agg::DEFAULT_WHALE_THRESHOLD_USDT;
use crate::normalize::NormalizedMdEvent;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};

const ORDERBOOK_PREBATCH_MAX_EVENTS: usize = 20_000;
const TRADE_PREBATCH_MAX_EVENTS: usize = 20_000;
const TRADE_PREAGG_PRICE_SCALE: f64 = 100.0;
const TRADE_VPIN_BUCKET_SIZE_ETH: f64 = 50.0;
const TRADE_VPIN_ROLLING_BUCKETS: usize = 50;
const TRADE_VPIN_EPS: f64 = 1e-12;
const OB_PREAGG_PRICE_SCALE: f64 = 100.0;
const OB_PREAGG_TOPK: usize = 5;
const OB_PREAGG_DW_LAMBDA: f64 = 0.35;
const OB_PREAGG_MICROPRICE_KAPPA: f64 = 0.35;
const OB_PREAGG_MICROPRICE_LAMBDA_OBI: f64 = 0.15;
const OB_PREAGG_SIZE_FLOOR: f64 = 1e-3;
const OB_PREAGG_TICK_SIZE: f64 = 1.0 / OB_PREAGG_PRICE_SCALE;

#[derive(Debug, Clone, Default)]
struct TradeLevelAgg {
    buy_qty: f64,
    sell_qty: f64,
}

#[derive(Debug, Clone, Default)]
struct TradeVpinSnapshot {
    current_buy: f64,
    current_sell: f64,
    current_fill: f64,
    imbalances: Vec<f64>,
    imbalance_sum: f64,
    last_vpin: f64,
}

#[derive(Debug, Clone)]
struct TradeVpinState {
    bucket_size: f64,
    rolling_bucket_count: usize,
    current_buy: f64,
    current_sell: f64,
    current_fill: f64,
    imbalances: Vec<f64>,
    imbalance_sum: f64,
    last_vpin: f64,
}

impl Default for TradeVpinState {
    fn default() -> Self {
        Self {
            bucket_size: TRADE_VPIN_BUCKET_SIZE_ETH,
            rolling_bucket_count: TRADE_VPIN_ROLLING_BUCKETS,
            current_buy: 0.0,
            current_sell: 0.0,
            current_fill: 0.0,
            imbalances: Vec::new(),
            imbalance_sum: 0.0,
            last_vpin: 0.0,
        }
    }
}

impl TradeVpinState {
    fn ingest_flow(&mut self, buy_qty: f64, sell_qty: f64) {
        let mut remain_buy = buy_qty.max(0.0);
        let mut remain_sell = sell_qty.max(0.0);

        while remain_buy + remain_sell > TRADE_VPIN_EPS {
            if self.current_fill + TRADE_VPIN_EPS >= self.bucket_size {
                self.close_bucket();
            }

            let remain_total = remain_buy + remain_sell;
            let capacity = (self.bucket_size - self.current_fill).max(0.0);
            if capacity <= TRADE_VPIN_EPS {
                break;
            }

            let take = remain_total.min(capacity);
            let buy_part = take * (remain_buy / remain_total);
            let sell_part = take - buy_part;

            self.current_buy += buy_part;
            self.current_sell += sell_part;
            self.current_fill += take;
            remain_buy -= buy_part;
            remain_sell -= sell_part;

            if self.current_fill + TRADE_VPIN_EPS >= self.bucket_size {
                self.close_bucket();
            }
        }
    }

    fn snapshot(&self) -> TradeVpinSnapshot {
        TradeVpinSnapshot {
            current_buy: self.current_buy,
            current_sell: self.current_sell,
            current_fill: self.current_fill,
            imbalances: self.imbalances.clone(),
            imbalance_sum: self.imbalance_sum,
            last_vpin: self.last_vpin,
        }
    }

    fn close_bucket(&mut self) {
        if self.current_fill <= TRADE_VPIN_EPS {
            self.current_buy = 0.0;
            self.current_sell = 0.0;
            self.current_fill = 0.0;
            return;
        }

        let imbalance = (self.current_buy - self.current_sell).abs();
        self.imbalances.push(imbalance);
        self.imbalance_sum += imbalance;
        while self.imbalances.len() > self.rolling_bucket_count {
            if let Some(dropped) = self.imbalances.first().copied() {
                self.imbalance_sum -= dropped;
            }
            if !self.imbalances.is_empty() {
                self.imbalances.remove(0);
            }
        }
        if self.imbalances.len() >= self.rolling_bucket_count && self.bucket_size > TRADE_VPIN_EPS {
            let denom = self.rolling_bucket_count as f64 * self.bucket_size;
            self.last_vpin = (self.imbalance_sum / denom).clamp(0.0, 1.0);
        } else {
            self.last_vpin = 0.0;
        }
        self.current_buy = 0.0;
        self.current_sell = 0.0;
        self.current_fill = 0.0;
    }
}

#[derive(Debug, Clone, Default)]
struct TradeWhaleStats {
    trade_count: i64,
    buy_count: i64,
    sell_count: i64,
    notional_total: f64,
    notional_buy: f64,
    notional_sell: f64,
    qty_eth_total: f64,
    qty_eth_buy: f64,
    qty_eth_sell: f64,
    max_single_notional: f64,
}

#[derive(Debug, Clone, Copy, Default)]
struct OrderbookScalarSample {
    spread: f64,
    topk_depth: f64,
    obi: f64,
    obi_l1: f64,
    obi_k: f64,
    obi_k_dw: f64,
    obi_k_dw_change: f64,
    obi_k_dw_adj: f64,
    microprice: f64,
    microprice_classic: f64,
    microprice_kappa: f64,
    microprice_adj: f64,
    ofi: f64,
    bbo_updates: i64,
}

#[derive(Debug, Clone, Copy, Default)]
struct OrderbookBboState {
    bid_qty: f64,
    ask_qty: f64,
}

#[derive(Debug, Clone, Default)]
struct HeatmapLevelAcc {
    qty: f64,
    last_sample_idx: i64,
    sum_qty_weighted: f64,
}

#[derive(Debug, Clone, Default)]
struct OrderbookPreAggState {
    bids: BTreeMap<i64, f64>,
    asks: BTreeMap<i64, f64>,
    last_bbo: Option<OrderbookBboState>,
    last_obi_k_dw: Option<f64>,
}

impl OrderbookPreAggState {
    fn apply_depth_delta(&mut self, bids_delta: &[(f64, f64)], asks_delta: &[(f64, f64)]) {
        for (price, qty) in bids_delta {
            let tick = ob_price_to_tick(*price);
            if *qty <= 0.0 {
                self.bids.remove(&tick);
            } else {
                self.bids.insert(tick, *qty);
            }
        }
        for (price, qty) in asks_delta {
            let tick = ob_price_to_tick(*price);
            if *qty <= 0.0 {
                self.asks.remove(&tick);
            } else {
                self.asks.insert(tick, *qty);
            }
        }
    }

    fn apply_snapshot(&mut self, bids: &[(f64, f64)], asks: &[(f64, f64)]) {
        self.bids.clear();
        self.asks.clear();
        for (price, qty) in bids {
            self.bids.insert(ob_price_to_tick(*price), *qty);
        }
        for (price, qty) in asks {
            self.asks.insert(ob_price_to_tick(*price), *qty);
        }
    }

    fn apply_bbo(&mut self, bid_price: f64, bid_qty: f64, ask_price: f64, ask_qty: f64) -> f64 {
        self.bids.insert(ob_price_to_tick(bid_price), bid_qty);
        self.asks.insert(ob_price_to_tick(ask_price), ask_qty);
        let current = OrderbookBboState { bid_qty, ask_qty };
        let ofi = if let Some(prev) = self.last_bbo {
            (current.bid_qty - prev.bid_qty) - (current.ask_qty - prev.ask_qty)
        } else {
            0.0
        };
        self.last_bbo = Some(current);
        ofi
    }

    fn sample_scalar_from_depth(&mut self) -> OrderbookScalarSample {
        let best_bid = self
            .bids
            .iter()
            .next_back()
            .map(|(p, q)| (ob_tick_to_price(*p), *q));
        let best_ask = self
            .asks
            .iter()
            .next()
            .map(|(p, q)| (ob_tick_to_price(*p), *q));

        let (spread, microprice_classic, microprice_kappa, microprice_adj, obi_l1) =
            match (best_bid, best_ask) {
                (Some((bp, bq)), Some((ap, aq))) => {
                    let spread = (ap - bp).max(0.0);
                    let denom = (bq + aq).max(1e-12);
                    let mid = (ap + bp) / 2.0;
                    let micro_classic = (ap * bq + bp * aq) / denom;
                    let bid_floor = bq.max(OB_PREAGG_SIZE_FLOOR);
                    let ask_floor = aq.max(OB_PREAGG_SIZE_FLOOR);
                    let bid_pow = (bid_floor + 1e-12).powf(OB_PREAGG_MICROPRICE_KAPPA);
                    let ask_pow = (ask_floor + 1e-12).powf(OB_PREAGG_MICROPRICE_KAPPA);
                    let micro_kappa = (bid_pow * ap + ask_pow * bp) / (bid_pow + ask_pow + 1e-12);
                    let obi_l1 = (bq - aq) / denom;
                    let (lo, hi) = if bp <= ap { (bp, ap) } else { (ap, bp) };
                    let micro_adj = (mid
                        + OB_PREAGG_MICROPRICE_LAMBDA_OBI * obi_l1 * (OB_PREAGG_TICK_SIZE / 2.0))
                        .clamp(lo, hi);
                    (spread, micro_classic, micro_kappa, micro_adj, obi_l1)
                }
                _ => (0.0, 0.0, 0.0, 0.0, 0.0),
            };

        let sum_bid: f64 = self
            .bids
            .iter()
            .rev()
            .take(OB_PREAGG_TOPK)
            .map(|(_, q)| *q)
            .sum();
        let sum_ask: f64 = self.asks.iter().take(OB_PREAGG_TOPK).map(|(_, q)| *q).sum();
        let topk_depth = sum_bid + sum_ask;
        let obi_k = if topk_depth > 0.0 {
            (sum_bid - sum_ask) / topk_depth
        } else {
            0.0
        };
        let (dw_bid, dw_ask) = ob_weighted_topk_depth(&self.bids, &self.asks, OB_PREAGG_TOPK);
        let dw_total = dw_bid + dw_ask;
        let obi_k_dw = if dw_total > 0.0 {
            (dw_bid - dw_ask) / dw_total
        } else {
            0.0
        };
        let obi_k_dw_change = self
            .last_obi_k_dw
            .map(|prev| obi_k_dw - prev)
            .unwrap_or(0.0);
        self.last_obi_k_dw = Some(obi_k_dw);
        let depth_factor = (topk_depth / (topk_depth + 1.0)).clamp(0.0, 1.0);
        let spread_factor = if spread <= 1e-12 {
            1.0
        } else {
            (OB_PREAGG_TICK_SIZE / spread).clamp(0.0, 1.0)
        };
        let obi_k_dw_adj = obi_k_dw * depth_factor * spread_factor;

        OrderbookScalarSample {
            spread,
            topk_depth,
            obi: obi_k,
            obi_l1,
            obi_k,
            obi_k_dw,
            obi_k_dw_change,
            obi_k_dw_adj,
            microprice: microprice_kappa,
            microprice_classic,
            microprice_kappa,
            microprice_adj,
            ofi: 0.0,
            bbo_updates: 0,
        }
    }
}

#[derive(Debug, Clone)]
struct OrderbookSecondChunk {
    second_ms: i64,
    market: String,
    symbol: String,
    chunk_start_ts: chrono::DateTime<Utc>,
    chunk_end_ts: chrono::DateTime<Utc>,
    backfill_in_progress: bool,
    source_event_count: i64,
    sample_count: i64,
    bbo_updates: i64,
    spread_sum: f64,
    topk_depth_sum: f64,
    obi_sum: f64,
    obi_l1_sum: f64,
    obi_k_sum: f64,
    obi_k_dw_sum: f64,
    obi_k_dw_change_sum: f64,
    obi_k_dw_adj_sum: f64,
    microprice_sum: f64,
    microprice_classic_sum: f64,
    microprice_kappa_sum: f64,
    microprice_adj_sum: f64,
    ofi_sum: f64,
    obi_k_dw_close: Option<f64>,
    sample_cursor: i64,
    heatmap_bids: HashMap<i64, HeatmapLevelAcc>,
    heatmap_asks: HashMap<i64, HeatmapLevelAcc>,
}

impl OrderbookSecondChunk {
    fn new(event: &NormalizedMdEvent) -> Self {
        let second_ms = floor_timestamp_millis(event.event_ts.timestamp_millis(), 1_000);
        Self {
            second_ms,
            market: event.market.clone(),
            symbol: event.symbol.clone(),
            chunk_start_ts: event.event_ts,
            chunk_end_ts: event.event_ts,
            backfill_in_progress: event.backfill_in_progress,
            source_event_count: 0,
            sample_count: 0,
            bbo_updates: 0,
            spread_sum: 0.0,
            topk_depth_sum: 0.0,
            obi_sum: 0.0,
            obi_l1_sum: 0.0,
            obi_k_sum: 0.0,
            obi_k_dw_sum: 0.0,
            obi_k_dw_change_sum: 0.0,
            obi_k_dw_adj_sum: 0.0,
            microprice_sum: 0.0,
            microprice_classic_sum: 0.0,
            microprice_kappa_sum: 0.0,
            microprice_adj_sum: 0.0,
            ofi_sum: 0.0,
            obi_k_dw_close: None,
            sample_cursor: 0,
            heatmap_bids: HashMap::new(),
            heatmap_asks: HashMap::new(),
        }
    }

    fn touch(&mut self, event: &NormalizedMdEvent) {
        if event.event_ts < self.chunk_start_ts {
            self.chunk_start_ts = event.event_ts;
        }
        if event.event_ts >= self.chunk_end_ts {
            self.chunk_end_ts = event.event_ts;
        }
        self.backfill_in_progress |= event.backfill_in_progress;
        self.source_event_count = self.source_event_count.saturating_add(1);
    }

    fn apply_scalar_sample(&mut self, sample: &OrderbookScalarSample, weight: i64) {
        let samples = weight.max(1);
        let weight_f = samples as f64;
        self.sample_count = self.sample_count.saturating_add(samples);
        self.bbo_updates = self
            .bbo_updates
            .saturating_add(sample.bbo_updates.saturating_mul(samples));
        self.spread_sum += sample.spread * weight_f;
        self.topk_depth_sum += sample.topk_depth * weight_f;
        self.obi_sum += sample.obi * weight_f;
        self.obi_l1_sum += sample.obi_l1 * weight_f;
        self.obi_k_sum += sample.obi_k * weight_f;
        self.obi_k_dw_sum += sample.obi_k_dw * weight_f;
        self.obi_k_dw_change_sum += sample.obi_k_dw_change * weight_f;
        self.obi_k_dw_adj_sum += sample.obi_k_dw_adj * weight_f;
        self.microprice_sum += sample.microprice * weight_f;
        self.microprice_classic_sum += sample.microprice_classic * weight_f;
        self.microprice_kappa_sum += sample.microprice_kappa * weight_f;
        self.microprice_adj_sum += sample.microprice_adj * weight_f;
        self.ofi_sum += sample.ofi * weight_f;
        self.obi_k_dw_close = Some(sample.obi_k_dw);
        self.sample_cursor = self.sample_cursor.saturating_add(samples);
    }

    fn update_bid_qty(&mut self, tick: i64, qty: f64) {
        Self::update_level_qty(&mut self.heatmap_bids, tick, qty, self.sample_cursor);
    }

    fn update_ask_qty(&mut self, tick: i64, qty: f64) {
        Self::update_level_qty(&mut self.heatmap_asks, tick, qty, self.sample_cursor);
    }

    fn update_level_qty(
        levels: &mut HashMap<i64, HeatmapLevelAcc>,
        tick: i64,
        qty: f64,
        sample_cursor: i64,
    ) {
        let entry = levels.entry(tick).or_insert_with(|| HeatmapLevelAcc {
            qty: 0.0,
            last_sample_idx: sample_cursor,
            sum_qty_weighted: 0.0,
        });
        let delta = sample_cursor.saturating_sub(entry.last_sample_idx);
        if delta > 0 {
            entry.sum_qty_weighted += entry.qty * (delta as f64);
            entry.last_sample_idx = sample_cursor;
        }
        entry.qty = qty.max(0.0);
    }

    fn finalize_to_event(mut self) -> NormalizedMdEvent {
        let final_cursor = self.sample_cursor;
        for level in self.heatmap_bids.values_mut() {
            let delta = final_cursor.saturating_sub(level.last_sample_idx);
            if delta > 0 {
                level.sum_qty_weighted += level.qty * (delta as f64);
                level.last_sample_idx = final_cursor;
            }
        }
        for level in self.heatmap_asks.values_mut() {
            let delta = final_cursor.saturating_sub(level.last_sample_idx);
            if delta > 0 {
                level.sum_qty_weighted += level.qty * (delta as f64);
                level.last_sample_idx = final_cursor;
            }
        }

        let mut merged_heatmap: BTreeMap<i64, (f64, f64)> = BTreeMap::new();
        for (tick, level) in &self.heatmap_bids {
            if level.sum_qty_weighted != 0.0 {
                let row = merged_heatmap.entry(*tick).or_insert((0.0, 0.0));
                row.0 += level.sum_qty_weighted;
            }
        }
        for (tick, level) in &self.heatmap_asks {
            if level.sum_qty_weighted != 0.0 {
                let row = merged_heatmap.entry(*tick).or_insert((0.0, 0.0));
                row.1 += level.sum_qty_weighted;
            }
        }
        let heatmap_levels = merged_heatmap
            .into_iter()
            .map(|(tick, (bid_liq, ask_liq))| json!([ob_tick_to_price(tick), bid_liq, ask_liq]))
            .collect::<Vec<_>>();

        let ts_bucket = millis_to_utc(self.second_ms);
        let data = json!({
            "ts_bucket": ts_bucket.to_rfc3339(),
            "chunk_start_ts": self.chunk_start_ts.to_rfc3339(),
            "chunk_end_ts": self.chunk_end_ts.to_rfc3339(),
            "source_event_count": self.source_event_count,
            "sample_count": self.sample_count,
            "bbo_updates": self.bbo_updates,
            "spread_sum": self.spread_sum,
            "topk_depth_sum": self.topk_depth_sum,
            "obi_sum": self.obi_sum,
            "obi_l1_sum": self.obi_l1_sum,
            "obi_k_sum": self.obi_k_sum,
            "obi_k_dw_sum": self.obi_k_dw_sum,
            "obi_k_dw_change_sum": self.obi_k_dw_change_sum,
            "obi_k_dw_adj_sum": self.obi_k_dw_adj_sum,
            "microprice_sum": self.microprice_sum,
            "microprice_classic_sum": self.microprice_classic_sum,
            "microprice_kappa_sum": self.microprice_kappa_sum,
            "microprice_adj_sum": self.microprice_adj_sum,
            "ofi_sum": self.ofi_sum,
            "obi_k_dw_close": self.obi_k_dw_close,
            "heatmap_levels": heatmap_levels,
            "payload_json": {
                "agg_mode": "batch_incremental_1s_combinable"
            }
        });
        NormalizedMdEvent {
            msg_type: "md.agg.orderbook.1s".to_string(),
            market: self.market.clone(),
            symbol: self.symbol.clone(),
            source_kind: "derived".to_string(),
            backfill_in_progress: self.backfill_in_progress,
            routing_key: format!(
                "md.agg.{}.orderbook.1s.{}",
                self.market,
                self.symbol.to_lowercase()
            ),
            stream_name: "agg.orderbook.1s".to_string(),
            event_ts: self.chunk_end_ts,
            data,
        }
    }
}

#[derive(Debug, Clone)]
struct TradeSecondChunk {
    second_ms: i64,
    market: String,
    symbol: String,
    chunk_start_ts: chrono::DateTime<Utc>,
    chunk_end_ts: chrono::DateTime<Utc>,
    backfill_in_progress: bool,
    source_event_count: i64,
    trade_count: i64,
    buy_qty: f64,
    sell_qty: f64,
    buy_notional: f64,
    sell_notional: f64,
    first_price: Option<f64>,
    last_price: Option<f64>,
    high_price: Option<f64>,
    low_price: Option<f64>,
    profile: BTreeMap<i64, TradeLevelAgg>,
    whale: TradeWhaleStats,
    vpin_snapshot: Option<TradeVpinSnapshot>,
}

impl TradeSecondChunk {
    fn new(event: &NormalizedMdEvent) -> Self {
        let second_ms = floor_timestamp_millis(event.event_ts.timestamp_millis(), 1_000);
        Self {
            second_ms,
            market: event.market.clone(),
            symbol: event.symbol.clone(),
            chunk_start_ts: event.event_ts,
            chunk_end_ts: event.event_ts,
            backfill_in_progress: event.backfill_in_progress,
            source_event_count: 0,
            trade_count: 0,
            buy_qty: 0.0,
            sell_qty: 0.0,
            buy_notional: 0.0,
            sell_notional: 0.0,
            first_price: None,
            last_price: None,
            high_price: None,
            low_price: None,
            profile: BTreeMap::new(),
            whale: TradeWhaleStats::default(),
            vpin_snapshot: None,
        }
    }

    fn touch(&mut self, event: &NormalizedMdEvent) {
        if event.event_ts < self.chunk_start_ts {
            self.chunk_start_ts = event.event_ts;
        }
        if event.event_ts >= self.chunk_end_ts {
            self.chunk_end_ts = event.event_ts;
        }
        self.backfill_in_progress |= event.backfill_in_progress;
        self.source_event_count = self.source_event_count.saturating_add(1);
    }

    fn apply_trade(
        &mut self,
        event: &NormalizedMdEvent,
        price: f64,
        qty_eth: f64,
        notional_usdt: f64,
        aggressor_side: i8,
        whale_threshold_usdt: f64,
    ) {
        self.touch(event);
        self.trade_count = self.trade_count.saturating_add(1);
        if aggressor_side >= 0 {
            self.buy_qty += qty_eth;
            self.buy_notional += notional_usdt;
        } else {
            self.sell_qty += qty_eth;
            self.sell_notional += notional_usdt;
        }

        if self.first_price.is_none() {
            self.first_price = Some(price);
        }
        self.last_price = Some(price);
        self.high_price = Some(self.high_price.map_or(price, |v| v.max(price)));
        self.low_price = Some(self.low_price.map_or(price, |v| v.min(price)));

        let tick = trade_price_to_tick(price);
        let level = self.profile.entry(tick).or_default();
        if aggressor_side >= 0 {
            level.buy_qty += qty_eth;
        } else {
            level.sell_qty += qty_eth;
        }

        if notional_usdt >= whale_threshold_usdt {
            self.whale.trade_count = self.whale.trade_count.saturating_add(1);
            self.whale.notional_total += notional_usdt;
            self.whale.qty_eth_total += qty_eth;
            if aggressor_side >= 0 {
                self.whale.buy_count = self.whale.buy_count.saturating_add(1);
                self.whale.notional_buy += notional_usdt;
                self.whale.qty_eth_buy += qty_eth;
            } else {
                self.whale.sell_count = self.whale.sell_count.saturating_add(1);
                self.whale.notional_sell += notional_usdt;
                self.whale.qty_eth_sell += qty_eth;
            }
            if notional_usdt > self.whale.max_single_notional {
                self.whale.max_single_notional = notional_usdt;
            }
        }
    }

    fn finalize_to_event(self) -> NormalizedMdEvent {
        let profile_levels = self
            .profile
            .into_iter()
            .map(|(tick, level)| json!([trade_tick_to_price(tick), level.buy_qty, level.sell_qty]))
            .collect::<Vec<_>>();
        let ts_bucket = millis_to_utc(self.second_ms);
        let mut payload_json = json!({
            "agg_mode": "batch_incremental_1s_combinable"
        });
        if let (Some(snapshot), Some(payload_obj)) =
            (self.vpin_snapshot.as_ref(), payload_json.as_object_mut())
        {
            payload_obj.insert(
                "vpin_state".to_string(),
                json!({
                    "current_buy": snapshot.current_buy,
                    "current_sell": snapshot.current_sell,
                    "current_fill": snapshot.current_fill,
                    "imbalances": snapshot.imbalances,
                    "imbalance_sum": snapshot.imbalance_sum,
                    "last_vpin": snapshot.last_vpin,
                }),
            );
        }

        let data = json!({
            "ts_bucket": ts_bucket.to_rfc3339(),
            "chunk_start_ts": self.chunk_start_ts.to_rfc3339(),
            "chunk_end_ts": self.chunk_end_ts.to_rfc3339(),
            "source_event_count": self.source_event_count,
            "trade_count": self.trade_count,
            "buy_qty": self.buy_qty,
            "sell_qty": self.sell_qty,
            "buy_notional": self.buy_notional,
            "sell_notional": self.sell_notional,
            "first_price": self.first_price,
            "last_price": self.last_price,
            "high_price": self.high_price,
            "low_price": self.low_price,
            "profile_levels": profile_levels,
            "whale": {
                "trade_count": self.whale.trade_count,
                "buy_count": self.whale.buy_count,
                "sell_count": self.whale.sell_count,
                "notional_total": self.whale.notional_total,
                "notional_buy": self.whale.notional_buy,
                "notional_sell": self.whale.notional_sell,
                "qty_eth_total": self.whale.qty_eth_total,
                "qty_eth_buy": self.whale.qty_eth_buy,
                "qty_eth_sell": self.whale.qty_eth_sell,
                "delta_qty_eth": self.whale.qty_eth_buy - self.whale.qty_eth_sell,
                "max_single_notional": self.whale.max_single_notional,
            },
            "payload_json": payload_json
        });
        NormalizedMdEvent {
            msg_type: "md.agg.trade.1s".to_string(),
            market: self.market.clone(),
            symbol: self.symbol.clone(),
            source_kind: "derived".to_string(),
            backfill_in_progress: self.backfill_in_progress,
            routing_key: format!(
                "md.agg.{}.trade.1s.{}",
                self.market,
                self.symbol.to_lowercase()
            ),
            stream_name: "agg.trade.1s".to_string(),
            event_ts: self.chunk_end_ts,
            data,
        }
    }
}

pub struct WsOneSecondPreAggregator {
    whale_threshold_usdt: f64,
    trade_chunks: HashMap<(String, String), TradeSecondChunk>,
    trade_vpin_states: HashMap<(String, String), TradeVpinState>,
    orderbook_states: HashMap<(String, String), OrderbookPreAggState>,
    orderbook_chunks: HashMap<(String, String), OrderbookSecondChunk>,
}

impl Default for WsOneSecondPreAggregator {
    fn default() -> Self {
        Self::new(DEFAULT_WHALE_THRESHOLD_USDT)
    }
}

impl WsOneSecondPreAggregator {
    pub fn new(whale_threshold_usdt: f64) -> Self {
        Self {
            whale_threshold_usdt,
            trade_chunks: HashMap::new(),
            trade_vpin_states: HashMap::new(),
            orderbook_states: HashMap::new(),
            orderbook_chunks: HashMap::new(),
        }
    }

    pub fn ingest_raw_event(&mut self, event: NormalizedMdEvent) -> Vec<NormalizedMdEvent> {
        match event.msg_type.as_str() {
            "md.trade" => self.ingest_trade_event(event),
            "md.depth" | "md.orderbook_snapshot_l2" | "md.bbo" => {
                self.ingest_orderbook_event(event)
            }
            _ => vec![event],
        }
    }

    pub fn flush_expired(&mut self, now: DateTime<Utc>) -> Vec<NormalizedMdEvent> {
        let now_second_ms = floor_timestamp_millis(now.timestamp_millis(), 1_000);
        let mut out = Vec::new();

        let trade_keys = self
            .trade_chunks
            .iter()
            .filter_map(|(key, chunk)| {
                if chunk.second_ms < now_second_ms {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        for key in trade_keys {
            if let Some(chunk) = self.trade_chunks.remove(&key) {
                out.push(chunk.finalize_to_event());
            }
        }

        let orderbook_keys = self
            .orderbook_chunks
            .iter()
            .filter_map(|(key, chunk)| {
                if chunk.second_ms < now_second_ms {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        for key in orderbook_keys {
            if let Some(chunk) = self.orderbook_chunks.remove(&key) {
                out.push(chunk.finalize_to_event());
            }
        }

        out.sort_by_key(|event| event.event_ts);
        out
    }

    pub fn flush_all(&mut self) -> Vec<NormalizedMdEvent> {
        let mut out = self
            .trade_chunks
            .drain()
            .map(|(_, chunk)| chunk.finalize_to_event())
            .collect::<Vec<_>>();
        out.extend(
            self.orderbook_chunks
                .drain()
                .map(|(_, chunk)| chunk.finalize_to_event()),
        );
        out.sort_by_key(|event| event.event_ts);
        out
    }

    fn ingest_trade_event(&mut self, event: NormalizedMdEvent) -> Vec<NormalizedMdEvent> {
        let mut out = Vec::new();
        let key = (event.market.clone(), event.symbol.clone());
        let event_second_ms = floor_timestamp_millis(event.event_ts.timestamp_millis(), 1_000);

        if self
            .trade_chunks
            .get(&key)
            .map(|chunk| chunk.second_ms != event_second_ms)
            .unwrap_or(false)
        {
            if let Some(chunk) = self.trade_chunks.remove(&key) {
                out.push(chunk.finalize_to_event());
            }
        }

        if !self.trade_chunks.contains_key(&key) {
            self.trade_chunks
                .insert(key.clone(), TradeSecondChunk::new(&event));
        }

        if let Some(chunk) = self.trade_chunks.get_mut(&key) {
            let vpin_state = self.trade_vpin_states.entry(key.clone()).or_default();
            apply_trade_raw_event(&event, chunk, vpin_state, self.whale_threshold_usdt);
            if chunk.source_event_count as usize >= TRADE_PREBATCH_MAX_EVENTS {
                if let Some(chunk) = self.trade_chunks.remove(&key) {
                    out.push(chunk.finalize_to_event());
                }
            }
        }

        out
    }

    fn ingest_orderbook_event(&mut self, event: NormalizedMdEvent) -> Vec<NormalizedMdEvent> {
        let mut out = Vec::new();
        let key = (event.market.clone(), event.symbol.clone());
        let event_second_ms = floor_timestamp_millis(event.event_ts.timestamp_millis(), 1_000);

        if self
            .orderbook_chunks
            .get(&key)
            .map(|chunk| chunk.second_ms != event_second_ms)
            .unwrap_or(false)
        {
            if let Some(chunk) = self.orderbook_chunks.remove(&key) {
                out.push(chunk.finalize_to_event());
            }
        }

        let preagg_state = self.orderbook_states.entry(key.clone()).or_default();

        if !self.orderbook_chunks.contains_key(&key) {
            let mut seeded_chunk = OrderbookSecondChunk::new(&event);
            for (tick, qty) in &preagg_state.bids {
                seeded_chunk.update_bid_qty(*tick, *qty);
            }
            for (tick, qty) in &preagg_state.asks {
                seeded_chunk.update_ask_qty(*tick, *qty);
            }
            self.orderbook_chunks.insert(key.clone(), seeded_chunk);
        }

        if let Some(chunk) = self.orderbook_chunks.get_mut(&key) {
            chunk.touch(&event);
            apply_orderbook_raw_event(&event, preagg_state, chunk);
            if chunk.source_event_count as usize >= ORDERBOOK_PREBATCH_MAX_EVENTS {
                if let Some(chunk) = self.orderbook_chunks.remove(&key) {
                    out.push(chunk.finalize_to_event());
                }
            }
        }

        out
    }
}

fn apply_trade_raw_event(
    event: &NormalizedMdEvent,
    chunk: &mut TradeSecondChunk,
    preagg_state: &mut TradeVpinState,
    whale_threshold_usdt: f64,
) {
    if event.msg_type != "md.trade" {
        return;
    }
    let Some(price) = value_f64(&event.data, "price") else {
        return;
    };
    let qty_eth = value_f64(&event.data, "qty_eth")
        .or_else(|| value_f64(&event.data, "qty_raw"))
        .unwrap_or(0.0);
    let notional_usdt = value_f64(&event.data, "notional_usdt").unwrap_or(price * qty_eth);
    let aggressor_side = value_i64(&event.data, "aggressor_side")
        .map(|v| if v >= 0 { 1 } else { -1 })
        .or_else(|| {
            event
                .data
                .get("is_buyer_maker")
                .and_then(Value::as_bool)
                .map(|v| if v { -1 } else { 1 })
        })
        .unwrap_or(1);

    chunk.apply_trade(
        event,
        price,
        qty_eth,
        notional_usdt,
        aggressor_side,
        whale_threshold_usdt,
    );
    if aggressor_side >= 0 {
        preagg_state.ingest_flow(qty_eth, 0.0);
    } else {
        preagg_state.ingest_flow(0.0, qty_eth);
    }
    chunk.vpin_snapshot = Some(preagg_state.snapshot());
}

fn apply_orderbook_raw_event(
    event: &NormalizedMdEvent,
    preagg_state: &mut OrderbookPreAggState,
    chunk: &mut OrderbookSecondChunk,
) {
    match event.msg_type.as_str() {
        "md.depth" => {
            let bids_delta = parse_level_pairs(event.data.get("bids_delta"));
            let asks_delta = parse_level_pairs(event.data.get("asks_delta"));
            preagg_state.apply_depth_delta(&bids_delta, &asks_delta);
            for (price, qty) in &bids_delta {
                chunk.update_bid_qty(ob_price_to_tick(*price), *qty);
            }
            for (price, qty) in &asks_delta {
                chunk.update_ask_qty(ob_price_to_tick(*price), *qty);
            }
            let sample = preagg_state.sample_scalar_from_depth();
            chunk.apply_scalar_sample(&sample, 1);
        }
        "md.orderbook_snapshot_l2" => {
            let bids = parse_level_pairs(event.data.get("bids"));
            let asks = parse_level_pairs(event.data.get("asks"));
            apply_snapshot_with_heatmap_diffs(preagg_state, chunk, &bids, &asks);
            let sample = preagg_state.sample_scalar_from_depth();
            chunk.apply_scalar_sample(&sample, 1);
        }
        "md.bbo" => {
            let Some(bid_price) = value_f64(&event.data, "bid_price") else {
                return;
            };
            let Some(bid_qty) = value_f64(&event.data, "bid_qty") else {
                return;
            };
            let Some(ask_price) = value_f64(&event.data, "ask_price") else {
                return;
            };
            let Some(ask_qty) = value_f64(&event.data, "ask_qty") else {
                return;
            };
            let ofi = preagg_state.apply_bbo(bid_price, bid_qty, ask_price, ask_qty);
            chunk.update_bid_qty(ob_price_to_tick(bid_price), bid_qty);
            chunk.update_ask_qty(ob_price_to_tick(ask_price), ask_qty);
            let mut sample = preagg_state.sample_scalar_from_depth();
            sample.ofi = ofi;
            sample.bbo_updates = 1;
            chunk.apply_scalar_sample(&sample, 1);
        }
        _ => {}
    }
}

fn apply_snapshot_with_heatmap_diffs(
    preagg_state: &mut OrderbookPreAggState,
    chunk: &mut OrderbookSecondChunk,
    bids: &[(f64, f64)],
    asks: &[(f64, f64)],
) {
    let old_bids = preagg_state.bids.clone();
    let old_asks = preagg_state.asks.clone();
    preagg_state.apply_snapshot(bids, asks);

    for (tick, old_qty) in &old_bids {
        let new_qty = preagg_state.bids.get(tick).copied().unwrap_or(0.0);
        if (new_qty - *old_qty).abs() > 1e-12 {
            chunk.update_bid_qty(*tick, new_qty);
        }
    }
    for (tick, new_qty) in &preagg_state.bids {
        if !old_bids.contains_key(tick) {
            chunk.update_bid_qty(*tick, *new_qty);
        }
    }

    for (tick, old_qty) in &old_asks {
        let new_qty = preagg_state.asks.get(tick).copied().unwrap_or(0.0);
        if (new_qty - *old_qty).abs() > 1e-12 {
            chunk.update_ask_qty(*tick, new_qty);
        }
    }
    for (tick, new_qty) in &preagg_state.asks {
        if !old_asks.contains_key(tick) {
            chunk.update_ask_qty(*tick, *new_qty);
        }
    }
}

fn parse_level_pairs(value: Option<&Value>) -> Vec<(f64, f64)> {
    let Some(Value::Array(levels)) = value else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(levels.len());
    for level in levels {
        let Value::Array(level_arr) = level else {
            continue;
        };
        if level_arr.len() < 2 {
            continue;
        }
        let Some(price) = parse_json_f64(&level_arr[0]) else {
            continue;
        };
        let Some(qty) = parse_json_f64(&level_arr[1]) else {
            continue;
        };
        out.push((price, qty.max(0.0)));
    }
    out
}

fn parse_json_f64(value: &Value) -> Option<f64> {
    match value {
        Value::String(s) => s.parse::<f64>().ok(),
        Value::Number(n) => n.as_f64(),
        _ => None,
    }
}

fn value_i64(data: &Value, key: &str) -> Option<i64> {
    data.get(key).and_then(Value::as_i64)
}

fn value_f64(data: &Value, key: &str) -> Option<f64> {
    match data.get(key) {
        Some(Value::String(s)) => s.parse::<f64>().ok(),
        Some(Value::Number(n)) => n.as_f64(),
        _ => None,
    }
}

fn floor_timestamp_millis(ts_ms: i64, window_ms: i64) -> i64 {
    ts_ms - ts_ms.rem_euclid(window_ms)
}

fn millis_to_utc(ts_ms: i64) -> chrono::DateTime<Utc> {
    chrono::DateTime::<Utc>::from_timestamp_millis(ts_ms).unwrap_or_else(Utc::now)
}

fn ob_price_to_tick(price: f64) -> i64 {
    (price * OB_PREAGG_PRICE_SCALE).round() as i64
}

fn ob_tick_to_price(tick: i64) -> f64 {
    tick as f64 / OB_PREAGG_PRICE_SCALE
}

fn trade_price_to_tick(price: f64) -> i64 {
    (price * TRADE_PREAGG_PRICE_SCALE).round() as i64
}

fn trade_tick_to_price(tick: i64) -> f64 {
    tick as f64 / TRADE_PREAGG_PRICE_SCALE
}

fn ob_weighted_topk_depth(
    bids: &BTreeMap<i64, f64>,
    asks: &BTreeMap<i64, f64>,
    topk: usize,
) -> (f64, f64) {
    let weighted_bid = bids
        .iter()
        .rev()
        .take(topk)
        .enumerate()
        .map(|(idx, (_, qty))| (-(OB_PREAGG_DW_LAMBDA) * idx as f64).exp() * *qty)
        .sum::<f64>();
    let weighted_ask = asks
        .iter()
        .take(topk)
        .enumerate()
        .map(|(idx, (_, qty))| (-(OB_PREAGG_DW_LAMBDA) * idx as f64).exp() * *qty)
        .sum::<f64>();
    (weighted_bid, weighted_ask)
}

#[cfg(test)]
mod tests {
    use super::WsOneSecondPreAggregator;
    use crate::normalize::NormalizedMdEvent;
    use chrono::{TimeZone, Utc};
    use serde_json::json;

    #[test]
    fn trade_1s_preagg_emits_vpin_snapshot_on_live_path() {
        let mut agg = WsOneSecondPreAggregator::default();
        let event = NormalizedMdEvent {
            msg_type: "md.trade".to_string(),
            market: "futures".to_string(),
            symbol: "ETHUSDT".to_string(),
            source_kind: "ws".to_string(),
            backfill_in_progress: false,
            routing_key: "md.futures.trade.ethusdt".to_string(),
            stream_name: "trade".to_string(),
            event_ts: Utc
                .timestamp_millis_opt(1_700_000_000_250)
                .single()
                .unwrap(),
            data: json!({
                "price": 2500.0,
                "qty_eth": 12.5,
                "notional_usdt": 31_250.0,
                "aggressor_side": 1
            }),
        };

        let mut outputs = agg.ingest_raw_event(event);
        outputs.extend(agg.flush_all());

        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].msg_type, "md.agg.trade.1s");
        assert!(outputs[0].data["payload_json"]["vpin_state"].is_object());
        assert_eq!(
            outputs[0].data["payload_json"]["vpin_state"]["current_fill"].as_f64(),
            Some(12.5)
        );
    }

    #[test]
    fn orderbook_1s_preagg_uses_topk_obi_for_obi_sum() {
        let mut agg = WsOneSecondPreAggregator::default();
        let base_ts = Utc
            .timestamp_millis_opt(1_700_000_000_100)
            .single()
            .unwrap();

        let snapshot = NormalizedMdEvent {
            msg_type: "md.orderbook_snapshot_l2".to_string(),
            market: "futures".to_string(),
            symbol: "ETHUSDT".to_string(),
            source_kind: "ws".to_string(),
            backfill_in_progress: false,
            routing_key: "md.futures.orderbook_snapshot_l2.ethusdt".to_string(),
            stream_name: "orderbook_snapshot_l2".to_string(),
            event_ts: base_ts,
            data: json!({
                "bids": [
                    [2499.0, 9.0],
                    [2498.0, 8.0],
                    [2497.0, 7.0],
                    [2496.0, 6.0],
                    [2495.0, 5.0]
                ],
                "asks": [
                    [2500.0, 1.0],
                    [2501.0, 1.0],
                    [2502.0, 1.0],
                    [2503.0, 1.0],
                    [2504.0, 1.0]
                ]
            }),
        };

        let mut outputs = agg.ingest_raw_event(snapshot);
        outputs.extend(agg.flush_all());

        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].msg_type, "md.agg.orderbook.1s");

        let obi_sum = outputs[0].data["obi_sum"].as_f64().unwrap();
        let obi_l1_sum = outputs[0].data["obi_l1_sum"].as_f64().unwrap();
        let obi_k_sum = outputs[0].data["obi_k_sum"].as_f64().unwrap();

        assert!((obi_sum - obi_k_sum).abs() < 1e-12);
        assert!((obi_sum - obi_l1_sum).abs() > 1e-6);
    }

    #[test]
    fn orderbook_1s_preagg_does_not_inject_virtual_seed_sample() {
        let mut agg = WsOneSecondPreAggregator::default();
        let first_ts = Utc
            .timestamp_millis_opt(1_700_000_000_100)
            .single()
            .unwrap();
        let second_ts = Utc
            .timestamp_millis_opt(1_700_000_001_100)
            .single()
            .unwrap();

        let snapshot = NormalizedMdEvent {
            msg_type: "md.orderbook_snapshot_l2".to_string(),
            market: "futures".to_string(),
            symbol: "ETHUSDT".to_string(),
            source_kind: "ws".to_string(),
            backfill_in_progress: false,
            routing_key: "md.futures.orderbook_snapshot_l2.ethusdt".to_string(),
            stream_name: "orderbook_snapshot_l2".to_string(),
            event_ts: first_ts,
            data: json!({
                "bids": [[2499.0, 5.0]],
                "asks": [[2500.0, 5.0]]
            }),
        };

        let depth = NormalizedMdEvent {
            msg_type: "md.depth".to_string(),
            market: "futures".to_string(),
            symbol: "ETHUSDT".to_string(),
            source_kind: "ws".to_string(),
            backfill_in_progress: false,
            routing_key: "md.futures.depth.ethusdt".to_string(),
            stream_name: "depth".to_string(),
            event_ts: second_ts,
            data: json!({
                "bids_delta": [[2499.0, 7.0]],
                "asks_delta": []
            }),
        };

        let mut outputs = agg.ingest_raw_event(snapshot);
        outputs.extend(agg.ingest_raw_event(depth));
        outputs.extend(agg.flush_all());
        let second_bucket = Utc
            .timestamp_millis_opt(1_700_000_001_000)
            .single()
            .unwrap();

        let second_chunk = outputs
            .iter()
            .find(|event| {
                event.msg_type == "md.agg.orderbook.1s"
                    && event.data["ts_bucket"].as_str() == Some(&second_bucket.to_rfc3339())
            })
            .expect("missing second orderbook chunk");

        assert_eq!(second_chunk.data["sample_count"].as_i64(), Some(1));
    }
}
