use crate::normalize::NormalizedMdEvent;
use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};

const PRICE_SCALE: f64 = 100.0;
const ORDERBOOK_TOPK: usize = 5;
const ORDERBOOK_DW_LAMBDA: f64 = 0.35;
const MICROPRICE_KAPPA: f64 = 0.35;
const MICROPRICE_LAMBDA_OBI: f64 = 0.15;
const SIZE_FLOOR: f64 = 1e-3;
const TICK_SIZE: f64 = 1.0 / PRICE_SCALE;
const EMITTED_CHUNK_RETENTION_MINUTES: i64 = 5;
const VPIN_BUCKET_SIZE_ETH: f64 = 50.0;
const VPIN_ROLLING_BUCKETS: usize = 50;
const VPIN_EPS: f64 = 1e-12;

pub const DEFAULT_WHALE_THRESHOLD_USDT: f64 = 300_000.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregateLane {
    Trade,
    NonTrade,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct MinuteKey {
    market: String,
    symbol: String,
    minute_ms: i64,
}

impl MinuteKey {
    fn new(event: &NormalizedMdEvent) -> Self {
        let minute_ms = floor_minute_millis(event.event_ts.timestamp_millis());
        Self {
            market: event.market.clone(),
            symbol: event.symbol.clone(),
            minute_ms,
        }
    }

    fn ts_bucket(&self) -> DateTime<Utc> {
        millis_to_utc(self.minute_ms)
    }

    fn canonical_chunk_start_ts(&self) -> DateTime<Utc> {
        self.ts_bucket()
    }

    fn canonical_chunk_end_ts(&self) -> DateTime<Utc> {
        self.ts_bucket() + Duration::minutes(1) - Duration::milliseconds(1)
    }

    fn symbol_lower(&self) -> String {
        self.symbol.to_lowercase()
    }
}

#[derive(Debug, Clone, Default)]
struct LevelAgg {
    buy_qty: f64,
    sell_qty: f64,
}

#[derive(Debug, Clone, Default)]
struct LiqAgg {
    long_liq: f64,
    short_liq: f64,
}

#[derive(Debug, Clone, Default)]
struct BookLevelAgg {
    bid_liquidity: f64,
    ask_liquidity: f64,
}

#[derive(Debug, Clone, Default)]
struct WhaleStats {
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

#[derive(Debug, Clone, Default)]
struct VpinStateSnapshot {
    current_buy: f64,
    current_sell: f64,
    current_fill: f64,
    imbalances: Vec<f64>,
    imbalance_sum: f64,
    last_vpin: f64,
}

#[derive(Debug, Clone)]
struct VpinState {
    bucket_size: f64,
    rolling_bucket_count: usize,
    current_buy: f64,
    current_sell: f64,
    current_fill: f64,
    imbalances: Vec<f64>,
    imbalance_sum: f64,
    last_vpin: f64,
}

impl Default for VpinState {
    fn default() -> Self {
        Self::new(VPIN_BUCKET_SIZE_ETH, VPIN_ROLLING_BUCKETS)
    }
}

impl VpinState {
    fn new(bucket_size: f64, rolling_bucket_count: usize) -> Self {
        Self {
            bucket_size,
            rolling_bucket_count,
            current_buy: 0.0,
            current_sell: 0.0,
            current_fill: 0.0,
            imbalances: Vec::new(),
            imbalance_sum: 0.0,
            last_vpin: 0.0,
        }
    }

    fn ingest_flow(&mut self, buy_qty: f64, sell_qty: f64) {
        let mut remain_buy = buy_qty.max(0.0);
        let mut remain_sell = sell_qty.max(0.0);

        while remain_buy + remain_sell > VPIN_EPS {
            if self.current_fill + VPIN_EPS >= self.bucket_size {
                self.close_bucket();
            }

            let remain_total = remain_buy + remain_sell;
            let capacity = (self.bucket_size - self.current_fill).max(0.0);
            if capacity <= VPIN_EPS {
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

            if self.current_fill + VPIN_EPS >= self.bucket_size {
                self.close_bucket();
            }
        }
    }

    fn snapshot(&self) -> VpinStateSnapshot {
        VpinStateSnapshot {
            current_buy: self.current_buy,
            current_sell: self.current_sell,
            current_fill: self.current_fill,
            imbalances: self.imbalances.clone(),
            imbalance_sum: self.imbalance_sum,
            last_vpin: self.last_vpin,
        }
    }

    fn restore(&mut self, snapshot: &VpinStateSnapshot) {
        self.current_buy = snapshot.current_buy;
        self.current_sell = snapshot.current_sell;
        self.current_fill = snapshot.current_fill;
        self.imbalances = snapshot.imbalances.clone();
        self.imbalance_sum = snapshot.imbalance_sum;
        self.last_vpin = snapshot.last_vpin;
    }

    fn close_bucket(&mut self) {
        if self.current_fill <= VPIN_EPS {
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
        if self.imbalances.len() >= self.rolling_bucket_count && self.bucket_size > VPIN_EPS {
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

#[derive(Debug, Clone)]
struct TradeChunk {
    chunk_start_ts: DateTime<Utc>,
    chunk_end_ts: DateTime<Utc>,
    backfill_in_progress: bool,
    dirty: bool,
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
    profile: BTreeMap<i64, LevelAgg>,
    whale: WhaleStats,
    vpin_snapshot: Option<VpinStateSnapshot>,
}

impl TradeChunk {
    fn new(event: &NormalizedMdEvent) -> Self {
        Self {
            chunk_start_ts: event.event_ts,
            chunk_end_ts: event.event_ts,
            backfill_in_progress: event.backfill_in_progress,
            dirty: true,
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
            whale: WhaleStats::default(),
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
        self.dirty = true;
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

        let tick = price_to_tick(price);
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

    fn apply_kline_1m(
        &mut self,
        event: &NormalizedMdEvent,
        open_price: f64,
        high_price: f64,
        low_price: f64,
        close_price: f64,
    ) {
        self.touch(event);
        if self.first_price.is_none() {
            self.first_price = Some(open_price);
        }
        self.last_price = Some(close_price);
        self.high_price = Some(self.high_price.map_or(high_price, |v| v.max(high_price)));
        self.low_price = Some(self.low_price.map_or(low_price, |v| v.min(low_price)));
    }
}

#[derive(Debug, Clone)]
struct OrderbookChunk {
    chunk_start_ts: DateTime<Utc>,
    chunk_end_ts: DateTime<Utc>,
    backfill_in_progress: bool,
    dirty: bool,
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
    heatmap: BTreeMap<i64, BookLevelAgg>,
}

impl OrderbookChunk {
    fn new(event: &NormalizedMdEvent) -> Self {
        Self {
            chunk_start_ts: event.event_ts,
            chunk_end_ts: event.event_ts,
            backfill_in_progress: event.backfill_in_progress,
            dirty: true,
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
            heatmap: BTreeMap::new(),
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
        self.dirty = true;
        self.source_event_count = self.source_event_count.saturating_add(1);
    }

    fn apply_sample(&mut self, event: &NormalizedMdEvent, sample: OrderbookSample, weight: i64) {
        let samples = weight.max(1);
        let weight_f = samples as f64;
        self.touch(event);
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

        for level in sample.heatmap_levels {
            let agg = self.heatmap.entry(level.tick).or_default();
            agg.bid_liquidity += level.bid_qty * weight_f;
            agg.ask_liquidity += level.ask_qty * weight_f;
        }
    }
}

#[derive(Debug, Clone)]
struct LiqChunk {
    chunk_start_ts: DateTime<Utc>,
    chunk_end_ts: DateTime<Utc>,
    backfill_in_progress: bool,
    dirty: bool,
    source_event_count: i64,
    force_liq: BTreeMap<i64, LiqAgg>,
}

impl LiqChunk {
    fn new(event: &NormalizedMdEvent) -> Self {
        Self {
            chunk_start_ts: event.event_ts,
            chunk_end_ts: event.event_ts,
            backfill_in_progress: event.backfill_in_progress,
            dirty: true,
            source_event_count: 0,
            force_liq: BTreeMap::new(),
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
        self.dirty = true;
        self.source_event_count = self.source_event_count.saturating_add(1);
    }
}

#[derive(Debug, Clone)]
struct MarkPoint {
    ts: DateTime<Utc>,
    mark_price: Option<f64>,
    index_price: Option<f64>,
    estimated_settle_price: Option<f64>,
    funding_rate: Option<f64>,
    next_funding_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
struct FundingPoint {
    ts: DateTime<Utc>,
    funding_time: Option<DateTime<Utc>>,
    funding_rate: f64,
    mark_price: Option<f64>,
    next_funding_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
struct FundingMarkChunk {
    chunk_start_ts: DateTime<Utc>,
    chunk_end_ts: DateTime<Utc>,
    backfill_in_progress: bool,
    dirty: bool,
    source_event_count: i64,
    mark_points: Vec<MarkPoint>,
    funding_points: Vec<FundingPoint>,
}

impl FundingMarkChunk {
    fn new(event: &NormalizedMdEvent) -> Self {
        Self {
            chunk_start_ts: event.event_ts,
            chunk_end_ts: event.event_ts,
            backfill_in_progress: event.backfill_in_progress,
            dirty: true,
            source_event_count: 0,
            mark_points: Vec::new(),
            funding_points: Vec::new(),
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
        self.dirty = true;
        self.source_event_count = self.source_event_count.saturating_add(1);
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct BboState {
    bid_price: f64,
    bid_qty: f64,
    ask_price: f64,
    ask_qty: f64,
}

#[derive(Debug, Clone)]
struct OrderbookState {
    bids: BTreeMap<i64, f64>,
    asks: BTreeMap<i64, f64>,
    last_bbo: Option<BboState>,
    last_obi_k_dw: Option<f64>,
}

impl Default for OrderbookState {
    fn default() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_bbo: None,
            last_obi_k_dw: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct HeatmapLevelSample {
    tick: i64,
    bid_qty: f64,
    ask_qty: f64,
}

#[derive(Debug, Clone, Default)]
struct OrderbookSample {
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
    heatmap_levels: Vec<HeatmapLevelSample>,
}

impl OrderbookState {
    fn apply_snapshot(&mut self, bids: &[(f64, f64)], asks: &[(f64, f64)]) {
        self.bids.clear();
        self.asks.clear();
        for (price, qty) in bids {
            self.bids.insert(price_to_tick(*price), *qty);
        }
        for (price, qty) in asks {
            self.asks.insert(price_to_tick(*price), *qty);
        }
    }

    fn apply_depth_delta(&mut self, bids_delta: &[(f64, f64)], asks_delta: &[(f64, f64)]) {
        for (price, qty) in bids_delta {
            let tick = price_to_tick(*price);
            if *qty <= 0.0 {
                self.bids.remove(&tick);
            } else {
                self.bids.insert(tick, *qty);
            }
        }
        for (price, qty) in asks_delta {
            let tick = price_to_tick(*price);
            if *qty <= 0.0 {
                self.asks.remove(&tick);
            } else {
                self.asks.insert(tick, *qty);
            }
        }
    }

    fn apply_bbo(
        &mut self,
        bid_price: f64,
        bid_qty: f64,
        ask_price: f64,
        ask_qty: f64,
    ) -> OrderbookSample {
        let current = BboState {
            bid_price,
            bid_qty,
            ask_price,
            ask_qty,
        };

        self.bids.insert(price_to_tick(bid_price), bid_qty);
        self.asks.insert(price_to_tick(ask_price), ask_qty);

        let ofi = if let Some(prev) = self.last_bbo {
            (current.bid_qty - prev.bid_qty) - (current.ask_qty - prev.ask_qty)
        } else {
            0.0
        };
        self.last_bbo = Some(current);

        let mut sample = self.sample_from_depth();
        sample.ofi = ofi;
        sample.bbo_updates = 1;
        sample
    }

    fn sample_from_depth(&mut self) -> OrderbookSample {
        let best_bid = self
            .bids
            .iter()
            .next_back()
            .map(|(p, q)| (tick_to_price(*p), *q));
        let best_ask = self
            .asks
            .iter()
            .next()
            .map(|(p, q)| (tick_to_price(*p), *q));

        let (spread, microprice_classic, microprice_kappa, microprice_adj, obi_l1) =
            match (best_bid, best_ask) {
                (Some((bp, bq)), Some((ap, aq))) => {
                    let spread = (ap - bp).max(0.0);
                    let denom = (bq + aq).max(1e-12);
                    let mid = (ap + bp) / 2.0;
                    let micro_classic = (ap * bq + bp * aq) / denom;
                    let bid_floor = bq.max(SIZE_FLOOR);
                    let ask_floor = aq.max(SIZE_FLOOR);
                    let bid_pow = (bid_floor + 1e-12).powf(MICROPRICE_KAPPA);
                    let ask_pow = (ask_floor + 1e-12).powf(MICROPRICE_KAPPA);
                    let micro_kappa = (bid_pow * ap + ask_pow * bp) / (bid_pow + ask_pow + 1e-12);
                    let obi_l1 = (bq - aq) / denom;
                    let (lo, hi) = if bp <= ap { (bp, ap) } else { (ap, bp) };
                    let micro_adj =
                        (mid + MICROPRICE_LAMBDA_OBI * obi_l1 * (TICK_SIZE / 2.0)).clamp(lo, hi);
                    (spread, micro_classic, micro_kappa, micro_adj, obi_l1)
                }
                _ => (0.0, 0.0, 0.0, 0.0, 0.0),
            };

        let sum_bid: f64 = self
            .bids
            .iter()
            .rev()
            .take(ORDERBOOK_TOPK)
            .map(|(_, q)| *q)
            .sum();
        let sum_ask: f64 = self.asks.iter().take(ORDERBOOK_TOPK).map(|(_, q)| *q).sum();
        let topk_depth = sum_bid + sum_ask;
        let obi_k = if topk_depth > 0.0 {
            (sum_bid - sum_ask) / topk_depth
        } else {
            0.0
        };
        let (dw_bid, dw_ask) = weighted_topk_depth(&self.bids, &self.asks, ORDERBOOK_TOPK);
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
            (TICK_SIZE / spread).clamp(0.0, 1.0)
        };
        let obi_k_dw_adj = obi_k_dw * depth_factor * spread_factor;
        let heatmap_levels = collect_heatmap_levels(&self.bids, &self.asks);

        OrderbookSample {
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
            heatmap_levels,
        }
    }
}

pub struct MinuteBatchAggregator {
    lane: AggregateLane,
    whale_threshold_usdt: f64,
    orderbooks: HashMap<(String, String), OrderbookState>,
    trade_vpin_states: HashMap<(String, String), VpinState>,
    trade_chunks: HashMap<MinuteKey, TradeChunk>,
    orderbook_chunks: HashMap<MinuteKey, OrderbookChunk>,
    liq_chunks: HashMap<MinuteKey, LiqChunk>,
    funding_mark_chunks: HashMap<MinuteKey, FundingMarkChunk>,
}

impl MinuteBatchAggregator {
    pub fn new(lane: AggregateLane, whale_threshold_usdt: f64) -> Self {
        Self {
            lane,
            whale_threshold_usdt,
            orderbooks: HashMap::new(),
            trade_vpin_states: HashMap::new(),
            trade_chunks: HashMap::new(),
            orderbook_chunks: HashMap::new(),
            liq_chunks: HashMap::new(),
            funding_mark_chunks: HashMap::new(),
        }
    }

    pub fn aggregate_batch(&mut self, events: &[NormalizedMdEvent]) -> Vec<NormalizedMdEvent> {
        self.aggregate_batch_refs(events.iter())
    }

    pub fn aggregate_batch_refs<'a, I>(&mut self, events: I) -> Vec<NormalizedMdEvent>
    where
        I: IntoIterator<Item = &'a NormalizedMdEvent>,
    {
        let mut max_minute_ms: Option<i64> = None;

        for event in events {
            let key = MinuteKey::new(event);
            max_minute_ms = Some(max_minute_ms.map_or(key.minute_ms, |cur| cur.max(key.minute_ms)));
            match event.msg_type.as_str() {
                "md.trade" if self.lane == AggregateLane::Trade => {
                    self.aggregate_trade(event, &key);
                }
                "md.agg.trade.1s" if self.lane == AggregateLane::Trade => {
                    self.aggregate_trade_1s_preagg(event, &key);
                }
                "md.depth" if self.lane == AggregateLane::NonTrade => {
                    self.aggregate_depth(event, &key);
                }
                "md.orderbook_snapshot_l2" if self.lane == AggregateLane::NonTrade => {
                    self.aggregate_orderbook_snapshot(event, &key);
                }
                "md.bbo" if self.lane == AggregateLane::NonTrade => {
                    self.aggregate_bbo(event, &key);
                }
                "md.agg.orderbook.1s" if self.lane == AggregateLane::NonTrade => {
                    self.aggregate_orderbook_1s_preagg(event, &key);
                }
                "md.force_order" if self.lane == AggregateLane::NonTrade => {
                    self.aggregate_force_order(event, &key);
                }
                "md.mark_price" if self.lane == AggregateLane::NonTrade => {
                    self.aggregate_mark_price(event, &key);
                }
                "md.funding_rate" if self.lane == AggregateLane::NonTrade => {
                    self.aggregate_funding_rate(event, &key);
                }
                "md.kline" if self.lane == AggregateLane::Trade => {
                    self.aggregate_kline_1m(event, &key);
                }
                _ => {}
            }
        }

        let Some(max_minute_ms) = max_minute_ms else {
            return Vec::new();
        };

        self.flush_ready_before_minute_ms(max_minute_ms)
    }

    pub fn flush_closed_minutes(&mut self, watermark_ts: DateTime<Utc>) -> Vec<NormalizedMdEvent> {
        self.flush_ready_before_minute_ms(floor_minute_millis(watermark_ts.timestamp_millis()))
    }

    fn flush_ready_before_minute_ms(
        &mut self,
        ready_before_minute_ms: i64,
    ) -> Vec<NormalizedMdEvent> {
        let mut out = Vec::with_capacity(
            self.trade_chunks
                .len()
                .saturating_add(self.orderbook_chunks.len())
                .saturating_add(self.liq_chunks.len())
                .saturating_add(self.funding_mark_chunks.len()),
        );

        collect_ready_trade_events(&mut self.trade_chunks, ready_before_minute_ms, &mut out);
        collect_ready_orderbook_events(
            &mut self.orderbook_chunks,
            ready_before_minute_ms,
            &mut out,
        );
        collect_ready_liq_events(&mut self.liq_chunks, ready_before_minute_ms, &mut out);
        collect_ready_funding_mark_events(
            &mut self.funding_mark_chunks,
            ready_before_minute_ms,
            &mut out,
        );
        self.trim_emitted_chunks(ready_before_minute_ms);
        sort_output_events(&mut out);
        out
    }

    fn trim_emitted_chunks(&mut self, reference_minute_ms: i64) {
        let retention_cutoff_ms =
            reference_minute_ms - EMITTED_CHUNK_RETENTION_MINUTES.saturating_mul(60_000);
        self.trade_chunks
            .retain(|key, chunk| key.minute_ms >= retention_cutoff_ms || chunk.dirty);
        self.orderbook_chunks
            .retain(|key, chunk| key.minute_ms >= retention_cutoff_ms || chunk.dirty);
        self.liq_chunks
            .retain(|key, chunk| key.minute_ms >= retention_cutoff_ms || chunk.dirty);
        self.funding_mark_chunks
            .retain(|key, chunk| key.minute_ms >= retention_cutoff_ms || chunk.dirty);
    }

    fn aggregate_trade(&mut self, event: &NormalizedMdEvent, key: &MinuteKey) {
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

        let chunk = self
            .trade_chunks
            .entry(key.clone())
            .or_insert_with(|| TradeChunk::new(event));
        chunk.apply_trade(
            event,
            price,
            qty_eth,
            notional_usdt,
            aggressor_side,
            self.whale_threshold_usdt,
        );
        let state_key = (key.market.clone(), key.symbol.clone());
        let vpin_state = self
            .trade_vpin_states
            .entry(state_key)
            .or_insert_with(VpinState::default);
        if aggressor_side >= 0 {
            vpin_state.ingest_flow(qty_eth, 0.0);
        } else {
            vpin_state.ingest_flow(0.0, qty_eth);
        }
        chunk.vpin_snapshot = Some(vpin_state.snapshot());
    }

    fn aggregate_kline_1m(&mut self, event: &NormalizedMdEvent, key: &MinuteKey) {
        let interval_code = event
            .data
            .get("interval_code")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if interval_code != "1m" {
            return;
        }
        let Some(open_price) = value_f64(&event.data, "open_price") else {
            return;
        };
        let Some(high_price) = value_f64(&event.data, "high_price") else {
            return;
        };
        let Some(low_price) = value_f64(&event.data, "low_price") else {
            return;
        };
        let Some(close_price) = value_f64(&event.data, "close_price") else {
            return;
        };

        let bucket_key = parse_optional_ts(&event.data, "open_time")
            .map(|open_ts| MinuteKey {
                market: key.market.clone(),
                symbol: key.symbol.clone(),
                minute_ms: floor_minute_millis(open_ts.timestamp_millis()),
            })
            .unwrap_or_else(|| key.clone());

        let chunk = self
            .trade_chunks
            .entry(bucket_key)
            .or_insert_with(|| TradeChunk::new(event));
        chunk.apply_kline_1m(event, open_price, high_price, low_price, close_price);
    }

    fn aggregate_trade_1s_preagg(&mut self, event: &NormalizedMdEvent, key: &MinuteKey) {
        let chunk = self
            .trade_chunks
            .entry(key.clone())
            .or_insert_with(|| TradeChunk::new(event));

        let source_event_count = value_i64(&event.data, "source_event_count")
            .unwrap_or(1)
            .max(0);
        let chunk_start_ts =
            parse_optional_ts(&event.data, "chunk_start_ts").unwrap_or(event.event_ts);
        let chunk_end_ts = parse_optional_ts(&event.data, "chunk_end_ts").unwrap_or(event.event_ts);
        let earlier_chunk = chunk_start_ts < chunk.chunk_start_ts;
        let later_chunk = chunk_end_ts >= chunk.chunk_end_ts;
        if earlier_chunk {
            chunk.chunk_start_ts = chunk_start_ts;
        }
        if later_chunk {
            chunk.chunk_end_ts = chunk_end_ts;
        }
        chunk.backfill_in_progress |= event.backfill_in_progress;
        chunk.dirty = true;
        chunk.source_event_count = chunk.source_event_count.saturating_add(source_event_count);

        let buy_qty = value_f64(&event.data, "buy_qty").unwrap_or(0.0);
        let sell_qty = value_f64(&event.data, "sell_qty").unwrap_or(0.0);
        chunk.trade_count = chunk
            .trade_count
            .saturating_add(value_i64(&event.data, "trade_count").unwrap_or(0).max(0));
        chunk.buy_qty += buy_qty;
        chunk.sell_qty += sell_qty;
        chunk.buy_notional += value_f64(&event.data, "buy_notional").unwrap_or(0.0);
        chunk.sell_notional += value_f64(&event.data, "sell_notional").unwrap_or(0.0);

        if let Some(first_price) = value_f64(&event.data, "first_price") {
            if chunk.first_price.is_none() || earlier_chunk {
                chunk.first_price = Some(first_price);
            }
        }
        if let Some(last_price) = value_f64(&event.data, "last_price") {
            if chunk.last_price.is_none() || later_chunk {
                chunk.last_price = Some(last_price);
            }
        }
        if let Some(high_price) = value_f64(&event.data, "high_price") {
            chunk.high_price = Some(chunk.high_price.map_or(high_price, |v| v.max(high_price)));
        }
        if let Some(low_price) = value_f64(&event.data, "low_price") {
            chunk.low_price = Some(chunk.low_price.map_or(low_price, |v| v.min(low_price)));
        }

        if let Some(Value::Array(levels)) = event.data.get("profile_levels") {
            for row in levels {
                let Value::Array(arr) = row else {
                    continue;
                };
                if arr.len() < 3 {
                    continue;
                }
                let Some(price) = to_f64(&arr[0]) else {
                    continue;
                };
                let buy_qty = to_f64(&arr[1]).unwrap_or(0.0);
                let sell_qty = to_f64(&arr[2]).unwrap_or(0.0);
                let level = chunk.profile.entry(price_to_tick(price)).or_default();
                level.buy_qty += buy_qty;
                level.sell_qty += sell_qty;
            }
        }

        if let Some(whale) = event.data.get("whale") {
            let qty_eth_total = value_f64(whale, "qty_eth_total").unwrap_or(0.0);
            let qty_eth_buy = value_f64(whale, "qty_eth_buy")
                .or_else(|| {
                    value_f64(whale, "delta_qty_eth")
                        .map(|delta| ((qty_eth_total + delta) / 2.0).max(0.0))
                })
                .unwrap_or(0.0);
            let qty_eth_sell = value_f64(whale, "qty_eth_sell")
                .or_else(|| {
                    value_f64(whale, "delta_qty_eth")
                        .map(|delta| ((qty_eth_total - delta) / 2.0).max(0.0))
                })
                .unwrap_or(0.0);

            chunk.whale.trade_count = chunk
                .whale
                .trade_count
                .saturating_add(value_i64(whale, "trade_count").unwrap_or(0).max(0));
            chunk.whale.buy_count = chunk
                .whale
                .buy_count
                .saturating_add(value_i64(whale, "buy_count").unwrap_or(0).max(0));
            chunk.whale.sell_count = chunk
                .whale
                .sell_count
                .saturating_add(value_i64(whale, "sell_count").unwrap_or(0).max(0));
            chunk.whale.notional_total += value_f64(whale, "notional_total").unwrap_or(0.0);
            chunk.whale.notional_buy += value_f64(whale, "notional_buy").unwrap_or(0.0);
            chunk.whale.notional_sell += value_f64(whale, "notional_sell").unwrap_or(0.0);
            chunk.whale.qty_eth_total += qty_eth_total;
            chunk.whale.qty_eth_buy += qty_eth_buy;
            chunk.whale.qty_eth_sell += qty_eth_sell;
            chunk.whale.max_single_notional = chunk
                .whale
                .max_single_notional
                .max(value_f64(whale, "max_single_notional").unwrap_or(0.0));
        }

        let state_key = (key.market.clone(), key.symbol.clone());
        let vpin_state = self
            .trade_vpin_states
            .entry(state_key)
            .or_insert_with(VpinState::default);
        if let Some(snapshot) = parse_optional_vpin_snapshot(
            event
                .data
                .get("payload_json")
                .and_then(|payload| payload.get("vpin_state")),
        ) {
            vpin_state.restore(&snapshot);
            chunk.vpin_snapshot = Some(snapshot);
        } else {
            vpin_state.ingest_flow(buy_qty, sell_qty);
            chunk.vpin_snapshot = Some(vpin_state.snapshot());
        }
    }

    fn aggregate_depth(&mut self, event: &NormalizedMdEvent, key: &MinuteKey) {
        let bids_delta = parse_level_pairs(event.data.get("bids_delta"));
        let asks_delta = parse_level_pairs(event.data.get("asks_delta"));
        let state_key = (key.market.clone(), key.symbol.clone());
        let state = self.orderbooks.entry(state_key).or_default();
        state.apply_depth_delta(&bids_delta, &asks_delta);
        let sample = state.sample_from_depth();
        let chunk = self
            .orderbook_chunks
            .entry(key.clone())
            .or_insert_with(|| OrderbookChunk::new(event));
        chunk.apply_sample(event, sample, 1);
    }

    fn aggregate_orderbook_snapshot(&mut self, event: &NormalizedMdEvent, key: &MinuteKey) {
        let bids = parse_level_pairs(event.data.get("bids"));
        let asks = parse_level_pairs(event.data.get("asks"));
        let state_key = (key.market.clone(), key.symbol.clone());
        let state = self.orderbooks.entry(state_key).or_default();
        state.apply_snapshot(&bids, &asks);
        let sample = state.sample_from_depth();
        let chunk = self
            .orderbook_chunks
            .entry(key.clone())
            .or_insert_with(|| OrderbookChunk::new(event));
        chunk.apply_sample(event, sample, 1);
    }

    fn aggregate_bbo(&mut self, event: &NormalizedMdEvent, key: &MinuteKey) {
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
        let sample_count = value_i64(&event.data, "sample_count")
            .or_else(|| value_i64(&event.data, "event_count"))
            .unwrap_or(1)
            .max(1);

        let state_key = (key.market.clone(), key.symbol.clone());
        let state = self.orderbooks.entry(state_key).or_default();
        let sample = state.apply_bbo(bid_price, bid_qty, ask_price, ask_qty);
        let chunk = self
            .orderbook_chunks
            .entry(key.clone())
            .or_insert_with(|| OrderbookChunk::new(event));
        chunk.apply_sample(event, sample, sample_count);
    }

    fn aggregate_orderbook_1s_preagg(&mut self, event: &NormalizedMdEvent, key: &MinuteKey) {
        let chunk = self
            .orderbook_chunks
            .entry(key.clone())
            .or_insert_with(|| OrderbookChunk::new(event));

        let source_event_count = value_i64(&event.data, "source_event_count")
            .unwrap_or(1)
            .max(0);
        let sample_count = value_i64(&event.data, "sample_count").unwrap_or(0).max(0);
        let bbo_updates = value_i64(&event.data, "bbo_updates").unwrap_or(0).max(0);

        let chunk_start_ts =
            parse_optional_ts(&event.data, "chunk_start_ts").unwrap_or(event.event_ts);
        let chunk_end_ts = parse_optional_ts(&event.data, "chunk_end_ts").unwrap_or(event.event_ts);
        if chunk_start_ts < chunk.chunk_start_ts {
            chunk.chunk_start_ts = chunk_start_ts;
        }
        if chunk_end_ts >= chunk.chunk_end_ts {
            chunk.chunk_end_ts = chunk_end_ts;
        }
        chunk.backfill_in_progress |= event.backfill_in_progress;
        chunk.dirty = true;
        chunk.source_event_count = chunk.source_event_count.saturating_add(source_event_count);
        chunk.sample_count = chunk.sample_count.saturating_add(sample_count);
        chunk.bbo_updates = chunk.bbo_updates.saturating_add(bbo_updates);

        chunk.spread_sum += value_f64(&event.data, "spread_sum").unwrap_or(0.0);
        chunk.topk_depth_sum += value_f64(&event.data, "topk_depth_sum").unwrap_or(0.0);
        chunk.obi_sum += value_f64(&event.data, "obi_sum").unwrap_or(0.0);
        chunk.obi_l1_sum += value_f64(&event.data, "obi_l1_sum").unwrap_or(0.0);
        chunk.obi_k_sum += value_f64(&event.data, "obi_k_sum").unwrap_or(0.0);
        chunk.obi_k_dw_sum += value_f64(&event.data, "obi_k_dw_sum").unwrap_or(0.0);
        chunk.obi_k_dw_change_sum += value_f64(&event.data, "obi_k_dw_change_sum").unwrap_or(0.0);
        chunk.obi_k_dw_adj_sum += value_f64(&event.data, "obi_k_dw_adj_sum").unwrap_or(0.0);
        chunk.microprice_sum += value_f64(&event.data, "microprice_sum").unwrap_or(0.0);
        chunk.microprice_classic_sum +=
            value_f64(&event.data, "microprice_classic_sum").unwrap_or(0.0);
        chunk.microprice_kappa_sum += value_f64(&event.data, "microprice_kappa_sum").unwrap_or(0.0);
        chunk.microprice_adj_sum += value_f64(&event.data, "microprice_adj_sum").unwrap_or(0.0);
        chunk.ofi_sum += value_f64(&event.data, "ofi_sum").unwrap_or(0.0);
        if let Some(close) = value_f64(&event.data, "obi_k_dw_close") {
            chunk.obi_k_dw_close = Some(close);
        }

        if let Some(Value::Array(levels)) = event.data.get("heatmap_levels") {
            for level in levels {
                let Value::Array(level_arr) = level else {
                    continue;
                };
                if level_arr.len() < 3 {
                    continue;
                }
                let Some(price) = to_f64(&level_arr[0]) else {
                    continue;
                };
                let bid_liq = to_f64(&level_arr[1]).unwrap_or(0.0);
                let ask_liq = to_f64(&level_arr[2]).unwrap_or(0.0);
                let row = chunk.heatmap.entry(price_to_tick(price)).or_default();
                row.bid_liquidity += bid_liq;
                row.ask_liquidity += ask_liq;
            }
        }
    }

    fn aggregate_force_order(&mut self, event: &NormalizedMdEvent, key: &MinuteKey) {
        let Some(price) = value_f64(&event.data, "price") else {
            return;
        };
        let qty = value_f64(&event.data, "notional_usdt")
            .or_else(|| value_f64(&event.data, "filled_qty").map(|filled| (filled * price).abs()));
        let Some(liq_qty) = qty else {
            return;
        };
        let liq_side = event
            .data
            .get("liq_side")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let tick = price_to_tick(price);

        let chunk = self
            .liq_chunks
            .entry(key.clone())
            .or_insert_with(|| LiqChunk::new(event));
        chunk.touch(event);
        let level = chunk.force_liq.entry(tick).or_default();
        if liq_side == "long_liq" {
            level.long_liq += liq_qty;
        } else if liq_side == "short_liq" {
            level.short_liq += liq_qty;
        }
    }

    fn aggregate_mark_price(&mut self, event: &NormalizedMdEvent, key: &MinuteKey) {
        let chunk = self
            .funding_mark_chunks
            .entry(key.clone())
            .or_insert_with(|| FundingMarkChunk::new(event));
        chunk.touch(event);
        chunk.mark_points.push(MarkPoint {
            ts: event.event_ts,
            mark_price: value_f64(&event.data, "mark_price"),
            index_price: value_f64(&event.data, "index_price"),
            estimated_settle_price: value_f64(&event.data, "estimated_settle_price"),
            funding_rate: value_f64(&event.data, "funding_rate"),
            next_funding_time: parse_optional_ts(&event.data, "next_funding_time"),
        });
    }

    fn aggregate_funding_rate(&mut self, event: &NormalizedMdEvent, key: &MinuteKey) {
        let Some(funding_rate) = value_f64(&event.data, "funding_rate") else {
            return;
        };
        let chunk = self
            .funding_mark_chunks
            .entry(key.clone())
            .or_insert_with(|| FundingMarkChunk::new(event));
        chunk.touch(event);
        chunk.funding_points.push(FundingPoint {
            ts: event.event_ts,
            funding_time: parse_optional_ts(&event.data, "funding_time"),
            funding_rate,
            mark_price: value_f64(&event.data, "mark_price"),
            next_funding_time: parse_optional_ts(&event.data, "next_funding_time"),
        });
    }
}

fn build_trade_event(key: MinuteKey, chunk: TradeChunk) -> NormalizedMdEvent {
    let profile_levels = chunk
        .profile
        .iter()
        .map(|(tick, level)| json!([tick_to_price(*tick), level.buy_qty, level.sell_qty]))
        .collect::<Vec<_>>();
    let ts_bucket = key.ts_bucket();
    let mut payload_json = json!({
        "agg_mode": "canonical_1m",
        "source_start_ts": chunk.chunk_start_ts.to_rfc3339(),
        "source_end_ts": chunk.chunk_end_ts.to_rfc3339(),
    });
    if let (Some(snapshot), Some(payload_obj)) =
        (chunk.vpin_snapshot.as_ref(), payload_json.as_object_mut())
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
        "chunk_start_ts": key.canonical_chunk_start_ts().to_rfc3339(),
        "chunk_end_ts": key.canonical_chunk_end_ts().to_rfc3339(),
        "source_event_count": chunk.source_event_count,
        "trade_count": chunk.trade_count,
        "buy_qty": chunk.buy_qty,
        "sell_qty": chunk.sell_qty,
        "buy_notional": chunk.buy_notional,
        "sell_notional": chunk.sell_notional,
        "first_price": chunk.first_price,
        "last_price": chunk.last_price,
        "high_price": chunk.high_price,
        "low_price": chunk.low_price,
        "profile_levels": profile_levels,
        "whale": {
            "trade_count": chunk.whale.trade_count,
            "buy_count": chunk.whale.buy_count,
            "sell_count": chunk.whale.sell_count,
            "notional_total": chunk.whale.notional_total,
            "notional_buy": chunk.whale.notional_buy,
            "notional_sell": chunk.whale.notional_sell,
            "qty_eth_total": chunk.whale.qty_eth_total,
            "qty_eth_buy": chunk.whale.qty_eth_buy,
            "qty_eth_sell": chunk.whale.qty_eth_sell,
            "delta_qty_eth": chunk.whale.qty_eth_buy - chunk.whale.qty_eth_sell,
            "max_single_notional": chunk.whale.max_single_notional,
        },
        "payload_json": payload_json
    });

    NormalizedMdEvent {
        msg_type: "md.agg.trade.1m".to_string(),
        market: key.market.clone(),
        symbol: key.symbol.clone(),
        source_kind: "derived".to_string(),
        backfill_in_progress: chunk.backfill_in_progress,
        routing_key: format!("md.agg.{}.trade.1m.{}", key.market, key.symbol_lower()),
        stream_name: "agg.trade.1m".to_string(),
        event_ts: chunk.chunk_end_ts,
        data,
    }
}

fn build_orderbook_event(key: MinuteKey, chunk: OrderbookChunk) -> NormalizedMdEvent {
    let heatmap_levels = chunk
        .heatmap
        .iter()
        .map(|(tick, level)| {
            json!([
                tick_to_price(*tick),
                level.bid_liquidity,
                level.ask_liquidity
            ])
        })
        .collect::<Vec<_>>();
    let ts_bucket = key.ts_bucket();

    let data = json!({
        "ts_bucket": ts_bucket.to_rfc3339(),
        "chunk_start_ts": key.canonical_chunk_start_ts().to_rfc3339(),
        "chunk_end_ts": key.canonical_chunk_end_ts().to_rfc3339(),
        "source_event_count": chunk.source_event_count,
        "sample_count": chunk.sample_count,
        "bbo_updates": chunk.bbo_updates,
        "spread_sum": chunk.spread_sum,
        "topk_depth_sum": chunk.topk_depth_sum,
        "obi_sum": chunk.obi_sum,
        "obi_l1_sum": chunk.obi_l1_sum,
        "obi_k_sum": chunk.obi_k_sum,
        "obi_k_dw_sum": chunk.obi_k_dw_sum,
        "obi_k_dw_change_sum": chunk.obi_k_dw_change_sum,
        "obi_k_dw_adj_sum": chunk.obi_k_dw_adj_sum,
        "microprice_sum": chunk.microprice_sum,
        "microprice_classic_sum": chunk.microprice_classic_sum,
        "microprice_kappa_sum": chunk.microprice_kappa_sum,
        "microprice_adj_sum": chunk.microprice_adj_sum,
        "ofi_sum": chunk.ofi_sum,
        "obi_k_dw_close": chunk.obi_k_dw_close,
        "heatmap_levels": heatmap_levels,
        "payload_json": {
            "agg_mode": "canonical_1m",
            "source_start_ts": chunk.chunk_start_ts.to_rfc3339(),
            "source_end_ts": chunk.chunk_end_ts.to_rfc3339(),
        }
    });

    NormalizedMdEvent {
        msg_type: "md.agg.orderbook.1m".to_string(),
        market: key.market.clone(),
        symbol: key.symbol.clone(),
        source_kind: "derived".to_string(),
        backfill_in_progress: chunk.backfill_in_progress,
        routing_key: format!("md.agg.{}.orderbook.1m.{}", key.market, key.symbol_lower()),
        stream_name: "agg.orderbook.1m".to_string(),
        event_ts: chunk.chunk_end_ts,
        data,
    }
}

fn build_liq_event(key: MinuteKey, chunk: LiqChunk) -> NormalizedMdEvent {
    let levels = chunk
        .force_liq
        .iter()
        .map(|(tick, level)| json!([tick_to_price(*tick), level.long_liq, level.short_liq]))
        .collect::<Vec<_>>();
    let ts_bucket = key.ts_bucket();

    let data = json!({
        "ts_bucket": ts_bucket.to_rfc3339(),
        "chunk_start_ts": key.canonical_chunk_start_ts().to_rfc3339(),
        "chunk_end_ts": key.canonical_chunk_end_ts().to_rfc3339(),
        "source_event_count": chunk.source_event_count,
        "force_liq_levels": levels,
        "payload_json": {
            "agg_mode": "canonical_1m",
            "source_start_ts": chunk.chunk_start_ts.to_rfc3339(),
            "source_end_ts": chunk.chunk_end_ts.to_rfc3339(),
        }
    });

    NormalizedMdEvent {
        msg_type: "md.agg.liq.1m".to_string(),
        market: key.market.clone(),
        symbol: key.symbol.clone(),
        source_kind: "derived".to_string(),
        backfill_in_progress: chunk.backfill_in_progress,
        routing_key: format!("md.agg.{}.liq.1m.{}", key.market, key.symbol_lower()),
        stream_name: "agg.liq.1m".to_string(),
        event_ts: chunk.chunk_end_ts,
        data,
    }
}

fn build_funding_mark_event(key: MinuteKey, chunk: FundingMarkChunk) -> NormalizedMdEvent {
    let mut mark_points_sorted = chunk.mark_points.clone();
    let mut funding_points_sorted = chunk.funding_points.clone();
    mark_points_sorted.sort_by(|a, b| a.ts.cmp(&b.ts));
    funding_points_sorted.sort_by(|a, b| a.ts.cmp(&b.ts));

    let mark_points = mark_points_sorted
        .into_iter()
        .map(|point| {
            json!({
                "ts": point.ts.to_rfc3339(),
                "mark_price": point.mark_price,
                "index_price": point.index_price,
                "estimated_settle_price": point.estimated_settle_price,
                "funding_rate": point.funding_rate,
                "next_funding_time": point.next_funding_time.map(|ts| ts.to_rfc3339()),
            })
        })
        .collect::<Vec<_>>();

    let funding_points = funding_points_sorted
        .into_iter()
        .map(|point| {
            json!({
                "ts": point.ts.to_rfc3339(),
                "funding_time": point.funding_time.map(|ts| ts.to_rfc3339()),
                "funding_rate": point.funding_rate,
                "mark_price": point.mark_price,
                "next_funding_time": point.next_funding_time.map(|ts| ts.to_rfc3339()),
            })
        })
        .collect::<Vec<_>>();

    let data = json!({
        "ts_bucket": key.ts_bucket().to_rfc3339(),
        "chunk_start_ts": key.canonical_chunk_start_ts().to_rfc3339(),
        "chunk_end_ts": key.canonical_chunk_end_ts().to_rfc3339(),
        "source_event_count": chunk.source_event_count,
        "mark_points": mark_points,
        "funding_points": funding_points,
        "payload_json": {
            "agg_mode": "canonical_1m",
            "source_start_ts": chunk.chunk_start_ts.to_rfc3339(),
            "source_end_ts": chunk.chunk_end_ts.to_rfc3339(),
        }
    });

    NormalizedMdEvent {
        msg_type: "md.agg.funding_mark.1m".to_string(),
        market: key.market.clone(),
        symbol: key.symbol.clone(),
        source_kind: "derived".to_string(),
        backfill_in_progress: chunk.backfill_in_progress,
        routing_key: format!(
            "md.agg.{}.funding_mark.1m.{}",
            key.market,
            key.symbol_lower()
        ),
        stream_name: "agg.funding_mark.1m".to_string(),
        event_ts: chunk.chunk_end_ts,
        data,
    }
}

fn collect_ready_trade_events(
    chunks: &mut HashMap<MinuteKey, TradeChunk>,
    ready_before_minute_ms: i64,
    out: &mut Vec<NormalizedMdEvent>,
) {
    let ready_keys = chunks
        .iter()
        .filter(|(key, chunk)| key.minute_ms < ready_before_minute_ms && chunk.dirty)
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    for key in ready_keys {
        if let Some(chunk) = chunks.get_mut(&key) {
            out.push(build_trade_event(key.clone(), chunk.clone()));
            chunk.dirty = false;
        }
    }
}

fn collect_ready_orderbook_events(
    chunks: &mut HashMap<MinuteKey, OrderbookChunk>,
    ready_before_minute_ms: i64,
    out: &mut Vec<NormalizedMdEvent>,
) {
    let ready_keys = chunks
        .iter()
        .filter(|(key, chunk)| key.minute_ms < ready_before_minute_ms && chunk.dirty)
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    for key in ready_keys {
        if let Some(chunk) = chunks.get_mut(&key) {
            out.push(build_orderbook_event(key.clone(), chunk.clone()));
            chunk.dirty = false;
        }
    }
}

fn collect_ready_liq_events(
    chunks: &mut HashMap<MinuteKey, LiqChunk>,
    ready_before_minute_ms: i64,
    out: &mut Vec<NormalizedMdEvent>,
) {
    let ready_keys = chunks
        .iter()
        .filter(|(key, chunk)| key.minute_ms < ready_before_minute_ms && chunk.dirty)
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    for key in ready_keys {
        if let Some(chunk) = chunks.get_mut(&key) {
            out.push(build_liq_event(key.clone(), chunk.clone()));
            chunk.dirty = false;
        }
    }
}

fn collect_ready_funding_mark_events(
    chunks: &mut HashMap<MinuteKey, FundingMarkChunk>,
    ready_before_minute_ms: i64,
    out: &mut Vec<NormalizedMdEvent>,
) {
    let ready_keys = chunks
        .iter()
        .filter(|(key, chunk)| key.minute_ms < ready_before_minute_ms && chunk.dirty)
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    for key in ready_keys {
        if let Some(chunk) = chunks.get_mut(&key) {
            out.push(build_funding_mark_event(key.clone(), chunk.clone()));
            chunk.dirty = false;
        }
    }
}

fn parse_level_pairs(value: Option<&Value>) -> Vec<(f64, f64)> {
    let Some(array) = value.and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(array.len());
    for row in array {
        let Some(level) = row.as_array() else {
            continue;
        };
        if level.len() < 2 {
            continue;
        }
        let Some(price) = to_f64(&level[0]) else {
            continue;
        };
        let Some(qty) = to_f64(&level[1]) else {
            continue;
        };
        out.push((price, qty));
    }
    out
}

fn sort_output_events(out: &mut [NormalizedMdEvent]) {
    out.sort_by(|a, b| {
        a.event_ts
            .cmp(&b.event_ts)
            .then_with(|| a.msg_type.cmp(&b.msg_type))
            .then_with(|| a.symbol.cmp(&b.symbol))
    });
}

fn value_i64(data: &Value, key: &str) -> Option<i64> {
    data.get(key).and_then(Value::as_i64)
}

fn value_f64(data: &Value, key: &str) -> Option<f64> {
    data.get(key).and_then(to_f64)
}

fn to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::String(s) => s.parse::<f64>().ok(),
        Value::Number(n) => n.as_f64(),
        _ => None,
    }
}

fn parse_optional_ts(data: &Value, key: &str) -> Option<DateTime<Utc>> {
    data.get(key)
        .and_then(Value::as_str)
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

fn parse_optional_vpin_snapshot(value: Option<&Value>) -> Option<VpinStateSnapshot> {
    let value = value?;
    let imbalances = value
        .get("imbalances")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(to_f64)
        .collect::<Vec<_>>();
    Some(VpinStateSnapshot {
        current_buy: value_f64(value, "current_buy").unwrap_or(0.0),
        current_sell: value_f64(value, "current_sell").unwrap_or(0.0),
        current_fill: value_f64(value, "current_fill").unwrap_or(0.0),
        imbalances,
        imbalance_sum: value_f64(value, "imbalance_sum").unwrap_or(0.0),
        last_vpin: value_f64(value, "last_vpin").unwrap_or(0.0),
    })
}

fn floor_minute_millis(ts_ms: i64) -> i64 {
    ts_ms - ts_ms.rem_euclid(60_000)
}

fn millis_to_utc(ts_ms: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_millis(ts_ms).unwrap_or_else(Utc::now)
}

fn price_to_tick(price: f64) -> i64 {
    (price * PRICE_SCALE).round() as i64
}

fn tick_to_price(tick: i64) -> f64 {
    tick as f64 / PRICE_SCALE
}

fn weighted_topk_depth(
    bids: &BTreeMap<i64, f64>,
    asks: &BTreeMap<i64, f64>,
    topk: usize,
) -> (f64, f64) {
    let weighted_bid = bids
        .iter()
        .rev()
        .take(topk)
        .enumerate()
        .map(|(idx, (_, qty))| (-(ORDERBOOK_DW_LAMBDA) * idx as f64).exp() * *qty)
        .sum::<f64>();
    let weighted_ask = asks
        .iter()
        .take(topk)
        .enumerate()
        .map(|(idx, (_, qty))| (-(ORDERBOOK_DW_LAMBDA) * idx as f64).exp() * *qty)
        .sum::<f64>();
    (weighted_bid, weighted_ask)
}

fn collect_heatmap_levels(
    bids: &BTreeMap<i64, f64>,
    asks: &BTreeMap<i64, f64>,
) -> Vec<HeatmapLevelSample> {
    let mut merged = BTreeMap::<i64, BookLevelAgg>::new();
    for (tick, qty) in bids {
        merged.entry(*tick).or_default().bid_liquidity = *qty;
    }
    for (tick, qty) in asks {
        merged.entry(*tick).or_default().ask_liquidity = *qty;
    }
    merged
        .into_iter()
        .map(|(tick, level)| HeatmapLevelSample {
            tick,
            bid_qty: level.bid_liquidity,
            ask_qty: level.ask_liquidity,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        parse_optional_ts, price_to_tick, value_f64, value_i64, AggregateLane,
        MinuteBatchAggregator, NormalizedMdEvent,
    };
    use chrono::{Duration as ChronoDuration, TimeZone, Utc};
    use serde_json::{json, Value};

    #[test]
    fn emits_one_canonical_trade_row_per_minute() {
        let mut aggregator = MinuteBatchAggregator::new(AggregateLane::Trade, 1_000.0);
        let minute_open = ts(2026, 3, 6, 0, 0, 0, 0);
        let outputs = aggregator.aggregate_batch(&[
            trade_event(minute_open + ChronoDuration::seconds(5), 100.0, 1.0, 1),
            trade_event(minute_open + ChronoDuration::seconds(40), 101.0, 2.0, -1),
            trade_event(
                minute_open + ChronoDuration::minutes(1) + ChronoDuration::seconds(1),
                102.0,
                0.5,
                1,
            ),
        ]);

        assert_eq!(outputs.len(), 1);
        let trade_1m = &outputs[0];
        assert_eq!(trade_1m.msg_type, "md.agg.trade.1m");
        assert_eq!(
            parse_optional_ts(&trade_1m.data, "ts_bucket"),
            Some(minute_open)
        );
        assert_eq!(
            parse_optional_ts(&trade_1m.data, "chunk_start_ts"),
            Some(minute_open)
        );
        assert_eq!(
            parse_optional_ts(&trade_1m.data, "chunk_end_ts"),
            Some(minute_open + ChronoDuration::seconds(59) + ChronoDuration::milliseconds(999))
        );
        assert_eq!(value_i64(&trade_1m.data, "trade_count"), Some(2));
        assert_eq!(value_f64(&trade_1m.data, "buy_qty"), Some(1.0));
        assert_eq!(value_f64(&trade_1m.data, "sell_qty"), Some(2.0));
        assert_eq!(value_f64(&trade_1m.data, "first_price"), Some(100.0));
        assert_eq!(value_f64(&trade_1m.data, "last_price"), Some(101.0));
        assert_eq!(
            parse_optional_ts(&trade_1m.data["payload_json"], "source_start_ts"),
            Some(minute_open + ChronoDuration::seconds(5))
        );
        assert_eq!(
            parse_optional_ts(&trade_1m.data["payload_json"], "source_end_ts"),
            Some(minute_open + ChronoDuration::seconds(40))
        );

        let profile = level_pairs(&trade_1m.data, "profile_levels");
        assert_eq!(profile.get(&price_to_tick(100.0)), Some(&(1.0, 0.0)));
        assert_eq!(profile.get(&price_to_tick(101.0)), Some(&(0.0, 2.0)));
    }

    #[test]
    fn late_trade_update_re_emits_same_canonical_minute() {
        let mut aggregator = MinuteBatchAggregator::new(AggregateLane::Trade, 1_000.0);
        let minute_open = ts(2026, 3, 6, 0, 0, 0, 0);

        let first = aggregator.aggregate_batch(&[
            trade_event(minute_open + ChronoDuration::seconds(5), 100.0, 1.0, 1),
            trade_event(
                minute_open + ChronoDuration::minutes(1) + ChronoDuration::seconds(5),
                101.0,
                1.0,
                -1,
            ),
        ]);
        assert_eq!(first.len(), 1);
        assert_eq!(value_i64(&first[0].data, "trade_count"), Some(1));

        let second = aggregator.aggregate_batch(&[
            trade_event(minute_open + ChronoDuration::seconds(45), 100.5, 2.0, 1),
            trade_event(
                minute_open + ChronoDuration::minutes(2) + ChronoDuration::seconds(5),
                103.0,
                1.0,
                1,
            ),
        ]);
        let updated = second
            .iter()
            .find(|event| parse_optional_ts(&event.data, "ts_bucket") == Some(minute_open))
            .expect("expected canonical re-emit for updated minute");

        assert_eq!(value_i64(&updated.data, "trade_count"), Some(2));
        assert_eq!(value_f64(&updated.data, "buy_qty"), Some(3.0));
        assert_eq!(value_f64(&updated.data, "sell_qty"), Some(0.0));
        assert_eq!(
            parse_optional_ts(&updated.data["payload_json"], "source_start_ts"),
            Some(minute_open + ChronoDuration::seconds(5))
        );
        assert_eq!(
            parse_optional_ts(&updated.data["payload_json"], "source_end_ts"),
            Some(minute_open + ChronoDuration::seconds(45))
        );
    }

    #[test]
    fn canonical_orderbook_row_merges_heatmap_levels() {
        let mut aggregator = MinuteBatchAggregator::new(AggregateLane::NonTrade, 1_000.0);
        let minute_open = ts(2026, 3, 6, 0, 0, 0, 0);
        let outputs = aggregator.aggregate_batch(&[
            bbo_event(
                minute_open + ChronoDuration::seconds(5),
                100.0,
                1.0,
                101.0,
                2.0,
            ),
            bbo_event(
                minute_open + ChronoDuration::seconds(40),
                100.0,
                2.0,
                101.0,
                1.0,
            ),
            bbo_event(
                minute_open + ChronoDuration::minutes(1) + ChronoDuration::seconds(1),
                100.5,
                3.0,
                101.5,
                3.0,
            ),
        ]);

        assert_eq!(outputs.len(), 1);
        let orderbook_1m = &outputs[0];
        assert_eq!(orderbook_1m.msg_type, "md.agg.orderbook.1m");
        assert_eq!(
            parse_optional_ts(&orderbook_1m.data, "ts_bucket"),
            Some(minute_open)
        );
        assert_eq!(value_i64(&orderbook_1m.data, "sample_count"), Some(2));
        assert_eq!(value_i64(&orderbook_1m.data, "bbo_updates"), Some(2));

        let heatmap = level_pairs(&orderbook_1m.data, "heatmap_levels");
        assert_eq!(heatmap.get(&price_to_tick(100.0)), Some(&(3.0, 0.0)));
        assert_eq!(heatmap.get(&price_to_tick(101.0)), Some(&(0.0, 3.0)));
    }

    #[test]
    fn flushes_closed_minute_without_next_event() {
        let mut aggregator = MinuteBatchAggregator::new(AggregateLane::Trade, 1_000.0);
        let minute_open = ts(2026, 3, 6, 0, 0, 0, 0);

        let outputs = aggregator.aggregate_batch(&[
            trade_event(minute_open + ChronoDuration::seconds(5), 100.0, 1.0, 1),
            trade_event(minute_open + ChronoDuration::seconds(40), 101.0, 2.0, -1),
        ]);
        assert!(outputs.is_empty());

        let flushed = aggregator.flush_closed_minutes(minute_open + ChronoDuration::minutes(1));
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].msg_type, "md.agg.trade.1m");
        assert_eq!(
            parse_optional_ts(&flushed[0].data, "ts_bucket"),
            Some(minute_open)
        );
        assert_eq!(value_i64(&flushed[0].data, "trade_count"), Some(2));
    }

    #[test]
    fn trade_1s_preagg_restores_vpin_snapshot() {
        let mut aggregator = MinuteBatchAggregator::new(AggregateLane::Trade, 1_000.0);
        let minute_open = ts(2026, 3, 6, 0, 0, 0, 0);
        let snapshot = json!({
            "current_buy": 3.0,
            "current_sell": 1.0,
            "current_fill": 4.0,
            "imbalances": [10.0, 12.0],
            "imbalance_sum": 22.0,
            "last_vpin": 0.44
        });

        let outputs = aggregator.aggregate_batch(&[
            trade_1s_event(
                minute_open + ChronoDuration::seconds(5),
                1.0,
                0.5,
                snapshot.clone(),
            ),
            trade_event(
                minute_open + ChronoDuration::minutes(1) + ChronoDuration::seconds(1),
                102.0,
                0.5,
                1,
            ),
        ]);

        assert_eq!(outputs.len(), 1);
        let trade_1m = &outputs[0];
        assert_eq!(
            trade_1m.data["payload_json"]["vpin_state"]["last_vpin"].as_f64(),
            Some(0.44)
        );
        assert_eq!(
            trade_1m.data["payload_json"]["vpin_state"]["current_fill"].as_f64(),
            Some(4.0)
        );
        assert_eq!(
            trade_1m.data["payload_json"]["vpin_state"]["imbalances"]
                .as_array()
                .map(|arr| arr.len()),
            Some(2)
        );
    }

    #[test]
    fn live_1m_kline_updates_fold_into_canonical_trade_row() {
        let mut aggregator = MinuteBatchAggregator::new(AggregateLane::Trade, 1_000.0);
        let minute_open = ts(2026, 3, 6, 4, 26, 0, 0);
        let outputs = aggregator.aggregate_batch(&[
            kline_event(
                minute_open + ChronoDuration::seconds(5),
                minute_open,
                2078.01,
                2078.81,
                2076.03,
                2076.35,
                false,
            ),
            trade_event(
                minute_open + ChronoDuration::minutes(1) + ChronoDuration::seconds(1),
                2075.0,
                0.5,
                1,
            ),
        ]);

        assert_eq!(outputs.len(), 1);
        let trade_1m = &outputs[0];
        assert_eq!(
            parse_optional_ts(&trade_1m.data, "ts_bucket"),
            Some(minute_open)
        );
        assert_eq!(value_f64(&trade_1m.data, "first_price"), Some(2078.01));
        assert_eq!(value_f64(&trade_1m.data, "high_price"), Some(2078.81));
        assert_eq!(value_f64(&trade_1m.data, "low_price"), Some(2076.03));
        assert_eq!(value_f64(&trade_1m.data, "last_price"), Some(2076.35));
    }

    fn trade_event(
        ts: chrono::DateTime<Utc>,
        price: f64,
        qty_eth: f64,
        aggressor_side: i64,
    ) -> NormalizedMdEvent {
        NormalizedMdEvent {
            msg_type: "md.trade".to_string(),
            market: "futures".to_string(),
            symbol: "ETHUSDT".to_string(),
            source_kind: "ws".to_string(),
            backfill_in_progress: false,
            routing_key: "md.futures.trade.ethusdt".to_string(),
            stream_name: "trade".to_string(),
            event_ts: ts,
            data: json!({
                "price": price,
                "qty_eth": qty_eth,
                "notional_usdt": price * qty_eth,
                "aggressor_side": aggressor_side,
            }),
        }
    }

    fn bbo_event(
        ts: chrono::DateTime<Utc>,
        bid_price: f64,
        bid_qty: f64,
        ask_price: f64,
        ask_qty: f64,
    ) -> NormalizedMdEvent {
        NormalizedMdEvent {
            msg_type: "md.bbo".to_string(),
            market: "futures".to_string(),
            symbol: "ETHUSDT".to_string(),
            source_kind: "ws".to_string(),
            backfill_in_progress: false,
            routing_key: "md.futures.bbo.ethusdt".to_string(),
            stream_name: "bbo".to_string(),
            event_ts: ts,
            data: json!({
                "bid_price": bid_price,
                "bid_qty": bid_qty,
                "ask_price": ask_price,
                "ask_qty": ask_qty,
                "sample_count": 1,
            }),
        }
    }

    fn trade_1s_event(
        ts: chrono::DateTime<Utc>,
        buy_qty: f64,
        sell_qty: f64,
        snapshot: Value,
    ) -> NormalizedMdEvent {
        NormalizedMdEvent {
            msg_type: "md.agg.trade.1s".to_string(),
            market: "futures".to_string(),
            symbol: "ETHUSDT".to_string(),
            source_kind: "derived".to_string(),
            backfill_in_progress: false,
            routing_key: "md.agg.futures.trade.1s.ethusdt".to_string(),
            stream_name: "agg.trade.1s".to_string(),
            event_ts: ts,
            data: json!({
                "ts_bucket": ts.to_rfc3339(),
                "chunk_start_ts": ts.to_rfc3339(),
                "chunk_end_ts": ts.to_rfc3339(),
                "source_event_count": 1,
                "trade_count": 1,
                "buy_qty": buy_qty,
                "sell_qty": sell_qty,
                "buy_notional": buy_qty * 100.0,
                "sell_notional": sell_qty * 100.0,
                "first_price": 100.0,
                "last_price": 100.0,
                "high_price": 100.0,
                "low_price": 100.0,
                "profile_levels": [[100.0, buy_qty, sell_qty]],
                "whale": {
                    "trade_count": 0,
                    "buy_count": 0,
                    "sell_count": 0,
                    "notional_total": 0.0,
                    "notional_buy": 0.0,
                    "notional_sell": 0.0,
                    "qty_eth_total": 0.0,
                    "qty_eth_buy": 0.0,
                    "qty_eth_sell": 0.0,
                    "delta_qty_eth": 0.0,
                    "max_single_notional": 0.0
                },
                "payload_json": {
                    "agg_mode": "batch_incremental_1s_combinable",
                    "vpin_state": snapshot
                }
            }),
        }
    }

    fn kline_event(
        ts: chrono::DateTime<Utc>,
        open_time: chrono::DateTime<Utc>,
        open_price: f64,
        high_price: f64,
        low_price: f64,
        close_price: f64,
        is_closed: bool,
    ) -> NormalizedMdEvent {
        NormalizedMdEvent {
            msg_type: "md.kline".to_string(),
            market: "futures".to_string(),
            symbol: "ETHUSDT".to_string(),
            source_kind: "ws".to_string(),
            backfill_in_progress: false,
            routing_key: "md.futures.kline.1m.ethusdt".to_string(),
            stream_name: "ethusdt@kline_1m".to_string(),
            event_ts: ts,
            data: json!({
                "interval_code": "1m",
                "open_time": open_time.to_rfc3339(),
                "close_time": (open_time + ChronoDuration::minutes(1) - ChronoDuration::milliseconds(1)).to_rfc3339(),
                "open_price": open_price,
                "high_price": high_price,
                "low_price": low_price,
                "close_price": close_price,
                "is_closed": is_closed,
            }),
        }
    }

    fn level_pairs(data: &Value, key: &str) -> std::collections::BTreeMap<i64, (f64, f64)> {
        data.get(key)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|level| {
                let arr = level.as_array()?;
                let price = arr.first().and_then(Value::as_f64)?;
                let left = arr.get(1).and_then(Value::as_f64).unwrap_or(0.0);
                let right = arr.get(2).and_then(Value::as_f64).unwrap_or(0.0);
                Some((price_to_tick(price), (left, right)))
            })
            .collect()
    }

    fn ts(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
        millis: i64,
    ) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
            .single()
            .unwrap()
            + ChronoDuration::milliseconds(millis)
    }
}
