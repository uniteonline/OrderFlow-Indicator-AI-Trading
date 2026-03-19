use crate::aggregate::minute_agg::{
    AggregateLane, MinuteBatchAggregator, DEFAULT_WHALE_THRESHOLD_USDT,
};
use crate::normalize::NormalizedMdEvent;
use crate::observability::metrics::AppMetrics;
use crate::sinks::{
    md_db_writer::MdDbWriter,
    mq_publisher::MqPublisher,
    ops_db_writer::{CheckpointUpsert, OpsDbWriter},
    outbox_writer::OutboxWriter,
    parquet_sink::ParquetSink,
};
use crate::state::checkpoints;
use anyhow::{anyhow, Result};
use chrono::Utc;
use futures_util::stream::{self, StreamExt};
use serde_json::{json, Map as JsonMap, Value};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::{timeout, Duration, Instant, MissedTickBehavior};
use tracing::{error, info, warn};

// Slow-path warn thresholds are tuned to avoid noisy WARN spam on normal
// sub-second jitter while still surfacing true SLA risks.
const PERSIST_SLOW_WARN_MS: u128 = 5_000;
const PERSIST_BATCH_SLOW_WARN_MS: u128 = 5_000;
const PERSIST_NON_TRADE_ORDERBOOK_WORKER_SHARDS: usize = 4;
const PERSIST_NON_TRADE_OTHER_WORKER_SHARDS: usize = 4;
const PERSIST_NON_TRADE_BATCH_SIZE: usize = 2_000;
const PERSIST_NON_TRADE_COALESCE_MS: u64 = 80;
const ORDERBOOK_PREBATCH_FLUSH_MS: u64 = 1_000;
const ORDERBOOK_PREBATCH_MAX_EVENTS: usize = 20_000;
const TRADE_PREBATCH_FLUSH_MS: u64 = 1_000;
const TRADE_PREBATCH_MAX_EVENTS: usize = 20_000;
const TRADE_PREAGG_PRICE_SCALE: f64 = 100.0;
const OB_PREAGG_PRICE_SCALE: f64 = 100.0;
const OB_PREAGG_TOPK: usize = 5;
const OB_PREAGG_DW_LAMBDA: f64 = 0.35;
const OB_PREAGG_MICROPRICE_KAPPA: f64 = 0.35;
const OB_PREAGG_MICROPRICE_LAMBDA_OBI: f64 = 0.15;
const OB_PREAGG_SIZE_FLOOR: f64 = 1e-3;
const OB_PREAGG_TICK_SIZE: f64 = 1.0 / OB_PREAGG_PRICE_SCALE;
const PERSIST_BBO_DB_ROLLUP_WINDOW_MS: i64 = 250;
const PERSIST_TRADE_WORKER_SHARDS: usize = 3;
const PERSIST_TRADE_BATCH_SIZE: usize = 512;
const PERSIST_TRADE_COALESCE_MS: u64 = 20;
// Bounded channel capacities per shard. Under persistent DB/MQ backpressure that fills the
// channel, events are dropped with a WARN log rather than accumulating unboundedly in memory.
// At peak TESTUSDT trade rate (~500 tps across 3 shards ~=167/shard), 2000 items ~=12 s buffer.
const PERSIST_TRADE_CHANNEL_CAPACITY: usize = 2_000;
// Orderbook-heavy non-trade events can burst very hard on bookTicker/depth streams.
const PERSIST_NON_TRADE_CHANNEL_CAPACITY: usize = 20_000;
const PERSIST_PENDING_WARN_STEP: usize = 20_000;
const PERSIST_EVENT_LAG_WARN_SECS: i64 = 5;
const CHECKPOINT_FLUSH_INTERVAL_MS: u64 = 10_000;
const CHECKPOINT_FLUSH_SLOW_WARN_MS: u128 = 5_000;
const CHECKPOINT_PENDING_WARN_STEP: usize = 10_000;
const CHECKPOINT_CHANNEL_CAPACITY: usize = 20_000;
const CHECKPOINT_BUFFER_FLUSH_SIZE: usize = 2_000;
const HOT_PATH_DIRECT_MQ_ENABLED: bool = true;
const HOT_PATH_DIRECT_MQ_PUBLISH_CONCURRENCY: usize = 64;
const RAW_PARQUET_QUEUE_CAPACITY: usize = 16_384;
const RAW_PARQUET_FLUSH_INTERVAL_MS: u64 = 5_000;
const RAW_PARQUET_PARTITION_FLUSH_ROWS: usize = 50_000;
const RAW_PARQUET_GLOBAL_MAX_BUFFERED_ROWS: usize = 500_000;
const MINUTE_AGG_FLUSH_INTERVAL_MS: u64 = 1_000;
static RAW_PARQUET_SINK: OnceLock<Arc<ParquetSink>> = OnceLock::new();
static RAW_PARQUET_TX: OnceLock<mpsc::Sender<Vec<NormalizedMdEvent>>> = OnceLock::new();
static PERSIST_QUEUE_REGISTRY: OnceLock<StdMutex<HashMap<&'static str, AsyncPersistQueue>>> =
    OnceLock::new();
static DIRECT_MINUTE_AGGREGATORS: OnceLock<
    StdMutex<HashMap<AggregateLane, MinuteBatchAggregator>>,
> = OnceLock::new();
static DIRECT_MINUTE_FLUSHER_STARTED: OnceLock<()> = OnceLock::new();

#[derive(Debug)]
struct PersistJob {
    events: Vec<NormalizedMdEvent>,
    source_count: usize,
    checkpoint_updates: Vec<PendingCheckpoint>,
    enqueued_at: Instant,
}

#[derive(Debug)]
struct PendingOrderbookEvent {
    event: NormalizedMdEvent,
    enqueued_at: Instant,
}

#[derive(Debug)]
struct PendingTradeEvent {
    event: NormalizedMdEvent,
    enqueued_at: Instant,
}

#[derive(Debug, Clone, Default)]
struct TradeLevelAgg {
    buy_qty: f64,
    sell_qty: f64,
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

const TRADE_VPIN_BUCKET_SIZE_BASE: f64 = 50.0;
const TRADE_VPIN_ROLLING_BUCKETS: usize = 50;
const TRADE_VPIN_EPS: f64 = 1e-12;

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
            bucket_size: TRADE_VPIN_BUCKET_SIZE_BASE,
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

#[derive(Debug, Clone)]
struct OrderbookSecondChunk {
    second_ms: i64,
    market: String,
    symbol: String,
    first_enqueued_at: Instant,
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
    latest_checkpoints: HashMap<String, PendingCheckpoint>,
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
            obi: obi_l1,
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

impl OrderbookSecondChunk {
    fn new(event: &NormalizedMdEvent, first_enqueued_at: Instant) -> Self {
        let second_ms = floor_timestamp_millis(event.event_ts.timestamp_millis(), 1_000);
        Self {
            second_ms,
            market: event.market.clone(),
            symbol: event.symbol.clone(),
            first_enqueued_at,
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
            latest_checkpoints: HashMap::new(),
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

        let cp = PendingCheckpoint::from_event(event);
        let key = cp.key();
        let replace = self
            .latest_checkpoints
            .get(&key)
            .map(|existing| cp.last_event_ts >= existing.last_event_ts)
            .unwrap_or(true);
        if replace {
            self.latest_checkpoints.insert(key, cp);
        }
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

    fn finalize_to_event(mut self) -> (NormalizedMdEvent, Vec<PendingCheckpoint>) {
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
        let event = NormalizedMdEvent {
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
        };
        let checkpoints = self.latest_checkpoints.into_values().collect::<Vec<_>>();
        (event, checkpoints)
    }
}

#[derive(Debug, Clone)]
struct TradeSecondChunk {
    second_ms: i64,
    market: String,
    symbol: String,
    first_enqueued_at: Instant,
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
    latest_checkpoints: HashMap<String, PendingCheckpoint>,
}

impl TradeSecondChunk {
    fn new(event: &NormalizedMdEvent, first_enqueued_at: Instant) -> Self {
        let second_ms = floor_timestamp_millis(event.event_ts.timestamp_millis(), 1_000);
        Self {
            second_ms,
            market: event.market.clone(),
            symbol: event.symbol.clone(),
            first_enqueued_at,
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
            latest_checkpoints: HashMap::new(),
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

        let cp = PendingCheckpoint::from_event(event);
        let key = cp.key();
        let replace = self
            .latest_checkpoints
            .get(&key)
            .map(|existing| cp.last_event_ts >= existing.last_event_ts)
            .unwrap_or(true);
        if replace {
            self.latest_checkpoints.insert(key, cp);
        }
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

    fn finalize_to_event(self) -> (NormalizedMdEvent, Vec<PendingCheckpoint>) {
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
                "qty_base_total": self.whale.qty_eth_total,
                "qty_base_buy": self.whale.qty_eth_buy,
                "qty_base_sell": self.whale.qty_eth_sell,
                "delta_qty_base": self.whale.qty_eth_buy - self.whale.qty_eth_sell,
                "qty_eth_total": self.whale.qty_eth_total,
                "qty_eth_buy": self.whale.qty_eth_buy,
                "qty_eth_sell": self.whale.qty_eth_sell,
                "delta_qty_eth": self.whale.qty_eth_buy - self.whale.qty_eth_sell,
                "max_single_notional": self.whale.max_single_notional,
            },
            "payload_json": payload_json
        });
        let event = NormalizedMdEvent {
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
        };
        let checkpoints = self.latest_checkpoints.into_values().collect::<Vec<_>>();
        (event, checkpoints)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PersistLane {
    Trade,
    NonTradeOrderbook,
    NonTradeOther,
}

pub fn configure_parquet_sink(sink: Arc<ParquetSink>) {
    if RAW_PARQUET_SINK.set(sink).is_err() {
        warn!("raw parquet sink already configured, skip reconfigure");
    }
    let (tx, rx) = mpsc::channel::<Vec<NormalizedMdEvent>>(RAW_PARQUET_QUEUE_CAPACITY);
    if RAW_PARQUET_TX.set(tx).is_err() {
        warn!("raw parquet queue already configured, skip reconfigure");
        return;
    }
    let sink_cloned = RAW_PARQUET_SINK
        .get()
        .cloned()
        .unwrap_or_else(|| Arc::new(ParquetSink::default()));
    tokio::spawn(async move {
        run_raw_parquet_worker(rx, sink_cloned).await;
    });
}

pub fn mirror_raw_event_to_cold_store(event: &NormalizedMdEvent) {
    if matches!(
        event.msg_type.as_str(),
        "md.trade" | "md.depth" | "md.bbo" | "md.orderbook_snapshot_l2"
    ) {
        enqueue_raw_single_for_parquet(event);
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct WindowKey {
    msg_type: String,
    market: String,
    symbol: String,
    stream_name: String,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct BboWindowKey {
    market: String,
    symbol: String,
    stream_name: String,
    window_start_ms: i64,
}

impl BboWindowKey {
    fn from_event(event: &NormalizedMdEvent) -> Self {
        let window_start_ms = floor_timestamp_millis(
            event.event_ts.timestamp_millis(),
            PERSIST_BBO_DB_ROLLUP_WINDOW_MS,
        );
        Self {
            market: event.market.clone(),
            symbol: event.symbol.clone(),
            stream_name: event.stream_name.clone(),
            window_start_ms,
        }
    }
}

impl WindowKey {
    fn from_event(event: &NormalizedMdEvent) -> Self {
        Self {
            msg_type: event.msg_type.clone(),
            market: event.market.clone(),
            symbol: event.symbol.clone(),
            stream_name: event.stream_name.clone(),
        }
    }
}

struct DepthWindowState {
    first_event_ts: chrono::DateTime<Utc>,
    latest_event_ts: chrono::DateTime<Utc>,
    first_update_id: Option<i64>,
    event_count: usize,
    total_level_updates: usize,
    max_level_updates: usize,
    latest_event: NormalizedMdEvent,
}

impl DepthWindowState {
    fn new(event: &NormalizedMdEvent) -> Self {
        let level_updates = depth_level_update_count(&event.data);
        Self {
            first_event_ts: event.event_ts,
            latest_event_ts: event.event_ts,
            first_update_id: value_i64(&event.data, "first_update_id"),
            event_count: 1,
            total_level_updates: level_updates,
            max_level_updates: level_updates,
            latest_event: event.clone(),
        }
    }

    fn update(&mut self, event: &NormalizedMdEvent) {
        let level_updates = depth_level_update_count(&event.data);
        self.event_count = self.event_count.saturating_add(1);
        self.total_level_updates = self.total_level_updates.saturating_add(level_updates);
        self.max_level_updates = self.max_level_updates.max(level_updates);
        if self.first_update_id.is_none() {
            self.first_update_id = value_i64(&event.data, "first_update_id");
        }
        if event.event_ts >= self.latest_event_ts {
            self.latest_event_ts = event.event_ts;
            self.latest_event = event.clone();
        }
    }

    fn into_event(mut self) -> NormalizedMdEvent {
        if let Some(data_obj) = self.latest_event.data.as_object_mut() {
            let latest_bid_levels = data_obj
                .get("bids_delta")
                .and_then(Value::as_array)
                .map(|v| v.len())
                .unwrap_or(0);
            let latest_ask_levels = data_obj
                .get("asks_delta")
                .and_then(Value::as_array)
                .map(|v| v.len())
                .unwrap_or(0);
            if let Some(first_uid) = self.first_update_id {
                data_obj.insert("first_update_id".to_string(), Value::from(first_uid));
            }
            data_obj.insert(
                "event_count".to_string(),
                Value::from(self.event_count as i64),
            );
            // Depth raw deltas are high-volume details; keep only window features in DB.
            data_obj.insert("bids_delta".to_string(), Value::Array(Vec::new()));
            data_obj.insert("asks_delta".to_string(), Value::Array(Vec::new()));
            upsert_payload_window_agg(
                data_obj,
                json!({
                    "agg_mode": "latest_in_window",
                    "window_start_ts": self.first_event_ts.to_rfc3339(),
                    "window_end_ts": self.latest_event_ts.to_rfc3339(),
                    "window_event_count": self.event_count as i64,
                    "depth_last_bid_levels": latest_bid_levels as i64,
                    "depth_last_ask_levels": latest_ask_levels as i64,
                    "depth_total_level_updates": self.total_level_updates as i64,
                    "depth_max_level_updates": self.max_level_updates as i64,
                }),
            );
        }
        self.latest_event
    }
}

struct BboWindowState {
    window_start_ts: chrono::DateTime<Utc>,
    window_end_ts: chrono::DateTime<Utc>,
    latest_event_ts: chrono::DateTime<Utc>,
    event_count: usize,
    spread_sum: f64,
    spread_min: f64,
    spread_max: f64,
    mid_sum: f64,
    latest_event: NormalizedMdEvent,
}

impl BboWindowState {
    fn new(event: &NormalizedMdEvent) -> Self {
        let (spread, mid) = bbo_spread_mid(&event.data);
        let window_start_ms = floor_timestamp_millis(
            event.event_ts.timestamp_millis(),
            PERSIST_BBO_DB_ROLLUP_WINDOW_MS,
        );
        let window_end_ms = window_start_ms.saturating_add(PERSIST_BBO_DB_ROLLUP_WINDOW_MS);
        // When the ask is missing, spread=0 which is invalid. Initialize spread_min to
        // f64::INFINITY so that the first valid (>0) spread wins the min comparison.
        // Previously spread_min was initialized to `spread` directly, which meant a window
        // starting with a missing-ask event permanently showed bbo_spread_min = 0.
        let valid_spread = spread > 0.0;
        Self {
            window_start_ts: millis_to_utc(window_start_ms),
            window_end_ts: millis_to_utc(window_end_ms),
            latest_event_ts: event.event_ts,
            event_count: 1,
            spread_sum: if valid_spread { spread } else { 0.0 },
            spread_min: if valid_spread { spread } else { f64::INFINITY },
            spread_max: spread,
            mid_sum: mid,
            latest_event: event.clone(),
        }
    }

    fn update(&mut self, event: &NormalizedMdEvent) {
        let (spread, mid) = bbo_spread_mid(&event.data);
        self.event_count = self.event_count.saturating_add(1);
        self.mid_sum += mid;
        if spread > 0.0 {
            // Only include valid spreads (ask present) in sum and min tracking.
            self.spread_sum += spread;
            self.spread_min = self.spread_min.min(spread);
        }
        self.spread_max = self.spread_max.max(spread);
        if event.event_ts >= self.latest_event_ts {
            self.latest_event_ts = event.event_ts;
            self.latest_event = event.clone();
        }
    }

    fn into_event(mut self) -> NormalizedMdEvent {
        if let Some(data_obj) = self.latest_event.data.as_object_mut() {
            let event_count = self.event_count.max(1) as f64;
            let spread_avg = self.spread_sum / event_count;
            let mid_avg = self.mid_sum / event_count;
            // If no valid spread was ever observed (all asks were missing), fall back to
            // spread_avg (which may be 0 if spread_sum=0) rather than emitting INFINITY.
            let spread_min_out = if self.spread_min.is_finite() {
                self.spread_min
            } else {
                spread_avg
            };
            data_obj.insert(
                "event_count".to_string(),
                Value::from(self.event_count as i64),
            );
            data_obj.insert(
                "sample_count".to_string(),
                Value::from(self.event_count as i64),
            );
            data_obj.insert(
                "window_start_ts".to_string(),
                Value::from(self.window_start_ts.to_rfc3339()),
            );
            data_obj.insert(
                "window_end_ts".to_string(),
                Value::from(self.window_end_ts.to_rfc3339()),
            );
            data_obj.insert(
                "spread_min".to_string(),
                Value::from(spread_min_rounded(spread_min_out)),
            );
            data_obj.insert(
                "spread_max".to_string(),
                Value::from(spread_min_rounded(self.spread_max)),
            );
            data_obj.insert(
                "spread_avg".to_string(),
                Value::from(spread_min_rounded(spread_avg)),
            );
            data_obj.insert(
                "mid_avg".to_string(),
                Value::from(spread_min_rounded(mid_avg)),
            );
            upsert_payload_window_agg(
                data_obj,
                json!({
                    "agg_mode": "latest_in_window",
                    "window_start_ts": self.window_start_ts.to_rfc3339(),
                    "window_end_ts": self.window_end_ts.to_rfc3339(),
                    "window_latest_event_ts": self.latest_event_ts.to_rfc3339(),
                    "window_event_count": self.event_count as i64,
                    "bbo_spread_min": spread_min_rounded(spread_min_out),
                    "bbo_spread_max": spread_min_rounded(self.spread_max),
                    "bbo_spread_avg": spread_min_rounded(spread_avg),
                    "bbo_mid_avg": spread_min_rounded(mid_avg),
                }),
            );
        }
        self.latest_event
    }
}

#[derive(Clone)]
pub struct AsyncPersistQueue {
    market: &'static str,
    trade_senders: Arc<Vec<mpsc::Sender<PersistJob>>>,
    trade_prebatch_senders: Arc<Vec<mpsc::UnboundedSender<PendingTradeEvent>>>,
    non_trade_orderbook_senders: Arc<Vec<mpsc::Sender<PersistJob>>>,
    non_trade_orderbook_prebatch_senders: Arc<Vec<mpsc::UnboundedSender<PendingOrderbookEvent>>>,
    non_trade_other_senders: Arc<Vec<mpsc::Sender<PersistJob>>>,
    pending: Arc<AtomicUsize>,
    trade_pending: Arc<AtomicUsize>,
    non_trade_pending: Arc<AtomicUsize>,
}

#[derive(Debug, Clone, Copy)]
pub struct PersistQueueStats {
    pub pending: usize,
    pub trade_pending: usize,
    pub non_trade_pending: usize,
}

#[derive(Clone)]
struct AsyncCheckpointQueue {
    market: &'static str,
    sender: mpsc::Sender<PendingCheckpoint>,
    pending: Arc<AtomicUsize>,
}

fn persist_queue_registry() -> &'static StdMutex<HashMap<&'static str, AsyncPersistQueue>> {
    PERSIST_QUEUE_REGISTRY.get_or_init(|| StdMutex::new(HashMap::new()))
}

fn register_persist_queue(market: &'static str, queue: &AsyncPersistQueue) {
    if let Ok(mut registry) = persist_queue_registry().lock() {
        registry.insert(market, queue.clone());
    } else {
        warn!(
            market = market,
            "persist queue registry lock poisoned, skip register"
        );
    }
}

pub async fn try_enqueue_registered_persist_event(
    market: &str,
    event: NormalizedMdEvent,
) -> Result<bool> {
    let queue = if let Ok(registry) = persist_queue_registry().lock() {
        registry.get(market).cloned()
    } else {
        warn!(
            market = market,
            "persist queue registry lock poisoned, fallback to direct persist"
        );
        None
    };

    if let Some(queue) = queue {
        queue.enqueue(event).await?;
        return Ok(true);
    }
    Ok(false)
}

pub fn has_registered_persist_queue(market: &str) -> bool {
    if let Ok(registry) = persist_queue_registry().lock() {
        return registry.contains_key(market);
    }
    false
}

pub fn registered_persist_queue_stats(market: &str) -> Option<PersistQueueStats> {
    let queue = if let Ok(registry) = persist_queue_registry().lock() {
        registry.get(market).cloned()
    } else {
        warn!(
            market = market,
            "persist queue registry lock poisoned, skip queue stats"
        );
        None
    }?;

    Some(PersistQueueStats {
        pending: queue.pending(),
        trade_pending: queue.trade_pending(),
        non_trade_pending: queue.non_trade_pending(),
    })
}

impl AsyncPersistQueue {
    pub fn spawn(
        market: &'static str,
        db_writer: Arc<MdDbWriter>,
        publisher: Arc<MqPublisher>,
        outbox_writer: Arc<OutboxWriter>,
        ops_writer: Arc<OpsDbWriter>,
        metrics: Arc<AppMetrics>,
    ) -> Self {
        let pending = Arc::new(AtomicUsize::new(0));
        let trade_pending = Arc::new(AtomicUsize::new(0));
        let non_trade_pending = Arc::new(AtomicUsize::new(0));
        let checkpoint_queue = AsyncCheckpointQueue::spawn(market, Arc::clone(&ops_writer));
        let mut trade_senders = Vec::with_capacity(PERSIST_TRADE_WORKER_SHARDS);
        let mut trade_prebatch_senders = Vec::with_capacity(PERSIST_TRADE_WORKER_SHARDS);
        for shard_id in 0..PERSIST_TRADE_WORKER_SHARDS {
            let (worker_tx, worker_rx) =
                mpsc::channel::<PersistJob>(PERSIST_TRADE_CHANNEL_CAPACITY);
            let (prebatch_tx, prebatch_rx) = mpsc::unbounded_channel::<PendingTradeEvent>();
            let pending_total_cloned = Arc::clone(&pending);
            let lane_pending_cloned = Arc::clone(&trade_pending);
            let pending_total_for_prebatch = Arc::clone(&pending);
            let lane_pending_for_prebatch = Arc::clone(&trade_pending);
            let db_writer_cloned = Arc::clone(&db_writer);
            let publisher_cloned = Arc::clone(&publisher);
            let outbox_writer_cloned = Arc::clone(&outbox_writer);
            let ops_writer_cloned = Arc::clone(&ops_writer);
            let metrics_cloned = Arc::clone(&metrics);
            let checkpoint_queue_cloned = checkpoint_queue.clone();
            let worker_tx_for_prebatch = worker_tx.clone();

            tokio::spawn(async move {
                run_worker(
                    market,
                    "trade",
                    shard_id,
                    worker_rx,
                    pending_total_cloned,
                    lane_pending_cloned,
                    PERSIST_TRADE_BATCH_SIZE,
                    PERSIST_TRADE_COALESCE_MS,
                    false,
                    db_writer_cloned,
                    publisher_cloned,
                    outbox_writer_cloned,
                    ops_writer_cloned,
                    checkpoint_queue_cloned,
                    metrics_cloned,
                )
                .await;
            });

            tokio::spawn(async move {
                run_trade_prebatch_worker(
                    market,
                    shard_id,
                    prebatch_rx,
                    worker_tx_for_prebatch,
                    pending_total_for_prebatch,
                    lane_pending_for_prebatch,
                )
                .await;
            });

            trade_senders.push(worker_tx);
            trade_prebatch_senders.push(prebatch_tx);
        }

        let mut non_trade_orderbook_senders =
            Vec::with_capacity(PERSIST_NON_TRADE_ORDERBOOK_WORKER_SHARDS);
        let mut non_trade_orderbook_prebatch_senders =
            Vec::with_capacity(PERSIST_NON_TRADE_ORDERBOOK_WORKER_SHARDS);
        for shard_id in 0..PERSIST_NON_TRADE_ORDERBOOK_WORKER_SHARDS {
            let (worker_tx, worker_rx) =
                mpsc::channel::<PersistJob>(PERSIST_NON_TRADE_CHANNEL_CAPACITY);
            let (prebatch_tx, prebatch_rx) = mpsc::unbounded_channel::<PendingOrderbookEvent>();
            let pending_total_cloned = Arc::clone(&pending);
            let lane_pending_cloned = Arc::clone(&non_trade_pending);
            let pending_total_for_prebatch = Arc::clone(&pending);
            let lane_pending_for_prebatch = Arc::clone(&non_trade_pending);
            let db_writer_cloned = Arc::clone(&db_writer);
            let publisher_cloned = Arc::clone(&publisher);
            let outbox_writer_cloned = Arc::clone(&outbox_writer);
            let ops_writer_cloned = Arc::clone(&ops_writer);
            let metrics_cloned = Arc::clone(&metrics);
            let checkpoint_queue_cloned = checkpoint_queue.clone();
            let worker_tx_for_prebatch = worker_tx.clone();

            tokio::spawn(async move {
                run_worker(
                    market,
                    "non_trade_orderbook",
                    shard_id,
                    worker_rx,
                    pending_total_cloned,
                    lane_pending_cloned,
                    PERSIST_NON_TRADE_BATCH_SIZE,
                    PERSIST_NON_TRADE_COALESCE_MS,
                    true,
                    db_writer_cloned,
                    publisher_cloned,
                    outbox_writer_cloned,
                    ops_writer_cloned,
                    checkpoint_queue_cloned,
                    metrics_cloned,
                )
                .await;
            });

            tokio::spawn(async move {
                run_orderbook_prebatch_worker(
                    market,
                    shard_id,
                    prebatch_rx,
                    worker_tx_for_prebatch,
                    pending_total_for_prebatch,
                    lane_pending_for_prebatch,
                )
                .await;
            });

            non_trade_orderbook_prebatch_senders.push(prebatch_tx);
            non_trade_orderbook_senders.push(worker_tx);
        }

        let mut non_trade_other_senders = Vec::with_capacity(PERSIST_NON_TRADE_OTHER_WORKER_SHARDS);
        for shard_id in 0..PERSIST_NON_TRADE_OTHER_WORKER_SHARDS {
            let (tx, rx) = mpsc::channel::<PersistJob>(PERSIST_NON_TRADE_CHANNEL_CAPACITY);
            let pending_total_cloned = Arc::clone(&pending);
            let lane_pending_cloned = Arc::clone(&non_trade_pending);
            let db_writer_cloned = Arc::clone(&db_writer);
            let publisher_cloned = Arc::clone(&publisher);
            let outbox_writer_cloned = Arc::clone(&outbox_writer);
            let ops_writer_cloned = Arc::clone(&ops_writer);
            let metrics_cloned = Arc::clone(&metrics);
            let checkpoint_queue_cloned = checkpoint_queue.clone();

            tokio::spawn(async move {
                run_worker(
                    market,
                    "non_trade_other",
                    shard_id,
                    rx,
                    pending_total_cloned,
                    lane_pending_cloned,
                    PERSIST_NON_TRADE_BATCH_SIZE,
                    PERSIST_NON_TRADE_COALESCE_MS,
                    true,
                    db_writer_cloned,
                    publisher_cloned,
                    outbox_writer_cloned,
                    ops_writer_cloned,
                    checkpoint_queue_cloned,
                    metrics_cloned,
                )
                .await;
            });

            non_trade_other_senders.push(tx);
        }

        info!(
            market = market,
            trade_shards = PERSIST_TRADE_WORKER_SHARDS,
            trade_batch_size = PERSIST_TRADE_BATCH_SIZE,
            trade_coalesce_ms = PERSIST_TRADE_COALESCE_MS,
            non_trade_orderbook_shards = PERSIST_NON_TRADE_ORDERBOOK_WORKER_SHARDS,
            non_trade_other_shards = PERSIST_NON_TRADE_OTHER_WORKER_SHARDS,
            non_trade_batch_size = PERSIST_NON_TRADE_BATCH_SIZE,
            non_trade_coalesce_ms = PERSIST_NON_TRADE_COALESCE_MS,
            trade_prebatch_flush_ms = TRADE_PREBATCH_FLUSH_MS,
            trade_prebatch_max_events = TRADE_PREBATCH_MAX_EVENTS,
            orderbook_prebatch_flush_ms = ORDERBOOK_PREBATCH_FLUSH_MS,
            orderbook_prebatch_max_events = ORDERBOOK_PREBATCH_MAX_EVENTS,
            "async persist queue started"
        );

        let queue = Self {
            market,
            trade_senders: Arc::new(trade_senders),
            trade_prebatch_senders: Arc::new(trade_prebatch_senders),
            non_trade_orderbook_senders: Arc::new(non_trade_orderbook_senders),
            non_trade_orderbook_prebatch_senders: Arc::new(non_trade_orderbook_prebatch_senders),
            non_trade_other_senders: Arc::new(non_trade_other_senders),
            pending,
            trade_pending,
            non_trade_pending,
        };
        register_persist_queue(market, &queue);
        queue
    }

    pub async fn enqueue(&self, event: NormalizedMdEvent) -> Result<()> {
        // Trade lane owns trade + 1m kline inputs for minute trade/ohlc aggregation.
        let lane = classify_persist_lane(&event);
        let pending_now = self.pending.fetch_add(1, Ordering::Relaxed) + 1;
        if pending_now % PERSIST_PENDING_WARN_STEP == 0 {
            warn!(
                market = self.market,
                pending = pending_now,
                trade_pending = self.trade_pending.load(Ordering::Relaxed),
                non_trade_pending = self.non_trade_pending.load(Ordering::Relaxed),
                "async persist queue backlog high"
            );
        }

        let lane_pending = match lane {
            PersistLane::Trade => &self.trade_pending,
            PersistLane::NonTradeOrderbook | PersistLane::NonTradeOther => &self.non_trade_pending,
        };
        lane_pending.fetch_add(1, Ordering::Relaxed);

        if lane == PersistLane::Trade && is_raw_trade_msg(event.msg_type.as_str()) {
            enqueue_raw_single_for_parquet(&event);
            let shard_id = shard_for_trade_event(&event, self.trade_prebatch_senders.len());
            let pending_event = PendingTradeEvent {
                event,
                enqueued_at: Instant::now(),
            };
            if self.trade_prebatch_senders[shard_id]
                .send(pending_event)
                .is_err()
            {
                self.pending.fetch_sub(1, Ordering::Relaxed);
                lane_pending.fetch_sub(1, Ordering::Relaxed);
                return Err(anyhow!(
                    "async persist enqueue failed: trade prebatch channel closed"
                ));
            }
            return Ok(());
        }

        if lane == PersistLane::NonTradeOrderbook && is_raw_orderbook_msg(event.msg_type.as_str()) {
            enqueue_raw_single_for_parquet(&event);
            let shard_id = shard_for_non_trade_orderbook_event(
                &event,
                self.non_trade_orderbook_prebatch_senders.len(),
            );
            let pending_event = PendingOrderbookEvent {
                event,
                enqueued_at: Instant::now(),
            };
            if self.non_trade_orderbook_prebatch_senders[shard_id]
                .send(pending_event)
                .is_err()
            {
                self.pending.fetch_sub(1, Ordering::Relaxed);
                lane_pending.fetch_sub(1, Ordering::Relaxed);
                return Err(anyhow!(
                    "async persist enqueue failed: orderbook prebatch channel closed"
                ));
            }
            return Ok(());
        }

        let (senders, lane_name, shard_id) = match lane {
            PersistLane::Trade => (
                &self.trade_senders,
                "trade",
                shard_for_trade_event(&event, self.trade_senders.len()),
            ),
            PersistLane::NonTradeOrderbook => (
                // md.agg.orderbook.1s events are already pre-aggregated and should
                // bypass raw orderbook prebatch workers, but they still must land on
                // the same downstream orderbook shard so snapshots and 1s chunks share
                // one canonical minute aggregator.
                &self.non_trade_orderbook_senders,
                "non_trade_orderbook_agg",
                shard_for_non_trade_orderbook_event(&event, self.non_trade_orderbook_senders.len()),
            ),
            PersistLane::NonTradeOther => (
                &self.non_trade_other_senders,
                "non_trade_other",
                shard_for_non_trade_other_event(&event, self.non_trade_other_senders.len()),
            ),
        };
        let checkpoint_update = PendingCheckpoint::from_event(&event);
        let job = PersistJob {
            events: vec![event],
            source_count: 1,
            checkpoint_updates: vec![checkpoint_update],
            enqueued_at: Instant::now(),
        };

        // Bounded channels are intentionally lossless now: enqueue awaits capacity
        // instead of dropping events when workers are congested.
        if senders[shard_id].send(job).await.is_err() {
            self.pending.fetch_sub(1, Ordering::Relaxed);
            lane_pending.fetch_sub(1, Ordering::Relaxed);
            return Err(anyhow!(
                "async persist enqueue failed: shard channel closed lane={} shard={}",
                lane_name,
                shard_id
            ));
        }
        Ok(())
    }

    pub fn pending(&self) -> usize {
        self.pending.load(Ordering::Relaxed)
    }

    pub fn trade_pending(&self) -> usize {
        self.trade_pending.load(Ordering::Relaxed)
    }

    pub fn non_trade_pending(&self) -> usize {
        self.non_trade_pending.load(Ordering::Relaxed)
    }
}

impl AsyncCheckpointQueue {
    fn spawn(market: &'static str, ops_writer: Arc<OpsDbWriter>) -> Self {
        let (tx, rx) = mpsc::channel::<PendingCheckpoint>(CHECKPOINT_CHANNEL_CAPACITY);
        let pending = Arc::new(AtomicUsize::new(0));
        let pending_cloned = Arc::clone(&pending);

        tokio::spawn(async move {
            run_checkpoint_worker(market, rx, pending_cloned, ops_writer).await;
        });

        info!(
            market = market,
            flush_interval_ms = CHECKPOINT_FLUSH_INTERVAL_MS,
            flush_size = CHECKPOINT_BUFFER_FLUSH_SIZE,
            channel_capacity = CHECKPOINT_CHANNEL_CAPACITY,
            "async checkpoint queue started"
        );

        Self {
            market,
            sender: tx,
            pending,
        }
    }

    async fn enqueue(
        &self,
        checkpoint: PendingCheckpoint,
    ) -> std::result::Result<(), PendingCheckpoint> {
        let pending_now = self.pending.fetch_add(1, Ordering::Relaxed) + 1;
        if pending_now % CHECKPOINT_PENDING_WARN_STEP == 0 {
            warn!(
                market = self.market,
                pending = pending_now,
                "async checkpoint queue backlog high"
            );
        }

        if let Err(err) = self.sender.send(checkpoint).await {
            self.pending.fetch_sub(1, Ordering::Relaxed);
            return Err(err.0);
        }

        Ok(())
    }
}

async fn run_checkpoint_worker(
    market: &'static str,
    mut receiver: mpsc::Receiver<PendingCheckpoint>,
    pending: Arc<AtomicUsize>,
    ops_writer: Arc<OpsDbWriter>,
) {
    let mut buffer: HashMap<String, PendingCheckpoint> = HashMap::new();
    let mut ticker = tokio::time::interval(Duration::from_millis(CHECKPOINT_FLUSH_INTERVAL_MS));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            maybe_cp = receiver.recv() => {
                let Some(cp) = maybe_cp else {
                    break;
                };
                pending.fetch_sub(1, Ordering::Relaxed);
                let key = cp.key();
                let replace = buffer
                    .get(&key)
                    .map(|existing| cp.last_event_ts >= existing.last_event_ts)
                    .unwrap_or(true);
                if replace {
                    buffer.insert(key, cp);
                }

                if buffer.len() >= CHECKPOINT_BUFFER_FLUSH_SIZE {
                    flush_checkpoint_buffer(market, &ops_writer, &mut buffer).await;
                }
            }
            _ = ticker.tick() => {
                if !buffer.is_empty() {
                    flush_checkpoint_buffer(market, &ops_writer, &mut buffer).await;
                }
            }
        }
    }

    if !buffer.is_empty() {
        flush_checkpoint_buffer(market, &ops_writer, &mut buffer).await;
    }

    warn!(market = market, "async checkpoint queue worker stopped");
}

async fn flush_checkpoint_buffer(
    market: &'static str,
    ops_writer: &Arc<OpsDbWriter>,
    buffer: &mut HashMap<String, PendingCheckpoint>,
) {
    if buffer.is_empty() {
        return;
    }

    let checkpoints: Vec<PendingCheckpoint> = buffer.drain().map(|(_, cp)| cp).collect();
    let checkpoint_count = checkpoints.len();
    let flush_started_at = Instant::now();

    let checkpoint_rows: Vec<CheckpointUpsert> = checkpoints
        .iter()
        .map(|cp| CheckpointUpsert {
            market: cp.market.clone(),
            symbol: cp.symbol.clone(),
            stream_name: cp.stream_name.clone(),
            source_kind: cp.source_kind.clone(),
            last_event_ts: cp.last_event_ts,
            last_recv_ts: Utc::now(),
            last_update_id: cp.checkpoint.last_update_id,
            last_trade_id: cp.checkpoint.last_trade_id,
            last_agg_trade_id: cp.checkpoint.last_agg_trade_id,
            checkpoint_json: cp.checkpoint.checkpoint_json.clone(),
        })
        .collect();

    if let Err(err) = ops_writer.upsert_checkpoints_batch(&checkpoint_rows).await {
        warn!(
            error = %err,
            market = market,
            checkpoint_count = checkpoint_count,
            "upsert checkpoint batch failed, fallback to single upsert"
        );
        for cp in &checkpoints {
            if let Err(err) = upsert_pending_checkpoint(ops_writer, cp).await {
                warn!(
                    error = %err,
                    market = %cp.market,
                    symbol = %cp.symbol,
                    stream_name = %cp.stream_name,
                    "upsert checkpoint failed"
                );
            }
        }
    }

    let flush_ms = flush_started_at.elapsed().as_millis();
    if flush_ms >= CHECKPOINT_FLUSH_SLOW_WARN_MS {
        warn!(
            market = market,
            checkpoint_count = checkpoint_count,
            flush_ms = flush_ms,
            "checkpoint flush slow path"
        );
    }
}

async fn run_worker(
    market: &'static str,
    lane: &'static str,
    shard_id: usize,
    mut receiver: mpsc::Receiver<PersistJob>,
    pending_total: Arc<AtomicUsize>,
    lane_pending: Arc<AtomicUsize>,
    batch_size: usize,
    coalesce_ms: u64,
    _coalesce_non_trade_for_db: bool,
    db_writer: Arc<MdDbWriter>,
    publisher: Arc<MqPublisher>,
    outbox_writer: Arc<OutboxWriter>,
    ops_writer: Arc<OpsDbWriter>,
    checkpoint_queue: AsyncCheckpointQueue,
    metrics: Arc<AppMetrics>,
) {
    let mut batch = Vec::with_capacity(batch_size);
    let mut batch_event_count: usize = 0;
    let mut flush_tick = tokio::time::interval(Duration::from_millis(MINUTE_AGG_FLUSH_INTERVAL_MS));
    flush_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let agg_lane = if lane == "trade" {
        AggregateLane::Trade
    } else {
        AggregateLane::NonTrade
    };
    let mut minute_aggregator = MinuteBatchAggregator::new(agg_lane, DEFAULT_WHALE_THRESHOLD_USDT);

    loop {
        tokio::select! {
            maybe = receiver.recv() => {
                let Some(first) = maybe else {
                    break;
                };
                batch_event_count = batch_event_count.saturating_add(first.source_count);
                batch.push(first);

                let deadline = Instant::now() + Duration::from_millis(coalesce_ms);
                while batch_event_count < batch_size {
                    let remain = deadline.saturating_duration_since(Instant::now());
                    if remain.is_zero() {
                        break;
                    }
                    match timeout(remain, receiver.recv()).await {
                        Ok(Some(next)) => {
                            batch_event_count = batch_event_count.saturating_add(next.source_count);
                            batch.push(next);
                        }
                        Ok(None) => break,
                        Err(_) => break,
                    }
                }

                while batch_event_count < batch_size {
                    match receiver.try_recv() {
                        Ok(next) => {
                            batch_event_count = batch_event_count.saturating_add(next.source_count);
                            batch.push(next);
                        }
                        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
                    }
                }

                for job in &batch {
                    emit_lag_log(market, shard_id, job);
                }

                if let Err(err) = persist_events_batch(
                    &batch,
                    &db_writer,
                    &publisher,
                    &outbox_writer,
                    &ops_writer,
                    &checkpoint_queue,
                    &mut minute_aggregator,
                    &metrics,
                )
                .await
                {
                    let error_chain = format_error_chain(&err);
                    warn!(
                        market = market,
                        lane = lane,
                        shard = shard_id,
                        batch_size = batch_event_count,
                        error = %error_chain,
                        "async persist batch failed, fallback to single-event persist"
                    );
                    for job in &batch {
                        let queue_wait_ms = job.enqueued_at.elapsed().as_millis();
                        for event in &job.events {
                            if let Err(single_err) = persist_event_internal(
                                event,
                                &db_writer,
                                &publisher,
                                &outbox_writer,
                                &ops_writer,
                                &metrics,
                                false,
                            )
                            .await
                            {
                                let error_chain = format_error_chain(&single_err);
                                error!(
                                    market = market,
                                    lane = lane,
                                    shard = shard_id,
                                    msg_type = %event.msg_type,
                                    routing_key = %event.routing_key,
                                    stream_name = %event.stream_name,
                                    queue_wait_ms = queue_wait_ms,
                                    error = %error_chain,
                                    "async persist event failed"
                                );
                            }
                        }
                    }
                }

                pending_total.fetch_sub(batch_event_count, Ordering::Relaxed);
                lane_pending.fetch_sub(batch_event_count, Ordering::Relaxed);
                batch.clear();
                batch_event_count = 0;
            }
            _ = flush_tick.tick() => {
                if let Err(err) = flush_minute_aggregator_events(
                    market,
                    lane,
                    shard_id,
                    &mut minute_aggregator,
                    &db_writer,
                    &publisher,
                    &outbox_writer,
                    &metrics,
                    Utc::now(),
                )
                .await
                {
                    warn!(
                        market = market,
                        lane = lane,
                        shard = shard_id,
                        error = %format_error_chain(&err),
                        "async minute aggregator flush failed"
                    );
                }
            }
        }
    }

    warn!(
        market = market,
        lane = lane,
        shard = shard_id,
        "async persist worker stopped"
    );
}

async fn run_trade_prebatch_worker(
    market: &'static str,
    shard_id: usize,
    mut prebatch_rx: mpsc::UnboundedReceiver<PendingTradeEvent>,
    worker_tx: mpsc::Sender<PersistJob>,
    pending_total: Arc<AtomicUsize>,
    lane_pending: Arc<AtomicUsize>,
) {
    let mut tick = tokio::time::interval(Duration::from_millis(TRADE_PREBATCH_FLUSH_MS));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut preagg_states: HashMap<(String, String), TradeVpinState> = HashMap::new();
    let mut current_chunks: HashMap<(String, String), TradeSecondChunk> = HashMap::new();

    loop {
        tokio::select! {
            maybe = prebatch_rx.recv() => {
                let Some(item) = maybe else {
                    break;
                };
                let key = (item.event.market.clone(), item.event.symbol.clone());
                let event_second_ms =
                    floor_timestamp_millis(item.event.event_ts.timestamp_millis(), 1_000);

                if current_chunks
                    .get(&key)
                    .map(|chunk| chunk.second_ms != event_second_ms)
                    .unwrap_or(false)
                {
                    if let Some(chunk) = current_chunks.remove(&key) {
                        dispatch_trade_second_chunk(
                            market,
                            shard_id,
                            &worker_tx,
                            &pending_total,
                            &lane_pending,
                            chunk,
                        )
                        .await;
                    }
                }

                if !current_chunks.contains_key(&key) {
                    current_chunks.insert(key.clone(), TradeSecondChunk::new(&item.event, item.enqueued_at));
                }

                {
                    let preagg_state = preagg_states.entry(key.clone()).or_default();
                    let chunk = current_chunks
                        .get_mut(&key)
                        .expect("chunk inserted above");
                    apply_trade_raw_event(&item.event, chunk, preagg_state);
                    if chunk.source_event_count as usize >= TRADE_PREBATCH_MAX_EVENTS {
                        if let Some(chunk) = current_chunks.remove(&key) {
                            dispatch_trade_second_chunk(
                                market,
                                shard_id,
                                &worker_tx,
                                &pending_total,
                                &lane_pending,
                                chunk,
                            )
                            .await;
                        }
                    }
                }
            }
            _ = tick.tick() => {
                let now_second_ms = floor_timestamp_millis(Utc::now().timestamp_millis(), 1_000);
                let flush_keys: Vec<(String, String)> = current_chunks
                    .iter()
                    .filter_map(|(key, chunk)| {
                        if chunk.second_ms < now_second_ms {
                            Some(key.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                for key in flush_keys {
                    if let Some(chunk) = current_chunks.remove(&key) {
                        dispatch_trade_second_chunk(
                            market,
                            shard_id,
                            &worker_tx,
                            &pending_total,
                            &lane_pending,
                            chunk,
                        )
                        .await;
                    }
                }
            }
        }
    }

    let tail_chunks: Vec<TradeSecondChunk> = current_chunks.into_values().collect();
    for chunk in tail_chunks {
        dispatch_trade_second_chunk(
            market,
            shard_id,
            &worker_tx,
            &pending_total,
            &lane_pending,
            chunk,
        )
        .await;
    }

    warn!(
        market = market,
        shard = shard_id,
        "trade prebatch worker stopped"
    );
}

async fn run_orderbook_prebatch_worker(
    market: &'static str,
    shard_id: usize,
    mut prebatch_rx: mpsc::UnboundedReceiver<PendingOrderbookEvent>,
    worker_tx: mpsc::Sender<PersistJob>,
    pending_total: Arc<AtomicUsize>,
    lane_pending: Arc<AtomicUsize>,
) {
    let mut tick = tokio::time::interval(Duration::from_millis(ORDERBOOK_PREBATCH_FLUSH_MS));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut preagg_states: HashMap<(String, String), OrderbookPreAggState> = HashMap::new();
    let mut current_chunks: HashMap<(String, String), OrderbookSecondChunk> = HashMap::new();

    loop {
        tokio::select! {
            maybe = prebatch_rx.recv() => {
                let Some(item) = maybe else {
                    break;
                };
                let key = (item.event.market.clone(), item.event.symbol.clone());
                let event_second_ms =
                    floor_timestamp_millis(item.event.event_ts.timestamp_millis(), 1_000);
                let preagg_state = preagg_states.entry(key.clone()).or_default();

                if current_chunks
                    .get(&key)
                    .map(|chunk| chunk.second_ms != event_second_ms)
                    .unwrap_or(false)
                {
                    if let Some(chunk) = current_chunks.remove(&key) {
                        dispatch_orderbook_second_chunk(
                            market,
                            shard_id,
                            &worker_tx,
                            &pending_total,
                            &lane_pending,
                            chunk,
                        )
                        .await;
                    }
                }

                if !current_chunks.contains_key(&key) {
                    let mut seeded_chunk = OrderbookSecondChunk::new(&item.event, item.enqueued_at);
                    // Seed full current book state at second-boundary start so unchanged levels
                    // still contribute heatmap weighted liquidity for this second.
                    for (tick, qty) in &preagg_state.bids {
                        seeded_chunk.update_bid_qty(*tick, *qty);
                    }
                    for (tick, qty) in &preagg_state.asks {
                        seeded_chunk.update_ask_qty(*tick, *qty);
                    }
                    current_chunks.insert(key.clone(), seeded_chunk);
                }

                {
                    let chunk = current_chunks
                        .get_mut(&key)
                        .expect("chunk inserted above");
                    chunk.touch(&item.event);
                    apply_orderbook_raw_event(&item.event, preagg_state, chunk);
                    if chunk.source_event_count as usize >= ORDERBOOK_PREBATCH_MAX_EVENTS {
                        if let Some(chunk) = current_chunks.remove(&key) {
                            dispatch_orderbook_second_chunk(
                                market,
                                shard_id,
                                &worker_tx,
                                &pending_total,
                                &lane_pending,
                                chunk,
                            )
                            .await;
                        }
                    }
                }
            }
            _ = tick.tick() => {
                let now_second_ms = floor_timestamp_millis(Utc::now().timestamp_millis(), 1_000);
                let flush_keys: Vec<(String, String)> = current_chunks
                    .iter()
                    .filter_map(|(key, chunk)| {
                        if chunk.second_ms < now_second_ms {
                            Some(key.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                for key in flush_keys {
                    if let Some(chunk) = current_chunks.remove(&key) {
                        dispatch_orderbook_second_chunk(
                            market,
                            shard_id,
                            &worker_tx,
                            &pending_total,
                            &lane_pending,
                            chunk,
                        )
                        .await;
                    }
                }
            }
        }
    }

    let tail_chunks: Vec<OrderbookSecondChunk> = current_chunks.into_values().collect();
    for chunk in tail_chunks {
        dispatch_orderbook_second_chunk(
            market,
            shard_id,
            &worker_tx,
            &pending_total,
            &lane_pending,
            chunk,
        )
        .await;
    }

    warn!(
        market = market,
        shard = shard_id,
        "orderbook prebatch worker stopped"
    );
}

fn apply_trade_raw_event(
    event: &NormalizedMdEvent,
    chunk: &mut TradeSecondChunk,
    preagg_state: &mut TradeVpinState,
) {
    if event.msg_type != "md.trade" {
        return;
    }
    let Some(price) = value_f64(&event.data, "price") else {
        return;
    };
    let qty_base = value_f64(&event.data, "qty_base")
        .or_else(|| value_f64(&event.data, "qty_eth"))
        .or_else(|| value_f64(&event.data, "qty_raw"))
        .unwrap_or(0.0);
    let notional_usdt = value_f64(&event.data, "notional_usdt").unwrap_or(price * qty_base);
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
        qty_base,
        notional_usdt,
        aggressor_side,
        DEFAULT_WHALE_THRESHOLD_USDT,
    );
    if aggressor_side >= 0 {
        preagg_state.ingest_flow(qty_base, 0.0);
    } else {
        preagg_state.ingest_flow(0.0, qty_base);
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

async fn dispatch_trade_second_chunk(
    market: &'static str,
    shard_id: usize,
    worker_tx: &mpsc::Sender<PersistJob>,
    pending_total: &Arc<AtomicUsize>,
    lane_pending: &Arc<AtomicUsize>,
    chunk: TradeSecondChunk,
) {
    let source_count = chunk.source_event_count.max(0) as usize;
    if source_count == 0 {
        return;
    }
    let enqueued_at = chunk.first_enqueued_at;
    let (agg_event_1s, checkpoint_updates) = chunk.finalize_to_event();

    if let Err(_err) = worker_tx
        .send(PersistJob {
            events: vec![agg_event_1s],
            source_count,
            checkpoint_updates,
            enqueued_at,
        })
        .await
    {
        pending_total.fetch_sub(source_count, Ordering::Relaxed);
        lane_pending.fetch_sub(source_count, Ordering::Relaxed);
        warn!(
            market = market,
            shard = shard_id,
            dropped_count = source_count,
            "trade 1s preagg flush failed: downstream worker channel closed"
        );
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

async fn dispatch_orderbook_second_chunk(
    market: &'static str,
    shard_id: usize,
    worker_tx: &mpsc::Sender<PersistJob>,
    pending_total: &Arc<AtomicUsize>,
    lane_pending: &Arc<AtomicUsize>,
    chunk: OrderbookSecondChunk,
) {
    let source_count = chunk.source_event_count.max(0) as usize;
    if source_count == 0 {
        return;
    }
    let enqueued_at = chunk.first_enqueued_at;
    let (agg_event_1s, checkpoint_updates) = chunk.finalize_to_event();

    if let Err(_err) = worker_tx
        .send(PersistJob {
            events: vec![agg_event_1s],
            source_count,
            checkpoint_updates,
            enqueued_at,
        })
        .await
    {
        pending_total.fetch_sub(source_count, Ordering::Relaxed);
        lane_pending.fetch_sub(source_count, Ordering::Relaxed);
        warn!(
            market = market,
            shard = shard_id,
            dropped_count = source_count,
            "orderbook 1s preagg flush failed: downstream worker channel closed"
        );
        return;
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

fn classify_persist_lane(event: &NormalizedMdEvent) -> PersistLane {
    if is_trade_lane_msg(event.msg_type.as_str()) {
        return PersistLane::Trade;
    }
    if is_orderbook_lane_msg(event.msg_type.as_str()) {
        return PersistLane::NonTradeOrderbook;
    }
    PersistLane::NonTradeOther
}

fn is_trade_lane_msg(msg_type: &str) -> bool {
    matches!(msg_type, "md.trade" | "md.kline" | "md.agg.trade.1s")
}

fn is_orderbook_lane_msg(msg_type: &str) -> bool {
    matches!(msg_type, "md.agg.orderbook.1s") || is_raw_orderbook_msg(msg_type)
}

fn is_raw_trade_msg(msg_type: &str) -> bool {
    msg_type == "md.trade"
}

fn is_raw_orderbook_msg(msg_type: &str) -> bool {
    matches!(msg_type, "md.depth" | "md.bbo" | "md.orderbook_snapshot_l2")
}

fn shard_for_non_trade_orderbook_event(event: &NormalizedMdEvent, shard_count: usize) -> usize {
    if shard_count <= 1 {
        return 0;
    }
    // Keep orderbook-heavy data for the same market+symbol on one shard so
    // orderbook minute aggregation remains complete.
    let mut hasher = DefaultHasher::new();
    event.market.hash(&mut hasher);
    event.symbol.hash(&mut hasher);
    (hasher.finish() as usize) % shard_count
}

fn shard_for_non_trade_other_event(event: &NormalizedMdEvent, shard_count: usize) -> usize {
    if shard_count <= 1 {
        return 0;
    }
    // Keep non-orderbook non-trade data for the same market+symbol on one shard so
    // funding/liquidation minute aggregation remains complete.
    let mut hasher = DefaultHasher::new();
    event.market.hash(&mut hasher);
    event.symbol.hash(&mut hasher);
    (hasher.finish() as usize) % shard_count
}

fn shard_for_trade_event(event: &NormalizedMdEvent, shard_count: usize) -> usize {
    if shard_count <= 1 {
        return 0;
    }
    // Keep deterministic trade ordering by symbol+market for #1 indicator path.
    let mut hasher = DefaultHasher::new();
    event.market.hash(&mut hasher);
    event.symbol.hash(&mut hasher);
    (hasher.finish() as usize) % shard_count
}

fn emit_lag_log(market: &'static str, shard_id: usize, job: &PersistJob) {
    let Some(oldest_event) = job.events.iter().min_by_key(|e| e.event_ts) else {
        return;
    };
    // Backfill/replay events are historical by definition; avoid noisy false-positive
    // lag alerts while catch-up is in progress.
    if oldest_event.backfill_in_progress {
        return;
    }
    let queue_wait_ms = job.enqueued_at.elapsed().as_millis();
    let event_lag_secs = (Utc::now() - oldest_event.event_ts).num_seconds();
    if event_lag_secs > PERSIST_EVENT_LAG_WARN_SECS {
        if oldest_event.msg_type == "md.trade" {
            error!(
                market = market,
                shard = shard_id,
                msg_type = %oldest_event.msg_type,
                routing_key = %oldest_event.routing_key,
                stream_name = %oldest_event.stream_name,
                event_lag_secs = event_lag_secs,
                queue_wait_ms = queue_wait_ms,
                "trade persist lag over 10s"
            );
        } else {
            warn!(
                market = market,
                shard = shard_id,
                msg_type = %oldest_event.msg_type,
                routing_key = %oldest_event.routing_key,
                stream_name = %oldest_event.stream_name,
                event_lag_secs = event_lag_secs,
                queue_wait_ms = queue_wait_ms,
                "persist lag over 10s"
            );
        }
    }
}

async fn persist_events_batch(
    jobs: &[PersistJob],
    db_writer: &Arc<MdDbWriter>,
    publisher: &Arc<MqPublisher>,
    outbox_writer: &Arc<OutboxWriter>,
    ops_writer: &Arc<OpsDbWriter>,
    checkpoint_queue: &AsyncCheckpointQueue,
    minute_aggregator: &mut MinuteBatchAggregator,
    metrics: &Arc<AppMetrics>,
) -> Result<()> {
    if jobs.is_empty() {
        return Ok(());
    }

    let persist_started_at = Instant::now();
    let total_event_count: usize = jobs.iter().map(|j| j.source_count).sum();
    enqueue_raw_jobs_for_parquet(jobs);
    let pipeline_events =
        minute_aggregator.aggregate_batch_refs(jobs.iter().flat_map(|j| j.events.iter()));
    let mut db_events = pipeline_events.clone();
    db_events.extend(collect_db_passthrough_klines(
        jobs.iter().flat_map(|job| job.events.iter()),
    ));

    let outbox_stage_started_at = Instant::now();
    let publish_results = stream::iter(pipeline_events.into_iter())
        .map(|event| {
            let publisher = Arc::clone(publisher);
            let outbox_writer = Arc::clone(outbox_writer);
            let metrics = Arc::clone(metrics);
            async move {
                let event_ts = event.event_ts;
                let (direct_delta, outbox_delta) =
                    publish_pipeline_event(event, publisher, outbox_writer, metrics).await;
                (direct_delta, outbox_delta, event_ts)
            }
        })
        .buffer_unordered(HOT_PATH_DIRECT_MQ_PUBLISH_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
    let mut hot_direct_count: usize = 0;
    let mut outbox_count: usize = 0;
    for (direct_delta, outbox_delta, event_ts) in publish_results {
        hot_direct_count = hot_direct_count.saturating_add(direct_delta);
        outbox_count = outbox_count.saturating_add(outbox_delta);
        metrics.inc_processed(event_ts);
    }
    let outbox_enqueue_ms = outbox_stage_started_at.elapsed().as_millis();

    // Publish path is decoupled from DB path: even when DB is slow, indicator MQ
    // messages are already emitted in real time.
    let db_stage_started_at = Instant::now();
    let mut fallback_single_write_failures: usize = 0;
    if let Err(err) = db_writer.write_md_events_batch(&db_events).await {
        metrics.inc_db_error();
        let error_chain = format_error_chain(&err);
        warn!(
            error = %error_chain,
            batch_size = db_events.len(),
            "batch db write failed, fallback to single-event db writes"
        );
        for db_event in &db_events {
            if let Err(single_err) = db_writer.write_md_event(db_event).await {
                metrics.inc_db_error();
                fallback_single_write_failures = fallback_single_write_failures.saturating_add(1);
                let error_chain = format_error_chain(&single_err);
                error!(
                    error = %error_chain,
                    msg_type = %db_event.msg_type,
                    routing_key = %db_event.routing_key,
                    "single-event db write failed"
                );
            }
        }
    }
    let db_write_ms = db_stage_started_at.elapsed().as_millis();
    if hot_direct_count > 0 && fallback_single_write_failures > 0 {
        error!(
            hot_direct_count = hot_direct_count,
            db_failures = fallback_single_write_failures,
            agg_batch_size = db_events.len(),
            "mq_pg_fork_detected: mq already emitted but some db writes still failed"
        );
    }

    let checkpoint_stage_started_at = Instant::now();
    let mut latest_checkpoints: HashMap<String, PendingCheckpoint> = HashMap::new();
    for job in jobs {
        for cp in &job.checkpoint_updates {
            let cp = cp.clone();
            let key = cp.key();
            let replace = latest_checkpoints
                .get(&key)
                .map(|existing| cp.last_event_ts >= existing.last_event_ts)
                .unwrap_or(true);
            if replace {
                latest_checkpoints.insert(key, cp);
            }
        }
    }

    let checkpoint_count = latest_checkpoints.len();
    for cp in latest_checkpoints.into_values() {
        if let Err(failed_cp) = checkpoint_queue.enqueue(cp).await {
            warn!(
                market = %failed_cp.market,
                symbol = %failed_cp.symbol,
                stream_name = %failed_cp.stream_name,
                "async checkpoint enqueue failed, fallback to direct upsert"
            );
            if let Err(err) = upsert_pending_checkpoint(ops_writer, &failed_cp).await {
                let error_chain = format_error_chain(&err);
                warn!(
                    error = %error_chain,
                    market = %failed_cp.market,
                    symbol = %failed_cp.symbol,
                    stream_name = %failed_cp.stream_name,
                    "upsert checkpoint failed"
                );
            }
        }
    }
    let checkpoint_enqueue_ms = checkpoint_stage_started_at.elapsed().as_millis();

    let total_ms = persist_started_at.elapsed().as_millis();
    if total_ms >= PERSIST_BATCH_SLOW_WARN_MS {
        warn!(
            batch_size = total_event_count,
            agg_batch_size = db_events.len(),
            db_batch_size = db_events.len(),
            hot_direct_count = hot_direct_count,
            outbox_count = outbox_count,
            checkpoint_count = checkpoint_count,
            db_write_ms = db_write_ms,
            mq_emit_ms = outbox_enqueue_ms,
            outbox_enqueue_ms = outbox_enqueue_ms,
            checkpoint_enqueue_ms = checkpoint_enqueue_ms,
            total_ms = total_ms,
            "persist batch slow path"
        );
    }

    Ok(())
}

async fn flush_minute_aggregator_events(
    market: &str,
    lane: &str,
    shard_id: usize,
    minute_aggregator: &mut MinuteBatchAggregator,
    db_writer: &Arc<MdDbWriter>,
    publisher: &Arc<MqPublisher>,
    outbox_writer: &Arc<OutboxWriter>,
    metrics: &Arc<AppMetrics>,
    watermark_ts: chrono::DateTime<Utc>,
) -> Result<()> {
    let pipeline_events = minute_aggregator.flush_closed_minutes(watermark_ts);
    if pipeline_events.is_empty() {
        return Ok(());
    }

    persist_pipeline_events_only(&pipeline_events, db_writer, publisher, outbox_writer, metrics)
        .await
        .map_err(|err| {
            anyhow!(
                "flush minute aggregator events failed market={market} lane={lane} shard={shard_id}: {err:#}"
            )
        })
}

async fn persist_pipeline_events_only(
    pipeline_events: &[NormalizedMdEvent],
    db_writer: &Arc<MdDbWriter>,
    publisher: &Arc<MqPublisher>,
    outbox_writer: &Arc<OutboxWriter>,
    metrics: &Arc<AppMetrics>,
) -> Result<()> {
    if pipeline_events.is_empty() {
        return Ok(());
    }

    let publish_results = stream::iter(pipeline_events.iter().cloned())
        .map(|event| {
            let publisher = Arc::clone(publisher);
            let outbox_writer = Arc::clone(outbox_writer);
            let metrics = Arc::clone(metrics);
            async move {
                let event_ts = event.event_ts;
                let (direct_delta, outbox_delta) =
                    publish_pipeline_event(event, publisher, outbox_writer, metrics).await;
                (direct_delta, outbox_delta, event_ts)
            }
        })
        .buffer_unordered(HOT_PATH_DIRECT_MQ_PUBLISH_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;

    for (_, _, event_ts) in publish_results {
        metrics.inc_processed(event_ts);
    }

    if let Err(err) = db_writer.write_md_events_batch(pipeline_events).await {
        metrics.inc_db_error();
        warn!(
            error = %format_error_chain(&err),
            batch_size = pipeline_events.len(),
            "minute aggregator db write failed, fallback to single-event db writes"
        );
        for db_event in pipeline_events {
            if let Err(single_err) = db_writer.write_md_event(db_event).await {
                metrics.inc_db_error();
                return Err(anyhow!(
                    "single-event db write failed msg_type={} routing_key={} err={:#}",
                    db_event.msg_type,
                    db_event.routing_key,
                    single_err
                ));
            }
        }
    }

    Ok(())
}

fn coalesce_non_trade_events_for_db(events: &[NormalizedMdEvent]) -> Vec<NormalizedMdEvent> {
    let mut passthrough = Vec::new();
    let mut depth_windows: HashMap<WindowKey, DepthWindowState> = HashMap::new();
    let mut bbo_windows: HashMap<BboWindowKey, BboWindowState> = HashMap::new();

    for event in events {
        match event.msg_type.as_str() {
            "md.depth" => {
                let key = WindowKey::from_event(event);
                if let Some(state) = depth_windows.get_mut(&key) {
                    state.update(event);
                } else {
                    depth_windows.insert(key, DepthWindowState::new(event));
                }
            }
            "md.bbo" => {
                let key = BboWindowKey::from_event(event);
                if let Some(state) = bbo_windows.get_mut(&key) {
                    state.update(event);
                } else {
                    bbo_windows.insert(key, BboWindowState::new(event));
                }
            }
            _ => passthrough.push(event.clone()),
        }
    }

    let mut coalesced = Vec::with_capacity(
        passthrough
            .len()
            .saturating_add(depth_windows.len())
            .saturating_add(bbo_windows.len()),
    );
    coalesced.extend(passthrough);
    coalesced.extend(
        depth_windows
            .into_values()
            .map(DepthWindowState::into_event),
    );
    coalesced.extend(bbo_windows.into_values().map(BboWindowState::into_event));
    coalesced
}

fn collect_db_passthrough_klines<'a, I>(events: I) -> Vec<NormalizedMdEvent>
where
    I: IntoIterator<Item = &'a NormalizedMdEvent>,
{
    events
        .into_iter()
        .filter(|event| event.msg_type == "md.kline")
        .cloned()
        .collect()
}

fn upsert_payload_window_agg(data_obj: &mut JsonMap<String, Value>, window_agg: Value) {
    let payload_entry = data_obj
        .entry("payload_json".to_string())
        .or_insert_with(|| Value::Object(JsonMap::new()));
    if !payload_entry.is_object() {
        *payload_entry = Value::Object(JsonMap::new());
    }
    if let Some(payload_obj) = payload_entry.as_object_mut() {
        payload_obj.insert("window_agg".to_string(), window_agg);
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

fn depth_level_update_count(data: &Value) -> usize {
    let bid_updates = data
        .get("bids_delta")
        .and_then(Value::as_array)
        .map(|v| v.len())
        .unwrap_or(0);
    let ask_updates = data
        .get("asks_delta")
        .and_then(Value::as_array)
        .map(|v| v.len())
        .unwrap_or(0);
    bid_updates.saturating_add(ask_updates)
}

fn bbo_spread_mid(data: &Value) -> (f64, f64) {
    let bid = value_f64(data, "bid_price").unwrap_or(0.0);
    let ask = value_f64(data, "ask_price").unwrap_or(bid);
    let spread = (ask - bid).max(0.0);
    let mid = (ask + bid) / 2.0;
    (spread, mid)
}

fn floor_timestamp_millis(ts_ms: i64, window_ms: i64) -> i64 {
    ts_ms - ts_ms.rem_euclid(window_ms)
}

fn millis_to_utc(ts_ms: i64) -> chrono::DateTime<Utc> {
    chrono::DateTime::<Utc>::from_timestamp_millis(ts_ms).unwrap_or_else(|| Utc::now())
}

fn spread_min_rounded(v: f64) -> f64 {
    (v * 1_000_000.0).round() / 1_000_000.0
}

fn format_error_chain(err: &anyhow::Error) -> String {
    format!("{err:#}")
}

#[derive(Debug, Clone)]
struct PendingCheckpoint {
    market: String,
    symbol: String,
    stream_name: String,
    source_kind: String,
    last_event_ts: chrono::DateTime<Utc>,
    checkpoint: checkpoints::CheckpointUpdate,
}

impl PendingCheckpoint {
    fn from_event(event: &NormalizedMdEvent) -> Self {
        Self {
            market: event.market.clone(),
            symbol: event.symbol.clone(),
            stream_name: event.stream_name.clone(),
            source_kind: event.source_kind.clone(),
            last_event_ts: event.event_ts,
            checkpoint: checkpoints::build_checkpoint(event),
        }
    }

    fn key(&self) -> String {
        format!(
            "{}|{}|{}|{}",
            self.market, self.symbol, self.stream_name, self.source_kind
        )
    }
}

async fn upsert_pending_checkpoint(
    ops_writer: &Arc<OpsDbWriter>,
    cp: &PendingCheckpoint,
) -> Result<()> {
    ops_writer
        .upsert_checkpoint(
            &cp.market,
            &cp.symbol,
            &cp.stream_name,
            &cp.source_kind,
            cp.last_event_ts,
            Utc::now(),
            cp.checkpoint.last_update_id,
            cp.checkpoint.last_trade_id,
            cp.checkpoint.last_agg_trade_id,
            cp.checkpoint.checkpoint_json.clone(),
        )
        .await
}

fn ensure_direct_minute_flusher(
    db_writer: Arc<MdDbWriter>,
    publisher: Arc<MqPublisher>,
    outbox_writer: Arc<OutboxWriter>,
    metrics: Arc<AppMetrics>,
) {
    DIRECT_MINUTE_FLUSHER_STARTED.get_or_init(|| {
        tokio::spawn(async move {
            run_direct_minute_flusher(db_writer, publisher, outbox_writer, metrics).await;
        });
    });
}

async fn run_direct_minute_flusher(
    db_writer: Arc<MdDbWriter>,
    publisher: Arc<MqPublisher>,
    outbox_writer: Arc<OutboxWriter>,
    metrics: Arc<AppMetrics>,
) {
    let mut tick = tokio::time::interval(Duration::from_millis(MINUTE_AGG_FLUSH_INTERVAL_MS));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tick.tick().await;
        let pipeline_events = {
            let registry = DIRECT_MINUTE_AGGREGATORS.get_or_init(|| StdMutex::new(HashMap::new()));
            let mut aggregators = registry.lock().expect("lock direct minute aggregators");
            let mut out = Vec::new();
            for aggregator in aggregators.values_mut() {
                out.extend(aggregator.flush_closed_minutes(Utc::now()));
            }
            out
        };

        if let Err(err) = persist_pipeline_events_only(
            &pipeline_events,
            &db_writer,
            &publisher,
            &outbox_writer,
            &metrics,
        )
        .await
        {
            warn!(
                error = %format_error_chain(&err),
                batch_size = pipeline_events.len(),
                "direct minute flusher persist failed"
            );
        }
    }
}

pub async fn persist_event(
    event: &NormalizedMdEvent,
    db_writer: &Arc<MdDbWriter>,
    publisher: &Arc<MqPublisher>,
    outbox_writer: &Arc<OutboxWriter>,
    ops_writer: &Arc<OpsDbWriter>,
    metrics: &Arc<AppMetrics>,
) -> Result<()> {
    persist_event_internal(
        event,
        db_writer,
        publisher,
        outbox_writer,
        ops_writer,
        metrics,
        true,
    )
    .await
}

async fn persist_event_internal(
    event: &NormalizedMdEvent,
    db_writer: &Arc<MdDbWriter>,
    publisher: &Arc<MqPublisher>,
    outbox_writer: &Arc<OutboxWriter>,
    ops_writer: &Arc<OpsDbWriter>,
    metrics: &Arc<AppMetrics>,
    write_raw_to_parquet: bool,
) -> Result<()> {
    let persist_started_at = Instant::now();
    ensure_direct_minute_flusher(
        Arc::clone(db_writer),
        Arc::clone(publisher),
        Arc::clone(outbox_writer),
        Arc::clone(metrics),
    );
    if write_raw_to_parquet {
        enqueue_raw_single_for_parquet(event);
    }
    let agg_lane = if matches!(event.msg_type.as_str(), "md.trade" | "md.kline") {
        AggregateLane::Trade
    } else {
        AggregateLane::NonTrade
    };
    let pipeline_events = {
        let registry = DIRECT_MINUTE_AGGREGATORS.get_or_init(|| StdMutex::new(HashMap::new()));
        let mut aggregators = registry.lock().expect("lock direct minute aggregators");
        let minute_aggregator = aggregators
            .entry(agg_lane)
            .or_insert_with(|| MinuteBatchAggregator::new(agg_lane, DEFAULT_WHALE_THRESHOLD_USDT));
        let mut pipeline_events = minute_aggregator.aggregate_batch(std::slice::from_ref(event));
        pipeline_events.extend(minute_aggregator.flush_closed_minutes(Utc::now()));
        pipeline_events
    };
    let mut db_events = pipeline_events.clone();
    db_events.extend(collect_db_passthrough_klines(std::iter::once(event)));

    let outbox_stage_started_at = Instant::now();
    let mut hot_direct_count: usize = 0;
    let mut outbox_count: usize = 0;
    for agg_event in pipeline_events.iter().cloned() {
        let (direct_delta, outbox_delta) = publish_pipeline_event(
            agg_event,
            Arc::clone(publisher),
            Arc::clone(outbox_writer),
            Arc::clone(metrics),
        )
        .await;
        hot_direct_count = hot_direct_count.saturating_add(direct_delta);
        outbox_count = outbox_count.saturating_add(outbox_delta);
    }
    let outbox_enqueue_ms = outbox_stage_started_at.elapsed().as_millis();
    for agg_event in &pipeline_events {
        metrics.inc_processed(agg_event.event_ts);
    }

    let db_stage_started_at = Instant::now();
    if let Err(err) = db_writer.write_md_events_batch(&db_events).await {
        metrics.inc_db_error();
        return Err(err);
    }
    let db_write_ms = db_stage_started_at.elapsed().as_millis();

    let checkpoint_stage_started_at = Instant::now();
    let cp = checkpoints::build_checkpoint(event);
    if let Err(err) = ops_writer
        .upsert_checkpoint(
            &event.market,
            &event.symbol,
            &event.stream_name,
            &event.source_kind,
            event.event_ts,
            Utc::now(),
            cp.last_update_id,
            cp.last_trade_id,
            cp.last_agg_trade_id,
            cp.checkpoint_json,
        )
        .await
    {
        let error_chain = format_error_chain(&err);
        warn!(
            error = %error_chain,
            routing_key = %event.routing_key,
            "upsert checkpoint failed"
        );
    }
    let checkpoint_upsert_ms = checkpoint_stage_started_at.elapsed().as_millis();
    let total_ms = persist_started_at.elapsed().as_millis();

    if total_ms >= PERSIST_SLOW_WARN_MS {
        warn!(
            market = %event.market,
            symbol = %event.symbol,
            msg_type = %event.msg_type,
            routing_key = %event.routing_key,
            hot_direct_count = hot_direct_count,
            outbox_count = outbox_count,
            db_write_ms = db_write_ms,
            mq_emit_ms = outbox_enqueue_ms,
            outbox_enqueue_ms = outbox_enqueue_ms,
            checkpoint_upsert_ms = checkpoint_upsert_ms,
            total_ms = total_ms,
            "persist_event slow path"
        );
    }

    Ok(())
}

fn enqueue_raw_jobs_for_parquet(jobs: &[PersistJob]) {
    let Some(tx) = RAW_PARQUET_TX.get() else {
        return;
    };
    if jobs.is_empty() {
        return;
    }

    let batch_size: usize = jobs
        .iter()
        .flat_map(|j| j.events.iter())
        .filter(|event| !event.msg_type.starts_with("md.agg."))
        .count();
    if batch_size == 0 {
        return;
    }
    match tx.try_reserve() {
        Ok(permit) => {
            // Clone only after we confirm queue capacity, so full-queue fast path
            // does not spend CPU/memory cloning large raw batches.
            let mut events = Vec::with_capacity(batch_size);
            for job in jobs {
                for event in &job.events {
                    if !event.msg_type.starts_with("md.agg.") {
                        events.push(event.clone());
                    }
                }
            }
            permit.send(events);
        }
        Err(TrySendError::Full(_)) => {
            warn!(
                batch_size = batch_size,
                "raw parquet queue full - dropping cold-store batch"
            );
        }
        Err(TrySendError::Closed(_)) => {
            warn!("raw parquet queue closed - dropping cold-store batch");
        }
    }
}

fn enqueue_raw_batch_for_parquet(events: &[NormalizedMdEvent]) {
    let Some(tx) = RAW_PARQUET_TX.get() else {
        return;
    };
    if events.is_empty() {
        return;
    }

    let batch_size = events.len();
    match tx.try_reserve() {
        Ok(permit) => {
            permit.send(events.to_vec());
        }
        Err(TrySendError::Full(_)) => {
            warn!(
                batch_size = batch_size,
                "raw parquet queue full - dropping cold-store batch"
            );
        }
        Err(TrySendError::Closed(_)) => {
            warn!("raw parquet queue closed - dropping cold-store batch");
        }
    }
}

fn enqueue_raw_single_for_parquet(event: &NormalizedMdEvent) {
    enqueue_raw_batch_for_parquet(std::slice::from_ref(event));
}

async fn run_raw_parquet_worker(
    mut rx: mpsc::Receiver<Vec<NormalizedMdEvent>>,
    sink: Arc<ParquetSink>,
) {
    let mut tick = tokio::time::interval(Duration::from_millis(RAW_PARQUET_FLUSH_INTERVAL_MS));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut partition_buffers: HashMap<(String, String, String), Vec<NormalizedMdEvent>> =
        HashMap::new();
    let mut buffered_rows: usize = 0;

    loop {
        tokio::select! {
            maybe_batch = rx.recv() => {
                let Some(batch) = maybe_batch else {
                    break;
                };
                if batch.is_empty() {
                    continue;
                }

                for event in batch {
                    let key = (
                        event.event_ts.format("%Y-%m-%d").to_string(),
                        event.market.to_lowercase(),
                        event.symbol.to_uppercase(),
                    );
                    let partition = partition_buffers.entry(key).or_default();
                    partition.push(event);
                    buffered_rows += 1;
                }

                if buffered_rows >= RAW_PARQUET_GLOBAL_MAX_BUFFERED_ROWS {
                    flush_raw_parquet_partitions(&sink, &mut partition_buffers, &mut buffered_rows).await;
                    continue;
                }

                let mut flush_keys = Vec::new();
                for (key, rows) in &partition_buffers {
                    if rows.len() >= RAW_PARQUET_PARTITION_FLUSH_ROWS {
                        flush_keys.push(key.clone());
                    }
                }
                if !flush_keys.is_empty() {
                    flush_raw_parquet_keys(&sink, &mut partition_buffers, &flush_keys, &mut buffered_rows).await;
                }
            }
            _ = tick.tick() => {
                if buffered_rows > 0 {
                    flush_raw_parquet_partitions(&sink, &mut partition_buffers, &mut buffered_rows).await;
                }
            }
        }
    }

    if buffered_rows > 0 {
        flush_raw_parquet_partitions(&sink, &mut partition_buffers, &mut buffered_rows).await;
    }
    warn!("raw parquet worker stopped");
}

async fn flush_raw_parquet_partitions(
    sink: &Arc<ParquetSink>,
    partition_buffers: &mut HashMap<(String, String, String), Vec<NormalizedMdEvent>>,
    buffered_rows: &mut usize,
) {
    let keys: Vec<(String, String, String)> = partition_buffers.keys().cloned().collect();
    flush_raw_parquet_keys(sink, partition_buffers, &keys, buffered_rows).await;
}

async fn flush_raw_parquet_keys(
    sink: &Arc<ParquetSink>,
    partition_buffers: &mut HashMap<(String, String, String), Vec<NormalizedMdEvent>>,
    keys: &[(String, String, String)],
    buffered_rows: &mut usize,
) {
    for key in keys {
        if let Some(mut rows) = partition_buffers.remove(key) {
            if rows.is_empty() {
                continue;
            }
            let batch_len = rows.len();
            if let Err(err) = sink.write_batch(&rows).await {
                warn!(
                    error = %err,
                    batch_size = batch_len,
                    date = %key.0,
                    market = %key.1,
                    symbol = %key.2,
                    "raw parquet worker write failed"
                );
            }
            rows.clear();
            *buffered_rows = buffered_rows.saturating_sub(batch_len);
        }
    }
}

async fn publish_pipeline_event(
    event: NormalizedMdEvent,
    publisher: Arc<MqPublisher>,
    outbox_writer: Arc<OutboxWriter>,
    metrics: Arc<AppMetrics>,
) -> (usize, usize) {
    if HOT_PATH_DIRECT_MQ_ENABLED && is_hot_path_md_event(&event) {
        match publisher.publish_md_event(&event).await {
            Ok(()) => return (1, 0),
            Err(err) => {
                metrics.inc_mq_error();
                warn!(
                    error = %err,
                    msg_type = %event.msg_type,
                    routing_key = %event.routing_key,
                    market = %event.market,
                    symbol = %event.symbol,
                    "direct mq publish failed, fallback to outbox enqueue"
                );
            }
        }
    }

    match enqueue_event_to_outbox(&event, publisher, outbox_writer).await {
        Ok(count) => (0, count),
        Err(err) => {
            metrics.inc_mq_error();
            error!(
                error = %err,
                msg_type = %event.msg_type,
                routing_key = %event.routing_key,
                market = %event.market,
                symbol = %event.symbol,
                "publish path failed: outbox enqueue failed"
            );
            (0, 0)
        }
    }
}

async fn enqueue_event_to_outbox(
    event: &NormalizedMdEvent,
    publisher: Arc<MqPublisher>,
    outbox_writer: Arc<OutboxWriter>,
) -> Result<usize> {
    let outbox_records = publisher.build_outbox_records_for_pipeline(event)?;
    let outbox_count = outbox_records.len();
    for outbox in outbox_records {
        outbox_writer
            .enqueue_without_ack(
                &outbox.exchange_name,
                &outbox.routing_key,
                outbox.message_id,
                outbox.schema_version,
                outbox.headers_json,
                outbox.payload_json,
            )
            .await?;
    }
    Ok(outbox_count)
}

fn is_hot_path_md_event(event: &NormalizedMdEvent) -> bool {
    matches!(
        event.msg_type.as_str(),
        "md.agg.trade.1m" | "md.agg.orderbook.1m" | "md.agg.liq.1m" | "md.agg.funding_mark.1m"
    )
}

#[cfg(test)]
mod tests {
    use super::{
        apply_trade_raw_event, collect_db_passthrough_klines, TradeSecondChunk, TradeVpinState,
    };
    use crate::normalize::NormalizedMdEvent;
    use chrono::{TimeZone, Utc};
    use serde_json::json;
    use tokio::time::Instant;

    #[test]
    fn trade_second_chunk_emits_vpin_snapshot() {
        let ts = Utc.with_ymd_and_hms(2026, 3, 6, 0, 0, 5).single().unwrap();
        let event = NormalizedMdEvent {
            msg_type: "md.trade".to_string(),
            market: "futures".to_string(),
            symbol: "TESTUSDT".to_string(),
            source_kind: "ws".to_string(),
            backfill_in_progress: false,
            routing_key: "md.futures.trade.testusdt".to_string(),
            stream_name: "trade".to_string(),
            event_ts: ts,
            data: json!({
                "price": 100.0,
                "qty_eth": 1.0,
                "notional_usdt": 100.0,
                "aggressor_side": 1
            }),
        };

        let mut chunk = TradeSecondChunk::new(&event, Instant::now());
        let mut vpin_state = TradeVpinState::default();
        apply_trade_raw_event(&event, &mut chunk, &mut vpin_state);
        let (agg_event, _) = chunk.finalize_to_event();

        assert_eq!(agg_event.msg_type, "md.agg.trade.1s");
        assert!(agg_event.data["payload_json"]["vpin_state"].is_object());
        assert_eq!(
            agg_event.data["payload_json"]["vpin_state"]["current_fill"].as_f64(),
            Some(1.0)
        );
    }

    #[test]
    fn db_path_preserves_raw_kline_events() {
        let ts = Utc.with_ymd_and_hms(2026, 3, 9, 0, 0, 0).single().unwrap();
        let kline = NormalizedMdEvent {
            msg_type: "md.kline".to_string(),
            market: "futures".to_string(),
            symbol: "TESTUSDT".to_string(),
            source_kind: "ws".to_string(),
            backfill_in_progress: false,
            routing_key: "md.futures.kline.1h.testusdt".to_string(),
            stream_name: "testusdt@kline_1h".to_string(),
            event_ts: ts,
            data: json!({
                "interval_code": "1h",
                "is_closed": true,
                "open_time": ts.to_rfc3339(),
                "close_time": (ts + chrono::Duration::hours(1)).to_rfc3339(),
                "open_price": "100.0",
                "high_price": "101.0",
                "low_price": "99.0",
                "close_price": "100.5"
            }),
        };
        let trade = NormalizedMdEvent {
            msg_type: "md.trade".to_string(),
            market: "futures".to_string(),
            symbol: "TESTUSDT".to_string(),
            source_kind: "ws".to_string(),
            backfill_in_progress: false,
            routing_key: "md.futures.trade.testusdt".to_string(),
            stream_name: "trade".to_string(),
            event_ts: ts,
            data: json!({}),
        };

        let passthrough = collect_db_passthrough_klines([&trade, &kline]);
        assert_eq!(passthrough.len(), 1);
        assert_eq!(passthrough[0].msg_type, "md.kline");
        assert_eq!(passthrough[0].data["interval_code"].as_str(), Some("1h"));
    }
}
