use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use indicator_engine::app::bootstrap::{build_db_pool, load_config};
use indicator_engine::app::runtime::{
    build_indicator_runtime_options, fetch_backfill_batch, load_kline_history_supplement,
    replay_row_to_engine_event, BackfillCursor,
};
use indicator_engine::indicators::context::{
    IndicatorContext, IndicatorRuntimeOptions, IndicatorSnapshotRow,
};
use indicator_engine::indicators::indicator_trait::Indicator;
use indicator_engine::indicators::registry::build_registry;
use indicator_engine::ingest::decoder::{
    decode_contract_body, AggFundingMark1mEvent, AggFundingPoint, AggHeatmapLevel, AggLiq1mEvent,
    AggLiqLevel, AggMarkPoint, AggOrderbook1mEvent, AggProfileLevel, AggTrade1mEvent,
    AggVpinSnapshot, AggWhaleStats, EngineEvent, MarketKind, MdData,
};
use indicator_engine::runtime::state_store::{
    BookLevelAgg, LevelAgg, LiqAgg, StateStore, WindowBundle,
};
use indicator_engine::runtime::window_scheduler::WindowScheduler;
use market_data_ingestor::aggregate::minute_agg::{AggregateLane, MinuteBatchAggregator};
use market_data_ingestor::normalize::NormalizedMdEvent;
use market_data_ingestor::pipelines::ws_preagg::WsOneSecondPreAggregator;
use serde::Deserialize;
use serde_json::Value;
use sqlx::PgPool;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use uuid::Uuid;

const DEFAULT_CONFIG_PATH: &str = "config/config.yaml";
const DEFAULT_BATCH_SIZE: i64 = 5_000;
const FLOAT_EPSILON: f64 = 1e-7;
const PRICE_SCALE: f64 = 100.0;
const CHUNK_SPILLOVER_GRACE_SECS: i64 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ValidationSource {
    Db,
    Raw,
}

#[derive(Debug)]
struct CliArgs {
    config_path: String,
    symbol: Option<String>,
    market: String,
    from_ts: DateTime<Utc>,
    to_ts: DateTime<Utc>,
    batch_size: i64,
    source: ValidationSource,
    raw_root: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawColdRow {
    event_ts_ms: i64,
    msg_type: String,
    market: String,
    symbol: String,
    source_kind: String,
    stream_name: String,
    routing_key: String,
    #[serde(default)]
    backfill_in_progress: bool,
    data: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MinuteMarketKey {
    ts_bucket: DateTime<Utc>,
    market: MarketKind,
}

impl PartialOrd for MinuteMarketKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MinuteMarketKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ts_bucket
            .cmp(&other.ts_bucket)
            .then_with(|| self.market.as_str().cmp(other.market.as_str()))
    }
}

#[derive(Debug, Default)]
struct CanonicalReplayAccumulator {
    minutes: BTreeMap<MinuteMarketKey, MinuteReplayAccumulator>,
    vpin_states: HashMap<MarketKind, ReplayVpinState>,
}

#[derive(Debug, Default)]
struct MinuteReplayAccumulator {
    trade: Option<TradeAgg>,
    orderbook: Option<OrderbookAgg>,
    liq: Option<LiqAggAcc>,
    funding_mark: Option<FundingMarkAgg>,
}

#[derive(Debug, Default)]
struct TradeAgg {
    source_event_count: i64,
    trade_count: i64,
    buy_qty: f64,
    sell_qty: f64,
    buy_notional: f64,
    sell_notional: f64,
    first_chunk_start_ts: Option<DateTime<Utc>>,
    first_price: Option<f64>,
    last_chunk_end_ts: Option<DateTime<Utc>>,
    last_price: Option<f64>,
    high_price: Option<f64>,
    low_price: Option<f64>,
    profile: BTreeMap<i64, LevelAgg>,
    whale: WhaleAcc,
    vpin_snapshot: Option<AggVpinSnapshot>,
}

#[derive(Debug, Default)]
struct WhaleAcc {
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

#[derive(Debug, Default)]
struct OrderbookAgg {
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
    last_chunk_end_ts: Option<DateTime<Utc>>,
    obi_k_dw_close: Option<f64>,
    heatmap: BTreeMap<i64, BookLevelAgg>,
}

#[derive(Debug, Default)]
struct LiqAggAcc {
    source_event_count: i64,
    levels: BTreeMap<i64, LiqAgg>,
}

#[derive(Debug, Default)]
struct FundingMarkAgg {
    source_event_count: i64,
    mark_points: Vec<AggMarkPoint>,
    funding_points: Vec<AggFundingPoint>,
}

#[derive(Debug, Clone)]
struct ReplayVpinState {
    bucket_size: f64,
    rolling_bucket_count: usize,
    current_buy: f64,
    current_sell: f64,
    current_fill: f64,
    imbalances: Vec<f64>,
    imbalance_sum: f64,
    last_vpin: f64,
}

impl Default for ReplayVpinState {
    fn default() -> Self {
        Self {
            bucket_size: 50.0,
            rolling_bucket_count: 50,
            current_buy: 0.0,
            current_sell: 0.0,
            current_fill: 0.0,
            imbalances: Vec::new(),
            imbalance_sum: 0.0,
            last_vpin: 0.0,
        }
    }
}

async fn run_db_validation(
    args: &CliArgs,
    pool: &PgPool,
    _default_symbol: &str,
    symbol: &str,
    registry: &[Arc<dyn Indicator>],
    runtime_options: &IndicatorRuntimeOptions,
    whale_threshold_usdt: f64,
    watermark_lateness_secs: i64,
) -> Result<()> {
    let fetch_from_ts = floor_minute(args.from_ts) - ChronoDuration::minutes(1);
    let compare_from_minute = ceil_minute(args.from_ts);

    let mut old_state = StateStore::new(symbol.to_string(), whale_threshold_usdt);
    let mut new_state = StateStore::new(symbol.to_string(), whale_threshold_usdt);
    let mut old_scheduler = WindowScheduler::new(watermark_lateness_secs);
    let mut new_scheduler = WindowScheduler::new(watermark_lateness_secs);
    let mut old_ready = VecDeque::new();
    let mut new_ready = VecDeque::new();
    let mut canonical_acc = CanonicalReplayAccumulator::default();
    let mut cursor: Option<BackfillCursor> = None;
    let mut old_max_seen_event_ts: Option<DateTime<Utc>> = None;
    let mut new_max_seen_event_ts: Option<DateTime<Utc>> = None;
    let mut minutes_validated: usize = 0;

    loop {
        let rows = fetch_backfill_batch(
            pool,
            fetch_from_ts,
            args.to_ts,
            symbol,
            &args.market,
            args.batch_size,
            cursor.as_ref(),
        )
        .await?;
        if rows.is_empty() {
            break;
        }

        for row in rows {
            cursor = Some(BackfillCursor {
                event_ts: row.event_ts,
                msg_type: row.msg_type.clone(),
                market: row.market.clone(),
                symbol: row.symbol.clone(),
                routing_key: row.routing_key.clone(),
            });

            let event = replay_row_to_engine_event(row)?;
            old_max_seen_event_ts = Some(match old_max_seen_event_ts {
                Some(prev) => prev.max(event.event_ts),
                None => event.event_ts,
            });
            canonical_acc.ingest(&event)?;
            old_state.ingest(event.clone());

            let ready_before_bucket =
                floor_minute(event.event_ts - ChronoDuration::seconds(CHUNK_SPILLOVER_GRACE_SECS));
            let canonical_events = canonical_acc.flush_before(ready_before_bucket, symbol);
            ingest_new_events(
                &mut new_state,
                &mut new_scheduler,
                &mut new_ready,
                &mut new_max_seen_event_ts,
                canonical_events,
            );

            let watermark_ts = old_max_seen_event_ts.expect("just set");
            old_ready.extend(old_scheduler.ready_minutes(watermark_ts));
            minutes_validated = compare_ready_minutes(
                pool,
                registry,
                runtime_options,
                symbol,
                compare_from_minute,
                &mut old_state,
                &mut new_state,
                &mut old_ready,
                &mut new_ready,
                minutes_validated,
            )
            .await?;
        }
    }

    let old_tail_watermark =
        old_max_seen_event_ts.map(|ts| ts + ChronoDuration::seconds(watermark_lateness_secs + 61));
    let canonical_tail_events = canonical_acc.flush_before(
        old_tail_watermark.unwrap_or_else(|| Utc::now() + ChronoDuration::minutes(10)),
        symbol,
    );
    ingest_new_events(
        &mut new_state,
        &mut new_scheduler,
        &mut new_ready,
        &mut new_max_seen_event_ts,
        canonical_tail_events,
    );

    if let Some(watermark_ts) = old_tail_watermark {
        old_ready.extend(old_scheduler.ready_minutes(watermark_ts));
    }
    if let Some(watermark_ts) =
        new_max_seen_event_ts.map(|ts| ts + ChronoDuration::seconds(watermark_lateness_secs + 61))
    {
        new_ready.extend(new_scheduler.ready_minutes(watermark_ts));
    }

    minutes_validated = compare_ready_minutes(
        pool,
        registry,
        runtime_options,
        symbol,
        compare_from_minute,
        &mut old_state,
        &mut new_state,
        &mut old_ready,
        &mut new_ready,
        minutes_validated,
    )
    .await?;

    if !old_ready.is_empty() || !new_ready.is_empty() {
        bail!(
            "scheduler tails diverged old_remaining={:?} new_remaining={:?}",
            old_ready,
            new_ready
        );
    }

    println!(
        "validated {} minute windows for symbol={} market={} range=[{}, {}) source=db",
        minutes_validated, symbol, args.market, args.from_ts, args.to_ts
    );
    Ok(())
}

async fn run_raw_validation(
    args: &CliArgs,
    pool: &PgPool,
    symbol: &str,
    registry: &[Arc<dyn Indicator>],
    runtime_options: &IndicatorRuntimeOptions,
    whale_threshold_usdt: f64,
    watermark_lateness_secs: i64,
) -> Result<()> {
    let raw_root = args
        .raw_root
        .as_deref()
        .ok_or_else(|| anyhow!("--raw-root is required when --source raw"))?;
    let load_from_ts = floor_minute(args.from_ts) - ChronoDuration::minutes(1);
    let compare_from_minute = ceil_minute(args.from_ts);
    let raw_events = load_raw_events(raw_root, symbol, &args.market, load_from_ts, args.to_ts)?;

    if raw_events.is_empty() {
        bail!(
            "no raw events found under {} for symbol={} market={} range=[{}, {})",
            raw_root,
            symbol,
            args.market,
            load_from_ts,
            args.to_ts
        );
    }

    let raw_trade_count = raw_events
        .iter()
        .filter(|event| event.msg_type == "md.trade")
        .count();
    let raw_orderbook_count = raw_events
        .iter()
        .filter(|event| {
            matches!(
                event.msg_type.as_str(),
                "md.depth" | "md.bbo" | "md.orderbook_snapshot_l2"
            )
        })
        .count();
    if raw_trade_count == 0 || raw_orderbook_count == 0 {
        bail!(
            "raw cold-store coverage incomplete for source=raw validation: md.trade={} orderbook_raw={} range=[{}, {})",
            raw_trade_count,
            raw_orderbook_count,
            load_from_ts,
            args.to_ts
        );
    }

    let mut old_state = StateStore::new(symbol.to_string(), whale_threshold_usdt);
    let mut new_state = StateStore::new(symbol.to_string(), whale_threshold_usdt);
    let mut old_scheduler = WindowScheduler::new(watermark_lateness_secs);
    let mut new_scheduler = WindowScheduler::new(watermark_lateness_secs);
    let mut old_ready = VecDeque::new();
    let mut new_ready = VecDeque::new();
    let mut ws_preagg = WsOneSecondPreAggregator::new(whale_threshold_usdt);
    let mut trade_agg = MinuteBatchAggregator::new(AggregateLane::Trade, whale_threshold_usdt);
    let mut non_trade_agg =
        MinuteBatchAggregator::new(AggregateLane::NonTrade, whale_threshold_usdt);
    let mut old_max_seen_event_ts: Option<DateTime<Utc>> = None;
    let mut new_max_seen_event_ts: Option<DateTime<Utc>> = None;
    let mut minutes_validated: usize = 0;

    for raw_event in raw_events {
        let raw_event_ts = raw_event.event_ts;
        let event = decode_normalized_event(&raw_event)?;
        old_max_seen_event_ts = Some(match old_max_seen_event_ts {
            Some(prev) => prev.max(event.event_ts),
            None => event.event_ts,
        });
        old_state.ingest(event.clone());

        let mut pipeline_events = ws_preagg.flush_expired(raw_event_ts);
        pipeline_events.extend(ws_preagg.ingest_raw_event(raw_event));
        sort_normalized_events(&mut pipeline_events);

        let mut canonical_events = Vec::new();
        for pipeline_event in pipeline_events {
            let mut produced = if matches!(
                pipeline_event.msg_type.as_str(),
                "md.trade" | "md.kline" | "md.agg.trade.1s"
            ) {
                trade_agg.aggregate_batch_refs(std::iter::once(&pipeline_event))
            } else {
                non_trade_agg.aggregate_batch_refs(std::iter::once(&pipeline_event))
            };
            canonical_events.append(&mut produced);
        }
        canonical_events.extend(trade_agg.flush_closed_minutes(raw_event_ts));
        canonical_events.extend(non_trade_agg.flush_closed_minutes(raw_event_ts));
        sort_normalized_events(&mut canonical_events);
        let canonical_engine_events = canonical_events
            .iter()
            .map(decode_normalized_event)
            .collect::<Result<Vec<_>>>()?;
        ingest_new_events(
            &mut new_state,
            &mut new_scheduler,
            &mut new_ready,
            &mut new_max_seen_event_ts,
            canonical_engine_events,
        );

        let watermark_ts = old_max_seen_event_ts.expect("just set");
        old_ready.extend(old_scheduler.ready_minutes(watermark_ts));
        minutes_validated = compare_ready_minutes(
            pool,
            registry,
            runtime_options,
            symbol,
            compare_from_minute,
            &mut old_state,
            &mut new_state,
            &mut old_ready,
            &mut new_ready,
            minutes_validated,
        )
        .await?;
    }

    let flush_watermark = old_max_seen_event_ts
        .map(|ts| ts + ChronoDuration::seconds(watermark_lateness_secs + 61))
        .unwrap_or_else(|| args.to_ts + ChronoDuration::minutes(10));
    let mut pipeline_tail_events = ws_preagg.flush_all();
    sort_normalized_events(&mut pipeline_tail_events);
    let mut canonical_tail_events = Vec::new();
    for pipeline_event in pipeline_tail_events {
        let mut produced = if matches!(
            pipeline_event.msg_type.as_str(),
            "md.trade" | "md.kline" | "md.agg.trade.1s"
        ) {
            trade_agg.aggregate_batch_refs(std::iter::once(&pipeline_event))
        } else {
            non_trade_agg.aggregate_batch_refs(std::iter::once(&pipeline_event))
        };
        canonical_tail_events.append(&mut produced);
    }
    canonical_tail_events.extend(trade_agg.flush_closed_minutes(flush_watermark));
    canonical_tail_events.extend(non_trade_agg.flush_closed_minutes(flush_watermark));
    sort_normalized_events(&mut canonical_tail_events);
    let canonical_tail_engine_events = canonical_tail_events
        .iter()
        .map(decode_normalized_event)
        .collect::<Result<Vec<_>>>()?;
    ingest_new_events(
        &mut new_state,
        &mut new_scheduler,
        &mut new_ready,
        &mut new_max_seen_event_ts,
        canonical_tail_engine_events,
    );

    if let Some(watermark_ts) =
        old_max_seen_event_ts.map(|ts| ts + ChronoDuration::seconds(watermark_lateness_secs + 61))
    {
        old_ready.extend(old_scheduler.ready_minutes(watermark_ts));
    }
    if let Some(watermark_ts) =
        new_max_seen_event_ts.map(|ts| ts + ChronoDuration::seconds(watermark_lateness_secs + 61))
    {
        new_ready.extend(new_scheduler.ready_minutes(watermark_ts));
    }

    minutes_validated = compare_ready_minutes(
        pool,
        registry,
        runtime_options,
        symbol,
        compare_from_minute,
        &mut old_state,
        &mut new_state,
        &mut old_ready,
        &mut new_ready,
        minutes_validated,
    )
    .await?;

    if !old_ready.is_empty() || !new_ready.is_empty() {
        bail!(
            "scheduler tails diverged old_remaining={:?} new_remaining={:?}",
            old_ready,
            new_ready
        );
    }

    println!(
        "validated {} minute windows for symbol={} market={} range=[{}, {}) source=raw root={}",
        minutes_validated, symbol, args.market, args.from_ts, args.to_ts, raw_root
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    indicator_engine::observability::tracing::init();

    let args = parse_args()?;
    let config = load_config(&args.config_path)
        .with_context(|| format!("load config from {}", args.config_path))?;
    let symbol = args
        .symbol
        .clone()
        .unwrap_or_else(|| config.indicator.symbol.clone())
        .to_uppercase();
    let pool = build_db_pool(&config).await?;
    let runtime_options = build_indicator_runtime_options(&config);
    let registry = build_registry();
    match args.source {
        ValidationSource::Db => {
            run_db_validation(
                &args,
                &pool,
                &config.indicator.symbol,
                &symbol,
                &registry,
                &runtime_options,
                config.indicator.whale_threshold_usdt,
                config.indicator.watermark_lateness_secs,
            )
            .await
        }
        ValidationSource::Raw => {
            run_raw_validation(
                &args,
                &pool,
                &symbol,
                &registry,
                &runtime_options,
                config.indicator.whale_threshold_usdt,
                config.indicator.watermark_lateness_secs,
            )
            .await
        }
    }
}

impl CanonicalReplayAccumulator {
    fn ingest(&mut self, event: &EngineEvent) -> Result<()> {
        let key = MinuteMarketKey {
            ts_bucket: logical_bucket_ts(event),
            market: event.market,
        };
        let entry = self.minutes.entry(key).or_default();
        match &event.data {
            MdData::AggTrade1m(trade) => {
                let trade_acc = entry.trade.get_or_insert_with(TradeAgg::default);
                trade_acc.apply(trade);
                let vpin_state = self
                    .vpin_states
                    .entry(event.market)
                    .or_insert_with(ReplayVpinState::default);
                if let Some(snapshot) = &trade.vpin_snapshot {
                    vpin_state.restore(snapshot);
                } else {
                    vpin_state.ingest_flow(trade.buy_qty, trade.sell_qty);
                }
                trade_acc.vpin_snapshot = Some(vpin_state.snapshot());
            }
            MdData::AggOrderbook1m(orderbook) => entry
                .orderbook
                .get_or_insert_with(OrderbookAgg::default)
                .apply(orderbook),
            MdData::AggLiq1m(liq) => entry.liq.get_or_insert_with(LiqAggAcc::default).apply(liq),
            MdData::AggFundingMark1m(funding_mark) => entry
                .funding_mark
                .get_or_insert_with(FundingMarkAgg::default)
                .apply(funding_mark),
            other => bail!(
                "unsupported replay msg for canonical validator: {:?}",
                other
            ),
        }
        Ok(())
    }

    fn flush_before(&mut self, ready_before: DateTime<Utc>, symbol: &str) -> Vec<EngineEvent> {
        let ready_keys = self
            .minutes
            .keys()
            .copied()
            .filter(|key| key.ts_bucket < ready_before)
            .collect::<Vec<_>>();
        let mut out = Vec::new();
        for key in ready_keys {
            if let Some(acc) = self.minutes.remove(&key) {
                out.extend(acc.into_events(key.ts_bucket, key.market, symbol));
            }
        }
        out.sort_by(|a, b| {
            a.event_ts
                .cmp(&b.event_ts)
                .then_with(|| a.msg_type.cmp(&b.msg_type))
                .then_with(|| a.market.as_str().cmp(b.market.as_str()))
        });
        out
    }
}

impl ReplayVpinState {
    fn ingest_flow(&mut self, buy_qty: f64, sell_qty: f64) {
        let mut remain_buy = buy_qty.max(0.0);
        let mut remain_sell = sell_qty.max(0.0);

        while remain_buy + remain_sell > 1e-12 {
            if self.current_fill + 1e-12 >= self.bucket_size {
                self.close_bucket();
            }

            let remain_total = remain_buy + remain_sell;
            let capacity = (self.bucket_size - self.current_fill).max(0.0);
            if capacity <= 1e-12 {
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

            if self.current_fill + 1e-12 >= self.bucket_size {
                self.close_bucket();
            }
        }
    }

    fn close_bucket(&mut self) {
        if self.current_fill <= 1e-12 {
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
        if self.imbalances.len() >= self.rolling_bucket_count && self.bucket_size > 1e-12 {
            let denom = self.rolling_bucket_count as f64 * self.bucket_size;
            self.last_vpin = (self.imbalance_sum / denom).clamp(0.0, 1.0);
        } else {
            self.last_vpin = 0.0;
        }
        self.current_buy = 0.0;
        self.current_sell = 0.0;
        self.current_fill = 0.0;
    }

    fn snapshot(&self) -> AggVpinSnapshot {
        AggVpinSnapshot {
            current_buy: self.current_buy,
            current_sell: self.current_sell,
            current_fill: self.current_fill,
            imbalances: self.imbalances.clone(),
            imbalance_sum: self.imbalance_sum,
            last_vpin: self.last_vpin,
        }
    }

    fn restore(&mut self, snapshot: &AggVpinSnapshot) {
        self.current_buy = snapshot.current_buy;
        self.current_sell = snapshot.current_sell;
        self.current_fill = snapshot.current_fill;
        self.imbalances = snapshot.imbalances.clone();
        self.imbalance_sum = snapshot.imbalance_sum;
        self.last_vpin = snapshot.last_vpin;
    }
}

impl MinuteReplayAccumulator {
    fn into_events(
        self,
        ts_bucket: DateTime<Utc>,
        market: MarketKind,
        symbol: &str,
    ) -> Vec<EngineEvent> {
        let mut out = Vec::new();
        if let Some(trade) = self.trade {
            out.push(trade.into_event(ts_bucket, market, symbol));
        }
        if let Some(orderbook) = self.orderbook {
            out.push(orderbook.into_event(ts_bucket, market, symbol));
        }
        if let Some(liq) = self.liq {
            out.push(liq.into_event(ts_bucket, market, symbol));
        }
        if let Some(funding_mark) = self.funding_mark {
            out.push(funding_mark.into_event(ts_bucket, market, symbol));
        }
        out
    }
}

impl TradeAgg {
    fn apply(&mut self, event: &AggTrade1mEvent) {
        self.source_event_count += event.source_event_count.max(0);
        self.trade_count += event.trade_count.max(0);
        self.buy_qty += event.buy_qty;
        self.sell_qty += event.sell_qty;
        self.buy_notional += event.buy_notional;
        self.sell_notional += event.sell_notional;

        if self
            .first_chunk_start_ts
            .map(|ts| event.chunk_start_ts < ts)
            .unwrap_or(true)
        {
            self.first_chunk_start_ts = Some(event.chunk_start_ts);
            self.first_price = event.first_price;
        }
        if self
            .last_chunk_end_ts
            .map(|ts| event.chunk_end_ts >= ts)
            .unwrap_or(true)
        {
            self.last_chunk_end_ts = Some(event.chunk_end_ts);
            self.last_price = event.last_price;
        }
        if let Some(high_price) = event.high_price {
            self.high_price = Some(
                self.high_price
                    .map_or(high_price, |cur| cur.max(high_price)),
            );
        }
        if let Some(low_price) = event.low_price {
            self.low_price = Some(self.low_price.map_or(low_price, |cur| cur.min(low_price)));
        }
        for level in &event.profile_levels {
            let dst = self.profile.entry(price_to_tick(level.price)).or_default();
            dst.buy_qty += level.buy_qty;
            dst.sell_qty += level.sell_qty;
        }
        self.whale.trade_count += event.whale.trade_count.max(0);
        self.whale.buy_count += event.whale.buy_count.max(0);
        self.whale.sell_count += event.whale.sell_count.max(0);
        self.whale.notional_total += event.whale.notional_total;
        self.whale.notional_buy += event.whale.notional_buy;
        self.whale.notional_sell += event.whale.notional_sell;
        self.whale.qty_eth_total += event.whale.qty_eth_total;
        self.whale.qty_eth_buy += event.whale.qty_eth_buy;
        self.whale.qty_eth_sell += event.whale.qty_eth_sell;
        self.whale.max_single_notional = self
            .whale
            .max_single_notional
            .max(event.whale.max_single_notional);
    }

    fn into_event(self, ts_bucket: DateTime<Utc>, market: MarketKind, symbol: &str) -> EngineEvent {
        build_engine_event(
            ts_bucket,
            market,
            symbol,
            "md.agg.trade.1m",
            format!(
                "validator.canonical.{}.trade.1m.{}",
                market.as_str(),
                symbol.to_lowercase()
            ),
            MdData::AggTrade1m(AggTrade1mEvent {
                ts_bucket,
                chunk_start_ts: ts_bucket,
                chunk_end_ts: canonical_minute_end(ts_bucket),
                source_event_count: self.source_event_count,
                trade_count: self.trade_count,
                buy_qty: self.buy_qty,
                sell_qty: self.sell_qty,
                buy_notional: self.buy_notional,
                sell_notional: self.sell_notional,
                first_price: self.first_price,
                last_price: self.last_price,
                high_price: self.high_price,
                low_price: self.low_price,
                profile_levels: self
                    .profile
                    .into_iter()
                    .map(|(tick, level)| AggProfileLevel {
                        price: tick_to_price(tick),
                        buy_qty: level.buy_qty,
                        sell_qty: level.sell_qty,
                    })
                    .collect(),
                whale: AggWhaleStats {
                    trade_count: self.whale.trade_count,
                    buy_count: self.whale.buy_count,
                    sell_count: self.whale.sell_count,
                    notional_total: self.whale.notional_total,
                    notional_buy: self.whale.notional_buy,
                    notional_sell: self.whale.notional_sell,
                    qty_eth_total: self.whale.qty_eth_total,
                    qty_eth_buy: self.whale.qty_eth_buy,
                    qty_eth_sell: self.whale.qty_eth_sell,
                    max_single_notional: self.whale.max_single_notional,
                },
                vpin_snapshot: self.vpin_snapshot,
            }),
        )
    }
}

impl OrderbookAgg {
    fn apply(&mut self, event: &AggOrderbook1mEvent) {
        self.source_event_count += event.source_event_count.max(0);
        self.sample_count += event.sample_count.max(0);
        self.bbo_updates += event.bbo_updates.max(0);
        self.spread_sum += event.spread_sum;
        self.topk_depth_sum += event.topk_depth_sum;
        self.obi_sum += event.obi_sum;
        self.obi_l1_sum += event.obi_l1_sum;
        self.obi_k_sum += event.obi_k_sum;
        self.obi_k_dw_sum += event.obi_k_dw_sum;
        self.obi_k_dw_change_sum += event.obi_k_dw_change_sum;
        self.obi_k_dw_adj_sum += event.obi_k_dw_adj_sum;
        self.microprice_sum += event.microprice_sum;
        self.microprice_classic_sum += event.microprice_classic_sum;
        self.microprice_kappa_sum += event.microprice_kappa_sum;
        self.microprice_adj_sum += event.microprice_adj_sum;
        self.ofi_sum += event.ofi_sum;
        if self
            .last_chunk_end_ts
            .map(|ts| event.chunk_end_ts >= ts)
            .unwrap_or(true)
        {
            self.last_chunk_end_ts = Some(event.chunk_end_ts);
            self.obi_k_dw_close = event.obi_k_dw_close;
        }
        for level in &event.heatmap_levels {
            let dst = self.heatmap.entry(price_to_tick(level.price)).or_default();
            dst.bid_liquidity += level.bid_liquidity;
            dst.ask_liquidity += level.ask_liquidity;
        }
    }

    fn into_event(self, ts_bucket: DateTime<Utc>, market: MarketKind, symbol: &str) -> EngineEvent {
        build_engine_event(
            ts_bucket,
            market,
            symbol,
            "md.agg.orderbook.1m",
            format!(
                "validator.canonical.{}.orderbook.1m.{}",
                market.as_str(),
                symbol.to_lowercase()
            ),
            MdData::AggOrderbook1m(AggOrderbook1mEvent {
                ts_bucket,
                chunk_start_ts: ts_bucket,
                chunk_end_ts: canonical_minute_end(ts_bucket),
                source_event_count: self.source_event_count,
                sample_count: self.sample_count,
                bbo_updates: self.bbo_updates,
                spread_sum: self.spread_sum,
                topk_depth_sum: self.topk_depth_sum,
                obi_sum: self.obi_sum,
                obi_l1_sum: self.obi_l1_sum,
                obi_k_sum: self.obi_k_sum,
                obi_k_dw_sum: self.obi_k_dw_sum,
                obi_k_dw_change_sum: self.obi_k_dw_change_sum,
                obi_k_dw_adj_sum: self.obi_k_dw_adj_sum,
                microprice_sum: self.microprice_sum,
                microprice_classic_sum: self.microprice_classic_sum,
                microprice_kappa_sum: self.microprice_kappa_sum,
                microprice_adj_sum: self.microprice_adj_sum,
                ofi_sum: self.ofi_sum,
                obi_k_dw_close: self.obi_k_dw_close,
                heatmap_levels: self
                    .heatmap
                    .into_iter()
                    .map(|(tick, level)| AggHeatmapLevel {
                        price: tick_to_price(tick),
                        bid_liquidity: level.bid_liquidity,
                        ask_liquidity: level.ask_liquidity,
                    })
                    .collect(),
            }),
        )
    }
}

impl LiqAggAcc {
    fn apply(&mut self, event: &AggLiq1mEvent) {
        self.source_event_count += event.source_event_count.max(0);
        for level in &event.levels {
            let dst = self.levels.entry(price_to_tick(level.price)).or_default();
            dst.long_liq += level.long_liq;
            dst.short_liq += level.short_liq;
        }
    }

    fn into_event(self, ts_bucket: DateTime<Utc>, market: MarketKind, symbol: &str) -> EngineEvent {
        build_engine_event(
            ts_bucket,
            market,
            symbol,
            "md.agg.liq.1m",
            format!(
                "validator.canonical.{}.liq.1m.{}",
                market.as_str(),
                symbol.to_lowercase()
            ),
            MdData::AggLiq1m(AggLiq1mEvent {
                ts_bucket,
                chunk_start_ts: ts_bucket,
                chunk_end_ts: canonical_minute_end(ts_bucket),
                source_event_count: self.source_event_count,
                levels: self
                    .levels
                    .into_iter()
                    .map(|(tick, level)| AggLiqLevel {
                        price: tick_to_price(tick),
                        long_liq: level.long_liq,
                        short_liq: level.short_liq,
                    })
                    .collect(),
            }),
        )
    }
}

impl FundingMarkAgg {
    fn apply(&mut self, event: &AggFundingMark1mEvent) {
        self.source_event_count += event.source_event_count.max(0);
        self.mark_points.extend(event.mark_points.iter().cloned());
        self.funding_points
            .extend(event.funding_points.iter().cloned());
    }

    fn into_event(self, ts_bucket: DateTime<Utc>, market: MarketKind, symbol: &str) -> EngineEvent {
        let mut mark_points = self.mark_points;
        let mut funding_points = self.funding_points;
        mark_points.sort_by_key(|point| point.ts);
        funding_points.sort_by_key(|point| point.ts);
        build_engine_event(
            ts_bucket,
            market,
            symbol,
            "md.agg.funding_mark.1m",
            format!(
                "validator.canonical.{}.funding_mark.1m.{}",
                market.as_str(),
                symbol.to_lowercase()
            ),
            MdData::AggFundingMark1m(AggFundingMark1mEvent {
                ts_bucket,
                chunk_start_ts: ts_bucket,
                chunk_end_ts: canonical_minute_end(ts_bucket),
                source_event_count: self.source_event_count,
                mark_points,
                funding_points,
            }),
        )
    }
}

fn parse_args() -> Result<CliArgs> {
    let mut config_path = DEFAULT_CONFIG_PATH.to_string();
    let mut symbol = None;
    let mut market = "all".to_string();
    let mut from_ts = Utc::now() - ChronoDuration::days(1);
    let mut to_ts = Utc::now();
    let mut batch_size = DEFAULT_BATCH_SIZE;
    let mut source = ValidationSource::Db;
    let mut raw_root = None;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = take_arg_value(&mut args, "--config")?;
            }
            "--symbol" => {
                symbol = Some(take_arg_value(&mut args, "--symbol")?);
            }
            "--market" => {
                market = take_arg_value(&mut args, "--market")?;
            }
            "--from" => {
                let raw = take_arg_value(&mut args, "--from")?;
                from_ts =
                    parse_rfc3339_utc(&raw).with_context(|| format!("parse --from value {raw}"))?;
            }
            "--to" => {
                let raw = take_arg_value(&mut args, "--to")?;
                to_ts =
                    parse_rfc3339_utc(&raw).with_context(|| format!("parse --to value {raw}"))?;
            }
            "--batch-size" => {
                let raw = take_arg_value(&mut args, "--batch-size")?;
                batch_size = raw
                    .parse::<i64>()
                    .with_context(|| format!("parse --batch-size value {raw}"))?;
            }
            "--source" => {
                let raw = take_arg_value(&mut args, "--source")?;
                source = match raw.as_str() {
                    "db" => ValidationSource::Db,
                    "raw" => ValidationSource::Raw,
                    _ => bail!("--source must be db or raw, got {raw}"),
                };
            }
            "--raw-root" => {
                raw_root = Some(take_arg_value(&mut args, "--raw-root")?);
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => bail!("unsupported arg: {other}"),
        }
    }

    if batch_size <= 0 {
        bail!("--batch-size must be > 0");
    }
    if from_ts >= to_ts {
        bail!("--from must be earlier than --to");
    }

    Ok(CliArgs {
        config_path,
        symbol,
        market,
        from_ts,
        to_ts,
        batch_size,
        source,
        raw_root,
    })
}

fn take_arg_value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    args.next()
        .ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn parse_rfc3339_utc(raw: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(raw)
        .with_context(|| format!("invalid RFC3339 timestamp: {raw}"))?
        .with_timezone(&Utc))
}

fn print_usage() {
    println!(
        "usage: cargo run -p indicator_engine --bin validate_storage_refactor -- [--config PATH] [--symbol SYMBOL] [--market all|futures|spot] [--from RFC3339] [--to RFC3339] [--batch-size N] [--source db|raw] [--raw-root PATH]"
    );
}

fn load_raw_events(
    raw_root: &str,
    symbol: &str,
    market: &str,
    from_ts: DateTime<Utc>,
    to_ts: DateTime<Utc>,
) -> Result<Vec<NormalizedMdEvent>> {
    let root = PathBuf::from(raw_root);
    let requested_markets = requested_markets(market)?;
    let mut day = from_ts.date_naive();
    let end_day = to_ts.date_naive();
    let mut seq: usize = 0;
    let mut rows = Vec::new();

    while day <= end_day {
        let date = day.format("%Y-%m-%d").to_string();
        for market_name in &requested_markets {
            let file_path = root
                .join(format!("date={date}"))
                .join(format!("market={market_name}"))
                .join(format!("symbol={}", symbol.to_uppercase()))
                .join("dataset=raw_md")
                .join("append.jsonl");
            if !file_path.exists() {
                continue;
            }
            read_raw_file(
                &file_path, symbol, market, from_ts, to_ts, &mut seq, &mut rows,
            )?;
        }
        day = day
            .succ_opt()
            .ok_or_else(|| anyhow!("date overflow while scanning raw events"))?;
    }

    rows.sort_by(|a, b| a.0.event_ts.cmp(&b.0.event_ts).then_with(|| a.1.cmp(&b.1)));
    Ok(rows.into_iter().map(|(event, _)| event).collect())
}

fn requested_markets(market: &str) -> Result<Vec<&'static str>> {
    match market {
        "all" => Ok(vec!["spot", "futures"]),
        "spot" => Ok(vec!["spot"]),
        "futures" => Ok(vec!["futures"]),
        other => bail!("unsupported market filter: {other}"),
    }
}

fn read_raw_file(
    file_path: &Path,
    symbol: &str,
    market_filter: &str,
    from_ts: DateTime<Utc>,
    to_ts: DateTime<Utc>,
    seq: &mut usize,
    out: &mut Vec<(NormalizedMdEvent, usize)>,
) -> Result<()> {
    let file = File::open(file_path)
        .with_context(|| format!("open raw cold-store file {}", file_path.display()))?;
    let mut reader = BufReader::new(file);
    let mut buf = String::new();
    let mut line_no: usize = 0;
    let from_ts_ms = from_ts.timestamp_millis();
    let to_ts_ms = to_ts.timestamp_millis();
    loop {
        buf.clear();
        let bytes = reader.read_line(&mut buf).with_context(|| {
            format!(
                "read raw cold-store line {} from {}",
                line_no + 1,
                file_path.display()
            )
        })?;
        if bytes == 0 {
            break;
        }
        line_no = line_no.saturating_add(1);
        let has_trailing_newline = buf.ends_with('\n');
        let line = buf.trim_end_matches(['\n', '\r']);
        if line.trim().is_empty() {
            continue;
        }
        if let Some(ts_ms) = extract_event_ts_ms(line) {
            if ts_ms < from_ts_ms {
                continue;
            }
            if ts_ms >= to_ts_ms {
                break;
            }
        }
        let row: RawColdRow = match serde_json::from_str(line) {
            Ok(row) => row,
            Err(err) if !has_trailing_newline => {
                eprintln!(
                    "warning: skipping unterminated raw cold-store tail line {} from {}: {}",
                    line_no,
                    file_path.display(),
                    err
                );
                break;
            }
            Err(err)
                if extract_event_ts_ms(line)
                    .map(|ts_ms| ts_ms < from_ts_ms || ts_ms >= to_ts_ms)
                    .unwrap_or(false) =>
            {
                eprintln!(
                    "warning: skipping malformed out-of-range raw cold-store line {} from {}: {}",
                    line_no,
                    file_path.display(),
                    err
                );
                continue;
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "decode raw cold-store line {} from {}",
                        line_no,
                        file_path.display()
                    )
                });
            }
        };
        if !should_include_raw_msg_type(&row.msg_type) {
            continue;
        }
        if !row.symbol.eq_ignore_ascii_case(symbol) {
            continue;
        }
        if market_filter != "all" && !row.market.eq_ignore_ascii_case(market_filter) {
            continue;
        }
        let Some(event_ts) = DateTime::<Utc>::from_timestamp_millis(row.event_ts_ms) else {
            continue;
        };
        out.push((
            NormalizedMdEvent {
                msg_type: row.msg_type,
                market: row.market.to_lowercase(),
                symbol: row.symbol.to_uppercase(),
                source_kind: row.source_kind,
                backfill_in_progress: row.backfill_in_progress,
                routing_key: row.routing_key,
                stream_name: row.stream_name,
                event_ts,
                data: row.data,
            },
            *seq,
        ));
        *seq = seq.saturating_add(1);
    }
    Ok(())
}

fn extract_event_ts_ms(line: &str) -> Option<i64> {
    let marker = "\"event_ts_ms\":";
    let start = line.find(marker)? + marker.len();
    let digits = line[start..]
        .chars()
        .skip_while(|ch| ch.is_ascii_whitespace())
        .take_while(|ch| ch.is_ascii_digit() || *ch == '-')
        .collect::<String>();
    digits.parse::<i64>().ok()
}

fn should_include_raw_msg_type(msg_type: &str) -> bool {
    matches!(
        msg_type,
        "md.trade"
            | "md.depth"
            | "md.orderbook_snapshot_l2"
            | "md.bbo"
            | "md.kline"
            | "md.mark_price"
            | "md.funding_rate"
            | "md.force_order"
    )
}

fn decode_normalized_event(event: &NormalizedMdEvent) -> Result<EngineEvent> {
    let payload = serde_json::json!({
        "schema_version": 1,
        "msg_type": event.msg_type,
        "message_id": Uuid::new_v4(),
        "trace_id": Uuid::new_v4(),
        "routing_key": event.routing_key,
        "market": event.market,
        "symbol": event.symbol,
        "source_kind": event.source_kind,
        "backfill_in_progress": event.backfill_in_progress,
        "event_ts": event.event_ts.to_rfc3339(),
        "published_at": event.event_ts.to_rfc3339(),
        "data": event.data,
    });
    let bytes = serde_json::to_vec(&payload).context("encode normalized event payload")?;
    decode_contract_body(&bytes).context("decode normalized event payload")
}

fn sort_normalized_events(events: &mut [NormalizedMdEvent]) {
    events.sort_by(|a, b| {
        a.event_ts
            .cmp(&b.event_ts)
            .then_with(|| a.msg_type.cmp(&b.msg_type))
            .then_with(|| a.market.cmp(&b.market))
            .then_with(|| a.symbol.cmp(&b.symbol))
    });
}

fn ingest_new_events(
    new_state: &mut StateStore,
    new_scheduler: &mut WindowScheduler,
    new_ready: &mut VecDeque<DateTime<Utc>>,
    new_max_seen_event_ts: &mut Option<DateTime<Utc>>,
    events: Vec<EngineEvent>,
) {
    for event in events {
        *new_max_seen_event_ts = Some(match new_max_seen_event_ts.as_ref().cloned() {
            Some(prev) => prev.max(event.event_ts),
            None => event.event_ts,
        });
        new_state.ingest(event);
    }
    if let Some(watermark_ts) = new_max_seen_event_ts.as_ref().cloned() {
        new_ready.extend(new_scheduler.ready_minutes(watermark_ts));
    }
}

async fn compare_ready_minutes(
    pool: &PgPool,
    registry: &[Arc<dyn Indicator>],
    runtime_options: &IndicatorRuntimeOptions,
    symbol: &str,
    compare_from_minute: DateTime<Utc>,
    old_state: &mut StateStore,
    new_state: &mut StateStore,
    old_ready: &mut VecDeque<DateTime<Utc>>,
    new_ready: &mut VecDeque<DateTime<Utc>>,
    mut minutes_validated: usize,
) -> Result<usize> {
    while let (Some(old_minute), Some(new_minute)) =
        (old_ready.front().copied(), new_ready.front().copied())
    {
        if old_minute != new_minute {
            bail!(
                "ready minute mismatch old_minute={} new_minute={}",
                old_minute,
                new_minute
            );
        }
        old_ready.pop_front();
        new_ready.pop_front();

        let old_bundle = old_state.finalize_minute(old_minute);
        let new_bundle = new_state.finalize_minute(new_minute);
        if old_minute < compare_from_minute {
            continue;
        }
        let old_snapshots =
            compute_snapshots(pool, registry, runtime_options, symbol, &old_bundle).await?;
        let new_snapshots =
            compute_snapshots(pool, registry, runtime_options, symbol, &new_bundle).await?;
        if let Err(err) = compare_snapshot_sets(old_minute, &old_snapshots, &new_snapshots) {
            let old_map = snapshot_map(&old_snapshots);
            let new_map = snapshot_map(&new_snapshots);
            if let (Some(old_vpin), Some(new_vpin)) =
                (old_map.get("vpin:1m"), new_map.get("vpin:1m"))
            {
                eprintln!("debug vpin old={old_vpin}");
                eprintln!("debug vpin new={new_vpin}");
            }
            return Err(err);
        }
        minutes_validated = minutes_validated.saturating_add(1);
    }
    Ok(minutes_validated)
}

async fn compute_snapshots(
    pool: &PgPool,
    registry: &[Arc<dyn Indicator>],
    runtime_options: &IndicatorRuntimeOptions,
    symbol: &str,
    bundle: &WindowBundle,
) -> Result<Vec<IndicatorSnapshotRow>> {
    let supplement = load_kline_history_supplement(
        pool,
        symbol,
        &bundle.history_futures,
        &bundle.history_spot,
        runtime_options.kline_history_bars_4h,
        runtime_options.kline_history_bars_1d,
        runtime_options.kline_history_fill_1d_from_db,
        runtime_options.ema_fill_from_db,
        runtime_options.ema_db_bars_4h,
        runtime_options.ema_db_bars_1d,
        runtime_options.fvg_fill_from_db,
        &runtime_options.fvg_windows,
        runtime_options.fvg_db_bars_4h,
        runtime_options.fvg_db_bars_1d,
        bundle.ts_bucket + ChronoDuration::minutes(1),
    )
    .await;
    let ctx = IndicatorContext::from_bundle(bundle, runtime_options, supplement);
    let mut snapshots = registry
        .iter()
        .filter_map(|indicator| indicator.evaluate(&ctx).snapshot)
        .collect::<Vec<_>>();
    snapshots.sort_by(|a, b| {
        a.indicator_code
            .cmp(b.indicator_code)
            .then_with(|| a.window_code.cmp(b.window_code))
    });

    let expected_snapshot_count = registry.len();
    if snapshots.len() != expected_snapshot_count {
        bail!(
            "expected {} indicator snapshots at minute {}, got {}",
            expected_snapshot_count,
            bundle.ts_bucket,
            snapshots.len()
        );
    }
    Ok(snapshots)
}

fn logical_bucket_ts(event: &EngineEvent) -> DateTime<Utc> {
    match &event.data {
        MdData::AggTrade1m(v) => v.ts_bucket,
        MdData::AggOrderbook1m(v) => v.ts_bucket,
        MdData::AggLiq1m(v) => v.ts_bucket,
        MdData::AggFundingMark1m(v) => v.ts_bucket,
        _ => event.event_ts,
    }
}

fn compare_snapshot_sets(
    minute: DateTime<Utc>,
    old_snapshots: &[IndicatorSnapshotRow],
    new_snapshots: &[IndicatorSnapshotRow],
) -> Result<()> {
    let old_map = snapshot_map(old_snapshots);
    let new_map = snapshot_map(new_snapshots);
    if old_map.len() != new_map.len() {
        bail!(
            "minute {} snapshot coverage mismatch old={} new={}",
            minute,
            old_map.len(),
            new_map.len()
        );
    }
    if old_map.keys().collect::<BTreeSet<_>>() != new_map.keys().collect::<BTreeSet<_>>() {
        bail!(
            "minute {} snapshot key mismatch old_keys={:?} new_keys={:?}",
            minute,
            old_map.keys().collect::<Vec<_>>(),
            new_map.keys().collect::<Vec<_>>()
        );
    }

    for (key, old_payload) in &old_map {
        let new_payload = new_map
            .get(key)
            .with_context(|| format!("minute {minute} missing new snapshot for {key}"))?;
        if let Some(diff) = diff_json("$", old_payload, new_payload) {
            bail!("minute {} snapshot {} mismatch: {}", minute, key, diff);
        }
    }
    Ok(())
}

fn snapshot_map<'a>(snapshots: &'a [IndicatorSnapshotRow]) -> BTreeMap<String, &'a Value> {
    snapshots
        .iter()
        .map(|row| {
            (
                format!("{}:{}", row.indicator_code, row.window_code),
                &row.payload_json,
            )
        })
        .collect()
}

fn diff_json(path: &str, left: &Value, right: &Value) -> Option<String> {
    match (left, right) {
        (Value::Null, Value::Null) => None,
        (Value::Bool(a), Value::Bool(b)) if a == b => None,
        (Value::String(a), Value::String(b)) if a == b => None,
        (Value::Number(a), Value::Number(b)) => {
            let left_num = a.as_f64()?;
            let right_num = b.as_f64()?;
            let scale = left_num.abs().max(right_num.abs()).max(1.0);
            if (left_num - right_num).abs() <= FLOAT_EPSILON * scale {
                None
            } else {
                Some(format!("{path}: left={left_num} right={right_num}"))
            }
        }
        (Value::Array(a), Value::Array(b)) => {
            if a.len() != b.len() {
                return Some(format!(
                    "{path}: array length mismatch left={} right={}",
                    a.len(),
                    b.len()
                ));
            }
            for (idx, (left_item, right_item)) in a.iter().zip(b.iter()).enumerate() {
                let child_path = format!("{path}[{idx}]");
                if let Some(diff) = diff_json(&child_path, left_item, right_item) {
                    return Some(diff);
                }
            }
            None
        }
        (Value::Object(a), Value::Object(b)) => {
            let left_keys = a.keys().collect::<BTreeSet<_>>();
            let right_keys = b.keys().collect::<BTreeSet<_>>();
            if left_keys != right_keys {
                return Some(format!(
                    "{path}: object key mismatch left={:?} right={:?}",
                    left_keys, right_keys
                ));
            }
            for key in left_keys {
                let child_path = format!("{path}.{key}");
                if let Some(diff) = diff_json(&child_path, &a[key.as_str()], &b[key.as_str()]) {
                    return Some(diff);
                }
            }
            None
        }
        _ => Some(format!("{path}: left={left:?} right={right:?}")),
    }
}

fn build_engine_event(
    ts_bucket: DateTime<Utc>,
    market: MarketKind,
    symbol: &str,
    msg_type: &str,
    routing_key: String,
    data: MdData,
) -> EngineEvent {
    let event_ts = canonical_minute_end(ts_bucket);
    EngineEvent {
        schema_version: 1,
        msg_type: msg_type.to_string(),
        message_id: Uuid::new_v4(),
        trace_id: Uuid::new_v4(),
        routing_key,
        market,
        symbol: symbol.to_string(),
        source_kind: "validator_canonical".to_string(),
        backfill_in_progress: true,
        event_ts,
        published_at: event_ts,
        data,
    }
}

fn canonical_minute_end(ts_bucket: DateTime<Utc>) -> DateTime<Utc> {
    ts_bucket + ChronoDuration::minutes(1) - ChronoDuration::milliseconds(1)
}

fn floor_minute(ts: DateTime<Utc>) -> DateTime<Utc> {
    ts - ChronoDuration::seconds(ts.timestamp().rem_euclid(60))
        - ChronoDuration::nanoseconds(i64::from(ts.timestamp_subsec_nanos()))
}

fn ceil_minute(ts: DateTime<Utc>) -> DateTime<Utc> {
    let floored = floor_minute(ts);
    if ts == floored {
        floored
    } else {
        floored + ChronoDuration::minutes(1)
    }
}

fn price_to_tick(price: f64) -> i64 {
    (price * PRICE_SCALE).round() as i64
}

fn tick_to_price(tick: i64) -> f64 {
    tick as f64 / PRICE_SCALE
}
