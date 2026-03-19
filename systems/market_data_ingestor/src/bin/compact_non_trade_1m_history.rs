use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Timelike, Utc};
use futures_util::TryStreamExt;
use market_data_ingestor::app::config::load_config;
use market_data_ingestor::normalize::NormalizedMdEvent;
use market_data_ingestor::observability::tracing as obs_tracing;
use market_data_ingestor::sinks::md_db_writer::MdDbWriter;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, FromRow, PgPool, Row};
use std::collections::BTreeMap;
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use tracing::info;

const DEFAULT_CONFIG_PATH: &str = "config/config.yaml";
const DEFAULT_BATCH_SIZE: usize = 256;
const PRICE_SCALE: f64 = 100.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TableKind {
    Orderbook,
    Liq,
    FundingMark,
}

impl TableKind {
    fn all() -> Vec<Self> {
        vec![Self::Orderbook, Self::Liq, Self::FundingMark]
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Orderbook => "agg_orderbook_1m",
            Self::Liq => "agg_liq_1m",
            Self::FundingMark => "agg_funding_mark_1m",
        }
    }

    fn msg_type(self) -> &'static str {
        match self {
            Self::Orderbook => "md.agg.orderbook.1m",
            Self::Liq => "md.agg.liq.1m",
            Self::FundingMark => "md.agg.funding_mark.1m",
        }
    }

    fn stream_name(self) -> &'static str {
        match self {
            Self::Orderbook => "agg.orderbook.1m",
            Self::Liq => "agg.liq.1m",
            Self::FundingMark => "agg.funding_mark.1m",
        }
    }

    fn routing_key(self, market: &str, symbol: &str) -> String {
        match self {
            Self::Orderbook => format!("md.agg.{market}.orderbook.1m.{}", symbol.to_lowercase()),
            Self::Liq => format!("md.agg.{market}.liq.1m.{}", symbol.to_lowercase()),
            Self::FundingMark => {
                format!("md.agg.{market}.funding_mark.1m.{}", symbol.to_lowercase())
            }
        }
    }

    fn temp_path(self, symbol: &str, from_ts: DateTime<Utc>, to_ts: DateTime<Utc>) -> PathBuf {
        PathBuf::from(format!(
            "/tmp/{}_{}_{}_{}.jsonl",
            self.as_str(),
            symbol,
            ts_slug(from_ts),
            ts_slug(to_ts)
        ))
    }
}

#[derive(Debug)]
struct CliArgs {
    config_path: String,
    symbol: String,
    market: String,
    from_ts: DateTime<Utc>,
    to_ts: DateTime<Utc>,
    batch_size: usize,
    apply: bool,
    tables: Vec<TableKind>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CanonicalMinuteKey {
    market: String,
    symbol: String,
    stream_name: String,
    ts_bucket: DateTime<Utc>,
}

impl CanonicalMinuteKey {
    fn canonical_chunk_start_ts(&self) -> DateTime<Utc> {
        self.ts_bucket
    }

    fn canonical_chunk_end_ts(&self) -> DateTime<Utc> {
        self.ts_bucket + ChronoDuration::minutes(1) - ChronoDuration::milliseconds(1)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredEvent {
    msg_type: String,
    market: String,
    symbol: String,
    source_kind: String,
    routing_key: String,
    stream_name: String,
    event_ts: DateTime<Utc>,
    data: Value,
}

impl StoredEvent {
    fn into_normalized(self) -> NormalizedMdEvent {
        NormalizedMdEvent {
            msg_type: self.msg_type,
            market: self.market,
            symbol: self.symbol,
            source_kind: self.source_kind,
            backfill_in_progress: false,
            routing_key: self.routing_key,
            stream_name: self.stream_name,
            event_ts: self.event_ts,
            data: self.data,
        }
    }
}

#[derive(Debug)]
struct MaterializedPlan {
    table: TableKind,
    path: PathBuf,
    physical_rows: i64,
    canonical_rows: i64,
}

#[derive(Debug, FromRow)]
struct OrderbookRow {
    market: String,
    symbol: String,
    source_kind: String,
    stream_name: String,
    ts_bucket: DateTime<Utc>,
    chunk_start_ts: DateTime<Utc>,
    chunk_end_ts: DateTime<Utc>,
    source_event_count: Option<i64>,
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
    heatmap_levels: Value,
}

#[derive(Debug, FromRow)]
struct LiqRow {
    market: String,
    symbol: String,
    source_kind: String,
    stream_name: String,
    ts_bucket: DateTime<Utc>,
    chunk_start_ts: DateTime<Utc>,
    chunk_end_ts: DateTime<Utc>,
    source_event_count: Option<i64>,
    force_liq_levels: Value,
}

#[derive(Debug, FromRow)]
struct FundingRow {
    market: String,
    symbol: String,
    source_kind: String,
    stream_name: String,
    ts_bucket: DateTime<Utc>,
    chunk_start_ts: DateTime<Utc>,
    chunk_end_ts: DateTime<Utc>,
    source_event_count: Option<i64>,
    mark_points: Value,
    funding_points: Value,
}

#[derive(Debug, Clone, Default)]
struct BookLevelAgg {
    bid_liquidity: f64,
    ask_liquidity: f64,
}

#[derive(Debug, Clone, Default)]
struct LiqLevelAgg {
    long_liq: f64,
    short_liq: f64,
}

#[derive(Debug)]
struct OrderbookAccumulator {
    key: CanonicalMinuteKey,
    source_kind: String,
    source_event_count: i64,
    saw_source_event_count: bool,
    source_start_ts: DateTime<Utc>,
    source_end_ts: DateTime<Utc>,
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

impl OrderbookAccumulator {
    fn new(row: &OrderbookRow) -> Self {
        Self {
            key: CanonicalMinuteKey {
                market: row.market.clone(),
                symbol: row.symbol.clone(),
                stream_name: row.stream_name.clone(),
                ts_bucket: row.ts_bucket,
            },
            source_kind: row.source_kind.clone(),
            source_event_count: 0,
            saw_source_event_count: false,
            source_start_ts: row.chunk_start_ts,
            source_end_ts: row.chunk_end_ts,
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

    fn apply(&mut self, row: OrderbookRow) -> Result<()> {
        if let Some(v) = row.source_event_count {
            self.source_event_count += v;
            self.saw_source_event_count = true;
        }
        self.source_start_ts = self.source_start_ts.min(row.chunk_start_ts);
        self.source_end_ts = self.source_end_ts.max(row.chunk_end_ts);
        self.sample_count += row.sample_count;
        self.bbo_updates += row.bbo_updates;
        self.spread_sum += row.spread_sum;
        self.topk_depth_sum += row.topk_depth_sum;
        self.obi_sum += row.obi_sum;
        self.obi_l1_sum += row.obi_l1_sum;
        self.obi_k_sum += row.obi_k_sum;
        self.obi_k_dw_sum += row.obi_k_dw_sum;
        self.obi_k_dw_change_sum += row.obi_k_dw_change_sum;
        self.obi_k_dw_adj_sum += row.obi_k_dw_adj_sum;
        self.microprice_sum += row.microprice_sum;
        self.microprice_classic_sum += row.microprice_classic_sum;
        self.microprice_kappa_sum += row.microprice_kappa_sum;
        self.microprice_adj_sum += row.microprice_adj_sum;
        self.ofi_sum += row.ofi_sum;
        if row.obi_k_dw_close.is_some() {
            self.obi_k_dw_close = row.obi_k_dw_close;
        }
        merge_heatmap_levels(&mut self.heatmap, &row.heatmap_levels)?;
        Ok(())
    }

    fn into_event(self) -> StoredEvent {
        let data = json!({
            "ts_bucket": self.key.ts_bucket.to_rfc3339(),
            "chunk_start_ts": self.key.canonical_chunk_start_ts().to_rfc3339(),
            "chunk_end_ts": self.key.canonical_chunk_end_ts().to_rfc3339(),
            "source_event_count": if self.saw_source_event_count { Value::from(self.source_event_count) } else { Value::Null },
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
            "heatmap_levels": build_heatmap_levels(&self.heatmap),
            "payload_json": {
                "agg_mode": "canonical_1m",
                "source_start_ts": self.source_start_ts.to_rfc3339(),
                "source_end_ts": self.source_end_ts.to_rfc3339(),
            }
        });
        StoredEvent {
            msg_type: TableKind::Orderbook.msg_type().to_string(),
            market: self.key.market.clone(),
            symbol: self.key.symbol.clone(),
            source_kind: self.source_kind,
            routing_key: TableKind::Orderbook.routing_key(&self.key.market, &self.key.symbol),
            stream_name: TableKind::Orderbook.stream_name().to_string(),
            event_ts: self.key.canonical_chunk_end_ts(),
            data,
        }
    }
}

#[derive(Debug)]
struct LiqAccumulator {
    key: CanonicalMinuteKey,
    source_kind: String,
    source_event_count: i64,
    saw_source_event_count: bool,
    source_start_ts: DateTime<Utc>,
    source_end_ts: DateTime<Utc>,
    levels: BTreeMap<i64, LiqLevelAgg>,
}

impl LiqAccumulator {
    fn new(row: &LiqRow) -> Self {
        Self {
            key: CanonicalMinuteKey {
                market: row.market.clone(),
                symbol: row.symbol.clone(),
                stream_name: row.stream_name.clone(),
                ts_bucket: row.ts_bucket,
            },
            source_kind: row.source_kind.clone(),
            source_event_count: 0,
            saw_source_event_count: false,
            source_start_ts: row.chunk_start_ts,
            source_end_ts: row.chunk_end_ts,
            levels: BTreeMap::new(),
        }
    }

    fn apply(&mut self, row: LiqRow) -> Result<()> {
        if let Some(v) = row.source_event_count {
            self.source_event_count += v;
            self.saw_source_event_count = true;
        }
        self.source_start_ts = self.source_start_ts.min(row.chunk_start_ts);
        self.source_end_ts = self.source_end_ts.max(row.chunk_end_ts);
        merge_liq_levels(&mut self.levels, &row.force_liq_levels)?;
        Ok(())
    }

    fn into_event(self) -> StoredEvent {
        let data = json!({
            "ts_bucket": self.key.ts_bucket.to_rfc3339(),
            "chunk_start_ts": self.key.canonical_chunk_start_ts().to_rfc3339(),
            "chunk_end_ts": self.key.canonical_chunk_end_ts().to_rfc3339(),
            "source_event_count": if self.saw_source_event_count { Value::from(self.source_event_count) } else { Value::Null },
            "force_liq_levels": build_liq_levels(&self.levels),
            "payload_json": {
                "agg_mode": "canonical_1m",
                "source_start_ts": self.source_start_ts.to_rfc3339(),
                "source_end_ts": self.source_end_ts.to_rfc3339(),
            }
        });
        StoredEvent {
            msg_type: TableKind::Liq.msg_type().to_string(),
            market: self.key.market.clone(),
            symbol: self.key.symbol.clone(),
            source_kind: self.source_kind,
            routing_key: TableKind::Liq.routing_key(&self.key.market, &self.key.symbol),
            stream_name: TableKind::Liq.stream_name().to_string(),
            event_ts: self.key.canonical_chunk_end_ts(),
            data,
        }
    }
}

#[derive(Debug)]
struct FundingAccumulator {
    key: CanonicalMinuteKey,
    source_kind: String,
    source_event_count: i64,
    saw_source_event_count: bool,
    source_start_ts: DateTime<Utc>,
    source_end_ts: DateTime<Utc>,
    mark_points: Vec<Value>,
    funding_points: Vec<Value>,
}

impl FundingAccumulator {
    fn new(row: &FundingRow) -> Self {
        Self {
            key: CanonicalMinuteKey {
                market: row.market.clone(),
                symbol: row.symbol.clone(),
                stream_name: row.stream_name.clone(),
                ts_bucket: row.ts_bucket,
            },
            source_kind: row.source_kind.clone(),
            source_event_count: 0,
            saw_source_event_count: false,
            source_start_ts: row.chunk_start_ts,
            source_end_ts: row.chunk_end_ts,
            mark_points: Vec::new(),
            funding_points: Vec::new(),
        }
    }

    fn apply(&mut self, row: FundingRow) -> Result<()> {
        if let Some(v) = row.source_event_count {
            self.source_event_count += v;
            self.saw_source_event_count = true;
        }
        self.source_start_ts = self.source_start_ts.min(row.chunk_start_ts);
        self.source_end_ts = self.source_end_ts.max(row.chunk_end_ts);
        append_json_array(&mut self.mark_points, &row.mark_points, "mark_points")?;
        append_json_array(
            &mut self.funding_points,
            &row.funding_points,
            "funding_points",
        )?;
        Ok(())
    }

    fn into_event(mut self) -> Result<StoredEvent> {
        sort_points_by_ts(&mut self.mark_points, "mark_points")?;
        sort_points_by_ts(&mut self.funding_points, "funding_points")?;
        let data = json!({
            "ts_bucket": self.key.ts_bucket.to_rfc3339(),
            "chunk_start_ts": self.key.canonical_chunk_start_ts().to_rfc3339(),
            "chunk_end_ts": self.key.canonical_chunk_end_ts().to_rfc3339(),
            "source_event_count": if self.saw_source_event_count { Value::from(self.source_event_count) } else { Value::Null },
            "mark_points": self.mark_points,
            "funding_points": self.funding_points,
            "payload_json": {
                "agg_mode": "canonical_1m",
                "source_start_ts": self.source_start_ts.to_rfc3339(),
                "source_end_ts": self.source_end_ts.to_rfc3339(),
            }
        });
        Ok(StoredEvent {
            msg_type: TableKind::FundingMark.msg_type().to_string(),
            market: self.key.market.clone(),
            symbol: self.key.symbol.clone(),
            source_kind: self.source_kind,
            routing_key: TableKind::FundingMark.routing_key(&self.key.market, &self.key.symbol),
            stream_name: TableKind::FundingMark.stream_name().to_string(),
            event_ts: self.key.canonical_chunk_end_ts(),
            data,
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    obs_tracing::init();
    let args = parse_args()?;
    validate_minute_boundaries(args.from_ts, args.to_ts)?;

    let cfg = load_config(&args.config_path)
        .with_context(|| format!("load config {}", args.config_path))?;
    let pool = connect_md_pool(&cfg.database.resolve_md().postgres_uri()).await?;
    let writer = MdDbWriter::new(pool.clone());

    for table in &args.tables {
        let plan = materialize_table(&pool, &args, *table).await?;
        println!(
            "{}: physical_rows={} canonical_rows={} duplicate_rows={} temp_file={}",
            table.as_str(),
            plan.physical_rows,
            plan.canonical_rows,
            plan.physical_rows - plan.canonical_rows,
            plan.path.display()
        );

        if args.apply {
            apply_plan(&pool, &writer, &args, &plan).await?;
        }
    }

    if !args.apply {
        println!("dry-run only; pass --apply to write canonical rows and purge legacy chunks");
    }

    Ok(())
}

fn parse_args() -> Result<CliArgs> {
    let mut config_path = DEFAULT_CONFIG_PATH.to_string();
    let mut symbol = None;
    let mut market = "all".to_string();
    let mut from_ts = None;
    let mut to_ts = None;
    let mut batch_size = DEFAULT_BATCH_SIZE;
    let mut apply = false;
    let mut tables = TableKind::all();

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => config_path = take_arg_value(&mut args, "--config")?,
            "--symbol" => symbol = Some(take_arg_value(&mut args, "--symbol")?),
            "--market" => market = take_arg_value(&mut args, "--market")?,
            "--from" => {
                let raw = take_arg_value(&mut args, "--from")?;
                from_ts = Some(parse_rfc3339_utc(&raw)?);
            }
            "--to" => {
                let raw = take_arg_value(&mut args, "--to")?;
                to_ts = Some(parse_rfc3339_utc(&raw)?);
            }
            "--batch-size" => {
                let raw = take_arg_value(&mut args, "--batch-size")?;
                batch_size = raw
                    .parse::<usize>()
                    .with_context(|| format!("parse --batch-size value {raw}"))?;
            }
            "--tables" => {
                let raw = take_arg_value(&mut args, "--tables")?;
                tables = parse_tables(&raw)?;
            }
            "--apply" => apply = true,
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => bail!("unsupported arg: {other}"),
        }
    }

    let symbol = symbol.ok_or_else(|| anyhow!("--symbol is required"))?;
    let from_ts = from_ts.ok_or_else(|| anyhow!("--from is required"))?;
    let to_ts = to_ts.ok_or_else(|| anyhow!("--to is required"))?;

    if batch_size == 0 {
        bail!("--batch-size must be > 0");
    }
    if from_ts >= to_ts {
        bail!("--from must be earlier than --to");
    }
    if !matches!(market.as_str(), "all" | "spot" | "futures") {
        bail!("--market must be one of all|spot|futures");
    }

    Ok(CliArgs {
        config_path,
        symbol: symbol.to_uppercase(),
        market,
        from_ts,
        to_ts,
        batch_size,
        apply,
        tables,
    })
}

fn print_usage() {
    eprintln!(
        "compact_non_trade_1m_history \\
  --symbol TESTUSDT --from 2026-03-05T02:52:00Z --to 2026-03-06T04:26:00Z \\
  [--market all|spot|futures] [--tables all|orderbook|liq|funding_mark|orderbook,liq] \\
  [--batch-size 256] [--apply] [--config config/config.yaml]"
    );
}

fn parse_tables(raw: &str) -> Result<Vec<TableKind>> {
    if raw.eq_ignore_ascii_case("all") {
        return Ok(TableKind::all());
    }

    let mut out = Vec::new();
    for part in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let table = match part {
            "orderbook" => TableKind::Orderbook,
            "liq" => TableKind::Liq,
            "funding_mark" | "funding" => TableKind::FundingMark,
            other => bail!("unsupported table selection: {other}"),
        };
        if !out.contains(&table) {
            out.push(table);
        }
    }
    if out.is_empty() {
        bail!("--tables resolved to empty set");
    }
    Ok(out)
}

async fn connect_md_pool(database_url: &str) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(4)
        .connect(database_url)
        .await
        .with_context(|| format!("connect md db {database_url}"))
}

async fn materialize_table(
    pool: &PgPool,
    args: &CliArgs,
    table: TableKind,
) -> Result<MaterializedPlan> {
    let path = table.temp_path(&args.symbol, args.from_ts, args.to_ts);
    match table {
        TableKind::Orderbook => materialize_orderbook(pool, args, &path).await,
        TableKind::Liq => materialize_liq(pool, args, &path).await,
        TableKind::FundingMark => materialize_funding(pool, args, &path).await,
    }
}

async fn materialize_orderbook(
    pool: &PgPool,
    args: &CliArgs,
    path: &Path,
) -> Result<MaterializedPlan> {
    let mut stream = sqlx::query_as::<_, OrderbookRow>(
        r#"
        SELECT
            market::text AS market,
            symbol,
            source_kind::text AS source_kind,
            stream_name,
            ts_bucket,
            chunk_start_ts,
            chunk_end_ts,
            source_event_count,
            sample_count,
            bbo_updates,
            spread_sum,
            topk_depth_sum,
            obi_sum,
            obi_l1_sum,
            obi_k_sum,
            obi_k_dw_sum,
            obi_k_dw_change_sum,
            obi_k_dw_adj_sum,
            microprice_sum,
            microprice_classic_sum,
            microprice_kappa_sum,
            microprice_adj_sum,
            ofi_sum,
            obi_k_dw_close,
            heatmap_levels
        FROM md.agg_orderbook_1m
        WHERE symbol = $1
          AND ts_bucket >= $2
          AND ts_bucket < $3
          AND ($4 = 'all' OR market = $4::cfg.market_type)
        ORDER BY market, symbol, stream_name, ts_bucket, chunk_start_ts, chunk_end_ts
        "#,
    )
    .bind(&args.symbol)
    .bind(args.from_ts)
    .bind(args.to_ts)
    .bind(&args.market)
    .fetch(pool);

    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    let mut current: Option<OrderbookAccumulator> = None;
    let mut physical_rows = 0_i64;
    let mut canonical_rows = 0_i64;

    while let Some(row) = stream.try_next().await.context("fetch orderbook rows")? {
        physical_rows += 1;
        let key = CanonicalMinuteKey {
            market: row.market.clone(),
            symbol: row.symbol.clone(),
            stream_name: row.stream_name.clone(),
            ts_bucket: row.ts_bucket,
        };
        if current.as_ref().map(|acc| acc.key != key).unwrap_or(false) {
            let acc = current.take().expect("orderbook accumulator exists");
            write_stored_event(&mut writer, &acc.into_event())?;
            canonical_rows += 1;
        }
        let acc = current.get_or_insert_with(|| OrderbookAccumulator::new(&row));
        acc.apply(row)?;
    }

    if let Some(acc) = current.take() {
        write_stored_event(&mut writer, &acc.into_event())?;
        canonical_rows += 1;
    }
    writer.flush().context("flush orderbook temp file")?;

    Ok(MaterializedPlan {
        table: TableKind::Orderbook,
        path: path.to_path_buf(),
        physical_rows,
        canonical_rows,
    })
}

async fn materialize_liq(pool: &PgPool, args: &CliArgs, path: &Path) -> Result<MaterializedPlan> {
    let mut stream = sqlx::query_as::<_, LiqRow>(
        r#"
        SELECT
            market::text AS market,
            symbol,
            source_kind::text AS source_kind,
            stream_name,
            ts_bucket,
            chunk_start_ts,
            chunk_end_ts,
            source_event_count,
            force_liq_levels
        FROM md.agg_liq_1m
        WHERE symbol = $1
          AND ts_bucket >= $2
          AND ts_bucket < $3
          AND ($4 = 'all' OR market = $4::cfg.market_type)
        ORDER BY market, symbol, stream_name, ts_bucket, chunk_start_ts, chunk_end_ts
        "#,
    )
    .bind(&args.symbol)
    .bind(args.from_ts)
    .bind(args.to_ts)
    .bind(&args.market)
    .fetch(pool);

    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    let mut current: Option<LiqAccumulator> = None;
    let mut physical_rows = 0_i64;
    let mut canonical_rows = 0_i64;

    while let Some(row) = stream.try_next().await.context("fetch liq rows")? {
        physical_rows += 1;
        let key = CanonicalMinuteKey {
            market: row.market.clone(),
            symbol: row.symbol.clone(),
            stream_name: row.stream_name.clone(),
            ts_bucket: row.ts_bucket,
        };
        if current.as_ref().map(|acc| acc.key != key).unwrap_or(false) {
            let acc = current.take().expect("liq accumulator exists");
            write_stored_event(&mut writer, &acc.into_event())?;
            canonical_rows += 1;
        }
        let acc = current.get_or_insert_with(|| LiqAccumulator::new(&row));
        acc.apply(row)?;
    }

    if let Some(acc) = current.take() {
        write_stored_event(&mut writer, &acc.into_event())?;
        canonical_rows += 1;
    }
    writer.flush().context("flush liq temp file")?;

    Ok(MaterializedPlan {
        table: TableKind::Liq,
        path: path.to_path_buf(),
        physical_rows,
        canonical_rows,
    })
}

async fn materialize_funding(
    pool: &PgPool,
    args: &CliArgs,
    path: &Path,
) -> Result<MaterializedPlan> {
    let mut stream = sqlx::query_as::<_, FundingRow>(
        r#"
        SELECT
            market::text AS market,
            symbol,
            source_kind::text AS source_kind,
            stream_name,
            ts_bucket,
            chunk_start_ts,
            chunk_end_ts,
            source_event_count,
            mark_points,
            funding_points
        FROM md.agg_funding_mark_1m
        WHERE symbol = $1
          AND ts_bucket >= $2
          AND ts_bucket < $3
          AND ($4 = 'all' OR market = $4::cfg.market_type)
        ORDER BY market, symbol, stream_name, ts_bucket, chunk_start_ts, chunk_end_ts
        "#,
    )
    .bind(&args.symbol)
    .bind(args.from_ts)
    .bind(args.to_ts)
    .bind(&args.market)
    .fetch(pool);

    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    let mut current: Option<FundingAccumulator> = None;
    let mut physical_rows = 0_i64;
    let mut canonical_rows = 0_i64;

    while let Some(row) = stream.try_next().await.context("fetch funding rows")? {
        physical_rows += 1;
        let key = CanonicalMinuteKey {
            market: row.market.clone(),
            symbol: row.symbol.clone(),
            stream_name: row.stream_name.clone(),
            ts_bucket: row.ts_bucket,
        };
        if current.as_ref().map(|acc| acc.key != key).unwrap_or(false) {
            let acc = current.take().expect("funding accumulator exists");
            write_stored_event(&mut writer, &acc.into_event()?)?;
            canonical_rows += 1;
        }
        let acc = current.get_or_insert_with(|| FundingAccumulator::new(&row));
        acc.apply(row)?;
    }

    if let Some(acc) = current.take() {
        write_stored_event(&mut writer, &acc.into_event()?)?;
        canonical_rows += 1;
    }
    writer.flush().context("flush funding temp file")?;

    Ok(MaterializedPlan {
        table: TableKind::FundingMark,
        path: path.to_path_buf(),
        physical_rows,
        canonical_rows,
    })
}

async fn apply_plan(
    pool: &PgPool,
    writer: &MdDbWriter,
    args: &CliArgs,
    plan: &MaterializedPlan,
) -> Result<()> {
    info!(
        table = plan.table.as_str(),
        temp_file = %plan.path.display(),
        physical_rows = plan.physical_rows,
        canonical_rows = plan.canonical_rows,
        "applying historical canonical compaction"
    );

    let file = File::open(&plan.path).with_context(|| format!("open {}", plan.path.display()))?;
    let reader = BufReader::new(file);
    let mut batch: Vec<NormalizedMdEvent> = Vec::with_capacity(args.batch_size);
    let mut inserted = 0_i64;

    for line in reader.lines() {
        let line = line.context("read temp file line")?;
        if line.trim().is_empty() {
            continue;
        }
        let stored: StoredEvent =
            serde_json::from_str(&line).context("decode stored canonical event")?;
        batch.push(stored.into_normalized());
        if batch.len() >= args.batch_size {
            writer
                .write_md_events_batch(&batch)
                .await
                .with_context(|| format!("write {} canonical batch", plan.table.as_str()))?;
            inserted += batch.len() as i64;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        writer
            .write_md_events_batch(&batch)
            .await
            .with_context(|| format!("write tail {} canonical batch", plan.table.as_str()))?;
        inserted += batch.len() as i64;
    }

    let deleted = delete_non_canonical_rows(pool, plan.table, args).await?;
    let (physical_after, canonical_after) = count_rows(pool, plan.table, args).await?;
    if physical_after != canonical_after {
        bail!(
            "{} compaction incomplete: physical_rows_after={} canonical_rows_after={}",
            plan.table.as_str(),
            physical_after,
            canonical_after
        );
    }

    println!(
        "applied {}: inserted_or_upserted={} deleted_legacy_rows={} final_rows={}",
        plan.table.as_str(),
        inserted,
        deleted,
        physical_after
    );
    Ok(())
}

async fn delete_non_canonical_rows(pool: &PgPool, table: TableKind, args: &CliArgs) -> Result<u64> {
    let sql = match table {
        TableKind::Orderbook => {
            r#"
            DELETE FROM md.agg_orderbook_1m
            WHERE symbol = $1
              AND ts_bucket >= $2
              AND ts_bucket < $3
              AND ($4 = 'all' OR market = $4::cfg.market_type)
              AND NOT (
                    chunk_start_ts = ts_bucket
                AND chunk_end_ts = ts_bucket + interval '1 minute' - interval '1 millisecond'
              )
            "#
        }
        TableKind::Liq => {
            r#"
            DELETE FROM md.agg_liq_1m
            WHERE symbol = $1
              AND ts_bucket >= $2
              AND ts_bucket < $3
              AND ($4 = 'all' OR market = $4::cfg.market_type)
              AND NOT (
                    chunk_start_ts = ts_bucket
                AND chunk_end_ts = ts_bucket + interval '1 minute' - interval '1 millisecond'
              )
            "#
        }
        TableKind::FundingMark => {
            r#"
            DELETE FROM md.agg_funding_mark_1m
            WHERE symbol = $1
              AND ts_bucket >= $2
              AND ts_bucket < $3
              AND ($4 = 'all' OR market = $4::cfg.market_type)
              AND NOT (
                    chunk_start_ts = ts_bucket
                AND chunk_end_ts = ts_bucket + interval '1 minute' - interval '1 millisecond'
              )
            "#
        }
    };

    let result = sqlx::query(sql)
        .bind(&args.symbol)
        .bind(args.from_ts)
        .bind(args.to_ts)
        .bind(&args.market)
        .execute(pool)
        .await
        .with_context(|| format!("delete legacy {} rows", table.as_str()))?;
    Ok(result.rows_affected())
}

async fn count_rows(pool: &PgPool, table: TableKind, args: &CliArgs) -> Result<(i64, i64)> {
    let sql = match table {
        TableKind::Orderbook => {
            r#"
            SELECT
                count(*) AS physical_rows,
                count(DISTINCT (market, symbol, stream_name, ts_bucket)) AS canonical_rows
            FROM md.agg_orderbook_1m
            WHERE symbol = $1
              AND ts_bucket >= $2
              AND ts_bucket < $3
              AND ($4 = 'all' OR market = $4::cfg.market_type)
            "#
        }
        TableKind::Liq => {
            r#"
            SELECT
                count(*) AS physical_rows,
                count(DISTINCT (market, symbol, stream_name, ts_bucket)) AS canonical_rows
            FROM md.agg_liq_1m
            WHERE symbol = $1
              AND ts_bucket >= $2
              AND ts_bucket < $3
              AND ($4 = 'all' OR market = $4::cfg.market_type)
            "#
        }
        TableKind::FundingMark => {
            r#"
            SELECT
                count(*) AS physical_rows,
                count(DISTINCT (market, symbol, stream_name, ts_bucket)) AS canonical_rows
            FROM md.agg_funding_mark_1m
            WHERE symbol = $1
              AND ts_bucket >= $2
              AND ts_bucket < $3
              AND ($4 = 'all' OR market = $4::cfg.market_type)
            "#
        }
    };

    let row = sqlx::query(sql)
        .bind(&args.symbol)
        .bind(args.from_ts)
        .bind(args.to_ts)
        .bind(&args.market)
        .fetch_one(pool)
        .await
        .with_context(|| format!("count rows for {}", table.as_str()))?;
    Ok((
        row.get::<i64, _>("physical_rows"),
        row.get::<i64, _>("canonical_rows"),
    ))
}

fn write_stored_event(writer: &mut BufWriter<File>, event: &StoredEvent) -> Result<()> {
    serde_json::to_writer(&mut *writer, event).context("encode stored event json")?;
    writer.write_all(b"\n").context("write newline")?;
    Ok(())
}

fn merge_heatmap_levels(dst: &mut BTreeMap<i64, BookLevelAgg>, value: &Value) -> Result<()> {
    let arr = value
        .as_array()
        .ok_or_else(|| anyhow!("heatmap_levels must be array"))?;
    for row in arr {
        let level = row
            .as_array()
            .ok_or_else(|| anyhow!("heatmap level row must be [price,bid,ask]"))?;
        if level.len() < 3 {
            continue;
        }
        let tick = price_to_tick(value_to_f64(&level[0])?);
        let agg = dst.entry(tick).or_default();
        agg.bid_liquidity += value_to_f64(&level[1])?;
        agg.ask_liquidity += value_to_f64(&level[2])?;
    }
    Ok(())
}

fn merge_liq_levels(dst: &mut BTreeMap<i64, LiqLevelAgg>, value: &Value) -> Result<()> {
    let arr = value
        .as_array()
        .ok_or_else(|| anyhow!("force_liq_levels must be array"))?;
    for row in arr {
        let level = row
            .as_array()
            .ok_or_else(|| anyhow!("liq level row must be [price,long,short]"))?;
        if level.len() < 3 {
            continue;
        }
        let tick = price_to_tick(value_to_f64(&level[0])?);
        let agg = dst.entry(tick).or_default();
        agg.long_liq += value_to_f64(&level[1])?;
        agg.short_liq += value_to_f64(&level[2])?;
    }
    Ok(())
}

fn append_json_array(dst: &mut Vec<Value>, value: &Value, field_name: &str) -> Result<()> {
    let arr = value
        .as_array()
        .ok_or_else(|| anyhow!("{field_name} must be array"))?;
    dst.extend(arr.iter().cloned());
    Ok(())
}

fn sort_points_by_ts(points: &mut [Value], field_name: &str) -> Result<()> {
    points.sort_by(|a, b| {
        let a_ts = point_ts(a, field_name).unwrap_or(DateTime::<Utc>::MIN_UTC);
        let b_ts = point_ts(b, field_name).unwrap_or(DateTime::<Utc>::MIN_UTC);
        a_ts.cmp(&b_ts)
    });
    Ok(())
}

fn point_ts(value: &Value, field_name: &str) -> Result<DateTime<Utc>> {
    let obj = value
        .as_object()
        .ok_or_else(|| anyhow!("{field_name} point must be object"))?;
    let ts = obj
        .get("ts")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("{field_name} point missing ts"))?;
    parse_rfc3339_utc(ts)
}

fn build_heatmap_levels(levels: &BTreeMap<i64, BookLevelAgg>) -> Value {
    Value::Array(
        levels
            .iter()
            .map(|(tick, level)| {
                Value::Array(vec![
                    Value::from(tick_to_price(*tick)),
                    Value::from(level.bid_liquidity),
                    Value::from(level.ask_liquidity),
                ])
            })
            .collect(),
    )
}

fn build_liq_levels(levels: &BTreeMap<i64, LiqLevelAgg>) -> Value {
    Value::Array(
        levels
            .iter()
            .map(|(tick, level)| {
                Value::Array(vec![
                    Value::from(tick_to_price(*tick)),
                    Value::from(level.long_liq),
                    Value::from(level.short_liq),
                ])
            })
            .collect(),
    )
}

fn value_to_f64(value: &Value) -> Result<f64> {
    match value {
        Value::Number(n) => n
            .as_f64()
            .ok_or_else(|| anyhow!("number is not representable as f64")),
        Value::String(s) => s.parse::<f64>().with_context(|| format!("parse float {s}")),
        other => Err(anyhow!("expected float-compatible value, got {other}")),
    }
}

fn price_to_tick(price: f64) -> i64 {
    (price * PRICE_SCALE).round() as i64
}

fn tick_to_price(tick: i64) -> f64 {
    tick as f64 / PRICE_SCALE
}

fn ts_slug(ts: DateTime<Utc>) -> String {
    ts.format("%Y%m%dT%H%M%SZ").to_string()
}

fn validate_minute_boundaries(from_ts: DateTime<Utc>, to_ts: DateTime<Utc>) -> Result<()> {
    if from_ts.second() != 0 || from_ts.nanosecond() != 0 {
        bail!("--from must be aligned to the minute: {from_ts}");
    }
    if to_ts.second() != 0 || to_ts.nanosecond() != 0 {
        bail!("--to must be aligned to the minute: {to_ts}");
    }
    Ok(())
}

fn take_arg_value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    args.next()
        .ok_or_else(|| anyhow!("{flag} requires a value"))
}

fn parse_rfc3339_utc(raw: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .with_context(|| format!("parse RFC3339 timestamp {raw}"))
        .map(|dt| dt.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merges_heatmap_levels_by_price() {
        let mut dst = BTreeMap::new();
        merge_heatmap_levels(&mut dst, &json!([[100.0, 1.0, 2.0], [100.0, 0.5, 0.25]])).unwrap();
        let level = dst.get(&price_to_tick(100.0)).unwrap();
        assert!((level.bid_liquidity - 1.5).abs() < 1e-9);
        assert!((level.ask_liquidity - 2.25).abs() < 1e-9);
    }

    #[test]
    fn funding_points_sort_by_ts() {
        let mut points = vec![
            json!({"ts": "2026-03-06T00:00:02Z", "mark_price": 2.0}),
            json!({"ts": "2026-03-06T00:00:01Z", "mark_price": 1.0}),
        ];
        sort_points_by_ts(&mut points, "mark_points").unwrap();
        assert_eq!(points[0]["mark_price"], 1.0);
        assert_eq!(points[1]["mark_price"], 2.0);
    }
}
