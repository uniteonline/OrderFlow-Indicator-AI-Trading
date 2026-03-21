use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, NaiveDateTime, Timelike, Utc};
use indicator_engine::app::bootstrap::{build_db_pool, load_config};
use indicator_engine::storage::snapshot_writer::SnapshotWriter;
use sqlx::{PgPool, Row};

const DEFAULT_CONFIG_PATH: &str = "config/config.yaml";

#[derive(Debug)]
struct Args {
    config_path: String,
    symbol: Option<String>,
    from: DateTime<Utc>,
    apply: bool,
    force_source_gap: bool,
}

#[derive(Debug)]
struct SourceCompleteness {
    trade_fut: bool,
    trade_spot: bool,
    ob_fut: bool,
    ob_spot: bool,
    liq_fut: bool,
    fund_fut: bool,
}

impl SourceCompleteness {
    fn is_indicator_replay_complete(&self) -> bool {
        self.trade_fut && self.trade_spot && self.ob_fut && self.ob_spot && self.fund_fut
    }
}

#[derive(Debug)]
struct TargetPresence {
    snapshot_rows: i64,
    level_rows: i64,
    liq_level_rows: i64,
    trade_flow_fut_rows: i64,
    trade_flow_spot_rows: i64,
    orderbook_rows: i64,
    funding_rows: i64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse()?;
    ensure_minute_boundary(args.from)?;

    let config =
        load_config(&args.config_path).with_context(|| format!("load {}", args.config_path))?;
    let symbol = args
        .symbol
        .unwrap_or_else(|| config.indicator.symbol.clone())
        .to_uppercase();
    let pool = build_db_pool(&config).await?;
    let writer = SnapshotWriter::new(pool.clone());

    let completeness = load_source_completeness(&pool, &symbol, args.from).await?;
    let target_presence = load_target_presence(&pool, &symbol, args.from).await?;
    let progress_before = latest_progress_ts(&pool, &symbol).await?;

    println!(
        "indicator tail repair {}",
        if args.apply { "apply" } else { "dry-run" }
    );
    println!("symbol={symbol}");
    println!("repair_start_ts={}", args.from);
    println!(
        "source_complete_for_indicator_replay={} trade_fut={} trade_spot={} ob_fut={} ob_spot={} liq_fut={} fund_fut={}",
        completeness.is_indicator_replay_complete(),
        completeness.trade_fut,
        completeness.trade_spot,
        completeness.ob_fut,
        completeness.ob_spot,
        completeness.liq_fut,
        completeness.fund_fut
    );
    println!(
        "target_rows snapshot={} level={} liq_level={} trade_flow_fut={} trade_flow_spot={} orderbook={} funding={}",
        target_presence.snapshot_rows,
        target_presence.level_rows,
        target_presence.liq_level_rows,
        target_presence.trade_flow_fut_rows,
        target_presence.trade_flow_spot_rows,
        target_presence.orderbook_rows,
        target_presence.funding_rows
    );
    println!(
        "indicator_progress_before={}",
        progress_before
            .map(|ts| ts.to_string())
            .unwrap_or_else(|| "NULL".to_string())
    );

    if !args.apply {
        println!("pass --apply to delete indicator/outbox tail from repair_start_ts onward and set progress to repair_start_ts - 1m");
        return Ok(());
    }

    if !completeness.is_indicator_replay_complete() && !args.force_source_gap {
        bail!(
            "requested repair minute {} does not have complete dense-source coverage for indicator replay; refuse to rewind without --force-source-gap",
            args.from
        );
    }

    writer
        .rewind_persisted_tail(&symbol, args.from, &config.mq.exchanges.ind.name)
        .await
        .with_context(|| format!("rewind indicator tail from {}", args.from))?;

    let progress_after = latest_progress_ts(&pool, &symbol).await?;
    let target_presence_after = load_target_presence(&pool, &symbol, args.from).await?;

    println!("repair applied");
    println!(
        "indicator_progress_after={}",
        progress_after
            .map(|ts| ts.to_string())
            .unwrap_or_else(|| "NULL".to_string())
    );
    println!(
        "target_rows_after snapshot={} level={} liq_level={} trade_flow_fut={} trade_flow_spot={} orderbook={} funding={}",
        target_presence_after.snapshot_rows,
        target_presence_after.level_rows,
        target_presence_after.liq_level_rows,
        target_presence_after.trade_flow_fut_rows,
        target_presence_after.trade_flow_spot_rows,
        target_presence_after.orderbook_rows,
        target_presence_after.funding_rows
    );
    println!(
        "next step: restart orderflow-indicator-engine so it replays from canonical md.* history"
    );

    Ok(())
}

impl Args {
    fn parse() -> Result<Self> {
        let mut config_path = DEFAULT_CONFIG_PATH.to_string();
        let mut symbol = None;
        let mut from = None;
        let mut apply = false;
        let mut force_source_gap = false;

        let mut it = std::env::args().skip(1);
        while let Some(arg) = it.next() {
            match arg.as_str() {
                "--config" => config_path = take_value(&mut it, "--config")?,
                "--symbol" => symbol = Some(take_value(&mut it, "--symbol")?),
                "--from" => {
                    let raw = take_value(&mut it, "--from")?;
                    from = Some(parse_utc_ts(&raw).with_context(|| format!("parse --from {raw}"))?);
                }
                "--apply" => apply = true,
                "--force-source-gap" => force_source_gap = true,
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => bail!("unsupported arg: {other}"),
            }
        }

        Ok(Self {
            config_path,
            symbol,
            from: from.ok_or_else(|| anyhow!("--from is required"))?,
            apply,
            force_source_gap,
        })
    }
}

fn take_value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    args.next()
        .ok_or_else(|| anyhow!("{flag} requires a value"))
}

fn print_usage() {
    println!(
        "Usage: cargo run -p indicator_engine --bin repair_indicator_tail -- --from <UTC minute> [--symbol BTCUSDT] [--config config/config.yaml] [--apply] [--force-source-gap]"
    );
}

fn parse_utc_ts(raw: &str) -> Result<DateTime<Utc>> {
    if let Ok(ts) = DateTime::parse_from_rfc3339(raw) {
        return Ok(ts.with_timezone(&Utc));
    }
    if let Ok(ts) = NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S") {
        return Ok(ts.and_utc());
    }
    if let Ok(ts) = NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S") {
        return Ok(ts.and_utc());
    }
    Err(anyhow!("unsupported timestamp format: {raw}"))
}

fn ensure_minute_boundary(ts: DateTime<Utc>) -> Result<()> {
    if ts.second() != 0 || ts.nanosecond() != 0 {
        bail!("--from must be aligned to the minute boundary");
    }
    Ok(())
}

async fn latest_progress_ts(pool: &PgPool, symbol: &str) -> Result<Option<DateTime<Utc>>> {
    let row = sqlx::query(
        r#"
        SELECT last_success_ts
        FROM feat.indicator_progress
        WHERE symbol = $1
        "#,
    )
    .bind(symbol)
    .fetch_optional(pool)
    .await
    .context("query feat.indicator_progress")?;
    Ok(row.map(|row| row.get("last_success_ts")))
}

async fn load_source_completeness(
    pool: &PgPool,
    symbol: &str,
    ts_bucket: DateTime<Utc>,
) -> Result<SourceCompleteness> {
    let row = sqlx::query(
        r#"
        SELECT
            EXISTS(
                SELECT 1 FROM md.agg_trade_1m
                WHERE symbol = $1 AND market = 'futures' AND ts_bucket = $2
            ) AS trade_fut,
            EXISTS(
                SELECT 1 FROM md.agg_trade_1m
                WHERE symbol = $1 AND market = 'spot' AND ts_bucket = $2
            ) AS trade_spot,
            EXISTS(
                SELECT 1 FROM md.agg_orderbook_1m
                WHERE symbol = $1 AND market = 'futures' AND ts_bucket = $2
            ) AS ob_fut,
            EXISTS(
                SELECT 1 FROM md.agg_orderbook_1m
                WHERE symbol = $1 AND market = 'spot' AND ts_bucket = $2
            ) AS ob_spot,
            EXISTS(
                SELECT 1 FROM md.agg_liq_1m
                WHERE symbol = $1 AND market = 'futures' AND ts_bucket = $2
            ) AS liq_fut,
            EXISTS(
                SELECT 1 FROM md.agg_funding_mark_1m
                WHERE symbol = $1 AND market = 'futures' AND ts_bucket = $2
            ) AS fund_fut
        "#,
    )
    .bind(symbol)
    .bind(ts_bucket)
    .fetch_one(pool)
    .await
    .context("query md source completeness")?;

    Ok(SourceCompleteness {
        trade_fut: row.get("trade_fut"),
        trade_spot: row.get("trade_spot"),
        ob_fut: row.get("ob_fut"),
        ob_spot: row.get("ob_spot"),
        liq_fut: row.get("liq_fut"),
        fund_fut: row.get("fund_fut"),
    })
}

async fn load_target_presence(
    pool: &PgPool,
    symbol: &str,
    ts_bucket: DateTime<Utc>,
) -> Result<TargetPresence> {
    let row = sqlx::query(
        r#"
        SELECT
            (SELECT count(*) FROM feat.indicator_snapshot WHERE symbol = $1 AND ts_snapshot = $2) AS snapshot_rows,
            (SELECT count(*) FROM feat.indicator_level_value WHERE symbol = $1 AND ts_snapshot = $2) AS level_rows,
            (SELECT count(*) FROM feat.liquidation_density_level WHERE symbol = $1 AND ts_snapshot = $2) AS liq_level_rows,
            (SELECT count(*) FROM feat.trade_flow_feature WHERE symbol = $1 AND market = 'futures' AND ts_bucket = $2) AS trade_flow_fut_rows,
            (SELECT count(*) FROM feat.trade_flow_feature WHERE symbol = $1 AND market = 'spot' AND ts_bucket = $2) AS trade_flow_spot_rows,
            (SELECT count(*) FROM feat.orderbook_feature WHERE symbol = $1 AND ts_bucket = $2) AS orderbook_rows,
            (SELECT count(*) FROM feat.funding_feature WHERE symbol = $1 AND ts_bucket = $2) AS funding_rows
        "#,
    )
    .bind(symbol)
    .bind(ts_bucket)
    .fetch_one(pool)
    .await
    .context("query indicator target presence")?;

    Ok(TargetPresence {
        snapshot_rows: row.get("snapshot_rows"),
        level_rows: row.get("level_rows"),
        liq_level_rows: row.get("liq_level_rows"),
        trade_flow_fut_rows: row.get("trade_flow_fut_rows"),
        trade_flow_spot_rows: row.get("trade_flow_spot_rows"),
        orderbook_rows: row.get("orderbook_rows"),
        funding_rows: row.get("funding_rows"),
    })
}
