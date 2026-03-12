use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Timelike, Utc};
use market_data_ingestor::aggregate::minute_agg::{
    AggregateLane, MinuteBatchAggregator, DEFAULT_WHALE_THRESHOLD_USDT,
};
use market_data_ingestor::app::config::load_config;
use market_data_ingestor::normalize::NormalizedMdEvent;
use market_data_ingestor::observability::tracing as obs_tracing;
use market_data_ingestor::sinks::md_db_writer::MdDbWriter;
use serde::Deserialize;
use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, PgPool, Postgres, Row, Transaction};
use std::collections::{BTreeSet, HashSet};
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use tracing::info;

const DEFAULT_CONFIG_PATH: &str = "config/config.yaml";
const DEFAULT_BATCH_SIZE: usize = 50_000;

#[derive(Debug)]
struct CliArgs {
    config_path: String,
    symbol: String,
    market: String,
    from_ts: DateTime<Utc>,
    to_ts: DateTime<Utc>,
    raw_root: String,
    batch_size: usize,
    apply: bool,
    rewind_indicators: bool,
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MinuteKey {
    market: String,
    ts_bucket: DateTime<Utc>,
}

#[tokio::main]
async fn main() -> Result<()> {
    obs_tracing::init();
    let args = parse_args()?;
    validate_minute_boundaries(args.from_ts, args.to_ts)?;

    let cfg = load_config(&args.config_path)
        .with_context(|| format!("load config {}", args.config_path))?;
    let pool = connect_md_pool(&cfg.database.resolve_md()).await?;

    let raw_events = load_raw_trade_events(
        &args.raw_root,
        &args.symbol,
        &args.market,
        args.from_ts,
        args.to_ts,
    )?;
    if raw_events.is_empty() {
        bail!(
            "no raw md.trade events found for symbol={} market={} range=[{}, {}) under {}",
            args.symbol,
            args.market,
            args.from_ts,
            args.to_ts,
            args.raw_root
        );
    }

    let rebuilt = rebuild_trade_minutes(&raw_events, args.to_ts, args.batch_size);
    if rebuilt.is_empty() {
        bail!("raw trade events loaded, but rebuild emitted zero md.agg.trade.1m rows; aborting");
    }

    let rebuilt_keys = collect_rebuilt_keys(&rebuilt)?;
    let existing_keys = load_existing_trade_minute_keys(
        &pool,
        &args.symbol,
        &args.market,
        args.from_ts,
        args.to_ts,
    )
    .await?;
    let missing_existing = existing_keys
        .difference(&rebuilt_keys)
        .cloned()
        .collect::<BTreeSet<_>>();
    if !missing_existing.is_empty() {
        let sample = missing_existing
            .iter()
            .take(10)
            .map(|key| format!("{}@{}", key.market, key.ts_bucket))
            .collect::<Vec<_>>()
            .join(", ");
        bail!(
            "raw coverage is incomplete: {} existing trade minutes are missing from rebuilt output; sample={}",
            missing_existing.len(),
            sample
        );
    }

    info!(
        symbol = %args.symbol,
        market = %args.market,
        from_ts = %args.from_ts,
        to_ts = %args.to_ts,
        raw_trade_events = raw_events.len(),
        rebuilt_trade_minutes = rebuilt.len(),
        existing_trade_minutes = existing_keys.len(),
        apply = args.apply,
        rewind_indicators = args.rewind_indicators,
        "historical trade 1m rebuild plan ready"
    );

    if !args.apply {
        println!(
            "dry-run: loaded {} raw trades and rebuilt {} canonical md.agg.trade.1m rows for {} {} [{}, {})",
            raw_events.len(),
            rebuilt.len(),
            args.market,
            args.symbol,
            args.from_ts,
            args.to_ts
        );
        println!("pass --apply to write rebuilt rows into md.agg_trade_1m");
        println!(
            "pass --rewind-indicators together with --apply to delete feat.indicator_snapshot from {} onward and rewind feat.indicator_progress",
            args.from_ts
        );
        return Ok(());
    }

    let writer = MdDbWriter::new(pool.clone());
    writer
        .write_md_events_batch(&rebuilt)
        .await
        .context("write rebuilt md.agg.trade.1m rows")?;

    let mut tx = pool
        .begin()
        .await
        .context("begin trade rebuild cleanup tx")?;
    let purged = purge_non_canonical_trade_rows(
        &mut tx,
        &args.symbol,
        &args.market,
        args.from_ts,
        args.to_ts,
    )
    .await?;

    if args.rewind_indicators {
        let deleted_snapshots =
            delete_indicator_artifacts_from(&mut tx, &args.symbol, args.from_ts).await?;
        rewind_indicator_progress(&mut tx, &args.symbol, args.from_ts).await?;
        info!(
            symbol = %args.symbol,
            deleted_indicator_artifacts = deleted_snapshots,
            rewind_from_ts = %args.from_ts,
            "indicator history rewound after trade rebuild"
        );
    }

    tx.commit()
        .await
        .context("commit trade rebuild cleanup tx")?;

    let final_canonical_count =
        count_canonical_trade_rows(&pool, &args.symbol, &args.market, args.from_ts, args.to_ts)
            .await?;

    println!(
        "applied: wrote/upserted {} canonical md.agg.trade.1m rows, purged {} legacy chunk rows, final canonical rows in range={}",
        rebuilt.len(),
        purged,
        final_canonical_count
    );
    if args.rewind_indicators {
        println!(
            "next step: restart indicator_engine so it replays from rebuilt md.agg.trade.1m history"
        );
    }

    Ok(())
}

fn parse_args() -> Result<CliArgs> {
    let mut config_path = DEFAULT_CONFIG_PATH.to_string();
    let mut symbol = None;
    let mut market = "all".to_string();
    let mut from_ts = None;
    let mut to_ts = None;
    let mut raw_root = None;
    let mut batch_size = DEFAULT_BATCH_SIZE;
    let mut apply = false;
    let mut rewind_indicators = false;

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
            "--raw-root" => raw_root = Some(take_arg_value(&mut args, "--raw-root")?),
            "--batch-size" => {
                let raw = take_arg_value(&mut args, "--batch-size")?;
                batch_size = raw
                    .parse::<usize>()
                    .with_context(|| format!("parse --batch-size value {raw}"))?;
            }
            "--apply" => apply = true,
            "--rewind-indicators" => rewind_indicators = true,
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
    let raw_root = raw_root.ok_or_else(|| anyhow!("--raw-root is required"))?;

    if rewind_indicators && !apply {
        bail!("--rewind-indicators requires --apply");
    }
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
        raw_root,
        batch_size,
        apply,
        rewind_indicators,
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
        "usage: cargo run -p market_data_ingestor --bin rebuild_trade_1m_from_raw -- [--config PATH] --symbol SYMBOL [--market all|futures|spot] --from RFC3339 --to RFC3339 --raw-root PATH [--batch-size N] [--apply] [--rewind-indicators]"
    );
}

fn validate_minute_boundaries(from_ts: DateTime<Utc>, to_ts: DateTime<Utc>) -> Result<()> {
    for (name, ts) in [("from", from_ts), ("to", to_ts)] {
        if ts.second() != 0 || ts.nanosecond() != 0 {
            bail!("--{name} must be aligned to an exact minute boundary, got {ts}");
        }
    }
    Ok(())
}

async fn connect_md_pool(
    config: &market_data_ingestor::app::config::ResolvedDatabaseConfig,
) -> Result<PgPool> {
    let mut pool_options = PgPoolOptions::new()
        .max_connections(
            config
                .pool
                .as_ref()
                .and_then(|p| p.max_connections)
                .unwrap_or(4),
        )
        .min_connections(
            config
                .pool
                .as_ref()
                .and_then(|p| p.min_connections)
                .unwrap_or(1),
        )
        .test_before_acquire(
            config
                .pool
                .as_ref()
                .and_then(|p| p.test_before_acquire)
                .unwrap_or(true),
        );

    if let Some(secs) = config.connect_timeout_secs {
        pool_options = pool_options.acquire_timeout(std::time::Duration::from_secs(secs));
    }

    let pool = pool_options
        .connect(&config.postgres_uri())
        .await
        .context("connect md postgres")?;
    sqlx::query("SELECT 1")
        .execute(&pool)
        .await
        .context("postgres health query")?;
    Ok(pool)
}

fn load_raw_trade_events(
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
            read_raw_trade_file(
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

fn read_raw_trade_file(
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
    let reader = BufReader::new(file);

    for (line_no, line) in reader.lines().enumerate() {
        let line = line.with_context(|| {
            format!(
                "read raw cold-store line {} from {}",
                line_no + 1,
                file_path.display()
            )
        })?;
        if line.trim().is_empty() {
            continue;
        }
        let row: RawColdRow = serde_json::from_str(&line).with_context(|| {
            format!(
                "decode raw cold-store line {} from {}",
                line_no + 1,
                file_path.display()
            )
        })?;
        if row.msg_type != "md.trade" {
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
        if event_ts < from_ts || event_ts >= to_ts {
            continue;
        }

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

fn rebuild_trade_minutes(
    raw_events: &[NormalizedMdEvent],
    to_ts: DateTime<Utc>,
    batch_size: usize,
) -> Vec<NormalizedMdEvent> {
    let mut aggregator =
        MinuteBatchAggregator::new(AggregateLane::Trade, DEFAULT_WHALE_THRESHOLD_USDT);
    let mut out = Vec::new();

    for batch in raw_events.chunks(batch_size.max(1)) {
        out.extend(aggregator.aggregate_batch(batch));
    }
    out.extend(aggregator.flush_closed_minutes(to_ts));
    out.retain(|event| event.msg_type == "md.agg.trade.1m");
    out
}

fn collect_rebuilt_keys(events: &[NormalizedMdEvent]) -> Result<HashSet<MinuteKey>> {
    let mut out = HashSet::with_capacity(events.len());
    for event in events {
        let ts_bucket = event
            .data
            .get("ts_bucket")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("rebuilt md.agg.trade.1m missing ts_bucket"))?;
        let ts_bucket = parse_rfc3339_utc(ts_bucket)?;
        out.insert(MinuteKey {
            market: event.market.clone(),
            ts_bucket,
        });
    }
    Ok(out)
}

async fn load_existing_trade_minute_keys(
    pool: &PgPool,
    symbol: &str,
    market: &str,
    from_ts: DateTime<Utc>,
    to_ts: DateTime<Utc>,
) -> Result<HashSet<MinuteKey>> {
    let rows = sqlx::query(
        r#"
        SELECT market::text AS market, ts_bucket
        FROM md.agg_trade_1m
        WHERE symbol = $1
          AND ts_bucket >= $2
          AND ts_bucket < $3
          AND ($4 = 'all' OR market::text = $4)
        GROUP BY market::text, ts_bucket
        "#,
    )
    .bind(symbol)
    .bind(from_ts)
    .bind(to_ts)
    .bind(market)
    .fetch_all(pool)
    .await
    .context("load existing trade minute keys")?;

    let mut out = HashSet::with_capacity(rows.len());
    for row in rows {
        out.insert(MinuteKey {
            market: row.get("market"),
            ts_bucket: row.get("ts_bucket"),
        });
    }
    Ok(out)
}

async fn purge_non_canonical_trade_rows(
    tx: &mut Transaction<'_, Postgres>,
    symbol: &str,
    market: &str,
    from_ts: DateTime<Utc>,
    to_ts: DateTime<Utc>,
) -> Result<u64> {
    let result = sqlx::query(
        r#"
        DELETE FROM md.agg_trade_1m
        WHERE symbol = $1
          AND ts_bucket >= $2
          AND ts_bucket < $3
          AND ($4 = 'all' OR market::text = $4)
          AND (
            chunk_start_ts <> ts_bucket
            OR chunk_end_ts <> (ts_bucket + INTERVAL '1 minute' - INTERVAL '1 millisecond')
          )
        "#,
    )
    .bind(symbol)
    .bind(from_ts)
    .bind(to_ts)
    .bind(market)
    .execute(&mut **tx)
    .await
    .context("purge legacy trade chunk rows")?;
    Ok(result.rows_affected())
}

async fn count_canonical_trade_rows(
    pool: &PgPool,
    symbol: &str,
    market: &str,
    from_ts: DateTime<Utc>,
    to_ts: DateTime<Utc>,
) -> Result<i64> {
    let row = sqlx::query(
        r#"
        SELECT count(*) AS cnt
        FROM md.agg_trade_1m
        WHERE symbol = $1
          AND ts_bucket >= $2
          AND ts_bucket < $3
          AND ($4 = 'all' OR market::text = $4)
          AND chunk_start_ts = ts_bucket
          AND chunk_end_ts = (ts_bucket + INTERVAL '1 minute' - INTERVAL '1 millisecond')
        "#,
    )
    .bind(symbol)
    .bind(from_ts)
    .bind(to_ts)
    .bind(market)
    .fetch_one(pool)
    .await
    .context("count canonical trade rows")?;
    Ok(row.get("cnt"))
}

async fn delete_indicator_artifacts_from(
    tx: &mut Transaction<'_, Postgres>,
    symbol: &str,
    from_ts: DateTime<Utc>,
) -> Result<u64> {
    let mut deleted = 0_u64;

    let result = sqlx::query(
        r#"
        DELETE FROM feat.indicator_snapshot
        WHERE symbol = $1
          AND ts_snapshot >= $2
        "#,
    )
    .bind(symbol)
    .bind(from_ts)
    .execute(&mut **tx)
    .await
    .context("delete indicator snapshots from repair start")?;
    deleted += result.rows_affected();

    for (table, ts_col) in [
        ("feat.indicator_level_value", "ts_snapshot"),
        ("feat.liquidation_density_level", "ts_snapshot"),
        ("feat.trade_flow_feature", "ts_bucket"),
        ("feat.orderbook_feature", "ts_bucket"),
        ("feat.funding_feature", "ts_bucket"),
        ("feat.avwap_feature", "ts_bucket"),
        ("feat.cvd_pack", "ts_bucket"),
        ("feat.whale_trade_rollup", "ts_bucket"),
        ("feat.funding_change_event", "ts_change"),
    ] {
        let query = format!("DELETE FROM {table} WHERE symbol = $1 AND {ts_col} >= $2",);
        let result = sqlx::query(&query)
            .bind(symbol)
            .bind(from_ts)
            .execute(&mut **tx)
            .await
            .with_context(|| format!("delete {table} from repair start"))?;
        deleted += result.rows_affected();
    }

    for table in [
        "evt.indicator_event",
        "evt.divergence_event",
        "evt.absorption_event",
        "evt.initiation_event",
        "evt.exhaustion_event",
    ] {
        let query = format!(
            "DELETE FROM {table} WHERE symbol = $1 AND COALESCE(event_available_ts, ts_event_end, ts_event_start) >= $2",
        );
        let result = sqlx::query(&query)
            .bind(symbol)
            .bind(from_ts)
            .execute(&mut **tx)
            .await
            .with_context(|| format!("delete {table} from repair start"))?;
        deleted += result.rows_affected();
    }

    Ok(deleted)
}

async fn rewind_indicator_progress(
    tx: &mut Transaction<'_, Postgres>,
    symbol: &str,
    from_ts: DateTime<Utc>,
) -> Result<()> {
    let rewind_ts = from_ts - ChronoDuration::minutes(1);
    sqlx::query(
        r#"
        INSERT INTO feat.indicator_progress (symbol, last_success_ts)
        VALUES ($1, $2)
        ON CONFLICT (symbol)
        DO UPDATE SET
            last_success_ts = LEAST(feat.indicator_progress.last_success_ts, EXCLUDED.last_success_ts),
            updated_at = now()
        "#,
    )
    .bind(symbol)
    .bind(rewind_ts)
    .execute(&mut **tx)
    .await
    .context("rewind indicator progress")?;
    Ok(())
}
