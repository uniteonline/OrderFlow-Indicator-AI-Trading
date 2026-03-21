use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, NaiveDateTime, Timelike, Utc};
use market_data_ingestor::app::config::{load_config, ResolvedDatabaseConfig};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};

const DEFAULT_CONFIG_PATH: &str = "config/config.yaml";
const DEFAULT_SAMPLE_LIMIT: i64 = 20;

#[derive(Debug)]
struct Args {
    config_path: String,
    symbol: String,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    apply: bool,
    sample_limit: i64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse()?;
    ensure_minute_boundary(args.from, "--from")?;
    ensure_minute_boundary(args.to, "--to")?;
    if args.from >= args.to {
        bail!("--from must be earlier than --to");
    }

    let cfg = load_config(&args.config_path)
        .with_context(|| format!("load config {}", args.config_path))?;
    let symbol = if args.symbol.is_empty() {
        cfg.market_data.symbol.to_uppercase()
    } else {
        args.symbol.to_uppercase()
    };
    let pool = connect_md_pool(&cfg.database.resolve_md()).await?;

    let (candidate_count, min_ts, max_ts) =
        count_candidate_zero_minutes(&pool, &symbol, args.from, args.to).await?;
    let sample =
        load_candidate_sample(&pool, &symbol, args.from, args.to, args.sample_limit).await?;

    println!(
        "liq zero-minute backfill {}",
        if args.apply { "apply" } else { "dry-run" }
    );
    println!("symbol={symbol}");
    println!("range=[{}, {})", args.from, args.to);
    println!("candidate_count={candidate_count}");
    println!(
        "candidate_min_ts={}",
        min_ts
            .map(|ts| ts.to_string())
            .unwrap_or_else(|| "NULL".to_string())
    );
    println!(
        "candidate_max_ts={}",
        max_ts
            .map(|ts| ts.to_string())
            .unwrap_or_else(|| "NULL".to_string())
    );
    if sample.is_empty() {
        println!("sample_minutes=none");
    } else {
        println!(
            "sample_minutes={}",
            sample
                .iter()
                .map(|ts| ts.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    if !args.apply {
        println!("pass --apply to insert zero-valued md.agg.liq.1m rows for candidate minutes");
        return Ok(());
    }

    let inserted = insert_zero_minutes(&pool, &symbol, args.from, args.to).await?;
    println!("inserted_rows={inserted}");
    Ok(())
}

impl Args {
    fn parse() -> Result<Self> {
        let mut config_path = DEFAULT_CONFIG_PATH.to_string();
        let mut symbol = String::new();
        let mut from = None;
        let mut to = None;
        let mut apply = false;
        let mut sample_limit = DEFAULT_SAMPLE_LIMIT;

        let mut it = std::env::args().skip(1);
        while let Some(arg) = it.next() {
            match arg.as_str() {
                "--config" => config_path = take_value(&mut it, "--config")?,
                "--symbol" => symbol = take_value(&mut it, "--symbol")?,
                "--from" => {
                    let raw = take_value(&mut it, "--from")?;
                    from = Some(parse_utc_ts(&raw).with_context(|| format!("parse --from {raw}"))?);
                }
                "--to" => {
                    let raw = take_value(&mut it, "--to")?;
                    to = Some(parse_utc_ts(&raw).with_context(|| format!("parse --to {raw}"))?);
                }
                "--sample-limit" => {
                    sample_limit = take_value(&mut it, "--sample-limit")?
                        .parse()
                        .context("parse --sample-limit")?;
                }
                "--apply" => apply = true,
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
            to: to.ok_or_else(|| anyhow!("--to is required"))?,
            apply,
            sample_limit,
        })
    }
}

fn print_usage() {
    println!(
        "Usage: cargo run -p market_data_ingestor --bin backfill_liq_zero_minutes -- --from <UTC minute> --to <UTC minute> [--symbol BTCUSDT] [--sample-limit 20] [--apply] [--config config/config.yaml]"
    );
}

fn take_value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    args.next()
        .ok_or_else(|| anyhow!("{flag} requires a value"))
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

fn ensure_minute_boundary(ts: DateTime<Utc>, flag: &str) -> Result<()> {
    if ts.second() != 0 || ts.nanosecond() != 0 {
        bail!("{flag} must be aligned to the minute boundary");
    }
    Ok(())
}

async fn connect_md_pool(config: &ResolvedDatabaseConfig) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(4)
        .connect(&config.postgres_uri())
        .await
        .with_context(|| format!("connect md db {}", config.database))
}

async fn count_candidate_zero_minutes(
    pool: &PgPool,
    symbol: &str,
    from_ts: DateTime<Utc>,
    to_ts: DateTime<Utc>,
) -> Result<(i64, Option<DateTime<Utc>>, Option<DateTime<Utc>>)> {
    let row = sqlx::query(
        r#"
        WITH candidate AS (
            SELECT t.ts_bucket
            FROM md.agg_trade_1m t
            JOIN md.agg_orderbook_1m o
              ON o.market = t.market
             AND o.symbol = t.symbol
             AND o.ts_bucket = t.ts_bucket
            JOIN md.agg_funding_mark_1m f
              ON f.market = t.market
             AND f.symbol = t.symbol
             AND f.ts_bucket = t.ts_bucket
            LEFT JOIN md.agg_liq_1m l
              ON l.market = t.market
             AND l.symbol = t.symbol
             AND l.ts_bucket = t.ts_bucket
            WHERE t.market = 'futures'::cfg.market_type
              AND t.symbol = $1
              AND t.ts_bucket >= $2
              AND t.ts_bucket < $3
              AND l.ts_bucket IS NULL
        )
        SELECT count(*) AS candidate_count, min(ts_bucket) AS min_ts, max(ts_bucket) AS max_ts
        FROM candidate
        "#,
    )
    .bind(symbol)
    .bind(from_ts)
    .bind(to_ts)
    .fetch_one(pool)
    .await
    .context("count candidate liq zero minutes")?;

    Ok((
        row.get("candidate_count"),
        row.get("min_ts"),
        row.get("max_ts"),
    ))
}

async fn load_candidate_sample(
    pool: &PgPool,
    symbol: &str,
    from_ts: DateTime<Utc>,
    to_ts: DateTime<Utc>,
    limit: i64,
) -> Result<Vec<DateTime<Utc>>> {
    let rows = sqlx::query(
        r#"
        SELECT t.ts_bucket
        FROM md.agg_trade_1m t
        JOIN md.agg_orderbook_1m o
          ON o.market = t.market
         AND o.symbol = t.symbol
         AND o.ts_bucket = t.ts_bucket
        JOIN md.agg_funding_mark_1m f
          ON f.market = t.market
         AND f.symbol = t.symbol
         AND f.ts_bucket = t.ts_bucket
        LEFT JOIN md.agg_liq_1m l
          ON l.market = t.market
         AND l.symbol = t.symbol
         AND l.ts_bucket = t.ts_bucket
        WHERE t.market = 'futures'::cfg.market_type
          AND t.symbol = $1
          AND t.ts_bucket >= $2
          AND t.ts_bucket < $3
          AND l.ts_bucket IS NULL
        ORDER BY t.ts_bucket
        LIMIT $4
        "#,
    )
    .bind(symbol)
    .bind(from_ts)
    .bind(to_ts)
    .bind(limit)
    .fetch_all(pool)
    .await
    .context("load candidate liq zero minute sample")?;

    Ok(rows.into_iter().map(|row| row.get("ts_bucket")).collect())
}

async fn insert_zero_minutes(
    pool: &PgPool,
    symbol: &str,
    from_ts: DateTime<Utc>,
    to_ts: DateTime<Utc>,
) -> Result<u64> {
    let result = sqlx::query(
        r#"
        WITH candidate AS (
            SELECT t.ts_bucket
            FROM md.agg_trade_1m t
            JOIN md.agg_orderbook_1m o
              ON o.market = t.market
             AND o.symbol = t.symbol
             AND o.ts_bucket = t.ts_bucket
            JOIN md.agg_funding_mark_1m f
              ON f.market = t.market
             AND f.symbol = t.symbol
             AND f.ts_bucket = t.ts_bucket
            LEFT JOIN md.agg_liq_1m l
              ON l.market = t.market
             AND l.symbol = t.symbol
             AND l.ts_bucket = t.ts_bucket
            WHERE t.market = 'futures'::cfg.market_type
              AND t.symbol = $1
              AND t.ts_bucket >= $2
              AND t.ts_bucket < $3
              AND l.ts_bucket IS NULL
        )
        INSERT INTO md.agg_liq_1m (
            ts_event, ts_bucket, market, symbol, source_kind, stream_name,
            chunk_start_ts, chunk_end_ts, source_event_count, force_liq_levels, payload_json
        )
        SELECT
            c.ts_bucket + interval '1 minute' - interval '1 millisecond' AS ts_event,
            c.ts_bucket,
            'futures'::cfg.market_type,
            $1,
            'derived'::cfg.source_type,
            'agg.liq.1m',
            c.ts_bucket,
            c.ts_bucket + interval '1 minute' - interval '1 millisecond',
            0,
            '[]'::jsonb,
            jsonb_build_object(
                'agg_mode', 'canonical_1m',
                'source_start_ts', to_char(c.ts_bucket AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),
                'source_end_ts', to_char((c.ts_bucket + interval '1 minute' - interval '1 millisecond') AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')
            )
        FROM candidate c
        ON CONFLICT (market, symbol, stream_name, ts_bucket, chunk_start_ts, chunk_end_ts)
        DO NOTHING
        "#,
    )
    .bind(symbol)
    .bind(from_ts)
    .bind(to_ts)
    .execute(pool)
    .await
    .context("insert zero md.agg.liq.1m minutes")?;

    Ok(result.rows_affected())
}
