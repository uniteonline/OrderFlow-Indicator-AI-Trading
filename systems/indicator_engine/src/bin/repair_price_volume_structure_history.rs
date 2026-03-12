use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, TimeZone, Utc};
use indicator_engine::app::bootstrap::{build_db_pool, load_config};
use indicator_engine::app::runtime::build_indicator_runtime_options;
use indicator_engine::indicators::context::{DivergenceSigTestMode, IndicatorContext};
use indicator_engine::indicators::i01_price_volume_structure::I01PriceVolumeStructure;
use indicator_engine::indicators::indicator_trait::Indicator;
use indicator_engine::ingest::decoder::MarketKind;
use indicator_engine::runtime::state_store::{
    price_to_tick, LevelAgg, MinuteHistory, MinuteWindowData,
};
use indicator_engine::storage::level_writer::LevelWriter;
use indicator_engine::storage::snapshot_writer::SnapshotWriter;
use serde_json::Value;
use sqlx::{PgPool, Row};
use std::collections::BTreeMap;

const MAX_PVS_LOOKBACK_MINUTES: i64 = 1440;

#[derive(Debug, Clone)]
struct TradeMinuteRow {
    ts_bucket: DateTime<Utc>,
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse()?;
    let config = load_config("config/config.yaml").context("load config/config.yaml")?;
    let runtime_options = build_indicator_runtime_options(&config);
    let pool = build_db_pool(&config).await?;
    let writer = SnapshotWriter::new(pool.clone());
    let level_writer = LevelWriter::new(pool.clone());

    let symbol = args
        .symbol
        .unwrap_or_else(|| config.indicator.symbol.clone());
    let from_ts = args.from;
    let to_ts = match args.to {
        Some(ts) => ts,
        None => latest_progress_ts(&pool, &symbol)
            .await?
            .ok_or_else(|| anyhow!("missing feat.indicator_progress row for symbol={symbol}"))?,
    };
    let history_front = args.history_front;

    if to_ts < from_ts {
        return Err(anyhow!("to_ts < from_ts"));
    }
    if history_front.map(|front| front > from_ts).unwrap_or(false) {
        return Err(anyhow!("history_front > from_ts"));
    }

    let history_start =
        history_front.unwrap_or(from_ts - Duration::minutes(MAX_PVS_LOOKBACK_MINUTES - 1));
    let trade_rows = load_trade_rows(&pool, &symbol, history_start, to_ts).await?;
    let sequence = build_contiguous_trade_history(history_start, to_ts, &trade_rows);

    let pvs = I01PriceVolumeStructure;
    let mut rewritten = 0usize;
    for idx in minutes_between(history_start, to_ts) {
        let ts = history_start + Duration::minutes(idx as i64);
        if ts < from_ts {
            continue;
        }
        let lookback_floor = idx.saturating_sub((MAX_PVS_LOOKBACK_MINUTES - 1) as usize);
        let history_floor = history_front
            .map(|front| (front - history_start).num_minutes().max(0) as usize)
            .unwrap_or(0);
        let start_idx = lookback_floor.max(history_floor);
        let trade_tail = sequence[start_idx..=idx].to_vec();
        let current = minute_window_from_history(trade_tail.last().expect("tail current"));
        let ctx = build_pvs_context(&symbol, ts, current, trade_tail, &runtime_options);
        let computation = pvs.evaluate(&ctx);
        let row = computation
            .snapshot
            .ok_or_else(|| anyhow!("missing PVS snapshot at ts={ts}"))?;
        let level_rows = computation.level_rows;
        if args.apply {
            delete_pvs_rows(&pool, &symbol, ts).await?;
            writer
                .write_snapshots(ts, &symbol, &[row])
                .await
                .with_context(|| format!("rewrite price_volume_structure snapshot ts={ts}"))?;
            level_writer
                .write_indicator_levels(ts, &symbol, &level_rows)
                .await
                .with_context(|| format!("rewrite price_volume_structure levels ts={ts}"))?;
        }
        rewritten += 1;
    }

    println!(
        "price_volume_structure repair {} for symbol={} range=[{}, {}] history_front={} rewritten_rows={}",
        if args.apply { "applied" } else { "dry-run" },
        symbol,
        from_ts,
        to_ts,
        history_front
            .map(|ts| ts.to_string())
            .unwrap_or_else(|| "auto".to_string()),
        rewritten
    );
    Ok(())
}

async fn delete_pvs_rows(pool: &PgPool, symbol: &str, ts: DateTime<Utc>) -> Result<()> {
    sqlx::query(
        r#"
        DELETE FROM feat.indicator_snapshot
        WHERE symbol = $1
          AND ts_snapshot = $2
          AND indicator_code = 'price_volume_structure'
        "#,
    )
    .bind(symbol.to_uppercase())
    .bind(ts)
    .execute(pool)
    .await
    .context("delete existing price_volume_structure snapshot")?;

    sqlx::query(
        r#"
        DELETE FROM feat.indicator_level_value
        WHERE symbol = $1
          AND ts_snapshot = $2
          AND indicator_code = 'price_volume_structure'
        "#,
    )
    .bind(symbol.to_uppercase())
    .bind(ts)
    .execute(pool)
    .await
    .context("delete existing price_volume_structure levels")?;

    Ok(())
}

#[derive(Debug)]
struct Args {
    symbol: Option<String>,
    from: DateTime<Utc>,
    to: Option<DateTime<Utc>>,
    history_front: Option<DateTime<Utc>>,
    apply: bool,
}

impl Args {
    fn parse() -> Result<Self> {
        let mut symbol = None;
        let mut from = None;
        let mut to = None;
        let mut history_front = None;
        let mut apply = false;

        let mut it = std::env::args().skip(1);
        while let Some(arg) = it.next() {
            match arg.as_str() {
                "--symbol" => {
                    symbol = Some(
                        it.next()
                            .ok_or_else(|| anyhow!("--symbol requires value"))?,
                    );
                }
                "--from" => {
                    let raw = it.next().ok_or_else(|| anyhow!("--from requires value"))?;
                    from = Some(parse_utc_ts(&raw).with_context(|| format!("parse --from {raw}"))?);
                }
                "--to" => {
                    let raw = it.next().ok_or_else(|| anyhow!("--to requires value"))?;
                    to = Some(parse_utc_ts(&raw).with_context(|| format!("parse --to {raw}"))?);
                }
                "--history-front" => {
                    let raw = it
                        .next()
                        .ok_or_else(|| anyhow!("--history-front requires value"))?;
                    history_front = Some(
                        parse_utc_ts(&raw)
                            .with_context(|| format!("parse --history-front {raw}"))?,
                    );
                }
                "--apply" => apply = true,
                other => return Err(anyhow!("unknown arg: {other}")),
            }
        }

        Ok(Self {
            symbol,
            from: from.ok_or_else(|| anyhow!("--from is required"))?,
            to,
            history_front,
            apply,
        })
    }
}

fn parse_utc_ts(raw: &str) -> Result<DateTime<Utc>> {
    if let Ok(ts) = DateTime::parse_from_rfc3339(raw) {
        return Ok(ts.with_timezone(&Utc));
    }
    if let Ok(ts) = Utc.datetime_from_str(raw, "%Y-%m-%d %H:%M:%S") {
        return Ok(ts);
    }
    if let Ok(ts) = Utc.datetime_from_str(raw, "%Y-%m-%dT%H:%M:%S") {
        return Ok(ts);
    }
    Err(anyhow!("unsupported timestamp format: {raw}"))
}

async fn latest_progress_ts(pool: &PgPool, symbol: &str) -> Result<Option<DateTime<Utc>>> {
    let row = sqlx::query(
        r#"
        SELECT last_success_ts
        FROM feat.indicator_progress
        WHERE symbol = $1
        "#,
    )
    .bind(symbol.to_uppercase())
    .fetch_optional(pool)
    .await
    .context("query feat.indicator_progress")?;
    Ok(row.map(|row| row.get("last_success_ts")))
}

async fn load_trade_rows(
    pool: &PgPool,
    symbol: &str,
    from_ts: DateTime<Utc>,
    to_ts: DateTime<Utc>,
) -> Result<BTreeMap<i64, TradeMinuteRow>> {
    let rows = sqlx::query(
        r#"
        SELECT
            ts_bucket,
            trade_count,
            buy_qty,
            sell_qty,
            buy_notional,
            sell_notional,
            first_price,
            last_price,
            high_price,
            low_price,
            profile_levels
        FROM md.agg_trade_1m
        WHERE symbol = $1
          AND market = 'futures'
          AND ts_bucket >= $2
          AND ts_bucket <= $3
        ORDER BY ts_bucket ASC
        "#,
    )
    .bind(symbol.to_uppercase())
    .bind(from_ts)
    .bind(to_ts)
    .fetch_all(pool)
    .await
    .context("fetch canonical md.agg_trade_1m rows")?;

    let mut out = BTreeMap::new();
    for row in rows {
        let ts_bucket: DateTime<Utc> = row.get("ts_bucket");
        let profile_json: Value = row.get("profile_levels");
        out.insert(
            ts_bucket.timestamp(),
            TradeMinuteRow {
                ts_bucket,
                trade_count: row.get("trade_count"),
                buy_qty: row.get("buy_qty"),
                sell_qty: row.get("sell_qty"),
                buy_notional: row.get("buy_notional"),
                sell_notional: row.get("sell_notional"),
                first_price: row.get("first_price"),
                last_price: row.get("last_price"),
                high_price: row.get("high_price"),
                low_price: row.get("low_price"),
                profile: parse_profile_levels(&profile_json),
            },
        );
    }
    Ok(out)
}

fn parse_profile_levels(value: &Value) -> BTreeMap<i64, LevelAgg> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let items = entry.as_array()?;
            let price = items.first()?.as_f64()?;
            let buy_qty = items.get(1).and_then(Value::as_f64).unwrap_or(0.0);
            let sell_qty = items.get(2).and_then(Value::as_f64).unwrap_or(0.0);
            Some((price_to_tick(price), LevelAgg { buy_qty, sell_qty }))
        })
        .collect()
}

fn build_contiguous_trade_history(
    from_ts: DateTime<Utc>,
    to_ts: DateTime<Utc>,
    rows: &BTreeMap<i64, TradeMinuteRow>,
) -> Vec<MinuteHistory> {
    let mut out = Vec::new();
    let mut ts = from_ts;
    while ts <= to_ts {
        let history = rows
            .get(&ts.timestamp())
            .map(history_from_trade_row)
            .unwrap_or_else(|| empty_history(ts));
        out.push(history);
        ts += Duration::minutes(1);
    }
    out
}

fn empty_history(ts_bucket: DateTime<Utc>) -> MinuteHistory {
    MinuteHistory {
        ts_bucket,
        market: MarketKind::Futures,
        open_price: None,
        high_price: None,
        low_price: None,
        close_price: None,
        last_price: None,
        buy_qty: 0.0,
        sell_qty: 0.0,
        total_qty: 0.0,
        total_notional: 0.0,
        delta: 0.0,
        relative_delta: 0.0,
        force_liq: BTreeMap::new(),
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
        profile: BTreeMap::new(),
    }
}

fn history_from_trade_row(row: &TradeMinuteRow) -> MinuteHistory {
    let total_qty = row.buy_qty + row.sell_qty;
    let total_notional = row.buy_notional + row.sell_notional;
    let delta = row.buy_qty - row.sell_qty;
    MinuteHistory {
        ts_bucket: row.ts_bucket,
        market: MarketKind::Futures,
        open_price: row.first_price,
        high_price: row.high_price,
        low_price: row.low_price,
        close_price: row.last_price,
        last_price: row.last_price,
        buy_qty: row.buy_qty,
        sell_qty: row.sell_qty,
        total_qty,
        total_notional,
        delta,
        relative_delta: if total_qty > 0.0 {
            delta / total_qty
        } else {
            0.0
        },
        force_liq: BTreeMap::new(),
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
        avwap_minute: (total_qty > 0.0).then_some(total_notional / total_qty),
        whale_trade_count: 0,
        whale_buy_count: 0,
        whale_sell_count: 0,
        whale_notional_total: 0.0,
        whale_notional_buy: 0.0,
        whale_notional_sell: 0.0,
        whale_qty_eth_total: 0.0,
        whale_qty_eth_buy: 0.0,
        whale_qty_eth_sell: 0.0,
        profile: row.profile.clone(),
    }
}

fn minute_window_from_history(history: &MinuteHistory) -> MinuteWindowData {
    let mut window = MinuteWindowData::empty(MarketKind::Futures, history.ts_bucket);
    window.trade_count = history
        .profile
        .values()
        .map(|level| {
            let total = level.total();
            if total > 0.0 {
                1
            } else {
                0
            }
        })
        .sum();
    window.buy_qty = history.buy_qty;
    window.sell_qty = history.sell_qty;
    window.total_qty = history.total_qty;
    window.buy_notional = 0.0;
    window.sell_notional = 0.0;
    window.total_notional = history.total_notional;
    window.delta = history.delta;
    window.relative_delta = history.relative_delta;
    window.first_price = history.open_price;
    window.last_price = history.last_price;
    window.high_price = history.high_price;
    window.low_price = history.low_price;
    window.profile = history.profile.clone();
    window.avwap = history.avwap_minute;
    window
}

fn build_pvs_context(
    symbol: &str,
    ts_bucket: DateTime<Utc>,
    futures: MinuteWindowData,
    trade_history_futures: Vec<MinuteHistory>,
    options: &indicator_engine::indicators::context::IndicatorRuntimeOptions,
) -> IndicatorContext {
    IndicatorContext {
        ts_bucket,
        symbol: symbol.to_string(),
        futures,
        spot: MinuteWindowData::empty(MarketKind::Spot, ts_bucket),
        history_futures: Vec::new(),
        history_spot: Vec::new(),
        trade_history_futures,
        trade_history_spot: Vec::new(),
        latest_mark: None,
        latest_funding: None,
        funding_changes_in_window: Vec::new(),
        funding_points_in_window: Vec::new(),
        mark_points_in_window: Vec::new(),
        funding_changes_recent: Vec::new(),
        funding_points_recent: Vec::new(),
        mark_points_recent: Vec::new(),
        whale_threshold_usdt: options.whale_threshold_usdt,
        kline_history_bars_1m: options.kline_history_bars_1m,
        kline_history_bars_15m: options.kline_history_bars_15m,
        kline_history_bars_4h: options.kline_history_bars_4h,
        kline_history_bars_1d: options.kline_history_bars_1d,
        kline_history_fill_1d_from_db: options.kline_history_fill_1d_from_db,
        fvg_windows: options.fvg_windows.clone(),
        fvg_fill_from_db: options.fvg_fill_from_db,
        fvg_db_bars_1h: options.fvg_db_bars_1h,
        fvg_db_bars_4h: options.fvg_db_bars_4h,
        fvg_db_bars_1d: options.fvg_db_bars_1d,
        fvg_epsilon_gap_ticks: options.fvg_epsilon_gap_ticks,
        fvg_atr_lookback: options.fvg_atr_lookback,
        fvg_min_body_ratio: options.fvg_min_body_ratio,
        fvg_min_impulse_atr_ratio: options.fvg_min_impulse_atr_ratio,
        fvg_min_gap_atr_ratio: options.fvg_min_gap_atr_ratio,
        fvg_max_gap_atr_ratio: options.fvg_max_gap_atr_ratio,
        fvg_mitigated_fill_threshold: options.fvg_mitigated_fill_threshold,
        fvg_invalid_close_bars: options.fvg_invalid_close_bars,
        kline_history_futures_1h_db: Vec::new(),
        kline_history_futures_4h_db: Vec::new(),
        kline_history_futures_1d_db: Vec::new(),
        kline_history_spot_4h_db: Vec::new(),
        kline_history_spot_1d_db: Vec::new(),
        tpo_rows_nb: options.tpo_rows_nb,
        tpo_value_area_pct: options.tpo_value_area_pct,
        tpo_session_windows: options.tpo_session_windows.clone(),
        tpo_ib_minutes: options.tpo_ib_minutes,
        tpo_dev_output_windows: options.tpo_dev_output_windows.clone(),
        rvwap_windows: options.rvwap_windows.clone(),
        rvwap_output_windows: options.rvwap_output_windows.clone(),
        rvwap_min_samples: options.rvwap_min_samples,
        high_volume_pulse_z_windows: options.high_volume_pulse_z_windows.clone(),
        high_volume_pulse_summary_windows: options.high_volume_pulse_summary_windows.clone(),
        high_volume_pulse_min_samples: options.high_volume_pulse_min_samples,
        ema_base_periods: options.ema_base_periods.clone(),
        ema_htf_periods: options.ema_htf_periods.clone(),
        ema_htf_windows: options.ema_htf_windows.clone(),
        ema_output_windows: options.ema_output_windows.clone(),
        ema_fill_from_db: options.ema_fill_from_db,
        ema_db_bars_4h: options.ema_db_bars_4h,
        ema_db_bars_1d: options.ema_db_bars_1d,
        divergence_sig_test_mode: match options.divergence_sig_test_mode {
            DivergenceSigTestMode::Threshold => DivergenceSigTestMode::Threshold,
            DivergenceSigTestMode::BlockBootstrap => DivergenceSigTestMode::BlockBootstrap,
        },
        divergence_bootstrap_b: options.divergence_bootstrap_b,
        divergence_bootstrap_block_len: options.divergence_bootstrap_block_len,
        divergence_p_value_threshold: options.divergence_p_value_threshold,
        window_codes: options.window_codes.clone(),
    }
}

fn minutes_between(
    from_ts: DateTime<Utc>,
    to_ts: DateTime<Utc>,
) -> std::ops::RangeInclusive<usize> {
    let minutes = (to_ts - from_ts).num_minutes();
    0..=(minutes as usize)
}
