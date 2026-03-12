use crate::app::bootstrap::AppContext;
use crate::exchange::binance::rest::client::BinanceRestClient;
use crate::normalize::{funding_rate_normalizer, mark_price_normalizer, NormalizedMdEvent};
use crate::observability::metrics::AppMetrics;
use crate::pipelines::persist_async;
use crate::sinks::{
    md_db_writer::MdDbWriter, mq_publisher::MqPublisher, ops_db_writer::OpsDbWriter,
    outbox_writer::OutboxWriter,
};
use crate::state::checkpoints;
use anyhow::Result;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{error, info, warn};

const SYMBOL: &str = "ETHUSDT";
const FUNDING_BACKFILL_INTERVAL_SECS: u64 = 60;
const PREMIUM_INDEX_BOOTSTRAP_INTERVAL_SECS: u64 = 20;
const EXCHANGE_INFO_REFRESH_INTERVAL_SECS: u64 = 86_400;

pub async fn run_funding_rate_backfill_loop(
    ctx: Arc<AppContext>,
    rest_client: Arc<BinanceRestClient>,
    db_writer: Arc<MdDbWriter>,
    ops_writer: Arc<OpsDbWriter>,
    publisher: Arc<MqPublisher>,
    outbox_writer: Arc<OutboxWriter>,
    metrics: Arc<AppMetrics>,
) -> Result<()> {
    let mut last_funding_time: Option<i64> = None;
    let mut last_premium_time: Option<i64> = None;
    let mut spot_exchange_info_bootstrapped = false;
    let mut futures_exchange_info_bootstrapped = false;
    match checkpoints::load_checkpoint_seeds(&ctx.ops_db_pool, "futures", SYMBOL).await {
        Ok(seeds) => {
            for seed in &seeds {
                let Some(last_ts) = seed.last_event_ts else {
                    continue;
                };
                let ts_ms = last_ts.timestamp_millis();
                match seed.stream_name.as_str() {
                    "fapi/v1/fundingRate" => {
                        let current = last_funding_time.unwrap_or(i64::MIN);
                        if ts_ms > current {
                            last_funding_time = Some(ts_ms);
                        }
                    }
                    "fapi/v1/premiumIndex" => {
                        let current = last_premium_time.unwrap_or(i64::MIN);
                        if ts_ms > current {
                            last_premium_time = Some(ts_ms);
                        }
                    }
                    _ => {}
                }
            }
            info!(
                market = "futures",
                symbol = SYMBOL,
                seed_count = seeds.len(),
                last_funding_time = ?last_funding_time,
                last_premium_time = ?last_premium_time,
                "backfill scheduler seeded from checkpoints"
            );
        }
        Err(err) => {
            warn!(error = %err, market = "futures", symbol = SYMBOL, "load checkpoint seeds for scheduler failed");
        }
    }

    let mut funding_ticker = interval(Duration::from_secs(FUNDING_BACKFILL_INTERVAL_SECS));
    funding_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut premium_ticker = interval(Duration::from_secs(PREMIUM_INDEX_BOOTSTRAP_INTERVAL_SECS));
    premium_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut exchange_info_ticker =
        interval(Duration::from_secs(EXCHANGE_INFO_REFRESH_INTERVAL_SECS));
    exchange_info_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    info!(
        symbol = SYMBOL,
        funding_interval_secs = FUNDING_BACKFILL_INTERVAL_SECS,
        premium_interval_secs = PREMIUM_INDEX_BOOTSTRAP_INTERVAL_SECS,
        exchange_info_interval_secs = EXCHANGE_INFO_REFRESH_INTERVAL_SECS,
        "backfill scheduler started"
    );

    loop {
        tokio::select! {
            _ = funding_ticker.tick() => {
                handle_funding_rate(
                    &rest_client,
                    &db_writer,
                    &ops_writer,
                    &publisher,
                    &outbox_writer,
                    &metrics,
                    &mut last_funding_time,
                ).await;
            }
            _ = premium_ticker.tick() => {
                handle_premium_index(
                    &rest_client,
                    &db_writer,
                    &ops_writer,
                    &publisher,
                    &outbox_writer,
                    &metrics,
                    &mut last_premium_time,
                    ctx.rest_proxy_url.is_some(),
                ).await;
            }
            _ = exchange_info_ticker.tick() => {
                let spot_trigger = if spot_exchange_info_bootstrapped {
                    "reconcile"
                } else {
                    "startup"
                };
                let futures_trigger = if futures_exchange_info_bootstrapped {
                    "reconcile"
                } else {
                    "startup"
                };

                handle_exchange_info(
                    "spot",
                    spot_trigger,
                    &rest_client,
                    &ops_writer,
                ).await;
                handle_exchange_info(
                    "futures",
                    futures_trigger,
                    &rest_client,
                    &ops_writer,
                ).await;

                spot_exchange_info_bootstrapped = true;
                futures_exchange_info_bootstrapped = true;
            }
        }
    }
}

async fn handle_funding_rate(
    rest_client: &Arc<BinanceRestClient>,
    db_writer: &Arc<MdDbWriter>,
    ops_writer: &Arc<OpsDbWriter>,
    publisher: &Arc<MqPublisher>,
    outbox_writer: &Arc<OutboxWriter>,
    metrics: &Arc<AppMetrics>,
    last_funding_time: &mut Option<i64>,
) {
    let row = match rest_client.fetch_latest_funding_rate(SYMBOL).await {
        Ok(row) => row,
        Err(err) => {
            warn!(error = %err, symbol = SYMBOL, "fetch funding rate failed");
            return;
        }
    };

    let Some(record) = row else {
        warn!(symbol = SYMBOL, "funding rate api returned empty result");
        return;
    };

    if *last_funding_time == Some(record.funding_time) {
        return;
    }

    let event = match funding_rate_normalizer::normalize_rest_record(&record, false) {
        Ok(event) => event,
        Err(err) => {
            warn!(
                error = %err,
                symbol = SYMBOL,
                funding_time = record.funding_time,
                "normalize funding rate failed"
            );
            metrics.inc_normalize_error();
            return;
        }
    };

    if let Err(err) = persist_scheduler_event(
        &event,
        db_writer,
        publisher,
        outbox_writer,
        ops_writer,
        metrics,
    )
    .await
    {
        error!(
            error = %err,
            symbol = SYMBOL,
            funding_time = record.funding_time,
            "persist funding rate event failed"
        );
        return;
    }

    if let Err(err) = ops_writer
        .insert_backfill_job_run(
            Some("futures"),
            Some(SYMBOL),
            "/fapi/v1/fundingRate",
            "funding_history_backfill",
            "reconcile",
            json!({ "limit": 1 }),
            Some(1),
            "success",
            None,
        )
        .await
    {
        warn!(error = %err, "write funding backfill job run failed");
    }

    *last_funding_time = Some(record.funding_time);
}

async fn handle_premium_index(
    rest_client: &Arc<BinanceRestClient>,
    db_writer: &Arc<MdDbWriter>,
    ops_writer: &Arc<OpsDbWriter>,
    publisher: &Arc<MqPublisher>,
    outbox_writer: &Arc<OutboxWriter>,
    metrics: &Arc<AppMetrics>,
    last_premium_time: &mut Option<i64>,
    proxy_enabled: bool,
) {
    let premium = match rest_client.fetch_premium_index(SYMBOL).await {
        Ok(v) => v,
        Err(err) => {
            warn!(
                error = %err,
                symbol = SYMBOL,
                proxy_enabled = proxy_enabled,
                interval_secs = PREMIUM_INDEX_BOOTSTRAP_INTERVAL_SECS,
                "fetch premium index failed"
            );
            return;
        }
    };

    let premium_time = premium.time.unwrap_or_default();
    if *last_premium_time == Some(premium_time) {
        return;
    }

    let event = match mark_price_normalizer::normalize_premium_index_rest(
        SYMBOL, &premium, "rest", false,
    ) {
        Ok(event) => event,
        Err(err) => {
            metrics.inc_normalize_error();
            warn!(error = %err, symbol = SYMBOL, "normalize premium index failed");
            return;
        }
    };

    if let Err(err) = persist_scheduler_event(
        &event,
        db_writer,
        publisher,
        outbox_writer,
        ops_writer,
        metrics,
    )
    .await
    {
        error!(error = %err, symbol = SYMBOL, "persist premium index mark price failed");
        return;
    }

    if let Err(err) = ops_writer
        .insert_backfill_job_run(
            Some("futures"),
            Some(SYMBOL),
            "/fapi/v1/premiumIndex",
            "mark_funding_bootstrap",
            "reconcile",
            json!({}),
            Some(1),
            "success",
            None,
        )
        .await
    {
        warn!(error = %err, "write premium index bootstrap job run failed");
    }

    *last_premium_time = Some(premium_time);
}

async fn persist_scheduler_event(
    event: &NormalizedMdEvent,
    _db_writer: &Arc<MdDbWriter>,
    _publisher: &Arc<MqPublisher>,
    _outbox_writer: &Arc<OutboxWriter>,
    _ops_writer: &Arc<OpsDbWriter>,
    _metrics: &Arc<AppMetrics>,
) -> Result<()> {
    match persist_async::try_enqueue_registered_persist_event(event.market.as_str(), event.clone())
        .await?
    {
        true => Ok(()),
        false => {
            warn!(
                market = event.market,
                symbol = event.symbol,
                msg_type = event.msg_type,
                "registered persist queue unavailable; skip direct scheduler persist to avoid duplicate canonical aggregates"
            );
            Ok(())
        }
    }
}

async fn handle_exchange_info(
    market: &str,
    trigger_type: &str,
    rest_client: &Arc<BinanceRestClient>,
    ops_writer: &Arc<OpsDbWriter>,
) {
    let endpoint = if market == "spot" {
        "/api/v3/exchangeInfo"
    } else {
        "/fapi/v1/exchangeInfo"
    };

    let info = match rest_client.fetch_exchange_info(market, Some(SYMBOL)).await {
        Ok(v) => v,
        Err(err) => {
            warn!(error = %err, market, symbol = SYMBOL, "fetch exchange info failed");
            let err_message = err.to_string();
            if let Err(e) = ops_writer
                .insert_backfill_job_run(
                    Some(market),
                    Some(SYMBOL),
                    endpoint,
                    "bootstrap_metadata",
                    trigger_type,
                    json!({ "symbol": SYMBOL }),
                    Some(0),
                    "failed",
                    Some(err_message.as_str()),
                )
                .await
            {
                warn!(error = %e, market, "write failed exchange info job run failed");
            }
            return;
        }
    };

    let mut rows = 0i64;
    for symbol_info in &info.symbols {
        if !symbol_info.symbol.eq_ignore_ascii_case(SYMBOL) {
            continue;
        }

        let is_active = symbol_info
            .status
            .as_deref()
            .map(|s| s.eq_ignore_ascii_case("TRADING"))
            .unwrap_or(true);
        let metadata = json!({
            "timezone": info.timezone,
            "status": symbol_info.status,
            "filters": symbol_info.filters,
        });

        if let Err(err) = ops_writer
            .upsert_instrument_metadata(
                market,
                &symbol_info.symbol,
                symbol_info.contract_type.as_deref(),
                symbol_info.quote_asset.as_deref(),
                symbol_info.base_asset.as_deref(),
                symbol_info.price_precision,
                symbol_info.quantity_precision,
                is_active,
                metadata,
            )
            .await
        {
            warn!(
                error = %err,
                market,
                symbol = %symbol_info.symbol,
                "upsert cfg.instrument metadata failed"
            );
            continue;
        }
        rows += 1;
    }

    if let Err(err) = ops_writer
        .insert_backfill_job_run(
            Some(market),
            Some(SYMBOL),
            endpoint,
            "bootstrap_metadata",
            trigger_type,
            json!({ "symbol": SYMBOL }),
            Some(rows),
            "success",
            None,
        )
        .await
    {
        warn!(error = %err, market, "write exchange info job run failed");
    }
}
