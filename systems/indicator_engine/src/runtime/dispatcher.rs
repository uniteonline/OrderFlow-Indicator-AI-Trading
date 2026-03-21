use crate::indicators::context::{
    AbsorptionEventRow, DivergenceEventRow, ExhaustionEventRow, IndicatorComputation,
    IndicatorContext, IndicatorEventRow, IndicatorLevelRow, IndicatorSnapshotRow,
    InitiationEventRow, LiquidationLevelRow,
};
use crate::indicators::indicator_trait::Indicator;
use crate::indicators::registry::build_registry;
use crate::publish::ind_publisher::IndPublisher;
use crate::storage::event_writer::EventWriter;
use crate::storage::feature_writer::FeatureWriter;
use crate::storage::level_writer::LevelWriter;
use crate::storage::snapshot_writer::SnapshotWriter;
use anyhow::{Context, Result};
use serde_json::{json, Map, Value};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::debug;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchMode {
    WarmStateOnly,
    ReplayMaterialize,
    Live,
}

impl DispatchMode {
    fn persist_outputs(self) -> bool {
        !matches!(self, Self::WarmStateOnly)
    }

    fn publish_outputs(self) -> bool {
        matches!(self, Self::Live)
    }
}

pub struct Dispatcher {
    flow_indicators: Vec<Arc<dyn Indicator>>,
    deriv_indicators: Vec<Arc<dyn Indicator>>,
    orderbook_indicators: Vec<Arc<dyn Indicator>>,
    feature_writer: FeatureWriter,
    snapshot_writer: SnapshotWriter,
    level_writer: LevelWriter,
    event_writer: EventWriter,
    publisher: IndPublisher,
}

#[derive(Default)]
struct GroupOutput {
    snapshots: Vec<IndicatorSnapshotRow>,
    levels: Vec<IndicatorLevelRow>,
    events: Vec<IndicatorEventRow>,
    divergence_rows: Vec<DivergenceEventRow>,
    absorption_rows: Vec<AbsorptionEventRow>,
    initiation_rows: Vec<InitiationEventRow>,
    exhaustion_rows: Vec<ExhaustionEventRow>,
    liq_rows: Vec<LiquidationLevelRow>,
}

impl Dispatcher {
    pub fn new(
        feature_writer: FeatureWriter,
        snapshot_writer: SnapshotWriter,
        level_writer: LevelWriter,
        event_writer: EventWriter,
        publisher: IndPublisher,
    ) -> Self {
        let indicators = build_registry();
        let mut flow_indicators = Vec::new();
        let mut deriv_indicators = Vec::new();
        let mut orderbook_indicators = Vec::new();
        for indicator in indicators {
            match indicator.code() {
                "price_volume_structure"
                | "footprint"
                | "divergence"
                | "cvd_pack"
                | "whale_trades"
                | "vpin"
                | "avwap"
                | "kline_history"
                | "tpo_market_profile"
                | "rvwap_sigma_bands"
                | "high_volume_pulse"
                | "ema_trend_regime"
                | "fvg" => flow_indicators.push(indicator),
                "liquidation_density" | "funding_rate" => deriv_indicators.push(indicator),
                _ => orderbook_indicators.push(indicator),
            }
        }

        Self {
            flow_indicators,
            deriv_indicators,
            orderbook_indicators,
            feature_writer,
            snapshot_writer,
            level_writer,
            event_writer,
            publisher,
        }
    }

    pub async fn process_window(
        &self,
        ctx: &IndicatorContext,
        mode: DispatchMode,
    ) -> Result<Vec<IndicatorSnapshotRow>> {
        let flow_handle = spawn_group_worker(self.flow_indicators.clone(), ctx.clone());
        let deriv_handle = spawn_group_worker(self.deriv_indicators.clone(), ctx.clone());
        let orderbook_handle = spawn_group_worker(self.orderbook_indicators.clone(), ctx.clone());

        let flow_output = join_group("flow", flow_handle).await?;
        let deriv_output = join_group("deriv", deriv_handle).await?;
        let orderbook_output = join_group("orderbook_heavy", orderbook_handle).await?;

        let flow_snapshot_count = flow_output.snapshots.len();
        let deriv_snapshot_count = deriv_output.snapshots.len();
        let orderbook_snapshot_count = orderbook_output.snapshots.len();

        let mut snapshots: Vec<IndicatorSnapshotRow> = Vec::new();
        let mut levels: Vec<IndicatorLevelRow> = Vec::new();
        let mut events: Vec<IndicatorEventRow> = Vec::new();
        let mut divergence_rows: Vec<DivergenceEventRow> = Vec::new();
        let mut absorption_rows: Vec<AbsorptionEventRow> = Vec::new();
        let mut initiation_rows: Vec<InitiationEventRow> = Vec::new();
        let mut exhaustion_rows: Vec<ExhaustionEventRow> = Vec::new();
        let mut liq_rows: Vec<LiquidationLevelRow> = Vec::new();

        merge_group_output(
            flow_output,
            &mut snapshots,
            &mut levels,
            &mut events,
            &mut divergence_rows,
            &mut absorption_rows,
            &mut initiation_rows,
            &mut exhaustion_rows,
            &mut liq_rows,
        );
        merge_group_output(
            deriv_output,
            &mut snapshots,
            &mut levels,
            &mut events,
            &mut divergence_rows,
            &mut absorption_rows,
            &mut initiation_rows,
            &mut exhaustion_rows,
            &mut liq_rows,
        );
        merge_group_output(
            orderbook_output,
            &mut snapshots,
            &mut levels,
            &mut events,
            &mut divergence_rows,
            &mut absorption_rows,
            &mut initiation_rows,
            &mut exhaustion_rows,
            &mut liq_rows,
        );
        snapshots.sort_by(|a, b| a.indicator_code.cmp(b.indicator_code));

        let mut indicators_json = Map::new();
        for s in &snapshots {
            indicators_json.insert(
                s.indicator_code.to_string(),
                json!({
                    "window_code": s.window_code,
                    "payload": s.payload_json.clone(),
                }),
            );
        }
        let live_messages = if mode.publish_outputs() {
            let indicators_json_value = Value::Object(indicators_json.clone());
            let mut messages = Vec::with_capacity(snapshots.len() + 1);
            messages.push(self.publisher.build_minute_bundle_message(
                ctx.ts_bucket,
                &ctx.symbol,
                &indicators_json_value,
                snapshots.len(),
            )?);
            for snapshot in &snapshots {
                messages.push(self.publisher.build_snapshot_message(
                    ctx.ts_bucket,
                    &ctx.symbol,
                    snapshot,
                )?);
            }
            Some(messages)
        } else {
            None
        };

        if mode.persist_outputs() {
            self.snapshot_writer
                .write_snapshots(ctx.ts_bucket, &ctx.symbol, &snapshots)
                .await?;
            self.level_writer
                .write_indicator_levels(ctx.ts_bucket, &ctx.symbol, &levels)
                .await?;
            self.level_writer
                .write_liquidation_levels(ctx.ts_bucket, &ctx.symbol, &liq_rows)
                .await?;
            let event_history_start_ts = event_history_start_ts(ctx);
            let event_history_end_ts = ctx.ts_bucket + chrono::Duration::minutes(1);
            self.event_writer
                .write_indicator_events(
                    &ctx.symbol,
                    event_history_start_ts,
                    event_history_end_ts,
                    &events,
                )
                .await?;
            self.event_writer
                .write_divergence_events(
                    &ctx.symbol,
                    event_history_start_ts,
                    event_history_end_ts,
                    &divergence_rows,
                )
                .await?;
            self.event_writer
                .write_absorption_events(
                    &ctx.symbol,
                    event_history_start_ts,
                    event_history_end_ts,
                    &absorption_rows,
                )
                .await?;
            self.event_writer
                .write_initiation_events(
                    &ctx.symbol,
                    event_history_start_ts,
                    event_history_end_ts,
                    &initiation_rows,
                )
                .await?;
            self.event_writer
                .write_exhaustion_events(
                    &ctx.symbol,
                    event_history_start_ts,
                    event_history_end_ts,
                    &exhaustion_rows,
                )
                .await?;
            self.feature_writer.write_all(ctx).await?;
            if let Some(messages) = live_messages.as_ref() {
                self.snapshot_writer
                    .advance_progress_with_outbox(&ctx.symbol, ctx.ts_bucket, messages)
                    .await?;
            } else {
                self.snapshot_writer
                    .advance_progress(&ctx.symbol, ctx.ts_bucket)
                    .await?;
            }
        }

        debug!(
            ts_bucket = %ctx.ts_bucket,
            snapshot_count = snapshots.len(),
            flow_snapshot_count = flow_snapshot_count,
            deriv_snapshot_count = deriv_snapshot_count,
            orderbook_snapshot_count = orderbook_snapshot_count,
            level_count = levels.len(),
            event_count = events.len(),
            divergence_count = divergence_rows.len(),
            absorption_count = absorption_rows.len(),
            initiation_count = initiation_rows.len(),
            exhaustion_count = exhaustion_rows.len(),
            liq_levels = liq_rows.len(),
            "indicator window processed"
        );

        Ok(snapshots)
    }

    pub async fn rewind_progress(
        &self,
        symbol: &str,
        ts_bucket: chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        self.snapshot_writer
            .rewind_progress(symbol, ts_bucket)
            .await
    }

    pub async fn rewind_persisted_tail(
        &self,
        symbol: &str,
        repair_start_ts: chrono::DateTime<chrono::Utc>,
        exchange_name: &str,
    ) -> Result<()> {
        self.snapshot_writer
            .rewind_persisted_tail(symbol, repair_start_ts, exchange_name)
            .await
    }
}

fn spawn_group_worker(
    indicators: Vec<Arc<dyn Indicator>>,
    ctx: IndicatorContext,
) -> JoinHandle<GroupOutput> {
    tokio::task::spawn_blocking(move || evaluate_indicator_group(indicators, &ctx))
}

fn event_history_start_ts(ctx: &IndicatorContext) -> chrono::DateTime<chrono::Utc> {
    match (ctx.history_futures.first(), ctx.history_spot.first()) {
        (Some(fut), Some(spot)) => fut.ts_bucket.max(spot.ts_bucket),
        (Some(fut), None) => fut.ts_bucket,
        (None, Some(spot)) => spot.ts_bucket,
        (None, None) => ctx.ts_bucket,
    }
}

async fn join_group(group_name: &str, handle: JoinHandle<GroupOutput>) -> Result<GroupOutput> {
    handle
        .await
        .with_context(|| format!("join indicator group worker failed group={}", group_name))
}

fn evaluate_indicator_group(
    indicators: Vec<Arc<dyn Indicator>>,
    ctx: &IndicatorContext,
) -> GroupOutput {
    let mut out = GroupOutput::default();
    for indicator in &indicators {
        let comp: IndicatorComputation = indicator.evaluate(ctx);

        if let Some(s) = comp.snapshot {
            out.snapshots.push(s);
        }
        if !comp.level_rows.is_empty() {
            out.levels.extend(comp.level_rows);
        }
        if !comp.event_rows.is_empty() {
            out.events.extend(comp.event_rows);
        }
        if !comp.divergence_rows.is_empty() {
            out.divergence_rows.extend(comp.divergence_rows);
        }
        if !comp.absorption_rows.is_empty() {
            out.absorption_rows.extend(comp.absorption_rows);
        }
        if !comp.initiation_rows.is_empty() {
            out.initiation_rows.extend(comp.initiation_rows);
        }
        if !comp.exhaustion_rows.is_empty() {
            out.exhaustion_rows.extend(comp.exhaustion_rows);
        }
        if !comp.liquidation_rows.is_empty() {
            out.liq_rows.extend(comp.liquidation_rows);
        }
    }
    out
}

fn merge_group_output(
    group: GroupOutput,
    snapshots: &mut Vec<IndicatorSnapshotRow>,
    levels: &mut Vec<IndicatorLevelRow>,
    events: &mut Vec<IndicatorEventRow>,
    divergence_rows: &mut Vec<DivergenceEventRow>,
    absorption_rows: &mut Vec<AbsorptionEventRow>,
    initiation_rows: &mut Vec<InitiationEventRow>,
    exhaustion_rows: &mut Vec<ExhaustionEventRow>,
    liq_rows: &mut Vec<LiquidationLevelRow>,
) {
    snapshots.extend(group.snapshots);
    levels.extend(group.levels);
    events.extend(group.events);
    divergence_rows.extend(group.divergence_rows);
    absorption_rows.extend(group.absorption_rows);
    initiation_rows.extend(group.initiation_rows);
    exhaustion_rows.extend(group.exhaustion_rows);
    liq_rows.extend(group.liq_rows);
}
