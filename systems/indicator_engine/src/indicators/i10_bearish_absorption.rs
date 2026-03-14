use crate::indicators::context::{
    IndicatorComputation, IndicatorContext, IndicatorEventRow, IndicatorSnapshotRow,
};
use crate::indicators::i06_absorption::{
    absorption_event_json, detect_absorption_all_history, detect_absorption_events,
};
use crate::indicators::indicator_trait::Indicator;
use crate::indicators::shared::event_ids::build_indicator_event_id;
use crate::indicators::shared::event_views::{build_event_window_view, build_recent_7d_payload};
use serde_json::json;

pub struct I10BearishAbsorption;

impl Indicator for I10BearishAbsorption {
    fn code(&self) -> &'static str {
        "bearish_absorption"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation {
        let all_events = detect_absorption_all_history(ctx)
            .into_iter()
            .filter(|e| e.direction < 0)
            .collect::<Vec<_>>();
        let window_view = build_event_window_view(
            ctx.ts_bucket,
            all_events
                .iter()
                .map(|event| absorption_event_json(&ctx.symbol, self.code(), event))
                .collect(),
        );
        let lookback_covered_minutes = ctx.history_futures.len().min(ctx.history_spot.len()) as i64;
        let events = detect_absorption_events(ctx)
            .into_iter()
            .filter(|e| e.direction < 0)
            .collect::<Vec<_>>();

        let mut out = IndicatorComputation {
            snapshot: Some(IndicatorSnapshotRow {
                indicator_code: self.code(),
                window_code: "1m",
                payload_json: json!({
                    "recent_7d": build_recent_7d_payload(
                        window_view.recent_events,
                        lookback_covered_minutes,
                        "in_memory_minute_history"
                    )
                }),
            }),
            ..Default::default()
        };

        for e in events {
            let event_id = build_indicator_event_id(
                &ctx.symbol,
                self.code(),
                &e.event_type,
                e.start_ts,
                Some(e.end_ts),
                e.direction,
                None,
                None,
            );
            let payload_json = absorption_event_json(&ctx.symbol, self.code(), &e).1;
            out.event_rows.push(IndicatorEventRow::new(
                event_id,
                self.code(),
                e.event_type,
                "info".to_string(),
                e.direction,
                e.start_ts,
                Some(e.end_ts),
                e.confirm_ts,
                "1m",
                None,
                None,
                None,
                None,
                Some(true),
                None,
                Some(e.score),
                Some(e.score),
                payload_json,
            ));
        }

        out
    }
}
