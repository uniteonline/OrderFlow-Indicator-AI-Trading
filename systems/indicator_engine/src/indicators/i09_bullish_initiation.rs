use crate::indicators::context::{
    IndicatorComputation, IndicatorContext, IndicatorEventRow, IndicatorSnapshotRow,
};
use crate::indicators::i07_initiation::{detect_initiation_events, initiation_event_json};
use crate::indicators::indicator_trait::Indicator;
use crate::indicators::shared::event_ids::build_indicator_event_id;
use serde_json::json;

pub struct I09BullishInitiation;

impl Indicator for I09BullishInitiation {
    fn code(&self) -> &'static str {
        "bullish_initiation"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation {
        let events = detect_initiation_events(ctx)
            .into_iter()
            .filter(|e| e.direction > 0)
            .collect::<Vec<_>>();

        let mut out = IndicatorComputation {
            snapshot: Some(IndicatorSnapshotRow {
                indicator_code: self.code(),
                window_code: "1m",
                payload_json: json!({
                    "event_count": events.len(),
                    "events": events.iter().map(|e| {
                        json!({
                            "type": e.event_type,
                            "start_ts": e.start_ts.to_rfc3339(),
                            "end_ts": e.end_ts.to_rfc3339(),
                            "confirm_ts": e.confirm_ts.to_rfc3339(),
                            "score": e.score
                        })
                    }).collect::<Vec<_>>()
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
            let payload_json = initiation_event_json(&ctx.symbol, self.code(), &e).1;
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
