use crate::indicators::context::{IndicatorComputation, IndicatorSnapshotRow};
use serde_json::Value;

pub fn snapshot_only(code: &'static str, payload: Value) -> IndicatorComputation {
    IndicatorComputation {
        snapshot: Some(IndicatorSnapshotRow {
            indicator_code: code,
            window_code: "1m",
            payload_json: payload,
        }),
        ..Default::default()
    }
}
