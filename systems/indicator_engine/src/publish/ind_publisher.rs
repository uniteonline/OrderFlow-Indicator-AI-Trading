use crate::indicators::context::IndicatorSnapshotRow;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use uuid::Uuid;

const INDICATOR_OUTBOX_SCHEMA_VERSION: i32 = 1;
const INDICATOR_MESSAGE_NAMESPACE: Uuid =
    Uuid::from_u128(0x8d9b_9f64_0eea_4f7b_9f35_4385_31a7_a651);

#[derive(Debug, Clone)]
pub struct OutboxMessage {
    pub exchange_name: String,
    pub routing_key: String,
    pub message_id: Uuid,
    pub schema_version: i32,
    pub headers_json: Value,
    pub payload_json: Value,
}

#[derive(Clone)]
pub struct IndPublisher {
    exchange_name: String,
    producer_instance_id: String,
}

impl IndPublisher {
    pub fn new(exchange_name: String, producer_instance_id: String) -> Self {
        Self {
            exchange_name,
            producer_instance_id,
        }
    }

    pub fn build_snapshot_message(
        &self,
        ts_bucket: DateTime<Utc>,
        symbol: &str,
        snapshot: &IndicatorSnapshotRow,
    ) -> Result<OutboxMessage> {
        let symbol_low = symbol.to_lowercase();
        let routing_key = format!("evt.{}.{}", snapshot.indicator_code, symbol_low);
        let identity = format!(
            "ind.snapshot|{}|{}|{}|{}|{}",
            symbol.to_uppercase(),
            ts_bucket.to_rfc3339(),
            snapshot.indicator_code,
            snapshot.window_code,
            snapshot.payload_json
        );
        let message_id = stable_uuid("message", &identity);
        let trace_id = stable_uuid("trace", &identity);

        Ok(OutboxMessage {
            exchange_name: self.exchange_name.clone(),
            routing_key: routing_key.clone(),
            message_id,
            schema_version: INDICATOR_OUTBOX_SCHEMA_VERSION,
            headers_json: self.build_headers_json(),
            payload_json: json!({
                "schema_version": INDICATOR_OUTBOX_SCHEMA_VERSION,
                "msg_type": "ind.snapshot",
                "message_id": message_id,
                "trace_id": trace_id,
                "routing_key": routing_key,
                "indicator_code": snapshot.indicator_code,
                "window_code": snapshot.window_code,
                "symbol": symbol,
                "event_ts": ts_bucket.to_rfc3339(),
                "published_at": Utc::now().to_rfc3339(),
                "producer": {
                    "service": "indicator_engine",
                    "instance_id": self.producer_instance_id,
                },
                "data": snapshot.payload_json,
            }),
        })
    }

    pub fn build_minute_bundle_message(
        &self,
        ts_bucket: DateTime<Utc>,
        symbol: &str,
        indicators_json: &Value,
        indicator_count: usize,
    ) -> Result<OutboxMessage> {
        let symbol_low = symbol.to_lowercase();
        let routing_key = format!("bundle.1m.{}", symbol_low);
        let identity = format!(
            "ind.minute_bundle|{}|{}|{}",
            symbol.to_uppercase(),
            ts_bucket.to_rfc3339(),
            indicators_json
        );
        let message_id = stable_uuid("message", &identity);
        let trace_id = stable_uuid("trace", &identity);

        Ok(OutboxMessage {
            exchange_name: self.exchange_name.clone(),
            routing_key: routing_key.clone(),
            message_id,
            schema_version: INDICATOR_OUTBOX_SCHEMA_VERSION,
            headers_json: self.build_headers_json(),
            payload_json: json!({
                "schema_version": INDICATOR_OUTBOX_SCHEMA_VERSION,
                "msg_type": "ind.minute_bundle",
                "message_id": message_id,
                "trace_id": trace_id,
                "routing_key": routing_key,
                "symbol": symbol,
                "ts_bucket": ts_bucket.to_rfc3339(),
                "window_code": "1m",
                "indicator_count": indicator_count,
                "published_at": Utc::now().to_rfc3339(),
                "producer": {
                    "service": "indicator_engine",
                    "instance_id": self.producer_instance_id,
                },
                "indicators": indicators_json,
            }),
        })
    }

    fn build_headers_json(&self) -> Value {
        json!({
            "schema_version": INDICATOR_OUTBOX_SCHEMA_VERSION,
            "producer_service": "indicator_engine",
            "producer_instance_id": self.producer_instance_id,
        })
    }
}

fn stable_uuid(scope: &str, identity: &str) -> Uuid {
    Uuid::new_v5(
        &INDICATOR_MESSAGE_NAMESPACE,
        format!("{}|{}", scope, identity).as_bytes(),
    )
}

#[cfg(test)]
mod tests {
    use super::IndPublisher;
    use crate::indicators::context::IndicatorSnapshotRow;
    use chrono::TimeZone;
    use serde_json::json;

    fn publisher() -> IndPublisher {
        IndPublisher::new("amq.ind".to_string(), "indicator-engine-test".to_string())
    }

    #[test]
    fn snapshot_message_id_is_stable_for_same_payload() {
        let publisher = publisher();
        let ts_bucket = chrono::Utc.with_ymd_and_hms(2026, 3, 21, 3, 0, 0).unwrap();
        let snapshot = IndicatorSnapshotRow {
            indicator_code: "orderbook_depth",
            window_code: "15m",
            payload_json: json!({"imbalance": 0.42}),
        };

        let left = publisher
            .build_snapshot_message(ts_bucket, "BTCUSDT", &snapshot)
            .unwrap();
        let right = publisher
            .build_snapshot_message(ts_bucket, "BTCUSDT", &snapshot)
            .unwrap();

        assert_eq!(left.message_id, right.message_id);
        assert_eq!(left.routing_key, "evt.orderbook_depth.btcusdt");
        assert_eq!(left.payload_json["window_code"], "15m");
    }

    #[test]
    fn snapshot_message_id_changes_when_payload_changes() {
        let publisher = publisher();
        let ts_bucket = chrono::Utc.with_ymd_and_hms(2026, 3, 21, 3, 0, 0).unwrap();
        let before = IndicatorSnapshotRow {
            indicator_code: "orderbook_depth",
            window_code: "15m",
            payload_json: json!({"imbalance": 0.42}),
        };
        let after = IndicatorSnapshotRow {
            indicator_code: "orderbook_depth",
            window_code: "15m",
            payload_json: json!({"imbalance": 0.73}),
        };

        let left = publisher
            .build_snapshot_message(ts_bucket, "BTCUSDT", &before)
            .unwrap();
        let right = publisher
            .build_snapshot_message(ts_bucket, "BTCUSDT", &after)
            .unwrap();

        assert_ne!(left.message_id, right.message_id);
    }
}
