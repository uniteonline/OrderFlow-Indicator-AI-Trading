use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use lapin::{
    options::BasicPublishOptions,
    publisher_confirm::Confirmation,
    types::{AMQPValue, FieldTable, LongString, ShortString},
    BasicProperties, Channel,
};
use serde_json::{json, Value};
use tracing::debug;
use uuid::Uuid;

#[derive(Clone)]
pub struct IndPublisher {
    channel: Channel,
    exchange_name: String,
    producer_instance_id: String,
}

impl IndPublisher {
    pub fn new(channel: Channel, exchange_name: String, producer_instance_id: String) -> Self {
        Self {
            channel,
            exchange_name,
            producer_instance_id,
        }
    }

    pub async fn publish_snapshot(
        &self,
        ts_bucket: DateTime<Utc>,
        symbol: &str,
        indicator_code: &str,
        payload_json: &Value,
    ) -> Result<()> {
        let symbol_low = symbol.to_lowercase();
        let routing_key = format!("evt.{}.{}", indicator_code, symbol_low);
        let message_id = Uuid::new_v4();
        let trace_id = Uuid::new_v4();

        let body = json!({
            "schema_version": 1,
            "msg_type": "ind.snapshot",
            "message_id": message_id,
            "trace_id": trace_id,
            "routing_key": routing_key,
            "indicator_code": indicator_code,
            "symbol": symbol,
            "event_ts": ts_bucket.to_rfc3339(),
            "published_at": Utc::now().to_rfc3339(),
            "producer": {
                "service": "indicator_engine",
                "instance_id": self.producer_instance_id,
            },
            "data": payload_json,
        });

        self.publish_raw(routing_key, message_id, body).await
    }

    pub async fn publish_minute_bundle(
        &self,
        ts_bucket: DateTime<Utc>,
        symbol: &str,
        indicators_json: &Value,
        indicator_count: usize,
    ) -> Result<()> {
        let symbol_low = symbol.to_lowercase();
        let routing_key = format!("bundle.1m.{}", symbol_low);
        let message_id = Uuid::new_v4();
        let trace_id = Uuid::new_v4();

        let body = json!({
            "schema_version": 1,
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
        });

        self.publish_raw(routing_key, message_id, body).await
    }

    async fn publish_raw(
        &self,
        routing_key: String,
        message_id: Uuid,
        payload_json: Value,
    ) -> Result<()> {
        let payload = serde_json::to_vec(&payload_json).context("serialize indicator payload")?;

        let mut headers = FieldTable::default();
        headers.insert(ShortString::from("schema_version"), AMQPValue::LongUInt(1));
        headers.insert(
            ShortString::from("producer_service"),
            AMQPValue::LongString(LongString::from("indicator_engine")),
        );
        headers.insert(
            ShortString::from("producer_instance_id"),
            AMQPValue::LongString(LongString::from(self.producer_instance_id.clone())),
        );

        let properties = BasicProperties::default()
            .with_content_type(ShortString::from("application/json"))
            .with_delivery_mode(2)
            .with_headers(headers)
            .with_message_id(ShortString::from(message_id.to_string()));

        let confirm = self
            .channel
            .basic_publish(
                &self.exchange_name,
                &routing_key,
                BasicPublishOptions::default(),
                &payload,
                properties,
            )
            .await
            .with_context(|| format!("publish indicator routing_key={} failed", routing_key))?;

        let confirmation = confirm.await.context("wait indicator publisher confirm")?;
        match confirmation {
            Confirmation::Ack(_) => {}
            Confirmation::Nack(_) => {
                return Err(anyhow::anyhow!(
                    "indicator publish nacked by broker routing_key={}",
                    routing_key
                ));
            }
            Confirmation::NotRequested => {
                return Err(anyhow::anyhow!(
                    "indicator publish confirmation not requested routing_key={}",
                    routing_key
                ));
            }
        }
        debug!(routing_key = %routing_key, message_id = %message_id, "indicator message published");
        Ok(())
    }
}
