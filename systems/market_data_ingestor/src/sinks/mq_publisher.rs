use crate::normalize::NormalizedMdEvent;
use anyhow::{Context, Result};
use chrono::Utc;
use lapin::{
    options::BasicPublishOptions,
    types::{AMQPValue, FieldTable, LongString, ShortString},
    BasicProperties, Channel,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{debug, info};
use uuid::Uuid;

const LIVE_DEPTH_CONFLATION_MS: i64 = 100;
const LIVE_DEPTH_SUPPRESS_LOG_EVERY: u64 = 10_000;
const LIVE_DEPTH_CONFLATION_ENABLED: bool = false;

#[derive(Debug, Clone)]
pub struct OutboxRecord {
    pub exchange_name: String,
    pub routing_key: String,
    pub message_id: Uuid,
    pub schema_version: i32,
    pub headers_json: Value,
    pub payload_json: Value,
}

#[derive(Clone)]
pub struct MqPublisher {
    channel: Channel,
    live_exchange_name: String,
    replay_exchange_name: String,
    producer_instance_id: String,
    live_depth_slots: Arc<Mutex<HashMap<(String, String, String), i64>>>,
    live_depth_emitted: Arc<AtomicU64>,
    live_depth_suppressed: Arc<AtomicU64>,
}

impl MqPublisher {
    pub fn new(
        channel: Channel,
        live_exchange_name: String,
        replay_exchange_name: String,
        producer_instance_id: String,
    ) -> Self {
        Self {
            channel,
            live_exchange_name,
            replay_exchange_name,
            producer_instance_id,
            live_depth_slots: Arc::new(Mutex::new(HashMap::new())),
            live_depth_emitted: Arc::new(AtomicU64::new(0)),
            live_depth_suppressed: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn build_outbox_record(&self, event: &NormalizedMdEvent) -> Result<OutboxRecord> {
        self.build_outbox_record_for_exchange(event, self.exchange_for_event(event))
    }

    pub fn build_outbox_records_for_pipeline(
        &self,
        event: &NormalizedMdEvent,
    ) -> Result<Vec<OutboxRecord>> {
        // Backfill data is emitted to replay exchange for dedicated replay consumers.
        // Aggregate backfill also goes to live exchange so live indicator consumers can
        // immediately observe repaired/late minute chunks.
        if event.backfill_in_progress {
            let mut records = Vec::with_capacity(2);
            records.push(self.build_outbox_record_for_exchange(event, &self.replay_exchange_name)?);
            if event.msg_type.starts_with("md.agg.") {
                records
                    .push(self.build_outbox_record_for_exchange(event, &self.live_exchange_name)?);
            }
            return Ok(records);
        }

        // Live mode emits to live exchange only. Replay should be produced by md_replayer.
        if LIVE_DEPTH_CONFLATION_ENABLED
            && self.is_live_conflation_target(event)
            && !self.should_emit_live_depth(event)
        {
            return Ok(Vec::new());
        }

        Ok(vec![self.build_outbox_record_for_exchange(
            event,
            &self.live_exchange_name,
        )?])
    }

    pub fn build_outbox_records_force_live(
        &self,
        event: &NormalizedMdEvent,
    ) -> Result<Vec<OutboxRecord>> {
        Ok(vec![self.build_outbox_record_for_exchange(
            event,
            &self.live_exchange_name,
        )?])
    }

    fn build_outbox_record_for_exchange(
        &self,
        event: &NormalizedMdEvent,
        exchange_name: &str,
    ) -> Result<OutboxRecord> {
        let message_id = Uuid::new_v4();
        let trace_id = Uuid::new_v4();
        let published_at = Utc::now();

        let payload_json = json!({
            "schema_version": 1,
            "msg_type": event.msg_type,
            "message_id": message_id,
            "trace_id": trace_id,
            "routing_key": event.routing_key,
            "market": event.market,
            "symbol": event.symbol,
            "source_kind": event.source_kind,
            "backfill_in_progress": event.backfill_in_progress,
            "event_ts": event.event_ts.to_rfc3339(),
            "published_at": published_at.to_rfc3339(),
            "producer": {
                "service": "market_data_ingestor",
                "instance_id": self.producer_instance_id,
            },
            "data": event.data,
        });

        let headers_json = json!({
            "schema_version": 1,
            "message_id": message_id.to_string(),
            "trace_id": trace_id.to_string(),
            "msg_type": event.msg_type,
            "market": event.market,
            "symbol": event.symbol,
            "routing_key": event.routing_key,
            "source_kind": event.source_kind,
            "backfill_in_progress": event.backfill_in_progress,
            "published_at": published_at.to_rfc3339(),
            "producer_service": "market_data_ingestor",
            "producer_instance_id": self.producer_instance_id,
            "content_type": "application/json",
        });

        Ok(OutboxRecord {
            exchange_name: exchange_name.to_string(),
            routing_key: event.routing_key.clone(),
            message_id,
            schema_version: 1,
            headers_json,
            payload_json,
        })
    }

    pub async fn publish_md_event(&self, event: &NormalizedMdEvent) -> Result<()> {
        let record = self.build_outbox_record(event)?;

        let payload = serde_json::to_vec(&record.payload_json).context("serialize mq body")?;

        let mut headers = FieldTable::default();
        headers.insert(ShortString::from("schema_version"), AMQPValue::LongUInt(1));
        headers.insert(
            ShortString::from("message_id"),
            AMQPValue::LongString(LongString::from(record.message_id.to_string())),
        );
        headers.insert(
            ShortString::from("trace_id"),
            AMQPValue::LongString(LongString::from(
                record
                    .headers_json
                    .get("trace_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            )),
        );
        headers.insert(
            ShortString::from("msg_type"),
            AMQPValue::LongString(LongString::from(event.msg_type.clone())),
        );
        headers.insert(
            ShortString::from("market"),
            AMQPValue::LongString(LongString::from(event.market.clone())),
        );
        headers.insert(
            ShortString::from("symbol"),
            AMQPValue::LongString(LongString::from(event.symbol.clone())),
        );
        headers.insert(
            ShortString::from("routing_key"),
            AMQPValue::LongString(LongString::from(event.routing_key.clone())),
        );
        headers.insert(
            ShortString::from("source_kind"),
            AMQPValue::LongString(LongString::from(event.source_kind.clone())),
        );
        headers.insert(
            ShortString::from("backfill_in_progress"),
            AMQPValue::Boolean(event.backfill_in_progress),
        );
        headers.insert(
            ShortString::from("published_at"),
            AMQPValue::LongString(LongString::from(
                record
                    .headers_json
                    .get("published_at")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            )),
        );
        headers.insert(
            ShortString::from("producer_service"),
            AMQPValue::LongString(LongString::from("market_data_ingestor")),
        );
        headers.insert(
            ShortString::from("producer_instance_id"),
            AMQPValue::LongString(LongString::from(self.producer_instance_id.clone())),
        );
        headers.insert(
            ShortString::from("content_type"),
            AMQPValue::LongString(LongString::from("application/json")),
        );

        let properties = BasicProperties::default()
            .with_content_type(ShortString::from("application/json"))
            .with_delivery_mode(2)
            .with_headers(headers)
            .with_message_id(ShortString::from(record.message_id.to_string()));

        // Fire-and-forget: await only the frame-send (TCP write), not the
        // broker ACK.  The outbox pattern (DB-backed) provides guaranteed
        // delivery; waiting for publisher confirms on the hot path blocks all
        // workers on the shared channel and causes cascading timeouts under
        // any burst or broker load.  A successful basic_publish means the
        // broker received the AMQP frame; loss is only possible if the broker
        // crashes before flushing, which the outbox relay covers on restart.
        let _confirm = self
            .channel
            .basic_publish(
                &record.exchange_name,
                &event.routing_key,
                BasicPublishOptions::default(),
                &payload,
                properties,
            )
            .await
            .with_context(|| format!("publish to routing_key={} failed", event.routing_key))?;
        // _confirm (PublisherConfirm) is intentionally dropped; lapin discards
        // the incoming ACK internally when no one is awaiting it.

        debug!(
            exchange = %record.exchange_name,
            msg_type = %event.msg_type,
            routing_key = %event.routing_key,
            message_id = %record.message_id,
            "mq message published"
        );

        Ok(())
    }

    fn exchange_for_event<'a>(&'a self, event: &NormalizedMdEvent) -> &'a str {
        if event.backfill_in_progress {
            &self.replay_exchange_name
        } else {
            &self.live_exchange_name
        }
    }

    fn should_emit_live_depth(&self, event: &NormalizedMdEvent) -> bool {
        let slot = event
            .event_ts
            .timestamp_millis()
            .div_euclid(LIVE_DEPTH_CONFLATION_MS);
        let key = (
            event.market.clone(),
            event.symbol.clone(),
            event.msg_type.clone(),
        );

        let mut slots = self
            .live_depth_slots
            .lock()
            .expect("live_depth_slots poisoned");
        match slots.get(&key).copied() {
            Some(last_slot) if slot <= last_slot => {
                let suppressed = self.live_depth_suppressed.fetch_add(1, Ordering::Relaxed) + 1;
                if suppressed % LIVE_DEPTH_SUPPRESS_LOG_EVERY == 0 {
                    info!(
                        suppressed = suppressed,
                        emitted = self.live_depth_emitted.load(Ordering::Relaxed),
                        window_ms = LIVE_DEPTH_CONFLATION_MS,
                        "live orderbook pre-aggregation stats"
                    );
                }
                false
            }
            _ => {
                slots.insert(key, slot);
                self.live_depth_emitted.fetch_add(1, Ordering::Relaxed);
                true
            }
        }
    }

    fn is_live_conflation_target(&self, event: &NormalizedMdEvent) -> bool {
        matches!(event.msg_type.as_str(), "md.depth" | "md.bbo")
    }
}
