use crate::app::config::ParquetConfig;
use crate::normalize::NormalizedMdEvent;
use anyhow::{Context, Result};
use serde_json::json;
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use tracing::{debug, warn};

#[derive(Debug, Clone)]
struct RawColdRow {
    event_ts_ms: i64,
    msg_type: String,
    market: String,
    symbol: String,
    source_kind: String,
    stream_name: String,
    routing_key: String,
    backfill_in_progress: bool,
    data_json: serde_json::Value,
}

#[derive(Clone)]
pub struct ParquetSink {
    enabled: bool,
    root_dir: PathBuf,
}

impl Default for ParquetSink {
    fn default() -> Self {
        Self {
            enabled: false,
            root_dir: PathBuf::new(),
        }
    }
}

impl ParquetSink {
    pub fn from_config(config: &ParquetConfig) -> Self {
        let enabled = config.enabled;
        let root_dir = PathBuf::from(config.root_dir.trim());
        if enabled {
            debug!(
                root_dir = %root_dir.display(),
                compression = %config.compression,
                "cold sink enabled (jsonl fallback)"
            );
        } else {
            debug!("cold sink disabled");
        }
        Self { enabled, root_dir }
    }

    pub async fn write(&self, event: &NormalizedMdEvent) -> Result<()> {
        self.write_batch(std::slice::from_ref(event)).await
    }

    pub async fn write_batch(&self, events: &[NormalizedMdEvent]) -> Result<()> {
        if !self.enabled || events.is_empty() {
            return Ok(());
        }

        let mut groups: BTreeMap<(String, String, String), Vec<RawColdRow>> = BTreeMap::new();
        for event in events {
            let date = event.event_ts.format("%Y-%m-%d").to_string();
            let key = (
                date,
                event.market.to_lowercase(),
                event.symbol.to_uppercase(),
            );
            groups.entry(key).or_default().push(RawColdRow {
                event_ts_ms: event.event_ts.timestamp_millis(),
                msg_type: event.msg_type.clone(),
                market: event.market.clone(),
                symbol: event.symbol.clone(),
                source_kind: event.source_kind.clone(),
                stream_name: event.stream_name.clone(),
                routing_key: event.routing_key.clone(),
                backfill_in_progress: event.backfill_in_progress,
                data_json: event.data.clone(),
            });
        }

        for ((date, market, symbol), rows) in groups {
            let root_dir = self.root_dir.clone();
            tokio::task::spawn_blocking(move || {
                write_group_rows(&root_dir, &date, &market, &symbol, rows)
            })
            .await
            .context("join cold-store write task")??;
        }

        Ok(())
    }
}

fn write_group_rows(
    root_dir: &Path,
    date: &str,
    market: &str,
    symbol: &str,
    rows: Vec<RawColdRow>,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    // NOTE: We keep the "dataset=raw_md" cold-tier contract and store batch files in
    // line-delimited JSON as an offline-safe fallback.
    // Use append mode so worker-side partition batching can reduce file churn.
    let dir = root_dir
        .join(format!("date={date}"))
        .join(format!("market={market}"))
        .join(format!("symbol={symbol}"))
        .join("dataset=raw_md");
    fs::create_dir_all(&dir).with_context(|| format!("create cold-store dir {}", dir.display()))?;

    let file_path = dir.join("append.jsonl");
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&file_path)
        .with_context(|| format!("open cold-store append file {}", file_path.display()))?;
    let mut file = BufWriter::with_capacity(8 * 1024 * 1024, file);

    for row in rows {
        let line = json!({
            "event_ts_ms": row.event_ts_ms,
            "msg_type": row.msg_type,
            "market": row.market,
            "symbol": row.symbol,
            "source_kind": row.source_kind,
            "stream_name": row.stream_name,
            "routing_key": row.routing_key,
            "backfill_in_progress": row.backfill_in_progress,
            "data": row.data_json,
        });
        serde_json::to_writer(&mut file, &line).context("encode cold-store row json")?;
        file.write_all(b"\n").context("write cold-store newline")?;
    }
    file.flush().context("flush cold-store file")?;
    Ok(())
}

#[allow(dead_code)]
fn log_parquet_unavailable_once() {
    warn!("parquet writer unavailable in offline build; cold-store uses jsonl fallback");
}
