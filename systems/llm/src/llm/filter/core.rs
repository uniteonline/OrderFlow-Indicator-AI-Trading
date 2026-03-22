use super::{core_entry, core_management, core_pending};
use crate::llm::{prompt, provider::ModelInvocationInput};
use anyhow::{anyhow, Context, Result};
use serde_json::{json, Map, Value};

pub(crate) struct CoreFilter;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CoreMode {
    Entry,
    Management,
    Pending,
}

impl CoreMode {
    pub(crate) fn from_flags(management_mode: bool, pending_order_mode: bool) -> Self {
        if pending_order_mode {
            Self::Pending
        } else if management_mode {
            Self::Management
        } else {
            Self::Entry
        }
    }

    fn from_input(input: &ModelInvocationInput) -> Self {
        Self::from_flags(input.management_mode, input.pending_order_mode)
    }

    pub(crate) fn stage_label(self) -> &'static str {
        match self {
            Self::Entry => "entry_core",
            Self::Management => "management_core",
            Self::Pending => "pending_core",
        }
    }
}

impl CoreFilter {
    pub(crate) fn build_value(input: &ModelInvocationInput) -> Result<Value> {
        let mut root =
            serde_json::to_value(input).context("serialize core invocation input value")?;
        super::TempIndicatorInputOptimizer::round_derived_fields(&mut root);
        Ok(build_core_root(&root, CoreMode::from_input(input)))
    }

    pub(crate) fn build_finalize_value(
        input: &ModelInvocationInput,
        prior_scan: &Value,
    ) -> Result<Value> {
        let mut value = Self::build_value(input)?;
        if let Some(root) = value.as_object_mut() {
            root.insert(
                "finalize_focus".to_string(),
                json!({
                    "scan_15m_trend": prior_scan.pointer("/15m/trend").and_then(Value::as_str).unwrap_or("unknown"),
                    "scan_4h_trend": prior_scan.pointer("/4h/trend").and_then(Value::as_str).unwrap_or("unknown"),
                    "scan_1d_trend": prior_scan.pointer("/1d/trend").and_then(Value::as_str).unwrap_or("unknown"),
                }),
            );
        }
        Ok(value)
    }

    pub(crate) fn serialize_minified_input(input: &ModelInvocationInput) -> Result<String> {
        super::serialize_prompt_value(
            Self::build_value(input)?,
            "serialize core prompt input json",
        )
    }

    pub(crate) fn serialize_finalize_input(
        input: &ModelInvocationInput,
        prior_scan: &Value,
    ) -> Result<String> {
        super::serialize_prompt_value(
            Self::build_finalize_value(input, prior_scan)?,
            "serialize entry finalize prompt input json",
        )
    }

    pub(crate) fn serialize_prompt_input_minified(
        input: &ModelInvocationInput,
        entry_stage: prompt::EntryPromptStage,
        prior_scan: Option<&Value>,
    ) -> Result<String> {
        if matches!(CoreMode::from_input(input), CoreMode::Entry)
            && matches!(entry_stage, prompt::EntryPromptStage::Finalize)
        {
            let scan =
                prior_scan.ok_or_else(|| anyhow!("missing prior scan for entry finalize"))?;
            return Self::serialize_finalize_input(input, scan);
        }

        Self::serialize_minified_input(input)
    }

    pub(crate) fn stage_label_for_flags(
        management_mode: bool,
        pending_order_mode: bool,
    ) -> &'static str {
        CoreMode::from_flags(management_mode, pending_order_mode).stage_label()
    }
}

fn build_core_root(root: &Value, mode: CoreMode) -> Value {
    let mut result = Map::new();
    if let Some(symbol) = root.get("symbol") {
        result.insert("symbol".to_string(), symbol.clone());
    }
    if let Some(ts_bucket) = root.get("ts_bucket") {
        result.insert("ts_bucket".to_string(), ts_bucket.clone());
    }

    let filtered_indicators = root
        .get("indicators")
        .and_then(Value::as_object)
        .map(|source| match mode {
            CoreMode::Entry => {
                core_entry::filter_indicators(source, root.get("ts_bucket").and_then(Value::as_str))
            }
            CoreMode::Management => core_management::filter_indicators(
                source,
                root.get("ts_bucket").and_then(Value::as_str),
                root.get("management_snapshot"),
            ),
            CoreMode::Pending => core_pending::filter_indicators(
                source,
                root.get("ts_bucket").and_then(Value::as_str),
                root.get("management_snapshot"),
            ),
        })
        .unwrap_or_default();
    result.insert("indicators".to_string(), Value::Object(filtered_indicators));

    if !matches!(mode, CoreMode::Entry) {
        if let Some(trading_state) = root
            .get("trading_state")
            .filter(|value| !value.is_null())
            .cloned()
            .map(|value| {
                if matches!(mode, CoreMode::Pending | CoreMode::Management) {
                    prune_nulls_root(value)
                } else {
                    value
                }
            })
        {
            result.insert("trading_state".to_string(), trading_state);
        }
        if let Some(snapshot) = root
            .get("management_snapshot")
            .filter(|value| !value.is_null())
            .cloned()
            .map(|value| {
                if matches!(mode, CoreMode::Pending | CoreMode::Management) {
                    let mut value = prune_nulls_root(value);
                    if matches!(mode, CoreMode::Management) {
                        strip_management_entry_reason(&mut value);
                    }
                    value
                } else {
                    value
                }
            })
        {
            result.insert("management_snapshot".to_string(), snapshot);
        }
    }

    Value::Object(result)
}

fn strip_management_entry_reason(value: &mut Value) {
    if let Some(entry_context) = value.pointer_mut("/position_context/entry_context") {
        if let Some(entry_context_object) = entry_context.as_object_mut() {
            entry_context_object.remove("entry_reason");
        }
    }
}

fn prune_nulls_root(value: Value) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .into_iter()
                .filter_map(|(key, value)| {
                    let pruned = prune_nulls_root(value);
                    if pruned.is_null() {
                        None
                    } else {
                        Some((key, pruned))
                    }
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(prune_nulls_root)
                .filter(|value| !value.is_null())
                .collect(),
        ),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::CoreFilter;
    use crate::execution::binance::{
        ActivePositionSnapshot, OpenOrderSnapshot, TradingStateSnapshot,
    };
    use crate::llm::provider::{
        EntryContextForLlm, ManagementSnapshotForLlm, ModelInvocationInput,
        PendingOrderSummaryForLlm, PositionContextForLlm, PositionSummaryForLlm,
    };
    use chrono::{DateTime, Duration, SecondsFormat, Utc};
    use serde_json::{json, Map, Value};
    use std::{fs, path::PathBuf};

    fn sample_input(
        indicators: Value,
        management_mode: bool,
        pending_order_mode: bool,
    ) -> ModelInvocationInput {
        let now = Utc::now();
        ModelInvocationInput {
            symbol: "TESTUSDT".to_string(),
            ts_bucket: now,
            window_code: "15m".to_string(),
            indicator_count: indicators.as_object().map(|obj| obj.len()).unwrap_or(0),
            source_routing_key: "llm_indicator_minute".to_string(),
            source_published_at: None,
            received_at: now,
            indicators,
            missing_indicator_codes: vec![],
            management_mode,
            pending_order_mode,
            trading_state: if management_mode || pending_order_mode {
                Some(sample_trading_state())
            } else {
                None
            },
            management_snapshot: if management_mode || pending_order_mode {
                Some(sample_management_snapshot())
            } else {
                None
            },
        }
    }

    fn sample_input_at(
        indicators: Value,
        management_mode: bool,
        pending_order_mode: bool,
        ts_bucket: DateTime<Utc>,
    ) -> ModelInvocationInput {
        let mut input = sample_input(indicators, management_mode, pending_order_mode);
        input.ts_bucket = ts_bucket;
        input.received_at = ts_bucket;
        input
    }

    fn sample_management_snapshot() -> ManagementSnapshotForLlm {
        ManagementSnapshotForLlm {
            context_state: "ACTIVE_POSITION".to_string(),
            has_active_positions: true,
            has_open_orders: true,
            active_position_count: 1,
            open_order_count: 1,
            positions: vec![PositionSummaryForLlm {
                position_side: "LONG".to_string(),
                direction: "LONG".to_string(),
                quantity: 1.0,
                leverage: 10,
                entry_price: 2010.0,
                mark_price: 2025.0,
                unrealized_pnl: 15.0,
                pnl_by_latest_price: 15.0,
                current_tp_price: Some(2055.0),
                current_sl_price: Some(1998.0),
            }],
            pending_order: Some(PendingOrderSummaryForLlm {
                position_side: "LONG".to_string(),
                direction: "LONG".to_string(),
                quantity: 1.0,
                leverage: Some(10),
                entry_price: Some(2005.0),
                current_tp_price: Some(2055.0),
                current_sl_price: Some(1998.0),
                planned_tp_price: Some(2055.0),
                planned_tp_source: Some("effective_context".to_string()),
                planned_sl_price: Some(1998.0),
                planned_sl_source: Some("effective_context".to_string()),
            }),
            last_management_reason: Some("keep structure".to_string()),
            position_context: Some(PositionContextForLlm {
                original_qty: 1.0,
                current_qty: 1.0,
                current_pct_of_original: 100.0,
                effective_leverage: Some(10),
                effective_entry_price: Some(2010.0),
                effective_take_profit: Some(2055.0),
                effective_stop_loss: Some(1998.0),
                reduction_history: vec![],
                times_reduced_at_current_level: 0,
                last_management_action: Some("HOLD".to_string()),
                last_management_reason: Some("keep structure".to_string()),
                entry_context: Some(EntryContextForLlm {
                    entry_strategy: Some("patient_retest".to_string()),
                    stop_model: Some("Value Area Invalidation Stop".to_string()),
                    entry_mode: Some("limit_below_zone".to_string()),
                    original_tp: Some(2055.0),
                    original_sl: Some(1998.0),
                    sweep_wick_extreme: None,
                    horizon: Some("4h".to_string()),
                    entry_reason: "test".to_string(),
                }),
            }),
        }
    }

    fn latest_eth_indicator_snapshot_path() -> Option<PathBuf> {
        let mut paths = fs::read_dir("/data/systems/llm/temp_indicator")
            .ok()?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .map(|name| name.ends_with("_TESTUSDT.json"))
                    .unwrap_or(false)
            })
            .collect::<Vec<_>>();
        paths.sort();
        paths.pop()
    }

    fn load_real_snapshot_input(
        management_mode: bool,
        pending_order_mode: bool,
    ) -> Option<(PathBuf, ModelInvocationInput)> {
        let path = latest_eth_indicator_snapshot_path()?;
        let raw = fs::read_to_string(&path).ok()?;
        let root: Value = serde_json::from_str(&raw).ok()?;
        let indicators = root.get("indicators")?.clone();
        let ts_bucket = root
            .get("ts_bucket")
            .and_then(Value::as_str)
            .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
            .map(|value| value.with_timezone(&Utc))?;

        Some((
            path,
            ModelInvocationInput {
                symbol: root.get("symbol")?.as_str()?.to_string(),
                ts_bucket,
                window_code: root
                    .get("window_code")
                    .and_then(Value::as_str)
                    .unwrap_or("1m")
                    .to_string(),
                indicator_count: root
                    .get("indicator_count")
                    .and_then(Value::as_u64)
                    .map(|value| value as usize)
                    .unwrap_or_else(|| indicators.as_object().map(|obj| obj.len()).unwrap_or(0)),
                source_routing_key: root
                    .get("routing_key")
                    .and_then(Value::as_str)
                    .unwrap_or("llm_indicator_minute")
                    .to_string(),
                source_published_at: root
                    .get("published_at")
                    .and_then(Value::as_str)
                    .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                    .map(|value| value.with_timezone(&Utc)),
                received_at: ts_bucket,
                indicators,
                missing_indicator_codes: vec![],
                management_mode,
                pending_order_mode,
                trading_state: if management_mode || pending_order_mode {
                    Some(sample_trading_state())
                } else {
                    None
                },
                management_snapshot: if management_mode || pending_order_mode {
                    Some(sample_management_snapshot())
                } else {
                    None
                },
            },
        ))
    }

    fn assert_snapshot_budget(
        label: &str,
        size: usize,
        soft_budget: usize,
        hard_budget: usize,
        path: &PathBuf,
    ) {
        if size > soft_budget {
            eprintln!(
                "{label} soft budget drift: bytes={size}, soft_budget={soft_budget}, hard_budget={hard_budget}, path={}",
                path.display()
            );
        }
        assert!(
            size <= hard_budget,
            "{label} minified bytes {size} exceeded hard guardrail {hard_budget} (soft target {soft_budget}) for {}",
            path.display()
        );
    }

    fn sample_trading_state() -> TradingStateSnapshot {
        TradingStateSnapshot {
            symbol: "TESTUSDT".to_string(),
            has_active_context: true,
            has_active_positions: true,
            has_open_orders: true,
            active_positions: vec![ActivePositionSnapshot {
                position_side: "LONG".to_string(),
                position_amt: 1.0,
                entry_price: 2010.0,
                mark_price: 2025.0,
                unrealized_pnl: 15.0,
                leverage: 10,
            }],
            open_orders: vec![OpenOrderSnapshot {
                order_id: 1,
                side: "BUY".to_string(),
                position_side: "LONG".to_string(),
                order_type: "LIMIT".to_string(),
                status: "NEW".to_string(),
                orig_qty: 1.0,
                executed_qty: 0.0,
                price: 2005.0,
                stop_price: 0.0,
                close_position: false,
                reduce_only: false,
                is_algo_order: false,
            }],
            total_wallet_balance: 1000.0,
            available_balance: 900.0,
        }
    }

    fn build_sample_indicators() -> Value {
        let mut indicators = Map::new();
        let footprint_15m_levels = vec![
            json!({"price_level": 2099.9, "buy": 1.0, "sell": 2.0, "delta": -1.0, "total": 3.0, "buy_imbalance": false, "sell_imbalance": false, "is_open": false, "is_high": false, "is_low": false, "is_close": false, "ua_top_flag": false, "ua_bottom_flag": false, "level_rank": 1}),
            json!({"price_level": 2100.0, "buy": 2.0, "sell": 1.0, "delta": 1.0, "total": 3.0, "buy_imbalance": true, "sell_imbalance": false, "is_open": false, "is_high": false, "is_low": false, "is_close": false, "ua_top_flag": false, "ua_bottom_flag": false, "level_rank": 2}),
            json!({"price_level": 2100.1, "buy": 2.0, "sell": 1.0, "delta": 1.0, "total": 999.0, "buy_imbalance": false, "sell_imbalance": false, "is_open": false, "is_high": false, "is_low": false, "is_close": false, "ua_top_flag": false, "ua_bottom_flag": false, "level_rank": 3}),
            json!({"price_level": 2100.2, "buy": 1.0, "sell": 2.0, "delta": -1.0, "total": 3.0, "buy_imbalance": false, "sell_imbalance": false, "is_open": false, "is_high": true, "is_low": false, "is_close": false, "ua_top_flag": false, "ua_bottom_flag": false, "level_rank": 4}),
            json!({"price_level": 2100.3, "buy": 20.0, "sell": 10.0, "delta": 10.0, "total": 500.0, "buy_imbalance": false, "sell_imbalance": false, "is_open": false, "is_high": false, "is_low": false, "is_close": false, "ua_top_flag": false, "ua_bottom_flag": false, "level_rank": 5}),
            json!({"price_level": 2100.4, "buy": 20.0, "sell": 10.0, "delta": 10.0, "total": 499.0, "buy_imbalance": false, "sell_imbalance": false, "is_open": false, "is_high": false, "is_low": false, "is_close": false, "ua_top_flag": false, "ua_bottom_flag": false, "level_rank": 6}),
            json!({"price_level": 2100.5, "buy": 20.0, "sell": 10.0, "delta": 10.0, "total": 498.0, "buy_imbalance": false, "sell_imbalance": false, "is_open": false, "is_high": false, "is_low": false, "is_close": false, "ua_top_flag": false, "ua_bottom_flag": false, "level_rank": 7}),
            json!({"price_level": 2100.6, "buy": 20.0, "sell": 10.0, "delta": 10.0, "total": 497.0, "buy_imbalance": false, "sell_imbalance": false, "is_open": false, "is_high": false, "is_low": false, "is_close": false, "ua_top_flag": false, "ua_bottom_flag": false, "level_rank": 8}),
            json!({"price_level": 2100.7, "buy": 20.0, "sell": 10.0, "delta": 10.0, "total": 496.0, "buy_imbalance": false, "sell_imbalance": false, "is_open": false, "is_high": false, "is_low": false, "is_close": false, "ua_top_flag": false, "ua_bottom_flag": false, "level_rank": 9}),
            json!({"price_level": 2100.8, "buy": 20.0, "sell": 10.0, "delta": 10.0, "total": 495.0, "buy_imbalance": false, "sell_imbalance": false, "is_open": false, "is_high": false, "is_low": false, "is_close": false, "ua_top_flag": false, "ua_bottom_flag": false, "level_rank": 10}),
            json!({"price_level": 2100.9, "buy": 20.0, "sell": 10.0, "delta": 10.0, "total": 494.0, "buy_imbalance": false, "sell_imbalance": false, "is_open": false, "is_high": false, "is_low": false, "is_close": false, "ua_top_flag": false, "ua_bottom_flag": false, "level_rank": 11}),
            json!({"price_level": 2101.0, "buy": 20.0, "sell": 10.0, "delta": 10.0, "total": 493.0, "buy_imbalance": false, "sell_imbalance": false, "is_open": false, "is_high": false, "is_low": false, "is_close": false, "ua_top_flag": false, "ua_bottom_flag": false, "level_rank": 12}),
            json!({"price_level": 2101.1, "buy": 20.0, "sell": 10.0, "delta": 10.0, "total": 492.0, "buy_imbalance": false, "sell_imbalance": false, "is_open": false, "is_high": false, "is_low": false, "is_close": false, "ua_top_flag": false, "ua_bottom_flag": false, "level_rank": 13}),
            json!({"price_level": 2101.2, "buy": 20.0, "sell": 10.0, "delta": 10.0, "total": 491.0, "buy_imbalance": false, "sell_imbalance": false, "is_open": false, "is_high": false, "is_low": false, "is_close": false, "ua_top_flag": false, "ua_bottom_flag": false, "level_rank": 14}),
        ];
        indicators.insert("vpin".to_string(), json!({"payload": {"score": 0.45}}));
        indicators.insert(
            "whale_trades".to_string(),
            json!({"payload": {"latest": {"side": "BUY", "notional": 100000.0}}}),
        );
        indicators.insert(
            "high_volume_pulse".to_string(),
            json!({"payload": {"pulse": "elevated"}}),
        );
        indicators.insert(
            "price_volume_structure".to_string(),
            json!({
                "payload": {
                    "poc_price": 2100.0,
                    "poc_volume": 12.0,
                    "vah": 2110.0,
                    "val": 2090.0,
                    "bar_volume": 100.0,
                    "hvn_levels": [2101.0],
                    "lvn_levels": [2098.0],
                    "value_area_levels": sample_value_area_levels(2090.0, 0.5, 8, 100.0),
                    "levels": [{"price": 2100.0}],
                    "volume_zscore": 1.2,
                    "volume_dryup": false,
                    "by_window": {
                        "15m": {"window_bars_used": 10, "levels": [{"price": 2100.0}], "poc_price": 2100.0, "poc_volume": 12.0, "vah": 2110.0, "val": 2090.0, "hvn_levels": [2101.0], "lvn_levels": [2098.0], "value_area_levels": sample_value_area_levels(2090.0, 0.25, 18, 120.0), "volume_zscore": 1.2, "volume_dryup": false, "bar_volume": 100.0},
                        "4h": {"window_bars_used": 20, "levels": [{"price": 2100.0}], "poc_price": 2100.0, "poc_volume": 12.0, "vah": 2110.0, "val": 2090.0, "hvn_levels": [2101.0], "lvn_levels": [2098.0], "value_area_levels": sample_value_area_levels(2088.0, 0.5, 12, 140.0), "volume_zscore": 1.2, "volume_dryup": false, "bar_volume": 100.0},
                        "1d": {"window_bars_used": 14, "levels": [{"price": 2100.0}], "poc_price": 2100.0, "poc_volume": 12.0, "vah": 2110.0, "val": 2090.0, "hvn_levels": [2101.0], "lvn_levels": [2098.0], "value_area_levels": sample_value_area_levels(2085.0, 1.0, 13, 180.0), "volume_zscore": 1.2, "volume_dryup": false, "bar_volume": 100.0},
                        "3d": {"window_bars_used": 30, "levels": [{"price": 2100.0}], "poc_price": 2100.0, "poc_volume": 12.0, "vah": 2110.0, "val": 2090.0, "hvn_levels": [2101.0], "lvn_levels": [2098.0], "value_area_levels": sample_value_area_levels(2080.0, 2.0, 14, 220.0), "volume_zscore": 1.2, "volume_dryup": false, "bar_volume": 100.0}
                    }
                }
            }),
        );
        indicators.insert(
            "fvg".to_string(),
            json!({
                "payload": {
                    "base_detection_uses_spot": true,
                    "source_market": "futures",
                    "by_window": {
                        "15m": sample_fvg_window(10),
                        "4h": sample_fvg_window(6),
                        "1d": sample_fvg_window(6),
                        "3d": sample_fvg_window(6)
                    }
                }
            }),
        );
        indicators.insert(
            "avwap".to_string(),
            json!({
                "payload": {
                    "anchor_ts": "2026-03-15T00:00:00Z",
                    "avwap_fut": 2000.0,
                    "avwap_spot": 2001.0,
                    "fut_last_price": 2002.0,
                    "fut_mark_price": 2002.0,
                    "lookback": "7d",
                    "price_minus_avwap_fut": 2.0,
                    "price_minus_spot_avwap_fut": 1.0,
                    "price_minus_spot_avwap_futmark": 1.5,
                    "xmk_avwap_gap_f_minus_s": -1.0,
                    "zavwap_gap": -0.2,
                    "series_by_window": {
                        "15m": sample_avwap_series("2026-03-15T00:00:00Z", 15, 10),
                        "4h": sample_avwap_series("2026-03-14T00:00:00Z", 240, 4),
                        "1d": sample_avwap_series("2026-03-10T00:00:00Z", 1440, 3)
                    }
                }
            }),
        );
        indicators.insert(
            "rvwap_sigma_bands".to_string(),
            json!({
                "payload": {
                    "as_of_ts": "2026-03-15T01:45:00Z",
                    "source_mode": "futures",
                    "by_window": {
                        "15m": {"rvwap_w": 2000.0, "rvwap_sigma_w": 4.0, "rvwap_band_minus_1": 1996.0, "rvwap_band_minus_2": 1992.0, "rvwap_band_plus_1": 2004.0, "rvwap_band_plus_2": 2008.0, "z_price_minus_rvwap": 0.5, "samples_used": 10},
                        "4h": {"rvwap_w": 2001.0, "rvwap_sigma_w": 5.0, "rvwap_band_minus_1": 1996.0, "rvwap_band_minus_2": 1991.0, "rvwap_band_plus_1": 2006.0, "rvwap_band_plus_2": 2011.0, "z_price_minus_rvwap": 0.2, "samples_used": 10},
                        "1d": {"rvwap_w": 2002.0, "rvwap_sigma_w": 6.0, "rvwap_band_minus_1": 1996.0, "rvwap_band_minus_2": 1990.0, "rvwap_band_plus_1": 2008.0, "rvwap_band_plus_2": 2014.0, "z_price_minus_rvwap": 0.1, "samples_used": 10}
                    },
                    "series_by_output_window": {"15m": [{"ts": "2026-03-15T01:30:00Z"}]}
                }
            }),
        );
        indicators.insert(
            "footprint".to_string(),
            json!({
                "payload": {
                    "by_window": {
                        "15m": {
                            "window_delta": 10.0,
                            "window_total_qty": 100.0,
                            "unfinished_auction": true,
                            "ua_top": 2102.0,
                            "ua_bottom": 2098.0,
                            "stacked_buy": true,
                            "stacked_sell": false,
                            "buy_imbalance_prices": [2100.11, 2100.14, 2100.26],
                            "sell_imbalance_prices": [2101.21, 2101.24],
                            "buy_stacks": [2100.1],
                            "sell_stacks": [2101.2],
                            "max_buy_stack_len": 2,
                            "max_sell_stack_len": 1,
                            "levels": footprint_15m_levels
                        },
                        "4h": {
                            "window_delta": 20.0,
                            "window_total_qty": 200.0,
                            "unfinished_auction": false,
                            "ua_top": 2110.0,
                            "ua_bottom": 2080.0,
                            "stacked_buy": false,
                            "stacked_sell": true,
                            "buy_imbalance_prices": [2100.1, 2100.4, 2100.6],
                            "sell_imbalance_prices": [2101.1, 2101.4],
                            "buy_stacks": [],
                            "sell_stacks": [],
                            "max_buy_stack_len": 0,
                            "max_sell_stack_len": 0
                        },
                        "1d": {
                            "window_delta": 30.0,
                            "window_total_qty": 300.0,
                            "unfinished_auction": true,
                            "ua_top": 2120.0,
                            "ua_bottom": 2070.0,
                            "stacked_buy": true,
                            "stacked_sell": true,
                            "buy_imbalance_prices": [2100.0, 2101.0, 2102.0],
                            "sell_imbalance_prices": [2103.0],
                            "buy_stacks": [],
                            "sell_stacks": [],
                            "max_buy_stack_len": 0,
                            "max_sell_stack_len": 0
                        }
                    }
                }
            }),
        );
        indicators.insert(
            "orderbook_depth".to_string(),
            json!({
                "payload": {
                    "obi_fut": 0.02,
                    "obi_k_dw_twa_fut": 0.12,
                    "microprice_fut": 2100.0,
                    "bbo_updates_fut": 42,
                    "relative_delta_spot": -0.33,
                    "levels": sample_orderbook_levels(120),
                    "by_window": {
                        "15m": {"obi_fut": 0.02, "spread_twa_fut": 0.003},
                        "1h": {"obi_fut": 0.03, "spread_twa_fut": 0.004}
                    }
                }
            }),
        );
        indicators.insert(
            "kline_history".to_string(),
            json!({
                "payload": {
                    "as_of_ts": "2026-03-15T01:45:00Z",
                    "intervals": {
                        "15m": {"markets": {"futures": {"bars": sample_kline_bars("2026-03-14T18:30:00Z", 15, 30)}}},
                        "4h": {"markets": {"futures": {"bars": sample_kline_bars("2026-03-12T00:00:00Z", 240, 18)}}},
                        "1d": {"markets": {"futures": {"bars": sample_kline_bars("2026-03-01T00:00:00Z", 1440, 12)}}}
                    }
                }
            }),
        );
        indicators.insert(
            "ema_trend_regime".to_string(),
            json!({
                "payload": {
                    "as_of_ts": "2026-03-15T01:45:00Z",
                    "output_sampling": "15m",
                    "ema_13": 1.0,
                    "ema_21": 2.0,
                    "ema_34": 3.0,
                    "ema_band_high": 4.0,
                    "ema_band_low": 5.0,
                    "trend_regime": "bull",
                    "ema_100_htf": {"4h": 6.0},
                    "ema_200_htf": {"4h": 7.0},
                    "trend_regime_by_tf": {"4h": "bull", "1d": "bull"},
                    "ffill_series_by_output_window": {"15m": [{"ts": "2026-03-15T01:30:00Z"}]}
                }
            }),
        );
        indicators.insert(
            "cvd_pack".to_string(),
            json!({
                "payload": {
                    "delta_fut": 1.0,
                    "delta_spot": 2.0,
                    "relative_delta_fut": 3.0,
                    "relative_delta_spot": 4.0,
                    "likely_driver": "spot_led",
                    "spot_flow_dominance": 0.7,
                    "spot_lead_score": 0.8,
                    "xmk_delta_gap_s_minus_f": 0.9,
                    "cvd_slope_fut": 1.1,
                    "cvd_slope_spot": 1.2,
                    "by_window": {
                        "15m": {"series": sample_cvd_series("2026-03-14T21:00:00Z", 15, 20)},
                        "4h": {"series": sample_cvd_series("2026-03-12T00:00:00Z", 240, 12)},
                        "1d": {"series": sample_cvd_series("2026-03-06T00:00:00Z", 1440, 7)}
                    }
                }
            }),
        );
        indicators.insert(
            "funding_rate".to_string(),
            json!({
                "payload": {
                    "funding_current": -0.00004527,
                    "funding_twa": -0.00003111,
                    "mark_price_last": 2002.0,
                    "mark_price_last_ts": "2026-03-15T01:45:00Z",
                    "mark_price_twap": 2001.0,
                    "funding_current_effective_ts": "2026-03-15T01:45:00Z",
                    "by_window": {
                        "15m": {"funding_twa": -0.0000123, "change_count": 6, "changes": sample_funding_changes("2026-03-15T00:00:00Z", 15, 6)},
                        "4h": {"funding_twa": -0.0000222, "change_count": 6, "changes": sample_funding_changes("2026-03-14T00:00:00Z", 240, 6)},
                        "1d": {"funding_twa": -0.0000333, "change_count": 6, "changes": sample_funding_changes("2026-03-10T00:00:00Z", 1440, 6)}
                    },
                    "recent_7d": sample_funding_recent("2026-03-13T00:00:00Z", 60, 36)
                }
            }),
        );
        indicators.insert(
            "liquidation_density".to_string(),
            json!({
                "payload": {
                    "by_window": {
                        "15m": {"long_total": 1.0, "short_total": 2.0, "peak_levels": [{"price": 2100.0}], "coverage_ratio": 1.0, "is_ready": true, "levels_count": 1},
                        "4h": {"long_total": 3.0, "short_total": 4.0, "peak_levels": [], "coverage_ratio": 1.0, "is_ready": true, "levels_count": 0},
                        "1d": {"long_total": 5.0, "short_total": 6.0, "peak_levels": [], "coverage_ratio": 1.0, "is_ready": true, "levels_count": 0}
                    },
                    "recent_7d": sample_liquidation_recent("2026-03-12T00:00:00Z", 60, 60)
                }
            }),
        );
        for code in [
            "absorption",
            "buying_exhaustion",
            "selling_exhaustion",
            "initiation",
            "bullish_absorption",
            "bearish_absorption",
            "bearish_initiation",
        ] {
            indicators.insert(
                code.to_string(),
                json!({
                    "payload": {
                        "recent_7d": {
                            "event_count": 12,
                            "lookback_coverage_ratio": 1.0,
                            "events": sample_signal_events("2026-03-14T18:00:00Z", 15, 12)
                        }
                    }
                }),
            );
        }
        indicators.insert(
            "bullish_initiation".to_string(),
            json!({
                "payload": {
                    "recent_7d": {
                        "event_count": 22,
                        "lookback_coverage_ratio": 1.0,
                        "events": sample_signal_events("2026-03-14T18:00:00Z", 15, 22)
                    }
                }
            }),
        );
        indicators.insert(
            "divergence".to_string(),
            json!({
                "payload": {
                    "signal": false,
                    "signals": {"bearish_divergence": false},
                    "latest_7d": {"event_end_ts": "2026-03-15T01:45:00Z", "score": 0.9},
                    "event_count": 12,
                    "divergence_type": null,
                    "likely_driver": "spot_led",
                    "spot_lead_score": 0.9,
                    "pivot_side": "high",
                    "reason": "no_candidate",
                    "recent_7d": {
                        "event_count": 12,
                        "events": sample_events("2026-03-14T18:00:00Z", 15, 12)
                    },
                    "candidates": sample_divergence_candidates(7)
                }
            }),
        );
        indicators.insert(
            "tpo_market_profile".to_string(),
            json!({
                "payload": {
                    "tpo_poc": 2100.0,
                    "tpo_vah": 2110.0,
                    "tpo_val": 2090.0,
                    "initial_balance_high": 2112.0,
                    "initial_balance_low": 2088.0,
                    "by_session": {
                        "1d": {
                            "tpo_poc": 2100.0,
                            "tpo_vah": 2110.0,
                            "tpo_val": 2090.0,
                            "initial_balance_high": 2112.0,
                            "initial_balance_low": 2088.0,
                            "dev_series": {
                                "15m": [
                                    {"ts": "2026-03-15T01:30:00Z", "tpo_dev_poc": 1.0},
                                    {"ts": "2026-03-15T01:45:00Z", "tpo_dev_poc": 2.0}
                                ]
                            }
                        }
                    },
                    "dev_series": {
                        "15m": [
                            {"ts": "2026-03-15T01:30:00Z", "tpo_dev_poc": 1.0},
                            {"ts": "2026-03-15T01:45:00Z", "tpo_dev_poc": 2.0}
                        ]
                    }
                }
            }),
        );

        Value::Object(indicators)
    }

    fn sample_fvg_window(count: usize) -> Value {
        let times = sample_time_strings("2026-03-14T20:00:00Z", 15, count);
        json!({
            "fvgs": times.iter().enumerate().map(|(idx, ts)| json!({"birth_ts": ts, "upper": 2000.0 + idx as f64, "lower": 1999.0 + idx as f64})).collect::<Vec<_>>(),
            "active_bull_fvgs": times.iter().rev().take(2).enumerate().map(|(idx, ts)| json!({"birth_ts": ts, "upper": 2000.0 + idx as f64, "lower": 1999.0 + idx as f64})).collect::<Vec<_>>(),
            "active_bear_fvgs": times.iter().rev().take(2).enumerate().map(|(idx, ts)| json!({"birth_ts": ts, "upper": 2010.0 + idx as f64, "lower": 2009.0 + idx as f64})).collect::<Vec<_>>(),
            "nearest_bull_fvg": {"upper": 2001.0, "lower": 2000.0},
            "nearest_bear_fvg": {"upper": 2009.0, "lower": 2008.0},
            "is_ready": true,
            "coverage_ratio": 1.0
        })
    }

    fn sample_avwap_series(start: &str, step_minutes: i64, count: usize) -> Vec<Value> {
        sample_time_strings(start, step_minutes, count)
            .into_iter()
            .enumerate()
            .map(|(idx, ts)| {
                json!({
                    "ts": ts,
                    "avwap_fut": 2000.0 + idx as f64,
                    "avwap_spot": 2001.0 + idx as f64,
                    "xmk_avwap_gap_f_minus_s": -0.1
                })
            })
            .collect()
    }

    fn sample_kline_bars(start: &str, step_minutes: i64, count: usize) -> Vec<Value> {
        sample_time_strings(start, step_minutes, count)
            .into_iter()
            .enumerate()
            .map(|(idx, ts)| {
                json!({
                    "open": 2000.0 + idx as f64,
                    "high": 2001.0 + idx as f64,
                    "low": 1999.0 + idx as f64,
                    "close": 2000.5 + idx as f64,
                    "volume_base": 100.0 + idx as f64,
                    "open_time": ts,
                    "is_closed": true
                })
            })
            .collect()
    }

    fn sample_cvd_series(start: &str, step_minutes: i64, count: usize) -> Vec<Value> {
        sample_time_strings(start, step_minutes, count)
            .into_iter()
            .enumerate()
            .map(|(idx, ts)| {
                json!({
                    "ts": ts,
                    "close_fut": 2000.0 + idx as f64,
                    "close_spot": 2001.0 + idx as f64,
                    "delta_fut": 1.0 + idx as f64,
                    "delta_spot": 2.0 + idx as f64,
                    "relative_delta_fut": 3.0 + idx as f64,
                    "relative_delta_spot": 4.0 + idx as f64,
                    "cvd_7d_fut": 5.0 + idx as f64,
                    "spot_flow_dominance": 0.5,
                    "xmk_delta_gap_s_minus_f": 0.1
                })
            })
            .collect()
    }

    fn sample_value_area_levels(
        start_price: f64,
        step: f64,
        count: usize,
        base_volume: f64,
    ) -> Vec<Value> {
        (0..count)
            .map(|idx| {
                json!({
                    "price_level": start_price + step * idx as f64,
                    "volume": base_volume + (count - idx) as f64 * 10.0
                })
            })
            .collect()
    }

    fn sample_funding_changes(start: &str, step_minutes: i64, count: usize) -> Vec<Value> {
        sample_time_strings(start, step_minutes, count)
            .into_iter()
            .enumerate()
            .map(|(idx, ts)| {
                json!({
                    "change_ts": ts,
                    "funding_delta": -(idx as f64) * 0.000001,
                    "funding_new": -(idx as f64) * 0.00001,
                    "funding_prev": -((idx + 1) as f64) * 0.00001
                })
            })
            .collect()
    }

    fn sample_funding_recent(start: &str, step_minutes: i64, count: usize) -> Vec<Value> {
        sample_time_strings(start, step_minutes, count)
            .into_iter()
            .enumerate()
            .map(|(idx, ts)| {
                let funding_new = if idx % 3 == 0 {
                    -((idx + 1) as f64) * 0.00001
                } else {
                    (idx as f64) * 0.00001
                };
                json!({"change_ts": ts, "funding_new": funding_new})
            })
            .collect()
    }

    fn sample_liquidation_recent(start: &str, step_minutes: i64, count: usize) -> Vec<Value> {
        sample_time_strings(start, step_minutes, count)
            .into_iter()
            .enumerate()
            .map(|(idx, ts)| {
                let is_spike = idx == count.saturating_sub(2);
                json!({
                    "ts_snapshot": ts,
                    "long_total": if is_spike { 25.0 } else { 10.0 + idx as f64 },
                    "short_total": if is_spike { 450.0 } else { 5.0 + (idx as f64) / 2.0 }
                })
            })
            .collect()
    }

    fn sample_events(start: &str, step_minutes: i64, count: usize) -> Vec<Value> {
        sample_time_strings(start, step_minutes, count)
            .into_iter()
            .enumerate()
            .map(|(idx, ts)| json!({"event_end_ts": ts, "score": idx as f64 / 10.0}))
            .collect()
    }

    fn sample_signal_events(start: &str, step_minutes: i64, count: usize) -> Vec<Value> {
        sample_time_strings(start, step_minutes, count)
            .into_iter()
            .enumerate()
            .map(|(idx, ts)| {
                json!({
                    "event_id": format!("evt-{idx}"),
                    "indicator_code": "absorption",
                    "start_ts": ts,
                    "end_ts": ts,
                    "event_start_ts": ts,
                    "event_end_ts": ts,
                    "event_available_ts": ts,
                    "confirm_ts": ts,
                    "direction": if idx % 2 == 0 { "bullish" } else { "bearish" },
                    "pivot_price": 2100.0 + idx as f64,
                    "price_high": 2101.0 + idx as f64,
                    "price_low": 2099.0 + idx as f64,
                    "score": idx as f64 / 10.0,
                    "score_base": idx as f64 / 20.0,
                    "strength_score_xmk": idx as f64 / 20.0,
                    "trigger_side": if idx % 2 == 0 { "buy" } else { "sell" },
                    "type": "absorption",
                    "delta_sum": idx as f64 * 10.0,
                    "reject_ratio": 0.5,
                    "stacked_buy_imbalance": idx % 2 == 0,
                    "stacked_sell_imbalance": idx % 2 == 1,
                    "key_distance_ticks": idx + 1,
                    "spot_flow_confirm_score": 0.7,
                    "spot_whale_confirm_score": 0.6,
                    "spot_rdelta_1m_mean": 12.0,
                    "spot_cvd_1m_change": 18.0
                })
            })
            .collect()
    }

    fn sample_divergence_candidates(count: usize) -> Vec<Value> {
        (0..count)
            .map(|idx| {
                json!({
                    "type": "bearish_divergence",
                    "score": (count - idx) as f64 / 10.0,
                    "price_start": 2000.0 + idx as f64,
                    "price_end": 2010.0 + idx as f64,
                    "likely_driver": if idx % 2 == 0 { "spot_led" } else { "mixed" }
                })
            })
            .collect()
    }

    fn sample_orderbook_levels(count: usize) -> Vec<Value> {
        (0..count)
            .map(|idx| {
                let total = (count - idx) as f64;
                json!({
                    "price_level": 1950.0 + idx as f64,
                    "bid_liquidity": total,
                    "ask_liquidity": total / 2.0,
                    "total_liquidity": total * 1.5,
                    "net_liquidity": total / 2.0,
                    "level_imbalance": 0.5
                })
            })
            .collect()
    }

    fn sample_time_strings(start: &str, step_minutes: i64, count: usize) -> Vec<String> {
        let start = DateTime::parse_from_rfc3339(start)
            .expect("parse sample start ts")
            .with_timezone(&Utc);
        (0..count)
            .map(|idx| {
                (start + Duration::minutes(step_minutes * idx as i64))
                    .to_rfc3339_opts(SecondsFormat::Secs, true)
            })
            .collect()
    }

    fn assert_newest_first(value: &Value, pointer: &str, ts_field: &str) {
        let series = value
            .pointer(pointer)
            .and_then(Value::as_array)
            .unwrap_or_else(|| panic!("missing array at {pointer}"));
        let first_ts = series
            .first()
            .and_then(|entry| entry.get(ts_field))
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("missing first {ts_field} at {pointer}"));
        let last_ts = series
            .last()
            .and_then(|entry| entry.get(ts_field))
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("missing last {ts_field} at {pointer}"));
        assert!(
            first_ts >= last_ts,
            "expected newest-first ordering at {pointer}, got first={first_ts}, last={last_ts}"
        );
    }

    fn assert_no_nulls(value: &Value) {
        match value {
            Value::Null => panic!("unexpected null in filtered output"),
            Value::Array(items) => {
                for item in items {
                    assert_no_nulls(item);
                }
            }
            Value::Object(object) => {
                for item in object.values() {
                    assert_no_nulls(item);
                }
            }
            _ => {}
        }
    }

    fn extracted_footprint_prices(value: &Value, pointer: &str) -> Vec<f64> {
        value
            .pointer(pointer)
            .and_then(Value::as_array)
            .unwrap_or(&Vec::new())
            .iter()
            .filter_map(|entry| entry.get("price_level").and_then(Value::as_f64))
            .collect()
    }

    #[test]
    fn build_entry_core_value_applies_v4_rules() {
        let input = sample_input(build_sample_indicators(), false, false);

        let value = CoreFilter::build_value(&input).expect("build entry core value");

        assert_eq!(value.as_object().map(|obj| obj.len()), Some(3));
        assert!(value.pointer("/management_snapshot").is_none());
        assert!(value.pointer("/trading_state").is_none());
        assert_eq!(
            value
                .pointer("/indicators/fvg")
                .and_then(Value::as_object)
                .map(|obj| obj.len()),
            Some(1)
        );
        assert!(value.pointer("/indicators/fvg/window_code").is_none());
        assert!(value
            .pointer("/indicators/atr_context/payload/latest_15m_detail/summary/atr14")
            .and_then(Value::as_f64)
            .map(|value| value > 0.0)
            .unwrap_or(false));
        assert!(value
            .pointer("/indicators/atr_context/payload/latest_4h_context/summary/atr14")
            .and_then(Value::as_f64)
            .map(|value| value > 0.0)
            .unwrap_or(false));
        assert!(value
            .pointer("/indicators/atr_context/payload/latest_1d_background/summary/atr14")
            .and_then(Value::as_f64)
            .map(|value| value > 0.0)
            .unwrap_or(false));
        assert!(value
            .pointer("/indicators/atr_context/payload/latest_15m_detail/summary/atr14_pct")
            .and_then(Value::as_f64)
            .map(|value| value > 0.0)
            .unwrap_or(false));
        assert!(value
            .pointer("/indicators/price_volume_structure/payload/levels")
            .is_none());
        assert!(value
            .pointer("/indicators/price_volume_structure/payload/by_window/15m/levels")
            .is_none());
        assert!(value
            .pointer("/indicators/price_volume_structure/payload/value_area_levels")
            .is_none());
        assert_eq!(
            value
                .pointer("/indicators/price_volume_structure/payload/va_top_levels")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(8)
        );
        assert_eq!(
            value
                .pointer("/indicators/price_volume_structure/payload/by_window/15m/va_top_levels")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(15)
        );
        assert_eq!(
            value
                .pointer("/indicators/price_volume_structure/payload/by_window/4h/va_top_levels")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(10)
        );
        assert!(value
            .pointer("/indicators/price_volume_structure/payload/by_window/15m/value_area_levels")
            .is_none());
        assert!(value
            .pointer(
                "/indicators/price_volume_structure/payload/by_window/15m/va_top_levels/0/volume"
            )
            .is_some());
        assert!(value
            .pointer(
                "/indicators/price_volume_structure/payload/by_window/15m/va_top_levels/0/vol_rank"
            )
            .is_some());
        assert!(value
            .pointer("/indicators/fvg/payload/by_window/3d")
            .is_some());
        assert_eq!(
            value
                .pointer("/indicators/fvg/payload/by_window/15m/fvgs")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(6)
        );
        let footprint_prices = extracted_footprint_prices(
            &value,
            "/indicators/footprint/payload/by_window/15m/levels",
        );
        assert!(footprint_prices.contains(&2100.0));
        assert!(footprint_prices.contains(&2100.1));
        assert!(!footprint_prices.contains(&2099.9));
        assert!(footprint_prices.len() <= 12);
        assert!(value
            .pointer("/indicators/footprint/payload/by_window/15m/buy_imbalance_prices")
            .is_none());
        assert!(value
            .pointer("/indicators/footprint/payload/by_window/15m/sell_imbalance_prices")
            .is_none());
        assert!(value
            .pointer("/indicators/footprint/payload/by_window/4h/buy_imbalance_prices")
            .is_none());
        assert!(value
            .pointer("/indicators/footprint/payload/by_window/15m/levels/0/level_rank")
            .is_none());
        assert!(value
            .pointer("/indicators/fvg/payload/by_window/15m/fvgs/0/fvg_id")
            .is_none());
        assert!(value
            .pointer("/indicators/fvg/payload/by_window/15m/fvgs/0/tf")
            .is_none());
        assert!(value
            .pointer("/indicators/fvg/payload/by_window/15m/fvgs/0/upper")
            .is_none());
        assert!(value
            .pointer("/indicators/fvg/payload/by_window/15m/fvgs/0/lower")
            .is_none());
        assert!(value
            .pointer("/indicators/fvg/payload/by_window/15m/fvgs/0/mid")
            .is_none());
        assert!(value
            .pointer("/indicators/fvg/payload/by_window/15m/fvgs/0/body_ratio_mid")
            .is_none());
        assert!(value
            .pointer("/indicators/fvg/payload/by_window/15m/fvgs/0/distance_to_avwap")
            .is_none());
        assert!(value
            .pointer("/indicators/fvg/payload/by_window/15m/fvgs/0/spot_confirm_at_birth")
            .is_none());
        assert!(value
            .pointer("/indicators/fvg/payload/by_window/15m/fvgs/0/fvg_top")
            .is_some());
        assert!(value
            .pointer("/indicators/fvg/payload/by_window/15m/fvgs/0/fvg_bottom")
            .is_some());
        assert!(value
            .pointer("/indicators/absorption/payload/recent_7d/events/0/event_id")
            .is_none());
        assert!(value
            .pointer("/indicators/absorption/payload/recent_7d/events/0/indicator_code")
            .is_none());
        assert!(value
            .pointer("/indicators/absorption/payload/recent_7d/events/0/start_ts")
            .is_none());
        assert!(value
            .pointer("/indicators/absorption/payload/recent_7d/events/0/end_ts")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/latest_7d/event_id")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/latest_7d/start_ts")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/latest_7d/end_ts")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/latest_7d/event_available_ts")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/latest_7d/cvd_norm_diff_fut")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/latest_7d/cvd_norm_diff_spot")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/latest_7d/price_norm_diff")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/latest_7d/sig_test_mode")
            .is_none());
        assert!(value
            .pointer("/indicators/footprint/payload/by_window/4h/buy_imb_clusters")
            .is_some());
        assert_eq!(
            value
                .pointer("/indicators/orderbook_depth/payload/top_liquidity_levels")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(20)
        );
        let top_liquidity_levels = value
            .pointer("/indicators/orderbook_depth/payload/top_liquidity_levels")
            .and_then(Value::as_array)
            .expect("entry top_liquidity_levels");
        assert!(top_liquidity_levels.iter().all(|entry| {
            entry
                .get("price_level")
                .and_then(Value::as_f64)
                .map(|price| (1961.96..=2042.04).contains(&price))
                .unwrap_or(false)
        }));
        assert!(value
            .pointer("/indicators/orderbook_depth/payload/liquidity_walls/bid_walls")
            .is_some());
        assert!(value
            .pointer("/indicators/orderbook_depth/payload/liquidity_walls/ask_walls")
            .is_some());
        assert!(value
            .pointer("/indicators/orderbook_depth/payload/liquidity_walls/depth_imbalance_1pct")
            .is_some());
        assert!(value
            .pointer("/indicators/orderbook_depth/payload/bbo_updates_fut")
            .is_none());
        assert!(value
            .pointer("/indicators/orderbook_depth/payload/obi")
            .is_none());
        assert_eq!(
            value
                .pointer("/indicators/avwap/payload/series_by_window/15m")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(5)
        );
        assert_eq!(
            value
                .pointer("/indicators/kline_history/payload/intervals/15m/markets/futures")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(20)
        );
        assert_eq!(
            value
                .pointer("/indicators/kline_history/payload/intervals/4h/markets/futures")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(12)
        );
        assert_eq!(
            value
                .pointer("/indicators/kline_history/payload/intervals/1d/markets/futures")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(8)
        );
        assert_eq!(
            value
                .pointer("/indicators/cvd_pack/payload/by_window/15m/series")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(15)
        );
        assert!(value
            .pointer("/indicators/cvd_pack/payload/by_window/15m/series/0/close_fut")
            .is_none());
        assert!(value
            .pointer("/indicators/cvd_pack/payload/by_window/15m/series/0/relative_delta_fut")
            .is_none());
        assert!(value
            .pointer("/indicators/cvd_pack/payload/by_window/15m/series/0/delta_fut")
            .is_some());
        assert_eq!(
            value
                .pointer("/indicators/funding_rate/payload/funding_trend_hourly")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(24)
        );
        assert!(value
            .pointer("/indicators/funding_rate/payload/funding_trend_stats/avg_7d")
            .is_some());
        assert!(value
            .pointer("/indicators/funding_rate/payload/funding_summary/ema_8h")
            .is_some());
        assert_eq!(
            value
                .pointer("/indicators/liquidation_density/payload/liq_trend_hourly")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(48)
        );
        assert_eq!(
            value
                .pointer("/indicators/liquidation_density/payload/liq_spike_events")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(1)
        );
        assert!(value.pointer("/indicators/bullish_absorption").is_none());
        assert!(value.pointer("/indicators/bearish_absorption").is_none());
        assert!(value.pointer("/indicators/bullish_initiation").is_none());
        assert!(value.pointer("/indicators/bearish_initiation").is_none());
        assert_eq!(
            value
                .pointer("/indicators/absorption/payload/recent_7d/events")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(10)
        );
        assert!(value
            .pointer("/indicators/absorption/payload/recent_7d/events/0/score_base")
            .is_none());
        assert!(value
            .pointer("/indicators/absorption/payload/recent_7d/events/0/strength_score_xmk")
            .is_none());
        assert!(value
            .pointer("/indicators/absorption/payload/recent_7d/events/0/pivot_price")
            .is_some());
        assert_eq!(
            value
                .pointer("/indicators/divergence/payload/recent_7d/events")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(10)
        );
        assert!(value
            .pointer("/indicators/divergence/payload/recent_7d/events/0/event_available_ts")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/recent_7d/events/0/cvd_norm_diff_fut")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/recent_7d/events/0/cvd_norm_diff_spot")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/recent_7d/events/0/price_norm_diff")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/recent_7d/events/0/sig_test_mode")
            .is_none());
        assert_eq!(
            value
                .pointer("/indicators/divergence/payload/candidates")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(5)
        );
        assert!(value
            .pointer("/indicators/divergence/payload/signal")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/signals")
            .is_none());
        assert!(value
            .pointer("/indicators/ema_trend_regime/payload/trend_regime")
            .is_none());
        assert!(value
            .pointer("/indicators/ema_trend_regime/payload/trend_regime_by_tf")
            .is_none());
        assert!(value
            .pointer("/indicators/ema_trend_regime/payload/output_sampling/15m/trend_regime")
            .is_none());
        assert!(value
            .pointer("/indicators/vpin/payload/toxicity_state")
            .is_none());
        assert!(value
            .pointer("/indicators/high_volume_pulse/payload/by_z_window/1h/is_volume_spike_z2")
            .is_none());
        assert!(value
            .pointer("/indicators/high_volume_pulse/payload/by_z_window/1h/is_volume_spike_z3")
            .is_none());
        assert!(value
            .pointer("/indicators/high_volume_pulse/payload/pulse")
            .is_none());
        assert!(value
            .pointer("/indicators/events_summary/payload/most_recent_absorption/minutes_ago")
            .is_some());
        assert!(value
            .pointer("/indicators/events_summary/payload/most_recent_absorption/price")
            .is_some());
        assert!(value
            .pointer("/indicators/events_summary/payload/most_recent_divergence")
            .is_some());
        assert!(
            value
                .pointer("/indicators/tpo_market_profile/payload/by_session/1d/dev_series/15m")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or_default()
                <= 8
        );
        assert!(
            value
                .pointer("/indicators/tpo_market_profile/payload/by_session/1d/dev_series/1h")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or_default()
                <= 5
        );
        assert!(value
            .pointer("/indicators/footprint/payload/by_window/4h/buy_stacks")
            .and_then(Value::as_array)
            .map(|items| items.iter().all(|item| {
                item.get("length")
                    .and_then(Value::as_u64)
                    .map(|length| length >= 4)
                    .unwrap_or(false)
            }))
            .unwrap_or(true));
        assert!(value
            .pointer("/indicators/footprint/payload/by_window/1d/buy_stacks")
            .and_then(Value::as_array)
            .map(|items| items.iter().all(|item| {
                item.get("length")
                    .and_then(Value::as_u64)
                    .map(|length| length >= 7)
                    .unwrap_or(false)
            }))
            .unwrap_or(true));
        assert_newest_first(
            &value,
            "/indicators/avwap/payload/series_by_window/15m",
            "ts",
        );
        assert_newest_first(
            &value,
            "/indicators/funding_rate/payload/by_window/15m/changes",
            "change_ts",
        );
        assert_newest_first(
            &value,
            "/indicators/divergence/payload/recent_7d/events",
            "event_end_ts",
        );
        let va_top_levels = value
            .pointer("/indicators/price_volume_structure/payload/by_window/15m/va_top_levels")
            .and_then(Value::as_array)
            .expect("15m va_top_levels");
        let first_price = va_top_levels
            .first()
            .and_then(|entry| entry.get("price"))
            .and_then(Value::as_f64)
            .expect("first va price");
        let last_price = va_top_levels
            .last()
            .and_then(|entry| entry.get("price"))
            .and_then(Value::as_f64)
            .expect("last va price");
        assert!(first_price >= last_price);
        assert_no_nulls(&value);
    }

    #[test]
    fn entry_core_orderbook_top_levels_expand_to_three_pct_when_two_pct_too_sparse() {
        let mut indicators = build_sample_indicators();
        let levels = vec![
            json!({"price_level": 2060.0, "bid_liquidity": 900.0, "ask_liquidity": 0.0, "total_liquidity": 900.0, "net_liquidity": 900.0, "level_imbalance": 1.0}),
            json!({"price_level": 2065.0, "bid_liquidity": 850.0, "ask_liquidity": 0.0, "total_liquidity": 850.0, "net_liquidity": 850.0, "level_imbalance": 1.0}),
            json!({"price_level": 2070.0, "bid_liquidity": 800.0, "ask_liquidity": 0.0, "total_liquidity": 800.0, "net_liquidity": 800.0, "level_imbalance": 1.0}),
            json!({"price_level": 2075.0, "bid_liquidity": 780.0, "ask_liquidity": 0.0, "total_liquidity": 780.0, "net_liquidity": 780.0, "level_imbalance": 1.0}),
            json!({"price_level": 2080.0, "bid_liquidity": 760.0, "ask_liquidity": 0.0, "total_liquidity": 760.0, "net_liquidity": 760.0, "level_imbalance": 1.0}),
            json!({"price_level": 2085.0, "bid_liquidity": 740.0, "ask_liquidity": 0.0, "total_liquidity": 740.0, "net_liquidity": 740.0, "level_imbalance": 1.0}),
            json!({"price_level": 2090.0, "bid_liquidity": 720.0, "ask_liquidity": 0.0, "total_liquidity": 720.0, "net_liquidity": 720.0, "level_imbalance": 1.0}),
            json!({"price_level": 2095.0, "bid_liquidity": 700.0, "ask_liquidity": 0.0, "total_liquidity": 700.0, "net_liquidity": 700.0, "level_imbalance": 1.0}),
            json!({"price_level": 2105.0, "bid_liquidity": 0.0, "ask_liquidity": 690.0, "total_liquidity": 690.0, "net_liquidity": -690.0, "level_imbalance": -1.0}),
            json!({"price_level": 2110.0, "bid_liquidity": 0.0, "ask_liquidity": 680.0, "total_liquidity": 680.0, "net_liquidity": -680.0, "level_imbalance": -1.0}),
            json!({"price_level": 2115.0, "bid_liquidity": 0.0, "ask_liquidity": 670.0, "total_liquidity": 670.0, "net_liquidity": -670.0, "level_imbalance": -1.0}),
            json!({"price_level": 2120.0, "bid_liquidity": 0.0, "ask_liquidity": 660.0, "total_liquidity": 660.0, "net_liquidity": -660.0, "level_imbalance": -1.0}),
        ];
        indicators
            .get_mut("orderbook_depth")
            .and_then(Value::as_object_mut)
            .and_then(|indicator| indicator.get_mut("payload"))
            .and_then(Value::as_object_mut)
            .expect("orderbook payload")
            .insert("levels".to_string(), Value::Array(levels));
        indicators
            .get_mut("avwap")
            .and_then(Value::as_object_mut)
            .and_then(|indicator| indicator.get_mut("payload"))
            .and_then(Value::as_object_mut)
            .expect("avwap payload")
            .insert("fut_mark_price".to_string(), Value::from(2100.0));
        indicators
            .get_mut("funding_rate")
            .and_then(Value::as_object_mut)
            .and_then(|indicator| indicator.get_mut("payload"))
            .and_then(Value::as_object_mut)
            .expect("funding payload")
            .insert("mark_price_last".to_string(), Value::from(2100.0));

        let input = sample_input(indicators, false, false);
        let value = CoreFilter::build_value(&input).expect("build entry core value");

        let top_levels = value
            .pointer("/indicators/orderbook_depth/payload/top_liquidity_levels")
            .and_then(Value::as_array)
            .expect("entry top levels");
        assert_eq!(top_levels.len(), 12);
        assert!(top_levels.iter().all(|entry| {
            entry
                .get("price_level")
                .and_then(Value::as_f64)
                .map(|price| (2037.0..=2163.0).contains(&price))
                .unwrap_or(false)
        }));
        assert!(top_levels
            .iter()
            .any(|entry| entry.get("price_level").and_then(Value::as_f64) == Some(2060.0)));
        assert!(top_levels
            .iter()
            .any(|entry| entry.get("price_level").and_then(Value::as_f64) == Some(2120.0)));
    }

    #[test]
    fn entry_core_events_summary_uses_latest_event_timing() {
        let mut indicators = build_sample_indicators();
        indicators
            .get_mut("absorption")
            .and_then(Value::as_object_mut)
            .and_then(|indicator| indicator.get_mut("payload"))
            .and_then(Value::as_object_mut)
            .and_then(|payload| payload.get_mut("recent_7d"))
            .and_then(Value::as_object_mut)
            .expect("absorption recent_7d")
            .insert(
                "events".to_string(),
                json!([
                    {
                        "confirm_ts": "2026-03-16T12:06:00Z",
                        "event_end_ts": "2026-03-16T12:05:00Z",
                        "direction": -1,
                        "pivot_price": 2293.0,
                        "score": 0.64,
                        "type": "bearish_absorption"
                    },
                    {
                        "confirm_ts": "2026-03-16T11:20:00Z",
                        "event_end_ts": "2026-03-16T11:19:00Z",
                        "direction": 1,
                        "pivot_price": 2261.0,
                        "score": 0.7,
                        "type": "bullish_absorption"
                    }
                ]),
            );
        indicators
            .get_mut("buying_exhaustion")
            .and_then(Value::as_object_mut)
            .and_then(|indicator| indicator.get_mut("payload"))
            .and_then(Value::as_object_mut)
            .and_then(|payload| payload.get_mut("recent_7d"))
            .and_then(Value::as_object_mut)
            .expect("buying_exhaustion recent_7d")
            .insert(
                "events".to_string(),
                json!([
                    {
                        "confirm_ts": "2026-03-16T11:04:00Z",
                        "event_end_ts": "2026-03-16T11:04:00Z",
                        "direction": -1,
                        "pivot_price": 2279.13,
                        "score": 0.77,
                        "type": "buying_exhaustion"
                    }
                ]),
            );
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-16T12:21:00Z")
            .expect("parse ts bucket")
            .with_timezone(&Utc);
        let input = sample_input_at(indicators, false, false, ts_bucket);

        let value = CoreFilter::build_value(&input).expect("build entry core value");

        assert_eq!(
            value
                .pointer("/indicators/events_summary/payload/most_recent_absorption/direction")
                .and_then(Value::as_str),
            Some("bearish")
        );
        assert_eq!(
            value
                .pointer("/indicators/events_summary/payload/most_recent_absorption/price")
                .and_then(Value::as_f64),
            Some(2293.0)
        );
        assert_eq!(
            value
                .pointer("/indicators/events_summary/payload/most_recent_absorption/minutes_ago")
                .and_then(Value::as_i64),
            Some(15)
        );
        assert!(value
            .pointer("/indicators/events_summary/payload/most_recent_absorption/price_distance_atr")
            .is_some());
        assert_eq!(
            value
                .pointer(
                    "/indicators/events_summary/payload/most_recent_buying_exhaustion/minutes_ago"
                )
                .and_then(Value::as_i64),
            Some(77)
        );
    }

    #[test]
    fn build_management_core_value_applies_v1_rules() {
        let input = sample_input(build_sample_indicators(), true, false);

        let value = CoreFilter::build_value(&input).expect("build management core value");

        assert_eq!(value.as_object().map(|obj| obj.len()), Some(5));
        assert!(value.pointer("/management_snapshot").is_some());
        assert!(value.pointer("/trading_state").is_some());
        assert!(value
            .pointer("/management_snapshot/position_context/entry_context")
            .is_some());
        assert!(value
            .pointer("/management_snapshot/position_context/entry_context/entry_reason")
            .is_none());
        assert!(value.pointer("/indicators/footprint/window_code").is_none());
        assert!(value
            .pointer("/indicators/price_volume_structure/payload/value_area_levels")
            .is_none());
        assert!(value
            .pointer("/indicators/price_volume_structure/payload/by_window/15m/value_area_levels")
            .is_none());
        assert_eq!(
            value
                .pointer("/indicators/price_volume_structure/payload/by_window/15m/va_top_levels")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(15)
        );
        assert_eq!(
            value
                .pointer("/indicators/price_volume_structure/payload/by_window/4h/va_top_levels")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(10)
        );
        assert!(value
            .pointer("/management_snapshot/positions/0/pnl_by_latest_price")
            .is_some());
        assert!(value
            .pointer("/management_snapshot/positions/0/current_tp_price")
            .is_some());
        assert!(value
            .pointer("/management_snapshot/positions/0/current_sl_price")
            .is_some());
        assert!(value
            .pointer("/indicators/fvg/payload/by_window/1d")
            .is_some());
        assert!(value
            .pointer("/indicators/fvg/payload/by_window/1d/fvgs")
            .is_none());
        assert!(value
            .pointer("/indicators/fvg/payload/by_window/1d/active_bull_fvgs")
            .is_none());
        assert!(value
            .pointer("/indicators/fvg/payload/by_window/1d/active_bear_fvgs")
            .is_none());
        assert!(value
            .pointer("/indicators/fvg/payload/by_window/1d/nearest_bull_fvg")
            .is_some());
        assert!(value
            .pointer("/indicators/fvg/payload/by_window/1d/nearest_bear_fvg")
            .is_some());
        assert_eq!(
            value
                .pointer("/indicators/fvg/payload/by_window/15m/fvgs")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(4)
        );
        assert!(
            value
                .pointer("/indicators/footprint/payload/by_window/15m/levels")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or_default()
                <= 20
        );
        assert_eq!(
            value
                .pointer("/indicators/orderbook_depth/payload/top_liquidity_levels")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(60)
        );
        assert_eq!(
            value
                .pointer("/indicators/avwap/payload/series_by_window/15m")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(3)
        );
        assert_eq!(
            value
                .pointer("/indicators/kline_history/payload/intervals/15m/markets/futures")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(16)
        );
        assert_eq!(
            value
                .pointer("/indicators/cvd_pack/payload/by_window/15m/series")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(12)
        );
        assert!(value.pointer("/indicators/bullish_absorption").is_none());
        assert!(value.pointer("/indicators/bearish_absorption").is_none());
        assert!(value.pointer("/indicators/bullish_initiation").is_none());
        assert!(value.pointer("/indicators/bearish_initiation").is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/signal")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/signals")
            .is_none());
        assert!(value
            .pointer("/indicators/ema_trend_regime/payload/trend_regime")
            .is_none());
        assert!(value
            .pointer("/indicators/ema_trend_regime/payload/trend_regime_by_tf")
            .is_none());
        assert!(value
            .pointer("/indicators/vpin/payload/toxicity_state")
            .is_none());
        assert!(value
            .pointer("/indicators/high_volume_pulse/payload/by_z_window/1h/is_volume_spike_z2")
            .is_none());
        assert!(value
            .pointer("/indicators/high_volume_pulse/payload/by_z_window/1h/is_volume_spike_z3")
            .is_none());
        assert!(value
            .pointer("/indicators/events_summary/payload/most_recent_absorption/minutes_ago")
            .is_some());
        assert!(value
            .pointer("/indicators/position_evidence/payload/price_to_entry_atr")
            .is_some());
        assert!(value
            .pointer("/indicators/position_evidence/payload/price_to_current_sl_atr")
            .is_some());
        assert!(value
            .pointer("/indicators/position_evidence/payload/price_to_current_tp_atr")
            .is_some());
        assert!(value
            .pointer("/indicators/position_evidence/payload/current_sl_beyond_nearest_structure")
            .is_some());
        assert!(value
            .pointer(
                "/indicators/position_evidence/payload/current_tp_before_next_major_resistance"
            )
            .is_some());
        assert!(value
            .pointer("/indicators/position_evidence/payload/position_in_4h_tpo_pct")
            .is_some());
        assert!(value
            .pointer("/indicators/position_evidence/payload/position_in_1d_range_pct")
            .is_some());
        assert_no_nulls(&value);
    }

    #[test]
    fn build_pending_core_value_applies_v1_rules() {
        let input = sample_input(build_sample_indicators(), false, true);

        let value = CoreFilter::build_value(&input).expect("build pending core value");

        assert_eq!(value.as_object().map(|obj| obj.len()), Some(5));
        assert!(value
            .pointer("/management_snapshot/pending_order")
            .is_some());
        assert!(value
            .pointer("/indicators/divergence/window_code")
            .is_none());
        assert!(value
            .pointer("/indicators/price_volume_structure/payload/value_area_levels")
            .is_none());
        assert!(value
            .pointer("/indicators/price_volume_structure/payload/by_window/15m/value_area_levels")
            .is_none());
        assert_eq!(
            value
                .pointer("/indicators/price_volume_structure/payload/by_window/15m/va_top_levels")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(15)
        );
        assert_eq!(
            value
                .pointer("/indicators/price_volume_structure/payload/by_window/4h/va_top_levels")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(10)
        );
        assert!(value.pointer("/management_snapshot/positions/0").is_some());
        assert!(value
            .pointer("/management_snapshot/position_context/entry_context")
            .is_some());
        assert!(value
            .pointer("/indicators/fvg/payload/by_window/3d")
            .is_none());
        assert!(
            value
                .pointer("/indicators/orderbook_depth/payload/top_liquidity_levels")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or_default()
                <= 100
        );
        assert_eq!(
            value
                .pointer("/indicators/avwap/payload/series_by_window/15m")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(8)
        );
        assert_eq!(
            value
                .pointer("/indicators/kline_history/payload/intervals/15m/markets/futures")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(24)
        );
        assert_eq!(
            value
                .pointer("/indicators/cvd_pack/payload/by_window/15m/series")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(18)
        );
        assert!(
            value
                .pointer("/indicators/footprint/payload/by_window/15m/levels")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or_default()
                <= 12
        );
        assert!(value.pointer("/indicators/bullish_absorption").is_none());
        assert!(value.pointer("/indicators/bearish_absorption").is_none());
        assert!(value.pointer("/indicators/bullish_initiation").is_none());
        assert!(value.pointer("/indicators/bearish_initiation").is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/signal")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/signals")
            .is_none());
        assert!(value
            .pointer("/indicators/ema_trend_regime/payload/trend_regime")
            .is_none());
        assert!(value
            .pointer("/indicators/vpin/payload/toxicity_state")
            .is_none());
        assert!(value
            .pointer("/indicators/high_volume_pulse/payload/by_z_window/1h/is_volume_spike_z2")
            .is_none());
        assert!(value
            .pointer("/indicators/high_volume_pulse/payload/by_z_window/1h/is_volume_spike_z3")
            .is_none());
        assert!(value
            .pointer("/indicators/events_summary/payload/most_recent_absorption/minutes_ago")
            .is_some());
        assert!(value
            .pointer("/indicators/pending_entry_evidence/payload/entry_to_current_price_atr")
            .is_some());
        assert!(value
            .pointer("/indicators/pending_entry_evidence/payload/spent_move_pct_of_atr")
            .is_some());
        assert!(value
            .pointer("/indicators/pending_entry_evidence/payload/rvwap_15m_z_current")
            .is_some());
        assert!(value
            .pointer("/indicators/pending_entry_evidence/payload/entry_anchor_wall_still_present")
            .is_some());
        assert!(value
            .pointer("/indicators/footprint/payload/by_window/4h/buy_stacks")
            .and_then(Value::as_array)
            .map(|items| items.iter().all(|item| {
                item.get("length")
                    .and_then(Value::as_u64)
                    .map(|length| length >= 4)
                    .unwrap_or(false)
            }))
            .unwrap_or(true));
        assert!(value
            .pointer("/indicators/footprint/payload/by_window/1d/buy_stacks")
            .and_then(Value::as_array)
            .map(|items| items.iter().all(|item| {
                item.get("length")
                    .and_then(Value::as_u64)
                    .map(|length| length >= 7)
                    .unwrap_or(false)
            }))
            .unwrap_or(true));
        assert_no_nulls(&value);
    }

    #[test]
    fn entry_core_real_snapshot_stays_under_soft_budget() {
        let Some((path, input)) = load_real_snapshot_input(false, false) else {
            return;
        };

        let value = CoreFilter::build_value(&input).expect("build entry core");
        assert!(value
            .pointer("/indicators/price_volume_structure/payload/value_area_levels")
            .is_none());
        assert!(value
            .pointer("/indicators/price_volume_structure/payload/by_window/15m/value_area_levels")
            .is_none());
        assert!(
            value
                .pointer("/indicators/price_volume_structure/payload/by_window/15m/va_top_levels")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or_default()
                <= 15,
            "entry_core va_top_levels exceeded 15 for {}",
            path.display()
        );
        assert!(value
            .pointer("/indicators/footprint/payload/by_window/15m/buy_imbalance_prices")
            .is_none());
        assert!(
            value
                .pointer("/indicators/footprint/payload/by_window/15m/levels")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or_default()
                <= 200,
            "entry_core footprint representative levels exceeded 200 for {}",
            path.display()
        );
        assert!(value
            .pointer("/indicators/orderbook_depth/payload/obi")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/latest_7d/event_available_ts")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/latest_7d/cvd_norm_diff_fut")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/latest_7d/cvd_norm_diff_spot")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/latest_7d/price_norm_diff")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/latest_7d/sig_test_mode")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/recent_7d/events/0/event_available_ts")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/recent_7d/events/0/cvd_norm_diff_fut")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/recent_7d/events/0/cvd_norm_diff_spot")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/recent_7d/events/0/price_norm_diff")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/recent_7d/events/0/sig_test_mode")
            .is_none());
        assert!(value.pointer("/indicators/bullish_absorption").is_none());
        assert!(value.pointer("/indicators/bearish_absorption").is_none());
        assert!(value.pointer("/indicators/bullish_initiation").is_none());
        assert!(value.pointer("/indicators/bearish_initiation").is_none());
        assert!(value
            .pointer("/indicators/ema_trend_regime/payload/trend_regime")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/signal")
            .is_none());
        assert!(value
            .pointer("/indicators/vpin/payload/toxicity_state")
            .is_none());
        assert!(value
            .pointer("/indicators/high_volume_pulse/payload/by_z_window/1h/is_volume_spike_z2")
            .is_none());
        assert!(value
            .pointer("/indicators/events_summary/payload/most_recent_absorption")
            .is_some());
        assert!(value
            .pointer("/indicators/atr_context/payload/latest_15m_detail/summary/atr14")
            .is_some());
        assert!(value
            .pointer("/indicators/atr_context/payload/latest_4h_context/summary/atr14")
            .is_some());
        assert!(value
            .pointer("/indicators/atr_context/payload/latest_1d_background/summary/atr14")
            .is_some());
        assert!(
            value
                .pointer("/indicators/tpo_market_profile/payload/by_session/1d/dev_series/15m")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or_default()
                <= 8,
            "entry_core tpo dev_series 15m exceeded 8 for {}",
            path.display()
        );
        assert!(
            value
                .pointer("/indicators/footprint/payload/by_window/1d/buy_stacks")
                .and_then(Value::as_array)
                .map(|items| items.iter().all(|item| {
                    item.get("length")
                        .and_then(Value::as_u64)
                        .map(|length| length >= 7)
                        .unwrap_or(false)
                }))
                .unwrap_or(true),
            "entry_core 1d buy_stacks still contains short stacks for {}",
            path.display()
        );
        assert_no_nulls(&value);
        let size = serde_json::to_string(&value)
            .expect("serialize entry core")
            .len();
        assert_snapshot_budget("entry_core", size, 120_000, 140_000, &path);
    }

    #[test]
    fn management_core_real_snapshot_stays_under_soft_budget() {
        let Some((path, input)) = load_real_snapshot_input(true, false) else {
            return;
        };

        let value = CoreFilter::build_value(&input).expect("build management core");
        assert!(value.pointer("/indicators/bullish_absorption").is_none());
        assert!(value.pointer("/indicators/bearish_absorption").is_none());
        assert!(value.pointer("/indicators/bullish_initiation").is_none());
        assert!(value.pointer("/indicators/bearish_initiation").is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/signal")
            .is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/signals")
            .is_none());
        assert!(value
            .pointer("/indicators/ema_trend_regime/payload/trend_regime")
            .is_none());
        assert!(value
            .pointer("/indicators/vpin/payload/toxicity_state")
            .is_none());
        assert!(value
            .pointer("/indicators/high_volume_pulse/payload/by_z_window/1h/is_volume_spike_z2")
            .is_none());
        assert!(value
            .pointer("/indicators/price_volume_structure/payload/value_area_levels")
            .is_none());
        assert!(value
            .pointer("/indicators/price_volume_structure/payload/by_window/15m/value_area_levels")
            .is_none());
        assert!(
            value
                .pointer("/indicators/price_volume_structure/payload/by_window/15m/va_top_levels")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or_default()
                <= 15,
            "management_core va_top_levels exceeded 15 for {}",
            path.display()
        );
        assert!(value
            .pointer("/indicators/fvg/payload/by_window/1d")
            .is_some());
        assert!(value
            .pointer("/indicators/fvg/payload/by_window/1d/fvgs")
            .is_none());
        assert!(value
            .pointer("/indicators/fvg/payload/by_window/1d/active_bull_fvgs")
            .is_none());
        assert!(value
            .pointer("/indicators/fvg/payload/by_window/1d/active_bear_fvgs")
            .is_none());
        assert!(value
            .pointer("/indicators/events_summary/payload/most_recent_absorption")
            .is_some());
        assert!(value
            .pointer("/indicators/position_evidence/payload/price_to_entry_atr")
            .is_some());
        assert!(value
            .pointer("/indicators/position_evidence/payload/price_to_current_sl_atr")
            .is_some());
        assert!(value
            .pointer("/indicators/position_evidence/payload/price_to_current_tp_atr")
            .is_some());
        assert!(value
            .pointer("/indicators/position_evidence/payload/current_sl_beyond_nearest_structure")
            .is_some());
        assert!(value
            .pointer(
                "/indicators/position_evidence/payload/current_tp_before_next_major_resistance"
            )
            .is_some());
        assert!(value
            .pointer("/indicators/position_evidence/payload/position_in_4h_tpo_pct")
            .is_some());
        assert!(value
            .pointer("/indicators/position_evidence/payload/position_in_1d_range_pct")
            .is_some());
        assert!(
            value
                .pointer("/indicators/footprint/payload/by_window/15m/levels")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or_default()
                <= 200,
            "management_core footprint representative levels exceeded 200 for {}",
            path.display()
        );
        assert_no_nulls(&value);
        let size = serde_json::to_string(&value)
            .expect("serialize management core")
            .len();
        assert_snapshot_budget("management_core", size, 180_000, 220_000, &path);
    }

    #[test]
    fn pending_core_real_snapshot_stays_under_soft_budget() {
        let Some((path, input)) = load_real_snapshot_input(false, true) else {
            return;
        };

        let value = CoreFilter::build_value(&input).expect("build pending core");
        assert!(value.pointer("/indicators/bullish_absorption").is_none());
        assert!(value.pointer("/indicators/bearish_absorption").is_none());
        assert!(value.pointer("/indicators/bullish_initiation").is_none());
        assert!(value.pointer("/indicators/bearish_initiation").is_none());
        assert!(value
            .pointer("/indicators/divergence/payload/signal")
            .is_none());
        assert!(value
            .pointer("/indicators/ema_trend_regime/payload/trend_regime")
            .is_none());
        assert!(value
            .pointer("/indicators/vpin/payload/toxicity_state")
            .is_none());
        assert!(value
            .pointer("/indicators/high_volume_pulse/payload/by_z_window/1h/is_volume_spike_z2")
            .is_none());
        assert!(value
            .pointer("/indicators/events_summary/payload/most_recent_absorption")
            .is_some());
        assert!(value
            .pointer("/indicators/pending_entry_evidence/payload/spent_move_pct_of_atr")
            .is_some());
        assert!(value
            .pointer("/indicators/pending_entry_evidence/payload/rvwap_15m_z_current")
            .is_some());
        assert!(value
            .pointer("/indicators/pending_entry_evidence/payload/entry_anchor_wall_still_present")
            .is_some());
        assert!(value
            .pointer("/indicators/price_volume_structure/payload/value_area_levels")
            .is_none());
        assert!(value
            .pointer("/indicators/price_volume_structure/payload/by_window/15m/value_area_levels")
            .is_none());
        assert!(
            value
                .pointer("/indicators/price_volume_structure/payload/by_window/15m/va_top_levels")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or_default()
                <= 15,
            "pending_core va_top_levels exceeded 15 for {}",
            path.display()
        );
        assert!(
            value
                .pointer("/indicators/footprint/payload/by_window/15m/levels")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or_default()
                <= 200,
            "pending_core footprint representative levels exceeded 200 for {}",
            path.display()
        );
        assert_no_nulls(&value);
        let size = serde_json::to_string(&value)
            .expect("serialize pending core")
            .len();
        assert_snapshot_budget("pending_core", size, 140_000, 190_000, &path);
    }
}
