use crate::llm::prompt;
use anyhow::{anyhow, Result};
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeDecision {
    Long,
    Short,
    NoTrade,
}

impl TradeDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Long => "LONG",
            Self::Short => "SHORT",
            Self::NoTrade => "NO_TRADE",
        }
    }
}

#[derive(Debug, Clone)]
pub struct TradeIntent {
    pub decision: TradeDecision,
    pub entry_price: Option<f64>,
    pub take_profit: Option<f64>,
    pub stop_loss: Option<f64>,
    pub leverage: Option<f64>,
    pub risk_reward_ratio: Option<f64>,
    pub horizon: Option<String>,
    pub swing_logic: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum PositionManagementDecision {
    Close,
    Add,
    Reduce,
    Hold,
    ModifyTpSl,
}

impl PositionManagementDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Close => "CLOSE",
            Self::Add => "ADD",
            Self::Reduce => "REDUCE",
            Self::Hold => "HOLD",
            Self::ModifyTpSl => "MODIFY_TPSL",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum PendingOrderManagementDecision {
    Hold,
    Close,
    ModifyMaker,
}

impl PendingOrderManagementDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hold => "HOLD",
            Self::Close => "CLOSE",
            Self::ModifyMaker => "MODIFY_MAKER",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PositionManagementIntent {
    pub decision: PositionManagementDecision,
    pub qty: Option<f64>,
    pub qty_ratio: Option<f64>,
    pub is_full_exit: Option<bool>,
    pub new_tp: Option<f64>,
    pub new_sl: Option<f64>,
    pub close_price: Option<f64>,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PendingOrderManagementIntent {
    pub decision: PendingOrderManagementDecision,
    pub new_entry: Option<f64>,
    pub new_tp: Option<f64>,
    pub new_sl: Option<f64>,
    pub new_leverage: Option<f64>,
    pub reason: String,
}

/// Current state of the live pending order, used to determine HOLD vs MODIFY_MAKER.
#[derive(Debug, Clone, Default)]
pub struct PendingOrderContext {
    pub has_open_orders: bool,
    pub current_entry: Option<f64>,
    pub current_tp: Option<f64>,
    pub current_sl: Option<f64>,
    pub current_leverage: Option<f64>,
}

pub fn validate_model_output(
    value: &Value,
    management_mode: bool,
    pending_order_mode: bool,
) -> Option<String> {
    if pending_order_mode {
        pending_order_management_intent_from_value(value)
            .err()
            .map(|err| err.to_string())
    } else if management_mode {
        position_management_intent_from_value(value)
            .err()
            .map(|err| err.to_string())
    } else {
        trade_intent_from_value(value)
            .err()
            .map(|err| err.to_string())
    }
}

pub fn trade_intent_from_value(value: &Value) -> Result<TradeIntent> {
    let decision_raw = value
        .get("decision")
        .and_then(Value::as_str)
        .map(str::trim)
        .ok_or_else(|| {
            anyhow!(
                "decision must be {}/{}/{}",
                prompt::DECISION_LONG,
                prompt::DECISION_SHORT,
                prompt::DECISION_NO_TRADE
            )
        })?;
    let decision = parse_trade_decision(decision_raw)?;
    let reason = value
        .get("reason")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .ok_or_else(|| anyhow!("reason must be non-empty"))?
        .to_string();

    let entry_price = find_f64(value, &["params.entry", "entry_price"]);
    let take_profit = find_f64(value, &["params.tp", "tp"]);
    let stop_loss = find_f64(value, &["params.sl", "sl"]);
    let leverage = find_f64(value, &["params.leverage", "leverage"]);
    let model_risk_reward_ratio = find_f64(value, &["params.rr", "risk_reward_ratio"]);
    let horizon = find_str(value, &["params.horizon", "holding_period"]).map(str::to_string);
    let swing_logic = find_str(value, &["params.swing_logic"]).map(str::to_string);

    match decision {
        TradeDecision::NoTrade => Ok(TradeIntent {
            decision,
            entry_price: None,
            take_profit: None,
            stop_loss: None,
            leverage: None,
            risk_reward_ratio: None,
            horizon: None,
            swing_logic,
            reason,
        }),
        TradeDecision::Long | TradeDecision::Short => {
            let entry_price = entry_price.ok_or_else(|| anyhow!("entry is missing"))?;
            let take_profit = take_profit.ok_or_else(|| anyhow!("tp is missing"))?;
            let stop_loss = stop_loss.ok_or_else(|| anyhow!("sl is missing"))?;
            let horizon = horizon
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| anyhow!("horizon is missing"))?;

            if entry_price <= 0.0 {
                return Err(anyhow!("entry must be > 0"));
            }
            if take_profit <= 0.0 {
                return Err(anyhow!("tp must be > 0"));
            }
            if stop_loss <= 0.0 {
                return Err(anyhow!("sl must be > 0"));
            }
            if leverage.is_some_and(|value| value <= 0.0) {
                return Err(anyhow!("leverage must be > 0"));
            }
            match decision {
                TradeDecision::Long => {
                    if !(take_profit > entry_price && stop_loss < entry_price) {
                        return Err(anyhow!("LONG requires tp > entry and sl < entry"));
                    }
                }
                TradeDecision::Short => {
                    if !(take_profit < entry_price && stop_loss > entry_price) {
                        return Err(anyhow!("SHORT requires tp < entry and sl > entry"));
                    }
                }
                TradeDecision::NoTrade => {}
            }
            let risk_reward_ratio = compute_rr_from_levels(entry_price, take_profit, stop_loss)
                .or_else(|| model_risk_reward_ratio.filter(|value| *value > 0.0))
                .ok_or_else(|| anyhow!("rr could not be computed from entry/tp/sl"))?;

            Ok(TradeIntent {
                decision,
                entry_price: Some(entry_price),
                take_profit: Some(take_profit),
                stop_loss: Some(stop_loss),
                leverage,
                risk_reward_ratio: Some(risk_reward_ratio),
                horizon: Some(horizon),
                swing_logic,
                reason,
            })
        }
    }
}

pub fn position_management_intent_from_value(value: &Value) -> Result<PositionManagementIntent> {
    let decision_raw = value
        .get("decision")
        .and_then(Value::as_str)
        .map(str::trim)
        .ok_or_else(|| {
            anyhow!(
                "decision must be {}/{}/{}",
                prompt::DECISION_VALID,
                prompt::DECISION_INVALID,
                prompt::DECISION_ADJUST,
            )
        })?;
    let decision = parse_management_decision(decision_raw)?;
    let reason = value
        .get("reason")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .ok_or_else(|| anyhow!("reason must be non-empty"))?
        .to_string();
    let new_tp = find_f64(value, &["params.new_tp", "new_tp", "params.tp", "tp"]);
    let new_sl = find_f64(value, &["params.new_sl", "new_sl", "params.sl", "sl"]);
    let close_price = find_f64(value, &["params.close_price", "close_price"]);
    let qty_ratio = find_f64(value, &["params.qty_ratio", "qty_ratio"]);

    // For ADJUST, refine the decision based on adjust_fields content
    let decision = if matches!(decision, PositionManagementDecision::ModifyTpSl) {
        let adjust_fields = value
            .pointer("/params/adjust_fields")
            .or_else(|| value.get("adjust_fields"))
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_ascii_lowercase)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        if adjust_fields.iter().any(|f| f == "add") {
            if qty_ratio.is_none() {
                return Err(anyhow!("ADJUST with add requires params.qty_ratio"));
            }
            PositionManagementDecision::Add
        } else if adjust_fields.iter().any(|f| f == "reduce") {
            if qty_ratio.is_none() {
                return Err(anyhow!("ADJUST with reduce requires params.qty_ratio"));
            }
            PositionManagementDecision::Reduce
        } else {
            // tp / sl adjustment
            if new_tp.is_none() && new_sl.is_none() {
                return Err(anyhow!(
                    "ADJUST requires at least one of: adjust_fields=[add|reduce] with qty_ratio, or params.new_tp/params.new_sl"
                ));
            }
            if new_tp.is_some_and(|v| v <= 0.0) {
                return Err(anyhow!("new_tp must be > 0 when provided"));
            }
            if new_sl.is_some_and(|v| v <= 0.0) {
                return Err(anyhow!("new_sl must be > 0 when provided"));
            }
            PositionManagementDecision::ModifyTpSl
        }
    } else {
        decision
    };

    Ok(PositionManagementIntent {
        decision,
        qty: None,
        qty_ratio,
        is_full_exit: None,
        new_tp,
        new_sl,
        close_price,
        reason,
    })
}

pub fn position_management_intent_from_value_with_context(
    value: &Value,
    has_active_positions: bool,
    has_open_orders: bool,
) -> Result<PositionManagementIntent> {
    let intent = position_management_intent_from_value(value)?;
    if matches!(intent.decision, PositionManagementDecision::ModifyTpSl) && !has_active_positions {
        return Err(anyhow!("ADJUST is invalid when no active positions exist"));
    }
    if !has_active_positions
        && !has_open_orders
        && matches!(intent.decision, PositionManagementDecision::Close)
    {
        return Err(anyhow!(
            "INVALID is not actionable when neither active positions nor open orders exist"
        ));
    }
    Ok(intent)
}

/// Parse the model's optimal order output (entry/tp/sl/leverage).
/// Decision is not read from the model — it is computed by comparison with the live order.
pub fn pending_order_management_intent_from_value(
    value: &Value,
) -> Result<PendingOrderManagementIntent> {
    let reason = value
        .get("reason")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .ok_or_else(|| anyhow!("reason must be non-empty"))?
        .to_string();

    let new_entry = find_f64(value, &["params.entry", "entry"]);
    let new_tp = find_f64(value, &["params.tp", "tp"]);
    let new_sl = find_f64(value, &["params.sl", "sl"]);
    let new_leverage = find_f64(value, &["params.leverage", "leverage"]);

    if new_entry.is_some_and(|v| v <= 0.0) {
        return Err(anyhow!("entry must be > 0 when provided"));
    }
    if new_tp.is_some_and(|v| v <= 0.0) {
        return Err(anyhow!("tp must be > 0 when provided"));
    }
    if new_sl.is_some_and(|v| v <= 0.0) {
        return Err(anyhow!("sl must be > 0 when provided"));
    }
    if new_leverage.is_some_and(|v| v <= 0.0) {
        return Err(anyhow!("leverage must be > 0 when provided"));
    }

    // Preliminary decision: Close if model sees no valid setup; ModifyMaker otherwise.
    // The _with_context function refines this to Hold when levels match the live order.
    let decision = if new_entry.is_none() && new_tp.is_none() && new_sl.is_none() {
        PendingOrderManagementDecision::Close
    } else {
        PendingOrderManagementDecision::ModifyMaker
    };

    Ok(PendingOrderManagementIntent {
        decision,
        new_entry,
        new_tp,
        new_sl,
        new_leverage,
        reason,
    })
}

/// Compare the model's optimal order against the live order.
/// If the model's levels match within tolerance → Hold. Otherwise → ModifyMaker (cancel + re-place).
pub fn pending_order_management_intent_from_value_with_context(
    value: &Value,
    ctx: &PendingOrderContext,
) -> Result<PendingOrderManagementIntent> {
    if !ctx.has_open_orders {
        return Err(anyhow!(
            "pending-order management is invalid when no open orders exist"
        ));
    }
    let mut intent = pending_order_management_intent_from_value(value)?;

    if matches!(intent.decision, PendingOrderManagementDecision::ModifyMaker) {
        if levels_match(intent.new_entry, ctx.current_entry)
            && levels_match(intent.new_tp, ctx.current_tp)
            && levels_match(intent.new_sl, ctx.current_sl)
            && levels_match(intent.new_leverage, ctx.current_leverage)
        {
            intent.decision = PendingOrderManagementDecision::Hold;
        }
    }

    Ok(intent)
}

/// Returns true if model level and current level are within 0.05% of each other (or both absent).
fn levels_match(model: Option<f64>, current: Option<f64>) -> bool {
    match (model, current) {
        (None, _) | (_, None) => true,
        (Some(m), Some(c)) => {
            if c == 0.0 {
                m == 0.0
            } else {
                ((m - c) / c).abs() < 0.0005
            }
        }
    }
}

fn compute_rr_from_levels(entry: f64, tp: f64, sl: f64) -> Option<f64> {
    let risk = (entry - sl).abs();
    let reward = (tp - entry).abs();
    if risk <= f64::EPSILON || reward <= f64::EPSILON {
        None
    } else {
        Some(reward / risk)
    }
}

pub fn normalize_decision(raw: &str) -> String {
    match raw.trim() {
        s if s.eq_ignore_ascii_case("LONG") => "LONG".to_string(),
        s if s.eq_ignore_ascii_case("SHORT") => "SHORT".to_string(),
        s if s.eq_ignore_ascii_case("NO_TRADE") => "NO_TRADE".to_string(),
        s if s == "鍋氬" => "LONG".to_string(),
        s if s == "鍋氱┖" => "SHORT".to_string(),
        s if s == "涓嶅仛" => "NO_TRADE".to_string(),
        s if s == "\u{4E0D}\u{4EA4}\u{6613}" => "NO_TRADE".to_string(),
        other => other.to_ascii_uppercase(),
    }
}

fn parse_trade_decision(raw: &str) -> Result<TradeDecision> {
    match normalize_decision(raw).as_str() {
        "LONG" => Ok(TradeDecision::Long),
        "SHORT" => Ok(TradeDecision::Short),
        "NO_TRADE" => Ok(TradeDecision::NoTrade),
        _ => Err(anyhow!(
            "decision must be {}/{}/{}",
            prompt::DECISION_LONG,
            prompt::DECISION_SHORT,
            prompt::DECISION_NO_TRADE
        )),
    }
}

fn parse_management_decision(raw: &str) -> Result<PositionManagementDecision> {
    match raw.trim().to_ascii_uppercase().as_str() {
        // New review-style decisions from model
        "VALID" => Ok(PositionManagementDecision::Hold),
        "INVALID" => Ok(PositionManagementDecision::Close),
        "ADJUST" => Ok(PositionManagementDecision::ModifyTpSl),
        _ => Err(anyhow!(
            "decision must be {}/{}/{}",
            prompt::DECISION_VALID,
            prompt::DECISION_INVALID,
            prompt::DECISION_ADJUST,
        )),
    }
}

fn find_f64<'a>(value: &'a Value, paths: &[&str]) -> Option<f64> {
    paths.iter().find_map(|path| {
        let v = get_path(value, path)?;
        if let Some(n) = v.as_f64() {
            return Some(n);
        }
        v.as_str().and_then(|raw| raw.trim().parse::<f64>().ok())
    })
}

fn find_str<'a>(value: &'a Value, paths: &[&str]) -> Option<&'a str> {
    paths
        .iter()
        .find_map(|path| get_path(value, path).and_then(Value::as_str).map(str::trim))
}

fn find_bool<'a>(value: &'a Value, paths: &[&str]) -> Option<bool> {
    paths.iter().find_map(|path| {
        let v = get_path(value, path)?;
        if let Some(b) = v.as_bool() {
            return Some(b);
        }
        v.as_str()
            .and_then(|raw| match raw.trim().to_ascii_lowercase().as_str() {
                "true" => Some(true),
                "false" => Some(false),
                _ => None,
            })
    })
}

fn get_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = value;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use super::{
        pending_order_management_intent_from_value,
        pending_order_management_intent_from_value_with_context, trade_intent_from_value,
        PendingOrderContext, PendingOrderManagementDecision, TradeDecision,
    };
    use serde_json::json;

    #[test]
    fn pending_modify_maker_parses_entry_tp_sl() {
        let value = json!({
            "reason": "better structural zone available",
            "params": {
                "entry": 1965.5,
                "tp": 1974.0,
                "sl": 1961.2,
                "leverage": 3.0
            }
        });

        let intent = pending_order_management_intent_from_value(&value).expect("pending parses");
        assert_eq!(intent.decision, PendingOrderManagementDecision::ModifyMaker);
        assert_eq!(intent.new_entry, Some(1965.5));
        assert_eq!(intent.new_tp, Some(1974.0));
        assert_eq!(intent.new_sl, Some(1961.2));
        assert_eq!(intent.new_leverage, Some(3.0));
    }

    #[test]
    fn pending_all_null_produces_close() {
        let value = json!({
            "reason": "no valid setup",
            "params": {
                "entry": null,
                "tp": null,
                "sl": null,
                "leverage": null
            }
        });

        let intent =
            pending_order_management_intent_from_value(&value).expect("pending null parses");
        assert_eq!(intent.decision, PendingOrderManagementDecision::Close);
        assert_eq!(intent.new_entry, None);
    }

    #[test]
    fn trade_intent_accepts_range_horizon_text() {
        let value = json!({
            "decision": "LONG",
            "reason": "range horizon text should not block execution",
            "params": {
                "entry": 2018.33,
                "tp": 2054.96,
                "sl": 2015.12,
                "leverage": 2,
                "rr": 11.41,
                "horizon": "2-5d"
            }
        });

        let intent = trade_intent_from_value(&value).expect("trade intent parses");
        assert_eq!(intent.decision, TradeDecision::Long);
        assert_eq!(intent.horizon.as_deref(), Some("2-5d"));
    }

    #[test]
    fn trade_intent_accepts_free_form_horizon_text() {
        let value = json!({
            "decision": "LONG",
            "reason": "free-form horizon text should be preserved",
            "params": {
                "entry": 2018.33,
                "tp": 2054.96,
                "sl": 2015.12,
                "leverage": 2,
                "rr": 11.41,
                "horizon": "next week"
            }
        });

        let intent = trade_intent_from_value(&value).expect("free-form horizon should parse");
        assert_eq!(intent.horizon.as_deref(), Some("next week"));
    }

    #[test]
    fn trade_intent_derives_rr_when_model_omits_it() {
        let value = json!({
            "decision": "LONG",
            "reason": "schema-compliant entry without rr should still parse",
            "params": {
                "entry": 2000.0,
                "tp": 2040.0,
                "sl": 1980.0,
                "leverage": 3,
                "horizon": "4h"
            }
        });

        let intent = trade_intent_from_value(&value).expect("trade intent should derive rr");
        assert_eq!(intent.risk_reward_ratio, Some(2.0));
    }

    #[test]
    fn pending_leverage_change_keeps_modify_maker() {
        let value = json!({
            "reason": "same prices but different leverage",
            "params": {
                "entry": 1965.5,
                "tp": 1974.0,
                "sl": 1961.2,
                "leverage": 5.0
            }
        });
        let ctx = PendingOrderContext {
            has_open_orders: true,
            current_entry: Some(1965.5),
            current_tp: Some(1974.0),
            current_sl: Some(1961.2),
            current_leverage: Some(3.0),
        };

        let intent = pending_order_management_intent_from_value_with_context(&value, &ctx)
            .expect("pending intent parses");
        assert_eq!(intent.decision, PendingOrderManagementDecision::ModifyMaker);
    }
}
