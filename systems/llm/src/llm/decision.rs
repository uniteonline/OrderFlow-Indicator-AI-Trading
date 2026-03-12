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
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PendingOrderManagementIntent {
    pub decision: PendingOrderManagementDecision,
    pub new_entry: Option<f64>,
    pub new_tp: Option<f64>,
    pub new_sl: Option<f64>,
    pub reason: String,
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
    let risk_reward_ratio = find_f64(value, &["params.rr", "risk_reward_ratio"]);
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
            let risk_reward_ratio = risk_reward_ratio.ok_or_else(|| anyhow!("rr is missing"))?;
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
            if risk_reward_ratio <= 0.0 {
                return Err(anyhow!("rr must be > 0"));
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
                "decision must be {}/{}/{}/{}/{}",
                prompt::DECISION_CLOSE,
                prompt::DECISION_ADD,
                prompt::DECISION_REDUCE,
                prompt::DECISION_HOLD,
                prompt::DECISION_MODIFY_TPSL,
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
    let qty = find_f64(value, &["params.qty", "qty"]);
    let qty_ratio = find_f64(value, &["params.qty_ratio", "qty_ratio"]);
    let is_full_exit = find_bool(value, &["params.is_full_exit", "is_full_exit"]);
    let new_tp = find_f64(value, &["params.new_tp", "new_tp", "params.tp", "tp"]);
    let new_sl = find_f64(value, &["params.new_sl", "new_sl", "params.sl", "sl"]);

    if qty.is_some_and(|v| v <= 0.0) {
        return Err(anyhow!("qty must be > 0"));
    }
    if qty_ratio.is_some_and(|v| v < 0.0 || v > 1.0) {
        return Err(anyhow!("qty_ratio must be in [0, 1]"));
    }

    match decision {
        PositionManagementDecision::Add | PositionManagementDecision::Reduce => {
            if qty.is_none() && qty_ratio.is_none() {
                return Err(anyhow!(
                    "ADD/REDUCE must provide params.qty or params.qty_ratio"
                ));
            }
            if qty_ratio.is_some_and(|v| v <= 0.0) {
                return Err(anyhow!("ADD/REDUCE qty_ratio must be in (0, 1]"));
            }
            if is_full_exit.is_some_and(|v| v) {
                return Err(anyhow!("ADD/REDUCE must not set is_full_exit=true"));
            }
        }
        PositionManagementDecision::Hold => {
            if qty.is_some_and(|v| v != 0.0) {
                return Err(anyhow!("HOLD qty must be null or 0"));
            }
            if qty_ratio.is_some_and(|v| v != 0.0) {
                return Err(anyhow!("HOLD qty_ratio must be null or 0"));
            }
            if is_full_exit.is_some_and(|v| v) {
                return Err(anyhow!("HOLD must not set is_full_exit=true"));
            }
            if new_tp.is_some() || new_sl.is_some() {
                return Err(anyhow!(
                    "HOLD must not include new_tp/new_sl; use MODIFY_TPSL for exit updates"
                ));
            }
        }
        PositionManagementDecision::ModifyTpSl => {
            if qty.is_some_and(|v| v != 0.0) {
                return Err(anyhow!("MODIFY_TPSL qty must be null or 0"));
            }
            if qty_ratio.is_some_and(|v| v != 0.0) {
                return Err(anyhow!("MODIFY_TPSL qty_ratio must be null or 0"));
            }
            if is_full_exit.is_some_and(|v| v) {
                return Err(anyhow!("MODIFY_TPSL must not set is_full_exit=true"));
            }
            if new_tp.is_none() && new_sl.is_none() {
                return Err(anyhow!(
                    "MODIFY_TPSL requires at least one of params.new_tp/params.new_sl"
                ));
            }
            if new_tp.is_some_and(|v| v <= 0.0) {
                return Err(anyhow!("new_tp must be > 0 when provided"));
            }
            if new_sl.is_some_and(|v| v <= 0.0) {
                return Err(anyhow!("new_sl must be > 0 when provided"));
            }
        }
        PositionManagementDecision::Close => {
            if qty_ratio.is_some_and(|v| (v - 1.0).abs() > f64::EPSILON) {
                return Err(anyhow!("CLOSE qty_ratio must be 1 when provided"));
            }
            if is_full_exit.is_some_and(|v| !v) {
                return Err(anyhow!("CLOSE requires is_full_exit=true when provided"));
            }
        }
    }

    Ok(PositionManagementIntent {
        decision,
        qty,
        qty_ratio,
        is_full_exit,
        new_tp,
        new_sl,
        reason,
    })
}

pub fn position_management_intent_from_value_with_context(
    value: &Value,
    has_active_positions: bool,
    has_open_orders: bool,
) -> Result<PositionManagementIntent> {
    let intent = position_management_intent_from_value(value)?;
    if matches!(intent.decision, PositionManagementDecision::Reduce) && !has_active_positions {
        return Err(anyhow!(
            "REDUCE is invalid when no active positions exist; use HOLD/CLOSE/ADD instead"
        ));
    }
    if matches!(intent.decision, PositionManagementDecision::ModifyTpSl) && !has_active_positions {
        return Err(anyhow!(
            "MODIFY_TPSL is invalid when no active positions exist"
        ));
    }
    if !has_active_positions
        && !has_open_orders
        && matches!(intent.decision, PositionManagementDecision::Close)
    {
        return Err(anyhow!(
            "CLOSE is invalid when neither active positions nor open orders exist"
        ));
    }
    Ok(intent)
}

pub fn pending_order_management_intent_from_value(
    value: &Value,
) -> Result<PendingOrderManagementIntent> {
    let decision_raw = value
        .get("decision")
        .and_then(Value::as_str)
        .map(str::trim)
        .ok_or_else(|| {
            anyhow!(
                "decision must be {}/{}/{}",
                prompt::DECISION_HOLD,
                prompt::DECISION_CLOSE,
                prompt::DECISION_MODIFY_MAKER,
            )
        })?;
    let decision = parse_pending_order_management_decision(decision_raw)?;
    let reason = value
        .get("reason")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .ok_or_else(|| anyhow!("reason must be non-empty"))?
        .to_string();
    let new_entry = find_f64(
        value,
        &["params.new_entry", "new_entry", "params.entry", "entry"],
    );
    let new_tp = find_f64(value, &["params.new_tp", "new_tp", "params.tp", "tp"]);
    let new_sl = find_f64(value, &["params.new_sl", "new_sl", "params.sl", "sl"]);

    if new_entry.is_some_and(|v| v <= 0.0) {
        return Err(anyhow!("new_entry must be > 0 when provided"));
    }
    if new_tp.is_some_and(|v| v <= 0.0) {
        return Err(anyhow!("new_tp must be > 0 when provided"));
    }
    if new_sl.is_some_and(|v| v <= 0.0) {
        return Err(anyhow!("new_sl must be > 0 when provided"));
    }

    match decision {
        PendingOrderManagementDecision::Hold | PendingOrderManagementDecision::Close => {
            if new_entry.is_some() || new_tp.is_some() || new_sl.is_some() {
                return Err(anyhow!(
                    "{} must not include new_entry/new_tp/new_sl",
                    decision.as_str()
                ));
            }
        }
        PendingOrderManagementDecision::ModifyMaker => {
            if new_entry.is_none() && new_tp.is_none() && new_sl.is_none() {
                return Err(anyhow!(
                    "MODIFY_MAKER requires at least one of params.new_entry/params.new_tp/params.new_sl"
                ));
            }
        }
    }

    Ok(PendingOrderManagementIntent {
        decision,
        new_entry,
        new_tp,
        new_sl,
        reason,
    })
}

pub fn pending_order_management_intent_from_value_with_context(
    value: &Value,
    has_open_orders: bool,
) -> Result<PendingOrderManagementIntent> {
    let intent = pending_order_management_intent_from_value(value)?;
    if !has_open_orders {
        return Err(anyhow!(
            "pending-order management is invalid when no open orders exist"
        ));
    }
    Ok(intent)
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
        "CLOSE" => Ok(PositionManagementDecision::Close),
        "ADD" => Ok(PositionManagementDecision::Add),
        "REDUCE" => Ok(PositionManagementDecision::Reduce),
        "HOLD" => Ok(PositionManagementDecision::Hold),
        "MODIFY_TPSL" => Ok(PositionManagementDecision::ModifyTpSl),
        "减仓" => Ok(PositionManagementDecision::Reduce),
        _ => Err(anyhow!(
            "decision must be {}/{}/{}/{}/{}",
            prompt::DECISION_CLOSE,
            prompt::DECISION_ADD,
            prompt::DECISION_REDUCE,
            prompt::DECISION_HOLD,
            prompt::DECISION_MODIFY_TPSL
        )),
    }
}

fn parse_pending_order_management_decision(raw: &str) -> Result<PendingOrderManagementDecision> {
    match raw.trim().to_ascii_uppercase().as_str() {
        "HOLD" => Ok(PendingOrderManagementDecision::Hold),
        "CLOSE" => Ok(PendingOrderManagementDecision::Close),
        "MODIFY_MAKER" => Ok(PendingOrderManagementDecision::ModifyMaker),
        _ => Err(anyhow!(
            "decision must be {}/{}/{}",
            prompt::DECISION_HOLD,
            prompt::DECISION_CLOSE,
            prompt::DECISION_MODIFY_MAKER
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
        pending_order_management_intent_from_value, trade_intent_from_value,
        PendingOrderManagementDecision, TradeDecision,
    };
    use serde_json::json;

    #[test]
    fn pending_modify_maker_parses_new_entry_tp_sl() {
        let value = json!({
            "decision": "MODIFY_MAKER",
            "reason": "same strategy, better maker zone",
            "params": {
                "new_entry": 1965.5,
                "new_tp": 1974.0,
                "new_sl": 1961.2
            }
        });

        let intent =
            pending_order_management_intent_from_value(&value).expect("pending modify parses");
        assert_eq!(intent.decision, PendingOrderManagementDecision::ModifyMaker);
        assert_eq!(intent.new_entry, Some(1965.5));
        assert_eq!(intent.new_tp, Some(1974.0));
        assert_eq!(intent.new_sl, Some(1961.2));
    }

    #[test]
    fn pending_hold_rejects_price_changes() {
        let value = json!({
            "decision": "HOLD",
            "reason": "thesis intact",
            "params": {
                "new_entry": 1965.5,
                "new_tp": null,
                "new_sl": null
            }
        });

        let err = pending_order_management_intent_from_value(&value)
            .expect_err("hold with new_entry should fail");
        assert!(err
            .to_string()
            .contains("must not include new_entry/new_tp/new_sl"));
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
}
