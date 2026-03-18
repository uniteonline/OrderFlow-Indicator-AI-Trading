mod entry;
mod management;
mod pending_order;
mod scan;

pub const DECISION_LONG: &str = "LONG";
pub const DECISION_SHORT: &str = "SHORT";
pub const DECISION_NO_TRADE: &str = "NO_TRADE";
pub const DECISION_CLOSE: &str = "CLOSE";
pub const DECISION_ADD: &str = "ADD";
pub const DECISION_REDUCE: &str = "REDUCE";
pub const DECISION_HOLD: &str = "HOLD";
pub const DECISION_MODIFY_TPSL: &str = "MODIFY_TPSL";
pub const DECISION_MODIFY_MAKER: &str = "MODIFY_MAKER";
// Management review decisions (model output)
pub const DECISION_VALID: &str = "VALID";
pub const DECISION_INVALID: &str = "INVALID";
pub const DECISION_ADJUST: &str = "ADJUST";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryPromptStage {
    Scan,
    Finalize,
}

pub fn system_prompt(
    management_mode: bool,
    pending_order_mode: bool,
    prompt_template: &str,
    entry_stage: EntryPromptStage,
) -> String {
    let template = prompt_template.trim().to_ascii_lowercase();
    // Scan is shared across all modes — always use the shared scan prompt
    if matches!(entry_stage, EntryPromptStage::Scan) {
        return scan::system_prompt(&template);
    }
    // Action stage: route by account state
    if pending_order_mode {
        return pending_order::system_prompt(&template);
    }
    if management_mode {
        return management::system_prompt(&template);
    }
    entry::system_prompt(&template)
}

pub fn user_prompt_prefix(
    management_mode: bool,
    pending_order_mode: bool,
    entry_stage: EntryPromptStage,
) -> &'static str {
    if matches!(entry_stage, EntryPromptStage::Scan) {
        return "You are in market scan mode. Analyze the order-flow snapshot and return only a market structure scan JSON. This scan will be passed to a subsequent stage to make a context-specific trading decision based on the current account state.\n\n";
    }
    if pending_order_mode {
        "You are in pending-order management mode. Analyze the current stage-2 indicators plus the live pending maker-order context from `trading_state` and `management_snapshot` (especially `management_snapshot.pending_order`, `management_snapshot.position_context.entry_context`, and the previous entry reason). Also review the STAGE_1_MARKET_SCAN_JSON provided above as earlier structural context, while treating the current stage-2 prompt input as the authoritative execution-time dataset. Decide whether to keep the pending order, cancel it, or modify the maker entry / TP / SL. Return only a pending-order management decision JSON.\n\n"
    } else if management_mode {
        "You are in management mode. Analyze the current stage-2 indicators plus current position/order context from `trading_state` and `management_snapshot` (especially `context_state`, leverage, direction, position quantity, open orders, and last management reason). Also review the STAGE_1_MARKET_SCAN_JSON provided above as earlier structural context, while treating the current stage-2 prompt input as the authoritative execution-time dataset. IMPORTANT: `management_snapshot.positions[].current_tp_price` and `current_sl_price` are the ACTUAL placed TP/SL order trigger prices on the exchange (sourced directly from Binance open orders — not model estimates). Use them as baselines for all MODIFY_TPSL decisions: HC-6 requires new values to differ from these actual current prices; HC-9 requires new_sl to be tighter (more favorable) than current_sl_price. Return only a management decision JSON.\n\n"
    } else {
        "You are in entry finalize mode. Analyze the current stage-2 order-flow snapshot together with the STAGE_1_MARKET_SCAN_JSON. Treat the current stage-2 prompt input as the authoritative execution-time dataset, and use the stage-1 scan as earlier structural context. Return only the final entry decision JSON.\n\n"
    }
}

fn load_prompt_asset(asset: &'static str, replacements: &[(&str, &str)]) -> String {
    let base = asset
        .strip_suffix("\r\n")
        .or_else(|| asset.strip_suffix('\n'))
        .unwrap_or(asset);
    replacements
        .iter()
        .fold(base.to_owned(), |text, (from, to)| text.replace(from, to))
}
