use super::{load_prompt_asset, DECISION_CLOSE, DECISION_HOLD, DECISION_MODIFY_MAKER};

const BIG_PROMPT: &str = include_str!("pending_order/big_opportunity.txt");
const MEDIUM_PROMPT: &str = include_str!("pending_order/medium_large_opportunity.txt");

pub(super) fn system_prompt(template: &str, symbol: &str) -> String {
    let asset = match template {
        "medium_large_opportunity" => MEDIUM_PROMPT,
        _ => BIG_PROMPT,
    };
    load_prompt_asset(
        asset,
        &[
            ("__HOLD__", DECISION_HOLD),
            ("__CLOSE__", DECISION_CLOSE),
            ("__MODIFY_MAKER__", DECISION_MODIFY_MAKER),
            ("__SYMBOL__", symbol),
        ],
    )
}
