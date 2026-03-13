use super::{
    load_prompt_asset, DECISION_ADD, DECISION_CLOSE, DECISION_HOLD, DECISION_MODIFY_TPSL,
    DECISION_REDUCE,
};

const BIG_PROMPT: &str = include_str!("management/big_opportunity.txt");
const MEDIUM_PROMPT: &str = include_str!("management/medium_large_opportunity.txt");

pub(super) fn system_prompt(template: &str) -> String {
    let asset = match template {
        "medium_large_opportunity" => MEDIUM_PROMPT,
        _ => BIG_PROMPT,
    };
    load_prompt_asset(
        asset,
        &[
            ("__CLOSE__", DECISION_CLOSE),
            ("__ADD__", DECISION_ADD),
            ("__REDUCE__", DECISION_REDUCE),
            ("__HOLD__", DECISION_HOLD),
            ("__MODIFY_TPSL__", DECISION_MODIFY_TPSL),
        ],
    )
}
