use super::{load_prompt_asset, DECISION_LONG, DECISION_NO_TRADE, DECISION_SHORT};

const BIG_PROMPT: &str = include_str!("entry/big_opportunity.txt");
const MEDIUM_PROMPT: &str = include_str!("entry/medium_large_opportunity.txt");

pub(super) fn system_prompt(template: &str, symbol: &str) -> String {
    let asset = match template {
        "medium_large_opportunity" => MEDIUM_PROMPT,
        _ => BIG_PROMPT,
    };
    load_prompt_asset(
        asset,
        &[
            ("__LONG__", DECISION_LONG),
            ("__SHORT__", DECISION_SHORT),
            ("__NO_TRADE__", DECISION_NO_TRADE),
            ("__SYMBOL__", symbol),
        ],
    )
}
