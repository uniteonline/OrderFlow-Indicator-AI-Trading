use super::{load_prompt_asset, DECISION_LONG, DECISION_NO_TRADE, DECISION_SHORT};

const BIG_SCAN_PROMPT: &str = include_str!("scan/big_opportunity.txt");
const MEDIUM_SCAN_PROMPT: &str = include_str!("scan/medium_large_opportunity.txt");

pub(super) fn system_prompt(template: &str) -> String {
    let asset = match template {
        "medium_large_opportunity" => MEDIUM_SCAN_PROMPT,
        _ => BIG_SCAN_PROMPT,
    };
    load_prompt_asset(
        asset,
        &[
            ("__LONG__", DECISION_LONG),
            ("__SHORT__", DECISION_SHORT),
            ("__NO_TRADE__", DECISION_NO_TRADE),
        ],
    )
}
