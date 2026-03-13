use super::{
    load_prompt_asset, EntryPromptStage, DECISION_LONG, DECISION_NO_TRADE, DECISION_SHORT,
};

const BIG_SCAN_PROMPT: &str = include_str!("entry/scan/big_opportunity.txt");
const BIG_FINALIZE_PROMPT: &str = include_str!("entry/finalize/big_opportunity.txt");
const MEDIUM_SCAN_PROMPT: &str = include_str!("entry/scan/medium_large_opportunity.txt");
const MEDIUM_FINALIZE_PROMPT: &str = include_str!("entry/finalize/medium_large_opportunity.txt");

pub(super) fn system_prompt(template: &str, stage: EntryPromptStage) -> String {
    let asset = match (template, stage) {
        ("medium_large_opportunity", EntryPromptStage::Scan) => MEDIUM_SCAN_PROMPT,
        ("medium_large_opportunity", EntryPromptStage::Finalize) => MEDIUM_FINALIZE_PROMPT,
        (_, EntryPromptStage::Scan) => BIG_SCAN_PROMPT,
        (_, EntryPromptStage::Finalize) => BIG_FINALIZE_PROMPT,
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
