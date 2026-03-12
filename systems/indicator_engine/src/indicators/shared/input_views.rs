use crate::runtime::state_store::MinuteWindowData;

pub fn window_has_activity(w: &MinuteWindowData) -> bool {
    w.trade_count > 0 || !w.profile.is_empty() || !w.force_liq.is_empty()
}
