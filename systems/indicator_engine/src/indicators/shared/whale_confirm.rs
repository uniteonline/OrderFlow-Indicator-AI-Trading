use crate::runtime::state_store::WhaleStats;

pub fn whale_confirms(stats: &WhaleStats) -> bool {
    stats.trade_count > 0 && stats.notional_total > 0.0
}
