use chrono::{DateTime, Utc};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct GapEvent {
    pub dataset_name: String,
    pub gap_from: i64,
    pub gap_to: i64,
    pub last_event_ts: DateTime<Utc>,
    pub current_event_ts: DateTime<Utc>,
}

#[derive(Default)]
pub struct GapDetector {
    trade_last_agg_id: HashMap<(String, String), i64>,
    trade_last_ts: HashMap<(String, String), DateTime<Utc>>,
    depth_last_final_update_id: HashMap<(String, String), i64>,
    depth_last_ts: HashMap<(String, String), DateTime<Utc>>,
}

impl GapDetector {
    pub fn last_trade_agg_id(&self, market: &str, symbol: &str) -> Option<i64> {
        self.trade_last_agg_id
            .get(&(market.to_string(), symbol.to_string()))
            .copied()
    }

    pub fn seed_trade(
        &mut self,
        market: &str,
        symbol: &str,
        agg_trade_id: i64,
        event_ts: DateTime<Utc>,
    ) {
        let key = (market.to_string(), symbol.to_string());
        let should_update = match self.trade_last_agg_id.get(&key).copied() {
            Some(prev) => agg_trade_id >= prev,
            None => true,
        };
        if should_update {
            self.trade_last_agg_id.insert(key.clone(), agg_trade_id);
            self.trade_last_ts.insert(key, event_ts);
        }
    }

    pub fn seed_depth(
        &mut self,
        market: &str,
        symbol: &str,
        final_update_id: i64,
        event_ts: DateTime<Utc>,
    ) {
        let key = (market.to_string(), symbol.to_string());
        let should_update = match self.depth_last_final_update_id.get(&key).copied() {
            Some(prev) => final_update_id >= prev,
            None => true,
        };
        if should_update {
            self.depth_last_final_update_id
                .insert(key.clone(), final_update_id);
            self.depth_last_ts.insert(key, event_ts);
        }
    }

    pub fn detect_trade_gap(
        &mut self,
        market: &str,
        symbol: &str,
        agg_trade_id: Option<i64>,
        event_ts: DateTime<Utc>,
    ) -> Option<GapEvent> {
        let id = agg_trade_id?;
        let key = (market.to_string(), symbol.to_string());

        if let Some(last) = self.trade_last_agg_id.get(&key).copied() {
            // Ignore duplicate/out-of-order trade ids. Regressing the baseline
            // creates false-positive large gaps and triggers replay storms.
            if id <= last {
                return None;
            }

            let out = if id > last + 1 {
                Some(GapEvent {
                    dataset_name: "md.trade_event".to_string(),
                    gap_from: last + 1,
                    gap_to: id - 1,
                    last_event_ts: self.trade_last_ts.get(&key).copied().unwrap_or(event_ts),
                    current_event_ts: event_ts,
                })
            } else {
                None
            };

            self.trade_last_agg_id.insert(key.clone(), id);
            self.trade_last_ts.insert(key, event_ts);
            return out;
        }

        self.trade_last_agg_id.insert(key.clone(), id);
        self.trade_last_ts.insert(key, event_ts);
        None
    }

    pub fn detect_depth_gap(
        &mut self,
        market: &str,
        symbol: &str,
        first_update_id: Option<i64>,
        final_update_id: Option<i64>,
        prev_final_update_id: Option<i64>,
        event_ts: DateTime<Utc>,
    ) -> Option<GapEvent> {
        let id = final_update_id?;
        let key = (market.to_string(), symbol.to_string());

        let last_seen = self.depth_last_final_update_id.get(&key).copied();
        let out = last_seen.and_then(|last| {
            // Ignore duplicate/out-of-order depth events. Regressing the
            // baseline creates false-positive gaps and reconcile loops.
            if id <= last {
                return None;
            }

            // Futures depth stream provides `pu` (previous final update id).
            // If present, it is the most reliable continuity signal.
            if let Some(prev_final) = prev_final_update_id {
                if prev_final > last {
                    return Some(GapEvent {
                        dataset_name: "md.depth_delta_l2".to_string(),
                        gap_from: last + 1,
                        gap_to: prev_final,
                        last_event_ts: self.depth_last_ts.get(&key).copied().unwrap_or(event_ts),
                        current_event_ts: event_ts,
                    });
                }
                return None;
            }

            // Spot stream has no `pu`. Fall back to FIRST update-id boundary.
            let gap_start = first_update_id.unwrap_or(id);
            if gap_start > last + 1 {
                Some(GapEvent {
                    dataset_name: "md.depth_delta_l2".to_string(),
                    gap_from: last + 1,
                    gap_to: gap_start - 1,
                    last_event_ts: self.depth_last_ts.get(&key).copied().unwrap_or(event_ts),
                    current_event_ts: event_ts,
                })
            } else {
                None
            }
        });

        if last_seen.map(|last| id > last).unwrap_or(true) {
            self.depth_last_final_update_id.insert(key.clone(), id);
            self.depth_last_ts.insert(key, event_ts);
        }
        out
    }
}
