use crate::exchange::binance::rest::funding_rate::BinanceFundingRateRecord;
use crate::normalize::{normalize_funding_rate_rest, NormalizedMdEvent};
use anyhow::Result;

const EIGHT_HOURS_MS: i64 = 8 * 60 * 60 * 1000;

pub fn normalize_rest_record(
    record: &BinanceFundingRateRecord,
    backfill_in_progress: bool,
) -> Result<NormalizedMdEvent> {
    let next_funding_time = Some(record.funding_time + EIGHT_HOURS_MS);

    normalize_funding_rate_rest(
        &record.symbol,
        record.funding_time,
        &record.funding_rate,
        record.mark_price.as_deref(),
        next_funding_time,
        backfill_in_progress,
    )
}

#[cfg(test)]
mod tests {
    use super::normalize_rest_record;
    use crate::exchange::binance::rest::funding_rate::BinanceFundingRateRecord;

    #[test]
    fn normalize_rest_record_respects_backfill_flag() {
        let record = BinanceFundingRateRecord {
            symbol: "ETHUSDT".to_string(),
            funding_rate: "-0.0001".to_string(),
            funding_time: 1_700_000_000_000,
            mark_price: Some("2000.0".to_string()),
        };

        let live = normalize_rest_record(&record, false).expect("live normalize");
        assert!(!live.backfill_in_progress);

        let replay = normalize_rest_record(&record, true).expect("replay normalize");
        assert!(replay.backfill_in_progress);
    }
}
