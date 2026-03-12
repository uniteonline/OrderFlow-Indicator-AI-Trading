use crate::indicators::context::{IndicatorContext, KlineHistoryBar};
use crate::indicators::indicator_trait::Indicator;
use crate::indicators::shared::output_mapper::snapshot_only;
use crate::runtime::state_store::MinuteHistory;
use chrono::{DateTime, Duration, Utc};
use serde_json::json;
use std::collections::BTreeMap;

pub struct I19KlineHistory;

impl Indicator for I19KlineHistory {
    fn code(&self) -> &'static str {
        "kline_history"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> crate::indicators::context::IndicatorComputation {
        let current_minute_close = ctx.ts_bucket + Duration::minutes(1);
        let interval_specs = [
            ("1m", 1, ctx.kline_history_bars_1m),
            ("15m", 15, ctx.kline_history_bars_15m),
            ("4h", 240, ctx.kline_history_bars_4h),
            ("1d", 1440, ctx.kline_history_bars_1d),
        ];

        let mut intervals = serde_json::Map::new();
        for (interval_code, interval_minutes, limit) in interval_specs {
            let futures_bars = match interval_code {
                "4h" => build_interval_bars_with_db(
                    &ctx.history_futures,
                    &ctx.kline_history_futures_4h_db,
                    interval_minutes,
                    limit,
                    current_minute_close,
                ),
                "1d" if ctx.kline_history_fill_1d_from_db => build_interval_bars_with_db(
                    &ctx.history_futures,
                    &ctx.kline_history_futures_1d_db,
                    interval_minutes,
                    limit,
                    current_minute_close,
                ),
                _ => build_interval_bars(
                    &ctx.history_futures,
                    interval_minutes,
                    limit,
                    current_minute_close,
                ),
            };
            let spot_bars = match interval_code {
                "4h" => build_interval_bars_with_db(
                    &ctx.history_spot,
                    &ctx.kline_history_spot_4h_db,
                    interval_minutes,
                    limit,
                    current_minute_close,
                ),
                "1d" if ctx.kline_history_fill_1d_from_db => build_interval_bars_with_db(
                    &ctx.history_spot,
                    &ctx.kline_history_spot_1d_db,
                    interval_minutes,
                    limit,
                    current_minute_close,
                ),
                _ => build_interval_bars(
                    &ctx.history_spot,
                    interval_minutes,
                    limit,
                    current_minute_close,
                ),
            };

            intervals.insert(
                interval_code.to_string(),
                json!({
                    "interval_code": interval_code,
                    "requested_count": limit,
                    "markets": {
                        "futures": {
                            "returned_count": futures_bars.len(),
                            "bars": futures_bars,
                        },
                        "spot": {
                            "returned_count": spot_bars.len(),
                            "bars": spot_bars,
                        }
                    }
                }),
            );
        }

        snapshot_only(
            self.code(),
            json!({
                "indicator": "kline_history",
                "window": "1m",
                "as_of_ts": current_minute_close.to_rfc3339(),
                "intervals": intervals,
            }),
        )
    }
}

#[derive(Clone)]
struct BarAccumulator {
    open_time: DateTime<Utc>,
    close_time: DateTime<Utc>,
    expected_minutes: i64,
    covered_minutes: i64,
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
    volume_base: f64,
    volume_quote: f64,
}

impl BarAccumulator {
    fn new(open_time: DateTime<Utc>, interval_minutes: i64) -> Self {
        Self {
            open_time,
            close_time: open_time + Duration::minutes(interval_minutes),
            expected_minutes: interval_minutes,
            covered_minutes: 0,
            open: None,
            high: None,
            low: None,
            close: None,
            volume_base: 0.0,
            volume_quote: 0.0,
        }
    }

    fn apply(&mut self, bar: &MinuteHistory) {
        let open = bar.open_price.or(bar.last_price).or(bar.close_price);
        let high = bar
            .high_price
            .or(bar.close_price)
            .or(bar.last_price)
            .or(open);
        let low = bar
            .low_price
            .or(bar.close_price)
            .or(bar.last_price)
            .or(open);
        let close = bar.close_price.or(bar.last_price).or(bar.open_price);

        if self.open.is_none() {
            self.open = open;
        }
        if let Some(value) = high {
            self.high = Some(self.high.map_or(value, |prev| prev.max(value)));
        }
        if let Some(value) = low {
            self.low = Some(self.low.map_or(value, |prev| prev.min(value)));
        }
        if close.is_some() {
            self.close = close;
        }

        self.covered_minutes += 1;
        self.volume_base += bar.total_qty;
        self.volume_quote += bar.total_notional;
    }

    fn to_bar(self, current_minute_close: DateTime<Utc>) -> KlineHistoryBar {
        KlineHistoryBar {
            open_time: self.open_time,
            close_time: self.close_time,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume_base: self.volume_base,
            volume_quote: self.volume_quote,
            is_closed: self.close_time <= current_minute_close,
            minutes_covered: self.covered_minutes,
            expected_minutes: self.expected_minutes,
        }
    }
}

pub fn build_interval_bar_records(
    history: &[MinuteHistory],
    interval_minutes: i64,
    limit: usize,
    current_minute_close: DateTime<Utc>,
) -> Vec<KlineHistoryBar> {
    if interval_minutes <= 0 || limit == 0 || history.is_empty() {
        return Vec::new();
    }

    if interval_minutes == 1 {
        return history
            .iter()
            .rev()
            .take(limit)
            .map(minute_bar_to_record)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
    }

    let mut grouped = BTreeMap::<DateTime<Utc>, BarAccumulator>::new();
    for minute in history {
        let open_time = floor_to_interval(minute.ts_bucket, interval_minutes);
        let entry = grouped
            .entry(open_time)
            .or_insert_with(|| BarAccumulator::new(open_time, interval_minutes));
        entry.apply(minute);
    }

    let mut bars = grouped
        .into_values()
        .map(|bar| bar.to_bar(current_minute_close))
        .collect::<Vec<_>>();
    if bars.len() > limit {
        bars = bars.split_off(bars.len() - limit);
    }
    bars
}

pub fn oldest_retained_bar_open_time(
    history: &[MinuteHistory],
    interval_minutes: i64,
    limit: usize,
    current_minute_close: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    build_interval_bar_records(history, interval_minutes, limit, current_minute_close)
        .first()
        .map(|bar| bar.open_time)
}

fn build_interval_bars(
    history: &[MinuteHistory],
    interval_minutes: i64,
    limit: usize,
    current_minute_close: DateTime<Utc>,
) -> Vec<serde_json::Value> {
    build_interval_bar_records(history, interval_minutes, limit, current_minute_close)
        .into_iter()
        .map(bar_to_json)
        .collect()
}

fn build_interval_bars_with_db(
    history: &[MinuteHistory],
    db_bars: &[KlineHistoryBar],
    interval_minutes: i64,
    limit: usize,
    current_minute_close: DateTime<Utc>,
) -> Vec<serde_json::Value> {
    let mut merged = BTreeMap::<DateTime<Utc>, KlineHistoryBar>::new();
    for bar in db_bars {
        merged.insert(bar.open_time, bar.clone());
    }
    for bar in
        build_interval_bar_records(history, interval_minutes, usize::MAX, current_minute_close)
    {
        merged.insert(bar.open_time, bar);
    }

    let mut bars = merged.into_values().map(bar_to_json).collect::<Vec<_>>();
    if bars.len() > limit {
        bars = bars.split_off(bars.len() - limit);
    }
    bars
}

fn minute_bar_to_record(bar: &MinuteHistory) -> KlineHistoryBar {
    let open_time = bar.ts_bucket;
    let close_time = open_time + Duration::minutes(1);
    KlineHistoryBar {
        open_time,
        close_time,
        open: bar.open_price.or(bar.last_price).or(bar.close_price),
        high: bar
            .high_price
            .or(bar.close_price)
            .or(bar.last_price)
            .or(bar.open_price),
        low: bar
            .low_price
            .or(bar.close_price)
            .or(bar.last_price)
            .or(bar.open_price),
        close: bar.close_price.or(bar.last_price).or(bar.open_price),
        volume_base: bar.total_qty,
        volume_quote: bar.total_notional,
        is_closed: true,
        minutes_covered: 1,
        expected_minutes: 1,
    }
}

fn bar_to_json(bar: KlineHistoryBar) -> serde_json::Value {
    json!({
        "open_time": bar.open_time.to_rfc3339(),
        "close_time": bar.close_time.to_rfc3339(),
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume_base": bar.volume_base,
        "volume_quote": bar.volume_quote,
        "is_closed": bar.is_closed,
        "minutes_covered": bar.minutes_covered,
        "expected_minutes": bar.expected_minutes,
    })
}

fn floor_to_interval(ts: DateTime<Utc>, interval_minutes: i64) -> DateTime<Utc> {
    let secs = interval_minutes * 60;
    let aligned = ts.timestamp().div_euclid(secs) * secs;
    DateTime::<Utc>::from_timestamp(aligned, 0).unwrap_or(ts)
}
