use crate::indicators::context::{IndicatorComputation, IndicatorContext, IndicatorSnapshotRow};
use crate::indicators::indicator_trait::Indicator;
use crate::runtime::state_store::MinuteHistory;
use chrono::Duration;
use serde_json::{json, Value};

const WINDOWS: [(&str, i64); 5] = [
    ("15m", 15),
    ("1h", 60),
    ("4h", 240),
    ("1d", 1440),
    ("3d", 4320),
];

#[derive(Debug, Clone, Copy, Default)]
struct WhaleAgg {
    trade_count: i64,
    buy_count: i64,
    sell_count: i64,
    notional_total: f64,
    notional_buy: f64,
    notional_sell: f64,
    qty_eth_total: f64,
    qty_eth_buy: f64,
    qty_eth_sell: f64,
}

impl WhaleAgg {
    fn delta_notional(self) -> f64 {
        self.notional_buy - self.notional_sell
    }

    fn delta_qty(self) -> f64 {
        self.qty_eth_buy - self.qty_eth_sell
    }
}

pub struct I15WhaleTrades;

impl Indicator for I15WhaleTrades {
    fn code(&self) -> &'static str {
        "whale_trades"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation {
        let mut by_window = serde_json::Map::new();
        for (label, mins) in WINDOWS {
            let fut = aggregate_whale_window(&ctx.history_futures, ctx.ts_bucket, mins);
            let spot = aggregate_whale_window(&ctx.history_spot, ctx.ts_bucket, mins);
            let fut_delta_notional = fut.delta_notional();
            let spot_delta_notional = spot.delta_notional();
            let fut_delta_qty = fut.delta_qty();
            let spot_delta_qty = spot.delta_qty();
            let gap = spot_delta_notional - fut_delta_notional;
            let spot_dom = spot_delta_notional.abs()
                / (spot_delta_notional.abs() + fut_delta_notional.abs() + 1e-12);

            by_window.insert(
                label.to_string(),
                json!({
                    "window": label,
                    "fut_count": fut.trade_count,
                    "fut_buy_count": fut.buy_count,
                    "fut_sell_count": fut.sell_count,
                    "fut_notional_sum_usd": fut.notional_total,
                    "fut_whale_delta_notional": fut_delta_notional,
                    "fut_whale_delta_qty": fut_delta_qty,
                    "fut_whale_qty_buy": fut.qty_eth_buy,
                    "fut_whale_qty_sell": fut.qty_eth_sell,
                    "spot_count": spot.trade_count,
                    "spot_buy_count": spot.buy_count,
                    "spot_sell_count": spot.sell_count,
                    "spot_notional_sum_usd": spot.notional_total,
                    "spot_whale_delta_notional": spot_delta_notional,
                    "spot_whale_delta_qty": spot_delta_qty,
                    "spot_whale_qty_buy": spot.qty_eth_buy,
                    "spot_whale_qty_sell": spot.qty_eth_sell,
                    "xmk_whale_delta_notional_gap_s_minus_f": gap,
                    "spot_whale_dominance": spot_dom
                }),
            );
        }

        let fut_delta_notional = ctx.futures.whale.notional_buy - ctx.futures.whale.notional_sell;
        let spot_delta_notional = ctx.spot.whale.notional_buy - ctx.spot.whale.notional_sell;
        let fut_delta_qty = ctx.futures.whale.qty_eth_buy - ctx.futures.whale.qty_eth_sell;
        let spot_delta_qty = ctx.spot.whale.qty_eth_buy - ctx.spot.whale.qty_eth_sell;
        let xmk_gap = spot_delta_notional - fut_delta_notional;
        let spot_whale_dominance = spot_delta_notional.abs()
            / (spot_delta_notional.abs() + fut_delta_notional.abs() + 1e-12);

        IndicatorComputation {
            snapshot: Some(IndicatorSnapshotRow {
                indicator_code: self.code(),
                window_code: "1m",
                payload_json: json!({
                    "threshold_usdt": ctx.whale_threshold_usdt,
                    "by_window": Value::Object(by_window),
                    "fut_count": ctx.futures.whale.trade_count,
                    "fut_buy_count": ctx.futures.whale.buy_count,
                    "fut_sell_count": ctx.futures.whale.sell_count,
                    "fut_notional_sum_usd": ctx.futures.whale.notional_total,
                    "fut_whale_delta_notional": fut_delta_notional,
                    "fut_whale_delta_qty": fut_delta_qty,
                    "fut_whale_qty_buy": ctx.futures.whale.qty_eth_buy,
                    "fut_whale_qty_sell": ctx.futures.whale.qty_eth_sell,
                    "fut_max_single_trade_notional": ctx.futures.whale.max_single_notional,

                    "spot_count": ctx.spot.whale.trade_count,
                    "spot_buy_count": ctx.spot.whale.buy_count,
                    "spot_sell_count": ctx.spot.whale.sell_count,
                    "spot_notional_sum_usd": ctx.spot.whale.notional_total,
                    "spot_whale_delta_notional": spot_delta_notional,
                    "spot_whale_delta_qty": spot_delta_qty,
                    "spot_whale_qty_buy": ctx.spot.whale.qty_eth_buy,
                    "spot_whale_qty_sell": ctx.spot.whale.qty_eth_sell,
                    "spot_max_single_trade_notional": ctx.spot.whale.max_single_notional,

                    "xmk_whale_delta_notional_gap_s_minus_f": xmk_gap,
                    "spot_whale_dominance": spot_whale_dominance,

                    "count": ctx.futures.whale.trade_count,
                    "buy_count": ctx.futures.whale.buy_count,
                    "sell_count": ctx.futures.whale.sell_count,
                    "notional_sum_usd": ctx.futures.whale.notional_total,
                    "whale_delta_qty": fut_delta_qty
                }),
            }),
            ..Default::default()
        }
    }
}

fn aggregate_whale_window(
    history: &[MinuteHistory],
    ts_bucket: chrono::DateTime<chrono::Utc>,
    mins: i64,
) -> WhaleAgg {
    let start = ts_bucket - Duration::minutes(mins);
    let mut out = WhaleAgg::default();
    for h in history {
        if h.ts_bucket <= start || h.ts_bucket > ts_bucket {
            continue;
        }
        out.trade_count += h.whale_trade_count;
        out.buy_count += h.whale_buy_count;
        out.sell_count += h.whale_sell_count;
        out.notional_total += h.whale_notional_total;
        out.notional_buy += h.whale_notional_buy;
        out.notional_sell += h.whale_notional_sell;
        out.qty_eth_total += h.whale_qty_eth_total;
        out.qty_eth_buy += h.whale_qty_eth_buy;
        out.qty_eth_sell += h.whale_qty_eth_sell;
    }
    out
}
