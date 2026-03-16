# Scan 数据源过滤规则 v4

**状态**：草案（待复核）
**基于**：v3.1（全量继承）
**适用文件**：`systems/llm/src/llm/filter/scan.rs`
**基线体积**：v3.1 ~110KB（minified）
**目标体积**：< 130KB（minified）
**核心思路**：v3.1 是"压缩后的原始指标"，v4 改为"按 timeframe 组织的标准特征层"

**v4 与 v3.1 的差异**：
1. `absorption` 三套合并为 `absorption_summary`（单一 timeframe 结构，删冗余两套）
2. `cvd_pack` 序列替换为 `cvd_summary`（每窗口 6 个标量，不再给历史 series）
3. `kline_history` bars 替换为 `kline_derived`（每窗口 8 个派生特征，不再给原始 OHLCV）
4. 新增 `by_timeframe` 顶层结构：把 structure/trend/flow/volatility 按 15m/4h/1d 聚合，LLM 直接读每个 timeframe 的固定骨架

> **保留 v3.1 的所有内容**：`level_book`、`timeframe_summary`、`split_select footprint`、`§3.9` 小优化全部继承。

---

## 一、v4 变更规则

### 4.1 absorption_summary — 合并三套 absorption（替换 v3.1 三个指标）

**问题**：`absorption`（方向混合）、`bullish_absorption`（方向=1 子集）、`bearish_absorption`（方向=-1 子集）在 v3.1 各自携带 timeframe_summary，信息高度重叠。三套合计 ~17KB，但 LLM 实际需要的只是"每个 timeframe 的多空计数 + 代表事件"。

**v4 规则**：删除 `bullish_absorption` 和 `bearish_absorption` 指标。将 `absorption` 重命名为 `absorption_summary`，**仅保留 timeframe_summary**，删除 `recent_7d.events` 原始列表。

```json
"absorption_summary": {
  "15m": {
    "bullish_count": 0,
    "bearish_count": 2,
    "bias": "bearish",
    "bullish_events": [],
    "bearish_events": [
      {"confirm_ts": "2026-03-16T02:07:00+00:00", "pivot_price": 2188.45, "score": 0.724, "type": "bearish_absorption"},
      {"confirm_ts": "2026-03-16T01:45:00+00:00", "pivot_price": 2201.30, "score": 0.851, "type": "bearish_absorption"}
    ]
  },
  "4h": { "bullish_count": 8, "bearish_count": 12, "bias": "bearish", "bullish_events": [...], "bearish_events": [...] },
  "1d": { ... }
}
```

事件选取规则沿用 v3.1 §3.8 三维选取（最近/最强/近价各 1，去重后 1-3 条）。

**节省**：~17KB → ~3KB = **−14KB**

---

### 4.2 cvd_summary — 序列改标量

**问题**：`cvd_pack` 当前每窗口给 30/20/7 条 delta series，实测 ~21.5KB。Stage1 方向判断只需"当前状态 + 近期趋势方向"，不需要模型自己从 30 根 bar 里推断 slope。

**v4 规则**：删除 `cvd_pack` 的全部 `by_window.series`，替换为每窗口 6 个标量。顶层快照字段（`delta_fut`/`delta_spot`/`relative_delta_*`/`likely_driver`/`spot_flow_dominance`/`cvd_slope_fut`/`cvd_slope_spot`）保留（这是当前 bar 瞬时值，非冗余）。

**数据源说明**：`scan.rs` 中 `take_last_n()` 在截取后调用 `.reverse()`（line 872），所以 scan 输出的 series 是 **newest-first**（`series[0]` / `.first()` = 最新 bar，`series[N-1]` / `.last()` = 最旧保留 bar）。每条 entry 包含字段 `delta_fut`、`delta_spot`、`relative_delta_fut`、`relative_delta_spot`、`spot_flow_dominance`、`cvd_7d_fut`、`ts`（字段名见 scan.rs `filter_cvd_pack` lines 406-420）。

**slope 计算方式**：不使用单点差（`delta_fut[0] - delta_fut[N-1]`），而是使用 series 中的 `cvd_7d_fut` 字段斜率。`cvd_7d_fut` 是 7 日累计 CVD，逐 bar 累积，对短期噪声有平滑效果。`slope_sign = sign(series[0].cvd_7d_fut - series[N-1].cvd_7d_fut)`（最新减最旧，正=7d CVD 上升=整体净买入增加）。

**每窗口标量定义**（Rust: `s = series.first().unwrap()`（最新），`s0 = series.last().unwrap()`（最旧））：

| 字段 | 含义 | 计算 |
|------|------|------|
| `delta_sign` | 最新 bar delta 方向 | `sign(s.delta_fut)`: +1/−1/0 |
| `relative_delta` | 最新 bar 相对多空强度 | `s.relative_delta_fut` ∈ [−1, 1]（直接取字段） |
| `slope_sign` | 近期 CVD 趋势方向（平滑） | `sign(s.cvd_7d_fut − s0.cvd_7d_fut)`；需 series ≥2 条，否则 0 |
| `lead` | 哪个市场在主导最新 bar | `s.spot_flow_dominance`（直接取字段："futures"/"spot"/"balanced"） |
| `bias` | 综合多空偏向 | `delta_sign == slope_sign` 时用该方向（"bullish"/"bearish"）；冲突时 "neutral" |
| `last_ts` | 最新 series 时间戳 | `s.ts` |

```json
"cvd_summary": {
  "snapshot": {
    "delta_fut": 12450.3,
    "delta_spot": -3210.5,
    "relative_delta_fut": 0.23,
    "relative_delta_spot": -0.08
  },
  "by_window": {
    "15m": {"delta_sign": 1, "relative_delta": 0.23, "slope_sign": 1, "lead": "futures", "bias": "bullish", "last_ts": "2026-03-16T02:29:00+00:00"},
    "4h":  {"delta_sign": -1, "relative_delta": -0.15, "slope_sign": -1, "lead": "balanced", "bias": "bearish", "last_ts": "2026-03-16T02:29:00+00:00"},
    "1d":  {"delta_sign": -1, "relative_delta": -0.31, "slope_sign": -1, "lead": "futures", "bias": "bearish", "last_ts": "2026-03-16T02:29:00+00:00"}
  }
}
```

**实现**：将 `filter_cvd_pack` 改为 `build_cvd_summary`，从 `by_window[tf].series` 读取数据后丢弃 series，只输出标量。

**节省**：~21.5KB → ~0.5KB = **−21KB**

---

### 4.3 kline_derived — bars 改派生特征

**问题**：`kline_history` 每窗口给一个 OHLCV bars 数组（15m 约 50 条、4h 约 30 条、1d 约 10-20 条），总计 ~15KB。LLM 从这些 bars 里做的工作（判断趋势方向、波动率、相对位置）完全可以提前计算后直接给出。

**v4 规则**：删除 `kline_history` 的 bars 数组，替换为每窗口 8 个派生特征。`compute_atr14` 等计算仍从原始 bars 执行，计算完后丢弃 bars。

**数据源说明**：kline_history 的 bars 在源文件（`temp_indicator`）里是**时间升序**（close/high/low 字段名），scan 输出里是 **newest-first**（c/h/l 紧凑键）。`build_kline_derived` 从源文件 bars 计算，统一用升序（`bars.last()` = 最新 bar）。

**`ema_trend_regime` 独立处理**：scan.rs 中 `filter_ema_trend_regime`（line 527）已将当前 regime 输出到 `regime_series_15m`（最近 20 条），每条有 `trend_regime`（15m）、`trend_regime_4h`、`trend_regime_1d`。`build_kline_derived` **不重新计算 regime**，`by_timeframe.trend.ema_regime` 直接从 `ema_trend_regime.regime_series_15m[0]` 对应字段取。

**每窗口派生字段**（`kline_derived`，共 8 个）：

| 字段 | 含义 | 计算方式 | 有效 tf |
|------|------|---------|---------|
| `atr14` | ATR(14)，波动率绝对值 | 标准 ATR 公式，基于 `high/low/close`（升序 bars） | 全部 |
| `atr14_pct` | ATR 相对价格百分比 | `atr14 / close × 100` | 全部 |
| `swing_high` | 近期摆动高点 | 最近 N 根 bars 的 max(high)：15m N=10，4h N=5，1d N=3 | 全部 |
| `swing_low` | 近期摆动低点 | 同上 min(low) | 全部 |
| `close_vs_ema21_pct` | 收盘价相对 EMA21 偏差 | `(close − ema21) / ema21 × 100`；从 `ema_trend_regime.payload.ema_21` 取（预计算，无需重算）| **15m 专用**；4h/1d = null |
| `close_vs_ema100_htf_pct` | 收盘价相对 HTF EMA100 偏差 | `(close − ema100_htf) / ema100_htf × 100`；从 `ema_trend_regime.payload.ema_100_htf[tf]` 取 | **4h/1d 专用**；15m = null |
| `close_vs_vah_pct` | 收盘价相对 PVS VAH 偏差 | `(close − vah) / vah × 100`；从 `price_volume_structure.by_window[tf].vah` 取 | 全部 |
| `value_area_position` | 收盘价在价值区间的离散位置 | `close > vah` → "above_vah"；`close < val` → "below_val"；否则 "inside_va" | 全部 |

> **为何不从 bars 重算 EMA**：kline_history 的 bars 数量有限（15m=30条, 4h=20条, 1d=14条），计算 EMA20/50 样本不足（1d 甚至算不出 EMA20）。`ema_trend_regime` 已有预计算的精确值，直接复用即可。
> `close_vs_vah_pct` 和 `value_area_position` 需要跨指标引用 PVS。实现时在 `build_kline_derived` 中接受 `pvs_by_window: &Map` 和 `ema_regime_payload: &Map` 两个参数。
> `atr14_pct_rank_30d`（v3 §3.5）**不在 scan.rs 中**，v4 不使用；若未来源数据提供预计算值可再加入。

```json
"kline_derived": {
  "15m": {
    "atr14": 12.4, "atr14_pct": 0.57,
    "swing_high": 2279.97, "swing_low": 2235.13,
    "close_vs_ema21_pct": -0.12,       // (2259.92 - 2262.63) / 2262.63 * 100
    "close_vs_ema100_htf_pct": null,   // ema_100_htf 仅含 4h/1d
    "close_vs_vah_pct": -0.16,         // pvs.by_window[15m].vah = 2263.54
    "value_area_position": "inside_va" // val=2258.72 < close=2259.92 < vah=2263.54
  },
  "4h": {
    "atr14": 44.8, "atr14_pct": 1.98,
    "swing_high": 2279.97, "swing_low": 2235.13,
    "close_vs_ema21_pct": null,        // ema_21 仅含 15m 级别
    "close_vs_ema100_htf_pct": 10.8,   // (2259.92 - 2039.63) / 2039.63 * 100
    "close_vs_vah_pct": -0.48,         // pvs.by_window[4h].vah = 2270.77
    "value_area_position": "inside_va" // val=2216.18 < close=2259.92 < vah=2270.77
  },
  "1d": {
    "atr14": 120.3, "atr14_pct": 5.32,
    "swing_high": 2279.97, "swing_low": 2163.32,
    "close_vs_ema21_pct": null,
    "close_vs_ema100_htf_pct": 17.4,   // (2259.92 - 1925.09) / 1925.09 * 100
    "close_vs_vah_pct": -0.88,         // pvs.by_window[1d].vah = 2279.97
    "value_area_position": "inside_va" // val=2118.89 < close=2259.92 < vah=2279.97
  }
}
```

```rust
fn build_kline_derived(
    source: &Map<String, Value>,
    pvs_by_window: &Map<String, Value>,
    ema_regime_payload: &Map<String, Value>,  // ema_trend_regime.payload
) -> Value {
    // Pre-extract EMA values from ema_trend_regime (pre-computed, no bar recomputation needed)
    // ema_21 is 15m-level only; ema_100_htf contains {"4h": ..., "1d": ...}
    let ema21 = ema_regime_payload.get("ema_21").and_then(Value::as_f64);
    let ema100_htf = ema_regime_payload.get("ema_100_htf").and_then(Value::as_object);

    let mut by_window = Map::new();
    for tf in SCAN_WINDOWS {
        // source bars: time-ascending (close/high/low field names)
        let bars = get_kline_bars_ascending(source, tf);
        let atr14 = compute_atr14(&bars);
        let close = bars.last().and_then(|b| b.get("close").and_then(Value::as_f64)).unwrap_or(0.0);
        let n_swing = match tf { "15m" => 10, "4h" => 5, _ => 3 };
        let swing_high = bars.iter().rev().take(n_swing)
            .filter_map(|b| b.get("high").and_then(Value::as_f64))
            .fold(f64::NEG_INFINITY, f64::max);
        let swing_low = bars.iter().rev().take(n_swing)
            .filter_map(|b| b.get("low").and_then(Value::as_f64))
            .fold(f64::INFINITY, f64::min);

        // EMA: use pre-computed values from ema_trend_regime (bars count is insufficient for recompute)
        // 15m: use ema_21 (pre-computed for 15m close); 4h/1d: null (ema_21 is 15m-level only)
        let close_vs_ema21 = if tf == "15m" {
            ema21.map(|e| (close - e) / e * 100.0).map(|v| json!(round2(v)))
                .unwrap_or(Value::Null)
        } else { Value::Null };
        // 4h/1d: use ema_100_htf[tf]; 15m: null (HTF indicator only covers 4h/1d)
        let close_vs_ema100_htf = if tf != "15m" {
            ema100_htf.and_then(|m| m.get(tf)).and_then(Value::as_f64)
                .map(|e| json!(round2((close - e) / e * 100.0)))
                .unwrap_or(Value::Null)
        } else { Value::Null };

        let pvs_tf = pvs_by_window.get(tf).and_then(Value::as_object);
        let vah = pvs_tf.and_then(|w| w.get("vah")).and_then(Value::as_f64);
        let val = pvs_tf.and_then(|w| w.get("val")).and_then(Value::as_f64);
        let value_area_pos: Value = match (vah, val) {
            (Some(vah), Some(_))   if close > vah => json!("above_vah"),
            (Some(_),   Some(val)) if close < val  => json!("below_val"),
            (Some(_),   Some(_))                   => json!("inside_va"),
            _ => Value::Null,
        };
        by_window.insert(tf.to_string(), json!({
            "atr14": round2(atr14),
            "atr14_pct": round2(atr14 / close * 100.0),
            "swing_high": swing_high,
            "swing_low": swing_low,
            "close_vs_ema21_pct": close_vs_ema21,
            "close_vs_ema100_htf_pct": close_vs_ema100_htf,
            "close_vs_vah_pct": vah.map(|v| json!(round2((close - v) / v * 100.0))).unwrap_or(Value::Null),
            "value_area_position": value_area_pos,
        }));
    }
    Value::Object(by_window)
}
```

**注意**：`volatility_summary`（v3 §3.5）的 `atr14/atr14_pct` 与 `kline_derived` 重叠。v4 中删除 `volatility_summary`，atr14 统一来自 `kline_derived`（已在 `by_timeframe.volatility` 中）。

**节省**：~15KB → ~1.5KB = **−13.5KB**

---

### 4.4 by_timeframe — 按 timeframe 的标准特征层

**目标**：当前 scan 输出是"指标平铺"，LLM 需要在 15+ 个指标间跳转才能拼出"15m 方向"。`by_timeframe` 直接给每个 timeframe 一个固定骨架，LLM 读 `by_timeframe.4h` 就能拿到 4h 的所有决策信息。

**设计**：`by_timeframe` 是一个**视图层**，不引入新数据，仅将 §4.1-4.3 的输出 + v3.1 的 `level_book` 按 timeframe 重组。**原始指标不重复输出**（`kline_derived`、`cvd_summary`、`absorption_summary` 只出现在 `by_timeframe` 内，不再作为顶层字段）。

**固定骨架**（每个 timeframe）：

```
by_timeframe[tf]:
  structure:
    swing_high / swing_low          ← kline_derived[tf]
    level_book (support/resistance) ← level_book[tf]  (v3.1 §3.7)
  trend:
    ema_regime                      ← ema_trend_regime.regime_series_15m[0]
                                      ⚠️ series 是 newest-first（实测 [0].ts = 最新 bar）
                                      Rust: regime_series_15m.first()，不是 .last()
                                      tf=15m → .trend_regime
                                      tf=4h  → .trend_regime_4h
                                      tf=1d  → .trend_regime_1d
    close_vs_ema21_pct              ← kline_derived[tf]  (15m 有效，4h/1d = null)
    close_vs_ema100_htf_pct         ← kline_derived[tf]  (4h/1d 有效，15m = null)
    close_vs_vah_pct                ← kline_derived[tf]
    value_area_position             ← kline_derived[tf] (§4.3)
  flow:
    cvd                             ← cvd_summary.by_window[tf]
    absorption                      ← absorption_summary[tf]
    buying_exhaustion               ← buying_exhaustion.timeframe_summary[tf]  (v3.1 §3.8)
    selling_exhaustion              ← selling_exhaustion.timeframe_summary[tf]
    divergence                      ← divergence.timeframe_summary[tf]
    initiation                      ← bullish/bearish_initiation.timeframe_summary[tf]（与 absorption 同处理）
    whale                           ← whale_trades.by_window[tf]（轻量摘要，见下）
    vpin                            ← vpin.by_window[tf]（轻量摘要，见下）
    funding                         ← funding_rate.by_window[tf]（轻量摘要，见下）
  volatility:
    atr14                           ← kline_derived[tf]
    atr14_pct                       ← kline_derived[tf]
```

> `ema_trend_regime.regime_series_15m` 是 scan.rs（line 527）已输出的现有字段。**实测确认**：`[0]` 是**最新**条目（newest-first），`[-1]` 是最旧。Rust 实现中用 `.first()` 取最新 regime 条目，**不是** `.last()`。

**flow 轻量摘要定义**（whale/vpin/funding）：

| 字段 | 来源 | 输出字段 |
|------|------|---------|
| `whale[tf]` | `whale_trades.by_window[tf]` | `{"net_delta_notional": f64, "spot_fut_gap": f64, "bias": "bullish"\|"bearish"\|"neutral"}`<br>`net_delta_notional = fut_whale_delta_notional`（正=净买入）；`spot_fut_gap = xmk_whale_delta_notional_gap_s_minus_f`；`bias = sign(net_delta_notional)`<br>⚠️ **不用 buy_count/sell_count**（易被小单数量误导），用 notional 反映真实资金方向 |
| `vpin[tf]` | `vpin.by_window[tf]` | `{"vpin": 0.xx, "elevated": bool}`（elevated = vpin > 0.5；具体阈值从源数据校准） |
| `funding[tf]` | `funding_rate.by_window[tf]` | `{"current_rate": 0.0001, "rate_sign": +1\|−1\|0, "trend_sign": +1\|−1\|0}`（trend_sign = sign of last change in `.changes`） |

> whale_trades 和 vpin 的 `by_window[tf]` 具体字段名需对照实际 scan 输出确认（scan.rs 目前用 `insert_full_indicator` 不过滤）。实现时先 `println!` 一条数据确认字段名后再写死。

**完整输出格式**：

```json
{
  "ts_bucket": "2026-03-16T04:00:00+00:00",
  "symbol": "ETHUSDT",
  "current_price": 2171.5,

  "by_timeframe": {
    "15m": {
      "structure": {
        "swing_high": 2201.3, "swing_low": 2155.2,
        "level_book": {
          "support":    [{"price": 2165.5, "strength": 3.4, "sources": ["pvs_val", "rvwap_minus1"]}],
          "resistance": [{"price": 2177.0, "strength": 2.9, "sources": ["rvwap_plus1", "pvs_vah"]}]
        }
      },
      "trend": {
        "ema_regime": "bear",
        "close_vs_ema21_pct": -0.12,       // 15m: 来自 ema_trend_regime.ema_21
        "close_vs_ema100_htf_pct": null,   // 15m 无此值
        "close_vs_vah_pct": -0.16,
        "value_area_position": "inside_va"
      },
      "flow": {
        "cvd":               {"delta_sign": 1, "relative_delta": 0.23, "slope_sign": 1, "lead": "futures", "bias": "bullish", "last_ts": "..."},
        "absorption":        {"bullish_count": 0, "bearish_count": 2, "bias": "bearish", "bullish_events": [], "bearish_events": [...]},
        "buying_exhaustion": {"bullish_count": 0, "bearish_count": 1, "bias": "bearish", "bullish_events": [], "bearish_events": [...]},
        "selling_exhaustion":{"bullish_count": 2, "bearish_count": 0, "bias": "bullish", "bullish_events": [...], "bearish_events": []},
        "divergence":        {"bullish_count": 1, "bearish_count": 0, "bias": "bullish", "bullish_events": [...], "bearish_events": []},
        "whale":             {"net_delta_notional": -12500000, "spot_fut_gap": -3200000, "bias": "bearish"},
        "vpin":              {"vpin": 0.62, "elevated": true},
        "funding":           {"current_rate": 0.0001, "rate_sign": 1, "trend_sign": 1}
      },
      "volatility": {
        "atr14": 12.4, "atr14_pct": 0.57
      }
    },
    "4h": { "structure": {...}, "trend": {...}, "flow": {...}, "volatility": {...} },
    "1d": { "structure": {...}, "trend": {...}, "flow": {...}, "volatility": {...} }
  },

  "indicators": {
    // 保留顶层：空间型/实时型，无法按 timeframe 规整
    "footprint":             { ... },  // 价格-成交量空间结构，v3.1 split_select
    "orderbook_depth":       { ... },  // 实时盘口
    "tpo_market_profile":    { ... },  // 市场轮廓（tpo_poc/vah/val/by_session）
    "rvwap_sigma_bands":     { ... },  // RVWAP 偏差带（by_window）
    "fvg":                   { ... },  // 公允价值缺口（by_window）
    "avwap":                 { ... },  // 锚定 VWAP（series_by_window，v3.1 截断到 3 条）
    "price_volume_structure":{ ... },  // PVS poc/vah/val/hvn（已被 level_book 引用，保留完整数据）
    "ema_trend_regime":      { ... }   // regime 原始 series 保留（by_timeframe 仅取 [0]/first() 即最新值，此处可供深度分析）
    // whale_trades、vpin、funding_rate 的 by_window 原始数据被 by_timeframe.flow 摘要替代，不再保留顶层
  }
}
```

> **removed from indicators**：`kline_history`（→ kline_derived）、`cvd_pack`（→ cvd_summary）、`absorption`/`bullish_absorption`/`bearish_absorption`（→ by_timeframe.flow.absorption）、`buying_exhaustion`/`selling_exhaustion`/`divergence`/`bullish_initiation`/`bearish_initiation`（→ by_timeframe.flow 各自 timeframe_summary）、`whale_trades`/`vpin`/`funding_rate`（→ by_timeframe.flow 摘要）。

**实现位置**：新增 `build_by_timeframe(kline_derived, cvd_summary, absorption_summary, ema_regime, level_book, flow_summaries)` 函数，组装后注入根对象。

**预估体积增量**：3 timeframes × 4 sections × ~300B = **+3.6KB**（含新增 whale/vpin/funding 摘要）

---

### 4.5 footprint — 仅保留离现价最近的上下各 1-2 个 stack/cluster

**问题**：当前 footprint 是最大体积指标（实测 64.2KB）。v3.1 split_select 后降至 ~10KB（≤18条/字段）。但 Stage1 只需要"现价附近的价格墙在哪、强度多大"，不需要更远处的历史 stack。

**v4 规则**：每窗口每字段（buy_stacks/sell_stacks/buy_imb_clusters/sell_imb_clusters）只保留：
- 下方最近 **2 条**（end_price 离现价最近的 2 条 stack；对 clusters 用 p 字段）
- 上方最近 **2 条**（start_price 离现价最近的 2 条 stack）
- 跨越现价的 stack **全保留**（通常 0-1 条）

每条保留字段：`start_price`/`end_price`（stacks）或 `p`（clusters）+ `length`/`n`（强度）+ `dist`（预计算的 `|price - current_price|`，方便 LLM 直接读距离）。

```json
"footprint": {
  "15m": {
    "buy_stacks": {
      "above": [{"start_price": 2195.0, "end_price": 2198.0, "length": 4.2, "dist": 23.5}],
      "below": [{"start_price": 2160.0, "end_price": 2165.0, "length": 6.1, "dist": 9.5},
                {"start_price": 2150.0, "end_price": 2155.0, "length": 3.8, "dist": 19.5}],
      "cross":  []
    },
    "buy_imb_clusters": {
      "above": [{"p": 2192.0, "n": 8, "dist": 17.5}],
      "below": [{"p": 2162.0, "n": 12, "dist": 12.5}]
    }
  },
  "4h": { ... },
  "1d": { ... }
}
```

```rust
/// For buy_stacks / sell_stacks — includes `cross` group (stacks spanning current_price).
/// Stack spans [start_price, end_price]; dist uses the closer boundary to current_price.
fn footprint_nearest_stacks(items: &[Value], current_price: f64, n: usize) -> Value {
    let span_dist = |item: &Value| -> f64 {
        let start = item.get("start_price").and_then(Value::as_f64).unwrap_or(f64::INFINITY);
        let end   = item.get("end_price").and_then(Value::as_f64).unwrap_or(f64::INFINITY);
        (start - current_price).abs().min((end - current_price).abs())
    };
    let is_cross = |item: &Value| -> bool {
        let start = item.get("start_price").and_then(Value::as_f64).unwrap_or(f64::INFINITY);
        let end   = item.get("end_price").and_then(Value::as_f64).unwrap_or(f64::NEG_INFINITY);
        start <= current_price && current_price <= end
    };
    let is_above = |item: &Value| -> bool {
        item.get("start_price").and_then(Value::as_f64).map(|p| p > current_price).unwrap_or(false)
    };

    let to_compact = |item: &Value| -> Value {
        let mut m = item.as_object().unwrap().clone();
        m.insert("dist".to_string(), json!(round2(span_dist(item))));
        for k in &["ts", "window", "bar_count"] { m.remove(*k); }
        Value::Object(m)
    };

    let mut above: Vec<_> = items.iter().filter(|i| !is_cross(i) &&  is_above(i)).collect();
    let mut below: Vec<_> = items.iter().filter(|i| !is_cross(i) && !is_above(i)).collect();
    let     cross: Vec<_> = items.iter().filter(|i|  is_cross(i)).collect();
    above.sort_by(|a, b| span_dist(a).partial_cmp(&span_dist(b)).unwrap_or(Ordering::Equal));
    below.sort_by(|a, b| span_dist(a).partial_cmp(&span_dist(b)).unwrap_or(Ordering::Equal));

    json!({
        "above": above.into_iter().take(n).map(|i| to_compact(i)).collect::<Vec<_>>(),
        "below": below.into_iter().take(n).map(|i| to_compact(i)).collect::<Vec<_>>(),
        "cross": cross.into_iter().map(|i| to_compact(i)).collect::<Vec<_>>(),
    })
}

/// For buy_imb_clusters / sell_imb_clusters — single price point `p`, no cross possible.
fn footprint_nearest_clusters(items: &[Value], current_price: f64, n: usize) -> Value {
    let dist = |item: &Value| -> f64 {
        item.get("p").and_then(Value::as_f64)
            .map(|p| (p - current_price).abs())
            .unwrap_or(f64::INFINITY)
    };
    let is_above = |item: &Value| -> bool {
        item.get("p").and_then(Value::as_f64).map(|p| p > current_price).unwrap_or(false)
    };
    let to_compact = |item: &Value| -> Value {
        let mut m = item.as_object().unwrap().clone();
        m.insert("dist".to_string(), json!(round2(dist(item))));
        for k in &["ts", "window", "bar_count"] { m.remove(*k); }
        Value::Object(m)
    };

    let mut above: Vec<_> = items.iter().filter(|i|  is_above(i)).collect();
    let mut below: Vec<_> = items.iter().filter(|i| !is_above(i)).collect();
    above.sort_by(|a, b| dist(a).partial_cmp(&dist(b)).unwrap_or(Ordering::Equal));
    below.sort_by(|a, b| dist(a).partial_cmp(&dist(b)).unwrap_or(Ordering::Equal));

    json!({
        "above": above.into_iter().take(n).map(|i| to_compact(i)).collect::<Vec<_>>(),
        "below": below.into_iter().take(n).map(|i| to_compact(i)).collect::<Vec<_>>(),
    })
}
```

**预估节省**：~10KB → **~2KB**（每窗口 4 字段 × 2-4 条）= **−8KB vs v3.1 split_select**

---

### 4.6 timeframe_evidence — 中性证据层（**必选**）

**目标**：`scan` 负责把噪音压缩成 LLM 易读的**证据层**，但**不直接给最终方向结论**。Stage1 仍然由 LLM 自己判断 `15m / 4h / 1d` 的方向、区间、风险和机会。  
换句话说，算法在这里做的是“整理证据”，不是“替模型下判决”。

**设计原则**：

- `timeframe_evidence` 只输出**结构化证据**，不输出 `Bullish / Bearish / Sideways` 这类最终标签。
- `ranges` 直接服务于“区间”目标；`display_range` 仅作为 `ranges.local_structure_range` 的兼容别名；`balance_scores` 只是证据平衡，不是答案。
- `breakout_above / breakdown_below`、`risk_opportunity`、`clearance_atr` 用于让 LLM 看到风险/机会，不强制导出方向。
- LLM 必须继续结合 `by_timeframe` 原始特征自行判断，而不是复述这一层。

**字段语义定义**：

| 字段 | 语义 | 来源 |
|------|------|------|
| `primary_support` | 最近结构支撑（最近括号，不是最终展示区间） | `level_book[tf].support[0].price` |
| `primary_resistance` | 最近结构阻力（最近括号，不是最终展示区间） | `level_book[tf].resistance[0].price` |
| `ranges.micro_range` | 最近微结构括号 | 直接对应 `primary_support/primary_resistance`，适合表达压缩/贴边状态 |
| `ranges.local_structure_range` | 当前 timeframe 的局部结构区间 | 在最近 bracket 过窄时，可扩到第二层更强结构；默认对应原 `display_range` |
| `ranges.session_value_area_range` | 当前 auction/value-area 上下文区间 | 优先来自 TPO session value area；缺失时回退到 PVS value area；不保证一定包住现价 |
| `display_range` | `ranges.local_structure_range` 的兼容别名 | 为兼容旧 prompt/下游读取而保留 |
| `ranges.*.range_role` | 区间角色标签 | `micro_range="compression"`、`local_structure_range="default_bracket"`、`session_value_area_range="context"` |
| `breakout_above` | 当前 bracket 上方的延伸参考位 | `swing_high` |
| `breakdown_below` | 当前 bracket 下方的延伸参考位 | `swing_low` |
| `balance_scores.trend_balance_score` | 趋势证据平衡分数 | `ema_regime + close_vs_ema + value_area_position` 压缩到 `[-1,+1]` |
| `balance_scores.flow_balance_score` | flow 证据平衡分数 | `cvd + weighted events + whale + funding` 压缩到 `[-1,+1]` |
| `balance_scores.structure_balance_score` | 结构位置证据平衡分数 | 现价在区间中的位置 + 锚点强弱 |
| `balance_scores.evidence_balance_score` | 综合证据平衡分数 | 三类证据加权平均 |
| `balance_scores.conflict_score` | 证据冲突度 | `0-1`，越高代表 trend/flow/structure 越矛盾 |
| `balance_scores.compression_score` | 震荡/压缩证据强度 | `0-1`，越高越接近 range/compression |
| `risk_opportunity.upside_opportunity_score` | 向上机会强度 | `0-1`，靠近上沿且上行动能更强时更高 |
| `risk_opportunity.downside_risk_score` | 向下风险强度 | `0-1`，靠近下沿且下行动能更强时更高 |
| `clearance_atr` | 到关键价位的 ATR 归一化距离 | `to_support / to_resistance / to_breakdown / to_breakout` |
| `signal_snapshot` | 一组轻量快照字段 | `ema_regime`、`value_area_position`、`cvd_bias` 等 |

> `primary_support/primary_resistance` 是“最近结构括号”；`ranges.micro_range` 用于表达短线压缩，`ranges.local_structure_range` 是默认结构区间，`ranges.session_value_area_range` 是更宽的 auction/value-area 上下文。`display_range` 仅是 `ranges.local_structure_range` 的兼容别名。

**输出格式**：

```json
"timeframe_evidence": {
  "15m": {
    "primary_support": 2165.5,
    "primary_resistance": 2177.0,
    "ranges": {
      "micro_range": {
        "support": 2165.5,
        "resistance": 2177.0,
        "mode": "micro_bracket",
        "range_role": "compression",
        "range_width_atr": 0.39,
        "position_in_range_pct": 0.81,
        "current_price_location": "inside",
        "quality": "compressed",
        "support_strength": 4.1,
        "support_source_count": 4,
        "resistance_strength": 1.8,
        "resistance_source_count": 2
      },
      "local_structure_range": {
        "support": 2165.5,
        "resistance": 2183.4,
        "mode": "expanded_resistance",
        "range_role": "default_bracket",
        "range_width_atr": 0.61,
        "position_in_range_pct": 0.42,
        "current_price_location": "inside",
        "quality": "clean",
        "support_strength": 4.8,
        "support_source_count": 4,
        "resistance_strength": 5.3,
        "resistance_source_count": 5
      },
      "session_value_area_range": {
        "support": 2158.8,
        "resistance": 2186.2,
        "mode": "session_value_area",
        "range_role": "context",
        "range_width_atr": 0.93,
        "position_in_range_pct": 0.57,
        "current_price_location": "inside",
        "quality": "clean",
        "source_window": "4h",
        "source_kind": "tpo_value_area"
      }
    },
    "display_range": {
      "support": 2165.5,
      "resistance": 2183.4,
      "mode": "expanded_resistance",
      "range_width_atr": 0.61,
      "position_in_range_pct": 0.42,
      "quality": "clean",
      "support_strength": 4.8,
      "support_source_count": 4,
      "resistance_strength": 5.3,
      "resistance_source_count": 5
    },
    "breakout_above": 2201.3,
    "breakdown_below": 2155.2,
    "balance_scores": {
      "trend_balance_score": -0.38,
      "flow_balance_score": -0.24,
      "structure_balance_score": -0.17,
      "evidence_balance_score": -0.25,
      "conflict_score": 0.22,
      "compression_score": 0.25
    },
    "risk_opportunity": {
      "upside_opportunity_score": 0.31,
      "downside_risk_score": 0.56
    },
    "clearance_atr": {
      "to_support": 0.24,
      "to_resistance": 0.58,
      "to_breakdown": 0.92,
      "to_breakout": 1.84
    },
    "signal_snapshot": {
      "ema_regime": "bear",
      "cvd_bias": "bearish",
      "absorption_bias": "bearish",
      "initiation_bias": "neutral",
      "whale_bias": "bearish",
      "value_area_position": "below_val",
      "funding_rate_sign": -1,
      "vpin_elevated": true
    },
    "temporal_stability": {
      "samples_used": 3,
      "trend_balance_consensus": "stable",
      "flow_balance_consensus": "mixed",
      "evidence_balance_consensus": "mixed",
      "range_stability": "stable",
      "balance_flip_count": 1,
      "support_drift_atr": 0.18,
      "resistance_drift_atr": 0.24,
      "evidence_balance_drift": 0.16
    }
  },
  "4h": { ... },
  "1d": { ... }
}
```

**`balance_scores` 解释**：

| 字段 | 范围 | 含义 |
|------|------|------|
| `trend_balance_score` / `flow_balance_score` / `structure_balance_score` / `evidence_balance_score` | `[-1, +1]` | 负值 = 偏空证据，正值 = 偏多证据，绝对值越大代表证据越明确 |
| `conflict_score` | `[0, 1]` | 越高代表三类证据之间越不一致 |
| `compression_score` | `[0, 1]` | 越高代表越接近震荡/压缩而非趋势推进 |

**实现约定**：

- `flow` 事件类摘要不再按简单计数做 bias，而是使用**时间衰减 + 现价距离衰减**后的加权分数。
- `ranges.micro_range` 保留最近括号；`ranges.local_structure_range` 允许在“最近 bracket 过窄”时扩展到第二层更强结构，但不改写 `primary_support/primary_resistance`。
- `ranges.session_value_area_range` 提供 auction/value-area 上下文区间，优先取 TPO session value area，缺失时回退到 PVS value area；若 `current_price_location != "inside"`，LLM 应把它视为上下文区而不是最终输出括号。
- 允许新增顶层 `multi_timeframe_evidence`，只提供跨周期**关系型证据**，不直接给“主导方向”。

---

### 4.7 temporal_stability — 最近 2-3 次证据一致性（推荐）

**目标**：很多“不稳”不是模型推理错误，而是证据本身在快速切换。`temporal_stability` 的任务是告诉 LLM：当前这组证据是稳定延续，还是刚发生重组。

**设计原则**：

- 只比较最近 `2-3` 个同 symbol 的 `scan` 产出物。
- 只比较 `timeframe_evidence` 的轻量字段；不重扫原始大指标。
- 只做“稳定性提示”，不输出最终方向标签。

**数据来源**：

- 当前样本 + 同 symbol 最近 `1-2` 个已生成 `scan` 文件。
- 优先读取新 schema 的 `timeframe_evidence`；若历史文件仍是旧 schema，可兼容读取 `timeframe_decision` 做过渡。

**输出格式**：

```json
"temporal_stability": {
  "samples_used": 3,
  "trend_balance_consensus": "stable",
  "flow_balance_consensus": "mixed",
  "evidence_balance_consensus": "mixed",
  "range_stability": "stable",
  "balance_flip_count": 1,
  "support_drift_atr": 0.18,
  "resistance_drift_atr": 0.24,
  "evidence_balance_drift": 0.16
}
```

**字段定义**：

| 字段 | 含义 | 计算 |
|------|------|------|
| `samples_used` | 实际参与比较的样本数 | `2` 或 `3` |
| `trend_balance_consensus` | 最近几次 trend 证据是否一致 | 对 `trend_balance_score` 分桶后做一致性比较 |
| `flow_balance_consensus` | 最近几次 flow 证据是否一致 | 对 `flow_balance_score` 分桶后做一致性比较 |
| `evidence_balance_consensus` | 最近几次综合证据是否一致 | 对 `evidence_balance_score` 分桶后做一致性比较 |
| `range_stability` | 区间边界是否明显漂移 | `max(support_drift_atr, resistance_drift_atr)` 分档 |
| `balance_flip_count` | 相邻样本间综合证据桶切换次数 | 2 个样本范围 `0-1`；3 个样本范围 `0-2` |
| `support_drift_atr` | 当前 support 相对最旧样本的漂移 | `abs(current_support - oldest_support) / current_atr14` |
| `resistance_drift_atr` | 当前 resistance 相对最旧样本的漂移 | `abs(current_resistance - oldest_resistance) / current_atr14` |
| `evidence_balance_drift` | 当前综合证据分数相对最旧样本的漂移 | `abs(current_evidence_balance - oldest_evidence_balance)` |

> LLM 使用建议：`evidence_balance_consensus = stable` 且 `range_stability = stable` 时，当前证据更像“延续”；若 `flow_balance_consensus` 或 `evidence_balance_consensus` 是 `mixed/unstable`，则应更强调“证据冲突”或“等待确认”。

**实现位置**：新增 `load_recent_scan_evidence(symbol, current_ts_bucket)` 和 `build_temporal_stability(history, current_evidence)`；在 `build_timeframe_evidence` 末尾尝试注入。若历史不足 2 个样本，则省略该字段。

**体积**：3 timeframes × ~450B = **+1.4KB**（增量很小，但能显著提升“更稳”）

---

## 二、v4 估算汇总（更新含 §4.5-4.7）

| 变更 | 方向 | 预估 minified |
|------|------|-------------|
| v3.1 基线（实测 206.5KB） | — | ~206KB |
| §4.1 absorption_summary（删 3 套 events） | −29.7KB | ~176KB |
| §4.2 cvd_summary（删 series） | −19.8KB | ~156KB |
| §4.3 kline_derived（删 bars） | −5.5KB | ~150KB |
| §4.4 by_timeframe 新增 | +3.6KB | ~154KB |
| 削减 exhaustion/divergence/initiation events（各 ~14-16KB） | −44KB | ~110KB |
| §4.5 footprint nearest-2（vs v3.1 split_select） | −8KB | ~102KB |
| §4.6 timeframe_evidence（balance_scores/display_range/risk_opportunity）新增 | +2.0KB | ~104.0KB |
| §4.7 temporal_stability（证据一致性）新增 | +1.4KB | ~105.4KB |
| **v4 合计** | | **~105KB** |

> `buying/selling_exhaustion + divergence` 的原始 `recent_7d.events` 合计约 19KB（v3.1 裁剪后：exhaustion 各 8KB，divergence 7.5KB，去掉 v3.1 timeframe_summary 的增量约 3.6KB）。若移入 by_timeframe 并删除顶层，节省 ~19KB 但失去事件的完整列表——Stage1 仅需方向和强度，timeframe_summary 已足够，**建议删除**。

---

## 三、第一目标覆盖对照

| 能力 | v3 / v3.1 | v4 方案 |
|------|-----------|---------|
| 知道每个 timeframe 的趋势方向 | ✗ 需要从 kline bars 自推 | ✅ `by_timeframe[tf].trend.* + flow.* + timeframe_evidence[tf].balance_scores` 提供完整证据，LLM 自行判断 |
| 知道价格相对结构的位置 | ✗ 需要跨 pvs/rvwap/avwap 拼 | ✅ `close_vs_vah_pct` + `value_area_position`（离散，直接可用） |
| 知道 flow 偏向 | ✗ 需要从 20 条 absorption 事件推断 | ✅ `by_timeframe[tf].flow.absorption.bias` 直接给 |
| 知道 CVD 趋势 | ✗ 需要从 30 条 series 推断 slope | ✅ `cvd.slope_sign` + `cvd.bias` 直接给（从 series 预计算） |
| 知道 whale/vpin/funding 倾向 | ✗ 需要在顶层原始数据里找 | ✅ `by_timeframe[tf].flow.whale/vpin/funding` 摘要直接给 |
| 知道支撑阻力在哪 | ✗ 需要跨 6 个指标聚类 | ✅ `level_book`（v3.1 §3.7）直接给 |
| 知道波动率基准 | ✗ 无或分散 | ✅ `volatility.atr14` / `atr14_pct` 直接给（rank_30d 留待后续） |
| 跨 timeframe 一致性阅读 | ✗ 需要在 15+ 指标间反复跳转 | ✅ `by_timeframe.15m` / `.4h` / `.1d` 固定骨架 |
| 方向判断仍由 LLM 完成，但证据是否更均衡 | ✗ LLM 容易遗漏或被噪音淹没 | ✅ `timeframe_evidence[tf].balance_scores` 把 trend/flow/structure 压缩成可解释证据 |
| 当前区间边界是否直接可读 | ✗ LLM 需要自己从多个结构字段选区间 | ✅ `timeframe_evidence[tf].display_range.support/resistance` 直接给 |
| 能否看到风险/机会 | ✗ 需要自己算离 breakout / breakdown 多远 | ✅ `risk_opportunity + clearance_atr + breakout_above/breakdown_below` |
| 能否知道证据是否互相打架 | ✗ 容易只看到单边信号 | ✅ `balance_scores.conflict_score + temporal_stability`（§4.6-4.7） |
| 能否看到跨周期关系 | ✗ 需要模型自己把三个 tf 再拼一层 | ✅ `multi_timeframe_evidence` 只给关系，不给最终结论 |
| footprint 远端 stack 噪音 | ✗ 几百条 stack 掩盖关键价格墙 | ✅ 上下各 2 条最近 stack/cluster + dist（§4.5） |

---

## 四、枚举值规范（统一表）

所有字段只允许下表中列出的取值，实现时以此为准，禁止出现同义别名（如 `bull`/`bullish_trend`/`buy_dominant` 均不合法，只允许一种）。

| 字段 | 合法取值 | 说明 |
|------|---------|------|
| `by_timeframe[tf].trend.ema_regime` | `"bull"` \| `"bear"` \| `"ranging"` | 来自 ema_trend_regime series；`"ranging"` 表示震荡/无趋势 |
| `timeframe_evidence[tf].primary_support` / `primary_resistance` | 数值 | 最近结构括号，不等同于最终输出区间 |
| `timeframe_evidence[tf].ranges.micro_range.mode` | `"micro_bracket"` | 最近微结构括号 |
| `timeframe_evidence[tf].ranges.local_structure_range.mode` | `"primary"` \| `"expanded_resistance"` \| `"expanded_support"` | 默认给 LLM 的局部结构区间 |
| `timeframe_evidence[tf].ranges.session_value_area_range.mode` | `"session_value_area"` | 当前 auction/value-area 上下文区间 |
| `timeframe_evidence[tf].ranges.*.current_price_location` | `"inside"` \| `"above"` \| `"below"` | 现价相对该区间的位置；session_value_area_range 可能不包住现价 |
| `timeframe_evidence[tf].ranges.*.range_role` | `"compression"` \| `"default_bracket"` \| `"context"` | 区间角色标签，帮助 LLM 理解该区间的用途 |
| `timeframe_evidence[tf].display_range.mode` | `"primary"` \| `"expanded_resistance"` \| `"expanded_support"` | 给 LLM 展示的结构区间模式 |
| `timeframe_evidence[tf].display_range.quality` | `"clean"` \| `"compressed"` \| `"fragile"` | 兼容别名；等同于 `ranges.local_structure_range.quality` |
| `timeframe_evidence[tf].ranges.*.quality` | `"clean"` \| `"compressed"` \| `"fragile"` | 区间质量标签 |
| `timeframe_evidence[tf].balance_scores.trend_balance_score/flow_balance_score/structure_balance_score/evidence_balance_score` | `-1.0 ~ +1.0` | 负值=偏空证据，正值=偏多证据 |
| `timeframe_evidence[tf].balance_scores.conflict_score` | `0.0 ~ 1.0` | 越高表示证据越冲突 |
| `timeframe_evidence[tf].balance_scores.compression_score` | `0.0 ~ 1.0` | 越高表示 range/compression 证据越强 |
| `timeframe_evidence[tf].risk_opportunity.upside_opportunity_score` / `downside_risk_score` | `0.0 ~ 1.0` | 风险/机会摘要，不是最终方向 |
| `timeframe_evidence[tf].signal_snapshot.ema_regime` | `"bull"` \| `"bear"` \| `"ranging"` | 与 by_timeframe 保持一致 |
| 所有 `*.bias` 字段（cvd.bias, absorption.bias, buying_exhaustion.bias, selling_exhaustion.bias, divergence.bias, whale.bias） | `"bullish"` \| `"bearish"` \| `"neutral"` | 全小写；whale.bias 亦使用此三值 |
| `by_timeframe[tf].trend.value_area_position` | `"above_vah"` \| `"inside_va"` \| `"below_val"` | 来自 kline_derived；与 timeframe_evidence.signal_snapshot 一致 |
| `by_timeframe[tf].flow.funding.rate_sign` | `1` \| `-1` \| `0` | 整数；正数=多头付资金费 |
| `by_timeframe[tf].flow.vpin.elevated` | `true` \| `false` | 布尔 |
| `timeframe_evidence[tf].temporal_stability.trend_balance_consensus` / `flow_balance_consensus` / `evidence_balance_consensus` | `"stable"` \| `"mixed"` \| `"unstable"` | 仅在 `samples_used >= 2` 时出现 |
| `timeframe_evidence[tf].temporal_stability.range_stability` | `"stable"` \| `"shifting"` \| `"unstable"` | 仅在 `samples_used >= 2` 时出现 |

**与 scan prompt 的输出映射**：

| scan 输入字段 | Stage1 输出字段 |
|------|------|
| `timeframe_evidence[tf].ranges.local_structure_range.support`（必要时参考 `ranges.session_value_area_range.support`） | 顶层 `{tf}.range.support` |
| `timeframe_evidence[tf].ranges.local_structure_range.resistance`（必要时参考 `ranges.session_value_area_range.resistance`） | 顶层 `{tf}.range.resistance` |
| `by_timeframe[tf].trend.* + by_timeframe[tf].flow.* + timeframe_evidence[tf].balance_scores` | 顶层 `{tf}.trend`（由 LLM 自行判断） |
| `timeframe_evidence[tf].balance_scores.conflict_score + temporal_stability + raw feature contradictions` | 顶层 `{tf}.signal_agreement`（由 LLM 自行判断） |
| `by_timeframe[tf].trend.* + flow.* + signal_snapshot` | 顶层 `{tf}.supporting_signals` / `conflicting_signals`（由 LLM 归纳） |
| `risk_opportunity + clearance_atr + breakout_above/breakdown_below` | 顶层 `{tf}.opportunity` / `risk`（由 LLM 归纳） |
| `multi_timeframe_evidence + breakout_above/breakdown_below + risk_opportunity` | `market_narrative` |

说明：
`timeframe_evidence[tf].ranges.*`、`current_price_location`、`range_role` 仅用于帮助 LLM 选择最终顶层 `{tf}.range`，不再输出额外的 `range_basis` / `range_role_used` 给下游。下游只消费最终区间、信号依据、风险和机会。

---

## 五、变更文件清单

仅需修改 **1 个文件**：[`systems/llm/src/llm/filter/scan.rs`](systems/llm/src/llm/filter/scan.rs)

| 位置 | 变更内容 |
|------|---------|
| `filter_event_indicator` for absorption | 删除 `recent_7d.events`，仅保留 timeframe_summary；重命名为 absorption_summary（§4.1） |
| `EVENT_INDICATOR_RULES` | 删除 `bullish_absorption`、`bearish_absorption` 两条规则（§4.1） |
| `filter_cvd_pack` → `build_cvd_summary` | 从 series 计算 6 个标量，丢弃 series（§4.2） |
| `filter_kline_history` → `build_kline_derived` | 从 bars 计算 8 个派生特征，丢弃 bars；接受 pvs_source 参数（§4.3） |
| `filter_event_indicator` for buying/selling_exhaustion、divergence | 同 absorption，删除 recent_7d events（§4.4） |
| `build_scan_root` | 新增 `build_by_timeframe(...)` 调用，注入根对象（§4.4） |
| `filter_footprint` | stacks 字段改为 `footprint_nearest_stacks(items, current_price, 2)`（输出 above/below/cross）；clusters 字段改为 `footprint_nearest_clusters(items, current_price, 2)`（输出 above/below）（§4.5） |
| `build_scan_root` | 在 `build_by_timeframe` 之后调用 `build_timeframe_evidence`，注入根对象（§4.6） |
| `build_scan_root` | 新增 `build_multi_timeframe_evidence`，只输出跨周期关系型证据（§4.6） |
| `build_timeframe_evidence` | 输出 `display_range / balance_scores / risk_opportunity / clearance_atr / signal_snapshot`，不输出最终方向标签（§4.6） |
| `build_scan_root` 或 `build_timeframe_evidence` | 若存在最近 2-3 个 scan 输出，则注入 `temporal_stability`（§4.7）；历史不足则省略 |
| 新增辅助函数 | `build_cvd_summary`、`build_kline_derived`（接受 `pvs_by_window` + `ema_regime_payload`，不再需要 `compute_ema`）、`build_by_timeframe`、`build_timeframe_evidence`、`build_multi_timeframe_evidence`、`build_temporal_stability`、`load_recent_scan_evidence`、`footprint_nearest_stacks`、`footprint_nearest_clusters`、`extract_ema_regime_current` |
| 删除辅助函数 | `mixed_select_stacks`、`mixed_select_clusters`（v3.1 已替换为 split_select）、`filter_whale_trades`/`filter_vpin`/`filter_funding_rate` 顶层输出路径（改为 by_timeframe 摘要） |
| 说明 | `ema_trend_regime` 保留 `filter_ema_trend_regime` 输出 `regime_series_15m`；`build_by_timeframe` 从 `regime_series_15m.first()`（即 `[0]`，newest-first）取当前 regime |

---

## 六、验证方法

```python
import json, glob

path = sorted(glob.glob("systems/llm/temp_model_input/*_scan_*.json"))[-1]
with open(path) as f:
    data = json.load(f)

raw = json.dumps(data, separators=(',', ':'))
print(f"minified: {len(raw) / 1024:.1f}KB")  # 目标 < 130KB

current_price = data.get('current_price') or (
    data['indicators'].get('avwap', {}).get('payload', {}).get('fut_last_price')
)
assert current_price, "current_price missing from root"

by_tf = data['by_timeframe']
assert set(by_tf.keys()) >= {'15m', '4h', '1d'}, "by_timeframe missing windows"

for tf in ['15m', '4h', '1d']:
    tf_data = by_tf[tf]

    # structure
    s = tf_data['structure']
    assert s['swing_low'] < s['swing_high'], f"{tf} swing_low >= swing_high"
    lb = s['level_book']
    for sup in lb.get('support', []):
        assert sup['price'] < current_price, f"{tf} support price {sup['price']} >= current {current_price}"
    for res in lb.get('resistance', []):
        assert res['price'] > current_price, f"{tf} resistance price {res['price']} <= current {current_price}"

    # trend — ema_regime 是字符串，来自 ema_trend_regime
    t = tf_data['trend']
    assert t.get('ema_regime') is not None, f"{tf}.trend.ema_regime missing"
    # close_vs_ema21_pct: 仅 15m 有值，4h/1d 应为 null
    if tf == '15m':
        assert t.get('close_vs_ema21_pct') is not None, "15m.trend.close_vs_ema21_pct missing"
    else:
        assert t.get('close_vs_ema21_pct') is None, f"{tf}.trend.close_vs_ema21_pct should be null"
    # close_vs_ema100_htf_pct: 仅 4h/1d 有值，15m 应为 null
    if tf in ('4h', '1d'):
        assert t.get('close_vs_ema100_htf_pct') is not None, f"{tf}.trend.close_vs_ema100_htf_pct missing"
    else:
        assert t.get('close_vs_ema100_htf_pct') is None, "15m.trend.close_vs_ema100_htf_pct should be null"
    # value_area_position 可为 null（pvs 无效时）
    vap = t.get('value_area_position')
    if vap is not None:
        assert vap in ('above_vah', 'inside_va', 'below_val'), f"{tf}.trend.value_area_position invalid: {vap}"

    # flow
    fl = tf_data['flow']
    for flow_key in ('cvd', 'absorption', 'buying_exhaustion', 'selling_exhaustion', 'divergence',
                     'whale', 'vpin', 'funding'):
        assert flow_key in fl, f"{tf}.flow.{flow_key} missing"

    cvd = fl['cvd']
    assert cvd['delta_sign'] in (1, -1, 0), f"{tf} cvd.delta_sign invalid"
    assert cvd['bias'] in ('bullish', 'bearish', 'neutral'), f"{tf} cvd.bias invalid"
    assert cvd['lead'] in ('futures', 'spot', 'balanced'), f"{tf} cvd.lead invalid"

    for flow_key in ('absorption', 'buying_exhaustion', 'selling_exhaustion', 'divergence'):
        fw = fl[flow_key]
        assert fw['bias'] in ('bullish', 'bearish', 'neutral'), f"{tf}.{flow_key}.bias invalid"
        for direction in ('bullish_events', 'bearish_events'):
            events = fw.get(direction, [])
            assert isinstance(events, list) and len(events) <= 3, \
                f"{tf}.{flow_key}.{direction} has {len(events)} items (max 3)"
            for ev in events:
                for k in ('confirm_ts', 'pivot_price', 'score', 'type'):
                    assert k in ev, f"{tf}.{flow_key}.{direction} event missing {k}"

    assert fl['whale']['bias'] in ('bullish', 'bearish', 'neutral')
    assert 0 <= fl['vpin']['vpin'] <= 1, f"{tf} vpin.vpin out of [0,1]"
    assert fl['funding']['rate_sign'] in (1, -1, 0)

    # volatility
    v = tf_data['volatility']
    assert v['atr14'] > 0, f"{tf} atr14 <= 0"
    assert 0 < v['atr14_pct'] < 10, f"{tf} atr14_pct out of range: {v['atr14_pct']}"
    # atr14_pct_rank_30d 不在 v4 schema，不断言

# 嵌套窗口一致性：1d ATR >= 15m ATR (通常成立，宽松检查)
atr_15m = by_tf['15m']['volatility']['atr14']
atr_1d  = by_tf['1d']['volatility']['atr14']
assert atr_1d >= atr_15m * 0.5, f"1d ATR {atr_1d:.2f} implausibly small vs 15m ATR {atr_15m:.2f}"

# by_timeframe.flow 嵌套窗口：1d count >= 4h count >= 15m count
for flow_key in ('absorption', 'buying_exhaustion', 'selling_exhaustion', 'divergence'):
    for direction in ('bullish_count', 'bearish_count'):
        c15 = by_tf['15m']['flow'][flow_key][direction]
        c4h = by_tf['4h']['flow'][flow_key][direction]
        c1d = by_tf['1d']['flow'][flow_key][direction]
        assert c1d >= c4h >= c15, (
            f"{flow_key}.{direction} nested count violated: 1d={c1d}, 4h={c4h}, 15m={c15}"
        )

# timeframe_evidence 与 Stage1 scan prompt 对齐
te = data['timeframe_evidence']
for tf in ('15m', '4h', '1d'):
    ev = te[tf]
    assert isinstance(ev['primary_support'], (int, float))
    assert isinstance(ev['primary_resistance'], (int, float))
    assert ev['primary_support'] < ev['primary_resistance'], f"{tf} primary range invalid"
    assert ev['breakdown_below'] <= ev['primary_support'], f"{tf} breakdown_below above support"
    assert ev['breakout_above'] >= ev['primary_resistance'], f"{tf} breakout_above below resistance"

    dr = ev['display_range']
    assert dr['mode'] in ('primary', 'expanded_resistance', 'expanded_support')
    assert dr['quality'] in ('clean', 'compressed', 'fragile')
    assert dr['support'] < dr['resistance'], f"{tf} display range invalid"
    assert dr['range_width_atr'] >= 0
    assert 0 <= dr['position_in_range_pct'] <= 1

    ranges = ev['ranges']
    assert ranges['micro_range']['mode'] == 'micro_bracket'
    assert ranges['micro_range']['range_role'] == 'compression'
    assert ranges['local_structure_range']['mode'] in ('primary', 'expanded_resistance', 'expanded_support')
    assert ranges['local_structure_range']['range_role'] == 'default_bracket'
    assert ranges['session_value_area_range']['mode'] == 'session_value_area'
    assert ranges['session_value_area_range']['range_role'] == 'context'
    assert ranges['micro_range']['support'] < ranges['micro_range']['resistance']
    assert ranges['local_structure_range']['support'] == dr['support']
    assert ranges['local_structure_range']['resistance'] == dr['resistance']
    assert ranges['micro_range']['current_price_location'] in ('inside', 'above', 'below')
    assert ranges['local_structure_range']['current_price_location'] in ('inside', 'above', 'below')
    assert ranges['session_value_area_range']['current_price_location'] in ('inside', 'above', 'below')

    scores = ev['balance_scores']
    for key in ('trend_balance_score', 'flow_balance_score', 'structure_balance_score',
                'evidence_balance_score'):
        assert -1 <= scores[key] <= 1, f"{tf}.balance_scores.{key} out of range"
    assert 0 <= scores['conflict_score'] <= 1, f"{tf}.balance_scores.conflict_score out of range"
    assert 0 <= scores['compression_score'] <= 1, f"{tf}.balance_scores.compression_score out of range"

    risk_opportunity = ev['risk_opportunity']
    assert 0 <= risk_opportunity['upside_opportunity_score'] <= 1
    assert 0 <= risk_opportunity['downside_risk_score'] <= 1

    sig = ev['signal_snapshot']
    assert sig['ema_regime'] in ('bull', 'bear', 'ranging')
    assert sig['cvd_bias'] in ('bullish', 'bearish', 'neutral')
    assert sig['absorption_bias'] in ('bullish', 'bearish', 'neutral')
    assert sig['initiation_bias'] in ('bullish', 'bearish', 'neutral')
    assert sig['whale_bias'] in ('bullish', 'bearish', 'neutral')
    assert sig['value_area_position'] in ('above_vah', 'inside_va', 'below_val')
    assert sig['funding_rate_sign'] in (-1, 0, 1)
    assert isinstance(sig['vpin_elevated'], bool)

    if ev.get('temporal_stability') is not None:
        ts = ev['temporal_stability']
        assert ts['samples_used'] in (2, 3), f"{tf} temporal samples invalid"
        assert ts['trend_balance_consensus'] in ('stable', 'mixed', 'unstable')
        assert ts['flow_balance_consensus'] in ('stable', 'mixed', 'unstable')
        assert ts['evidence_balance_consensus'] in ('stable', 'mixed', 'unstable')
        assert ts['range_stability'] in ('stable', 'shifting', 'unstable')
        assert 0 <= ts['balance_flip_count'] <= 2
        assert ts['support_drift_atr'] >= 0
        assert ts['resistance_drift_atr'] >= 0
        assert ts['evidence_balance_drift'] >= 0

mte = data['multi_timeframe_evidence']
for key in ('15m', '4h', '1d'):
    assert -1 <= mte['balance_scores'][key] <= 1
for key in ('1d_4h', '4h_15m'):
    assert -1 <= mte['pair_coherence'][key] <= 1

# indicators 顶层：确认已删除的字段不存在
inds = data['indicators']
for removed in ('kline_history', 'cvd_pack', 'bullish_absorption', 'bearish_absorption',
                'buying_exhaustion', 'selling_exhaustion', 'divergence', 'absorption',
                'whale_trades', 'vpin', 'funding_rate'):
    assert removed not in inds, f"indicators.{removed} should have been removed in v4"

# footprint 仍在顶层 indicators
assert 'footprint' in inds, "footprint missing from indicators"
assert 'orderbook_depth' in inds, "orderbook_depth missing from indicators"

print("all v4 assertions passed")
```
