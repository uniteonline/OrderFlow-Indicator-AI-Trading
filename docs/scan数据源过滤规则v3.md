# Scan 数据源过滤规则 v3

**状态**：草案（待复核）
**适用文件**：`systems/llm/src/llm/filter/scan.rs`
**适用提示词**：`systems/llm/src/llm/prompt/scan/big_opportunity.txt`、`medium_large_opportunity.txt`
**数据来源**：`temp_indicator/{ts}_ETHUSDT.json`（经 `round_derived_fields` 四舍五入后的数据）
**基线体积**：v2 规则实测 scan 输出 **187.2KB（minified）**，373KB（pretty-printed，磁盘文件大小）
**目标体积**：< 115KB（minified）
**与 v2 差异**：
1. `footprint` stacks / clusters 改为**近价优先 + 强度补充**混合策略（近价 top N + 全局最强 top M，去重后输出），替代纯强度排序，避免远端旧结构霸屏
2. `bearish_absorption` / `bullish_absorption` 截断数 20 → **5**（保留各自最近 5 条，保证方向可见性，不全删）
3. `buying_exhaustion`、`selling_exhaustion`、`divergence` 事件截断数 20 → 10
4. `cvd_pack` series 每条删除 `cvd_7d_fut` / `cvd_7d_spot` 两个累积字段
5. `kline_history` 过滤阶段新增 `volatility_summary` 派生字段（由已有 OHLCV bars 计算，不新增数据源）

> **只为 Scan 服务**：以下规则仅针对 Stage 1 市场扫描提示词需求设计，不适用于 Entry / Management / Pending Order 阶段。
> 所有保留或新增的时间序列统一按时间 **newest-first** 输出。

---

## 一、设计原则

v3 完整继承 v2 设计原则，补充一条：

**原则 5（v3 新增）：子集指标不重复输出**
若指标 A 的数据集完全覆盖指标 B（B 是 A 按某维度过滤的子集，且 A 的每条事件上已有区分维度的字段），则删除 B，保留 A。

> **原则 5 在 absorption 家族的例外**：`absorption` 展示最近 20 条混合事件（多空交替），`bullish_absorption` / `bearish_absorption` 在语义上是其子集，按原则 5 应删除。但 scan 需要**方向双向可见性保证**——若市场近期持续单边，混合的 20 条可能全为一个方向，LLM 将无法看到另一方向的历史信号。因此例外地保留两者各 **5 条**，以最小代价保证双向可见，不适用原则 5 的完全删除逻辑。

---

## 二、顶层 JSON 结构

与 v2 相同，只保留：
- `indicators`（过滤后）
- `ts_bucket`
- `symbol`

---

## 三、v3 变更规则

### 3.1 footprint — 近价优先 + 强度补充混合策略（v3 新增）

**背景**：v2 规则对 `buy_stacks`/`sell_stacks`/`buy_imb_clusters`/`sell_imb_clusters` 未做数量限制。实测当前数据：

| 窗口 | buy_stacks | sell_stacks | buy_imb_clusters | sell_imb_clusters |
|------|-----------|------------|-----------------|-----------------|
| 15m  | 11 条     | 45 条       | 69 条           | 97 条            |
| 4h   | **237 条** | 30 条      | 137 条          | 128 条           |
| 1d   | **202 条** | 40 条      | 118 条          | 117 条           |

实测 footprint 总大小 **49.5KB**，为单指标体积最大项（v2 估算为 ~17KB，大幅超预期）。

**为什么不能只按强度（length / n）排序**：纯强度排序会将历史高量结构排到前列，而这些结构可能在当前价格很远处，对当前 15m/4h 方向判断意义有限。例如 1d 窗口内早期的强 buy_stack 可能在 $2050，而当前价 $2171，已无直接方向指导意义。

**关于时间维度**：stacks 字段只含 `start_price`/`end_price`/`length`，clusters 字段只含 `p`/`n`，**均无时间戳**，无法按时间过滤。price distance（与现价的距离）是唯一可用的近期性代理指标——近价结构在 4h/1d 窗口内通常意味着近期活动，尽管不是严格保证。

**v3 规则：近价优先 + 强度补充**

每个字段取两组，去重后输出，总数不超过 **20** 条：

| 组别 | 选取逻辑 | 数量 |
|------|---------|------|
| 近价组 | 按距现价距离升序，取最近 N 条 | N = **10** |
| 强度组 | 按强度（stacks→`length`，clusters→`n`）降序，取最强 M 条 | M = **10** |

- **stacks 距离**：`min(|start_price − current_price|, |end_price − current_price|)`（若 current_price 在区间内，distance = 0）
- **clusters 距离**：`|p − current_price|`
- **去重**：stacks 按 `(start_price, end_price)` 去重；clusters 按 `p` 去重
- **current_price 来源（优先级链）**：由调用方依次尝试以下来源，取第一个有效值后传入 `filter_footprint`；全部缺失时退化为纯强度排序（见下方 fallback 说明）：
  1. `avwap.payload.fut_last_price`（首选，mark price 贴近实时）
  2. `kline_history.payload.intervals.15m.markets.futures.bars` 最后一条的 `close`（次选；此处操作的是原始 source，`futures` 是对象，`bars` 是按时间升序的数组，`.last()` = 最新 bar，字段名为完整 `close`）
  3. `kline_history.payload.intervals.4h.markets.futures.bars` 最后一条的 `close`（再次）

  > **注意 scan 输出 vs 原始 source 的区别**：`filter_kline_history` 执行后，scan 输出里 `markets.futures` 会变成紧凑键 `c/h/l` 的数组，且按 newest-first 排列（`[0]` = 最新）。但 `filter_indicators` 拿到的 `source` 是**原始 temp_indicator 数据**，结构是 `futures: {bars: [...]}` 对象，字段为完整 `close/high/low`，bars 按**时间升序**（`.last()` = 最新）。两个上下文不能混用路径。

  > **Fallback 说明**：若三级 fallback 全部缺失，`current_price = None`，`filter_footprint` 退化为全局纯强度排序。这会把修掉的"远端旧结构霸屏"问题带回来，因此生产环境中 avwap 或 kline_history 至少一个必须正常存在；启动时应有断言检查。

**其余 footprint 规则不变**（继承 v2 §3.6）：
- 保留窗口：`15m`、`4h`、`1d`
- 保留标量：`window_delta`、`window_total_qty`、`unfinished_auction`、`ua_top`、`ua_bottom`、`stacked_buy`、`stacked_sell`

**实现**（需修改 `filter_footprint` 函数签名以接受 `current_price: Option<f64>`）：

```rust
// 近价组 + 强度组混合，返回 unique items，上限 near_n + strong_m 条
fn mixed_select_stacks(
    stacks: &[Value],
    current_price: f64,
    near_n: usize,
    strong_m: usize,
) -> Vec<Value> {
    // 距离函数：区间到当前价的最短距离
    let distance = |s: &Value| -> f64 {
        let lo = s.get("start_price").and_then(Value::as_f64).unwrap_or(f64::MAX);
        let hi = s.get("end_price").and_then(Value::as_f64).unwrap_or(f64::MAX);
        if current_price >= lo && current_price <= hi { 0.0 }
        else { (lo - current_price).abs().min((hi - current_price).abs()) }
    };
    let strength = |s: &Value| -> f64 {
        s.get("length").and_then(Value::as_f64).unwrap_or(0.0)
    };

    let mut by_distance = stacks.to_vec();
    by_distance.sort_by(|a, b| distance(a).partial_cmp(&distance(b)).unwrap_or(Ordering::Equal));
    let near: Vec<Value> = by_distance.into_iter().take(near_n).collect();

    let mut by_strength = stacks.to_vec();
    by_strength.sort_by(|a, b| strength(b).partial_cmp(&strength(a)).unwrap_or(Ordering::Equal));
    let strong: Vec<Value> = by_strength.into_iter().take(strong_m).collect();

    // 合并去重：以 (start_price, end_price) 为 key
    let key = |s: &Value| -> (u64, u64) {
        let lo = (s.get("start_price").and_then(Value::as_f64).unwrap_or(0.0) * 100.0) as u64;
        let hi = (s.get("end_price").and_then(Value::as_f64).unwrap_or(0.0) * 100.0) as u64;
        (lo, hi)
    };
    let mut seen = std::collections::HashSet::new();
    near.into_iter().chain(strong).filter(|s| seen.insert(key(s))).collect()
}

fn mixed_select_clusters(
    clusters: &[Value],
    current_price: f64,
    near_n: usize,
    strong_m: usize,
) -> Vec<Value> {
    let distance = |c: &Value| -> f64 {
        (c.get("p").and_then(Value::as_f64).unwrap_or(f64::MAX) - current_price).abs()
    };
    let strength = |c: &Value| -> f64 {
        c.get("n").and_then(Value::as_f64).unwrap_or(0.0)
    };

    let mut by_distance = clusters.to_vec();
    by_distance.sort_by(|a, b| distance(a).partial_cmp(&distance(b)).unwrap_or(Ordering::Equal));
    let near: Vec<Value> = by_distance.into_iter().take(near_n).collect();

    let mut by_strength = clusters.to_vec();
    by_strength.sort_by(|a, b| strength(b).partial_cmp(&strength(a)).unwrap_or(Ordering::Equal));
    let strong: Vec<Value> = by_strength.into_iter().take(strong_m).collect();

    let key = |c: &Value| -> u64 {
        (c.get("p").and_then(Value::as_f64).unwrap_or(0.0) * 100.0) as u64
    };
    let mut seen = std::collections::HashSet::new();
    near.into_iter().chain(strong).filter(|c| seen.insert(key(c))).collect()
}
```

在 `filter_indicators` 中，按优先级链提取 current_price，再用闭包传入：

```rust
// 优先级链：avwap.fut_last_price → kline_history 15m 最新 close → 4h 最新 close
// 全部缺失时为 None，filter_footprint 退化为纯强度排序
let current_price: Option<f64> = source
    .get("avwap")
    .and_then(|v| v.get("payload"))
    .and_then(|p| p.get("fut_last_price"))
    .and_then(Value::as_f64)
    .or_else(|| {
        // 原始 source 中 futures 是对象 {bars: [...]}, 字段为完整名 close/high/low
        // bars 按时间升序，.last() = 最新 bar
        source
            .get("kline_history")
            .and_then(|v| v.get("payload"))
            .and_then(|p| p.get("intervals"))
            .and_then(|i| i.get("15m"))
            .and_then(|w| w.get("markets"))
            .and_then(|m| m.get("futures"))
            .and_then(|f| f.get("bars"))        // source 有 bars 子层
            .and_then(Value::as_array)
            .and_then(|bars| bars.last())       // 升序，last = 最新
            .and_then(|b| b.get("close"))       // 完整字段名
            .and_then(Value::as_f64)
    })
    .or_else(|| {
        source
            .get("kline_history")
            .and_then(|v| v.get("payload"))
            .and_then(|p| p.get("intervals"))
            .and_then(|i| i.get("4h"))
            .and_then(|w| w.get("markets"))
            .and_then(|m| m.get("futures"))
            .and_then(|f| f.get("bars"))
            .and_then(Value::as_array)
            .and_then(|bars| bars.last())
            .and_then(|b| b.get("close"))
            .and_then(Value::as_f64)
    });

// 用闭包而非 fn pointer，捕获 current_price
if let Some(indicator) = source.get("footprint") {
    let payload = indicator
        .get("payload")
        .map(|p| filter_footprint(p, current_price))
        .unwrap_or(Value::Null);
    indicators.insert("footprint".to_string(), rebuild_indicator(indicator, payload));
}
```

`filter_footprint` 签名改为 `fn filter_footprint(payload: &Value, current_price: Option<f64>) -> Value`，`current_price` 为 `None` 时退化为纯强度排序（兜底）。

**预估节省**：49.5KB → **~12KB**（节省 ~37KB）

---

### 3.2 bearish_absorption / bullish_absorption 截断 20 → 5（v3 调整）

**背景**：`absorption` 展示最近 20 条**混合**事件（多空交替），若市场近期持续单边，20 条可能全为一个方向，LLM 看不到另一方向的历史吸收情况。完全删除 `bearish_absorption` / `bullish_absorption` 会丢失这个方向可见性保证。

**v3 规则**：保留 `bearish_absorption` 与 `bullish_absorption`，但截断数从 20 降至 **5**，保证两个方向各自最近的信号始终可见。

```rust
// v2
const EVENT_INDICATOR_RULES: &[(&str, usize)] = &[
    ("absorption", 20),
    ("buying_exhaustion", 20),
    ("selling_exhaustion", 20),
    ("bullish_absorption", 20),   // ← v3 改为 5
    ("bearish_absorption", 20),   // ← v3 改为 5
    ("bullish_initiation", 10),
    ("bearish_initiation", 10),
];

// v3
const EVENT_INDICATOR_RULES: &[(&str, usize)] = &[
    ("absorption", 20),
    ("buying_exhaustion", 10),
    ("selling_exhaustion", 10),
    ("bullish_absorption", 5),    // ← 20 → 5
    ("bearish_absorption", 5),    // ← 20 → 5
    ("bullish_initiation", 10),
    ("bearish_initiation", 10),
];
```

**信息保留分析**：
- `absorption`（20 条混合）：覆盖最近活跃的双向吸收，顺序信号完整
- `bullish_absorption`（5 条）：保证多头吸收最新 5 个信号可见，即使市场近期以空头为主
- `bearish_absorption`（5 条）：同上，保证空头吸收最新信号可见
- 三者合计最多 30 个事件引用，但实际重叠度高，LLM 获得的去重信息密度更高

**预估节省**：
- v2 三者合计 ~35KB；v3 bullish(5/20) + bearish(5/20) ≈ 各 ~2.9KB；合计节省 ~23.3 × (15/20) ≈ **~17.5KB**

---

### 3.3 事件截断数减半（v3 新增）

**背景**：`buying_exhaustion`、`selling_exhaustion` 和 `divergence` 均保留最近 20 条事件。LLM 判断 15m/4h/1d 方向依赖的是**近期信号延续性**，7 天前的事件贡献极低。

**v3 规则**（`buying_exhaustion`/`selling_exhaustion` 已写入 §3.2 的 `EVENT_INDICATOR_RULES`，此处补充 `divergence`）：

| 指标 | v2 截断数 | v3 截断数 |
|------|---------|---------|
| `absorption` | 20 | 20（不变） |
| `buying_exhaustion` | 20 | **10**（见 §3.2） |
| `selling_exhaustion` | 20 | **10**（见 §3.2） |
| `bullish_absorption` | 20 | **5**（见 §3.2） |
| `bearish_absorption` | 20 | **5**（见 §3.2） |
| `bullish_initiation` | 10 | 10（不变） |
| `bearish_initiation` | 10 | 10（不变） |
| `divergence.recent_7d.events` | 20 | **10** |

`divergence` 的 `recent_7d.events` take_last_n 参数从 `20` 改为 `10`（[scan.rs:814](systems/llm/src/llm/filter/scan.rs#L814)）：

```rust
// filter_divergence 函数中
let events = recent_7d
    .get("events")
    .and_then(Value::as_array)
    .map(|events| take_last_n(events, 10))  // ← 20 → 10
    .unwrap_or_default();
```

**预估节省**：~24KB（buying/selling exhaustion 各 ~8KB + divergence ~8KB）

---

### 3.4 cvd_pack series 删除 cvd_7d 累积字段（v3 新增）

**背景**：每条 series 条目当前保留 11 个字段，其中 `cvd_7d_fut` 和 `cvd_7d_spot` 是 7 日累积 CVD 值（滚动累加，每条 bar 都携带）。LLM 判断流量方向依赖的是 per-bar 的 `delta_fut`/`delta_spot` 以及 `relative_delta_*`，累积值在 series 上下文中信息冗余。

**v3 规则**：从 cvd series 每条条目中删除 `cvd_7d_fut` 和 `cvd_7d_spot`：

```rust
// v2: 11 个字段
copy_fields(
    &mut filtered_entry,
    &entry,
    &[
        "ts",
        "close_fut",
        "close_spot",
        "delta_fut",
        "delta_spot",
        "relative_delta_fut",
        "relative_delta_spot",
        "cvd_7d_fut",          // ← v3 删除
        "cvd_7d_spot",         // ← v3 删除
        "spot_flow_dominance",
        "xmk_delta_gap_s_minus_f",
    ],
);

// v3: 9 个字段
copy_fields(
    &mut filtered_entry,
    &entry,
    &[
        "ts",
        "close_fut",
        "close_spot",
        "delta_fut",
        "delta_spot",
        "relative_delta_fut",
        "relative_delta_spot",
        "spot_flow_dominance",
        "xmk_delta_gap_s_minus_f",
    ],
);
```

**影响评估**：顶层 `cvd_pack` 标量中已保留 `cvd_slope_fut`/`cvd_slope_spot` 等趋势概要。per-bar delta 序列已足够 LLM 判断 CVD 方向，删除 7 日累积值无信息损失。

**预估节省**：~4KB（57 条 series × 2 字段 × ~35 chars/field）

---

### 3.5 volatility_summary — 由 kline_history 派生（v3 新增）

**背景与动机**：

现有指标中与波动率相关的数据：
- `rvwap_sigma_bands.by_window.*.rvwap_sigma_w`：rolling VWAP 的价格标准差，实测值 15m=2.63 / 4h=12.75 / 1d=35.82
- `kline_history` OHLCV bars：包含每根 K 线的 H/L，可计算 ATR

**实测对比**（以当前数据为例）：

| 窗口 | rvwap_sigma_w | ATR(14) | 倍差 |
|------|-------------|---------|------|
| 15m  | 2.63 (0.12%) | 12.43 (0.57%) | ×4.7 |
| 4h   | 12.75 (0.59%) | 29.79 (1.37%) | ×2.3 |
| 1d   | 35.82 (1.65%) | 100.18 (4.61%) | ×2.8 |

`rvwap_sigma_w` 是价格偏离 VWAP 的分散度，**不等于**一根 bar 的价格波动幅度。LLM 用 `rvwap_sigma_w` 做区间预测会系统性低估实际运动幅度 2–5 倍。加入 ATR 可直接回答"一根 4h 典型波动是多少点"。

**v3 规则**：在 `filter_kline_history` 处理结束后，由已有 bars 原地计算 `volatility_summary`，**不新增数据源，不引入新指标**。

```json
"volatility_summary": {
  "15m": {"atr14": 12.43, "atr14_pct": 0.572, "atr14_pct_rank_30d": 0.38},
  "4h":  {"atr14": 29.79, "atr14_pct": 1.372, "atr14_pct_rank_30d": 0.45},
  "1d":  {"atr14": 100.18, "atr14_pct": 4.614, "atr14_pct_rank_30d": 0.62}
}
```

字段说明：
- `atr14`：ATR(14)，单位与价格相同（绝对值，用于区间计算）
- `atr14_pct`：`atr14 / current_price × 100`，百分比（跨品种可比）
- `atr14_pct_rank_30d`：当前 `atr14_pct` 在过去 30d 同窗口 ATR 序列中的百分位（0=历史最低，1=历史最高；用于判断当前是低波还是高波，指导区间宽窄）

**计算逻辑**（在 `filter_kline_history` 执行完截断后，基于截断前的完整 bars 计算）：

```rust
// compute_atr14 / atr_pct_rank 在 filter_kline_history 内、截断前调用，
// 此时 bars 来自原始 source，字段为完整名 high/low/close，按时间升序排列
fn compute_atr14(bars: &[Value]) -> Option<f64> {
    let trs: Vec<f64> = bars.windows(2).filter_map(|w| {
        let h = w[1].get("high").and_then(Value::as_f64)?;   // 原始 source 完整字段名
        let l = w[1].get("low").and_then(Value::as_f64)?;
        let pc = w[0].get("close").and_then(Value::as_f64)?;
        Some((h - l).max((h - pc).abs()).max((l - pc).abs()))
    }).collect();
    if trs.len() < 14 { return None; }
    Some(trs[trs.len() - 14..].iter().sum::<f64>() / 14.0)
}

fn atr_pct_rank(current_atr_pct: f64, bars: &[Value]) -> Option<f64> {
    let current_price = bars.last()?.get("close").and_then(Value::as_f64)?;  // 升序，last = 最新
    let historical_atrs: Vec<f64> = (14..bars.len())
        .filter_map(|i| {
            compute_atr14(&bars[i.saturating_sub(14)..=i])
                .map(|a| a / current_price * 100.0)
        })
        .collect();
    if historical_atrs.is_empty() { return None; }
    let below = historical_atrs.iter().filter(|&&v| v <= current_atr_pct).count();
    Some(below as f64 / historical_atrs.len() as f64)
}
```

- 计算使用截断**前**的完整 bars（kline_history 原始长度：15m=120条, 4h=73条, 1d=40条）
- 每个窗口的 `atr14_pct_rank_30d` 基于各自窗口的全量 bars 历史
- 结果以 4 位有效数字输出（`round_derived_fields` 已处理，无需额外精度控制）

**预估大小**：`volatility_summary` 3 个窗口 × 3 个标量 ≈ **~0.3KB**（极低增量）

---

## 四、v3 估算汇总

> 以下基于实测 v2 输出（**187.2KB minified**，373KB pretty-printed）计算各项节省
> 各指标大小使用 Python `json.dumps(v)` 默认分隔符测量（含空格，比严格 minified 约大 7%），节省估算同口径，可互相比较

| 指标 | v2 实测大小 | v3 变更 | v3 估算大小 | 节省 |
|------|-----------|--------|-----------|------|
| `vpin` | ~2KB | — | ~2KB | — |
| `whale_trades` | ~4KB | — | ~4KB | — |
| `high_volume_pulse` | ~1KB | — | ~1KB | — |
| `tpo_market_profile` | ~5KB | — | ~5KB | — |
| `price_volume_structure` | ~2KB | — | ~2KB | — |
| `fvg` | ~2KB | — | ~2KB | — |
| `kline_history` | ~8KB | — | ~8KB | — |
| `cvd_pack` | ~21.5KB | series 删除 cvd_7d 2 字段 | ~17.5KB | ~4KB |
| `avwap` | ~3KB | — | ~3KB | — |
| `rvwap_sigma_bands` | ~3KB | — | ~3KB | — |
| `ema_trend_regime` | ~3KB | — | ~3KB | — |
| `funding_rate` | ~3.4KB | — | ~3.4KB | — |
| `liquidation_density` | ~0.6KB | — | ~0.6KB | — |
| `footprint` | **~49.5KB** | 混合策略，各字段 top(近价10+强度10) | **~12KB** | **~37KB** |
| `kline_history` | ~8KB | 新增 volatility_summary 派生字段 | ~8.3KB | **−0.3KB** |
| `orderbook_depth` | ~4KB | — | ~4KB | — |
| `absorption` | ~11.6KB | — | ~11.6KB | — |
| `buying_exhaustion` | ~16KB | 20 → 10 条 | ~8KB | ~8KB |
| `selling_exhaustion` | ~16KB | 20 → 10 条 | ~8KB | ~8KB |
| `bullish_absorption` | ~11.6KB | 20 → **5** 条 | ~2.9KB | ~8.7KB |
| `bearish_absorption` | ~11.7KB | 20 → **5** 条 | ~2.9KB | ~8.8KB |
| `bullish_initiation` | ~5KB | — | ~5KB | — |
| `bearish_initiation` | ~5KB | — | ~5KB | — |
| `divergence` | ~15KB | 20 → 10 条 | ~7.5KB | ~7.5KB |
| **合计（测量口径）** | **~202KB** | | **~120KB** | **~82KB** |
| **对应 minified 估算** | **187.2KB** | | **~111KB** | **~76KB**（约 ×0.93 换算） |

---

## 五、变更文件清单

仅需修改 **1 个文件**：[`systems/llm/src/llm/filter/scan.rs`](systems/llm/src/llm/filter/scan.rs)

| 位置 | 变更内容 |
|------|---------|
| `EVENT_INDICATOR_RULES`（第 9-17 行） | `bullish_absorption`/`bearish_absorption` 20→5；`buying_exhaustion`/`selling_exhaustion` 20→10 |
| `filter_divergence`（第 814 行） | `take_last_n(events, 20)` → `take_last_n(events, 10)` |
| `filter_footprint`（第 662-709 行） | 签名加 `current_price: Option<f64>` 参数；stacks/clusters 各调用 `mixed_select_*` 混合策略函数 |
| `filter_indicators`（第 137 行附近） | 按优先级链提取 current_price（avwap.fut_last_price → kline_history 15m 最新 close → 4h 最新 close），改用闭包调用 `filter_footprint` |
| `filter_cvd_pack` series 字段列表（第 406-422 行） | 删除 `"cvd_7d_fut"`、`"cvd_7d_spot"` |
| `filter_kline_history`（第 311-366 行） | 截断完成后新增 `volatility_summary` 字段（由完整 bars 原地计算） |
| 新增辅助函数 | `mixed_select_stacks`、`mixed_select_clusters`、`compute_atr14`、`atr_pct_rank`（见各节实现代码） |

---

## 六、验证方法

```python
import json, glob

path = sorted(glob.glob("systems/llm/temp_model_input/*_scan_*.json"))[-1]
with open(path) as f:
    data = json.load(f)

raw = json.dumps(data, separators=(',', ':'))
print(f"minified: {len(raw) / 1024:.1f}KB")   # 目标 < 115KB

inds = data['indicators']

# current_price: 优先级链（此处读 scan 输出）
# scan 输出中 markets.futures 已被 filter_kline_history 转为数组，字段紧凑键 c/h/l
# newest-first：[0] 是最新 bar，[-1] 是最旧保留 bar
current_price = (
    inds.get('avwap', {}).get('payload', {}).get('fut_last_price')
    or (inds.get('kline_history', {}).get('payload', {})
            .get('intervals', {}).get('15m', {})
            .get('markets', {}).get('futures', [{}])[0].get('c'))  # [0] = newest
    or (inds.get('kline_history', {}).get('payload', {})
            .get('intervals', {}).get('4h', {})
            .get('markets', {}).get('futures', [{}])[0].get('c'))
)
assert current_price, "could not resolve current_price from any fallback source"

# 3.1 footprint 混合策略验证
for win in ['15m', '4h', '1d']:
    fp_win = inds['footprint']['payload']['by_window'][win]
    for field in ['buy_stacks', 'sell_stacks', 'buy_imb_clusters', 'sell_imb_clusters']:
        items = fp_win.get(field, [])
        assert len(items) <= 20, f"footprint {win}.{field} has {len(items)} items (max 20)"

# 近价组有效性验证：与原始 temp_indicator 对比，输出应包含全量最近10条中的大部分
# scan 输出的 bars 已截断，ATR 从 scan 输出中估算仅作宽松校验
def compute_atr14_compact(bars):
    """scan 输出格式：newest-first 数组，字段紧凑键 h/l/c
    验证用途：bars 已被截断（非完整历史），ATR 仅作宽松边界校验"""
    # newest-first → 需要反转后才能按时序做 TR 计算
    bars_asc = list(reversed(bars))
    trs = []
    for i in range(1, len(bars_asc)):
        h = bars_asc[i].get('h', 0) or 0
        l = bars_asc[i].get('l', 0) or 0
        pc = bars_asc[i-1].get('c', 0) or 0
        if h and l and pc:
            trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs[-14:]) / 14 if len(trs) >= 14 else None

# scan 输出 markets.futures 是数组（newest-first）
bars_1d = inds['kline_history']['payload']['intervals']['1d']['markets']['futures']
atr_1d = compute_atr14_compact(bars_1d)
# 阈值：2×ATR_1d，宽松地覆盖"近价结构区"；fallback 用 5% of price
near_threshold = 2 * atr_1d if atr_1d else current_price * 0.05

for win in ['15m', '4h', '1d']:
    fp_win = inds['footprint']['payload']['by_window'][win]
    for field, price_key in [('buy_stacks','start_price'), ('sell_stacks','start_price'),
                              ('buy_imb_clusters','p'), ('sell_imb_clusters','p')]:
        items = fp_win.get(field, [])
        if len(items) < 5:
            continue  # 全量本身就少于 5 条，跳过
        near_count = sum(1 for s in items if abs(s.get(price_key, 0) - current_price) <= near_threshold)
        assert near_count >= min(5, len(items) // 2), (
            f"footprint {win}.{field}: only {near_count}/{len(items)} items within "
            f"±{near_threshold:.1f} of price, near 组可能未生效"
        )

# 3.2 bearish/bullish absorption 截断为 5 条
for code in ['bearish_absorption', 'bullish_absorption']:
    assert code in inds, f"{code} should still exist in v3 (truncated to 5, not deleted)"
    events = inds[code]['payload']['recent_7d']['events']
    assert len(events) <= 5, f"{code} has {len(events)} events (max 5)"
# absorption 保留 20 条且含 type/direction
abs_events = inds['absorption']['payload']['recent_7d']['events']
assert len(abs_events) <= 20
assert all('type' in e for e in abs_events), "absorption events missing type field"
assert all('direction' in e for e in abs_events), "absorption events missing direction field"

# 3.3 事件截断数验证
for code, max_n in [('buying_exhaustion', 10), ('selling_exhaustion', 10)]:
    events = inds[code]['payload']['recent_7d']['events']
    assert len(events) <= max_n, f"{code} has {len(events)} events (max {max_n})"
div_events = inds['divergence']['payload']['recent_7d']['events']
assert len(div_events) <= 10, f"divergence has {len(div_events)} events (max 10)"

# 3.4 cvd_pack series 不含 cvd_7d 字段
for win in ['15m', '4h', '1d']:
    series = inds['cvd_pack']['payload']['by_window'][win]['series']
    for entry in series:
        assert 'cvd_7d_fut' not in entry, f"cvd_pack {win} series still has cvd_7d_fut"
        assert 'cvd_7d_spot' not in entry, f"cvd_pack {win} series still has cvd_7d_spot"

# 3.5 volatility_summary 验证
# 注意：volatility_summary 由截断前的完整 bars 计算，scan 输出中只包含结果标量
# 验证仅做字段存在性 + 值域合理性检查，不重新反推 ATR（原始 bars 已被截断）
kl = inds['kline_history']['payload']
vs = kl.get('volatility_summary', {})
assert vs, "volatility_summary missing from kline_history payload"
for win in ['15m', '4h', '1d']:
    assert win in vs, f"volatility_summary missing window {win}"
    w = vs[win]
    assert 'atr14' in w and w['atr14'] > 0, f"volatility_summary.{win}.atr14 invalid"
    assert 'atr14_pct' in w and 0 < w['atr14_pct'] < 100, f"volatility_summary.{win}.atr14_pct out of range"
    assert 'atr14_pct_rank_30d' in w and 0.0 <= w['atr14_pct_rank_30d'] <= 1.0, \
        f"volatility_summary.{win}.atr14_pct_rank_30d out of [0,1]"
# atr14_pct 应在合理范围内（不做跨窗口单调性断言，真实市场不保证）
for win, lo, hi in [('15m', 0.01, 5.0), ('4h', 0.1, 20.0), ('1d', 0.5, 50.0)]:
    pct = vs[win]['atr14_pct']
    assert lo < pct < hi, f"volatility_summary.{win}.atr14_pct={pct:.3f} outside plausible range [{lo},{hi}]"

print("all v3 assertions passed")
```

---

## 七、v4 方向（待开发）

### 背景

v3 验证结论：
- **目标 2**（体积）：实测 187.17KB → 111.22KB，达到 `< 115KB` 目标 ✓
- **目标 1**（LLM 稳定给出 15m/4h/1d 方向和区间）：v3 是"明显变好"，但未从根上解决

### v3 未解决的核心问题

1. **flow 事件无时间框架归属**：`absorption`/`exhaustion`/`divergence` 均为 `recent_7d` 混合流，LLM 需要自己判断哪些事件属于 15m、哪些属于 4h/1d，稳定性差
2. **支撑阻力分散**：分布在 `price_volume_structure`、`tpo_market_profile`、`rvwap_sigma_bands`、`footprint`、`orderbook_depth`、`fvg` 六处，模型需要自己拼装"区间"，容易不一致
3. **无归属的顶层字段干扰**：`cvd_pack.payload.delta_*`、`price_volume_structure` 顶层 poc/vah/val（无窗口归属）、`whale_trades.payload` 顶层大量 0 值，比 `by_window` 上下文更模糊
4. **LLM 同时做特征提取 + 结构判断**：比"先做 summary，再让 LLM 叙述"不稳定

### v4 设计方向：v3 + 极薄 summary layer

#### 7.1 每个 timeframe 的固定摘要块

```json
"by_timeframe": {
  "15m": {
    "structure": {
      "poc": ..., "vah": ..., "val": ...,
      "tpo_poc": ..., "tpo_vah": ..., "tpo_val": ...,
      "support_candidates": [...],     // 聚类后上方前2支撑
      "resistance_candidates": [...]   // 聚类后下方前2阻力
    },
    "flow": {
      "cvd_bias": "bullish|bearish|neutral",
      "whale_bias": ..., "footprint_bias": ...,
      "absorption_bias": ..., "exhaustion_bias": ..., "divergence_bias": ...
    },
    "volatility": {
      "atr14": ..., "atr14_pct": ..., "expected_move_1bar": ...
    },
    "agreement": {
      "bullish_signals": N, "bearish_signals": N, "neutral_signals": N,
      "key_contradiction": "..."   // 最主要矛盾点
    }
  },
  "4h": { ... },
  "1d": { ... }
}
```

#### 7.2 level_book：聚类后的价格结构

把 PVS/TPO/AVWAP/RVWAP/FVG/footprint/orderbook/liquidation 的价格先聚类，再输出结构化结果，替代让 LLM 从六处原始数组自己拼装：

```json
"level_book": {
  "current_price": 2171.46,
  "support": [
    {"price": 2155.0, "strength": 0.82, "sources": ["tpo_val", "footprint_buy_stack", "fvg"]},
    {"price": 2135.0, "strength": 0.65, "sources": ["pvs_hvn", "avwap"]}
  ],
  "resistance": [
    {"price": 2188.0, "strength": 0.74, "sources": ["footprint_sell_stack", "orderbook_wall"]},
    {"price": 2210.0, "strength": 0.58, "sources": ["tpo_vah", "rvwap_band_plus_1"]}
  ]
}
```

> 聚类方式：按价格 bin=0.5% 分组，合并同组内所有来源的 level，`strength` = 归一化的来源数量 × 各来源权重

#### 7.3 footprint 上下分开选

当前 v3 用绝对距离排序，可能把所有 item 选在价格同一侧。v4 改为：
- 下方（`price < current_price`）：取距离最近的 top 5 → 支撑候选
- 上方（`price > current_price`）：取距离最近的 top 5 → 阻力候选
- 两侧分别补 top 3 全局最强（去重）

#### 7.4 flow 事件按时间框架归属 + per-direction 三维选取

**时间归属分桶**：对 `absorption`/`exhaustion`/`divergence`/`initiation` 的 `event_start_ts` 做归属：
- 过去 4h 内：归 15m 层
- 过去 24h 内：归 4h 层
- 过去 7d 内：归 1d 层

**per-direction 三维选取**：对分桶后的同方向事件（bullish / bearish），各方向保留：

| 维度 | 选取逻辑 | 数量 |
|------|---------|------|
| 最近 | 按 `event_start_ts` 降序 | 1 条 |
| 最强 | 按 `score` 降序 | 1 条 |
| 近价 | 按 `|pivot_price − current_price|` 升序 | 1 条 |

去重后每方向最多 3 条，每个事件类型每个 timeframe 最多 bullish×3 + bearish×3 = 6 条。

**bias_summary**：在三维选取之外，为每个事件类型附加方向倾向摘要：

```json
"absorption_bias": {
  "direction": "bearish",          // 近期主导方向（近 4h/24h 内事件计数多的一侧）
  "bullish_count_window": 2,       // 当前时间窗口内多头事件数
  "bearish_count_window": 5,       // 当前时间窗口内空头事件数
  "last_event_direction": "bearish" // 最近一条事件的方向
}
```

bias_summary 是纯统计摘要，不依赖原始事件数组，体积极小（~4 个标量/指标/timeframe）。

#### 7.5 体积进一步优化（可叠加到 v3）

| 操作 | 预估节省 |
|------|---------|
| `whale_trades` / `vpin` 删除 `1h`、`3d` 窗口 | ~1.7KB |
| `tpo_market_profile.dev_series` 各序列截断到最近 5 条 | ~0.5KB |
| `avwap.series_by_window` 各窗口截断到 3 条 | ~0.5KB |
| `funding_rate.by_window.changes` 从 last-10 → last-5 | ~0.3KB |
| 顶层无窗口归属字段（`cvd_pack.payload.delta_*` 等）移除或迁入 `by_window` | ~1.0KB |

以上叠加 v3 后预估 minified 可从 111KB 进一步降至 **~102KB**。

### v4 工作量评估

| 模块 | 工作量 | 说明 |
|------|-------|------|
| `level_book` 聚类 | 中 | 需要汇聚 6 个来源的价格，纯 Rust 计算 |
| `by_timeframe` 摘要块 | 大 | 需要定义每类 bias 的计算规则 |
| flow 事件时间归属 | 小 | 只需按 event_start_ts 分桶 |
| footprint 上下分选 | 小 | 在 v3 mixed_select 基础上改方向逻辑 |
| 体积优化 §7.5 | 小 | 修常量和截断参数 |

**建议顺序**：先做 §7.5（快、体积收益确定）+ footprint 上下分选，再做 level_book，最后做 by_timeframe 完整摘要块。
