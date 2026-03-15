# Scan 数据源过滤规则 v2

**状态**：草案
**适用文件**：`systems/llm/src/llm/filter/scan.rs`
**适用提示词**：`systems/llm/src/llm/prompt/scan/medium_large_opportunity.txt`
**数据来源**：`temp_indicator/{ts}_ETHUSDT.json`（经 `round_derived_fields` 四舍五入后的数据）
**基线体积**：v1 实测 362.7KB（minified）；新数据源实测 scan raw ≈ 4MB（price_volume_structure.levels 独占 1.84MB）
**目标体积**：< 260KB（minified）
**与 v1 差异**：
1. 对事件字段、FVG 字段、Divergence 字段做冗余字段去重（不改变截断条数）
2. 将原始时间序列（funding/liq 小时桶、PVS levels、OB 100档）替换为通用指标标量，原始序列信息完全可由这些标量表达，模型无需扫描长序列即可理解
3. **v2 新增（数据源变更后）**：`price_volume_structure.levels` 和 `value_area_levels` 在新数据源下体积暴增（15m=543条、4h=2542条、1d=4692条），必须丢弃；同步引入 `va_top_levels`（参见 §3.2），以 5 个高量锚点替代 VA 内部明细
4. **v2 新增（与 core v3 对齐）**：事件类字段去重补充 4 个字段（`score_base`、`spot_flow_confirm_score`、`spot_whale_confirm_score`、`spot_cvd_1m_change`）

> **只为 Scan 服务**：以下规则仅针对 Stage 1 市场扫描提示词的需求设计，不适用于 Entry / Management / Pending Order 阶段。
> 所有保留或新增的时间序列统一按时间 **newest-first** 输出。

---

## 一、设计原则

v2 与 v1 设计原则完全一致，补充一条去重原则：

**原则 0（新增）：冗余字段不输出**
若某字段的值在语义上完全等价于另一已保留字段（字段值恒相等、或在指标层级上下文中信息已知），则丢弃冗余字段，保留语义更明确的那个。

1. **趋势基座（Trend Base）**：OHLCV、EMA 状态、CVD 趋势 → 必须充分
2. **结构区间（Structure）**：PVS、TPO、FVG、Footprint 不平衡区、OB 流动性墙、AVWAP/RVWAP 动态 S/R → 必须完整
3. **流量信号（Flow Context）**：Whale、VPIN、Absorption、Exhaustion、Initiation、Divergence、Funding → 必须覆盖
4. **压缩是第二需求**：只在不损害上述信息质量的前提下压缩

---

## 二、顶层 JSON 结构

与 v1 相同，只保留：
- `indicators`（过滤后）
- `ts_bucket`
- `symbol`

---

## 三、各指标过滤规则

### 3.1 完整保留（体积已很小，无变化）

| 指标 | 估算大小 | 说明 |
|------|---------|------|
| `vpin` | ~2KB | 纯标量 + by_window，全量保留 |
| `whale_trades` | ~4KB | 顶层标量 + by_window，全量保留 |
| `high_volume_pulse` | ~1KB | 纯标量，全量保留 |
| `tpo_market_profile` | ~4KB | 含 by_session 1d/4h，dev_series newest-first |

---

### 3.2 保留但裁剪 3d 窗口

#### `price_volume_structure`（新数据源实测 1.84MB → v2 ~2KB）

**新数据源实测体积**：

| 字段 | 条数 | chars |
|------|------|-------|
| `by_window.15m.levels` | 543 | 105,387 |
| `by_window.4h.levels` | 2,542 | 523,651 |
| `by_window.1d.levels` | 4,692 | 993,409 |
| `by_window.15m.value_area_levels` | 453 | 20,795 |
| `by_window.4h.value_area_levels` | 1,307 | 63,703 |
| `by_window.1d.value_area_levels` | 2,539 | 127,004 |
| 顶层 `value_area_levels` | 126 | 5,556 |
| **可丢弃合计** | | **~1,839,505 chars** |
| **丢弃后 PVS 剩余** | | **~1,126 chars** |

- **保留窗口**：`15m`、`4h`、`1d`（丢弃 `3d`）
- **每个窗口保留字段**：`poc_price`、`poc_volume`、`vah`、`val`、`hvn_levels`、`lvn_levels`、`volume_zscore`、`volume_dryup`、`bar_volume`、`window_bars_used`
- **每个窗口丢弃**：`levels`（全量价格档位明细）、`value_area_levels`（全量 VA 档位）
- **顶层保留**：`poc_price`、`poc_volume`、`vah`、`val`、`bar_volume`、`hvn_levels`、`lvn_levels`
- **顶层丢弃**：`value_area_levels`
- **v2 新增**：每个窗口增加 `va_top_levels`，取 volume 降序前 **5** 条，按 `price` **降序**输出：

  ```json
  "va_top_levels": [
    {"price": 2130.0, "volume": 38530.5, "vol_pct": 0.07},
    {"price": 2100.0, "volume": 34592.1, "vol_pct": 0.06},
    {"price": 2090.0, "volume": 44419.6, "vol_pct": 0.08},
    {"price": 2080.0, "volume": 39758.1, "vol_pct": 0.07},
    {"price": 2054.0, "volume": 28100.0, "vol_pct": 0.05}
  ]
  ```

  - `vol_pct`：该档占本窗口 value area 总成交量比例
  - scan 只需 5 条（entry_core 需要更多，见 core v3 §3.1）

> **去除原因**：新数据源下 `levels` 结构包含 9 个字段（`buy_volume`/`sell_volume`/`delta`/`volume`/`price_level`/`is_hvn`/`is_lvn`/`is_in_value_area`/`level_rank`），1d 窗口 4692 条 ≈ 993KB，是 scan 文件体积爆炸的唯一根因。`poc_price`/`vah`/`val`/`hvn_levels`/`lvn_levels` 已是 Volume Profile 的通用摘要；实测 hvn_levels 在多数窗口为空，新增 `va_top_levels` 补充 VA 内部最高量锚点（实测 1d top-5 均为整数关口如 2090/2080/2130/2100/2098），是 scan 唯一需要的 VA 内部结构信息。

#### `fvg`（~27KB → v2 ~1KB）

- **保留窗口**：`15m`、`4h`、`1d`（丢弃 `3d`）
- **每个窗口保留**：`active_bull_fvgs`、`active_bear_fvgs`、`nearest_bull_fvg`、`nearest_bear_fvg`、`is_ready`、`coverage_ratio`
- **兼容字段**：每个 FVG item 在保留原始 `upper` / `lower` 的同时，额外补 `fvg_top = upper`、`fvg_bottom = lower`，方便下游统一按边界语义读取
- **v2 新增：每个 FVG item 去掉以下冗余字段**：

  | 字段 | 去除原因 |
  |------|---------|
  | `fvg_id` | 长字符串唯一 ID（格式如 `fvg\|ETHUSDT\|15m\|bull\|...`），LLM 无法引用，无语义 |
  | `event_available_ts` | 数据管道内部时间戳，非业务字段 |
  | `tf` | 值恒等于所在 by_window 的窗口名（如 `"15m"`），上下文已知 |

  适用范围：`active_bull_fvgs[]`、`active_bear_fvgs[]`、`nearest_bull_fvg`、`nearest_bear_fvg` 内所有 item。

---

### 3.3 时间序列截断类（截断条数与 v1 相同，无变化）

#### `kline_history`（~7KB）

- 保留 `15m`（30根）、`4h`（20根）、`1d`（14根），`futures` only
- 字段缩写：`o/h/l/c/v/t/closed`
- 输出 newest-first

#### `cvd_pack`（~21KB）

- 顶层标量全量保留
- 保留 `15m`（30条）、`4h`（20条）、`1d`（全量）
- 每条 series 保留字段：`ts`、`close_fut`、`close_spot`、`delta_fut`、`delta_spot`、`relative_delta_fut`、`relative_delta_spot`、`cvd_7d_fut`、`cvd_7d_spot`、`spot_flow_dominance`、`xmk_delta_gap_s_minus_f`
- 输出 newest-first

#### `avwap`（~3KB）

- 顶层标量全量保留
- 保留 `15m`（10条）、`4h`（5条）、`1d`（全量）
- 每条 series：`ts`、`avwap_fut`、`avwap_spot`、`xmk_avwap_gap_f_minus_s`
- 输出 newest-first

---

### 3.4 序列提取精简类（无变化）

#### `rvwap_sigma_bands`（~2KB）

- 丢弃 `series_by_output_window`
- 保留 `by_window.15m/4h/1d` 当前标量 + `as_of_ts`、`source_mode`
- 新增 `z_series_15m`：最近 20 条，每条：`{ts, z: {15m, 4h, 1d}}`（提取 `z_price_minus_rvwap`）

#### `ema_trend_regime`（~3KB）

- 丢弃 `ffill_series_by_output_window`
- 保留顶层标量：`ema_13`、`ema_21`、`ema_34`、`ema_band_high`、`ema_band_low`、`trend_regime`、`ema_100_htf`、`ema_200_htf`、`trend_regime_by_tf`、`as_of_ts`、`output_sampling`
- 新增 `regime_series_15m`：最近 20 条，每条：`{ts, trend_regime, trend_regime_4h, trend_regime_1d}`

---

### 3.5 按时间重新聚合 → 通用指标标量替换

#### `funding_rate`（~14KB → v2 ~5KB）

- **丢弃** `funding_trend_hourly`（原 139 条小时桶，9.1KB）
- **新增** `funding_summary` 对象，由 `recent_7d` 计算：

  ```json
  "funding_summary": {
    "ema_8h":   -0.0000382,   // 最近 8h 指数加权平均资金费率
    "ema_24h":  -0.0000214,   // 最近 24h 指数加权平均资金费率
    "z_score_7d": -1.83,      // 当前资金费率相对 7 日历史均值的 z-score
    "consecutive_direction_hours": -14  // 负=连续 N 小时为负费率，正=连续 N 小时为正费率
  }
  ```

- `by_window` 保留 `15m`、`4h`、`1d`，每窗口 last-10 changes
- 顶层标量：`funding_current`、`funding_twa`、`mark_price_last`、`mark_price_last_ts`、`mark_price_twap`、`funding_current_effective_ts`

> **去除原因**：139 条小时桶需要模型自行分析趋势；z-score 和 EMA 是普适统计量，模型立即理解"资金费率处于 -1.83σ、连续 14 小时为负"，信息等价但体积节省 ~9KB。

#### `liquidation_density`（~10KB → v2 ~2KB）

- **丢弃** `liq_trend_hourly`（原 139 条小时桶，9.1KB）
- **新增** `liq_summary` 对象，由 `recent_7d` 计算：

  ```json
  "liq_summary": {
    "long_24h":  24500000,   // 最近 24h 多头清算总量（USD）
    "short_24h": 156000000,  // 最近 24h 空头清算总量（USD）
    "ratio_24h": 0.14,       // long/(long+short)，<0.5 表示空头被清算更多
    "long_7d":   180000000,
    "short_7d":  620000000,
    "intensity_z7d": 1.8     // 当前 24h 清算总量相对 7 日历史的 z-score
  }
  ```

- `by_window` 保留 `15m`、`4h`、`1d`，每窗口保留：`long_total`、`short_total`、`peak_levels`、`coverage_ratio`

> **去除原因**：多空清算比（ratio）是期货市场标准指标，z-score 普适，两者合计 6 个标量即可替代 139 条原始时序，信息损失极低。

---

### 3.6 大体积结构聚合重构（无变化）

#### `footprint`（~13KB）

- 保留窗口：`15m`、`4h`、`1d`（丢弃 `1h`、`3d`）
- 丢弃 `levels`
- 保留标量：`window_delta`、`window_total_qty`、`unfinished_auction`、`ua_top`、`ua_bottom`、`stacked_buy`、`stacked_sell`、`buy_stacks`、`sell_stacks`
- 不平衡价格分箱聚合：`buy_imb_clusters`、`sell_imb_clusters`，格式 `[{p, n}]`（15m bin=0.1，4h bin=0.5，1d bin=1.0）

#### `orderbook_depth`（~16KB → v2 ~5KB）

- 丢弃原始 `levels`（56062条）
- **不再**输出 `top_liquidity_levels`（原 100 条，13.1KB）
- **新增** `liquidity_walls` 对象：

  ```json
  "liquidity_walls": {
    "bid_walls": [
      {"price_level": 2050.0, "total_liquidity": 11790000, "distance_pct": -1.8},
      ...
    ],  // 按 bid_liquidity 降序聚类后取前 5 个最大买方流动性墙
    "ask_walls": [
      {"price_level": 2120.0, "total_liquidity": 8500000, "distance_pct": +2.1},
      ...
    ],  // 按 ask_liquidity 降序聚类后取前 5 个最大卖方流动性墙
    "depth_imbalance_1pct": 0.62,  // bid/(bid+ask)，当前价 ±1% 范围内
    "depth_imbalance_3pct": 0.55   // bid/(bid+ask)，当前价 ±3% 范围内
  }
  ```

  > 聚类方式：直接基于全量 `levels`，先按距当前价 `±15%` 做距离过滤，再按 bin=0.5% 价格区间分组，取每组 liquidity 总量最大的代表价格，再取 bid/ask 分别的前 5 组。`distance_pct` = (wall_price - mid_price) / mid_price × 100。  
  > 其中 `bid_walls` 仅保留 `distance_pct ∈ [-15%, 0]`，`ask_walls` 仅保留 `distance_pct ∈ [0, +15%]`；`depth_imbalance_1pct / 3pct` 直接使用全量 `levels` 在近价窗口内计算，不再基于 top100 档位。

- 顶层标量全量保留（含已有的 `obi`、`obi_fut`、`ofi_norm_fut` 等通用指标）
- `by_window` 只保留 `15m`（丢弃 `1h`）

> **去除原因**：100 条明细需要模型自行聚合才能找到流动性墙；"买方流动性墙在 $2050，$11.8M，距价格 -1.8%" 是即时可用的结构信息。`obi`/`ofi_norm` 等标量本身即为通用的订单簿失衡指标，与流动性墙合并足以覆盖原 100 档的信息。

---

### 3.7 事件列表类（v2 新增字段去重）

`scan` 不再保留通用 `initiation`，只保留方向化后的 `bullish_initiation` / `bearish_initiation`；其余保留事件继续做 newest-first 截断和字段去重。

#### v2 新增：事件 item 通用去重规则

适用所有事件类指标（`absorption`、`bullish_absorption`、`bearish_absorption`、`buying_exhaustion`、`selling_exhaustion`、`bullish_initiation`、`bearish_initiation`）中 `recent_7d.events[]` 内的每条事件：

| 字段 | 去除原因 |
|------|---------|
| `event_id` | 长字符串唯一 ID（~80字符），LLM 无法引用，无语义价值 |
| `end_ts` | 值完全等于 `event_end_ts`（完全重复） |
| `start_ts` | 值完全等于 `event_start_ts`（完全重复） |
| `event_available_ts` | 数据管道内部时间戳，非业务字段 |
| `indicator_code` | 值恒等于所在指标名，在指标层级上下文中已知 |
| `min_follow_required_minutes` | 静态配置参数，非运行时数据 |
| `strength_score_xmk` | 值完全等于 `score`（完全重复） |
| `spot_rdelta_mean` | 值完全等于 `spot_rdelta_1m_mean`（完全重复，仅 initiation 类存在） |
| `score_base` | score 的计算前身，已有 score，信息完全包含 |
| `spot_flow_confirm_score` | `spot_confirm: true/false` 已是聚合结论，分量分数冗余 |
| `spot_whale_confirm_score` | 同上 |
| `spot_cvd_1m_change` | cvd_pack 提供更精确、更全面的 CVD 数据，此字段冗余 |

> 不同指标的事件字段集不完全相同（如 exhaustion 类无 `spot_rdelta_mean`），去重时忽略不存在的字段。

**initiation 家族额外去重**（仅适用于 `bullish_initiation`、`bearish_initiation`）：

| 字段 | 去除原因 |
|------|---------|
| `follow_through_delta_sum` | 突破后跟随期 delta 累积，偏 entry 质量评估，scan 不需要 |
| `follow_through_hold_ok` | 持有质量评估，偏 entry 决策 |
| `follow_through_minutes` | 同上 |
| `follow_through_max_adverse_excursion_ticks` | 入场后最大回撤，scan 不需要 |
| `spot_cvd_change` | `cvd_pack` 提供更完整的 CVD 数据 |
| `spot_rdelta_1m_mean` | `spot_break_confirm` 已是聚合结论 |

#### 各指标截断条数

| 指标 | 截断规则 | v2 估算大小 |
|------|---------|-----------|
| `absorption` | 最近 20 条 | ~12KB |
| `buying_exhaustion` | 最近 20 条 | ~13KB |
| `selling_exhaustion` | 最近 20 条 | ~13KB |
| `bullish_absorption` | 最近 20 条 | ~12KB |
| `bearish_absorption` | 最近 20 条 | ~12KB |
| `bullish_initiation` | 最近 10 条 | ~6KB |
| `bearish_initiation` | 最近 10 条 | ~6KB |

> 保留 `recent_7d` 内的元信息字段：`event_count`、`history_source`、`lookback_coverage_ratio`、`lookback_covered_minutes`、`lookback_missing_minutes`、`lookback_requested_minutes`

#### `divergence`（特殊结构，v2 新增字段去重）

路径与其他事件不同，保留结构与 v1 相同：
- 顶层保留：`signal`、`signals`、`latest_7d`、`event_count`、`divergence_type`、`likely_driver`、`spot_lead_score`、`pivot_side`、`reason`
- `recent_7d.event_count` + `recent_7d.events` 最近 **20** 条
- 不再保留 `candidates`

**v2 新增：`recent_7d.events[]` 每条去除以下冗余字段**：

| 字段 | 去除原因 |
|------|---------|
| `end_ts` | 值完全等于 `event_end_ts` |
| `start_ts` | 值完全等于 `event_start_ts` |
| `event_id` | 长字符串唯一 ID，LLM 无法引用，无语义价值 |
| `event_available_ts` | 管道内部字段 |
| `price_norm_diff` | 值完全等于 `price_diff` |
| `cvd_norm_diff_fut` | 值完全等于 `cvd_diff_fut` |
| `cvd_norm_diff_spot` | 值完全等于 `cvd_diff_spot` |
| `sig_test_mode` | 值恒为静态配置，非运行时业务信息 |

---

## 四、v2 估算汇总

> 括号内标注变更来源：(v2事件去重) / (v2通用指标替换) / (v2数据源变更)
> 新数据源列基于实测 scan JSON（4MB raw）计算

| 指标 | 新数据源 raw | v2 过滤后 | 节省 | 备注 |
|------|------------|---------|------|------|
| `vpin` | ~2KB | ~2KB | — | |
| `whale_trades` | ~4KB | ~4KB | — | |
| `high_volume_pulse` | ~1KB | ~1KB | — | |
| `tpo_market_profile` | ~7KB | ~7KB | — | |
| `price_volume_structure` | **~1,800KB** | **~2KB** | **~1,798KB** | (数据源变更) levels 1.84MB → 丢弃；va_top_levels 5条 ~0.5KB/窗口 |
| `fvg` | ~29KB | ~1KB | ~28KB | (仅保留 active + nearest) |
| `kline_history` | ~8KB | ~8KB | — | |
| `cvd_pack` | ~22KB | ~22KB | — | |
| `avwap` | ~3KB | ~3KB | — | |
| `rvwap_sigma_bands` | ~3KB | ~3KB | — | |
| `ema_trend_regime` | ~3KB | ~3KB | — | |
| `funding_rate` | ~15KB | ~5KB | ~10KB | (通用指标替换) hourly 140桶 → funding_summary 4标量 |
| `liquidation_density` | ~11KB | ~2KB | ~9KB | (通用指标替换) hourly 140桶 → liq_summary 6标量 |
| `footprint` | ~17KB | ~17KB | — | |
| `orderbook_depth` | ~17KB | ~6KB | ~11KB | (通用指标替换) 全量 levels 近价聚类 → liquidity_walls + OBI |
| `absorption` | ~21KB | ~11KB | ~10KB | (事件去重含v2新增4字段) |
| `buying_exhaustion` | ~23KB | ~12KB | ~11KB | (事件去重含v2新增4字段) |
| `selling_exhaustion` | ~23KB | ~12KB | ~11KB | (事件去重含v2新增4字段) |
| `bullish_absorption` | ~21KB | ~11KB | ~10KB | (事件去重含v2新增4字段) |
| `bearish_absorption` | ~21KB | ~11KB | ~10KB | (事件去重含v2新增4字段) |
| `bullish_initiation` | ~24KB | ~6KB | ~18KB | (仅保留最近10条) |
| `bearish_initiation` | ~23KB | ~6KB | ~17KB | (仅保留最近10条) |
| `divergence` | ~24KB | ~15KB | ~9KB | (去掉 candidates) |
| **合计** | **~2,175KB** | **~219KB** | **~1,956KB** | 主要节省来自 PVS levels 丢弃 |

---

## 五、实现备注（给 scan.rs 开发参考）

v1 备注全部适用，v2 新增：

**v2 新增备注 12：事件字段去重实现**

新增 helper `prune_event_fields(event: &Value) -> Value`，复用已有的 `clone_object_without_keys` 函数：

```rust
const EVENT_DROP_FIELDS: &[&str] = &[
    "event_id", "end_ts", "start_ts", "event_available_ts",
    "indicator_code", "min_follow_required_minutes",
    "strength_score_xmk", "spot_rdelta_mean",
    // v2 新增（与 core v3 对齐）
    "score_base", "spot_flow_confirm_score", "spot_whale_confirm_score", "spot_cvd_1m_change",
];
```

在 `filter_event_indicator` 中，`take_last_n` 返回的每个 event 调用 `prune_event_fields`。字段不存在时忽略（`clone_object_without_keys` 本身跳过不存在的 key）。

**v2 新增备注 13：FVG item 字段去重实现**

新增 helper `prune_fvg_item_fields(item: &Value) -> Value`：

```rust
const FVG_DROP_FIELDS: &[&str] = &["fvg_id", "event_available_ts", "tf"];
```

在 `filter_fvg` 中，仅对 `active_bull_fvgs`、`active_bear_fvgs`（各为数组）、`nearest_bull_fvg`、`nearest_bear_fvg`（各为单对象）调用 `prune_fvg_item_fields`，并补充兼容 alias：`fvg_top = upper`、`fvg_bottom = lower`。

**v2 新增备注 14：Divergence event 字段去重实现**

新增 helper `prune_divergence_event_fields(event: &Value) -> Value`：

```rust
const DIVERGENCE_EVENT_DROP_FIELDS: &[&str] = &[
    "event_id", "end_ts", "start_ts", "event_available_ts",
    "price_norm_diff", "cvd_norm_diff_fut", "cvd_norm_diff_spot",
    "sig_test_mode",
];
```

在 `filter_divergence` 中，对 `latest_7d` 和 `recent_7d.events` 内每条 event 调用 `prune_divergence_event_fields`。

**v2 新增备注 15：PVS levels 去除 + va_top_levels 生成**

在 `filter_price_volume_structure` 中：
1. 对每个保留窗口（15m/4h/1d）的 window 对象调用 `clone_object_without_keys`，丢弃 `"levels"` 和 `"value_area_levels"`
2. 顶层对象同样去掉 `"value_area_levels"`
3. 新增 `va_top_levels` 生成（每个窗口）：

```rust
// 从 value_area_levels 中取 volume 降序前 5 条，按 price 降序输出
fn build_va_top_levels(value_area_levels: &[Value], n: usize) -> Vec<Value> {
    let total_vol: f64 = value_area_levels.iter()
        .filter_map(|l| l["volume"].as_f64()).sum();
    let mut sorted = value_area_levels.to_vec();
    sorted.sort_by(|a, b| b["volume"].as_f64().unwrap_or(0.0)
        .partial_cmp(&a["volume"].as_f64().unwrap_or(0.0)).unwrap());
    let mut top_n = sorted[..n.min(sorted.len())].to_vec();
    top_n.sort_by(|a, b| b["price_level"].as_f64().unwrap_or(0.0)
        .partial_cmp(&a["price_level"].as_f64().unwrap_or(0.0)).unwrap());
    top_n.iter().map(|l| json!({
        "price": l["price_level"],
        "volume": l["volume"],
        "vol_pct": l["volume"].as_f64().unwrap_or(0.0) / total_vol
    })).collect()
}
// scan 每窗口 n=5；core entry 每窗口 n=10/15（见 core v3 §3.1）
```

注意：`va_top_levels` 必须在丢弃 `value_area_levels` **之前**计算，否则数据已被删除。

**v2 新增备注 16：funding_summary 计算实现**

新增函数 `build_funding_summary(recent_7d_events: &[Value]) -> Value`：
- 按时间排序事件，取最近 8h / 24h 的 `funding_rate` 字段计算 EMA（decay = 1 - 2/(n+1)）
- `z_score_7d`：(current - mean_7d) / std_7d，其中 mean/std 由所有事件计算
- `consecutive_direction_hours`：从最新事件向前统计连续同号小时数，负号表示连续负费率

在 `filter_funding_rate` 中：
1. 丢弃 `funding_trend_hourly` 字段
2. 用 `build_funding_summary` 结果插入 `funding_summary` 字段

**v2 新增备注 17：liq_summary 计算实现**

新增函数 `build_liq_summary(recent_7d_events: &[Value]) -> Value`：
- 遍历事件，按时间戳分桶到 24h / 7d 窗口，累加 `long` / `short` 字段
- `ratio_24h` = long_24h / (long_24h + short_24h)，除零时输出 null
- `intensity_z7d`：将 24h 总量与过去 7 天每日总量对比，计算 z-score

在 `filter_liquidation_density` 中：
1. 丢弃 `liq_trend_hourly` 字段
2. 用 `build_liq_summary` 结果插入 `liq_summary` 字段

**v2 新增备注 18：liquidity_walls 计算实现**

新增函数 `build_liquidity_walls(levels: &[Value], mid_price: f64) -> Value`：

```rust
// 1. 直接对全量 levels 做 distance filter：只保留 mid_price ±15% 范围
// 2. 按 bin=0.5% 价格区间分组
// 3. 每组取 bid_liquidity/ask_liquidity 总和及代表价格（加权均价）
// 4. bid_walls: 仅保留 distance_pct ∈ [-15%, 0]，再取 bid_liquidity 最大的前 5 组
// 5. ask_walls: 仅保留 distance_pct ∈ [0, +15%]，再取 ask_liquidity 最大的前 5 组
// 6. depth_imbalance: 直接使用全量 levels，过滤出 price 在 mid±1%/3% 内的档位，求 bid/(bid+ask)
```

在 `filter_orderbook_depth` 中：
1. 用全量 `levels` 直接计算 `liquidity_walls`，取 mid_price = `microprice_fut` 标量
2. 不再生成或依赖 `top100` 中间结果
3. 插入 `liquidity_walls` 字段

---

## 六、验证方法

```python
# 验证去重是否生效
import json

with open("temp_model_input/*_scan_*.json") as f:
    data = json.load(f)

raw = json.dumps(data, separators=(',', ':'))
print(f"minified: {len(raw) / 1024:.1f}KB")  # 目标 < 300KB

inds = data['indicators']

# 事件去重验证（含 v2 新增字段）
EVENT_NAMES = ['initiation', 'bullish_initiation', 'absorption', 'buying_exhaustion',
               'selling_exhaustion', 'bearish_initiation', 'bullish_absorption', 'bearish_absorption']
DROP_FIELDS = ['event_id', 'strength_score_xmk', 'end_ts', 'start_ts', 'event_available_ts',
               'indicator_code', 'score_base', 'spot_flow_confirm_score',
               'spot_whale_confirm_score', 'spot_cvd_1m_change']
for name in EVENT_NAMES:
    events = inds[name]['payload']['recent_7d']['events']
    for e in events:
        for f in DROP_FIELDS:
            assert f not in e, f"{name} still has {f}"
for name in ['initiation', 'bullish_initiation', 'bearish_initiation']:
    events = inds[name]['payload']['recent_7d']['events']
    for e in events:
        for f in ['follow_through_delta_sum', 'follow_through_hold_ok', 'follow_through_minutes',
                  'follow_through_max_adverse_excursion_ticks', 'spot_cvd_change', 'spot_rdelta_1m_mean']:
            assert f not in e, f"{name} still has {f}"

# FVG 去重验证
for win in ['15m', '4h', '1d']:
    fvgs = inds['fvg']['payload']['by_window'][win].get('fvgs', [])
    for fvg in fvgs:
        assert 'fvg_id' not in fvg, f"fvg {win} still has fvg_id"
        assert 'tf' not in fvg, f"fvg {win} still has tf"

# Divergence 去重验证
div_events = inds['divergence']['payload']['recent_7d']['events']
for e in div_events:
    assert 'event_id' not in e
    assert 'price_norm_diff' not in e
    assert 'sig_test_mode' not in e
    assert 'cvd_norm_diff_fut' not in e
    assert 'end_ts' not in e

# PVS levels 去除 + va_top_levels 验证
pvs_payload = inds['price_volume_structure']['payload']
assert 'value_area_levels' not in pvs_payload, "top-level value_area_levels should be removed"
for win in ['15m', '4h', '1d']:
    pvs_win = pvs_payload['by_window'][win]
    assert 'levels' not in pvs_win, f"pvs {win} still has levels"
    assert 'value_area_levels' not in pvs_win, f"pvs {win} still has value_area_levels"
    assert 'va_top_levels' in pvs_win, f"pvs {win} missing va_top_levels"
    assert len(pvs_win['va_top_levels']) <= 5, f"pvs {win} va_top_levels too long"
    # 验证 price 降序
    prices = [l['price'] for l in pvs_win['va_top_levels']]
    assert prices == sorted(prices, reverse=True), f"pvs {win} va_top_levels not price-descending"

# funding_summary 验证
fund = inds['funding_rate']['payload']
assert 'funding_trend_hourly' not in fund, "funding_trend_hourly should be removed"
fs = fund['funding_summary']
assert all(k in fs for k in ['ema_8h', 'ema_24h', 'z_score_7d', 'consecutive_direction_hours'])

# liq_summary 验证
liq = inds['liquidation_density']['payload']
assert 'liq_trend_hourly' not in liq, "liq_trend_hourly should be removed"
ls = liq['liq_summary']
assert all(k in ls for k in ['long_24h', 'short_24h', 'ratio_24h', 'long_7d', 'short_7d', 'intensity_z7d'])

# liquidity_walls 验证
ob = inds['orderbook_depth']['payload']
assert 'top_liquidity_levels' not in ob, "top_liquidity_levels should be removed"
lw = ob['liquidity_walls']
assert 'bid_walls' in lw and 'ask_walls' in lw
assert len(lw['bid_walls']) <= 5 and len(lw['ask_walls']) <= 5
assert 'depth_imbalance_1pct' in lw and 'depth_imbalance_3pct' in lw
assert all(-15 <= wall['distance_pct'] <= 0 for wall in lw['bid_walls'])
assert all(0 <= wall['distance_pct'] <= 15 for wall in lw['ask_walls'])
```
