# Scan 数据源过滤规则

**适用文件**：`systems/llm/src/llm/filter/scan.rs`
**适用提示词**：`systems/llm/src/llm/prompt/scan/medium_large_opportunity.txt`
**数据来源**：`temp_indicator/{ts}_ETHUSDT.json`（经 `round_derived_fields` 四舍五入后的数据）
**目标大小**：< 1MB，实测估算 ≈ **353KB**（余量充足）
特别注意：过滤聚合后，所有保留或新增的时间序列统一按时间 **newest-first** 输出，也就是最新的数据越靠前
> **只为 Scan 服务**：以下规则仅针对 Stage 1 市场扫描提示词的需求设计，不适用于 Entry / Management / Pending Order 阶段。

---

## 一、设计原则

Scan 提示词的核心输出：
- 每个时间框架（15m / 4h / 1d）的趋势方向、结构区间、信号一致性、叙事
- 跨时间框架的流量主偏向与市场叙事

提示词 `BEFORE YOU OUTPUT` 第 4 条明确要求模型评估：
**cvd_pack、whale_trades、vpin、tpo_market_profile、absorption、buying_exhaustion、selling_exhaustion**

数据优先级：
1. **趋势基座（Trend Base）**：OHLCV、EMA 状态、CVD 趋势 → 必须充分
2. **结构区间（Structure）**：PVS、TPO、FVG、Footprint 不平衡区、OB 流动性墙、AVWAP/RVWAP 动态 S/R → 必须完整
3. **流量信号（Flow Context）**：Whale、VPIN、Absorption、Exhaustion、Initiation、Divergence、Funding → 必须覆盖
4. **压缩是第二需求**：只在不损害上述信息质量的前提下压缩

时间序列过长的重要数据，**按时间重新聚合**后保留，不直接丢弃。
所有保留或新增的时序数组统一按 **newest-first** 输出，包括：
- Kline / CVD / AVWAP
- RVWAP `z_series_15m`
- EMA `regime_series_15m`
- Funding / Liquidation 的 hourly 聚合
- 各类事件 `recent_7d.events`
- TPO `dev_series`

---

## 二、顶层 JSON 结构

原始 JSON 顶层字段：`indicator_count`、`indicators`、`message_id`、`msg_type`、`producer`、`published_at`、`routing_key`、`schema_version`、`symbol`、`trace_id`、`ts_bucket`、`window_code`

Scan 输入**只保留**：
- `indicators`（过滤后）
- `ts_bucket`（快照时间戳，供模型定位当前时间）
- `symbol`

其余顶层元信息字段丢弃。

---

## 三、各指标过滤规则

### 3.1 完整保留（体积已很小）

| 指标 | 估算大小 | 说明 |
|------|---------|------|
| `vpin` | ~1KB | 纯标量 + by_window（15m/1h/4h/1d/3d），全量保留 |
| `whale_trades` | ~3KB | 顶层标量 + by_window（15m/1h/4h/1d），全量保留 |
| `high_volume_pulse` | ~1KB | 纯标量，全量保留 |
| `tpo_market_profile` | ~10KB | 含 by_session 1d 和 4h，包含 dev_series（TPO POC/VAH/VAL 的会话内漂移序列），字段全量保留，dev_series 顺序统一 newest-first |

> **tpo_market_profile 重点**：`by_session.1d.tpo_poc/tpo_vah/tpo_val`、`initial_balance_high/low`、`tpo_single_print_zones`（薄弱区）、`dev_series.15m`（31 条 POC 演变序列）均有价值，字段全部保留；时序输出统一 newest-first。

---

### 3.2 保留但裁剪 3d 窗口

Scan 只需 15m / 4h / 1d 三个时间框架，`3d` 窗口对趋势判断无直接贡献。

#### `price_volume_structure`（~15KB 过滤后）

实际数据路径：`payload.by_window.{window}`（windows: 15m/4h/1d/3d）；顶层有全局 poc/vah/val。

- **保留窗口**：`15m`、`4h`、`1d`
- **丢弃窗口**：`3d`
- **每个窗口保留全量字段**：`poc_price`、`poc_volume`、`vah`、`val`、`hvn_levels`、`lvn_levels`、`levels`（完整价格-成交量分布）、`value_area_levels`、`volume_zscore`、`volume_dryup`、`bar_volume`、`window_bars_used`
- **顶层字段保留**：`poc_price`、`poc_volume`、`vah`、`val`、`bar_volume`、`hvn_levels`、`lvn_levels`、`value_area_levels`（当前实时快照值）

#### `fvg`（~26KB 过滤后）

- **保留窗口**：`15m`、`4h`、`1d`
- **丢弃窗口**：`3d`
- **每个窗口保留**：`fvgs`（全量 FVG 列表）、`active_bull_fvgs`、`active_bear_fvgs`、`nearest_bull_fvg`、`nearest_bear_fvg`、`is_ready`、`coverage_ratio`

---

### 3.3 时间序列截断类

#### `kline_history`（原始 ~673KB → 过滤后 ~8KB）

实际数据路径：`payload.intervals.{interval}.markets.futures[]`（interval: 15m/4h/1d/1m；不存在 1h interval）。也保留 `payload.as_of_ts`。

- **保留 interval**：`15m`、`4h`、`1d`（丢弃 `1m`）
- **保留市场**：`futures` only（丢弃 `spot`；CVD pack 已包含 spot OHLCV）
- **截断规则**：
  - `15m`：保留最近 **30** 根（7.5 小时，短期趋势结构充分，带 `open_time`）
  - `4h`：保留最近 **20** 根（≈3.3 天，4h 趋势上下文充分，带 `open_time`）
  - `1d`：保留最近 **14** 根（2 周，1d 宏观趋势，带 `open_time`）
- **输出顺序**：统一 newest-first（最新 bar 在数组最前）
- **字段精简**（字段名缩写，减少 token）：`open`→`o`、`high`→`h`、`low`→`l`、`close`→`c`、`volume_base`→`v`、`open_time`→`t`、`is_closed`→`closed`
- **丢弃**：`volume_quote`（与 volume_base 重复）、`close_time`、`expected_minutes`、`minutes_covered`

#### `cvd_pack`（原始 ~573KB → 过滤后 ~21KB）

- **顶层标量全量保留**：`delta_fut`、`delta_spot`、`relative_delta_fut`、`relative_delta_spot`、`likely_driver`、`spot_flow_dominance`、`spot_lead_score`、`xmk_delta_gap_s_minus_f`、`cvd_slope_fut`、`cvd_slope_spot`
- **保留 by_window**：`15m`、`4h`、`1d`（丢弃 `1h`、`3d`）
- **截断规则**：
  - `15m`：保留最近 **30** 条 series
  - `4h`：保留最近 **20** 条
  - `1d`：全量保留（最多 7 条）
- **输出顺序**：统一 newest-first（最新 series 在数组最前）
- **每条 series 保留字段**：`ts`、`close_fut`、`close_spot`、`delta_fut`、`delta_spot`、`relative_delta_fut`、`relative_delta_spot`、`cvd_7d_fut`、`cvd_7d_spot`、`spot_flow_dominance`、`xmk_delta_gap_s_minus_f`
- **丢弃字段**：`open_fut/spot`、`high_fut/spot`、`low_fut/spot`（kline 已有 OHLCV）、`volume_fut/spot`（kline 已有 volume）、`delta_slant_fut/spot`、`xmk_cvd_gap_s_minus_f`

#### `avwap`（原始 ~124KB → 过滤后 ~3KB）

- **顶层标量全量保留**：`anchor_ts`、`fut_last_price`、`fut_mark_price`、`lookback`、所有价格偏差字段
- **保留 series_by_window**：`15m`、`4h`、`1d`（丢弃 `1h`、`3d`）
- **截断规则**：
  - `15m`：保留最近 **10** 条（2.5 小时 AVWAP 轨迹）
  - `4h`：保留最近 **5** 条（20 小时）
  - `1d`：全量（最多 7 条）
- **输出顺序**：统一 newest-first（最新 series 在数组最前）
- **每条 series 保留字段**：`ts`、`avwap_fut`、`avwap_spot`、`xmk_avwap_gap_f_minus_s`

---

### 3.4 序列提取精简类（重要但历史序列过大）

#### `rvwap_sigma_bands`（原始 ~772KB → 过滤后 ~3KB）

原始 `series_by_output_window.15m` 有 612 条历史序列，每条包含 15m/4h/1d 三个窗口的完整 sigma band 数值（9字段/条）。Z-score 历史可显示"价格相对于 RVWAP 的持续偏离方向"——对信号一致性评分有贡献。

**处理方案**：
- **丢弃**：`series_by_output_window`（612+153 条完整历史）
- **保留**：`by_window.15m/4h/1d` 当前状态标量（9 个字段/窗口，最重要的结构位置信息）
- **新增精简序列 `z_series_15m`**：从 `series_by_output_window.15m` 取最近 **20** 条，每条只保留 `ts` + 三窗口 `z_price_minus_rvwap`

```json
"z_series_15m": [
  {"ts": "2026-03-14T07:45:00Z", "z": {"15m": 0.24, "4h": -0.89, "1d": -1.50}},
  {"ts": "2026-03-14T07:30:00Z", "z": {"15m": 1.22, "4h": -0.91, "1d": -1.48}},
  {"ts": "2026-03-14T07:15:00Z", "z": {"15m": 1.31, "4h": -1.02, "1d": -1.49}}
]
```

实际数据路径：`payload.series_by_output_window.{15m/1h}[]`。每条序列 entry 格式：`{ts, by_window: {15m: {z_price_minus_rvwap, rvwap_w, rvwap_sigma_w, ...}, 4h: {...}, 1d: {...}}}`。提取 z 值时路径为 `entry.by_window.{window}.z_price_minus_rvwap`。

**当前 by_window 保留字段**：`rvwap_w`、`rvwap_sigma_w`、`rvwap_band_minus_1`、`rvwap_band_minus_2`、`rvwap_band_plus_1`、`rvwap_band_plus_2`、`z_price_minus_rvwap`、`samples_used`；也保留顶层 `as_of_ts`、`source_mode`

#### `ema_trend_regime`（原始 ~278KB → 过滤后 ~1KB）

原始 `ffill_series_by_output_window.15m` 有 612 条历史序列，记录每根 15m K 线时的 EMA 趋势状态。趋势状态持续时长对信号一致性判断有价值。

**处理方案**：

实际数据路径：`payload`（顶层标量）+ `payload.ffill_series_by_output_window.{15m/1h}[]`。每条序列 entry 格式：`{ts, trend_regime, by_tf: {4h: {ema_100_htf, ema_200_htf, source_close_ts, trend_regime}, 1d: {...}}}`。

- **丢弃**：`ffill_series_by_output_window`（612+153 条完整历史）
- **保留顶层标量**：`ema_13`、`ema_21`、`ema_34`、`ema_band_high`、`ema_band_low`、`trend_regime`
- **保留 HTF 字典**：`ema_100_htf`（4h/1d 数值）、`ema_200_htf`（4h/1d 数值）、`trend_regime_by_tf`（4h/1d 当前趋势状态）
- **也保留**：`as_of_ts`、`output_sampling`
- **新增精简序列 `regime_series_15m`**：从 `ffill_series_by_output_window.15m` 取最近 **20** 条，每条提取 `ts`、顶层 `trend_regime`，以及 `by_tf.4h.trend_regime`、`by_tf.1d.trend_regime`

```json
"regime_series_15m": [
  {"ts": "2026-03-14T07:45:00Z", "trend_regime": "bull", "trend_regime_4h": "bull", "trend_regime_1d": "bull"},
  {"ts": "2026-03-14T07:30:00Z", "trend_regime": "bull", "trend_regime_4h": "bull", "trend_regime_1d": "bull"}
]
```

---

### 3.5 按时间重新聚合（体积过大但信息重要）

#### `funding_rate`（原始 ~1653KB → 过滤后 ~14KB）

`recent_7d` 有 5238 条逐分钟资金费率变化记录（约 5 天，2026-03-09 至 2026-03-14）。
直接丢弃会损失资金费率的中长期趋势信息——持续负费率（空头主导）或正费率（多头主导）对 4h/1d 趋势判断有实质性贡献。

**处理方案**：
- **`recent_7d` 按小时聚合**，生成 `funding_trend_hourly`（替代 `recent_7d`）
  - 按 `change_ts[..13]`（`YYYY-MM-DDTHH`）分箱
  - 每桶：`{ts, funding_avg（funding_new 均值）, n（变动次数）}`
  - 约 120 个小时桶，8KB，保留完整 5 天资金费率走势

```json
"funding_trend_hourly": [
  {"ts": "2026-03-14T07", "funding_avg": -0.0000188, "n": 35},
  ...
  {"ts": "2026-03-09T08", "funding_avg": -0.0000152, "n": 43}
]
```

- **`by_window` 保留** `15m`、`4h`、`1d`（丢弃 `1h`、`3d`）
  - 每个窗口保留：`funding_twa`、`change_count`、`changes` 最近 **10** 条
  - 每条 change 字段：`change_ts`、`funding_delta`、`funding_new`、`funding_prev`
- **顶层标量保留**：`funding_current`、`funding_twa`、`mark_price_last`、`mark_price_last_ts`（时间戳）、`mark_price_twap`、`funding_current_effective_ts`（时间戳）

#### `liquidation_density`（原始 ~1244KB → 过滤后 ~7KB）

`recent_7d` 有 10080 条逐分钟爆仓密度快照（7 天），其中 2224 条非零（120 个非零小时桶）。
实际数据中爆仓规模较大（如 2026-03-14T06 多头爆仓 550217 ETH），对趋势动力分析有价值。

**处理方案**：
- **`recent_7d` 按小时聚合**，只保留非零桶，生成 `liq_trend_hourly`（替代 `recent_7d`）
  - 按 `ts_snapshot[..13]` 分箱
  - 每桶：`{ts, long（long_total 小时累计）, short（short_total 小时累计）}`
  - 过滤掉 `long + short == 0` 的空桶
  - 约 120 个非零桶，5KB，保留 7 天爆仓方向和规模

```json
"liq_trend_hourly": [
  {"ts": "2026-03-14T07", "long": 469.43,    "short": 1871.47},
  {"ts": "2026-03-14T06", "long": 550217.89, "short": 164.35},
  {"ts": "2026-03-14T05", "long": 84614.94,  "short": 834.68},
  {"ts": "2026-03-14T04", "long": 35772.33,  "short": 83988.40},
  {"ts": "2026-03-14T03", "long": 1832.78,   "short": 298647.23}
]
```

- **`by_window` 保留** `15m`、`4h`、`1d`（丢弃 `1h`、`3d`）
  - 每个窗口保留：`long_total`、`short_total`、`peak_levels`（全量，通常 0-10 条）、`coverage_ratio`

---

### 3.6 大体积结构聚合重构

#### `footprint`（原始 ~11739KB → 过滤后 ~32KB）

`levels` 数组是体积来源（15m=343档，4h=3863档，1d=14235档）。Scan 不需要完整逐档明细，但需要不平衡区位置、窗口汇总和未完成拍卖信号。

- **保留窗口**：`15m`、`4h`、`1d`（丢弃 `1h`、`3d`）
- **丢弃**：`levels` 完整价格档数组
- **保留标量**：`window_delta`、`window_total_qty`、`unfinished_auction`、`ua_top`、`ua_bottom`、`stacked_buy`、`stacked_sell`
- **保留数组（全量）**：`buy_stacks`（3-36条）、`sell_stacks`（13-118条）
- **不平衡价格聚合**：将原始 `buy/sell_imbalance_prices` 按价格分箱，输出 `{p: price, n: count}` 格式

  | 窗口 | 原始 buy/sell 数量 | 分箱精度 | 聚合后约 |
  |------|-----------------|---------|---------|
  | `15m` | 54 / 150 | 0.1 | ~100 bins |
  | `4h` | 674 / 1267 | 0.5 | ~77 bins |
  | `1d` | 1780 / 1442 | 1.0 | ~142 bins |

  字段命名：`buy_imb_clusters`、`sell_imb_clusters`

#### `orderbook_depth`（原始 ~11410KB → 过滤后 ~22KB）

56062 条原始价位档位（\$39 至 \$72700）占全部体积，绝大多数档位 `total_liquidity` 接近 0。

> **注意**：`orderbook_depth` 的 `by_window` **只有 `15m` 和 `1h`**，没有 4h/1d 窗口。这是数据本身特性。OB 结构性 S/R 通过 `top_liquidity_levels` 跨时间框架共享。

- **丢弃**：`levels` 原始数组（56062 条）
- **新增**：`top_liquidity_levels`：按 `total_liquidity` 降序取前 **100** 条
  - 保留字段：`price_level`、`bid_liquidity`、`ask_liquidity`、`net_liquidity`、`level_imbalance`
- **顶层标量全量保留**：`obi_fut`、`obi`、`obi_k_dw_twa_fut`、`obi_k_twa_fut`、`obi_l1_twa_fut`、`ofi_fut`、`ofi_norm_fut`、`microprice_fut`、`microprice_adj_fut`、`spot_confirm`、`exec_confirm_fut`、`fake_order_risk_fut`、`relative_delta_spot`、`cvd_slope_spot`、`cross_cvd_attribution`、`heatmap_summary_fut` 等全部顶层标量
- **保留 by_window**：只保留 `15m`（全量字段，约 37 个标量）；丢弃 `1h`

---

### 3.7 事件列表类（取最近 N 条）

**实际数据路径**：事件嵌套在 `payload.recent_7d.events[]` 内（`recent_7d` 是一个对象，不是数组）。
元信息 `event_count`、`lookback_coverage_ratio` 来自 `payload.recent_7d`（非顶层）。
字段**全量保留**（事件字段是分析信息的核心载体）。
源事件通常按 `event_end_ts` 升序存储；过滤输出统一改为 newest-first，也就是最新事件排在最前。

| 指标 | recent_7d.events 数 | 保留规则 | 估算大小 |
|------|-----------|---------|---------|
| `absorption` | 120 | 最近 20 条 | ~20KB |
| `buying_exhaustion` | 92 | 最近 20 条 | ~22KB |
| `selling_exhaustion` | 73 | 最近 20 条 | ~22KB |
| `initiation` | 43 | 全量 | ~21KB |
| `bullish_absorption` | 58 | 最近 20 条 | ~20KB |
| `bearish_absorption` | 62 | 最近 20 条 | ~20KB |
| `bullish_initiation` | 17 | 全量 | ~18KB |
| `bearish_initiation` | 26 | 最近 20 条 | ~22KB |

> 暂不过滤 `sig_pass=false`，让模型自行评估（`score`、`sig_pass` 均在字段中）。

**`divergence`（特殊结构，独立处理，估算 ~8KB）**：

divergence 数据路径与其他事件完全不同：

```
payload.signal                → bool（当前是否有 divergence 信号）
payload.signals               → {bearish_divergence, bullish_divergence, hidden_bearish_divergence, hidden_bullish_divergence}
payload.latest_7d             → 最近 7d 内最新一条 divergence 事件（含完整字段和时间戳）
payload.event_count           → 事件计数（当前快照为 0 表示当前无新信号）
payload.divergence_type       → 当前 divergence 类型（可为 null）
payload.likely_driver         → spot_led / fut_led（可为 null）
payload.reason                → 无信号原因（如 "no_candidate"）
payload.recent_7d.events[]   → 历史 divergence 事件列表（63 条），取最近 20 条并按 newest-first 输出
payload.candidates[]          → 68 个候选（按 score 降序取前 5 条，含 price_start/price_end/score/type/sig_pass）
```

保留内容：`signal`、`signals`（全量 bool 字典）、`latest_7d`（全字段，含所有时间戳）、`event_count`、`divergence_type`、`likely_driver`、`spot_lead_score`、`pivot_side`、`reason` + `recent_7d.event_count` + `recent_7d.events` 最近 **20** 条 + `candidates` 按 `score` 降序前 **5** 条（保留 `type`、`score`、`sig_pass`、`price_start`、`price_end`、`likely_driver`、`fut_divergence_sign`）。

> divergence 是双 pivot leg 结构，关键价格统一使用 `price_start` / `price_end`，不使用单点 `pivot_price`。

---

## 四、完整估算汇总

| 指标 | 处理方式 | 估算大小 |
|------|---------|---------|
| `vpin` | 全量 | ~1KB |
| `whale_trades` | 全量 | ~3KB |
| `high_volume_pulse` | 全量 | ~1KB |
| `tpo_market_profile` | 全量 | ~10KB |
| `price_volume_structure` | 裁 3d 窗口 | ~15KB |
| `fvg` | 裁 3d 窗口 | ~26KB |
| `kline_history` | 截断 30/20/14 根，futures only，字段缩写 | ~8KB |
| `cvd_pack` | 截断 30/20/7 条，精简字段 | ~21KB |
| `avwap` | 截断 10/5/3 条 | ~3KB |
| `rvwap_sigma_bands` | 当前标量 + 20 条 Z-score 精简序列 | ~3KB |
| `ema_trend_regime` | 当前标量 + 20 条 regime 精简序列 | ~1KB |
| `funding_rate` | 顶层标量 + by_window last-10 + hourly agg（5天） | ~14KB |
| `liquidation_density` | by_window + hourly agg 非零桶（7天） | ~7KB |
| `footprint` | 15m/4h/1d，丢弃 levels，imbalance 分箱聚合 | ~32KB |
| `orderbook_depth` | 顶层标量 + top 100 levels + by_window 15m | ~22KB |
| `absorption` | 最近 20 条事件 | ~20KB |
| `buying_exhaustion` | 最近 20 条事件 | ~22KB |
| `selling_exhaustion` | 最近 20 条事件 | ~22KB |
| `initiation` | 全量（43 条） | ~21KB |
| `bullish_absorption` | 最近 20 条事件 | ~20KB |
| `bearish_absorption` | 最近 20 条事件 | ~20KB |
| `bullish_initiation` | 全量（17 条） | ~18KB |
| `bearish_initiation` | 最近 20 条事件 | ~22KB |
| `divergence` | 特殊结构（signal+signals+latest_7d+recent_7d 20条+top-5 candidates） | ~8KB |
| **合计** | | **≈ 340KB** |

目标 < 1MB，实测 340KB，**余量约 660KB**。

---

## 五、数据充分性核查

| 模型需要判断 | 数据来源 | 覆盖情况 |
|------------|---------|---------|
| **15m 趋势方向** | kline 15m (30根)、ema regime 当前 + 20条序列、cvd 15m (30条) | ✅ |
| **4h 趋势方向** | kline 4h (20根)、ema trend_regime_by_tf 4h + HTF EMA、cvd 4h (20条) | ✅ |
| **1d 趋势方向** | kline 1d (14根)、ema trend_regime_by_tf 1d + HTF EMA、cvd 1d (全量) | ✅ |
| **15m 结构区间** | PVS 15m、FVG 15m、Footprint 15m 不平衡 cluster、AVWAP 15m、RVWAP 15m band、OB top 100 | ✅ |
| **4h 结构区间** | PVS 4h、FVG 4h、TPO 4h session、Footprint 4h cluster、RVWAP 4h band | ✅ |
| **1d 结构区间** | PVS 1d、FVG 1d、TPO 1d session、Footprint 1d cluster、RVWAP 1d band | ✅ |
| **OB 流动性墙** | OB top 100 levels（结构通用）、OB by_window 15m OBI/OFI 标量 | ✅（注：OB 无 4h/1d 子窗口，系数据本身特性）|
| **流量主偏向** | CVD scalars (likely_driver/spot_flow_dominance)、whale_trades by_window、VPIN by_window | ✅ |
| **吸收/主动买卖** | absorption、bullish/bearish_absorption、initiation、bullish/bearish_initiation | ✅ |
| **买卖耗竭** | buying_exhaustion、selling_exhaustion（提示词明确要求） | ✅ |
| **资金费率趋势** | funding_current + by_window TWA + hourly agg（5 天走势） | ✅ |
| **爆仓聚集趋势** | liquidation_density by_window peak_levels + hourly agg 非零桶（7 天） | ✅ |
| **Spot-Futures 背离** | divergence events、cvd xmk_delta_gap、OB cross_cvd_attribution | ✅ |
| **价格相对 RVWAP σ** | RVWAP by_window z_price_minus_rvwap（当前）+ z_series_15m（20条轨迹） | ✅ |
| **TPO 会话结构** | tpo_market_profile 全量（1d + 4h session POC/VAH/VAL/IB/单印区/dev_series） | ✅ |
| **信号一致性** | 所有指标均保留，模型可跨指标对比方向 | ✅ |

---

## 六、实现备注（给 scan.rs 开发参考）

1. **处理顺序**：先对全量数据调用 `round_derived_fields` 四舍五入，再执行本文档的结构过滤和截断。

2. **事件列表取最近 N 条**：事件按 `event_end_ts` 升序存储时，取最后 N 条后统一反转为 newest-first 输出；也就是最新事件在 `events[0]`。

3. **Footprint 不平衡分箱聚合**：
   ```
   BTreeMap<OrderedFloat<f64>, usize>
   bin_key = (price / bin_size).round() * bin_size
   输出: Vec<{p: f64, n: usize}>，按 price 升序
   ```

4. **Funding rate hourly 聚合**：按 `change_ts[..13]`（`YYYY-MM-DDTHH`）分组，取 `funding_new` 均值，输出含 `n`（变动次数）供模型评估置信度。

5. **Liquidation density hourly 聚合**：按 `ts_snapshot[..13]` 分组，对 `long_total` 和 `short_total` 分别求和，过滤掉 `long + short == 0` 的空桶。

6. **RVWAP z_series_15m**：取 `series_by_output_window.15m[-20:]`，每条提取 `ts` + `by_window.{15m,4h,1d}.z_price_minus_rvwap`（注意 by_window key 可能不包含全部三个窗口，做 `if key in entry` 保护）。每条 entry 格式：`{ts, by_window: {15m: {z_price_minus_rvwap, ...}, 4h: {...}, 1d: {...}}}`。

7. **EMA regime_series_15m**：取 `ffill_series_by_output_window.15m[-20:]`，每条提取 `ts`、顶层 `trend_regime`、`by_tf.4h.trend_regime`、`by_tf.1d.trend_regime`。每条 entry 格式：`{ts, trend_regime, by_tf: {4h: {trend_regime, ema_100_htf, ema_200_htf, source_close_ts}, 1d: {...}}}`。输出字段命名：`{ts, trend_regime, trend_regime_4h, trend_regime_1d}`。

8. **OB top_liquidity_levels**：按 `total_liquidity` 降序排序后取前 100（Scan 阶段全局 top-100，无价格范围过滤）。注意 `by_window` 只有 `15m` 和 `1h`，只保留 `15m`，`1h` 丢弃。

9. **kline_history 字段缩写**：仅在 Scan 输入中使用缩写（`o/h/l/c/v/t/closed`）以节省 token；Entry / Management 阶段沿用原始字段名。路径为 `payload.intervals.{interval}.markets.futures[]`。

10. **事件列表路径**：取事件时路径为 `payload.recent_7d.events`，不是 `payload.events`。元信息 `event_count`、`lookback_coverage_ratio` 来自 `payload.recent_7d`。

11. **divergence 特殊处理**：`latest_7d` 是顶层字段（非 `recent_7d` 内），`candidates` 也是顶层，`signals` 也是顶层。仅 `recent_7d.events` 在 `recent_7d` 对象内。
