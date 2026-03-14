# Core（Stage 2）数据源过滤规则

**适用文件**：`systems/llm/src/llm/filter/core.rs`
**适用提示词**：
- `systems/llm/src/llm/prompt/entry/*.txt`
- `systems/llm/src/llm/prompt/management/*.txt`
- `systems/llm/src/llm/prompt/pending_order/*.txt`
**数据来源**：`temp_indicator/{ts}_ETHUSDT.json`（经 `round_derived_fields` 四舍五入后的数据）
**目标大小**：< 500KB，实测估算 ≈ **275KB**（余量充足）
特别注意：过滤聚合后，**所有时间序列、事件序列、小时聚合序列统一 newest-first**，也就是最新数据永远排在最前面。
> **统一用于第二阶段提示词**：无论是 Entry、Management 还是 Pending Order，都会先完成 Stage 1 Scan，再把 `scan` 结果和本文档定义的 `core` 过滤结果一起送入对应的第二阶段提示词。Entry 重点是找入场/止损/目标位；Management 和 Pending Order 在复用同一套结构化指标的同时，还需要额外读取实时上下文。

---

## 一、设计原则

Entry 提示词的核心职责（见 `medium_large_opportunity.txt`）：
- **Entry**：找到真实结构性支撑的入场价（GTC Maker 限价单）
- **SL**：找到能证伪交易逻辑的失效位（必须能命名具体指标字段和数值）
- **TP**：找到价格能真实到达的目标位

**与 Scan 过滤规则的关键差异**：

| 维度 | Scan | Entry (Core) |
|------|------|-------------|
| 历史深度 | 深（判断趋势） | 浅（验证近期方向，找结构位） |
| 精度需求 | 聚合/区间 | 精确价格档位（entry/sl/tp 必须可追溯） |
| Footprint | 聚合不平衡 cluster | **15m 保留关键价格档位明细**（不平衡+OHLC+高成交量档位） |
| PVS | 全量 levels + 丢弃 3d | **全量 levels，保留 3d**（3d PVS 是关键结构位） |
| FVG | 全量含 3d | 全量含 3d（FVG 是核心 TP 目标） |
| 事件指标 | 最近 20 条 | 最近 10 条（Scan 已提供宏观事件上下文） |
| RVWAP | 当前 + Z 序列 | **当前标量只**（σ 带位置是入场位参考，历史轨迹 Scan 已覆盖） |
| EMA | 当前 + regime 序列 | **当前标量只**（趋势方向已由 Scan 确认） |
| AVWAP | 10/5/3 条序列 | **5/3/2 条序列**（当前 AVWAP 位置为主） |

**时间维度要求**（需求第 8 条）：
所有序列数据保留 `ts` 字段；事件数据保留 `event_end_ts`/`event_start_ts`/`confirm_ts`；FVG 保留 `birth_ts`/`left_ts`/`mid_ts`；kline 保留 `open_time`；快照字段（PVS levels、OB levels）不强制添加时间戳，但窗口标签（15m/4h/1d）本身即是时间维度标识。

---

## 二、顶层 JSON 结构

所有第二阶段模式统一保留：
- `indicators`（按本文档过滤后）
- `ts_bucket`（当前快照时间戳）
- `symbol`

仅 `management` / `pending_order` 第二阶段额外保留：
- `trading_state`（实时账户 / 持仓 / 挂单状态）
- `management_snapshot`（LLM 管理上下文）

说明：
- `entry` 第二阶段只需要 `indicators + symbol + ts_bucket`
- `management` 和 `pending_order` 第二阶段必须同时读取 `indicators + trading_state + management_snapshot + symbol + ts_bucket`

---

## 三、各指标过滤规则

### 3.1 完整保留（体积已很小）

| 指标 | 估算大小 | 说明 |
|------|---------|------|
| `vpin` | ~2KB | 顶层标量全量（`vpin_fut`、`vpin_spot`、`toxicity_state`、`z_vpin_fut` 等）+ `by_window`（15m/1d/1h/3d/4h）全量，每个窗口含 `z_vpin_fut`、`z_vpin_spot`、`z_vpin_gap_s_minus_f` |
| `whale_trades` | ~4KB | 顶层标量全量（`fut_whale_delta_notional`、`xmk_whale_delta_notional_gap_s_minus_f` 等）+ `by_window`（15m/1d/1h/3d/4h）全量 |
| `high_volume_pulse` | ~1KB | 含 `as_of_ts`（时间戳）+ `intrabar_poc_price`、`intrabar_poc_volume`、`intrabar_poc_max_by_window` + `by_z_window`（1d/1h/4h，含 `is_volume_spike_z2`/`z3`），全量保留 |
| `tpo_market_profile` | ~6KB | 含 `as_of_ts` + 顶层 TPO 标量（`tpo_poc`、`tpo_vah`、`tpo_val`、`initial_balance_high/low`）+ `by_session`（1d/4h，含 `dev_series`、`tpo_single_print_zones`），全量保留 |

---

### 3.2 价格结构类（精确保留，Entry 直接从这里取 TP/SL 坐标）

#### `price_volume_structure`（~15KB 过滤后）

实际数据路径：`payload.by_window.{window}`（windows: 15m/4h/1d/3d）；顶层有全局 poc/vah/val。

- **保留窗口**：`15m`、`4h`、`1d`、**`3d`**（4 个全部保留）
  - 理由：3d PVS 是关键长期价值区，PVS 数据量极小（15m=24档/4h=20档/1d=16档/3d=14档），保留 3d 总体积仅增加约 2KB
- **每个窗口全量保留**：`poc_price`、`poc_volume`、`vah`、`val`、`hvn_levels`、`lvn_levels`、`levels`（完整价格-成交量分布）、`value_area_levels`、`volume_zscore`、`volume_dryup`、`bar_volume`、`window_bars_used`
- **顶层字段保留**：`poc_price`、`poc_volume`、`vah`、`val`、`hvn_levels`、`lvn_levels`、`levels`、`value_area_levels`、`bar_volume`、`volume_zscore`、`volume_dryup`（当前实时值）
- **理由**：PVS levels 共约 74 个档位（含 3d），体积极小，对 entry 的 SL/TP 精确定位价值极高

#### `fvg`（~27KB 过滤后）

实际数据路径：`payload.by_window.{window}`（windows: 15m/4h/1d/3d）。

- **保留窗口**：`15m`、`4h`、`1d`、`3d`（全部保留，FVG 是核心 TP 目标）
- **每个窗口全量保留**：`fvgs`（含完整字段：`birth_ts`、`left_ts`、`mid_ts`、`event_available_ts`、`upper`、`lower`、`mid`、`side`、`state`、`fill_pct`、`touch_count`、`displacement_score`、`age_bars` 等）、`active_bull_fvgs`、`active_bear_fvgs`、`nearest_bull_fvg`、`nearest_bear_fvg`、`is_ready`、`coverage_ratio`
- **理由**：FVG 边界（`upper`/`lower`/`mid`）是核心 TP 目标（填充 FVG），`birth_ts`/`mid_ts` 提供时间维度，`state`/`fill_pct` 表示 FVG 是否仍有效

#### `avwap`（~1KB 过滤后）

实际数据路径：`payload`（顶层标量）+ `payload.series_by_window.{window}[]`。

- **顶层标量全量保留**：`anchor_ts`（时间维度）、`avwap_fut`、`avwap_spot`、`fut_last_price`、`fut_mark_price`、`lookback`、`price_minus_avwap_fut`、`price_minus_spot_avwap_fut`、`xmk_avwap_gap_f_minus_s`、`zavwap_gap`
- **保留 series_by_window**：`15m`、`4h`、`1d`（丢弃 `1h`、`3d`）
- **截断规则**：
  - `15m`：最近 **5** 条（当前 AVWAP 轨迹，带 `ts`）
  - `4h`：最近 **3** 条（带 `ts`）
  - `1d`：最近 **2** 条（带 `ts`）
- **理由**：Entry 需要当前 AVWAP 位置判断价格相对 VWAP 的偏离，不需要长历史

#### `rvwap_sigma_bands`（~1KB 过滤后）

实际数据路径：`payload.by_window.{window}`（windows: 15m/4h/1d）；`payload.series_by_output_window`（windows: 15m/1h，每个 entry 格式为 `{ts, by_window: {15m:{...}, 4h:{...}, 1d:{...}}}`）。

- **只保留**：`payload.by_window.{15m/4h/1d}` 当前状态标量
- **丢弃**：`series_by_output_window`（历史序列，613 entries，Scan 已覆盖）
- **每个 by_window 窗口保留字段**：`rvwap_w`、`rvwap_sigma_w`、`rvwap_band_minus_1`、`rvwap_band_minus_2`、`rvwap_band_plus_1`、`rvwap_band_plus_2`、`z_price_minus_rvwap`、`samples_used`
- **也保留顶层**：`as_of_ts`（时间维度）、`source_mode`
- **理由**：Entry 需要当前价格处于哪个 σ 区间（-1σ 到 -2σ 是做多入场区），不需要历史 Z 轨迹

---

### 3.3 Footprint（关键差异：15m 保留档位明细）

#### `footprint`（原始 ~11748KB → 过滤后 ~76KB）

实际数据路径：`payload.by_window.{window}`（windows: 15m/1h/4h/3d/1d）。

Footprint 是 Entry 的核心来源之一：不平衡价格档位 = 潜在入场点和 SL 失效点。

**15m 窗口（保留档位明细）**：

保留以下价格档位（带所有字段），排序按 `price_level` 升序：
1. `buy_imbalance = true` 的档位（买方不平衡 → 做多支撑锚点）
2. `sell_imbalance = true` 的档位（卖方不平衡 → 做空阻力锚点）
3. `is_open / is_high / is_low / is_close = true` 的档位（K 线结构关键价格）
4. `ua_top_flag = true / ua_bottom_flag = true` 的档位（未完成拍卖边界）
5. 按 `total` 成交量降序取前 **20** 档（高成交量密集区）

实测：15m 共 301 个档位，过滤后约 **160 个档位（44KB）**。

每个档位保留字段：`price_level`、`buy`、`sell`、`delta`、`total`、`buy_imbalance`、`sell_imbalance`、`is_open`、`is_high`、`is_low`、`is_close`、`ua_top_flag`、`ua_bottom_flag`、`level_rank`

同时保留窗口汇总字段（**全量**）：
- 汇总标量：`window_delta`、`window_total_qty`、`unfinished_auction`、`ua_top`、`ua_bottom`、`stacked_buy`、`stacked_sell`
- **`buy_imbalance_prices`**（预计算的买方不平衡价格列表，59 个价格锚点，约 0.5KB）—— Entry 可直接引用
- **`sell_imbalance_prices`**（预计算的卖方不平衡价格列表，107 个价格锚点，约 1KB）—— Entry 可直接引用
- `buy_stacks`（全量堆叠列表）、`sell_stacks`（全量堆叠列表）、`max_buy_stack_len`、`max_sell_stack_len`

**4h 和 1d 窗口（聚合 imbalance cluster，与 Scan 相同）**：

档位过多（4h=3863，1d=14235），无需明细，用 cluster 表达结构分布：

| 窗口 | 分箱精度 | 输出字段 |
|------|---------|---------|
| `4h` | 0.5 | `buy_imb_clusters [{p,n}]`、`sell_imb_clusters [{p,n}]` |
| `1d` | 1.0 | `buy_imb_clusters [{p,n}]`、`sell_imb_clusters [{p,n}]` |

4h/1d 也保留窗口汇总标量（同 15m，含 `buy_imbalance_prices`/`sell_imbalance_prices`/`stacks`）。

**丢弃窗口**：`1h`、`3d`

---

### 3.4 OrderBook（流动性墙 = TP/SL 参考）

#### `orderbook_depth`（原始 ~11450KB → 过滤后 ~22KB）

实际数据路径：`payload.levels[]`（56152 条）；`payload.by_window.{15m/1h}`；顶层标量。

- **丢弃**：`levels` 原始数组（56152 条）
- **新增**：`top_liquidity_levels`：先过滤 `price_level` 在 **mark_price ± 20%** 范围内（实测约 45460 条），再按 `total_liquidity` 降序取前 **100** 条
  - 保留字段：`price_level`、`bid_liquidity`、`ask_liquidity`、`total_liquidity`、`net_liquidity`、`level_imbalance`
  - 限制价格范围（±20%）确保 100 条都是当前价附近的有效流动性墙，直接可用于 SL/TP 定位；全局 top-100 的价格跨度为 $100–$2974，对 Entry 无意义
- **顶层标量全量保留**：`obi_fut`、`obi`、`obi_k_dw_twa_fut`、`obi_k_dw_adj_twa_fut`、`obi_k_dw_change_fut`、`obi_k_dw_slope_fut`、`obi_l1_twa_fut`、`obi_shock_fut`、`ofi_fut`、`ofi_norm_fut`、`ofi_spot`、`ofi_norm_spot`、`microprice_fut`、`microprice_adj_fut`、`microprice_classic_fut`、`microprice_kappa_fut`、`spot_confirm`、`spot_driven_divergence_flag`、`exec_confirm_fut`、`fake_order_risk_fut`、`heatmap_summary_fut`、`spread_twa_fut`、`spread_twa_spot`、`topk_depth_twa_fut`、`topk_depth_twa_spot`、`weak_price_resp_fut`、`cross_cvd_attribution`、`depth_k`
- **保留 by_window**：`15m` 全量字段；**也保留 `1h`**（含 TWA 系列均值，对 Entry 滑点/流动性 assessment 有参考）；两个窗口都保留全字段

---

### 3.5 趋势基座类（截短历史，Scan 已完成宏观判断）

#### `kline_history`（原始 ~674KB → 过滤后 ~5KB）

实际数据路径：`payload.intervals.{interval}.markets.{market}[]`（interval: 15m/4h/1d/1m；market: futures/spot）。

- **保留 interval**：`15m`、`4h`、`1d`（丢弃 `1m`）
- **保留市场**：`futures` only（丢弃 `spot`）
- **截断规则**：
  - `15m`：最近 **20** 根（5 小时，精确判断近期入场时机，带 `open_time`）
  - `4h`：最近 **15** 根（2.5 天，确认趋势方向，带 `open_time`）
  - `1d`：最近 **10** 根（2 周，主要 S/R 参考，带 `open_time`）
- **字段保留**：`open`、`high`、`low`、`close`、`volume_base`、`open_time`（时间维度必须）、`is_closed`（使用原始字段名，与 Management 阶段保持一致）
- **也保留顶层**：`as_of_ts`

#### `ema_trend_regime`（~1KB 过滤后）

实际数据路径：`payload`（顶层标量）+ `payload.ffill_series_by_output_window.{15m/1h}[]`（15m=613 entries，1h=154 entries）。

- **只保留**：`ema_13`、`ema_21`、`ema_34`、`ema_band_high`、`ema_band_low`、`trend_regime`、`ema_100_htf`（4h/1d 值）、`ema_200_htf`（4h/1d 值）、`trend_regime_by_tf`
- **丢弃**：`ffill_series_by_output_window`（趋势历史序列 613 entries，Scan 已覆盖）
- **也保留**：`as_of_ts`（时间维度）、`output_sampling`
- **理由**：Entry 只需确认当前 EMA 状态和 HTF 多空方向

#### `cvd_pack`（原始 ~574KB → 过滤后 ~10KB）

实际数据路径：`payload`（顶层标量）+ `payload.by_window.{window}.series[]`（15m=672 series）。

- **顶层标量全量保留**：`delta_fut`、`delta_spot`、`relative_delta_fut`、`relative_delta_spot`、`likely_driver`、`spot_flow_dominance`、`spot_lead_score`、`xmk_delta_gap_s_minus_f`、`cvd_slope_fut`、`cvd_slope_spot`
- **保留 by_window**：`15m`、`4h`、`1d`（丢弃 `1h`、`3d`）
- **截断规则（比 Scan 更短）**：
  - `15m`：最近 **15** 条（带 `ts`）
  - `4h`：最近 **10** 条（带 `ts`）
  - `1d`：最近 **5** 条（带 `ts`）
- **每条 series 保留字段**：`ts`、`close_fut`、`close_spot`、`delta_fut`、`delta_spot`、`relative_delta_fut`、`relative_delta_spot`、`cvd_7d_fut`、`spot_flow_dominance`、`xmk_delta_gap_s_minus_f`
- **丢弃**：OHLCV 字段（kline 已有）、`delta_slant_fut`、`delta_slant_spot`、`cvd_7d_spot`

---

### 3.6 按时间重新聚合（重要但体积过大）

#### `funding_rate`（原始 ~1655KB → 过滤后 ~9KB）

实际数据路径：`payload`（顶层标量）+ `payload.recent_7d[]`（5247 entries，字段：`change_ts`、`funding_delta`、`funding_new`、`funding_prev`、`mark_price_at_change`）+ `payload.by_window.{window}`。

- **顶层标量保留**：`funding_current`、`funding_twa`、`mark_price_last`、`mark_price_last_ts`（时间戳）、`mark_price_twap`、`funding_current_effective_ts`（时间戳）
- **`recent_7d` → 按小时聚合** `funding_trend_hourly`（替代 `recent_7d`）
  - 每桶：`{ts, avg_funding}` — `ts` 格式 `YYYY-MM-DDTHH:00:00Z`，`avg_funding` = 该小时 `funding_new` 均值，约 120 桶（8KB）
  - 带 `ts`，模型可看出资金费率的方向性持续趋势（持续负费率 = 看空情绪强化）
- **`by_window` 保留** `15m`、`4h`、`1d`（丢弃 `1h`、`3d`）
  - 每个窗口：`funding_twa`、`change_count`、`changes` 最近 **5** 条（含 `change_ts`）

#### `liquidation_density`（原始 ~1245KB → 过滤后 ~8KB）

实际数据路径：`payload.recent_7d[]`（10080 entries，字段：`ts_snapshot`、`long_total`、`short_total`、`levels_count`、`peak_levels`）+ `payload.by_window.{window}`。

注意：`peak_levels` 在大多数条目中为空列表（`[]`），爆仓聚集数据主要通过 `long_total`/`short_total` 体现；无顶层标量。

- **`recent_7d` → 按小时聚合（只保留非零桶）** `liq_trend_hourly`（替代 `recent_7d`）
  - 每桶：`{ts, long_total, short_total}` — `ts` 为小时截断，只保留 `long_total + short_total > 0` 的桶，约 90–120 个非零桶（5KB）
  - 带 `ts`，模型可识别近期爆仓方向和强度
  - **Entry 用途**：避免把 SL 放在爆仓密集区，且识别被 stop-hunt 的区域
- **`by_window` 保留** `15m`、`4h`、`1d`（丢弃 `1h`、`3d`）
  - 每个窗口：`long_total`、`short_total`、`peak_levels`（全量，含价格锚点，注意实测通常为空）、`coverage_ratio`、`is_ready`、`levels_count`

---

### 3.7 事件列表类（取最近 10 条）

Entry 阶段事件只需近期信号，Scan 已提供宏观事件上下文。

**实际数据路径**：`payload.recent_7d.events[]`（注意：events 嵌套在 `recent_7d` 对象内，而非顶层）。

**顶层元信息保留**（从 `payload.recent_7d` 取）：`event_count`、`lookback_coverage_ratio`。

每个事件保留**全字段**（含所有时间戳字段：`event_start_ts`、`event_end_ts`、`event_available_ts`、`confirm_ts`、`pivot_price`、`score`、`direction`、`type` 等）。

| 指标 | recent_7d.events 数 | 保留规则 | 估算大小 |
|------|-----------|---------|---------|
| `absorption` | 120 | 最近 10 条 | ~10KB |
| `buying_exhaustion` | 91 | 最近 10 条 | ~11KB |
| `selling_exhaustion` | 73 | 最近 10 条 | ~11KB |
| `initiation` | 43 | 最近 10 条 | ~10KB |
| `bullish_absorption` | 58 | 最近 10 条 | ~10KB |
| `bearish_absorption` | 62 | 最近 10 条 | ~10KB |
| `bullish_initiation` | 17 | 全量（17 条，带时间戳） | ~10KB |
| `bearish_initiation` | 26 | 最近 10 条 | ~11KB |

**`divergence`（特殊结构，独立处理）**：

`divergence` 的数据路径与其他事件不同：

```
payload.signal                → bool（当前是否有 divergence 信号）
payload.signals               → {bearish_divergence, bullish_divergence, hidden_bearish_divergence, hidden_bullish_divergence}（各类型信号布尔值）
payload.latest_7d             → 最近 7d 内最新一条 divergence 事件（完整字段含所有时间戳）
payload.event_count           → 事件计数
payload.divergence_type       → 当前 divergence 类型（可为 null）
payload.likely_driver         → 驱动方向（可为 null）
payload.recent_7d.events[]   → 历史 divergence 事件列表，最近 10 条
payload.candidates[]          → 68 个候选事件（按 score 降序取前 5 条，带 price_start/price_end/score/type/sig_pass）
```

保留内容：`signal`、`signals`、`latest_7d`（全字段）、`event_count`、`divergence_type`、`likely_driver`、`spot_lead_score`、`pivot_side`、`reason` + `recent_7d.event_count` + `recent_7d.events` 最近 10 条 + `candidates` 按 `score` 降序前 5 条（保留 `type`、`score`、`sig_pass`、`price_start`、`price_end`、`likely_driver`）。

---

## 四、完整估算汇总

| 指标 | 处理方式 | 估算大小 |
|------|---------|---------|
| `vpin` | 全量 | ~2KB |
| `whale_trades` | 全量 | ~4KB |
| `high_volume_pulse` | 全量 | ~1KB |
| `tpo_market_profile` | 全量 | ~6KB |
| `price_volume_structure` | 全量 levels，保留 3d | ~15KB |
| `fvg` | 全量含 3d | ~27KB |
| `avwap` | 标量 + 5/3/2 条序列（带 ts） | ~1KB |
| `rvwap_sigma_bands` | by_window 当前标量 + as_of_ts | ~1KB |
| `ema_trend_regime` | 当前标量 + as_of_ts | ~1KB |
| `kline_history` | 20/15/10 根 futures，带 open_time | ~5KB |
| `cvd_pack` | 标量 + 15/10/5 条序列（带 ts） | ~10KB |
| `funding_rate` | 标量 + by_window last-5 + hourly agg（带 ts） | ~9KB |
| `liquidation_density` | by_window + hourly 非零桶（带 ts） | ~8KB |
| `footprint` | 15m 明细+预计算数组，4h/1d cluster | ~76KB |
| `orderbook_depth` | 顶层标量 + ±20% top-100 + by_window 15m/1h | ~22KB |
| `absorption` | 最近 10 条（含时间戳） | ~10KB |
| `buying_exhaustion` | 最近 10 条 | ~11KB |
| `selling_exhaustion` | 最近 10 条 | ~11KB |
| `initiation` | 最近 10 条 | ~10KB |
| `bullish_absorption` | 最近 10 条 | ~10KB |
| `bearish_absorption` | 最近 10 条 | ~10KB |
| `bullish_initiation` | 全量（17 条） | ~10KB |
| `bearish_initiation` | 最近 10 条 | ~11KB |
| `divergence` | 特殊结构（signal+signals+latest_7d+top candidates） | ~5KB |
| **合计** | | **≈ 277KB** |

目标 < 500KB，实测 277KB，**余量约 223KB**。

---

## 五、Entry 核心需求验证

| 模型需要确定 | 数据来源 | 精度 |
|------------|---------|------|
| **Entry 价格**（结构性锚点） | Footprint 15m `buy_imbalance_prices`/`sell_imbalance_prices`（直接精确价格列表）、PVS 15m poc/val/vah、FVG 15m 边界（`upper`/`lower`/`mid`）、AVWAP 当前值 | ✅ 精确 |
| **SL 位置**（失效位） | Footprint 15m imbalance 档位（`ua_bottom`/`ua_top`）、PVS val/vah（价值区边界）、`liq_trend_hourly` 爆仓密集时段（带 ts）、RVWAP `z_price_minus_rvwap` | ✅ 精确 |
| **TP 位置**（目标区） | FVG 精确边界（含 3d 长期 FVG）、PVS vah/poc（4h/1d/3d 价值区）、AVWAP 偏差、OB top-100 近价流动性墙（±20% mark price）、RVWAP `rvwap_band_plus_1/2` | ✅ 精确 |
| **方向确认** | EMA `trend_regime_by_tf`、CVD `likely_driver`/`cvd_slope_fut`、whale `fut_whale_delta_notional`、VPIN `toxicity_state`/`z_vpin_fut`、absorption `direction` | ✅ |
| **近期流量信号** | CVD 15m 最近 15 条（带 `ts`）、absorption/exhaustion/initiation 最近 10 条（带 `event_end_ts`） | ✅ 有时间维度 |
| **当前资金情绪** | `funding_current` + `funding_trend_hourly`（带 ts）+ `mark_price_last_ts` | ✅ 有时间维度 |
| **爆仓噪声评估** | `liq_trend_hourly`（带 ts）+ by_window `long_total`/`short_total` | ✅ 有时间维度 |
| **入场时机** | kline 15m 最近 20 根（带 `open_time`）、CVD 15m 最近 15 条（带 `ts`）、absorption/initiation 最近 10 条（带 `event_end_ts`、`confirm_ts`） | ✅ 有时间维度 |
| **Path obstruction（途中阻力）** | FVG `active_bull/bear_fvgs`、PVS `hvn_levels`、OB top-100 流动性墙（±20%）、TPO `tpo_single_print_zones` | ✅ |
| **Divergence（趋势动能背离）** | `divergence.signal`/`signals`/`latest_7d`（带完整时间戳）、top-5 candidates（带 `price_start/price_end`） | ✅ 有时间维度 |

---

## 六、实现备注（给 core.rs 开发参考）

1. **处理顺序**：`round_derived_fields` → 结构过滤。

2. **Footprint 15m 档位过滤**：
   ```
   // 选择条件（OR关系）：
   buy_imbalance || sell_imbalance ||
   is_open || is_high || is_low || is_close ||
   ua_top_flag || ua_bottom_flag ||
   price_level in top_20_by_total_volume

   // 输出按 price_level 升序排列
   // 同时直接输出 buy_imbalance_prices 和 sell_imbalance_prices 数组（预计算，直接保留）
   ```

3. **Footprint 4h/1d cluster 聚合**（与 scan.rs 相同逻辑，可复用）：
   ```
   bin_key = (price / bin_size).round() * bin_size
   输出: [{p: f64, n: usize}]，按 price 升序
   ```

4. **kline 字段名**：保留原始字段名（`open`、`high`、`low`、`close`、`volume_base`、`open_time`、`is_closed`），不做缩写。路径为 `payload.intervals.{interval}.markets.futures[]`。

5. **CVD series 的时间字段 `ts`**：每条 series entry 中 `ts` 必须保留（时间维度要求）。

6. **事件列表路径**：取事件时路径为 `payload.recent_7d.events`，不是 `payload.events`。元信息 `event_count`、`lookback_coverage_ratio` 来自 `payload.recent_7d`（非顶层）。取最近 N 条后统一输出 newest-first，也就是 `event_end_ts` 最新的事件放在数组最前面。

7. **Divergence 特殊处理**：`latest_7d` 是顶层字段（非 `recent_7d` 内），`candidates` 也是顶层，`signals` 也是顶层。仅 `recent_7d.events` 在 `recent_7d` 内。

8. **OB top-100 过滤**：先 `price_level` in `[mark_price * 0.8, mark_price * 1.2]`（±20%），再按 `total_liquidity` desc 取前 100。

9. **Funding hourly 聚合**：按 `change_ts` 截断到小时分组，取 `funding_new` 均值，输出 `{ts, avg_funding}`，约 120 桶，`ts` 格式 `YYYY-MM-DDTHH:00:00Z`，并统一 newest-first。

10. **Liquidation hourly 聚合**：按 `ts_snapshot` 截断到小时分组，对 `long_total`/`short_total` 求和，只保留 `long_total + short_total > 0` 的桶，输出 `{ts, long_total, short_total}`，约 90–120 非零桶。

11. **PVS 保留 3d 窗口**：与 Scan 不同，Core 保留 3d 窗口（约 14 个价格档位，~2KB），它是长期结构位（TP 目标）。
