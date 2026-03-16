# Core Pending（Stage 2）数据源过滤规则 v1

**状态**：草案（已评估，待落地）
**适用代码**：`systems/llm/src/llm/filter/core_pending.rs`
**适用阶段**：LLM Stage 2 — `pending_core`
**目标**：让 LLM 在不被指标结论误导的前提下，清晰判断未成交 maker 单是否仍值得保留，以及当前更优的 `entry / tp / sl / leverage`。

> **实现缺口（落地前必须修复）**
>
> 当前 `core_pending.rs` 实现落后于本规则，以下 5 项必须在代码层对齐 `core_entry.rs` v4：
>
> 1. 将 `EVENT_INDICATOR_RULES` 替换为仅含合并事件数组的版本（去掉 `bullish_absorption` / `bearish_absorption` / `bullish_initiation` / `bearish_initiation` 四个拆分指标）
> 2. 改用 `filter_event_indicator_entry_v3`（额外去除 6 个噪音字段）
> 3. 所有 `insert_filtered_indicator` 调用包裹 `prune_nulls`
> 4. 实现 `events_summary`（含 `minutes_ago`，复用 `build_events_summary`）
> 5. 实现 `pending_entry_evidence`（见第五节）

---

## 一、任务定位

`pending_core` 不是重新找交易，而是回答 4 个问题：

1. 原挂单 thesis 还在不在
2. 当前挂单价是否仍处于真实结构价值区
3. 当前挂单是否太容易被 `15m` 噪音 sweep
4. 是否已经出现更优的 `entry / tp / sl / leverage`

因此，`pending_core` 的数据源应该：

- 比 `entry_core` 更强调 **15m fillability / 微结构**
- 保留 `4h` 作为结构锚点
- `1d` 只保留足够的背景，不要过重
- 不允许指标自己先给出 bull/bear/signal 判决

---

## 二、核心原则

### 2.1 共享原则：沿用 `entry_core v4` 的中性证据层

`pending_core` 必须同步采用 `entry_core v4` 已验证有效的中性化规则：

- 删除 `bullish_absorption` / `bearish_absorption`
- 删除 `bullish_initiation` / `bearish_initiation`
- 删除 `ema_trend_regime.trend_regime*`
- 删除 `divergence.signal / signals`
- 删除 `vpin.toxicity_state`
- 删除 `high_volume_pulse.is_volume_spike_z2 / is_volume_spike_z3`
- 全局不输出 `null`

原则上：

- 保留 **价格、结构、流动性、事件本身**
- 删除 **指标自己的方向标签、阈值标签、判决字段**

### 2.2 `pending_core` 的独特原则

对 `pending_core` 来说，最重要的不是“大方向结论”，而是：

- 当前挂单附近是否真有支撑/阻力/失衡/墙位
- 当前价与挂单价之间的距离，是否已经 spent move
- 挂单被触发后，SL 是否更像噪音位而不是失效位

所以 `pending_core` 的过滤策略应该是：

- `15m` 保留得比 `management_core` 更丰富
- `4h` 保留必要结构锚点
- `1d` 只保留背景锚点，不保留太多近端执行细节

---

## 三、顶层上下文必须保留

必须保留：

- `trading_state`
- `management_snapshot`
- `management_snapshot.pending_order`
- `management_snapshot.position_context.entry_context`
- `management_snapshot.last_management_reason`

如果运行时存在 `positions[]`，也必须保留：

- `management_snapshot.positions[]`
- `management_snapshot.positions[].pnl_by_latest_price`

原因：

- LLM 必须知道这是一张“未成交挂单”而不是新建仓
- LLM 必须看到原 entry 理由，才能判断是不是 spent move / 结构已变

---

## 四、指标过滤建议

### 4.1 `price_volume_structure`

与 `entry_core v4` 一致：

- 保留摘要锚点
- 不保留 `levels`
- 保留 `va_top_levels`
- 保留 `by_window.{15m,4h,1d}`

原因：

- `pending_core` 需要 `POC / VAH / VAL / HVN / LVN` 去判断挂单是不是在真实结构价值区

### 4.2 `fvg`

建议：

- 仅保留 `15m / 4h / 1d`
- `fvgs` 最近 `6`
- `active_bull_fvgs`
- `active_bear_fvgs`
- `nearest_bull_fvg`
- `nearest_bear_fvg`
- `coverage_ratio`
- `is_ready`

单个 FVG 条目沿用 `entry_core v4` 的 9 字段白名单：

- `fvg_bottom`
- `fvg_top`
- `side`
- `state`
- `fill_pct`
- `touch_count`
- `displacement_score`
- `age_bars`
- `width`

理由：

- `pending` 对“挂单价是否卡在失衡带里”高度敏感
- `15m / 4h` 是主用窗口，`1d` 保留最近结构背景即可

### 4.3 `footprint`

建议保留 `15m` 的 `Detailed` 模式，但加入 `entry_core v4` 的长度过滤规则：

- `15m`: `length >= 3`
- `4h`: `length >= 4`
- `1d`: `length >= 7`

保留：

- `buy_stacks`
- `sell_stacks`
- `buy_imb_clusters`
- `sell_imb_clusters`
- `window_delta`
- `window_total_qty`
- `unfinished_auction`
- `ua_top`
- `ua_bottom`
- `stacked_buy`
- `stacked_sell`

理由：

- `pending` 的核心问题是 fillability 和 sweep risk
- 它比 `management` 更需要近端成交量与失衡区信息

### 4.4 `orderbook_depth`

建议：

- 保留 summary 标量
- 保留 `by_window.{15m,1h}`
- `top_liquidity_levels` 建议 `80 - 100`

理由：

- 挂单是否容易被扫掉，和当前墙位距离直接相关
- 对 `pending` 来说，盘口比 `management` 更重要

### 4.5 `kline_history`

建议保留：

- `15m` 最近 `24`
- `4h` 最近 `12`
- `1d` 最近 `8`

理由：

- `pending` 仍需要看到价格是否已走太远
- 但不需要像 raw source 一样给太长 K 线历史

### 4.6 `cvd_pack`

建议保留：

- `15m` 最近 `18`
- `4h` 最近 `8`
- `1d` 最近 `5`

并同步采用 `entry_core v4` 的 null 清理：

- `series[*].xmk_delta_gap_s_minus_f = null` 时不输出该字段

理由：

- `pending` 更关心短线单子还能不能接到
- `15m` delta / cvd 比 `management` 更重要

### 4.7 `avwap`

建议保留：

- 顶层标量
- `15m` 最近 `8`
- `4h` 最近 `3`
- `1d` 最近 `2`

理由：

- 这是挂单是否 still at value 的核心锚点之一

### 4.8 `ema_trend_regime`

建议：

- 只保留 EMA 数值
- 删除所有 `trend_regime*`

保留：

- `as_of_ts`
- `ema_13`
- `ema_21`
- `ema_34`
- `ema_band_high`
- `ema_band_low`
- `ema_100_htf`
- `ema_200_htf`
- `output_sampling.*.ts`

### 4.9 `divergence`

建议：

- 保留 `recent_7d`
- 保留 `latest_7d`
- 保留 `event_count`
- 保留 `candidates`
- 删除 `signal / signals`

### 4.10 `vpin`

建议：

- 保留连续值
- 删除 `toxicity_state`

### 4.11 `high_volume_pulse`

建议：

- 保留 `volume_spike_z_w`
- 删除 `is_volume_spike_z2`
- 删除 `is_volume_spike_z3`

### 4.12 `funding_rate / whale_trades / liquidation_density / rvwap_sigma_bands / tpo_market_profile`

建议：

- 全部继续保留
- 但同步采用 `entry_core v4` 的 null 清理与 TPO 截断规则

尤其是 `tpo_market_profile`：

- `by_session.*.dev_series.15m <= 8`
- `by_session.*.dev_series.1h <= 5`

---

## 五、建议新增的轻量证据字段

`pending_core` 最值得新增的不是方向结论，而是”挂单相对结构”的证据字段。

建议新增一个 `pending_entry_evidence`：

```json
“pending_entry_evidence”: {
  “entry_to_current_price_atr”: 0.42,
  “entry_to_nearest_support_atr”: 0.18,
  “entry_to_nearest_resistance_atr”: 0.56,
  “entry_inside_fvg”: true,
  “entry_inside_value_area”: false,
  “entry_near_orderbook_wall”: true,
  “spent_move_pct_of_atr”: 1.42,
  “rvwap_15m_z_current”: 1.01,
  “entry_anchor_wall_still_present”: true
}
```

### 字段定义

#### 原有字段

| 字段 | 计算方式 | 用途 |
|------|---------|------|
| `entry_to_current_price_atr` | `abs(current_price - entry_price) / atr14_15m` | 当前价距挂单价有多远（ATR 单位） |
| `entry_to_nearest_support_atr` | `abs(entry_price - nearest_bid_wall_price) / atr14_15m` | 挂单价下方最近支撑距离 |
| `entry_to_nearest_resistance_atr` | `abs(nearest_ask_wall_price - entry_price) / atr14_15m` | 挂单价上方最近阻力距离 |
| `entry_inside_fvg` | 挂单价是否落在任意激活 FVG 范围内 | 失衡带内挂单更易成交 |
| `entry_inside_value_area` | 挂单价是否在 4h TPO VAL–VAH 范围内 | 结构价值区确认 |
| `entry_near_orderbook_wall` | 挂单价是否在最近 OB 墙 0.2% 范围内 | 流动性锚点确认 |

#### 新增字段（近15分钟风险专用）

| 字段 | 计算方式 | 用途 |
|------|---------|------|
| `spent_move_pct_of_atr` | `(current_price - entry_price) / atr14_15m`（多单为正，空单取绝对值） | 价格已从挂单位出发走了几个 ATR；>1.5 通常意味着 entry 时机已过 |
| `rvwap_15m_z_current` | 当前价相对 15m RVWAP 的 z-score（直接取 `rvwap_sigma_bands.by_window.15m.z_price_minus_rvwap`） | LLM 可直接判断当前价是否偏离 15m 均衡太远：多单挂价时 z>+1 意味追高；z<-1 意味价格已超卖利于接入 |
| `entry_anchor_wall_still_present` | 当建仓依据中最近的 bid/ask wall 在当前 OB 快照中于挂单价 ±0.3% 范围内仍存在，则为 `true` | 原支撑/阻力墙是否依然有效；`false` 表示 entry thesis 的流动性锚点已消失 |

### 原则

这些字段是**证据层**，不是结论层：

- 不直接输出”该挂/不该挂”
- `spent_move_pct_of_atr > 1.5` 不自动等于”取消挂单”，只是 LLM 的重要输入
- 只帮助 LLM 更稳地判断”这个 entry 现在是不是 still good”

---

## 六、软目标

建议目标：

- minified 体积：`140KB - 190KB`

说明：

- `pending_core` 可以比 `entry_core` 略宽
- 但不应回到旧版 `240KB+` 的状态

---

## 七、验证清单

落地后至少验证：

1. `pending_core` 不再输出 split event 指标（无 `bullish_absorption` / `bearish_absorption` / `bullish_initiation` / `bearish_initiation` 独立数组）
2. `trend_regime / signal / toxicity_state / is_volume_spike_z2/z3` 全部不存在
3. 全文件无 `null`
4. `TPO dev_series` 截断生效
5. `FVG` 单条仅保留 9 字段
6. `footprint` 的 `4h/1d` stacks 满足最小长度
7. `pending_order` 相关顶层上下文完整保留
8. `events_summary` 存在且含 `minutes_ago`
9. `pending_entry_evidence` 存在且含 `spent_move_pct_of_atr` / `rvwap_15m_z_current` / `entry_anchor_wall_still_present` 三个字段

---

## 八、本版明确不做的事

- 不在 `pending_core` 中新增 `Bullish/Bearish/NoTrade` 之类方向字段
- 不把挂单是否有效先计算成 bool 给 LLM
- 不删除 `orderbook_depth`
- 不删除 `kline_history`
- 不删除 `whale_trades`

原则：**给 LLM 更好的结构证据，不替 LLM 做 pending verdict。**
