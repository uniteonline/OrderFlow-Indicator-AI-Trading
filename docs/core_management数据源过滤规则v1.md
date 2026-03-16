# Core Management（Stage 2）数据源过滤规则 v1

**状态**：草案（已评估，待落地）
**适用代码**：`systems/llm/src/llm/filter/core_management.rs`
**适用阶段**：LLM Stage 2 — `management_core`
**目标**：让 LLM 在不被指标直接引导的前提下，判断已有仓位是 `VALID / INVALID / ADJUST`，以及是否需要修改 `tp / sl / add / reduce`。

> **实现缺口（落地前必须修复）**
>
> 当前 `core_management.rs` 实现落后于本规则，以下项目必须在代码层对齐：
>
> **A. `core_management.rs` 自身需修改（5项）**
>
> 1. 将 `EVENT_INDICATOR_RULES` 替换为仅含合并事件数组的版本（去掉 `bullish_absorption` / `bearish_absorption` / `bullish_initiation` / `bearish_initiation` 四个拆分指标）
> 2. 改用 `filter_event_indicator_entry_v3`（额外去除 6 个噪音字段）
> 3. 所有 `insert_filtered_indicator` 调用包裹 `prune_nulls`
> 4. 实现 `events_summary`（含 `minutes_ago`，复用 `build_events_summary`）
> 5. 实现 `position_evidence`（见第五节）
>
> **B. `core_shared.rs` 共享函数存在禁止字段泄漏风险（2项，影响 management / pending / entry）**
>
> 6. `filter_ema_trend_regime` 的 `copy_fields` 白名单里包含 `trend_regime` 和 `trend_regime_by_tf`（禁止字段）。management 没有 `prune_nulls` 兜底，一旦指标引擎输出非 null 值，这两个字段会直接泄漏进 LLM 输入。需从白名单中删除。
> 7. `filter_divergence` 的 `copy_fields` 白名单里包含 `signal` 和 `signals`（禁止字段）。同上，需从白名单中删除。

---

## 一、任务定位

`management_core` 的目标不是重新找交易，而是回答：

1. 原交易 thesis 还在不在
2. 当前恶化是结构失效，还是只是该容忍的噪音
3. `tp / sl` 是否需要重估
4. 是否应该 `add / reduce / close`

因此它和 `entry_core / pending_core` 的区别是：

- 更强调 `4h / 1d thesis`
- 更强调当前仓位上下文
- 更强调当前位置相对 `SL / TP / 结构区间`
- 不需要像 `pending` 那样保留过多 fillability 微结构

---

## 二、核心原则

### 2.1 共享原则：同步采用 `entry_core v4` 的中性证据层

`management_core` 必须同步采用以下规则：

- 删除 `bullish_absorption` / `bearish_absorption`
- 删除 `bullish_initiation` / `bearish_initiation`
- 删除 `ema_trend_regime.trend_regime*`
- 删除 `divergence.signal / signals`
- 删除 `vpin.toxicity_state`
- 删除 `high_volume_pulse.is_volume_spike_z2 / is_volume_spike_z3`
- 全局不输出 `null`

原则上：

- 保留对 thesis 审查有用的结构证据
- 不保留任何会先把仓位判成“还有效/已失效”的结论字段

### 2.2 `management_core` 的独特原则

它最重要的不是“找新 entry”，而是：

- 当前仓位离真正失效位还有多远
- 现价和当前 `tp/sl` 的关系是否还合理
- `15m` 恶化是否值得行动，还是只是噪音

所以它的数据源应该：

- 比 `pending_core` 更重 `4h / 1d`
- 比 `pending_core` 更轻 `15m` fillability
- 更重仓位相对结构的证据

---

## 三、顶层上下文必须保留

必须保留：

- `trading_state`
- `management_snapshot`
- `management_snapshot.positions[]`
- `management_snapshot.positions[].pnl_by_latest_price`
- `management_snapshot.positions[].current_tp_price`
- `management_snapshot.positions[].current_sl_price`
- `management_snapshot.position_context`
- `management_snapshot.position_context.entry_context`

如果存在挂单联动，也建议保留：

- `management_snapshot.pending_order`

原因：

- `management` 必须看到现有仓位和原始 thesis
- 它不是纯市场分析，而是“仓位+市场”的联合审查

---

## 四、指标过滤建议

### 4.1 `price_volume_structure`

与 `entry_core v4` 一致：

- 保留摘要锚点
- 不保留 `levels`
- 保留 `va_top_levels`
- 保留 `by_window.{15m,4h,1d,3d}` 中仍有价值的摘要字段

原因：

- `management` 需要重新审视当前仓位在 4h/1d auction 中所处的位置

### 4.2 `fvg`

建议：

- 默认保留 `15m / 4h`
- `15m / 4h` 保留 `fvgs` 最近 `4`
- `15m / 4h` 保留 `active_bull_fvgs`
- `15m / 4h` 保留 `active_bear_fvgs`
- `15m / 4h / 1d` 保留 `nearest_bull_fvg`
- `15m / 4h / 1d` 保留 `nearest_bear_fvg`
- `coverage_ratio`
- `is_ready`

单个 FVG 条目同样只保留 9 字段白名单。

理由：

- `management` 更关心当前仍有效的失衡区是否挡住 `TP` 或失效 `SL`
- 不需要像 `entry/pending` 那样保留过多 FVG 历史
- 但为了识别更高周期 opposing zone，建议额外保留 `1d nearest_bull_fvg / nearest_bear_fvg` 作为轻量上下文
- 不保留 `1d active_*_fvgs` 和 `1d fvgs` 历史数组，避免体积膨胀和噪音回流

### 4.3 `footprint`

建议保留 `Defensive` 模式，并同步长度过滤：

- `15m >= 3`
- `4h >= 4`
- `1d >= 7`

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

- `management` 关心的是 thesis 是否被破坏
- 不需要像 `pending` 一样围绕 maker fillability 展开

### 4.4 `orderbook_depth`

建议：

- 保留 summary 标量
- 保留 `by_window.{15m,1h}`
- `top_liquidity_levels` 建议保留前 `60`

理由：

- 够用来判断当前 `TP/SL` 前方是否出现新的强墙位
- 不需要像 `pending` 那么宽

### 4.5 `kline_history`

建议保留：

- `15m` 最近 `16`
- `4h` 最近 `15`
- `1d` 最近 `10`

理由：

- `management` 需要更看重高周期的结构延续性

### 4.6 `cvd_pack`

建议保留：

- `15m` 最近 `12`
- `4h` 最近 `10`
- `1d` 最近 `6`

并同步 null 清理。

理由：

- `management` 更关注 thesis 是否在中周期上被 flow 否定

### 4.7 `avwap`

建议保留：

- 顶层标量
- `15m` 最近 `3`
- `4h` 最近 `2`
- `1d` 最近 `2`

### 4.8 `ema_trend_regime`

建议：

- 只保留 EMA 数值
- 删除 `trend_regime*`

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
- 同步采用 `entry_core v4` 的 TPO 截断和 null 清理

尤其是 `tpo_market_profile`：

- `by_session.*.dev_series.15m <= 8`
- `by_session.*.dev_series.1h <= 5`

### 4.13 `events_summary`

`management_core` 必须实现 `events_summary`，用于把近端事件的时间权重显式暴露给 LLM。

建议结构：

```json
"events_summary": {
  "payload": {
    "most_recent_absorption": {
      "confirm_ts": "2026-03-16T12:06:00Z",
      "minutes_ago": 15,
      "direction": "bearish",
      "type": "bearish_absorption",
      "price": 2293.0,
      "price_distance_atr": 0.42,
      "score": 0.64,
      "sig_pass": true
    },
    "most_recent_initiation": { "...": "..." },
    "most_recent_buying_exhaustion": { "...": "..." },
    "most_recent_selling_exhaustion": { "...": "..." },
    "most_recent_divergence": { "...": "..." }
  }
}
```

必须包含：

- `most_recent_absorption`
- `most_recent_initiation`
- `most_recent_buying_exhaustion`
- `most_recent_selling_exhaustion`
- `most_recent_divergence`

单条摘要允许保留的字段：

| 字段 | 计算方式 | 说明 |
|------|---------|------|
| `confirm_ts` | 直接取事件原始字段 | 事件确认时间 |
| `minutes_ago` | `floor((current_ts - confirm_ts) / 60)` | 距当前时间的分钟数，帮助 LLM 判断事件是否仍有约束力 |
| `direction` | 直接取事件原始字段（`bullish` / `bearish`） | 事件方向 |
| `type` | 直接取事件原始字段 | 事件类型（如 `bearish_absorption`） |
| `price` | 直接取事件原始 `pivot_price` | 事件发生的价格锚点；divergence 无此字段时合法省略 |
| `price_distance_atr` | `abs(pivot_price - current_mark_price) / atr14_15m` | 事件价格锚点距当前市价的距离（ATR 单位）；值越小表示该事件对当前价格仍有约束力；divergence 无 pivot_price 时合法省略 |
| `score` | 直接取事件原始字段 | 事件强度评分 |
| `sig_pass` | 直接取事件原始字段 | 是否通过信号阈值 |

原则：

- 这是事件索引摘要，不是事件判决
- 不允许新增 `dominant_recent_event`、`recent_event_bias`、`most_important_event` 这类结论字段
- 若某类最近事件没有可用价格锚点，可合法省略 `price / price_distance_atr`
- 若某类事件当前不存在，则省略该子字段，不输出 `null`

用途：

- 帮 LLM 快速识别最近 `15m` 风险是刚发生，还是已经钝化
- 帮 LLM 区分 `4h / 1d thesis` 仍在，但 `15m` 是否刚出现新的恶化迹象

---

## 五、建议新增的轻量证据字段

`management_core` 最值得新增的是”当前仓位相对结构”的证据字段，而不是算法先给 `VALID/INVALID/ADJUST`。

建议新增一个 `position_evidence`：

```json
“position_evidence”: {
  “price_to_entry_atr”: 0.84,
  “price_to_current_sl_atr”: 0.56,
  “price_to_current_tp_atr”: 1.43,
  “current_sl_beyond_nearest_structure”: true,
  “current_tp_before_next_major_resistance”: false,
  “position_in_4h_tpo_pct”: 0.73,
  “position_in_1d_range_pct”: 0.41,
  “thesis_bars_elapsed_4h”: 3
}
```

### 字段定义

#### 原有字段

| 字段 | 计算方式 | 用途 |
|------|---------|------|
| `price_to_entry_atr` | `abs(current_price - entry_price) / atr14_15m` | 当前价距入场价有多远（ATR 单位），帮助区分正常回撤 vs 趋势失效 |
| `price_to_current_sl_atr` | `abs(current_price - current_sl_price) / atr14_15m` | 当前价距 SL 有多远；<0.5 表示 15m 噪音已逼近 SL，需要判断是否是真实失效信号 |
| `price_to_current_tp_atr` | `abs(current_tp_price - current_price) / atr14_15m` | 当前价距 TP 有多远；帮助判断 TP 是否还现实 |
| `current_sl_beyond_nearest_structure` | 见下方计算基准说明 | `true` 表示 SL 仍在最近结构支撑/阻力的外侧，止损位有结构意义；`false` 表示 SL 已夹在两个结构区之间，止损位失去结构意义 |
| `current_tp_before_next_major_resistance` | 见下方计算基准说明 | `true` 表示 TP 在下一个主要阻力区之前，触达路径畅通；`false` 表示 TP 已超越主阻力，触达概率降低 |
| `position_in_1d_range_pct` | `(current_price - 1d_tpo_val) / (1d_tpo_vah - 1d_tpo_val)`，值域 0–1（超出区间可超过此范围） | 仓位在 1d TPO VAL–VAH 区间中的位置，帮助判断 1d 结构背景 |

#### 新增字段

| 字段 | 计算方式 | 用途 |
|------|---------|------|
| `position_in_4h_tpo_pct` | `(current_price - 4h_tpo_val) / (4h_tpo_vah - 4h_tpo_val)`，值域 0–1（超出区间可超过此范围） | 仓位在 4h TPO VAL–VAH 区间中的位置；>0.85 表示接近4h顶部，多单需谨慎加仓；<0.15 表示接近4h底部，空单需谨慎加仓。**计算基准明确为 4h TPO VAL-VAH，避免与 K 线高低点或 IB 混淆** |
| `thesis_bars_elapsed_4h` | `floor((current_ts - thesis_start_ts) / 4h)` | 入场以来已经过了几根 4h 棒；0–2 表示 thesis 刚成立，可容忍更多回调；≥8 表示 time-decay 风险上升，如果价格仍未靠近 TP，需重新审视 thesis 有效性 |

### `current_sl_beyond_nearest_structure` 计算基准

多单（SL 在入场价下方）按以下优先级查找"最近支撑"：

1. 最近的 4h PVS HVN，且该 HVN 与 SL 价格距离 ≤ 1 ATR
2. 4h TPO VAL（当前4h session）
3. 最近 OB bid wall，且距 SL 价格 ≤ 0.3%

空单（SL 在入场价上方）方向对称，依次替换为 HVN → 4h TPO VAH → OB ask wall。

判定规则：

- 多单：若 SL < 上述找到的最近支撑价，则 `true`（SL 在支撑外侧）；否则 `false`
- 空单：若 SL > 上述找到的最近阻力价，则 `true`；否则 `false`
- 若以上三个候选均未找到有效锚点，则**省略该字段**（不输出 `null`）

---

### `current_tp_before_next_major_resistance` 计算基准

多单（TP 在入场价上方）按以下优先级查找"最近主阻力"：

1. 最近的 active bear FVG `fvg_top`，且与 TP 价格距离 ≤ 1 ATR
2. 最近 OB ask wall，且距 TP 价格 ≤ 0.5%
3. 4h TPO VAH（当前4h session）

空单（TP 在入场价下方）方向对称，依次替换为 bull FVG `fvg_bottom` → OB bid wall → 4h TPO VAL。

判定规则：

- 多单：若 TP < 上述找到的最近阻力价，则 `true`（TP 在阻力前方）；否则 `false`
- 空单：若 TP > 上述找到的最近支撑价，则 `true`；否则 `false`
- 若以上三个候选均未找到有效锚点，则**省略该字段**（不输出 `null`）

---

### `position_in_4h_tpo_pct / position_in_1d_range_pct` 计算保护

这两个字段必须遵守以下规则：

- 只有当 `val` 和 `vah` 都存在时才计算
- 只有当 `vah > val` 时才计算
- 若 `vah <= val`、任一端点缺失、或目标 session 不存在，则**直接省略字段**
- 禁止输出 `null`
- 禁止用 `0` 或其他默认值代替缺失

目的：

- 让 LLM 读到的是有效位置证据，而不是被异常分母污染后的伪数值
- 保持与本规则“全文件无 `null`”一致

### `thesis_bars_elapsed_4h` 的明确时间源

`thesis_bars_elapsed_4h` 必须按以下优先级取时间源：

1. `management_snapshot.positions[].opened_at` 或等价的真实开仓时间
2. `management_snapshot.position_context.entry_context.analysis_ts`
3. 若以上都不存在，则**不输出该字段**

禁止做法：

- 禁止用 `ts_bucket - 1根K线` 之类的推测值代替
- 禁止用 `entry_reason` 文本中解析出的模糊时间代替
- 禁止输出 `null`

说明：

- 该字段的价值在于量化 thesis 的时间衰减
- 只有在存在稳定时间源时，它才是证据；否则会变成误导

### 原则

这些字段是**证据层**，不是结论层：

- 不允许直接输出 `trade_invalid=true` 这类字段
- `price_to_current_sl_atr < 0.5` 不自动等于”立即止损”，只是 LLM 的重要输入
- `thesis_bars_elapsed_4h >= 8` 不自动等于”平仓”，只是 time-decay 的量化参考
- 帮模型判断现在的 `SL` 是不是 still structural
- 帮模型判断 `TP` 是否已经被新形成的 opposing zone 挡住
- 帮模型区分”仓位恶化”与”只是正常回撤”

### 数据契约补充要求

若要稳定输出 `thesis_bars_elapsed_4h`，运行时或管理快照需要补一项明确时间字段：

- `management_snapshot.positions[].opened_at`
或
- `management_snapshot.position_context.entry_context.analysis_ts`

若短期内数据契约不扩展，则本版允许：

- 先实现 `position_evidence` 的其他字段
- 暂时省略 `thesis_bars_elapsed_4h`

但不允许：

- 在没有可靠时间源的情况下伪造该字段

---

## 六、软目标

建议目标：

- minified 体积：`130KB - 180KB`

说明：

- `management_core` 应该略宽于 `entry_core`
- 但应明显轻于旧版 `260KB+`

---

## 七、验证清单

落地后至少验证：

1. `management_core` 不再输出 split event 指标（无 `bullish_absorption` / `bearish_absorption` / `bullish_initiation` / `bearish_initiation` 独立数组）
2. `trend_regime / signal / toxicity_state / is_volume_spike_z2/z3` 全部不存在（含 `core_shared.rs` 修复后）
3. 全文件无 `null`
4. `FVG` 单条只保留 9 字段
5. `TPO dev_series` 截断生效
6. `4h / 1d footprint` 短 stacks 已清理
7. `management_snapshot.positions[].current_tp_price / current_sl_price` 仍完整保留
8. `events_summary` 存在，且至少覆盖 `absorption / initiation / buying_exhaustion / selling_exhaustion / divergence` 五类中的可计算项，并含 `minutes_ago`
9. `position_evidence` 存在且含所有可计算字段，包括 `position_in_4h_tpo_pct`（非旧的 `position_in_4h_range_pct`）；若没有稳定时间源，可合法省略 `thesis_bars_elapsed_4h`
10. `core_shared.rs` 的 `filter_ema_trend_regime` 和 `filter_divergence` 不再 copy 禁止字段
11. `position_in_4h_tpo_pct / position_in_1d_range_pct` 在 `vah <= val` 或端点缺失时必须省略，不能输出 `null`

---

## 八、本版明确不做的事

- 不在 `management_core` 中直接输出 `VALID / INVALID / ADJUST`
- 不把是否该加仓/减仓先算成 bool 给模型
- 不删除 `orderbook_depth`
- 不删除 `kline_history`
- 不删除 `whale_trades`

原则：**给 LLM 更好的仓位审查证据，不替 LLM 先做管理判决。**
