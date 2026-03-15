# Core（Stage 2）数据源过滤规则 v3

**状态**：草案
**适用代码**：`systems/llm/src/llm/filter/core_entry.rs`（PVS 规则同时适用于 `management_core / pending_core`）
**适用阶段**：LLM Stage 2 — `entry_core` 为主；`management_core / pending_core` 至少同步采用 PVS 压缩规则
**基线样本**：
- `systems/llm/temp_model_input/20260315T034500Z_ETHUSDT_entry_core_20260315T034710759Z.json`
- 该样本为 entry_core，已按 v2 规则过滤，minified 约 400KB

---

## 一、v3 的目标

v2 的过滤规则基于对字段语义的推理，部分假设在实测样本里是错的。
v3 的所有变更均来自对 entry_core 实测样本的字节级分析，**每条规则都有实测数据支撑**。

变更原则：
- **不以统计量替代事件数据**（spike / 脉冲会被均值掩盖）
- **不以"语义重叠"为由删除字段**（需验证数据是否真的重复）
- **只删或压缩经实测确认为冗余的部分**

---

## 二、v2 关键假设的实测核验结果

| v2 假设 | 实测结果 | 结论 |
|---------|---------|------|
| `footprint.buy/sell_imbalance_prices` 与 `levels[].buy_imbalance` 重复 | **初步假设错误**：buy_imbalance_prices 有 117 条，levels 过滤后只有 20 条 buy_imbalance=true；但进一步核验发现，levels 的 top-20 by total 已覆盖高量孤立 imbalance，`buy/sell_stacks` 覆盖连续强区间，两者合计可替代原始价格列表 | ✅ 可删（由 stacks + levels 覆盖） |
| `liq_trend_hourly` 可用 long_avg/short_avg 替代 | **假设错误**：实测发现单小时 short_total=460,288，其余小时均 <25,000，by_window 聚合完全掩盖此脉冲 | ❌ 不能替换成标量，改截断 |
| `cvd_pack.by_window.*.series` 可由顶层斜率替代 | **假设错误**：by_window 内无独立窗口标量，series 是唯一的窗口级时序数据 | ❌ 不能删，改精简字段 |
| `value_area_levels` 与 poc/vah/val 重复 | **部分正确**：vah/val/poc 已是边界，但 1d/3d 的 hvn_levels 为空，value_area_levels 是唯一的 VA 内部分布信息；实测 3d top-5 为 2090/2080/2130/2100/2098，均为整数关口，直接取 top-N by volume 比分箱更准确 | ✅ 替换为 `va_top_levels` |
| `funding_trend_hourly` 可由 by_window TWA 替代 | **基本正确**：by_window.{15m/4h/1d}.funding_twa 已覆盖方向，hourly 数据无脉冲事件 | ✅ 可截断压缩 |

---

## 三、v3 新增优化规则（相对 v2）

> **统一原则**：所有时间序列、事件序列、小时聚合序列，均以**最新数据在最前**（newest-first）输出。

---

### 3.1 `price_volume_structure.value_area_levels` → `va_top_levels`（top-N by volume）

> 这条 PVS 规则不只用于 `entry_core`，`management_core / pending_core` 也必须同步采用；否则在 full PVS 源数据下体积会明显失控。

**v2 现状**：各窗口完整输出 {price_level, volume} 数组
**实测体积**：15m=453 条，4h=1307 条，1d=2539 条，3d=7766 条，合计 ~136KB
**问题**：3d 窗口 7766 条是 val=2054.7 到 vah=2132.35 按 $0.01 步长全展开；实测 top-5 为 `2090, 2080, 2130, 2100, 2098`，均为整数关口

**为何不用分箱 cluster**：分箱以"价格步长"为轴，bin center 未必落在真实高量价位上；直接 top-N by volume 更准确，模型一看即懂，且可直接作为 TP/SL 锚点引用。

**v3 规则**：将 `value_area_levels` 替换为 `va_top_levels`，取 volume 降序前 N 条：

| 窗口 | 保留条数 |
|------|---------|
| 15m | top **15** |
| 4h | top **10** |
| 1d | top **10** |
| 3d | top **10** |

每条输出结构：
```json
{
  "price": 2090.0,
  "volume": 44419.6,
  "vol_pct": 0.08,
  "vol_rank": 1
}
```

- `vol_pct`：该档占本窗口 value area 总成交量的比例
- `vol_rank`：按 volume 降序的名次，`1` 表示本窗口 value area 内成交量最高的价位
- 输出按 `price` **降序**排列（高价在前 = 阻力到支撑顺序）

注意：
- `va_top_levels` 不是单纯价格列表，必须保留 `price + volume + vol_pct + vol_rank`
- `price` 解决“价位在哪里”，`volume / vol_pct` 解决“这个价位的重要性有多高”
- 对 `entry / tp / sl` 来说，这两部分信息缺一不可；只保留价格会丢掉强弱排序，只保留成交量会失去可执行的定价锚点

**实测验证**（3d 窗口 top-5）：

| price | volume | vol_pct |
|-------|--------|---------|
| 2130.0 | 38530.5 | 0.07 |
| 2100.0 | 34592.1 | 0.06 |
| 2098.0 | 31298.5 | 0.06 |
| 2090.0 | 44419.6 | 0.08 |
| 2080.0 | 39758.1 | 0.07 |

**理由**：
- poc/vah/val 已是边界，`va_top_levels` 补充 VA 内部的次级密集锚点
- hvn_levels 在 1d/3d 窗口为空，`va_top_levels` 是这两个窗口唯一的 VA 内部结构信息
- 从 ~136KB 压缩到 ~2KB，锚点质量更高（直接是成交最密集的价格位）
- 对模型来说，`price + volume` 比单纯价格列表更适合做 `entry / tp / sl` 优先级判断

---

### 3.2 `funding_rate.funding_trend_hourly` → 截断到 24h + 统计摘要

**v2 现状**：140 个小时桶，约 9.9KB，newest-first
**实测**：顶层已有 `funding_current`、`funding_twa`；`by_window.{15m/4h/1d}.funding_twa` 已覆盖短/中/长期方向

**v3 规则**：
1. `funding_trend_hourly` 截断为最近 **24 个小时桶**（newest-first）
2. 新增 `funding_trend_stats` 对象，基于原始 140 桶计算：

```json
"funding_trend_stats": {
  "avg_7d": -4.1e-05,
  "slope_per_hour": -0.000001,
  "pct_negative_hours": 0.68
}
```

- `avg_7d`：140 桶 avg_funding 均值
- `slope_per_hour`：最小二乘斜率（正=费率上升趋势，负=下降趋势）
- `pct_negative_hours`：140 桶中 avg_funding < 0 的占比

**理由**：
- funding 对 entry 是方向情绪背景，不是直接价格锚点
- 24h 桶保留最近一天的节奏感，统计量覆盖 7 日趋势
- 节省约 8KB（80% 压缩）

---

### 3.3 `liquidation_density.liq_trend_hourly` → 截断到 48h + spike 索引

**v2 现状**：140 个小时桶，约 9KB，newest-first
**实测关键发现**：
```
2026-03-15T03:00Z  long=14,277    short=826
2026-03-15T02:00Z  long=10,007    short=460,288   ← 单小时脉冲
2026-03-15T01:00Z  long=22,147    short=9,601
```
`by_window.4h.short_total=476,791` 完全掩盖了这 46 万全部集中在 02:00 一小时的事实。
模型需要知道这个时间点，才能判断 02:00 是否是 sweep 反转锚点、SL 是否应放在该价位下方。

**v3 规则**：
1. `liq_trend_hourly` 截断为最近 **48 个小时桶**（newest-first）
2. 新增 `liq_spike_events`：从完整 140 桶中提取 `long_total + short_total > 均值 × 3` 的桶（最多 10 个，newest-first）：

```json
"liq_spike_events": [
  {
    "ts": "2026-03-15T02:00:00Z",
    "long_total": 10007.3,
    "short_total": 460288.1,
    "dominant": "short"
  }
]
```

- `dominant`：`"long"` / `"short"` / `"mixed"`（哪侧爆仓量更大）
- 如果 48h 内所有桶均无脉冲，`liq_spike_events` 输出空数组

**理由**：
- 爆仓脉冲是结构性事件，直接影响 SL 定位（避免把 SL 放在 sweep 区内）
- 48h 覆盖最近两个交易日，足够 entry 决策
- 节省约 6KB（65% 压缩），保留 spike 定位能力

---

### 3.4 `cvd_pack.by_window.*.series` → 字段精简（保留序列，删冗余字段）

**v2 现状**：每条 series entry 10 个字段，15m=15 条，4h=8 条，1d=5 条
**实测**：部分字段与其他指标完全重复

**v3 规则**：每条 series entry 精简为 5 个字段：

| 保留 | 删除 | 删除原因 |
|------|------|---------|
| `ts` | `close_fut` | kline_history 已有 |
| `delta_fut` | `close_spot` | kline_history 已有 |
| `delta_spot` | `relative_delta_fut` | cvd_pack 顶层标量已有 |
| `cvd_7d_fut` | `relative_delta_spot` | cvd_pack 顶层标量已有 |
| `xmk_delta_gap_s_minus_f` | `spot_flow_dominance` | cvd_pack 顶层标量已有 |

输出示例（newest-first）：
```json
"series": [
  {
    "ts": "2026-03-15T03:45:00+00:00",
    "delta_fut": -1480.534,
    "delta_spot": -47.518,
    "cvd_7d_fut": 467717.809,
    "xmk_delta_gap_s_minus_f": 0.177
  }
]
```

**理由**：
- 序列的核心价值是时序模式（15 根 delta 持续为负 vs 刚转负是不同信号）
- 删掉的 5 个字段均有更权威的来源
- 节省约 40%（~1.6KB）

---

### 3.5 `footprint.buy_imbalance_prices` / `sell_imbalance_prices` → 删除

**v2 现状**：15m 窗口两个完整价格列表（buy=117 条，sell=246 条），约 3KB
**实测发现**：
- `buy_stacks`（10 个区间）覆盖连续多档叠加的强支撑区，格式 `{start_price, end_price, length}`
- `sell_stacks`（28 个区间）同理
- `levels[]` 的 top-20 buy/sell imbalance by total 已涵盖孤立 imbalance 中成交量最高的档位

**v3 规则**：删除 `buy_imbalance_prices` 和 `sell_imbalance_prices`，由以下两者替代：

| 替代来源 | 覆盖的信息 |
|---------|----------|
| `buy_stacks` / `sell_stacks` | 连续堆叠的强 imbalance 区间（机构定价带，最可靠的 entry/SL 锚点） |
| `levels[buy_imbalance=true]` top-20 by total | 高成交量孤立 imbalance 档位（次级锚点） |

**理由**：
- 363 个原始价格点中，孤立单档 imbalance 是弱信号，不应作为 entry/SL 锚点
- stacks 的区间格式（start/end/length）比原始价格列表对模型更友好，能直接表达"支撑带宽度"
- 节省约 3KB，同时提高锚点的信噪比

---

### 3.6 `orderbook_depth`（entry_core）保留近价结构墙 + top liquidity 双视角

**v3 规则**：`entry_core` 同时保留：

- `top_liquidity_levels`
- `liquidity_walls`

其中 `liquidity_walls` 结构与 `scan` 保持一致：

```json
"liquidity_walls": {
  "bid_walls": [{"price_level": 2098.0, "total_liquidity": 12345.6, "distance_pct": -0.5}],
  "ask_walls": [{"price_level": 2108.0, "total_liquidity": 11888.2, "distance_pct": 0.4}],
  "depth_imbalance_1pct": 0.47,
  "depth_imbalance_3pct": 0.51
}
```

**理由**：

- `liquidity_walls` 更适合模型直接理解近价支撑/阻力带
- `top_liquidity_levels` 仍保留逐档高流动性锚点，适合微观定价
- 两者服务不同，不视为重复字段

---

### 3.7 事件类指标字段精简 + 数量统一（absorption / exhaustion / initiation 家族）

**v2 现状**：每个事件约 24 个字段；数量上矩阵写"10 条"、详细规则写"暂不再缩"、验证写"≤17"，三处不一致
**v3 规则**：字段和数量同时统一

**数量规则（权威定义）**：

> 所有事件类指标：保留最近 **10 条**（newest-first by `event_end_ts`）。若 7d 内总事件数 < 10，全量保留。

适用范围：`absorption`、`buying_exhaustion`、`selling_exhaustion`、`initiation`、`bullish_initiation`、`bearish_initiation`、`bullish_absorption`、`bearish_absorption`。divergence 结构不同，不适用此规则。

**字段规则**：每个事件删除以下 6 个字段：

| 删除字段 | 原因 |
|---------|------|
| `score_base` | score 的计算前身，已有 score |
| `strength_score_xmk` | 实测与 score 几乎相等（冗余合成分） |
| `spot_flow_confirm_score` | `spot_confirm: true/false` 已是结论 |
| `spot_whale_confirm_score` | 同上 |
| `spot_rdelta_1m_mean` | `spot_confirm` 聚合结论已覆盖 |
| `spot_cvd_1m_change` | cvd_pack 提供更精确的 cvd 数据 |

**保留字段**（含全部时间戳和价格锚点）：
`event_start_ts`, `event_end_ts`, `event_available_ts`, `confirm_ts`,
`direction`, `pivot_price`, `price_high`, `price_low`,
`score`, `sig_pass`, `trigger_side`, `type`,
`delta_sum`, `reject_ratio`,
`stacked_buy_imbalance`, `stacked_sell_imbalance`,
`key_distance_ticks`

事件列表统一 **newest-first**（`event_end_ts` 最新的排最前）。

**理由**：
- 保留了模型建立入场判断所需的全部信号：时间、价格锚点、方向、强度、类型
- 删掉的 6 个字段是 score 的分量，不提供额外信息
- 8 个事件类指标统一 10 条 + 字段精简，合计节省约 15-20KB

---

## 四、v3 明确排除的优化（附实测原因）

| 提案 | 排除原因 |
|------|---------|
| 用 5 个统计标量替换 `liq_trend_hourly` | 实测：单小时 short_liq=460,288 被聚合掩盖，模型无法定位 sweep 事件发生的时间点，丢失 SL 锚点；改为截断 48h + spike 索引（见 §3.3） |
| 整体删除 `cvd_pack.by_window.*.series` | by_window 内无独立窗口标量，series 是窗口级唯一时序来源；流量持续性 vs 刚转向是不同入场信号；改为精简字段（见 §3.4） |
| 裸删 `value_area_levels`（不替换） | 1d/3d 窗口 hvn_levels 为空，裸删后 VA 内部结构完全丢失；改为 `va_top_levels`（见 §3.1） |

---

## 五、v3 优化效果估算

| 优化 | 节省 | 数据损失 |
|------|------|---------|
| value_area_levels → va_top_levels（top-N by volume） | ~134 KB | 无（保留最高量价格锚点，信噪比更高） |
| footprint buy/sell_imbalance_prices 删除 | ~3 KB | 无（stacks 覆盖强区间，levels 覆盖高量档） |
| funding_trend_hourly → 24h + stats | ~8 KB | 无 |
| liq_trend_hourly → 48h + spike | ~6 KB | 无（7日前旧历史对 entry 无用） |
| cvd series 字段精简（10→5 字段） | ~1.6 KB | 无 |
| 事件类：统一 10 条 + 字段精简 | ~15 KB | 无 |
| **合计** | **~168 KB** | **无锚点损失** |

当前 entry_core 样本 minified 约 400KB，优化后预计 **约 230KB**，达到 v2 软目标（220-280KB）下限。

---

## 六、实现备注

1. **va_top_levels 计算逻辑**：
   ```
   // 从 value_area_levels 中取 volume 降序前 N 条
   let total_vol: f64 = value_area_levels.iter().map(|l| l.volume).sum();
   let mut sorted = value_area_levels.clone();
   sorted.sort_by(|a, b| b.volume.partial_cmp(&a.volume).unwrap());
   let top_n = &sorted[..n.min(sorted.len())];
   // 追加 vol_rank 后，输出按 price 降序（高价在前）
   top_n.sort_by(|a, b| b.price_level.partial_cmp(&a.price_level).unwrap());
   // 每条输出: { price, volume, vol_pct: volume / total_vol, vol_rank }
   ```
   各窗口 N 值：15m=15，4h=10，1d=10，3d=10。

2. **liq_spike_events 阈值计算**：
   - 计算全量 140 桶 `long_total + short_total` 的均值 μ
   - spike 条件：`long_total + short_total > μ × 3`
   - 最多保留 10 个，输出按 `ts` **降序**（newest-first）
   - 若无 spike，输出空数组 `[]`

3. **funding_trend_stats.slope_per_hour 计算**：
   - 对全量 140 桶的 `avg_funding` 序列做最小二乘线性回归
   - x = 桶索引（0=最旧，139=最新），y = avg_funding
   - slope 单位：每小时 funding 变化量（正=上升，负=下降）

4. **事件类数量和字段精简适用范围**：
   - 适用：`absorption`、`buying_exhaustion`、`selling_exhaustion`、`initiation`、`bullish_initiation`、`bearish_initiation`、`bullish_absorption`、`bearish_absorption`
   - divergence 结构不同，**不适用**本规则，保持 v2 处理方式不变
   - 数量：取 `event_end_ts` 降序前 10 条；若总数 < 10 则全量

5. **newest-first 统一检查点**：
   - `funding_trend_hourly`（截断后）：按 `ts` 降序
   - `liq_trend_hourly`（截断后）：按 `ts` 降序
   - `liq_spike_events`：按 `ts` 降序
   - `cvd_pack.by_window.*.series`：按 `ts` 降序
   - 所有事件类 `recent_7d.events`：按 `event_end_ts` 降序
   - `va_top_levels`：按 `price` 降序（高价在前）

6. **验证方法**（每次过滤后用 Python 脚本断言）：
   - `price_volume_structure.*.value_area_levels` key **不存在**
   - `price_volume_structure.*.va_top_levels` 长度 ≤ 15
   - `footprint.payload.by_window.15m` 不含 `buy_imbalance_prices` / `sell_imbalance_prices`
   - `funding_trend_hourly` 长度 ≤ 24
   - `liq_trend_hourly` 长度 ≤ 48
   - `cvd_pack.by_window.*.series[0]` 不含 `close_fut`
   - 所有事件类 `recent_7d.events` 长度 ≤ 10
   - 所有事件类 `recent_7d.events[0]` 不含 `score_base`
