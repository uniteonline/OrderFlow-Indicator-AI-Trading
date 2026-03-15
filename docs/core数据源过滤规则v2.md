# Core（Stage 2）数据源过滤规则 v2（草案）

**状态**：草案  
**适用代码**：`systems/llm/src/llm/filter/core.rs` 及其后续拆分子模块  
**适用阶段**：LLM Stage 2  
**适用提示词**：
- `systems/llm/src/llm/prompt/entry/*.txt`
- `systems/llm/src/llm/prompt/management/*.txt`
- `systems/llm/src/llm/prompt/pending_order/*.txt`
**基线样本**：
- 当前共享 core 样本：`systems/llm/temp_model_input/20260314T112000Z_ETHUSDT_core_20260314T112212381Z.json`
- 该样本为 `entry` 场景：顶层仅有 `symbol / ts_bucket / indicators`

---

## 一、为什么需要 v2

当前 v1 的 `core` 是一套共享输入，同时服务：
- `entry`
- `management`
- `pending_order`

这带来两个问题：

1. `entry` 为了算 `entry / tp / sl`，实际不需要和 `management` 一样宽的上下文。
2. `management / pending_order` 需要实时持仓和挂单状态，但它们对部分“入场几何细节”的依赖又和 `entry` 不完全相同。

当前基线样本的实际体积：
- pretty JSON：`764,669 bytes`
- minified 后真实 prompt 体积：`391,079 bytes`

当前基线样本内，体积最大的指标如下：

| 指标 | 估算字符数 |
|------|-----------:|
| `footprint` | 99,072 |
| `price_volume_structure` | 30,802 |
| `fvg` | 25,600 |
| `orderbook_depth` | 20,912 |
| `bullish_initiation` | 19,076 |
| `divergence` | 12,324 |
| `liquidation_density` | 11,190 |
| `funding_rate` | 10,968 |
| `bearish_initiation` | 10,825 |
| `buying_exhaustion` | 10,772 |
| `selling_exhaustion` | 10,766 |
| `initiation` | 10,620 |
| `cvd_pack` | 10,170 |

结论：
- 当前 `core` 不是“算 entry/tp/sl 的最小必要输入”
- 当前 `core` 更像“安全冗余的共享 Stage 2 上下文”

---

## 二、v2 总体目标

**第一优先级**：该流程节点需要的数据不遗漏  
**第二优先级**：节约 token / 降低 prompt 体积  

v2 采用三套 core：
- `entry_core`
- `management_core`
- `pending_core`

统一链路保持不变：
1. 无论是 `entry / management / pending_order`
2. 第一阶段都先走共享 `scan`
3. 第二阶段再根据模式路由到对应的 `*_core`
4. 第二阶段提示词始终接收：
   - `STAGE_1_MARKET_SCAN_JSON`
   - 对应模式的 `*_core`

---

## 三、v2 设计原则

### 3.1 先保守拆分，再做激进瘦身

v2 第一版应遵守：
- 不因为节省 token 而删除关键价格锚点
- 不因为“scan 已经有趋势”就删除 Stage 2 还要用来定价的结构字段
- 不因为某字段当前提示词里没有明确写出，就删除其在 runtime 后续逻辑中会消费的字段

### 3.2 统一 newest-first

所有时间序列、事件序列、小时聚合序列，统一：
- 最新数据永远排在最前
- 所有 `series / events / changes / hourly buckets` 统一 newest-first

### 3.3 结构位优先于历史轨迹

如果二选一：
- 优先保留价格锚点
- 次优先保留短历史轨迹

价格锚点包括但不限于：
- `poc / vah / val / hvn / lvn`
- FVG 边界
- footprint imbalance price levels
- 未完成拍卖边界
- orderbook liquidity walls
- 当前 TP/SL 对应的实际结构位

### 3.4 相同含义字段不重复保留

如果 Stage 2 已经能从更小的字段集合得到同样语义，则优先保留：
- summary / anchor
- 不保留冗长原始明细数组

例子：
- `price_volume_structure.levels` 常常可由 `poc / vah / val / hvn_levels / lvn_levels / value_area_levels` 替代
- `fvg.fvgs` 全历史常常可由 `active_* + nearest_* + 最近少量 fvgs` 替代

### 3.5 顶层上下文只按模式保留

`entry_core` 不带持仓/挂单上下文。  
`management_core` 和 `pending_core` 必须带实时上下文。

---

## 四、建议的代码组织方式

为后续维护，建议代码也同步拆成三套 serializer，而不是继续在一个函数里用 if/else 混排：

- `systems/llm/src/llm/filter/core.rs`
  - 只保留路由入口
- `systems/llm/src/llm/filter/core_entry.rs`
- `systems/llm/src/llm/filter/core_management.rs`
- `systems/llm/src/llm/filter/core_pending.rs`
- `systems/llm/src/llm/filter/core_shared.rs`
  - 放公共 helper，例如：
  - newest-first 截断
  - event 保留最近 N 条
  - orderbook top liquidity 构造
  - divergence / funding / liquidation 的聚合 helper

推荐落盘命名也改成显式模式：
- `*_entry_core_*.json`
- `*_management_core_*.json`
- `*_pending_core_*.json`

这样后续复核时，能直接从 `temp_model_input` 判断当前喂给哪条第二阶段链路的数据。

---

## 五、三套 core 的顶层结构

### 5.1 `entry_core`

顶层只保留：
- `symbol`
- `ts_bucket`
- `indicators`

明确不保留：
- `trading_state`
- `management_snapshot`

### 5.2 `management_core`

顶层保留：
- `symbol`
- `ts_bucket`
- `indicators`
- `trading_state`
- `management_snapshot`

其中 `management_snapshot` 必须保留：
- `context_state`
- `positions[]`
- `positions[].pnl_by_latest_price`
- `positions[].current_tp_price`
- `positions[].current_sl_price`
- `pending_order`
- `last_management_reason`
- `position_context`
- `position_context.entry_context`

### 5.3 `pending_core`

顶层保留：
- `symbol`
- `ts_bucket`
- `indicators`
- `trading_state`
- `management_snapshot`

其中 `management_snapshot` 对 `pending_core` 至少必须保留：
- `pending_order`
- `position_context`
- `position_context.entry_context`
- `last_management_reason`

如果运行时后续允许 `pending_order` 和 `active_position` 并存，则还必须保留：
- `positions[]`
- `positions[].pnl_by_latest_price`

注意：
- 当前实现里的 `pending_order_mode` 通常是“只有挂单，没有活动持仓”
- 但 v2 文档不应把 `positions[]` 从 `pending_core` 规范里彻底排除

---

## 六、基线样本驱动的拆分建议

### 6.1 当前样本里最值得优先处理的字段

从 `20260314T112000Z_ETHUSDT_core_20260314T112212381Z.json` 看，优先处理顺序建议如下：

1. `footprint`
2. `price_volume_structure`
3. `fvg`
4. `orderbook_depth`
5. 事件家族（尤其 `bullish_initiation`）

原因：
- 这几类字段体积最大
- 同时又最有“可以保结构、不保冗余明细”的空间

### 6.2 当前样本里已确认的事实

基线样本里：
- `footprint.payload.by_window.15m.levels` 长度是 `157`
- `footprint.payload.by_window.4h` 已经没有 `levels`
- `orderbook_depth.payload.top_liquidity_levels` 长度是 `100`
- `bullish_initiation.payload.recent_7d.events` 长度是 `18`

说明：
- v1 已经在部分指标上做了裁剪
- 但对 `entry` 来说仍然偏宽，对 `management` / `pending` 来说仍然偏共享

---

## 七、指标分拆矩阵（v2）

下表中的“保留策略”是 v2 的建议目标，不是对 v1 的复述。

| 指标 | `entry_core` | `management_core` | `pending_core` |
|------|--------------|-------------------|----------------|
| `vpin` | 全量保留 | 全量保留 | 全量保留 |
| `whale_trades` | 全量保留 | 全量保留 | 全量保留 |
| `high_volume_pulse` | 全量保留 | 全量保留 | 全量保留 |
| `tpo_market_profile` | 全量保留 | 全量保留 | 全量保留 |
| `price_volume_structure` | 摘要锚点版 | 摘要锚点版 | 摘要锚点版 |
| `fvg` | active + nearest + 最近少量 fvgs | active + nearest | active + nearest + 最近少量 fvgs |
| `avwap` | 短序列 | 短序列 | 稍丰富的 15m 短序列 |
| `rvwap_sigma_bands` | 当前标量 | 当前标量 | 当前标量 |
| `footprint` | 15m 详细锚点版 | 15m 防守锚点版 | 15m 详细锚点版 |
| `orderbook_depth` | top liquidity 丰富版 | top liquidity 缩减版 | top liquidity 丰富版 |
| `kline_history` | 入场短序列 | 管理短序列 | 15m 略长序列 |
| `ema_trend_regime` | 当前标量 | 当前标量 | 当前标量 |
| `cvd_pack` | 短序列 | 短序列 | 15m 略长序列 |
| `funding_rate` | 小时聚合 + window 摘要 | 小时聚合 + window 摘要 | 小时聚合 + window 摘要 |
| `liquidation_density` | 小时聚合 + window 摘要 | 小时聚合 + window 摘要 | 小时聚合 + window 摘要 |
| 事件家族 | 最近 10 条 | 最近 10 条 | 最近 10 条 |
| `divergence` | recent_7d 最近 10 + top 5 candidates | recent_7d 最近 10 + top 5 candidates | recent_7d 最近 10 + top 5 candidates |

---

## 八、各套 core 的详细过滤规则

## 8.1 `entry_core`

### 8.1.1 目标

`entry_core` 的唯一目标是：
- 帮模型做 `entry / tp / sl / leverage / horizon`
- 让每一个价格都能追溯到真实结构位

它不承担：
- 实时持仓管理上下文
- 挂单上下文
- 账户状态展示

### 8.1.2 建议保留策略

#### `price_volume_structure`

保留：
- 顶层：`poc_price`、`poc_volume`、`vah`、`val`、`hvn_levels`、`lvn_levels`、`value_area_levels`、`bar_volume`、`volume_zscore`、`volume_dryup`
- `by_window.{15m,4h,1d,3d}`：
  - `poc_price`
  - `poc_volume`
  - `vah`
  - `val`
  - `hvn_levels`
  - `lvn_levels`
  - `value_area_levels`
  - `bar_volume`
  - `volume_zscore`
  - `volume_dryup`
  - `window_bars_used`

丢弃：
- 顶层 `levels`
- `by_window.*.levels`

理由：
- `entry / tp / sl` 真正需要的是价值区边界和成交密集锚点
- 原始 volume profile 全部分布档位体积较大，但边际价值有限

#### `fvg`

保留：
- `by_window.{15m,4h,1d,3d}.active_bull_fvgs`
- `by_window.{15m,4h,1d,3d}.active_bear_fvgs`
- `by_window.{15m,4h,1d,3d}.nearest_bull_fvg`
- `by_window.{15m,4h,1d,3d}.nearest_bear_fvg`
- 每个 FVG item 保留原始 `upper` / `lower`，并额外补兼容字段：`fvg_top = upper`、`fvg_bottom = lower`
- `coverage_ratio`
- `is_ready`
- `fvgs` 仅保留最近少量候选，建议每窗口最近 `6-8` 个

理由：
- 需要目标区和失效区
- 不需要保留完整历史 FVG 列表

#### `footprint`

保留 `15m`：
- `buy_imbalance_prices`
- `sell_imbalance_prices`
- `buy_stacks`
- `sell_stacks`
- `max_buy_stack_len`
- `max_sell_stack_len`
- `stacked_buy`
- `stacked_sell`
- `ua_top`
- `ua_bottom`
- `unfinished_auction`
- `window_delta`
- `window_total_qty`
- `levels` 只保留“代表性档位”，不再展开全部 imbalance 明细：
  - `is_open / is_high / is_low / is_close = true`
  - `ua_top_flag = true / ua_bottom_flag = true`
  - `buy_imbalance = true` 中按 `total` 排名前 `20`
  - `sell_imbalance = true` 中按 `total` 排名前 `20`
  - 按 `total` 排名前 `10` 的高成交量档位
- 保留完整档位字段：
  - `price_level`
  - `buy`
  - `sell`
  - `delta`
  - `total`
  - `buy_imbalance`
  - `sell_imbalance`
  - `is_open`
  - `is_high`
  - `is_low`
  - `is_close`
  - `ua_top_flag`
  - `ua_bottom_flag`

保留 `4h / 1d`：
- 汇总字段（全量保留）
- 将原始 `buy_imbalance_prices` / `sell_imbalance_prices` 合并为 cluster 输出，**不保留原始价格数组**

`4h / 1d` footprint cluster 定义：
- 将 `buy_imbalance_prices` 中的价格档位按 **0.5% 价格步长** 分组聚合，输出 `buy_imb_clusters`
- 将 `sell_imbalance_prices` 按同样规则聚合，输出 `sell_imb_clusters`
- 每个 cluster 的输出结构：
  ```json
  {
    "center_price": 2034.50,
    "level_count": 3,
    "price_range": [2033.0, 2036.0]
  }
  ```
- cluster 按 `center_price` 降序排列（价格从高到低）
- 单侧最多保留前 **20 个** cluster
- `levels` 数组在 4h/1d 中**不保留**（如原始数据有则丢弃）

#### `orderbook_depth`

保留：
- 当前顶层 summary 标量
- `by_window.{15m,1h}` 摘要字段
- `top_liquidity_levels` 建议保留前 `80`

理由：
- `entry` 需要看当前价附近墙位
- 但不需要和 `pending` 一样保守地保留更宽的 fillability 数据

#### `kline_history`

保留：
- `15m` 最近 `20`
- `4h` 最近 `12`
- `1d` 最近 `8`
- `futures` only

#### `cvd_pack`

保留：
- 顶层标量全量
- `15m` 最近 `15`
- `4h` 最近 `8`
- `1d` 最近 `5`

#### `avwap`

保留：
- 顶层标量全量
- `15m` 最近 `5`
- `4h` 最近 `3`
- `1d` 最近 `2`

#### `rvwap_sigma_bands`

保留：
- `by_window.{15m,4h,1d}` 当前标量
- `as_of_ts`
- `source_mode`

丢弃：
- 历史序列

#### `ema_trend_regime`

保留：
- 当前 EMA 标量
- `trend_regime`
- `trend_regime_by_tf`
- `ema_100_htf`
- `ema_200_htf`

#### `funding_rate / liquidation_density / event families / divergence`

保留策略：
- 继续沿用 v1 的 newest-first 小时聚合和最近事件集
- `entry_core` 暂不再比 v1 进一步缩事件数量

理由：
- 第一优先级是不遗漏数据
- 这些指标虽有体积，但目前不是最危险的误删点

### 8.1.3 `entry_core` 软目标

建议目标：
- minified 体积：`220KB - 280KB`

说明：
- 这是软目标，不是硬闸门
- 如果为保证结构位不遗漏，允许阶段性超过

---

## 8.2 `management_core`

### 8.2.1 目标

`management_core` 的目标是：
- 判断原交易 thesis 是否仍有效
- 是否需要 `VALID / INVALID / ADJUST`
- 如果调整，是否调整 `tp / sl / add / reduce`

它比 `entry_core` 更强调：
- 持仓上下文
- 当前实际 TP/SL
- 最新价格下的浮盈亏
- 原始 entry thesis 的延续性

### 8.2.2 顶层上下文要求

必须保留：
- `trading_state`
- `management_snapshot`
- `management_snapshot.positions[].pnl_by_latest_price`
- `management_snapshot.positions[].current_tp_price`
- `management_snapshot.positions[].current_sl_price`
- `management_snapshot.position_context`
- `management_snapshot.position_context.entry_context`

### 8.2.3 建议保留策略

#### `price_volume_structure`

与 `entry_core` 相同：
- 只保留摘要锚点
- 不保留 `levels`

#### `fvg`

保留：
- `active_bull_fvgs`
- `active_bear_fvgs`
- `nearest_bull_fvg`
- `nearest_bear_fvg`
- `coverage_ratio`
- `is_ready`
- `fvgs` 保留最近 **4 个**（per window，15m / 4h）

丢弃：
- 超出最近 4 个的 `fvgs` 历史

理由：
- `management` 更关注当前仍有效的目标区和失效区
- `nearest_bull_fvg` / `nearest_bear_fvg` 是距离当前价格最近的 FVG，不是距离 TP/SL 最近的
- 若 TP 恰好落入新形成的 FVG 区间内，management 提示词不能对此无感知
- 保留最近 4 个 fvgs 是最小必要保留量，不需要像 `entry` 保留 6-8 个

#### `footprint`

`management_core` 的 `15m footprint` 只保留“防守相关锚点版”：
- `buy_imbalance_prices`
- `sell_imbalance_prices`
- `buy_stacks`
- `sell_stacks`
- `max_buy_stack_len`
- `max_sell_stack_len`
- `stacked_buy`
- `stacked_sell`
- `ua_top`
- `ua_bottom`
- `unfinished_auction`
- `window_delta`
- `window_total_qty`
- `levels` 仅保留代表性防守档位：
  - `is_open / is_high / is_low / is_close = true`
  - `ua_top_flag / ua_bottom_flag = true`
  - `buy_imbalance = true` 中按 `total` 排名前 `10`
  - `sell_imbalance = true` 中按 `total` 排名前 `10`

不再额外保留：
- 高成交量前 10 档

理由：
- `management` 关心的是 thesis 是否破坏、SL 是否该收紧
- 对“新入场最优价值区”的需求弱于 `entry` / `pending`

#### `orderbook_depth`

保留：
- summary 标量
- `by_window.{15m,1h}`
- `top_liquidity_levels` 建议前 `60`

#### `kline_history`

保留：
- `15m` 最近 `16`
- `4h` 最近 `15`
- `1d` 最近 `10`

#### `cvd_pack`

保留：
- `15m` 最近 `12`
- `4h` 最近 `10`
- `1d` 最近 `6`

#### `avwap`

保留：
- 顶层标量
- `15m` 最近 `3`
- `4h` 最近 `2`
- `1d` 最近 `2`

#### 其余指标

保留原则：
- `vpin / whale_trades / high_volume_pulse / tpo_market_profile / funding_rate / liquidation_density / divergence / event families`
- 继续保留

原因：
- `management` 提示词明确会用这些指标审查 thesis 延续性

### 8.2.4 `management_core` 软目标

建议目标：
- minified 体积：`260KB - 340KB`

---

## 8.3 `pending_core`

### 8.3.1 目标

`pending_core` 的目标是：
- 评估未成交 maker 单现在是否还值得保留
- 是否要取消
- 是否要改 entry / tp / sl / leverage

它和 `entry_core` 很像，但更强调：
- 15m fillability
- 是否容易被 sweep
- 当前市场是否已经 spent move

### 8.3.2 顶层上下文要求

必须保留：
- `trading_state`
- `management_snapshot`
- `management_snapshot.pending_order`
- `management_snapshot.position_context.entry_context`
- `management_snapshot.last_management_reason`

如果运行时提供 `positions[]`，也必须保留：
- `positions[]`
- `positions[].pnl_by_latest_price`

### 8.3.3 建议保留策略

#### `price_volume_structure`

与 `entry_core` 一致：
- 保留摘要锚点
- 不保留 `levels`

#### `fvg`

保留：
- `active_bull_fvgs`
- `active_bear_fvgs`
- `nearest_bull_fvg`
- `nearest_bear_fvg`
- `coverage_ratio`
- `is_ready`
- `fvgs` 最近 `6` 个

#### `footprint`

`pending_core` 采用比 `management_core` 更丰富的 15m 版：
- 与 `entry_core` 相同
- 保留 `buy/sell_imbalance_prices`
- 保留 `buy/sell_stacks`
- `levels` 仅保留代表性 imbalance / OHLC / UA 档位
- 保留高成交量前 `10` 档

理由：
- 挂单是否会被扫掉、是否该改 entry，强依赖 15m 微结构

#### `orderbook_depth`

保留：
- summary 标量
- `by_window.{15m,1h}`
- `top_liquidity_levels` 建议保留前 `100`

理由：
- `pending_order` 对 fillability 和当前墙位最敏感
- 这类信息不建议比 `entry` 更激进地瘦身

#### `kline_history`

保留：
- `15m` 最近 `24`
- `4h` 最近 `12`
- `1d` 最近 `8`

#### `cvd_pack`

保留：
- `15m` 最近 `18`
- `4h` 最近 `8`
- `1d` 最近 `5`

#### `avwap`

保留：
- `15m` 最近 `8`
- `4h` 最近 `3`
- `1d` 最近 `2`

#### 其余指标

保留原则：
- `vpin / whale_trades / high_volume_pulse / tpo_market_profile / funding_rate / liquidation_density / divergence / event families`
- 与 `entry_core` 同级保留

### 8.3.4 `pending_core` 软目标

建议目标：
- minified 体积：`240KB - 320KB`

---

## 九、v1 已知 Bug — v2 实施时必须一并修正

v2 迁移时，不能直接复制 v1 过滤逻辑，否则以下 bug 会被继承：

| bug | v1 现状 | v2 修正要求 |
|-----|---------|------------|
| `footprint 15m` 过滤逻辑未生效 | 实测 335 levels（目标应为 ~120 after 过滤） | 三套 core 都必须正确执行“价格数组 + stacks + 代表性 levels”策略；`levels` 只保留 OHLC/UA、每侧 top-N imbalance 和 top-10 by total |
| `price_volume_structure.levels` 未移除 | levels 数组仍在输出，占 ~47KB | 三套 core 全部禁止输出 `levels` |
| 事件数量超出上限 | `bullish_initiation` 实测 22 条（目标 17/10） | 所有事件类指标截取逻辑必须统一验证 |
| `4h / 1d footprint buy/sell_imbalance_prices` 未 cluster 化 | 原始价格数组直接保留 | 按 §8.1.2 的 cluster 定义输出，丢弃原始数组 |

**验证方法**：每次 core 过滤后，用 Python 脚本对落盘 JSON 做以下断言：
- `footprint.payload.by_window.15m.levels` 长度 ≤ 200（宽松门槛）
- `price_volume_structure.payload.by_window.*.levels` key 不存在
- 所有事件类指标 `recent_7d.events` 长度 ≤ 17
- `footprint.payload.by_window.4h` 不含 `buy_imbalance_prices` / `sell_imbalance_prices`

---

## 十、v2 和 v1 的关键差异

v1：
- 一套共享 `core`
- 逻辑简单
- 体积偏大
- 维护时容易“为了某一模式加字段，三种模式一起变大”

v2：
- 三套 `core`
- 每套围绕实际任务目标裁剪
- 体积更可控
- 后续维护可以单独演进，不会互相绑死

---

## 十二、迁移策略（建议）

### 第一步：先拆路由，不急着激进瘦身

先完成：
- `entry_core / management_core / pending_core` 三套序列化入口
- 各自落盘
- 各自接入 prompt

第一版即使部分指标规则还接近，也比共享 `core` 更易维护。

### 第二步：优先砍最重但最安全的冗余

优先处理：
- `price_volume_structure.levels`
- `fvg.fvgs` 全历史
- `management_core` 的 `footprint 15m` 高成交量补充档位
- `management_core` 的 `orderbook_depth.top_liquidity_levels`

### 第三步：用真实样本回归验证

每次调整都必须验证：
- 提示词需要的字段没有丢
- runtime 后续消费字段没有丢
- `entry / management / pending_order` 三条链路都还能生成正确 prompt input
- minified 体积变化被记录

---

## 十三、建议补的测试

至少新增以下回归测试：

- `build_entry_core_value_applies_v2_rules`
- `build_management_core_value_applies_v2_rules`
- `build_pending_core_value_applies_v2_rules`
- `entry_core_real_snapshot_stays_under_soft_budget`
- `management_core_real_snapshot_stays_under_soft_budget`
- `pending_core_real_snapshot_stays_under_soft_budget`
- `entry_prompt_uses_entry_core`
- `management_prompt_uses_management_core`
- `pending_prompt_uses_pending_core`

同时保留真实样本级校验：
- `temp_model_input` 产物字段存在性
- newest-first
- 关键价格锚点不丢失

---

## 十四、最终建议

v2 的建议不是“盲目做小”，而是：
- 让 `entry` 拿到更像“定价核心”的输入
- 让 `management` 拿到更像“持仓审查核心”的输入
- 让 `pending_order` 拿到更像“挂单复核核心”的输入

原则上：
- 能被 `scan` 完整覆盖的宏观趋势解释，不应在 `core` 里再保留大段冗余轨迹
- 不能丢的，是会直接影响价格决策的结构位、状态位、和 runtime 后续消费字段

补充去重原则：
- 所有 indicator wrapper 不保留 `window_code`，只保留 `payload`
- 所有事件类对象不保留纯追踪字段，如 `event_id`
- `event_start_ts / event_end_ts` 已存在时，不再额外保留别名 `start_ts / end_ts`
- `indicator_code` 由外层 indicator key 已经表达，不重复保留
- FVG 不保留 `fvg_id`、`tf` 这类可由外层路径表达的字段
- footprint `levels` 不保留 `level_rank`

这份 v2 草案的核心取向是：
- **先把模式拆开**
- **再按模式做保守瘦身**
- **始终坚持“数据不遗漏优先，节约 token 次之”**
