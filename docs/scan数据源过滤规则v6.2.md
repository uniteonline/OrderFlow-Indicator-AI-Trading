# Scan 数据源过滤规则 v6.2

**状态**：已执行  
**定位**：在 `v6.1` 已完成去重和结构压缩之后，继续增强 `stage1 scan` 对“方向判断”的支持  
**适用文件**：`systems/llm/src/llm/filter/scan.rs`  
**依赖文档**：
- [scan数据源过滤规则v6.md](/data/docs/scan数据源过滤规则v6.md)
- [scan数据源过滤规则v6.1.md](/data/docs/scan数据源过滤规则v6.1.md)

**唯一目标**：让 `scan filter -> 候选证据` 更直接地服务于 LLM 对 `15m / 4h / 1d` 的方向判断，同时不改变当前 `range` 相关语义，不引入新的规则性约束。

---

## 一、v6.2 的范围

本次 `v6.2` 只做一件事：

1. 在 `now` 中增加 `momentum_snapshot`

明确**不做**：

1. `volatility_anchor`
2. `bracket_board.role`

原因如下：

- `ATR` 已经存在于 `path_newest_to_oldest.*.summary` 中，单独前置成 `volatility_anchor` 的收益有限；而 `min_range_pct` 这类命名容易引入规则味道。
- `bracket_board` 的角色，应该由 prompt 中对输入阅读顺序和结构语义的解释来承担，不必在数据层再额外打标签。

因此，`v6.2` 的目标收敛为：

- 聚合分散的动量证据
- 让模型更快看见“速度”和“延续性”
- 但不在数据层写入任何结论标签

---

## 二、为什么只做 `momentum_snapshot`

当前 `stage1 scan` 在方向判断上仍有一个明显问题：

- 模型可以看见 bar、CVD、whale、事件
- 但这些事实分散在多个区域
- 模型需要自行把“最近 3 根 bar 是不是连续走弱、CVD 是否继续恶化、whale 是否仍然偏单边”拼成一个短线动量判断

这会导致：

- 短线 continuation 容易被低估
- `Sideways` 判定偏多
- `range` 也容易因为方向判断偏保守而缩窄

所以 `v6.2` 不再新增更多结构字段，而是做一个**纯数值、纯事实层**的动量摘要。

---

## 三、设计原则

`momentum_snapshot` 必须遵守三条原则：

### 3.1 只放数值，不放结论

不允许出现：

- `momentum = bullish`
- `momentum = bearish`
- `continuation = true`
- `trend = down`

允许出现的是：

- 最近 3 根 bar 的 return
- 最近 3 根 bar 的累计 return
- 最近 3 个 CVD 点
- CVD slope
- 当前 timeframe 的 whale delta notional

也就是说：

- 模型自己判断方向
- `scan filter` 只提供更容易读的数值证据

### 3.2 不引入规则性约束

`momentum_snapshot` 不是 hard gate，也不是打分器。  
它只是把原本散落在不同模块里的动量证据放到一起。

### 3.3 只使用当前已有可稳定提取的来源

当前阶段不为了 `v6.2` 新增上游依赖。

所以：

- `bar return` 来自现有 `kline_history`
- `CVD` 来自现有 `cvd_pack.by_window.<tf>.series`
- `whale` 只取现有 timeframe 当前窗口的 `fut_whale_delta_notional`

特别注意：

- 当前 `whale_trades` 没有稳定保留每个 timeframe 的最近 3 点序列
- 所以 `v6.2` **不输出 `whale trend`、`whale acceleration`**
- 只输出 `whale_delta_notional`

---

## 四、新结构

`momentum_snapshot` 放在：

- `now`

结构如下：

```jsonc
"now": {
  "momentum_snapshot": {
    "15m": {
      "bar_return_last_3_pct": [0.0, 0.0, 0.0],
      "bar_return_sum_last_3_pct": 0.0,
      "last_closed_bar_range_vs_atr": 0.0,
      "last_closed_bar_close_location_pct": 0.0,
      "cvd_delta_last_3": [0.0, 0.0, 0.0],
      "cvd_slope": 0.0,
      "whale_delta_notional": 0.0
    },
    "4h": {
      "bar_return_last_3_pct": [0.0, 0.0, 0.0],
      "bar_return_sum_last_3_pct": 0.0,
      "last_closed_bar_range_vs_atr": 0.0,
      "last_closed_bar_close_location_pct": 0.0,
      "cvd_delta_last_3": [0.0, 0.0, 0.0],
      "cvd_slope": 0.0,
      "whale_delta_notional": 0.0
    },
    "1d": {
      "bar_return_last_3_pct": [0.0, 0.0, 0.0],
      "bar_return_sum_last_3_pct": 0.0,
      "last_closed_bar_range_vs_atr": 0.0,
      "last_closed_bar_close_location_pct": 0.0,
      "cvd_delta_last_3": [0.0, 0.0, 0.0],
      "cvd_slope": 0.0,
      "whale_delta_notional": 0.0
    }
  }
}
```

---

## 五、字段定义

### 5.1 `bar_return_last_3_pct`

含义：

- 最近 `3` 根闭合 bar 各自的涨跌幅（百分比）

来源：

- `kline_history` 的 futures 闭合 bar

规则：

- 对最近 `3` 根闭合 bar，逐根计算：
  - `(close / open - 1) * 100`

输出顺序：

- 从旧到新

示例：

```json
"bar_return_last_3_pct": [-0.76, -0.08, -0.04]
```

### 5.2 `bar_return_sum_last_3_pct`

含义：

- 最近 `3` 根闭合 bar 合并后的累计涨跌幅（百分比）

规则：

- `latest_close / oldest_open - 1`

这个字段的作用是让模型快速知道：

- 不是单根 bar 有多大
- 而是最近这段 path 整体在朝哪个方向走、走了多远

### 5.3 `cvd_delta_last_3`

含义：

- 当前 timeframe 最近 `3` 个 CVD 点的 `delta_fut`

来源：

- `supporting_context.cvd_path_snapshot.by_window.<tf>.series`

输出顺序：

- 从旧到新

### 5.4 `last_closed_bar_range_vs_atr`

含义：

- 最近 `1` 根闭合 bar 的真实波动范围，相对于该 timeframe `ATR14` 的倍数

规则：

- `(latest_closed_bar.high - latest_closed_bar.low) / atr14`

这个字段的作用是让模型快速知道：

- 最近这根 bar 是否只是正常波动
- 还是已经明显扩张到异常水平

示例：

```json
"last_closed_bar_range_vs_atr": 2.55
```

### 5.5 `last_closed_bar_close_location_pct`

含义：

- 最近 `1` 根闭合 bar 的收盘位置，位于该 bar 自身区间中的百分位

规则：

- `(close - low) / (high - low) * 100`

解释方式：

- 对大阳线来说，越接近 `100`，越像延续式收盘
- 对大阴线来说，越接近 `0`，越像延续式收盘
- 如果最近 bar 很大，但收盘位置没有停留在扩张方向的一端，更像耗竭或反转前兆

示例：

```json
"last_closed_bar_close_location_pct": 16.7
```

### 5.6 `cvd_slope`

含义：

- 最近两个 CVD 点之间的变化量

规则：

- `latest_delta_fut - previous_delta_fut`

这个字段的作用是让模型快速知道：

- 当前 CVD 是在继续同向推进
- 还是已经明显衰减

### 5.7 `whale_delta_notional`

含义：

- 当前 timeframe 当前窗口的 `fut_whale_delta_notional`

来源：

- `whale_trades.payload.by_window.<tf>.fut_whale_delta_notional`

注意：

- 这里是单值
- 不表示趋势
- 只表示当前该 timeframe 的大单方向强度

---

## 六、字段来源与代码落点

### 6.1 代码入口

最自然的落点是：

- `build_now(...)`

位置参考：

- [scan.rs](/data/systems/llm/src/llm/filter/scan.rs#L202)

在当前 `build_now()` 中新增：

```rust
"momentum_snapshot": build_momentum_snapshot(source),
```

### 6.2 `bar_return_last_3_pct / bar_return_sum_last_3_pct`

直接复用现有：

- `extract_closed_futures_bars(source, tf)`

位置参考：

- [scan.rs](/data/systems/llm/src/llm/filter/scan.rs#L1334)

### 6.3 `cvd_delta_last_3 / cvd_slope`

直接取现有：

- `cvd_pack.payload.by_window.<tf>.series`

当前相关保留逻辑在：

- [scan.rs](/data/systems/llm/src/llm/filter/scan.rs#L3658)

### 6.4 `whale_delta_notional`

直接取现有：

- `whale_trades.payload.by_window.<tf>.fut_whale_delta_notional`

当前相关保留逻辑在：

- [scan.rs](/data/systems/llm/src/llm/filter/scan.rs#L3704)

---

## 七、为什么不做 `whale trend`

这是本次 `v6.2` 的一个明确边界。

原因不是“不重要”，而是：

- 当前上游稳定暴露的是每个 timeframe 的当前窗口聚合值
- 不是同一 timeframe 的最近多点时间序列

因此当前阶段如果强行加：

- `whale_delta_trend`
- `whale_delta_acceleration`

就会把“单点”伪装成“趋势”，这不符合事实层原则。

所以 `v6.2` 明确规定：

- `whale` 先只给当前窗口数值
- 不给趋势标签

---

## 八、为什么不做 `volatility_anchor`

这次明确不做，不是因为它完全没价值，而是因为：

- `ATR` 已经存在于 `path_newest_to_oldest.*.summary`
- 当前更影响方向判断的问题不是缺 ATR，而是缺动量事实层的聚合
- `min_range_pct` 这类设计容易让数据层染上“规则性约束”的味道

所以：

- `v6.2` 先不碰这项
- 如果未来确实需要，也应以“事实型命名”重新设计，而不是规则型命名

---

## 九、为什么不做 `bracket_board.role`

这次也明确不做。

原因是：

- 当前 prompt 已经在解释 `bracket_board` 是当前结构参考层
- 这个语义更适合由 prompt 负责，而不是在数据层再显式打角色标签
- 当前更该优先解决的，是模型对“速度”和“延续性”读得不够直

所以：

- `bracket_board.role` 不进入 `v6.2`

---

## 十、验收标准

### 10.1 结构验收

新产物中必须存在：

- `now.momentum_snapshot.15m`
- `now.momentum_snapshot.4h`
- `now.momentum_snapshot.1d`

且每个 timeframe 必须包含：

- `bar_return_last_3_pct`
- `bar_return_sum_last_3_pct`
- `last_closed_bar_range_vs_atr`
- `last_closed_bar_close_location_pct`
- `cvd_delta_last_3`
- `cvd_slope`
- `whale_delta_notional`

### 10.2 语义验收

不得出现：

- `momentum = bullish/bearish`
- `trend = up/down`
- `continuation = true/false`
- `whale_delta_trend`
- `whale_delta_acceleration`

也就是说：

- 只允许数值
- 不允许标签化结论

### 10.3 目标验收

在真实样本复核中，模型应更少出现：

- 明明最近 3 根 bar 和 CVD 已经连续转弱，仍然轻易报 `Sideways`
- 明明显著 continuation 正在发生，但短线方向仍被局部结构拉回保守结论

---

## 十一、最终判断

`v6.2` 的这次收敛版本非常明确：

- 不扩张范围
- 不引入规则字段
- 不给默认方向
- 只把最关键的动量证据前置成一个纯数值摘要

这与当前目标完全一致：

**让 `scan filter -> 候选证据` 更直接地服务于 LLM 对 `15m / 4h / 1d` 方向判断。**
