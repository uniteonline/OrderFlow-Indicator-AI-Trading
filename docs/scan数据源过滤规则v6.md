# Scan 数据源过滤规则 v6

**状态**：草案（待实现）
**定位**：可实现版重构；**不继承 v5 的主组织方式**
**适用文件**：`systems/llm/src/llm/filter/scan.rs`
**唯一目标**：让 `scan filter -> 候选证据`，由 LLM 自己判断 `15m / 4h / 1d` 的方向与区间
**设计立场**：JSON 不是给工程师读的，而是给模型按顺序读的；主结构必须服务于模型阅读，而不是服务于指标分类
**体积原则**：**不设硬性上限**；只要求避免无意义重复，任何对方向/区间判断有帮助的事实优先保留

---

## 一、v6 的第一性原理

如果唯一目标是：

**`scan filter -> 候选证据`，让模型根据数据给出 `15m / 4h / 1d` 方向和区间**

那么 JSON 顶层结构就不应该优先回答：

- 数据来自哪个指标
- 数据属于哪个模块
- 工程上怎么组装最方便

而应该优先回答：

1. 现在价格在哪里？
2. 最近是怎么走到这里的？
3. 最近发生了什么关键事件？
4. 当前有哪些最值得看的 bracket？
5. 如果主视图没有覆盖到，原始结构细节在哪里？

这意味着 v6 的主组织方式必须改成：

- 先给“现在”
- 再给“最近路径”
- 再给“最近事件”
- 再给“当前 bracket / 结构”
- 最后给“兜底原始结构”

而不是：

- 先按指标分类
- 或先按 `15m / 4h / 1d` 分成 3 个大桶

---

## 二、v6 的阅读顺序原则

### 2.1 顶层顺序固定

v6 顶层固定为：

1. `version / symbol / ts_bucket / current_price`
2. `now`
3. `path_newest_to_oldest`
4. `events_newest_to_oldest`
5. `supporting_context`
6. `raw_overflow`

### 2.2 所有时间数组默认按“从新到旧”排序

用户明确要求“从新到旧为主显示顺序”，因此以下数组默认都按**最新在前**排序：

- `bars_newest_to_oldest`
- `events_newest_to_oldest`
- `nearest_above`
- `nearest_below`
- `current_inside`

为了避免模型因顺序问题产生困惑，每个元素还应保留：

- `ts` / `reference_ts`
- `age_minutes`
- `scope`

### 2.3 近的数据详细，远的数据概括

v6 必须显式体现：

- 最近的数据更细
- 更远的数据更粗
- 但不能因为聚合而丢失对区间判断重要的结构

因此：

- `15m` 用较详细的 bar 路径
- `4h` 用中等细度的上下文
- `1d` 用更概括的背景
- 但区间相关的 bracket 不能因为“远”就直接省略

### 2.4 主视图和兜底层同时存在

v6 必须同时保留：

- 主视图：整理好的、最利于模型阅读的结构
- 兜底层：更原始、更细颗粒度的结构与 level

因为 v6 可以重组信息，但**不能比 v5 丢失区间信息**。

---

## 三、v6 顶层 JSON 结构

```jsonc
{
  "version": "scan_v6",
  "symbol": "ETHUSDT",
  "ts_bucket": "2026-03-18T03:15:00+00:00",
  "current_price": 2329.29,

  "now": {
    "price_anchor": {
      "futures_last_price": 0.0,
      "futures_mark_price": 0.0,
      "spot_proxy_price": 0.0
    },

    "location_snapshot": {
      "vs_avwap": {
        "avwap_fut": 0.0,
        "price_minus_avwap_fut": 0.0,
        "price_minus_avwap_pct": 0.0,
        "xmk_avwap_gap_f_minus_s": 0.0
      },
      "vs_ema_band": {
        "ema_13": 0.0,
        "ema_21": 0.0,
        "ema_34": 0.0,
        "ema_band_low": 0.0,
        "ema_band_high": 0.0,
        "position": "above | inside | below",
        "distance_to_band_low_pct": 0.0,
        "distance_to_band_high_pct": 0.0
      },
      "vs_value": [
        {
          "scope": "15m | 4h | 1d",
          "source": "price_volume_structure | tpo_market_profile",
          "value_low": 0.0,
          "value_high": 0.0,
          "poc": 0.0,
          "position": "above | inside | below",
          "distance_to_low_pct": 0.0,
          "distance_to_high_pct": 0.0
        }
      ],
      "vs_rvwap": [
        {
          "scope": "15m | 4h | 1d",
          "rvwap": 0.0,
          "sigma_w": 0.0,
          "z_price_minus_rvwap": 0.0,
          "band_minus_1": 0.0,
          "band_plus_1": 0.0,
          "band_minus_2": 0.0,
          "band_plus_2": 0.0
        }
      ]
    },

    "acceptance_board": [
      {
        "scope": "15m | 4h | 1d",
        "source": "price_volume_structure | tpo_market_profile",
        "value_low": 0.0,
        "value_high": 0.0,
        "current_position": "above | inside | below",
        "recent_closes_inside_count": 0,
        "basis_bars": 2,
        "state": "accepted_above | accepted_below | inside_value | rejected_from_above | rejected_from_below | reentered_value"
      }
    ],

    "bracket_board": {
      "current_inside": [
        {
          "scope": "15m | 4h | 1d",
          "kind": "pvs_value_area | tpo_value_area | recent_swing_bracket | active_imbalance_bracket | session_ib_bracket | liquidation_bracket | wall_bracket | overlapping_value_bracket",
          "source_indicators": [
            "price_volume_structure",
            "tpo_market_profile"
          ],
          "reference_ts": "2026-03-18T03:15:00+00:00",
          "support": 0.0,
          "resistance": 0.0,
          "width_pct": 0.0,
          "mid_price": 0.0,
          "distance_to_support_pct": 0.0,
          "distance_to_resistance_pct": 0.0,
          "support_evidence": [],
          "resistance_evidence": []
        }
      ],
      "nearest_above": [],
      "nearest_below": [],
      "higher_context": []
    },

    "structure_nodes_near_current": {
      "above": [],
      "below": []
    },

    "current_flow_snapshot": {
      "cvd": {},
      "orderbook": {},
      "footprint": {},
      "whales": {}
    },

    "current_volume_nodes": {
      "high_volume_pulse": {}
    }
  },

  "path_newest_to_oldest": {
    "latest_15m_detail": {
      "summary": {
        "bars_count": 12,
        "oldest_open": 0.0,
        "latest_close": 0.0,
        "atr14": 0.0,
        "atr14_pct": 0.0,
        "net_change_pct": 0.0,
        "range_high": 0.0,
        "range_low": 0.0,
        "range_pct": 0.0
      },
      "current_partial_bar": {},
      "bars_newest_to_oldest": []
    },
    "latest_4h_context": {
      "summary": {
        "bars_count": 8,
        "oldest_open": 0.0,
        "latest_close": 0.0,
        "atr14": 0.0,
        "atr14_pct": 0.0,
        "net_change_pct": 0.0,
        "range_high": 0.0,
        "range_low": 0.0,
        "range_pct": 0.0
      },
      "current_partial_bar": {},
      "bars_newest_to_oldest": []
    },
    "latest_1d_background": {
      "summary": {
        "bars_count": 10,
        "oldest_open": 0.0,
        "latest_close": 0.0,
        "atr14": 0.0,
        "atr14_pct": 0.0,
        "net_change_pct": 0.0,
        "range_high": 0.0,
        "range_low": 0.0,
        "range_pct": 0.0
      },
      "current_partial_bar": {},
      "bars_newest_to_oldest": []
    }
  },

  "events_newest_to_oldest": {
    "latest_24h_detail": [],
    "latest_7d_major": []
  },

  "supporting_context": {
    "cvd_path_snapshot": {},
    "indicator_snapshots": {
      "vpin": {},
      "funding_rate": {},
      "liquidation_density": {},
      "high_volume_pulse_full": {}
    }
  },

  "raw_overflow": {
    "price_structures": [],
    "price_levels": []
  }
}
```

---

## 四、为什么这个结构更利于模型阅读

### 4.1 `now` 放最前面

模型先需要知道的是：

- 当前价在哪里
- 当前相对 value、RVWAP、EMA、AVWAP 在哪里
- 当前价正被哪些 bracket 包住
- 当前上下最近有哪些结构

这就是 `now` 的职责。

### 4.2 `path_newest_to_oldest` 放第二层

模型在知道“现在在哪里”之后，下一步就要知道：

- 最近是怎么走到这里的
- 这个路径是单边、冲高回落、回到平衡，还是持续接受

因此路径应该紧跟在 `now` 后面，而不是埋在某个 indicator 里。

### 4.3 `events_newest_to_oldest` 单独成层

事件的价值不在于它属于哪个指标，而在于：

- 什么时候发生
- 发生在哪个价位
- 离现在有多远

因此事件应按时间线单独展示，而不是分散在多个 payload 中。

### 4.4 `bracket_board` 比平铺 `range_candidates` 更利于阅读

模型最关心的不是“全量候选列表按生成顺序排出来”，而是：

- 当前价现在包在哪些 bracket 里
- 最近的上方 bracket 是什么
- 最近的下方 bracket 是什么
- 更大的 4h / 1d 背景 bracket 是什么

所以 `bracket_board` 必须按**当前相关性**分组，而不是简单平铺。

### 4.5 `raw_overflow` 防止重组丢信息

即使主视图已经更利于阅读，也不能假设主视图已经覆盖所有有效结构。

因此必须有：

- `raw_overflow.price_structures`
- `raw_overflow.price_levels`

作为区间信息兜底层。

---

## 五、子结构定义

### 5.1 `now`

`now` 是模型的起点。

它的职责是让模型在看完整个 JSON 前，先快速回答：

- 当前价在什么位置
- 当前最 relevant 的 bracket 是什么
- 当前上下最近的结构是什么
- 当前流动性快照是什么

#### 5.1.1 `price_anchor`

最小价格锚点：

- `futures_last_price`
- `futures_mark_price`
- `spot_proxy_price`

#### 5.1.2 `location_snapshot`

只保留当前最关键的位置关系：

- `vs_avwap`
- `vs_ema_band`
- `vs_value`
- `vs_rvwap`

这层不复制完整指标，只表达当前价相对关键参考系的位置。

#### 5.1.3 `acceptance_board`

这是方向判断的关键事实层之一。

它不是方向结论，而是告诉模型：

- 当前相对某个 value 在哪里
- 最近 `2-3` 根 close 是否稳定在 value 内/外
- 当前是 accepted、rejected 还是 reentered

#### 5.1.4 `bracket_board`

这是 v6 最重要的主视图区间层。

它不再把所有区间候选平铺成一个长数组，而是按当前价的相关性分成：

- `current_inside`
  - 当前价落在其内的 bracket
- `nearest_above`
  - 最近的上方 bracket / 阻力型 bracket
- `nearest_below`
  - 最近的下方 bracket / 支撑型 bracket
- `higher_context`
  - 重要但不一定贴着当前价的更大级别 bracket

这比单纯的 `range_candidates[]` 更符合模型阅读方式。

#### 5.1.5 `structure_nodes_near_current`

这是对原 `active_structure_map` 的再收敛。

它只保留“当前附近最 relevant 的结构节点”，按：

- `above`
- `below`

分组。

每个节点至少带：

- `scope`
- `kind`
- `source_indicator`
- `price_low / price_high / mid_price`
- `distance_to_current_pct`
- `status`

#### 5.1.6 `current_flow_snapshot`

只表达当前窗口事实，不伪造历史。

保留：

- `cvd`
- `orderbook`
- `footprint`
- `whales`

#### 5.1.7 `current_volume_nodes`

这里用于保留“当前量能节点”。

第一版明确保留：

- `high_volume_pulse`

原因：

- 它当前没有 `events[]`
- 更像状态型量能节点
- 应该出现在 `now` 里，让模型知道当前最 relevant 的 volume node

### 5.2 `path_newest_to_oldest`

这是 v6 的主路径层。

路径必须遵循：

- 最近详细
- 更远概括
- 从新到旧排序

#### 5.2.1 `latest_15m_detail`

这是最细的路径层。

推荐保留：

- `current_partial_bar`
- 最近 `12-16` 根闭合 `15m` bar
- 每根 bar 的派生字段
- 一个 summary
- `summary.atr14 / summary.atr14_pct`

#### 5.2.2 `latest_4h_context`

这是中周期上下文层。

推荐保留：

- `current_partial_bar`
- 最近 `8-12` 根 `4h` bar
- 比 `15m` 更轻的字段集
- 一个 summary
- `summary.atr14 / summary.atr14_pct`

#### 5.2.3 `latest_1d_background`

这是更远背景层。

推荐保留：

- `current_partial_bar`
- 最近 `7-10` 根 `1d` bar
- 较轻的字段集
- 一个 summary
- `summary.atr14 / summary.atr14_pct`

#### 5.2.4 为什么要同时保留 summary 和 bars

因为模型既需要：

- 快速抓整体轮廓

也需要：

- 在必要时读更细的 bar 序列

所以每个 block 应同时给：

- `summary`
- `bars_newest_to_oldest`

`summary` 中加入 `atr14 / atr14_pct` 后，模型还能立刻知道该时间桶的标准波动单位，不需要再从 bar 序列里自行估算波动尺度。

### 5.3 `events_newest_to_oldest`

这是事件层。

不再使用“重 timeline”，而改成两个更利于模型阅读的区块：

- `latest_24h_detail`
  - 最近 24h 的去重后详细事件
- `latest_7d_major`
  - 24h 之外、7d 之内的高分重要事件

这样能兼顾：

- 最近事件的细节
- 更远事件的概括

#### 5.3.1 `high_volume_pulse` 不进入事件层

明确规则：

- `high_volume_pulse` **不进入** `events_newest_to_oldest`

原因：

- 当前源数据里没有离散 `events[]`
- 只有状态摘要和窗口 POC
- 它更适合出现在：
  - `now.current_volume_nodes.high_volume_pulse`
  - `supporting_context.indicator_snapshots.high_volume_pulse_full`

### 5.4 `supporting_context`

这层不是模型主阅读层，而是补充上下文层。

用于保留：

- `cvd_path_snapshot`
- `indicator_snapshots`

其中 `indicator_snapshots` 首批建议保留：

- `vpin`
- `funding_rate`
- `liquidation_density`
- `high_volume_pulse_full`

### 5.5 `raw_overflow`

这是兜底层。

它的职责不是成为模型主视图，而是确保：

- 主视图没有覆盖到的细颗粒度结构，模型仍能访问
- v6 不会因为“重组得更漂亮”而比 v5 丢掉区间信息

建议保留：

- `price_structures`
- `price_levels`

---

## 六、字段提取与聚合规则

### 6.1 通用派生公式

- `distance_pct(a, b) = (a - b) / b * 100`
- `width_pct(low, high) = (high - low) / ((high + low) / 2) * 100`
- `range_pct = (high - low) / open * 100`
- `close_location_pct = (close - low) / max(high - low, eps)`
- `upper_wick_pct_of_range = (high - max(open, close)) / max(high - low, eps)`
- `lower_wick_pct_of_range = (min(open, close) - low) / max(high - low, eps)`
- `overlap_with_prev_bar_pct = overlap(curr_range, prev_range) / max(curr_range, prev_range, eps)`
- `position(price, low, high) = above | inside | below`
- `age_minutes = (ts_bucket - reference_ts) / 60`
- `atr14 = avg(last up to 14 true_range)`
- `true_range = max(high - low, abs(high - prev_close), abs(low - prev_close))`
- `atr14_pct = atr14 / latest_close * 100`

### 6.2 `acceptance_board` 计算规则

推荐用：

- 当前价格
- 最近 `2-3` 根闭合 bar close
- 同 timeframe 的 `VAL / VAH`

共同判定：

- `inside_value`
- `accepted_above`
- `accepted_below`
- `rejected_from_above`
- `rejected_from_below`
- `reentered_value`

### 6.3 路径保留数量规则

推荐：

- `latest_15m_detail.bars_newest_to_oldest`：`12-16` 根
- `latest_4h_context.bars_newest_to_oldest`：`8-12` 根
- `latest_1d_background.bars_newest_to_oldest`：`7-10` 根

最低配不得少于：

- `15m`: `6`
- `4h`: `4`
- `1d`: `5`

### 6.4 事件去重规则

推荐规则：

- 相同 `indicator_code`
- 相同方向
- 时间差 `< 30m`
- `pivot_price` 偏差 `< 0.15%`

merge 后保留：

- 最早 `event_start_ts`
- 最晚 `event_end_ts`
- 最近 `confirm_ts`
- 最大 `score`
- `event_count`

### 6.5 `bracket_board` 组装原则

每个候选必须是完整 bracket，而不是单个 level。

每条候选至少要有：

- `scope`
- `support`
- `resistance`
- `width_pct`
- `current_inside`

分组规则：

- 当前价在 bracket 内 -> `current_inside`
- bracket 完全在当前价上方 -> `nearest_above`
- bracket 完全在当前价下方 -> `nearest_below`
- 更宽、更远、但仍重要 -> `higher_context`

### 6.6 `raw_overflow` 保留原则

`raw_overflow` 允许：

- 轻清洗
- 轻去重

但不允许：

- 因为“主视图更清晰”就删掉原本有价值的结构

---

## 七、字段映射总表

以下源路径均以：

`/data/systems/llm/temp_indicator/20260318T031500Z_ETHUSDT.json`

为根。

记法：

- `/indicators/<indicator>/payload/...`
- `[-1]` = 最后一个元素
- `[-N:]` = 最后 N 个元素
- `<tf>` = `15m | 4h | 1d`

### 7.1 顶层字段映射

| v6 字段 | 当前源路径 | 聚合方式 | 为什么保留 |
|---|---|---|---|
| `version` | 常量 | 写死 `scan_v6` | 协议版本 |
| `symbol` | `/symbol` | 直接复制 | 主键 |
| `ts_bucket` | `/ts_bucket` | 直接复制 | 对齐时点 |
| `current_price` | `/indicators/avwap/payload/fut_last_price`，fallback `/indicators/kline_history/payload/intervals/15m/markets/futures/bars[-1].close` | 优先 fut last | 当前价格锚点 |

### 7.2 `now` 字段映射

| v6 字段 | 当前源路径 | 聚合方式 | 为什么保留 |
|---|---|---|---|
| `now.price_anchor.futures_last_price` | `/indicators/avwap/payload/fut_last_price` | 直接复制 | 当前主价格 |
| `now.price_anchor.futures_mark_price` | `/indicators/avwap/payload/fut_mark_price` | 直接复制 | 标记价参考 |
| `now.price_anchor.spot_proxy_price` | `/indicators/avwap/payload/avwap_spot` 及相关差值字段 | 近似 spot 价格 | 跨市场位置 |
| `now.location_snapshot.vs_avwap.*` | `/indicators/avwap/payload/avwap_fut` `/price_minus_avwap_fut` `/xmk_avwap_gap_f_minus_s` | 直接复制并派生百分比 | 当前锚定均值位置 |
| `now.location_snapshot.vs_ema_band.*` | `/indicators/ema_trend_regime/payload/ema_13` `/ema_21` `/ema_34` `/ema_band_low` `/ema_band_high` | 直接复制并计算位置 | 当前相对 EMA band 位置 |
| `now.location_snapshot.vs_value[]` | `/indicators/price_volume_structure/payload/by_window/<tf>/val,vah,poc_price` 与 `/indicators/tpo_market_profile/payload/tpo_val,tpo_vah,tpo_poc` | 每个 `<tf>`、每个来源生成一条 | 当前相对 value 的位置 |
| `now.location_snapshot.vs_rvwap[]` | `/indicators/rvwap_sigma_bands/payload/by_window/<tf>/...` | 每个 `<tf>` 生成一条 | 当前相对 RVWAP 的位置 |
| `now.acceptance_board[]` | `current_price` + `/indicators/kline_history/payload/intervals/<tf>/markets/futures/bars[-3:]` + PVS/TPO 的 `VAL/VAH` | 计算 accepted / rejected / reentered | 方向判断关键事实 |
| `now.bracket_board.*` | 由第八章定义的 bracket 源组装 | 先生成完整候选，再按 `current_inside / nearest_above / nearest_below / higher_context` 分组 | 这是模型最应该先看的区间层 |
| `now.structure_nodes_near_current.*` | 由 FVG / PVS / TPO / wall / liq / swing / absorption zone 生成 | 只保留离当前最近的结构节点 | 当前附近结构地图 |
| `now.current_flow_snapshot.cvd` | `/indicators/cvd_pack/payload/...` | 保留当前摘要 | 当前主动成交偏向 |
| `now.current_flow_snapshot.orderbook` | `/indicators/orderbook_depth/payload/by_window/15m/...` 及必要顶层字段 | 当前窗口快照 | 当前微观流动性状态 |
| `now.current_flow_snapshot.footprint` | `/indicators/footprint/payload/window_delta` `/window_total_qty` `/unfinished_auction` `/ua_top` `/ua_bottom` `/max_buy_stack_len` `/max_sell_stack_len` | 直接复制或轻归一 | 当前 footprint 状态 |
| `now.current_flow_snapshot.whales` | `/indicators/whale_trades/payload/by_window/<tf>` | 保留当前窗口摘要 | 当前大单行为 |
| `now.current_volume_nodes.high_volume_pulse` | `/indicators/high_volume_pulse/payload` | 提取 `intrabar_poc_price` `intrabar_poc_volume` `intrabar_poc_max_by_window` `by_z_window` 的当前 relevant 部分 | 当前量能节点 |

### 7.3 `path_newest_to_oldest` 字段映射

| v6 字段 | 当前源路径 | 聚合方式 | 为什么保留 |
|---|---|---|---|
| `latest_15m_detail.current_partial_bar` | `/indicators/kline_history/payload/intervals/15m/markets/futures/bars[-1]` | 直接复制 | 当前未闭合 15m bar |
| `latest_15m_detail.bars_newest_to_oldest` | `/indicators/kline_history/payload/intervals/15m/markets/futures/bars[-17:-1]` 视样本充足度截到 `12-16` 根 | 复制 OHLCV 并派生特征后，按新到旧排序 | 最近路径最细层 |
| `latest_15m_detail.summary` | `/indicators/kline_history/payload/intervals/15m/markets/futures/bars` | 聚合 `oldest_open/latest_close/net_change_pct/range_high/range_low/range_pct`，并基于最近最多 `14` 根闭合 bar 计算 `atr14/atr14_pct` | 给模型先看轮廓再读细节，并立即知道 15m 波动单位 |
| `latest_4h_context.current_partial_bar` | `/indicators/kline_history/payload/intervals/4h/markets/futures/bars[-1]` | 直接复制 | 当前未闭合 4h bar，帮助模型理解中周期正在形成的状态 |
| `latest_4h_context.bars_newest_to_oldest` | `/indicators/kline_history/payload/intervals/4h/markets/futures/bars[-12:]` 视样本截到 `8-12` 根 | 复制 OHLCV 并派生较轻特征，按新到旧排序 | 中周期上下文 |
| `latest_4h_context.summary` | `/indicators/kline_history/payload/intervals/4h/markets/futures/bars` | 聚合 summary，并基于最近最多 `14` 根闭合 bar 计算 `atr14/atr14_pct` | 先给模型中周期轮廓和标准波动单位 |
| `latest_1d_background.current_partial_bar` | `/indicators/kline_history/payload/intervals/1d/markets/futures/bars[-1]` | 直接复制 | 当前未闭合 1d bar，帮助模型把日线背景和正在演化的当日状态连起来 |
| `latest_1d_background.bars_newest_to_oldest` | `/indicators/kline_history/payload/intervals/1d/markets/futures/bars[-10:]` | 复制 OHLCV 并派生较轻特征，按新到旧排序 | 更远背景 |
| `latest_1d_background.summary` | `/indicators/kline_history/payload/intervals/1d/markets/futures/bars` | 聚合 summary，并基于最近最多 `14` 根闭合 bar 计算 `atr14/atr14_pct` | 先给模型大背景轮廓和标准波动单位 |

### 7.4 `events_newest_to_oldest` 字段映射

| v6 字段 | 当前源路径 | 聚合方式 | 为什么保留 |
|---|---|---|---|
| `latest_24h_detail[]` | 各事件源 `events[*]` | 取最近 24h、去重后、按新到旧排序 | 最近事件的细节层 |
| `latest_7d_major[]` | 各事件源 `events[*]` | 取 24h 之外 7d 之内的高分事件/事件簇，按新到旧排序 | 更远事件的概括层 |
| `indicator_code / event_type / direction / time / price / score / spot_confirm / trigger_side / distance_to_current_pct / event_count` | 各事件源字段 | 统一归一 | 让模型按统一语义读事件 |

事件源包括但不限于：

- `/indicators/bullish_absorption/payload/recent_7d/events`
- `/indicators/bearish_absorption/payload/recent_7d/events`
- `/indicators/bullish_initiation/payload/recent_7d/events`
- `/indicators/bearish_initiation/payload/recent_7d/events`
- `/indicators/buying_exhaustion/payload/recent_7d/events`
- `/indicators/selling_exhaustion/payload/recent_7d/events`
- `/indicators/divergence/payload/events`

明确不包括：

- `/indicators/high_volume_pulse/payload`

### 7.5 `supporting_context` 字段映射

| v6 字段 | 当前源路径 | 聚合方式 | 为什么保留 |
|---|---|---|---|
| `supporting_context.cvd_path_snapshot` | `/indicators/cvd_pack/payload/by_window` 或相关序列字段 | 提取最近相关序列/摘要 | 方向补充证据 |
| `supporting_context.indicator_snapshots.vpin` | `/indicators/vpin/payload` | 保留轻量摘要 | 毒性背景 |
| `supporting_context.indicator_snapshots.funding_rate` | `/indicators/funding_rate/payload` | 保留轻量摘要 | 拥挤背景 |
| `supporting_context.indicator_snapshots.liquidation_density` | `/indicators/liquidation_density/payload/by_window` 及必要摘要 | 保留轻量摘要 | 清算背景 |
| `supporting_context.indicator_snapshots.high_volume_pulse_full` | `/indicators/high_volume_pulse/payload` | 保留完整或较完整摘要 | volume node 补充背景 |

### 7.6 `raw_overflow` 字段映射

| v6 字段 | 当前源路径 | 聚合方式 | 为什么保留 |
|---|---|---|---|
| `raw_overflow.price_structures` | 当前 scan 中的 `price_structures`，或由源数据重新生成的等价结构列表 | 允许轻清洗、轻去重，但不强行重组成 bracket | 区间信息兜底 |
| `raw_overflow.price_levels` | 当前 scan 中的 `price_levels`，或由源数据重新生成的等价 level 列表 | 允许轻清洗、轻去重 | 原始边界细节兜底 |

---

## 八、`bracket_board` 与 `structure_nodes_near_current` 的候选来源

### 8.1 `pvs_value_area`

- 源路径：
  - `/indicators/price_volume_structure/payload/by_window/<tf>/val`
  - `/indicators/price_volume_structure/payload/by_window/<tf>/vah`
  - `/indicators/price_volume_structure/payload/by_window/<tf>/poc_price`
- 生成：
  - `bracket_board` 候选
  - 近端时也进入 `structure_nodes_near_current`

### 8.2 `tpo_value_area`

- 源路径：
  - `/indicators/tpo_market_profile/payload/tpo_val`
  - `/indicators/tpo_market_profile/payload/tpo_vah`
  - `/indicators/tpo_market_profile/payload/tpo_poc`
  - `/indicators/tpo_market_profile/payload/initial_balance_low`
  - `/indicators/tpo_market_profile/payload/initial_balance_high`
- 生成：
  - `bracket_board`
  - 重要 `session_ib_bracket`

### 8.3 `bull_fvg / bear_fvg`

- 源路径：
  - `/indicators/fvg/payload/by_window/<tf>/active_bull_fvgs`
  - `/indicators/fvg/payload/by_window/<tf>/active_bear_fvgs`
  - `/indicators/fvg/payload/by_window/<tf>/nearest_bull_fvg`
  - `/indicators/fvg/payload/by_window/<tf>/nearest_bear_fvg`
- 生成：
  - `active_imbalance_bracket`
  - `structure_nodes_near_current`

### 8.4 `recent_swing_bracket`

- 源路径：
  - `/indicators/kline_history/payload/intervals/<tf>/markets/futures/bars`
- 推荐 lookback：
  - `15m`: 最近 `12-16` 根 bar
  - `4h`: 最近 `8-12` 根 bar
  - `1d`: 最近 `7-10` 根 bar
- 生成：
  - `recent_swing_bracket`

### 8.5 `liquidation_bracket`

- 源路径：
  - `/indicators/liquidation_density/payload/by_window/<tf>/peak_levels`
  - `/indicators/liquidation_density/payload/recent_7d`
- 生成：
  - `liquidation_bracket`
  - 近端峰值节点进入 `structure_nodes_near_current`

### 8.6 `wall_bracket`

- 源路径：
  - `/indicators/orderbook_depth/payload/liquidity_walls`
  - `/indicators/orderbook_depth/payload/level_clusters`
- 生成：
  - `wall_bracket`
  - 近端墙进入 `structure_nodes_near_current`

### 8.7 `absorption_zone`

- 源路径：
  - `/indicators/bullish_absorption/payload/recent_7d/events`
  - `/indicators/bearish_absorption/payload/recent_7d/events`
- 生成：
  - 不一定强行形成主 bracket
  - 但可成为 `support_evidence / resistance_evidence`
  - 近端时可进入 `structure_nodes_near_current`

### 8.8 `high_volume_pulse`

- 源路径：
  - `/indicators/high_volume_pulse/payload`
- 当前定位：
  - `now.current_volume_nodes.high_volume_pulse`
  - `supporting_context.indicator_snapshots.high_volume_pulse_full`
- 当前**不进入**事件层

---

## 九、实施分期

### P0：必须先做

#### P0.1 重构顶层 JSON 顺序

- 按以下顶层顺序输出：
  - `now`
  - `path_newest_to_oldest`
  - `events_newest_to_oldest`
  - `supporting_context`
  - `raw_overflow`

#### P0.2 实现 `now.bracket_board`

- 用 PVS / TPO / FVG / swing / liq / wall 等来源生成完整 bracket
- 按：
  - `current_inside`
  - `nearest_above`
  - `nearest_below`
  - `higher_context`
  分组

#### P0.3 实现 `path_newest_to_oldest`

- `latest_15m_detail`
- `latest_4h_context`
- `latest_1d_background`
- 每个 block 都有 `summary + bars_newest_to_oldest`

#### P0.4 保留 `raw_overflow`

- 保留原始 `price_structures`
- 保留原始 `price_levels`
- 允许轻清洗、轻去重

### P1：强烈推荐

#### P1.1 实现 `acceptance_board`

- 对 `PVS/TPO value` 计算 accepted / rejected / reentered

#### P1.2 实现 `events_newest_to_oldest`

- 最近 24h 详细事件
- 7d 重大事件概括
- 去重与统一字段

#### P1.3 实现 `structure_nodes_near_current`

- 只保留当前附近最 relevant 的结构节点

### P2：推荐做

#### P2.1 实现 `current_flow_snapshot`

- 汇总当前 `cvd / orderbook / footprint / whales`

#### P2.2 实现 `current_volume_nodes.high_volume_pulse`

- 当前量能节点直接放进 `now`

#### P2.3 实现 `supporting_context`

- `cvd_path_snapshot`
- `indicator_snapshots`

---

## 十、v6 明确不做的事情

v6 不做：

- 默认方向
- 默认区间
- `bias / dominant / best / preferred`
- prose 型 narrative
- 重型跨引用图
- `event_links`
- `structure_refs`
- 假装存在的历史 orderbook / footprint / whale flow 序列

一句话：

**v6 只负责把真实证据重排成最利于模型阅读的顺序，不负责替模型先得出答案。**

---

## 十一、最终设计判断

从第一性原理看，模型要给出 `15m / 4h / 1d` 的方向和区间，它最需要的是：

- 当前在哪里
- 最近怎么走到这里
- 最近发生了什么
- 当前有哪些 bracket 候选
- 原始结构细节还能在哪里看到

因此，v6 的正确顶层结构不是：

- `by_indicator`
- 也不是 `by_timeframe`

而是：

- `now`
- `path_newest_to_oldest`
- `events_newest_to_oldest`
- `supporting_context`
- `raw_overflow`

这就是 v6 围绕唯一目标的最终设计：

**`scan filter -> 候选证据`，让模型根据数据自己判断 `15m / 4h / 1d` 的方向和区间。**
