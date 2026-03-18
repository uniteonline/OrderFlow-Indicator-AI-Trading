# Scan 数据源过滤规则 v6.1

**状态**：草案（待复核，未执行）
**定位**：在 `v6` 已落地结构之上做增量优化；目标不是重写，而是去重复、提可读性、提候选证据质量
**适用文件**：`systems/llm/src/llm/filter/scan.rs`
**依赖文档**：[scan数据源过滤规则v6.md](/data/docs/scan数据源过滤规则v6.md)
**唯一目标**：让 `scan filter -> 候选证据`，由 LLM 更稳定地判断 `15m / 4h / 1d` 的方向与区间

---

## 一、v6.1 的定位

`v6` 已经解决了主组织方式问题：

- 不再以指标分类作为主阅读路径
- 改成 `now -> path_newest_to_oldest -> events_newest_to_oldest -> supporting_context -> raw_overflow`
- 为模型提供了当前状态、路径、事件、兜底结构

但从真实产物看，`v6` 仍然存在明显的**结构重复**和**微结构噪声过多**问题。

本次 `v6.1` 不改变根本目标，也不改变“不给默认方向、不给默认主结论”的原则。

`v6.1` 只做四件事：

1. 合并 `vs_value + acceptance_board`
2. 合并同边界 bracket 的多 scope 镜像
3. 强力压缩 `raw_overflow.price_structures`
4. 让 `raw_overflow` 变成“近端优先、远端稀疏”

---

## 二、为什么要做 v6.1

本次评估基于样本：

- [20260318T083000Z_ETHUSDT_scan_20260318T083257217Z.json](/data/systems/llm/temp_model_input/20260318T083000Z_ETHUSDT_scan_20260318T083257217Z.json)

样本中的核心问题：

- 文件总体积约 `312,865` bytes
- `raw_overflow` 单块约 `111,863` bytes，占比约 `36%`
- `raw_overflow.price_structures = 298` 条
- `now.location_snapshot.vs_value = 6` 条
- `now.acceptance_board = 6` 条
- `vs_value` 和 `acceptance_board` 的 `scope/source/value_low/value_high` 是 `6/6` 一一对应
- `bracket_board` 中已经出现同边界、多 scope 的镜像重复，例如：
  - `session_ib_bracket 2316.09-2322.79` 同时存在于 `15m` 和 `4h`
  - `tpo_value_area 2319.02-2321.53` 同时存在于 `15m` 和 `4h`

这说明当前问题不是“信息不够”，而是：

- 同一事实被重复表达
- 同一 bracket 被镜像复制
- `raw_overflow` 中大量近价微结构会稀释模型注意力

这些问题不会直接破坏正确性，但会削弱模型对**方向**和**主区间**的聚焦。

---

## 三、v6.1 是否违背目标

不会。

`v6.1` 的原则不是“删信息”，而是：

- 保留事实
- 合并重复
- 压缩噪声
- 提高模型阅读时的信噪比

只要实现时满足以下原则，`v6.1` 就是在增强而不是削弱目标：

- 不把不同语义结构错误合并
- 不把关键 HTF 结构因为“远”而删掉
- 不把多周期共振压缩成单周期信息
- 不向模型注入默认方向或默认结论

---

## 四、改动一：合并 `vs_value + acceptance_board`

### 4.1 当前问题

`v6` 中：

- `now.location_snapshot.vs_value`
- `now.acceptance_board`

本质上都在描述“当前价相对 value 的状态”。

区别只在于：

- `vs_value` 更偏位置与距离
- `acceptance_board` 更偏状态与最近 close 行为

但两者的基础主键完全相同：

- `scope`
- `source`
- `value_low`
- `value_high`

这意味着模型需要在两个数组之间做人工 join，增加阅读负担。

### 4.2 v6.1 新结构

用一个统一数组替代两者：

```jsonc
"now": {
  "location_snapshot": {
    "vs_avwap": {},
    "vs_ema_band": {},
    "vs_rvwap": []
  },
  "value_state_board": [
    {
      "scope": "15m | 4h | 1d",
      "source": "price_volume_structure | tpo_market_profile",
      "value_low": 0.0,
      "value_high": 0.0,
      "poc": 0.0,
      "position": "above | inside | below",
      "distance_to_low_pct": 0.0,
      "distance_to_high_pct": 0.0,
      "state": "accepted_above | accepted_below | inside_value | rejected_from_above | rejected_from_below | reentered_value",
      "recent_closes_inside_count": 0,
      "basis_bars": 0
    }
  ]
}
```

### 4.3 字段映射

| v6 字段 | v6.1 字段 |
|---|---|
| `now.location_snapshot.vs_value[].scope` | `now.value_state_board[].scope` |
| `now.location_snapshot.vs_value[].source` | `now.value_state_board[].source` |
| `now.location_snapshot.vs_value[].value_low` | `now.value_state_board[].value_low` |
| `now.location_snapshot.vs_value[].value_high` | `now.value_state_board[].value_high` |
| `now.location_snapshot.vs_value[].poc` | `now.value_state_board[].poc` |
| `now.location_snapshot.vs_value[].position` | `now.value_state_board[].position` |
| `now.location_snapshot.vs_value[].distance_to_low_pct` | `now.value_state_board[].distance_to_low_pct` |
| `now.location_snapshot.vs_value[].distance_to_high_pct` | `now.value_state_board[].distance_to_high_pct` |
| `now.acceptance_board[].state` | `now.value_state_board[].state` |
| `now.acceptance_board[].recent_closes_inside_count` | `now.value_state_board[].recent_closes_inside_count` |
| `now.acceptance_board[].basis_bars` | `now.value_state_board[].basis_bars` |

### 4.4 对目标的影响

这是**增强**：

- 模型一次就能读到“位置 + 状态”
- 更容易回答“当前是 accepted、reentered，还是 rejected”
- 对方向判断和区间判断都更直接

---

## 五、改动二：合并同边界 bracket 的多 scope 镜像

### 5.1 当前问题

当前 `bracket_board` 中，存在 support/resistance/kind 完全相同的 bracket，只因 `scope` 不同而保留两份。

这会导致模型误以为这是两条不同证据，实际上它们只是“同一 bracket 在多个尺度下有效”。

### 5.2 v6.1 合并原则

如果以下字段完全相同，则合并为一条 bracket：

- `kind`
- `support`
- `resistance`

合并后不再保留单个 `scope`，改为：

```jsonc
{
  "scopes": ["15m", "4h"],
  "kind": "tpo_value_area",
  "support": 2319.02,
  "resistance": 2321.53
}
```

### 5.3 v6.1 新 bracket 结构

```jsonc
{
  "scopes": ["15m", "4h", "1d"],
  "kind": "pvs_value_area | tpo_value_area | recent_swing_bracket | active_imbalance_bracket | session_ib_bracket | liquidation_bracket | wall_bracket | overlapping_value_bracket",
  "source_indicators": [
    "price_volume_structure",
    "tpo_market_profile"
  ],
  "reference_ts": "2026-03-18T03:15:00+00:00",
  "support": 0.0,
  "resistance": 0.0,
  "width_pct": 0.0,
  "current_inside": true,
  "mid_price": 0.0,
  "distance_to_support_pct": 0.0,
  "distance_to_resistance_pct": 0.0,
  "support_evidence": [],
  "resistance_evidence": []
}
```

### 5.4 合并规则

合并后字段处理如下：

- `scopes`
  - 取所有命中 scope 的并集
- `source_indicators`
  - 取并集并去重
- `reference_ts`
  - 取最新值
- `support_evidence / resistance_evidence`
  - 文本去重后合并
- `current_inside`
  - 只要有一条为 `true`，则为 `true`
- `distance_to_support_pct / distance_to_resistance_pct`
  - 保留相同值；若不同，取与当前价最近的那条

### 5.5 对目标的影响

这是**增强**：

- 模型更容易理解“跨周期共振”
- 可以减少“镜像复制”带来的伪多样性
- 区间判断会更自然地把这些 bracket 视为一个更强的候选

---

## 六、改动三：强力压缩 `raw_overflow.price_structures`

### 6.1 当前问题

当前 `raw_overflow.price_structures` 不是“兜底主结构”，而是包含大量极细的 micro cluster。

样本中：

- `raw_overflow.price_structures = 298`
- 很多结构集中在当前价附近极窄范围
- 比如 `2318.23 ~ 2318.52` 附近连续堆叠多个：
  - `footprint_price_cluster`
  - `pvs_value_cluster`

这会造成：

- 模型视觉上被近价碎片淹没
- 原始兜底层太重，影响主证据的权重感

### 6.2 v6.1 的原则

`raw_overflow` 仍保留，但不再输出“逐个微结构点”的平铺列表。  
改为输出**聚合后的 structure zone**。

### 6.2.1 `absorption_zone` 的源头限流

对 `absorption_zone`，建议在进入 `raw_overflow.price_structures` 生产链之前，先做一次**仅限 overflow 路径**的源头限流。

注意：

- 这个限流**只作用于 `raw_overflow` 的 `absorption_zone` 生产链**
- 不作用于：
  - `events_newest_to_oldest`
  - `bracket_board` 的 `support_evidence / resistance_evidence`
- 也就是说，通用的 absorption 事件收集仍可保留完整或轻过滤版本，避免误伤 bracket 附近但分数不算最高的关键 absorption

推荐规则：

- 先按 `(scope, direction)` 分桶
- 每个桶按 `score DESC, confirm_ts DESC, event_start_ts DESC` 排序
- 每个桶最多保留 `10` 条事件进入 `raw_overflow` 的后续 zone 聚合

这样做的目的不是删除 absorption 证据，而是避免 `raw_overflow` 被大量同类 absorption 微结构淹没。

### 6.3 聚合前提

必须先按语义分桶，再按价格邻近聚合。

第一层分桶 key：

- `source_kind`
- `source_window`
- `direction`（若有）
- `side`（若有）
- `state`（若有）
- `event_type`（若有）

只有语义一致的结构，才允许做价格聚合。

这里需要强调：

- `v6.1` 采用的是**保守聚合**
- 既然第一层分桶已经包含 `source_kind + source_window`，那么一个聚合 zone 内的原始结构在这两个维度上必然一致
- 因此 `v6.1` 不再使用 `source_kinds[] / source_windows[]` 数组表达 provenance，而改为单值：
  - `source_kind`
  - `source_window`

这样做的目的，是避免为了保留数组而放宽分桶边界，导致不同来源语义被错误合并。

如果后续确实需要表达“跨 source_kind / source_window 的近价共振”，那应该在更高一层做 `meta_zone` 或 `confluence_overlay`，而不是在 `v6.1` 这一层直接混并原始结构。

### 6.4 价格聚合规则

在同一语义桶内，对价格相近结构进行合并：

- 默认邻近阈值：`0.02%` 的当前价
- 即两个 structure 的 `price_mid` 距离小于等于 `current_price * 0.0002` 时，可归为同一 zone

### 6.5 v6.1 新结构

将 `raw_overflow.price_structures` 重定义为“聚合 zone 列表”：

```jsonc
"raw_overflow": {
  "price_structures": [
    {
      "zone_id": "agg_001",
      "count": 5,
      "price_low": 2318.23,
      "price_high": 2318.52,
      "price_mid": 2318.38,
      "distance_to_mid_pct": -0.00,
      "source_kind": "footprint_price_cluster",
      "source_window": "4h",
      "max_score": 0.0,
      "total_touch_count": 7.0,
      "representative_direction": "bullish | bearish | none | mixed(fallback_only)",
      "reference_ts_latest": "2026-03-18T08:30:00+00:00"
    }
  ],
  "price_levels": []
}
```

### 6.6 字段说明

- `count`
  - 该 zone 内被合并的原始 structure 数量
- `source_kind`
  - 该 zone 所属的单一来源类型
  - 因为 `v6.1` 第一层分桶已经包含 `source_kind`，所以这里必然是单值
- `source_window`
  - 该 zone 所属的单一 timeframe/窗口
  - 因为 `v6.1` 第一层分桶已经包含 `source_window`，所以这里也必然是单值
- `max_score`
  - 该 zone 内最大 `score`
- `total_touch_count`
  - 聚合后的 `touch_count` 总和
- `representative_direction`
  - 聚合方向摘要；不是结论，只是原始结构方向的汇总
  - 理论上在 `source_kind + source_window + direction + side + state + event_type` 分桶后，不应出现 `mixed`
  - 但为容错起见，允许 `mixed` 作为 fallback：
    - 仅当原始结构缺失 `direction`，或某些 legacy 结构在聚合前未带可判定方向时出现
    - 正常实现中应优先产出 `bullish / bearish / none`
    - 若出现 `mixed`，只表示“方向信息不纯”，不表示模型结论

### 6.6.1 `representative_direction` 的 fallback 规则

默认优先级：

1. 若聚合桶有明确 `direction=bullish`，则为 `bullish`
2. 若聚合桶有明确 `direction=bearish`，则为 `bearish`
3. 若该类结构天然无方向，或全部原始项都无方向，则为 `none`
4. 只有在 fallback 容错场景下才允许 `mixed`

实现验收上，`mixed` 应视为异常低频值，而不是常规值。

### 6.6.2 为什么这里不用数组 provenance

`source_kind` 和 `source_window` 在 `v6.1` 中故意保持单值，不是遗漏，而是设计选择。

理由：

- `raw_overflow` 的首要目标是“保守聚合后仍保留清晰语义”
- 一旦为了保留 `source_kinds[] / source_windows[]` 数组而去掉 `source_kind / source_window` 分桶，就会增加以下风险：
  - 把不同结构类型误合并成同一个 zone
  - 把不同窗口的结构强行混为一体，削弱模型对结构来源的判断

因此：

- `v6.1` 先优先保证“聚合后的 zone 语义清晰”
- “多来源共振”的表达，后续若需要，应在更高层单独设计，而不是挤进当前的 `raw_overflow` 聚合层

### 6.7 对目标的影响

这是**增强**，因为：

- 原始细节仍在，但从“散点雨”变成了“结构带”
- 更利于模型把近端微结构读成候选支撑/阻力 zone
- 对区间判断尤其有帮助

---

## 七、改动四：`raw_overflow` 做“近端优先，远端稀疏”

### 7.1 当前问题

当前 `raw_overflow` 对近端和远端结构一视同仁。

结果是：

- 离当前价极近的密集 micro cluster 数量过多
- 离当前价较远但更高周期的重要结构，没有被“重要性放大”

模型会天然把视觉上更密集的近价结构当成更重要的内容。

### 7.2 v6.1 的处理方式

对 `raw_overflow.price_structures` 引入距离分层：

- `near`
- `mid`
- `far`

### 7.3 默认距离分层

以 `distance_to_mid_pct` 绝对值划分：

- `near`: `<= 0.30%`
- `mid`: `> 0.30% 且 <= 1.00%`
- `far`: `> 1.00%`

### 7.4 保留策略

- `near`
  - 聚合后保留更多 zone
  - 允许较细颗粒度
  - 默认上限：`36` 个 zone
  - 排序建议：`abs(distance_to_mid_pct) ASC -> max_score DESC -> total_touch_count DESC -> count DESC`
- `mid`
  - 聚合后按 `max_score / total_touch_count / count` 排序保留
  - 默认上限：`18` 个 zone
  - 排序建议：`max_score DESC -> total_touch_count DESC -> count DESC -> abs(distance_to_mid_pct) ASC`
- `far`
  - 只保留真正重要的结构类
  - 默认上限：`12` 个 zone
  - 例如：
    - `fvg_boundary`
    - `recent_swing_bracket`
    - `pvs_value_area`
    - `tpo_value_area`
    - `wall_band`
    - `liquidation_band`
    - 高分 `absorption_zone`
  - 远端普通 micro cluster 默认不进入最终产物

### 7.4.1 `far` 层高分 `absorption_zone` 的阈值

结合样本 [20260318T083000Z_ETHUSDT_scan_20260318T083257217Z.json](/data/systems/llm/temp_model_input/20260318T083000Z_ETHUSDT_scan_20260318T083257217Z.json) 中 `absorption_zone.score` 的分布：

- `count = 137`
- `p25 ≈ 0.513`
- `p50 ≈ 0.610`
- `p75 ≈ 0.685`
- `max ≈ 0.892`

`far` 层默认只允许保留：

- `score >= 0.70` 的 `absorption_zone`

若满足阈值的 `far absorption_zone` 仍然过多，则：

- 先按 `score DESC -> age_minutes ASC -> abs(distance_to_mid_pct) ASC` 排序
- 最多保留 `3` 个 `far absorption_zone`

这个阈值的目的，是让 `far` 层只保留真正有说明力的高分 absorption，而不是把远端普通 absorption 微结构继续平铺给模型。

### 7.5 重要约束

“远端稀疏”不等于“远端删除”。

必须始终保留对 `1d / 3d` 区间判断重要的 HTF 结构。

### 7.6 对目标的影响

这是**增强**：

- `15m` 不会被 200 多条微结构噪声包围
- `4h / 1d` 的关键远端 bracket 仍在
- 模型会更容易形成“近端执行 + 远端背景”的结构阅读

---

## 八、v6.1 后的关键结构草图

```jsonc
{
  "version": "scan_v6_1",
  "symbol": "ETHUSDT",
  "ts_bucket": "2026-03-18T08:30:00+00:00",
  "current_price": 2318.39,

  "now": {
    "price_anchor": {},
    "location_snapshot": {
      "vs_avwap": {},
      "vs_ema_band": {},
      "vs_rvwap": []
    },
    "value_state_board": [],
    "bracket_board": {
      "current_inside": [],
      "nearest_above": [],
      "nearest_below": [],
      "higher_context": []
    },
    "structure_nodes_near_current": {},
    "current_flow_snapshot": {},
    "current_volume_nodes": {}
  },

  "path_newest_to_oldest": {},
  "events_newest_to_oldest": {},
  "supporting_context": {},

  "raw_overflow": {
    "price_structures": [],
    "price_levels": []
  }
}
```

---

## 九、实施顺序建议

### P0

- 合并 `vs_value + acceptance_board -> value_state_board`
- 合并同边界 bracket 的多 scope 镜像

### P1

- `raw_overflow.price_structures` 改成聚合 zone

### P2

- `raw_overflow` 引入“近端优先、远端稀疏”的保留策略

---

## 十、验收标准

如果执行 `v6.1`，应满足：

1. `now.location_snapshot.vs_value` 删除
2. `now.acceptance_board` 删除
3. 新增 `now.value_state_board`
4. 同边界 bracket 不再因为 scope 不同而复制多份
5. `bracket_board` 的跨周期共振用 `scopes` 表达
6. `raw_overflow.price_structures` 数量显著下降
7. `raw_overflow` 中近端结构仍充分，远端 HTF 关键结构不丢
8. 产物整体更利于模型回答：
   - `15m` 当前方向是什么
   - `4h` 当前主 bracket 是什么
   - `1d` 背景区间和支撑/阻力在哪里

---

## 十一、最终判断

`v6.1` 不是为了“更小”，而是为了“更像候选证据包”。

只要实现时坚持：

- 不删关键事实
- 不混并不同语义结构
- 不丢关键 HTF 锚点

那么这四项改动不会削弱目标，反而会让 `scan filter -> 候选证据 -> 模型判断方向和区间` 这条链路更清晰。
