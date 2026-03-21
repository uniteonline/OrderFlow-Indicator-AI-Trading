# Orderflow Indicator Engine Backfill 优化方案

更新时间：2026-03-21  
适用对象：`orderflow-indicator-engine`  
目标：解决服务重启后 startup backfill 追 live 过慢的问题，同时不牺牲指标正确性

---

## 1. 结论

本次问题的推荐解决方向只有一套，不再拆成多个竞争方案：

1. `snapshot` 合约统一成 `9d + source floor`
2. startup replay 保留严格重算
3. replay 期间从主路径剥离 live 副作用，尤其是 RabbitMQ publish 和 publish confirm 等待

一句话概括：

> 让 startup backfill 只为 correctness 付费，不再为 live delivery 付费。

---

## 2. 先明确什么不是问题

`backfill` 期间需要按时间顺序 replay，并持续重建 rolling state，这件事本身不是 bug。

原因很简单：

- engine 的多个指标依赖分钟级历史状态
- rolling 7d / 3d / 1d / 4h 等窗口都要求时间顺序一致
- dirty recompute 语义也要求“旧分钟修正后，后续分钟要重新算”

因此：

- `严格重算` 是正确性要求
- `严格重算导致存在计算成本` 是正常现象

真正的问题不是“要不要重算”，而是：

- 当前 recovery path 复用了 live path
- 导致 startup replay 除了重算，还在同步做完整副作用链路
- 于是 backfill 变成了“重算 + 写库 + 发消息 + 等 broker confirm”的串行过程

这才是当前启动恢复极慢的主要原因。

---

## 3. 问题归因

### 3.1 Snapshot 合约不一致

当前代码语义里有 3 个不同口径：

- `StateStore` 保留 `9d` 历史
- startup 在没有可用 snapshot 时，也会把 backfill 扩到这段历史
- 但 snapshot save/load 只要求 `>= 1440 bars`

这会导致两个问题：

1. snapshot 可用性判断过松或过紧都不稳定
2. restart 语义和 runtime 的真实历史依赖不一致

结果表现为：

- 明明有 snapshot，但经常因为不合适的校验被拒绝，退回 full replay
- 或者保存了一个 technically 可读、但不足以支撑恢复语义的 snapshot

### 3.2 Startup replay 和 live 副作用耦合

当前 startup replay 期间，历史 minute 会沿用 live 路径执行：

- 写 `feat.indicator_snapshot`
- 写 `feat.*` feature 表
- 写 level / event 表
- 发 minute bundle
- 发每个 indicator snapshot
- 同步等待 MQ publish confirm

这样做虽然“严谨”，但把恢复过程变成了高计算量叠加高 I/O 延迟的串行链路。

### 3.3 旧前沿表现成“总卡在同一个时间点”

`indicator_progress.last_success_ts` 本质上只是“上一次已经成功 materialize 的最晚分钟”。

因此在本次 replay 重新跑回这个时间点之前：

- service 实际上可能一直在工作
- 但外部看到的 `last_success_ts` 会长时间停在旧前沿

这会造成“每次都卡在同一个时间点”的错觉。

---

## 4. 方案总览

本方案分为两个核心面：

1. `snapshot 合约统一`
2. `startup replay 执行路径分层`

这两个面一起落地，才能真正解决当前 restart 后 backfill 慢的问题。

---

## 5. Snapshot 合约：统一为 9d + source floor

### 5.1 为什么不是 1440 bars

`1440 bars` 只是 1 天。

它不足以表达当前 engine 的真实恢复语义，因为：

- runtime 内部 retention 是 `9d`
- 多个特征和 payload 明确依赖 `recent_7d` / `rolling_7d`
- restart 后希望恢复到“尽量接近未重启实例”的状态

所以继续沿用 `1440 bars` 作为 snapshot save/load 门槛，不具备一致性。

### 5.2 为什么这里建议定成 9d，而不是只定成 7d

这里建议统一到 `9d`，不是因为业务指标显式依赖 9d，而是因为：

- `7d` 是当前最小 correctness window
- `9d` 是 engine 现在真实维护的 state retention

如果 snapshot 只满足 7d：

- 理论上可能足以支撑当前 rolling_7d 指标
- 但不保证恢复后的内部状态与持续运行实例一致

如果 snapshot 满足 9d：

- 能覆盖当前 retention 契约
- restart 恢复语义与 runtime 保持一致
- 未来即使新增依赖近 7d 以上缓冲的指标，也不容易再次出合同错位

因此，本方案定义：

- `required_resume_history_minutes = HISTORY_LIMIT_MINUTES = 12960`

### 5.3 Source floor 与 continuity floor 的定义

不能机械要求“必须满 9d”，因为数据库可能本身没有这么长的可回放数据。

因此必须引入 3 个概念：

1. `raw_source_floor_ts`
   - 该 symbol 在 canonical `md.agg.*.1m` 回放路径上可获得的最早 minute bucket

2. `continuity_floor_ts`
   - 面向本次 restart/replay，可安全接续到当前 replay 尾部的“最后一段连续数据”的起点

3. `effective_source_floor_ts`
   - 真正用于 snapshot 合约和 startup replay 的地板
   - 定义为 `max(raw_source_floor_ts, continuity_floor_ts)`

这里强调的是：

- 不是“交易所理论上应该有的数据起点”
- 而是“当前数据库里该 symbol 真实可回放的最早 canonical minute”
- 更不是“只要数据库里更早还有老数据，就一定要把它接到本次 replay 上”

### 5.3.1 为什么需要 continuity floor

存在一个必须处理的 corner case：

- 机器之前死机
- 导致数据库里有一整段 canonical minute 数据缺失
- 缺失段之前还有更老的数据，缺失段之后又恢复正常

如果此时仍然把“缺失段之前的老数据”接到“缺失段之后的恢复数据”上一起 replay：

- rolling 指标会跨越一个实际上不存在的数据断层
- 内存状态会带入错误的历史上下文
- 结果会表现为指标异常、coverage 失真，甚至出现错误计算

因此本方案明确规定：

- 遇到 `不可恢复的历史断层` 时，不跨断层接历史
- 本次 replay 只从“最后一段连续数据”的起点开始
- 断层之前的老数据不参与当前 rolling state 重建

这里的“抛弃之前的不连续数据”，是指：

- 在本次 startup 恢复语义里，不再把 gap 之前的数据接入当前状态链
- 不是要求物理删除数据库历史

### 5.3.2 continuity floor 如何确定

建议规则：

1. 先确定本次目标 replay 尾部，比如 `last_finalized_ts` 或 `startup_replay_cutoff_ts`
2. 从该尾部向前回看，寻找最近一段能够连续接到尾部的 canonical minute run
3. 如果中间存在不可恢复 gap，则 `continuity_floor_ts` 定义为“最后一个 gap 之后的第一根连续 minute”

需要注意：

- `continuity` 的判定应基于构建 minute history 所需的最小价格主源
- 不能把天然稀疏的辅助源单独缺失，误判成整体历史断裂
- 例如 `liq` / `funding_mark` 可能天然稀疏，它们单独缺失不应构成 continuity break
- 真正的 continuity break 应该是“价格主链本身已经断了，无法可靠构造分钟历史”

### 5.4 Snapshot 有效性契约

对于某个 snapshot，定义：

- `last_finalized_ts` = 该 snapshot 覆盖的最新 finalized minute
- `required_floor_ts = max(last_finalized_ts - 9d, effective_source_floor_ts)`

只有同时满足以下条件，snapshot 才算可用于 resume：

1. version / symbol / age 校验通过
2. history 在 `required_floor_ts -> last_finalized_ts` 上连续
3. snapshot 的起点不晚于 `required_floor_ts`
4. 如果数据库历史里存在不可恢复 gap，snapshot 不能跨越该 gap 把更老数据接进当前恢复窗口
5. 历史里的老缺口如果落在 `effective_source_floor_ts` 之前或不侵入最近恢复窗口，不应单独成为拒绝理由
6. 真正侵入最近恢复窗口的尾部断裂、尾部长 null run、尾部不连续，才应拒绝

### 5.5 Save 合约

save 规则要和 load 规则对齐，避免“保存了一份自己下次又不认的 snapshot”。

因此建议：

1. 只在 snapshot 满足 `9d + effective source floor` 合约时，才标记为可恢复 snapshot
2. mid-backfill 退出时，如果当前 snapshot 不满足该合约，不要覆盖已有的更优 snapshot
3. snapshot 保存需要带明确元数据，至少包括：
   - `last_finalized_ts`
   - `covered_from_ts`
   - `raw_source_floor_ts`
   - `continuity_floor_ts`
   - `effective_source_floor_ts`
   - `saved_at`
   - `history_len_minutes`

### 5.6 没有 9d 数据或历史中间有断层时怎么处理

这是本方案的兜底规则：

- 如果数据库里没有 9d 数据，就从数据库第一根可回放 minute 开始
- 如果数据库历史中间存在不可恢复断层，就从最后一段连续数据开始
- 也就是 `required_floor_ts = max(last_finalized_ts - 9d, effective_source_floor_ts)`

因此不会出现“因为数据库历史不够 9d，所以永远拒绝 snapshot，永远 full replay”的情况。

同样也不会出现另一种错误情况：

- 为了拼满 9d
- 强行把 gap 之前的老历史接到 gap 之后的当前数据上
- 结果把 rolling state 重新接错

当连续可用历史不足 9d 时，系统应该接受“当前 coverage 不足”，而不是伪造一条跨断层的连续历史。

---

## 6. Startup Replay：保留严格重算，但拆掉 live 副作用

### 6.1 基本原则

startup replay 仍然必须：

- 按分钟顺序 ingest canonical 1m 数据
- 重建 `state_store`
- 重建 rolling history
- 在需要时执行 dirty recompute
- 但 replay 起点必须服从 `effective_source_floor_ts`

这些都保留，不降级。

但是 startup replay 不应该继续承担 live delivery 的职责。

### 6.2 建议拆成 3 个阶段

#### 阶段 A：Warm State

范围：

- 从 `replay_from_ts` 到 `persisted_frontier_ts`

行为：

- 严格 replay
- 严格重算
- 只重建内存 state
- 不写外部结果
- 不发 MQ

原因：

- 这段历史的外部结果数据库里通常已经存在
- replay 这段的目的是恢复内存状态，不是重复对外投递

#### 阶段 B：Gap Fill Materialization

范围：

- 从 `persisted_frontier_ts + 1m` 到 `startup_replay_cutoff_ts`

行为：

- 严格 replay
- 严格重算
- 只写必要的持久化结果
- 仍然不发 live MQ

这里“必要的持久化结果”至少包括：

- `feat.indicator_snapshot`
- `feat.indicator_progress`
- 业务上必须补齐的 `feat.* / evt.* / level` 表

但不应该包括：

- `publish_minute_bundle`
- `publish_snapshot`
- 同步等待 RabbitMQ publish confirm

原因：

- 这段是为了补数据库缺口
- 不是为了对下游做历史消息重放

如果未来确实需要历史重放给下游，应单独做历史 republisher，不应阻塞 startup 路径。

#### 阶段 C：Pure Live

范围：

- 收到 `event_bucket_ts >= startup_replay_cutoff_ts` 之后

行为：

- 切回完整 live path
- 开启 MQ publish
- 开启 publish confirm
- 正常推进 live processing

### 6.3 为什么这是当前问题的主解法

因为这会把启动恢复的成本分解成两类：

- `必须付的成本`：严格重算
- `可以从关键路径剥离的成本`：live 副作用

当前 3 小时级别的慢，明显不只是重算本身，而是重算叠加大量串行 I/O 的结果。

---

## 7. Replay 期间哪些副作用应该禁掉

### 7.1 必须从 startup replay 主路径移除

1. RabbitMQ `minute_bundle` 发布
2. RabbitMQ 单 indicator snapshot 发布
3. publish confirm 等待

### 7.2 应按阶段控制，而不是一刀切删除

建议不要把所有持久化都禁掉，而是按 replay 阶段区分：

- `Warm State`：不写外部结果
- `Gap Fill Materialization`：只写必要数据库结果
- `Pure Live`：恢复完整副作用

### 7.3 为什么不是“所有 replay 期间都什么都不写”

因为 startup replay 的后半段本质上是在补 gap：

- 如果完全不写库
- 那么 replay 结束后，数据库前沿仍然不会推进
- `indicator_progress` 也不会越过旧前沿

这就不能算真正追到了 live。

所以：

- `不写任何东西` 不是正确方案
- `只保留必要 materialization，移除 live delivery` 才是正确方案

---

## 8. 可观测性要求

当前 heartbeat 对 backfill 期不够友好，容易误导。

建议至少增加这些字段：

1. `startup_phase`
   - `warm_state`
   - `gap_fill_materialize`
   - `pure_live`

2. `replay_from_ts`
3. `replay_cutoff_ts`
4. `replay_cursor_ts`
5. `persisted_frontier_ts`
6. `snapshot_loaded`
7. `snapshot_covered_from_ts`
8. `mq_publish_enabled`
9. `mq_publish_confirm_enabled`

这样才能直接回答：

- 现在是在 replay 还是 live
- replay 已经算到哪
- 数据库已经写到哪
- snapshot 这次到底有没有吃上

---

## 9. 成功判据

本方案落地后，验收标准固定如下：

1. 重启后优先加载 snapshot，而不是频繁退回 full replay
2. 如果 snapshot 可用，startup replay 时间应接近“缺口分钟数”的线性成本
3. replay 期间不再产生历史 minute 的同步 MQ publish confirm 等待
4. `indicator_progress` 超过旧前沿的时间，应显著短于当前的 3 小时级别
5. 如果数据库本身没有 9d 数据，engine 应从 `effective_source_floor` 正常恢复，而不是永久拒绝 snapshot
6. 如果数据库中间存在不可恢复断层，engine 应从最后一段连续数据开始 replay，而不是跨断层拼接旧历史
7. shutdown 或再次重启时，不应把已有更优 snapshot 覆盖成更差的 mid-backfill snapshot

---

## 10. 预期效果

按本方案落地后：

- 有效 snapshot 情况下，restart 成本应主要取决于“缺口分钟数”，而不是重新扫 7d/9d 全量历史
- 即使不得不 full replay，关键路径也只剩“严格重算 + 必要 materialization”，不再叠加整条 live MQ side effects
- `indicator_progress` 不会再长时间停在旧前沿，误导为“卡死在同一时间点”

---

## 11. 非目标

本方案不追求以下事情：

1. 把 startup replay 变成近乎零成本  
   严格重算本身仍然会消耗 CPU 和时间

2. 用 snapshot 替代 correctness  
   snapshot 只是缩短恢复路径，不是替代 replay 校正

3. 在 startup 关键路径里顺便做历史消息补发  
   如果业务需要历史 MQ replay，应该做独立流程

---

## 12. 最终决策

本次 backfill 优化方案最终定为：

1. snapshot 合约按 `9d + source floor`
2. startup replay 保留严格重算
3. replay 主路径剥离 live 副作用，只保留必要 materialization

这是当前解决 `orderflow-indicator-engine` 重启后 backfill 过慢问题的主方案。
