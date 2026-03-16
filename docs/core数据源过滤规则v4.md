# Core（Stage 2）数据源过滤规则 v4

**状态**：草案
**适用代码**：`systems/llm/src/llm/filter/core_entry.rs`
**适用阶段**：LLM Stage 2 — `entry_core` 为主；`management_core / pending_core` 同步采用 §4.1 方向拆分删除规则
**基线样本**：
- `systems/llm/temp_model_input/20260316T113100Z_ETHUSDT_entry_core_20260316T113319393Z.json`
- 该样本为 entry_core，已按 v3 规则过滤，minified **184KB**

---

## 一、设计原则

**第一目标**：LLM 读完数据后能独立推理出 entry、TP、SL 的具体价格，以及 15m / 4h / 1d 的方向和区间。

**第二目标**：在不损失锚点信息的前提下减少体积。

**核心约束**：
- 数据里**不得出现预计算的方向结论**（`bias`、`signal`、`direction_label` 等摘要字段），LLM 必须从原始事件、价格、成交量中自行推断。
- 所有删减需经实测验证；不以"语义重叠"为由盲目合并。
- 时间序列、事件序列统一 **newest-first**（最新在最前）。

### 1.1 允许保留的摘要字段边界

v4 允许保留的"摘要"仅限以下三类：

1. **几何锚点**：价格区间、宽度、fill_pct、touch_count、stack length、VAH/VAL/POC 等
2. **时效与强度**：`age_bars`、`displacement_score`、z-score、rolling stats、窗口聚合数值
3. **原始事件标签**：事件自身的 `direction`、`type`、`side`、`state`

v4 **不允许**保留以下会替 LLM 先下判断的字段：

- 直接给出 `bull/bear/neutral` 结论的 regime / bias / state
- 将一组原始事件折叠成 `signal=true/false` 的顶层判断
- 将连续数值硬分类成 `high/low/toxic` 等离散标签

原则上：**entry_core 可以保留证据压缩，但不能保留结论压缩。**

---

## 二、v3 规则实施确认（v4 继承）

以下 v3 规则已在基线样本中全部落地，v4 直接继承：

| v3 规则 | 实测状态 | 验证数据 |
|---------|---------|---------|
| `value_area_levels` → `va_top_levels`（top-N by volume） | ✅ 已实现 | 15m=15条，4h/1d/3d=10条 |
| `liq_trend_hourly` 截断 48h + `liq_spike_events` | ✅ 已实现 | len=48，spike len=10 |
| `funding_trend_hourly` 截断 24h + `funding_trend_stats` | ✅ 已实现 | len=24，stats 字段完整 |
| CVD series → 5字段（删 `close_fut/spot`, `relative_delta_*`, `spot_flow_dominance`） | ✅ 已实现 | 字段集正确 |
| 所有事件类 `recent_7d.events` → 最近 10 条 | ✅ 已实现 | 全部=10 |
| `footprint.buy/sell_imbalance_prices` 删除 | ✅ 已实现 | 字段不存在 |
| `orderbook_depth` 保留 `top_liquidity_levels`（±2% 过滤 20 条）+ `liquidity_walls` | ✅ 已实现 | top=20，walls 完整 |

---

## 三、v3 实测发现的字段问题（v4 修正）

### 3.1 事件类字段保留列表 — 按指标类型分类

v3 给出了一张通用的 17 字段保留列表，但实测发现三类事件（absorption / exhaustion / initiation）字段结构不同，通用列表无法正确映射。v4 按类型分别定义。

#### absorption 类（`absorption` / `bullish_absorption` / `bearish_absorption`）

实测当前字段：18 字段。全部保留，无需额外删减。

```
confirm_ts, delta_sum, direction, event_end_ts, event_start_ts,
key_distance_ticks, pivot_price, price_high, price_low,
rdelta_mean, reject_ratio, score, sig_pass, spot_confirm,
stacked_buy_imbalance, stacked_sell_imbalance, trigger_side, type
```

> `spot_confirm`（bool）是 v3 规定保留的结论字段（替代 `spot_flow_confirm_score`），已正确实现。

#### exhaustion 类（`selling_exhaustion` / `buying_exhaustion`）

实测当前字段：21 字段。全部保留，无需额外删减。

exhaustion 特有的以下字段对 entry/SL 判断**有独立价值**，不应按 v3 通用列表删除：

| 字段 | 用途 |
|------|------|
| `exhaustion_quality_score` | 枯竭事件综合质量，直接影响 LLM 信心权重 |
| `spot_continuation_risk` | bool — 现货是否有延续风险，影响 SL 放置 |
| `spot_exhaustion_confirm` | bool — 现货确认枯竭，是多空判断的独立信号 |
| `price_push_ticks` | 推进幅度，用于判断 TP 空间是否足够 |
| `rdelta_lift`/`rdelta_drop` | delta 强度，无法从 cvd_pack 还原 |
| `spot_cvd_push_post_pivot` / `spot_whale_push_post_pivot` | pivot 后的延续确认，SL 定位辅助 |

#### initiation 类（`initiation` / `bullish_initiation` / `bearish_initiation`）

实测当前字段：22 字段。全部保留，无需额外删减。

initiation 特有的以下字段对 entry 判断**有独立价值**：

| 字段 | 用途 |
|------|------|
| `break_mag_ticks` | 突破幅度，判断突破强度 |
| `follow_through_hold_ok` | bool — 跟进持续性确认 |
| `follow_through_max_adverse_excursion_ticks` | 突破后最大回撤，SL 参考 |
| `z_delta` | delta z-score，突破信号强度归一化 |
| `spot_break_confirm` / `spot_whale_break_confirm` | 现货/鲸鱼双重确认 |

### 3.2 当前基线中仍存在的"预计算结论"字段（v4 修正）

虽然 v3 体积已经大幅压缩，但基线样本里仍保留了几类会直接引导 LLM 的字段：

| indicator | 当前字段 | 问题 |
|----------|---------|------|
| `ema_trend_regime` | `trend_regime`, `trend_regime_by_tf`, `output_sampling.*.trend_regime` | 直接给出 bull/bear 结论，LLM 容易照单全收 |
| `divergence` | `signal`, `signals` | 把背离事件先折叠成 true/false 顶层判决，弱化 LLM 自己看事件的能力 |
| `vpin` | `toxicity_state` | 将连续 VPIN 数值预先离散化为 `high/normal/...`，属于算法结论，不是原始证据 |
| `high_volume_pulse` | `is_volume_spike_z2`, `is_volume_spike_z3` | 将连续 `volume_spike_z_w` 硬阈值化为 bool，属于结论压缩 |

这些字段与 Stage 2 的目标冲突：Stage 2 已经有 Stage 1 的市场方向背景，`entry_core` 更应该提供结构、流动性、价格锚点和事件本身，而不是再给一套二次方向判决。

---

## 四、v4 新增优化规则

### 4.1 删除事件类方向拆分组 ← 最大冗余项

**实测冗余量**：

| 冗余组 | 组合大小 | 三份合计 | 可删除 |
|--------|---------|---------|-------|
| `initiation` + `bullish_initiation` + `bearish_initiation` | 7.5KB | 22.3KB | **14.8KB** |
| `absorption` + `bullish_absorption` + `bearish_absorption` | 5.4KB | 16.1KB | **10.7KB** |
| **合计** | | | **25.5KB（67% 冗余）** |

**为什么是冗余**：

`initiation` 里的每个事件都有 `direction` 字段（`1` = 多头启动，`-1` = 空头启动）。LLM 可以直接过滤 `direction` 字段，无需预拆分的方向版本。`bullish_initiation` 的 10 条事件是从全量事件中取最近 10 条 `direction=1` 的，与 `initiation` 的内容有大量重叠（实测交集：bull 4/10，bear 6/10）。

**关键风险**：方向拆分组让 LLM 看到"10 个连续多头启动信号"——但这些信号的时间跨度可能长达 7 天，最近 1 天内只有 1-2 个。**这会系统性地误导 LLM 高估某一方向的近期强度。** 保留混合的 `initiation`（含 `direction` 字段）反而更中立。

**v4 规则**：

```
删除：bullish_initiation、bearish_initiation、bullish_absorption、bearish_absorption
保留：initiation（含 direction 字段）、absorption（含 direction / type 字段）
```

**预期节省：约 25.5KB**

---

### 4.2 footprint `buy_stacks` / `sell_stacks` 按窗口设最小长度过滤

**实测问题**：

| 窗口 | buy_stacks 总数 | length=3 占比 | 体积 |
|------|--------------|-------------|------|
| 15m | 34 | 44% | 2.0KB |
| 4h | 62 | 47% | 3.7KB |
| **1d** | **375** | **50%** | **22.3KB** |

1d 窗口 375 条 stacks 中，188 条 length=3（连续仅 3 个价格档的不平衡区），在 1d 时间粒度下是噪声，不是有效的 entry/SL 锚点。对 1d 窗口，length >= 7 才代表机构级支撑/压力带。

**为什么不能整体删除短 stacks**：在 15m 窗口，length=3 的 stack 是有意义的（15 分钟内 3 档连续不平衡 = 实质支撑）。只有在更长的时间窗口中，短 stack 才变成噪声。

**v4 规则**：按窗口设最小保留长度：

| 窗口 | min_stack_length | 理由 |
|------|----------------|------|
| 15m | 3 | 短周期执行窗口，3档已有意义 |
| 4h | 4 | 4h窗口内3档过短，4档起才有结构意义 |
| 1d | 7 | 日线窗口，7档以上才是可靠价格带 |

实测效果（buy_stacks）：

| 窗口 | 当前 | 过滤后 | 节省 |
|------|------|-------|------|
| 15m | 34 条 2.0KB | 19 条 1.2KB | 0.8KB |
| 4h | 62 条 3.7KB | 23 条 1.5KB | 2.2KB |
| 1d | 375 条 22.3KB | 45 条 2.7KB | **19.6KB** |

sell_stacks 同规则，节省约等比。

**预期节省（buy+sell 合计）：约 22KB**

---

### 4.3 TPO `dev_series` 截断为最近 N 条

**实测问题**：

`tpo_market_profile.by_session.1d.dev_series.15m` 含 **45 条记录**（4.9KB），是当天 1d 价值区（POC/VAH/VAL）每 15 分钟的演化历史，从今天开盘至当前时刻的全量快照。

对 entry/TP/SL 决策，LLM 需要的是：
1. 当前 POC/VAH/VAL（已在 `tpo_poc / tpo_vah / tpo_val` 标量字段中）
2. 最近 1-2 小时价值区是否漂移、速度如何（帮助判断区间稳定性）

全天 45 条对 entry 没有额外价值，只占用上下文。

**v4 规则**：

```
by_session.*.dev_series.15m → 截断为最近 8 条（newest-first，约 2 小时窗口）
by_session.*.dev_series.1h  → 截断为最近 5 条（newest-first，约 5 小时窗口）
```

实测节省（1d session dev_series.15m：45条→8条）：

```
原始：4,865 chars → 截断后：858 chars → 节省 4,007 chars（~4.0KB）
```

**预期节省：约 4.3KB**

---

### 4.4 FVG 字段精简（每条 24 字段 → 9 字段）

**实测问题**：每个 FVG 事件有 24 个字段，其中 15 个是几何计算中间量，对 LLM 没有独立信息价值。

**v4 保留字段（9 个）**：

| 字段 | 用途 |
|------|------|
| `fvg_bottom` / `fvg_top` | 价格区间（entry/TP/SL 锚点） |
| `side` | `bull` / `bear` — 缺口方向 |
| `state` | `fresh` / `partially_filled` / `fully_filled` — 填充状态 |
| `fill_pct` | 已回填比例（0=未填，1=完全填充） |
| `touch_count` | 测试次数（高 = 价格重复回测该缺口） |
| `displacement_score` | 位移强度（0-1），反映缺口是否由大 impulse 产生 |
| `age_bars` | 缺口年龄（多少根 bar 前生成）|
| `width` | 缺口宽度（点数），直接用于 TP 定位 |

**v4 删除字段（15 个）**：

```
body_ratio_mid, close_location_mid, distance_to_avwap,
impulse_atr_ratio, inside_value_area, left_ts, lower, mid, mid_ts,
overlap_lvn, spot_confirm_at_birth, upper, width_atr_ratio,
birth_ts（用 age_bars 替代）, event_available_ts
```

删除理由：
- `lower/upper/mid` 与 `fvg_bottom/fvg_top` 重复（仅命名不同）
- `body_ratio_mid`, `close_location_mid`, `impulse_atr_ratio` 是衍生比例，`displacement_score` 已整合
- `birth_ts`, `left_ts`, `mid_ts` 三个时间戳过多，`age_bars` 已表达时效信息
- `distance_to_avwap` 实测多为 `None`（null）

实测节省：

```
15m fvgs（6条）：4,240 chars → 1,265 chars → 节省 2,975 chars（70%）
4h fvgs（2条）：同比例，节省约 1KB
```

**预期节省：约 3KB**

---

### 4.5 删除预计算方向 / 信号摘要字段

**原则**：Stage 2 需要的是可追溯的证据锚点，不是指标自己的最终判决。Stage 1 scan 已经提供大周期方向背景，`entry_core` 不应重复给出 bull/bear/signal 结论。

#### `ema_trend_regime`

**保留**（数值证据）：

```text
as_of_ts
ema_13, ema_21, ema_34
ema_band_high, ema_band_low
ema_100_htf, ema_200_htf
output_sampling.*.ts
```

**删除**（结论标签）：

```text
trend_regime
trend_regime_by_tf
output_sampling.*.trend_regime
```

#### `divergence`

**保留**（事件证据）：

```text
recent_7d
latest_7d
event_count
candidates
```

**删除**（顶层信号判决）：

```text
signal
signals
```

> 如后续实测发现 `reason` 也会明显引导 LLM，可进一步删除；v4 先优先移除最强的 `signal/signals`。

#### `vpin`

**保留**（连续数值）：

```text
vpin_fut, vpin_spot, vpin_ratio_s_over_f
xmk_vpin_gap_s_minus_f
z_vpin_fut, z_vpin_gap_s_minus_f, z_vpin_spot
by_window.*
```

**删除**（离散标签）：

```text
toxicity_state
```

#### `high_volume_pulse`

**保留**（连续数值）：

```text
by_z_window.*.volume_spike_z_w
by_z_window.*.rolling_volume_w
by_z_window.*.lookback_samples
by_z_window.*.window_minutes
as_of_ts
indicator
intrabar_poc_price
intrabar_poc_volume
intrabar_poc_max_by_window
window
```

**删除**（布尔阈值标签）：

```text
by_z_window.*.is_volume_spike_z2
by_z_window.*.is_volume_spike_z3
```

理由：旁边已经有 `volume_spike_z_w` 连续值，LLM 可自行判断是否达到 z2 / z3 阈值；保留布尔字段会把同一信息以“算法已判定”的形式重复输出。

**预期节省**：体积收益小（<1KB），但可显著减少对 LLM 的方向引导。

---

### 4.6 通用 null / 非适用字段清理

实测发现当前基线样本中有多类固定 `null` 或"当前不存在但仍强行输出"的字段。这类字段体积收益不大，但会增加 LLM 的误解成本。

#### `divergence` 顶层 null

当前有 4 个固定输出为 `null` 的字段：

```json
"divergence_type": null,
"likely_driver": null,
"pivot_side": null,
"spot_lead_score": null
```

这 4 个字段仅在有活跃背离信号时才有值；无信号时应直接省略而非输出 `null`。

#### `fvg` null 占位

实测当前样本中存在以下无信息量 null：

```text
nearest_bull_fvg = null
nearest_bear_fvg = null
distance_to_avwap = null
spot_confirm_at_birth = null
```

v4 规则：

- `nearest_*_fvg` 仅在真实存在时输出；不存在则直接省略该字段
- 单个 FVG 条目中的 `distance_to_avwap`、`spot_confirm_at_birth` 若为 null，则直接省略

#### 其他非适用字段

实测当前样本中还存在：

```text
cvd_pack.by_window.*.series[*].xmk_delta_gap_s_minus_f = null
price_volume_structure.by_window.3d.volume_zscore = null
```

这类字段都属于"当前窗口/当前数据源下无值"，应统一按以下规则处理：

**v4 规则**：

- 若字段值为 `null`，直接省略该字段，不输出 `null`
- 若对象内所有可选字段都为空，则只保留仍有信息量的字段
- 若当前没有该类事件，保留空数组 `[]`，**不要**用 `null` 表示"没有事件"

这条规则适用于所有 indicator，而不仅限于 `divergence`。

**预期节省**：体积收益小（<1KB），但清晰度收益明显。

---

## 五、v4 优化效果汇总

### 相对 v3 基线（184KB minified）的新增节省

| 规则 | 节省 | 主要来源 |
|------|------|---------|
| 4.1 删除方向拆分事件组 | **~25.5KB** | bullish/bearish_initiation + absorption |
| 4.2 footprint stacks min_length 过滤 | **~22KB** | 1d buy/sell_stacks 降噪 |
| 4.3 TPO dev_series 截断 | **~4.3KB** | 1d session 15m 快照 45→8 条 |
| 4.4 FVG 字段精简 | **~5KB** | 每条 24→9 字段，同时作用于 fvgs[]、active_*_fvgs[]、nearest_*_fvg |
| 4.5 删除预计算方向/信号摘要字段 | <1KB | ema/divergence/vpin/high_volume_pulse 结论标签 |
| 4.6 通用 null / 非适用字段清理 | <1KB | FVG/CVD/PVS/divergence |
| **v4 合计新增节省** | **~65KB** | |

### 体积路径

```
原始 temp_indicator: ~73MB
v2 过滤后:           ~400KB
v3 过滤后:            184KB（基线）
v4 目标:              ~120KB（实测 119.3KB，优于预期 130KB）
```

---

## 六、实现备注

### 6.1 stacks min_length 过滤逻辑

```rust
// 按窗口配置最小保留长度
let min_len: usize = match window {
    "15m" => 3,
    "4h"  => 4,
    "1d"  => 7,
    _     => 3,
};
buy_stacks.retain(|s| s.length >= min_len);
sell_stacks.retain(|s| s.length >= min_len);
```

### 6.2 FVG 字段白名单

```rust
const FVG_KEEP: &[&str] = &[
    "fvg_bottom", "fvg_top", "side", "state",
    "fill_pct", "touch_count", "displacement_score",
    "age_bars", "width",
];
// 过滤：只输出 FVG_KEEP 中的字段
```

### 6.3 TPO dev_series 截断

```rust
// newest-first 已保证；直接截断前 N 条
dev_series_15m.truncate(8);
dev_series_1h.truncate(5);
```

### 6.4 方向拆分删除范围

完整删除以下 4 个顶层 indicator key：
```
bullish_initiation
bearish_initiation
bullish_absorption
bearish_absorption
```

保留 `initiation`（含 `direction: 1/-1`）和 `absorption`（含 `direction: 1/-1` 与 `type: bullish_absorption/bearish_absorption`）。

### 6.5 预计算结论字段删除

```rust
// ema_trend_regime: 保留 EMA 数值，删除 bull/bear regime 标签
ema.remove("trend_regime");
ema.remove("trend_regime_by_tf");
for sample in ema["output_sampling"].values_mut() {
    sample.remove("trend_regime");
}

// divergence: 保留事件列表，删除顶层 signal 判决
divergence.remove("signal");
divergence.remove("signals");

// vpin: 保留连续数值，删除 toxicity_state
vpin.remove("toxicity_state");

// high_volume_pulse: 保留 z-score 连续值，删除布尔阈值标签
for bucket in high_volume_pulse["by_z_window"].values_mut() {
    bucket.remove("is_volume_spike_z2");
    bucket.remove("is_volume_spike_z3");
}
```

### 6.6 通用 null 省略规则

```rust
fn insert_if_not_null(dst: &mut Map<String, Value>, key: &str, value: Value) {
    if !value.is_null() {
        dst.insert(key.to_string(), value);
    }
}

// 适用于 divergence/FVG/CVD/PVS 等所有可选字段
// 规则：null 直接省略；[] 保留，表示当前无事件
```

---

## 七、验证方法（Python 断言脚本）

每次过滤后运行以下断言：

```python
# v3 继承规则
assert "value_area_levels" not in pvs_15m                    # va_top_levels 已替换
assert len(pvs_15m["va_top_levels"]) <= 15
assert len(liq["liq_trend_hourly"]) <= 48
assert len(funding["funding_trend_hourly"]) <= 24
assert "buy_imbalance_prices" not in fp_15m                  # footprint 原始 imbalance 列表删除
assert all(len(s["series"][0]) == 5 for s in cvd_windows)   # CVD series 5字段
assert all(len(ev["recent_7d"]["events"]) <= 10
           for ev in event_indicators)                        # 事件最多 10 条

# v4 新规则
assert "bullish_initiation" not in indicators                 # 方向拆分已删除
assert "bearish_initiation" not in indicators
assert "bullish_absorption" not in indicators
assert "bearish_absorption" not in indicators

ema = indicators["ema_trend_regime"]["payload"]
assert "trend_regime" not in ema                              # EMA 结论标签已删除
assert "trend_regime_by_tf" not in ema
for sample in ema.get("output_sampling", {}).values():
    assert "trend_regime" not in sample

divergence = indicators["divergence"]["payload"]
assert "signal" not in divergence                             # divergence 顶层信号判决已删除
assert "signals" not in divergence

vpin = indicators["vpin"]["payload"]
assert "toxicity_state" not in vpin                           # VPIN 离散标签已删除

hvp = indicators["high_volume_pulse"]["payload"]
for bucket in hvp.get("by_z_window", {}).values():
    assert "is_volume_spike_z2" not in bucket                 # HVP 布尔阈值标签已删除
    assert "is_volume_spike_z3" not in bucket
    assert "volume_spike_z_w" in bucket                       # 保留连续 z-score

fp_1d = footprint["by_window"]["1d"]
assert all(s["length"] >= 7 for s in fp_1d["buy_stacks"])   # 1d min_length=7
assert all(s["length"] >= 7 for s in fp_1d["sell_stacks"])

fp_4h = footprint["by_window"]["4h"]
assert all(s["length"] >= 4 for s in fp_4h["buy_stacks"])   # 4h min_length=4
assert all(s["length"] >= 4 for s in fp_4h["sell_stacks"])

tpo_dev_15m = tpo["by_session"]["1d"]["dev_series"]["15m"]
assert len(tpo_dev_15m) <= 8                                 # TPO dev_series 截断

fvg_item = fvg["by_window"]["15m"]["fvgs"][0]
assert "body_ratio_mid" not in fvg_item                      # FVG 字段精简
assert "fvg_bottom" in fvg_item and "width" in fvg_item     # 关键字段保留

# 通用 null 清理：整份 JSON 不应出现 null
def assert_no_nulls(node):
    if node is None:
        raise AssertionError("null field found")
    if isinstance(node, dict):
        for v in node.values():
            assert_no_nulls(v)
    elif isinstance(node, list):
        for v in node:
            assert_no_nulls(v)

assert_no_nulls(filtered_json)
```

---

## 八、本版明确不做的优化

| 提案 | 排除原因 |
|------|---------|
| 删除 `orderbook_depth` 顶层标量（obi/ofi 等） | 实测：顶层标量是**实时快照**，`by_window.15m` 是 15 分钟 TWA — 两者数值完全不同，不是重复，各有独立含义 |
| 用"方向标量"替代 `divergence.recent_7d.events` | divergence 事件含 `p_value`、`leg_minutes`、`pivot_side` 等无法折叠的时序信息，10 条 8.7KB 已合理 |
| 删除 `initiation` 保留 `bullish/bearish_initiation` | 反向操作会损失混合方向的时序对比能力，LLM 需要在同一列表中看到多空交替才能判断强弱比 |
| 保留 `ema_trend_regime.trend_regime` 这类 bull/bear 标签 | Stage 2 已有 Stage 1 scan 方向背景；entry_core 应提供数值证据，不应重复给结论标签 |
| 保留 `high_volume_pulse.is_volume_spike_z2/z3` | 同一位置已保留 `volume_spike_z_w` 连续值；布尔阈值属于结论压缩，不额外保留 |
| `whale_trades.by_window` 压缩 | 总大小 3.3KB，窗口级鲸鱼行为对 entry 方向判断有直接价值，不压缩 |
| 删除 `kline_history` | 6.3KB，提供近期 K 线 OHLCV，LLM 需要此数据做价格位置和动量判断 |
