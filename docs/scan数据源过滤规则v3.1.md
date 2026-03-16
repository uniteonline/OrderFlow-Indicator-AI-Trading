# Scan 数据源过滤规则 v3.1

**状态**：草案 rev2（待复核）
**基于**：v3（全量继承，仅记录增量变更）
**适用文件**：`systems/llm/src/llm/filter/scan.rs`
**基线体积**：v3 规则估算 ~111KB（minified）
**目标体积**：< 110KB（minified）
**与 v3 差异**：
1. `footprint` 混合选取改为**上下分开选**：下方选支撑候选，上方选阻力候选，分别保证两侧都有足够条目
2. 新增轻量 `level_book`：汇聚 PVS/TPO/RVWAP/FVG/OB 价格聚类，每个 timeframe 输出"下方前 2 支撑、上方前 2 阻力 + 来源"
3. 为 flow 事件指标新增 `timeframe_summary`：按 `event_start_ts` **嵌套窗口**分桶（15m=近4h, 4h=近24h, 1d=近7d），每窗口每方向三维选取（最近/最强/近价）≤3 条事件数组 + 计数 + bias
4. 修正 `support_candidates` / `resistance_candidates` 注释方向错误（v3 §七 7.1 注释写反）
5. §7.5 小优化全部落地（whale/vpin 1h/3d、avwap series、tpo dev_series、funding changes）

> **只为 Scan 服务**：以下规则仅针对 Stage 1 市场扫描提示词需求设计。
> 所有时间序列统一按时间 **newest-first** 输出。

---

## 一、v3.1 变更规则

### 3.6 footprint — 上下分开选（替换 v3 §3.1 的 mixed_select）

**问题**：v3 的 `mixed_select`（绝对距离 top10 + 强度 top10 去重）可能把 20 条全选在价格同侧——若当前价在区间低端，所有近价条目都在下方，上方的阻力候选因强度不足被排除。这导致 LLM 区间判断单边化。

**v3.1 规则**：stacks 和 clusters 各自按**上下分开**选取：

| 分组 | 条件 | 近价组 | 强度补充组 |
|------|------|-------|----------|
| 下方（支撑候选） | `end_price < current_price`（stacks）/ `p < current_price`（clusters） | 距离最近 top **5** | 强度最大 top **3** |
| 上方（阻力候选） | `start_price > current_price`（stacks）/ `p > current_price`（clusters） | 距离最近 top **5** | 强度最大 top **3** |
| 跨越当前价 | 区间包含 current_price（仅 stacks） | **全部保留**（通常 0-2 条） | — |

两侧各自去重，总数上限 `(5+3)×2 + cross ≤ 18` 条/字段（< v3 的 ≤20）。

```rust
fn split_select_stacks(
    stacks: &[Value],
    current_price: f64,
    near_n: usize,   // = 5
    strong_m: usize, // = 3
) -> Vec<Value> {
    let lo = |s: &Value| s.get("start_price").and_then(Value::as_f64).unwrap_or(0.0);
    let hi = |s: &Value| s.get("end_price").and_then(Value::as_f64).unwrap_or(0.0);
    let len = |s: &Value| s.get("length").and_then(Value::as_f64).unwrap_or(0.0);

    let below: Vec<_> = stacks.iter().filter(|s| hi(s) < current_price).cloned().collect();
    let above: Vec<_> = stacks.iter().filter(|s| lo(s) > current_price).cloned().collect();
    let cross: Vec<_> = stacks.iter().filter(|s| lo(s) <= current_price && hi(s) >= current_price).cloned().collect();

    let select_side = |side: Vec<Value>| -> Vec<Value> {
        let dist_below = |s: &Value| (hi(s) - current_price).abs();
        let dist_above = |s: &Value| (lo(s) - current_price).abs();
        // below 取 end_price 距离，above 取 start_price 距离
        let dist_fn: Box<dyn Fn(&Value) -> f64> = if side.first().map(|s| hi(s) < current_price).unwrap_or(false) {
            Box::new(|s: &Value| (hi(s) - current_price).abs())
        } else {
            Box::new(|s: &Value| (lo(s) - current_price).abs())
        };
        let mut by_dist = side.clone();
        by_dist.sort_by(|a, b| dist_fn(a).partial_cmp(&dist_fn(b)).unwrap_or(Ordering::Equal));
        let near: Vec<_> = by_dist.into_iter().take(near_n).collect();

        let mut by_str = side.clone();
        by_str.sort_by(|a, b| len(b).partial_cmp(&len(a)).unwrap_or(Ordering::Equal));
        let strong: Vec<_> = by_str.into_iter().take(strong_m).collect();

        let key = |s: &Value| -> (u64, u64) {
            ((lo(s) * 100.0) as u64, (hi(s) * 100.0) as u64)
        };
        let mut seen = std::collections::HashSet::new();
        near.into_iter().chain(strong).filter(|s| seen.insert(key(s))).collect()
    };

    let mut result = select_side(below);
    result.extend(select_side(above));
    result.extend(cross);
    result
}

fn split_select_clusters(
    clusters: &[Value],
    current_price: f64,
    near_n: usize,
    strong_m: usize,
) -> Vec<Value> {
    let p = |c: &Value| c.get("p").and_then(Value::as_f64).unwrap_or(0.0);
    let n = |c: &Value| c.get("n").and_then(Value::as_f64).unwrap_or(0.0);

    let below: Vec<_> = clusters.iter().filter(|c| p(c) < current_price).cloned().collect();
    let above: Vec<_> = clusters.iter().filter(|c| p(c) > current_price).cloned().collect();

    let select_side = |side: Vec<Value>, is_below: bool| -> Vec<Value> {
        let mut by_dist = side.clone();
        by_dist.sort_by(|a, b| {
            (p(a) - current_price).abs().partial_cmp(&(p(b) - current_price).abs()).unwrap_or(Ordering::Equal)
        });
        let near: Vec<_> = by_dist.into_iter().take(near_n).collect();

        let mut by_str = side.clone();
        by_str.sort_by(|a, b| n(b).partial_cmp(&n(a)).unwrap_or(Ordering::Equal));
        let strong: Vec<_> = by_str.into_iter().take(strong_m).collect();

        let key = |c: &Value| -> u64 { (p(c) * 100.0) as u64 };
        let mut seen = std::collections::HashSet::new();
        near.into_iter().chain(strong).filter(|c| seen.insert(key(c))).collect()
    };

    let mut result = select_side(below, true);
    result.extend(select_side(above, false));
    result
}
```

`filter_footprint` 中将原 `mixed_select_*` 调用替换为 `split_select_*`，函数签名不变（仍接受 `current_price: Option<f64>`，`None` 时退化为全局强度排序）。

**预估节省**：footprint 从 v3 ~12KB → **~10KB**（每字段上限 18 → 较 v3 的 20 稍减）

---

### 3.7 level_book — 轻量价格聚类（v3.1 新增）

**目标**：把 PVS/TPO/RVWAP/FVG/OB 价格层提前聚类，每个 timeframe 给 LLM 一个现成的"上方 2 阻力、下方 2 支撑"，消除让模型在 6 处数据源自己拼装区间的不稳定性。

**来源列表**（按 timeframe 分组采集，权重见下表）：

| 来源 | 字段路径 | 权重 |
|------|---------|------|
| PVS poc | `price_volume_structure.by_window[tf].poc_price` | 1.5 |
| PVS vah | `price_volume_structure.by_window[tf].vah` | 1.2 |
| PVS val | `price_volume_structure.by_window[tf].val` | 1.2 |
| PVS hvn | `price_volume_structure.by_window[tf].hvn_levels[]` | 1.3 |
| TPO poc | `tpo_market_profile.tpo_poc`（top-level，适用所有 tf）/ `by_session[tf].tpo_poc` | 1.5 |
| TPO vah | 同上 `.tpo_vah` | 1.2 |
| TPO val | 同上 `.tpo_val` | 1.2 |
| RVWAP ±1σ | `rvwap_sigma_bands.by_window[tf].rvwap_band_plus_1` / `rvwap_band_minus_1` | 1.0 |
| RVWAP ±2σ | 同上 `rvwap_band_plus_2` / `rvwap_band_minus_2` | 0.7 |
| FVG nearest | `fvg.by_window[tf].nearest_bull_fvg.fvg_top/bottom` / `nearest_bear_fvg.*` | 0.9 |
| OB bid walls | `orderbook_depth.liquidity_walls.bid_walls[0..2].price_level` | 0.8 |
| OB ask walls | `orderbook_depth.liquidity_walls.ask_walls[0..2].price_level` | 0.8 |

> TPO 数据注意：`by_session` 只有 `4h` 和 `1d` key，`15m` 使用 tpo 顶层 `tpo_poc`/`tpo_vah`/`tpo_val`。
> OB walls 无 timeframe 区分，全部 timeframe 均引用同一 `liquidity_walls`，只取最近的 2 个 bid/ask wall。
> AVWAP 不参与 level_book 聚类（当前样本中锚点已偏离 ~94 点，信噪比低；若 `|avwap_fut - current_price| < 2×ATR_1d` 则可加入，实现时按此条件判断）。

**聚类算法**：

bin_size 按 timeframe 分开（精度越细的周期用越小的 bin，避免 15m 级别把相邻但含义不同的价位合并）：

| timeframe | bin_size |
|-----------|----------|
| 15m | `max(current_price × 0.002, atr14_15m × 0.3)` 示例：max(4.3, 3.7) = **4.3** |
| 4h  | `max(current_price × 0.004, atr14_4h × 0.3)`  示例：max(8.7, ~7) = **~9** |
| 1d  | `max(current_price × 0.008, atr14_1d × 0.3)`  示例：max(17.4, ~18) = **~18** |

```
for each collected price level:
    bin_idx = floor(price / bin_size)
    bins[bin_idx].members.append({price, source_weight, source_label})
    bins[bin_idx].strength += source_weight

for each bin:
    # 代表价：选 bin 内离 current_price 最近的真实成员价
    # （不使用加权均值，避免产生"合成价位"）
    representative_price = min(members, key=|m| abs(m.price - current_price)).price

support_bins = [b for b in bins if representative_price < current_price]
resistance_bins = [b for b in bins if representative_price > current_price]

support_bins.sort_by(distance_to_current_price, ascending)
resistance_bins.sort_by(distance_to_current_price, ascending)

output top 2 from each
```

> **为什么用真实成员价而非加权均值**：scan prompt 要求支撑阻力是"数据源中真实存在的价位"。加权均值产出的是插值合成价，模型无法在原始数据中找到对应点位，反而引入歧义。离现价最近的真实成员价既保留了原始含义，又是 cluster 内"最敏感"的边界。

**输出格式**（每个 timeframe）：

```json
"level_book": {
  "15m": {
    "support": [
      {"price": 2165.5, "strength": 3.4, "sources": ["pvs_val", "rvwap_minus1", "ob_bid_wall"]},
      {"price": 2151.8, "strength": 1.7, "sources": ["rvwap_minus2", "ob_bid_wall"]}
    ],
    "resistance": [
      {"price": 2177.0, "strength": 2.9, "sources": ["rvwap_plus1", "pvs_vah", "ob_ask_wall"]},
      {"price": 2186.5, "strength": 3.6, "sources": ["pvs_poc", "tpo_vah", "rvwap_plus1_4h"]}
    ]
  },
  "4h": { ... },
  "1d": { ... }
}
```

**实现位置**：在 `build_scan_root` 或 `filter_indicators` 之后，作为独立计算步骤插入根对象，访问全量 `source` indicators。

```rust
struct AtrByTf { tf_15m: f64, tf_4h: f64, tf_1d: f64 }

fn bin_size_for_tf(tf: &str, current_price: f64, atr: &AtrByTf) -> f64 {
    match tf {
        "15m" => (current_price * 0.002).max(atr.tf_15m * 0.3),
        "4h"  => (current_price * 0.004).max(atr.tf_4h  * 0.3),
        "1d"  => (current_price * 0.008).max(atr.tf_1d  * 0.3),
        _     => (current_price * 0.005).max(atr.tf_15m * 0.5),
    }
}

fn build_level_book(source: &Map<String, Value>, current_price: f64, atr: &AtrByTf) -> Value {
    let mut by_tf = Map::new();
    for tf in SCAN_WINDOWS {
        let bin_size = bin_size_for_tf(tf, current_price, atr);
        let levels = collect_levels_for_tf(source, tf, current_price);
        let clusters = cluster_levels(&levels, bin_size, current_price);
        // representative_price = closest real member to current_price (not weighted mean)
        let support: Vec<_> = clusters.iter()
            .filter(|c| c.price < current_price)
            .take(2).map(to_json).collect();
        let resistance: Vec<_> = clusters.iter()
            .filter(|c| c.price > current_price)
            .take(2).map(to_json).collect();
        by_tf.insert(tf.to_string(), json!({"support": support, "resistance": resistance}));
    }
    Value::Object(by_tf)
}
```

`AtrByTf` 三个字段均从 `kline_history` 各 timeframe 的 bars 计算（已在 `volatility_summary` 路径上有 `compute_atr14`，对 3 个窗口各调用一次）。

`cluster_levels` 内的 representative_price 逻辑：
```rust
fn representative_price(members: &[(f64, f64, &str)], current_price: f64) -> f64 {
    // members: (price, weight, source_label)
    // 选离 current_price 最近的真实成员价
    members.iter()
        .min_by(|a, b| {
            (a.0 - current_price).abs()
                .partial_cmp(&(b.0 - current_price).abs())
                .unwrap_or(Ordering::Equal)
        })
        .map(|m| m.0)
        .unwrap_or(0.0)
}
```

**预估大小**：3 timeframes × 4 levels × ~80 bytes = **~1KB**（极小增量）

---

### 3.8 flow 事件 timeframe 分桶 + bias_summary（v3.1 新增）

**目标**：在不重构现有 `recent_7d.events` 的前提下，为每个事件指标附加轻量 `timeframe_summary`，让 LLM 直接知道"4h 框架内出现了几次空头吸收、最强信号是什么"，而不需要自己扫 10-20 条事件推断框架。

**适用指标**：`absorption`、`bullish_absorption`、`bearish_absorption`、`buying_exhaustion`、`selling_exhaustion`、`divergence`

**分桶规则**（嵌套窗口，基于 `event_start_ts` 与 `ts_bucket` 的相对时间）：

| 时间窗口 | 归属 timeframe | 说明 |
|---------|--------------|------|
| `ts_bucket - 4h` 到 `ts_bucket` | 15m | 最近 4h，覆盖最新事件 |
| `ts_bucket - 24h` 到 `ts_bucket` | 4h | 最近 24h，**包含 15m 窗口** |
| `ts_bucket - 7d` 到 `ts_bucket` | 1d | 最近 7d，**包含 4h/15m 窗口** |

> **为什么用嵌套而非互斥**：4h / 1d 方向判断也需要最新流。互斥分桶会让高周期摘要丢失最近 4h 的事件，导致模型看到的"4h 框架信号"排除了刚刚发生的最重要事件。嵌套窗口的代价是同一事件会在多个 timeframe 都计数——这是正确行为，因为一个近期强信号对 15m/4h/1d 三个框架都是有效信息。

**per-direction 三维选取**：在每个 timeframe 窗口内，对 bullish（direction=1）和 bearish（direction=-1）各保留最多 3 条，三维各取 1，去重后输出数组：

| 维度 | 排序字段 | 取 1 条 | 保留信息 |
|------|---------|---------|---------|
| `recent` | `event_start_ts` 降序 | 最新 1 条 | 时效性 |
| `strong` | `score` 降序 | 最强 1 条 | 信号强度 |
| `nearest` | `\|pivot_price - current_price\|` 升序 | 离现价最近 1 条 | 结构贴近度 |

三条去重（同一事件可能被多个维度选中），最终每方向输出 **1-3 条**，每条保留 4 个字段：`confirm_ts`、`pivot_price`、`score`、`type`。

**输出格式**（附加在各指标的 `payload` 内，与 `recent_7d` 并列）：

```json
"absorption": {
  "payload": {
    "recent_7d": { ... },    // v3 保留不变
    "timeframe_summary": {
      "15m": {
        "bullish_count": 0,
        "bearish_count": 2,
        "bias": "bearish",
        "bullish_events": [],
        "bearish_events": [
          // 三维选取后 1-3 条，覆盖时效/强度/结构三个维度
          {"confirm_ts": "2026-03-16T02:07:00+00:00", "pivot_price": 2188.45, "score": 0.724, "type": "bearish_absorption"},
          {"confirm_ts": "2026-03-16T01:45:00+00:00", "pivot_price": 2201.30, "score": 0.851, "type": "bearish_absorption"}
        ]
      },
      "4h": {
        "bullish_count": 8,
        "bearish_count": 12,
        "bias": "bearish",
        "bullish_events": [
          {"confirm_ts": "...", "pivot_price": 2155.20, "score": 0.612, "type": "bullish_absorption"}
        ],
        "bearish_events": [
          {"confirm_ts": "...", "pivot_price": 2201.30, "score": 0.851, "type": "bearish_absorption"},
          {"confirm_ts": "...", "pivot_price": 2188.45, "score": 0.724, "type": "bearish_absorption"},
          {"confirm_ts": "...", "pivot_price": 2175.00, "score": 0.680, "type": "bearish_absorption"}
        ]
      },
      "1d": { ... }
    }
  }
}
```

> 三维选取保证了每方向至少携带 `recent`（时效）、`strong`（强度）、`nearest`（结构）三个视角中的最优代表，LLM 不需要自己从 10-20 条事件中推断。去重后 1-3 条数组比单个 `best_*` 字段更完整，同时比直接暴露全量事件更紧凑。

**bias 计算**：
- `bullish_count > bearish_count × 1.5` → `"bullish"`
- `bearish_count > bullish_count × 1.5` → `"bearish"`
- 其余 → `"mixed"`

**实现位置**：在 `filter_event_indicator` 完成 `recent_7d` 过滤后，调用新函数 `build_timeframe_summary(payload, ts_bucket, current_price)` 并插入。`ts_bucket` 从根对象传入。

**预估大小**：6 指标 × 3 timeframes × ~200 bytes = **~3.6KB** 增量（但对 LLM 方向判断价值极高）

---

### 3.9 §7.5 小优化（全部落地）

以下优化基于实测体积，均为安全裁剪：

#### whale_trades / vpin 删除 1h、3d 窗口

实测：`whale_trades.by_window.1h` = 547B，`3d` = 641B；`vpin.by_window.1h` = 269B，`3d` = 270B。
scan 只用 `15m`/`4h`/`1d` 三个窗口做方向判断，`1h` 和 `3d` 对 stage1 无增量价值。

**改动**：`insert_full_indicator` 对 whale_trades / vpin 改为过滤函数，只保留 `15m`/`4h`/`1d`：

```rust
for code in ["vpin", "whale_trades", "high_volume_pulse"] {
    insert_filtered_indicator(&mut indicators, source, code, filter_keep_scan_windows);
}

fn filter_keep_scan_windows(payload: &Value) -> Value {
    let Some(payload) = payload.as_object() else { return Value::Null; };
    let mut result = clone_object_without_keys(payload, &["by_window"]);
    if let Some(by_window) = payload.get("by_window").and_then(Value::as_object) {
        let mut filtered = Map::new();
        for window in SCAN_WINDOWS {
            if let Some(v) = by_window.get(*window) {
                filtered.insert((*window).to_string(), v.clone());
            }
        }
        result.insert("by_window".to_string(), Value::Object(filtered));
    }
    Value::Object(result)
}
```

**节省**：~1.7KB

#### tpo_market_profile.dev_series — 截断到最近 5 条

实测 `dev_series.15m` 有 10 条（~1009B），`1h` 有 2 条（~203B）。
scan 只需最近趋势，保留最新 5 条已足够。

**改动**：`filter_tpo_market_profile` 在 reverse 后额外 truncate：

```rust
if let Some(items) = series.as_array_mut() {
    items.reverse();
    items.truncate(5);   // v3.1 新增
}
```

**节省**：~500B

#### avwap.series_by_window — 截断到最近 3 条

实测 15m=10 条、4h=5 条、1d=7 条（合计 ~2390B）。avwap 系列主要提供当前 VWAP 趋势，3 条已足够给 LLM 看方向变化。

**改动**：`filter_avwap` 中将 limit 改为：
```rust
for (window, limit) in [("15m", 3usize), ("4h", 3usize), ("1d", 3usize)]
```
（v3 是 10/5/MAX）

**节省**：~1.4KB

#### funding_rate.by_window.changes — last-10 → last-5

实测 `4h` 和 `1d` 各 10 条 changes（`15m` 为 0 条）。
stage1 方向判断只需最近几次资金费率变动趋势。

**改动**：`filter_funding_rate` 中：
```rust
.map(|changes| take_last_n(changes, 5))  // v3.1: 10 → 5
```

**节省**：~600B

#### cvd_pack 顶层字段说明（不删除，补注释）

`cvd_pack.payload` 顶层的 `delta_fut`/`delta_spot`/`relative_delta_*` 经核查与 `by_window` series 里的值不同（顶层是不同时间粒度的聚合，非冗余），**v3.1 保留**。
建议在 prompt 或数据字典中补充说明："cvd_pack 顶层标量代表当前 15m bar 结束时刻的瞬时快照，by_window.series 代表历史序列"。

---

### 3.10 修正注释方向错误

v3 §七 7.1 `by_timeframe.structure` 的注释：
```json
// 原错误写法（v3 §7.1）：
"support_candidates": [...],      // 聚类后上方前2支撑   ← 错：support 在下方
"resistance_candidates": [...]    // 聚类后下方前2阻力   ← 错：resistance 在上方

// v3.1 正确：
"support_candidates": [...],      // 下方前2支撑候选（price < current_price）
"resistance_candidates": [...]    // 上方前2阻力候选（price > current_price）
```

`level_book` 的输出字段命名沿用 `support`（下方）/ `resistance`（上方），与此定义一致。

---

## 二、v3.1 估算汇总

| 变更 | 方向 | 预估 minified |
|------|------|-------------|
| v3 基线 | — | ~111KB |
| §3.6 footprint 上下分选（≤18 vs ≤20） | −1KB | ~110KB |
| §3.7 level_book 新增 | +1KB | ~111KB |
| §3.8 flow timeframe_summary 新增 | +3.6KB | ~114.6KB |
| §3.9 whale/vpin 1h/3d 删除 | −1.7KB | ~112.9KB |
| §3.9 tpo dev_series 截断 | −0.5KB | ~112.4KB |
| §3.9 avwap series 截断 | −1.4KB | ~111KB |
| §3.9 funding changes 截断 | −0.6KB | ~110.4KB |
| **v3.1 合计** | | **~110KB** |

> `level_book` 和 `timeframe_summary` 增加了 ~4.6KB，但带来时间框架归属和支撑阻力直接可用两个结构性改善，是目标一的关键补丁。体积小幅增加是值得的交换。

---

## 三、第一目标覆盖对照

| 你的需求 | v3.1 覆盖方式 | 状态 |
|---------|-------------|------|
| flow 事件按 15m/4h/1d 分桶 | §3.8 `timeframe_summary`，event_start_ts 分桶 + bias | ✅ |
| 支撑阻力先聚类再给模型 | §3.7 `level_book`，PVS/TPO/RVWAP/FVG/OB 聚类 | ✅ |
| footprint 上下分开选 | §3.6 `split_select_*`，下方支撑/上方阻力分别保证 | ✅ |
| `support/resistance_candidates` 注释方向修正 | §3.10 | ✅ |
| whale/vpin 1h/3d 删除 | §3.9 | ✅ |
| tpo/avwap/funding 再收 → ~102KB | §3.9（-4.2KB）+ §3.7 (+1KB) + §3.8 (+3.6KB） = 净 ~-0.6KB | ✅（约 ~110KB，原 102KB 目标因新增 level_book+summary 调整为 110KB） |

---

## 四、变更文件清单

仅需修改 **1 个文件**：[`systems/llm/src/llm/filter/scan.rs`](systems/llm/src/llm/filter/scan.rs)
新增 **1 个计算步骤**（`build_level_book`，可在同文件内实现）

| 位置 | 变更内容 |
|------|---------|
| `filter_footprint`（继承 v3 签名） | 将 `mixed_select_*` 替换为 `split_select_*`（§3.6） |
| `build_scan_root` 或 `filter_indicators` 后 | 调用 `build_level_book`，注入根对象（§3.7） |
| `filter_event_indicator` | 在 `recent_7d` 过滤后调用 `build_timeframe_summary` 并写入（§3.8） |
| `insert_full_indicator` for whale/vpin/hvp | 改为 `filter_keep_scan_windows` 过滤（§3.9） |
| `filter_tpo_market_profile` | dev_series reverse 后加 `truncate(5)`（§3.9） |
| `filter_avwap` series limit | 15m/4h/1d 均改为 3（§3.9） |
| `filter_funding_rate` changes | `take_last_n(changes, 5)`（§3.9） |
| 新增辅助函数 | `split_select_stacks`、`split_select_clusters`、`build_level_book`、`collect_levels_for_tf`、`cluster_levels`、`build_timeframe_summary`、`filter_keep_scan_windows` |

---

## 五、验证方法（v3.1 新增断言）

```python
import json, glob

path = sorted(glob.glob("systems/llm/temp_model_input/*_scan_*.json"))[-1]
with open(path) as f:
    data = json.load(f)

raw = json.dumps(data, separators=(',', ':'))
print(f"minified: {len(raw) / 1024:.1f}KB")  # 目标 < 110KB

inds = data['indicators']
current_price = (
    inds.get('avwap', {}).get('payload', {}).get('fut_last_price')
    or inds['kline_history']['payload']['intervals']['15m']['markets']['futures'][0].get('c')
)
assert current_price

# §3.6 footprint 上下分开选：下方和上方都应有条目
for win in ['15m', '4h', '1d']:
    fp_win = inds['footprint']['payload']['by_window'][win]
    for field, price_key in [('buy_stacks','start_price'), ('sell_stacks','start_price'),
                              ('buy_imb_clusters','p'), ('sell_imb_clusters','p')]:
        items = fp_win.get(field, [])
        assert len(items) <= 18, f"footprint {win}.{field} has {len(items)} items (max 18)"
        if len(items) >= 4:
            prices = [s.get(price_key, 0) for s in items]
            below = [p for p in prices if p < current_price]
            above = [p for p in prices if p > current_price]
            # 两侧都应有条目（除非全量本身只有一侧）
            # 宽松验证：not all on one side
            assert not (len(below) == 0 or len(above) == 0), (
                f"footprint {win}.{field}: all {len(items)} items on one side of {current_price:.2f}"
                f" (below={len(below)}, above={len(above)})"
            )

# §3.8 flow timeframe_summary（嵌套窗口 + events 数组）
flow_indicators = ['absorption', 'buying_exhaustion', 'selling_exhaustion', 'divergence']
for code in flow_indicators:
    if code not in inds: continue
    ts = inds[code]['payload'].get('timeframe_summary', {})
    assert ts, f"{code} missing timeframe_summary"
    for tf in ['15m', '4h', '1d']:
        assert tf in ts, f"{code}.timeframe_summary missing {tf}"
        tfs = ts[tf]
        assert 'bias' in tfs and tfs['bias'] in ('bullish', 'bearish', 'mixed')
        assert 'bullish_count' in tfs and 'bearish_count' in tfs
        # 嵌套窗口：4h 计数应 >= 15m 计数（因为 4h 包含 15m 范围）
        if tf == '4h':
            tfs_15m = ts.get('15m', {})
            assert tfs.get('bullish_count', 0) >= tfs_15m.get('bullish_count', 0), \
                f"{code}: 4h.bullish_count < 15m.bullish_count (nested window violated)"
            assert tfs.get('bearish_count', 0) >= tfs_15m.get('bearish_count', 0), \
                f"{code}: 4h.bearish_count < 15m.bearish_count (nested window violated)"
        # bullish_events/bearish_events 是数组，每个方向 0-3 条
        for direction in ('bullish_events', 'bearish_events'):
            events = tfs.get(direction, [])
            assert isinstance(events, list), f"{code}.timeframe_summary.{tf}.{direction} is not a list"
            assert len(events) <= 3, f"{code}.timeframe_summary.{tf}.{direction} has {len(events)} items (max 3)"
            for ev in events:
                for k in ('confirm_ts', 'pivot_price', 'score', 'type'):
                    assert k in ev, f"{code}.timeframe_summary.{tf}.{direction} event missing {k}"

# §3.9 whale/vpin 无 1h/3d
for code in ['whale_trades', 'vpin']:
    bw = inds[code]['payload'].get('by_window', {})
    assert '1h' not in bw, f"{code}.by_window still has 1h"
    assert '3d' not in bw, f"{code}.by_window still has 3d"

# §3.9 avwap series ≤ 3 条
avwap_sbw = inds['avwap']['payload'].get('series_by_window', {})
for win in ['15m', '4h', '1d']:
    series = avwap_sbw.get(win, [])
    assert len(series) <= 3, f"avwap.series_by_window.{win} has {len(series)} items (max 3)"

# §3.9 tpo dev_series ≤ 5 条
tpo_ds = inds['tpo_market_profile']['payload'].get('dev_series', {})
for win, items in tpo_ds.items():
    assert len(items) <= 5, f"tpo.dev_series.{win} has {len(items)} items (max 5)"

print("all v3.1 assertions passed")
```
