# Backfill 启动优化 — StateStore 文件快照方案

## 背景与问题

每次重启 indicator-engine，即使宕机只有 5 分钟，也必须从 DB 重建 9 天（10,080 分钟）的 `StateStore` 内存历史：
- `StateStore.history_futures/history_spot: VecDeque<MinuteHistory>` 是纯内存结构，无持久化
- 主要瓶颈：读 `md.agg_orderbook_1m`（平均 32KB/行 JSONB × 10,080 行 ≈ 320MB 传输）+ 运行 24 指标 × 10,080 分钟

## 方案：退出前保存文件快照，启动时直接加载

```
现在：DB 读 10,080 分钟 → 跑 24 指标 × 10,080 次 → 10-60 分钟
快照：读 ~40MB gz 文件（<5s）→ DB 只读间隔 → 跑 24 指标 × <60 次 → <30 秒
```

## 结构体需加 serde derives（共 11 个）

| 结构体 | 文件 | 说明 |
|--------|------|------|
| `LevelAgg` | state_store.rs:28 | MinuteHistory.profile 的值类型 |
| `LiqAgg` | state_store.rs:44 | MinuteHistory.force_liq 的值类型 |
| `BookLevelAgg` | state_store.rs:56 | MinuteWindowData.heatmap 的值类型 |
| `WhaleStats` | state_store.rs:81 | 鲸鱼统计 |
| `MarketKind` | ingest/decoder.rs:6 | Spot/Futures 枚举 |
| `MinuteHistory` | state_store.rs:178 | 核心历史 bar（37 字段） |
| `VpinState` | state_store.rs:824 | VPIN 滚动状态 |
| `FinalizedVpinState` | state_store.rs | VPIN 历史快照 |
| `FundingChange` | state_store.rs:242 | 资金费率变化记录 |
| `LatestMarkState` | state_store.rs:225 | Mark 价格时间线 |
| `LatestFundingState` | state_store.rs:234 | 资金费率时间线 |

## StateSnapshot 结构体（新增到 state_store.rs）

```rust
pub const STATE_SNAPSHOT_VERSION: u32 = 1;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct StateSnapshot {
    pub version: u32,
    pub symbol: String,
    pub last_finalized_ts: DateTime<Utc>,
    pub saved_at: DateTime<Utc>,
    pub cvd_futures: f64,   // 仅用于信息记录，restore 时从 history tail 派生
    pub cvd_spot: f64,
    pub vpin_futures: VpinState,
    pub vpin_spot: VpinState,
    pub finalized_vpin_futures: Vec<FinalizedVpinState>,
    pub finalized_vpin_spot: Vec<FinalizedVpinState>,
    pub history_futures: Vec<MinuteHistory>,   // VecDeque → Vec for serde
    pub history_spot: Vec<MinuteHistory>,
    pub latest_mark: Option<LatestMarkState>,
    pub latest_funding: Option<LatestFundingState>,
    pub funding_changes: Vec<FundingChange>,
    pub mark_timeline: Vec<LatestMarkState>,
    pub funding_timeline: Vec<LatestFundingState>,
}
```

**不包含**（可从 gap replay 重建）：`buckets`、`canonical_minutes`、`orderbooks`、`depth_conflation`

## StateStore 新增两个方法

```rust
pub fn extract_snapshot(&self) -> StateSnapshot { /* VecDeque → Vec */ }
pub fn restore_from_snapshot(&mut self, snap: StateSnapshot) { /* Vec → VecDeque */ }
```

**restore 关键实现**：CVD/last_finalized_minute 从 history tail 派生，防止累积值不一致：

```rust
pub fn restore_from_snapshot(&mut self, snap: StateSnapshot) {
    self.vpin_futures = snap.vpin_futures;
    self.vpin_spot = snap.vpin_spot;
    self.finalized_vpin_futures = snap.finalized_vpin_futures.into_iter().collect();
    self.finalized_vpin_spot = snap.finalized_vpin_spot.into_iter().collect();
    self.history_futures = snap.history_futures.into_iter().collect();
    self.history_spot = snap.history_spot.into_iter().collect();
    self.latest_mark = snap.latest_mark;
    self.latest_funding = snap.latest_funding;
    self.funding_changes = snap.funding_changes.into_iter().collect();
    self.mark_timeline = snap.mark_timeline.into_iter().collect();
    self.funding_timeline = snap.funding_timeline.into_iter().collect();
    // ⚠️ CVD 必须从 history 末尾派生（而非存储值），确保准确
    self.cvd_futures = self.history_futures.back().map(|h| h.cvd).unwrap_or(0.0);
    self.cvd_spot = self.history_spot.back().map(|h| h.cvd).unwrap_or(0.0);
    // last_finalized_minute 也从 history tail 派生
    self.last_finalized_minute = self.history_futures.back().map(|h| h.ts_bucket);
}
```

## runtime.rs 改动

### startup（run_startup_backfill 开头）

```rust
if let Some(snap) = try_load_state_snapshot(path, &symbol, max_age_hours) {
    let snap_ts = snap.last_finalized_ts;
    state_store.restore_from_snapshot(snap);
    // ⚠️ 关键：从 snap_ts + 1 min 开始，不设重叠 overlap！
    // 原因：快照的 CVD/VPIN 已包含 snap_ts 之前所有分钟的累计量。
    // 若使用 snap_ts - 180min 的 overlap，这 180 分钟的 delta/volume
    // 会被 finalize_minute() 再次加到 cvd/vpin_state，造成双重计入。
    from_ts = snap_ts + Duration::minutes(1);
    info!(snap_ts = %snap_ts, from_ts = %from_ts, "State snapshot loaded, gap-only backfill");
}
```

### shutdown（ctrl_c/SIGTERM 后，任务 abort 前）

```rust
let snap = state_store.extract_snapshot();
save_state_snapshot(&snap, path).await; // atomic: .tmp → rename
```

### SIGTERM 处理（关键修复：信号注册必须在 backfill 之前）

**错误根因**（已修复）：原代码在 `run_startup_backfill().await` 完成后才注册 SIGTERM handler。
backfill 耗时 10-60 分钟，期间 systemd 发送 SIGTERM 时无 handler → 进程不退出 → 120s 后 SIGKILL。

**修复方案**：在 backfill 之前注册信号，用 `tokio::select!` 同时等待 backfill 和信号：

```rust
// ⚠️ 关键：在 run_startup_backfill 之前注册，而非之后
use tokio::signal::unix::{signal, SignalKind};
let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler setup failed");

let mut got_signal_before_live = false;
let startup_replay_cutoff_bucket = tokio::select! {
    biased;
    _ = tokio::signal::ctrl_c() => { got_signal_before_live = true; None }
    _ = sigterm.recv() => { got_signal_before_live = true; None }
    result = run_startup_backfill(...) => { /* 正常流程 */ }
};

if got_signal_before_live {
    // 只有 history >= 1440 bars 时才保存快照（防止早期 backfill 的残缺快照）
    if history_len >= 1440 {
        let snap = state_store.extract_snapshot();
        save_state_snapshot(&snap, &snapshot_path).await;
    }
    return Ok(());
}

// 主 loop 里同样使用已注册的 sigterm：
loop {
    tokio::select! {
        _ = tokio::signal::ctrl_c() => { break; }
        _ = sigterm.recv() => { break; }
        // ... 其他 arms
    }
}
```

**try_load_state_snapshot 验证**：
- `snap.history_futures.len() >= 1440`（小于 1 天历史的快照被丢弃）
- 其他校验：version、symbol、max_age

## 新增辅助函数

```rust
fn try_load_state_snapshot(path: &str, symbol: &str, max_age_hours: u64) -> Option<StateSnapshot>
// 校验顺序：文件存在 → JSON 解析成功 → version == STATE_SNAPSHOT_VERSION
//          → symbol 匹配 → saved_at 未超期
// 任意失败 → 返回 None（触发全量 backfill）

async fn save_state_snapshot(snap: &StateSnapshot, path: &str) -> anyhow::Result<()>
// 写法：先写 {path}.tmp，成功后 rename 到 path（原子写，防崩溃损坏快照）
// 使用 serde_json + GzEncoder(Compression::fast())
```

## 依赖变更

```toml
# systems/indicator_engine/Cargo.toml
flate2 = "1.0"   # gzip 压缩（纯 Rust，无 unsafe）
# serde + serde_json 已存在，无需新增
```

## config.yaml 新增（3 个配置）

```yaml
indicator:
  # 快照文件路径（空字符串 = 完全禁用快照功能）
  snapshot_file_path: "/tmp/indicator_engine_{symbol}.json.gz"
  # 快照最大有效时长（超期后回退全量 backfill）
  snapshot_max_age_hours: 24
  # 验证模式：同时跑全量内存重算，与快照路径结果做逐字段对比（仅用于验证，生产禁用）
  snapshot_verify_full_recompute: false
```

`bootstrap.rs IndicatorConfig` 新增 3 个字段（均带 `#[serde(default)]`）：
```rust
pub snapshot_file_path: String,             // default: ""
pub snapshot_max_age_hours: u64,            // default: 24
pub snapshot_verify_full_recompute: bool,   // default: false
```

---

## 验证模式设计（snapshot_verify_full_recompute = true）

**核心思路**：快照路径 + 全量重算路径各跑一遍，对比 gap 期间每分钟的 `MinuteHistory` 关键字段是否完全一致。

```
启动时 (verify mode)

  Path A（快照路径，写 DB）
    load snapshot → restore StateStore A
    gap replay (snap_ts+1 to now) → StateStore A 最终态

  Path B（全量重算路径，仅内存，不写 DB）
    新建 StateStore B（空）
    full backfill (snap_ts-9d to now) → StateStore B

  对比：
    对 gap 期间每分钟 t：
      A.history_futures[t] vs B.history_futures[t]
    逐字段比较（CVD、VPIN、delta、OBI 等），允许浮点误差 1e-9
    有差异 → warn + 记录差异详情
    完全一致 → info "Snapshot verification passed"
```

**比较的关键字段**：

| 字段 | 说明 |
|------|------|
| `cvd` | 累计 CVD，最容易出现双重计入 |
| `vpin` | VPIN 值 |
| `delta` | 每分钟 delta |
| `relative_delta` | 相对 delta |
| `buy_qty / sell_qty` | 交易量 |
| `obi_k_twa / obi_twa` | 订单簿不平衡 |
| `whale_notional_buy / sell` | 鲸鱼成交量 |

---

## 数据准确性保障

| 风险 | 如何解决 |
|------|---------|
| CVD 双重计入 | gap replay 从 `snap_ts + 1min` 开始，无重叠；CVD 从 history 末尾派生 |
| VPIN 双重计入 | 同上，gap replay 无重叠，VpinState 从快照正确状态继续累积 |
| 快照文件损坏/不完整 | `.tmp` → `rename` 原子写，解析失败自动 fallback 全量 backfill |
| 版本不兼容 | `version` 字段不匹配时 fallback，旧快照文件自动作废 |
| symbol 不匹配 | 明确校验 `snap.symbol == symbol`，防止换标的误用 |
| 快照过期（长时间宕机） | `max_age_hours` 配置，超期 fallback 全量 backfill |
| 崩溃未保存快照 | 文件不存在 → fallback 全量 backfill |
| 快照内修正缺失 | 快照保存时已是内存最终状态；快照后的新分钟走正常 gap replay |

---

## 改动文件清单

| 文件 | 改动 |
|------|------|
| `systems/indicator_engine/src/runtime/state_store.rs` | 给 11 个结构体加 serde derives，新增 StateSnapshot + extract/restore 方法 |
| `systems/indicator_engine/src/ingest/decoder.rs` | MarketKind 加 Serialize/Deserialize |
| `systems/indicator_engine/src/app/runtime.rs` | 新增 try_load/save_snapshot 函数，startup 加载逻辑，shutdown 保存逻辑，SIGTERM handler |
| `systems/indicator_engine/src/app/bootstrap.rs` | IndicatorConfig 新增 3 个 snapshot 配置字段 |
| `systems/indicator_engine/Cargo.toml` | 新增 flate2 = "1.0" |
| `config/config.yaml` | 新增 snapshot 配置项 |

## 验证命令

```bash
# 编译
cargo build -p indicator-engine

# 运行 → Ctrl+C → 观察 "State snapshot saved" 日志
# 重启 → 观察 "State snapshot loaded, gap-only backfill"
# 对比重启时间：之前 10-60min vs 之后 <30s

# 查看文件大小
ls -lh /tmp/indicator_engine_*.json.gz  # 预期 ~30-60MB

# 验证 CVD 无跳变（重启前后对比）
psql -h 127.0.0.1 -U postgres -d orderflow -c "
  SELECT ts_snapshot, payload_json->'cvd' AS cvd
  FROM feat.indicator_snapshot
  WHERE indicator_code = 'cvd_pack' AND symbol = 'BTCUSDT'
  ORDER BY ts_snapshot DESC LIMIT 10;"
# 快照后第一分钟的 CVD 应 = 快照末尾 CVD + 该分钟 delta，无跳变
```
