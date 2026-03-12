# Outbox分区与双库落地

## 目标
- `ops.outbox_event` 改为“发送成功即删除”，避免 `sent -> gc` 写放大。
- outbox 按天分区，降低单表膨胀和vacuum压力。
- `market_data_ingestor` 拆分 `md` 与 `ops/outbox` 数据库连接池，支持拆库/拆实例。

## 已完成改动（代码）
- outbox dispatcher:
  - 成功发送后 `DELETE FROM ops.outbox_event`（不再 `mark_sent`）。
  - 保留失败重试和 `dead` 状态。
  - 定时清理 `dead` 行。
  - 定时调用 `ops.ensure_outbox_event_partitions(...)` 预创建分区。
- outbox writer:
  - 插入语句改为 `ON CONFLICT DO NOTHING`（兼容旧表/新分区表）。
- 配置与连接池:
  - `database.md` 与 `database.ops` 覆盖配置已支持。
  - `bootstrap` 里创建双池（若 md/ops 配置完全一致则复用一个池）。
  - `MdDbWriter` 使用 `md_db_pool`；`OpsDbWriter/outbox` 使用 `ops_db_pool`。
- DDL:
  - `sql/orderflow_timescaledb_init.sql` 已改为带 `bucket_date` 的 outbox 分区模型。
  - 新增 `sql/migrate_outbox_partitioned.sql`（存量库迁移脚本）。

## 执行步骤（生产建议）
1. 停止 `market_data_ingestor`。
2. 对 `ops` 数据库做备份。
3. 在 `ops` 数据库执行：
```sql
\i sql/migrate_outbox_partitioned.sql
```
4. 修改 `config/config.yaml` 中 `database.md` 与 `database.ops`：
   - 若拆实例：分别填不同 `host/port/database/user/password_env`。
   - 若暂不拆实例：先保持一致，代码会自动复用池。
5. 启动 `market_data_ingestor`。
6. 验证：
   - `ops.outbox_event` 不再持续出现 `status='sent'` 堆积。
   - 慢日志中 `UPDATE ops.outbox_event SET status='sent' ...` 消失。
   - 分区存在：`ops.outbox_event_pYYYYMMDD` 连续创建。

## 校验SQL
```sql
-- outbox状态分布
SELECT status, count(*) FROM ops.outbox_event GROUP BY status ORDER BY status;

-- 分区列表
SELECT c.relname AS partition_name
FROM pg_inherits i
JOIN pg_class p ON i.inhparent = p.oid
JOIN pg_class c ON i.inhrelid = c.oid
JOIN pg_namespace n ON p.relnamespace = n.oid
WHERE n.nspname = 'ops' AND p.relname = 'outbox_event'
ORDER BY c.relname;
```

## 注意
- 若 `md` 与 `ops` 拆到不同数据库，两边都需要有 `cfg` 枚举类型（当前写入SQL依赖 `cfg.market_type/cfg.source_type`）。
- 本次未删除旧 `outbox_event_legacy`，用于回滚观察；确认稳定后可人工归档/删除。
