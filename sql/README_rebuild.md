# Database Rebuild

`rebuild_common_base.sql` is the shared baseline schema for both databases.

- `rebuild_orderflow.sql`: `orderflow` entrypoint
- `rebuild_orderflow_ops.sql`: `orderflow_ops` entrypoint
- `rebuild_all_databases.sh`: create databases on a new server and apply both entrypoints

## New server

```bash
cd /data
PGPASSWORD=your_password bash sql/rebuild_all_databases.sh
```

Optional environment variables:

- `PGHOST`, `PGPORT`, `PGUSER`, `POSTGRES_DB`
- `ORDERFLOW_DB`, `ORDERFLOW_OPS_DB`
- `RECREATE_DATABASES=1` to drop and recreate target databases
- `OUTBOX_PARTITION_DAYS_AHEAD`, `OUTBOX_PARTITION_DAYS_BACK`

## Single database

```bash
PGPASSWORD=your_password psql -h 127.0.0.1 -U postgres -d orderflow -f sql/rebuild_orderflow.sql
PGPASSWORD=your_password psql -h 127.0.0.1 -U postgres -d orderflow_ops -f sql/rebuild_orderflow_ops.sql
```

If you need a non-default database name, override `expected_db`:

```bash
PGPASSWORD=your_password psql -h 127.0.0.1 -U postgres \
  -d orderflow_rebuild_check \
  -v expected_db=orderflow_rebuild_check \
  -f sql/rebuild_orderflow.sql
```
