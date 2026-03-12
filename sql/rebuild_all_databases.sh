#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
POSTGRES_DB="${POSTGRES_DB:-postgres}"

ORDERFLOW_DB="${ORDERFLOW_DB:-orderflow}"
ORDERFLOW_OPS_DB="${ORDERFLOW_OPS_DB:-orderflow_ops}"

OUTBOX_PARTITION_DAYS_AHEAD="${OUTBOX_PARTITION_DAYS_AHEAD:-7}"
OUTBOX_PARTITION_DAYS_BACK="${OUTBOX_PARTITION_DAYS_BACK:-2}"
RECREATE_DATABASES="${RECREATE_DATABASES:-0}"

psql_base=(
  psql
  -h "$PGHOST"
  -p "$PGPORT"
  -U "$PGUSER"
  -v ON_ERROR_STOP=1
)

createdb_base=(
  createdb
  -h "$PGHOST"
  -p "$PGPORT"
  -U "$PGUSER"
)

dropdb_base=(
  dropdb
  -h "$PGHOST"
  -p "$PGPORT"
  -U "$PGUSER"
  --if-exists
)

db_exists() {
  local db_name="$1"
  "${psql_base[@]}" -d "$POSTGRES_DB" -Atqc \
    "SELECT 1 FROM pg_database WHERE datname = '$db_name'" | grep -qx '1'
}

ensure_database() {
  local db_name="$1"

  if [[ "$RECREATE_DATABASES" == "1" ]]; then
    "${dropdb_base[@]}" "$db_name"
  fi

  if ! db_exists "$db_name"; then
    "${createdb_base[@]}" "$db_name"
  fi
}

apply_schema() {
  local db_name="$1"
  local entry_file="$2"
  local is_orderflow="$3"

  "${psql_base[@]}" \
    -d "$db_name" \
    -v expected_db="$db_name" \
    -v is_orderflow="$is_orderflow" \
    -v outbox_partition_days_ahead="$OUTBOX_PARTITION_DAYS_AHEAD" \
    -v outbox_partition_days_back="$OUTBOX_PARTITION_DAYS_BACK" \
    -f "$entry_file"
}

ensure_database "$ORDERFLOW_DB"
ensure_database "$ORDERFLOW_OPS_DB"

apply_schema "$ORDERFLOW_DB" "$SCRIPT_DIR/rebuild_orderflow.sql" 1
apply_schema "$ORDERFLOW_OPS_DB" "$SCRIPT_DIR/rebuild_orderflow_ops.sql" 0

echo "Rebuild complete:"
echo "  - $ORDERFLOW_DB"
echo "  - $ORDERFLOW_OPS_DB"
