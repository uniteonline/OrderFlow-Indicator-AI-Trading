-- =============================================================
-- Rebuild database: orderflow_ops
-- Usage:
--   psql -U <user> -d orderflow_ops -f sql/rebuild_orderflow_ops.sql
-- =============================================================

\if :{?expected_db}
\else
\set expected_db orderflow_ops
\endif

\if :{?is_orderflow}
\else
\set is_orderflow 0
\endif

\ir rebuild_common_base.sql
