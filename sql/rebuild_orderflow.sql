-- =============================================================
-- Rebuild database: orderflow
-- Usage:
--   psql -U <user> -d orderflow -f sql/rebuild_orderflow.sql
-- =============================================================

\if :{?expected_db}
\else
\set expected_db orderflow
\endif

\if :{?is_orderflow}
\else
\set is_orderflow 1
\endif

\ir rebuild_common_base.sql
