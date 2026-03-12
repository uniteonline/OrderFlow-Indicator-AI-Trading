-- Top SQL by total execution time.
SELECT
    calls,
    round(total_exec_time::numeric, 2) AS total_ms,
    round(mean_exec_time::numeric, 3) AS mean_ms,
    rows,
    left(regexp_replace(query, E'\\s+', ' ', 'g'), 240) AS query_sample
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 20;

-- Top SQL by frequency.
SELECT
    calls,
    round(total_exec_time::numeric, 2) AS total_ms,
    round(mean_exec_time::numeric, 3) AS mean_ms,
    rows,
    left(regexp_replace(query, E'\\s+', ' ', 'g'), 240) AS query_sample
FROM pg_stat_statements
ORDER BY calls DESC
LIMIT 20;

