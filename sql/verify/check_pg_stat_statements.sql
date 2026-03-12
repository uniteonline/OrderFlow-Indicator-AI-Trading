-- Ensure pg_stat_statements is available in this database.

SELECT extname
FROM pg_extension
WHERE extname = 'pg_stat_statements';

