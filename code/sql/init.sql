
--
-- multipass exec deduplifier-host -- docker exec -it deduplifier_db_1 su - postgres -c 'psql -d deduplifier -h localhost -p 3368 -f /app/code/sql/init.sql'
--

ALTER SYSTEM SET max_connections TO '600';

\include_relative ddl.sql
\include_relative code.sql

