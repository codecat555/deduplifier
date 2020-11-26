
--
-- multipass exec deduplifier-host -- docker exec -it deduplifier_db_1 su - postgres -c 'psql -d deduplifier -h localhost -p 3368 -f /app/code/sql/init.sql'
--

\include_relative ddl.sql
\include_relative code.sql

