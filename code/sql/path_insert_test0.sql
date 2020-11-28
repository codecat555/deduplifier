\set ON_ERROR_STOP on

select upsert_path('/a/b', '/');
select upsert_path('/a/d', '/');
select upsert_path('/a/d/x', '/');
select upsert_path('/a/d/a/x', '/');

\unset ON_ERROR_STOP
