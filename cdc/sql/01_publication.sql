-- Publication = the list of tables whose changes go into the Postgres log for Debezium.
-- Run:  psql -v ON_ERROR_STOP=1 -f cdc/sql/01_publication.sql
select 'create publication dbz_customers_shermatova for table shermatova.customers '
       || 'with (publish = ''insert, update, delete'')'
where not exists (select 1 from pg_publication where pubname = 'dbz_customers_shermatova')
\gexec

select pubname, pubowner::regrole as owner, pubinsert, pubupdate, pubdelete
from pg_publication where pubname = 'dbz_customers_shermatova';
select * from pg_publication_tables where pubname = 'dbz_customers_shermatova';
