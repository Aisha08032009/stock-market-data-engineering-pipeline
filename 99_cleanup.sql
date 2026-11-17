-- Cleanup after the demo. First run: python cdc/register.py --delete
select pg_drop_replication_slot(slot_name) from pg_replication_slots
where slot_name = 'debezium_customers_shermatova' and not active;
drop publication if exists dbz_customers_shermatova;
drop table if exists shermatova_ods.ods_customers;
