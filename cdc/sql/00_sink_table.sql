-- Target table of the CDC sink (current state of customers). Create it BEFORE registering the connectors.
-- The sink is told the full name shermatova_ods.ods_customers, because on the shared database other students
-- have tables with the same short name and the connector must never touch them.
-- Run:  psql -f cdc/sql/00_sink_table.sql
create table if not exists shermatova_ods.ods_customers (
    customer_id integer primary key,
    city        text,
    signup_date date
);
