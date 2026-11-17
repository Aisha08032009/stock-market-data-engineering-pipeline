-- Three changes in the source: an update, an insert, and (in 03_revert.sql) a delete.
-- After ~30 seconds they must be visible in shermatova_ods.ods_customers.
update shermatova.customers set city = 'CDC test city' where customer_id = 1;
insert into shermatova.customers (customer_id, city, signup_date) values (20001, 'Osh', current_date)
on conflict (customer_id) do update set city = excluded.city;
select * from shermatova.customers where customer_id in (1, 20001) order by 1;
