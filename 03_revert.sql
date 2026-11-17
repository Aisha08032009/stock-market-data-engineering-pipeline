-- Puts the source back and shows a DELETE travelling through CDC.
update shermatova.customers set city = 'Bishkek' where customer_id = 1;
delete from shermatova.customers where customer_id = 20001;
select * from shermatova.customers where customer_id in (1, 20001) order by 1;
