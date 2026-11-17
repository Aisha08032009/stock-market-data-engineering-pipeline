-- Returns rows (= test fails) when the dbt mart and the DAG mart disagree on any day they both contain.
with dbt_side as (
    select trade_date, tickers_count, total_volume, total_traded_value, advancers, decliners, unchanged
    from {{ ref('dm_prices_daily_dbt') }}
    where trade_date in (select trade_date from {{ source('datamart', 'dm_prices_daily') }})
),
dag_side as (
    select trade_date, tickers_count, total_volume, total_traded_value, advancers, decliners, unchanged
    from {{ source('datamart', 'dm_prices_daily') }}
)
(select * from dbt_side except select * from dag_side)
union all
(select * from dag_side except select * from dbt_side)
