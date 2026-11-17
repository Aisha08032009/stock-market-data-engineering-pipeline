-- Same business logic as sql/03_dm_prices_daily.sql, but for all days present in ods.
with day as (
    select * from {{ ref('ods_prices') }}
),
api as (
    select a.trade_date,
           count(*) as n,
           round(avg(abs(a.close_price - d.close_price) / nullif(d.close_price, 0) * 100), 4) as diff
    from {{ ref('ods_twelvedata_prices') }} a
    join day d on d.ticker = replace(a.symbol, '.', '-') and d.trade_date = a.trade_date
    group by a.trade_date
)
select d.trade_date,
       count(*)::int                                              as tickers_count,
       coalesce(sum(d.volume), 0)::bigint                         as total_volume,
       coalesce(round(sum(d.close_price * d.volume), 2), 0)::numeric(24,2) as total_traded_value,
       (count(*) filter (where d.close_price > d.open_price))::int as advancers,
       (count(*) filter (where d.close_price < d.open_price))::int as decliners,
       (count(*) filter (where d.close_price = d.open_price))::int as unchanged,
       round(avg((d.high_price - d.low_price) / nullif(d.open_price, 0) * 100), 4)::numeric(10,4) as avg_range_pct,
       coalesce(api.n, 0)::int                                    as api_tickers_checked,
       api.diff::numeric(10,4)                                    as api_avg_abs_close_diff_pct,
       case when api.n is null then 'no_api_data'
            when api.diff < 1  then 'ok'
            else 'check' end                                      as api_check_status
from day d
left join api on api.trade_date = d.trade_date
group by d.trade_date, api.n, api.diff
