select * from {{ ref('dm_prices_daily_dbt') }}
where tickers_count <= 0 or total_volume < 0 or total_traded_value < 0
