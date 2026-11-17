DELETE FROM shermatova_datamart.dm_prices_daily WHERE trade_date = %(ds)s::date;

INSERT INTO shermatova_datamart.dm_prices_daily
    (trade_date, tickers_count, total_volume, total_traded_value, advancers, decliners,
     unchanged, avg_range_pct, api_tickers_checked, api_avg_abs_close_diff_pct)
WITH day AS (
    SELECT * FROM shermatova_ods.ods_prices WHERE trade_date = %(ds)s::date
),
api AS (
    SELECT count(*) AS n,
           round(avg(abs(a.close_price - d.close_price) / NULLIF(d.close_price, 0) * 100), 4) AS diff
    FROM shermatova_ods.ods_twelvedata_prices a
    JOIN day d ON d.ticker = replace(a.symbol, '.', '-') AND d.trade_date = a.trade_date
)
SELECT %(ds)s::date,
       count(*),
       COALESCE(sum(day.volume), 0),
       COALESCE(round(sum(day.close_price * day.volume), 2), 0),
       count(*) FILTER (WHERE day.close_price > day.open_price),
       count(*) FILTER (WHERE day.close_price < day.open_price),
       count(*) FILTER (WHERE day.close_price = day.open_price),
       round(avg((day.high_price - day.low_price) / NULLIF(day.open_price, 0) * 100), 4),
       api.n,
       api.diff
FROM day CROSS JOIN api
GROUP BY api.n, api.diff
HAVING count(*) > 0;
