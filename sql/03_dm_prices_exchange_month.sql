DELETE FROM shermatova_datamart.dm_prices_exchange_month
WHERE month_start = date_trunc('month', %(ds)s::date)::date;

INSERT INTO shermatova_datamart.dm_prices_exchange_month
    (month_start, exchange_code, exchange_name, tickers_count, trading_days,
     total_volume, total_traded_value, avg_close_price)
SELECT date_trunc('month', p.trade_date)::date,
       COALESCE(s.mic_code, 'UNKNOWN'),
       COALESCE(max(e.exchange_name), 'Unknown'),
       count(DISTINCT p.ticker),
       count(DISTINCT p.trade_date),
       COALESCE(sum(p.volume), 0),
       COALESCE(round(sum(p.close_price * p.volume), 2), 0),
       round(avg(p.close_price), 4)
FROM shermatova_ods.ods_prices p
LEFT JOIN shermatova_ods.v_stock_primary s ON s.ticker = p.ticker
LEFT JOIN shermatova_ods.ods_stock_exchanges e ON e.exchange_code = s.mic_code
WHERE p.trade_date >= date_trunc('month', %(ds)s::date)::date
  AND p.trade_date <  (date_trunc('month', %(ds)s::date) + interval '1 month')::date
GROUP BY 1, 2;
