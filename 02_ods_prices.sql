DELETE FROM shermatova_ods.ods_prices WHERE trade_date = %(ds)s::date;

INSERT INTO shermatova_ods.ods_prices
    (trade_date, ticker, open_price, high_price, low_price, close_price, volume)
SELECT date::date, ticker, open::numeric, high::numeric, low::numeric, close::numeric,
       round(volume::numeric)::bigint
FROM (
    SELECT upper(trim(date)) AS date, upper(trim(ticker)) AS ticker,
           open, high, low, close, volume,
           row_number() OVER (PARTITION BY upper(trim(date)), upper(trim(ticker))
                              ORDER BY _loaded_at DESC) AS rn
    FROM shermatova_stg.stg_prices_daily
    WHERE _ds = %(ds)s::date
      AND NULLIF(trim(ticker), '') IS NOT NULL
      AND NULLIF(trim(date), '') IS NOT NULL
      AND NULLIF(trim(close), '') IS NOT NULL
) t
WHERE rn = 1;
