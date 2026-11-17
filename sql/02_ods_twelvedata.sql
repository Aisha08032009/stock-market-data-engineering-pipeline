DELETE FROM shermatova_ods.ods_twelvedata_prices WHERE trade_date = %(ds)s::date;

INSERT INTO shermatova_ods.ods_twelvedata_prices
    (trade_date, symbol, open_price, high_price, low_price, close_price, volume, currency)
SELECT trade_date::date, symbol, open::numeric, high::numeric, low::numeric, close::numeric,
       round(NULLIF(volume, '')::numeric)::bigint, currency
FROM (
    SELECT left(trim(datetime), 10) AS trade_date, upper(trim(symbol)) AS symbol,
           open, high, low, close, volume, NULLIF(trim(currency), '') AS currency,
           row_number() OVER (PARTITION BY left(trim(datetime), 10), upper(trim(symbol))
                              ORDER BY _loaded_at DESC) AS rn
    FROM shermatova_stg.stg_twelvedata
    WHERE _ds = %(ds)s::date
      AND NULLIF(trim(symbol), '') IS NOT NULL
      AND NULLIF(trim(close), '') IS NOT NULL
) t
WHERE rn = 1;
