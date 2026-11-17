TRUNCATE shermatova_ods.ods_stock_exchanges;

INSERT INTO shermatova_ods.ods_stock_exchanges
    (exchange_code, exchange_title, exchange_name, country, timezone)
SELECT DISTINCT ON (upper(trim(code)))
       upper(trim(code)), NULLIF(trim(title), ''), NULLIF(trim(name), ''),
       NULLIF(trim(country), ''), NULLIF(trim(timezone), '')
FROM shermatova_stg.stg_stock_exchanges
WHERE NULLIF(trim(code), '') IS NOT NULL
ORDER BY upper(trim(code)), _loaded_at DESC;
