TRUNCATE shermatova_ods.ods_stocks;

INSERT INTO shermatova_ods.ods_stocks
    (symbol, mic_code, company_name, currency, exchange_name, country, security_type)
SELECT DISTINCT ON (upper(trim(symbol)), upper(trim(mic_code)))
       upper(trim(symbol)), upper(trim(mic_code)),
       NULLIF(trim(name), ''), NULLIF(trim(currency), ''), NULLIF(trim(exchange), ''),
       NULLIF(trim(country), ''), NULLIF(trim(type), '')
FROM shermatova_stg.stg_stocks
WHERE NULLIF(trim(symbol), '') IS NOT NULL
  AND NULLIF(trim(mic_code), '') IS NOT NULL
ORDER BY upper(trim(symbol)), upper(trim(mic_code)), _loaded_at DESC;
