-- Idempotent DDL: safe to run on every DAG run.
CREATE SCHEMA IF NOT EXISTS shermatova_stg;
CREATE SCHEMA IF NOT EXISTS shermatova_ods;
CREATE SCHEMA IF NOT EXISTS shermatova_datamart;

-- ---------- journal ----------
CREATE TABLE IF NOT EXISTS shermatova_stg.etl_load_log (
    id            BIGSERIAL PRIMARY KEY,
    dag_id        TEXT,
    run_id        TEXT,
    task_id       TEXT,
    try_number    INTEGER,
    source        TEXT,
    ds            DATE,
    started_at    TIMESTAMP,
    finished_at   TIMESTAMP,
    status        TEXT,
    rows_loaded   INTEGER,
    error_message TEXT
);

-- ---------- STG: everything as text + service columns ----------
CREATE TABLE IF NOT EXISTS shermatova_stg.stg_prices_daily (
    date TEXT, ticker TEXT, open TEXT, high TEXT, low TEXT, close TEXT, volume TEXT,
    _ds DATE, _source_file TEXT, _loaded_at TIMESTAMP DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_stg_prices_ds ON shermatova_stg.stg_prices_daily (_ds);

CREATE TABLE IF NOT EXISTS shermatova_stg.stg_stocks (
    symbol TEXT, name TEXT, currency TEXT, exchange TEXT, mic_code TEXT, country TEXT, type TEXT,
    _ds DATE, _source_file TEXT, _loaded_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS shermatova_stg.stg_stock_exchanges (
    title TEXT, name TEXT, code TEXT, country TEXT, timezone TEXT,
    _ds DATE, _source_file TEXT, _loaded_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS shermatova_stg.stg_twelvedata (
    symbol TEXT, datetime TEXT, open TEXT, high TEXT, low TEXT, close TEXT, volume TEXT,
    exchange TEXT, currency TEXT,
    _ds DATE, _source_file TEXT, _loaded_at TIMESTAMP DEFAULT now()
);

-- ---------- ODS: typed, keyed, no duplicates ----------
CREATE TABLE IF NOT EXISTS shermatova_ods.ods_prices (
    trade_date  DATE          NOT NULL,
    ticker      TEXT          NOT NULL,
    open_price  NUMERIC(18,6),
    high_price  NUMERIC(18,6),
    low_price   NUMERIC(18,6),
    close_price NUMERIC(18,6),
    volume      BIGINT,
    loaded_at   TIMESTAMP DEFAULT now(),
    PRIMARY KEY (trade_date, ticker)
);

CREATE TABLE IF NOT EXISTS shermatova_ods.ods_stocks (
    symbol        TEXT NOT NULL,
    mic_code      TEXT NOT NULL,
    company_name  TEXT,
    currency      TEXT,
    exchange_name TEXT,
    country       TEXT,
    security_type TEXT,
    loaded_at     TIMESTAMP DEFAULT now(),
    PRIMARY KEY (symbol, mic_code)
);

CREATE TABLE IF NOT EXISTS shermatova_ods.ods_stock_exchanges (
    exchange_code  TEXT PRIMARY KEY,
    exchange_title TEXT,
    exchange_name  TEXT,
    country        TEXT,
    timezone       TEXT,
    loaded_at      TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS shermatova_ods.ods_twelvedata_prices (
    trade_date  DATE NOT NULL,
    symbol      TEXT NOT NULL,
    open_price  NUMERIC(18,6),
    high_price  NUMERIC(18,6),
    low_price   NUMERIC(18,6),
    close_price NUMERIC(18,6),
    volume      BIGINT,
    currency    TEXT,
    loaded_at   TIMESTAMP DEFAULT now(),
    PRIMARY KEY (trade_date, symbol)
);

-- one listing per ticker: US exchanges first
-- (the price file writes BRK-B, the reference writes BRK.B, so '.' is replaced by '-')
CREATE OR REPLACE VIEW shermatova_ods.v_stock_primary AS
SELECT DISTINCT ON (replace(symbol, '.', '-'))
       replace(symbol, '.', '-') AS ticker, mic_code, company_name, country
FROM shermatova_ods.ods_stocks
ORDER BY replace(symbol, '.', '-'),
         (country = 'United States') DESC NULLS LAST,
         CASE mic_code WHEN 'XNYS' THEN 1 WHEN 'XNGS' THEN 2 WHEN 'XNMS' THEN 3
                       WHEN 'XNCM' THEN 4 WHEN 'XASE' THEN 5 ELSE 9 END,
         (security_type = 'Common Stock') DESC NULLS LAST,
         mic_code;

-- ---------- DATAMART ----------
CREATE TABLE IF NOT EXISTS shermatova_datamart.dm_prices_daily (
    trade_date                 DATE PRIMARY KEY,
    tickers_count              INTEGER       NOT NULL,
    total_volume               BIGINT        NOT NULL,
    total_traded_value         NUMERIC(24,2) NOT NULL,
    advancers                  INTEGER       NOT NULL,
    decliners                  INTEGER       NOT NULL,
    unchanged                  INTEGER       NOT NULL,
    avg_range_pct              NUMERIC(10,4),
    api_tickers_checked        INTEGER       NOT NULL DEFAULT 0,
    api_avg_abs_close_diff_pct NUMERIC(10,4),
    loaded_at                  TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS shermatova_datamart.dm_prices_exchange_month (
    month_start        DATE NOT NULL,
    exchange_code      TEXT NOT NULL,
    exchange_name      TEXT,
    tickers_count      INTEGER       NOT NULL,
    trading_days       INTEGER       NOT NULL,
    total_volume       BIGINT        NOT NULL,
    total_traded_value NUMERIC(24,2) NOT NULL,
    avg_close_price    NUMERIC(18,4),
    loaded_at          TIMESTAMP DEFAULT now(),
    PRIMARY KEY (month_start, exchange_code)
);
