# Data model

All objects live in the Postgres database `aiacademy`, schemas `shermatova_stg`, `shermatova_ods`, `shermatova_datamart`.
ClickHouse: database `shermatova`.

## Layers

| Layer | Idea | How a re-run behaves |
|-------|------|----------------------|
| **STG** | data exactly as received, every column is text, plus `_ds`, `_source_file`, `_loaded_at` | event tables: delete the day `_ds`, insert again. Reference tables: truncate and reload the whole file |
| **ODS** | real types, clear names, primary key, no duplicates | event tables: delete the day, insert again. Reference tables: truncate and reload |
| **DATAMART** | ready numbers, one query answers a question, no JOIN | the processed day (or month) is deleted and rebuilt |

## Tables and grain

| Table | Grain (one row = ...) | Key |
|-------|----------------------|-----|
| `shermatova_stg.stg_prices_daily` | a line of the price file (only the processed day) | none (text copy) |
| `shermatova_stg.stg_stocks` | a line of `12data_stocks.csv` | none |
| `shermatova_stg.stg_stock_exchanges` | a line of `12data_stock_exchanges.csv` | none |
| `shermatova_stg.stg_twelvedata` | one daily candle returned by the API | none |
| `shermatova_stg.etl_load_log` | one attempt of one task | `id` |
| `shermatova_ods.ods_prices` | one ticker on one trading day | (`trade_date`, `ticker`) |
| `shermatova_ods.ods_stocks` | one instrument on one exchange (rows without symbol are dropped) | (`symbol`, `mic_code`) |
| `shermatova_ods.ods_stock_exchanges` | one exchange | `exchange_code` |
| `shermatova_ods.ods_twelvedata_prices` | one API candle (symbol and day) | (`trade_date`, `symbol`) |
| `shermatova_ods.ods_customers` | one customer, **current state** (filled by CDC, not by Airflow) | `customer_id` |
| `shermatova_datamart.dm_prices_daily` | **one trading day** | `trade_date` |
| `shermatova_datamart.dm_prices_exchange_month` | **one month x one exchange** | (`month_start`, `exchange_code`) |
| `shermatova_datamart.dm_prices_daily_dbt` | one trading day, built by dbt (same numbers as `dm_prices_daily`) | `trade_date` |
| ClickHouse `shermatova.dm_prices_daily` | copy of `dm_prices_daily` | `trade_date` |

`ods_stocks` can hold the same ticker on several exchanges. The view `shermatova_ods.v_stock_primary` picks one listing per
ticker (US exchanges first) so that every price row joins to exactly one exchange. Tickers not found in the reference go to
the exchange `UNKNOWN`, so the totals of the two marts always match.

## CDC: changes of the site table arrive in ODS
```
shermatova.customers  --Postgres log-->  Debezium source  -->  Kafka topic  -->  Debezium JDBC sink  -->  shermatova_ods.ods_customers
(site database)        (publication)      customers-source-shermatova   shermatova.shermatova.customers   sink-shermatova
```
* Source connector `customers-source-shermatova` reads the Postgres log through the publication `dbz_customers_shermatova`
  and the slot `debezium_customers_shermatova` (plugin `pgoutput`). At the start it copies all rows once (snapshot).
* Sink connector `sink-shermatova` writes with `upsert`, the key of the table is the record key, `delete.enabled` removes
  deleted customers. The table name is given in full (`shermatova_ods.ods_customers`) because other students on the same
  database have tables with the same short name.
* Result: an insert, update or delete in `shermatova.customers` shows up in `ods_customers` within about 30 seconds.
* Check: `python cdc/verify_cdc.py` compares row count and a checksum of all values of both tables.
* No secret is stored in the connector files: `cdc/register.py` fills the placeholders from the terminal variables.

## Metric definitions

### dm_prices_daily
| Column | Definition |
|--------|-----------|
| `tickers_count` | number of tickers with a price that day |
| `total_volume` | sum of `volume` (shares) over all tickers |
| `total_traded_value` | sum of `close_price * volume` over all tickers, USD |
| `advancers` / `decliners` / `unchanged` | tickers with close above / below / equal to open |
| `avg_range_pct` | average of `(high - low) / open * 100` |
| `api_tickers_checked` | tickers that were also loaded from the Twelve Data API that day |
| `api_avg_abs_close_diff_pct` | **metric from the API**: average of `abs(api_close - file_close) / file_close * 100` over the checked tickers |

### dm_prices_exchange_month
| Column | Definition |
|--------|-----------|
| `tickers_count` | distinct tickers of that exchange in the month |
| `trading_days` | distinct loaded trading days in the month |
| `total_volume`, `total_traded_value` | same definitions as above, summed over the month and the exchange |
| `avg_close_price` | average close price of all rows of that exchange in the month |

## Questions answered with one query (no JOIN)
```sql
-- how many shares traded on the whole market on a day?
select total_volume from shermatova_datamart.dm_prices_daily where trade_date = '2026-11-06';

-- which exchange traded the most in a month?
select exchange_name, total_volume from shermatova_datamart.dm_prices_exchange_month
where month_start = '2026-11-01' order by total_volume desc limit 1;

-- the two marts agree for a month
select (select sum(total_volume) from shermatova_datamart.dm_prices_daily
        where trade_date >= '2026-11-01' and trade_date < '2026-12-01') as daily_sum,
       (select sum(total_volume) from shermatova_datamart.dm_prices_exchange_month
        where month_start = '2026-11-01') as exchange_sum;
```

## Data quality checks (run inside the DAGs, a failed check turns the task red)
* STG: duplicate key (date, ticker) for the day; rows in STG vs rows in ODS
* ODS: null keys, duplicate keys, invalid values (`close_price <= 0`, `volume < 0`, `high_price < low_price`)
* DATAMART: duplicate key, non-positive metrics, a day that exists in ODS but not in the mart, the two marts disagree on `total_volume`
* dbt tests: `unique`, `not_null`, `accepted_values` for `api_check_status` (`ok`, `check`, `no_api_data`), and a test that
  compares the dbt mart with the DAG mart.

## Orchestration
* `final_shermatova_pipeline` (daily): init -> 4 STG loads -> 4 ODS loads -> quality checks -> trigger of the datamart DAG
  with the same logical date -> wait until it is green.
* `final_shermatova_datamart`: build both marts -> quality checks -> copy `dm_prices_daily` to ClickHouse -> reconcile row
  count and `sum(total_volume)` between Postgres and ClickHouse.
* Every task writes a row into `shermatova_stg.etl_load_log` (source, ds, start, end, status, rows, error text).

## Known limits
* The free API plan allows 8 credits per minute, so the API loads 8 tickers per run (`API_SYMBOLS` in `dag/etl_common.py`).
* On weekends and holidays the price file has no rows: the run finishes green with 0 rows and no mart row.
