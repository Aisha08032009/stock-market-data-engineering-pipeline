# Data files

The three input files are **not stored in this repository** because the price file is 190 MB (GitHub allows 100 MB per file).
Put them into this folder before running the pipeline. All three are read by the Airflow DAG `final_shermatova_pipeline`.

| File | Role | How to get it |
|---|---|---|
| `prices_daily.csv` | event file: one row per ticker per trading day (date, ticker, open, high, low, close, volume), about 2.0 M rows, 503 tickers, 2010-01-04 to 2026-11-06 | download the Kaggle dataset "S&P500 All assets - Daily Updated dataset" (file `SnP_daily_update.csv`, licence CC0), save it in this folder and run `python prepare_prices.py` (1 to 2 minutes). The Kaggle file has one column per ticker; the script turns it into one row per ticker per day |
| `12data_stocks.csv` | reference: 211,265 instruments (symbol, name, currency, exchange, mic_code, country, type), separator `;` | Twelve Data account, Market data page, button "Equities CSV" |
| `12data_stock_exchanges.csv` | reference: 90 exchanges (title, name, code, country, timezone), separator `;` | Twelve Data account, Market data page, button "Exchanges CSV" |

`sample/` holds the small exchange file and the first 200 rows of the instruments file, only to show the format.
