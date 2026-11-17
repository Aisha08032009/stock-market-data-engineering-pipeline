# Dataset passport

**Student:** Aisha Shermatova (login `shermatova`)
**Topic:** Daily US stock prices (OHLCV) with a live API check, built as a small data warehouse.
**Approval:** approved by: Nurbek (mentor), 2026-11-09 (confirmed in Telegram: "approved")

## Sources

| # | Source | Role | Content | Volume |
|---|--------|------|---------|--------|
| 1 | `prices_daily.csv` | **event file** (has a date) | one row per ticker per trading day: date, ticker, open, high, low, close, volume | ~2.0 M rows, 503 tickers, 2010-01-04 to 2026-11-06 |
| 2 | `12data_stocks.csv` | **reference** | all listed instruments: symbol, name, currency, exchange, mic_code, country, type | 211,265 rows |
| 3 | `12data_stock_exchanges.csv` | **reference** | stock exchanges: title, name, code (MIC), country, timezone | 90 rows |
| 4 | Twelve Data API `GET /time_series` | **external API** | daily candle of 8 tickers for the processed date (Basic plan: 800 credits/day, 8 per minute) | 8 rows per run |
| 5 | table `shermatova.customers` | **"site database"** for CDC | customers of the site: customer_id, city, signup_date | 20,000 rows |

## Where the files come from
* `prices_daily.csv` is built once by `data/prepare_prices.py` from the public Kaggle dataset
  "S&P500 All assets - Daily Updated dataset" (file `SnP_daily_update.csv`, licence CC0). The Kaggle file has one column per
  ticker per field; the script turns it into one row per ticker per day.
* The two reference files are downloaded from the Twelve Data account (Market data page: "Equities CSV" and "Exchanges CSV").
* The API needs a key. The key is stored only in the Airflow Connection `twelvedata_shermatova`.

## Join keys
* prices.ticker = stocks.symbol (the price file writes `BRK-B`, the reference writes `BRK.B`, so `.` is replaced by `-`)
* stocks.mic_code = exchanges.code
* API symbol = prices.ticker (used to compare the close price from the file with the close from the API)
