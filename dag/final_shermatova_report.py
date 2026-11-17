"""Helper DAG (manual): prints what is in every layer, the last journal rows and the PG vs ClickHouse sums.
Read-only: it changes nothing. Open the log of the task 'report'."""
from __future__ import annotations

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from _lib.tenant import student_dag
from airflow.decorators import task

from etl_common import DM, ODS, STG, ch_client, log, pg_hook


def show(title, sql):
    rows = pg_hook().get_records(sql)
    log.info("=== %s (%d rows)", title, len(rows))
    for r in rows:
        log.info("    %s", " | ".join(str(x) for x in r))


with student_dag(
    __file__,
    dag_id="final_shermatova_report",
    default_args={"owner": "shermatova"},
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["final", "shermatova"],
) as dag:

    @task
    def report():
        show("etl_load_log, last 20 task runs", f"""
            SELECT id, task_id, ds, status, rows_loaded, left(coalesce(error_message, ''), 70)
            FROM {STG}.etl_load_log ORDER BY id DESC LIMIT 20""")
        show("stg_prices_daily rows per _ds", f"""
            SELECT _ds, count(*) FROM {STG}.stg_prices_daily GROUP BY 1 ORDER BY 1 DESC LIMIT 10""")
        show("stg_twelvedata rows per _ds", f"""
            SELECT _ds, count(*) FROM {STG}.stg_twelvedata GROUP BY 1 ORDER BY 1 DESC LIMIT 10""")
        show("ods_prices rows per trade_date", f"""
            SELECT trade_date, count(*) FROM {ODS}.ods_prices GROUP BY 1 ORDER BY 1 DESC LIMIT 10""")
        show("ods_twelvedata_prices rows per trade_date", f"""
            SELECT trade_date, count(*) FROM {ODS}.ods_twelvedata_prices GROUP BY 1 ORDER BY 1 DESC LIMIT 10""")
        show("reference tables", f"""
            SELECT 'ods_stocks', count(*) FROM {ODS}.ods_stocks
            UNION ALL SELECT 'ods_stock_exchanges', count(*) FROM {ODS}.ods_stock_exchanges""")
        show("dm_prices_daily", f"""
            SELECT trade_date, tickers_count, total_volume, advancers, decliners, api_tickers_checked,
                   api_avg_abs_close_diff_pct FROM {DM}.dm_prices_daily ORDER BY 1 DESC LIMIT 10""")
        show("dm_prices_exchange_month (latest month)", f"""
            SELECT month_start, exchange_code, exchange_name, tickers_count, trading_days, total_volume
            FROM {DM}.dm_prices_exchange_month
            WHERE month_start = (SELECT max(month_start) FROM {DM}.dm_prices_exchange_month)
            ORDER BY total_volume DESC""")
        show("two marts agree? (per month: daily sum vs exchange sum)", f"""
            SELECT d.m, d.v AS daily_sum, e.v AS exchange_sum, d.v = e.v AS equal FROM
              (SELECT date_trunc('month', trade_date)::date m, sum(total_volume) v
               FROM {DM}.dm_prices_daily GROUP BY 1) d
            JOIN (SELECT month_start m, sum(total_volume) v
                  FROM {DM}.dm_prices_exchange_month GROUP BY 1) e USING (m) ORDER BY 1 DESC LIMIT 6""")
        pg_n, pg_sum = pg_hook().get_first(
            f"SELECT count(*), COALESCE(sum(total_volume), 0) FROM {DM}.dm_prices_daily")
        try:
            ch_n, ch_sum = ch_client().query(
                "SELECT count(), sum(total_volume) FROM dm_prices_daily").result_rows[0]
        except Exception as exc:
            ch_n, ch_sum = "ERROR", f"{type(exc).__name__}: {exc}"[:120]
        log.info("=== PG vs ClickHouse dm_prices_daily: postgres rows=%s volume=%s | clickhouse rows=%s volume=%s",
                 pg_n, pg_sum, ch_n, ch_sum)

    report()
