"""DAG 2: ods -> 2 marts -> checks -> copy of dm_prices_daily to ClickHouse -> PG/CH reconciliation.
Triggered by final_shermatova_pipeline with the same logical date (can also be run by hand)."""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from _lib.tenant import student_dag
from airflow.decorators import task

from etl_common import (CHECKS_DM, DM, ch_client, ensure_ddl, log, logged, pg_hook, run_checks, run_ds,
                        run_sql_file, scalar)

default_args = {"owner": "shermatova", "retries": 2, "retry_delay": timedelta(seconds=30)}

CH_TABLE = "dm_prices_daily"
CH_COLS = ["trade_date", "tickers_count", "total_volume", "total_traded_value", "advancers",
           "decliners", "unchanged", "avg_range_pct", "api_tickers_checked", "api_avg_abs_close_diff_pct"]

with student_dag(
    __file__,
    dag_id="final_shermatova_datamart",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["final", "shermatova"],
) as dag:

    @task
    def build_dm_prices_daily(**context):
        ds = run_ds(context)
        ensure_ddl()
        with logged(context, "dm_prices_daily") as rec:
            run_sql_file("03_dm_prices_daily.sql", ds)
            rec["rows"] = scalar(f"SELECT count(*) FROM {DM}.dm_prices_daily WHERE trade_date = %(ds)s::date", ds)

    @task
    def build_dm_prices_exchange_month(**context):
        ds = run_ds(context)
        with logged(context, "dm_prices_exchange_month") as rec:
            run_sql_file("03_dm_prices_exchange_month.sql", ds)
            rec["rows"] = scalar(
                f"SELECT count(*) FROM {DM}.dm_prices_exchange_month "
                "WHERE month_start = date_trunc('month', %(ds)s::date)::date", ds)

    @task
    def dq_datamart(**context):
        with logged(context, "quality_checks_datamart") as rec:
            run_checks(CHECKS_DM, run_ds(context))
            rec["rows"] = len(CHECKS_DM)

    @task
    def copy_to_clickhouse(**context):
        """Same ds, no duplicates: delete this day in ClickHouse, then insert it again."""
        ds = run_ds(context)
        with logged(context, "clickhouse." + CH_TABLE) as rec:
            rows = pg_hook().get_records(
                f"SELECT {', '.join(CH_COLS)} FROM {DM}.dm_prices_daily WHERE trade_date = %s", parameters=(ds,))
            client = ch_client()
            client.command(f"""
                CREATE TABLE IF NOT EXISTS {CH_TABLE} (
                    trade_date Date, tickers_count UInt32, total_volume UInt64,
                    total_traded_value Decimal(24, 2), advancers UInt32, decliners UInt32,
                    unchanged UInt32, avg_range_pct Nullable(Decimal(10, 4)),
                    api_tickers_checked UInt32, api_avg_abs_close_diff_pct Nullable(Decimal(10, 4)),
                    loaded_at DateTime DEFAULT now()
                ) ENGINE = MergeTree ORDER BY trade_date""")
            client.command(f"ALTER TABLE {CH_TABLE} DELETE WHERE trade_date = '{ds}' SETTINGS mutations_sync = 2")
            if rows:
                client.insert(CH_TABLE, [list(r) for r in rows], column_names=CH_COLS)
            log.info("ClickHouse %s: %d rows for %s", CH_TABLE, len(rows), ds)
            rec["rows"] = len(rows)

    @task
    def reconcile_clickhouse(**context):
        """PG and CH must agree on row count and on sum(total_volume) (whole table)."""
        with logged(context, "reconcile_pg_vs_clickhouse") as rec:
            pg_n, pg_sum = pg_hook().get_first(
                f"SELECT count(*), COALESCE(sum(total_volume), 0) FROM {DM}.dm_prices_daily")
            ch_n, ch_sum = ch_client().query(f"SELECT count(), sum(total_volume) FROM {CH_TABLE}").result_rows[0]
            log.info("RECONCILE postgres rows=%s volume=%s | clickhouse rows=%s volume=%s", pg_n, pg_sum, ch_n, ch_sum)
            if (int(pg_n), int(pg_sum)) != (int(ch_n), int(ch_sum or 0)):
                raise ValueError(f"Postgres {pg_n}/{pg_sum} != ClickHouse {ch_n}/{ch_sum}")
            rec["rows"] = int(ch_n)

    daily, monthly = build_dm_prices_daily(), build_dm_prices_exchange_month()
    [daily, monthly] >> dq_datamart() >> copy_to_clickhouse() >> reconcile_clickhouse()
