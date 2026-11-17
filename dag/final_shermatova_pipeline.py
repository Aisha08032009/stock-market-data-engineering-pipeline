"""DAG 1: CSV files + Twelve Data API -> stg -> ods -> quality checks -> triggers the datamart DAG."""
from __future__ import annotations

import os
import sys
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))   # lets Airflow import etl_common

import requests
from _lib.tenant import student_dag
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.models import DagRun
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.python import PythonSensor

from etl_common import (API_SYMBOLS, CHECKS_ODS, DATA_DIR, ODS, STG, TD_CONN, ensure_ddl, log, run_ds,
                        logged, reload_full, replace_day, run_checks, run_sql_file, scalar)

default_args = {"owner": "shermatova", "retries": 2, "retry_delay": timedelta(seconds=30)}

with student_dag(
    __file__,
    dag_id="final_shermatova_pipeline",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["final", "shermatova"],
) as dag:

    @task
    def init_schema(**context):
        ensure_ddl()                                   # creates schemas, tables, journal
        with logged(context, "ddl") as rec:
            rec["rows"] = 0

    # ------------------------------------------------------------------ STG
    @task
    def load_stg_prices(**context):
        """Event file: only rows of this ds. Re-run = delete this ds + insert again."""
        ds = run_ds(context)
        with logged(context, "prices_daily.csv") as rec:
            path = DATA_DIR / "prices_daily.csv"
            prefix, rows = ds + ",", []
            with open(path, encoding="utf-8") as f:
                f.readline()                           # header
                for line in f:
                    if line.startswith(prefix):
                        rows.append(line.rstrip("\r\n").split(","))
            cols = ["date", "ticker", "open", "high", "low", "close", "volume"]
            replace_day(f"{STG}.stg_prices_daily", ds, cols, rows, path.name)
            in_stg = scalar(f"SELECT count(*) FROM {STG}.stg_prices_daily WHERE _ds = %(ds)s::date", ds)
            if in_stg != len(rows):
                raise ValueError(f"stg has {in_stg} rows, file has {len(rows)} for {ds}")
            log.info("prices_daily.csv: %d rows for %s (file = stg)", len(rows), ds)
            rec["rows"] = len(rows)

    @task
    def load_stg_stocks(**context):
        with logged(context, "12data_stocks.csv") as rec:
            rec["rows"] = reload_full(
                f"{STG}.stg_stocks", ["symbol", "name", "currency", "exchange", "mic_code", "country", "type"],
                DATA_DIR / "12data_stocks.csv", run_ds(context))

    @task
    def load_stg_exchanges(**context):
        with logged(context, "12data_stock_exchanges.csv") as rec:
            rec["rows"] = reload_full(
                f"{STG}.stg_stock_exchanges", ["title", "name", "code", "country", "timezone"],
                DATA_DIR / "12data_stock_exchanges.csv", run_ds(context))

    @task(retries=3, retry_delay=timedelta(seconds=30))
    def load_stg_api(**context):
        """Twelve Data /time_series for API_SYMBOLS on ds. Any error -> task fails, nothing is written."""
        ds = run_ds(context)
        with logged(context, "twelvedata_api") as rec:
            conn = BaseHook.get_connection(TD_CONN)
            base = conn.host or "https://api.twelvedata.com"
            if not base.startswith("http"):
                base = "https://" + base
            end = (date.fromisoformat(ds) + timedelta(days=1)).isoformat()   # end_date is exclusive
            resp = requests.get(
                f"{base.rstrip('/')}/time_series",
                params={"symbol": ",".join(API_SYMBOLS), "interval": "1day",
                        "start_date": ds, "end_date": end},
                headers={"Authorization": f"apikey {conn.password}"},        # key never goes into the URL
                timeout=20)
            resp.raise_for_status()
            payload = resp.json()

            # one symbol -> flat answer, many symbols -> {SYMBOL: answer}
            answers = payload if all(isinstance(v, dict) for v in payload.values()) else {API_SYMBOLS[0]: payload}
            if payload.get("status") == "error":
                answers = {API_SYMBOLS[0]: payload}
            rows, errors, no_data = [], [], 0
            for sym, ans in answers.items():
                if ans.get("status") == "error":
                    msg = str(ans.get("message", ""))
                    if "no data" in msg.lower():
                        no_data += 1                    # weekend / holiday: normal, not an error
                    else:
                        errors.append(f"{sym}: {ans.get('code')} {msg}")
                    continue
                meta = ans.get("meta", {})
                for v in ans.get("values", []):
                    rows.append((sym, v.get("datetime"), v.get("open"), v.get("high"), v.get("low"),
                                 v.get("close"), v.get("volume"), meta.get("exchange"), meta.get("currency")))
            if errors:
                raise RuntimeError("Twelve Data errors: " + " | ".join(errors))

            cols = ["symbol", "datetime", "open", "high", "low", "close", "volume", "exchange", "currency"]
            replace_day(f"{STG}.stg_twelvedata", ds, cols, rows, "twelvedata:/time_series")
            log.info("Twelve Data: %d rows for %s (%d symbols without data)", len(rows), ds, no_data)
            rec["rows"] = len(rows)

    # ------------------------------------------------------------------ ODS
    def ods_task(task_id, sql_file, source, count_sql):
        @task(task_id=task_id)
        def _run(**context):
            ds = run_ds(context)
            with logged(context, source) as rec:
                run_sql_file(sql_file, ds)
                rec["rows"] = scalar(count_sql, ds)
        return _run()

    @task
    def dq_ods(**context):
        with logged(context, "quality_checks_ods") as rec:
            run_checks(CHECKS_ODS, run_ds(context))
            rec["rows"] = len(CHECKS_ODS)

    start = init_schema()
    s_prices, s_stocks, s_exch, s_api = load_stg_prices(), load_stg_stocks(), load_stg_exchanges(), load_stg_api()
    start >> [s_prices, s_stocks, s_exch, s_api]

    o_prices = ods_task("ods_prices", "02_ods_prices.sql", "ods_prices",
                        f"SELECT count(*) FROM {ODS}.ods_prices WHERE trade_date = %(ds)s::date")
    o_stocks = ods_task("ods_stocks", "02_ods_stocks.sql", "ods_stocks", f"SELECT count(*) FROM {ODS}.ods_stocks")
    o_exch = ods_task("ods_stock_exchanges", "02_ods_stock_exchanges.sql", "ods_stock_exchanges",
                      f"SELECT count(*) FROM {ODS}.ods_stock_exchanges")
    o_api = ods_task("ods_twelvedata", "02_ods_twelvedata.sql", "ods_twelvedata",
                     f"SELECT count(*) FROM {ODS}.ods_twelvedata_prices WHERE trade_date = %(ds)s::date")
    s_prices >> o_prices
    s_stocks >> o_stocks
    s_exch >> o_exch
    s_api >> o_api

    checks = dq_ods()
    [o_prices, o_stocks, o_exch, o_api] >> checks

    # the datamart DAG runs for the SAME logical date; reset_dag_run lets a re-run work.
    # We do NOT wait inside the trigger task (it would hold a worker slot for the whole time):
    # a sensor in "reschedule" mode checks the result and frees the slot between checks.
    datamart_dag_id = dag.dag_id.replace("final_shermatova_pipeline", "final_shermatova_datamart")

    trigger_datamart = TriggerDagRunOperator(
        task_id="trigger_datamart",
        trigger_dag_id=datamart_dag_id,
        logical_date="{{ logical_date }}",
        conf={"ds": "{{ (dag_run.conf or {}).get('ds') or ds }}"},
        reset_dag_run=True,
        wait_for_completion=False,
    )

    def _datamart_done(**context):
        runs = DagRun.find(dag_id=datamart_dag_id, execution_date=context["logical_date"])
        state = str(runs[0].state) if runs else None
        log.info("datamart run for %s is: %s", context["logical_date"], state)
        if state == "failed":
            raise AirflowFailException("final_shermatova_datamart failed for this logical date")
        return state == "success"

    wait_datamart = PythonSensor(
        task_id="wait_datamart",
        python_callable=_datamart_done,
        mode="reschedule",
        poke_interval=15,
        timeout=1800,
    )
    checks >> trigger_datamart >> wait_datamart
