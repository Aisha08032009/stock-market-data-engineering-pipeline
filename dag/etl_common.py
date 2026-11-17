"""Shared helpers for the final project DAGs (connections, SQL files, load log, quality checks)."""
from __future__ import annotations

import contextlib
import logging
from datetime import datetime
from pathlib import Path

log = logging.getLogger("airflow.task")

# ---- names: change here only ------------------------------------------------
PG_CONN = "aiacademy_pg_shermatova"      # Airflow Connection (Postgres)
CH_CONN = "shermatova_clickhouse"
TD_CONN = "twelvedata_shermatova"     # Connection: host + password (API key)

STG, ODS, DM = "shermatova_stg", "shermatova_ods", "shermatova_datamart"
API_SYMBOLS = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "JPM"]  # 8 credits = 1 minute

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
SQL_DIR = ROOT / "sql"


# ---- which date does this run process? ------------------------------------------
def run_ds(context) -> str:
    """Date to process. Normally the logical date (ds). For manual tests it can be overridden with
    the Trigger config JSON {"ds": "2026-11-06"}; the pipeline passes it on to the datamart DAG."""
    from datetime import date
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run is not None else {}
    ds = conf.get("ds") or context["ds"]
    date.fromisoformat(str(ds))                      # fail early on a typo
    return str(ds)


# ---- Postgres ----------------------------------------------------------------
def pg_hook():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    return PostgresHook(postgres_conn_id=PG_CONN)


def read_sql(name: str) -> str:
    return (SQL_DIR / name).read_text(encoding="utf-8")


def run_sql_file(name: str, ds: str | None = None) -> None:
    """Run a whole SQL file in ONE transaction (commit at the end, rollback on error)."""
    params = {"ds": ds} if ds else None
    pg_hook().run(read_sql(name), parameters=params)


def ensure_ddl() -> None:
    run_sql_file("01_ddl.sql")


def scalar(sql: str, ds: str | None = None):
    params = {"ds": ds} if ds else None
    return pg_hook().get_first(sql, parameters=params)[0]


# ---- ClickHouse --------------------------------------------------------------
def ch_client():
    import clickhouse_connect
    from airflow.hooks.base import BaseHook
    conn = BaseHook.get_connection(CH_CONN)
    return clickhouse_connect.get_client(
        host=conn.host, port=conn.port or 8123,
        username=conn.login or "default", password=conn.password or "",
        database=conn.schema or "default")


# ---- load journal (one row per task attempt) -----------------------------------
@contextlib.contextmanager
def logged(context, source: str):
    """with logged(context, 'prices_csv') as rec:  ...;  rec['rows'] = n
    Writes 'running' at start, then 'success' / 'failed' (+ error text) at the end."""
    ti, ds = context["ti"], run_ds(context)
    key = (ti.dag_id, ti.run_id, ti.task_id, ti.try_number)
    hook = pg_hook()
    rec = {"rows": None}
    try:
        hook.run(f"""INSERT INTO {STG}.etl_load_log
                     (dag_id, run_id, task_id, try_number, source, ds, started_at, status)
                     VALUES (%s, %s, %s, %s, %s, %s, now(), 'running')""",
                 autocommit=True, parameters=key + (source, ds))
    except Exception as exc:                      # journal problems must not hide the real task
        log.warning("etl_load_log insert failed: %s", exc)

    def finish(status: str, error: str | None = None):
        try:
            hook.run(f"""UPDATE {STG}.etl_load_log
                         SET finished_at = now(), status = %s, rows_loaded = %s, error_message = %s
                         WHERE dag_id = %s AND run_id = %s AND task_id = %s AND try_number = %s""",
                     autocommit=True, parameters=(status, rec["rows"], error) + key)
        except Exception as exc:
            log.warning("etl_load_log update failed: %s", exc)

    try:
        yield rec
    except BaseException as exc:
        finish("failed", f"{type(exc).__name__}: {exc}"[:2000])
        raise
    else:
        finish("success")


# ---- STG writers -----------------------------------------------------------------
def replace_day(table: str, ds: str, cols: list[str], rows: list[tuple], source_file: str) -> int:
    """STG, event-style: delete this ds, insert new rows, one transaction (re-run safe)."""
    from psycopg2.extras import execute_values
    conn = pg_hook().get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {table} WHERE _ds = %s", (ds,))
            if rows:
                execute_values(
                    cur, f"INSERT INTO {table} ({', '.join(cols)}, _ds, _source_file) VALUES %s",
                    [tuple(r) + (ds, source_file) for r in rows], page_size=2000)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return len(rows)


def reload_full(table: str, cols: list[str], csv_path: Path, ds: str) -> int:
    """STG, reference-style: whole file replaces the table (TRUNCATE + COPY), one transaction."""
    conn = pg_hook().get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE {table}")
            with open(csv_path, encoding="utf-8", newline="") as f:
                cur.copy_expert(
                    f"COPY {table} ({', '.join(cols)}) FROM STDIN "
                    "WITH (FORMAT csv, HEADER true, DELIMITER ';')", f)
            cur.execute(f"UPDATE {table} SET _ds = %s, _source_file = %s", (ds, csv_path.name))
            cur.execute(f"SELECT count(*) FROM {table}")
            n = cur.fetchone()[0]
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return n


# ---- data quality ------------------------------------------------------------------
def run_checks(checks: dict[str, str], ds: str) -> None:
    """Every check is a SELECT that returns the number of violations. Any > 0 -> task fails."""
    failed = []
    for name, sql in checks.items():
        n = scalar(sql, ds)
        log.info("CHECK %-48s violations=%s", name, n)
        if n:
            failed.append(f"{name}={n}")
    if failed:
        raise ValueError("Data quality checks failed: " + "; ".join(failed))


CHECKS_ODS = {
    "stg_prices: duplicate keys (date,ticker)": f"""
        SELECT count(*) FROM (SELECT 1 FROM {STG}.stg_prices_daily WHERE _ds = %(ds)s::date
                              GROUP BY date, ticker HAVING count(*) > 1) t""",
    "stg_prices vs ods_prices: row count differs": f"""
        SELECT abs((SELECT count(*) FROM {STG}.stg_prices_daily WHERE _ds = %(ds)s::date)
                 - (SELECT count(*) FROM {ODS}.ods_prices WHERE trade_date = %(ds)s::date))""",
    "ods_prices: null key": f"""
        SELECT count(*) FROM {ODS}.ods_prices
        WHERE trade_date = %(ds)s::date AND (ticker IS NULL OR trade_date IS NULL)""",
    "ods_prices: duplicate key (trade_date,ticker)": f"""
        SELECT count(*) FROM (SELECT 1 FROM {ODS}.ods_prices WHERE trade_date = %(ds)s::date
                              GROUP BY trade_date, ticker HAVING count(*) > 1) t""",
    "ods_prices: invalid values (close<=0, volume<0, high<low)": f"""
        SELECT count(*) FROM {ODS}.ods_prices
        WHERE trade_date = %(ds)s::date
          AND (close_price <= 0 OR volume < 0 OR high_price < low_price)""",
    "ods_stocks: null key": f"""
        SELECT count(*) FROM {ODS}.ods_stocks WHERE symbol IS NULL OR mic_code IS NULL""",
    "ods_stocks: duplicate key (symbol,mic_code)": f"""
        SELECT count(*) FROM (SELECT 1 FROM {ODS}.ods_stocks
                              GROUP BY symbol, mic_code HAVING count(*) > 1) t""",
    "ods_stock_exchanges: duplicate or null key": f"""
        SELECT (SELECT count(*) FROM {ODS}.ods_stock_exchanges WHERE exchange_code IS NULL)
             + (SELECT count(*) FROM (SELECT 1 FROM {ODS}.ods_stock_exchanges
                                      GROUP BY exchange_code HAVING count(*) > 1) t)""",
    "ods_twelvedata_prices: duplicate key or invalid values": f"""
        SELECT (SELECT count(*) FROM (SELECT 1 FROM {ODS}.ods_twelvedata_prices
                                      WHERE trade_date = %(ds)s::date
                                      GROUP BY trade_date, symbol HAVING count(*) > 1) t)
             + (SELECT count(*) FROM {ODS}.ods_twelvedata_prices
                WHERE trade_date = %(ds)s::date AND (close_price <= 0 OR high_price < low_price))""",
}

CHECKS_DM = {
    "dm_prices_daily: duplicate key (trade_date)": f"""
        SELECT count(*) FROM (SELECT 1 FROM {DM}.dm_prices_daily
                              GROUP BY trade_date HAVING count(*) > 1) t""",
    "dm_prices_daily: null or non-positive metrics": f"""
        SELECT count(*) FROM {DM}.dm_prices_daily
        WHERE tickers_count IS NULL OR tickers_count <= 0 OR total_volume IS NULL OR total_volume < 0""",
    "dm_prices_daily: day has ods rows but no mart row": f"""
        SELECT CASE WHEN EXISTS (SELECT 1 FROM {ODS}.ods_prices WHERE trade_date = %(ds)s::date)
                     AND NOT EXISTS (SELECT 1 FROM {DM}.dm_prices_daily WHERE trade_date = %(ds)s::date)
                    THEN 1 ELSE 0 END""",
    "dm_prices_exchange_month: duplicate or null key": f"""
        SELECT (SELECT count(*) FROM {DM}.dm_prices_exchange_month
                WHERE month_start IS NULL OR exchange_code IS NULL)
             + (SELECT count(*) FROM (SELECT 1 FROM {DM}.dm_prices_exchange_month
                                      GROUP BY month_start, exchange_code HAVING count(*) > 1) t)""",
    "two marts disagree on total_volume for the month": f"""
        SELECT CASE WHEN
          (SELECT COALESCE(sum(total_volume), 0) FROM {DM}.dm_prices_daily
            WHERE trade_date >= date_trunc('month', %(ds)s::date)::date
              AND trade_date <  (date_trunc('month', %(ds)s::date) + interval '1 month')::date)
          = (SELECT COALESCE(sum(total_volume), 0) FROM {DM}.dm_prices_exchange_month
            WHERE month_start = date_trunc('month', %(ds)s::date)::date)
          THEN 0 ELSE 1 END""",
}
