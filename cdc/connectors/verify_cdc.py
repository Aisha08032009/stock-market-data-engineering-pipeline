#!/usr/bin/env python3
"""Compares the source table with the table filled through Debezium + Kafka (row count and a checksum of all values).
Run:  python cdc/verify_cdc.py        (connection comes from the PG* variables of the terminal)"""
import sys

import psycopg2

SRC, DST, KEY = "shermatova.customers", "shermatova_ods.ods_customers", "customer_id"
COLS = ["customer_id", "city", "signup_date"]

cur = psycopg2.connect("").cursor()


def stats(table):
    row = "md5(row(" + ", ".join(COLS) + ")::text)"
    cur.execute(f"select count(*), coalesce(md5(string_agg({row}, '' order by {KEY})), '') from {table}")
    return cur.fetchone()


try:
    a, b = stats(SRC), stats(DST)
except psycopg2.Error as exc:
    sys.exit(f"FAIL  cannot read {DST}: {str(exc).splitlines()[0]}")
ok = a == b and a[0] > 0
print("OK  " if ok else "FAIL", f"{SRC}: {a[0]} rows | {DST}: {b[0]} rows | checksums {'equal' if a[1] == b[1] else 'DIFFERENT'}")
if not ok:
    cur.execute(f"""(select 'only in source' as side, {', '.join(COLS)} from {SRC}
                     except select 'only in source', {', '.join(COLS)} from {DST})
                    union all
                    (select 'only in ods', {', '.join(COLS)} from {DST}
                     except select 'only in ods', {', '.join(COLS)} from {SRC}) limit 5""")
    for r in cur.fetchall():
        print("   ", r)
sys.exit(0 if ok else 1)
