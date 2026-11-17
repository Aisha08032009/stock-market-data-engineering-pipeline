#!/usr/bin/env python3
"""Creates (or updates) the two Kafka Connect connectors from the templates in cdc/connectors.
Secrets are NOT in the files: they are read from the terminal variables PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE
at the moment the script runs, and they are never printed.

Run:   python cdc/register.py            (create / update both connectors)
       python cdc/register.py --delete   (remove both connectors)
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
CONNECT = os.environ.get("CONNECT_URL", "http://kafka-connect:8083").rstrip("/")
SOURCE, SINK = "customers-source-shermatova", "sink-shermatova"


def need(name):
    value = os.environ.get(name)
    if not value:
        sys.exit(f"Variable {name} is not set in this terminal.")
    return value


def call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(CONNECT + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()[:400]


def render(template_name):
    values = {
        "${PG_HOST}": need("PGHOST"),
        "${PG_PORT}": os.environ.get("PGPORT", "5432"),
        "${PG_USER}": need("PGUSER"),
        "${PG_SECRET}": need("PGPASSWORD"),
        "${PG_DBNAME}": need("PGDATABASE"),
    }
    text = (HERE / "connectors" / template_name).read_text(encoding="utf-8")
    for key, value in values.items():          # only known placeholders; ${source.table} stays for the sink
        text = text.replace(key, value)
    return json.loads(text)


def state(name):
    code, body = call("GET", f"/connectors/{name}/status")
    if code != 200:
        return f"{name}: HTTP {code}"
    tasks = ", ".join(t["state"] for t in body.get("tasks", [])) or "no tasks yet"
    return f"{name}: connector={body['connector']['state']} tasks=[{tasks}]"


def wait_running(name, seconds=60):
    end = time.time() + seconds
    while time.time() < end:
        line = state(name)
        if "connector=RUNNING" in line and "FAILED" not in line and "no tasks" not in line:
            return line
        time.sleep(3)
    return state(name)


if "--delete" in sys.argv:
    for name in (SINK, SOURCE):
        print(name, "->", call("DELETE", f"/connectors/{name}")[0])
    sys.exit(0)

for name, template in ((SOURCE, "source.json.template"), (SINK, "sink.json.template")):
    code, body = call("PUT", f"/connectors/{name}/config", render(template)["config"])
    print(f"{name}: HTTP {code}" + ("" if code in (200, 201) else f" -> {body}"))
    if code not in (200, 201):
        sys.exit(1)
    print("  ", wait_running(name))
    if name == SOURCE:
        time.sleep(8)                           # let the source create the topic before the sink starts

print("\nFinal state:")
print("  ", state(SOURCE))
print("  ", state(SINK))
