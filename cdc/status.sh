#!/bin/sh
# Shows the state of both connectors. Both must say RUNNING.
for c in customers-source-shermatova sink-shermatova; do
  echo "== $c"
  curl -s http://kafka-connect:8083/connectors/$c/status | python3 -c "import sys,json; d=json.load(sys.stdin); print('connector:', d['connector']['state'], '| tasks:', [t['state'] for t in d['tasks']])"
done
