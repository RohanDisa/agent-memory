#!/usr/bin/env bash
# Partition {A,B}|{C} via each node's HTTP deny-list, write on the majority,
# show C stale, heal, wait for anti-entropy, show C caught up.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
docker compose up -d --build
trap 'docker compose down' EXIT

for p in 8081 8082 8083; do
  for i in $(seq 1 40); do
    curl -sf "http://127.0.0.1:$p/health" >/dev/null && break
    sleep 0.25
  done
done

echo "== partition {A,B} | {C}"
curl -sS -X POST http://127.0.0.1:8081/debug/fault -H 'content-type: application/json' -d '{"action":"block","peers":["C"]}'
echo
curl -sS -X POST http://127.0.0.1:8082/debug/fault -H 'content-type: application/json' -d '{"action":"block","peers":["C"]}'
echo
curl -sS -X POST http://127.0.0.1:8083/debug/fault -H 'content-type: application/json' -d '{"action":"block","peers":["A","B"]}'
echo

echo "== majority write W=2"
curl -sS -X POST http://127.0.0.1:8081/write -H 'content-type: application/json' \
  -d '{"key":"customer-7:plan","data":"enterprise","policy":"LWW","w":2}'
echo

echo "== C local store (should be missing the key)"
curl -sS "http://127.0.0.1:8083/debug/kv?key=customer-7:plan"
echo

echo "== heal + anti-entropy"
curl -sS -X POST http://127.0.0.1:8081/debug/fault -H 'content-type: application/json' -d '{"action":"heal"}'
echo
curl -sS -X POST http://127.0.0.1:8082/debug/fault -H 'content-type: application/json' -d '{"action":"heal"}'
echo
curl -sS -X POST http://127.0.0.1:8083/debug/fault -H 'content-type: application/json' -d '{"action":"heal"}'
echo
curl -sS -X POST http://127.0.0.1:8081/debug/anti-entropy
echo
curl -sS -X POST http://127.0.0.1:8082/debug/anti-entropy
echo

echo "== C after anti-entropy"
curl -sS "http://127.0.0.1:8083/debug/kv?key=customer-7:plan"
echo
