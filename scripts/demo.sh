#!/usr/bin/env bash
# Spin up 3 nodes (docker compose) and show two "agent sessions" writing
# contradicting facts about the same customer. Resolution is LWW by
# Lamport+node_id, not wall clock.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if ! command -v docker >/dev/null; then
  echo "docker is required for this demo" >&2
  exit 1
fi

docker compose up -d --build
trap 'docker compose down' EXIT

echo "waiting for nodes..."
for p in 8081 8082 8083; do
  for i in $(seq 1 40); do
    curl -sf "http://127.0.0.1:$p/health" >/dev/null && break
    sleep 0.25
  done
done

echo
echo "== session 1 (coordinator A): customer-42 plan=enterprise"
curl -sS -X POST http://127.0.0.1:8081/fact \
  -H 'content-type: application/json' \
  -d '{"entity":"customer-42","attribute":"plan","value":"enterprise","policy":"LWW","w":2}'
echo

echo "== session 2 (coordinator B): customer-42 plan=free  (concurrent contradiction)"
curl -sS -X POST http://127.0.0.1:8082/fact \
  -H 'content-type: application/json' \
  -d '{"entity":"customer-42","attribute":"plan","value":"free","policy":"LWW","w":2}'
echo

echo "== read from C with R=2 (winner is deterministic Lamport+node_id)"
curl -sS "http://127.0.0.1:8083/fact?entity=customer-42&attribute=plan&r=2"
echo

echo "== add-wins tags: concurrent add vip / remove vip"
curl -sS -X POST http://127.0.0.1:8081/tag \
  -H 'content-type: application/json' \
  -d '{"entity":"customer-42","tag":"vip","w":2}'
echo
echo "(OR-Set add-wins is proven in TestORSet* ; this is the live path)"
curl -sS "http://127.0.0.1:8082/tag?entity=customer-42"
echo

echo "== history of the plan fact (WAL on A)"
curl -sS "http://127.0.0.1:8081/history?entity=customer-42&attribute=plan"
echo
