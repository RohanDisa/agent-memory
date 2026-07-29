#!/usr/bin/env bash
# Optional garnish. Phrases two contradicting facts with a local Ollama model.
# No test depends on this. The store runs fine if Ollama is missing.
set -euo pipefail
if ! command -v ollama >/dev/null; then
  echo "ollama not installed; skip. core tests do not use this script."
  exit 0
fi
if ! ollama list >/dev/null 2>&1; then
  echo "ollama daemon not running; skip."
  exit 0
fi
MODEL="${OLLAMA_MODEL:-llama3.2:1b}"
echo "session A would say: $(ollama run "$MODEL" "One short sentence: the customer is on the enterprise plan.")"
echo "session B would say: $(ollama run "$MODEL" "One short sentence: the customer is on the free plan.")"
echo "the store resolves the contradiction with LWW (Lamport+node_id), not the model."
