#!/usr/bin/env bash
set -euo pipefail

BROKER="kafka:9092"
TOPICS=("cdr_raw" "cdr_error" "cdr_flattened")

for t in "${TOPICS[@]}"; do
  echo "Waiting for topic $t ..."
  until kafka-topics --bootstrap-server "$BROKER" --describe --topic "$t" >/dev/null 2>&1; do
    sleep 2
  done
  echo "Topic $t is available."
done
