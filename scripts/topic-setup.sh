#!/bin/sh
set -euo pipefail

# Wait for Kafka broker to be available
/scripts/wait-for-it.sh kafka:9092 -- echo "Kafka is up"

# Create topics (idempotent)
kafka-topics --create --if-not-exists \
  --topic cdr_raw \
  --bootstrap-server kafka:9092 \
  --partitions 3 --replication-factor 1

kafka-topics --create --if-not-exists \
  --topic cdr_error \
  --bootstrap-server kafka:9092 \
  --partitions 3 --replication-factor 1

kafka-topics --create --if-not-exists \
  --topic cdr_flattened \
  --bootstrap-server kafka:9092 \
  --partitions 3 --replication-factor 1

# Set retention policy on raw topic (7 days)
kafka-configs --alter \
  --bootstrap-server kafka:9092 \
  --entity-type topics \
  --entity-name cdr_raw \
  --add-config retention.ms=604800000,cleanup.policy=delete
