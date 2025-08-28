#!/usr/bin/env bash
set -euo pipefail

# Wait for Kafka to come online
./wait-for-it.sh kafka:9092 -- echo "Kafka is up"

# Create topics if not exists
kafka-topics --create --if-not-exists \
  --topic cdr_raw --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1

kafka-topics --create --if-not-exists \
  --topic cdr_error --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1

kafka-topics --create --if-not-exists \
  --topic cdr_flattened --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
