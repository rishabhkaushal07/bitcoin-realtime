#!/usr/bin/env bash
# =============================================================================
# Create Kafka topics for the Bitcoin real-time pipeline
# V3 Plan Section 9.2 — 4 data topics + 1 control topic
# =============================================================================
set -euo pipefail

BOOTSTRAP="localhost:9092"

echo "Creating Kafka topics..."

# Data topics — 1 partition each (Bitcoin volume is low)
for TOPIC in btc.blocks.v1 btc.transactions.v1 btc.tx_inputs.v1 btc.tx_outputs.v1; do
    docker exec kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server "$BOOTSTRAP" \
        --create \
        --topic "$TOPIC" \
        --partitions 1 \
        --replication-factor 1 \
        --config retention.ms=604800000 \
        --if-not-exists
    echo "  ✓ $TOPIC"
done

# Control topic for Kafka Connect Iceberg Sink commit coordination
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --create \
    --topic control-iceberg \
    --partitions 1 \
    --replication-factor 1 \
    --config cleanup.policy=compact \
    --if-not-exists
echo "  ✓ control-iceberg"

echo ""
echo "All topics created. Listing:"
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --list
