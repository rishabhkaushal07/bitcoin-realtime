# Scripts

Operational scripts for the Bitcoin real-time pipeline.

```
scripts/
  |
  +-- create-kafka-topics.sh    Create 4 data topics + 1 control topic
  +-- (future) start-pipeline.sh
  +-- (future) health-check.sh
```

---

## Available Scripts

| Script | Purpose | Prerequisites |
|--------|---------|---------------|
| `create-kafka-topics.sh` | Creates Kafka topics for the pipeline | Kafka container running |

---

## Kafka Topics

| Topic | Partitions | Retention | Cleanup | Purpose |
|-------|-----------|-----------|---------|---------|
| `btc.blocks.v1` | 1 | 7 days | delete | Block records |
| `btc.transactions.v1` | 1 | 7 days | delete | Transaction records |
| `btc.tx_inputs.v1` | 1 | 7 days | delete | Input records |
| `btc.tx_outputs.v1` | 1 | 7 days | delete | Output records |
| `control-iceberg` | 1 | forever | compact | Kafka Connect commit coordination |

---

## Usage

```bash
# Create all topics (run once after Kafka is up)
bash scripts/create-kafka-topics.sh

# Verify topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list
```
