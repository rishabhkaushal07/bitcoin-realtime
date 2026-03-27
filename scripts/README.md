# Scripts

Operational scripts for the Bitcoin real-time pipeline.

```
scripts/
  |
  +-- create-kafka-topics.sh      Create 4 data topics + 1 control topic
  +-- create_iceberg_tables.py    Create 4 Iceberg tables via PyIceberg
```

---

## Available Scripts

| Script | Purpose | Prerequisites |
|--------|---------|---------------|
| `create-kafka-topics.sh` | Creates Kafka topics for the pipeline | Kafka container running |
| `create_iceberg_tables.py` | Creates 4 Iceberg tables in btc namespace | HMS + MinIO running |

---

## Kafka Topics

| Topic | Partitions | Retention | Cleanup | Purpose |
|-------|-----------|-----------|---------|---------|
| `btc.blocks.v1` | 1 | 7 days | delete | Block records |
| `btc.transactions.v1` | 1 | 7 days | delete | Transaction records |
| `btc.tx_inputs.v1` | 1 | 7 days | delete | Input records |
| `btc.tx_outputs.v1` | 1 | 7 days | delete | Output records |
| `control-iceberg` | 1 | forever | compact | Finality/reorg events + coordination |

---

## Iceberg Tables

| Table | Namespace | Identifier Fields |
|-------|-----------|-------------------|
| `blocks` | `btc` | `block_hash` |
| `transactions` | `btc` | `txid` |
| `tx_in` | `btc` | `txid, hashPrevOut, indexPrevOut` |
| `tx_out` | `btc` | `txid, indexOut` |

---

## Usage

```bash
# Create all Kafka topics (run once after Kafka is up)
bash scripts/create-kafka-topics.sh

# Verify topics
sudo docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list

# Create Iceberg tables (run once after HMS + MinIO are healthy)
source .venv/bin/activate
python scripts/create_iceberg_tables.py

# Verify tables via PyIceberg
python -c "
from pyiceberg.catalog import load_catalog
catalog = load_catalog('default', **{
    'type': 'hive', 'uri': 'thrift://localhost:9083',
    's3.endpoint': 'http://localhost:9000',
    's3.access-key-id': 'minioadmin',
    's3.secret-access-key': 'minioadmin',
})
for t in catalog.list_tables('btc'):
    print(t)
"
```
