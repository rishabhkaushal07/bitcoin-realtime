# Scripts

Operational scripts for bootstrapping the Bitcoin real-time pipeline infrastructure.

```
scripts/
  |
  +-- create-kafka-topics.sh          Bash: create 4 data topics + 1 control topic
  +-- create_iceberg_tables.py        Python: create 4 Iceberg tables via PyIceberg
  +-- create_iceberg_tables_spark.py  Python: create 4 Iceberg tables via Spark SQL
  +-- benchmark_e2e_latency.py        Python: end-to-end latency benchmark
```

```
+=====================================================================+
|                     Script Execution Order                           |
+=====================================================================+
|                                                                     |
|  1. docker compose up -d          (start all services)              |
|          |                                                          |
|          v                                                          |
|  2. create-kafka-topics.sh        (requires: Kafka healthy)         |
|          |                                                          |
|          v                                                          |
|  3. create_iceberg_tables.py      (requires: HMS + MinIO healthy)   |
|          |                                                          |
|          v                                                          |
|  4. iceberg_catalog_v3.sql        (requires: StarRocks + HMS)       |
|     (run in StarRocks via mysql)                                    |
|                                                                     |
+=====================================================================+
```

---

## Script Details

### `create-kafka-topics.sh`

Creates the 5 Kafka topics needed by the pipeline. Idempotent (`--if-not-exists`).

| Aspect | Detail |
|--------|--------|
| Language | Bash |
| Prerequisites | Kafka container running and healthy |
| Idempotent | Yes (`--if-not-exists` flag) |
| Docker access | Runs `docker exec kafka ...` (needs Docker permission) |

**Topics created:**

| Topic | Partitions | Retention | Cleanup | Purpose |
|-------|-----------|-----------|---------|---------|
| `btc.blocks.v1` | 1 | 7 days | delete | Block records from normalizer |
| `btc.transactions.v1` | 1 | 7 days | delete | Transaction records |
| `btc.tx_inputs.v1` | 1 | 7 days | delete | Input records |
| `btc.tx_outputs.v1` | 1 | 7 days | delete | Output records |
| `control-iceberg` | 1 | forever | compact | Finality/reorg events + coordination |

**Why 1 partition per topic:** Bitcoin produces ~1 block every 10 minutes. Even the
largest block generates ~10K transactions. A single partition handles this throughput
trivially and preserves strict ordering (block height sequence).

---

### `create_iceberg_tables.py`

Creates the `btc` namespace and 4 Iceberg raw tables in HMS via PyIceberg.

| Aspect | Detail |
|--------|--------|
| Language | Python 3.10 |
| Prerequisites | HMS running on port 9083, MinIO running on port 9000, `warehouse` bucket exists |
| Dependencies | `pyiceberg` (from pyproject.toml) |
| Idempotent | Yes (skips existing tables with "already exists" check) |
| Virtual env | Must activate `.venv` first |

**Usage:**

```bash
source .venv/bin/activate

# Create tables (skips if they already exist)
python scripts/create_iceberg_tables.py

# Drop and recreate (destructive!)
python scripts/create_iceberg_tables.py --drop
```

**Tables created:**

| Table | Identifier Fields | Partition | Purpose |
|-------|-------------------|-----------|---------|
| `btc.blocks` | `block_hash` | `bucket(10, height)` | Block headers |
| `btc.transactions` | `txid` | `bucket(10, block_height)` | Transactions |
| `btc.tx_in` | `txid, hashPrevOut, indexPrevOut` | `bucket(10, block_height)` | Inputs |
| `btc.tx_out` | `txid, indexOut` | `bucket(10, height)` | Outputs |

**Configuration (hardcoded in script):**

| Setting | Value |
|---------|-------|
| HMS URI | `thrift://localhost:9083` |
| S3 endpoint | `http://localhost:9000` |
| S3 credentials | `minioadmin` / `minioadmin` |
| Warehouse | `s3://warehouse/` |
| IO implementation | `pyiceberg.io.pyarrow.PyArrowFileIO` |

---

### `create_iceberg_tables_spark.py`

Alternative table creation using Spark SQL instead of PyIceberg. Creates the same 4
tables with identical schemas and partitioning. Also sets identifier fields via
`ALTER TABLE ... SET IDENTIFIER FIELDS`.

| Aspect | Detail |
|--------|--------|
| Language | Python 3.10 (PySpark) |
| Prerequisites | HMS + MinIO running, Spark JARs downloadable |
| Dependencies | `pyspark` (from pyproject.toml) |
| When to use | If PyIceberg has issues, or for testing Spark connectivity |

**Note:** Downloads JARs on first run (`iceberg-spark-runtime`, `hadoop-aws`, `aws-sdk`).
Uses hadoop-aws-3.4.1 (different from HMS's 3.1.0) because Spark bundles its own Hadoop.

---

## Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| `create-kafka-topics.sh` fails with "connection refused" | Kafka not running | `sudo docker compose up -d kafka` and wait for healthy |
| `create_iceberg_tables.py` hangs | HMS not responding (thread pool exhaustion or still starting) | Check HMS: `sudo docker exec hive-metastore grep 'Started the new metaserver' /tmp/hive/hive.log` |
| `create_iceberg_tables.py` fails with Thrift error | HMS unhealthy or wrong version | Ensure HMS is Hive 3.1.3 (not 4.x). See `hive-metastore/RCA-HMS-ISSUES.md` |
| "warehouse bucket not found" | MinIO bucket not created | `sudo docker compose up minio-init` |

---

## Verification

```bash
# Verify Kafka topics
sudo docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list

# Verify Iceberg tables via PyIceberg
source .venv/bin/activate
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
