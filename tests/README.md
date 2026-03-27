# Tests

Test suite for the Bitcoin real-time pipeline.

```
+-------------------------------------------+
|              Test Pyramid                 |
+-------------------------------------------+
|                                           |
|          /  Integration Tests  \          |
|         /  (Docker + Bitcoin)   \         |
|        /     ~60 tests           \        |
|       +---------------------------+       |
|      /      Unit Tests             \      |
|     /    (no external deps)         \     |
|    /       133 tests                 \    |
|   +-----------------------------------+   |
+-------------------------------------------+
```

---

## Test Suites

### Unit Tests (133 total, no external dependencies)

| Suite | File | Count | Scope |
|-------|------|------:|-------|
| Normalizer | `unit/test_normalizer.py` | 28 | Genesis, block 170, large blocks, edge cases |
| Main entry | `unit/test_main.py` | 13 | CLI args, mode selection, error handling |
| DDL Validation | `unit/test_ddl_validation.py` | 17 | Schema consistency across SQL/Python files |
| Iceberg Writer | `unit/test_iceberg_writer.py` | 21 | Topic mapping, buffering, flush, stop |
| Flat Table Builder | `unit/test_flat_table_builder.py` | 21 | SQL structure, range, incremental logic |
| Kafka Producer | `unit/test_kafka_producer.py` | 9 | Routing, keys, serialization |
| Checkpoint | `unit/test_checkpoint_store.py` | 7 | Persistence, atomicity, recovery |
| Finality Updater | `unit/test_finality_updater.py` | 7 | Event validation, upsert logic |
| RPC Client | `unit/test_rpc_client.py` | 6 | Payload, auth, error handling |
| ZMQ Listener | `unit/test_zmq_listener.py` | 4 | Parsing, gap detection |

### Integration Tests (~60 total, require Docker services)

| Suite | File | Count | Prerequisites |
|-------|------|------:|---------------|
| Kafka E2E | `integration/test_kafka_e2e.py` | 15 | Kafka + Bitcoin Core |
| Iceberg Tables | `integration/test_iceberg_tables.py` | 22 | HMS + MinIO + MySQL |
| Kafka->Iceberg | `integration/test_kafka_to_iceberg.py` | 12 | Kafka + HMS + MinIO |
| StarRocks | `integration/test_starrocks.py` | 11 | StarRocks FE/BE + HMS + MinIO |
| Docker Health | `integration/test_docker_services.py` | ~5 | Docker |
| Live RPC | `integration/test_normalizer_live.py` | ~5 | Bitcoin Core |

---

## Running Tests

```bash
# Activate venv
source .venv/bin/activate

# All unit tests (fast, no deps)
pytest tests/unit/ -v
# Expected: 133 passed in <1s

# All integration tests (requires Docker services healthy)
pytest tests/integration/ -m integration -v

# Individual suites
pytest tests/unit/test_normalizer.py -v
pytest tests/unit/test_iceberg_writer.py -v
pytest tests/integration/test_starrocks.py -m integration -v

# Everything
pytest -v

# With coverage
pytest tests/unit/ --cov=live-normalizer --cov-report=term-missing
```

---

## Markers

| Marker | Meaning |
|--------|---------|
| `unit` | No external dependencies required |
| `integration` | Requires Docker services and/or Bitcoin Core |
| `slow` | Long-running tests (e.g., block range iteration) |

---

## Test Coverage by Phase

```
Phase 0 (Prerequisites):
  test_normalizer.py .............. 28 tests  [normalizer correctness]
  test_rpc_client.py .............. 6 tests   [RPC protocol]
  test_zmq_listener.py ........... 4 tests   [ZMQ parsing]
  test_checkpoint_store.py ....... 7 tests   [persistence]

Phase 1a (Event Backbone):
  test_kafka_producer.py ......... 9 tests   [topic routing]
  test_main.py ................... 13 tests  [entry point modes]
  test_ddl_validation.py ......... 17 tests  [schema consistency]
  test_kafka_e2e.py .............. 15 tests  [end-to-end Kafka]

Phase 1b (Raw Lake):
  test_iceberg_writer.py ......... 21 tests  [writer logic]
  test_finality_updater.py ....... 7 tests   [finality logic]
  test_iceberg_tables.py ......... 22 tests  [schema, write/read]
  test_kafka_to_iceberg.py ....... 12 tests  [flush, integrity]

Phase 1c (Serving Bridge):
  test_flat_table_builder.py ..... 21 tests  [SQL, builder logic]
  test_starrocks.py .............. 11 tests  [catalog, flat table]
```
