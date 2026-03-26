# Tests

Test suite for the Bitcoin real-time pipeline.

```
+-------------------------------------------+
|              Test Pyramid                 |
+-------------------------------------------+
|                                           |
|          /  Integration Tests  \          |
|         /  (Docker + Bitcoin)   \         |
|        /     ~15 tests           \        |
|       +---------------------------+       |
|      /      Unit Tests             \      |
|     /    (no external deps)         \     |
|    /       71 tests                  \    |
|   +-----------------------------------+   |
+-------------------------------------------+
```

---

## Test Suites

| Suite | Directory | Count | External Deps | Command |
|-------|-----------|------:|---------------|---------|
| Normalizer | `unit/test_normalizer.py` | 30 | None | `pytest tests/unit/test_normalizer.py` |
| DDL Validation | `unit/test_ddl_validation.py` | 13 | None | `pytest tests/unit/test_ddl_validation.py` |
| Kafka Producer | `unit/test_kafka_producer.py` | 9 | None | `pytest tests/unit/test_kafka_producer.py` |
| Checkpoint | `unit/test_checkpoint_store.py` | 7 | None | `pytest tests/unit/test_checkpoint_store.py` |
| RPC Client | `unit/test_rpc_client.py` | 6 | None | `pytest tests/unit/test_rpc_client.py` |
| ZMQ Listener | `unit/test_zmq_listener.py` | 4 | None | `pytest tests/unit/test_zmq_listener.py` |
| Docker Health | `integration/test_docker_services.py` | ~10 | Docker | `pytest tests/integration/ -m integration` |
| Live RPC | `integration/test_normalizer_live.py` | ~5 | Bitcoin Core | `pytest tests/integration/ -m integration` |

---

## Running Tests

```bash
# Activate venv
source .venv/bin/activate

# All unit tests
pytest tests/unit/ -v

# All integration tests
pytest tests/integration/ -m integration -v

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
