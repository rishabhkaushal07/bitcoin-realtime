# Project Conventions

## Directory and Data Layout

All project data and code lives under `/local-scratch4/bitcoin_2025/` (SSD).

### Code and Docs
- **Active repo:** `/local-scratch4/bitcoin_2025/bitcoin-realtime/` — all new code, configs, and docs go here.
- **Old repo (read-only):** `/local-scratch4/bitcoin_2025/dmg-bitcoin/` — reference only. Do NOT modify. May read for schema/code reference.

### Persistent Data (all on `/local-scratch4/bitcoin_2025/`)
- **Bitcoin Core:** `bitcoin-core-data/` (~600 GB) — full node with txindex, RPC, ZMQ
- **MinIO (Iceberg):** `minio-data/` (~1 TB) — raw source of truth, 4 Iceberg tables
- **StarRocks BE:** `starrocks-data/` (~200+ GB) — native flat serving table + data cache
- **StarRocks FE:** `starrocks-fe-meta/` — FE metadata
- **Kafka:** `kafka-data/` (~50 GB) — KRaft broker, 7-day retention
- **HMS MySQL:** `hms-mysql-data/` — Iceberg catalog metadata

Docker volumes must NOT be used for persistent data — bind-mount to the paths above instead.

## Bitcoin Core

- Data dir: `/local-scratch4/bitcoin_2025/bitcoin-core-data/`
- RPC: `bitcoinrpc` / `changeme_strong_password_here` on port 8332
- ZMQ: hashblock=28332, rawblock=28333, sequence=28334
- Check sync: `bitcoin-cli -datadir=/local-scratch4/bitcoin_2025/bitcoin-core-data getblockchaininfo`

## Architecture Plan

Full V3 plan: `/local-scratch4/bitcoin_2025/nifty-sauteeing-spring-evaluation-report-v3.md`
Implementation plan: `/local-scratch4/bitcoin_2025/bitcoin-realtime/plan.md`

## Code Style and Best Practices

### Python
- **Package manager:** UV (v0.11.2) — `uv pip install -e ".[dev]"`
- **Virtual env:** `.venv/` in project root (managed by UV)
- **Python version:** 3.10+
- **Max file size:** ~200 LOC per module. Split if larger.
- **Testing:** pytest with markers (`unit`, `integration`, `slow`)
- **Linting:** ruff (configured in pyproject.toml)
- **Type checking:** mypy (optional, configured in pyproject.toml)
- **Dependencies:** All in `pyproject.toml` (NOT requirements.txt for new code)

### File Organization
- One responsibility per file
- Each subfolder has its own `README.md` with ASCII art and tables
- Test files mirror source structure under `tests/unit/` and `tests/integration/`
- SQL files live in their component directories (`iceberg/`, `starrocks/`)

### Docker
- All images pinned to specific versions (no `:latest`)
- All data bind-mounted to `/local-scratch4/bitcoin_2025/`
- Current versions: Kafka 4.0.2, MinIO 2025-09-07, MySQL 8.4.8, Hive 3.1.3, StarRocks 4.0.8

### Testing
- Run unit tests: `source .venv/bin/activate && pytest tests/unit/`
- Run integration tests: `source .venv/bin/activate && pytest tests/integration/ -m integration`
- DDL validation: `pytest tests/unit/test_ddl_validation.py`
- All unit tests must pass before committing

### Git
- `.gitignore` covers Python, Rust, Kafka, Docker, Iceberg, Bitcoin Core, IDE
- Never commit data files, secrets, or Docker volumes
- Never commit `checkpoint.json` (runtime state)
