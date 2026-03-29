# Live Normalizer

Real-time Bitcoin block normalizer — listens for new blocks via ZMQ, fetches decoded data via RPC, normalizes into 4 record types, and publishes to Kafka.

```
                    +-----------+
                    | Bitcoin   |
                    | Core Node |
                    +-----+-----+
                          |
              +-----------+-----------+
              |                       |
        ZMQ hashblock            RPC getblock
        (block hash only)        (full decoded JSON)
              |                       |
              v                       v
        +-------------+    +------------------+
        | zmq_listener|    | rpc_client       |
        | .py         |--->| .py              |
        +------+------+    +--------+---------+
               |                    |
               +--------+-----------+
                        |
                        v
              +-------------------+
              | normalizer.py     |
              | Maps RPC JSON to: |
              |  - blocks         |
              |  - transactions   |
              |  - tx_inputs      |
              |  - tx_outputs     |
              +--------+----------+
                       |
                       v
              +-------------------+
              | kafka_producer.py |
              | 4 Kafka topics    |
              +--------+----------+
                       |
                       v
              +-------------------+
              | checkpoint_store  |
              | .py               |
              | (restart recovery)|
              +-------------------+
```

---

## Module Breakdown

| Module | LOC | Responsibility |
|--------|----:|----------------|
| `main.py` | ~200 | Entry point — `--test-block`, `--catchup`, live ZMQ mode |
| `zmq_listener.py` | ~70 | ZMQ SUB socket for `hashblock` topic, sequence gap detection |
| `rpc_client.py` | ~50 | Thin Bitcoin Core JSON-RPC wrapper (`getblock`, `getblockhash`, etc.) |
| `normalizer.py` | ~140 | Maps RPC block JSON to 4 flat record types (BTC->satoshi via Decimal) |
| `kafka_producer.py` | ~100 | Idempotent Kafka producer, publishes to 4 topics with proper keys |
| `checkpoint_store.py` | ~70 | File-based JSON checkpoint for restart recovery |

---

## Data Flow Per Block

```
1. ZMQ delivers block hash (32 bytes)          ~0ms latency
2. RPC getblock(hash, 2) fetches decoded JSON   ~3-426ms (scales with tx count)
3. Normalizer maps JSON to 4 record types       ~0.04-14ms
4. Kafka producer publishes to 4 topics         ~2-77ms (with flush)
5. Checkpoint updated to disk                   ~1ms
                                          Total: ~6-520ms per block
```

### Measured Latency (baseline benchmark, 2026-03-27)

| Block size | RPC ms | Normalize ms | Kafka ms | Total ms | Records |
|-----------|-------:|------------:|--------:|--------:|--------:|
| 1 tx (early) | 4.8 | 1.8 | 3.1 | 9.7 | 4 |
| ~100 tx (200K era) | 137.6 | 0.5 | 7.4 | 145.5 | 818 |
| ~2,500 tx (500K era) | 425.9 | 14.3 | 76.5 | 516.7 | 13,423 |

RPC is the dominant cost — it scales linearly with block size because Bitcoin Core
must serialize the full decoded JSON. Normalization and Kafka produce are negligible
relative to RPC.

---

## Record Types

| Topic | Key | Fields |
|-------|-----|--------|
| `btc.blocks.v1` | `block_hash` | hash, height, version, size, prev, merkle, time, bits, nonce, finality |
| `btc.transactions.v1` | `txid` | txid, hashBlock, version, lockTime, block_height, block_timestamp |
| `btc.tx_inputs.v1` | `txid:hashPrevOut:indexPrevOut` | txid, hashPrevOut, indexPrevOut, scriptSig, sequence |
| `btc.tx_outputs.v1` | `txid:indexOut` | txid, indexOut, height, value (satoshis), scriptPubKey, address |

---

## Usage

```bash
# Test mode — normalize a single block, print output (no Kafka needed)
python main.py --test-block 170

# Catch-up mode — replay from checkpoint to current tip, then go live
python main.py --catchup

# Live mode — listen for new blocks via ZMQ (default)
python main.py

# Custom endpoints
python main.py --rpc-url http://127.0.0.1:8332 \
               --rpc-user bitcoinrpc \
               --rpc-password changeme_strong_password_here \
               --kafka-bootstrap localhost:9092 \
               --zmq-url tcp://127.0.0.1:28332
```

---

## Live Pipeline Status (2026-03-28)

The normalizer is running in live ZMQ mode, connected to the fully-synced Bitcoin Core
node (height 942,722+). Blocks are processed within ~600ms of the ZMQ notification.

**First live blocks processed:**

| Block Height | Transactions | Inputs | Outputs | Kafka Records | Processing Time |
|-------------:|-----------:|---------:|---------:|-------------:|:--------------:|
| 942,725 | 5,684 | 7,240 | 13,075 | 26,000 | ~600ms |
| 942,726 | 7,385 | 7,653 | 15,057 | 30,096 | ~680ms |
| 942,727 | 5,383 | 7,077 | 12,575 | 25,036 | ~580ms |

### Running as Background Service

```bash
# Start in background with logging
nohup .venv/bin/python live-normalizer/main.py \
    --rpc-url http://127.0.0.1:8332 \
    --rpc-user bitcoinrpc \
    --rpc-password changeme_strong_password_here \
    --kafka-bootstrap localhost:9092 \
    --zmq-url tcp://127.0.0.1:28332 \
    > logs/normalizer.log 2>&1 &

# Monitor
tail -f logs/normalizer.log

# Check checkpoint state
cat checkpoint.json
```

---

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| ZMQ + RPC (not rawblock parsing) | Avoids writing a binary parser; RPC returns decoded JSON |
| Idempotent Kafka producer | `enable.idempotence=True` prevents duplicates on retry |
| Decimal for BTC->satoshi | Avoids IEEE 754 float precision loss (V3 Section 8.9) |
| Lazy imports for Kafka/ZMQ | `--test-block` works without Kafka/ZMQ installed |
| File-based checkpoint | Simple, atomic (write-tmp + rename), survives restarts |
