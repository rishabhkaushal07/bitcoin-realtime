"""Bitcoin Real-Time Normalizer — main entry point.

Lifecycle (V3 plan Section 17.3):
  1. On startup: load checkpoint, compare with current tip, backfill missing
  2. Switch to steady-state ZMQ-triggered mode
  3. On each new block: fetch via RPC, normalize, publish to Kafka, checkpoint

Usage:
  python main.py                     # ZMQ live mode (default)
  python main.py --catchup           # catch up from checkpoint to tip, then live
  python main.py --test-block 100    # normalize a single block and print (no Kafka)
"""

import argparse
import json
import logging
import sys
import time

from rpc_client import BitcoinRPC
from normalizer import normalize_block
from checkpoint_store import CheckpointStore

# Lazy imports for modules with heavy dependencies (Kafka, ZMQ)
# so --test-block works without them installed
BlockKafkaProducer = None
ZMQListener = None


def _ensure_kafka():
    global BlockKafkaProducer
    if BlockKafkaProducer is None:
        from kafka_producer import BlockKafkaProducer as _P
        BlockKafkaProducer = _P


def _ensure_zmq():
    global ZMQListener
    if ZMQListener is None:
        from zmq_listener import ZMQListener as _Z
        ZMQListener = _Z

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("main")


def process_block(rpc: BitcoinRPC, producer, checkpoint,
                  block_hash: str, zmq_seq: int = -1) -> dict:
    """Fetch, normalize, and publish a single block.

    Returns the normalized block data.
    """
    rpc_block = rpc.getblock(block_hash, 2)
    height = rpc_block["height"]

    normalized = normalize_block(rpc_block, source_seq=zmq_seq)

    n_tx = len(normalized["transactions"])
    n_in = len(normalized["tx_inputs"])
    n_out = len(normalized["tx_outputs"])

    if producer:
        count = producer.publish_block(normalized)
        logger.info("Block %d (%s): %d tx, %d in, %d out → %d Kafka records",
                    height, block_hash[:16] + "...", n_tx, n_in, n_out, count)
    else:
        logger.info("Block %d (%s): %d tx, %d in, %d out (dry run, no Kafka)",
                    height, block_hash[:16] + "...", n_tx, n_in, n_out)

    if checkpoint:
        checkpoint.update(height, block_hash, zmq_seq=zmq_seq)

    return normalized


def catchup(rpc: BitcoinRPC, producer, checkpoint: CheckpointStore,
            overlap: int = 10):
    """Catch up from checkpoint to current tip.

    V3 plan Section 11.3-11.6: safe overlap strategy.
    """
    start_height = max(0, checkpoint.last_height - overlap + 1)
    tip = rpc.getblockcount()

    if start_height > tip:
        logger.info("Already at tip (%d), nothing to catch up", tip)
        return

    logger.info("Catching up: %d → %d (%d blocks, overlap=%d)",
                start_height, tip, tip - start_height + 1, overlap)

    checkpoint.update(checkpoint.last_height, checkpoint.last_hash, mode="catchup")

    for h in range(start_height, tip + 1):
        block_hash = rpc.getblockhash(h)
        process_block(rpc, producer, checkpoint, block_hash)

        if h % 1000 == 0 and h > start_height:
            logger.info("Catch-up progress: %d / %d (%.1f%%)",
                        h, tip, (h - start_height) / (tip - start_height + 1) * 100)

    logger.info("Catch-up complete at height %d", tip)


def live_loop(rpc: BitcoinRPC, producer, checkpoint: CheckpointStore,
              zmq_url: str):
    """Steady-state ZMQ-triggered live mode.

    V3 plan Section 8.6: ZMQ says block arrived → RPC fetches decoded data.
    """
    listener = ZMQListener(zmq_url)
    listener.connect()

    checkpoint.update(checkpoint.last_height, checkpoint.last_hash, mode="live")
    logger.info("Entering live mode, listening for new blocks via ZMQ...")

    try:
        while True:
            block_hash, seq = listener.receive()
            try:
                process_block(rpc, producer, checkpoint, block_hash, zmq_seq=seq)
            except Exception as e:
                logger.error("Failed to process block %s: %s", block_hash, e)
                # Don't crash on a single block failure — log and continue
                time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down live loop")
    finally:
        listener.close()


def test_block(rpc: BitcoinRPC, height: int):
    """Normalize a single block and print the output. No Kafka needed."""
    block_hash = rpc.getblockhash(height)
    normalized = normalize_block(rpc.getblock(block_hash, 2))

    print(f"\n=== Block {height} ({block_hash}) ===")
    print(f"Transactions: {len(normalized['transactions'])}")
    print(f"Inputs:       {len(normalized['tx_inputs'])}")
    print(f"Outputs:      {len(normalized['tx_outputs'])}")
    print(f"\n--- Block record ---")
    print(json.dumps(normalized["block"], indent=2))
    print(f"\n--- First transaction ---")
    print(json.dumps(normalized["transactions"][0], indent=2))
    print(f"\n--- First input (coinbase) ---")
    print(json.dumps(normalized["tx_inputs"][0], indent=2))
    print(f"\n--- First output ---")
    print(json.dumps(normalized["tx_outputs"][0], indent=2))


def main():
    parser = argparse.ArgumentParser(description="Bitcoin Real-Time Normalizer")
    parser.add_argument("--rpc-url", default="http://127.0.0.1:8332",
                        help="Bitcoin Core RPC URL")
    parser.add_argument("--rpc-user", default="bitcoinrpc")
    parser.add_argument("--rpc-password", default="changeme_strong_password_here")
    parser.add_argument("--kafka-bootstrap", default="localhost:9092",
                        help="Kafka bootstrap servers")
    parser.add_argument("--zmq-url", default="tcp://127.0.0.1:28332",
                        help="Bitcoin Core ZMQ hashblock endpoint")
    parser.add_argument("--catchup", action="store_true",
                        help="Catch up from checkpoint to tip before live mode")
    parser.add_argument("--test-block", type=int, default=None,
                        help="Normalize a single block height and print (no Kafka)")
    parser.add_argument("--overlap", type=int, default=10,
                        help="Number of blocks to overlap during catch-up")

    args = parser.parse_args()

    rpc = BitcoinRPC(args.rpc_url, args.rpc_user, args.rpc_password)

    # Quick test mode — no Kafka needed
    if args.test_block is not None:
        test_block(rpc, args.test_block)
        return

    # Full mode — needs Kafka and ZMQ
    _ensure_kafka()
    _ensure_zmq()

    producer = BlockKafkaProducer(args.kafka_bootstrap)
    checkpoint = CheckpointStore()

    try:
        if args.catchup:
            catchup(rpc, producer, checkpoint, overlap=args.overlap)

        live_loop(rpc, producer, checkpoint, args.zmq_url)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
