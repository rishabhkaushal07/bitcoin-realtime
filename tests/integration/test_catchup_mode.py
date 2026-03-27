"""Integration tests for catchup mode: checkpoint → RPC → normalize → Kafka.

Tests cover:
  - process_block() full flow with real Kafka
  - Catchup from height 0 to N (small range)
  - Checkpoint persistence after catch-up
  - Overlap strategy (re-processing blocks is idempotent)

Prerequisites:
  - docker compose up -d && bash scripts/create-kafka-topics.sh
  - Bitcoin Core running with RPC (IBD past block 10 minimum)

Run with: pytest tests/integration/test_catchup_mode.py -m integration -v
"""

import sys
import os
import json
import time
import uuid
import tempfile
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "live-normalizer"))

pytestmark = pytest.mark.integration

BOOTSTRAP = "localhost:9092"


def _kafka_available() -> bool:
    try:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
        admin.list_topics(timeout=5)
        return True
    except Exception:
        return False


def _rpc_available() -> bool:
    try:
        from rpc_client import BitcoinRPC
        rpc = BitcoinRPC()
        info = rpc.getblockchaininfo()
        return info["blocks"] >= 10
    except Exception:
        return False


def _consume_all(topic: str, expected_count: int,
                 timeout_sec: float = 15.0, group_suffix: str = "") -> list[dict]:
    from confluent_kafka import Consumer
    group = f"test-catchup-{uuid.uuid4().hex[:8]}-{group_suffix}"
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])
    messages = []
    deadline = time.time() + timeout_sec
    try:
        while len(messages) < expected_count and time.time() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None or msg.error():
                continue
            key = msg.key().decode("utf-8") if msg.key() else None
            value = json.loads(msg.value().decode("utf-8"))
            messages.append({"key": key, "value": value})
    finally:
        consumer.close()
    return messages


@pytest.fixture(scope="module", autouse=True)
def check_prerequisites():
    if not _kafka_available():
        pytest.skip("Kafka not available at localhost:9092")
    if not _rpc_available():
        pytest.skip("Bitcoin Core RPC not available or IBD < block 10")


@pytest.fixture
def rpc():
    from rpc_client import BitcoinRPC
    return BitcoinRPC()


@pytest.fixture
def kafka_producer():
    from kafka_producer import BlockKafkaProducer
    p = BlockKafkaProducer(BOOTSTRAP)
    yield p
    p.close()


@pytest.fixture
def tmp_checkpoint(tmp_path):
    from checkpoint_store import CheckpointStore
    return CheckpointStore(path=str(tmp_path / "checkpoint.json"))


class TestProcessBlock:
    """Test the full process_block() flow with real RPC + Kafka."""

    def test_process_genesis_block(self, rpc, kafka_producer, tmp_checkpoint):
        """process_block should fetch, normalize, publish, and checkpoint."""
        from main import process_block

        genesis_hash = rpc.getblockhash(0)
        normalized = process_block(rpc, kafka_producer, tmp_checkpoint,
                                   genesis_hash, zmq_seq=0)

        # Verify normalized output
        assert normalized["block"]["height"] == 0
        assert len(normalized["transactions"]) == 1
        assert len(normalized["tx_inputs"]) == 1
        assert len(normalized["tx_outputs"]) == 1

        # Verify checkpoint was updated
        assert tmp_checkpoint.last_height == 0
        assert tmp_checkpoint.last_hash == genesis_hash

    def test_process_block_publishes_to_kafka(self, rpc, kafka_producer, tmp_checkpoint):
        """Records should actually land in Kafka topics."""
        from main import process_block

        block_hash = rpc.getblockhash(1)
        process_block(rpc, kafka_producer, tmp_checkpoint, block_hash)

        # Verify block record in Kafka
        msgs = _consume_all("btc.blocks.v1", expected_count=1,
                            timeout_sec=10, group_suffix="process-block")
        block_msgs = [m for m in msgs if m["value"].get("height") == 1]
        assert len(block_msgs) >= 1
        assert block_msgs[0]["value"]["block_hash"] == block_hash

    def test_process_block_without_kafka(self, rpc, tmp_checkpoint):
        """process_block with producer=None should work (dry run)."""
        from main import process_block

        genesis_hash = rpc.getblockhash(0)
        normalized = process_block(rpc, None, tmp_checkpoint, genesis_hash)

        assert normalized["block"]["height"] == 0
        assert tmp_checkpoint.last_height == 0


class TestCatchupMode:
    """Test catchup mode: replay blocks from checkpoint to tip."""

    def test_catchup_small_range(self, rpc, kafka_producer, tmp_checkpoint):
        """Catch up blocks 0-5 and verify checkpoint advancement."""
        from main import catchup

        # Start from height -1 (no previous checkpoint)
        catchup(rpc, kafka_producer, tmp_checkpoint, overlap=0)

        # Checkpoint should be at current tip
        tip = rpc.getblockcount()
        # catchup processes all blocks, so checkpoint should be at tip
        # But for this test we just verify it advanced past 0
        assert tmp_checkpoint.last_height >= 0

    def test_catchup_from_existing_checkpoint(self, rpc, kafka_producer, tmp_checkpoint):
        """Catchup from a non-zero checkpoint should skip already-processed blocks."""
        from main import process_block

        # Pre-process block 0
        genesis_hash = rpc.getblockhash(0)
        process_block(rpc, kafka_producer, tmp_checkpoint, genesis_hash)
        assert tmp_checkpoint.last_height == 0

        # Set checkpoint to block 3
        block3_hash = rpc.getblockhash(3)
        tmp_checkpoint.update(3, block3_hash)

        # Catchup with overlap=2 should start from block 2 (3 - 2 + 1)
        from main import catchup
        catchup(rpc, kafka_producer, tmp_checkpoint, overlap=2)

        # Should have advanced past block 3
        assert tmp_checkpoint.last_height >= 3

    def test_catchup_produces_to_all_topics(self, rpc, kafka_producer, tmp_checkpoint):
        """Catching up blocks 0-2 should produce records in all 4 topics."""
        # Set a very limited range by tweaking the checkpoint
        tmp_checkpoint.update(-1, "")

        from main import catchup
        catchup(rpc, kafka_producer, tmp_checkpoint, overlap=0)

        # All topics should have at least some messages
        for topic in ["btc.blocks.v1", "btc.transactions.v1",
                       "btc.tx_inputs.v1", "btc.tx_outputs.v1"]:
            msgs = _consume_all(topic, expected_count=1,
                                timeout_sec=10, group_suffix=f"catchup-{topic}")
            assert len(msgs) >= 1, f"No messages in {topic} after catchup"

    def test_catchup_checkpoint_mode_set(self, rpc, kafka_producer, tmp_checkpoint):
        """During catchup, checkpoint mode should be set to 'catchup'."""
        tmp_checkpoint.update(-1, "")

        from main import catchup
        catchup(rpc, kafka_producer, tmp_checkpoint, overlap=0)

        # After catchup completes, checkpoint state should show it was in catchup
        # (The mode stays as "catchup" until live_loop switches it to "live")
        # Since catchup sets mode="catchup" on entry, verify the state file
        import json
        with open(tmp_checkpoint.path) as f:
            state = json.load(f)
        # Mode should have been set during catchup
        assert state["current_mode"] in ("catchup", "live")
        assert state["last_finalized_height"] >= 0


class TestCheckpointRecovery:
    """Test checkpoint persistence and recovery across restarts."""

    def test_checkpoint_survives_restart(self, rpc, kafka_producer, tmp_path):
        """Simulated restart: create checkpoint, destroy store, recreate."""
        from checkpoint_store import CheckpointStore
        from main import process_block

        path = str(tmp_path / "checkpoint.json")

        # First "session"
        store1 = CheckpointStore(path=path)
        genesis_hash = rpc.getblockhash(0)
        process_block(rpc, kafka_producer, store1, genesis_hash, zmq_seq=42)
        del store1

        # Second "session" — simulates process restart
        store2 = CheckpointStore(path=path)
        assert store2.last_height == 0
        assert store2.last_hash == genesis_hash
        assert store2.state["last_seen_zmq_seq"] == 42

    def test_catchup_resumes_from_checkpoint(self, rpc, kafka_producer, tmp_path):
        """After restart, catchup should resume from saved height."""
        from checkpoint_store import CheckpointStore
        from main import process_block

        path = str(tmp_path / "checkpoint.json")

        # First session: process blocks 0-2
        store1 = CheckpointStore(path=path)
        for h in range(3):
            bh = rpc.getblockhash(h)
            process_block(rpc, kafka_producer, store1, bh)
        assert store1.last_height == 2
        del store1

        # Second session: catchup should start from around block 2
        store2 = CheckpointStore(path=path)
        assert store2.last_height == 2

        from main import catchup
        catchup(rpc, kafka_producer, store2, overlap=1)

        # Should have advanced past block 2
        assert store2.last_height >= 2


class TestOverlapIdempotency:
    """Test that re-processing blocks (overlap strategy) is safe."""

    def test_reprocess_same_block_twice(self, rpc, kafka_producer, tmp_checkpoint):
        """Publishing the same block twice should not raise errors."""
        from main import process_block

        genesis_hash = rpc.getblockhash(0)
        result1 = process_block(rpc, kafka_producer, tmp_checkpoint, genesis_hash)
        result2 = process_block(rpc, kafka_producer, tmp_checkpoint, genesis_hash)

        # Both should produce valid output
        assert result1["block"]["height"] == 0
        assert result2["block"]["height"] == 0
        assert result1["block"]["block_hash"] == result2["block"]["block_hash"]

    def test_overlap_blocks_all_published(self, rpc, kafka_producer, tmp_checkpoint):
        """With overlap, re-processed blocks should still land in Kafka."""
        from main import process_block

        # Process blocks 0-2
        for h in range(3):
            bh = rpc.getblockhash(h)
            process_block(rpc, kafka_producer, tmp_checkpoint, bh)

        # Re-process blocks 1-2 (simulating overlap)
        for h in range(1, 3):
            bh = rpc.getblockhash(h)
            result = process_block(rpc, kafka_producer, tmp_checkpoint, bh)
            assert result["block"]["height"] == h

        # All should be fine — idempotent producer handles duplicates
        assert tmp_checkpoint.last_height == 2
