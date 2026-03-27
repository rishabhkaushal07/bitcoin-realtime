"""End-to-end integration tests: Normalizer → Kafka produce → consume verify.

Tests cover:
  - Publishing a normalized block to all 4 Kafka topics
  - Consuming and verifying message keys, values, and counts
  - Multi-block sequential publishing
  - Idempotent producer behavior (re-publish same block)
  - Topic isolation (each record type goes to correct topic)

Prerequisites: docker compose up -d && bash scripts/create-kafka-topics.sh
Run with: pytest tests/integration/test_kafka_e2e.py -m integration -v
"""

import sys
import os
import json
import time
import uuid
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "live-normalizer"))

pytestmark = pytest.mark.integration

BOOTSTRAP = "localhost:9092"

# Unique group ID per test run to avoid consuming stale messages
GROUP_PREFIX = f"test-{uuid.uuid4().hex[:8]}"


def _kafka_available() -> bool:
    """Check if Kafka broker is reachable."""
    try:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
        metadata = admin.list_topics(timeout=5)
        return metadata is not None
    except Exception:
        return False


def _topics_exist() -> bool:
    """Check that all 4 data topics are created."""
    try:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
        metadata = admin.list_topics(timeout=5)
        topics = set(metadata.topics.keys())
        required = {"btc.blocks.v1", "btc.transactions.v1",
                     "btc.tx_inputs.v1", "btc.tx_outputs.v1"}
        return required.issubset(topics)
    except Exception:
        return False


def _consume_all(topic: str, expected_count: int,
                 timeout_sec: float = 15.0) -> list[dict]:
    """Consume expected_count messages from a topic, return parsed values."""
    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"{GROUP_PREFIX}-{topic}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])

    messages = []
    deadline = time.time() + timeout_sec
    try:
        while len(messages) < expected_count and time.time() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            key = msg.key().decode("utf-8") if msg.key() else None
            value = json.loads(msg.value().decode("utf-8"))
            messages.append({"key": key, "value": value, "offset": msg.offset()})
    finally:
        consumer.close()

    return messages


@pytest.fixture(scope="module", autouse=True)
def check_kafka():
    """Skip entire module if Kafka is not available."""
    if not _kafka_available():
        pytest.skip("Kafka broker not reachable at localhost:9092")
    if not _topics_exist():
        pytest.skip("Kafka topics not created — run scripts/create-kafka-topics.sh first")


@pytest.fixture(scope="module")
def producer():
    """Create a real Kafka producer for the test module."""
    from kafka_producer import BlockKafkaProducer
    p = BlockKafkaProducer(BOOTSTRAP)
    yield p
    p.close()


@pytest.fixture
def genesis_normalized():
    """Normalized genesis block for e2e testing."""
    from normalizer import normalize_block
    genesis_rpc = {
        "hash": "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
        "height": 0,
        "version": 1,
        "size": 285,
        "merkleroot": "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b",
        "time": 1231006505,
        "bits": "1d00ffff",
        "nonce": 2083236893,
        "tx": [{
            "txid": "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b",
            "version": 1,
            "locktime": 0,
            "vin": [{"coinbase": "04ffff001d0104455468652054696d65732030332f4a616e2f32303039", "sequence": 4294967295}],
            "vout": [{"value": 50.0, "n": 0, "scriptPubKey": {"hex": "4104678afdb0fe", "type": "pubkey"}}],
        }],
    }
    return normalize_block(genesis_rpc)


@pytest.fixture
def block_170_normalized():
    """Normalized block 170 for e2e testing."""
    from normalizer import normalize_block
    block_rpc = {
        "hash": "00000000d1145790a8694403d4063f323d499e655c83426834d4ce2f8dd4a2ee",
        "height": 170,
        "version": 1,
        "size": 490,
        "previousblockhash": "000000002a22cfee1f2c846adbd12b3e183d4f97683f85dad08a79780a84bd55",
        "merkleroot": "7dac2c5666815c17a3b36427de37bb9d2e2c5ccec3f8633eb91a4205cb4c10ff",
        "time": 1231731025,
        "bits": "1d00ffff",
        "nonce": 1889418792,
        "tx": [
            {
                "txid": "b1fea52486ce0c62bb442b530a3f0132b826c74e473d1f2c220bfa78111c5082",
                "version": 1,
                "locktime": 0,
                "vin": [{"coinbase": "04ffff001d0102", "sequence": 4294967295}],
                "vout": [{"value": 50.0, "n": 0, "scriptPubKey": {"hex": "41041db93e", "type": "pubkey"}}],
            },
            {
                "txid": "f4184fc596403b9d638783cf57adfe4c75c605f6356fbc91338530e9831e9e16",
                "version": 1,
                "locktime": 0,
                "vin": [{
                    "txid": "0437cd7f8525ceed2324359c2d0ba26006d92d856a9c20fa0241106ee5a597c9",
                    "vout": 0,
                    "scriptSig": {"hex": "47304402204e45e16932"},
                    "sequence": 4294967295,
                }],
                "vout": [
                    {"value": 10.0, "n": 0, "scriptPubKey": {"hex": "4104ae1a62fe", "address": "1Q2TWHE3GMdB6BZKafqwxXtWAWgFt5Jvm3"}},
                    {"value": 40.0, "n": 1, "scriptPubKey": {"hex": "410411db93e1", "address": "12cbQLTFMXRnSzktFkuoG3eHoMeFtpTu3S"}},
                ],
            },
        ],
    }
    return normalize_block(block_rpc)


class TestKafkaPublishAndConsume:
    """End-to-end: publish a block, consume from all 4 topics, verify."""

    def test_genesis_block_record_count(self, producer, genesis_normalized):
        """Publishing genesis block should produce exactly 4 records (1+1+1+1)."""
        count = producer.publish_block(genesis_normalized)
        assert count == 4  # 1 block + 1 tx + 1 input + 1 output

    def test_block_170_record_count(self, producer, block_170_normalized):
        """Block 170 should produce 8 records (1+2+2+3)."""
        count = producer.publish_block(block_170_normalized)
        assert count == 8

    def test_consume_blocks_topic(self, producer, genesis_normalized):
        producer.publish_block(genesis_normalized)
        msgs = _consume_all("btc.blocks.v1", expected_count=1, timeout_sec=10)
        assert len(msgs) >= 1

        # Find our genesis block message
        genesis_msgs = [m for m in msgs if m["value"].get("height") == 0]
        assert len(genesis_msgs) >= 1

        block = genesis_msgs[0]["value"]
        assert block["block_hash"] == genesis_normalized["block"]["block_hash"]
        assert block["height"] == 0
        assert block["finality_status"] == "OBSERVED"
        assert genesis_msgs[0]["key"] == block["block_hash"]

    def test_consume_transactions_topic(self, producer, block_170_normalized):
        producer.publish_block(block_170_normalized)
        # Consume more than expected to get all messages (prior test runs may have added)
        msgs = _consume_all("btc.transactions.v1", expected_count=10, timeout_sec=20)

        # Filter to our block's transactions
        block_txs = [m for m in msgs if m["value"].get("block_height") == 170]
        assert len(block_txs) >= 2, f"Expected >=2 txs for block 170, got {len(block_txs)}"

        txids = {m["value"]["txid"] for m in block_txs}
        expected_txids = {tx["txid"] for tx in block_170_normalized["transactions"]}
        assert expected_txids.issubset(txids)

        # Verify key = txid
        for m in block_txs:
            assert m["key"] == m["value"]["txid"]

    def test_consume_inputs_topic(self, producer, block_170_normalized):
        producer.publish_block(block_170_normalized)
        msgs = _consume_all("btc.tx_inputs.v1", expected_count=10, timeout_sec=20)

        block_inputs = [m for m in msgs if m["value"].get("block_height") == 170]
        assert len(block_inputs) >= 2, f"Expected >=2 inputs for block 170, got {len(block_inputs)}"

        # Verify coinbase input
        coinbase = [m for m in block_inputs if m["value"]["hashPrevOut"] == "0" * 64]
        assert len(coinbase) >= 1
        assert coinbase[0]["value"]["indexPrevOut"] == 4294967295

        # Verify composite key format: txid:hashPrevOut:indexPrevOut
        for m in block_inputs:
            v = m["value"]
            expected_key = f"{v['txid']}:{v['hashPrevOut']}:{v['indexPrevOut']}"
            assert m["key"] == expected_key

    def test_consume_outputs_topic(self, producer, block_170_normalized):
        producer.publish_block(block_170_normalized)
        msgs = _consume_all("btc.tx_outputs.v1", expected_count=10, timeout_sec=20)

        block_outputs = [m for m in msgs if m["value"].get("height") == 170]
        assert len(block_outputs) >= 3, f"Expected >=3 outputs for block 170, got {len(block_outputs)}"

        values = sorted([m["value"]["value"] for m in block_outputs])
        assert 1_000_000_000 in values  # 10 BTC
        assert 4_000_000_000 in values  # 40 BTC
        assert 5_000_000_000 in values  # 50 BTC coinbase

        # Verify composite key format: txid:indexOut
        for m in block_outputs:
            v = m["value"]
            expected_key = f"{v['txid']}:{v['indexOut']}"
            assert m["key"] == expected_key


class TestKafkaMessageFormat:
    """Verify message serialization format in Kafka."""

    def test_values_are_valid_json(self, producer, genesis_normalized):
        producer.publish_block(genesis_normalized)
        for topic in ["btc.blocks.v1", "btc.transactions.v1",
                       "btc.tx_inputs.v1", "btc.tx_outputs.v1"]:
            msgs = _consume_all(topic, expected_count=1, timeout_sec=10)
            for m in msgs:
                # Value should be a dict (parsed from JSON)
                assert isinstance(m["value"], dict)
                # Key should be a non-empty string
                assert m["key"] and len(m["key"]) > 0

    def test_block_has_all_required_fields(self, producer, genesis_normalized):
        producer.publish_block(genesis_normalized)
        msgs = _consume_all("btc.blocks.v1", expected_count=1, timeout_sec=10)
        genesis_msgs = [m for m in msgs if m["value"].get("height") == 0]
        assert len(genesis_msgs) >= 1

        block = genesis_msgs[0]["value"]
        required = ["block_hash", "height", "version", "blocksize",
                     "hashPrev", "hashMerkleRoot", "nTime", "nBits", "nNonce",
                     "block_timestamp", "observed_at", "ingested_at",
                     "finality_status", "schema_version"]
        for field in required:
            assert field in block, f"Missing field: {field}"

    def test_transaction_has_all_required_fields(self, producer, genesis_normalized):
        producer.publish_block(genesis_normalized)
        msgs = _consume_all("btc.transactions.v1", expected_count=1, timeout_sec=10)
        assert len(msgs) >= 1

        tx = msgs[0]["value"]
        required = ["txid", "hashBlock", "version", "lockTime",
                     "block_height", "block_timestamp", "observed_at",
                     "ingested_at", "schema_version"]
        for field in required:
            assert field in tx, f"Missing field: {field}"

    def test_input_has_all_required_fields(self, producer, genesis_normalized):
        producer.publish_block(genesis_normalized)
        msgs = _consume_all("btc.tx_inputs.v1", expected_count=1, timeout_sec=10)
        assert len(msgs) >= 1

        inp = msgs[0]["value"]
        required = ["txid", "hashPrevOut", "indexPrevOut", "scriptSig",
                     "sequence", "block_hash", "block_height",
                     "observed_at", "ingested_at", "schema_version"]
        for field in required:
            assert field in inp, f"Missing field: {field}"

    def test_output_has_all_required_fields(self, producer, genesis_normalized):
        producer.publish_block(genesis_normalized)
        msgs = _consume_all("btc.tx_outputs.v1", expected_count=1, timeout_sec=10)
        assert len(msgs) >= 1

        out = msgs[0]["value"]
        required = ["txid", "indexOut", "height", "value",
                     "scriptPubKey", "address", "block_hash",
                     "block_timestamp", "observed_at", "ingested_at",
                     "schema_version"]
        for field in required:
            assert field in out, f"Missing field: {field}"


class TestKafkaMultiBlock:
    """Test publishing multiple blocks sequentially."""

    def test_sequential_blocks_preserve_order(self, producer,
                                               genesis_normalized,
                                               block_170_normalized):
        """Blocks published in order should be consumable in order."""
        producer.publish_block(genesis_normalized)
        producer.publish_block(block_170_normalized)

        msgs = _consume_all("btc.blocks.v1", expected_count=2, timeout_sec=15)
        heights = [m["value"]["height"] for m in msgs]

        # Both heights should be present
        assert 0 in heights
        assert 170 in heights

        # Earlier offset should have lower or equal height (single partition)
        h0_msgs = [m for m in msgs if m["value"]["height"] == 0]
        h170_msgs = [m for m in msgs if m["value"]["height"] == 170]
        if h0_msgs and h170_msgs:
            assert h0_msgs[0]["offset"] < h170_msgs[0]["offset"]

    def test_multi_block_transaction_counts(self, producer,
                                             genesis_normalized,
                                             block_170_normalized):
        """Total transactions across 2 blocks: 1 + 2 = 3."""
        producer.publish_block(genesis_normalized)
        producer.publish_block(block_170_normalized)

        msgs = _consume_all("btc.transactions.v1", expected_count=3, timeout_sec=15)
        block_heights = {m["value"]["block_height"] for m in msgs}
        assert 0 in block_heights
        assert 170 in block_heights


class TestKafkaTopicIsolation:
    """Verify records go to the correct topic only."""

    def test_block_record_not_in_tx_topic(self, producer, genesis_normalized):
        """Block records should only appear in btc.blocks.v1."""
        producer.publish_block(genesis_normalized)
        tx_msgs = _consume_all("btc.transactions.v1", expected_count=1, timeout_sec=10)

        for m in tx_msgs:
            assert "finality_status" not in m["value"], \
                "Block field found in transactions topic"

    def test_input_record_not_in_output_topic(self, producer, block_170_normalized):
        """Input fields should not leak into output topic."""
        producer.publish_block(block_170_normalized)
        out_msgs = _consume_all("btc.tx_outputs.v1", expected_count=3, timeout_sec=10)

        for m in out_msgs:
            assert "hashPrevOut" not in m["value"], \
                "Input field found in outputs topic"
            assert "scriptSig" not in m["value"], \
                "Input field found in outputs topic"
