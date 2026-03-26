"""Unit tests for the Kafka producer.

Tests cover:
  - Topic routing (4 topics)
  - Key construction for each record type
  - Message serialization (JSON + UTF-8)
  - Delivery failure handling
  - Flush behavior
"""

import sys
import os
import json
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "live-normalizer"))


class TestBlockKafkaProducer:
    """Test Kafka producer message routing and serialization."""

    @pytest.fixture
    def mock_confluent_producer(self, mocker):
        """Mock confluent_kafka.Producer."""
        mock_producer_cls = mocker.patch("kafka_producer.Producer")
        mock_instance = mocker.Mock()
        mock_producer_cls.return_value = mock_instance
        mock_instance.flush = mocker.Mock()
        return mock_instance

    @pytest.fixture
    def producer(self, mock_confluent_producer):
        from kafka_producer import BlockKafkaProducer
        return BlockKafkaProducer("localhost:9092")

    @pytest.fixture
    def sample_normalized(self):
        """Minimal normalized block output."""
        return {
            "block": {
                "block_hash": "hash_abc",
                "height": 100,
                "version": 1,
                "blocksize": 1000,
                "hashPrev": "hash_prev",
                "hashMerkleRoot": "merkle",
                "nTime": 1700000000,
                "nBits": 486604799,
                "nNonce": 1234,
                "finality_status": "OBSERVED",
            },
            "transactions": [
                {"txid": "tx1", "hashBlock": "hash_abc", "version": 1, "lockTime": 0},
                {"txid": "tx2", "hashBlock": "hash_abc", "version": 2, "lockTime": 100},
            ],
            "tx_inputs": [
                {"txid": "tx1", "hashPrevOut": "0" * 64, "indexPrevOut": 4294967295,
                 "scriptSig": "coinbase_sig", "sequence": 4294967295},
                {"txid": "tx2", "hashPrevOut": "prev_hash", "indexPrevOut": 0,
                 "scriptSig": "sig_hex", "sequence": 4294967294},
            ],
            "tx_outputs": [
                {"txid": "tx1", "indexOut": 0, "height": 100, "value": 5000000000,
                 "scriptPubKey": "pk1", "address": "addr1"},
                {"txid": "tx2", "indexOut": 0, "height": 100, "value": 1000000,
                 "scriptPubKey": "pk2", "address": "addr2"},
                {"txid": "tx2", "indexOut": 1, "height": 100, "value": 500000,
                 "scriptPubKey": "pk3", "address": ""},
            ],
        }

    def test_publish_block_returns_total_count(self, producer, sample_normalized,
                                                mock_confluent_producer):
        count = producer.publish_block(sample_normalized)
        # 1 block + 2 txs + 2 inputs + 3 outputs = 8
        assert count == 8

    def test_block_topic_and_key(self, producer, sample_normalized,
                                  mock_confluent_producer):
        producer.publish_block(sample_normalized)

        calls = mock_confluent_producer.produce.call_args_list
        block_call = calls[0]
        assert block_call.kwargs["topic"] == "btc.blocks.v1"
        assert block_call.kwargs["key"] == b"hash_abc"

    def test_transaction_topic_and_keys(self, producer, sample_normalized,
                                         mock_confluent_producer):
        producer.publish_block(sample_normalized)

        calls = mock_confluent_producer.produce.call_args_list
        # calls[1] and [2] are transactions
        tx_calls = calls[1:3]
        for call in tx_calls:
            assert call.kwargs["topic"] == "btc.transactions.v1"

        tx_keys = [call.kwargs["key"] for call in tx_calls]
        assert b"tx1" in tx_keys
        assert b"tx2" in tx_keys

    def test_input_key_format(self, producer, sample_normalized,
                               mock_confluent_producer):
        """Input keys should be txid:hashPrevOut:indexPrevOut."""
        producer.publish_block(sample_normalized)

        calls = mock_confluent_producer.produce.call_args_list
        # calls[3] and [4] are inputs
        input_calls = [c for c in calls if c.kwargs["topic"] == "btc.tx_inputs.v1"]
        assert len(input_calls) == 2

        keys = [c.kwargs["key"].decode() for c in input_calls]
        assert f"tx1:{'0' * 64}:4294967295" in keys
        assert "tx2:prev_hash:0" in keys

    def test_output_key_format(self, producer, sample_normalized,
                                mock_confluent_producer):
        """Output keys should be txid:indexOut."""
        producer.publish_block(sample_normalized)

        calls = mock_confluent_producer.produce.call_args_list
        output_calls = [c for c in calls if c.kwargs["topic"] == "btc.tx_outputs.v1"]
        assert len(output_calls) == 3

        keys = [c.kwargs["key"].decode() for c in output_calls]
        assert "tx1:0" in keys
        assert "tx2:0" in keys
        assert "tx2:1" in keys

    def test_values_are_json_encoded(self, producer, sample_normalized,
                                      mock_confluent_producer):
        producer.publish_block(sample_normalized)

        for call in mock_confluent_producer.produce.call_args_list:
            value_bytes = call.kwargs["value"]
            parsed = json.loads(value_bytes.decode("utf-8"))
            assert isinstance(parsed, dict)

    def test_flush_called_after_publish(self, producer, sample_normalized,
                                         mock_confluent_producer):
        producer.publish_block(sample_normalized)
        mock_confluent_producer.flush.assert_called_once_with(timeout=30)

    def test_delivery_failure_raises(self, producer, sample_normalized,
                                      mock_confluent_producer, mocker):
        """If any delivery callback reports failure, publish_block should raise."""
        mock_msg = mocker.Mock()
        mock_msg.topic.return_value = "btc.blocks.v1"
        mock_msg.key.return_value = b"hash_abc"

        def produce_with_error(**kwargs):
            if kwargs.get("callback"):
                err = Exception("broker unavailable")
                kwargs["callback"](err, mock_msg)

        mock_confluent_producer.produce.side_effect = produce_with_error
        with pytest.raises(RuntimeError, match="Failed to deliver"):
            producer.publish_block(sample_normalized)

    def test_idempotent_producer_config(self, mocker):
        """Producer should be configured with enable.idempotence=True."""
        mock_producer_cls = mocker.patch("kafka_producer.Producer")
        mock_producer_cls.return_value = mocker.Mock()

        from kafka_producer import BlockKafkaProducer
        BlockKafkaProducer("localhost:9092")

        config = mock_producer_cls.call_args[0][0]
        assert config["enable.idempotence"] is True
        assert config["acks"] == "all"
