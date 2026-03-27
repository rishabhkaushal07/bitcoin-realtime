"""Unit tests for the Iceberg writer service.

Tests cover:
  - Message parsing (JSON to dict, schema_version removal)
  - Buffer management (per-topic buffering, flush triggers)
  - Topic-to-table mapping
  - Flush logic (batch size trigger, time-based trigger)
  - Error handling on flush failures
"""

import sys
import os
import json
import time
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "pyiceberg-sidecar"))

from iceberg_writer import IcebergWriter, TOPIC_TABLE_MAP


@pytest.fixture
def writer():
    """Create an IcebergWriter with small batch for testing."""
    return IcebergWriter(batch_size=3, flush_interval_sec=1.0)


class TestTopicTableMapping:
    """Verify topic → Iceberg table mapping."""

    def test_all_four_topics_mapped(self):
        assert len(TOPIC_TABLE_MAP) == 4

    def test_blocks_topic_maps_to_blocks_table(self):
        assert TOPIC_TABLE_MAP["btc.blocks.v1"] == "btc.blocks"

    def test_transactions_topic_maps(self):
        assert TOPIC_TABLE_MAP["btc.transactions.v1"] == "btc.transactions"

    def test_inputs_topic_maps(self):
        assert TOPIC_TABLE_MAP["btc.tx_inputs.v1"] == "btc.tx_in"

    def test_outputs_topic_maps(self):
        assert TOPIC_TABLE_MAP["btc.tx_outputs.v1"] == "btc.tx_out"


class TestMessageParsing:
    """Test _parse_message strips schema_version and parses JSON."""

    def test_parse_valid_json(self, writer):
        msg = json.dumps({"block_hash": "abc", "height": 1, "schema_version": 1})
        result = writer._parse_message("btc.blocks.v1", msg.encode("utf-8"))
        assert result["block_hash"] == "abc"
        assert result["height"] == 1

    def test_schema_version_removed(self, writer):
        msg = json.dumps({"txid": "def", "schema_version": 1})
        result = writer._parse_message("btc.transactions.v1", msg.encode("utf-8"))
        assert "schema_version" not in result

    def test_missing_schema_version_ok(self, writer):
        msg = json.dumps({"txid": "def"})
        result = writer._parse_message("btc.transactions.v1", msg.encode("utf-8"))
        assert result["txid"] == "def"


class TestBufferManagement:
    """Test per-topic buffering and flush triggers."""

    def test_initial_buffers_empty(self, writer):
        for topic in TOPIC_TABLE_MAP:
            assert len(writer._buffers[topic]) == 0

    def test_buffer_accumulates_records(self, writer):
        writer._buffers["btc.blocks.v1"].append({"block_hash": "a"})
        writer._buffers["btc.blocks.v1"].append({"block_hash": "b"})
        assert len(writer._buffers["btc.blocks.v1"]) == 2

    def test_should_flush_batch_size(self, writer):
        """Flush when any buffer reaches batch_size."""
        writer._buffers["btc.blocks.v1"] = [{"x": i} for i in range(3)]
        assert writer._should_flush() is True

    def test_should_not_flush_below_batch(self, writer):
        writer._buffers["btc.blocks.v1"] = [{"x": i} for i in range(2)]
        writer._last_flush = time.time()  # Reset timer
        assert writer._should_flush() is False

    def test_should_flush_time_interval(self, writer):
        """Flush after flush_interval even with empty buffers."""
        writer._last_flush = time.time() - 2.0  # 2 sec ago, interval is 1 sec
        assert writer._should_flush() is True

    def test_should_not_flush_within_interval(self, writer):
        writer._last_flush = time.time()
        assert writer._should_flush() is False


class TestFlushLogic:
    """Test flush_topic and flush_all behavior."""

    def test_flush_empty_buffer_returns_zero(self, writer):
        """Flushing empty buffer should return 0 and not call Iceberg."""
        count = writer.flush_topic("btc.blocks.v1")
        assert count == 0

    @patch.object(IcebergWriter, "_records_to_arrow")
    def test_flush_topic_clears_buffer(self, mock_to_arrow, writer):
        """After flush, buffer should be empty."""
        mock_table = MagicMock()
        writer._tables["btc.blocks.v1"] = mock_table
        mock_to_arrow.return_value = MagicMock()

        writer._buffers["btc.blocks.v1"] = [{"block_hash": "a"}]
        count = writer.flush_topic("btc.blocks.v1")

        assert count == 1
        assert len(writer._buffers["btc.blocks.v1"]) == 0
        mock_table.append.assert_called_once()

    @patch.object(IcebergWriter, "_records_to_arrow")
    def test_flush_topic_raises_on_error(self, mock_to_arrow, writer):
        """Flush should propagate Iceberg write errors."""
        mock_table = MagicMock()
        mock_table.append.side_effect = Exception("S3 write failed")
        writer._tables["btc.blocks.v1"] = mock_table
        mock_to_arrow.return_value = MagicMock()

        writer._buffers["btc.blocks.v1"] = [{"block_hash": "a"}]
        with pytest.raises(Exception, match="S3 write failed"):
            writer.flush_topic("btc.blocks.v1")

    @patch.object(IcebergWriter, "flush_topic")
    def test_flush_all_calls_all_topics(self, mock_flush, writer):
        """flush_all should call flush_topic for each topic."""
        mock_flush.return_value = 0
        writer._consumer = MagicMock()

        writer.flush_all()
        assert mock_flush.call_count == len(TOPIC_TABLE_MAP)

    @patch.object(IcebergWriter, "flush_topic")
    def test_flush_all_commits_offsets(self, mock_flush, writer):
        """flush_all should commit Kafka offsets after flushing."""
        mock_flush.side_effect = [5, 0, 0, 0]
        writer._consumer = MagicMock()

        total = writer.flush_all()
        assert total == 5
        writer._consumer.commit.assert_called_once()

    @patch.object(IcebergWriter, "flush_topic")
    def test_flush_all_no_commit_if_empty(self, mock_flush, writer):
        """flush_all should not commit if nothing was flushed."""
        mock_flush.return_value = 0
        writer._consumer = MagicMock()

        total = writer.flush_all()
        assert total == 0
        writer._consumer.commit.assert_not_called()


class TestStopBehavior:
    """Test graceful shutdown."""

    def test_stop_sets_running_false(self, writer):
        writer._running = True
        writer.stop()
        assert writer._running is False
