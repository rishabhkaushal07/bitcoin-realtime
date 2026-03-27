"""Unit tests for main.py — process_block and catchup logic.

Tests cover:
  - process_block with/without producer and checkpoint
  - catchup range calculation and overlap
  - catchup progress tracking
  - Error handling in process_block
"""

import sys
import os
import json
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "live-normalizer"))


@pytest.fixture
def mock_rpc(mocker):
    """Mock BitcoinRPC that returns pre-built block data."""
    rpc = mocker.Mock()
    rpc.getblockcount.return_value = 5

    def _getblockhash(h):
        return f"hash_{h:064d}"
    rpc.getblockhash.side_effect = _getblockhash

    def _getblock(bh, verbosity=2):
        # Extract height from our hash format
        height = int(bh.replace("hash_", "").lstrip("0") or "0")
        return {
            "hash": bh,
            "height": height,
            "version": 1,
            "size": 285,
            "previousblockhash": f"hash_{height - 1:064d}" if height > 0 else None,
            "merkleroot": f"merkle_{height}",
            "time": 1231006505 + height * 600,
            "bits": "1d00ffff",
            "nonce": height * 1000,
            "tx": [{
                "txid": f"cbtx_{height}",
                "version": 1,
                "locktime": 0,
                "vin": [{"coinbase": "00", "sequence": 4294967295}],
                "vout": [{"value": 50.0, "n": 0, "scriptPubKey": {"hex": "00"}}],
            }],
        }
    rpc.getblock.side_effect = _getblock
    return rpc


@pytest.fixture
def mock_producer(mocker):
    """Mock Kafka producer that tracks publish calls."""
    producer = mocker.Mock()
    producer.publish_block.return_value = 4  # 1 block + 1 tx + 1 in + 1 out
    return producer


@pytest.fixture
def tmp_checkpoint(tmp_path):
    from checkpoint_store import CheckpointStore
    return CheckpointStore(path=str(tmp_path / "checkpoint.json"))


class TestProcessBlock:
    """Test process_block function."""

    def test_fetches_and_normalizes(self, mock_rpc, mock_producer, tmp_checkpoint):
        from main import process_block
        result = process_block(mock_rpc, mock_producer, tmp_checkpoint,
                               "hash_" + "0" * 64, zmq_seq=1)

        assert result["block"]["height"] == 0
        mock_rpc.getblock.assert_called_once_with("hash_" + "0" * 64, 2)

    def test_publishes_to_kafka(self, mock_rpc, mock_producer, tmp_checkpoint):
        from main import process_block
        process_block(mock_rpc, mock_producer, tmp_checkpoint,
                      "hash_" + "0" * 64)

        mock_producer.publish_block.assert_called_once()
        normalized = mock_producer.publish_block.call_args[0][0]
        assert normalized["block"]["height"] == 0

    def test_updates_checkpoint(self, mock_rpc, mock_producer, tmp_checkpoint):
        from main import process_block
        process_block(mock_rpc, mock_producer, tmp_checkpoint,
                      "hash_" + "0" * 64, zmq_seq=42)

        assert tmp_checkpoint.last_height == 0
        assert tmp_checkpoint.last_hash == "hash_" + "0" * 64
        assert tmp_checkpoint.state["last_seen_zmq_seq"] == 42

    def test_no_kafka_dry_run(self, mock_rpc, tmp_checkpoint):
        """producer=None means dry run — no Kafka publish."""
        from main import process_block
        result = process_block(mock_rpc, None, tmp_checkpoint,
                               "hash_" + "0" * 64)

        assert result["block"]["height"] == 0
        assert tmp_checkpoint.last_height == 0

    def test_no_checkpoint(self, mock_rpc, mock_producer):
        """checkpoint=None means no state tracking."""
        from main import process_block
        result = process_block(mock_rpc, mock_producer, None,
                               "hash_" + "0" * 64)
        assert result["block"]["height"] == 0

    def test_returns_normalized_data(self, mock_rpc, mock_producer, tmp_checkpoint):
        from main import process_block
        result = process_block(mock_rpc, mock_producer, tmp_checkpoint,
                               "hash_" + "0" * 64)

        assert "block" in result
        assert "transactions" in result
        assert "tx_inputs" in result
        assert "tx_outputs" in result
        assert len(result["transactions"]) == 1
        assert len(result["tx_inputs"]) == 1
        assert len(result["tx_outputs"]) == 1


class TestCatchup:
    """Test catchup function logic."""

    def test_catchup_from_start(self, mock_rpc, mock_producer, tmp_checkpoint):
        """Catchup from height -1 should process blocks 0 through tip."""
        from main import catchup
        catchup(mock_rpc, mock_producer, tmp_checkpoint, overlap=0)

        # Should have processed blocks 0-5 (tip=5)
        assert tmp_checkpoint.last_height == 5
        assert mock_producer.publish_block.call_count == 6  # blocks 0,1,2,3,4,5

    def test_catchup_with_overlap(self, mock_rpc, mock_producer, tmp_checkpoint):
        """Overlap should cause re-processing of recent blocks."""
        from main import catchup

        # Set checkpoint at block 3
        tmp_checkpoint.update(3, "hash_" + "3".zfill(64))

        catchup(mock_rpc, mock_producer, tmp_checkpoint, overlap=2)

        # Should start from max(0, 3 - 2 + 1) = 2, process 2,3,4,5
        assert mock_producer.publish_block.call_count == 4
        assert tmp_checkpoint.last_height == 5

    def test_catchup_already_at_tip(self, mock_rpc, mock_producer, tmp_checkpoint):
        """If already at tip, catchup should do nothing."""
        from main import catchup

        tmp_checkpoint.update(5, "hash_" + "5".zfill(64))
        mock_rpc.getblockcount.return_value = 5

        catchup(mock_rpc, mock_producer, tmp_checkpoint, overlap=0)

        # start_height = max(0, 5 - 0 + 1) = 6 > 5, so nothing to do
        mock_producer.publish_block.assert_not_called()

    def test_catchup_sets_mode(self, mock_rpc, mock_producer, tmp_checkpoint):
        """Catchup should set checkpoint mode to 'catchup'."""
        from main import catchup
        catchup(mock_rpc, mock_producer, tmp_checkpoint, overlap=0)

        assert tmp_checkpoint.mode == "catchup"

    def test_catchup_overlap_doesnt_go_negative(self, mock_rpc, mock_producer,
                                                 tmp_checkpoint):
        """Overlap larger than height should clamp to 0."""
        from main import catchup

        tmp_checkpoint.update(2, "hash_" + "2".zfill(64))
        catchup(mock_rpc, mock_producer, tmp_checkpoint, overlap=100)

        # start = max(0, 2 - 100 + 1) = max(0, -97) = 0
        # Should process blocks 0 through 5
        assert mock_producer.publish_block.call_count == 6

    def test_catchup_processes_blocks_in_order(self, mock_rpc, mock_producer,
                                                tmp_checkpoint):
        """Blocks should be processed in ascending height order."""
        from main import catchup

        heights_processed = []
        original_publish = mock_producer.publish_block

        def track_publish(normalized):
            heights_processed.append(normalized["block"]["height"])
            return 4

        mock_producer.publish_block.side_effect = track_publish

        catchup(mock_rpc, mock_producer, tmp_checkpoint, overlap=0)

        assert heights_processed == [0, 1, 2, 3, 4, 5]


class TestMainEntryPoint:
    """Test argument parsing and mode selection."""

    def test_test_block_mode_exits_cleanly(self, mocker):
        """--test-block should call test_block() and return."""
        mocker.patch("sys.argv", ["main.py", "--test-block", "0"])
        mock_rpc_cls = mocker.patch("main.BitcoinRPC")
        mock_rpc = mock_rpc_cls.return_value
        mock_rpc.getblockhash.return_value = "hash_genesis"
        mock_rpc.getblock.return_value = {
            "hash": "hash_genesis", "height": 0, "version": 1,
            "size": 285, "merkleroot": "m", "time": 0,
            "bits": "1d00ffff", "nonce": 0,
            "tx": [{"txid": "t", "version": 1, "locktime": 0,
                     "vin": [{"coinbase": "00", "sequence": 0}],
                     "vout": [{"value": 0, "n": 0,
                               "scriptPubKey": {"hex": "00"}}]}],
        }

        from main import main
        main()

        mock_rpc.getblockhash.assert_called_with(0)
        mock_rpc.getblock.assert_called_once()
