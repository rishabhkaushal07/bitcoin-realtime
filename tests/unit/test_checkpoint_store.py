"""Unit tests for the checkpoint store."""

import sys
import os
import json
import tempfile
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "live-normalizer"))

from checkpoint_store import CheckpointStore


class TestCheckpointStore:
    """Test file-based checkpoint persistence."""

    @pytest.fixture
    def tmp_checkpoint(self, tmp_path):
        """Create a CheckpointStore with a temp file path."""
        path = str(tmp_path / "checkpoint.json")
        return CheckpointStore(path=path)

    def test_initial_state(self, tmp_checkpoint):
        assert tmp_checkpoint.last_height == -1
        assert tmp_checkpoint.last_hash == ""
        assert tmp_checkpoint.mode == "idle"

    def test_update_and_read(self, tmp_checkpoint):
        tmp_checkpoint.update(100, "hash_100", zmq_seq=5, mode="live")
        assert tmp_checkpoint.last_height == 100
        assert tmp_checkpoint.last_hash == "hash_100"
        assert tmp_checkpoint.mode == "live"
        assert tmp_checkpoint.state["last_seen_zmq_seq"] == 5

    def test_persistence_across_instances(self, tmp_path):
        """Checkpoint should survive process restart."""
        path = str(tmp_path / "checkpoint.json")

        store1 = CheckpointStore(path=path)
        store1.update(500, "hash_500", zmq_seq=10, mode="catchup")

        store2 = CheckpointStore(path=path)
        assert store2.last_height == 500
        assert store2.last_hash == "hash_500"
        assert store2.state["last_seen_zmq_seq"] == 10
        assert store2.mode == "catchup"

    def test_atomic_write(self, tmp_path):
        """Write should use tmp+rename for atomicity."""
        path = str(tmp_path / "checkpoint.json")
        store = CheckpointStore(path=path)
        store.update(1, "hash_1")

        assert os.path.exists(path)
        assert not os.path.exists(path + ".tmp")

        with open(path) as f:
            data = json.load(f)
        assert data["last_finalized_height"] == 1

    def test_updated_at_timestamp(self, tmp_checkpoint):
        tmp_checkpoint.update(1, "hash_1")
        assert tmp_checkpoint.state["updated_at"] != ""
        assert "T" in tmp_checkpoint.state["updated_at"]  # ISO format

    def test_mode_not_changed_if_none(self, tmp_checkpoint):
        tmp_checkpoint.update(1, "hash_1", mode="live")
        tmp_checkpoint.update(2, "hash_2")  # no mode arg
        assert tmp_checkpoint.mode == "live"

    def test_zmq_seq_not_changed_if_negative(self, tmp_checkpoint):
        tmp_checkpoint.update(1, "hash_1", zmq_seq=10)
        tmp_checkpoint.update(2, "hash_2", zmq_seq=-1)
        assert tmp_checkpoint.state["last_seen_zmq_seq"] == 10
