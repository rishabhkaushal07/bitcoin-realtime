"""Unit tests for the finality status updater.

Tests cover:
  - Event validation (type, finality_status)
  - Processing finality events (OBSERVED → CONFIRMED)
  - Processing reorg events (→ REORGED)
  - Handling missing blocks
  - Stop behavior
"""

import sys
import os
from unittest.mock import MagicMock, patch, PropertyMock

import pyarrow as pa
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "pyiceberg-sidecar"))

from finality_updater import FinalityUpdater


@pytest.fixture
def updater():
    """Create a FinalityUpdater with mocked internals."""
    u = FinalityUpdater()
    u._blocks_table = MagicMock()
    u._arrow_schema = pa.schema([
        pa.field("block_hash", pa.large_string(), nullable=False),
        pa.field("height", pa.int32()),
        pa.field("finality_status", pa.large_string()),
    ])
    return u


class TestEventValidation:
    """Test event type and status validation."""

    def test_rejects_unknown_event_type(self, updater):
        event = {"type": "unknown", "block_hash": "abc", "finality_status": "CONFIRMED"}
        assert updater.process_event(event) is False

    def test_rejects_invalid_status(self, updater):
        event = {"type": "finality", "block_hash": "abc", "finality_status": "INVALID"}
        assert updater.process_event(event) is False

    def test_accepts_finality_confirmed(self, updater):
        """Should accept finality event with CONFIRMED status."""
        # Mock scan to return an existing block
        mock_scan = MagicMock()
        mock_scan.to_arrow.return_value = pa.table({
            "block_hash": pa.array(["abc"], type=pa.large_string()),
            "height": pa.array([100], type=pa.int32()),
            "finality_status": pa.array(["OBSERVED"], type=pa.large_string()),
        })
        updater._blocks_table.scan.return_value = mock_scan

        event = {
            "type": "finality",
            "block_hash": "abc",
            "height": 100,
            "finality_status": "CONFIRMED",
        }
        assert updater.process_event(event) is True

    def test_accepts_reorg_reorged(self, updater):
        """Should accept reorg event with REORGED status."""
        mock_scan = MagicMock()
        mock_scan.to_arrow.return_value = pa.table({
            "block_hash": pa.array(["abc"], type=pa.large_string()),
            "height": pa.array([100], type=pa.int32()),
            "finality_status": pa.array(["OBSERVED"], type=pa.large_string()),
        })
        updater._blocks_table.scan.return_value = mock_scan

        event = {
            "type": "reorg",
            "block_hash": "abc",
            "height": 100,
            "finality_status": "REORGED",
        }
        assert updater.process_event(event) is True


class TestFinalityProcessing:
    """Test actual finality update logic."""

    def test_missing_block_returns_false(self, updater):
        """Should return False if block not found in Iceberg."""
        mock_scan = MagicMock()
        mock_scan.to_arrow.return_value = pa.table({
            "block_hash": pa.array([], type=pa.large_string()),
            "height": pa.array([], type=pa.int32()),
            "finality_status": pa.array([], type=pa.large_string()),
        })
        updater._blocks_table.scan.return_value = mock_scan

        event = {
            "type": "finality",
            "block_hash": "nonexistent",
            "height": 999,
            "finality_status": "CONFIRMED",
        }
        assert updater.process_event(event) is False

    def test_overwrite_called_with_updated_status(self, updater):
        """Should call overwrite with the updated finality_status."""
        mock_scan = MagicMock()
        existing = pa.table({
            "block_hash": pa.array(["abc123"], type=pa.large_string()),
            "height": pa.array([100], type=pa.int32()),
            "finality_status": pa.array(["OBSERVED"], type=pa.large_string()),
        })
        mock_scan.to_arrow.return_value = existing
        updater._blocks_table.scan.return_value = mock_scan

        event = {
            "type": "finality",
            "block_hash": "abc123",
            "height": 100,
            "finality_status": "CONFIRMED",
        }
        updater.process_event(event)

        # Verify overwrite was called
        updater._blocks_table.overwrite.assert_called_once()
        call_args = updater._blocks_table.overwrite.call_args
        updated_table = call_args[0][0]
        assert updated_table.column("finality_status")[0].as_py() == "CONFIRMED"


class TestStopBehavior:
    def test_stop_sets_running_false(self, updater):
        updater._running = True
        updater.stop()
        assert updater._running is False
