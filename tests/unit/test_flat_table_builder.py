"""Unit tests for the StarRocks flat table builder.

Tests cover:
  - SQL template validation (correct table references, joins, filters)
  - Incremental mode logic
  - Range mode processing
  - Database setup

These are SQL-level unit tests that validate the query structure
without needing a running StarRocks instance.
"""

import sys
import os
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "starrocks"))

from flat_table_builder import (
    INSERT_BLOCK_SQL,
    MAX_HEIGHT_SQL,
    ICEBERG_MAX_HEIGHT_SQL,
    build_block,
    build_range,
    build_incremental,
    setup_database,
)


class TestSQLStructure:
    """Validate SQL query structure and references."""

    def test_insert_targets_flat_table(self):
        assert "INSERT INTO bitcoin_v3.bitcoin_flat_v3" in INSERT_BLOCK_SQL

    def test_insert_reads_from_iceberg_blocks(self):
        assert "iceberg_catalog.btc.blocks" in INSERT_BLOCK_SQL

    def test_insert_reads_from_iceberg_transactions(self):
        assert "iceberg_catalog.btc.transactions" in INSERT_BLOCK_SQL

    def test_insert_reads_from_iceberg_tx_in(self):
        assert "iceberg_catalog.btc.tx_in" in INSERT_BLOCK_SQL

    def test_insert_reads_from_iceberg_tx_out(self):
        assert "iceberg_catalog.btc.tx_out" in INSERT_BLOCK_SQL

    def test_insert_joins_blocks_to_transactions(self):
        assert "t.hashBlock = b.block_hash" in INSERT_BLOCK_SQL

    def test_insert_filters_reorged_blocks(self):
        assert "finality_status != 'REORGED'" in INSERT_BLOCK_SQL

    def test_insert_filters_by_height(self):
        assert "b.height = %s" in INSERT_BLOCK_SQL

    def test_insert_uses_array_agg_for_inputs(self):
        assert "ARRAY_AGG" in INSERT_BLOCK_SQL

    def test_insert_uses_named_struct(self):
        assert "NAMED_STRUCT" in INSERT_BLOCK_SQL

    def test_insert_computes_total_output_value(self):
        assert "SUM(value) AS total_output_value" in INSERT_BLOCK_SQL

    def test_max_height_queries_flat_table(self):
        assert "bitcoin_v3.bitcoin_flat_v3" in MAX_HEIGHT_SQL

    def test_iceberg_max_height_queries_blocks(self):
        assert "iceberg_catalog.btc.blocks" in ICEBERG_MAX_HEIGHT_SQL


class TestBuildBlock:
    """Test build_block function."""

    def test_executes_insert_with_height(self):
        conn = MagicMock()
        cursor = MagicMock()
        cursor.rowcount = 5
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        rows = build_block(conn, 170)
        cursor.execute.assert_called_once_with(INSERT_BLOCK_SQL, (170, 170, 170))
        assert rows == 5

    def test_returns_zero_for_empty_block(self):
        conn = MagicMock()
        cursor = MagicMock()
        cursor.rowcount = 0
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        rows = build_block(conn, 999999)
        assert rows == 0


class TestBuildRange:
    """Test build_range function."""

    @patch("flat_table_builder.build_block")
    def test_processes_all_heights_in_range(self, mock_build):
        mock_build.return_value = 1
        conn = MagicMock()

        total = build_range(conn, 0, 4)
        assert mock_build.call_count == 5  # 0,1,2,3,4
        assert total == 5

    @patch("flat_table_builder.build_block")
    def test_single_height_range(self, mock_build):
        mock_build.return_value = 3
        conn = MagicMock()

        total = build_range(conn, 100, 100)
        assert mock_build.call_count == 1
        assert total == 3


class TestBuildIncremental:
    """Test incremental mode."""

    def test_no_work_when_up_to_date(self):
        conn = MagicMock()
        cursor = MagicMock()
        # First call: flat table max = 100
        # Second call: iceberg max = 100
        cursor.fetchone.side_effect = [(100,), (100,)]
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        total = build_incremental(conn)
        assert total == 0

    def test_no_work_when_flat_ahead(self):
        conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchone.side_effect = [(100,), (50,)]
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        total = build_incremental(conn)
        assert total == 0

    @patch("flat_table_builder.build_range")
    def test_builds_missing_range(self, mock_range):
        mock_range.return_value = 10
        conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchone.side_effect = [(50,), (55,)]
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        total = build_incremental(conn)
        mock_range.assert_called_once_with(conn, 51, 55)
        assert total == 10


class TestSetupDatabase:
    """Test database setup."""

    def test_creates_database(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        setup_database(conn)
        cursor.execute.assert_called_once_with("CREATE DATABASE IF NOT EXISTS bitcoin_v3")
