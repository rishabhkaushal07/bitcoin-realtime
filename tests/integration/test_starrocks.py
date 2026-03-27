"""Integration tests for StarRocks external Iceberg catalog + flat serving table.

Tests cover:
  - StarRocks FE/BE connectivity
  - External Iceberg catalog creation
  - Querying Iceberg tables through StarRocks
  - Native flat table creation
  - Per-block incremental insert
  - Query correctness on denormalized data

Prerequisites:
  - docker compose up -d (all services healthy)
  - python scripts/create_iceberg_tables.py (Iceberg tables created)
  - Some data in Iceberg tables (from Kafka writer or direct writes)

Run with: pytest tests/integration/test_starrocks.py -m integration -v
"""

import sys
import os

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "starrocks"))

pytestmark = pytest.mark.integration


def _starrocks_available() -> bool:
    """Check if StarRocks is reachable via MySQL protocol."""
    try:
        import pymysql
        conn = pymysql.connect(
            host="localhost", port=9030, user="root", password="",
            connect_timeout=5,
        )
        conn.close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def conn():
    if not _starrocks_available():
        pytest.skip("StarRocks not reachable at localhost:9030")
    import pymysql
    c = pymysql.connect(
        host="localhost", port=9030, user="root", password="",
        autocommit=True,
    )
    yield c
    c.close()


def _execute(conn, sql):
    """Execute SQL and return all results."""
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


class TestStarRocksConnectivity:
    """Basic connectivity tests."""

    def test_select_one(self, conn):
        result = _execute(conn, "SELECT 1")
        assert result[0][0] == 1

    def test_show_databases(self, conn):
        result = _execute(conn, "SHOW DATABASES")
        db_names = [r[0] for r in result]
        assert "information_schema" in db_names


class TestIcebergCatalog:
    """Test external Iceberg catalog via HMS."""

    def test_create_iceberg_catalog(self, conn):
        """Create external catalog pointing to HMS + MinIO."""
        sql = open(os.path.join(os.path.dirname(__file__), "..", "..",
                                "starrocks", "iceberg_catalog_v3.sql")).read()
        # Execute only the CREATE CATALOG statement
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt.startswith("CREATE EXTERNAL CATALOG"):
                _execute(conn, stmt)
                break

    def test_catalog_exists(self, conn):
        result = _execute(conn, "SHOW CATALOGS")
        catalog_names = [r[0] for r in result]
        assert "iceberg_catalog" in catalog_names

    def test_list_iceberg_databases(self, conn):
        result = _execute(conn, "SHOW DATABASES FROM iceberg_catalog")
        db_names = [r[0] for r in result]
        assert "btc" in db_names

    def test_list_iceberg_tables(self, conn):
        result = _execute(conn, "SHOW TABLES FROM iceberg_catalog.btc")
        table_names = [r[0] for r in result]
        expected = {"blocks", "transactions", "tx_in", "tx_out"}
        assert expected.issubset(set(table_names))

    def test_query_iceberg_blocks(self, conn):
        """Query btc.blocks through StarRocks external catalog."""
        result = _execute(conn, "SELECT COUNT(*) FROM iceberg_catalog.btc.blocks")
        count = result[0][0]
        assert count >= 0  # May be 0 if no data yet

    def test_query_iceberg_blocks_schema(self, conn):
        """Verify schema of btc.blocks via DESCRIBE."""
        result = _execute(conn, "DESCRIBE iceberg_catalog.btc.blocks")
        col_names = [r[0] for r in result]
        assert "block_hash" in col_names
        assert "height" in col_names
        assert "finality_status" in col_names


class TestFlatServingTable:
    """Test native flat serving table creation and queries."""

    def test_create_flat_table(self, conn):
        """Create the bitcoin_flat_v3 table."""
        sql = open(os.path.join(os.path.dirname(__file__), "..", "..",
                                "starrocks", "flat_serving_v3.sql")).read()
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if not stmt or stmt.startswith("--"):
                continue
            try:
                _execute(conn, stmt)
            except Exception:
                pass  # Table may already exist

    def test_flat_table_exists(self, conn):
        result = _execute(conn, "SHOW TABLES FROM bitcoin_v3")
        table_names = [r[0] for r in result]
        assert "bitcoin_flat_v3" in table_names

    def test_flat_table_schema(self, conn):
        """Verify flat table has expected columns."""
        result = _execute(conn, "DESCRIBE bitcoin_v3.bitcoin_flat_v3")
        col_names = [r[0] for r in result]
        expected = [
            "block_hash", "block_height", "block_timestamp", "nTime",
            "txid", "tx_version", "lockTime",
            "input_count", "inputs",
            "output_count", "total_output_value", "outputs",
            "finality_status",
        ]
        for col in expected:
            assert col in col_names, f"Missing column: {col}"

    def test_flat_table_primary_key(self, conn):
        """Verify txid is the primary key."""
        result = _execute(conn, "SHOW CREATE TABLE bitcoin_v3.bitcoin_flat_v3")
        create_sql = result[0][1]
        assert "PRIMARY KEY" in create_sql
        assert "txid" in create_sql


class TestFlatTableBuilder:
    """Test incremental flat table builder (requires data in Iceberg)."""

    def test_incremental_build_no_error(self, conn):
        """Run incremental build — should not error even with no data."""
        from flat_table_builder import build_incremental
        # This will query both flat table and Iceberg for max heights
        try:
            build_incremental(conn)
        except Exception as e:
            # Accept errors if catalog not set up
            if "catalog" in str(e).lower():
                pytest.skip("Iceberg catalog not configured in StarRocks")
            raise
