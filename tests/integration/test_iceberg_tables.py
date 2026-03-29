"""Integration tests for Iceberg tables via PyIceberg + HMS.

Tests cover:
  - HMS connectivity and namespace existence
  - Table existence and schema validation for all 4 tables
  - Partition spec validation
  - Table properties (format-version, compression)
  - Identifier fields for upserts
  - Write and read roundtrip for each table
  - Idempotent re-creation (IF NOT EXISTS semantics)

Prerequisites: docker compose up -d (MySQL + HMS + MinIO)
Run with: pytest tests/integration/test_iceberg_tables.py -m integration -v
"""

import sys
import os
import pytest
import pyarrow as pa
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "live-normalizer"))

pytestmark = pytest.mark.integration

CATALOG_CONFIG = {
    "type": "hive",
    "uri": "thrift://localhost:9083",
    "s3.endpoint": "http://localhost:9000",
    "s3.access-key-id": "minioadmin",
    "s3.secret-access-key": "minioadmin",
    "s3.region": "us-east-1",
    "warehouse": "s3://warehouse/",
    "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
}


def _hms_available() -> bool:
    """Check if HMS is reachable."""
    try:
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3)
        s.connect(("localhost", 9083))
        s.close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def catalog():
    """Load PyIceberg catalog connected to HMS."""
    if not _hms_available():
        pytest.skip("HMS not reachable at localhost:9083")
    from pyiceberg.catalog import load_catalog
    return load_catalog("hive", **CATALOG_CONFIG)


class TestHMSConnectivity:
    """Verify HMS is reachable and namespace exists."""

    def test_list_namespaces(self, catalog):
        namespaces = catalog.list_namespaces()
        ns_names = [n[0] for n in namespaces]
        assert "btc" in ns_names, f"btc namespace not found. Got: {ns_names}"

    def test_list_tables(self, catalog):
        tables = catalog.list_tables("btc")
        table_names = {t[1] for t in tables}
        expected = {"blocks", "transactions", "tx_in", "tx_out"}
        assert expected == table_names, f"Expected {expected}, got {table_names}"


class TestBlocksTable:
    """Validate btc.blocks schema, partitioning, and properties."""

    def test_schema_columns(self, catalog):
        table = catalog.load_table("btc.blocks")
        field_names = [f.name for f in table.schema().fields]
        expected = [
            "block_hash", "height", "version", "blocksize",
            "hashPrev", "hashMerkleRoot", "nTime", "nBits", "nNonce",
            "block_timestamp", "observed_at", "ingested_at",
            "finality_status", "source_seq",
        ]
        assert field_names == expected

    def test_schema_types(self, catalog):
        table = catalog.load_table("btc.blocks")
        schema = table.schema()
        assert str(schema.find_type("block_hash")) == "string"
        assert str(schema.find_type("height")) == "int"
        assert str(schema.find_type("block_timestamp")) == "timestamp"
        assert str(schema.find_type("source_seq")) == "int"

    def test_identifier_fields(self, catalog):
        table = catalog.load_table("btc.blocks")
        id_fields = table.schema().identifier_field_ids
        id_names = [table.schema().find_field(fid).name for fid in id_fields]
        assert id_names == ["block_hash"]

    def test_partition_spec(self, catalog):
        table = catalog.load_table("btc.blocks")
        spec = table.spec()
        assert len(spec.fields) == 1
        pf = spec.fields[0]
        assert pf.name == "height_bucket"
        assert "bucket[10]" in str(pf.transform)

    def test_format_version(self, catalog):
        table = catalog.load_table("btc.blocks")
        assert table.metadata.format_version == 2

    def test_properties(self, catalog):
        table = catalog.load_table("btc.blocks")
        props = table.properties
        assert props.get("write.format.default") == "parquet"
        assert props.get("write.parquet.compression-codec") == "zstd"


class TestTransactionsTable:
    """Validate btc.transactions schema and partitioning."""

    def test_schema_columns(self, catalog):
        table = catalog.load_table("btc.transactions")
        field_names = [f.name for f in table.schema().fields]
        expected = [
            "txid", "hashBlock", "version", "lockTime",
            "block_height", "block_timestamp", "observed_at", "ingested_at",
        ]
        assert field_names == expected

    def test_identifier_fields(self, catalog):
        table = catalog.load_table("btc.transactions")
        id_fields = table.schema().identifier_field_ids
        id_names = [table.schema().find_field(fid).name for fid in id_fields]
        assert id_names == ["txid"]

    def test_partition_spec(self, catalog):
        table = catalog.load_table("btc.transactions")
        spec = table.spec()
        assert len(spec.fields) == 1
        assert spec.fields[0].name == "block_height_bucket"
        assert "bucket[10]" in str(spec.fields[0].transform)


class TestTxInTable:
    """Validate btc.tx_in schema and composite identifier."""

    def test_schema_columns(self, catalog):
        table = catalog.load_table("btc.tx_in")
        field_names = [f.name for f in table.schema().fields]
        expected = [
            "txid", "hashPrevOut", "indexPrevOut", "scriptSig",
            "sequence", "block_hash", "block_height",
            "observed_at", "ingested_at",
        ]
        assert field_names == expected

    def test_composite_identifier_fields(self, catalog):
        table = catalog.load_table("btc.tx_in")
        id_fields = table.schema().identifier_field_ids
        id_names = [table.schema().find_field(fid).name for fid in id_fields]
        assert id_names == ["txid", "hashPrevOut", "indexPrevOut"]

    def test_sequence_is_long(self, catalog):
        """Sequence numbers can exceed INT32_MAX."""
        table = catalog.load_table("btc.tx_in")
        seq_field = table.schema().find_field("sequence")
        assert str(seq_field.field_type) == "long"


class TestTxOutTable:
    """Validate btc.tx_out schema and composite identifier."""

    def test_schema_columns(self, catalog):
        table = catalog.load_table("btc.tx_out")
        field_names = [f.name for f in table.schema().fields]
        expected = [
            "txid", "indexOut", "height", "value", "scriptPubKey",
            "address", "block_hash", "block_timestamp",
            "observed_at", "ingested_at",
        ]
        assert field_names == expected

    def test_composite_identifier_fields(self, catalog):
        table = catalog.load_table("btc.tx_out")
        id_fields = table.schema().identifier_field_ids
        id_names = [table.schema().find_field(fid).name for fid in id_fields]
        assert id_names == ["txid", "indexOut"]

    def test_value_is_long(self, catalog):
        """Value in satoshis can exceed INT32_MAX (21M BTC = 2.1e15 sat)."""
        table = catalog.load_table("btc.tx_out")
        val_field = table.schema().find_field("value")
        assert str(val_field.field_type) == "long"


class TestIcebergWriteRead:
    """Write sample data to Iceberg and read it back."""

    def _arrow_schema_for(self, table):
        """Build a PyArrow schema matching the Iceberg table schema."""
        from pyiceberg.io.pyarrow import schema_to_pyarrow
        return schema_to_pyarrow(table.schema())

    def test_write_and_read_block(self, catalog):
        table = catalog.load_table("btc.blocks")
        arrow_schema = self._arrow_schema_for(table)
        now = datetime(2026, 3, 27, 0, 0, 0)
        block_data = pa.table({
            "block_hash": pa.array(["0" * 64], type=pa.large_string()),
            "height": pa.array([0], type=pa.int32()),
            "version": pa.array([1], type=pa.int32()),
            "blocksize": pa.array([285], type=pa.int32()),
            "hashPrev": pa.array(["0" * 64], type=pa.large_string()),
            "hashMerkleRoot": pa.array(["4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"], type=pa.large_string()),
            "nTime": pa.array([1231006505], type=pa.int32()),
            "nBits": pa.array([486604799], type=pa.int64()),
            "nNonce": pa.array([2083236893], type=pa.int64()),
            "block_timestamp": pa.array([now], type=pa.timestamp("us")),
            "observed_at": pa.array(["2026-03-27T00:00:00Z"], type=pa.large_string()),
            "ingested_at": pa.array(["2026-03-27T00:00:00Z"], type=pa.large_string()),
            "finality_status": pa.array(["OBSERVED"], type=pa.large_string()),
            "source_seq": pa.array([0], type=pa.int32()),
        }, schema=arrow_schema)
        table.append(block_data)

        # Read back
        result = table.scan().to_arrow()
        assert len(result) >= 1
        row = result.filter(pa.compute.equal(result["height"], 0)).to_pydict()
        assert row["block_hash"][0] == "0" * 64
        assert row["height"][0] == 0
        assert row["finality_status"][0] == "OBSERVED"

    def test_write_and_read_transaction(self, catalog):
        table = catalog.load_table("btc.transactions")
        arrow_schema = self._arrow_schema_for(table)
        now = datetime(2026, 3, 27, 0, 0, 0)
        tx_data = pa.table({
            "txid": pa.array(["4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"], type=pa.large_string()),
            "hashBlock": pa.array(["0" * 64], type=pa.large_string()),
            "version": pa.array([1], type=pa.int32()),
            "lockTime": pa.array([0], type=pa.int32()),
            "block_height": pa.array([0], type=pa.int32()),
            "block_timestamp": pa.array([now], type=pa.timestamp("us")),
            "observed_at": pa.array(["2026-03-27T00:00:00Z"], type=pa.large_string()),
            "ingested_at": pa.array(["2026-03-27T00:00:00Z"], type=pa.large_string()),
        }, schema=arrow_schema)
        table.append(tx_data)

        result = table.scan().to_arrow()
        assert len(result) >= 1

    def test_write_and_read_tx_in(self, catalog):
        table = catalog.load_table("btc.tx_in")
        arrow_schema = self._arrow_schema_for(table)
        tx_in_data = pa.table({
            "txid": pa.array(["4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"], type=pa.large_string()),
            "hashPrevOut": pa.array(["0" * 64], type=pa.large_string()),
            "indexPrevOut": pa.array([0], type=pa.int32()),
            "scriptSig": pa.array(["04ffff001d0104"], type=pa.large_string()),
            "sequence": pa.array([4294967295], type=pa.int64()),
            "block_hash": pa.array(["0" * 64], type=pa.large_string()),
            "block_height": pa.array([0], type=pa.int32()),
            "observed_at": pa.array(["2026-03-27T00:00:00Z"], type=pa.large_string()),
            "ingested_at": pa.array(["2026-03-27T00:00:00Z"], type=pa.large_string()),
        }, schema=arrow_schema)
        table.append(tx_in_data)

        result = table.scan().to_arrow()
        assert len(result) >= 1

    def test_write_and_read_tx_out(self, catalog):
        table = catalog.load_table("btc.tx_out")
        arrow_schema = self._arrow_schema_for(table)
        now = datetime(2026, 3, 27, 0, 0, 0)
        tx_out_data = pa.table({
            "txid": pa.array(["4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"], type=pa.large_string()),
            "indexOut": pa.array([0], type=pa.int32()),
            "height": pa.array([0], type=pa.int32()),
            "value": pa.array([5000000000], type=pa.int64()),
            "scriptPubKey": pa.array(["4104678afdb0fe"], type=pa.large_string()),
            "address": pa.array(["unknown"], type=pa.large_string()),
            "block_hash": pa.array(["0" * 64], type=pa.large_string()),
            "block_timestamp": pa.array([now], type=pa.timestamp("us")),
            "observed_at": pa.array(["2026-03-27T00:00:00Z"], type=pa.large_string()),
            "ingested_at": pa.array(["2026-03-27T00:00:00Z"], type=pa.large_string()),
        }, schema=arrow_schema)
        table.append(tx_out_data)

        result = table.scan().to_arrow()
        assert len(result) >= 1
        row = result.filter(pa.compute.equal(result["height"], 0)).to_pydict()
        assert row["value"][0] == 5000000000


class TestIdempotentCreation:
    """Re-running create script should not fail."""

    def test_rerun_create_tables_no_error(self, catalog):
        """Running create_iceberg_tables.py again should succeed (IF NOT EXISTS)."""
        import subprocess
        result = subprocess.run(
            ["python3", "scripts/create_iceberg_tables.py"],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 0
        assert "already exists" in result.stdout.lower() or "ready" in result.stdout.lower()
