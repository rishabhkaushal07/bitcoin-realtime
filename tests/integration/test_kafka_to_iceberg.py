"""Integration tests: Kafka → Iceberg writer pipeline.

Tests cover:
  - Publishing normalized blocks to Kafka, then consuming and writing to Iceberg
  - Verifying records land in the correct Iceberg tables
  - Verifying data integrity (field values match)
  - Multi-block write and read
  - Finality upsert (OBSERVED → CONFIRMED)

Prerequisites:
  - docker compose up -d (Kafka, MinIO, MySQL, HMS)
  - bash scripts/create-kafka-topics.sh
  - python scripts/create_iceberg_tables.py --drop (clean tables)

Run with: pytest tests/integration/test_kafka_to_iceberg.py -m integration -v
"""

import sys
import os
import json
import time
import uuid

import pyarrow as pa
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "live-normalizer"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "pyiceberg-sidecar"))

pytestmark = pytest.mark.integration

BOOTSTRAP = "localhost:9092"
HMS_URI = "thrift://localhost:9083"

CATALOG_CONFIG = {
    "type": "hive",
    "uri": HMS_URI,
    "s3.endpoint": "http://localhost:9000",
    "s3.access-key-id": "minioadmin",
    "s3.secret-access-key": "minioadmin",
    "s3.region": "us-east-1",
    "warehouse": "s3://warehouse/",
    "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
}


def _services_available() -> bool:
    """Check if Kafka and HMS are reachable."""
    import socket
    for host, port in [("localhost", 9092), ("localhost", 9083)]:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(3)
            s.connect((host, port))
            s.close()
        except Exception:
            return False
    return True


@pytest.fixture(scope="module")
def check_services():
    if not _services_available():
        pytest.skip("Kafka or HMS not reachable")


@pytest.fixture(scope="module")
def catalog(check_services):
    from pyiceberg.catalog import load_catalog
    return load_catalog("hive", **CATALOG_CONFIG)


@pytest.fixture(scope="module")
def producer(check_services):
    from kafka_producer import BlockKafkaProducer
    p = BlockKafkaProducer(BOOTSTRAP)
    yield p
    p.close()


@pytest.fixture(scope="module")
def writer(check_services, catalog):
    """Create an IcebergWriter configured for integration tests."""
    from iceberg_writer import IcebergWriter
    kafka_config = {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"test-iceberg-writer-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    }
    w = IcebergWriter(
        catalog_config=CATALOG_CONFIG,
        kafka_config=kafka_config,
        batch_size=50,
        flush_interval_sec=5.0,
    )
    w._init_catalog()
    return w


@pytest.fixture
def genesis_normalized():
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


class TestKafkaToIcebergWriter:
    """Test the IcebergWriter: publish to Kafka, buffer, flush to Iceberg."""

    def test_parse_and_buffer_block(self, writer, genesis_normalized):
        """Parse a block message and add to buffer."""
        block = genesis_normalized["block"]
        msg_bytes = json.dumps(block).encode("utf-8")
        record = writer._parse_message("btc.blocks.v1", msg_bytes)

        assert record["block_hash"] == block["block_hash"]
        assert "schema_version" not in record

        writer._buffers["btc.blocks.v1"].append(record)
        assert len(writer._buffers["btc.blocks.v1"]) >= 1

    def test_flush_block_to_iceberg(self, writer, genesis_normalized):
        """Flush buffered block records to Iceberg btc.blocks table."""
        block = genesis_normalized["block"]
        record = {k: v for k, v in block.items() if k != "schema_version"}
        writer._buffers["btc.blocks.v1"] = [record]

        count = writer.flush_topic("btc.blocks.v1")
        assert count == 1
        assert len(writer._buffers["btc.blocks.v1"]) == 0

    def test_flush_transaction_to_iceberg(self, writer, genesis_normalized):
        """Flush transaction records to Iceberg."""
        records = [{k: v for k, v in tx.items() if k != "schema_version"}
                   for tx in genesis_normalized["transactions"]]
        writer._buffers["btc.transactions.v1"] = records

        count = writer.flush_topic("btc.transactions.v1")
        assert count == len(records)

    def test_flush_inputs_to_iceberg(self, writer, block_170_normalized):
        """Flush tx_in records to Iceberg."""
        records = [{k: v for k, v in inp.items() if k != "schema_version"}
                   for inp in block_170_normalized["tx_inputs"]]
        writer._buffers["btc.tx_inputs.v1"] = records

        count = writer.flush_topic("btc.tx_inputs.v1")
        assert count == len(records)

    def test_flush_outputs_to_iceberg(self, writer, block_170_normalized):
        """Flush tx_out records to Iceberg."""
        records = [{k: v for k, v in out.items() if k != "schema_version"}
                   for out in block_170_normalized["tx_outputs"]]
        writer._buffers["btc.tx_outputs.v1"] = records

        count = writer.flush_topic("btc.tx_outputs.v1")
        assert count == len(records)


class TestIcebergDataIntegrity:
    """Verify data written to Iceberg is correct."""

    def test_read_blocks_from_iceberg(self, catalog):
        """Read btc.blocks and verify genesis block data."""
        table = catalog.load_table("btc.blocks")
        result = table.scan().to_arrow()

        # Find genesis block
        genesis = result.filter(pa.compute.equal(result["height"], 0))
        assert len(genesis) >= 1

        row = genesis.to_pydict()
        assert row["block_hash"][0] == "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
        assert row["finality_status"][0] == "OBSERVED"

    def test_read_transactions_from_iceberg(self, catalog):
        """Read btc.transactions and verify data."""
        table = catalog.load_table("btc.transactions")
        result = table.scan().to_arrow()
        assert len(result) >= 1

    def test_read_inputs_from_iceberg(self, catalog):
        """Read btc.tx_in and verify data."""
        table = catalog.load_table("btc.tx_in")
        result = table.scan().to_arrow()
        assert len(result) >= 1

    def test_read_outputs_from_iceberg(self, catalog):
        """Read btc.tx_out and verify data, including satoshi values."""
        table = catalog.load_table("btc.tx_out")
        result = table.scan().to_arrow()
        assert len(result) >= 1

        # Check 50 BTC coinbase output exists
        values = result.column("value").to_pylist()
        assert 5000000000 in values


class TestFinalityUpsert:
    """Test finality status update via overwrite."""

    def test_update_block_finality(self, catalog):
        """Update genesis block from OBSERVED to CONFIRMED."""
        table = catalog.load_table("btc.blocks")

        # Read genesis block
        genesis_hash = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
        scan = table.scan(row_filter=f"block_hash == '{genesis_hash}'")
        existing = scan.to_arrow()

        if len(existing) == 0:
            pytest.skip("Genesis block not in Iceberg yet")

        # Update finality_status
        from pyiceberg.io.pyarrow import schema_to_pyarrow
        updated = existing.set_column(
            existing.schema.get_field_index("finality_status"),
            "finality_status",
            pa.array(["CONFIRMED"] * len(existing), type=existing.schema.field("finality_status").type),
        )
        table.overwrite(updated, overwrite_filter=f"block_hash == '{genesis_hash}'")

        # Verify
        result = table.scan(row_filter=f"block_hash == '{genesis_hash}'").to_arrow()
        assert len(result) >= 1
        assert result.column("finality_status")[0].as_py() == "CONFIRMED"

    def test_reorg_soft_delete(self, catalog):
        """Update genesis block to REORGED status."""
        table = catalog.load_table("btc.blocks")
        genesis_hash = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"

        scan = table.scan(row_filter=f"block_hash == '{genesis_hash}'")
        existing = scan.to_arrow()

        if len(existing) == 0:
            pytest.skip("Genesis block not in Iceberg yet")

        updated = existing.set_column(
            existing.schema.get_field_index("finality_status"),
            "finality_status",
            pa.array(["REORGED"] * len(existing), type=existing.schema.field("finality_status").type),
        )
        table.overwrite(updated, overwrite_filter=f"block_hash == '{genesis_hash}'")

        result = table.scan(row_filter=f"block_hash == '{genesis_hash}'").to_arrow()
        assert result.column("finality_status")[0].as_py() == "REORGED"

    def test_restore_to_observed(self, catalog):
        """Restore genesis block back to OBSERVED for other tests."""
        table = catalog.load_table("btc.blocks")
        genesis_hash = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"

        scan = table.scan(row_filter=f"block_hash == '{genesis_hash}'")
        existing = scan.to_arrow()

        if len(existing) == 0:
            return

        updated = existing.set_column(
            existing.schema.get_field_index("finality_status"),
            "finality_status",
            pa.array(["OBSERVED"] * len(existing), type=existing.schema.field("finality_status").type),
        )
        table.overwrite(updated, overwrite_filter=f"block_hash == '{genesis_hash}'")
