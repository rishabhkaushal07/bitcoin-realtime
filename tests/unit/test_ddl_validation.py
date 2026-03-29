"""Validate DDL files for consistency with the normalizer schema.

These tests ensure that:
  - Iceberg table columns match normalizer output fields
  - StarRocks flat table columns match the join query
  - Kafka topic names are consistent across codebase
  - No schema drift between components
"""

import sys
import os
import re
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "live-normalizer"))

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..", "..")


def _read_file(relpath: str) -> str:
    return open(os.path.join(PROJECT_ROOT, relpath)).read()


class TestIcebergDDLConsistency:
    """Verify Iceberg DDL matches normalizer output fields."""

    def test_blocks_table_has_all_normalizer_fields(self):
        ddl = _read_file("iceberg/create_raw_tables.sql")
        from normalizer import normalize_block

        # Normalize a minimal block to get field names
        block = {
            "hash": "a" * 64, "height": 0, "version": 1, "size": 100,
            "merkleroot": "m", "time": 0, "bits": "1d00ffff", "nonce": 0,
            "tx": [{"txid": "t", "version": 1, "locktime": 0,
                     "vin": [{"coinbase": "00", "sequence": 0}],
                     "vout": [{"value": 0, "n": 0, "scriptPubKey": {"hex": "00"}}]}],
        }
        result = normalize_block(block)

        for field in result["block"]:
            if field in ("schema_version",):
                continue  # metadata not stored in Iceberg
            assert field in ddl, f"Block field '{field}' missing from Iceberg DDL"

    def test_transactions_table_has_all_fields(self):
        ddl = _read_file("iceberg/create_raw_tables.sql")
        expected_fields = ["txid", "hashBlock", "version", "lockTime",
                           "block_height", "block_timestamp", "observed_at", "ingested_at"]
        for field in expected_fields:
            assert field in ddl, f"Transaction field '{field}' missing"

    def test_tx_in_table_has_all_fields(self):
        ddl = _read_file("iceberg/create_raw_tables.sql")
        expected_fields = ["txid", "hashPrevOut", "indexPrevOut", "scriptSig",
                           "sequence", "block_hash", "block_height"]
        for field in expected_fields:
            assert field in ddl, f"tx_in field '{field}' missing"

    def test_tx_out_table_has_all_fields(self):
        ddl = _read_file("iceberg/create_raw_tables.sql")
        expected_fields = ["txid", "indexOut", "height", "value",
                           "scriptPubKey", "address", "block_hash"]
        for field in expected_fields:
            assert field in ddl, f"tx_out field '{field}' missing"

    def test_format_version_is_2(self):
        """Iceberg format v2 required for row-level deletes."""
        ddl = _read_file("iceberg/create_raw_tables.sql")
        assert "'format-version' = '2'" in ddl

    def test_parquet_with_zstd(self):
        ddl = _read_file("iceberg/create_raw_tables.sql")
        assert "'write.format.default' = 'parquet'" in ddl
        assert "'write.parquet.compression-codec' = 'zstd'" in ddl

    def test_identifier_fields_set(self):
        """Each table should have identifier fields for PyIceberg upserts."""
        ddl = _read_file("iceberg/create_raw_tables.sql")
        assert "ALTER TABLE btc.blocks SET IDENTIFIER FIELDS block_hash" in ddl
        assert "ALTER TABLE btc.transactions SET IDENTIFIER FIELDS txid" in ddl
        assert "ALTER TABLE btc.tx_in SET IDENTIFIER FIELDS txid, hashPrevOut, indexPrevOut" in ddl
        assert "ALTER TABLE btc.tx_out SET IDENTIFIER FIELDS txid, indexOut" in ddl


class TestKafkaTopicConsistency:
    """Verify Kafka topic names are consistent across codebase."""

    EXPECTED_TOPICS = [
        "btc.blocks.v1",
        "btc.transactions.v1",
        "btc.tx_inputs.v1",
        "btc.tx_outputs.v1",
    ]

    def test_topics_in_kafka_producer(self):
        producer_code = _read_file("live-normalizer/kafka_producer.py")
        for topic in self.EXPECTED_TOPICS:
            assert topic in producer_code, f"Topic '{topic}' missing from kafka_producer.py"

    def test_topics_in_create_script(self):
        script = _read_file("scripts/create-kafka-topics.sh")
        for topic in self.EXPECTED_TOPICS:
            assert topic in script, f"Topic '{topic}' missing from create-kafka-topics.sh"

    def test_control_topic_in_script(self):
        script = _read_file("scripts/create-kafka-topics.sh")
        assert "control-iceberg" in script


class TestStarRocksDDLConsistency:
    """Verify StarRocks DDL is consistent."""

    def test_flat_table_has_txid_primary_key(self):
        ddl = _read_file("starrocks/flat_serving_v3.sql")
        assert "PRIMARY KEY (txid, block_height)" in ddl

    def test_flat_table_partitioned_by_height(self):
        ddl = _read_file("starrocks/flat_serving_v3.sql")
        assert "PARTITION BY RANGE (block_height)" in ddl

    def test_catalog_points_to_hms(self):
        ddl = _read_file("starrocks/iceberg_catalog_v3.sql")
        assert "thrift://hive-metastore:9083" in ddl

    def test_catalog_points_to_minio(self):
        ddl = _read_file("starrocks/iceberg_catalog_v3.sql")
        assert "http://minio:9000" in ddl

    def test_datacache_enabled(self):
        ddl = _read_file("starrocks/iceberg_catalog_v3.sql")
        assert '"enable_datacache" = "true"' in ddl


class TestDockerComposeConsistency:
    """Verify docker-compose.yml data paths are on local-scratch4."""

    def test_all_volumes_on_ssd(self):
        compose = _read_file("docker-compose.yml")
        volume_lines = [l.strip() for l in compose.split("\n")
                        if "/local-scratch4/" in l and ":" in l]
        assert len(volume_lines) >= 5, "Expected at least 5 volume mounts"
        for line in volume_lines:
            assert line.strip().startswith("- /local-scratch4/bitcoin_2025/"), \
                f"Volume not on SSD: {line}"

    def test_no_docker_volumes(self):
        """Named Docker volumes should NOT be used for persistent data."""
        compose = _read_file("docker-compose.yml")
        assert "volumes:" not in compose.split("networks:")[0].split("services:")[0], \
            "Top-level 'volumes:' section found — use bind mounts instead"
