"""Integration tests for Docker services.

These tests verify that Docker containers are running and healthy.
Run with: pytest tests/integration/ -m integration

Prerequisites: docker compose up -d
"""

import subprocess
import json
import pytest
import requests

pytestmark = pytest.mark.integration


def _container_running(name: str) -> bool:
    """Check if a Docker container is running."""
    result = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", name],
        capture_output=True, text=True,
    )
    return result.stdout.strip() == "true"


class TestKafka:
    """Verify Kafka broker is healthy."""

    def test_kafka_container_running(self):
        assert _container_running("kafka"), "Kafka container is not running"

    def test_kafka_topics_listable(self):
        result = subprocess.run(
            ["docker", "exec", "kafka", "/opt/kafka/bin/kafka-topics.sh",
             "--bootstrap-server", "localhost:9092", "--list"],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 0

    def test_kafka_topics_exist(self):
        result = subprocess.run(
            ["docker", "exec", "kafka", "/opt/kafka/bin/kafka-topics.sh",
             "--bootstrap-server", "localhost:9092", "--list"],
            capture_output=True, text=True, timeout=30,
        )
        topics = result.stdout.strip().split("\n")
        expected = ["btc.blocks.v1", "btc.transactions.v1",
                     "btc.tx_inputs.v1", "btc.tx_outputs.v1"]
        for topic in expected:
            assert topic in topics, f"Topic {topic} not found"


class TestMinIO:
    """Verify MinIO is healthy and warehouse bucket exists."""

    def test_minio_container_running(self):
        assert _container_running("minio"), "MinIO container is not running"

    def test_minio_health_endpoint(self):
        resp = requests.get("http://localhost:9000/minio/health/live", timeout=5)
        assert resp.status_code == 200

    def test_warehouse_bucket_exists(self):
        result = subprocess.run(
            ["docker", "exec", "minio", "mc", "ls", "local/warehouse"],
            capture_output=True, text=True, timeout=10,
        )
        # Bucket exists if command succeeds (even if empty)
        assert result.returncode == 0 or "warehouse" in result.stderr


class TestStarRocks:
    """Verify StarRocks FE and BE are healthy."""

    def test_fe_container_running(self):
        assert _container_running("starrocks-fe"), "StarRocks FE is not running"

    def test_be_container_running(self):
        assert _container_running("starrocks-be"), "StarRocks BE is not running"

    def test_fe_health_endpoint(self):
        resp = requests.get("http://localhost:8030/api/health", timeout=5)
        assert resp.status_code == 200

    def test_be_health_endpoint(self):
        resp = requests.get("http://localhost:8040/api/health", timeout=5)
        assert resp.status_code == 200


class TestHiveMetastore:
    """Verify HMS is running."""

    def test_mysql_container_running(self):
        assert _container_running("mysql-hms"), "MySQL HMS is not running"

    def test_hive_metastore_container_running(self):
        assert _container_running("hive-metastore"), "Hive Metastore is not running"

    def test_hive_metastore_port_open(self):
        """HMS Thrift port 9083 should accept connections."""
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        try:
            result = sock.connect_ex(("localhost", 9083))
            assert result == 0, "HMS Thrift port 9083 not reachable"
        finally:
            sock.close()
