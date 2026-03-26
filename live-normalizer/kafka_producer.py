"""Kafka producer — publishes normalized records to 4 Kafka topics.

Topic layout (V3 plan Section 9.2):
  btc.blocks.v1         key=block_hash
  btc.transactions.v1   key=txid
  btc.tx_inputs.v1      key=txid:hashPrevOut:indexPrevOut
  btc.tx_outputs.v1     key=txid:indexOut
"""

import json
import logging

from confluent_kafka import Producer

logger = logging.getLogger(__name__)

TOPIC_BLOCKS = "btc.blocks.v1"
TOPIC_TRANSACTIONS = "btc.transactions.v1"
TOPIC_TX_INPUTS = "btc.tx_inputs.v1"
TOPIC_TX_OUTPUTS = "btc.tx_outputs.v1"


class BlockKafkaProducer:
    """Publishes normalized block records to Kafka topics."""

    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "max.in.flight.requests.per.connection": 5,
            "retries": 5,
            "linger.ms": 100,
        })
        self._delivery_failures = 0

    def _delivery_callback(self, err, msg):
        if err is not None:
            self._delivery_failures += 1
            logger.error("Delivery failed for %s [%s]: %s",
                         msg.topic(), msg.key(), err)
        else:
            logger.debug("Delivered to %s [%d] @ offset %d",
                         msg.topic(), msg.partition(), msg.offset())

    def _produce(self, topic: str, key: str, value: dict):
        self.producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=json.dumps(value).encode("utf-8"),
            callback=self._delivery_callback,
        )

    def publish_block(self, normalized: dict) -> int:
        """Publish all records from a normalized block to Kafka.

        Args:
            normalized: Output from normalizer.normalize_block()

        Returns:
            Total number of records published.
        """
        count = 0
        self._delivery_failures = 0

        # Block record
        block = normalized["block"]
        self._produce(TOPIC_BLOCKS, block["block_hash"], block)
        count += 1

        # Transaction records
        for tx in normalized["transactions"]:
            self._produce(TOPIC_TRANSACTIONS, tx["txid"], tx)
            count += 1

        # Input records
        for inp in normalized["tx_inputs"]:
            key = f"{inp['txid']}:{inp['hashPrevOut']}:{inp['indexPrevOut']}"
            self._produce(TOPIC_TX_INPUTS, key, inp)
            count += 1

        # Output records
        for out in normalized["tx_outputs"]:
            key = f"{out['txid']}:{out['indexOut']}"
            self._produce(TOPIC_TX_OUTPUTS, key, out)
            count += 1

        # Flush to ensure all records are delivered
        self.producer.flush(timeout=30)

        if self._delivery_failures > 0:
            raise RuntimeError(
                f"Failed to deliver {self._delivery_failures} records to Kafka"
            )

        return count

    def close(self):
        self.producer.flush(timeout=30)
