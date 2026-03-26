"""ZMQ listener — receives block notifications from Bitcoin Core.

Subscribes to Bitcoin Core's ZMQ interface for real-time block notifications.
On each new block, triggers RPC fetch + normalization + Kafka publish.

V3 plan Section 5: ZMQ is the doorbell, RPC is the clerk at the desk.
"""

import logging
import struct

import zmq

logger = logging.getLogger(__name__)


class ZMQListener:
    """Listens to Bitcoin Core ZMQ for new block notifications."""

    def __init__(self, zmq_url: str = "tcp://127.0.0.1:28332"):
        """
        Args:
            zmq_url: ZMQ endpoint for hashblock notifications.
        """
        self.zmq_url = zmq_url
        self.context = zmq.Context()
        self.socket = None
        self.last_seq = -1

    def connect(self):
        """Connect and subscribe to hashblock topic."""
        self.socket = self.context.socket(zmq.SUB)
        self.socket.setsockopt(zmq.RCVHWM, 0)  # no message drop
        self.socket.setsockopt_string(zmq.SUBSCRIBE, "hashblock")
        self.socket.connect(self.zmq_url)
        logger.info("ZMQ connected to %s, subscribed to hashblock", self.zmq_url)

    def receive(self) -> tuple[str, int]:
        """Block until a new block hash is received.

        Returns:
            (block_hash_hex, sequence_number)
        """
        msg = self.socket.recv_multipart()
        # ZMQ message: [topic, body, sequence]
        topic = msg[0].decode("utf-8")
        body = msg[1]
        seq = struct.unpack("<I", msg[2])[0]

        # Detect missed messages
        if self.last_seq >= 0 and seq != self.last_seq + 1:
            gap = seq - self.last_seq - 1
            logger.warning("ZMQ sequence gap detected: expected %d, got %d (missed %d)",
                           self.last_seq + 1, seq, gap)

        self.last_seq = seq

        # hashblock body is 32 bytes (block hash in binary, little-endian)
        block_hash = body.hex()
        logger.info("ZMQ %s: hash=%s seq=%d", topic, block_hash, seq)

        return block_hash, seq

    def close(self):
        if self.socket:
            self.socket.close()
        self.context.term()
