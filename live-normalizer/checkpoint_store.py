"""Checkpoint store — persists last processed block for restart recovery.

Stores the last finalized block height/hash and ZMQ sequence so the
normalizer can resume from where it left off after a restart.

V3 plan Section 17.9.
"""

import json
import os
import logging

logger = logging.getLogger(__name__)

DEFAULT_PATH = "/local-scratch4/bitcoin_2025/bitcoin-realtime/live-normalizer/checkpoint.json"


class CheckpointStore:
    """File-based checkpoint for the normalizer's processing state."""

    def __init__(self, path: str = DEFAULT_PATH):
        self.path = path
        self.state = {
            "current_mode": "idle",          # idle, catchup, live
            "last_finalized_height": -1,
            "last_finalized_hash": "",
            "last_seen_zmq_seq": -1,
            "updated_at": "",
        }
        self._load()

    def _load(self):
        if os.path.exists(self.path):
            with open(self.path, "r") as f:
                self.state.update(json.load(f))
            logger.info("Checkpoint loaded: height=%d hash=%s mode=%s",
                        self.state["last_finalized_height"],
                        self.state["last_finalized_hash"][:16] + "..." if self.state["last_finalized_hash"] else "",
                        self.state["current_mode"])

    def save(self):
        from datetime import datetime, timezone
        self.state["updated_at"] = datetime.now(timezone.utc).isoformat()
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.state, f, indent=2)
        os.replace(tmp, self.path)

    def update(self, height: int, block_hash: str, zmq_seq: int = -1,
               mode: str | None = None):
        self.state["last_finalized_height"] = height
        self.state["last_finalized_hash"] = block_hash
        if zmq_seq >= 0:
            self.state["last_seen_zmq_seq"] = zmq_seq
        if mode:
            self.state["current_mode"] = mode
        self.save()

    @property
    def last_height(self) -> int:
        return self.state["last_finalized_height"]

    @property
    def last_hash(self) -> str:
        return self.state["last_finalized_hash"]

    @property
    def mode(self) -> str:
        return self.state["current_mode"]
