"""Unit tests for the ZMQ listener."""

import sys
import os
import struct
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "live-normalizer"))


class TestZMQListener:
    """Test ZMQ message parsing and sequence gap detection."""

    @pytest.fixture
    def mock_zmq(self, mocker):
        """Mock the zmq module."""
        mock_zmq_mod = mocker.patch.dict("sys.modules", {"zmq": mocker.MagicMock()})
        import zmq
        return zmq

    @pytest.fixture
    def listener(self, mock_zmq):
        from zmq_listener import ZMQListener
        return ZMQListener("tcp://127.0.0.1:28332")

    def test_connect_subscribes_to_hashblock(self, listener, mock_zmq):
        listener.connect()
        socket = listener.socket
        socket.setsockopt_string.assert_called()

    def test_receive_parses_block_hash(self, listener, mock_zmq):
        listener.connect()

        block_hash_bytes = bytes.fromhex(
            "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
        )
        seq_bytes = struct.pack("<I", 1)
        listener.socket.recv_multipart.return_value = [
            b"hashblock",
            block_hash_bytes,
            seq_bytes,
        ]

        block_hash, seq = listener.receive()
        assert block_hash == "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
        assert seq == 1

    def test_sequence_gap_detection(self, listener, mock_zmq, caplog):
        listener.connect()

        def make_msg(seq_num):
            return [
                b"hashblock",
                b"\x00" * 32,
                struct.pack("<I", seq_num),
            ]

        # First message: seq=0
        listener.socket.recv_multipart.return_value = make_msg(0)
        listener.receive()

        # Second message: seq=5 (gap of 4)
        listener.socket.recv_multipart.return_value = make_msg(5)
        import logging
        with caplog.at_level(logging.WARNING):
            listener.receive()

        assert any("sequence gap" in r.message.lower() for r in caplog.records)

    def test_no_gap_warning_on_sequential(self, listener, mock_zmq, caplog):
        listener.connect()

        def make_msg(seq_num):
            return [b"hashblock", b"\x00" * 32, struct.pack("<I", seq_num)]

        listener.socket.recv_multipart.return_value = make_msg(0)
        listener.receive()

        listener.socket.recv_multipart.return_value = make_msg(1)
        import logging
        with caplog.at_level(logging.WARNING):
            listener.receive()

        assert not any("gap" in r.message.lower() for r in caplog.records)
