"""Unit tests for the Bitcoin Core RPC client."""

import sys
import os
import json
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "live-normalizer"))

from rpc_client import BitcoinRPC


class TestBitcoinRPC:
    """Test RPC client request formatting and response handling."""

    def test_call_formats_jsonrpc_payload(self, mocker):
        """Verify JSON-RPC 2.0 payload structure."""
        mock_resp = mocker.Mock()
        mock_resp.json.return_value = {"result": "abc123", "error": None}
        mock_resp.raise_for_status = mocker.Mock()
        mock_post = mocker.patch("rpc_client.requests.post", return_value=mock_resp)

        rpc = BitcoinRPC("http://localhost:8332", "user", "pass")
        result = rpc.getbestblockhash()

        call_args = mock_post.call_args
        payload = call_args.kwargs["json"]
        assert payload["jsonrpc"] == "2.0"
        assert payload["method"] == "getbestblockhash"
        assert payload["params"] == []
        assert result == "abc123"

    def test_getblock_passes_verbosity(self, mocker):
        """getblock should pass [hash, verbosity] as params."""
        mock_resp = mocker.Mock()
        mock_resp.json.return_value = {"result": {"height": 100}, "error": None}
        mock_resp.raise_for_status = mocker.Mock()
        mock_post = mocker.patch("rpc_client.requests.post", return_value=mock_resp)

        rpc = BitcoinRPC()
        rpc.getblock("abc123", 2)

        payload = mock_post.call_args.kwargs["json"]
        assert payload["params"] == ["abc123", 2]

    def test_rpc_error_raises(self, mocker):
        """RPC errors should raise RuntimeError."""
        mock_resp = mocker.Mock()
        mock_resp.json.return_value = {
            "result": None,
            "error": {"code": -5, "message": "Block not found"},
        }
        mock_resp.raise_for_status = mocker.Mock()
        mocker.patch("rpc_client.requests.post", return_value=mock_resp)

        rpc = BitcoinRPC()
        with pytest.raises(RuntimeError, match="Block not found"):
            rpc.getblockhash(999999999)

    def test_auth_credentials_passed(self, mocker):
        """Auth tuple should be passed to requests.post."""
        mock_resp = mocker.Mock()
        mock_resp.json.return_value = {"result": 100, "error": None}
        mock_resp.raise_for_status = mocker.Mock()
        mock_post = mocker.patch("rpc_client.requests.post", return_value=mock_resp)

        rpc = BitcoinRPC("http://host:8332", "myuser", "mypass")
        rpc.getblockcount()

        assert mock_post.call_args.kwargs["auth"] == ("myuser", "mypass")

    def test_timeout_set(self, mocker):
        """Requests should have a 60s timeout."""
        mock_resp = mocker.Mock()
        mock_resp.json.return_value = {"result": {}, "error": None}
        mock_resp.raise_for_status = mocker.Mock()
        mock_post = mocker.patch("rpc_client.requests.post", return_value=mock_resp)

        rpc = BitcoinRPC()
        rpc.getblockchaininfo()

        assert mock_post.call_args.kwargs["timeout"] == 60

    def test_incremental_request_ids(self, mocker):
        """Each RPC call should have a unique incremented ID."""
        mock_resp = mocker.Mock()
        mock_resp.json.return_value = {"result": 0, "error": None}
        mock_resp.raise_for_status = mocker.Mock()
        mock_post = mocker.patch("rpc_client.requests.post", return_value=mock_resp)

        rpc = BitcoinRPC()
        rpc.getblockcount()
        rpc.getblockcount()

        ids = [call.kwargs["json"]["id"] for call in mock_post.call_args_list]
        assert ids[0] < ids[1]
