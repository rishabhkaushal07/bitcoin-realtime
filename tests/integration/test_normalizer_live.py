"""Integration tests for the live normalizer against Bitcoin Core RPC.

Prerequisites: Bitcoin Core running with RPC enabled.
These tests query real RPC data — they require IBD to have progressed past
at least block 170.

Run with: pytest tests/integration/test_normalizer_live.py -m integration
"""

import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "live-normalizer"))

pytestmark = pytest.mark.integration

from rpc_client import BitcoinRPC
from normalizer import normalize_block


@pytest.fixture(scope="module")
def rpc():
    """Connect to local Bitcoin Core RPC."""
    client = BitcoinRPC(
        url="http://127.0.0.1:8332",
        user="bitcoinrpc",
        password="changeme_strong_password_here",
    )
    # Verify connection
    try:
        info = client.getblockchaininfo()
        if info["blocks"] < 170:
            pytest.skip("Bitcoin Core has not synced past block 170 yet")
    except Exception as e:
        pytest.skip(f"Bitcoin Core RPC not available: {e}")
    return client


class TestGenesisBlockLive:
    """Test normalization of the real genesis block."""

    def test_genesis_block(self, rpc):
        block_hash = rpc.getblockhash(0)
        rpc_block = rpc.getblock(block_hash, 2)
        result = normalize_block(rpc_block)

        assert result["block"]["height"] == 0
        assert result["block"]["hashPrev"] == "0" * 64
        assert len(result["transactions"]) == 1
        assert len(result["tx_inputs"]) == 1
        assert len(result["tx_outputs"]) == 1
        assert result["tx_outputs"][0]["value"] == 5_000_000_000


class TestBlock170Live:
    """Test normalization of block 170 — first real transaction."""

    def test_block_170_structure(self, rpc):
        block_hash = rpc.getblockhash(170)
        rpc_block = rpc.getblock(block_hash, 2)
        result = normalize_block(rpc_block)

        assert result["block"]["height"] == 170
        assert len(result["transactions"]) == 2
        # 1 coinbase + 1 regular
        assert len(result["tx_inputs"]) == 2
        # 1 coinbase output + 2 regular outputs
        assert len(result["tx_outputs"]) == 3

    def test_first_btc_transfer_value(self, rpc):
        """The famous 10 BTC pizza-predecessor transfer."""
        block_hash = rpc.getblockhash(170)
        rpc_block = rpc.getblock(block_hash, 2)
        result = normalize_block(rpc_block)

        values = sorted([o["value"] for o in result["tx_outputs"]])
        assert 1_000_000_000 in values   # 10 BTC
        assert 4_000_000_000 in values   # 40 BTC change
        assert 5_000_000_000 in values   # 50 BTC coinbase


class TestBlockChainContinuity:
    """Verify block hash chain continuity over a range."""

    def test_first_100_blocks_chain(self, rpc):
        """Each block's hashPrev should match the previous block's hash."""
        prev_hash = None
        for h in range(0, min(101, rpc.getblockcount() + 1)):
            block_hash = rpc.getblockhash(h)
            rpc_block = rpc.getblock(block_hash, 2)
            result = normalize_block(rpc_block)

            if h == 0:
                assert result["block"]["hashPrev"] == "0" * 64
            else:
                assert result["block"]["hashPrev"] == prev_hash, \
                    f"Block {h}: hashPrev mismatch"

            prev_hash = result["block"]["block_hash"]
