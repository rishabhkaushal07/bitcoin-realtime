"""Unit tests for the canonical normalizer.

Tests cover:
  - Genesis block (coinbase, no previousblockhash)
  - Block 170 (first real transaction with inputs/outputs)
  - Large block (50 txs, multi-input/output)
  - BTC-to-satoshi conversion precision
  - Edge cases (empty address, non-standard scripts)
"""

import sys
import os
import pytest
from decimal import Decimal

# Add live-normalizer to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "live-normalizer"))

from normalizer import normalize_block, btc_to_satoshis


# ---------------------------------------------------------------------------
# btc_to_satoshis tests
# ---------------------------------------------------------------------------

class TestBtcToSatoshis:
    """Test BTC -> satoshi conversion via Decimal (V3 Section 8.9)."""

    def test_whole_btc(self):
        assert btc_to_satoshis(50.0) == 5_000_000_000

    def test_fractional_btc(self):
        assert btc_to_satoshis(0.001) == 100_000

    def test_one_satoshi(self):
        assert btc_to_satoshis(0.00000001) == 1

    def test_zero(self):
        assert btc_to_satoshis(0.0) == 0

    def test_large_value(self):
        """21 million BTC cap."""
        assert btc_to_satoshis(21_000_000.0) == 2_100_000_000_000_000

    def test_ieee754_trap(self):
        """0.1 + 0.2 != 0.3 in float, but our conversion must be exact."""
        assert btc_to_satoshis(0.1) == 10_000_000
        assert btc_to_satoshis(0.2) == 20_000_000
        assert btc_to_satoshis(0.3) == 30_000_000

    def test_precision_at_8_decimals(self):
        """Bitcoin has exactly 8 decimal places."""
        assert btc_to_satoshis(1.23456789) == 123_456_789

    def test_block_reward_values(self):
        """Common block rewards through halvings."""
        assert btc_to_satoshis(50.0) == 5_000_000_000       # era 1
        assert btc_to_satoshis(25.0) == 2_500_000_000       # era 2
        assert btc_to_satoshis(12.5) == 1_250_000_000       # era 3
        assert btc_to_satoshis(6.25) == 625_000_000         # era 4
        assert btc_to_satoshis(3.125) == 312_500_000        # era 5


# ---------------------------------------------------------------------------
# Genesis block normalization
# ---------------------------------------------------------------------------

class TestNormalizeGenesisBlock:
    """Test normalization of block 0 (genesis)."""

    def test_block_record(self, genesis_block_rpc):
        result = normalize_block(genesis_block_rpc)
        block = result["block"]

        assert block["height"] == 0
        assert block["block_hash"] == genesis_block_rpc["hash"]
        assert block["version"] == 1
        assert block["blocksize"] == 285
        assert block["hashPrev"] == "0" * 64  # genesis has no prev
        assert block["nNonce"] == 2083236893
        assert block["finality_status"] == "OBSERVED"
        assert block["schema_version"] == 1

    def test_single_coinbase_transaction(self, genesis_block_rpc):
        result = normalize_block(genesis_block_rpc)
        assert len(result["transactions"]) == 1

        tx = result["transactions"][0]
        assert tx["txid"] == genesis_block_rpc["tx"][0]["txid"]
        assert tx["hashBlock"] == genesis_block_rpc["hash"]
        assert tx["version"] == 1
        assert tx["lockTime"] == 0
        assert tx["block_height"] == 0

    def test_coinbase_input(self, genesis_block_rpc):
        result = normalize_block(genesis_block_rpc)
        assert len(result["tx_inputs"]) == 1

        inp = result["tx_inputs"][0]
        assert inp["hashPrevOut"] == "0" * 64
        assert inp["indexPrevOut"] == 4294967295
        assert "coinbase" not in inp  # coinbase hex goes into scriptSig
        assert len(inp["scriptSig"]) > 0
        assert inp["block_height"] == 0

    def test_coinbase_output(self, genesis_block_rpc):
        result = normalize_block(genesis_block_rpc)
        assert len(result["tx_outputs"]) == 1

        out = result["tx_outputs"][0]
        assert out["value"] == 5_000_000_000  # 50 BTC
        assert out["indexOut"] == 0
        assert out["height"] == 0

    def test_record_counts(self, genesis_block_rpc):
        result = normalize_block(genesis_block_rpc)
        assert len(result["transactions"]) == 1
        assert len(result["tx_inputs"]) == 1
        assert len(result["tx_outputs"]) == 1


# ---------------------------------------------------------------------------
# Block 170 normalization (first real transaction)
# ---------------------------------------------------------------------------

class TestNormalizeBlock170:
    """Test normalization of block 170 — first block with a real tx."""

    def test_block_record(self, block_170_rpc):
        result = normalize_block(block_170_rpc)
        block = result["block"]

        assert block["height"] == 170
        assert block["hashPrev"] == block_170_rpc["previousblockhash"]
        assert block["nBits"] == int("1d00ffff", 16)

    def test_two_transactions(self, block_170_rpc):
        result = normalize_block(block_170_rpc)
        assert len(result["transactions"]) == 2

        txids = [tx["txid"] for tx in result["transactions"]]
        assert block_170_rpc["tx"][0]["txid"] in txids
        assert block_170_rpc["tx"][1]["txid"] in txids

    def test_coinbase_plus_regular_inputs(self, block_170_rpc):
        result = normalize_block(block_170_rpc)
        # 1 coinbase input + 1 regular input = 2
        assert len(result["tx_inputs"]) == 2

        coinbase_inputs = [i for i in result["tx_inputs"] if i["hashPrevOut"] == "0" * 64]
        regular_inputs = [i for i in result["tx_inputs"] if i["hashPrevOut"] != "0" * 64]

        assert len(coinbase_inputs) == 1
        assert len(regular_inputs) == 1

        # Regular input references a previous tx
        reg = regular_inputs[0]
        assert reg["hashPrevOut"] == "0437cd7f8525ceed2324359c2d0ba26006d92d856a9c20fa0241106ee5a597c9"
        assert reg["indexPrevOut"] == 0

    def test_outputs(self, block_170_rpc):
        result = normalize_block(block_170_rpc)
        # 1 coinbase output + 2 regular outputs = 3
        assert len(result["tx_outputs"]) == 3

        values = sorted([o["value"] for o in result["tx_outputs"]])
        # 10 BTC, 40 BTC, 50 BTC (coinbase)
        assert values == [1_000_000_000, 4_000_000_000, 5_000_000_000]

    def test_address_extraction(self, block_170_rpc):
        result = normalize_block(block_170_rpc)
        addrs = [o["address"] for o in result["tx_outputs"] if o["address"]]
        assert "1Q2TWHE3GMdB6BZKafqwxXtWAWgFt5Jvm3" in addrs
        assert "12cbQLTFMXRnSzktFkuoG3eHoMeFtpTu3S" in addrs

    def test_empty_address_for_non_standard(self, block_170_rpc):
        """Coinbase output in early blocks has no address field."""
        result = normalize_block(block_170_rpc)
        coinbase_outs = [o for o in result["tx_outputs"]
                         if o["txid"] == block_170_rpc["tx"][0]["txid"]]
        assert len(coinbase_outs) == 1
        assert coinbase_outs[0]["address"] == ""


# ---------------------------------------------------------------------------
# Large block normalization
# ---------------------------------------------------------------------------

class TestNormalizeLargeBlock:
    """Test normalization with 50 transactions (1 coinbase + 49 regular)."""

    def test_transaction_count(self, large_block_rpc):
        result = normalize_block(large_block_rpc)
        assert len(result["transactions"]) == 50

    def test_input_count(self, large_block_rpc):
        result = normalize_block(large_block_rpc)
        # 1 coinbase input + 49 * 2 regular inputs = 99
        assert len(result["tx_inputs"]) == 99

    def test_output_count(self, large_block_rpc):
        result = normalize_block(large_block_rpc)
        # 1 coinbase output + 49 * 3 regular outputs = 148
        assert len(result["tx_outputs"]) == 148

    def test_all_records_have_block_height(self, large_block_rpc):
        result = normalize_block(large_block_rpc)
        for tx in result["transactions"]:
            assert tx["block_height"] == 200000
        for inp in result["tx_inputs"]:
            assert inp["block_height"] == 200000
        for out in result["tx_outputs"]:
            assert out["height"] == 200000

    def test_all_records_have_timestamps(self, large_block_rpc):
        result = normalize_block(large_block_rpc)
        block = result["block"]
        assert block["observed_at"] is not None
        assert block["ingested_at"] is not None
        for tx in result["transactions"]:
            assert tx["observed_at"] is not None


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestNormalizerEdgeCases:

    def test_source_seq_passed_through(self, genesis_block_rpc):
        result = normalize_block(genesis_block_rpc, source_seq=42)
        assert result["block"]["source_seq"] == 42

    def test_source_seq_none_default(self, genesis_block_rpc):
        result = normalize_block(genesis_block_rpc)
        assert result["block"]["source_seq"] is None

    def test_nbits_hex_to_int(self, genesis_block_rpc):
        """nBits should be converted from hex string to integer."""
        result = normalize_block(genesis_block_rpc)
        # "1d00ffff" -> 486604799
        assert result["block"]["nBits"] == 0x1d00ffff

    def test_output_missing_address_key(self):
        """Output with no 'address' in scriptPubKey (e.g., OP_RETURN)."""
        block = {
            "hash": "abc123",
            "height": 999,
            "version": 1,
            "size": 100,
            "merkleroot": "merkle",
            "time": 1700000000,
            "bits": "17034219",
            "nonce": 1,
            "tx": [
                {
                    "txid": "tx_opreturn",
                    "version": 1,
                    "locktime": 0,
                    "vin": [{"coinbase": "00", "sequence": 4294967295}],
                    "vout": [
                        {
                            "value": 0.0,
                            "n": 0,
                            "scriptPubKey": {"hex": "6a0b68656c6c6f", "type": "nulldata"},
                        }
                    ],
                }
            ],
        }
        result = normalize_block(block)
        assert result["tx_outputs"][0]["address"] == ""
        assert result["tx_outputs"][0]["value"] == 0
