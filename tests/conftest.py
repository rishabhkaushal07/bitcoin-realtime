"""Shared test fixtures for the Bitcoin real-time pipeline."""

import pytest


# ---------------------------------------------------------------------------
# Sample RPC block responses (from Bitcoin Core getblock(hash, 2))
# ---------------------------------------------------------------------------

@pytest.fixture
def genesis_block_rpc():
    """Block 0 — the genesis block. Coinbase only, no inputs from prior txs."""
    return {
        "hash": "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
        "confirmations": 900000,
        "height": 0,
        "version": 1,
        "versionHex": "00000001",
        "merkleroot": "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b",
        "time": 1231006505,
        "mediantime": 1231006505,
        "nonce": 2083236893,
        "bits": "1d00ffff",
        "difficulty": 1.0,
        "chainwork": "0000000000000000000000000000000000000000000000000000000100010001",
        "nTx": 1,
        "nextblockhash": "00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048",
        "strippedsize": 285,
        "size": 285,
        "weight": 1140,
        "tx": [
            {
                "txid": "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b",
                "hash": "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b",
                "version": 1,
                "size": 204,
                "vsize": 204,
                "weight": 816,
                "locktime": 0,
                "vin": [
                    {
                        "coinbase": "04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73",
                        "sequence": 4294967295,
                    }
                ],
                "vout": [
                    {
                        "value": 50.00000000,
                        "n": 0,
                        "scriptPubKey": {
                            "asm": "04678afdb0fe...",
                            "hex": "4104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac",
                            "type": "pubkey",
                        },
                    }
                ],
            }
        ],
    }


@pytest.fixture
def block_170_rpc():
    """Block 170 — first block with a real (non-coinbase) transaction."""
    return {
        "hash": "00000000d1145790a8694403d4063f323d499e655c83426834d4ce2f8dd4a2ee",
        "confirmations": 899830,
        "height": 170,
        "version": 1,
        "versionHex": "00000001",
        "merkleroot": "7dac2c5666815c17a3b36427de37bb9d2e2c5ccec3f8633eb91a4205cb4c10ff",
        "time": 1231731025,
        "mediantime": 1231716245,
        "nonce": 1889418792,
        "bits": "1d00ffff",
        "difficulty": 1.0,
        "chainwork": "000000000000000000000000000000000000000000000000000000ab00ab00ab",
        "previousblockhash": "000000002a22cfee1f2c846adbd12b3e183d4f97683f85dad08a79780a84bd55",
        "nTx": 2,
        "strippedsize": 490,
        "size": 490,
        "weight": 1960,
        "tx": [
            {
                "txid": "b1fea52486ce0c62bb442b530a3f0132b826c74e473d1f2c220bfa78111c5082",
                "hash": "b1fea52486ce0c62bb442b530a3f0132b826c74e473d1f2c220bfa78111c5082",
                "version": 1,
                "size": 134,
                "vsize": 134,
                "weight": 536,
                "locktime": 0,
                "vin": [
                    {
                        "coinbase": "04ffff001d0102",
                        "sequence": 4294967295,
                    }
                ],
                "vout": [
                    {
                        "value": 50.00000000,
                        "n": 0,
                        "scriptPubKey": {
                            "asm": "04...",
                            "hex": "410411db93e1dcdb8a016b49840f8c53bc1eb68a382e97b1482ecad7b148a6909a5cb2e0eaddfb84ccf9744464f82e160bfa9b8b64f9d4c03f999b8643f656b412a3ac",
                            "type": "pubkey",
                        },
                    }
                ],
            },
            {
                "txid": "f4184fc596403b9d638783cf57adfe4c75c605f6356fbc91338530e9831e9e16",
                "hash": "f4184fc596403b9d638783cf57adfe4c75c605f6356fbc91338530e9831e9e16",
                "version": 1,
                "size": 275,
                "vsize": 275,
                "weight": 1100,
                "locktime": 0,
                "vin": [
                    {
                        "txid": "0437cd7f8525ceed2324359c2d0ba26006d92d856a9c20fa0241106ee5a597c9",
                        "vout": 0,
                        "scriptSig": {
                            "asm": "304402204e45e16932...",
                            "hex": "47304402204e45e16932b8af514961a1d3a1a25fdf3f4f7732e9d624c6c61548ab5fb8cd410220181522ec8eca07de4860a4acdd12909d831cc56cbbac4622082221a8768d1d0901",
                        },
                        "sequence": 4294967295,
                    }
                ],
                "vout": [
                    {
                        "value": 10.00000000,
                        "n": 0,
                        "scriptPubKey": {
                            "asm": "04ae1a62fe09...",
                            "hex": "4104ae1a62fe09c5f51b13905f07f06b99a2f7159b2225f374cd378d71302fa28414e7aab37397f554a7df5f142c21c1b7303b8a0626f1baded5c72a704f7e6cd84cac",
                            "type": "pubkey",
                            "address": "1Q2TWHE3GMdB6BZKafqwxXtWAWgFt5Jvm3",
                        },
                    },
                    {
                        "value": 40.00000000,
                        "n": 1,
                        "scriptPubKey": {
                            "asm": "0411db93e1dc...",
                            "hex": "410411db93e1dcdb8a016b49840f8c53bc1eb68a382e97b1482ecad7b148a6909a5cb2e0eaddfb84ccf9744464f82e160bfa9b8b64f9d4c03f999b8643f656b412a3ac",
                            "type": "pubkey",
                            "address": "12cbQLTFMXRnSzktFkuoG3eHoMeFtpTu3S",
                        },
                    },
                ],
            },
        ],
    }


@pytest.fixture
def large_block_rpc():
    """Minimal block with multiple transactions for scale testing."""
    txs = []
    # Coinbase
    txs.append({
        "txid": "coinbase_tx_hash_000",
        "version": 1,
        "locktime": 0,
        "vin": [{"coinbase": "deadbeef", "sequence": 4294967295}],
        "vout": [
            {"value": 6.25, "n": 0, "scriptPubKey": {"hex": "abcd", "address": "miner_addr"}},
        ],
    })
    # Regular transactions
    for i in range(1, 50):
        txs.append({
            "txid": f"tx_hash_{i:03d}",
            "version": 2,
            "locktime": 0,
            "vin": [
                {
                    "txid": f"prev_tx_{i:03d}",
                    "vout": 0,
                    "scriptSig": {"hex": f"sig_{i:03d}"},
                    "sequence": 4294967294,
                },
                {
                    "txid": f"prev_tx_{i:03d}_b",
                    "vout": 1,
                    "scriptSig": {"hex": f"sig_{i:03d}_b"},
                    "sequence": 4294967294,
                },
            ],
            "vout": [
                {"value": 0.5, "n": 0, "scriptPubKey": {"hex": f"pk_{i:03d}_0", "address": f"addr_{i:03d}_0"}},
                {"value": 0.3, "n": 1, "scriptPubKey": {"hex": f"pk_{i:03d}_1", "address": f"addr_{i:03d}_1"}},
                {"value": 0.1, "n": 2, "scriptPubKey": {"hex": f"pk_{i:03d}_2"}},
            ],
        })

    return {
        "hash": "00000000000000000001abc123def456",
        "height": 200000,
        "version": 536870912,
        "size": 250000,
        "previousblockhash": "00000000000000000001abc123def455",
        "merkleroot": "merkle_200000",
        "time": 1700000000,
        "bits": "17034219",
        "nonce": 12345678,
        "nTx": 50,
        "tx": txs,
    }
