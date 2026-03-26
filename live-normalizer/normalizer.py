"""Canonical normalizer — maps Bitcoin Core RPC block JSON to four record types.

Takes the nested JSON from getblock(hash, 2) and produces flat records
matching the rusty-blockparser schema, plus live metadata fields.

Field mapping reference: V3 plan Section 8.8
"""

from datetime import datetime, timezone
from decimal import Decimal


def btc_to_satoshis(btc_value) -> int:
    """Convert BTC float from RPC to satoshis integer safely.

    Uses Decimal via str() to avoid IEEE 754 floating-point precision loss.
    See V3 plan Section 8.9.
    """
    return int(Decimal(str(btc_value)) * Decimal("100000000"))


def normalize_block(rpc_block: dict, source_seq: int | None = None) -> dict:
    """Normalize a single RPC block JSON into four lists of records.

    Args:
        rpc_block: Response from getblock(hash, 2)
        source_seq: ZMQ sequence number that triggered this fetch (optional)

    Returns:
        {
            "block": dict,
            "transactions": [dict, ...],
            "tx_inputs": [dict, ...],
            "tx_outputs": [dict, ...],
        }
    """
    now_utc = datetime.now(timezone.utc).isoformat()

    block_hash = rpc_block["hash"]
    height = rpc_block["height"]
    block_time = rpc_block["time"]
    block_timestamp = datetime.utcfromtimestamp(block_time).strftime("%Y-%m-%d %H:%M:%S")

    # --- Block record ---
    # RPC "bits" is hex string, rusty-blockparser stores as integer
    nbits = int(rpc_block["bits"], 16)

    # Genesis block has no previousblockhash
    hash_prev = rpc_block.get("previousblockhash", "0" * 64)

    block_record = {
        "schema_version": 1,
        "block_hash": block_hash,
        "height": height,
        "version": rpc_block["version"],
        "blocksize": rpc_block["size"],
        "hashPrev": hash_prev,
        "hashMerkleRoot": rpc_block["merkleroot"],
        "nTime": block_time,
        "nBits": nbits,
        "nNonce": rpc_block["nonce"],
        "block_timestamp": block_timestamp,
        "observed_at": now_utc,
        "ingested_at": now_utc,
        "finality_status": "OBSERVED",
        "source_seq": source_seq,
    }

    # --- Transaction, input, output records ---
    transactions = []
    tx_inputs = []
    tx_outputs = []

    for tx in rpc_block["tx"]:
        txid = tx["txid"]

        # Transaction record
        transactions.append({
            "schema_version": 1,
            "txid": txid,
            "hashBlock": block_hash,
            "version": tx["version"],
            "lockTime": tx["locktime"],
            "block_height": height,
            "block_timestamp": block_timestamp,
            "observed_at": now_utc,
            "ingested_at": now_utc,
        })

        # Input records
        for vin in tx["vin"]:
            if "coinbase" in vin:
                # Coinbase transaction — preserve historical conventions (V3 Section 7.4)
                tx_inputs.append({
                    "schema_version": 1,
                    "txid": txid,
                    "hashPrevOut": "0" * 64,
                    "indexPrevOut": 4294967295,
                    "scriptSig": vin["coinbase"],
                    "sequence": vin["sequence"],
                    "block_hash": block_hash,
                    "block_height": height,
                    "observed_at": now_utc,
                    "ingested_at": now_utc,
                })
            else:
                tx_inputs.append({
                    "schema_version": 1,
                    "txid": txid,
                    "hashPrevOut": vin["txid"],
                    "indexPrevOut": vin["vout"],
                    "scriptSig": vin.get("scriptSig", {}).get("hex", ""),
                    "sequence": vin["sequence"],
                    "block_hash": block_hash,
                    "block_height": height,
                    "observed_at": now_utc,
                    "ingested_at": now_utc,
                })

        # Output records
        for vout in tx["vout"]:
            tx_outputs.append({
                "schema_version": 1,
                "txid": txid,
                "indexOut": vout["n"],
                "height": height,
                "value": btc_to_satoshis(vout["value"]),
                "scriptPubKey": vout["scriptPubKey"].get("hex", ""),
                "address": vout["scriptPubKey"].get("address", ""),
                "block_hash": block_hash,
                "block_timestamp": block_timestamp,
                "observed_at": now_utc,
                "ingested_at": now_utc,
            })

    return {
        "block": block_record,
        "transactions": transactions,
        "tx_inputs": tx_inputs,
        "tx_outputs": tx_outputs,
    }
