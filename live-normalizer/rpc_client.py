"""Bitcoin Core RPC client.

Thin wrapper around Bitcoin Core's JSON-RPC interface.
Used by the normalizer to fetch decoded block data via getblock(hash, 2).
"""

import json
import requests


class BitcoinRPC:
    """Minimal Bitcoin Core RPC client."""

    def __init__(self, url: str = "http://127.0.0.1:8332",
                 user: str = "bitcoinrpc",
                 password: str = "changeme_strong_password_here"):
        self.url = url
        self.auth = (user, password)
        self._id = 0

    def _call(self, method: str, params: list = None) -> dict:
        self._id += 1
        payload = {
            "jsonrpc": "2.0",
            "id": self._id,
            "method": method,
            "params": params or [],
        }
        resp = requests.post(self.url, json=payload, auth=self.auth, timeout=60)
        resp.raise_for_status()
        result = resp.json()
        if result.get("error"):
            raise RuntimeError(f"RPC error: {result['error']}")
        return result["result"]

    def getbestblockhash(self) -> str:
        return self._call("getbestblockhash")

    def getblock(self, blockhash: str, verbosity: int = 2) -> dict:
        """Fetch a fully decoded block. verbosity=2 includes decoded txs."""
        return self._call("getblock", [blockhash, verbosity])

    def getblockhash(self, height: int) -> str:
        return self._call("getblockhash", [height])

    def getblockchaininfo(self) -> dict:
        return self._call("getblockchaininfo")

    def getblockcount(self) -> int:
        return self._call("getblockcount")
