#!/usr/bin/env python3
"""Flat table builder — incrementally populates StarRocks flat serving table.

Reads from Iceberg raw tables via StarRocks' external catalog and inserts
denormalized rows into the native bitcoin_flat_v3 table.

Modes:
  - Single block:  python flat_table_builder.py --height 170
  - Range:         python flat_table_builder.py --from-height 0 --to-height 1000
  - Incremental:   python flat_table_builder.py --incremental

The incremental mode queries the max block_height already in the flat table
and processes all new blocks from Iceberg above that height.

Usage:
    python starrocks/flat_table_builder.py --height 170
    python starrocks/flat_table_builder.py --from-height 0 --to-height 1000
    python starrocks/flat_table_builder.py --incremental
"""

import argparse
import logging
import sys
import time

import pymysql

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("flat-table-builder")

STARROCKS_HOST = "localhost"
STARROCKS_PORT = 9030
STARROCKS_USER = "root"
STARROCKS_PASSWORD = ""

# Per-block INSERT SQL using Iceberg external catalog
INSERT_BLOCK_SQL = """
INSERT INTO bitcoin_v3.bitcoin_flat_v3
SELECT
    b.block_hash,
    b.height AS block_height,
    b.block_timestamp,
    b.nTime,
    t.txid,
    t.version AS tx_version,
    t.lockTime,
    -- Inputs
    IFNULL(ic.input_count, 0) AS input_count,
    ic.inputs,
    -- Outputs
    IFNULL(oc.output_count, 0) AS output_count,
    IFNULL(oc.total_output_value, 0) AS total_output_value,
    oc.outputs,
    -- Metadata
    b.finality_status
FROM iceberg_catalog.btc.blocks b
JOIN iceberg_catalog.btc.transactions t
    ON t.hashBlock = b.block_hash
LEFT JOIN (
    SELECT
        txid,
        COUNT(*) AS input_count,
        ARRAY_AGG(
            NAMED_STRUCT(
                'hashPrevOut', hashPrevOut,
                'indexPrevOut', indexPrevOut,
                'scriptSig', scriptSig,
                'sequence', sequence
            )
        ) AS inputs
    FROM iceberg_catalog.btc.tx_in
    WHERE block_height = %s
    GROUP BY txid
) ic ON ic.txid = t.txid
LEFT JOIN (
    SELECT
        txid,
        COUNT(*) AS output_count,
        SUM(value) AS total_output_value,
        ARRAY_AGG(
            NAMED_STRUCT(
                'indexOut', indexOut,
                'value', value,
                'scriptPubKey', scriptPubKey,
                'address', address
            )
        ) AS outputs
    FROM iceberg_catalog.btc.tx_out
    WHERE height = %s
    GROUP BY txid
) oc ON oc.txid = t.txid
WHERE b.height = %s
  AND b.finality_status != 'REORGED'
"""

# Query to find max height already in flat table
MAX_HEIGHT_SQL = """
SELECT IFNULL(MAX(block_height), -1) FROM bitcoin_v3.bitcoin_flat_v3
"""

# Query to find max height in Iceberg blocks table
ICEBERG_MAX_HEIGHT_SQL = """
SELECT IFNULL(MAX(height), -1) FROM iceberg_catalog.btc.blocks
WHERE finality_status != 'REORGED'
"""


def get_connection():
    """Connect to StarRocks via MySQL protocol."""
    return pymysql.connect(
        host=STARROCKS_HOST,
        port=STARROCKS_PORT,
        user=STARROCKS_USER,
        password=STARROCKS_PASSWORD,
        database="bitcoin_v3",
        autocommit=True,
    )


def build_block(conn, height: int) -> int:
    """Insert one block's denormalized data. Returns rows inserted."""
    with conn.cursor() as cur:
        cur.execute(INSERT_BLOCK_SQL, (height, height, height))
        return cur.rowcount


def build_range(conn, from_height: int, to_height: int) -> int:
    """Build flat table for a range of blocks. Returns total rows."""
    total = 0
    for h in range(from_height, to_height + 1):
        rows = build_block(conn, h)
        total += rows
        if h % 100 == 0 or h == to_height:
            log.info(f"  Height {h}: {rows} txs (total: {total})")
    return total


def build_incremental(conn) -> int:
    """Build flat table for all new blocks since last build."""
    with conn.cursor() as cur:
        cur.execute(MAX_HEIGHT_SQL)
        flat_max = cur.fetchone()[0]

        cur.execute(ICEBERG_MAX_HEIGHT_SQL)
        iceberg_max = cur.fetchone()[0]

    if iceberg_max <= flat_max:
        log.info(f"Flat table up to date (flat={flat_max}, iceberg={iceberg_max})")
        return 0

    from_h = flat_max + 1
    log.info(f"Building flat table: heights {from_h} to {iceberg_max}")
    return build_range(conn, from_h, iceberg_max)


def setup_database(conn):
    """Ensure database and catalog exist."""
    with conn.cursor() as cur:
        cur.execute("CREATE DATABASE IF NOT EXISTS bitcoin_v3")


def main():
    parser = argparse.ArgumentParser(description="Flat table builder")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--height", type=int, help="Build single block height")
    group.add_argument("--from-height", type=int, help="Start of range")
    group.add_argument("--incremental", action="store_true",
                       help="Build all new blocks since last build")
    parser.add_argument("--to-height", type=int, help="End of range (with --from-height)")
    parser.add_argument("--host", default=STARROCKS_HOST)
    parser.add_argument("--port", type=int, default=STARROCKS_PORT)
    args = parser.parse_args()

    conn = pymysql.connect(
        host=args.host,
        port=args.port,
        user=STARROCKS_USER,
        password=STARROCKS_PASSWORD,
        database="bitcoin_v3",
        autocommit=True,
    )
    setup_database(conn)

    start = time.time()

    if args.height is not None:
        log.info(f"Building flat table for block {args.height}")
        rows = build_block(conn, args.height)
        log.info(f"Inserted {rows} transactions")
    elif args.from_height is not None:
        to_h = args.to_height or args.from_height
        log.info(f"Building flat table for heights {args.from_height} to {to_h}")
        rows = build_range(conn, args.from_height, to_h)
        log.info(f"Inserted {rows} total transactions")
    elif args.incremental:
        rows = build_incremental(conn)
        log.info(f"Inserted {rows} total transactions")

    elapsed = time.time() - start
    log.info(f"Done in {elapsed:.1f}s")
    conn.close()


if __name__ == "__main__":
    main()
