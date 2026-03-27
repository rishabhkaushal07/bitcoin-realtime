#!/usr/bin/env python3
"""Create Iceberg raw tables in HMS via PyIceberg.

Creates the btc namespace and 4 raw tables:
  - btc.blocks
  - btc.transactions
  - btc.tx_in
  - btc.tx_out

Usage:
  python scripts/create_iceberg_tables.py
  python scripts/create_iceberg_tables.py --drop  # drop and recreate
"""

import argparse
import sys

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    NestedField,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import BucketTransform
from pyiceberg.table.sorting import SortOrder


CATALOG_CONFIG = {
    "type": "hive",
    "uri": "thrift://localhost:9083",
    "s3.endpoint": "http://localhost:9000",
    "s3.access-key-id": "minioadmin",
    "s3.secret-access-key": "minioadmin",
    "s3.region": "us-east-1",
    "warehouse": "s3://warehouse/",
    "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
}

NAMESPACE = "btc"


# --- Schema definitions matching V3 plan and create_raw_tables.sql ---

BLOCKS_SCHEMA = Schema(
    NestedField(1, "block_hash", StringType(), required=True),
    NestedField(2, "height", IntegerType(), required=True),
    NestedField(3, "version", IntegerType()),
    NestedField(4, "blocksize", IntegerType()),
    NestedField(5, "hashPrev", StringType()),
    NestedField(6, "hashMerkleRoot", StringType()),
    NestedField(7, "nTime", IntegerType()),
    NestedField(8, "nBits", IntegerType()),
    NestedField(9, "nNonce", IntegerType()),
    NestedField(10, "block_timestamp", TimestampType()),
    NestedField(11, "observed_at", StringType()),
    NestedField(12, "ingested_at", StringType()),
    NestedField(13, "finality_status", StringType()),
    NestedField(14, "source_seq", IntegerType()),
    identifier_field_ids=[1],  # block_hash
)

TRANSACTIONS_SCHEMA = Schema(
    NestedField(1, "txid", StringType(), required=True),
    NestedField(2, "hashBlock", StringType()),
    NestedField(3, "version", IntegerType()),
    NestedField(4, "lockTime", IntegerType()),
    NestedField(5, "block_height", IntegerType()),
    NestedField(6, "block_timestamp", TimestampType()),
    NestedField(7, "observed_at", StringType()),
    NestedField(8, "ingested_at", StringType()),
    identifier_field_ids=[1],  # txid
)

TX_IN_SCHEMA = Schema(
    NestedField(1, "txid", StringType(), required=True),
    NestedField(2, "hashPrevOut", StringType(), required=True),
    NestedField(3, "indexPrevOut", IntegerType(), required=True),
    NestedField(4, "scriptSig", StringType()),
    NestedField(5, "sequence", LongType()),
    NestedField(6, "block_hash", StringType()),
    NestedField(7, "block_height", IntegerType()),
    NestedField(8, "observed_at", StringType()),
    NestedField(9, "ingested_at", StringType()),
    identifier_field_ids=[1, 2, 3],  # txid + hashPrevOut + indexPrevOut
)

TX_OUT_SCHEMA = Schema(
    NestedField(1, "txid", StringType(), required=True),
    NestedField(2, "indexOut", IntegerType(), required=True),
    NestedField(3, "height", IntegerType()),
    NestedField(4, "value", LongType()),
    NestedField(5, "scriptPubKey", StringType()),
    NestedField(6, "address", StringType()),
    NestedField(7, "block_hash", StringType()),
    NestedField(8, "block_timestamp", TimestampType()),
    NestedField(9, "observed_at", StringType()),
    NestedField(10, "ingested_at", StringType()),
    identifier_field_ids=[1, 2],  # txid + indexOut
)


TABLE_PROPERTIES = {
    "format-version": "2",
    "write.format.default": "parquet",
    "write.parquet.compression-codec": "zstd",
}

TABLES = {
    "blocks": {
        "schema": BLOCKS_SCHEMA,
        "partition_field": PartitionField(
            source_id=2, field_id=1000, transform=BucketTransform(10), name="height_bucket"
        ),
    },
    "transactions": {
        "schema": TRANSACTIONS_SCHEMA,
        "partition_field": PartitionField(
            source_id=5, field_id=1000, transform=BucketTransform(10), name="block_height_bucket"
        ),
    },
    "tx_in": {
        "schema": TX_IN_SCHEMA,
        "partition_field": PartitionField(
            source_id=7, field_id=1000, transform=BucketTransform(10), name="block_height_bucket"
        ),
    },
    "tx_out": {
        "schema": TX_OUT_SCHEMA,
        "partition_field": PartitionField(
            source_id=3, field_id=1000, transform=BucketTransform(10), name="height_bucket"
        ),
    },
}


def create_tables(drop: bool = False):
    print("Connecting to Iceberg catalog (HMS)...")
    catalog = load_catalog("hive", **CATALOG_CONFIG)

    # Create namespace
    try:
        catalog.create_namespace(NAMESPACE)
        print(f"  Created namespace: {NAMESPACE}")
    except Exception as e:
        if "already exists" in str(e).lower() or "AlreadyExistsException" in str(e):
            print(f"  Namespace {NAMESPACE} already exists")
        else:
            raise

    for table_name, config in TABLES.items():
        full_name = f"{NAMESPACE}.{table_name}"

        if drop:
            try:
                catalog.drop_table(full_name)
                print(f"  Dropped table: {full_name}")
            except Exception:
                pass

        try:
            partition_spec = PartitionSpec(config["partition_field"])
            catalog.create_table(
                identifier=full_name,
                schema=config["schema"],
                partition_spec=partition_spec,
                properties=TABLE_PROPERTIES,
            )
            print(f"  Created table: {full_name}")
        except Exception as e:
            if "already exists" in str(e).lower() or "AlreadyExistsException" in str(e):
                print(f"  Table {full_name} already exists")
            else:
                raise

    # Verify
    print("\nVerifying tables:")
    for table_name in TABLES:
        full_name = f"{NAMESPACE}.{table_name}"
        table = catalog.load_table(full_name)
        print(f"  {full_name}: {len(table.schema().fields)} columns, "
              f"format-version={table.metadata.format_version}")

    print("\nAll Iceberg raw tables ready.")


def main():
    parser = argparse.ArgumentParser(description="Create Iceberg raw tables")
    parser.add_argument("--drop", action="store_true", help="Drop and recreate tables")
    args = parser.parse_args()
    create_tables(drop=args.drop)


if __name__ == "__main__":
    main()
