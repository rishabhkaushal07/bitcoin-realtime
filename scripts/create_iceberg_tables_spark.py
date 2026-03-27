#!/usr/bin/env python3
"""Create Iceberg raw tables via PySpark + HMS.

Uses Spark SQL to create 4 Iceberg tables in the btc namespace
on MinIO via Hive Metastore.

Usage:
  python scripts/create_iceberg_tables_spark.py
  python scripts/create_iceberg_tables_spark.py --drop
"""

import argparse
import sys

from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """Create a Spark session configured for Iceberg + HMS + MinIO."""
    return (
        SparkSession.builder
        .appName("iceberg-table-setup")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.hive_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hive_catalog.type", "hive")
        .config("spark.sql.catalog.hive_catalog.uri", "thrift://localhost:9083")
        .config("spark.sql.catalog.hive_catalog.warehouse", "s3a://warehouse/")
        .config("spark.sql.catalog.hive_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.hive_catalog.s3.endpoint", "http://localhost:9000")
        .config("spark.sql.catalog.hive_catalog.s3.access-key-id", "minioadmin")
        .config("spark.sql.catalog.hive_catalog.s3.secret-access-key", "minioadmin")
        .config("spark.sql.catalog.hive_catalog.s3.path-style-access", "true")
        # Hadoop S3A config for MinIO
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Iceberg + Spark JARs
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,"
                "org.apache.hadoop:hadoop-aws:3.4.1,"
                "software.amazon.awssdk:bundle:2.29.51")
        .config("spark.sql.defaultCatalog", "hive_catalog")
        .getOrCreate()
    )


DDL_STATEMENTS = [
    # Create namespace
    "CREATE NAMESPACE IF NOT EXISTS btc",

    # blocks
    """
    CREATE TABLE IF NOT EXISTS btc.blocks (
        block_hash      STRING      COMMENT 'Block hash (64-char hex)',
        height          INT         COMMENT 'Block height',
        version         INT         COMMENT 'Block version',
        blocksize       INT         COMMENT 'Block size in bytes',
        hashPrev        STRING      COMMENT 'Previous block hash',
        hashMerkleRoot  STRING      COMMENT 'Merkle root hash',
        nTime           INT         COMMENT 'Block timestamp (Unix epoch)',
        nBits           INT         COMMENT 'Compact target (integer)',
        nNonce          INT         COMMENT 'Nonce',
        block_timestamp TIMESTAMP   COMMENT 'Block timestamp as datetime',
        observed_at     STRING      COMMENT 'UTC ISO when normalizer processed',
        ingested_at     STRING      COMMENT 'UTC ISO when published to Kafka',
        finality_status STRING      COMMENT 'OBSERVED | CONFIRMED | REORGED',
        source_seq      INT         COMMENT 'ZMQ sequence number'
    )
    USING iceberg
    PARTITIONED BY (bucket(10, height))
    TBLPROPERTIES (
        'format-version' = '2',
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'zstd'
    )
    """,

    # transactions
    """
    CREATE TABLE IF NOT EXISTS btc.transactions (
        txid            STRING      COMMENT 'Transaction ID (64-char hex)',
        hashBlock       STRING      COMMENT 'Parent block hash',
        version         INT         COMMENT 'Transaction version',
        lockTime        INT         COMMENT 'Lock time',
        block_height    INT         COMMENT 'Parent block height',
        block_timestamp TIMESTAMP   COMMENT 'Parent block timestamp',
        observed_at     STRING      COMMENT 'UTC ISO when normalizer processed',
        ingested_at     STRING      COMMENT 'UTC ISO when published to Kafka'
    )
    USING iceberg
    PARTITIONED BY (bucket(10, block_height))
    TBLPROPERTIES (
        'format-version' = '2',
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'zstd'
    )
    """,

    # tx_in
    """
    CREATE TABLE IF NOT EXISTS btc.tx_in (
        txid            STRING      COMMENT 'Parent transaction ID',
        hashPrevOut     STRING      COMMENT 'Previous output tx hash',
        indexPrevOut    INT         COMMENT 'Previous output index',
        scriptSig       STRING      COMMENT 'Input script (hex)',
        sequence        BIGINT      COMMENT 'Sequence number',
        block_hash      STRING      COMMENT 'Parent block hash',
        block_height    INT         COMMENT 'Parent block height',
        observed_at     STRING      COMMENT 'UTC ISO when normalizer processed',
        ingested_at     STRING      COMMENT 'UTC ISO when published to Kafka'
    )
    USING iceberg
    PARTITIONED BY (bucket(10, block_height))
    TBLPROPERTIES (
        'format-version' = '2',
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'zstd'
    )
    """,

    # tx_out
    """
    CREATE TABLE IF NOT EXISTS btc.tx_out (
        txid            STRING      COMMENT 'Parent transaction ID',
        indexOut         INT         COMMENT 'Output index within transaction',
        height          INT         COMMENT 'Block height',
        value           BIGINT      COMMENT 'Value in satoshis',
        scriptPubKey    STRING      COMMENT 'Output script (hex)',
        address         STRING      COMMENT 'Decoded address',
        block_hash      STRING      COMMENT 'Parent block hash',
        block_timestamp TIMESTAMP   COMMENT 'Parent block timestamp',
        observed_at     STRING      COMMENT 'UTC ISO when normalizer processed',
        ingested_at     STRING      COMMENT 'UTC ISO when published to Kafka'
    )
    USING iceberg
    PARTITIONED BY (bucket(10, height))
    TBLPROPERTIES (
        'format-version' = '2',
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'zstd'
    )
    """,

    # Set identifier fields for PyIceberg upserts
    "ALTER TABLE btc.blocks SET IDENTIFIER FIELDS block_hash",
    "ALTER TABLE btc.transactions SET IDENTIFIER FIELDS txid",
    "ALTER TABLE btc.tx_in SET IDENTIFIER FIELDS txid, hashPrevOut, indexPrevOut",
    "ALTER TABLE btc.tx_out SET IDENTIFIER FIELDS txid, indexOut",
]

DROP_STATEMENTS = [
    "DROP TABLE IF EXISTS btc.blocks",
    "DROP TABLE IF EXISTS btc.transactions",
    "DROP TABLE IF EXISTS btc.tx_in",
    "DROP TABLE IF EXISTS btc.tx_out",
]


def create_tables(drop: bool = False):
    print("Starting Spark session...")
    spark = get_spark()

    if drop:
        print("Dropping existing tables...")
        for stmt in DROP_STATEMENTS:
            print(f"  {stmt}")
            spark.sql(stmt)

    print("Creating Iceberg tables...")
    for stmt in DDL_STATEMENTS:
        label = stmt.strip().split("\n")[0].strip()
        print(f"  Executing: {label[:80]}...")
        spark.sql(stmt)

    # Verify
    print("\nVerifying tables:")
    for table in ["btc.blocks", "btc.transactions", "btc.tx_in", "btc.tx_out"]:
        df = spark.sql(f"DESCRIBE TABLE EXTENDED {table}")
        col_count = df.filter(df.col_name != "").count()
        print(f"  {table}: OK ({col_count} metadata rows)")

    spark.stop()
    print("\nAll Iceberg raw tables ready.")


def main():
    parser = argparse.ArgumentParser(description="Create Iceberg raw tables via Spark")
    parser.add_argument("--drop", action="store_true", help="Drop and recreate tables")
    args = parser.parse_args()
    create_tables(drop=args.drop)


if __name__ == "__main__":
    main()
