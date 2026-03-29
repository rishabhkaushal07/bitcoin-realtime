-- =============================================================================
-- StarRocks Native Flat Serving Table (V3 Plan Section 17.8)
-- =============================================================================
-- This is the denormalized serving table on StarRocks BE local disks.
-- Built from the Iceberg raw layer via per-block incremental INSERT.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS bitcoin_v3;
USE bitcoin_v3;

-- ----- Flat serving table -----
-- One row per transaction with ARRAY<STRUCT> for inputs and outputs.
-- Matches the design from the original legacy batch pipeline flat_table_v1.
CREATE TABLE IF NOT EXISTS bitcoin_flat_v3 (
    -- Primary key columns first (txid + block_height for partitioning)
    txid                VARCHAR(64),
    block_height        INT,
    -- Block fields
    block_hash          VARCHAR(64),
    block_timestamp     DATETIME,
    nTime               INT,
    -- Transaction fields
    tx_version          INT,
    lockTime            INT,
    -- Aggregated inputs
    input_count         INT,
    inputs              ARRAY<STRUCT<
        hashPrevOut     VARCHAR(64),
        indexPrevOut    INT,
        scriptSig       STRING,
        sequence        BIGINT
    >>,
    -- Aggregated outputs
    output_count        INT,
    total_output_value  BIGINT COMMENT 'Sum of output values in satoshis',
    outputs             ARRAY<STRUCT<
        indexOut        INT,
        value           BIGINT,
        scriptPubKey    STRING,
        address         VARCHAR(128)
    >>,
    -- Metadata
    finality_status     VARCHAR(16)
)
ENGINE = OLAP
PRIMARY KEY (txid, block_height)
PARTITION BY RANGE (block_height) (
    -- Partitions in chunks of 100,000 blocks
    -- Add new partitions as chain grows
    PARTITION p0      VALUES [("0"),       ("100000")),
    PARTITION p100k   VALUES [("100000"),  ("200000")),
    PARTITION p200k   VALUES [("200000"),  ("300000")),
    PARTITION p300k   VALUES [("300000"),  ("400000")),
    PARTITION p400k   VALUES [("400000"),  ("500000")),
    PARTITION p500k   VALUES [("500000"),  ("600000")),
    PARTITION p600k   VALUES [("600000"),  ("700000")),
    PARTITION p700k   VALUES [("700000"),  ("800000")),
    PARTITION p800k   VALUES [("800000"),  ("900000")),
    PARTITION p900k   VALUES [("900000"),  ("1000000"))
)
DISTRIBUTED BY HASH(txid)
PROPERTIES (
    "replication_num" = "1",
    "enable_persistent_index" = "true"
);


-- =============================================================================
-- Per-Block Incremental Flat Table Insert (V3 Plan Section 17.8)
-- =============================================================================
-- Run this for each new block height H after Iceberg data is visible.
-- Replace ${BLOCK_HEIGHT} with the actual block height.
-- =============================================================================

-- INSERT INTO bitcoin_v3.bitcoin_flat_v3
-- SELECT
--     t.txid,
--     b.height AS block_height,
--     b.block_hash,
--     b.block_timestamp,
--     b.nTime,
--     t.version AS tx_version,
--     t.lockTime,
--     -- Inputs
--     COUNT(DISTINCT CONCAT(i.txid, ':', i.hashPrevOut, ':', CAST(i.indexPrevOut AS VARCHAR))) AS input_count,
--     ARRAY_AGG(
--         NAMED_STRUCT(
--             'hashPrevOut', i.hashPrevOut,
--             'indexPrevOut', i.indexPrevOut,
--             'scriptSig', i.scriptSig,
--             'sequence', i.sequence
--         )
--     ) AS inputs,
--     -- Outputs
--     COUNT(DISTINCT CONCAT(o.txid, ':', CAST(o.indexOut AS VARCHAR))) AS output_count,
--     SUM(o.value) AS total_output_value,
--     ARRAY_AGG(
--         NAMED_STRUCT(
--             'indexOut', o.indexOut,
--             'value', o.value,
--             'scriptPubKey', o.scriptPubKey,
--             'address', o.address
--         )
--     ) AS outputs,
--     -- Metadata
--     b.finality_status
-- FROM iceberg_catalog.btc.blocks b
-- JOIN iceberg_catalog.btc.transactions t ON t.hashBlock = b.block_hash
-- LEFT JOIN iceberg_catalog.btc.tx_in i ON i.txid = t.txid
-- LEFT JOIN iceberg_catalog.btc.tx_out o ON o.txid = t.txid
-- WHERE b.height = ${BLOCK_HEIGHT}
--   AND b.finality_status != 'REORGED'
-- GROUP BY
--     b.block_hash, b.height, b.block_timestamp, b.nTime,
--     t.txid, t.version, t.lockTime, b.finality_status;
