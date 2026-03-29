-- =============================================================================
-- Iceberg Raw Table Definitions (V3 Plan Section 10.3-10.4)
-- =============================================================================
-- These are the source-of-truth tables living in Iceberg on MinIO.
-- Created via Spark SQL (PySpark session connected to HMS).
-- Partitioned by block height ranges (buckets of 100,000 blocks).
-- =============================================================================

-- ----- blocks -----
CREATE TABLE IF NOT EXISTS btc.blocks (
    -- Historical core columns (match rusty-blockparser CSV)
    block_hash      STRING      COMMENT 'Block hash (64-char hex)',
    height          INT         COMMENT 'Block height',
    version         INT         COMMENT 'Block version',
    blocksize       INT         COMMENT 'Block size in bytes',
    hashPrev        STRING      COMMENT 'Previous block hash',
    hashMerkleRoot  STRING      COMMENT 'Merkle root hash',
    nTime           INT         COMMENT 'Block timestamp (Unix epoch)',
    nBits           BIGINT      COMMENT 'Compact target (integer, not hex)',
    nNonce          BIGINT      COMMENT 'Nonce',
    -- Live/derived metadata columns
    block_timestamp TIMESTAMP   COMMENT 'Block timestamp as datetime (from nTime)',
    observed_at     STRING      COMMENT 'UTC ISO timestamp when normalizer processed this block',
    ingested_at     STRING      COMMENT 'UTC ISO timestamp when published to Kafka',
    finality_status STRING      COMMENT 'OBSERVED | CONFIRMED | REORGED',
    source_seq      INT         COMMENT 'ZMQ sequence number (NULL for historical)'
)
USING iceberg
PARTITIONED BY (bucket(10, height))
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd'
);

-- Set identifier fields for merge/upsert operations (PyIceberg)
ALTER TABLE btc.blocks SET IDENTIFIER FIELDS block_hash;


-- ----- transactions -----
CREATE TABLE IF NOT EXISTS btc.transactions (
    -- Historical core columns
    txid            STRING      COMMENT 'Transaction ID (64-char hex)',
    hashBlock       STRING      COMMENT 'Parent block hash',
    version         INT         COMMENT 'Transaction version',
    lockTime        BIGINT      COMMENT 'Lock time',
    -- Live/derived metadata columns
    block_height    INT         COMMENT 'Parent block height',
    block_timestamp TIMESTAMP   COMMENT 'Parent block timestamp as datetime',
    observed_at     STRING      COMMENT 'UTC ISO timestamp when normalizer processed',
    ingested_at     STRING      COMMENT 'UTC ISO timestamp when published to Kafka'
)
USING iceberg
PARTITIONED BY (bucket(10, block_height))
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd'
);

ALTER TABLE btc.transactions SET IDENTIFIER FIELDS txid;


-- ----- tx_in -----
CREATE TABLE IF NOT EXISTS btc.tx_in (
    -- Historical core columns
    txid            STRING      COMMENT 'Parent transaction ID',
    hashPrevOut     STRING      COMMENT 'Previous output tx hash (64 zeros for coinbase)',
    indexPrevOut    BIGINT      COMMENT 'Previous output index (4294967295 for coinbase)',
    scriptSig       STRING      COMMENT 'Input script (hex)',
    sequence        BIGINT      COMMENT 'Sequence number',
    -- Live/derived metadata columns
    block_hash      STRING      COMMENT 'Parent block hash',
    block_height    INT         COMMENT 'Parent block height',
    observed_at     STRING      COMMENT 'UTC ISO timestamp when normalizer processed',
    ingested_at     STRING      COMMENT 'UTC ISO timestamp when published to Kafka'
)
USING iceberg
PARTITIONED BY (bucket(10, block_height))
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd'
);

ALTER TABLE btc.tx_in SET IDENTIFIER FIELDS txid, hashPrevOut, indexPrevOut;


-- ----- tx_out -----
CREATE TABLE IF NOT EXISTS btc.tx_out (
    -- Historical core columns
    txid            STRING      COMMENT 'Parent transaction ID',
    indexOut         INT         COMMENT 'Output index within transaction',
    height          INT         COMMENT 'Block height',
    value           BIGINT      COMMENT 'Value in satoshis',
    scriptPubKey    STRING      COMMENT 'Output script (hex)',
    address         STRING      COMMENT 'Decoded address (empty for non-standard)',
    -- Live/derived metadata columns
    block_hash      STRING      COMMENT 'Parent block hash',
    block_timestamp TIMESTAMP   COMMENT 'Parent block timestamp as datetime',
    observed_at     STRING      COMMENT 'UTC ISO timestamp when normalizer processed',
    ingested_at     STRING      COMMENT 'UTC ISO timestamp when published to Kafka'
)
USING iceberg
PARTITIONED BY (bucket(10, height))
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd'
);

ALTER TABLE btc.tx_out SET IDENTIFIER FIELDS txid, indexOut;
