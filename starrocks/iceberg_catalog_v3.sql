-- =============================================================================
-- StarRocks External Iceberg Catalog (V3 Plan Section 14.5)
-- =============================================================================
-- Connects StarRocks to the Iceberg raw tables on MinIO via HMS.
-- Run this after HMS and MinIO are up and Iceberg tables are created.
-- =============================================================================

-- Create the external catalog pointing to HMS
CREATE EXTERNAL CATALOG IF NOT EXISTS iceberg_catalog
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "hive",
    "hive.metastore.uris" = "thrift://hive-metastore:9083",
    -- MinIO S3 connection
    "aws.s3.endpoint" = "http://minio:9000",
    "aws.s3.access_key" = "minioadmin",
    "aws.s3.secret_key" = "minioadmin",
    "aws.s3.enable_path_style_access" = "true",
    -- Enable data cache on BE local disks for faster repeated reads
    "enable_datacache" = "true"
);

-- Verify the catalog is working
-- SHOW DATABASES FROM iceberg_catalog;
-- SELECT COUNT(*) FROM iceberg_catalog.btc.blocks;

-- =============================================================================
-- Metadata Refresh Strategy
-- =============================================================================
-- StarRocks caches Iceberg metadata by default. After each Kafka Connect
-- commit cycle writes new data to Iceberg, run these to make it visible:
--
-- REFRESH EXTERNAL TABLE iceberg_catalog.btc.blocks;
-- REFRESH EXTERNAL TABLE iceberg_catalog.btc.transactions;
-- REFRESH EXTERNAL TABLE iceberg_catalog.btc.tx_in;
-- REFRESH EXTERNAL TABLE iceberg_catalog.btc.tx_out;
--
-- Alternatively, set a short cache TTL (trades latency for freshness):
-- ALTER CATALOG iceberg_catalog SET ("iceberg_meta_cache_ttl_sec" = "30");
