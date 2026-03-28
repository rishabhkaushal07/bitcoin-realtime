# Hive Metastore

Apache Hive Metastore 3.1.3 — serves as the Iceberg catalog backend for the pipeline. Stores table metadata (schemas, partitions, properties) that PyIceberg and StarRocks use to locate and read Iceberg data files on MinIO.

```
+=========================================================================+
|                     HIVE METASTORE IN THE PIPELINE                      |
+=========================================================================+
|                                                                         |
|  Writers (create/update table metadata):                                |
|                                                                         |
|  +--------------------+      +----------------------+                   |
|  | PyIceberg Writer   |      | PyIceberg Finality   |                   |
|  | (iceberg_writer.py)|      | (finality_updater.py)|                   |
|  +--------+-----------+      +-----------+----------+                   |
|           |                              |                              |
|           +--- Thrift (port 9083) -------+                              |
|           |                                                             |
|           v                                                             |
|  +-------------------------------+                                      |
|  |    Hive Metastore 3.1.3       |                                      |
|  |    (apache/hive:3.1.3)        |                                      |
|  |                               |                                      |
|  |  +-------------------------+  |      +-------------------------+     |
|  |  | Thrift Server           |  |      | MySQL 8.4.8             |     |
|  |  | Port 9083               |<------->| (mysql-hms container)   |     |
|  |  | Protocol: Hive 3.x     |  | JDBC  | Port 3306 (internal)   |     |
|  |  +-------------------------+  |      | Port 3307 (host)       |     |
|  |                               |      |                         |     |
|  |  +-------------------------+  |      | Stores:                 |     |
|  |  | S3A Filesystem          |  |      |  - Table schemas        |     |
|  |  | hadoop-aws-3.1.0        |<------->|  - Partition specs      |     |
|  |  | aws-sdk-1.11.271        |  | S3A  |  - Table properties     |     |
|  |  +-------------------------+  |      |  - Namespace metadata   |     |
|  +-------------------------------+      +-------------------------+     |
|           |                                                             |
|           +--- Thrift (port 9083) -------+                              |
|           |                              |                              |
|           v                              v                              |
|  +--------------------+      +----------------------+                   |
|  | scripts/           |      | StarRocks FE         |                   |
|  | create_iceberg_    |      | (external catalog)   |                   |
|  | tables.py          |      | iceberg_catalog_v3   |                   |
|  +--------------------+      +----------------------+                   |
|                                                                         |
|  Readers (query table metadata):                                        |
|                                                                         |
|  +--------------------+                                                 |
|  | MinIO (S3)         |<-- HMS tells clients where data files live      |
|  | s3://warehouse/    |    (e.g., s3a://warehouse/btc.db/blocks/...)    |
|  +--------------------+                                                 |
+=========================================================================+
```

---

## Status: COMPLETE (Phase 1b)

HMS is fully operational as the Iceberg catalog backend. All 4 Iceberg tables
are registered and accessible via PyIceberg and StarRocks external catalog.

---

## Why Hive Metastore?

| Alternative | Why Not |
|-------------|---------|
| Nessie catalog | Planned in V3 architecture but adds complexity; HMS is simpler for Phase 1 |
| REST catalog | PyIceberg support is newer; HMS is battle-tested |
| JDBC catalog | Would need another database; HMS already has MySQL |
| Glue catalog | AWS-only; we use MinIO (on-prem S3) |

HMS was chosen because:
1. Native PyIceberg support via `HiveCatalog`
2. Native StarRocks support via `iceberg.catalog.type = hive`
3. Simple Docker deployment with MySQL backend
4. Well-documented in the Iceberg ecosystem

---

## Version History and Why 3.1.3

```
Original plan:  Hive 4.2.0
  |
  +-- Problem: Hive 4.x changed Thrift API, breaks PyIceberg
  |
  v
Downgraded to:  Hive 3.1.3
  |
  +-- Compatible with PyIceberg Thrift client
  +-- Bundles Hadoop 3.1.0
  +-- Requires matching JARs (see below)
```

| Version | Status | Why |
|---------|--------|-----|
| `apache/hive:4.2.0` | REJECTED | Thrift API incompatible with PyIceberg |
| `apache/hive:3.1.3` | ACTIVE | PyIceberg compatible, stable, well-tested |

---

## JAR Dependencies

HMS 3.1.3 bundles Hadoop 3.1.0. All S3/MinIO JARs must match this version.

```
Dependency Chain:
  Hive 3.1.3
    └── bundles Hadoop 3.1.0
          └── needs hadoop-aws-3.1.0.jar
                └── needs aws-java-sdk-bundle-1.11.271.jar
```

### Files in This Directory

| File | Version | Status | Purpose |
|------|---------|--------|---------|
| `hadoop-aws-3.1.0.jar` | 3.1.0 | ACTIVE | S3A filesystem for Hadoop 3.1.0 |
| `aws-java-sdk-bundle-1.11.271.jar` | 1.11.271 | ACTIVE | AWS SDK matching hadoop-aws-3.1.0 |
| `mysql-connector-j-8.4.0.jar` | 8.4.0 | ACTIVE | MySQL 8.4 JDBC driver |
| `hive-site.xml` | - | ACTIVE | S3/MinIO + MySQL config |
| `RCA-HMS-ISSUES.md` | - | ACTIVE | Root cause analysis of all HMS issues |
| `hadoop-aws-3.4.1.jar` | 3.4.1 | DEPRECATED | Requires Hadoop 3.3+ (ClassNotFoundException) |
| `aws-java-sdk-bundle-1.12.367.jar` | 1.12.367 | DEPRECATED | Wrong AWS SDK version |
| `aws-sdk-bundle-2.29.51.jar` | 2.29.51 | DEPRECATED | AWS SDK v2, incompatible |

### JAR Compatibility Rule

**The hadoop-aws version MUST exactly match the Hadoop version bundled in Hive.**

To verify the bundled version:
```bash
sudo docker exec hive-metastore ls /opt/hadoop/share/hadoop/common/ | grep hadoop-common
# Output: hadoop-common-3.1.0.jar
```

---

## Configuration

### hive-site.xml

```xml
<configuration>
    <!-- MinIO as Iceberg warehouse -->
    <property>
        <name>hive.metastore.warehouse.dir</name>
        <value>s3a://warehouse/</value>
    </property>

    <!-- S3/MinIO connectivity -->
    <property><name>fs.s3a.endpoint</name><value>http://minio:9000</value></property>
    <property><name>fs.s3a.access.key</name><value>minioadmin</value></property>
    <property><name>fs.s3a.secret.key</name><value>minioadmin</value></property>
    <property><name>fs.s3a.path.style.access</name><value>true</value></property>
    <property><name>fs.s3a.impl</name><value>org.apache.hadoop.fs.s3a.S3AFileSystem</value></property>

    <!-- MySQL backend for metadata -->
    <property><name>javax.jdo.option.ConnectionDriverName</name><value>com.mysql.cj.jdbc.Driver</value></property>
    <property><name>javax.jdo.option.ConnectionURL</name>
        <value>jdbc:mysql://mysql-hms:3306/metastore_db?createDatabaseIfNotExist=true&amp;useSSL=false&amp;allowPublicKeyRetrieval=true</value>
    </property>
    <property><name>javax.jdo.option.ConnectionUserName</name><value>hive</value></property>
    <property><name>javax.jdo.option.ConnectionPassword</name><value>hive</value></property>
</configuration>
```

### docker-compose.yml (HMS section)

```yaml
hive-metastore:
  image: apache/hive:3.1.3
  container_name: hive-metastore
  hostname: hive-metastore
  ports:
    - "9083:9083"    # Thrift
  environment:
    SERVICE_NAME: metastore
    DB_DRIVER: mysql
    IS_RESUME: "true"              # Skip schema init on restart
    SERVICE_OPTS: >-               # MySQL JDBC connection params
      -Djavax.jdo.option.ConnectionDriverName=com.mysql.cj.jdbc.Driver
      -Djavax.jdo.option.ConnectionURL=jdbc:mysql://mysql-hms:3306/metastore_db?...
      -Djavax.jdo.option.ConnectionUserName=hive
      -Djavax.jdo.option.ConnectionPassword=hive
  volumes:
    - ./hive-metastore/mysql-connector-j-8.4.0.jar:/opt/hive/lib/mysql-connector-j-8.4.0.jar:ro
    - ./hive-metastore/hadoop-aws-3.1.0.jar:/opt/hive/lib/hadoop-aws-3.1.0.jar:ro
    - ./hive-metastore/aws-java-sdk-bundle-1.11.271.jar:/opt/hive/lib/aws-java-sdk-bundle-1.11.271.jar:ro
    - ./hive-metastore/hive-site.xml:/opt/hive/conf/hive-site.xml:ro
  depends_on:
    mysql-hms:
      condition: service_healthy
  healthcheck:
    test: bash -c "cat < /dev/tcp/localhost/9083"
    interval: 15s
    timeout: 10s
    retries: 10
    start_period: 300s
```

### Key Environment Variables

| Variable | Value | Purpose |
|----------|-------|---------|
| `SERVICE_NAME` | `metastore` | Tells entrypoint to start Thrift metastore (not HiveServer2) |
| `DB_DRIVER` | `mysql` | Selects MySQL as the schema backend |
| `IS_RESUME` | `true` | Maps to `SKIP_SCHEMA_INIT=true` — prevents re-running `schematool -initSchema` |
| `SERVICE_OPTS` | JDO connection props | Passed to JVM as `-D` flags for JDBC connection |

---

## How HMS Startup Works

```
+-------------------------------------------------------------------+
|                     HMS Startup Sequence                           |
+-------------------------------------------------------------------+
|                                                                   |
|  T+0s    Docker starts container                                  |
|  T+1s    Entrypoint script: sets env vars, checks IS_RESUME      |
|  T+2s    JVM starts, loads classpath (300+ JARs)                  |
|  T+2s    Last stdout message: SLF4J binding (appears "stuck")     |
|  T+30s   (silent: JVM loading classes)                            |
|  T+60s   HikariCP connects to MySQL                               |
|          "HikariPool-1 - Start completed" in /tmp/hive/hive.log  |
|  T+65s   DataNucleus ObjectStore initialized                      |
|  T+90s   Thrift server starts on port 9083                        |
|          "Started the new metaserver on port [9083]" in log       |
|  T+120s+ First healthcheck passes                                 |
|                                                                   |
|  NOTE: stdout only shows up to SLF4J messages.                    |
|        Real progress is in /tmp/hive/hive.log inside container.   |
+-------------------------------------------------------------------+
```

---

## Diagnostic Commands

```bash
# Check if HMS Thrift port is open
sudo docker exec hive-metastore bash -c "cat < /dev/tcp/localhost/9083"

# Watch real-time startup progress (the REAL log)
sudo docker exec hive-metastore tail -f /tmp/hive/hive.log

# Check for the "ready" message
sudo docker exec hive-metastore grep "Started the new metaserver" /tmp/hive/hive.log

# Test PyIceberg connectivity
python -c "
from pyiceberg.catalog import load_catalog
catalog = load_catalog('default', **{
    'type': 'hive', 'uri': 'thrift://localhost:9083',
    's3.endpoint': 'http://localhost:9000',
    's3.access-key-id': 'minioadmin',
    's3.secret-access-key': 'minioadmin',
})
print(catalog.list_namespaces())
"

# Check MySQL backend connectivity from inside HMS
sudo docker exec hive-metastore bash -c \
  "timeout 3 bash -c 'echo > /dev/tcp/mysql-hms/3306' && echo OK || echo FAIL"

# View registered Iceberg tables
sudo docker exec hive-metastore bash -c \
  "grep 'getTable\|createTable' /tmp/hive/hive.log | tail -10"
```

---

## Iceberg Tables Registered in HMS

| Namespace | Table | Identifier Fields | Partition |
|-----------|-------|-------------------|-----------|
| `btc` | `blocks` | `block_hash` | `bucket(10, height)` |
| `btc` | `transactions` | `txid` | `bucket(10, block_height)` |
| `btc` | `tx_in` | `txid, hashPrevOut, indexPrevOut` | `bucket(10, block_height)` |
| `btc` | `tx_out` | `txid, indexOut` | `bucket(10, height)` |

Tables created by `scripts/create_iceberg_tables.py` using PyIceberg `HiveCatalog`.

---

## Clients That Connect to HMS

| Client | Protocol | Port | Purpose |
|--------|----------|------|---------|
| PyIceberg (writer) | Thrift | 9083 | Create tables, append data, update finality |
| PyIceberg (scripts) | Thrift | 9083 | Table creation, schema queries |
| StarRocks FE | Thrift | 9083 | External Iceberg catalog metadata |
| pytest (integration) | Thrift | 9083 | Test table operations |

---

## Important Paths Inside Container

| Path | Purpose |
|------|---------|
| `/tmp/hive/hive.log` | Primary log file (NOT stdout!) |
| `/opt/hive/lib/` | JAR directory (custom JARs mounted here) |
| `/opt/hive/conf/hive-site.xml` | Config file (mounted from host) |
| `/opt/hadoop/share/hadoop/common/` | Bundled Hadoop JARs (version reference) |
| `/tmp/hadoop-unjar*/` | Temp dirs created during startup (normal, not errors) |

---

## Known Issues

7 issues have been documented with full root cause analysis, diagnostic steps,
and fixes. See [RCA-HMS-ISSUES.md](RCA-HMS-ISSUES.md) for the complete document.

| # | Issue | Status |
|---|-------|--------|
| 1 | Hive 4.2.0 Thrift incompatible with PyIceberg | Fixed (3.1.3) |
| 2 | IOStatisticsSource ClassNotFoundException | Fixed (matching JARs) |
| 3 | Schema init failure on restart | Fixed (IS_RESUME=true) |
| 4 | Container-to-container networking broken | Fixed (removed MicroK8s) |
| 5 | Docker healthcheck stuck as "unhealthy" | Mitigated (300s start_period) |
| 6 | JVM startup takes 2-5 min | Expected behavior |
| 7 | Connection pool errors after MySQL restart | Fixed (depends_on) |

---

## Data Stored in MySQL (hms-mysql-data/)

HMS stores metadata only (not actual data). The MySQL tables include:

| MySQL Table | Iceberg Concept |
|-------------|----------------|
| `TBLS` | Table names and properties |
| `SDS` | Storage descriptors (S3 locations) |
| `COLUMNS_V2` | Column schemas |
| `PARTITIONS` | Partition metadata |
| `TABLE_PARAMS` | Iceberg table properties (format-version, write.format, etc.) |
| `DBS` | Namespaces (e.g., `btc`, `default`) |

Data volume is minimal (<100 MB even for the full blockchain).

If MySQL data is wiped (`/local-scratch4/bitcoin_2025/hms-mysql-data/`):
1. Set `IS_RESUME: "false"` in docker-compose.yml
2. Restart HMS (will run `schematool -initSchema`)
3. Set `IS_RESUME: "true"` back
4. Re-run `python scripts/create_iceberg_tables.py` to recreate tables
