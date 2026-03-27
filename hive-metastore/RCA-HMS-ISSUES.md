# RCA: Hive Metastore (HMS) Issues — Root Cause Analysis

**Date:** 2026-03-27
**Service:** Apache Hive Metastore 3.1.3 (`apache/hive:3.1.3`)
**Role in pipeline:** Iceberg catalog backend — stores table metadata for PyIceberg and StarRocks

---

## Executive Summary

HMS has been the single most problematic service in the Docker Compose stack.
Over two sessions, we encountered **7 distinct issues** spanning JAR compatibility,
networking, schema initialization, Docker healthcheck behavior, and JVM startup
time. Five have been fully resolved; two are operational nuisances (slow startup,
sticky healthcheck) that require awareness but no code fix.

```
+=========================================================================+
|                  HMS ISSUE TIMELINE                                      |
+=========================================================================+
|                                                                         |
|  Issue #1: Hive 4.2.0 Thrift incompatible with PyIceberg               |
|    Fix: Downgrade to Hive 3.1.3                                        |
|                                                                         |
|  Issue #2: IOStatisticsSource ClassNotFoundException                    |
|    Fix: Downgrade hadoop-aws to 3.1.0 + matching AWS SDK               |
|                                                                         |
|  Issue #3: Schema init failure on container restart                     |
|    Fix: IS_RESUME=true environment variable                             |
|                                                                         |
|  Issue #4: Container-to-container networking broken                     |
|    Fix: Remove stale MicroK8s/Calico iptables + zombie bridges         |
|                                                                         |
|  Issue #5: HMS marked "unhealthy" permanently by Docker                 |
|    Workaround: Restart HMS after it fully starts                        |
|                                                                         |
|  Issue #6: JVM startup takes 2-5 minutes (appears stuck)               |
|    Status: Expected behavior, not a bug                                 |
|                                                                         |
|  Issue #7: Connection pool errors after MySQL restart                   |
|    Fix: Restart HMS after MySQL is fully healthy                        |
|                                                                         |
+=========================================================================+
```

---

## Issue #1: Hive 4.2.0 Thrift API Incompatible with PyIceberg

### Symptom
PyIceberg `HiveCatalog` fails to connect to Hive Metastore 4.2.0 with Thrift
protocol errors. The Thrift interface changed between Hive 3.x and 4.x.

### Root Cause
Hive 4.2.0 uses a newer Thrift API version that PyIceberg's built-in Thrift client
does not support. PyIceberg was built against the Hive 3.x Thrift interface.

### Fix
Downgraded from `apache/hive:4.2.0` to `apache/hive:3.1.3` in docker-compose.yml.

### Impact
- docker-compose.yml: image changed
- CLAUDE.md: version reference updated
- plan.md: version reference updated

### Prevention
When upgrading Hive, always test PyIceberg connectivity first:
```python
from pyiceberg.catalog import load_catalog
catalog = load_catalog('default', **{
    'type': 'hive', 'uri': 'thrift://localhost:9083',
    's3.endpoint': 'http://localhost:9000',
    's3.access-key-id': 'minioadmin',
    's3.secret-access-key': 'minioadmin',
})
catalog.list_namespaces()
```

---

## Issue #2: IOStatisticsSource ClassNotFoundException

### Symptom
```
java.lang.ClassNotFoundException: org.apache.hadoop.fs.statistics.IOStatisticsSource
```
HMS crashes immediately on startup with this error in the Java stack trace.

### Root Cause
The `hadoop-aws-3.4.1.jar` JAR requires the `IOStatisticsSource` class which was
introduced in Hadoop 3.3+. But Hive 3.1.3 bundles Hadoop 3.1.0, which does not
have this class. The same issue affects `aws-java-sdk-bundle-1.12.367.jar` and
`aws-sdk-bundle-2.29.51.jar`.

### Dependency Chain
```
Hive 3.1.3
  └── bundles Hadoop 3.1.0
        └── needs hadoop-aws-3.1.0.jar (NOT 3.4.1)
              └── needs aws-java-sdk-bundle-1.11.271.jar (NOT 1.12.x)
```

### Fix
Downloaded matching JARs from Maven Central:
- `hadoop-aws-3.1.0.jar` (replaces hadoop-aws-3.4.1.jar)
- `aws-java-sdk-bundle-1.11.271.jar` (replaces aws-java-sdk-bundle-1.12.367.jar)

Updated docker-compose.yml volume mounts:
```yaml
volumes:
  - ./hive-metastore/mysql-connector-j-8.4.0.jar:/opt/hive/lib/mysql-connector-j-8.4.0.jar:ro
  - ./hive-metastore/hadoop-aws-3.1.0.jar:/opt/hive/lib/hadoop-aws-3.1.0.jar:ro
  - ./hive-metastore/aws-java-sdk-bundle-1.11.271.jar:/opt/hive/lib/aws-java-sdk-bundle-1.11.271.jar:ro
  - ./hive-metastore/hive-site.xml:/opt/hive/conf/hive-site.xml:ro
```

### Files in hive-metastore/ directory

| File | Status | Purpose |
|------|--------|---------|
| `hadoop-aws-3.1.0.jar` | ACTIVE | S3A filesystem for Hadoop 3.1.0 |
| `aws-java-sdk-bundle-1.11.271.jar` | ACTIVE | AWS SDK matching hadoop-aws-3.1.0 |
| `mysql-connector-j-8.4.0.jar` | ACTIVE | MySQL JDBC driver for HMS schema |
| `hive-site.xml` | ACTIVE | S3/MinIO config + MySQL backend config |
| `hadoop-aws-3.4.1.jar` | DEPRECATED | Do NOT use — requires Hadoop 3.3+ |
| `aws-java-sdk-bundle-1.12.367.jar` | DEPRECATED | Do NOT use — wrong SDK version |
| `aws-sdk-bundle-2.29.51.jar` | DEPRECATED | Do NOT use — AWS SDK v2, incompatible |

### Prevention
**Rule:** The hadoop-aws version MUST exactly match the Hadoop version bundled in
the Hive Docker image. Check with:
```bash
docker exec hive-metastore ls /opt/hadoop/share/hadoop/common/ | grep hadoop-common
# Output: hadoop-common-3.1.0.jar  <-- this is the version to match
```

---

## Issue #3: Schema Init Failure on Container Restart

### Symptom
```
Exception in thread "main" MetaException(message:Error creating transactional
connection factory)
```
HMS crashes on restart because it tries to run `schematool -initSchema` again but
the MySQL schema already exists from the first run.

### Root Cause
The Hive 3.1.3 Docker entrypoint script runs schema initialization on every
container start. When the MySQL data is persisted (bind-mounted), the schema
already exists and the init fails with a "table already exists" error, which
cascades into a connection factory error.

### Fix
Added `IS_RESUME: "true"` environment variable to docker-compose.yml:
```yaml
hive-metastore:
  environment:
    SERVICE_NAME: metastore
    DB_DRIVER: mysql
    IS_RESUME: "true"    # <-- This maps to SKIP_SCHEMA_INIT=true
```

The Hive entrypoint script checks this variable and skips schema initialization
when set to "true".

### How the Entrypoint Works
```
entrypoint.sh:
  if [ "$IS_RESUME" == "true" ]; then
    SKIP_SCHEMA_INIT=true    # Skip schematool
  fi

  if [ "$SKIP_SCHEMA_INIT" != "true" ]; then
    schematool -initSchema   # This fails if tables exist
  fi
```

### When to Set IS_RESUME=false
Only set to false (or remove) when:
1. You intentionally wiped the HMS MySQL data directory
2. You need a fresh schema initialization

### Prevention
Always keep `IS_RESUME: "true"` in docker-compose.yml. If you wipe
`/local-scratch4/bitcoin_2025/hms-mysql-data/`, temporarily set to false for the
first start, then set back to true.

---

## Issue #4: Container-to-Container Networking Broken

### Symptom
```
java.net.SocketTimeoutException: connect timed out
```
HMS cannot connect to MySQL (`mysql-hms:3306`). No container can reach any other
container on the Docker bridge network. Host-to-container traffic works fine.

### Root Cause (Multi-layered)

**Layer 1: Stale Calico/MicroK8s iptables-legacy rules**

MicroK8s (Kubernetes) with Calico CNI had been installed on this machine previously.
Even though MicroK8s was stopped, it left behind 389+ iptables-legacy rules.

Key problematic chains:
```
iptables-legacy FORWARD chain:
  1. cali-FORWARD (Calico main chain)
     -> cali-from-wl-dispatch
        -> DROP all (catch-all for "Unknown interface")
     -> cali-to-wl-dispatch
        -> DROP all (catch-all for "Unknown interface")
```

Docker bridge `veth` interfaces are NOT Calico workload interfaces, so they
hit the catch-all DROP rules.

**Layer 2: Dual iptables backends**

Linux kernel processes BOTH iptables-legacy and iptables-nft (nftables) chains.
Docker adds its rules to the nftables backend. Calico adds to the legacy backend.
A packet must be ACCEPTed by BOTH backends to pass. Even inserting an ACCEPT rule
at position 1 in the legacy FORWARD chain didn't fully fix it because of mark
manipulation in Calico sub-chains.

**Layer 3: Zombie bridge interfaces**

After old Docker networks were deleted, their bridge interfaces (`br-35a718ea0f57`,
`br-81bb1357e580`) remained as zombie interfaces. These had conflicting
NAT/masquerade rules for the same 172.28.0.0/16 subnet as our bitcoin-realtime
network, causing return traffic to be misrouted.

### Diagnostic Commands Used

```bash
# Check for stale Calico rules
sudo iptables-legacy-save | grep -c "cali"

# Check FORWARD chain
sudo iptables-legacy -L FORWARD -n -v --line-numbers

# Check for DROP rules in Calico chains
sudo iptables-legacy -L cali-from-wl-dispatch -n
sudo iptables-legacy -L cali-to-wl-dispatch -n

# Test connectivity between containers
sudo docker exec hive-metastore bash -c "timeout 5 bash -c 'echo > /dev/tcp/mysql-hms/3306' && echo REACHABLE || echo UNREACHABLE"

# Packet capture on bridge (shows SYN/SYN-ACK but no ACK)
sudo tcpdump -i br-4b4ed517335e -n -c 20 host 172.28.0.3

# Quick test: disable bridge-nf-call-iptables (BREAKS port forwarding)
sudo sysctl net.bridge.bridge-nf-call-iptables=0

# Check for zombie bridge interfaces
ip link show | grep "br-"
sudo docker network ls  # Compare with actual bridges

# Check bridge-nf-call setting
cat /proc/sys/net/bridge/bridge-nf-call-iptables
```

### Fix (3 steps)

```bash
# Step 1: Uninstall MicroK8s completely
sudo microk8s stop
sudo snap remove microk8s --purge

# Step 2: Flush ALL iptables-legacy rules
sudo iptables-legacy -F && sudo iptables-legacy -X
sudo iptables-legacy -t nat -F && sudo iptables-legacy -t nat -X
sudo iptables-legacy -t mangle -F && sudo iptables-legacy -t mangle -X
sudo iptables-legacy -t raw -F && sudo iptables-legacy -t raw -X

# Step 3: Remove zombie bridge interfaces
sudo ip link set br-35a718ea0f57 down && sudo ip link delete br-35a718ea0f57
sudo ip link set br-81bb1357e580 down && sudo ip link delete br-81bb1357e580

# Step 4: Restart Docker to rebuild clean iptables rules
sudo systemctl restart docker
sudo docker compose up -d
```

### How to Diagnose in the Future

If container-to-container networking breaks again:

```
Step 1: Quick smoke test
  sudo docker exec <container_a> bash -c \
    "timeout 3 bash -c 'echo > /dev/tcp/<container_b_hostname>/<port>' && echo OK || echo FAIL"

Step 2: If FAIL, check tcpdump
  sudo tcpdump -i <bridge_interface> -n -c 20 host <container_b_ip>

  Look for:
    SYN only (no SYN-ACK)     -> destination firewall dropping
    SYN + SYN-ACK (no ACK)    -> return path dropping (iptables/NAT issue)
    Nothing                    -> bridge forwarding or ARP issue

Step 3: Check iptables for interfering rules
  sudo iptables-legacy-save | grep -c "cali\|KUBE"
  # If non-zero, something is adding rules

Step 4: Check for zombie bridges
  ip link show | grep "br-"
  sudo docker network ls
  # Any bridge interface without a matching Docker network is a zombie

Step 5: Nuclear option
  sudo iptables-legacy -F && sudo iptables-legacy -X
  sudo iptables-legacy -t nat -F && sudo iptables-legacy -t nat -X
  # Remove zombie bridges
  sudo systemctl restart docker
  sudo docker compose up -d
```

### Prevention
- Do NOT install Kubernetes/MicroK8s on a machine running Docker Compose workloads
- If you must coexist, use `network_mode: host` in docker-compose.yml
- After uninstalling any Kubernetes distribution, always flush iptables-legacy

---

## Issue #5: HMS Marked "unhealthy" Permanently by Docker

### Symptom
`docker compose ps` shows HMS as "unhealthy" even though HMS is fully functional
and serving Thrift requests on port 9083.

### Root Cause
Docker health status is sticky. The healthcheck configuration is:
```yaml
healthcheck:
  test: bash -c "cat < /dev/tcp/localhost/9083"
  interval: 10s
  timeout: 5s
  retries: 10
  start_period: 120s
```

HMS JVM startup takes 2-5 minutes. The 120s `start_period` is often not enough.
Once the retries (10) are exhausted during the start_period, Docker marks the
container as "unhealthy" and this state never recovers (Docker does not re-enter
the start_period on subsequent successful checks).

### Workaround
After HMS is confirmed running, restart it:
```bash
# Wait for HMS to fully start (check logs)
sudo docker logs hive-metastore 2>&1 | grep "Started the new metaserver"

# Restart to reset healthcheck state
sudo docker restart hive-metastore
```

### Potential Fix (not yet applied)
Increase `start_period` to 300s:
```yaml
healthcheck:
  test: bash -c "cat < /dev/tcp/localhost/9083"
  interval: 15s
  timeout: 10s
  retries: 10
  start_period: 300s
```

### How to Check if HMS is Actually Running
Do NOT rely on `docker compose ps` health status. Instead:
```bash
# Method 1: Check the Hive log for the startup message
sudo docker exec hive-metastore bash -c "grep 'Started the new metaserver' /tmp/hive/hive.log"

# Method 2: Test Thrift port directly
sudo docker exec hive-metastore bash -c "cat < /dev/tcp/localhost/9083"

# Method 3: Test PyIceberg connectivity
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
```

---

## Issue #6: JVM Startup Takes 2-5 Minutes (Appears Stuck)

### Symptom
After `docker start hive-metastore`, the container logs show only:
```
2026-03-27 07:29:31: Starting Hive Metastore Server
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: ...
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
```

No further output for 2-5 minutes. Appears to be frozen or stuck.

### Root Cause
This is **normal behavior**. HMS 3.1.3 has a massive classpath (300+ JARs) and
the JVM takes 2-5 minutes to:
1. Load and verify all classes
2. Initialize HikariCP connection pools
3. Initialize DataNucleus ORM
4. Set up the ObjectStore
5. Start the Thrift server

The Docker `stdout` only shows the startup script output. The real progress is
logged to `/tmp/hive/hive.log` inside the container.

### Typical Startup Timeline
```
T+0s    Docker starts container
T+1s    Entrypoint script runs, sets env vars
T+2s    JVM starts, loads classpath
T+2s    SLF4J binding messages (last stdout output)
T+60s   HikariCP connects to MySQL
T+65s   ObjectStore initialized
T+90s   Thrift server starts on port 9083
T+120s  First healthcheck could pass (but start_period may expire)
```

### How to Monitor Progress
```bash
# Watch the real log (updates in real time)
sudo docker exec hive-metastore bash -c "tail -f /tmp/hive/hive.log"

# Key milestones to watch for:
#   "HikariPool-1 - Start completed"    -> MySQL connected
#   "Initialized ObjectStore"           -> Schema loaded
#   "Started the new metaserver"        -> Thrift server running
```

### Do NOT Do
- Do NOT `docker restart` HMS because it seems stuck — it's just slow
- Do NOT increase JVM heap unless you have evidence of OOM
- Do NOT reduce healthcheck timeouts — they need to be generous

---

## Issue #7: Connection Pool Errors After MySQL Restart

### Symptom
```
com.zaxxer.hikari.pool.HikariPool$PoolInitializationException:
  Failed to initialize pool: Communications link failure
```

### Root Cause
If MySQL restarts or is temporarily unavailable while HMS is starting, HikariCP
fails to create the initial connection pool. Hive 3.1.3 does not retry pool
initialization — it crashes.

### Fix
Ensure MySQL is fully healthy before HMS starts. Docker Compose `depends_on`
with `condition: service_healthy` handles this for initial startup:
```yaml
hive-metastore:
  depends_on:
    mysql-hms:
      condition: service_healthy
```

But if MySQL restarts while HMS is running, HMS will eventually lose its
connections and may need to be restarted manually.

### Prevention
- Never restart `mysql-hms` without also restarting `hive-metastore`
- If MySQL data is wiped, must restart both services

---

## Configuration Reference

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
    IS_RESUME: "true"    # Skip schema init on restart
    SERVICE_OPTS: >-
      -Djavax.jdo.option.ConnectionDriverName=com.mysql.cj.jdbc.Driver
      -Djavax.jdo.option.ConnectionURL=jdbc:mysql://mysql-hms:3306/metastore_db?createDatabaseIfNotExist=true&useSSL=false&allowPublicKeyRetrieval=true
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
    interval: 10s
    timeout: 5s
    retries: 10
    start_period: 120s
```

### hive-site.xml
```xml
<configuration>
    <property>
        <name>hive.metastore.warehouse.dir</name>
        <value>s3a://warehouse/</value>
    </property>
    <property>
        <name>fs.s3a.endpoint</name>
        <value>http://minio:9000</value>
    </property>
    <property>
        <name>fs.s3a.access.key</name>
        <value>minioadmin</value>
    </property>
    <property>
        <name>fs.s3a.secret.key</name>
        <value>minioadmin</value>
    </property>
    <property>
        <name>fs.s3a.path.style.access</name>
        <value>true</value>
    </property>
    <property>
        <name>fs.s3a.impl</name>
        <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
    </property>
</configuration>
```

---

## Important Paths Inside the Container

| Path | Purpose |
|------|---------|
| `/tmp/hive/hive.log` | **Primary log file** (NOT stdout) |
| `/opt/hive/lib/` | JAR directory (our custom JARs mounted here) |
| `/opt/hive/conf/hive-site.xml` | Config file (mounted from host) |
| `/opt/hadoop/share/hadoop/common/` | Bundled Hadoop JARs (version reference) |
| `/tmp/hadoop-unjar*/` | Temp directories created during startup (normal) |

---

## Quick Diagnostic Playbook

```
HMS won't start?
  |
  +-- Check stdout: sudo docker logs hive-metastore 2>&1 | tail -30
  |
  +-- Is it "Communications link failure"?
  |     YES --> MySQL unreachable
  |              +-- Check MySQL: sudo docker exec mysql-hms mysql -u root -padmin -e "SELECT 1"
  |              +-- Check networking: sudo docker exec hive-metastore bash -c \
  |              |     "timeout 3 bash -c 'echo > /dev/tcp/mysql-hms/3306' && echo OK || echo FAIL"
  |              +-- If FAIL --> See Issue #4 (networking)
  |              +-- If OK --> Restart HMS (MySQL may have been briefly down)
  |
  +-- Is it "ClassNotFoundException"?
  |     YES --> JAR mismatch. See Issue #2.
  |              Check: sudo docker exec hive-metastore ls /opt/hive/lib/ | grep -E "hadoop-aws|aws"
  |
  +-- Is it "table already exists" or "schema already exists"?
  |     YES --> IS_RESUME not set. See Issue #3.
  |              Check: sudo docker inspect hive-metastore | grep IS_RESUME
  |
  +-- Just SLF4J messages and nothing else for >5 min?
  |     +-- Check internal log: sudo docker exec hive-metastore tail -20 /tmp/hive/hive.log
  |     +-- If "Started the new metaserver" --> It's running! Docker healthcheck is wrong.
  |     +-- If no log file exists yet --> JVM still loading, wait 2-5 min.
  |     +-- If error in log --> Read the error, likely a JAR or config issue.
  |
  +-- Shows "unhealthy" in docker compose ps?
        +-- Check Thrift port: sudo docker exec hive-metastore \
        |     bash -c "cat < /dev/tcp/localhost/9083"
        +-- If port open --> HMS is fine. Docker healthcheck is sticky. See Issue #5.
        +-- If port closed --> Check internal log for errors.
```

---

## Expected Future Issues

| Scenario | Expected Behavior | Mitigation |
|----------|-------------------|------------|
| Docker daemon restart | All containers restart; HMS may come up before MySQL | `depends_on: condition: service_healthy` handles it, but HMS may need a second restart if MySQL was slow |
| Machine reboot | Same as Docker restart + possible stale FE metadata | Follow full startup procedure in main README |
| Hive version upgrade | JAR compatibility will break | Must re-match hadoop-aws + aws-sdk versions to new bundled Hadoop |
| MinIO endpoint change | HMS can't reach S3 | Update both `hive-site.xml` and HMS SERVICE_OPTS |
| MySQL data directory wiped | HMS schema gone | Set `IS_RESUME: "false"` for one start, then back to `"true"` |
| Disk failure / read-only remount | Docker stops, containers die | After disk recovery: restart Docker, follow startup procedure |
| Network changes (VPN, firewall) | May affect bridge networking | Use diagnostic playbook above; check `bridge-nf-call-iptables` |

---

## Summary of All Fixes Applied

| Fix # | Issue | Change | File(s) Modified |
|------:|-------|--------|-----------------|
| 1 | Hive 4.2.0 Thrift incompatible | Downgraded to 3.1.3 | `docker-compose.yml`, `CLAUDE.md`, `plan.md` |
| 2 | IOStatisticsSource ClassNotFound | Replaced JARs with 3.1.0-compatible versions | `docker-compose.yml`, downloaded `hadoop-aws-3.1.0.jar` + `aws-java-sdk-bundle-1.11.271.jar` |
| 3 | Schema init failure on restart | Added `IS_RESUME: "true"` | `docker-compose.yml` |
| 4a | Calico iptables blocking Docker | Uninstalled MicroK8s, flushed legacy iptables | System-level (not in repo) |
| 4b | Zombie bridge interfaces | Deleted stale bridges, restarted Docker | System-level (not in repo) |
| 5 | Sticky unhealthy status | Manual restart after HMS is running | Operational procedure (documented) |
| 6 | Slow JVM startup | No fix needed; documented expected behavior | This RCA document |
| 7 | Connection pool after MySQL restart | Ensure MySQL healthy before HMS starts | `docker-compose.yml` (`depends_on`) |
