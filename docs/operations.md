# Filecoin Gateway Operations Guide

This document provides comprehensive operational guidance for the Filecoin Gateway (FGW), covering metrics, logging, backup procedures, and monitoring integration.

## Table of Contents

1. [Metrics](#1-metrics)
2. [Logging](#2-logging)
3. [Backup & Restore](#3-backup--restore)
4. [Monitoring Integration](#4-monitoring-integration)

---

## 1. Metrics

The Filecoin Gateway exposes Prometheus-compatible metrics for comprehensive observability of the system.

### Overview

FGW uses the Prometheus client library to expose metrics via an HTTP endpoint. All metrics use the `fgw` namespace prefix for easy identification and filtering.

### Accessing Metrics

**Endpoint:** `http://<host>:2112/metrics`

The metrics port is configurable via the `RIBS_PROMETHEUS_PORT` environment variable (default: `2112`).

```bash
# Test metrics endpoint
curl http://localhost:2112/metrics

# Filter for FGW metrics only
curl -s http://localhost:2112/metrics | grep ^fgw_
```

### Metrics by Category

#### S3 Frontend Metrics (`fgw_s3frontend_*`)

These metrics track the S3-compatible API frontend performance.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `fgw_s3frontend_requests_total` | Counter | `method`, `status` | Total S3 frontend requests |
| `fgw_s3frontend_request_duration_seconds` | Histogram | `method` | Duration of S3 frontend requests |
| `fgw_s3frontend_requests_in_flight` | Gauge | - | Current number of requests being processed |
| `fgw_s3frontend_backend_health` | Gauge | `node_id` | Backend health status (1=healthy, 0=unhealthy) |
| `fgw_s3frontend_backend_requests_total` | Counter | `node_id`, `method` | Total requests sent to each backend |
| `fgw_s3frontend_backend_duration_seconds` | Histogram | `node_id` | Duration of requests to backends |
| `fgw_s3frontend_backend_errors_total` | Counter | `node_id`, `type` | Total errors from backends |
| `fgw_s3frontend_routing_lookups_total` | Counter | - | Total object routing lookups |
| `fgw_s3frontend_routing_lookup_errors_total` | Counter | - | Total routing lookup errors |
| `fgw_s3frontend_routing_lookup_duration_seconds` | Histogram | - | Duration of routing lookups |
| `fgw_s3frontend_routing_cache_hits_total` | Counter | - | Total routing cache hits |
| `fgw_s3frontend_routing_cache_misses_total` | Counter | - | Total routing cache misses |
| `fgw_s3frontend_backends_total` | Gauge | - | Total number of configured backends |
| `fgw_s3frontend_backends_healthy` | Gauge | - | Number of healthy backends |
| `fgw_s3frontend_backends_unhealthy` | Gauge | - | Number of unhealthy backends |
| `fgw_s3frontend_multipart_uploads_active` | Gauge | - | Number of active multipart uploads |
| `fgw_s3frontend_multipart_uploads_started_total` | Counter | - | Total multipart uploads started |
| `fgw_s3frontend_multipart_uploads_complete_total` | Counter | - | Total multipart uploads completed |
| `fgw_s3frontend_multipart_uploads_aborted_total` | Counter | - | Total multipart uploads aborted |

**Histogram Buckets:**
- Request duration: `0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30` seconds
- Routing lookup: `0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1` seconds

#### Deal Pipeline Metrics (`fgw_deals_*`)

These metrics track the Filecoin deal lifecycle.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `fgw_deals_proposed_total` | Counter | `provider` | Total deals proposed to storage providers |
| `fgw_deals_accepted_total` | Counter | `provider` | Total deals accepted by storage providers |
| `fgw_deals_rejected_total` | Counter | `provider`, `reason` | Total deals rejected by storage providers |
| `fgw_deals_published_total` | Counter | `provider` | Total deals published on chain |
| `fgw_deals_sealed_total` | Counter | `provider` | Total deals sealed by storage providers |
| `fgw_deals_failed_total` | Counter | `reason` | Total number of failed deals |
| `fgw_deals_expired_total` | Counter | - | Total number of expired deals |
| `fgw_deals_active` | Gauge | - | Current number of active (sealed) deals |
| `fgw_deals_in_progress` | Gauge | - | Current number of deals in progress |
| `fgw_deals_pending` | Gauge | - | Current number of deals pending proposal |
| `fgw_deals_proposal_duration_seconds` | Histogram | - | Time from deal creation to proposal acceptance |
| `fgw_deals_publish_duration_seconds` | Histogram | - | Time from proposal acceptance to on-chain publish |
| `fgw_deals_sealing_duration_seconds` | Histogram | - | Time from publish to seal completion |
| `fgw_deals_total_duration_seconds` | Histogram | - | Total time from deal creation to seal |
| `fgw_deals_provider_selection_duration_seconds` | Histogram | - | Time to select storage providers |
| `fgw_deals_providers_queried_total` | Counter | - | Total providers queried for deals |
| `fgw_deals_providers_selected_total` | Counter | - | Total providers selected for deals |
| `fgw_deals_groups_needing_deals` | Gauge | - | Number of groups that need more deals |
| `fgw_deals_groups_ready_for_deals` | Gauge | - | Number of groups ready to make deals |
| `fgw_deals_groups_offloaded` | Gauge | - | Number of groups offloaded to Filecoin |

**Histogram Buckets:**
- Proposal duration: `1, 5, 10, 30, 60, 120, 300, 600` seconds
- Publish duration: `60, 300, 600, 1800, 3600, 7200, 14400` seconds
- Sealing duration: `3600, 7200, 14400, 28800, 57600, 86400, 172800` seconds
- Total duration: `3600, 7200, 14400, 28800, 57600, 86400, 172800, 345600` seconds

#### Financial Metrics (`fgw_balance_*`)

These metrics track wallet, market, and datacap balances.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `fgw_balance_wallet_fil` | Gauge | - | Current wallet balance in FIL |
| `fgw_balance_market_fil` | Gauge | - | Current market escrow balance in FIL |
| `fgw_balance_datacap_bytes` | Gauge | - | Remaining datacap in bytes |
| `fgw_balance_market_topup_total` | Counter | - | Total number of market balance top-ups |
| `fgw_balance_market_topup_fil_total` | Counter | - | Total FIL amount added to market balance |
| `fgw_balance_faucet_requests_total` | Counter | `type`, `status` | Total faucet requests by type and status |
| `fgw_balance_faucet_fil_success_total` | Counter | - | Total successful FIL faucet requests |
| `fgw_balance_faucet_fil_failed_total` | Counter | - | Total failed FIL faucet requests |
| `fgw_balance_datacap_requests_total` | Counter | - | Total datacap faucet requests |
| `fgw_balance_datacap_requests_success_total` | Counter | - | Total successful datacap requests |
| `fgw_balance_datacap_requests_failed_total` | Counter | - | Total failed datacap requests |

#### Database Metrics (`fgw_sql_*`, `fgw_cql_*`)

These metrics track database performance for both SQL (PostgreSQL/YugabyteDB) and CQL (Cassandra/YugabyteDB) operations.

**SQL Metrics:**

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `fgw_sql_query_duration_seconds` | Histogram | `operation` | Duration of SQL queries in seconds |
| `fgw_sql_query_errors_total` | Counter | `operation` | Total number of SQL query errors |
| `fgw_sql_queries_total` | Counter | `operation` | Total number of SQL queries |
| `fgw_sql_active_connections` | Gauge | - | Number of active SQL connections |
| `fgw_sql_idle_connections` | Gauge | - | Number of idle SQL connections |

**CQL Metrics:**

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `fgw_cql_query_duration_seconds` | Histogram | `operation` | Duration of CQL queries in seconds |
| `fgw_cql_query_errors_total` | Counter | `operation` | Total number of CQL query errors |
| `fgw_cql_queries_total` | Counter | `operation` | Total number of CQL queries |
| `fgw_cql_batch_size` | Histogram | - | Number of statements in CQL batches |
| `fgw_cql_batch_duration_seconds` | Histogram | - | Duration of CQL batch operations |
| `fgw_cql_batch_errors_total` | Counter | - | Total number of CQL batch errors |
| `fgw_cql_active_connections` | Gauge | - | Number of active CQL connections |

**Histogram Buckets:**
- Query duration: `0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10` seconds
- Batch duration: `0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30` seconds
- Batch size: `1, 5, 10, 25, 50, 100, 250, 500, 1000` statements

#### Cache Metrics (`fgw_cache_*`)

Cache metrics are referenced in the recording rules but track multi-tier caching performance:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `fgw_cache_hits_total` | Counter | `tier` | Total cache hits by tier (l1, l2) |
| `fgw_cache_misses_total` | Counter | `tier` | Total cache misses by tier |
| `fgw_cache_requests_total` | Counter | `tier` | Total cache requests by tier |

### Prometheus Scrape Configuration

Add the following to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'fgw'
    static_configs:
      - targets: ['fgw-host:2112']
    scrape_interval: 15s
    scrape_timeout: 10s
    metrics_path: /metrics
    
  # For multiple FGW nodes
  - job_name: 'fgw-cluster'
    static_configs:
      - targets:
        - 'fgw-node1:2112'
        - 'fgw-node2:2112'
        - 'fgw-node3:2112'
    relabel_configs:
      - source_labels: [__address__]
        target_label: instance
        regex: '([^:]+):\d+'
        replacement: '${1}'
```

### Recording Rules

FGW provides pre-computed recording rules for efficient dashboard queries and alerting. Install by adding to your Prometheus configuration:

```yaml
rule_files:
  - "/etc/prometheus/rules/fgw-rules.yml"
```

**Key Recording Rules:**

| Rule | Description |
|------|-------------|
| `fgw:s3_success_rate:5m` | Overall S3 success rate (5-minute window) |
| `fgw:s3_latency_p50:5m` | P50 latency by operation |
| `fgw:s3_latency_p95:5m` | P95 latency by operation |
| `fgw:s3_latency_p99:5m` | P99 latency by operation |
| `fgw:deal_success_rate:24h` | Deal success rate (24-hour window) |
| `fgw:wallet_balance_fil:current` | Current wallet balance in FIL |
| `fgw:wallet_runway_days:current` | Projected days until wallet depletion |
| `fgw:cache_hit_rate:5m` | Cache hit rate by tier |
| `fgw:health_score:current` | Overall system health score (0-1) |

### Grafana Dashboard Import

FGW includes pre-built Grafana dashboards located in `ansible/files/dashboards/`:

| Dashboard | File | Description |
|-----------|------|-------------|
| FGW Overview | `fgw-overview.json` | High-level system health, traffic, and financials |
| FGW Deals | `fgw-deals.json` | Deal pipeline status and provider performance |
| FGW S3 SLA | `fgw-s3-sla.json` | S3 API availability and latency SLA tracking |
| FGW Storage | `fgw-storage.json` | Storage tiers and retrieval metrics |
| FGW Financials | `fgw-financials.json` | Wallet balance and spending analysis |

**Import Instructions:**

1. Open Grafana and navigate to **Dashboards > Import**
2. Upload the JSON file or paste its contents
3. Select your Prometheus data source
4. Click **Import**

All dashboards are tagged with `fgw` and include cross-linking for easy navigation.

### Alerting Rules

The recording rules file includes pre-configured alerts:

| Alert | Severity | Condition | Description |
|-------|----------|-----------|-------------|
| `FGWWalletBalanceLow` | Critical | Runway < 7 days | Wallet will be depleted soon |
| `FGWS3AvailabilityLow` | Critical | Success rate < 99.9% | S3 availability below SLA |
| `FGWS3LatencyHigh` | Warning | P95 GET > 2s | High S3 GET latency |
| `FGWCacheHitRateLow` | Warning | L1 hit rate < 50% | Low cache efficiency |
| `FGWDealSuccessRateLow` | Warning | Success rate < 90% | Deal pipeline issues |
| `FGWDatabasePoolHigh` | Warning | Pool utilization > 85% | Database connection pool exhaustion |
| `FGWDatabaseErrors` | Critical | Error rate > 0.1/s | Database errors detected |
| `FGWGCBacklogHigh` | Warning | Candidates > 1000 | Garbage collection backlog |
| `FGWNoActiveDeals` | Critical | No deals in progress for 6h | Deal pipeline stalled |
| `FGWProviderPerformanceLow` | Warning | 7-day success < 80% | Storage provider issues |

---

## 2. Logging

FGW uses structured logging based on the `go-log/v2` library, providing flexible output formats and log level control.

### Log Format Configuration

**Environment Variable:** `RIBS_LOG_FORMAT`

| Value | Description |
|-------|-------------|
| `text` | Human-readable colorized output (default) |
| `json` | Structured JSON output for log aggregation |

```bash
# Enable JSON logging for production
export RIBS_LOG_FORMAT=json
```

### JSON Log Format

When JSON logging is enabled, log entries include these fields:

```json
{
  "level": "info",
  "ts": "2024-01-15T10:30:45.123Z",
  "logger": "ribs:deals",
  "caller": "rbdeal/deal_maker.go:245",
  "msg": "Deal proposed successfully",
  "trace_id": "a1b2c3d4e5f6789012345678",
  "provider": "f01234",
  "deal_cid": "bafy..."
}
```

**Field Reference:**

| Field | Description |
|-------|-------------|
| `level` | Log level: `debug`, `info`, `warn`, `error` |
| `ts` | ISO 8601 timestamp |
| `logger` | Component name (e.g., `ribs:deals`, `ribs:s3api`) |
| `caller` | Source file and line number |
| `msg` | Log message |
| `trace_id` | Request trace ID for correlation (when available) |

### Log Level Configuration

**Environment Variable:** `RIBS_LOGLEVEL`

Set log levels globally or per-component:

```bash
# Global debug logging
export RIBS_LOGLEVEL="ribs=debug"

# Multiple component levels
export RIBS_LOGLEVEL="ribs:deals=debug,ribs:s3api=info,ribs:repair=warn"

# Common patterns
export RIBS_LOGLEVEL="ribs=info"           # All RIBS components at info
export RIBS_LOGLEVEL="ribs:deals=debug"    # Debug deal operations only
```

**Log Levels:**
- `debug`: Verbose debugging information
- `info`: Normal operational messages
- `warn`: Warning conditions
- `error`: Error conditions

### Trace ID Support

FGW includes request tracing for correlating logs across operations:

- **HTTP Header:** `X-Trace-ID` or `X-Request-ID`
- **Context Key:** `trace_id`
- **Format:** 32-character hex string

Trace IDs are:
1. Extracted from incoming request headers if present
2. Generated automatically if not provided
3. Propagated to downstream services
4. Included in response headers for debugging

```bash
# Make a request with a custom trace ID
curl -H "X-Trace-ID: my-custom-trace-123" http://localhost:8078/bucket/object

# Response includes the trace ID header
# X-Trace-ID: my-custom-trace-123
```

### Viewing Logs

**Using journalctl (systemd):**

```bash
# Follow FGW logs
journalctl -u fgw -f

# Last 100 lines
journalctl -u fgw -n 100

# Logs since specific time
journalctl -u fgw --since "1 hour ago"

# Filter by log level (requires JSON format and jq)
journalctl -u fgw -o cat | jq 'select(.level == "error")'

# Search for specific trace ID
journalctl -u fgw -o cat | jq 'select(.trace_id == "abc123...")'
```

**Using docker logs:**

```bash
# Follow container logs
docker logs -f fgw-container

# With timestamps
docker logs -f --timestamps fgw-container
```

### Log Aggregation with Promtail/Loki

Example Promtail configuration for FGW logs:

```yaml
server:
  http_listen_port: 9080
  grpc_listen_port: 0

positions:
  filename: /tmp/positions.yaml

clients:
  - url: http://loki:3100/loki/api/v1/push

scrape_configs:
  - job_name: fgw
    journal:
      labels:
        job: fgw
      path: /var/log/journal
    relabel_configs:
      - source_labels: ['__journal__systemd_unit']
        target_label: 'unit'
      - source_labels: ['__journal__systemd_unit']
        regex: 'fgw\.service'
        action: keep
    pipeline_stages:
      # Parse JSON logs
      - json:
          expressions:
            level: level
            logger: logger
            trace_id: trace_id
            msg: msg
      - labels:
          level:
          logger:
      - output:
          source: msg
```

**Loki LogQL Queries:**

```logql
# All errors from FGW
{unit="fgw.service"} | json | level="error"

# Deal-related logs
{unit="fgw.service"} | json | logger=~"ribs:deals.*"

# Trace specific request
{unit="fgw.service"} | json | trace_id="abc123..."

# Error rate (requires metrics)
sum(rate({unit="fgw.service"} | json | level="error" [5m]))
```

---

## 3. Backup & Restore

Proper backup procedures are critical for FGW operations. This section covers backup strategies for all critical components.

### Backup Overview

| Component | Criticality | Backup Method | Frequency |
|-----------|-------------|---------------|-----------|
| **Wallet** | Critical | Export + Encryption | Every 4 hours |
| **YugabyteDB (SQL)** | High | pg_dump / YSQL dump | Daily |
| **YugabyteDB (CQL)** | High | sstableloader / YCQL dump | Daily |
| **Configuration** | Medium | File backup | On change |
| **RIBS Data Directory** | Medium | Filesystem snapshot | Daily |

### Configuration Environment Variables

```bash
# Backup S3 destination
export BACKUP_S3_ENDPOINT="https://s3.amazonaws.com"
export BACKUP_S3_BUCKET="fgw-backups"
export BACKUP_S3_ACCESS_KEY="your-access-key"
export BACKUP_S3_SECRET_KEY="your-secret-key"
export BACKUP_S3_REGION="us-east-1"

# Encryption (highly recommended for wallet backups)
export BACKUP_ENCRYPTION_KEY_PATH="/etc/fgw/backup.key"

# Wallet backup settings
export BACKUP_WALLET_ENABLED="true"
export BACKUP_WALLET_INTERVAL="4h"

# Database backup settings
export BACKUP_DATABASE_ENABLED="true"
export BACKUP_DATABASE_INTERVAL="24h"
```

### Wallet Backup Procedure

The wallet contains the private key used for Filecoin transactions. **Loss of the wallet means loss of all FIL and inability to extend deals.**

#### Manual Wallet Backup

```bash
# Export wallet (produces JSON with encrypted private key)
fgw wallet export > wallet-backup-$(date +%Y%m%d).json

# Encrypt the backup (recommended)
gpg --symmetric --cipher-algo AES256 wallet-backup-$(date +%Y%m%d).json

# Upload to secure storage
aws s3 cp wallet-backup-$(date +%Y%m%d).json.gpg \
  s3://fgw-backups/wallets/

# Verify the backup
gpg --decrypt wallet-backup-$(date +%Y%m%d).json.gpg | \
  fgw wallet verify
```

#### Automated Wallet Backup

Enable automated backups in configuration:

```bash
export BACKUP_WALLET_ENABLED=true
export BACKUP_WALLET_INTERVAL=4h
export BACKUP_ENCRYPTION_KEY_PATH=/etc/fgw/backup-key.asc
```

The automated backup:
1. Exports the wallet in JSON format
2. Encrypts using the GPG key at `BACKUP_ENCRYPTION_KEY_PATH`
3. Uploads to `s3://BACKUP_S3_BUCKET/wallets/wallet-TIMESTAMP.json.gpg`
4. Retains the last 30 backups

### Database Backup Procedure

FGW uses YugabyteDB with both SQL (YSQL) and CQL (YCQL) interfaces.

#### SQL Database Backup

```bash
# Connect to YugabyteDB and dump the database
pg_dump -h ${RIBS_YUGABYTE_SQL_HOST} \
        -p ${RIBS_YUGABYTE_SQL_PORT} \
        -U ${RIBS_YUGABYTE_SQL_USER} \
        -d ${RIBS_YUGABYTE_SQL_DB} \
        -F custom \
        -f fgw-sql-backup-$(date +%Y%m%d).dump

# Compress and upload
gzip fgw-sql-backup-$(date +%Y%m%d).dump
aws s3 cp fgw-sql-backup-$(date +%Y%m%d).dump.gz \
  s3://fgw-backups/database/sql/
```

#### CQL Database Backup

```bash
# Use ycqlsh to export schema
ycqlsh ${RIBS_YUGABYTE_CQL_HOSTS} -e \
  "DESCRIBE KEYSPACE ${RIBS_YUGABYTE_CQL_KEYSPACE}" \
  > schema-backup-$(date +%Y%m%d).cql

# Export data using sstableloader or COPY commands
# For each table:
ycqlsh ${RIBS_YUGABYTE_CQL_HOSTS} -e \
  "COPY ${RIBS_YUGABYTE_CQL_KEYSPACE}.table_name TO 'table_name.csv'"

# Upload to S3
tar -czf fgw-cql-backup-$(date +%Y%m%d).tar.gz *.csv schema-backup-*.cql
aws s3 cp fgw-cql-backup-$(date +%Y%m%d).tar.gz \
  s3://fgw-backups/database/cql/
```

#### Automated Database Backup Script

```bash
#!/bin/bash
# /usr/local/bin/fgw-backup-db.sh

set -e

BACKUP_DIR="/var/backups/fgw"
DATE=$(date +%Y%m%d-%H%M%S)
S3_BUCKET="${BACKUP_S3_BUCKET}"

mkdir -p ${BACKUP_DIR}

# SQL backup
echo "Backing up SQL database..."
pg_dump -h ${RIBS_YUGABYTE_SQL_HOST} \
        -p ${RIBS_YUGABYTE_SQL_PORT} \
        -U ${RIBS_YUGABYTE_SQL_USER} \
        -d ${RIBS_YUGABYTE_SQL_DB} \
        -F custom \
        -f ${BACKUP_DIR}/sql-${DATE}.dump

# Upload SQL backup
gzip ${BACKUP_DIR}/sql-${DATE}.dump
aws s3 cp ${BACKUP_DIR}/sql-${DATE}.dump.gz \
  s3://${S3_BUCKET}/database/sql/

# CQL schema backup
echo "Backing up CQL schema..."
ycqlsh ${RIBS_YUGABYTE_CQL_HOSTS} -e \
  "DESCRIBE KEYSPACE ${RIBS_YUGABYTE_CQL_KEYSPACE}" \
  > ${BACKUP_DIR}/cql-schema-${DATE}.cql

aws s3 cp ${BACKUP_DIR}/cql-schema-${DATE}.cql \
  s3://${S3_BUCKET}/database/cql/

# Cleanup old local backups (keep 7 days)
find ${BACKUP_DIR} -type f -mtime +7 -delete

echo "Backup completed: ${DATE}"
```

### Restore Procedures

#### Wallet Restore

```bash
# Download encrypted backup
aws s3 cp s3://fgw-backups/wallets/wallet-LATEST.json.gpg .

# Decrypt
gpg --decrypt wallet-LATEST.json.gpg > wallet-restore.json

# Import wallet
fgw wallet import wallet-restore.json

# Verify wallet address matches expected
fgw wallet list

# Secure cleanup
shred -u wallet-restore.json
```

#### SQL Database Restore

```bash
# Download backup
aws s3 cp s3://fgw-backups/database/sql/sql-LATEST.dump.gz .
gunzip sql-LATEST.dump.gz

# Stop FGW service
systemctl stop fgw

# Restore database (drop and recreate)
dropdb -h ${RIBS_YUGABYTE_SQL_HOST} \
       -p ${RIBS_YUGABYTE_SQL_PORT} \
       -U ${RIBS_YUGABYTE_SQL_USER} \
       ${RIBS_YUGABYTE_SQL_DB}

createdb -h ${RIBS_YUGABYTE_SQL_HOST} \
         -p ${RIBS_YUGABYTE_SQL_PORT} \
         -U ${RIBS_YUGABYTE_SQL_USER} \
         ${RIBS_YUGABYTE_SQL_DB}

pg_restore -h ${RIBS_YUGABYTE_SQL_HOST} \
           -p ${RIBS_YUGABYTE_SQL_PORT} \
           -U ${RIBS_YUGABYTE_SQL_USER} \
           -d ${RIBS_YUGABYTE_SQL_DB} \
           sql-LATEST.dump

# Start FGW service
systemctl start fgw
```

#### CQL Database Restore

```bash
# Download backup
aws s3 cp s3://fgw-backups/database/cql/cql-schema-LATEST.cql .

# Stop FGW service
systemctl stop fgw

# Drop and recreate keyspace
ycqlsh ${RIBS_YUGABYTE_CQL_HOSTS} -e \
  "DROP KEYSPACE IF EXISTS ${RIBS_YUGABYTE_CQL_KEYSPACE}"

# Restore schema
ycqlsh ${RIBS_YUGABYTE_CQL_HOSTS} -f cql-schema-LATEST.cql

# Restore data (if CSV exports were made)
# ycqlsh ${RIBS_YUGABYTE_CQL_HOSTS} -e \
#   "COPY ${RIBS_YUGABYTE_CQL_KEYSPACE}.table_name FROM 'table_name.csv'"

# Start FGW service
systemctl start fgw
```

### Backup Validation

Regularly validate backups to ensure they can be restored:

```bash
#!/bin/bash
# /usr/local/bin/fgw-validate-backup.sh

# Test wallet backup decryption
aws s3 cp s3://${BACKUP_S3_BUCKET}/wallets/wallet-LATEST.json.gpg /tmp/
gpg --decrypt /tmp/wallet-LATEST.json.gpg > /tmp/wallet-test.json
if jq -e '.address' /tmp/wallet-test.json > /dev/null; then
  echo "Wallet backup: VALID"
else
  echo "Wallet backup: INVALID" >&2
  exit 1
fi
shred -u /tmp/wallet-test.json /tmp/wallet-LATEST.json.gpg

# Test SQL backup integrity
aws s3 cp s3://${BACKUP_S3_BUCKET}/database/sql/sql-LATEST.dump.gz /tmp/
gunzip -t /tmp/sql-LATEST.dump.gz && echo "SQL backup: VALID" || echo "SQL backup: INVALID"
rm /tmp/sql-LATEST.dump.gz

echo "Backup validation complete"
```

---

## 4. Monitoring Integration

This section covers setting up comprehensive monitoring for FGW.

### Prometheus Setup Guide

#### Installation

```bash
# Download Prometheus
wget https://github.com/prometheus/prometheus/releases/download/v2.48.0/prometheus-2.48.0.linux-amd64.tar.gz
tar xvfz prometheus-*.tar.gz
cd prometheus-*

# Create systemd service
sudo tee /etc/systemd/system/prometheus.service << EOF
[Unit]
Description=Prometheus
After=network.target

[Service]
Type=simple
ExecStart=/opt/prometheus/prometheus \
  --config.file=/etc/prometheus/prometheus.yml \
  --storage.tsdb.path=/var/lib/prometheus \
  --web.enable-lifecycle
Restart=always

[Install]
WantedBy=multi-user.target
EOF
```

#### Configuration

```yaml
# /etc/prometheus/prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  - "/etc/prometheus/rules/fgw-rules.yml"

alerting:
  alertmanagers:
    - static_configs:
        - targets: ['localhost:9093']

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']

  - job_name: 'fgw'
    static_configs:
      - targets: ['localhost:2112']
    relabel_configs:
      - target_label: instance
        replacement: 'fgw-prod-1'
```

#### Install Recording Rules

```bash
# Copy FGW recording rules
sudo mkdir -p /etc/prometheus/rules
sudo cp ansible/files/prometheus/fgw-rules.yml /etc/prometheus/rules/

# Validate configuration
promtool check config /etc/prometheus/prometheus.yml
promtool check rules /etc/prometheus/rules/fgw-rules.yml

# Reload Prometheus
curl -X POST http://localhost:9090/-/reload
```

### Grafana Dashboard Descriptions

#### FGW Overview (`fgw-overview`)

The main operational dashboard providing:
- **System Health Score**: Composite health metric (0-1)
- **S3 Availability**: Current success rate with SLA threshold
- **Wallet Runway**: Days until wallet depletion
- **Total Storage**: Aggregate stored data
- **Cache Hit Rate**: L1 cache efficiency
- **Deal Success Rate**: 24-hour deal completion rate
- **Traffic Graphs**: Request rate and latency trends
- **Storage Distribution**: Data by tier (local, staging, Filecoin)
- **Financial Summary**: Wallet balance and spend rate

#### FGW Deals (`fgw-deals`)

Deal pipeline monitoring:
- **Pipeline Overview**: Deals by state (pending, sealing, active, failed)
- **Deal Activity**: Creation and completion over time
- **Provider Performance Table**: Success rate, duration, and cost by provider
- **Deal Timing**: Duration analysis and sealing times

#### FGW S3 SLA (`fgw-s3-sla`)

S3 API performance tracking:
- **SLA Compliance**: Current status vs. 99.9% target
- **Latency Percentiles**: P50, P95, P99 for GET and PUT operations
- **Availability Over Time**: Success rate trend with SLA line
- **Error Analysis**: Error rate by type and operation
- **Throughput**: Bytes transferred per second

#### FGW Storage (`fgw-storage`)

Storage and retrieval metrics:
- **Storage by Tier**: Local, staging, and Filecoin data distribution
- **Cache Performance**: Hit rates and efficiency by tier
- **Retrieval Metrics**: Rate and latency by source
- **GC Status**: Garbage collection candidates and freed space

#### FGW Financials (`fgw-financials`)

Financial monitoring:
- **Wallet Balance**: Current and historical balance
- **Spend Analysis**: Daily and weekly spend rates
- **Cost per GB**: Storage cost trends
- **Provider Costs**: Average deal cost by provider
- **Datacap Status**: Remaining datacap and request history

### Alert Configuration

#### Alertmanager Setup

```yaml
# /etc/alertmanager/alertmanager.yml
global:
  smtp_smarthost: 'smtp.example.com:587'
  smtp_from: 'alerts@example.com'
  smtp_auth_username: 'alerts@example.com'
  smtp_auth_password: 'password'

route:
  group_by: ['alertname', 'severity']
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h
  receiver: 'fgw-team'
  routes:
    - match:
        severity: critical
      receiver: 'fgw-critical'
      repeat_interval: 1h

receivers:
  - name: 'fgw-team'
    email_configs:
      - to: 'fgw-team@example.com'
    slack_configs:
      - api_url: 'https://hooks.slack.com/services/...'
        channel: '#fgw-alerts'

  - name: 'fgw-critical'
    email_configs:
      - to: 'oncall@example.com'
    pagerduty_configs:
      - service_key: 'your-pagerduty-key'

inhibit_rules:
  - source_match:
      severity: 'critical'
    target_match:
      severity: 'warning'
    equal: ['alertname']
```

### Health Check Endpoints

FGW exposes health information through its metrics endpoint:

```bash
# Basic health check (metrics endpoint responding)
curl -sf http://localhost:2112/metrics > /dev/null && echo "healthy" || echo "unhealthy"

# Check specific metrics for health
curl -s http://localhost:2112/metrics | grep -E '^fgw_' | head -5

# Kubernetes/container health probe
# Add to your container spec:
# livenessProbe:
#   httpGet:
#     path: /metrics
#     port: 2112
#   initialDelaySeconds: 30
#   periodSeconds: 10
```

#### Custom Health Check Script

```bash
#!/bin/bash
# /usr/local/bin/fgw-health-check.sh

METRICS_URL="http://localhost:2112/metrics"

# Check metrics endpoint
if ! curl -sf ${METRICS_URL} > /dev/null; then
  echo "CRITICAL: Metrics endpoint not responding"
  exit 2
fi

# Check wallet balance (requires jq and promtool)
WALLET_BALANCE=$(curl -s ${METRICS_URL} | grep '^fgw_balance_wallet_fil' | awk '{print $2}')
if (( $(echo "$WALLET_BALANCE < 0.001" | bc -l) )); then
  echo "WARNING: Wallet balance low: ${WALLET_BALANCE} FIL"
  exit 1
fi

# Check for active deals
ACTIVE_DEALS=$(curl -s ${METRICS_URL} | grep '^fgw_deals_active ' | awk '{print $2}')
if [[ "${ACTIVE_DEALS}" == "0" ]]; then
  echo "WARNING: No active deals"
  exit 1
fi

echo "OK: FGW is healthy"
exit 0
```

### Monitoring Best Practices

1. **Alert Tuning**: Start with the provided alert thresholds and adjust based on your baseline
2. **Dashboard Rotation**: Display the Overview dashboard on a monitoring screen
3. **Trend Analysis**: Use the financials dashboard to predict wallet top-up needs
4. **Correlation**: Use trace IDs to correlate logs with metrics during incidents
5. **Capacity Planning**: Monitor `fgw:wallet_runway_days:current` for proactive wallet management
6. **Provider Health**: Regularly review provider performance and remove underperformers

---

## Quick Reference

### Essential Commands

```bash
# Check FGW status
systemctl status fgw

# View recent logs
journalctl -u fgw -n 100

# Check metrics
curl localhost:2112/metrics | grep fgw_

# Wallet balance
curl -s localhost:2112/metrics | grep fgw_balance_wallet_fil

# Active deals
curl -s localhost:2112/metrics | grep fgw_deals_active
```

### Environment Variables Summary

| Variable | Default | Description |
|----------|---------|-------------|
| `RIBS_PROMETHEUS_PORT` | `2112` | Metrics endpoint port |
| `RIBS_LOG_FORMAT` | `text` | Log format (text/json) |
| `RIBS_LOGLEVEL` | - | Log level configuration |
| `BACKUP_WALLET_ENABLED` | `false` | Enable wallet backup |
| `BACKUP_DATABASE_ENABLED` | `false` | Enable database backup |
| `BACKUP_S3_BUCKET` | - | S3 bucket for backups |
| `BACKUP_ENCRYPTION_KEY_PATH` | - | GPG key for wallet encryption |

### Key Metrics to Watch

| Metric | Warning | Critical |
|--------|---------|----------|
| `fgw:s3_success_rate:5m` | < 99.9% | < 99% |
| `fgw:wallet_runway_days:current` | < 30 days | < 7 days |
| `fgw:l1_cache_efficiency:5m` | < 50% | < 25% |
| `fgw:deal_success_rate:24h` | < 90% | < 75% |
| `fgw:db_pool_utilization:current` | > 85% | > 95% |
