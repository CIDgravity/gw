# Test Cluster Setup (Scalable S3 Architecture)

This directory contains configuration to run a test cluster with the **scalable S3 architecture**:
- **S3 Frontend Proxy** (stateless) - Routes S3 requests to storage nodes
- **2 Kuri Storage Nodes** (kuri-1, kuri-2) - Storage backends with separate ports
- **1 YugabyteDB** - Shared database for coordination and object placement tracking

**Note:** This cluster uses **host network mode** to avoid Docker proxy bottlenecks during high-throughput testing.

## Architecture

```
                    Clients
                       │
                       │ S3 API requests
                       ▼
              ┌─────────────────┐
              │  S3 Frontend    │  Stateless proxy
              │     Proxy       │  Round-robin writes, YCQL lookup for reads
              │    :8078        │
              └────────┬────────┘
                       │
          ┌────────────┴────────────┐
          │                         │
          ▼                         ▼
   ┌─────────────┐           ┌─────────────┐
   │   kuri-1    │           │   kuri-2    │
   │ S3 :8079    │           │ S3 :8080    │
   │ Web :9010   │           │ Web :9011   │
   │ CAR :7001   │           │ CAR :7002   │
   └──────┬──────┘           └──────┬──────┘
          │                         │
          └────────────┬────────────┘
                       │
              ┌────────┴────────┐
              │   YugabyteDB    │
              │  CQL :9042      │
              │  SQL :5433      │
              └─────────────────┘
```

**Host Network Mode:** All services bind directly to localhost ports (no Docker proxy overhead).

**Components:**

1. **S3 Frontend Proxy** (`:8078`)
   - Stateless load balancer
   - Round-robin distribution for PUT requests
   - YCQL lookup to route GET requests to correct Kuri node
   - No local storage

2. **Kuri Storage Nodes**
   - **kuri-1**: S3 API `:8079`, WebUI `:9010`, LocalWeb `:7001`
   - **kuri-2**: S3 API `:8080`, WebUI `:9011`, LocalWeb `:7002`
   - Store objects and make Filecoin deals
   - LocalWeb servers for CAR file staging (for Filecoin SP downloads)

3. **YugabyteDB**
   - CQL `:9042`, SQL `:5433`
   - Shared metadata for S3 object routing
   - Per-node keyspaces for RIBS data

**How it works:**
1. Client sends S3 request to Frontend Proxy (`:8078`)
2. **PUT**: Proxy round-robins to a Kuri node, which stores object and records `node_id` in YCQL
3. **GET**: Proxy queries YCQL for object's `node_id`, routes to correct Kuri node
4. Each Kuri node independently makes Filecoin deals using its own LocalWeb endpoint
5. Web UI aggregates cluster state from YCQL

**Port Allocation (Host Network Mode):**
| Port | Service | Description |
|------|---------|-------------|
| 8078 | S3 Proxy | Public S3 API endpoint |
| 8079 | kuri-1 | Internal S3 API |
| 8080 | kuri-2 | Internal S3 API |
| 7001 | kuri-1 | LocalWeb (CAR files) |
| 7002 | kuri-2 | LocalWeb (CAR files) |
| 9010 | kuri-1 | Web UI |
| 9011 | kuri-2 | Web UI |
| 9042 | YugabyteDB | CQL |
| 5433 | YugabyteDB | SQL |

## Prerequisites

The S3 Frontend Proxy binary is built automatically when you build the Docker image. No manual build step required.

## Quick Start

### 1. Build the Docker Image

```bash
# From the project root directory
docker build . -t fgw:local
```

### 2. Generate Configuration

```bash
cd test-cluster
./gen-config.sh /data/fgw-test
```

This creates separate configs for each Kuri node with their own LocalWeb URLs.

### 3. Start the Cluster

```bash
./start.sh /data/fgw-test
```

Or with a custom domain for external access:
```bash
./gen-config.sh /data/fgw-test fgw.example.com
./start.sh /data/fgw-test
```

### 4. Verify Everything is Running

```bash
# Set your data directory
export FGW_DATA_DIR=/data/fgw-test

# Check all containers
docker-compose ps

# Check logs
./logs.sh /data/fgw-test

# Check YugabyteDB
docker-compose exec yugabyte bin/ysqlsh -h yugabyte -U yugabyte -c "SELECT 1"
```

### 5. Access the Services

| Service | URL | Description |
|---------|-----|-------------|
| S3 API | http://localhost:8078 | Frontend Proxy (routes to Kuri nodes) |
| Web UI | http://localhost:9010/webui | Cluster monitoring |
| Cluster Monitor | http://localhost:9010/webui/cluster | New cluster view |

### 6. Configure S3 Client

```bash
# Using rclone
cat > ~/.config/rclone/rclone.conf << EOF
[fgw-test]
type = s3
provider = Other
access_key_id = test
secret_access_key = test
region = us-east-1
endpoint = http://localhost:8078
acl = private
EOF

# Test connection
rclone lsd fgw-test:
```

## Testing the Scalable Architecture

### Test 1: Round-Robin Write Distribution

```bash
# Upload multiple objects - they should be distributed across kuri-1 and kuri-2
for i in {1..10}; do
  echo "test data $i" | rclone rcat fgw-test:bucket/object-$i
done

# Check WebUI cluster view to see distribution
```

### Test 2: Read Routing via YCQL

```bash
# Read back objects - requests should be routed to correct nodes
rclone cat fgw-test:bucket/object-1
rclone cat fgw-test:bucket/object-5
```

### Test 3: Node Failure Simulation

```bash
export FGW_DATA_DIR=/data/fgw-test

# Stop kuri-1
docker-compose stop kuri-1

# Writes should continue (routed to kuri-2)
# Reads of objects on kuri-1 will fail until node recovers

# Restart node
docker-compose start kuri-1
```

## Configuration

### Per-Node Configs

Each Kuri node has its own configuration:
- `config/kuri-1/settings.env` - EXTERNAL_LOCALWEB_URL=http://localhost:7001
- `config/kuri-2/settings.env` - EXTERNAL_LOCALWEB_URL=http://localhost:7002

### NAT/Reverse Proxy Setup

For production with external access:

```bash
# Generate configs with your domain
./gen-config.sh /data/fgw-test fgw.example.com
```

This creates:
- kuri-1: `EXTERNAL_LOCALWEB_URL=http://fgw.example.com:7001`
- kuri-2: `EXTERNAL_LOCALWEB_URL=http://fgw.example.com:7002`

**Configure your reverse proxy:**
- `fgw.example.com:8078` → host:8078 (S3 API)
- `fgw.example.com:7001` → host:7001 (kuri-1 CAR files)
- `fgw.example.com:7002` → host:7002 (kuri-2 CAR files)
- `fgw.example.com:9010` → host:9010 (Web UI)

## Helper Scripts

| Script | Purpose |
|--------|---------|
| `./start.sh <dir>` | Start the cluster |
| `./stop.sh <dir>` | Stop the cluster |
| `./stop.sh <dir> --clean` | Stop and remove all data |
| `./test.sh <dir>` | Test cluster functionality |
| `./logs.sh <dir> [service]` | View logs |
| `./gen-config.sh <dir> [domain]` | Generate per-node configurations |

## Cleanup

### Stop the cluster (preserve data):
```bash
./stop.sh /data/fgw-test
```

### Stop and remove all data:
```bash
./stop.sh /data/fgw-test --clean
```

## Architecture Notes

**Why separate Frontend Proxy?**
- Stateless - can scale horizontally by adding more proxy instances
- No data stored locally - pure routing layer
- Can be restarted without affecting stored data

**Why separate LocalWeb per Kuri node?**
- Each node stages its own CAR files for Filecoin deals
- Storage providers download from the specific node that has the data
- Independent deal-making per node

**Object Placement Tracking:**
- YCQL `S3Objects` table tracks which `node_id` stores each object
- Frontend proxy queries this for read routing
- Enables read-after-write consistency

## Limitations

This is a test setup. For production:
1. Add TLS termination for S3 API
2. Configure proper authentication
3. Set up monitoring and alerting
4. Consider implementing additional frontend proxy instances for HA

## Support

For issues or questions:
- Check logs: `./logs.sh /data/fgw-test [yugabyte|kuri-1|kuri-2|s3-proxy]`
- Web UI: http://localhost:9010/webui
- Cluster Monitor: http://localhost:9010/webui/cluster
