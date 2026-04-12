# Filecoin-gw  
**An S3-Compatible Gateway for the Filecoin Network**  
**License:** Apache-2.0 / MIT (dual-licensed)  

Filecoin-gw is an open-source gateway that exposes a fully S3-compatible API backed by the Filecoin network.  
It allows you to try out Filecoin with zero prior knowledge using Docker, making it easy to experiment with decentralized storage.
It provides a scalable blockstore, automated Filecoin dealmaking, and familiar S3 interfaces for seamless data onboarding and retrieval.  

---

## Key Features

- **S3-Compatible Endpoint** — Works with common S3 clients and tooling  
- **Improved Data Locality & Parallelism** — Groups blocks into log-files for efficient storage and Filecoin-friendly formatting  
- **Automated Filecoin Offloading** — Converts full block groups into `.car` files, computes deal CIDs, selects storage providers, and executes deals  
- **Automatic Deal Repair** — Maintains user-defined redundancy by recreating missing or failed deals  
- **Retrieval Probing** — Ensures storage providers provide reliable retrievals  
- **Advanced Storage Provider Selection** — Reputation-based system to select the most reliable providers  
- **Web UI** — Web-based interface for managing nodes and monitoring system status  
- **Flexible Storage Backends** — Block groups can be stored on distributed filesystems or other backends  
- **High Availability & Multi-Node Support** — Group managers can run redundantly; scalable KV store for indexes  
- **Future-Ready Architecture** — Supports additional caching servers, retrieval workers, and session-aware storage drivers  

![architecture](./doc/ribsweb.png)  

---

## System Requirements

| Resource | Requirement |
|---------|-------------|
| OS | Ubuntu 24.04 |
| CPU | 8 vCPUs |
| RAM | 16 GB |
| Storage | ≥ 128 GB NVMe |

---

## Deployment

### Option 1 — Docker
```bash
apt install -y docker.io docker-compose
git clone git@github.com:CIDgravity/filecoin-gateway.git
cd filecoin-gateway

# Build the container image
docker build . -t fgw:local

# Create data directories
mkdir -p ${DATA_DIR:-./data}/{config,wallet,fgw,carstage,yb,ipfs}

# Start YugabyteDB first (the gateway depends on it being healthy)
docker compose up -d yugabyte

# Run the interactive configuration wizard (gwcfg).
# This will:
#   - Create a Filecoin wallet and back it up
#   - Request faucet funds and wait for on-chain confirmation
#   - Request DataCap
#   - Create a CIDGravity account for provider discovery
#   - Configure the staging (localweb) server URL and path
#   - Write all settings to settings.env
#
# When prompted for the staging URL, enter the public root URL that storage
# providers will use to fetch CAR files (e.g. https://your-host.example.com).
# Do not include a path component; RIBS appends the randomized CAR filename.
# When prompted for the staging path, accept the default (`<RIBS_DATA>/cardata`)
# unless you intentionally want a different persistent location.
# When asked to test the endpoint, choose No — the test server runs inside
# the container without port mapping and cannot be reached externally.
docker run -it --rm \
  --entrypoint ./gwcfg \
  -v ${DATA_DIR:-./data}/config:/app/config \
  -v ${DATA_DIR:-./data}/wallet:/root/.ribswallet \
  fgw:local -f /app/config/settings.env

# Start the gateway
docker compose up -d
```

#### LocalWeb Modes

Choose one staging mode explicitly:

- **Built-in autocert mode**
  - Keep `EXTERNAL_LOCALWEB_SERVER_TLS=true`
  - Set `EXTERNAL_LOCALWEB_URL=https://your-host.example.com`
  - Point public DNS at the gateway node
  - Leave `443:8443` published so the built-in server can answer ACME and serve CARs

- **Reverse-proxy mode**
  - Set `EXTERNAL_LOCALWEB_SERVER_TLS=false`
  - Keep `EXTERNAL_LOCALWEB_URL=https://your-host.example.com`
  - Terminate TLS in nginx, Caddy, or an ingress and forward to `127.0.0.1:8443`
  - Keep the gateway's raw `8443` listener private; the default compose file binds it to localhost only

CAR download auth for the built-in LocalWeb server uses randomized staged CAR filenames as capability URLs. There is no shared JWT secret to distribute.

#### Production Checklist

- Wallet secret mounted and backed up from `${DATA_DIR}/wallet`
- S3 API auth enabled before exposing `8078`
- LocalWeb mode selected explicitly and `EXTERNAL_LOCALWEB_URL` set to the public root URL
- Port `443` reachable if using built-in autocert mode
- Metrics (`2112`), RIBSWeb (`9010`), and raw LocalWeb (`8443`) kept private by default

> **Tip — YugabyteDB tuning:** The default docker-compose ships with conservative
> memory settings (512 MiB block cache, 25% RAM ratio) suitable for 16-32 GB
> machines. For larger hosts (128+ GB RAM), increase `db_block_cache_size_bytes`
> and `default_memory_limit_to_ram_ratio` in the yugabyte `command:` section.
> A 256 GB host can use 16 GiB block cache and 0.6 ratio for ~10x throughput.

> **Operational note:** `docker-compose.yml` is a hardened single-node deployment helper. For multi-node or public internet-facing deployments, prefer a real orchestrator and make the LocalWeb ingress mode explicit.

#### Data Storage Locations

In Docker mode, the following directories are mounted as volumes:

| Host Path | Container Path | Description |
|-----------|---------------|-------------|
| `${DATA_DIR}/fgw/` | `/root/.ribsdata` | Block groups and sector data (largest) |
| `${DATA_DIR}/carstage/` | `/root/.ribsdata/cardata` | Staged CAR files served by LocalWeb |
| `${DATA_DIR}/wallet/` | `/root/.ribswallet` | Filecoin wallet keys (backup this!) |
| `${DATA_DIR}/ipfs/` | `/root/.ipfs` | IPFS/Kubo data |
| `${DATA_DIR}/yb/` | `/root/var` | YugabyteDB database |

**Changing the Data Location:**

By default, data is stored in `./data/` relative to the docker-compose.yml file. To use a different location (e.g., `/data/fgw-data`), set the `DATA_DIR` environment variable:

**Option 1 — Environment Variable (recommended)**
```bash
export DATA_DIR=/data/fgw-data
docker-compose up
```

**Option 2 — .env File (persistent, not committed)**
Create a `.env` file in the same directory as `docker-compose.yml`:
```bash
echo "DATA_DIR=/data/fgw-data" > .env
docker-compose up
```

**Option 3 — Direct Path Override**
For one-time use without modifying files:
```bash
DATA_DIR=/data/fgw-data docker-compose up
```

> **Note:** The data directory (especially `fgw/`) will grow significantly as you onboard data. Plan for sufficient storage capacity based on your expected data volume.

#### Resetting Data

> **WARNING:** This is a destructive operation that permanently deletes all stored data, including uploaded files, Filecoin deals, and database state. Only use this if you want to completely start over.

To reset all data while keeping your config and wallet:

```bash
docker-compose down && docker-compose run --rm reset-data
```

This removes YugabyteDB data, block groups, and IPFS data, but preserves your `settings.env` and wallet keys.

### Option 2 — Build From Source
#### Prerequisites
- YugabyteDB instance (YSQL port 5433, YCQL port 9042)
- Go 1.24+ toolchain
- Rclone (for data upload)

#### Install
```bash
git clone git@github.com:CIDgravity/filecoin-gateway.git
cd filecoin-gateway

go build -o filecoin-gw ./integrations/kuri/cmd/kuri
go build -o gwcfg ./integrations/gwcfg
```

#### Configure
```bash
# Interactive wizard — creates wallet, configures CIDGravity, staging server, etc.
./gwcfg
```

`gwcfg` writes a `settings.env` file with all configuration. To re-edit settings
later, run `./gwcfg` again — it detects the existing file and offers section-by-section
editing.

#### Start
```bash
source settings.env
./filecoin-gw daemon
```

### Option 3 — Ansible (Multi-Node Clusters)

Use Ansible for deploying production clusters with multiple Kuri storage nodes and S3 frontend proxies.

#### Prerequisites
- Ansible 2.9+
- YugabyteDB cluster (YSQL port 5433, YCQL port 9042)
- Target hosts with Ubuntu 24.04

#### Quick Start
```bash
cd ansible

# 1. Prepare wallet and config
go build -o gwcfg ../integrations/gwcfg
./gwcfg -f settings.env
cp -r ~/.ribswallet files/wallet/

# 2. Create inventory from example
cp inventory/production/hosts.yml.example inventory/production/hosts.yml
# Edit hosts.yml with your servers

# 3. Set secrets with Ansible Vault
ansible-vault encrypt_string 'your-cidgravity-token' --name 'cidgravity_api_token'
# Add output to inventory/production/group_vars/all.yml

# 4. Deploy cluster
ansible-playbook playbooks/site.yml -i inventory/production/hosts.yml

# 5. Verify deployment
ansible-playbook playbooks/verify.yml -i inventory/production/hosts.yml
```

#### Inventory Structure
```
inventory/production/
├── hosts.yml              # Host definitions (kuri nodes, frontends)
├── group_vars/
│   ├── all.yml            # Shared settings (YB hosts, deal settings)
│   ├── kuri.yml           # Kuri node defaults
│   └── s3_frontend.yml    # Frontend defaults
```

#### Example hosts.yml
```yaml
all:
  children:
    yugabyte:
      hosts:
        yb-node-01:
          ansible_host: 10.0.1.10
    kuri:
      hosts:
        kuri-01:
          ansible_host: 10.0.1.11
          fgw_node_id: "kuri_01"
          ribs_data: /data/fgw
        kuri-02:
          ansible_host: 10.0.1.12
          fgw_node_id: "kuri_02"
          ribs_data: /data/fgw
    s3_frontend:
      hosts:
        s3-fe-01:
          ansible_host: 10.0.1.10
          fgw_node_id: "s3_proxy_01"
```

#### Available Playbooks
| Playbook | Description |
|----------|-------------|
| `site.yml` | Full cluster deployment |
| `deploy-kuri.yml` | Deploy/update Kuri nodes only |
| `deploy-frontend.yml` | Deploy/update S3 frontends only |
| `verify.yml` | Health check all services |

#### Operations
```bash
# Add a new Kuri node
ansible-playbook playbooks/deploy-kuri.yml --limit kuri-03

# Rolling update (one node at a time)
ansible-playbook playbooks/deploy-kuri.yml

# View logs on target host
journalctl -u kuri-kuri_01 -f
```

For detailed configuration options, see `ansible/ansible-spec.md`.

---

## Interfaces

| Component | URL |
|----------|-----|
| Backend WebUI | http://localhost:9010/webui |
| S3 Endpoint | http://localhost:8078 |
| Kubo WebUI | http://localhost:5001/webui |

---

## Onboarding Data with Rclone

### Configure Rclone
```bash
rclone config create gw s3 \
  provider=Other \
  endpoint=http://localhost:8078 \
  acl=private \
  no_check_bucket=true \
  list_version=2 \
  force_path_style=true
```

> **Note:** `list_version=2` is required — the gateway only implements
> ListObjectsV2. Without it rclone defaults to v1 and receives 400 errors.

### Upload Data

The gateway achieves best throughput with high parallelism. S3 read-after-write
semantics require each request to complete before the response is visible, so
write batching only helps when many requests are in flight simultaneously.

```bash
# Recommended: high parallelism for single-user uploads
rclone copy /mnt/data gw:mybucket/ -v \
  --transfers=100 \
  --checkers=4 \
  --progress

# For very large files (multi-GB), default rclone multipart settings work well.
# For many small files (<1 MiB), high --transfers is critical for throughput.
```

| File size | Recommended --transfers | Notes |
|-----------|------------------------|-------|
| < 1 MiB | 100+ | Throughput scales linearly with parallelism |
| 1-100 MiB | 50-100 | Good balance of throughput and memory |
| 100+ MiB | 20-50 | Multipart upload handles chunking; bandwidth is the bottleneck |

### Verify Upload
```bash
# List uploaded objects
rclone ls gw:mybucket/ | head

# Read back and verify a specific file
rclone cat gw:mybucket/path/to/file.bin | sha256sum
```

---

## Troubleshooting

**YugabyteDB exits immediately (code 137)**
- Check `data/yb/logs/master.err` — if it mentions SSE4.2, the CPU doesn't support
  the required instruction set (common in some VMs without CPU passthrough).
- If it's OOM, reduce `db_block_cache_size_bytes` and
  `default_memory_limit_to_ram_ratio` in `docker-compose.yml`.

**Rclone returns "400 Bad Request" on list/copy**
- Ensure `list_version=2` is set in your rclone remote config. The gateway only
  implements S3 ListObjectsV2.

**CIDGravity shows "Not Configured" in the Web UI**
- Verify `CIDGRAVITY_API_TOKEN` is set in `settings.env` and not empty.
- Rebuild the Docker image after code updates (`docker build . -t fgw:local`)
  so the embedded Web UI picks up fixes.

**Gateway can't start — "no external module configured"**
- Run `gwcfg` to configure the staging server URL and path, or set
  `EXTERNAL_LOCALWEB_URL` and `EXTERNAL_LOCALWEB_PATH` in `settings.env`.

---

## License
Dual-licensed under **Apache 2.0** and **MIT**. See LICENSE files.  

---

## Contributing

Contributions, issues, and feature requests are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) to get started.

---

## Support

Need help? Here's how to get support:

- **GitHub Issues** — [Report bugs or request features](https://github.com/CIDgravity/filecoin-gateway/issues)
- **Filecoin Slack** — Join the **#filecoin-gateway** channel on [Filecoin Slack](https://filecoin.io/slack) for community discussion
- **Contributing** — See our [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on contributing to the project
- **CIDGravity Support** — For enterprise support and service-related questions, visit [CIDGravity](https://www.cidgravity.com/contact)
