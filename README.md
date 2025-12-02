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
apt install -y docker.io docker-compose rclone
git clone git@github.com:CIDgravity/filecoin-gateway.git
cd filecoin-gateway

docker build . -t fgw:local

docker run -it --rm --entrypoint ./gwcfg   -v ./data/config:/app/config   -v ./data/wallet:/root/.ribswallet   fgw:local -f config/settings.env

docker-compose up
```

### Option 2 — Build From Source
#### Prerequisites
- YugabyteDB instance  
- Rclone (optional)  
- Go toolchain  

#### Install
```bash
git clone git@github.com:CIDgravity/filecoin-gateway.git
cd filecoin-gateway

go build -o filecoin-gw ./integrations/kuri/cmd/kuri
go build -o gwcfg ./integrations/gwcfg
```

#### Configure
```bash
./gwcfg
```

#### Start
```bash
source settings.env
./filecoin-gw daemon
```

---

## Interfaces

| Component | URL |
|----------|-----|
| Backend WebUI | http://localhost:9010/webui |
| S3 Endpoint | http://localhost:8078 |
| Kubo WebUI | http://localhost:5001/webui |

---

## Onboarding Data with Rclone

### Example `rclone.conf`
```
cat > ~/.config/rclone/rclone.conf
[gw]
type = s3
provider = Other
access_key_id = test-access-key
secret_access_key = test-secret-key
region = us-east-1
endpoint = http://localhost:8078
acl = private
```

### Upload Data
```
rclone --s3-no-check-bucket --s3-force-path-style --s3-list-version=2   copy /mnt/data32 gw:mybucket/data32 -v
```

---

## License
Dual-licensed under **Apache 2.0** and **MIT**. See LICENSE files.  

---

## Contributing
Contributions, issues, and feature requests are welcome.
