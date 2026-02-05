# Ansible Playbook Testing

This directory contains a Docker-based test harness for validating Ansible playbooks before production deployment.

## Architecture

The test environment creates:
- **YugabyteDB**: Single-node database for testing
- **kuri-01, kuri-02**: Simulated Kuri storage node targets (Ubuntu 24.04 + systemd)
- **s3-fe-01**: Simulated S3 frontend target
- **ansible-controller**: Container with Ansible installed to run playbooks

All containers are connected via a Docker bridge network with fixed IPs.

## Quick Start

```bash
# 1. Setup test environment (builds binaries, starts containers)
./setup.sh

# 2. Run all tests
./run-tests.sh

# 3. Cleanup when done
./cleanup.sh
```

## Manual Testing

After running `./setup.sh`, you can manually run Ansible commands:

```bash
# Enter the controller container
docker compose -f docker/docker-compose.yml exec ansible-controller bash

# Inside container, run playbooks
cd /ansible
ansible-playbook -i inventory/test playbooks/site.yml -v

# Or run specific playbooks
ansible-playbook -i inventory/test playbooks/deploy-kuri.yml --limit kuri-01

# Check connectivity
ansible -i inventory/test all -m ping
```

## Inspecting Target Hosts

```bash
# Shell into a target host
docker compose -f docker/docker-compose.yml exec kuri-01 bash

# View service logs
journalctl -u kuri-kuri-01 -f

# Check systemd status
systemctl status kuri-kuri-01
```

## Test Structure

1. **Connectivity Check**: Verify Ansible can reach all targets
2. **YB Init**: Create keyspaces and tables
3. **Deploy Kuri**: Install and configure storage nodes
4. **Deploy Frontend**: Install and configure S3 proxies
5. **Verify**: Run health checks
6. **Idempotency**: Re-run site.yml to ensure no unnecessary changes

## Files

```
test/
├── setup.sh                    # Initialize test environment
├── run-tests.sh                # Run automated tests
├── cleanup.sh                  # Tear down environment
├── README.md                   # This file
└── docker/
    ├── docker-compose.yml      # Container definitions
    ├── Dockerfile.target       # Ubuntu + systemd target image
    ├── binaries/               # Built FGW binaries (populated by setup.sh)
    ├── test-inventory/         # Test inventory files
    │   ├── hosts.yml
    │   └── group_vars/all.yml
    ├── test-wallet/            # Mock wallet for testing
    └── ssh-keys/               # SSH keys (if needed)
```

## Notes

- Target hosts run with `--privileged` for systemd support
- Binaries are pre-installed via shared volume (no download in tests)
- Mock wallet is used (not a real Filecoin wallet)
- CIDGravity token is a test placeholder
- Services won't actually start daemons properly without real configuration,
  but the playbook execution and file generation can be validated
