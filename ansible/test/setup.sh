#!/bin/bash
# Setup script for Ansible playbook testing
# Creates Docker environment and builds required binaries

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DOCKER_DIR="$SCRIPT_DIR/docker"

echo "=== FGW Ansible Test Setup ==="
echo "Project root: $PROJECT_ROOT"
echo "Docker dir: $DOCKER_DIR"

# Build FGW binaries
echo ""
echo "=== Building FGW binaries ==="
cd "$PROJECT_ROOT"

mkdir -p "$DOCKER_DIR/binaries"

echo "Building kuri..."
go build -o "$DOCKER_DIR/binaries/kuri" ./integrations/kuri/cmd/kuri

echo "Building s3-proxy..."
go build -o "$DOCKER_DIR/binaries/s3-proxy" ./server/s3frontend/cmd

echo "Building gwcfg..."
go build -o "$DOCKER_DIR/binaries/gwcfg" ./integrations/gwcfg

# Set permissions
chmod +x "$DOCKER_DIR/binaries/"*

echo ""
echo "=== Building Docker images ==="
cd "$DOCKER_DIR"

# Build target host image
docker compose build

echo ""
echo "=== Starting test infrastructure ==="
docker compose up -d yugabyte

echo "Waiting for YugabyteDB to be healthy..."
timeout=120
while ! docker compose exec -T yugabyte bin/ysqlsh -h yugabyte -U yugabyte -tAc "select 1" -d yugabyte 2>/dev/null; do
    sleep 3
    timeout=$((timeout - 3))
    if [ $timeout -le 0 ]; then
        echo "ERROR: YugabyteDB failed to start"
        exit 1
    fi
    echo "  Waiting... ($timeout seconds remaining)"
done
echo "YugabyteDB is ready!"

echo ""
echo "=== Starting target hosts ==="
docker compose up -d kuri-01 kuri-02 s3-fe-01

echo "Waiting for SSH to be available and system ready..."
for host in kuri-01 kuri-02 s3-fe-01; do
    container="ansible-test-$host"
    timeout=60
    while ! docker exec "$container" nc -z localhost 22 2>/dev/null; do
        sleep 1
        timeout=$((timeout - 1))
        if [ $timeout -le 0 ]; then
            echo "ERROR: SSH on $host failed to start"
            exit 1
        fi
    done
    # Remove nologin file that systemd creates during boot
    docker exec "$container" rm -f /run/nologin /var/run/nologin 2>/dev/null || true
    echo "  $host SSH ready"
done

echo ""
echo "=== Starting Ansible controller ==="
docker compose up -d ansible-controller

# Install Ansible in controller and setup workspace
echo "Installing Ansible and setting up workspace..."
docker compose exec -T ansible-controller bash -c "
    apt-get update -qq && apt-get install -qq -y sshpass postgresql-client >/dev/null 2>&1 &&
    pip install --quiet ansible cqlsh &&
    echo 'Ansible installed successfully'
"

# Copy ansible files to work volume
echo "Copying Ansible files to workspace..."
docker compose exec -T ansible-controller bash -c "
    cp -r /ansible-src/* /ansible/ 2>/dev/null || true
    mkdir -p /ansible/inventory/test
    cp -r /test-inventory/* /ansible/inventory/test/
    mkdir -p /ansible/files/wallet
    # Copy wallet files if any exist (may be empty for testing - kuri creates wallet on init)
    cp -r /test-wallet/* /ansible/files/wallet/ 2>/dev/null || true
    chmod 700 /ansible/files/wallet
    chmod 600 /ansible/files/wallet/* 2>/dev/null || true
    # Remove any dotfiles that might interfere with wallet parsing
    find /ansible/files/wallet -maxdepth 1 -name '.*' -type f -delete 2>/dev/null || true
"

# Copy binaries to target hosts
echo ""
echo "=== Copying binaries to target hosts ==="
for host in kuri-01 kuri-02 s3-fe-01; do
    container="ansible-test-$host"
    docker exec "$container" mkdir -p /opt/fgw/bin
    docker cp "$DOCKER_DIR/binaries/kuri" "$container:/opt/fgw/bin/"
    docker cp "$DOCKER_DIR/binaries/s3-proxy" "$container:/opt/fgw/bin/"
    docker exec "$container" chown -R fgw:fgw /opt/fgw
    docker exec "$container" chmod +x /opt/fgw/bin/*
    echo "  $host binaries installed"
done

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Test environment is ready. Run tests with:"
echo "  cd $SCRIPT_DIR && ./run-tests.sh"
echo ""
echo "Or run Ansible manually:"
echo "  docker compose -f $DOCKER_DIR/docker-compose.yml exec ansible-controller bash"
echo "  cd /ansible && ansible-playbook -i inventory/test playbooks/site.yml"
echo ""
echo "To stop the test environment:"
echo "  cd $SCRIPT_DIR && ./cleanup.sh"
