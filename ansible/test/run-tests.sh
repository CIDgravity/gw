#!/bin/bash
# Run Ansible playbook tests against Docker targets

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$SCRIPT_DIR/docker"

cd "$DOCKER_DIR"

echo "=== FGW Ansible Playbook Tests ==="
echo ""

# Check if environment is running
if ! docker compose ps --quiet ansible-controller 2>/dev/null | grep -q .; then
    echo "ERROR: Test environment not running. Run ./setup.sh first."
    exit 1
fi

run_ansible() {
    local playbook="$1"
    local extra_args="${2:-}"
    
    echo ""
    echo "=== Running: $playbook $extra_args ==="
    docker compose exec -T ansible-controller bash -c "
        cd /ansible && \
        ANSIBLE_CONFIG=/ansible/ansible.cfg \
        ansible-playbook \
            -i inventory/test \
            $playbook \
            $extra_args \
            -v
    "
}

# Test 1: Connectivity check (exclude yugabyte - it's just a database reference)
echo ""
echo "=== Test 1: Connectivity Check ==="
docker compose exec -T ansible-controller bash -c "
    cd /ansible && \
    ANSIBLE_CONFIG=/ansible/ansible.cfg \
    ansible -i inventory/test 'all:!yugabyte' -m ping
"

# Test 2: YugabyteDB initialization
echo ""
echo "=== Test 2: YugabyteDB Initialization ==="
run_ansible "playbooks/setup-yb.yml"

# Test 3: Deploy Kuri nodes
echo ""
echo "=== Test 3: Deploy Kuri Nodes ==="
run_ansible "playbooks/deploy-kuri.yml"

# Test 4: Deploy S3 frontend
echo ""
echo "=== Test 4: Deploy S3 Frontend ==="
run_ansible "playbooks/deploy-frontend.yml"

# Test 5: Verification
echo ""
echo "=== Test 5: Verify Deployment ==="
run_ansible "playbooks/verify.yml"

# Test 6: Idempotency check - run site.yml again
echo ""
echo "=== Test 6: Idempotency Check (re-run site.yml) ==="
run_ansible "playbooks/site.yml"

echo ""
echo "=== All Tests Completed ==="
echo ""
echo "To inspect the test environment:"
echo "  docker compose -f $DOCKER_DIR/docker-compose.yml exec ansible-controller bash"
echo "  docker compose -f $DOCKER_DIR/docker-compose.yml exec kuri-01 bash"
echo ""
echo "To view service logs:"
echo "  docker compose -f $DOCKER_DIR/docker-compose.yml exec kuri-01 journalctl -u kuri-kuri-01 -f"
echo ""
echo "To cleanup:"
echo "  ./cleanup.sh"
