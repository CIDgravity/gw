#!/bin/bash
# Cleanup script for Ansible test environment

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$SCRIPT_DIR/docker"

echo "=== Cleaning up Ansible test environment ==="

cd "$DOCKER_DIR"

# Stop and remove containers
echo "Stopping containers..."
docker compose down -v --remove-orphans

# Remove built binaries
echo "Removing built binaries..."
rm -rf "$DOCKER_DIR/binaries/"*

# Remove any generated SSH keys
rm -rf "$DOCKER_DIR/ssh-keys/"*

echo ""
echo "=== Cleanup Complete ==="
echo ""
echo "To remove Docker images as well, run:"
echo "  docker rmi \$(docker images -q 'docker-*target*')"
