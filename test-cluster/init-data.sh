#!/bin/bash
# Initialize test cluster data directories

set -e

if [ $# -eq 0 ]; then
    echo "Usage: $0 <data-directory>"
    echo ""
    echo "Example:"
    echo "  $0 /data/fgw-test"
    echo "  $0 /mnt/storage/fgw-cluster"
    exit 1
fi

DATA_DIR="$1"

echo "Initializing test cluster data directories in $DATA_DIR"

# Create base directory
mkdir -p "$DATA_DIR"

# Create data directories for each node
# Only create if they don't exist (preserve existing data)
mkdir -p "$DATA_DIR/yugabyte"
mkdir -p "$DATA_DIR/kuri-1"
mkdir -p "$DATA_DIR/kuri-2"
mkdir -p "$DATA_DIR/config"

# Try to set permissions, but don't fail if it doesn't work
# (YugabyteDB files may be owned by root from Docker)
echo "Setting permissions (may require sudo for existing data)..."
if ! chmod -R 755 "$DATA_DIR" 2>/dev/null; then
    echo "⚠️  Permission update skipped (some files may be owned by root)"
    echo "   This is normal if reusing existing data directories"
fi

echo "Data directories ready:"
echo "  - $DATA_DIR/yugabyte (YugabyteDB data)"
echo "  - $DATA_DIR/kuri-1 (Kuri storage node 1)"
echo "  - $DATA_DIR/kuri-2 (Kuri storage node 2)"
echo "  - $DATA_DIR/config (Shared configuration)"

echo ""
echo "✅ Directory structure ready!"
echo ""
echo "Next steps:"
echo "  1. Build the Docker image: docker build . -t fgw:local"
echo "  2. Start the cluster: ./start.sh $DATA_DIR"
