#!/bin/bash
# Initialize Kuri configuration for test cluster

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ $# -eq 0 ]; then
    echo "Usage: $0 <data-directory>"
    echo ""
    echo "This script initializes Kuri configuration for both nodes."
    echo "It runs gwcfg on kuri-1 and copies the config to kuri-2."
    echo ""
    echo "Example:"
    echo "  $0 /data/fgw-test"
    exit 1
fi

DATA_DIR="$1"
export FGW_DATA_DIR="$DATA_DIR"

echo "========================================"
echo "Initializing Kuri Configuration"
echo "========================================"
echo ""
echo "Data directory: $DATA_DIR"
echo ""

# Check if config already exists
if [ -f "$DATA_DIR/config/settings.env" ]; then
    echo "⚠️  Configuration already exists at $DATA_DIR/config/settings.env"
    read -p "Reconfigure? [y/N] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Using existing configuration"
        exit 0
    fi
fi

# Ensure YugabyteDB is running
if ! docker-compose ps yugabyte 2>/dev/null | grep -q "Up"; then
    echo "❌ YugabyteDB is not running. Start the cluster first:"
    echo "   ./start.sh $DATA_DIR"
    exit 1
fi

echo "📝 Running gwcfg on kuri-1..."
echo "   Please answer the configuration questions."
echo "   For LocalWeb URL, use: http://localhost:7001"
echo ""

# Run gwcfg interactively on kuri-1
docker-compose exec -T kuri-1 ./gwcfg -f /app/config/settings.env

# Copy config to shared location
echo ""
echo "📋 Copying configuration to shared location..."
docker-compose exec kuri-1 cp /app/config/settings.env /data/ribs/config/settings.env 2>/dev/null || \
docker-compose exec kuri-1 cp /app/config/settings.env /data/config/settings.env 2>/dev/null || \
echo "⚠️  Could not copy to shared location, config remains in kuri-1"

echo ""
echo "✅ Configuration complete!"
echo ""
echo "The settings.env has been created. To apply to kuri-2:"
echo "  1. Copy $DATA_DIR/kuri-1/config/settings.env to $DATA_DIR/kuri-2/config/"
echo "  2. Modify EXTERNAL_LOCALWEB_URL for kuri-2 (e.g., http://localhost:7002)"
echo "  3. Restart the cluster: ./stop.sh $DATA_DIR && ./start.sh $DATA_DIR"
echo ""
