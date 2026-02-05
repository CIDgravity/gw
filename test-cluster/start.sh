#!/bin/bash
# Quick start script for test cluster

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ $# -eq 0 ]; then
    echo "Usage: $0 <data-directory>"
    echo ""
    echo "Example:"
    echo "  $0 /data/fgw-test"
    echo "  $0 /mnt/storage/fgw-cluster"
    echo ""
    echo "The data directory will be created if it doesn't exist."
    exit 1
fi

DATA_DIR="$1"

# Export for docker-compose
export FGW_DATA_DIR="$DATA_DIR"

echo "========================================"
echo "FGW Test Cluster (2 Storage Nodes)"
echo "========================================"
echo ""
echo "Data directory: $DATA_DIR"
echo ""
echo "Architecture:"
echo "  - S3 Proxy: S3 API (:8078) - routes to Kuri nodes"
echo "  - kuri-1: LocalWeb (:7001) + Web UI (:9010)"
echo "  - kuri-2: LocalWeb (:7002)"
echo "  - YugabyteDB: Shared metadata"
echo ""

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ docker-compose is not installed. Please install docker-compose first."
    exit 1
fi

# Check if image exists
if ! docker image inspect fgw:local &> /dev/null; then
    echo "🔨 Building fgw:local Docker image..."
    docker build ../.. -t fgw:local
else
    echo "✅ Docker image fgw:local exists"
fi

# Initialize data directories
echo ""
echo "📁 Initializing data directories..."
./init-data.sh "$DATA_DIR"

# Generate configuration if needed
if [ ! -f "$DATA_DIR/config/kuri-1/settings.env" ] || [ ! -f "$DATA_DIR/config/kuri-2/settings.env" ]; then
    echo ""
    echo "⚙️  Generating default configuration..."
    ./gen-config.sh "$DATA_DIR"
else
    echo ""
    echo "✅ Configuration exists:"
    echo "   - $DATA_DIR/config/kuri-1/settings.env"
    echo "   - $DATA_DIR/config/kuri-2/settings.env"
fi

# Start the cluster sequentially to avoid migration race
echo ""
echo "🚀 Starting test cluster..."

# Start infrastructure first
docker-compose up -d yugabyte db-init

# Wait for DB to be fully ready
echo "⏳ Waiting for YugabyteDB to be fully ready..."
sleep 10

# Start kuri-1 first (let it run migrations)
echo "🚀 Starting kuri-1..."
docker-compose up -d kuri-1

# Wait for kuri-1 to initialize
echo "⏳ Waiting for kuri-1 to initialize..."
sleep 15

# Start kuri-2 after kuri-1 is ready
echo "🚀 Starting kuri-2..."
docker-compose up -d kuri-2

# Start remaining services
echo "🚀 Starting remaining services..."
docker-compose up -d s3-proxy webui

# Wait for all services to be ready
echo ""
echo "⏳ Waiting for all services to be ready..."
sleep 5

# Check Kuri nodes
echo "🔍 Checking Kuri storage nodes..."
for node in kuri-1 kuri-2; do
    if docker-compose ps | grep -q "$node.*Up"; then
        echo "✅ $node is running"
    else
        echo "❌ $node is not running. Check logs: docker-compose logs $node"
    fi
done

echo ""
echo "========================================"
echo "✅ Test cluster is ready!"
echo "========================================"
echo ""
echo "Services:"
echo "  📡 S3 API:             http://localhost:8078 (Frontend Proxy)"
echo "  🖥️  Web UI:             http://localhost:9010/webui"
echo "  📊 Cluster Monitor:    http://localhost:9010/webui/cluster"
echo "  📦 kuri-1 LocalWeb:    http://localhost:7001 (CAR files)"
echo "  📦 kuri-2 LocalWeb:    http://localhost:7002 (CAR files)"
echo ""
echo "Data directories:"
echo "  $DATA_DIR/yugabyte/"
echo "  $DATA_DIR/kuri-1/"
echo "  $DATA_DIR/kuri-2/"
echo ""
echo "Commands:"
echo "  View logs:     ./logs.sh $DATA_DIR [yugabyte|kuri-1|kuri-2]"
echo "  Stop cluster:  ./stop.sh $DATA_DIR"
echo "  Test cluster:  ./test.sh $DATA_DIR"
echo ""
echo "Test with rclone:"
echo "  rclone config create fgw-test s3 provider=Other access_key_id=test secret_access_key=test endpoint=http://localhost:8078"
echo "  rclone lsd fgw-test:"
echo ""
