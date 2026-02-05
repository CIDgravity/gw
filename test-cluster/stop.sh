#!/bin/bash
# Stop the test cluster

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ $# -eq 0 ]; then
    echo "Usage: $0 <data-directory> [--clean]"
    echo ""
    echo "Examples:"
    echo "  $0 /data/fgw-test          # Stop cluster"
    echo "  $0 /data/fgw-test --clean  # Stop and remove all data"
    echo ""
    echo "WARNING: --clean will delete all data in the specified directory!"
    exit 1
fi

DATA_DIR="$1"
CLEAN="${2:-}"

# Export for docker-compose
export FGW_DATA_DIR="$DATA_DIR"

echo "========================================"
echo "Stopping FGW Test Cluster"
echo "========================================"
echo ""
echo "Data directory: $DATA_DIR"
echo ""

# Check if cluster is running
if ! docker-compose ps 2>/dev/null | grep -q "Up"; then
    echo "ℹ️  Cluster is not running (no containers found)"
    
    if [ "$CLEAN" == "--clean" ]; then
        echo ""
        read -p "⚠️  Delete all data in $DATA_DIR? [y/N] " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo "🗑️  Removing data..."
            rm -rf "$DATA_DIR"/*
            echo "✅ Data removed"
        else
            echo "❌ Aborted"
            exit 1
        fi
    fi
    exit 0
fi

# Stop the cluster
echo "🛑 Stopping containers..."
docker-compose down

if [ $? -eq 0 ]; then
    echo "✅ Cluster stopped successfully"
else
    echo "❌ Failed to stop cluster"
    exit 1
fi

# Clean up if requested
if [ "$CLEAN" == "--clean" ]; then
    echo ""
    echo "⚠️  WARNING: This will DELETE all data in $DATA_DIR"
    read -p "Are you sure? Type 'yes' to confirm: " -r
    
    if [ "$REPLY" == "yes" ]; then
        echo "🗑️  Removing data directories..."
        rm -rf "$DATA_DIR"/*
        echo "✅ All data removed from $DATA_DIR"
    else
        echo "❌ Clean aborted (data preserved)"
    fi
fi

echo ""
echo "========================================"
echo "Done!"
echo "========================================"
echo ""
echo "To start the cluster again:"
echo "  ./start.sh $DATA_DIR"
echo ""
