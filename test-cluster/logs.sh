#!/bin/bash
# View logs from the test cluster

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ $# -lt 1 ]; then
    echo "Usage: $0 <data-directory> [service]"
    echo ""
    echo "Examples:"
    echo "  $0 /data/fgw-test          # View all logs"
    echo "  $0 /data/fgw-test kuri-1   # View kuri-1 logs"
    echo ""
    echo "Available services:"
    docker-compose config --services | sed 's/^/  - /'
    exit 1
fi

DATA_DIR="$1"
export FGW_DATA_DIR="$DATA_DIR"

if [ $# -ge 2 ]; then
    SERVICE="$2"
    docker-compose logs -f "$SERVICE"
else
    docker-compose logs -f
fi
