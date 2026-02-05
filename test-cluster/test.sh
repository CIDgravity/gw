#!/bin/bash
# Test script for the 2-node cluster

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ $# -eq 0 ]; then
    echo "Usage: $0 <data-directory>"
    echo ""
    echo "Example:"
    echo "  $0 /data/fgw-test"
    exit 1
fi

DATA_DIR="$1"
export FGW_DATA_DIR="$DATA_DIR"

echo "========================================"
echo "Testing FGW Test Cluster (2 Storage Nodes)"
echo "Data directory: $DATA_DIR"
echo "========================================"
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Test 1: Check all services are running
echo "Test 1: Checking service health..."
FAILED=0

for service in yugabyte kuri-1 kuri-2; do
    if docker-compose ps | grep -q "$service.*Up"; then
        echo -e "  ${GREEN}✓${NC} $service is running"
    else
        echo -e "  ${RED}✗${NC} $service is NOT running"
        FAILED=1
    fi
done

if [ $FAILED -eq 1 ]; then
    echo ""
    echo "Some services are not running. Start the cluster with:"
    echo "  ./start.sh $DATA_DIR"
    exit 1
fi

# Test 2: Check S3 API endpoint
echo ""
echo "Test 2: Testing S3 API endpoint..."

# Test kuri-1 S3 API
if curl -s -o /dev/null -w "%{http_code}" http://localhost:8078/ | grep -q "200\|403\|404"; then
    echo -e "  ${GREEN}✓${NC} S3 API (localhost:8078) is responding"
else
    echo -e "  ${RED}✗${NC} S3 API (localhost:8078) is NOT responding"
fi

# Test 3: Check Web UI
echo ""
echo "Test 3: Testing Web UI..."
if curl -s -o /dev/null -w "%{http_code}" http://localhost:9010/webui | grep -q "200"; then
    echo -e "  ${GREEN}✓${NC} Web UI is accessible at http://localhost:9010/webui"
else
    echo -e "  ${RED}✗${NC} Web UI is NOT responding"
fi

# Test 4: Simple S3 operations (if rclone is available)
if command -v rclone &> /dev/null; then
    echo ""
    echo "Test 4: Testing S3 operations with rclone..."
    
    # Create temporary config
    export RCLONE_CONFIG_FGWTEST_TYPE=s3
    export RCLONE_CONFIG_FGWTEST_PROVIDER=Other
    export RCLONE_CONFIG_FGWTEST_ACCESS_KEY_ID=test
    export RCLONE_CONFIG_FGWTEST_SECRET_ACCESS_KEY=test
    export RCLONE_CONFIG_FGWTEST_ENDPOINT=http://localhost:8078
    
    # Test bucket creation
    if rclone mkdir fgwtest:test-bucket 2>/dev/null; then
        echo -e "  ${GREEN}✓${NC} Bucket creation works"
    else
        echo -e "  ${RED}✗${NC} Bucket creation failed"
    fi
    
    # Test object upload
    if echo "test data" | rclone rcat fgwtest:test-bucket/test-object 2>/dev/null; then
        echo -e "  ${GREEN}✓${NC} Object upload works"
    else
        echo -e "  ${RED}✗${NC} Object upload failed"
    fi
    
    # Test object download
    if rclone cat fgwtest:test-bucket/test-object 2>/dev/null | grep -q "test data"; then
        echo -e "  ${GREEN}✓${NC} Object download works"
    else
        echo -e "  ${RED}✗${NC} Object download failed"
    fi
    
    # Cleanup
    rclone purge fgwtest:test-bucket 2>/dev/null || true
else
    echo ""
    echo "Test 4: Skipping S3 operations (rclone not installed)"
    echo "  Install rclone to test full S3 functionality"
fi

# Test 5: Check cluster monitoring page
echo ""
echo "Test 5: Checking cluster monitoring..."
echo "  Access the cluster monitor at: http://localhost:9010/webui/cluster"
echo "  You should see:"
echo "    - Topology diagram with 2 storage nodes (kuri-1, kuri-2)"
echo "    - Node statistics from YCQL"
echo "    - Performance charts (when metrics collection is implemented)"

echo ""
echo "========================================"
echo "Test Summary"
echo "========================================"
echo ""
echo "Cluster endpoints:"
echo "  S3 API:             http://localhost:8078 (kuri-1)"
echo "  Web UI:             http://localhost:9010/webui (cluster view)"
echo "  Cluster Monitor:    http://localhost:9010/webui/cluster"
echo ""
echo "Architecture:"
echo "  - kuri-1: Exposes S3 API (:8078) and Web UI (:9010)"
echo "  - kuri-2: Internal only (no exposed ports)"
echo "  - Both nodes share metadata via YugabyteDB"
echo ""
echo "To test manually:"
echo "  curl -X PUT http://localhost:8078/mybucket/myobject -d 'test data'"
echo "  curl http://localhost:8078/mybucket/myobject"
echo ""
echo "To stop the cluster:"
echo "  ./stop.sh $DATA_DIR"
echo ""
