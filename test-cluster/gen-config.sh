#!/bin/bash
# Generate Kuri configurations for test cluster (separate config per node)

set -e

if [ $# -eq 0 ]; then
    echo "Usage: $0 <data-directory> [external-domain]"
    echo ""
    echo "Examples:"
    echo "  $0 /data/fgw-test                    # Use localhost"
    echo "  $0 /data/fgw-test fgw.example.com    # Use custom domain"
    echo ""
    echo "This creates separate configs for each node:"
    echo "  - kuri-1: http://{domain}:7001"
    echo "  - kuri-2: http://{domain}:7002"
    exit 1
fi

DATA_DIR="$1"
EXTERNAL_DOMAIN="${2:-localhost}"

echo "Generating Kuri configurations..."
echo "  Data directory: $DATA_DIR"
echo "  External domain: $EXTERNAL_DOMAIN"
echo ""

# Create config directories
mkdir -p "$DATA_DIR/config/kuri-1"
mkdir -p "$DATA_DIR/config/kuri-2"

# Common settings
COMMON_CONFIG=$(cat << 'EOF'
# Core settings
export RIBS_DATA="/data/ribs"
export RIBS_MAX_LOCAL_GROUP_COUNT="4"
export RIBS_FILECOIN_API_ENDPOINT="https://pac-l-gw.devtty.eu/rpc/v1"

# Database settings
export RIBS_YUGABYTE_CQL_HOSTS="yugabyte"
export RIBS_YUGABYTE_CQL_PORT="9042"
export RIBS_YUGABYTE_CQL_KEYSPACE="filecoingw"
export RIBS_YUGABYTE_SQL_HOST="yugabyte"
export RIBS_YUGABYTE_SQL_PORT="5433"
export RIBS_YUGABYTE_SQL_DB="filecoingw"

# S3 API settings
export RIBS_S3API_BINDADDR=":8078"
export RIBS_S3API_AUTH_ENABLED="false"

# LocalWeb settings
export EXTERNAL_LOCALWEB_PATH="/data/ribs/cardata"
export EXTERNAL_LOCALWEB_BUILTIN_SERVER="true"
export EXTERNAL_LOCALWEB_SERVER_TLS="false"

# Deal settings (minimal test values)
export RIBS_MINIMUM_REPLICA_COUNT="1"
export RIBS_MAXIMUM_REPLICA_COUNT="2"
export RIBS_MINIMUM_RETRIEVABLE_COUNT="1"
export RIBS_RETRIEVALBLE_REPAIR_THRESHOLD="1"
export RIBS_DEAL_START_TIME="72"
export RIBS_DEAL_DURATION="180"

# Disable balance manager (no auto top-ups in test mode)
export RIBS_BALANCES_AUTO_TRANSFER_ENABLED="false"
EOF
)

# Generate kuri-1 config
cat > "$DATA_DIR/config/kuri-1/settings.env" << EOF
# Kuri Node 1 Configuration
# Generated on $(date)

$COMMON_CONFIG

# Node-specific settings
export FGW_NODE_ID="kuri-1"
export EXTERNAL_LOCALWEB_URL="http://${EXTERNAL_DOMAIN}:7001"
export EXTERNAL_LOCALWEB_SERVER_PORT="7001"

# Per-node RIBS database (groups, deals, blockstore index)
export RIBS_YUGABYTE_CQL_KEYSPACE="filecoingw_kuri1"
export RIBS_YUGABYTE_SQL_DB="filecoingw_kuri1"

# Shared S3 database (object routing metadata)
export RIBS_S3_CQL_KEYSPACE="filecoingw_s3"
EOF

# Generate kuri-2 config
cat > "$DATA_DIR/config/kuri-2/settings.env" << EOF
# Kuri Node 2 Configuration  
# Generated on $(date)

$COMMON_CONFIG

# Node-specific settings
export FGW_NODE_ID="kuri-2"
export EXTERNAL_LOCALWEB_URL="http://${EXTERNAL_DOMAIN}:7002"
export EXTERNAL_LOCALWEB_SERVER_PORT="7002"

# Per-node RIBS database (groups, deals, blockstore index)
export RIBS_YUGABYTE_CQL_KEYSPACE="filecoingw_kuri2"
export RIBS_YUGABYTE_SQL_DB="filecoingw_kuri2"

# Shared S3 database (object routing metadata)
export RIBS_S3_CQL_KEYSPACE="filecoingw_s3"
EOF

# Generate nginx config for webui proxy
cat > "$DATA_DIR/config/nginx.conf" << 'EOF'
events {
    worker_connections 1024;
}

http {
    # kuri-1 web UI on port 9010
    server {
        listen 9010;
        
        location / {
            proxy_pass http://kuri-1:9010;
            proxy_http_version 1.1;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection "upgrade";
        }
    }
    
    # kuri-2 web UI on port 9011
    server {
        listen 9011;
        
        location / {
            proxy_pass http://kuri-2:9010;
            proxy_http_version 1.1;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection "upgrade";
        }
    }
}
EOF

echo "✅ Configuration files created:"
echo "  - $DATA_DIR/config/kuri-1/settings.env (http://${EXTERNAL_DOMAIN}:7001)"
echo "  - $DATA_DIR/config/kuri-2/settings.env (http://${EXTERNAL_DOMAIN}:7002)"
echo "  - $DATA_DIR/config/nginx.conf (webui proxy)"
echo ""
echo "To use these configurations:"
echo "  ./start.sh $DATA_DIR"
echo ""
echo "For NAT/reverse proxy setup:"
echo "  - Route ${EXTERNAL_DOMAIN}:7001 → host:7001 (kuri-1)"
echo "  - Route ${EXTERNAL_DOMAIN}:7002 → host:7002 (kuri-2)"
