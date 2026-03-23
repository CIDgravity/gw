package configuration

import (
	"fmt"
	"os"
	"strings"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/kelseyhightower/envconfig"
	"golang.org/x/xerrors"
)

var log = logging.Logger("ribs:config")

// LocalwebConfig configures the local web server for serving CAR files to storage providers.
// This is an alternative to S3 for staging storage, useful for local/on-premise deployments.
type LocalwebConfig struct {
	// Path is the local filesystem path where CAR files are stored.
	// Example: "/data/ribs/carfiles"
	Path string `envconfig:"EXTERNAL_LOCALWEB_PATH"`

	// Url is the public URL where storage providers can fetch CAR files.
	// Must be reachable from the internet if making deals with external SPs.
	// Example: "https://myserver.example.com:8443/cars"
	Url string `envconfig:"EXTERNAL_LOCALWEB_URL"`

	// BuiltinServer enables the built-in HTTP server for serving CAR files.
	// When true, RIBS will start its own HTTPS server.
	// When false, you must configure an external web server (nginx, caddy, etc.).
	BuiltinServer bool `envconfig:"EXTERNAL_LOCALWEB_BUILTIN_SERVER" default:"true"`

	// ServerPort is the port for the built-in HTTP server.
	ServerPort string `envconfig:"EXTERNAL_LOCALWEB_SERVER_PORT" default:"8443"`

	// ServerTLS enables TLS (HTTPS) for the built-in server using Let's Encrypt.
	// Requires a valid domain name pointing to this server.
	ServerTLS bool `envconfig:"EXTERNAL_LOCALWEB_SERVER_TLS" default:"true"`

	// MaxConcurrentUploadsPerDeal limits concurrent uploads per deal to prevent
	// overwhelming the server when multiple SPs fetch simultaneously.
	MaxConcurrentUploadsPerDeal int `envconfig:"EXTERNAL_LOCALWEB_MAX_CONCURRENT_UPLOADS_PER_DEAL" default:"15"`
}

// S3Config configures S3-compatible object storage for CAR file staging.
// Supports AWS S3, MinIO, Backblaze B2, Cloudflare R2, and other S3-compatible services.
type S3Config struct {
	// Endpoint is the S3 API endpoint URL.
	// For AWS: leave empty or use region-specific endpoint.
	// For MinIO: "http://minio.example.com:9000"
	// For Backblaze B2: "https://s3.us-west-000.backblazeb2.com"
	Endpoint string `envconfig:"EXTERNAL_S3_ENDPOINT"`

	// Region is the AWS region or equivalent for other providers.
	// Examples: "us-east-1", "eu-west-1", "auto"
	Region string `envconfig:"EXTERNAL_S3_REGION"`

	// AccessKey is the S3 access key ID for authentication.
	AccessKey string `envconfig:"EXTERNAL_S3_ACCESS_KEY"`

	// SecretKey is the S3 secret access key for authentication.
	SecretKey string `envconfig:"EXTERNAL_S3_SECRET_KEY"`

	// Token is an optional session token for temporary credentials (AWS STS).
	// Leave empty for permanent credentials.
	Token string `envconfig:"EXTERNAL_S3_TOKEN"`

	// Bucket is the S3 bucket name where CAR files will be stored.
	Bucket string `envconfig:"EXTERNAL_S3_BUCKET"`

	// BucketUrl is the public URL for accessing objects in the bucket.
	// Used by storage providers to download CAR files.
	// Example: "https://mybucket.s3.amazonaws.com" or custom CDN URL.
	BucketUrl string `envconfig:"EXTERNAL_S3_BUCKET_URL"`
}

type ExternalConfig struct {
	Localweb LocalwebConfig
	S3       S3Config
}

// CidGravityConfig configures integration with the CIDGravity service for
// intelligent storage provider selection and deal monitoring.
type CidGravityConfig struct {
	// ApiToken is your CIDGravity API authentication token.
	// Obtain from https://cidgravity.com
	ApiToken string `envconfig:"CIDGRAVITY_API_TOKEN"`

	// ApiEndpointGetProviders is the API endpoint for fetching recommended storage providers.
	// Override only if using a custom/self-hosted CIDGravity instance.
	ApiEndpointGetProviders string `envconfig:"CIDGRAVITY_API_ENDPOINT_GBAP" default:"https://service.cidgravity.com/private/v1/get-best-available-providers"`

	// ApiEndpointGetDeals is the API endpoint for fetching on-chain deal information.
	// Override only if using a custom/self-hosted CIDGravity instance.
	ApiEndpointGetDeals string `envconfig:"CIDGRAVITY_API_ENDPOINT_GOCD" default:"https://service.cidgravity.com/private/v1/get-on-chain-deals"`

	// MaxConns limits the maximum concurrent connections to the CIDGravity API.
	MaxConns int64 `envconfig:"CIDGRAVITY_MAX_CONNECTIONS" default:"4"`

	// AltClients is a comma-separated list of alternative client names for multi-client setups.
	// Each client requires a corresponding CIDGRAVITY_API_TOKEN_{CLIENT} environment variable.
	// Example: "client1,client2" requires CIDGRAVITY_API_TOKEN_client1 and CIDGRAVITY_API_TOKEN_client2.
	AltClients []string `envconfig:"CIDGRAVITY_ALT_CLIENTS"`

	// AltTokens stores the resolved tokens for alternative clients.
	// Populated automatically from CIDGRAVITY_API_TOKEN_{CLIENT} variables.
	AltTokens map[string]string
}

// RibsConfig contains the core RIBS configuration settings.
type RibsConfig struct {
	// DataDir is the root directory for all RIBS data including groups, indexes, and databases.
	// Should be on fast storage (SSD/NVMe) for optimal performance.
	DataDir string `envconfig:"RIBS_DATA" default:"~/.ribsdata"`

	// SendExtends enables sending DataCap extension messages for expiring deals.
	// Requires wallet to have sufficient FIL for message fees.
	SendExtends bool `envconfig:"RIBS_SEND_EXTENDS" default:"false"`

	// FilecoinApiEndpoint is the Lotus Gateway API endpoint for chain operations.
	// Can use public gateways or your own Lotus node.
	// Examples: "https://pac-l-gw.devtty.eu/rpc/v1", "http://localhost:1234/rpc/v1"
	FilecoinApiEndpoint string `envconfig:"RIBS_FILECOIN_API_ENDPOINT" default:"https://pac-l-gw.devtty.eu/rpc/v1"`

	// MinimumRetrievableCount is the minimum number of deals that must be retrievable
	// for a group to be considered healthy. Triggers repair deals if below this threshold.
	MinimumRetrievableCount int `envconfig:"RIBS_MINIMUM_RETRIEVABLE_COUNT" default:"5"`

	// MinimumReplicaCount is the minimum number of total deals (retrievable or not)
	// for a group. New deals are made if below this count.
	MinimumReplicaCount int `envconfig:"RIBS_MINIMUM_REPLICA_COUNT" default:"5"`

	// MaximumReplicaCount is the maximum number of deals per group.
	// No new deals are made once this limit is reached.
	MaximumReplicaCount int `envconfig:"RIBS_MAXIMUM_REPLICA_COUNT" default:"10"`

	// RetrievableRepairThreshold is the retrieval success count below which
	// repair deals are triggered to restore redundancy.
	RetrievableRepairThreshold int `envconfig:"RIBS_RETRIEVABLE_REPAIR_THRESHOLD" default:"3"`

	// MaxLocalGroupCount is the maximum number of groups to keep in local storage.
	// When exceeded, oldest finalized groups are offloaded to staging/Filecoin.
	// Each group is ~30GB, so 64 groups ≈ 2TB local storage.
	MaxLocalGroupCount int `envconfig:"RIBS_MAX_LOCAL_GROUP_COUNT" default:"64"`

	// MaxStagingGroupCount is the maximum number of groups to keep in staging.
	// Defaults to MaxLocalGroupCount if not set.
	MaxStagingGroupCount int `envconfig:"RIBS_MAX_STAGING_GROUP_COUNT" default:"0"`

	// DealCheckInterval is how often to check deal status and trigger new deals.
	DealCheckInterval time.Duration `envconfig:"RIBS_DEAL_CHECK_INTERVAL" default:"30s"`

	// DealCanSendCommand is an optional external command to check if deals can be sent.
	// If set, the command is executed and must return exit code 0 to allow deals.
	// Useful for rate limiting or external approval workflows.
	// Example: "/usr/local/bin/check-deal-quota.sh"
	DealCanSendCommand string `envconfig:"RIBS_DEAL_CAN_SEND_COMMAND" default:""`

	// MongoDBUri is the MongoDB connection string for file metadata storage.
	// Required for MFS (Mutable File System) functionality.
	// Example: "mongodb://localhost:27017/ribs"
	MongoDBUri string `envconfig:"RIBS_MONGODB_URI"`

	// RunSpCrawler enables the background storage provider crawler that
	// discovers and monitors Filecoin storage providers for deal making.
	RunSpCrawler bool `envconfig:"RIBS_RUN_SP_CRAWLER" default:"true"`

	// CidLocationWorkerCount is the number of workers for CID location lookups.
	CidLocationWorkerCount int `envconfig:"RIBS_CID_LOCATION_WORKER_COUNT" default:"16"`

	// GC (Garbage Collection) settings
	// GCEnabled enables passive garbage collection for groups with no live references.
	// When enabled, groups with no S3 object references will not have their claims extended,
	// allowing the data to naturally expire when deals reach their term.
	GCEnabled bool `envconfig:"RIBS_GC_ENABLED" default:"false"`

	// GCScanInterval is how often to scan for GC candidates.
	GCScanInterval time.Duration `envconfig:"RIBS_GC_SCAN_INTERVAL" default:"1h"`

	// GCGracePeriod is how long a group must have zero references before being
	// confirmed for GC. This prevents race conditions with ongoing uploads.
	GCGracePeriod time.Duration `envconfig:"RIBS_GC_GRACE_PERIOD" default:"24h"`

	// GCMinGroupAge is the minimum age of a group before it can be GC'd.
	// This prevents GC of recently created groups that might not have all
	// their S3 objects indexed yet.
	GCMinGroupAge time.Duration `envconfig:"RIBS_GC_MIN_GROUP_AGE" default:"168h"`

	// Repair settings
	// RepairEnabled enables the repair worker to create new deals for groups
	// with low retrievability.
	RepairEnabled bool `envconfig:"RIBS_REPAIR_ENABLED" default:"false"`

	// RepairWorkers is the number of concurrent repair workers.
	RepairWorkers int `envconfig:"RIBS_REPAIR_WORKERS" default:"4"`

	// RepairStagingPath is the directory for temporary repair staging files.
	// Defaults to empty which disables repair workers. Set to a path under RIBS_DATA.
	RepairStagingPath string `envconfig:"RIBS_REPAIR_STAGING_PATH" default:""`
}

// ParallelWriteConfig controls the parallel writer feature for improved write throughput.
// When enabled, multiple groups can receive writes concurrently instead of serializing
// all writes through a single group.
type ParallelWriteConfig struct {
	// Enabled controls whether parallel writes are active.
	// When false (default), behavior matches the original single-writer mode.
	Enabled bool `envconfig:"RIBS_ENABLE_PARALLEL_WRITES" default:"false"`

	// MaxParallelGroups is the maximum number of groups that can receive writes
	// concurrently. Higher values increase throughput but use more memory.
	// Recommended: 4-8 for most workloads.
	MaxParallelGroups int `envconfig:"RIBS_MAX_PARALLEL_GROUPS" default:"4"`

	// SpaceReservationTimeout is how long to wait for space reservation
	// before trying a different group.
	SpaceReservationTimeout time.Duration `envconfig:"RIBS_SPACE_RESERVATION_TIMEOUT" default:"100ms"`

	// DrainTimeout is how long to wait for writers to drain during
	// finalization transitions.
	DrainTimeout time.Duration `envconfig:"RIBS_DRAIN_TIMEOUT" default:"30s"`
}

// DealConfig configures Filecoin deal parameters.
type DealConfig struct {
	// StartTime is the delay in hours before a deal becomes active after proposal.
	// Must allow enough time for the storage provider to seal the data.
	// Minimum is typically 48 hours; 96 hours provides safety margin.
	StartTime uint `envconfig:"RIBS_DEAL_START_TIME" default:"96"`

	// Duration is the deal duration in days.
	// Maximum is ~540 days (1.5 years) due to Filecoin protocol limits.
	// 530 days is the default to stay safely under the limit.
	Duration int `envconfig:"RIBS_DEAL_DURATION" default:"530"`

	// RemoveUnsealedCopy requests the storage provider to delete the unsealed copy
	// after sealing. Reduces SP storage costs but makes retrieval slower (requires unsealing).
	RemoveUnsealedCopy bool `envconfig:"RIBS_DEAL_REMOVE_UNSEALED" default:"false"`

	// SkipIPNIAnnounce skips announcing deals to the InterPlanetary Network Indexer (IPNI).
	// Set to true for private data that shouldn't be publicly discoverable.
	SkipIPNIAnnounce bool `envconfig:"RIBS_DEAL_SKIP_IPNI_ANNOUNCE" default:"false"`

	// FallbackProviders is a comma-separated list of storage provider IDs to use
	// when CIDgravity GBAP returns no providers. Format: "f02620,f03623016,f03623017"
	// This allows deal-making to continue even when CIDgravity has no providers configured.
	FallbackProviders string `envconfig:"RIBS_DEAL_FALLBACK_PROVIDERS" default:""`

	// FallbackProvidersOnly skips CIDgravity GBAP entirely and only uses fallback providers.
	// Useful when CIDgravity is not configured or unavailable.
	FallbackProvidersOnly bool `envconfig:"RIBS_DEAL_FALLBACK_PROVIDERS_ONLY" default:"false"`
}

// WalletConfig configures the Filecoin wallet for market operations.
type WalletConfig struct {
	// UpgradeInterval is how often to check and upgrade market balance.
	UpgradeInterval time.Duration `envconfig:"RIBS_WALLET_UPGRADE_INTERVAL" default:"1m"`
}

// BalancesConfig configures automatic balance management for wallet, market, and datacap.
type BalancesConfig struct {
	// FaucetURL is the URL of the faucet service for automatic top-ups.
	// Leave empty to disable automatic faucet top-ups.
	FaucetURL string `envconfig:"RIBS_BALANCES_FAUCET_URL"`

	// WalletFilMin is the wallet FIL balance (in FIL) below which
	// a faucet top-up is requested. Default: 0.00001 FIL
	WalletFilMin float64 `envconfig:"RIBS_BALANCES_WALLET_FIL_MIN" default:"0.00001"`

	// MarketFilMin is the market balance (in FIL) below which
	// automatic top-up from wallet to market is triggered. Default: 0.00001 FIL
	MarketFilMin float64 `envconfig:"RIBS_BALANCES_MARKET_FIL_MIN" default:"0.00001"`

	// MarketFilTarget is the target market balance (in FIL) when topping up.
	// Default: 0.00004 FIL
	MarketFilTarget float64 `envconfig:"RIBS_BALANCES_MARKET_FIL_TARGET" default:"0.00004"`

	// DatacapTiBMin is the datacap balance (in TiB) below which
	// a faucet datacap top-up is requested. Default: 1 TiB
	DatacapTiBMin int `envconfig:"RIBS_BALANCES_DATACAP_TIB_MIN" default:"1"`

	// DatacapTiBRequest is the amount of datacap (in TiB) to request
	// from the faucet when topping up. Default: 10 TiB
	DatacapTiBRequest int `envconfig:"RIBS_BALANCES_DATACAP_TIB_REQUEST" default:"10"`
}

// YugabyteCqlConfig configures the Yugabyte CQL (Cassandra-compatible) connection.
type YugabyteCqlConfig struct {
	Hosts    string `envconfig:"RIBS_YUGABYTE_CQL_HOSTS" default:"yugabyte"`
	Port     int    `envconfig:"RIBS_YUGABYTE_CQL_PORT" default:"9042"`
	Keyspace string `envconfig:"RIBS_YUGABYTE_CQL_KEYSPACE" default:"filecoingw"`
	User     string `envconfig:"RIBS_YUGABYTE_CQL_USER"`
	Pass     string `envconfig:"RIBS_YUGABYTE_CQL_PASS"`

	// ForceHosts prevents Yugabyte from advertising its docker container IP.
	// Set to true for local development on MacOS or Windows WSL.
	ForceHosts bool `envconfig:"RIBS_YUGABYTE_CQL_FORCE_HOSTS" default:"false"`

	Timeout         int `envconfig:"RIBS_YUGABYTE_CQL_TIMEOUT" default:"11"`
	ConnectTimeout  int `envconfig:"RIBS_YUGABYTE_CQL_CONNECT_TIMEOUT" default:"11"`
	SocketKeepalive int `envconfig:"RIBS_YUGABYTE_CQL_SOCKET_KEEPALIVE" default:"0"`
}

// YugabyteSqlConfig configures the Yugabyte SQL (PostgreSQL-compatible) connection.
type YugabyteSqlConfig struct {
	Host string `envconfig:"RIBS_YUGABYTE_SQL_HOST" default:"yugabyte"`
	Port int    `envconfig:"RIBS_YUGABYTE_SQL_PORT" default:"5433"`
	User string `envconfig:"RIBS_YUGABYTE_SQL_USER" default:"postgres"`
	Pass string `envconfig:"RIBS_YUGABYTE_SQL_PASS" default:"postgres"`
	Db   string `envconfig:"RIBS_YUGABYTE_SQL_DB" default:"filecoingw"`

	// Connection pool settings
	// MaxOpenConns is the maximum number of open connections to the database.
	// Default: 100 (suitable for high-throughput workloads)
	MaxOpenConns int `envconfig:"RIBS_YUGABYTE_SQL_MAX_OPEN_CONNS" default:"100"`

	// MaxIdleConns is the maximum number of idle connections in the pool.
	// Should be less than or equal to MaxOpenConns.
	// Default: 25
	MaxIdleConns int `envconfig:"RIBS_YUGABYTE_SQL_MAX_IDLE_CONNS" default:"25"`

	// ConnMaxLifetimeMins is the maximum lifetime of a connection in minutes.
	// Connections older than this will be closed and replaced.
	// Default: 30 minutes
	ConnMaxLifetimeMins int `envconfig:"RIBS_YUGABYTE_SQL_CONN_MAX_LIFETIME_MINS" default:"30"`

	// ConnMaxIdleTimeMins is the maximum time a connection can be idle before being closed.
	// Default: 5 minutes
	ConnMaxIdleTimeMins int `envconfig:"RIBS_YUGABYTE_SQL_CONN_MAX_IDLE_TIME_MINS" default:"5"`
}

// S3APIConfig configures the S3-compatible API server.
type S3APIConfig struct {
	Region          string `envconfig:"RIBS_S3API_REGION" default:"EU"`
	BindAddr        string `envconfig:"RIBS_S3API_BINDADDR" default:":8078"`
	AuthEnabled     bool   `envconfig:"RIBS_S3API_AUTH_ENABLED" default:"false"`
	RootAccessKeyId string `envconfig:"RIBS_S3API_ROOT_ACCESS_KEY_ID"`
	RootSecretKey   string `envconfig:"RIBS_S3API_ROOT_SECRET_KEY"`
}

// S3CqlConfig configures the CQL connection for S3 object metadata.
// This is a SHARED keyspace across all Kuri nodes for object routing.
// Separate from YugabyteCqlConfig which is per-node for RIBS data.
type S3CqlConfig struct {
	Hosts    string `envconfig:"RIBS_S3_CQL_HOSTS" default:""`    // Falls back to YugabyteCql.Hosts if empty
	Port     int    `envconfig:"RIBS_S3_CQL_PORT" default:"0"`    // Falls back to YugabyteCql.Port if 0
	Keyspace string `envconfig:"RIBS_S3_CQL_KEYSPACE" default:""` // Falls back to YugabyteCql.Keyspace if empty
	User     string `envconfig:"RIBS_S3_CQL_USER"`
	Pass     string `envconfig:"RIBS_S3_CQL_PASS"`
}

// PrometheusConfig configures the Prometheus metrics endpoint.
type PrometheusConfig struct {
	Port int `envconfig:"RIBS_PROMETHEUS_PORT" default:"2112"`
}

// CacheConfig configures the multi-tier caching system for retrieval.
type CacheConfig struct {
	// L1SizeMiB is the size of the L1 (memory) cache in MiB.
	// This cache uses the ARC algorithm for scan resistance.
	// Default: 2048 (2GB)
	L1SizeMiB int `envconfig:"FGW_L1_CACHE_SIZE_MIB" default:"2048"`

	// L1Policy is the eviction policy for L1 cache.
	// Options: "arc" (adaptive replacement cache, default), "lru"
	L1Policy string `envconfig:"FGW_L1_POLICY" default:"arc"`

	// L2Enabled enables the L2 (SSD) cache layer.
	L2Enabled bool `envconfig:"FGW_L2_CACHE_ENABLED" default:"false"`

	// L2SizeGB is the maximum size of the L2 cache in GB.
	// Default: 256GB
	L2SizeGB int `envconfig:"FGW_L2_CACHE_SIZE_GB" default:"256"`

	// L2Path is the directory for L2 cache data.
	// Should be on fast SSD storage.
	L2Path string `envconfig:"FGW_L2_CACHE_PATH" default:"/data/cache/l2"`

	// PrefetchEnabled enables predictive prefetching.
	PrefetchEnabled bool `envconfig:"FGW_PREFETCH_ENABLED" default:"false"`

	// PrefetchWorkers is the number of prefetch worker goroutines.
	PrefetchWorkers int `envconfig:"FGW_PREFETCH_WORKERS" default:"4"`

	// PrefetchDepth is how many levels of DAG to prefetch.
	PrefetchDepth int `envconfig:"FGW_PREFETCH_DEPTH" default:"2"`
}

// FrontendConfig configures the S3 frontend proxy server.
type FrontendConfig struct {
	// NodeID is the unique identifier for this frontend node.
	NodeID string `envconfig:"FGW_NODE_ID" default:"frontend-default"`

	// NodeType is the type of this node (frontend, storage, etc.).
	NodeType string `envconfig:"FGW_NODE_TYPE" default:"frontend"`

	// BackendNodes is a comma-separated list of backend storage nodes.
	// Format: "node1:http://host1:port,node2:http://host2:port"
	BackendNodes string `envconfig:"FGW_BACKEND_NODES" default:""`

	// YCQLHosts is the comma-separated list of Yugabyte CQL hosts.
	YCQLHosts string `envconfig:"FGW_YCQL_HOSTS" default:"localhost"`

	// YCQLPort is the Yugabyte CQL port.
	YCQLPort int `envconfig:"FGW_YCQL_PORT" default:"9042"`

	// YCQLKeyspace is the Yugabyte CQL keyspace.
	YCQLKeyspace string `envconfig:"FGW_YCQL_KEYSPACE" default:"filecoingw"`

	// YCQLUser is the Yugabyte CQL user.
	YCQLUser string `envconfig:"FGW_YCQL_USER" default:""`

	// YCQLPass is the Yugabyte CQL password.
	YCQLPass string `envconfig:"FGW_YCQL_PASS" default:""`

	// InternalAPIBindAddr is the bind address for the internal API.
	InternalAPIBindAddr string `envconfig:"FGW_INTERNAL_API_BINDADDR" default:":9090"`
}

// Config is the root configuration structure containing all RIBS settings.
type Config struct {
	External      ExternalConfig
	CidGravity    CidGravityConfig
	Ribs          RibsConfig
	ParallelWrite ParallelWriteConfig
	Wallet        WalletConfig
	Balances      BalancesConfig
	Deal          DealConfig
	YugabyteCql   YugabyteCqlConfig // Per-node RIBS data (groups, deals, blockstore index)
	YugabyteSql   YugabyteSqlConfig // Per-node RIBS data (groups, deals)
	S3Cql         S3CqlConfig       // Shared S3 object metadata (routing across nodes)
	S3API         S3APIConfig
	Prometheus    PrometheusConfig
	Cache         CacheConfig    // Multi-tier cache configuration
	Frontend      FrontendConfig // S3 frontend configuration

	// LogLevel sets the logging verbosity for RIBS components.
	// Format: "level" for all components or "component=level,component=level" for specific ones.
	// Levels: debug, info, warn, error
	// Examples:
	//   "debug" - debug logging for all RIBS components
	//   "info" - info logging for all components (default)
	//   "rbs=debug,deal=info" - debug for rbs, info for deal components
	LogLevel string `envconfig:"RIBS_LOGLEVEL"`

	// LogFormat sets the log output format.
	// Options: "text" (default, human-readable) or "json" (structured, machine-readable)
	// JSON format is recommended for production with log aggregation systems.
	LogFormat string `envconfig:"RIBS_LOG_FORMAT" default:"text"`

	// Backup configures automated backup settings
	Backup BackupConfig
}

// BackupConfig configures automated backup settings for wallet and database.
type BackupConfig struct {
	// S3Endpoint is the S3-compatible endpoint for storing backups.
	// Examples: "https://s3.amazonaws.com", "http://minio.local:9000"
	S3Endpoint string `envconfig:"BACKUP_S3_ENDPOINT" default:"https://s3.amazonaws.com"`

	// S3Bucket is the bucket name for storing backups.
	S3Bucket string `envconfig:"BACKUP_S3_BUCKET"`

	// S3AccessKey is the access key for S3 authentication.
	S3AccessKey string `envconfig:"BACKUP_S3_ACCESS_KEY"`

	// S3SecretKey is the secret key for S3 authentication.
	S3SecretKey string `envconfig:"BACKUP_S3_SECRET_KEY"`

	// S3Region is the AWS region for the S3 bucket.
	S3Region string `envconfig:"BACKUP_S3_REGION" default:"us-east-1"`

	// EncryptionKeyPath is the path to the GPG encryption key file for wallet backups.
	// If empty, wallet backups will not be encrypted (NOT RECOMMENDED for production).
	EncryptionKeyPath string `envconfig:"BACKUP_ENCRYPTION_KEY_PATH"`

	// WalletBackupEnabled enables automatic wallet backup.
	WalletBackupEnabled bool `envconfig:"BACKUP_WALLET_ENABLED" default:"false"`

	// WalletBackupInterval is how often to backup the wallet.
	WalletBackupInterval string `envconfig:"BACKUP_WALLET_INTERVAL" default:"4h"`

	// DatabaseBackupEnabled enables automatic database backup.
	DatabaseBackupEnabled bool `envconfig:"BACKUP_DATABASE_ENABLED" default:"false"`

	// DatabaseBackupInterval is how often to backup the database.
	DatabaseBackupInterval string `envconfig:"BACKUP_DATABASE_INTERVAL" default:"24h"`
}

var config Config

func GetConfig() *Config {
	return &config
}

// GetS3CqlConfig returns the effective S3 CQL configuration.
// Falls back to YugabyteCql settings if S3-specific settings are not configured.
func (c *Config) GetS3CqlConfig() YugabyteCqlConfig {
	s3cfg := YugabyteCqlConfig{
		Hosts:           c.S3Cql.Hosts,
		Port:            c.S3Cql.Port,
		Keyspace:        c.S3Cql.Keyspace,
		User:            c.S3Cql.User,
		Pass:            c.S3Cql.Pass,
		ForceHosts:      c.YugabyteCql.ForceHosts,
		Timeout:         c.YugabyteCql.Timeout,
		ConnectTimeout:  c.YugabyteCql.ConnectTimeout,
		SocketKeepalive: c.YugabyteCql.SocketKeepalive,
	}
	// Fall back to RIBS CQL settings if S3-specific not set
	if s3cfg.Hosts == "" {
		s3cfg.Hosts = c.YugabyteCql.Hosts
	}
	if s3cfg.Port == 0 {
		s3cfg.Port = c.YugabyteCql.Port
	}
	if s3cfg.Keyspace == "" {
		s3cfg.Keyspace = c.YugabyteCql.Keyspace
	}
	if s3cfg.User == "" {
		s3cfg.User = c.YugabyteCql.User
	}
	if s3cfg.Pass == "" {
		s3cfg.Pass = c.YugabyteCql.Pass
	}
	return s3cfg
}

func LoadConfig() error {
	if err := envconfig.Process("", &config); err != nil {
		return err
	}

	// Configure log format first (before any logging)
	if err := configureLogFormat(config.LogFormat); err != nil {
		return xerrors.Errorf("configuring log format: %w", err)
	}

	rcfg := config.Ribs
	if rcfg.MinimumRetrievableCount > rcfg.MinimumReplicaCount {
		return xerrors.Errorf("MinimunRetriveable count greater than MinimumReplica: %d > %d\n", rcfg.MinimumRetrievableCount, rcfg.MinimumReplicaCount)
	}
	if rcfg.MinimumReplicaCount > rcfg.MaximumReplicaCount {
		return xerrors.Errorf("MinimunReplica count greater than MaximumReplica: %d > %d\n", rcfg.MinimumReplicaCount, rcfg.MaximumReplicaCount)
	}
	if rcfg.RetrievableRepairThreshold > rcfg.MinimumReplicaCount {
		return xerrors.Errorf("RetrievableRepairThreshold greater than MinimumReplicaCount: %d > %d\n", rcfg.RetrievableRepairThreshold, rcfg.MinimumRetrievableCount)
	}
	if rcfg.RetrievableRepairThreshold < 0 {
		return xerrors.Errorf("RetrievableRepairThreshold negative: %d < 0\n", rcfg.RetrievableRepairThreshold)
	}
	if rcfg.MaxStagingGroupCount == 0 {
		config.Ribs.MaxStagingGroupCount = rcfg.MaxLocalGroupCount
	}
	config.CidGravity.AltTokens = make(map[string]string)
	for _, client := range config.CidGravity.AltClients {
		token := os.Getenv("CIDGRAVITY_API_TOKEN_" + client)
		if token == "" {
			return xerrors.Errorf("AltClients %s token not provided\n", client)
		}
		config.CidGravity.AltTokens[client] = token
	}
	err := config.configureLogLevels()
	if err != nil {
		return err
	}

	// Validate parallel write config
	pwcfg := config.ParallelWrite
	if pwcfg.Enabled {
		if pwcfg.MaxParallelGroups < 1 {
			return xerrors.Errorf("MaxParallelGroups must be at least 1, got %d\n", pwcfg.MaxParallelGroups)
		}
		if pwcfg.MaxParallelGroups > 16 {
			log.Warnw("MaxParallelGroups is very high, this may cause memory issues", "value", pwcfg.MaxParallelGroups)
		}
		log.Infow("Parallel writes enabled",
			"maxGroups", pwcfg.MaxParallelGroups,
			"reservationTimeout", pwcfg.SpaceReservationTimeout,
			"drainTimeout", pwcfg.DrainTimeout)
	}

	log.Debugw("Loaded config")
	return nil
}

func (c *Config) configureLogLevels() error {
	if c.LogLevel == "" {
		return nil
	}
	levels := strings.Split(c.LogLevel, ",")
	for _, level := range levels {
		s := strings.Split(level, "=")
		if len(s) != 2 {
			return fmt.Errorf("invalid log level: %s", level)
		}
		err := logging.SetLogLevelRegex(s[0], s[1])
		if err != nil {
			return fmt.Errorf("invalid log level: %s, error: %w", s[0], err)
		}
	}
	return nil
}

// configureLogFormat sets the global logging format.
// Supported formats: "text" (default), "json"
func configureLogFormat(format string) error {
	switch strings.ToLower(format) {
	case "json":
		// Configure JSON output for structured logging
		logging.SetupLogging(logging.Config{
			Format: logging.JSONOutput,
			Stdout: true,
		})
	case "text", "":
		// Default text format (do nothing, it's the default)
		logging.SetupLogging(logging.Config{
			Format: logging.ColorizedOutput,
			Stdout: true,
		})
	default:
		return fmt.Errorf("unsupported log format: %s (supported: text, json)", format)
	}
	return nil
}
