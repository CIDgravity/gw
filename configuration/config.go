package configuration

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/chain/types"
	logging "github.com/ipfs/go-log/v2"
	"github.com/kelseyhightower/envconfig"
	"golang.org/x/xerrors"
)

var log = logging.Logger("ribs:config")

type LocalwebConfig struct {
	Path string `envconfig:"EXTERNAL_LOCALWEB_PATH"`
	Url  string `envconfig:"EXTERNAL_LOCALWEB_URL"`

	BuiltinServer bool   `envconfig:"EXTERNAL_LOCALWEB_BUILTIN_SERVER" default:"true"`
	ServerPort    string `envconfig:"EXTERNAL_LOCALWEB_SERVER_PORT" default:"8443"`
	ServerTLS     bool   `envconfig:"EXTERNAL_LOCALWEB_SERVER_TLS" default:"true"`

	MaxConcurrentUploadsPerDeal int `envconfig:"EXTERNAL_LOCALWEB_MAX_CONCURRENT_UPLOADS_PER_DEAL" default:"15"`
}

type S3Config struct {
	Endpoint  string `envconfig:"EXTERNAL_S3_ENDPOINT"`
	Region    string `envconfig:"EXTERNAL_S3_REGION"`
	AccessKey string `envconfig:"EXTERNAL_S3_ACCESS_KEY"`
	SecretKey string `envconfig:"EXTERNAL_S3_SECRET_KEY"`
	Token     string `envconfig:"EXTERNAL_S3_TOKEN"`
	Bucket    string `envconfig:"EXTERNAL_S3_BUCKET"`
	BucketUrl string `envconfig:"EXTERNAL_S3_BUCKET_URL"`
}

type ExternalConfig struct {
	Localweb LocalwebConfig
	S3       S3Config
}

type CidGravityConfig struct {
	ApiToken                string   `envconfig:"CIDGRAVITY_API_TOKEN"`
	ApiEndpointGetProviders string   `envconfig:"CIDGRAVITY_API_ENDPOINT_GBAP" default:"https://service.cidgravity.com/private/v1/get-best-available-providers"`
	ApiEndpointGetDeals     string   `envconfig:"CIDGRAVITY_API_ENDPOINT_GOCD" default:"https://service.cidgravity.com/private/v1/get-on-chain-deals"`
	MaxConns                int64    `envconfig:"CIDGRAVITY_MAX_CONNECTIONS" default:"4"`
	AltClients              []string `envconfig:"CIDGRAVITY_ALT_CLIENTS"`
	AltTokens               map[string]string
}
type RibsConfig struct {
	DataDir                    string        `envconfig:"RIBS_DATA" default:"~/.ribsdata"`
	SendExtends                bool          `envconfig:"RIBS_SEND_EXTENDS"`
	FilecoinApiEndpoint        string        `envconfig:"RIBS_FILECOIN_API_ENDPOINT" default:"https://api.chain.love/rpc/v1"`
	MinimumRetrievableCount    int           `envconfig:"RIBS_MINIMUM_RETRIEVABLE_COUNT" default:"5"`
	MinimumReplicaCount        int           `envconfig:"RIBS_MINIMUM_REPLICA_COUNT" default:"5"`
	MaximumReplicaCount        int           `envconfig:"RIBS_MAXIMUM_REPLICA_COUNT" default:"10"`
	RetrievableRepairThreshold int           `envconfig:"RIBS_RETRIEVALBLE_REPAIR_THRESHOLD" default:"3"`
	MaxLocalGroupCount         int           `envconfig:"RIBS_MAX_LOCAL_GROUP_COUNT" default:"64"`
	MaxStagingGroupCount       int           `envconfig:"RIBS_MAX_STAGING_GROUP_COUNT" default:"0"`
	DealCheckInterval          time.Duration `envconfig:"RIBS_DEAL_CHECK_INTERVAL" default:"30s"`
	DealCanSendCommand         string        `envconfig:"RIBS_DEAL_CAN_SEND_COMMAND" default:""`
	MongoDBUri                 string        `envconfig:"RIBS_MONGODB_URI"`
	RunSpCrawler               bool          `envconfig:"RIBS_RUN_SP_CRAWLER" default:"true"`
	CidLocationWorkerCount     int           `envconfig:"RIBS_CID_LOCATION_WORKER_COUNT" default:"128"`
}
type DealConfig struct {
	StartTime          uint `envconfig:"RIBS_DEAL_START_TIME" default:"96"` // hours
	Duration           int  `envconfig:"RIBS_DEAL_DURATION" default:"530"`  // days
	RemoveUnsealedCopy bool `envconfig:"RIBS_DEAL_REMOVE_UNSEALED" default:"false"`
	SkipIPNIAnnounce   bool `envconfig:"RIBS_DEAL_SKIP_IPNI_ANNOUNCE" default:"false"`
}
type WalletConfig struct {
	MinMarketBalance  big.Int       `envconfig:"RIBS_WALLET_MIN_BALANCE" default:"100_000_000_000_000_000"`    // 100 mFil
	AutoMarketBalance big.Int       `envconfig:"RIBS_WALLET_AUTO_BALANCE" default:"1_000_000_000_000_000_000"` // 1 Fil
	UpgradeInterval   time.Duration `envconfig:"RIBS_WALLET_UPGRADE_INTERVAL" default:"1m"`
}

type YugabyteCqlConfig struct {
	Hosts    string `envconfig:"RIBS_YUGABYTE_CQL_HOSTS" default:"127.0.0.1"`
	Port     int    `envconfig:"RIBS_YUGABYTE_CQL_PORT" default:"9042"`
	Keyspace string `envconfig:"RIBS_YUGABYTE_CQL_KEYSPACE" default:"filecoingw"`
	User     string `envconfig:"RIBS_YUGABYTE_CQL_USER"`
	Pass     string `envconfig:"RIBS_YUGABYTE_CQL_PASS"`

	// Yugabyte deployed in docker on MacOS and Windows WSL will advertise its docker container IP, which will replace the configured host and break the connection.
	// Set to true for local development on those systems
	ForceHosts bool `envconfig:"RIBS_YUGABYTE_CQL_FORCE_HOSTS" default:"false"`

	Timeout         int `envconfig:"RIBS_YUGABYTE_CQL_TIMEOUT" default:"11"`
	ConnectTimeout  int `envconfig:"RIBS_YUGABYTE_CQL_CONNECT_TIMEOUT" default:"11"`
	SocketKeepalive int `envconfig:"RIBS_YUGABYTE_CQL_SOCKET_KEEPALIVE" default:"0"`
}

type YugabyteSqlConfig struct {
	Host string `envconfig:"RIBS_YUGABYTE_SQL_HOST" default:"127.0.0.1"`
	Port int    `envconfig:"RIBS_YUGABYTE_SQL_PORT" default:"5433"`
	User string `envconfig:"RIBS_YUGABYTE_SQL_USER" default:"postgres"`
	Pass string `envconfig:"RIBS_YUGABYTE_SQL_PASS" default:"postgres"`
	Db   string `envconfig:"RIBS_YUGABYTE_SQL_DB" default:"filecoingw"`
}

type S3APIConfig struct {
	Region          string `envconfig:"RIBS_S3API_REGION" default:"EU"`
	BindAddr        string `envconfig:"RIBS_S3API_BINDADDR" default:":8078"`
	AuthEnabled     bool   `envconfig:"RIBS_S3API_AUTH_ENABLED" default:"false"`
	RootAccessKeyId string `envconfig:"RIBS_S3API_ROOT_ACCESS_KEY_ID"`
	RootSecretKey   string `envconfig:"RIBS_S3API_ROOT_SECRET_KEY"`
}

type PrometheusConfig struct {
	Port int `envconfig:"RIBS_PROMETHEUS_PORT" default:"2112"`
}

type Config struct {
	External    ExternalConfig
	CidGravity  CidGravityConfig
	Ribs        RibsConfig
	Wallet      WalletConfig
	Deal        DealConfig
	YugabyteCql YugabyteCqlConfig
	YugabyteSql YugabyteSqlConfig
	S3API       S3APIConfig
	Prometheus  PrometheusConfig
	LogLevel    string `envconfig:"RIBS_LOGLEVEL"`
}

var config Config

func GetConfig() *Config {
	return &config
}

func LoadConfig() error {
	// need to initialize those types so they are not nil
	config.Wallet.MinMarketBalance = types.NewInt(0)
	config.Wallet.AutoMarketBalance = types.NewInt(0)
	if err := envconfig.Process("", &config); err != nil {
		return err
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
	if !config.Wallet.AutoMarketBalance.GreaterThan(config.Wallet.MinMarketBalance) {
		// auto > min
		// allow auto == min == 0
		if config.Wallet.MinMarketBalance.GreaterThan(types.NewInt(0)) {
			return xerrors.Errorf("AutoMarketBalance must be greater than MinMarketBalance\n")
		}
	}

	log.Debugw("Loaded config")
	return nil
}

func (c *Config) configureLogLevels() error {
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
