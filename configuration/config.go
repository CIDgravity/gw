package configuration

import (
	"os"
	"strings"
	"time"
	logging "github.com/ipfs/go-log/v2"
	"github.com/kelseyhightower/envconfig"
	"golang.org/x/xerrors"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/go-state-types/big"
)

var log = logging.Logger("ribs:config")

type LocalwebConfig struct {
	Path string `envconfig:"EXTERNAL_LOCALWEB_PATH"`
	Url  string `envconfig:"EXTERNAL_LOCALWEB_URL"`

	BuiltinServer bool   `envconfig:"EXTERNAL_LOCALWEB_BUILTIN_SERVER" default:"true"`
	ServerPort string `envconfig:"EXTERNAL_LOCALWEB_SERVER_PORT" default:"8443"`
	ServerTLS  bool   `envconfig:"EXTERNAL_LOCALWEB_SERVER_TLS" default:"true"`
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
	ApiToken                string            `envconfig:"CIDGRAVITY_API_TOKEN"`
	ApiEndpointGetProviders string            `envconfig:"CIDGRAVITY_API_ENDPOINT_GBAP" default:"https://service.cidgravity.com/private/v1/get-best-available-providers"`
	ApiEndpointGetDeals     string            `envconfig:"CIDGRAVITY_API_ENDPOINT_GOCD" default:"https://service.cidgravity.com/private/v1/get-on-chain-deals"`
	MaxConns                int64             `envconfig:"CIDGRAVITY_MAX_CONNECTIONS" default:"4"`
	AltClients              []string          `envconfig:"CIDGRAVITY_ALT_CLIENTS"`
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
	DealCheckInterval          time.Duration `envconfig:"RIBS_DEAL_CHECK_INTERVAL" default:"30s"`
	DealCanSendCommand         string        `envconfig:"RIBS_DEAL_CAN_SEND_COMMAND" default:""`
	MongoDBUri                 string        `envconfig:"RIBS_MONGODB_URI"`
	RunSpCrawler               bool          `envconfig:"RIBS_RUN_SP_CRAWLER" default:false`
}
type DealConfig struct {
	StartTime          uint `envconfig:"RIBS_DEAL_START_TIME" default:"96"` // hours
	Duration           int  `envconfig:"RIBS_DEAL_DURATION" default:"530"`  // days
	RemoveUnsealedCopy bool `envconfig:"RIBS_DEAL_REMOVE_UNSEALED" default:false`
	SkipIPNIAnnounce   bool `envconfig:"RIBS_DEAL_SKIP_IPNI_ANNOUNCE" default:false`
}
type WalletConfig struct {
	MinMarketBalance    big.Int  `envconfig:"RIBS_WALLET_MIN_BALANCE" default:"100_000_000_000_000_000"` // 100 mFil
	AutoMarketBalance   big.Int  `envconfig:"RIBS_WALLET_AUTO_BALANCE" default:"1_000_000_000_000_000_000"` // 1 Fil
	UpgradeInterval     time.Duration `envconfig:"RIBS_WALLET_UPGRADE_INTERVAL" default:"1m"`
}

type Config struct {
	Loaded     bool
	External   ExternalConfig
	CidGravity CidGravityConfig
	Ribs       RibsConfig
	Wallet     WalletConfig
	Deal       DealConfig
	LogLevel   string           `envconfig:"RIBS_LOGLEVEL"`
}

var config Config

func GetConfig() *Config {
	if !config.Loaded {
		err := LoadConfig()
		if err != nil {
			panic(err)
		}
	}
	return &config
}

func LoadConfig() error {
	// neew to initialize those types so they are not nil
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
	config.CidGravity.AltTokens = make(map[string]string)
	for _, client := range config.CidGravity.AltClients {
		token := os.Getenv("CIDGRAVITY_API_TOKEN_" + client)
		if token == "" {
			return xerrors.Errorf("AltClients %s token not provided\n", client)
		}
		config.CidGravity.AltTokens[client] = token
	}
        if config.LogLevel != "" {
                for _, kvs := range strings.Split(config.LogLevel, ",") {
                        kv := strings.SplitN(kvs, "=", 2)
                        lvl := kv[len(kv)-1]
                        switch len(kv) {
                        case 1:
				if err := logging.SetLogLevelRegex("ribs:.*", lvl); err != nil {
					log.Fatal("Failed to initialize ribs loglevel", "error", err.Error())
				}
                        case 2:
				if err := logging.SetLogLevelRegex("ribs:" + kv[0], lvl); err != nil {
					log.Fatal("Failed to initialize ribs loglevel", "error", err.Error())
				}
                        }
                }
        }
	if !config.Wallet.AutoMarketBalance.GreaterThan(config.Wallet.MinMarketBalance) {
		// auto > min
		// allow auto == min == 0
		if config.Wallet.MinMarketBalance.GreaterThan(types.NewInt(0)) {
			return xerrors.Errorf("AutoMarketBalance must be greater than MinMarketBalance\n")
		}
	}

	config.Loaded = true
	log.Debugw("Loaded config", "config", config)
	return nil
}
