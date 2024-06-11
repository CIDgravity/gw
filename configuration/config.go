package configuration

import (
	"fmt"
	"github.com/davecgh/go-spew/spew"
	logging "github.com/ipfs/go-log/v2"
	"github.com/kelseyhightower/envconfig"
	"golang.org/x/xerrors"
)

var log = logging.Logger("config")

type LocalwebConfig struct {
	Path string `envconfig:"EXTERNAL_LOCALWEB_PATH"`
	Url  string `envconfig:"EXTERNAL_LOCALWEB_URL"`
}

type ExternalConfig struct {
	Localweb LocalwebConfig
}

type S3Config struct {
	Endpoint  string `envconfig:"S3_ENDPOINT"`
	Region    string `envconfig:"S3_REGION"`
	AccessKey string `envconfig:"S3_ACCESS_KEY"`
	SecretKey string `envconfig:"S3_SECRET_KEY"`
	Token     string `envconfig:"S3_TOKEN"`
	Bucket    string `envconfig:"S3_BUCKET"`
	BucketUrl string `envconfig:"S3_BUCKET_URL"`
}
type CidGravityConfig struct {
	ApiToken                string `envconfig:"CIDGRAVITY_API_TOKEN"`
	ApiEndpointGetProviders string `envconfig:"CIDGRAVITY_API_ENDPOINT_GBAP" default:"https://service.cidgravity.com/private/v1/get-best-available-providers"`
}
type RibsConfig struct {
	DataDir                    string `envconfig:"RIBS_DATA" default:"~/.ribsdata"`
	SendExtends                bool   `envconfig:"RIBS_SEND_EXTENDS"`
	FilecoinApiEndpoint        string `envconfig:"RIBS_FILECOIN_API_ENDPOINT" default:"https://api.chain.love/rpc/v1"`
	MinimumRetrievableCount    int    `envconfig:"RIBS_MINIMUM_RETRIEVABLE_COUNT" default:"5"`
	MinimumReplicaCount        int    `envconfig:"RIBS_MINIMUM_REPLICA_COUNT" default:"5"`
	MaximumReplicaCount        int    `envconfig:"RIBS_MAXIMUM_REPLICA_COUNT" default:"10"`
	RetrievableRepairThreshold int    `envconfig:"RIBS_RETRIEVALBLE_REPAIR_THRESHOLD" default:"3"`
	MaxLocalGroupCount         int    `envconfig:"RIBS_MAX_LOCAL_GROUP_COUNT" default:"64"`
}
type DealConfig struct {
	StartTime          uint `envconfig:"RIBS_DEAL_START_TIME" default:"96"` // hours
	Duration           int  `envconfig:"RIBS_DEAL_DURATION" default:"530"`  // days
	RemoveUnsealedCopy bool `envconfig:"RIBS_DEAL_REMOVE_UNSEALED" default:false`
	SkipIPNIAnnounce   bool `envconfig:"RIBS_DEAL_SKIP_IPNI_ANNOUNCE" default:false`
}

type Config struct {
	Loaded     bool
	S3         S3Config
	External   ExternalConfig
	CidGravity CidGravityConfig
	Ribs       RibsConfig
	Deal       DealConfig
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
	config.Loaded = true
	sconfig := spew.Sdump(config)
	log.Errorf("Config: %s", sconfig)
	return nil
}

func main() {
	err := LoadConfig()
	if err != nil {
		fmt.Printf("Error load config: %+#v\n", err)
	}
	cfg := GetConfig()
	spew.Dump(cfg)
}
