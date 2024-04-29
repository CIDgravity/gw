package configuration

import (
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/kelseyhightower/envconfig"
)

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
	DataDir             string `envconfig:"RIBS_DATA" default:"~/.ribsdata"`
	SendExtends         bool   `envconfig:"RIBS_SEND_EXTENDS"`
	FilecoinApiEndpoint string `envconfig:"RIBS_FILECOIN_API_ENDPOINT" default:"https://api.chain.love/rpc/v1"`
	MinimumReplicaCount int    `envconfig:"RIBS_MINIMUM_REPLICA_COUNT" default:"5"`
	TargetReplicaCount  int    `envconfig:"RIBS_TARGET_REPLICA_COUNT" default:"10"`
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
	config.Loaded = true
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
