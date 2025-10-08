package s3

import (
	"github.com/CIDgravity/filecoin-gateway/configuration"
	"github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/CIDgravity/filecoin-gateway/integrations/blockstore"
	"github.com/CIDgravity/filecoin-gateway/rbstor/cidlocation"
	"github.com/CIDgravity/filecoin-gateway/server/s3"
	"go.uber.org/fx"

	"github.com/ipfs/boxo/blockservice"
	chunk "github.com/ipfs/boxo/chunker"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	logging "github.com/ipfs/go-log"
	"github.com/ipfs/kubo/repo"
)

var log = logging.Logger("gw/s3")

var Module = fx.Module(
	"s3",
	fx.Provide(
		MakeS3Server,
		s3.NewAuthenticator,
	),
)

type ServerIn struct {
	fx.In
	Repo              repo.Repo
	Rbs               *ribsbstore.Blockstore
	Index             iface.S3ObjectIndex
	CidLocationWorker *cidlocation.Worker
	Auth              *s3.Authenticator
	Cfg               *configuration.S3APIConfig
}

func MakeS3Server(in ServerIn) (*s3.S3Server, error) {
	log.Info("Starting S3 plugin")

	bsv := blockservice.New(in.Rbs, offline.Exchange(in.Rbs))
	dag := merkledag.NewDAGService(bsv)

	region := &Region{
		name:  in.Cfg.Region,
		index: in.Index,

		blockstore:  in.Rbs,
		dag:         dag,
		splitterGen: chunk.SizeSplitterGen(1024 * 1024),
		repo:        in.Repo,
		cidlocation: in.CidLocationWorker,

		buckets: map[string]iface.Bucket{},
	}

	return s3.NewS3Server(region, in.Auth), nil
}
