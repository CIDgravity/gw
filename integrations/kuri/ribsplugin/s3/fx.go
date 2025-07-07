package s3

import (
	"context"
	"errors"

	"go.uber.org/fx"

	"github.com/filecoin-project/go-hamt-ipld"
	"github.com/ipfs/boxo/blockservice"
	chunk "github.com/ipfs/boxo/chunker"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	cbor "github.com/ipfs/go-ipld-cbor"
	logging "github.com/ipfs/go-log"
	"github.com/ipfs/kubo/core/node/helpers"
	"github.com/ipfs/kubo/repo"

	agw_s3 "github.com/aurorainfra/gw/agw/server/s3"
	"github.com/aurorainfra/gw/configuration"
	ribsbstore "github.com/aurorainfra/gw/integrations/blockstore"
)

var log = logging.Logger("kuri/s3")

func MakeS3Server(mctx helpers.MetricsCtx, lc fx.Lifecycle, repo repo.Repo, rbs *ribsbstore.Blockstore) (*agw_s3.S3Server, error) {
	log.Info("Starting S3 plugin")

	lctx := helpers.LifecycleCtx(mctx, lc)
	bsv := blockservice.New(rbs, offline.Exchange(rbs))
	dag := merkledag.NewDAGService(bsv)
	ipldStore := cbor.NewCborStore(rbs)

	node, err := loadHamtNode(lctx, repo, ipldStore)
	if err != nil {
		return nil, err
	}

	cfg := configuration.GetConfig()
	region := &Region{
		name: cfg.S3API.Region,
		index: &Index{
			node:  node,
			store: ipldStore,
		},

		blockstore:  rbs,
		dag:         dag,
		splitterGen: chunk.SizeSplitterGen(1024 * 1024),
		repo:        repo,
	}

	return agw_s3.NewS3Server(region), nil
}

func loadHamtNode(ctx context.Context, repo repo.Repo, store cbor.IpldStore) (*hamt.Node, error) {
	key := datastore.NewKey("/local/s3/index")
	value, err := repo.Datastore().Get(ctx, key)

	switch {
	case errors.Is(err, datastore.ErrNotFound) || value == nil:
		node := hamt.NewNode(store)
		return node, nil
	case err == nil:
		c, err := cid.Cast(value)
		if err != nil {
			return nil, err
		}

		return hamt.LoadNode(ctx, store, c)
	default:
		return nil, err
	}
}
