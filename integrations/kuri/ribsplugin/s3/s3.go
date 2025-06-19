package s3

import (
	"context"
	"errors"
	"github.com/filecoin-project/go-hamt-ipld"
	"github.com/ipfs/boxo/blockservice"
	chunk "github.com/ipfs/boxo/chunker"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	cbor "github.com/ipfs/go-ipld-cbor"
	format "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log"
	"github.com/ipfs/kubo/core/node/helpers"
	"github.com/ipfs/kubo/repo"
	ribsbstore "github.com/lotus-web3/ribs/integrations/blockstore"
	"go.uber.org/fx"
	"sync"
)

var log = logging.Logger("ribs:plugin:s3")

type Context struct {
	index       Index
	blockstore  *ribsbstore.Blockstore
	dag         format.DAGService
	lctx        context.Context
	splitterGen chunk.SplitterGen
	repo        repo.Repo
}

func StartS3Plugin(mctx helpers.MetricsCtx, lc fx.Lifecycle, repo repo.Repo, rbs *ribsbstore.Blockstore) error {
	log.Infow("Starting S3 plugin")
	lctx := helpers.LifecycleCtx(mctx, lc)
	bsv := blockservice.New(rbs, offline.Exchange(rbs))
	dag := merkledag.NewDAGService(bsv)
	ipldStore := cbor.NewCborStore(rbs)

	node, err := loadHamtNode(lctx, repo, ipldStore)
	if err != nil {
		return err
	}

	s3Context := &Context{
		index: Index{
			node,
			sync.RWMutex{},
			ipldStore,
		},

		blockstore:  rbs,
		dag:         dag,
		lctx:        lctx,
		splitterGen: chunk.SizeSplitterGen(1024 * 1024),
		repo:        repo,
	}
	startS3Server(lc, s3Context)
	return nil
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
