package kuboribs

import (
	"context"
	"fmt"
    "github.com/ipfs/boxo/blockservice"
    "github.com/ipfs/boxo/exchange/offline"
	"os"

	lotusbstore "github.com/filecoin-project/lotus/blockstore"
	blockstore "github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log"

	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/boxo/mfs"

	"github.com/ipfs/kubo/core"
	"github.com/ipfs/kubo/core/node"
	"github.com/ipfs/kubo/core/node/helpers"
	"github.com/ipfs/kubo/plugin"
	"github.com/ipfs/kubo/repo"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/lotus-web3/ribs"
	ribsbstore "github.com/lotus-web3/ribs/integrations/blockstore"
	"github.com/lotus-web3/ribs/integrations/web"
	"github.com/lotus-web3/ribs/rbdeal"
	"github.com/lotus-web3/ribs/configuration"
	"github.com/mitchellh/go-homedir"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
)

var log = logging.Logger("ribs:plugin")

var Plugin plugin.Plugin = &ribsPlugin{}

// ribsPlugin is used for testing the fx plugin.
// It merely adds an fx option that logs a debug statement, so we can verify that it works in tests.
type ribsPlugin struct{}

var _ plugin.PluginFx = (*ribsPlugin)(nil)

func (p *ribsPlugin) Name() string {
	return "ribs-bs"
}

func (p *ribsPlugin) Version() string {
	return "0.0.0"
}

func (p *ribsPlugin) Init(env *plugin.Environment) error {
	return nil
}

func (p *ribsPlugin) Options(info core.FXNodeInfo) ([]fx.Option, error) {
	opts := info.FXOptions
	opts = append(opts,
		fx.Provide(makeRibs),
		fx.Provide(ribsBlockstore),
		fx.Provide(ribsMetadata),

		fx.Decorate(func(rbs *ribsbstore.Blockstore) node.BaseBlocks {
			return rbs
		}),

		fx.Decorate(func(bb node.BaseBlocks, rbs *ribsbstore.Blockstore) (gclocker blockstore.GCLocker, gcbs blockstore.GCBlockstore, bs blockstore.Blockstore) {
			gclocker = &flushingGCLocker{
				flusher: rbs,
			}
			gcbs = blockstore.NewGCBlockstore(bb, gclocker)

			bs = gcbs
			return
		}),

		fx.Decorate(RibsFiles),

		fx.Invoke(StartMfsDav),
		fx.Invoke(StartMfsNFSFs),
		fx.Invoke(StartMeta),
	)
	return opts, nil
}

// node.BaseBlocks, blockstore.Blockstore, blockstore.GCLocker, blockstore.GCBlockstore

type ribsIn struct {
	fx.In

	Lc fx.Lifecycle
	H  host.Host `optional:"true"`
}

func makeRibs(ri ribsIn) (ribs.RIBS, error) {
	var opts []rbdeal.OpenOption
	if ri.H != nil {
		opts = append(opts, rbdeal.WithHostGetter(func(...libp2p.Option) (host.Host, error) {
			return ri.H, nil
		}))
	}

	cfg := configuration.GetConfig()
	dataDir, err := homedir.Expand(cfg.Ribs.DataDir)
	if err != nil {
		return nil, xerrors.Errorf("expand data dir: %w", err)
	}

	r, err := rbdeal.Open(dataDir, opts...)
	if err != nil {
		return nil, xerrors.Errorf("open ribs: %w", err)
	}

	ri.Lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			return r.Close()
		},
	})

	if ri.H != nil || true {
		go func() {
			if err := web.Serve(context.TODO(), ":9010", r); err != nil {
				panic("ribsweb serve failed")
			}
		}()
		_, _ = fmt.Fprintf(os.Stderr, "RIBSWeb at http://%s\n", "127.0.0.1:9010")
	}

	return r, nil
}

func ribsBlockstore(r ribs.RIBS, lc fx.Lifecycle) *ribsbstore.Blockstore {
	rbs := ribsbstore.New(context.TODO(), r)

	// assert interface
	var _ blockstore.Blockstore = rbs

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			return rbs.Close()
		},
	})

	return rbs
}

func ribsMetadata(r ribs.RIBS /*, lc fx.Lifecycle */) ribs.MetadataDB {
	rbmeta := r.MetaDB()

	/*
	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			return ribs.Close()
		},
	})*/

	return rbmeta
}

// Adder Durability

type flushingGCLocker struct {
	flusher lotusbstore.Flusher
}

func (d *flushingGCLocker) Unlock(ctx context.Context) {
	// This is a potentially disturbing hack, used to gain a lot of performance
	// while still maintaining reasonable durability guarantees.
	// Normally unixfs Add will call PutMany with ~4-10 blocks, and expect the
	// blockstore to sync that - which is horrendously slow, and not really
	// needed.
	// Here we exploit the fact that the adder takes a GC lock once for the whole
	// add operation, so we just flush the blockstore here, which still guarantees
	// that the data is durable after the adder returns.
	err := d.flusher.Flush(ctx)
	if err != nil {
		log.Errorw("flushing blockstore through GCLocker", "error", err)
	}
}

func (d *flushingGCLocker) GCLock(ctx context.Context) blockstore.Unlocker {
	panic("no gc")
}

func (d *flushingGCLocker) PinLock(ctx context.Context) blockstore.Unlocker {
	return d
}

func (d *flushingGCLocker) GCRequested(ctx context.Context) bool {
	return false
}

var _ blockstore.GCLocker = (*flushingGCLocker)(nil)

// MFS Durability

func RibsFiles(mctx helpers.MetricsCtx, lc fx.Lifecycle, repo repo.Repo, rbs *ribsbstore.Blockstore, mdb ribs.MetadataDB) (*mfs.Root, error) {
    bsv := blockservice.New(rbs, offline.Exchange(rbs))
    dag := merkledag.NewDAGService(bsv)

    dsk := datastore.NewKey("/local/filesroot")
    pf := func(ctx context.Context, c cid.Cid) error {
        rootDS := repo.Datastore()
        /*if err := rootDS.Sync(ctx, blockstore.BlockPrefix); err != nil {
            return err
		}
		if err := rootDS.Sync(ctx, filestore.FilestorePrefix); err != nil {
			return err
		}*/
		log.Infow("new files root", "cid", c.String())

		if err := rbs.Flush(ctx); err != nil {
			return xerrors.Errorf("ribs flush: %w", err)
		}

		if err := rootDS.Put(ctx, dsk, c.Bytes()); err != nil {
			return err
		}
		if err := mdb.WriteDir("/", c.String()); err != nil {
			log.Errorw("Metadata: failed to write new root", "error", err)
		}

		return rootDS.Sync(ctx, dsk)
	}

	var nd *merkledag.ProtoNode
	ctx := helpers.LifecycleCtx(mctx, lc)
	val, err := repo.Datastore().Get(ctx, dsk)

	switch {
	case err == datastore.ErrNotFound || val == nil:
		nd = unixfs.EmptyDirNode()
		err := dag.Add(ctx, nd)
		if err != nil {
			return nil, fmt.Errorf("failure writing to dagstore: %s", err)
		}
	case err == nil:
		c, err := cid.Cast(val)
		if err != nil {
			return nil, err
		}

		rnd, err := dag.Get(ctx, c)
		if err != nil {
			return nil, fmt.Errorf("error loading filesroot from DAG: %s", err)
		}

		pbnd, ok := rnd.(*merkledag.ProtoNode)
		if !ok {
			return nil, merkledag.ErrNotProtobuf
		}

		nd = pbnd
	default:
		return nil, err
	}

	root, err := mfs.NewRoot(ctx, dag, nd, pf)

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			if root == nil {
				return nil
			}

			return root.Close()
		},
	})

	if err := mdb.WriteDir("/", nd.Cid().String()); err != nil {
		log.Errorw("Metadata: failed to write base root", "error", err)
	}

	return root, err
}
