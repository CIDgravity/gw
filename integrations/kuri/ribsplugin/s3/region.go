package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/CIDgravity/filecoin-gateway/integrations/blockstore"
	"github.com/CIDgravity/filecoin-gateway/rbstor/cidlocation"
	"github.com/CIDgravity/filecoin-gateway/server/metrics"
	chunk "github.com/ipfs/boxo/chunker"
	dag "github.com/ipfs/boxo/ipld/merkledag"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	"github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/kubo/repo"
)

type Region struct {
	name        string
	nodeID      string // Node ID for scalable architecture
	index       iface.S3ObjectIndex
	blockstore  *ribsbstore.Blockstore
	dag         format.DAGService
	splitterGen chunk.SplitterGen
	repo        repo.Repo
	cidlocation *cidlocation.Worker

	mx      sync.Mutex
	buckets map[string]iface.Bucket
}

var _ iface.Region = (*Region)(nil)

func (r *Region) Name() string {
	return r.name
}

func (r *Region) NodeID() string {
	return r.nodeID
}

func (r *Region) ListBuckets(ctx context.Context) ([]string, error) {
	// TODO
	return nil, errors.New("TODO: Region.ListBuckets")
}

func (r *Region) CreateBucket(ctx context.Context, name iface.BucketName) error {
	// TODO
	return errors.New("TODO: Region.CreateBucket")
}

func (r *Region) GetBucket(ctx context.Context, name iface.BucketName) (iface.Bucket, error) {
	r.mx.Lock()
	defer r.mx.Unlock()

	b, ok := r.buckets[name.String()]
	if !ok {
		b = metrics.NewMeteredBucket(&Bucket{
			name:   name,
			region: r,
		})
		r.buckets[name.String()] = b
	}

	return b, nil
}

func (r *Region) DeleteBucket(ctx context.Context, name iface.BucketName) error {
	// TODO
	return errors.New("TODO: Region.DeleteBucket")
}

func (r *Region) putObject(ctx context.Context, input io.Reader) (cid.Cid, uint64, error) {
	dbp := helpers.DagBuilderParams{
		Dagserv:   r.dag,
		Maxlinks:  1024,
		RawLeaves: true,
	}

	splitter := r.splitterGen(input)
	db, err := dbp.New(splitter)
	if err != nil {
		return cid.Cid{}, 0, fmt.Errorf("failed to create dag builder: %w", err)
	}

	node, err := balanced.Layout(db)
	if err != nil {
		return cid.Cid{}, 0, fmt.Errorf("failed to layout dag: %w", err)
	}

	var size uint64
	switch n := node.(type) {
	case *dag.RawNode:
		size = uint64(len(n.RawData()))
	case *dag.ProtoNode:
		fsNode, err := ft.FSNodeFromBytes(n.Data())
		if err != nil {
			return cid.Cid{}, 0, fmt.Errorf("failed to parse fsnode: %w", err)
		}
		size = fsNode.FileSize()
	default:
		return cid.Cid{}, 0, fmt.Errorf("unknown node type: %T", n)
	}

	log.Debugf("put object %s", node.Cid())
	return node.Cid(), size, nil
}

func (r *Region) flush(ctx context.Context) error {
	if err := r.blockstore.Flush(ctx); err != nil {
		return fmt.Errorf("failed to flush blockstore: %w", err)
	}

	return nil
}
