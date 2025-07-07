package s3

import (
	"context"
	"errors"
	"fmt"
	"io"

	chunk "github.com/ipfs/boxo/chunker"
	"github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	"github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/kubo/repo"

	agw_iface "github.com/aurorainfra/gw/agw/iface"
	ribsbstore "github.com/aurorainfra/gw/integrations/blockstore"
)

type Region struct {
	name        string
	index       *Index
	blockstore  *ribsbstore.Blockstore
	dag         format.DAGService
	splitterGen chunk.SplitterGen
	repo        repo.Repo
}

var _ agw_iface.Region = (*Region)(nil)

func (r *Region) Name() string {
	return r.name
}

func (r *Region) ListBuckets(ctx context.Context) ([]string, error) {
	// TODO
	return nil, errors.New("TODO: Region.ListBuckets")
}

func (r *Region) CreateBucket(ctx context.Context, name string) error {
	// TODO
	return errors.New("TODO: Region.CreateBucket")
}

func (r *Region) GetBucket(ctx context.Context, name string) (agw_iface.Bucket, error) {
	return &Bucket{
		name:   name,
		region: r,
	}, nil
}

func (r *Region) DeleteBucket(ctx context.Context, name string) error {
	// TODO
	return errors.New("TODO: Region.DeleteBucket")
}

func (r *Region) putObject(ctx context.Context, input io.Reader) (cid.Cid, error) {
	dbp := helpers.DagBuilderParams{
		Dagserv:   r.dag,
		Maxlinks:  1024,
		RawLeaves: true,
	}

	splitter := r.splitterGen(input)
	db, err := dbp.New(splitter)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to create dag builder: %w", err)
	}

	node, err := balanced.Layout(db)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to layout dag: %w", err)
	}

	log.Debugf("put object %s", node.Cid())
	return node.Cid(), nil
}

func (r *Region) flush(ctx context.Context) error {
	return r._flush(ctx, true)
}

func (r *Region) flushIndex(ctx context.Context) error {
	return r._flush(ctx, false)
}

func (r *Region) _flush(ctx context.Context, flushbs bool) error {
	// TODO make this transactional if possible

	c, err := r.index.Flush(ctx)
	if err != nil {
		return fmt.Errorf("failed to flush hamt: %w", err)
	}

	if flushbs {
		if err := r.blockstore.Flush(ctx); err != nil {
			return fmt.Errorf("failed to flush blockstore: %w", err)
		}
	}

	err = r.repo.Datastore().Put(ctx, datastore.NewKey("/local/s3/index"), c.Bytes())
	if err != nil {
		return fmt.Errorf("failed to save new hamt cid: %w", err)
	}

	log.Debugf("flush %s", c.String())
	return nil
}
