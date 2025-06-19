package s3

import (
	"errors"
	"fmt"
	"github.com/filecoin-project/go-hamt-ipld"
	"github.com/golang/protobuf/proto"
	dag "github.com/ipfs/boxo/ipld/merkledag"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	"github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	uio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/ipfs/boxo/ipld/unixfs/pb"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	format "github.com/ipfs/go-ipld-format"
	mh "github.com/multiformats/go-multihash"
	"io"
)

var objectNotFoundErr = errors.New("object not found")

var v1CidPrefix = cid.Prefix{
	Codec:    cid.DagProtobuf,
	MhLength: -1,
	MhType:   mh.SHA2_256,
	Version:  1,
}

func (ctx *Context) getObject(key string) (uio.DagReader, error) {
	objCid, err := ctx.index.Get(ctx.lctx, key)
	if errors.Is(err, hamt.ErrNotFound) {
		return nil, objectNotFoundErr
	} else if err != nil {
		return nil, fmt.Errorf("failed to get object from hamt: %w", err)
	}

	node, err := ctx.dag.Get(ctx.lctx, objCid)
	if err != nil {
		return nil, fmt.Errorf("failed to get node for cid %s and key %s: %w", objCid, key, err)
	}

	return uio.NewDagReader(ctx.lctx, node, ctx.dag)
}

func (ctx *Context) putObject(key string, r io.Reader) error {
	c, err := ctx.createFileNode(r)
	if err != nil {
		return fmt.Errorf("failed to create file node: %w", err)
	}

	err = ctx.index.Set(ctx.lctx, key, c)
	if err != nil {
		return fmt.Errorf("failed to update hamt: %w", err)
	}
	return ctx.flushHamt()
}

func (ctx *Context) putPart(r io.Reader) (string, error) {
	c, err := ctx.createFileNode(r)
	if err != nil {
		return "", fmt.Errorf("failed to create file node: %w", err)
	}
	return c.String(), err
}

func (ctx *Context) completeMultipartPut(key string, req CompleteMultipartUpload) (string, error) {
	links := make([]*format.Link, 0)
	totalSize := uint64(0)
	pbfile := new(pb.Data)
	typ := pb.Data_File
	pbfile.Type = &typ

	for _, part := range req.Parts {
		c, err := cid.Parse(part.ETag)
		if err != nil {
			return "", fmt.Errorf("failed to parse etag %s: %w", part.ETag, err)
		}
		node, err := ctx.dag.Get(ctx.lctx, c)
		if err != nil {
			return "", fmt.Errorf("failed to get node for cid %s: %w", c, err)
		}

		var blockSize uint64
		switch n := node.(type) {
		case *dag.RawNode:
			blockSize = uint64(len(n.RawData()))
		case *dag.ProtoNode:
			fsNode, err := ft.FSNodeFromBytes(n.Data())
			if err != nil {
				return "", fmt.Errorf("failed to parse fsnode: %w", err)
			}
			blockSize = fsNode.FileSize()
		default:
			return "", fmt.Errorf("unknown node type: %T", n)
		}

		totalSize += blockSize
		pbfile.Blocksizes = append(pbfile.Blocksizes, blockSize)
		link, err := format.MakeLink(node)
		if err != nil {
			return "", fmt.Errorf("failed to create link: %w", err)
		}
		link.Name = ""
		links = append(links, link)
	}

	pbfile.Filesize = proto.Uint64(totalSize)
	data, err := proto.Marshal(pbfile)
	if err != nil {
		return "", err
	}
	finalNode := dag.NodeWithData(data)
	if err := finalNode.SetCidBuilder(v1CidPrefix); err != nil {
		return "", err
	}
	if err := finalNode.SetLinks(links); err != nil {
		return "", fmt.Errorf("failed to set links: %w", err)
	}
	if err := ctx.dag.Add(ctx.lctx, finalNode); err != nil {
		return "", fmt.Errorf("failed to add node: %w", err)
	}
	if err := ctx.index.Set(ctx.lctx, key, finalNode.Cid()); err != nil {
		return "", fmt.Errorf("failed to update hamt: %w", err)
	}
	return finalNode.Cid().String(), ctx.flushHamt()
}

func (ctx *Context) deleteObject(key string) error {
	_, err := ctx.index.Get(ctx.lctx, key)
	if errors.Is(err, hamt.ErrNotFound) {
		log.Warnw("Received delete request for non-existent object", "key", key)
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to get object from hamt: %w", err)
	}

	err = ctx.index.Delete(ctx.lctx, key)
	if err != nil {
		return fmt.Errorf("failed to delete object from hamt: %w", err)
	}

	return ctx.flushHamt()
}

func (ctx *Context) createFileNode(r io.Reader) (cid.Cid, error) {
	dbp := helpers.DagBuilderParams{
		Dagserv:   ctx.dag,
		Maxlinks:  1024,
		RawLeaves: true,
	}

	splitter := ctx.splitterGen(r)

	db, err := dbp.New(splitter)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to create dag builder: %w", err)
	}
	node, err := balanced.Layout(db)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to layout dag: %w", err)
	}

	return node.Cid(), nil
}

func (ctx *Context) flushHamt() error {
	//todo updating hamt state is a multi step process, probably would benefit from some sort of transaction mechanism

	hamtCid, err := ctx.index.Flush(ctx.lctx)
	if err != nil {
		return fmt.Errorf("failed to flush hamt: %w", err)
	}

	if err := ctx.blockstore.Flush(ctx.lctx); err != nil {
		return fmt.Errorf("failed to flush blockstore: %w", err)
	}

	err = ctx.repo.Datastore().Put(ctx.lctx, datastore.NewKey("/local/s3/index"), hamtCid.Bytes())
	if err != nil {
		return fmt.Errorf("failed to save new hamt cid: %w", err)
	}
	return nil
}
