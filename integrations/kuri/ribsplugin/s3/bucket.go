package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/filecoin-project/go-hamt-ipld"
	dag "github.com/ipfs/boxo/ipld/merkledag"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	uio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/ipfs/boxo/ipld/unixfs/pb"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	mh "github.com/multiformats/go-multihash"

	agw_iface "github.com/aurorainfra/gw/agw/iface"
)

var v1CidPrefix = cid.Prefix{
	Codec:    cid.DagProtobuf,
	MhLength: -1,
	MhType:   mh.SHA2_256,
	Version:  1,
}

type Bucket struct {
	name   string
	region *Region
}

func (b *Bucket) objectKey(name string) string {
	return fmt.Sprintf("/%s/%s", b.name, name)
}

func (b *Bucket) List(ctx context.Context) ([]string, error) {
	// TODO this is just not possible to implement with current indexing!
	return nil, errors.New("TODO: Bucket.List")
}

func (b *Bucket) Put(ctx context.Context, name string, input io.Reader) (agw_iface.Stat, error) {
	key := b.objectKey(name)
	c, err := b.region.putObject(ctx, input)
	if err != nil {
		return agw_iface.Stat{}, fmt.Errorf("failed to put object %s: %w", key, err)
	}

	err = b.region.index.Set(ctx, key, c)
	if err != nil {
		return agw_iface.Stat{}, fmt.Errorf("failed to update hamt for %s: %w", key, err)
	}

	err = b.region.flush(ctx)
	if err != nil {
		return agw_iface.Stat{}, fmt.Errorf("failed to flush hamt for %s: %w", key, err)
	}

	log.Debugf("put %s -> %s", key, c.String())
	return agw_iface.Stat{
		Name: name,
		ETag: c.String(),
		// TODO Size, real Timestamp
		Timestamp: time.Now(),
	}, nil
}

func (b *Bucket) Get(ctx context.Context, name string) (agw_iface.ObjectReader, error) {
	key := b.objectKey(name)

	objCid, err := b.region.index.Get(ctx, key)
	if errors.Is(err, hamt.ErrNotFound) {
		return nil, agw_iface.ErrNotFound
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get object from hamt: %w", err)
	}

	node, err := b.region.dag.Get(ctx, objCid)
	if err != nil {
		return nil, fmt.Errorf("failed to get node for cid %s and key %s: %w", objCid, key, err)
	}

	dagr, err := uio.NewDagReader(ctx, node, b.region.dag)
	if err != nil {
		return nil, fmt.Errorf("failed to get dag reader for cid %s and key %s: %w", objCid, key, err)
	}

	log.Debugf("get %s -> %s", key, objCid.String())
	return &Reader{
		DagReader: dagr,
		stat: agw_iface.Stat{
			Name: name,
			ETag: objCid.String(),
			// TODO Size, real Timestamp
			Timestamp: time.Now(),
		},
	}, nil
}

func (b *Bucket) Delete(ctx context.Context, name string) error {
	key := b.objectKey(name)
	if err := b.region.index.Delete(ctx, key); err != nil {
		return fmt.Errorf("error deleting object %s from index: %w", key, err)
	}

	if err := b.region.flushIndex(ctx); err != nil {
		return fmt.Errorf("error flushing index: %w", err)
	}

	// TODO we should also delete the data, but ok for now
	log.Debugf("delete %s", key)
	return nil
}

func (b *Bucket) Stat(ctx context.Context, name string) (agw_iface.Stat, error) {
	// TODO
	return agw_iface.Stat{}, errors.New("TODO: Bucket.Stat")
}

func (b *Bucket) BeginMultipartPut(ctx context.Context, name string) (string, error) {
	// TODO should we track those for gc purposes? see related notes below
	uploadId := uuid.New().String()

	log.Debugf("begin multipart upload %s -> %s", b.objectKey(name), uploadId)
	return uploadId, nil
}

func (b *Bucket) ContinueMultipartPut(ctx context.Context, name, uploadId string, partNumber int64, input io.Reader) (agw_iface.Stat, error) {
	// TODO should we track orphan uploads and gc the parts?
	//      this needs robust logic, but it's ok for mvp.
	key := fmt.Sprintf("%s:%s:%d", b.objectKey(name), uploadId, partNumber)
	c, err := b.region.putObject(ctx, input)
	if err != nil {
		return agw_iface.Stat{}, fmt.Errorf("failed to put object %s: %w", key, err)
	}

	log.Debugf("continue multipart upload %s -> %s %d -> %s", key, uploadId, partNumber, c.String())
	return agw_iface.Stat{
		Name: key,
		ETag: c.String(),
		// TODO Size, real Timestamp
		Timestamp: time.Now(),
	}, nil
}

func (b *Bucket) CompleteMultipartPut(ctx context.Context, name, uploadId string, completion *agw_iface.CompleteMultipartUpload) (agw_iface.Stat, error) {
	key := b.objectKey(name)
	links := make([]*format.Link, 0)
	totalSize := uint64(0)
	pbfile := new(pb.Data)
	typ := pb.Data_File
	pbfile.Type = &typ

	for _, part := range completion.Parts {
		c, err := cid.Parse(part.ETag)
		if err != nil {
			return agw_iface.Stat{}, fmt.Errorf("failed to parse etag %s: %w", part.ETag, err)
		}
		node, err := b.region.dag.Get(ctx, c)
		if err != nil {
			return agw_iface.Stat{}, fmt.Errorf("failed to get node for cid %s: %w", c, err)
		}

		var blockSize uint64
		switch n := node.(type) {
		case *dag.RawNode:
			blockSize = uint64(len(n.RawData()))
		case *dag.ProtoNode:
			fsNode, err := ft.FSNodeFromBytes(n.Data())
			if err != nil {
				return agw_iface.Stat{}, fmt.Errorf("failed to parse fsnode: %w", err)
			}
			blockSize = fsNode.FileSize()
		default:
			return agw_iface.Stat{}, fmt.Errorf("unknown node type: %T", n)
		}

		totalSize += blockSize
		pbfile.Blocksizes = append(pbfile.Blocksizes, blockSize)
		link, err := format.MakeLink(node)
		if err != nil {
			return agw_iface.Stat{}, fmt.Errorf("failed to create link: %w", err)
		}
		link.Name = fmt.Sprintf("part-%d", part.PartNumber)
		links = append(links, link)
	}

	pbfile.Filesize = proto.Uint64(totalSize)
	data, err := proto.Marshal(pbfile)
	if err != nil {
		return agw_iface.Stat{}, err
	}
	finalNode := dag.NodeWithData(data)
	if err := finalNode.SetCidBuilder(v1CidPrefix); err != nil {
		return agw_iface.Stat{}, err
	}
	if err := finalNode.SetLinks(links); err != nil {
		return agw_iface.Stat{}, fmt.Errorf("failed to set links: %w", err)
	}
	if err := b.region.dag.Add(ctx, finalNode); err != nil {
		return agw_iface.Stat{}, fmt.Errorf("failed to add node: %w", err)
	}
	if err := b.region.index.Set(ctx, key, finalNode.Cid()); err != nil {
		return agw_iface.Stat{}, fmt.Errorf("failed to update hamt: %w", err)
	}
	if err := b.region.flush(ctx); err != nil {
		return agw_iface.Stat{}, fmt.Errorf("failed to flush hamt: %w", err)
	}

	log.Debugf("complete multipart upload %s -> %s -> %s", key, uploadId, finalNode.Cid().String())
	return agw_iface.Stat{
		Name: name,
		ETag: finalNode.Cid().String(),
		Size: totalSize,
	}, nil
}

func (b *Bucket) AbortMultipartPut(ctx context.Context, name, uploadId string) error {
	// TODO related to the note in ContinueMultipartPut
	//      we should keep track of those orphas and gc; ok for mvp though.
	return nil
}

type Reader struct {
	uio.DagReader
	stat agw_iface.Stat
}

var _ agw_iface.ObjectReader = (*Reader)(nil)

func (r *Reader) Stat() agw_iface.Stat {
	return r.stat

}
