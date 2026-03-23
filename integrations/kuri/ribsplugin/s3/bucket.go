package s3

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	dag "github.com/ipfs/boxo/ipld/merkledag"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	uio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/ipfs/boxo/ipld/unixfs/pb"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	mh "github.com/multiformats/go-multihash"
)

var v1CidPrefix = cid.Prefix{
	Codec:    cid.DagProtobuf,
	MhLength: -1,
	MhType:   mh.SHA2_256,
	Version:  1,
}

type Bucket struct {
	name   iface.BucketName
	region *Region
}

var _ iface.Bucket = (*Bucket)(nil)

func (b *Bucket) Name() iface.BucketName {
	return b.name
}

func (b *Bucket) List(ctx context.Context, query *iface.ListObjectsQuery) (iface.ListObjectsResult, error) {
	startAfter := query.StartAfter

	if query.ContinuationToken != "" {
		decodedToken, err := base64.StdEncoding.DecodeString(query.ContinuationToken)
		if err != nil {
			return iface.ListObjectsResult{}, fmt.Errorf("failed to decode continuation token: %w", err)
		}
		continuationToken := string(decodedToken)

		if continuationToken > startAfter {
			startAfter = continuationToken
		}
	}

	var result *iface.ObjectList
	var err error

	if query.Delimiter == "" {
		result, err = b.region.index.List(ctx, b.name, query.Prefix, startAfter, query.MaxKeys)
	} else {
		result, err = b.region.index.ListDir(ctx, b.name, query.Prefix, startAfter, query.MaxKeys, query.Delimiter)
	}

	if err != nil {
		return iface.ListObjectsResult{}, fmt.Errorf("failed to list objects: %w", err)
	}

	objs := make([]iface.Stat, len(result.Objects))
	for i, obj := range result.Objects {
		objs[i] = iface.Stat{
			Bucket:    b.name,
			Key:       obj.Key,
			ETag:      obj.Cid.String(),
			Size:      obj.Size,
			Timestamp: obj.Updated,
		}
	}

	response := iface.ListObjectsResult{
		IsTruncated:    result.IsTruncated,
		Contents:       objs,
		CommonPrefixes: result.CommonPrefixes,
	}

	if result.IsTruncated {
		var nextCt string
		if len(result.Objects) > 0 {
			nextCt = result.Objects[len(result.Objects)-1].Key.String()
		}

		if len(result.CommonPrefixes) > 0 {
			lastPrefix := result.CommonPrefixes[len(result.CommonPrefixes)-1]
			if lastPrefix > nextCt {
				nextCt = lastPrefix
			}
		}
		response.NextContinuationToken = base64.StdEncoding.EncodeToString([]byte(nextCt))
	}

	return response, nil
}

func (b *Bucket) Put(ctx context.Context, key iface.S3Key, input io.Reader) (iface.Stat, error) {
	c, size, err := b.region.putObject(ctx, input)
	if err != nil {
		return iface.Stat{}, fmt.Errorf("failed to put object %s/%s: %w", b.name, key, err)
	}

	if err = b.region.flush(ctx); err != nil {
		return iface.Stat{}, fmt.Errorf("failed to flush region for %s/%s: %w", b.name, key, err)
	}

	obj := iface.NewS3Object(b.name, key, c, size, time.Now())
	obj.NodeID = b.region.nodeID // Set the node ID for scalable routing
	err = b.region.index.Put(ctx, obj)
	if err != nil {
		return iface.Stat{}, fmt.Errorf("failed to update index for %s/%s: %w", b.name, key, err)
	}

	log.Debugf("put %s/%s -> %s", b.name, key, c)
	b.region.cidlocation.Schedule(c)
	return iface.Stat{
		Bucket:    b.name,
		Key:       key,
		ETag:      c.String(),
		Size:      obj.Size,
		Timestamp: obj.Updated,
	}, nil
}

func (b *Bucket) Get(ctx context.Context, key iface.S3Key) (iface.ObjectReader, error) {
	obj, err := b.region.index.Get(ctx, b.name, key)
	if errors.Is(err, iface.ErrNotFound) {
		return nil, err
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get object from index: %w", err)
	}

	node, err := b.region.dag.Get(ctx, obj.Cid)
	if err != nil {
		return nil, fmt.Errorf("failed to get node for cid %s and key %s/%s: %w", obj.Cid, b.name, key, err)
	}

	dagr, err := uio.NewDagReader(ctx, node, b.region.dag)
	if err != nil {
		return nil, fmt.Errorf("failed to get dag reader for cid %s and key %s/%s: %w", obj.Cid, b.name, key, err)
	}

	log.Debugf("get %s -> %s", key, obj.Cid)
	return &Reader{
		DagReader: dagr,
		stat: iface.Stat{
			Bucket:    b.name,
			Key:       key,
			Size:      obj.Size,
			ETag:      obj.Cid.String(),
			Timestamp: obj.Updated,
		},
	}, nil
}

func (b *Bucket) Delete(ctx context.Context, key iface.S3Key) error {
	if err := b.region.index.Delete(ctx, b.name, key); err != nil {
		return fmt.Errorf("error deleting object %s/%s from index: %w", b.name, key, err)
	}

	// TODO we should also delete the data, but ok for now
	log.Debugf("delete %s", key)
	return nil
}

func (b *Bucket) Stat(ctx context.Context, key iface.S3Key, full bool) (iface.Stat, error) {
	obj, err := b.region.index.Get(ctx, b.name, key)
	if errors.Is(err, iface.ErrNotFound) {
		return iface.Stat{}, err
	}

	var status iface.OffloadStatus
	if full {
		status, err = b.region.cidlocation.GetOffloadStatus(ctx, obj.Cid)
		if err != nil {
			return iface.Stat{}, fmt.Errorf("failed to get offload status for %s/%s: %w", b.name, key, err)
		}
	}

	return iface.Stat{
		Bucket:        b.name,
		Key:           key,
		Size:          obj.Size,
		ETag:          obj.Cid.String(),
		Timestamp:     obj.Updated,
		OffloadStatus: status,
	}, nil
}

func (b *Bucket) BeginMultipartPut(ctx context.Context, key iface.S3Key) (string, error) {
	// TODO should we track those for gc purposes? see related notes below
	uploadId := uuid.New().String()

	log.Debugf("begin multipart upload %s/%s -> %s", b.name, key, uploadId)
	return uploadId, nil
}

func (b *Bucket) ContinueMultipartPut(ctx context.Context, key iface.S3Key, uploadId string, partNumber int64, input io.Reader) (iface.Stat, error) {
	partKey := fmt.Sprintf("%s/%s:%s:%d", b.name, key, uploadId, partNumber)
	c, size, err := b.region.putObject(ctx, input)
	if err != nil {
		return iface.Stat{}, fmt.Errorf("failed to put object %s: %w", partKey, err)
	}

	if err = b.region.flush(ctx); err != nil {
		return iface.Stat{}, fmt.Errorf("failed to flush region for part %s: %w", partKey, err)
	}

	// Store part with expiration for GC cleanup
	partObj := iface.NewS3Object(b.name, iface.S3Key(partKey), c, size, time.Now())
	partObj.NodeID = b.region.nodeID
	expiresAt := time.Now().Add(24 * time.Hour) // Parts expire after 24 hours
	partObj.ExpiresAt = &expiresAt

	if err = b.region.index.Put(ctx, partObj); err != nil {
		return iface.Stat{}, fmt.Errorf("failed to update index for part %s: %w", partKey, err)
	}

	log.Debugf("continue multipart upload %s -> %s %d -> %s", partKey, uploadId, partNumber, c.String())
	return iface.Stat{
		Bucket:    b.name,
		Key:       key,
		ETag:      c.String(),
		Size:      size,
		Timestamp: time.Now(),
	}, nil
}

func (b *Bucket) CompleteMultipartPut(ctx context.Context, key iface.S3Key, uploadId string, completion *iface.CompleteMultipartUpload) (iface.Stat, error) {
	links := make([]*format.Link, 0)
	totalSize := uint64(0)
	pbfile := new(pb.Data)
	typ := pb.Data_File
	pbfile.Type = &typ

	for _, part := range completion.Parts {
		c, err := cid.Parse(part.ETag)
		if err != nil {
			return iface.Stat{}, fmt.Errorf("failed to parse etag %s: %w", part.ETag, err)
		}
		node, err := b.region.dag.Get(ctx, c)
		if err != nil {
			return iface.Stat{}, fmt.Errorf("failed to get node for cid %s: %w", c, err)
		}

		var blockSize uint64
		switch n := node.(type) {
		case *dag.RawNode:
			blockSize = uint64(len(n.RawData()))
		case *dag.ProtoNode:
			fsNode, err := ft.FSNodeFromBytes(n.Data())
			if err != nil {
				return iface.Stat{}, fmt.Errorf("failed to parse fsnode: %w", err)
			}
			blockSize = fsNode.FileSize()
		default:
			return iface.Stat{}, fmt.Errorf("unknown node type: %T", n)
		}

		totalSize += blockSize
		pbfile.Blocksizes = append(pbfile.Blocksizes, blockSize)
		link, err := format.MakeLink(node)
		if err != nil {
			return iface.Stat{}, fmt.Errorf("failed to create link: %w", err)
		}
		link.Name = fmt.Sprintf("part-%05d", part.PartNumber)
		links = append(links, link)
	}

	pbfile.Filesize = proto.Uint64(totalSize)
	data, err := proto.Marshal(pbfile)
	if err != nil {
		return iface.Stat{}, err
	}
	finalNode := dag.NodeWithData(data)
	if err := finalNode.SetCidBuilder(v1CidPrefix); err != nil {
		return iface.Stat{}, err
	}
	if err := finalNode.SetLinks(links); err != nil {
		return iface.Stat{}, fmt.Errorf("failed to set links: %w", err)
	}
	if err := b.region.dag.Add(ctx, finalNode); err != nil {
		return iface.Stat{}, fmt.Errorf("failed to add node: %w", err)
	}

	if err = b.region.flush(ctx); err != nil {
		return iface.Stat{}, fmt.Errorf("failed to flush region for %s/%s: %w", b.name, key, err)
	}

	obj := iface.NewS3Object(b.name, key, finalNode.Cid(), totalSize, time.Now())
	obj.NodeID = b.region.nodeID // Set the node ID for scalable routing
	if err := b.region.index.Put(ctx, obj); err != nil {
		return iface.Stat{}, fmt.Errorf("failed to update index: %w", err)
	}

	// Clean up part entries from S3Objects table
	for _, part := range completion.Parts {
		partKey := iface.S3Key(fmt.Sprintf("%s/%s:%s:%d", b.name, key, uploadId, part.PartNumber))
		if err := b.region.index.Delete(ctx, b.name, partKey); err != nil {
			log.Warnf("failed to clean up multipart part %s: %s", partKey, err)
		}
	}

	log.Debugf("complete multipart upload %s/%s -> %s -> %s", b.name, key, uploadId, finalNode.Cid().String())
	b.region.cidlocation.Schedule(obj.Cid)
	return iface.Stat{
		Bucket:    b.name,
		Key:       key,
		ETag:      obj.Cid.String(),
		Size:      obj.Size,
		Timestamp: obj.Updated,
	}, nil
}

func (b *Bucket) AbortMultipartPut(ctx context.Context, key iface.S3Key, uploadId string) error {
	// TODO related to the note in ContinueMultipartPut
	//      we should keep track of those orphas and gc; ok for mvp though.
	return nil
}

type Reader struct {
	uio.DagReader
	stat iface.Stat
}

var _ iface.ObjectReader = (*Reader)(nil)

func (r *Reader) Stat() iface.Stat {
	return r.stat

}
