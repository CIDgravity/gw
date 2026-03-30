package iface

import (
	"context"
	"encoding/xml"
	"io"
	"time"

	"github.com/ipfs/go-cid"
)

type Region interface {
	Name() string
	NodeID() string
	ListBuckets(ctx context.Context) ([]string, error)
	CreateBucket(ctx context.Context, name BucketName) error
	GetBucket(ctx context.Context, name BucketName) (Bucket, error)
	DeleteBucket(ctx context.Context, name BucketName) error
}

type Bucket interface {
	Name() BucketName

	// CRUD
	List(ctx context.Context, query *ListObjectsQuery) (ListObjectsResult, error)
	Put(ctx context.Context, key S3Key, input io.Reader) (Stat, error)
	Get(ctx context.Context, key S3Key) (ObjectReader, error)
	Delete(ctx context.Context, key S3Key) error
	Stat(ctx context.Context, key S3Key, full bool) (Stat, error)

	// Multipart support
	BeginMultipartPut(ctx context.Context, key S3Key) (string, error)
	ContinueMultipartPut(ctx context.Context, name S3Key, uploadId string, partNumber int64, input io.Reader) (Stat, error)
	CompleteMultipartPut(ctx context.Context, name S3Key, uploadId string, completion *CompleteMultipartUpload) (Stat, error)
	AbortMultipartPut(ctx context.Context, name S3Key, uploadId string) error
	ListParts(ctx context.Context, key S3Key, query *ListPartsQuery) (*ListPartsResult, error)
}

type BucketName string

func (b BucketName) String() string {
	return string(b)
}

type S3Key string

func (k S3Key) String() string {
	return string(k)
}

type S3Object struct {
	Bucket    BucketName `cql:"bucket"`
	Key       S3Key      `cql:"key"`
	Cid       cid.Cid    `cql:"cid"`
	Size      uint64     `cql:"size"`
	Updated   time.Time  `cql:"updated"`
	NodeID    string     `cql:"node_id"`    // Node storing this object (for scalable architecture)
	ExpiresAt *time.Time `cql:"expires_at"` // For temporary multipart parts
}

func NewS3Object(bucket BucketName, key S3Key, cid cid.Cid, size uint64, updated time.Time) S3Object {
	return S3Object{
		Bucket:  bucket,
		Key:     key,
		Cid:     cid,
		Size:    size,
		Updated: updated,
	}
}

type S3ObjectIndex interface {
	Get(ctx context.Context, bucket BucketName, key S3Key) (S3Object, error)
	Put(ctx context.Context, obj S3Object) error
	Delete(ctx context.Context, bucket BucketName, key S3Key) error
	List(ctx context.Context, bucket BucketName, prefix string, startAfter string, limit int32) (*ObjectList, error)
	ListDir(ctx context.Context, bucket BucketName, prefix, startAfter string, limit int32, delimiter string) (*ObjectList, error)
}

type ObjectList struct {
	Objects        []S3Object
	CommonPrefixes []string
	IsTruncated    bool
}

type ObjectReader interface {
	io.ReadSeeker
	io.Closer
	Stat() Stat
}

type OffloadStatus string

type Stat struct {
	Bucket        BucketName
	Key           S3Key
	ETag          string
	Size          uint64
	Timestamp     time.Time
	OffloadStatus OffloadStatus
}

type CompleteMultipartUpload struct {
	XMLName xml.Name `xml:"CompleteMultipartUpload"`
	Parts   []struct {
		PartNumber int    `xml:"PartNumber"`
		ETag       string `xml:"ETag"`
	} `xml:"Part"`
}

type ListPartsQuery struct {
	UploadID         string
	MaxParts         int32
	PartNumberMarker int
}

type ListPartsResult struct {
	Bucket               BucketName
	Key                  S3Key
	UploadID             string
	PartNumberMarker     int
	NextPartNumberMarker int
	MaxParts             int32
	IsTruncated          bool
	Parts                []PartInfo
}

type PartInfo struct {
	PartNumber   int
	ETag         string
	Size         uint64
	LastModified time.Time
}

type ListObjectsQuery struct {
	ContinuationToken string
	Delimiter         string
	MaxKeys           int32
	Prefix            string
	StartAfter        string
}

type ListObjectsResult struct {
	IsTruncated           bool
	Contents              []Stat
	CommonPrefixes        []string
	NextContinuationToken string
}
