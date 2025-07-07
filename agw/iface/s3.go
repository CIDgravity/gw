package iface

import (
	"context"
	"encoding/xml"
	"io"
	"time"
)

type Region interface {
	Name() string
	ListBuckets(ctx context.Context) ([]string, error)
	CreateBucket(ctx context.Context, name string) error
	GetBucket(ctx context.Context, name string) (Bucket, error)
	DeleteBucket(ctx context.Context, name string) error
}

type Bucket interface {
	// CRUD
	List(ctx context.Context) ([]string, error)
	Put(ctx context.Context, name string, input io.Reader) (Stat, error)
	Get(ctx context.Context, name string) (ObjectReader, error)
	Delete(ctx context.Context, name string) error
	Stat(ctx context.Context, name string) (Stat, error)

	// Multipart support
	BeginMultipartPut(ctx context.Context, name string) (string, error)
	ContinueMultipartPut(ctx context.Context, name, uploadId string, partNumber int64, input io.Reader) (Stat, error)
	CompleteMultipartPut(ctx context.Context, name, uploadId string, completion *CompleteMultipartUpload) (Stat, error)
	AbortMultipartPut(ctx context.Context, name, uploadId string) error
}

type ObjectReader interface {
	io.ReadSeeker
	io.Closer
	Stat() Stat
}

type Stat struct {
	Name      string
	ETag      string
	Size      uint64
	Timestamp time.Time
}

type CompleteMultipartUpload struct {
	XMLName xml.Name `xml:"CompleteMultipartUpload"`
	Parts   []struct {
		PartNumber int    `xml:"PartNumber"`
		ETag       string `xml:"ETag"`
	} `xml:"Part"`
}
