package metrics

import (
	"context"
	"io"

	fgw_iface "github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	baseOpCounters = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "fgw",
		Subsystem: "s3",
		Name:      "bucket_operations_total",
	}, []string{"operation", "bucket"})
	baseErrcounters = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "fgw",
		Subsystem: "s3",
		Name:      "bucket_errors_total",
	}, []string{"operation", "bucket"})
)

type MeteredBucket struct {
	opCounters  *prometheus.CounterVec
	errCounters *prometheus.CounterVec
	bucket      fgw_iface.Bucket
}

var _ fgw_iface.Bucket = (*MeteredBucket)(nil)

func NewMeteredBucket(bucket fgw_iface.Bucket) fgw_iface.Bucket {
	return &MeteredBucket{
		opCounters:  baseOpCounters.MustCurryWith(prometheus.Labels{"bucket": bucket.Name().String()}),
		errCounters: baseErrcounters.MustCurryWith(prometheus.Labels{"bucket": bucket.Name().String()}),
		bucket:      bucket,
	}
}

func (mb *MeteredBucket) Name() fgw_iface.BucketName {
	return mb.bucket.Name()
}

func (mb *MeteredBucket) List(ctx context.Context, query *fgw_iface.ListObjectsQuery) (fgw_iface.ListObjectsResult, error) {
	mb.opCounters.WithLabelValues("list").Inc()
	result, err := mb.bucket.List(ctx, query)
	mb.checkError(err, "list")
	return result, err
}

func (mb *MeteredBucket) Put(ctx context.Context, key fgw_iface.S3Key, input io.Reader) (fgw_iface.Stat, error) {
	mb.opCounters.WithLabelValues("put").Inc()
	result, err := mb.bucket.Put(ctx, key, input)
	mb.checkError(err, "put")
	return result, err
}

func (mb *MeteredBucket) Get(ctx context.Context, key fgw_iface.S3Key) (fgw_iface.ObjectReader, error) {
	mb.opCounters.WithLabelValues("get").Inc()
	result, err := mb.bucket.Get(ctx, key)
	mb.checkError(err, "get")
	return result, err
}

func (mb *MeteredBucket) Delete(ctx context.Context, key fgw_iface.S3Key) error {
	mb.opCounters.WithLabelValues("delete").Inc()
	err := mb.bucket.Delete(ctx, key)
	mb.checkError(err, "delete")
	return err
}

func (mb *MeteredBucket) Stat(ctx context.Context, key fgw_iface.S3Key, full bool) (fgw_iface.Stat, error) {
	mb.opCounters.WithLabelValues("stat").Inc()
	result, err := mb.bucket.Stat(ctx, key, full)
	mb.checkError(err, "stat")
	return result, err
}

func (mb *MeteredBucket) BeginMultipartPut(ctx context.Context, key fgw_iface.S3Key) (string, error) {
	mb.opCounters.WithLabelValues("begin_multipart_put").Inc()
	result, err := mb.bucket.BeginMultipartPut(ctx, key)
	mb.checkError(err, "begin_multipart_put")
	return result, err
}

func (mb *MeteredBucket) ContinueMultipartPut(ctx context.Context, key fgw_iface.S3Key, uploadId string, partNumber int64, input io.Reader) (fgw_iface.Stat, error) {
	mb.opCounters.WithLabelValues("continue_multipart_put").Inc()
	result, err := mb.bucket.ContinueMultipartPut(ctx, key, uploadId, partNumber, input)
	mb.checkError(err, "continue_multipart_put")
	return result, err
}

func (mb *MeteredBucket) CompleteMultipartPut(ctx context.Context, key fgw_iface.S3Key, uploadId string, completion *fgw_iface.CompleteMultipartUpload) (fgw_iface.Stat, error) {
	mb.opCounters.WithLabelValues("complete_multipart_put").Inc()
	result, err := mb.bucket.CompleteMultipartPut(ctx, key, uploadId, completion)
	mb.checkError(err, "complete_multipart_put")
	return result, err
}

func (mb *MeteredBucket) AbortMultipartPut(ctx context.Context, key fgw_iface.S3Key, uploadId string) error {
	mb.opCounters.WithLabelValues("abort_multipart_put").Inc()
	err := mb.bucket.AbortMultipartPut(ctx, key, uploadId)
	mb.checkError(err, "abort_multipart_put")
	return err
}

func (mb *MeteredBucket) ListParts(ctx context.Context, key fgw_iface.S3Key, query *fgw_iface.ListPartsQuery) (*fgw_iface.ListPartsResult, error) {
	mb.opCounters.WithLabelValues("list_parts").Inc()
	result, err := mb.bucket.ListParts(ctx, key, query)
	mb.checkError(err, "list_parts")
	return result, err
}

func (mb *MeteredBucket) checkError(e error, operation string) {
	if e != nil {
		mb.errCounters.WithLabelValues(operation).Inc()
	}
}
