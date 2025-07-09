package metrics

import (
	"context"
	"io"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	agw_iface "github.com/aurorainfra/gw/agw/iface"
)

var (
	baseOpCounters = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "agw",
		Subsystem: "s3",
		Name:      "bucket_operations_total",
	}, []string{"operation", "bucket"})
	baseErrcounters = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "agw",
		Subsystem: "s3",
		Name:      "bucket_errors_total",
	}, []string{"operation", "bucket"})
)

type MeteredBucket struct {
	opCounters  *prometheus.CounterVec
	errCounters *prometheus.CounterVec
	bucket      agw_iface.Bucket
}

var _ agw_iface.Bucket = (*MeteredBucket)(nil)

func NewMeteredBucket(bucket agw_iface.Bucket) agw_iface.Bucket {
	return &MeteredBucket{
		opCounters:  baseOpCounters.MustCurryWith(prometheus.Labels{"bucket": bucket.Name()}),
		errCounters: baseErrcounters.MustCurryWith(prometheus.Labels{"bucket": bucket.Name()}),
		bucket:      bucket,
	}
}

func (mb *MeteredBucket) Name() string {
	return mb.bucket.Name()
}

func (mb *MeteredBucket) List(ctx context.Context) ([]string, error) {
	mb.opCounters.WithLabelValues("list").Inc()
	result, err := mb.bucket.List(ctx)
	mb.checkError(err, "list")
	return result, nil
}

func (mb *MeteredBucket) Put(ctx context.Context, name string, input io.Reader) (agw_iface.Stat, error) {
	mb.opCounters.WithLabelValues("put").Inc()
	result, err := mb.bucket.Put(ctx, name, input)
	mb.checkError(err, "put")
	return result, nil
}

func (mb *MeteredBucket) Get(ctx context.Context, name string) (agw_iface.ObjectReader, error) {
	mb.opCounters.WithLabelValues("get").Inc()
	result, err := mb.bucket.Get(ctx, name)
	mb.checkError(err, "get")
	return result, nil
}

func (mb *MeteredBucket) Delete(ctx context.Context, name string) error {
	mb.opCounters.WithLabelValues("delete").Inc()
	err := mb.bucket.Delete(ctx, name)
	mb.checkError(err, "delete")
	return err
}

func (mb *MeteredBucket) Stat(ctx context.Context, name string) (agw_iface.Stat, error) {
	mb.opCounters.WithLabelValues("stat").Inc()
	result, err := mb.bucket.Stat(ctx, name)
	mb.checkError(err, "stat")
	return result, nil
}

func (mb *MeteredBucket) BeginMultipartPut(ctx context.Context, name string) (string, error) {
	mb.opCounters.WithLabelValues("begin_multipart_put").Inc()
	result, err := mb.bucket.BeginMultipartPut(ctx, name)
	mb.checkError(err, "begin_multipart_put")
	return result, nil
}

func (mb *MeteredBucket) ContinueMultipartPut(ctx context.Context, name, uploadId string, partNumber int64, input io.Reader) (agw_iface.Stat, error) {
	mb.opCounters.WithLabelValues("continue_multipart_put").Inc()
	result, err := mb.bucket.ContinueMultipartPut(ctx, name, uploadId, partNumber, input)
	mb.checkError(err, "continue_multipart_put")
	return result, nil
}

func (mb *MeteredBucket) CompleteMultipartPut(ctx context.Context, name, uploadId string, completion *agw_iface.CompleteMultipartUpload) (agw_iface.Stat, error) {
	mb.opCounters.WithLabelValues("complete_multipart_put").Inc()
	result, err := mb.bucket.CompleteMultipartPut(ctx, name, uploadId, completion)
	mb.checkError(err, "complete_multipart_put")
	return result, nil
}

func (mb *MeteredBucket) AbortMultipartPut(ctx context.Context, name, uploadId string) error {
	mb.opCounters.WithLabelValues("abort_multipart_put").Inc()
	err := mb.bucket.AbortMultipartPut(ctx, name, uploadId)
	mb.checkError(err, "abort_multipart_put")
	return err
}

func (mb *MeteredBucket) checkError(e error, operation string) {
	if e != nil {
		mb.errCounters.WithLabelValues(operation).Inc()
	}
}
