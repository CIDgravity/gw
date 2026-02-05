package rbstor

import (
	"context"
	"sync"
	"time"

	"github.com/gocql/gocql"
	mh "github.com/multiformats/go-multihash"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// RefCounter tracks reference counts for multihashes.
// When an S3 object is created, all its blocks get their ref count incremented.
// When deleted, the ref counts are decremented.
// Blocks with ref_count = 0 are candidates for garbage collection.
type RefCounter struct {
	session *gocql.Session

	// Batch updates for efficiency
	mu       sync.Mutex
	incBatch []refOp
	decBatch []refOp

	// Background flush
	flushInterval time.Duration
	stopCh        chan struct{}
	wg            sync.WaitGroup

	// Metrics
	metrics *refCountMetrics
}

type refOp struct {
	multihash mh.Multihash
	groupID   int64
}

type refCountMetrics struct {
	increments   prometheus.Counter
	decrements   prometheus.Counter
	flushes      prometheus.Counter
	flushLatency prometheus.Histogram
	batchSize    prometheus.Histogram
	errors       prometheus.Counter
}

var (
	refCountMetricsOnce     sync.Once
	refCountMetricsInstance *refCountMetrics
)

func getRefCountMetrics() *refCountMetrics {
	refCountMetricsOnce.Do(func() {
		refCountMetricsInstance = &refCountMetrics{
			increments: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "fgw",
				Subsystem: "refcount",
				Name:      "increments_total",
				Help:      "Total reference count increments",
			}),
			decrements: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "fgw",
				Subsystem: "refcount",
				Name:      "decrements_total",
				Help:      "Total reference count decrements",
			}),
			flushes: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "fgw",
				Subsystem: "refcount",
				Name:      "flushes_total",
				Help:      "Total batch flushes",
			}),
			flushLatency: promauto.NewHistogram(prometheus.HistogramOpts{
				Namespace: "fgw",
				Subsystem: "refcount",
				Name:      "flush_latency_seconds",
				Help:      "Latency of batch flushes",
				Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15),
			}),
			batchSize: promauto.NewHistogram(prometheus.HistogramOpts{
				Namespace: "fgw",
				Subsystem: "refcount",
				Name:      "batch_size",
				Help:      "Size of flush batches",
				Buckets:   prometheus.ExponentialBuckets(1, 2, 15),
			}),
			errors: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "fgw",
				Subsystem: "refcount",
				Name:      "errors_total",
				Help:      "Total errors during ref count operations",
			}),
		}
	})
	return refCountMetricsInstance
}

// RefCounterConfig holds configuration for the reference counter.
type RefCounterConfig struct {
	FlushInterval time.Duration
	MaxBatchSize  int
}

// DefaultRefCounterConfig returns sensible defaults.
func DefaultRefCounterConfig() RefCounterConfig {
	return RefCounterConfig{
		FlushInterval: 5 * time.Second,
		MaxBatchSize:  1000,
	}
}

// NewRefCounter creates a new reference counter.
func NewRefCounter(session *gocql.Session, cfg RefCounterConfig) *RefCounter {
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 5 * time.Second
	}

	rc := &RefCounter{
		session:       session,
		flushInterval: cfg.FlushInterval,
		stopCh:        make(chan struct{}),
		metrics:       getRefCountMetrics(),
	}

	// Start background flusher
	rc.wg.Add(1)
	go rc.flushLoop()

	return rc
}

// IncrementRef increments the reference count for a multihash.
// This is called when an S3 object referencing the block is created.
func (rc *RefCounter) IncrementRef(mhash mh.Multihash, groupID int64) {
	rc.mu.Lock()
	rc.incBatch = append(rc.incBatch, refOp{multihash: mhash, groupID: groupID})
	rc.mu.Unlock()
	rc.metrics.increments.Inc()
}

// DecrementRef decrements the reference count for a multihash.
// This is called when an S3 object referencing the block is deleted.
func (rc *RefCounter) DecrementRef(mhash mh.Multihash, groupID int64) {
	rc.mu.Lock()
	rc.decBatch = append(rc.decBatch, refOp{multihash: mhash, groupID: groupID})
	rc.mu.Unlock()
	rc.metrics.decrements.Inc()
}

// IncrementRefs increments reference counts for multiple multihashes.
func (rc *RefCounter) IncrementRefs(hashes []mh.Multihash, groupID int64) {
	rc.mu.Lock()
	for _, h := range hashes {
		rc.incBatch = append(rc.incBatch, refOp{multihash: h, groupID: groupID})
	}
	rc.mu.Unlock()
	rc.metrics.increments.Add(float64(len(hashes)))
}

// DecrementRefs decrements reference counts for multiple multihashes.
func (rc *RefCounter) DecrementRefs(hashes []mh.Multihash, groupID int64) {
	rc.mu.Lock()
	for _, h := range hashes {
		rc.decBatch = append(rc.decBatch, refOp{multihash: h, groupID: groupID})
	}
	rc.mu.Unlock()
	rc.metrics.decrements.Add(float64(len(hashes)))
}

// flushLoop periodically flushes batched operations.
func (rc *RefCounter) flushLoop() {
	defer rc.wg.Done()

	ticker := time.NewTicker(rc.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rc.stopCh:
			// Final flush
			rc.flush()
			return
		case <-ticker.C:
			rc.flush()
		}
	}
}

// flush processes batched operations.
func (rc *RefCounter) flush() {
	rc.mu.Lock()
	incs := rc.incBatch
	decs := rc.decBatch
	rc.incBatch = nil
	rc.decBatch = nil
	rc.mu.Unlock()

	if len(incs) == 0 && len(decs) == 0 {
		return
	}

	start := time.Now()
	rc.metrics.batchSize.Observe(float64(len(incs) + len(decs)))

	// Process increments
	for _, op := range incs {
		if err := rc.doIncrement(op.multihash); err != nil {
			log.Warnw("failed to increment ref count", "error", err)
			rc.metrics.errors.Inc()
		}
	}

	// Process decrements
	for _, op := range decs {
		if err := rc.doDecrement(op.multihash, op.groupID); err != nil {
			log.Warnw("failed to decrement ref count", "error", err)
			rc.metrics.errors.Inc()
		}
	}

	rc.metrics.flushes.Inc()
	rc.metrics.flushLatency.Observe(time.Since(start).Seconds())
}

// doIncrement performs a single increment operation.
func (rc *RefCounter) doIncrement(mhash mh.Multihash) error {
	// Use CQL counter increment
	return rc.session.Query(
		"UPDATE MultihashRefCount SET RefCount = RefCount + 1 WHERE Multihash = ?",
		[]byte(mhash),
	).Exec()
}

// doDecrement performs a single decrement operation.
// If ref count drops to 0, the block is added to the GC queue.
func (rc *RefCounter) doDecrement(mhash mh.Multihash, groupID int64) error {
	// Use CQL counter decrement
	if err := rc.session.Query(
		"UPDATE MultihashRefCount SET RefCount = RefCount - 1 WHERE Multihash = ?",
		[]byte(mhash),
	).Exec(); err != nil {
		return err
	}

	// Note: With Cassandra counters, we can't atomically check if count reached 0
	// The GC process will scan for zero-count entries periodically
	// This is acceptable for passive GC where we just don't extend claims

	return nil
}

// GetRefCount returns the current reference count for a multihash.
func (rc *RefCounter) GetRefCount(ctx context.Context, mhash mh.Multihash) (int64, error) {
	var count int64
	err := rc.session.Query(
		"SELECT RefCount FROM MultihashRefCount WHERE Multihash = ?",
		[]byte(mhash),
	).WithContext(ctx).Scan(&count)

	if err == gocql.ErrNotFound {
		return 0, nil
	}
	return count, err
}

// Close stops the reference counter and flushes pending operations.
func (rc *RefCounter) Close() error {
	close(rc.stopCh)
	rc.wg.Wait()
	return nil
}

// Stats returns current reference counter statistics.
type RefCountStats struct {
	PendingIncrements int
	PendingDecrements int
}

// Stats returns current statistics.
func (rc *RefCounter) Stats() RefCountStats {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return RefCountStats{
		PendingIncrements: len(rc.incBatch),
		PendingDecrements: len(rc.decBatch),
	}
}
