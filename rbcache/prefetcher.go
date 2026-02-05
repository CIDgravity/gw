// Package rbcache provides caching implementations for the retrieval path.
package rbcache

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Prefetcher implements intelligent prefetching for the cache hierarchy.
// It supports:
// - DAG-aware prefetching (prefetch linked blocks from UnixFS/DAG structures)
// - Sequential pattern detection (prefetch next items in detected sequences)
// - Priority-based scheduling
type Prefetcher struct {
	// Cache hierarchy
	l1Cache CacheInterface
	l2Cache CacheInterface

	// Fetcher for retrieving data
	fetcher DataFetcher

	// Link resolver for DAG traversal
	linkResolver LinkResolver

	// Configuration
	cfg PrefetcherConfig

	// Priority queue for prefetch jobs
	mu       sync.Mutex
	queue    *prefetchQueue
	queueCh  chan struct{} // Signal for new items
	inFlight map[string]struct{}

	// Workers
	stopCh chan struct{}
	wg     sync.WaitGroup

	// Metrics
	metrics *prefetcherMetrics
}

// CacheInterface is the minimal interface required for caches.
type CacheInterface interface {
	Has(key string) bool
	Put(key string, data []byte)
}

// DataFetcher retrieves data for a CID.
type DataFetcher interface {
	Fetch(ctx context.Context, c cid.Cid) ([]byte, error)
}

// LinkResolver extracts links from a block.
type LinkResolver interface {
	// GetLinks returns the CIDs linked from a block.
	// Returns nil if the block has no links (leaf node).
	GetLinks(ctx context.Context, c cid.Cid, data []byte) ([]cid.Cid, error)
}

// PrefetcherConfig holds configuration for the prefetcher.
type PrefetcherConfig struct {
	// NumWorkers is the number of concurrent prefetch workers
	NumWorkers int

	// MaxQueueSize limits the prefetch queue
	MaxQueueSize int

	// MaxDepth limits DAG traversal depth
	MaxDAGDepth int

	// MaxLinksPerBlock limits links followed per block
	MaxLinksPerBlock int

	// PrefetchTimeout is the timeout for each prefetch operation
	PrefetchTimeout time.Duration

	// MinPriority is the minimum priority to accept
	MinPriority float64
}

// DefaultPrefetcherConfig returns sensible defaults.
func DefaultPrefetcherConfig() PrefetcherConfig {
	return PrefetcherConfig{
		NumWorkers:       4,
		MaxQueueSize:     10000,
		MaxDAGDepth:      5,
		MaxLinksPerBlock: 100,
		PrefetchTimeout:  30 * time.Second,
		MinPriority:      0.01,
	}
}

// prefetchJob represents a single prefetch task.
type prefetchJob struct {
	cid      cid.Cid
	key      string // String key for cache
	priority float64
	reason   PrefetchReason
	depth    int // Current DAG depth (for limiting)
	index    int // Heap index
}

// PrefetchReason indicates why a prefetch was triggered.
type PrefetchReason string

const (
	ReasonDAG        PrefetchReason = "dag"        // DAG link traversal
	ReasonSequential PrefetchReason = "sequential" // Sequential pattern
	ReasonPopular    PrefetchReason = "popular"    // Popular object warmup
	ReasonManual     PrefetchReason = "manual"     // Explicitly requested
)

// prefetchQueue implements a priority queue.
type prefetchQueue []*prefetchJob

func (pq prefetchQueue) Len() int { return len(pq) }

func (pq prefetchQueue) Less(i, j int) bool {
	// Higher priority first
	return pq[i].priority > pq[j].priority
}

func (pq prefetchQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *prefetchQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*prefetchJob)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *prefetchQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

type prefetcherMetrics struct {
	jobsQueued    prometheus.Counter
	jobsCompleted prometheus.Counter
	jobsSkipped   prometheus.Counter
	jobsFailed    prometheus.Counter
	queueSize     prometheus.Gauge
	prefetchBytes prometheus.Counter
	prefetchTime  prometheus.Histogram
}

var (
	prefetcherMetricsOnce     sync.Once
	prefetcherMetricsInstance *prefetcherMetrics
)

func getPrefetcherMetrics() *prefetcherMetrics {
	prefetcherMetricsOnce.Do(func() {
		prefetcherMetricsInstance = &prefetcherMetrics{
			jobsQueued: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "fgw",
				Subsystem: "prefetcher",
				Name:      "jobs_queued_total",
				Help:      "Total prefetch jobs queued",
			}),
			jobsCompleted: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "fgw",
				Subsystem: "prefetcher",
				Name:      "jobs_completed_total",
				Help:      "Total prefetch jobs completed successfully",
			}),
			jobsSkipped: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "fgw",
				Subsystem: "prefetcher",
				Name:      "jobs_skipped_total",
				Help:      "Total prefetch jobs skipped (already cached)",
			}),
			jobsFailed: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "fgw",
				Subsystem: "prefetcher",
				Name:      "jobs_failed_total",
				Help:      "Total prefetch jobs failed",
			}),
			queueSize: promauto.NewGauge(prometheus.GaugeOpts{
				Namespace: "fgw",
				Subsystem: "prefetcher",
				Name:      "queue_size",
				Help:      "Current prefetch queue size",
			}),
			prefetchBytes: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "fgw",
				Subsystem: "prefetcher",
				Name:      "bytes_total",
				Help:      "Total bytes prefetched",
			}),
			prefetchTime: promauto.NewHistogram(prometheus.HistogramOpts{
				Namespace: "fgw",
				Subsystem: "prefetcher",
				Name:      "fetch_duration_seconds",
				Help:      "Time to fetch prefetch data",
				Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15),
			}),
		}
	})
	return prefetcherMetricsInstance
}

// NewPrefetcher creates a new prefetcher.
func NewPrefetcher(l1Cache, l2Cache CacheInterface, fetcher DataFetcher, linkResolver LinkResolver, cfg PrefetcherConfig) *Prefetcher {
	if cfg.NumWorkers <= 0 {
		cfg.NumWorkers = 4
	}
	if cfg.MaxQueueSize <= 0 {
		cfg.MaxQueueSize = 10000
	}
	if cfg.MaxDAGDepth <= 0 {
		cfg.MaxDAGDepth = 5
	}
	if cfg.MaxLinksPerBlock <= 0 {
		cfg.MaxLinksPerBlock = 100
	}
	if cfg.PrefetchTimeout <= 0 {
		cfg.PrefetchTimeout = 30 * time.Second
	}

	pq := make(prefetchQueue, 0)
	heap.Init(&pq)

	p := &Prefetcher{
		l1Cache:      l1Cache,
		l2Cache:      l2Cache,
		fetcher:      fetcher,
		linkResolver: linkResolver,
		cfg:          cfg,
		queue:        &pq,
		queueCh:      make(chan struct{}, 1),
		inFlight:     make(map[string]struct{}),
		stopCh:       make(chan struct{}),
		metrics:      getPrefetcherMetrics(),
	}

	// Start workers
	for i := 0; i < cfg.NumWorkers; i++ {
		p.wg.Add(1)
		go p.worker()
	}

	return p
}

// OnDAGAccess should be called when a DAG block is accessed.
// It schedules prefetch of linked blocks.
func (p *Prefetcher) OnDAGAccess(ctx context.Context, c cid.Cid, data []byte, depth int) {
	if p.linkResolver == nil {
		return
	}
	if depth >= p.cfg.MaxDAGDepth {
		return
	}

	links, err := p.linkResolver.GetLinks(ctx, c, data)
	if err != nil || len(links) == 0 {
		return
	}

	// Limit number of links
	if len(links) > p.cfg.MaxLinksPerBlock {
		links = links[:p.cfg.MaxLinksPerBlock]
	}

	// Schedule prefetch for each link with decreasing priority
	for i, link := range links {
		// Priority decreases with depth and link position
		priority := 1.0 / float64(depth+1) / float64(i+1)

		p.schedulePrefetch(link, priority, ReasonDAG, depth+1)
	}
}

// OnSequentialPattern should be called when a sequential access pattern is detected.
// It schedules prefetch of predicted next items.
func (p *Prefetcher) OnSequentialPattern(cids []cid.Cid) {
	for i, c := range cids {
		// Priority decreases with distance from current position
		priority := 0.8 / float64(i+1)

		p.schedulePrefetch(c, priority, ReasonSequential, 0)
	}
}

// PrefetchCID schedules a manual prefetch for a specific CID.
func (p *Prefetcher) PrefetchCID(c cid.Cid, priority float64) {
	p.schedulePrefetch(c, priority, ReasonManual, 0)
}

// schedulePrefetch adds a prefetch job to the queue.
func (p *Prefetcher) schedulePrefetch(c cid.Cid, priority float64, reason PrefetchReason, depth int) {
	if priority < p.cfg.MinPriority {
		return
	}

	key := c.String()

	p.mu.Lock()
	defer p.mu.Unlock()

	// Skip if already cached
	if p.l1Cache != nil && p.l1Cache.Has(key) {
		p.metrics.jobsSkipped.Inc()
		return
	}
	if p.l2Cache != nil && p.l2Cache.Has(key) {
		p.metrics.jobsSkipped.Inc()
		return
	}

	// Skip if already in flight
	if _, ok := p.inFlight[key]; ok {
		return
	}

	// Check queue size
	if p.queue.Len() >= p.cfg.MaxQueueSize {
		// Drop lowest priority item if new item has higher priority
		if p.queue.Len() > 0 {
			lowest := (*p.queue)[p.queue.Len()-1]
			if priority <= lowest.priority {
				return // New item has lower priority, drop it
			}
			// Remove lowest priority item
			heap.Remove(p.queue, lowest.index)
		}
	}

	job := &prefetchJob{
		cid:      c,
		key:      key,
		priority: priority,
		reason:   reason,
		depth:    depth,
	}

	heap.Push(p.queue, job)
	p.metrics.jobsQueued.Inc()
	p.metrics.queueSize.Set(float64(p.queue.Len()))

	// Signal workers
	select {
	case p.queueCh <- struct{}{}:
	default:
	}
}

// worker processes prefetch jobs.
func (p *Prefetcher) worker() {
	defer p.wg.Done()

	for {
		select {
		case <-p.stopCh:
			return
		case <-p.queueCh:
			if !p.processJobs() {
				return
			}
		}
	}
}

// processJobs processes available jobs from the queue.
// Returns false if the worker should stop.
func (p *Prefetcher) processJobs() bool {
	for {
		// Check for stop signal
		select {
		case <-p.stopCh:
			return false
		default:
		}

		job := p.dequeueJob()
		if job == nil {
			return true
		}

		p.executeJob(job)
	}
}

// dequeueJob gets the next job from the queue.
func (p *Prefetcher) dequeueJob() *prefetchJob {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.queue.Len() == 0 {
		return nil
	}

	job := heap.Pop(p.queue).(*prefetchJob)
	p.inFlight[job.key] = struct{}{}
	p.metrics.queueSize.Set(float64(p.queue.Len()))

	return job
}

// executeJob fetches data and stores in cache.
func (p *Prefetcher) executeJob(job *prefetchJob) {
	defer func() {
		p.mu.Lock()
		delete(p.inFlight, job.key)
		p.mu.Unlock()
	}()

	// Check again if already cached (might have been fetched while in queue)
	if p.l1Cache != nil && p.l1Cache.Has(job.key) {
		p.metrics.jobsSkipped.Inc()
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.cfg.PrefetchTimeout)
	defer cancel()

	start := time.Now()
	data, err := p.fetcher.Fetch(ctx, job.cid)
	elapsed := time.Since(start)

	if err != nil {
		p.metrics.jobsFailed.Inc()
		return
	}

	p.metrics.prefetchTime.Observe(elapsed.Seconds())
	p.metrics.prefetchBytes.Add(float64(len(data)))

	// Store in L1 cache
	if p.l1Cache != nil {
		p.l1Cache.Put(job.key, data)
	}

	p.metrics.jobsCompleted.Inc()

	// Continue DAG traversal if applicable
	if job.reason == ReasonDAG && job.depth < p.cfg.MaxDAGDepth {
		p.OnDAGAccess(ctx, job.cid, data, job.depth)
	}
}

// QueueLen returns the current queue length.
func (p *Prefetcher) QueueLen() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.queue.Len()
}

// InFlightCount returns the number of in-flight prefetch operations.
func (p *Prefetcher) InFlightCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.inFlight)
}

// Close stops the prefetcher and waits for workers to finish.
func (p *Prefetcher) Close() {
	close(p.stopCh)
	p.wg.Wait()
}

// Stats returns current prefetcher statistics.
type PrefetcherStats struct {
	QueueLen     int
	InFlight     int
	WorkerCount  int
	MaxQueueSize int
	MaxDAGDepth  int
}

// Stats returns current statistics.
func (p *Prefetcher) Stats() PrefetcherStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	return PrefetcherStats{
		QueueLen:     p.queue.Len(),
		InFlight:     len(p.inFlight),
		WorkerCount:  p.cfg.NumWorkers,
		MaxQueueSize: p.cfg.MaxQueueSize,
		MaxDAGDepth:  p.cfg.MaxDAGDepth,
	}
}

// NullLinkResolver is a LinkResolver that returns no links.
type NullLinkResolver struct{}

func (NullLinkResolver) GetLinks(ctx context.Context, c cid.Cid, data []byte) ([]cid.Cid, error) {
	return nil, nil
}

// SimpleLinkResolver extracts links from CBOR-encoded DAG nodes.
// This is a basic implementation - a production version would handle
// various IPLD codecs properly.
type SimpleLinkResolver struct{}

func (SimpleLinkResolver) GetLinks(ctx context.Context, c cid.Cid, data []byte) ([]cid.Cid, error) {
	// For a real implementation, this would:
	// 1. Check the CID codec to determine format
	// 2. Decode the block according to its codec
	// 3. Extract CID links from the decoded structure
	//
	// For now, return nil as this requires IPLD codec support
	return nil, nil
}
