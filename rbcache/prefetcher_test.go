package rbcache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// mockCache implements CacheInterface for testing.
type mockCache struct {
	mu     sync.RWMutex
	data   map[string][]byte
	puts   int64
	checks int64
}

func newMockCache() *mockCache {
	return &mockCache{
		data: make(map[string][]byte),
	}
}

func (c *mockCache) Has(key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	atomic.AddInt64(&c.checks, 1)
	_, ok := c.data[key]
	return ok
}

func (c *mockCache) Put(key string, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	atomic.AddInt64(&c.puts, 1)
	c.data[key] = data
}

func (c *mockCache) PutCount() int64 {
	return atomic.LoadInt64(&c.puts)
}

func (c *mockCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.data)
}

// mockFetcher implements DataFetcher for testing.
type mockFetcher struct {
	mu       sync.Mutex
	data     map[string][]byte
	fetches  int64
	delay    time.Duration
	failNext bool
}

func newMockFetcher() *mockFetcher {
	return &mockFetcher{
		data: make(map[string][]byte),
	}
}

func (f *mockFetcher) SetData(c cid.Cid, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[c.String()] = data
}

func (f *mockFetcher) Fetch(ctx context.Context, c cid.Cid) ([]byte, error) {
	f.mu.Lock()
	shouldFail := f.failNext
	f.failNext = false
	delay := f.delay
	f.mu.Unlock()

	atomic.AddInt64(&f.fetches, 1)

	if delay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}

	if shouldFail {
		return nil, context.DeadlineExceeded
	}

	f.mu.Lock()
	data, ok := f.data[c.String()]
	f.mu.Unlock()

	if !ok {
		return []byte("default data"), nil
	}
	return data, nil
}

func (f *mockFetcher) FetchCount() int64 {
	return atomic.LoadInt64(&f.fetches)
}

// mockLinkResolver implements LinkResolver for testing.
type mockLinkResolver struct {
	mu    sync.RWMutex
	links map[string][]cid.Cid
}

func newMockLinkResolver() *mockLinkResolver {
	return &mockLinkResolver{
		links: make(map[string][]cid.Cid),
	}
}

func (r *mockLinkResolver) SetLinks(c cid.Cid, links []cid.Cid) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.links[c.String()] = links
}

func (r *mockLinkResolver) GetLinks(ctx context.Context, c cid.Cid, data []byte) ([]cid.Cid, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.links[c.String()], nil
}

// Helper to create test CIDs
func testCID(s string) cid.Cid {
	hash, _ := mh.Sum([]byte(s), mh.SHA2_256, -1)
	return cid.NewCidV1(cid.Raw, hash)
}

func TestPrefetcher_BasicPrefetch(t *testing.T) {
	cache := newMockCache()
	fetcher := newMockFetcher()

	cfg := DefaultPrefetcherConfig()
	cfg.NumWorkers = 2

	prefetcher := NewPrefetcher(cache, nil, fetcher, NullLinkResolver{}, cfg)
	defer prefetcher.Close()

	// Schedule a prefetch
	c := testCID("test")
	prefetcher.PrefetchCID(c, 1.0)

	// Wait for processing
	time.Sleep(100 * time.Millisecond)

	// Should have fetched and cached
	if fetcher.FetchCount() < 1 {
		t.Error("Expected fetch to be called")
	}
	if cache.PutCount() < 1 {
		t.Error("Expected data to be cached")
	}
	if !cache.Has(c.String()) {
		t.Error("Expected CID to be in cache")
	}
}

func TestPrefetcher_SkipsAlreadyCached(t *testing.T) {
	cache := newMockCache()
	fetcher := newMockFetcher()

	cfg := DefaultPrefetcherConfig()
	prefetcher := NewPrefetcher(cache, nil, fetcher, NullLinkResolver{}, cfg)
	defer prefetcher.Close()

	// Pre-populate cache
	c := testCID("already-cached")
	cache.Put(c.String(), []byte("existing data"))

	// Try to prefetch
	prefetcher.PrefetchCID(c, 1.0)

	// Wait
	time.Sleep(100 * time.Millisecond)

	// Should not have fetched
	if fetcher.FetchCount() > 0 {
		t.Errorf("Expected no fetch, got %d", fetcher.FetchCount())
	}
}

func TestPrefetcher_PriorityOrdering(t *testing.T) {
	cache := newMockCache()
	fetcher := newMockFetcher()
	fetcher.delay = 50 * time.Millisecond // Slow down fetches

	cfg := DefaultPrefetcherConfig()
	cfg.NumWorkers = 1 // Single worker to observe ordering

	prefetcher := NewPrefetcher(cache, nil, fetcher, NullLinkResolver{}, cfg)
	defer prefetcher.Close()

	// Add items with different priorities
	low := testCID("low-priority")
	high := testCID("high-priority")

	prefetcher.PrefetchCID(low, 0.1)
	prefetcher.PrefetchCID(high, 1.0)

	// Wait for first fetch
	time.Sleep(75 * time.Millisecond)

	// High priority should be fetched first
	if !cache.Has(high.String()) {
		t.Error("High priority item should be fetched first")
	}
}

func TestPrefetcher_DAGTraversal(t *testing.T) {
	cache := newMockCache()
	fetcher := newMockFetcher()
	resolver := newMockLinkResolver()

	cfg := DefaultPrefetcherConfig()
	cfg.MaxDAGDepth = 3

	prefetcher := NewPrefetcher(cache, nil, fetcher, resolver, cfg)
	defer prefetcher.Close()

	// Set up a simple DAG: root -> child1, child2
	root := testCID("root")
	child1 := testCID("child1")
	child2 := testCID("child2")

	resolver.SetLinks(root, []cid.Cid{child1, child2})
	fetcher.SetData(root, []byte("root data"))

	// Trigger DAG access
	prefetcher.OnDAGAccess(context.Background(), root, []byte("root data"), 0)

	// Wait for processing
	time.Sleep(200 * time.Millisecond)

	// Children should be prefetched
	if !cache.Has(child1.String()) {
		t.Error("Expected child1 to be prefetched")
	}
	if !cache.Has(child2.String()) {
		t.Error("Expected child2 to be prefetched")
	}
}

func TestPrefetcher_MaxDepthLimit(t *testing.T) {
	cache := newMockCache()
	fetcher := newMockFetcher()
	resolver := newMockLinkResolver()

	cfg := DefaultPrefetcherConfig()
	cfg.MaxDAGDepth = 1

	prefetcher := NewPrefetcher(cache, nil, fetcher, resolver, cfg)
	defer prefetcher.Close()

	root := testCID("root")
	child := testCID("child")
	grandchild := testCID("grandchild")

	resolver.SetLinks(root, []cid.Cid{child})
	resolver.SetLinks(child, []cid.Cid{grandchild})

	// Trigger at depth 0
	prefetcher.OnDAGAccess(context.Background(), root, []byte("root"), 0)

	time.Sleep(200 * time.Millisecond)

	// Child should be prefetched (depth 0 -> 1)
	if !cache.Has(child.String()) {
		t.Error("Expected child to be prefetched")
	}

	// Grandchild should NOT be prefetched (would be depth 2)
	if cache.Has(grandchild.String()) {
		t.Error("Expected grandchild to NOT be prefetched (max depth)")
	}
}

func TestPrefetcher_SequentialPattern(t *testing.T) {
	cache := newMockCache()
	fetcher := newMockFetcher()

	cfg := DefaultPrefetcherConfig()
	prefetcher := NewPrefetcher(cache, nil, fetcher, NullLinkResolver{}, cfg)
	defer prefetcher.Close()

	// Simulate sequential pattern detection
	cids := []cid.Cid{
		testCID("seq1"),
		testCID("seq2"),
		testCID("seq3"),
	}

	prefetcher.OnSequentialPattern(cids)

	time.Sleep(200 * time.Millisecond)

	// All should be prefetched
	for _, c := range cids {
		if !cache.Has(c.String()) {
			t.Errorf("Expected %s to be prefetched", c.String())
		}
	}
}

func TestPrefetcher_QueueSizeLimit(t *testing.T) {
	cache := newMockCache()
	fetcher := newMockFetcher()
	fetcher.delay = 1 * time.Second // Very slow

	cfg := DefaultPrefetcherConfig()
	cfg.MaxQueueSize = 10
	cfg.NumWorkers = 1

	prefetcher := NewPrefetcher(cache, nil, fetcher, NullLinkResolver{}, cfg)
	defer prefetcher.Close()

	// Queue more items than max
	for i := 0; i < 20; i++ {
		c := testCID(string(rune('a' + i)))
		prefetcher.PrefetchCID(c, float64(i)/20.0) // Increasing priority
	}

	// Queue should be limited
	if prefetcher.QueueLen() > cfg.MaxQueueSize+1 { // +1 for in-flight
		t.Errorf("Queue size %d exceeds max %d", prefetcher.QueueLen(), cfg.MaxQueueSize)
	}
}

func TestPrefetcher_MinPriorityFilter(t *testing.T) {
	cache := newMockCache()
	fetcher := newMockFetcher()

	cfg := DefaultPrefetcherConfig()
	cfg.MinPriority = 0.5

	prefetcher := NewPrefetcher(cache, nil, fetcher, NullLinkResolver{}, cfg)
	defer prefetcher.Close()

	lowPriority := testCID("low")
	highPriority := testCID("high")

	prefetcher.PrefetchCID(lowPriority, 0.1)  // Below min
	prefetcher.PrefetchCID(highPriority, 1.0) // Above min

	time.Sleep(100 * time.Millisecond)

	if cache.Has(lowPriority.String()) {
		t.Error("Low priority item should be filtered out")
	}
	if !cache.Has(highPriority.String()) {
		t.Error("High priority item should be prefetched")
	}
}

func TestPrefetcher_ConcurrentAccess(t *testing.T) {
	cache := newMockCache()
	fetcher := newMockFetcher()

	cfg := DefaultPrefetcherConfig()
	cfg.NumWorkers = 4

	prefetcher := NewPrefetcher(cache, nil, fetcher, NullLinkResolver{}, cfg)
	defer prefetcher.Close()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c := testCID(string(rune(id)))
			prefetcher.PrefetchCID(c, float64(id)/100.0)
		}(i)
	}

	wg.Wait()
	time.Sleep(500 * time.Millisecond)

	// Should have processed many items without crashing
	if cache.Len() < 10 {
		t.Errorf("Expected more items to be cached, got %d", cache.Len())
	}
}

func TestPrefetcher_Stats(t *testing.T) {
	cache := newMockCache()
	fetcher := newMockFetcher()
	fetcher.delay = 100 * time.Millisecond

	cfg := DefaultPrefetcherConfig()
	cfg.NumWorkers = 2
	cfg.MaxQueueSize = 100
	cfg.MaxDAGDepth = 5

	prefetcher := NewPrefetcher(cache, nil, fetcher, NullLinkResolver{}, cfg)
	defer prefetcher.Close()

	// Queue some items
	for i := 0; i < 10; i++ {
		c := testCID(string(rune('a' + i)))
		prefetcher.PrefetchCID(c, 0.5)
	}

	stats := prefetcher.Stats()
	if stats.WorkerCount != 2 {
		t.Errorf("WorkerCount = %d, want 2", stats.WorkerCount)
	}
	if stats.MaxQueueSize != 100 {
		t.Errorf("MaxQueueSize = %d, want 100", stats.MaxQueueSize)
	}
	if stats.MaxDAGDepth != 5 {
		t.Errorf("MaxDAGDepth = %d, want 5", stats.MaxDAGDepth)
	}
}

func TestPrefetcher_Close(t *testing.T) {
	cache := newMockCache()
	fetcher := newMockFetcher()
	fetcher.delay = 1 * time.Second

	cfg := DefaultPrefetcherConfig()
	prefetcher := NewPrefetcher(cache, nil, fetcher, NullLinkResolver{}, cfg)

	// Queue some work
	for i := 0; i < 10; i++ {
		c := testCID(string(rune('a' + i)))
		prefetcher.PrefetchCID(c, 0.5)
	}

	// Close should complete without hanging
	done := make(chan struct{})
	go func() {
		prefetcher.Close()
		close(done)
	}()

	select {
	case <-done:
		// Good
	case <-time.After(2 * time.Second):
		t.Fatal("Close timed out")
	}
}

func BenchmarkPrefetcher_Schedule(b *testing.B) {
	cache := newMockCache()
	fetcher := newMockFetcher()

	cfg := DefaultPrefetcherConfig()
	cfg.MaxQueueSize = 100000

	prefetcher := NewPrefetcher(cache, nil, fetcher, NullLinkResolver{}, cfg)
	defer prefetcher.Close()

	cids := make([]cid.Cid, 1000)
	for i := range cids {
		cids[i] = testCID(string(rune(i)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := cids[i%len(cids)]
		prefetcher.PrefetchCID(c, 0.5)
	}
}
