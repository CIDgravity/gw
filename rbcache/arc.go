// Package rbcache provides caching implementations for the retrieval path.
package rbcache

import (
	"container/list"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// EvictionCallback is called when an item is evicted from the cache.
// It receives the key, value, and access statistics for the evicted item.
type EvictionCallback[K comparable, V any] func(key K, value V, stats EvictionStats)

// EvictionStats contains statistics about an evicted cache entry.
type EvictionStats struct {
	AccessCount int
	FromT1      bool // true if evicted from T1 (recent), false if from T2 (frequent)
}

// ARCCache implements the Adaptive Replacement Cache algorithm.
// ARC dynamically balances between recency and frequency, making it scan-resistant.
// It maintains four lists:
// - T1: recently accessed items (accessed once recently)
// - T2: frequently accessed items (accessed multiple times)
// - B1: ghost entries for items evicted from T1 (tracks recent history)
// - B2: ghost entries for items evicted from T2 (tracks frequency history)
//
// The adaptive parameter 'p' adjusts the target size of T1 vs T2 based on
// hit patterns in the ghost lists.
type ARCCache[K comparable, V any] struct {
	capacity int64 // Maximum total size in bytes

	// Recent items (single access)
	t1     *list.List
	t1Keys map[K]*list.Element
	t1Size int64

	// Frequent items (multiple accesses)
	t2     *list.List
	t2Keys map[K]*list.Element
	t2Size int64

	// Ghost lists (track evicted items for adaptation)
	b1     *list.List
	b1Keys map[K]*list.Element

	b2     *list.List
	b2Keys map[K]*list.Element

	// Adaptive target size for T1
	p int64

	mu sync.RWMutex

	// Size calculator for values
	sizeFunc func(V) int64

	// Optional callback for evicted items
	onEvict EvictionCallback[K, V]

	// Metrics
	metrics *arcMetrics
}

type arcEntry[K comparable, V any] struct {
	key   K
	value V
	size  int64
}

type arcGhost[K comparable] struct {
	key  K
	size int64
}

type arcMetrics struct {
	hits        prometheus.Counter
	misses      prometheus.Counter
	evictions   prometheus.Counter
	size        prometheus.Gauge
	t1Size      prometheus.Gauge
	t2Size      prometheus.Gauge
	pValue      prometheus.Gauge
	ghostHitsB1 prometheus.Counter
	ghostHitsB2 prometheus.Counter
}

func newARCMetrics(name string) *arcMetrics {
	return &arcMetrics{
		hits: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "cache",
			Name:      name + "_hits_total",
			Help:      "Total cache hits",
		}),
		misses: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "cache",
			Name:      name + "_misses_total",
			Help:      "Total cache misses",
		}),
		evictions: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "cache",
			Name:      name + "_evictions_total",
			Help:      "Total cache evictions",
		}),
		size: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "cache",
			Name:      name + "_size_bytes",
			Help:      "Current cache size in bytes",
		}),
		t1Size: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "cache",
			Name:      name + "_t1_size_bytes",
			Help:      "Size of T1 (recent) list in bytes",
		}),
		t2Size: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "cache",
			Name:      name + "_t2_size_bytes",
			Help:      "Size of T2 (frequent) list in bytes",
		}),
		pValue: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "cache",
			Name:      name + "_p_value",
			Help:      "Current adaptive parameter p",
		}),
		ghostHitsB1: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "cache",
			Name:      name + "_ghost_hits_b1_total",
			Help:      "Hits in B1 ghost list (indicates need for more recent items)",
		}),
		ghostHitsB2: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "cache",
			Name:      name + "_ghost_hits_b2_total",
			Help:      "Hits in B2 ghost list (indicates need for more frequent items)",
		}),
	}
}

// NewARCCache creates a new ARC cache with the given capacity.
// The sizeFunc calculates the size of a value in bytes.
// If sizeFunc is nil, each entry is counted as 1 byte.
func NewARCCache[K comparable, V any](capacity int64, sizeFunc func(V) int64, metricsName string) *ARCCache[K, V] {
	if sizeFunc == nil {
		sizeFunc = func(V) int64 { return 1 }
	}

	return &ARCCache[K, V]{
		capacity: capacity,
		t1:       list.New(),
		t1Keys:   make(map[K]*list.Element),
		t2:       list.New(),
		t2Keys:   make(map[K]*list.Element),
		b1:       list.New(),
		b1Keys:   make(map[K]*list.Element),
		b2:       list.New(),
		b2Keys:   make(map[K]*list.Element),
		p:        0,
		sizeFunc: sizeFunc,
		metrics:  newARCMetrics(metricsName),
	}
}

// SetEvictionCallback sets a callback function that will be called when items are evicted.
// The callback receives the key, value, and statistics about the evicted item.
// This is useful for implementing multi-tier caching (e.g., promoting to L2 SSD cache).
func (c *ARCCache[K, V]) SetEvictionCallback(callback EvictionCallback[K, V]) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onEvict = callback
}

// Get retrieves a value from the cache.
// Returns the value and true if found, zero value and false otherwise.
func (c *ARCCache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check T1
	if elem, ok := c.t1Keys[key]; ok {
		entry := elem.Value.(*arcEntry[K, V])
		// Move from T1 to T2 (now frequently accessed)
		c.t1.Remove(elem)
		delete(c.t1Keys, key)
		c.t1Size -= entry.size

		newElem := c.t2.PushFront(entry)
		c.t2Keys[key] = newElem
		c.t2Size += entry.size

		c.updateMetrics()
		c.metrics.hits.Inc()
		return entry.value, true
	}

	// Check T2
	if elem, ok := c.t2Keys[key]; ok {
		entry := elem.Value.(*arcEntry[K, V])
		// Move to front of T2 (still frequently accessed)
		c.t2.MoveToFront(elem)

		c.metrics.hits.Inc()
		return entry.value, true
	}

	c.metrics.misses.Inc()
	var zero V
	return zero, false
}

// Put adds a value to the cache.
func (c *ARCCache[K, V]) Put(key K, value V) {
	size := c.sizeFunc(value)

	c.mu.Lock()
	defer c.mu.Unlock()

	// If key exists in T1, move to T2
	if elem, ok := c.t1Keys[key]; ok {
		entry := elem.Value.(*arcEntry[K, V])
		c.t1.Remove(elem)
		delete(c.t1Keys, key)
		c.t1Size -= entry.size

		entry.value = value
		entry.size = size
		newElem := c.t2.PushFront(entry)
		c.t2Keys[key] = newElem
		c.t2Size += size

		c.updateMetrics()
		return
	}

	// If key exists in T2, update and move to front
	if elem, ok := c.t2Keys[key]; ok {
		entry := elem.Value.(*arcEntry[K, V])
		c.t2Size -= entry.size
		entry.value = value
		entry.size = size
		c.t2Size += size
		c.t2.MoveToFront(elem)

		c.updateMetrics()
		return
	}

	// Check ghost lists for adaptation
	if _, ok := c.b1Keys[key]; ok {
		// Hit in B1: increase p (favor recency)
		delta := c.adaptDelta(c.b1, c.b2)
		c.p = min(c.capacity, c.p+delta)
		c.removeFromGhost(key, c.b1, c.b1Keys)
		c.metrics.ghostHitsB1.Inc()
	} else if _, ok := c.b2Keys[key]; ok {
		// Hit in B2: decrease p (favor frequency)
		delta := c.adaptDelta(c.b2, c.b1)
		c.p = max(0, c.p-delta)
		c.removeFromGhost(key, c.b2, c.b2Keys)
		c.metrics.ghostHitsB2.Inc()
	}

	// Ensure we have space
	c.ensureSpace(size)

	// Add to T1 (new item)
	entry := &arcEntry[K, V]{key: key, value: value, size: size}
	elem := c.t1.PushFront(entry)
	c.t1Keys[key] = elem
	c.t1Size += size

	c.updateMetrics()
}

// Has checks if a key exists in the cache (without updating access patterns).
func (c *ARCCache[K, V]) Has(key K) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, inT1 := c.t1Keys[key]
	_, inT2 := c.t2Keys[key]
	return inT1 || inT2
}

// Delete removes a key from the cache.
func (c *ARCCache[K, V]) Delete(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.t1Keys[key]; ok {
		entry := elem.Value.(*arcEntry[K, V])
		c.t1.Remove(elem)
		delete(c.t1Keys, key)
		c.t1Size -= entry.size
		c.updateMetrics()
		return
	}

	if elem, ok := c.t2Keys[key]; ok {
		entry := elem.Value.(*arcEntry[K, V])
		c.t2.Remove(elem)
		delete(c.t2Keys, key)
		c.t2Size -= entry.size
		c.updateMetrics()
	}
}

// Size returns the current size of the cache in bytes.
func (c *ARCCache[K, V]) Size() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.t1Size + c.t2Size
}

// Len returns the number of items in the cache.
func (c *ARCCache[K, V]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.t1Keys) + len(c.t2Keys)
}

// Clear removes all items from the cache.
func (c *ARCCache[K, V]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.t1.Init()
	c.t1Keys = make(map[K]*list.Element)
	c.t1Size = 0

	c.t2.Init()
	c.t2Keys = make(map[K]*list.Element)
	c.t2Size = 0

	c.b1.Init()
	c.b1Keys = make(map[K]*list.Element)

	c.b2.Init()
	c.b2Keys = make(map[K]*list.Element)

	c.p = 0

	c.updateMetrics()
}

// adaptDelta calculates the adaptation amount.
func (c *ARCCache[K, V]) adaptDelta(primary, other *list.List) int64 {
	if primary.Len() == 0 {
		return 1
	}
	// Adaptation is proportional to the ratio of list sizes
	ratio := float64(other.Len()) / float64(primary.Len())
	delta := int64(max(1, ratio))
	return delta
}

// ensureSpace makes room for a new item of the given size.
func (c *ARCCache[K, V]) ensureSpace(needed int64) {
	for c.t1Size+c.t2Size+needed > c.capacity {
		if c.t1Size > 0 && (c.t1Size > c.p || (c.t2Size == 0)) {
			// Evict from T1
			c.evictFromT1()
		} else if c.t2Size > 0 {
			// Evict from T2
			c.evictFromT2()
		} else {
			// Nothing to evict
			break
		}
		c.metrics.evictions.Inc()
	}

	// Also limit ghost list sizes
	c.trimGhostLists()
}

// evictFromT1 removes the least recently used item from T1.
func (c *ARCCache[K, V]) evictFromT1() {
	elem := c.t1.Back()
	if elem == nil {
		return
	}

	entry := elem.Value.(*arcEntry[K, V])

	// Call eviction callback if set
	if c.onEvict != nil {
		c.onEvict(entry.key, entry.value, EvictionStats{
			AccessCount: 1, // Items in T1 have been accessed once
			FromT1:      true,
		})
	}

	c.t1.Remove(elem)
	delete(c.t1Keys, entry.key)
	c.t1Size -= entry.size

	// Add to B1 (ghost list for T1)
	ghost := &arcGhost[K]{key: entry.key, size: entry.size}
	ghostElem := c.b1.PushFront(ghost)
	c.b1Keys[entry.key] = ghostElem
}

// evictFromT2 removes the least recently used item from T2.
func (c *ARCCache[K, V]) evictFromT2() {
	elem := c.t2.Back()
	if elem == nil {
		return
	}

	entry := elem.Value.(*arcEntry[K, V])

	// Call eviction callback if set
	if c.onEvict != nil {
		c.onEvict(entry.key, entry.value, EvictionStats{
			AccessCount: 2, // Items in T2 have been accessed at least twice
			FromT1:      false,
		})
	}

	c.t2.Remove(elem)
	delete(c.t2Keys, entry.key)
	c.t2Size -= entry.size

	// Add to B2 (ghost list for T2)
	ghost := &arcGhost[K]{key: entry.key, size: entry.size}
	ghostElem := c.b2.PushFront(ghost)
	c.b2Keys[entry.key] = ghostElem
}

// removeFromGhost removes a key from a ghost list.
func (c *ARCCache[K, V]) removeFromGhost(key K, l *list.List, keys map[K]*list.Element) {
	if elem, ok := keys[key]; ok {
		l.Remove(elem)
		delete(keys, key)
	}
}

// trimGhostLists ensures ghost lists don't grow unbounded.
func (c *ARCCache[K, V]) trimGhostLists() {
	// Keep ghost list size bounded to capacity / averageEntrySize
	maxGhostEntries := int(c.capacity / 1024) // Rough estimate
	if maxGhostEntries < 100 {
		maxGhostEntries = 100
	}

	for c.b1.Len() > maxGhostEntries {
		elem := c.b1.Back()
		if elem == nil {
			break
		}
		ghost := elem.Value.(*arcGhost[K])
		c.b1.Remove(elem)
		delete(c.b1Keys, ghost.key)
	}

	for c.b2.Len() > maxGhostEntries {
		elem := c.b2.Back()
		if elem == nil {
			break
		}
		ghost := elem.Value.(*arcGhost[K])
		c.b2.Remove(elem)
		delete(c.b2Keys, ghost.key)
	}
}

// updateMetrics updates the Prometheus metrics.
func (c *ARCCache[K, V]) updateMetrics() {
	c.metrics.size.Set(float64(c.t1Size + c.t2Size))
	c.metrics.t1Size.Set(float64(c.t1Size))
	c.metrics.t2Size.Set(float64(c.t2Size))
	c.metrics.pValue.Set(float64(c.p))
}

// Stats returns cache statistics.
type CacheStats struct {
	Size     int64
	Capacity int64
	T1Size   int64
	T2Size   int64
	B1Len    int
	B2Len    int
	P        int64
	Items    int
}

// Stats returns current cache statistics.
func (c *ARCCache[K, V]) Stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return CacheStats{
		Size:     c.t1Size + c.t2Size,
		Capacity: c.capacity,
		T1Size:   c.t1Size,
		T2Size:   c.t2Size,
		B1Len:    c.b1.Len(),
		B2Len:    c.b2.Len(),
		P:        c.p,
		Items:    len(c.t1Keys) + len(c.t2Keys),
	}
}
