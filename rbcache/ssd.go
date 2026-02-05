// Package rbcache provides caching implementations for the retrieval path.
package rbcache

import (
	"container/list"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// SSDCache implements a persistent SSD-based cache using SLRU eviction policy.
// It provides a second-level cache that complements the in-memory ARC cache.
//
// Key features:
// - SLRU (Segmented LRU) eviction with probationary and protected segments
// - Admission policy: only items evicted from L1 with 2+ accesses are admitted
// - Write buffering for sequential SSD writes
// - In-memory index with on-disk data storage
// - CRC32 checksums for data integrity
type SSDCache struct {
	path     string
	maxSize  int64
	dataFile *os.File

	// In-memory index
	mu    sync.RWMutex
	index map[string]*ssdEntry

	// SLRU segments
	probation       *list.List // 80% of space - new items
	protected       *list.List // 20% of space - items with multiple accesses
	probationSize   int64
	protectedSize   int64
	probationTarget int64 // 80% of maxSize
	protectedTarget int64 // 20% of maxSize

	// Write buffer for batching writes
	writeBuf   []writeOp
	writeMu    sync.Mutex
	writeCond  *sync.Cond
	flushSize  int // Flush when buffer reaches this size
	flushTimer *time.Timer
	stopCh     chan struct{}
	wg         sync.WaitGroup

	// Free space management (simple append-only with periodic compaction)
	writeOffset int64
	freeSpace   int64

	// Metrics
	metrics *ssdMetrics

	// State
	closed bool
}

type ssdEntry struct {
	key       string
	offset    int64 // Offset in data file
	size      int32 // Size of data
	checksum  uint32
	accesses  uint8 // Access counter for promotion
	segment   string
	listElem  *list.Element
	protected bool // true if in protected segment
}

type writeOp struct {
	key      string
	data     []byte
	size     int32
	checksum uint32
}

// SSDCacheConfig holds configuration for the SSD cache.
type SSDCacheConfig struct {
	Path           string        // Directory for cache files
	MaxSizeBytes   int64         // Maximum cache size in bytes
	FlushSize      int           // Number of writes before flush (default: 100)
	FlushInterval  time.Duration // Max time between flushes (default: 1s)
	MetricsName    string        // Prometheus metrics prefix
	ProbationRatio float64       // Ratio of space for probation segment (default: 0.8)
}

type ssdMetrics struct {
	hits          prometheus.Counter
	misses        prometheus.Counter
	admissions    prometheus.Counter
	rejections    prometheus.Counter
	evictions     prometheus.Counter
	promotions    prometheus.Counter
	size          prometheus.Gauge
	probationSize prometheus.Gauge
	protectedSize prometheus.Gauge
	writeLatency  prometheus.Histogram
	readLatency   prometheus.Histogram
	flushes       prometheus.Counter
	corruptReads  prometheus.Counter
}

func newSSDMetrics(name string) *ssdMetrics {
	return &ssdMetrics{
		hits: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "cache",
			Name:      name + "_hits_total",
			Help:      "Total SSD cache hits",
		}),
		misses: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "cache",
			Name:      name + "_misses_total",
			Help:      "Total SSD cache misses",
		}),
		admissions: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "cache",
			Name:      name + "_admissions_total",
			Help:      "Total items admitted to SSD cache",
		}),
		rejections: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "cache",
			Name:      name + "_rejections_total",
			Help:      "Total items rejected from SSD cache (admission policy)",
		}),
		evictions: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "cache",
			Name:      name + "_evictions_total",
			Help:      "Total SSD cache evictions",
		}),
		promotions: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "cache",
			Name:      name + "_promotions_total",
			Help:      "Total promotions from probation to protected",
		}),
		size: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "cache",
			Name:      name + "_size_bytes",
			Help:      "Current SSD cache size in bytes",
		}),
		probationSize: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "cache",
			Name:      name + "_probation_size_bytes",
			Help:      "Size of probation segment in bytes",
		}),
		protectedSize: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "cache",
			Name:      name + "_protected_size_bytes",
			Help:      "Size of protected segment in bytes",
		}),
		writeLatency: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: "fgw",
			Subsystem: "cache",
			Name:      name + "_write_latency_seconds",
			Help:      "Latency of SSD cache writes",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 15),
		}),
		readLatency: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: "fgw",
			Subsystem: "cache",
			Name:      name + "_read_latency_seconds",
			Help:      "Latency of SSD cache reads",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 15),
		}),
		flushes: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "cache",
			Name:      name + "_flushes_total",
			Help:      "Total write buffer flushes",
		}),
		corruptReads: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "cache",
			Name:      name + "_corrupt_reads_total",
			Help:      "Total reads with checksum mismatch",
		}),
	}
}

// L1EvictStats contains statistics about an item evicted from L1.
// Used by the admission policy to decide whether to admit to L2.
type L1EvictStats struct {
	AccessCount int // Number of times the item was accessed in L1
	WriteCount  int // Number of times the item was written in L1
	ReadCount   int // Number of times the item was read in L1
}

// NewSSDCache creates a new SSD-based cache.
func NewSSDCache(cfg SSDCacheConfig) (*SSDCache, error) {
	if cfg.Path == "" {
		return nil, errors.New("cache path is required")
	}
	if cfg.MaxSizeBytes <= 0 {
		return nil, errors.New("max size must be positive")
	}
	if cfg.FlushSize <= 0 {
		cfg.FlushSize = 100
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = time.Second
	}
	if cfg.MetricsName == "" {
		cfg.MetricsName = "l2_ssd"
	}
	if cfg.ProbationRatio <= 0 || cfg.ProbationRatio >= 1 {
		cfg.ProbationRatio = 0.8
	}

	// Create cache directory
	if err := os.MkdirAll(cfg.Path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}

	// Open or create data file
	dataPath := filepath.Join(cfg.Path, "cache.data")
	dataFile, err := os.OpenFile(dataPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}

	// Get current file size for write offset
	stat, err := dataFile.Stat()
	if err != nil {
		dataFile.Close()
		return nil, fmt.Errorf("failed to stat data file: %w", err)
	}

	c := &SSDCache{
		path:            cfg.Path,
		maxSize:         cfg.MaxSizeBytes,
		dataFile:        dataFile,
		index:           make(map[string]*ssdEntry),
		probation:       list.New(),
		protected:       list.New(),
		probationTarget: int64(float64(cfg.MaxSizeBytes) * cfg.ProbationRatio),
		protectedTarget: int64(float64(cfg.MaxSizeBytes) * (1 - cfg.ProbationRatio)),
		writeBuf:        make([]writeOp, 0, cfg.FlushSize),
		flushSize:       cfg.FlushSize,
		stopCh:          make(chan struct{}),
		writeOffset:     stat.Size(),
		freeSpace:       cfg.MaxSizeBytes - stat.Size(),
		metrics:         newSSDMetrics(cfg.MetricsName),
	}
	c.writeCond = sync.NewCond(&c.writeMu)

	// Load existing index if available
	if err := c.loadIndex(); err != nil {
		// Index load failure is not fatal - we start with empty cache
		// The data file might have orphaned data that will be reclaimed on compaction
	}

	// Start flush timer
	c.flushTimer = time.AfterFunc(cfg.FlushInterval, c.timerFlush)

	// Start background writer
	c.wg.Add(1)
	go c.backgroundWriter()

	return c, nil
}

// ShouldAdmit determines if an item evicted from L1 should be admitted to L2.
// Admission policy: only admit items with 2+ accesses and more reads than writes.
func (c *SSDCache) ShouldAdmit(key string, l1Stats *L1EvictStats) bool {
	if l1Stats == nil {
		return false
	}
	// Require at least 2 accesses
	if l1Stats.AccessCount < 2 {
		return false
	}
	// Prefer read-heavy items (writes indicate temporary data)
	if l1Stats.WriteCount > l1Stats.ReadCount*2 {
		return false
	}
	return true
}

// Get retrieves a value from the SSD cache.
func (c *SSDCache) Get(key string) ([]byte, bool) {
	start := time.Now()

	c.mu.Lock()
	entry, ok := c.index[key]
	if !ok {
		c.mu.Unlock()
		c.metrics.misses.Inc()
		return nil, false
	}

	// Update access count and potentially promote
	entry.accesses++
	if !entry.protected && entry.accesses >= 2 {
		c.promote(entry)
	} else if entry.protected {
		// Move to front of protected list
		c.protected.MoveToFront(entry.listElem)
	} else {
		// Move to front of probation list
		c.probation.MoveToFront(entry.listElem)
	}

	offset := entry.offset
	size := entry.size
	checksum := entry.checksum
	c.mu.Unlock()

	// Read data from file
	data := make([]byte, size)
	n, err := c.dataFile.ReadAt(data, offset)
	if err != nil || n != int(size) {
		c.metrics.corruptReads.Inc()
		c.Delete(key) // Remove corrupt entry
		return nil, false
	}

	// Verify checksum
	if crc32.ChecksumIEEE(data) != checksum {
		c.metrics.corruptReads.Inc()
		c.Delete(key) // Remove corrupt entry
		return nil, false
	}

	c.metrics.hits.Inc()
	c.metrics.readLatency.Observe(time.Since(start).Seconds())
	return data, true
}

// Put adds a value to the SSD cache.
// This should only be called after ShouldAdmit returns true.
func (c *SSDCache) Put(key string, data []byte) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}

	// Check if key already exists
	if existing, ok := c.index[key]; ok {
		// Update: mark old space as free and update entry
		c.freeSpace += int64(existing.size)
		existing.accesses = 0
		c.mu.Unlock()

		c.queueWrite(key, data)
		return
	}
	c.mu.Unlock()

	c.queueWrite(key, data)
}

// PutWithStats adds a value to the SSD cache with admission policy check.
func (c *SSDCache) PutWithStats(key string, data []byte, l1Stats *L1EvictStats) bool {
	if !c.ShouldAdmit(key, l1Stats) {
		c.metrics.rejections.Inc()
		return false
	}

	c.Put(key, data)
	c.metrics.admissions.Inc()
	return true
}

// Delete removes a key from the cache.
func (c *SSDCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.index[key]
	if !ok {
		return
	}

	// Remove from segment list
	if entry.protected {
		c.protected.Remove(entry.listElem)
		c.protectedSize -= int64(entry.size)
	} else {
		c.probation.Remove(entry.listElem)
		c.probationSize -= int64(entry.size)
	}

	// Mark space as free
	c.freeSpace += int64(entry.size)

	delete(c.index, key)
	c.updateMetrics()
}

// Has checks if a key exists in the cache.
func (c *SSDCache) Has(key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.index[key]
	return ok
}

// Size returns the current size of the cache in bytes.
func (c *SSDCache) Size() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.probationSize + c.protectedSize
}

// Len returns the number of items in the cache.
func (c *SSDCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.index)
}

// Close shuts down the cache and flushes pending writes.
func (c *SSDCache) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()

	// Signal writer to stop
	close(c.stopCh)
	c.writeCond.Signal()
	c.wg.Wait()

	// Stop flush timer
	c.flushTimer.Stop()

	// Final flush
	c.flush()

	// Save index
	if err := c.saveIndex(); err != nil {
		return fmt.Errorf("failed to save index: %w", err)
	}

	return c.dataFile.Close()
}

// queueWrite adds a write operation to the buffer.
func (c *SSDCache) queueWrite(key string, data []byte) {
	checksum := crc32.ChecksumIEEE(data)
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	c.writeMu.Lock()
	c.writeBuf = append(c.writeBuf, writeOp{
		key:      key,
		data:     dataCopy,
		size:     int32(len(data)),
		checksum: checksum,
	})

	shouldFlush := len(c.writeBuf) >= c.flushSize
	c.writeMu.Unlock()

	if shouldFlush {
		c.writeCond.Signal()
	}
}

// backgroundWriter processes write operations.
func (c *SSDCache) backgroundWriter() {
	defer c.wg.Done()

	for {
		c.writeMu.Lock()
		for len(c.writeBuf) == 0 {
			select {
			case <-c.stopCh:
				c.writeMu.Unlock()
				return
			default:
				c.writeCond.Wait()
			}
		}

		// Grab the buffer
		ops := c.writeBuf
		c.writeBuf = make([]writeOp, 0, c.flushSize)
		c.writeMu.Unlock()

		// Process writes
		c.processWrites(ops)
	}
}

// timerFlush triggers a flush from the timer.
func (c *SSDCache) timerFlush() {
	c.writeCond.Signal()

	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()

	if !closed {
		c.flushTimer.Reset(time.Second)
	}
}

// processWrites writes a batch of operations to disk.
func (c *SSDCache) processWrites(ops []writeOp) {
	if len(ops) == 0 {
		return
	}

	start := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, op := range ops {
		// Ensure we have space
		for c.probationSize+c.protectedSize+int64(op.size) > c.maxSize {
			c.evict()
		}

		// Write data to file
		offset := c.writeOffset
		n, err := c.dataFile.WriteAt(op.data, offset)
		if err != nil || n != len(op.data) {
			continue // Skip failed write
		}

		c.writeOffset += int64(n)

		// Create or update index entry
		if existing, ok := c.index[op.key]; ok {
			// Update existing entry
			existing.offset = offset
			existing.size = op.size
			existing.checksum = op.checksum
		} else {
			// New entry - add to probation segment
			entry := &ssdEntry{
				key:       op.key,
				offset:    offset,
				size:      op.size,
				checksum:  op.checksum,
				accesses:  0,
				protected: false,
			}
			elem := c.probation.PushFront(entry)
			entry.listElem = elem
			c.index[op.key] = entry
			c.probationSize += int64(op.size)
		}
	}

	// Sync to disk
	c.dataFile.Sync()
	c.metrics.flushes.Inc()
	c.metrics.writeLatency.Observe(time.Since(start).Seconds())
	c.updateMetrics()
}

// flush forces a flush of the write buffer.
func (c *SSDCache) flush() {
	c.writeMu.Lock()
	ops := c.writeBuf
	c.writeBuf = make([]writeOp, 0, c.flushSize)
	c.writeMu.Unlock()

	c.processWrites(ops)
}

// promote moves an entry from probation to protected segment.
func (c *SSDCache) promote(entry *ssdEntry) {
	// Remove from probation
	c.probation.Remove(entry.listElem)
	c.probationSize -= int64(entry.size)

	// Make room in protected if needed
	for c.protectedSize+int64(entry.size) > c.protectedTarget {
		c.demoteFromProtected()
	}

	// Add to protected
	elem := c.protected.PushFront(entry)
	entry.listElem = elem
	entry.protected = true
	c.protectedSize += int64(entry.size)

	c.metrics.promotions.Inc()
	c.updateMetrics()
}

// demoteFromProtected moves the LRU item from protected to probation.
func (c *SSDCache) demoteFromProtected() {
	elem := c.protected.Back()
	if elem == nil {
		return
	}

	entry := elem.Value.(*ssdEntry)
	c.protected.Remove(elem)
	c.protectedSize -= int64(entry.size)

	// Add to front of probation (it was popular recently)
	newElem := c.probation.PushFront(entry)
	entry.listElem = newElem
	entry.protected = false
	entry.accesses = 1 // Reset access count but keep one
	c.probationSize += int64(entry.size)

	c.updateMetrics()
}

// evict removes the least valuable item from the cache.
func (c *SSDCache) evict() {
	// Prefer evicting from probation
	if c.probation.Len() > 0 {
		elem := c.probation.Back()
		if elem != nil {
			entry := elem.Value.(*ssdEntry)
			c.probation.Remove(elem)
			c.probationSize -= int64(entry.size)
			c.freeSpace += int64(entry.size)
			delete(c.index, entry.key)
			c.metrics.evictions.Inc()
			c.updateMetrics()
			return
		}
	}

	// Fall back to protected
	if c.protected.Len() > 0 {
		elem := c.protected.Back()
		if elem != nil {
			entry := elem.Value.(*ssdEntry)
			c.protected.Remove(elem)
			c.protectedSize -= int64(entry.size)
			c.freeSpace += int64(entry.size)
			delete(c.index, entry.key)
			c.metrics.evictions.Inc()
			c.updateMetrics()
		}
	}
}

// updateMetrics updates the Prometheus metrics.
func (c *SSDCache) updateMetrics() {
	c.metrics.size.Set(float64(c.probationSize + c.protectedSize))
	c.metrics.probationSize.Set(float64(c.probationSize))
	c.metrics.protectedSize.Set(float64(c.protectedSize))
}

// SSDCacheStats holds cache statistics.
type SSDCacheStats struct {
	Size          int64
	MaxSize       int64
	ProbationSize int64
	ProtectedSize int64
	Items         int
	FreeSpace     int64
}

// Stats returns current cache statistics.
func (c *SSDCache) Stats() SSDCacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return SSDCacheStats{
		Size:          c.probationSize + c.protectedSize,
		MaxSize:       c.maxSize,
		ProbationSize: c.probationSize,
		ProtectedSize: c.protectedSize,
		Items:         len(c.index),
		FreeSpace:     c.freeSpace,
	}
}

// Index file format:
// [4 bytes: magic] [4 bytes: version] [4 bytes: entry count]
// For each entry:
//   [2 bytes: key length] [key bytes] [8 bytes: offset] [4 bytes: size] [4 bytes: checksum] [1 byte: accesses] [1 byte: protected]

const (
	indexMagic   = 0x53534443 // "SSDC"
	indexVersion = 1
)

// saveIndex persists the in-memory index to disk.
func (c *SSDCache) saveIndex() error {
	indexPath := filepath.Join(c.path, "cache.idx")
	f, err := os.Create(indexPath)
	if err != nil {
		return err
	}
	defer f.Close()

	// Write header
	header := make([]byte, 12)
	binary.LittleEndian.PutUint32(header[0:4], indexMagic)
	binary.LittleEndian.PutUint32(header[4:8], indexVersion)
	binary.LittleEndian.PutUint32(header[8:12], uint32(len(c.index)))
	if _, err := f.Write(header); err != nil {
		return err
	}

	// Write entries
	for key, entry := range c.index {
		// Key length and key
		keyBytes := []byte(key)
		if err := binary.Write(f, binary.LittleEndian, uint16(len(keyBytes))); err != nil {
			return err
		}
		if _, err := f.Write(keyBytes); err != nil {
			return err
		}

		// Entry fields
		if err := binary.Write(f, binary.LittleEndian, entry.offset); err != nil {
			return err
		}
		if err := binary.Write(f, binary.LittleEndian, entry.size); err != nil {
			return err
		}
		if err := binary.Write(f, binary.LittleEndian, entry.checksum); err != nil {
			return err
		}
		if err := binary.Write(f, binary.LittleEndian, entry.accesses); err != nil {
			return err
		}
		protectedByte := uint8(0)
		if entry.protected {
			protectedByte = 1
		}
		if err := binary.Write(f, binary.LittleEndian, protectedByte); err != nil {
			return err
		}
	}

	return f.Sync()
}

// loadIndex loads the index from disk.
func (c *SSDCache) loadIndex() error {
	indexPath := filepath.Join(c.path, "cache.idx")
	f, err := os.Open(indexPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No index file, start fresh
		}
		return err
	}
	defer f.Close()

	// Read header
	header := make([]byte, 12)
	if _, err := io.ReadFull(f, header); err != nil {
		return err
	}

	magic := binary.LittleEndian.Uint32(header[0:4])
	version := binary.LittleEndian.Uint32(header[4:8])
	count := binary.LittleEndian.Uint32(header[8:12])

	if magic != indexMagic {
		return errors.New("invalid index file magic")
	}
	if version != indexVersion {
		return errors.New("unsupported index version")
	}

	// Read entries
	for i := uint32(0); i < count; i++ {
		// Key length
		var keyLen uint16
		if err := binary.Read(f, binary.LittleEndian, &keyLen); err != nil {
			return err
		}

		// Key
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(f, keyBytes); err != nil {
			return err
		}
		key := string(keyBytes)

		// Entry fields
		var offset int64
		var size int32
		var checksum uint32
		var accesses uint8
		var protectedByte uint8

		if err := binary.Read(f, binary.LittleEndian, &offset); err != nil {
			return err
		}
		if err := binary.Read(f, binary.LittleEndian, &size); err != nil {
			return err
		}
		if err := binary.Read(f, binary.LittleEndian, &checksum); err != nil {
			return err
		}
		if err := binary.Read(f, binary.LittleEndian, &accesses); err != nil {
			return err
		}
		if err := binary.Read(f, binary.LittleEndian, &protectedByte); err != nil {
			return err
		}

		entry := &ssdEntry{
			key:       key,
			offset:    offset,
			size:      size,
			checksum:  checksum,
			accesses:  accesses,
			protected: protectedByte == 1,
		}

		if entry.protected {
			elem := c.protected.PushBack(entry)
			entry.listElem = elem
			c.protectedSize += int64(size)
		} else {
			elem := c.probation.PushBack(entry)
			entry.listElem = elem
			c.probationSize += int64(size)
		}

		c.index[key] = entry
	}

	c.updateMetrics()
	return nil
}

// Compact reclaims free space by rewriting the data file.
// This is an expensive operation and should be called during low-traffic periods.
func (c *SSDCache) Compact(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Create temporary file
	tempPath := filepath.Join(c.path, "cache.data.tmp")
	tempFile, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	// Copy all live data to temp file
	newOffset := int64(0)
	for _, entry := range c.index {
		select {
		case <-ctx.Done():
			tempFile.Close()
			os.Remove(tempPath)
			return ctx.Err()
		default:
		}

		// Read old data
		data := make([]byte, entry.size)
		if _, err := c.dataFile.ReadAt(data, entry.offset); err != nil {
			continue // Skip corrupt entries
		}

		// Verify checksum
		if crc32.ChecksumIEEE(data) != entry.checksum {
			continue // Skip corrupt entries
		}

		// Write to new file
		if _, err := tempFile.WriteAt(data, newOffset); err != nil {
			tempFile.Close()
			os.Remove(tempPath)
			return fmt.Errorf("failed to write to temp file: %w", err)
		}

		entry.offset = newOffset
		newOffset += int64(entry.size)
	}

	if err := tempFile.Sync(); err != nil {
		tempFile.Close()
		os.Remove(tempPath)
		return fmt.Errorf("failed to sync temp file: %w", err)
	}

	tempFile.Close()

	// Replace old file
	oldPath := filepath.Join(c.path, "cache.data")
	c.dataFile.Close()

	if err := os.Rename(tempPath, oldPath); err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	// Reopen data file
	c.dataFile, err = os.OpenFile(oldPath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen data file: %w", err)
	}

	c.writeOffset = newOffset
	c.freeSpace = c.maxSize - newOffset

	return c.saveIndex()
}
