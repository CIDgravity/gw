package rbstor

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CIDgravity/filecoin-gateway/iface"
)

// IndexSyncCoordinator batches and coalesces index sync requests from multiple
// parallel writers. This prevents excessive Pebble flushes while ensuring
// durability guarantees.
//
// Design:
// - Collects sync requests from multiple groups
// - Batches syncs to reduce Pebble flush frequency
// - Ensures minimum interval between flushes (configurable)
// - Provides async sync option for fire-and-forget durability
type IndexSyncCoordinator struct {
	index iface.GroupIndex

	// Configuration
	minSyncInterval time.Duration // Minimum time between syncs
	maxPendingSync  int           // Max pending syncs before forced flush

	// State
	mu             sync.Mutex
	pendingGroups  map[iface.GroupKey]bool // Groups with pending sync
	lastSync       time.Time               // Last sync timestamp
	syncInProgress atomic.Bool             // Prevents concurrent syncs

	// Metrics
	syncCount       atomic.Int64
	coalescedSyncs  atomic.Int64
	totalSyncTimeNs atomic.Int64
}

// NewIndexSyncCoordinator creates a new sync coordinator.
func NewIndexSyncCoordinator(index iface.GroupIndex, minInterval time.Duration, maxPending int) *IndexSyncCoordinator {
	if minInterval == 0 {
		minInterval = 50 * time.Millisecond
	}
	if maxPending == 0 {
		maxPending = 8
	}

	return &IndexSyncCoordinator{
		index:           index,
		minSyncInterval: minInterval,
		maxPendingSync:  maxPending,
		pendingGroups:   make(map[iface.GroupKey]bool),
		lastSync:        time.Now(),
	}
}

// RequestSync marks a group as needing sync and optionally triggers a sync.
// This is a non-blocking call that coalesces multiple sync requests.
func (c *IndexSyncCoordinator) RequestSync(group iface.GroupKey) {
	c.mu.Lock()
	c.pendingGroups[group] = true
	pendingCount := len(c.pendingGroups)
	c.mu.Unlock()

	// Track coalesced syncs
	c.coalescedSyncs.Add(1)

	// Check if we should trigger a sync
	if pendingCount >= c.maxPendingSync {
		// Many pending syncs, trigger immediately
		go c.maybeSync(context.Background())
	}
}

// Sync ensures all pending groups are synced.
// This blocks until the sync completes.
func (c *IndexSyncCoordinator) Sync(ctx context.Context) error {
	return c.doSync(ctx)
}

// maybeSync performs a sync if enough time has passed since the last one.
func (c *IndexSyncCoordinator) maybeSync(ctx context.Context) error {
	c.mu.Lock()
	if len(c.pendingGroups) == 0 {
		c.mu.Unlock()
		return nil
	}

	timeSinceLastSync := time.Since(c.lastSync)
	if timeSinceLastSync < c.minSyncInterval && len(c.pendingGroups) < c.maxPendingSync {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	return c.doSync(ctx)
}

// doSync performs the actual sync operation.
func (c *IndexSyncCoordinator) doSync(ctx context.Context) error {
	// Prevent concurrent syncs
	if !c.syncInProgress.CompareAndSwap(false, true) {
		// Another sync is in progress, wait for it
		for c.syncInProgress.Load() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Millisecond):
			}
		}
		// The other sync handled our request
		return nil
	}
	defer c.syncInProgress.Store(false)

	// Clear pending groups
	c.mu.Lock()
	c.pendingGroups = make(map[iface.GroupKey]bool)
	c.mu.Unlock()

	// Perform the sync
	start := time.Now()
	err := c.index.Sync(ctx)
	elapsed := time.Since(start)

	// Update metrics
	c.syncCount.Add(1)
	c.totalSyncTimeNs.Add(elapsed.Nanoseconds())

	// Update last sync time
	c.mu.Lock()
	c.lastSync = time.Now()
	c.mu.Unlock()

	return err
}

// Metrics returns current coordinator metrics.
type IndexSyncMetrics struct {
	SyncCount      int64
	CoalescedSyncs int64
	AvgSyncTimeMs  float64
	PendingGroups  int
	CoalesceRatio  float64 // Higher = more requests coalesced per sync
}

// Metrics returns current sync coordinator metrics.
func (c *IndexSyncCoordinator) Metrics() IndexSyncMetrics {
	syncCount := c.syncCount.Load()
	coalescedSyncs := c.coalescedSyncs.Load()
	totalSyncTimeNs := c.totalSyncTimeNs.Load()

	c.mu.Lock()
	pendingGroups := len(c.pendingGroups)
	c.mu.Unlock()

	var avgSyncTimeMs float64
	var coalesceRatio float64

	if syncCount > 0 {
		avgSyncTimeMs = float64(totalSyncTimeNs) / float64(syncCount) / 1e6
		coalesceRatio = float64(coalescedSyncs) / float64(syncCount)
	}

	return IndexSyncMetrics{
		SyncCount:      syncCount,
		CoalescedSyncs: coalescedSyncs,
		AvgSyncTimeMs:  avgSyncTimeMs,
		PendingGroups:  pendingGroups,
		CoalesceRatio:  coalesceRatio,
	}
}

// HasPendingSyncs returns true if there are groups waiting for sync.
func (c *IndexSyncCoordinator) HasPendingSyncs() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pendingGroups) > 0
}

// PendingGroupCount returns the number of groups waiting for sync.
func (c *IndexSyncCoordinator) PendingGroupCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pendingGroups)
}
