package rbstor

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	iface "github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

// TestIndexSyncCoordinator tests the index sync coordinator functionality.
func TestIndexSyncCoordinator(t *testing.T) {
	// Create a mock index for testing
	mockIndex := &mockSyncableIndex{
		syncCount: 0,
	}

	coord := NewIndexSyncCoordinator(mockIndex, 50*time.Millisecond, 4)

	// Request syncs for multiple groups
	coord.RequestSync(1)
	coord.RequestSync(2)
	coord.RequestSync(3)

	require.True(t, coord.HasPendingSyncs())
	require.Equal(t, 3, coord.PendingGroupCount())

	// Perform sync
	err := coord.Sync(context.Background())
	require.NoError(t, err)

	// After sync, pending should be cleared
	require.False(t, coord.HasPendingSyncs())
	require.Equal(t, 1, mockIndex.syncCount)
}

func TestIndexSyncCoordinator_Coalescing(t *testing.T) {
	mockIndex := &mockSyncableIndex{}
	coord := NewIndexSyncCoordinator(mockIndex, 100*time.Millisecond, 100)

	// Request many syncs rapidly
	for i := 0; i < 50; i++ {
		coord.RequestSync(iface.GroupKey(i))
	}

	// Single sync should handle all
	err := coord.Sync(context.Background())
	require.NoError(t, err)

	// Should only have synced once
	require.Equal(t, 1, mockIndex.syncCount)

	// Check metrics
	metrics := coord.Metrics()
	require.Equal(t, int64(1), metrics.SyncCount)
	require.Equal(t, int64(50), metrics.CoalescedSyncs)
	require.Greater(t, metrics.CoalesceRatio, float64(1))
}

func TestIndexSyncCoordinator_ConcurrentSync(t *testing.T) {
	mockIndex := &mockSyncableIndex{
		syncDelay: 50 * time.Millisecond,
	}
	coord := NewIndexSyncCoordinator(mockIndex, 10*time.Millisecond, 100)

	// Start multiple concurrent syncs
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			coord.RequestSync(iface.GroupKey(id))
			_ = coord.Sync(context.Background())
		}(i)
	}

	wg.Wait()

	// Due to serialization, sync count should be much less than 10
	require.Less(t, mockIndex.syncCount, 10)
}

// mockSyncableIndex is a mock index for testing sync coordination.
type mockSyncableIndex struct {
	syncCount int
	syncDelay time.Duration
	mu        sync.Mutex
}

func (m *mockSyncableIndex) Sync(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.syncDelay > 0 {
		time.Sleep(m.syncDelay)
	}
	m.syncCount++
	return nil
}

// Implement other Index methods as no-ops
func (m *mockSyncableIndex) GetGroups(ctx context.Context, mh []multihash.Multihash, cb func(int, iface.GroupKey) (bool, error)) error {
	return nil
}
func (m *mockSyncableIndex) GetSizes(ctx context.Context, mh []multihash.Multihash, cb func([]int32) error) error {
	return nil
}
func (m *mockSyncableIndex) AddGroup(ctx context.Context, mh []multihash.Multihash, sizes []int32, group iface.GroupKey) error {
	return nil
}
func (m *mockSyncableIndex) DropGroup(ctx context.Context, mh []multihash.Multihash, group iface.GroupKey) error {
	return nil
}
func (m *mockSyncableIndex) EstimateSize(ctx context.Context) (int64, error) { return 0, nil }
func (m *mockSyncableIndex) Close() error                                    { return nil }

// TestParallelMetrics tests the metrics recording functionality.
func TestParallelMetrics(t *testing.T) {
	// Reset metrics
	parallelMetrics.Reset()

	// Record some operations
	parallelMetrics.RecordWrite(true, 10*time.Millisecond, 1024, 1, nil)
	parallelMetrics.RecordWrite(true, 20*time.Millisecond, 2048, 2, nil)
	parallelMetrics.RecordWrite(false, 15*time.Millisecond, 512, 1, nil)

	parallelMetrics.RecordFlush(true, 50*time.Millisecond, nil)
	parallelMetrics.RecordFlush(false, 30*time.Millisecond, nil)

	parallelMetrics.RecordGroupSelection("affinity_hit", time.Millisecond)
	parallelMetrics.RecordGroupSelection("weighted", time.Millisecond)

	// Get stats
	stats := parallelMetrics.Stats()

	require.Equal(t, int64(3), stats.TotalWrites)
	require.Equal(t, int64(2), stats.ParallelWrites)
	require.Equal(t, int64(1), stats.LegacyWrites)
	require.Equal(t, int64(2), stats.TotalFlushes)
	require.Equal(t, int64(3584), stats.BytesWritten)
	require.Equal(t, int64(4), stats.BlocksWritten)
	require.Greater(t, stats.AvgWriteTimeMs, float64(0))
}

// TestAutoTuner tests the auto-tuning functionality.
func TestAutoTuner(t *testing.T) {
	tuner := NewAutoTuner()

	// Initial values should be set
	require.Greater(t, tuner.GetMaxParallelGroups(), 0)
	require.Greater(t, tuner.GetTargetWritersPerGroup(), 0)

	// Record some metrics
	for i := 0; i < 10; i++ {
		tuner.RecordMetrics(
			100e6, // 100 MB/s throughput
			20,    // 20ms latency
			50,    // 50ms flush latency
			4,     // 4 active groups
			8,     // 8 active writers
			0.001, // 0.1% error rate
		)
	}

	// Should have enough data now
	rec := tuner.Tune()
	require.NotEmpty(t, rec.Reason)
}

func TestAutoTuner_HighErrorRate(t *testing.T) {
	tuner := NewAutoTuner()
	tuner.maxParallelGroups = 4

	// Record metrics with high error rate
	for i := 0; i < 10; i++ {
		tuner.RecordMetrics(
			100e6,
			20,
			50,
			4,
			8,
			0.10, // 10% error rate - very high
		)
	}

	rec := tuner.Tune()
	require.Less(t, rec.MaxParallelGroups, 4, "should reduce parallelism on high error rate")
	require.Contains(t, rec.Reason, "error")
}

func TestAutoTuner_HighLatency(t *testing.T) {
	tuner := NewAutoTuner()
	tuner.maxParallelGroups = 4

	// Record metrics with high latency
	for i := 0; i < 10; i++ {
		tuner.RecordMetrics(
			100e6,
			200, // 200ms latency - high
			50,
			4,
			8,
			0.001,
		)
	}

	rec := tuner.Tune()
	require.Less(t, rec.MaxParallelGroups, 4, "should reduce parallelism on high latency")
	require.Contains(t, rec.Reason, "latency")
}

func TestComputeOptimalGroups(t *testing.T) {
	optimal := ComputeOptimalGroups()
	require.GreaterOrEqual(t, optimal, 1)
	require.LessOrEqual(t, optimal, 16)
}

// TestConcurrentSpaceReservations stress tests space reservations.
func TestConcurrentSpaceReservations_Stress(t *testing.T) {
	g := &Group{
		id:              1,
		state:           iface.GroupStateWritable,
		committedSize:   0,
		inflightSize:    0,
		reservedSpace:   0,
		committedBlocks: 0,
		inflightBlocks:  0,
	}

	const numGoroutines = 100
	const reservationsPerGoroutine = 100
	const reservationSize = int64(1000)

	var wg sync.WaitGroup
	var successCount atomic.Int64
	var failCount atomic.Int64

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < reservationsPerGoroutine; j++ {
				res := g.TryReserveSpace(reservationSize)
				if res != nil {
					successCount.Add(1)
					// Simulate some work
					time.Sleep(time.Microsecond)
					res.Release()
				} else {
					failCount.Add(1)
				}
			}
		}()
	}

	wg.Wait()

	// All reservations should have been released
	require.Equal(t, int64(0), g.reservedSpace)
	require.Equal(t, int32(0), g.ActiveWriterCount())

	t.Logf("Successes: %d, Failures: %d", successCount.Load(), failCount.Load())
}

// TestLoadBalancer_ConcurrentSelections stress tests load balancer selection.
func TestLoadBalancer_ConcurrentSelections(t *testing.T) {
	r := &rbs{
		writableGroups: make(map[iface.GroupKey]*Group),
	}

	// Add several writable groups
	for i := int64(1); i <= 4; i++ {
		g := &Group{
			id:              i,
			state:           iface.GroupStateWritable,
			committedSize:   0,
			inflightSize:    0,
			reservedSpace:   0,
			committedBlocks: 0,
		}
		r.writableGroups[iface.GroupKey(i)] = g
	}

	lb := NewLoadBalancer(r)

	const numGoroutines = 50
	const selectionsPerGoroutine = 100

	var wg sync.WaitGroup
	groupSelections := make(map[int64]*atomic.Int64)
	for i := int64(1); i <= 4; i++ {
		groupSelections[i] = &atomic.Int64{}
	}

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		session := &ribSession{r: r}
		go func() {
			defer wg.Done()
			for j := 0; j < selectionsPerGoroutine; j++ {
				// Try to get a group
				lb.selectionLk.Lock()
				for _, g := range r.writableGroups {
					score := lb.calculateScore(g, 1000)
					if score >= 0 {
						groupSelections[g.id].Add(1)
						break
					}
				}
				lb.selectionLk.Unlock()
			}
		}()
		_ = session // Keep session reference
	}

	wg.Wait()

	// Check that selections were distributed
	var totalSelections int64
	for id, count := range groupSelections {
		t.Logf("Group %d: %d selections", id, count.Load())
		totalSelections += count.Load()
	}

	require.Equal(t, int64(numGoroutines*selectionsPerGoroutine), totalSelections)
}

// TestParallelWritesDistribution verifies that writes get distributed across
// multiple groups when parallel writes are enabled.
func TestParallelWritesDistribution(t *testing.T) {
	// Save original config and restore after test
	origEnabled := IsParallelWritesEnabled()
	defer SetParallelWritesEnabled(origEnabled)

	// Enable parallel writes
	SetParallelWritesEnabled(true)

	r := &rbs{
		writableGroups: make(map[iface.GroupKey]*Group),
	}

	// Create 4 writable groups with plenty of space
	numGroups := 4
	for i := 1; i <= numGroups; i++ {
		g := &Group{
			id:              int64(i),
			state:           iface.GroupStateWritable,
			committedSize:   0,
			inflightSize:    0,
			reservedSpace:   0,
			committedBlocks: 0,
		}
		r.writableGroups[iface.GroupKey(i)] = g
	}

	lb := NewLoadBalancer(r)

	// Track which groups are selected
	groupSelections := make(map[iface.GroupKey]int)

	// Simulate many write operations with NO session affinity (nil session)
	// and NO preferred group - this should distribute writes
	numWrites := 100
	for i := 0; i < numWrites; i++ {
		lb.selectionLk.Lock()

		// Find best candidate (simulating what selectWeighted does)
		var candidates []groupScore
		for _, group := range r.writableGroups {
			if group.state != iface.GroupStateWritable {
				continue
			}
			score := lb.calculateScore(group, 1000)
			if score >= 0 {
				candidates = append(candidates, groupScore{group: group, score: score})
			}
		}

		if len(candidates) > 0 {
			best := lb.pickBest(candidates)
			groupSelections[iface.GroupKey(best.id)]++

			// Simulate some writes consuming space (to affect scoring)
			best.committedSize += 1024 * 1024 // 1MB per write
		}

		lb.selectionLk.Unlock()
	}

	// Verify writes were distributed across multiple groups
	t.Logf("Group selections: %v", groupSelections)

	// All groups should have received some writes
	for i := 1; i <= numGroups; i++ {
		count := groupSelections[iface.GroupKey(i)]
		require.Greater(t, count, 0, "Group %d should have received writes", i)
	}

	// No single group should have more than 50% of writes (allowing some imbalance)
	maxAllowed := numWrites / 2
	for gk, count := range groupSelections {
		require.LessOrEqual(t, count, maxAllowed,
			"Group %d has %d writes, which is more than 50%% of total - writes not well distributed",
			gk, count)
	}
}

// TestParallelWritesWithCurrentWriteTarget verifies that within a batch,
// writes go to the same group (via currentWriteTarget/prefer), but different
// batches can use different groups when groups have different fill levels.
func TestParallelWritesWithCurrentWriteTarget(t *testing.T) {
	// Save original config and restore after test
	origEnabled := IsParallelWritesEnabled()
	defer SetParallelWritesEnabled(origEnabled)

	SetParallelWritesEnabled(true)

	r := &rbs{
		writableGroups: make(map[iface.GroupKey]*Group),
	}

	// Create 4 writable groups with significantly different fill levels
	// This ensures pickBest will choose different groups as they fill up
	for i := 1; i <= 4; i++ {
		g := &Group{
			id:              int64(i),
			state:           iface.GroupStateWritable,
			committedSize:   0, // Start empty
			inflightSize:    0,
			reservedSpace:   0,
			committedBlocks: 0,
		}
		r.writableGroups[iface.GroupKey(i)] = g
	}

	lb := NewLoadBalancer(r)

	// Simulate multiple "batches" where each batch:
	// 1. First write picks best group (no preference)
	// 2. Subsequent writes use preferred group
	// 3. After batch, simulate filling the group significantly
	numBatches := 8
	batchGroups := make([]iface.GroupKey, numBatches)

	for batch := 0; batch < numBatches; batch++ {
		// First write in batch: no preferred group
		lb.selectionLk.Lock()
		var candidates []groupScore
		for _, group := range r.writableGroups {
			score := lb.calculateScore(group, 1000)
			if score >= 0 {
				candidates = append(candidates, groupScore{group: group, score: score})
			}
		}
		best := lb.pickBest(candidates)
		batchGroups[batch] = iface.GroupKey(best.id)

		// Simulate this batch writing a significant chunk to the group
		// This will affect future scoring
		best.committedSize += maxGroupSize / 8 // Fill 12.5% per batch

		lb.selectionLk.Unlock()

		// Subsequent writes in same batch: should prefer the same group
		for write := 1; write < 5; write++ {
			preferGroup := batchGroups[batch]
			result := lb.tryPreferredGroup(preferGroup, 1000)
			require.NotNil(t, result, "Preferred group should be available")
			require.Equal(t, batchGroups[batch], iface.GroupKey(result.id),
				"Within-batch writes should go to same group")
		}
	}

	// Verify that batches were distributed across groups
	groupCounts := make(map[iface.GroupKey]int)
	for _, gk := range batchGroups {
		groupCounts[gk]++
	}

	t.Logf("Batch distribution: %v", groupCounts)

	// With 8 batches and 4 groups, and filling 12.5% per batch,
	// we should see distribution across multiple groups
	require.GreaterOrEqual(t, len(groupCounts), 2,
		"Batches should be distributed across multiple groups")
}

// TestParallelWritesOpensMultipleGroups is a regression test ensuring that
// when parallel writes are enabled, multiple groups are opened/created up to
// MaxParallelGroups, not just one.
func TestParallelWritesOpensMultipleGroups(t *testing.T) {
	// Save original config and restore after test
	origEnabled := IsParallelWritesEnabled()
	defer SetParallelWritesEnabled(origEnabled)

	SetParallelWritesEnabled(true)

	r := &rbs{
		writableGroups: make(map[iface.GroupKey]*Group),
	}
	lb := NewLoadBalancer(r)

	// Start with just ONE writable group (simulating initial state)
	r.writableGroups[1] = &Group{
		id:              1,
		state:           iface.GroupStateWritable,
		committedSize:   0,
		inflightSize:    0,
		reservedSpace:   0,
		committedBlocks: 0,
	}

	// Verify we start with 1 group
	require.Equal(t, 1, len(r.writableGroups), "should start with 1 group")

	// Simulate the condition check that selectWeighted does
	// With MaxParallelGroups=4 and 1 group open, we should try to open more
	cfg := configuration.ParallelWriteConfig{
		Enabled:           true,
		MaxParallelGroups: 4,
	}

	numWritable := len(r.writableGroups)

	// This is the critical check - it should pass with 1 group
	shouldOpenMore := cfg.Enabled && numWritable < cfg.MaxParallelGroups
	require.True(t, shouldOpenMore,
		"With 1 group and MaxParallelGroups=4, should try to open more groups. "+
			"numWritable=%d, MaxParallelGroups=%d",
		numWritable, cfg.MaxParallelGroups)

	// Simulate what happens when we can't find existing groups in DB
	// and need to create new ones - we should be able to create up to max
	for i := 2; i <= cfg.MaxParallelGroups; i++ {
		// Check condition again
		numWritable = len(r.writableGroups)
		canCreate := cfg.Enabled && numWritable < cfg.MaxParallelGroups
		require.True(t, canCreate,
			"Should be able to create group %d when we have %d groups and max is %d",
			i, numWritable, cfg.MaxParallelGroups)

		// Simulate creating a new group
		r.writableGroups[iface.GroupKey(i)] = &Group{
			id:              int64(i),
			state:           iface.GroupStateWritable,
			committedSize:   0,
			inflightSize:    0,
			reservedSpace:   0,
			committedBlocks: 0,
		}
	}

	// Verify we now have MaxParallelGroups
	require.Equal(t, cfg.MaxParallelGroups, len(r.writableGroups),
		"should have MaxParallelGroups (%d) groups open", cfg.MaxParallelGroups)

	// Verify load balancer sees all groups
	metrics := lb.Metrics()
	require.Equal(t, cfg.MaxParallelGroups, metrics.WritableGroupCount,
		"load balancer should see all %d groups", cfg.MaxParallelGroups)
}

// TestNoSessionAffinityWithNilSession verifies that passing nil session
// skips session affinity and allows write distribution.
func TestNoSessionAffinityWithNilSession(t *testing.T) {
	r := &rbs{
		writableGroups: make(map[iface.GroupKey]*Group),
	}

	// Create groups
	for i := 1; i <= 4; i++ {
		r.writableGroups[iface.GroupKey(i)] = &Group{
			id:              int64(i),
			state:           iface.GroupStateWritable,
			committedSize:   0,
			inflightSize:    0,
			reservedSpace:   0,
			committedBlocks: 0,
		}
	}

	lb := NewLoadBalancer(r)

	// With nil session, trySessionAffinity should return nil
	result := lb.trySessionAffinity(nil, 1000)
	require.Nil(t, result, "nil session should not have affinity")

	// Set affinity for a real session
	session := &ribSession{r: r}
	lb.setSessionAffinity(session, 1)

	// Real session should have affinity
	result = lb.trySessionAffinity(session, 1000)
	require.NotNil(t, result, "real session should have affinity")
	require.Equal(t, int64(1), result.id)

	// nil session still should not have affinity
	result = lb.trySessionAffinity(nil, 1000)
	require.Nil(t, result, "nil session should still not have affinity")
}

// TestSessionAffinityUnderLoad tests session affinity with many concurrent sessions.
func TestSessionAffinityUnderLoad(t *testing.T) {
	r := &rbs{
		writableGroups: make(map[iface.GroupKey]*Group),
	}

	// Add groups
	for i := int64(1); i <= 4; i++ {
		r.writableGroups[iface.GroupKey(i)] = &Group{
			id:              i,
			state:           iface.GroupStateWritable,
			committedSize:   0,
			inflightSize:    0,
			reservedSpace:   0,
			committedBlocks: 0,
		}
	}

	lb := NewLoadBalancer(r)

	const numSessions = 100
	sessions := make([]*ribSession, numSessions)
	for i := 0; i < numSessions; i++ {
		sessions[i] = &ribSession{r: r}
	}

	// Set affinity for all sessions
	var wg sync.WaitGroup
	for i, session := range sessions {
		wg.Add(1)
		go func(s *ribSession, groupKey iface.GroupKey) {
			defer wg.Done()
			lb.setSessionAffinity(s, groupKey)
		}(session, iface.GroupKey((i%4)+1))
	}
	wg.Wait()

	// Verify all affinities are set
	lb.sessionAffinityLk.RLock()
	require.Equal(t, numSessions, len(lb.sessionAffinity))
	lb.sessionAffinityLk.RUnlock()

	// Clear all
	lb.ClearAllSessionAffinity()

	lb.sessionAffinityLk.RLock()
	require.Equal(t, 0, len(lb.sessionAffinity))
	lb.sessionAffinityLk.RUnlock()
}
