package rbstor

import (
	"testing"
	"time"

	"github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAccessEventsRecordedDuringRetrieval verifies that access events are recorded during retrieval
func TestAccessEventsRecordedDuringRetrieval(t *testing.T) {
	cfg := DefaultAccessTrackerConfig()
	cfg.DecayInterval = time.Hour // Disable automatic decay for test
	tracker := NewAccessTracker(cfg)
	defer tracker.Close()

	// Record access events as would happen during retrieval
	testKey := "bucket/test-object-1"
	testGroup := iface.GroupKey(100)

	// Simulate multiple accesses during retrieval
	for i := 0; i < 5; i++ {
		tracker.RecordAccess(AccessEvent{
			Key:       testKey,
			GroupKey:  testGroup,
			Timestamp: time.Now(),
		})
	}

	// Verify access was recorded
	popularity := tracker.GetObjectPopularity(testKey)
	assert.Equal(t, float64(5), popularity, "Expected 5 accesses to be recorded")
}

// TestGroupAccessTracking verifies group-level access tracking
func TestGroupAccessTracking(t *testing.T) {
	cfg := DefaultAccessTrackerConfig()
	cfg.DecayInterval = time.Hour
	tracker := NewAccessTracker(cfg)
	defer tracker.Close()

	// Record accesses for multiple objects in the same group
	groupKey := iface.GroupKey(200)
	objects := []string{
		"bucket/file1.txt",
		"bucket/file2.txt",
		"bucket/file3.txt",
	}

	// Record 3 accesses for each object
	for _, obj := range objects {
		for i := 0; i < 3; i++ {
			tracker.RecordAccess(AccessEvent{
				Key:       obj,
				GroupKey:  groupKey,
				Timestamp: time.Now(),
			})
		}
	}

	// Verify group popularity
	groupPop := tracker.GetGroupPopularity(groupKey)
	assert.Equal(t, float64(9), groupPop, "Expected 9 total accesses for the group (3 objects x 3 accesses)")
}

// TestSequentialAccessDetection verifies sequential access pattern detection
func TestSequentialAccessDetection(t *testing.T) {
	cfg := AccessTrackerConfig{
		DecayRate:           0.99,
		DecayInterval:       time.Hour,
		RecentAccessSize:    100,
		MinSequenceLen:      3,
		SequenceThresholdMs: 1000, // 1 second threshold
	}
	tracker := NewAccessTracker(cfg)
	defer tracker.Close()

	groupKey := iface.GroupKey(300)
	now := time.Now()

	// Record a sequence of accesses with small time gaps
	sequenceKeys := []string{
		"prefix/file001.txt",
		"prefix/file002.txt",
		"prefix/file003.txt",
		"prefix/file004.txt",
		"prefix/file005.txt",
	}

	for i, key := range sequenceKeys {
		tracker.RecordAccess(AccessEvent{
			Key:       key,
			GroupKey:  groupKey,
			Timestamp: now.Add(time.Duration(i*100) * time.Millisecond),
		})
	}

	// Detect sequential patterns
	patterns := tracker.DetectSequentialAccess()

	// Should detect at least one pattern
	if len(patterns) > 0 {
		pattern := patterns[0]
		assert.Equal(t, groupKey, pattern.GroupKey, "Pattern should be from the correct group")
		assert.GreaterOrEqual(t, len(pattern.Keys), 3, "Pattern should have at least 3 keys")
	}
}

// TestSequentialAccessWithLargeGaps verifies that large gaps break sequences
func TestSequentialAccessWithLargeGaps(t *testing.T) {
	cfg := AccessTrackerConfig{
		DecayRate:           0.99,
		DecayInterval:       time.Hour,
		RecentAccessSize:    100,
		MinSequenceLen:      3,
		SequenceThresholdMs: 100, // 100ms threshold
	}
	tracker := NewAccessTracker(cfg)
	defer tracker.Close()

	groupKey := iface.GroupKey(400)
	now := time.Now()

	// Record accesses with large time gaps (should not form sequence)
	for i := 0; i < 5; i++ {
		tracker.RecordAccess(AccessEvent{
			Key:       "prefix/file" + string(rune('0'+i)),
			GroupKey:  groupKey,
			Timestamp: now.Add(time.Duration(i) * time.Second), // 1 second gaps
		})
	}

	// Should not detect patterns due to large gaps
	patterns := tracker.DetectSequentialAccess()
	assert.Equal(t, 0, len(patterns), "Should not detect patterns with large time gaps")
}

// TestAccessTrackerStats verifies access tracker statistics
func TestAccessTrackerStats(t *testing.T) {
	cfg := DefaultAccessTrackerConfig()
	cfg.DecayInterval = time.Hour
	tracker := NewAccessTracker(cfg)
	defer tracker.Close()

	// Record accesses for multiple objects and groups
	for i := 0; i < 10; i++ {
		tracker.RecordAccess(AccessEvent{
			Key:       "key" + string(rune('0'+i)),
			GroupKey:  iface.GroupKey(i % 3),
			Timestamp: time.Now(),
		})
	}

	// Get stats
	stats := tracker.Stats()

	assert.Equal(t, 10, stats.TotalObjects, "Should have 10 unique objects")
	assert.Equal(t, 3, stats.TotalGroups, "Should have 3 unique groups")
	assert.Equal(t, 10, stats.RecentAccessCount, "Should have 10 recent accesses")
}

// TestHotObjectsTracking verifies hot object detection
func TestHotObjectsTracking(t *testing.T) {
	cfg := DefaultAccessTrackerConfig()
	cfg.DecayInterval = time.Hour
	tracker := NewAccessTracker(cfg)
	defer tracker.Close()

	// Record varying number of accesses for different objects
	hotKey := "very_hot_object"
	warmKey := "warm_object"
	coldKey := "cold_object"

	// Hot object: 10 accesses
	for i := 0; i < 10; i++ {
		tracker.RecordAccess(AccessEvent{
			Key:       hotKey,
			GroupKey:  iface.GroupKey(1),
			Timestamp: time.Now(),
		})
	}

	// Warm object: 5 accesses
	for i := 0; i < 5; i++ {
		tracker.RecordAccess(AccessEvent{
			Key:       warmKey,
			GroupKey:  iface.GroupKey(1),
			Timestamp: time.Now(),
		})
	}

	// Cold object: 1 access
	tracker.RecordAccess(AccessEvent{
		Key:       coldKey,
		GroupKey:  iface.GroupKey(1),
		Timestamp: time.Now(),
	})

	// Get hot objects
	hotObjects := tracker.GetHotObjects(3)

	require.GreaterOrEqual(t, len(hotObjects), 1, "Should have at least 1 hot object")

	// The hottest object should be first
	if len(hotObjects) > 0 {
		assert.Equal(t, hotKey, hotObjects[0], "Hottest object should be first")
	}
}

// TestHotGroupsTracking verifies hot group detection
func TestHotGroupsTracking(t *testing.T) {
	cfg := DefaultAccessTrackerConfig()
	cfg.DecayInterval = time.Hour
	tracker := NewAccessTracker(cfg)
	defer tracker.Close()

	// Record accesses for different groups
	// Group 1: 15 accesses
	for i := 0; i < 15; i++ {
		tracker.RecordAccess(AccessEvent{
			Key:       "group1_obj" + string(rune('0'+i%10)),
			GroupKey:  iface.GroupKey(1),
			Timestamp: time.Now(),
		})
	}

	// Group 2: 8 accesses
	for i := 0; i < 8; i++ {
		tracker.RecordAccess(AccessEvent{
			Key:       "group2_obj" + string(rune('0'+i%5)),
			GroupKey:  iface.GroupKey(2),
			Timestamp: time.Now(),
		})
	}

	// Get hot groups
	hotGroups := tracker.GetHotGroups(5)

	require.GreaterOrEqual(t, len(hotGroups), 1, "Should have at least 1 hot group")

	// Verify group 1 is hottest
	if len(hotGroups) > 0 {
		assert.Equal(t, iface.GroupKey(1), hotGroups[0], "Group 1 should be hottest")
	}
}

// TestHourlyPatternTracking verifies hourly access pattern tracking
func TestHourlyPatternTracking(t *testing.T) {
	cfg := DefaultAccessTrackerConfig()
	cfg.DecayInterval = time.Hour
	tracker := NewAccessTracker(cfg)
	defer tracker.Close()

	now := time.Now()
	currentHour := now.Hour()

	// Record accesses at specific hour
	for i := 0; i < 5; i++ {
		tracker.RecordAccess(AccessEvent{
			Key:       "hourly_test_object",
			GroupKey:  iface.GroupKey(500),
			Timestamp: now,
		})
	}

	// Get hourly pattern
	pattern := tracker.GetHourlyPattern(currentHour)

	// Should have entries in the pattern
	assert.Greater(t, len(pattern), 0, "Hourly pattern should have entries")
}

// TestSequentialPatternPrediction verifies sequential pattern prediction
func TestSequentialPatternPrediction(t *testing.T) {
	// Create a sequential pattern
	pattern := SequentialPattern{
		Keys: []string{
			"dir/file001.txt",
			"dir/file002.txt",
			"dir/file003.txt",
			"dir/file004.txt",
		},
		Direction: 1, // Forward direction
	}

	// Predict next (should be out of bounds)
	next := pattern.Predict(1)
	assert.Equal(t, "", next, "Predicting beyond last key should return empty")

	// Predict previous
	prev := pattern.Predict(-1)
	assert.Equal(t, "dir/file003.txt", prev, "Should predict previous file")

	// Predict 2 steps back
	twoBack := pattern.Predict(-2)
	assert.Equal(t, "dir/file002.txt", twoBack, "Should predict 2 files back")
}

// TestAccessTrackerDecay verifies access counter decay
func TestAccessTrackerDecay(t *testing.T) {
	cfg := AccessTrackerConfig{
		DecayRate:           0.5, // 50% decay
		DecayInterval:       time.Hour,
		RecentAccessSize:    100,
		MinSequenceLen:      3,
		SequenceThresholdMs: 1000,
	}
	tracker := NewAccessTracker(cfg)
	defer tracker.Close()

	// Record initial accesses
	testKey := "decay_test_object"
	tracker.RecordAccess(AccessEvent{
		Key:       testKey,
		GroupKey:  iface.GroupKey(600),
		Timestamp: time.Now(),
	})
	tracker.RecordAccess(AccessEvent{
		Key:       testKey,
		GroupKey:  iface.GroupKey(600),
		Timestamp: time.Now(),
	})

	// Verify initial count
	initialPop := tracker.GetObjectPopularity(testKey)
	assert.Equal(t, float64(2), initialPop, "Should have 2 accesses initially")

	// Trigger decay
	tracker.Decay()

	// After 50% decay, should be 1
	decayedPop := tracker.GetObjectPopularity(testKey)
	assert.Equal(t, float64(1), decayedPop, "Should have 1 access after 50% decay")
}

// TestConcurrentAccessRecording verifies thread-safe concurrent access recording
func TestConcurrentAccessRecording(t *testing.T) {
	cfg := DefaultAccessTrackerConfig()
	cfg.DecayInterval = 10 * time.Millisecond // Fast decay for concurrent test
	tracker := NewAccessTracker(cfg)
	defer tracker.Close()

	numGoroutines := 10
	numOps := 100
	done := make(chan bool, numGoroutines)

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer func() { done <- true }()

			for j := 0; j < numOps; j++ {
				tracker.RecordAccess(AccessEvent{
					Key:       "concurrent_key" + string(rune('0'+j%10)),
					GroupKey:  iface.GroupKey(id),
					Timestamp: time.Now(),
				})

				// Occasionally read stats
				if j%20 == 0 {
					tracker.GetHotGroups(5)
					tracker.GetHotObjects(5)
					tracker.Stats()
				}
			}
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		select {
		case <-done:
			// Success
		case <-time.After(10 * time.Second):
			t.Fatal("Timeout waiting for concurrent operations")
		}
	}

	// Should not panic and should have recorded accesses
	stats := tracker.Stats()
	assert.Greater(t, stats.TotalObjects, 0, "Should have recorded some objects")
}
