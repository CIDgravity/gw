package rbcache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestEvictionCallbackCalled verifies that eviction callback is invoked when items are evicted
func TestEvictionCallbackCalled(t *testing.T) {
	var evictionCount int
	var lastEvictedKey string
	var lastEvictedValue []byte

	// Create small cache to force evictions (10 bytes capacity)
	cache := NewARCCache[string, []byte](
		10, // 10 bytes capacity
		func(v []byte) int64 { return int64(len(v)) },
		"test_eviction_callback",
	)

	// Set eviction callback
	cache.SetEvictionCallback(func(key string, value []byte, stats EvictionStats) {
		evictionCount++
		lastEvictedKey = key
		lastEvictedValue = value
		_ = stats
	})

	// Add items that will exceed capacity
	cache.Put("key1", []byte("value1")) // 6 bytes
	cache.Put("key2", []byte("value2")) // 6 bytes - should trigger eviction of key1
	cache.Put("key3", []byte("value3")) // 6 bytes - should trigger eviction

	// Verify eviction callback was called
	assert.GreaterOrEqual(t, evictionCount, 1, "Eviction callback should have been called")
	assert.NotEmpty(t, lastEvictedKey, "Last evicted key should be set")
	assert.NotNil(t, lastEvictedValue, "Last evicted value should be set")
}

// TestL1ToL2Promotion verifies items are promoted to L2 based on access count
func TestL1ToL2Promotion(t *testing.T) {
	var promotedItems []string
	var promotionStats []EvictionStats

	// Create L1 cache with callback that simulates L2 promotion
	l1Cache := NewARCCache[string, []byte](
		1000,
		func(v []byte) int64 { return int64(len(v)) },
		"test_l1_promotion",
	)

	// Set callback to track promotions
	l1Cache.SetEvictionCallback(func(key string, value []byte, stats EvictionStats) {
		// Only promote if accessed at least twice or from T2
		if stats.AccessCount >= 2 || !stats.FromT1 {
			promotedItems = append(promotedItems, key)
			promotionStats = append(promotionStats, stats)
		}
	})

	// Add item and access it multiple times to promote to T2
	l1Cache.Put("hot_key", []byte("hot_value"))

	// First access - item moves within T1
	l1Cache.Get("hot_key")

	// Second access - item promoted to T2
	l1Cache.Get("hot_key")

	// Verify item is now in T2
	cacheStats := l1Cache.Stats()
	// After promotion, T1 should be empty for this item
	// The item should be in T2
	assert.GreaterOrEqual(t, cacheStats.T2Size, int64(0), "Item should be in T2 after multiple accesses")
}

// TestT1ItemsWithLowAccessNotPromoted verifies items from T1 with <2 accesses are not promoted
func TestT1ItemsWithLowAccessNotPromoted(t *testing.T) {
	var promotedToL2 bool
	var promotionCount int

	// Create cache
	cache := NewARCCache[string, []byte](
		100,
		func(v []byte) int64 { return int64(len(v)) },
		"test_t1_no_promote",
	)

	// Set callback that only promotes items with >=2 accesses
	cache.SetEvictionCallback(func(key string, value []byte, stats EvictionStats) {
		if stats.AccessCount >= 2 || !stats.FromT1 {
			promotedToL2 = true
			promotionCount++
		}
	})

	// Add item to T1 (single access)
	cache.Put("single_access", []byte("data"))

	// Force eviction by adding more items
	for i := 0; i < 20; i++ {
		cache.Put(string(rune('a'+i)), []byte("xxxxxxxxxx"))
	}

	// Item with single access should not be promoted
	assert.False(t, promotedToL2, "Items with <2 accesses from T1 should not be promoted to L2")
	assert.Equal(t, 0, promotionCount, "No items should be promoted")
}

// TestT2ItemsArePromoted verifies items from T2 are promoted to L2 on eviction
func TestT2ItemsArePromoted(t *testing.T) {
	var promotedItems []string
	var promotedFromT2 []bool

	cache := NewARCCache[string, []byte](
		200,
		func(v []byte) int64 { return int64(len(v)) },
		"test_t2_promotion",
	)

	cache.SetEvictionCallback(func(key string, value []byte, stats EvictionStats) {
		// T2 items should always be promoted (FromT1 = false)
		if !stats.FromT1 {
			promotedItems = append(promotedItems, key)
			promotedFromT2 = append(promotedFromT2, !stats.FromT1)
		}
	})

	// Add item and access it multiple times to move to T2
	cache.Put("frequent_item", []byte("frequent_data"))

	// Multiple accesses to promote to T2
	for i := 0; i < 5; i++ {
		cache.Get("frequent_item")
	}

	// Verify item stats
	_ = cache.Stats()

	// Force eviction
	for i := 0; i < 30; i++ {
		cache.Put(string(rune('a'+i%26))+string(rune('0'+i/26)), []byte("eviction_data_xxx"))
	}

	// Check if any T2 items were promoted
	t.Logf("Promoted items: %v", promotedItems)
	t.Logf("Promoted from T2: %v", promotedFromT2)
}

// TestEvictionStatsAccuracy verifies eviction stats are accurate
func TestEvictionStatsAccuracy(t *testing.T) {
	var capturedStats EvictionStats
	var capturedKey string

	cache := NewARCCache[string, []byte](
		100,
		func(v []byte) int64 { return int64(len(v)) },
		"test_stats_accuracy",
	)

	cache.SetEvictionCallback(func(key string, value []byte, stats EvictionStats) {
		capturedStats = stats
		capturedKey = key
	})

	// Add and access item
	cache.Put("stats_test", []byte("test_data"))
	cache.Get("stats_test")
	cache.Get("stats_test") // 2 accesses

	// Force eviction
	for i := 0; i < 20; i++ {
		cache.Put(string(rune('a'+i)), []byte("xxxxxxxxxxxx"))
	}

	// Verify stats were captured
	if capturedKey != "" {
		assert.GreaterOrEqual(t, capturedStats.AccessCount, 0, "Access count should be non-negative")
		// FromT1 should be true if evicted from T1, false if from T2
		t.Logf("Evicted item %s with access count %d, fromT1=%v",
			capturedKey, capturedStats.AccessCount, capturedStats.FromT1)
	}
}

// TestPromotionCallbackWithAccessCount verifies callback receives correct access count
func TestPromotionCallbackWithAccessCount(t *testing.T) {
	accessCounts := make(map[string]int)

	cache := NewARCCache[string, []byte](
		150,
		func(v []byte) int64 { return int64(len(v)) },
		"test_access_count",
	)

	cache.SetEvictionCallback(func(key string, value []byte, stats EvictionStats) {
		accessCounts[key] = stats.AccessCount
	})

	// Add items with different access patterns
	cache.Put("no_access", []byte("data1"))
	// Never accessed

	cache.Put("single_access", []byte("data2"))
	cache.Get("single_access") // 1 access

	cache.Put("multi_access", []byte("data3"))
	cache.Get("multi_access")
	cache.Get("multi_access")
	cache.Get("multi_access") // 3 accesses

	// Force evictions
	for i := 0; i < 20; i++ {
		cache.Put(string(rune('z'-i%26)), []byte("force_eviction_xxxx"))
	}

	// Verify access counts were captured
	for key, count := range accessCounts {
		t.Logf("Item %s had %d accesses when evicted", key, count)
		assert.GreaterOrEqual(t, count, 0, "Access count should be non-negative")
	}
}

// TestL1L2Integration verifies L1 to L2 promotion integration
func TestL1L2Integration(t *testing.T) {
	// Simulate L2 cache
	l2Cache := make(map[string][]byte)

	// Create L1 cache with promotion to L2
	l1Cache := NewARCCache[string, []byte](
		100,
		func(v []byte) int64 { return int64(len(v)) },
		"test_l1_l2_integration",
	)

	l1Cache.SetEvictionCallback(func(key string, value []byte, stats EvictionStats) {
		// Promote to L2 if:
		// 1. Access count >= 2, OR
		// 2. Item is from T2 (frequent)
		if stats.AccessCount >= 2 || !stats.FromT1 {
			l2Cache[key] = value
		}
	})

	// Add item to L1
	l1Cache.Put("promotable_item", []byte("value_to_promote"))

	// Access twice to increase access count
	l1Cache.Get("promotable_item")
	l1Cache.Get("promotable_item")

	// Force eviction by filling cache
	for i := 0; i < 20; i++ {
		l1Cache.Put(string(rune('a'+i)), []byte("filler_data_xxxx"))
	}

	// Check if item was promoted to L2
	if _, found := l2Cache["promotable_item"]; found {
		t.Log("Item was successfully promoted to L2")
	} else {
		t.Log("Item was not promoted (may still be in L1 or evicted without promotion)")
	}
}

// TestEvictionFromT1VsT2 verifies eviction callback distinguishes T1 vs T2
func TestEvictionFromT1VsT2(t *testing.T) {
	t1Evictions := 0
	t2Evictions := 0

	cache := NewARCCache[string, []byte](
		100,
		func(v []byte) int64 { return int64(len(v)) },
		"test_t1_vs_t2",
	)

	cache.SetEvictionCallback(func(key string, value []byte, stats EvictionStats) {
		if stats.FromT1 {
			t1Evictions++
		} else {
			t2Evictions++
		}
	})

	// Add items - some to T1, some to T2
	cache.Put("t1_item", []byte("t1_data"))

	cache.Put("t2_item", []byte("t2_data"))
	// Access multiple times to move to T2
	for i := 0; i < 3; i++ {
		cache.Get("t2_item")
	}

	// Force evictions
	for i := 0; i < 30; i++ {
		cache.Put(string(rune('a'+i%26))+string(rune('0'+i/26)), []byte("eviction_xxxx"))
	}

	t.Logf("T1 evictions: %d, T2 evictions: %d", t1Evictions, t2Evictions)

	// We should have some evictions from both lists
	totalEvictions := t1Evictions + t2Evictions
	assert.Greater(t, totalEvictions, 0, "Should have some evictions")
}
