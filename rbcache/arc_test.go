package rbcache

import (
	"fmt"
	"sync"
	"testing"
)

func TestARCCache_Basic(t *testing.T) {
	cache := NewARCCache[string, []byte](1000, func(v []byte) int64 { return int64(len(v)) }, "test_basic")

	// Test Put and Get
	cache.Put("key1", []byte("value1"))

	val, found := cache.Get("key1")
	if !found {
		t.Error("Expected to find key1")
	}
	if string(val) != "value1" {
		t.Errorf("Expected 'value1', got '%s'", string(val))
	}

	// Test missing key
	_, found = cache.Get("missing")
	if found {
		t.Error("Expected not to find missing key")
	}

	// Test Has
	if !cache.Has("key1") {
		t.Error("Expected Has to return true for key1")
	}
	if cache.Has("missing") {
		t.Error("Expected Has to return false for missing key")
	}
}

func TestARCCache_Eviction(t *testing.T) {
	// Small cache that can hold ~10 bytes
	cache := NewARCCache[string, []byte](10, func(v []byte) int64 { return int64(len(v)) }, "test_eviction")

	// Add items that exceed capacity
	cache.Put("k1", []byte("aaaa")) // 4 bytes
	cache.Put("k2", []byte("bbbb")) // 4 bytes
	cache.Put("k3", []byte("cccc")) // 4 bytes - should evict k1

	// k1 should be evicted
	if cache.Has("k1") {
		t.Error("Expected k1 to be evicted")
	}

	// k2 and k3 should still be there
	if !cache.Has("k2") {
		t.Error("Expected k2 to be present")
	}
	if !cache.Has("k3") {
		t.Error("Expected k3 to be present")
	}

	// Size should be within capacity
	if cache.Size() > 10 {
		t.Errorf("Cache size %d exceeds capacity 10", cache.Size())
	}
}

func TestARCCache_Promotion(t *testing.T) {
	cache := NewARCCache[string, []byte](100, func(v []byte) int64 { return int64(len(v)) }, "test_promotion")

	// Add item to T1
	cache.Put("key", []byte("value"))

	// First access - item is in T1
	stats := cache.Stats()
	if stats.T1Size == 0 {
		t.Error("Expected item in T1 after Put")
	}

	// Second access - should promote to T2
	cache.Get("key")

	stats = cache.Stats()
	if stats.T1Size != 0 {
		t.Error("Expected T1 to be empty after promotion")
	}
	if stats.T2Size == 0 {
		t.Error("Expected item in T2 after promotion")
	}
}

func TestARCCache_ScanResistance(t *testing.T) {
	// Create cache that can hold 20 items
	cache := NewARCCache[int, int](20, func(v int) int64 { return 1 }, "test_scan")

	// Simulate working set of 10 items accessed frequently
	for i := 0; i < 10; i++ {
		cache.Put(i, i)
		cache.Get(i) // Access again to promote to T2
	}

	// Verify working set is in T2
	stats := cache.Stats()
	if stats.T2Size < 10 {
		t.Errorf("Expected working set in T2, got T2Size=%d", stats.T2Size)
	}

	// Simulate a scan of 100 new items (scan attack)
	for i := 100; i < 200; i++ {
		cache.Put(i, i)
	}

	// Working set items (frequently accessed) should mostly survive
	survivedCount := 0
	for i := 0; i < 10; i++ {
		if cache.Has(i) {
			survivedCount++
		}
	}

	// Due to ARC's scan resistance, most working set items should survive
	// We expect at least half to survive (ARC favors T2 over T1)
	if survivedCount < 5 {
		t.Errorf("Expected at least 5 working set items to survive scan, got %d", survivedCount)
	}
}

func TestARCCache_GhostListAdaptation(t *testing.T) {
	cache := NewARCCache[int, int](10, func(v int) int64 { return 1 }, "test_ghost")

	// Fill cache
	for i := 0; i < 10; i++ {
		cache.Put(i, i)
	}

	// Cause eviction from T1 - these go to B1
	for i := 10; i < 15; i++ {
		cache.Put(i, i)
	}

	// Access an item that was evicted (in B1 ghost list)
	// This should trigger adaptation and increase p
	oldP := cache.Stats().P
	cache.Put(0, 0) // Item 0 was evicted, putting it back should adapt

	newP := cache.Stats().P
	// Note: adaptation only happens if item is found in ghost list
	// The Put of 0 should have increased p if 0 was in B1
	t.Logf("P changed from %d to %d", oldP, newP)
}

func TestARCCache_Update(t *testing.T) {
	cache := NewARCCache[string, []byte](100, func(v []byte) int64 { return int64(len(v)) }, "test_update")

	cache.Put("key", []byte("value1"))

	// Update with new value
	cache.Put("key", []byte("value2_longer"))

	val, found := cache.Get("key")
	if !found {
		t.Error("Expected to find key after update")
	}
	if string(val) != "value2_longer" {
		t.Errorf("Expected 'value2_longer', got '%s'", string(val))
	}
}

func TestARCCache_Delete(t *testing.T) {
	cache := NewARCCache[string, []byte](100, func(v []byte) int64 { return int64(len(v)) }, "test_delete")

	cache.Put("key", []byte("value"))
	cache.Delete("key")

	if cache.Has("key") {
		t.Error("Expected key to be deleted")
	}

	_, found := cache.Get("key")
	if found {
		t.Error("Expected not to find deleted key")
	}
}

func TestARCCache_Clear(t *testing.T) {
	cache := NewARCCache[int, int](100, func(v int) int64 { return 1 }, "test_clear")

	for i := 0; i < 50; i++ {
		cache.Put(i, i)
	}

	if cache.Len() == 0 {
		t.Error("Expected items in cache before clear")
	}

	cache.Clear()

	if cache.Len() != 0 {
		t.Errorf("Expected empty cache after clear, got %d items", cache.Len())
	}
	if cache.Size() != 0 {
		t.Errorf("Expected zero size after clear, got %d", cache.Size())
	}
}

func TestARCCache_Concurrent(t *testing.T) {
	cache := NewARCCache[int, int](1000, func(v int) int64 { return 1 }, "test_concurrent")

	var wg sync.WaitGroup
	numGoroutines := 10
	numOps := 1000

	// Concurrent writes
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < numOps; i++ {
				key := base*numOps + i
				cache.Put(key, key)
			}
		}(g)
	}

	// Concurrent reads
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < numOps; i++ {
				key := base*numOps + i
				cache.Get(key)
			}
		}(g)
	}

	wg.Wait()

	// Should not panic and cache should be in valid state
	stats := cache.Stats()
	t.Logf("After concurrent test: Size=%d, Items=%d", stats.Size, stats.Items)
}

var (
	benchCache     *ARCCache[int, []byte]
	benchCacheOnce sync.Once
)

func getBenchCache() *ARCCache[int, []byte] {
	benchCacheOnce.Do(func() {
		benchCache = NewARCCache[int, []byte](1<<20, func(v []byte) int64 { return int64(len(v)) }, "bench")
	})
	return benchCache
}

func BenchmarkARCCache_Put(b *testing.B) {
	cache := getBenchCache()
	value := make([]byte, 256)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Put(i%10000, value)
	}
}

func BenchmarkARCCache_Get(b *testing.B) {
	cache := getBenchCache()
	value := make([]byte, 256)

	// Pre-populate
	for i := 0; i < 10000; i++ {
		cache.Put(i, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get(i % 10000)
	}
}

func BenchmarkARCCache_Mixed(b *testing.B) {
	cache := getBenchCache()
	value := make([]byte, 256)

	// Pre-populate
	for i := 0; i < 10000; i++ {
		cache.Put(i, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%3 == 0 {
			cache.Put(i%10000, value)
		} else {
			cache.Get(i % 10000)
		}
	}
}

// Test to verify the cache respects capacity exactly
func TestARCCache_CapacityEnforcement(t *testing.T) {
	capacity := int64(100)
	cache := NewARCCache[string, []byte](capacity, func(v []byte) int64 { return int64(len(v)) }, "test_capacity")

	// Add items that should fill to capacity
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key%d", i)
		value := []byte("12345") // 5 bytes each
		cache.Put(key, value)
	}

	// Size should not exceed capacity
	if cache.Size() > capacity {
		t.Errorf("Cache size %d exceeds capacity %d", cache.Size(), capacity)
	}
}
