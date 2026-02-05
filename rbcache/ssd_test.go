package rbcache

import (
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestSSDCache_BasicOperations(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewSSDCache(SSDCacheConfig{
		Path:          dir,
		MaxSizeBytes:  1024 * 1024, // 1MB
		FlushSize:     10,
		FlushInterval: 100 * time.Millisecond,
		MetricsName:   "test_ssd_basic",
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Put some data
	key1 := "key1"
	data1 := []byte("hello world")
	cache.Put(key1, data1)

	// Wait for flush
	time.Sleep(200 * time.Millisecond)

	// Get should work
	got, ok := cache.Get(key1)
	if !ok {
		t.Fatal("Expected to find key1")
	}
	if string(got) != string(data1) {
		t.Errorf("Got %q, want %q", got, data1)
	}

	// Has should work
	if !cache.Has(key1) {
		t.Error("Expected Has(key1) to be true")
	}

	// Delete should work
	cache.Delete(key1)
	if cache.Has(key1) {
		t.Error("Expected Has(key1) to be false after delete")
	}

	_, ok = cache.Get(key1)
	if ok {
		t.Error("Expected Get(key1) to fail after delete")
	}
}

func TestSSDCache_AdmissionPolicy(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewSSDCache(SSDCacheConfig{
		Path:          dir,
		MaxSizeBytes:  1024 * 1024,
		FlushSize:     10,
		FlushInterval: 100 * time.Millisecond,
		MetricsName:   "test_ssd_admission",
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Test admission policy
	tests := []struct {
		name     string
		stats    *L1EvictStats
		expected bool
	}{
		{
			name:     "nil stats should reject",
			stats:    nil,
			expected: false,
		},
		{
			name:     "0 accesses should reject",
			stats:    &L1EvictStats{AccessCount: 0, ReadCount: 0, WriteCount: 0},
			expected: false,
		},
		{
			name:     "1 access should reject",
			stats:    &L1EvictStats{AccessCount: 1, ReadCount: 1, WriteCount: 0},
			expected: false,
		},
		{
			name:     "2 accesses, read-heavy should admit",
			stats:    &L1EvictStats{AccessCount: 2, ReadCount: 2, WriteCount: 0},
			expected: true,
		},
		{
			name:     "write-heavy should reject",
			stats:    &L1EvictStats{AccessCount: 5, ReadCount: 1, WriteCount: 5},
			expected: false,
		},
		{
			name:     "balanced should admit",
			stats:    &L1EvictStats{AccessCount: 10, ReadCount: 5, WriteCount: 5},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cache.ShouldAdmit("testkey", tt.stats)
			if result != tt.expected {
				t.Errorf("ShouldAdmit() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestSSDCache_PutWithStats(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewSSDCache(SSDCacheConfig{
		Path:          dir,
		MaxSizeBytes:  1024 * 1024,
		FlushSize:     10,
		FlushInterval: 100 * time.Millisecond,
		MetricsName:   "test_ssd_putwithstats",
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Should reject with low access count
	admitted := cache.PutWithStats("key1", []byte("data"), &L1EvictStats{
		AccessCount: 1,
		ReadCount:   1,
		WriteCount:  0,
	})
	if admitted {
		t.Error("Expected rejection with 1 access")
	}

	// Should admit with sufficient accesses
	admitted = cache.PutWithStats("key2", []byte("data"), &L1EvictStats{
		AccessCount: 3,
		ReadCount:   3,
		WriteCount:  0,
	})
	if !admitted {
		t.Error("Expected admission with 3 accesses")
	}

	// Wait for flush and verify
	time.Sleep(200 * time.Millisecond)

	if cache.Has("key1") {
		t.Error("key1 should not be in cache")
	}
	if !cache.Has("key2") {
		t.Error("key2 should be in cache")
	}
}

func TestSSDCache_SLRU_Promotion(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewSSDCache(SSDCacheConfig{
		Path:          dir,
		MaxSizeBytes:  1024 * 1024,
		FlushSize:     10,
		FlushInterval: 100 * time.Millisecond,
		MetricsName:   "test_ssd_slru",
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Add an item
	cache.Put("key1", []byte("test data"))
	time.Sleep(200 * time.Millisecond)

	// First access - still in probation
	stats := cache.Stats()
	if stats.ProtectedSize != 0 {
		t.Errorf("Expected empty protected segment, got %d", stats.ProtectedSize)
	}

	// Access multiple times to trigger promotion
	for i := 0; i < 3; i++ {
		cache.Get("key1")
	}

	// Should now be in protected
	stats = cache.Stats()
	if stats.ProtectedSize == 0 {
		t.Error("Expected item to be promoted to protected segment")
	}
}

func TestSSDCache_Eviction(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewSSDCache(SSDCacheConfig{
		Path:          dir,
		MaxSizeBytes:  1000, // Very small
		FlushSize:     5,
		FlushInterval: 50 * time.Millisecond,
		MetricsName:   "test_ssd_eviction",
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Add items until we force eviction
	for i := 0; i < 20; i++ {
		key := string(rune('a' + i))
		data := make([]byte, 100)
		cache.Put(key, data)
	}

	// Wait for all writes
	time.Sleep(300 * time.Millisecond)

	// Cache should not exceed max size
	stats := cache.Stats()
	if stats.Size > cache.maxSize {
		t.Errorf("Cache size %d exceeds max %d", stats.Size, cache.maxSize)
	}

	// Should have fewer than 20 items
	if stats.Items >= 20 {
		t.Errorf("Expected some items to be evicted, got %d items", stats.Items)
	}
}

func TestSSDCache_Persistence(t *testing.T) {
	dir := t.TempDir()

	// Create cache and add data
	cache1, err := NewSSDCache(SSDCacheConfig{
		Path:          dir,
		MaxSizeBytes:  1024 * 1024,
		FlushSize:     10,
		FlushInterval: 100 * time.Millisecond,
		MetricsName:   "test_ssd_persist1",
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	cache1.Put("persist_key", []byte("persist_data"))
	time.Sleep(200 * time.Millisecond)

	// Verify data is there
	got, ok := cache1.Get("persist_key")
	if !ok {
		t.Fatal("Expected to find persist_key")
	}
	if string(got) != "persist_data" {
		t.Errorf("Got %q, want %q", got, "persist_data")
	}

	// Close cache
	if err := cache1.Close(); err != nil {
		t.Fatalf("Failed to close cache: %v", err)
	}

	// Reopen cache with different metrics name to avoid duplicate registration
	cache2, err := NewSSDCache(SSDCacheConfig{
		Path:          dir,
		MaxSizeBytes:  1024 * 1024,
		FlushSize:     10,
		FlushInterval: 100 * time.Millisecond,
		MetricsName:   "test_ssd_persist2",
	})
	if err != nil {
		t.Fatalf("Failed to reopen cache: %v", err)
	}
	defer cache2.Close()

	// Data should still be there
	got, ok = cache2.Get("persist_key")
	if !ok {
		t.Fatal("Expected to find persist_key after reopen")
	}
	if string(got) != "persist_data" {
		t.Errorf("Got %q, want %q", got, "persist_data")
	}
}

func TestSSDCache_ChecksumValidation(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewSSDCache(SSDCacheConfig{
		Path:          dir,
		MaxSizeBytes:  1024 * 1024,
		FlushSize:     10,
		FlushInterval: 100 * time.Millisecond,
		MetricsName:   "test_ssd_checksum",
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	cache.Put("key1", []byte("original data"))
	time.Sleep(200 * time.Millisecond)

	// Corrupt the data file
	cache.mu.Lock()
	entry := cache.index["key1"]
	offset := entry.offset
	cache.mu.Unlock()

	// Write garbage at the offset
	cache.dataFile.WriteAt([]byte("corrupted!"), offset)

	// Get should fail due to checksum mismatch
	_, ok := cache.Get("key1")
	if ok {
		t.Error("Expected Get to fail on corrupted data")
	}

	// Entry should be removed
	if cache.Has("key1") {
		t.Error("Expected corrupted entry to be removed")
	}

	cache.Close()
}

func TestSSDCache_Concurrent(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewSSDCache(SSDCacheConfig{
		Path:          dir,
		MaxSizeBytes:  10 * 1024 * 1024, // 10MB
		FlushSize:     50,
		FlushInterval: 100 * time.Millisecond,
		MetricsName:   "test_ssd_concurrent",
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	opsPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				key := string(rune('a'+id)) + string(rune('0'+j%10))
				data := make([]byte, 100)
				rand.Read(data)

				cache.Put(key, data)

				// Sometimes read
				if j%3 == 0 {
					cache.Get(key)
				}

				// Sometimes delete
				if j%7 == 0 {
					cache.Delete(key)
				}
			}
		}(i)
	}

	wg.Wait()

	// Cache should be in valid state
	stats := cache.Stats()
	if stats.Size < 0 {
		t.Errorf("Invalid cache size: %d", stats.Size)
	}
}

func TestSSDCache_Compact(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewSSDCache(SSDCacheConfig{
		Path:          dir,
		MaxSizeBytes:  1024 * 1024,
		FlushSize:     10,
		FlushInterval: 100 * time.Millisecond,
		MetricsName:   "test_ssd_compact",
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Add and delete many items to create fragmentation
	for i := 0; i < 50; i++ {
		key := string(rune('a' + (i % 26)))
		data := make([]byte, 1000)
		cache.Put(key, data)
	}
	time.Sleep(300 * time.Millisecond)

	// Delete half
	for i := 0; i < 13; i++ {
		key := string(rune('a' + i))
		cache.Delete(key)
	}

	// Get file size before compaction
	dataPath := filepath.Join(dir, "cache.data")
	statBefore, _ := os.Stat(dataPath)
	sizeBefore := statBefore.Size()

	// Compact
	ctx := context.Background()
	if err := cache.Compact(ctx); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	// File should be smaller after compaction
	statAfter, _ := os.Stat(dataPath)
	sizeAfter := statAfter.Size()

	if sizeAfter >= sizeBefore {
		t.Errorf("Expected file to shrink after compaction: before=%d, after=%d", sizeBefore, sizeAfter)
	}

	// Remaining items should still be accessible
	for i := 13; i < 26; i++ {
		key := string(rune('a' + i))
		if !cache.Has(key) {
			t.Errorf("Expected key %q to exist after compaction", key)
		}
	}
}

func TestSSDCache_LargeItems(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewSSDCache(SSDCacheConfig{
		Path:          dir,
		MaxSizeBytes:  10 * 1024 * 1024, // 10MB
		FlushSize:     5,
		FlushInterval: 100 * time.Millisecond,
		MetricsName:   "test_ssd_large",
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Write a large item
	largeData := make([]byte, 1024*1024) // 1MB
	rand.Read(largeData)

	cache.Put("large_key", largeData)
	time.Sleep(300 * time.Millisecond)

	// Read it back
	got, ok := cache.Get("large_key")
	if !ok {
		t.Fatal("Expected to find large_key")
	}
	if len(got) != len(largeData) {
		t.Errorf("Got data length %d, want %d", len(got), len(largeData))
	}

	// Verify content
	for i := 0; i < len(largeData); i++ {
		if got[i] != largeData[i] {
			t.Errorf("Data mismatch at byte %d", i)
			break
		}
	}
}

var (
	benchSSDCache     *SSDCache
	benchSSDDir       string
	benchSSDCacheOnce sync.Once
)

func getBenchSSDCache(b *testing.B) *SSDCache {
	benchSSDCacheOnce.Do(func() {
		var err error
		benchSSDDir, err = os.MkdirTemp("", "ssd_bench_*")
		if err != nil {
			b.Fatalf("Failed to create temp dir: %v", err)
		}
		benchSSDCache, err = NewSSDCache(SSDCacheConfig{
			Path:          benchSSDDir,
			MaxSizeBytes:  100 * 1024 * 1024, // 100MB
			FlushSize:     1000,
			FlushInterval: time.Second,
			MetricsName:   "bench_ssd",
		})
		if err != nil {
			b.Fatalf("Failed to create cache: %v", err)
		}
	})
	return benchSSDCache
}

func BenchmarkSSDCache_Put(b *testing.B) {
	cache := getBenchSSDCache(b)

	data := make([]byte, 1024)
	rand.Read(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := string(rune(i % 10000))
		cache.Put(key, data)
	}
}

func BenchmarkSSDCache_Get(b *testing.B) {
	cache := getBenchSSDCache(b)

	// Prepopulate
	data := make([]byte, 1024)
	rand.Read(data)
	numItems := 1000
	for i := 0; i < numItems; i++ {
		key := string(rune(i))
		cache.Put(key, data)
	}
	time.Sleep(500 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := string(rune(i % numItems))
		cache.Get(key)
	}
}
