package rbstor

import (
	"sync"
	"testing"
	"time"

	"github.com/CIDgravity/filecoin-gateway/iface"
)

func TestDecayingCounter_Basic(t *testing.T) {
	counter := NewDecayingCounter[string](0.9)

	// Increment a few times
	counter.Increment("key1")
	counter.Increment("key1")
	counter.Increment("key2")

	if got := counter.Get("key1"); got != 2 {
		t.Errorf("Get(key1) = %v, want 2", got)
	}
	if got := counter.Get("key2"); got != 1 {
		t.Errorf("Get(key2) = %v, want 1", got)
	}
	if got := counter.Get("key3"); got != 0 {
		t.Errorf("Get(key3) = %v, want 0", got)
	}
}

func TestDecayingCounter_Decay(t *testing.T) {
	counter := NewDecayingCounter[string](0.5) // 50% decay

	counter.Add("key1", 10)
	counter.Add("key2", 1)

	// After decay
	counter.Decay()

	if got := counter.Get("key1"); got != 5 {
		t.Errorf("Get(key1) after decay = %v, want 5", got)
	}

	// key2 starts at 1, after one decay it's 0.5, after another it's 0.25, etc.
	// After first decay key2 = 0.5, key1 = 5
	// Need more decays to get key2 below 0.01 threshold
	// 1 * 0.5^n < 0.01 => n > log(0.01)/log(0.5) => n > 6.64
	for i := 0; i < 7; i++ {
		counter.Decay()
	}

	// key2 should now be removed (below threshold of 0.01)
	if counter.Get("key2") >= 0.01 {
		t.Errorf("Expected key2 to be removed after decay, got %v", counter.Get("key2"))
	}
}

func TestDecayingCounter_TopN(t *testing.T) {
	counter := NewDecayingCounter[string](0.99)

	counter.Add("a", 10)
	counter.Add("b", 30)
	counter.Add("c", 20)
	counter.Add("d", 5)

	top2 := counter.TopN(2)
	if len(top2) != 2 {
		t.Fatalf("TopN(2) returned %d items, want 2", len(top2))
	}
	if top2[0] != "b" {
		t.Errorf("TopN[0] = %v, want b", top2[0])
	}
	if top2[1] != "c" {
		t.Errorf("TopN[1] = %v, want c", top2[1])
	}

	// Test requesting more than available
	top10 := counter.TopN(10)
	if len(top10) != 4 {
		t.Errorf("TopN(10) returned %d items, want 4", len(top10))
	}
}

func TestDecayingCounter_Concurrent(t *testing.T) {
	counter := NewDecayingCounter[string](0.99)

	var wg sync.WaitGroup
	numGoroutines := 10
	numOps := 1000

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := string(rune('a' + (j % 26)))
				counter.Increment(key)
				counter.Get(key)
				if j%100 == 0 {
					counter.Decay()
					counter.TopN(5)
				}
			}
		}(i)
	}

	wg.Wait()
	// Should not panic
}

func TestAccessTracker_RecordAccess(t *testing.T) {
	cfg := DefaultAccessTrackerConfig()
	cfg.DecayInterval = time.Hour // Disable automatic decay for test
	tracker := NewAccessTracker(cfg)
	defer tracker.Close()

	// Record some accesses
	tracker.RecordAccess(AccessEvent{
		Key:      "bucket/key1",
		GroupKey: 100,
	})
	tracker.RecordAccess(AccessEvent{
		Key:      "bucket/key1",
		GroupKey: 100,
	})
	tracker.RecordAccess(AccessEvent{
		Key:      "bucket/key2",
		GroupKey: 200,
	})

	// Check object popularity
	if pop := tracker.GetObjectPopularity("bucket/key1"); pop != 2 {
		t.Errorf("GetObjectPopularity(bucket/key1) = %v, want 2", pop)
	}

	// Check group popularity
	if pop := tracker.GetGroupPopularity(100); pop != 2 {
		t.Errorf("GetGroupPopularity(100) = %v, want 2", pop)
	}

	// Check hot objects
	hot := tracker.GetHotObjects(5)
	if len(hot) != 2 {
		t.Errorf("GetHotObjects(5) returned %d items, want 2", len(hot))
	}
	if hot[0] != "bucket/key1" {
		t.Errorf("Hot object 0 = %v, want bucket/key1", hot[0])
	}
}

func TestAccessTracker_HourlyPattern(t *testing.T) {
	cfg := DefaultAccessTrackerConfig()
	cfg.DecayInterval = time.Hour
	tracker := NewAccessTracker(cfg)
	defer tracker.Close()

	// Record accesses at specific hour
	now := time.Now()
	hour := now.Hour()

	tracker.RecordAccess(AccessEvent{
		Key:       "hourly_key",
		GroupKey:  100,
		Timestamp: now,
	})

	pattern := tracker.GetHourlyPattern(hour)
	if len(pattern) == 0 {
		t.Error("Expected hourly pattern to have entries")
	}
}

func TestAccessTracker_SequentialDetection(t *testing.T) {
	cfg := AccessTrackerConfig{
		DecayRate:           0.99,
		DecayInterval:       time.Hour,
		RecentAccessSize:    100,
		MinSequenceLen:      3,
		SequenceThresholdMs: 1000, // 1 second threshold
	}
	tracker := NewAccessTracker(cfg)
	defer tracker.Close()

	// Record a sequence of accesses
	now := time.Now()
	gk := iface.GroupKey(100)

	// Sequential accesses with small time gaps
	for i := 0; i < 5; i++ {
		tracker.RecordAccess(AccessEvent{
			Key:       "prefix/file" + string(rune('0'+i)),
			GroupKey:  gk,
			Timestamp: now.Add(time.Duration(i*100) * time.Millisecond),
		})
	}

	patterns := tracker.DetectSequentialAccess()
	if len(patterns) == 0 {
		t.Error("Expected to detect sequential pattern")
	}

	if len(patterns) > 0 {
		p := patterns[0]
		if p.GroupKey != gk {
			t.Errorf("Pattern group key = %v, want %v", p.GroupKey, gk)
		}
		if len(p.Keys) < 3 {
			t.Errorf("Pattern has %d keys, want at least 3", len(p.Keys))
		}
	}
}

func TestAccessTracker_NoSequenceWithLargeGaps(t *testing.T) {
	cfg := AccessTrackerConfig{
		DecayRate:           0.99,
		DecayInterval:       time.Hour,
		RecentAccessSize:    100,
		MinSequenceLen:      3,
		SequenceThresholdMs: 100, // 100ms threshold
	}
	tracker := NewAccessTracker(cfg)
	defer tracker.Close()

	now := time.Now()
	gk := iface.GroupKey(100)

	// Accesses with large time gaps (should not form sequence)
	for i := 0; i < 5; i++ {
		tracker.RecordAccess(AccessEvent{
			Key:       "prefix/file" + string(rune('0'+i)),
			GroupKey:  gk,
			Timestamp: now.Add(time.Duration(i) * time.Second), // 1 second gaps
		})
	}

	patterns := tracker.DetectSequentialAccess()
	if len(patterns) > 0 {
		t.Errorf("Expected no patterns due to large gaps, got %d", len(patterns))
	}
}

func TestAccessTracker_Stats(t *testing.T) {
	cfg := DefaultAccessTrackerConfig()
	cfg.DecayInterval = time.Hour
	tracker := NewAccessTracker(cfg)
	defer tracker.Close()

	// Record some accesses
	for i := 0; i < 10; i++ {
		tracker.RecordAccess(AccessEvent{
			Key:      "key" + string(rune('0'+i)),
			GroupKey: iface.GroupKey(i % 3),
		})
	}

	stats := tracker.Stats()
	if stats.TotalObjects != 10 {
		t.Errorf("TotalObjects = %d, want 10", stats.TotalObjects)
	}
	if stats.TotalGroups != 3 {
		t.Errorf("TotalGroups = %d, want 3", stats.TotalGroups)
	}
	if stats.RecentAccessCount != 10 {
		t.Errorf("RecentAccessCount = %d, want 10", stats.RecentAccessCount)
	}
}

func TestSequentialPattern_Predict(t *testing.T) {
	pattern := SequentialPattern{
		Keys:      []string{"file0", "file1", "file2", "file3"},
		Direction: 1,
	}

	// Predict next (offset 1)
	// With direction 1, offset 1 from last (index 3) = index 4, out of bounds
	next := pattern.Predict(1)
	if next != "" {
		t.Errorf("Predict(1) = %v, expected empty for out of bounds", next)
	}

	// Predict previous
	prev := pattern.Predict(-1)
	if prev != "file2" {
		t.Errorf("Predict(-1) = %v, want file2", prev)
	}
}

func TestCommonPrefix(t *testing.T) {
	tests := []struct {
		strs     []string
		expected string
	}{
		{[]string{"abc", "abd", "abe"}, "ab"},
		{[]string{"hello", "hello"}, "hello"},
		{[]string{"a", "b", "c"}, ""},
		{[]string{}, ""},
		{[]string{"single"}, "single"},
		{[]string{"prefix/a", "prefix/b", "prefix/c"}, "prefix/"},
	}

	for _, tt := range tests {
		got := commonPrefix(tt.strs)
		if got != tt.expected {
			t.Errorf("commonPrefix(%v) = %q, want %q", tt.strs, got, tt.expected)
		}
	}
}

func TestAccessTracker_Concurrent(t *testing.T) {
	cfg := DefaultAccessTrackerConfig()
	cfg.DecayInterval = 10 * time.Millisecond // Fast decay for test
	tracker := NewAccessTracker(cfg)
	defer tracker.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	numOps := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				tracker.RecordAccess(AccessEvent{
					Key:      "key" + string(rune('0'+j%10)),
					GroupKey: iface.GroupKey(id),
				})

				// Occasionally read stats
				if j%20 == 0 {
					tracker.GetHotGroups(5)
					tracker.GetHotObjects(5)
					tracker.Stats()
					tracker.DetectSequentialAccess()
				}
			}
		}(i)
	}

	wg.Wait()
	// Should not panic
}

func BenchmarkAccessTracker_RecordAccess(b *testing.B) {
	cfg := DefaultAccessTrackerConfig()
	tracker := NewAccessTracker(cfg)
	defer tracker.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tracker.RecordAccess(AccessEvent{
			Key:      "key" + string(rune(i%1000)),
			GroupKey: iface.GroupKey(i % 100),
		})
	}
}

func BenchmarkAccessTracker_GetHotObjects(b *testing.B) {
	cfg := DefaultAccessTrackerConfig()
	tracker := NewAccessTracker(cfg)
	defer tracker.Close()

	// Populate
	for i := 0; i < 10000; i++ {
		tracker.RecordAccess(AccessEvent{
			Key:      "key" + string(rune(i%1000)),
			GroupKey: iface.GroupKey(i % 100),
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tracker.GetHotObjects(10)
	}
}

func BenchmarkDecayingCounter_TopN(b *testing.B) {
	counter := NewDecayingCounter[string](0.99)

	// Populate
	for i := 0; i < 10000; i++ {
		counter.Add(string(rune(i)), float64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		counter.TopN(10)
	}
}
