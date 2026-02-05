package rbstor

import (
	"container/ring"
	"sort"
	"sync"
	"time"

	"github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// AccessTracker tracks access patterns for objects and groups.
// It provides:
// - Decaying counters for popularity tracking
// - Sequential access detection
// - Hourly access patterns for temporal analysis
//
// This information is used by the prefetch engine to make intelligent
// predictions about what data to fetch ahead of time.
type AccessTracker struct {
	// Object-level popularity (bucket/key -> count)
	objectAccess *DecayingCounter[string]

	// Group-level popularity (group key -> count)
	groupAccess *DecayingCounter[iface.GroupKey]

	// Sequential access detection
	recentMu     sync.Mutex
	recentAccess *ring.Ring // Ring buffer of AccessEvent

	// Hourly access patterns for each hour of day
	hourlyMu     sync.RWMutex
	hourlyAccess [24]*DecayingCounter[string]

	// Configuration
	cfg AccessTrackerConfig

	// Background tasks
	stopCh chan struct{}
	wg     sync.WaitGroup

	// Metrics
	metrics *accessTrackerMetrics
}

// AccessTrackerConfig holds configuration for the access tracker.
type AccessTrackerConfig struct {
	// DecayRate for popularity counters (0.99 = 1% decay per interval)
	DecayRate float64

	// DecayInterval is how often to apply decay
	DecayInterval time.Duration

	// RecentAccessSize is the ring buffer size for recent accesses
	RecentAccessSize int

	// MinSequenceLen is minimum consecutive accesses to detect a sequence
	MinSequenceLen int

	// SequenceThresholdMs is max time between accesses to consider sequential
	SequenceThresholdMs int64
}

// DefaultAccessTrackerConfig returns sensible defaults.
func DefaultAccessTrackerConfig() AccessTrackerConfig {
	return AccessTrackerConfig{
		DecayRate:           0.99,
		DecayInterval:       time.Minute,
		RecentAccessSize:    1000,
		MinSequenceLen:      3,
		SequenceThresholdMs: 500,
	}
}

// AccessEvent represents a single access to an object.
type AccessEvent struct {
	Key       string
	GroupKey  iface.GroupKey
	Timestamp time.Time
	ByteRange *ByteRange // nil if full object access
}

// ByteRange represents a byte range request.
type ByteRange struct {
	Start int64
	End   int64
}

// SequentialPattern represents detected sequential access.
type SequentialPattern struct {
	Prefix     string         // Common prefix for keys
	GroupKey   iface.GroupKey // Group being accessed
	Keys       []string       // Keys in sequence
	Direction  int            // 1 for forward, -1 for backward
	LastAccess time.Time
}

// Predict returns the predicted next key in the sequence.
func (p *SequentialPattern) Predict(offset int) string {
	if len(p.Keys) == 0 {
		return ""
	}
	// For simple sequential patterns, just return keys with offset
	// This could be extended to support more sophisticated prediction
	idx := len(p.Keys) - 1 + (offset * p.Direction)
	if idx >= 0 && idx < len(p.Keys) {
		return p.Keys[idx]
	}
	return ""
}

type accessTrackerMetrics struct {
	accessesRecorded prometheus.Counter
	sequencesFound   prometheus.Counter
	hotGroupsCount   prometheus.Gauge
	hotObjectsCount  prometheus.Gauge
	decayCycles      prometheus.Counter
}

var (
	accessTrackerMetricsOnce     sync.Once
	accessTrackerMetricsInstance *accessTrackerMetrics
)

func getAccessTrackerMetrics() *accessTrackerMetrics {
	accessTrackerMetricsOnce.Do(func() {
		accessTrackerMetricsInstance = &accessTrackerMetrics{
			accessesRecorded: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "fgw",
				Subsystem: "access_tracker",
				Name:      "accesses_recorded_total",
				Help:      "Total access events recorded",
			}),
			sequencesFound: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "fgw",
				Subsystem: "access_tracker",
				Name:      "sequences_found_total",
				Help:      "Total sequential patterns detected",
			}),
			hotGroupsCount: promauto.NewGauge(prometheus.GaugeOpts{
				Namespace: "fgw",
				Subsystem: "access_tracker",
				Name:      "hot_groups_count",
				Help:      "Number of hot groups (above threshold)",
			}),
			hotObjectsCount: promauto.NewGauge(prometheus.GaugeOpts{
				Namespace: "fgw",
				Subsystem: "access_tracker",
				Name:      "hot_objects_count",
				Help:      "Number of hot objects (above threshold)",
			}),
			decayCycles: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "fgw",
				Subsystem: "access_tracker",
				Name:      "decay_cycles_total",
				Help:      "Total decay cycles executed",
			}),
		}
	})
	return accessTrackerMetricsInstance
}

// NewAccessTracker creates a new access tracker.
func NewAccessTracker(cfg AccessTrackerConfig) *AccessTracker {
	if cfg.DecayRate <= 0 || cfg.DecayRate >= 1 {
		cfg.DecayRate = 0.99
	}
	if cfg.DecayInterval <= 0 {
		cfg.DecayInterval = time.Minute
	}
	if cfg.RecentAccessSize <= 0 {
		cfg.RecentAccessSize = 1000
	}
	if cfg.MinSequenceLen <= 0 {
		cfg.MinSequenceLen = 3
	}
	if cfg.SequenceThresholdMs <= 0 {
		cfg.SequenceThresholdMs = 500
	}

	t := &AccessTracker{
		objectAccess: NewDecayingCounter[string](cfg.DecayRate),
		groupAccess:  NewDecayingCounter[iface.GroupKey](cfg.DecayRate),
		recentAccess: ring.New(cfg.RecentAccessSize),
		cfg:          cfg,
		stopCh:       make(chan struct{}),
		metrics:      getAccessTrackerMetrics(),
	}

	// Initialize hourly counters
	for i := 0; i < 24; i++ {
		t.hourlyAccess[i] = NewDecayingCounter[string](cfg.DecayRate)
	}

	// Start background decay
	t.wg.Add(1)
	go t.decayLoop()

	return t
}

// RecordAccess records an access event.
func (t *AccessTracker) RecordAccess(event AccessEvent) {
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	// Update object popularity
	t.objectAccess.Increment(event.Key)

	// Update group popularity
	t.groupAccess.Increment(event.GroupKey)

	// Update hourly access pattern
	hour := event.Timestamp.Hour()
	t.hourlyMu.RLock()
	t.hourlyAccess[hour].Increment(event.Key)
	t.hourlyMu.RUnlock()

	// Add to recent access buffer for sequential detection
	t.recentMu.Lock()
	t.recentAccess.Value = event
	t.recentAccess = t.recentAccess.Next()
	t.recentMu.Unlock()

	t.metrics.accessesRecorded.Inc()
}

// GetHotGroups returns the N most popular groups.
func (t *AccessTracker) GetHotGroups(n int) []iface.GroupKey {
	return t.groupAccess.TopN(n)
}

// GetHotObjects returns the N most popular objects.
func (t *AccessTracker) GetHotObjects(n int) []string {
	return t.objectAccess.TopN(n)
}

// GetGroupPopularity returns the popularity score for a group.
func (t *AccessTracker) GetGroupPopularity(gk iface.GroupKey) float64 {
	return t.groupAccess.Get(gk)
}

// GetObjectPopularity returns the popularity score for an object.
func (t *AccessTracker) GetObjectPopularity(key string) float64 {
	return t.objectAccess.Get(key)
}

// Decay manually triggers decay on all counters.
// This is primarily used for testing; normally decay happens automatically
// via the background decay loop.
func (t *AccessTracker) Decay() {
	t.objectAccess.Decay()
	t.groupAccess.Decay()
	t.hourlyMu.RLock()
	for _, counter := range t.hourlyAccess {
		counter.Decay()
	}
	t.hourlyMu.RUnlock()
}

// GetHourlyPattern returns the hot objects for a specific hour.
func (t *AccessTracker) GetHourlyPattern(hour int) []string {
	if hour < 0 || hour >= 24 {
		return nil
	}
	t.hourlyMu.RLock()
	defer t.hourlyMu.RUnlock()
	return t.hourlyAccess[hour].TopN(10)
}

// DetectSequentialAccess analyzes recent accesses for sequential patterns.
func (t *AccessTracker) DetectSequentialAccess() []SequentialPattern {
	t.recentMu.Lock()
	defer t.recentMu.Unlock()

	// Collect recent events
	var events []AccessEvent
	t.recentAccess.Do(func(v interface{}) {
		if v != nil {
			events = append(events, v.(AccessEvent))
		}
	})

	if len(events) < t.cfg.MinSequenceLen {
		return nil
	}

	// Sort by timestamp
	sort.Slice(events, func(i, j int) bool {
		return events[i].Timestamp.Before(events[j].Timestamp)
	})

	// Group events by group key
	byGroup := make(map[iface.GroupKey][]AccessEvent)
	for _, e := range events {
		byGroup[e.GroupKey] = append(byGroup[e.GroupKey], e)
	}

	var patterns []SequentialPattern

	// Detect sequences within each group
	for gk, groupEvents := range byGroup {
		if len(groupEvents) < t.cfg.MinSequenceLen {
			continue
		}

		// Look for consecutive accesses with short time gaps
		var currentSeq []AccessEvent
		for i, e := range groupEvents {
			if len(currentSeq) == 0 {
				currentSeq = append(currentSeq, e)
				continue
			}

			lastEvent := currentSeq[len(currentSeq)-1]
			gap := e.Timestamp.Sub(lastEvent.Timestamp).Milliseconds()

			if gap <= t.cfg.SequenceThresholdMs {
				currentSeq = append(currentSeq, e)
			} else {
				// Gap too large, check if we have a sequence
				if len(currentSeq) >= t.cfg.MinSequenceLen {
					pattern := t.buildPattern(gk, currentSeq)
					if pattern != nil {
						patterns = append(patterns, *pattern)
						t.metrics.sequencesFound.Inc()
					}
				}
				currentSeq = []AccessEvent{e}
			}

			// Last event
			if i == len(groupEvents)-1 && len(currentSeq) >= t.cfg.MinSequenceLen {
				pattern := t.buildPattern(gk, currentSeq)
				if pattern != nil {
					patterns = append(patterns, *pattern)
					t.metrics.sequencesFound.Inc()
				}
			}
		}
	}

	return patterns
}

// buildPattern creates a SequentialPattern from a sequence of events.
func (t *AccessTracker) buildPattern(gk iface.GroupKey, events []AccessEvent) *SequentialPattern {
	if len(events) < 2 {
		return nil
	}

	keys := make([]string, len(events))
	for i, e := range events {
		keys[i] = e.Key
	}

	// Find common prefix
	prefix := commonPrefix(keys)

	// Determine direction (simple heuristic: lexicographic order)
	direction := 1
	if len(keys) >= 2 && keys[0] > keys[1] {
		direction = -1
	}

	return &SequentialPattern{
		Prefix:     prefix,
		GroupKey:   gk,
		Keys:       keys,
		Direction:  direction,
		LastAccess: events[len(events)-1].Timestamp,
	}
}

// commonPrefix finds the longest common prefix of strings.
func commonPrefix(strs []string) string {
	if len(strs) == 0 {
		return ""
	}
	if len(strs) == 1 {
		return strs[0]
	}

	prefix := strs[0]
	for _, s := range strs[1:] {
		for len(prefix) > 0 && !hasPrefix(s, prefix) {
			prefix = prefix[:len(prefix)-1]
		}
	}
	return prefix
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// decayLoop periodically applies decay to all counters.
func (t *AccessTracker) decayLoop() {
	defer t.wg.Done()

	ticker := time.NewTicker(t.cfg.DecayInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.stopCh:
			return
		case <-ticker.C:
			t.objectAccess.Decay()
			t.groupAccess.Decay()

			t.hourlyMu.Lock()
			for i := 0; i < 24; i++ {
				t.hourlyAccess[i].Decay()
			}
			t.hourlyMu.Unlock()

			t.metrics.decayCycles.Inc()
			t.updateMetrics()
		}
	}
}

// updateMetrics updates the gauge metrics.
func (t *AccessTracker) updateMetrics() {
	// Count hot items (above threshold of 1.0)
	hotGroups := 0
	hotObjects := 0

	t.groupAccess.mu.RLock()
	for _, v := range t.groupAccess.counts {
		if v > 1.0 {
			hotGroups++
		}
	}
	t.groupAccess.mu.RUnlock()

	t.objectAccess.mu.RLock()
	for _, v := range t.objectAccess.counts {
		if v > 1.0 {
			hotObjects++
		}
	}
	t.objectAccess.mu.RUnlock()

	t.metrics.hotGroupsCount.Set(float64(hotGroups))
	t.metrics.hotObjectsCount.Set(float64(hotObjects))
}

// Close stops the access tracker.
func (t *AccessTracker) Close() {
	close(t.stopCh)
	t.wg.Wait()
}

// Stats returns current tracker statistics.
type AccessTrackerStats struct {
	TotalObjects      int
	TotalGroups       int
	HotObjects        int
	HotGroups         int
	RecentAccessCount int
}

// Stats returns current statistics.
func (t *AccessTracker) Stats() AccessTrackerStats {
	t.objectAccess.mu.RLock()
	totalObjects := len(t.objectAccess.counts)
	hotObjects := 0
	for _, v := range t.objectAccess.counts {
		if v > 1.0 {
			hotObjects++
		}
	}
	t.objectAccess.mu.RUnlock()

	t.groupAccess.mu.RLock()
	totalGroups := len(t.groupAccess.counts)
	hotGroups := 0
	for _, v := range t.groupAccess.counts {
		if v > 1.0 {
			hotGroups++
		}
	}
	t.groupAccess.mu.RUnlock()

	t.recentMu.Lock()
	recentCount := 0
	t.recentAccess.Do(func(v interface{}) {
		if v != nil {
			recentCount++
		}
	})
	t.recentMu.Unlock()

	return AccessTrackerStats{
		TotalObjects:      totalObjects,
		TotalGroups:       totalGroups,
		HotObjects:        hotObjects,
		HotGroups:         hotGroups,
		RecentAccessCount: recentCount,
	}
}

// DecayingCounter implements a counter that decays over time.
// This allows recent accesses to have more weight than old ones.
type DecayingCounter[K comparable] struct {
	mu        sync.RWMutex
	counts    map[K]float64
	decayRate float64
}

// NewDecayingCounter creates a new decaying counter.
func NewDecayingCounter[K comparable](decayRate float64) *DecayingCounter[K] {
	return &DecayingCounter[K]{
		counts:    make(map[K]float64),
		decayRate: decayRate,
	}
}

// Increment increases the counter for a key by 1.
func (c *DecayingCounter[K]) Increment(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[key]++
}

// Add adds a value to the counter for a key.
func (c *DecayingCounter[K]) Add(key K, value float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[key] += value
}

// Get returns the current count for a key.
func (c *DecayingCounter[K]) Get(key K) float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.counts[key]
}

// Decay applies decay to all counters.
func (c *DecayingCounter[K]) Decay() {
	c.mu.Lock()
	defer c.mu.Unlock()

	threshold := 0.01 // Remove entries below this threshold
	for k, v := range c.counts {
		newVal := v * c.decayRate
		if newVal < threshold {
			delete(c.counts, k)
		} else {
			c.counts[k] = newVal
		}
	}
}

// TopN returns the top N keys by count.
func (c *DecayingCounter[K]) TopN(n int) []K {
	c.mu.RLock()
	defer c.mu.RUnlock()

	type kv struct {
		key   K
		count float64
	}

	// Collect all entries
	entries := make([]kv, 0, len(c.counts))
	for k, v := range c.counts {
		entries = append(entries, kv{k, v})
	}

	// Sort by count descending
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].count > entries[j].count
	})

	// Return top N
	if n > len(entries) {
		n = len(entries)
	}
	result := make([]K, n)
	for i := 0; i < n; i++ {
		result[i] = entries[i].key
	}
	return result
}

// Len returns the number of tracked keys.
func (c *DecayingCounter[K]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.counts)
}

// Clear removes all entries.
func (c *DecayingCounter[K]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts = make(map[K]float64)
}
