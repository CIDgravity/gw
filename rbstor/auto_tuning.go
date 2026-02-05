package rbstor

import (
	"runtime"
	"sync"
	"time"
)

// AutoTuner dynamically adjusts parallel write parameters based on
// system resources and observed performance.
type AutoTuner struct {
	mu sync.RWMutex

	// Current tuned values
	maxParallelGroups     int
	targetWritersPerGroup int

	// System constraints
	numCPU       int
	availableRAM int64 // bytes

	// Performance observations
	lastTuneTime  time.Time
	tuneInterval  time.Duration
	recentMetrics []tuneMetricSnapshot
	maxSnapshots  int
}

// tuneMetricSnapshot captures metrics at a point in time for analysis.
type tuneMetricSnapshot struct {
	timestamp         time.Time
	writeThroughput   float64 // bytes/sec
	avgWriteLatencyMs float64
	avgFlushLatencyMs float64
	activeGroups      int
	activeWriters     int32
	errorRate         float64
}

// TuningRecommendation contains suggested parameter adjustments.
type TuningRecommendation struct {
	MaxParallelGroups     int
	TargetWritersPerGroup int
	Reason                string
}

// NewAutoTuner creates an auto-tuner with default settings.
func NewAutoTuner() *AutoTuner {
	return &AutoTuner{
		maxParallelGroups:     4,
		targetWritersPerGroup: 2,
		numCPU:                runtime.NumCPU(),
		availableRAM:          estimateAvailableRAM(),
		tuneInterval:          30 * time.Second,
		maxSnapshots:          60, // 30 minutes of history at 30s intervals
	}
}

// estimateAvailableRAM returns a conservative estimate of available RAM.
func estimateAvailableRAM() int64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Use 50% of system memory as a conservative estimate
	// In practice, you'd want to query actual available memory
	return int64(m.Sys) / 2
}

// GetMaxParallelGroups returns the current recommended max parallel groups.
func (t *AutoTuner) GetMaxParallelGroups() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.maxParallelGroups
}

// GetTargetWritersPerGroup returns the target concurrent writers per group.
func (t *AutoTuner) GetTargetWritersPerGroup() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.targetWritersPerGroup
}

// RecordMetrics records current performance metrics for analysis.
func (t *AutoTuner) RecordMetrics(
	writeThroughput float64,
	avgWriteLatencyMs float64,
	avgFlushLatencyMs float64,
	activeGroups int,
	activeWriters int32,
	errorRate float64,
) {
	t.mu.Lock()
	defer t.mu.Unlock()

	snapshot := tuneMetricSnapshot{
		timestamp:         time.Now(),
		writeThroughput:   writeThroughput,
		avgWriteLatencyMs: avgWriteLatencyMs,
		avgFlushLatencyMs: avgFlushLatencyMs,
		activeGroups:      activeGroups,
		activeWriters:     activeWriters,
		errorRate:         errorRate,
	}

	t.recentMetrics = append(t.recentMetrics, snapshot)

	// Trim old snapshots
	if len(t.recentMetrics) > t.maxSnapshots {
		t.recentMetrics = t.recentMetrics[len(t.recentMetrics)-t.maxSnapshots:]
	}
}

// ShouldTune returns true if enough time has passed for a tuning check.
func (t *AutoTuner) ShouldTune() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return time.Since(t.lastTuneTime) >= t.tuneInterval
}

// Tune analyzes recent metrics and returns tuning recommendations.
func (t *AutoTuner) Tune() TuningRecommendation {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.lastTuneTime = time.Now()

	if len(t.recentMetrics) < 5 {
		return TuningRecommendation{
			MaxParallelGroups:     t.maxParallelGroups,
			TargetWritersPerGroup: t.targetWritersPerGroup,
			Reason:                "insufficient data",
		}
	}

	// Analyze recent metrics
	_ = t.averageThroughput() // Used for logging, kept for future dashboard integration
	avgLatency := t.averageWriteLatency()
	avgErrorRate := t.averageErrorRate()
	trend := t.throughputTrend()

	// Decision logic
	recommendation := TuningRecommendation{
		MaxParallelGroups:     t.maxParallelGroups,
		TargetWritersPerGroup: t.targetWritersPerGroup,
	}

	// High error rate - reduce parallelism
	if avgErrorRate > 0.05 {
		recommendation.MaxParallelGroups = max(1, t.maxParallelGroups-1)
		recommendation.Reason = "high error rate, reducing parallelism"
		t.applyRecommendation(recommendation)
		return recommendation
	}

	// High latency - reduce parallelism
	if avgLatency > 100 { // 100ms threshold
		recommendation.MaxParallelGroups = max(1, t.maxParallelGroups-1)
		recommendation.Reason = "high latency, reducing parallelism"
		t.applyRecommendation(recommendation)
		return recommendation
	}

	// Good performance with positive trend - consider increasing
	if avgErrorRate < 0.01 && avgLatency < 50 && trend > 0.1 {
		maxByRAM := t.maxGroupsByRAM()
		maxByCPU := t.maxGroupsByCPU()
		upperLimit := min(min(maxByRAM, maxByCPU), 16) // Hard cap at 16

		if t.maxParallelGroups < upperLimit {
			recommendation.MaxParallelGroups = min(t.maxParallelGroups+1, upperLimit)
			recommendation.Reason = "good performance, increasing parallelism"
			t.applyRecommendation(recommendation)
			return recommendation
		}
	}

	// Stable performance
	recommendation.Reason = "stable performance, no changes"
	return recommendation
}

// averageThroughput calculates average throughput from recent metrics.
func (t *AutoTuner) averageThroughput() float64 {
	if len(t.recentMetrics) == 0 {
		return 0
	}
	var sum float64
	for _, m := range t.recentMetrics {
		sum += m.writeThroughput
	}
	return sum / float64(len(t.recentMetrics))
}

// averageWriteLatency calculates average write latency from recent metrics.
func (t *AutoTuner) averageWriteLatency() float64 {
	if len(t.recentMetrics) == 0 {
		return 0
	}
	var sum float64
	for _, m := range t.recentMetrics {
		sum += m.avgWriteLatencyMs
	}
	return sum / float64(len(t.recentMetrics))
}

// averageErrorRate calculates average error rate from recent metrics.
func (t *AutoTuner) averageErrorRate() float64 {
	if len(t.recentMetrics) == 0 {
		return 0
	}
	var sum float64
	for _, m := range t.recentMetrics {
		sum += m.errorRate
	}
	return sum / float64(len(t.recentMetrics))
}

// throughputTrend returns the throughput trend (-1 to 1).
// Positive means increasing, negative means decreasing.
func (t *AutoTuner) throughputTrend() float64 {
	if len(t.recentMetrics) < 10 {
		return 0
	}

	// Compare first half to second half
	mid := len(t.recentMetrics) / 2
	var firstHalf, secondHalf float64

	for i := 0; i < mid; i++ {
		firstHalf += t.recentMetrics[i].writeThroughput
	}
	firstHalf /= float64(mid)

	for i := mid; i < len(t.recentMetrics); i++ {
		secondHalf += t.recentMetrics[i].writeThroughput
	}
	secondHalf /= float64(len(t.recentMetrics) - mid)

	if firstHalf == 0 {
		return 0
	}

	return (secondHalf - firstHalf) / firstHalf
}

// maxGroupsByRAM estimates max parallel groups based on available RAM.
// Each group needs ~500MB for buffers and indexes.
func (t *AutoTuner) maxGroupsByRAM() int {
	const groupRAMRequirement = 500 * 1024 * 1024 // 500MB per group
	maxGroups := int(t.availableRAM / groupRAMRequirement)
	if maxGroups < 1 {
		return 1
	}
	return maxGroups
}

// maxGroupsByCPU estimates max parallel groups based on CPU cores.
// Rule of thumb: 1 group per 2 cores.
func (t *AutoTuner) maxGroupsByCPU() int {
	maxGroups := t.numCPU / 2
	if maxGroups < 1 {
		return 1
	}
	return maxGroups
}

// applyRecommendation updates internal state with the recommendation.
func (t *AutoTuner) applyRecommendation(rec TuningRecommendation) {
	t.maxParallelGroups = rec.MaxParallelGroups
	t.targetWritersPerGroup = rec.TargetWritersPerGroup
}

// ComputeOptimalGroups calculates optimal parallel groups for the current system.
// This is a static calculation that doesn't require runtime metrics.
func ComputeOptimalGroups() int {
	numCPU := runtime.NumCPU()
	availableRAM := estimateAvailableRAM()

	// RAM-based limit: ~500MB per group
	const groupRAMRequirement = 500 * 1024 * 1024
	maxByRAM := int(availableRAM / groupRAMRequirement)

	// CPU-based limit: 1 group per 2 cores
	maxByCPU := numCPU / 2

	// Take the minimum, with bounds
	optimal := min(maxByRAM, maxByCPU)
	if optimal < 1 {
		optimal = 1
	}
	if optimal > 16 {
		optimal = 16
	}

	return optimal
}

// min returns the minimum of two integers.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// max returns the maximum of two integers.
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
