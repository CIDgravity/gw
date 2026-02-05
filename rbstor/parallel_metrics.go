package rbstor

import (
	"sync/atomic"
	"time"
)

// ParallelWriteMetrics tracks performance metrics for parallel writes.
// All fields are atomic for thread-safe access.
type ParallelWriteMetrics struct {
	// Write metrics
	TotalWrites    atomic.Int64 // Total write operations
	ParallelWrites atomic.Int64 // Writes that used parallel mode
	LegacyWrites   atomic.Int64 // Writes that used legacy mode
	WriteErrors    atomic.Int64 // Write errors

	// Group selection metrics
	AffinityHits    atomic.Int64 // Times session affinity was used
	AffinityMisses  atomic.Int64 // Times session affinity failed
	PreferredHits   atomic.Int64 // Times preferred group was used
	WeightedSelects atomic.Int64 // Times weighted selection was used
	GroupCreations  atomic.Int64 // New groups created

	// Flush metrics
	TotalFlushes    atomic.Int64 // Total flush operations
	ParallelFlushes atomic.Int64 // Flushes that used parallel mode
	LegacyFlushes   atomic.Int64 // Flushes that used legacy mode
	FlushErrors     atomic.Int64 // Flush errors

	// Timing metrics (nanoseconds)
	TotalWriteTimeNs  atomic.Int64
	TotalFlushTimeNs  atomic.Int64
	TotalSelectTimeNs atomic.Int64

	// Throughput tracking
	BytesWritten  atomic.Int64
	BlocksWritten atomic.Int64
}

// Global metrics instance
var parallelMetrics = &ParallelWriteMetrics{}

// GetParallelWriteMetrics returns the global parallel write metrics.
func GetParallelWriteMetrics() *ParallelWriteMetrics {
	return parallelMetrics
}

// ParallelWriteStats provides a snapshot of parallel write statistics.
type ParallelWriteStats struct {
	// Counts
	TotalWrites    int64
	ParallelWrites int64
	LegacyWrites   int64
	WriteErrors    int64

	// Group selection
	AffinityHitRate  float64
	PreferredHitRate float64
	GroupCreations   int64

	// Flush stats
	TotalFlushes    int64
	ParallelFlushes int64
	LegacyFlushes   int64

	// Timing (milliseconds)
	AvgWriteTimeMs  float64
	AvgFlushTimeMs  float64
	AvgSelectTimeMs float64

	// Throughput
	BytesWritten   int64
	BlocksWritten  int64
	BytesPerSecond float64 // Requires external timing
}

// Stats returns a snapshot of current metrics.
func (m *ParallelWriteMetrics) Stats() ParallelWriteStats {
	totalWrites := m.TotalWrites.Load()
	affinityHits := m.AffinityHits.Load()
	affinityMisses := m.AffinityMisses.Load()
	preferredHits := m.PreferredHits.Load()
	weightedSelects := m.WeightedSelects.Load()

	var affinityHitRate, preferredHitRate float64
	totalSelects := affinityHits + affinityMisses + preferredHits + weightedSelects
	if totalSelects > 0 {
		affinityHitRate = float64(affinityHits) / float64(totalSelects)
		preferredHitRate = float64(preferredHits) / float64(totalSelects)
	}

	var avgWriteTimeMs, avgFlushTimeMs, avgSelectTimeMs float64
	if totalWrites > 0 {
		avgWriteTimeMs = float64(m.TotalWriteTimeNs.Load()) / float64(totalWrites) / 1e6
	}
	totalFlushes := m.TotalFlushes.Load()
	if totalFlushes > 0 {
		avgFlushTimeMs = float64(m.TotalFlushTimeNs.Load()) / float64(totalFlushes) / 1e6
	}
	if totalSelects > 0 {
		avgSelectTimeMs = float64(m.TotalSelectTimeNs.Load()) / float64(totalSelects) / 1e6
	}

	return ParallelWriteStats{
		TotalWrites:      totalWrites,
		ParallelWrites:   m.ParallelWrites.Load(),
		LegacyWrites:     m.LegacyWrites.Load(),
		WriteErrors:      m.WriteErrors.Load(),
		AffinityHitRate:  affinityHitRate,
		PreferredHitRate: preferredHitRate,
		GroupCreations:   m.GroupCreations.Load(),
		TotalFlushes:     totalFlushes,
		ParallelFlushes:  m.ParallelFlushes.Load(),
		LegacyFlushes:    m.LegacyFlushes.Load(),
		AvgWriteTimeMs:   avgWriteTimeMs,
		AvgFlushTimeMs:   avgFlushTimeMs,
		AvgSelectTimeMs:  avgSelectTimeMs,
		BytesWritten:     m.BytesWritten.Load(),
		BlocksWritten:    m.BlocksWritten.Load(),
	}
}

// Reset clears all metrics. Useful for testing.
func (m *ParallelWriteMetrics) Reset() {
	m.TotalWrites.Store(0)
	m.ParallelWrites.Store(0)
	m.LegacyWrites.Store(0)
	m.WriteErrors.Store(0)
	m.AffinityHits.Store(0)
	m.AffinityMisses.Store(0)
	m.PreferredHits.Store(0)
	m.WeightedSelects.Store(0)
	m.GroupCreations.Store(0)
	m.TotalFlushes.Store(0)
	m.ParallelFlushes.Store(0)
	m.LegacyFlushes.Store(0)
	m.FlushErrors.Store(0)
	m.TotalWriteTimeNs.Store(0)
	m.TotalFlushTimeNs.Store(0)
	m.TotalSelectTimeNs.Store(0)
	m.BytesWritten.Store(0)
	m.BlocksWritten.Store(0)
}

// RecordWrite records a write operation with timing.
func (m *ParallelWriteMetrics) RecordWrite(parallel bool, duration time.Duration, bytes int64, blocks int64, err error) {
	m.TotalWrites.Add(1)
	if parallel {
		m.ParallelWrites.Add(1)
	} else {
		m.LegacyWrites.Add(1)
	}
	if err != nil {
		m.WriteErrors.Add(1)
	}
	m.TotalWriteTimeNs.Add(duration.Nanoseconds())
	m.BytesWritten.Add(bytes)
	m.BlocksWritten.Add(blocks)
}

// RecordFlush records a flush operation with timing.
func (m *ParallelWriteMetrics) RecordFlush(parallel bool, duration time.Duration, err error) {
	m.TotalFlushes.Add(1)
	if parallel {
		m.ParallelFlushes.Add(1)
	} else {
		m.LegacyFlushes.Add(1)
	}
	if err != nil {
		m.FlushErrors.Add(1)
	}
	m.TotalFlushTimeNs.Add(duration.Nanoseconds())
}

// RecordGroupSelection records group selection metrics.
func (m *ParallelWriteMetrics) RecordGroupSelection(selectionType string, duration time.Duration) {
	switch selectionType {
	case "affinity_hit":
		m.AffinityHits.Add(1)
	case "affinity_miss":
		m.AffinityMisses.Add(1)
	case "preferred":
		m.PreferredHits.Add(1)
	case "weighted":
		m.WeightedSelects.Add(1)
	case "created":
		m.GroupCreations.Add(1)
	}
	m.TotalSelectTimeNs.Add(duration.Nanoseconds())
}
