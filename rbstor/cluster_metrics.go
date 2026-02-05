package rbstor

import (
	"sync"
	"time"

	"github.com/CIDgravity/filecoin-gateway/iface"
)

// ClusterMetrics collects and stores metrics for cluster monitoring
type ClusterMetrics struct {
	mu sync.RWMutex

	// Request counters
	totalReads      int64
	totalWrites     int64
	totalErrors     int64
	activeReads     int64
	activeWrites    int64
	activeMultipart int64

	// Byte counters
	totalReadBytes  int64
	totalWriteBytes int64

	// Time series data (last 10 minutes at 10-second intervals = 60 points)
	timestamps     []int64
	readCounts     []int64
	writeCounts    []int64
	errorCounts    []int64
	readLatencies  [][]float64 // each entry is latencies for that interval
	writeLatencies [][]float64
	readBytes      []int64
	writeBytes     []int64

	// Per-interval counters (reset each interval)
	intervalReads          int64
	intervalWrites         int64
	intervalErrors         int64
	intervalReadLatencies  []float64
	intervalWriteLatencies []float64
	intervalReadBytes      int64
	intervalWriteBytes     int64

	// Events
	events []iface.ClusterEvent

	// Last collection time
	lastCollect time.Time
	startTime   time.Time
}

const (
	maxDataPoints   = 60 // 10 minutes of data
	collectInterval = 10 * time.Second
	maxEvents       = 100
)

var globalClusterMetrics = &ClusterMetrics{
	startTime:   time.Now(),
	lastCollect: time.Now(),
}

// GetClusterMetrics returns the global cluster metrics instance
func GetClusterMetrics() *ClusterMetrics {
	return globalClusterMetrics
}

// RecordRead records a read operation with bytes transferred
func (m *ClusterMetrics) RecordRead(latencyMs float64, bytes int64, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.totalReads++
	m.intervalReads++
	m.intervalReadLatencies = append(m.intervalReadLatencies, latencyMs)
	m.totalReadBytes += bytes
	m.intervalReadBytes += bytes
	if err != nil {
		m.totalErrors++
		m.intervalErrors++
	}
}

// RecordWrite records a write operation with bytes transferred
func (m *ClusterMetrics) RecordWrite(latencyMs float64, bytes int64, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.totalWrites++
	m.totalWriteBytes += bytes
	m.intervalWriteBytes += bytes
	m.intervalWrites++
	m.intervalWriteLatencies = append(m.intervalWriteLatencies, latencyMs)
	if err != nil {
		m.totalErrors++
		m.intervalErrors++
	}
}

// StartRead marks a read as in-flight
func (m *ClusterMetrics) StartRead() {
	m.mu.Lock()
	m.activeReads++
	m.mu.Unlock()
}

// EndRead marks a read as complete
func (m *ClusterMetrics) EndRead() {
	m.mu.Lock()
	m.activeReads--
	m.mu.Unlock()
}

// StartWrite marks a write as in-flight
func (m *ClusterMetrics) StartWrite() {
	m.mu.Lock()
	m.activeWrites++
	m.mu.Unlock()
}

// EndWrite marks a write as complete
func (m *ClusterMetrics) EndWrite() {
	m.mu.Lock()
	m.activeWrites--
	m.mu.Unlock()
}

// AddEvent adds a cluster event
func (m *ClusterMetrics) AddEvent(eventType, message, nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	event := iface.ClusterEvent{
		Timestamp: time.Now().Unix(),
		Type:      eventType,
		Message:   message,
		NodeID:    nodeID,
	}

	m.events = append([]iface.ClusterEvent{event}, m.events...)
	if len(m.events) > maxEvents {
		m.events = m.events[:maxEvents]
	}
}

// collectInterval moves current interval data to time series
func (m *ClusterMetrics) collect() {
	now := time.Now()

	// Add current interval to time series
	m.timestamps = append(m.timestamps, now.Unix())
	m.readCounts = append(m.readCounts, m.intervalReads)
	m.writeCounts = append(m.writeCounts, m.intervalWrites)
	m.errorCounts = append(m.errorCounts, m.intervalErrors)
	m.readLatencies = append(m.readLatencies, m.intervalReadLatencies)
	m.writeLatencies = append(m.writeLatencies, m.intervalWriteLatencies)
	m.readBytes = append(m.readBytes, m.intervalReadBytes)
	m.writeBytes = append(m.writeBytes, m.intervalWriteBytes)

	// Trim to max data points
	if len(m.timestamps) > maxDataPoints {
		m.timestamps = m.timestamps[1:]
		m.readCounts = m.readCounts[1:]
		m.writeCounts = m.writeCounts[1:]
		m.errorCounts = m.errorCounts[1:]
		m.readLatencies = m.readLatencies[1:]
		m.writeLatencies = m.writeLatencies[1:]
		m.readBytes = m.readBytes[1:]
		m.writeBytes = m.writeBytes[1:]
	}

	// Reset interval counters
	m.intervalReads = 0
	m.intervalWrites = 0
	m.intervalErrors = 0
	m.intervalReadLatencies = nil
	m.intervalWriteLatencies = nil
	m.intervalReadBytes = 0
	m.intervalWriteBytes = 0
	m.lastCollect = now
}

// maybeCollect collects if enough time has passed
func (m *ClusterMetrics) maybeCollect() {
	if time.Since(m.lastCollect) >= collectInterval {
		m.collect()
	}
}

// GetThroughputHistory returns historical throughput data
func (m *ClusterMetrics) GetThroughputHistory(duration string) iface.ThroughputHistory {
	m.mu.Lock()
	m.maybeCollect()
	m.mu.Unlock()

	m.mu.RLock()
	defer m.mu.RUnlock()

	// Calculate how many points to return based on duration
	points := len(m.timestamps)
	if points == 0 {
		// Return at least current state
		now := time.Now().Unix()
		return iface.ThroughputHistory{
			Timestamps: []int64{now},
			Total:      []float64{float64(m.intervalReads+m.intervalWrites) / collectInterval.Seconds()},
			Reads:      []float64{float64(m.intervalReads) / collectInterval.Seconds()},
			Writes:     []float64{float64(m.intervalWrites) / collectInterval.Seconds()},
			ByProxy:    make(map[string][]float64),
		}
	}

	timestamps := make([]int64, points)
	total := make([]float64, points)
	reads := make([]float64, points)
	writes := make([]float64, points)

	for i := 0; i < points; i++ {
		timestamps[i] = m.timestamps[i]
		// Convert counts to requests per second
		reads[i] = float64(m.readCounts[i]) / collectInterval.Seconds()
		writes[i] = float64(m.writeCounts[i]) / collectInterval.Seconds()
		total[i] = reads[i] + writes[i]
	}

	return iface.ThroughputHistory{
		Timestamps: timestamps,
		Total:      total,
		Reads:      reads,
		Writes:     writes,
		ByProxy:    make(map[string][]float64),
	}
}

// GetIOThroughputHistory returns historical I/O bytes throughput data
func (m *ClusterMetrics) GetIOThroughputHistory(duration string) iface.IOThroughputHistory {
	m.mu.Lock()
	m.maybeCollect()
	m.mu.Unlock()

	m.mu.RLock()
	defer m.mu.RUnlock()

	points := len(m.timestamps)
	if points == 0 {
		now := time.Now().Unix()
		return iface.IOThroughputHistory{
			Timestamps: []int64{now},
			ReadBytes:  []float64{float64(m.intervalReadBytes) / collectInterval.Seconds()},
			WriteBytes: []float64{float64(m.intervalWriteBytes) / collectInterval.Seconds()},
			TotalBytes: []float64{float64(m.intervalReadBytes+m.intervalWriteBytes) / collectInterval.Seconds()},
		}
	}

	timestamps := make([]int64, points)
	readBytes := make([]float64, points)
	writeBytes := make([]float64, points)
	totalBytes := make([]float64, points)

	for i := 0; i < points; i++ {
		timestamps[i] = m.timestamps[i]
		// Convert bytes to bytes per second
		readBytes[i] = float64(m.readBytes[i]) / collectInterval.Seconds()
		writeBytes[i] = float64(m.writeBytes[i]) / collectInterval.Seconds()
		totalBytes[i] = readBytes[i] + writeBytes[i]
	}

	return iface.IOThroughputHistory{
		Timestamps: timestamps,
		ReadBytes:  readBytes,
		WriteBytes: writeBytes,
		TotalBytes: totalBytes,
	}
}

// percentile calculates the p-th percentile of a sorted slice
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

// GetLatencyDistribution returns latency percentiles
func (m *ClusterMetrics) GetLatencyDistribution(duration string) iface.LatencyDistribution {
	m.mu.Lock()
	m.maybeCollect()
	m.mu.Unlock()

	m.mu.RLock()
	defer m.mu.RUnlock()

	points := len(m.timestamps)
	if points == 0 {
		return iface.LatencyDistribution{
			Timestamps: []int64{time.Now().Unix()},
			P50:        []float64{0},
			P95:        []float64{0},
			P99:        []float64{0},
			ByOperation: make(map[string]struct {
				P50 []float64 `json:"p50"`
				P95 []float64 `json:"p95"`
				P99 []float64 `json:"p99"`
			}),
		}
	}

	timestamps := make([]int64, points)
	p50 := make([]float64, points)
	p95 := make([]float64, points)
	p99 := make([]float64, points)

	for i := 0; i < points; i++ {
		timestamps[i] = m.timestamps[i]

		// Combine read and write latencies for this interval
		all := append([]float64{}, m.readLatencies[i]...)
		all = append(all, m.writeLatencies[i]...)

		if len(all) > 0 {
			// Sort for percentile calculation
			sorted := make([]float64, len(all))
			copy(sorted, all)
			for j := 0; j < len(sorted)-1; j++ {
				for k := j + 1; k < len(sorted); k++ {
					if sorted[j] > sorted[k] {
						sorted[j], sorted[k] = sorted[k], sorted[j]
					}
				}
			}
			p50[i] = percentile(sorted, 0.50)
			p95[i] = percentile(sorted, 0.95)
			p99[i] = percentile(sorted, 0.99)
		}
	}

	return iface.LatencyDistribution{
		Timestamps: timestamps,
		P50:        p50,
		P95:        p95,
		P99:        p99,
		ByOperation: make(map[string]struct {
			P50 []float64 `json:"p50"`
			P95 []float64 `json:"p95"`
			P99 []float64 `json:"p99"`
		}),
	}
}

// GetErrorRates returns error statistics
func (m *ClusterMetrics) GetErrorRates() iface.ErrorRates {
	m.mu.RLock()
	defer m.mu.RUnlock()

	totalOps := m.totalReads + m.totalWrites
	errorRate := float64(0)
	if totalOps > 0 {
		errorRate = float64(m.totalErrors) / float64(totalOps) * 100
	}

	trend := "stable"
	if errorRate > 5 {
		trend = "degrading"
	} else if errorRate < 1 && totalOps > 100 {
		trend = "improving"
	}

	return iface.ErrorRates{
		Nodes: map[string]iface.NodeErrorStats{
			"local": {
				ErrorRate: errorRate,
				ByType:    make(map[string]int),
				Trend:     trend,
			},
		},
	}
}

// GetActiveRequests returns current in-flight requests
func (m *ClusterMetrics) GetActiveRequests() iface.ActiveRequests {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return iface.ActiveRequests{
		Total:     int(m.activeReads + m.activeWrites + m.activeMultipart),
		Reads:     int(m.activeReads),
		Writes:    int(m.activeWrites),
		Multipart: int(m.activeMultipart),
		ByProxy:   make(map[string]int),
		ByStorage: make(map[string]int),
	}
}

// GetClusterEvents returns recent cluster events
func (m *ClusterMetrics) GetClusterEvents(limit int) []iface.ClusterEvent {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if limit <= 0 || limit > len(m.events) {
		limit = len(m.events)
	}

	result := make([]iface.ClusterEvent, limit)
	copy(result, m.events[:limit])
	return result
}
