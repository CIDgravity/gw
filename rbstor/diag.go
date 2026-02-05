package rbstor

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/CIDgravity/filecoin-gateway/iface"
	"golang.org/x/xerrors"
)

// remoteNodeStats matches the NodeStats struct from web/ribsweb.go
type remoteNodeStats struct {
	NodeID            string  `json:"nodeId"`
	GroupsCount       int     `json:"groupsCount"`
	StorageUsed       uint64  `json:"storageUsed"`
	RequestsPerSecond float64 `json:"requestsPerSecond"`
}

func (r *rbs) StorageDiag() iface.RBSDiag {
	return r
}

func (r *rbs) Groups() ([]iface.GroupKey, error) {
	return r.db.Groups()
}

func (r *rbs) GroupMeta(gk iface.GroupKey) (iface.GroupMeta, error) {
	m, err := r.db.GroupMeta(gk)
	if err != nil {
		return iface.GroupMeta{}, xerrors.Errorf("get group meta: %w", err)
	}

	r.lk.Lock()
	g, ok := r.openGroups[gk]
	r.lk.Unlock()

	if ok {
		m.ReadBlocks = g.readBlocks.Load()
		m.ReadBytes = g.readSize.Load()
		m.WriteBlocks = g.writeBlocks.Load()
		m.WriteBytes = g.writeSize.Load()
	}

	return m, nil
}

func (r *rbs) GetGroupStats() (*iface.GroupStats, error) {
	gs, err := r.db.GetGroupStats()
	if err != nil {
		return nil, err
	}

	r.lk.Lock()
	gs.OpenGroups = len(r.openGroups)
	gs.OpenWritable = len(r.writableGroups)
	r.lk.Unlock()

	return gs, nil
}

func (r *rbs) GroupIOStats() iface.GroupIOStats {
	r.lk.Lock()
	defer r.lk.Unlock()

	// first update global counters
	for _, group := range r.openGroups {
		readBlocks := group.readBlocks.Load()
		readSize := group.readSize.Load()
		writeBlocks := group.writeBlocks.Load()
		writeSize := group.writeSize.Load()

		r.grpReadBlocks += readBlocks - group.readBlocksSnap
		r.grpReadSize += readSize - group.readSizeSnap
		r.grpWriteBlocks += writeBlocks - group.writeBlocksSnap
		r.grpWriteSize += writeSize - group.writeSizeSnap

		group.readBlocksSnap = readBlocks
		group.readSizeSnap = readSize
		group.writeBlocksSnap = writeBlocks
		group.writeSizeSnap = writeSize
	}

	// then return the global counters
	stats := iface.GroupIOStats{
		ReadBlocks:  r.grpReadBlocks,
		ReadBytes:   r.grpReadSize,
		WriteBlocks: r.grpWriteBlocks,
		WriteBytes:  r.grpWriteSize,
	}

	return stats
}

func (r *rbs) TopIndexStats(ctx context.Context) (iface.TopIndexStats, error) {
	s, err := r.index.EstimateSize(ctx)
	if err != nil {
		return iface.TopIndexStats{}, xerrors.Errorf("estimate size: %w", err)
	}

	return iface.TopIndexStats{
		Entries: s,
		Writes:  atomic.LoadInt64(&r.index.writes),
		Reads:   atomic.LoadInt64(&r.index.reads),
	}, nil
}

func (r *rbs) WorkerStats() iface.WorkerStats {
	return iface.WorkerStats{
		Available:  r.workersAvail.Load(),
		InFinalize: r.workersFinalizing.Load(),
		InCommP:    r.workersCommP.Load(),
		InReload:   r.workersFinDataReload.Load(),
		TaskQueue:  int64(len(r.tasks)),
		CommPBytes: globalCommpBytes.Load(),
	}
}

func (r *rbs) ParallelWriteStats() iface.ParallelWriteStats {
	stats := parallelMetrics.Stats()
	return iface.ParallelWriteStats{
		Enabled:          IsParallelWritesEnabled(),
		TotalWrites:      stats.TotalWrites,
		ParallelWrites:   stats.ParallelWrites,
		LegacyWrites:     stats.LegacyWrites,
		WriteErrors:      stats.WriteErrors,
		AffinityHitRate:  stats.AffinityHitRate,
		PreferredHitRate: stats.PreferredHitRate,
		GroupCreations:   stats.GroupCreations,
		TotalFlushes:     stats.TotalFlushes,
		ParallelFlushes:  stats.ParallelFlushes,
		LegacyFlushes:    stats.LegacyFlushes,
		AvgWriteTimeMs:   stats.AvgWriteTimeMs,
		AvgFlushTimeMs:   stats.AvgFlushTimeMs,
		AvgSelectTimeMs:  stats.AvgSelectTimeMs,
		BytesWritten:     stats.BytesWritten,
		BlocksWritten:    stats.BlocksWritten,
	}
}

func (r *rbs) LoadBalancerMetrics() iface.LoadBalancerMetrics {
	if r.loadBalancer == nil {
		return iface.LoadBalancerMetrics{}
	}
	m := r.loadBalancer.Metrics()
	return iface.LoadBalancerMetrics{
		WritableGroupCount: m.WritableGroupCount,
		TotalActiveWriters: m.TotalActiveWriters,
		SessionAffinities:  m.SessionAffinities,
	}
}

func (r *rbs) WritableGroups() []iface.WritableGroupInfo {
	r.lk.Lock()
	defer r.lk.Unlock()

	var groups []iface.WritableGroupInfo
	for key, group := range r.writableGroups {
		info := iface.WritableGroupInfo{
			GroupKey:       key,
			Blocks:         group.committedBlocks,
			Bytes:          group.committedSize,
			AvailableSpace: group.AvailableSpace(),
			ActiveWriters:  group.ActiveWriterCount(),
		}

		// Check if any session has affinity to this group
		if r.loadBalancer != nil {
			r.loadBalancer.sessionAffinityLk.RLock()
			for _, affinityGroup := range r.loadBalancer.sessionAffinity {
				if affinityGroup == key {
					info.HasAffinity = true
					break
				}
			}
			r.loadBalancer.sessionAffinityLk.RUnlock()
		}

		groups = append(groups, info)
	}

	return groups
}

// ClusterTopology returns the current cluster layout
// Reads FGW_BACKEND_NODES environment variable to discover cluster nodes
func (r *rbs) ClusterTopology() iface.ClusterTopology {
	nodesConfig := os.Getenv("FGW_BACKEND_NODES")
	selfNodeID := os.Getenv("FGW_NODE_ID")

	if nodesConfig == "" {
		return iface.ClusterTopology{
			Proxies:      []iface.ProxyInfo{},
			StorageNodes: []iface.StorageNodeInfo{},
			DataFlows:    []iface.DataFlowInfo{},
		}
	}

	// Get local stats for this node
	var localGroupCount int
	var localStorageUsed uint64
	if gs, err := r.GetGroupStats(); err == nil {
		localGroupCount = int(gs.GroupCount)
		localStorageUsed = uint64(gs.TotalDataSize)
	}

	// Get request throughput from metrics
	metrics := GetClusterMetrics()
	throughputHistory := metrics.GetThroughputHistory("5m")
	var localReqPerSec float64
	if len(throughputHistory.Total) > 0 {
		// Use the most recent value
		localReqPerSec = throughputHistory.Total[len(throughputHistory.Total)-1]
	}

	var storageNodes []iface.StorageNodeInfo

	// Parse FGW_BACKEND_NODES format: "node1:http://host1:port,node2:http://host2:port"
	for _, nodeSpec := range strings.Split(nodesConfig, ",") {
		parts := strings.SplitN(nodeSpec, ":", 2)
		if len(parts) != 2 {
			continue
		}
		nodeID := parts[0]
		nodeURL := parts[1]

		// Check node health
		healthy := false
		healthURL := nodeURL + "/healthz"
		client := &http.Client{Timeout: 2 * time.Second}
		resp, err := client.Get(healthURL)
		if err == nil {
			healthy = resp.StatusCode == http.StatusOK
			resp.Body.Close()
		}

		status := "unhealthy"
		if healthy {
			status = "healthy"
		}

		nodeInfo := iface.StorageNodeInfo{
			ID:      nodeID,
			Address: nodeURL,
			Status:  status,
		}

		// If this is the local node, add our stats
		if nodeID == selfNodeID {
			nodeInfo.GroupsCount = localGroupCount
			nodeInfo.StorageUsed = localStorageUsed
			nodeInfo.RequestsPerSecond = localReqPerSec
		} else if healthy {
			// Fetch stats from remote node via /api/stats endpoint
			// The web UI is served on port 9010 by default. Extract the host from
			// nodeURL and use the web UI port. nodeURL can be either the proxy port
			// (8078) or the internal S3 port (8079).
			statsURL := nodeURL
			if idx := strings.LastIndex(statsURL, ":"); idx != -1 {
				// Extract host (including scheme) up to the last colon, then append web UI port
				statsURL = statsURL[:idx] + ":9010"
			}
			statsURL += "/api/stats"
			statsResp, err := client.Get(statsURL)
			if err == nil && statsResp.StatusCode == http.StatusOK {
				var stats remoteNodeStats
				if err := json.NewDecoder(statsResp.Body).Decode(&stats); err == nil {
					nodeInfo.GroupsCount = stats.GroupsCount
					nodeInfo.StorageUsed = stats.StorageUsed
					nodeInfo.RequestsPerSecond = stats.RequestsPerSecond
				}
				statsResp.Body.Close()
			}
		}

		storageNodes = append(storageNodes, nodeInfo)
	}

	// Add self as proxy if we have FGW_NODE_ID set
	var proxies []iface.ProxyInfo
	if selfNodeID != "" {
		proxies = append(proxies, iface.ProxyInfo{
			ID:                selfNodeID,
			Address:           "localhost",
			Status:            "healthy",
			RequestsPerSecond: localReqPerSec,
		})
	}

	return iface.ClusterTopology{
		Proxies:      proxies,
		StorageNodes: storageNodes,
		DataFlows:    []iface.DataFlowInfo{},
	}
}

// RequestThroughput returns historical throughput data
func (r *rbs) RequestThroughput(duration string) iface.ThroughputHistory {
	return GetClusterMetrics().GetThroughputHistory(duration)
}

// IOThroughput returns historical I/O bytes throughput data
func (r *rbs) IOThroughput(duration string) iface.IOThroughputHistory {
	return GetClusterMetrics().GetIOThroughputHistory(duration)
}

// LatencyDistribution returns latency percentiles
func (r *rbs) LatencyDistribution(duration string) iface.LatencyDistribution {
	return GetClusterMetrics().GetLatencyDistribution(duration)
}

// ErrorRates returns error statistics per node
func (r *rbs) ErrorRates() iface.ErrorRates {
	return GetClusterMetrics().GetErrorRates()
}

// ActiveRequests returns current in-flight requests
func (r *rbs) ActiveRequests() iface.ActiveRequests {
	return GetClusterMetrics().GetActiveRequests()
}

// ClusterEvents returns recent cluster events
func (r *rbs) ClusterEvents(limit int) []iface.ClusterEvent {
	return GetClusterMetrics().GetClusterEvents(limit)
}
