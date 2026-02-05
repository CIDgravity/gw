package iface

import (
	"context"
	"io"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/ipfs/go-cid"
)

type RIBS interface {
	RBS

	Wallet() Wallet
	DealDiag() RIBSDiag

	MetaDB() MetadataDB

	io.Closer
}

type RIBSDiag interface {
	//CarUploadStats() UploadStats
	DealSummary() (DealSummary, error)
	GroupDeals(gk GroupKey) ([]DealMeta, error)

	ProviderInfo(id int64) (ProviderInfo, error)
	CrawlState() CrawlState
	ReachableProviders() []ProviderMeta

	RetrStats() (RetrStats, error)

	StagingStats() (StagingStats, error)

	Filecoin(context.Context) (api.Gateway, jsonrpc.ClientCloser, error)

	P2PNodes(ctx context.Context) (map[string]Libp2pInfo, error)

	RetrChecker() RetrCheckerStats

	RetrievableDealCounts() ([]DealCountStats, error)
	SealedDealCounts() ([]DealCountStats, error)

	RepairQueue() (RepairQueueStats, error)
	RepairStats() (map[int]RepairJob, error)

	// CIDGravityStatus checks the connection status to the CIDGravity service
	CIDGravityStatus(ctx context.Context) CIDGravityStatus

	// CacheStats returns L1/L2 cache statistics
	CacheStats() CacheStats
}

type RepairQueueStats struct {
	Total, Assigned int
}

type RepairJob struct {
	GroupKey GroupKey

	State RepairJobState

	FetchProgress, FetchSize int64
	FetchUrl                 string
}

type RepairJobState string

const (
	RepairJobStateFetching  RepairJobState = "fetching"
	RepairJobStateVerifying RepairJobState = "verifying"
	RepairJobStateImporting RepairJobState = "importing"
)

type DealCountStats struct {
	Count  int
	Groups int
}

type RetrCheckerStats struct {
	ToDo       int64
	Started    int64
	Success    int64
	Fail       int64
	SuccessAll int64
	FailAll    int64
}

type Libp2pInfo struct {
	PeerID string

	Listen []string

	Peers int
}

type StagingStats struct {
	UploadBytes, UploadWaiting, UploadStarted, UploadDone, Staging, UploadErr, Redirects, ReadReqs, ReadBytes int64
}

type RetrStats struct {
	Success, Bytes, Fail, CacheHit, CacheMiss, Active int64
	HTTPTries, HTTPSuccess, HTTPBytes                 int64
}

type UploadStats struct {
	ByGroup map[GroupKey]*GroupUploadStats

	LastTotalBytes int64
}

type GroupUploadStats struct {
	ActiveRequests int
	UploadBytes    int64
}

type DealMeta struct {
	UUID     string
	Provider int64

	Sealed, Failed, Rejected bool

	StartEpoch, EndEpoch, StartTime int64

	Status     string
	SealStatus string
	Error      string
	DealID     int64

	BytesRecv int64
	TxSize    int64
	PubCid    string

	RetrTTFBMs            int64
	RetrSuccess, RetrFail int64
	NoRecentSuccess       bool
}

type Wallet interface {
	WalletInfo() (WalletInfo, error)

	MarketWithdraw(ctx context.Context, amount abi.TokenAmount) (cid.Cid, error)

	Withdraw(ctx context.Context, amount abi.TokenAmount, to address.Address) (cid.Cid, error)

	// BalanceManagerInfo returns the current state and configuration of the balance manager
	BalanceManagerInfo(ctx context.Context) (BalanceManagerInfo, error)

	// RequestFaucetFil manually requests FIL from the faucet
	RequestFaucetFil(ctx context.Context) error

	// RequestFaucetDatacap manually requests datacap from the faucet
	RequestFaucetDatacap(ctx context.Context) error

	// TopUpMarketBalance manually tops up the market balance from wallet
	TopUpMarketBalance(ctx context.Context) error
}

type WalletInfo struct {
	Addr, IDAddr string

	DataCap string

	Balance       string
	MarketBalance string
	MarketLocked  string

	MarketBalanceDetailed api.MarketBalance
}

// BalanceManagerInfo contains the current state and configuration of the balance manager
type BalanceManagerInfo struct {
	// Whether the faucet is configured
	FaucetEnabled bool

	// Current balances (in FIL for wallet/market, TiB for datacap)
	WalletBalanceFil float64
	MarketBalanceFil float64
	DatacapTiB       float64

	// Thresholds (configured values)
	FaucetFilThreshold  float64 // Wallet FIL threshold for faucet top-up
	MarketBalanceMin    float64 // Market balance minimum
	MarketBalanceTarget float64 // Market balance target after top-up
	DatacapThresholdTiB float64 // Datacap threshold for faucet top-up

	// Status indicators
	WalletBelowThreshold  bool
	MarketBelowThreshold  bool
	DatacapBelowThreshold bool

	// Last action timestamps (unix seconds, 0 if never)
	LastFaucetFilRequest     int64
	LastFaucetDatacapRequest int64
	LastMarketTopUp          int64
}

type CrawlState struct {
	State string

	At, Reachable, Total int64
	Boost, BBswap, BHttp int64
}

type DealSummary struct {
	NonFailed, InProgress, Done, Failed int64

	TotalDataSize, TotalDealSize   int64
	StoredDataSize, StoredDealSize int64
}

type ProviderInfo struct {
	Meta        ProviderMeta
	RecentDeals []DealMeta
}

type ProviderMeta struct {
	ID     int64
	PingOk bool

	BoostDeals     bool
	BoosterHttp    bool
	BoosterBitswap bool

	IndexedSuccess int64
	IndexedFail    int64

	DealStarted  int64
	DealSuccess  int64
	DealFail     int64
	DealRejected int64

	MostRecentDealStart int64

	// price in fil/gib/epoch
	AskPrice         float64
	AskVerifiedPrice float64

	AskMinPieceSize float64
	AskMaxPieceSize float64

	RetrievDeals, UnretrievDeals int64
}

// Cluster Monitoring Types

// ClusterTopology represents the current cluster layout and health
type ClusterTopology struct {
	Proxies      []ProxyInfo       `json:"proxies"`
	StorageNodes []StorageNodeInfo `json:"storageNodes"`
	DataFlows    []DataFlowInfo    `json:"dataFlows"`
}

type ProxyInfo struct {
	ID                string   `json:"id"`
	Address           string   `json:"address"`
	Status            string   `json:"status"` // "healthy", "degraded", "unhealthy"
	RequestsPerSecond float64  `json:"requestsPerSecond"`
	ActiveConnections int      `json:"activeConnections"`
	BackendPool       []string `json:"backendPool"`
	LatencyMs         float64  `json:"latencyMs"`
	ErrorRate         float64  `json:"errorRate"`
}

type StorageNodeInfo struct {
	ID                string  `json:"id"`
	Address           string  `json:"address"`
	Status            string  `json:"status"`
	StorageUsed       uint64  `json:"storageUsed"`
	StorageTotal      uint64  `json:"storageTotal"`
	ObjectsStored     int64   `json:"objectsStored"`
	RequestsPerSecond float64 `json:"requestsPerSecond"`
	GroupsCount       int     `json:"groupsCount"`
	DealsCount        int     `json:"dealsCount"`
}

type DataFlowInfo struct {
	From      string  `json:"from"`
	To        string  `json:"to"`
	Rate      float64 `json:"rate"` // requests/sec
	Type      string  `json:"type"` // "read", "write", "multipart"
	LatencyMs float64 `json:"latencyMs"`
}

// ThroughputHistory represents historical request throughput
type ThroughputHistory struct {
	Timestamps []int64              `json:"timestamps"` // Unix timestamps
	Total      []float64            `json:"total"`      // Total requests per second
	Reads      []float64            `json:"reads"`      // Read requests per second
	Writes     []float64            `json:"writes"`     // Write requests per second
	ByProxy    map[string][]float64 `json:"byProxy"`    // Per-proxy breakdown
}

// IOThroughputHistory represents historical I/O bytes throughput
type IOThroughputHistory struct {
	Timestamps []int64   `json:"timestamps"` // Unix timestamps
	ReadBytes  []float64 `json:"readBytes"`  // Read bytes per second
	WriteBytes []float64 `json:"writeBytes"` // Write bytes per second
	TotalBytes []float64 `json:"totalBytes"` // Total bytes per second
}

// LatencyDistribution represents latency percentiles over time
type LatencyDistribution struct {
	Timestamps  []int64   `json:"timestamps"` // Unix timestamps
	P50         []float64 `json:"p50"`
	P95         []float64 `json:"p95"`
	P99         []float64 `json:"p99"`
	ByOperation map[string]struct {
		P50 []float64 `json:"p50"`
		P95 []float64 `json:"p95"`
		P99 []float64 `json:"p99"`
	} `json:"byOperation"`
}

// ErrorRates represents error statistics per node
type ErrorRates struct {
	Nodes map[string]NodeErrorStats `json:"nodes"`
}

type NodeErrorStats struct {
	ErrorRate float64        `json:"errorRate"` // percentage
	ByType    map[string]int `json:"byType"`    // 5xx, 4xx, timeout
	Trend     string         `json:"trend"`     // "improving", "stable", "degrading"
}

// ActiveRequests represents current in-flight requests
type ActiveRequests struct {
	Total     int            `json:"total"`
	Reads     int            `json:"reads"`
	Writes    int            `json:"writes"`
	Multipart int            `json:"multipart"`
	ByProxy   map[string]int `json:"byProxy"`
	ByStorage map[string]int `json:"byStorage"`
}

// ClusterEvent represents a cluster event
type ClusterEvent struct {
	Timestamp int64  `json:"timestamp"` // Unix timestamp
	Type      string `json:"type"`      // "health_change", "latency_alert", "node_join", etc.
	NodeID    string `json:"nodeId"`
	Message   string `json:"message"`
	Severity  string `json:"severity"` // "info", "warning", "error"
}

// CIDGravityStatus represents the connection status to CIDGravity service
type CIDGravityStatus struct {
	// Connected indicates if the API is reachable
	Connected bool `json:"connected"`
	// TokenValid indicates if the API token is valid (authenticated successfully)
	TokenValid bool `json:"tokenValid"`
	// Endpoint is the API endpoint being used
	Endpoint string `json:"endpoint"`
	// Error contains any error message if connection failed
	Error string `json:"error,omitempty"`
	// LastCheck is the unix timestamp of when this status was checked
	LastCheck int64 `json:"lastCheck"`
	// ResponseTimeMs is the API response time in milliseconds
	ResponseTimeMs int64 `json:"responseTimeMs"`
	// TokenConfigured indicates if a token is configured (non-empty)
	TokenConfigured bool `json:"tokenConfigured"`
}

// CacheStats represents combined L1/L2 cache statistics
type CacheStats struct {
	// L1 (ARC memory cache) stats
	L1Enabled  bool  `json:"l1Enabled"`
	L1Size     int64 `json:"l1Size"`     // Current size in bytes
	L1Capacity int64 `json:"l1Capacity"` // Maximum capacity in bytes
	L1Items    int   `json:"l1Items"`    // Number of items
	L1T1Size   int64 `json:"l1T1Size"`   // T1 (recent) list size
	L1T2Size   int64 `json:"l1T2Size"`   // T2 (frequent) list size
	L1B1Len    int   `json:"l1B1Len"`    // B1 ghost list length
	L1B2Len    int   `json:"l1B2Len"`    // B2 ghost list length
	L1P        int64 `json:"l1P"`        // Adaptive parameter p

	// L2 (SSD cache) stats
	L2Enabled       bool  `json:"l2Enabled"`
	L2Size          int64 `json:"l2Size"`          // Current size in bytes
	L2MaxSize       int64 `json:"l2MaxSize"`       // Maximum size in bytes
	L2Items         int   `json:"l2Items"`         // Number of items
	L2ProbationSize int64 `json:"l2ProbationSize"` // Probation segment size
	L2ProtectedSize int64 `json:"l2ProtectedSize"` // Protected segment size
	L2FreeSpace     int64 `json:"l2FreeSpace"`     // Free space available

	// Hit/miss counters (from retrieval metrics)
	Hits   int64 `json:"hits"`
	Misses int64 `json:"misses"`
}
