package rbdeal

import (
	"context"
	"time"

	iface2 "github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/CIDgravity/filecoin-gateway/server/metrics"
	"github.com/libp2p/go-libp2p/core/host"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
)

func (r *ribs) DealDiag() iface2.RIBSDiag {
	return r
}

func (r *ribs) CrawlState() iface2.CrawlState {
	cs := r.crawlState.Load()
	if cs == nil {
		return iface2.CrawlState{
			State: "disabled",
		}
	}

	return *cs
}

func (r *ribs) ReachableProviders() []iface2.ProviderMeta {
	out := r.db.ReachableProviders()
	now := time.Now()
	for i := range out {
		r.applyProviderCooldown(&out[i], now)
	}
	return out
}

func (r *ribs) DealLoopStats() iface2.DealLoopStats {
	r.dealLoopStatsLk.Lock()
	defer r.dealLoopStatsLk.Unlock()
	return r.dealLoopStats
}

func (r *ribs) ProviderInfo(id int64) (iface2.ProviderInfo, error) {
	info, err := r.db.ProviderInfo(id)
	if err != nil {
		return iface2.ProviderInfo{}, err
	}
	r.applyProviderCooldown(&info.Meta, time.Now())
	return info, nil
}

func (r *ribs) DealSummary() (iface2.DealSummary, error) {
	return r.db.DealSummary()
}

func (r *ribs) GroupDeals(gk iface2.GroupKey) ([]iface2.DealMeta, error) {
	return r.db.GroupDeals(gk)
}

func (r *ribs) StagingStats() (iface2.StagingStats, error) {
	m := r.externalOffloader.GetMetrics()
	return iface2.StagingStats{
		UploadBytes:   int64(metrics.GetCounterValue(m.uploadedBytes)),
		UploadWaiting: int64(metrics.GetGaugeValue(m.uploadsWaiting)),
		UploadStarted: int64(metrics.GetCounterValue(m.uploadsStarted)),
		UploadDone:    int64(metrics.GetCounterValue(m.uploadsDone)),
		Staging:       int64(metrics.GetGaugeValue(m.staging)),
		UploadErr:     int64(metrics.GetCounterValue(m.uploadErr)),
		Redirects:     0,
		ReadReqs:      int64(metrics.GetCounterValue(m.readReqs)),
		ReadBytes:     int64(metrics.GetCounterValue(m.readBytes)),
	}, nil
}

func (r *ribs) Filecoin(ctx context.Context) (api.Gateway, jsonrpc.ClientCloser, error) {
	gw, closer, err := client.NewGatewayRPCV1(ctx, r.lotusRPCAddr, nil)
	if err != nil {
		panic(err)
	}

	return gw, closer, nil
}

func getLibP2PInfoForHost(h host.Host) iface2.Libp2pInfo {
	if h == nil {
		return iface2.Libp2pInfo{
			Listen: []string{},
			PeerID: "n/a",
		}
	}

	out := iface2.Libp2pInfo{
		PeerID: h.ID().String(),
		Peers:  len(h.Network().Peers()),
	}

	for _, ma := range h.Network().ListenAddresses() {
		out.Listen = append(out.Listen, ma.String())
	}

	return out
}

func (r *ribs) P2PNodes(ctx context.Context) (map[string]iface2.Libp2pInfo, error) {
	out := map[string]iface2.Libp2pInfo{}

	out["main"] = getLibP2PInfoForHost(r.host)
	out["crawl"] = getLibP2PInfoForHost(r.crawlHost)
	out["retrieval"] = getLibP2PInfoForHost(r.retrHost)

	return out, nil
}

func (r *ribs) RetrChecker() iface2.RetrCheckerStats {
	return iface2.RetrCheckerStats{
		ToDo:       int64(metrics.GetGaugeValue(r.retrCheckMetrics.todo)),
		Started:    int64(metrics.GetGaugeValue(r.retrCheckMetrics.started)),
		Success:    int64(metrics.GetGaugeValue(r.retrCheckMetrics.success)),
		Fail:       int64(metrics.GetGaugeValue(r.retrCheckMetrics.failed)),
		SuccessAll: int64(metrics.GetCounterValue(r.retrCheckMetrics.successAll)),
		FailAll:    int64(metrics.GetCounterValue(r.retrCheckMetrics.failedAll)),
	}
}

func (r *ribs) RetrievableDealCounts() ([]iface2.DealCountStats, error) {
	return r.db.GetRetrievableDealStats()
}

func (r *ribs) SealedDealCounts() ([]iface2.DealCountStats, error) {
	return r.db.GetSealedDealStats()
}

func (r *ribs) RepairQueue() (iface2.RepairQueueStats, error) {
	return r.db.GetRepairStats()
}

func (r *ribs) RepairStats() (map[int]iface2.RepairJob, error) {
	r.repairStatsLk.Lock()
	defer r.repairStatsLk.Unlock()

	out := map[int]iface2.RepairJob{}
	for k, v := range r.repairStats {
		out[k] = *v
	}

	return out, nil
}

// CIDGravityStatus checks the connection status to the CIDGravity service
func (r *ribs) CIDGravityStatus(ctx context.Context) iface2.CIDGravityStatus {
	status := r.cidg.CheckStatus(ctx)
	return iface2.CIDGravityStatus{
		Connected:       status.Connected,
		TokenValid:      status.TokenValid,
		Endpoint:        status.Endpoint,
		Error:           status.Error,
		LastCheck:       status.LastCheck,
		ResponseTimeMs:  status.ResponseTimeMs,
		TokenConfigured: status.TokenConfigured,
	}
}

// CacheStats returns L1/L2 cache statistics
func (r *ribs) CacheStats() iface2.CacheStats {
	return r.retrProv.CacheStats()
}
