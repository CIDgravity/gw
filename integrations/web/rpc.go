package web

import (
	"context"
	"runtime"

	iface2 "github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
)

type RIBSRpc struct {
	ribs iface2.RIBS
}

func (rc *RIBSRpc) WalletInfo(ctx context.Context) (iface2.WalletInfo, error) {
	return rc.ribs.Wallet().WalletInfo()
}

func (rc *RIBSRpc) WalletMarketAdd(ctx context.Context, amt abi.TokenAmount) (cid.Cid, error) {
	return rc.ribs.Wallet().MarketAdd(ctx, amt)
}

func (rc *RIBSRpc) WalletMarketWithdraw(ctx context.Context, amt abi.TokenAmount) (cid.Cid, error) {
	return rc.ribs.Wallet().MarketWithdraw(ctx, amt)
}

func (rc *RIBSRpc) WalletWithdraw(ctx context.Context, amt abi.TokenAmount, to address.Address) (cid.Cid, error) {
	return rc.ribs.Wallet().Withdraw(ctx, amt, to)
}

func (rc *RIBSRpc) Groups(ctx context.Context) ([]iface2.GroupKey, error) {
	return rc.ribs.StorageDiag().Groups()
}

func (rc *RIBSRpc) FindCid(ctx context.Context, hash cid.Cid) ([]iface2.GroupKey, error) {
	return rc.ribs.Storage().FindHashes(ctx, hash.Hash())
}

func (rc *RIBSRpc) GroupMeta(ctx context.Context, group iface2.GroupKey) (iface2.GroupMeta, error) {
	return rc.ribs.StorageDiag().GroupMeta(group)
}

func (rc *RIBSRpc) GroupDeals(ctx context.Context, group iface2.GroupKey) ([]iface2.DealMeta, error) {
	return rc.ribs.DealDiag().GroupDeals(group)
}

func (rc *RIBSRpc) CrawlState(ctx context.Context) (iface2.CrawlState, error) {
	return rc.ribs.DealDiag().CrawlState(), nil
}

func (rc *RIBSRpc) CarUploadStats(ctx context.Context) (iface2.UploadStats, error) {
	//return rc.ribs.DealDiag().CarUploadStats(), nil
	return iface2.UploadStats{}, nil
}

func (rc *RIBSRpc) ReachableProviders(ctx context.Context) ([]iface2.ProviderMeta, error) {
	return rc.ribs.DealDiag().ReachableProviders(), nil
}

func (rc *RIBSRpc) ProviderInfo(ctx context.Context, id int64) (iface2.ProviderInfo, error) {
	return rc.ribs.DealDiag().ProviderInfo(id)
}

func (rc *RIBSRpc) DealSummary(ctx context.Context) (iface2.DealSummary, error) {
	return rc.ribs.DealDiag().DealSummary()
}

func (rc *RIBSRpc) RetrStats(ctx context.Context) (iface2.RetrStats, error) {
	return rc.ribs.DealDiag().RetrStats()
}

func (rc *RIBSRpc) StagingStats(ctx context.Context) (iface2.StagingStats, error) {
	return rc.ribs.DealDiag().StagingStats()
}

func (rc *RIBSRpc) TopIndexStats(ctx context.Context) (iface2.TopIndexStats, error) {
	return rc.ribs.StorageDiag().TopIndexStats(ctx)
}

func (rc *RIBSRpc) GroupIOStats(ctx context.Context) (iface2.GroupIOStats, error) {
	return rc.ribs.StorageDiag().GroupIOStats(), nil
}

func (rc *RIBSRpc) GetGroupStats(ctx context.Context) (*iface2.GroupStats, error) {
	return rc.ribs.StorageDiag().GetGroupStats()
}

func (rc *RIBSRpc) RuntimeStats(ctx context.Context) (runtime.MemStats, error) {
	var out runtime.MemStats
	runtime.ReadMemStats(&out)
	return out, nil
}

func (rc *RIBSRpc) RetrChecker() (iface2.RetrCheckerStats, error) {
	return rc.ribs.DealDiag().RetrChecker(), nil
}

func (rc *RIBSRpc) P2PNodes(ctx context.Context) (map[string]iface2.Libp2pInfo, error) {
	return rc.ribs.DealDiag().P2PNodes(ctx)
}

func (rc *RIBSRpc) WorkerStats(ctx context.Context) (iface2.WorkerStats, error) {
	return rc.ribs.StorageDiag().WorkerStats(), nil
}

func (rc *RIBSRpc) RetrievableDealCounts(ctx context.Context) ([]iface2.DealCountStats, error) {
	return rc.ribs.DealDiag().RetrievableDealCounts()
}

func (rc *RIBSRpc) SealedDealCounts(ctx context.Context) ([]iface2.DealCountStats, error) {
	return rc.ribs.DealDiag().SealedDealCounts()
}

func (rc *RIBSRpc) RepairQueue() (iface2.RepairQueueStats, error) {
	return rc.ribs.DealDiag().RepairQueue()
}

func (rc *RIBSRpc) RepairStats() (map[int]iface2.RepairJob, error) {
	return rc.ribs.DealDiag().RepairStats()
}

func MakeRPCServer(ctx context.Context, ribs iface2.RIBS) (*jsonrpc.RPCServer, jsonrpc.ClientCloser, error) {
	hnd := &RIBSRpc{ribs: ribs}

	fgw, closer, err := ribs.DealDiag().Filecoin(ctx)
	if err != nil {
		return nil, nil, err
	}

	sv := jsonrpc.NewServer()
	sv.Register("RIBS", hnd)
	sv.Register("Filecoin", fgw)

	return sv, closer, nil
}
