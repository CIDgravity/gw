package rbdeal

import (
	"context"
	"time"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	"github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/chain/types"
	"golang.org/x/xerrors"

	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
)

func (r *ribs) MarketWithdraw(ctx context.Context, amount abi.TokenAmount) (cid.Cid, error) {
	//TODO implement me
	panic("implement me")
}

func (r *ribs) Withdraw(ctx context.Context, amount abi.TokenAmount, to address.Address) (cid.Cid, error) {
	//TODO implement me
	panic("implement me")
}

func (r *ribs) WalletInfo() (iface.WalletInfo, error) {
	r.marketFundsLk.Lock()
	defer r.marketFundsLk.Unlock()

	cfg := configuration.GetConfig()
	if r.cachedWalletInfo != nil && time.Since(r.lastWalletInfoUpdate) < cfg.Wallet.UpgradeInterval {
		return *r.cachedWalletInfo, nil
	}

	addr, err := r.wallet.GetDefault()
	if err != nil {
		return iface.WalletInfo{}, xerrors.Errorf("get default wallet: %w", err)
	}

	ctx := context.TODO()

	gw, closer, err := client.NewGatewayRPCV1(ctx, r.lotusRPCAddr, nil)
	if err != nil {
		panic(err)
	}
	defer closer()

	b, err := gw.WalletBalance(ctx, addr)
	if err != nil {
		return iface.WalletInfo{}, xerrors.Errorf("get wallet balance: %w", err)
	}

	mb, err := gw.StateMarketBalance(ctx, addr, types.EmptyTSK)
	if err != nil {
		return iface.WalletInfo{}, xerrors.Errorf("get market balance: %w", err)
	}

	dc, err := gw.StateVerifiedClientStatus(ctx, addr, types.EmptyTSK)
	if err != nil {
		return iface.WalletInfo{}, xerrors.Errorf("get verified client status: %w", err)
	}

	id, err := gw.StateLookupID(ctx, addr, types.EmptyTSK)
	if err != nil {
		return iface.WalletInfo{}, xerrors.Errorf("get address id: %w", err)
	}

	wi := iface.WalletInfo{
		Addr:                  addr.String(),
		IDAddr:                id.String(),
		Balance:               types.FIL(b).Short(),
		MarketBalance:         types.FIL(mb.Escrow).Short(),
		MarketLocked:          types.FIL(mb.Locked).Short(),
		MarketBalanceDetailed: mb,
	}

	if dc != nil {
		wi.DataCap = types.SizeStr(*dc)
	}

	r.cachedWalletInfo = &wi
	r.lastWalletInfoUpdate = time.Now()

	return wi, nil
}
