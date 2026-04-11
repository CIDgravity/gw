package rbdeal

import (
	"context"
	"strings"
	"time"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	"github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/chain/types"
	"golang.org/x/xerrors"

	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
)

func isTransientChainVisibilityError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "actor not found") || strings.Contains(errStr, "execution reverted")
}

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
		if !isTransientChainVisibilityError(err) {
			return iface.WalletInfo{}, xerrors.Errorf("get verified client status: %w", err)
		}
		log.Warnw("wallet datacap not yet visible on chain", "addr", addr, "error", err)
		dc = nil
	}

	id, err := gw.StateLookupID(ctx, addr, types.EmptyTSK)
	idAddr := addr.String()
	if err != nil {
		if !isTransientChainVisibilityError(err) {
			return iface.WalletInfo{}, xerrors.Errorf("get address id: %w", err)
		}
		log.Warnw("wallet id address not yet visible on chain", "addr", addr, "error", err)
	} else {
		idAddr = id.String()
	}

	wi := iface.WalletInfo{
		Addr:                  addr.String(),
		IDAddr:                idAddr,
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

// BalanceManagerInfo returns the current state and configuration of the balance manager
func (r *ribs) BalanceManagerInfo(ctx context.Context) (iface.BalanceManagerInfo, error) {
	r.balanceManagerLk.Lock()
	defer r.balanceManagerLk.Unlock()

	cfg := configuration.GetConfig()

	// Use cached info if fresh enough (5 seconds)
	if r.cachedBalanceManagerInfo != nil && time.Since(r.lastBalanceManagerInfoUpdate) < 5*time.Second {
		return *r.cachedBalanceManagerInfo, nil
	}

	addr, err := r.wallet.GetDefault()
	if err != nil {
		return iface.BalanceManagerInfo{}, xerrors.Errorf("get default wallet: %w", err)
	}

	gw, closer, err := client.NewGatewayRPCV1(ctx, r.lotusRPCAddr, nil)
	if err != nil {
		return iface.BalanceManagerInfo{}, xerrors.Errorf("connect to gateway: %w", err)
	}
	defer closer()

	// Get current wallet balance
	walletBalance, err := gw.WalletBalance(ctx, addr)
	if err != nil {
		return iface.BalanceManagerInfo{}, xerrors.Errorf("get wallet balance: %w", err)
	}

	// Get current market balance
	marketBalance, err := gw.StateMarketBalance(ctx, addr, types.EmptyTSK)
	if err != nil {
		return iface.BalanceManagerInfo{}, xerrors.Errorf("get market balance: %w", err)
	}

	// Get current datacap
	datacap, err := gw.StateVerifiedClientStatus(ctx, addr, types.EmptyTSK)
	if err != nil {
		log.Warnw("failed to get datacap for balance manager info", "error", err)
		// Don't return error, datacap might not be allocated
	}

	walletBalanceFil := filToFloat(walletBalance)
	marketBalanceFil := filToFloat(marketBalance.Escrow)
	datacapTiB := float64(0)
	if datacap != nil {
		datacapTiB = float64(datacap.Int64()) / float64(humanize.TiByte)
	}

	hasFaucet := cfg.Balances.FaucetURL != ""

	info := iface.BalanceManagerInfo{
		FaucetEnabled:         hasFaucet,
		WalletBalanceFil:      walletBalanceFil,
		MarketBalanceFil:      marketBalanceFil,
		DatacapTiB:            datacapTiB,
		FaucetFilThreshold:    cfg.Balances.WalletFilMin,
		MarketBalanceMin:      cfg.Balances.MarketFilMin,
		MarketBalanceTarget:   cfg.Balances.MarketFilTarget,
		DatacapThresholdTiB:   float64(cfg.Balances.DatacapTiBMin),
		WalletBelowThreshold:  walletBalanceFil < cfg.Balances.WalletFilMin,
		MarketBelowThreshold:  marketBalanceFil < cfg.Balances.MarketFilMin,
		DatacapBelowThreshold: datacapTiB < float64(cfg.Balances.DatacapTiBMin),
	}

	// Add timestamps for last actions
	if !r.lastFaucetFilRequest.IsZero() {
		info.LastFaucetFilRequest = r.lastFaucetFilRequest.Unix()
	}
	if !r.lastFaucetDatacapRequest.IsZero() {
		info.LastFaucetDatacapRequest = r.lastFaucetDatacapRequest.Unix()
	}
	if !r.lastMarketTopUp.IsZero() {
		info.LastMarketTopUp = r.lastMarketTopUp.Unix()
	}

	r.cachedBalanceManagerInfo = &info
	r.lastBalanceManagerInfoUpdate = time.Now()

	return info, nil
}

// RequestFaucetFil manually requests FIL from the faucet
func (r *ribs) RequestFaucetFil(ctx context.Context) error {
	cfg := configuration.GetConfig()
	if cfg.Balances.FaucetURL == "" {
		return xerrors.Errorf("faucet URL not configured")
	}

	addr, err := r.wallet.GetDefault()
	if err != nil {
		return xerrors.Errorf("get default wallet: %w", err)
	}

	if err := r.requestFaucetFil(cfg.Balances.FaucetURL, addr.String()); err != nil {
		return xerrors.Errorf("request faucet FIL: %w", err)
	}

	r.balanceManagerLk.Lock()
	r.lastFaucetFilRequest = time.Now()
	r.cachedBalanceManagerInfo = nil // Invalidate cache
	r.balanceManagerLk.Unlock()

	return nil
}

// RequestFaucetDatacap manually requests datacap from the faucet
func (r *ribs) RequestFaucetDatacap(ctx context.Context) error {
	cfg := configuration.GetConfig()
	if cfg.Balances.FaucetURL == "" {
		return xerrors.Errorf("faucet URL not configured")
	}

	addr, err := r.wallet.GetDefault()
	if err != nil {
		return xerrors.Errorf("get default wallet: %w", err)
	}

	if err := r.requestFaucetDatacap(cfg.Balances.FaucetURL, addr.String(), cfg.Balances.DatacapTiBRequest); err != nil {
		return xerrors.Errorf("request faucet datacap: %w", err)
	}

	r.balanceManagerLk.Lock()
	r.lastFaucetDatacapRequest = time.Now()
	r.cachedBalanceManagerInfo = nil // Invalidate cache
	r.balanceManagerLk.Unlock()

	return nil
}

// TopUpMarketBalance manually tops up the market balance from wallet
func (r *ribs) TopUpMarketBalance(ctx context.Context) error {
	cfg := configuration.GetConfig()

	addr, err := r.wallet.GetDefault()
	if err != nil {
		return xerrors.Errorf("get default wallet: %w", err)
	}

	gw, closer, err := client.NewGatewayRPCV1(ctx, r.lotusRPCAddr, nil)
	if err != nil {
		return xerrors.Errorf("connect to gateway: %w", err)
	}
	defer closer()

	// Get current balances
	walletBalance, err := gw.WalletBalance(ctx, addr)
	if err != nil {
		return xerrors.Errorf("get wallet balance: %w", err)
	}

	marketBalance, err := gw.StateMarketBalance(ctx, addr, types.EmptyTSK)
	if err != nil {
		return xerrors.Errorf("get market balance: %w", err)
	}

	walletBalanceFil := filToFloat(walletBalance)
	marketBalanceFil := filToFloat(marketBalance.Escrow)

	// Calculate top-up amount
	topUpAmount := cfg.Balances.MarketFilTarget - marketBalanceFil
	if topUpAmount <= 0 {
		topUpAmount = cfg.Balances.MarketFilTarget
	}

	// Check if we have enough in wallet
	requiredWalletBalance := topUpAmount + 0.0001 // 0.0001 FIL for gas buffer
	if walletBalanceFil < requiredWalletBalance {
		return xerrors.Errorf("insufficient wallet balance: have %f FIL, need %f FIL", walletBalanceFil, requiredWalletBalance)
	}

	if err := r.addMarketFunds(ctx, gw, floatToFil(topUpAmount)); err != nil {
		return xerrors.Errorf("add market funds: %w", err)
	}

	r.balanceManagerLk.Lock()
	r.lastMarketTopUp = time.Now()
	r.cachedBalanceManagerInfo = nil // Invalidate cache
	r.balanceManagerLk.Unlock()

	return nil
}
