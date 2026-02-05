package rbdeal

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"time"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/go-state-types/abi"
	lotbig "github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/builtin"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/chain/actors"
	"github.com/filecoin-project/lotus/chain/actors/builtin/market"
	"github.com/filecoin-project/lotus/chain/types"
)

// balanceManager periodically checks and tops up wallet and market balances
func (r *ribs) balanceManager(ctx context.Context) {
	cfg := configuration.GetConfig()

	// If no faucet URL is configured, this manager only handles market balance top-ups
	hasFaucet := cfg.Balances.FaucetURL != ""

	ticker := time.NewTicker(cfg.Wallet.UpgradeInterval)
	defer ticker.Stop()

	// Run immediately on start
	r.checkAndTopUpBalances(ctx, cfg, hasFaucet)

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.close:
			return
		case <-ticker.C:
			r.checkAndTopUpBalances(ctx, cfg, hasFaucet)
		}
	}
}

func (r *ribs) checkAndTopUpBalances(ctx context.Context, cfg *configuration.Config, hasFaucet bool) {
	addr, err := r.wallet.GetDefault()
	if err != nil {
		log.Errorw("balance manager: failed to get default wallet", "error", err)
		return
	}

	gw, closer, err := client.NewGatewayRPCV1(ctx, r.lotusRPCAddr, nil)
	if err != nil {
		log.Errorw("balance manager: failed to connect to lotus gateway", "error", err)
		return
	}
	defer closer()

	// Get current wallet balance
	walletBalance, err := gw.WalletBalance(ctx, addr)
	if err != nil {
		log.Errorw("balance manager: failed to get wallet balance", "error", err)
		return
	}

	// Get current market balance
	marketBalance, err := gw.StateMarketBalance(ctx, addr, types.EmptyTSK)
	if err != nil {
		log.Errorw("balance manager: failed to get market balance", "error", err)
		return
	}

	// Get current datacap
	datacap, err := gw.StateVerifiedClientStatus(ctx, addr, types.EmptyTSK)
	if err != nil {
		log.Errorw("balance manager: failed to get datacap", "error", err)
		// Don't return - we can still check wallet and market balances
	}

	walletBalanceFil := filToFloat(walletBalance)
	marketBalanceFil := filToFloat(marketBalance.Escrow)
	datacapTiB := uint64(0)
	if datacap != nil {
		datacapTiB = uint64(datacap.Int64()) / humanize.TiByte
	}

	log.Debugw("balance manager: current balances",
		"wallet", walletBalanceFil,
		"market", marketBalanceFil,
		"datacapTiB", datacapTiB,
		"walletMin", cfg.Balances.WalletFilMin,
		"marketMin", cfg.Balances.MarketFilMin,
		"marketTarget", cfg.Balances.MarketFilTarget,
		"datacapMinTiB", cfg.Balances.DatacapTiBMin)

	// Record balance metrics
	metrics := GetBalanceMetrics()
	metrics.SetWalletBalance(walletBalanceFil)
	metrics.SetMarketBalance(marketBalanceFil)
	if datacap != nil {
		metrics.SetDatacapRemaining(float64(datacap.Int64()))
	}

	// Check if we need to request FIL from faucet
	if hasFaucet && walletBalanceFil < cfg.Balances.WalletFilMin {
		log.Infow("balance manager: wallet balance below threshold, requesting faucet top-up",
			"balance", walletBalanceFil,
			"threshold", cfg.Balances.WalletFilMin)

		if err := r.requestFaucetFil(cfg.Balances.FaucetURL, addr.String()); err != nil {
			log.Warnw("balance manager: faucet FIL request failed", "error", err)
			metrics.IncFaucetRequest("fil", false)
		} else {
			log.Info("balance manager: faucet FIL request submitted")
			metrics.IncFaucetRequest("fil", true)
		}
	}

	// Check if we need to request datacap from faucet
	if hasFaucet && datacap != nil && datacapTiB < uint64(cfg.Balances.DatacapTiBMin) {
		log.Infow("balance manager: datacap below threshold, requesting faucet top-up",
			"datacapTiB", datacapTiB,
			"thresholdTiB", cfg.Balances.DatacapTiBMin,
			"requestTiB", cfg.Balances.DatacapTiBRequest)

		if err := r.requestFaucetDatacap(cfg.Balances.FaucetURL, addr.String(), cfg.Balances.DatacapTiBRequest); err != nil {
			log.Warnw("balance manager: faucet datacap request failed", "error", err)
			metrics.IncFaucetRequest("datacap", false)
		} else {
			log.Info("balance manager: faucet datacap request submitted")
			metrics.IncFaucetRequest("datacap", true)
		}
	}

	// Check if we need to top up market balance
	if marketBalanceFil < cfg.Balances.MarketFilMin {
		// Calculate how much to add to reach target
		topUpAmount := cfg.Balances.MarketFilTarget - marketBalanceFil
		if topUpAmount <= 0 {
			topUpAmount = cfg.Balances.MarketFilTarget
		}

		// Make sure we have enough in wallet
		// We need topUpAmount plus some for gas
		requiredWalletBalance := topUpAmount + 0.0001 // 0.0001 FIL for gas buffer
		if walletBalanceFil < requiredWalletBalance {
			log.Warnw("balance manager: insufficient wallet balance to top up market",
				"walletBalance", walletBalanceFil,
				"required", requiredWalletBalance,
				"topUpAmount", topUpAmount)
			return
		}

		log.Infow("balance manager: market balance below minimum, topping up",
			"marketBalance", marketBalanceFil,
			"marketMin", cfg.Balances.MarketFilMin,
			"topUpAmount", topUpAmount,
			"marketTarget", cfg.Balances.MarketFilTarget)

		if err := r.addMarketFunds(ctx, gw, floatToFil(topUpAmount)); err != nil {
			log.Errorw("balance manager: failed to add market funds", "error", err)
		} else {
			log.Infow("balance manager: market funds top-up message sent", "amount", topUpAmount)
			metrics.IncMarketTopup(topUpAmount)
		}
	}
}

// requestFaucetFil requests FIL from the faucet
func (r *ribs) requestFaucetFil(faucetURL, addr string) error {
	url := fmt.Sprintf("%s/fil?wallet=%s", faucetURL, addr)
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("faucet returned %s: %s", resp.Status, string(body))
	}

	var result struct {
		Success bool   `json:"success"`
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}
	if !result.Success {
		msg := result.Error
		if msg == "" {
			msg = result.Message
		}
		return fmt.Errorf("faucet error: %s", msg)
	}
	return nil
}

// requestFaucetDatacap requests datacap from the faucet
func (r *ribs) requestFaucetDatacap(faucetURL, addr string, tibs int) error {
	url := fmt.Sprintf("%s/datacap?wallet=%s&tibs=%d", faucetURL, addr, tibs)
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("faucet returned %s: %s", resp.Status, string(body))
	}

	var result struct {
		Success bool   `json:"success"`
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}
	if !result.Success {
		msg := result.Error
		if msg == "" {
			msg = result.Message
		}
		return fmt.Errorf("faucet error: %s", msg)
	}
	return nil
}

// addMarketFunds sends a message to add funds to the market actor
func (r *ribs) addMarketFunds(ctx context.Context, gw api.Gateway, amount abi.TokenAmount) error {
	r.msgSendLk.Lock()
	defer r.msgSendLk.Unlock()

	addr, err := r.wallet.GetDefault()
	if err != nil {
		return fmt.Errorf("get default wallet: %w", err)
	}

	params, err := actors.SerializeParams(&addr)
	if err != nil {
		return fmt.Errorf("serialize params: %w", err)
	}

	msg := &types.Message{
		To:     market.Address,
		From:   addr,
		Value:  amount,
		Method: builtin.MethodsMarket.AddBalance,
		Params: params,
	}

	// Estimate gas
	msg, err = gw.GasEstimateMessageGas(ctx, msg, nil, types.EmptyTSK)
	if err != nil {
		return fmt.Errorf("estimate gas: %w", err)
	}

	// Sign the message
	sig, err := r.wallet.WalletSign(ctx, addr, msg.Cid().Bytes(), api.MsgMeta{
		Type: api.MTChainMsg,
	})
	if err != nil {
		return fmt.Errorf("sign message: %w", err)
	}

	signedMsg := &types.SignedMessage{
		Message:   *msg,
		Signature: *sig,
	}

	// Push to mpool
	_, err = gw.MpoolPush(ctx, signedMsg)
	if err != nil {
		return fmt.Errorf("push message: %w", err)
	}

	return nil
}

// filToFloat converts abi.TokenAmount to float64 FIL
func filToFloat(amount abi.TokenAmount) float64 {
	// 1 FIL = 10^18 attoFIL
	attoFil := amount.Int
	fil := new(big.Float).SetInt(attoFil)
	divisor := new(big.Float).SetInt64(1e18)
	result, _ := new(big.Float).Quo(fil, divisor).Float64()
	return result
}

// floatToFil converts float64 FIL to abi.TokenAmount
func floatToFil(fil float64) abi.TokenAmount {
	// 1 FIL = 10^18 attoFIL
	filFloat := new(big.Float).SetFloat64(fil)
	multiplier := new(big.Float).SetInt64(1e18)
	attoFilFloat := new(big.Float).Mul(filFloat, multiplier)
	attoFilInt, _ := attoFilFloat.Int(nil)
	return abi.TokenAmount{Int: attoFilInt}
}

// Ensure lotbig is used to avoid unused import
var _ = lotbig.Zero
