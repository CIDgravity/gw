package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/CIDgravity/filecoin-gateway/rbdeal"
	"github.com/CIDgravity/filecoin-gateway/ributil"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/chain/types"
)

const (
	FaucetAPI         = "http://localhost:7788/fil"
	WaitWalletTimeout = 60 * time.Second
	WaitWalletPoll    = 2 * time.Second
)

func EnsureWalletExists(walletPath string) (*ributil.LocalWallet, address.Address, error) {
	wallet, addr, err := rbdeal.OpenOrCreateWallet(walletPath)
	if err != nil {
		return nil, address.Undef, fmt.Errorf("could not open/create wallet: %w", err)
	}
	return wallet, addr, nil
}

func WalletExistsOnChain(ctx context.Context, lotusAPIAddr, addrStr string) (bool, error) {
	addr, err := address.NewFromString(addrStr)
	if err != nil {
		return false, fmt.Errorf("invalid address: %w", err)
	}
	gapi, closer, err := client.NewGatewayRPCV1(ctx, lotusAPIAddr, nil)
	if err != nil {
		return false, fmt.Errorf("connect lotus: %w", err)
	}
	defer closer()
	_, err = gapi.StateLookupID(ctx, addr, types.EmptyTSK)
	if err != nil {
		if err.Error() == "actor not found" || errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("lookupid: %w", err)
	}
	return true, nil
}

func FundWalletViaFaucet(addr string, amount string) error {
	q := fmt.Sprintf("%s?wallet=%s", FaucetAPI, addr)
	if amount != "" {
		q += "&fil=" + amount
	}
	res, err := http.Get(q)
	if err != nil {
		return fmt.Errorf("http faucet: %w", err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 {
		return fmt.Errorf("faucet non-200: %s: %s", res.Status, string(b))
	}
	var fr struct {
		Success bool   `json:"success"`
		Message string `json:"message"`
		Error   string `json:"error"`
	}
	_ = json.Unmarshal(b, &fr)
	if !fr.Success {
		return fmt.Errorf("faucet: %s", fr.Error)
	}
	return nil
}

func WaitWalletAppearsOnChain(ctx context.Context, lotusAPIAddr, addr string) error {
	deadline := time.Now().Add(WaitWalletTimeout)
	for {
		exists, err := WalletExistsOnChain(ctx, lotusAPIAddr, addr)
		if err != nil {
			return err
		}
		if exists {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for wallet %s to appear on-chain", addr)
		}
		time.Sleep(WaitWalletPoll)
	}
}

func EnsureWalletOnChain(ctx context.Context, lotusAPIAddr, addr string, amount string) error {
	exists, err := WalletExistsOnChain(ctx, lotusAPIAddr, addr)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	if err := FundWalletViaFaucet(addr, amount); err != nil {
		return fmt.Errorf("could not fund via faucet: %w", err)
	}
	return WaitWalletAppearsOnChain(ctx, lotusAPIAddr, addr)
}
