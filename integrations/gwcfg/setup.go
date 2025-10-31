package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/charmbracelet/huh"
)

func resolveWalletPath(opts Opts) string {
	walletPath := opts.walletLocation
	if walletPath == "" || walletPath == "default" {
		home, _ := os.UserHomeDir()
		walletPath = filepath.Join(home, ".ribswallet")
	}
	return walletPath
}

func ensureLocalWallet(walletPath string) (string, error) {
	_, addr, err := EnsureWalletExists(walletPath)
	if err != nil {
		return "", err
	}
	return addr.String(), nil
}

func maybeInitializeOnChain(ctx context.Context, opts Opts, addr string) error {
	exists, err := WalletExistsOnChain(ctx, opts.lotusGateway, addr)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	fund := true
	if err := huh.NewForm(
		huh.NewGroup(
			huh.NewConfirm().
				Title("Initialize wallet on-chain?").
				Description(fmt.Sprintf("Wallet %s is not visible on-chain yet.\nYour wallet must appear on-chain to receive DataCap, which is required to use the gateway.\nIf you have a faucet configured (%s), the wizard can request a small amount of FIL to make the address visible and will wait until it appears on-chain.", addr, opts.faucetUrl)).
				Affirmative("Initialize now").
				Negative("Skip").
				Value(&fund),
		),
	).Run(); err != nil {
		return err
	}
	if fund {
		if err := EnsureWalletOnChain(ctx, opts.lotusGateway, opts.faucetUrl, addr, "", opts.walletTimeout); err != nil {
			return err
		}
	}
	return nil
}

func setCIDGravityToken(keys []groupedEnvKey, walletPath string, env map[string]string) error {
	for _, k := range keys {
		if k.Var == "CIDGRAVITY_API_TOKEN" {
			val, err := handleCIDGravityTokenInput(walletPath, env[k.Var])
			if err != nil {
				return err
			}
			env[k.Var] = val
			break
		}
	}
	return nil
}

func setRibsData(keys []groupedEnvKey, env map[string]string) error {
	for _, k := range keys {
		if k.Section == "RIBS" && k.Var == "RIBS_DATA" {
			val := k.DefaultValue
			comment := envComment(k.Var)
			field := huh.NewInput().
				Title(k.Var).
				Value(&val).
				Placeholder(k.DefaultValue)
			if comment != "" {
				field = field.Description(comment)
			}
			if err := huh.NewForm(huh.NewGroup(field)).Run(); err != nil {
				return err
			}
			env[k.Var] = val
		}
	}
	return nil
}

func setDealsConfig(keys []groupedEnvKey, env map[string]string) error {
	for _, k := range keys {
		if k.Section == "Deals" {
			val := k.DefaultValue
			comment := envComment(k.Var)
			field := huh.NewInput().
				Title(k.Var).
				Value(&val).
				Placeholder(k.DefaultValue)
			if comment != "" {
				field = field.Description(comment)
			}
			if err := huh.NewForm(huh.NewGroup(field)).Run(); err != nil {
				return err
			}
			env[k.Var] = val
		}
	}
	return nil
}

func setExternalConfig(keys []groupedEnvKey, env map[string]string) error {
	var extType string
	extOpts := []huh.Option[string]{
		huh.NewOption("LocalWeb", "localweb"),
		huh.NewOption("S3", "s3"),
	}
	if err := huh.NewForm(
		huh.NewGroup(
			huh.NewSelect[string]().Title("Upload config type").Options(extOpts...).Value(&extType),
		),
	).Run(); err != nil {
		return err
	}

	switch extType {
	case "s3":
		for _, k := range keys {
			if k.Section == "Upload:S3" {
				val := k.DefaultValue
				field := huh.NewInput().Title(k.Var).Value(&val).Placeholder(k.DefaultValue)
				if err := huh.NewForm(huh.NewGroup(field)).Run(); err != nil {
					return err
				}
				env[k.Var] = val
			}
		}
	case "localweb":
		for _, k := range keys {
			if k.Section == "Upload:LocalWeb" {
				val := k.DefaultValue
				field := huh.NewInput().Title(k.Var).Value(&val).Placeholder(k.DefaultValue)
				if err := huh.NewForm(huh.NewGroup(field)).Run(); err != nil {
					return err
				}
				env[k.Var] = val
			}
		}
	}
	return nil
}

func saveConfig(envPath string, env map[string]string) error {
	if err := saveEnv(envPath, env, envComment); err != nil {
		return err
	}
	fmt.Printf("Initial configuration saved to %s\n", envPath)
	return nil
}

func initialSetupWizard(envPath string, keys []groupedEnvKey, opts Opts) error {
	ctx := context.Background()

	walletPath := resolveWalletPath(opts)
	addr, err := ensureLocalWallet(walletPath)
	if err != nil {
		return err
	}
	if err := maybeInitializeOnChain(ctx, opts, addr); err != nil {
		return err
	}

	env := map[string]string{}

	if err := setCIDGravityToken(keys, walletPath, env); err != nil {
		return err
	}

	if err := setRibsData(keys, env); err != nil {
		return err
	}

	if err := setDealsConfig(keys, env); err != nil {
		return err
	}

	if err := setExternalConfig(keys, env); err != nil {
		return err
	}

	return saveConfig(envPath, env)
}
