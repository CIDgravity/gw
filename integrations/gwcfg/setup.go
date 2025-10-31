package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

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
		fmt.Println("Requesting faucet funds...")
		if err := FundWalletViaFaucet(opts.faucetUrl, addr); err != nil {
			return err
		}
		fmt.Println("✅ Faucet request successful. Waiting for wallet to become visible on-chain...")

		stop := startSpinner(fmt.Sprintf("Waiting for %s to appear on-chain", addr))
		err := WaitWalletAppearsOnChain(ctx, opts.lotusGateway, addr, opts.walletTimeout)
		stop()
		if err != nil {
			return err
		}
		fmt.Printf("\n✅ Wallet is now visible on-chain: %s\n", addr)
	}
	return nil
}

func setCIDGravityToken(keys []groupedEnvKey, walletPath string, env map[string]string, baseURL string) error {
	for _, k := range keys {
		if k.Var != "CIDGRAVITY_API_TOKEN" {
			continue
		}
		friendly := ""
		contactEmail := ""
		entityName := ""

		group := huh.NewGroup(
			huh.NewInput().Title("Account Friendly Name").Value(&friendly).Placeholder("my-gateway"),
			huh.NewInput().Title("Contact Email").Value(&contactEmail).Placeholder("you@example.com"),
			huh.NewInput().Title("Entity Name").Value(&entityName).Placeholder("Your Org / Project"),
		)
		if err := huh.NewForm(group).Run(); err != nil {
			return err
		}

		_, addr, err := EnsureWalletExists(walletPath)
		if err != nil {
			return err
		}

		client := NewCidGravity(baseURL)

		{
			stop := startSpinner("Contacting CIDGravity to get challenge...")
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			gc, gcErr := client.GetChallenge(ctx, addr.String())
			cancel()
			stop()
			if gcErr == nil {
				// Sign challenge
				sigHex, sigErr := signChallengeWithWallet(walletPath, gc.Challenge)
				if sigErr == nil {
					// Create account
					stop2 := startSpinner("Creating CIDGravity account and obtaining API token...")
					ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
					res, caErr := client.CreateAccount(ctx2, CreateAccountRequest{
						Challenge:     gc.Challenge,
						AddressID:     gc.Address,
						FriendlyName:  friendly,
						SignedMessage: sigHex,
						AddressInformation: CreateAccountAddressEntity{
							EntityName:   entityName,
							EntityType:   "company",
							ContactEmail: contactEmail,
						},
					})
					cancel2()
					stop2()
					if caErr == nil && res.Token != "" {
						fmt.Println("\n✅ Obtained CIDGravity API token via API.")
						env[k.Var] = res.Token
						return nil
					}
					if caErr != nil {
						fmt.Printf("\n❌ CIDGravity create-account failed: %v\n", caErr)
					} else {
						fmt.Println("\n❌ CIDGravity returned empty token")
					}
				} else {
					fmt.Printf("\n❌ Failed to sign challenge: %v\n", sigErr)
				}
			} else {
				fmt.Printf("\n❌ CIDGravity get-challenge failed: %v\n", gcErr)
			}
		}

		// Manual fallback
		fmt.Println("You can enter the token manually or paste a challenge to sign.")
		val, err := handleCIDGravityTokenInput(walletPath, env[k.Var])
		if err != nil {
			return err
		}
		env[k.Var] = val
		return nil
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

	if err := setCIDGravityToken(keys, walletPath, env, opts.cidgravityUrl); err != nil {
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

func startSpinner(message string) func() {
	stop := make(chan struct{})
	go func() {
		frames := []rune{'⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'}
		i := 0
		ticker := time.NewTicker(120 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				// Clear the spinner line
				fmt.Printf("\r%-80s\r", "")
				return
			case <-ticker.C:
				fmt.Printf("\r%c %s", frames[i%len(frames)], message)
				i++
			}
		}
	}()
	return func() { close(stop) }
}
