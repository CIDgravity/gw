package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/charmbracelet/huh"
	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/chain/types"
)

const (
	datacapRequestAmountTiB = 10
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
				Description(fmt.Sprintf("Wallet %s is not visible on-chain yet.\nYour wallet must appear on-chain to receive DataCap, which is required to use the gateway.\nIf you have a faucet configured, the wizard can request a small amount of FIL to make the address visible and will wait until it appears on-chain.", addr)).
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
		exists = true
	}
	return nil
}

func maybeEnsureDatacap(ctx context.Context, opts Opts, addr string) error {
	if opts.faucetUrl == "" {
		return nil
	}
	hasCap, err := WalletHasDatacap(ctx, opts.lotusGateway, addr, datacapRequestAmountTiB)
	if err != nil {
		return err
	}
	if hasCap {
		return nil
	}

	request := true
	if err := huh.NewForm(
		huh.NewGroup(
			huh.NewConfirm().
				Title("Request datacap allocation?").
				Description(fmt.Sprintf("Your wallet needs verified datacap to use the gateway.\nThe wizard can request %d TiB from the faucet and wait until it lands on-chain.", datacapRequestAmountTiB)).
				Affirmative("Request datacap").
				Negative("Skip").
				Value(&request),
		),
	).Run(); err != nil {
		return err
	}
	if !request {
		return nil
	}

	fmt.Println("Requesting datacap allocation...")
	messageCID, err := RequestDatacapViaFaucet(opts.faucetUrl, addr, datacapRequestAmountTiB)
	if err != nil {
		return err
	}
	if messageCID != "" {
		fmt.Printf("✅ Datacap request submitted (message CID: %s). Waiting for allocation to become visible...\n", messageCID)
	} else {
		fmt.Println("✅ Datacap request submitted. Waiting for allocation to become visible...")
	}

	stop := startSpinner(fmt.Sprintf("Waiting for datacap to appear for %s", addr))
	err = WaitForDatacapAllocation(ctx, opts.lotusGateway, addr, humanize.TiByte*datacapRequestAmountTiB, opts.walletTimeout)
	stop()
	if err != nil {
		return err
	}
	fmt.Printf("\n✅ Wallet now has at least %d TiB of datacap\n", datacapRequestAmountTiB)
	return nil
}

func setCIDGravityToken(keys []groupedEnvKey, walletPath string, env map[string]string, opts Opts) error {
	k, ok := findKey("CIDGRAVITY_API_TOKEN", keys)
	if !ok {
		return fmt.Errorf("unable to configure the CIDGravity token")
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

	gapi, closer, err := client.NewGatewayRPCV1(context.Background(), opts.lotusGateway, nil)
	if err != nil {
		return fmt.Errorf("connect lotus: %w", err)
	}
	idAddr, err := gapi.StateLookupID(context.Background(), addr, types.EmptyTSK)
	closer()
	if err != nil {
		return fmt.Errorf("lookup id address: %w", err)
	}
	idAddrStr := idAddr.String()

	cgClient := NewCidGravity(opts.cidgravityUrl)

	{
		stop := startSpinner("Contacting CIDGravity to get challenge...")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		gc, gcErr := cgClient.GetChallenge(ctx, idAddrStr)
		cancel()
		stop()
		if gcErr == nil {
			// Sign challenge
			sigHex, sigErr := signChallengeWithWallet(walletPath, gc.Challenge)
			if sigErr == nil {
				// Create account
				stop2 := startSpinner("Creating CIDGravity account and obtaining API token...")
				ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
				res, caErr := cgClient.CreateAccount(ctx2, CreateAccountRequest{
					Challenge:     gc.Challenge,
					AddressID:     idAddrStr,
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
			return configureRibsDiskSpace(env)
		}
	}
	return nil
}

const (
	ribsDiskGroupSizeGB = 64
	minRibsDiskGB       = 128
)

func configureRibsDiskSpace(env map[string]string) error {
	var diskInput string

	for {
		field := huh.NewInput().
			Title("Disk space for RIBS data (GB)").
			Description(fmt.Sprintf("Minimum %dGB; roughly %dGB per local group.", minRibsDiskGB, ribsDiskGroupSizeGB)).
			Value(&diskInput).
			Placeholder("128GB").
			Validate(func(val string) error {
				bytes, err := humanize.ParseBytes(val)
				if err != nil {
					return err
				}
				gb := bytes / 1e9
				if gb < minRibsDiskGB {
					return fmt.Errorf("Disk space must be at least %dGB.\n", minRibsDiskGB)
				}
				return err
			})
		if err := huh.NewForm(huh.NewGroup(field)).Run(); err != nil {
			return err
		}

		bytes, err := humanize.ParseBytes(diskInput)
		if err != nil {
			fmt.Println(err)
			continue
		}
		gb := bytes / 1e9
		groupCount := (gb + ribsDiskGroupSizeGB - 1) / ribsDiskGroupSizeGB
		env["RIBS_MAX_LOCAL_GROUP_COUNT"] = strconv.Itoa(int(groupCount))
		return nil
	}
}

func setStagingConfig(keys []groupedEnvKey, env map[string]string) error {
	urlKey, ok := findKey("EXTERNAL_LOCALWEB_URL", keys)
	if !ok {
		return fmt.Errorf("unable to configure staging storage")
	}

	var urlVal string
	err := huh.NewForm(huh.NewGroup(
		huh.NewInput().Title(urlKey.Var).Value(&urlVal).Placeholder(urlKey.DefaultValue).Description(envComment(urlKey.Var)),
	)).Run()
	if err != nil {
		return err
	}

	env[urlKey.Var] = urlVal
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

	if err := maybeEnsureDatacap(ctx, opts, addr); err != nil {
		return err
	}

	env := map[string]string{}

	if err := setCIDGravityToken(keys, walletPath, env, opts); err != nil {
		return err
	}

	if err := setRibsData(keys, env); err != nil {
		return err
	}

	if err := setStagingConfig(keys, env); err != nil {
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

func findKey(name string, keys []groupedEnvKey) (groupedEnvKey, bool) {
	for _, k := range keys {
		if k.Var == name {
			return k, true
		}
	}
	return groupedEnvKey{}, false
}
