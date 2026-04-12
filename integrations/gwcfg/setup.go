package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/huh"
	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/chain/types"
)

var emailRe = regexp.MustCompile(`^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$`)

const (
	datacapRequestAmountTiB = 10
	faucetFilRequestAmount  = "0.000001"
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

func maybeInitializeOnChain(ctx context.Context, opts Opts, addr string) (bool, error) {
	exists, err := WalletExistsOnChain(ctx, opts.lotusGateway, addr)
	if err != nil {
		return false, err
	}
	if exists {
		return false, nil
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
		return false, err
	}
	if fund {
		fmt.Println("Requesting faucet funds...")
		if err := RequestFilViaFaucet(opts.faucetUrl, addr, faucetFilRequestAmount); err != nil {
			return false, err
		}
		fmt.Println("✅ Faucet request successful. Waiting for wallet to become visible on-chain...")

		stop := startSpinner(fmt.Sprintf("Waiting for %s to appear on-chain", addr))
		err := WaitWalletAppearsOnChain(ctx, opts.lotusGateway, addr, opts.walletTimeout)
		stop()
		if err != nil {
			return true, err
		}
		fmt.Printf("\n✅ Wallet is now visible on-chain: %s\n", addr)
		exists = true
		return true, nil
	}
	return false, nil
}

func maybeEnsureDatacap(ctx context.Context, opts Opts, addr string, filAlreadyRequested bool) error {
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

	if filAlreadyRequested {
		fmt.Println("Requesting datacap allocation...")
	} else {
		fmt.Println("Requesting datacap allocation and FIL top-up...")
	}

	// Request datacap and, if needed, a FIL top-up in parallel.
	type datacapResult struct {
		messageCID string
		err        error
	}
	type filResult struct {
		err error
	}

	datacapCh := make(chan datacapResult, 1)
	var filCh chan filResult

	go func() {
		messageCID, err := RequestDatacapViaFaucet(opts.faucetUrl, addr, datacapRequestAmountTiB)
		datacapCh <- datacapResult{messageCID: messageCID, err: err}
	}()

	if !filAlreadyRequested {
		filCh = make(chan filResult, 1)
		go func() {
			err := RequestFilViaFaucet(opts.faucetUrl, addr, faucetFilRequestAmount)
			filCh <- filResult{err: err}
		}()
	}

	// Wait for both results
	dcRes := <-datacapCh
	var filRes filResult
	if filCh != nil {
		filRes = <-filCh
	}

	// Report FIL faucet result (non-fatal)
	if filCh != nil && filRes.err != nil {
		fmt.Printf("⚠️  FIL faucet request failed (non-fatal): %v\n", filRes.err)
	} else if filCh != nil {
		fmt.Println("✅ FIL top-up request submitted.")
	}

	// Handle datacap result (fatal on error)
	if dcRes.err != nil {
		return dcRes.err
	}
	if dcRes.messageCID != "" {
		fmt.Printf("✅ Datacap request submitted (message CID: %s). Waiting for allocation to become visible...\n", dcRes.messageCID)
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

func collectCIDGravityAccountInfo() (friendly, contactEmail, entityName string, err error) {
	group := huh.NewGroup(
		huh.NewInput().
			Title("Account Friendly Name").
			Value(&friendly).
			Placeholder("my-gateway").
			Validate(func(val string) error {
				v := strings.TrimSpace(val)
				if v == "" {
					return fmt.Errorf("friendly name is required")
				}
				if len(v) > 100 {
					return fmt.Errorf("friendly name must be 100 characters or fewer")
				}
				return nil
			}),
		huh.NewInput().
			Title("Contact Email").
			Value(&contactEmail).
			Placeholder("you@example.com").
			Validate(func(val string) error {
				v := strings.TrimSpace(val)
				if v == "" {
					return fmt.Errorf("email is required")
				}
				if !emailRe.MatchString(v) {
					return fmt.Errorf("enter a valid email address")
				}
				return nil
			}),
		huh.NewInput().
			Title("Entity Name").
			Value(&entityName).
			Placeholder("Your Org / Project").
			Validate(func(val string) error {
				v := strings.TrimSpace(val)
				if v == "" {
					return fmt.Errorf("entity name is required")
				}
				if len(v) > 200 {
					return fmt.Errorf("entity name must be 200 characters or fewer")
				}
				return nil
			}),
	)
	if err = huh.NewForm(group).Run(); err != nil {
		return
	}
	friendly = strings.TrimSpace(friendly)
	contactEmail = strings.TrimSpace(contactEmail)
	entityName = strings.TrimSpace(entityName)
	return
}

func setCIDGravityToken(keys []groupedEnvKey, walletPath string, env map[string]string, opts Opts) error {
	k, ok := findKey("CIDGRAVITY_API_TOKEN", keys)
	if !ok {
		return fmt.Errorf("unable to configure the CIDGravity token")
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

	for attempts := 0; attempts < 3; attempts++ {
		friendly, contactEmail, entityName, err := collectCIDGravityAccountInfo()
		if err != nil {
			return err
		}

		stop := startSpinner("Contacting CIDGravity to get challenge...")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		gc, gcErr := cgClient.GetChallenge(ctx, idAddrStr)
		cancel()
		stop()
		if gcErr != nil {
			fmt.Printf("\n❌ CIDGravity get-challenge failed: %v\n", gcErr)
			if !promptRetryOrManual() {
				break
			}
			continue
		}

		sigHex, sigErr := signChallengeWithWallet(walletPath, gc.Challenge)
		if sigErr != nil {
			fmt.Printf("\n❌ Failed to sign challenge: %v\n", sigErr)
			if !promptRetryOrManual() {
				break
			}
			continue
		}

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

		if caErr != nil {
			fmt.Printf("\n❌ CIDGravity create-account failed: %v\n", caErr)
			if !promptRetryOrManual() {
				break
			}
			continue
		}
		if res.Token == "" {
			fmt.Println("\n❌ CIDGravity returned empty token")
			if !promptRetryOrManual() {
				break
			}
			continue
		}

		fmt.Println("\n✅ Obtained CIDGravity API token via API.")
		env[k.Var] = res.Token
		claimed := false
		message := fmt.Sprintf("Click this link to claim your account and manage CIDGravity settings:\n%s", res.URL)
		confirm := huh.NewConfirm().
			Title("Claim CIDGravity account").
			Description(message).
			Affirmative("I've claimed the account").
			Negative("Skip").
			Value(&claimed)
		if err := huh.NewForm(huh.NewGroup(confirm)).Run(); err != nil {
			return err
		}
		return nil
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

// promptRetryOrManual asks the user whether to retry with corrected input
// or fall through to the manual token entry flow. Returns true for retry.
func promptRetryOrManual() bool {
	var choice string
	sel := huh.NewSelect[string]().
		Title("What would you like to do?").
		Options(
			huh.NewOption("Retry with different details", "retry"),
			huh.NewOption("Enter API token manually", "manual"),
		).
		Value(&choice)
	if err := huh.NewForm(huh.NewGroup(sel)).Run(); err != nil {
		return false
	}
	return choice == "retry"
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

	pathKey, ok := findKey("EXTERNAL_LOCALWEB_PATH", keys)
	if !ok {
		return fmt.Errorf("unable to configure staging storage path")
	}

	portKey, ok := findKey("EXTERNAL_LOCALWEB_SERVER_PORT", keys)
	if !ok {
		return fmt.Errorf("unable to configure staging server port")
	}

	var urlVal string
	var pathVal string
	var portVal string
	mode := "autocert"
	pathDefault := pathKey.DefaultValue
	if pathDefault == "" {
		ribsDataPath := strings.TrimSpace(env["RIBS_DATA"])
		if ribsDataPath == "" {
			ribsDataPath = filepath.Join("~", ".ribsdata")
		}
		expanded, err := expandLocalPath(ribsDataPath)
		if err == nil && expanded != "" {
			ribsDataPath = expanded
		}
		pathDefault = filepath.Join(ribsDataPath, "cardata")
	}
	if portKey.DefaultValue != "" {
		portVal = portKey.DefaultValue
	} else {
		portVal = "8443"
	}

	if err := huh.NewForm(huh.NewGroup(
		huh.NewSelect[string]().
			Title("LocalWeb mode").
			Description("Choose whether the gateway terminates TLS itself or an external reverse proxy/ingress handles TLS and forwards to the gateway.").
			Options(
				huh.NewOption("Built-in autocert TLS on the gateway", "autocert"),
				huh.NewOption("Delegate TLS to a reverse proxy / ingress", "delegated"),
			).
			Value(&mode),
		huh.NewInput().Title(urlKey.Var).Value(&urlVal).Placeholder(urlKey.DefaultValue).Description("Public root URL for staged CAR downloads, for example https://example.com (no path component)"),
		huh.NewInput().Title(portKey.Var).Value(&portVal).Placeholder("8443").Description("Internal LocalWeb listen port. In delegated-TLS mode your reverse proxy should forward to 127.0.0.1:<port> on the gateway host."),
		huh.NewInput().Title(pathKey.Var).Value(&pathVal).Placeholder(pathDefault).Description("Local filesystem path for staging CAR files. The default is a persistent cardata directory under RIBS_DATA."),
	)).Run(); err != nil {
		return err
	}

	env[urlKey.Var] = urlVal
	if pathVal == "" {
		pathVal = pathDefault
	}
	expandedPath, err := expandLocalPath(pathVal)
	if err != nil {
		return err
	}
	env[pathKey.Var] = expandedPath

	if portVal == "" {
		portVal = "8443"
	}

	// Set sensible defaults for the built-in server
	env["EXTERNAL_LOCALWEB_BUILTIN_SERVER"] = "true"
	env["EXTERNAL_LOCALWEB_SERVER_PORT"] = portVal
	if mode == "delegated" {
		env["EXTERNAL_LOCALWEB_SERVER_TLS"] = "false"
	} else {
		env["EXTERNAL_LOCALWEB_SERVER_TLS"] = "true"
	}

	if err := maybeTestStagingEndpoint(env); err != nil {
		if errors.Is(err, errEditStaging) {
			return setStagingConfig(keys, env)
		}
		return err
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
	filRequested, err := maybeInitializeOnChain(ctx, opts, addr)
	if err != nil {
		return err
	}

	if err := maybeEnsureDatacap(ctx, opts, addr, filRequested); err != nil {
		return err
	}

	env := map[string]string{}

	if err := setCIDGravityToken(keys, walletPath, env, opts); err != nil {
		return err
	}

	if err := setRibsData(keys, env); err != nil {
		return err
	}

	for {
		if err := setStagingConfig(keys, env); err != nil {
			return err
		}

		verified, err := runValidator("Staging", env)
		if err != nil {
			return err
		}
		if verified {
			break
		}
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
