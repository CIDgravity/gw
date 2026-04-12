package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	"github.com/CIDgravity/filecoin-gateway/rbdeal"
	"github.com/charmbracelet/huh"
	"github.com/fatih/color"
	"github.com/filecoin-project/lotus/api"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/joho/godotenv"
)

var (
	cidgHexChallengeRe = regexp.MustCompile(`^[a-f0-9]+$`)
	cidgLotusSignRe    = regexp.MustCompile(`^lotus wallet sign f1[a-z0-9]+ [a-f0-9]+$`)
	cidgApiKeyRe       = regexp.MustCompile(`^f0[a-z0-9]+-[A-Za-z0-9_\-]+$`)
)

// ---------------------- meta‑data helpers ------------------------------- //

type groupedEnvKey struct {
	Var          string
	DefaultValue string
	Section      string
	Advanced     bool
}

func collectKeys() ([]groupedEnvKey, error) {
	var keys []groupedEnvKey
	cfgT := reflect.TypeOf(configuration.Config{})

	sectionFor := func(env string) (section string, advanced bool) {
		switch {
		// --- Top-level sections ---
		case strings.HasPrefix(env, "EXTERNAL_LOCALWEB_"):
			return "Staging", false

		case env == "CIDGRAVITY_API_TOKEN":
			return "CIDGravity", false

		case strings.HasPrefix(env, "RIBS_S3API_"):
			return "S3 API", false

		case strings.HasPrefix(env, "RIBS_WALLET_"):
			return "Wallet", false

		// Deals: scheduling knobs are advanced, rest is top-level
		case env == "RIBS_DEAL_CAN_SEND_COMMAND" || env == "RIBS_DEAL_CHECK_INTERVAL":
			return "Deals", true
		case strings.HasPrefix(env, "RIBS_DEAL_"):
			return "Deals", false

		// YugabyteDB: connection pool / timeout knobs are advanced
		case strings.HasPrefix(env, "RIBS_YUGABYTE_"):
			switch env {
			case "RIBS_YUGABYTE_CQL_FORCE_HOSTS",
				"RIBS_YUGABYTE_CQL_TIMEOUT",
				"RIBS_YUGABYTE_CQL_CONNECT_TIMEOUT",
				"RIBS_YUGABYTE_CQL_SOCKET_KEEPALIVE",
				"RIBS_YUGABYTE_SQL_MAX_OPEN_CONNS",
				"RIBS_YUGABYTE_SQL_MAX_IDLE_CONNS",
				"RIBS_YUGABYTE_SQL_CONN_MAX_LIFETIME_MINS",
				"RIBS_YUGABYTE_SQL_CONN_MAX_IDLE_TIME_MINS":
				return "Database Tuning", true
			default:
				return "YugabyteDB", false
			}

		// --- Advanced sections (specific prefixes before RIBS_ catch-all) ---
		case strings.HasPrefix(env, "CIDGRAVITY_"):
			return "CIDGravity", true

		case strings.HasPrefix(env, "RIBS_BALANCES_"):
			return "Balances", true

		case strings.HasPrefix(env, "RIBS_S3_CQL_"):
			return "Database Tuning", true

		case strings.HasPrefix(env, "RIBS_GC_"):
			return "Garbage Collection", true

		case strings.HasPrefix(env, "RIBS_REPAIR_"):
			return "Replication & Repair", true

		// Parallelism
		case env == "RIBS_ENABLE_PARALLEL_WRITES" || env == "RIBS_MAX_PARALLEL_GROUPS" ||
			env == "RIBS_SPACE_RESERVATION_TIMEOUT" || env == "RIBS_DRAIN_TIMEOUT":
			return "Parallelism", true

		// Replication
		case env == "RIBS_MINIMUM_RETRIEVABLE_COUNT" || env == "RIBS_MINIMUM_REPLICA_COUNT" ||
			env == "RIBS_MAXIMUM_REPLICA_COUNT" || env == "RIBS_RETRIEVABLE_REPAIR_THRESHOLD" ||
			env == "RIBS_SEND_EXTENDS":
			return "Replication & Repair", true

		// Storage
		case env == "RIBS_DATA" || env == "RIBS_MAX_LOCAL_GROUP_COUNT" || env == "RIBS_MAX_STAGING_GROUP_COUNT":
			return "Storage", true

		// Logging & Monitoring
		case env == "RIBS_LOGLEVEL" || env == "RIBS_LOG_FORMAT" || env == "RIBS_PROMETHEUS_PORT":
			return "Logging & Monitoring", true

		// Network & Services
		case env == "RIBS_FILECOIN_API_ENDPOINT" || env == "RIBS_RUN_SP_CRAWLER" ||
			env == "RIBS_CID_LOCATION_WORKER_COUNT" || env == "RIBS_MONGODB_URI":
			return "Network & Services", true

		// Remaining RIBS_* catch-all
		case strings.HasPrefix(env, "RIBS_"):
			return "Misc", true

		// Caching & Prefetch
		case strings.HasPrefix(env, "FGW_L1_") || strings.HasPrefix(env, "FGW_L2_") ||
			strings.HasPrefix(env, "FGW_PREFETCH_"):
			return "Caching & Prefetch", true

		// Frontend Proxy (remaining FGW_*)
		case strings.HasPrefix(env, "FGW_"):
			return "Frontend Proxy", true

		// Backup
		case strings.HasPrefix(env, "BACKUP_"):
			return "Backup", true

		default:
			return "Misc", true
		}
	}

	var walk func(reflect.Type)
	walk = func(t reflect.Type) {
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if f.Anonymous {
				ft := f.Type
				if ft.Kind() == reflect.Ptr && ft.Elem().Kind() == reflect.Struct {
					walk(ft.Elem())
				} else if ft.Kind() == reflect.Struct {
					walk(ft)
				}
				continue
			}
			if f.Type.Kind() == reflect.Struct {
				walk(f.Type)
				continue
			}
			envTag := f.Tag.Get("envconfig")
			if envTag == "" {
				continue
			}
			def := f.Tag.Get("default")
			sec, adv := sectionFor(envTag)
			keys = append(keys, groupedEnvKey{Var: envTag, DefaultValue: def, Section: sec, Advanced: adv})
		}
	}
	walk(cfgT)
	sort.Slice(keys, func(i, j int) bool { return keys[i].Var < keys[j].Var })
	return keys, nil
}

// ------------------- env helpers ---------------------------------------- //

type EnvCommentFunc func(key string) string

func loadEnv(path string) (map[string]string, error) {
	m, err := godotenv.Read(path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]string{}, nil
	}
	return m, err
}

func saveEnv(path string, m map[string]string, commentFn EnvCommentFunc) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	fmt.Fprintln(w, "# Generated by gwcfg on", time.Now().Format(time.RFC3339))

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := m[k]
		if v == "" {
			continue // skip empty values — let envconfig use struct defaults
		}
		if comment := commentFn(k); comment != "" {
			for _, line := range strings.Split(comment, "\n") {
				fmt.Fprintf(w, "# %s\n", line)
			}
		}
		// Only quote values that need it (contain spaces, #, or newlines).
		// Docker Compose env_file handles unquoted values most reliably.
		if strings.ContainsAny(v, " \t\n#") {
			_, _ = fmt.Fprintf(w, "%s=%q\n", k, v)
		} else {
			_, _ = fmt.Fprintf(w, "%s=%s\n", k, v)
		}
	}
	return w.Flush()
}

// -------------------- validation hooks ---------------------------------- //

type validator func(env map[string]string) (bool, error)

var validators = map[string]validator{
	"Staging": validateExternal,
}

func validateExternal(env map[string]string) (bool, error) {
	path := env["EXTERNAL_LOCALWEB_PATH"]
	url := env["EXTERNAL_LOCALWEB_URL"]
	if path == "" || url == "" {
		return false, fmt.Errorf("EXTERNAL_LOCALWEB_PATH or URL not set")
	}

	name := uuid.NewString() + ".ribscfg"
	full := filepath.Join(path, name)
	if err := os.WriteFile(full, []byte("ribscfg connectivity check\n"), 0o644); err != nil {
		return false, err
	}
	defer os.Remove(full)

	resp, err := http.Get(strings.TrimSuffix(url, "/") + "/" + name)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300, nil
}

// ---------------------- wizard helpers ---------------------------------- //

func ensureDefaults(keys []groupedEnvKey, env map[string]string) {
	keyDefaults := make(map[string]string)
	for _, k := range keys {
		keyDefaults[k.Var] = k.DefaultValue
	}

	for _, k := range keys {
		val := env[k.Var]
		// If empty and has default, use the default value
		if val == "" && k.DefaultValue != "" {
			env[k.Var] = k.DefaultValue
		}
	}

	// Remove empty string entries for keys that have defaults
	// This prevents issues with envconfig trying to parse "" as bool/int
	for key, val := range env {
		if val == "" {
			if def, ok := keyDefaults[key]; ok && def != "" {
				delete(env, key)
			}
		}
	}
}

func menu(keys []groupedEnvKey) (string, error) {
	secMap := map[string]struct{}{}
	for _, k := range keys {
		if k.Advanced {
			continue
		}
		secMap[k.Section] = struct{}{}
	}
	sections := make([]string, 0, len(secMap)+3)
	for s := range secMap {
		sections = append(sections, s)
	}
	sort.Strings(sections)
	sections = append([]string{"Save & Exit", "Exit without saving"}, sections...)
	sections = append(sections, "Advanced ✦")

	opts := make([]huh.Option[string], len(sections))
	for i, s := range sections {
		opts[i] = huh.NewOption(s, s)
	}

	var sel string
	form := huh.NewForm(
		huh.NewGroup(
			huh.NewSelect[string]().Title("Select configuration section").Options(opts...).Value(&sel),
		),
	)
	if err := form.Run(); err != nil {
		return "", err
	}
	return sel, nil
}

func advancedMenu(keys []groupedEnvKey) (string, error) {
	secMap := map[string]struct{}{}
	for _, k := range keys {
		if k.Advanced {
			secMap[k.Section] = struct{}{}
		}
	}
	sections := make([]string, 0, len(secMap)+1)
	for s := range secMap {
		sections = append(sections, s)
	}
	sort.Strings(sections)
	sections = append([]string{"← Back"}, sections...)

	opts := make([]huh.Option[string], len(sections))
	for i, s := range sections {
		opts[i] = huh.NewOption(s, s)
	}

	var sel string
	form := huh.NewForm(
		huh.NewGroup(
			huh.NewSelect[string]().Title("Advanced settings").Options(opts...).Value(&sel),
		),
	)
	if err := form.Run(); err != nil {
		return "", err
	}
	return sel, nil
}

func editSection(section string, keys []groupedEnvKey, env map[string]string, editAdvanced bool) error {
	type binding struct {
		key      string
		value    *string
		hadValue bool // key had a non-empty value in env before editing
	}
	bindings := []binding{}
	fields := []huh.Field{}

	home, _ := os.UserHomeDir()
	walletPath := filepath.Join(home, ".ribswallet")

	// Special handling for CIDGRAVITY_API_TOKEN: run before the form, skip adding to fields
	if section == "CIDGravity" && !editAdvanced {
		for _, k := range keys {
			if k.Section == section && k.Var == "CIDGRAVITY_API_TOKEN" && (k.Advanced == editAdvanced) {
				val := env[k.Var]
				newVal, err := handleCIDGravityTokenInput(walletPath, val)
				if err != nil {
					return err
				}
				env[k.Var] = newVal
				break
			}
		}
	}

	for _, k := range keys {
		if k.Section != section || (k.Advanced != editAdvanced) {
			continue
		}
		if k.Var == "CIDGRAVITY_API_TOKEN" && section == "CIDGravity" && !editAdvanced {
			// already handled above
			continue
		}
		val := env[k.Var] // local copy bound to input
		hadValue := val != ""
		v := k // copy
		bindings = append(bindings, binding{key: v.Var, value: &val, hadValue: hadValue})

		// Add envComment as Description if present
		comment := envComment(v.Var)
		input := huh.NewInput().Title(v.Var).Value(&val).Placeholder(v.DefaultValue)
		if comment != "" {
			input = input.Description(comment)
		}
		fields = append(fields, input)
	}

	if len(fields) == 0 {
		return nil
	}
	if err := huh.NewForm(huh.NewGroup(fields...)).Run(); err != nil {
		return err
	}
	// commit changes — skip empty values that were not previously set
	for _, b := range bindings {
		newVal := *b.value
		if newVal != "" {
			env[b.key] = newVal
		} else if b.hadValue {
			// user cleared a previously-set value — remove so default applies
			delete(env, b.key)
		}
		// empty and was not previously set → skip (don't pollute env)
	}

	if section == "Staging" && !editAdvanced {
		builtin := env["EXTERNAL_LOCALWEB_BUILTIN_SERVER"]
		port := env["EXTERNAL_LOCALWEB_SERVER_PORT"]
		urlStr := env["EXTERNAL_LOCALWEB_URL"]
		if builtin == "true" {
			// Validate port and URL
			if !isValidPort(port) {
				fmt.Printf("❌ Port %q is not valid. Please edit the settings.\n", port)
				return editSection("Staging", keys, env, false)
			}
			if !isValidURL(urlStr) {
				fmt.Printf("❌ URL %q is not valid. Please edit the settings.\n", urlStr)
				return editSection("Staging", keys, env, false)
			}

			// Ask if user wants to test
			doTest := false
			testDesc := "* A temporary server will be started on 0.0.0.0:" + port + ".\n* A request will be made to the configured URL to verify connectivity."
			if isInContainer() {
				testDesc += "\n\nNOTE: You appear to be running inside a container.\n" +
					"The test server binds inside the container, so it will only\n" +
					"work if the container has the port mapped (e.g. --network=host\n" +
					"or -p " + port + ":" + port + "). If you are running gwcfg via\n" +
					"'docker run' without port mapping, skip this test."
			}
			if err := huh.NewForm(
				huh.NewGroup(
					huh.NewConfirm().
						Title("Do you want to test the endpoint online?").
						Description(testDesc).
						Value(&doTest),
				),
			).Run(); err != nil {
				return err
			}
			if doTest {
				for {
					// Start temp server
					handler := http.NewServeMux()
					handler.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
						fmt.Fprintln(w, "gwcfg test OK")
					})
					shutdown, err := startTempHTTPServer(port, handler)
					if err != nil {
						fmt.Printf("❌ Failed to start temporary server: %s\n", err)
						goto afterTest
					}

					// Wait a moment for server to start
					time.Sleep(500 * time.Millisecond)

					{
						// Test endpoint
						testURL := urlStr
						_, err := url.Parse(testURL)
						if err != nil {
							fmt.Printf("❌ Failed to parse URL: %s\n", err)
							goto afterTest
						}

						fmt.Printf("Testing endpoint: %s ...\n", testURL)
						testErr := testEndpoint(testURL)
						if testErr == nil {
							fmt.Println("✅ Endpoint is reachable!")
						} else {
							fmt.Printf("❌ Endpoint test failed: %s\n", testErr)
						}
					}

					// Stop the server immediately after the test
					shutdown()

				afterTest:
					// Prompt for retry/edit/continue
					var action string
					opts := []huh.Option[string]{
						huh.NewOption("Retry test", "retry"),
						huh.NewOption("Edit settings", "edit"),
						huh.NewOption("Continue", "continue"),
					}
					if err := huh.NewForm(
						huh.NewGroup(
							huh.NewSelect[string]().Title("What do you want to do?").Options(opts...).Value(&action),
						),
					).Run(); err != nil {
						return err
					}
					switch action {
					case "retry":
						continue // re-run the test loop
					case "edit":
						return editSection("Staging", keys, env, false)
					case "continue":
						break // exit the test loop and continue
					}
					break // exit the test loop
				}
			}
		}
	}
	return nil
}

// isInContainer returns true if we're likely running inside a Docker/OCI container.
func isInContainer() bool {
	// /.dockerenv exists in Docker containers
	if _, err := os.Stat("/.dockerenv"); err == nil {
		return true
	}
	return false
}

func confirm(title string, def bool) (bool, error) {
	yes := def
	err := huh.NewForm(
		huh.NewGroup(
			huh.NewConfirm().Title(title).Affirmative("Yes").Negative("No").Value(&yes),
		),
	).Run()
	return yes, err
}

func runValidator(section string, env map[string]string) (bool, error) {
	v, ok := validators[section]
	if !ok {
		return true, nil
	}
	do, err := confirm("Verify?", true)
	if err != nil || !do {
		return true, err
	}
	okRes, err := v(env)
	if err != nil {
		fmt.Printf("Validation error: %v\n", err)
		return false, nil
	}
	if okRes {
		fmt.Println("✅ Validation succeeded.")
		return true, nil
	}
	fmt.Println("❌ Validation failed.")
	return false, nil
}

func wizard(envPath string) error {
	keys, err := collectKeys()
	if err != nil {
		return err
	}
	env, err := loadEnv(envPath)
	if err != nil {
		return err
	}
	ensureDefaults(keys, env)

	for {
		sel, err := menu(keys)
		if err != nil {
			return err
		}
		switch sel {
		case "Save & Exit":
			if err := saveEnv(envPath, env, envComment); err != nil {
				return err
			}
			fmt.Printf("Configuration saved to %s\n", envPath)
			return nil
		case "Exit without saving":
			fmt.Println("Exiting without saving changes.")
			return nil
		case "Advanced ✦":
			for {
				sel, err := advancedMenu(keys)
				if err != nil {
					return err
				}
				if sel == "← Back" {
					break
				}
				if err := editSection(sel, keys, env, true); err != nil {
					return err
				}
			}
		default:
			for {
				if err := editSection(sel, keys, env, false); err != nil {
					return err
				}
				verified, err := runValidator(sel, env)
				if err != nil {
					return err
				}
				if verified {
					break
				}
			}
		}
	}
}

// ---------------------- CLI helpers ------------------------------------- //

func cmdGet(envPath, key string) error {
	env, err := loadEnv(envPath)
	if err != nil {
		return err
	}
	if v, ok := env[key]; ok {
		fmt.Println(v)
		return nil
	}
	return fmt.Errorf("key %s not found", key)
}

func cmdSet(envPath, key, val string) error {
	env, err := loadEnv(envPath)
	if err != nil {
		return err
	}
	env[key] = val
	return saveEnv(envPath, env, envComment)
}

// --------------------------- main --------------------------------------- //

func envComment(key string) string {
	switch key {
	case "CIDGRAVITY_API_TOKEN":
		// Try to open/create wallet in the default location
		home, err := os.UserHomeDir()
		var walletAddr string
		if err == nil {
			walletPath := filepath.Join(home, ".ribswallet")
			_, addr, err := rbdeal.OpenOrCreateWallet(walletPath)
			if err == nil {
				walletAddr = addr.String()
			}
		}
		if walletAddr == "" {
			walletAddr = "<your-wallet-address>"
		}
		return fmt.Sprintf(`To get your API token:
1. Sign up at https://app.cidgravity.com
2. Go to https://app.cidgravity.com/wizard
3. Input %s as the wallet address
  NOTE: The address MUST exist on the Filecoin network. The easiest
    way to ensure that is to send a small amount of FIL to that address.
4. Click "Next"
5. Paste the challenge (hex or the whole command) into the prompt below
6. Paste the signed challenge back into the CIDGravity wizard
7. Click "Submit"
8. Copy the API token and paste it into the prompt below
`, walletAddr)
	case "RIBS_DEAL_DURATION":
		return "The duration of the deal in days"
	case "RIBS_DEAL_REMOVE_UNSEALED":
		return "Whether Storage Providers should remove hot-retrieval copy (true/false)"
	case "RIBS_DEAL_SKIP_IPNI_ANNOUNCE":
		return "Whether to ask Storage Providers to skip IPNI announcements (true/false) - note this doesn't stop others from retrieving the data if they have the CID"
	case "RIBS_DEAL_START_TIME":
		return "Time SPs have to handle the deal (Hours)"
	case "RIBS_DATA":
		return "The path to the RIBS data directory"
	case "EXTERNAL_LOCALWEB_BUILTIN_SERVER":
		return "Keep this enabled. Use EXTERNAL_LOCALWEB_SERVER_TLS=false when a reverse proxy or ingress owns 443"
	case "EXTERNAL_LOCALWEB_SERVER_PORT":
		return "Internal port for the LocalWeb server. The default 8443 works for both built-in autocert and reverse-proxy mode"
	case "EXTERNAL_LOCALWEB_URL":
		return "Public root URL that storage providers will use to fetch staged data (for example https://example.com). Do not include a path; RIBS appends the randomized CAR filename"
	case "EXTERNAL_LOCALWEB_SERVER_TLS":
		return "true = built-in autocert mode on 443. false = reverse-proxy mode where nginx/caddy/ingress terminates TLS and forwards to the gateway"
	case "EXTERNAL_LOCALWEB_PATH":
		return "The path to the local web server's data directory. Defaults to <RIBS_DATA>/cardata"
	}
	return ""
}

func handleCIDGravityTokenInput(walletPath, currentValue string) (string, error) {
	for {
		val := currentValue
		field := huh.NewInput().
			Title("CIDGravity API Token").
			Value(&val).
			Description(envComment("CIDGRAVITY_API_TOKEN"))
		if err := huh.NewForm(huh.NewGroup(field)).Run(); err != nil {
			return "", err
		}
		val = strings.TrimSpace(val)
		switch {
		case cidgApiKeyRe.MatchString(val):
			return val, nil
		case cidgHexChallengeRe.MatchString(val) || cidgLotusSignRe.MatchString(val):
			sig, err := signChallengeWithWallet(walletPath, val)
			if err != nil {
				fmt.Printf("❌ Signing failed: %s\n", err)
			} else {
				fmt.Printf("Signature (hex):\n%s\n", color.GreenString("%s", sig))
				fmt.Println("Paste this signature into the CIDGravity wizard, then paste the API token here.")
			}
			// Loop again for API key input
		default:
			fmt.Println("Input does not look like a valid API key or challenge. Please try again.")
		}
	}
}

// Signs a challenge with the wallet at the given path, returns hex signature
func signChallengeWithWallet(walletPath string, challenge string) (string, error) {
	wallet, addr, err := rbdeal.OpenOrCreateWallet(walletPath)
	if err != nil {
		return "", err
	}
	var msg []byte
	// If challenge is a "lotus wallet sign ..." command, extract the hex part
	if strings.HasPrefix(challenge, "lotus wallet sign ") {
		parts := strings.Fields(challenge)
		if len(parts) < 5 {
			return "", fmt.Errorf("invalid sign command")
		}
		challenge = parts[len(parts)-1]
	}

	fmt.Printf("Signing challenge: %s with wallet %s\n", challenge, addr.String())
	msg, err = hex.DecodeString(challenge)
	if err != nil {
		return "", fmt.Errorf("invalid hex challenge: %w", err)
	}
	sig, err := wallet.WalletSign(context.TODO(), addr, msg, api.MsgMeta{Type: api.MTUnknown})
	if err != nil {
		return "", fmt.Errorf("signing failed: %w", err)
	}
	sigBytes := append([]byte{byte(sig.Type)}, sig.Data...)

	return hex.EncodeToString(sigBytes), nil
}

// Helper to validate port
func isValidPort(portStr string) bool {
	port, err := strconv.Atoi(portStr)
	return err == nil && port > 0 && port < 65536
}

// Helper to validate URL
func isValidURL(u string) bool {
	_, err := url.ParseRequestURI(u)
	return err == nil
}

// Start a temporary HTTP server on the given port, returns a shutdown function and the actual port used
func startTempHTTPServer(port string, handler http.Handler) (shutdown func(), err error) {
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return nil, err
	}
	server := &http.Server{Handler: handler}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		server.Serve(ln)
	}()
	wg.Wait()
	return func() {
		server.Close()
	}, nil
}

// Call an external API to test the endpoint (for demo, just GET the URL)
func testEndpoint(url string) error {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}
	return nil
}

func quietThirdPartyLoggers() {
	for _, name := range []string{"rpc"} {
		if err := logging.SetLogLevel(name, "FATAL"); err != nil {
			log.Printf("set log level for %s: %v", name, err)
		}
	}
}

func main() {
	quietThirdPartyLoggers()
	opts := loadOpts()

	abs, _ := filepath.Abs(opts.envFile)

	switch flag.NArg() {
	case 0:
		keys, _ := collectKeys()
		env, _ := loadEnv(abs)
		if len(env) == 0 {
			// Initial setup wizard (file missing, empty, or has no keys)
			if err := initialSetupWizard(abs, keys, opts); err != nil {
				log.Fatalf("initial setup: %v", err)
			}
		} else {
			if err := wizard(abs); err != nil {
				log.Fatalf("wizard: %v", err)
			}
		}
	case 1: // get KEY
		if err := cmdGet(abs, flag.Arg(0)); err != nil {
			log.Fatal(err)
		}
	case 2: // set KEY VAL
		if err := cmdSet(abs, flag.Arg(0), flag.Arg(1)); err != nil {
			log.Fatal(err)
		}
	default:
		fmt.Fprintln(os.Stderr, "Usage: gwcfg [options]            # interactive wizard")
		fmt.Fprintln(os.Stderr, "       gwcfg get KEY              # print value")
		fmt.Fprintln(os.Stderr, "       gwcfg set KEY VAL          # non‑interactive update")
		flag.PrintDefaults()
		os.Exit(1)
	}
}
