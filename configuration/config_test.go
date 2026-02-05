package configuration

import (
	"os"
	"testing"
	"time"
)

// resetConfig resets the global config to zero values before each test
func resetConfig() {
	config = Config{}
}

// setValidLogLevel sets a valid log level to avoid parse errors
func setValidLogLevel(t *testing.T) {
	t.Setenv("RIBS_LOGLEVEL", "ribs.*=info")
}

func TestLoadConfig_Defaults(t *testing.T) {
	resetConfig()

	// Clear all relevant env vars to test defaults
	envVars := []string{
		"RIBS_DATA",
		"RIBS_SEND_EXTENDS",
		"RIBS_FILECOIN_API_ENDPOINT",
		"RIBS_MINIMUM_RETRIEVABLE_COUNT",
		"RIBS_MINIMUM_REPLICA_COUNT",
		"RIBS_MAXIMUM_REPLICA_COUNT",
		"RIBS_RETRIEVABLE_REPAIR_THRESHOLD",
		"RIBS_MAX_LOCAL_GROUP_COUNT",
		"RIBS_MAX_STAGING_GROUP_COUNT",
		"RIBS_DEAL_CHECK_INTERVAL",
		"RIBS_RUN_SP_CRAWLER",
		"RIBS_CID_LOCATION_WORKER_COUNT",
		"RIBS_GC_ENABLED",
		"RIBS_GC_SCAN_INTERVAL",
		"RIBS_GC_GRACE_PERIOD",
		"RIBS_GC_MIN_GROUP_AGE",
		"RIBS_REPAIR_ENABLED",
		"RIBS_REPAIR_WORKERS",
		"RIBS_DEAL_START_TIME",
		"RIBS_DEAL_DURATION",
		"RIBS_DEAL_REMOVE_UNSEALED",
		"RIBS_DEAL_SKIP_IPNI_ANNOUNCE",
		"RIBS_WALLET_UPGRADE_INTERVAL",
		"RIBS_BALANCES_WALLET_FIL_MIN",
		"RIBS_BALANCES_MARKET_FIL_MIN",
		"RIBS_BALANCES_MARKET_FIL_TARGET",
		"RIBS_BALANCES_DATACAP_TIB_MIN",
		"RIBS_BALANCES_DATACAP_TIB_REQUEST",
		"RIBS_YUGABYTE_CQL_HOSTS",
		"RIBS_YUGABYTE_CQL_PORT",
		"RIBS_YUGABYTE_CQL_KEYSPACE",
		"RIBS_YUGABYTE_SQL_HOST",
		"RIBS_YUGABYTE_SQL_PORT",
		"RIBS_YUGABYTE_SQL_USER",
		"RIBS_YUGABYTE_SQL_PASS",
		"RIBS_YUGABYTE_SQL_DB",
		"RIBS_S3API_REGION",
		"RIBS_S3API_BINDADDR",
		"RIBS_S3API_AUTH_ENABLED",
		"RIBS_PROMETHEUS_PORT",
		"CIDGRAVITY_API_ENDPOINT_GBAP",
		"CIDGRAVITY_API_ENDPOINT_GOCD",
		"CIDGRAVITY_MAX_CONNECTIONS",
		"CIDGRAVITY_ALT_CLIENTS",
		"RIBS_LOGLEVEL",
		"RIBS_LOG_FORMAT",
		"FGW_L1_CACHE_SIZE_MIB",
		"FGW_L1_POLICY",
		"FGW_L2_CACHE_ENABLED",
		"FGW_L2_CACHE_SIZE_GB",
		"FGW_L2_CACHE_PATH",
		"FGW_PREFETCH_ENABLED",
		"FGW_PREFETCH_WORKERS",
		"FGW_PREFETCH_DEPTH",
		"EXTERNAL_LOCALWEB_BUILTIN_SERVER",
		"EXTERNAL_LOCALWEB_SERVER_PORT",
		"EXTERNAL_LOCALWEB_SERVER_TLS",
		"EXTERNAL_LOCALWEB_MAX_CONCURRENT_UPLOADS_PER_DEAL",
		"RIBS_ENABLE_PARALLEL_WRITES",
		"RIBS_MAX_PARALLEL_GROUPS",
		"RIBS_SPACE_RESERVATION_TIMEOUT",
		"RIBS_DRAIN_TIMEOUT",
		"BACKUP_S3_ENDPOINT",
		"BACKUP_S3_REGION",
		"BACKUP_WALLET_ENABLED",
		"BACKUP_WALLET_INTERVAL",
		"BACKUP_DATABASE_ENABLED",
		"BACKUP_DATABASE_INTERVAL",
	}
	for _, v := range envVars {
		t.Setenv(v, "")
		os.Unsetenv(v)
	}

	// Set a valid log level to avoid parse errors
	setValidLogLevel(t)

	err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	cfg := GetConfig()

	// Test RibsConfig defaults
	tests := []struct {
		name     string
		got      interface{}
		expected interface{}
	}{
		{"DataDir", cfg.Ribs.DataDir, "~/.ribsdata"},
		{"SendExtends", cfg.Ribs.SendExtends, false},
		{"FilecoinApiEndpoint", cfg.Ribs.FilecoinApiEndpoint, "https://pac-l-gw.devtty.eu/rpc/v1"},
		{"MinimumRetrievableCount", cfg.Ribs.MinimumRetrievableCount, 5},
		{"MinimumReplicaCount", cfg.Ribs.MinimumReplicaCount, 5},
		{"MaximumReplicaCount", cfg.Ribs.MaximumReplicaCount, 10},
		{"RetrievableRepairThreshold", cfg.Ribs.RetrievableRepairThreshold, 3},
		{"MaxLocalGroupCount", cfg.Ribs.MaxLocalGroupCount, 64},
		{"DealCheckInterval", cfg.Ribs.DealCheckInterval, 30 * time.Second},
		{"RunSpCrawler", cfg.Ribs.RunSpCrawler, true},
		{"CidLocationWorkerCount", cfg.Ribs.CidLocationWorkerCount, 16},
		{"GCEnabled", cfg.Ribs.GCEnabled, false},
		{"GCScanInterval", cfg.Ribs.GCScanInterval, time.Hour},
		{"GCGracePeriod", cfg.Ribs.GCGracePeriod, 24 * time.Hour},
		{"GCMinGroupAge", cfg.Ribs.GCMinGroupAge, 168 * time.Hour},
		{"RepairEnabled", cfg.Ribs.RepairEnabled, false},
		{"RepairWorkers", cfg.Ribs.RepairWorkers, 4},
		// MaxStagingGroupCount should default to MaxLocalGroupCount when 0
		{"MaxStagingGroupCount", cfg.Ribs.MaxStagingGroupCount, 64},

		// DealConfig defaults
		{"Deal.StartTime", cfg.Deal.StartTime, uint(96)},
		{"Deal.Duration", cfg.Deal.Duration, 530},
		{"Deal.RemoveUnsealedCopy", cfg.Deal.RemoveUnsealedCopy, false},
		{"Deal.SkipIPNIAnnounce", cfg.Deal.SkipIPNIAnnounce, false},
		{"Deal.FallbackProvidersOnly", cfg.Deal.FallbackProvidersOnly, false},

		// WalletConfig defaults
		{"Wallet.UpgradeInterval", cfg.Wallet.UpgradeInterval, time.Minute},

		// BalancesConfig defaults
		{"Balances.WalletFilMin", cfg.Balances.WalletFilMin, 0.00001},
		{"Balances.MarketFilMin", cfg.Balances.MarketFilMin, 0.00001},
		{"Balances.MarketFilTarget", cfg.Balances.MarketFilTarget, 0.00004},
		{"Balances.DatacapTiBMin", cfg.Balances.DatacapTiBMin, 1},
		{"Balances.DatacapTiBRequest", cfg.Balances.DatacapTiBRequest, 10},

		// YugabyteCqlConfig defaults
		{"YugabyteCql.Hosts", cfg.YugabyteCql.Hosts, "yugabyte"},
		{"YugabyteCql.Port", cfg.YugabyteCql.Port, 9042},
		{"YugabyteCql.Keyspace", cfg.YugabyteCql.Keyspace, "filecoingw"},
		{"YugabyteCql.ForceHosts", cfg.YugabyteCql.ForceHosts, false},
		{"YugabyteCql.Timeout", cfg.YugabyteCql.Timeout, 11},
		{"YugabyteCql.ConnectTimeout", cfg.YugabyteCql.ConnectTimeout, 11},
		{"YugabyteCql.SocketKeepalive", cfg.YugabyteCql.SocketKeepalive, 0},

		// YugabyteSqlConfig defaults
		{"YugabyteSql.Host", cfg.YugabyteSql.Host, "yugabyte"},
		{"YugabyteSql.Port", cfg.YugabyteSql.Port, 5433},
		{"YugabyteSql.User", cfg.YugabyteSql.User, "postgres"},
		{"YugabyteSql.Pass", cfg.YugabyteSql.Pass, "postgres"},
		{"YugabyteSql.Db", cfg.YugabyteSql.Db, "filecoingw"},

		// S3APIConfig defaults
		{"S3API.Region", cfg.S3API.Region, "EU"},
		{"S3API.BindAddr", cfg.S3API.BindAddr, ":8078"},
		{"S3API.AuthEnabled", cfg.S3API.AuthEnabled, false},

		// PrometheusConfig defaults
		{"Prometheus.Port", cfg.Prometheus.Port, 2112},

		// CidGravityConfig defaults
		{"CidGravity.ApiEndpointGetProviders", cfg.CidGravity.ApiEndpointGetProviders, "https://service.cidgravity.com/private/v1/get-best-available-providers"},
		{"CidGravity.ApiEndpointGetDeals", cfg.CidGravity.ApiEndpointGetDeals, "https://service.cidgravity.com/private/v1/get-on-chain-deals"},
		{"CidGravity.MaxConns", cfg.CidGravity.MaxConns, int64(4)},

		// CacheConfig defaults
		{"Cache.L1SizeMiB", cfg.Cache.L1SizeMiB, 2048},
		{"Cache.L1Policy", cfg.Cache.L1Policy, "arc"},
		{"Cache.L2Enabled", cfg.Cache.L2Enabled, false},
		{"Cache.L2SizeGB", cfg.Cache.L2SizeGB, 256},
		{"Cache.L2Path", cfg.Cache.L2Path, "/data/cache/l2"},
		{"Cache.PrefetchEnabled", cfg.Cache.PrefetchEnabled, false},
		{"Cache.PrefetchWorkers", cfg.Cache.PrefetchWorkers, 4},
		{"Cache.PrefetchDepth", cfg.Cache.PrefetchDepth, 2},

		// LocalwebConfig defaults
		{"External.Localweb.BuiltinServer", cfg.External.Localweb.BuiltinServer, true},
		{"External.Localweb.ServerPort", cfg.External.Localweb.ServerPort, "8443"},
		{"External.Localweb.ServerTLS", cfg.External.Localweb.ServerTLS, true},
		{"External.Localweb.MaxConcurrentUploadsPerDeal", cfg.External.Localweb.MaxConcurrentUploadsPerDeal, 15},

		// ParallelWriteConfig defaults
		{"ParallelWrite.Enabled", cfg.ParallelWrite.Enabled, false},
		{"ParallelWrite.MaxParallelGroups", cfg.ParallelWrite.MaxParallelGroups, 4},
		{"ParallelWrite.SpaceReservationTimeout", cfg.ParallelWrite.SpaceReservationTimeout, 100 * time.Millisecond},
		{"ParallelWrite.DrainTimeout", cfg.ParallelWrite.DrainTimeout, 30 * time.Second},

		// BackupConfig defaults
		{"Backup.S3Endpoint", cfg.Backup.S3Endpoint, "https://s3.amazonaws.com"},
		{"Backup.S3Region", cfg.Backup.S3Region, "us-east-1"},
		{"Backup.WalletBackupEnabled", cfg.Backup.WalletBackupEnabled, false},
		{"Backup.WalletBackupInterval", cfg.Backup.WalletBackupInterval, "4h"},
		{"Backup.DatabaseBackupEnabled", cfg.Backup.DatabaseBackupEnabled, false},
		{"Backup.DatabaseBackupInterval", cfg.Backup.DatabaseBackupInterval, "24h"},

		// LogFormat default
		{"LogFormat", cfg.LogFormat, "text"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.expected {
				t.Errorf("%s = %v, want %v", tt.name, tt.got, tt.expected)
			}
		})
	}
}

func TestLoadConfig_EnvOverrides(t *testing.T) {
	resetConfig()

	// Set environment variables to override defaults
	t.Setenv("RIBS_DATA", "/custom/data/path")
	t.Setenv("RIBS_SEND_EXTENDS", "true")
	t.Setenv("RIBS_FILECOIN_API_ENDPOINT", "http://localhost:1234/rpc/v1")
	t.Setenv("RIBS_MINIMUM_RETRIEVABLE_COUNT", "3")
	t.Setenv("RIBS_MINIMUM_REPLICA_COUNT", "6")
	t.Setenv("RIBS_MAXIMUM_REPLICA_COUNT", "12")
	t.Setenv("RIBS_RETRIEVABLE_REPAIR_THRESHOLD", "2")
	t.Setenv("RIBS_MAX_LOCAL_GROUP_COUNT", "128")
	t.Setenv("RIBS_MAX_STAGING_GROUP_COUNT", "200")
	t.Setenv("RIBS_DEAL_CHECK_INTERVAL", "1m")
	t.Setenv("RIBS_RUN_SP_CRAWLER", "false")
	t.Setenv("RIBS_DEAL_START_TIME", "72")
	t.Setenv("RIBS_DEAL_DURATION", "400")
	t.Setenv("RIBS_DEAL_REMOVE_UNSEALED", "true")
	t.Setenv("RIBS_DEAL_SKIP_IPNI_ANNOUNCE", "true")
	t.Setenv("RIBS_YUGABYTE_CQL_HOSTS", "custom-yugabyte")
	t.Setenv("RIBS_YUGABYTE_CQL_PORT", "9999")
	t.Setenv("RIBS_S3API_REGION", "US")
	t.Setenv("RIBS_S3API_AUTH_ENABLED", "true")
	t.Setenv("CIDGRAVITY_API_TOKEN", "test-token")
	t.Setenv("CIDGRAVITY_MAX_CONNECTIONS", "10")
	t.Setenv("RIBS_PROMETHEUS_PORT", "9090")
	t.Setenv("FGW_L1_CACHE_SIZE_MIB", "4096")
	t.Setenv("FGW_L2_CACHE_ENABLED", "true")
	t.Setenv("RIBS_LOG_FORMAT", "json")
	t.Setenv("EXTERNAL_LOCALWEB_PATH", "/custom/cars")
	t.Setenv("EXTERNAL_LOCALWEB_URL", "https://example.com/cars")
	t.Setenv("RIBS_ENABLE_PARALLEL_WRITES", "true")
	t.Setenv("RIBS_MAX_PARALLEL_GROUPS", "8")

	// Set a valid log level to avoid parse errors
	setValidLogLevel(t)

	err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	cfg := GetConfig()

	tests := []struct {
		name     string
		got      interface{}
		expected interface{}
	}{
		{"DataDir", cfg.Ribs.DataDir, "/custom/data/path"},
		{"SendExtends", cfg.Ribs.SendExtends, true},
		{"FilecoinApiEndpoint", cfg.Ribs.FilecoinApiEndpoint, "http://localhost:1234/rpc/v1"},
		{"MinimumRetrievableCount", cfg.Ribs.MinimumRetrievableCount, 3},
		{"MinimumReplicaCount", cfg.Ribs.MinimumReplicaCount, 6},
		{"MaximumReplicaCount", cfg.Ribs.MaximumReplicaCount, 12},
		{"RetrievableRepairThreshold", cfg.Ribs.RetrievableRepairThreshold, 2},
		{"MaxLocalGroupCount", cfg.Ribs.MaxLocalGroupCount, 128},
		{"MaxStagingGroupCount", cfg.Ribs.MaxStagingGroupCount, 200},
		{"DealCheckInterval", cfg.Ribs.DealCheckInterval, time.Minute},
		{"RunSpCrawler", cfg.Ribs.RunSpCrawler, false},
		{"Deal.StartTime", cfg.Deal.StartTime, uint(72)},
		{"Deal.Duration", cfg.Deal.Duration, 400},
		{"Deal.RemoveUnsealedCopy", cfg.Deal.RemoveUnsealedCopy, true},
		{"Deal.SkipIPNIAnnounce", cfg.Deal.SkipIPNIAnnounce, true},
		{"YugabyteCql.Hosts", cfg.YugabyteCql.Hosts, "custom-yugabyte"},
		{"YugabyteCql.Port", cfg.YugabyteCql.Port, 9999},
		{"S3API.Region", cfg.S3API.Region, "US"},
		{"S3API.AuthEnabled", cfg.S3API.AuthEnabled, true},
		{"CidGravity.ApiToken", cfg.CidGravity.ApiToken, "test-token"},
		{"CidGravity.MaxConns", cfg.CidGravity.MaxConns, int64(10)},
		{"Prometheus.Port", cfg.Prometheus.Port, 9090},
		{"Cache.L1SizeMiB", cfg.Cache.L1SizeMiB, 4096},
		{"Cache.L2Enabled", cfg.Cache.L2Enabled, true},
		{"LogFormat", cfg.LogFormat, "json"},
		{"External.Localweb.Path", cfg.External.Localweb.Path, "/custom/cars"},
		{"External.Localweb.Url", cfg.External.Localweb.Url, "https://example.com/cars"},
		{"ParallelWrite.Enabled", cfg.ParallelWrite.Enabled, true},
		{"ParallelWrite.MaxParallelGroups", cfg.ParallelWrite.MaxParallelGroups, 8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.expected {
				t.Errorf("%s = %v, want %v", tt.name, tt.got, tt.expected)
			}
		})
	}
}

func TestLoadConfig_Validation_ReplicaCounts(t *testing.T) {
	tests := []struct {
		name           string
		minRetrievable string
		minReplica     string
		maxReplica     string
		wantErr        bool
		errContains    string
	}{
		{
			name:           "valid_defaults",
			minRetrievable: "5",
			minReplica:     "5",
			maxReplica:     "10",
			wantErr:        false,
		},
		{
			name:           "valid_min_equals_max",
			minRetrievable: "5",
			minReplica:     "5",
			maxReplica:     "5",
			wantErr:        false,
		},
		{
			name:           "invalid_minRetrievable_greater_than_minReplica",
			minRetrievable: "10",
			minReplica:     "5",
			maxReplica:     "15",
			wantErr:        true,
			errContains:    "MinimunRetriveable count greater than MinimumReplica",
		},
		{
			name:           "invalid_minReplica_greater_than_maxReplica",
			minRetrievable: "5",
			minReplica:     "15",
			maxReplica:     "10",
			wantErr:        true,
			errContains:    "MinimunReplica count greater than MaximumReplica",
		},
		{
			name:           "valid_all_equal",
			minRetrievable: "5",
			minReplica:     "5",
			maxReplica:     "5",
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetConfig()

			t.Setenv("RIBS_MINIMUM_RETRIEVABLE_COUNT", tt.minRetrievable)
			t.Setenv("RIBS_MINIMUM_REPLICA_COUNT", tt.minReplica)
			t.Setenv("RIBS_MAXIMUM_REPLICA_COUNT", tt.maxReplica)
			// Set valid repair threshold
			t.Setenv("RIBS_RETRIEVABLE_REPAIR_THRESHOLD", "1")
			// Set valid log level
			setValidLogLevel(t)

			err := LoadConfig()

			if tt.wantErr {
				if err == nil {
					t.Errorf("LoadConfig() expected error containing %q, got nil", tt.errContains)
				} else if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Errorf("LoadConfig() error = %v, want error containing %q", err, tt.errContains)
				}
			} else {
				if err != nil {
					t.Errorf("LoadConfig() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestLoadConfig_Validation_RetrievableThreshold(t *testing.T) {
	tests := []struct {
		name            string
		repairThreshold string
		minReplica      string
		maxReplica      string
		minRetrievable  string
		wantErr         bool
		errContains     string
	}{
		{
			name:            "valid_threshold",
			repairThreshold: "3",
			minReplica:      "5",
			maxReplica:      "10",
			minRetrievable:  "5",
			wantErr:         false,
		},
		{
			name:            "valid_threshold_equals_min",
			repairThreshold: "5",
			minReplica:      "5",
			maxReplica:      "10",
			minRetrievable:  "5",
			wantErr:         false,
		},
		{
			name:            "valid_zero_threshold",
			repairThreshold: "0",
			minReplica:      "5",
			maxReplica:      "10",
			minRetrievable:  "5",
			wantErr:         false,
		},
		{
			name:            "invalid_threshold_greater_than_minReplica",
			repairThreshold: "6",
			minReplica:      "5",
			maxReplica:      "10",
			minRetrievable:  "5",
			wantErr:         true,
			errContains:     "RetrievableRepairThreshold greater than MinimumReplicaCount",
		},
		{
			name:            "invalid_negative_threshold",
			repairThreshold: "-1",
			minReplica:      "5",
			maxReplica:      "10",
			minRetrievable:  "5",
			wantErr:         true,
			errContains:     "RetrievableRepairThreshold negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetConfig()

			t.Setenv("RIBS_RETRIEVABLE_REPAIR_THRESHOLD", tt.repairThreshold)
			t.Setenv("RIBS_MINIMUM_REPLICA_COUNT", tt.minReplica)
			t.Setenv("RIBS_MAXIMUM_REPLICA_COUNT", tt.maxReplica)
			t.Setenv("RIBS_MINIMUM_RETRIEVABLE_COUNT", tt.minRetrievable)
			// Set valid log level
			setValidLogLevel(t)

			err := LoadConfig()

			if tt.wantErr {
				if err == nil {
					t.Errorf("LoadConfig() expected error containing %q, got nil", tt.errContains)
				} else if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Errorf("LoadConfig() error = %v, want error containing %q", err, tt.errContains)
				}
			} else {
				if err != nil {
					t.Errorf("LoadConfig() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestLoadConfig_AltClients(t *testing.T) {
	tests := []struct {
		name         string
		altClients   string
		altTokens    map[string]string
		wantErr      bool
		errContains  string
		expectedKeys []string
	}{
		{
			name:       "no_alt_clients",
			altClients: "",
			altTokens:  map[string]string{},
			wantErr:    false,
		},
		{
			name:       "single_alt_client_with_token",
			altClients: "client1",
			altTokens: map[string]string{
				"client1": "token1",
			},
			wantErr:      false,
			expectedKeys: []string{"client1"},
		},
		{
			name:       "multiple_alt_clients_with_tokens",
			altClients: "client1,client2,client3",
			altTokens: map[string]string{
				"client1": "token1",
				"client2": "token2",
				"client3": "token3",
			},
			wantErr:      false,
			expectedKeys: []string{"client1", "client2", "client3"},
		},
		{
			name:        "missing_token_for_alt_client",
			altClients:  "client1",
			altTokens:   map[string]string{}, // No token provided
			wantErr:     true,
			errContains: "AltClients client1 token not provided",
		},
		{
			name:       "partial_tokens_missing",
			altClients: "client1,client2",
			altTokens: map[string]string{
				"client1": "token1",
				// client2 token missing
			},
			wantErr:     true,
			errContains: "AltClients client2 token not provided",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetConfig()

			// Set up valid replica counts
			t.Setenv("RIBS_MINIMUM_RETRIEVABLE_COUNT", "5")
			t.Setenv("RIBS_MINIMUM_REPLICA_COUNT", "5")
			t.Setenv("RIBS_MAXIMUM_REPLICA_COUNT", "10")
			t.Setenv("RIBS_RETRIEVABLE_REPAIR_THRESHOLD", "3")

			t.Setenv("CIDGRAVITY_ALT_CLIENTS", tt.altClients)

			// Set tokens for alt clients
			for client, token := range tt.altTokens {
				t.Setenv("CIDGRAVITY_API_TOKEN_"+client, token)
			}

			// Set valid log level
			setValidLogLevel(t)

			err := LoadConfig()

			if tt.wantErr {
				if err == nil {
					t.Errorf("LoadConfig() expected error containing %q, got nil", tt.errContains)
				} else if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Errorf("LoadConfig() error = %v, want error containing %q", err, tt.errContains)
				}
			} else {
				if err != nil {
					t.Errorf("LoadConfig() unexpected error = %v", err)
				}

				cfg := GetConfig()

				// Verify alt tokens are correctly populated
				for _, key := range tt.expectedKeys {
					expectedToken := tt.altTokens[key]
					if gotToken, ok := cfg.CidGravity.AltTokens[key]; !ok {
						t.Errorf("AltTokens missing key %q", key)
					} else if gotToken != expectedToken {
						t.Errorf("AltTokens[%q] = %q, want %q", key, gotToken, expectedToken)
					}
				}
			}
		})
	}
}

func TestGetS3CqlConfig_Fallback(t *testing.T) {
	resetConfig()

	// Set up YugabyteCql values (the fallback source)
	t.Setenv("RIBS_YUGABYTE_CQL_HOSTS", "yugabyte-host")
	t.Setenv("RIBS_YUGABYTE_CQL_PORT", "9042")
	t.Setenv("RIBS_YUGABYTE_CQL_KEYSPACE", "main_keyspace")
	t.Setenv("RIBS_YUGABYTE_CQL_USER", "yugabyte_user")
	t.Setenv("RIBS_YUGABYTE_CQL_PASS", "yugabyte_pass")
	t.Setenv("RIBS_YUGABYTE_CQL_FORCE_HOSTS", "true")
	t.Setenv("RIBS_YUGABYTE_CQL_TIMEOUT", "15")
	t.Setenv("RIBS_YUGABYTE_CQL_CONNECT_TIMEOUT", "20")
	t.Setenv("RIBS_YUGABYTE_CQL_SOCKET_KEEPALIVE", "30")

	// Don't set S3 CQL values - they should fall back
	t.Setenv("RIBS_S3_CQL_HOSTS", "")
	t.Setenv("RIBS_S3_CQL_PORT", "0")
	t.Setenv("RIBS_S3_CQL_KEYSPACE", "")
	t.Setenv("RIBS_S3_CQL_USER", "")
	t.Setenv("RIBS_S3_CQL_PASS", "")

	// Set valid log level
	setValidLogLevel(t)

	err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	cfg := GetConfig()
	s3Cfg := cfg.GetS3CqlConfig()

	// Verify fallback values
	tests := []struct {
		name     string
		got      interface{}
		expected interface{}
	}{
		{"Hosts", s3Cfg.Hosts, "yugabyte-host"},
		{"Port", s3Cfg.Port, 9042},
		{"Keyspace", s3Cfg.Keyspace, "main_keyspace"},
		{"User", s3Cfg.User, "yugabyte_user"},
		{"Pass", s3Cfg.Pass, "yugabyte_pass"},
		// These always come from YugabyteCql
		{"ForceHosts", s3Cfg.ForceHosts, true},
		{"Timeout", s3Cfg.Timeout, 15},
		{"ConnectTimeout", s3Cfg.ConnectTimeout, 20},
		{"SocketKeepalive", s3Cfg.SocketKeepalive, 30},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.expected {
				t.Errorf("%s = %v, want %v", tt.name, tt.got, tt.expected)
			}
		})
	}
}

func TestGetS3CqlConfig_Override(t *testing.T) {
	resetConfig()

	// Set up YugabyteCql values (the fallback source)
	t.Setenv("RIBS_YUGABYTE_CQL_HOSTS", "yugabyte-host")
	t.Setenv("RIBS_YUGABYTE_CQL_PORT", "9042")
	t.Setenv("RIBS_YUGABYTE_CQL_KEYSPACE", "main_keyspace")
	t.Setenv("RIBS_YUGABYTE_CQL_USER", "yugabyte_user")
	t.Setenv("RIBS_YUGABYTE_CQL_PASS", "yugabyte_pass")
	t.Setenv("RIBS_YUGABYTE_CQL_FORCE_HOSTS", "true")
	t.Setenv("RIBS_YUGABYTE_CQL_TIMEOUT", "15")
	t.Setenv("RIBS_YUGABYTE_CQL_CONNECT_TIMEOUT", "20")
	t.Setenv("RIBS_YUGABYTE_CQL_SOCKET_KEEPALIVE", "30")

	// Set S3 CQL specific overrides
	t.Setenv("RIBS_S3_CQL_HOSTS", "s3-cql-host")
	t.Setenv("RIBS_S3_CQL_PORT", "19042")
	t.Setenv("RIBS_S3_CQL_KEYSPACE", "s3_keyspace")
	t.Setenv("RIBS_S3_CQL_USER", "s3_user")
	t.Setenv("RIBS_S3_CQL_PASS", "s3_pass")

	// Set valid log level
	setValidLogLevel(t)

	err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	cfg := GetConfig()
	s3Cfg := cfg.GetS3CqlConfig()

	// Verify S3-specific values are used
	tests := []struct {
		name     string
		got      interface{}
		expected interface{}
	}{
		{"Hosts", s3Cfg.Hosts, "s3-cql-host"},
		{"Port", s3Cfg.Port, 19042},
		{"Keyspace", s3Cfg.Keyspace, "s3_keyspace"},
		{"User", s3Cfg.User, "s3_user"},
		{"Pass", s3Cfg.Pass, "s3_pass"},
		// These always come from YugabyteCql
		{"ForceHosts", s3Cfg.ForceHosts, true},
		{"Timeout", s3Cfg.Timeout, 15},
		{"ConnectTimeout", s3Cfg.ConnectTimeout, 20},
		{"SocketKeepalive", s3Cfg.SocketKeepalive, 30},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.expected {
				t.Errorf("%s = %v, want %v", tt.name, tt.got, tt.expected)
			}
		})
	}
}

func TestConfigureLogLevels(t *testing.T) {
	tests := []struct {
		name        string
		logLevel    string
		wantErr     bool
		errContains string
	}{
		{
			name:     "valid_single_component",
			logLevel: "ribs=debug",
			wantErr:  false,
		},
		{
			name:     "valid_multiple_components",
			logLevel: "ribs=debug,deal=info",
			wantErr:  false,
		},
		{
			name:     "valid_wildcard",
			logLevel: "ribs.*=debug",
			wantErr:  false,
		},
		{
			name:        "invalid_format_no_equals",
			logLevel:    "ribsdebug",
			wantErr:     true,
			errContains: "invalid log level",
		},
		{
			name:        "invalid_format_multiple_equals",
			logLevel:    "ribs=debug=extra",
			wantErr:     true,
			errContains: "invalid log level",
		},
		{
			name:        "invalid_level_value",
			logLevel:    "ribs=invalid_level",
			wantErr:     true,
			errContains: "invalid log level",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{LogLevel: tt.logLevel}
			err := cfg.configureLogLevels()

			if tt.wantErr {
				if err == nil {
					t.Errorf("configureLogLevels() expected error, got nil")
				} else if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Errorf("configureLogLevels() error = %v, want error containing %q", err, tt.errContains)
				}
			} else {
				if err != nil {
					t.Errorf("configureLogLevels() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestConfigureLogFormat_Text(t *testing.T) {
	err := configureLogFormat("text")
	if err != nil {
		t.Errorf("configureLogFormat(text) error = %v", err)
	}

	// Test case-insensitivity
	err = configureLogFormat("TEXT")
	if err != nil {
		t.Errorf("configureLogFormat(TEXT) error = %v", err)
	}

	// Test empty string defaults to text
	err = configureLogFormat("")
	if err != nil {
		t.Errorf("configureLogFormat('') error = %v", err)
	}
}

func TestConfigureLogFormat_JSON(t *testing.T) {
	err := configureLogFormat("json")
	if err != nil {
		t.Errorf("configureLogFormat(json) error = %v", err)
	}

	// Test case-insensitivity
	err = configureLogFormat("JSON")
	if err != nil {
		t.Errorf("configureLogFormat(JSON) error = %v", err)
	}

	err = configureLogFormat("Json")
	if err != nil {
		t.Errorf("configureLogFormat(Json) error = %v", err)
	}
}

func TestConfigureLogFormat_Invalid(t *testing.T) {
	tests := []struct {
		name        string
		format      string
		errContains string
	}{
		{
			name:        "invalid_xml",
			format:      "xml",
			errContains: "unsupported log format: xml",
		},
		{
			name:        "invalid_yaml",
			format:      "yaml",
			errContains: "unsupported log format: yaml",
		},
		{
			name:        "invalid_random",
			format:      "random_format",
			errContains: "unsupported log format: random_format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := configureLogFormat(tt.format)
			if err == nil {
				t.Errorf("configureLogFormat(%q) expected error, got nil", tt.format)
			} else if !contains(err.Error(), tt.errContains) {
				t.Errorf("configureLogFormat(%q) error = %v, want error containing %q", tt.format, err, tt.errContains)
			}
		})
	}
}

// TestGetConfig verifies that GetConfig returns the global config pointer
func TestGetConfig(t *testing.T) {
	resetConfig()

	t.Setenv("RIBS_DATA", "/test/path")
	setValidLogLevel(t)

	err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	cfg1 := GetConfig()
	cfg2 := GetConfig()

	// Should return the same pointer
	if cfg1 != cfg2 {
		t.Error("GetConfig() should return the same pointer each time")
	}

	// Verify the value
	if cfg1.Ribs.DataDir != "/test/path" {
		t.Errorf("GetConfig().Ribs.DataDir = %v, want %v", cfg1.Ribs.DataDir, "/test/path")
	}
}

// TestLoadConfig_MaxStagingGroupCountDefault verifies that MaxStagingGroupCount
// defaults to MaxLocalGroupCount when set to 0
func TestLoadConfig_MaxStagingGroupCountDefault(t *testing.T) {
	resetConfig()

	t.Setenv("RIBS_MAX_LOCAL_GROUP_COUNT", "100")
	t.Setenv("RIBS_MAX_STAGING_GROUP_COUNT", "0") // Should default to MaxLocalGroupCount
	setValidLogLevel(t)

	err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	cfg := GetConfig()

	if cfg.Ribs.MaxStagingGroupCount != 100 {
		t.Errorf("MaxStagingGroupCount = %v, want %v (should default to MaxLocalGroupCount)", cfg.Ribs.MaxStagingGroupCount, 100)
	}
}

// TestLoadConfig_ParallelWriteValidation tests parallel write config validation
func TestLoadConfig_ParallelWriteValidation(t *testing.T) {
	tests := []struct {
		name              string
		enabled           string
		maxParallelGroups string
		wantErr           bool
		errContains       string
	}{
		{
			name:              "disabled_with_invalid_groups",
			enabled:           "false",
			maxParallelGroups: "0",
			wantErr:           false, // No validation when disabled
		},
		{
			name:              "enabled_with_valid_groups",
			enabled:           "true",
			maxParallelGroups: "4",
			wantErr:           false,
		},
		{
			name:              "enabled_with_zero_groups",
			enabled:           "true",
			maxParallelGroups: "0",
			wantErr:           true,
			errContains:       "MaxParallelGroups must be at least 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetConfig()

			t.Setenv("RIBS_ENABLE_PARALLEL_WRITES", tt.enabled)
			t.Setenv("RIBS_MAX_PARALLEL_GROUPS", tt.maxParallelGroups)
			setValidLogLevel(t)

			err := LoadConfig()

			if tt.wantErr {
				if err == nil {
					t.Errorf("LoadConfig() expected error containing %q, got nil", tt.errContains)
				} else if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Errorf("LoadConfig() error = %v, want error containing %q", err, tt.errContains)
				}
			} else {
				if err != nil {
					t.Errorf("LoadConfig() unexpected error = %v", err)
				}
			}
		})
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
