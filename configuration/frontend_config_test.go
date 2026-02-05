package configuration

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestFrontendConfigEnvVars verifies all FGW_* env vars are loaded correctly
func TestFrontendConfigEnvVars(t *testing.T) {
	resetConfig()

	// Set all FrontendConfig env vars
	t.Setenv("FGW_NODE_ID", "test-node-01")
	t.Setenv("FGW_NODE_TYPE", "storage")
	t.Setenv("FGW_BACKEND_NODES", "node1:http://host1:8080,node2:http://host2:8080")
	t.Setenv("FGW_YCQL_HOSTS", "yugabyte-test")
	t.Setenv("FGW_YCQL_PORT", "19042")
	t.Setenv("FGW_YCQL_KEYSPACE", "test_keyspace")
	t.Setenv("FGW_YCQL_USER", "test_user")
	t.Setenv("FGW_YCQL_PASS", "test_pass")
	t.Setenv("FGW_INTERNAL_API_BINDADDR", ":9091")

	// Set valid log level to avoid parse errors
	setValidLogLevel(t)

	err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	cfg := GetConfig()

	// Verify all FrontendConfig values
	tests := []struct {
		name     string
		got      interface{}
		expected interface{}
	}{
		{"Frontend.NodeID", cfg.Frontend.NodeID, "test-node-01"},
		{"Frontend.NodeType", cfg.Frontend.NodeType, "storage"},
		{"Frontend.BackendNodes", cfg.Frontend.BackendNodes, "node1:http://host1:8080,node2:http://host2:8080"},
		{"Frontend.YCQLHosts", cfg.Frontend.YCQLHosts, "yugabyte-test"},
		{"Frontend.YCQLPort", cfg.Frontend.YCQLPort, 19042},
		{"Frontend.YCQLKeyspace", cfg.Frontend.YCQLKeyspace, "test_keyspace"},
		{"Frontend.YCQLUser", cfg.Frontend.YCQLUser, "test_user"},
		{"Frontend.YCQLPass", cfg.Frontend.YCQLPass, "test_pass"},
		{"Frontend.InternalAPIBindAddr", cfg.Frontend.InternalAPIBindAddr, ":9091"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.expected {
				t.Errorf("%s = %v, want %v", tt.name, tt.got, tt.expected)
			}
		})
	}
}

// TestFrontendConfigDefaults verifies default values for FrontendConfig
func TestFrontendConfigDefaults(t *testing.T) {
	resetConfig()

	// Clear all FGW_* env vars to test defaults
	envVars := []string{
		"FGW_NODE_ID",
		"FGW_NODE_TYPE",
		"FGW_BACKEND_NODES",
		"FGW_YCQL_HOSTS",
		"FGW_YCQL_PORT",
		"FGW_YCQL_KEYSPACE",
		"FGW_YCQL_USER",
		"FGW_YCQL_PASS",
		"FGW_INTERNAL_API_BINDADDR",
	}
	for _, v := range envVars {
		t.Setenv(v, "")
		os.Unsetenv(v)
	}

	// Set valid log level to avoid parse errors
	setValidLogLevel(t)

	err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	cfg := GetConfig()

	// Verify default values
	tests := []struct {
		name     string
		got      interface{}
		expected interface{}
	}{
		{"Frontend.NodeID default", cfg.Frontend.NodeID, "frontend-default"},
		{"Frontend.NodeType default", cfg.Frontend.NodeType, "frontend"},
		{"Frontend.BackendNodes default", cfg.Frontend.BackendNodes, ""},
		{"Frontend.YCQLHosts default", cfg.Frontend.YCQLHosts, "localhost"},
		{"Frontend.YCQLPort default", cfg.Frontend.YCQLPort, 9042},
		{"Frontend.YCQLKeyspace default", cfg.Frontend.YCQLKeyspace, "filecoingw"},
		{"Frontend.YCQLUser default", cfg.Frontend.YCQLUser, ""},
		{"Frontend.YCQLPass default", cfg.Frontend.YCQLPass, ""},
		{"Frontend.InternalAPIBindAddr default", cfg.Frontend.InternalAPIBindAddr, ":9090"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.expected {
				t.Errorf("%s = %v, want %v", tt.name, tt.got, tt.expected)
			}
		})
	}
}

// TestFrontendConfigValidation verifies FrontendConfig validation
func TestFrontendConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		nodeID      string
		nodeType    string
		bindAddr    string
		wantErr     bool
		errContains string
	}{
		{
			name:     "valid_config",
			nodeID:   "node-001",
			nodeType: "frontend",
			bindAddr: ":9090",
			wantErr:  false,
		},
		{
			name:     "empty_node_id",
			nodeID:   "",
			nodeType: "frontend",
			bindAddr: ":9090",
			wantErr:  false, // Empty node ID should use default
		},
		{
			name:     "valid_storage_node",
			nodeID:   "storage-001",
			nodeType: "storage",
			bindAddr: ":9090",
			wantErr:  false,
		},
		{
			name:     "custom_bind_addr",
			nodeID:   "node-002",
			nodeType: "frontend",
			bindAddr: "0.0.0.0:8080",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetConfig()

			t.Setenv("FGW_NODE_ID", tt.nodeID)
			t.Setenv("FGW_NODE_TYPE", tt.nodeType)
			t.Setenv("FGW_INTERNAL_API_BINDADDR", tt.bindAddr)

			// Set valid log level
			setValidLogLevel(t)

			err := LoadConfig()

			if tt.wantErr {
				if err == nil {
					t.Errorf("LoadConfig() expected error, got nil")
				} else if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Errorf("LoadConfig() error = %v, want error containing %q", err, tt.errContains)
				}
			} else {
				if err != nil {
					t.Errorf("LoadConfig() unexpected error = %v", err)
				}
			}

			cfg := GetConfig()
			if tt.nodeID != "" {
				assert.Equal(t, tt.nodeID, cfg.Frontend.NodeID)
			}
			if tt.nodeType != "" {
				assert.Equal(t, tt.nodeType, cfg.Frontend.NodeType)
			}
			if tt.bindAddr != "" {
				assert.Equal(t, tt.bindAddr, cfg.Frontend.InternalAPIBindAddr)
			}
		})
	}
}

// TestFrontendConfigBackendNodesParsing verifies backend nodes string parsing
func TestFrontendConfigBackendNodesParsing(t *testing.T) {
	tests := []struct {
		name         string
		backendNodes string
		expectedLen  int // Number of nodes (rough check)
	}{
		{
			name:         "single_node",
			backendNodes: "node1:http://host1:8080",
			expectedLen:  1,
		},
		{
			name:         "multiple_nodes",
			backendNodes: "node1:http://host1:8080,node2:http://host2:8080,node3:http://host3:8080",
			expectedLen:  3,
		},
		{
			name:         "empty_nodes",
			backendNodes: "",
			expectedLen:  0,
		},
		{
			name:         "nodes_with_special_chars",
			backendNodes: "node-1:http://host-1.local:8080,node_2:http://192.168.1.1:9090",
			expectedLen:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetConfig()

			t.Setenv("FGW_BACKEND_NODES", tt.backendNodes)
			setValidLogLevel(t)

			err := LoadConfig()
			if err != nil {
				t.Fatalf("LoadConfig() error = %v", err)
			}

			cfg := GetConfig()
			assert.Equal(t, tt.backendNodes, cfg.Frontend.BackendNodes)

			// Rough check: count commas to estimate nodes
			if tt.backendNodes != "" {
				nodeCount := 1
				for _, c := range tt.backendNodes {
					if c == ',' {
						nodeCount++
					}
				}
				assert.Equal(t, tt.expectedLen, nodeCount)
			}
		})
	}
}

// TestFrontendConfigYCQLConnection verifies YCQL connection settings
func TestFrontendConfigYCQLConnection(t *testing.T) {
	resetConfig()

	t.Setenv("FGW_YCQL_HOSTS", "yb-node1,yb-node2,yb-node3")
	t.Setenv("FGW_YCQL_PORT", "9042")
	t.Setenv("FGW_YCQL_KEYSPACE", "my_keyspace")
	t.Setenv("FGW_YCQL_USER", "my_user")
	t.Setenv("FGW_YCQL_PASS", "my_password")
	setValidLogLevel(t)

	err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	cfg := GetConfig()

	// Verify YCQL settings
	assert.Equal(t, "yb-node1,yb-node2,yb-node3", cfg.Frontend.YCQLHosts)
	assert.Equal(t, 9042, cfg.Frontend.YCQLPort)
	assert.Equal(t, "my_keyspace", cfg.Frontend.YCQLKeyspace)
	assert.Equal(t, "my_user", cfg.Frontend.YCQLUser)
	assert.Equal(t, "my_password", cfg.Frontend.YCQLPass)
}
