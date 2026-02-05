package rbdeal

import (
	"testing"
)

// TestParseFallbackProviders_Basic tests parsing a standard comma-separated list of providers.
func TestParseFallbackProviders_Basic(t *testing.T) {
	input := "f02620,f03623016,f03623017"
	expected := []string{"f02620", "f03623016", "f03623017"}

	result := parseFallbackProviders(input)

	if len(result) != len(expected) {
		t.Fatalf("expected %d providers, got %d", len(expected), len(result))
	}

	for i, v := range expected {
		if result[i] != v {
			t.Errorf("expected result[%d] = %q, got %q", i, v, result[i])
		}
	}
}

// TestParseFallbackProviders_WithSpaces tests parsing providers with leading/trailing spaces.
func TestParseFallbackProviders_WithSpaces(t *testing.T) {
	input := " f02620 , f03623016 "
	expected := []string{"f02620", "f03623016"}

	result := parseFallbackProviders(input)

	if len(result) != len(expected) {
		t.Fatalf("expected %d providers, got %d", len(expected), len(result))
	}

	for i, v := range expected {
		if result[i] != v {
			t.Errorf("expected result[%d] = %q, got %q", i, v, result[i])
		}
	}
}

// TestParseFallbackProviders_NoPrefix tests parsing providers without the 'f' prefix.
func TestParseFallbackProviders_NoPrefix(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "numeric only",
			input:    "2620,3623016",
			expected: []string{"f02620", "f03623016"},
		},
		{
			name:     "with leading zeros",
			input:    "02620,03623016",
			expected: []string{"f002620", "f003623016"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := parseFallbackProviders(tc.input)

			if len(result) != len(tc.expected) {
				t.Fatalf("expected %d providers, got %d", len(tc.expected), len(result))
			}

			for i, v := range tc.expected {
				if result[i] != v {
					t.Errorf("expected result[%d] = %q, got %q", i, v, result[i])
				}
			}
		})
	}
}

// TestParseFallbackProviders_FPrefix tests parsing providers with 'f' prefix but missing '0'.
func TestParseFallbackProviders_FPrefix(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "single f prefix",
			input:    "f2620",
			expected: []string{"f02620"},
		},
		{
			name:     "multiple f prefix",
			input:    "f2620,f3623016",
			expected: []string{"f02620", "f03623016"},
		},
		{
			name:     "mixed f and f0 prefix",
			input:    "f2620,f03623016",
			expected: []string{"f02620", "f03623016"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := parseFallbackProviders(tc.input)

			if len(result) != len(tc.expected) {
				t.Fatalf("expected %d providers, got %d", len(tc.expected), len(result))
			}

			for i, v := range tc.expected {
				if result[i] != v {
					t.Errorf("expected result[%d] = %q, got %q", i, v, result[i])
				}
			}
		})
	}
}

// TestParseFallbackProviders_Empty tests that an empty string returns nil.
func TestParseFallbackProviders_Empty(t *testing.T) {
	result := parseFallbackProviders("")

	if result != nil {
		t.Errorf("expected nil for empty input, got %v", result)
	}
}

// TestParseFallbackProviders_EmptyElements tests handling of empty elements in the input.
func TestParseFallbackProviders_EmptyElements(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "empty element in middle",
			input:    "f02620,,f03623016",
			expected: []string{"f02620", "f03623016"},
		},
		{
			name:     "multiple empty elements",
			input:    "f02620,,,f03623016",
			expected: []string{"f02620", "f03623016"},
		},
		{
			name:     "trailing comma",
			input:    "f02620,f03623016,",
			expected: []string{"f02620", "f03623016"},
		},
		{
			name:     "leading comma",
			input:    ",f02620,f03623016",
			expected: []string{"f02620", "f03623016"},
		},
		{
			name:     "only commas",
			input:    ",,,",
			expected: []string{},
		},
		{
			name:     "whitespace elements",
			input:    "f02620, ,f03623016",
			expected: []string{"f02620", "f03623016"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := parseFallbackProviders(tc.input)

			if len(result) != len(tc.expected) {
				t.Fatalf("expected %d providers, got %d: %v", len(tc.expected), len(result), result)
			}

			for i, v := range tc.expected {
				if result[i] != v {
					t.Errorf("expected result[%d] = %q, got %q", i, v, result[i])
				}
			}
		})
	}
}

// TestErrRejected_Error tests the error message format for ErrRejected.
func TestErrRejected_Error(t *testing.T) {
	tests := []struct {
		name     string
		reason   string
		expected string
	}{
		{
			name:     "simple reason",
			reason:   "price too low",
			expected: "deal proposal rejected: price too low",
		},
		{
			name:     "empty reason",
			reason:   "",
			expected: "deal proposal rejected: ",
		},
		{
			name:     "complex reason",
			reason:   "provider f01234 does not accept deals from this client",
			expected: "deal proposal rejected: provider f01234 does not accept deals from this client",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ErrRejected{Reason: tc.reason}
			result := err.Error()

			if result != tc.expected {
				t.Errorf("expected %q, got %q", tc.expected, result)
			}
		})
	}
}

// TestErrRejected_ImplementsError verifies that ErrRejected implements the error interface.
func TestErrRejected_ImplementsError(t *testing.T) {
	var err error = ErrRejected{Reason: "test"}
	if err == nil {
		t.Error("ErrRejected should implement error interface")
	}
}

// TestMakeMoreDeals_Placeholder documents the makeMoreDeals function behavior.
// This test is skipped because makeMoreDeals requires extensive mocking of:
// - Database operations (db.GetDealParams, db.GetNonFailedDealCount, etc.)
// - Lotus gateway RPC client (client.NewGatewayRPCV1)
// - CIDGravity API (cidg.GetBestAvailableProviders)
// - libp2p host (host.Connect, host.NewStream)
// - Local wallet (w.GetDefault, w.WalletSign)
// - Configuration (configuration.GetConfig)
//
// Future integration tests should cover:
// 1. Deal creation when copies are required
// 2. Skipping deal creation when enough copies exist
// 3. Fallback provider selection when GBAP returns no providers
// 4. FallbackProvidersOnly mode
// 5. Deal rejection handling
// 6. Provider connection failures
// 7. Protocol version compatibility checks
// 8. Datacap verification
// 9. Price validation
// 10. canSendMoreDeals rate limiting
func TestMakeMoreDeals_Placeholder(t *testing.T) {
	t.Skip("makeMoreDeals requires extensive mocking of external services (database, Lotus gateway, CIDGravity, libp2p). See test documentation for what needs to be tested.")
}
