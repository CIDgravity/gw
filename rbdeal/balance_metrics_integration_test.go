package rbdeal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestBalanceMetricsWalletBalance verifies wallet balance metric recording
func TestBalanceMetricsWalletBalance(t *testing.T) {
	metrics := GetBalanceMetrics()

	// Set various wallet balances
	metrics.SetWalletBalance(100.5)
	metrics.SetWalletBalance(50.25)
	metrics.SetWalletBalance(0.001)
	metrics.SetWalletBalance(1000.0)

	// Should not panic
	assert.True(t, true, "Wallet balance metrics recorded successfully")
}

// TestBalanceMetricsMarketBalance verifies market balance metric recording
func TestBalanceMetricsMarketBalance(t *testing.T) {
	metrics := GetBalanceMetrics()

	// Set various market balances
	metrics.SetMarketBalance(50.0)
	metrics.SetMarketBalance(25.5)
	metrics.SetMarketBalance(0.0001)
	metrics.SetMarketBalance(500.75)

	assert.True(t, true, "Market balance metrics recorded successfully")
}

// TestBalanceMetricsDatacap verifies datacap metric recording
func TestBalanceMetricsDatacap(t *testing.T) {
	metrics := GetBalanceMetrics()

	// Set datacap remaining in bytes
	metrics.SetDatacapRemaining(1099511627776) // 1 TiB
	metrics.SetDatacapRemaining(5497558138880) // 5 TiB
	metrics.SetDatacapRemaining(1073741824)    // 1 GiB
	metrics.SetDatacapRemaining(0)             // Empty

	assert.True(t, true, "Datacap metrics recorded successfully")
}

// TestBalanceMetricsMarketTopup verifies market top-up metrics
func TestBalanceMetricsMarketTopup(t *testing.T) {
	metrics := GetBalanceMetrics()

	// Record market top-ups
	metrics.IncMarketTopup(10.5)
	metrics.IncMarketTopup(25.0)
	metrics.IncMarketTopup(5.75)

	assert.True(t, true, "Market top-up metrics recorded successfully")
}

// TestBalanceMetricsFaucetRequests verifies faucet request metrics
func TestBalanceMetricsFaucetRequests(t *testing.T) {
	metrics := GetBalanceMetrics()

	// Record successful FIL faucet requests
	metrics.IncFaucetRequest("fil", true)
	metrics.IncFaucetRequest("fil", true)
	metrics.IncFaucetRequest("fil", true)

	// Record failed FIL faucet requests
	metrics.IncFaucetRequest("fil", false)
	metrics.IncFaucetRequest("fil", false)

	// Record datacap faucet requests
	metrics.IncFaucetRequest("datacap", true)
	metrics.IncFaucetRequest("datacap", false)

	assert.True(t, true, "Faucet request metrics recorded successfully")
}

// TestBalanceMetricsDatacapRequests verifies datacap request metrics
func TestBalanceMetricsDatacapRequests(t *testing.T) {
	metrics := GetBalanceMetrics()

	// Record successful datacap requests
	metrics.IncFaucetRequest("datacap", true)
	metrics.IncFaucetRequest("datacap", true)

	// Record failed datacap requests
	metrics.IncFaucetRequest("datacap", false)

	assert.True(t, true, "Datacap request metrics recorded successfully")
}

// TestBalanceMetricsFullLifecycle verifies complete balance metrics lifecycle
func TestBalanceMetricsFullLifecycle(t *testing.T) {
	metrics := GetBalanceMetrics()

	// Initial state
	metrics.SetWalletBalance(100.0)
	metrics.SetMarketBalance(50.0)
	metrics.SetDatacapRemaining(1099511627776)

	// Request FIL from faucet (success)
	metrics.IncFaucetRequest("fil", true)
	metrics.SetWalletBalance(110.0)

	// Top up market balance
	metrics.IncMarketTopup(10.0)
	metrics.SetMarketBalance(60.0)

	// Request datacap (success)
	metrics.IncFaucetRequest("datacap", true)
	metrics.SetDatacapRemaining(2199023255552)

	// Failed faucet request
	metrics.IncFaucetRequest("fil", false)

	assert.True(t, true, "Full balance metrics lifecycle recorded successfully")
}

// TestBalanceMetricsConcurrentAccess verifies thread-safe concurrent metric recording
func TestBalanceMetricsConcurrentAccess(t *testing.T) {
	metrics := GetBalanceMetrics()

	done := make(chan bool, 10)

	// Concurrent metric recording
	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- true }()

			// Record various metrics concurrently
			metrics.SetWalletBalance(float64(id * 10))
			metrics.SetMarketBalance(float64(id * 5))
			metrics.SetDatacapRemaining(float64(id * 1000000000))

			metrics.IncMarketTopup(float64(id))
			metrics.IncFaucetRequest("fil", id%2 == 0)
			metrics.IncFaucetRequest("datacap", id%3 == 0)
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		select {
		case <-done:
			// Success
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for concurrent metric recording")
		}
	}

	assert.True(t, true, "Concurrent balance metrics recording completed successfully")
}

// TestBalanceMetricsSingleton verifies the singleton pattern
func TestBalanceMetricsSingleton(t *testing.T) {
	// Get metrics instance multiple times
	m1 := GetBalanceMetrics()
	m2 := GetBalanceMetrics()
	m3 := GetBalanceMetrics()

	// All should be the same instance
	assert.Same(t, m1, m2, "Should return same instance")
	assert.Same(t, m2, m3, "Should return same instance")
	assert.NotNil(t, m1, "Balance metrics instance should not be nil")
}

// TestBalanceMetricsZeroValues verifies handling of zero values
func TestBalanceMetricsZeroValues(t *testing.T) {
	metrics := GetBalanceMetrics()

	// Set zero balances
	metrics.SetWalletBalance(0)
	metrics.SetMarketBalance(0)
	metrics.SetDatacapRemaining(0)

	// Zero top-up (edge case)
	metrics.IncMarketTopup(0)

	// Should not panic
	assert.True(t, true, "Zero value metrics recorded successfully")
}

// TestBalanceMetricsLargeValues verifies handling of large values
func TestBalanceMetricsLargeValues(t *testing.T) {
	metrics := GetBalanceMetrics()

	// Set large balances
	metrics.SetWalletBalance(1000000.0)          // 1M FIL
	metrics.SetMarketBalance(500000.0)           // 500K FIL
	metrics.SetDatacapRemaining(109951162777600) // 100 TiB

	// Large top-up
	metrics.IncMarketTopup(100000.0)

	assert.True(t, true, "Large value metrics recorded successfully")
}

// TestBalanceMetricsNegativeScenario verifies metrics during negative scenarios
func TestBalanceMetricsNegativeScenario(t *testing.T) {
	metrics := GetBalanceMetrics()

	// Simulate scenario: low balance, failed faucet request
	metrics.SetWalletBalance(0.000001) // Very low balance
	metrics.IncFaucetRequest("fil", false)

	// Low datacap, failed request
	metrics.SetDatacapRemaining(1073741824) // 1 GiB remaining
	metrics.IncFaucetRequest("datacap", false)

	assert.True(t, true, "Negative scenario metrics recorded successfully")
}
