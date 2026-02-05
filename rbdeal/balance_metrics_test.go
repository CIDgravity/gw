package rbdeal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBalanceMetrics_SetWalletBalance(t *testing.T) {
	metrics := GetBalanceMetrics()

	// Set balance in FIL - should not panic
	metrics.SetWalletBalance(100.5)
	metrics.SetWalletBalance(50.25)
}

func TestBalanceMetrics_SetMarketBalance(t *testing.T) {
	metrics := GetBalanceMetrics()

	metrics.SetMarketBalance(200.75)
}

func TestBalanceMetrics_SetDatacapRemaining(t *testing.T) {
	metrics := GetBalanceMetrics()

	// Set datacap in bytes (1 TiB)
	datacapBytes := float64(1024 * 1024 * 1024 * 1024)
	metrics.SetDatacapRemaining(datacapBytes)
}

func TestBalanceMetrics_IncMarketTopup(t *testing.T) {
	metrics := GetBalanceMetrics()

	metrics.IncMarketTopup(10.5)
	metrics.IncMarketTopup(5.25)
}

func TestBalanceMetrics_IncFaucetRequest(t *testing.T) {
	metrics := GetBalanceMetrics()

	metrics.IncFaucetRequest("fil", true)
	metrics.IncFaucetRequest("fil", true)
	metrics.IncFaucetRequest("fil", false)
	metrics.IncFaucetRequest("datacap", true)
	metrics.IncFaucetRequest("datacap", false)
}

func TestGetBalanceMetrics_Singleton(t *testing.T) {
	metrics := GetBalanceMetrics()
	assert.NotNil(t, metrics)

	// Should return same instance
	metrics2 := GetBalanceMetrics()
	assert.Same(t, metrics, metrics2)
}

func TestBalanceMetrics_AttoFILToFIL(t *testing.T) {
	// Test the conversion factor
	// 1 FIL = 1e18 attoFIL
	attoFIL := uint64(1_500_000_000_000_000_000) // 1.5 FIL in attoFIL
	fil := float64(attoFIL) / 1e18

	assert.InDelta(t, 1.5, fil, 0.0001)
}

func TestBalanceMetrics_LargeDatacap(t *testing.T) {
	metrics := GetBalanceMetrics()

	// Test with large datacap (1 PiB)
	pib := float64(1024 * 1024 * 1024 * 1024 * 1024)
	metrics.SetDatacapRemaining(pib)
}
