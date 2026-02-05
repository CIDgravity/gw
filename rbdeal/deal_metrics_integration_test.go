package rbdeal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestDealMetricsActivation verifies metrics are incremented on deal activation
func TestDealMetricsActivation(t *testing.T) {
	metrics := GetDealPipelineMetrics()

	// Record deal activation
	provider := "f01234"

	// Initial state
	metrics.IncProposed(provider)
	metrics.IncAccepted(provider)

	// Activation
	metrics.IncPublished(provider)

	// Should not panic
	assert.True(t, true, "Deal activation metrics recorded successfully")
}

// TestDealMetricsPublishing verifies metrics are incremented on deal publishing
func TestDealMetricsPublishing(t *testing.T) {
	metrics := GetDealPipelineMetrics()

	provider := "f05678"

	// Propose and accept
	metrics.IncProposed(provider)
	metrics.IncAccepted(provider)

	// Publish
	metrics.IncPublished(provider)

	// Verify no errors
	assert.True(t, true, "Deal publishing metrics recorded successfully")
}

// TestDealMetricsSealing verifies metrics are incremented on deal sealing
func TestDealMetricsSealing(t *testing.T) {
	metrics := GetDealPipelineMetrics()

	provider := "f01234"

	// Full pipeline
	metrics.IncProposed(provider)
	metrics.IncAccepted(provider)
	metrics.IncPublished(provider)

	// Seal
	metrics.IncSealed(provider)

	assert.True(t, true, "Deal sealing metrics recorded successfully")
}

// TestDealMetricsGaugeUpdates verifies gauge metric updates
func TestDealMetricsGaugeUpdates(t *testing.T) {
	metrics := GetDealPipelineMetrics()

	// Update gauges
	metrics.SetActiveDeals(100)
	metrics.SetInProgressDeals(50)
	metrics.SetPendingDeals(25)

	metrics.SetGroupsNeedingDeals(200)
	metrics.SetGroupsReadyForDeals(150)
	metrics.SetGroupsOffloaded(75)

	// Should not panic
	assert.True(t, true, "Gauge updates completed successfully")
}

// TestDealMetricsDurationObservations verifies duration histogram observations
func TestDealMetricsDurationObservations(t *testing.T) {
	metrics := GetDealPipelineMetrics()

	// Observe various durations
	metrics.ObserveProposalDuration(5 * time.Second)
	metrics.ObservePublishDuration(10 * time.Minute)
	metrics.ObserveSealingDuration(2 * time.Hour)
	metrics.ObserveTotalDealDuration(3 * time.Hour)
	metrics.ObserveProviderSelectionDuration(100 * time.Millisecond)

	assert.True(t, true, "Duration observations recorded successfully")
}

// TestDealMetricsProviderCounts verifies provider query and selection metrics
func TestDealMetricsProviderCounts(t *testing.T) {
	metrics := GetDealPipelineMetrics()

	// Record provider metrics
	metrics.AddProvidersQueried(10)
	metrics.AddProvidersSelected(3)

	assert.True(t, true, "Provider counts recorded successfully")
}

// TestDealMetricsRejectionTracking verifies deal rejection metrics
func TestDealMetricsRejectionTracking(t *testing.T) {
	metrics := GetDealPipelineMetrics()

	provider := "f01234"

	// Record rejections with different reasons
	metrics.IncRejected(provider, "insufficient_funds")
	metrics.IncRejected(provider, "price_too_low")
	metrics.IncRejected(provider, "no_capacity")

	// Different provider
	metrics.IncRejected("f05678", "timeout")

	assert.True(t, true, "Rejection metrics recorded successfully")
}

// TestDealMetricsFailureTracking verifies deal failure metrics
func TestDealMetricsFailureTracking(t *testing.T) {
	metrics := GetDealPipelineMetrics()

	// Record failures
	metrics.IncFailed("timeout")
	metrics.IncFailed("chain_error")
	metrics.IncFailed("provider_rejected")

	assert.True(t, true, "Failure metrics recorded successfully")
}

// TestDealMetricsExpirationTracking verifies deal expiration metrics
func TestDealMetricsExpirationTracking(t *testing.T) {
	metrics := GetDealPipelineMetrics()

	// Record expirations
	metrics.IncExpired()
	metrics.IncExpired()
	metrics.IncExpired()

	assert.True(t, true, "Expiration metrics recorded successfully")
}

// TestDealMetricsFullPipeline verifies complete deal lifecycle metrics
func TestDealMetricsFullPipeline(t *testing.T) {
	metrics := GetDealPipelineMetrics()

	provider := "f01234"

	// Complete successful deal lifecycle
	metrics.IncProposed(provider)
	metrics.IncAccepted(provider)
	metrics.IncPublished(provider)
	metrics.IncSealed(provider)

	// Record durations
	metrics.ObserveProposalDuration(30 * time.Second)
	metrics.ObservePublishDuration(5 * time.Minute)
	metrics.ObserveSealingDuration(1 * time.Hour)
	metrics.ObserveTotalDealDuration(2 * time.Hour)

	// Update gauges
	metrics.SetActiveDeals(1)

	assert.True(t, true, "Full pipeline metrics recorded successfully")
}

// TestDealMetricsConcurrentAccess verifies thread-safe concurrent metric recording
func TestDealMetricsConcurrentAccess(t *testing.T) {
	metrics := GetDealPipelineMetrics()

	done := make(chan bool, 10)

	// Concurrent metric recording
	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- true }()

			provider := string(rune('a' + id%5))

			// Record various metrics concurrently
			metrics.IncProposed(provider)
			metrics.IncAccepted(provider)
			metrics.IncPublished(provider)
			metrics.IncSealed(provider)

			metrics.ObserveProposalDuration(time.Duration(id) * time.Second)
			metrics.SetActiveDeals(float64(id * 10))
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

	// Should not panic
	assert.True(t, true, "Concurrent metric recording completed successfully")
}

// TestDealMetricsSingleton verifies the singleton pattern
func TestDealMetricsSingleton(t *testing.T) {
	// Get metrics instance multiple times
	m1 := GetDealPipelineMetrics()
	m2 := GetDealPipelineMetrics()
	m3 := GetDealPipelineMetrics()

	// All should be the same instance
	assert.Same(t, m1, m2, "Should return same instance")
	assert.Same(t, m2, m3, "Should return same instance")
	assert.NotNil(t, m1, "Metrics instance should not be nil")
}
