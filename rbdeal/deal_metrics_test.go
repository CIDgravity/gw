package rbdeal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDealPipelineMetrics_IncProposed(t *testing.T) {
	metrics := GetDealPipelineMetrics()

	// Should not panic
	metrics.IncProposed("f01234")
	metrics.IncProposed("f01234")
	metrics.IncProposed("f05678")
}

func TestDealPipelineMetrics_IncAccepted(t *testing.T) {
	metrics := GetDealPipelineMetrics()

	metrics.IncAccepted("f01234")
	metrics.IncAccepted("f01234")
}

func TestDealPipelineMetrics_IncRejected(t *testing.T) {
	metrics := GetDealPipelineMetrics()

	metrics.IncRejected("f01234", "insufficient_funds")
	metrics.IncRejected("f01234", "price_too_low")
	metrics.IncRejected("f05678", "no_capacity")
}

func TestDealPipelineMetrics_IncPublished(t *testing.T) {
	metrics := GetDealPipelineMetrics()

	metrics.IncPublished("f01234")
}

func TestDealPipelineMetrics_IncSealed(t *testing.T) {
	metrics := GetDealPipelineMetrics()

	metrics.IncSealed("f01234")
}

func TestDealPipelineMetrics_IncFailed(t *testing.T) {
	metrics := GetDealPipelineMetrics()

	metrics.IncFailed("timeout")
	metrics.IncFailed("chain_error")
}

func TestDealPipelineMetrics_IncExpired(t *testing.T) {
	metrics := GetDealPipelineMetrics()

	metrics.IncExpired()
}

func TestDealPipelineMetrics_SetGauges(t *testing.T) {
	metrics := GetDealPipelineMetrics()

	metrics.SetActiveDeals(100)
	metrics.SetInProgressDeals(50)
	metrics.SetPendingDeals(25)
}

func TestDealPipelineMetrics_ObserveDurations(t *testing.T) {
	metrics := GetDealPipelineMetrics()

	metrics.ObserveProposalDuration(5 * time.Second)
	metrics.ObservePublishDuration(10 * time.Minute)
	metrics.ObserveSealingDuration(2 * time.Hour)
	metrics.ObserveTotalDealDuration(3 * time.Hour)
	metrics.ObserveProviderSelectionDuration(100 * time.Millisecond)
}

func TestDealPipelineMetrics_ProviderMetrics(t *testing.T) {
	metrics := GetDealPipelineMetrics()

	metrics.AddProvidersQueried(10)
	metrics.AddProvidersSelected(3)
}

func TestDealPipelineMetrics_GroupMetrics(t *testing.T) {
	metrics := GetDealPipelineMetrics()

	metrics.SetGroupsNeedingDeals(100)
	metrics.SetGroupsReadyForDeals(50)
	metrics.SetGroupsOffloaded(200)
}

func TestGetDealPipelineMetrics_Singleton(t *testing.T) {
	metrics := GetDealPipelineMetrics()
	assert.NotNil(t, metrics)

	// Should return same instance
	metrics2 := GetDealPipelineMetrics()
	assert.Same(t, metrics, metrics2)
}
