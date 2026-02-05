package rbdeal

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// DealPipelineMetrics tracks deal lifecycle metrics for observability.
type DealPipelineMetrics struct {
	// Deal counts by state
	dealsProposed  *prometheus.CounterVec
	dealsAccepted  *prometheus.CounterVec
	dealsRejected  *prometheus.CounterVec
	dealsPublished *prometheus.CounterVec
	dealsSealed    *prometheus.CounterVec
	dealsFailed    *prometheus.CounterVec
	dealsExpired   prometheus.Counter

	// Current state gauges
	dealsActiveGauge     prometheus.Gauge
	dealsInProgressGauge prometheus.Gauge
	dealsPendingGauge    prometheus.Gauge

	// Timing histograms
	proposalDuration  prometheus.Histogram
	publishDuration   prometheus.Histogram
	sealingDuration   prometheus.Histogram
	totalDealDuration prometheus.Histogram

	// Provider selection
	providerSelectionDuration prometheus.Histogram
	providersQueried          prometheus.Counter
	providersSelected         prometheus.Counter

	// Group deal tracking
	groupsNeedingDeals  prometheus.Gauge
	groupsReadyForDeals prometheus.Gauge
	groupsOffloaded     prometheus.Gauge
}

// NewDealPipelineMetrics creates a new DealPipelineMetrics instance.
func NewDealPipelineMetrics() *DealPipelineMetrics {
	return &DealPipelineMetrics{
		dealsProposed: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "deals",
			Name:      "proposed_total",
			Help:      "Total number of deals proposed to storage providers",
		}, []string{"provider"}),

		dealsAccepted: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "deals",
			Name:      "accepted_total",
			Help:      "Total number of deals accepted by storage providers",
		}, []string{"provider"}),

		dealsRejected: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "deals",
			Name:      "rejected_total",
			Help:      "Total number of deals rejected by storage providers",
		}, []string{"provider", "reason"}),

		dealsPublished: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "deals",
			Name:      "published_total",
			Help:      "Total number of deals published on chain",
		}, []string{"provider"}),

		dealsSealed: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "deals",
			Name:      "sealed_total",
			Help:      "Total number of deals sealed by storage providers",
		}, []string{"provider"}),

		dealsFailed: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "deals",
			Name:      "failed_total",
			Help:      "Total number of failed deals",
		}, []string{"reason"}),

		dealsExpired: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "deals",
			Name:      "expired_total",
			Help:      "Total number of expired deals",
		}),

		dealsActiveGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "deals",
			Name:      "active",
			Help:      "Current number of active (sealed) deals",
		}),

		dealsInProgressGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "deals",
			Name:      "in_progress",
			Help:      "Current number of deals in progress (proposed but not sealed)",
		}),

		dealsPendingGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "deals",
			Name:      "pending",
			Help:      "Current number of deals pending proposal",
		}),

		proposalDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: "fgw",
			Subsystem: "deals",
			Name:      "proposal_duration_seconds",
			Help:      "Time from deal creation to proposal acceptance",
			Buckets:   []float64{1, 5, 10, 30, 60, 120, 300, 600},
		}),

		publishDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: "fgw",
			Subsystem: "deals",
			Name:      "publish_duration_seconds",
			Help:      "Time from proposal acceptance to on-chain publish",
			Buckets:   []float64{60, 300, 600, 1800, 3600, 7200, 14400},
		}),

		sealingDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: "fgw",
			Subsystem: "deals",
			Name:      "sealing_duration_seconds",
			Help:      "Time from publish to seal completion",
			Buckets:   []float64{3600, 7200, 14400, 28800, 57600, 86400, 172800},
		}),

		totalDealDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: "fgw",
			Subsystem: "deals",
			Name:      "total_duration_seconds",
			Help:      "Total time from deal creation to seal completion",
			Buckets:   []float64{3600, 7200, 14400, 28800, 57600, 86400, 172800, 345600},
		}),

		providerSelectionDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: "fgw",
			Subsystem: "deals",
			Name:      "provider_selection_duration_seconds",
			Help:      "Time to select storage providers for a deal",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
		}),

		providersQueried: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "deals",
			Name:      "providers_queried_total",
			Help:      "Total number of providers queried for deals",
		}),

		providersSelected: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "deals",
			Name:      "providers_selected_total",
			Help:      "Total number of providers selected for deals",
		}),

		groupsNeedingDeals: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "deals",
			Name:      "groups_needing_deals",
			Help:      "Number of groups that need more deals",
		}),

		groupsReadyForDeals: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "deals",
			Name:      "groups_ready_for_deals",
			Help:      "Number of groups ready to make deals (have CommP)",
		}),

		groupsOffloaded: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "deals",
			Name:      "groups_offloaded",
			Help:      "Number of groups that have been offloaded to Filecoin",
		}),
	}
}

// IncProposed increments the proposed deals counter.
func (m *DealPipelineMetrics) IncProposed(provider string) {
	m.dealsProposed.WithLabelValues(provider).Inc()
}

// IncAccepted increments the accepted deals counter.
func (m *DealPipelineMetrics) IncAccepted(provider string) {
	m.dealsAccepted.WithLabelValues(provider).Inc()
}

// IncRejected increments the rejected deals counter.
func (m *DealPipelineMetrics) IncRejected(provider, reason string) {
	m.dealsRejected.WithLabelValues(provider, reason).Inc()
}

// IncPublished increments the published deals counter.
func (m *DealPipelineMetrics) IncPublished(provider string) {
	m.dealsPublished.WithLabelValues(provider).Inc()
}

// IncSealed increments the sealed deals counter.
func (m *DealPipelineMetrics) IncSealed(provider string) {
	m.dealsSealed.WithLabelValues(provider).Inc()
}

// IncFailed increments the failed deals counter.
func (m *DealPipelineMetrics) IncFailed(reason string) {
	m.dealsFailed.WithLabelValues(reason).Inc()
}

// IncExpired increments the expired deals counter.
func (m *DealPipelineMetrics) IncExpired() {
	m.dealsExpired.Inc()
}

// SetActiveDeals sets the gauge for active deals.
func (m *DealPipelineMetrics) SetActiveDeals(count float64) {
	m.dealsActiveGauge.Set(count)
}

// SetInProgressDeals sets the gauge for in-progress deals.
func (m *DealPipelineMetrics) SetInProgressDeals(count float64) {
	m.dealsInProgressGauge.Set(count)
}

// SetPendingDeals sets the gauge for pending deals.
func (m *DealPipelineMetrics) SetPendingDeals(count float64) {
	m.dealsPendingGauge.Set(count)
}

// ObserveProposalDuration records the time taken for a proposal.
func (m *DealPipelineMetrics) ObserveProposalDuration(d time.Duration) {
	m.proposalDuration.Observe(d.Seconds())
}

// ObservePublishDuration records the time taken for publishing.
func (m *DealPipelineMetrics) ObservePublishDuration(d time.Duration) {
	m.publishDuration.Observe(d.Seconds())
}

// ObserveSealingDuration records the time taken for sealing.
func (m *DealPipelineMetrics) ObserveSealingDuration(d time.Duration) {
	m.sealingDuration.Observe(d.Seconds())
}

// ObserveTotalDealDuration records the total deal time.
func (m *DealPipelineMetrics) ObserveTotalDealDuration(d time.Duration) {
	m.totalDealDuration.Observe(d.Seconds())
}

// ObserveProviderSelectionDuration records provider selection time.
func (m *DealPipelineMetrics) ObserveProviderSelectionDuration(d time.Duration) {
	m.providerSelectionDuration.Observe(d.Seconds())
}

// AddProvidersQueried adds to the providers queried counter.
func (m *DealPipelineMetrics) AddProvidersQueried(count int) {
	m.providersQueried.Add(float64(count))
}

// AddProvidersSelected adds to the providers selected counter.
func (m *DealPipelineMetrics) AddProvidersSelected(count int) {
	m.providersSelected.Add(float64(count))
}

// SetGroupsNeedingDeals sets the gauge for groups needing deals.
func (m *DealPipelineMetrics) SetGroupsNeedingDeals(count float64) {
	m.groupsNeedingDeals.Set(count)
}

// SetGroupsReadyForDeals sets the gauge for groups ready for deals.
func (m *DealPipelineMetrics) SetGroupsReadyForDeals(count float64) {
	m.groupsReadyForDeals.Set(count)
}

// SetGroupsOffloaded sets the gauge for offloaded groups.
func (m *DealPipelineMetrics) SetGroupsOffloaded(count float64) {
	m.groupsOffloaded.Set(count)
}

// Global instance for deal pipeline metrics
var dealPipelineMetrics *DealPipelineMetrics

func init() {
	dealPipelineMetrics = NewDealPipelineMetrics()
}

// GetDealPipelineMetrics returns the global deal pipeline metrics instance.
func GetDealPipelineMetrics() *DealPipelineMetrics {
	return dealPipelineMetrics
}
