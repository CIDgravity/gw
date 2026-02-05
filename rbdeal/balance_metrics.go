package rbdeal

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// BalanceMetrics tracks wallet, market, and datacap balance metrics.
type BalanceMetrics struct {
	// Balance gauges
	walletBalanceFil      prometheus.Gauge
	marketBalanceFil      prometheus.Gauge
	datacapRemainingBytes prometheus.Gauge

	// Top-up counters
	marketTopupTotal      prometheus.Counter
	marketTopupAmountFil  prometheus.Counter
	faucetRequestsTotal   *prometheus.CounterVec
	faucetRequestsSuccess prometheus.Counter
	faucetRequestsFailed  prometheus.Counter

	// Datacap counters
	datacapRequestsTotal   prometheus.Counter
	datacapRequestsSuccess prometheus.Counter
	datacapRequestsFailed  prometheus.Counter
}

// NewBalanceMetrics creates a new BalanceMetrics instance.
func NewBalanceMetrics() *BalanceMetrics {
	return &BalanceMetrics{
		walletBalanceFil: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "balance",
			Name:      "wallet_fil",
			Help:      "Current wallet balance in FIL",
		}),

		marketBalanceFil: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "balance",
			Name:      "market_fil",
			Help:      "Current market escrow balance in FIL",
		}),

		datacapRemainingBytes: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "balance",
			Name:      "datacap_bytes",
			Help:      "Remaining datacap in bytes",
		}),

		marketTopupTotal: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "balance",
			Name:      "market_topup_total",
			Help:      "Total number of market balance top-ups",
		}),

		marketTopupAmountFil: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "balance",
			Name:      "market_topup_fil_total",
			Help:      "Total FIL amount added to market balance",
		}),

		faucetRequestsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "balance",
			Name:      "faucet_requests_total",
			Help:      "Total faucet requests by type and status",
		}, []string{"type", "status"}),

		faucetRequestsSuccess: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "balance",
			Name:      "faucet_fil_success_total",
			Help:      "Total successful FIL faucet requests",
		}),

		faucetRequestsFailed: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "balance",
			Name:      "faucet_fil_failed_total",
			Help:      "Total failed FIL faucet requests",
		}),

		datacapRequestsTotal: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "balance",
			Name:      "datacap_requests_total",
			Help:      "Total datacap faucet requests",
		}),

		datacapRequestsSuccess: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "balance",
			Name:      "datacap_requests_success_total",
			Help:      "Total successful datacap requests",
		}),

		datacapRequestsFailed: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "balance",
			Name:      "datacap_requests_failed_total",
			Help:      "Total failed datacap requests",
		}),
	}
}

// SetWalletBalance sets the wallet balance gauge.
func (m *BalanceMetrics) SetWalletBalance(fil float64) {
	m.walletBalanceFil.Set(fil)
}

// SetMarketBalance sets the market balance gauge.
func (m *BalanceMetrics) SetMarketBalance(fil float64) {
	m.marketBalanceFil.Set(fil)
}

// SetDatacapRemaining sets the datacap remaining gauge.
func (m *BalanceMetrics) SetDatacapRemaining(bytes float64) {
	m.datacapRemainingBytes.Set(bytes)
}

// IncMarketTopup increments the market top-up counter.
func (m *BalanceMetrics) IncMarketTopup(amountFil float64) {
	m.marketTopupTotal.Inc()
	m.marketTopupAmountFil.Add(amountFil)
}

// IncFaucetRequest increments faucet request counters.
func (m *BalanceMetrics) IncFaucetRequest(requestType string, success bool) {
	status := "success"
	if !success {
		status = "failed"
	}
	m.faucetRequestsTotal.WithLabelValues(requestType, status).Inc()

	if requestType == "fil" {
		if success {
			m.faucetRequestsSuccess.Inc()
		} else {
			m.faucetRequestsFailed.Inc()
		}
	} else if requestType == "datacap" {
		m.datacapRequestsTotal.Inc()
		if success {
			m.datacapRequestsSuccess.Inc()
		} else {
			m.datacapRequestsFailed.Inc()
		}
	}
}

// Global instance for balance metrics
var balanceMetrics *BalanceMetrics

func init() {
	balanceMetrics = NewBalanceMetrics()
}

// GetBalanceMetrics returns the global balance metrics instance.
func GetBalanceMetrics() *BalanceMetrics {
	return balanceMetrics
}
