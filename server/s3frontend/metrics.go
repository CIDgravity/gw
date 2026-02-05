package s3frontend

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// FrontendMetrics tracks S3 frontend proxy metrics.
type FrontendMetrics struct {
	// Request metrics
	requestsTotal    *prometheus.CounterVec
	requestDuration  *prometheus.HistogramVec
	requestsInFlight prometheus.Gauge

	// Backend metrics
	backendHealth   *prometheus.GaugeVec
	backendRequests *prometheus.CounterVec
	backendDuration *prometheus.HistogramVec
	backendErrors   *prometheus.CounterVec

	// Routing metrics
	routingLookups       prometheus.Counter
	routingLookupErrors  prometheus.Counter
	routingLookupLatency prometheus.Histogram
	routingCacheHits     prometheus.Counter
	routingCacheMisses   prometheus.Counter

	// Backend pool metrics
	backendsTotal     prometheus.Gauge
	backendsHealthy   prometheus.Gauge
	backendsUnhealthy prometheus.Gauge

	// Multipart metrics
	multipartUploadsActive   prometheus.Gauge
	multipartUploadsStarted  prometheus.Counter
	multipartUploadsComplete prometheus.Counter
	multipartUploadsAborted  prometheus.Counter
}

// NewFrontendMetrics creates a new FrontendMetrics instance.
func NewFrontendMetrics() *FrontendMetrics {
	latencyBuckets := []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}
	lookupBuckets := []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1}

	return &FrontendMetrics{
		requestsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "s3frontend",
			Name:      "requests_total",
			Help:      "Total S3 frontend requests",
		}, []string{"method", "status"}),

		requestDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "fgw",
			Subsystem: "s3frontend",
			Name:      "request_duration_seconds",
			Help:      "Duration of S3 frontend requests",
			Buckets:   latencyBuckets,
		}, []string{"method"}),

		requestsInFlight: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "s3frontend",
			Name:      "requests_in_flight",
			Help:      "Current number of S3 frontend requests in flight",
		}),

		backendHealth: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "s3frontend",
			Name:      "backend_health",
			Help:      "Backend health status (1=healthy, 0=unhealthy)",
		}, []string{"node_id"}),

		backendRequests: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "s3frontend",
			Name:      "backend_requests_total",
			Help:      "Total requests sent to each backend",
		}, []string{"node_id", "method"}),

		backendDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "fgw",
			Subsystem: "s3frontend",
			Name:      "backend_duration_seconds",
			Help:      "Duration of requests to backends",
			Buckets:   latencyBuckets,
		}, []string{"node_id"}),

		backendErrors: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "s3frontend",
			Name:      "backend_errors_total",
			Help:      "Total errors from backends",
		}, []string{"node_id", "type"}),

		routingLookups: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "s3frontend",
			Name:      "routing_lookups_total",
			Help:      "Total object routing lookups",
		}),

		routingLookupErrors: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "s3frontend",
			Name:      "routing_lookup_errors_total",
			Help:      "Total routing lookup errors",
		}),

		routingLookupLatency: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: "fgw",
			Subsystem: "s3frontend",
			Name:      "routing_lookup_duration_seconds",
			Help:      "Duration of routing lookups",
			Buckets:   lookupBuckets,
		}),

		routingCacheHits: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "s3frontend",
			Name:      "routing_cache_hits_total",
			Help:      "Total routing cache hits",
		}),

		routingCacheMisses: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "s3frontend",
			Name:      "routing_cache_misses_total",
			Help:      "Total routing cache misses",
		}),

		backendsTotal: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "s3frontend",
			Name:      "backends_total",
			Help:      "Total number of configured backends",
		}),

		backendsHealthy: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "s3frontend",
			Name:      "backends_healthy",
			Help:      "Number of healthy backends",
		}),

		backendsUnhealthy: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "s3frontend",
			Name:      "backends_unhealthy",
			Help:      "Number of unhealthy backends",
		}),

		multipartUploadsActive: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "s3frontend",
			Name:      "multipart_uploads_active",
			Help:      "Number of active multipart uploads",
		}),

		multipartUploadsStarted: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "s3frontend",
			Name:      "multipart_uploads_started_total",
			Help:      "Total multipart uploads started",
		}),

		multipartUploadsComplete: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "s3frontend",
			Name:      "multipart_uploads_complete_total",
			Help:      "Total multipart uploads completed",
		}),

		multipartUploadsAborted: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "s3frontend",
			Name:      "multipart_uploads_aborted_total",
			Help:      "Total multipart uploads aborted",
		}),
	}
}

// RecordRequest records a frontend request.
func (m *FrontendMetrics) RecordRequest(method string, status int, duration time.Duration) {
	m.requestsTotal.WithLabelValues(method, strconv.Itoa(status)).Inc()
	m.requestDuration.WithLabelValues(method).Observe(duration.Seconds())
}

// IncRequestsInFlight increments in-flight requests counter.
func (m *FrontendMetrics) IncRequestsInFlight() {
	m.requestsInFlight.Inc()
}

// DecRequestsInFlight decrements in-flight requests counter.
func (m *FrontendMetrics) DecRequestsInFlight() {
	m.requestsInFlight.Dec()
}

// SetBackendHealth sets the health status for a backend.
func (m *FrontendMetrics) SetBackendHealth(nodeID string, healthy bool) {
	val := 0.0
	if healthy {
		val = 1.0
	}
	m.backendHealth.WithLabelValues(nodeID).Set(val)
}

// RecordBackendRequest records a request to a backend.
func (m *FrontendMetrics) RecordBackendRequest(nodeID, method string, duration time.Duration, err error) {
	m.backendRequests.WithLabelValues(nodeID, method).Inc()
	m.backendDuration.WithLabelValues(nodeID).Observe(duration.Seconds())
	if err != nil {
		m.backendErrors.WithLabelValues(nodeID, "request_error").Inc()
	}
}

// RecordBackendError records a backend error.
func (m *FrontendMetrics) RecordBackendError(nodeID, errorType string) {
	m.backendErrors.WithLabelValues(nodeID, errorType).Inc()
}

// RecordRoutingLookup records a routing lookup.
func (m *FrontendMetrics) RecordRoutingLookup(duration time.Duration, err error) {
	m.routingLookups.Inc()
	m.routingLookupLatency.Observe(duration.Seconds())
	if err != nil {
		m.routingLookupErrors.Inc()
	}
}

// RecordRoutingCacheHit records a routing cache hit.
func (m *FrontendMetrics) RecordRoutingCacheHit() {
	m.routingCacheHits.Inc()
}

// RecordRoutingCacheMiss records a routing cache miss.
func (m *FrontendMetrics) RecordRoutingCacheMiss() {
	m.routingCacheMisses.Inc()
}

// SetBackendCounts sets the backend count gauges.
func (m *FrontendMetrics) SetBackendCounts(total, healthy, unhealthy int) {
	m.backendsTotal.Set(float64(total))
	m.backendsHealthy.Set(float64(healthy))
	m.backendsUnhealthy.Set(float64(unhealthy))
}

// SetMultipartUploadsActive sets the active multipart uploads gauge.
func (m *FrontendMetrics) SetMultipartUploadsActive(count int) {
	m.multipartUploadsActive.Set(float64(count))
}

// IncMultipartStarted increments the started uploads counter.
func (m *FrontendMetrics) IncMultipartStarted() {
	m.multipartUploadsStarted.Inc()
}

// IncMultipartComplete increments the completed uploads counter.
func (m *FrontendMetrics) IncMultipartComplete() {
	m.multipartUploadsComplete.Inc()
}

// IncMultipartAborted increments the aborted uploads counter.
func (m *FrontendMetrics) IncMultipartAborted() {
	m.multipartUploadsAborted.Inc()
}

// Global instance for frontend metrics
var frontendMetrics *FrontendMetrics

func init() {
	frontendMetrics = NewFrontendMetrics()
}

// GetFrontendMetrics returns the global frontend metrics instance.
func GetFrontendMetrics() *FrontendMetrics {
	return frontendMetrics
}

// MetricsMiddleware wraps an http.Handler with metrics collection.
func MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		frontendMetrics.IncRequestsInFlight()
		defer frontendMetrics.DecRequestsInFlight()

		start := time.Now()

		// Wrap response writer to capture status code
		rw := &responseWriter{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(rw, r)

		frontendMetrics.RecordRequest(r.Method, rw.status, time.Since(start))
	})
}

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}
