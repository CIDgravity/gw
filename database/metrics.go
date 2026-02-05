package database

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// DBMetrics tracks database operation metrics for both SQL and CQL.
type DBMetrics struct {
	// SQL operation metrics
	sqlQueryDuration *prometheus.HistogramVec
	sqlQueryErrors   *prometheus.CounterVec
	sqlQueryTotal    *prometheus.CounterVec

	// CQL operation metrics
	cqlQueryDuration *prometheus.HistogramVec
	cqlQueryErrors   *prometheus.CounterVec
	cqlQueryTotal    *prometheus.CounterVec

	// Batch operation metrics
	cqlBatchSize     prometheus.Histogram
	cqlBatchDuration prometheus.Histogram
	cqlBatchErrors   prometheus.Counter

	// Connection pool metrics
	sqlActiveConns prometheus.Gauge
	sqlIdleConns   prometheus.Gauge
	cqlActiveConns prometheus.Gauge
}

// NewDBMetrics creates a new DBMetrics instance.
func NewDBMetrics() *DBMetrics {
	buckets := []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	batchBuckets := []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}

	return &DBMetrics{
		sqlQueryDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "fgw",
			Subsystem: "sql",
			Name:      "query_duration_seconds",
			Help:      "Duration of SQL queries in seconds",
			Buckets:   buckets,
		}, []string{"operation"}),

		sqlQueryErrors: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "sql",
			Name:      "query_errors_total",
			Help:      "Total number of SQL query errors",
		}, []string{"operation"}),

		sqlQueryTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "sql",
			Name:      "queries_total",
			Help:      "Total number of SQL queries",
		}, []string{"operation"}),

		cqlQueryDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "fgw",
			Subsystem: "cql",
			Name:      "query_duration_seconds",
			Help:      "Duration of CQL queries in seconds",
			Buckets:   buckets,
		}, []string{"operation"}),

		cqlQueryErrors: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "cql",
			Name:      "query_errors_total",
			Help:      "Total number of CQL query errors",
		}, []string{"operation"}),

		cqlQueryTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "cql",
			Name:      "queries_total",
			Help:      "Total number of CQL queries",
		}, []string{"operation"}),

		cqlBatchSize: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: "fgw",
			Subsystem: "cql",
			Name:      "batch_size",
			Help:      "Number of statements in CQL batches",
			Buckets:   []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
		}),

		cqlBatchDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: "fgw",
			Subsystem: "cql",
			Name:      "batch_duration_seconds",
			Help:      "Duration of CQL batch operations",
			Buckets:   batchBuckets,
		}),

		cqlBatchErrors: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "cql",
			Name:      "batch_errors_total",
			Help:      "Total number of CQL batch errors",
		}),

		sqlActiveConns: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "sql",
			Name:      "active_connections",
			Help:      "Number of active SQL connections",
		}),

		sqlIdleConns: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "sql",
			Name:      "idle_connections",
			Help:      "Number of idle SQL connections",
		}),

		cqlActiveConns: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "cql",
			Name:      "active_connections",
			Help:      "Number of active CQL connections",
		}),
	}
}

// ObserveSQLQuery records a SQL query metric.
func (m *DBMetrics) ObserveSQLQuery(operation string, duration time.Duration, err error) {
	m.sqlQueryDuration.WithLabelValues(operation).Observe(duration.Seconds())
	m.sqlQueryTotal.WithLabelValues(operation).Inc()
	if err != nil {
		m.sqlQueryErrors.WithLabelValues(operation).Inc()
	}
}

// ObserveCQLQuery records a CQL query metric.
func (m *DBMetrics) ObserveCQLQuery(operation string, duration time.Duration, err error) {
	m.cqlQueryDuration.WithLabelValues(operation).Observe(duration.Seconds())
	m.cqlQueryTotal.WithLabelValues(operation).Inc()
	if err != nil {
		m.cqlQueryErrors.WithLabelValues(operation).Inc()
	}
}

// ObserveCQLBatch records a CQL batch operation.
func (m *DBMetrics) ObserveCQLBatch(size int, duration time.Duration, err error) {
	m.cqlBatchSize.Observe(float64(size))
	m.cqlBatchDuration.Observe(duration.Seconds())
	if err != nil {
		m.cqlBatchErrors.Inc()
	}
}

// SetSQLConnections updates SQL connection pool metrics.
func (m *DBMetrics) SetSQLConnections(active, idle int) {
	m.sqlActiveConns.Set(float64(active))
	m.sqlIdleConns.Set(float64(idle))
}

// SetCQLConnections updates CQL connection pool metrics.
func (m *DBMetrics) SetCQLConnections(active int) {
	m.cqlActiveConns.Set(float64(active))
}

// Global instance for database metrics
var dbMetrics *DBMetrics

func init() {
	dbMetrics = NewDBMetrics()
}

// GetDBMetrics returns the global database metrics instance.
func GetDBMetrics() *DBMetrics {
	return dbMetrics
}

// SQLQueryTimer provides a convenient way to time SQL queries.
type SQLQueryTimer struct {
	operation string
	start     time.Time
}

// NewSQLQueryTimer starts timing a SQL query.
func NewSQLQueryTimer(operation string) *SQLQueryTimer {
	return &SQLQueryTimer{
		operation: operation,
		start:     time.Now(),
	}
}

// Done finishes timing and records the metric.
func (t *SQLQueryTimer) Done(err error) {
	dbMetrics.ObserveSQLQuery(t.operation, time.Since(t.start), err)
}

// CQLQueryTimer provides a convenient way to time CQL queries.
type CQLQueryTimer struct {
	operation string
	start     time.Time
}

// NewCQLQueryTimer starts timing a CQL query.
func NewCQLQueryTimer(operation string) *CQLQueryTimer {
	return &CQLQueryTimer{
		operation: operation,
		start:     time.Now(),
	}
}

// Done finishes timing and records the metric.
func (t *CQLQueryTimer) Done(err error) {
	dbMetrics.ObserveCQLQuery(t.operation, time.Since(t.start), err)
}
