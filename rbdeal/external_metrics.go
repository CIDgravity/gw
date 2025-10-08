package rbdeal

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type externalStorageMetrics struct {
	uploadedBytes, uploadsStarted, uploadsDone, uploadErr, readReqs, readBytes, readErr, deleteReqs, deleteErr *prometheus.CounterVec
	uploadsWaiting, staging                                                                                    *prometheus.GaugeVec
}

func newExternalStorageMetrics() *externalStorageMetrics {
	return &externalStorageMetrics{
		uploadedBytes: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "external_storage",
			Name:      "bytes_uploaded_total",
		}, []string{"module"}),
		uploadsStarted: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "external_storage",
			Name:      "uploads_started_total",
		}, []string{"module"}),
		uploadsDone: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "external_storage",
			Name:      "uploads_done_total",
		}, []string{"module"}),
		uploadErr: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "external_storage",
			Name:      "upload_errors_total",
		}, []string{"module"}),
		readReqs: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "external_storage",
			Name:      "read_requests_total",
		}, []string{"module"}),
		readBytes: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "external_storage",
			Name:      "bytes_read_total",
		}, []string{"module"}),
		readErr: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "external_storage",
			Name:      "read_errors_total",
		}, []string{"module"}),
		deleteReqs: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "external_storage",
			Name:      "delete_requests_total",
		}, []string{"module"}),
		deleteErr: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "external_storage",
			Name:      "delete_errors_total",
		}, []string{"module"}),
		uploadsWaiting: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "external_storage",
			Name:      "uploads_waiting",
		}, []string{"module"}),
		staging: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "external_storage",
			Name:      "staging",
		}, []string{"module"}),
	}
}

type ExternalStorageModuleMetrics struct {
	uploadedBytes, uploadsStarted, uploadsDone, uploadErr, readReqs, readBytes, readErr, deleteReqs, deleteErr prometheus.Counter
	uploadsWaiting, staging                                                                                    prometheus.Gauge
}

func (m *externalStorageMetrics) forModule(module string) *ExternalStorageModuleMetrics {
	return &ExternalStorageModuleMetrics{
		uploadedBytes:  m.uploadedBytes.WithLabelValues(module),
		uploadsStarted: m.uploadsStarted.WithLabelValues(module),
		uploadsDone:    m.uploadsDone.WithLabelValues(module),
		uploadsWaiting: m.uploadsWaiting.WithLabelValues(module),
		uploadErr:      m.uploadErr.WithLabelValues(module),
		readReqs:       m.readReqs.WithLabelValues(module),
		readBytes:      m.readBytes.WithLabelValues(module),
		readErr:        m.readErr.WithLabelValues(module),
		deleteReqs:     m.deleteReqs.WithLabelValues(module),
		deleteErr:      m.deleteErr.WithLabelValues(module),
		staging:        m.staging.WithLabelValues(module),
	}
}
