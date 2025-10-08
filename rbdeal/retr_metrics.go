package rbdeal

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type RetrievalMetrics struct {
	success, bytes, fail, cacheHit, cacheMiss, httpTries, httpSuccess, httpBytes prometheus.Counter
}

func newRetrievalMetrics() *RetrievalMetrics {
	return &RetrievalMetrics{
		success: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "deal",
			Name:      "retrieval_success_total",
		}),
		bytes: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "deal",
			Name:      "retrieval_bytes_total",
		}),
		fail: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "deal",
			Name:      "retrieval_fail_total",
		}),
		cacheHit: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "deal",
			Name:      "retrieval_cache_hit_total",
		}),
		cacheMiss: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "deal",
			Name:      "retrieval_cache_miss_total",
		}),
		httpTries: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "deal",
			Name:      "retrieval_http_tries_total",
		}),
		httpSuccess: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "deal",
			Name:      "retrieval_http_success_total",
		}),
		httpBytes: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "deal",
			Name:      "retrieval_http_bytes_total",
		}),
	}
}

func (r *RetrievalMetrics) AddCacheHits(hits int64) {
	r.cacheHit.Add(float64(hits))
	r.success.Add(float64(hits))
}

func (r *RetrievalMetrics) AddCacheMisses(misses int64) {
	r.cacheMiss.Add(float64(misses))
}

func (r *RetrievalMetrics) IncHttpTries() {
	r.httpTries.Inc()
}

func (r *RetrievalMetrics) IncHttpSuccess(bytes int64) {
	r.httpSuccess.Inc()
	r.success.Inc()
	r.httpBytes.Add(float64(bytes))
}

func (r *RetrievalMetrics) AddBytesTotal(bytes int64) {
	r.bytes.Add(float64(bytes))
}

func (r *RetrievalMetrics) AddFailed(failed int64) {
	r.fail.Add(float64(failed))
}

type retrievalCheckMetrics struct {
	todo, started, success, failed    prometheus.Gauge
	startedAll, successAll, failedAll prometheus.Counter
}

func newRetrievalCheckMetrics() *retrievalCheckMetrics {
	return &retrievalCheckMetrics{
		todo: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "deal",
			Name:      "retrieval_check_to_do",
		}),
		started: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "deal",
			Name:      "retrieval_check_started",
		}),
		success: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "deal",
			Name:      "retrieval_check_success",
		}),
		failed: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "fgw",
			Subsystem: "deal",
			Name:      "retrieval_check_failed",
		}),
		startedAll: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "deal",
			Name:      "retrieval_check_started_total",
		}),
		successAll: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "deal",
			Name:      "retrieval_check_success_total",
		}),
		failedAll: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "deal",
			Name:      "retrieval_check_failed_total",
		}),
	}
}

func (r *retrievalCheckMetrics) Begin(todo int64) {
	r.todo.Set(float64(todo))
	r.success.Set(0)
	r.started.Set(0)
	r.failed.Set(0)
}

func (r *retrievalCheckMetrics) IncStarted() {
	r.started.Inc()
	r.startedAll.Inc()
}

func (r *retrievalCheckMetrics) IncSuccess() {
	r.success.Inc()
	r.successAll.Inc()
}

func (r *retrievalCheckMetrics) IncFailed() {
	r.failed.Inc()
	r.failedAll.Inc()
}
