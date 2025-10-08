package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func GetCounterValue(metric prometheus.Counter) float64 {
	var m = &dto.Metric{}
	if err := metric.Write(m); err != nil {
		log.Error(err)
		return 0
	}
	return m.Counter.GetValue()
}

func GetGaugeValue(metric prometheus.Gauge) float64 {
	var m = &dto.Metric{}
	if err := metric.Write(m); err != nil {
		log.Error(err)
		return 0
	}
	return m.Gauge.GetValue()
}
