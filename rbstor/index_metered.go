package rbstor

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/multiformats/go-multihash"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	write string = "write"
	read  string = "read"
)

// Metrics are registered once and shared across all MeteredIndex instances
var (
	metricsOnce         sync.Once
	groupIndexCounters  *prometheus.CounterVec
	groupIndexDurations *prometheus.HistogramVec
)

func initMetrics() {
	metricsOnce.Do(func() {
		groupIndexCounters = promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "fgw",
			Subsystem: "group_index",
			Name:      "operations_total",
		}, []string{"operation", "type"})

		groupIndexDurations = promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "fgw",
			Subsystem: "group_index",
			Name:      "request_duration_seconds",
			Help:      "Duration of the group index requests.",
			Buckets:   prometheus.DefBuckets,
		},
			[]string{"operation", "type"},
		)
	})
}

type MeteredIndex struct {
	sub iface.GroupIndex

	counters  *prometheus.CounterVec
	durations *prometheus.HistogramVec

	reads  int64
	writes int64
}

func (m *MeteredIndex) Close() error {
	return m.sub.Close()
}

func (m *MeteredIndex) EstimateSize(ctx context.Context) (int64, error) {
	timer := m.incrementCounter("EstimateSize", read)
	defer timer.ObserveDuration()
	return m.sub.EstimateSize(ctx)
}

func (m *MeteredIndex) GetGroups(ctx context.Context, mh []multihash.Multihash, cb func(cidx int, gk iface.GroupKey) (more bool, err error)) error {
	timer := m.incrementCounter("GetGroups", read)
	defer timer.ObserveDuration()
	return m.sub.GetGroups(ctx, mh, cb)
}

func (m *MeteredIndex) GetSizes(ctx context.Context, mh []multihash.Multihash, cb func([]int32) error) error {
	timer := m.incrementCounter("GetSizes", read)
	defer timer.ObserveDuration()
	return m.sub.GetSizes(ctx, mh, cb)
}

func (m *MeteredIndex) AddGroup(ctx context.Context, mh []multihash.Multihash, sizes []int32, group iface.GroupKey) error {
	timer := m.incrementCounter("AddGroup", write)
	defer timer.ObserveDuration()
	return m.sub.AddGroup(ctx, mh, sizes, group)
}

func (m *MeteredIndex) Sync(ctx context.Context) error {
	return m.sub.Sync(ctx)
}

func (m *MeteredIndex) DropGroup(ctx context.Context, mh []multihash.Multihash, group iface.GroupKey) error {
	timer := m.incrementCounter("DropGroup", write)
	defer timer.ObserveDuration()
	return m.sub.DropGroup(ctx, mh, group)
}

func (m *MeteredIndex) incrementCounter(operation string, t string) *prometheus.Timer {
	switch t {
	case read:
		atomic.AddInt64(&m.reads, 1)
	case write:
		atomic.AddInt64(&m.writes, 1)
	}

	m.counters.With(prometheus.Labels{
		"operation": operation,
		"type":      t,
	}).Inc()

	return prometheus.NewTimer(m.durations.With(prometheus.Labels{
		"operation": operation,
		"type":      t,
	}))
}

func NewMeteredIndex(sub iface.GroupIndex) *MeteredIndex {
	initMetrics()
	return &MeteredIndex{
		sub:       sub,
		counters:  groupIndexCounters,
		durations: groupIndexDurations,
	}
}

var _ iface.GroupIndex = &MeteredIndex{}
