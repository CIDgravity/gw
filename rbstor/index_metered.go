package rbstor

import (
	"context"
	"sync/atomic"

	"github.com/multiformats/go-multihash"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	iface "github.com/aurorainfra/gw"
)

const (
	write string = "write"
	read  string = "read"
)

type MeteredIndex struct {
	sub iface.Index

	counters *prometheus.CounterVec
	reads    int64
	writes   int64
}

func (m *MeteredIndex) Close() error {
	return m.sub.Close()
}

func (m *MeteredIndex) EstimateSize(ctx context.Context) (int64, error) {
	return m.sub.EstimateSize(ctx)
}

func (m *MeteredIndex) GetGroups(ctx context.Context, mh []multihash.Multihash, cb func(cidx int, gk iface.GroupKey) (more bool, err error)) error {
	m.incrementCounter("GetGroups", read)
	return m.sub.GetGroups(ctx, mh, cb)
}

func (m *MeteredIndex) GetSizes(ctx context.Context, mh []multihash.Multihash, cb func([]int32) error) error {
	m.incrementCounter("GetSizes", read)
	return m.sub.GetSizes(ctx, mh, cb)
}

func (m *MeteredIndex) AddGroup(ctx context.Context, mh []multihash.Multihash, sizes []int32, group iface.GroupKey) error {
	m.incrementCounter("AddGroup", write)
	return m.sub.AddGroup(ctx, mh, sizes, group)
}

func (m *MeteredIndex) Sync(ctx context.Context) error {
	return m.sub.Sync(ctx)
}

func (m *MeteredIndex) DropGroup(ctx context.Context, mh []multihash.Multihash, group iface.GroupKey) error {
	m.incrementCounter("DropGroup", write)
	return m.sub.DropGroup(ctx, mh, group)
}

func (m *MeteredIndex) incrementCounter(operation string, t string) {
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
}

func NewMeteredIndex(sub iface.Index) *MeteredIndex {
	return &MeteredIndex{sub: sub,
		counters: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "agw",
			Subsystem: "group_index",
			Name:      "operations_total",
		}, []string{"operation", "type"}),
	}
}

var _ iface.Index = &MeteredIndex{}
