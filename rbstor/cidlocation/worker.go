package cidlocation

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	iface2 "github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/CIDgravity/filecoin-gateway/integrations/blockstore"
	"github.com/CIDgravity/filecoin-gateway/rbstor"
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multihash"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/fx"
)

type Metrics struct {
	cidsScheduled                  prometheus.Counter
	cidsProcessed                  prometheus.Counter
	queueSize                      prometheus.Gauge
	cidProcessingErrors            prometheus.Counter
	groupQueries                   prometheus.Counter
	offloadStatusQueries           prometheus.Counter
	groupQueriesWithRecovery       prometheus.Counter
	objectRawNodesCrawled          prometheus.Counter
	objectIntermediateNodesCrawled prometheus.Counter
}

type Worker struct {
	dag           format.DAGService
	groupIndex    iface2.GroupIndex
	groupdb       *rbstor.RbsDB
	locationIndex LocationIndex
	metrics       Metrics

	queue chan cid.Cid
}

func NewWorker(rbs *ribsbstore.Blockstore, groupIndex iface2.GroupIndex, locationIndex LocationIndex, groupdb *rbstor.RbsDB) *Worker {
	bsv := blockservice.New(rbs, offline.Exchange(rbs))

	return &Worker{
		dag:           merkledag.NewDAGService(bsv),
		groupIndex:    groupIndex,
		groupdb:       groupdb,
		locationIndex: locationIndex,
		queue:         make(chan cid.Cid, 10000),
		metrics: Metrics{
			cidsScheduled: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "fgw",
				Subsystem: "cidlocation",
				Name:      "scheduled_cids_total",
			}),
			cidsProcessed: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "fgw",
				Subsystem: "cidlocation",
				Name:      "processed_cids_total",
			}),
			queueSize: promauto.NewGauge(prometheus.GaugeOpts{
				Namespace: "fgw",
				Subsystem: "cidlocation",
				Name:      "queue_size",
			}),
			cidProcessingErrors: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "fgw",
				Subsystem: "cidlocation",
				Name:      "cid_processing_errors_total",
			}),
			groupQueries: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "fgw",
				Subsystem: "cidlocation",
				Name:      "group_queries_total",
			}),
			offloadStatusQueries: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "fgw",
				Subsystem: "cidlocation",
				Name:      "offload_status_queries_total",
			}),
			groupQueriesWithRecovery: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "fgw",
				Subsystem: "cidlocation",
				Name:      "queries_with_recovery_total",
			}),
			objectRawNodesCrawled: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "fgw",
				Subsystem: "cidlocation",
				Name:      "object_raw_nodes_crawled_total",
			}),
			objectIntermediateNodesCrawled: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "fgw",
				Subsystem: "cidlocation",
				Name:      "object_intermediate_nodes_crawled_total",
			}),
		},
	}
}

func StartWorkers(lc fx.Lifecycle, w *Worker, cfg *configuration.RibsConfig) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}

	lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			log.Infow("Starting cid location workers", "count", cfg.CidLocationWorkerCount)
			wg.Add(cfg.CidLocationWorkerCount)

			for i := 0; i < cfg.CidLocationWorkerCount; i++ {
				go func() {
					defer wg.Done()

					for {
						select {
						case <-ctx.Done():
							log.Info("cid location worker stopped")
							return
						case c := <-w.queue:
							_, err := w.process(ctx, c)
							if err != nil {
								log.Errorf("error processing cid %s: %s", c, err)
							}
							w.metrics.queueSize.Set(float64(len(w.queue)))
						}
					}
				}()
			}
			return nil
		},
		OnStop: func(ctx context.Context) error {
			log.Info("Stopping cid location workers")
			cancel()
			wg.Wait()

			return nil
		},
	})
}

func (w *Worker) Schedule(c cid.Cid) {
	w.queue <- c
	w.metrics.cidsScheduled.Inc()
	w.metrics.queueSize.Set(float64(len(w.queue)))
}

func (w *Worker) GetOffloadStatus(ctx context.Context, c cid.Cid) (iface2.OffloadStatus, error) {
	w.metrics.offloadStatusQueries.Inc()
	groups, err := w.GetGroups(ctx, c)
	if err != nil {
		return "", err
	}
	if len(groups) == 0 {
		return "", fmt.Errorf("no groups found for cid %s", c)
	}

	states, err := w.groupdb.GroupStates(groups)
	if err != nil {
		return "", fmt.Errorf("getting group states: %w", err)
	}
	if len(states) != len(groups) {
		return "", fmt.Errorf("group states not found for cid %s", c)
	}

	minState := iface2.GroupState(math.MaxInt)
	for _, state := range states {
		if state < minState {
			minState = state
		}
	}

	if minState >= iface2.GroupStateOffloaded {
		return OffloadStatusComplete, nil
	} else {
		return OffloadStatusStaging, nil
	}
}

func (w *Worker) GetGroups(ctx context.Context, c cid.Cid) ([]iface2.GroupKey, error) {
	w.metrics.groupQueries.Inc()
	groups, err := w.locationIndex.GetCidLocation(c)
	if errors.Is(err, errNotFound) {
		log.Warnf("Location not found for cid %s. Attempting recovery...", c)
		w.metrics.groupQueriesWithRecovery.Inc()
		groups, err = w.process(ctx, c)
	}
	if err != nil {
		return nil, fmt.Errorf("getting cid locations: %w", err)
	}
	return groups, nil
}

func (w *Worker) process(ctx context.Context, c cid.Cid) ([]iface2.GroupKey, error) {
	log.Debugf("processing cid %s", c)
	groups, err := w.retrieveGroups(ctx, c)
	if err != nil {
		w.metrics.cidProcessingErrors.Inc()
		return nil, err
	}

	err = w.locationIndex.PutCidLocation(c, groups)
	if err != nil {
		w.metrics.cidProcessingErrors.Inc()
		return nil, fmt.Errorf("saving cid locations: %w", err)
	}

	w.metrics.cidsProcessed.Inc()
	log.Debugf("processed cid %s", c)
	return groups, nil
}

func (w *Worker) retrieveGroups(ctx context.Context, root cid.Cid) ([]iface2.GroupKey, error) {
	mhs, err := w.retrieveCids(ctx, root)
	if err != nil {
		return nil, fmt.Errorf("retrieving cids: %w", err)
	}

	groupSet := make(map[iface2.GroupKey]struct{})

	err = w.groupIndex.GetGroups(ctx, mhs, func(cidx int, group iface2.GroupKey) (bool, error) {
		groupSet[group] = struct{}{}
		return true, nil
	})

	if err != nil {
		return nil, fmt.Errorf("querying groups with cids: %w", err)
	}

	groups := make([]iface2.GroupKey, 0, len(groupSet))
	for g := range groupSet {
		groups = append(groups, g)
	}
	return groups, nil
}

func (w *Worker) retrieveCids(ctx context.Context, root cid.Cid) ([]multihash.Multihash, error) {
	queue := []cid.Cid{root}
	res := make([]multihash.Multihash, 0)
	visited := make(map[cid.Cid]struct{})

	var rawNodesCrawled, intermediateNodesCrawled int64

	for len(queue) > 0 {
		// pop from the end, ensures queue is linear with depth of the tree. Also better for allocations
		// as we work on a mostly fixed size array
		current := queue[len(queue)-1]
		queue = queue[:len(queue)-1]

		res = append(res, current.Hash())
		visited[current] = struct{}{}

		if current.Prefix().Codec == cid.Raw {
			// no links here
			// note: we still get here even though we don't add raw links in the crawl below
			// this is because small objects are Raw at the top level
			rawNodesCrawled++
			continue
		}
		intermediateNodesCrawled++

		node, err := w.dag.Get(ctx, current)
		if err != nil {
			return nil, err
		}

		for _, l := range node.Links() {
			if l == nil {
				continue
			}
			c := l.Cid
			if _, ok := visited[c]; ok {
				continue
			}

			// don't bother adding raw links to the queue
			if c.Prefix().Codec == cid.Raw {
				continue
			}
			queue = append(queue, c)
		}
	}

	w.metrics.objectRawNodesCrawled.Add(float64(rawNodesCrawled))
	w.metrics.objectIntermediateNodesCrawled.Add(float64(intermediateNodesCrawled))
	return res, nil
}
