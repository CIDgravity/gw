package rbstor

import (
	"context"
	"io"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	"github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/filecoin-project/lotus/lib/must"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mitchellh/go-homedir"
	mh "github.com/multiformats/go-multihash"
	"go.uber.org/fx"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

var log = logging.Logger("ribs:rbs")

var Module = fx.Module(
	"rbstor",
	fx.Provide(
		NewCqlIndex,
		NewRibsDB,
		Open,
	),
)

var workerCount = func() int {
	var wc int
	if wcs := os.Getenv("RBS_WORKERS"); wcs != "" {
		wc = int(must.One(strconv.ParseInt(wcs, 10, 64)))
	}

	if wc == 0 {
		wc = runtime.NumCPU() / 8
		if wc < 4 {
			wc = 4
		}
	}

	return wc
}()

// todo separate data / data index / index (/ staging?) paths
func Open(config *configuration.RibsConfig, db *RbsDB, idx iface.GroupIndex) (iface.RBS, error) {
	root, err := homedir.Expand(config.DataDir)
	if err != nil {
		return nil, xerrors.Errorf("expand data dir: %w", err)
	}
	if err := os.Mkdir(root, 0755); err != nil && !os.IsExist(err) {
		return nil, xerrors.Errorf("make root dir: %w", err)
	}

	r := &rbs{
		root:  root,
		db:    db,
		index: NewMeteredIndex(idx),

		writableGroups: make(map[iface.GroupKey]*Group),

		// all open groups (including all writable)
		openGroups: make(map[iface.GroupKey]*Group),

		tasks: make(chan task, 1024),

		close: make(chan struct{}),
	}

	// Initialize load balancer for parallel writes support
	r.loadBalancer = NewLoadBalancer(r)

	// Enable parallel writes based on config
	SetParallelWritesEnabled(configuration.GetConfig().ParallelWrite.Enabled)

	for i := 0; i < workerCount; i++ {
		r.workerClosed = append(r.workerClosed, make(chan struct{}))
	}

	log.Infow("ribs storage open", "dataDir", root)
	return r, nil
}

func (r *rbs) Start() error {
	for i := 0; i < workerCount; i++ {
		go r.groupWorker(i)
	}
	go r.resumeGroups(context.TODO())

	return nil
}

type taskType int

const (
	taskTypeFinalize taskType = iota
	taskTypeGenCommP
	taskTypeFinDataReload
)

type task struct {
	tt    taskType
	group iface.GroupKey
}

type rbs struct {
	root string

	// todo hide this db behind an interface
	db    *RbsDB
	index *MeteredIndex

	lk      sync.Mutex
	writeLk sync.Mutex // Legacy: global write lock (used when parallel writes disabled)

	// loadBalancer manages group selection for parallel writes.
	// When parallel writes are enabled, this replaces the global writeLk.
	loadBalancer *LoadBalancer

	/* subs */
	subLk sync.Mutex
	subs  []iface.GroupSub

	/* storage */

	close        chan struct{}
	workerClosed []chan struct{}

	tasks chan task

	openGroups     map[int64]*Group
	writableGroups map[int64]*Group

	external atomic.Pointer[iface.ExternalStorageProvider]
	staging  atomic.Pointer[iface.StagingStorageProvider]

	// diag cache

	grpReadBlocks  int64
	grpReadSize    int64
	grpWriteBlocks int64
	grpWriteSize   int64

	// workers
	workersAvail         atomic.Int64
	workersFinalizing    atomic.Int64
	workersCommP         atomic.Int64
	workersFinDataReload atomic.Int64
}

func (r *rbs) Close() error {
	close(r.close)
	for i := 0; i < workerCount; i++ {
		<-r.workerClosed[i]
	}

	r.lk.Lock()
	defer r.lk.Unlock()

	for _, g := range r.openGroups {
		if err := g.Close(); err != nil {
			return xerrors.Errorf("closing group %d: %w", g.id, err)
		}
	}

	if err := r.index.Close(); err != nil {
		return xerrors.Errorf("closing index: %w", err)
	}

	log.Errorf("TODO mark closed")

	return nil
}

type ribSession struct {
	r *rbs
}

type ribBatch struct {
	r       *rbs
	session *ribSession // Back-reference to session for affinity tracking

	currentWriteTarget iface.GroupKey
	toFlush            map[iface.GroupKey]struct{}
	puts               map[string]struct{}

	// todo: use lru
}

func (r *rbs) Session(ctx context.Context) iface.Session {
	return &ribSession{
		r: r,
	}
}

func (r *ribSession) View(ctx context.Context, c []mh.Multihash, cb func(cidx int, data []byte)) error {
	done := map[int]struct{}{}
	byGroup := map[iface.GroupKey][]int{}

	err := r.r.index.GetGroups(ctx, c, func(cidx int, group iface.GroupKey) (bool, error) {
		if _, ok := done[cidx]; ok {
			return false, nil
		}
		done[cidx] = struct{}{}

		if group == iface.UndefGroupKey {
			return true, nil
		}

		byGroup[group] = append(byGroup[group], cidx)

		return false, nil
	})
	if err != nil {
		return err
	}

	for g, cidxs := range byGroup {
		toGet := make([]mh.Multihash, len(cidxs))
		for i, cidx := range cidxs {
			toGet[i] = c[cidx]
		}

		err := r.r.withReadableGroup(ctx, g, func(g *Group) error {
			return g.View(ctx, toGet, func(cidx int, found bool, data []byte) {
				if !found {
					c := cid.NewCidV1(cid.Raw, toGet[cidx])
					log.Errorw("group: block not found", "mh", toGet[cidx], "cid", c.String(), "group", g.id)
					return
				}

				cb(cidxs[cidx], data)
			})
		})
		if err == ErrOffloaded {
			extp := r.r.external.Load()
			if extp == nil {
				return xerrors.Errorf("no external storage, group %d is offloaded", g)
			}

			ext := *extp
			return ext.FetchBlocks(ctx, g, toGet, func(cidx int, data []byte) {
				cb(cidxs[cidx], data)
			})
		} else if err != nil {
			return xerrors.Errorf("with readable group(%d)/view: %w", g, err)
		}
	}

	return nil
}

func (r *ribSession) GetSize(ctx context.Context, c []mh.Multihash, cb func(i []int32) error) error {
	return r.r.index.GetSizes(ctx, c, cb)
}

func (r *ribSession) Batch(ctx context.Context) iface.Batch {
	return &ribBatch{
		r:                  r.r,
		session:            r,
		currentWriteTarget: iface.UndefGroupKey,
		toFlush:            map[iface.GroupKey]struct{}{},
		puts:               map[string]struct{}{},
	}
}

func (r *ribBatch) Put(ctx context.Context, b []blocks.Block) error {
	// todo filter blocks that already exist
	var done int
	for done < len(b) {
		// Note: We pass nil for session to disable session affinity.
		// Session affinity is counterproductive when there's a single global session
		// (as in the Blockstore). Instead, we rely on currentWriteTarget for locality
		// within a batch, while allowing different batches to use different groups.
		startTime := time.Now()
		var bytesWritten int64
		var blocksWritten int
		gk, err := r.r.withWritableGroupForSession(ctx, nil, r.currentWriteTarget, func(g *Group) error {
			wrote, err := g.Put(ctx, b[done:])
			if err != nil {
				return err
			}
			blocksWritten = wrote
			// Calculate bytes written
			for i := done; i < done+wrote; i++ {
				bytesWritten += int64(len(b[i].RawData()))
			}
			done += wrote
			return nil
		})

		// Record metrics
		isParallel := IsParallelWritesEnabled()
		parallelMetrics.RecordWrite(isParallel, time.Since(startTime), bytesWritten, int64(blocksWritten), err)

		if err != nil {
			return xerrors.Errorf("write to group: %w", err)
		}

		for i := done - blocksWritten; i < done; i++ {
			r.puts[string(b[i].Cid().Hash())] = struct{}{}
		}

		r.toFlush[gk] = struct{}{}
		r.currentWriteTarget = gk
	}

	return nil
}

func (r *ribBatch) Unlink(ctx context.Context, c []mh.Multihash) error {
	filtered := make([]mh.Multihash, 0, len(c))
	for _, hash := range c {
		if _, ok := r.puts[string(hash)]; ok {
			continue
		}
		filtered = append(filtered, hash)
	}

	if len(filtered) == 0 {
		return nil
	}

	// Group multihashes by their current group location
	byGroup := make(map[iface.GroupKey][]mh.Multihash)

	err := r.r.index.GetGroups(ctx, filtered, func(cidx int, gk iface.GroupKey) (bool, error) {
		if gk == iface.UndefGroupKey {
			// Block doesn't exist, nothing to unlink
			return true, nil
		}
		byGroup[gk] = append(byGroup[gk], filtered[cidx])
		return true, nil
	})
	if err != nil {
		return xerrors.Errorf("lookup groups for unlink: %w", err)
	}

	// Unlink from each group
	for gk, mhs := range byGroup {
		// Try to open the group (may be offloaded)
		err := r.r.withReadableGroup(ctx, gk, func(g *Group) error {
			return g.Unlink(ctx, mhs)
		})
		if err != nil {
			if err == ErrOffloaded {
				// Group is offloaded - we can still remove from index
				// The data will be cleaned up when the group is reloaded or through GC
				log.Debugw("unlink from offloaded group - removing index entries only",
					"group", gk, "count", len(mhs))
			} else {
				// Log but continue - some groups may be unavailable
				log.Warnw("unlink from group failed", "group", gk, "error", err, "count", len(mhs))
				continue
			}
		}

		// Remove from index regardless of group state
		if err := r.r.index.DropGroup(ctx, mhs, gk); err != nil {
			log.Errorw("failed to drop group from index", "group", gk, "error", err)
			continue
		}

		r.toFlush[gk] = struct{}{}
	}

	return nil
}

func (r *ribBatch) Flush(ctx context.Context) error {
	cfg := configuration.GetConfig().ParallelWrite

	// Reset write target after flush so the next cycle goes through the load
	// balancer and picks a (potentially different) group.  This spreads writes
	// across groups instead of sticking to one until it fills up.
	defer func() { r.currentWriteTarget = iface.UndefGroupKey }()

	if cfg.Enabled && len(r.toFlush) > 1 {
		return r.flushParallel(ctx)
	}
	return r.flushLegacy(ctx)
}

// flushLegacy is the original sequential flush implementation.
func (r *ribBatch) flushLegacy(ctx context.Context) error {
	startTime := time.Now()
	r.r.lk.Lock()
	defer r.r.lk.Unlock()

	for key := range r.toFlush {
		g, found := r.r.writableGroups[key]
		if !found {
			continue // already flushed
		}
		r.r.lk.Unlock()
		err := g.Sync(ctx)
		r.r.lk.Lock()
		if err != nil {
			parallelMetrics.RecordFlush(false, time.Since(startTime), err)
			return xerrors.Errorf("sync group %d: %w", key, err)
		}
	}

	if err := r.r.index.Sync(ctx); err != nil {
		parallelMetrics.RecordFlush(false, time.Since(startTime), err)
		return xerrors.Errorf("flush top index: %w", err)
	}

	r.toFlush = map[iface.GroupKey]struct{}{}

	parallelMetrics.RecordFlush(false, time.Since(startTime), nil)
	return nil
}

// flushParallel syncs multiple groups concurrently for improved throughput.
func (r *ribBatch) flushParallel(ctx context.Context) error {
	startTime := time.Now()

	// Collect groups to flush
	r.r.lk.Lock()
	groupsToFlush := make([]*Group, 0, len(r.toFlush))
	for key := range r.toFlush {
		g, found := r.r.writableGroups[key]
		if found {
			groupsToFlush = append(groupsToFlush, g)
		}
	}
	r.r.lk.Unlock()

	if len(groupsToFlush) == 0 {
		// Nothing to flush, just sync the index
		if err := r.r.index.Sync(ctx); err != nil {
			parallelMetrics.RecordFlush(true, time.Since(startTime), err)
			return xerrors.Errorf("flush top index: %w", err)
		}
		r.toFlush = map[iface.GroupKey]struct{}{}
		parallelMetrics.RecordFlush(true, time.Since(startTime), nil)
		return nil
	}

	// Flush groups in parallel using errgroup
	eg, egCtx := errgroup.WithContext(ctx)

	for _, g := range groupsToFlush {
		g := g // capture for goroutine
		eg.Go(func() error {
			if err := g.Sync(egCtx); err != nil {
				return xerrors.Errorf("sync group %d: %w", g.id, err)
			}
			return nil
		})
	}

	// Wait for all group syncs to complete
	if err := eg.Wait(); err != nil {
		parallelMetrics.RecordFlush(true, time.Since(startTime), err)
		return err
	}

	// Sync the top-level index after all groups are synced
	if err := r.r.index.Sync(ctx); err != nil {
		parallelMetrics.RecordFlush(true, time.Since(startTime), err)
		return xerrors.Errorf("flush top index: %w", err)
	}

	r.toFlush = map[iface.GroupKey]struct{}{}

	parallelMetrics.RecordFlush(true, time.Since(startTime), nil)
	return nil
}

func (r *rbs) Offload(ctx context.Context, group iface.GroupKey) error {
	return r.withReadableGroup(ctx, group, func(g *Group) error {
		err := g.offload()
		return err
	})
}

func (r *rbs) FindHashes(ctx context.Context, hash mh.Multihash) ([]iface.GroupKey, error) {
	var out []iface.GroupKey

	err := r.index.GetGroups(ctx, []mh.Multihash{hash}, func(cidx int, group iface.GroupKey) (bool, error) {
		if group == iface.UndefGroupKey {
			return true, nil
		}

		out = append(out, group)

		return true, nil
	})

	if err != nil {
		return nil, err
	}

	return out, nil
}

func (r *rbs) DescibeGroup(ctx context.Context, group iface.GroupKey) (iface.GroupDesc, error) {
	return r.db.DescibeGroup(ctx, group)
}

func (r *rbs) ReadCar(ctx context.Context, group iface.GroupKey, sz func(int64), out io.Writer) error {
	gm, err := r.db.GroupMeta(group)
	if err != nil {
		return xerrors.Errorf("getting group meta: %w", err)
	}
	if gm.DealCarSize == nil {
		return xerrors.Errorf("group has no deal car size set")
	}

	sz(*gm.DealCarSize)

	return r.withReadableGroup(ctx, group, func(g *Group) error {
		_, _, err := g.writeCar(out)
		return err
	})
}

func (r *rbs) HashSample(ctx context.Context, group iface.GroupKey) ([]mh.Multihash, error) {
	var out []mh.Multihash
	err := r.withReadableGroup(ctx, group, func(g *Group) error {
		var err error
		out, err = g.hashSample()
		return err
	})

	return out, err
}

func (r *rbs) LoadFilCar(ctx context.Context, group iface.GroupKey, f io.Reader, sz int64) error {
	err := r.withReadableGroup(ctx, group, func(g *Group) error {
		if err := g.LoadFilCar(ctx, f, sz); err != nil {
			return xerrors.Errorf("load data into group: %w", err)
		}

		r.tasks <- task{
			tt:    taskTypeFinDataReload,
			group: group,
		}

		return nil
	})
	return err
}

func (r *rbs) Storage() iface.Storage {
	return r
}

func (r *rbs) ExternalStorage() iface.RBSExternalStorage {
	return r
}

func (r *rbs) InstallStagingProvider(provider iface.StagingStorageProvider) {
	r.staging.Store(&provider)
}

func (r *rbs) InstallProvider(provider iface.ExternalStorageProvider) {
	r.external.Store(&provider)
}

func (r *rbs) StagingStorage() iface.RBSStagingStorage {
	return r
}

var _ iface.RBS = &rbs{}
