package rbstor

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CIDgravity/filecoin-gateway/carlog"
	"github.com/CIDgravity/filecoin-gateway/iface"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

var (
	// TODO: make this configurable
	maxGroupSize int64 = 29500 << 20

	maxGroupBlocks int64 = 20 << 20
)

var ErrOffloaded = fmt.Errorf("group is offloaded")

type Group struct {
	db    *RbsDB
	index iface.GroupIndex

	path string
	id   int64

	// access with dataLk
	state iface.GroupState

	// db lock
	// note: can be taken when dataLk is held
	dblk sync.Mutex

	// data lock - serializes CarLog writes (CarLog is NOT thread-safe for writes)
	dataLk sync.RWMutex

	// reader protectors
	readers   sync.WaitGroup
	offloaded atomic.Int64

	// inflight counters track current jbob writes which are not yet committed
	inflightBlocks int64
	inflightSize   int64

	// committed counters match the db
	committedBlocks int64
	committedSize   int64

	// === Parallel Writer Support ===
	// spaceLk is a lightweight lock for space reservation checks only.
	// It protects reservedSpace but NOT the actual write (dataLk does that).
	// Lock ordering: spaceLk -> dataLk (never hold dataLk then take spaceLk)
	spaceLk sync.Mutex

	// reservedSpace tracks bytes reserved by in-flight write operations.
	// Writers reserve space before acquiring dataLk to allow early rejection.
	reservedSpace int64

	// activeWriters tracks the number of concurrent writers to this group.
	// Used for load balancing and graceful shutdown during finalization.
	activeWriters atomic.Int32
	// === End Parallel Writer Support ===

	// atomic perf/diag counters
	readBlocks  atomic.Int64
	readSize    atomic.Int64
	writeBlocks atomic.Int64
	writeSize   atomic.Int64

	// perf counter snapshots, owned by group manager
	readBlocksSnap  int64
	readSizeSnap    int64
	writeBlocksSnap int64
	writeSizeSnap   int64

	jb *carlog.CarLog
}

func OpenGroup(ctx context.Context, db *RbsDB, index iface.GroupIndex, staging *atomic.Pointer[iface.StagingStorageProvider],
	id, committedBlocks, committedSize, recordedHead int64,
	path string, state iface.GroupState, create bool) (*Group, error) {
	groupPath := filepath.Join(path, "grp", strconv.FormatInt(id, 32))

	if err := os.MkdirAll(groupPath, 0755); err != nil {
		return nil, xerrors.Errorf("create group directory: %w", err)
	}

	// open jbob

	jbOpenFunc := carlog.Open
	if create {
		jbOpenFunc = carlog.Create
	}

	var stw carlog.CarStorageProvider
	st := staging.Load()
	if st == nil {
		return nil, fmt.Errorf("no staging provider configured")
	}
	stw = &carStorageWrapper{
		storage: *st,
		group:   id,
	}

	jb, err := jbOpenFunc(stw, filepath.Join(groupPath, "blklog.meta"), groupPath, func(to int64, h []mh.Multihash) error {
		if to < recordedHead {
			return xerrors.Errorf("cannot rewind jbob head to %d, recorded group head is %d", to, recordedHead)
		}

		return index.DropGroup(ctx, h, id)
	})
	if err != nil {
		return nil, xerrors.Errorf("open jbob (grp: %s): %w", groupPath, err)
	}

	g := &Group{
		db:    db,
		index: index,

		jb: jb,

		committedBlocks: committedBlocks,
		committedSize:   committedSize,

		path:  groupPath,
		id:    id,
		state: state,
	}

	if state >= iface.GroupStateOffloaded {
		g.offloaded.Store(1)
	}

	return g, nil
}

func (m *Group) Put(ctx context.Context, b []blocks.Block) (int, error) {
	// NOTE: Put is the only method which writes data to jbob

	if len(b) == 0 {
		return 0, nil
	}

	// carlog writes are not thread safe, take the lock to get serial access
	m.dataLk.Lock()
	defer m.dataLk.Unlock()

	if m.state != iface.GroupStateWritable {
		return 0, nil
	}

	// reserve space
	availSpace := maxGroupSize - m.committedSize - m.inflightSize // todo async - inflight

	var writeSize int64
	var writeBlocks int

	for _, blk := range b {
		if int64(len(blk.RawData()))+writeSize > availSpace || m.committedBlocks+int64(writeBlocks) >= maxGroupBlocks {
			break
		}
		writeSize += int64(len(blk.RawData()))
		writeBlocks++
	}

	if writeBlocks < len(b) {
		// this group is full
		m.state = iface.GroupStateFull
	}

	m.inflightBlocks += int64(writeBlocks)
	m.inflightSize += writeSize

	m.writeBlocks.Add(int64(writeBlocks))
	m.writeSize.Add(writeSize)

	// backend write

	// 1. (buffer) writes to jbob

	c := make([]mh.Multihash, len(b))
	sz := make([]int32, len(b))
	for i, blk := range b {
		c[i] = blk.Cid().Hash()
		sz[i] = int32(len(blk.RawData()))
	}

	// parallel data(log) / index write; In case of unclean shutdown we may get
	// orphan entries in the top index, but that should be fine - unclean shutdowns
	// generally don't happen a lot, and if we use one of those bad entries, and
	// don't find the data in the correct block group, we'll just try another one.
	// Proper cleanup is also possible, but very expensive as it requires scanning of
	// all the data.
	// tldr we do stuff in parallel because more speed good
	eg := new(errgroup.Group)

	eg.Go(func() error {
		err := m.jb.Put(c[:writeBlocks], b[:writeBlocks])
		if err != nil {
			// todo handle properly (abort, close, check disk space / resources, repopen)
			// todo docrement inflight?
			return xerrors.Errorf("writing to jbob: %w", err)
		}

		return nil
	})

	eg.Go(func() error {
		// 3. write top-level index (before we update group head so replay is possible, before jbob commit so that it's faster)
		//    missed, uncommitted jbob writes should be ignored.
		// ^ TODO: Test this commit edge case
		// TODO: Async index queue
		err := m.index.AddGroup(ctx, c[:writeBlocks], sz[:writeBlocks], m.id)
		if err != nil {
			// todo handle properly (abort, close, check disk space / resources, repopen)
			return xerrors.Errorf("writing index: %w", err)
		}

		return nil
	})

	if err := eg.Wait(); err != nil {
		return 0, xerrors.Errorf("data/index write: %w", err)
	}

	// 3.5 mark as read-only if full
	// todo is this the right place to do this?
	if m.state == iface.GroupStateFull {
		if err := m.sync(ctx); err != nil {
			// todo handle properly (abort, close, check disk space / resources, repopen)
			return 0, xerrors.Errorf("sync full group: %w", err)
		}

		if err := m.jb.MarkReadOnly(); err != nil {
			// todo handle properly (abort, close, check disk space / resources, repopen)
			// todo combine with commit?
			return 0, xerrors.Errorf("mark jbob read-only: %w", err)
		}
	}

	return writeBlocks, nil
}

func (m *Group) Sync(ctx context.Context) error {
	m.dataLk.Lock()
	defer m.dataLk.Unlock()

	return m.sync(ctx)
}

func (m *Group) sync(ctx context.Context) error {
	fmt.Println("syncing group", m.id)
	// 1. commit jbob (so puts above are now on disk)

	at, err := m.jb.Commit()
	if err != nil {
		// todo handle properly (abort, close, check disk space / resources, repopen)
		return xerrors.Errorf("committing jbob: %w", err)
	}

	// todo with async index queue, also wait for index queue to be flushed

	// 2. update head
	m.committedBlocks += m.inflightBlocks
	m.committedSize += m.inflightSize
	m.inflightBlocks = 0
	m.inflightSize = 0

	m.dblk.Lock()
	err = m.db.SetGroupHead(ctx, m.id, m.state, m.committedBlocks, m.committedSize, at)
	m.dblk.Unlock()
	if err != nil {
		// todo handle properly (retry, abort, close, check disk space / resources, repopen)
		return xerrors.Errorf("update group head: %w", err)
	}

	return nil
}

// === Parallel Writer Space Reservation Methods ===

// SpaceReservation represents a successful space reservation that must be
// released after the write completes (successfully or not).
type SpaceReservation struct {
	group    *Group
	bytes    int64
	released bool
}

// Release returns the reserved space back to the group.
// Safe to call multiple times (idempotent).
func (r *SpaceReservation) Release() {
	if r.released {
		return
	}
	r.released = true

	r.group.spaceLk.Lock()
	r.group.reservedSpace -= r.bytes
	r.group.spaceLk.Unlock()

	r.group.activeWriters.Add(-1)
}

// AvailableSpace returns the space available for new writes.
// This is an approximate value as it doesn't account for block overhead.
// Must be called with spaceLk held.
func (m *Group) availableSpaceLocked() int64 {
	return maxGroupSize - m.committedSize - m.inflightSize - m.reservedSpace
}

// AvailableSpace returns the approximate space available for new writes.
// Thread-safe but the returned value may change immediately after return.
func (m *Group) AvailableSpace() int64 {
	m.spaceLk.Lock()
	defer m.spaceLk.Unlock()
	return m.availableSpaceLocked()
}

// AvailableBlocks returns the number of blocks that can still be written.
func (m *Group) AvailableBlocks() int64 {
	m.spaceLk.Lock()
	defer m.spaceLk.Unlock()
	return maxGroupBlocks - m.committedBlocks - m.inflightBlocks
}

// TryReserveSpace attempts to reserve space for a write operation.
// Returns a SpaceReservation if successful, or nil if not enough space.
// The reservation MUST be released after the write completes.
//
// This method is designed for parallel writer support:
//   - Takes spaceLk (lightweight) to check/reserve space
//   - Does NOT take dataLk (heavy) - that happens during actual write
//   - Allows early rejection without blocking other writers
//
// Usage:
//
//	reservation := group.TryReserveSpace(1024)
//	if reservation == nil {
//	    return ErrGroupFull
//	}
//	defer reservation.Release()
//	// ... perform write with dataLk ...
func (m *Group) TryReserveSpace(bytes int64) *SpaceReservation {
	m.spaceLk.Lock()
	defer m.spaceLk.Unlock()

	// Check if group is still writable
	if m.state != iface.GroupStateWritable {
		return nil
	}

	// Check available space
	available := m.availableSpaceLocked()
	if bytes > available {
		return nil
	}

	// Check block limit (approximate - we don't know exact block count yet)
	// This is a soft check; the actual Put() will enforce the hard limit
	if m.committedBlocks+m.inflightBlocks >= maxGroupBlocks {
		return nil
	}

	// Reserve the space
	m.reservedSpace += bytes
	m.activeWriters.Add(1)

	return &SpaceReservation{
		group: m,
		bytes: bytes,
	}
}

// HasActiveWriters returns true if there are writers currently using this group.
// Used during finalization to wait for writers to drain.
func (m *Group) HasActiveWriters() bool {
	return m.activeWriters.Load() > 0
}

// ActiveWriterCount returns the number of active writers to this group.
func (m *Group) ActiveWriterCount() int32 {
	return m.activeWriters.Load()
}

// WaitForWritersDrain blocks until all active writers have completed.
// Used during graceful finalization transitions.
func (m *Group) WaitForWritersDrain(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if !m.HasActiveWriters() {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Continue waiting
		}
	}
}

// === End Parallel Writer Space Reservation Methods ===

func (m *Group) Unlink(ctx context.Context, c []mh.Multihash) error {
	m.dataLk.Lock()
	defer m.dataLk.Unlock()

	if m.offloaded.Load() != 0 {
		return ErrOffloaded
	}

	// Update local counters - we track how many blocks/size are "deleted"
	// Note: This is a logical delete. The data remains in the CarLog until
	// compaction, but becomes unreachable through the index.
	var deletedBlocks int64
	var deletedSize int64

	for _, hash := range c {
		// Try to get the size from the index to update counters accurately
		err := m.index.GetSizes(ctx, []mh.Multihash{hash}, func(sizes []int32) error {
			if len(sizes) > 0 && sizes[0] > 0 {
				deletedBlocks++
				deletedSize += int64(sizes[0])
			}
			return nil
		})
		if err != nil {
			// Log but continue - block may not exist
			log.Debugw("unlink: failed to get size", "multihash", hash, "error", err)
		}
	}

	// Update committed counters (logical deletion)
	// Note: We don't actually subtract from committed counters because
	// the data is still physically present. Instead, we track dead blocks
	// separately for GC purposes.
	if deletedBlocks > 0 {
		// Update dead block tracking in the database
		if err := m.db.UpdateGroupDeadBlocks(ctx, m.id, deletedBlocks, deletedSize); err != nil {
			log.Warnw("unlink: failed to update dead block counters",
				"group", m.id, "error", err)
			// Continue - this is best-effort
		}
	}

	log.Infow("unlink completed",
		"group", m.id,
		"blocks", deletedBlocks,
		"size", deletedSize,
		"requested", len(c))

	return nil
}

func (m *Group) View(ctx context.Context, c []mh.Multihash, cb func(cidx int, found bool, data []byte)) error {
	m.readers.Add(1)
	defer m.readers.Done()

	if m.offloaded.Load() != 0 {
		return ErrOffloaded
	}

	// right now we just read from jbob

	// View is thread safe
	return m.jb.View(c, func(cidx int, found bool, data []byte) error {
		if !found {
			cb(cidx, false, nil)
			return nil
		}

		m.readBlocks.Add(1)
		m.readSize.Add(int64(len(data)))

		cb(cidx, true, data)
		return nil
	})
}

func (m *Group) Close() error {
	if err := m.Sync(context.Background()); err != nil {
		return err
	}

	m.dataLk.Lock()
	defer m.dataLk.Unlock()

	err := m.jb.Close()
	// todo mark as closed
	return err
}

// returns car size and root cid
func (m *Group) writeCar(w io.Writer) (int64, cid.Cid, error) {
	m.readers.Add(1)
	defer m.readers.Done()

	if m.offloaded.Load() != 0 {
		return 0, cid.Undef, ErrOffloaded
	}

	// writeCar is thread safe
	return m.jb.WriteCar(w)
}

func (m *Group) hashSample() ([]mh.Multihash, error) {
	// hashSample is thread safe
	return m.jb.HashSample()
}

type carStorageWrapper struct {
	storage iface.StagingStorageProvider
	group   iface.GroupKey
}

func (c *carStorageWrapper) Has(ctx context.Context) (bool, error) {
	return c.storage.HasCar(ctx, c.group)
}

func (c *carStorageWrapper) ReadAt(p []byte, off int64) (n int, err error) {
	rc, err := c.storage.ReadCar(context.TODO(), c.group, off, int64(len(p)))
	if err != nil {
		return 0, err
	}

	n, err = io.ReadFull(rc, p)
	cerr := rc.Close()

	if err != nil {
		return n, err
	}

	return n, cerr
}

func (c *carStorageWrapper) Upload(ctx context.Context, size int64, src func(writer io.Writer) error) error {
	return c.storage.Upload(ctx, c.group, size, src)
}

var _ carlog.CarStorageProvider = &carStorageWrapper{}
