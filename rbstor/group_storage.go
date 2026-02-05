package rbstor

import (
	"context"
	"time"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	"github.com/CIDgravity/filecoin-gateway/iface"
	"golang.org/x/xerrors"
)

func (r *rbs) createGroup(ctx context.Context) (iface.GroupKey, *Group, error) {
	if err := r.ensureSpaceForGroup(ctx); err != nil {
		return 0, nil, xerrors.Errorf("ensure space for group: %w", err)
	}

	selectedGroup, err := r.db.CreateGroup()
	if err != nil {
		return iface.UndefGroupKey, nil, xerrors.Errorf("creating group: %w", err)
	}

	g, err := r.openGroup(ctx, selectedGroup, 0, 0, 0, iface.GroupStateWritable, true)
	if err != nil {
		return iface.UndefGroupKey, nil, xerrors.Errorf("opening group: %w", err)
	}

	return selectedGroup, g, nil
}

func (r *rbs) openGroup(ctx context.Context, group iface.GroupKey, blocks, bytes, jbhead int64, state iface.GroupState, create bool) (*Group, error) {
	g, err := OpenGroup(ctx, r.db, r.index, &r.staging, group, blocks, bytes, jbhead, r.root, state, create)
	if err != nil {
		return nil, xerrors.Errorf("opening group: %w", err)
	}

	if state == iface.GroupStateWritable {
		r.writableGroups[group] = g
	}
	r.openGroups[group] = g

	return g, nil
}

// withWritableGroup executes the callback with a writable group.
// In legacy mode (parallel writes disabled), this uses global locking.
// In parallel mode, this uses the load balancer for group selection.
func (r *rbs) withWritableGroup(ctx context.Context, prefer iface.GroupKey, cb func(group *Group) error) (selectedGroup iface.GroupKey, err error) {
	cfg := configuration.GetConfig().ParallelWrite
	if cfg.Enabled {
		return r.withWritableGroupParallel(ctx, nil, prefer, cb)
	}
	return r.withWritableGroupLegacy(ctx, prefer, cb)
}

// withWritableGroupForSession is like withWritableGroup but with session affinity support.
// Used by ribBatch to maintain locality for related blocks.
func (r *rbs) withWritableGroupForSession(ctx context.Context, session *ribSession, prefer iface.GroupKey, cb func(group *Group) error) (selectedGroup iface.GroupKey, err error) {
	cfg := configuration.GetConfig().ParallelWrite
	if cfg.Enabled {
		return r.withWritableGroupParallel(ctx, session, prefer, cb)
	}
	return r.withWritableGroupLegacy(ctx, prefer, cb)
}

// withWritableGroupLegacy is the original single-writer implementation.
// It uses global locking to serialize all writes to a single group.
func (r *rbs) withWritableGroupLegacy(ctx context.Context, prefer iface.GroupKey, cb func(group *Group) error) (selectedGroup iface.GroupKey, err error) {
	r.lk.Lock()
	defer r.lk.Unlock()

	r.writeLk.Lock()
	defer r.writeLk.Unlock()

	defer func() {
		if err != nil || selectedGroup == iface.UndefGroupKey {
			return
		}
		// if the group was filled, drop it from writableGroups and start finalize
		if r.writableGroups[selectedGroup].state != iface.GroupStateWritable {
			delete(r.writableGroups, selectedGroup)

			r.tasks <- task{
				tt:    taskTypeFinalize,
				group: selectedGroup,
			}
		}
	}()

	// todo prefer
	for g, grp := range r.writableGroups {
		return g, cb(grp)
	}

	// no writable groups, try to open one

	selectedGroup = iface.UndefGroupKey
	{
		var blocks, bytes, jbhead int64
		var state iface.GroupState

		selectedGroup, blocks, bytes, jbhead, state, err = r.db.GetWritableGroup()
		if err != nil {
			return iface.UndefGroupKey, xerrors.Errorf("finding writable groups: %w", err)
		}

		if selectedGroup != iface.UndefGroupKey {
			g, err := r.openGroup(ctx, selectedGroup, blocks, bytes, jbhead, state, false)
			if err != nil {
				return iface.UndefGroupKey, xerrors.Errorf("opening group: %w", err)
			}

			return selectedGroup, cb(g)
		}
	}

	// no writable groups, create one

	selectedGroup, g, err := r.createGroup(ctx)
	if err != nil {
		return iface.UndefGroupKey, xerrors.Errorf("creating group: %w", err)
	}

	return selectedGroup, cb(g)
}

// withWritableGroupParallel uses the load balancer for group selection.
// This allows concurrent writes to multiple groups for improved throughput.
func (r *rbs) withWritableGroupParallel(ctx context.Context, session *ribSession, prefer iface.GroupKey, cb func(group *Group) error) (selectedGroup iface.GroupKey, err error) {
	// Estimate size for reservation (use average block size estimate)
	// In practice, the actual Put() will handle space limits precisely
	estimatedSize := int64(256 * 1024) // 256KB estimate

	group, cleanup, err := r.loadBalancer.SelectGroup(ctx, session, prefer, estimatedSize)
	if err != nil {
		return iface.UndefGroupKey, xerrors.Errorf("selecting group: %w", err)
	}
	defer cleanup()

	selectedGroup = group.id

	// Execute the callback
	err = cb(group)
	if err != nil {
		return selectedGroup, err
	}

	// Check if group became full and needs finalization
	r.lk.Lock()
	if group.state != iface.GroupStateWritable {
		delete(r.writableGroups, selectedGroup)

		// Clear session affinity for this group since it's no longer writable
		if session != nil {
			r.loadBalancer.clearSessionAffinity(session)
		}

		r.tasks <- task{
			tt:    taskTypeFinalize,
			group: selectedGroup,
		}
	}
	r.lk.Unlock()

	return selectedGroup, nil
}

func (r *rbs) withReadableGroup(ctx context.Context, group iface.GroupKey, cb func(group *Group) error) (err error) {
	r.lk.Lock()

	// todo prefer
	if r.openGroups[group] != nil {
		r.lk.Unlock()
		return cb(r.openGroups[group])
	}

	// not open, open it

	blocks, bytes, jbhead, state, err := r.db.OpenGroup(group)
	if err != nil {
		r.lk.Unlock()
		return xerrors.Errorf("getting group metadata: %w", err)
	}

	g, err := r.openGroup(ctx, group, blocks, bytes, jbhead, state, false)
	if err != nil {
		r.lk.Unlock()
		return xerrors.Errorf("opening group: %w", err)
	}

	r.resumeGroup(group)

	r.lk.Unlock()
	return cb(g)
}

func (r *rbs) ensureSpaceForGroup(ctx context.Context) error {
	localCount, err := r.db.CountNonOffloadedGroups()
	if err != nil {
		return xerrors.Errorf("counting non-offloaded groups: %w", err)
	}

	cfg := configuration.GetConfig()
	if localCount < cfg.Ribs.MaxLocalGroupCount {
		return nil
	}

	var offloadCandidate iface.GroupKey
	for {
		offloadCandidate, err = r.db.GetOffloadCandidate()
		if err != nil {
			return xerrors.Errorf("getting offload candidate: %w", err)
		}

		if offloadCandidate != iface.UndefGroupKey {
			break
		}

		log.Errorw("no offload candidate, waiting for space", "localCount", localCount)

		// wait 1 min, then try again
		r.lk.Unlock()

		select {
		case <-ctx.Done():
			r.lk.Lock()
			return ctx.Err()
		case <-time.After(time.Minute):
		}

		r.lk.Lock()
	}

	log.Errorw("local space full, offloading group", "group", offloadCandidate)

	// release read side
	r.lk.Unlock()
	defer r.lk.Lock()

	return r.withReadableGroup(ctx, offloadCandidate, func(g *Group) error {
		err := g.offloadStaging()
		return err
	})
}
