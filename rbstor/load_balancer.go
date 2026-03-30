package rbstor

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	"github.com/CIDgravity/filecoin-gateway/iface"
	"golang.org/x/xerrors"
)

// LoadBalancer manages distribution of writes across multiple writable groups.
// It implements weighted selection based on available space and active writers,
// with support for session affinity to keep related blocks together.
type LoadBalancer struct {
	r *rbs

	// selectionLk protects group selection logic
	// This is a lightweight lock - actual writes use per-group dataLk
	selectionLk sync.Mutex

	// Session affinity tracking
	// Maps session pointer to preferred group for locality
	sessionAffinity   map[*ribSession]iface.GroupKey
	sessionAffinityLk sync.RWMutex
}

// NewLoadBalancer creates a new load balancer for the given RBS instance.
func NewLoadBalancer(r *rbs) *LoadBalancer {
	return &LoadBalancer{
		r:               r,
		sessionAffinity: make(map[*ribSession]iface.GroupKey),
	}
}

// groupScore represents a writable group with its selection score.
type groupScore struct {
	group *Group
	score float64
}

// SelectGroup chooses the best writable group for a write operation.
// It considers:
// 1. Session affinity (if provided) - prefer same group for related blocks
// 2. Available space - prefer groups with more space
// 3. Active writers - prefer groups with fewer active writers (load balancing)
//
// Returns the selected group and a cleanup function that MUST be called after
// the write completes (successfully or not) to release the reservation.
func (lb *LoadBalancer) SelectGroup(
	ctx context.Context,
	session *ribSession,
	preferGroup iface.GroupKey,
	estimatedSize int64,
) (*Group, func(), error) {
	lb.selectionLk.Lock()
	defer lb.selectionLk.Unlock()

	cfg := configuration.GetConfig().ParallelWrite

	// Try session affinity first (if enabled and session provided)
	if session != nil && cfg.Enabled {
		if group := lb.trySessionAffinity(session, estimatedSize); group != nil {
			cleanup := func() {
				// No reservation to release for affinity hit
			}
			return group, cleanup, nil
		}
	}

	// Try preferred group (from previous write in same batch)
	if preferGroup != iface.UndefGroupKey {
		if group := lb.tryPreferredGroup(preferGroup, estimatedSize); group != nil {
			cleanup := func() {}
			return group, cleanup, nil
		}
	}

	// Fall back to weighted selection from all writable groups
	return lb.selectWeighted(ctx, session, estimatedSize, cfg)
}

// trySessionAffinity checks if the session has affinity to a group that can accept the write.
func (lb *LoadBalancer) trySessionAffinity(session *ribSession, size int64) *Group {
	start := time.Now()

	lb.sessionAffinityLk.RLock()
	affinityGroup, hasAffinity := lb.sessionAffinity[session]
	lb.sessionAffinityLk.RUnlock()

	if !hasAffinity {
		return nil
	}

	lb.r.lk.Lock()
	group, found := lb.r.writableGroups[affinityGroup]
	lb.r.lk.Unlock()

	if !found || group == nil {
		// Group no longer writable, clear affinity
		lb.clearSessionAffinity(session)
		parallelMetrics.RecordGroupSelection("affinity_miss", time.Since(start))
		return nil
	}

	// Check if group has space
	if group.AvailableSpace() < size {
		parallelMetrics.RecordGroupSelection("affinity_miss", time.Since(start))
		return nil
	}

	parallelMetrics.RecordGroupSelection("affinity_hit", time.Since(start))
	return group
}

// tryPreferredGroup attempts to use the preferred group if it has space.
func (lb *LoadBalancer) tryPreferredGroup(preferGroup iface.GroupKey, size int64) *Group {
	start := time.Now()

	lb.r.lk.Lock()
	group, found := lb.r.writableGroups[preferGroup]
	lb.r.lk.Unlock()

	if !found || group == nil {
		return nil
	}

	if group.AvailableSpace() < size {
		return nil
	}

	parallelMetrics.RecordGroupSelection("preferred", time.Since(start))
	return group
}

// selectWeighted performs weighted selection across all writable groups.
func (lb *LoadBalancer) selectWeighted(
	ctx context.Context,
	session *ribSession,
	estimatedSize int64,
	cfg configuration.ParallelWriteConfig,
) (*Group, func(), error) {
	start := time.Now()

	lb.r.lk.Lock()

	// Collect candidate groups with scores
	var candidates []groupScore
	for _, group := range lb.r.writableGroups {
		if group.state != iface.GroupStateWritable {
			continue
		}
		score := lb.calculateScore(group, estimatedSize)
		if score > 0 {
			candidates = append(candidates, groupScore{group: group, score: score})
		}
	}

	numWritable := len(lb.r.writableGroups)

	// Try to open more groups for parallelism if:
	// - Parallel writes enabled
	// - We have fewer than max groups
	if cfg.Enabled && numWritable < cfg.MaxParallelGroups {
		// Try to open existing writable group from DB first (one that isn't already open)
		allWritable, err := lb.r.db.GetAllWritableGroups(cfg.MaxParallelGroups)
		if err == nil {
			for _, info := range allWritable {
				// Skip if already open
				if _, alreadyOpen := lb.r.writableGroups[info.GroupKey]; alreadyOpen {
					continue
				}
				group, err := lb.r.openGroup(ctx, info.GroupKey, info.Blocks, info.Bytes, info.JBHead, info.State, false)
				if err == nil {
					if session != nil {
						lb.setSessionAffinity(session, group.id)
					}
					lb.r.lk.Unlock()
					parallelMetrics.RecordGroupSelection("parallel_open", time.Since(start))
					return group, func() {}, nil
				}
				// Failed to open this one, try next
			}
		}

		// No existing groups to open, try to create a new group.
		// createGroup may block (ensureSpaceForGroup waits for offloading).
		// Release selectionLk first so other writers can still use existing
		// groups while we wait for space.
		lb.r.lk.Unlock()
		lb.selectionLk.Unlock()

		lb.r.lk.Lock()
		_, group, err := lb.r.createGroup(ctx)
		lb.r.lk.Unlock()

		lb.selectionLk.Lock()

		if err == nil {
			if session != nil {
				lb.setSessionAffinity(session, group.id)
			}
			parallelMetrics.RecordGroupSelection("parallel_created", time.Since(start))
			return group, func() {}, nil
		}

		// Failed to create — re-acquire r.lk for the fallback paths below.
		lb.r.lk.Lock()
		// Refresh candidates; map may have changed while locks were released.
		candidates = candidates[:0]
		for _, g := range lb.r.writableGroups {
			if g.state != iface.GroupStateWritable {
				continue
			}
			if s := lb.calculateScore(g, estimatedSize); s > 0 {
				candidates = append(candidates, groupScore{group: g, score: s})
			}
		}
		numWritable = len(lb.r.writableGroups)
	}

	// Use existing candidates if available
	if len(candidates) > 0 {
		best := lb.pickBest(candidates)
		lb.r.lk.Unlock()

		if session != nil && cfg.Enabled {
			lb.setSessionAffinity(session, best.id)
		}

		parallelMetrics.RecordGroupSelection("weighted", time.Since(start))
		return best, func() {}, nil
	}

	// No candidates available, need to open or create a group
	if !cfg.Enabled || numWritable < cfg.MaxParallelGroups {
		// Try to open existing writable group from DB
		selectedGroup, blk, byt, jbhead, state, err := lb.r.db.GetWritableGroup()
		if err != nil {
			lb.r.lk.Unlock()
			return nil, nil, err
		}

		if selectedGroup != iface.UndefGroupKey {
			group, err := lb.r.openGroup(ctx, selectedGroup, blk, byt, jbhead, state, false)
			if err != nil {
				lb.r.lk.Unlock()
				return nil, nil, err
			}

			if session != nil && cfg.Enabled {
				lb.setSessionAffinity(session, group.id)
			}

			lb.r.lk.Unlock()
			parallelMetrics.RecordGroupSelection("weighted", time.Since(start))
			return group, func() {}, nil
		}
	}

	// Check if we can create a new group (same unlock dance for selectionLk)
	if !cfg.Enabled || numWritable < cfg.MaxParallelGroups {
		lb.r.lk.Unlock()
		lb.selectionLk.Unlock()

		lb.r.lk.Lock()
		_, group, err := lb.r.createGroup(ctx)
		lb.r.lk.Unlock()

		lb.selectionLk.Lock()

		if err == nil {
			if session != nil && cfg.Enabled {
				lb.setSessionAffinity(session, group.id)
			}
			parallelMetrics.RecordGroupSelection("created", time.Since(start))
			return group, func() {}, nil
		}

		// Re-lock for the final return
		return nil, nil, err
	}

	lb.r.lk.Unlock()

	// All groups are full and we can't create more
	// This shouldn't happen in normal operation
	return nil, nil, ErrNoWritableGroup
}

// calculateScore computes a selection weight for a group.
// The weight is proportional to available space so that groups with more
// free space receive proportionally more writes.  This naturally creates
// staggered fill levels and avoids thundering-herd finalization.
func (lb *LoadBalancer) calculateScore(group *Group, estimatedSize int64) float64 {
	available := group.AvailableSpace()

	// Not enough space
	if available < estimatedSize {
		return 0
	}

	// Weight = available space (bytes).  Groups with 2× the free space get
	// 2× the probability of being selected.  As a group fills up its weight
	// drops, naturally sending new writes elsewhere.
	return float64(available)
}

// pickBest selects a group using weighted random sampling.
// Groups with more available space are proportionally more likely to be
// chosen, which distributes writes so that groups fill at staggered rates.
func (lb *LoadBalancer) pickBest(candidates []groupScore) *Group {
	if len(candidates) == 0 {
		return nil
	}
	if len(candidates) == 1 {
		return candidates[0].group
	}

	var total float64
	for _, c := range candidates {
		total += c.score
	}

	r := rand.Float64() * total
	for _, c := range candidates {
		r -= c.score
		if r <= 0 {
			return c.group
		}
	}

	// Fallback (shouldn't reach here due to float rounding)
	return candidates[len(candidates)-1].group
}

// setSessionAffinity records that a session prefers a specific group.
func (lb *LoadBalancer) setSessionAffinity(session *ribSession, groupKey iface.GroupKey) {
	lb.sessionAffinityLk.Lock()
	lb.sessionAffinity[session] = groupKey
	lb.sessionAffinityLk.Unlock()
}

// clearSessionAffinity removes affinity for a session.
func (lb *LoadBalancer) clearSessionAffinity(session *ribSession) {
	lb.sessionAffinityLk.Lock()
	delete(lb.sessionAffinity, session)
	lb.sessionAffinityLk.Unlock()
}

// ClearAllSessionAffinity removes all session affinities (e.g., during shutdown).
func (lb *LoadBalancer) ClearAllSessionAffinity() {
	lb.sessionAffinityLk.Lock()
	lb.sessionAffinity = make(map[*ribSession]iface.GroupKey)
	lb.sessionAffinityLk.Unlock()
}

// ErrNoWritableGroup is returned when no writable group is available.
var ErrNoWritableGroup = xerrors.New("no writable group available")

// Metrics returns current load balancer metrics.
type LoadBalancerMetrics struct {
	WritableGroupCount int
	TotalActiveWriters int32
	SessionAffinities  int
}

// Metrics returns current load balancer state for monitoring.
func (lb *LoadBalancer) Metrics() LoadBalancerMetrics {
	lb.r.lk.Lock()
	groupCount := len(lb.r.writableGroups)
	var totalWriters int32
	for _, g := range lb.r.writableGroups {
		totalWriters += g.ActiveWriterCount()
	}
	lb.r.lk.Unlock()

	lb.sessionAffinityLk.RLock()
	affinityCount := len(lb.sessionAffinity)
	lb.sessionAffinityLk.RUnlock()

	return LoadBalancerMetrics{
		WritableGroupCount: groupCount,
		TotalActiveWriters: totalWriters,
		SessionAffinities:  affinityCount,
	}
}

// parallelWritesEnabled is a cached check for whether parallel writes are enabled.
var parallelWritesEnabled atomic.Bool

func init() {
	// Will be set properly when config is loaded
	parallelWritesEnabled.Store(false)
}

// IsParallelWritesEnabled returns whether parallel writes are enabled.
func IsParallelWritesEnabled() bool {
	return parallelWritesEnabled.Load()
}

// SetParallelWritesEnabled updates the parallel writes enabled flag.
// Called when configuration is loaded.
func SetParallelWritesEnabled(enabled bool) {
	parallelWritesEnabled.Store(enabled)
	if enabled {
		log.Infow("parallel writes enabled")
	}
}
