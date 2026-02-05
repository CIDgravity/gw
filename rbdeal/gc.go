package rbdeal

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Singleton pattern for GC metrics to prevent duplicate registration during tests
var (
	gcMetricsOnce     sync.Once
	gcMetricsInstance *gcMetrics
)

// GC state constants matching the SQL schema
const (
	GCStateActive    = 0 // Group is live, claims should be extended
	GCStateCandidate = 1 // Group has no live references, candidate for GC
	GCStateConfirmed = 2 // Confirmed for GC, claims will not be extended
	GCStateComplete  = 3 // Claims have expired, group can be cleaned up
)

// GarbageCollector implements passive garbage collection for groups.
// "Passive" means we don't actively delete data - we simply stop extending
// claims for groups that have no live references. The data naturally expires
// when the Filecoin deals reach their term.
type GarbageCollector struct {
	db *ribsDB

	// Configuration
	cfg GCConfig

	// Background task
	stopCh chan struct{}
	doneCh chan struct{}

	// Metrics
	metrics *gcMetrics
}

// GCConfig holds configuration for the garbage collector.
type GCConfig struct {
	// Enabled controls whether GC is active
	Enabled bool

	// ScanInterval is how often to scan for GC candidates
	ScanInterval time.Duration

	// GracePeriod is how long a group must have zero references
	// before being confirmed for GC (prevents race conditions)
	GracePeriod time.Duration

	// MinGroupAge is the minimum age of a group before it can be GC'd
	// This prevents GC of recently created groups that might not have
	// all their S3 objects indexed yet
	MinGroupAge time.Duration
}

// DefaultGCConfig returns sensible defaults.
func DefaultGCConfig() GCConfig {
	return GCConfig{
		Enabled:      false, // Disabled by default, must be explicitly enabled
		ScanInterval: 1 * time.Hour,
		GracePeriod:  24 * time.Hour,
		MinGroupAge:  7 * 24 * time.Hour, // 1 week
	}
}

type gcMetrics struct {
	scansTotal      prometheus.Counter
	candidatesFound prometheus.Counter
	groupsMarked    prometheus.Counter
	groupsConfirmed prometheus.Counter
	scanDuration    prometheus.Histogram
	candidateGroups prometheus.Gauge
	confirmedGroups prometheus.Gauge
}

func newGCMetrics() *gcMetrics {
	gcMetricsOnce.Do(func() {
		gcMetricsInstance = &gcMetrics{
			scansTotal: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "fgw",
				Subsystem: "gc",
				Name:      "scans_total",
				Help:      "Total GC scans performed",
			}),
			candidatesFound: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "fgw",
				Subsystem: "gc",
				Name:      "candidates_found_total",
				Help:      "Total GC candidates found",
			}),
			groupsMarked: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "fgw",
				Subsystem: "gc",
				Name:      "groups_marked_total",
				Help:      "Total groups marked as GC candidates",
			}),
			groupsConfirmed: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "fgw",
				Subsystem: "gc",
				Name:      "groups_confirmed_total",
				Help:      "Total groups confirmed for GC",
			}),
			scanDuration: promauto.NewHistogram(prometheus.HistogramOpts{
				Namespace: "fgw",
				Subsystem: "gc",
				Name:      "scan_duration_seconds",
				Help:      "Duration of GC scans",
				Buckets:   prometheus.ExponentialBuckets(1, 2, 12),
			}),
			candidateGroups: promauto.NewGauge(prometheus.GaugeOpts{
				Namespace: "fgw",
				Subsystem: "gc",
				Name:      "candidate_groups",
				Help:      "Current number of GC candidate groups",
			}),
			confirmedGroups: promauto.NewGauge(prometheus.GaugeOpts{
				Namespace: "fgw",
				Subsystem: "gc",
				Name:      "confirmed_groups",
				Help:      "Current number of confirmed GC groups",
			}),
		}
	})
	return gcMetricsInstance
}

// NewGarbageCollector creates a new garbage collector.
func NewGarbageCollector(db *ribsDB, cfg GCConfig) *GarbageCollector {
	return &GarbageCollector{
		db:      db,
		cfg:     cfg,
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
		metrics: newGCMetrics(),
	}
}

// Start begins the background GC process.
func (gc *GarbageCollector) Start(ctx context.Context) {
	if !gc.cfg.Enabled {
		log.Info("garbage collector disabled")
		close(gc.doneCh)
		return
	}

	go gc.run(ctx)
}

// run is the main GC loop.
func (gc *GarbageCollector) run(ctx context.Context) {
	defer close(gc.doneCh)

	log.Infow("garbage collector started",
		"scanInterval", gc.cfg.ScanInterval,
		"gracePeriod", gc.cfg.GracePeriod,
		"minGroupAge", gc.cfg.MinGroupAge)

	// Initial delay to let the system stabilize
	select {
	case <-ctx.Done():
		return
	case <-gc.stopCh:
		return
	case <-time.After(5 * time.Minute):
	}

	ticker := time.NewTicker(gc.cfg.ScanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-gc.stopCh:
			return
		case <-ticker.C:
			if err := gc.cycle(ctx); err != nil {
				log.Errorw("gc cycle failed", "error", err)
			}
		}
	}
}

// cycle performs one GC scan cycle.
func (gc *GarbageCollector) cycle(ctx context.Context) error {
	start := time.Now()
	defer func() {
		gc.metrics.scansTotal.Inc()
		gc.metrics.scanDuration.Observe(time.Since(start).Seconds())
	}()

	log.Debug("starting gc cycle")

	// Phase 1: Find new GC candidates (groups with no live blocks)
	candidates, err := gc.findCandidates(ctx)
	if err != nil {
		return err
	}

	gc.metrics.candidatesFound.Add(float64(len(candidates)))
	log.Debugw("found gc candidates", "count", len(candidates))

	// Phase 2: Mark new candidates
	for _, groupID := range candidates {
		if err := gc.markCandidate(ctx, groupID); err != nil {
			log.Warnw("failed to mark gc candidate", "group", groupID, "error", err)
			continue
		}
		gc.metrics.groupsMarked.Inc()
	}

	// Phase 3: Confirm candidates that have passed the grace period
	confirmed, err := gc.confirmCandidates(ctx)
	if err != nil {
		return err
	}

	gc.metrics.groupsConfirmed.Add(float64(confirmed))
	log.Debugw("confirmed gc candidates", "count", confirmed)

	// Update gauges
	gc.updateGauges(ctx)

	return nil
}

// findCandidates finds groups that are candidates for GC.
// A group is a candidate if:
// - It's in offloaded state (g_state = 4)
// - It has no live blocks (live_blocks = 0)
// - It's not already marked for GC
// - It's older than MinGroupAge
func (gc *GarbageCollector) findCandidates(ctx context.Context) ([]int64, error) {
	minAge := time.Now().Add(-gc.cfg.MinGroupAge)

	rows, err := gc.db.db.QueryContext(ctx, `
		SELECT id FROM groups 
		WHERE g_state = 4 
		AND live_blocks = 0 
		AND gc_state = 0
		AND id < $1
		ORDER BY id
		LIMIT 1000
	`, gc.estimateGroupIDForTime(minAge))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var candidates []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		candidates = append(candidates, id)
	}

	return candidates, rows.Err()
}

// estimateGroupIDForTime estimates the group ID for a given time.
// Groups are created sequentially, so older groups have lower IDs.
// This is a rough estimate used to filter old groups.
func (gc *GarbageCollector) estimateGroupIDForTime(t time.Time) int64 {
	// Get the max group ID and estimate based on time difference
	// This is a simple heuristic - in practice, you might want to
	// store creation timestamps
	var maxID int64
	err := gc.db.db.QueryRow("SELECT COALESCE(MAX(id), 0) FROM groups").Scan(&maxID)
	if err != nil {
		return 0
	}

	// Assume roughly 100 groups per day (adjust based on actual usage)
	daysAgo := time.Since(t).Hours() / 24
	estimatedID := maxID - int64(daysAgo*100)
	if estimatedID < 0 {
		estimatedID = 0
	}
	return estimatedID
}

// markCandidate marks a group as a GC candidate.
func (gc *GarbageCollector) markCandidate(ctx context.Context, groupID int64) error {
	_, err := gc.db.db.ExecContext(ctx, `
		UPDATE groups 
		SET gc_state = $1, gc_marked_at = NOW() 
		WHERE id = $2 AND gc_state = 0
	`, GCStateCandidate, groupID)
	return err
}

// confirmCandidates confirms candidates that have passed the grace period.
func (gc *GarbageCollector) confirmCandidates(ctx context.Context) (int, error) {
	graceTime := time.Now().Add(-gc.cfg.GracePeriod)

	result, err := gc.db.db.ExecContext(ctx, `
		UPDATE groups 
		SET gc_state = $1 
		WHERE gc_state = $2 
		AND gc_marked_at < $3
		AND live_blocks = 0
	`, GCStateConfirmed, GCStateCandidate, graceTime)
	if err != nil {
		return 0, err
	}

	affected, _ := result.RowsAffected()
	return int(affected), nil
}

// updateGauges updates the gauge metrics.
func (gc *GarbageCollector) updateGauges(ctx context.Context) {
	var candidateCount, confirmedCount int64

	gc.db.db.QueryRow("SELECT COUNT(*) FROM groups WHERE gc_state = $1", GCStateCandidate).Scan(&candidateCount)
	gc.db.db.QueryRow("SELECT COUNT(*) FROM groups WHERE gc_state = $1", GCStateConfirmed).Scan(&confirmedCount)

	gc.metrics.candidateGroups.Set(float64(candidateCount))
	gc.metrics.confirmedGroups.Set(float64(confirmedCount))
}

// Stop stops the garbage collector.
func (gc *GarbageCollector) Stop() {
	close(gc.stopCh)
	<-gc.doneCh
}

// IsGroupGCCandidate checks if a group is marked for GC.
// Used by the claim extender to skip GC'd groups.
func (gc *GarbageCollector) IsGroupGCCandidate(ctx context.Context, groupID int64) (bool, error) {
	var gcState int
	err := gc.db.db.QueryRow(
		"SELECT gc_state FROM groups WHERE id = $1",
		groupID,
	).Scan(&gcState)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return gcState >= GCStateCandidate, nil
}

// GetGCCandidateGroups returns all groups that are GC candidates or confirmed.
// Used by the claim extender to skip these groups.
func (gc *GarbageCollector) GetGCCandidateGroups(ctx context.Context) ([]int64, error) {
	rows, err := gc.db.db.QueryContext(ctx, `
		SELECT id FROM groups 
		WHERE gc_state >= $1
		ORDER BY id
	`, GCStateCandidate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var groups []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		groups = append(groups, id)
	}
	return groups, rows.Err()
}

// UnmarkGroup removes GC marking from a group.
// Called when a group gets new references and should no longer be GC'd.
func (gc *GarbageCollector) UnmarkGroup(ctx context.Context, groupID int64) error {
	_, err := gc.db.db.ExecContext(ctx, `
		UPDATE groups 
		SET gc_state = $1, gc_marked_at = NULL 
		WHERE id = $2 AND gc_state < $3
	`, GCStateActive, groupID, GCStateConfirmed)
	return err
}

// UpdateLiveBlocks updates the live_blocks count for a group.
// This should be called when S3 objects are created/deleted.
func (gc *GarbageCollector) UpdateLiveBlocks(ctx context.Context, groupID int64, delta int64) error {
	_, err := gc.db.db.ExecContext(ctx, `
		UPDATE groups 
		SET live_blocks = GREATEST(0, live_blocks + $1)
		WHERE id = $2
	`, delta, groupID)
	return err
}

// GCStats returns current GC statistics.
type GCStats struct {
	ActiveGroups    int64
	CandidateGroups int64
	ConfirmedGroups int64
	CompleteGroups  int64
}

// Stats returns current GC statistics.
func (gc *GarbageCollector) Stats(ctx context.Context) (GCStats, error) {
	var stats GCStats

	rows, err := gc.db.db.QueryContext(ctx, `
		SELECT gc_state, COUNT(*) 
		FROM groups 
		GROUP BY gc_state
	`)
	if err != nil {
		return stats, err
	}
	defer rows.Close()

	for rows.Next() {
		var state int
		var count int64
		if err := rows.Scan(&state, &count); err != nil {
			return stats, err
		}
		switch state {
		case GCStateActive:
			stats.ActiveGroups = count
		case GCStateCandidate:
			stats.CandidateGroups = count
		case GCStateConfirmed:
			stats.ConfirmedGroups = count
		case GCStateComplete:
			stats.CompleteGroups = count
		}
	}

	return stats, rows.Err()
}
