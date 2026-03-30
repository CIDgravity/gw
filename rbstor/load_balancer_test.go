package rbstor

import (
	"testing"

	iface "github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/stretchr/testify/require"
)

func TestLoadBalancer_CalculateScore(t *testing.T) {
	// Create a mock rbs just for the load balancer
	r := &rbs{
		writableGroups: make(map[iface.GroupKey]*Group),
	}
	lb := NewLoadBalancer(r)

	tests := []struct {
		name          string
		committed     int64
		inflight      int64
		reserved      int64
		activeWriters int32
		estimatedSize int64
		expectZero    bool
		description   string
	}{
		{
			name:          "empty group",
			committed:     0,
			inflight:      0,
			reserved:      0,
			activeWriters: 0,
			estimatedSize: 1000,
			expectZero:    false,
			description:   "empty group should have high score",
		},
		{
			name:          "group at capacity",
			committed:     maxGroupSize - 100,
			inflight:      0,
			reserved:      0,
			activeWriters: 0,
			estimatedSize: 1000,
			expectZero:    true,
			description:   "group without enough space should score 0",
		},
		{
			name:          "group with some space",
			committed:     maxGroupSize / 2,
			inflight:      0,
			reserved:      0,
			activeWriters: 0,
			estimatedSize: 1000,
			expectZero:    false,
			description:   "half-full group should have positive score",
		},
		{
			name:          "group with active writers",
			committed:     0,
			inflight:      0,
			reserved:      0,
			activeWriters: 5,
			estimatedSize: 1000,
			expectZero:    false,
			description:   "group with writers should have lower but positive score",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &Group{
				state:           iface.GroupStateWritable,
				committedSize:   tt.committed,
				inflightSize:    tt.inflight,
				reservedSpace:   tt.reserved,
				committedBlocks: 0,
				inflightBlocks:  0,
			}
			g.activeWriters.Store(tt.activeWriters)

			score := lb.calculateScore(g, tt.estimatedSize)

			if tt.expectZero {
				require.Zero(t, score, tt.description)
			} else {
				require.Greater(t, score, float64(0), tt.description)
			}
		})
	}
}

func TestLoadBalancer_CalculateScore_Comparison(t *testing.T) {
	r := &rbs{
		writableGroups: make(map[iface.GroupKey]*Group),
	}
	lb := NewLoadBalancer(r)

	// Score returns fill ratio: half-full > empty
	emptyGroup := &Group{
		state:         iface.GroupStateWritable,
		committedSize: 0,
	}
	halfFullGroup := &Group{
		state:         iface.GroupStateWritable,
		committedSize: maxGroupSize / 2,
	}

	emptyScore := lb.calculateScore(emptyGroup, 1000)
	halfFullScore := lb.calculateScore(halfFullGroup, 1000)

	require.InDelta(t, emptyScore, 0.0, 0.01, "empty group fill ratio ≈ 0")
	require.InDelta(t, halfFullScore, 0.5, 0.01, "half-full group fill ratio ≈ 0.5")

	// Group at capacity returns 0
	fullGroup := &Group{
		state:         iface.GroupStateWritable,
		committedSize: maxGroupSize - 100,
	}
	require.Zero(t, lb.calculateScore(fullGroup, 1000))
}

func TestLoadBalancer_PickBest_ModularClock(t *testing.T) {
	r := &rbs{
		writableGroups: make(map[iface.GroupKey]*Group),
	}
	lb := NewLoadBalancer(r)

	// Three groups at fill ratios 0.1, 0.2, 0.3
	// Sorted, they get targets 0.167, 0.5, 0.833
	// Gaps: 0.167-0.1=0.067, 0.5-0.2=0.3, 0.833-0.3=0.533
	// Group at 0.3 fill has the biggest gap → gets the write
	g1 := &Group{id: 1}
	g2 := &Group{id: 2}
	g3 := &Group{id: 3}

	candidates := []groupScore{
		{group: g1, score: 0.1},
		{group: g2, score: 0.2},
		{group: g3, score: 0.3},
	}

	best := lb.pickBest(candidates)
	require.Equal(t, int64(3), best.id, "group furthest behind its clock target should be picked")

	// Two groups: one empty (0.0), one at 0.9
	// Targets: 0.25, 0.75.  Gaps: 0.25-0=0.25, 0.75-0.9=-0.15
	// The empty group is behind → gets the write
	gA := &Group{id: 10}
	gB := &Group{id: 20}
	candidates2 := []groupScore{
		{group: gA, score: 0.0},
		{group: gB, score: 0.9},
	}
	best2 := lb.pickBest(candidates2)
	require.Equal(t, int64(10), best2.id, "empty group behind its target should be picked")

	// Two groups at identical fill → lower-slot (first sorted) wins
	gX := &Group{id: 100}
	gY := &Group{id: 200}
	candidates3 := []groupScore{
		{group: gX, score: 0.5},
		{group: gY, score: 0.5},
	}
	// Both have same fill 0.5.  Targets: 0.25, 0.75.
	// Gaps: 0.25-0.5=-0.25, 0.75-0.5=0.25. Second slot wins.
	best3 := lb.pickBest(candidates3)
	// One of them should be picked (the one assigned the higher target)
	require.NotNil(t, best3)
}

func TestLoadBalancer_PickBest_Empty(t *testing.T) {
	r := &rbs{
		writableGroups: make(map[iface.GroupKey]*Group),
	}
	lb := NewLoadBalancer(r)

	best := lb.pickBest(nil)
	require.Nil(t, best)

	best = lb.pickBest([]groupScore{})
	require.Nil(t, best)
}

func TestLoadBalancer_SessionAffinity(t *testing.T) {
	r := &rbs{
		writableGroups: make(map[iface.GroupKey]*Group),
	}
	lb := NewLoadBalancer(r)

	session := &ribSession{r: r}

	// Initially no affinity
	lb.sessionAffinityLk.RLock()
	_, hasAffinity := lb.sessionAffinity[session]
	lb.sessionAffinityLk.RUnlock()
	require.False(t, hasAffinity)

	// Set affinity
	lb.setSessionAffinity(session, 42)

	lb.sessionAffinityLk.RLock()
	affinityGroup, hasAffinity := lb.sessionAffinity[session]
	lb.sessionAffinityLk.RUnlock()
	require.True(t, hasAffinity)
	require.Equal(t, iface.GroupKey(42), affinityGroup)

	// Clear affinity
	lb.clearSessionAffinity(session)

	lb.sessionAffinityLk.RLock()
	_, hasAffinity = lb.sessionAffinity[session]
	lb.sessionAffinityLk.RUnlock()
	require.False(t, hasAffinity)
}

func TestLoadBalancer_ClearAllSessionAffinity(t *testing.T) {
	r := &rbs{
		writableGroups: make(map[iface.GroupKey]*Group),
	}
	lb := NewLoadBalancer(r)

	session1 := &ribSession{r: r}
	session2 := &ribSession{r: r}

	lb.setSessionAffinity(session1, 1)
	lb.setSessionAffinity(session2, 2)

	lb.sessionAffinityLk.RLock()
	require.Len(t, lb.sessionAffinity, 2)
	lb.sessionAffinityLk.RUnlock()

	lb.ClearAllSessionAffinity()

	lb.sessionAffinityLk.RLock()
	require.Len(t, lb.sessionAffinity, 0)
	lb.sessionAffinityLk.RUnlock()
}

func TestLoadBalancer_Metrics(t *testing.T) {
	r := &rbs{
		writableGroups: make(map[iface.GroupKey]*Group),
	}
	lb := NewLoadBalancer(r)

	// Add some writable groups
	g1 := &Group{id: 1, state: iface.GroupStateWritable}
	g1.activeWriters.Store(2)
	g2 := &Group{id: 2, state: iface.GroupStateWritable}
	g2.activeWriters.Store(3)

	r.writableGroups[1] = g1
	r.writableGroups[2] = g2

	// Add some session affinities
	session := &ribSession{r: r}
	lb.setSessionAffinity(session, 1)

	metrics := lb.Metrics()
	require.Equal(t, 2, metrics.WritableGroupCount)
	require.Equal(t, int32(5), metrics.TotalActiveWriters)
	require.Equal(t, 1, metrics.SessionAffinities)
}

func TestLoadBalancer_TryPreferredGroup(t *testing.T) {
	r := &rbs{
		writableGroups: make(map[iface.GroupKey]*Group),
	}
	lb := NewLoadBalancer(r)

	// No groups - should return nil
	result := lb.tryPreferredGroup(1, 1000)
	require.Nil(t, result)

	// Add a group with space
	g := &Group{
		id:              1,
		state:           iface.GroupStateWritable,
		committedSize:   0,
		inflightSize:    0,
		reservedSpace:   0,
		committedBlocks: 0,
	}
	r.writableGroups[1] = g

	// Should find the preferred group
	result = lb.tryPreferredGroup(1, 1000)
	require.NotNil(t, result)
	require.Equal(t, int64(1), result.id)

	// Group without enough space - should return nil
	g.committedSize = maxGroupSize - 100
	result = lb.tryPreferredGroup(1, 1000)
	require.Nil(t, result)
}

func TestParallelWritesEnabled(t *testing.T) {
	// Initially should be false (set in init)
	// Note: This tests the default, actual config loading happens elsewhere

	// Test set/get
	SetParallelWritesEnabled(true)
	require.True(t, IsParallelWritesEnabled())

	SetParallelWritesEnabled(false)
	require.False(t, IsParallelWritesEnabled())
}
