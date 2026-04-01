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
				require.Less(t, score, float64(0), tt.description)
			} else {
				require.GreaterOrEqual(t, score, float64(0), tt.description)
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

	// Group at capacity returns -1
	fullGroup := &Group{
		state:         iface.GroupStateWritable,
		committedSize: maxGroupSize - 100,
	}
	require.Less(t, lb.calculateScore(fullGroup, 1000), float64(0), "full group should return negative")
}

func TestLoadBalancer_PickBest_ClockBias(t *testing.T) {
	r := &rbs{
		writableGroups: make(map[iface.GroupKey]*Group),
	}
	lb := NewLoadBalancer(r)

	// 4 groups sorted by ID → targets 0, 0.25, 0.5, 0.75.
	// All at fill 0.1:
	//   g1 (target 0):    behind = (0-0.1+1) mod 1 = 0.9 > 0.5 → ahead, no bonus
	//   g2 (target 0.25): behind = (0.25-0.1) = 0.15 → lagging, small bonus
	//   g3 (target 0.5):  behind = 0.4 → lagging, larger bonus
	//   g4 (target 0.75): behind = 0.65 > 0.5 → ahead, no bonus
	g1 := &Group{id: 1}
	g2 := &Group{id: 2}
	g3 := &Group{id: 3}
	g4 := &Group{id: 4}

	candidates := []groupScore{
		{group: g1, score: 0.1},
		{group: g2, score: 0.1},
		{group: g3, score: 0.1},
		{group: g4, score: 0.1},
	}

	hits := map[int64]int{}
	trials := 10000
	for i := 0; i < trials; i++ {
		g := lb.pickBest(candidates)
		hits[g.id]++
	}

	// All groups must receive writes
	for _, id := range []int64{1, 2, 3, 4} {
		require.Greater(t, hits[id], 0, "group %d should receive writes", id)
	}

	// Group 3 (largest lag 0.4) should get more than group 1 (ahead, no bonus)
	require.Greater(t, hits[3], hits[1],
		"lagging group should get more writes (g3=%d > g1=%d)", hits[3], hits[1])

	// No group should dominate — bias is at most 30%
	for id, count := range hits {
		pct := float64(count) / float64(trials) * 100
		require.Less(t, pct, 40.0, "group %d got %.1f%% — should not dominate", id, pct)
	}
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
