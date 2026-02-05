package rbstor

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	iface "github.com/CIDgravity/filecoin-gateway/iface"
)

// BenchmarkSpaceReservation benchmarks space reservation operations.
func BenchmarkSpaceReservation(b *testing.B) {
	g := &Group{
		id:              1,
		state:           iface.GroupStateWritable,
		committedSize:   0,
		inflightSize:    0,
		reservedSpace:   0,
		committedBlocks: 0,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := g.TryReserveSpace(1000)
		if res != nil {
			res.Release()
		}
	}
}

// BenchmarkSpaceReservation_Parallel benchmarks concurrent space reservations.
func BenchmarkSpaceReservation_Parallel(b *testing.B) {
	g := &Group{
		id:              1,
		state:           iface.GroupStateWritable,
		committedSize:   0,
		inflightSize:    0,
		reservedSpace:   0,
		committedBlocks: 0,
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			res := g.TryReserveSpace(1000)
			if res != nil {
				res.Release()
			}
		}
	})
}

// BenchmarkLoadBalancer_CalculateScore benchmarks score calculation.
func BenchmarkLoadBalancer_CalculateScore(b *testing.B) {
	r := &rbs{
		writableGroups: make(map[iface.GroupKey]*Group),
	}
	lb := NewLoadBalancer(r)

	g := &Group{
		id:              1,
		state:           iface.GroupStateWritable,
		committedSize:   maxGroupSize / 2,
		inflightSize:    0,
		reservedSpace:   0,
		committedBlocks: 0,
	}
	g.activeWriters.Store(3)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = lb.calculateScore(g, 10000)
	}
}

// BenchmarkLoadBalancer_PickBest benchmarks best group selection.
func BenchmarkLoadBalancer_PickBest(b *testing.B) {
	r := &rbs{
		writableGroups: make(map[iface.GroupKey]*Group),
	}
	lb := NewLoadBalancer(r)

	// Create 8 candidate groups with varying scores
	candidates := make([]groupScore, 8)
	for i := range candidates {
		g := &Group{id: int64(i), state: iface.GroupStateWritable}
		candidates[i] = groupScore{group: g, score: float64(i) * 0.1}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = lb.pickBest(candidates)
	}
}

// BenchmarkSessionAffinity_Set benchmarks setting session affinity.
func BenchmarkSessionAffinity_Set(b *testing.B) {
	r := &rbs{
		writableGroups: make(map[iface.GroupKey]*Group),
	}
	lb := NewLoadBalancer(r)
	sessions := make([]*ribSession, 100)
	for i := range sessions {
		sessions[i] = &ribSession{r: r}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		session := sessions[i%len(sessions)]
		lb.setSessionAffinity(session, iface.GroupKey(i%8))
	}
}

// BenchmarkSessionAffinity_Get benchmarks checking session affinity.
func BenchmarkSessionAffinity_Get(b *testing.B) {
	r := &rbs{
		writableGroups: make(map[iface.GroupKey]*Group),
	}

	// Add writable groups
	for i := int64(1); i <= 8; i++ {
		r.writableGroups[iface.GroupKey(i)] = &Group{
			id:              i,
			state:           iface.GroupStateWritable,
			committedSize:   0,
			inflightSize:    0,
			reservedSpace:   0,
			committedBlocks: 0,
		}
	}

	lb := NewLoadBalancer(r)

	// Pre-populate affinities
	sessions := make([]*ribSession, 100)
	for i := range sessions {
		sessions[i] = &ribSession{r: r}
		lb.setSessionAffinity(sessions[i], iface.GroupKey((i%8)+1))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		session := sessions[i%len(sessions)]
		_ = lb.trySessionAffinity(session, 10000)
	}
}

// BenchmarkParallelMetrics_Record benchmarks metrics recording.
func BenchmarkParallelMetrics_Record(b *testing.B) {
	m := &ParallelWriteMetrics{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.RecordWrite(true, time.Millisecond, 1024, 1, nil)
	}
}

// BenchmarkParallelMetrics_Record_Parallel benchmarks concurrent metrics recording.
func BenchmarkParallelMetrics_Record_Parallel(b *testing.B) {
	m := &ParallelWriteMetrics{}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.RecordWrite(true, time.Millisecond, 1024, 1, nil)
		}
	})
}

// BenchmarkAvailableSpace benchmarks available space calculation.
func BenchmarkAvailableSpace(b *testing.B) {
	g := &Group{
		id:              1,
		state:           iface.GroupStateWritable,
		committedSize:   maxGroupSize / 3,
		inflightSize:    maxGroupSize / 10,
		reservedSpace:   maxGroupSize / 20,
		committedBlocks: 1000,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = g.AvailableSpace()
	}
}

// BenchmarkAvailableSpace_Parallel benchmarks concurrent available space checks.
func BenchmarkAvailableSpace_Parallel(b *testing.B) {
	g := &Group{
		id:              1,
		state:           iface.GroupStateWritable,
		committedSize:   maxGroupSize / 3,
		inflightSize:    maxGroupSize / 10,
		reservedSpace:   maxGroupSize / 20,
		committedBlocks: 1000,
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = g.AvailableSpace()
		}
	})
}

// BenchmarkLoadBalancer_Selection_Contended simulates contended group selection.
func BenchmarkLoadBalancer_Selection_Contended(b *testing.B) {
	r := &rbs{
		writableGroups: make(map[iface.GroupKey]*Group),
	}

	// Add several writable groups
	for i := int64(1); i <= 4; i++ {
		r.writableGroups[iface.GroupKey(i)] = &Group{
			id:              i,
			state:           iface.GroupStateWritable,
			committedSize:   0,
			inflightSize:    0,
			reservedSpace:   0,
			committedBlocks: 0,
		}
	}

	lb := NewLoadBalancer(r)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			lb.selectionLk.Lock()
			for _, g := range r.writableGroups {
				_ = lb.calculateScore(g, 10000)
			}
			lb.selectionLk.Unlock()
		}
	})
}

// BenchmarkActiveWriterCount benchmarks atomic active writer count operations.
func BenchmarkActiveWriterCount(b *testing.B) {
	var count atomic.Int32

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count.Add(1)
		_ = count.Load()
		count.Add(-1)
	}
}

// BenchmarkActiveWriterCount_Parallel benchmarks concurrent active writer operations.
func BenchmarkActiveWriterCount_Parallel(b *testing.B) {
	var count atomic.Int32

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			count.Add(1)
			_ = count.Load()
			count.Add(-1)
		}
	})
}

// Comparison benchmarks to show legacy vs parallel overhead

// BenchmarkMutex_Lock simulates legacy global lock pattern.
func BenchmarkMutex_Lock(b *testing.B) {
	var mu sync.Mutex

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		mu.Unlock()
	}
}

// BenchmarkMutex_Lock_Contended simulates contended global lock.
func BenchmarkMutex_Lock_Contended(b *testing.B) {
	var mu sync.Mutex

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.Lock()
			mu.Unlock()
		}
	})
}

// BenchmarkRWMutex_Read simulates read-heavy pattern.
func BenchmarkRWMutex_Read(b *testing.B) {
	var mu sync.RWMutex

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mu.RLock()
		mu.RUnlock()
	}
}

// BenchmarkRWMutex_Read_Contended simulates contended read pattern.
func BenchmarkRWMutex_Read_Contended(b *testing.B) {
	var mu sync.RWMutex

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.RLock()
			mu.RUnlock()
		}
	})
}
