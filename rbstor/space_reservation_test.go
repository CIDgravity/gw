package rbstor

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	iface "github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/stretchr/testify/require"
)

// mockGroup creates a minimal Group for testing space reservation.
// It doesn't have a real CarLog or database connection.
func mockGroup(id int64, committedSize, committedBlocks int64) *Group {
	return &Group{
		id:              id,
		state:           iface.GroupStateWritable,
		committedSize:   committedSize,
		committedBlocks: committedBlocks,
		inflightSize:    0,
		inflightBlocks:  0,
		reservedSpace:   0,
	}
}

func TestSpaceReservation_Basic(t *testing.T) {
	g := mockGroup(1, 0, 0)

	// Should be able to reserve space in an empty group
	res := g.TryReserveSpace(1000)
	require.NotNil(t, res, "should reserve space in empty group")
	require.Equal(t, int32(1), g.ActiveWriterCount())
	require.Equal(t, int64(1000), g.reservedSpace)

	// Release the reservation
	res.Release()
	require.Equal(t, int32(0), g.ActiveWriterCount())
	require.Equal(t, int64(0), g.reservedSpace)
}

func TestSpaceReservation_IdempotentRelease(t *testing.T) {
	g := mockGroup(1, 0, 0)

	res := g.TryReserveSpace(1000)
	require.NotNil(t, res)

	// Release multiple times should be safe
	res.Release()
	res.Release()
	res.Release()

	require.Equal(t, int32(0), g.ActiveWriterCount())
	require.Equal(t, int64(0), g.reservedSpace)
}

func TestSpaceReservation_SpaceLimits(t *testing.T) {
	// Group with most space used
	g := mockGroup(1, maxGroupSize-100, 0)

	// Should fail to reserve more than available
	res := g.TryReserveSpace(200)
	require.Nil(t, res, "should not reserve more than available")

	// Should succeed for small reservation
	res = g.TryReserveSpace(50)
	require.NotNil(t, res, "should reserve small amount")
	res.Release()
}

func TestSpaceReservation_BlockLimits(t *testing.T) {
	// Group at block limit
	g := mockGroup(1, 0, maxGroupBlocks)

	// Should fail when at block limit
	res := g.TryReserveSpace(100)
	require.Nil(t, res, "should not reserve when at block limit")
}

func TestSpaceReservation_NonWritableGroup(t *testing.T) {
	g := mockGroup(1, 0, 0)
	g.state = iface.GroupStateFull

	// Should fail for non-writable group
	res := g.TryReserveSpace(100)
	require.Nil(t, res, "should not reserve in non-writable group")
}

func TestSpaceReservation_ConcurrentReservations(t *testing.T) {
	g := mockGroup(1, 0, 0)

	// Available space is maxGroupSize (about 29.5GB)
	// We'll try to reserve 10 concurrent chunks
	numWriters := 10
	chunkSize := int64(1000000) // 1MB each

	var wg sync.WaitGroup
	reservations := make([]*SpaceReservation, numWriters)
	var successCount atomic.Int32

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			res := g.TryReserveSpace(chunkSize)
			if res != nil {
				reservations[idx] = res
				successCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	require.Equal(t, int32(numWriters), successCount.Load(), "all writers should succeed")
	require.Equal(t, int32(numWriters), g.ActiveWriterCount())
	require.Equal(t, int64(numWriters)*chunkSize, g.reservedSpace)

	// Release all reservations
	for _, res := range reservations {
		if res != nil {
			res.Release()
		}
	}

	require.Equal(t, int32(0), g.ActiveWriterCount())
	require.Equal(t, int64(0), g.reservedSpace)
}

func TestSpaceReservation_ConcurrentWithExhaustion(t *testing.T) {
	// Small group that will fill up
	g := mockGroup(1, maxGroupSize-5000, 0)

	numWriters := 10
	chunkSize := int64(1000) // 1KB each, but only 5KB available

	var wg sync.WaitGroup
	var successCount atomic.Int32
	reservations := make([]*SpaceReservation, numWriters)
	var mu sync.Mutex

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			res := g.TryReserveSpace(chunkSize)
			if res != nil {
				mu.Lock()
				reservations[idx] = res
				mu.Unlock()
				successCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	// Only 5 should succeed (5KB available, 1KB each)
	require.Equal(t, int32(5), successCount.Load(), "only 5 writers should succeed")

	// Release all
	for _, res := range reservations {
		if res != nil {
			res.Release()
		}
	}

	require.Equal(t, int32(0), g.ActiveWriterCount())
	require.Equal(t, int64(0), g.reservedSpace)
}

func TestAvailableSpace(t *testing.T) {
	g := mockGroup(1, 1000, 10)
	g.inflightSize = 500
	g.reservedSpace = 200

	expected := maxGroupSize - 1000 - 500 - 200
	require.Equal(t, expected, g.AvailableSpace())
}

func TestHasActiveWriters(t *testing.T) {
	g := mockGroup(1, 0, 0)

	require.False(t, g.HasActiveWriters())

	res := g.TryReserveSpace(100)
	require.NotNil(t, res)
	require.True(t, g.HasActiveWriters())

	res.Release()
	require.False(t, g.HasActiveWriters())
}

func TestWaitForWritersDrain(t *testing.T) {
	g := mockGroup(1, 0, 0)

	// No writers - should return immediately
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := g.WaitForWritersDrain(ctx)
	require.NoError(t, err)

	// Start a writer
	res := g.TryReserveSpace(100)
	require.NotNil(t, res)

	// Should timeout waiting for drain
	ctx2, cancel2 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel2()
	err = g.WaitForWritersDrain(ctx2)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// Release and try again
	res.Release()
	ctx3, cancel3 := context.WithTimeout(context.Background(), time.Second)
	defer cancel3()
	err = g.WaitForWritersDrain(ctx3)
	require.NoError(t, err)
}

func TestWaitForWritersDrain_Concurrent(t *testing.T) {
	g := mockGroup(1, 0, 0)

	// Start multiple writers
	numWriters := 5
	reservations := make([]*SpaceReservation, numWriters)
	for i := 0; i < numWriters; i++ {
		reservations[i] = g.TryReserveSpace(100)
		require.NotNil(t, reservations[i])
	}

	// Start drain waiter
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done <- g.WaitForWritersDrain(ctx)
	}()

	// Release writers one by one with small delay
	for _, res := range reservations {
		time.Sleep(10 * time.Millisecond)
		res.Release()
	}

	// Drain should complete
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("drain did not complete")
	}
}
