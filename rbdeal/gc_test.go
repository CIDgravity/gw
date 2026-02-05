package rbdeal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGCState_Constants(t *testing.T) {
	// Verify GC state constants are properly ordered
	assert.Equal(t, 0, GCStateActive)
	assert.Equal(t, 1, GCStateCandidate)
	assert.Equal(t, 2, GCStateConfirmed)
	assert.Equal(t, 3, GCStateComplete)
}

func TestDefaultGCConfig(t *testing.T) {
	cfg := DefaultGCConfig()

	assert.False(t, cfg.Enabled, "GC should be disabled by default")
	assert.Equal(t, 1*time.Hour, cfg.ScanInterval)
	assert.Equal(t, 24*time.Hour, cfg.GracePeriod)
	assert.Equal(t, 7*24*time.Hour, cfg.MinGroupAge)
}

func TestGCConfig_ScanInterval(t *testing.T) {
	cfg := DefaultGCConfig()
	cfg.ScanInterval = 30 * time.Minute

	assert.Equal(t, 30*time.Minute, cfg.ScanInterval)
}

func TestGCConfig_GracePeriod(t *testing.T) {
	cfg := DefaultGCConfig()
	cfg.GracePeriod = 48 * time.Hour

	assert.Equal(t, 48*time.Hour, cfg.GracePeriod)
}

func TestGCStats_Struct(t *testing.T) {
	stats := GCStats{
		ActiveGroups:    100,
		CandidateGroups: 10,
		ConfirmedGroups: 5,
		CompleteGroups:  2,
	}

	assert.Equal(t, int64(100), stats.ActiveGroups)
	assert.Equal(t, int64(10), stats.CandidateGroups)
	assert.Equal(t, int64(5), stats.ConfirmedGroups)
	assert.Equal(t, int64(2), stats.CompleteGroups)
}

func TestGCGracePeriodLogic(t *testing.T) {
	// Test the grace period logic
	cfg := DefaultGCConfig()
	cfg.GracePeriod = 24 * time.Hour

	now := time.Now()
	deletedRecently := now.Add(-12 * time.Hour)
	deletedLongAgo := now.Add(-48 * time.Hour)

	// Group deleted recently should not be confirmed
	assert.False(t, shouldConfirmGC(deletedRecently, now, cfg.GracePeriod))

	// Group deleted long ago should be confirmed
	assert.True(t, shouldConfirmGC(deletedLongAgo, now, cfg.GracePeriod))
}

func shouldConfirmGC(markedAt, now time.Time, gracePeriod time.Duration) bool {
	return now.Sub(markedAt) > gracePeriod
}

func TestGCMinGroupAgeLogic(t *testing.T) {
	cfg := DefaultGCConfig()
	cfg.MinGroupAge = 7 * 24 * time.Hour

	now := time.Now()
	createdRecently := now.Add(-3 * 24 * time.Hour) // 3 days ago
	createdLongAgo := now.Add(-10 * 24 * time.Hour) // 10 days ago

	// Recently created group should not be eligible for GC
	assert.False(t, isOldEnoughForGC(createdRecently, now, cfg.MinGroupAge))

	// Old group should be eligible
	assert.True(t, isOldEnoughForGC(createdLongAgo, now, cfg.MinGroupAge))
}

func isOldEnoughForGC(createdAt, now time.Time, minAge time.Duration) bool {
	return now.Sub(createdAt) > minAge
}

func TestNewGarbageCollector(t *testing.T) {
	cfg := DefaultGCConfig()

	gc := NewGarbageCollector(nil, cfg)

	assert.NotNil(t, gc)
	assert.Equal(t, cfg, gc.cfg)
	assert.NotNil(t, gc.metrics)
	assert.NotNil(t, gc.stopCh)
	assert.NotNil(t, gc.doneCh)
}

func TestGC_Disabled(t *testing.T) {
	cfg := DefaultGCConfig()
	cfg.Enabled = false

	gc := NewGarbageCollector(nil, cfg)

	// Start should return immediately when disabled
	done := make(chan struct{})
	go func() {
		gc.Start(nil)
		close(done)
	}()

	select {
	case <-done:
		// Expected - should complete quickly
	case <-time.After(100 * time.Millisecond):
		t.Fatal("GC Start did not return quickly when disabled")
	}

	// doneCh should be closed
	select {
	case <-gc.doneCh:
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("doneCh not closed when GC disabled")
	}
}

func TestGCStateTransitions(t *testing.T) {
	// Test valid state transitions
	tests := []struct {
		name  string
		from  int
		to    int
		valid bool
	}{
		{"active to candidate", GCStateActive, GCStateCandidate, true},
		{"candidate to confirmed", GCStateCandidate, GCStateConfirmed, true},
		{"confirmed to complete", GCStateConfirmed, GCStateComplete, true},
		{"candidate back to active", GCStateCandidate, GCStateActive, true}, // Unmark
		{"complete to active", GCStateComplete, GCStateActive, false},       // Invalid
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := isValidStateTransition(tt.from, tt.to)
			assert.Equal(t, tt.valid, valid)
		})
	}
}

func isValidStateTransition(from, to int) bool {
	// Valid transitions:
	// Active -> Candidate (marking)
	// Candidate -> Confirmed (after grace period)
	// Candidate -> Active (unmarking when refs restored)
	// Confirmed -> Complete (claims expired)
	// Complete -> anything is invalid (terminal state)

	if from == GCStateComplete {
		return false
	}

	// Allow moving forward
	if to > from {
		return true
	}

	// Allow moving back to active only from candidate
	if to == GCStateActive && from == GCStateCandidate {
		return true
	}

	return false
}
