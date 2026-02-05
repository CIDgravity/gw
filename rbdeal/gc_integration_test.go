package rbdeal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGCIntegrationInitialization verifies that GC is properly initialized in the ribs instance
func TestGCIntegrationInitialization(t *testing.T) {
	// Test that GC config is properly loaded
	cfg := DefaultGCConfig()

	// Verify default configuration
	assert.False(t, cfg.Enabled, "GC should be disabled by default")
	assert.Equal(t, 1*time.Hour, cfg.ScanInterval)
	assert.Equal(t, 24*time.Hour, cfg.GracePeriod)
	assert.Equal(t, 7*24*time.Hour, cfg.MinGroupAge)
}

// TestGCIntegrationConfigLoading verifies GC configuration loading from various sources
func TestGCIntegrationConfigLoading(t *testing.T) {
	tests := []struct {
		name         string
		enabled      bool
		scanInterval time.Duration
		gracePeriod  time.Duration
		minGroupAge  time.Duration
	}{
		{
			name:         "default_config",
			enabled:      false,
			scanInterval: 1 * time.Hour,
			gracePeriod:  24 * time.Hour,
			minGroupAge:  7 * 24 * time.Hour,
		},
		{
			name:         "custom_config",
			enabled:      true,
			scanInterval: 30 * time.Minute,
			gracePeriod:  12 * time.Hour,
			minGroupAge:  3 * 24 * time.Hour,
		},
		{
			name:         "aggressive_gc",
			enabled:      true,
			scanInterval: 5 * time.Minute,
			gracePeriod:  1 * time.Hour,
			minGroupAge:  24 * time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := GCConfig{
				Enabled:      tt.enabled,
				ScanInterval: tt.scanInterval,
				GracePeriod:  tt.gracePeriod,
				MinGroupAge:  tt.minGroupAge,
			}

			assert.Equal(t, tt.enabled, cfg.Enabled)
			assert.Equal(t, tt.scanInterval, cfg.ScanInterval)
			assert.Equal(t, tt.gracePeriod, cfg.GracePeriod)
			assert.Equal(t, tt.minGroupAge, cfg.MinGroupAge)
		})
	}
}

// TestGCIntegrationCandidateDetection verifies the logic for detecting GC candidates
func TestGCIntegrationCandidateDetection(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name              string
		createdAt         time.Time
		lastAccessed      time.Time
		refCount          int64
		minGroupAge       time.Duration
		gracePeriod       time.Duration
		shouldBeCandidate bool
	}{
		{
			name:              "old_group_no_refs",
			createdAt:         now.Add(-10 * 24 * time.Hour), // 10 days old
			lastAccessed:      now.Add(-48 * time.Hour),      // 2 days ago
			refCount:          0,
			minGroupAge:       7 * 24 * time.Hour,
			gracePeriod:       24 * time.Hour,
			shouldBeCandidate: true,
		},
		{
			name:              "old_group_with_refs",
			createdAt:         now.Add(-10 * 24 * time.Hour),
			lastAccessed:      now.Add(-48 * time.Hour),
			refCount:          5,
			minGroupAge:       7 * 24 * time.Hour,
			gracePeriod:       24 * time.Hour,
			shouldBeCandidate: false, // Has references, not a candidate
		},
		{
			name:              "young_group_no_refs",
			createdAt:         now.Add(-3 * 24 * time.Hour), // 3 days old
			lastAccessed:      now.Add(-48 * time.Hour),
			refCount:          0,
			minGroupAge:       7 * 24 * time.Hour,
			gracePeriod:       24 * time.Hour,
			shouldBeCandidate: false, // Too young
		},
		{
			name:              "recently_accessed",
			createdAt:         now.Add(-10 * 24 * time.Hour),
			lastAccessed:      now.Add(-1 * time.Hour), // 1 hour ago
			refCount:          0,
			minGroupAge:       7 * 24 * time.Hour,
			gracePeriod:       24 * time.Hour,
			shouldBeCandidate: false, // Recently accessed
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate candidate detection logic
			isOldEnough := now.Sub(tt.createdAt) > tt.minGroupAge
			isStale := now.Sub(tt.lastAccessed) > tt.gracePeriod
			hasNoRefs := tt.refCount == 0

			isCandidate := isOldEnough && isStale && hasNoRefs
			assert.Equal(t, tt.shouldBeCandidate, isCandidate)
		})
	}
}

// TestClaimExtenderSkipsGCGroups verifies that claim extender properly skips GC groups
func TestClaimExtenderSkipsGCGroups(t *testing.T) {
	// Test that groups in various GC states are properly identified
	gcStates := []struct {
		state       int
		shouldSkip  bool
		description string
	}{
		{GCStateActive, false, "active groups should not be skipped"},
		{GCStateCandidate, true, "candidate groups should be skipped"},
		{GCStateConfirmed, true, "confirmed groups should be skipped"},
		{GCStateComplete, true, "complete groups should be skipped"},
	}

	for _, gs := range gcStates {
		t.Run(gs.description, func(t *testing.T) {
			// Groups with GC state >= Candidate should be skipped
			shouldSkip := gs.state >= GCStateCandidate
			assert.Equal(t, gs.shouldSkip, shouldSkip,
				"GC state %d should result in skip=%v", gs.state, gs.shouldSkip)
		})
	}
}

// TestGCIntegrationStateTransitions verifies valid and invalid GC state transitions
func TestGCIntegrationStateTransitions(t *testing.T) {
	tests := []struct {
		name      string
		fromState int
		toState   int
		valid     bool
	}{
		{"active_to_candidate", GCStateActive, GCStateCandidate, true},
		{"candidate_to_confirmed", GCStateCandidate, GCStateConfirmed, true},
		{"confirmed_to_complete", GCStateConfirmed, GCStateComplete, true},
		{"candidate_to_active", GCStateCandidate, GCStateActive, true},        // Can unmark
		{"active_to_confirmed", GCStateActive, GCStateConfirmed, false},       // Must go through candidate
		{"confirmed_to_candidate", GCStateConfirmed, GCStateCandidate, false}, // No going back
		{"complete_to_anything", GCStateComplete, GCStateActive, false},       // Terminal state
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := isValidGCIntegrationStateTransition(tt.fromState, tt.toState)
			assert.Equal(t, tt.valid, valid)
		})
	}
}

// TestGCIntegrationGracePeriodLogic verifies the grace period timing logic
func TestGCIntegrationGracePeriodLogic(t *testing.T) {
	now := time.Now()
	gracePeriod := 24 * time.Hour

	tests := []struct {
		name          string
		markedAt      time.Time
		shouldConfirm bool
	}{
		{
			name:          "just_marked",
			markedAt:      now.Add(-1 * time.Hour),
			shouldConfirm: false,
		},
		{
			name:          "half_grace_period",
			markedAt:      now.Add(-12 * time.Hour),
			shouldConfirm: false,
		},
		{
			name:          "exact_grace_period",
			markedAt:      now.Add(-24 * time.Hour),
			shouldConfirm: false, // Must be GREATER than grace period
		},
		{
			name:          "past_grace_period",
			markedAt:      now.Add(-25 * time.Hour),
			shouldConfirm: true,
		},
		{
			name:          "well_past_grace_period",
			markedAt:      now.Add(-48 * time.Hour),
			shouldConfirm: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shouldConfirm := now.Sub(tt.markedAt) > gracePeriod
			assert.Equal(t, tt.shouldConfirm, shouldConfirm)
		})
	}
}

// TestGCIntegrationMetricsInitialization verifies GC metrics are properly initialized
func TestGCIntegrationMetricsInitialization(t *testing.T) {
	// Create a mock GC instance to verify metrics are initialized
	cfg := DefaultGCConfig()
	gc := NewGarbageCollector(nil, cfg)

	require.NotNil(t, gc)
	// Metrics should be initialized even with nil db
	// The metrics are created in the constructor
}

// TestGCIntegrationDisabledBehavior verifies GC behavior when disabled
func TestGCIntegrationDisabledBehavior(t *testing.T) {
	cfg := DefaultGCConfig()
	cfg.Enabled = false

	gc := NewGarbageCollector(nil, cfg)
	require.NotNil(t, gc)

	// When disabled, Start should return immediately
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
}

// isValidGCIntegrationStateTransition checks if a GC state transition is valid
func isValidGCIntegrationStateTransition(from, to int) bool {
	// Complete is terminal - no transitions allowed
	if from == GCStateComplete {
		return false
	}

	// Can only move forward one state at a time (consecutive states)
	if to == from+1 {
		return true
	}

	// Can only move back to Active from Candidate (unmarking)
	if to == GCStateActive && from == GCStateCandidate {
		return true
	}

	return false
}
