package rbdeal

import (
	"testing"
	"time"

	iface2 "github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/stretchr/testify/require"
)

func TestRecordProviderCooldownBumpsAttempts(t *testing.T) {
	r := &ribs{providerCooldowns: map[int64]providerCooldownState{}}

	r.recordProviderCooldown(1234, ErrRejected{Reason: "provider busy, try again later"})
	first, ok := r.providerCooldown(1234, time.Now())
	require.True(t, ok)
	require.Equal(t, int64(1), first.Attempts)
	require.Equal(t, "busy", first.Key)
	require.WithinDuration(t, time.Now().Add(time.Minute), first.Until, 5*time.Second)

	r.recordProviderCooldown(1234, ErrRejected{Reason: "provider busy, try again later"})
	second, ok := r.providerCooldown(1234, time.Now())
	require.True(t, ok)
	require.Equal(t, int64(2), second.Attempts)
	require.WithinDuration(t, time.Now().Add(2*time.Minute), second.Until, 5*time.Second)
}

func TestProviderCooldownExpiresAndClears(t *testing.T) {
	r := &ribs{providerCooldowns: map[int64]providerCooldownState{
		1234: {
			Key:      "busy",
			Reason:   "busy",
			Until:    time.Now().Add(-time.Minute),
			Attempts: 1,
		},
	}}

	_, ok := r.providerCooldown(1234, time.Now())
	require.False(t, ok)
	require.Empty(t, r.providerCooldowns)
}

func TestApplyProviderCooldownAddsDiagnostics(t *testing.T) {
	r := &ribs{providerCooldowns: map[int64]providerCooldownState{
		1234: {
			Key:      "connect",
			Reason:   "connect to miner: i/o timeout",
			Until:    time.Now().Add(15 * time.Minute),
			Attempts: 2,
		},
	}}

	meta := iface2.ProviderMeta{ID: 1234}
	r.applyProviderCooldown(&meta, time.Now())

	require.Equal(t, int64(2), meta.DealCooldownAttempts)
	require.Equal(t, "connect to miner: i/o timeout", meta.DealCooldownReason)
	require.NotZero(t, meta.DealCooldownUntil)
}
