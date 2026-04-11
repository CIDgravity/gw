package rbdeal

import (
	"fmt"
	"strings"
	"time"

	iface2 "github.com/CIDgravity/filecoin-gateway/iface"
)

type providerCooldownState struct {
	Key      string
	Reason   string
	Until    time.Time
	Attempts int64
}

type providerCooldownPolicy struct {
	key    string
	reason string
	base   time.Duration
	max    time.Duration
}

func providerCooldownForError(err error) providerCooldownPolicy {
	if err == nil {
		return providerCooldownPolicy{}
	}

	if rejected, ok := err.(ErrRejected); ok {
		reason := strings.TrimSpace(rejected.Reason)
		lower := strings.ToLower(reason)
		switch {
		case strings.Contains(lower, "busy"), strings.Contains(lower, "try again"), strings.Contains(lower, "too many"), strings.Contains(lower, "later"), strings.Contains(lower, "temporarily unavailable"):
			return providerCooldownPolicy{key: "busy", reason: reason, base: 30 * time.Minute, max: 4 * time.Hour}
		default:
			return providerCooldownPolicy{key: "rejected", reason: reason, base: time.Hour, max: 12 * time.Hour}
		}
	}

	reason := strings.TrimSpace(err.Error())
	lower := strings.ToLower(reason)

	switch {
	case strings.Contains(lower, "does not support protocol version"):
		return providerCooldownPolicy{key: "protocol", reason: reason, base: 6 * time.Hour, max: 24 * time.Hour}
	case strings.Contains(lower, "connect to miner"), strings.Contains(lower, "opening deal proposal stream"), strings.Contains(lower, "connection refused"), strings.Contains(lower, "i/o timeout"), strings.Contains(lower, "deadline exceeded"), strings.Contains(lower, "stream reset"):
		return providerCooldownPolicy{key: "connect", reason: reason, base: 15 * time.Minute, max: 2 * time.Hour}
	default:
		return providerCooldownPolicy{key: "error", reason: reason, base: 20 * time.Minute, max: 3 * time.Hour}
	}
}

func (r *ribs) recordProviderCooldown(provider int64, err error) {
	policy := providerCooldownForError(err)
	if policy.key == "" {
		return
	}

	now := time.Now()
	attempts := int64(1)

	r.providerCooldownsLk.Lock()
	defer r.providerCooldownsLk.Unlock()

	if cur, ok := r.providerCooldowns[provider]; ok {
		if cur.Key == policy.key && cur.Until.After(now) {
			attempts = cur.Attempts + 1
		}
	}

	duration := policy.base
	for i := int64(1); i < attempts; i++ {
		duration *= 2
		if duration >= policy.max {
			duration = policy.max
			break
		}
	}

	r.providerCooldowns[provider] = providerCooldownState{
		Key:      policy.key,
		Reason:   policy.reason,
		Until:    now.Add(duration),
		Attempts: attempts,
	}
}

func (r *ribs) clearProviderCooldown(provider int64) {
	r.providerCooldownsLk.Lock()
	defer r.providerCooldownsLk.Unlock()
	delete(r.providerCooldowns, provider)
}

func (r *ribs) providerCooldown(provider int64, now time.Time) (providerCooldownState, bool) {
	r.providerCooldownsLk.Lock()
	defer r.providerCooldownsLk.Unlock()

	state, ok := r.providerCooldowns[provider]
	if !ok {
		return providerCooldownState{}, false
	}
	if !state.Until.After(now) {
		delete(r.providerCooldowns, provider)
		return providerCooldownState{}, false
	}
	return state, true
}

func (r *ribs) applyProviderCooldown(meta *iface2.ProviderMeta, now time.Time) {
	state, ok := r.providerCooldown(meta.ID, now)
	if !ok {
		meta.DealCooldownUntil = 0
		meta.DealCooldownAttempts = 0
		meta.DealCooldownReason = ""
		return
	}

	meta.DealCooldownUntil = state.Until.Unix()
	meta.DealCooldownAttempts = state.Attempts
	meta.DealCooldownReason = state.Reason
	if meta.DealCooldownReason == "" {
		meta.DealCooldownReason = fmt.Sprintf("provider cooldown (%s)", state.Key)
	}
}
