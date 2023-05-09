package ributil

import (
	"golang.org/x/xerrors"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type RateCounters[K comparable] struct {
	lk              sync.Mutex
	counters        map[K]*RateCounter
	globalTransfers atomic.Int64

	rateFunc RateFunc
}

type RateFunc func(transferRateMbps float64, peerTransfers, totalTransfers int64) error

func MinAvgGlobalLogPeerRate(minTxRateMbps, linkMbps float64) RateFunc {
	return func(transferRateMbps float64, peerTransfers, totalTransfers int64) error {
		peerTransferFactor := math.Log2(float64(peerTransfers) + 1)
		minPeerTransferRate := minTxRateMbps * peerTransferFactor

		maxAvgTransferRate := linkMbps / float64(totalTransfers)
		if maxAvgTransferRate < minPeerTransferRate {
			minPeerTransferRate = maxAvgTransferRate
		}

		if transferRateMbps < minPeerTransferRate {
			return xerrors.Errorf("transfer rate %.3fMbps less than minimum %.3fMbps (%d peer tx, %d global tx)", transferRateMbps, minPeerTransferRate, peerTransfers, totalTransfers)
		}

		return nil
	}
}

func NewRateCounters[K comparable](rateFunc RateFunc) *RateCounters[K] {
	return &RateCounters[K]{
		counters: make(map[K]*RateCounter),
		rateFunc: rateFunc,
	}
}

func (rc *RateCounters[K]) Get(key K) *RateCounter {
	rc.lk.Lock()
	defer rc.lk.Unlock()

	c, ok := rc.counters[key]
	if !ok {
		c = &RateCounter{
			rateFunc:        rc.rateFunc,
			globalTransfers: &rc.globalTransfers,

			unlink: func(check func() bool) {
				rc.lk.Lock()
				defer rc.lk.Unlock()

				rc.globalTransfers.Add(-1)

				if check() {
					delete(rc.counters, key)
				}
			},
		}
		rc.counters[key] = c
	}

	rc.globalTransfers.Add(1)
	c.transfers.Add(1)

	return c
}

type RateCounter struct {
	transferred atomic.Int64

	lk sync.Mutex

	// only write with RateCounters.lk (inside unlink check func)
	transfers atomic.Int64

	globalTransfers *atomic.Int64

	rateFunc RateFunc
	unlink   func(func() bool)
}

func (rc *RateCounter) Release() {
	rc.lk.Lock()
	defer rc.lk.Unlock()

	rc.release()
}

func (rc *RateCounter) release() {
	rc.unlink(func() bool {
		rc.transfers.Add(-1)
		return rc.transfers.Load() == 0
	})
}

// Check allows only single concurrent check per peer - this is to prevent
// multiple concurrent checks causing all transfers to fail at once.
// When we drop a peer, we'll reduce rc.transfers, so the next check will
// require less total bandwidth (assuming that MinAvgGlobalLogPeerRate is used).
func (rc *RateCounter) Check(cb func() error) error {
	rc.lk.Lock()
	defer rc.lk.Unlock()

	err := cb()
	if err != nil {
		rc.release()
	}

	return err
}

type RateEnforcingWriter struct {
	w        io.Writer
	rateMbps float64

	writeError error

	rc *RateCounter

	bytesTransferredSnap int64
	lastSpeedCheck       time.Time
	windowDuration       time.Duration
}

func NewRateEnforcingWriter(w io.Writer, rc *RateCounter, windowDuration time.Duration) *RateEnforcingWriter {
	return &RateEnforcingWriter{
		w:              w,
		rc:             rc,
		windowDuration: windowDuration,
	}
}

func (rew *RateEnforcingWriter) Write(p []byte) (int, error) {
	if rew.writeError != nil {
		return 0, rew.writeError
	}

	now := time.Now()

	if !rew.lastSpeedCheck.IsZero() && now.Sub(rew.lastSpeedCheck) >= rew.windowDuration {
		elapsedTime := now.Sub(rew.lastSpeedCheck)

		checkErr := rew.rc.Check(func() error {
			ctrTransferred := rew.rc.transferred.Load()
			transferredInWindow := ctrTransferred - rew.bytesTransferredSnap

			rew.bytesTransferredSnap = ctrTransferred
			rew.lastSpeedCheck = now

			transferSpeedMbps := float64(transferredInWindow*8) / 1e6 / elapsedTime.Seconds()

			return rew.rc.rateFunc(transferSpeedMbps, rew.rc.transfers.Load(), rew.rc.globalTransfers.Load())
		})

		if checkErr != nil {
			rew.writeError = xerrors.Errorf("write rate over past %s is too slow: %w", rew.windowDuration, checkErr)
			return 0, rew.writeError
		}

		// Set write deadline
		if w, ok := rew.w.(interface{ SetWriteDeadline(time.Time) error }); ok {
			_ = w.SetWriteDeadline(now.Add(rew.windowDuration * 2))
		}
	} else if rew.lastSpeedCheck.IsZero() {
		// Set last speed check time and transferred bytes snapshot
		rew.lastSpeedCheck = now
		rew.bytesTransferredSnap = rew.rc.transferred.Load()

		// Set write deadline
		if w, ok := rew.w.(interface{ SetWriteDeadline(time.Time) error }); ok {
			_ = w.SetWriteDeadline(now.Add(rew.windowDuration * 2))
		}
	}

	n, err := rew.w.Write(p)
	rew.rc.transferred.Add(int64(n))
	return n, err
}

func (rew *RateEnforcingWriter) WriteError() error {
	return rew.writeError
}

func (rew *RateEnforcingWriter) Done() {
	if rew.writeError == nil {
		rew.rc.Release()
	}
}
