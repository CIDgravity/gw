package cqldb

import (
	"context"
	"sync"
	"time"

	"github.com/yugabyte/gocql"
)

// Batcher configuration defaults
const (
	DefaultBatchSize      = 15000
	DefaultIdleTimeout    = 10 * time.Millisecond
	DefaultMaxLatency     = 30 * time.Millisecond
	DefaultWorkers        = 8
	DefaultMaxRetries     = 5
	DefaultInitialBackoff = 100 * time.Millisecond
	DefaultMaxBackoff     = 10 * time.Second
)

// BatcherConfig holds configuration for CQLBatcher
type BatcherConfig struct {
	BatchSize   int
	IdleTimeout time.Duration
	MaxLatency  time.Duration
	Workers     int
}

// DefaultBatcherConfig returns default configuration
func DefaultBatcherConfig() BatcherConfig {
	return BatcherConfig{
		BatchSize:   DefaultBatchSize,
		IdleTimeout: DefaultIdleTimeout,
		MaxLatency:  DefaultMaxLatency,
		Workers:     DefaultWorkers,
	}
}

// batchRequest wraps an entry with its result channel
type batchRequest struct {
	stmt       string
	args       []interface{}
	resultChan chan error
}

// pendingBatch holds accumulated entries waiting to be flushed
type pendingBatch struct {
	entries    []gocql.BatchEntry
	waiters    []chan error
	firstEntry time.Time
}

// CQLBatcher collects CQL writes and executes them in batches for high throughput.
// It flushes batches when:
// - Batch reaches configured size (default 15k)
// - No new entries for idle timeout (default 10ms)
// - First entry in batch exceeds max latency (default 30ms)
type CQLBatcher struct {
	session *gocql.Session
	config  BatcherConfig

	inputChan  chan batchRequest
	workerChan chan pendingBatch

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewCQLBatcher creates a new batcher with default configuration
func NewCQLBatcher(session *gocql.Session) *CQLBatcher {
	return NewCQLBatcherWithConfig(session, DefaultBatcherConfig())
}

// NewCQLBatcherWithConfig creates a new batcher with custom configuration
func NewCQLBatcherWithConfig(session *gocql.Session, config BatcherConfig) *CQLBatcher {
	ctx, cancel := context.WithCancel(context.Background())
	b := &CQLBatcher{
		session:    session,
		config:     config,
		inputChan:  make(chan batchRequest, config.BatchSize*2),
		workerChan: make(chan pendingBatch, config.Workers*2),
		ctx:        ctx,
		cancel:     cancel,
	}

	// Start worker pool
	for i := 0; i < config.Workers; i++ {
		b.wg.Add(1)
		go b.worker()
	}

	// Start collector goroutine
	b.wg.Add(1)
	go b.collector()

	return b
}

// Submit adds a CQL statement to be batched and blocks until the batch is committed.
// Returns error if the batch execution fails or context is cancelled.
func (b *CQLBatcher) Submit(ctx context.Context, stmt string, args ...interface{}) error {
	resultChan := make(chan error, 1)

	select {
	case b.inputChan <- batchRequest{
		stmt:       stmt,
		args:       args,
		resultChan: resultChan,
	}:
	case <-ctx.Done():
		return ctx.Err()
	case <-b.ctx.Done():
		return b.ctx.Err()
	}

	select {
	case err := <-resultChan:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// collector receives requests, accumulates them into batches, and dispatches to workers
func (b *CQLBatcher) collector() {
	defer b.wg.Done()
	defer close(b.workerChan)

	var current pendingBatch
	idleTimer := time.NewTimer(b.config.IdleTimeout)
	idleTimer.Stop()

	resetCurrent := func() {
		current = pendingBatch{
			entries: make([]gocql.BatchEntry, 0, b.config.BatchSize),
			waiters: make([]chan error, 0, b.config.BatchSize),
		}
	}
	resetCurrent()

	flushBatch := func() {
		if len(current.entries) == 0 {
			return
		}

		// Send to worker pool
		select {
		case b.workerChan <- current:
		case <-b.ctx.Done():
			// On shutdown, notify waiters of cancellation
			for _, w := range current.waiters {
				w <- b.ctx.Err()
				close(w)
			}
		}

		resetCurrent()
		idleTimer.Stop()
	}

	for {
		select {
		case req, ok := <-b.inputChan:
			if !ok {
				flushBatch()
				return
			}

			if len(current.entries) == 0 {
				current.firstEntry = time.Now()
				idleTimer.Reset(b.config.IdleTimeout)
			}

			current.entries = append(current.entries, gocql.BatchEntry{
				Stmt:       req.stmt,
				Args:       req.args,
				Idempotent: true,
			})
			current.waiters = append(current.waiters, req.resultChan)

			// Check batch size - flush if full
			if len(current.entries) >= b.config.BatchSize {
				flushBatch()
				continue
			}

			// Check max latency - flush if oldest entry is too old
			if time.Since(current.firstEntry) >= b.config.MaxLatency {
				flushBatch()
			}

		case <-idleTimer.C:
			flushBatch()

		case <-b.ctx.Done():
			flushBatch()
			return
		}
	}
}

// worker executes batches received from the collector
func (b *CQLBatcher) worker() {
	defer b.wg.Done()

	for batch := range b.workerChan {
		err := b.executeBatchWithRetry(batch.entries)

		// Notify all waiters
		for _, w := range batch.waiters {
			w <- err
			close(w)
		}
	}
}

// executeBatchWithRetry executes a batch with exponential backoff retry
func (b *CQLBatcher) executeBatchWithRetry(entries []gocql.BatchEntry) error {
	batch := b.session.NewBatch(gocql.UnloggedBatch)
	batch.Entries = entries

	backoff := DefaultInitialBackoff

	for attempt := 0; attempt <= DefaultMaxRetries; attempt++ {
		start := time.Now()
		err := b.session.ExecuteBatch(batch)
		elapsed := time.Since(start)

		if err == nil {
			if elapsed > 5*time.Second {
				log.Warnw("Batch insert slow", "took", elapsed, "entries", len(entries))
			}
			return nil
		}

		// Check if context is done
		if b.ctx.Err() != nil {
			return b.ctx.Err()
		}

		log.Warnw("Batch insert failed, retrying",
			"attempt", attempt+1,
			"maxRetries", DefaultMaxRetries,
			"entries", len(entries),
			"error", err)

		if attempt == DefaultMaxRetries {
			return err
		}

		// Sleep with backoff
		timer := time.NewTimer(backoff)
		select {
		case <-b.ctx.Done():
			timer.Stop()
			return b.ctx.Err()
		case <-timer.C:
		}

		// Exponential backoff
		backoff *= 2
		if backoff > DefaultMaxBackoff {
			backoff = DefaultMaxBackoff
		}
	}

	return nil
}

// Close gracefully shuts down the batcher, flushing any pending batches
func (b *CQLBatcher) Close() {
	b.cancel()
	close(b.inputChan)
	b.wg.Wait()
}
