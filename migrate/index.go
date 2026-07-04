package migrate

import (
	"context"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CIDgravity/filecoin-gateway/database/cqldb"
	"github.com/cockroachdb/pebble"
	"github.com/yugabyte/gocql"
	"golang.org/x/xerrors"
)

const cqlInsertStmt = `INSERT INTO MultihashToGroup (Multihash, Group, Size) VALUES (?, ?, ?)`

type indexEntry struct {
	mh    []byte
	group int64
	size  int32
}

// cqlLoader writes index entries to CQL from a pool of workers. Inserts are
// idempotent upserts, so batches may be freely re-done on retry or resume.
//
// This intentionally mirrors rbstor.CqlIndex.AddGroup (unlogged batches with
// retry/backoff) but accepts mixed-group entries, which is what a
// multihash-ordered scan of the old index produces.
type cqlLoader struct {
	db cqldb.Database

	batchSize int
	batches   chan []indexEntry
	wg        sync.WaitGroup

	dispatched atomic.Int64 // batches sent to workers
	completed  atomic.Int64 // batches fully inserted
	inserted   atomic.Int64 // entries fully inserted

	errLk    sync.Mutex
	firstErr error

	pending []indexEntry
}

func newCqlLoader(ctx context.Context, db cqldb.Database, workers, batchSize int) *cqlLoader {
	l := &cqlLoader{
		db:        db,
		batchSize: batchSize,
		batches:   make(chan []indexEntry, workers*2),
	}

	for i := 0; i < workers; i++ {
		l.wg.Add(1)
		go func() {
			defer l.wg.Done()
			for batch := range l.batches {
				if err := l.insertBatch(ctx, batch); err != nil {
					l.errLk.Lock()
					if l.firstErr == nil {
						l.firstErr = err
					}
					l.errLk.Unlock()
				} else {
					l.inserted.Add(int64(len(batch)))
				}
				l.completed.Add(1)
			}
		}()
	}

	return l
}

func (l *cqlLoader) insertBatch(ctx context.Context, entries []indexEntry) error {
	var err error
	backoff := 500 * time.Millisecond
	const maxBackoff = 30 * time.Second
	const maxRetries = 12

	for attempt := 0; ; attempt++ {
		batch := l.db.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
		for _, e := range entries {
			batch.Entries = append(batch.Entries, gocql.BatchEntry{
				Stmt:       cqlInsertStmt,
				Args:       []interface{}{e.mh, e.group, e.size},
				Idempotent: true,
			})
		}

		err = l.db.ExecuteBatch(batch)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt == maxRetries {
			return xerrors.Errorf("inserting index batch after %d attempts: %w", attempt+1, err)
		}

		log.Warnw("index batch insert failed, retrying", "attempt", attempt+1, "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func (l *cqlLoader) err() error {
	l.errLk.Lock()
	defer l.errLk.Unlock()
	return l.firstErr
}

// add queues one entry; the multihash is copied.
func (l *cqlLoader) add(mh []byte, group int64, size int32) {
	l.pending = append(l.pending, indexEntry{
		mh:    append([]byte{}, mh...),
		group: group,
		size:  size,
	})
	if len(l.pending) >= l.batchSize {
		l.flush()
	}
}

func (l *cqlLoader) flush() {
	if len(l.pending) == 0 {
		return
	}
	l.dispatched.Add(1)
	l.batches <- l.pending
	l.pending = nil
}

// drain flushes and waits until every dispatched batch completed, then
// returns the first worker error, if any. After a clean drain the CQL
// database durably contains everything emitted so far, which is what makes
// checkpoints valid.
func (l *cqlLoader) drain(ctx context.Context) error {
	l.flush()
	for l.completed.Load() != l.dispatched.Load() {
		if err := ctx.Err(); err != nil {
			return err
		}
		time.Sleep(5 * time.Millisecond)
	}
	return l.err()
}

func (l *cqlLoader) close(ctx context.Context) error {
	err := l.drain(ctx)
	close(l.batches)
	l.wg.Wait()
	if err != nil {
		return err
	}
	return l.err()
}

// migrateIndex streams the old pebble index into the CQL MultihashToGroup
// table, checkpointing progress into state as it goes.
func migrateIndex(ctx context.Context, pdb *pebble.DB, cql cqldb.Database, state *State, destDir string, workers, batchSize int, checkpointEvery int64) (scanStats, error) {
	loader := newCqlLoader(ctx, cql, workers, batchSize)

	var startAt []byte
	if state.IndexCheckpoint != "" {
		var err error
		startAt, err = hex.DecodeString(state.IndexCheckpoint)
		if err != nil {
			return scanStats{}, xerrors.Errorf("parsing index checkpoint: %w", err)
		}
		log.Infow("resuming index migration", "checkpoint", state.IndexCheckpoint, "entriesSoFar", state.IndexEntries)
	}

	var sinceCheckpoint int64
	lastLog := time.Now()

	st, err := scanTopIndex(pdb, startAt, func(mh []byte, size int32, groups []int64) error {
		if err := loader.err(); err != nil {
			return err
		}

		for _, g := range groups {
			loader.add(mh, g, size)
		}

		sinceCheckpoint += int64(len(groups))
		if sinceCheckpoint >= checkpointEvery {
			if err := loader.drain(ctx); err != nil {
				return err
			}
			state.IndexCheckpoint = hex.EncodeToString(mh)
			state.IndexEntries += sinceCheckpoint
			sinceCheckpoint = 0
			if err := state.save(destDir); err != nil {
				return err
			}
		}

		if time.Since(lastLog) > 30*time.Second {
			log.Infow("index migration progress", "entries", state.IndexEntries+sinceCheckpoint, "inserted", loader.inserted.Load())
			lastLog = time.Now()
		}

		return ctx.Err()
	})
	if err != nil {
		_ = loader.close(ctx)
		return st, err
	}

	if err := loader.close(ctx); err != nil {
		return st, err
	}

	state.IndexEntries += sinceCheckpoint
	return st, nil
}
