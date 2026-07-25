package rbstor

import (
	"context"
	_ "embed"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"github.com/CIDgravity/filecoin-gateway/database/cqldb"
	"github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/multiformats/go-multihash"
	"github.com/yugabyte/gocql"
)

const maxBatchSize = 2000 //todo choose max batch size

type CqlIndex struct {
	db cqldb.Database

	estimatedEntries atomic.Int64
}

func NewCqlIndex(db cqldb.Database) (iface.GroupIndex, error) {
	index := &CqlIndex{
		db: db,
	}

	// The entry count only feeds diagnostics (TopIndexStats). A full
	// COUNT(*) over a large index exceeds the connection timeout (a
	// migrated index easily holds tens of millions of rows), so it runs in
	// the background and must never block or fail node startup.
	go index.initEntryCountEstimate(context.Background())

	return index, nil
}

// countHashRanges splits the entry count into partition_hash ranges so each
// query touches a fraction of the table and completes within the connection
// timeout regardless of index size.
const countHashRanges = 64

func (ci *CqlIndex) initEntryCountEstimate(ctx context.Context) {
	start := time.Now()

	const hashSpace = 1 << 16 // YCQL partition_hash range: 0..65535
	step := hashSpace / countHashRanges

	for lo := 0; lo < hashSpace; lo += step {
		n, err := ci.countHashRange(ctx, lo, lo+step-1)
		if err != nil {
			log.Warnw("index size estimate initialization failed; index stats will under-report",
				"error", err, "countedSoFar", ci.estimatedEntries.Load())
			return
		}
		ci.estimatedEntries.Add(n)
	}

	log.Infow("index size estimate initialized", "entries", ci.estimatedEntries.Load(), "took", time.Since(start))
}

func (ci *CqlIndex) countHashRange(ctx context.Context, lo, hi int) (int64, error) {
	statement := `SELECT COUNT(*) FROM MultihashToGroup WHERE partition_hash(Multihash) >= ? AND partition_hash(Multihash) <= ?`

	var err error
	for attempt := 0; attempt < 5; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(time.Duration(attempt) * 5 * time.Second):
			}
		}

		var n int64
		iter := ci.db.Query(statement, lo, hi).WithContext(ctx).Iter()
		iter.Scan(&n)
		if err = iter.Close(); err == nil {
			return n, nil
		}
	}

	return 0, fmt.Errorf("counting index hash range [%d, %d]: %w", lo, hi, err)
}

func (ci *CqlIndex) executeBatchWithRetry(ctx context.Context, batch *gocql.Batch) error {
	var err error
	maxRetries := 20
	backoff := 20 * time.Second
	maxBackoff := 180 * time.Second

	for attempt := 0; attempt <= maxRetries; attempt++ {
		start := time.Now()
		err = ci.db.ExecuteBatch(batch)
		if time.Since(start) > 30*time.Second {
			log.Warnw("Batch Insert", "took", time.Since(start), "entries", len(batch.Entries))
		} else {
			log.Debugw("Batch Insert", "took", time.Since(start), "entries", len(batch.Entries))
		}

		if err == nil {
			return nil
		}

		// If context is done, exit immediately
		if ctx.Err() != nil {
			return ctx.Err()
		}

		log.Warnw("Batch insert attempt failed", "attempt", attempt+1, "error", err)

		// If max retries reached, return error
		if attempt == maxRetries {
			return fmt.Errorf("execute batch insert: %w", err)
		}

		// Sleep for backoff duration before retrying
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}

		// Exponential backoff
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}

	return nil
}

func (ci *CqlIndex) GetGroups(ctx context.Context, mh []multihash.Multihash, cb func(cidx int, gk iface.GroupKey) (more bool, err error)) error {
	statement := `SELECT Group FROM MultihashToGroup WHERE Multihash = ?`

	for idx, mh := range mh {
		iter := ci.db.Query(statement, mh).WithContext(ctx).Iter()
		var group iface.GroupKey

		for iter.Scan(&group) {
			more, err := cb(idx, group)
			if err != nil {
				return err
			}
			if !more {
				break
			}
		}

		if err := iter.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (ci *CqlIndex) GetSizes(ctx context.Context, mh []multihash.Multihash, cb func([]int32) error) error {
	statement := `SELECT Size FROM MultihashToGroup WHERE Multihash = ?`

	sizes := make([]int32, len(mh))

	for idx, mh := range mh {
		iter := ci.db.Query(statement, mh).WithContext(ctx).Iter()
		var size int32
		if ok := iter.Scan(&size); !ok {
			return iter.Close()
		}

		if err := iter.Close(); err != nil {
			return err
		}

		sizes[idx] = size
	}

	return cb(sizes)
}

func (ci *CqlIndex) AddGroup(ctx context.Context, mh []multihash.Multihash, sizes []int32, group iface.GroupKey) error {
	ichunk := 0

	for chunk := range slices.Chunk(mh, maxBatchSize) {
		if err := ci.executeAddGroupBatch(ctx, chunk, sizes[ichunk*maxBatchSize:ichunk*maxBatchSize+len(chunk)], group); err != nil {
			return err
		}
		ci.estimatedEntries.Add(int64(len(chunk)))
		ichunk++
	}
	return nil
}

func (ci *CqlIndex) executeAddGroupBatch(ctx context.Context, mh []multihash.Multihash, sizes []int32, group iface.GroupKey) error {
	statement := `INSERT INTO MultihashToGroup (Multihash, Group, Size) VALUES (?, ?, ?)`

	batch := ci.db.NewBatch(gocql.UnloggedBatch).WithContext(ctx)

	for idx, mh := range mh {
		batch.Entries = append(batch.Entries, gocql.BatchEntry{
			Stmt:       statement,
			Args:       []interface{}{mh, group, sizes[idx]},
			Idempotent: true,
		})
	}

	return ci.executeBatchWithRetry(ctx, batch)
}

func (ci *CqlIndex) Sync(_ context.Context) error {
	return nil
}

func (ci *CqlIndex) DropGroup(ctx context.Context, mh []multihash.Multihash, group iface.GroupKey) error {
	for chunk := range slices.Chunk(mh, maxBatchSize) {
		if err := ci.executeDropGroupBatch(ctx, chunk, group); err != nil {
			return err
		}
		ci.estimatedEntries.Add(-int64(len(chunk)))
	}
	return nil
}

func (ci *CqlIndex) executeDropGroupBatch(ctx context.Context, mh []multihash.Multihash, group iface.GroupKey) error {
	statement := `DELETE FROM MultihashToGroup WHERE Multihash = ? and Group = ?`
	batch := ci.db.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	for _, mh := range mh {
		batch.Entries = append(batch.Entries, gocql.BatchEntry{
			Stmt:       statement,
			Args:       []interface{}{mh, group},
			Idempotent: true,
		})
	}

	return ci.executeBatchWithRetry(ctx, batch)
}

func (ci *CqlIndex) EstimateSize(ctx context.Context) (int64, error) {
	_ = ctx
	return ci.estimatedEntries.Load(), nil
}

func (ci *CqlIndex) Close() error {
	return nil
}
