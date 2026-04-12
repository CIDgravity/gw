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

	entries, err := index.queryEntryCount(context.Background())
	if err != nil {
		return nil, fmt.Errorf("initialize cql index size estimate: %w", err)
	}
	index.estimatedEntries.Store(entries)

	return index, nil
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

func (ci *CqlIndex) queryEntryCount(ctx context.Context) (int64, error) {
	query := `SELECT COUNT(*) FROM MultihashToGroup`

	iter := ci.db.Query(query).WithContext(ctx).Iter()

	var entries int64
	iter.Scan(&entries)
	err := iter.Close()
	return entries, err
}

func (ci *CqlIndex) Close() error {
	return nil
}
