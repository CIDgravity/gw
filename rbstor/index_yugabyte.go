package rbstor

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/lotus-web3/ribs"
	"github.com/multiformats/go-multihash"
	"github.com/yugabyte/gocql"
	"slices"
	"strings"
	"time"
)

//go:embed index_init.cql
var createIndexCQL string

const maxBatchSize = 2000 //todo choose max batch size

type YugabyteIndex struct {
	cluster *gocql.ClusterConfig
	session *gocql.Session
	ctx     context.Context
}

func NewYugabyteIndex(hosts []string, port int, keyspace string) (*YugabyteIndex, error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Port = port
	cluster.Consistency = gocql.Quorum

	index := &YugabyteIndex{
		cluster: cluster,
		ctx:     context.Background(),
	}

	return index, index.Start(keyspace)
}

func (yi *YugabyteIndex) Start(keyspace string) error {
	log.Info("Starting Yugabyte Index")
	session, err := yi.cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("create yugabyte session: %w", err)
	}
	statement := `CREATE KEYSPACE IF NOT EXISTS ` + keyspace
	log.Debugf("Executing CQL statement: %s", statement)
	if err := session.Query(statement, keyspace).WithContext(yi.ctx).Exec(); err != nil {
		return fmt.Errorf("create keyspace: %w", err)
	}

	session.Close()

	yi.cluster.Keyspace = keyspace
	session, err = yi.cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("create yugabyte session: %w", err)
	}

	queries := strings.Split(createIndexCQL, ";")
	for _, statement := range queries {
		statement = strings.TrimSpace(statement)
		if statement == "" {
			continue
		}
		log.Debugf("Executing CQL statement: %s", statement)
		if err := session.Query(statement).WithContext(yi.ctx).Exec(); err != nil {
			return fmt.Errorf("yugabyte table initialization:\n%s\n%w", statement, err)
		}
	}
	yi.session = session
	log.Info("Yugabyte Index started")
	return nil
}

func (yi *YugabyteIndex) executeBatchWithRetry(ctx context.Context, batch *gocql.Batch) error {
	var err error
	maxRetries := 20
	backoff := 20 * time.Second
	maxBackoff := 180 * time.Second

	for attempt := 0; attempt <= maxRetries; attempt++ {
		start := time.Now()
		err = yi.session.ExecuteBatch(batch)
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

func (yi *YugabyteIndex) GetGroups(ctx context.Context, mh []multihash.Multihash, cb func(cidx int, gk ribs.GroupKey) (more bool, err error)) error {
	statement := `SELECT Group FROM MultihashToGroup WHERE Multihash = ?`

	for idx, mh := range mh {
		iter := yi.session.Query(statement, mh).WithContext(ctx).Iter()
		var group ribs.GroupKey

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

func (yi *YugabyteIndex) GetSizes(ctx context.Context, mh []multihash.Multihash, cb func([]int32) error) error {
	statement := `SELECT Size FROM MultihashToGroup WHERE Multihash = ?`

	sizes := make([]int32, len(mh))

	for idx, mh := range mh {
		iter := yi.session.Query(statement, mh).WithContext(ctx).Iter()
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

func (yi *YugabyteIndex) AddGroup(ctx context.Context, mh []multihash.Multihash, sizes []int32, group ribs.GroupKey) error {
	ichunk := 0

	for chunk := range slices.Chunk(mh, maxBatchSize) {
		if err := yi.executeAddGroupBatch(ctx, chunk, sizes[ichunk*maxBatchSize:ichunk*maxBatchSize+len(chunk)], group); err != nil {
			return err
		}
		ichunk++
	}
	return nil
}

func (yi *YugabyteIndex) executeAddGroupBatch(ctx context.Context, mh []multihash.Multihash, sizes []int32, group ribs.GroupKey) error {
	statement := `INSERT INTO MultihashToGroup (Multihash, Group, Size) VALUES (?, ?, ?)`

	batch := yi.session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)

	for idx, mh := range mh {
		batch.Entries = append(batch.Entries, gocql.BatchEntry{
			Stmt:       statement,
			Args:       []interface{}{mh, group, sizes[idx]},
			Idempotent: true,
		})
	}

	return yi.executeBatchWithRetry(ctx, batch)
}

func (yi *YugabyteIndex) Sync(_ context.Context) error {
	return nil
}

func (yi *YugabyteIndex) DropGroup(ctx context.Context, mh []multihash.Multihash, group ribs.GroupKey) error {
	for chunk := range slices.Chunk(mh, maxBatchSize) {
		if err := yi.executeDropGroupBatch(ctx, chunk, group); err != nil {
			return err
		}
	}
	return nil
}

func (yi *YugabyteIndex) executeDropGroupBatch(ctx context.Context, mh []multihash.Multihash, group ribs.GroupKey) error {
	statement := `DELETE FROM MultihashToGroup WHERE Multihash = ? and Group = ?`
	batch := yi.session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	for _, mh := range mh {
		batch.Entries = append(batch.Entries, gocql.BatchEntry{
			Stmt:       statement,
			Args:       []interface{}{mh, group},
			Idempotent: true,
		})
	}

	return yi.executeBatchWithRetry(ctx, batch)
}

func (yi *YugabyteIndex) EstimateSize(ctx context.Context) (int64, error) {
	query := `SELECT COUNT(*) FROM MultihashToGroup`

	iter := yi.session.Query(query).WithContext(ctx).Iter()

	var entries int64
	iter.Scan(&entries)
	err := iter.Close()
	return entries, err
}

func (yi *YugabyteIndex) Close() error {
	yi.session.Close()
	return nil
}
