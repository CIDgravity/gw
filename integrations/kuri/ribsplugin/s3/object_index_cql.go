package s3

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/CIDgravity/filecoin-gateway/database/cqldb"
	iface2 "github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/ipfs/go-cid"
	"github.com/yugabyte/gocql"
)

type ObjectIndexCql struct {
	db      cqldb.Database
	batcher *cqldb.CQLBatcher
}

type Scannable interface {
	Scan(dest ...interface{}) error
}

func NewObjectIndexCql(db cqldb.Database) *ObjectIndexCql {
	return &ObjectIndexCql{
		db:      db,
		batcher: cqldb.NewCQLBatcher(db.Session()),
	}
}

func (or *ObjectIndexCql) List(ctx context.Context, bucket iface2.BucketName, prefix string, startAfter string, limit int32) (*iface2.ObjectList, error) {
	return or.listFiltered(ctx, bucket, prefix, startAfter, limit, false)
}

func (or *ObjectIndexCql) ListTemporary(ctx context.Context, bucket iface2.BucketName, prefix string, startAfter string, limit int32) (*iface2.ObjectList, error) {
	return or.listFiltered(ctx, bucket, prefix, startAfter, limit, true)
}

func (or *ObjectIndexCql) listFiltered(ctx context.Context, bucket iface2.BucketName, prefix string, startAfter string, limit int32, temporaryOnly bool) (*iface2.ObjectList, error) {
	if limit <= 0 {
		limit = 1000
	}

	var res []iface2.S3Object
	rawStartAfter := startAfter
	batchLimit := limit + 1
	if batchLimit < 32 {
		batchLimit = 32
	}

	for len(res) <= int(limit) {
		page, err := or.listPage(ctx, bucket, prefix, rawStartAfter, batchLimit)
		if err != nil {
			return nil, err
		}

		for _, obj := range page.Objects {
			isTemporary := obj.ExpiresAt != nil
			if temporaryOnly != isTemporary {
				continue
			}
			res = append(res, obj)
			if len(res) > int(limit) {
				break
			}
		}

		if len(res) > int(limit) || !page.IsTruncated || len(page.Objects) == 0 {
			break
		}

		rawStartAfter = page.Objects[len(page.Objects)-1].Key.String()
	}

	truncated := len(res) > int(limit)
	if truncated {
		res = res[:limit]
	}

	return &iface2.ObjectList{
		IsTruncated: truncated,
		Objects:     res,
	}, nil
}

func (or *ObjectIndexCql) listPage(ctx context.Context, bucket iface2.BucketName, prefix string, startAfter string, limit int32) (*iface2.ObjectList, error) {
	var res []iface2.S3Object

	statement := "select key, cid, size, updated, node_id, expires_at from S3Objects where bucket = ?"
	args := []interface{}{bucket.String()}

	if startAfter != "" && startAfter > prefix {
		statement += " and key > ?"
		args = append(args, startAfter)
	}

	if prefix != "" && prefix >= startAfter {
		statement += " and key >= ?"
		args = append(args, prefix)
	}

	end, ok := prefixEnd(prefix)
	if ok {
		statement += " and key < ?"
		args = append(args, end)
	}
	statement += " order by key asc limit ?"
	args = append(args, limit+1) // limit+1 to check if the response is truncated

	scanner := or.db.Query(statement, args...).
		WithContext(ctx).Iter().Scanner()

	for scanner.Next() {

		obj, err := scanS3Object(bucket, scanner)

		if err != nil {
			return nil, fmt.Errorf("listing s3 objects: %w", err)
		}
		res = append(res, obj)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("list objects scanner: %w", err)
	}

	truncated := len(res) > int(limit)
	if truncated {
		res = res[:limit]
	}

	return &iface2.ObjectList{
		IsTruncated: truncated,
		Objects:     res,
	}, nil
}

func (or *ObjectIndexCql) ListDir(ctx context.Context, bucket iface2.BucketName, prefix, startAfter string, limit int32, delimiter string) (*iface2.ObjectList, error) {
	originalPrefix := prefix
	originalStartAfter := startAfter
	end, endPrefixFound := prefixEnd(prefix)

	buildStatement := func(prefix, startAfter string) (string, []interface{}) {
		s := "select key, cid, size, updated, node_id, expires_at from S3Objects where bucket = ?"
		args := []interface{}{bucket.String()}
		if startAfter != "" && startAfter > prefix {
			s += " and key > ?"
			args = append(args, startAfter)
		}
		if prefix != "" && prefix >= startAfter {
			s += " and key >= ?"
			args = append(args, prefix)
		}

		if endPrefixFound {
			s += " and key < ?"
			args = append(args, end)
		}

		s += " order by key asc limit 1 "
		return s, args
	}

	var commonPrefixes []string
	var objs []iface2.S3Object
	truncated := false

	for len(commonPrefixes)+len(objs) < int(limit+1) { // +1 to check if the response is truncated
		statement, args := buildStatement(prefix, startAfter)
		query := or.db.Query(statement, args...).
			WithContext(ctx)
		obj, err := scanS3Object(bucket, query)

		if errors.Is(err, gocql.ErrNotFound) {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("listing s3 dir: %w", err)
		}

		startAfter = obj.Key.String()
		if obj.ExpiresAt != nil {
			continue
		}

		if int(limit) == len(commonPrefixes)+len(objs) {
			truncated = true
			break
		}

		trimmed := strings.TrimPrefix(obj.Key.String(), originalPrefix)
		if strings.Contains(trimmed, delimiter) {
			before, _, ok := strings.Cut(trimmed, delimiter)
			if !ok {
				return nil, fmt.Errorf("unable to process delimiter")
			}
			newCommonPrefix := originalPrefix + before + delimiter
			if newCommonPrefix > originalStartAfter {
				commonPrefixes = append(commonPrefixes, newCommonPrefix)
			}
			prefix, ok = prefixEnd(originalPrefix + before)
			if !ok {
				return nil, fmt.Errorf("unable to process common prefix")
			}
			continue
		} else {
			objs = append(objs, obj)
		}
	}

	return &iface2.ObjectList{
		IsTruncated:    truncated,
		Objects:        objs,
		CommonPrefixes: commonPrefixes,
	}, nil
}

func (or *ObjectIndexCql) Get(ctx context.Context, bucket iface2.BucketName, key iface2.S3Key) (iface2.S3Object, error) {
	query := or.db.Query("select key, cid, size, updated, node_id, expires_at from S3Objects where bucket = ? and key = ?", bucket.String(), key.String()).
		WithContext(ctx)

	obj, err := scanS3Object(bucket, query)

	if errors.Is(err, gocql.ErrNotFound) {
		return iface2.S3Object{}, iface2.ErrNotFound
	}
	if err != nil {
		return iface2.S3Object{}, fmt.Errorf("querying s3 object: %w", err)
	}

	return obj, nil
}

func (or *ObjectIndexCql) Put(ctx context.Context, obj iface2.S3Object) error {
	var expiresAt interface{}
	if obj.ExpiresAt != nil {
		expiresAt = *obj.ExpiresAt
	} else {
		expiresAt = nil
	}

	// Use batcher for high-throughput writes
	// The batcher collects writes and executes them in batches, blocking until committed
	err := or.batcher.Submit(ctx,
		"insert into S3Objects (bucket, key, cid, size, updated, node_id, expires_at) values (?, ?, ?, ?, ?, ?, ?)",
		obj.Bucket.String(),
		obj.Key.String(),
		obj.Cid.String(),
		obj.Size,
		obj.Updated,
		obj.NodeID,
		expiresAt,
	)
	if err != nil {
		return fmt.Errorf("inserting s3 object: %w", err)
	}
	return nil
}

func (or *ObjectIndexCql) Delete(ctx context.Context, bucket iface2.BucketName, key iface2.S3Key) error {
	err := or.db.Query("delete from S3Objects where bucket = ? and key = ?", bucket.String(), key.String()).WithContext(ctx).Exec()
	if err != nil {
		return fmt.Errorf("deleting s3 object: %w", err)
	}
	return nil
}

func prefixEnd(prefix string) (string, bool) {
	if prefix == "" {
		return "", false
	}

	rs := []rune(prefix)

	const maxRune = rune(0x10FFFF)
	for i := len(rs) - 1; i >= 0; i-- {
		if rs[i] < maxRune {
			rs[i]++
			return string(rs[:i+1]), true
		}
	}
	return "", false
}

func scanS3Object(bucket iface2.BucketName, scanner Scannable) (iface2.S3Object, error) {
	var cidString string
	var size uint64
	var updated time.Time
	var key string
	var nodeID string
	var expiresAt *time.Time

	err := scanner.Scan(&key, &cidString, &size, &updated, &nodeID, &expiresAt)
	if err != nil {
		return iface2.S3Object{}, err
	}

	c, err := cid.Decode(cidString)
	if err != nil {
		return iface2.S3Object{}, fmt.Errorf("decoding s3 object cid: %w", err)
	}

	obj := iface2.NewS3Object(bucket, iface2.S3Key(key), c, size, updated)
	obj.NodeID = nodeID
	obj.ExpiresAt = expiresAt
	return obj, nil
}

func (or *ObjectIndexCql) DeleteExpired(ctx context.Context) (int, error) {
	now := time.Now()
	deleted := 0

	// Scan for objects with non-null expires_at that are in the past.
	// ALLOW FILTERING is required because expires_at is not in the primary key.
	// This is acceptable as a low-frequency background GC operation.
	scanner := or.db.Query(
		"SELECT bucket, key, expires_at FROM S3Objects WHERE expires_at < ? ALLOW FILTERING", now,
	).WithContext(ctx).Iter().Scanner()

	type expiredEntry struct {
		bucket string
		key    string
	}
	var toDelete []expiredEntry

	for scanner.Next() {
		var bucket, key string
		var expiresAt time.Time
		if err := scanner.Scan(&bucket, &key, &expiresAt); err != nil {
			continue
		}
		toDelete = append(toDelete, expiredEntry{bucket: bucket, key: key})
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("scanning expired objects: %w", err)
	}

	for _, e := range toDelete {
		err := or.db.Query("DELETE FROM S3Objects WHERE bucket = ? AND key = ?",
			e.bucket, e.key).WithContext(ctx).Exec()
		if err != nil {
			log.Warnf("failed to delete expired object %s/%s: %v", e.bucket, e.key, err)
			continue
		}
		deleted++
	}

	return deleted, nil
}
