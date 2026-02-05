package cqldb

import "github.com/yugabyte/gocql"

type Database interface {
	Query(stmt string, values ...interface{}) *gocql.Query
	NewBatch(typ gocql.BatchType) *gocql.Batch
	ExecuteBatch(batch *gocql.Batch) error
	Session() *gocql.Session
}
