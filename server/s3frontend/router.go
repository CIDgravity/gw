package s3frontend

import (
	"context"
	"fmt"
	"time"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	"github.com/CIDgravity/filecoin-gateway/database/cqldb"
	"github.com/yugabyte/gocql"
)

// ObjectRouter handles routing of S3 requests to the correct backend node
type ObjectRouter struct {
	db cqldb.Database
}

// NewObjectRouter creates a new object router with YCQL connection
func NewObjectRouter() (*ObjectRouter, error) {
	cfg := configuration.GetConfig().Frontend

	config := configuration.YugabyteCqlConfig{
		Hosts:           cfg.YCQLHosts,
		Port:            cfg.YCQLPort,
		Keyspace:        cfg.YCQLKeyspace,
		User:            cfg.YCQLUser,
		Pass:            cfg.YCQLPass,
		Timeout:         30,
		ConnectTimeout:  10,
		SocketKeepalive: 30,
	}

	// Use existing cqldb package for connection
	db, err := cqldb.NewYugabyteCqlDb(config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to YCQL: %w", err)
	}

	return &ObjectRouter{
		db: db,
	}, nil
}

// LookupObjectNode queries YCQL to find which node stores an object
func (r *ObjectRouter) LookupObjectNode(ctx context.Context, bucket, key string) (string, error) {
	var nodeID string

	// Query with QUORUM consistency for read-after-write guarantee
	query := r.db.Query(
		"SELECT node_id FROM S3Objects WHERE bucket = ? AND key = ?",
		bucket, key,
	).WithContext(ctx).Consistency(gocql.Quorum)

	err := query.Scan(&nodeID)
	if err == gocql.ErrNotFound {
		return "", fmt.Errorf("object not found")
	}
	if err != nil {
		return "", fmt.Errorf("failed to lookup object: %w", err)
	}

	return nodeID, nil
}

// WaitForObjectVisibility polls YCQL until an object becomes visible
// Used to ensure read-after-write consistency
func (r *ObjectRouter) WaitForObjectVisibility(ctx context.Context, bucket, key, expectedNodeID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		nodeID, err := r.LookupObjectNode(ctx, bucket, key)
		if err == nil && nodeID == expectedNodeID {
			return nil // Found!
		}

		// Wait a bit before retrying
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
			// Continue polling
		}
	}

	return fmt.Errorf("timeout waiting for object visibility")
}

// Close closes the YCQL connection
func (r *ObjectRouter) Close() error {
	// The Database interface doesn't have a Close method
	// The connection is managed by the underlying implementation
	return nil
}
