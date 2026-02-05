package s3frontend

import (
	"context"
	"net/http"
	"time"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	"github.com/CIDgravity/filecoin-gateway/database/cqldb"
	"github.com/CIDgravity/filecoin-gateway/server/s3"
	"go.uber.org/fx"
)

// Module provides the S3 frontend proxy dependencies
var Module = fx.Module(
	"s3frontend",
	fx.Provide(
		NewBackendPoolFromConfig,
		NewYCQLDatabase,
		NewObjectRouterFromDB,
		NewMultipartTrackerFromDB,
		MakeFrontendServer,
	),
)

// ServerIn contains the dependencies for creating the frontend server
type ServerIn struct {
	fx.In
	Auth *s3.Authenticator
	Cfg  *configuration.S3APIConfig
}

// NewYCQLDatabase creates a shared YCQL database connection
func NewYCQLDatabase() (cqldb.Database, error) {
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

	return cqldb.NewYugabyteCqlDb(config)
}

// NewObjectRouterFromDB creates an ObjectRouter from a shared DB connection
func NewObjectRouterFromDB(db cqldb.Database) *ObjectRouter {
	return &ObjectRouter{db: db}
}

// NewMultipartTrackerFromDB creates a MultipartTracker from a shared DB connection
func NewMultipartTrackerFromDB(db cqldb.Database) *MultipartTracker {
	return NewMultipartTracker(db)
}

// NewBackendPoolFromConfig creates a BackendPool from configuration
func NewBackendPoolFromConfig() (*BackendPool, error) {
	cfg := configuration.GetConfig().Frontend

	// Get backend nodes from configuration
	nodesConfig := cfg.BackendNodes
	if nodesConfig == "" {
		// For development, default to local Kuri
		nodesConfig = "kuri-1:http://localhost:8078"
	}

	pool, err := NewBackendPool(nodesConfig)
	if err != nil {
		return nil, err
	}

	// Start health checks
	ctx, cancel := context.WithCancel(context.Background())
	pool.StartHealthChecks(ctx, 10*time.Second)

	// TODO: Properly handle the cancel function with lifecycle hooks
	_ = cancel

	return pool, nil
}

// MakeFrontendServer creates the S3 frontend server
func MakeFrontendServer(in ServerIn, pool *BackendPool, router *ObjectRouter, multipartTracker *MultipartTracker) (*http.Server, error) {
	cfg := configuration.GetConfig().Frontend

	nodeID := cfg.NodeID
	if nodeID == "" {
		nodeID = "frontend-default"
	}

	return Start(in.Cfg, in.Auth, pool, router, multipartTracker, nodeID)
}
