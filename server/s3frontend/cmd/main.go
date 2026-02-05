package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	"github.com/CIDgravity/filecoin-gateway/server/s3"
	"github.com/CIDgravity/filecoin-gateway/server/s3frontend"
)

func main() {
	os.Exit(mainRet())
}

func mainRet() int {
	// Load configuration
	if err := configuration.LoadConfig(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load configuration: %v\n", err)
		return 1
	}

	cfg := configuration.GetConfig()

	// Create S3 authenticator
	auth, err := s3.NewAuthenticator(&cfg.S3API)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create authenticator: %v\n", err)
		return 1
	}

	// Create backend pool from environment
	pool, err := s3frontend.NewBackendPoolFromConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create backend pool: %v\n", err)
		return 1
	}

	// Create YCQL database connection
	db, err := s3frontend.NewYCQLDatabase()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to YCQL: %v\n", err)
		return 1
	}

	// Create router and multipart tracker
	router := s3frontend.NewObjectRouterFromDB(db)
	multipartTracker := s3frontend.NewMultipartTrackerFromDB(db)

	// Get node ID from configuration
	nodeID := cfg.Frontend.NodeID
	if nodeID == "" {
		nodeID = "frontend-default"
	}

	// Start health checks
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.StartHealthChecks(ctx, 10*time.Second)

	// Start the frontend server
	server, err := s3frontend.Start(&cfg.S3API, auth, pool, router, multipartTracker, nodeID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start server: %v\n", err)
		return 1
	}

	fmt.Printf("S3 Frontend Proxy started on %s (node: %s)\n", cfg.S3API.BindAddr, nodeID)
	fmt.Printf("Backend nodes: %d configured\n", len(pool.AllBackends()))

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nShutting down...")

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		fmt.Fprintf(os.Stderr, "Server shutdown error: %v\n", err)
		return 1
	}

	fmt.Println("Server stopped")
	return 0
}
