package s3frontend

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	logging "github.com/ipfs/go-log"
)

var log = logging.Logger("gw/s3frontend/backend")

// Backend represents a Kuri storage node
type Backend struct {
	id      string
	baseURL string
	client  *http.Client
	healthy atomic.Bool
}

// ID returns the backend node ID
func (b *Backend) ID() string {
	return b.id
}

// URL returns the backend base URL
func (b *Backend) URL() string {
	return b.baseURL
}

// IsHealthy returns true if the backend is healthy
func (b *Backend) IsHealthy() bool {
	return b.healthy.Load()
}

// setHealth sets the health status of the backend
func (b *Backend) setHealth(healthy bool) {
	b.healthy.Store(healthy)
}

// BackendPool manages a pool of Kuri backend nodes
type BackendPool struct {
	backends []*Backend
	counter  atomic.Uint64
	mu       sync.RWMutex
}

// NewBackendPool creates a new backend pool from a list of node addresses
// Format: "node-id:http://host:port,node-id2:http://host2:port2"
func NewBackendPool(nodesConfig string) (*BackendPool, error) {
	if nodesConfig == "" {
		return nil, fmt.Errorf("no backend nodes configured")
	}

	pool := &BackendPool{
		backends: make([]*Backend, 0),
	}

	nodes := strings.Split(nodesConfig, ",")
	for _, node := range nodes {
		node = strings.TrimSpace(node)
		if node == "" {
			continue
		}

		parts := strings.SplitN(node, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid backend node format: %s (expected id:url)", node)
		}

		id := parts[0]
		url := parts[1]

		backend := &Backend{
			id:      id,
			baseURL: url,
			client: &http.Client{
				Timeout: 30 * time.Second,
			},
		}
		backend.setHealth(true) // Assume healthy initially

		pool.backends = append(pool.backends, backend)
		log.Infow("Added backend", "id", id, "url", url)
	}

	if len(pool.backends) == 0 {
		return nil, fmt.Errorf("no valid backend nodes configured")
	}

	return pool, nil
}

// SelectRoundRobin selects a backend using round-robin algorithm
func (p *BackendPool) SelectRoundRobin() *Backend {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.backends) == 0 {
		return nil
	}

	// Try to find a healthy backend
	attempts := 0
	maxAttempts := len(p.backends)

	for attempts < maxAttempts {
		idx := p.counter.Add(1) % uint64(len(p.backends))
		backend := p.backends[idx]

		if backend.IsHealthy() {
			return backend
		}

		attempts++
	}

	// If no healthy backends, return nil
	return nil
}

// SelectAny selects any available backend (for operations that don't need specific routing)
func (p *BackendPool) SelectAny() *Backend {
	return p.SelectRoundRobin()
}

// Get returns a specific backend by ID
func (p *BackendPool) Get(id string) *Backend {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, backend := range p.backends {
		if backend.id == id {
			return backend
		}
	}

	return nil
}

// StartHealthChecks starts background health checking of backends
func (p *BackendPool) StartHealthChecks(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.checkHealth()
			}
		}
	}()
}

// checkHealth performs health checks on all backends
func (p *BackendPool) checkHealth() {
	p.mu.RLock()
	backends := make([]*Backend, len(p.backends))
	copy(backends, p.backends)
	p.mu.RUnlock()

	for _, backend := range backends {
		go func(b *Backend) {
			healthy := p.performHealthCheck(b)
			if healthy != b.IsHealthy() {
				b.setHealth(healthy)
				if healthy {
					log.Infow("Backend became healthy", "id", b.id)
				} else {
					log.Warnw("Backend became unhealthy", "id", b.id)
				}
			}
		}(backend)
	}
}

// performHealthCheck checks if a backend is healthy
func (p *BackendPool) performHealthCheck(backend *Backend) bool {
	// Simple health check - try to GET a non-existent object
	// Should return 404 if healthy, error if not
	url := backend.baseURL + "/healthz"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return false
	}

	resp, err := backend.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	// Consider healthy if we get any response (even 404)
	return resp.StatusCode < 500
}

// AllBackends returns a copy of all backends
func (p *BackendPool) AllBackends() []*Backend {
	p.mu.RLock()
	defer p.mu.RUnlock()

	backends := make([]*Backend, len(p.backends))
	copy(backends, p.backends)
	return backends
}
