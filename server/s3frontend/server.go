package s3frontend

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	"github.com/CIDgravity/filecoin-gateway/server/s3"
)

// FrontendServer is a stateless S3 proxy that routes requests to Kuri backend nodes
type FrontendServer struct {
	auth             *s3.Authenticator
	backendPool      *BackendPool
	router           *ObjectRouter
	multipartTracker *MultipartTracker
	nodeID           string
	writeCounter     atomic.Uint64
}

// NewFrontendServer creates a new S3 frontend proxy server
func NewFrontendServer(auth *s3.Authenticator, backendPool *BackendPool, router *ObjectRouter, multipartTracker *MultipartTracker, nodeID string) *FrontendServer {
	return &FrontendServer{
		auth:             auth,
		backendPool:      backendPool,
		router:           router,
		multipartTracker: multipartTracker,
		nodeID:           nodeID,
	}
}

// ServeHTTP implements the http.Handler interface
func (s *FrontendServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Health check endpoint
	if r.URL.Path == "/healthz" {
		s.handleHealthz(w, r)
		return
	}

	// Log request
	log.Debugw("S3 request",
		"method", r.Method,
		"path", r.URL.Path,
		"remote", r.RemoteAddr,
	)

	// Route based on method and query params
	switch r.Method {
	case "GET":
		s.handleGet(w, r)
	case "PUT":
		s.handlePut(w, r)
	case "POST":
		s.handlePost(w, r)
	case "DELETE":
		s.handleDelete(w, r)
	case "HEAD":
		s.handleHead(w, r)
	default:
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	}
}

// handleHealthz returns health status of the proxy
func (s *FrontendServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	// Check if we have any healthy backends
	if s.backendPool.SelectAny() == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("unhealthy: no backends available"))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func (s *FrontendServer) handleGet(w http.ResponseWriter, r *http.Request) {
	// Check for query params to determine operation
	params := r.URL.Query()

	if params.Has("location") {
		// Get bucket location - proxy to any backend
		s.proxyToAnyBackend(w, r)
		return
	}

	if params.Has("list-type") {
		// List objects - needs coordination, proxy to any for now
		s.proxyToAnyBackend(w, r)
		return
	}

	if params.Has("uploadId") {
		// ListParts - route to coordinator node that owns the upload
		uploadID := params.Get("uploadId")
		s.routeToCoordinator(w, r, uploadID)
		return
	}

	// Regular GET object - route to correct backend using YCQL lookup
	bucket, key, err := parseBucketAndKey(r.URL.Path)
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	// Lookup which node has this object
	nodeID, err := s.router.LookupObjectNode(r.Context(), bucket, key)
	if err != nil {
		if err.Error() == "object not found" {
			http.Error(w, "Not Found", http.StatusNotFound)
		} else {
			log.Errorw("Failed to lookup object", "error", err, "bucket", bucket, "key", key)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		}
		return
	}

	// Get the backend for this node
	backend := s.backendPool.Get(nodeID)
	if backend == nil {
		log.Warnw("Backend not found for node", "node_id", nodeID, "bucket", bucket, "key", key)
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}

	if !backend.IsHealthy() {
		log.Warnw("Backend unhealthy", "node_id", nodeID, "bucket", bucket, "key", key)
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}

	// Proxy to the specific backend
	s.proxyRequest(backend, w, r)
}

func (s *FrontendServer) handlePut(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()

	// Check if this is a multipart part upload
	if params.Has("partNumber") && params.Has("uploadId") {
		// Multipart part upload - round robin to distribute load
		s.proxyRoundRobin(w, r)
		return
	}

	// Regular PUT object - round robin for write distribution
	s.proxyRoundRobin(w, r)
}

func (s *FrontendServer) handlePost(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()

	if params.Has("uploads") {
		// Initiate multipart upload - round robin to select coordinator
		// The backend will create the upload record with its node_id
		s.proxyRoundRobin(w, r)
		return
	}

	if params.Has("uploadId") {
		// Complete multipart upload - route to coordinator
		uploadID := params.Get("uploadId")
		s.routeToCoordinator(w, r, uploadID)
		return
	}

	// Unknown POST operation
	http.Error(w, "Bad Request", http.StatusBadRequest)
}

// routeToCoordinator routes a request to the coordinator node for a multipart upload
func (s *FrontendServer) routeToCoordinator(w http.ResponseWriter, r *http.Request, uploadID string) {
	// Lookup the coordinator node for this upload
	upload, err := s.multipartTracker.GetUpload(r.Context(), uploadID)
	if err != nil {
		if err.Error() == "upload not found" {
			http.Error(w, "Not Found", http.StatusNotFound)
		} else {
			log.Errorw("Failed to lookup upload", "error", err, "upload_id", uploadID)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		}
		return
	}

	// Get the backend for the coordinator node
	backend := s.backendPool.Get(upload.NodeID)
	if backend == nil {
		log.Warnw("Coordinator backend not found", "node_id", upload.NodeID, "upload_id", uploadID)
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}

	if !backend.IsHealthy() {
		log.Warnw("Coordinator backend unhealthy", "node_id", upload.NodeID, "upload_id", uploadID)
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}

	// Proxy to the coordinator backend
	s.proxyRequest(backend, w, r)
}

func (s *FrontendServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()

	if params.Has("uploadId") {
		// Abort multipart upload - route to coordinator
		uploadID := params.Get("uploadId")
		s.routeToCoordinator(w, r, uploadID)
		return
	}

	// Regular DELETE - route to correct backend using YCQL lookup
	s.routeToObjectNode(w, r)
}

func (s *FrontendServer) handleHead(w http.ResponseWriter, r *http.Request) {
	// HEAD object - route to correct backend using YCQL lookup
	s.routeToObjectNode(w, r)
}

// routeToObjectNode looks up the object in YCQL and routes to the correct node
func (s *FrontendServer) routeToObjectNode(w http.ResponseWriter, r *http.Request) {
	bucket, key, err := parseBucketAndKey(r.URL.Path)
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	// Lookup which node has this object
	nodeID, err := s.router.LookupObjectNode(r.Context(), bucket, key)
	if err != nil {
		if err.Error() == "object not found" {
			http.Error(w, "Not Found", http.StatusNotFound)
		} else {
			log.Errorw("Failed to lookup object", "error", err, "bucket", bucket, "key", key)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		}
		return
	}

	// Get the backend for this node
	backend := s.backendPool.Get(nodeID)
	if backend == nil {
		log.Warnw("Backend not found for node", "node_id", nodeID, "bucket", bucket, "key", key)
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}

	if !backend.IsHealthy() {
		log.Warnw("Backend unhealthy", "node_id", nodeID, "bucket", bucket, "key", key)
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}

	// Proxy to the specific backend
	s.proxyRequest(backend, w, r)
}

// proxyRoundRobin selects a backend using round-robin and proxies the request
func (s *FrontendServer) proxyRoundRobin(w http.ResponseWriter, r *http.Request) {
	backend := s.backendPool.SelectRoundRobin()
	if backend == nil {
		http.Error(w, "Service Unavailable - No healthy backends", http.StatusServiceUnavailable)
		return
	}

	log.Infow("Round-robin selected backend", "backend", backend.ID(), "method", r.Method, "path", r.URL.Path)
	s.proxyRequest(backend, w, r)
}

// proxyToAnyBackend proxies to any available backend (for operations that don't care which node)
func (s *FrontendServer) proxyToAnyBackend(w http.ResponseWriter, r *http.Request) {
	backend := s.backendPool.SelectAny()
	if backend == nil {
		http.Error(w, "Service Unavailable - No healthy backends", http.StatusServiceUnavailable)
		return
	}

	s.proxyRequest(backend, w, r)
}

// proxyTransport is a shared http.Transport with connection pooling for backend requests.
var proxyTransport = &http.Transport{
	MaxIdleConns:        100,
	MaxIdleConnsPerHost: 10,
	IdleConnTimeout:     90 * time.Second,
	DialContext: (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext,
}

// proxyClient is a shared http.Client using the pooled transport.
var proxyClient = &http.Client{
	Transport: proxyTransport,
}

// proxyRequest proxies an HTTP request to a backend, streaming the body directly
// without buffering the entire request in memory.
func (s *FrontendServer) proxyRequest(backend *Backend, w http.ResponseWriter, r *http.Request) {
	// Create new URL for backend
	backendURL := backend.URL() + r.URL.Path
	if r.URL.RawQuery != "" {
		backendURL += "?" + r.URL.RawQuery
	}

	// Stream the request body directly to the backend without buffering
	req, err := http.NewRequestWithContext(r.Context(), r.Method, backendURL, r.Body)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Preserve content length for the backend
	req.ContentLength = r.ContentLength

	// Copy headers
	for name, values := range r.Header {
		for _, value := range values {
			req.Header.Add(name, value)
		}
	}

	// Add source proxy header
	req.Header.Set("X-Source-Proxy", s.nodeID)

	// Ensure x-amz-content-sha256 is set for PUT/POST requests
	// If not set by client, use UNSIGNED-PAYLOAD to allow pass-through
	if (r.Method == "PUT" || r.Method == "POST") && req.Header.Get("X-Amz-Content-Sha256") == "" {
		req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	}

	// Execute request using shared client with connection pooling
	resp, err := proxyClient.Do(req)
	if err != nil {
		log.Errorw("Failed to proxy request", "error", err, "backend", backend.ID())
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// Copy response headers
	for name, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(name, value)
		}
	}

	// Copy status code
	w.WriteHeader(resp.StatusCode)

	// Stream response body to client
	_, _ = io.Copy(w, resp.Body)
}

// parseBucketAndKey extracts bucket and key from the URL path
func parseBucketAndKey(path string) (bucket, key string, err error) {
	parts := strings.Split(strings.TrimLeft(path, "/"), "/")
	if len(parts) < 1 {
		return "", "", fmt.Errorf("malformed path: %s", path)
	}

	bucket = parts[0]
	if len(parts) > 1 {
		key = strings.Join(parts[1:], "/")
	}

	return bucket, key, nil
}

// Start starts the frontend server
func Start(cfg *configuration.S3APIConfig, auth *s3.Authenticator, backendPool *BackendPool, router *ObjectRouter, multipartTracker *MultipartTracker, nodeID string) (*http.Server, error) {
	server := NewFrontendServer(auth, backendPool, router, multipartTracker, nodeID)

	httpServer := &http.Server{
		Addr:    cfg.BindAddr,
		Handler: server,
	}

	log.Infow("Starting S3 frontend proxy", "addr", cfg.BindAddr, "node_id", nodeID)

	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalw("Frontend server error", "error", err)
		}
	}()

	return httpServer, nil
}
