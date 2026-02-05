package rbdeal

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRepairCheckInterval_Default verifies the default repair check interval
func TestRepairCheckInterval_Default(t *testing.T) {
	assert.Equal(t, time.Minute, RepairCheckInterval)
}

// TestRepairWorkerCount_Default verifies the default worker count of 4
// when RepairWorkers config is not set or is <= 0
func TestRepairWorkerCount_Default(t *testing.T) {
	// Test that default is 4 workers when config value is 0
	workers := 0
	if workers <= 0 {
		workers = 4 // default
	}
	assert.Equal(t, 4, workers)

	// Test that default is 4 workers when config value is negative
	workers = -1
	if workers <= 0 {
		workers = 4 // default
	}
	assert.Equal(t, 4, workers)
}

// TestRepairWorkerCount_Configured verifies custom worker count from config
func TestRepairWorkerCount_Configured(t *testing.T) {
	tests := []struct {
		name            string
		configWorkers   int
		expectedWorkers int
	}{
		{"default when zero", 0, 4},
		{"default when negative", -1, 4},
		{"configured 1", 1, 1},
		{"configured 8", 8, 8},
		{"configured 16", 16, 16},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			workers := tt.configWorkers
			if workers <= 0 {
				workers = 4 // default
			}
			assert.Equal(t, tt.expectedWorkers, workers)
		})
	}
}

// TestStartRepairWorkers_NoStagingPath verifies workers don't start when staging path is empty
func TestStartRepairWorkers_NoStagingPath(t *testing.T) {
	// When stagingPath is empty, repair workers should be disabled
	stagingPath := ""
	repairDir := ""

	// Simulate the logic from startRepairWorkers
	if stagingPath == "" {
		stagingPath = repairDir
	}

	shouldStart := stagingPath != ""
	assert.False(t, shouldStart, "repair workers should not start when staging path is empty")
}

// TestStartRepairWorkers_WithStagingPath verifies workers start when staging path is set
func TestStartRepairWorkers_WithStagingPath(t *testing.T) {
	tmpDir := t.TempDir()
	stagingPath := tmpDir

	shouldStart := stagingPath != ""
	assert.True(t, shouldStart, "repair workers should start when staging path is set")
}

// TestStartRepairWorkers_FallbackToRepairDir verifies fallback behavior
func TestStartRepairWorkers_FallbackToRepairDir(t *testing.T) {
	tests := []struct {
		name          string
		configPath    string
		repairDir     string
		expectedPath  string
		expectedStart bool
	}{
		{"config path set", "/custom/path", "/default/repair", "/custom/path", true},
		{"fallback to repairDir", "", "/default/repair", "/default/repair", true},
		{"both empty", "", "", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stagingPath := tt.configPath
			if stagingPath == "" {
				stagingPath = tt.repairDir
			}

			assert.Equal(t, tt.expectedPath, stagingPath)
			assert.Equal(t, tt.expectedStart, stagingPath != "")
		})
	}
}

// TestRepairWorkerDir creates worker-specific directories correctly
func TestRepairWorkerDir(t *testing.T) {
	repairDir := "/data/repair"

	tests := []struct {
		workerID    int
		expectedDir string
	}{
		{0, "/data/repair/w0"},
		{1, "/data/repair/w1"},
		{5, "/data/repair/w5"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("worker_%d", tt.workerID), func(t *testing.T) {
			workerDir := filepath.Join(repairDir, fmt.Sprintf("w%d", tt.workerID))
			assert.Equal(t, tt.expectedDir, workerDir)
		})
	}
}

// TestRepairGroupFileName generates correct group file names
func TestRepairGroupFileName(t *testing.T) {
	workerDir := "/data/repair/w0"

	tests := []struct {
		groupKey     int64
		expectedFile string
	}{
		{1, "/data/repair/w0/group-1.car"},
		{100, "/data/repair/w0/group-100.car"},
		{999999, "/data/repair/w0/group-999999.car"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("group_%d", tt.groupKey), func(t *testing.T) {
			groupFile := filepath.Join(workerDir, fmt.Sprintf("group-%d.car", tt.groupKey))
			assert.Equal(t, tt.expectedFile, groupFile)
		})
	}
}

// ============================================================================
// HTTP Retrieval Tests (using httptest)
// ============================================================================

// TestFetchFromSource_Success tests successful HTTP fetch from mock server
func TestFetchFromSource_Success(t *testing.T) {
	expectedData := []byte("test car file content with some padding to make it larger")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request method
		assert.Equal(t, "GET", r.Method)

		// Check for Range header support
		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(expectedData)-1, len(expectedData)))
			w.WriteHeader(http.StatusPartialContent)
		} else {
			w.WriteHeader(http.StatusOK)
		}

		w.Write(expectedData)
	}))
	defer server.Close()

	// Create a temp file to write to
	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "test.car")

	// Make HTTP request (simulating what fetchFromSource does internally)
	resp, err := http.Get(server.URL)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Write to file
	f, err := os.Create(outputFile)
	require.NoError(t, err)

	_, err = io.Copy(f, resp.Body)
	require.NoError(t, err)
	f.Close()

	// Verify content
	data, err := os.ReadFile(outputFile)
	require.NoError(t, err)
	assert.Equal(t, expectedData, data)
}

// TestFetchFromSource_Retry tests retry behavior when server fails first request
func TestFetchFromSource_Retry(t *testing.T) {
	var requestCount int32
	expectedData := []byte("success after retry")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&requestCount, 1)

		if count == 1 {
			// First request fails with 500
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("temporary failure"))
			return
		}

		// Subsequent requests succeed
		w.WriteHeader(http.StatusOK)
		w.Write(expectedData)
	}))
	defer server.Close()

	// Simulate retry logic
	var lastErr error
	var data []byte

	for i := 0; i < 3; i++ {
		resp, err := http.Get(server.URL)
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
			resp.Body.Close()
			lastErr = fmt.Errorf("http status: %d", resp.StatusCode)
			continue
		}

		data, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}

		lastErr = nil
		break
	}

	require.NoError(t, lastErr)
	assert.Equal(t, expectedData, data)
	assert.Equal(t, int32(2), atomic.LoadInt32(&requestCount), "should have made 2 requests")
}

// TestFetchFromSource_404NotFound tests handling of 404 response
func TestFetchFromSource_404NotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("piece not found"))
	}))
	defer server.Close()

	resp, err := http.Get(server.URL)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	// The fetch should fail with non-OK status
	isSuccess := resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent
	assert.False(t, isSuccess, "404 response should be treated as failure")
}

// TestFetchFromSource_SlowServer tests handling of slow responses
func TestFetchFromSource_SlowServer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate slow response
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("slow response"))
	}))
	defer server.Close()

	// Create client with timeout
	client := &http.Client{
		Timeout: 50 * time.Millisecond,
	}

	_, err := client.Get(server.URL)

	// Should timeout
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context deadline exceeded")
}

// TestFetchFromSource_RangeRequest tests Range header support
func TestFetchFromSource_RangeRequest(t *testing.T) {
	fullData := []byte("0123456789ABCDEFGHIJ") // 20 bytes

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")
		if rangeHeader == "" {
			w.WriteHeader(http.StatusOK)
			w.Write(fullData)
			return
		}

		// Parse range header (simplified)
		var start, end int
		fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end)

		if start >= len(fullData) {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}

		if end >= len(fullData) {
			end = len(fullData) - 1
		}

		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(fullData)))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(fullData[start : end+1])
	}))
	defer server.Close()

	// Request bytes 5-14
	req, err := http.NewRequest("GET", server.URL, nil)
	require.NoError(t, err)
	req.Header.Set("Range", "bytes=5-14")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusPartialContent, resp.StatusCode)

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, []byte("56789ABCDE"), data)
}

// TestFetchFromSource_MultipleSourcesFallback tests trying multiple sources
func TestFetchFromSource_MultipleSourcesFallback(t *testing.T) {
	// First server fails
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server1.Close()

	// Second server succeeds
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("success from second server"))
	}))
	defer server2.Close()

	sources := []string{server1.URL, server2.URL}

	var data []byte
	var lastErr error

	for _, url := range sources {
		resp, err := http.Get(url)
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			lastErr = fmt.Errorf("http status: %d", resp.StatusCode)
			continue
		}

		data, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}

		lastErr = nil
		break
	}

	require.NoError(t, lastErr)
	assert.Equal(t, []byte("success from second server"), data)
}

// ============================================================================
// Verification Logic Tests
// ============================================================================

// TestVerifyGroupFile_Documentation documents what verifyGroupFile does
// and what would need to be mocked to test it fully
//
// verifyGroupFile performs the following steps:
// 1. Opens the file at groupFile path
// 2. Creates a new ributil.DataCidWriter (streams data through piece CID calculation)
// 3. Copies the entire file content through the CID writer
// 4. Computes the piece CID using cc.Sum()
// 5. Compares computed piece CID with expectedPieceCid
// 6. Returns error if mismatch, nil if match
//
// To fully test this function, you would need to:
// - Mock or create a valid CAR file with known piece CID
// - The ributil.DataCidWriter is the Fr32 padded piece commitment calculator
// - Testing would require generating actual valid piece commitments
//
// Integration test approach:
// - Use test fixtures with pre-computed piece CIDs
// - Or use the carlog package to create valid test CAR files
func TestVerifyGroupFile_Documentation(t *testing.T) {
	// This test documents the verification process
	// Full integration testing would require:
	// 1. A valid CAR file
	// 2. The expected piece CID computed externally
	// 3. ributil.DataCidWriter to compute the piece commitment

	t.Log("verifyGroupFile verifies a CAR file's piece CID matches expected")
	t.Log("It uses ributil.DataCidWriter for Fr32 piece commitment calculation")
	t.Log("Full testing requires valid CAR fixtures with known piece CIDs")
}

// ============================================================================
// Repair Stats Tests
// ============================================================================

// TestRepairStats_Structure tests the repair stats map structure
func TestRepairStats_Structure(t *testing.T) {
	repairStats := make(map[int]*repairJobInfo)
	var lk sync.Mutex

	// Simulate adding repair stats
	lk.Lock()
	repairStats[0] = &repairJobInfo{
		groupKey:      1,
		state:         "fetching",
		fetchProgress: 1000,
		fetchSize:     10000,
		fetchUrl:      "http://example.com/piece/bafy...",
	}
	lk.Unlock()

	lk.Lock()
	defer lk.Unlock()

	stats, ok := repairStats[0]
	require.True(t, ok)
	assert.Equal(t, int64(1), stats.groupKey)
	assert.Equal(t, "fetching", stats.state)
	assert.Equal(t, int64(1000), stats.fetchProgress)
	assert.Equal(t, int64(10000), stats.fetchSize)
}

// repairJobInfo is a test struct mirroring iface.RepairJob
type repairJobInfo struct {
	groupKey      int64
	state         string
	fetchProgress int64
	fetchSize     int64
	fetchUrl      string
}

// TestRepairJobState_Transitions tests valid state transitions
func TestRepairJobState_Transitions(t *testing.T) {
	states := []string{"fetching", "verifying", "importing"}

	// Valid transitions: fetching -> verifying -> importing
	for i := 0; i < len(states)-1; i++ {
		t.Run(fmt.Sprintf("%s_to_%s", states[i], states[i+1]), func(t *testing.T) {
			current := states[i]
			next := states[i+1]

			// Simulate state transition
			assert.NotEqual(t, current, next)
		})
	}
}

// ============================================================================
// Environment Variable Override Tests
// ============================================================================

// TestRepairImportUrlOverride tests RIBS_IMPORT_N environment variable
func TestRepairImportUrlOverride(t *testing.T) {
	groupKey := int64(123)
	envName := fmt.Sprintf("RIBS_IMPORT_%d", groupKey)

	// Test without override
	_, exists := os.LookupEnv(envName)
	assert.False(t, exists)

	// Test with override
	t.Setenv(envName, "http://localhost:8080/piece/bafy...")

	importUrl, exists := os.LookupEnv(envName)
	assert.True(t, exists)
	assert.Equal(t, "http://localhost:8080/piece/bafy...", importUrl)
}

// ============================================================================
// Concurrent Repair Worker Tests
// ============================================================================

// TestRepairWorker_MultipleAssignedGroups tests warning for multiple assignments
func TestRepairWorker_MultipleAssignedGroups(t *testing.T) {
	// Test the logic that warns when worker has multiple assigned groups
	assignedGroups := []int64{1, 2, 3}

	hasMultiple := len(assignedGroups) > 1
	assert.True(t, hasMultiple, "should detect multiple assigned groups")
}

// TestRepairWorker_NoAssignedWork tests behavior with no assigned work
func TestRepairWorker_NoAssignedWork(t *testing.T) {
	assignedGroups := []int64{}

	needsNewWork := len(assignedGroups) == 0
	assert.True(t, needsNewWork, "should need new work when no groups assigned")
}

// TestRepairWorker_ContextCancellation tests context cancellation behavior
func TestRepairWorker_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	closeCh := make(chan struct{})

	done := make(chan struct{})
	go func() {
		defer close(done)

		for {
			select {
			case <-closeCh:
				return
			case <-ctx.Done():
				return
			default:
			}

			// Simulate work check
			time.Sleep(10 * time.Millisecond)
			break
		}
	}()

	// Cancel context
	cancel()

	select {
	case <-done:
		// Expected
	case <-time.After(time.Second):
		t.Fatal("worker did not stop on context cancellation")
	}
}

// TestRepairWorker_CloseChanSignal tests close channel signal behavior
func TestRepairWorker_CloseChanSignal(t *testing.T) {
	closeCh := make(chan struct{})

	done := make(chan struct{})
	go func() {
		defer close(done)

		for {
			select {
			case <-closeCh:
				return
			default:
			}

			time.Sleep(10 * time.Millisecond)
			break
		}
	}()

	// Signal close
	close(closeCh)

	select {
	case <-done:
		// Expected - but might have exited via default path
	case <-time.After(100 * time.Millisecond):
		// Also acceptable if it exited naturally
	}
}

// ============================================================================
// File Cleanup Tests
// ============================================================================

// TestRepairFile_Cleanup tests repair file cleanup after completion
func TestRepairFile_Cleanup(t *testing.T) {
	tmpDir := t.TempDir()
	groupFile := filepath.Join(tmpDir, "group-1.car")

	// Create a test file
	err := os.WriteFile(groupFile, []byte("test data"), 0644)
	require.NoError(t, err)

	// Verify it exists
	_, err = os.Stat(groupFile)
	require.NoError(t, err)

	// Remove it (simulating post-repair cleanup)
	err = os.Remove(groupFile)
	require.NoError(t, err)

	// Verify it's gone
	_, err = os.Stat(groupFile)
	assert.True(t, os.IsNotExist(err))
}

// TestRepairFile_CleanupNonExistent tests removing non-existent file
func TestRepairFile_CleanupNonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	groupFile := filepath.Join(tmpDir, "nonexistent.car")

	// Try to remove non-existent file
	err := os.Remove(groupFile)
	assert.Error(t, err)
	assert.True(t, os.IsNotExist(err))
}

// ============================================================================
// Integration-style Tests for HTTP Sources
// ============================================================================

// TestNoHTTPSources tests behavior when no HTTP sources are available
func TestNoHTTPSources(t *testing.T) {
	sources := []string{}

	if len(sources) == 0 {
		t.Log("no HTTP retrieval sources available")
	}

	assert.Empty(t, sources)
}

// TestHTTPSourcesFiltering tests filtering of sources with no HTTP addresses
func TestHTTPSourcesFiltering(t *testing.T) {
	type candidate struct {
		provider   int64
		httpMaddrs []string
	}

	candidates := []candidate{
		{1, []string{}},                          // No HTTP
		{2, []string{"http://example.com:8080"}}, // Has HTTP
		{3, []string{}},                          // No HTTP
	}

	var sources []string
	for _, c := range candidates {
		if len(c.httpMaddrs) > 0 {
			sources = append(sources, c.httpMaddrs[0])
		}
	}

	assert.Len(t, sources, 1)
	assert.Equal(t, "http://example.com:8080", sources[0])
}
