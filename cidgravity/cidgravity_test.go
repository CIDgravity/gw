package cidgravity

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	"github.com/filecoin-project/go-state-types/abi"
)

// setupTestConfig configures the global config for testing with the given server URLs
func setupTestConfig(providersURL, dealsURL string) {
	cfg := configuration.GetConfig()
	cfg.CidGravity.ApiToken = "test-api-token"
	cfg.CidGravity.MaxConns = 4
	cfg.CidGravity.ApiEndpointGetProviders = providersURL
	cfg.CidGravity.ApiEndpointGetDeals = dealsURL
	cfg.CidGravity.AltTokens = map[string]string{}
}

// =============================================================================
// GetBestAvailableProviders Tests
// =============================================================================

func TestGetBestAvailableProviders_Success(t *testing.T) {
	// Create mock server that returns providers
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request
		if r.Method != "POST" {
			t.Errorf("Expected POST method, got %s", r.Method)
		}
		if r.Header.Get("X-API-KEY") != "test-api-token" {
			t.Errorf("Expected X-API-KEY header to be 'test-api-token', got '%s'", r.Header.Get("X-API-KEY"))
		}

		// Decode request body to verify params
		var req CIDgravityGetBestAvailableProvidersRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("Failed to decode request body: %v", err)
		}
		if req.PieceCid != "baga6ea4seaqtest" {
			t.Errorf("Expected PieceCid 'baga6ea4seaqtest', got '%s'", req.PieceCid)
		}

		// Return successful response
		resp := CIDgravityAPIResponse{
			Error: CIDgravityAPIError{},
			Result: &CIDgravityAPIResult{
				Providers: []string{"f01234", "f05678", "f09999"},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	setupTestConfig(server.URL, "")

	cidg := &CIDGravity{}
	providers, err := cidg.GetBestAvailableProviders(CIDgravityGetBestAvailableProvidersRequest{
		PieceCid:             "baga6ea4seaqtest",
		Provider:             "f01234",
		StartEpoch:           1000,
		Duration:             518400,
		StoragePricePerEpoch: "0",
		ProviderCollateral:   "0",
		TransferSize:         1000000,
		TransferType:         "http",
	})

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(providers) != 3 {
		t.Errorf("Expected 3 providers, got %d", len(providers))
	}
	if providers[0] != "f01234" || providers[1] != "f05678" || providers[2] != "f09999" {
		t.Errorf("Unexpected providers: %v", providers)
	}
}

func TestGetBestAvailableProviders_NoProviders(t *testing.T) {
	// Create mock server that returns empty provider list with reason
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reason := "NO_PROVIDERS_AVAILABLE"
		resp := CIDgravityAPIResponse{
			Error: CIDgravityAPIError{},
			Result: &CIDgravityAPIResult{
				Reason:    &reason,
				Providers: []string{},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	setupTestConfig(server.URL, "")

	cidg := &CIDGravity{}
	providers, err := cidg.GetBestAvailableProviders(CIDgravityGetBestAvailableProvidersRequest{
		PieceCid: "baga6ea4seaqtest",
	})

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(providers) != 0 {
		t.Errorf("Expected 0 providers, got %d", len(providers))
	}
}

func TestGetBestAvailableProviders_APIError(t *testing.T) {
	// Create mock server that returns an API error with non-200 status
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := CIDgravityAPIResponse{
			Error: CIDgravityAPIError{
				Code:    "INVALID_TOKEN",
				Message: "The provided API token is invalid",
			},
			Result: nil,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	setupTestConfig(server.URL, "")

	cidg := &CIDGravity{}
	result, err := cidg.GetBestAvailableProviders(CIDgravityGetBestAvailableProvidersRequest{
		PieceCid: "baga6ea4seaqtest",
	})

	if err == nil {
		t.Fatal("Expected error, got nil")
	}
	if err.Error() != "status code is not 200" {
		t.Errorf("Expected 'status code is not 200' error, got '%v'", err)
	}
	// When status code is not 200, the function returns error code and message
	if len(result) != 2 {
		t.Errorf("Expected result to contain [code, message], got %v", result)
	}
	if result[0] != "INVALID_TOKEN" {
		t.Errorf("Expected error code 'INVALID_TOKEN', got '%s'", result[0])
	}
}

func TestGetBestAvailableProviders_HTTPError(t *testing.T) {
	// Create mock server that returns 500 error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := CIDgravityAPIResponse{
			Error: CIDgravityAPIError{
				Code:    "INTERNAL_ERROR",
				Message: "Internal server error",
			},
			Result: nil,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	setupTestConfig(server.URL, "")

	cidg := &CIDGravity{}
	result, err := cidg.GetBestAvailableProviders(CIDgravityGetBestAvailableProvidersRequest{
		PieceCid: "baga6ea4seaqtest",
	})

	if err == nil {
		t.Fatal("Expected error, got nil")
	}
	if err.Error() != "status code is not 200" {
		t.Errorf("Expected 'status code is not 200' error, got '%v'", err)
	}
	if len(result) != 2 || result[0] != "INTERNAL_ERROR" {
		t.Errorf("Expected error info in result, got %v", result)
	}
}

func TestGetBestAvailableProviders_Timeout(t *testing.T) {
	// Create mock server that delays response beyond timeout
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Sleep longer than the client timeout
		// Note: The actual client uses 30s timeout, but we can't wait that long in tests
		// Instead, we'll close the connection immediately to simulate timeout-like behavior
		time.Sleep(100 * time.Millisecond)
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Fatal("ResponseWriter does not support hijacking")
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			t.Fatalf("Failed to hijack connection: %v", err)
		}
		conn.Close()
	}))
	defer server.Close()

	setupTestConfig(server.URL, "")

	cidg := &CIDGravity{}
	_, err := cidg.GetBestAvailableProviders(CIDgravityGetBestAvailableProvidersRequest{
		PieceCid: "baga6ea4seaqtest",
	})

	if err == nil {
		t.Fatal("Expected error due to connection close, got nil")
	}
}

func TestGetBestAvailableProviders_InvalidJSON(t *testing.T) {
	// Create mock server that returns invalid JSON
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("not valid json {{{"))
	}))
	defer server.Close()

	setupTestConfig(server.URL, "")

	cidg := &CIDGravity{}
	_, err := cidg.GetBestAvailableProviders(CIDgravityGetBestAvailableProvidersRequest{
		PieceCid: "baga6ea4seaqtest",
	})

	if err == nil {
		t.Fatal("Expected JSON parse error, got nil")
	}
}

// =============================================================================
// GetDealStates Tests
// =============================================================================

func TestGetDealStates_Success(t *testing.T) {
	// Create mock server that returns deal states
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request
		if r.Method != "POST" {
			t.Errorf("Expected POST method, got %s", r.Method)
		}
		if r.Header.Get("X-API-KEY") != "test-api-token" {
			t.Errorf("Expected X-API-KEY header to be 'test-api-token', got '%s'", r.Header.Get("X-API-KEY"))
		}

		// Return successful response with deals
		resp := CIDgravityDealStatesAPIResponse{
			Error: CIDgravityAPIError{},
			Next:  nil,
			Result: map[abi.DealID]CIDgravityDealStatus{
				12345: {
					Proposal: CIDgravityDealProposalStatus{
						PieceCid:             Cid{Root: "baga6ea4seaqtest1"},
						PieceSize:            34359738368,
						VerifiedDeal:         true,
						Client:               "f01234",
						Provider:             "f05678",
						Label:                "test-label",
						StartEpoch:           1000,
						EndEpoch:             1518400,
						StoragePricePerEpoch: "0",
						ProviderCollateral:   "0",
						ClientCollateral:     "0",
					},
					State: CIDgravityDealProposalState{
						Status:            "active",
						PublishedEpoch:    900,
						OnChainStartEpoch: 1000,
						OnChainEndEpoch:   1518400,
					},
					LastUpdate: 1234567890.0,
				},
				67890: {
					Proposal: CIDgravityDealProposalStatus{
						PieceCid:  Cid{Root: "baga6ea4seaqtest2"},
						PieceSize: 34359738368,
					},
					State: CIDgravityDealProposalState{
						Status: "pending",
					},
					LastUpdate: 1234567891.0,
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	setupTestConfig("", server.URL)

	cidg := &CIDGravity{}
	states, err := cidg.GetDealStates(context.Background())

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(states) != 2 {
		t.Errorf("Expected 2 deal states, got %d", len(states))
	}

	deal1, ok := states[12345]
	if !ok {
		t.Error("Expected deal 12345 to exist")
	} else {
		if deal1.Proposal.PieceCid.Root != "baga6ea4seaqtest1" {
			t.Errorf("Expected PieceCid 'baga6ea4seaqtest1', got '%s'", deal1.Proposal.PieceCid.Root)
		}
		if deal1.State.Status != "active" {
			t.Errorf("Expected status 'active', got '%s'", deal1.State.Status)
		}
	}
}

func TestGetDealStates_Pagination(t *testing.T) {
	callCount := 0
	// Create mock server that returns paginated results
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++

		// Decode request to check pagination cursor
		var req CIDgravityDealStatusRequest
		json.NewDecoder(r.Body).Decode(&req)

		var resp CIDgravityDealStatesAPIResponse
		if req.Next == nil {
			// First page
			nextCursor := "page2"
			resp = CIDgravityDealStatesAPIResponse{
				Error: CIDgravityAPIError{},
				Next:  &nextCursor,
				Result: map[abi.DealID]CIDgravityDealStatus{
					11111: {
						Proposal: CIDgravityDealProposalStatus{
							PieceCid: Cid{Root: "baga6ea4seaqpage1"},
						},
					},
				},
			}
		} else if *req.Next == "page2" {
			// Second page (last)
			resp = CIDgravityDealStatesAPIResponse{
				Error: CIDgravityAPIError{},
				Next:  nil,
				Result: map[abi.DealID]CIDgravityDealStatus{
					22222: {
						Proposal: CIDgravityDealProposalStatus{
							PieceCid: Cid{Root: "baga6ea4seaqpage2"},
						},
					},
				},
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	setupTestConfig("", server.URL)

	cidg := &CIDGravity{}
	states, err := cidg.GetDealStates(context.Background())

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if callCount != 2 {
		t.Errorf("Expected 2 API calls for pagination, got %d", callCount)
	}
	if len(states) != 2 {
		t.Errorf("Expected 2 deal states (from both pages), got %d", len(states))
	}

	// Verify deals from both pages are present
	if _, ok := states[11111]; !ok {
		t.Error("Expected deal 11111 from page 1")
	}
	if _, ok := states[22222]; !ok {
		t.Error("Expected deal 22222 from page 2")
	}
}

func TestGetDealStates_EmptyResult(t *testing.T) {
	// Create mock server that returns empty result
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := CIDgravityDealStatesAPIResponse{
			Error:  CIDgravityAPIError{},
			Next:   nil,
			Result: map[abi.DealID]CIDgravityDealStatus{},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	setupTestConfig("", server.URL)

	cidg := &CIDGravity{}
	states, err := cidg.GetDealStates(context.Background())

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(states) != 0 {
		t.Errorf("Expected 0 deal states, got %d", len(states))
	}
}

func TestGetDealStates_APIError(t *testing.T) {
	// Create mock server that returns error status
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := CIDgravityDealStatesAPIResponse{
			Error: CIDgravityAPIError{
				Code:    "RATE_LIMITED",
				Message: "Too many requests",
			},
			Next:   nil,
			Result: nil,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	setupTestConfig("", server.URL)

	cidg := &CIDGravity{}
	_, err := cidg.GetDealStates(context.Background())

	if err == nil {
		t.Fatal("Expected error, got nil")
	}
	if err.Error() != "status code is not 200" {
		t.Errorf("Expected 'status code is not 200' error, got '%v'", err)
	}
}

// =============================================================================
// Client Tests
// =============================================================================

func TestCIDGravity_Init(t *testing.T) {
	setupTestConfig("", "")

	cidg := &CIDGravity{}

	// Before init, semaphore should be nil
	if cidg.sem != nil {
		t.Error("Expected semaphore to be nil before init")
	}

	// Call init
	err := cidg.init()
	if err != nil {
		t.Fatalf("Expected no error from init, got %v", err)
	}

	// After init, semaphore should be created
	if cidg.sem == nil {
		t.Error("Expected semaphore to be created after init")
	}

	// Calling init again should not change the semaphore
	originalSem := cidg.sem
	err = cidg.init()
	if err != nil {
		t.Fatalf("Expected no error from second init, got %v", err)
	}
	if cidg.sem != originalSem {
		t.Error("Expected semaphore to remain the same after second init")
	}
}

func TestCIDGravity_RateLimit(t *testing.T) {
	// Set MaxConns to 2 for testing
	cfg := configuration.GetConfig()
	cfg.CidGravity.MaxConns = 2

	var activeRequests int32
	var maxConcurrent int32

	// Create mock server that tracks concurrent requests
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := atomic.AddInt32(&activeRequests, 1)

		// Track maximum concurrent requests
		for {
			old := atomic.LoadInt32(&maxConcurrent)
			if current <= old {
				break
			}
			if atomic.CompareAndSwapInt32(&maxConcurrent, old, current) {
				break
			}
		}

		// Simulate some work
		time.Sleep(50 * time.Millisecond)

		atomic.AddInt32(&activeRequests, -1)

		resp := CIDgravityAPIResponse{
			Error: CIDgravityAPIError{},
			Result: &CIDgravityAPIResult{
				Providers: []string{"f01234"},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	setupTestConfig(server.URL, "")
	cfg.CidGravity.MaxConns = 2

	cidg := &CIDGravity{}

	// Launch 5 concurrent requests
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = cidg.GetBestAvailableProviders(CIDgravityGetBestAvailableProvidersRequest{
				PieceCid: "baga6ea4seaqtest",
			})
		}()
	}

	wg.Wait()

	// Verify that maximum concurrent requests was limited by semaphore
	observed := atomic.LoadInt32(&maxConcurrent)
	if observed > 2 {
		t.Errorf("Expected maximum concurrent requests to be limited to 2, got %d", observed)
	}
}

// =============================================================================
// Table-driven tests for various response scenarios
// =============================================================================

func TestGetBestAvailableProviders_VariousResponses(t *testing.T) {
	tests := []struct {
		name              string
		serverResponse    CIDgravityAPIResponse
		statusCode        int
		expectError       bool
		expectedProviders int
	}{
		{
			name: "single provider",
			serverResponse: CIDgravityAPIResponse{
				Error:  CIDgravityAPIError{},
				Result: &CIDgravityAPIResult{Providers: []string{"f01234"}},
			},
			statusCode:        200,
			expectError:       false,
			expectedProviders: 1,
		},
		{
			name: "multiple providers",
			serverResponse: CIDgravityAPIResponse{
				Error:  CIDgravityAPIError{},
				Result: &CIDgravityAPIResult{Providers: []string{"f01234", "f05678", "f09012"}},
			},
			statusCode:        200,
			expectError:       false,
			expectedProviders: 3,
		},
		{
			name: "nil result",
			serverResponse: CIDgravityAPIResponse{
				Error:  CIDgravityAPIError{},
				Result: nil,
			},
			statusCode:        200,
			expectError:       true,
			expectedProviders: 0,
		},
		{
			name: "bad request error",
			serverResponse: CIDgravityAPIResponse{
				Error:  CIDgravityAPIError{Code: "BAD_REQUEST", Message: "Invalid request"},
				Result: nil,
			},
			statusCode:        400,
			expectError:       true,
			expectedProviders: 2, // Error code and message
		},
		{
			name: "forbidden error",
			serverResponse: CIDgravityAPIResponse{
				Error:  CIDgravityAPIError{Code: "FORBIDDEN", Message: "Access denied"},
				Result: nil,
			},
			statusCode:        403,
			expectError:       true,
			expectedProviders: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.statusCode)
				json.NewEncoder(w).Encode(tc.serverResponse)
			}))
			defer server.Close()

			setupTestConfig(server.URL, "")

			cidg := &CIDGravity{}
			providers, err := cidg.GetBestAvailableProviders(CIDgravityGetBestAvailableProvidersRequest{
				PieceCid: "baga6ea4seaqtest",
			})

			if tc.expectError && err == nil {
				t.Error("Expected error, got nil")
			}
			if !tc.expectError && err != nil {
				t.Errorf("Expected no error, got %v", err)
			}
			if len(providers) != tc.expectedProviders {
				t.Errorf("Expected %d providers, got %d", tc.expectedProviders, len(providers))
			}
		})
	}
}

func TestGetDealStates_AltTokens(t *testing.T) {
	callCount := 0
	receivedTokens := make([]string, 0)
	var mu sync.Mutex

	// Create mock server that tracks which tokens it receives
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		callCount++
		token := r.Header.Get("X-API-KEY")
		receivedTokens = append(receivedTokens, token)
		mu.Unlock()

		resp := CIDgravityDealStatesAPIResponse{
			Error:  CIDgravityAPIError{},
			Next:   nil,
			Result: map[abi.DealID]CIDgravityDealStatus{},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	setupTestConfig("", server.URL)

	// Configure alt tokens
	cfg := configuration.GetConfig()
	cfg.CidGravity.AltTokens = map[string]string{
		"client1": "alt-token-1",
		"client2": "alt-token-2",
	}

	cidg := &CIDGravity{}
	_, err := cidg.GetDealStates(context.Background())

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Should have called the API once for main token and once for each alt token
	expectedCalls := 1 + len(cfg.CidGravity.AltTokens)
	if callCount != expectedCalls {
		t.Errorf("Expected %d API calls, got %d", expectedCalls, callCount)
	}

	// Verify all tokens were used
	hasMainToken := false
	hasAlt1 := false
	hasAlt2 := false
	for _, token := range receivedTokens {
		if token == "test-api-token" {
			hasMainToken = true
		}
		if token == "alt-token-1" {
			hasAlt1 = true
		}
		if token == "alt-token-2" {
			hasAlt2 = true
		}
	}
	if !hasMainToken {
		t.Error("Expected main token to be used")
	}
	if !hasAlt1 {
		t.Error("Expected alt-token-1 to be used")
	}
	if !hasAlt2 {
		t.Error("Expected alt-token-2 to be used")
	}
}

func TestGetDealStates_ContextCancellation(t *testing.T) {
	// Create a context that's already cancelled
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("Server should not be called when context is cancelled")
	}))
	defer server.Close()

	setupTestConfig("", server.URL)

	cidg := &CIDGravity{}
	_, err := cidg.GetDealStates(ctx)

	if err == nil {
		t.Fatal("Expected error due to cancelled context, got nil")
	}
}
