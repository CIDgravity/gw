package cidgravity

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/CIDgravity/filecoin-gateway/configuration"
)

// CIDGravityStatus represents the connection status to CIDGravity service
type CIDGravityStatus struct {
	// Connected indicates if the API is reachable
	Connected bool `json:"connected"`
	// TokenValid indicates if the API token is valid (authenticated successfully)
	TokenValid bool `json:"tokenValid"`
	// Endpoint is the API endpoint being used
	Endpoint string `json:"endpoint"`
	// Error contains any error message if connection failed
	Error string `json:"error,omitempty"`
	// LastCheck is the unix timestamp of when this status was checked
	LastCheck int64 `json:"lastCheck"`
	// ResponseTimeMs is the API response time in milliseconds
	ResponseTimeMs int64 `json:"responseTimeMs"`
	// TokenConfigured indicates if a token is configured (non-empty)
	TokenConfigured bool `json:"tokenConfigured"`
}

// CheckStatus checks the connection status to CIDGravity API by making a
// lightweight request to the get-on-chain-deals endpoint
func (cidg *CIDGravity) CheckStatus(ctx context.Context) CIDGravityStatus {
	cidg.init()

	cfg := configuration.GetConfig()
	status := CIDGravityStatus{
		Endpoint:        cfg.CidGravity.ApiEndpointGetDeals,
		LastCheck:       time.Now().Unix(),
		TokenConfigured: cfg.CidGravity.ApiToken != "" && cfg.CidGravity.ApiToken != "CHANGE_ME" && cfg.CidGravity.ApiToken != "CHANGE_ME_WITH_VAULT",
	}

	// If no token is configured, return early
	if !status.TokenConfigured {
		status.Error = "API token not configured"
		return status
	}

	// Try to acquire semaphore with timeout
	semCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := cidg.sem.Acquire(semCtx, 1); err != nil {
		status.Error = "Service busy (semaphore timeout)"
		return status
	}
	defer cidg.sem.Release(1)

	// Prepare a minimal request to check token validity
	// Using get-on-chain-deals with empty body is lightweight
	requestParams := CIDgravityDealStatusRequest{}
	var requestBody = new(bytes.Buffer)
	if err := json.NewEncoder(requestBody).Encode(requestParams); err != nil {
		status.Error = "Failed to encode request: " + err.Error()
		return status
	}

	req, err := http.NewRequestWithContext(ctx, "POST", cfg.CidGravity.ApiEndpointGetDeals, requestBody)
	if err != nil {
		status.Error = "Failed to create request: " + err.Error()
		return status
	}

	req.Header.Set("X-API-KEY", cfg.CidGravity.ApiToken)
	req.Header.Set("Content-Type", "application/json")

	client := http.Client{
		Transport: &http.Transport{
			DisableKeepAlives:   true,
			MaxIdleConnsPerHost: -1,
		},
		Timeout: 10 * time.Second,
	}

	startTime := time.Now()
	resp, err := client.Do(req)
	status.ResponseTimeMs = time.Since(startTime).Milliseconds()

	if err != nil {
		status.Error = "Connection failed: " + err.Error()
		return status
	}
	defer resp.Body.Close()

	// Connection succeeded
	status.Connected = true

	// Check if token is valid based on response status
	switch resp.StatusCode {
	case 200:
		status.TokenValid = true
	case 401, 403:
		status.Error = "Invalid or unauthorized API token"
	default:
		status.Error = "Unexpected status code: " + resp.Status
	}

	return status
}
