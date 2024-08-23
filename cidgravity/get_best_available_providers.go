package cidgravity

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
	"github.com/lotus-web3/ribs/configuration"
)

type CIDgravityGetBestAvailableProvidersRequest struct {
	PieceCid             string  `json:"pieceCid"`
	Provider             string  `json:"provider"`
	StartEpoch           uint64  `json:"startEpoch"`
	Duration             uint64  `json:"duration"`
	StoragePricePerEpoch string  `json:"storagePricePerEpoch"`
	ProviderCollateral   string  `json:"providerCollateral"`
	VerifiedDeal         *bool   `json:"verifiedDeal"`
	TransferSize         uint64  `json:"transferSize"`
	TransferType         string  `json:"transferType"`
	RemoveUnsealedCopy   *bool   `json:"removeUnsealedCopy"`
}

type CIDgravityAPIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type CIDgravityAPIResult struct {
	Reason    *string  `json:"reason"`
	Providers []string `json:"providers"`
}

type CIDgravityAPIResponse struct {
	Error  CIDgravityAPIError    `json:"error"`
	Result *CIDgravityAPIResult  `json:"result"`
}

func (cidg *CIDGravity) GetBestAvailableProviders(params CIDgravityGetBestAvailableProvidersRequest) ([]string, error) {
	log.Debugw("GetBestAvailableProviders", "params", params)
	cidg.init()

	cfg := configuration.GetConfig()

	// Define params
	method := "POST"
	authToken := cfg.CidGravity.ApiToken
	endpoint := cfg.CidGravity.ApiEndpointGetProviders

	// Parse params for request body
	var requestBody = new(bytes.Buffer)
	err := json.NewEncoder(requestBody).Encode(params)

	if err != nil {
		return nil, err
	}

	// Create HTTP request
	req, err := http.NewRequest(method, endpoint, requestBody)

	if err != nil {
		return nil, err
	}

	// Add authorization header
	req.Header.Set("X-API-KEY", authToken)

	// Create HTTP client
	// This will also define the request timeout to 30 seconds
	client := http.Client{
		Transport: &http.Transport{
			DisableKeepAlives:   true,
			MaxIdleConnsPerHost: -1,
		},
		Timeout: 30 * time.Second,
	}

	if err := cidg.sem.Acquire(context.TODO(), 1); err != nil {
		return nil, fmt.Errorf("Failed to acquire semaphore: %w", err)
	}
	// Send the request
	resp, err := client.Do(req)
	cidg.sem.Release(1)

	if err != nil {
		return nil, err
	}

	// Defer closing the response body
	defer resp.Body.Close()

	// Read the response content
	responseBody, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return nil, err
	}

	// Parse response to a valid response struct
	var result CIDgravityAPIResponse
	err = json.Unmarshal(responseBody, &result)

	if err != nil {
		return nil, err
	}

	// result object can contain both response message or error depending on request status code
	// If status code is not 200, return the object, but with an error that can be checked in main function
	if resp.StatusCode != 200 {
		log.Errorw("GetBestAvailableProviders", "code", resp.StatusCode, "result", result)
		return []string{result.Error.Code, result.Error.Message}, fmt.Errorf("status code is not 200")
	}

	log.Debugw("GetBestAvailableProviders", "result", result.Result)
	if result.Result == nil {
		return nil, fmt.Errorf("Failed to parse cidgravity providers")
	}
	return result.Result.Providers, nil
}
