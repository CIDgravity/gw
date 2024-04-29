package cidgravity

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
	"github.com/lotus-web3/ribs/configuration"
)

type CIDgravityGetBestAvailableProvidersRequest struct {
	PieceCid             string      `json:"pieceCid"`
	Provider             string      `json:"provider"`
	StartEpoch           uint64       `json:"startEpoch"`
	Duration             uint64       `json:"duration"`
	StoragePricePerEpoch json.Number `json:"storagePricePerEpoch"`
	ProviderCollateral   json.Number `json:"providerCollateral"`
	VerifiedDeal         *bool       `json:"verifiedDeal"`
	TransferSize         uint64       `json:"transferSize"`
	TransferType         string      `json:"transferType"`
	RemoveUnsealedCopy   *bool       `json:"removeUnsealedCopy"`
}

type CIDgravityAPIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type CIDgravityAPIResponse struct {
	Error  CIDgravityAPIError `json:"error"`
	Result []string           `json:"result"`
}

func GetBestAvailableProviders(params CIDgravityGetBestAvailableProvidersRequest) ([]string, error) {
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

	// Send the request
	resp, err := client.Do(req)

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
		return []string{result.Error.Code, result.Error.Message}, fmt.Errorf("status code is not 200")
	}

	return result.Result, nil
}

func main() {
	isVerifiedDeal := true
	removeUnsealedCopy := false

	params := CIDgravityGetBestAvailableProvidersRequest{
		PieceCid:             "baga6ea4seaqfyiicys4rxe6pncl3np4g4eeavhh5qrq2lvqluhufkdk5iuvgyli",
		StartEpoch:           3700038,
		Duration:             1051200,
		StoragePricePerEpoch: "0",
		ProviderCollateral:   "0",
		VerifiedDeal:         &isVerifiedDeal,
		TransferSize:         30059738368,
		TransferType:         "http",
		RemoveUnsealedCopy:   &removeUnsealedCopy,
	}

	response, err := GetBestAvailableProviders(params)

	if err != nil {
		log.Fatal(err, response)
	}

	// Print available providers
	log.Println(response)
}
