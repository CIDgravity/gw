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
	"github.com/filecoin-project/go-state-types/abi"
)

type Cid struct {
        Root string `json:"/" cidgravity:"required"`
}
type CIDgravityDealProposalStatus struct {
	PieceCid             Cid            `json:"PieceCID"`
	PieceSize            uint64         `json:"PieceSize"`
	VerifiedDeal         bool           `json:"VerifiedDeal"`
	Client               string         `json:"Client"`
	Provider             string         `json:"Provider"`
	Label                string         `json:"Label"`
	StartEpoch           abi.ChainEpoch `json:"StartEpoch"`
	EndEpoch             abi.ChainEpoch `json:"EndEpoch"`
	StoragePricePerEpoch string         `json:"StoragePricePerEpoch"`
	ProviderCollateral   string         `json:"ProviderCollateral"`
	ClientCollateral     string         `json:"ClientCollateral"`
}
type CIDgravityDealProposalState struct {
	Status            string         `json:"Status"`
	PublishedEpoch    abi.ChainEpoch `json:"publishedEpoch"`
	OnChainStartEpoch abi.ChainEpoch `json:"onChainStartEpoch"`
	OnChainEndEpoch   abi.ChainEpoch `json:"onChainEndEpoch"`
}
type CIDgravityDealStatus struct {
	Proposal   CIDgravityDealProposalStatus `json:"proposal"`
	State	   CIDgravityDealProposalState  `json:"state"`
	LastUpdate float64                      `json:"lastUpdate"`
}
type CIDgravityDealStatesAPIResponse struct {
	Error  CIDgravityAPIError                  `json:"error"`
	Next   *string                             `json:"next"`
	Result map[abi.DealID]CIDgravityDealStatus `json:"result"`
}
type CIDgravityDealStatusRequest struct {
	Next   *string `json:"next"`
	SortBy *string `json:"sortBy"`
}

func (cidg *CIDGravity) getDealStates(client *http.Client, token string, states *map[abi.DealID]CIDgravityDealStatus) error {
	cfg := configuration.GetConfig()

	method := "POST"
	endpoint := cfg.CidGravity.ApiEndpointGetDeals
	requestParams := CIDgravityDealStatusRequest{}

	for {
		// Parse params for request body
		var requestBody = new(bytes.Buffer)
		err := json.NewEncoder(requestBody).Encode(requestParams)

		if err != nil {
			return err
		}

		// Create HTTP request
		req, err := http.NewRequest(method, endpoint, requestBody)

		if err != nil {
			return err
		}

		// Add authorization header
		req.Header.Set("X-API-KEY", token)


		// Send the request
		resp, err := client.Do(req)

		if err != nil {
			return err
		}

		// Defer closing the response body
		defer resp.Body.Close()

		// Read the response content
		responseBody, err := ioutil.ReadAll(resp.Body)

		if err != nil {
			return err
		}

		// Parse response to a valid response struct
		var result CIDgravityDealStatesAPIResponse
		err = json.Unmarshal(responseBody, &result)

		if err != nil {
			return err
		}

		// result object can contain both response message or error depending on request status code
		// If status code is not 200, return the object, but with an error that can be checked in main function
		if resp.StatusCode != 200 {
			return fmt.Errorf("status code is not 200")
		}
		for k, v := range result.Result {
			(*states)[k] = v
		}
		if result.Next == nil {
			break
		}
		requestParams.Next = result.Next
	}
	return nil
}

func (cidg *CIDGravity) GetDealStates(ctx context.Context) (map[abi.DealID]CIDgravityDealStatus, error) {
	cidg.init()

	states := map[abi.DealID]CIDgravityDealStatus{}

	cfg := configuration.GetConfig()

	authToken := cfg.CidGravity.ApiToken

	log.Debugw("getDealStates")

	if err := cidg.sem.Acquire(ctx, 1); err != nil {
		return nil, fmt.Errorf("Failed to acquire semaphore: %w", err)
	}
	defer cidg.sem.Release(1)

	// Create HTTP client
	// This will also define the request timeout to 30 seconds
	client := http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 1,
		},
		Timeout: 30 * time.Second,
	}
	if err := cidg.getDealStates(&client, authToken, &states); err != nil {
		return nil, err
	}
	for _, token := range cfg.CidGravity.AltTokens {
		if err := cidg.getDealStates(&client, token, &states); err != nil {
			return nil, err
		}
	}
	log.Debugw("getDealStates", "states", len(states))
	return states, nil
}
