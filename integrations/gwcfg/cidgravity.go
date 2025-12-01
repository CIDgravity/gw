package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type CidGravity struct {
	apiURL string
	client *http.Client
}

type cidgAPIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type GetChallengeResult struct {
	Address   string `json:"address"`
	Challenge string `json:"challenge"`
	WorkerKey string `json:"workerKey"`
}

type getChallengeResponse struct {
	Error  *cidgAPIError      `json:"error"`
	Result GetChallengeResult `json:"result"`
}

type CreateAccountRequest struct {
	Challenge          string                     `json:"challenge"`
	AddressID          string                     `json:"addressId"`
	FriendlyName       string                     `json:"friendlyName"`
	SignedMessage      string                     `json:"signedMessage"`
	AddressInformation CreateAccountAddressEntity `json:"addressInformation"`
}

type CreateAccountAddressEntity struct {
	EntityName   string `json:"entityName"`
	EntityType   string `json:"entityType"`
	ContactEmail string `json:"contactEmail"`
}

type createAccountResult struct {
	URL   string `json:"url"`
	Token string `json:"token"`
}

type createAccountResponse struct {
	Error  *cidgAPIError       `json:"error"`
	Result createAccountResult `json:"result"`
}

func NewCidGravity(baseURL string) *CidGravity {
	return &CidGravity{
		apiURL: strings.TrimRight(baseURL, "/"),
		client: &http.Client{Timeout: 15 * time.Second},
	}
}

func (cd *CidGravity) GetChallenge(ctx context.Context, addressID string) (GetChallengeResult, error) {
	var out GetChallengeResult
	endpoint := fmt.Sprintf("%s/api/addresses/gateway/get-challenge/%s", cd.apiURL, addressID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return out, err
	}

	resp, err := cd.client.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return out, fmt.Errorf("cidgravity get-challenge: http %s: %s", resp.Status, string(body))
	}
	var gc getChallengeResponse
	if err := json.Unmarshal(body, &gc); err != nil {
		return out, fmt.Errorf("cidgravity get-challenge: decode: %w", err)
	}
	if gc.Error != nil {
		return out, fmt.Errorf("cidgravity get-challenge: %s (%s)", gc.Error.Message, gc.Error.Code)
	}
	return gc.Result, nil
}

func (cd *CidGravity) CreateAccount(ctx context.Context, reqBody CreateAccountRequest) (createAccountResult, error) {
	var out createAccountResult
	endpoint := fmt.Sprintf("%s/api/addresses/gateway/create-account", cd.apiURL)

	b, err := json.Marshal(reqBody)
	if err != nil {
		return out, err
	}
	fmt.Println(string(b))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(string(b)))
	if err != nil {
		return out, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := cd.client.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return out, fmt.Errorf("cidgravity create-account: http %s: %s", resp.Status, string(body))
	}
	var ca createAccountResponse
	if err := json.Unmarshal(body, &ca); err != nil {
		return out, fmt.Errorf("cidgravity create-account: decode: %w", err)
	}
	if ca.Error != nil {
		return out, fmt.Errorf("cidgravity create-account: %s (%s)", ca.Error.Message, ca.Error.Code)
	}
	return ca.Result, nil
}
