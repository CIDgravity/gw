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
	apiURL     string
	serviceURL string
	client     *http.Client
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

type CreateAccountResult struct {
	URL   string `json:"url"`
	Token string `json:"token"`
}

type createAccountResponse struct {
	Error  *cidgAPIError       `json:"error"`
	Result CreateAccountResult `json:"result"`
}

type OnboardingPolicySettingsRequest struct {
	Priority          string `json:"priority"`
	MixNumberOfCopies int    `json:"mixNumberOfCopies"`
}

type onboardingPolicySettingsResponse struct {
	Error  *cidgAPIError `json:"error"`
	Result any           `json:"result"`
}

type testGBAPRequest struct {
	PieceCid             string `json:"pieceCid"`
	StartEpochHeadOffset int64  `json:"startEpochHeadOffset"`
	Duration             int64  `json:"duration"`
	StoragePricePerEpoch string `json:"storagePricePerEpoch"`
	ProviderCollateral   string `json:"providerCollateral"`
	VerifiedDeal         bool   `json:"verifiedDeal"`
	TransferSize         int64  `json:"transferSize"`
	TransferType         string `json:"transferType"`
	RemoveUnsealedCopy   bool   `json:"removeUnsealedCopy"`
}

type gbapResponse struct {
	Error  *cidgAPIError `json:"error"`
	Result any           `json:"result"`
}

func NewCidGravity(baseURL, serviceURL string) *CidGravity {
	return &CidGravity{
		apiURL:     strings.TrimRight(baseURL, "/"),
		serviceURL: strings.TrimRight(serviceURL, "/"),
		client:     &http.Client{Timeout: 15 * time.Second},
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

func (cd *CidGravity) CreateAccount(ctx context.Context, reqBody CreateAccountRequest) (CreateAccountResult, error) {
	var out CreateAccountResult
	endpoint := fmt.Sprintf("%s/api/addresses/gateway/create-account", cd.apiURL)

	b, err := json.Marshal(reqBody)
	if err != nil {
		return out, err
	}
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

func (cd *CidGravity) InitializeOnboardingPolicy(ctx context.Context, bearerToken, addressID string, reqBody OnboardingPolicySettingsRequest) error {
	endpoint := fmt.Sprintf("%s/jwt/v1/client-backend/onboarding-policy/settings/new", cd.serviceURL)

	b, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(string(b)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(bearerToken))
	req.Header.Set("X-Address-ID", strings.TrimSpace(addressID))

	resp, err := cd.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("cidgravity initialize onboarding policy: http %s: %s", resp.Status, string(body))
	}

	var out onboardingPolicySettingsResponse
	if len(body) > 0 {
		if err := json.Unmarshal(body, &out); err == nil && out.Error != nil {
			return fmt.Errorf("cidgravity initialize onboarding policy: %s (%s)", out.Error.Message, out.Error.Code)
		}
	}

	return nil
}

func (cd *CidGravity) TestGetBestAvailableProviders(ctx context.Context, apiToken string) error {
	endpoint := fmt.Sprintf("%s/private/v1/get-best-available-providers", cd.serviceURL)

	b, err := json.Marshal(testGBAPRequest{
		PieceCid:             "baga6ea4seaqfyiicys4rxe6pncl3np4g4eeavhh5qrq2lvqluhufkdk5iuvgyli",
		StartEpochHeadOffset: 5760,
		Duration:             518400,
		StoragePricePerEpoch: "0",
		ProviderCollateral:   "0",
		VerifiedDeal:         true,
		TransferSize:         34359738368,
		TransferType:         "http",
		RemoveUnsealedCopy:   false,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(string(b)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-KEY", strings.TrimSpace(apiToken))

	resp, err := cd.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var out gbapResponse
	if len(body) > 0 {
		_ = json.Unmarshal(body, &out)
	}
	if out.Error != nil {
		return fmt.Errorf("cidgravity GBAP test: %s (%s)", out.Error.Message, out.Error.Code)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("cidgravity GBAP test: http %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	return nil
}
