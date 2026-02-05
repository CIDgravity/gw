package s3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testAccessKeyID = "AKIAIOSFODNN7EXAMPLE"
	testSecretKey   = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	testRegion      = "us-east-1"
	emptyBodyHash   = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

func newTestAuthenticator() *Authenticator {
	return &Authenticator{
		Enabled:     true,
		AccessKeyID: testAccessKeyID,
		SecretKey:   testSecretKey,
		Region:      testRegion,
	}
}

// signRequest is a helper that signs an HTTP request using AWS SDK's v4 signer
func signRequest(t *testing.T, req *http.Request, accessKey, secretKey, region, payloadHash string, signTime time.Time) {
	t.Helper()
	creds := aws.Credentials{
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
	}
	signer := v4.NewSigner()
	err := signer.SignHTTP(context.Background(), creds, req, payloadHash, "s3", region, signTime)
	require.NoError(t, err, "signing request should not fail")
}

// ========================================
// Signature Validation Tests
// ========================================

func TestValidateSignatureV4_ValidRequest(t *testing.T) {
	auth := newTestAuthenticator()

	req, err := http.NewRequest("GET", "http://example.com/bucket/key", nil)
	require.NoError(t, err)

	now := time.Now().UTC()
	req.Header.Set("x-amz-date", now.Format("20060102T150405Z"))
	req.Header.Set("x-amz-content-sha256", emptyBodyHash)

	signRequest(t, req, testAccessKeyID, testSecretKey, testRegion, emptyBodyHash, now)

	authCtx, err := auth.validateSignatureV4(req)
	require.NoError(t, err)
	assert.NotNil(t, authCtx)
	assert.NotEmpty(t, authCtx.seedSignature)
	assert.NotEmpty(t, authCtx.signingKey)
}

func TestValidateSignatureV4_InvalidSignature(t *testing.T) {
	auth := newTestAuthenticator()

	req, err := http.NewRequest("GET", "http://example.com/bucket/key", nil)
	require.NoError(t, err)

	now := time.Now().UTC()
	req.Header.Set("x-amz-date", now.Format("20060102T150405Z"))
	req.Header.Set("x-amz-content-sha256", emptyBodyHash)

	// Sign with wrong secret key
	signRequest(t, req, testAccessKeyID, "wrong-secret-key", testRegion, emptyBodyHash, now)

	authCtx, err := auth.validateSignatureV4(req)
	require.Error(t, err)
	assert.Nil(t, authCtx)
	assert.Contains(t, err.Error(), "signature mismatch")
}

func TestValidateSignatureV4_ExpiredRequest(t *testing.T) {
	auth := newTestAuthenticator()

	req, err := http.NewRequest("GET", "http://example.com/bucket/key", nil)
	require.NoError(t, err)

	// Use a time that's more than 15 minutes old (outside the allowed skew)
	oldTime := time.Now().UTC().Add(-20 * time.Minute)
	req.Header.Set("x-amz-date", oldTime.Format("20060102T150405Z"))
	req.Header.Set("x-amz-content-sha256", emptyBodyHash)

	signRequest(t, req, testAccessKeyID, testSecretKey, testRegion, emptyBodyHash, oldTime)

	authCtx, err := auth.validateSignatureV4(req)
	require.Error(t, err)
	assert.Nil(t, authCtx)
	assert.Contains(t, err.Error(), "skewed")
}

func TestValidateSignatureV4_FutureRequest(t *testing.T) {
	auth := newTestAuthenticator()

	req, err := http.NewRequest("GET", "http://example.com/bucket/key", nil)
	require.NoError(t, err)

	// Use a time that's more than 15 minutes in the future
	futureTime := time.Now().UTC().Add(20 * time.Minute)
	req.Header.Set("x-amz-date", futureTime.Format("20060102T150405Z"))
	req.Header.Set("x-amz-content-sha256", emptyBodyHash)

	signRequest(t, req, testAccessKeyID, testSecretKey, testRegion, emptyBodyHash, futureTime)

	authCtx, err := auth.validateSignatureV4(req)
	require.Error(t, err)
	assert.Nil(t, authCtx)
	assert.Contains(t, err.Error(), "skewed")
}

func TestValidateSignatureV4_MissingAuthHeader(t *testing.T) {
	auth := newTestAuthenticator()

	req, err := http.NewRequest("GET", "http://example.com/bucket/key", nil)
	require.NoError(t, err)

	now := time.Now().UTC()
	req.Header.Set("x-amz-date", now.Format("20060102T150405Z"))
	req.Header.Set("x-amz-content-sha256", emptyBodyHash)
	// Don't set Authorization header

	authCtx, err := auth.validateSignatureV4(req)
	require.Error(t, err)
	assert.Nil(t, authCtx)
	assert.Contains(t, err.Error(), "missing Authorization header")
}

func TestValidateSignatureV4_MalformedAuthHeader(t *testing.T) {
	auth := newTestAuthenticator()

	testCases := []struct {
		name   string
		header string
		errMsg string
	}{
		{
			name:   "empty header",
			header: "",
			errMsg: "missing Authorization header",
		},
		{
			name:   "wrong auth scheme",
			header: "Basic dXNlcjpwYXNz",
			errMsg: "unsupported auth scheme",
		},
		{
			name:   "scheme only",
			header: "AWS4-HMAC-SHA256",
			errMsg: "malformed Authorization header",
		},
		{
			name:   "missing Credential",
			header: "AWS4-HMAC-SHA256 SignedHeaders=host;x-amz-date, Signature=abc123",
			errMsg: "missing Credential",
		},
		{
			name:   "missing SignedHeaders",
			header: "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20230101/us-east-1/s3/aws4_request, Signature=abc123",
			errMsg: "missing SignedHeaders",
		},
		{
			name:   "missing Signature",
			header: "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20230101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date",
			errMsg: "missing Signature",
		},
		{
			name:   "malformed Credential (too few parts)",
			header: "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20230101, SignedHeaders=host;x-amz-date, Signature=abc123",
			errMsg: "malformed Credential scope",
		},
		{
			name:   "invalid pair format",
			header: "AWS4-HMAC-SHA256 Credential, SignedHeaders=host, Signature=abc",
			errMsg: "invalid Authorization pair",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", "http://example.com/bucket/key", nil)
			require.NoError(t, err)

			req.Header.Set("Authorization", tc.header)
			req.Header.Set("x-amz-date", time.Now().UTC().Format("20060102T150405Z"))
			req.Header.Set("x-amz-content-sha256", emptyBodyHash)

			authCtx, err := auth.validateSignatureV4(req)
			require.Error(t, err)
			assert.Nil(t, authCtx)
			assert.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func TestValidateSignatureV4_AccessKeyMismatch(t *testing.T) {
	auth := newTestAuthenticator()

	req, err := http.NewRequest("GET", "http://example.com/bucket/key", nil)
	require.NoError(t, err)

	now := time.Now().UTC()
	req.Header.Set("x-amz-date", now.Format("20060102T150405Z"))
	req.Header.Set("x-amz-content-sha256", emptyBodyHash)

	// Sign with different access key
	signRequest(t, req, "DIFFERENT-ACCESS-KEY-ID", testSecretKey, testRegion, emptyBodyHash, now)

	authCtx, err := auth.validateSignatureV4(req)
	require.Error(t, err)
	assert.Nil(t, authCtx)
	assert.Contains(t, err.Error(), "access key mismatch")
}

func TestValidateSignatureV4_AuthDisabled(t *testing.T) {
	auth := &Authenticator{
		Enabled:     false,
		AccessKeyID: testAccessKeyID,
		SecretKey:   testSecretKey,
		Region:      testRegion,
	}

	req, err := http.NewRequest("GET", "http://example.com/bucket/key", nil)
	require.NoError(t, err)
	// No auth headers set

	authCtx, err := auth.validateSignatureV4(req)
	require.NoError(t, err)
	assert.Nil(t, authCtx) // Returns nil context when auth is disabled
}

func TestValidateSignatureV4_UnsignedPayload(t *testing.T) {
	auth := newTestAuthenticator()

	req, err := http.NewRequest("GET", "http://example.com/bucket/key", nil)
	require.NoError(t, err)

	now := time.Now().UTC()
	req.Header.Set("x-amz-date", now.Format("20060102T150405Z"))
	req.Header.Set("x-amz-content-sha256", "UNSIGNED-PAYLOAD")

	signRequest(t, req, testAccessKeyID, testSecretKey, testRegion, "UNSIGNED-PAYLOAD", now)

	authCtx, err := auth.validateSignatureV4(req)
	require.NoError(t, err)
	assert.NotNil(t, authCtx)
}

// ========================================
// Header Parsing Tests
// ========================================

func TestParseAuthorizationHeader_Valid(t *testing.T) {
	auth := newTestAuthenticator()

	header := "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20230615/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=abc123def456"

	parsed, err := auth.parseAuthorizationHeaderV4(header)
	require.NoError(t, err)

	assert.Equal(t, "AKIAIOSFODNN7EXAMPLE", parsed.AccessKeyID)
	assert.Equal(t, "20230615", parsed.Date)
	assert.Equal(t, "us-east-1", parsed.Region)
	assert.Equal(t, "s3", parsed.Service)
	assert.Equal(t, "abc123def456", parsed.Signature)

	// Check signed headers
	assert.Contains(t, parsed.SignedHeaders, "host")
	assert.Contains(t, parsed.SignedHeaders, "x-amz-content-sha256")
	assert.Contains(t, parsed.SignedHeaders, "x-amz-date")
}

func TestParseAuthorizationHeader_WithQuotedValues(t *testing.T) {
	auth := newTestAuthenticator()

	// Some implementations might quote values
	header := `AWS4-HMAC-SHA256 Credential="AKIAIOSFODNN7EXAMPLE/20230615/us-east-1/s3/aws4_request", SignedHeaders="host;x-amz-date", Signature="abc123"`

	parsed, err := auth.parseAuthorizationHeaderV4(header)
	require.NoError(t, err)

	assert.Equal(t, "AKIAIOSFODNN7EXAMPLE", parsed.AccessKeyID)
	assert.Equal(t, "abc123", parsed.Signature)
}

func TestParseAuthorizationHeader_WithExtraWhitespace(t *testing.T) {
	auth := newTestAuthenticator()

	header := "AWS4-HMAC-SHA256   Credential=AKIAIOSFODNN7EXAMPLE/20230615/us-east-1/s3/aws4_request,   SignedHeaders=host;x-amz-date,  Signature=abc123  "

	parsed, err := auth.parseAuthorizationHeaderV4(header)
	require.NoError(t, err)

	assert.Equal(t, "AKIAIOSFODNN7EXAMPLE", parsed.AccessKeyID)
	assert.Equal(t, "abc123", parsed.Signature)
}

func TestParseCredential(t *testing.T) {
	auth := newTestAuthenticator()

	testCases := []struct {
		name         string
		header       string
		wantAccessID string
		wantDate     string
		wantRegion   string
		wantService  string
	}{
		{
			name:         "standard credential",
			header:       "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20230615/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc",
			wantAccessID: "AKIAIOSFODNN7EXAMPLE",
			wantDate:     "20230615",
			wantRegion:   "us-east-1",
			wantService:  "s3",
		},
		{
			name:         "different region",
			header:       "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20230615/eu-west-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc",
			wantAccessID: "AKIAIOSFODNN7EXAMPLE",
			wantDate:     "20230615",
			wantRegion:   "eu-west-1",
			wantService:  "s3",
		},
		{
			name:         "long access key",
			header:       "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLEEXTENDED123/20230615/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc",
			wantAccessID: "AKIAIOSFODNN7EXAMPLEEXTENDED123",
			wantDate:     "20230615",
			wantRegion:   "us-east-1",
			wantService:  "s3",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := auth.parseAuthorizationHeaderV4(tc.header)
			require.NoError(t, err)

			assert.Equal(t, tc.wantAccessID, parsed.AccessKeyID)
			assert.Equal(t, tc.wantDate, parsed.Date)
			assert.Equal(t, tc.wantRegion, parsed.Region)
			assert.Equal(t, tc.wantService, parsed.Service)
		})
	}
}

// ========================================
// Signature Calculation Tests
// ========================================

func TestCalculateSignature_MatchesAWSSDK(t *testing.T) {
	auth := newTestAuthenticator()

	// Create a request and sign it with the AWS SDK
	req, err := http.NewRequest("GET", "http://example.com/bucket/key", nil)
	require.NoError(t, err)

	now := time.Now().UTC()
	req.Header.Set("x-amz-date", now.Format("20060102T150405Z"))
	req.Header.Set("x-amz-content-sha256", emptyBodyHash)

	signRequest(t, req, testAccessKeyID, testSecretKey, testRegion, emptyBodyHash, now)

	// Parse the Authorization header created by the SDK
	sigHeader := req.Header.Get("Authorization")
	parsedSig, err := auth.parseAuthorizationHeaderV4(sigHeader)
	require.NoError(t, err)

	// Compute signature using our implementation
	authCtx, err := auth.computeSignatureV4(req, &parsedSig)
	require.NoError(t, err)

	// The computed signature should match what's in the Authorization header
	assert.Equal(t, parsedSig.Signature, authCtx.seedSignature)
}

func TestCanonicalRequest_BasicGET(t *testing.T) {
	auth := newTestAuthenticator()

	req, err := http.NewRequest("GET", "http://example.com/bucket/key", nil)
	require.NoError(t, err)

	now := time.Now().UTC()
	req.Header.Set("x-amz-date", now.Format("20060102T150405Z"))
	req.Header.Set("x-amz-content-sha256", emptyBodyHash)
	req.Header.Set("Host", "example.com")

	signRequest(t, req, testAccessKeyID, testSecretKey, testRegion, emptyBodyHash, now)

	// Verify the canonical path
	canonPath := auth.canonicalizePath(req.URL)
	assert.Equal(t, "/bucket/key", canonPath)
}

func TestCanonicalRequest_RootPath(t *testing.T) {
	auth := newTestAuthenticator()

	u, err := url.Parse("http://example.com/")
	require.NoError(t, err)

	canonPath := auth.canonicalizePath(u)
	assert.Equal(t, "/", canonPath)
}

func TestCanonicalRequest_EmptyPath(t *testing.T) {
	auth := newTestAuthenticator()

	u, err := url.Parse("http://example.com")
	require.NoError(t, err)

	canonPath := auth.canonicalizePath(u)
	assert.Equal(t, "/", canonPath)
}

func TestCanonicalRequest_NilURL(t *testing.T) {
	auth := newTestAuthenticator()

	canonPath := auth.canonicalizePath(nil)
	assert.Equal(t, "/", canonPath)
}

func TestCanonicalQuery_Empty(t *testing.T) {
	auth := newTestAuthenticator()

	u, err := url.Parse("http://example.com/bucket/key")
	require.NoError(t, err)

	canonQuery := auth.canonicalizeQuery(u)
	assert.Equal(t, "", canonQuery)
}

func TestCanonicalQuery_SingleParam(t *testing.T) {
	auth := newTestAuthenticator()

	u, err := url.Parse("http://example.com/bucket?list-type=2")
	require.NoError(t, err)

	canonQuery := auth.canonicalizeQuery(u)
	assert.Equal(t, "list-type=2", canonQuery)
}

func TestCanonicalQuery_MultipleParams(t *testing.T) {
	auth := newTestAuthenticator()

	u, err := url.Parse("http://example.com/bucket?prefix=foo&delimiter=/&max-keys=100")
	require.NoError(t, err)

	canonQuery := auth.canonicalizeQuery(u)
	// Parameters should be sorted alphabetically
	assert.Equal(t, "delimiter=%2F&max-keys=100&prefix=foo", canonQuery)
}

func TestCanonicalQuery_SpecialCharacters(t *testing.T) {
	auth := newTestAuthenticator()

	u, err := url.Parse("http://example.com/bucket?prefix=path/to/obj&marker=key%20with%20spaces")
	require.NoError(t, err)

	canonQuery := auth.canonicalizeQuery(u)
	assert.Contains(t, canonQuery, "marker=key%20with%20spaces")
	assert.Contains(t, canonQuery, "prefix=path%2Fto%2Fobj")
}

func TestCanonicalQuery_NilURL(t *testing.T) {
	auth := newTestAuthenticator()

	canonQuery := auth.canonicalizeQuery(nil)
	assert.Equal(t, "", canonQuery)
}

func TestCanonicalHeaders_Basic(t *testing.T) {
	auth := newTestAuthenticator()

	headers := http.Header{}
	headers.Set("Host", "example.com")
	headers.Set("x-amz-date", "20230615T120000Z")
	headers.Set("x-amz-content-sha256", emptyBodyHash)

	signedHeaders := map[string]struct{}{
		"host":                 {},
		"x-amz-date":           {},
		"x-amz-content-sha256": {},
	}

	canonHeaders, signedHeadersList := auth.canonicalizeHeaders(headers, signedHeaders)

	// Headers should be sorted alphabetically
	assert.Contains(t, canonHeaders, "host:example.com\n")
	assert.Contains(t, canonHeaders, "x-amz-date:20230615T120000Z\n")
	assert.Contains(t, canonHeaders, "x-amz-content-sha256:"+emptyBodyHash+"\n")

	// Signed headers list should also be sorted
	assert.Equal(t, "host;x-amz-content-sha256;x-amz-date", signedHeadersList)
}

func TestCanonicalHeaders_WithDuplicateSpaces(t *testing.T) {
	auth := newTestAuthenticator()

	headers := http.Header{}
	headers.Set("Host", "example.com")
	headers.Set("x-amz-date", "20230615T120000Z")
	headers.Set("x-amz-content-sha256", emptyBodyHash)
	headers.Set("x-amz-meta-custom", "value  with   multiple    spaces")

	signedHeaders := map[string]struct{}{
		"host":                 {},
		"x-amz-date":           {},
		"x-amz-content-sha256": {},
		"x-amz-meta-custom":    {},
	}

	canonHeaders, _ := auth.canonicalizeHeaders(headers, signedHeaders)

	// Multiple spaces should be collapsed to single space
	assert.Contains(t, canonHeaders, "x-amz-meta-custom:value with multiple spaces\n")
}

func TestStringToSign_Components(t *testing.T) {
	auth := newTestAuthenticator()

	req, err := http.NewRequest("GET", "http://example.com/bucket/key", nil)
	require.NoError(t, err)

	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", emptyBodyHash)

	signRequest(t, req, testAccessKeyID, testSecretKey, testRegion, emptyBodyHash, now)

	sigHeader := req.Header.Get("Authorization")
	parsedSig, err := auth.parseAuthorizationHeaderV4(sigHeader)
	require.NoError(t, err)

	// Compute signature and verify auth context
	authCtx, err := auth.computeSignatureV4(req, &parsedSig)
	require.NoError(t, err)

	// Verify the scope format
	expectedScope := now.Format("20060102") + "/" + testRegion + "/s3/aws4_request"
	assert.Equal(t, expectedScope, authCtx.scope)

	// Verify the amzDate is captured
	assert.Equal(t, amzDate, authCtx.amzDate)
}

// ========================================
// Signed Header Validation Tests
// ========================================

func TestValidateSignedHeaders_MissingHost(t *testing.T) {
	auth := newTestAuthenticator()

	parsedSig := &sigV4AuthParts{
		SignedHeaders: map[string]struct{}{
			"x-amz-date":           {},
			"x-amz-content-sha256": {},
		},
	}

	err := auth.validateSignedHeaders(parsedSig)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing host header")
}

func TestValidateSignedHeaders_MissingContentSha256(t *testing.T) {
	auth := newTestAuthenticator()

	parsedSig := &sigV4AuthParts{
		SignedHeaders: map[string]struct{}{
			"host":       {},
			"x-amz-date": {},
		},
	}

	err := auth.validateSignedHeaders(parsedSig)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing x-amz-content-sha256")
}

func TestValidateSignedHeaders_MissingAmzDate(t *testing.T) {
	auth := newTestAuthenticator()

	parsedSig := &sigV4AuthParts{
		SignedHeaders: map[string]struct{}{
			"host":                 {},
			"x-amz-content-sha256": {},
		},
	}

	err := auth.validateSignedHeaders(parsedSig)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing x-amz-date")
}

func TestValidateSignedHeaders_AllRequired(t *testing.T) {
	auth := newTestAuthenticator()

	parsedSig := &sigV4AuthParts{
		SignedHeaders: map[string]struct{}{
			"host":                 {},
			"x-amz-date":           {},
			"x-amz-content-sha256": {},
		},
	}

	err := auth.validateSignedHeaders(parsedSig)
	require.NoError(t, err)
}

// ========================================
// AWS URL Encoding Tests
// ========================================

func TestAwsURLEncode_Unreserved(t *testing.T) {
	// Unreserved characters should not be encoded
	testCases := []struct {
		input    string
		expected string
	}{
		{"abc", "abc"},
		{"ABC", "ABC"},
		{"123", "123"},
		{"a-b_c.d~e", "a-b_c.d~e"},
	}

	for _, tc := range testCases {
		result := awsURLEncode(tc.input)
		assert.Equal(t, tc.expected, result)
	}
}

func TestAwsURLEncode_Reserved(t *testing.T) {
	// Reserved characters should be percent-encoded
	testCases := []struct {
		input    string
		expected string
	}{
		{" ", "%20"},
		{"/", "%2F"},
		{"?", "%3F"},
		{"=", "%3D"},
		{"&", "%26"},
		{"+", "%2B"},
	}

	for _, tc := range testCases {
		result := awsURLEncode(tc.input)
		assert.Equal(t, tc.expected, result)
	}
}

// ========================================
// Helper Function Tests
// ========================================

func TestSha256Hex(t *testing.T) {
	// Test empty string
	result := sha256Hex("")
	assert.Equal(t, emptyBodyHash, result)

	// Test known value
	result = sha256Hex("test")
	h := sha256.Sum256([]byte("test"))
	expected := hex.EncodeToString(h[:])
	assert.Equal(t, expected, result)
}

func TestHmacSHA256(t *testing.T) {
	key := []byte("key")
	data := []byte("data")

	result := hmacSHA256(key, data)
	assert.NotNil(t, result)
	assert.Len(t, result, 32) // SHA256 produces 32 bytes
}

func TestHmacSHA256Hex(t *testing.T) {
	key := []byte("key")
	data := []byte("data")

	result := hmacSHA256Hex(key, data)
	assert.Len(t, result, 64) // Hex encoded 32 bytes = 64 characters
}

// ========================================
// Edge Case Tests
// ========================================

func TestValidateSignatureV4_MissingAmzDateHeader(t *testing.T) {
	auth := newTestAuthenticator()

	req, err := http.NewRequest("GET", "http://example.com/bucket/key", nil)
	require.NoError(t, err)

	// Build auth header manually without x-amz-date in the actual headers
	req.Header.Set("x-amz-content-sha256", emptyBodyHash)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20230615/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=abc123")

	authCtx, err := auth.validateSignatureV4(req)
	require.Error(t, err)
	assert.Nil(t, authCtx)
	assert.Contains(t, err.Error(), "missing x-amz-date")
}

func TestValidateSignatureV4_InvalidAmzDateFormat(t *testing.T) {
	auth := newTestAuthenticator()

	req, err := http.NewRequest("GET", "http://example.com/bucket/key", nil)
	require.NoError(t, err)

	req.Header.Set("x-amz-date", "2023-06-15T12:00:00Z") // Wrong format, should be 20230615T120000Z
	req.Header.Set("x-amz-content-sha256", emptyBodyHash)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20230615/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=abc123")

	authCtx, err := auth.validateSignatureV4(req)
	require.Error(t, err)
	assert.Nil(t, authCtx)
	assert.Contains(t, err.Error(), "invalid x-amz-date format")
}

func TestValidateSignatureV4_MissingContentSha256Header(t *testing.T) {
	auth := newTestAuthenticator()

	req, err := http.NewRequest("GET", "http://example.com/bucket/key", nil)
	require.NoError(t, err)

	now := time.Now().UTC()
	req.Header.Set("x-amz-date", now.Format("20060102T150405Z"))
	// Don't set x-amz-content-sha256
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20230615/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=abc123")

	authCtx, err := auth.validateSignatureV4(req)
	require.Error(t, err)
	assert.Nil(t, authCtx)
	assert.Contains(t, err.Error(), "x-amz-content-sha256")
}

func TestValidateSignatureV4_WithQueryString(t *testing.T) {
	auth := newTestAuthenticator()

	req, err := http.NewRequest("GET", "http://example.com/bucket?list-type=2&prefix=test/", nil)
	require.NoError(t, err)

	now := time.Now().UTC()
	req.Header.Set("x-amz-date", now.Format("20060102T150405Z"))
	req.Header.Set("x-amz-content-sha256", emptyBodyHash)

	signRequest(t, req, testAccessKeyID, testSecretKey, testRegion, emptyBodyHash, now)

	authCtx, err := auth.validateSignatureV4(req)
	require.NoError(t, err)
	assert.NotNil(t, authCtx)
}

func TestValidateSignatureV4_DifferentHTTPMethods(t *testing.T) {
	methods := []string{"GET", "PUT", "POST", "DELETE", "HEAD"}

	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			auth := newTestAuthenticator()

			req, err := http.NewRequest(method, "http://example.com/bucket/key", nil)
			require.NoError(t, err)

			now := time.Now().UTC()
			req.Header.Set("x-amz-date", now.Format("20060102T150405Z"))
			req.Header.Set("x-amz-content-sha256", emptyBodyHash)

			signRequest(t, req, testAccessKeyID, testSecretKey, testRegion, emptyBodyHash, now)

			authCtx, err := auth.validateSignatureV4(req)
			require.NoError(t, err)
			assert.NotNil(t, authCtx)
		})
	}
}

func TestValidateSignatureV4_WithinAllowedSkew(t *testing.T) {
	auth := newTestAuthenticator()

	// Test within the 15 minute window
	testCases := []struct {
		name   string
		offset time.Duration
	}{
		{"5 minutes ago", -5 * time.Minute},
		{"5 minutes ahead", 5 * time.Minute},
		{"14 minutes ago", -14 * time.Minute},
		{"14 minutes ahead", 14 * time.Minute},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", "http://example.com/bucket/key", nil)
			require.NoError(t, err)

			signTime := time.Now().UTC().Add(tc.offset)
			req.Header.Set("x-amz-date", signTime.Format("20060102T150405Z"))
			req.Header.Set("x-amz-content-sha256", emptyBodyHash)

			signRequest(t, req, testAccessKeyID, testSecretKey, testRegion, emptyBodyHash, signTime)

			authCtx, err := auth.validateSignatureV4(req)
			require.NoError(t, err)
			assert.NotNil(t, authCtx)
		})
	}
}
