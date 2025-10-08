package s3

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/CIDgravity/filecoin-gateway/configuration"
)

const allowedSkew = 15 * time.Minute
const authScheme = "AWS4-HMAC-SHA256"

type sigV4AuthParts struct {
	AccessKeyID   string
	Date          string
	Region        string
	Service       string
	SignedHeaders map[string]struct{}
	Signature     string
}

type AuthenticationContext struct {
	seedSignature string
	amzDate       string
	scope         string
	signingKey    []byte
}

type Authenticator struct {
	Enabled                bool
	AccessKeyID, SecretKey string
	Region                 string
}

func NewAuthenticator(c *configuration.S3APIConfig) (*Authenticator, error) {
	if c.AuthEnabled {
		if c.RootAccessKeyId == "" || c.RootSecretKey == "" {
			return nil, fmt.Errorf("missing root credentials")
		}
	}

	return &Authenticator{
		Enabled:     c.AuthEnabled,
		AccessKeyID: c.RootAccessKeyId,
		SecretKey:   c.RootSecretKey,
		Region:      c.Region,
	}, nil
}

func (a *Authenticator) validateSignatureV4(req *http.Request) (*AuthenticationContext, error) {
	if !a.Enabled { //TODO remove the enable flag after production deployment
		log.Warnf("S3 auth disabled, skipping signature validation")
		return nil, nil
	}

	sigHeader := req.Header.Get("Authorization")
	parsedSig, err := a.parseAuthorizationHeaderV4(sigHeader)
	if err != nil {
		return nil, fmt.Errorf("parse signatureV4 header: %w", err)
	}

	if err := a.validateSignedHeaders(&parsedSig); err != nil {
		return nil, fmt.Errorf("validate signed headers: %w", err)
	}

	if a.AccessKeyID != parsedSig.AccessKeyID {
		return nil, fmt.Errorf("credential access key mismatch")
	}

	authCtx, err := a.computeSignatureV4(req, &parsedSig)
	if err != nil {
		return nil, fmt.Errorf("compute signatureV4: %w", err)
	}
	if authCtx.seedSignature != parsedSig.Signature {
		return nil, fmt.Errorf("signature mismatch")
	}

	return authCtx, nil
}

func (a *Authenticator) parseAuthorizationHeaderV4(header string) (sigV4AuthParts, error) {
	var out sigV4AuthParts
	header = strings.TrimSpace(header)
	if header == "" {
		return out, fmt.Errorf("missing Authorization header")
	}
	if !strings.HasPrefix(header, authScheme) {
		return out, fmt.Errorf("unsupported auth scheme")
	}

	rest := strings.TrimSpace(strings.TrimPrefix(header, authScheme))
	if rest == "" {
		return out, fmt.Errorf("malformed Authorization header")
	}

	parts := strings.Split(rest, ",")
	kv := make(map[string]string, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		eq := strings.IndexByte(p, '=')
		if eq <= 0 {
			return out, fmt.Errorf("invalid Authorization pair: %s", p)
		}
		k := strings.TrimSpace(p[:eq])
		v := strings.TrimSpace(p[eq+1:])
		v = strings.Trim(v, "\"")
		kv[strings.ToLower(k)] = v
	}
	cred, ok := kv["credential"]
	if !ok || cred == "" {
		return out, fmt.Errorf("missing Credential in Authorization header")
	}
	credParts := strings.Split(cred, "/")
	if len(credParts) < 5 {
		return out, fmt.Errorf("malformed Credential scope")
	}
	out.AccessKeyID = credParts[0]
	out.Date = credParts[1]
	out.Region = credParts[2]
	out.Service = credParts[3]

	if sh, ok := kv["signedheaders"]; ok {

		shSet := make(map[string]struct{})
		for _, h := range strings.Split(sh, ";") {
			name := strings.ToLower(strings.TrimSpace(h))
			if name == "" {
				continue
			}
			shSet[name] = struct{}{}
		}

		out.SignedHeaders = shSet
	} else {
		return out, fmt.Errorf("missing SignedHeaders in Authorization header")
	}
	if sig, ok := kv["signature"]; ok {
		out.Signature = sig
	} else {
		return out, fmt.Errorf("missing Signature in Authorization header")
	}
	return out, nil
}

func (a *Authenticator) validateSignedHeaders(parsedSig *sigV4AuthParts) error {
	if _, ok := parsedSig.SignedHeaders["host"]; !ok {
		return fmt.Errorf("missing host header in SignedHeaders")
	}
	if _, ok := parsedSig.SignedHeaders["x-amz-content-sha256"]; !ok {
		return fmt.Errorf("missing x-amz-content-sha256 header in SignedHeaders")
	}
	if _, ok := parsedSig.SignedHeaders["x-amz-date"]; !ok {
		return fmt.Errorf("missing x-amz-date header in SignedHeaders")
	}
	return nil
}

func (a *Authenticator) computeSignatureV4(req *http.Request, parsedSig *sigV4AuthParts) (ctx *AuthenticationContext, err error) {
	if req == nil {
		return nil, fmt.Errorf("nil request")
	}

	headers := req.Header
	if headers.Get("Host") == "" {
		headers.Set("Host", req.Host)
	}

	amzDate, err := a.getAmzDate(headers)
	if err != nil {
		return nil, err
	}

	payloadHash := headers.Get("x-amz-content-sha256")
	if payloadHash == "" {
		return nil, fmt.Errorf("missing x-amz-content-sha256 header")
	}

	canonicalURI := a.canonicalizePath(req.URL)
	canonicalQuery := a.canonicalizeQuery(req.URL)
	canonHeaders, signedHeadersList := a.canonicalizeHeaders(headers, parsedSig.SignedHeaders)

	b := &strings.Builder{}
	b.WriteString(req.Method)
	b.WriteString("\n")
	b.WriteString(canonicalURI)
	b.WriteString("\n")
	b.WriteString(canonicalQuery)
	b.WriteString("\n")
	b.WriteString(canonHeaders)
	b.WriteString("\n")
	b.WriteString(signedHeadersList)
	b.WriteString("\n")
	b.WriteString(payloadHash)
	canonicalRequest := b.String()
	canonHash := sha256Hex(canonicalRequest)

	date := amzDate[:8]
	scope := fmt.Sprintf("%s/%s/s3/aws4_request", date, parsedSig.Region)

	algorithm := "AWS4-HMAC-SHA256"
	stringToSign := algorithm + "\n" + amzDate + "\n" + scope + "\n" + canonHash

	kDate := hmacSHA256([]byte("AWS4"+a.SecretKey), []byte(date))
	kRegion := hmacSHA256(kDate, []byte(parsedSig.Region))
	kService := hmacSHA256(kRegion, []byte("s3"))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	sig := hmacSHA256Hex(kSigning, []byte(stringToSign))

	return &AuthenticationContext{
		seedSignature: sig,
		amzDate:       amzDate,
		scope:         scope,
		signingKey:    kSigning,
	}, nil
}

func (a *Authenticator) getAmzDate(headers http.Header) (string, error) {
	amzDate := headers.Get("x-amz-date")
	if amzDate == "" {
		return "", fmt.Errorf("missing x-amz-date header")
	}

	parsedAmzDate, perr := time.Parse("20060102T150405Z", amzDate)
	if perr != nil {
		return "", fmt.Errorf("invalid x-amz-date format: %w", perr)
	}
	now := time.Now().UTC()
	if d := now.Sub(parsedAmzDate); d > allowedSkew || d < -allowedSkew {
		return "", fmt.Errorf("x-amz-date too skewed")
	}

	return amzDate, nil
}

func (a *Authenticator) canonicalizePath(u *url.URL) string {
	if u == nil {
		return "/"
	}
	p := u.EscapedPath()
	if p == "" {
		p = "/"
	}
	return p
}

func (a *Authenticator) canonicalizeQuery(u *url.URL) string {
	if u == nil {
		return ""
	}
	q := u.Query()
	if len(q) == 0 {
		return ""
	}

	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	pairs := make([]string, 0, len(q))
	for _, k := range keys {
		vals := q[k]
		sort.Strings(vals)
		for _, v := range vals {
			pairs = append(pairs, awsURLEncode(k)+"="+awsURLEncode(v))
		}
	}
	return strings.Join(pairs, "&")
}

func (a *Authenticator) canonicalizeHeaders(headers http.Header, signedHeaders map[string]struct{}) (canonicalHeaders string, signedHeadersString string) {
	keys := make([]string, 0, len(headers))
	for k := range signedHeaders {
		lk := strings.ToLower(strings.TrimSpace(k))
		if lk == "" {
			continue
		}
		keys = append(keys, lk)
	}
	sort.Strings(keys)
	b := &strings.Builder{}
	signed := make([]string, 0, len(keys))
	for _, k := range keys {
		vals := headers.Values(k)
		if len(vals) == 0 {
			vals = []string{""}
		}

		canonVals := make([]string, 0, len(vals))
		for _, v := range vals {
			v = strings.Join(strings.Fields(v), " ") // remove duplicate spaces, trim
			canonVals = append(canonVals, v)
		}
		sort.Strings(canonVals)
		b.WriteString(k)
		b.WriteString(":")
		b.WriteString(strings.Join(canonVals, ","))
		b.WriteString("\n")
		signed = append(signed, k)
	}
	return b.String(), strings.Join(signed, ";")
}

func awsURLEncode(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if shouldEscape(c) {
			_, err := fmt.Fprintf(&b, "%%%02X", c)
			if err != nil {
				log.Errorf("failed to encode url: %s", err)
			}
		} else {
			b.WriteByte(c)
		}
	}
	return b.String()
}

func shouldEscape(c byte) bool {
	if 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || '0' <= c && c <= '9' {
		return false
	}

	switch c {
	case '-', '_', '.', '~':
		return false
	}
	return true
}

func sha256Hex(s string) string {
	h := sha256.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func hmacSHA256Hex(key, data []byte) string {
	mac := hmacSHA256(key, data)
	return hex.EncodeToString(mac)
}
