package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ──────────────────────────────────────────────────────────────────────────────
// SigV4 Validator  (incoming client → router)
// ──────────────────────────────────────────────────────────────────────────────

// ValidateRequest checks that the incoming request carries a valid AWS
// Signature Version 4 produced with the router's own access/secret key pair.
// If the request has no Authorization header it is rejected (we do not support
// anonymous access in the default configuration).
func ValidateRequest(r *http.Request, accessKey, secretKey string) error {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		if r.URL.Query().Get("X-Amz-Signature") != "" {
			return validateQueryRequest(r, accessKey, secretKey)
		}
		return fmt.Errorf("missing Authorization header")
	}

	// Parse the credential scope from the Authorization header.
	// Expected format (abbreviated):
	//   AWS4-HMAC-SHA256 Credential=KEY/DATE/REGION/s3/aws4_request,
	//   SignedHeaders=..., Signature=SIG
	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || parts[0] != "AWS4-HMAC-SHA256" {
		return fmt.Errorf("unsupported authorization scheme %q", parts[0])
	}

	fields := parseAuthFields(parts[1])
	credParts := strings.SplitN(fields["Credential"], "/", 2)
	if len(credParts) < 1 {
		return fmt.Errorf("malformed Credential in Authorization")
	}
	if credParts[0] != accessKey {
		return fmt.Errorf("unknown access key %q", credParts[0])
	}

	// Re-compute the signature and compare.
	// We reconstruct the canonical request from the actual HTTP request.
	signedHeaders := strings.Split(fields["SignedHeaders"], ";")
	amzDate := r.Header.Get("x-amz-date")
	if amzDate == "" {
		return fmt.Errorf("missing x-amz-date header")
	}
	t, err := time.Parse("20060102T150405Z", amzDate)
	if err != nil {
		return fmt.Errorf("invalid x-amz-date: %w", err)
	}

	// Read the body hash (or the header value if already pre-computed).
	payloadHash := r.Header.Get("x-amz-content-sha256")
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}

	credScope := strings.Join(strings.Split(fields["Credential"], "/")[1:], "/")
	region := strings.Split(credScope, "/")[1]

	canonical := buildCanonicalRequest(r, signedHeaders, payloadHash)
	stringToSign := buildStringToSign(t, credScope, canonical)
	sig := computeSignature(secretKey, t.Format("20060102"), region, "s3", stringToSign)

	if !hmac.Equal([]byte(sig), []byte(fields["Signature"])) {
		if matchedHost, matchedAE, _, _, ok := tryResolveHostSignature(r, signedHeaders, payloadHash, credScope, region, secretKey, fields["Signature"], t); ok {
			r.Host = matchedHost
			if matchedAE != "" {
				r.Header.Set("Accept-Encoding", matchedAE)
			} else {
				r.Header.Del("Accept-Encoding")
			}
			return nil
		}
		log.Printf("[signer] signature mismatch!\nCalculated: %s\nClient:     %s\nStringToSign:\n%s\nCanonicalRequest:\n%s",
			sig, fields["Signature"], stringToSign, canonical)
		return fmt.Errorf("signature mismatch")
	}
	return nil
}

func validateQueryRequest(r *http.Request, accessKey, secretKey string) error {
	q := r.URL.Query()
	sig := q.Get("X-Amz-Signature")
	algo := q.Get("X-Amz-Algorithm")
	cred := q.Get("X-Amz-Credential")
	amzDate := q.Get("X-Amz-Date")
	signedHeadersStr := q.Get("X-Amz-SignedHeaders")

	if algo != "AWS4-HMAC-SHA256" {
		return fmt.Errorf("unsupported algorithm %q", algo)
	}

	credParts := strings.Split(cred, "/")
	if len(credParts) < 5 {
		return fmt.Errorf("malformed X-Amz-Credential")
	}
	if credParts[0] != accessKey {
		return fmt.Errorf("unknown access key %q", credParts[0])
	}

	t, err := time.Parse("20060102T150405Z", amzDate)
	if err != nil {
		return fmt.Errorf("invalid X-Amz-Date: %w", err)
	}

	payloadHash := "UNSIGNED-PAYLOAD"
	credScope := strings.Join(credParts[1:], "/")
	region := credParts[2]

	signedHeaders := strings.Split(signedHeadersStr, ";")

	canonical := buildCanonicalRequest(r, signedHeaders, payloadHash)
	stringToSign := buildStringToSign(t, credScope, canonical)
	computedSig := computeSignature(secretKey, t.Format("20060102"), region, "s3", stringToSign)

	if !hmac.Equal([]byte(computedSig), []byte(sig)) {
		if matchedHost, matchedAE, _, _, ok := tryResolveHostSignature(r, signedHeaders, payloadHash, credScope, region, secretKey, sig, t); ok {
			r.Host = matchedHost
			if matchedAE != "" {
				r.Header.Set("Accept-Encoding", matchedAE)
			} else {
				r.Header.Del("Accept-Encoding")
			}
			return nil
		}
		return fmt.Errorf("signature mismatch")
	}
	return nil
}

func parseAuthFields(s string) map[string]string {
	result := make(map[string]string)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if idx := strings.IndexByte(part, '='); idx >= 0 {
			result[strings.TrimSpace(part[:idx])] = strings.TrimSpace(part[idx+1:])
		}
	}
	return result
}

// ──────────────────────────────────────────────────────────────────────────────
// SigV4 Resigner  (router → backend)
// ──────────────────────────────────────────────────────────────────────────────

// ResignRequest strips the original client signature from r, sets the Host
// header to the backend endpoint, and applies a fresh SigV4 signature using
// the backend's credentials.
func ResignRequest(r *http.Request, backend *Backend, payloadHash string) error {
	// Remove all headers that were part of the original client signature so
	// we start from a clean slate.
	r.Header.Del("Authorization")
	r.Header.Del("x-amz-security-token")

	now := time.Now().UTC()
	dateStr := now.Format("20060102")
	dateTimeStr := now.Format("20060102T150405Z")

	r.Header.Set("x-amz-date", dateTimeStr)
	if payloadHash != "" && payloadHash != "UNSIGNED-PAYLOAD" {
		r.Header.Set("x-amz-content-sha256", payloadHash)
	} else if r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodDelete || r.ContentLength == 0 {
		payloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
		r.Header.Set("x-amz-content-sha256", payloadHash)
	} else {
		r.Header.Set("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
		payloadHash = "UNSIGNED-PAYLOAD"
	}

	// The Host header must reflect the backend endpoint.
	// In Go's http.Request, the Host field takes precedence over Header["Host"];
	// we set both to be safe.
	r.Host = r.URL.Host
	r.Header.Del("Host") // net/http sets Host from r.Host automatically

	region := backend.Config.Region
	credScope := fmt.Sprintf("%s/%s/s3/aws4_request", dateStr, region)

	// Determine which headers to sign (host + all x-amz-* headers).
	signedHeaders := collectSignedHeaders(r)
	headerList := strings.Join(signedHeaders, ";")

	canonical := buildCanonicalRequest(r, signedHeaders, payloadHash)
	stringToSign := buildStringToSign(now, credScope, canonical)
	sig := computeSignature(backend.Config.SecretKey, dateStr, region, "s3", stringToSign)

	r.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		backend.Config.AccessKey, credScope, headerList, sig,
	))
	return nil
}

// ──────────────────────────────────────────────────────────────────────────────
// Canonical request construction helpers
// ──────────────────────────────────────────────────────────────────────────────

func buildCanonicalRequest(r *http.Request, signedHeaders []string, payloadHash string) string {
	method := r.Method
	path := r.URL.Path
	if r.URL.RawPath != "" {
		path = r.URL.RawPath
	}
	canonicalURI := canonicalizeURI(path)
	canonicalQuery := canonicalizeQuery(r.URL.RawQuery)
	canonicalHeaders := canonicalizeHeaders(r, signedHeaders)
	joinedHeaders := strings.Join(signedHeaders, ";")

	return strings.Join([]string{
		method,
		canonicalURI,
		canonicalQuery,
		canonicalHeaders,
		joinedHeaders,
		payloadHash,
	}, "\n")
}

func buildStringToSign(t time.Time, credScope, canonicalRequest string) string {
	hash := sha256.Sum256([]byte(canonicalRequest))
	return strings.Join([]string{
		"AWS4-HMAC-SHA256",
		t.Format("20060102T150405Z"),
		credScope,
		hex.EncodeToString(hash[:]),
	}, "\n")
}

func computeSignature(secretKey, dateStr, region, service, stringToSign string) string {
	kDate := hmacSHA256([]byte("AWS4"+secretKey), dateStr)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	kSigning := hmacSHA256(kService, "aws4_request")
	return hex.EncodeToString(hmacSHA256(kSigning, stringToSign))
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}


func escapeRFC3986(s string, encodeSlash bool) string {
	var sb strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '~' || c == '.' {
			sb.WriteByte(c)
		} else if c == '/' && !encodeSlash {
			sb.WriteByte(c)
		} else {
			sb.WriteString(fmt.Sprintf("%%%02X", c))
		}
	}
	return sb.String()
}

func canonicalizeURI(path string) string {
	if path == "" || path == "/" {
		return "/"
	}
	segments := strings.Split(path, "/")
	for i, seg := range segments {
		if unescaped, err := url.PathUnescape(seg); err == nil {
			seg = unescaped
		}
		segments[i] = escapeRFC3986(seg, true)
	}
	result := strings.Join(segments, "/")
	if !strings.HasPrefix(result, "/") {
		result = "/" + result
	}
	return result
}

type queryParam struct {
	key string
	val string
}

func canonicalizeQuery(rawQuery string) string {
	if rawQuery == "" {
		return ""
	}
	pairs := strings.Split(rawQuery, "&")
	var params []queryParam
	for _, pair := range pairs {
		if pair == "" {
			continue
		}
		k, v, _ := strings.Cut(pair, "=")
		if strings.EqualFold(k, "X-Amz-Signature") {
			continue
		}
		if uk, err := url.QueryUnescape(k); err == nil {
			k = uk
		}
		if uv, err := url.QueryUnescape(v); err == nil {
			v = uv
		}
		params = append(params, queryParam{
			key: escapeRFC3986(k, true),
			val: escapeRFC3986(v, true),
		})
	}
	sort.Slice(params, func(i, j int) bool {
		if params[i].key == params[j].key {
			return params[i].val < params[j].val
		}
		return params[i].key < params[j].key
	})
	var parts []string
	for _, p := range params {
		parts = append(parts, p.key+"="+p.val)
	}
	return strings.Join(parts, "&")
}

func canonicalizeHeaders(r *http.Request, signedHeaders []string) string {
	var sb strings.Builder
	for _, h := range signedHeaders {
		var val string
		if h == "host" {
			// r.Host takes precedence over r.Header["Host"] in Go.
			val = r.Host
			if val == "" {
				val = r.URL.Host
			}
		} else {
			val = strings.TrimSpace(r.Header.Get(h))
		}
		sb.WriteString(h)
		sb.WriteByte(':')
		sb.WriteString(val)
		sb.WriteByte('\n')
	}
	return sb.String()
}

func collectSignedHeaders(r *http.Request) []string {
	headers := []string{"host"}
	for k := range r.Header {
		lower := strings.ToLower(k)
		if strings.HasPrefix(lower, "x-amz-") || lower == "content-type" || lower == "content-md5" || lower == "range" {
			headers = append(headers, lower)
		}
	}
	sort.Strings(headers)
	return headers
}

func tryResolveHostSignature(r *http.Request, signedHeaders []string, payloadHash, credScope, region, secretKey, expectedSig string, t time.Time) (string, string, string, string, bool) {
	origHost := r.Host
	origAE := r.Header.Get("Accept-Encoding")
	defer func() {
		r.Host = origHost
		if origAE != "" {
			r.Header.Set("Accept-Encoding", origAE)
		} else {
			r.Header.Del("Accept-Encoding")
		}
	}()

	var aeCandidates []string
	hasAE := false
	for _, h := range signedHeaders {
		if h == "accept-encoding" {
			hasAE = true
			break
		}
	}
	if hasAE {
		seen := map[string]bool{}
		for _, c := range []string{origAE, "identity", "gzip", "gzip, br", ""} {
			if !seen[c] {
				seen[c] = true
				aeCandidates = append(aeCandidates, c)
			}
		}
	} else {
		aeCandidates = []string{origAE}
	}

	for _, hostCandidate := range generateHostCandidates(r) {
		r.Host = hostCandidate
		for _, aeCandidate := range aeCandidates {
			if hasAE {
				if aeCandidate != "" {
					r.Header.Set("Accept-Encoding", aeCandidate)
				} else {
					r.Header.Del("Accept-Encoding")
				}
			}
			canonical := buildCanonicalRequest(r, signedHeaders, payloadHash)
			stringToSign := buildStringToSign(t, credScope, canonical)
			sig := computeSignature(secretKey, t.Format("20060102"), region, "s3", stringToSign)
			if hmac.Equal([]byte(sig), []byte(expectedSig)) {
				return hostCandidate, aeCandidate, canonical, stringToSign, true
			}
		}
	}
	return "", "", "", "", false
}

func generateHostCandidates(r *http.Request) []string {
	seen := make(map[string]bool)
	var list []string
	add := func(h string) {
		h = strings.TrimSpace(h)
		if h != "" && !seen[h] {
			seen[h] = true
			list = append(list, h)
		}
	}

	add(r.Host)
	add(r.Header.Get("Host"))

	if xfh := r.Header.Get("X-Forwarded-Host"); xfh != "" {
		for _, part := range strings.Split(xfh, ",") {
			add(part)
		}
	}

	baseList := append([]string{}, list...)
	xfp := r.Header.Get("X-Forwarded-Port")

	for _, h := range baseList {
		hostOnly := h
		if idx := strings.IndexByte(h, ':'); idx != -1 {
			hostOnly = h[:idx]
		}
		add(hostOnly)
		add(hostOnly + ":443")
		add(hostOnly + ":80")
		add(hostOnly + ":8080")
		if xfp != "" {
			add(hostOnly + ":" + strings.TrimSpace(xfp))
		}
	}
	return list
}

// GeneratePresignedURL returns a Pre-signed URL for the given object on the
// specified backend. It uses the AWS SDK's built-in presigner.
func GeneratePresignedURL(ctx context.Context, b *Backend, objectKey string, ttl time.Duration) (string, error) {
	presigner := s3.NewPresignClient(b.Client)
	req, err := presigner.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.Config.Bucket),
		Key:    aws.String(objectKey),
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return "", fmt.Errorf("presign %s/%s: %w", b.Config.Name, objectKey, err)
	}
	log.Printf("[signer] presigned URL for %s/%s (ttl=%s)", b.Config.Name, objectKey, ttl)
	return req.URL, nil
}
