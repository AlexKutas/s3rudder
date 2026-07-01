package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
)

// uuidNew is an alias kept for readability.
var uuidNew = uuid.New

// Router is the central HTTP handler. It owns the backend pool, health
// monitor, replication queue, and routing configuration.
type Router struct {
	cfg      *Config
	backends []*Backend
	health   *HealthMonitor
	queue    *Queue
	worker   *ReplicationWorker
}

// NewRouter wires all components together and returns a ready-to-serve Router.
func NewRouter(cfg *Config, backends []*Backend, queue *Queue) *Router {
	health := NewHealthMonitor(backends)
	worker := NewReplicationWorker(queue, backends)
	return &Router{
		cfg:      cfg,
		backends: backends,
		health:   health,
		queue:    queue,
		worker:   worker,
	}
}

// Start launches background goroutines (health monitor, replication workers, periodic sync, periodic cleanup).
func (rt *Router) Start(ctx context.Context) {
	// Ensure canary objects exist on every backend before probing starts.
	for _, b := range rt.backends {
		EnsureCanaryObject(ctx, b)
	}
	rt.health.Start()
	rt.worker.Start(rt.cfg.Queue.Workers)

	if rt.cfg.Routing.SyncInterval > 0 {
		go rt.runPeriodicSync(ctx)
	}
	if rt.cfg.Routing.CleanupInterval > 0 {
		go rt.runPeriodicCleanup(ctx)
	}
}

// Stop shuts down background goroutines gracefully.
func (rt *Router) Stop() {
	rt.health.Stop()
	rt.worker.Stop()
}

func (rt *Router) runPeriodicSync(ctx context.Context) {
	log.Printf("[sync] periodic background sync scheduled every %s", rt.cfg.Routing.SyncInterval)
	ticker := time.NewTicker(rt.cfg.Routing.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Println("[sync] starting automatic periodic background reconciliation...")
			if len(rt.backends) < 2 {
				log.Println("[sync] periodic sync skipped: fewer than 2 backends configured")
				continue
			}
			src := rt.backends[0]
			for _, dst := range rt.backends[1:] {
				if !src.Healthy() || !dst.Healthy() {
					log.Printf("[sync] periodic sync skipped for %s -> %s because one of them is unhealthy", src.Config.Name, dst.Config.Name)
					continue
				}
				_, err := SyncBackends(ctx, src, dst, false)
				if err != nil {
					log.Printf("[sync] periodic sync error to %s: %v", dst.Config.Name, err)
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (rt *Router) runPeriodicCleanup(ctx context.Context) {
	log.Printf("[sync] periodic background cleanup (orphan deletion) scheduled every %s", rt.cfg.Routing.CleanupInterval)
	ticker := time.NewTicker(rt.cfg.Routing.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Println("[sync] starting automatic periodic background cleanup (reconciliation with orphan deletion)...")
			if len(rt.backends) < 2 {
				log.Println("[sync] periodic cleanup skipped: fewer than 2 backends configured")
				continue
			}
			src := rt.backends[0]
			for _, dst := range rt.backends[1:] {
				if !src.Healthy() || !dst.Healthy() {
					log.Printf("[sync] periodic cleanup skipped for %s -> %s because one of them is unhealthy", src.Config.Name, dst.Config.Name)
					continue
				}
				_, err := SyncBackends(ctx, src, dst, true) // pass deleteOrphans = true
				if err != nil {
					log.Printf("[sync] periodic cleanup error to %s: %v", dst.Config.Name, err)
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

// ServeHTTP is the main entry point for all incoming S3-protocol requests.
func (rt *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// ── 0. Parse S3 path ──────────────────────────────────────────────────
	bucket, objectKey := parseS3Path(r)

	// ── 1. Health-check / ListBuckets shortcut ─────────────────────────────
	// Allow unauthenticated GET / or HEAD / so load-balancers can verify the
	// router is alive, but authenticate and return bucket list if auth is sent.
	if bucket == "" && r.URL.Path == "/" && (r.Method == http.MethodGet || r.Method == http.MethodHead) {
		hasAuth := r.Header.Get("Authorization") != "" || r.URL.Query().Get("X-Amz-Signature") != ""
		if hasAuth {
			if err := ValidateRequest(r, rt.cfg.Server.AccessKey, rt.cfg.Server.SecretKey); err != nil {
				log.Printf("[proxy] auth error on ListBuckets: %v", err)
				writeS3Error(w, http.StatusForbidden, "AccessDenied", err.Error())
				return
			}
			rt.handleListBuckets(w, r)
		} else {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			if r.Method == http.MethodGet {
				fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><ListAllMyBucketsResult></ListAllMyBucketsResult>`)
			}
		}
		return
	}

	if bucket == "" {
		writeS3Error(w, http.StatusBadRequest, "InvalidRequest", "cannot determine bucket from request")
		return
	}

	// ── 2. Authenticate ───────────────────────────────────────────────────
	if err := ValidateRequest(r, rt.cfg.Server.AccessKey, rt.cfg.Server.SecretKey); err != nil {
		log.Printf("[proxy] auth error: %v", err)
		writeS3Error(w, http.StatusForbidden, "AccessDenied", err.Error())
		return
	}

	log.Printf("[proxy] %s /%s/%s  mode=%s", r.Method, bucket, objectKey, rt.cfg.Routing.ReadMode)

	// ── 3. Route by method ────────────────────────────────────────────────
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		// Bucket-level operations (ListObjects, GetBucketLocation, etc.)
		// have no object key — forward them directly to the backend.
		if objectKey == "" {
			rt.handlePassthrough(w, r)
			return
		}
		rt.handleRead(w, r, objectKey)
	case http.MethodPut:
		if objectKey == "" {
			// CreateBucket or bucket-level PUT — forward to primary backend.
			rt.handlePassthrough(w, r)
			return
		}
		rt.handleWrite(w, r, objectKey)
	case http.MethodDelete:
		rt.handleDelete(w, r, objectKey)
	default:
		// All other S3 operations (CreateMultipartUpload, CompleteMultipart,
		// ListObjectsV2, GetBucketVersioning, etc.) forwarded transparently.
		rt.handlePassthrough(w, r)
	}
}

// ── Read (GET / HEAD) ─────────────────────────────────────────────────────────

func (rt *Router) handleRead(w http.ResponseWriter, r *http.Request, objectKey string) {
	candidates := SelectReadBackends(rt.backends, rt.cfg.Routing.ReadPolicy)
	if len(candidates) == 0 {
		writeS3Error(w, http.StatusServiceUnavailable, "ServiceUnavailable", "no healthy backends")
		return
	}

	// redirect mode: generate a Pre-signed URL and return HTTP 302.
	if rt.cfg.Routing.ReadMode == "redirect" {
		for _, b := range candidates {
			purl, err := GeneratePresignedURL(r.Context(), b, objectKey, rt.cfg.Routing.RedirectTTL)
			if err != nil {
				log.Printf("[proxy] presign error on %s: %v", b.Config.Name, err)
				continue
			}
			http.Redirect(w, r, purl, http.StatusFound)
			return
		}
		writeS3Error(w, http.StatusServiceUnavailable, "ServiceUnavailable", "could not generate pre-signed URL")
		return
	}

	// proxy mode: stream the body through the router with failover.
	for _, b := range candidates {
		err := rt.proxyRead(w, r, b, objectKey)
		if err == nil {
			return // success
		}
		log.Printf("[proxy] read failed on %s, trying next backend: %v", b.Config.Name, err)
	}
	writeS3Error(w, http.StatusBadGateway, "ServiceUnavailable", "all backends failed")
}

// proxyRead forwards a GET/HEAD to the specified backend and streams the
// response back to the client. Returns an error when the backend responds
// with 5xx or a network error (so the caller can try the next candidate).
func (rt *Router) proxyRead(w http.ResponseWriter, r *http.Request, b *Backend, objectKey string) error {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	if r.Method == http.MethodHead {
		out, err := b.Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(b.Config.Bucket),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			return err
		}
		if out.ContentLength != nil {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", *out.ContentLength))
		}
		if out.ContentType != nil {
			w.Header().Set("Content-Type", *out.ContentType)
		}
		if out.ETag != nil {
			w.Header().Set("ETag", *out.ETag)
		}
		w.WriteHeader(http.StatusOK)
		return nil
	}

	// GET — stream the body.
	out, err := b.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.Config.Bucket),
		Key:    aws.String(objectKey),
		Range:  aws.String(r.Header.Get("Range")),
	})
	if err != nil {
		return err
	}
	defer out.Body.Close()

	if out.ContentLength != nil {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", *out.ContentLength))
	}
	if out.ContentType != nil {
		w.Header().Set("Content-Type", *out.ContentType)
	}
	if out.ETag != nil {
		w.Header().Set("ETag", *out.ETag)
	}
	if out.LastModified != nil {
		w.Header().Set("Last-Modified", out.LastModified.Format(http.TimeFormat))
	}

	statusCode := http.StatusOK
	if r.Header.Get("Range") != "" {
		statusCode = http.StatusPartialContent
	}
	w.WriteHeader(statusCode)
	_, _ = io.Copy(w, out.Body)
	return nil
}

// ── Write (PUT) ───────────────────────────────────────────────────────────────

func (rt *Router) handleWrite(w http.ResponseWriter, r *http.Request, objectKey string) {
	primary := SelectWriteBackend(rt.backends)
	if primary == nil {
		writeS3Error(w, http.StatusServiceUnavailable, "ServiceUnavailable", "no healthy backends")
		return
	}

	// Forward the PUT directly via HTTP rather than through the AWS SDK.
	// The SDK's PutObject requires a seekable body to compute checksums when
	// the connection is plain HTTP (no TLS). Forwarding via raw HTTP avoids
	// this constraint and streams the body without buffering.
	resp, err := rt.forwardHTTPToBackend(r, primary, objectKey)
	if err != nil {
		log.Printf("[proxy] write error on %s: %v", primary.Config.Name, err)
		writeS3Error(w, http.StatusBadGateway, "InternalError", err.Error())
		return
	}
	defer resp.Body.Close()

	// Relay response headers and status to the client.
	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)

	if resp.StatusCode >= 300 {
		// Non-2xx from backend — don't enqueue replication.
		return
	}

	// Enqueue async replication to all other backends.
	if rt.cfg.Routing.WritePolicy == "weight_async" {
		for _, b := range rt.backends {
			if b.Config.Name == primary.Config.Name {
				continue
			}
			task := &ReplicationTask{
				ID:         uuidNew().String(),
				Kind:       TaskCopy,
				SrcBackend: primary.Config.Name,
				DstBackend: b.Config.Name,
				ObjectKey:  objectKey,
				CreatedAt:  time.Now(),
			}
			if enqErr := rt.queue.Enqueue(task); enqErr != nil {
				log.Printf("[proxy] failed to enqueue replication to %s: %v", b.Config.Name, enqErr)
			}
		}
	}
}

// ── Delete ────────────────────────────────────────────────────────────────────

func (rt *Router) handleDelete(w http.ResponseWriter, r *http.Request, objectKey string) {
	var lastErr error
	for _, b := range rt.backends {
		if !b.Healthy() {
			continue
		}
		inp := deleteObjectInput(b, objectKey)
		_, err := b.Client.DeleteObject(r.Context(), &inp)
		if err != nil {
			log.Printf("[proxy] delete error on %s: %v", b.Config.Name, err)
			lastErr = err
		}
	}
	if lastErr != nil {
		writeS3Error(w, http.StatusInternalServerError, "InternalError", lastErr.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ── Passthrough (ListObjects, CreateMultipartUpload, etc.) ────────────────────

func (rt *Router) handlePassthrough(w http.ResponseWriter, r *http.Request) {
	candidates := SelectReadBackends(rt.backends, "failover")
	if len(candidates) == 0 {
		writeS3Error(w, http.StatusServiceUnavailable, "ServiceUnavailable", "no healthy backends")
		return
	}
	b := candidates[0]
	log.Printf("[proxy] passthrough %s %s → %s", r.Method, r.URL.RequestURI(), b.Config.Name)

	_, objectKey := parseS3Path(r)
	resp, err := rt.forwardHTTPToBackend(r, b, objectKey)
	if err != nil {
		log.Printf("[proxy] passthrough forward error on %s: %v", b.Config.Name, err)
		writeS3Error(w, http.StatusBadGateway, "ServiceUnavailable", err.Error())
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	log.Printf("[proxy] passthrough response from %s: status=%d body=%s", b.Config.Name, resp.StatusCode, string(body))

	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(body)
}


// forwardHTTP clones the incoming request, re-points it at the backend,
// re-signs it, and streams the response to w.
func (rt *Router) forwardHTTP(w http.ResponseWriter, r *http.Request, b *Backend) error {
	_, objectKey := parseS3Path(r)
	resp, err := rt.forwardHTTPToBackend(r, b, objectKey)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
	return nil
}

// forwardHTTPToBackend builds and executes a re-signed HTTP request to the
// given backend. It returns the raw *http.Response so the caller can decide
// how to relay headers/body (used for both passthrough and PUT writes).
// If objectKey is non-empty the URL path is rewritten to /<bucket>/<key>.
func (rt *Router) forwardHTTPToBackend(r *http.Request, b *Backend, objectKey string) (*http.Response, error) {
	outReq := r.Clone(r.Context())

	// Determine scheme from the backend endpoint.
	backendURL := strings.TrimRight(b.Config.Endpoint, "/")
	if strings.HasPrefix(backendURL, "https://") {
		outReq.URL.Scheme = "https"
		outReq.URL.Host = strings.TrimPrefix(backendURL, "https://")
	} else {
		outReq.URL.Scheme = "http"
		outReq.URL.Host = strings.TrimPrefix(backendURL, "http://")
	}
	outReq.Host = outReq.URL.Host
	outReq.RequestURI = ""

	// Rewrite path and host correctly based on backend style.
	if b.Config.PathStyle {
		outReq.URL.Path = "/" + b.Config.Bucket
		if objectKey != "" {
			outReq.URL.Path += "/" + strings.TrimPrefix(objectKey, "/")
		}
	} else {
		// Virtual-hosted style: /<key>
		outReq.URL.Path = "/" + strings.TrimPrefix(objectKey, "/")
		// Prepend bucket name to host
		bucketHost := b.Config.Bucket + "." + outReq.URL.Host
		outReq.URL.Host = bucketHost
		outReq.Host = bucketHost
	}

	payloadHash := r.Header.Get("x-amz-content-sha256")
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}
	if err := ResignRequest(outReq, b, payloadHash); err != nil {
		return nil, fmt.Errorf("resign: %w", err)
	}

	client := &http.Client{Timeout: 5 * time.Minute}
	resp, err := client.Do(outReq)
	if err != nil {
		return nil, fmt.Errorf("forward: %w", err)
	}
	return resp, nil
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// parseS3Path extracts the bucket and object key from the request URL.
// Supports both virtual-host style (bucket.router.local/key) and
// path style (router.local/bucket/key).
func parseS3Path(r *http.Request) (bucket, objectKey string) {
	host := r.Host
	// Virtual-host style: the first label of the Host header is the bucket.
	// e.g.  my-bucket.s3rudder.local:8080 → bucket=my-bucket
	if idx := strings.Index(host, "."); idx > 0 {
		bucket = host[:idx]
		objectKey = strings.TrimPrefix(r.URL.Path, "/")
		return
	}
	// Path style: /bucket/key/...
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
	if len(parts) >= 1 {
		bucket = parts[0]
	}
	if len(parts) == 2 {
		objectKey = parts[1]
	}
	return
}

// writeS3Error writes an S3-compatible XML error response.
func writeS3Error(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>%s</Code>
  <Message>%s</Message>
</Error>`, code, message)
}

// handleListBuckets returns an S3-compatible list of configured buckets.
func (rt *Router) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	uniqueBuckets := make(map[string]bool)
	var bucketsList []string
	for _, b := range rt.backends {
		if b.Config.Bucket != "" && !uniqueBuckets[b.Config.Bucket] {
			uniqueBuckets[b.Config.Bucket] = true
			bucketsList = append(bucketsList, b.Config.Bucket)
		}
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}

	var sb strings.Builder
	sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
	sb.WriteString(`<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	sb.WriteString(`<Buckets>`)
	for _, bucketName := range bucketsList {
		sb.WriteString(fmt.Sprintf("<Bucket><Name>%s</Name><CreationDate>%s</CreationDate></Bucket>", 
			bucketName, time.Now().Format("2006-01-02T15:04:05.000Z")))
	}
	sb.WriteString(`</Buckets>`)
	sb.WriteString(`<Owner><ID>s3rudder-owner-id</ID><DisplayName>s3rudder</DisplayName></Owner>`)
	sb.WriteString(`</ListAllMyBucketsResult>`)
	fmt.Fprint(w, sb.String())
}
