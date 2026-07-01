package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// BackendStatus represents the current health state of a backend.
type BackendStatus int32

const (
	StatusHealthy   BackendStatus = 0
	StatusUnhealthy BackendStatus = 1
)

// Backend wraps a BackendConfig with a live S3 client and health state.
type Backend struct {
	Config  BackendConfig
	Client  *s3.Client
	status  atomic.Int32 // BackendStatus (0=healthy, 1=unhealthy)
	latency atomic.Int64 // latest RTT in nanoseconds
}

func (b *Backend) Healthy() bool {
	return BackendStatus(b.status.Load()) == StatusHealthy
}

func (b *Backend) SetHealthy(h bool) {
	if h {
		b.status.Store(int32(StatusHealthy))
	} else {
		b.status.Store(int32(StatusUnhealthy))
	}
}

// LatencyNs returns the most recently measured round-trip time.
func (b *Backend) LatencyNs() int64 {
	return b.latency.Load()
}

// HealthMonitor periodically checks all backends and updates their status.
type HealthMonitor struct {
	backends []*Backend
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewHealthMonitor creates a HealthMonitor for the given backends.
func NewHealthMonitor(backends []*Backend) *HealthMonitor {
	return &HealthMonitor{
		backends: backends,
		stopCh:   make(chan struct{}),
	}
}

// Start launches background health-check goroutines (one per backend).
func (hm *HealthMonitor) Start() {
	for _, b := range hm.backends {
		b := b
		hm.wg.Add(1)
		go func() {
			defer hm.wg.Done()
			hm.runLoop(b)
		}()
	}
	log.Printf("[health] monitor started for %d backend(s)", len(hm.backends))
}

// Stop signals all goroutines to exit and waits for them.
func (hm *HealthMonitor) Stop() {
	close(hm.stopCh)
	hm.wg.Wait()
}

func (hm *HealthMonitor) runLoop(b *Backend) {
	// Run the first check immediately so the router starts with fresh data.
	hm.probe(b)

	ticker := time.NewTicker(b.Config.HealthCheck.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hm.probe(b)
		case <-hm.stopCh:
			return
		}
	}
}

// probe performs a single HEAD request to the canary object and updates
// the backend's status and measured latency.
func (hm *HealthMonitor) probe(b *Backend) {
	ctx, cancel := context.WithTimeout(context.Background(), b.Config.HealthCheck.Timeout)
	defer cancel()

	start := time.Now()
	_, err := b.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(b.Config.Bucket),
		Key:    aws.String(b.Config.HealthCheck.ObjectKey),
	})
	rtt := time.Since(start)

	// A 404 (NoSuchKey) still means the backend is reachable.
	reachable := err == nil || isNotFound(err)

	b.SetHealthy(reachable)
	if reachable {
		b.latency.Store(rtt.Nanoseconds())
	}

	if reachable {
		log.Printf("[health] %s  OK  (latency=%s)", b.Config.Name, rtt.Round(time.Millisecond))
	} else {
		log.Printf("[health] %s  FAIL  (%v)", b.Config.Name, err)
	}
}

// isNotFound returns true for AWS S3 NoSuchKey / 404 errors.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	type httpStatusCoder interface{ HTTPStatusCode() int }
	if e, ok := err.(httpStatusCoder); ok {
		return e.HTTPStatusCode() == http.StatusNotFound
	}
	return false
}

// EnsureCanaryObject uploads a tiny placeholder object so HEAD probes work
// even on a freshly-created bucket.
func EnsureCanaryObject(ctx context.Context, b *Backend) {
	key := b.Config.HealthCheck.ObjectKey
	_, err := b.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(b.Config.Bucket),
		Key:    aws.String(key),
	})
	if err == nil {
		return // already exists
	}
	if !isNotFound(err) {
		log.Printf("[health] cannot check canary on %s: %v", b.Config.Name, err)
		return
	}
	// Upload the canary.
	_, putErr := b.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(b.Config.Bucket),
		Key:           aws.String(key),
		Body:          io.NopCloser(io.LimitReader(zeroReader{}, 1)),
		ContentLength: aws.Int64(1),
	})
	if putErr != nil {
		log.Printf("[health] cannot create canary on %s: %v", b.Config.Name, putErr)
	} else {
		log.Printf("[health] canary created on %s (%s)", b.Config.Name, key)
	}
}

// zeroReader is an io.Reader that emits a single zero byte.
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	p[0] = 0
	return 1, io.EOF
}

// newS3Client builds an S3 client for the given BackendConfig.
func newS3Client(cfg BackendConfig) (*s3.Client, error) {
	creds := aws.NewCredentialsCache(
		credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
	)

	endpointURL, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("invalid endpoint %q: %w", cfg.Endpoint, err)
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithCredentialsProvider(creds),
		awsconfig.WithRegion(cfg.Region),
	)
	if err != nil {
		return nil, fmt.Errorf("cannot load AWS config for %q: %w", cfg.Name, err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpointURL.String())
		o.UsePathStyle = cfg.PathStyle
		o.DisableLogOutputChecksumValidationSkipped = true
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		o.APIOptions = append(o.APIOptions, v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware)
	})
	return client, nil
}

// BuildBackends creates Backend instances from a slice of BackendConfigs.
func BuildBackends(cfgs []BackendConfig) ([]*Backend, error) {
	backends := make([]*Backend, 0, len(cfgs))
	for _, c := range cfgs {
		client, err := newS3Client(c)
		if err != nil {
			return nil, fmt.Errorf("cannot create S3 client for %q: %w", c.Name, err)
		}
		b := &Backend{Config: c, Client: client}
		b.SetHealthy(true) // optimistic until first probe
		backends = append(backends, b)
	}
	return backends, nil
}
