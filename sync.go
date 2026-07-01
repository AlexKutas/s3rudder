package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ──────────────────────────────────────────────────────────────────────────────
// Backend selector
// ──────────────────────────────────────────────────────────────────────────────

// SelectWriteBackend picks a single healthy backend for a write operation
// using weighted random selection. Higher-weight backends are chosen more
// often, proportionally to their weight relative to the total weight sum of
// all currently healthy backends.
func SelectWriteBackend(backends []*Backend) *Backend {
	var pool []*Backend
	totalWeight := 0
	for _, b := range backends {
		if b.Healthy() {
			pool = append(pool, b)
			totalWeight += b.Config.Weight
		}
	}
	if len(pool) == 0 {
		return nil
	}

	r := rand.Intn(totalWeight)
	cumulative := 0
	for _, b := range pool {
		cumulative += b.Config.Weight
		if r < cumulative {
			return b
		}
	}
	return pool[len(pool)-1]
}

// SelectReadBackends filters backends and returns them sorted by routing policy.
func SelectReadBackends(backends []*Backend, policy string) []*Backend {
	var healthy []*Backend
	for _, b := range backends {
		if b.Healthy() {
			healthy = append(healthy, b)
		}
	}
	if len(healthy) == 0 {
		return nil
	}

	switch policy {
	case "round-robin":
		// Shuffle healthy backends.
		rand.Shuffle(len(healthy), func(i, j int) {
			healthy[i], healthy[j] = healthy[j], healthy[i]
		})
	case "latency":
		// Sort by measured latency (lowest first).
		sort.Slice(healthy, func(i, j int) bool {
			return healthy[i].LatencyNs() < healthy[j].LatencyNs()
		})
	default:
		// "failover" (default): sort by weight descending (higher weight first).
		sort.Slice(healthy, func(i, j int) bool {
			return healthy[i].Config.Weight > healthy[j].Config.Weight
		})
	}
	return healthy
}

// ──────────────────────────────────────────────────────────────────────────────
// Object copy helper (used by sync and replication worker)
// ──────────────────────────────────────────────────────────────────────────────

// copyObject streams an object from src to dst without buffering it fully
// in memory. It uses the standard AWS SDK S3 Manager Uploader to handle
// multipart streaming uploads for unseekable body readers.
func copyObject(ctx context.Context, src, dst *Backend, objectKey string) error {
	// Step 1: Get the object metadata from source (mainly for Content-Type).
	headOut, err := src.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(src.Config.Bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return fmt.Errorf("copyObject: head %s/%s: %w", src.Config.Name, objectKey, err)
	}

	// Step 2: Fetch the body from source.
	getOut, err := src.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(src.Config.Bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return fmt.Errorf("copyObject: get %s/%s: %w", src.Config.Name, objectKey, err)
	}
	defer getOut.Body.Close()

	// Step 3: Upload to destination using S3 Uploader.
	uploader := manager.NewUploader(dst.Client)
	_, putErr := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(dst.Config.Bucket),
		Key:         aws.String(objectKey),
		Body:        getOut.Body,
		ContentType: headOut.ContentType,
	})

	if putErr != nil {
		return fmt.Errorf("copyObject: upload %s/%s: %w", dst.Config.Name, objectKey, putErr)
	}

	log.Printf("[sync] copied %s → %s  key=%s", src.Config.Name, dst.Config.Name, objectKey)
	return nil
}

// deleteObjectInput builds the DeleteObjectInput for a backend.
func deleteObjectInput(b *Backend, objectKey string) s3.DeleteObjectInput {
	return s3.DeleteObjectInput{
		Bucket: aws.String(b.Config.Bucket),
		Key:    aws.String(objectKey),
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Full reconciliation sync  (used by CLI `s3rudder sync`)
// ──────────────────────────────────────────────────────────────────────────────

// SyncStats tracks objects compared, copied and failed during a sync run.
type SyncStats struct {
	Compared int
	Copied   int
	Deleted  int
	Failed   int
}

// SyncBackends performs a full listing-based reconciliation from src to dst.
// For each object in src that is missing from dst, or has a different ETag
// or size, it copies the object. Deletion of objects removed from src is
// optional (controlled by the deleteOrphans flag).
func SyncBackends(ctx context.Context, src, dst *Backend, deleteOrphans bool) (*SyncStats, error) {
	log.Printf("[sync] starting full sync  %s → %s", src.Config.Name, dst.Config.Name)

	srcObjects, err := listAllObjects(ctx, src)
	if err != nil {
		return nil, fmt.Errorf("sync: list src %s: %w", src.Config.Name, err)
	}

	dstObjects, err := listAllObjects(ctx, dst)
	if err != nil {
		return nil, fmt.Errorf("sync: list dst %s: %w", dst.Config.Name, err)
	}

	stats := &SyncStats{}

	// Copy missing or stale objects from src → dst, skipping health check canaries.
	for key, srcMeta := range srcObjects {
		if key == src.Config.HealthCheck.ObjectKey || key == dst.Config.HealthCheck.ObjectKey {
			continue
		}
		stats.Compared++
		dstMeta, exists := dstObjects[key]
		if !exists || dstMeta.etag != srcMeta.etag || dstMeta.size != srcMeta.size {
			if err := copyObject(ctx, src, dst, key); err != nil {
				log.Printf("[sync] FAILED copy %s: %v", key, err)
				stats.Failed++
			} else {
				stats.Copied++
			}
		}
	}

	// Optionally delete objects in dst that no longer exist in src.
	if deleteOrphans {
		for key := range dstObjects {
			if key == src.Config.HealthCheck.ObjectKey || key == dst.Config.HealthCheck.ObjectKey {
				continue
			}
			if _, exists := srcObjects[key]; !exists {
				inp := deleteObjectInput(dst, key)
				if _, err := dst.Client.DeleteObject(ctx, &inp); err != nil {
					log.Printf("[sync] FAILED delete orphan %s on %s: %v", key, dst.Config.Name, err)
					stats.Failed++
				} else {
					log.Printf("[sync] deleted orphan %s on %s", key, dst.Config.Name)
					stats.Deleted++
				}
			}
		}
	}

	log.Printf("[sync] done  compared=%d copied=%d deleted=%d failed=%d",
		stats.Compared, stats.Copied, stats.Deleted, stats.Failed)
	return stats, nil
}

type objectMeta struct {
	etag string
	size int64
}

// listAllObjects returns a map of object key → metadata for all objects in
// the backend's bucket, handling S3 pagination transparently.
func listAllObjects(ctx context.Context, b *Backend) (map[string]objectMeta, error) {
	result := make(map[string]objectMeta)
	var continuationToken *string

	for {
		out, err := b.Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(b.Config.Bucket),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, err
		}
		for _, obj := range out.Contents {
			etag := ""
			if obj.ETag != nil {
				etag = *obj.ETag
			}
			size := int64(0)
			if obj.Size != nil {
				size = *obj.Size
			}
			result[*obj.Key] = objectMeta{etag: etag, size: size}
		}
		if out.IsTruncated == nil || !*out.IsTruncated {
			break
		}
		continuationToken = out.NextContinuationToken
	}
	return result, nil
}
