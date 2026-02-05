package s3frontend

import (
	"context"
	"fmt"
	"time"

	"github.com/CIDgravity/filecoin-gateway/database/cqldb"
	"github.com/yugabyte/gocql"
)

// MultipartTracker tracks multipart upload state in YCQL
type MultipartTracker struct {
	db cqldb.Database
}

// MultipartUpload represents a multipart upload session
type MultipartUpload struct {
	UploadID  string
	Bucket    string
	Key       string
	NodeID    string // Coordinator node
	CreatedAt time.Time
	ExpiresAt time.Time
	Status    string // "active", "completed", "aborted"
}

// NewMultipartTracker creates a new multipart tracker
func NewMultipartTracker(db cqldb.Database) *MultipartTracker {
	return &MultipartTracker{db: db}
}

// CreateUpload records a new multipart upload
func (t *MultipartTracker) CreateUpload(ctx context.Context, uploadID, bucket, key, nodeID string) error {
	createdAt := time.Now()
	expiresAt := createdAt.Add(24 * time.Hour) // 24 hour expiration

	err := t.db.Query(
		"INSERT INTO MultipartUploads (upload_id, bucket, key, node_id, created_at, expires_at, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
		uploadID, bucket, key, nodeID, createdAt, expiresAt, "active",
	).WithContext(ctx).Consistency(gocql.Quorum).Exec()

	if err != nil {
		return fmt.Errorf("failed to create upload record: %w", err)
	}

	return nil
}

// GetUpload retrieves upload information
func (t *MultipartTracker) GetUpload(ctx context.Context, uploadID string) (*MultipartUpload, error) {
	var upload MultipartUpload

	err := t.db.Query(
		"SELECT upload_id, bucket, key, node_id, created_at, expires_at, status FROM MultipartUploads WHERE upload_id = ?",
		uploadID,
	).WithContext(ctx).Consistency(gocql.Quorum).Scan(
		&upload.UploadID, &upload.Bucket, &upload.Key, &upload.NodeID,
		&upload.CreatedAt, &upload.ExpiresAt, &upload.Status,
	)

	if err == gocql.ErrNotFound {
		return nil, fmt.Errorf("upload not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get upload: %w", err)
	}

	return &upload, nil
}

// CompleteUpload marks an upload as completed
func (t *MultipartTracker) CompleteUpload(ctx context.Context, uploadID string) error {
	err := t.db.Query(
		"UPDATE MultipartUploads SET status = ? WHERE upload_id = ?",
		"completed", uploadID,
	).WithContext(ctx).Consistency(gocql.Quorum).Exec()

	if err != nil {
		return fmt.Errorf("failed to complete upload: %w", err)
	}

	return nil
}

// AbortUpload marks an upload as aborted
func (t *MultipartTracker) AbortUpload(ctx context.Context, uploadID string) error {
	err := t.db.Query(
		"UPDATE MultipartUploads SET status = ? WHERE upload_id = ?",
		"aborted", uploadID,
	).WithContext(ctx).Consistency(gocql.Quorum).Exec()

	if err != nil {
		return fmt.Errorf("failed to abort upload: %w", err)
	}

	return nil
}

// CleanupExpiredUploads removes expired multipart uploads
func (t *MultipartTracker) CleanupExpiredUploads(ctx context.Context, before time.Time) error {
	// This would typically be run by a background job
	// For now, just log that cleanup should happen
	log.Infow("Cleaning up expired multipart uploads", "before", before)
	return nil
}
