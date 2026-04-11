package s3

import (
	"bytes"
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ========================================
// Mock Implementations
// ========================================

// mockRegion implements iface.Region for testing
type mockRegion struct {
	name    string
	nodeID  string
	buckets map[iface.BucketName]*mockBucket
}

func newMockRegion(name, nodeID string) *mockRegion {
	return &mockRegion{
		name:    name,
		nodeID:  nodeID,
		buckets: make(map[iface.BucketName]*mockBucket),
	}
}

func (r *mockRegion) Name() string {
	return r.name
}

func (r *mockRegion) NodeID() string {
	return r.nodeID
}

func (r *mockRegion) ListBuckets(ctx context.Context) ([]string, error) {
	names := make([]string, 0, len(r.buckets))
	for name := range r.buckets {
		names = append(names, name.String())
	}
	return names, nil
}

func (r *mockRegion) CreateBucket(ctx context.Context, name iface.BucketName) error {
	if _, exists := r.buckets[name]; exists {
		return nil
	}
	r.buckets[name] = newMockBucket(name)
	return nil
}

func (r *mockRegion) GetBucket(ctx context.Context, name iface.BucketName) (iface.Bucket, error) {
	bucket, exists := r.buckets[name]
	if !exists {
		return nil, iface.ErrNotFound
	}
	return bucket, nil
}

func (r *mockRegion) DeleteBucket(ctx context.Context, name iface.BucketName) error {
	delete(r.buckets, name)
	return nil
}

func (r *mockRegion) addBucket(name iface.BucketName, bucket *mockBucket) {
	r.buckets[name] = bucket
}

// mockBucket implements iface.Bucket for testing
type mockBucket struct {
	name    iface.BucketName
	objects map[iface.S3Key]mockObject
}

type mockObject struct {
	data      []byte
	stat      iface.Stat
	timestamp time.Time
}

func newMockBucket(name iface.BucketName) *mockBucket {
	return &mockBucket{
		name:    name,
		objects: make(map[iface.S3Key]mockObject),
	}
}

func (b *mockBucket) Name() iface.BucketName {
	return b.name
}

func (b *mockBucket) List(ctx context.Context, query *iface.ListObjectsQuery) (iface.ListObjectsResult, error) {
	var result iface.ListObjectsResult
	var commonPrefixes []string
	prefixSeen := make(map[string]bool)

	count := int32(0)
	for key, obj := range b.objects {
		keyStr := key.String()

		// Apply prefix filter
		if query.Prefix != "" && !strings.HasPrefix(keyStr, query.Prefix) {
			continue
		}

		// Apply start-after filter
		if query.StartAfter != "" && keyStr <= query.StartAfter {
			continue
		}

		// Apply continuation token (treated as start-after in this mock)
		if query.ContinuationToken != "" && keyStr <= query.ContinuationToken {
			continue
		}

		// Apply delimiter
		if query.Delimiter != "" {
			suffix := keyStr
			if query.Prefix != "" {
				suffix = strings.TrimPrefix(keyStr, query.Prefix)
			}
			if idx := strings.Index(suffix, query.Delimiter); idx >= 0 {
				prefix := query.Prefix + suffix[:idx+1]
				if !prefixSeen[prefix] {
					prefixSeen[prefix] = true
					commonPrefixes = append(commonPrefixes, prefix)
				}
				continue
			}
		}

		if count >= query.MaxKeys {
			result.IsTruncated = true
			result.NextContinuationToken = keyStr
			break
		}

		result.Contents = append(result.Contents, iface.Stat{
			Bucket:    b.name,
			Key:       key,
			ETag:      obj.stat.ETag,
			Size:      obj.stat.Size,
			Timestamp: obj.stat.Timestamp,
		})
		count++
	}

	result.CommonPrefixes = commonPrefixes
	return result, nil
}

func (b *mockBucket) Put(ctx context.Context, key iface.S3Key, input io.Reader) (iface.Stat, error) {
	data, err := io.ReadAll(input)
	if err != nil {
		return iface.Stat{}, err
	}

	now := time.Now()
	stat := iface.Stat{
		Bucket:    b.name,
		Key:       key,
		ETag:      `"d41d8cd98f00b204e9800998ecf8427e"`,
		Size:      uint64(len(data)),
		Timestamp: now,
	}

	b.objects[key] = mockObject{
		data:      data,
		stat:      stat,
		timestamp: now,
	}

	return stat, nil
}

func (b *mockBucket) Get(ctx context.Context, key iface.S3Key) (iface.ObjectReader, error) {
	obj, exists := b.objects[key]
	if !exists {
		return nil, iface.ErrNotFound
	}

	return &mockObjectReader{
		reader: bytes.NewReader(obj.data),
		stat:   obj.stat,
	}, nil
}

func (b *mockBucket) Delete(ctx context.Context, key iface.S3Key) error {
	if _, exists := b.objects[key]; !exists {
		return iface.ErrNotFound
	}
	delete(b.objects, key)
	return nil
}

func (b *mockBucket) Stat(ctx context.Context, key iface.S3Key, full bool) (iface.Stat, error) {
	obj, exists := b.objects[key]
	if !exists {
		return iface.Stat{}, iface.ErrNotFound
	}
	return obj.stat, nil
}

func (b *mockBucket) BeginMultipartPut(ctx context.Context, key iface.S3Key) (string, error) {
	return "mock-upload-id", nil
}

func (b *mockBucket) ContinueMultipartPut(ctx context.Context, name iface.S3Key, uploadId string, partNumber int64, input io.Reader) (iface.Stat, error) {
	return iface.Stat{ETag: `"part-etag"`}, nil
}

func (b *mockBucket) CompleteMultipartPut(ctx context.Context, name iface.S3Key, uploadId string, completion *iface.CompleteMultipartUpload) (iface.Stat, error) {
	return iface.Stat{ETag: `"completed-etag"`}, nil
}

func (b *mockBucket) AbortMultipartPut(ctx context.Context, name iface.S3Key, uploadId string) error {
	return nil
}

func (b *mockBucket) ListParts(ctx context.Context, key iface.S3Key, query *iface.ListPartsQuery) (*iface.ListPartsResult, error) {
	return &iface.ListPartsResult{
		Bucket:   b.name,
		Key:      key,
		UploadID: query.UploadID,
		MaxParts: query.MaxParts,
	}, nil
}

func (b *mockBucket) addObject(key iface.S3Key, data []byte, etag string, timestamp time.Time) {
	b.objects[key] = mockObject{
		data: data,
		stat: iface.Stat{
			Bucket:    b.name,
			Key:       key,
			ETag:      etag,
			Size:      uint64(len(data)),
			Timestamp: timestamp,
		},
		timestamp: timestamp,
	}
}

// mockObjectReader implements iface.ObjectReader
type mockObjectReader struct {
	reader *bytes.Reader
	stat   iface.Stat
}

func (r *mockObjectReader) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

func (r *mockObjectReader) Seek(offset int64, whence int) (int64, error) {
	return r.reader.Seek(offset, whence)
}

func (r *mockObjectReader) Close() error {
	return nil
}

func (r *mockObjectReader) Stat() iface.Stat {
	return r.stat
}

// mockAuthenticator for testing (auth disabled)
func newMockAuthenticator() *Authenticator {
	return &Authenticator{
		Enabled:     false,
		AccessKeyID: "test-access-key",
		SecretKey:   "test-secret-key",
		Region:      "us-east-1",
	}
}

// ========================================
// Request Parsing Tests
// ========================================

func TestRequestToObject(t *testing.T) {
	tests := []struct {
		name           string
		path           string
		expectError    bool
		expectedBucket iface.BucketName
		expectedKey    iface.S3Key
	}{
		{
			name:           "simple bucket and key",
			path:           "/mybucket/myobject",
			expectError:    false,
			expectedBucket: "mybucket",
			expectedKey:    "myobject",
		},
		{
			name:           "nested key path",
			path:           "/mybucket/path/to/object.txt",
			expectError:    false,
			expectedBucket: "mybucket",
			expectedKey:    "path/to/object.txt",
		},
		{
			name:           "key with special characters",
			path:           "/mybucket/my-file_v2.tar.gz",
			expectError:    false,
			expectedBucket: "mybucket",
			expectedKey:    "my-file_v2.tar.gz",
		},
		{
			name:           "deep nested path",
			path:           "/mybucket/a/b/c/d/e/f/file.txt",
			expectError:    false,
			expectedBucket: "mybucket",
			expectedKey:    "a/b/c/d/e/f/file.txt",
		},
		{
			name:        "bucket only - no key",
			path:        "/mybucket",
			expectError: true,
		},
		{
			name:        "empty path",
			path:        "/",
			expectError: true,
		},
		{
			name:        "root path only",
			path:        "",
			expectError: true,
		},
		{
			name:           "trailing slash in bucket with key",
			path:           "/mybucket/key/",
			expectError:    false,
			expectedBucket: "mybucket",
			expectedKey:    "key/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", "http://example.com"+tt.path, nil)
			require.NoError(t, err)

			bucket, key, err := requestToObject(req)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedBucket, bucket)
				assert.Equal(t, tt.expectedKey, key)
			}
		})
	}
}

func TestRequestToBucket(t *testing.T) {
	tests := []struct {
		name           string
		path           string
		expectError    bool
		expectedBucket iface.BucketName
	}{
		{
			name:           "bucket only",
			path:           "/mybucket",
			expectError:    false,
			expectedBucket: "mybucket",
		},
		{
			name:           "bucket with trailing slash",
			path:           "/mybucket/",
			expectError:    false,
			expectedBucket: "mybucket",
		},
		{
			name:        "bucket with key - should fail",
			path:        "/mybucket/somekey",
			expectError: true,
		},
		{
			name:        "empty path",
			path:        "/",
			expectError: true,
		},
		{
			name:        "just slash",
			path:        "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", "http://example.com"+tt.path, nil)
			require.NoError(t, err)

			bucket, err := requestToBucket(req)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedBucket, bucket)
			}
		})
	}
}

func TestParseBucketAndKey(t *testing.T) {
	// This tests the pattern parsing similar to requestToObject
	tests := []struct {
		name           string
		input          string
		expectedBucket string
		expectedKey    string
		expectError    bool
	}{
		{
			name:           "bucket/key",
			input:          "/bucket/key",
			expectedBucket: "bucket",
			expectedKey:    "key",
		},
		{
			name:           "bucket/nested/key",
			input:          "/bucket/nested/key",
			expectedBucket: "bucket",
			expectedKey:    "nested/key",
		},
		{
			name:           "bucket/deep/nested/path/key.txt",
			input:          "/bucket/deep/nested/path/key.txt",
			expectedBucket: "bucket",
			expectedKey:    "deep/nested/path/key.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", "http://example.com"+tt.input, nil)
			require.NoError(t, err)

			bucket, key, err := requestToObject(req)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, iface.BucketName(tt.expectedBucket), bucket)
				assert.Equal(t, iface.S3Key(tt.expectedKey), key)
			}
		})
	}
}

// ========================================
// Handler Tests
// ========================================

func TestHandleGetLocation(t *testing.T) {
	region := newMockRegion("us-west-2", "node-123")
	auth := newMockAuthenticator()
	srv := NewS3Server(region, auth)

	req := httptest.NewRequest("GET", "/mybucket?location", nil)
	w := httptest.NewRecorder()

	err := srv.handleGetLocation(w, req)
	require.NoError(t, err)

	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)

	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(body), "us-west-2")
	assert.Contains(t, string(body), "LocationConstraint")
}

func TestHandleListObjects_Empty(t *testing.T) {
	region := newMockRegion("us-east-1", "node-123")
	bucket := newMockBucket("test-bucket")
	region.addBucket("test-bucket", bucket)

	auth := newMockAuthenticator()
	srv := NewS3Server(region, auth)

	req := httptest.NewRequest("GET", "/test-bucket/?list-type=2", nil)
	w := httptest.NewRecorder()

	err := srv.handleListObjects(w, req)
	require.NoError(t, err)

	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)

	assert.Equal(t, 200, resp.StatusCode)

	var listResp ListObjectsResponse
	err = xml.Unmarshal(body, &listResp)
	require.NoError(t, err)

	assert.Equal(t, "test-bucket", listResp.Name)
	assert.Equal(t, 0, len(listResp.Contents))
	assert.False(t, listResp.IsTruncated)
	assert.Equal(t, int32(0), listResp.KeyCount)
}

func TestHandleListObjects_WithObjects(t *testing.T) {
	region := newMockRegion("us-east-1", "node-123")
	bucket := newMockBucket("test-bucket")

	now := time.Now()
	bucket.addObject("file1.txt", []byte("content1"), `"etag1"`, now)
	bucket.addObject("file2.txt", []byte("content2"), `"etag2"`, now)
	bucket.addObject("file3.txt", []byte("content3"), `"etag3"`, now)

	region.addBucket("test-bucket", bucket)

	auth := newMockAuthenticator()
	srv := NewS3Server(region, auth)

	req := httptest.NewRequest("GET", "/test-bucket/?list-type=2", nil)
	w := httptest.NewRecorder()

	err := srv.handleListObjects(w, req)
	require.NoError(t, err)

	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)

	assert.Equal(t, 200, resp.StatusCode)

	var listResp ListObjectsResponse
	err = xml.Unmarshal(body, &listResp)
	require.NoError(t, err)

	assert.Equal(t, "test-bucket", listResp.Name)
	assert.Equal(t, 3, len(listResp.Contents))
	assert.False(t, listResp.IsTruncated)
}

func TestHandleListObjects_Pagination(t *testing.T) {
	region := newMockRegion("us-east-1", "node-123")
	bucket := newMockBucket("test-bucket")

	now := time.Now()
	// Add more objects than max-keys
	for i := 0; i < 5; i++ {
		key := iface.S3Key("file" + string(rune('a'+i)) + ".txt")
		bucket.addObject(key, []byte("content"), `"etag"`, now)
	}

	region.addBucket("test-bucket", bucket)

	auth := newMockAuthenticator()
	srv := NewS3Server(region, auth)

	// Request with max-keys=2
	req := httptest.NewRequest("GET", "/test-bucket/?list-type=2&max-keys=2", nil)
	w := httptest.NewRecorder()

	err := srv.handleListObjects(w, req)
	require.NoError(t, err)

	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)

	assert.Equal(t, 200, resp.StatusCode)

	var listResp ListObjectsResponse
	err = xml.Unmarshal(body, &listResp)
	require.NoError(t, err)

	assert.Equal(t, int32(2), listResp.MaxKeys)
	// Note: actual truncation depends on mock implementation
}

func TestHandleListObjects_Prefix(t *testing.T) {
	region := newMockRegion("us-east-1", "node-123")
	bucket := newMockBucket("test-bucket")

	now := time.Now()
	bucket.addObject("docs/file1.txt", []byte("content1"), `"etag1"`, now)
	bucket.addObject("docs/file2.txt", []byte("content2"), `"etag2"`, now)
	bucket.addObject("images/photo.jpg", []byte("image"), `"etag3"`, now)

	region.addBucket("test-bucket", bucket)

	auth := newMockAuthenticator()
	srv := NewS3Server(region, auth)

	req := httptest.NewRequest("GET", "/test-bucket/?list-type=2&prefix=docs/", nil)
	w := httptest.NewRecorder()

	err := srv.handleListObjects(w, req)
	require.NoError(t, err)

	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)

	assert.Equal(t, 200, resp.StatusCode)

	var listResp ListObjectsResponse
	err = xml.Unmarshal(body, &listResp)
	require.NoError(t, err)

	assert.Equal(t, "docs/", listResp.Prefix)
	// Only docs/ prefixed files should be returned
	for _, obj := range listResp.Contents {
		assert.True(t, strings.HasPrefix(obj.Key, "docs/"))
	}
}

func TestHandleListObjects_Delimiter(t *testing.T) {
	region := newMockRegion("us-east-1", "node-123")
	bucket := newMockBucket("test-bucket")

	now := time.Now()
	bucket.addObject("docs/file1.txt", []byte("content1"), `"etag1"`, now)
	bucket.addObject("docs/subdir/file2.txt", []byte("content2"), `"etag2"`, now)
	bucket.addObject("images/photo.jpg", []byte("image"), `"etag3"`, now)
	bucket.addObject("root.txt", []byte("root"), `"etag4"`, now)

	region.addBucket("test-bucket", bucket)

	auth := newMockAuthenticator()
	srv := NewS3Server(region, auth)

	req := httptest.NewRequest("GET", "/test-bucket/?list-type=2&delimiter=/", nil)
	w := httptest.NewRecorder()

	err := srv.handleListObjects(w, req)
	require.NoError(t, err)

	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)

	assert.Equal(t, 200, resp.StatusCode)

	var listResp ListObjectsResponse
	err = xml.Unmarshal(body, &listResp)
	require.NoError(t, err)

	assert.Equal(t, "/", listResp.Delimiter)
	// Common prefixes should include directories
	// Contents should only include root level files
}

func TestHandleListObjects_BucketNotFound(t *testing.T) {
	region := newMockRegion("us-east-1", "node-123")
	auth := newMockAuthenticator()
	srv := NewS3Server(region, auth)

	req := httptest.NewRequest("GET", "/nonexistent-bucket/?list-type=2", nil)
	w := httptest.NewRecorder()

	err := srv.handleListObjects(w, req)
	require.NoError(t, err)

	resp := w.Result()
	assert.Equal(t, 404, resp.StatusCode)
}

func TestHandleGetObject_Success(t *testing.T) {
	region := newMockRegion("us-east-1", "node-123")
	bucket := newMockBucket("test-bucket")

	content := []byte("Hello, World!")
	now := time.Now()
	bucket.addObject("test.txt", content, `"abc123"`, now)

	region.addBucket("test-bucket", bucket)

	auth := newMockAuthenticator()
	srv := NewS3Server(region, auth)

	req := httptest.NewRequest("GET", "/test-bucket/test.txt", nil)
	w := httptest.NewRecorder()

	err := srv.handleGetObject(w, req)
	require.NoError(t, err)

	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)

	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, content, body)
	assert.Equal(t, `"abc123"`, resp.Header.Get("ETag"))
	assert.Equal(t, "node-123", resp.Header.Get("X-Node-ID"))
}

func TestHandleGetObject_NotFound(t *testing.T) {
	region := newMockRegion("us-east-1", "node-123")
	bucket := newMockBucket("test-bucket")
	region.addBucket("test-bucket", bucket)

	auth := newMockAuthenticator()
	srv := NewS3Server(region, auth)

	req := httptest.NewRequest("GET", "/test-bucket/nonexistent.txt", nil)
	w := httptest.NewRecorder()

	err := srv.handleGetObject(w, req)
	require.NoError(t, err)

	resp := w.Result()
	assert.Equal(t, 404, resp.StatusCode)
}

func TestHandleGetObject_BucketNotFound(t *testing.T) {
	region := newMockRegion("us-east-1", "node-123")
	auth := newMockAuthenticator()
	srv := NewS3Server(region, auth)

	req := httptest.NewRequest("GET", "/nonexistent-bucket/test.txt", nil)
	w := httptest.NewRecorder()

	err := srv.handleGetObject(w, req)
	require.NoError(t, err)

	resp := w.Result()
	assert.Equal(t, 404, resp.StatusCode)
}

func TestHandleHeadObject(t *testing.T) {
	region := newMockRegion("us-east-1", "node-123")
	bucket := newMockBucket("test-bucket")

	content := []byte("Hello, World!")
	now := time.Now()
	bucket.addObject("test.txt", content, `"abc123"`, now)

	region.addBucket("test-bucket", bucket)

	auth := newMockAuthenticator()
	srv := NewS3Server(region, auth)

	req := httptest.NewRequest("HEAD", "/test-bucket/test.txt", nil)
	w := httptest.NewRecorder()

	err := srv.handleHeadObject(w, req)
	require.NoError(t, err)

	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)

	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, 0, len(body)) // HEAD should have no body
	assert.Equal(t, `"abc123"`, resp.Header.Get("ETag"))
	assert.Equal(t, "13", resp.Header.Get("Content-Length"))
	assert.Equal(t, "node-123", resp.Header.Get("X-Node-ID"))
	assert.NotEmpty(t, resp.Header.Get("Last-Modified"))
}

func TestHandleHeadObject_NotFound(t *testing.T) {
	region := newMockRegion("us-east-1", "node-123")
	bucket := newMockBucket("test-bucket")
	region.addBucket("test-bucket", bucket)

	auth := newMockAuthenticator()
	srv := NewS3Server(region, auth)

	req := httptest.NewRequest("HEAD", "/test-bucket/nonexistent.txt", nil)
	w := httptest.NewRecorder()

	err := srv.handleHeadObject(w, req)
	require.NoError(t, err)

	resp := w.Result()
	assert.Equal(t, 404, resp.StatusCode)
}

func TestHandlePutObject(t *testing.T) {
	region := newMockRegion("us-east-1", "node-123")
	bucket := newMockBucket("test-bucket")
	region.addBucket("test-bucket", bucket)

	auth := newMockAuthenticator()
	srv := NewS3Server(region, auth)

	content := []byte("New file content")
	req := httptest.NewRequest("PUT", "/test-bucket/newfile.txt", bytes.NewReader(content))
	req.Header.Set("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
	w := httptest.NewRecorder()

	err := srv.handlePutObject(w, req)
	require.NoError(t, err)

	resp := w.Result()
	assert.Equal(t, 200, resp.StatusCode)
	assert.NotEmpty(t, resp.Header.Get("ETag"))
	assert.Equal(t, "node-123", resp.Header.Get("X-Node-ID"))

	// Verify object was stored
	_, exists := bucket.objects["newfile.txt"]
	assert.True(t, exists)
}

func TestHandlePutObject_BucketNotFound(t *testing.T) {
	region := newMockRegion("us-east-1", "node-123")
	auth := newMockAuthenticator()
	srv := NewS3Server(region, auth)

	content := []byte("New file content")
	req := httptest.NewRequest("PUT", "/nonexistent-bucket/newfile.txt", bytes.NewReader(content))
	req.Header.Set("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
	w := httptest.NewRecorder()

	err := srv.handlePutObject(w, req)
	require.NoError(t, err)

	resp := w.Result()
	assert.Equal(t, 404, resp.StatusCode)
}

func TestHandleDeleteObject(t *testing.T) {
	region := newMockRegion("us-east-1", "node-123")
	bucket := newMockBucket("test-bucket")

	now := time.Now()
	bucket.addObject("todelete.txt", []byte("delete me"), `"etag"`, now)
	region.addBucket("test-bucket", bucket)

	auth := newMockAuthenticator()
	srv := NewS3Server(region, auth)

	req := httptest.NewRequest("DELETE", "/test-bucket/todelete.txt", nil)
	w := httptest.NewRecorder()

	err := srv.handleDeleteObject(w, req)
	require.NoError(t, err)

	resp := w.Result()
	assert.Equal(t, 204, resp.StatusCode)

	// Verify object was deleted
	_, exists := bucket.objects["todelete.txt"]
	assert.False(t, exists)
}

func TestHandleDeleteObject_NotFound(t *testing.T) {
	region := newMockRegion("us-east-1", "node-123")
	bucket := newMockBucket("test-bucket")
	region.addBucket("test-bucket", bucket)

	auth := newMockAuthenticator()
	srv := NewS3Server(region, auth)

	req := httptest.NewRequest("DELETE", "/test-bucket/nonexistent.txt", nil)
	w := httptest.NewRecorder()

	err := srv.handleDeleteObject(w, req)
	require.NoError(t, err)

	resp := w.Result()
	assert.Equal(t, 404, resp.StatusCode)
}

func TestHandleDeleteObject_BucketNotFound(t *testing.T) {
	region := newMockRegion("us-east-1", "node-123")
	auth := newMockAuthenticator()
	srv := NewS3Server(region, auth)

	req := httptest.NewRequest("DELETE", "/nonexistent-bucket/test.txt", nil)
	w := httptest.NewRecorder()

	err := srv.handleDeleteObject(w, req)
	require.NoError(t, err)

	resp := w.Result()
	assert.Equal(t, 404, resp.StatusCode)
}

func TestHandleUploadPart_InvalidPartNumberRange(t *testing.T) {
	region := newMockRegion("us-east-1", "node-123")
	bucket := newMockBucket("test-bucket")
	region.addBucket("test-bucket", bucket)

	auth := newMockAuthenticator()
	srv := NewS3Server(region, auth)

	req := httptest.NewRequest("PUT", "/test-bucket/object.txt?uploadId=u1&partNumber=0", bytes.NewReader([]byte("part")))
	req.Header.Set("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
	w := httptest.NewRecorder()

	err := srv.handleUploadPart(w, req)
	require.NoError(t, err)
	assert.Equal(t, 400, w.Result().StatusCode)
}

func TestHandleListParts_InvalidMaxPartsRange(t *testing.T) {
	region := newMockRegion("us-east-1", "node-123")
	bucket := newMockBucket("test-bucket")
	region.addBucket("test-bucket", bucket)

	auth := newMockAuthenticator()
	srv := NewS3Server(region, auth)

	req := httptest.NewRequest("GET", "/test-bucket/object.txt?uploadId=u1&max-parts=1001", nil)
	w := httptest.NewRecorder()

	err := srv.handleListParts(w, req)
	require.NoError(t, err)
	assert.Equal(t, 400, w.Result().StatusCode)
}

func TestHandleListParts_InvalidPartNumberMarkerRange(t *testing.T) {
	region := newMockRegion("us-east-1", "node-123")
	bucket := newMockBucket("test-bucket")
	region.addBucket("test-bucket", bucket)

	auth := newMockAuthenticator()
	srv := NewS3Server(region, auth)

	req := httptest.NewRequest("GET", "/test-bucket/object.txt?uploadId=u1&part-number-marker=10001", nil)
	w := httptest.NewRecorder()

	err := srv.handleListParts(w, req)
	require.NoError(t, err)
	assert.Equal(t, 400, w.Result().StatusCode)
}

// ========================================
// Edge Cases and Error Handling
// ========================================

func TestHandleListObjects_InvalidMaxKeys(t *testing.T) {
	region := newMockRegion("us-east-1", "node-123")
	bucket := newMockBucket("test-bucket")
	region.addBucket("test-bucket", bucket)

	auth := newMockAuthenticator()
	srv := NewS3Server(region, auth)

	// max-keys = 0 is invalid
	req := httptest.NewRequest("GET", "/test-bucket/?list-type=2&max-keys=0", nil)
	w := httptest.NewRecorder()

	err := srv.handleListObjects(w, req)
	require.NoError(t, err)

	resp := w.Result()
	assert.Equal(t, 400, resp.StatusCode)
}

func TestHandleListObjects_MaxKeysExceedsLimit(t *testing.T) {
	region := newMockRegion("us-east-1", "node-123")
	bucket := newMockBucket("test-bucket")
	region.addBucket("test-bucket", bucket)

	auth := newMockAuthenticator()
	srv := NewS3Server(region, auth)

	// max-keys > 1000 is invalid
	req := httptest.NewRequest("GET", "/test-bucket/?list-type=2&max-keys=1001", nil)
	w := httptest.NewRecorder()

	err := srv.handleListObjects(w, req)
	require.NoError(t, err)

	resp := w.Result()
	assert.Equal(t, 400, resp.StatusCode)
}

func TestHandleGetObject_MalformedPath(t *testing.T) {
	region := newMockRegion("us-east-1", "node-123")
	auth := newMockAuthenticator()
	srv := NewS3Server(region, auth)

	// No object key in path
	req := httptest.NewRequest("GET", "/bucket-only", nil)
	w := httptest.NewRecorder()

	err := srv.handleGetObject(w, req)
	require.NoError(t, err)

	resp := w.Result()
	assert.Equal(t, 400, resp.StatusCode)
}

func TestHandleListObjects_ContinuationToken(t *testing.T) {
	region := newMockRegion("us-east-1", "node-123")
	bucket := newMockBucket("test-bucket")

	now := time.Now()
	bucket.addObject("file1.txt", []byte("content1"), `"etag1"`, now)
	bucket.addObject("file2.txt", []byte("content2"), `"etag2"`, now)
	bucket.addObject("file3.txt", []byte("content3"), `"etag3"`, now)

	region.addBucket("test-bucket", bucket)

	auth := newMockAuthenticator()
	srv := NewS3Server(region, auth)

	// Request with continuation token
	req := httptest.NewRequest("GET", "/test-bucket/?list-type=2&continuation-token=file1.txt", nil)
	w := httptest.NewRecorder()

	err := srv.handleListObjects(w, req)
	require.NoError(t, err)

	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)

	assert.Equal(t, 200, resp.StatusCode)

	var listResp ListObjectsResponse
	err = xml.Unmarshal(body, &listResp)
	require.NoError(t, err)

	assert.Equal(t, "file1.txt", listResp.ContinuationToken)
}

func TestHandleHeadObject_WithFilIncludeMeta(t *testing.T) {
	region := newMockRegion("us-east-1", "node-123")
	bucket := newMockBucket("test-bucket")

	content := []byte("test content")
	now := time.Now()
	bucket.addObject("test.txt", content, `"abc123"`, now)

	region.addBucket("test-bucket", bucket)

	auth := newMockAuthenticator()
	srv := NewS3Server(region, auth)

	req := httptest.NewRequest("HEAD", "/test-bucket/test.txt?fil-include-meta=1", nil)
	w := httptest.NewRecorder()

	err := srv.handleHeadObject(w, req)
	require.NoError(t, err)

	resp := w.Result()
	assert.Equal(t, 200, resp.StatusCode)
}
