package s3frontend

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubMultipartLookup struct {
	upload *MultipartUpload
	err    error
}

func (s stubMultipartLookup) GetUpload(context.Context, string) (*MultipartUpload, error) {
	return s.upload, s.err
}

func TestParseBucketAndKey(t *testing.T) {
	tests := []struct {
		name           string
		path           string
		expectError    bool
		expectedBucket string
		expectedKey    string
	}{
		{
			name:           "bucket only",
			path:           "/mybucket",
			expectError:    false,
			expectedBucket: "mybucket",
			expectedKey:    "",
		},
		{
			name:           "bucket and key",
			path:           "/mybucket/myobject",
			expectError:    false,
			expectedBucket: "mybucket",
			expectedKey:    "myobject",
		},
		{
			name:           "nested key",
			path:           "/mybucket/path/to/object",
			expectError:    false,
			expectedBucket: "mybucket",
			expectedKey:    "path/to/object",
		},
		{
			name:           "empty path",
			path:           "/",
			expectError:    false,
			expectedBucket: "",
			expectedKey:    "",
		},
		{
			name:           "no leading slash",
			path:           "mybucket/myobject",
			expectError:    false,
			expectedBucket: "mybucket",
			expectedKey:    "myobject",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, key, err := parseBucketAndKey(tt.path)

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

func TestListPartsRejectsInactiveUploads(t *testing.T) {
	tests := []struct {
		name   string
		status string
	}{
		{name: "completed upload", status: "completed"},
		{name: "aborted upload", status: "aborted"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := NewFrontendServer(nil, &BackendPool{}, nil, stubMultipartLookup{upload: &MultipartUpload{
				UploadID:  "u1",
				NodeID:    "node-1",
				Status:    tt.status,
				ExpiresAt: time.Now().Add(time.Hour),
			}}, "frontend-1")

			req := httptest.NewRequest(http.MethodGet, "/bucket/object?uploadId=u1", nil)
			rr := httptest.NewRecorder()

			srv.ServeHTTP(rr, req)

			require.Equal(t, http.StatusNotFound, rr.Code)
		})
	}
}

func TestListPartsRejectsExpiredUploads(t *testing.T) {
	srv := NewFrontendServer(nil, &BackendPool{}, nil, stubMultipartLookup{upload: &MultipartUpload{
		UploadID:  "u1",
		NodeID:    "node-1",
		Status:    "active",
		ExpiresAt: time.Now().Add(-time.Minute),
	}}, "frontend-1")

	req := httptest.NewRequest(http.MethodGet, "/bucket/object?uploadId=u1", nil)
	rr := httptest.NewRecorder()

	srv.ServeHTTP(rr, req)

	require.Equal(t, http.StatusNotFound, rr.Code)
}
