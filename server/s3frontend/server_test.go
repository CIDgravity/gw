package s3frontend

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
