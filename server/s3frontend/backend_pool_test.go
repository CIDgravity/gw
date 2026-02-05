package s3frontend

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBackendPool(t *testing.T) {
	tests := []struct {
		name        string
		config      string
		expectError bool
		numBackends int
	}{
		{
			name:        "single node",
			config:      "kuri-1:http://localhost:8080",
			expectError: false,
			numBackends: 1,
		},
		{
			name:        "multiple nodes",
			config:      "kuri-1:http://localhost:8080,kuri-2:http://localhost:8081,kuri-3:http://localhost:8082",
			expectError: false,
			numBackends: 3,
		},
		{
			name:        "empty config",
			config:      "",
			expectError: true,
			numBackends: 0,
		},
		{
			name:        "invalid format",
			config:      "invalid-format",
			expectError: true,
			numBackends: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool, err := NewBackendPool(tt.config)

			if tt.expectError {
				require.Error(t, err)
				assert.Nil(t, pool)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, pool)
				assert.Len(t, pool.backends, tt.numBackends)
			}
		})
	}
}

func TestBackendPoolSelectRoundRobin(t *testing.T) {
	pool, err := NewBackendPool("kuri-1:http://localhost:8080,kuri-2:http://localhost:8081,kuri-3:http://localhost:8082")
	require.NoError(t, err)

	// Get first backend (counter starts at 0, first increment makes it 1 -> index 1)
	backend1 := pool.SelectRoundRobin()
	require.NotNil(t, backend1)

	// Get second backend
	backend2 := pool.SelectRoundRobin()
	require.NotNil(t, backend2)

	// Get third backend
	backend3 := pool.SelectRoundRobin()
	require.NotNil(t, backend3)

	// Get fourth backend (should wrap around)
	backend4 := pool.SelectRoundRobin()
	require.NotNil(t, backend4)

	// Verify all backends are different in the first 3 calls
	ids := map[string]bool{
		backend1.ID(): true,
		backend2.ID(): true,
		backend3.ID(): true,
	}
	assert.Len(t, ids, 3, "First 3 backends should be different")

	// Fourth should be one of the first 3
	assert.Contains(t, ids, backend4.ID(), "Fourth backend should wrap around")
}

func TestBackendPoolGet(t *testing.T) {
	pool, err := NewBackendPool("kuri-1:http://localhost:8080,kuri-2:http://localhost:8081")
	require.NoError(t, err)

	// Get existing backend
	backend := pool.Get("kuri-1")
	require.NotNil(t, backend)
	assert.Equal(t, "kuri-1", backend.ID())
	assert.Equal(t, "http://localhost:8080", backend.URL())

	// Get non-existent backend
	backend = pool.Get("kuri-999")
	assert.Nil(t, backend)
}

func TestBackendHealth(t *testing.T) {
	backend := &Backend{
		id:      "test-node",
		baseURL: "http://localhost:8080",
	}
	backend.setHealth(true)

	// Initially healthy
	assert.True(t, backend.IsHealthy())

	// Set unhealthy
	backend.setHealth(false)
	assert.False(t, backend.IsHealthy())

	// Set healthy again
	backend.setHealth(true)
	assert.True(t, backend.IsHealthy())
}
