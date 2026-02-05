package main

import (
	"bytes"
	"context"
	"crypto/md5"
	cryptoRand "crypto/rand"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockS3Server creates a mock S3 server for testing
type mockS3Server struct {
	objects  sync.Map // map[string][]byte
	putCount atomic.Int64
	getCount atomic.Int64
}

func newMockS3Server() *mockS3Server {
	return &mockS3Server{}
}

func (m *mockS3Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path

	switch r.Method {
	case "PUT":
		data, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		m.objects.Store(key, data)
		m.putCount.Add(1)
		w.WriteHeader(http.StatusOK)

	case "GET":
		data, ok := m.objects.Load(key)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		m.getCount.Add(1)
		w.Write(data.([]byte))

	case "DELETE":
		m.objects.Delete(key)
		w.WriteHeader(http.StatusNoContent)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func TestShardedDataGenerator(t *testing.T) {
	gen := NewShardedDataGenerator(4*1024, 64)

	t.Run("GenerateCorrectSize", func(t *testing.T) {
		sizes := []int{100, 1024, 10 * 1024, 100 * 1024, 1024 * 1024}
		for _, size := range sizes {
			data, checksum := gen.Generate(size)
			assert.Equal(t, size, len(data), "Generated data should be exactly %d bytes", size)
			assert.NotEmpty(t, checksum, "Checksum should not be empty")

			// Verify checksum matches
			expected := fmt.Sprintf("%x", md5.Sum(data))
			assert.Equal(t, expected, checksum, "Checksum should match")
		}
	})

	t.Run("GenerateUniqueData", func(t *testing.T) {
		// Generate multiple buffers and verify they're not identical
		data1, _ := gen.Generate(10 * 1024)
		data2, _ := gen.Generate(10 * 1024)
		assert.False(t, bytes.Equal(data1, data2), "Generated data should be unique")
	})

	t.Run("FillBuffer", func(t *testing.T) {
		buf := make([]byte, 10*1024)
		gen.FillBuffer(buf)
		// Verify buffer was filled (not all zeros)
		allZero := true
		for _, b := range buf {
			if b != 0 {
				allZero = false
				break
			}
		}
		assert.False(t, allZero, "Buffer should be filled with random data")
	})

	t.Run("GenerateData", func(t *testing.T) {
		data := gen.GenerateData(5 * 1024)
		assert.Equal(t, 5*1024, len(data))
	})

	t.Run("GenerateSizeInRange", func(t *testing.T) {
		minSize := int64(1024)
		maxSize := int64(10 * 1024)
		for i := 0; i < 100; i++ {
			size := gen.GenerateSize(minSize, maxSize)
			assert.GreaterOrEqual(t, size, minSize, "Size should be >= minSize")
			assert.LessOrEqual(t, size, maxSize, "Size should be <= maxSize")
		}
	})

	t.Run("ShouldReadRatio", func(t *testing.T) {
		// Test that ShouldRead returns roughly the expected ratio
		readCount := 0
		iterations := 10000
		for i := 0; i < iterations; i++ {
			if gen.ShouldRead(0.3) {
				readCount++
			}
		}
		ratio := float64(readCount) / float64(iterations)
		assert.InDelta(t, 0.3, ratio, 0.05, "Read ratio should be approximately 0.3")
	})
}

func TestS3Client(t *testing.T) {
	mock := newMockS3Server()
	server := httptest.NewServer(mock)
	defer server.Close()

	client := newS3Client(server.URL, "test-bucket")
	ctx := context.Background()

	t.Run("PutAndGet", func(t *testing.T) {
		data := []byte("hello world")
		err := client.putObject(ctx, "test-key", data)
		require.NoError(t, err)

		got, err := client.getObject(ctx, "test-key")
		require.NoError(t, err)
		assert.Equal(t, data, got)
	})

	t.Run("Delete", func(t *testing.T) {
		data := []byte("to be deleted")
		err := client.putObject(ctx, "delete-key", data)
		require.NoError(t, err)

		err = client.deleteObject(ctx, "delete-key")
		require.NoError(t, err)

		_, err = client.getObject(ctx, "delete-key")
		assert.Error(t, err, "Should error on deleted object")
	})
}

func TestLoadtestStats(t *testing.T) {
	stats := &loadtestStats{}

	// Record some writes
	stats.recordWrite(1024, 10*time.Millisecond, nil)
	stats.recordWrite(2048, 20*time.Millisecond, nil)
	stats.recordWrite(0, 5*time.Millisecond, fmt.Errorf("write error"))

	// Record some reads
	stats.recordRead(512, 5*time.Millisecond, nil)
	stats.recordRead(0, 3*time.Millisecond, fmt.Errorf("read error"))

	// Record verification
	stats.recordVerify(true)
	stats.recordVerify(true)
	stats.recordVerify(false)

	assert.Equal(t, int64(2), stats.writeOps)
	assert.Equal(t, int64(1), stats.readOps)
	assert.Equal(t, int64(3072), stats.writeBytes)
	assert.Equal(t, int64(512), stats.readBytes)
	assert.Equal(t, int64(1), stats.writeErrors)
	assert.Equal(t, int64(1), stats.readErrors)
	assert.Equal(t, int64(2), stats.verifySuccess)
	assert.Equal(t, int64(1), stats.verifyErrors)
}

// Benchmarks

func BenchmarkShardedDataGenerator_Generate(b *testing.B) {
	gen := NewShardedDataGenerator(4*1024, 256)

	sizes := []int{
		1 * 1024,         // 1KB
		10 * 1024,        // 10KB
		100 * 1024,       // 100KB
		1024 * 1024,      // 1MB
		10 * 1024 * 1024, // 10MB
	}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%dKB_WithMD5", size/1024), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				gen.Generate(size)
			}
		})
	}

	// Benchmark without MD5 (just data generation)
	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%dKB_DataOnly", size/1024), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				gen.GenerateData(size)
			}
		})
	}

	// Benchmark with pre-allocated buffer (no allocation)
	for _, size := range sizes {
		buf := make([]byte, size)
		b.Run(fmt.Sprintf("Size_%dKB_FillBuffer", size/1024), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				gen.FillBuffer(buf)
			}
		})
	}
}

func BenchmarkCryptoRand_Generate(b *testing.B) {
	// Benchmark crypto/rand for comparison
	sizes := []int{
		1 * 1024,         // 1KB
		10 * 1024,        // 10KB
		100 * 1024,       // 100KB
		1024 * 1024,      // 1MB
		10 * 1024 * 1024, // 10MB
	}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%dKB", size/1024), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				data := make([]byte, size)
				cryptoRand.Read(data)
			}
		})
	}
}

func BenchmarkMD5Checksum(b *testing.B) {
	sizes := []int{
		1 * 1024,    // 1KB
		100 * 1024,  // 100KB
		1024 * 1024, // 1MB
	}

	for _, size := range sizes {
		data := make([]byte, size)
		b.Run(fmt.Sprintf("Size_%dKB", size/1024), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = md5.Sum(data)
			}
		})
	}
}

func BenchmarkS3Client_PutObject(b *testing.B) {
	mock := newMockS3Server()
	server := httptest.NewServer(mock)
	defer server.Close()

	client := newS3Client(server.URL, "bench-bucket")
	ctx := context.Background()
	gen := NewShardedDataGenerator(4*1024, 256)

	sizes := []int{
		1 * 1024,   // 1KB
		10 * 1024,  // 10KB
		100 * 1024, // 100KB
	}

	for _, size := range sizes {
		data, _ := gen.Generate(size)
		b.Run(fmt.Sprintf("Size_%dKB", size/1024), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("bench-key-%d", i)
				_ = client.putObject(ctx, key, data)
			}
		})
	}
}

func BenchmarkS3Client_GetObject(b *testing.B) {
	mock := newMockS3Server()
	server := httptest.NewServer(mock)
	defer server.Close()

	client := newS3Client(server.URL, "bench-bucket")
	ctx := context.Background()
	gen := NewShardedDataGenerator(4*1024, 256)

	sizes := []int{
		1 * 1024,   // 1KB
		10 * 1024,  // 10KB
		100 * 1024, // 100KB
	}

	for _, size := range sizes {
		data, _ := gen.Generate(size)
		key := fmt.Sprintf("bench-get-key-%d", size)
		_ = client.putObject(ctx, key, data)

		b.Run(fmt.Sprintf("Size_%dKB", size/1024), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = client.getObject(ctx, key)
			}
		})
	}
}

func BenchmarkLoadtest_EndToEnd(b *testing.B) {
	mock := newMockS3Server()
	server := httptest.NewServer(mock)
	defer server.Close()

	client := newS3Client(server.URL, "bench-bucket")
	ctx := context.Background()
	gen := NewShardedDataGenerator(4*1024, 256)

	b.Run("WriteReadCycle_10KB", func(b *testing.B) {
		size := 10 * 1024
		b.SetBytes(int64(size * 2)) // write + read

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("cycle-key-%d", i)
			data, checksum := gen.Generate(size)

			// Write
			_ = client.putObject(ctx, key, data)

			// Read
			got, _ := client.getObject(ctx, key)

			// Verify (without checksum calculation in hot path for fairness)
			_ = got
			_ = checksum
		}
	})

	b.Run("WriteReadVerifyCycle_10KB", func(b *testing.B) {
		size := 10 * 1024
		b.SetBytes(int64(size * 2))

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("verify-key-%d", i)
			data, checksum := gen.Generate(size)

			// Write
			_ = client.putObject(ctx, key, data)

			// Read
			got, _ := client.getObject(ctx, key)

			// Verify
			actualChecksum := fmt.Sprintf("%x", md5.Sum(got))
			if actualChecksum != checksum {
				b.Fatalf("checksum mismatch")
			}
		}
	})
}

// Test concurrent data generation doesn't cause data races
func TestShardedDataGenerator_Concurrent(t *testing.T) {
	gen := NewShardedDataGenerator(4*1024, 64)
	const numGoroutines = 10
	const iterationsPerGoroutine = 100

	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterationsPerGoroutine; j++ {
				size := gen.GenerateSize(1024, 10*1024)
				data, checksum := gen.Generate(int(size))

				// Verify checksum
				expected := fmt.Sprintf("%x", md5.Sum(data))
				assert.Equal(t, expected, checksum)
			}
		}()
	}
	wg.Wait()
}
