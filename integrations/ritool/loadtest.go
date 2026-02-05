package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	mrand "math/rand"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cheggaaa/pb"
	"github.com/fatih/color"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var loadtestCmd = &cli.Command{
	Name:  "loadtest",
	Usage: "S3 endpoint load testing utilities",
	Subcommands: []*cli.Command{
		loadtestRunCmd,
	},
}

var loadtestRunCmd = &cli.Command{
	Name:      "run",
	Usage:     "Run a load test against an S3 endpoint",
	ArgsUsage: "[endpoint URL]",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "bucket",
			Value: "loadtest",
			Usage: "Bucket name to use for testing",
		},
		&cli.IntFlag{
			Name:  "concurrency",
			Value: 10,
			Usage: "Number of concurrent workers",
		},
		&cli.DurationFlag{
			Name:  "duration",
			Value: 60 * time.Second,
			Usage: "Test duration",
		},
		&cli.StringFlag{
			Name:  "min-size",
			Value: "1KB",
			Usage: "Minimum object size (e.g., 1KB, 512KB, 1MB)",
		},
		&cli.StringFlag{
			Name:  "max-size",
			Value: "1MB",
			Usage: "Maximum object size (e.g., 1KB, 512KB, 10MB)",
		},
		&cli.Float64Flag{
			Name:  "read-ratio",
			Value: 0.5,
			Usage: "Ratio of read operations (0.0-1.0, rest are writes)",
		},
		&cli.IntFlag{
			Name:  "multipart-threshold",
			Value: 5 * 1024 * 1024, // 5MB
			Usage: "Size threshold for multipart uploads (bytes)",
		},
		&cli.IntFlag{
			Name:  "multipart-part-size",
			Value: 5 * 1024 * 1024, // 5MB
			Usage: "Part size for multipart uploads (bytes)",
		},
		&cli.BoolFlag{
			Name:  "verify",
			Value: true,
			Usage: "Verify read-after-write correctness using checksums",
		},
		&cli.BoolFlag{
			Name:  "cleanup",
			Value: true,
			Usage: "Clean up test objects after completion",
		},
		&cli.BoolFlag{
			Name:  "verbose",
			Value: false,
			Usage: "Print verbose output including individual operations",
		},
	},
	Action: runLoadtest,
}

// parseSize parses a human-readable size string (e.g., "1KB", "5MB") to bytes
func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	var multiplier int64 = 1

	if strings.HasSuffix(s, "KB") {
		multiplier = 1024
		s = strings.TrimSuffix(s, "KB")
	} else if strings.HasSuffix(s, "MB") {
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(s, "MB")
	} else if strings.HasSuffix(s, "GB") {
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "GB")
	} else if strings.HasSuffix(s, "B") {
		s = strings.TrimSuffix(s, "B")
	}

	var value int64
	_, err := fmt.Sscanf(s, "%d", &value)
	if err != nil {
		return 0, xerrors.Errorf("invalid size format: %s", s)
	}

	return value * multiplier, nil
}

// formatSize formats bytes to human-readable format
func formatSize(bytes int64) string {
	if bytes >= 1024*1024*1024 {
		return fmt.Sprintf("%.2f GB", float64(bytes)/(1024*1024*1024))
	} else if bytes >= 1024*1024 {
		return fmt.Sprintf("%.2f MB", float64(bytes)/(1024*1024))
	} else if bytes >= 1024 {
		return fmt.Sprintf("%.2f KB", float64(bytes)/1024)
	}
	return fmt.Sprintf("%d B", bytes)
}

// formatRate formats bytes per second to human-readable throughput
func formatRate(bytesPerSec float64) string {
	if bytesPerSec >= 1024*1024*1024 {
		return fmt.Sprintf("%.2f GB/s", bytesPerSec/(1024*1024*1024))
	} else if bytesPerSec >= 1024*1024 {
		return fmt.Sprintf("%.2f MB/s", bytesPerSec/(1024*1024))
	} else if bytesPerSec >= 1024 {
		return fmt.Sprintf("%.2f KB/s", bytesPerSec/1024)
	}
	return fmt.Sprintf("%.2f B/s", bytesPerSec)
}

// ShardedDataGenerator generates random data by shuffling pre-generated shards.
// This avoids crypto/rand being a bottleneck during high-throughput tests.
type ShardedDataGenerator struct {
	shards      [][]byte
	shardSize   int
	numShards   int
	shardMask   int // For fast modulo when numShards is power of 2
	rng         *mrand.Rand
	mu          sync.Mutex
	reuseBuf    []byte // Reusable buffer to avoid allocations
	reuseBufCap int
}

// NewShardedDataGenerator creates a new generator with pre-generated random shards.
// shardSize is the size of each shard (e.g., 4KB), numShards is how many shards to generate.
// For best performance, numShards should be a power of 2.
func NewShardedDataGenerator(shardSize, numShards int) *ShardedDataGenerator {
	shards := make([][]byte, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = make([]byte, shardSize)
		rand.Read(shards[i])
	}

	// Seed math/rand from crypto/rand
	var seed int64
	binary.Read(rand.Reader, binary.LittleEndian, &seed)

	// Calculate mask for fast modulo (only works if numShards is power of 2)
	shardMask := -1
	if numShards > 0 && (numShards&(numShards-1)) == 0 {
		shardMask = numShards - 1
	}

	return &ShardedDataGenerator{
		shards:    shards,
		shardSize: shardSize,
		numShards: numShards,
		shardMask: shardMask,
		rng:       mrand.New(mrand.NewSource(seed)),
	}
}

// FillBuffer fills the provided buffer with random data by assembling shuffled shards.
// This avoids allocation overhead. The buffer must be pre-allocated.
func (g *ShardedDataGenerator) FillBuffer(data []byte) {
	g.mu.Lock()
	defer g.mu.Unlock()

	size := len(data)
	offset := 0

	for offset < size {
		// Pick a random shard using fast path if possible
		var shardIdx int
		if g.shardMask >= 0 {
			shardIdx = int(g.rng.Uint32()) & g.shardMask
		} else {
			shardIdx = int(g.rng.Uint32() % uint32(g.numShards))
		}
		shard := g.shards[shardIdx]

		// Copy full shard if possible, otherwise partial
		copyLen := g.shardSize
		if offset+copyLen > size {
			copyLen = size - offset
		}

		copy(data[offset:], shard[:copyLen])
		offset += copyLen
	}
}

// Generate creates a byte slice of the given size by assembling shuffled shards.
// Returns the data and its MD5 checksum.
func (g *ShardedDataGenerator) Generate(size int) ([]byte, string) {
	data := make([]byte, size)
	g.FillBuffer(data)
	checksum := fmt.Sprintf("%x", md5.Sum(data))
	return data, checksum
}

// GenerateData creates a byte slice of the given size without computing checksum.
// Use this when you need to compute the checksum separately or don't need it.
func (g *ShardedDataGenerator) GenerateData(size int) []byte {
	data := make([]byte, size)
	g.FillBuffer(data)
	return data
}

// GenerateSize generates a random size between min and max (inclusive)
func (g *ShardedDataGenerator) GenerateSize(minSize, maxSize int64) int64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	if maxSize <= minSize {
		return minSize
	}
	return minSize + g.rng.Int63n(maxSize-minSize+1)
}

// ShouldRead returns true if a read operation should be performed based on ratio
func (g *ShardedDataGenerator) ShouldRead(readRatio float64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.rng.Float64() < readRatio
}

// PickIndex returns a random index in range [0, max)
func (g *ShardedDataGenerator) PickIndex(max int) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.rng.Intn(max)
}

// testObject represents an object written during the test
type testObject struct {
	key      string
	size     int64
	checksum string // MD5 hex
}

// loadtestStats tracks test statistics
type loadtestStats struct {
	mu sync.Mutex

	// Counters
	writeOps       int64
	readOps        int64
	multipartOps   int64
	writeBytes     int64
	readBytes      int64
	writeErrors    int64
	readErrors     int64
	verifyErrors   int64 // Actual data corruption (checksum mismatch)
	verifyTimeouts int64 // Read timeouts during verification (not corruption)
	verifySuccess  int64

	// Latencies (in milliseconds)
	writeLatencies []float64
	readLatencies  []float64
}

func (s *loadtestStats) recordWrite(bytes int64, latency time.Duration, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err != nil {
		s.writeErrors++
	} else {
		s.writeOps++
		s.writeBytes += bytes
		s.writeLatencies = append(s.writeLatencies, float64(latency.Milliseconds()))
	}
}

func (s *loadtestStats) recordMultipart(bytes int64, latency time.Duration, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err != nil {
		s.writeErrors++
	} else {
		s.multipartOps++
		s.writeOps++
		s.writeBytes += bytes
		s.writeLatencies = append(s.writeLatencies, float64(latency.Milliseconds()))
	}
}

func (s *loadtestStats) recordRead(bytes int64, latency time.Duration, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err != nil {
		s.readErrors++
	} else {
		s.readOps++
		s.readBytes += bytes
		s.readLatencies = append(s.readLatencies, float64(latency.Milliseconds()))
	}
}

func (s *loadtestStats) recordVerify(success bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if success {
		s.verifySuccess++
	} else {
		s.verifyErrors++
	}
}

func (s *loadtestStats) recordVerifyTimeout() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.verifyTimeouts++
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func (s *loadtestStats) summary(duration time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	green := color.New(color.FgGreen).SprintFunc()
	red := color.New(color.FgRed).SprintFunc()
	yellow := color.New(color.FgYellow).SprintFunc()
	cyan := color.New(color.FgCyan).SprintFunc()

	fmt.Println()
	fmt.Println(cyan("═══════════════════════════════════════════════════════════════"))
	fmt.Println(cyan("                      LOAD TEST RESULTS"))
	fmt.Println(cyan("═══════════════════════════════════════════════════════════════"))
	fmt.Println()

	// Operations
	fmt.Println(yellow("Operations:"))
	fmt.Printf("  Write ops:      %s (%d multipart)\n", green(s.writeOps), s.multipartOps)
	fmt.Printf("  Read ops:       %s\n", green(s.readOps))
	fmt.Printf("  Write errors:   %s\n", red(s.writeErrors))
	fmt.Printf("  Read errors:    %s\n", red(s.readErrors))
	fmt.Println()

	// Throughput
	durationSec := duration.Seconds()
	fmt.Println(yellow("Throughput:"))
	fmt.Printf("  Write:          %s (%s total)\n",
		green(formatRate(float64(s.writeBytes)/durationSec)),
		formatSize(s.writeBytes))
	fmt.Printf("  Read:           %s (%s total)\n",
		green(formatRate(float64(s.readBytes)/durationSec)),
		formatSize(s.readBytes))
	fmt.Printf("  Write ops/sec:  %s\n", green(fmt.Sprintf("%.2f", float64(s.writeOps)/durationSec)))
	fmt.Printf("  Read ops/sec:   %s\n", green(fmt.Sprintf("%.2f", float64(s.readOps)/durationSec)))
	fmt.Println()

	// Latencies
	fmt.Println(yellow("Latency (ms):"))
	if len(s.writeLatencies) > 0 {
		sort.Float64s(s.writeLatencies)
		fmt.Printf("  Write p50:      %s\n", green(fmt.Sprintf("%.1f", percentile(s.writeLatencies, 0.50))))
		fmt.Printf("  Write p95:      %s\n", green(fmt.Sprintf("%.1f", percentile(s.writeLatencies, 0.95))))
		fmt.Printf("  Write p99:      %s\n", green(fmt.Sprintf("%.1f", percentile(s.writeLatencies, 0.99))))
	}
	if len(s.readLatencies) > 0 {
		sort.Float64s(s.readLatencies)
		fmt.Printf("  Read p50:       %s\n", green(fmt.Sprintf("%.1f", percentile(s.readLatencies, 0.50))))
		fmt.Printf("  Read p95:       %s\n", green(fmt.Sprintf("%.1f", percentile(s.readLatencies, 0.95))))
		fmt.Printf("  Read p99:       %s\n", green(fmt.Sprintf("%.1f", percentile(s.readLatencies, 0.99))))
	}
	fmt.Println()

	// Verification
	if s.verifySuccess > 0 || s.verifyErrors > 0 || s.verifyTimeouts > 0 {
		fmt.Println(yellow("Read-After-Write Verification:"))
		fmt.Printf("  Successful:     %s\n", green(s.verifySuccess))
		if s.verifyErrors > 0 {
			fmt.Printf("  Corrupted:      %s (DATA CORRUPTION DETECTED!)\n", red(s.verifyErrors))
		} else {
			fmt.Printf("  Corrupted:      %s\n", green(0))
		}
		if s.verifyTimeouts > 0 {
			fmt.Printf("  Timeouts:       %s (not corruption, test ended)\n", yellow(s.verifyTimeouts))
		}
		fmt.Println()
	}

	fmt.Println(cyan("═══════════════════════════════════════════════════════════════"))
}

// s3Client is a simple S3 client for load testing
type s3Client struct {
	endpoint   string
	bucket     string
	httpClient *http.Client
}

func newS3Client(endpoint, bucket string) *s3Client {
	return &s3Client{
		endpoint: strings.TrimSuffix(endpoint, "/"),
		bucket:   bucket,
		httpClient: &http.Client{
			Timeout: 5 * time.Minute,
		},
	}
}

func (c *s3Client) putObject(ctx context.Context, key string, data []byte) error {
	url := fmt.Sprintf("%s/%s/%s", c.endpoint, c.bucket, key)

	req, err := http.NewRequestWithContext(ctx, "PUT", url, bytes.NewReader(data))
	if err != nil {
		return xerrors.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("x-amz-content-sha256", "UNSIGNED-PAYLOAD")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return xerrors.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return xerrors.Errorf("PUT failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (c *s3Client) getObject(ctx context.Context, key string) ([]byte, error) {
	url := fmt.Sprintf("%s/%s/%s", c.endpoint, c.bucket, key)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, xerrors.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, xerrors.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return nil, xerrors.Errorf("GET failed with status %d: %s", resp.StatusCode, string(body))
	}

	return io.ReadAll(resp.Body)
}

func (c *s3Client) deleteObject(ctx context.Context, key string) error {
	url := fmt.Sprintf("%s/%s/%s", c.endpoint, c.bucket, key)

	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return xerrors.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return xerrors.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 && resp.StatusCode != 404 {
		body, _ := io.ReadAll(resp.Body)
		return xerrors.Errorf("DELETE failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// initiateMultipartUpload starts a multipart upload and returns the upload ID
func (c *s3Client) initiateMultipartUpload(ctx context.Context, key string) (string, error) {
	url := fmt.Sprintf("%s/%s/%s?uploads", c.endpoint, c.bucket, key)

	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return "", xerrors.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", xerrors.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return "", xerrors.Errorf("initiate multipart failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse XML response to get UploadId
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", xerrors.Errorf("read response: %w", err)
	}

	// Simple XML parsing - look for <UploadId>...</UploadId>
	bodyStr := string(body)
	start := strings.Index(bodyStr, "<UploadId>")
	end := strings.Index(bodyStr, "</UploadId>")
	if start == -1 || end == -1 {
		return "", xerrors.Errorf("UploadId not found in response: %s", bodyStr)
	}
	uploadID := bodyStr[start+10 : end]

	return uploadID, nil
}

// uploadPart uploads a part of a multipart upload and returns the ETag
func (c *s3Client) uploadPart(ctx context.Context, key, uploadID string, partNum int, data []byte) (string, error) {
	url := fmt.Sprintf("%s/%s/%s?partNumber=%d&uploadId=%s", c.endpoint, c.bucket, key, partNum, uploadID)

	req, err := http.NewRequestWithContext(ctx, "PUT", url, bytes.NewReader(data))
	if err != nil {
		return "", xerrors.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", xerrors.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return "", xerrors.Errorf("upload part failed with status %d: %s", resp.StatusCode, string(body))
	}

	etag := resp.Header.Get("ETag")
	if etag == "" {
		etag = fmt.Sprintf(`"part-%d"`, partNum) // fallback
	}

	return etag, nil
}

// completeMultipartUpload completes the multipart upload
func (c *s3Client) completeMultipartUpload(ctx context.Context, key, uploadID string, parts []string) error {
	url := fmt.Sprintf("%s/%s/%s?uploadId=%s", c.endpoint, c.bucket, key, uploadID)

	// Build XML body
	var sb strings.Builder
	sb.WriteString("<CompleteMultipartUpload>")
	for i, etag := range parts {
		sb.WriteString(fmt.Sprintf("<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", i+1, etag))
	}
	sb.WriteString("</CompleteMultipartUpload>")

	req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(sb.String()))
	if err != nil {
		return xerrors.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/xml")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return xerrors.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return xerrors.Errorf("complete multipart failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// putMultipartObject uploads an object using multipart upload
func (c *s3Client) putMultipartObject(ctx context.Context, key string, data []byte, partSize int) error {
	uploadID, err := c.initiateMultipartUpload(ctx, key)
	if err != nil {
		return xerrors.Errorf("initiate: %w", err)
	}

	var parts []string
	partNum := 1
	for offset := 0; offset < len(data); offset += partSize {
		end := offset + partSize
		if end > len(data) {
			end = len(data)
		}

		etag, err := c.uploadPart(ctx, key, uploadID, partNum, data[offset:end])
		if err != nil {
			return xerrors.Errorf("upload part %d: %w", partNum, err)
		}
		parts = append(parts, etag)
		partNum++
	}

	if err := c.completeMultipartUpload(ctx, key, uploadID, parts); err != nil {
		return xerrors.Errorf("complete: %w", err)
	}

	return nil
}

func runLoadtest(c *cli.Context) error {
	if c.NArg() != 1 {
		return cli.Exit("Usage: ritool loadtest run [endpoint URL]", 1)
	}

	endpoint := c.Args().First()
	bucket := c.String("bucket")
	concurrency := c.Int("concurrency")
	duration := c.Duration("duration")
	readRatio := c.Float64("read-ratio")
	multipartThreshold := c.Int("multipart-threshold")
	multipartPartSize := c.Int("multipart-part-size")
	verify := c.Bool("verify")
	cleanup := c.Bool("cleanup")
	verbose := c.Bool("verbose")

	minSize, err := parseSize(c.String("min-size"))
	if err != nil {
		return cli.Exit(fmt.Sprintf("Invalid min-size: %v", err), 1)
	}
	maxSize, err := parseSize(c.String("max-size"))
	if err != nil {
		return cli.Exit(fmt.Sprintf("Invalid max-size: %v", err), 1)
	}

	if minSize > maxSize {
		return cli.Exit("min-size cannot be greater than max-size", 1)
	}
	if readRatio < 0 || readRatio > 1 {
		return cli.Exit("read-ratio must be between 0.0 and 1.0", 1)
	}

	green := color.New(color.FgGreen).SprintFunc()
	cyan := color.New(color.FgCyan).SprintFunc()

	fmt.Println(cyan("═══════════════════════════════════════════════════════════════"))
	fmt.Println(cyan("                      S3 LOAD TEST"))
	fmt.Println(cyan("═══════════════════════════════════════════════════════════════"))
	fmt.Println()
	fmt.Printf("  Endpoint:       %s\n", green(endpoint))
	fmt.Printf("  Bucket:         %s\n", green(bucket))
	fmt.Printf("  Concurrency:    %s workers\n", green(concurrency))
	fmt.Printf("  Duration:       %s\n", green(duration))
	fmt.Printf("  Object sizes:   %s - %s\n", green(formatSize(minSize)), green(formatSize(maxSize)))
	fmt.Printf("  Read ratio:     %s\n", green(fmt.Sprintf("%.0f%%", readRatio*100)))
	fmt.Printf("  Multipart:      > %s (part size: %s)\n",
		green(formatSize(int64(multipartThreshold))),
		green(formatSize(int64(multipartPartSize))))
	fmt.Printf("  Verify R-A-W:   %s\n", green(verify))
	fmt.Println()

	client := newS3Client(endpoint, bucket)
	stats := &loadtestStats{}

	// Create sharded data generators for each worker to avoid contention
	// Use 4KB shards and 256 shards = 1MB of pre-generated random data per worker
	const shardSize = 4 * 1024
	const numShards = 256
	generators := make([]*ShardedDataGenerator, concurrency)
	for i := 0; i < concurrency; i++ {
		generators[i] = NewShardedDataGenerator(shardSize, numShards)
	}

	// Track objects written for read operations and cleanup
	var objectsMu sync.Mutex
	objects := make(map[string]*testObject)

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	// Progress bar
	bar := pb.New64(int64(duration.Seconds()))
	bar.SetUnits(pb.U_NO)
	bar.Prefix("Running load test ")
	bar.Start()

	// Start progress ticker
	startTime := time.Now()
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				elapsed := time.Since(startTime).Seconds()
				bar.Set64(int64(elapsed))
			}
		}
	}()

	// Worker function
	worker := func(workerID int, gen *ShardedDataGenerator) {
		opCount := atomic.Int64{}

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Decide operation type based on read ratio
			// But only do reads if we have objects to read
			objectsMu.Lock()
			numObjects := len(objects)
			objectsMu.Unlock()

			doRead := false
			if numObjects > 0 {
				doRead = gen.ShouldRead(readRatio)
			}

			if doRead {
				// Read operation
				objectsMu.Lock()
				// Pick a random object
				var obj *testObject
				targetIdx := gen.PickIndex(numObjects)
				i := 0
				for _, o := range objects {
					if i == targetIdx {
						obj = o
						break
					}
					i++
				}
				objectsMu.Unlock()

				if obj == nil {
					continue
				}

				start := time.Now()
				data, err := client.getObject(ctx, obj.key)
				latency := time.Since(start)

				if err != nil {
					if verbose {
						fmt.Printf("[W%d] READ ERROR %s: %v\n", workerID, obj.key, err)
					}
					stats.recordRead(0, latency, err)
					continue
				}

				stats.recordRead(int64(len(data)), latency, nil)

				if verbose {
					fmt.Printf("[W%d] READ %s (%s) in %v\n", workerID, obj.key, formatSize(int64(len(data))), latency)
				}

				// Verify if enabled
				if verify {
					actualChecksum := fmt.Sprintf("%x", md5.Sum(data))
					if actualChecksum != obj.checksum {
						if verbose {
							fmt.Printf("[W%d] VERIFY FAILED %s: expected %s, got %s\n",
								workerID, obj.key, obj.checksum, actualChecksum)
						}
						stats.recordVerify(false)
					} else {
						stats.recordVerify(true)
					}
				}
			} else {
				// Write operation
				op := opCount.Add(1)
				key := fmt.Sprintf("loadtest/w%d/obj%d-%d", workerID, op, time.Now().UnixNano())

				// Generate random size and data using optimized shard-based generator
				size := gen.GenerateSize(minSize, maxSize)
				data, checksum := gen.Generate(int(size))

				start := time.Now()
				var err error
				isMultipart := size > int64(multipartThreshold)

				if isMultipart {
					err = client.putMultipartObject(ctx, key, data, multipartPartSize)
				} else {
					err = client.putObject(ctx, key, data)
				}
				latency := time.Since(start)

				if err != nil {
					if verbose {
						fmt.Printf("[W%d] WRITE ERROR %s: %v\n", workerID, key, err)
					}
					if isMultipart {
						stats.recordMultipart(0, latency, err)
					} else {
						stats.recordWrite(0, latency, err)
					}
					continue
				}

				if isMultipart {
					stats.recordMultipart(size, latency, nil)
				} else {
					stats.recordWrite(size, latency, nil)
				}

				if verbose {
					mpLabel := ""
					if isMultipart {
						mpLabel = " [multipart]"
					}
					fmt.Printf("[W%d] WRITE %s (%s)%s in %v\n", workerID, key, formatSize(size), mpLabel, latency)
				}

				// Store object for later reads
				objectsMu.Lock()
				objects[key] = &testObject{
					key:      key,
					size:     size,
					checksum: checksum,
				}
				objectsMu.Unlock()

				// Immediate read-after-write verification
				if verify {
					readData, err := client.getObject(ctx, key)
					if err != nil {
						// Distinguish between timeouts and other errors
						if ctx.Err() != nil {
							// Context cancelled/timeout - not a data integrity issue
							stats.recordVerifyTimeout()
						} else {
							// Real error reading the object
							fmt.Printf("[W%d] VERIFY READ ERROR %s: %v\n", workerID, key, err)
							stats.recordVerify(false)
						}
					} else {
						actualChecksum := fmt.Sprintf("%x", md5.Sum(readData))
						if actualChecksum != checksum {
							// Actual data corruption detected!
							fmt.Printf("[W%d] DATA CORRUPTION %s: expected %s, got %s (wrote %d bytes, read %d bytes)\n",
								workerID, key, checksum, actualChecksum, size, len(readData))
							stats.recordVerify(false)
						} else {
							stats.recordVerify(true)
						}
					}
				}
			}
		}
	}

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int, gen *ShardedDataGenerator) {
			defer wg.Done()
			worker(id, gen)
		}(i, generators[i])
	}

	// Wait for completion
	wg.Wait()
	bar.Finish()

	actualDuration := time.Since(startTime)
	stats.summary(actualDuration)

	// Cleanup
	if cleanup && len(objects) > 0 {
		fmt.Printf("\nCleaning up %d test objects...\n", len(objects))
		cleanupBar := pb.New(len(objects))
		cleanupBar.Prefix("Deleting ")
		cleanupBar.Start()

		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cleanupCancel()

		deleted := 0
		for key := range objects {
			if err := client.deleteObject(cleanupCtx, key); err != nil {
				if verbose {
					fmt.Printf("Failed to delete %s: %v\n", key, err)
				}
			} else {
				deleted++
			}
			cleanupBar.Increment()
		}
		cleanupBar.Finish()
		fmt.Printf("Deleted %d/%d objects\n", deleted, len(objects))
	}

	// Return error if there were verification failures
	if stats.verifyErrors > 0 {
		return cli.Exit(fmt.Sprintf("VERIFICATION FAILED: %d read-after-write mismatches detected", stats.verifyErrors), 1)
	}

	return nil
}
