package rbstor

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	"github.com/CIDgravity/filecoin-gateway/database/cqldb"
	"github.com/CIDgravity/filecoin-gateway/database/sqldb"
	"github.com/CIDgravity/filecoin-gateway/iface"
	blocks "github.com/ipfs/go-block-format"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func init() {
	// Set required environment variables for test configuration
	// This ensures configuration defaults are properly applied
	os.Setenv("RIBS_DATA", os.TempDir())
	os.Setenv("RIBS_MAX_LOCAL_GROUP_COUNT", "64")
	_ = configuration.LoadConfig()
}

// mockStagingProvider implements iface.StagingStorageProvider for testing
type mockStagingProvider struct {
	mu   sync.RWMutex
	cars map[iface.GroupKey][]byte
}

func newMockStagingProvider() *mockStagingProvider {
	return &mockStagingProvider{
		cars: make(map[iface.GroupKey][]byte),
	}
}

func (m *mockStagingProvider) Upload(ctx context.Context, group iface.GroupKey, size int64, src func(writer io.Writer) error) error {
	buf := new(bytes.Buffer)
	if err := src(buf); err != nil {
		return err
	}
	m.mu.Lock()
	m.cars[group] = buf.Bytes()
	m.mu.Unlock()
	return nil
}

func (m *mockStagingProvider) ReadCar(ctx context.Context, group iface.GroupKey, off, size int64) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.cars[group]
	if !ok {
		return nil, fmt.Errorf("car not found for group %d", group)
	}

	if off >= int64(len(data)) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}

	end := off + size
	if end > int64(len(data)) {
		end = int64(len(data))
	}

	return io.NopCloser(bytes.NewReader(data[off:end])), nil
}

func (m *mockStagingProvider) HasCar(ctx context.Context, group iface.GroupKey) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.cars[group]
	return ok, nil
}

// setupTestRBS creates a test RBS instance with real database connections.
// It uses the testHarness initialized in index_cql_test.go's TestMain.
func setupTestRBS(t *testing.T, td string) (iface.RBS, func()) {
	host := testHarness.GetYugabyteHost(t)
	cqlPort := testHarness.GetYugabyteCqlPort(t)
	sqlPort := testHarness.GetYugabyteSqlPort(t)

	// Create CQL database connection for the index
	cqlDb, err := cqldb.NewYugabyteCqlDb(configuration.YugabyteCqlConfig{
		Hosts:           host,
		Port:            cqlPort,
		Keyspace:        "filecoingw_test",
		ForceHosts:      false,
		Timeout:         30,
		ConnectTimeout:  30,
		SocketKeepalive: 30,
	})
	require.NoError(t, err)

	// Create SQL database connection for RbsDB
	sqlDb, err := sqldb.NewYugabyteDB(configuration.YugabyteSqlConfig{
		Host: host,
		Port: sqlPort,
		User: "yugabyte",
		Pass: "yugabyte",
		Db:   "filecoingw_test",
	})
	require.NoError(t, err)

	// Create the index
	idx, err := NewCqlIndex(cqlDb)
	require.NoError(t, err)

	// Create the RbsDB
	rbsDb := NewRibsDB(sqlDb)

	// Open the RBS
	ri, err := Open(&configuration.RibsConfig{DataDir: td}, rbsDb, idx)
	require.NoError(t, err)

	// Install mock staging provider
	mockStaging := newMockStagingProvider()
	ri.StagingStorage().InstallStagingProvider(mockStaging)

	cleanup := func() {
		if err := ri.Close(); err != nil {
			t.Logf("warning: failed to close RBS: %v", err)
		}
		if err := idx.Close(); err != nil {
			t.Logf("warning: failed to close index: %v", err)
		}
		// Clean up database state for test isolation
		// Delete all groups so next test starts fresh
		_, _ = sqlDb.Exec("DELETE FROM groups")
		if err := sqlDb.Close(); err != nil {
			t.Logf("warning: failed to close SQL db: %v", err)
		}
	}

	return ri, cleanup
}

func TestBasic(t *testing.T) {
	td := t.TempDir()
	t.Cleanup(func() {
		if err := filepath.Walk(td, func(path string, info fs.FileInfo, err error) error {
			t.Log(path)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})

	ctx := context.Background()

	ri, cleanup := setupTestRBS(t, td)
	defer cleanup()

	// Start the workers
	require.NoError(t, ri.Start())

	sess := ri.Session(ctx)

	wb := sess.Batch(ctx)

	b := blocks.NewBlock([]byte("hello world"))
	h := b.Cid().Hash()

	err := wb.Put(ctx, []blocks.Block{b})
	require.NoError(t, err)

	err = wb.Flush(ctx)
	require.NoError(t, err)

	err = sess.View(ctx, []multihash.Multihash{h}, func(i int, data []byte) {
		require.Equal(t, 0, i)
		require.Equal(t, []byte("hello world"), data)
	})
	require.NoError(t, err)
}

func TestMultipleBlocks(t *testing.T) {
	td := t.TempDir()
	ctx := context.Background()

	ri, cleanup := setupTestRBS(t, td)
	defer cleanup()

	// Start the workers
	require.NoError(t, ri.Start())

	sess := ri.Session(ctx)
	wb := sess.Batch(ctx)

	// Create multiple blocks
	testData := []string{"block1", "block2", "block3", "test data", "more test data"}
	testBlocks := make([]blocks.Block, len(testData))
	testHashes := make([]multihash.Multihash, len(testData))

	for i, data := range testData {
		testBlocks[i] = blocks.NewBlock([]byte(data))
		testHashes[i] = testBlocks[i].Cid().Hash()
	}

	// Put all blocks
	err := wb.Put(ctx, testBlocks)
	require.NoError(t, err)

	err = wb.Flush(ctx)
	require.NoError(t, err)

	// Verify all blocks can be retrieved
	retrieved := make(map[int][]byte)
	err = sess.View(ctx, testHashes, func(i int, data []byte) {
		retrieved[i] = append([]byte(nil), data...) // copy data
	})
	require.NoError(t, err)

	require.Len(t, retrieved, len(testData))
	for i, data := range testData {
		require.Equal(t, []byte(data), retrieved[i], "block %d mismatch", i)
	}
}

func TestFullGroup(t *testing.T) {
	t.Skip("worker gate needed to re-enable this test")
	maxGroupSize = 100 << 20

	td := t.TempDir()
	t.Cleanup(func() {
		if err := filepath.Walk(td, func(path string, info fs.FileInfo, err error) error {
			t.Log(path)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})

	ctx := context.Background()

	ri, cleanup := setupTestRBS(t, td)
	defer cleanup()

	// Start the workers
	require.NoError(t, ri.Start())

	sess := ri.Session(ctx)

	wb := sess.Batch(ctx)

	var h multihash.Multihash

	for i := 0; i < 500; i++ {
		var blk [200_000]byte
		binary.BigEndian.PutUint64(blk[:], uint64(i))

		b := blocks.NewBlock(blk[:])
		h = b.Cid().Hash()

		err := wb.Put(ctx, []blocks.Block{b})
		require.NoError(t, err)

		err = wb.Flush(ctx)
		require.NoError(t, err)
	}

	gs, err := ri.StorageDiag().GroupMeta(1)
	require.NoError(t, err)
	require.Equal(t, iface.GroupStateFull, gs.State)

	err = sess.View(ctx, []multihash.Multihash{h}, func(i int, data []byte) {
		require.Equal(t, 0, i)
		//require.Equal(t, b, []byte("hello world"))
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		gs, err := ri.StorageDiag().GroupMeta(1)
		require.NoError(t, err)
		fmt.Println("state now ", gs.State)
		return gs.State == iface.GroupStateVRCARDone
	}, 10*time.Second, 40*time.Millisecond)

	/*f, err := os.OpenFile("/tmp/ri.car", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	require.NoError(t, err)
	defer f.Close()

	err = ri.(*rbs).openGroups[1].writeCar(f)
	require.NoError(t, err)*/
}
