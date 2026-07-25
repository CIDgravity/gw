package rbstor

import (
	"context"
	"crypto/rand"
	"math/big"
	"testing"
	"time"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	"github.com/CIDgravity/filecoin-gateway/database/cqldb"
	"github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/CIDgravity/filecoin-gateway/test"
	"github.com/multiformats/go-multihash"
	"github.com/test-go/testify/require"
)

var testHarness *test.YugabyteHarness

func TestMain(m *testing.M) {
	testHarness = test.NewYugabyteHarness()
	defer testHarness.Stop()
	m.Run()
}

func TestYugabyteIndex(t *testing.T) {
	host := testHarness.GetYugabyteHost(t)
	port := testHarness.GetYugabyteCqlPort(t)

	db, err := cqldb.NewYugabyteCqlDb(configuration.YugabyteCqlConfig{
		Hosts:           host,
		Port:            port,
		Keyspace:        "filecoingw_test",
		ForceHosts:      false,
		Timeout:         30,
		ConnectTimeout:  30,
		SocketKeepalive: 30,
	})
	require.NoError(t, err)

	idx, err := NewCqlIndex(db)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, idx.Close())
	})

	mhs, sizes := genMhashList(t, 10)
	testGroup := iface.GroupKey(2)

	err = idx.AddGroup(context.Background(), mhs, sizes, testGroup)
	require.NoError(t, err)

	result := map[int][]iface.GroupKey{}
	err = idx.GetGroups(context.Background(), mhs, func(cidx int, group iface.GroupKey) (bool, error) {
		result[cidx] = append(result[cidx], group)
		return true, nil
	})
	require.NoError(t, err)

	for _, groupKeys := range result {
		require.Contains(t, groupKeys, testGroup)
	}

	err = idx.GetSizes(context.Background(), mhs, func(result []int32) error {
		require.Equal(t, sizes, result)
		return nil
	})
	require.NoError(t, err)

	err = idx.DropGroup(context.Background(), mhs, testGroup)
	require.NoError(t, err)

	err = idx.GetGroups(context.Background(), mhs, func(cidx int, group iface.GroupKey) (bool, error) {
		require.NotEqual(t, testGroup, group, "g %d should have been dropped", testGroup)
		return true, nil
	})
	require.NoError(t, err)
}

func TestMultipleGroupsPerHash(t *testing.T) {
	host := testHarness.GetYugabyteHost(t)
	port := testHarness.GetYugabyteCqlPort(t)
	db, err := cqldb.NewYugabyteCqlDb(configuration.YugabyteCqlConfig{
		Hosts:           host,
		Port:            port,
		Keyspace:        "filecoingw_test",
		ForceHosts:      false,
		Timeout:         30,
		ConnectTimeout:  30,
		SocketKeepalive: 30,
	})
	require.NoError(t, err)

	idx, err := NewCqlIndex(db)

	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, idx.Close())
	})

	mhs, sizes := genMhashList(t, 10)
	group1 := iface.GroupKey(2)
	group2 := iface.GroupKey(3)

	err = idx.AddGroup(context.Background(), mhs, sizes, group1)
	require.NoError(t, err)

	err = idx.AddGroup(context.Background(), mhs, sizes, group2)
	require.NoError(t, err)

	result := map[int][]iface.GroupKey{}
	err = idx.GetGroups(context.Background(), mhs, func(cidx int, group iface.GroupKey) (bool, error) {
		result[cidx] = append(result[cidx], group)
		return true, nil
	})
	require.NoError(t, err)

	for _, groupKeys := range result {
		require.Contains(t, groupKeys, group1)
		require.Contains(t, groupKeys, group2)
	}

	err = idx.DropGroup(context.Background(), mhs, group1)
	require.NoError(t, err)

	err = idx.GetGroups(context.Background(), mhs, func(cidx int, group iface.GroupKey) (bool, error) {
		require.NotEqual(t, group1, group, "g %d should have been dropped", group1)
		require.Equal(t, group2, group, "g %d should not have been dropped", group2)
		return false, nil
	})
	require.NoError(t, err)
}

func TestEstimateSize(t *testing.T) {
	host := testHarness.GetYugabyteHost(t)
	port := testHarness.GetYugabyteCqlPort(t)
	db, err := cqldb.NewYugabyteCqlDb(configuration.YugabyteCqlConfig{
		Hosts:           host,
		Port:            port,
		Keyspace:        "filecoingw_test",
		ForceHosts:      false,
		Timeout:         30,
		ConnectTimeout:  30,
		SocketKeepalive: 30,
	})
	require.NoError(t, err)
	idx, err := NewCqlIndex(db)

	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, idx.Close())
	})

	initialSize, err := idx.EstimateSize(context.Background())
	require.NoError(t, err)

	mhs, sizes := genMhashList(t, 10)
	testGroup := iface.GroupKey(2)

	err = idx.AddGroup(context.Background(), mhs, sizes, testGroup)
	require.NoError(t, err)

	result, err := idx.EstimateSize(context.Background())
	require.NoError(t, err)
	require.Equal(t, initialSize+10, result)

	err = idx.DropGroup(context.Background(), mhs, testGroup)
	require.NoError(t, err)

	result, err = idx.EstimateSize(context.Background())
	require.NoError(t, err)
	require.Equal(t, initialSize, result)
}

func genMhashList(t testing.TB, count int) ([]multihash.Multihash, []int32) {
	const maxSize = 1 << 20 // 1 MiB
	maxSizeBigInt := big.NewInt(maxSize)
	mhashes := make([]multihash.Multihash, count)
	sizes := make([]int32, count)
	for i := 0; i < count; i++ {
		buf := make([]byte, 32)
		_, err := rand.Read(buf)
		if err != nil {
			panic(err)
		}
		mhash, err := multihash.Sum(buf, multihash.SHA2_256, -1)
		if err != nil {
			panic(err)
		}
		mhashes[i] = mhash
		size, err := rand.Int(rand.Reader, maxSizeBigInt)
		require.NoError(t, err)
		sizes[i] = int32(size.Int64())
	}
	return mhashes, sizes
}

// TestCqlIndexEstimateInit verifies the background entry-count estimate: a
// freshly opened index must converge on the table's row count without
// blocking construction (the count runs in partition_hash ranges so it also
// works on tables far larger than one query timeout allows).
func TestCqlIndexEstimateInit(t *testing.T) {
	host := testHarness.GetYugabyteHost(t)
	port := testHarness.GetYugabyteCqlPort(t)

	db, err := cqldb.NewYugabyteCqlDb(configuration.YugabyteCqlConfig{
		Hosts:           host,
		Port:            port,
		Keyspace:        "filecoingw_test",
		Timeout:         30,
		ConnectTimeout:  30,
		SocketKeepalive: 30,
	})
	require.NoError(t, err)

	require.NoError(t, db.Session().Query("TRUNCATE TABLE MultihashToGroup").Exec())

	seed, err := NewCqlIndex(db)
	require.NoError(t, err)
	mhs, sizes := genMhashList(t, 500)
	require.NoError(t, seed.AddGroup(context.Background(), mhs, sizes, iface.GroupKey(7)))

	// a second instance knows nothing and must count what is there
	idx, err := NewCqlIndex(db)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Session().Query("TRUNCATE TABLE MultihashToGroup").Exec())
	})

	deadline := time.Now().Add(60 * time.Second)
	for {
		n, err := idx.EstimateSize(context.Background())
		require.NoError(t, err)
		if n == int64(len(mhs)) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("estimate did not converge: got %d, want %d", n, len(mhs))
		}
		time.Sleep(200 * time.Millisecond)
	}
}
