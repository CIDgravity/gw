package rbstor

import (
	"context"
	"crypto/rand"
	iface "github.com/lotus-web3/ribs"
	"github.com/lotus-web3/ribs/test"
	"github.com/multiformats/go-multihash"
	"github.com/test-go/testify/require"
	"math/big"
	"testing"
)

var testHarness *test.Harness

func TestMain(m *testing.M) {
	testHarness = test.NewHarness()
	defer testHarness.Stop()
	m.Run()
}

func TestYugabyteIndex(t *testing.T) {
	host, err := testHarness.GetYugabyteHost()
	require.NoError(t, err)
	idx, err := NewYugabyteIndex([]string{host}, test.YugabytePort, "test", false)
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
	host, err := testHarness.GetYugabyteHost()
	require.NoError(t, err)
	idx, err := NewYugabyteIndex([]string{host}, test.YugabytePort, "test", false)
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
	host, err := testHarness.GetYugabyteHost()
	require.NoError(t, err)
	idx, err := NewYugabyteIndex([]string{host}, test.YugabytePort, "test", false)
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
