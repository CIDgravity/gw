package carlog

import (
	"crypto/rand"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/CIDgravity/filecoin-gateway/test"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

// helper: generate N distinct SHA2-256 multihashes from random data
func genMultihashes(t *testing.T, n int) []multihash.Multihash {
	t.Helper()
	out := make([]multihash.Multihash, n)
	for i := range out {
		buf := make([]byte, 32)
		_, err := rand.Read(buf)
		require.NoError(t, err)
		mh, err := multihash.Sum(buf, multihash.SHA2_256, -1)
		require.NoError(t, err)
		out[i] = mh
	}
	return out
}

// -------- Unit tests for WalIndex --------

func TestWalIndex_CreateAndLookup(t *testing.T) {
	p := filepath.Join(t.TempDir(), "idx.wal")
	idx, err := CreateWalIndex(p)
	require.NoError(t, err)

	mhs := genMultihashes(t, 10)
	offs := make([]int64, 10)
	for i := range offs {
		offs[i] = makeOffsetLen(int64(i*1000), 100+i)
	}

	require.NoError(t, idx.Put(mhs, offs))

	// Has
	has, err := idx.Has(mhs)
	require.NoError(t, err)
	for i, h := range has {
		require.True(t, h, "expected mh %d to exist", i)
	}

	// Get
	got, err := idx.Get(mhs)
	require.NoError(t, err)
	for i, v := range got {
		require.Equal(t, offs[i], v)
		o, l := fromOffsetLen(v)
		require.Equal(t, int64(i*1000), o)
		require.Equal(t, 100+i, l)
	}

	require.NoError(t, idx.Close())
}

func TestWalIndex_NotFound(t *testing.T) {
	p := filepath.Join(t.TempDir(), "idx.wal")
	idx, err := CreateWalIndex(p)
	require.NoError(t, err)

	mhs := genMultihashes(t, 5)

	has, err := idx.Has(mhs)
	require.NoError(t, err)
	for _, h := range has {
		require.False(t, h)
	}

	got, err := idx.Get(mhs)
	require.NoError(t, err)
	for _, v := range got {
		require.Equal(t, int64(-1), v)
	}

	require.NoError(t, idx.Close())
}

func TestWalIndex_Dedup(t *testing.T) {
	p := filepath.Join(t.TempDir(), "idx.wal")
	idx, err := CreateWalIndex(p)
	require.NoError(t, err)

	mhs := genMultihashes(t, 1)

	// Write same mh twice with different offsets
	require.NoError(t, idx.Put(mhs, []int64{makeOffsetLen(100, 50)}))
	require.NoError(t, idx.Put(mhs, []int64{makeOffsetLen(200, 60)}))

	// Should have last-write-wins
	got, err := idx.Get(mhs)
	require.NoError(t, err)
	o, l := fromOffsetLen(got[0])
	require.Equal(t, int64(200), o)
	require.Equal(t, 60, l)

	// Count should be 1 (not 2)
	ents, err := idx.Entries()
	require.NoError(t, err)
	require.Equal(t, int64(1), ents)

	require.NoError(t, idx.Close())
}

func TestWalIndex_SkipMinusOne(t *testing.T) {
	p := filepath.Join(t.TempDir(), "idx.wal")
	idx, err := CreateWalIndex(p)
	require.NoError(t, err)

	mhs := genMultihashes(t, 3)
	offs := []int64{makeOffsetLen(100, 10), -1, makeOffsetLen(300, 30)}

	require.NoError(t, idx.Put(mhs, offs))

	has, err := idx.Has(mhs)
	require.NoError(t, err)
	require.True(t, has[0])
	require.False(t, has[1]) // skipped
	require.True(t, has[2])

	ents, err := idx.Entries()
	require.NoError(t, err)
	require.Equal(t, int64(2), ents)

	require.NoError(t, idx.Close())
}

func TestWalIndex_Entries(t *testing.T) {
	p := filepath.Join(t.TempDir(), "idx.wal")
	idx, err := CreateWalIndex(p)
	require.NoError(t, err)

	ents, err := idx.Entries()
	require.NoError(t, err)
	require.Equal(t, int64(0), ents)

	mhs := genMultihashes(t, 100)
	offs := make([]int64, 100)
	for i := range offs {
		offs[i] = makeOffsetLen(int64(i*100), 50)
	}
	require.NoError(t, idx.Put(mhs, offs))

	ents, err = idx.Entries()
	require.NoError(t, err)
	require.Equal(t, int64(100), ents)

	require.NoError(t, idx.Close())
}

func TestWalIndex_List(t *testing.T) {
	p := filepath.Join(t.TempDir(), "idx.wal")
	idx, err := CreateWalIndex(p)
	require.NoError(t, err)

	mhs := genMultihashes(t, 50)
	offs := make([]int64, 50)
	for i := range offs {
		offs[i] = makeOffsetLen(int64(i*200), 80)
	}
	require.NoError(t, idx.Put(mhs, offs))

	// Build reference map
	ref := make(map[string]int64)
	for i, mh := range mhs {
		ref[string([]byte(mh))] = offs[i]
	}

	// List and verify
	seen := 0
	err = idx.List(func(c multihash.Multihash, o []int64) error {
		require.Len(t, o, 1)
		expected, ok := ref[string([]byte(c))]
		require.True(t, ok, "unexpected mh in List")
		require.Equal(t, expected, o[0])
		seen++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 50, seen)

	require.NoError(t, idx.Close())
}

func TestWalIndex_ToTruncate(t *testing.T) {
	p := filepath.Join(t.TempDir(), "idx.wal")
	idx, err := CreateWalIndex(p)
	require.NoError(t, err)

	mhs := genMultihashes(t, 10)
	offs := make([]int64, 10)
	for i := range offs {
		offs[i] = makeOffsetLen(int64(i*1000), 50)
	}
	require.NoError(t, idx.Put(mhs, offs))

	// Entries at offsets 5000..9000 should be truncated
	trunc, err := idx.ToTruncate(5000)
	require.NoError(t, err)
	require.Len(t, trunc, 5) // offsets 5000, 6000, 7000, 8000, 9000

	require.NoError(t, idx.Close())
}

func TestWalIndex_Del(t *testing.T) {
	p := filepath.Join(t.TempDir(), "idx.wal")
	idx, err := CreateWalIndex(p)
	require.NoError(t, err)

	mhs := genMultihashes(t, 10)
	offs := make([]int64, 10)
	for i := range offs {
		offs[i] = makeOffsetLen(int64(i*100), 20)
	}
	require.NoError(t, idx.Put(mhs, offs))

	// Delete first 5
	require.NoError(t, idx.Del(mhs[:5]))

	has, err := idx.Has(mhs)
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		require.False(t, has[i])
	}
	for i := 5; i < 10; i++ {
		require.True(t, has[i])
	}

	ents, err := idx.Entries()
	require.NoError(t, err)
	require.Equal(t, int64(5), ents)

	require.NoError(t, idx.Close())
}

// -------- Close/Reopen (WAL replay) tests --------

func TestWalIndex_CloseReopen(t *testing.T) {
	p := filepath.Join(t.TempDir(), "idx.wal")

	mhs := genMultihashes(t, 100)
	offs := make([]int64, 100)
	for i := range offs {
		offs[i] = makeOffsetLen(int64(i*500), 40+i%50)
	}

	// Write and close
	idx, err := CreateWalIndex(p)
	require.NoError(t, err)
	require.NoError(t, idx.Put(mhs, offs))
	require.NoError(t, idx.Sync())
	require.NoError(t, idx.Close())

	// Reopen — all entries present (truncateAt=-1 means no truncation)
	idx, err = OpenWalIndex(p, -1)
	require.NoError(t, err)

	got, err := idx.Get(mhs)
	require.NoError(t, err)
	for i, v := range got {
		require.Equal(t, offs[i], v, "mismatch at entry %d", i)
	}

	ents, err := idx.Entries()
	require.NoError(t, err)
	require.Equal(t, int64(100), ents)

	require.NoError(t, idx.Close())
}

func TestWalIndex_CloseReopenLargeN(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large test in short mode")
	}

	p := filepath.Join(t.TempDir(), "idx.wal")
	const N = 200_000

	mhs := genMultihashes(t, N)
	offs := make([]int64, N)
	for i := range offs {
		offs[i] = makeOffsetLen(int64(i*100), 64)
	}

	idx, err := CreateWalIndex(p)
	require.NoError(t, err)

	// Write in batches of 10K
	for start := 0; start < N; start += 10_000 {
		end := start + 10_000
		if end > N {
			end = N
		}
		require.NoError(t, idx.Put(mhs[start:end], offs[start:end]))
	}
	require.NoError(t, idx.Sync())
	require.NoError(t, idx.Close())

	// Reopen
	idx, err = OpenWalIndex(p, -1)
	require.NoError(t, err)

	ents, err := idx.Entries()
	require.NoError(t, err)
	require.Equal(t, int64(N), ents)

	// Spot-check some entries
	spot := []int{0, 1, N / 2, N - 1}
	spotMhs := make([]multihash.Multihash, len(spot))
	for i, s := range spot {
		spotMhs[i] = mhs[s]
	}
	got, err := idx.Get(spotMhs)
	require.NoError(t, err)
	for i, s := range spot {
		require.Equal(t, offs[s], got[i])
	}

	require.NoError(t, idx.Close())
}

func TestWalIndex_ReopenWithTruncation(t *testing.T) {
	p := filepath.Join(t.TempDir(), "idx.wal")

	mhs := genMultihashes(t, 20)
	offs := make([]int64, 20)
	for i := range offs {
		offs[i] = makeOffsetLen(int64(i*1000), 50)
	}

	idx, err := CreateWalIndex(p)
	require.NoError(t, err)
	require.NoError(t, idx.Put(mhs, offs))
	require.NoError(t, idx.Sync())
	require.NoError(t, idx.Close())

	// Reopen with truncateAt=10000 — entries at offsets 10000..19000 are discarded
	idx, err = OpenWalIndex(p, 10000)
	require.NoError(t, err)

	ents, err := idx.Entries()
	require.NoError(t, err)
	require.Equal(t, int64(10), ents)

	has, err := idx.Has(mhs)
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		require.True(t, has[i], "entry %d (offset %d) should survive", i, i*1000)
	}
	for i := 10; i < 20; i++ {
		require.False(t, has[i], "entry %d (offset %d) should be truncated", i, i*1000)
	}

	require.NoError(t, idx.Close())
}

// -------- Crash simulation tests --------

func TestWalCrash_TornWriteAfterMhLen(t *testing.T) {
	p := filepath.Join(t.TempDir(), "idx.wal")

	idx, err := CreateWalIndex(p)
	require.NoError(t, err)

	mhs := genMultihashes(t, 5)
	offs := make([]int64, 5)
	for i := range offs {
		offs[i] = makeOffsetLen(int64(i*100), 30)
	}
	require.NoError(t, idx.Put(mhs, offs))
	require.NoError(t, idx.Sync())
	require.NoError(t, idx.Close())

	// Corrupt: append a partial entry (just 2 bytes of mh_len, no body)
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND, 0666)
	require.NoError(t, err)
	_, err = f.Write([]byte{0x22, 0x00}) // mh_len=34 but no mh bytes follow
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Reopen — should recover the 5 valid entries, ignore the torn one
	idx, err = OpenWalIndex(p, -1)
	require.NoError(t, err)

	ents, err := idx.Entries()
	require.NoError(t, err)
	require.Equal(t, int64(5), ents)

	got, err := idx.Get(mhs)
	require.NoError(t, err)
	for i, v := range got {
		require.Equal(t, offs[i], v)
	}

	require.NoError(t, idx.Close())
}

func TestWalCrash_TornWriteMidMh(t *testing.T) {
	p := filepath.Join(t.TempDir(), "idx.wal")

	idx, err := CreateWalIndex(p)
	require.NoError(t, err)

	mhs := genMultihashes(t, 3)
	offs := make([]int64, 3)
	for i := range offs {
		offs[i] = makeOffsetLen(int64(i*500), 40)
	}
	require.NoError(t, idx.Put(mhs, offs))
	require.NoError(t, idx.Sync())
	require.NoError(t, idx.Close())

	// Append partial entry: mh_len header + half the multihash bytes
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND, 0666)
	require.NoError(t, err)
	var hdr [2]byte
	binary.LittleEndian.PutUint16(hdr[:], 34)
	_, err = f.Write(hdr[:])
	require.NoError(t, err)
	_, err = f.Write(make([]byte, 17)) // only half the multihash
	require.NoError(t, err)
	require.NoError(t, f.Close())

	idx, err = OpenWalIndex(p, -1)
	require.NoError(t, err)

	ents, err := idx.Entries()
	require.NoError(t, err)
	require.Equal(t, int64(3), ents)

	require.NoError(t, idx.Close())
}

func TestWalCrash_TornWriteMissingOffset(t *testing.T) {
	p := filepath.Join(t.TempDir(), "idx.wal")

	idx, err := CreateWalIndex(p)
	require.NoError(t, err)

	mhs := genMultihashes(t, 2)
	offs := []int64{makeOffsetLen(100, 20), makeOffsetLen(200, 30)}
	require.NoError(t, idx.Put(mhs, offs))
	require.NoError(t, idx.Sync())
	require.NoError(t, idx.Close())

	// Append: valid mh_len + full mh but truncated offset bytes
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND, 0666)
	require.NoError(t, err)
	var hdr [2]byte
	binary.LittleEndian.PutUint16(hdr[:], 34)
	_, err = f.Write(hdr[:])
	require.NoError(t, err)
	_, err = f.Write(make([]byte, 34)) // full mh
	require.NoError(t, err)
	_, err = f.Write(make([]byte, 3)) // only 3 of 8 offset bytes
	require.NoError(t, err)
	require.NoError(t, f.Close())

	idx, err = OpenWalIndex(p, -1)
	require.NoError(t, err)

	ents, err := idx.Entries()
	require.NoError(t, err)
	require.Equal(t, int64(2), ents)

	require.NoError(t, idx.Close())
}

func TestWalCrash_CorruptMagic(t *testing.T) {
	p := filepath.Join(t.TempDir(), "idx.wal")

	idx, err := CreateWalIndex(p)
	require.NoError(t, err)
	require.NoError(t, idx.Close())

	// Corrupt the magic
	f, err := os.OpenFile(p, os.O_WRONLY, 0666)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte("BADMAGIC"), 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = OpenWalIndex(p, -1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid wal magic")
}

func TestWalCrash_MagicOnly(t *testing.T) {
	p := filepath.Join(t.TempDir(), "idx.wal")

	idx, err := CreateWalIndex(p)
	require.NoError(t, err)
	require.NoError(t, idx.Close())

	idx, err = OpenWalIndex(p, -1)
	require.NoError(t, err)

	ents, err := idx.Entries()
	require.NoError(t, err)
	require.Equal(t, int64(0), ents)

	require.NoError(t, idx.Close())
}

func TestWalCrash_ZeroLengthFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "idx.wal")

	f, err := os.Create(p)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = OpenWalIndex(p, -1)
	require.Error(t, err) // can't read magic from 0-byte file
}

func TestWalCrash_TruncMidBatch(t *testing.T) {
	p := filepath.Join(t.TempDir(), "idx.wal")

	idx, err := CreateWalIndex(p)
	require.NoError(t, err)

	mhs := genMultihashes(t, 100)
	offs := make([]int64, 100)
	for i := range offs {
		offs[i] = makeOffsetLen(int64(i*100), 50)
	}
	require.NoError(t, idx.Put(mhs, offs))
	require.NoError(t, idx.Sync())
	require.NoError(t, idx.Close())

	// Each entry is 2 + 34 + 8 = 44 bytes, plus 8-byte magic header
	// First 50 entries: 8 + 50*44 = 2208 bytes
	require.NoError(t, os.Truncate(p, 8+50*44))

	idx, err = OpenWalIndex(p, -1)
	require.NoError(t, err)

	ents, err := idx.Entries()
	require.NoError(t, err)
	require.Equal(t, int64(50), ents)

	// First 50 should be present
	has, err := idx.Has(mhs[:50])
	require.NoError(t, err)
	for i, h := range has {
		require.True(t, h, "entry %d should exist", i)
	}

	// Rest should be gone
	has, err = idx.Has(mhs[50:])
	require.NoError(t, err)
	for i, h := range has {
		require.False(t, h, "entry %d should not exist", i+50)
	}

	require.NoError(t, idx.Close())
}

// -------- CarLog integration tests --------

func TestCarLog_CrashBeforeCommit(t *testing.T) {
	td := t.TempDir()
	tsp := &test.StagingProvider{}

	jb, err := Create(tsp, filepath.Join(td, "index"), td, nil)
	require.NoError(t, err)

	mhs := genMultihashes(t, 50)
	blks := genBlocksForMhs(t, mhs)

	require.NoError(t, jb.Put(mhSlice(mhs), blks))

	// Close WITHOUT committing — simulates crash before Commit
	require.NoError(t, jb.Close())

	jb, err = Open(tsp, filepath.Join(td, "index"), td, func(to int64, h []multihash.Multihash) error {
		return nil
	})
	require.NoError(t, err)

	// After recovery, uncommitted blocks should be gone
	has, err := jb.rIdx.Has(mhs)
	require.NoError(t, err)
	for i, h := range has {
		require.False(t, h, "entry %d should not exist after crash before commit", i)
	}

	require.NoError(t, jb.Close())
}

func TestCarLog_CommitThenReopen(t *testing.T) {
	td := t.TempDir()
	tsp := &test.StagingProvider{}

	jb, err := Create(tsp, filepath.Join(td, "index"), td, nil)
	require.NoError(t, err)

	mhs := genMultihashes(t, 50)
	blks := genBlocksForMhs(t, mhs)

	require.NoError(t, jb.Put(mhSlice(mhs), blks))
	_, err = jb.Commit()
	require.NoError(t, err)
	require.NoError(t, jb.Close())

	noTrunc := func(to int64, h []multihash.Multihash) error {
		require.Fail(t, "truncation should not be called for clean shutdown")
		return nil
	}

	jb, err = Open(tsp, filepath.Join(td, "index"), td, noTrunc)
	require.NoError(t, err)

	// All committed blocks should survive
	err = jb.View(mhs, func(i int, found bool, data []byte) error {
		require.True(t, found, "block %d should be found after commit+reopen", i)
		return nil
	})
	require.NoError(t, err)

	require.NoError(t, jb.Close())
}

func TestCarLog_RebuildFromDataFile(t *testing.T) {
	td := t.TempDir()
	tsp := &test.StagingProvider{}
	idxDir := filepath.Join(td, "index")

	jb, err := Create(tsp, idxDir, td, nil)
	require.NoError(t, err)

	mhs := genMultihashes(t, 100)
	blks := genBlocksForMhs(t, mhs)

	require.NoError(t, jb.Put(mhSlice(mhs), blks))
	_, err = jb.Commit()
	require.NoError(t, err)
	require.NoError(t, jb.Close())

	// Delete the WAL index — force a rebuild from the data file
	require.NoError(t, os.Remove(filepath.Join(idxDir, WalIndexFile)))

	noTrunc := func(to int64, h []multihash.Multihash) error {
		return nil
	}

	jb, err = Open(tsp, idxDir, td, noTrunc)
	require.NoError(t, err)

	// All blocks should be readable after rebuild
	err = jb.View(mhs, func(i int, found bool, data []byte) error {
		require.True(t, found, "block %d should be found after index rebuild", i)
		return nil
	})
	require.NoError(t, err)

	require.NoError(t, jb.Close())
}

// -------- helpers --------

func genBlocksForMhs(t *testing.T, mhs []multihash.Multihash) []blocks.Block {
	t.Helper()
	out := make([]blocks.Block, len(mhs))
	for i := range mhs {
		data := make([]byte, 64)
		_, err := rand.Read(data)
		require.NoError(t, err)
		// Create a proper block with CIDv1+Raw codec
		realMh, err := multihash.Sum(data, multihash.SHA2_256, -1)
		require.NoError(t, err)
		b, err := blocks.NewBlockWithCid(data, cid.NewCidV1(cid.Raw, realMh))
		require.NoError(t, err)
		out[i] = b
		// Overwrite the multihash in the caller's slice to match
		copy(mhs[i], realMh)
	}
	return out
}

func mhSlice(mhs []multihash.Multihash) []multihash.Multihash {
	return mhs
}
