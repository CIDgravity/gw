package migrate

import (
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/CIDgravity/filecoin-gateway/migrate/goldenrepo"
	"github.com/stretchr/testify/require"
)

const goldenSeed = 42

// expectedPairs returns the number of (multihash, group) pairs the golden
// repo's top-level index holds.
func expectedPairs(m *goldenrepo.Manifest) int64 {
	var n int64
	for _, b := range m.Blocks {
		n += int64(len(b.Groups))
	}
	return n
}

func TestGoldenPebbleScan(t *testing.T) {
	src := filepath.Join(t.TempDir(), "src")
	m, err := goldenrepo.Generate(src, goldenSeed)
	require.NoError(t, err)

	pdb, err := openSourcePebble(src, t.TempDir())
	require.NoError(t, err)
	defer pdb.Close() // nolint:errcheck

	want := map[string]goldenrepo.Block{}
	for _, b := range m.Blocks {
		want[string(b.Mh)] = b
	}

	got := map[string][]int64{}
	st, err := scanTopIndex(pdb, nil, func(mh []byte, size int32, groups []int64) error {
		b, ok := want[string(mh)]
		require.True(t, ok, "unexpected multihash in index")
		require.Equal(t, int32(len(b.Data)), size)

		got[string(mh)] = append([]int64{}, groups...)
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, int64(len(m.Blocks)), st.Multihashes)
	require.Equal(t, expectedPairs(m), st.Pairs)
	require.Zero(t, st.MissingSize)

	require.Len(t, got, len(m.Blocks))
	for _, b := range m.Blocks {
		groups := got[string(b.Mh)]
		slices.Sort(groups)
		wantGroups := append([]int64{}, b.Groups...)
		slices.Sort(wantGroups)
		require.Equal(t, wantGroups, groups, "groups for block %s", b.Mh)
	}
}

func TestGoldenPebbleScanResume(t *testing.T) {
	src := filepath.Join(t.TempDir(), "src")
	m, err := goldenrepo.Generate(src, goldenSeed)
	require.NoError(t, err)

	pdb, err := openSourcePebble(src, t.TempDir())
	require.NoError(t, err)
	defer pdb.Close() // nolint:errcheck

	// collect the full ordered multihash list first
	var order [][]byte
	_, err = scanTopIndex(pdb, nil, func(mh []byte, size int32, groups []int64) error {
		order = append(order, append([]byte{}, mh...))
		return nil
	})
	require.NoError(t, err)
	require.Len(t, order, len(m.Blocks))

	// resuming from the k-th multihash must yield exactly the tail,
	// starting with the checkpoint multihash itself
	k := len(order) / 2
	var resumed [][]byte
	st, err := scanTopIndex(pdb, order[k], func(mh []byte, size int32, groups []int64) error {
		resumed = append(resumed, append([]byte{}, mh...))
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, int64(len(order)-k), st.Multihashes)
	require.Equal(t, order[k:], resumed)
}

func TestGoldenSQLiteRead(t *testing.T) {
	src := filepath.Join(t.TempDir(), "src")
	m, err := goldenrepo.Generate(src, goldenSeed)
	require.NoError(t, err)

	db, err := openSourceSQLite(src, t.TempDir())
	require.NoError(t, err)
	defer db.Close() // nolint:errcheck

	counts := map[string]int64{}
	for _, table := range []string{"groups", "offloads", "deals", "deals_archive", "providers", "offloads_s3", "external_path", "repairs"} {
		var n int64
		require.NoError(t, db.QueryRow(`select count(*) from `+table).Scan(&n))
		counts[table] = n
	}

	require.Equal(t, int64(len(m.GroupStates)), counts["groups"])
	require.Equal(t, int64(1), counts["offloads"])
	require.Equal(t, int64(len(m.Deals)), counts["deals"])
	require.Equal(t, int64(m.ArchivedDeals), counts["deals_archive"])
	require.Equal(t, int64(m.Providers), counts["providers"])
	require.Equal(t, int64(0), counts["offloads_s3"])
	require.Equal(t, int64(1), counts["external_path"])
	require.Equal(t, int64(m.Repairs), counts["repairs"])

	// the columns the migrator expects must all be present in a
	// freshly-generated old-schema database
	for _, spec := range tableSpecs {
		present, err := sqliteColumns(db, spec.name)
		require.NoError(t, err)
		for _, c := range spec.cols {
			require.True(t, present[c.name], "column %s.%s missing in old schema", spec.name, c.name)
		}
	}
}

func TestMigrateTree(t *testing.T) {
	src := filepath.Join(t.TempDir(), "src")
	m, err := goldenrepo.Generate(src, goldenSeed)
	require.NoError(t, err)

	dest := filepath.Join(t.TempDir(), "dest")
	require.NoError(t, os.MkdirAll(dest, 0755))

	st, err := migrateTree(src, dest, false, nil)
	require.NoError(t, err)
	require.Positive(t, st.Files)
	require.Equal(t, st.Files, st.Linked+st.Copied)

	// databases replaced by Yugabyte must not be carried over
	for _, name := range []string{"index.pebble", "store.db", "store.db-wal", "store.db-shm"} {
		_, err := os.Lstat(filepath.Join(dest, name))
		require.True(t, os.IsNotExist(err), "%s must not be replicated", name)
	}

	// group data and unknown files must be carried over; hardlinking must
	// point at the same inode (t.TempDir subdirs share a filesystem)
	checks := append([]string{
		filepath.Join("grp", "1", "blklog.car"),
		filepath.Join("grp", "2", "blklog.meta", "head"),
		filepath.Join("grp", "3", "blklog.meta", "sample.mhlist"),
	}, m.ExtraFiles...)

	for _, rel := range checks {
		si, err := os.Stat(filepath.Join(src, rel))
		require.NoError(t, err, rel)
		di, err := os.Stat(filepath.Join(dest, rel))
		require.NoError(t, err, rel)
		require.True(t, os.SameFile(si, di), "%s should be hardlinked", rel)
	}

	// the offloaded group must not have a data file (and so neither may the copy)
	_, err = os.Lstat(filepath.Join(dest, "grp", "3", "blklog.car"))
	require.True(t, os.IsNotExist(err))

	// re-running is a cheap no-op
	st2, err := migrateTree(src, dest, false, nil)
	require.NoError(t, err)
	require.Equal(t, st.Files, st2.Files)
	require.Zero(t, st2.Linked)
	require.Zero(t, st2.Copied)
}

func TestMigrateTreeForceCopy(t *testing.T) {
	src := filepath.Join(t.TempDir(), "src")
	_, err := goldenrepo.Generate(src, goldenSeed)
	require.NoError(t, err)

	dest := filepath.Join(t.TempDir(), "dest")
	require.NoError(t, os.MkdirAll(dest, 0755))

	// group 1 is writable in the golden repo: its files must not share
	// inodes with the source
	forceCopy := func(rel string) bool {
		return strings.HasPrefix(rel, filepath.Join("grp", "1")+string(filepath.Separator))
	}

	_, err = migrateTree(src, dest, false, forceCopy)
	require.NoError(t, err)

	mutable := filepath.Join("grp", "1", "blklog.car")
	si, err := os.Stat(filepath.Join(src, mutable))
	require.NoError(t, err)
	di, err := os.Stat(filepath.Join(dest, mutable))
	require.NoError(t, err)
	require.False(t, os.SameFile(si, di), "mutable group data must be copied")
	require.Equal(t, si.Size(), di.Size())

	immutable := filepath.Join("grp", "2", "blklog.car")
	si, err = os.Stat(filepath.Join(src, immutable))
	require.NoError(t, err)
	di, err = os.Stat(filepath.Join(dest, immutable))
	require.NoError(t, err)
	require.True(t, os.SameFile(si, di), "finalized group data can be hardlinked")
}

func TestMigrateTreeCopyMode(t *testing.T) {
	src := filepath.Join(t.TempDir(), "src")
	_, err := goldenrepo.Generate(src, goldenSeed)
	require.NoError(t, err)

	dest := filepath.Join(t.TempDir(), "dest")
	require.NoError(t, os.MkdirAll(dest, 0755))

	st, err := migrateTree(src, dest, true, nil)
	require.NoError(t, err)
	require.Zero(t, st.Linked)
	require.Equal(t, st.Files, st.Copied)

	rel := filepath.Join("grp", "1", "blklog.car")
	si, err := os.Stat(filepath.Join(src, rel))
	require.NoError(t, err)
	di, err := os.Stat(filepath.Join(dest, rel))
	require.NoError(t, err)
	require.False(t, os.SameFile(si, di))
	require.Equal(t, si.Size(), di.Size())
}

func TestStateRoundtrip(t *testing.T) {
	dir := t.TempDir()

	s, err := loadState(dir)
	require.NoError(t, err)
	require.False(t, s.SQLDone)

	s.SQLDone = true
	s.IndexCheckpoint = "abcd"
	s.IndexEntries = 1234
	require.NoError(t, s.save(dir))

	s2, err := loadState(dir)
	require.NoError(t, err)
	require.Equal(t, s, s2)
}

func TestConvertValue(t *testing.T) {
	cases := []struct {
		name    string
		kind    colKind
		in      interface{}
		out     interface{}
		clamped bool
		err     bool
	}{
		{"null int", kInt, nil, nil, false, false},
		{"int", kInt, int64(42), int64(42), false, false},
		{"float in range", kNullInt, float64(1e18), int64(1e18), false, false},
		{"float above int64", kInt, float64(1.23e20), int64(math.MaxInt64), true, false},
		{"float below int64", kInt, float64(-1e20), int64(math.MinInt64), true, false},
		{"bool from float", kBool, float64(1), true, false, false},
		{"bool from int", kBool, int64(0), false, false, false},
		{"time from int", kTimeUnix, int64(1700000000), time.Unix(1700000000, 0).UTC(), false, false},
		{"text", kText, "hello", "hello", false, false},
		{"text from bytes", kNullText, []byte("hi"), "hi", false, false},
		{"blob", kBlob, []byte{1, 2}, []byte{1, 2}, false, false},
		{"text into int", kInt, "nope", nil, false, true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, clamped, err := convertValue(c.kind, c.in)
			if c.err {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, c.out, out)
			require.Equal(t, c.clamped, clamped)
		})
	}

	// blob conversion must copy: the driver may reuse the buffer while the
	// row is still queued for insertion
	src := []byte{1, 2, 3}
	out, _, err := convertValue(kBlob, src)
	require.NoError(t, err)
	src[0] = 9
	require.Equal(t, []byte{1, 2, 3}, out)
}

func TestMhScanProgress(t *testing.T) {
	// sha2-256 multihash: 0x12 0x20 header then the digest; position comes
	// from the leading digest bytes
	mid := append([]byte{0x12, 0x20, 0x80, 0, 0, 0, 0, 0, 0, 0}, make([]byte, 24)...)
	require.InDelta(t, 0.5, mhScanProgress(mid), 0.01)

	low := append([]byte{0x12, 0x20}, make([]byte, 32)...)
	require.InDelta(t, 0.0, mhScanProgress(low), 0.001)

	high := append([]byte{0x12, 0x20, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, make([]byte, 24)...)
	require.InDelta(t, 1.0, mhScanProgress(high), 0.001)

	// too short to carry a digest window
	require.Zero(t, mhScanProgress([]byte{1, 2, 3}))
	require.Zero(t, mhScanProgress(nil))

	// progress increases with scan order
	require.Less(t, mhScanProgress(low), mhScanProgress(mid))
	require.Less(t, mhScanProgress(mid), mhScanProgress(high))
}
