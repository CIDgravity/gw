package migrate

import (
	"context"
	"io"
	"io/fs"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	"github.com/CIDgravity/filecoin-gateway/database/cqldb"
	"github.com/CIDgravity/filecoin-gateway/database/sqldb"
	"github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/CIDgravity/filecoin-gateway/migrate/goldenrepo"
	"github.com/CIDgravity/filecoin-gateway/rbstor"
	"github.com/CIDgravity/filecoin-gateway/test"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"github.com/yugabyte/gocql"
	"golang.org/x/xerrors"
)

func init() {
	os.Setenv("RIBS_DATA", os.TempDir())
	_ = configuration.LoadConfig()
}

// The Yugabyte container is started lazily so the plain unit tests in this
// package keep working without docker.
var (
	harnessOnce sync.Once
	harness     *test.YugabyteHarness
)

func TestMain(m *testing.M) {
	code := m.Run()
	if harness != nil {
		harness.Stop()
	}
	os.Exit(code)
}

func getHarness(t *testing.T) *test.YugabyteHarness {
	if os.Getenv("MIGRATE_SKIP_YB_TESTS") == "1" {
		t.Skip("MIGRATE_SKIP_YB_TESTS set")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available")
	}
	harnessOnce.Do(func() {
		harness = test.NewYugabyteHarness()
	})
	return harness
}

func dialTargets(t *testing.T) (sqldb.Database, cqldb.Database) {
	h := getHarness(t)

	sqlDb, err := sqldb.NewYugabyteDB(configuration.YugabyteSqlConfig{
		Host: h.GetYugabyteHost(t),
		Port: h.GetYugabyteSqlPort(t),
		User: "yugabyte",
		Pass: "yugabyte",
		Db:   "filecoingw_test",
	})
	require.NoError(t, err)

	cqlDb, err := cqldb.NewYugabyteCqlDb(configuration.YugabyteCqlConfig{
		Hosts:           h.GetYugabyteHost(t),
		Port:            h.GetYugabyteCqlPort(t),
		Keyspace:        "filecoingw_test",
		Timeout:         30,
		ConnectTimeout:  30,
		SocketKeepalive: 30,
	})
	require.NoError(t, err)

	return sqlDb, cqlDb
}

// cleanTargets empties everything the migrator writes, isolating tests that
// share the Yugabyte container.
func cleanTargets(t *testing.T, sqlDb sqldb.Database, cqlDb cqldb.Database) {
	require.NoError(t, cqlDb.Session().Query("TRUNCATE TABLE MultihashToGroup").Exec())
	for i := len(tableSpecs) - 1; i >= 0; i-- {
		_, err := sqlDb.Exec("delete from " + tableSpecs[i].name)
		require.NoError(t, err)
	}
}

func TestMigrateGolden(t *testing.T) {
	sqlDb, cqlDb := dialTargets(t)
	cleanTargets(t, sqlDb, cqlDb)

	ctx := context.Background()

	src := filepath.Join(t.TempDir(), "src")
	golden, err := goldenrepo.Generate(src, goldenSeed)
	require.NoError(t, err)

	dest := filepath.Join(t.TempDir(), "dest")

	opts := Options{
		SourceDir: src,
		DestDir:   dest,
		SQL:       sqlDb,
		CQL:       cqlDb,
		Workers:   4,
		BatchSize: 16,
	}

	summary, err := Run(ctx, opts)
	require.NoError(t, err)
	require.False(t, summary.Resumed)

	t.Run("summary", func(t *testing.T) {
		require.Equal(t, int64(len(golden.GroupStates)), summary.SQLRows["groups"])
		require.Equal(t, int64(len(golden.Deals)), summary.SQLRows["deals"])
		require.Equal(t, int64(golden.ArchivedDeals), summary.SQLRows["deals_archive"])
		require.Equal(t, int64(golden.Providers), summary.SQLRows["providers"])
		require.Equal(t, int64(golden.Repairs), summary.SQLRows["repairs"])
		require.Equal(t, int64(1), summary.SQLRows["offloads"])
		require.Equal(t, int64(1), summary.SQLRows["external_path"])
		require.Equal(t, int64(0), summary.SQLRows["offloads_s3"])

		require.Equal(t, int64(len(golden.Blocks)), summary.Index.Multihashes)
		require.Equal(t, expectedPairs(golden), summary.Index.Pairs)
		require.Zero(t, summary.Index.MissingSize)
	})

	t.Run("sql-contents", func(t *testing.T) {
		// groups arrive with ids, states and carlog heads intact
		for gid, state := range golden.GroupStates {
			var gState, blocks int64
			require.NoError(t, sqlDb.QueryRow(`select g_state, blocks from groups where id = $1`, gid).Scan(&gState, &blocks))
			require.Equal(t, state, gState, "group %d state", gid)
			require.Equal(t, golden.GroupBlocks[gid], blocks, "group %d blocks", gid)
		}

		// a finalized group keeps its commp
		var commp []byte
		require.NoError(t, sqlDb.QueryRow(`select commp from groups where id = 2`).Scan(&commp))
		require.Len(t, commp, 32)

		// the sealed deal survives with converted types
		sealed := golden.Deals[0]
		var (
			verified, kept  bool
			dealID, epoch   int64
			sealedI, failed int64
		)
		require.NoError(t, sqlDb.QueryRow(
			`select verified, keep_unsealed, deal_id, extract(epoch from start_time)::bigint, sealed, failed from deals where uuid = $1`,
			sealed.UUID).Scan(&verified, &kept, &dealID, &epoch, &sealedI, &failed))
		require.True(t, verified)
		require.True(t, kept)
		require.EqualValues(t, 4242, dealID)
		require.Equal(t, sealed.StartTime, epoch)
		require.EqualValues(t, 1, sealedI)
		require.Zero(t, failed)

		// failed deal keeps its error message
		var errMsg string
		require.NoError(t, sqlDb.QueryRow(`select error_msg from deals where uuid = $1`, golden.Deals[2].UUID).Scan(&errMsg))
		require.Contains(t, errMsg, "price too low")

		// providers with boolean conversions
		var inMarket, pingOk, askOk bool
		var askPrice int64
		require.NoError(t, sqlDb.QueryRow(`select in_market, ping_ok, ask_ok, ask_price from providers where id = 1001`).
			Scan(&inMarket, &pingOk, &askOk, &askPrice))
		require.True(t, inMarket)
		require.True(t, pingOk)
		require.True(t, askOk)
		require.EqualValues(t, 100000, askPrice)

		// float ask values: out-of-range clamps to MaxInt64, in-range converts
		var floatAsk, floatVerif int64
		require.NoError(t, sqlDb.QueryRow(`select ask_price, ask_verif_price from providers where id = 1002`).
			Scan(&floatAsk, &floatVerif))
		require.EqualValues(t, int64(math.MaxInt64), floatAsk)
		require.EqualValues(t, int64(1e18), floatVerif)

		// offload bookkeeping for the offloaded group
		var module, path string
		require.NoError(t, sqlDb.QueryRow(`select module, path from external_path where group_id = $1`, golden.OffloadedGroup).
			Scan(&module, &path))
		require.Equal(t, "local-web", module)
		require.Equal(t, filepath.Base(golden.ExtraFiles[0]), path)

		// repairs row with a NULL worker
		var worker *int64
		var retrievable int64
		require.NoError(t, sqlDb.QueryRow(`select worker, retrievable_deals from repairs where group_id = 2`).Scan(&worker, &retrievable))
		require.Nil(t, worker)
		require.EqualValues(t, 3, retrievable)
	})

	t.Run("cql-contents", func(t *testing.T) {
		for _, b := range golden.Blocks {
			var groups []int64
			var size int32
			iter := cqlDb.Query(`SELECT Group, Size FROM MultihashToGroup WHERE Multihash = ?`, []byte(b.Mh)).Iter()
			var g int64
			var s int32
			for iter.Scan(&g, &s) {
				groups = append(groups, g)
				size = s
			}
			require.NoError(t, iter.Close())

			want := append([]int64{}, b.Groups...)
			slices.Sort(want)
			slices.Sort(groups)
			require.Equal(t, want, groups, "groups for %s", b.Mh)
			require.Equal(t, int32(len(b.Data)), size, "size for %s", b.Mh)
		}
	})

	t.Run("files", func(t *testing.T) {
		for _, name := range []string{"index.pebble", "store.db", "store.db-wal", "store.db-shm"} {
			_, err := os.Lstat(filepath.Join(dest, name))
			require.True(t, os.IsNotExist(err), "%s must not be in the migrated tree", name)
		}

		// immutable (finalized/offloaded) group files and unknown files are
		// hardlinked
		for _, rel := range append([]string{
			filepath.Join("grp", "2", "blklog.car"),
			filepath.Join("grp", "2", "blklog.meta", "index.bsst"),
			filepath.Join("grp", "3", "blklog.meta", "sample.mhlist"),
		}, golden.ExtraFiles...) {
			si, err := os.Stat(filepath.Join(src, rel))
			require.NoError(t, err, rel)
			di, err := os.Stat(filepath.Join(dest, rel))
			require.NoError(t, err, rel)
			require.True(t, os.SameFile(si, di), "%s should be hardlinked", rel)
		}

		// still-mutable groups (writable 1, full 4) must not share inodes
		// with the source: the new node appends to them
		for _, rel := range []string{
			filepath.Join("grp", "1", "blklog.car"),
			filepath.Join("grp", "1", "blklog.meta", "index.level", "CURRENT"),
			filepath.Join("grp", "4", "blklog.car"),
		} {
			si, err := os.Stat(filepath.Join(src, rel))
			require.NoError(t, err, rel)
			di, err := os.Stat(filepath.Join(dest, rel))
			require.NoError(t, err, rel)
			require.False(t, os.SameFile(si, di), "%s should be a copy", rel)
			require.Equal(t, si.Size(), di.Size(), rel)
		}

		_, err = os.Lstat(filepath.Join(dest, "grp", "3", "blklog.car"))
		require.True(t, os.IsNotExist(err), "offloaded group has no local data")
	})

	t.Run("verify-full", func(t *testing.T) {
		report, err := Verify(ctx, opts, VerifyOptions{SampleEvery: 1})
		require.NoError(t, err)
		require.Empty(t, report.Problems)
		require.Equal(t, int64(len(golden.Blocks)), report.IndexChecked)
		require.Positive(t, report.TreeFiles)
	})

	t.Run("rerun-with-state-skips", func(t *testing.T) {
		summary2, err := Run(ctx, opts)
		require.NoError(t, err)
		require.True(t, summary2.Resumed)

		report, err := Verify(ctx, opts, VerifyOptions{SampleEvery: 1})
		require.NoError(t, err)
		require.Empty(t, report.Problems)
	})

	t.Run("redo-without-state", func(t *testing.T) {
		require.NoError(t, os.Remove(filepath.Join(dest, stateFileName)))

		// a full redo clears the previously-migrated tables and does not
		// duplicate anything (deals_archive has no primary key)
		_, err := Run(ctx, opts)
		require.NoError(t, err)

		report, err := Verify(ctx, opts, VerifyOptions{SampleEvery: 1})
		require.NoError(t, err)
		require.Empty(t, report.Problems)
	})

	t.Run("read-back", func(t *testing.T) {
		idx, err := rbstor.NewCqlIndex(cqlDb)
		require.NoError(t, err)

		ri, err := rbstor.Open(&configuration.RibsConfig{DataDir: dest}, rbstor.NewRibsDB(sqlDb), idx)
		require.NoError(t, err)
		ri.StagingStorage().InstallStagingProvider(&failStaging{})
		require.NoError(t, ri.Start())
		defer func() {
			require.NoError(t, ri.Close())
		}()

		sess := ri.Session(ctx)

		readable := map[int64]bool{}
		for _, g := range golden.ReadableGroups {
			readable[g] = true
		}

		var mhs []multihash.Multihash
		var want [][]byte
		for _, b := range golden.Blocks {
			ok := false
			for _, g := range b.Groups {
				if readable[g] {
					ok = true
					break
				}
			}
			if !ok {
				continue // only present in the offloaded group
			}
			mhs = append(mhs, b.Mh)
			want = append(want, b.Data)
		}
		require.NotEmpty(t, mhs)

		got := make([][]byte, len(mhs))
		err = sess.View(ctx, mhs, func(i int, data []byte) {
			got[i] = append([]byte{}, data...)
		})
		require.NoError(t, err)

		for i := range want {
			require.Equal(t, want[i], got[i], "block %s", mhs[i])
		}
	})

	t.Run("group-sequence", func(t *testing.T) {
		newGroup, err := rbstor.NewRibsDB(sqlDb).CreateGroup()
		require.NoError(t, err)
		require.EqualValues(t, 5, newGroup, "first new group id must not collide with migrated ids")
	})
}

// failStaging fails everything: nothing in the migrated golden repo may
// need staging storage.
type failStaging struct{}

func (failStaging) Upload(context.Context, iface.GroupKey, int64, func(io.Writer) error) error {
	return xerrors.New("staging must not be used")
}
func (failStaging) ReadCar(context.Context, iface.GroupKey, int64, int64) (io.ReadCloser, error) {
	return nil, xerrors.New("staging must not be used")
}
func (failStaging) HasCar(context.Context, iface.GroupKey) (bool, error) { return false, nil }

// flakyCql fails ExecuteBatch after a number of calls and cancels the
// migration context, simulating an operator interrupt mid-index-load.
type flakyCql struct {
	cqldb.Database
	calls  atomic.Int64
	after  int64
	cancel context.CancelFunc
}

func (f *flakyCql) ExecuteBatch(b *gocql.Batch) error {
	if f.calls.Add(1) > f.after {
		f.cancel()
		return xerrors.New("injected batch failure")
	}
	return f.Database.ExecuteBatch(b)
}

func TestMigrateResume(t *testing.T) {
	sqlDb, cqlDb := dialTargets(t)
	cleanTargets(t, sqlDb, cqlDb)

	src := filepath.Join(t.TempDir(), "src")
	golden, err := goldenrepo.Generate(src, goldenSeed)
	require.NoError(t, err)

	dest := filepath.Join(t.TempDir(), "dest")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	flaky := &flakyCql{Database: cqlDb, after: 12, cancel: cancel}

	opts := Options{
		SourceDir:       src,
		DestDir:         dest,
		SQL:             sqlDb,
		CQL:             flaky,
		Workers:         2,
		BatchSize:       4,
		CheckpointEvery: 16,
	}

	_, err = Run(ctx, opts)
	require.Error(t, err, "interrupted migration must fail")

	st, err := loadState(dest)
	require.NoError(t, err)
	require.True(t, st.SQLDone)
	require.True(t, st.TreeDone)
	require.False(t, st.IndexDone)
	require.NotEmpty(t, st.IndexCheckpoint, "at least one checkpoint must have been written")
	require.Positive(t, st.IndexEntries)

	// resume with a healthy connection
	opts.CQL = cqlDb
	summary, err := Run(context.Background(), opts)
	require.NoError(t, err)
	require.True(t, summary.Resumed)

	st, err = loadState(dest)
	require.NoError(t, err)
	require.True(t, st.IndexDone)

	report, err := Verify(context.Background(), opts, VerifyOptions{SampleEvery: 1})
	require.NoError(t, err)
	require.Empty(t, report.Problems)
	require.Equal(t, int64(len(golden.Blocks)), report.IndexChecked)
}

func TestMigrateReadOnlySource(t *testing.T) {
	sqlDb, cqlDb := dialTargets(t)
	cleanTargets(t, sqlDb, cqlDb)

	base := t.TempDir()
	src := filepath.Join(base, "src")
	_, err := goldenrepo.Generate(src, goldenSeed)
	require.NoError(t, err)

	// simulate a read-only snapshot
	require.NoError(t, filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return os.Chmod(path, 0555)
		}
		return os.Chmod(path, 0444)
	}))
	t.Cleanup(func() {
		// the dest mirrors the source's read-only permissions; open both
		// back up so TempDir cleanup can remove them
		_ = filepath.WalkDir(base, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return os.Chmod(path, 0755)
			}
			return nil
		})
	})

	dest := filepath.Join(base, "dest")

	opts := Options{
		SourceDir: src,
		DestDir:   dest,
		SQL:       sqlDb,
		CQL:       cqlDb,
	}

	_, err = Run(context.Background(), opts)
	require.NoError(t, err)

	report, err := Verify(context.Background(), opts, VerifyOptions{SampleEvery: 1})
	require.NoError(t, err)
	require.Empty(t, report.Problems)

	// the source must be untouched: no state file, no tmp dir, no new files
	entries, err := os.ReadDir(src)
	require.NoError(t, err)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	for _, n := range names {
		require.False(t, strings.HasPrefix(n, ".ribsdata-migrate"), "source polluted with %s", n)
	}
}

// ensure interfaces stay satisfied
var (
	_ iface.StagingStorageProvider = failStaging{}
	_ cqldb.Database               = (*flakyCql)(nil)
)
