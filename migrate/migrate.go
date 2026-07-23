// Package migrate implements the offline .ribsdata migration from the old
// all-local storage stack (top-level pebble index + sqlite store.db) to the
// Yugabyte-backed stack (CQL MultihashToGroup index + SQL metadata tables).
//
// The migrator works on a read-only copy of the old .ribsdata and produces a
// new .ribsdata directory next to populated Yugabyte databases:
//
//   - metadata tables (groups, deals, providers, ...) are copied from
//     store.db into the Yugabyte SQL database, preserving ids (including
//     advancing the groups id sequence past the migrated ids),
//   - the top-level block index is streamed from index.pebble into the CQL
//     MultihashToGroup table,
//   - everything else (grp/, cardata/, ...) is hardlinked or copied
//     verbatim: per-group formats (carlog, leveldb write indexes, bsst) are
//     unchanged and remain readable by the new code.
//
// Progress is checkpointed into the destination directory, and every loader
// is idempotent, so an interrupted migration can simply be re-run.
package migrate

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/CIDgravity/filecoin-gateway/database/cqldb"
	"github.com/CIDgravity/filecoin-gateway/database/sqldb"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
)

var log = logging.Logger("gw/migrate")

type Options struct {
	// SourceDir is the old .ribsdata; it is never written to.
	SourceDir string
	// DestDir receives the migrated .ribsdata (and migration state).
	DestDir string

	// SQL is the target Yugabyte SQL database (schema already migrated by
	// sqldb.NewYugabyteDB).
	SQL sqldb.Database
	// CQL is the target Yugabyte CQL keyspace (schema already migrated by
	// cqldb.NewYugabyteCqlDb).
	CQL cqldb.Database

	// CopyFiles disables hardlinking and always copies file data.
	CopyFiles bool

	// Workers is the number of concurrent CQL insert workers.
	Workers int
	// BatchSize is the number of index entries per CQL batch.
	BatchSize int
	// CheckpointEvery is the number of index entries between checkpoints.
	CheckpointEvery int64
}

func (o *Options) withDefaults() Options {
	out := *o
	if out.Workers == 0 {
		out.Workers = 16
	}
	if out.BatchSize == 0 {
		out.BatchSize = 1024
	}
	if out.CheckpointEvery == 0 {
		out.CheckpointEvery = 1 << 20
	}
	return out
}

type Summary struct {
	SQLRows  map[string]int64
	Tree     treeStats
	Index    scanStats
	Resumed  bool
	Duration time.Duration
}

func (o *Options) validate() error {
	if o.SourceDir == "" || o.DestDir == "" {
		return xerrors.New("both source and destination directories are required")
	}
	src, err := filepath.Abs(o.SourceDir)
	if err != nil {
		return err
	}
	dst, err := filepath.Abs(o.DestDir)
	if err != nil {
		return err
	}
	if src == dst || strings.HasPrefix(dst+string(filepath.Separator), src+string(filepath.Separator)) {
		return xerrors.New("destination must not be inside the source directory")
	}
	if o.SQL == nil || o.CQL == nil {
		return xerrors.New("both SQL and CQL database connections are required")
	}
	return nil
}

// Run performs (or resumes) the migration.
func Run(ctx context.Context, opts Options) (*Summary, error) {
	opts = opts.withDefaults()
	if err := opts.validate(); err != nil {
		return nil, err
	}

	start := time.Now()

	if err := os.MkdirAll(opts.DestDir, 0755); err != nil {
		return nil, xerrors.Errorf("creating destination: %w", err)
	}

	state, err := loadState(opts.DestDir)
	if err != nil {
		return nil, err
	}

	summary := &Summary{
		SQLRows: map[string]int64{},
		Resumed: state.SQLDone || state.TreeDone || state.IndexCheckpoint != "",
	}

	tmpDir := filepath.Join(opts.DestDir, tmpDirName)

	// 1. sqlite → Yugabyte SQL
	if state.SQLDone {
		log.Infow("sql phase already done, skipping")
	} else {
		log.Infow("migrating sql metadata", "source", opts.SourceDir)
		srcDB, err := openSourceSQLite(opts.SourceDir, tmpDir)
		if err != nil {
			return nil, xerrors.Errorf("opening source sqlite: %w", err)
		}

		counts, err := migrateSQL(ctx, srcDB, opts.SQL)
		cerr := srcDB.Close()
		if err != nil {
			return nil, xerrors.Errorf("migrating sql metadata: %w", err)
		}
		if cerr != nil {
			return nil, xerrors.Errorf("closing source sqlite: %w", cerr)
		}

		summary.SQLRows = counts
		state.SQLDone = true
		if err := state.save(opts.DestDir); err != nil {
			return nil, err
		}
	}

	// 2. file tree
	if state.TreeDone {
		log.Infow("tree phase already done, skipping")
	} else {
		log.Infow("replicating file tree", "copy", opts.CopyFiles)

		// groups that can still be written to must never share inodes with
		// the source, so their directories are copied instead of hardlinked
		srcDB, err := openSourceSQLite(opts.SourceDir, tmpDir)
		if err != nil {
			return nil, xerrors.Errorf("opening source sqlite for group states: %w", err)
		}
		mutableDirs, err := mutableGroupDirs(srcDB)
		if cerr := srcDB.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return nil, xerrors.Errorf("listing mutable groups: %w", err)
		}
		log.Infow("groups needing a full copy", "count", len(mutableDirs))

		forceCopy := func(rel string) bool {
			parts := strings.SplitN(rel, string(filepath.Separator), 3)
			return len(parts) >= 2 && parts[0] == "grp" && mutableDirs[parts[1]]
		}

		ts, err := migrateTree(opts.SourceDir, opts.DestDir, opts.CopyFiles, forceCopy)
		if err != nil {
			return nil, xerrors.Errorf("replicating file tree: %w", err)
		}
		summary.Tree = ts
		state.TreeDone = true
		if err := state.save(opts.DestDir); err != nil {
			return nil, err
		}
		log.Infow("file tree replicated", "files", ts.Files, "linked", ts.Linked, "copied", ts.Copied, "bytes", ts.Bytes)
	}

	// 3. pebble → CQL index (the long pole, runs last)
	if state.IndexDone {
		log.Infow("index phase already done, skipping")
	} else {
		log.Infow("migrating block index", "workers", opts.Workers, "batchSize", opts.BatchSize)
		pdb, err := openSourcePebble(opts.SourceDir, tmpDir)
		if err != nil {
			return nil, xerrors.Errorf("opening source pebble index: %w", err)
		}

		st, err := migrateIndex(ctx, pdb, opts.CQL, state, opts.DestDir, opts.Workers, opts.BatchSize, opts.CheckpointEvery)
		cerr := pdb.Close()
		if err != nil {
			return nil, xerrors.Errorf("migrating block index: %w", err)
		}
		if cerr != nil {
			return nil, xerrors.Errorf("closing source pebble index: %w", cerr)
		}

		summary.Index = st
		if st.MissingSize > 0 {
			log.Warnw("some blocks had no size entry in the old index; size recorded as 0", "count", st.MissingSize)
		}

		state.IndexDone = true
		state.IndexCheckpoint = ""
		if err := state.save(opts.DestDir); err != nil {
			return nil, err
		}
		log.Infow("block index migrated", "multihashes", st.Multihashes, "pairs", st.Pairs)
	}

	if err := os.RemoveAll(tmpDir); err != nil {
		return nil, xerrors.Errorf("cleaning tmp dir: %w", err)
	}

	summary.Duration = time.Since(start)
	log.Infow("migration complete", "took", summary.Duration)
	return summary, nil
}
