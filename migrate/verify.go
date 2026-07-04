package migrate

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/cockroachdb/pebble"
	"golang.org/x/xerrors"
)

type VerifyOptions struct {
	// SampleEvery checks every Nth multihash of the old index against CQL;
	// 1 verifies everything, 0 skips index verification.
	SampleEvery int
}

type VerifyReport struct {
	// TableCounts maps table name to [source, destination] row counts.
	TableCounts map[string][2]int64

	IndexMultihashes int64 // multihashes scanned in the old index
	IndexPairs       int64 // (multihash, group) pairs scanned
	IndexChecked     int64 // multihashes compared against CQL

	TreeFiles int64 // source files checked against the destination

	Problems []string
}

func (r *VerifyReport) OK() bool {
	return len(r.Problems) == 0
}

const maxReportedProblems = 40

func (r *VerifyReport) problemf(format string, args ...interface{}) {
	if len(r.Problems) < maxReportedProblems {
		r.Problems = append(r.Problems, fmt.Sprintf(format, args...))
	} else if len(r.Problems) == maxReportedProblems {
		r.Problems = append(r.Problems, "... more problems omitted")
	}
}

// Verify compares the migrated databases and file tree against the source
// .ribsdata. It opens its own read-only view of the source and never writes
// to either side (a temporary copy of the source databases is made under the
// destination, as in Run, and removed afterwards).
func Verify(ctx context.Context, opts Options, vo VerifyOptions) (*VerifyReport, error) {
	opts = opts.withDefaults()
	if err := opts.validate(); err != nil {
		return nil, err
	}

	report := &VerifyReport{TableCounts: map[string][2]int64{}}
	tmpDir := filepath.Join(opts.DestDir, tmpDirName+".verify")
	defer os.RemoveAll(tmpDir) // nolint:errcheck

	srcDB, err := openSourceSQLite(opts.SourceDir, tmpDir)
	if err != nil {
		return nil, xerrors.Errorf("opening source sqlite: %w", err)
	}
	defer srcDB.Close() // nolint:errcheck

	if err := verifySQL(ctx, srcDB, opts, report); err != nil {
		return nil, err
	}

	if err := verifyTree(opts, report); err != nil {
		return nil, err
	}

	if vo.SampleEvery > 0 {
		pdb, err := openSourcePebble(opts.SourceDir, tmpDir)
		if err != nil {
			return nil, xerrors.Errorf("opening source pebble index: %w", err)
		}
		defer pdb.Close() // nolint:errcheck

		if err := verifyIndex(ctx, pdb, opts, vo.SampleEvery, report); err != nil {
			return nil, err
		}
	}

	return report, nil
}

func verifySQL(ctx context.Context, src *sql.DB, opts Options, report *VerifyReport) error {
	for _, spec := range tableSpecs {
		var srcCount, dstCount int64

		present, err := sqliteColumns(src, spec.name)
		if err != nil {
			return err
		}
		if len(present) > 0 {
			if err := src.QueryRowContext(ctx, fmt.Sprintf(`select count(*) from %s`, spec.name)).Scan(&srcCount); err != nil {
				return xerrors.Errorf("counting source %s: %w", spec.name, err)
			}
		}

		if err := opts.SQL.QueryRow(fmt.Sprintf(`select count(*) from %s`, spec.name)).Scan(&dstCount); err != nil {
			return xerrors.Errorf("counting destination %s: %w", spec.name, err)
		}

		report.TableCounts[spec.name] = [2]int64{srcCount, dstCount}
		if srcCount != dstCount {
			report.problemf("table %s: source has %d rows, destination has %d", spec.name, srcCount, dstCount)
		}
	}

	if err := verifyGroupsDeep(ctx, src, opts, report); err != nil {
		return err
	}

	return verifyGroupsSequence(opts, report)
}

type groupRow struct {
	id, blocks, bytes, gState, jbHead int64
	pieceSize, carSize                sql.NullInt64
	commp, root                       []byte
}

func scanGroupRows(rows *sql.Rows) (map[int64]groupRow, error) {
	defer rows.Close() // nolint:errcheck

	out := map[int64]groupRow{}
	for rows.Next() {
		var g groupRow
		if err := rows.Scan(&g.id, &g.blocks, &g.bytes, &g.gState, &g.jbHead, &g.pieceSize, &g.commp, &g.carSize, &g.root); err != nil {
			return nil, err
		}
		out[g.id] = g
	}
	return out, rows.Err()
}

const groupSelect = `select id, blocks, bytes, g_state, jb_recorded_head, piece_size, commp, car_size, root from groups`

func verifyGroupsDeep(ctx context.Context, src *sql.DB, opts Options, report *VerifyReport) error {
	srcRows, err := src.QueryContext(ctx, groupSelect)
	if err != nil {
		return xerrors.Errorf("selecting source groups: %w", err)
	}
	srcGroups, err := scanGroupRows(srcRows)
	if err != nil {
		return xerrors.Errorf("scanning source groups: %w", err)
	}

	dstRows, err := opts.SQL.QueryContext(ctx, groupSelect)
	if err != nil {
		return xerrors.Errorf("selecting destination groups: %w", err)
	}
	dstGroups, err := scanGroupRows(dstRows)
	if err != nil {
		return xerrors.Errorf("scanning destination groups: %w", err)
	}

	for id, sg := range srcGroups {
		dg, ok := dstGroups[id]
		if !ok {
			report.problemf("group %d missing from destination", id)
			continue
		}
		if sg.blocks != dg.blocks || sg.bytes != dg.bytes || sg.gState != dg.gState || sg.jbHead != dg.jbHead ||
			sg.pieceSize != dg.pieceSize || sg.carSize != dg.carSize ||
			!bytes.Equal(sg.commp, dg.commp) || !bytes.Equal(sg.root, dg.root) {
			report.problemf("group %d differs: source %+v destination %+v", id, sg, dg)
		}
	}
	for id := range dstGroups {
		if _, ok := srcGroups[id]; !ok {
			report.problemf("group %d present in destination but not in source", id)
		}
	}
	return nil
}

func verifyGroupsSequence(opts Options, report *VerifyReport) error {
	var seqName sql.NullString
	if err := opts.SQL.QueryRow(`select pg_get_serial_sequence('groups', 'id')`).Scan(&seqName); err != nil {
		return xerrors.Errorf("resolving groups id sequence: %w", err)
	}
	if !seqName.Valid {
		report.problemf("groups.id has no backing sequence")
		return nil
	}

	var ok bool
	q := fmt.Sprintf(`select last_value >= (select coalesce(max(id), 0) from groups) from %s`, seqName.String)
	if err := opts.SQL.QueryRow(q).Scan(&ok); err != nil {
		return xerrors.Errorf("checking groups id sequence: %w", err)
	}
	if !ok {
		report.problemf("groups id sequence is behind max(id); new groups would collide")
	}
	return nil
}

func verifyTree(opts Options, report *VerifyReport) error {
	return filepath.WalkDir(opts.SourceDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(opts.SourceDir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}

		top := rel
		if i := strings.IndexByte(rel, filepath.Separator); i >= 0 {
			top = rel[:i]
		}
		if treeExcludes[top] {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.IsDir() || d.Type()&fs.ModeSymlink != 0 {
			return nil
		}

		report.TreeFiles++

		info, err := d.Info()
		if err != nil {
			return err
		}
		dstInfo, err := os.Stat(filepath.Join(opts.DestDir, rel))
		if err != nil {
			report.problemf("file %s missing from destination: %s", rel, err)
			return nil
		}
		if dstInfo.Size() != info.Size() {
			report.problemf("file %s size differs: source %d destination %d", rel, info.Size(), dstInfo.Size())
		}
		return nil
	})
}

func verifyIndex(ctx context.Context, pdb *pebble.DB, opts Options, sampleEvery int, report *VerifyReport) error {
	var n int64

	st, err := scanTopIndex(pdb, nil, func(mh []byte, size int32, groups []int64) error {
		n++
		if (n-1)%int64(sampleEvery) != 0 {
			return nil
		}
		report.IndexChecked++

		gotGroups := make([]int64, 0, len(groups))
		var gotSize int32
		iter := opts.CQL.Query(`SELECT Group, Size FROM MultihashToGroup WHERE Multihash = ?`, mh).WithContext(ctx).Iter()
		var g int64
		var s int32
		for iter.Scan(&g, &s) {
			gotGroups = append(gotGroups, g)
			gotSize = s
		}
		if err := iter.Close(); err != nil {
			return xerrors.Errorf("querying cql index: %w", err)
		}

		want := append([]int64{}, groups...)
		slices.Sort(want)
		slices.Sort(gotGroups)

		if !slices.Equal(want, gotGroups) {
			report.problemf("multihash %x: groups differ: pebble %v cql %v", mh, want, gotGroups)
		} else if len(gotGroups) > 0 && gotSize != size {
			report.problemf("multihash %x: size differs: pebble %d cql %d", mh, size, gotSize)
		}
		return ctx.Err()
	})
	if err != nil {
		return err
	}

	report.IndexMultihashes = st.Multihashes
	report.IndexPairs = st.Pairs
	return nil
}
