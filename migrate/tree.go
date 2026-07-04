package migrate

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/xerrors"
)

// treeExcludes are top-level entries of the old .ribsdata that must not be
// carried into the migrated directory: they are replaced by the Yugabyte
// databases (or belong to the migrator itself).
var treeExcludes = map[string]bool{
	pebbleIndexDir: true,
	"store.db":     true,
	"store.db-wal": true,
	"store.db-shm": true,
	stateFileName:  true,
	tmpDirName:     true,
}

type treeStats struct {
	Files  int64
	Linked int64
	Copied int64
	Bytes  int64
}

// migrateTree replicates the source .ribsdata into destDir. Files are
// hardlinked where possible (same filesystem) and copied otherwise, when
// copyFiles is set, or when forceCopy matches (used for groups that are
// still mutable: appending to a hardlinked file would write through to the
// supposedly read-only source copy).
//
// Group data (grp/), localweb CAR data (cardata/) and anything else living
// in the old directory is intentionally treated as opaque: the on-disk group
// formats are unchanged across this migration.
func migrateTree(sourceDir, destDir string, copyFiles bool, forceCopy func(rel string) bool) (treeStats, error) {
	var st treeStats

	// Directory permissions are applied after the walk: replicating a
	// read-only snapshot (0555 directories) must not lock the migrator out
	// of directories it still has to fill.
	type dirPerm struct {
		path string
		mode fs.FileMode
	}
	var dirPerms []dirPerm

	err := filepath.WalkDir(sourceDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		rel, err := filepath.Rel(sourceDir, path)
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

		target := filepath.Join(destDir, rel)

		switch {
		case d.IsDir():
			info, err := d.Info()
			if err != nil {
				return err
			}
			if err := os.MkdirAll(target, info.Mode().Perm()|0300); err != nil {
				return xerrors.Errorf("creating dir %s: %w", target, err)
			}
			// re-runs may find the final (possibly read-only) mode applied
			if err := os.Chmod(target, info.Mode().Perm()|0300); err != nil {
				return xerrors.Errorf("opening up dir %s: %w", target, err)
			}
			dirPerms = append(dirPerms, dirPerm{target, info.Mode().Perm()})
			return nil

		case d.Type()&fs.ModeSymlink != 0:
			dst, err := os.Readlink(path)
			if err != nil {
				return err
			}
			if err := os.Symlink(dst, target); err != nil && !os.IsExist(err) {
				return xerrors.Errorf("creating symlink %s: %w", target, err)
			}
			return nil

		default:
			info, err := d.Info()
			if err != nil {
				return err
			}

			if _, err := os.Lstat(target); err == nil {
				// left over from a previous run
				st.Files++
				return nil
			}

			st.Files++
			st.Bytes += info.Size()

			if !copyFiles && (forceCopy == nil || !forceCopy(rel)) {
				if err := os.Link(path, target); err == nil {
					st.Linked++
					return nil
				}
				// cross-device or filesystem without hardlink support
			}

			if err := copyFileIfExists(path, target); err != nil {
				return xerrors.Errorf("copying %s: %w", rel, err)
			}
			if forceCopy != nil && forceCopy(rel) {
				// mutable-group files are copied to be written to; a
				// read-only source snapshot must not make them read-only
				if err := os.Chmod(target, info.Mode().Perm()|0600); err != nil {
					return xerrors.Errorf("opening up %s: %w", rel, err)
				}
			}
			if err := os.Chtimes(target, info.ModTime(), info.ModTime()); err != nil {
				return xerrors.Errorf("setting times on %s: %w", rel, err)
			}
			st.Copied++
			return nil
		}
	})
	if err != nil {
		return st, err
	}

	// children first, so restoring a read-only parent can't block a child
	for i := len(dirPerms) - 1; i >= 0; i-- {
		if err := os.Chmod(dirPerms[i].path, dirPerms[i].mode); err != nil {
			return st, xerrors.Errorf("applying dir permissions: %w", err)
		}
	}

	return st, nil
}
