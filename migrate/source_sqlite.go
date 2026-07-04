package migrate

import (
	"database/sql"
	"io"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/xerrors"
)

// openSourceSQLite opens the old store.db from sourceDir.
//
// The source is treated as strictly read-only, but sqlite may need to replay
// a WAL (and needs to create a -shm file to do so), so the database files are
// first copied into tmpDir and opened there.
func openSourceSQLite(sourceDir, tmpDir string) (*sql.DB, error) {
	src := filepath.Join(sourceDir, "store.db")
	if _, err := os.Stat(src); err != nil {
		return nil, xerrors.Errorf("source store.db: %w", err)
	}

	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return nil, xerrors.Errorf("creating tmp dir: %w", err)
	}

	for _, suffix := range []string{"", "-wal", "-shm"} {
		target := filepath.Join(tmpDir, "store.db"+suffix)
		if err := copyFileIfExists(src+suffix, target); err != nil {
			return nil, xerrors.Errorf("copying store.db%s: %w", suffix, err)
		}
		// the copy must be writable (WAL replay) even off a read-only source
		if err := os.Chmod(target, 0600); err != nil && !os.IsNotExist(err) {
			return nil, xerrors.Errorf("fixing permissions on store.db%s copy: %w", suffix, err)
		}
	}

	db, err := sql.Open("sqlite3", filepath.Join(tmpDir, "store.db"))
	if err != nil {
		return nil, xerrors.Errorf("opening sqlite copy: %w", err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, xerrors.Errorf("pinging sqlite copy: %w", err)
	}

	return db, nil
}

func copyFileIfExists(src, dst string) error {
	in, err := os.Open(src)
	if os.IsNotExist(err) {
		// a leftover from a previous run must not shadow a missing source file
		if err := os.Remove(dst); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	if err != nil {
		return err
	}
	defer in.Close() // nolint:errcheck

	st, err := in.Stat()
	if err != nil {
		return err
	}

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, st.Mode().Perm())
	if err != nil {
		return err
	}

	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

// sqliteColumns returns the set of columns present on a table in the source
// database. Old deployments can predate columns that the final schema
// declares (sqlite's `create table if not exists` never alters existing
// tables), so table copies intersect the expected column list with what is
// actually there.
func sqliteColumns(db *sql.DB, table string) (map[string]bool, error) {
	rows, err := db.Query(`select name from pragma_table_info(?)`, table)
	if err != nil {
		return nil, xerrors.Errorf("querying table info for %s: %w", table, err)
	}
	defer rows.Close() // nolint:errcheck

	cols := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, xerrors.Errorf("scanning column name: %w", err)
		}
		cols[name] = true
	}
	return cols, rows.Err()
}
