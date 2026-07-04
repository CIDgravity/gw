package migrate

import (
	"encoding/json"
	"os"
	"path/filepath"

	"golang.org/x/xerrors"
)

const (
	stateFileName = ".ribsdata-migrate.state.json"
	tmpDirName    = ".ribsdata-migrate.tmp"

	stateVersion = 1
)

// State tracks migration progress in the destination directory so an
// interrupted migration can be resumed. All loaders are idempotent
// (CQL inserts are upserts, SQL inserts use ON CONFLICT DO NOTHING),
// so re-doing a partially completed step is always safe.
type State struct {
	Version int `json:"version"`

	SQLDone   bool `json:"sql_done"`
	TreeDone  bool `json:"tree_done"`
	IndexDone bool `json:"index_done"`

	// IndexCheckpoint is the hex multihash of the last index entry that was
	// fully flushed to CQL. On resume the pebble scan restarts at this
	// multihash (inclusive; the overlap is harmless).
	IndexCheckpoint string `json:"index_checkpoint,omitempty"`

	// IndexEntries counts (multihash, group) pairs inserted so far. After a
	// resume this over-counts by the re-done checkpoint window; it is
	// informational only.
	IndexEntries int64 `json:"index_entries"`
}

func statePath(destDir string) string {
	return filepath.Join(destDir, stateFileName)
}

func loadState(destDir string) (*State, error) {
	data, err := os.ReadFile(statePath(destDir))
	if os.IsNotExist(err) {
		return &State{Version: stateVersion}, nil
	}
	if err != nil {
		return nil, xerrors.Errorf("reading migration state: %w", err)
	}

	var s State
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, xerrors.Errorf("parsing migration state %s: %w", statePath(destDir), err)
	}
	if s.Version != stateVersion {
		return nil, xerrors.Errorf("migration state %s has version %d, expected %d", statePath(destDir), s.Version, stateVersion)
	}
	return &s, nil
}

func (s *State) save(destDir string) error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return xerrors.Errorf("marshaling migration state: %w", err)
	}

	tmp := statePath(destDir) + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return xerrors.Errorf("writing migration state: %w", err)
	}
	if err := os.Rename(tmp, statePath(destDir)); err != nil {
		return xerrors.Errorf("renaming migration state: %w", err)
	}
	return nil
}
