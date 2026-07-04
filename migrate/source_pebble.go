package migrate

import (
	"bytes"
	"encoding/binary"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble"
	"golang.org/x/xerrors"
)

// The old top-level index (rbstor/index_pebble.go before the CQL port)
// stored two kinds of keys:
//
//	's:[mh bytes]'                 -> [size u32 BE][best group u64 BE]
//	'i:[mh bytes][group u64 BE]'   -> {}
//
// The 'i:' keys enumerate every (multihash, group) pair and are the source
// of truth; the 's:' value carries the block size (and a "best group" hint
// that has no equivalent in the CQL schema and is dropped).
const pebbleIndexDir = "index.pebble"

// openSourcePebble opens the old pebble index read-only. If the source sits
// on a read-only filesystem (pebble needs to create/lock its LOCK file even
// in read-only mode), the index is copied into tmpDir and opened there.
func openSourcePebble(sourceDir, tmpDir string) (*pebble.DB, error) {
	p := filepath.Join(sourceDir, pebbleIndexDir)
	if _, err := os.Stat(p); err != nil {
		return nil, xerrors.Errorf("source index.pebble: %w", err)
	}

	db, err := pebble.Open(p, &pebble.Options{ReadOnly: true})
	if err == nil {
		return db, nil
	}
	openErr := err

	log.Warnw("read-only pebble open failed, copying index to temp space", "error", openErr)

	cp := filepath.Join(tmpDir, pebbleIndexDir)
	if err := os.RemoveAll(cp); err != nil {
		return nil, xerrors.Errorf("cleaning pebble tmp copy: %w", err)
	}
	if err := copyDir(p, cp); err != nil {
		return nil, xerrors.Errorf("copying pebble index (after read-only open failed with: %s): %w", openErr, err)
	}

	db, err = pebble.Open(cp, &pebble.Options{})
	if err != nil {
		return nil, xerrors.Errorf("opening pebble index copy: %w", err)
	}
	return db, nil
}

func copyDir(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)

		if d.IsDir() {
			return os.MkdirAll(target, 0755)
		}
		if err := copyFileIfExists(path, target); err != nil {
			return err
		}

		// the copy exists to be opened read-write; a read-only source
		// (0444 LOCK file) must not carry over
		info, err := d.Info()
		if err != nil {
			return err
		}
		return os.Chmod(target, info.Mode().Perm()|0600)
	})
}

var (
	idxKeyPrefix   = []byte("i:")
	idxKeyEnd      = []byte("i;") // 0x3b immediately follows 0x3a
	sizeKeyPrefix  = []byte("s:")
	idxGroupKeyLen = 8
)

// scanStats reports counters accumulated by scanTopIndex.
type scanStats struct {
	Multihashes int64 // distinct multihashes seen
	Pairs       int64 // (multihash, group) pairs seen
	MissingSize int64 // multihashes without a readable 's:' entry (size recorded as 0)
}

// scanTopIndex iterates the old index in multihash order, invoking cb once
// per multihash with all groups that contain it and the block size.
//
// startAt, when non-nil, is a multihash to (re)start the scan from,
// inclusive; it is used for checkpoint-based resume.
//
// cb must not retain mh or groups past its return.
func scanTopIndex(db *pebble.DB, startAt []byte, cb func(mh []byte, size int32, groups []int64) error) (scanStats, error) {
	var st scanStats

	lower := idxKeyPrefix
	if len(startAt) > 0 {
		lower = append(append([]byte{}, idxKeyPrefix...), startAt...)
	}

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: idxKeyEnd,
	})
	if err != nil {
		return st, xerrors.Errorf("creating pebble iterator: %w", err)
	}
	defer iter.Close() // nolint:errcheck

	var (
		curMh  []byte
		groups []int64
	)

	emit := func() error {
		if curMh == nil {
			return nil
		}
		size, ok, err := lookupSize(db, curMh)
		if err != nil {
			return err
		}
		if !ok {
			st.MissingSize++
		}
		st.Multihashes++
		st.Pairs += int64(len(groups))
		return cb(curMh, size, groups)
	}

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) < len(idxKeyPrefix)+idxGroupKeyLen+1 {
			return st, xerrors.Errorf("malformed index key of length %d", len(key))
		}

		mh := key[len(idxKeyPrefix) : len(key)-idxGroupKeyLen]
		group := int64(binary.BigEndian.Uint64(key[len(key)-idxGroupKeyLen:]))

		if !bytes.Equal(mh, curMh) {
			if err := emit(); err != nil {
				return st, err
			}
			curMh = append(curMh[:0], mh...)
			groups = groups[:0]
		}
		groups = append(groups, group)
	}
	if err := iter.Error(); err != nil {
		return st, xerrors.Errorf("iterating index: %w", err)
	}

	if err := emit(); err != nil {
		return st, err
	}

	return st, nil
}

// lookupSize reads the block size from the 's:' key. Values are
// [size u32 BE][best group u64 BE]; very old entries may be size-only.
func lookupSize(db *pebble.DB, mh []byte) (int32, bool, error) {
	key := append(append([]byte{}, sizeKeyPrefix...), mh...)
	val, closer, err := db.Get(key)
	if err == pebble.ErrNotFound {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, xerrors.Errorf("reading size entry: %w", err)
	}
	defer closer.Close() // nolint:errcheck

	if len(val) < 4 {
		return 0, false, nil
	}
	return int32(binary.BigEndian.Uint32(val[:4])), true, nil
}
