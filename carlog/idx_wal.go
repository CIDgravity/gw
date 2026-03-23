package carlog

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
)

// WalIndex is a write-ahead-log backed index that replaces LevelDB for the
// writable phase of a CarLog. It stores multihash → packed(offset,length)
// mappings in an append-only journal file and maintains an in-memory hash map
// for O(1) lookups.
//
// Crash recovery: on Open the WAL is replayed into the in-memory map. Entries
// whose decoded data-file offset is at or above a caller-supplied truncation
// boundary are discarded (matching the existing CarLog truncation semantics
// where the head file's RetiredAt is the commit checkpoint).
type WalIndex struct {
	f     *os.File
	w     *bufio.Writer
	mu    sync.Mutex // protects w and f writes
	m     map[string]int64
	count int64
}

const (
	walMagic   = "CIDXWAL\x01"
	walBufSize = 256 << 10 // 256 KiB write buffer
)

// CreateWalIndex creates a new WAL index file at path. The file must not
// already exist.
func CreateWalIndex(path string) (*WalIndex, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return nil, xerrors.Errorf("creating wal index: %w", err)
	}

	w := bufio.NewWriterSize(f, walBufSize)
	if _, err := w.WriteString(walMagic); err != nil {
		f.Close()
		return nil, xerrors.Errorf("writing wal magic: %w", err)
	}

	return &WalIndex{
		f:     f,
		w:     w,
		m:     make(map[string]int64),
		count: 0,
	}, nil
}

// OpenWalIndex opens an existing WAL index and replays it into memory.
// Entries with a data-file offset >= truncateAt are discarded during replay.
// Pass -1 for truncateAt to load all entries.
func OpenWalIndex(path string, truncateAt int64) (*WalIndex, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		return nil, xerrors.Errorf("opening wal index: %w", err)
	}

	m, validEnd, err := replayWal(f, truncateAt)
	if err != nil {
		f.Close()
		return nil, xerrors.Errorf("replaying wal: %w", err)
	}

	// Truncate file to remove any torn/uncommitted entries past the last
	// valid boundary. This is safe because anything past validEnd was either
	// a torn write or an entry past truncateAt.
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, xerrors.Errorf("stat wal: %w", err)
	}
	if fi.Size() > validEnd {
		if err := f.Truncate(validEnd); err != nil {
			f.Close()
			return nil, xerrors.Errorf("truncating wal: %w", err)
		}
	}

	// Seek to end for appending
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, xerrors.Errorf("seeking wal to end: %w", err)
	}

	return &WalIndex{
		f:     f,
		w:     bufio.NewWriterSize(f, walBufSize),
		m:     m,
		count: int64(len(m)),
	}, nil
}

// replayWal reads a WAL file from the beginning, building the in-memory map.
// Returns the map, the file offset of the end of the last valid entry (for
// truncation), and any hard error. Torn writes at the end are silently ignored.
func replayWal(f *os.File, truncateAt int64) (map[string]int64, int64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, 0, xerrors.Errorf("seek to start: %w", err)
	}

	r := bufio.NewReaderSize(f, walBufSize)

	// Verify magic
	var magic [8]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return nil, 0, xerrors.Errorf("reading wal magic: %w", err)
	}
	if string(magic[:]) != walMagic {
		return nil, 0, fmt.Errorf("invalid wal magic: %x", magic[:])
	}

	m := make(map[string]int64)
	pos := int64(len(walMagic)) // current read position in file
	validEnd := pos             // last position of a fully-read valid entry

	var hdr [2]byte
	var offBuf [8]byte

	for {
		// Read 2-byte multihash length
		_, err := io.ReadFull(r, hdr[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break // end of file or torn write
		}
		if err != nil {
			return nil, 0, xerrors.Errorf("reading entry header at %d: %w", pos, err)
		}

		mhLen := int(binary.LittleEndian.Uint16(hdr[:]))
		if mhLen == 0 || mhLen > 256 {
			// Likely corrupt or torn entry — stop replay here
			break
		}

		entrySize := int64(2 + mhLen + 8)

		// Read multihash bytes
		mhBuf := make([]byte, mhLen)
		_, err = io.ReadFull(r, mhBuf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break // torn write
		}
		if err != nil {
			return nil, 0, xerrors.Errorf("reading mh at %d: %w", pos, err)
		}

		// Read packed offset
		_, err = io.ReadFull(r, offBuf[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break // torn write
		}
		if err != nil {
			return nil, 0, xerrors.Errorf("reading offset at %d: %w", pos, err)
		}

		off := int64(binary.LittleEndian.Uint64(offBuf[:]))
		pos += entrySize

		// Apply truncation filter: discard entries at or above the boundary
		if truncateAt >= 0 {
			dataOff, _ := fromOffsetLen(off)
			if dataOff >= truncateAt {
				continue // skip this entry, don't advance validEnd
			}
		}

		m[string(mhBuf)] = off
		validEnd = pos
	}

	return m, validEnd, nil
}

// Put records entries in the index. Offsets of -1 are skipped (existing blocks).
func (w *WalIndex) Put(c []multihash.Multihash, offs []int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var hdr [2]byte
	var offBuf [8]byte

	for i, mh := range c {
		if offs[i] == -1 {
			continue
		}

		mhBytes := []byte(mh)
		if len(mhBytes) > 65535 {
			return fmt.Errorf("multihash too long: %d bytes", len(mhBytes))
		}

		// Write entry to WAL: [2-byte mh_len][mh_bytes][8-byte offset]
		binary.LittleEndian.PutUint16(hdr[:], uint16(len(mhBytes)))
		if _, err := w.w.Write(hdr[:]); err != nil {
			return xerrors.Errorf("writing mh len: %w", err)
		}
		if _, err := w.w.Write(mhBytes); err != nil {
			return xerrors.Errorf("writing mh: %w", err)
		}
		binary.LittleEndian.PutUint64(offBuf[:], uint64(offs[i]))
		if _, err := w.w.Write(offBuf[:]); err != nil {
			return xerrors.Errorf("writing offset: %w", err)
		}

		key := string(mhBytes)
		if _, exists := w.m[key]; !exists {
			w.count++
		}
		w.m[key] = offs[i]
	}

	return nil
}

// Has checks whether each multihash exists in the index.
func (w *WalIndex) Has(c []multihash.Multihash) ([]bool, error) {
	out := make([]bool, len(c))
	for i, mh := range c {
		_, out[i] = w.m[string([]byte(mh))]
	}
	return out, nil
}

// Get returns the packed offset for each multihash, or -1 if not found.
func (w *WalIndex) Get(c []multihash.Multihash) ([]int64, error) {
	out := make([]int64, len(c))
	for i, mh := range c {
		v, ok := w.m[string([]byte(mh))]
		if !ok {
			out[i] = -1
		} else {
			out[i] = v
		}
	}
	return out, nil
}

// Entries returns the number of indexed entries. O(1).
func (w *WalIndex) Entries() (int64, error) {
	return w.count, nil
}

// List iterates all entries, calling f for each one. The iteration order
// is not guaranteed to be sorted. The BSST builder sorts internally, so
// this is fine.
func (w *WalIndex) List(f func(c multihash.Multihash, offs []int64) error) error {
	for mh, off := range w.m {
		if err := f(multihash.Multihash(mh), []int64{off}); err != nil {
			return err
		}
	}
	return nil
}

// ToTruncate returns multihashes whose data-file offset is at or above the
// given boundary. Used during crash recovery.
func (w *WalIndex) ToTruncate(atOrAbove int64) ([]multihash.Multihash, error) {
	var out []multihash.Multihash
	for mh, off := range w.m {
		dataOff, _ := fromOffsetLen(off)
		if dataOff >= atOrAbove {
			out = append(out, multihash.Multihash(mh))
		}
	}
	return out, nil
}

// Del removes the given multihashes from the in-memory index. The WAL file
// is not rewritten — deleted entries simply won't be present in the map.
// Only called during crash recovery truncation.
func (w *WalIndex) Del(c []multihash.Multihash) error {
	for _, mh := range c {
		key := string([]byte(mh))
		if _, ok := w.m[key]; ok {
			delete(w.m, key)
			w.count--
		}
	}
	return nil
}

// Sync flushes the buffered writer and fdatasyncs the WAL file.
func (w *WalIndex) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.w.Flush(); err != nil {
		return xerrors.Errorf("flushing wal buffer: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return xerrors.Errorf("syncing wal file: %w", err)
	}
	return nil
}

// Close flushes and closes the WAL file.
func (w *WalIndex) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.w.Flush(); err != nil {
		return xerrors.Errorf("flushing wal on close: %w", err)
	}
	return w.f.Close()
}

// Compile-time interface checks
var _ WritableIndex = &WalIndex{}
var _ ReadableIndex = &WalIndex{}
