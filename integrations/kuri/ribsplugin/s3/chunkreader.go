package s3

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type ChunkReader struct {
	source *bufio.Reader
	remain int64
	done   bool
}

func newChunkReader(source io.Reader) *ChunkReader {
	return &ChunkReader{source: bufio.NewReader(source)}
}

// todo validate chunk signatures
func (cr *ChunkReader) Read(out []byte) (n int, err error) {
	if cr.done {
		return 0, io.EOF
	}

	for cr.remain == 0 {
		// todo consider optimization by removing string operations
		line, err := cr.source.ReadString('\n')
		if err != nil {
			return 0, fmt.Errorf("reading chunk size: %w", err)
		}
		chunkSize, err := parseChunkSize(line)
		if err != nil {
			return 0, fmt.Errorf("parsing chunk size %q: %w", line, err)
		}
		if chunkSize == 0 {
			cr.done = true
			_, _ = cr.source.Discard(2)
			return 0, io.EOF
		}
		cr.remain = chunkSize
	}
	toRead := min(int64(len(out)), cr.remain)
	n, err = cr.source.Read(out[:toRead])
	if err != nil {
		return n, fmt.Errorf("reading chunk data: %w", err)
	}
	cr.remain -= int64(n)
	if cr.remain == 0 {
		_, err = cr.source.Discard(2)
		if err != nil {
			return n, fmt.Errorf("discarding chunk CRLF: %w", err)
		}
	}
	return n, nil
}

func parseChunkSize(line string) (int64, error) {
	if idx := strings.IndexRune(line, ';'); idx >= 0 {
		line = line[:idx]
	}
	chunkSize, err := strconv.ParseInt(line, 16, 64)
	if err != nil {
		return 0, err
	}
	return chunkSize, nil
}
