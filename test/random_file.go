package test

import (
	"crypto/sha256"
	"errors"
	"hash"
	"io"
	"math/rand"
	"testing"

	"github.com/test-go/testify/require"
)

type RandomFile struct {
	size int64
	seed int64

	pos int64
	r   *rand.Rand
	h   hash.Hash

	buf []byte
}

func NewRandomFile(t *testing.T, size int64) *RandomFile {
	seed := rand.Int63()
	t.Logf("Random file seed: %d", seed)
	s := &RandomFile{
		size: size,
		seed: seed,
		h:    sha256.New(),
		buf:  make([]byte, 1<<20), // 1 MiB
	}
	s.reset()
	return s
}

func (s *RandomFile) Read(p []byte) (int, error) {
	if s.pos >= s.size {
		return 0, io.EOF
	}
	if int64(len(p)) > s.size-s.pos {
		p = p[:s.size-s.pos]
	}

	n, _ := s.r.Read(p)
	s.h.Write(p[:n])
	s.pos += int64(n)

	if s.pos >= s.size {
		return n, io.EOF
	}
	return n, nil
}

func (s *RandomFile) Seek(offset int64, whence int) (int64, error) {
	var target int64
	switch whence {
	case io.SeekStart:
		target = offset
	case io.SeekCurrent:
		target = s.pos + offset
	case io.SeekEnd:
		target = s.size + offset
	default:
		return 0, errors.New("invalid whence")
	}
	if target < 0 || target > s.size {
		return 0, errors.New("seek out of range")
	}

	// Reset to the beginning, then consume up to target.
	s.reset()
	if target > 0 {
		if err := s.fastForward(target); err != nil {
			return 0, err
		}
	}
	return s.pos, nil
}

func (s *RandomFile) Sum(t *testing.T) string {
	if s.pos < s.size {
		t.Fatal("Random file not fully consumed!")
	}

	return string(s.h.Sum(nil))
}

func (s *RandomFile) Bytes(t *testing.T) []byte {
	_, err := s.Seek(0, io.SeekStart)
	require.NoError(t, err)
	bytes, err := io.ReadAll(s)
	require.NoError(t, err)
	return bytes
}

func (s *RandomFile) reset() {
	s.r = rand.New(rand.NewSource(s.seed))
	s.h.Reset()
	s.pos = 0
}

func (s *RandomFile) fastForward(n int64) error {
	for n > 0 {
		chunk := int64(len(s.buf))
		if chunk > n {
			chunk = n
		}
		s.r.Read(s.buf[:chunk])
		s.h.Write(s.buf[:chunk])
		s.pos += chunk
		n -= chunk
	}
	return nil
}
