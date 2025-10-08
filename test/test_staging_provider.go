package test

import (
	"bytes"
	"context"
	"io"
	"sync"

	"golang.org/x/xerrors"
)

type StagingProvider struct {
	lk sync.Mutex

	bdata []byte
}

func (t *StagingProvider) Upload(ctx context.Context, size int64, src func(writer io.Writer) error) error {
	t.lk.Lock()
	defer t.lk.Unlock()

	if len(t.bdata) > 0 {
		return xerrors.New("had data")
	}

	var buf bytes.Buffer

	err := src(&buf)
	if err != nil {
		return err
	}

	t.bdata = buf.Bytes()

	return nil
}

func (t *StagingProvider) ReadAt(p []byte, off int64) (n int, err error) {
	t.lk.Lock()
	defer t.lk.Unlock()
	n = copy(p, t.bdata[off:])
	if n != len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (t *StagingProvider) Has(ctx context.Context) (bool, error) {
	return true, nil
}

func (t *StagingProvider) URL(ctx context.Context) (string, error) {
	return "http://aaaaaaa", nil
}
