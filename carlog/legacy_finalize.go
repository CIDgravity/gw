package carlog

import (
	"context"
	"path/filepath"

	"golang.org/x/xerrors"
)

// FinalizeLegacyLocal finalizes a read-only car log the way pre-staging
// versions did when no staging storage was configured: a hash-sorted bsst
// index (index.bsst) is written from the current read index, the data file
// stays local, and the head is marked Finalized without External.
//
// This is the on-disk layout that long-lived deployments accumulated before
// staging storage existed; Open retains a read path for it (the h.Finalized
// && !h.External branch). It exists so migration tests (migrate/goldenrepo)
// can generate old-format group directories with current code; new groups
// must keep using Finalize.
func (j *CarLog) FinalizeLegacyLocal(_ context.Context) error {
	j.idxLk.Lock()

	if j.finalizing {
		j.idxLk.Unlock()
		return xerrors.Errorf("already finalizing")
	}
	j.finalizing = true

	if j.wIdx != nil {
		j.idxLk.Unlock()
		return xerrors.Errorf("cannot finalize read-write jbob")
	}

	var fin, hasTop bool
	err := j.mutHead(func(h *Head) error {
		fin = h.Finalized
		hasTop = len(h.LayerOffsets) > 0
		return nil
	})
	if err != nil {
		j.idxLk.Unlock()
		return xerrors.Errorf("checking if finalized: %w", err)
	}

	if fin {
		j.idxLk.Unlock()
		return nil
	}

	j.idxLk.Unlock()

	bss, err := CreateBSSTIndex(filepath.Join(j.IndexPath, BsstIndex), j.rIdx)
	if err != nil {
		return xerrors.Errorf("creating bsst index: %w", err)
	}

	if err := SaveMHList(filepath.Join(j.IndexPath, HashSample), bss.bsi.CreateSample); err != nil {
		return xerrors.Errorf("saving hash sample: %w", err)
	}

	j.idxLk.Lock()
	defer j.idxLk.Unlock()

	err = j.mutHead(func(h *Head) error {
		h.Finalized = true
		return nil
	})
	if err != nil {
		return xerrors.Errorf("marking as finalized: %w", err)
	}

	err = j.rIdx.Close()
	j.rIdx = bss
	if err != nil {
		return err
	}
	if err := j.dropLevel(); err != nil {
		return xerrors.Errorf("drop write index: %w", err)
	}

	if !hasTop {
		j.idxLk.Unlock()
		err := j.genTopCar()
		j.idxLk.Lock()
		if err != nil {
			return xerrors.Errorf("generating top car: %w", err)
		}
	}

	return nil
}
