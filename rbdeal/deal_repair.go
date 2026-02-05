package rbdeal

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	agw "github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/CIDgravity/filecoin-gateway/ributil"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
)

/*
REPAIR WORKERS:
* check if they have active work
* if not, manage repair queue
* go to top

If they have active work:
* Fetch group data via HTTP from storage providers
* Verify PieceCID matches
* Re-import to local storage

Tables:
* repair_queue: group, retrievable_deals, assigned_worker, last_attempt
*/

var RepairCheckInterval = time.Minute

func (r *ribs) startRepairWorkers(ctx context.Context) {
	cfg := configuration.GetConfig()

	// Use configured path or default to repairDir (set in ribs constructor)
	stagingPath := cfg.Ribs.RepairStagingPath
	if stagingPath == "" {
		stagingPath = r.repairDir
	}

	if stagingPath == "" {
		log.Info("repair workers disabled: no staging path configured")
		return
	}

	// Update repairDir to the resolved path
	r.repairDir = stagingPath

	// Ensure staging directory exists
	if err := os.MkdirAll(stagingPath, 0755); err != nil {
		log.Errorw("failed to create repair staging directory, repair workers disabled", "error", err, "path", stagingPath)
		return
	}

	workers := cfg.Ribs.RepairWorkers
	if workers <= 0 {
		workers = 4 // default
	}

	log.Infow("starting repair workers", "workers", workers, "stagingPath", stagingPath)

	for i := 0; i < workers; i++ {
		go r.repairWorker(ctx, i)
	}
}

func (r *ribs) repairWorker(ctx context.Context, workerID int) {
	for {
		select {
		case <-r.close:
			return
		case <-ctx.Done():
			return
		default:
		}

		err := r.repairStep(ctx, workerID)
		if err != nil {
			log.Errorw("repair step failed", "error", err, "worker", workerID)

			if err := r.db.UpdateRepairOnStepNotDone(workerID); err != nil {
				log.Errorw("unassigning worker from failed repair", "worker", workerID)
			}
		}
	}
}

func (r *ribs) repairStep(ctx context.Context, workerID int) error {
	assignedGroups, err := r.db.GetAssignedRepairWorkByWorkerID(workerID)
	if err != nil {
		return xerrors.Errorf("get assigned work: %w", err)
	}

	if len(assignedGroups) > 1 {
		log.Warnw("repair worker has more than one assigned group", "worker", workerID, "groups", len(assignedGroups))
	}

	var assigned *agw.GroupKey

	if len(assignedGroups) == 0 {
		if err := r.db.AddRepairsForLowRetrievableDeals(); err != nil {
			return xerrors.Errorf("AddRepairsForLowRetrievableDeals: %w", err)
		}

		assigned, err = r.db.AssignRepairToWorker(workerID)
		if err != nil {
			return xerrors.Errorf("assign repair to worker: %w", err)
		}
	} else {
		assigned = &assignedGroups[0]
	}

	if assigned == nil {
		select {
		case <-r.close:
		case <-ctx.Done():
		case <-time.After(RepairCheckInterval):
		}

		return nil
	}

	log.Infow("starting repair for group", "group", *assigned, "worker", workerID)

	// fetch group if not fetched
	groupFile, err := r.fetchGroupForRepair(ctx, workerID, *assigned)
	if err != nil {
		return xerrors.Errorf("fetch group (group %d): %w", *assigned, err)
	}

	groupReader, err := os.OpenFile(groupFile, os.O_RDONLY, 0644)
	if err != nil {
		return xerrors.Errorf("opening repair .car file: %w", err)
	}
	defer groupReader.Close()

	st, err := groupReader.Stat()
	if err != nil {
		return xerrors.Errorf("stat repair file: %w", err)
	}

	// here we have the group fetched and verified
	log.Infow("importing repaired group", "group", *assigned, "size", st.Size(), "worker", workerID)

	err = r.RBS.Storage().LoadFilCar(ctx, *assigned, groupReader, st.Size())
	if err != nil {
		return xerrors.Errorf("reload data file (group %d): %w", *assigned, err)
	}

	if err := r.db.DelRepair(*assigned); err != nil {
		return xerrors.Errorf("marking group %d as repaired: %w", *assigned, err)
	}

	// remove repair file
	if err := os.Remove(groupFile); err != nil {
		log.Warnw("failed to remove repair file", "error", err, "file", groupFile)
	}

	log.Infow("repair complete", "group", *assigned, "worker", workerID)

	return nil
}

func (r *ribs) fetchGroupForRepair(ctx context.Context, workerID int, group agw.GroupKey) (string, error) {
	rstat := agw.RepairJob{
		GroupKey:      group,
		State:         agw.RepairJobStateFetching,
		FetchProgress: 0,
		FetchSize:     0,
	}

	r.repairStatsLk.Lock()
	r.repairStats[workerID] = &rstat
	r.repairStatsLk.Unlock()

	workerDir := filepath.Join(r.repairDir, fmt.Sprintf("w%d", workerID))

	if err := os.MkdirAll(workerDir, 0755); err != nil {
		return "", xerrors.Errorf("mkdir repair worker dir: %w", err)
	}

	groupFile := filepath.Join(workerDir, fmt.Sprintf("group-%d.car", group))

	// Check if file already exists and is complete (from a previous interrupted attempt)
	if fi, err := os.Stat(groupFile); err == nil {
		gm, err := r.Storage().DescibeGroup(ctx, group)
		if err == nil && fi.Size() == gm.CarSize {
			// File exists and is the right size, verify it
			if err := r.verifyGroupFile(groupFile, gm.PieceCid); err == nil {
				log.Infow("using existing repair file", "group", group, "file", groupFile)
				return groupFile, nil
			}
			// File is corrupt, remove it
			_ = os.Remove(groupFile)
		}
	}

	if err := r.fetchGroupHttp(ctx, workerID, group, groupFile); err != nil {
		return "", xerrors.Errorf("http fetch failed: %w", err)
	}

	return groupFile, nil
}

func (r *ribs) verifyGroupFile(groupFile string, expectedPieceCid cid.Cid) error {
	f, err := os.Open(groupFile)
	if err != nil {
		return xerrors.Errorf("open file: %w", err)
	}
	defer f.Close()

	cc := new(ributil.DataCidWriter)
	if _, err := io.Copy(cc, f); err != nil {
		return xerrors.Errorf("read file: %w", err)
	}

	dc, err := cc.Sum()
	if err != nil {
		return xerrors.Errorf("compute piece cid: %w", err)
	}

	if dc.PieceCID != expectedPieceCid {
		return xerrors.Errorf("piece cid mismatch: got %s, expected %s", dc.PieceCID, expectedPieceCid)
	}

	return nil
}

func (r *ribs) updateRepairStats(worker int, cb func(*agw.RepairJob)) {
	r.repairStatsLk.Lock()
	defer r.repairStatsLk.Unlock()

	if r.repairStats[worker] != nil {
		cb(r.repairStats[worker])
	}
}

func (r *ribs) fetchGroupHttp(ctx context.Context, workerID int, group agw.GroupKey, groupFile string) error {
	cc, err := r.retrProv.retrievalCandidatesForGroupCached(group)
	if err != nil {
		return xerrors.Errorf("failed to get retrieval candidates: %w", err)
	}
	candidates := cc.candidates

	gm, err := r.Storage().DescibeGroup(ctx, group)
	if err != nil {
		return xerrors.Errorf("failed to get group metadata: %w", err)
	}

	r.updateRepairStats(workerID, func(r *agw.RepairJob) {
		r.FetchSize = gm.CarSize
	})

	type retrievalSource struct {
		provider string
		reqUrl   url.URL
	}

	var sources []retrievalSource

	// Check for local import URL override (useful for manual recovery)
	envName := fmt.Sprintf("RIBS_IMPORT_%d", group)
	if importUrl, ok := os.LookupEnv(envName); ok {
		u, err := url.Parse(importUrl)
		if err != nil {
			log.Warnw("failed to parse import url", "error", err, "url", importUrl)
		} else {
			sources = append(sources, retrievalSource{
				provider: "local",
				reqUrl:   *u,
			})
		}
	}

	// Add HTTP retrieval sources from storage providers
	for _, candidate := range candidates {
		addrInfo, err := r.retrProv.getAddrInfoCached(candidate.Provider)
		if err != nil {
			log.Debugw("failed to get addrinfo", "provider", candidate.Provider, "err", err)
			continue
		}

		if len(addrInfo.HttpMaddrs) == 0 {
			continue
		}

		u, err := ributil.MaddrsToUrl(addrInfo.HttpMaddrs)
		if err != nil {
			log.Warnw("failed to parse addrinfo", "provider", candidate.Provider, "err", err)
			continue
		}

		reqUrl := *u
		reqUrl.Path = path.Join(reqUrl.Path, "piece", gm.PieceCid.String())

		sources = append(sources, retrievalSource{
			provider: fmt.Sprint(candidate.Provider),
			reqUrl:   reqUrl,
		})
	}

	if len(sources) == 0 {
		return xerrors.Errorf("no HTTP retrieval sources available for group %d", group)
	}

	log.Infow("attempting repair retrieval", "group", group, "sources", len(sources), "worker", workerID)

	var lastErr error
	for _, source := range sources {
		r.updateRepairStats(workerID, func(r *agw.RepairJob) {
			r.State = agw.RepairJobStateFetching
			r.FetchProgress = 0
			r.FetchUrl = source.reqUrl.String()
		})

		log.Infow("trying repair source", "url", source.reqUrl.String(), "group", group, "provider", source.provider)

		err := r.fetchFromSource(ctx, workerID, group, groupFile, source.reqUrl, gm)
		if err == nil {
			return nil
		}

		lastErr = err
		log.Warnw("repair source failed", "error", err, "provider", source.provider, "group", group)
	}

	return xerrors.Errorf("all retrieval sources failed, last error: %w", lastErr)
}

func (r *ribs) fetchFromSource(ctx context.Context, workerID int, group agw.GroupKey, groupFile string, reqUrl url.URL, gm agw.GroupDesc) error {
	robustReqReader := ributil.RobustGet(reqUrl.String(), gm.CarSize, func() *ributil.RateCounter {
		return r.repairFetchCounters.Get(group)
	})
	defer robustReqReader.Close()

	// Create output file
	f, err := os.OpenFile(groupFile, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		return xerrors.Errorf("open group file: %w", err)
	}

	// Progress monitoring goroutine
	progressCtx, cancelProgress := context.WithCancel(ctx)
	defer cancelProgress()

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-progressCtx.Done():
				return
			case <-ticker.C:
				fi, err := f.Stat()
				if err == nil {
					r.updateRepairStats(workerID, func(r *agw.RepairJob) {
						r.FetchProgress = fi.Size()
					})
				}
			}
		}
	}()

	// Create repair reader that can fetch individual blocks on error
	repairReader, err := ributil.NewCarRepairReader(robustReqReader, gm.RootCid, func(b cid.Cid, badData []byte) ([]byte, error) {
		var outData []byte
		err := r.retrProv.FetchBlocks(ctx, group, []multihash.Multihash{b.Hash()}, func(cidx int, data []byte) {
			outData = make([]byte, len(data))
			copy(outData, data)
		})
		if err != nil {
			return nil, xerrors.Errorf("fetch repair block: %w", err)
		}
		return outData, nil
	})
	if err != nil {
		_ = f.Close()
		_ = os.Remove(groupFile)
		return xerrors.Errorf("create repair reader: %w", err)
	}

	// Stream through piece CID calculator
	cc := new(ributil.DataCidWriter)
	commdReader := io.TeeReader(repairReader, cc)

	_, err = io.Copy(f, commdReader)
	cancelProgress()

	if err != nil {
		_ = f.Close()
		_ = os.Remove(groupFile)
		return xerrors.Errorf("copy data: %w", err)
	}

	if err := f.Close(); err != nil {
		_ = os.Remove(groupFile)
		return xerrors.Errorf("close group file: %w", err)
	}

	// Verify piece CID
	r.updateRepairStats(workerID, func(r *agw.RepairJob) {
		r.FetchProgress = r.FetchSize
		r.State = agw.RepairJobStateVerifying
	})

	dc, err := cc.Sum()
	if err != nil {
		_ = os.Remove(groupFile)
		return xerrors.Errorf("compute piece cid: %w", err)
	}

	if dc.PieceCID != gm.PieceCid {
		_ = os.Remove(groupFile)
		return xerrors.Errorf("piece cid mismatch: got %s, expected %s", dc.PieceCID, gm.PieceCid)
	}

	r.updateRepairStats(workerID, func(r *agw.RepairJob) {
		r.State = agw.RepairJobStateImporting
	})

	log.Infow("repair fetch complete", "group", group, "pieceCid", dc.PieceCID, "size", dc.PayloadSize, "worker", workerID)

	return nil
}
