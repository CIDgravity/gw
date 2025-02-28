package rbmeta

import (
	"encoding/json"
	"fmt"
	"net/http"

	iface "github.com/lotus-web3/ribs"
	"golang.org/x/xerrors"
)

type reqBody struct {
	Filepath *string `json:"filepath"`
	CID      *string `json:"cid"`
	Verbose  bool    `json:"verbose"`
}
type resFileInfoErr struct {
	Success bool   `json:"success"`
	Error   string `json:"error"`
}
type verboseDealDetailResult struct {
	Provider      string `json:"provider"`
	EndEpoch      *int64 `json:"endEpoch,omitempty"`
	DealID        *int64 `json:"dealId,omitempty"`
	IsRetrievable bool   `json:"isRetrievable"`
	State         string `json:"state"`
}
type verboseGrpDetailResult struct {
	Id                   string                    `json:"pieceCid,omitempty"`
	Deals                []verboseDealDetailResult `json:"deals,omitempty"`
	State                string                    `json:"state"`
	RetrievableCopies    int64                     `json:"retrievableCopies"`
	lastEndEpoch         int64
	isPartiallyOffloaded bool
	isFullyOffloaded     bool
}
type verboseDetailResult struct {
	Groups            []verboseGrpDetailResult `json:"groups"`
	State             string                   `json:"state"`
	RetrievableCopies int64                    `json:"retrievableCopies"`
	ExpirationEpoch   *int64                   `json:"expriationEpoch,omitempty"`
	ExpirationTs      *int64                   `json:"expriationTimestamp,omitempty"`
}
type resFileInfoDetail struct {
	CID     string               `json:"cid"`
	Path    string               `json:"path"`
	File    string               `json:"file"`
	Details *verboseDetailResult `json:"details,omitempty"`
}
type resFileResult struct {
	File resFileInfoDetail `json:"file"`
}
type resFileInfo struct {
	Success bool          `json:"success"`
	Result  resFileResult `json:"result"`
}

const (
	// Deal state
	DealStateProposed  = "proposed"
	DealStatePublished = "published"
	DealStateActive    = "active"

	// Group State
	GroupStateWritable      = "writable"
	GroupStateFull          = "full"
	GroupStateVRCARDone     = "full"
	GroupStateReadyForDeals = "ready_for_deals"
	GroupStateOffloaded     = "offloaded"
	GroupStateReload        = "reload"

	// File State
	FileStateStaging          = "staging"
	FileStateOffloading       = "offloading"
	FileStatePartiallyOffload = "partially_offloaded"
	FileStateOffloaded        = "offloaded"
)

/*
Deal status:
- proposed
- published
- active

Group status:
- writable
- full
- readyForDeal
- partially-offloaded
- offloaded

FileStatus
- staging
- offloading
- partially offloaded (all groups have at least 1 copy)
- offloaded


*/

const (
	FILECOIN_GENESIS_UNIX_EPOCH = 1598306400
)

func Epoch2Timestamp(epoch int64) int64 {
	return (epoch * 30) + FILECOIN_GENESIS_UNIX_EPOCH
}

func (mdb *metaDB) getFileDetails(fi *iface.FileMetadata) (*verboseDetailResult, error) {
	var ret verboseDetailResult
	ret.State = FileStateOffloading
	if fi.Groups == nil {
		ret.State = FileStateStaging
		return &ret, nil
	}
	for _, grp := range *fi.Groups {
		var grpDetails verboseGrpDetailResult
		meta, err := mdb.ribs.StorageDiag().GroupMeta(grp)
		if err != nil {
			log.Errorw("Failed to get group meta", "error", err)
			return nil, xerrors.Errorf("Failed to get details")
		}
		switch meta.State {
		case iface.GroupStateWritable:
			// Writable, so no deals
			grpDetails.State = GroupStateWritable
			ret.State = FileStateStaging
			ret.Groups = append(ret.Groups, grpDetails)
			continue
		case iface.GroupStateFull:
			// full, but not yet uploaded... still mark it as current for now
			grpDetails.State = GroupStateFull
			ret.State = FileStateStaging
			ret.Groups = append(ret.Groups, grpDetails)
			continue
		case iface.GroupStateVRCARDone:
			// full, but not yet uploaded... still mark it as current for now
			grpDetails.State = GroupStateVRCARDone
			ret.State = FileStateStaging
			ret.Groups = append(ret.Groups, grpDetails)
			continue
		case iface.GroupStateLocalReadyForDeals:
			grpDetails.State = GroupStateReadyForDeals
		case iface.GroupStateOffloaded:
			grpDetails.State = GroupStateOffloaded
			grpDetails.isFullyOffloaded = true
		case iface.GroupStateReload:
			grpDetails.State = GroupStateReload
		}
		grpDetails.Id = meta.PieceCID
		deals, err := mdb.ribs.DealDiag().GroupDeals(grp)
		if err != nil {
			log.Errorw("Failed to get group deals", "error", err)
			return nil, xerrors.Errorf("Failed to get details")
		}
		for _, deal := range deals {
			if deal.Failed {
				continue
			}
			var dealDetails verboseDealDetailResult
			dealDetails.Provider = fmt.Sprintf("f0%d", deal.Provider)
			dealDetails.State = DealStateProposed
			if deal.DealID != 0 {
				id := deal.DealID
				dealDetails.DealID = &id
				dealDetails.State = DealStatePublished
			}
			if deal.Sealed {
				grpDetails.isPartiallyOffloaded = true
				endEpoch := deal.EndEpoch
				dealDetails.EndEpoch = &endEpoch
				dealDetails.IsRetrievable = deal.RetrSuccess > 0 && !deal.NoRecentSuccess
				dealDetails.State = DealStateActive
				if dealDetails.IsRetrievable {
					grpDetails.RetrievableCopies += 1
					if deal.EndEpoch > grpDetails.lastEndEpoch {
						grpDetails.lastEndEpoch = deal.EndEpoch
					}
				}
			} else {
				grpDetails.isPartiallyOffloaded = true
			}
			grpDetails.Deals = append(grpDetails.Deals, dealDetails)
		}
		ret.Groups = append(ret.Groups, grpDetails)
	}
	if ret.State != FileStateStaging {
		partialOffload := true
		fullOffload := true
		retrievableCopies := ret.Groups[0].RetrievableCopies
		for _, grp := range ret.Groups {
			if grp.RetrievableCopies < retrievableCopies {
				retrievableCopies = grp.RetrievableCopies
			}
			if !grp.isPartiallyOffloaded {
				partialOffload = false
				fullOffload = false
			} else if !grp.isFullyOffloaded {
				fullOffload = false
			}
		}
		ret.RetrievableCopies = retrievableCopies
		if fullOffload {
			ret.State = FileStateOffloaded
		} else if partialOffload {
			ret.State = FileStatePartiallyOffload
		}
		if ret.RetrievableCopies > 0 {
			expiration := ret.Groups[0].lastEndEpoch
			for _, grp := range ret.Groups {
				if expiration > grp.lastEndEpoch {
					expiration = grp.lastEndEpoch
				}
			}
			ret.ExpirationEpoch = &expiration
			expTs := Epoch2Timestamp(expiration)
			ret.ExpirationTs = &expTs
		}
	}
	return &ret, nil
}

func (mdb *metaDB) getFileInfoHandler() func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var req reqBody
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		log.Debugw("Received", "Req", req)
		if req.Filepath == nil && req.CID == nil {
			log.Errorw("Filepath or CID are required")
			http.Error(w, "Filepath or CID are required", http.StatusBadRequest)
			return
		}

		if req.Filepath != nil && req.CID != nil {
			log.Errorw("Filepath and CID cannot be provided at the same time")
			http.Error(w, "Filepath and CID cannot be provided at the same time", http.StatusBadRequest)
			return
		}

		// handle case where filePath is provided
		if req.Filepath != nil {
			user, parent, name, err := SplitFilePath(*req.Filepath)

			filemeta, err := mdb.GetFileInfo(user, parent, name, nil)
			if err != nil {
				log.Errorw("handleFileInfo GetFileInfo", "error", err)
				http.Error(w, "Error retrieving fileinfo", http.StatusBadRequest)
				return
			}

			resp := mdb.buildResponse(filemeta, req.Verbose)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		}

		// handle case where CID is provided
		if req.CID != nil {
			filemeta, err := mdb.GetFileInfoFromCID(*req.CID, nil)
			if err != nil {
				log.Errorw("handleFileInfo GetFileInfoFromCID", "error", err)
				http.Error(w, "Error retrieving fileinfo with CID", http.StatusBadRequest)
				return
			}

			resp := mdb.buildResponse(filemeta, req.Verbose)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		}
	}
}

func (mdb *metaDB) buildResponse(filemeta *iface.FileMetadata, isVerbose bool) interface{} {
	if filemeta == nil {
		return resFileInfoErr{
			Success: false,
			Error:   "Not found",
		}
	}

	var details *verboseDetailResult
	var err error

	if isVerbose {
		details, err = mdb.getFileDetails(filemeta)

		if err != nil {
			return resFileInfoErr{
				Success: false,
				Error:   "Not found",
			}
		}
	}

	return resFileInfo{
		Success: true,
		Result: resFileResult{
			File: resFileInfoDetail{
				CID:     *filemeta.Cid,
				Path:    *filemeta.ParentPath,
				File:    *filemeta.Filename,
				Details: details,
			},
		},
	}
}

func (mdb *metaDB) LaunchServer() error {
	if mdb.conn == nil {
		return nil
	}
	http.HandleFunc("/file-info", mdb.getFileInfoHandler())

	go func() {
		log.Fatal(http.ListenAndServe(":9011", nil))
	}()
	return nil
}
