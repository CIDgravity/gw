package rbmeta

import (
	"fmt"
	"encoding/json"
	"net/http"
	"golang.org/x/xerrors"
        iface "github.com/lotus-web3/ribs"
)


type reqBody struct {
	Filepath  string `json:"filepath"`
	User      string `json:"User"`
	Verbose   bool   `json:"verbose"`
}
type resFileInfoErr struct {
	Success bool   `json:"success"`
	Error   string `json:"error"`
}
type verboseDealDetailResult struct {
	Provider string  `json:"provider"`
	Expire   *string `json:"expire"`
	DealID   *int64  `json:"dealId"`
	Status   string  `json:"status"`
}
type verboseGrpDetailResult struct {
	Id     string                `json:"id"`
	Deals  []verboseDealDetailResult `json:"deals"`
	Status string                `json:"status"`
}
type verboseDetailResult struct {
	Groups []verboseGrpDetailResult `json:"blocks"`
	Status string                   `json:"status"`
}
type resFileInfoDetail struct {
	CID     string               `json:"cid"`
	Details *verboseDetailResult `json:"details"`
}
type resFileResult struct {
	File resFileInfoDetail `json:"file"`
}
type resFileInfo struct {
	Success bool          `json:"success"`
	Result  resFileResult `json:"result"`
}

func (mdb *metaDB) getFileDetails(fi *iface.FileMetadata) (*verboseDetailResult, error) {
	var ret verboseDetailResult
	if fi.Groups == nil {
		ret.Status = "Unknown groups. Try again later."
		return &ret, nil
	}
	for _, grp := range *fi.Groups {
		var grpDetails verboseGrpDetailResult
		meta, err := mdb.ribs.StorageDiag().GroupMeta(grp)
		if err != nil {
			log.Errorw("Failed to get group meta", "error", err)
			return nil, xerrors.Errorf("Failed to get details")
		}
		if meta.State == iface.GroupStateWritable {
			// Writable, so no deals
			grpDetails.Status = "current"
			ret.Groups = append(ret.Groups, grpDetails)
			continue
		}
		if meta.State == iface.GroupStateFull {
			// full, but not yet uploaded... still mark it as current for now
			grpDetails.Status = "inprogress"
			ret.Groups = append(ret.Groups, grpDetails)
			continue
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
			if deal.DealID != 0 {
				id := deal.DealID
				dealDetails.DealID = &id
			}
			if deal.Sealed {
				expire := fmt.Sprintf("%d", deal.EndEpoch)
				dealDetails.Expire = &expire
			}
			grpDetails.Deals = append(grpDetails.Deals, dealDetails)
		}
		ret.Groups = append(ret.Groups, grpDetails)
	}
	return &ret, nil
}

func (mdb *metaDB) getFileInfoHandler() (func (w http.ResponseWriter, r *http.Request)) {
	return func (w http.ResponseWriter, r *http.Request) {
		var req reqBody
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		log.Debugw("Received", "Req", req)
		user, parent, name, err := SplitFilePath(req.Filepath)
		/* XXX: user not linked to ribs user
		if user != req.FileOwner {
			log.Warnw("Inconsistent fileowner, but ignored", "user", user, "provided-owner", req.FileOwner)
		}
		*/
		filemeta, err := mdb.GetFileInfo(user, parent, name, nil)
		if err != nil {
			log.Errorw("handleFileInfo GetFileInfo", "error", err)
			http.Error(w, "Error retrieving fileinfo", http.StatusBadRequest)
			return
		}

		if filemeta == nil {
			respBody := resFileInfoErr{
				Success: false,
				Error: "File not found",
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(respBody)
			return
		}
		var details *verboseDetailResult
		if req.Verbose {
			details, err = mdb.getFileDetails(filemeta)
		}
		w.Header().Set("Content-Type", "application/json")
		respBody := resFileInfo{
			Success: true,
			Result: resFileResult{
				File: resFileInfoDetail{
					CID: *filemeta.Cid,
					Details: details,
				},
			},
		}
		json.NewEncoder(w).Encode(respBody)
	}
}


func (mdb *metaDB) LaunchServer() error {
	http.HandleFunc("/file-info", mdb.getFileInfoHandler())

	go func() {
		log.Fatal(http.ListenAndServe(":9011", nil))
	}()
	return nil
}
