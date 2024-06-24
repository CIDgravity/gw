package rbmeta

import (
	"encoding/json"
	"net/http"
)


type reqBody struct {
	FilePath  string `json:"filePath"`
	FileOwner string `json:"fileOwner"`
}
type resFileInfoDetail struct {
	CID     string `json:"cid"`
}
type resFileInfoErr struct {
	Success bool   `json:"success"`
	Error   string `json:"error"`
}
type resFileInfo struct {
	Success bool              `json:"success"`
	Info    resFileInfoDetail `json:"file"`
}

func getFileInfoHandler(mdb *metaDB) (func (w http.ResponseWriter, r *http.Request)) {
	return func (w http.ResponseWriter, r *http.Request) {
		var req reqBody
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
	
		log.Debugw("Received", "Req", req)
		user, parent, name, err := SplitFilePath(req.FilePath)
		if user != req.FileOwner {
			log.Warnw("Inconsistent fileowner, but ignored", "user", user, "provided-owner", req.FileOwner)
		}
		filemeta, err := mdb.GetFileInfo(user, parent, name)
		if err != nil {
			log.Errorw("handleFileInfo GetFileInfo", "error", err)
			http.Error(w, "Error retrieving fileinfo", http.StatusBadRequest)
			return
		}
	
		w.Header().Set("Content-Type", "application/json")
		if filemeta == nil {
			respBody := resFileInfoErr{
				Success: false,
				Error: "File not found",
			}
			json.NewEncoder(w).Encode(respBody)
			return
		}
		respBody := resFileInfo{
			Success: true,
			Info: resFileInfoDetail{
				CID: *filemeta.Cid,
			},
		}
		json.NewEncoder(w).Encode(respBody)
	}
}


func (mdb *metaDB) LaunchServer() error {
	http.HandleFunc("/file-info", getFileInfoHandler(mdb))

	go func() {
		log.Fatal(http.ListenAndServe(":9011", nil))
	}()
	return nil
}
