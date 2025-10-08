package s3

import (
	"net/http"
	"net/url"

	"github.com/CIDgravity/filecoin-gateway/iface"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("gw/s3")

type S3Server struct {
	region iface.Region
	auth   *Authenticator
}

func NewS3Server(region iface.Region, auth *Authenticator) *S3Server {
	return &S3Server{
		region: region,
		auth:   auth,
	}
}

func (srv *S3Server) handleGet(w http.ResponseWriter, r *http.Request) {
	params, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		goto out
	}

	if params.Has("location") {
		err = srv.handleGetLocation(w, r)
	} else if params.Get("list-type") == "2" {
		err = srv.handleListObjects(w, r)
	} else {
		err = srv.handleGetObject(w, r)
	}

out:
	if err != nil {
		log.Errorw("error handling HTTP GET request", "URL", r.URL, "error", err.Error())
		w.WriteHeader(500)
	}
}

func (srv *S3Server) handlePut(w http.ResponseWriter, r *http.Request) {
	params, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		goto out
	}

	if params.Has("partNumber") && params.Has("uploadId") {
		err = srv.handleUploadPart(w, r)
	} else if len(params) == 0 || params.Get("x-id") == "PutObject" {
		err = srv.handlePutObject(w, r)
	} else {
		w.WriteHeader(404)
		return
	}

out:
	if err != nil {
		log.Errorw("Error handling HTTP PUT request", "URL", r.URL, "error", err.Error())
		w.WriteHeader(500)
	}
}

func (srv *S3Server) handlePost(w http.ResponseWriter, r *http.Request) {
	params, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		goto out
	}

	if params.Has("uploads") {
		err = srv.handleCreateMultipartUpload(w, r)
	} else if params.Has("uploadId") {
		err = srv.handleCompleteMultipartUpload(w, r)
	} else {
		w.WriteHeader(404)
		return
	}

out:
	if err != nil {
		log.Errorw("Error handling HTTP POST request", "URL", r.URL, "error", err.Error())
		w.WriteHeader(500)
	}
}

func (srv *S3Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	params, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		goto out
	}

	if params.Has("uploadId") {
		err = srv.handleAbortMultipartUpload(w, r)
	} else {
		err = srv.handleDeleteObject(w, r)
	}

out:
	if err != nil {
		log.Errorf("Error handling delete request: %s", err)
		w.WriteHeader(500)
	}
}

func (srv *S3Server) handleHead(w http.ResponseWriter, r *http.Request) {
	err := srv.handleHeadObject(w, r)

	if err != nil {
		log.Errorf("Error handling head request: %s", err)
		w.WriteHeader(500)
	}
}
