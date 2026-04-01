package s3

import (
	"net/http"
	"net/url"
	"time"

	"github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/CIDgravity/filecoin-gateway/rbstor"
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

// responseRecorder wraps http.ResponseWriter to capture the response size
type responseRecorder struct {
	http.ResponseWriter
	bytesWritten int64
}

func (rr *responseRecorder) Write(b []byte) (int, error) {
	n, err := rr.ResponseWriter.Write(b)
	rr.bytesWritten += int64(n)
	return n, err
}

func (srv *S3Server) handleGet(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	metrics := rbstor.GetClusterMetrics()
	metrics.StartRead()
	defer metrics.EndRead()

	// Wrap response writer to track bytes
	rr := &responseRecorder{ResponseWriter: w}

	params, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		goto out
	}

	if params.Has("location") {
		err = srv.handleGetLocation(rr, r)
	} else if params.Get("list-type") == "2" {
		err = srv.handleListObjects(rr, r)
	} else if params.Has("uploadId") {
		err = srv.handleListParts(rr, r)
	} else {
		err = srv.handleGetObject(rr, r)
	}

out:
	latencyMs := float64(time.Since(start).Milliseconds())
	metrics.RecordRead(latencyMs, rr.bytesWritten, err)
	if err != nil {
		log.Errorw("error handling HTTP GET request", "URL", r.URL, "error", err.Error())
		w.WriteHeader(500)
	}
}

func (srv *S3Server) handlePut(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	metrics := rbstor.GetClusterMetrics()
	metrics.StartWrite()
	defer metrics.EndWrite()

	// Get content length for byte tracking
	contentLength := r.ContentLength
	if contentLength < 0 {
		contentLength = 0
	}

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
	latencyMs := float64(time.Since(start).Milliseconds())
	metrics.RecordWrite(latencyMs, contentLength, err)
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
	start := time.Now()
	metrics := rbstor.GetClusterMetrics()
	metrics.StartRead()
	defer metrics.EndRead()

	err := srv.handleHeadObject(w, r)

	latencyMs := float64(time.Since(start).Milliseconds())
	// HEAD requests don't transfer body bytes
	metrics.RecordRead(latencyMs, 0, err)
	if err != nil {
		log.Errorf("Error handling head request: %s", err)
		w.WriteHeader(500)
	}
}

func (srv *S3Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}
