package s3

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"go.uber.org/fx"
	"go.uber.org/zap/zapcore"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func LoggingMiddleware(next http.Handler) http.Handler {
	if log.Level() != zapcore.DebugLevel {
		return next
	}
	log.Debug("http logging middleware enabled")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Debugw("Http request",
			"Method", r.Method,
			"URL", r.URL.String(),
			"Headers", r.Header)
		next.ServeHTTP(w, r)
	})
}

func startS3Server(lc fx.Lifecycle, ctx *Context) {
	log.Info("Starting S3 plugin server")

	mux := http.NewServeMux()
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		handleGet(ctx, w, r)
	})
	mux.HandleFunc("PUT /", func(w http.ResponseWriter, r *http.Request) {
		handlePut(ctx, w, r)
	})
	mux.HandleFunc("POST /", func(w http.ResponseWriter, r *http.Request) {
		handlePost(ctx, w, r)
	})
	mux.HandleFunc("DELETE /", func(w http.ResponseWriter, r *http.Request) {
		handleDelete(ctx, w, r)
	})

	srv := &http.Server{
		Addr:    ":8078",
		Handler: LoggingMiddleware(mux),
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go func() {
				fmt.Println("S3 http at http://localhost:8078")
				err := srv.ListenAndServe()
				if errors.Is(err, http.ErrServerClosed) {
					log.Info("S3 http server closed")
				} else if err != nil {
					log.Errorf("failed to start S3 plugin http server: %s", err)
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			return srv.Shutdown(ctx)
		},
	})
	log.Infow("S3 plugin: Started")
}

func handleGet(ctx *Context, w http.ResponseWriter, r *http.Request) {
	params, _ := url.ParseQuery(r.URL.RawQuery)

	var err error
	if params.Has("location") {
		err = handleGetLocation(w, r)
	} else if params.Get("list-type") == "2" {
		err = handleListObjects(w, r)
	} else {
		err = handleGetObject(ctx, w, r)
	}

	if err != nil {
		log.Errorf("Error handling get request: %s", err)
		w.WriteHeader(500)
	}
}

func handlePut(ctx *Context, w http.ResponseWriter, r *http.Request) {
	params, _ := url.ParseQuery(r.URL.RawQuery)

	var err error
	if params.Has("partNumber") && params.Has("uploadId") {
		err = handleUploadPart(ctx, w, r)
	} else if len(params) == 0 {
		err = handlePutObject(ctx, w, r)
	} else {
		w.WriteHeader(404)
		return
	}

	if err != nil {
		log.Errorf("Error handling put request: %s", err)
		w.WriteHeader(500)
	}
}

func handlePost(ctx *Context, w http.ResponseWriter, r *http.Request) {
	params, _ := url.ParseQuery(r.URL.RawQuery)

	var err error
	if params.Has("uploads") {
		err = handleCreateMultipartUpload(w, r)
	} else if params.Has("uploadId") {
		err = handleCompleteMultipartUpload(ctx, w, r)
	} else {
		w.WriteHeader(404)
		return
	}

	if err != nil {
		log.Errorf("Error handling post request: %s", err)
		w.WriteHeader(500)
	}
}

func handleDelete(ctx *Context, w http.ResponseWriter, r *http.Request) {
	params, _ := url.ParseQuery(r.URL.RawQuery)
	var err error
	if params.Has("uploadId") {
		err = handleAbortMultipartUpload(w, r)
	} else {
		err = handleDeleteObject(ctx, w, r)
	}

	if err != nil {
		log.Errorf("Error handling delete request: %s", err)
		w.WriteHeader(500)
	}
}

func handleGetLocation(w http.ResponseWriter, r *http.Request) error {
	return locationTemplate.Execute(w, nil)
}

func handleListObjects(w http.ResponseWriter, r *http.Request) error {
	// endpoint used only to check if a bucket is currently empty
	// validated by s3 when adding and removing a warm backend
	bucket := strings.TrimLeft(r.URL.Path, "/")
	return listObjectsTemplate.Execute(w, listObjectsResponseParams{Name: bucket})
}

func handleGetObject(ctx *Context, w http.ResponseWriter, r *http.Request) error {
	log.Infow("handleGetObject", "key", r.URL.Path)

	fileReader, err := ctx.getObject(r.URL.Path)
	if errors.Is(objectNotFoundErr, err) {
		w.WriteHeader(404)
		log.Warnf("Object not found: %s", r.URL.Path)
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to to open object: %w", err)
	}
	defer fileReader.Close()
	w.Header().Set("Last-Modified", FormatDateTime(time.Now()))

	http.ServeContent(w, r, "", time.Now(), fileReader)
	return nil
}

func handleUploadPart(ctx *Context, w http.ResponseWriter, r *http.Request) error {
	//todo verify data integrity with the Content-Md5 header
	log.Infow("handleUploadPart", "key", r.URL.Path)
	params, _ := url.ParseQuery(r.URL.RawQuery)
	uploadId := params.Get("uploadId")
	_, err := strconv.ParseInt(params.Get("partNumber"), 10, 64)

	if err != nil || uploadId == "" {
		return fmt.Errorf("invalid upload parameters: %s", params)
	}

	if err := validateChunkedPayload(r); err != nil {
		return err
	}

	cid, err := ctx.putPart(newChunkReader(r.Body))
	if err != nil {
		return err
	}
	w.Header().Set("Etag", cid)
	return nil
}

func handlePutObject(ctx *Context, w http.ResponseWriter, r *http.Request) error {
	//todo verify data integrity with the Content-Md5 header
	log.Infow("handlePutObject", "key", r.URL.Path)
	if err := validateChunkedPayload(r); err != nil {
		return err
	}
	return ctx.putObject(r.URL.Path, newChunkReader(r.Body))
}

func handleCreateMultipartUpload(w http.ResponseWriter, r *http.Request) error {
	uploadId := uuid.New().String()
	segments := strings.Split(strings.TrimLeft(r.URL.Path, "/"), "/")
	bucket := segments[0]
	key := strings.Join(segments[1:], "/")

	return createMultipartUploadTemplate.Execute(w, createMultipartUploadResponseParams{UploadId: uploadId, Key: key, Bucket: bucket})
}

type CompleteMultipartUpload struct {
	XMLName xml.Name `xml:"CompleteMultipartUpload"`
	Parts   []struct {
		PartNumber int    `xml:"PartNumber"`
		ETag       string `xml:"ETag"`
	} `xml:"Part"`
}

func handleCompleteMultipartUpload(ctx *Context, w http.ResponseWriter, r *http.Request) error {
	log.Infow("handleCompleteMultipartUpload", "key", r.URL.Path)
	segments := strings.Split(strings.TrimLeft(r.URL.Path, "/"), "/")
	bucket := segments[0]
	key := strings.Join(segments[1:], "/")

	var completeReq CompleteMultipartUpload
	if err := xml.NewDecoder(r.Body).Decode(&completeReq); err != nil {
		w.WriteHeader(400)
		return fmt.Errorf("failed to decode complete multipart upload request: %w", err)
	}

	cid, err := ctx.completeMultipartPut(r.URL.Path, completeReq)
	if err != nil {
		return err
	}

	return completeMultipartUploadTemplate.Execute(w, completeMultipartUploadResponseParams{
		Bucket: bucket,
		Key:    key,
		ETag:   cid,
	})
}

func handleAbortMultipartUpload(w http.ResponseWriter, r *http.Request) error {
	w.WriteHeader(204)
	return nil
}

func handleDeleteObject(ctx *Context, w http.ResponseWriter, r *http.Request) error {
	log.Infow("handleDeleteObject", "key", r.URL.Path)
	if err := ctx.deleteObject(r.URL.Path); err != nil {
		return err
	}
	w.WriteHeader(204)
	return nil
}

func validateChunkedPayload(r *http.Request) error {
	if r.Header.Get("X-Amz-Content-Sha256") != "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" {
		return fmt.Errorf("request does not have chunked payload")
	}
	return nil
}
