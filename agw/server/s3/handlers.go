package s3

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/aurorainfra/gw/agw/iface"
)

func (srv *S3Server) handleGetLocation(w http.ResponseWriter, r *http.Request) error {
	return locationTemplate.Execute(w, locationResponseParams{Region: srv.region.Name()})
}

func (srv *S3Server) handleListObjects(w http.ResponseWriter, r *http.Request) error {
	// TODO implement actual object listing!
	// endpoint used only to check if a bucket is currently empty
	// validated by s3 when adding and removing a warm backend
	bucket := strings.TrimLeft(r.URL.Path, "/")
	return listObjectsTemplate.Execute(w, listObjectsResponseParams{Name: bucket})
}

func (srv *S3Server) handleGetObject(w http.ResponseWriter, r *http.Request) error {
	bucketName, objectName, err := requestToObject(r)
	if err != nil {
		log.Infow("error parsing url", "URL", r.URL, "error", err)
		w.WriteHeader(400)
		return nil
	}

	bucket, err := srv.region.GetBucket(r.Context(), bucketName)
	if err != nil {
		if errors.Is(err, iface.ErrNotFound) {
			log.Infow("bucket not found", "URL", r.URL)
			w.WriteHeader(404)
			return nil
		}

		return fmt.Errorf("error getting bucket: %w", err)
	}

	rd, err := bucket.Get(r.Context(), objectName)
	if err != nil {
		if errors.Is(err, iface.ErrNotFound) {
			log.Infow("object not found", "URL", r.URL)
			w.WriteHeader(404)
			return nil
		}

		return fmt.Errorf("error getting object reader: %w", err)
	}
	defer rd.Close() //nolint

	stat := rd.Stat()
	w.Header().Set("ETag", stat.ETag)
	http.ServeContent(w, r, objectName, stat.Timestamp, rd)
	return nil
}

func (srv *S3Server) handlePutObject(w http.ResponseWriter, r *http.Request) error {
	//TODO verify data integrity
	bucketName, objectName, err := requestToObject(r)
	if err != nil {
		log.Infow("error parsing url", "URL", r.URL, "error", err)
		w.WriteHeader(400)
		return nil
	}

	bucket, err := srv.region.GetBucket(r.Context(), bucketName)
	if err != nil {
		if errors.Is(err, iface.ErrNotFound) {
			log.Infow("bucket not found", "URL", r.URL)
			w.WriteHeader(404)
			return nil
		}

		return fmt.Errorf("error getting bucket: %w", err)
	}

	stat, err := bucket.Put(r.Context(), objectName, NewChunkReader(r.Body))
	if err != nil {
		return fmt.Errorf("error putting object: %w", err)
	}

	w.Header().Set("ETag", stat.ETag)
	return nil
}

func (srv *S3Server) handleDeleteObject(w http.ResponseWriter, r *http.Request) error {
	bucketName, objectName, err := requestToObject(r)
	if err != nil {
		log.Infow("error parsing url", "URL", r.URL, "error", err)
		w.WriteHeader(400)
		return nil
	}

	bucket, err := srv.region.GetBucket(r.Context(), bucketName)
	if err != nil {
		if errors.Is(err, iface.ErrNotFound) {
			log.Infow("bucket not found", "URL", r.URL)
			w.WriteHeader(404)
			return nil
		}

		return fmt.Errorf("error getting bucket: %w", err)
	}

	err = bucket.Delete(r.Context(), objectName)
	if err != nil {
		if errors.Is(err, iface.ErrNotFound) {
			log.Infow("object not found", "URL", r.URL)
			w.WriteHeader(404)
			return nil
		}

		return fmt.Errorf("error getting bucket: %w", err)
	}

	w.WriteHeader(204)
	return nil
}

func (srv *S3Server) handleCreateMultipartUpload(w http.ResponseWriter, r *http.Request) error {
	bucketName, objectName, err := requestToObject(r)
	if err != nil {
		log.Infow("error parsing url", "URL", r.URL, "error", err)
		w.WriteHeader(400)
		return nil
	}

	bucket, err := srv.region.GetBucket(r.Context(), bucketName)
	if err != nil {
		if errors.Is(err, iface.ErrNotFound) {
			log.Infow("bucket not found", "URL", r.URL)
			w.WriteHeader(404)
			return nil
		}

		return fmt.Errorf("error getting bucket: %w", err)
	}

	uploadId, err := bucket.BeginMultipartPut(r.Context(), objectName)
	if err != nil {
		return fmt.Errorf("error getting object writer: %w", err)
	}

	return createMultipartUploadTemplate.Execute(w, createMultipartUploadResponseParams{UploadId: uploadId, Key: objectName, Bucket: bucketName})
}

func (srv *S3Server) handleUploadPart(w http.ResponseWriter, r *http.Request) error {
	// TODO verify data integrity
	params, _ := url.ParseQuery(r.URL.RawQuery) // already parsed
	uploadId := params.Get("uploadId")
	if uploadId == "" {
		log.Infow("missing uploadId", "URL", r.URL)
		w.WriteHeader(400)
		return nil
	}

	partNumber, err := strconv.ParseInt(params.Get("partNumber"), 10, 64)
	if err != nil {
		log.Infow("bad partNumber", "URL", r.URL)
		w.WriteHeader(400)
		return nil
	}

	bucketName, objectName, err := requestToObject(r)
	if err != nil {
		log.Infow("error parsing url", "URL", r.URL, "error", err)
		w.WriteHeader(400)
		return nil
	}

	bucket, err := srv.region.GetBucket(r.Context(), bucketName)
	if err != nil {
		if errors.Is(err, iface.ErrNotFound) {
			log.Infow("bucket not found", "URL", r.URL)
			w.WriteHeader(404)
			return nil
		}

		return fmt.Errorf("error getting bucket: %w", err)
	}

	stat, err := bucket.ContinueMultipartPut(r.Context(), objectName, uploadId, partNumber, NewChunkReader(r.Body))
	if err != nil {
		if errors.Is(err, iface.ErrNotFound) {
			log.Infow("unknown multipart id", "URL", r.URL)
			w.WriteHeader(404)
			return nil
		}

		return fmt.Errorf("error continuing multipart upload: %w", err)
	}

	w.Header().Set("ETag", stat.ETag)
	return nil
}

func (srv *S3Server) handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request) error {
	params, _ := url.ParseQuery(r.URL.RawQuery) // already parsed
	uploadId := params.Get("uploadId")
	if uploadId == "" {
		log.Infow("missing uploadId", "URL", r.URL)
		w.WriteHeader(400)
		return nil
	}

	var completeReq iface.CompleteMultipartUpload
	if err := xml.NewDecoder(r.Body).Decode(&completeReq); err != nil {
		log.Infow("error parsing request body", "URL", r.URL, "error", err)
		w.WriteHeader(400)
		return nil
	}

	bucketName, objectName, err := requestToObject(r)
	if err != nil {
		log.Infow("error parsing url", "URL", r.URL, "error", err)
		w.WriteHeader(400)
		return nil
	}

	bucket, err := srv.region.GetBucket(r.Context(), bucketName)
	if err != nil {
		if errors.Is(err, iface.ErrNotFound) {
			log.Infow("bucket not found", "URL", r.URL)
			w.WriteHeader(404)
			return nil
		}

		return fmt.Errorf("error getting bucket: %w", err)
	}

	stat, err := bucket.CompleteMultipartPut(r.Context(), objectName, uploadId, &completeReq)
	if err != nil {
		if errors.Is(err, iface.ErrNotFound) {
			log.Infow("object not found", "URL", r.URL)
			w.WriteHeader(404)
			return nil
		}

		return fmt.Errorf("error completing multipart upload: %w", err)
	}

	return completeMultipartUploadTemplate.Execute(w, completeMultipartUploadResponseParams{
		Bucket: bucketName,
		Key:    objectName,
		ETag:   stat.ETag,
	})
}

func (srv *S3Server) handleAbortMultipartUpload(w http.ResponseWriter, r *http.Request) error {
	params, _ := url.ParseQuery(r.URL.RawQuery) // already parsed
	uploadId := params.Get("uploadId")
	if uploadId == "" {
		log.Infow("missing uploadId", "URL", r.URL)
		w.WriteHeader(400)
		return nil
	}

	bucketName, objectName, err := requestToObject(r)
	if err != nil {
		log.Infow("error parsing url", "URL", r.URL, "error", err)
		w.WriteHeader(400)
		return nil
	}

	bucket, err := srv.region.GetBucket(r.Context(), bucketName)
	if err != nil {
		if errors.Is(err, iface.ErrNotFound) {
			log.Infow("bucket not found", "URL", r.URL)
			w.WriteHeader(404)
			return nil
		}

		return fmt.Errorf("error getting bucket: %w", err)
	}

	err = bucket.AbortMultipartPut(r.Context(), objectName, uploadId)
	if err != nil {
		if errors.Is(err, iface.ErrNotFound) {
			log.Infow("object not found", "URL", r.URL)
			w.WriteHeader(404)
			return nil
		}

		return fmt.Errorf("error aborting multipart upload: %w", err)
	}

	w.WriteHeader(204)
	return nil
}

func requestToObject(r *http.Request) (string, string, error) {
	// NOTE
	// S3 raw api puts the bucket as the first part of the host name
	// minio however, apparently puts it as the first segment of the URL path
	// we may need to adjust this if we move away from minio
	parts := strings.Split(strings.TrimLeft(r.URL.Path, "/"), "/")
	if len(parts) < 2 {
		return "", "", fmt.Errorf("malformed object path: %s", r.URL.Path)
	}

	bucket := parts[0]
	object := strings.Join(parts[1:], "/")
	return bucket, object, nil
}
