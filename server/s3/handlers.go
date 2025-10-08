package s3

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	iface2 "github.com/CIDgravity/filecoin-gateway/iface"
)

func (srv *S3Server) handleGetLocation(w http.ResponseWriter, r *http.Request) error {
	_, err := srv.auth.validateSignatureV4(r)
	if err != nil {
		srv.respondUnauthenticated(err, w)
		return nil
	}

	return locationTemplate.Execute(w, locationResponseParams{Region: srv.region.Name()})
}

func (srv *S3Server) handleListObjects(w http.ResponseWriter, r *http.Request) error {
	_, err := srv.auth.validateSignatureV4(r)
	if err != nil {
		srv.respondUnauthenticated(err, w)
		return nil
	}

	bucketName, err := requestToBucket(r)
	if err != nil {
		log.Infow("error parsing url", "URL", r.URL, "error", err)
		w.WriteHeader(400)
		return nil
	}

	bucket, err := srv.region.GetBucket(r.Context(), bucketName)
	if err != nil {
		if errors.Is(err, iface2.ErrNotFound) {
			log.Infow("bucket not found", "URL", r.URL)
			w.WriteHeader(404)
			return nil
		}

		return fmt.Errorf("error getting bucket: %w", err)
	}

	query, err := parseListObjectsQuery(r)
	if err != nil {
		log.Infow("error parsing query", "URL", r.URL, "error", err)
		w.WriteHeader(400)
		return nil
	}

	list, err := bucket.List(r.Context(), query)
	if err != nil {
		return fmt.Errorf("error listing objects: %w", err)
	}

	objs := make([]ListObjectsEntry, len(list.Contents))
	for i, obj := range list.Contents {
		objs[i] = ListObjectsEntry{
			Etag:         obj.ETag,
			Key:          obj.Key.String(),
			LastModified: obj.Timestamp.Format("2006-01-02T15:04:05.000Z"),
			Size:         obj.Size,
		}
	}

	commonPrefixes := make([]ListObjectsCommonPrefix, len(list.CommonPrefixes))
	for i, prefix := range list.CommonPrefixes {
		commonPrefixes[i] = ListObjectsCommonPrefix{
			Prefix: prefix,
		}
	}

	response := ListObjectsResponse{
		IsTruncated:           list.IsTruncated,
		Contents:              objs,
		Name:                  bucketName.String(),
		Prefix:                query.Prefix,
		Delimiter:             query.Delimiter,
		MaxKeys:               query.MaxKeys,
		CommonPrefixes:        commonPrefixes,
		KeyCount:              int32(len(list.Contents) + len(list.CommonPrefixes)),
		ContinuationToken:     query.ContinuationToken,
		NextContinuationToken: list.NextContinuationToken,
		StartAfter:            query.StartAfter,
	}

	err = xml.NewEncoder(w).Encode(response)
	if err != nil {
		return fmt.Errorf("error encoding response: %w", err)
	}

	return nil
}

func parseListObjectsQuery(r *http.Request) (*iface2.ListObjectsQuery, error) {
	queryParams := r.URL.Query()

	query := &iface2.ListObjectsQuery{
		ContinuationToken: queryParams.Get("continuation-token"),
		Delimiter:         queryParams.Get("delimiter"),
		Prefix:            queryParams.Get("prefix"),
		StartAfter:        queryParams.Get("start-after"),
		MaxKeys:           1000,
	}

	if queryParams.Has("max-keys") {
		maxKeys, err := strconv.Atoi(queryParams.Get("max-keys"))
		if err != nil {
			return nil, fmt.Errorf("error parsing max-keys: %w", err)
		}
		query.MaxKeys = int32(maxKeys)
	}

	if query.MaxKeys < 1 || query.MaxKeys > 1000 {
		return nil, fmt.Errorf("max-keys out of allowed range: %d", query.MaxKeys)
	}
	return query, nil
}

func (srv *S3Server) handleGetObject(w http.ResponseWriter, r *http.Request) error {
	_, err := srv.auth.validateSignatureV4(r)
	if err != nil {
		srv.respondUnauthenticated(err, w)
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
		if errors.Is(err, iface2.ErrNotFound) {
			log.Infow("bucket not found", "URL", r.URL)
			w.WriteHeader(404)
			return nil
		}

		return fmt.Errorf("error getting bucket: %w", err)
	}

	rd, err := bucket.Get(r.Context(), objectName)
	if err != nil {
		if errors.Is(err, iface2.ErrNotFound) {
			log.Infow("object not found", "URL", r.URL)
			w.WriteHeader(404)
			return nil
		}

		return fmt.Errorf("error getting object reader: %w", err)
	}
	defer rd.Close() //nolint

	stat := rd.Stat()
	w.Header().Set("ETag", stat.ETag)
	http.ServeContent(w, r, objectName.String(), stat.Timestamp, rd)
	return nil
}

func (srv *S3Server) handlePutObject(w http.ResponseWriter, r *http.Request) error {
	//TODO verify data integrity
	_, err := srv.auth.validateSignatureV4(r)
	if err != nil {
		srv.respondUnauthenticated(err, w)
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
		if errors.Is(err, iface2.ErrNotFound) {
			log.Infow("bucket not found", "URL", r.URL)
			w.WriteHeader(404)
			return nil
		}

		return fmt.Errorf("error getting bucket: %w", err)
	}

	reader, err := getBodyReader(r)
	if err != nil {
		return fmt.Errorf("error getting body reader: %w", err)
	}
	stat, err := bucket.Put(r.Context(), objectName, reader)
	if err != nil {
		return fmt.Errorf("error putting object: %w", err)
	}

	w.Header().Set("ETag", stat.ETag)
	return nil
}

func (srv *S3Server) handleDeleteObject(w http.ResponseWriter, r *http.Request) error {
	_, err := srv.auth.validateSignatureV4(r)
	if err != nil {
		srv.respondUnauthenticated(err, w)
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
		if errors.Is(err, iface2.ErrNotFound) {
			log.Infow("bucket not found", "URL", r.URL)
			w.WriteHeader(404)
			return nil
		}

		return fmt.Errorf("error getting bucket: %w", err)
	}

	err = bucket.Delete(r.Context(), objectName)
	if err != nil {
		if errors.Is(err, iface2.ErrNotFound) {
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
	_, err := srv.auth.validateSignatureV4(r)
	if err != nil {
		srv.respondUnauthenticated(err, w)
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
		if errors.Is(err, iface2.ErrNotFound) {
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

	return createMultipartUploadTemplate.Execute(w, createMultipartUploadResponseParams{
		UploadId: uploadId,
		Key:      objectName.String(),
		Bucket:   bucketName.String(),
	})
}

func (srv *S3Server) handleUploadPart(w http.ResponseWriter, r *http.Request) error {
	// TODO verify data integrity
	_, err := srv.auth.validateSignatureV4(r)
	if err != nil {
		srv.respondUnauthenticated(err, w)
		return nil
	}

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
		if errors.Is(err, iface2.ErrNotFound) {
			log.Infow("bucket not found", "URL", r.URL)
			w.WriteHeader(404)
			return nil
		}

		return fmt.Errorf("error getting bucket: %w", err)
	}

	reader, err := getBodyReader(r)
	if err != nil {
		return fmt.Errorf("error getting body reader: %w", err)
	}
	stat, err := bucket.ContinueMultipartPut(r.Context(), objectName, uploadId, partNumber, reader)
	if err != nil {
		if errors.Is(err, iface2.ErrNotFound) {
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
	_, err := srv.auth.validateSignatureV4(r)
	if err != nil {
		srv.respondUnauthenticated(err, w)
		return nil
	}

	params, _ := url.ParseQuery(r.URL.RawQuery) // already parsed
	uploadId := params.Get("uploadId")
	if uploadId == "" {
		log.Infow("missing uploadId", "URL", r.URL)
		w.WriteHeader(400)
		return nil
	}

	var completeReq iface2.CompleteMultipartUpload
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
		if errors.Is(err, iface2.ErrNotFound) {
			log.Infow("bucket not found", "URL", r.URL)
			w.WriteHeader(404)
			return nil
		}

		return fmt.Errorf("error getting bucket: %w", err)
	}

	stat, err := bucket.CompleteMultipartPut(r.Context(), objectName, uploadId, &completeReq)
	if err != nil {
		if errors.Is(err, iface2.ErrNotFound) {
			log.Infow("object not found", "URL", r.URL)
			w.WriteHeader(404)
			return nil
		}

		return fmt.Errorf("error completing multipart upload: %w", err)
	}

	return completeMultipartUploadTemplate.Execute(w, completeMultipartUploadResponseParams{
		Bucket: bucketName.String(),
		Key:    objectName.String(),
		ETag:   stat.ETag,
	})
}

func (srv *S3Server) handleAbortMultipartUpload(w http.ResponseWriter, r *http.Request) error {
	_, err := srv.auth.validateSignatureV4(r)
	if err != nil {
		srv.respondUnauthenticated(err, w)
		return nil
	}

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
		if errors.Is(err, iface2.ErrNotFound) {
			log.Infow("bucket not found", "URL", r.URL)
			w.WriteHeader(404)
			return nil
		}

		return fmt.Errorf("error getting bucket: %w", err)
	}

	err = bucket.AbortMultipartPut(r.Context(), objectName, uploadId)
	if err != nil {
		if errors.Is(err, iface2.ErrNotFound) {
			log.Infow("object not found", "URL", r.URL)
			w.WriteHeader(404)
			return nil
		}

		return fmt.Errorf("error aborting multipart upload: %w", err)
	}

	w.WriteHeader(204)
	return nil
}

func (srv *S3Server) handleHeadObject(w http.ResponseWriter, r *http.Request) error {
	_, err := srv.auth.validateSignatureV4(r)
	if err != nil {
		srv.respondUnauthenticated(err, w)
		return nil
	}

	bucketName, objectName, err := requestToObject(r)
	if err != nil {
		log.Infow("error parsing url", "URL", r.URL, "error", err)
		w.WriteHeader(400)
	}
	includeAurMeta := r.URL.Query().Get("fil-include-meta") == "1"
	bucket, err := srv.region.GetBucket(r.Context(), bucketName)
	if err != nil {
		if errors.Is(err, iface2.ErrNotFound) {
			log.Infow("bucket not found", "URL", r.URL)
			w.WriteHeader(404)
			return nil
		}

		return fmt.Errorf("error getting bucket: %w", err)
	}

	stat, err := bucket.Stat(r.Context(), objectName, includeAurMeta)
	if err != nil {
		if errors.Is(err, iface2.ErrNotFound) {
			log.Infow("object not found", "URL", r.URL)
			w.WriteHeader(404)
			return nil
		}
		return fmt.Errorf("error getting object stat: %w", err)
	}
	w.Header().Set("ETag", stat.ETag)
	w.Header().Set("Last-Modified", stat.Timestamp.Format(http.TimeFormat))
	w.Header().Set("Content-Length", strconv.Itoa(int(stat.Size)))

	if stat.OffloadStatus != "" {
		w.Header().Set("X-Fil-Offload-Status", string(stat.OffloadStatus))
	}
	return nil
}

func (srv *S3Server) respondUnauthenticated(err error, w http.ResponseWriter) {
	log.Infow("Authentication failed", "error", err)
	w.WriteHeader(401)
}

func requestToObject(r *http.Request) (iface2.BucketName, iface2.S3Key, error) {
	// NOTE
	// S3 raw api puts the bucket as the first part of the host name
	// minio however, apparently puts it as the first segment of the URL path
	// we may need to adjust this if we move away from minio
	parts := strings.Split(strings.TrimLeft(r.URL.Path, "/"), "/")
	if len(parts) < 2 {
		return "", "", fmt.Errorf("malformed object path: %s", r.URL.Path)
	}

	bucket := iface2.BucketName(parts[0])
	object := iface2.S3Key(strings.Join(parts[1:], "/"))
	return bucket, object, nil
}

func requestToBucket(r *http.Request) (iface2.BucketName, error) {
	parts := strings.Split(strings.TrimLeft(r.URL.Path, "/"), "/")

	var bucket string
	if len(parts) == 1 {
		bucket = parts[0]
	}
	if len(parts) == 2 && parts[1] == "" {
		bucket = parts[0]
	}
	if bucket == "" {
		return "", fmt.Errorf("malformed bucket path: %s", r.URL.Path)
	}
	return iface2.BucketName(bucket), nil
}

func getBodyReader(r *http.Request) (io.Reader, error) {
	if r.Body == nil {
		return nil, fmt.Errorf("request body is nil")
	}
	contentSha256 := r.Header.Get("x-amz-content-sha256")
	if contentSha256 == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" {
		log.Debugf("streaming request body detected")
		return NewChunkReader(r.Body), nil
	}
	if contentSha256 == "UNSIGNED-PAYLOAD" {
		log.Debugf("unsigned request body detected")
		return r.Body, nil
	}
	if len(contentSha256) != 64 {
		return nil, fmt.Errorf("invalid content sha256: %s", contentSha256)
	}
	return r.Body, nil
}
