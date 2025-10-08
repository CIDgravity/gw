package rbdeal

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/url"
	"path"
	"sort"
	"sync"
	"time"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	"github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/google/uuid"
	pool "github.com/libp2p/go-buffer-pool"
	"go.uber.org/multierr"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"golang.org/x/xerrors"
)

type S3OffloadInfo struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Token     string
	Bucket    string

	s3          *s3.S3
	s3Bucket    string
	s3BucketUrl *url.URL

	s3Uploads map[iface.GroupKey]struct{}
	s3Lk      sync.Mutex

	r *ribs

	metrics *ExternalStorageModuleMetrics
}

const EXTERNAL_S3 = "s3"

func (s *S3OffloadInfo) maybeInitExternal(r *ribs, metrics *ExternalStorageModuleMetrics) (bool, error) {
	cfg := configuration.GetConfig()

	if cfg.External.S3.Endpoint == "" {
		return false, nil
	}

	log.Infow("S3 offload enabled", "endpoint", cfg.External.S3.Endpoint)

	burl, err := url.Parse(cfg.External.S3.BucketUrl)
	if err != nil {
		return false, xerrors.Errorf("failed to parse S3_BUCKET_URL: %w", err)
	}

	s3Config := &aws.Config{
		Endpoint:    aws.String(cfg.External.S3.Endpoint),
		Credentials: credentials.NewStaticCredentials(cfg.External.S3.AccessKey, cfg.External.S3.SecretKey, cfg.External.S3.Token),
		Region:      aws.String(cfg.External.S3.Region),
	}

	asess, err := session.NewSession(s3Config)
	if err != nil {
		return false, xerrors.Errorf("failed to create S3 session: %w", err)
	}

	s.s3 = s3.New(asess)
	s.s3Bucket = cfg.External.S3.Bucket
	s.s3BucketUrl = burl
	s.s3Uploads = map[iface.GroupKey]struct{}{}
	s.metrics = metrics

	return true, nil
}

func (s *S3OffloadInfo) GetModuleName() string {
	return EXTERNAL_S3
}

func (s *S3OffloadInfo) EnsureExternalPush(gid iface.GroupKey, src CarSource) error {
	r := s.r

	module, _, err := r.db.GetExternalPath(gid)
	if err != nil {
		return xerrors.Errorf("XYZ: External: fail to get ext path: %w", err)
	}
	if module != nil {
		// already pushed
		return nil
	}

	if err := s.acquireExternalPushOpportunity(gid); err != nil {
		return err
	}

	s.s3Uploads[gid] = struct{}{}

	defer func() {
		s.s3Lk.Lock()
		delete(s.s3Uploads, gid)
		s.s3Lk.Unlock()
	}()
	s.s3Lk.Unlock()

	ctx := context.TODO()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	pr, pw := io.Pipe()

	sizeCh := make(chan int64, 1)
	setSize := func(sz int64) {
		sizeCh <- sz
		close(sizeCh)
	}

	syncWait := make(chan struct{})
	go func() {
		defer close(syncWait)

		bw := bufio.NewWriterSize(pw, 4<<20)

		err := src(ctx, gid, setSize, bw)
		if err != nil {
			perr := pw.CloseWithError(err)
			if perr != nil {
				log.Errorw("failed to close pipe", "error", perr)
			}
		}

		perr := pw.CloseWithError(bw.Flush())
		if perr != nil {
			log.Errorw("failed to close pipe", "error", perr)
		}
	}()

	size := <-sizeCh

	// generate random uuid
	fname := fmt.Sprintf("%d-%s.car", gid, uuid.New().String())

	upErr := s.uploadGroupData(gid, fname, size, pr)
	if upErr != nil {
		return xerrors.Errorf("failed to upload group %d: %w", gid, upErr)
	}

	perr := pr.Close()

	<-syncWait

	if perr != nil {
		return xerrors.Errorf("failed to close pipe (read): %w", perr)
	}

	// store file name in db
	err = r.db.AddExternalPath(gid, EXTERNAL_S3, fname)
	if err != nil {
		return xerrors.Errorf("XYZ: LocalWeb: Failed to store localpath: %w", err)
	}

	return nil
}

func (s *S3OffloadInfo) acquireExternalPushOpportunity(gid iface.GroupKey) error {
	cfg := configuration.GetConfig()
	maxStagingCount := cfg.Ribs.MaxStagingGroupCount

	// If no limit is set, return immediately
	if maxStagingCount <= 0 {
		return nil
	}

	for {
		// Check if this group is already uploading
		s.s3Lk.Lock()
		_, ok := s.s3Uploads[gid]
		if ok {
			s.s3Lk.Unlock()
			return xerrors.Errorf("group %d has an ongoing upload", gid)
		}

		// Get current staging count from database
		stagingCnt, err := s.r.db.GetStagingGroupCount()
		if err != nil {
			s.s3Lk.Unlock()
			return xerrors.Errorf("failed to get staging group count from db: %w", err)
		}

		// Check if we have space available
		currentUploads := len(s.s3Uploads)
		if stagingCnt+currentUploads < maxStagingCount {
			return nil
		}

		s.s3Lk.Unlock()

		// Log waiting status
		log.Infow("staging storage full, waiting for space",
			"group", gid,
			"stagingCount", stagingCnt,
			"currentUploads", currentUploads,
			"max", maxStagingCount)

		time.Sleep(time.Second)
	}
}

func (s *S3OffloadInfo) GetGroupExternalURL(gid iface.GroupKey, lpath string) (*string, error) {
	base := *s.s3BucketUrl
	base.Path = path.Join(base.Path, lpath)
	u := base.String()
	return &u, nil
}

func (s *S3OffloadInfo) CleanExternal(gid iface.GroupKey, lpath string) error {
	r := s.r

	objKey := lpath

	_, err := s.s3.DeleteObject(&s3.DeleteObjectInput{
		Bucket: &s.s3Bucket,
		Key:    &objKey,
	})

	if err != nil {
		return xerrors.Errorf("failed to delete object: %w", err)
	}

	if err := r.db.DropExternalPath(gid); err != nil {
		return xerrors.Errorf("XYZ: External: failed to remove external path from db for group %d: %w", gid, err)
	}

	return nil
}

func (s *S3OffloadInfo) ReadCar(ctx context.Context, group iface.GroupKey, path string, off int64, size int64) (io.ReadCloser, error) {
	s.metrics.readBytes.Add(float64(size))

	key := path

	req, _ := s.s3.GetObjectRequest(&s3.GetObjectInput{
		Bucket: &s.Bucket,
		Key:    &key,
	})

	req.HTTPRequest.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", off, off+size-1))

	err := req.Send()

	if err != nil {
		return nil, xerrors.Errorf("failed to send request: %w", err)
	}

	return req.HTTPResponse.Body, nil
}

func (s *S3OffloadInfo) ReadCarFile(ctx context.Context, group iface.GroupKey) (io.ReadSeekCloser, error) {
	return nil, xerrors.Errorf("RCF not implemented for S3")
}

const partSize = 128 << 20 // todo investigate streaming much larger parts
const minPartSize = 8 << 20
const adjustmentSize = 100 << 10 // 100 KiB

func CalculateChunkSize(fileSize int64) int {
	numParts := int64(math.Ceil(float64(fileSize) / float64(partSize)))
	chunkSize := int64(partSize)

	lastPartSize := fileSize - ((numParts - 1) * chunkSize)

	// Ensure the last part is at least 5 MiB and not larger than chunk size
	for lastPartSize < minPartSize || lastPartSize > chunkSize {
		// Decrease the chunk size if last part size is too small
		// Or increase it if last part size is too large
		if lastPartSize < minPartSize {
			chunkSize -= adjustmentSize
		} else if lastPartSize > chunkSize {
			chunkSize += adjustmentSize
		}

		numParts = int64(math.Ceil(float64(fileSize) / float64(chunkSize)))
		lastPartSize = fileSize - ((numParts - 1) * chunkSize)
		log.Debugw("try chunk size", "chunkSize", chunkSize, "lastPartSize", lastPartSize, "numParts", numParts, "fileSize", fileSize)
	}

	log.Debugw("using chunk size", "chunkSize", chunkSize, "lastPartSize", lastPartSize, "numParts", numParts, "fileSize", fileSize)

	return int(chunkSize)
}

func (s *S3OffloadInfo) uploadGroupData(gid iface.GroupKey, fname string, size int64, src io.Reader) (err error) {
	s.metrics.uploadsStarted.Inc()

	objKey := fname

	createResp, err := s.s3.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: &s.s3Bucket,
		Key:    &objKey,
	})
	if err != nil {
		return xerrors.Errorf("failed to create multipart upload: %w", err)
	}

	uploadId := *createResp.UploadId
	var partsLk sync.Mutex
	var completedParts []*s3.CompletedPart
	var errors []error

	partNumber := int64(1)

	maxParallel := 4 // todo: make this configurable

	throttle := make(chan struct{}, maxParallel)

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	partSize := CalculateChunkSize(size)

	curPart := pool.Get(partSize)
	n, err := io.ReadFull(src, curPart)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return xerrors.Errorf("failed to read part: %w", err)
	}
	curPart = curPart[:n]

	for {
		if len(curPart) == 0 {
			break
		}

		// read next part now, so that in case it's smaller than 5MB, we can merge it with the current one
		nextPart := pool.Get(partSize)
		n, err := io.ReadFull(src, nextPart)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return xerrors.Errorf("failed to read part: %w", err)
		}
		nextPart = nextPart[:n]

		if len(nextPart) < minPartSize && len(nextPart) > 0 {
			// last part is too small, merge it with the current one
			log.Debugw("last part merge", "cur", len(curPart), "next", len(nextPart), "total", len(curPart)+len(nextPart))

			temp := pool.Get(len(curPart) + len(nextPart))

			cn := copy(temp, curPart)

			if cn != len(curPart) {
				return xerrors.Errorf("failed to copy part: %d != %d", cn, len(curPart))
			}

			pool.Put(curPart)

			copy(temp[cn:], nextPart)
			pool.Put(nextPart)

			curPart = temp
			nextPart = []byte{}
		}

		throttle <- struct{}{}

		go func(part []byte, partNumber int64) {
			defer func() {
				<-throttle

				pool.Put(part)
			}()

			maxRetries := 6
			for i := 0; i < maxRetries; i++ {
				uploadResp, err := s.s3.UploadPartWithContext(ctx, &s3.UploadPartInput{
					Body:       bytes.NewReader(part),
					Bucket:     &s.s3Bucket,
					Key:        &objKey,
					PartNumber: aws.Int64(partNumber),
					UploadId:   &uploadId,
				})

				s.metrics.uploadedBytes.Add(float64(len(part)))

				partsLk.Lock()
				if err != nil {
					// If we've reached the maximum retries, append the error
					if i == maxRetries-1 {
						errors = append(errors, xerrors.Errorf("failed to upload part %d: %w", partNumber, err))
					}
					log.Errorw("failed to upload part", "part", partNumber, "group", gid, "error", err)
					time.Sleep(time.Second << uint(i))
				} else {
					completedParts = append(completedParts, &s3.CompletedPart{
						ETag:       uploadResp.ETag,
						PartNumber: aws.Int64(partNumber),
					})
					partsLk.Unlock()
					log.Infow("uploaded part", "part", partNumber, "group", gid, "size", len(part))
					break
				}
				partsLk.Unlock()
			}
		}(curPart, partNumber)

		curPart = nextPart

		partNumber++
	}

	// wait for all upload goroutines to finish
	for i := 0; i < maxParallel; i++ {
		throttle <- struct{}{}
	}

	if len(errors) > 0 {
		// remove failed upload
		_, err2 := s.s3.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
			Bucket:   &s.s3Bucket,
			Key:      &objKey,
			UploadId: &uploadId,
		})
		if err2 != nil {
			log.Errorw("failed to abort multipart upload", "group", gid, "err", err2, "completeErr", err)
		}

		return xerrors.Errorf("failed to upload parts: %w", multierr.Combine(errors...))
	}

	// sort completed parts by part number
	sort.Slice(completedParts, func(i, j int) bool {
		return *completedParts[i].PartNumber < *completedParts[j].PartNumber
	})

	// Complete the multipart upload
	_, err = s.s3.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:   &s.s3Bucket,
		Key:      &objKey,
		UploadId: &uploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		// remove failed upload
		_, err2 := s.s3.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
			Bucket:   &s.s3Bucket,
			Key:      &objKey,
			UploadId: &uploadId,
		})
		if err2 != nil {
			log.Errorw("failed to abort multipart upload", "group", gid, "err", err2, "completeErr", err)
		}

		log.Errorw("failed to complete multipart upload", "group", gid, "err", err, "parts", completedParts)

		return xerrors.Errorf("failed to complete multipart upload: %w", err)
	}

	s.metrics.uploadsDone.Inc()
	return nil
}

func (s *S3OffloadInfo) GetMetrics() *ExternalStorageModuleMetrics {
	return s.metrics
}
