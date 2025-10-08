package s3_tests_native

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/CIDgravity/filecoin-gateway/test"
	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/test-go/testify/require"
)

var harness *test.FgwHarness

var creds = aws.Credentials{
	AccessKeyID:     "test-access-key",
	SecretAccessKey: "test-secret-key",
}

const emptyStringSha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func TestMain(m *testing.M) {
	harness = test.NewFgwHarness()
	defer harness.Stop()
	os.Exit(m.Run())
}

func TestShouldAcceptSigV4(t *testing.T) {
	endpoint := harness.GetS3Endpoint()
	req, err := http.NewRequest("GET",
		fmt.Sprintf("%s/example?location=", endpoint),
		nil)

	require.NoError(t, err)
	now := time.Now().UTC()
	req.Header.Set("x-amz-date", now.Format("20060102T150405Z"))
	req.Header.Set("x-amz-content-sha256", emptyStringSha256)

	signer := v4.NewSigner()
	err = signer.SignHTTP(context.Background(), creds, req, emptyStringSha256, "s3", "us-east-1", now)
	require.NoError(t, err)

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
}

func TestShouldAcceptSigV4UnsignedBody(t *testing.T) {
	endpoint := harness.GetS3Endpoint()
	req, err := http.NewRequest("GET",
		fmt.Sprintf("%s/example?location=", endpoint),
		nil)

	require.NoError(t, err)
	now := time.Now().UTC()
	req.Header.Set("x-amz-date", now.Format("20060102T150405Z"))
	req.Header.Set("x-amz-content-sha256", "UNSIGNED-PAYLOAD")

	signer := v4.NewSigner()
	err = signer.SignHTTP(context.Background(), creds, req, "UNSIGNED-PAYLOAD", "s3", "us-east-1", now)
	require.NoError(t, err)

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
}

func TestShouldNotAcceptInvalidSigV4(t *testing.T) {
	endpoint := harness.GetS3Endpoint()
	req, err := http.NewRequest("GET",
		fmt.Sprintf("%s/example?location=", endpoint),
		nil)

	require.NoError(t, err)
	now := time.Now().UTC()
	req.Header.Set("x-amz-date", now.Format("20060102T150405Z"))
	req.Header.Set("x-amz-content-sha256", emptyStringSha256)

	signer := v4.NewSigner()
	invalidCreds := aws.Credentials{
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "invalid-secret-key",
	}
	err = signer.SignHTTP(context.Background(), invalidCreds, req, emptyStringSha256, "s3", "us-east-1", now)
	require.NoError(t, err)

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, 401, res.StatusCode)
}

func TestShouldNotAcceptInvalidAccessKeyId(t *testing.T) {
	endpoint := harness.GetS3Endpoint()
	req, err := http.NewRequest("GET",
		fmt.Sprintf("%s/example?location=", endpoint),
		nil)

	require.NoError(t, err)
	now := time.Now().UTC()
	req.Header.Set("x-amz-date", now.Format("20060102T150405Z"))
	req.Header.Set("x-amz-content-sha256", emptyStringSha256)

	signer := v4.NewSigner()
	invalidCreds := aws.Credentials{
		AccessKeyID:     "invalid-access-key-id",
		SecretAccessKey: "test-secret-key",
	}
	err = signer.SignHTTP(context.Background(), invalidCreds, req, emptyStringSha256, "s3", "us-east-1", now)
	require.NoError(t, err)

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, 401, res.StatusCode)
}

func TestShouldNotAcceptModifiedSignedHeader(t *testing.T) {
	endpoint := harness.GetS3Endpoint()
	req, err := http.NewRequest("GET",
		fmt.Sprintf("%s/example?location=", endpoint),
		nil)

	require.NoError(t, err)
	now := time.Now().UTC()
	req.Header.Set("x-amz-date", now.Format("20060102T150405Z"))
	req.Header.Set("x-amz-content-sha256", emptyStringSha256)

	signer := v4.NewSigner()
	err = signer.SignHTTP(context.Background(), creds, req, emptyStringSha256, "s3", "us-east-1", now)
	require.NoError(t, err)

	req.Header.Set("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, 401, res.StatusCode)
}

func TestShouldNotAcceptClockSkew(t *testing.T) {
	endpoint := harness.GetS3Endpoint()
	req, err := http.NewRequest("GET",
		fmt.Sprintf("%s/example?location=", endpoint),
		nil)

	require.NoError(t, err)
	now := time.Now().UTC().Add(time.Hour)
	req.Header.Set("x-amz-date", now.Format("20060102T150405Z"))
	req.Header.Set("x-amz-content-sha256", emptyStringSha256)

	signer := v4.NewSigner()
	err = signer.SignHTTP(context.Background(), creds, req, emptyStringSha256, "s3", "us-east-1", now)
	require.NoError(t, err)

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, 401, res.StatusCode)
}

func TestShouldAcceptModifiedUnsignedHeader(t *testing.T) {
	endpoint := harness.GetS3Endpoint()
	req, err := http.NewRequest("GET",
		fmt.Sprintf("%s/example?location=", endpoint),
		nil)

	require.NoError(t, err)
	now := time.Now().UTC()
	req.Header.Set("x-amz-date", now.Format("20060102T150405Z"))
	req.Header.Set("x-amz-content-sha256", emptyStringSha256)

	signer := v4.NewSigner()
	err = signer.SignHTTP(context.Background(), creds, req, emptyStringSha256, "s3", "us-east-1", now)
	require.NoError(t, err)

	req.Header.Set("x-some-other-header", "example")
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
}

func TestShouldNotAcceptMissingSigV4(t *testing.T) {
	endpoint := harness.GetS3Endpoint()
	req, err := http.NewRequest("GET",
		fmt.Sprintf("%s/example?location=", endpoint),
		nil)

	require.NoError(t, err)
	now := time.Now().UTC()
	req.Header.Set("x-amz-date", now.Format("20060102T150405Z"))
	req.Header.Set("x-amz-content-sha256", emptyStringSha256)

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, 401, res.StatusCode)
}

func TestShouldGetBucketLocation(t *testing.T) {
	s3c := createS3Client(t, creds, harness.GetS3Endpoint())
	response, err := s3c.GetBucketLocation(context.Background(), &s3.GetBucketLocationInput{
		Bucket: aws.String("example"),
	})
	require.NoError(t, err)
	require.Equal(t, types.BucketLocationConstraint("EU"), response.LocationConstraint)
}

func TestShouldNotGetBucketLocationInvalidSig(t *testing.T) {
	invalidCreds := aws.Credentials{
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "invalid-secret-key",
	}

	s3c := createS3Client(t, invalidCreds, harness.GetS3Endpoint())
	_, err := s3c.GetBucketLocation(context.Background(), &s3.GetBucketLocationInput{
		Bucket: aws.String("example"),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Unauthorized")
}

func TestShouldPutObject(t *testing.T) {
	s3c := createS3Client(t, creds, harness.GetS3Endpoint())

	key := uuid.New().String()
	content := test.NewRandomFile(t, int64(1024*1024))

	res, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("example"),
		Key:    aws.String(key),
		Body:   content,
	})
	require.NoError(t, err)
	require.NotEmpty(t, res.ETag)
}

func TestShouldNotPutObjectInvalidSig(t *testing.T) {
	invalidCreds := aws.Credentials{
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "invalid-secret-key",
	}
	s3c := createS3Client(t, invalidCreds, harness.GetS3Endpoint())

	key := uuid.New().String()
	content := test.NewRandomFile(t, int64(1024))
	_, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("example"),
		Key:    aws.String(key),
		Body:   content,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Unauthorized")
}

func TestShouldGetObject(t *testing.T) {
	s3c := createS3Client(t, creds, harness.GetS3Endpoint())

	key := uuid.New().String()
	content := test.NewRandomFile(t, int64(1024))
	_, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("example"),
		Key:    aws.String(key),
		Body:   content,
	})
	require.NoError(t, err)

	res, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String("example"),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	require.Equal(t, int64(1024), *res.ContentLength)

	buf, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, content.Bytes(t), buf)
}

func TestShouldNotGetObjectInvalidSig(t *testing.T) {
	s3c := createS3Client(t, creds, harness.GetS3Endpoint())

	key := uuid.New().String()
	content := test.NewRandomFile(t, int64(1024))
	_, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("example"),
		Key:    aws.String(key),
		Body:   content,
	})
	require.NoError(t, err)

	invalidCreds := aws.Credentials{
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "invalid-secret-key",
	}
	s3c = createS3Client(t, invalidCreds, harness.GetS3Endpoint())
	_, err = s3c.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String("example"),
		Key:    aws.String(key),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Unauthorized")
}

func TestShouldDeleteObject(t *testing.T) {
	s3c := createS3Client(t, creds, harness.GetS3Endpoint())

	key := uuid.New().String()
	content := test.NewRandomFile(t, int64(1024))
	_, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("example"),
		Key:    aws.String(key),
		Body:   content,
	})
	require.NoError(t, err)

	_, err = s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String("example"),
		Key:    aws.String(key),
	})
	require.NoError(t, err)

	_, err = s3c.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String("example"),
		Key:    aws.String(key),
	})
	require.Error(t, err)
}

func TestShouldNotDeleteObjectInvalidSig(t *testing.T) {
	s3c := createS3Client(t, creds, harness.GetS3Endpoint())

	key := uuid.New().String()
	content := test.NewRandomFile(t, int64(1024))
	_, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("example"),
		Key:    aws.String(key),
		Body:   content,
	})
	require.NoError(t, err)

	invalidCreds := aws.Credentials{
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "invalid-secret-key",
	}
	s3c = createS3Client(t, invalidCreds, harness.GetS3Endpoint())
	_, err = s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String("example"),
		Key:    aws.String(key),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Unauthorized")
}

func TestShouldCreateMultipartUpload(t *testing.T) {
	s3c := createS3Client(t, creds, harness.GetS3Endpoint())
	key := uuid.New().String()
	res, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String("example"),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	require.NotEmpty(t, res.UploadId)
}

func TestShouldNotCreateMultipartUploadInvalidSig(t *testing.T) {
	invalidCreds := aws.Credentials{
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "invalid-secret-key",
	}
	s3c := createS3Client(t, invalidCreds, harness.GetS3Endpoint())
	key := uuid.New().String()
	_, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String("example"),
		Key:    aws.String(key),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Unauthorized")
}

func TestShouldUploadPart(t *testing.T) {
	s3c := createS3Client(t, creds, harness.GetS3Endpoint())
	key := uuid.New().String()
	content := test.NewRandomFile(t, int64(1024))
	createRes, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String("example"),
		Key:    aws.String(key),
	})
	require.NoError(t, err)

	res, err := s3c.UploadPart(context.Background(), &s3.UploadPartInput{
		Bucket:     aws.String("example"),
		Key:        aws.String(key),
		UploadId:   aws.String(*createRes.UploadId),
		Body:       content,
		PartNumber: aws.Int32(1),
	})
	require.NoError(t, err)
	require.NotEmpty(t, res.ETag)
}

func TestShouldNotUploadPartInvalidSig(t *testing.T) {
	s3c := createS3Client(t, creds, harness.GetS3Endpoint())
	key := uuid.New().String()
	content := test.NewRandomFile(t, int64(1024))

	createRes, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String("example"),
		Key:    aws.String(key),
	})
	require.NoError(t, err)

	invalidCreds := aws.Credentials{
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "invalid-secret-key",
	}
	s3c = createS3Client(t, invalidCreds, harness.GetS3Endpoint())
	_, err = s3c.UploadPart(context.Background(), &s3.UploadPartInput{
		Bucket:     aws.String("example"),
		Key:        aws.String(key),
		UploadId:   aws.String(*createRes.UploadId),
		Body:       content,
		PartNumber: aws.Int32(1),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Unauthorized")
}

func TestShouldCompleteMultipartUpload(t *testing.T) {
	s3c := createS3Client(t, creds, harness.GetS3Endpoint())
	key := uuid.New().String()
	content1 := test.NewRandomFile(t, int64(1024*1024))
	content2 := test.NewRandomFile(t, int64(1024*1024))

	createRes, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String("example"),
		Key:    aws.String(key),
	})
	require.NoError(t, err)

	part1Res, err := s3c.UploadPart(context.Background(), &s3.UploadPartInput{
		Bucket:     aws.String("example"),
		Key:        aws.String(key),
		UploadId:   aws.String(*createRes.UploadId),
		Body:       content1,
		PartNumber: aws.Int32(1),
	})
	require.NoError(t, err)

	part2Res, err := s3c.UploadPart(context.Background(), &s3.UploadPartInput{
		Bucket:     aws.String("example"),
		Key:        aws.String(key),
		UploadId:   aws.String(*createRes.UploadId),
		Body:       content2,
		PartNumber: aws.Int32(2),
	})
	require.NoError(t, err)

	_, err = s3c.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String("example"),
		Key:      aws.String(key),
		UploadId: aws.String(*createRes.UploadId),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{
					ETag:       part1Res.ETag,
					PartNumber: aws.Int32(1),
				},
				{
					ETag:       part2Res.ETag,
					PartNumber: aws.Int32(2),
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = content1.Seek(0, io.SeekStart)
	require.NoError(t, err)
	_, err = content2.Seek(0, io.SeekStart)
	require.NoError(t, err)
	getRes, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String("example"),
		Key:    aws.String(key),
	})
	require.NoError(t, err)

	originalBuf, err := io.ReadAll(io.MultiReader(content1, content2))
	require.NoError(t, err)
	buf, err := io.ReadAll(getRes.Body)
	require.NoError(t, err)

	require.Equal(t, originalBuf, buf)

}

func TestShouldNotCompleteMultipartUploadInvalidSig(t *testing.T) {
	s3c := createS3Client(t, creds, harness.GetS3Endpoint())
	key := uuid.New().String()
	content := test.NewRandomFile(t, int64(1024*1024))

	createRes, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String("example"),
		Key:    aws.String(key),
	})
	require.NoError(t, err)

	partRes, err := s3c.UploadPart(context.Background(), &s3.UploadPartInput{
		Bucket:     aws.String("example"),
		Key:        aws.String(key),
		UploadId:   aws.String(*createRes.UploadId),
		Body:       content,
		PartNumber: aws.Int32(1),
	})
	require.NoError(t, err)

	invalidCreds := aws.Credentials{
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "invalid-secret-key",
	}
	s3c = createS3Client(t, invalidCreds, harness.GetS3Endpoint())
	_, err = s3c.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String("example"),
		Key:      aws.String(key),
		UploadId: aws.String(*createRes.UploadId),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{
					ETag:       partRes.ETag,
					PartNumber: aws.Int32(1),
				},
			},
		},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Unauthorized")
}

func TestShouldAbortMultipartUpload(t *testing.T) {
	s3c := createS3Client(t, creds, harness.GetS3Endpoint())
	key := uuid.New().String()
	content := test.NewRandomFile(t, int64(1024*1024))

	createRes, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String("example"),
		Key:    aws.String(key),
	})
	require.NoError(t, err)

	_, err = s3c.UploadPart(context.Background(), &s3.UploadPartInput{
		Bucket:     aws.String("example"),
		Key:        aws.String(key),
		UploadId:   aws.String(*createRes.UploadId),
		Body:       content,
		PartNumber: aws.Int32(1),
	})
	require.NoError(t, err)

	_, err = s3c.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
		Bucket:   aws.String("example"),
		Key:      aws.String(key),
		UploadId: aws.String(*createRes.UploadId),
	})
	require.NoError(t, err)
}

func TestShouldNotAbortMultipartUploadInvalidSig(t *testing.T) {
	s3c := createS3Client(t, creds, harness.GetS3Endpoint())
	key := uuid.New().String()
	content := test.NewRandomFile(t, int64(1024*1024))

	createRes, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String("example"),
		Key:    aws.String(key),
	})
	require.NoError(t, err)

	_, err = s3c.UploadPart(context.Background(), &s3.UploadPartInput{
		Bucket:     aws.String("example"),
		Key:        aws.String(key),
		UploadId:   aws.String(*createRes.UploadId),
		Body:       content,
		PartNumber: aws.Int32(1),
	})
	require.NoError(t, err)

	invalidCreds := aws.Credentials{
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "invalid-secret-key",
	}
	s3c = createS3Client(t, invalidCreds, harness.GetS3Endpoint())
	_, err = s3c.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
		Bucket:   aws.String("example"),
		Key:      aws.String(key),
		UploadId: aws.String(*createRes.UploadId),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Unauthorized")
}

func createS3Client(t *testing.T, creds aws.Credentials, endpoint string) *s3.Client {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(creds.AccessKeyID, creds.SecretAccessKey, "")),
		config.WithRegion("us-east-1"),
	)
	require.NoError(t, err)

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
}
