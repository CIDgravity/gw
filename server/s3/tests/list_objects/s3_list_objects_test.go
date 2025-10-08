package list_objects

import (
	"context"
	"os"
	"sort"
	"testing"

	"github.com/CIDgravity/filecoin-gateway/test"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/test-go/testify/require"
)

var harness *test.FgwHarness

var creds = aws.Credentials{
	AccessKeyID:     "test-access-key",
	SecretAccessKey: "test-secret-key",
}

func TestMain(m *testing.M) {
	harness = test.NewFgwHarness()
	defer harness.Stop()
	os.Exit(m.Run())
}

func createS3Client(t *testing.T, creds aws.Credentials) *s3.Client {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(creds.AccessKeyID, creds.SecretAccessKey, "")),
		config.WithRegion("us-east-1"),
	)
	require.NoError(t, err)

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(harness.GetS3Endpoint())
		o.UsePathStyle = true
	})
}

func createObjects(t *testing.T, s3c *s3.Client, bucket string, keys []string) {
	for _, k := range keys {
		_, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(k),
			Body:   test.NewRandomFile(t, int64(128)),
		})
		require.NoError(t, err)
	}
}

func TestListObjectsV2_EmptyBucket(t *testing.T) {
	bucket := uuid.NewString()
	s3c := createS3Client(t, creds)
	res, err := s3c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)
	require.Empty(t, res.Contents)
	require.Empty(t, res.CommonPrefixes)
	require.Equal(t, int32(0), *res.KeyCount)
	require.Empty(t, aws.ToString(res.NextContinuationToken))
	require.False(t, aws.ToBool(res.IsTruncated))
	require.Equal(t, bucket, aws.ToString(res.Name))
}

func TestListObjectsV2_ListAll(t *testing.T) {
	bucket := uuid.NewString()
	keys := []string{"ĄĘ", "key1", "key4", "key2", "key3", "👍", "</Contents>"}
	s3c := createS3Client(t, creds)
	createObjects(t, s3c, bucket, keys)

	res, err := s3c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)
	require.Equal(t, 7, len(res.Contents))
	require.Equal(t, 0, len(res.CommonPrefixes))
	require.Equal(t, int32(7), *res.KeyCount)
	require.Empty(t, aws.ToString(res.NextContinuationToken))
	require.False(t, aws.ToBool(res.IsTruncated))
	require.Equal(t, bucket, aws.ToString(res.Name))

	var collected []string
	for _, o := range res.Contents {
		collected = append(collected, aws.ToString(o.Key))
	}
	sort.Strings(keys)
	require.Equal(t, keys, collected)
}

func TestListObjectsV2_ListAllWithPagination(t *testing.T) {
	bucket := uuid.NewString()
	keys := []string{"key1", "key10", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9"}
	s3c := createS3Client(t, creds)
	createObjects(t, s3c, bucket, keys)
	res, err := s3c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		MaxKeys: aws.Int32(5),
	})
	require.NoError(t, err)
	require.Equal(t, 5, len(res.Contents))
	require.Equal(t, 0, len(res.CommonPrefixes))
	require.Equal(t, int32(5), *res.KeyCount)
	require.NotEmpty(t, aws.ToString(res.NextContinuationToken))
	require.True(t, aws.ToBool(res.IsTruncated))
	require.Equal(t, bucket, aws.ToString(res.Name))

	var collected []string
	for _, o := range res.Contents {
		collected = append(collected, aws.ToString(o.Key))
	}
	require.Equal(t, keys[:5], collected)

	res, err = s3c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket:            aws.String(bucket),
		MaxKeys:           aws.Int32(5),
		ContinuationToken: res.NextContinuationToken,
	})
	require.NoError(t, err)
	require.Equal(t, 5, len(res.Contents))
	require.Equal(t, 0, len(res.CommonPrefixes))
	require.Equal(t, int32(5), *res.KeyCount)
	require.Empty(t, aws.ToString(res.NextContinuationToken))
	require.False(t, aws.ToBool(res.IsTruncated))

	var collected2 []string
	for _, o := range res.Contents {
		collected2 = append(collected2, aws.ToString(o.Key))
	}
	require.Equal(t, keys[5:], collected2)
}

func TestListObjectsV2_StartAfter(t *testing.T) {
	bucket := uuid.NewString()
	keys := []string{"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9", "key10"}
	s3c := createS3Client(t, creds)
	createObjects(t, s3c, bucket, keys)
	res, err := s3c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket:     aws.String(bucket),
		StartAfter: aws.String("key5"),
	})
	require.NoError(t, err)
	require.Equal(t, 4, len(res.Contents))
	require.Equal(t, 0, len(res.CommonPrefixes))
	require.Equal(t, int32(4), *res.KeyCount)
	require.Empty(t, aws.ToString(res.NextContinuationToken))
	require.False(t, aws.ToBool(res.IsTruncated))
	require.Equal(t, bucket, aws.ToString(res.Name))

	var collected []string
	for _, o := range res.Contents {
		collected = append(collected, aws.ToString(o.Key))
	}
	sort.Strings(keys)
	require.Equal(t, keys[6:], collected)
}

func TestListObjectsV2_ListPrefix(t *testing.T) {
	bucket := uuid.NewString()
	keys := []string{"key1", "key2", "aakey3", "aakey4", "abkey5", "aa/akey6"}
	s3c := createS3Client(t, creds)
	createObjects(t, s3c, bucket, keys)
	res, err := s3c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String("aa"),
	})
	require.NoError(t, err)
	require.Equal(t, 3, len(res.Contents))
	require.Equal(t, 0, len(res.CommonPrefixes))
	require.Equal(t, int32(3), *res.KeyCount)
	require.Empty(t, aws.ToString(res.NextContinuationToken))
	require.False(t, aws.ToBool(res.IsTruncated))
	require.Equal(t, bucket, aws.ToString(res.Name))

	var collected []string
	for _, o := range res.Contents {
		collected = append(collected, aws.ToString(o.Key))
	}
	sort.Strings(keys)
	require.Equal(t, []string{"aa/akey6", "aakey3", "aakey4"}, collected)
}

func TestListObjectsV2_ListPrefixWithPagination(t *testing.T) {
	bucket := uuid.NewString()
	keys := []string{"key1", "key2", "aakey3", "aakey4", "abkey5", "aa/akey6"}
	s3c := createS3Client(t, creds)
	createObjects(t, s3c, bucket, keys)
	res, err := s3c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String("aa"),
		MaxKeys: aws.Int32(2),
	})
	require.NoError(t, err)
	require.Equal(t, 2, len(res.Contents))
	require.Equal(t, 0, len(res.CommonPrefixes))
	require.Equal(t, int32(2), *res.KeyCount)
	require.NotEmpty(t, aws.ToString(res.NextContinuationToken))
	require.True(t, aws.ToBool(res.IsTruncated))
	require.Equal(t, bucket, aws.ToString(res.Name))

	var collected []string
	for _, o := range res.Contents {
		collected = append(collected, aws.ToString(o.Key))
	}
	require.Equal(t, []string{"aa/akey6", "aakey3"}, collected)

	res, err = s3c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket:            aws.String(bucket),
		Prefix:            aws.String("aa"),
		MaxKeys:           aws.Int32(2),
		ContinuationToken: res.NextContinuationToken,
	})

	require.NoError(t, err)
	require.Equal(t, 1, len(res.Contents))
	require.Equal(t, 0, len(res.CommonPrefixes))
	require.Equal(t, int32(1), *res.KeyCount)
	require.Empty(t, aws.ToString(res.NextContinuationToken))
	require.False(t, aws.ToBool(res.IsTruncated))
	require.Equal(t, "aakey4", aws.ToString(res.Contents[0].Key))
}

func TestListObjectsV2_ListPrefixStartAfter(t *testing.T) {
	bucket := uuid.NewString()
	keys := []string{"key1", "key2", "aakey3", "aakey4", "abkey5", "aa/akey6"}
	s3c := createS3Client(t, creds)
	createObjects(t, s3c, bucket, keys)
	res, err := s3c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket:     aws.String(bucket),
		Prefix:     aws.String("aa"),
		StartAfter: aws.String("aakey3"),
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(res.Contents))
	require.Equal(t, 0, len(res.CommonPrefixes))
	require.Equal(t, int32(1), *res.KeyCount)
	require.Empty(t, aws.ToString(res.NextContinuationToken))
	require.False(t, aws.ToBool(res.IsTruncated))
	require.Equal(t, bucket, aws.ToString(res.Name))

	var collected []string
	for _, o := range res.Contents {
		collected = append(collected, aws.ToString(o.Key))
	}
	require.Equal(t, []string{"aakey4"}, collected)
}

func TestListObjectsV2_MaxKeysZero(t *testing.T) {
	bucket := uuid.NewString()
	keys := []string{"key1", "key2", "key3"}
	s3c := createS3Client(t, creds)
	createObjects(t, s3c, bucket, keys)
	_, err := s3c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		MaxKeys: aws.Int32(0),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Bad Request")
}

func TestListObjectsV2_MaxKeysOver1000(t *testing.T) {
	bucket := uuid.NewString()
	keys := []string{"key1", "key2", "key3"}
	s3c := createS3Client(t, creds)
	createObjects(t, s3c, bucket, keys)
	_, err := s3c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		MaxKeys: aws.Int32(1001),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Bad Request")
}

func TestListObjectsV2_Delimiter(t *testing.T) {
	bucket := uuid.NewString()
	keys := []string{"key1", "key2", "dir1/key3", "dir2/key4"}
	s3c := createS3Client(t, creds)
	createObjects(t, s3c, bucket, keys)
	res, err := s3c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Delimiter: aws.String("/"),
	})
	require.NoError(t, err)
	require.Equal(t, 2, len(res.Contents))
	require.Equal(t, 2, len(res.CommonPrefixes))
	require.Equal(t, int32(4), *res.KeyCount)
	require.Empty(t, aws.ToString(res.NextContinuationToken))
	require.False(t, aws.ToBool(res.IsTruncated))
	require.Equal(t, bucket, aws.ToString(res.Name))

	var collectedKeys []string
	for _, o := range res.Contents {
		collectedKeys = append(collectedKeys, aws.ToString(o.Key))
	}
	require.Equal(t, []string{"key1", "key2"}, collectedKeys)

	var collectedPrefixes []string
	for _, cp := range res.CommonPrefixes {
		collectedPrefixes = append(collectedPrefixes, aws.ToString(cp.Prefix))
	}
	require.Equal(t, []string{"dir1/", "dir2/"}, collectedPrefixes)
}

func TestListObjectsV2_DelimiterWithPagination(t *testing.T) {
	bucket := uuid.NewString()
	keys := []string{"key1", "key2", "dir1/key3", "dir1/key4", "dir1/dir2/key5", "dir1/dir2/key6", "dir1/dir3/key7", "dir2/key8"}
	s3c := createS3Client(t, creds)
	createObjects(t, s3c, bucket, keys)
	res, err := s3c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int32(2),
		Prefix:    aws.String("dir1/"),
	})
	require.NoError(t, err)
	require.Equal(t, 0, len(res.Contents))
	require.Equal(t, 2, len(res.CommonPrefixes))
	require.Equal(t, int32(2), *res.KeyCount)
	require.NotEmpty(t, aws.ToString(res.NextContinuationToken))
	require.True(t, aws.ToBool(res.IsTruncated))
	require.Equal(t, bucket, aws.ToString(res.Name))
	require.Equal(t, "dir1/dir2/", aws.ToString(res.CommonPrefixes[0].Prefix))
	require.Equal(t, "dir1/dir3/", aws.ToString(res.CommonPrefixes[1].Prefix))

	res, err = s3c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket:            aws.String(bucket),
		Delimiter:         aws.String("/"),
		MaxKeys:           aws.Int32(2),
		Prefix:            aws.String("dir1/"),
		ContinuationToken: res.NextContinuationToken,
	})
	require.NoError(t, err)
	require.Equal(t, 2, len(res.Contents))
	require.Equal(t, 0, len(res.CommonPrefixes))
	require.Equal(t, int32(2), *res.KeyCount)
	require.Empty(t, aws.ToString(res.NextContinuationToken))
	require.False(t, aws.ToBool(res.IsTruncated))
	require.Equal(t, "dir1/key3", aws.ToString(res.Contents[0].Key))
	require.Equal(t, "dir1/key4", aws.ToString(res.Contents[1].Key))
}

func TestListObjectsV2_DelimiterWithStartAfter(t *testing.T) {
	bucket := uuid.NewString()
	keys := []string{"key1", "key2", "dir1/key3", "dir1/key4", "dir1/dir2/key5", "dir1/dir2/key6", "dir1/dir1key", "dir1/dir3/key7", "dir2/key8"}
	s3c := createS3Client(t, creds)
	createObjects(t, s3c, bucket, keys)
	res, err := s3c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket:     aws.String(bucket),
		Delimiter:  aws.String("/"),
		Prefix:     aws.String("dir1/"),
		StartAfter: aws.String("dir1/dir2/key5"),
	})
	require.NoError(t, err)
	require.Equal(t, 2, len(res.Contents))
	require.Equal(t, 1, len(res.CommonPrefixes))
	require.Equal(t, int32(3), *res.KeyCount)
	require.Empty(t, aws.ToString(res.NextContinuationToken))
	require.False(t, aws.ToBool(res.IsTruncated))
	require.Equal(t, "dir1/dir3/", aws.ToString(res.CommonPrefixes[0].Prefix))
	require.Equal(t, "dir1/key3", aws.ToString(res.Contents[0].Key))
	require.Equal(t, "dir1/key4", aws.ToString(res.Contents[1].Key))
}

func TestListObjectsV2_InvalidSignature(t *testing.T) {
	invalid := aws.Credentials{AccessKeyID: "test-access-key", SecretAccessKey: "invalid-secret-key"}
	bucket := uuid.NewString()
	s3c := createS3Client(t, invalid)
	_, err := s3c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Unauthorized")
}
