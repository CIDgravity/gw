package test

import (
	"context"
	"crypto/sha256"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	"github.com/aws/smithy-go/transport/http"
)

type S3TestClient struct {
	*s3.Client
}

func (stc *S3TestClient) GetObjectSha(bucket, key string) (string, error) {
	return stc.getObjectSha(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
}

func (stc *S3TestClient) GetObjectVersionSha(bucket, key, version string) (string, error) {
	return stc.getObjectSha(&s3.GetObjectInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(key),
		VersionId: aws.String(version),
	})
}

func (stc *S3TestClient) getObjectSha(opts *s3.GetObjectInput) (string, error) {
	result, err := stc.GetObject(context.Background(), opts)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	_, err = io.Copy(hash, result.Body)
	if err != nil {
		return "", err
	}

	return string(hash.Sum(nil)), nil
}

type HeadObjectExtOutput struct {
	*s3.HeadObjectOutput
	Cid           string
	OffloadStatus string
}

func (s *S3TestClient) HeadObjectExt(ctx context.Context, params *s3.HeadObjectInput) (*HeadObjectExtOutput, error) {
	var cid string
	var offloadStatus string
	res, err := s.HeadObject(ctx, params, func(o *s3.Options) {
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			err := stack.Build.Add(middleware.BuildMiddlewareFunc("AddQueryParam", func(ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler) (
				out middleware.BuildOutput, metadata middleware.Metadata, err error,
			) {
				if req, ok := in.Request.(*http.Request); ok {
					q := req.URL.Query()
					q.Set("fil-include-meta", "1")
					req.URL.RawQuery = q.Encode()
				}
				return next.HandleBuild(ctx, in)
			}), middleware.Before)
			if err != nil {
				return err
			}

			err = stack.Deserialize.Add(middleware.DeserializeMiddlewareFunc("CaptureHeaders",
				func(ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler) (middleware.DeserializeOutput, middleware.Metadata, error) {
					out, md, err := next.HandleDeserialize(ctx, in)
					if resp, ok := out.RawResponse.(*http.Response); ok && resp != nil {
						cid = resp.Header.Get("X-Fil-Cid")
						offloadStatus = resp.Header.Get("X-Fil-Offload-Status")
					}
					return out, md, err
				},
			), middleware.After)
			if err != nil {
				return err
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return &HeadObjectExtOutput{
		HeadObjectOutput: res,
		Cid:              cid,
		OffloadStatus:    offloadStatus,
	}, nil
}
