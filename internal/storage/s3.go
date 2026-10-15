package storage

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager" //nolint:staticcheck // transfermanager is not yet stable
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	partSize    = 16 * 1024 * 1024
	concurrency = 3
	deleteBatch = 1000
)

// s3API is the subset of the S3 client used for non-upload operations.
type s3API interface {
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObjects(ctx context.Context, in *s3.DeleteObjectsInput, opts ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// multipartUploader is the subset of manager.Uploader we rely on (mockable).
type multipartUploader interface {
	Upload(ctx context.Context, in *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error)
}

// S3Store implements ObjectStore on top of the AWS SDK v2.
type S3Store struct {
	bucket   string
	client   s3API
	uploader multipartUploader
}

// NewS3 builds an S3Store for AWS S3 or any S3-compatible endpoint such as MinIO.
func NewS3(ctx context.Context, cfg Config) (ObjectStore, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("bucket is required")
	}
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	opts := []func(*config.LoadOptions) error{config.WithRegion(region)}
	if cfg.AccessKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")))
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("loading aws config: %w", err)
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.ForcePathStyle
	})
	up := manager.NewUploader(client, func(u *manager.Uploader) {
		u.PartSize = partSize
		u.Concurrency = concurrency
	})
	return &S3Store{bucket: cfg.Bucket, client: client, uploader: up}, nil
}

// countingReader tracks bytes consumed by the uploader.
type countingReader struct {
	r io.Reader
	n atomic.Int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(int64(n))
	return n, err
}

// Upload streams body to S3 without buffering the full object on disk.
func (s *S3Store) Upload(ctx context.Context, key string, body io.Reader) (int64, error) {
	cr := &countingReader{r: body}
	_, err := s.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   cr,
	})
	if err != nil {
		return 0, fmt.Errorf("uploading %q: %w", key, err)
	}
	return cr.n.Load(), nil
}

// Download opens an object for streaming.
func (s *S3Store) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		return nil, fmt.Errorf("downloading %q: %w", key, err)
	}
	return out.Body, nil
}

// List returns every object under prefix, following pagination.
func (s *S3Store) List(ctx context.Context, prefix string) ([]Object, error) {
	var (
		objs  []Object
		token *string
	)
	for {
		out, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("listing %q: %w", prefix, err)
		}
		for _, o := range out.Contents {
			obj := Object{Key: aws.ToString(o.Key), Size: aws.ToInt64(o.Size)}
			if o.LastModified != nil {
				obj.LastModified = *o.LastModified
			}
			objs = append(objs, obj)
		}
		if !aws.ToBool(out.IsTruncated) || out.NextContinuationToken == nil {
			return objs, nil
		}
		token = out.NextContinuationToken
	}
}

// Delete removes keys in batches of up to 1000.
func (s *S3Store) Delete(ctx context.Context, keys []string) error {
	for start := 0; start < len(keys); start += deleteBatch {
		end := min(start+deleteBatch, len(keys))
		ids := make([]types.ObjectIdentifier, 0, end-start)
		for _, k := range keys[start:end] {
			ids = append(ids, types.ObjectIdentifier{Key: aws.String(k)})
		}
		out, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(s.bucket),
			Delete: &types.Delete{Objects: ids, Quiet: aws.Bool(true)},
		})
		if err != nil {
			return fmt.Errorf("deleting objects: %w", err)
		}
		if len(out.Errors) > 0 {
			e := out.Errors[0]
			return fmt.Errorf("deleting %q: %s: %s", aws.ToString(e.Key), aws.ToString(e.Code), aws.ToString(e.Message))
		}
	}
	return nil
}

