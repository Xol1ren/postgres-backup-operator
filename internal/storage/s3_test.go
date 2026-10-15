package storage

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager" //nolint:staticcheck // see s3.go
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type mockUploader struct {
	gotKey   string
	gotBytes string
	err      error
}

func (m *mockUploader) Upload(_ context.Context, in *s3.PutObjectInput, _ ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
	m.gotKey = aws.ToString(in.Key)
	b, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	m.gotBytes = string(b)
	if m.err != nil {
		return nil, m.err
	}
	return &manager.UploadOutput{}, nil
}

type mockS3 struct {
	pages   []*s3.ListObjectsV2Output
	calls   int
	deleted [][]string
	delErrs []types.Error
	getBody string
}

func (m *mockS3) ListObjectsV2(_ context.Context, _ *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	p := m.pages[m.calls]
	m.calls++
	return p, nil
}

func (m *mockS3) DeleteObjects(_ context.Context, in *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	var ks []string
	for _, o := range in.Delete.Objects {
		ks = append(ks, aws.ToString(o.Key))
	}
	m.deleted = append(m.deleted, ks)
	return &s3.DeleteObjectsOutput{Errors: m.delErrs}, nil
}

func (m *mockS3) GetObject(_ context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return &s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader(m.getBody))}, nil
}

func TestUploadStreamsAndCounts(t *testing.T) {
	up := &mockUploader{}
	st := &S3Store{bucket: "b", uploader: up}
	n, err := st.Upload(context.Background(), "a/b.sql.gz", strings.NewReader("hello world"))
	if err != nil {
		t.Fatal(err)
	}
	if n != 11 || up.gotKey != "a/b.sql.gz" || up.gotBytes != "hello world" {
		t.Errorf("unexpected upload: n=%d key=%q body=%q", n, up.gotKey, up.gotBytes)
	}
}

func TestUploadError(t *testing.T) {
	up := &mockUploader{err: errors.New("boom")}
	st := &S3Store{bucket: "b", uploader: up}
	if _, err := st.Upload(context.Background(), "k", strings.NewReader("x")); err == nil {
		t.Fatal("expected error")
	}
}

func TestListPagination(t *testing.T) {
	now := time.Now()
	m := &mockS3{pages: []*s3.ListObjectsV2Output{
		{
			Contents:              []types.Object{{Key: aws.String("p/1"), Size: aws.Int64(1), LastModified: &now}},
			IsTruncated:           aws.Bool(true),
			NextContinuationToken: aws.String("t"),
		},
		{Contents: []types.Object{{Key: aws.String("p/2"), Size: aws.Int64(2)}}},
	}}
	st := &S3Store{bucket: "b", client: m}
	objs, err := st.List(context.Background(), "p/")
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 2 || objs[0].Key != "p/1" || objs[1].Size != 2 || m.calls != 2 {
		t.Errorf("unexpected list result: %+v calls=%d", objs, m.calls)
	}
}

func TestDeleteBatches(t *testing.T) {
	m := &mockS3{}
	st := &S3Store{bucket: "b", client: m}
	keys := make([]string, 2500)
	for i := range keys {
		keys[i] = "k"
	}
	if err := st.Delete(context.Background(), keys); err != nil {
		t.Fatal(err)
	}
	if len(m.deleted) != 3 || len(m.deleted[0]) != 1000 || len(m.deleted[2]) != 500 {
		t.Errorf("unexpected batching: %d batches", len(m.deleted))
	}
	m.delErrs = []types.Error{{Key: aws.String("k"), Code: aws.String("AccessDenied"), Message: aws.String("no")}}
	if err := st.Delete(context.Background(), keys[:1]); err == nil {
		t.Error("expected per-object delete error")
	}
}

func TestDownload(t *testing.T) {
	st := &S3Store{bucket: "b", client: &mockS3{getBody: "data"}}
	rc, err := st.Download(context.Background(), "k")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rc.Close() }()
	b, _ := io.ReadAll(rc)
	if string(b) != "data" {
		t.Errorf("got %q", b)
	}
}

func TestNewS3Validation(t *testing.T) {
	if _, err := NewS3(context.Background(), Config{}); err == nil {
		t.Error("expected error for missing bucket")
	}
	if _, err := NewS3(context.Background(), Config{Bucket: "b", Endpoint: "http://localhost:9000", AccessKey: "a", SecretKey: "s", ForcePathStyle: true}); err != nil {
		t.Errorf("unexpected: %v", err)
	}
}

