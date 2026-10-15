// Package storage abstracts S3-compatible object storage used for backups.
package storage

import (
	"context"
	"io"
	"time"
)

// Object describes a stored object.
type Object struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// ObjectStore is the minimal object storage surface the operator needs.
type ObjectStore interface {
	// Upload streams body to key using multipart upload and returns the bytes uploaded.
	Upload(ctx context.Context, key string, body io.Reader) (int64, error)
	// Download opens key for streaming.
	Download(ctx context.Context, key string) (io.ReadCloser, error)
	// List returns all objects under prefix.
	List(ctx context.Context, prefix string) ([]Object, error)
	// Delete removes the given keys.
	Delete(ctx context.Context, keys []string) error
}

// Config holds connection settings for an S3-compatible store.
type Config struct {
	Endpoint       string
	Bucket         string
	Region         string
	AccessKey      string
	SecretKey      string
	ForcePathStyle bool
}

// Factory builds an ObjectStore from a Config; injected so controllers can be tested without S3.
type Factory func(ctx context.Context, cfg Config) (ObjectStore, error)

