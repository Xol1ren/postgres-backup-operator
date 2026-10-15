// Package runner implements the streaming backup/restore pipelines executed inside Job pods.
package runner

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/klauspost/compress/zstd"

	"github.com/Xol1ren/postgres-backup-operator/internal/storage"
)

// Extension returns the object-key suffix for a compression algorithm.
func Extension(compression string) string {
	if compression == "zstd" {
		return ".sql.zst"
	}
	return ".sql.gz"
}

// Backup compresses src on the fly and streams it to store at key via multipart upload.
// No data touches local disk. It returns the compressed bytes uploaded.
func Backup(ctx context.Context, store storage.ObjectStore, key, compression string, src io.Reader) (int64, error) {
	pr, pw := io.Pipe()
	go func() {
		pw.CloseWithError(compress(pw, compression, src))
	}()
	n, err := store.Upload(ctx, key, pr)
	if err != nil {
		// Unblock the compressor goroutine if the upload aborted early.
		_ = pr.CloseWithError(err)
		return 0, err
	}
	return n, nil
}

func compress(dst io.Writer, compression string, src io.Reader) error {
	switch compression {
	case "zstd":
		zw, err := zstd.NewWriter(dst)
		if err != nil {
			return err
		}
		if _, err := io.Copy(zw, src); err != nil {
			_ = zw.Close()
			return err
		}
		return zw.Close()
	case "gzip", "":
		gw := gzip.NewWriter(dst)
		if _, err := io.Copy(gw, src); err != nil {
			_ = gw.Close()
			return err
		}
		return gw.Close()
	default:
		return fmt.Errorf("unsupported compression %q", compression)
	}
}

// Restore streams the object at key from store, decompresses it (detected by suffix) and writes it to dst.
func Restore(ctx context.Context, store storage.ObjectStore, key string, dst io.Writer) (int64, error) {
	rc, err := store.Download(ctx, key)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rc.Close() }()

	var r io.Reader
	switch {
	case strings.HasSuffix(key, ".zst"):
		zr, err := zstd.NewReader(rc)
		if err != nil {
			return 0, err
		}
		defer zr.Close()
		r = zr
	case strings.HasSuffix(key, ".gz"):
		gr, err := gzip.NewReader(rc)
		if err != nil {
			return 0, fmt.Errorf("opening gzip stream: %w", err)
		}
		defer func() { _ = gr.Close() }()
		r = gr
	default:
		r = rc
	}
	return io.Copy(dst, r)
}

