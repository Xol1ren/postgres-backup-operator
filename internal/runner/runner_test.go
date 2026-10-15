package runner

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/Xol1ren/postgres-backup-operator/internal/storage"
)

type memStore struct {
	data map[string][]byte
	err  error
}

func (m *memStore) Upload(_ context.Context, key string, r io.Reader) (int64, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	if m.err != nil {
		return 0, m.err
	}
	if m.data == nil {
		m.data = map[string][]byte{}
	}
	m.data[key] = b
	return int64(len(b)), nil
}

func (m *memStore) Download(_ context.Context, key string) (io.ReadCloser, error) {
	b, ok := m.data[key]
	if !ok {
		return nil, errors.New("not found")
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (m *memStore) List(context.Context, string) ([]storage.Object, error) { return nil, nil }
func (m *memStore) Delete(context.Context, []string) error                  { return nil }

func roundTrip(t *testing.T, compression string) {
	t.Helper()
	st := &memStore{}
	payload := strings.Repeat("INSERT INTO t VALUES (1);\n", 5000)
	key := "ns/name/x" + Extension(compression)
	n, err := Backup(context.Background(), st, key, compression, strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	if n <= 0 || n >= int64(len(payload)) {
		t.Errorf("expected compressed size in (0,%d), got %d", len(payload), n)
	}
	var out bytes.Buffer
	if _, err := Restore(context.Background(), st, key, &out); err != nil {
		t.Fatal(err)
	}
	if out.String() != payload {
		t.Error("round trip mismatch")
	}
}

func TestRoundTripGzip(t *testing.T) { roundTrip(t, "gzip") }
func TestRoundTripZstd(t *testing.T) { roundTrip(t, "zstd") }

func TestBackupSourceError(t *testing.T) {
	st := &memStore{}
	_, err := Backup(context.Background(), st, "k.sql.gz", "gzip", errReader{})
	if err == nil {
		t.Fatal("expected source error to abort the upload")
	}
	if len(st.data) != 0 {
		t.Error("no object should be stored when the source fails")
	}
}

func TestBackupUploadError(t *testing.T) {
	st := &memStore{err: errors.New("s3 down")}
	if _, err := Backup(context.Background(), st, "k", "gzip", strings.NewReader("x")); err == nil {
		t.Fatal("expected upload error")
	}
}

func TestUnsupportedCompression(t *testing.T) {
	st := &memStore{}
	if _, err := Backup(context.Background(), st, "k", "lz4", strings.NewReader("x")); err == nil {
		t.Fatal("expected error")
	}
}

func TestRestoreMissingObject(t *testing.T) {
	if _, err := Restore(context.Background(), &memStore{}, "nope.sql.gz", io.Discard); err == nil {
		t.Fatal("expected error")
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("pg_dump failed") }

