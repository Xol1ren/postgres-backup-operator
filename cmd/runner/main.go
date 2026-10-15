// Command runner executes inside backup/restore Job pods. It streams pg_dump output
// through a compressor straight into S3 (backup) or S3 through a decompressor into psql (restore).
package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"syscall"

	"github.com/Xol1ren/postgres-backup-operator/internal/runner"
	"github.com/Xol1ren/postgres-backup-operator/internal/storage"
)

func main() {
	if len(os.Args) != 2 || (os.Args[1] != "backup" && os.Args[1] != "restore") {
		fmt.Fprintln(os.Stderr, "usage: runner backup|restore")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	var err error
	switch os.Args[1] {
	case "backup":
		err = runBackup(ctx)
	case "restore":
		err = runRestore(ctx)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func newStore(ctx context.Context) (storage.ObjectStore, error) {
	return storage.NewS3(ctx, storage.Config{
		Endpoint:       os.Getenv("S3_ENDPOINT"),
		Bucket:         os.Getenv("S3_BUCKET"),
		Region:         os.Getenv("S3_REGION"),
		AccessKey:      os.Getenv("S3_ACCESS_KEY"),
		SecretKey:      os.Getenv("S3_SECRET_KEY"),
		ForcePathStyle: os.Getenv("S3_FORCE_PATH_STYLE") == "true",
	})
}

func pgArgs() []string {
	port := os.Getenv("PG_PORT")
	if port == "" {
		port = "5432"
	}
	return []string{"-h", os.Getenv("PG_HOST"), "-p", port, "-U", os.Getenv("PG_USER"), "-d", os.Getenv("PG_DBNAME")}
}

func runBackup(ctx context.Context) error {
	store, err := newStore(ctx)
	if err != nil {
		return err
	}
	key := os.Getenv("S3_KEY")
	args := append(pgArgs(), "--no-owner", "--clean", "--if-exists")
	cmd := exec.CommandContext(ctx, "pg_dump", args...) //nolint:gosec // args come from operator-controlled env
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting pg_dump: %w", err)
	}
	n, upErr := runner.Backup(ctx, store, key, os.Getenv("COMPRESSION"), stdout)
	waitErr := cmd.Wait()
	if waitErr != nil || upErr != nil {
		// Never leave a truncated dump behind that retention or restore could pick up.
		_ = store.Delete(context.WithoutCancel(ctx), []string{key})
		if waitErr != nil {
			return fmt.Errorf("pg_dump failed: %w", waitErr)
		}
		return upErr
	}
	fmt.Printf("backup complete: %s (%d bytes compressed)\n", key, n)
	return nil
}

func runRestore(ctx context.Context) error {
	store, err := newStore(ctx)
	if err != nil {
		return err
	}
	args := append(pgArgs(), "-v", "ON_ERROR_STOP=1", "-q")
	cmd := exec.CommandContext(ctx, "psql", args...) //nolint:gosec // args come from operator-controlled env
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting psql: %w", err)
	}
	n, rErr := runner.Restore(ctx, store, os.Getenv("S3_KEY"), stdin)
	_ = stdin.Close()
	waitErr := cmd.Wait()
	if rErr != nil {
		return rErr
	}
	if waitErr != nil {
		return fmt.Errorf("psql failed: %w", waitErr)
	}
	fmt.Printf("restore complete: %d bytes replayed\n", n)
	return nil
}

