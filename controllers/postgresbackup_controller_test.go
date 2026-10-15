package controllers

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/Xol1ren/postgres-backup-operator/api/v1alpha1"
	"github.com/Xol1ren/postgres-backup-operator/internal/metrics"
	"github.com/Xol1ren/postgres-backup-operator/internal/storage"
)

const ns = "default"

var baseTime = time.Date(2026, 10, 15, 1, 0, 0, 0, time.UTC)

type fakeStore struct {
	objs    []storage.Object
	deleted []string
}

func (f *fakeStore) Upload(context.Context, string, io.Reader) (int64, error) { return 0, nil }
func (f *fakeStore) Download(context.Context, string) (io.ReadCloser, error)  { return nil, nil }
func (f *fakeStore) List(context.Context, string) ([]storage.Object, error)   { return f.objs, nil }
func (f *fakeStore) Delete(_ context.Context, keys []string) error {
	f.deleted = append(f.deleted, keys...)
	return nil
}

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func newClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithStatusSubresource(&v1alpha1.PostgresBackup{}, &v1alpha1.PostgresRestore{}).
		WithObjects(objs...).Build()
}

func credSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "s3-creds", Namespace: ns},
		Data:       map[string][]byte{"accessKeyId": []byte("ak"), "secretAccessKey": []byte("sk")},
	}
}

func backupObj(schedule string) *v1alpha1.PostgresBackup {
	return &v1alpha1.PostgresBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: ns, UID: "uid-1", CreationTimestamp: metav1.NewTime(baseTime)},
		Spec: v1alpha1.PostgresBackupSpec{
			DatabaseURISecretRef: corev1.LocalObjectReference{Name: "db"},
			Schedule:             schedule,
			RetentionDays:        7,
			Compression:          v1alpha1.CompressionGzip,
			Storage: v1alpha1.StorageSpec{
				Endpoint: "http://minio:9000", Bucket: "backups", Region: "us-east-1",
				CredentialSecretRef: corev1.LocalObjectReference{Name: "s3-creds"},
			},
		},
	}
}

type harness struct {
	c     client.Client
	r     *PostgresBackupReconciler
	store *fakeStore
	now   time.Time
}

func newHarness(t *testing.T, b *v1alpha1.PostgresBackup, extra ...client.Object) *harness {
	t.Helper()
	h := &harness{store: &fakeStore{}, now: baseTime.Add(30 * time.Minute)}
	h.c = newClient(t, append([]client.Object{b, credSecret()}, extra...)...)
	h.r = &PostgresBackupReconciler{
		Client: h.c, Scheme: newScheme(t), RunnerImage: "runner:test",
		StoreFactory: func(context.Context, storage.Config) (storage.ObjectStore, error) { return h.store, nil },
		Now:          func() time.Time { return h.now },
	}
	return h
}

func (h *harness) reconcile(t *testing.T) ctrl.Result {
	t.Helper()
	res, err := h.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "nightly"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return res
}

func (h *harness) backup(t *testing.T) *v1alpha1.PostgresBackup {
	t.Helper()
	var b v1alpha1.PostgresBackup
	if err := h.c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "nightly"}, &b); err != nil {
		t.Fatal(err)
	}
	return &b
}

func (h *harness) finishJob(t *testing.T, name string, cond batchv1.JobConditionType, msg string) {
	t.Helper()
	var job batchv1.Job
	if err := h.c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, &job); err != nil {
		t.Fatal(err)
	}
	st := metav1.NewTime(h.now.Add(-2 * time.Minute))
	ct := metav1.NewTime(h.now)
	job.Status.StartTime = &st
	job.Status.CompletionTime = &ct
	job.Status.Conditions = []batchv1.JobCondition{{Type: cond, Status: corev1.ConditionTrue, Message: msg}}
	if err := h.c.Status().Update(context.Background(), &job); err != nil {
		t.Fatal(err)
	}
}

func TestBackupInvalidSchedule(t *testing.T) {
	h := newHarness(t, backupObj("garbage"))
	h.reconcile(t)
	if got := h.backup(t).Status.Phase; got != v1alpha1.PhaseFailed {
		t.Errorf("phase = %q, want Failed", got)
	}
}

func TestBackupPendingWaitsUntilDue(t *testing.T) {
	h := newHarness(t, backupObj("0 2 * * *")) // next tick 02:00, now 01:30
	res := h.reconcile(t)
	b := h.backup(t)
	if b.Status.Phase != v1alpha1.PhasePending {
		t.Errorf("phase = %q, want Pending", b.Status.Phase)
	}
	if res.RequeueAfter != 30*time.Minute {
		t.Errorf("RequeueAfter = %v, want 30m", res.RequeueAfter)
	}
	if b.Status.NextBackupTime == nil || !b.Status.NextBackupTime.Time.Equal(baseTime.Add(time.Hour)) {
		t.Errorf("unexpected NextBackupTime %v", b.Status.NextBackupTime)
	}
	var jobs batchv1.JobList
	_ = h.c.List(context.Background(), &jobs)
	if len(jobs.Items) != 0 {
		t.Error("no job should exist before schedule is due")
	}
}

func TestBackupPendingToRunningCreatesJob(t *testing.T) {
	h := newHarness(t, backupObj("0 2 * * *"))
	h.now = baseTime.Add(90 * time.Minute) // 02:30 — due
	h.reconcile(t)

	b := h.backup(t)
	if b.Status.Phase != v1alpha1.PhaseRunning || b.Status.ActiveJob == "" || b.Status.S3Path == "" {
		t.Fatalf("unexpected status: %+v", b.Status)
	}
	var job batchv1.Job
	if err := h.c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: b.Status.ActiveJob}, &job); err != nil {
		t.Fatal(err)
	}
	if len(job.OwnerReferences) != 1 || job.OwnerReferences[0].Name != "nightly" {
		t.Errorf("job must be owned by the backup: %+v", job.OwnerReferences)
	}
	c := job.Spec.Template.Spec.Containers[0]
	if c.Image != "runner:test" || c.Args[0] != "backup" {
		t.Errorf("unexpected container: %+v", c)
	}
	wantKey := "default/nightly/20261015T023000Z.sql.gz"
	if b.Status.S3Path != wantKey {
		t.Errorf("S3Path = %q, want %q", b.Status.S3Path, wantKey)
	}
}

func TestBackupRunningToSucceededPrunesOldObjects(t *testing.T) {
	h := newHarness(t, backupObj("0 2 * * *"))
	h.now = baseTime.Add(90 * time.Minute)
	h.reconcile(t)
	b := h.backup(t)

	old := h.now.Add(-10 * 24 * time.Hour)
	h.store.objs = []storage.Object{
		{Key: "default/nightly/old.sql.gz", Size: 1, LastModified: old},
		{Key: "default/nightly/recent.sql.gz", Size: 2, LastModified: h.now.Add(-24 * time.Hour)},
		{Key: b.Status.S3Path, Size: 4242, LastModified: h.now},
	}
	h.finishJob(t, b.Status.ActiveJob, batchv1.JobComplete, "")
	before := testutil.ToFloat64(metrics.Total.WithLabelValues(ns, "nightly", metrics.StatusSuccess))
	h.reconcile(t)

	b = h.backup(t)
	if b.Status.Phase != v1alpha1.PhaseSucceeded {
		t.Fatalf("phase = %q, want Succeeded (msg %q)", b.Status.Phase, b.Status.Message)
	}
	if b.Status.BackupSize != 4242 || b.Status.LastBackupTime == nil || b.Status.ActiveJob != "" {
		t.Errorf("unexpected status: %+v", b.Status)
	}
	if len(h.store.deleted) != 1 || h.store.deleted[0] != "default/nightly/old.sql.gz" {
		t.Errorf("deleted = %v, want only old.sql.gz", h.store.deleted)
	}
	after := testutil.ToFloat64(metrics.Total.WithLabelValues(ns, "nightly", metrics.StatusSuccess))
	if after != before+1 {
		t.Errorf("pgbackup_total{success} = %v, want %v", after, before+1)
	}
	if got := testutil.ToFloat64(metrics.LastTimestamp.WithLabelValues(ns, "nightly")); got != float64(h.now.Unix()) {
		t.Errorf("last timestamp gauge = %v", got)
	}

	// Next reconcile must wait for the following tick instead of relaunching.
	res := h.reconcile(t)
	if res.RequeueAfter <= 0 || h.backup(t).Status.Phase != v1alpha1.PhaseSucceeded {
		t.Errorf("expected wait for next tick, got %+v", res)
	}
}

func TestBackupRunningToFailed(t *testing.T) {
	h := newHarness(t, backupObj("0 2 * * *"))
	h.now = baseTime.Add(90 * time.Minute)
	h.reconcile(t)
	b := h.backup(t)
	h.finishJob(t, b.Status.ActiveJob, batchv1.JobFailed, "BackoffLimitExceeded")
	before := testutil.ToFloat64(metrics.Total.WithLabelValues(ns, "nightly", metrics.StatusFailed))
	h.reconcile(t)

	b = h.backup(t)
	if b.Status.Phase != v1alpha1.PhaseFailed || b.Status.Message != "BackoffLimitExceeded" {
		t.Errorf("unexpected status: %+v", b.Status)
	}
	if got := testutil.ToFloat64(metrics.Total.WithLabelValues(ns, "nightly", metrics.StatusFailed)); got != before+1 {
		t.Errorf("failed counter = %v, want %v", got, before+1)
	}
}

func TestBackupRunningJobStillActive(t *testing.T) {
	h := newHarness(t, backupObj("0 2 * * *"))
	h.now = baseTime.Add(90 * time.Minute)
	h.reconcile(t)
	res := h.reconcile(t)
	if res.RequeueAfter != runningPollInterval || h.backup(t).Status.Phase != v1alpha1.PhaseRunning {
		t.Errorf("expected to keep polling a running job, got %+v", res)
	}
}

func TestBackupRunningJobMissing(t *testing.T) {
	h := newHarness(t, backupObj("0 2 * * *"))
	h.now = baseTime.Add(90 * time.Minute)
	h.reconcile(t)
	b := h.backup(t)
	if err := h.c.Delete(context.Background(), &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: b.Status.ActiveJob, Namespace: ns}}); err != nil {
		t.Fatal(err)
	}
	h.reconcile(t)
	if got := h.backup(t).Status.Phase; got != v1alpha1.PhaseFailed {
		t.Errorf("phase = %q, want Failed", got)
	}
}

func TestBackupNotFoundIsIgnored(t *testing.T) {
	h := newHarness(t, backupObj("0 2 * * *"))
	_, err := h.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "ghost"}})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestExpiredKeys(t *testing.T) {
	now := baseTime
	objs := []storage.Object{
		{Key: "a", LastModified: now.Add(-8 * 24 * time.Hour)},
		{Key: "b", LastModified: now.Add(-6 * 24 * time.Hour)},
		{Key: "keep", LastModified: now.Add(-30 * 24 * time.Hour)},
		{Key: "dir/", LastModified: now.Add(-30 * 24 * time.Hour)},
		{Key: "zero"},
	}
	got := ExpiredKeys(objs, "keep", now, 7)
	if len(got) != 1 || got[0] != "a" {
		t.Errorf("ExpiredKeys = %v, want [a]", got)
	}
	if ExpiredKeys(objs, "", now, 0) != nil {
		t.Error("retention 0 must disable pruning")
	}
}

func TestStorageConfigErrors(t *testing.T) {
	c := newClient(t, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "bad", Namespace: ns}})
	spec := v1alpha1.StorageSpec{Bucket: "b", CredentialSecretRef: corev1.LocalObjectReference{Name: "bad"}}
	if _, err := storageConfig(context.Background(), c, ns, spec); err == nil {
		t.Error("expected error for secret missing keys")
	}
	spec.CredentialSecretRef.Name = "missing"
	if _, err := storageConfig(context.Background(), c, ns, spec); err == nil {
		t.Error("expected error for missing secret")
	}
}
