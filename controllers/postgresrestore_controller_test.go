package controllers

import (
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/Xol1ren/postgres-backup-operator/api/v1alpha1"
	"github.com/Xol1ren/postgres-backup-operator/internal/storage"
)

type restoreHarness struct {
	c     client.Client
	r     *PostgresRestoreReconciler
	store *fakeStore
}

func newRestoreHarness(t *testing.T, rs *v1alpha1.PostgresRestore, extra ...client.Object) *restoreHarness {
	t.Helper()
	h := &restoreHarness{store: &fakeStore{}}
	h.c = newClient(t, append([]client.Object{rs, credSecret()}, extra...)...)
	h.r = &PostgresRestoreReconciler{
		Client: h.c, Scheme: newScheme(t), RunnerImage: "runner:test",
		StoreFactory: func(context.Context, storage.Config) (storage.ObjectStore, error) { return h.store, nil },
		Now:          func() time.Time { return baseTime },
	}
	return h
}

func (h *restoreHarness) reconcile(t *testing.T) {
	t.Helper()
	if _, err := h.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "restore"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
}

func (h *restoreHarness) get(t *testing.T) *v1alpha1.PostgresRestore {
	t.Helper()
	var rs v1alpha1.PostgresRestore
	if err := h.c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "restore"}, &rs); err != nil {
		t.Fatal(err)
	}
	return &rs
}

func restoreObj(spec v1alpha1.PostgresRestoreSpec) *v1alpha1.PostgresRestore {
	spec.TargetDatabaseURISecretRef = corev1.LocalObjectReference{Name: "target-db"}
	return &v1alpha1.PostgresRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "restore", Namespace: ns, UID: "uid-r"},
		Spec:       spec,
	}
}

func succeededBackup() *v1alpha1.PostgresBackup {
	b := backupObj("0 2 * * *")
	t := metav1.NewTime(baseTime)
	b.Status = v1alpha1.PostgresBackupStatus{
		Phase: v1alpha1.PhaseSucceeded, LastBackupTime: &t, S3Path: "default/nightly/latest.sql.zst",
	}
	return b
}

func (h *restoreHarness) finishJob(t *testing.T, cond batchv1.JobConditionType, msg string) {
	t.Helper()
	name := h.get(t).Status.ActiveJob
	var job batchv1.Job
	if err := h.c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, &job); err != nil {
		t.Fatal(err)
	}
	ct := metav1.NewTime(baseTime.Add(time.Minute))
	job.Status.CompletionTime = &ct
	job.Status.Conditions = []batchv1.JobCondition{{Type: cond, Status: corev1.ConditionTrue, Message: msg}}
	if err := h.c.Status().Update(context.Background(), &job); err != nil {
		t.Fatal(err)
	}
}

func TestRestoreFromBackupSucceeds(t *testing.T) {
	h := newRestoreHarness(t, restoreObj(v1alpha1.PostgresRestoreSpec{BackupName: "nightly"}), succeededBackup())
	h.reconcile(t)

	rs := h.get(t)
	if rs.Status.Phase != v1alpha1.PhaseRunning || rs.Status.ResolvedS3Path != "default/nightly/latest.sql.zst" {
		t.Fatalf("unexpected status after start: %+v", rs.Status)
	}
	var job batchv1.Job
	if err := h.c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: rs.Status.ActiveJob}, &job); err != nil {
		t.Fatal(err)
	}
	if *job.Spec.BackoffLimit != 0 {
		t.Error("restore jobs must not be retried")
	}
	env := map[string]string{}
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		env[e.Name] = e.Value
	}
	if env["COMPRESSION"] != "zstd" || env["S3_KEY"] != "default/nightly/latest.sql.zst" {
		t.Errorf("unexpected env: %v", env)
	}

	h.finishJob(t, batchv1.JobComplete, "")
	h.reconcile(t)
	rs = h.get(t)
	if rs.Status.Phase != v1alpha1.PhaseSucceeded || rs.Status.CompletionTime == nil {
		t.Errorf("unexpected final status: %+v", rs.Status)
	}
	// Terminal state is sticky.
	h.reconcile(t)
	if h.get(t).Status.Phase != v1alpha1.PhaseSucceeded {
		t.Error("terminal phase must not change")
	}
}

func TestRestoreJobFailure(t *testing.T) {
	h := newRestoreHarness(t, restoreObj(v1alpha1.PostgresRestoreSpec{BackupName: "nightly"}), succeededBackup())
	h.reconcile(t)
	h.finishJob(t, batchv1.JobFailed, "psql exited 3")
	h.reconcile(t)
	rs := h.get(t)
	if rs.Status.Phase != v1alpha1.PhaseFailed || rs.Status.Error != "psql exited 3" || rs.Status.CompletionTime == nil {
		t.Errorf("unexpected status: %+v", rs.Status)
	}
}

func TestRestoreValidationFailures(t *testing.T) {
	tests := []struct {
		name  string
		spec  v1alpha1.PostgresRestoreSpec
		extra []client.Object
	}{
		{"neither source", v1alpha1.PostgresRestoreSpec{}, nil},
		{"both sources", v1alpha1.PostgresRestoreSpec{BackupName: "x", S3Path: "y"}, nil},
		{"missing backup", v1alpha1.PostgresRestoreSpec{BackupName: "nope"}, nil},
		{"backup without success", v1alpha1.PostgresRestoreSpec{BackupName: "nightly"}, []client.Object{backupObj("0 2 * * *")}},
		{"s3path without storage", v1alpha1.PostgresRestoreSpec{S3Path: "k.sql.gz"}, nil},
		{"pitr without match", v1alpha1.PostgresRestoreSpec{BackupName: "nightly", PITRTimestamp: &metav1.Time{Time: baseTime}}, []client.Object{succeededBackup()}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newRestoreHarness(t, restoreObj(tc.spec), tc.extra...)
			h.reconcile(t)
			rs := h.get(t)
			if rs.Status.Phase != v1alpha1.PhaseFailed || rs.Status.Error == "" {
				t.Errorf("expected Failed with error, got %+v", rs.Status)
			}
		})
	}
}

func TestRestoreExplicitS3Path(t *testing.T) {
	spec := v1alpha1.PostgresRestoreSpec{
		S3Path:  "some/key.sql.gz",
		Storage: &v1alpha1.StorageSpec{Bucket: "b", CredentialSecretRef: corev1.LocalObjectReference{Name: "s3-creds"}},
	}
	h := newRestoreHarness(t, restoreObj(spec))
	h.reconcile(t)
	if rs := h.get(t); rs.Status.Phase != v1alpha1.PhaseRunning || rs.Status.ResolvedS3Path != "some/key.sql.gz" {
		t.Errorf("unexpected status: %+v", rs.Status)
	}
}

func TestRestorePITRSelectsNewestAtOrBefore(t *testing.T) {
	target := baseTime.Add(-36 * time.Hour)
	spec := v1alpha1.PostgresRestoreSpec{BackupName: "nightly", PITRTimestamp: &metav1.Time{Time: target}}
	h := newRestoreHarness(t, restoreObj(spec), succeededBackup())
	h.store.objs = []storage.Object{
		{Key: "k/day0.sql.gz", LastModified: baseTime},
		{Key: "k/day-1.sql.gz", LastModified: baseTime.Add(-24 * time.Hour)},
		{Key: "k/day-2.sql.gz", LastModified: baseTime.Add(-48 * time.Hour)},
	}
	h.reconcile(t)
	if rs := h.get(t); rs.Status.ResolvedS3Path != "k/day-2.sql.gz" || rs.Status.Phase != v1alpha1.PhaseRunning {
		t.Errorf("unexpected status: %+v", rs.Status)
	}
}

func TestSelectAtOrBefore(t *testing.T) {
	if _, err := SelectAtOrBefore(nil, baseTime); err == nil {
		t.Error("expected error for empty list")
	}
	k, err := SelectAtOrBefore([]storage.Object{{Key: "a", LastModified: baseTime}}, baseTime)
	if err != nil || k != "a" {
		t.Errorf("got %q, %v", k, err)
	}
}

func TestRestoreJobMissing(t *testing.T) {
	h := newRestoreHarness(t, restoreObj(v1alpha1.PostgresRestoreSpec{BackupName: "nightly"}), succeededBackup())
	h.reconcile(t)
	name := h.get(t).Status.ActiveJob
	if err := h.c.Delete(context.Background(), &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}}); err != nil {
		t.Fatal(err)
	}
	h.reconcile(t)
	if h.get(t).Status.Phase != v1alpha1.PhaseFailed {
		t.Error("expected Failed when job disappears")
	}
}
