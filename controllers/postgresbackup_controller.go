package controllers

import (
	"context"
	"fmt"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/Xol1ren/postgres-backup-operator/api/v1alpha1"
	"github.com/Xol1ren/postgres-backup-operator/internal/metrics"
	"github.com/Xol1ren/postgres-backup-operator/internal/runner"
	"github.com/Xol1ren/postgres-backup-operator/internal/schedule"
	"github.com/Xol1ren/postgres-backup-operator/internal/storage"
)

const runningPollInterval = 15 * time.Second

// PostgresBackupReconciler drives the PostgresBackup state machine:
//
//	Pending --(schedule due)--> Running --(Job complete)--> Succeeded
//	                                     \--(Job failed)--> Failed
//	Succeeded|Failed --(schedule due)--> Running
type PostgresBackupReconciler struct {
	client.Client
	// APIReader is an uncached reader used for Secrets (least-privilege: only `get`).
	APIReader    client.Reader
	Scheme       *runtime.Scheme
	RunnerImage  string
	StoreFactory storage.Factory
	// Now is injectable for tests; defaults to time.Now.
	Now func() time.Time
}

// +kubebuilder:rbac:groups=backup.xol1ren.io,resources=postgresbackups,verbs=get;list;watch
// +kubebuilder:rbac:groups=backup.xol1ren.io,resources=postgresbackups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get

func (r *PostgresBackupReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *PostgresBackupReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// Reconcile implements reconcile.Reconciler. controller-runtime guarantees a single worker per object key,
// so the state machine below never races with itself for the same PostgresBackup.
func (r *PostgresBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var b v1alpha1.PostgresBackup
	if err := r.Get(ctx, req.NamespacedName, &b); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !b.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil // owned Jobs are garbage-collected
	}

	sched, err := schedule.Parse(b.Spec.Schedule)
	if err != nil {
		return ctrl.Result{}, r.setTerminal(ctx, &b, v1alpha1.PhaseFailed, "InvalidSchedule", err.Error())
	}

	switch b.Status.Phase {
	case "":
		b.Status.Phase = v1alpha1.PhasePending
		b.Status.Message = "waiting for first scheduled run"
		if err := r.Status().Update(ctx, &b); err != nil {
			return ctrl.Result{}, err
		}
	case v1alpha1.PhaseRunning:
		return r.reconcileRunning(ctx, &b)
	}

	// Pending, Succeeded or Failed: wait for the next tick.
	base := b.CreationTimestamp.Time.UTC() // cron schedules are always evaluated in UTC
	if b.Status.LastScheduleTime != nil {
		base = b.Status.LastScheduleTime.Time.UTC()
	}
	next := sched.Next(base)
	now := r.now()
	if now.Before(next) {
		nt := metav1.NewTime(next)
		if b.Status.NextBackupTime == nil || !b.Status.NextBackupTime.Equal(&nt) {
			b.Status.NextBackupTime = &nt
			if err := r.Status().Update(ctx, &b); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: next.Sub(now)}, nil
	}

	logger.Info("schedule due, launching backup job", "scheduled", next)
	return r.startJob(ctx, &b, sched.Next(now.UTC()))
}

func (r *PostgresBackupReconciler) startJob(ctx context.Context, b *v1alpha1.PostgresBackup, nextAfter time.Time) (ctrl.Result, error) {
	ts := r.now().UTC()
	comp := string(b.Spec.Compression)
	if comp == "" {
		comp = string(v1alpha1.CompressionGzip)
	}
	key := backupPrefix(b) + ts.Format("20060102T150405Z") + runner.Extension(comp)
	image := r.RunnerImage
	if image == "" {
		image = DefaultRunnerImage
	}
	job := buildJob(jobSpec{
		name:        fmt.Sprintf("%s-%d", truncName(b.Name, 45), ts.Unix()),
		namespace:   b.Namespace,
		image:       image,
		mode:        "backup",
		owner:       b.Name,
		dbSecret:    b.Spec.DatabaseURISecretRef.Name,
		storage:     b.Spec.Storage,
		key:         key,
		compression: comp,
	})
	if err := controllerutil.SetControllerReference(b, job, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.Create(ctx, job); err != nil && !apierrors.IsAlreadyExists(err) {
		return ctrl.Result{}, fmt.Errorf("creating backup job: %w", err)
	}

	t := metav1.NewTime(ts)
	nt := metav1.NewTime(nextAfter)
	b.Status.Phase = v1alpha1.PhaseRunning
	b.Status.ActiveJob = job.Name
	b.Status.S3Path = key
	b.Status.LastScheduleTime = &t
	b.Status.NextBackupTime = &nt
	b.Status.Message = "backup job running"
	meta.SetStatusCondition(&b.Status.Conditions, metav1.Condition{
		Type: v1alpha1.ConditionReady, Status: metav1.ConditionFalse, Reason: "BackupRunning",
		Message: "backup job " + job.Name + " is running", ObservedGeneration: b.Generation,
	})
	if err := r.Status().Update(ctx, b); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: runningPollInterval}, nil
}

func (r *PostgresBackupReconciler) reconcileRunning(ctx context.Context, b *v1alpha1.PostgresBackup) (ctrl.Result, error) {
	var job batchv1.Job
	err := r.Get(ctx, types.NamespacedName{Namespace: b.Namespace, Name: b.Status.ActiveJob}, &job)
	if apierrors.IsNotFound(err) {
		metrics.RecordFailure(b.Namespace, b.Name, 0)
		return ctrl.Result{}, r.setTerminal(ctx, b, v1alpha1.PhaseFailed, "JobMissing", "backup job disappeared")
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	done, success, msg := jobOutcome(&job)
	if !done {
		return ctrl.Result{RequeueAfter: runningPollInterval}, nil
	}

	started := b.CreationTimestamp.Time
	if job.Status.StartTime != nil {
		started = job.Status.StartTime.Time
	} else if b.Status.LastScheduleTime != nil {
		started = b.Status.LastScheduleTime.Time
	}
	finished := r.now()
	if job.Status.CompletionTime != nil {
		finished = job.Status.CompletionTime.Time
	}
	dur := max(finished.Sub(started), 0)

	if !success {
		metrics.RecordFailure(b.Namespace, b.Name, dur)
		if err := r.setTerminal(ctx, b, v1alpha1.PhaseFailed, "BackupFailed", msg); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	metrics.RecordSuccess(b.Namespace, b.Name, dur, finished)
	retentionCond := r.applyRetention(ctx, b)

	ft := metav1.NewTime(finished)
	b.Status.Phase = v1alpha1.PhaseSucceeded
	b.Status.LastBackupTime = &ft
	b.Status.ActiveJob = ""
	b.Status.Message = "backup completed"
	meta.SetStatusCondition(&b.Status.Conditions, metav1.Condition{
		Type: v1alpha1.ConditionReady, Status: metav1.ConditionTrue, Reason: "BackupSucceeded",
		Message: "last backup succeeded", ObservedGeneration: b.Generation,
	})
	meta.SetStatusCondition(&b.Status.Conditions, retentionCond)
	if err := r.Status().Update(ctx, b); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil // re-enter to compute the next tick
}

// applyRetention records the size of the new backup and prunes objects older than retentionDays.
// Failures are reported through a condition but never fail the backup itself.
func (r *PostgresBackupReconciler) applyRetention(ctx context.Context, b *v1alpha1.PostgresBackup) metav1.Condition {
	cond := metav1.Condition{Type: v1alpha1.ConditionRetention, ObservedGeneration: b.Generation}
	fail := func(reason string, err error) metav1.Condition {
		cond.Status, cond.Reason, cond.Message = metav1.ConditionFalse, reason, err.Error()
		return cond
	}
	cfg, err := storageConfig(ctx, r.reader(), b.Namespace, b.Spec.Storage)
	if err != nil {
		return fail("CredentialsError", err)
	}
	factory := r.StoreFactory
	if factory == nil {
		factory = storage.NewS3
	}
	store, err := factory(ctx, cfg)
	if err != nil {
		return fail("StoreError", err)
	}
	objs, err := store.List(ctx, backupPrefix(b))
	if err != nil {
		return fail("ListError", err)
	}
	for _, o := range objs {
		if o.Key == b.Status.S3Path {
			b.Status.BackupSize = o.Size
		}
	}
	doomed := ExpiredKeys(objs, b.Status.S3Path, r.now(), int(b.Spec.RetentionDays))
	if len(doomed) > 0 {
		if err := store.Delete(ctx, doomed); err != nil {
			return fail("DeleteError", err)
		}
	}
	cond.Status, cond.Reason = metav1.ConditionTrue, "Pruned"
	cond.Message = fmt.Sprintf("pruned %d expired backup(s)", len(doomed))
	return cond
}

// ExpiredKeys returns keys older than retentionDays, never including `keep` (the newest backup).
// A non-positive retention disables pruning.
func ExpiredKeys(objs []storage.Object, keep string, now time.Time, retentionDays int) []string {
	if retentionDays <= 0 {
		return nil
	}
	cutoff := now.Add(-time.Duration(retentionDays) * 24 * time.Hour)
	var out []string
	for _, o := range objs {
		if o.Key == keep || strings.HasSuffix(o.Key, "/") {
			continue
		}
		if !o.LastModified.IsZero() && o.LastModified.Before(cutoff) {
			out = append(out, o.Key)
		}
	}
	return out
}

func (r *PostgresBackupReconciler) setTerminal(ctx context.Context, b *v1alpha1.PostgresBackup, phase v1alpha1.Phase, reason, msg string) error {
	b.Status.Phase = phase
	b.Status.ActiveJob = ""
	b.Status.Message = msg
	meta.SetStatusCondition(&b.Status.Conditions, metav1.Condition{
		Type: v1alpha1.ConditionReady, Status: metav1.ConditionFalse, Reason: reason,
		Message: msg, ObservedGeneration: b.Generation,
	})
	return r.Status().Update(ctx, b)
}

// SetupWithManager registers the controller; Jobs owned by a PostgresBackup trigger reconciles.
func (r *PostgresBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.PostgresBackup{}).
		Owns(&batchv1.Job{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 4}).
		Complete(r)
}
