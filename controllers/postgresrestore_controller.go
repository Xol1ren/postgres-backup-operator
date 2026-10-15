package controllers

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1alpha1 "github.com/Xol1ren/postgres-backup-operator/api/v1alpha1"
	"github.com/Xol1ren/postgres-backup-operator/internal/storage"
)

// PostgresRestoreReconciler drives: Pending -> Running -> Succeeded|Failed.
// A restore runs exactly once; terminal phases are never re-entered.
type PostgresRestoreReconciler struct {
	client.Client
	APIReader    client.Reader
	Scheme       *runtime.Scheme
	RunnerImage  string
	StoreFactory storage.Factory
	Now          func() time.Time
}

// +kubebuilder:rbac:groups=backup.xol1ren.io,resources=postgresrestores,verbs=get;list;watch
// +kubebuilder:rbac:groups=backup.xol1ren.io,resources=postgresrestores/status,verbs=get;update;patch

func (r *PostgresRestoreReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *PostgresRestoreReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// Reconcile implements reconcile.Reconciler.
func (r *PostgresRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var rs v1alpha1.PostgresRestore
	if err := r.Get(ctx, req.NamespacedName, &rs); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !rs.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	switch rs.Status.Phase {
	case v1alpha1.PhaseSucceeded, v1alpha1.PhaseFailed:
		return ctrl.Result{}, nil
	case v1alpha1.PhaseRunning:
		return r.reconcileRunning(ctx, &rs)
	default:
		return r.start(ctx, &rs)
	}
}

func (r *PostgresRestoreReconciler) fail(ctx context.Context, rs *v1alpha1.PostgresRestore, msg string) error {
	t := metav1.NewTime(r.now())
	rs.Status.Phase = v1alpha1.PhaseFailed
	rs.Status.Error = msg
	rs.Status.CompletionTime = &t
	return r.Status().Update(ctx, rs)
}

// start validates the spec, resolves the source object and launches the restore Job.
func (r *PostgresRestoreReconciler) start(ctx context.Context, rs *v1alpha1.PostgresRestore) (ctrl.Result, error) {
	if rs.Status.Phase == "" {
		rs.Status.Phase = v1alpha1.PhasePending
		if err := r.Status().Update(ctx, rs); err != nil {
			return ctrl.Result{}, err
		}
	}

	key, st, comp, err := r.resolveSource(ctx, rs)
	if err != nil {
		var terr *terminalError
		if errors.As(err, &terr) {
			return ctrl.Result{}, r.fail(ctx, rs, err.Error())
		}
		return ctrl.Result{}, err // transient: retry with backoff
	}

	image := r.RunnerImage
	if image == "" {
		image = DefaultRunnerImage
	}
	job := buildJob(jobSpec{
		name:        truncName(rs.Name, 50) + "-restore",
		namespace:   rs.Namespace,
		image:       image,
		mode:        "restore",
		owner:       rs.Name,
		dbSecret:    rs.Spec.TargetDatabaseURISecretRef.Name,
		storage:     st,
		key:         key,
		compression: comp,
	})
	job.Spec.BackoffLimit = ptrInt32(0) // restores are not idempotent: never retry automatically
	if err := controllerutil.SetControllerReference(rs, job, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.Create(ctx, job); err != nil && !apierrors.IsAlreadyExists(err) {
		return ctrl.Result{}, fmt.Errorf("creating restore job: %w", err)
	}
	rs.Status.Phase = v1alpha1.PhaseRunning
	rs.Status.ActiveJob = job.Name
	rs.Status.ResolvedS3Path = key
	if err := r.Status().Update(ctx, rs); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: runningPollInterval}, nil
}

type terminalError struct{ msg string }

func (e *terminalError) Error() string { return e.msg }

func terminalf(format string, a ...any) error { return &terminalError{msg: fmt.Sprintf(format, a...)} }

// resolveSource determines (object key, storage spec, compression) for the restore.
func (r *PostgresRestoreReconciler) resolveSource(ctx context.Context, rs *v1alpha1.PostgresRestore) (string, v1alpha1.StorageSpec, string, error) {
	spec := rs.Spec
	if (spec.BackupName == "") == (spec.S3Path == "") {
		return "", v1alpha1.StorageSpec{}, "", terminalf("exactly one of backupName or s3Path must be set")
	}

	var (
		key string
		st  v1alpha1.StorageSpec
		bk  v1alpha1.PostgresBackup
	)
	if spec.BackupName != "" {
		err := r.Get(ctx, types.NamespacedName{Namespace: rs.Namespace, Name: spec.BackupName}, &bk)
		if apierrors.IsNotFound(err) {
			return "", st, "", terminalf("PostgresBackup %q not found", spec.BackupName)
		}
		if err != nil {
			return "", st, "", err
		}
		st = bk.Spec.Storage
		if spec.PITRTimestamp != nil {
			k, err := r.pickAtOrBefore(ctx, &bk, spec.PITRTimestamp.Time)
			if err != nil {
				return "", st, "", err
			}
			key = k
		} else {
			if bk.Status.LastBackupTime == nil || bk.Status.S3Path == "" {
				return "", st, "", terminalf("PostgresBackup %q has no successful backup yet", spec.BackupName)
			}
			key = bk.Status.S3Path
		}
	} else {
		if spec.Storage == nil {
			return "", st, "", terminalf("storage is required when s3Path is used")
		}
		key, st = spec.S3Path, *spec.Storage
	}

	comp := "gzip"
	if strings.HasSuffix(key, ".zst") {
		comp = "zstd"
	}
	return key, st, comp, nil
}

// pickAtOrBefore selects the newest backup object whose timestamp is <= t.
// This is snapshot-granularity recovery: WAL replay is outside this operator's scope.
func (r *PostgresRestoreReconciler) pickAtOrBefore(ctx context.Context, bk *v1alpha1.PostgresBackup, t time.Time) (string, error) {
	cfg, err := storageConfig(ctx, r.reader(), bk.Namespace, bk.Spec.Storage)
	if err != nil {
		return "", err
	}
	factory := r.StoreFactory
	if factory == nil {
		factory = storage.NewS3
	}
	store, err := factory(ctx, cfg)
	if err != nil {
		return "", err
	}
	objs, err := store.List(ctx, backupPrefix(bk))
	if err != nil {
		return "", err
	}
	return SelectAtOrBefore(objs, t)
}

// SelectAtOrBefore returns the key of the newest object with LastModified <= t.
func SelectAtOrBefore(objs []storage.Object, t time.Time) (string, error) {
	sorted := append([]storage.Object(nil), objs...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].LastModified.After(sorted[j].LastModified) })
	for _, o := range sorted {
		if !o.LastModified.After(t) {
			return o.Key, nil
		}
	}
	return "", terminalf("no backup found at or before %s", t.UTC().Format(time.RFC3339))
}

func (r *PostgresRestoreReconciler) reconcileRunning(ctx context.Context, rs *v1alpha1.PostgresRestore) (ctrl.Result, error) {
	var job batchv1.Job
	err := r.Get(ctx, types.NamespacedName{Namespace: rs.Namespace, Name: rs.Status.ActiveJob}, &job)
	if apierrors.IsNotFound(err) {
		return ctrl.Result{}, r.fail(ctx, rs, "restore job disappeared")
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	done, success, msg := jobOutcome(&job)
	if !done {
		return ctrl.Result{RequeueAfter: runningPollInterval}, nil
	}
	if !success {
		if msg == "" {
			msg = "restore job failed"
		}
		return ctrl.Result{}, r.fail(ctx, rs, msg)
	}
	t := metav1.NewTime(r.now())
	if job.Status.CompletionTime != nil {
		t = *job.Status.CompletionTime
	}
	rs.Status.Phase = v1alpha1.PhaseSucceeded
	rs.Status.Error = ""
	rs.Status.CompletionTime = &t
	return ctrl.Result{}, r.Status().Update(ctx, rs)
}

// SetupWithManager registers the controller.
func (r *PostgresRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.PostgresRestore{}).
		Owns(&batchv1.Job{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 2}).
		Complete(r)
}

func ptrInt32(v int32) *int32 { return &v }

