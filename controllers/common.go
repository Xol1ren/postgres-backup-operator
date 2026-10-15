// Package controllers contains the PostgresBackup and PostgresRestore reconcilers.
package controllers

import (
	"context"
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/Xol1ren/postgres-backup-operator/api/v1alpha1"
	"github.com/Xol1ren/postgres-backup-operator/internal/storage"
)

const (
	// DefaultRunnerImage is the image containing pg_dump, psql and the runner binary.
	DefaultRunnerImage = "ghcr.io/xol1ren/postgres-backup-runner:latest"

	labelManagedBy = "app.kubernetes.io/managed-by"
	labelOwner     = "backup.xol1ren.io/owner"
	managedByValue = "postgres-backup-operator"

	secretKeyAccess = "accessKeyId"
	secretKeySecret = "secretAccessKey"
)

// storageConfig resolves a StorageSpec (including credentials) into a storage.Config.
// reader should be an uncached reader so the operator only needs `get` on Secrets.
func storageConfig(ctx context.Context, reader client.Reader, ns string, spec v1alpha1.StorageSpec) (storage.Config, error) {
	var sec corev1.Secret
	if err := reader.Get(ctx, types.NamespacedName{Namespace: ns, Name: spec.CredentialSecretRef.Name}, &sec); err != nil {
		return storage.Config{}, fmt.Errorf("reading credential secret %q: %w", spec.CredentialSecretRef.Name, err)
	}
	ak, sk := string(sec.Data[secretKeyAccess]), string(sec.Data[secretKeySecret])
	if ak == "" || sk == "" {
		return storage.Config{}, fmt.Errorf("secret %q must contain keys %s and %s", sec.Name, secretKeyAccess, secretKeySecret)
	}
	return storage.Config{
		Endpoint:       spec.Endpoint,
		Bucket:         spec.Bucket,
		Region:         spec.Region,
		AccessKey:      ak,
		SecretKey:      sk,
		ForcePathStyle: spec.ForcePathStyle,
	}, nil
}

// backupPrefix is the object-key prefix owned by a PostgresBackup.
func backupPrefix(b *v1alpha1.PostgresBackup) string {
	p := strings.Trim(b.Spec.Storage.Prefix, "/")
	if p != "" {
		p += "/"
	}
	return fmt.Sprintf("%s%s/%s/", p, b.Namespace, b.Name)
}

func secretEnv(name, secretName, key string, optional bool) corev1.EnvVar {
	return corev1.EnvVar{Name: name, ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
		Key:                  key,
		Optional:             ptr.To(optional),
	}}}
}

// jobSpec describes a runner Job.
type jobSpec struct {
	name, namespace, image, mode, owner string
	dbSecret                            string
	storage                             v1alpha1.StorageSpec
	key                                 string
	compression                         string
}

func buildJob(s jobSpec) *batchv1.Job {
	env := []corev1.EnvVar{
		secretEnv("PG_HOST", s.dbSecret, "host", false),
		secretEnv("PG_PORT", s.dbSecret, "port", true),
		secretEnv("PG_USER", s.dbSecret, "user", false),
		secretEnv("PGPASSWORD", s.dbSecret, "password", false),
		secretEnv("PG_DBNAME", s.dbSecret, "dbname", false),
		secretEnv("S3_ACCESS_KEY", s.storage.CredentialSecretRef.Name, secretKeyAccess, false),
		secretEnv("S3_SECRET_KEY", s.storage.CredentialSecretRef.Name, secretKeySecret, false),
		{Name: "S3_ENDPOINT", Value: s.storage.Endpoint},
		{Name: "S3_BUCKET", Value: s.storage.Bucket},
		{Name: "S3_REGION", Value: s.storage.Region},
		{Name: "S3_FORCE_PATH_STYLE", Value: fmt.Sprintf("%t", s.storage.ForcePathStyle)},
		{Name: "S3_KEY", Value: s.key},
		{Name: "COMPRESSION", Value: s.compression},
	}
	labels := map[string]string{labelManagedBy: managedByValue, labelOwner: s.owner}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: s.name, Namespace: s.namespace, Labels: labels},
		Spec: batchv1.JobSpec{
			BackoffLimit:            ptr.To[int32](2),
			ActiveDeadlineSeconds:   ptr.To[int64](6 * 60 * 60),
			TTLSecondsAfterFinished: ptr.To[int32](24 * 60 * 60),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot:   ptr.To(true),
						RunAsUser:      ptr.To[int64](70),
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Containers: []corev1.Container{{
						Name:  "runner",
						Image: s.image,
						Args:  []string{s.mode},
						Env:   env,
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: ptr.To(false),
							ReadOnlyRootFilesystem:   ptr.To(true),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("100m"),
								corev1.ResourceMemory: resource.MustParse("128Mi"),
							},
							Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("512Mi")},
						},
					}},
				},
			},
		},
	}
}

// jobOutcome inspects a Job and reports whether it finished.
func jobOutcome(job *batchv1.Job) (done, success bool, message string) {
	for _, c := range job.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}
		switch c.Type {
		case batchv1.JobComplete:
			return true, true, ""
		case batchv1.JobFailed:
			msg := c.Message
			if msg == "" {
				msg = c.Reason
			}
			return true, false, msg
		}
	}
	return false, false, ""
}

func truncName(name string, max int) string {
	if len(name) > max {
		return strings.TrimRight(name[:max], "-.")
	}
	return name
}

