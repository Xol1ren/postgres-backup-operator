package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Phase describes the lifecycle stage of a backup or restore.
// +kubebuilder:validation:Enum=Pending;Running;Succeeded;Failed
type Phase string

// Lifecycle phases shared by PostgresBackup and PostgresRestore.
const (
	PhasePending   Phase = "Pending"
	PhaseRunning   Phase = "Running"
	PhaseSucceeded Phase = "Succeeded"
	PhaseFailed    Phase = "Failed"
)

// Compression selects the stream compression applied to the dump.
// +kubebuilder:validation:Enum=gzip;zstd
type Compression string

// Supported compression algorithms.
const (
	CompressionGzip Compression = "gzip"
	CompressionZstd Compression = "zstd"
)

// Condition types reported on PostgresBackup.
const (
	ConditionReady     = "Ready"
	ConditionRetention = "RetentionApplied"
)

// StorageSpec describes the S3-compatible destination.
type StorageSpec struct {
	// Endpoint is the S3-compatible endpoint URL (e.g. http://minio:9000). Empty means AWS S3.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`
	// Bucket is the destination bucket.
	// +kubebuilder:validation:MinLength=1
	Bucket string `json:"bucket"`
	// Region is the bucket region.
	// +kubebuilder:default=us-east-1
	Region string `json:"region,omitempty"`
	// Prefix is an optional key prefix inside the bucket.
	// +optional
	Prefix string `json:"prefix,omitempty"`
	// ForcePathStyle enables path-style addressing (required by most MinIO setups).
	// +optional
	ForcePathStyle bool `json:"forcePathStyle,omitempty"`
	// CredentialSecretRef references a Secret with keys accessKeyId and secretAccessKey.
	CredentialSecretRef corev1.LocalObjectReference `json:"credentialSecretRef"`
}

// PostgresBackupSpec defines the desired state of PostgresBackup.
type PostgresBackupSpec struct {
	// DatabaseURISecretRef references a Secret with keys host, user, password, dbname and optional port.
	DatabaseURISecretRef corev1.LocalObjectReference `json:"databaseUriSecretRef"`
	// Schedule is a standard 5-field cron expression (also accepts @hourly, @daily, ...).
	// +kubebuilder:validation:MinLength=1
	Schedule string `json:"schedule"`
	// Storage is the S3/MinIO destination.
	Storage StorageSpec `json:"storage"`
	// RetentionDays is the number of days backups are kept before being pruned.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=7
	RetentionDays int32 `json:"retentionDays,omitempty"`
	// Compression is the stream compression algorithm.
	// +kubebuilder:default=gzip
	Compression Compression `json:"compression,omitempty"`
}

// PostgresBackupStatus defines the observed state of PostgresBackup.
type PostgresBackupStatus struct {
	// Phase is the current lifecycle phase.
	// +optional
	Phase Phase `json:"phase,omitempty"`
	// LastBackupTime is when the last successful backup finished.
	// +optional
	LastBackupTime *metav1.Time `json:"lastBackupTime,omitempty"`
	// LastScheduleTime is when the most recent backup Job was launched.
	// +optional
	LastScheduleTime *metav1.Time `json:"lastScheduleTime,omitempty"`
	// NextBackupTime is the next scheduled run.
	// +optional
	NextBackupTime *metav1.Time `json:"nextBackupTime,omitempty"`
	// BackupSize is the size in bytes of the last successful backup object.
	// +optional
	BackupSize int64 `json:"backupSize,omitempty"`
	// S3Path is the object key of the latest (or in-flight) backup.
	// +optional
	S3Path string `json:"s3Path,omitempty"`
	// ActiveJob is the name of the running backup Job, if any.
	// +optional
	ActiveJob string `json:"activeJob,omitempty"`
	// Message carries a human-readable detail for the current phase.
	// +optional
	Message string `json:"message,omitempty"`
	// Conditions are standard Kubernetes conditions.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=pgb
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Schedule",type=string,JSONPath=`.spec.schedule`
// +kubebuilder:printcolumn:name="Last Backup",type=date,JSONPath=`.status.lastBackupTime`
// +kubebuilder:printcolumn:name="Size",type=integer,JSONPath=`.status.backupSize`

// PostgresBackup schedules and tracks PostgreSQL backups streamed to S3.
type PostgresBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PostgresBackupSpec   `json:"spec,omitempty"`
	Status PostgresBackupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PostgresBackupList contains a list of PostgresBackup.
type PostgresBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PostgresBackup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PostgresBackup{}, &PostgresBackupList{})
}

