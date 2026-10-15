package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PostgresRestoreSpec defines the desired state of PostgresRestore.
// Exactly one of BackupName or S3Path must be set.
type PostgresRestoreSpec struct {
	// BackupName references a PostgresBackup in the same namespace; its latest successful backup is restored.
	// +optional
	BackupName string `json:"backupName,omitempty"`
	// S3Path is an explicit object key to restore. Requires Storage.
	// +optional
	S3Path string `json:"s3Path,omitempty"`
	// Storage is required when S3Path is used without BackupName.
	// +optional
	Storage *StorageSpec `json:"storage,omitempty"`
	// TargetDatabaseURISecretRef references a Secret (host, user, password, dbname, optional port) for the restore target.
	TargetDatabaseURISecretRef corev1.LocalObjectReference `json:"targetDatabaseUriSecretRef"`
	// PITRTimestamp, when set together with BackupName, selects the newest backup taken at or before this time.
	// +optional
	PITRTimestamp *metav1.Time `json:"pitrTimestamp,omitempty"`
}

// PostgresRestoreStatus defines the observed state of PostgresRestore.
type PostgresRestoreStatus struct {
	// Phase is the current lifecycle phase.
	// +optional
	Phase Phase `json:"phase,omitempty"`
	// CompletionTime is when the restore finished (successfully or not).
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`
	// Error holds the failure reason when Phase is Failed.
	// +optional
	Error string `json:"error,omitempty"`
	// ResolvedS3Path is the object key chosen for the restore.
	// +optional
	ResolvedS3Path string `json:"resolvedS3Path,omitempty"`
	// ActiveJob is the restore Job name.
	// +optional
	ActiveJob string `json:"activeJob,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=pgr
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Completed",type=date,JSONPath=`.status.completionTime`

// PostgresRestore restores a backup from S3 into a target database.
type PostgresRestore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PostgresRestoreSpec   `json:"spec,omitempty"`
	Status PostgresRestoreStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PostgresRestoreList contains a list of PostgresRestore.
type PostgresRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PostgresRestore `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PostgresRestore{}, &PostgresRestoreList{})
}

