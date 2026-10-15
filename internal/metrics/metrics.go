// Package metrics exposes the operator's Prometheus instruments.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// Duration observes backup job duration in seconds.
	Duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pgbackup_duration_seconds",
		Help:    "Duration of PostgreSQL backup jobs in seconds.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 14),
	}, []string{"namespace", "name"})

	// Total counts finished backups partitioned by status (success|failed).
	Total = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pgbackup_total",
		Help: "Total number of finished backups partitioned by status.",
	}, []string{"namespace", "name", "status"})

	// LastTimestamp is the unix time of the last successful backup.
	LastTimestamp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pgbackup_last_timestamp_seconds",
		Help: "Unix timestamp of the last successful backup.",
	}, []string{"namespace", "name"})
)

// Status label values.
const (
	StatusSuccess = "success"
	StatusFailed  = "failed"
)

func init() {
	ctrlmetrics.Registry.MustRegister(Duration, Total, LastTimestamp)
}

// RecordSuccess records a successful backup.
func RecordSuccess(ns, name string, d time.Duration, finished time.Time) {
	Duration.WithLabelValues(ns, name).Observe(d.Seconds())
	Total.WithLabelValues(ns, name, StatusSuccess).Inc()
	LastTimestamp.WithLabelValues(ns, name).Set(float64(finished.Unix()))
}

// RecordFailure records a failed backup.
func RecordFailure(ns, name string, d time.Duration) {
	Duration.WithLabelValues(ns, name).Observe(d.Seconds())
	Total.WithLabelValues(ns, name, StatusFailed).Inc()
}

