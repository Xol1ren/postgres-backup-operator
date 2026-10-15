// Command manager runs the postgres-backup-operator controllers.
package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	v1alpha1 "github.com/Xol1ren/postgres-backup-operator/api/v1alpha1"
	"github.com/Xol1ren/postgres-backup-operator/controllers"
	"github.com/Xol1ren/postgres-backup-operator/internal/storage"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
}

func main() {
	var (
		metricsAddr string
		probeAddr   string
		leaderElect bool
		runnerImage string
	)
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "Address the Prometheus metrics endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "Address the health probe endpoint binds to.")
	flag.BoolVar(&leaderElect, "leader-elect", false, "Enable leader election for HA deployments.")
	flag.StringVar(&runnerImage, "runner-image", controllers.DefaultRunnerImage, "Image used for backup/restore Jobs (needs pg_dump, psql and the runner binary).")
	opts := zap.Options{Development: false}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog := ctrl.Log.WithName("setup")

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         leaderElect,
		LeaderElectionID:       "postgres-backup-operator.backup.xol1ren.io",
	})
	if err != nil {
		setupLog.Error(err, "unable to create manager")
		os.Exit(1)
	}

	if err := (&controllers.PostgresBackupReconciler{
		Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), Scheme: mgr.GetScheme(),
		RunnerImage: runnerImage, StoreFactory: storage.NewS3,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create PostgresBackup controller")
		os.Exit(1)
	}
	if err := (&controllers.PostgresRestoreReconciler{
		Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), Scheme: mgr.GetScheme(),
		RunnerImage: runnerImage, StoreFactory: storage.NewS3,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create PostgresRestore controller")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "manager exited with error")
		os.Exit(1)
	}
}

