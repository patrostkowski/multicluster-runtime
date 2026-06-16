/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"errors"
	"flag"
	"os"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	ctrl "sigs.k8s.io/controller-runtime"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mcbuilder "sigs.k8s.io/multicluster-runtime/pkg/builder"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"
	kamajiprovider "sigs.k8s.io/multicluster-runtime/providers/kamaji"
)

func main() {
	var namespace string
	var kubeconfigSecretKey string

	flag.StringVar(&namespace, "namespace", "", "Namespace to watch for TenantControlPlanes (empty for all namespaces)")
	flag.StringVar(&kubeconfigSecretKey, "kubeconfig-key", "super-admin.conf",
		"Key in the admin kubeconfig Secret to use")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrllog.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	entryLog := ctrllog.Log.WithName("entrypoint")
	ctx := ctrl.SetupSignalHandler()

	entryLog.Info("Starting application", "namespace", namespace, "kubeconfigSecretKey", kubeconfigSecretKey)

	// Create the Kamaji provider.
	provider := kamajiprovider.New(kamajiprovider.Options{
		Namespace:           namespace,
		KubeconfigSecretKey: kubeconfigSecretKey,
	})

	// Setup a cluster-aware Manager with the Kamaji provider.
	managerOpts := mcmanager.Options{
		Metrics: metricsserver.Options{
			BindAddress: "0",
		},
	}

	entryLog.Info("Creating manager")
	mgr, err := mcmanager.New(ctrl.GetConfigOrDie(), provider, managerOpts)
	if err != nil {
		entryLog.Error(err, "Unable to create manager")
		os.Exit(1)
	}

	// Setup the provider controller with the manager.
	if err := provider.SetupWithManager(ctx, mgr); err != nil {
		entryLog.Error(err, "Unable to setup provider with manager")
		os.Exit(1)
	}

	// Create a controller that watches ConfigMaps across all Kamaji tenant clusters.
	err = mcbuilder.ControllerManagedBy(mgr).
		Named("kamaji-multicluster-configmaps").
		For(&corev1.ConfigMap{}).
		Complete(mcreconcile.Func(
			func(ctx context.Context, req mcreconcile.Request) (ctrl.Result, error) {
				log := ctrllog.FromContext(ctx).WithValues("cluster", req.ClusterName)
				log.Info("Reconciling ConfigMap")

				cl, err := mgr.GetCluster(ctx, req.ClusterName)
				if err != nil {
					return reconcile.Result{}, err
				}

				cm := &corev1.ConfigMap{}
				if err := cl.GetClient().Get(ctx, req.Request.NamespacedName, cm); err != nil {
					if apierrors.IsNotFound(err) {
						return reconcile.Result{}, nil
					}
					return reconcile.Result{}, err
				}

				log.Info("ConfigMap found", "namespace", cm.Namespace, "name", cm.Name)
				return ctrl.Result{}, nil
			},
		))
	if err != nil {
		entryLog.Error(err, "unable to create controller")
		os.Exit(1)
	}

	// Start the manager.
	err = mgr.Start(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		entryLog.Error(err, "unable to start")
		os.Exit(1)
	}
}
