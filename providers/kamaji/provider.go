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

package kamaji

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/go-logr/logr"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"
)

const (
	// DefaultKubeconfigSecretKey is the default key in the admin kubeconfig Secret
	// that contains the kubeconfig for accessing the tenant cluster.
	DefaultKubeconfigSecretKey = "super-admin.conf"

	// ControllerName is the name of the controller registered with the manager.
	ControllerName = "kamaji-provider"
)

var _ multicluster.Provider = &Provider{}

// New creates a new Kamaji Provider.
func New(opts Options) *Provider {
	if opts.KubeconfigSecretKey == "" {
		opts.KubeconfigSecretKey = DefaultKubeconfigSecretKey
	}
	if opts.ControllerName == "" {
		opts.ControllerName = ControllerName
	}

	return &Provider{
		opts:     opts,
		log:      log.Log.WithName("kamaji-provider"),
		clusters: map[multicluster.ClusterName]activeCluster{},
	}
}

// Options configures the Kamaji cluster provider.
type Options struct {
	// Namespace, when set, restricts the provider to TenantControlPlanes
	// in the given namespace. When unset, all namespaces are watched.
	Namespace string

	// KubeconfigSecretKey is the key in the admin kubeconfig Secret to use.
	// The annotation kamaji.clastix.io/kubeconfig-secret-key on the
	// TenantControlPlane can override this value.
	// Defaults to "super-admin.conf".
	KubeconfigSecretKey string

	// ClusterOptions are passed to cluster.New when creating clusters.
	ClusterOptions []cluster.Option

	// RESTOptions are applied to the REST config for each tenant cluster.
	RESTOptions []func(cfg *rest.Config) error

	// ControllerName overrides the name of the controller registered with the manager.
	ControllerName string

	// IsReady is an optional function that determines if a TenantControlPlane
	// is ready to be engaged. If not provided, defaults to checking that the
	// Kubernetes version status is "Ready", the admin kubeconfig secret name
	// is set, and the control plane endpoint is not empty.
	IsReady func(ctx context.Context, tcp *TenantControlPlane) bool

	// NewCluster is an optional function that creates a new cluster from a
	// rest.Config. The cluster will be started by the provider.
	NewCluster func(ctx context.Context, tcp *TenantControlPlane, cfg *rest.Config, opts ...cluster.Option) (cluster.Cluster, error)
}

type index struct {
	object       client.Object
	field        string
	extractValue client.IndexerFunc
}

// Provider discovers TenantControlPlanes managed by Kamaji and creates
// controller-runtime clusters for each ready control plane.
type Provider struct {
	opts     Options
	log      logr.Logger
	lock     sync.RWMutex
	clusters map[multicluster.ClusterName]activeCluster
	indexers []index
	mgr      mcmanager.Manager
	cl       client.Client
}

type activeCluster struct {
	Cluster cluster.Cluster
	Context context.Context
	Cancel  context.CancelFunc
	Hash    string
}

// clusterName returns the cluster name for a TenantControlPlane in "namespace/name" format.
func clusterName(tcp *TenantControlPlane) multicluster.ClusterName {
	return multicluster.ClusterName(tcp.Namespace + "/" + tcp.Name)
}

// getCluster retrieves a cluster by name with read lock.
func (p *Provider) getCluster(clusterName multicluster.ClusterName) (activeCluster, bool) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	ac, exists := p.clusters[clusterName]
	return ac, exists
}

// setCluster adds a cluster with write lock.
func (p *Provider) setCluster(clusterName multicluster.ClusterName, ac activeCluster) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.clusters[clusterName] = ac
}

// addIndexer registers a field indexer for future clusters.
func (p *Provider) addIndexer(idx index) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.indexers = append(p.indexers, idx)
}

// Get returns the cluster with the given name.
func (p *Provider) Get(ctx context.Context, clusterName multicluster.ClusterName) (cluster.Cluster, error) {
	ac, exists := p.getCluster(clusterName)
	if !exists {
		return nil, multicluster.ErrClusterNotFound
	}
	return ac.Cluster, nil
}

// IndexField indexes a field on all current and future clusters.
func (p *Provider) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
	p.addIndexer(index{
		object:       obj,
		field:        field,
		extractValue: extractValue,
	})

	p.lock.RLock()
	defer p.lock.RUnlock()

	for name, ac := range p.clusters {
		if err := ac.Cluster.GetFieldIndexer().IndexField(ctx, obj, field, extractValue); err != nil {
			return fmt.Errorf("failed to index field %q on cluster %q: %w", field, name, err)
		}
	}

	return nil
}

// SetupWithManager registers the TenantControlPlane controller with the
// multicluster manager's local manager.
func (p *Provider) SetupWithManager(ctx context.Context, mgr mcmanager.Manager) error {
	log := p.log
	log.Info("Starting kamaji provider", "options", p.opts)

	if mgr == nil {
		return fmt.Errorf("manager is nil")
	}
	p.mgr = mgr

	localMgr := mgr.GetLocalManager()
	if localMgr == nil {
		return fmt.Errorf("local manager is nil")
	}

	p.cl = localMgr.GetClient()

	// Register the TenantControlPlane types with the local scheme.
	if err := AddToScheme(localMgr.GetScheme()); err != nil {
		return fmt.Errorf("failed to register Kamaji types: %w", err)
	}

	// Build controller options.
	controllerOpts := controller.Options{
		MaxConcurrentReconciles: 1,
	}

	var predicates predicate.Predicate
	if p.opts.Namespace != "" {
		predicates = predicate.NewPredicateFuncs(func(obj client.Object) bool {
			return obj.GetNamespace() == p.opts.Namespace
		})
	}

	// Create the controller.
	err := ctrl.NewControllerManagedBy(localMgr).
		For(&TenantControlPlane{}, builder.WithPredicates(predicates)).
		WithOptions(controllerOpts).
		Named(p.opts.ControllerName).
		Complete(p)
	if err != nil {
		return fmt.Errorf("failed to create controller: %w", err)
	}

	return nil
}

// Reconcile handles TenantControlPlane reconciliation.
func (p *Provider) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := p.log.WithValues("tenantcontrolplane", req.NamespacedName.String())

	tcp, err := p.getTenantControlPlane(ctx, req.NamespacedName)
	if err != nil {
		return reconcile.Result{}, err
	}
	if tcp == nil {
		// TCP deleted — remove the cluster.
		name := multicluster.ClusterName(req.Namespace + "/" + req.Name)
		p.removeCluster(name)
		return reconcile.Result{}, nil
	}

	cn := clusterName(tcp)
	log = log.WithValues("cluster", cn)

	// Skip paused TCPs.
	if tcp.Annotations[PausedReconciliationAnnotation] == "true" {
		log.Info("TenantControlPlane is paused, skipping")
		return reconcile.Result{}, nil
	}

	// Handle deletion.
	if tcp.DeletionTimestamp != nil {
		p.removeCluster(cn)
		return reconcile.Result{}, nil
	}

	// Check readiness.
	if !p.isReady(ctx, tcp) {
		log.Info("TenantControlPlane is not ready, skipping")
		return reconcile.Result{}, nil
	}

	// Get the kubeconfig from the admin Secret.
	kubeconfigData, err := p.getKubeconfig(ctx, tcp)
	if err != nil {
		log.Error(err, "Failed to get kubeconfig")
		return reconcile.Result{}, err
	}

	// Hash for change detection.
	hashStr := hashKubeconfig(kubeconfigData)

	// Check if already engaged with the same kubeconfig.
	existingCluster, exists := p.getCluster(cn)
	if exists {
		if existingCluster.Hash == hashStr {
			log.Info("Cluster already engaged with the same kubeconfig, skipping")
			return reconcile.Result{}, nil
		}
		log.Info("Cluster kubeconfig changed, updating")
		p.removeCluster(cn)
	}

	// Create and engage the cluster.
	if err := p.createAndEngageCluster(ctx, cn, tcp, kubeconfigData, hashStr, log); err != nil {
		return reconcile.Result{}, err
	}

	return reconcile.Result{}, nil
}

// getTenantControlPlane retrieves the TCP from the local cache.
func (p *Provider) getTenantControlPlane(ctx context.Context, key client.ObjectKey) (*TenantControlPlane, error) {
	tcp := &TenantControlPlane{}
	if err := p.cl.Get(ctx, key, tcp); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get TenantControlPlane: %w", err)
	}
	return tcp, nil
}

// isReady checks whether the TenantControlPlane is ready to be engaged.
func (p *Provider) isReady(ctx context.Context, tcp *TenantControlPlane) bool {
	if p.opts.IsReady != nil {
		return p.opts.IsReady(ctx, tcp)
	}

	// Default readiness: Kubernetes version status is "Ready",
	// admin kubeconfig secret is set, and control plane endpoint is non-empty.
	status := tcp.Status.Kubernetes.Version.Status
	if status == nil || *status != VersionReady {
		return false
	}
	if tcp.Status.KubeConfig.Admin.SecretName == "" {
		return false
	}
	if tcp.Status.ControlPlaneEndpoint == "" {
		return false
	}
	return true
}

// getKubeconfig retrieves the kubeconfig from the admin Secret.
func (p *Provider) getKubeconfig(ctx context.Context, tcp *TenantControlPlane) ([]byte, error) {
	secretName := tcp.Status.KubeConfig.Admin.SecretName
	if secretName == "" {
		return nil, fmt.Errorf("admin kubeconfig secret name not set for TenantControlPlane %s/%s",
			tcp.Namespace, tcp.Name)
	}

	// Determine which key to use from the Secret.
	key := p.opts.KubeconfigSecretKey
	if annotationKey, ok := tcp.Annotations[KubeconfigSecretKeyAnnotation]; ok && annotationKey != "" {
		key = annotationKey
	}

	secret := &corev1.Secret{}
	if err := p.cl.Get(ctx, client.ObjectKey{Namespace: tcp.Namespace, Name: secretName}, secret); err != nil {
		return nil, fmt.Errorf("failed to get kubeconfig secret %s/%s: %w",
			tcp.Namespace, secretName, err)
	}

	kubeconfigData, ok := secret.Data[key]
	if !ok || len(kubeconfigData) == 0 {
		return nil, fmt.Errorf("kubeconfig secret %s/%s does not contain key %q",
			tcp.Namespace, secretName, key)
	}

	return kubeconfigData, nil
}

// createAndEngageCluster creates, starts, and engages a cluster.
func (p *Provider) createAndEngageCluster(ctx context.Context, cn multicluster.ClusterName, tcp *TenantControlPlane, kubeconfigData []byte, hashStr string, log logr.Logger) error {
	restConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeconfigData)
	if err != nil {
		return fmt.Errorf("failed to parse kubeconfig: %w", err)
	}

	for _, opt := range p.opts.RESTOptions {
		if err := opt(restConfig); err != nil {
			return fmt.Errorf("failed to apply REST option: %w", err)
		}
	}

	log.Info("Creating new cluster from kubeconfig")

	var cl cluster.Cluster
	if p.opts.NewCluster != nil {
		cl, err = p.opts.NewCluster(ctx, tcp, restConfig, p.opts.ClusterOptions...)
	} else {
		cl, err = cluster.New(restConfig, p.opts.ClusterOptions...)
	}
	if err != nil {
		return fmt.Errorf("failed to create cluster: %w", err)
	}

	// Apply deferred field indexers.
	if err := p.applyIndexers(ctx, cl); err != nil {
		return err
	}

	clusterCtx, cancel := context.WithCancel(ctx)

	go func() {
		if err := cl.Start(clusterCtx); err != nil {
			log.Error(err, "Failed to start cluster")
		}
	}()

	log.Info("Waiting for cluster cache to be ready")
	if !cl.GetCache().WaitForCacheSync(clusterCtx) {
		cancel()
		return fmt.Errorf("failed to wait for cache sync")
	}
	log.Info("Cluster cache is ready")

	p.setCluster(cn, activeCluster{
		Cluster: cl,
		Context: clusterCtx,
		Cancel:  cancel,
		Hash:    hashStr,
	})
	log.Info("Successfully added cluster")

	if err := p.mgr.Engage(clusterCtx, cn, cl); err != nil {
		p.removeCluster(cn)
		return fmt.Errorf("failed to engage manager: %w", err)
	}
	log.Info("Successfully engaged manager")

	return nil
}

// applyIndexers applies all deferred field indexers to a cluster.
func (p *Provider) applyIndexers(ctx context.Context, cl cluster.Cluster) error {
	p.lock.RLock()
	defer p.lock.RUnlock()

	for _, idx := range p.indexers {
		if err := cl.GetFieldIndexer().IndexField(ctx, idx.object, idx.field, idx.extractValue); err != nil {
			return fmt.Errorf("failed to index field %q: %w", idx.field, err)
		}
	}
	return nil
}

// removeCluster removes a cluster by name with proper cleanup.
func (p *Provider) removeCluster(clusterName multicluster.ClusterName) {
	log := p.log.WithValues("cluster", clusterName)

	p.lock.Lock()
	ac, exists := p.clusters[clusterName]
	if !exists {
		p.lock.Unlock()
		log.Info("Cluster not found, nothing to remove")
		return
	}

	log.Info("Removing cluster")
	delete(p.clusters, clusterName)
	p.lock.Unlock()

	ac.Cancel()
	log.Info("Successfully removed cluster and cancelled cluster context")
}

// hashKubeconfig creates a SHA-256 hash of the kubeconfig data.
func hashKubeconfig(kubeconfigData []byte) string {
	hash := sha256.New()
	hash.Write(kubeconfigData)
	return hex.EncodeToString(hash.Sum(nil))
}
