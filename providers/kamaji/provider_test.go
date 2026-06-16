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
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"

	mcluster "sigs.k8s.io/multicluster-runtime/pkg/multicluster"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("New", func() {
	It("should create a provider with default options", func() {
		p := New(Options{})
		Expect(p).NotTo(BeNil())
		Expect(p.opts.KubeconfigSecretKey).To(Equal(DefaultKubeconfigSecretKey))
		Expect(p.opts.ControllerName).To(Equal(ControllerName))
	})

	It("should create a provider with custom options", func() {
		p := New(Options{
			Namespace:           "kamaji-system",
			KubeconfigSecretKey: "admin.conf",
			ControllerName:      "custom-name",
		})
		Expect(p.opts.Namespace).To(Equal("kamaji-system"))
		Expect(p.opts.KubeconfigSecretKey).To(Equal("admin.conf"))
		Expect(p.opts.ControllerName).To(Equal("custom-name"))
	})

	It("should not override explicit KubeconfigSecretKey with default", func() {
		p := New(Options{
			KubeconfigSecretKey: "",
		})
		Expect(p.opts.KubeconfigSecretKey).To(Equal(DefaultKubeconfigSecretKey))
	})
})

var _ = Describe("clusterName", func() {
	It("should return namespace/name format", func() {
		tcp := &TenantControlPlane{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "my-cluster",
				Namespace: "default",
			},
		}
		Expect(clusterName(tcp)).To(Equal(mcluster.ClusterName("default/my-cluster")))
	})
})

var _ = Describe("hashKubeconfig", func() {
	It("should produce a consistent hash", func() {
		data := []byte("test-kubeconfig-data")
		hash1 := hashKubeconfig(data)
		hash2 := hashKubeconfig(data)
		Expect(hash1).To(Equal(hash2))
	})

	It("should produce different hashes for different data", func() {
		hash1 := hashKubeconfig([]byte("data1"))
		hash2 := hashKubeconfig([]byte("data2"))
		Expect(hash1).NotTo(Equal(hash2))
	})

	It("should produce a hex-encoded SHA-256 hash", func() {
		hash := hashKubeconfig([]byte("data"))
		Expect(hash).To(HaveLen(64)) // SHA-256 produces 32 bytes = 64 hex chars
	})
})

var _ = Describe("isReady", func() {
	var p *Provider
	var tcp *TenantControlPlane

	BeforeEach(func() {
		p = New(Options{})
		tcp = &TenantControlPlane{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-cluster",
				Namespace: "default",
			},
		}
	})

	It("should return false when version status is nil", func() {
		ready := p.isReady(context.Background(), tcp)
		Expect(ready).To(BeFalse())
	})

	It("should return false when version status is not Ready", func() {
		status := KubernetesVersionStatus(VersionProvisioning)
		tcp.Status.Kubernetes.Version.Status = &status
		ready := p.isReady(context.Background(), tcp)
		Expect(ready).To(BeFalse())
	})

	It("should return false when admin secret name is empty", func() {
		status := KubernetesVersionStatus(VersionReady)
		tcp.Status.Kubernetes.Version.Status = &status
		ready := p.isReady(context.Background(), tcp)
		Expect(ready).To(BeFalse())
	})

	It("should return false when control plane endpoint is empty", func() {
		status := KubernetesVersionStatus(VersionReady)
		tcp.Status.Kubernetes.Version.Status = &status
		tcp.Status.KubeConfig.Admin.SecretName = "my-admin-kubeconfig"
		ready := p.isReady(context.Background(), tcp)
		Expect(ready).To(BeFalse())
	})

	It("should return true when all conditions are met", func() {
		status := KubernetesVersionStatus(VersionReady)
		tcp.Status.Kubernetes.Version.Status = &status
		tcp.Status.KubeConfig.Admin.SecretName = "my-admin-kubeconfig"
		tcp.Status.ControlPlaneEndpoint = "192.168.1.10:6443"
		ready := p.isReady(context.Background(), tcp)
		Expect(ready).To(BeTrue())
	})

	It("should use custom IsReady function when provided", func() {
		customReady := false
		p = New(Options{
			IsReady: func(ctx context.Context, tcp *TenantControlPlane) bool {
				return customReady
			},
		})
		ready := p.isReady(context.Background(), tcp)
		Expect(ready).To(BeFalse())

		customReady = true
		ready = p.isReady(context.Background(), tcp)
		Expect(ready).To(BeTrue())
	})
})

var _ = Describe("Get", func() {
	It("should return ErrClusterNotFound for unknown cluster", func() {
		p := New(Options{})
		_, err := p.Get(context.Background(), "unknown")
		Expect(err).To(MatchError(mcluster.ErrClusterNotFound))
	})

	It("should return the cluster for known cluster", func() {
		p := New(Options{})
		mockCl := &mockCluster{}
		p.clusters["test"] = activeCluster{
			Cluster: mockCl,
			Cancel:  func() {},
		}
		cl, err := p.Get(context.Background(), "test")
		Expect(err).NotTo(HaveOccurred())
		Expect(cl).To(Equal(mockCl))
	})
})

var _ = Describe("removeCluster", func() {
	It("should not panic when removing a non-existent cluster", func() {
		p := New(Options{})
		Expect(func() {
			p.removeCluster("nonexistent")
		}).NotTo(Panic())
	})

	It("should remove the cluster and cancel its context", func() {
		p := New(Options{})
		cancelCalled := false
		p.clusters["test"] = activeCluster{
			Cluster: &mockCluster{},
			Cancel:  func() { cancelCalled = true },
		}
		p.removeCluster("test")
		Expect(cancelCalled).To(BeTrue())
		_, err := p.Get(context.Background(), "test")
		Expect(err).To(MatchError(mcluster.ErrClusterNotFound))
	})
})

var _ = Describe("DeepCopy", func() {
	It("should deep copy TenantControlPlane", func() {
		status := KubernetesVersionStatus(VersionReady)
		tcp := &TenantControlPlane{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "default",
				Annotations: map[string]string{
					KubeconfigSecretKeyAnnotation: "admin.conf",
				},
			},
			Status: TenantControlPlaneStatus{
				KubeConfig: KubeconfigsStatus{
					Admin: KubeconfigStatus{
						SecretName: "secret",
					},
				},
				Kubernetes: KubernetesStatus{
					Version: KubernetesVersion{
						Status: &status,
					},
				},
				ControlPlaneEndpoint: "1.2.3.4:6443",
			},
		}

		copied := tcp.DeepCopy()
		Expect(copied.Name).To(Equal("test"))
		Expect(copied.Namespace).To(Equal("default"))
		Expect(copied.Annotations[KubeconfigSecretKeyAnnotation]).To(Equal("admin.conf"))
		Expect(*copied.Status.Kubernetes.Version.Status).To(Equal(VersionReady))
		Expect(copied.Status.KubeConfig.Admin.SecretName).To(Equal("secret"))
		Expect(copied.Status.ControlPlaneEndpoint).To(Equal("1.2.3.4:6443"))

		// Verify it's a true copy
		*copied.Status.Kubernetes.Version.Status = VersionNotReady
		Expect(*tcp.Status.Kubernetes.Version.Status).To(Equal(VersionReady))
	})

	It("should deep copy TenantControlPlaneList", func() {
		status := KubernetesVersionStatus(VersionReady)
		list := &TenantControlPlaneList{
			Items: []TenantControlPlane{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test1",
						Namespace: "default",
					},
					Status: TenantControlPlaneStatus{
						Kubernetes: KubernetesStatus{
							Version: KubernetesVersion{
								Status: &status,
							},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test2",
						Namespace: "default",
					},
				},
			},
		}

		copied := list.DeepCopy()
		Expect(copied.Items).To(HaveLen(2))
		Expect(copied.Items[0].Name).To(Equal("test1"))
		Expect(copied.Items[1].Name).To(Equal("test2"))
	})

	It("should handle nil TenantControlPlane", func() {
		var tcp *TenantControlPlane
		Expect(tcp.DeepCopy()).To(BeNil())
	})

	It("should handle nil TenantControlPlaneList", func() {
		var list *TenantControlPlaneList
		Expect(list.DeepCopy()).To(BeNil())
	})

	It("should return nil from DeepCopyObject on nil receiver", func() {
		var tcp *TenantControlPlane
		Expect(tcp.DeepCopyObject()).To(BeNil())
	})

	It("should implement runtime.Object via DeepCopyObject", func() {
		tcp := &TenantControlPlane{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "default",
			},
		}
		var obj runtime.Object = tcp.DeepCopyObject()
		copied, ok := obj.(*TenantControlPlane)
		Expect(ok).To(BeTrue())
		Expect(copied.Name).To(Equal("test"))
	})
})

var _ = Describe("Concurrency", func() {
	It("should handle concurrent Get, IndexField, and removeCluster safely", func() {
		p := New(Options{})

		numClusters := 20
		for i := 0; i < numClusters; i++ {
			clusterName := mcluster.ClusterName(fmt.Sprintf("cluster-%d", i))
			p.clusters[clusterName] = activeCluster{
				Cluster: &mockCluster{},
				Cancel:  func() {},
			}
		}

		var wg sync.WaitGroup
		numGoroutines := 40
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(i int) {
				defer GinkgoRecover()
				defer wg.Done()

				switch i % 4 {
				case 0:
					// Index a field.
					err := p.IndexField(context.Background(), &corev1.Pod{}, "spec.nodeName", func(rawObj client.Object) []string {
						return nil
					})
					Expect(err).NotTo(HaveOccurred())
				case 1:
					// Get a cluster.
					_, err := p.Get(context.Background(), "cluster-1")
					Expect(err).To(Or(BeNil(), MatchError(mcluster.ErrClusterNotFound)))
				case 2:
					// Remove a cluster.
					clusterToRemove := mcluster.ClusterName(fmt.Sprintf("cluster-%d", i/4))
					p.removeCluster(clusterToRemove)
				case 3:
					// Store a new cluster.
					p.setCluster(mcluster.ClusterName(fmt.Sprintf("new-cluster-%d", i/4)), activeCluster{
						Cluster: &mockCluster{},
						Cancel:  func() {},
					})
				}
			}(i)
		}

		wg.Wait()
	})
})

// mockCluster implements cluster.Cluster for testing.
type mockCluster struct {
	cluster.Cluster
}

func (c *mockCluster) GetFieldIndexer() client.FieldIndexer {
	return &mockFieldIndexer{}
}

type mockFieldIndexer struct{}

func (f *mockFieldIndexer) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
	time.Sleep(time.Millisecond)
	return nil
}
