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
	kamajiv1alpha1 "github.com/clastix/kamaji/api/v1alpha1"
)

// Re-export the Kamaji CRD types for convenience.
type (
	TenantControlPlane       = kamajiv1alpha1.TenantControlPlane
	TenantControlPlaneList   = kamajiv1alpha1.TenantControlPlaneList
	TenantControlPlaneStatus = kamajiv1alpha1.TenantControlPlaneStatus
	KubeconfigsStatus        = kamajiv1alpha1.KubeconfigsStatus
	KubeconfigStatus         = kamajiv1alpha1.KubeconfigStatus
	KubernetesStatus         = kamajiv1alpha1.KubernetesStatus
	KubernetesVersion        = kamajiv1alpha1.KubernetesVersion
	KubernetesVersionStatus  = kamajiv1alpha1.KubernetesVersionStatus
)

// Re-export Kamaji scheme registration.
var AddToScheme = kamajiv1alpha1.AddToScheme

// KubeconfigSecretKeyAnnotation is the annotation on a TenantControlPlane that
// specifies the key in the admin kubeconfig Secret to use.
const KubeconfigSecretKeyAnnotation = kamajiv1alpha1.KubeconfigSecretKeyAnnotation

// PausedReconciliationAnnotation is the annotation that can be applied to
// TenantControlPlane objects to prevent processing.
const PausedReconciliationAnnotation = "kamaji.clastix.io/paused"

// Re-export version status values.
var (
	VersionProvisioning = kamajiv1alpha1.VersionProvisioning
	VersionCARotating   = kamajiv1alpha1.VersionCARotating
	VersionUpgrading    = kamajiv1alpha1.VersionUpgrading
	VersionMigrating    = kamajiv1alpha1.VersionMigrating
	VersionReady        = kamajiv1alpha1.VersionReady
	VersionNotReady     = kamajiv1alpha1.VersionNotReady
)
