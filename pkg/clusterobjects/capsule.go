/*
Copyright 2026 PipeOps and the Portage Authors.

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

package clusterobjects

import (
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TenantLabel is the Capsule label that names the Tenant for a Namespace.
// Capsule 0.14 rejects the object unless a Tenant ownerReference is present
// as well, and it writes that ownerReference only when the caller is a
// registered owner of the Tenant. A label by itself is not ownership.
const TenantLabel = "capsule.clastix.io/tenant"

const capsuleGroup = "capsule.clastix.io"

// CapsuleTenant returns the Tenant name from the Capsule label, if any.
func CapsuleTenant(obj *unstructured.Unstructured) string {
	if obj == nil {
		return ""
	}
	return obj.GetLabels()[TenantLabel]
}

// HasCapsuleOwner reports whether obj has a Tenant ownerReference.
// Capsule compares that reference to the destination Tenant UID. A source
// UID does not count, and a namespace that already exists without one cannot
// gain it on update ("namespace can not be patched into a tenant").
func HasCapsuleOwner(obj *unstructured.Unstructured) bool {
	if obj == nil {
		return false
	}
	for _, ref := range obj.GetOwnerReferences() {
		if capsuleOwnerRef(ref) {
			return true
		}
	}
	return false
}

func capsuleOwnerRef(ref metav1.OwnerReference) bool {
	return ref.Kind == "Tenant" && strings.HasPrefix(ref.APIVersion, capsuleGroup+"/")
}

// KeepDestCapsuleOwners copies destination Tenant ownerReferences onto obj.
// Sanitize drops every ownerReference (the source Tenant UID is wrong on
// dest). Writing that empty list back removes ownership, and Capsule will
// not put it back on update.
func KeepDestCapsuleOwners(obj, cur *unstructured.Unstructured) {
	if obj == nil || cur == nil || !HasCapsuleOwner(cur) {
		return
	}
	if obj.GetKind() != "Namespace" && obj.GetKind() != "" {
		return
	}
	merged := make([]metav1.OwnerReference, 0, len(cur.GetOwnerReferences()))
	for _, ref := range cur.GetOwnerReferences() {
		if capsuleOwnerRef(ref) {
			merged = append(merged, ref)
		}
	}
	for _, ref := range obj.GetOwnerReferences() {
		if !capsuleOwnerRef(ref) {
			merged = append(merged, ref)
		}
	}
	obj.SetOwnerReferences(merged)
}

// AnnotateNamespaceError adds the operator step Capsule will not infer.
// Admission text alone says the label and the ownerReference disagree.
// The caller has to be a Tenant owner, and an already-created unowned
// namespace has to be deleted before a later create can be owned.
func AnnotateNamespaceError(err error, namespace, tenant string) error {
	if err == nil || tenant == "" {
		return err
	}
	s := err.Error()
	switch {
	case strings.Contains(s, "non-owned Tenant") ||
		strings.Contains(s, "only tenant owners") ||
		strings.Contains(s, "no Tenant ownerReference"):
		// Create was rejected. The namespace is not on dest, so there is
		// nothing to delete. The caller has to be a Tenant owner.
		return fmt.Errorf("%w. %s", err, ownerHint(tenant))
	case strings.Contains(s, "patched into a tenant") ||
		strings.Contains(s, "both be set"):
		return fmt.Errorf("%w. %s", err, UnmanagedNamespace(namespace, tenant))
	case strings.Contains(s, "not found") && strings.Contains(s, "tenants.capsule"):
		return fmt.Errorf("%w. %s", err, MissingTenant(namespace, tenant))
	default:
		// Webhook denials we do not recognize still need the owner step.
		// A plain RBAC Forbidden on tenants.capsule.clastix.io does not.
		if strings.Contains(s, "projectcapsule") || strings.Contains(s, "admission webhook") {
			return fmt.Errorf("%w. %s", err, ownerHint(tenant))
		}
		return err
	}
}

// UnmanagedNamespace is the permanent-failure text for a destination
// namespace Capsule will not adopt after the fact.
func UnmanagedNamespace(namespace, tenant string) string {
	return fmt.Sprintf(
		"destination namespace %q exists without a Tenant ownerReference for %q. Capsule will not adopt it when the label is added later. %s Then delete namespace %q and retry so the next create is authenticated as that owner. Deleting the namespace also deletes PVCs replicated into it",
		namespace, tenant, ownerHint(tenant), namespace,
	)
}

// MissingTenant is the permanent-failure text when the destination has no
// Tenant CR for a Capsule-labeled namespace.
func MissingTenant(namespace, tenant string) string {
	return fmt.Sprintf(
		"Tenant %q is not on the destination, so Capsule cannot own namespace %q. Tenants are cluster-scoped and are copied when clusterObjects.includeClusterScoped is true. Confirm capsule.clastix.io/tenants can be listed on the source, create Tenant %q on the destination if it was skipped, then retry",
		tenant, namespace, tenant,
	)
}

func ownerHint(tenant string) string {
	return fmt.Sprintf(
		"Capsule sets a Tenant ownerReference only when the caller is a registered owner of Tenant %q. Add the Portage destination ServiceAccount to that Tenant's spec.owners on the destination (kind ServiceAccount, name system:serviceaccount:<portage-namespace>:<portage-sa>)",
		tenant,
	)
}
