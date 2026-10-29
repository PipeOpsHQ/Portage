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

// KeepDestTenantOwners unions destination Tenant spec.owners into obj.
// Cluster-object sync mirrors the source Tenant. A destination-only owner
// (the subject Portage actually presents on that cluster) is required for
// Capsule to admit namespaces, and the next sync used to replace spec.owners
// with the source list, which dropped that grant before the namespace create
// in the same pass. Source owners are applied. An owner present only on
// the destination is kept, including one the source used to list and has
// since removed. Other destination-only fields are still replaced.
// clusterObjects.excludeGVKs is the opt-out for the whole Tenant.
func KeepDestTenantOwners(obj, cur *unstructured.Unstructured) {
	if obj == nil || cur == nil || obj.GetKind() != "Tenant" {
		return
	}
	if !strings.HasPrefix(obj.GetAPIVersion(), capsuleGroup+"/") {
		return
	}
	curOwners, _, _ := unstructured.NestedSlice(cur.Object, "spec", "owners")
	if len(curOwners) == 0 {
		return
	}
	objOwners, _, _ := unstructured.NestedSlice(obj.Object, "spec", "owners")
	seen := map[string]struct{}{}
	merged := make([]any, 0, len(objOwners)+len(curOwners))
	for _, owner := range objOwners {
		merged = append(merged, owner)
		if key, ok := ownerIdentity(owner); ok {
			seen[key] = struct{}{}
		}
	}
	for _, owner := range curOwners {
		if key, ok := ownerIdentity(owner); ok {
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
		}
		merged = append(merged, owner)
	}
	_ = unstructured.SetNestedSlice(obj.Object, merged, "spec", "owners")
}

func ownerIdentity(owner any) (string, bool) {
	m, ok := owner.(map[string]any)
	if !ok {
		return "", false
	}
	kind, _, _ := unstructured.NestedString(m, "kind")
	name, _, _ := unstructured.NestedString(m, "name")
	if kind == "" || name == "" {
		return "", false
	}
	return kind + "\x00" + name, true
}

// AnnotateNamespaceError adds the operator step Capsule will not infer.
// Admission text alone says the label and the ownerReference disagree.
// The caller has to be a Tenant owner, and an already-created unowned
// namespace has to be deleted before a later create can be owned.
// caller is the subject SelfSubjectReview reported for this destination
// client. An empty caller keeps the hint from naming a hub ServiceAccount
// that is not the caller on azure, aws, gcp, or kubeconfig destinations.
func AnnotateNamespaceError(err error, namespace, tenant string, caller Caller) error {
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
		return fmt.Errorf("%w. %s", err, ownerHint(tenant, caller))
	case strings.Contains(s, "patched into a tenant") ||
		strings.Contains(s, "both be set"):
		return fmt.Errorf("%w. %s", err, UnmanagedNamespace(namespace, tenant, caller))
	case strings.Contains(s, "not found") && strings.Contains(s, "tenants.capsule"):
		return fmt.Errorf("%w. %s", err, MissingTenant(namespace, tenant))
	default:
		// Webhook denials we do not recognize still need the owner step.
		// A plain RBAC Forbidden on tenants.capsule.clastix.io does not.
		if strings.Contains(s, "projectcapsule") || strings.Contains(s, "admission webhook") {
			return fmt.Errorf("%w. %s", err, ownerHint(tenant, caller))
		}
		return err
	}
}

// UnmanagedNamespace is the permanent-failure text for a destination
// namespace Capsule will not adopt after the fact.
func UnmanagedNamespace(namespace, tenant string, caller Caller) string {
	return fmt.Sprintf(
		"destination namespace %q exists without a Tenant ownerReference for %q. Capsule will not adopt it when the label is added later. %s Then delete namespace %q and retry so the next create is authenticated as that owner. Deleting the namespace also deletes PVCs replicated into it",
		namespace, tenant, ownerHint(tenant, caller), namespace,
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

func ownerHint(tenant string, caller Caller) string {
	if caller.Name != "" {
		return fmt.Sprintf(
			"Capsule sets a Tenant ownerReference only when the caller is a registered owner of Tenant %q. The destination API server authenticated Portage as %s %q. Add that subject to destination Tenant %q spec.owners (kind: %s, name: %q). SelfSubjectReview on the destination client reported this subject. For an Entra token (destination.azure.credentialsSecret) the username is the service principal object ID, the token oid claim",
			tenant, caller.Kind, caller.Name, tenant, caller.Kind, caller.Name,
		)
	}
	return fmt.Sprintf(
		"Capsule sets a Tenant ownerReference only when the caller is a registered owner of Tenant %q. Add the subject the destination API server reports for this client to Tenant %q spec.owners. SelfSubjectReview did not return a username. When the destination is this cluster the subject is kind ServiceAccount, name system:serviceaccount:<portage-namespace>:<portage-sa>. When destination.azure.credentialsSecret is set, the subject is kind User and the name is the service principal object ID (the token oid claim, which kubectl auth whoami prints). The kubeconfig user clusterUser and the application clientID are different values",
		tenant, tenant,
	)
}
