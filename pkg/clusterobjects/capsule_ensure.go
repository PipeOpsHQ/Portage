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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// tenantOwnerLabel selects the Tenant a TenantOwner applies to. Capsule ORs
// this with the Tenant's own matchOwners selector. aggregate (default true)
// copies the subject into CapsuleConfiguration.status.users. It does not
// write Tenant.spec.owners. Admission reads Tenant.status.owners.
const tenantOwnerLabel = "projectcapsule.dev/tenant"

const managedByLabel = "app.kubernetes.io/managed-by"
const managedByPortage = "portage"

var tenantOwnerGVRs = []schema.GroupVersionResource{
	{Group: capsuleGroup, Version: "v1beta2", Resource: "tenantowners"},
	{Group: capsuleGroup, Version: "v1beta1", Resource: "tenantowners"},
}

// ensureCapsuleCaller registers the destination caller for both Capsule
// checks. status.users (via TenantOwner, aggregate left at the default)
// makes the namespace mutating webhook run. Tenant.spec.owners is what the
// Tenant controller copies into status.owners, which is the list that
// webhook then requires. A TenantOwner alone does not do the second half.
// An empty caller is left for the operator. Forbidden is returned so the
// reconcile retries with the RBAC error instead of hiding it.
func ensureCapsuleCaller(ctx context.Context, dyn dynamic.Interface, tenant string, caller Caller) error {
	if dyn == nil || tenant == "" || caller.Name == "" {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ensureTenantSpecOwner(ctx, dyn, tenant, caller); err != nil {
		return err
	}
	listed, known := capsuleUserListed(ctx, dyn, caller)
	if known && listed {
		return nil
	}
	return ensureTenantOwner(ctx, dyn, tenant, caller)
}

func ensureTenantSpecOwner(ctx context.Context, dyn dynamic.Interface, tenant string, caller Caller) error {
	var err error
	for i := 0; i < 4; i++ {
		err = ensureTenantSpecOwnerOnce(ctx, dyn, tenant, caller)
		if err == nil || !apierrors.IsConflict(err) {
			return err
		}
	}
	return err
}

func ensureTenantSpecOwnerOnce(ctx context.Context, dyn dynamic.Interface, tenant string, caller Caller) error {
	obj, gvr, err := findDestTenant(ctx, dyn, tenant)
	if err != nil {
		return fmt.Errorf("get Tenant %q: %w", tenant, err)
	}
	owners, _, _ := unstructured.NestedSlice(obj.Object, "spec", "owners")
	if subjectListed(owners, caller) {
		return nil
	}
	owners = append(owners, map[string]any{
		"kind":         caller.Kind,
		"name":         caller.Name,
		"clusterRoles": []any{"admin"},
	})
	if err := unstructured.SetNestedSlice(obj.Object, owners, "spec", "owners"); err != nil {
		return fmt.Errorf("set Tenant %q spec.owners: %w", tenant, err)
	}
	if _, err := dyn.Resource(gvr).Update(ctx, obj, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update Tenant %q spec.owners: %w", tenant, err)
	}
	return nil
}

func findDestTenant(ctx context.Context, dyn dynamic.Interface, name string) (*unstructured.Unstructured, schema.GroupVersionResource, error) {
	var denied error
	for _, gvr := range tenantGVRs {
		obj, err := dyn.Resource(gvr).Get(ctx, name, metav1.GetOptions{})
		if err == nil {
			return obj, gvr, nil
		}
		if apierrors.IsNotFound(err) || isNoMatch(err) {
			continue
		}
		denied = err
	}
	if denied != nil {
		return nil, schema.GroupVersionResource{}, denied
	}
	return nil, schema.GroupVersionResource{}, apierrors.NewNotFound(schema.GroupResource{Group: capsuleGroup, Resource: "tenants"}, name)
}

// capsuleUserListed reports whether CapsuleConfiguration.status.users
// already contains the caller. known is false when the list could not be read.
func capsuleUserListed(ctx context.Context, dyn dynamic.Interface, caller Caller) (listed, known bool) {
	configs, ok := listCapsuleConfigurations(ctx, dyn)
	if !ok {
		return false, false
	}
	pick := pickCapsuleConfiguration(configs)
	if pick == nil {
		return false, true
	}
	statusUsers, _, _ := unstructured.NestedSlice(pick.Object, "status", "users")
	return subjectListed(statusUsers, caller), true
}

func ensureTenantOwner(ctx context.Context, dyn dynamic.Interface, tenant string, caller Caller) error {
	primary := tenantOwnerName(tenant, "")
	alt := tenantOwnerName(tenant, callerSuffix(caller))
	name, err := pickTenantOwnerName(ctx, dyn, primary, alt, caller)
	if err != nil {
		return err
	}
	if name == "" {
		return nil
	}
	gvr, err := tenantOwnerGVR(ctx, dyn, name)
	if err != nil {
		return fmt.Errorf("create TenantOwner %s: %w. Add the subject to CapsuleConfiguration spec.users (kind: %s, name: %q) so status.users lists them", name, err, caller.Kind, caller.Name)
	}
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": gvr.GroupVersion().String(),
		"kind":       "TenantOwner",
		"metadata": map[string]any{
			"name": name,
			"labels": map[string]any{
				tenantOwnerLabel: tenant,
				managedByLabel:   managedByPortage,
			},
		},
		"spec": map[string]any{
			"kind":         caller.Kind,
			"name":         caller.Name,
			"clusterRoles": []any{"admin"},
		},
	}}
	if _, err := dyn.Resource(gvr).Create(ctx, obj, metav1.CreateOptions{}); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("create TenantOwner %s: %w", name, err)
	}
	return nil
}

// pickTenantOwnerName returns the object name to create. An empty name means
// an object for this subject already exists and must be left alone, including
// one an operator created at the stable name without Portage's label.
func pickTenantOwnerName(ctx context.Context, dyn dynamic.Interface, primary, alt string, caller Caller) (string, error) {
	existing, _, err := getTenantOwner(ctx, dyn, primary)
	if apierrors.IsNotFound(err) || isNoMatch(err) {
		return primary, nil
	}
	if err != nil {
		return "", fmt.Errorf("get TenantOwner %s: %w", primary, err)
	}
	if tenantOwnerSubject(existing, caller) {
		return "", nil
	}
	if alt == primary {
		return "", fmt.Errorf("TenantOwner %q belongs to a different subject and an alternate name does not fit", primary)
	}
	other, _, err := getTenantOwner(ctx, dyn, alt)
	if apierrors.IsNotFound(err) || isNoMatch(err) {
		return alt, nil
	}
	if err != nil {
		return "", fmt.Errorf("get TenantOwner %s: %w", alt, err)
	}
	if tenantOwnerSubject(other, caller) {
		return "", nil
	}
	return "", fmt.Errorf("TenantOwner %q and %q belong to other subjects", primary, alt)
}

// tenantOwnerGVR is the first served version whose API answers a get of name.
// A missing object is NotFound from a live API. A missing CRD is no match.
func tenantOwnerGVR(ctx context.Context, dyn dynamic.Interface, name string) (schema.GroupVersionResource, error) {
	var last error
	for _, gvr := range tenantOwnerGVRs {
		_, err := dyn.Resource(gvr).Get(ctx, name, metav1.GetOptions{})
		if err == nil || apierrors.IsNotFound(err) {
			return gvr, nil
		}
		if isNoMatch(err) {
			last = err
			continue
		}
		return schema.GroupVersionResource{}, err
	}
	if last == nil {
		last = fmt.Errorf("tenantowners.capsule.clastix.io not found")
	}
	return schema.GroupVersionResource{}, last
}

func getTenantOwner(ctx context.Context, dyn dynamic.Interface, name string) (*unstructured.Unstructured, schema.GroupVersionResource, error) {
	gvr, err := tenantOwnerGVR(ctx, dyn, name)
	if err != nil {
		return nil, schema.GroupVersionResource{}, err
	}
	obj, err := dyn.Resource(gvr).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, gvr, err
	}
	return obj, gvr, nil
}

func tenantOwnerSubject(obj *unstructured.Unstructured, caller Caller) bool {
	if obj == nil {
		return false
	}
	kind, _, _ := unstructured.NestedString(obj.Object, "spec", "kind")
	name, _, _ := unstructured.NestedString(obj.Object, "spec", "name")
	return kind == caller.Kind && name == caller.Name
}

func tenantOwnerName(tenant, suffix string) string {
	base := "portage-" + tenant
	if suffix != "" {
		base += "-" + suffix
	}
	if len(base) > 253 {
		base = base[:253]
	}
	return strings.TrimRight(base, "-.")
}

func callerSuffix(caller Caller) string {
	sum := sha256.Sum256([]byte(caller.Kind + "\x00" + caller.Name))
	return hex.EncodeToString(sum[:4])
}

func ownerNotYetAdmitted(tenant string, caller Caller) string {
	return fmt.Sprintf(
		"Tenant %q spec.owners lists kind: %s, name: %q and status.owners does not yet. Capsule admission reads status.owners, which the Tenant controller copies from spec.owners. A TenantOwner registers a Capsule user (CapsuleConfiguration status.users, from spec.users or spec.aggregate) and does not write Tenant spec.owners. Retrying until status.owners lists this subject",
		tenant, caller.Kind, caller.Name,
	)
}
