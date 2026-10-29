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
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

var applyOrder = map[string]int{
	// Capsule will not own a namespace until the Tenant exists on dest.
	"tenants":                   -1,
	"namespaces":                0,
	"customresourcedefinitions": 1,
	"clusterroles":              2,
	"clusterrolebindings":       3,
	"serviceaccounts":           4,
	"secrets":                   5,
	"configmaps":                6,
	"roles":                     7,
	"rolebindings":              8,
	"services":                  9,
	"ingresses":                 10,
	"networkpolicies":           11,
}

func rank(resource string) int {
	if n, ok := applyOrder[resource]; ok {
		return n
	}
	return 50
}

func ri(dyn dynamic.Interface, it Item) dynamic.ResourceInterface {
	if !it.Namespaced || it.Obj.GetNamespace() == "" || it.GVR.Resource == "namespaces" {
		return dyn.Resource(it.GVR)
	}
	return dyn.Resource(it.GVR).Namespace(it.Obj.GetNamespace())
}

// Sync creates or updates dest objects. Existing live PVC data is never in
// this graph. Already-exists is updated (active replication), not skipped.
// Source objects with deletionTimestamp are Deleted on dest. One object's
// error does not abort the rest of the pass.
func Sync(ctx context.Context, dyn dynamic.Interface, items []Item) error {
	if dyn == nil {
		return fmt.Errorf("clusterobjects: dest dynamic client required")
	}
	sorted := append([]Item(nil), items...)
	sort.SliceStable(sorted, func(i, j int) bool {
		oi, oj := rank(sorted[i].GVR.Resource), rank(sorted[j].GVR.Resource)
		if oi != oj {
			return oi < oj
		}
		return sorted[i].Obj.GetName() < sorted[j].Obj.GetName()
	})
	var first error
	for _, it := range sorted {
		if it.Obj == nil {
			continue
		}
		if err := syncOne(ctx, dyn, it); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func syncOne(ctx context.Context, dyn dynamic.Interface, it Item) error {
	r := ri(dyn, it)
	if it.Deleting || isDeleting(it.Obj) {
		err := r.Delete(ctx, it.Obj.GetName(), metav1.DeleteOptions{})
		if errors.IsNotFound(err) || skipListErr(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("delete dest %s/%s: %w", it.Obj.GetKind(), it.Obj.GetName(), err)
		}
		return nil
	}
	it.Obj.SetDeletionTimestamp(nil)
	it.Obj.SetDeletionGracePeriodSeconds(nil)
	obj := it.Obj.DeepCopy()
	if it.GVR.Resource == "namespaces" {
		return syncNamespace(ctx, dyn, r, obj)
	}
	_, err := r.Create(ctx, obj, metav1.CreateOptions{})
	if errors.IsAlreadyExists(err) {
		err = updateWithRetry(ctx, r, obj, 4)
	}
	if skipListErr(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("sync %s/%s: %w", it.Obj.GetKind(), it.Obj.GetName(), err)
	}
	return nil
}

// tenantGVRs are the Capsule Tenant versions a destination may serve.
// Preferred discovery on the source can be either; the object is cluster-scoped.
var tenantGVRs = []schema.GroupVersionResource{
	{Group: capsuleGroup, Version: "v1beta2", Resource: "tenants"},
	{Group: capsuleGroup, Version: "v1beta1", Resource: "tenants"},
}

func syncNamespace(ctx context.Context, dyn dynamic.Interface, r dynamic.ResourceInterface, obj *unstructured.Unstructured) error {
	name := obj.GetName()
	tenant := CapsuleTenant(obj)
	// SelfSubjectReview only on the error path. A successful create does
	// not need to know which subject the apiserver saw.
	var caller Caller
	var lookedUp bool
	subject := func() Caller {
		if !lookedUp {
			lookedUp = true
			if tenant != "" {
				caller = CallerFromDynamic(ctx, dyn)
			}
		}
		return caller
	}
	if tenant != "" {
		ok, err := destTenantExists(ctx, dyn, tenant)
		if err != nil {
			return fmt.Errorf("sync Namespace/%s: %w", name, AnnotateNamespaceError(err, name, tenant, subject()))
		}
		if !ok {
			return fmt.Errorf("sync Namespace/%s: %s", name, MissingTenant(name, tenant))
		}
	}
	_, err := r.Create(ctx, obj, metav1.CreateOptions{})
	if errors.IsAlreadyExists(err) {
		cur, getErr := r.Get(ctx, name, metav1.GetOptions{})
		if getErr != nil {
			return fmt.Errorf("sync Namespace/%s: %w", name, getErr)
		}
		// Ownership is assigned only on create. Patching the label onto a
		// namespace Portage already created bare is rejected forever.
		if tenant != "" && !HasCapsuleOwner(cur) {
			return fmt.Errorf("sync Namespace/%s: %s", name, UnmanagedNamespace(name, tenant, subject()))
		}
		err = updateWithRetry(ctx, r, obj, 4)
	}
	// NotFound and MethodNotSupported are the list-skip cases. Forbidden is
	// how Capsule admission.Denied comes back (403). Treating that as a skip
	// drops the owner-registration error and the Action only reports the
	// namespace as missing.
	if errors.IsNotFound(err) || errors.IsMethodNotSupported(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("sync Namespace/%s: %w", name, AnnotateNamespaceError(err, name, tenant, subject()))
	}
	return nil
}

func destTenantExists(ctx context.Context, dyn dynamic.Interface, name string) (bool, error) {
	var denied error
	for _, gvr := range tenantGVRs {
		_, err := dyn.Resource(gvr).Get(ctx, name, metav1.GetOptions{})
		if err == nil {
			return true, nil
		}
		if errors.IsNotFound(err) || isNoMatch(err) {
			continue
		}
		denied = err
	}
	if denied != nil {
		return false, denied
	}
	return false, nil
}

func isNoMatch(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "no matches for") || strings.Contains(s, "could not find the requested resource") || strings.Contains(s, "not registered")
}

func updateWithRetry(ctx context.Context, r dynamic.ResourceInterface, obj *unstructured.Unstructured, attempts int) error {
	var err error
	for i := 0; i < attempts; i++ {
		cur, getErr := r.Get(ctx, obj.GetName(), metav1.GetOptions{})
		if getErr != nil {
			return getErr
		}
		obj.SetResourceVersion(cur.GetResourceVersion())
		obj.SetUID(cur.GetUID())
		KeepDestCapsuleOwners(obj, cur)
		KeepDestTenantOwners(obj, cur)
		_, err = r.Update(ctx, obj, metav1.UpdateOptions{})
		// Forbidden must propagate. syncOne still skips it for other
		// resources; syncNamespace reports it.
		if err == nil || errors.IsNotFound(err) || errors.IsMethodNotSupported(err) {
			return nil
		}
		if !errors.IsConflict(err) {
			return err
		}
	}
	return err
}

// Attest is the object-graph probe: dest Get must succeed. CRDs must be Established.
func Attest(ctx context.Context, dyn dynamic.Interface, items []Item) (bool, string) {
	if dyn == nil {
		return false, "dest dynamic client required"
	}
	if len(items) == 0 {
		return true, "no cluster objects"
	}
	var live, missing int
	var first string
	for _, it := range items {
		if it.Deleting {
			continue
		}
		live++
		obj, err := ri(dyn, it).Get(ctx, it.Obj.GetName(), metav1.GetOptions{})
		if err != nil {
			missing++
			if first == "" {
				first = it.Obj.GetKind() + "/" + it.Obj.GetName() + ": " + err.Error()
			}
			continue
		}
		if it.GVR.Resource == "customresourcedefinitions" && !crdEstablished(obj) {
			missing++
			if first == "" {
				first = it.Obj.GetName() + " CRD not Established"
			}
		}
	}
	if live == 0 {
		return true, "no cluster objects"
	}
	if missing > 0 {
		return false, fmt.Sprintf("%d/%d dest objects missing (%s)", missing, live, first)
	}
	return true, fmt.Sprintf("%d dest objects attested", live)
}

func crdEstablished(obj *unstructured.Unstructured) bool {
	if obj == nil {
		return false
	}
	conds, ok, _ := unstructuredNestedSlice(obj.Object, "status", "conditions")
	if !ok {
		return false
	}
	for _, c := range conds {
		m, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if m["type"] == "Established" && (m["status"] == "True" || m["status"] == true) {
			return true
		}
	}
	return false
}

func unstructuredNestedSlice(obj map[string]any, keys ...string) ([]any, bool, error) {
	cur := any(obj)
	for _, k := range keys {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false, nil
		}
		cur, ok = m[k]
		if !ok {
			return nil, false, nil
		}
	}
	s, ok := cur.([]any)
	return s, ok, nil
}
