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

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
)

var applyOrder = map[string]int{
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
	for _, it := range sorted {
		if it.Obj == nil {
			continue
		}
		r := ri(dyn, it)
		_, err := r.Create(ctx, it.Obj, metav1.CreateOptions{})
		if errors.IsAlreadyExists(err) {
			cur, getErr := r.Get(ctx, it.Obj.GetName(), metav1.GetOptions{})
			if getErr != nil {
				return fmt.Errorf("get dest %s/%s: %w", it.Obj.GetKind(), it.Obj.GetName(), getErr)
			}
			it.Obj.SetResourceVersion(cur.GetResourceVersion())
			it.Obj.SetUID(cur.GetUID())
			_, err = r.Update(ctx, it.Obj, metav1.UpdateOptions{})
		}
		if skipListErr(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("sync %s/%s: %w", it.Obj.GetKind(), it.Obj.GetName(), err)
		}
	}
	return nil
}

// Attest is the object-graph probe: dest Get must succeed. CRDs must be Established.
func Attest(ctx context.Context, dyn dynamic.Interface, items []Item) (bool, string) {
	if dyn == nil {
		return false, "dest dynamic client required"
	}
	if len(items) == 0 {
		return true, "no cluster objects"
	}
	var missing int
	var first string
	for _, it := range items {
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
	if missing > 0 {
		return false, fmt.Sprintf("%d/%d dest objects missing (%s)", missing, len(items), first)
	}
	return true, fmt.Sprintf("%d dest objects attested", len(items))
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
