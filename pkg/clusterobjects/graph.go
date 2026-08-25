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

// Package clusterobjects is live-sync of Kubernetes API objects (the
// Kubernetes API is the data plane). Same rules as workloads: sanitize,
// unknown CRs stay in the graph, dest Get is the probe.
package clusterobjects

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/classify"
	"github.com/PipeOpsHQ/portage/pkg/transform"
)

// Item is one object plus the GVR it was listed from.
type Item struct {
	GVR        schema.GroupVersionResource
	Namespaced bool
	Obj        *unstructured.Unstructured
}

// Ref is a discovered GVR. Namespaced=false is listed cluster-wide (CRDs, ClusterRoles, cluster-scoped CRs).
type Ref struct {
	schema.GroupVersionResource
	Namespaced bool
}

var skipResource = map[string]struct{}{
	"pods": {}, "replicasets": {}, "replicationcontrollers": {},
	"endpoints": {}, "endpointslices": {}, "events": {}, "bindings": {},
	"componentstatuses": {}, "limitranges": {},
	"tokenreviews": {}, "subjectaccessreviews": {}, "selfsubjectaccessreviews": {},
	"selfsubjectrulesreviews": {}, "localsubjectaccessreviews": {},
	"leases": {}, "nodes": {}, "persistentvolumes": {}, "persistentvolumeclaims": {},
	"volumeattachments": {}, "csidrivers": {}, "csinodes": {}, "csistoragecapacities": {},
	"certificatesigningrequests": {}, "apiservices": {},
	"flowschemas": {}, "prioritylevelconfigurations": {},
	"runtimeclasses": {}, "controllerrevisions": {}, "evictions": {},
	"podtemplates": {}, "statefulsets": {}, "deployments": {}, "daemonsets": {},
	"storageclasses": {}, "volumeattributesclasses": {}, "jobs": {},
	"volumesnapshots": {}, "volumesnapshotcontents": {}, "volumesnapshotclasses": {},
}

var skipGroup = map[string]struct{}{
	"events.k8s.io":                {},
	"metrics.k8s.io":               {},
	"discovery.k8s.io":             {},
	"coordination.k8s.io":          {},
	"node.k8s.io":                  {},
	"authentication.k8s.io":        {},
	"authorization.k8s.io":         {},
	"portage.io":                   {},
	"apiregistration.k8s.io":       {},
	"flowcontrol.apiserver.k8s.io": {},
	"admissionregistration.k8s.io": {},
}

var skipClusterRole = map[string]struct{}{
	"cluster-admin": {}, "admin": {}, "edit": {}, "view": {},
}

var skipNS = map[string]struct{}{
	"kube-system": {}, "kube-public": {}, "kube-node-lease": {},
}

// Synthetic is the Action/Policy inventory row for the object graph.
func Synthetic(ns string) classify.Workload {
	if ns == "" {
		ns = "default"
	}
	return classify.Workload{
		Namespace:  ns,
		Name:       "cluster-objects",
		Kind:       "ObjectGraph",
		APIVersion: "portage.io/v1alpha1",
		Class:      portagev1alpha1.ClassClusterObjects,
	}
}

// Is reports whether w is the object-graph synthetic workload.
func Is(w classify.Workload) bool {
	return w.Class == portagev1alpha1.ClassClusterObjects || w.Kind == "ObjectGraph"
}

// Capture is Discover + List + Sanitize. This is the live-replication read.
func Capture(ctx context.Context, d discovery.DiscoveryInterface, dyn dynamic.Interface, spec portagev1alpha1.ClusterObjectsSpec, namespaces []string, opt transform.Options) ([]Item, error) {
	gvrs, err := Discover(d, includeClusterScoped(spec))
	if err != nil {
		return nil, err
	}
	items, err := List(ctx, dyn, gvrs, namespaces, spec.ExcludeNamespaces)
	if err != nil {
		return nil, err
	}
	return Sanitize(items, opt), nil
}

func includeClusterScoped(spec portagev1alpha1.ClusterObjectsSpec) bool {
	if spec.IncludeClusterScoped == nil {
		return true
	}
	return *spec.IncludeClusterScoped
}

// Discover preferred GVRs, minus ephemeral/cluster-local kinds.
// CRDs are always included (unknown CRs cannot restore without them).
// Other cluster-scoped APIs follow clusterScoped (default on).
func Discover(d discovery.DiscoveryInterface, clusterScoped bool) ([]Ref, error) {
	if d == nil {
		return nil, fmt.Errorf("clusterobjects: discovery client required")
	}
	lists, err := d.ServerPreferredResources()
	if len(lists) == 0 {
		// FakeDiscovery and partial aggregated APIs leave preferred empty.
		_, lists, err = d.ServerGroupsAndResources()
	}
	if err != nil && len(lists) == 0 {
		return nil, err
	}
	var out []Ref
	seen := map[string]struct{}{}
	for _, list := range lists {
		gv, err := schema.ParseGroupVersion(list.GroupVersion)
		if err != nil {
			continue
		}
		if _, skip := skipGroup[gv.Group]; skip {
			continue
		}
		for _, r := range list.APIResources {
			if strings.Contains(r.Name, "/") {
				continue // status/scale subresources
			}
			if _, skip := skipResource[r.Name]; skip {
				continue
			}
			key := gv.Group + "/" + r.Name
			if _, dup := seen[key]; dup {
				continue
			}
			if !r.Namespaced {
				if r.Name != "customresourcedefinitions" && !clusterScoped {
					continue
				}
			}
			verbs := strings.Join(r.Verbs, ",")
			if !strings.Contains(verbs, "list") || !strings.Contains(verbs, "create") {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, Ref{
				GroupVersionResource: schema.GroupVersionResource{Group: gv.Group, Version: gv.Version, Resource: r.Name},
				Namespaced:           r.Namespaced,
			})
		}
	}
	return out, nil
}

// List copies objects from source. Forbidden GVRs are skipped (unknown we
// cannot see is logged by the caller as missing, not silently dropped from
// a GVR we could list).
func List(ctx context.Context, dyn dynamic.Interface, gvrs []Ref, namespaces []string, extraSkip []string) ([]Item, error) {
	if dyn == nil {
		return nil, fmt.Errorf("clusterobjects: dynamic client required")
	}
	skip := map[string]struct{}{}
	for k := range skipNS {
		skip[k] = struct{}{}
	}
	for _, n := range extraSkip {
		skip[n] = struct{}{}
	}
	var out []Item
	for _, ref := range gvrs {
		gvr := ref.GroupVersionResource
		if !ref.Namespaced {
			list, err := dyn.Resource(gvr).List(ctx, metav1.ListOptions{})
			if skipListErr(err) {
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("list %s: %w", gvr.String(), err)
			}
			for i := range list.Items {
				obj := list.Items[i]
				if gvr.Resource == "namespaces" {
					if _, no := skip[obj.GetName()]; no {
						continue
					}
					if !contains(namespaces, obj.GetName()) {
						continue
					}
				}
				if skipObj(gvr, &obj) {
					continue
				}
				out = append(out, Item{GVR: gvr, Namespaced: false, Obj: obj.DeepCopy()})
			}
			continue
		}
		for _, ns := range namespaces {
			if _, no := skip[ns]; no {
				continue
			}
			list, err := dyn.Resource(gvr).Namespace(ns).List(ctx, metav1.ListOptions{})
			if skipListErr(err) {
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("list %s %s: %w", ns, gvr.String(), err)
			}
			for i := range list.Items {
				if skipObj(gvr, &list.Items[i]) {
					continue
				}
				out = append(out, Item{GVR: gvr, Namespaced: true, Obj: list.Items[i].DeepCopy()})
			}
		}
	}
	return out, nil
}

func skipListErr(err error) bool {
	return err != nil && (errors.IsForbidden(err) || errors.IsNotFound(err) || errors.IsMethodNotSupported(err))
}

func skipObj(gvr schema.GroupVersionResource, obj *unstructured.Unstructured) bool {
	if obj == nil {
		return true
	}
	if obj.GetName() == "kube-root-ca.crt" {
		return true
	}
	if gvr.Resource == "secrets" {
		t, _, _ := unstructured.NestedString(obj.Object, "type")
		if t == "kubernetes.io/service-account-token" {
			return true
		}
	}
	if gvr.Resource == "services" && obj.GetName() == "kubernetes" && obj.GetNamespace() == "default" {
		return true
	}
	if gvr.Resource == "customresourcedefinitions" && strings.HasSuffix(obj.GetName(), ".portage.io") {
		return true
	}
	if gvr.Resource == "clusterroles" || gvr.Resource == "clusterrolebindings" {
		n := obj.GetName()
		if strings.HasPrefix(n, "system:") || strings.HasPrefix(n, "kubeadm:") {
			return true
		}
		if _, skip := skipClusterRole[n]; skip {
			return true
		}
	}
	return false
}

func contains(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}

// Sanitize dest-shapes every object (UID/RV/status/zone pins).
func Sanitize(items []Item, opt transform.Options) []Item {
	out := make([]Item, 0, len(items))
	for _, it := range items {
		cp := it.Obj.DeepCopy()
		transform.Object(cp, opt)
		out = append(out, Item{GVR: it.GVR, Namespaced: it.Namespaced, Obj: cp})
	}
	return out
}
