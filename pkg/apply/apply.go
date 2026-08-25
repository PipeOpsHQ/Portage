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

// Package apply writes rendered objects to the destination cluster.
// PVCs first, then config, then workloads — never VCT-first empty disks.
package apply

import (
	"context"
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

var order = map[string]int{
	"Namespace":             0,
	"PersistentVolumeClaim": 1,
	"Secret":                2,
	"ConfigMap":             3,
	"Service":               4,
	"StatefulSet":           5,
	"Deployment":            6,
	"DaemonSet":             7,
}

func gvrFor(kind string) schema.GroupVersionResource {
	switch kind {
	case "Namespace":
		return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "namespaces"}
	case "PersistentVolumeClaim":
		return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "persistentvolumeclaims"}
	case "Secret":
		return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "secrets"}
	case "ConfigMap":
		return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}
	case "Service":
		return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"}
	case "StatefulSet":
		return schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "statefulsets"}
	case "Deployment":
		return schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	case "DaemonSet":
		return schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "daemonsets"}
	default:
		return schema.GroupVersionResource{}
	}
}

// Objects creates dest resources. Existing objects are left (never overwrite live PVC data).
func Objects(ctx context.Context, dyn dynamic.Interface, objs []*unstructured.Unstructured) error {
	if dyn == nil {
		return fmt.Errorf("apply: dynamic client required")
	}
	sorted := append([]*unstructured.Unstructured(nil), objs...)
	sort.SliceStable(sorted, func(i, j int) bool {
		return order[sorted[i].GetKind()] < order[sorted[j].GetKind()]
	})
	for _, obj := range sorted {
		gvr := gvrFor(obj.GetKind())
		if gvr.Resource == "" {
			continue
		}
		ns := obj.GetNamespace()
		ri := dyn.Resource(gvr)
		var err error
		if ns == "" || obj.GetKind() == "Namespace" {
			_, err = ri.Create(ctx, obj, metav1.CreateOptions{})
		} else {
			_, err = ri.Namespace(ns).Create(ctx, obj, metav1.CreateOptions{})
		}
		if errors.IsAlreadyExists(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("apply %s/%s: %w", obj.GetKind(), obj.GetName(), err)
		}
	}
	return nil
}
