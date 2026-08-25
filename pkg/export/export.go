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

// Package export reads source objects for the Sanitize renderer.
package export

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"

	"github.com/PipeOpsHQ/portage/pkg/classify"
)

// Workload returns PVC + controller objects for one classified unit.
func Workload(ctx context.Context, kube kubernetes.Interface, w classify.Workload) ([]*unstructured.Unstructured, error) {
	var out []*unstructured.Unstructured
	for _, name := range w.PVCNames {
		pvc, err := kube.CoreV1().PersistentVolumeClaims(w.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			continue
		}
		u, err := toUnstructured(pvc)
		if err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	switch w.Kind {
	case "StatefulSet":
		sts, err := kube.AppsV1().StatefulSets(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
		if err == nil {
			if u, err := toUnstructured(sts); err == nil {
				out = append(out, u)
			}
		}
	case "Deployment":
		dep, err := kube.AppsV1().Deployments(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
		if err == nil {
			if u, err := toUnstructured(dep); err == nil {
				out = append(out, u)
			}
		}
	case "DaemonSet":
		ds, err := kube.AppsV1().DaemonSets(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
		if err == nil {
			if u, err := toUnstructured(ds); err == nil {
				out = append(out, u)
			}
		}
	}
	return out, nil
}

func toUnstructured(obj runtime.Object) (*unstructured.Unstructured, error) {
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, err
	}
	u := &unstructured.Unstructured{Object: m}
	switch obj.(type) {
	case *appsv1.StatefulSet:
		u.SetAPIVersion("apps/v1")
		u.SetKind("StatefulSet")
	case *appsv1.Deployment:
		u.SetAPIVersion("apps/v1")
		u.SetKind("Deployment")
	case *appsv1.DaemonSet:
		u.SetAPIVersion("apps/v1")
		u.SetKind("DaemonSet")
	default:
		u.SetAPIVersion("v1")
		u.SetKind("PersistentVolumeClaim")
	}
	return u, nil
}
