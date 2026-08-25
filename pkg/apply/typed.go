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

package apply

import (
	"context"
	"fmt"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
)

// Typed creates dest objects via the typed client (works with client-go fake).
func Typed(ctx context.Context, kube kubernetes.Interface, objs []*unstructured.Unstructured) error {
	if kube == nil {
		return fmt.Errorf("apply: kube client required")
	}
	sorted := append([]*unstructured.Unstructured(nil), objs...)
	sort.SliceStable(sorted, func(i, j int) bool {
		return order[sorted[i].GetKind()] < order[sorted[j].GetKind()]
	})
	for _, obj := range sorted {
		if err := createTyped(ctx, kube, obj); err != nil && !errors.IsAlreadyExists(err) {
			return fmt.Errorf("apply %s/%s: %w", obj.GetKind(), obj.GetName(), err)
		}
	}
	return nil
}

func createTyped(ctx context.Context, kube kubernetes.Interface, obj *unstructured.Unstructured) error {
	switch obj.GetKind() {
	case "PersistentVolumeClaim":
		var o corev1.PersistentVolumeClaim
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &o); err != nil {
			return err
		}
		_, err := kube.CoreV1().PersistentVolumeClaims(o.Namespace).Create(ctx, &o, metav1.CreateOptions{})
		return err
	case "ConfigMap":
		var o corev1.ConfigMap
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &o); err != nil {
			return err
		}
		_, err := kube.CoreV1().ConfigMaps(o.Namespace).Create(ctx, &o, metav1.CreateOptions{})
		return err
	case "Secret":
		var o corev1.Secret
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &o); err != nil {
			return err
		}
		_, err := kube.CoreV1().Secrets(o.Namespace).Create(ctx, &o, metav1.CreateOptions{})
		return err
	case "Service":
		var o corev1.Service
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &o); err != nil {
			return err
		}
		_, err := kube.CoreV1().Services(o.Namespace).Create(ctx, &o, metav1.CreateOptions{})
		return err
	case "StatefulSet":
		var o appsv1.StatefulSet
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &o); err != nil {
			return err
		}
		_, err := kube.AppsV1().StatefulSets(o.Namespace).Create(ctx, &o, metav1.CreateOptions{})
		return err
	case "Deployment":
		var o appsv1.Deployment
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &o); err != nil {
			return err
		}
		_, err := kube.AppsV1().Deployments(o.Namespace).Create(ctx, &o, metav1.CreateOptions{})
		return err
	default:
		return nil
	}
}
