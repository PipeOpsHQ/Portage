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
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

func TestTypedAppliesSTSToDest(t *testing.T) {
	t.Parallel()
	kube := k8sfake.NewSimpleClientset()
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "pg", Namespace: "ns"}}
	m, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(sts)
	u := &unstructured.Unstructured{Object: m}
	u.SetKind("StatefulSet")
	u.SetAPIVersion("apps/v1")
	if err := Typed(context.Background(), kube, []*unstructured.Unstructured{u}); err != nil {
		t.Fatal(err)
	}
	got, err := kube.AppsV1().StatefulSets("ns").Get(context.Background(), "pg", metav1.GetOptions{})
	if err != nil || got.Name != "pg" {
		t.Fatalf("dest missing STS: %v", err)
	}
}

func TestTypedSkipsBoundPVC(t *testing.T) {
	t.Parallel()
	existing := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data-pg", Namespace: "ns"},
		Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: "pvc-live"},
		Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	kube := k8sfake.NewSimpleClientset(existing)
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "PersistentVolumeClaim",
		"metadata": map[string]any{"name": "data-pg", "namespace": "ns"},
		"spec":     map[string]any{"volumeName": ""},
	}}
	if err := Typed(context.Background(), kube, []*unstructured.Unstructured{u}); err != nil {
		t.Fatal(err)
	}
	got, err := kube.CoreV1().PersistentVolumeClaims("ns").Get(context.Background(), "data-pg", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Spec.VolumeName != "pvc-live" {
		t.Fatalf("bound volumeName=%q", got.Spec.VolumeName)
	}
}

func TestTypedUpdatesExistingSTS(t *testing.T) {
	t.Parallel()
	kube := k8sfake.NewSimpleClientset(&appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "pg", Namespace: "ns"},
		Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{NodeSelector: map[string]string{"kubernetes.io/hostname": "src-node"}},
		}},
	})
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1", "kind": "StatefulSet",
		"metadata": map[string]any{"name": "pg", "namespace": "ns"},
		"spec": map[string]any{
			"template": map[string]any{
				"spec": map[string]any{"containers": []any{map[string]any{"name": "pg", "image": "pg:17"}}},
			},
		},
	}}
	if err := Typed(context.Background(), kube, []*unstructured.Unstructured{u}); err != nil {
		t.Fatal(err)
	}
	got, err := kube.AppsV1().StatefulSets("ns").Get(context.Background(), "pg", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Spec.Template.Spec.NodeSelector) != 0 {
		t.Fatalf("nodeSelector=%v", got.Spec.Template.Spec.NodeSelector)
	}
}
