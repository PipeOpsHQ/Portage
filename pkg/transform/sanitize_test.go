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

package transform

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/ptr"
)

func TestPVCStripsTopologyAndRemapsSC(t *testing.T) {
	t.Parallel()
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "data",
			Annotations: map[string]string{
				"volume.kubernetes.io/selected-node":                "gke-node-1",
				"pv.kubernetes.io/bind-completed":                   "yes",
				"app.kubernetes.io/name":                            "keep-me",
				"service.beta.kubernetes.io/aws-load-balancer-type": "nlb",
			},
			Labels: map[string]string{
				"app":                         "pg",
				"topology.kubernetes.io/zone": "europe-west2-b",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: ptr.To("premium-rwo"),
			VolumeName:       "pvc-deadbeef",
		},
	}
	PVC(pvc, Options{StorageClassMap: map[string]string{"premium-rwo": "standard-csi"}})
	if pvc.Spec.VolumeName != "" {
		t.Fatalf("volumeName should be cleared, got %q", pvc.Spec.VolumeName)
	}
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != "standard-csi" {
		t.Fatalf("storageClassName=%v", pvc.Spec.StorageClassName)
	}
	if _, ok := pvc.Annotations["volume.kubernetes.io/selected-node"]; ok {
		t.Fatal("selected-node should be dropped")
	}
	if pvc.Annotations["app.kubernetes.io/name"] != "keep-me" {
		t.Fatal("app annotation should be kept")
	}
	if _, ok := pvc.Labels["topology.kubernetes.io/zone"]; ok {
		t.Fatal("zone label should be dropped")
	}
}

func TestObjectStripsNodeAffinityAndStatus(t *testing.T) {
	t.Parallel()
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "StatefulSet",
		"metadata": map[string]any{
			"name":            "pg",
			"uid":             "abc",
			"resourceVersion": "9",
		},
		"spec": map[string]any{
			"template": map[string]any{
				"spec": map[string]any{
					"nodeSelector": map[string]any{"topology.kubernetes.io/zone": "a"},
					"containers":   []any{map[string]any{"name": "pg"}},
				},
			},
		},
		"status": map[string]any{"readyReplicas": int64(1)},
	}}
	Object(obj, Options{})
	if obj.GetUID() != "" || obj.GetResourceVersion() != "" {
		t.Fatal("identity fields must be cleared")
	}
	if _, found, _ := unstructured.NestedFieldNoCopy(obj.Object, "status"); found {
		t.Fatal("status must be removed")
	}
	if _, found, _ := unstructured.NestedFieldNoCopy(obj.Object, "spec", "template", "spec", "nodeSelector"); found {
		t.Fatal("nodeSelector must be removed")
	}
}
