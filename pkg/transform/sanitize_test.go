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

func TestObjectStripsUnmappedRuntimeClass(t *testing.T) {
	t.Parallel()
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "StatefulSet",
		"metadata":   map[string]any{"name": "redis"},
		"spec": map[string]any{
			"template": map[string]any{
				"spec": map[string]any{
					"runtimeClassName": "gvisor",
					"containers":       []any{map[string]any{"name": "redis"}},
				},
			},
		},
	}}
	Object(obj, Options{})
	if _, found, _ := unstructured.NestedString(obj.Object, "spec", "template", "spec", "runtimeClassName"); found {
		t.Fatal("unmapped runtimeClassName must be stripped")
	}
}

func TestObjectRemapsRuntimeClass(t *testing.T) {
	t.Parallel()
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata":   map[string]any{"name": "app"},
		"spec": map[string]any{
			"template": map[string]any{
				"spec": map[string]any{
					"runtimeClassName": "gvisor",
					"containers":       []any{map[string]any{"name": "app"}},
				},
			},
		},
	}}
	Object(obj, Options{RuntimeClassMap: map[string]string{"gvisor": "kata-vm-isolation"}})
	got, found, _ := unstructured.NestedString(obj.Object, "spec", "template", "spec", "runtimeClassName")
	if !found || got != "kata-vm-isolation" {
		t.Fatalf("runtimeClassName=%q found=%v", got, found)
	}
}

func TestObjectStripsMappedEmptyRuntimeClass(t *testing.T) {
	t.Parallel()
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata":   map[string]any{"name": "p"},
		"spec": map[string]any{
			"runtimeClassName": "nvidia",
			"containers":       []any{map[string]any{"name": "c"}},
		},
	}}
	Object(obj, Options{RuntimeClassMap: map[string]string{"nvidia": ""}})
	if _, found, _ := unstructured.NestedString(obj.Object, "spec", "runtimeClassName"); found {
		t.Fatal("empty mapping must strip runtimeClassName")
	}
}

func TestObjectRemapsCronJobRuntimeClass(t *testing.T) {
	t.Parallel()
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "batch/v1",
		"kind":       "CronJob",
		"metadata":   map[string]any{"name": "job"},
		"spec": map[string]any{
			"jobTemplate": map[string]any{
				"spec": map[string]any{
					"template": map[string]any{
						"spec": map[string]any{
							"runtimeClassName": "wasmtime",
							"containers":       []any{map[string]any{"name": "job"}},
						},
					},
				},
			},
		},
	}}
	Object(obj, Options{RuntimeClassMap: map[string]string{"wasmtime": "runc"}})
	got, found, _ := unstructured.NestedString(obj.Object, "spec", "jobTemplate", "spec", "template", "spec", "runtimeClassName")
	if !found || got != "runc" {
		t.Fatalf("cronjob runtimeClassName=%q found=%v", got, found)
	}
}
