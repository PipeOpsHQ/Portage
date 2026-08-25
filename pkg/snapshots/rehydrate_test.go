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

package snapshots

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynfake "k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	"github.com/PipeOpsHQ/portage/pkg/classify"
)

func TestEnsurePVCFromSnapshotCreatesByName(t *testing.T) {
	t.Parallel()
	w := classify.Workload{Namespace: "ns", Name: "pg", Kind: "StatefulSet", PVCNames: []string{"data-pg"}}
	snap := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "snapshot.storage.k8s.io/v1",
		"kind":       "VolumeSnapshot",
		"metadata": map[string]any{
			"name":      "snap-1",
			"namespace": "ns",
			"labels": map[string]any{
				labelName: "pg", labelKind: "StatefulSet",
			},
		},
		"status": map[string]any{"readyToUse": true, "restoreSize": "8Gi"},
	}}
	snap.SetGroupVersionKind(schema.GroupVersionKind{Group: "snapshot.storage.k8s.io", Version: "v1", Kind: "VolumeSnapshot"})
	scheme := runtime.NewScheme()
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		snapshotGVR: "VolumeSnapshotList",
	}, snap)
	kube := k8sfake.NewSimpleClientset()
	c := Client{Dynamic: dyn}
	done, healed, err := c.EnsurePVCFromSnapshot(context.Background(), kube, w, "data-pg", RehydrateOptions{StorageClass: "standard-csi"})
	if err != nil {
		t.Fatal(err)
	}
	if done {
		t.Fatal("create is not Bound yet")
	}
	if len(healed) == 0 {
		t.Fatal("expected bind-pvc-by-name")
	}
	pvc, err := kube.CoreV1().PersistentVolumeClaims("ns").Get(context.Background(), "data-pg", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if pvc.Spec.DataSource == nil || pvc.Spec.DataSource.Name != "snap-1" {
		t.Fatalf("datasource=%v", pvc.Spec.DataSource)
	}
	if pvc.Name != "data-pg" {
		t.Fatal("must restore into original PVC name")
	}
}

func TestEnsurePVCFromSnapshotNeverOverwritesBound(t *testing.T) {
	t.Parallel()
	w := classify.Workload{Namespace: "ns", Name: "pg", Kind: "StatefulSet"}
	existing := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data-pg", Namespace: "ns"},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("8Gi")},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	kube := k8sfake.NewSimpleClientset(existing)
	c := Client{}
	done, _, err := c.EnsurePVCFromSnapshot(context.Background(), kube, w, "data-pg", RehydrateOptions{NeverOverwrite: true})
	if err != nil || !done {
		t.Fatalf("done=%v err=%v", done, err)
	}
}
