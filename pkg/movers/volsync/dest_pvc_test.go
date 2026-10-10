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

package volsync

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

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/classify"
)

func TestEnsureDestPVCCopiesSizeAndRemapsStorageClass(t *testing.T) {
	t.Parallel()
	sc := "premium-rwo"
	srcPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "redis-data-redis-0", Namespace: "ns"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &sc,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("8Gi")},
			},
			VolumeName: "pvc-src-vol",
		},
	}
	src := k8sfake.NewSimpleClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns"}}, srcPVC)
	dst := k8sfake.NewSimpleClientset()
	m := Mover{
		Kube: src, DestKube: dst,
		StorageClassMap: map[string]string{"premium-rwo": "standard-csi"},
	}
	if err := m.ensureDestPVC(context.Background(), "ns", "redis-data-redis-0"); err != nil {
		t.Fatal(err)
	}
	got, err := dst.CoreV1().PersistentVolumeClaims("ns").Get(context.Background(), "redis-data-redis-0", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Spec.VolumeName != "" {
		t.Fatal("dest PVC must not pin source volumeName")
	}
	if got.Spec.StorageClassName == nil || *got.Spec.StorageClassName != "standard-csi" {
		t.Fatalf("storageClass=%v", got.Spec.StorageClassName)
	}
	if q := got.Spec.Resources.Requests[corev1.ResourceStorage]; q.Cmp(resource.MustParse("8Gi")) != 0 {
		t.Fatalf("size=%s", q.String())
	}
}

func TestReplicateCreatesDestPVCBeforeDestinationCR(t *testing.T) {
	t.Parallel()
	sc := "longhorn"
	srcKube := k8sfake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns"}},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "data-pg-0", Namespace: "ns"},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				StorageClassName: &sc,
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
				},
			},
		},
	)
	dstKube := k8sfake.NewSimpleClientset()
	dynKinds := map[schema.GroupVersionResource]string{
		srcGVR: "ReplicationSourceList",
		dstGVR: "ReplicationDestinationList",
	}
	srcDyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), dynKinds)
	dstDyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), dynKinds)
	m := Mover{
		Dynamic: srcDyn, DestDynamic: dstDyn,
		Kube: srcKube, DestKube: dstKube,
		Transport:       portagev1alpha1.TransportObjectStore,
		StorageClassMap: map[string]string{"longhorn": "local-path"},
	}
	w := classify.Workload{Namespace: "ns", Name: "pg", PVCNames: []string{"data-pg-0"}}
	replicateUntilDest(t, m, w)
	if _, err := dstKube.CoreV1().PersistentVolumeClaims("ns").Get(context.Background(), "data-pg-0", metav1.GetOptions{}); err != nil {
		t.Fatalf("dest PVC must exist before VolSync destinationPVC: %v", err)
	}
	rd, err := dstDyn.Resource(dstGVR).Namespace("ns").Get(context.Background(), "portage-pg", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	destPVC, _, _ := unstructured.NestedString(rd.Object, "spec", "restic", "destinationPVC")
	if destPVC != "data-pg-0" {
		t.Fatalf("destinationPVC=%q", destPVC)
	}
}

func TestEnsureDestPVCErrorsWhenSourceClaimMissing(t *testing.T) {
	t.Parallel()
	src := k8sfake.NewSimpleClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns"}})
	dst := k8sfake.NewSimpleClientset()
	m := Mover{Kube: src, DestKube: dst}
	err := m.ensureDestPVC(context.Background(), "ns", "redis-data")
	if err == nil {
		t.Fatal("bare template name that is not a PVC must fail")
	}
}
