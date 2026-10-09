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

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynfake "k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/classify"
	"github.com/PipeOpsHQ/portage/pkg/movers"
)

func TestProtectScratchPVCsMarksK8upAndVelero(t *testing.T) {
	t.Parallel()
	ns := "adequate-knov-beta"
	cache := "volsync-portage-hidden-surf-adequate-knov-beta-cache"
	data := "data-hidden-surf-0"
	kube := k8sfake.NewSimpleClientset(
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: data, Namespace: ns}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: cache, Namespace: ns}},
	)
	if err := ProtectScratchPVCs(context.Background(), kube, ns); err != nil {
		t.Fatal(err)
	}
	got, err := kube.CoreV1().PersistentVolumeClaims(ns).Get(context.Background(), cache, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Annotations[k8upBackupAnn] != "false" {
		t.Fatalf("k8up annotation=%q", got.Annotations[k8upBackupAnn])
	}
	if got.Annotations[k8upLegacyAnn] != "false" || got.Annotations[k8upAppuioAnn] != "false" {
		t.Fatalf("legacy k8up annotations=%v", got.Annotations)
	}
	if got.Labels[veleroExcludeLbl] != "true" {
		t.Fatalf("velero label=%q", got.Labels[veleroExcludeLbl])
	}
	if got.Labels[portageScratchLbl] != "true" {
		t.Fatalf("portage scratch label=%q", got.Labels[portageScratchLbl])
	}
	user, err := kube.CoreV1().PersistentVolumeClaims(ns).Get(context.Background(), data, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if user.Annotations[k8upBackupAnn] != "" {
		t.Fatal("user data PVC must stay backupable")
	}
	if user.Labels[veleroExcludeLbl] != "" {
		t.Fatal("user data PVC must stay backupable")
	}
}

func TestProtectScratchPVCsEvictsK8upJob(t *testing.T) {
	t.Parallel()
	ns := "adequate-knov-beta"
	cache := "volsync-portage-restless-pond-adequate-knov-beta-cache"
	jobName := "backup-pipeops-backup-adequate-knov-beta-123-0"
	kube := k8sfake.NewSimpleClientset(
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: cache, Namespace: ns}},
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: ns}},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      jobName,
				Namespace: ns,
				Labels:    map[string]string{"k8up.io/type": "backup"},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "batch/v1",
					Kind:       "Job",
					Name:       jobName,
				}},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name:  "backup",
					Image: "ghcr.io/k8up-io/k8up:v2.11.0",
				}},
				Volumes: []corev1.Volume{{
					Name: "pvc",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: cache},
					},
				}},
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "volsync-dst-portage-restless-pond-adequate-knov-beta-abc",
				Namespace: ns,
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "mover", Image: "quay.io/backube/volsync:0.12.0"}},
				Volumes: []corev1.Volume{{
					Name: "cache",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: cache},
					},
				}},
			},
		},
	)
	if err := ProtectScratchPVCs(context.Background(), kube, ns); err != nil {
		t.Fatal(err)
	}
	if _, err := kube.BatchV1().Jobs(ns).Get(context.Background(), jobName, metav1.GetOptions{}); err == nil {
		t.Fatal("k8up backup Job must be deleted so the dest mover can attach")
	}
	if _, err := kube.CoreV1().Pods(ns).Get(context.Background(), "volsync-dst-portage-restless-pond-adequate-knov-beta-abc", metav1.GetOptions{}); err != nil {
		t.Fatalf("dest mover pod must stay: %v", err)
	}
}

func TestReplicateProtectsDestCachePVCs(t *testing.T) {
	t.Parallel()
	ns := "adequate-knov-beta"
	cache := "volsync-portage-hidden-surf-adequate-knov-beta-cache"
	srcKube := k8sfake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-pg-0", Namespace: ns}},
	)
	dstKube := k8sfake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: cache, Namespace: ns}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-pg-0", Namespace: ns}},
	)
	scheme := runtime.NewScheme()
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		srcGVR: "ReplicationSourceList",
		dstGVR: "ReplicationDestinationList",
	})
	dstDyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		srcGVR: "ReplicationSourceList",
		dstGVR: "ReplicationDestinationList",
	})
	m := Mover{
		Dynamic:     dyn,
		DestDynamic: dstDyn,
		Kube:        srcKube,
		DestKube:    dstKube,
		Transport:   portagev1alpha1.TransportObjectStore,
	}
	w := classify.Workload{Namespace: ns, Name: "pg", PVCNames: []string{"data-pg-0"}}
	if err := m.Replicate(context.Background(), w, movers.ClusterHandle{}, movers.ClusterHandle{}); err != nil {
		t.Fatal(err)
	}
	got, err := dstKube.CoreV1().PersistentVolumeClaims(ns).Get(context.Background(), cache, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Annotations[k8upBackupAnn] != "false" || got.Labels[veleroExcludeLbl] != "true" {
		t.Fatalf("dest cache unmarked: ann=%v lbl=%v", got.Annotations, got.Labels)
	}
	user, err := dstKube.CoreV1().PersistentVolumeClaims(ns).Get(context.Background(), "data-pg-0", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if user.Annotations[k8upBackupAnn] != "" || user.Labels[veleroExcludeLbl] != "" {
		t.Fatal("dest data PVC must stay backupable")
	}
}

func TestReplicateSkipsLegacyVolSyncCachePVC(t *testing.T) {
	t.Parallel()
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		srcGVR: "ReplicationSourceList",
		dstGVR: "ReplicationDestinationList",
	})
	m := Mover{Dynamic: dyn, Transport: portagev1alpha1.TransportObjectStore}
	w := classify.Workload{
		Namespace: "files",
		Name:      "volsync-portage-hidden-surf-adequate-knov-beta-cache",
		PVCNames:  []string{"volsync-portage-hidden-surf-adequate-knov-beta-cache"},
	}
	if err := m.Replicate(context.Background(), w, movers.ClusterHandle{}, movers.ClusterHandle{}); err != nil {
		t.Fatal(err)
	}
	list, err := dyn.Resource(srcGVR).Namespace("files").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 0 {
		t.Fatalf("legacy cache PVC must not get a ReplicationSource, got %d", len(list.Items))
	}
}

func TestFirstDataPVCSkipsLegacyCache(t *testing.T) {
	t.Parallel()
	got := firstDataPVC([]string{
		"volsync-portage-hidden-surf-adequate-knov-beta-cache",
		"data-pg-0",
	})
	if got != "data-pg-0" {
		t.Fatalf("got %q", got)
	}
}
