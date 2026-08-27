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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynfake "k8s.io/client-go/dynamic/fake"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/classify"
	"github.com/PipeOpsHQ/portage/pkg/movers"
)

func TestReplicateObjectStoreUsesResticIncremental(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		srcGVR: "ReplicationSourceList",
		dstGVR: "ReplicationDestinationList",
	})
	m := Mover{Dynamic: dyn, Transport: portagev1alpha1.TransportObjectStore, DestPath: "s3://bucket/ns/pg"}
	w := classify.Workload{Namespace: "ns", Name: "pg", Kind: "StatefulSet", PVCNames: []string{"data-pg"}, Class: portagev1alpha1.ClassSQLLogical}
	if err := m.Replicate(context.Background(), w, movers.ClusterHandle{}, movers.ClusterHandle{}); err != nil {
		t.Fatal(err)
	}
	src, err := dyn.Resource(srcGVR).Namespace("ns").Get(context.Background(), "portage-pg", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	repo, _, _ := unstructured.NestedString(src.Object, "spec", "restic", "repository")
	if repo != resticSecretName {
		t.Fatalf("restic repository=%q", repo)
	}
	cm, _, _ := unstructured.NestedString(src.Object, "spec", "restic", "copyMethod")
	if cm != "Direct" {
		t.Fatalf("copyMethod=%q want Direct (kind-safe)", cm)
	}
	dst, err := dyn.Resource(dstGVR).Namespace("ns").Get(context.Background(), "portage-pg", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	sched, _, _ := unstructured.NestedString(dst.Object, "spec", "trigger", "schedule")
	if sched == "" {
		t.Fatal("dest must schedule-pull; manual trigger is the live-sync hole")
	}
	manual, _, _ := unstructured.NestedString(src.Object, "spec", "trigger", "manual")
	if manual == "" {
		t.Fatal("source must fire immediately (manual trigger), not wait for cron")
	}
}

func TestReplicateObjectStoreRcloneOverride(t *testing.T) {
	t.Parallel()
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		srcGVR: "ReplicationSourceList",
		dstGVR: "ReplicationDestinationList",
	})
	m := Mover{Dynamic: dyn, Transport: portagev1alpha1.TransportObjectStore, DestPath: "s3://bucket/ns/pg", ObjectMover: "rclone"}
	w := classify.Workload{Namespace: "ns", Name: "pg", PVCNames: []string{"data-pg"}}
	if err := m.Replicate(context.Background(), w, movers.ClusterHandle{}, movers.ClusterHandle{}); err != nil {
		t.Fatal(err)
	}
	obj, err := dyn.Resource(srcGVR).Namespace("ns").Get(context.Background(), "portage-pg", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	path, _, _ := unstructured.NestedString(obj.Object, "spec", "rclone", "rcloneDestPath")
	if path != "s3://bucket/ns/pg" {
		t.Fatalf("rclone path=%q", path)
	}
}

func TestReplicateWritesDestCROnDestClient(t *testing.T) {
	t.Parallel()
	kinds := map[schema.GroupVersionResource]string{
		srcGVR: "ReplicationSourceList",
		dstGVR: "ReplicationDestinationList",
	}
	src := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), kinds)
	dst := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), kinds)
	m := Mover{Dynamic: src, DestDynamic: dst, Transport: portagev1alpha1.TransportObjectStore, DestPath: "s3://b/p"}
	w := classify.Workload{Namespace: "ns", Name: "pg", PVCNames: []string{"data-pg"}}
	if err := m.Replicate(context.Background(), w, movers.ClusterHandle{}, movers.ClusterHandle{}); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Resource(srcGVR).Namespace("ns").Get(context.Background(), "portage-pg", metav1.GetOptions{}); err != nil {
		t.Fatalf("source CR: %v", err)
	}
	if _, err := src.Resource(dstGVR).Namespace("ns").Get(context.Background(), "portage-pg", metav1.GetOptions{}); err == nil {
		t.Fatal("destination CR must not live on the source client")
	}
	if _, err := dst.Resource(dstGVR).Namespace("ns").Get(context.Background(), "portage-pg", metav1.GetOptions{}); err != nil {
		t.Fatalf("dest CR: %v", err)
	}
}

func TestProbeRequiresLastSyncTimeOnBothSides(t *testing.T) {
	t.Parallel()
	kinds := map[schema.GroupVersionResource]string{
		srcGVR: "ReplicationSourceList",
		dstGVR: "ReplicationDestinationList",
	}
	src := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), kinds)
	dst := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), kinds)
	m := Mover{Dynamic: src, DestDynamic: dst}
	w := classify.Workload{Namespace: "ns", Name: "pg", PVCNames: []string{"data-pg"}}
	if err := m.Replicate(context.Background(), w, movers.ClusterHandle{}, movers.ClusterHandle{}); err != nil {
		t.Fatal(err)
	}
	pr, _ := m.Probe(context.Background(), w, movers.ClusterHandle{})
	if pr.OK {
		t.Fatal("probe must fail before lastSyncTime")
	}
	markSync := func(c dynamic.Interface, gvr schema.GroupVersionResource) {
		obj, err := c.Resource(gvr).Namespace("ns").Get(context.Background(), "portage-pg", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		_ = unstructured.SetNestedField(obj.Object, "2026-08-25T12:00:00Z", "status", "lastSyncTime")
		if _, err := c.Resource(gvr).Namespace("ns").UpdateStatus(context.Background(), obj, metav1.UpdateOptions{}); err != nil {
			if _, err = c.Resource(gvr).Namespace("ns").Update(context.Background(), obj, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
		}
	}
	markSync(src, srcGVR)
	markSync(dst, dstGVR)
	pr, _ = m.Probe(context.Background(), w, movers.ClusterHandle{})
	if !pr.OK {
		t.Fatalf("probe after sync: %+v", pr)
	}
}

func TestReplicateDirectUsesRsyncTLS(t *testing.T) {
	t.Parallel()
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		srcGVR: "ReplicationSourceList",
		dstGVR: "ReplicationDestinationList",
	})
	m := Mover{Dynamic: dyn, Transport: portagev1alpha1.TransportDirect}
	w := classify.Workload{Namespace: "ns", Name: "pg", Kind: "StatefulSet", PVCNames: []string{"data-pg"}}
	if err := m.Replicate(context.Background(), w, movers.ClusterHandle{}, movers.ClusterHandle{}); err != nil {
		t.Fatal(err)
	}
	obj, err := dyn.Resource(srcGVR).Namespace("ns").Get(context.Background(), "portage-pg", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, found, _ := unstructured.NestedFieldNoCopy(obj.Object, "spec", "rsyncTLS")
	if !found {
		t.Fatal("expected rsyncTLS")
	}
}
