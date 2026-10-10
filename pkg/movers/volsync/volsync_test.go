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
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynfake "k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/classify"
	"github.com/PipeOpsHQ/portage/pkg/movers"
)

// replicateUntilDest runs Replicate, and for restic stamps a source snapshot
// so the destination CR is created. Dest restic is held until lastSyncTime.
func replicateUntilDest(t *testing.T, m Mover, w classify.Workload) {
	t.Helper()
	ctx := context.Background()
	if err := m.Replicate(ctx, w, movers.ClusterHandle{}, movers.ClusterHandle{}); err != nil {
		t.Fatal(err)
	}
	if !m.resticCopy() {
		return
	}
	name := "portage-" + w.Name
	obj, err := m.Dynamic.Resource(srcGVR).Namespace(w.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedField(obj.Object, "2026-10-09T00:00:00Z", "status", "lastSyncTime"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Dynamic.Resource(srcGVR).Namespace(w.Namespace).Update(ctx, obj, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := m.Replicate(ctx, w, movers.ClusterHandle{}, movers.ClusterHandle{}); err != nil {
		t.Fatal(err)
	}
}

func TestReplicateObjectStoreUsesResticIncremental(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		srcGVR: "ReplicationSourceList",
		dstGVR: "ReplicationDestinationList",
	})
	m := Mover{Dynamic: dyn, Transport: portagev1alpha1.TransportObjectStore, DestPath: "s3://bucket/ns/pg"}
	w := classify.Workload{Namespace: "ns", Name: "pg", Kind: "StatefulSet", PVCNames: []string{"data-pg"}, Class: portagev1alpha1.ClassSQLLogical}
	replicateUntilDest(t, m, w)
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
	manual, found, _ := unstructured.NestedString(src.Object, "spec", "trigger", "manual")
	if found && manual != "" {
		t.Fatal("manual+schedule makes VolSync ignore the cron; incrementals never fire")
	}
	if _, found, _ := unstructured.NestedFieldNoCopy(dst.Object, "spec", "restic", "pruneIntervalDays"); found {
		t.Fatal("dest restic must not set source-only pruneIntervalDays")
	}
	destPVC, _, _ := unstructured.NestedString(dst.Object, "spec", "restic", "destinationPVC")
	if destPVC != "data-pg" {
		t.Fatalf("dest destinationPVC=%q", destPVC)
	}
	tols, found, _ := unstructured.NestedSlice(dst.Object, "spec", "restic", "moverTolerations")
	if !found || tols == nil {
		t.Fatal("dest restic must set empty moverTolerations so Direct copy does not inherit source gvisor/hostname pins")
	}
	srcTols, srcFound, _ := unstructured.NestedSlice(src.Object, "spec", "restic", "moverTolerations")
	if srcFound && srcTols != nil {
		t.Fatal("source restic must keep VolSync's AffinityFromVolume (RWO on source node)")
	}
}

func TestReplicateHoldsDestUntilSourceSnapshot(t *testing.T) {
	t.Parallel()
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		srcGVR: "ReplicationSourceList",
		dstGVR: "ReplicationDestinationList",
	})
	m := Mover{Dynamic: dyn, Transport: portagev1alpha1.TransportObjectStore, DestPath: "s3://bucket/e2e"}
	w := classify.Workload{Namespace: "files", Name: "data", PVCNames: []string{"data"}}
	if err := m.Replicate(context.Background(), w, movers.ClusterHandle{}, movers.ClusterHandle{}); err != nil {
		t.Fatal(err)
	}
	if _, err := dyn.Resource(srcGVR).Namespace("files").Get(context.Background(), "portage-data", metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := dyn.Resource(dstGVR).Namespace("files").Get(context.Background(), "portage-data", metav1.GetOptions{}); err == nil {
		t.Fatal("dest restic must wait until the source backup has lastSyncTime")
	}
	replicateUntilDest(t, m, w)
	if _, err := dyn.Resource(dstGVR).Namespace("files").Get(context.Background(), "portage-data", metav1.GetOptions{}); err != nil {
		t.Fatalf("dest after source snapshot: %v", err)
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
	replicateUntilDest(t, m, w)
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

func TestReplicateSkipsVolSyncCachePVC(t *testing.T) {
	t.Parallel()
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		srcGVR: "ReplicationSourceList",
		dstGVR: "ReplicationDestinationList",
	})
	m := Mover{Dynamic: dyn, Transport: portagev1alpha1.TransportObjectStore}
	w := classify.Workload{Namespace: "files", Name: "volsync-src-portage-data-cache", PVCNames: []string{"volsync-src-portage-data-cache"}}
	if err := m.Replicate(context.Background(), w, movers.ClusterHandle{}, movers.ClusterHandle{}); err != nil {
		t.Fatal(err)
	}
	list, err := dyn.Resource(srcGVR).Namespace("files").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 0 {
		t.Fatalf("cache PVC must not get a ReplicationSource, got %d", len(list.Items))
	}
}

func TestResticMoverSecurityContextFromWorkload(t *testing.T) {
	t.Parallel()
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		srcGVR: "ReplicationSourceList",
		dstGVR: "ReplicationDestinationList",
	})
	fs, uid := int64(999), int64(999)
	m := Mover{Dynamic: dyn, Transport: portagev1alpha1.TransportObjectStore}
	w := classify.Workload{Namespace: "ns", Name: "pg", PVCNames: []string{"data-pg-0"}, FSGroup: &fs, RunAsUser: &uid}
	if err := m.Replicate(context.Background(), w, movers.ClusterHandle{}, movers.ClusterHandle{}); err != nil {
		t.Fatal(err)
	}
	src, err := dyn.Resource(srcGVR).Namespace("ns").Get(context.Background(), "portage-pg", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	g, _, _ := unstructured.NestedInt64(src.Object, "spec", "restic", "moverSecurityContext", "fsGroup")
	if g != 999 {
		t.Fatalf("fsGroup=%d", g)
	}
}

func TestDestResticOmitsRootFSGroup(t *testing.T) {
	t.Parallel()
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		srcGVR: "ReplicationSourceList",
		dstGVR: "ReplicationDestinationList",
	})
	zero := int64(0)
	m := Mover{Dynamic: dyn, Transport: portagev1alpha1.TransportObjectStore}
	w := classify.Workload{
		Namespace: "ns", Name: "pg", PVCNames: []string{"data-pg-0"},
		Class: portagev1alpha1.ClassSQLLogical, Engine: "postgres", FSGroup: &zero,
	}
	replicateUntilDest(t, m, w)
	dst, err := dyn.Resource(dstGVR).Namespace("ns").Get(context.Background(), "portage-pg", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, found, _ := unstructured.NestedInt64(dst.Object, "spec", "restic", "moverSecurityContext", "fsGroup"); found {
		t.Fatal("dest restic must not set fsGroup (kubelet ORs 0660 and Postgres rejects server.key 0600)")
	}
	uid, found, _ := unstructured.NestedInt64(dst.Object, "spec", "restic", "moverSecurityContext", "runAsUser")
	if !found || uid != 999 {
		t.Fatalf("dest runAsUser=%d found=%v want 999", uid, found)
	}
	src, err := dyn.Resource(srcGVR).Namespace("ns").Get(context.Background(), "portage-pg", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	sg, _, _ := unstructured.NestedInt64(src.Object, "spec", "restic", "moverSecurityContext", "fsGroup")
	if sg != 0 {
		t.Fatalf("source may keep declared fsGroup 0, got %d", sg)
	}
}

func TestResticPrivilegedMoversWhenFSGroupZero(t *testing.T) {
	t.Parallel()
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		srcGVR: "ReplicationSourceList",
		dstGVR: "ReplicationDestinationList",
	})
	kube := k8sfake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns"}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-pg-0", Namespace: "ns"}},
	)
	zero := int64(0)
	m := Mover{Dynamic: dyn, Kube: kube, Transport: portagev1alpha1.TransportObjectStore}
	w := classify.Workload{Namespace: "ns", Name: "pg", PVCNames: []string{"data-pg-0"}, FSGroup: &zero}
	if err := m.Replicate(context.Background(), w, movers.ClusterHandle{}, movers.ClusterHandle{}); err != nil {
		t.Fatal(err)
	}
	ns, err := kube.CoreV1().Namespaces().Get(context.Background(), "ns", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if ns.Annotations[privilegedMoversAnnotation] != "true" {
		t.Fatal("fsGroup 0 is root; restic still needs DAC_OVERRIDE to read mode 700 PGDATA")
	}
}

func TestResticPrivilegedMoversWhenNoFSGroup(t *testing.T) {
	t.Parallel()
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		srcGVR: "ReplicationSourceList",
		dstGVR: "ReplicationDestinationList",
	})
	kube := k8sfake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns"}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-pg-0", Namespace: "ns"}},
	)
	m := Mover{Dynamic: dyn, Kube: kube, Transport: portagev1alpha1.TransportObjectStore}
	w := classify.Workload{Namespace: "ns", Name: "pg", PVCNames: []string{"data-pg-0"}}
	if err := m.Replicate(context.Background(), w, movers.ClusterHandle{}, movers.ClusterHandle{}); err != nil {
		t.Fatal(err)
	}
	ns, err := kube.CoreV1().Namespaces().Get(context.Background(), "ns", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if ns.Annotations[privilegedMoversAnnotation] != "true" {
		t.Fatalf("annotations=%v", ns.Annotations)
	}
}

func TestResticNoPrivilegedMoversWhenFSGroupSet(t *testing.T) {
	t.Parallel()
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		srcGVR: "ReplicationSourceList",
		dstGVR: "ReplicationDestinationList",
	})
	kube := k8sfake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns"}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-pg-0", Namespace: "ns"}},
	)
	fs := int64(999)
	m := Mover{Dynamic: dyn, Kube: kube, Transport: portagev1alpha1.TransportObjectStore}
	w := classify.Workload{Namespace: "ns", Name: "pg", PVCNames: []string{"data-pg-0"}, FSGroup: &fs}
	if err := m.Replicate(context.Background(), w, movers.ClusterHandle{}, movers.ClusterHandle{}); err != nil {
		t.Fatal(err)
	}
	ns, err := kube.CoreV1().Namespaces().Get(context.Background(), "ns", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if ns.Annotations[privilegedMoversAnnotation] == "true" {
		t.Fatal("fsGroup workloads must not elevate the namespace")
	}
}

func TestResticPrivilegedMoversOnDestNamespace(t *testing.T) {
	t.Parallel()
	kinds := map[schema.GroupVersionResource]string{
		srcGVR: "ReplicationSourceList",
		dstGVR: "ReplicationDestinationList",
	}
	srcDyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), kinds)
	dstDyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), kinds)
	srcKube := k8sfake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns"}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-pg-0", Namespace: "ns"}},
	)
	dstKube := k8sfake.NewSimpleClientset()
	m := Mover{
		Dynamic: srcDyn, DestDynamic: dstDyn,
		Kube: srcKube, DestKube: dstKube,
		Transport: portagev1alpha1.TransportObjectStore,
	}
	w := classify.Workload{Namespace: "ns", Name: "pg", PVCNames: []string{"data-pg-0"}}
	if err := m.Replicate(context.Background(), w, movers.ClusterHandle{}, movers.ClusterHandle{}); err != nil {
		t.Fatal(err)
	}
	ns, err := dstKube.CoreV1().Namespaces().Get(context.Background(), "ns", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if ns.Annotations[privilegedMoversAnnotation] != "true" {
		t.Fatalf("dest annotations=%v", ns.Annotations)
	}
}

func TestDestDynNilWhenRemoteWithoutDestDynamic(t *testing.T) {
	t.Parallel()
	srcKube := k8sfake.NewSimpleClientset(
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-pg-0", Namespace: "ns"}},
	)
	dstKube := k8sfake.NewSimpleClientset()
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		srcGVR: "ReplicationSourceList",
		dstGVR: "ReplicationDestinationList",
	})
	m := Mover{Dynamic: dyn, Kube: srcKube, DestKube: dstKube, Transport: portagev1alpha1.TransportObjectStore}
	if m.destDyn() != nil {
		t.Fatal("remote dest without DestDynamic must not fall back to source")
	}
	w := classify.Workload{Namespace: "ns", Name: "pg", PVCNames: []string{"data-pg-0"}}
	err := m.Replicate(context.Background(), w, movers.ClusterHandle{}, movers.ClusterHandle{})
	if err == nil {
		t.Fatal("must refuse to write ReplicationDestination on the source cluster")
	}
}

func TestReplicateScrubsMisplacedCRs(t *testing.T) {
	t.Parallel()
	kinds := map[schema.GroupVersionResource]string{
		srcGVR: "ReplicationSourceList",
		dstGVR: "ReplicationDestinationList",
	}
	srcDyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), kinds)
	dstDyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), kinds)
	srcKube := k8sfake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns"}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-pg-0", Namespace: "ns"}},
	)
	dstKube := k8sfake.NewSimpleClientset()
	misRS := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "volsync.backube/v1alpha1", "kind": "ReplicationSource",
		"metadata": map[string]any{"name": "portage-pg", "namespace": "ns"},
	}}
	misRD := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "volsync.backube/v1alpha1", "kind": "ReplicationDestination",
		"metadata": map[string]any{"name": "portage-pg", "namespace": "ns"},
	}}
	if _, err := dstDyn.Resource(srcGVR).Namespace("ns").Create(context.Background(), misRS, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := srcDyn.Resource(dstGVR).Namespace("ns").Create(context.Background(), misRD, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	m := Mover{
		Dynamic: srcDyn, DestDynamic: dstDyn,
		Kube: srcKube, DestKube: dstKube,
		Transport: portagev1alpha1.TransportObjectStore,
	}
	w := classify.Workload{Namespace: "ns", Name: "pg", PVCNames: []string{"data-pg-0"}}
	if err := m.Replicate(context.Background(), w, movers.ClusterHandle{}, movers.ClusterHandle{}); err != nil {
		t.Fatal(err)
	}
	if _, err := dstDyn.Resource(srcGVR).Namespace("ns").Get(context.Background(), "portage-pg", metav1.GetOptions{}); err == nil {
		t.Fatal("ReplicationSource must not remain on dest")
	}
	if _, err := srcDyn.Resource(dstGVR).Namespace("ns").Get(context.Background(), "portage-pg", metav1.GetOptions{}); err == nil {
		t.Fatal("ReplicationDestination must not remain on source")
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

func TestParseResticBytes(t *testing.T) {
	t.Parallel()
	n := parseResticBytes("processed 3 files, 75.964 KiB in 0:01\nsnapshot abc saved")
	if n < 70*1024 || n > 80*1024 {
		t.Fatalf("parsed %d", n)
	}
	if parseResticBytes("no size here") != 0 {
		t.Fatal("empty logs must parse as 0")
	}
}

func TestBackupReadsResticMoverSize(t *testing.T) {
	t.Parallel()
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		srcGVR: "ReplicationSourceList",
		dstGVR: "ReplicationDestinationList",
	})
	m := Mover{Dynamic: dyn, Transport: portagev1alpha1.TransportObjectStore}
	w := classify.Workload{Namespace: "ns", Name: "lone", Kind: "PersistentVolumeClaim", PVCNames: []string{"lone"}, Class: portagev1alpha1.ClassGenericPVC}
	art, err := m.Backup(context.Background(), w, movers.ClusterHandle{})
	if err != nil {
		t.Fatal(err)
	}
	if art.Useful || art.Message == "volsync is replicate, not backup" {
		t.Fatalf("pending restic must wait, not claim replicate-only: %+v", art)
	}
	src, err := dyn.Resource(srcGVR).Namespace("ns").Get(context.Background(), "portage-lone", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = unstructured.SetNestedField(src.Object, "2026-10-01T00:00:00Z", "status", "lastSyncTime")
	_ = unstructured.SetNestedMap(src.Object, map[string]any{
		"result": "Successful",
		"logs":   "processed 12 files, 128.0 KiB in 0:02\nsnapshot abc saved",
	}, "status", "latestMoverStatus")
	if _, err := dyn.Resource(srcGVR).Namespace("ns").Update(context.Background(), src, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	art, err = m.Backup(context.Background(), w, movers.ClusterHandle{})
	if err != nil {
		t.Fatal(err)
	}
	if !art.Useful || art.SizeBytes < 64*1024 {
		t.Fatalf("restic bytes must pass the volume floor: %+v", art)
	}
}

func TestStripDestSchedulingRemovesSourceHostname(t *testing.T) {
	t.Parallel()
	kinds := map[schema.GroupVersionResource]string{
		srcGVR: "ReplicationSourceList",
		dstGVR: "ReplicationDestinationList",
	}
	srcDyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), kinds)
	dstDyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), kinds)
	srcKube := k8sfake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns"}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "ns"}},
	)
	zero := int64(0)
	dstKube := k8sfake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns"}},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "aks-dest-1", Labels: map[string]string{hostnameLabel: "aks-dest-1"}},
			Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}},
		},
		// A NotReady Node object with a source hostname must not count as dest.
		// AffinityFromVolume resolves it and the mover stays Pending.
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "aks-doks-compat-z8vml", Labels: map[string]string{hostnameLabel: "aks-doks-compat-z8vml"}},
			Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}},
		},
		&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "redis", Namespace: "ns"},
			Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				NodeSelector:    map[string]string{hostnameLabel: "aks-doks-compat-z8vml"},
				SecurityContext: &corev1.PodSecurityContext{FSGroup: &zero},
				Containers:      []corev1.Container{{Name: "pg", Image: "postgres:16"}},
			}}},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "kept-0", Namespace: "ns"},
			Spec:       corev1.PodSpec{NodeName: "aks-dest-1"},
		},
		&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: "volsync-dst-portage-redis", Namespace: "ns"},
			Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				NodeSelector: map[string]string{hostnameLabel: "aks-doks-compat-z8vml"},
			}}},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "redis-0", Namespace: "ns"},
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{hostnameLabel: "aks-doks-compat-z8vml"},
				Volumes: []corev1.Volume{{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"},
					},
				}},
			},
			Status: corev1.PodStatus{Phase: corev1.PodPending},
		},
	)
	m := Mover{
		Dynamic: srcDyn, DestDynamic: dstDyn,
		Kube: srcKube, DestKube: dstKube,
		Transport: portagev1alpha1.TransportObjectStore,
	}
	w := classify.Workload{Namespace: "ns", Name: "redis", PVCNames: []string{"data"}}
	if err := m.Replicate(context.Background(), w, movers.ClusterHandle{}, movers.ClusterHandle{}); err != nil {
		t.Fatal(err)
	}
	sts, err := dstKube.AppsV1().StatefulSets("ns").Get(context.Background(), "redis", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if sts.Spec.Template.Spec.NodeSelector[hostnameLabel] != "" {
		t.Fatalf("dest STS still pinned: %v", sts.Spec.Template.Spec.NodeSelector)
	}
	if sts.Spec.Template.Spec.SecurityContext != nil && sts.Spec.Template.Spec.SecurityContext.FSGroup != nil {
		t.Fatalf("dest postgres fsGroup=%v must be cleared so kubelet does not chmod server.key", *sts.Spec.Template.Spec.SecurityContext.FSGroup)
	}
	if _, err := dstKube.CoreV1().Pods("ns").Get(context.Background(), "kept-0", metav1.GetOptions{}); err != nil {
		t.Fatal("pod scheduled on a Ready dest node must be kept")
	}
	if _, err := dstKube.BatchV1().Jobs("ns").Get(context.Background(), "volsync-dst-portage-redis", metav1.GetOptions{}); err == nil {
		t.Fatal("mover job pinned to a source-only hostname must be deleted so VolSync recreates it")
	}
	if _, err := dstKube.CoreV1().Pods("ns").Get(context.Background(), "redis-0", metav1.GetOptions{}); err == nil {
		t.Fatal("dest user pod pinned to a source-only hostname must be deleted so AffinityFromVolume does not copy it")
	}
	probe, err := m.Probe(context.Background(), w, movers.ClusterHandle{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(probe.Message, "source-only node") {
		t.Fatalf("deleted pin must not remain in probe: %s", probe.Message)
	}
}

func TestReplicatePausesWhenDestPodIsRunning(t *testing.T) {
	t.Parallel()
	kinds := map[schema.GroupVersionResource]string{
		srcGVR: "ReplicationSourceList",
		dstGVR: "ReplicationDestinationList",
	}
	srcDyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), kinds)
	dstDyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), kinds)
	srcKube := k8sfake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns"}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "ns"}},
	)
	dstKube := k8sfake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns"}},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pg-0", Namespace: "ns"},
			Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"},
				},
			}}},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "volsync-dst-portage-pg-abc", Namespace: "ns"},
			Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"},
				},
			}}},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		},
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "volsync-dst-portage-pg-abc", Namespace: "ns"}},
	)
	m := Mover{
		Dynamic: srcDyn, DestDynamic: dstDyn,
		Kube: srcKube, DestKube: dstKube,
		Transport: portagev1alpha1.TransportObjectStore,
	}
	w := classify.Workload{Namespace: "ns", Name: "pg", PVCNames: []string{"data"}}
	replicateUntilDest(t, m, w)
	rd, err := dstDyn.Resource(dstGVR).Namespace("ns").Get(context.Background(), "portage-pg", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	paused, _, _ := unstructured.NestedBool(rd.Object, "spec", "paused")
	if !paused {
		t.Fatal("dest restic must pause while a user pod is Running on the PVC")
	}
	if _, err := dstKube.BatchV1().Jobs("ns").Get(context.Background(), "volsync-dst-portage-pg-abc", metav1.GetOptions{}); err == nil {
		t.Fatal("in-flight dest mover job must be deleted so it cannot rewrite postmaster.pid")
	}
}

func TestReplicateDropsPairWhenSourcePVCIsGone(t *testing.T) {
	t.Parallel()
	kinds := map[schema.GroupVersionResource]string{
		srcGVR: "ReplicationSourceList",
		dstGVR: "ReplicationDestinationList",
	}
	srcDyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), kinds)
	dstDyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), kinds)
	rs := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "volsync.backube/v1alpha1", "kind": "ReplicationSource",
		"metadata": map[string]any{
			"name": "portage-postgres-data-hidden-surf-0", "namespace": "ns",
			"labels": map[string]any{"portage.io/name": "postgres-data-hidden-surf-0"},
		},
		"spec": map[string]any{"sourcePVC": "postgres-data-hidden-surf-0"},
	}}
	rd := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "volsync.backube/v1alpha1", "kind": "ReplicationDestination",
		"metadata": map[string]any{
			"name": "portage-postgres-data-hidden-surf-0", "namespace": "ns",
			"labels": map[string]any{"portage.io/name": "postgres-data-hidden-surf-0"},
		},
	}}
	if _, err := srcDyn.Resource(srcGVR).Namespace("ns").Create(context.Background(), rs, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := dstDyn.Resource(dstGVR).Namespace("ns").Create(context.Background(), rd, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	m := Mover{
		Dynamic: srcDyn, DestDynamic: dstDyn,
		Kube:      k8sfake.NewSimpleClientset(),
		DestKube:  k8sfake.NewSimpleClientset(),
		Transport: portagev1alpha1.TransportObjectStore,
	}
	w := classify.Workload{
		Namespace: "ns", Name: "postgres-data-hidden-surf-0", Kind: "PersistentVolumeClaim",
		PVCNames: []string{"postgres-data-hidden-surf-0"},
	}
	if err := m.Replicate(context.Background(), w, movers.ClusterHandle{}, movers.ClusterHandle{}); err != nil {
		t.Fatal(err)
	}
	if _, err := srcDyn.Resource(srcGVR).Namespace("ns").Get(context.Background(), rs.GetName(), metav1.GetOptions{}); err == nil {
		t.Fatal("ReplicationSource must be deleted when the source PVC is gone")
	}
	if _, err := dstDyn.Resource(dstGVR).Namespace("ns").Get(context.Background(), rd.GetName(), metav1.GetOptions{}); err == nil {
		t.Fatal("ReplicationDestination must be deleted when the source PVC is gone")
	}
}

func TestPruneRemovesReplicationForDeletedWorkload(t *testing.T) {
	t.Parallel()
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		srcGVR: "ReplicationSourceList",
		dstGVR: "ReplicationDestinationList",
	})
	for _, name := range []string{"gone", "live"} {
		obj := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "volsync.backube/v1alpha1", "kind": "ReplicationSource",
			"metadata": map[string]any{
				"name": "portage-" + name, "namespace": "ns",
				"labels": map[string]any{"portage.io/name": name},
			},
			"spec": map[string]any{"sourcePVC": name},
		}}
		if _, err := dyn.Resource(srcGVR).Namespace("ns").Create(context.Background(), obj, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		dst := obj.DeepCopy()
		dst.SetKind("ReplicationDestination")
		dst.SetAPIVersion("volsync.backube/v1alpha1")
		if _, err := dyn.Resource(dstGVR).Namespace("ns").Create(context.Background(), dst, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	foreign := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "volsync.backube/v1alpha1", "kind": "ReplicationSource",
		"metadata": map[string]any{"name": "someone-else", "namespace": "ns"},
	}}
	if _, err := dyn.Resource(srcGVR).Namespace("ns").Create(context.Background(), foreign, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	kube := k8sfake.NewSimpleClientset(
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "live", Namespace: "ns"}},
	)
	m := Mover{Dynamic: dyn, Kube: kube}
	if err := m.Prune(context.Background(), "ns", map[string]struct{}{"live": {}}); err != nil {
		t.Fatal(err)
	}
	if _, err := dyn.Resource(srcGVR).Namespace("ns").Get(context.Background(), "portage-gone", metav1.GetOptions{}); err == nil {
		t.Fatal("pair for a deleted workload must be pruned")
	}
	if _, err := dyn.Resource(srcGVR).Namespace("ns").Get(context.Background(), "portage-live", metav1.GetOptions{}); err != nil {
		t.Fatal("live workload pair must stay")
	}
	if _, err := dyn.Resource(srcGVR).Namespace("ns").Get(context.Background(), "someone-else", metav1.GetOptions{}); err != nil {
		t.Fatal("unlabeled ReplicationSource must stay")
	}
}
