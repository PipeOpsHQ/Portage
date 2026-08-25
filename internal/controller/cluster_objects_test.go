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

package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynfake "k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/clusters"
	"github.com/PipeOpsHQ/portage/pkg/objectstore"
)

func TestRestoreClusterObjectsDoesNotSucceedUntilDestGet(t *testing.T) {
	t.Parallel()
	r, dst, key, store := newObjectGraphHarness(t, true)
	_ = store
	dst.PrependReactor("get", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, "app")
	})
	drain(t, r, key)
	got := getAction(t, r, key)
	if got.Status.Phase == portagev1alpha1.ActionPhaseSucceeded {
		t.Fatalf("apply without dest Get must not Succeeded: %+v", got.Status)
	}
}

func TestRestoreClusterObjectsSucceedsAfterDestGet(t *testing.T) {
	t.Parallel()
	r, dst, key, _ := newObjectGraphHarness(t, true)
	drain(t, r, key)
	got := getAction(t, r, key)
	if got.Status.Phase != portagev1alpha1.ActionPhaseSucceeded {
		t.Fatalf("phase=%s message=%s", got.Status.Phase, got.Status.Message)
	}
	if got.Status.Attestation == nil {
		t.Fatal("expected attestation")
	}
	cm, err := dst.Resource(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}).
		Namespace("ns").Get(context.Background(), "app", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, _, _ := unstructuredData(cm.Object)
	if data["k"] != "v1" {
		t.Fatalf("dest cm = %v", cm.Object)
	}
}

func TestBackupThenRestoreClusterObjectsFromSnapshot(t *testing.T) {
	t.Parallel()
	scheme := newScheme(t)
	store := &objectstore.Memory{}
	srcKube, srcDyn := objectGraphClients(t, &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{Kind: "ConfigMap", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "ns"},
		Data:       map[string]string{"k": "snap"},
	})
	dstKube, dstDyn := objectGraphClients(t)
	pol := objectGraphPolicy()
	bak := &portagev1alpha1.Action{
		ObjectMeta: metav1.ObjectMeta{Name: "bak", Namespace: "ns"},
		Spec:       portagev1alpha1.ActionSpec{Type: portagev1alpha1.ActionBackup, PolicyRef: "p"},
	}
	c := ctrlfake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&portagev1alpha1.Action{}, &portagev1alpha1.Policy{}).
		WithObjects(pol, bak).Build()
	r := &ActionReconciler{
		Client:  c,
		Scheme:  scheme,
		Kube:    srcKube,
		Dynamic: srcDyn,
		Store:   store,
		Now:     func() time.Time { return time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC) },
		Resolve: func(context.Context, *portagev1alpha1.ClusterPair) (clusters.Pair, error) {
			return clusters.Pair{
				Source: clusters.Endpoints{Name: "aws", Kube: srcKube, Dynamic: srcDyn},
				Dest:   clusters.Endpoints{Name: "gcp", Kube: dstKube, Dynamic: dstDyn},
			}, nil
		},
	}
	drain(t, r, types.NamespacedName{Name: "bak", Namespace: "ns"})
	got := getAction(t, r, types.NamespacedName{Name: "bak", Namespace: "ns"})
	if got.Status.Phase != portagev1alpha1.ActionPhaseSucceeded {
		t.Fatalf("backup phase=%s %s", got.Status.Phase, got.Status.Message)
	}
	updated := &portagev1alpha1.Policy{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "p", Namespace: "ns"}, updated); err != nil {
		t.Fatal(err)
	}
	if !updated.Status.BackupHealthy || len(updated.Status.Artifacts) == 0 || updated.Status.Artifacts[0].ArtifactID == "" {
		t.Fatalf("expected useful object-graph artifact: %+v", updated.Status.Artifacts)
	}

	rst := &portagev1alpha1.Action{
		ObjectMeta: metav1.ObjectMeta{Name: "rst", Namespace: "ns"},
		Spec:       portagev1alpha1.ActionSpec{Type: portagev1alpha1.ActionRestore, PolicyRef: "p"},
	}
	if err := c.Create(context.Background(), rst); err != nil {
		t.Fatal(err)
	}
	drain(t, r, types.NamespacedName{Name: "rst", Namespace: "ns"})
	got = getAction(t, r, types.NamespacedName{Name: "rst", Namespace: "ns"})
	if got.Status.Phase != portagev1alpha1.ActionPhaseSucceeded {
		t.Fatalf("restore phase=%s %s", got.Status.Phase, got.Status.Message)
	}
	cm, err := dstDyn.Resource(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}).
		Namespace("ns").Get(context.Background(), "app", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, _, _ := unstructuredData(cm.Object)
	if data["k"] != "snap" {
		t.Fatalf("dest from snapshot = %v", cm.Object)
	}
}

func TestReplicateClusterObjectsLiveUpdatesDest(t *testing.T) {
	t.Parallel()
	scheme := newScheme(t)
	srcCM := &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{Kind: "ConfigMap", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "ns"},
		Data:       map[string]string{"k": "v1"},
	}
	srcKube, srcDyn := objectGraphClients(t, srcCM)
	dstKube, dstDyn := objectGraphClients(t)
	pol := objectGraphPolicy()
	act := &portagev1alpha1.Action{
		ObjectMeta: metav1.ObjectMeta{Name: "repl", Namespace: "ns"},
		Spec:       portagev1alpha1.ActionSpec{Type: portagev1alpha1.ActionReplicate, PolicyRef: "p"},
	}
	c := ctrlfake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&portagev1alpha1.Action{}, &portagev1alpha1.Policy{}).
		WithObjects(pol, act).Build()
	r := &ActionReconciler{
		Client:  c,
		Scheme:  scheme,
		Kube:    srcKube,
		Dynamic: srcDyn,
		Now:     func() time.Time { return time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC) },
		Resolve: func(context.Context, *portagev1alpha1.ClusterPair) (clusters.Pair, error) {
			return clusters.Pair{
				Source: clusters.Endpoints{Name: "aws", Kube: srcKube, Dynamic: srcDyn},
				Dest:   clusters.Endpoints{Name: "gcp", Kube: dstKube, Dynamic: dstDyn},
			}, nil
		},
	}
	drain(t, r, types.NamespacedName{Name: "repl", Namespace: "ns"})
	got := getAction(t, r, types.NamespacedName{Name: "repl", Namespace: "ns"})
	if got.Status.Phase != portagev1alpha1.ActionPhaseSucceeded {
		t.Fatalf("replicate phase=%s %s", got.Status.Phase, got.Status.Message)
	}

	srcCM.Data["k"] = "v2"
	srcKube, srcDyn = objectGraphClients(t, srcCM)
	r.Kube, r.Dynamic = srcKube, srcDyn
	r.Resolve = func(context.Context, *portagev1alpha1.ClusterPair) (clusters.Pair, error) {
		return clusters.Pair{
			Source: clusters.Endpoints{Name: "aws", Kube: srcKube, Dynamic: srcDyn},
			Dest:   clusters.Endpoints{Name: "gcp", Kube: dstKube, Dynamic: dstDyn},
		}, nil
	}
	act2 := &portagev1alpha1.Action{
		ObjectMeta: metav1.ObjectMeta{Name: "repl2", Namespace: "ns"},
		Spec:       portagev1alpha1.ActionSpec{Type: portagev1alpha1.ActionReplicate, PolicyRef: "p"},
	}
	if err := c.Create(context.Background(), act2); err != nil {
		t.Fatal(err)
	}
	drain(t, r, types.NamespacedName{Name: "repl2", Namespace: "ns"})
	got = getAction(t, r, types.NamespacedName{Name: "repl2", Namespace: "ns"})
	if got.Status.Phase != portagev1alpha1.ActionPhaseSucceeded {
		t.Fatalf("replicate-2 phase=%s %s", got.Status.Phase, got.Status.Message)
	}
	cm, err := dstDyn.Resource(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}).
		Namespace("ns").Get(context.Background(), "app", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, _, _ := unstructuredData(cm.Object)
	if data["k"] != "v2" {
		t.Fatalf("dest not live-synced: %v", cm.Object)
	}
}

func TestPolicyInventoryIncludesObjectGraph(t *testing.T) {
	t.Parallel()
	scheme := newScheme(t)
	pol := objectGraphPolicy()
	c := ctrlfake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&portagev1alpha1.Policy{}, &portagev1alpha1.Action{}).
		WithObjects(pol).Build()
	r := &PolicyReconciler{
		Client:     c,
		Scheme:     scheme,
		KubeClient: k8sfake.NewSimpleClientset(),
	}
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "p", Namespace: "ns"}})
	if err != nil {
		t.Fatal(err)
	}
	got := &portagev1alpha1.Policy{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "p", Namespace: "ns"}, got); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, it := range got.Status.Inventory {
		if it.Class == portagev1alpha1.ClassClusterObjects && it.Kind == "ObjectGraph" {
			found = true
		}
	}
	if !found {
		t.Fatalf("inventory missing ObjectGraph: %+v", got.Status.Inventory)
	}
}

func newObjectGraphHarness(t *testing.T, useful bool) (*ActionReconciler, *dynfake.FakeDynamicClient, types.NamespacedName, *objectstore.Memory) {
	t.Helper()
	scheme := newScheme(t)
	store := &objectstore.Memory{}
	srcKube, srcDyn := objectGraphClients(t, &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{Kind: "ConfigMap", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "ns"},
		Data:       map[string]string{"k": "v1"},
	})
	dstKube, dstDyn := objectGraphClients(t)
	pol := objectGraphPolicy()
	if useful {
		raw := []byte(`[{"group":"","version":"v1","resource":"configmaps","object":{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"app","namespace":"ns"},"data":{"k":"v1"}}}]`)
		if err := store.Put(context.Background(), "ns/ns_ObjectGraph_cluster-objects/objects.json", raw); err != nil {
			t.Fatal(err)
		}
		pol.Status.Artifacts = []portagev1alpha1.ArtifactHealth{{
			Workload:   "ns/ObjectGraph/cluster-objects",
			Useful:     true,
			SizeBytes:  int64(len(raw)),
			ArtifactID: "ns/ns_ObjectGraph_cluster-objects/objects.json",
			Message:    "1 objects snapshot",
		}}
	}
	act := &portagev1alpha1.Action{
		ObjectMeta: metav1.ObjectMeta{Name: "restore", Namespace: "ns"},
		Spec:       portagev1alpha1.ActionSpec{Type: portagev1alpha1.ActionRestore, PolicyRef: "p"},
	}
	c := ctrlfake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&portagev1alpha1.Action{}, &portagev1alpha1.Policy{}).
		WithObjects(pol, act).Build()
	r := &ActionReconciler{
		Client:  c,
		Scheme:  scheme,
		Kube:    srcKube,
		Dynamic: srcDyn,
		Store:   store,
		Now:     func() time.Time { return time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC) },
		Resolve: func(context.Context, *portagev1alpha1.ClusterPair) (clusters.Pair, error) {
			return clusters.Pair{
				Source: clusters.Endpoints{Name: "aws", Kube: srcKube, Dynamic: srcDyn},
				Dest:   clusters.Endpoints{Name: "gcp", Kube: dstKube, Dynamic: dstDyn},
			}, nil
		},
	}
	return r, dstDyn, types.NamespacedName{Name: "restore", Namespace: "ns"}, store
}

func objectGraphPolicy() *portagev1alpha1.Policy {
	return &portagev1alpha1.Policy{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns"},
		Spec: portagev1alpha1.PolicySpec{
			Selector:       portagev1alpha1.TargetSelector{Namespaces: []string{"ns"}},
			ClusterObjects: portagev1alpha1.ClusterObjectsSpec{Enabled: true},
		},
	}
}

func objectGraphClients(t *testing.T, objs ...runtime.Object) (*k8sfake.Clientset, *dynfake.FakeDynamicClient) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube := k8sfake.NewSimpleClientset()
	kube.Resources = []*metav1.APIResourceList{{
		GroupVersion: "v1",
		APIResources: []metav1.APIResource{
			{Name: "configmaps", Namespaced: true, Kind: "ConfigMap", Verbs: []string{"list", "get", "create", "update", "watch"}},
			{Name: "pods", Namespaced: true, Kind: "Pod", Verbs: []string{"list", "get", "create"}},
		},
	}}
	dyn := dynfake.NewSimpleDynamicClient(scheme, objs...)
	return kube, dyn
}

func unstructuredData(obj map[string]any) (map[string]any, bool, error) {
	m, ok := obj["data"].(map[string]any)
	return m, ok, nil
}
