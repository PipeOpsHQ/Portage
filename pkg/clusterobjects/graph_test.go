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

package clusterobjects

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	fakediscovery "k8s.io/client-go/discovery/fake"
	dynfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/PipeOpsHQ/portage/pkg/transform"
)

func TestSyncThenAttestConfigMap(t *testing.T) {
	t.Parallel()
	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	src := dynfake.NewSimpleDynamicClient(scheme, &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{Kind: "ConfigMap", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "ns", UID: "src-uid"},
		Data:       map[string]string{"k": "v"},
	})
	dst := dynfake.NewSimpleDynamicClient(scheme)

	items, err := List(context.Background(), src, []Ref{{GroupVersionResource: gvr, Namespaced: true}}, []string{"ns"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("listed %d", len(items))
	}
	items = Sanitize(items, transform.Options{})
	if items[0].Obj.GetUID() != "" {
		t.Fatal("sanitize must drop UID")
	}
	if err := Sync(context.Background(), dst, items); err != nil {
		t.Fatal(err)
	}
	ok, msg := Attest(context.Background(), dst, items)
	if !ok {
		t.Fatal(msg)
	}
	got, err := dst.Resource(gvr).Namespace("ns").Get(context.Background(), "app", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Object["data"].(map[string]any)["k"] != "v" {
		t.Fatalf("%v", got.Object)
	}
}

func TestActiveReplicationUpdatesDest(t *testing.T) {
	t.Parallel()
	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	cm := &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{Kind: "ConfigMap", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "ns"},
		Data:       map[string]string{"k": "v1"},
	}
	src := dynfake.NewSimpleDynamicClient(scheme, cm.DeepCopy())
	dst := dynfake.NewSimpleDynamicClient(scheme)
	items, _ := List(context.Background(), src, []Ref{{GroupVersionResource: gvr, Namespaced: true}}, []string{"ns"}, nil)
	_ = Sync(context.Background(), dst, Sanitize(items, transform.Options{}))

	cm.Data["k"] = "v2"
	src = dynfake.NewSimpleDynamicClient(scheme, cm)
	items, _ = List(context.Background(), src, []Ref{{GroupVersionResource: gvr, Namespaced: true}}, []string{"ns"}, nil)
	if err := Sync(context.Background(), dst, Sanitize(items, transform.Options{})); err != nil {
		t.Fatal(err)
	}
	got, err := dst.Resource(gvr).Namespace("ns").Get(context.Background(), "app", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Object["data"].(map[string]any)["k"] != "v2" {
		t.Fatalf("dest not live-synced: %v", got.Object["data"])
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	t.Parallel()
	items := []Item{{
		GVR: schema.GroupVersionResource{Version: "v1", Resource: "secrets"},
		Obj: &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Secret",
			"metadata": map[string]any{"name": "s", "namespace": "ns"},
		}},
	}}
	raw, err := Marshal(items)
	if err != nil {
		t.Fatal(err)
	}
	back, err := Unmarshal(raw)
	if err != nil || len(back) != 1 || back[0].Obj.GetName() != "s" {
		t.Fatalf("%v %v", err, back)
	}
}

func TestAttestFailsWhenDestMissing(t *testing.T) {
	t.Parallel()
	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	dst := dynfake.NewSimpleDynamicClient(scheme)
	items := []Item{{
		GVR: gvr,
		Obj: &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "app", "namespace": "ns"},
		}},
	}}
	ok, msg := Attest(context.Background(), dst, items)
	if ok {
		t.Fatal("dest Get miss must not attest")
	}
	if msg == "" {
		t.Fatal("expected missing-object message")
	}
}

func TestDiscoverSkipsEphemeralKeepsUnknownCR(t *testing.T) {
	t.Parallel()
	disco := &fakediscovery.FakeDiscovery{Fake: &clienttesting.Fake{
		Resources: []*metav1.APIResourceList{
			{GroupVersion: "v1", APIResources: []metav1.APIResource{
				{Name: "pods", Namespaced: true, Kind: "Pod", Verbs: []string{"list", "create", "get"}},
				{Name: "configmaps", Namespaced: true, Kind: "ConfigMap", Verbs: []string{"list", "create", "get"}},
				{Name: "nodes", Namespaced: false, Kind: "Node", Verbs: []string{"list", "create"}},
				{Name: "namespaces", Namespaced: false, Kind: "Namespace", Verbs: []string{"list", "create"}},
			}},
			{GroupVersion: "apiextensions.k8s.io/v1", APIResources: []metav1.APIResource{
				{Name: "customresourcedefinitions", Namespaced: false, Kind: "CustomResourceDefinition", Verbs: []string{"list", "create", "get"}},
			}},
			{GroupVersion: "stable.example.com/v1", APIResources: []metav1.APIResource{
				{Name: "widgets", Namespaced: true, Kind: "Widget", Verbs: []string{"list", "create", "get"}},
				{Name: "clusterwidgets", Namespaced: false, Kind: "ClusterWidget", Verbs: []string{"list", "create", "get"}},
			}},
			{GroupVersion: "events.k8s.io/v1", APIResources: []metav1.APIResource{
				{Name: "events", Namespaced: true, Kind: "Event", Verbs: []string{"list", "create"}},
			}},
		},
	}}
	gvrs, err := Discover(disco, false)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, g := range gvrs {
		got[g.Resource] = true
	}
	if got["pods"] || got["nodes"] || got["events"] {
		t.Fatalf("ephemeral leaked: %v", gvrs)
	}
	if !got["configmaps"] {
		t.Fatal("configmaps must stay in the graph")
	}
	if !got["widgets"] {
		t.Fatal("unknown CR must stay in the graph")
	}
	if !got["customresourcedefinitions"] {
		t.Fatal("CRDs must always be in the graph")
	}
	if got["namespaces"] || got["clusterwidgets"] {
		t.Fatalf("cluster-scoped except CRDs require IncludeClusterScoped: %v", gvrs)
	}
	scoped, err := Discover(disco, true)
	if err != nil {
		t.Fatal(err)
	}
	var ns, crd, clusterCR bool
	for _, g := range scoped {
		if g.Resource == "namespaces" {
			ns = true
		}
		if g.Resource == "customresourcedefinitions" {
			crd = true
		}
		if g.Resource == "clusterwidgets" {
			clusterCR = true
		}
		if g.Resource == "nodes" {
			t.Fatal("nodes are dest-local")
		}
	}
	if !ns {
		t.Fatal("IncludeClusterScoped must add namespaces")
	}
	if !crd {
		t.Fatal("CRDs must stay when IncludeClusterScoped")
	}
	if !clusterCR {
		t.Fatal("unknown cluster-scoped CR must stay in the graph")
	}
}

func TestForbiddenGVRSkipped(t *testing.T) {
	t.Parallel()
	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "secrets"}
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	src := dynfake.NewSimpleDynamicClient(scheme)
	src.PrependReactor("list", "secrets", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, "", fmt.Errorf("no"))
	})
	items, err := List(context.Background(), src, []Ref{{GroupVersionResource: gvr, Namespaced: true}}, []string{"ns"}, nil)
	if err != nil {
		t.Fatalf("403 must skip, not fail: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("listed %d", len(items))
	}
}

func TestSkipDestLocalObjects(t *testing.T) {
	t.Parallel()
	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "secrets"}
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	src := dynfake.NewSimpleDynamicClient(scheme, &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{Kind: "Secret", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{Name: "default-token", Namespace: "ns"},
		Type:       corev1.SecretTypeServiceAccountToken,
	}, &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{Kind: "Secret", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "ns"},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{"k": []byte("v")},
	})
	items, err := List(context.Background(), src, []Ref{{GroupVersionResource: gvr, Namespaced: true}}, []string{"ns"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Obj.GetName() != "app" {
		t.Fatalf("SA tokens are dest-local: %+v", items)
	}
}

func TestCRDAttestRequiresEstablished(t *testing.T) {
	t.Parallel()
	gvr := schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}
	crd := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata":   map[string]any{"name": "widgets.stable.example.com"},
	}}
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		gvr: "CustomResourceDefinitionList",
	}, crd)
	items := []Item{{GVR: gvr, Obj: crd}}
	ok, _ := Attest(context.Background(), dyn, items)
	if ok {
		t.Fatal("CRD without Established must not attest")
	}
	_ = unstructured.SetNestedSlice(crd.Object, []any{
		map[string]any{"type": "Established", "status": "True"},
	}, "status", "conditions")
	dyn = dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		gvr: "CustomResourceDefinitionList",
	}, crd)
	ok, msg := Attest(context.Background(), dyn, items)
	if !ok {
		t.Fatal(msg)
	}
}

func TestUnknownCRStaysInGraphAndLiveSyncs(t *testing.T) {
	t.Parallel()
	gvr := schema.GroupVersionResource{Group: "stable.example.com", Version: "v1", Resource: "widgets"}
	widget := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "stable.example.com/v1",
		"kind":       "Widget",
		"metadata":   map[string]any{"name": "w1", "namespace": "ns", "uid": "src"},
		"spec":       map[string]any{"n": "v1"},
	}}
	lists := map[schema.GroupVersionResource]string{gvr: "WidgetList"}
	src := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), lists, widget)
	dst := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), lists)
	items, err := List(context.Background(), src, []Ref{{GroupVersionResource: gvr, Namespaced: true}}, []string{"ns"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("unknown CR dropped: %d", len(items))
	}
	if err := Sync(context.Background(), dst, Sanitize(items, transform.Options{})); err != nil {
		t.Fatal(err)
	}
	ok, msg := Attest(context.Background(), dst, items)
	if !ok {
		t.Fatal(msg)
	}
	got, err := dst.Resource(gvr).Namespace("ns").Get(context.Background(), "w1", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetUID() == "src" {
		t.Fatal("sanitize must drop source UID")
	}
}

func TestCRDThenCRLiveSync(t *testing.T) {
	t.Parallel()
	crdGVR := schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}
	crGVR := schema.GroupVersionResource{Group: "stable.example.com", Version: "v1", Resource: "widgets"}
	crd := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata":   map[string]any{"name": "widgets.stable.example.com"},
		"spec":       map[string]any{"group": "stable.example.com"},
		"status": map[string]any{"conditions": []any{
			map[string]any{"type": "Established", "status": "True"},
		}},
	}}
	widget := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "stable.example.com/v1",
		"kind":       "Widget",
		"metadata":   map[string]any{"name": "w1", "namespace": "ns"},
		"spec":       map[string]any{"k": "v1"},
	}}
	lists := map[schema.GroupVersionResource]string{
		crdGVR: "CustomResourceDefinitionList",
		crGVR:  "WidgetList",
	}
	src := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), lists, crd, widget)
	dst := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), lists)
	items, err := List(context.Background(), src, []Ref{
		{GroupVersionResource: crdGVR, Namespaced: false},
		{GroupVersionResource: crGVR, Namespaced: true},
	}, []string{"ns"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("want CRD+CR, got %d", len(items))
	}
	if err := Sync(context.Background(), dst, Sanitize(items, transform.Options{})); err != nil {
		t.Fatal(err)
	}
	if _, err := dst.Resource(crdGVR).Get(context.Background(), "widgets.stable.example.com", metav1.GetOptions{}); err != nil {
		t.Fatalf("dest CRD: %v", err)
	}
	got, err := dst.Resource(crGVR).Namespace("ns").Get(context.Background(), "w1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("dest CR: %v", err)
	}
	spec, _, _ := unstructured.NestedString(got.Object, "spec", "k")
	if spec != "v1" {
		t.Fatalf("widget spec.k=%s", spec)
	}
}

func TestSkipPortageCRDAndSystemClusterRole(t *testing.T) {
	t.Parallel()
	crdGVR := schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}
	roleGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
	lists := map[schema.GroupVersionResource]string{
		crdGVR:  "CustomResourceDefinitionList",
		roleGVR: "ClusterRoleList",
	}
	src := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), lists,
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
			"metadata": map[string]any{"name": "policies.portage.io"},
		}},
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
			"metadata": map[string]any{"name": "widgets.stable.example.com"},
		}},
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
			"metadata": map[string]any{"name": "system:controller:generic-garbage-collector"},
		}},
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
			"metadata": map[string]any{"name": "tenant-admin"},
		}},
	)
	items, err := List(context.Background(), src, []Ref{
		{GroupVersionResource: crdGVR, Namespaced: false},
		{GroupVersionResource: roleGVR, Namespaced: false},
	}, []string{"ns"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, it := range items {
		got[it.Obj.GetName()] = true
	}
	if got["policies.portage.io"] {
		t.Fatal("portage CRDs are dest-local")
	}
	if got["system:controller:generic-garbage-collector"] {
		t.Fatal("system ClusterRoles are dest-local")
	}
	if !got["widgets.stable.example.com"] || !got["tenant-admin"] {
		t.Fatalf("user CRD and ClusterRole dropped: %v", got)
	}
}
